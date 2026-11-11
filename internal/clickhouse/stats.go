package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/mr-karan/logchef/internal/queryescape"
)

const (
	// Keep stats queries fast so one slow sub-query doesn't block the whole response.
	tableStatsTimeoutSeconds     = 2
	ingestionStatsTimeoutSeconds = 3
	ingestionStatsTimeoutGrace   = 500 * time.Millisecond

	// DefaultViewActivityMaxRowsToRead bounds rows scanned when computing Recent
	// activity through an opted-in ClickHouse View unless overridden per source.
	DefaultViewActivityMaxRowsToRead int64 = 50_000_000
	// DefaultViewActivityMaxBytesToRead bounds uncompressed bytes scanned (1 GiB)
	// when computing Recent activity through an opted-in ClickHouse View.
	DefaultViewActivityMaxBytesToRead int64 = 1 << 30
)

// IngestionStatsOptions configures execution safeguards for IngestionStatsWithOptions.
type IngestionStatsOptions struct {
	// EnforceScanLimits applies finite row/byte scan caps and forces ClickHouse
	// overflow modes to "throw" so partial aggregates are never returned.
	EnforceScanLimits bool
	MaxRowsToRead     *int64
	MaxBytesToRead    *int64
}

func (c *Client) statsQueryContext(ctx context.Context, timeoutSeconds int) (context.Context, context.CancelFunc) {
	if timeoutSeconds <= 0 {
		return ctx, func() {}
	}

	queryCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	queryCtx = clickhouse.Context(queryCtx, clickhouse.WithSettings(buildQuerySettings(timeoutSeconds, nil, c.querySettings)))
	return queryCtx, cancel
}

// TableStat represents overall statistics for a ClickHouse table,
// typically retrieved from system.parts.
type TableStat struct {
	Database     string  `json:"database"`
	Table        string  `json:"table"`
	Compressed   string  `json:"compressed"`   // Total size on disk (human-readable).
	Uncompressed string  `json:"uncompressed"` // Total original size (human-readable).
	ComprRate    float64 `json:"compr_rate"`   // Overall compression rate.
	Rows         uint64  `json:"rows"`         // Total rows in the table partition/part.
	PartCount    uint64  `json:"part_count"`   // Number of data parts.
}

// IngestionBucket represents ingestion volume for a given time bucket.
type IngestionBucket struct {
	Bucket time.Time `json:"bucket"`
	Rows   uint64    `json:"rows"`
}

// IngestionStats represents recent ingestion activity for a table.
type IngestionStats struct {
	Rows1h        uint64            `json:"rows_1h"`
	Rows24h       uint64            `json:"rows_24h"`
	Rows7d        uint64            `json:"rows_7d"`
	LatestTS      *time.Time        `json:"latest_ts,omitempty"`
	HourlyBuckets []IngestionBucket `json:"hourly_buckets"`
	DailyBuckets  []IngestionBucket `json:"daily_buckets"`
}

// TableStats retrieves overall statistics for a specific table from active parts.
func (c *Client) TableStats(ctx context.Context, database, table string) (*TableStat, error) {
	// Query system.parts for aggregated table statistics.
	query := fmt.Sprintf(`
		SELECT
			database,
			table,
			formatReadableSize(sum(data_compressed_bytes) AS size) AS compressed,
			formatReadableSize(sum(data_uncompressed_bytes) AS usize) AS uncompressed,
			round(usize / size, 2) AS compr_rate,
			sum(rows) AS rows,
			count() AS part_count
		FROM system.parts
		WHERE (active = 1) AND (database = '%s') AND (table = '%s')
		GROUP BY
			database,
			table
		ORDER BY size DESC
	`, queryescape.ClickHouseString(database), queryescape.ClickHouseString(table))

	queryCtx, cancel := c.statsQueryContext(ctx, tableStatsTimeoutSeconds)
	defer cancel()

	rows, err := c.conn.Query(queryCtx, query)
	if err != nil {
		return nil, fmt.Errorf("error executing table stats query: %w", err)
	}
	defer rows.Close()

	var stats []TableStat // Use a slice in case query returns multiple rows unexpectedly.
	for rows.Next() {
		var stat TableStat
		if err := rows.Scan(
			&stat.Database,
			&stat.Table,
			&stat.Compressed,
			&stat.Uncompressed,
			&stat.ComprRate,
			&stat.Rows,
			&stat.PartCount,
		); err != nil {
			return nil, fmt.Errorf("error scanning table stats row: %w", err)
		}

		// Replace NaN resulting from division by zero with 0.
		if math.IsNaN(stat.ComprRate) {
			stat.ComprRate = 0
		}

		stats = append(stats, stat)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table stats rows: %w", err)
	}

	// If no active parts found, return default empty stats.
	if len(stats) == 0 {
		return &TableStat{
			Database:     database,
			Table:        table,
			Compressed:   "0B",
			Uncompressed: "0B",
			ComprRate:    0,
			Rows:         0,
			PartCount:    0,
		}, nil
	}

	// Return the first row (should be the only one).
	return &stats[0], nil
}

type ingestionActivityRow struct {
	bucket time.Time
	rows   uint64
	rows1h uint64
	latest *time.Time
}

func ingestionActivityQuery(database, table, timestampField string) (string, error) {
	if timestampField == "" {
		return "", fmt.Errorf("timestamp field is required for ingestion stats")
	}
	tsField := queryescape.ClickHouseIdentifier(timestampField)
	qualifiedTable := fmt.Sprintf("%s.%s", queryescape.ClickHouseIdentifier(database), queryescape.ClickHouseIdentifier(table))
	return fmt.Sprintf(`
		WITH now64(3) AS anchor
		SELECT toStartOfHour(%s) AS bucket,
			count() AS rows,
			countIf(%s >= anchor - INTERVAL 1 HOUR AND %s <= anchor) AS rows_1h,
			max(%s) AS latest_ts
		FROM %s
		WHERE %s >= anchor - INTERVAL 24 HOUR AND %s <= anchor
		GROUP BY bucket
		ORDER BY bucket ASC
	`, tsField, tsField, tsField, tsField, qualifiedTable, tsField, tsField), nil
}

func accumulateIngestionActivity(rows []ingestionActivityRow) *IngestionStats {
	stats := &IngestionStats{HourlyBuckets: make([]IngestionBucket, 0, len(rows))}
	for _, row := range rows {
		stats.HourlyBuckets = append(stats.HourlyBuckets, IngestionBucket{Bucket: row.bucket, Rows: row.rows})
		stats.Rows24h += row.rows
		stats.Rows1h += row.rows1h
		if row.latest != nil && (stats.LatestTS == nil || row.latest.After(*stats.LatestTS)) {
			stats.LatestTS = row.latest
		}
	}
	return stats
}

func activityError(queryCtx context.Context, operation string, err error) error {
	if errors.Is(context.Cause(queryCtx), context.DeadlineExceeded) || errors.Is(queryCtx.Err(), context.DeadlineExceeded) || IsTimeoutError(err) {
		return fmt.Errorf("%s: %w", operation, context.DeadlineExceeded)
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func positiveInt64Setting(settings clickhouse.Settings, key string) (int64, bool) {
	if settings == nil {
		return 0, false
	}
	switch v := settings[key].(type) {
	case int:
		return int64(v), v > 0
	case int64:
		return v, v > 0
	case uint64:
		if v > 0 && v <= math.MaxInt64 {
			return int64(v), true
		}
	}
	return 0, false
}

func buildIngestionStatsSettings(source clickhouse.Settings, opts IngestionStatsOptions) clickhouse.Settings {
	if !opts.EnforceScanLimits {
		return buildQuerySettings(ingestionStatsTimeoutSeconds, clickhouse.Settings{"max_threads": 2}, source)
	}

	settings := make(clickhouse.Settings, len(source)+7)
	maps.Copy(settings, source)

	timeout := int64(ingestionStatsTimeoutSeconds)
	if srcTimeout, ok := positiveInt64Setting(source, "max_execution_time"); ok && srcTimeout < timeout {
		timeout = srcTimeout
	}
	settings["max_execution_time"] = int(timeout)

	rowsCap := DefaultViewActivityMaxRowsToRead
	if opts.MaxRowsToRead != nil && *opts.MaxRowsToRead > 0 {
		rowsCap = *opts.MaxRowsToRead
	}
	if srcRows, ok := positiveInt64Setting(source, "max_rows_to_read"); ok && srcRows < rowsCap {
		rowsCap = srcRows
	}
	settings["max_rows_to_read"] = rowsCap

	bytesCap := DefaultViewActivityMaxBytesToRead
	if opts.MaxBytesToRead != nil && *opts.MaxBytesToRead > 0 {
		bytesCap = *opts.MaxBytesToRead
	}
	if srcBytes, ok := positiveInt64Setting(source, "max_bytes_to_read"); ok && srcBytes < bytesCap {
		bytesCap = srcBytes
	}
	settings["max_bytes_to_read"] = bytesCap

	settings["max_threads"] = 2
	// Force overflow modes to "throw" so ClickHouse cannot stop early and
	// return partial count()/max() aggregates when a cap or timeout is hit.
	settings["read_overflow_mode"] = "throw"
	settings["timeout_overflow_mode"] = "throw"
	settings["result_overflow_mode"] = "throw"
	return settings
}

// ingestionActivityQueryContext attaches explicit ClickHouse query settings and
// a Go-side timeout backstop without exposing a context.Deadline to clickhouse-go.
// clickhouse-go's queryOptions(ctx) overwrites WithSettings' max_execution_time
// with int(remainingSeconds+5) whenever ctx.Deadline() is more than 1s away,
// which would turn a 1s or 3s server cap into 7s.
func ingestionActivityQueryContext(ctx context.Context, settings clickhouse.Settings, grace time.Duration) (context.Context, context.CancelFunc) {
	backstopSeconds := int64(ingestionStatsTimeoutSeconds)
	if configured, ok := positiveInt64Setting(settings, "max_execution_time"); ok && configured < backstopSeconds {
		backstopSeconds = configured
	}
	backstop := time.Duration(backstopSeconds)*time.Second + grace

	queryCtx, cancelCause := context.WithCancelCause(context.WithoutCancel(ctx))
	if ctx.Err() != nil {
		cancelCause(context.Cause(ctx))
	}
	stopParent := context.AfterFunc(ctx, func() {
		cancelCause(context.Cause(ctx))
	})
	timer := time.AfterFunc(backstop, func() {
		cancelCause(context.DeadlineExceeded)
	})

	queryCtx = clickhouse.Context(queryCtx, clickhouse.WithSettings(settings))
	cleanup := func() {
		timer.Stop()
		stopParent()
		cancelCause(nil)
	}
	return queryCtx, cleanup
}

// IngestionStats retrieves bounded, recent ingestion activity in one query.
func (c *Client) IngestionStats(ctx context.Context, database, table, timestampField string) (*IngestionStats, error) {
	return c.IngestionStatsWithOptions(ctx, database, table, timestampField, IngestionStatsOptions{})
}

// IngestionStatsWithOptions retrieves bounded, recent ingestion activity in one
// query with optional scan caps and overflow enforcement.
func (c *Client) IngestionStatsWithOptions(ctx context.Context, database, table, timestampField string, opts IngestionStatsOptions) (*IngestionStats, error) {
	query, err := ingestionActivityQuery(database, table, timestampField)
	if err != nil {
		return nil, err
	}
	settings := buildIngestionStatsSettings(c.querySettings, opts)
	queryCtx, cancel := ingestionActivityQueryContext(ctx, settings, ingestionStatsTimeoutGrace)
	defer cancel()
	rows, err := c.conn.Query(queryCtx, query)
	if err != nil {
		return nil, activityError(queryCtx, "error executing ingestion activity query", err)
	}
	defer rows.Close()
	resultRows := make([]ingestionActivityRow, 0)
	for rows.Next() {
		var row ingestionActivityRow
		if err := rows.Scan(&row.bucket, &row.rows, &row.rows1h, &row.latest); err != nil {
			return nil, activityError(queryCtx, "scan ingestion activity row", err)
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, activityError(queryCtx, "iterate ingestion activity rows", err)
	}
	return accumulateIngestionActivity(resultRows), nil
}
