package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestIngestionActivityQueryIsBoundedToOneDay(t *testing.T) {
	query, err := ingestionActivityQuery("logs", "events", "event_time")
	if err != nil {
		t.Fatal(err)
	}
	upper := strings.ToUpper(query)
	if strings.Count(upper, "WHERE") != 1 || !strings.Contains(upper, "INTERVAL 24 HOUR") {
		t.Fatalf("expected exactly one 24 hour WHERE clause, got: %s", query)
	}
	if strings.Count(upper, "<= ANCHOR") != 2 {
		t.Fatalf("expected future rows excluded from the window and 1h count, got: %s", query)
	}
	for _, forbidden := range []string{"INTERVAL 7 DAY", "INTERVAL 30 DAY", "ROWS_7D", "DAILY"} {
		if strings.Contains(upper, forbidden) {
			t.Fatalf("unexpected unbounded summary %q in query: %s", forbidden, query)
		}
	}
	if !strings.Contains(query, "now64(3)") {
		t.Fatalf("expected millisecond anchor, got: %s", query)
	}
}

func TestAccumulateIngestionActivity(t *testing.T) {
	first := time.Date(2025, time.January, 2, 9, 0, 0, 0, time.UTC)
	latest := first.Add(90 * time.Minute)
	stats := accumulateIngestionActivity([]ingestionActivityRow{
		{bucket: first, rows: 4, rows1h: 1, latest: &first},
		{bucket: first.Add(time.Hour), rows: 6, rows1h: 3, latest: &latest},
	})
	if stats.Rows1h != 4 || stats.Rows24h != 10 {
		t.Fatalf("unexpected totals: %#v", stats)
	}
	if stats.LatestTS == nil || !stats.LatestTS.Equal(latest) {
		t.Fatalf("unexpected latest timestamp: %#v", stats.LatestTS)
	}
	if len(stats.HourlyBuckets) != 2 || stats.HourlyBuckets[0].Rows != 4 || !stats.HourlyBuckets[1].Bucket.Equal(first.Add(time.Hour)) {
		t.Fatalf("unexpected hourly aggregation: %#v", stats.HourlyBuckets)
	}
}

func TestBuildIngestionStatsSettingsDefaultPreservesSourceSettings(t *testing.T) {
	t.Parallel()

	source := clickhouse.Settings{
		"max_execution_time":   60,
		"result_overflow_mode": "break",
		"readonly":             2,
	}
	got := buildIngestionStatsSettings(source, IngestionStatsOptions{})
	if got["max_execution_time"] != 60 {
		t.Fatalf("max_execution_time = %v, want 60 from source settings", got["max_execution_time"])
	}
	if got["result_overflow_mode"] != "break" {
		t.Fatalf("result_overflow_mode = %v, want break", got["result_overflow_mode"])
	}
	if _, hasRows := got["max_rows_to_read"]; hasRows {
		t.Fatalf("unexpected max_rows_to_read on non-view settings: %#v", got)
	}
}

func TestBuildIngestionStatsSettingsEnforcesViewScanCapsAndThrowOverflow(t *testing.T) {
	t.Parallel()

	permissiveSource := clickhouse.Settings{
		"max_execution_time":    120,
		"max_rows_to_read":      int64(500_000_000),
		"max_bytes_to_read":     int64(10 << 30),
		"read_overflow_mode":    "break",
		"timeout_overflow_mode": "break",
		"result_overflow_mode":  "break",
		"readonly":              2,
	}
	got := buildIngestionStatsSettings(permissiveSource, IngestionStatsOptions{EnforceScanLimits: true})
	if got["max_execution_time"] != ingestionStatsTimeoutSeconds {
		t.Fatalf("max_execution_time = %v, want %d", got["max_execution_time"], ingestionStatsTimeoutSeconds)
	}
	if got["max_rows_to_read"] != DefaultViewActivityMaxRowsToRead {
		t.Fatalf("max_rows_to_read = %v, want %d", got["max_rows_to_read"], DefaultViewActivityMaxRowsToRead)
	}
	if got["max_bytes_to_read"] != DefaultViewActivityMaxBytesToRead {
		t.Fatalf("max_bytes_to_read = %v, want %d", got["max_bytes_to_read"], DefaultViewActivityMaxBytesToRead)
	}
	for _, modeKey := range []string{"read_overflow_mode", "timeout_overflow_mode", "result_overflow_mode"} {
		if got[modeKey] != "throw" {
			t.Fatalf("%s = %v, want throw", modeKey, got[modeKey])
		}
	}
	if got["readonly"] != 2 {
		t.Fatalf("readonly = %v, want 2", got["readonly"])
	}

	customRows := int64(250_000)
	customBytes := int64(64 << 20)
	tighterSource := clickhouse.Settings{
		"max_execution_time": 1,
		"max_rows_to_read":   int64(100_000),
	}
	clamped := buildIngestionStatsSettings(tighterSource, IngestionStatsOptions{
		EnforceScanLimits: true,
		MaxRowsToRead:     &customRows,
		MaxBytesToRead:    &customBytes,
	})
	if clamped["max_execution_time"] != 1 {
		t.Fatalf("max_execution_time = %v, want tighter source cap 1", clamped["max_execution_time"])
	}
	if clamped["max_rows_to_read"] != int64(100_000) {
		t.Fatalf("max_rows_to_read = %v, want tighter source cap 100000", clamped["max_rows_to_read"])
	}
	if clamped["max_bytes_to_read"] != customBytes {
		t.Fatalf("max_bytes_to_read = %v, want custom cap %d", clamped["max_bytes_to_read"], customBytes)
	}
}

func TestIsReadLimitExceededError(t *testing.T) {
	t.Parallel()

	if IsReadLimitExceededError(nil) {
		t.Fatal("nil error reported as read limit exceeded")
	}
	for _, code := range []int32{chExceptionTooManyRows, chExceptionTooManyBytes} {
		err := fmt.Errorf("activity failed: %w", publicQueryError(&clickhouse.Exception{Code: code, Message: "limit exceeded"}))
		if !IsReadLimitExceededError(err) {
			t.Fatalf("code %d not classified as read limit exceeded", code)
		}
	}
	timeoutErr := &clickhouse.Exception{Code: chExceptionTimeoutExceeded, Message: "timeout"}
	if IsReadLimitExceededError(timeoutErr) {
		t.Fatal("timeout exception classified as read limit exceeded")
	}
}

func TestIngestionActivityQueryContextStripsDeadlineAndMapsTimeoutCause(t *testing.T) {
	t.Parallel()

	parent, cancelParent := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancelParent()

	// Even when the caller context has a >1s deadline, queryCtx must not expose
	// Deadline() or clickhouse-go's queryOptions(ctx) will overwrite max_execution_time.
	queryCtx, cleanup := ingestionActivityQueryContext(parent, clickhouse.Settings{"max_execution_time": 1}, -950*time.Millisecond)
	defer cleanup()

	if deadline, ok := queryCtx.Deadline(); ok {
		t.Fatalf("queryCtx.Deadline() = (%v, true), want (_, false) so clickhouse-go preserves max_execution_time", deadline)
	}

	// With max_execution_time=1 and -950ms test grace, the 50ms backstop fires quickly
	// with cause context.DeadlineExceeded, and activityError classifies it as DeadlineExceeded.
	select {
	case <-queryCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ingestion activity backstop timer")
	}
	if !errors.Is(context.Cause(queryCtx), context.DeadlineExceeded) {
		t.Fatalf("context.Cause(queryCtx) = %v, want context.DeadlineExceeded", context.Cause(queryCtx))
	}
	wrapped := activityError(queryCtx, "error executing ingestion activity query", context.Canceled)
	if !errors.Is(wrapped, context.DeadlineExceeded) {
		t.Fatalf("activityError(%v) = %v, want wrapped context.DeadlineExceeded", queryCtx.Err(), wrapped)
	}

	// Explicit caller cancellation (non-timeout) must NOT be misclassified as DeadlineExceeded.
	callerCtx, cancelCaller := context.WithCancel(context.Background())
	canceledQueryCtx, cleanupCanceled := ingestionActivityQueryContext(callerCtx, clickhouse.Settings{"max_execution_time": 3}, ingestionStatsTimeoutGrace)
	defer cleanupCanceled()
	cancelCaller()
	select {
	case <-canceledQueryCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for parent cancellation propagation")
	}
	canceledWrapped := activityError(canceledQueryCtx, "error executing ingestion activity query", context.Canceled)
	if errors.Is(canceledWrapped, context.DeadlineExceeded) {
		t.Fatalf("explicitly canceled query misclassified as DeadlineExceeded: %v", canceledWrapped)
	}
}
