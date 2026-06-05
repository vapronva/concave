package insights_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/insights"
)

func TestIngestRecognizesOCCAndReadLimit(t *testing.T) {
	t.Parallel()
	i := insights.New(10)
	events := []insights.AnyEvent{
		{"FunctionCall": map[string]any{
			"is_occ": true, "udf_id": "f1", "id": "e1", "request_id": "r1",
			"component_path": "_default", "occ_table_name": "users",
			"occ_retry_count": 2, "status": "success",
		}},
		{"FunctionCall": map[string]any{
			"is_occ": false, "udf_id": "f2",
		}},
		{"InsightReadLimit": map[string]any{
			"udf_id": "g1", "id": "e2", "request_id": "r2",
			"success": true,
			"calls": []any{
				map[string]any{"table_name": "items", "bytes_read": 1_000_000, "documents_read": 10},
			},
		}},
		{"UnknownVariant": map[string]any{"v": 1}},
	}
	n, err := i.Ingest(context.Background(), "convex-prod", events)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("kept=%d want 2", n)
	}
	if i.MemLen() != 2 {
		t.Errorf("MemLen=%d", i.MemLen())
	}
}

func TestRingBufferCap(t *testing.T) {
	t.Parallel()
	i := insights.New(3)
	for range 5 {
		_, _ = i.Ingest(context.Background(), "d", []insights.AnyEvent{
			{"FunctionCall": map[string]any{"is_occ": true, "udf_id": "f", "id": "x"}},
		})
	}
	if i.MemLen() != 3 {
		t.Errorf("ring cap broken: MemLen=%d", i.MemLen())
	}
}

func TestQueryDateValidation(t *testing.T) {
	t.Parallel()
	i := insights.New(10)
	if _, err := i.Query(context.Background(), "x", "bad", "2026-05-21"); err == nil {
		t.Error("expected bad-date error")
	} else if !errors.Is(err, insights.ErrBadDateRange) {
		t.Errorf("expected ErrBadDateRange, got %v", err)
	}
	if _, err := i.Query(context.Background(), "x", "2026-05-22", "2026-05-21"); err == nil {
		t.Error("expected reversed-range error")
	}
}

func TestQueryOCCAggregation(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	for k := range 5 {
		_, _ = i.Ingest(context.Background(), "p", []insights.AnyEvent{
			{"FunctionCall": map[string]any{
				"is_occ": true, "udf_id": "counters:increment",
				"id": "id" + string(rune('A'+k)), "request_id": "r" + string(rune('A'+k)),
				"component_path": "_default", "occ_table_name": "counters",
				"occ_retry_count": 1, "status": "retried",
			}},
		})
	}
	today := time.Now().UTC().Format("2006-01-02")
	out, err := i.Query(context.Background(), "p", today, today)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 group row, got %d (%v)", len(out), out)
	}
	row := out[0]
	if row[0] != "occRetried" {
		t.Errorf("kind=%v", row[0])
	}
	if row[1] != "counters:increment" {
		t.Errorf("udfId=%v", row[1])
	}
	body, _ := row[3].(string)
	if !strings.Contains(body, `"occCalls":5`) {
		t.Errorf("body missing occCalls:5: %s", body)
	}
	if !strings.Contains(body, `"occTableName":"counters"`) {
		t.Errorf("body missing occTableName: %s", body)
	}
}

func TestQueryOCCFailedPermanentlyWhenStatusFailure(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	_, _ = i.Ingest(context.Background(), "p", []insights.AnyEvent{
		{"FunctionCall": map[string]any{
			"is_occ": true, "udf_id": "f", "id": "i",
			"component_path": "_default",
			"status":         "failure",
		}},
	})
	today := time.Now().UTC().Format("2006-01-02")
	out, _ := i.Query(context.Background(), "p", today, today)
	if len(out) == 0 || out[0][0] != "occFailedPermanently" {
		t.Errorf("expected occFailedPermanently; got %v", out)
	}
}

func TestQueryReadLimitBytesThresholdAndLimit(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	_, _ = i.Ingest(context.Background(), "p", []insights.AnyEvent{
		{"InsightReadLimit": map[string]any{
			"udf_id": "q", "id": "i", "request_id": "r",
			"calls": []any{
				map[string]any{"table_name": "items", "bytes_read": 17 * 1024 * 1024, "documents_read": 1},
			},
		}},
	})
	today := time.Now().UTC().Format("2006-01-02")
	out, _ := i.Query(context.Background(), "p", today, today)
	var sawLimit bool
	for _, row := range out {
		if row[0] == "bytesReadLimit" {
			sawLimit = true
		}
	}
	if !sawLimit {
		t.Errorf("expected bytesReadLimit row; got %v", out)
	}
}

func TestOCCGroupKeySpaceSafe(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	for _, table := range []string{"users by_name", "users by_idx"} {
		_, _ = i.Ingest(context.Background(), "p", []insights.AnyEvent{
			{"FunctionCall": map[string]any{
				"is_occ": true, "udf_id": "u:f", "id": "x",
				"component_path": "_default", "occ_table_name": table,
				"status": "retried",
			}},
		})
	}
	today := time.Now().UTC().Format("2006-01-02")
	out, _ := i.Query(context.Background(), "p", today, today)
	if len(out) != 2 {
		t.Errorf("expected 2 groups (one per table), got %d (%v)", len(out), out)
	}
}

func TestIngestQueryRoundTripRowShapes(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	_, _ = i.Ingest(context.Background(), "p", []insights.AnyEvent{
		{"FunctionCall": map[string]any{
			"is_occ": true, "udf_id": "mod:occFn", "id": "occ1", "request_id": "rq1",
			"component_path": "-root-component-", "occ_table_name": "docs",
			"occ_retry_count": 3, "status": "retried",
		}},
		{"InsightReadLimit": map[string]any{
			"udf_id": "mod:readFn", "id": "rd1", "request_id": "rq2",
			"component_path": "-root-component-", "success": false,
			"calls": []any{
				map[string]any{"table_name": "big", "bytes_read": 2_000_000, "documents_read": 40_000},
			},
		}},
	})
	today := time.Now().UTC().Format("2006-01-02")
	out, err := i.Query(context.Background(), "p", today, today)
	if err != nil {
		t.Fatal(err)
	}
	kinds := make(map[string][]any)
	for _, row := range out {
		if len(row) != 4 {
			t.Fatalf("row must have 4 cells, got %d: %v", len(row), row)
		}
		k, _ := row[0].(string)
		if _, ok := row[3].(string); !ok {
			t.Fatalf("cell 4 must be a JSON string, got %T", row[3])
		}
		kinds[k] = row
	}
	occ, ok := kinds["occRetried"]
	if !ok {
		t.Fatalf("missing occRetried row; got %v", out)
	}
	if occ[1] != "mod:occFn" || occ[2] != "-root-component-" {
		t.Errorf("occ row udfId/comp = %v/%v", occ[1], occ[2])
	}
	if !strings.Contains(occ[3].(string), `"occTableName":"docs"`) {
		t.Errorf("occ body missing occTableName: %s", occ[3])
	}
	if _, ok = kinds["bytesReadThreshold"]; !ok {
		t.Errorf("missing bytesReadThreshold row; got %v", out)
	}
	if _, ok = kinds["documentsReadLimit"]; !ok {
		t.Errorf("missing documentsReadLimit row; got %v", out)
	}
}

func TestRingGrowsInMemory(t *testing.T) {
	t.Parallel()
	i := insights.New(10)
	_, _ = i.Ingest(context.Background(), "p", []insights.AnyEvent{
		{"FunctionCall": map[string]any{"is_occ": true, "udf_id": "f", "id": "i"}},
	})
	if i.MemLen() != 1 {
		t.Errorf("expected mem to grow in in-memory mode; got %d", i.MemLen())
	}
}
