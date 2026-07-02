package insights_test

import (
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
	n := i.Ingest("convex-prod", events)
	if n != 2 {
		t.Errorf("kept=%d want 2", n)
	}
	if i.MemLen() != 2 {
		t.Errorf("MemLen=%d", i.MemLen())
	}
}

func TestRingBufferCapPerDeployment(t *testing.T) {
	t.Parallel()
	i := insights.New(3)
	floodA := func() {
		_ = i.Ingest("A", []insights.AnyEvent{
			{"FunctionCall": map[string]any{"is_occ": true, "udf_id": "fa", "id": "xa"}},
		})
	}
	_ = i.Ingest("B", []insights.AnyEvent{
		{"FunctionCall": map[string]any{
			"is_occ": true, "udf_id": "fb", "id": "xb", "request_id": "rb",
			"component_path": "_default", "occ_table_name": "tb", "status": "retried",
		}},
	})
	for range 50 {
		floodA()
	}
	if i.MemLen() != 4 {
		t.Errorf("MemLen=%d want 4 (A capped at 3 + B's 1)", i.MemLen())
	}
	today := time.Now().UTC().Format("2006-01-02")
	out, err := i.Query("B", today, today)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0][1] != "fb" {
		t.Fatalf("flooding A evicted B's row: %v", out)
	}
	outA, err := i.Query("A", today, today)
	if err != nil {
		t.Fatal(err)
	}
	if len(outA) != 1 {
		t.Fatalf("expected 1 grouped row for A, got %v", outA)
	}
	if !strings.Contains(outA[0][3].(string), `"occCalls":3`) {
		t.Errorf("A should be capped at 3 rows: %s", outA[0][3])
	}
}

func TestQueryIsolatedPerDeployment(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	_ = i.Ingest("A", []insights.AnyEvent{
		{"FunctionCall": map[string]any{
			"is_occ": true, "udf_id": "fa", "id": "xa",
			"component_path": "_default", "status": "retried",
		}},
	})
	today := time.Now().UTC().Format("2006-01-02")
	out, err := i.Query("B", today, today)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("deployment B must not see A's rows: %v", out)
	}
}

func TestQueryDateValidation(t *testing.T) {
	t.Parallel()
	i := insights.New(10)
	if _, err := i.Query("x", "bad", "2026-05-21"); err == nil {
		t.Error("expected bad-date error")
	} else if !errors.Is(err, insights.ErrBadDateRange) {
		t.Errorf("expected ErrBadDateRange, got %v", err)
	}
	if _, err := i.Query("x", "2026-05-22", "2026-05-21"); err == nil {
		t.Error("expected reversed-range error")
	}
}

func TestQueryOCCAggregation(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	for k := range 5 {
		_ = i.Ingest("p", []insights.AnyEvent{
			{"FunctionCall": map[string]any{
				"is_occ": true, "udf_id": "counters:increment",
				"id": "id" + string(rune('A'+k)), "request_id": "r" + string(rune('A'+k)),
				"component_path": "_default", "occ_table_name": "counters",
				"occ_retry_count": 1, "status": "retried",
			}},
		})
	}
	today := time.Now().UTC().Format("2006-01-02")
	out, err := i.Query("p", today, today)
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

func TestQueryOCCFailedPermanentlyUsesExplicitSignal(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	_ = i.Ingest("p", []insights.AnyEvent{
		{"FunctionCall": map[string]any{
			"is_occ": true, "udf_id": "f", "id": "i",
			"component_path":         "_default",
			"status":                 "failure",
			"occ_failed_permanently": true,
		}},
	})
	today := time.Now().UTC().Format("2006-01-02")
	out, _ := i.Query("p", today, today)
	if len(out) == 0 || out[0][0] != "occFailedPermanently" {
		t.Errorf("expected occFailedPermanently; got %v", out)
	}
}

func TestIngestStripsGroupSeparator(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	_ = i.Ingest("p", []insights.AnyEvent{
		{"FunctionCall": map[string]any{
			"is_occ": true, "udf_id": "u:f\x1fevil", "id": "x",
			"component_path": "comp\x1fonent", "occ_table_name": "t",
			"status": "retried",
		}},
	})
	today := time.Now().UTC().Format("2006-01-02")
	out, err := i.Query("p", today, today)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 group row, got %v", out)
	}
	if out[0][1] != "u:fevil" || out[0][2] != "component" {
		t.Errorf("0x1f must be stripped from udf_id/component_path at ingest, got %v/%v", out[0][1], out[0][2])
	}
}

func TestOCCGroupKeySpaceSafe(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	for _, table := range []string{"users by_name", "users by_idx"} {
		_ = i.Ingest("p", []insights.AnyEvent{
			{"FunctionCall": map[string]any{
				"is_occ": true, "udf_id": "u:f", "id": "x",
				"component_path": "_default", "occ_table_name": table,
				"status": "retried",
			}},
		})
	}
	today := time.Now().UTC().Format("2006-01-02")
	out, _ := i.Query("p", today, today)
	if len(out) != 2 {
		t.Errorf("expected 2 groups (one per table), got %d (%v)", len(out), out)
	}
}

func TestIngestQueryRoundTripRowShapes(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	_ = i.Ingest("p", []insights.AnyEvent{
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
	out, err := i.Query("p", today, today)
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
