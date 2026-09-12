package insights_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/insights"
)

func TestRingBufferCapPerDeployment(t *testing.T) {
	t.Parallel()
	i := insights.New(3)
	floodA := func() {
		_ = i.Ingest("A", insights.DefaultReadLimits(), []insights.AnyEvent{
			{"FunctionCall": map[string]any{"is_occ": true, "udf_id": "fa", "id": "xa"}},
		})
	}
	_ = i.Ingest("B", insights.DefaultReadLimits(), []insights.AnyEvent{
		{"FunctionCall": map[string]any{
			"is_occ": true, "udf_id": "fb", "id": "xb", "request_id": "rb",
			"component_path": "_default", "occ_table_name": "tb", "status": "retried",
		}},
	})
	for range 50 {
		floodA()
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

func TestQueryOCCFailedPermanentlyUsesExplicitSignal(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	_ = i.Ingest("p", insights.DefaultReadLimits(), []insights.AnyEvent{
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

func TestIngestQueryRoundTripRowShapes(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	kept := i.Ingest("p", insights.DefaultReadLimits(), []insights.AnyEvent{
		{"FunctionCall": map[string]any{"is_occ": false, "udf_id": "mod:plainFn"}},
		{"UnknownVariant": map[string]any{"v": 1}},
		{"FunctionCall": map[string]any{
			"is_occ": true, "udf_id": "mod:occFn", "id": "occ1", "request_id": "rq1",
			"component_path": "-root-component-", "occ_table_name": "docs",
			"occ_retry_count": json.Number("3"), "status": "retried",
		}},
		{"InsightReadLimit": map[string]any{
			"udf_id": "mod:readFn", "id": "rd1", "request_id": "rq2",
			"component_path": "-root-component-", "success": false,
			"calls": []any{
				map[string]any{
					"table_name":     "big",
					"bytes_read":     json.Number("2000000"),
					"documents_read": json.Number("40000"),
				},
			},
		}},
	})
	if kept != 2 {
		t.Fatalf("only OCC calls and read-limit insights are kept, got kept=%d", kept)
	}
	if _, err := i.Query("p", "2026-05-22", "2026-05-21"); !errors.Is(err, insights.ErrBadDateRange) {
		t.Fatalf("a reversed range must be ErrBadDateRange, got %v", err)
	}
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
	if _, ok = kinds["documentsReadLimit"]; !ok {
		t.Errorf("missing documentsReadLimit row; got %v", out)
	}
	for _, kind := range []string{"bytesReadThreshold", "bytesReadLimit", "documentsReadThreshold"} {
		if _, ok = kinds[kind]; ok {
			t.Errorf("2 MB / 40,000 docs must not produce %s; got %v", kind, out)
		}
	}
}

func TestQueryOCCPermanenceSplitsGroups(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	_ = i.Ingest("p", insights.DefaultReadLimits(), []insights.AnyEvent{
		{"FunctionCall": map[string]any{
			"is_occ": true, "udf_id": "f", "id": "a", "component_path": "_default", "occ_table_name": "t",
		}},
		{"FunctionCall": map[string]any{
			"is_occ": true, "udf_id": "f", "id": "b", "component_path": "_default", "occ_table_name": "t",
			"occ_failed_permanently": true,
		}},
	})
	today := time.Now().UTC().Format("2006-01-02")
	out, _ := i.Query("p", today, today)
	bodies := make(map[string]string)
	for _, row := range out {
		bodies[row[0].(string)] = row[3].(string)
	}
	for _, kind := range []string{"occRetried", "occFailedPermanently"} {
		if body, ok := bodies[kind]; !ok || !strings.Contains(body, `"occCalls":1`) {
			t.Errorf("want a %s row counting one call; got %v", kind, out)
		}
	}
}

func TestQueryReadDimensionsCountTheirOwnRows(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	calls := func(bytes, docs string) []any {
		return []any{map[string]any{
			"table_name": "t", "bytes_read": json.Number(bytes), "documents_read": json.Number(docs),
		}}
	}
	_ = i.Ingest("p", insights.DefaultReadLimits(), []insights.AnyEvent{
		{"InsightReadLimit": map[string]any{
			"udf_id": "f", "id": "a", "component_path": "_default", "calls": calls("16777216", "1"),
		}},
		{"InsightReadLimit": map[string]any{
			"udf_id": "f", "id": "b", "component_path": "_default", "calls": calls("1", "30000"),
		}},
	})
	today := time.Now().UTC().Format("2006-01-02")
	out, _ := i.Query("p", today, today)
	bodies := make(map[string]string)
	for _, row := range out {
		bodies[row[0].(string)] = row[3].(string)
	}
	if len(bodies) != 2 {
		t.Fatalf("want one bytes row and one documents row, got %v", out)
	}
	for _, kind := range []string{"bytesReadLimit", "documentsReadThreshold"} {
		if body, ok := bodies[kind]; !ok || !strings.Contains(body, `"count":1`) {
			t.Errorf("want a %s row counting only its own execution; got %v", kind, out)
		}
	}
}
