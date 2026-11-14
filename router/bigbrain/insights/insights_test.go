package insights_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/insights"
)

func testLimits() insights.ReadLimits {
	return insights.ReadLimits{Documents: 32000, Bytes: 16 * 1024 * 1024, WarningRatio: 0.8}
}

func events(t *testing.T, raw string) []insights.Event {
	t.Helper()
	var out []insights.Event
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("decode events: %v", err)
	}
	return out
}

func kindBodies(t *testing.T, i *insights.Insights, deployment string) map[string]string {
	t.Helper()
	today := time.Now().UTC().Format("2006-01-02")
	out, err := i.Query(deployment, today, today)
	if err != nil {
		t.Fatal(err)
	}
	bodies := make(map[string]string, len(out))
	for _, row := range out {
		bodies[row[0].(string)] = row[3].(string)
	}
	return bodies
}

func TestRingBufferCapPerDeployment(t *testing.T) {
	t.Parallel()
	i := insights.New(3)
	_, _ = i.Ingest("B", testLimits(), events(t, `[{"FunctionCall":{
		"is_occ":true,"udf_id":"fb","id":"xb","request_id":"rb",
		"component_path":"_default","occ_table_name":"tb","status":"failure"}}]`))
	for range 50 {
		_, _ = i.Ingest("A", testLimits(), events(t, `[{"FunctionCall":{"is_occ":true,"udf_id":"fa","id":"xa"}}]`))
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

func TestRingEvictsFromTheMostPopulousKind(t *testing.T) {
	t.Parallel()
	i := insights.New(4)
	_, _ = i.Ingest("p", testLimits(), events(t, `[
		{"FunctionCall":{"is_occ":true,"udf_id":"f","id":"perm","occ_failed_permanently":true,"status":"failure"}},
		{"InsightReadLimit":{"udf_id":"r","id":"read","success":true,
			"calls":[{"table_name":"t","bytes_read":1,"documents_read":30000}]}}]`))
	retry := events(t, `[{"FunctionCall":{"is_occ":true,"udf_id":"f","id":"retry","status":"failure"}}]`)
	for range 100 {
		_, _ = i.Ingest("p", testLimits(), retry)
	}
	bodies := kindBodies(t, i, "p")
	for kind, want := range map[string]string{
		"occFailedPermanently":   `"occCalls":1`,
		"documentsReadThreshold": `"count":1`,
		"occRetried":             `"occCalls":2`,
	} {
		if !strings.Contains(bodies[kind], want) {
			t.Errorf("%s: want %s, got %q (all: %v)", kind, want, bodies[kind], bodies)
		}
	}
}

func TestQueryOCCFailedPermanentlyUsesExplicitSignal(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	_, _ = i.Ingest("p", testLimits(), events(t, `[{"FunctionCall":{
		"is_occ":true,"udf_id":"f","id":"i","component_path":"_default",
		"status":"failure","occ_failed_permanently":true}}]`))
	today := time.Now().UTC().Format("2006-01-02")
	out, _ := i.Query("p", today, today)
	if len(out) == 0 || out[0][0] != "occFailedPermanently" {
		t.Errorf("expected occFailedPermanently; got %v", out)
	}
}

func TestIngestQueryRoundTripRowShapes(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	kept, _ := i.Ingest("p", testLimits(), events(t, `[
		{"FunctionCall":{"is_occ":false,"udf_id":"mod:plainFn"}},
		{"UnknownVariant":{"v":1}},
		{"FunctionCall":{"is_occ":true,"udf_id":"mod:occFn","id":"occ1","request_id":"rq1",
			"component_path":"-root-component-","occ_table_name":"docs","occ_retry_count":3,"status":"failure"}},
		{"InsightReadLimit":{"udf_id":"mod:readFn","id":"rd1","request_id":"rq2",
			"component_path":"-root-component-","success":false,
			"calls":[{"table_name":"big","bytes_read":2000000,"documents_read":40000}]}}]`))
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
	_, _ = i.Ingest("p", testLimits(), events(t, `[
		{"FunctionCall":{"is_occ":true,"udf_id":"f","id":"a","component_path":"_default","occ_table_name":"t"}},
		{"FunctionCall":{"is_occ":true,"udf_id":"f","id":"b","component_path":"_default","occ_table_name":"t",
			"occ_failed_permanently":true}}]`))
	bodies := kindBodies(t, i, "p")
	for _, kind := range []string{"occRetried", "occFailedPermanently"} {
		if body, ok := bodies[kind]; !ok || !strings.Contains(body, `"occCalls":1`) {
			t.Errorf("want a %s row counting one call; got %v", kind, bodies)
		}
	}
}

func TestQueryReadDimensionsCountTheirOwnRows(t *testing.T) {
	t.Parallel()
	i := insights.New(100)
	_, _ = i.Ingest("p", testLimits(), events(t, `[
		{"InsightReadLimit":{"udf_id":"f","id":"a","component_path":"_default",
			"calls":[{"table_name":"t","bytes_read":16777216,"documents_read":1}]}},
		{"InsightReadLimit":{"udf_id":"f","id":"b","component_path":"_default",
			"calls":[{"table_name":"t","bytes_read":1,"documents_read":32001}]}}]`))
	bodies := kindBodies(t, i, "p")
	if len(bodies) != 2 {
		t.Fatalf("want one bytes row and one documents row, got %v", bodies)
	}
	for _, kind := range []string{"bytesReadThreshold", "documentsReadLimit"} {
		if body, ok := bodies[kind]; !ok || !strings.Contains(body, `"count":1`) {
			t.Errorf("want a %s row counting only its own execution (reading exactly the limit succeeds); got %v",
				kind, bodies)
		}
	}
	if body := bodies["documentsReadLimit"]; !strings.Contains(body, `"limit":32000`) ||
		!strings.Contains(body, `"threshold":25600`) {
		t.Errorf("a read row must carry the deployment's limit and threshold; got %s", body)
	}
}
