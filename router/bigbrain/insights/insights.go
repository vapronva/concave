package insights

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	documentsReadLimit         = 32000
	bytesReadLimit             = 16 * 1024 * 1024
	rootComponent              = "-root-component-"
	groupSep                   = "\x1f"
	defaultRingCap             = 50_000
	maxRecentPerGroup          = 50
	dayLookbackHours           = 24
	occKeyPartsCount           = 3
	readKeyPartsCount          = 2
	statusFailure              = "failure"
	kindOCC                    = "occ"
	kindRead                   = "read"
	kindOCCRetried             = "occRetried"
	kindOCCFailedPermanent     = "occFailedPermanently"
	kindBytesReadLimit         = "bytesReadLimit"
	kindBytesReadThreshold     = "bytesReadThreshold"
	kindDocumentsReadLimit     = "documentsReadLimit"
	kindDocumentsReadThreshold = "documentsReadThreshold"
	eventFunctionCall          = "FunctionCall"
	eventInsightReadLimit      = "InsightReadLimit"
	fieldRequestID             = "request_id"
	fieldOCCRetryCount         = "occ_retry_count"
	fieldCalls                 = "calls"
	fieldSuccess               = "success"
)

var ErrBadDateRange = errors.New("from/to must be YYYY-MM-DD and from < to")

var dateRE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

type Row struct {
	Deployment     string
	TS             time.Time
	Kind           string
	UDFID          string
	ComponentPath  *string
	RequestID      string
	ExecutionID    string
	OCCTableName   *string
	OCCDocumentID  *string
	OCCWriteSource *string
	OCCRetryCount  int
	Status         *string
	Success        *bool
	Calls          []Call
}

type Call struct {
	TableName     string `json:"table_name"`
	BytesRead     int    `json:"bytes_read"`
	DocumentsRead int    `json:"documents_read"`
}

type Insights struct {
	memMu sync.Mutex
	mem   []Row
	cap   int
}

func New(ringCap int) *Insights {
	if ringCap <= 0 {
		ringCap = defaultRingCap
	}
	return &Insights{cap: ringCap}
}

type AnyEvent map[string]map[string]any

func (i *Insights) Ingest(_ context.Context, deployment string, events []AnyEvent) (int, error) {
	kept := 0
	for _, ev := range events {
		for k, payload := range ev {
			row, ok := makeRow(deployment, k, payload)
			if !ok {
				continue
			}
			i.store(row)
			kept++
		}
	}
	return kept, nil
}

func (i *Insights) store(r Row) {
	i.memMu.Lock()
	defer i.memMu.Unlock()
	i.mem = append(i.mem, r)
	if len(i.mem) > i.cap {
		i.mem = i.mem[len(i.mem)-i.cap:]
	}
}

func (i *Insights) MemLen() int {
	i.memMu.Lock()
	defer i.memMu.Unlock()
	return len(i.mem)
}

func (i *Insights) rows(deployment string, fromMs, toMs int64) []Row {
	i.memMu.Lock()
	defer i.memMu.Unlock()
	var out []Row
	for _, r := range i.mem {
		if r.Deployment != deployment {
			continue
		}
		ms := r.TS.UnixMilli()
		if ms >= fromMs && ms < toMs {
			out = append(out, r)
		}
	}
	return out
}

func (i *Insights) Query(_ context.Context, deployment, fromDate, toDate string) ([][]any, error) {
	if !dateRE.MatchString(fromDate) || !dateRE.MatchString(toDate) {
		return nil, ErrBadDateRange
	}
	from, err := time.Parse("2006-01-02", fromDate)
	if err != nil {
		return nil, ErrBadDateRange
	}
	to, err := time.Parse("2006-01-02", toDate)
	if err != nil {
		return nil, ErrBadDateRange
	}
	fromMs := from.UTC().UnixMilli()
	toMs := to.UTC().Add(dayLookbackHours * time.Hour).UnixMilli()
	if fromMs >= toMs {
		return nil, ErrBadDateRange
	}
	return aggregate(i.rows(deployment, fromMs, toMs)), nil
}

func aggregate(rows []Row) [][]any {
	out := aggregateOCC(rows)
	out = append(out, aggregateRead(rows)...)
	return out
}

func aggregateOCC(rows []Row) [][]any {
	groups := groupByOCC(rows)
	out := make([][]any, 0, len(groups))
	for key, grp := range groups {
		parts := strings.SplitN(key, groupSep, occKeyPartsCount)
		udfID, comp, occTable := parts[0], parts[1], parts[2]
		out = append(out, buildOCCRow(udfID, comp, occTable, grp))
	}
	return out
}

func buildOCCRow(udfID, comp, occTable string, grp []Row) []any {
	permanently := false
	for _, r := range grp {
		if r.Status != nil && *r.Status == statusFailure {
			permanently = true
			break
		}
	}
	kind := kindOCCRetried
	if permanently {
		kind = kindOCCFailedPermanent
	}
	hourly := bucket(timestampsOf(grp))
	sort.Slice(grp, func(i, j int) bool { return grp[i].TS.After(grp[j].TS) })
	occCalls := len(grp)
	if len(grp) > maxRecentPerGroup {
		grp = grp[:maxRecentPerGroup]
	}
	recent := make([]map[string]any, 0, len(grp))
	for _, r := range grp {
		rec := map[string]any{
			"timestamp":        r.TS.UTC().Format(time.RFC3339Nano),
			"id":               r.ExecutionID,
			fieldRequestID:     r.RequestID,
			fieldOCCRetryCount: r.OCCRetryCount,
		}
		if r.OCCDocumentID != nil {
			rec["occ_document_id"] = *r.OCCDocumentID
		}
		if r.OCCWriteSource != nil {
			rec["occ_write_source"] = *r.OCCWriteSource
		}
		recent = append(recent, rec)
	}
	body := map[string]any{
		"occCalls":     occCalls,
		"hourlyCounts": hourly,
		"recentEvents": recent,
	}
	if occTableField := pickOCCTableField(occTable, grp); occTableField != nil {
		body["occTableName"] = occTableField
	}
	return []any{kind, udfID, comp, string(mustJSON(body))}
}

func pickOCCTableField(occTable string, grp []Row) any {
	if occTable != "" {
		return occTable
	}
	if len(grp) > 0 && grp[0].OCCTableName != nil {
		return *grp[0].OCCTableName
	}
	return nil
}

func groupByOCC(rows []Row) map[string][]Row {
	g := make(map[string][]Row)
	for _, r := range rows {
		if r.Kind != kindOCC {
			continue
		}
		comp := derefOr(r.ComponentPath, rootComponent)
		occTbl := derefOr(r.OCCTableName, "")
		key := r.UDFID + groupSep + comp + groupSep + occTbl
		g[key] = append(g[key], r)
	}
	return g
}

func aggregateRead(rows []Row) [][]any {
	groups := groupByRead(rows)
	out := make([][]any, 0)
	for key, grp := range groups {
		parts := strings.SplitN(key, groupSep, readKeyPartsCount)
		udfID, comp := parts[0], parts[1]
		maxBytes, maxDocs := readMaxes(grp)
		hourly := bucket(timestampsOf(grp))
		count := len(grp)
		sort.Slice(grp, func(i, j int) bool { return grp[i].TS.After(grp[j].TS) })
		if len(grp) > maxRecentPerGroup {
			grp = grp[:maxRecentPerGroup]
		}
		recent := readRecent(grp)
		emit := func(kind string) {
			body := map[string]any{
				"count":        count,
				"hourlyCounts": hourly,
				"recentEvents": recent,
			}
			out = append(out, []any{kind, udfID, comp, string(mustJSON(body))})
		}
		switch {
		case maxBytes >= bytesReadLimit:
			emit(kindBytesReadLimit)
		case maxBytes > 0:
			emit(kindBytesReadThreshold)
		}
		switch {
		case maxDocs >= documentsReadLimit:
			emit(kindDocumentsReadLimit)
		case maxDocs > 0:
			emit(kindDocumentsReadThreshold)
		}
	}
	return out
}

func readMaxes(grp []Row) (int, int) {
	maxBytes, maxDocs := 0, 0
	for _, r := range grp {
		bsum, dsum := 0, 0
		for _, c := range r.Calls {
			bsum += c.BytesRead
			dsum += c.DocumentsRead
		}
		if bsum > maxBytes {
			maxBytes = bsum
		}
		if dsum > maxDocs {
			maxDocs = dsum
		}
	}
	return maxBytes, maxDocs
}

func readRecent(grp []Row) []map[string]any {
	recent := make([]map[string]any, 0, len(grp))
	for _, r := range grp {
		succ := true
		if r.Success != nil {
			succ = *r.Success
		}
		recent = append(recent, map[string]any{
			"timestamp":    r.TS.UTC().Format(time.RFC3339Nano),
			"id":           r.ExecutionID,
			fieldRequestID: r.RequestID,
			fieldCalls:     r.Calls,
			fieldSuccess:   succ,
		})
	}
	return recent
}

func groupByRead(rows []Row) map[string][]Row {
	g := make(map[string][]Row)
	for _, r := range rows {
		if r.Kind != kindRead {
			continue
		}
		comp := derefOr(r.ComponentPath, rootComponent)
		key := r.UDFID + groupSep + comp
		g[key] = append(g[key], r)
	}
	return g
}

func timestampsOf(grp []Row) []time.Time {
	times := make([]time.Time, 0, len(grp))
	for _, r := range grp {
		times = append(times, r.TS)
	}
	return times
}

func bucket(times []time.Time) []map[string]any {
	m := make(map[string]int)
	for _, t := range times {
		h := t.UTC().Format("2006-01-02 15:00:00")
		m[h]++
	}
	out := make([]map[string]any, 0, len(m))
	for h, c := range m {
		out = append(out, map[string]any{"hour": h, "count": c})
	}
	sort.Slice(out, func(i, j int) bool {
		left, _ := out[i]["hour"].(string)
		right, _ := out[j]["hour"].(string)
		return left < right
	})
	return out
}

func makeRow(deployment, kind string, p map[string]any) (Row, bool) {
	now := time.Now().UTC()
	switch kind {
	case eventFunctionCall:
		if isOCC, _ := p["is_occ"].(bool); !isOCC {
			return Row{}, false
		}
		return Row{
			Deployment:     deployment,
			TS:             now,
			Kind:           kindOCC,
			UDFID:          getString(p, "udf_id"),
			ComponentPath:  getStringPtr(p, "component_path"),
			RequestID:      getString(p, fieldRequestID),
			ExecutionID:    getString(p, "id"),
			OCCTableName:   getStringPtr(p, "occ_table_name"),
			OCCDocumentID:  getStringPtr(p, "occ_document_id"),
			OCCWriteSource: getStringPtr(p, "occ_write_source"),
			OCCRetryCount:  getInt(p, fieldOCCRetryCount),
			Status:         getStringPtr(p, "status"),
		}, true
	case eventInsightReadLimit:
		return Row{
			Deployment:    deployment,
			TS:            now,
			Kind:          kindRead,
			UDFID:         getString(p, "udf_id"),
			ComponentPath: getStringPtr(p, "component_path"),
			RequestID:     getString(p, fieldRequestID),
			ExecutionID:   getString(p, "id"),
			Success:       getBoolPtr(p, fieldSuccess),
			Calls:         getCalls(p, fieldCalls),
		}, true
	}
	return Row{}, false
}

func getString(m map[string]any, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}

func getStringPtr(m map[string]any, k string) *string {
	if v, ok := m[k].(string); ok && v != "" {
		s := v
		return &s
	}
	return nil
}

func getBoolPtr(m map[string]any, k string) *bool {
	if v, ok := m[k].(bool); ok {
		b := v
		return &b
	}
	return nil
}

func getInt(m map[string]any, k string) int {
	switch v := m[k].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return 0
}

func getCalls(m map[string]any, k string) []Call {
	v, ok := m[k]
	if !ok {
		return nil
	}
	switch xs := v.(type) {
	case []any:
		out := make([]Call, 0, len(xs))
		for _, x := range xs {
			obj, isObj := x.(map[string]any)
			if !isObj {
				continue
			}
			out = append(out, Call{
				TableName:     getString(obj, "table_name"),
				BytesRead:     getInt(obj, "bytes_read"),
				DocumentsRead: getInt(obj, "documents_read"),
			})
		}
		return out
	case []Call:
		return xs
	}
	return nil
}

func derefOr(p *string, def string) string {
	if p == nil {
		return def
	}
	return *p
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
