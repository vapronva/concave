package insights

import (
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"sort"
	"strconv"
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
	inclusiveEndHours          = 24
	occKeyPartsCount           = 3
	readKeyPartsCount          = 2
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
	fieldOCCFailedPermanently  = "occ_failed_permanently"
	fieldCalls                 = "calls"
	fieldSuccess               = "success"
)

var ErrBadDateRange = errors.New("from must be on or before to")

var dateRE = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

type Row struct {
	Deployment           string
	TS                   time.Time
	Kind                 string
	UDFID                string
	ComponentPath        *string
	RequestID            string
	ExecutionID          string
	OCCTableName         *string
	OCCDocumentID        *string
	OCCWriteSource       *string
	OCCRetryCount        int
	OCCFailedPermanently bool
	Success              *bool
	Calls                []Call
}

type Call struct {
	TableName     string `json:"table_name"`
	BytesRead     int    `json:"bytes_read"`
	DocumentsRead int    `json:"documents_read"`
}

type Insights struct {
	memMu sync.Mutex
	mem   map[string]*deploymentRows
	cap   int
}

type deploymentRows struct {
	rows  []Row
	start int
}

func New(ringCap int) *Insights {
	if ringCap <= 0 {
		ringCap = defaultRingCap
	}
	return &Insights{mem: make(map[string]*deploymentRows), cap: ringCap}
}

type AnyEvent map[string]map[string]any

func (i *Insights) Ingest(deployment string, events []AnyEvent) int {
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
	return kept
}

func (i *Insights) store(r Row) {
	i.memMu.Lock()
	defer i.memMu.Unlock()
	dep := i.mem[r.Deployment]
	if dep == nil {
		dep = &deploymentRows{}
		i.mem[r.Deployment] = dep
	}
	if len(dep.rows) < i.cap {
		dep.rows = append(dep.rows, r)
		return
	}
	dep.rows[dep.start] = r
	dep.start = (dep.start + 1) % len(dep.rows)
}

func (i *Insights) rows(deployment string, fromMs, toMs int64) []Row {
	i.memMu.Lock()
	defer i.memMu.Unlock()
	var out []Row
	dep := i.mem[deployment]
	if dep == nil {
		return nil
	}
	for n := range len(dep.rows) {
		r := dep.rows[(dep.start+n)%len(dep.rows)]
		ms := r.TS.UnixMilli()
		if ms >= fromMs && ms < toMs {
			out = append(out, r)
		}
	}
	return out
}

func (i *Insights) Query(deployment, fromDate, toDate string) ([][]any, error) {
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
	toMs := to.UTC().Add(inclusiveEndHours * time.Hour).UnixMilli()
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
	permanently := slices.ContainsFunc(grp, func(r Row) bool { return r.OCCFailedPermanently })
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
	if occTable != "" {
		body["occTableName"] = occTable
	}
	return []any{kind, udfID, comp, string(mustJSON(body))}
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
		maxBytes = max(maxBytes, bsum)
		maxDocs = max(maxDocs, dsum)
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
	now := eventTime(p)
	switch kind {
	case eventFunctionCall:
		if isOCC, _ := p["is_occ"].(bool); !isOCC {
			return Row{}, false
		}
		return Row{
			Deployment:           deployment,
			TS:                   now,
			Kind:                 kindOCC,
			UDFID:                getGroupableString(p, "udf_id"),
			ComponentPath:        getGroupableStringPtr(p, "component_path"),
			RequestID:            getString(p, fieldRequestID),
			ExecutionID:          getString(p, "id"),
			OCCTableName:         getStringPtr(p, "occ_table_name"),
			OCCDocumentID:        getStringPtr(p, "occ_document_id"),
			OCCWriteSource:       getStringPtr(p, "occ_write_source"),
			OCCRetryCount:        getInt(p, fieldOCCRetryCount),
			OCCFailedPermanently: getBool(p, fieldOCCFailedPermanently),
		}, true
	case eventInsightReadLimit:
		return Row{
			Deployment:    deployment,
			TS:            now,
			Kind:          kindRead,
			UDFID:         getGroupableString(p, "udf_id"),
			ComponentPath: getGroupableStringPtr(p, "component_path"),
			RequestID:     getString(p, fieldRequestID),
			ExecutionID:   getString(p, "id"),
			Success:       getBoolPtr(p, fieldSuccess),
			Calls:         getCalls(p, fieldCalls),
		}, true
	}
	return Row{}, false
}

func eventTime(p map[string]any) time.Time {
	switch v := p["timestamp"].(type) {
	case json.Number:
		if ms, err := v.Int64(); err == nil {
			return time.UnixMilli(ms).UTC()
		}
	case float64:
		return time.UnixMilli(int64(v)).UTC()
	}
	return time.Now().UTC()
}

func getString(m map[string]any, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}

func getGroupableString(m map[string]any, k string) string {
	return strings.ReplaceAll(getString(m, k), groupSep, "")
}

func getGroupableStringPtr(m map[string]any, k string) *string {
	v := strings.ReplaceAll(getString(m, k), groupSep, "")
	if v == "" {
		return nil
	}
	return &v
}

func getStringPtr(m map[string]any, k string) *string {
	if v, ok := m[k].(string); ok && v != "" {
		return &v
	}
	return nil
}

func getBoolPtr(m map[string]any, k string) *bool {
	if v, ok := m[k].(bool); ok {
		return &v
	}
	return nil
}

func getBool(m map[string]any, k string) bool {
	v, _ := m[k].(bool)
	return v
}

func getInt(m map[string]any, k string) int {
	switch v := m[k].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case json.Number:
		if n, err := strconv.ParseInt(v.String(), 10, 64); err == nil {
			return int(n)
		}
	}
	return 0
}

func getCalls(m map[string]any, k string) []Call {
	xs, ok := m[k].([]any)
	if !ok {
		return nil
	}
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
