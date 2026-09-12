package insights

import (
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultDocumentsReadLimit  = 32000
	defaultBytesReadLimit      = 16 * 1024 * 1024
	rootComponent              = "-root-component-"
	groupSep                   = "\x1f"
	maxRecentPerGroup          = 50
	inclusiveEndHours          = 24
	occKeyPartsCount           = 4
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

type Row struct {
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

type ReadLimits struct {
	Documents int `json:"documents"`
	Bytes     int `json:"bytes"`
}

func DefaultReadLimits() ReadLimits {
	return ReadLimits{Documents: defaultDocumentsReadLimit, Bytes: defaultBytesReadLimit}
}

type Insights struct {
	memMu sync.Mutex
	mem   map[string]*deploymentRows
	cap   int
}

type deploymentRows struct {
	rows   []Row
	start  int
	limits ReadLimits
}

func New(ringCap int) *Insights {
	return &Insights{mem: make(map[string]*deploymentRows), cap: ringCap}
}

type AnyEvent map[string]map[string]any

func (i *Insights) Ingest(deployment string, limits ReadLimits, events []AnyEvent) int {
	if limits.Documents <= 0 || limits.Bytes <= 0 {
		limits = DefaultReadLimits()
	}
	dep := i.deployment(deployment, limits)
	kept := 0
	for _, ev := range events {
		for k, payload := range ev {
			row, ok := makeRow(k, payload)
			if !ok {
				continue
			}
			i.store(dep, row)
			kept++
		}
	}
	return kept
}

func (i *Insights) deployment(name string, limits ReadLimits) *deploymentRows {
	i.memMu.Lock()
	defer i.memMu.Unlock()
	dep := i.mem[name]
	if dep == nil {
		dep = &deploymentRows{}
		i.mem[name] = dep
	}
	dep.limits = limits
	return dep
}

func (i *Insights) store(dep *deploymentRows, r Row) {
	i.memMu.Lock()
	defer i.memMu.Unlock()
	if len(dep.rows) < i.cap {
		dep.rows = append(dep.rows, r)
		return
	}
	dep.rows[dep.start] = r
	dep.start = (dep.start + 1) % len(dep.rows)
}

func (i *Insights) rows(deployment string, fromMs, toMs int64) ([]Row, ReadLimits) {
	i.memMu.Lock()
	defer i.memMu.Unlock()
	var out []Row
	dep := i.mem[deployment]
	if dep == nil {
		return nil, DefaultReadLimits()
	}
	for n := range len(dep.rows) {
		r := dep.rows[(dep.start+n)%len(dep.rows)]
		ms := r.TS.UnixMilli()
		if ms >= fromMs && ms < toMs {
			out = append(out, r)
		}
	}
	return out, dep.limits
}

func (i *Insights) Query(deployment, fromDate, toDate string) ([][]any, error) {
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
	rows, limits := i.rows(deployment, fromMs, toMs)
	out := aggregateOCC(rows)
	return append(out, aggregateRead(rows, limits)...), nil
}

func aggregateOCC(rows []Row) [][]any {
	groups := groupByOCC(rows)
	out := make([][]any, 0, len(groups))
	for key, grp := range groups {
		parts := strings.SplitN(key, groupSep, occKeyPartsCount)
		udfID, comp, occTable, kind := parts[0], parts[1], parts[2], parts[3]
		out = append(out, buildOCCRow(udfID, comp, occTable, kind, grp))
	}
	return out
}

func buildOCCRow(udfID, comp, occTable, kind string, grp []Row) []any {
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
		kind := kindOCCRetried
		if r.OCCFailedPermanently {
			kind = kindOCCFailedPermanent
		}
		key := strings.Join([]string{r.UDFID, comp, occTbl, kind}, groupSep)
		g[key] = append(g[key], r)
	}
	return g
}

func aggregateRead(rows []Row, limits ReadLimits) [][]any {
	groups := groupByRead(rows)
	out := make([][]any, 0)
	for key, grp := range groups {
		parts := strings.SplitN(key, groupSep, readKeyPartsCount)
		udfID, comp := parts[0], parts[1]
		bytes := readDimension{
			limit:         limits.Bytes,
			measure:       bytesRead,
			kindLimit:     kindBytesReadLimit,
			kindThreshold: kindBytesReadThreshold,
		}
		docs := readDimension{
			limit:         limits.Documents,
			measure:       documentsRead,
			kindLimit:     kindDocumentsReadLimit,
			kindThreshold: kindDocumentsReadThreshold,
		}
		for _, dim := range []readDimension{bytes, docs} {
			if row, ok := dim.row(udfID, comp, grp); ok {
				out = append(out, row)
			}
		}
	}
	return out
}

type readDimension struct {
	limit         int
	measure       func(Row) int
	kindLimit     string
	kindThreshold string
}

func (d readDimension) row(udfID, comp string, grp []Row) ([]any, bool) {
	var rows []Row
	peak := 0
	for _, r := range grp {
		v := d.measure(r)
		if v < d.limit*4/5 {
			continue
		}
		rows = append(rows, r)
		peak = max(peak, v)
	}
	if len(rows) == 0 {
		return nil, false
	}
	kind := d.kindThreshold
	if peak >= d.limit {
		kind = d.kindLimit
	}
	count := len(rows)
	hourly := bucket(timestampsOf(rows))
	sort.Slice(rows, func(i, j int) bool { return rows[i].TS.After(rows[j].TS) })
	if len(rows) > maxRecentPerGroup {
		rows = rows[:maxRecentPerGroup]
	}
	body := map[string]any{
		"count":        count,
		"hourlyCounts": hourly,
		"recentEvents": readRecent(rows),
	}
	return []any{kind, udfID, comp, string(mustJSON(body))}, true
}

func bytesRead(r Row) int {
	sum := 0
	for _, c := range r.Calls {
		sum += c.BytesRead
	}
	return sum
}

func documentsRead(r Row) int {
	sum := 0
	for _, c := range r.Calls {
		sum += c.DocumentsRead
	}
	return sum
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

func bucket(times []time.Time) []hourlyCount {
	counts := make(map[string]int)
	for _, t := range times {
		counts[t.UTC().Format("2006-01-02 15:00:00")]++
	}
	out := make([]hourlyCount, 0, len(counts))
	for hour, count := range counts {
		out = append(out, hourlyCount{Hour: hour, Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hour < out[j].Hour })
	return out
}

type hourlyCount struct {
	Hour  string `json:"hour"`
	Count int    `json:"count"`
}

func makeRow(kind string, p map[string]any) (Row, bool) {
	now := eventTime(p)
	switch kind {
	case eventFunctionCall:
		if isOCC, _ := p["is_occ"].(bool); !isOCC {
			return Row{}, false
		}
		return Row{
			TS:                   now,
			Kind:                 kindOCC,
			UDFID:                getString(p, "udf_id"),
			ComponentPath:        getStringPtr(p, "component_path"),
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

func eventTime(p map[string]any) time.Time {
	if v, ok := p["timestamp"].(json.Number); ok {
		if ms, err := v.Int64(); err == nil {
			return time.UnixMilli(ms).UTC()
		}
	}
	return time.Now().UTC()
}

func getString(m map[string]any, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
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
	if v, ok := m[k].(json.Number); ok {
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
