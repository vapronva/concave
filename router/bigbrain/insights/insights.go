package insights

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	rootComponent              = "-root-component-"
	maxRecentPerGroup          = 50
	inclusiveEndHours          = 24
	kindRead                   = "read"
	kindOCCRetried             = "occRetried"
	kindOCCFailedPermanent     = "occFailedPermanently"
	kindBytesReadLimit         = "bytesReadLimit"
	kindBytesReadThreshold     = "bytesReadThreshold"
	kindDocumentsReadLimit     = "documentsReadLimit"
	kindDocumentsReadThreshold = "documentsReadThreshold"
	fieldRequestID             = "request_id"
	fieldOCCRetryCount         = "occ_retry_count"
	fieldCalls                 = "calls"
	fieldSuccess               = "success"
)

var (
	ErrBadDate       = errors.New("dates must be YYYY-MM-DD")
	ErrBadDateRange  = errors.New("from must be on or before to")
	ErrBadReadLimits = errors.New("read_limits needs documents > 0, bytes > 0 and 0 < warning_ratio <= 1")
)

type Row struct {
	TS             time.Time
	Kind           string
	UDFID          string
	ComponentPath  string
	RequestID      string
	ExecutionID    string
	OCCTableName   string
	OCCDocumentID  string
	OCCWriteSource string
	OCCRetryCount  int
	Success        *bool
	Calls          []Call
}

type Call struct {
	TableName     string `json:"table_name"`
	BytesRead     int    `json:"bytes_read"`
	DocumentsRead int    `json:"documents_read"`
}

type ReadLimits struct {
	Documents    int     `json:"documents"`
	Bytes        int     `json:"bytes"`
	WarningRatio float64 `json:"warning_ratio"`
}

func (l ReadLimits) valid() bool {
	return l.Documents > 0 && l.Bytes > 0 && l.WarningRatio > 0 && l.WarningRatio <= 1
}

func (l ReadLimits) threshold(limit int) int {
	return int(float64(limit) * l.WarningRatio)
}

type Insights struct {
	memMu sync.Mutex
	mem   map[string]*deploymentRows
	cap   int
}

type deploymentRows struct {
	byKind map[string][]Row
	limits ReadLimits
}

func New(ringCap int) *Insights {
	return &Insights{mem: make(map[string]*deploymentRows), cap: ringCap}
}

type Event struct {
	FunctionCall     *functionCall     `json:"FunctionCall"`
	InsightReadLimit *insightReadLimit `json:"InsightReadLimit"`
}

type execution struct {
	UDFID         string `json:"udf_id"`
	ComponentPath string `json:"component_path"`
	RequestID     string `json:"request_id"`
	ID            string `json:"id"`
	Timestamp     *int64 `json:"timestamp"`
}

type functionCall struct {
	execution

	IsOCC                bool   `json:"is_occ"`
	OCCTableName         string `json:"occ_table_name"`
	OCCDocumentID        string `json:"occ_document_id"`
	OCCWriteSource       string `json:"occ_write_source"`
	OCCRetryCount        int    `json:"occ_retry_count"`
	OCCFailedPermanently bool   `json:"occ_failed_permanently"`
}

type insightReadLimit struct {
	execution

	Success *bool  `json:"success"`
	Calls   []Call `json:"calls"`
}

func (i *Insights) Ingest(deployment string, limits ReadLimits, events []Event) (int, error) {
	if !limits.valid() {
		return 0, ErrBadReadLimits
	}
	rows := makeRows(events)
	i.memMu.Lock()
	defer i.memMu.Unlock()
	dep := i.mem[deployment]
	if dep == nil {
		dep = &deploymentRows{byKind: make(map[string][]Row)}
		i.mem[deployment] = dep
	}
	dep.limits = limits
	for _, r := range rows {
		dep.push(r, i.cap)
	}
	return len(rows), nil
}

func makeRows(events []Event) []Row {
	var rows []Row
	for _, ev := range events {
		if fc := ev.FunctionCall; fc != nil && fc.IsOCC {
			kind := kindOCCRetried
			if fc.OCCFailedPermanently {
				kind = kindOCCFailedPermanent
			}
			r := fc.row(kind)
			r.OCCTableName = fc.OCCTableName
			r.OCCDocumentID = fc.OCCDocumentID
			r.OCCWriteSource = fc.OCCWriteSource
			r.OCCRetryCount = fc.OCCRetryCount
			rows = append(rows, r)
		}
		if rl := ev.InsightReadLimit; rl != nil {
			r := rl.row(kindRead)
			r.Success = rl.Success
			r.Calls = rl.Calls
			rows = append(rows, r)
		}
	}
	return rows
}

func (e execution) row(kind string) Row {
	ts := time.Now().UTC()
	if e.Timestamp != nil {
		ts = time.UnixMilli(*e.Timestamp).UTC()
	}
	return Row{
		TS:            ts,
		Kind:          kind,
		UDFID:         e.UDFID,
		ComponentPath: e.ComponentPath,
		RequestID:     e.RequestID,
		ExecutionID:   e.ID,
	}
}

func (d *deploymentRows) len() int {
	n := 0
	for _, rows := range d.byKind {
		n += len(rows)
	}
	return n
}

func (d *deploymentRows) push(r Row, ringCap int) {
	if d.len() >= ringCap {
		evict := r.Kind
		for kind, rows := range d.byKind {
			if len(rows) > len(d.byKind[evict]) {
				evict = kind
			}
		}
		rows := d.byKind[evict]
		rows[0] = Row{}
		d.byKind[evict] = rows[1:]
	}
	d.byKind[r.Kind] = append(d.byKind[r.Kind], r)
}

func (i *Insights) rows(deployment string, fromMs, toMs int64) ([]Row, ReadLimits) {
	i.memMu.Lock()
	defer i.memMu.Unlock()
	dep := i.mem[deployment]
	if dep == nil {
		return nil, ReadLimits{}
	}
	out := make([]Row, 0, dep.len())
	for _, rows := range dep.byKind {
		for _, r := range rows {
			if ms := r.TS.UnixMilli(); ms >= fromMs && ms < toMs {
				out = append(out, r)
			}
		}
	}
	return out, dep.limits
}

func (i *Insights) Query(deployment, fromDate, toDate string) ([][]any, error) {
	from, err := time.Parse("2006-01-02", fromDate)
	if err != nil {
		return nil, ErrBadDate
	}
	to, err := time.Parse("2006-01-02", toDate)
	if err != nil {
		return nil, ErrBadDate
	}
	fromMs := from.UTC().UnixMilli()
	toMs := to.UTC().Add(inclusiveEndHours * time.Hour).UnixMilli()
	if fromMs >= toMs {
		return nil, ErrBadDateRange
	}
	rows, limits := i.rows(deployment, fromMs, toMs)
	out := append(aggregateOCC(rows), aggregateRead(rows, limits)...)
	sort.Slice(out, func(a, b int) bool { return rowLess(out[a], out[b]) })
	return out, nil
}

func rowLess(a, b []any) bool {
	for i := range min(len(a), len(b)) {
		as, bs := fmt.Sprint(a[i]), fmt.Sprint(b[i])
		if as != bs {
			return as < bs
		}
	}
	return len(a) < len(b)
}

func aggregateOCC(rows []Row) [][]any {
	groups := groupByOCC(rows)
	out := make([][]any, 0, len(groups))
	for key, grp := range groups {
		out = append(out, buildOCCRow(key.udfID, key.comp, key.table, key.kind, grp))
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
		if r.OCCDocumentID != "" {
			rec["occ_document_id"] = r.OCCDocumentID
		}
		if r.OCCWriteSource != "" {
			rec["occ_write_source"] = r.OCCWriteSource
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

type occKey struct {
	udfID, comp, table, kind string
}

func groupByOCC(rows []Row) map[occKey][]Row {
	g := make(map[occKey][]Row)
	for _, r := range rows {
		if r.Kind == kindRead {
			continue
		}
		key := occKey{r.UDFID, cmp.Or(r.ComponentPath, rootComponent), r.OCCTableName, r.Kind}
		g[key] = append(g[key], r)
	}
	return g
}

func aggregateRead(rows []Row, limits ReadLimits) [][]any {
	groups := groupByRead(rows)
	out := make([][]any, 0)
	for key, grp := range groups {
		udfID, comp := key.udfID, key.comp
		bytes := readDimension{
			limit:         limits.Bytes,
			threshold:     limits.threshold(limits.Bytes),
			measure:       bytesRead,
			kindLimit:     kindBytesReadLimit,
			kindThreshold: kindBytesReadThreshold,
		}
		docs := readDimension{
			limit:         limits.Documents,
			threshold:     limits.threshold(limits.Documents),
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
	threshold     int
	measure       func(Row) int
	kindLimit     string
	kindThreshold string
}

func (d readDimension) row(udfID, comp string, grp []Row) ([]any, bool) {
	var rows []Row
	peak := 0
	for _, r := range grp {
		v := d.measure(r)
		if v < d.threshold {
			continue
		}
		rows = append(rows, r)
		peak = max(peak, v)
	}
	if len(rows) == 0 {
		return nil, false
	}
	kind := d.kindThreshold
	if peak > d.limit {
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
		"limit":        d.limit,
		"threshold":    d.threshold,
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

type readKey struct {
	udfID, comp string
}

func groupByRead(rows []Row) map[readKey][]Row {
	g := make(map[readKey][]Row)
	for _, r := range rows {
		if r.Kind != kindRead {
			continue
		}
		key := readKey{r.UDFID, cmp.Or(r.ComponentPath, rootComponent)}
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

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
