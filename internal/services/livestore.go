package services

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
)

// istZoneLS is the IST fixed timezone offset (UTC+05:30) for LiveStore timestamp parsing.
var istZoneLS = time.FixedZone("IST", 5*60*60+30*60)

// parseWindowStart attempts to parse a windowStart value (string or int64) into time.Time.
// Primary format is "2006-01-02 15:04:05" (IST, space-separated, no ms, no offset) — the
// naive format Flink's TimeUtils.IST_FMT emits for every AggregationResult, and what
// ClickHouse's bootstrap query also returns for the same underlying data. The ISO-8601
// fallback below is kept for older/alternate producers, not the current Flink output.
func parseWindowStart(v interface{}) (time.Time, bool) {
	switch val := v.(type) {
	case string:
		if val == "" {
			return time.Time{}, false
		}
		if t, err := time.ParseInLocation("2006-01-02 15:04:05", val, istZoneLS); err == nil {
			return t, true
		}
		// ISO-8601 with timezone: "2006-01-02T15:04:05+05:30" or "2006-01-02T15:04:05Z"
		for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05"} {
			if t, err := time.Parse(layout, val); err == nil {
				return t, true
			}
		}
		return time.Time{}, false
	case int64:
		// Epoch milliseconds (Flink may emit this)
		return time.Unix(0, val*int64(time.Millisecond)), true
	case float64:
		return time.Unix(0, int64(val)*int64(time.Millisecond)), true
	default:
		return time.Time{}, false
	}
}

// rowCompositeKey builds the per-rule dedup identity for a row:
// groupKey + windowStart. Uses a null-byte separator so a groupKey containing
// a space or other printable char can't collide with a windowStart boundary.
func rowCompositeKey(row map[string]interface{}) string {
	groupKey, _ := row["groupKey"].(string)
	return groupKey + "\x00" + fmt.Sprintf("%v", row["windowStart"])
}

// isFinalRow reports whether a row is Flink's authoritative end-of-window
// emission rather than an early-fire partial preview. Consumers normalize a
// missing isFinal to true (rows predating the field are always settled), so
// only an explicit false is treated as partial.
func isFinalRow(row map[string]interface{}) bool {
	switch v := row["isFinal"].(type) {
	case bool:
		return v
	case float64:
		return v != 0
	case nil:
		return true
	default:
		return true
	}
}

// shouldReplace decides whether a newly-arrived row may overwrite the row
// already held for the same (ruleId, groupKey, windowStart).
//
// This is the ordering backstop for concurrent processing. Messages are now
// drained by a worker pool (see consumer.go), dispatched key-affine so two
// updates for the same window normally land on the same worker in arrival
// order — but that relies on the producer's partition key, which this
// backend does not control. If a stale early-fire partial ever did get
// applied after the final row for its window, the chart would show a
// half-counted window as though it were settled. Refusing that one
// direction of overwrite makes the out-of-order case harmless rather than
// wrong: final never regresses to partial; partial→partial and
// partial→final still update normally, and a corrected final row still
// replaces an earlier final one.
func shouldReplace(existing, incoming map[string]interface{}) bool {
	if existing == nil {
		return true
	}
	return !(isFinalRow(existing) && !isFinalRow(incoming))
}

// initialRuleCap is the ring buffer size a rule's shard starts at; it doubles
// on demand up to the configured per-rule cap. Most rules hold far fewer rows
// than the cap allows, so allocating the full cap up front for every rule that
// has ever emitted a single row would waste most of it.
const initialRuleCap = 256

// ruleShard holds one rule's rows behind its own lock.
//
// Rows live in a ring buffer rather than a plain slice so that enforcing the
// row cap is O(1): the oldest row is overwritten in place instead of
// reallocating and copying the whole rule's backing array on every eviction,
// which the previous implementation did while holding a store-wide write lock.
//
// index maps a row's composite key to its ABSOLUTE sequence number, not to a
// slice position. Absolute sequence numbers are what make front-eviction free:
// dropping the oldest row just advances start, and every surviving row's
// recorded sequence stays valid, so there is no index rebuild.
type ruleShard struct {
	mu      sync.Mutex
	buf     []map[string]interface{} // ring buffer; len(buf) is the current capacity
	start   int64                    // absolute sequence number of the oldest live row
	count   int                      // number of live rows
	index   map[string]int64         // composite key -> absolute sequence number
	maxRows int
}

func newRuleShard(maxRows int) *ruleShard {
	if maxRows < 1 {
		maxRows = 1
	}
	return &ruleShard{index: make(map[string]int64), maxRows: maxRows}
}

// pos maps an absolute sequence number to its slot in the ring buffer.
func (sh *ruleShard) pos(seq int64) int { return int(seq % int64(len(sh.buf))) }

// grow doubles the ring buffer (bounded by maxRows), re-laying rows out in
// logical order. Sequence numbers are renormalized to start at 0 because
// pos() is modulo the capacity, so an existing sequence means a different
// slot once the capacity changes. Amortized O(1) per row across doublings.
// Must be called with sh.mu held.
func (sh *ruleShard) grow() {
	newCap := len(sh.buf) * 2
	if newCap == 0 {
		newCap = initialRuleCap
	}
	if newCap > sh.maxRows {
		newCap = sh.maxRows
	}
	if newCap <= len(sh.buf) {
		return
	}
	nb := make([]map[string]interface{}, newCap)
	for i := 0; i < sh.count; i++ {
		nb[i] = sh.buf[sh.pos(sh.start+int64(i))]
	}
	sh.buf = nb
	sh.start = 0
	for k := range sh.index {
		delete(sh.index, k)
	}
	for i := 0; i < sh.count; i++ {
		sh.index[rowCompositeKey(nb[i])] = int64(i)
	}
}

// add upserts a row. Returns (added, evicted): added is 1 when this grew the
// live row count, evicted is 1 when the cap forced the oldest row out.
// Both are 0 for an in-place update of an existing window.
func (sh *ruleShard) add(row map[string]interface{}) (added int, evicted int) {
	ck := rowCompositeKey(row)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if seq, ok := sh.index[ck]; ok && seq >= sh.start && seq < sh.start+int64(sh.count) {
		p := sh.pos(seq)
		if shouldReplace(sh.buf[p], row) {
			sh.buf[p] = row
		}
		return 0, 0
	}

	if sh.count == len(sh.buf) {
		if sh.count < sh.maxRows {
			sh.grow()
		} else {
			// At the per-rule cap: drop this rule's own oldest row. Crucially,
			// this can never touch any other rule's data — the old global
			// trimExcess evicted from whichever rule held the most rows, which
			// is what let a busy rule silently empty a quiet rule's snapshot.
			oldest := sh.buf[sh.pos(sh.start)]
			if oldest != nil {
				ok := rowCompositeKey(oldest)
				if seq, present := sh.index[ok]; present && seq == sh.start {
					delete(sh.index, ok)
				}
			}
			sh.buf[sh.pos(sh.start)] = nil
			sh.start++
			sh.count--
			evicted = 1
		}
	}

	seq := sh.start + int64(sh.count)
	sh.buf[sh.pos(seq)] = row
	sh.index[ck] = seq
	sh.count++
	return 1, evicted
}

// pruneOlderThan drops the leading run of rows whose windowStart is at or
// before cutoff, and reports how many it dropped. Like the implementation it
// replaces, this relies on rows being appended in roughly chronological order
// and stops at the first row that is new enough (or whose windowStart can't be
// parsed).
func (sh *ruleShard) pruneOlderThan(cutoff time.Time) int {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	removed := 0
	for removed < sh.count {
		p := sh.pos(sh.start + int64(removed))
		row := sh.buf[p]
		if row == nil {
			break
		}
		t, ok := parseWindowStart(row["windowStart"])
		if !ok || t.After(cutoff) {
			break
		}
		removed++
	}
	if removed == 0 {
		return 0
	}
	for i := 0; i < removed; i++ {
		seq := sh.start + int64(i)
		p := sh.pos(seq)
		if row := sh.buf[p]; row != nil {
			ck := rowCompositeKey(row)
			if cur, present := sh.index[ck]; present && cur == seq {
				delete(sh.index, ck)
			}
		}
		sh.buf[p] = nil
	}
	sh.start += int64(removed)
	sh.count -= removed

	// Fully drained: hand the ring buffer back so an idle rule doesn't pin a
	// grown-out buffer forever. It re-grows lazily on the next add.
	if sh.count == 0 && len(sh.buf) > initialRuleCap {
		sh.buf = nil
		sh.start = 0
		sh.index = make(map[string]int64)
	}
	return removed
}

// snapshot returns a copy of this rule's rows in chronological (insertion)
// order. The rows themselves are shared, not deep-copied — callers treat them
// as read-only, exactly as the previous implementation's slice copy did.
func (sh *ruleShard) snapshot() []map[string]interface{} {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.count == 0 {
		return nil
	}
	out := make([]map[string]interface{}, sh.count)
	for i := 0; i < sh.count; i++ {
		out[i] = sh.buf[sh.pos(sh.start+int64(i))]
	}
	return out
}

func (sh *ruleShard) len() int {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.count
}

// LiveStore is a thread-safe in-memory rolling store for live rule results,
// sharded per rule.
//
// The store-wide RWMutex now guards only the rule -> shard map, which changes
// just once per newly-seen rule. All row reads and writes take that rule's own
// shard lock instead, so traffic on one rule no longer serializes against
// traffic on any other. Previously a single global write lock was taken on
// every single message across all rules — on a ~1,000+ msg/sec ingest that
// made the store the hot path's bottleneck no matter how many cores the pod
// had.
type LiveStore struct {
	mu     sync.RWMutex
	shards map[string]*ruleShard

	// totalRows/droppedRows are aggregated atomically rather than derived by
	// walking every shard, so the Prometheus collectors (which scrape these
	// live) never contend with ingestion. droppedRows must stay monotonic:
	// it backs a CounterFunc, so it is kept here on the store rather than in
	// shards, which can be emptied.
	totalRows   atomic.Int64
	droppedRows atomic.Int64

	maxRowsPerRule int
	hours          int

	stopPruner chan struct{}
	stopOnce   sync.Once
}

// NewLiveStore creates a new LiveStore with configured limits.
func NewLiveStore() *LiveStore {
	ls := &LiveStore{
		shards:         make(map[string]*ruleShard),
		maxRowsPerRule: config.LiveStoreMaxRowsPerRule,
		hours:          config.LiveStoreHours,
		stopPruner:     make(chan struct{}),
	}
	// Prune stale data on a background goroutine every 30 seconds rather than
	// on every Add, so time-based eviction never runs on the ingest path.
	go ls.runPruner()
	return ls
}

// shardFor returns the shard for ruleID, creating it if needed.
func (s *LiveStore) shardFor(ruleID string) *ruleShard {
	s.mu.RLock()
	sh := s.shards[ruleID]
	s.mu.RUnlock()
	if sh != nil {
		return sh
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if sh = s.shards[ruleID]; sh != nil {
		return sh // another goroutine created it while we upgraded the lock
	}
	sh = newRuleShard(s.maxRowsPerRule)
	s.shards[ruleID] = sh
	return sh
}

// runPruner is a background goroutine that periodically evicts stale rows.
func (s *LiveStore) runPruner() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.pruneStale()
		case <-s.stopPruner:
			return
		}
	}
}

// StopPruner stops the background pruning goroutine. Safe to call more than once.
func (s *LiveStore) StopPruner() {
	s.stopOnce.Do(func() { close(s.stopPruner) })
}

// Add appends or updates a single result row. Rows are upserted by
// (ruleId, groupKey, windowStart): a Flink early-fire (partial) row and the
// eventual final row for the same window share that key, so later ticks
// replace the prior entry in place instead of piling up one row per partial
// tick.
//
// Safe for concurrent use by the consumer's worker pool: it takes only the
// target rule's shard lock, and only for the duration of an O(1) ring-buffer
// write.
func (s *LiveStore) Add(row map[string]interface{}) {
	ruleID, _ := row["ruleId"].(string)
	if ruleID == "" {
		ruleID = "unknown"
	}
	added, evicted := s.shardFor(ruleID).add(row)
	if added > 0 {
		s.totalRows.Add(int64(added))
	}
	if evicted > 0 {
		s.totalRows.Add(-int64(evicted))
		s.droppedRows.Add(int64(evicted))
	}
}

// Bootstrap replaces all data with the given rows (used at startup).
// Bootstrap rows come from ClickHouse ... FINAL (already deduped by window), so
// they're appended directly.
func (s *LiveStore) Bootstrap(rows []map[string]interface{}) {
	fresh := make(map[string]*ruleShard)
	var total int64
	for _, row := range rows {
		ruleID, _ := row["ruleId"].(string)
		if ruleID == "" {
			ruleID = "unknown"
		}
		sh := fresh[ruleID]
		if sh == nil {
			sh = newRuleShard(s.maxRowsPerRule)
			fresh[ruleID] = sh
		}
		added, evicted := sh.add(row)
		total += int64(added - evicted)
	}

	s.mu.Lock()
	s.shards = fresh
	s.mu.Unlock()
	s.totalRows.Store(total)

	slog.Info("LiveStore bootstrapped", "total_rows", total, "rule_count", len(fresh))
}

// GetAll returns rows for the specified rule IDs.
func (s *LiveStore) GetAll(ruleIDs []string) map[string][]map[string]interface{} {
	s.mu.RLock()
	targets := make([]*ruleShard, 0, len(ruleIDs))
	names := make([]string, 0, len(ruleIDs))
	for _, rid := range ruleIDs {
		if sh, ok := s.shards[rid]; ok {
			targets = append(targets, sh)
			names = append(names, rid)
		}
	}
	s.mu.RUnlock()

	result := make(map[string][]map[string]interface{}, len(targets))
	for i, sh := range targets {
		if rows := sh.snapshot(); len(rows) > 0 {
			result[names[i]] = rows
		}
	}
	return result
}

// RemoveRule drops everything held for one rule. Not called on the live path
// today; it exists so a rule's memory can be released without waiting for the
// time-based prune to walk it out row by row.
func (s *LiveStore) RemoveRule(ruleID string) {
	s.mu.Lock()
	sh, ok := s.shards[ruleID]
	delete(s.shards, ruleID)
	s.mu.Unlock()
	if ok {
		s.totalRows.Add(-int64(sh.len()))
	}
}

// Stats returns current store statistics including dropped-rows counter.
func (s *LiveStore) Stats() map[string]interface{} {
	s.mu.RLock()
	shards := make([]*ruleShard, 0, len(s.shards))
	for _, sh := range s.shards {
		shards = append(shards, sh)
	}
	s.mu.RUnlock()

	active := 0
	for _, sh := range shards {
		if sh.len() > 0 {
			active++
		}
	}
	return map[string]interface{}{
		"total_rows":   s.totalRows.Load(),
		"rule_count":   active,
		"dropped_rows": s.droppedRows.Load(),
	}
}

// pruneStale removes rows older than s.hours from every rule. Each rule is
// pruned under its own lock, so this never blocks ingestion for the whole
// store at once.
func (s *LiveStore) pruneStale() {
	cutoff := time.Now().Add(-time.Duration(s.hours) * time.Hour)

	s.mu.RLock()
	shards := make([]*ruleShard, 0, len(s.shards))
	for _, sh := range s.shards {
		shards = append(shards, sh)
	}
	s.mu.RUnlock()

	var removed int
	for _, sh := range shards {
		removed += sh.pruneOlderThan(cutoff)
	}
	if removed > 0 {
		s.totalRows.Add(-int64(removed))
	}
}

// TotalRows returns the current row count across all rules — used by the
// livestore_rows metric (see internal/metrics.RegisterLiveStoreCollectors).
func (s *LiveStore) TotalRows() int {
	return int(s.totalRows.Load())
}

// DroppedRowsTotal returns the cumulative count of rows evicted due to the
// per-rule maxRows cap — used by the livestore_evictions_total metric (a
// CounterFunc, since droppedRows only ever increases).
func (s *LiveStore) DroppedRowsTotal() float64 {
	return float64(s.droppedRows.Load())
}

// StatsString returns a formatted stats string for logging.
func (s *LiveStore) StatsString() string {
	stats := s.Stats()
	return fmt.Sprintf("total_rows=%v, rule_count=%v", stats["total_rows"], stats["rule_count"])
}
