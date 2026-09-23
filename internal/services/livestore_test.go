package services

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// nowIST returns an IST wall-clock "2006-01-02 15:04:05" string offset by
// deltaMinutes from now — the format Flink/ClickHouse emit for windowStart.
func nowIST(deltaMinutes int) string {
	return time.Now().In(istZoneLS).Add(time.Duration(deltaMinutes) * time.Minute).Format("2006-01-02 15:04:05")
}

func row(ruleID, groupKey, windowStart string, count float64, isFinal bool) map[string]interface{} {
	return map[string]interface{}{
		"ruleId":      ruleID,
		"groupKey":    groupKey,
		"windowStart": windowStart,
		"aggResult":   map[string]interface{}{"count": count},
		"isFinal":     isFinal,
	}
}

// newTestLiveStore builds a store with explicit limits, bypassing the
// env-driven config NewLiveStore reads (and its background pruner, which
// these tests drive directly instead).
func newTestLiveStore(maxRowsPerRule, hours int) *LiveStore {
	return &LiveStore{
		shards:         make(map[string]*ruleShard),
		maxRowsPerRule: maxRowsPerRule,
		hours:          hours,
		stopPruner:     make(chan struct{}),
	}
}

func countOf(t *testing.T, rows []map[string]interface{}, i int) float64 {
	t.Helper()
	return rows[i]["aggResult"].(map[string]interface{})["count"].(float64)
}

// TestLiveStore_UpsertReplacesInPlace verifies that a partial (isFinal:false)
// row followed by the final row for the same (ruleId, groupKey, windowStart)
// results in exactly one row holding the final value — not two.
func TestLiveStore_UpsertReplacesInPlace(t *testing.T) {
	s := newTestLiveStore(1000, 24)
	ws := nowIST(0)

	s.Add(row("R1", "g1", ws, 3, false)) // partial
	s.Add(row("R1", "g1", ws, 7, false)) // partial update
	s.Add(row("R1", "g1", ws, 9, true))  // final

	rows := s.GetAll([]string{"R1"})["R1"]
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after upserts, got %d", len(rows))
	}
	if got := countOf(t, rows, 0); got != 9 {
		t.Fatalf("expected final count 9, got %v", got)
	}
	if s.TotalRows() != 1 {
		t.Fatalf("expected totalRows 1, got %d", s.TotalRows())
	}
}

// TestLiveStore_FinalRowNeverRegressesToPartial covers the ordering backstop
// that makes concurrent processing safe: messages are now drained by a
// worker pool, so if a stale early-fire partial were ever applied after its
// window's final row, the chart would show a half-counted window as settled.
// That one direction of overwrite is refused.
func TestLiveStore_FinalRowNeverRegressesToPartial(t *testing.T) {
	s := newTestLiveStore(1000, 24)
	ws := nowIST(0)

	s.Add(row("R1", "g1", ws, 9, true))  // final arrives first
	s.Add(row("R1", "g1", ws, 4, false)) // stale partial arrives late

	rows := s.GetAll([]string{"R1"})["R1"]
	if got := countOf(t, rows, 0); got != 9 {
		t.Fatalf("a late partial overwrote the final row: expected 9, got %v", got)
	}

	// A corrected FINAL row must still replace an earlier final one.
	s.Add(row("R1", "g1", ws, 12, true))
	rows = s.GetAll([]string{"R1"})["R1"]
	if got := countOf(t, rows, 0); got != 12 {
		t.Fatalf("a later final row must replace an earlier one: expected 12, got %v", got)
	}
}

// TestLiveStore_DistinctWindowsAndGroupsAppend verifies distinct keys append.
func TestLiveStore_DistinctWindowsAndGroupsAppend(t *testing.T) {
	s := newTestLiveStore(1000, 24)
	s.Add(row("R1", "g1", nowIST(0), 1, true))
	s.Add(row("R1", "g2", nowIST(0), 1, true))  // different groupKey
	s.Add(row("R1", "g1", nowIST(-1), 1, true)) // different windowStart
	if s.TotalRows() != 3 {
		t.Fatalf("expected 3 distinct rows, got %d", s.TotalRows())
	}
}

// TestLiveStore_BusyRuleNeverEvictsAnotherRule is the regression test for the
// backend half of the "chart flashes to a different/smaller dataset" bug.
//
// The row cap used to be one global budget, and trimming evicted from
// whichever rule happened to hold the most rows. So a high-volume rule's
// traffic would silently shrink — or entirely empty — a completely different,
// quiet rule's snapshot; the next WebSocket bootstrap for that quiet rule
// then returned far less data than the client already had, which is exactly
// what the user saw as the chart snapping to a smaller dataset. Eviction is
// now strictly per rule.
func TestLiveStore_BusyRuleNeverEvictsAnotherRule(t *testing.T) {
	const capPerRule = 10
	s := newTestLiveStore(capPerRule, 24)

	// A quiet rule with a handful of rows.
	for i := 0; i < 3; i++ {
		s.Add(row("QUIET", "g1", fmt.Sprintf("2026-09-23 10:%02d:00", i), float64(i), true))
	}
	// A busy rule that blows way past the cap.
	for i := 0; i < capPerRule*50; i++ {
		s.Add(row("BUSY", "g1", fmt.Sprintf("2026-09-23 11:%02d:%02d", i/60, i%60), float64(i), true))
	}

	quiet := s.GetAll([]string{"QUIET"})["QUIET"]
	if len(quiet) != 3 {
		t.Fatalf("busy rule's traffic evicted the quiet rule's rows: expected 3, got %d", len(quiet))
	}
	for i := 0; i < 3; i++ {
		if got := countOf(t, quiet, i); got != float64(i) {
			t.Fatalf("quiet rule row %d was altered: expected count %d, got %v", i, i, got)
		}
	}

	busy := s.GetAll([]string{"BUSY"})["BUSY"]
	if len(busy) != capPerRule {
		t.Fatalf("busy rule should be capped at %d rows, got %d", capPerRule, len(busy))
	}
	// Eviction drops the OLDEST rows, so the survivors are the newest run.
	if got := countOf(t, busy, 0); got != float64(capPerRule*50-capPerRule) {
		t.Fatalf("expected the oldest surviving busy row to be %d, got %v", capPerRule*50-capPerRule, got)
	}
	if got := countOf(t, busy, capPerRule-1); got != float64(capPerRule*50-1) {
		t.Fatalf("expected the newest busy row to be %d, got %v", capPerRule*50-1, got)
	}
}

// TestLiveStore_IndexConsistentAfterTrim verifies the composite-key index
// stays correct after the ring buffer has wrapped and evicted, so a later
// upsert still updates in place rather than appending a duplicate.
func TestLiveStore_IndexConsistentAfterTrim(t *testing.T) {
	s := newTestLiveStore(3, 24)
	// 4 distinct windows on one rule → cap is 3 → oldest row evicted.
	for i := 0; i < 4; i++ {
		s.Add(row("R1", "g1", nowIST(-i), float64(i), true))
	}
	if s.TotalRows() != 3 {
		t.Fatalf("expected totalRows capped at 3, got %d", s.TotalRows())
	}
	// Now upsert the newest window (delta 0) — must update in place, not append.
	s.Add(row("R1", "g1", nowIST(0), 99, true))
	if s.TotalRows() != 3 {
		t.Fatalf("upsert after trim must not grow the store; got %d", s.TotalRows())
	}
	rows := s.GetAll([]string{"R1"})["R1"]
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Every index entry must point at a live sequence holding that same key.
	sh := s.shards["R1"]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if len(sh.index) != sh.count {
		t.Fatalf("index holds %d entries for %d live rows — stale entries leaked", len(sh.index), sh.count)
	}
	for ck, seq := range sh.index {
		if seq < sh.start || seq >= sh.start+int64(sh.count) {
			t.Fatalf("index entry %q points at evicted sequence %d (live range [%d,%d))", ck, seq, sh.start, sh.start+int64(sh.count))
		}
		if rowCompositeKey(sh.buf[sh.pos(seq)]) != ck {
			t.Fatalf("index entry %q points at a row with a different key", ck)
		}
	}
}

// TestLiveStore_RingBufferGrowsAndKeepsOrder verifies the ring buffer's
// doubling path preserves both chronological order and upsert-in-place,
// since growing renormalizes every sequence number.
func TestLiveStore_RingBufferGrowsAndKeepsOrder(t *testing.T) {
	s := newTestLiveStore(4096, 24)
	const n = initialRuleCap * 3 // forces at least two doublings
	for i := 0; i < n; i++ {
		s.Add(row("R1", "g1", fmt.Sprintf("w%05d", i), float64(i), true))
	}
	rows := s.GetAll([]string{"R1"})["R1"]
	if len(rows) != n {
		t.Fatalf("expected %d rows after growth, got %d", n, len(rows))
	}
	for i := 0; i < n; i++ {
		if got := countOf(t, rows, i); got != float64(i) {
			t.Fatalf("row order lost across buffer growth at %d: got %v", i, got)
		}
	}
	// An upsert of a row written before the last growth must still land in place.
	s.Add(row("R1", "g1", "w00000", 777, true))
	if s.TotalRows() != n {
		t.Fatalf("upsert after growth appended instead of replacing: %d rows", s.TotalRows())
	}
	rows = s.GetAll([]string{"R1"})["R1"]
	if got := countOf(t, rows, 0); got != 777 {
		t.Fatalf("expected the upserted value 777 at position 0, got %v", got)
	}
}

// TestLiveStore_PruneStaleKeepsIndexConsistent verifies pruneStale removes old
// rows and keeps the index valid so subsequent upserts still work.
func TestLiveStore_PruneStaleKeepsIndexConsistent(t *testing.T) {
	s := newTestLiveStore(1000, 1)
	// One row 2h old (stale, hours=1), one fresh.
	s.Add(row("R1", "g1", nowIST(-120), 1, true))
	s.Add(row("R1", "g1", nowIST(0), 2, true))
	s.pruneStale()

	rows := s.GetAll([]string{"R1"})["R1"]
	if len(rows) != 1 {
		t.Fatalf("expected 1 fresh row after prune, got %d", len(rows))
	}
	if s.TotalRows() != 1 {
		t.Fatalf("totalRows not decremented by prune: got %d", s.TotalRows())
	}
	// The surviving fresh row must still be upsertable in place.
	s.Add(row("R1", "g1", nowIST(0), 5, true))
	rows = s.GetAll([]string{"R1"})["R1"]
	if len(rows) != 1 {
		t.Fatalf("upsert after prune must not append; got %d rows", len(rows))
	}
	if got := countOf(t, rows, 0); got != 5 {
		t.Fatalf("expected upserted count 5, got %v", got)
	}
}

// TestLiveStore_PruneReleasesDrainedBuffer verifies a rule that fully ages
// out hands its grown ring buffer back instead of pinning it forever.
func TestLiveStore_PruneReleasesDrainedBuffer(t *testing.T) {
	s := newTestLiveStore(4096, 1)
	// Distinct windows (so they actually accumulate rather than upserting
	// onto one another) and all well past the 1h retention.
	for i := 0; i < initialRuleCap*2; i++ {
		s.Add(row("R1", "g1", nowIST(-120-i), float64(i), true))
	}
	s.pruneStale()
	if s.TotalRows() != 0 {
		t.Fatalf("expected every stale row pruned, got %d", s.TotalRows())
	}
	sh := s.shards["R1"]
	sh.mu.Lock()
	bufLen := len(sh.buf)
	sh.mu.Unlock()
	if bufLen != 0 {
		t.Fatalf("drained shard kept a %d-slot buffer instead of releasing it", bufLen)
	}
	// Must still accept new rows afterwards.
	s.Add(row("R1", "g1", nowIST(0), 1, true))
	if s.TotalRows() != 1 {
		t.Fatalf("shard unusable after its buffer was released: %d rows", s.TotalRows())
	}
}

// TestLiveStore_ConcurrentAddAcrossRules is the race-detector assertion for
// per-rule sharding: the store is written concurrently by the consumer's
// worker pool now, not by one goroutine. Run with `go test -race`.
func TestLiveStore_ConcurrentAddAcrossRules(t *testing.T) {
	s := newTestLiveStore(5000, 24)

	const writers = 8
	const perWriter = 500
	var writeWG, readWG sync.WaitGroup

	// Concurrent readers/pruner, mirroring bootstrap snapshots being served
	// and background pruning running while Kafka ingest is in flight.
	stop := make(chan struct{})
	readWG.Add(2)
	go func() {
		defer readWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.GetAll([]string{"R0", "R1", "R2", "R3"})
				s.TotalRows()
				s.Stats()
			}
		}
	}()
	go func() {
		defer readWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.pruneStale()
			}
		}
	}()

	for w := 0; w < writers; w++ {
		writeWG.Add(1)
		go func(w int) {
			defer writeWG.Done()
			ruleID := fmt.Sprintf("R%d", w%4)
			for i := 0; i < perWriter; i++ {
				s.Add(row(ruleID, fmt.Sprintf("g%d", w), fmt.Sprintf("w%05d", i), float64(i), true))
			}
		}(w)
	}

	writeWG.Wait()
	close(stop)
	readWG.Wait()

	// 8 writers x 500 distinct (groupKey, windowStart) pairs, none colliding.
	if got := s.TotalRows(); got != writers*perWriter {
		t.Fatalf("expected %d rows after concurrent ingest, got %d", writers*perWriter, got)
	}
}

// TestLiveStore_RemoveRule verifies a rule's memory can be released outright.
func TestLiveStore_RemoveRule(t *testing.T) {
	s := newTestLiveStore(1000, 24)
	s.Add(row("R1", "g1", nowIST(0), 1, true))
	s.Add(row("R2", "g1", nowIST(0), 1, true))

	s.RemoveRule("R1")

	if _, ok := s.GetAll([]string{"R1"})["R1"]; ok {
		t.Fatal("R1 still present after RemoveRule")
	}
	if len(s.GetAll([]string{"R2"})["R2"]) != 1 {
		t.Fatal("RemoveRule affected an unrelated rule")
	}
	if s.TotalRows() != 1 {
		t.Fatalf("expected totalRows 1 after removing one of two rules, got %d", s.TotalRows())
	}
}
