package services

import (
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

// TestLiveStore_UpsertReplacesInPlace verifies that a partial (isFinal:false)
// row followed by the final row for the same (ruleId, groupKey, windowStart)
// results in exactly one row holding the final value — not two.
func TestLiveStore_UpsertReplacesInPlace(t *testing.T) {
	s := &LiveStore{data: map[string][]map[string]interface{}{}, index: map[string]map[string]int{}, maxRows: 1000, hours: 24}
	ws := nowIST(0)

	s.Add(row("R1", "g1", ws, 3, false)) // partial
	s.Add(row("R1", "g1", ws, 7, false)) // partial update
	s.Add(row("R1", "g1", ws, 9, true))  // final

	rows := s.GetAll([]string{"R1"})["R1"]
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after upserts, got %d", len(rows))
	}
	if got := rows[0]["aggResult"].(map[string]interface{})["count"].(float64); got != 9 {
		t.Fatalf("expected final count 9, got %v", got)
	}
	if s.totalRows != 1 {
		t.Fatalf("expected totalRows 1, got %d", s.totalRows)
	}
}

// TestLiveStore_DistinctWindowsAndGroupsAppend verifies distinct keys append.
func TestLiveStore_DistinctWindowsAndGroupsAppend(t *testing.T) {
	s := &LiveStore{data: map[string][]map[string]interface{}{}, index: map[string]map[string]int{}, maxRows: 1000, hours: 24}
	s.Add(row("R1", "g1", nowIST(0), 1, true))
	s.Add(row("R1", "g2", nowIST(0), 1, true)) // different groupKey
	s.Add(row("R1", "g1", nowIST(-1), 1, true)) // different windowStart
	if s.totalRows != 3 {
		t.Fatalf("expected 3 distinct rows, got %d", s.totalRows)
	}
}

// TestLiveStore_IndexConsistentAfterTrim verifies the index stays correct after
// trimExcess shifts positions, so a later upsert still updates in place.
func TestLiveStore_IndexConsistentAfterTrim(t *testing.T) {
	s := &LiveStore{data: map[string][]map[string]interface{}{}, index: map[string]map[string]int{}, maxRows: 3, hours: 24}
	// 4 distinct windows on one rule → cap is 3 → oldest front row trimmed.
	for i := 0; i < 4; i++ {
		s.Add(row("R1", "g1", nowIST(-i), float64(i), true))
	}
	if s.totalRows != 3 {
		t.Fatalf("expected totalRows capped at 3, got %d", s.totalRows)
	}
	// Now upsert the newest window (delta 0) — must update in place, not append.
	s.Add(row("R1", "g1", nowIST(0), 99, true))
	if s.totalRows != 3 {
		t.Fatalf("upsert after trim must not grow the store; got %d", s.totalRows)
	}
	// Verify the index still points at valid positions for every surviving row.
	for ck, pos := range s.index["R1"] {
		if pos < 0 || pos >= len(s.data["R1"]) {
			t.Fatalf("index entry %q points at out-of-range pos %d (len %d)", ck, pos, len(s.data["R1"]))
		}
		if rowCompositeKey(s.data["R1"][pos]) != ck {
			t.Fatalf("index entry %q points at a row with a different key", ck)
		}
	}
}

// TestLiveStore_PruneStaleKeepsIndexConsistent verifies pruneStale removes old
// rows and keeps the index valid so subsequent upserts still work.
func TestLiveStore_PruneStaleKeepsIndexConsistent(t *testing.T) {
	s := &LiveStore{data: map[string][]map[string]interface{}{}, index: map[string]map[string]int{}, maxRows: 1000, hours: 1}
	// One row 2h old (stale, hours=1), one fresh.
	s.Add(row("R1", "g1", nowIST(-120), 1, true))
	s.Add(row("R1", "g1", nowIST(0), 2, true))
	s.mu.Lock()
	s.pruneStale()
	s.mu.Unlock()
	rows := s.GetAll([]string{"R1"})["R1"]
	if len(rows) != 1 {
		t.Fatalf("expected 1 fresh row after prune, got %d", len(rows))
	}
	// The surviving fresh row must still be upsertable in place.
	s.Add(row("R1", "g1", nowIST(0), 5, true))
	rows = s.GetAll([]string{"R1"})["R1"]
	if len(rows) != 1 {
		t.Fatalf("upsert after prune must not append; got %d rows", len(rows))
	}
	if got := rows[0]["aggResult"].(map[string]interface{})["count"].(float64); got != 5 {
		t.Fatalf("expected upserted count 5, got %v", got)
	}
}
