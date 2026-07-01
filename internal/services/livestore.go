package services

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
)

// istZoneLS is the IST fixed timezone offset (UTC+05:30) for LiveStore timestamp parsing.
var istZoneLS = time.FixedZone("IST", 5*60*60+30*60)

// parseWindowStart attempts to parse a windowStart value (string or int64) into time.Time.
// Supports the IST-formatted string that ClickHouse returns and ISO-8601 that Flink may send.
func parseWindowStart(v interface{}) (time.Time, bool) {
	switch val := v.(type) {
	case string:
		if val == "" {
			return time.Time{}, false
		}
		// Common format from ClickHouse bootstrap: "2006-01-02 15:04:05" (IST, no 'T', no 'Z')
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

// LiveStore is a thread-safe in-memory rolling store for live rule results.
type LiveStore struct {
	mu          sync.RWMutex
	data        map[string][]map[string]interface{} // ruleId -> rows
	totalRows   int
	droppedRows int64 // cumulative count of rows dropped due to capacity cap
	maxRows     int
	hours       int
}

// NewLiveStore creates a new LiveStore with configured limits.
func NewLiveStore() *LiveStore {
	ls := &LiveStore{
		data:    make(map[string][]map[string]interface{}),
		maxRows: config.LiveStoreMaxRows,
		hours:   config.LiveStoreHours,
	}
	// Start a background goroutine to prune stale data every 30 seconds.
	// This replaces the O(N) prune-on-every-add pattern which blocked the write lock.
	go ls.runPruner()
	return ls
}

// runPruner is a background goroutine that periodically evicts stale rows.
// It runs every 30 seconds so the write mutex is not held on every Add() call.
func (s *LiveStore) runPruner() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		s.pruneStale()
		s.mu.Unlock()
	}
}

// Add appends a single result row to the store.
func (s *LiveStore) Add(row map[string]interface{}) {
	ruleID, _ := row["ruleId"].(string)
	if ruleID == "" {
		ruleID = "unknown"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[ruleID] = append(s.data[ruleID], row)
	s.totalRows++
	// Only enforce the hard cap inline; time-based pruning is done by the background goroutine.
	if s.totalRows > s.maxRows {
		s.trimExcess()
	}
}

// Bootstrap replaces all data with the given rows (used at startup).
func (s *LiveStore) Bootstrap(rows []map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string][]map[string]interface{})
	s.totalRows = 0
	for _, row := range rows {
		ruleID, _ := row["ruleId"].(string)
		if ruleID == "" {
			ruleID = "unknown"
		}
		s.data[ruleID] = append(s.data[ruleID], row)
		s.totalRows++
	}
	slog.Info("LiveStore bootstrapped", "total_rows", s.totalRows, "rule_count", len(s.data))
}

// GetAll returns rows for the specified rule IDs.
func (s *LiveStore) GetAll(ruleIDs []string) map[string][]map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string][]map[string]interface{})
	for _, rid := range ruleIDs {
		if rows, ok := s.data[rid]; ok {
			// Return a copy of the slice
			copied := make([]map[string]interface{}, len(rows))
			copy(copied, rows)
			result[rid] = copied
		}
	}
	return result
}

// Stats returns current store statistics including dropped-rows counter.
func (s *LiveStore) Stats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return map[string]interface{}{
		"total_rows":   s.totalRows,
		"rule_count":   len(s.data),
		"dropped_rows": s.droppedRows,
	}
}

// pruneStale removes rows older than s.hours from all rules.
// Must be called with s.mu held (write).
func (s *LiveStore) pruneStale() {
	cutoff := time.Now().Add(-time.Duration(s.hours) * time.Hour)

	for rid, rows := range s.data {
		startIdx := 0
		for startIdx < len(rows) {
			ws := rows[startIdx]["windowStart"]
			t, ok := parseWindowStart(ws)
			if !ok || t.After(cutoff) {
				break
			}
			startIdx++
		}
		if startIdx > 0 {
			s.totalRows -= startIdx
			// Copy to new slice to release old backing array (avoid memory leak)
			newRows := make([]map[string]interface{}, len(rows)-startIdx)
			copy(newRows, rows[startIdx:])
			s.data[rid] = newRows
		}
		if len(s.data[rid]) == 0 {
			delete(s.data, rid)
		}
	}
}

// trimExcess removes rows from the largest rule until we are within maxRows.
// Must be called with s.mu held (write).
func (s *LiveStore) trimExcess() {
	excess := s.totalRows - s.maxRows
	for excess > 0 {
		// Find rule with the most rows
		largestRID := ""
		largestCount := 0
		for rid, rows := range s.data {
			if len(rows) > largestCount {
				largestRID = rid
				largestCount = len(rows)
			}
		}
		if largestRID == "" || largestCount == 0 {
			break
		}
		// Remove up to 'excess' rows from the front of the largest rule
		toRemove := excess
		if toRemove > largestCount {
			toRemove = largestCount
		}
		old := s.data[largestRID]
		newRows := make([]map[string]interface{}, len(old)-toRemove)
		copy(newRows, old[toRemove:])
		s.data[largestRID] = newRows
		s.totalRows -= toRemove
		s.droppedRows += int64(toRemove)
		excess -= toRemove
		if len(s.data[largestRID]) == 0 {
			delete(s.data, largestRID)
		}
	}
}

// StatsString returns a formatted stats string for logging.
func (s *LiveStore) StatsString() string {
	stats := s.Stats()
	return fmt.Sprintf("total_rows=%v, rule_count=%v", stats["total_rows"], stats["rule_count"])
}
