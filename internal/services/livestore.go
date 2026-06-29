package services

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
)

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
	return &LiveStore{
		data:    make(map[string][]map[string]interface{}),
		maxRows: config.LiveStoreMaxRows,
		hours:   config.LiveStoreHours,
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
	s.pruneIfNeeded()
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

// pruneIfNeeded removes stale rows and enforces max row cap.
// Must be called with s.mu held.
func (s *LiveStore) pruneIfNeeded() {
	cutoff := time.Now().Add(-time.Duration(s.hours) * time.Hour).Format("2006-01-02 15:04:05")

	for rid, rows := range s.data {
		startIdx := 0
		for startIdx < len(rows) {
			ws, _ := rows[startIdx]["windowStart"].(string)
			if ws >= cutoff {
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

	// If still over max rows, batch-trim by removing excess from the largest rule(s)
	if s.totalRows > s.maxRows {
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
}

// StatsString returns a formatted stats string for logging.
func (s *LiveStore) StatsString() string {
	stats := s.Stats()
	return fmt.Sprintf("total_rows=%v, rule_count=%v", stats["total_rows"], stats["rule_count"])
}
