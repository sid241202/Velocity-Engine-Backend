package services

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// istZone is the IST fixed timezone offset (UTC+05:30).
// All timestamps returned to the frontend are formatted in this zone.
var istZone = time.FixedZone("IST", 5*60*60+30*60)

var (
	chDB    *sql.DB
	chDBMu  sync.Mutex // guards chDB; unlike sync.Once, allows retry after failure
	chDBErr error
)

// getClickHouseDB returns the singleton ClickHouse database connection.
// Unlike sync.Once, this retries on failure so a transient startup failure
// (e.g. ClickHouse not yet ready) does not permanently break the backend.
func getClickHouseDB() (*sql.DB, error) {
	chDBMu.Lock()
	defer chDBMu.Unlock()

	if chDB != nil {
		return chDB, nil
	}

	if !regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`).MatchString(config.ClickHouseTable) {
		chDBErr = fmt.Errorf("invalid clickhouse table name")
		slog.Error("Invalid ClickHouseTable config")
		return nil, chDBErr
	}

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.ClickHouseHost, config.ClickHousePort)},
		Auth: clickhouse.Auth{
			Database: config.ClickHouseDB,
			Username: config.ClickHouseUser,
			Password: config.ClickHousePassword,
		},
		Protocol:    clickhouse.HTTP,
		DialTimeout: 10 * time.Second,
		ReadTimeout: 120 * time.Second, // Increased from 30s — heavy agg scans over 24h can take >30s
	})
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute) // proactively recycle idle conns before server kills them

	if err := db.Ping(); err != nil {
		slog.Error("Failed to ping ClickHouse — will retry on next request", "error", err)
		chDBErr = err
		return nil, err
	}

	chDB = db
	chDBErr = nil
	slog.Info("ClickHouse client initialized",
		"host", config.ClickHouseHost,
		"port", config.ClickHousePort,
		"db", config.ClickHouseDB,
	)
	return chDB, nil
}

// ResetClickHouseConn forces a reconnect attempt on the next request.
// Called after a ping failure so the readiness probe can recover.
func ResetClickHouseConn() {
	chDBMu.Lock()
	defer chDBMu.Unlock()
	if chDB != nil {
		_ = chDB.Close()
	}
	chDB = nil
	chDBErr = nil
}

// rowsToMaps converts sql.Rows into a slice of map[string]interface{},
// parsing aggregationResults JSON strings and converting time.Time to strings.
func rowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var result []map[string]interface{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		row := make(map[string]interface{}, len(columns))
		for i, col := range columns {
			val := values[i]

			// Convert time.Time to IST string so the frontend always gets IST-formatted timestamps
			if t, ok := val.(time.Time); ok {
				row[col] = t.In(istZone).Format("2006-01-02 15:04:05")
			} else if t, ok := val.(*time.Time); ok {
				if t != nil {
					row[col] = t.In(istZone).Format("2006-01-02 15:04:05")
				} else {
					row[col] = nil
				}
			} else {
				row[col] = val
			}
		}

		// Parse aggResult (new schema) — stored as a JSON string in ClickHouse, expand for frontend
		if aggStr, ok := row["aggResult"].(string); ok && aggStr != "" {
			var parsed interface{}
			if err := json.Unmarshal([]byte(aggStr), &parsed); err == nil {
				row["aggResult"] = parsed
			}
		}
		// Backward-compat: old schema used aggregationResults
		if aggStr, ok := row["aggregationResults"].(string); ok && aggStr != "" {
			var parsed interface{}
			if err := json.Unmarshal([]byte(aggStr), &parsed); err == nil {
				row["aggregationResults"] = parsed
			}
		}

		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return result, nil
}

// GetLiveResults fetches the latest rule results from ClickHouse for a single rule.
func GetLiveResults(ctx context.Context, ruleID string, limit int) ([]map[string]interface{}, error) {
	db, err := getClickHouseDB()
	if err != nil {
		return nil, fmt.Errorf("clickhouse not available: %w", err)
	}

	query := fmt.Sprintf(`SELECT * FROM %s FINAL
		WHERE ruleId = ?
		ORDER BY windowStart DESC
		LIMIT ?`, config.ClickHouseTable)

	rows, err := db.QueryContext(ctx, query, ruleID, limit)
	if err != nil {
		// If the connection died, reset so next caller gets a fresh one
		ResetClickHouseConn()
		return nil, fmt.Errorf("clickhouse query failed: %w", err)
	}
	defer rows.Close()

	return rowsToMaps(rows)
}

// GetLiveResultsMulti returns results grouped by ruleId for the given rule IDs
// within the last N hours. If ruleIDs is empty, queries ALL rules (for bootstrap).
func GetLiveResultsMulti(ruleIDs []string, hours int) (map[string][]map[string]interface{}, error) {
	db, err := getClickHouseDB()
	if err != nil {
		return nil, fmt.Errorf("clickhouse not available: %w", err)
	}

	var queryRows *sql.Rows

	if len(ruleIDs) > 0 {
		// Build IN clause with placeholders
		query := fmt.Sprintf(`SELECT * FROM %s FINAL
			WHERE ruleId IN (`+placeholders(len(ruleIDs))+`)
			AND windowStart >= now() - INTERVAL ? HOUR
			ORDER BY windowStart DESC
			LIMIT 5000`, config.ClickHouseTable)

		args := make([]interface{}, 0, len(ruleIDs)+1)
		for _, id := range ruleIDs {
			args = append(args, id)
		}
		args = append(args, hours)

		queryRows, err = db.Query(query, args...)
	} else {
		// No rule_ids filter — fetch ALL rules for bootstrap
		query := fmt.Sprintf(`SELECT * FROM %s FINAL
			WHERE windowStart >= now() - INTERVAL ? HOUR
			ORDER BY windowStart DESC
			LIMIT 5000`, config.ClickHouseTable)

		queryRows, err = db.Query(query, hours)
	}

	if err != nil {
		return nil, fmt.Errorf("clickhouse query failed: %w", err)
	}
	defer queryRows.Close()

	rows, err := rowsToMaps(queryRows)
	if err != nil {
		return nil, err
	}

	// Group by ruleId
	grouped := make(map[string][]map[string]interface{})
	for _, row := range rows {
		ruleID, _ := row["ruleId"].(string)
		if ruleID == "" {
			ruleID = "unknown"
		}
		grouped[ruleID] = append(grouped[ruleID], row)
	}

	return grouped, nil
}

// GetAggResults queries ClickHouse for aggregated results within an explicit time range.
func GetAggResults(ctx context.Context, ruleIDs []string, startTS, endTS string) (map[string][]map[string]interface{}, error) {
	if len(ruleIDs) == 0 {
		return map[string][]map[string]interface{}{}, nil
	}

	db, err := getClickHouseDB()
	if err != nil {
		return nil, fmt.Errorf("clickhouse not available: %w", err)
	}

	query := fmt.Sprintf(`SELECT * FROM %s FINAL
		WHERE ruleId IN (`+placeholders(len(ruleIDs))+`)
		AND windowStart >= parseDateTimeBestEffort(?)
		AND windowStart <= parseDateTimeBestEffort(?)
		ORDER BY windowStart DESC
		LIMIT 5000`, config.ClickHouseTable)

	args := make([]interface{}, 0, len(ruleIDs)+2)
	for _, id := range ruleIDs {
		args = append(args, id)
	}
	args = append(args, startTS, endTS)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse query failed: %w", err)
	}
	defer rows.Close()

	resultRows, err := rowsToMaps(rows)
	if err != nil {
		return nil, err
	}

	grouped := make(map[string][]map[string]interface{})
	for _, row := range resultRows {
		ruleID, _ := row["ruleId"].(string)
		if ruleID == "" {
			ruleID = "unknown"
		}
		grouped[ruleID] = append(grouped[ruleID], row)
	}

	return grouped, nil
}

// CloseClickHouse closes the ClickHouse connection pool.
func CloseClickHouse() {
	chDBMu.Lock()
	defer chDBMu.Unlock()
	if chDB != nil {
		if err := chDB.Close(); err != nil {
			slog.Error("Error closing ClickHouse", "error", err)
		} else {
			slog.Info("ClickHouse connection closed")
		}
		chDB = nil
	}
}

// IsReady checks whether all critical dependencies are reachable.
// Returns (true, "") if ready, or (false, reason) if not.
func IsReady() (bool, string) {
	db, err := getClickHouseDB()
	if err != nil {
		return false, fmt.Sprintf("clickhouse unavailable: %s", err.Error())
	}
	if err := db.Ping(); err != nil {
		// Reset the connection so the next request re-tries
		ResetClickHouseConn()
		return false, fmt.Sprintf("clickhouse ping failed: %s", err.Error())
	}
	return true, ""
}

// placeholders generates a comma-separated list of ? placeholders.
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	s := "?"
	for i := 1; i < n; i++ {
		s += ", ?"
	}
	return s
}
