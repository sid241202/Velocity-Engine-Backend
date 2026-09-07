package services

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
	"velocity-engine-control-plane-backend-go/internal/metrics"

	_ "github.com/go-sql-driver/mysql"
)

var (
	mysqlDB    *sql.DB
	mysqlDBMu  sync.Mutex // guards mysqlDB; allows retry after a failed init, unlike sync.Once
	mysqlDBErr error
)

// mysqlDSN builds the connection string for the app's MySQL pool. Never
// enables multiStatements — this backend only ever runs single, parameterized
// statements against MySQL (schema changes are a manual, out-of-band step;
// see VerifyMySQLSchema), so a compromised or buggy query path can't smuggle
// in a stacked statement.
func mysqlDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4",
		config.MySQLUser, config.MySQLPassword, config.MySQLHost, config.MySQLPort, config.MySQLDatabase)
}

// getMySQLDB returns the singleton MySQL connection pool used for regular RBAC
// queries, creating it on first call. Mirrors getClickHouseDB's retry-on-
// failure pattern (sync.Mutex, not sync.Once) so a transient startup failure
// doesn't permanently disable RBAC for the process lifetime.
func getMySQLDB() (*sql.DB, error) {
	mysqlDBMu.Lock()
	defer mysqlDBMu.Unlock()

	if mysqlDB != nil {
		return mysqlDB, nil
	}

	db, err := sql.Open("mysql", mysqlDSN())
	if err != nil {
		mysqlDBErr = err
		return nil, err
	}
	db.SetMaxOpenConns(config.MySQLMaxOpenConns)
	db.SetMaxIdleConns(config.MySQLMaxIdleConns)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute) // proactively recycle idle conns before server kills them — mirrors getClickHouseDB

	if err := db.Ping(); err != nil {
		_ = db.Close()
		mysqlDBErr = err
		return nil, fmt.Errorf("mysql ping failed: %w", err)
	}

	mysqlDB = db
	mysqlDBErr = nil
	slog.Info("MySQL connection pool initialized", "host", config.MySQLHost, "database", config.MySQLDatabase)
	return mysqlDB, nil
}

// ResetMySQLConn forces a reconnect attempt on the next getMySQLDB call.
func ResetMySQLConn() {
	mysqlDBMu.Lock()
	defer mysqlDBMu.Unlock()
	if mysqlDB != nil {
		_ = mysqlDB.Close()
	}
	mysqlDB = nil
	mysqlDBErr = nil
}

// CloseMySQL closes the MySQL connection pool. Called during graceful shutdown.
func CloseMySQL() {
	mysqlDBMu.Lock()
	defer mysqlDBMu.Unlock()
	if mysqlDB != nil {
		if err := mysqlDB.Close(); err != nil {
			slog.Error("Error closing MySQL", "error", err)
		} else {
			slog.Info("MySQL connection pool closed")
		}
		mysqlDB = nil
	}
}

// IsMySQLReady reports whether MySQL is currently reachable. Non-fatal by
// design — callers use this for observability (e.g. /health), not to gate
// process startup or the whole-service readiness probe, since most of this
// backend's functionality does not depend on MySQL/RBAC at all.
func IsMySQLReady() (bool, string) {
	db, err := getMySQLDB()
	if err != nil {
		return false, err.Error()
	}
	if err := db.Ping(); err != nil {
		ResetMySQLConn()
		return false, err.Error()
	}
	return true, ""
}

// MySQLPoolStats returns a snapshot of the MySQL connection pool's stats for
// the mysql_pool_* metrics (see internal/metrics.RegisterMySQLPoolCollectors).
// Returns a zero-value sql.DBStats if the pool hasn't been initialized yet —
// safe to register as a collector before the first MySQL call happens.
func MySQLPoolStats() sql.DBStats {
	mysqlDBMu.Lock()
	defer mysqlDBMu.Unlock()
	if mysqlDB == nil {
		return sql.DBStats{}
	}
	return mysqlDB.Stats()
}

// requiredTables lists every table this backend expects to already exist in
// MySQL: the RBAC tables (see internal/migrations/mysql/0001_init_rbac.sql),
// the five rule-definition store tables (see
// internal/migrations/mysql/0002_init_rules_store.sql and
// internal/services/rule_store.go), and the multi-team RBAC extension (see
// internal/migrations/mysql/0003_add_teams.sql). This backend never creates,
// alters, or seeds any of this schema — it's provisioned manually, per
// environment, by whoever operates MySQL there.
//
// This only checks table existence (SHOW TABLES), not column-level shape —
// same granularity as before 0003_add_teams.sql, which also adds a
// users.team_id column this check does not separately verify.
var requiredTables = []string{
	"users", "roles", "permissions", "user_roles", "role_permissions", "audit_log",
	"rules", "window_configs", "sink_configs", "aggregation_specs", "breach_conditions",
	"teams", "team_leads",
}

// VerifyMySQLSchema checks that all required tables already exist in MySQL.
// It is read-only: it never creates, alters, or seeds anything. Returns a
// non-nil error if MySQL is unreachable or any required table is missing,
// naming exactly which ones — callers should treat this as non-fatal to
// process startup (log loudly and continue), matching the resilience
// philosophy used for ClickHouse/DuckDB: routes/features depending on a
// missing table fail closed until the schema is confirmed present, but the
// rest of the backend does not depend on MySQL.
func VerifyMySQLSchema(ctx context.Context) error {
	start := time.Now()
	err := verifyMySQLSchemaImpl(ctx)
	metrics.MySQLQueryDuration.WithLabelValues("verify_schema").Observe(time.Since(start).Seconds())
	return err
}

func verifyMySQLSchemaImpl(ctx context.Context) error {
	db, err := getMySQLDB()
	if err != nil {
		return fmt.Errorf("cannot verify MySQL schema — connection failed: %w", err)
	}

	// SHOW TABLES FROM `<db>` is simpler and avoids dynamic string/placeholder generation.
	// We backtick the database name to handle any special characters safely.
	query := fmt.Sprintf("SHOW TABLES FROM `%s`", config.MySQLDatabase)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute SHOW TABLES: %w", err)
	}
	defer rows.Close()

	// Read all existing tables into a map for O(1) lookups
	found := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		found[name] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating table rows: %w", err)
	}

	// Check required tables against the found tables
	var missing []string
	for _, t := range requiredTables {
		if !found[t] {
			missing = append(missing, t)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf(
			"MySQL database %q is missing required table(s) %v — run the relevant internal/migrations/mysql/*.sql file(s) manually against this database to provision them",
			config.MySQLDatabase, missing,
		)
	}

	slog.Info("MySQL schema verified", "database", config.MySQLDatabase, "tables", requiredTables)
	return nil
}
