package services

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"velocity-engine-control-plane-backend-go/internal/config"

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

// requiredRBACTables lists the tables the RBAC subsystem expects to already
// exist in MySQL. This backend never creates, alters, or seeds this schema —
// tables are provisioned manually, per environment, by whoever operates
// MySQL there. See internal/migrations/mysql/0001_init_rbac.sql for the
// canonical DDL + seed data to run by hand with a mysql client.
var requiredRBACTables = []string{"users", "roles", "permissions", "user_roles", "role_permissions", "audit_log"}

// VerifyMySQLSchema checks that the RBAC tables already exist in MySQL. It
// is read-only: it never creates, alters, or seeds anything. Returns a
// non-nil error if MySQL is unreachable or any required table is missing,
// naming exactly which ones — callers should treat this as non-fatal to
// process startup (log loudly and continue), matching the resilience
// philosophy used for ClickHouse/DuckDB: RBAC-protected routes fail closed
// (503) until the schema is confirmed present, but the rest of the backend
// does not depend on MySQL.
func VerifyMySQLSchema(ctx context.Context) error {
	db, err := getMySQLDB()
	if err != nil {
		return fmt.Errorf("cannot verify MySQL RBAC schema — connection failed: %w", err)
	}

	placeholders := make([]string, len(requiredRBACTables))
	args := make([]interface{}, 0, len(requiredRBACTables)+1)
	args = append(args, config.MySQLDatabase)
	for i, t := range requiredRBACTables {
		placeholders[i] = "?"
		args = append(args, t)
	}
	query := fmt.Sprintf(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name IN (%s)",
		strings.Join(placeholders, ", "),
	)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query information_schema for RBAC tables: %w", err)
	}
	defer rows.Close()

	found := make(map[string]bool, len(requiredRBACTables))
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("failed to scan information_schema row: %w", err)
		}
		found[name] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating information_schema rows: %w", err)
	}

	var missing []string
	for _, t := range requiredRBACTables {
		if !found[t] {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf(
			"MySQL database %q is missing required RBAC table(s) %v — run internal/migrations/mysql/0001_init_rbac.sql manually against this database to provision them",
			config.MySQLDatabase, missing,
		)
	}

	slog.Info("MySQL RBAC schema verified", "database", config.MySQLDatabase, "tables", requiredRBACTables)
	return nil
}
