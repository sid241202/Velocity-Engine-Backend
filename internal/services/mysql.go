package services

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"velocity-engine-control-plane-backend-go/internal/config"
	"velocity-engine-control-plane-backend-go/internal/migrations"

	_ "github.com/go-sql-driver/mysql"
)

var (
	mysqlDB    *sql.DB
	mysqlDBMu  sync.Mutex // guards mysqlDB; allows retry after a failed init, unlike sync.Once
	mysqlDBErr error
)

// mysqlDSN builds the connection string for the given database/user, with the
// given extra options appended. multiStatements is intentionally NOT part of
// the base app DSN (getMySQLDB) — it's only enabled on the dedicated
// migration connection (runMigrationsConn), so a compromised or buggy query
// path elsewhere in the app can never smuggle in a stacked statement.
func mysqlDSN(extra string) string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4",
		config.MySQLUser, config.MySQLPassword, config.MySQLHost, config.MySQLPort, config.MySQLDatabase)
	if extra != "" {
		dsn += "&" + extra
	}
	return dsn
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

	db, err := sql.Open("mysql", mysqlDSN(""))
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

// RunMySQLMigrations applies any not-yet-applied embedded migrations, in
// filename order, tracked in a schema_migrations table. Guarded by a MySQL
// advisory lock (GET_LOCK) so multiple backend replicas booting concurrently
// don't race to apply the same migration twice.
//
// Uses its own short-lived connection with multiStatements=true — migration
// files contain multiple DDL/DML statements — kept entirely separate from the
// pooled app connection (getMySQLDB), which never enables multiStatements.
//
// Returns an error if migrations cannot be applied; callers should treat this
// as non-fatal to process startup (log loudly and continue) since routes
// protected by RBAC will simply fail closed until this is resolved, but the
// rest of the backend does not depend on MySQL.
func RunMySQLMigrations(ctx context.Context) error {
	db, err := sql.Open("mysql", mysqlDSN("multiStatements=true"))
	if err != nil {
		return fmt.Errorf("failed to open migration connection: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1) // GET_LOCK is session-scoped; must stay on the same connection throughout

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire migration connection: %w", err)
	}
	defer conn.Close()

	const lockName = "velocity_engine_rbac_migrations"
	var lockAcquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 30)", lockName).Scan(&lockAcquired); err != nil {
		return fmt.Errorf("failed to acquire migration lock: %w", err)
	}
	if !lockAcquired.Valid || lockAcquired.Int64 != 1 {
		return fmt.Errorf("could not acquire migration lock %q within timeout — another instance may be migrating, or MySQL is unavailable", lockName)
	}
	defer func() {
		if _, err := conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", lockName); err != nil {
			slog.Warn("Failed to release migration lock", "error", err)
		}
	}()

	if _, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     VARCHAR(255) PRIMARY KEY,
			applied_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`); err != nil {
		return fmt.Errorf("failed to ensure schema_migrations table: %w", err)
	}

	entries, err := migrations.MySQLFS.ReadDir("mysql")
	if err != nil {
		return fmt.Errorf("failed to read embedded migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	applied := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		version := e.Name()

		var exists int
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations WHERE version = ?", version).Scan(&exists); err != nil {
			return fmt.Errorf("failed to check migration status for %s: %w", version, err)
		}
		if exists > 0 {
			continue
		}

		content, err := migrations.MySQLFS.ReadFile("mysql/" + version)
		if err != nil {
			return fmt.Errorf("failed to read migration %s: %w", version, err)
		}

		slog.Info("Applying MySQL migration", "version", version)
		if _, err := conn.ExecContext(ctx, string(content)); err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", version, err)
		}
		if _, err := conn.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES (?)", version); err != nil {
			return fmt.Errorf("failed to record migration %s as applied: %w", version, err)
		}
		applied++
	}

	slog.Info("MySQL migrations complete", "applied", applied, "total", len(entries))
	return nil
}
