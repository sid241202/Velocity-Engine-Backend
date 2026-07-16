package services

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
)

// ErrUserNotFound and ErrUserDisabled are authoritative negative results from
// MySQL (as opposed to a transient connectivity failure) — callers must NOT
// fall back to a stale cache entry for these, since that could keep granting
// access to a user who was just disabled.
var (
	ErrUserNotFound = errors.New("user not found")
	ErrUserDisabled = errors.New("user is disabled")
)

// userPermissionCacheEntry is the cached (roles, permission-set) for one user.
type userPermissionCacheEntry struct {
	roles     []string
	perms     map[string]bool
	fetchedAt time.Time
}

var (
	permCacheMu sync.RWMutex
	permCache   = make(map[int64]*userPermissionCacheEntry)
)

// fetchFunc is the DB-fetching function GetUserPermissions calls on a cache
// miss/expiry. A package-level variable (not called directly) so tests can
// swap in a fake without a real MySQL connection — the same dependency-
// injection pattern used for middleware.PermissionResolver.
var fetchFunc = fetchUserPermissionsFromDB

// GetUserPermissions returns the resolved (roles, permission-set) for a user.
//
// Served from an in-memory cache (TTL: config.RBACPermissionCacheTTLSeconds)
// to avoid a multi-table JOIN on every authorization check — this is called
// from RequirePermission middleware on every protected request, so it must
// stay O(1) in the common case, the same reasoning behind LiveStore and the
// pinned DuckDB connection elsewhere in this codebase.
//
// Fault tolerance: if MySQL is unreachable when a refresh is due, a stale
// cache entry is served for up to config.RBACPermissionMaxStaleSeconds so a
// brief MySQL blip doesn't lock every user out of the control plane — beyond
// that bound, or if MySQL authoritatively says the user doesn't exist or is
// disabled, this fails closed (returns an error) rather than trusting old
// data indefinitely.
func GetUserPermissions(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
	permCacheMu.RLock()
	entry, ok := permCache[userID]
	permCacheMu.RUnlock()

	ttl := time.Duration(config.RBACPermissionCacheTTLSeconds) * time.Second
	if ok && time.Since(entry.fetchedAt) < ttl {
		return entry.roles, entry.perms, nil
	}

	roles, perms, err := fetchFunc(ctx, userID)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) || errors.Is(err, ErrUserDisabled) {
			permCacheMu.Lock()
			delete(permCache, userID)
			permCacheMu.Unlock()
			return nil, nil, err
		}

		maxStale := time.Duration(config.RBACPermissionMaxStaleSeconds) * time.Second
		if ok && time.Since(entry.fetchedAt) < maxStale {
			slog.Warn("RBAC permission refresh failed — serving stale cache",
				"user_id", userID, "cache_age", time.Since(entry.fetchedAt), "error", err)
			return entry.roles, entry.perms, nil
		}
		return nil, nil, fmt.Errorf("permission resolution unavailable: %w", err)
	}

	permCacheMu.Lock()
	permCache[userID] = &userPermissionCacheEntry{roles: roles, perms: perms, fetchedAt: time.Now()}
	permCacheMu.Unlock()
	return roles, perms, nil
}

// InvalidateUserPermissions evicts a user's cached permission set, forcing the
// next GetUserPermissions call to re-resolve from MySQL. Call this whenever a
// user's role assignments change (future admin endpoints) so the effect is
// immediate rather than waiting out the TTL.
func InvalidateUserPermissions(userID int64) {
	permCacheMu.Lock()
	delete(permCache, userID)
	permCacheMu.Unlock()
}

// fetchUserPermissionsFromDB resolves a user's roles and flat permission set
// via two queries: a cheap existence/status check (so ErrUserNotFound and
// ErrUserDisabled are distinguishable from "active user with zero roles"),
// then the actual join across user_roles -> roles -> role_permissions ->
// permissions. This only runs on a cache miss, so two round-trips instead of
// one is negligible — the clarity is worth it.
func fetchUserPermissionsFromDB(ctx context.Context, userID int64) (roles []string, perms map[string]bool, err error) {
	db, err := getMySQLDB()
	if err != nil {
		return nil, nil, fmt.Errorf("mysql unavailable: %w", err)
	}

	var status string
	err = db.QueryRowContext(ctx, "SELECT status FROM users WHERE id = ?", userID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, ErrUserNotFound
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to look up user: %w", err)
	}
	if status != "ACTIVE" {
		return nil, nil, ErrUserDisabled
	}

	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT r.name, p.resource, p.action
		FROM user_roles ur
		JOIN roles r ON r.id = ur.role_id
		JOIN role_permissions rp ON rp.role_id = r.id
		JOIN permissions p ON p.id = rp.permission_id
		WHERE ur.user_id = ?
	`, userID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to resolve permissions: %w", err)
	}
	defer rows.Close()

	roleSet := make(map[string]bool)
	perms = make(map[string]bool)
	for rows.Next() {
		var roleName, resource, action string
		if scanErr := rows.Scan(&roleName, &resource, &action); scanErr != nil {
			return nil, nil, fmt.Errorf("failed to scan permission row: %w", scanErr)
		}
		roleSet[roleName] = true
		perms[resource+":"+action] = true
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("rows iteration error: %w", err)
	}

	roles = make([]string, 0, len(roleSet))
	for r := range roleSet {
		roles = append(roles, r)
	}
	sort.Strings(roles)
	return roles, perms, nil
}

// GetUserByExternalSubject resolves a WSO2/OIDC "sub" claim to this system's
// local users.id + status, via the external_subject column that schema was
// designed for (see 0001_init_rbac.sql). Used only by IdentityMiddleware's
// "wso2" path — the dev-mode X-Debug-User-Id shim already carries a local
// user ID directly and never needs this lookup.
//
// Deliberately does NOT auto-create a user row on a miss: ErrUserNotFound
// here means "reject the request," matching the reference operator360
// backend's deny-until-provisioned behavior — an admin must have already
// created this user via the existing manual MySQL provisioning process.
func GetUserByExternalSubject(ctx context.Context, sub string) (userID int64, status string, err error) {
	db, err := getMySQLDB()
	if err != nil {
		return 0, "", fmt.Errorf("mysql unavailable: %w", err)
	}

	err = db.QueryRowContext(ctx, "SELECT id, status FROM users WHERE external_subject = ?", sub).Scan(&userID, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", ErrUserNotFound
	}
	if err != nil {
		return 0, "", fmt.Errorf("failed to look up user by external_subject: %w", err)
	}
	return userID, status, nil
}

// RecordAuditEvent inserts one audit_log row. actorUserID is nil for
// system-initiated actions. Not currently called from any request path in
// this task — it's wired up ready for the future admin endpoints that grant/
// revoke roles, which is where audit events actually originate.
func RecordAuditEvent(ctx context.Context, actorUserID *int64, action, targetType, targetID string, metadata map[string]interface{}) error {
	db, err := getMySQLDB()
	if err != nil {
		return fmt.Errorf("mysql unavailable: %w", err)
	}

	var metaJSON interface{}
	if metadata != nil {
		b, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal audit metadata: %w", err)
		}
		metaJSON = string(b)
	}

	_, err = db.ExecContext(ctx,
		"INSERT INTO audit_log (actor_user_id, action, target_type, target_id, metadata) VALUES (?, ?, ?, ?, ?)",
		actorUserID, action, targetType, targetID, metaJSON,
	)
	if err != nil {
		return fmt.Errorf("failed to record audit event: %w", err)
	}
	return nil
}
