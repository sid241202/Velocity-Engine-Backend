package services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
)

// ErrUserNotFound and ErrUserDisabled are authoritative negative results (as
// opposed to a transient lookup failure) — callers must NOT fall back to a
// stale cache entry for these, since that could keep granting access to a
// user who was just disabled.
var (
	ErrUserNotFound = errors.New("user not found")
	ErrUserDisabled = errors.New("user is disabled")
)

// ── In-memory RBAC store (demo branch) ──────────────────────────────────
//
// This branch runs with zero MySQL tables provisioned: roles, permissions,
// role_permissions, and users/user_roles all live in this file instead of
// the tables described in the now-removed internal/migrations/mysql/*.sql.
// This is a storage-layer swap only — IdentityMiddleware's X-Debug-User-Id
// flow (internal/middleware/auth.go), GetUserPermissions's caching
// behavior, and the resource:action permission model are all unchanged.
//
// rolePermissions mirrors 0001_init_rbac.sql's role_permissions seed
// exactly (verified permission-for-permission against that file's INSERT
// statements before this replaced it) — the four roles and ten
// resource:action keys must still match frontend/src/permissions.js.
var rolePermissions = map[string]map[string]bool{
	"SUPER_ADMIN": {
		"rules:create": true, "rules:read": true, "rules:update": true, "rules:delete": true, "rules:publish": true,
		"live_analysis:read": true, "aggregated_analysis:read": true,
		"historical_analysis:read": true, "historical_analysis:execute": true,
		"iam:manage": true,
	},
	"RULE_MANAGER": {
		"rules:create": true, "rules:read": true, "rules:update": true, "rules:delete": true, "rules:publish": true,
		"live_analysis:read": true, "aggregated_analysis:read": true,
		"historical_analysis:read": true, "historical_analysis:execute": true,
	},
	"RULE_EDITOR": {
		"rules:create": true, "rules:read": true, "rules:update": true,
		"live_analysis:read": true, "aggregated_analysis:read": true,
		"historical_analysis:read": true, "historical_analysis:execute": true,
	},
	"READ_ONLY_ANALYST": {
		"rules:read": true,
		"live_analysis:read": true, "aggregated_analysis:read": true,
		"historical_analysis:read": true, "historical_analysis:execute": true,
	},
}

// demoUser is a seeded identity for the X-Debug-User-Id flow. Real
// deployments (see release branch) provision users individually per
// environment; demo instead ships one fixed user per role so the debug
// login flow works with zero external setup — type the id into
// DebugIdentitySwitcher (frontend) to try each role.
type demoUser struct {
	role   string
	status string // "ACTIVE" or "DISABLED"
}

// demoUsers is seed data, populated once at package init and never written
// to afterward (no admin endpoints exist to add/remove demo users) — safe
// for concurrent reads without a mutex, unlike permCache below, which is
// genuinely mutated on every request.
var demoUsers = map[int64]demoUser{
	1: {role: "SUPER_ADMIN", status: "ACTIVE"},        // Ava
	2: {role: "RULE_MANAGER", status: "ACTIVE"},       // Rahul
	3: {role: "RULE_EDITOR", status: "ACTIVE"},        // Priya
	4: {role: "READ_ONLY_ANALYST", status: "ACTIVE"},  // Devika
}

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

// fetchFunc is the function GetUserPermissions calls on a cache miss/expiry.
// A package-level variable (not called directly) so tests can swap in a
// fake — the same dependency-injection pattern used for
// middleware.PermissionResolver.
var fetchFunc = fetchUserPermissionsFromMemory

// GetUserPermissions returns the resolved (roles, permission-set) for a user.
//
// Served from an in-memory cache (TTL: config.RBACPermissionCacheTTLSeconds)
// in front of the (already in-memory) demoUsers/rolePermissions lookup — kept
// rather than calling fetchFunc directly so this function's behavior and
// signature stay identical to the MySQL-backed version it replaced, and so
// GetUserPermissions is still O(1) per call regardless of how the underlying
// store is implemented.
//
// Fault tolerance: fetchUserPermissionsFromMemory has no real failure mode
// (no network, no DB), but the stale-serving/fail-closed machinery is kept
// as-is rather than stripped out — ErrUserNotFound/ErrUserDisabled are still
// genuine, reachable outcomes (an unseeded or disabled demo user id), and
// this keeps GetUserPermissions's contract identical to release's.
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

// InvalidateUserPermissions evicts a user's cached permission set, forcing
// the next GetUserPermissions call to re-resolve it. No demo code path
// mutates demoUsers/rolePermissions at runtime (no admin endpoints exist),
// so this currently has no real effect on demo — kept for interface parity
// with release, where a future admin endpoint would call it after a role
// change.
func InvalidateUserPermissions(userID int64) {
	permCacheMu.Lock()
	delete(permCache, userID)
	permCacheMu.Unlock()
}

// fetchUserPermissionsFromMemory resolves a user's roles and flat permission
// set from the seeded demoUsers/rolePermissions maps — the in-memory
// replacement for the MySQL join this used to run. Each demo user has
// exactly one role (matching the one-role-per-user constraint the real
// user_roles table enforced), so "roles" here is always length 0 or 1, kept
// as a slice to match GetUserPermissions's existing signature.
func fetchUserPermissionsFromMemory(ctx context.Context, userID int64) (roles []string, perms map[string]bool, err error) {
	u, ok := demoUsers[userID]
	if !ok {
		return nil, nil, ErrUserNotFound
	}
	if u.status != "ACTIVE" {
		return nil, nil, ErrUserDisabled
	}

	rolePerms := rolePermissions[u.role]
	perms = make(map[string]bool, len(rolePerms))
	for k := range rolePerms {
		perms[k] = true
	}
	return []string{u.role}, perms, nil
}
