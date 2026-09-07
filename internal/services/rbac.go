package services

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
	"velocity-engine-control-plane-backend-go/internal/metrics"

	"github.com/go-sql-driver/mysql"
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
		metrics.RBACCacheHitsTotal.Inc()
		return entry.roles, entry.perms, nil
	}
	metrics.RBACCacheMissesTotal.Inc()

	fetchStart := time.Now()
	roles, perms, err := fetchFunc(ctx, userID)
	metrics.RBACResolutionDuration.Observe(time.Since(fetchStart).Seconds())
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
// next GetUserPermissions call to re-resolve from MySQL. Called from every
// Admin Panel mutation that changes a user's role/team/status (see
// internal/services/admin.go) so the effect is immediate rather than waiting
// out the TTL — the active-invalidation half of the agreed cache strategy,
// with the TTL/stale-serving behavior kept as-is for the now-rarer case of a
// direct external DB edit.
func InvalidateUserPermissions(userID int64) {
	permCacheMu.Lock()
	delete(permCache, userID)
	permCacheMu.Unlock()
	metrics.RBACCacheInvalidationsTotal.Inc()
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
// designed for (see 0001_init_rbac.sql).
//
// Deliberately does NOT auto-create a user row on a miss: ErrUserNotFound
// here means "reject the request," matching the reference operator360
// backend's original deny-until-provisioned behavior. IdentityMiddleware is
// now wired to GetOrProvisionUserByExternalSubject instead (see
// cmd/server/main.go), which auto-provisions on a miss — this function
// remains available (and still fully tested) for any future caller that must
// NOT auto-provision.
func GetUserByExternalSubject(ctx context.Context, sub string) (userID int64, status string, err error) {
	start := time.Now()
	userID, status, err = getUserByExternalSubjectImpl(ctx, sub)
	metrics.MySQLQueryDuration.WithLabelValues("get_user_by_external_subject").Observe(time.Since(start).Seconds())
	return userID, status, err
}

func getUserByExternalSubjectImpl(ctx context.Context, sub string) (userID int64, status string, err error) {
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

// jitRateLimiter is a simple fixed-window counter per source IP, guarding
// GetOrProvisionUserByExternalSubject. This is the compensating control for
// JIT auto-provisioning's real exposure: IdentityMiddleware trusts the
// client-supplied X-User-Subject header without cryptographic verification
// (see that function's doc comment), so without a bound, auto-creating a
// user for every unrecognized subject would let anyone who can reach this
// backend mint unlimited accounts just by varying the header value. A fixed
// window (not a sliding one or a token bucket) is deliberately simple here —
// this only needs to catch a sustained flood, not shape traffic precisely.
var (
	jitRateMu          sync.Mutex
	jitRateWindowStart time.Time
	jitRateCounts      = make(map[string]int)
)

const jitRateWindow = time.Minute

func jitRateLimited(sourceIP string) bool {
	jitRateMu.Lock()
	defer jitRateMu.Unlock()

	now := time.Now()
	if now.Sub(jitRateWindowStart) > jitRateWindow {
		jitRateWindowStart = now
		jitRateCounts = make(map[string]int)
	}
	jitRateCounts[sourceIP]++
	return jitRateCounts[sourceIP] > config.JITProvisionRateLimitPerMinute
}

// GetOrProvisionUserByExternalSubject resolves a WSO2/OIDC "sub" claim to a
// local user, auto-creating one (READ_ONLY_ANALYST, the MISC default team)
// on first sight instead of GetUserByExternalSubject's deny-until-provisioned
// behavior. This is a deliberate reversal of a previously-deliberate security
// posture — see the design discussion for the full tradeoff — made more
// defensible by three compensating controls: a per-source-IP rate limit
// (jitRateLimited, above), an audit_log row for every provisioning event, and
// the IAMJITProvisionedUsersTotal/IAMJITProvisionRateLimitedTotal metrics so
// an unusual burst of new-user creation is visible in the same dashboard as
// everything else, not something someone has to notice by reading a table.
//
// Wired in as IdentityMiddleware's resolver (see cmd/server/main.go) in place
// of GetUserByExternalSubject — that function still exists and is still the
// right choice for anything that must NOT auto-provision.
func GetOrProvisionUserByExternalSubject(ctx context.Context, sub, sourceIP string) (userID int64, status string, err error) {
	userID, status, err = getUserByExternalSubjectImpl(ctx, sub)
	if err == nil {
		return userID, status, nil
	}
	if !errors.Is(err, ErrUserNotFound) {
		return 0, "", err
	}

	if jitRateLimited(sourceIP) {
		metrics.IAMJITProvisionRateLimitedTotal.Inc()
		slog.Warn("JIT provisioning rate-limited", "source_ip", sourceIP)
		return 0, "", ErrUserNotFound
	}

	userID, status, err = provisionUserByExternalSubject(ctx, sub)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			// Lost a race with a concurrent request provisioning the same
			// subject — the other insert already landed, so look it up.
			return getUserByExternalSubjectImpl(ctx, sub)
		}
		return 0, "", err
	}

	metrics.IAMJITProvisionedUsersTotal.Inc()
	if auditErr := RecordAuditEvent(ctx, nil, "user.jit_provision", "user", strconv.FormatInt(userID, 10), map[string]interface{}{
		"external_subject": sub,
		"source_ip":        sourceIP,
	}); auditErr != nil {
		slog.Error("Failed to record JIT-provisioning audit event", "user_id", userID, "error", auditErr)
	}
	return userID, status, nil
}

// provisionUserByExternalSubject creates a new user row, assigns it to the
// MISC default team, and grants READ_ONLY_ANALYST — all in one transaction
// so a partial provision (a user row with no role, or no team) can never
// happen. There's no email/display name available at this layer (the
// frontend only ever sends the WSO2 "sub" via X-User-Subject, not profile
// claims — see apiClient.js's getAuthHeaders), so both are placeholder-
// derived from sub itself; a super admin or team lead can rename the user
// once they know who it actually is.
func provisionUserByExternalSubject(ctx context.Context, sub string) (userID int64, status string, err error) {
	db, err := getMySQLDB()
	if err != nil {
		return 0, "", fmt.Errorf("mysql unavailable: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, "", fmt.Errorf("failed to begin provisioning transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed

	email := sub
	if !strings.Contains(email, "@") {
		email = sub + "@jit.invalid" // never a real deliverable address — placeholder only
	}

	res, err := tx.ExecContext(ctx,
		"INSERT INTO users (external_subject, email, display_name, status) VALUES (?, ?, ?, 'ACTIVE')",
		sub, email, sub,
	)
	if err != nil {
		return 0, "", err // caller checks for a duplicate-key race
	}
	newID, err := res.LastInsertId()
	if err != nil {
		return 0, "", fmt.Errorf("failed to read new user id: %w", err)
	}

	var miscTeamID int64
	if err := tx.QueryRowContext(ctx, "SELECT id FROM teams WHERE is_default = TRUE LIMIT 1").Scan(&miscTeamID); err != nil {
		return 0, "", fmt.Errorf("failed to resolve default team: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE users SET team_id = ? WHERE id = ?", miscTeamID, newID); err != nil {
		return 0, "", fmt.Errorf("failed to assign default team: %w", err)
	}

	var roleID int64
	if err := tx.QueryRowContext(ctx, "SELECT id FROM roles WHERE name = 'READ_ONLY_ANALYST'").Scan(&roleID); err != nil {
		return 0, "", fmt.Errorf("failed to resolve default role: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO user_roles (user_id, role_id) VALUES (?, ?)", newID, roleID); err != nil {
		return 0, "", fmt.Errorf("failed to assign default role: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, "", fmt.Errorf("failed to commit provisioning transaction: %w", err)
	}
	return newID, "ACTIVE", nil
}

// GetLedTeamIDs returns the team IDs a user leads (team_leads rows), or an
// empty slice if they lead none. Deliberately uncached — unlike
// GetUserPermissions, this is a single indexed lookup (idx_team_leads_user)
// and team-lead changes are rare, so a second cache to keep in sync with
// InvalidateUserPermissions would be complexity with no real payoff.
func GetLedTeamIDs(ctx context.Context, userID int64) ([]int64, error) {
	db, err := getMySQLDB()
	if err != nil {
		return nil, fmt.Errorf("mysql unavailable: %w", err)
	}
	rows, err := db.QueryContext(ctx, "SELECT team_id FROM team_leads WHERE user_id = ?", userID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve led teams: %w", err)
	}
	defer rows.Close()

	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan led team id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return ids, nil
}

// RecordAuditEvent inserts one audit_log row. actorUserID is nil for
// system-initiated actions. Not currently called from any request path in
// this task — it's wired up ready for the future admin endpoints that grant/
// revoke roles, which is where audit events actually originate.
func RecordAuditEvent(ctx context.Context, actorUserID *int64, action, targetType, targetID string, metadata map[string]interface{}) error {
	start := time.Now()
	err := recordAuditEventImpl(ctx, actorUserID, action, targetType, targetID, metadata)
	metrics.MySQLQueryDuration.WithLabelValues("record_audit_event").Observe(time.Since(start).Seconds())
	return err
}

func recordAuditEventImpl(ctx context.Context, actorUserID *int64, action, targetType, targetID string, metadata map[string]interface{}) error {
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
