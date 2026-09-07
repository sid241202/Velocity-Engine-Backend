// Package middleware provides the authorization layer for the control plane:
// resolving the current request's identity and enforcing per-endpoint
// permission requirements against the RBAC store.
package middleware

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// ContextKeyUserID is the gin.Context key IdentityMiddleware sets and
// RequirePermission reads. Exported so handlers (e.g. GET /me) can read the
// resolved identity directly.
const ContextKeyUserID = "auth_user_id"

// ExternalSubjectResolver resolves a WSO2 "sub" claim (plus the request's
// source IP, passed through for JIT-provisioning rate limiting — see
// services.GetOrProvisionUserByExternalSubject) to a local user ID and
// status. Must return ErrIdentityNotFound (not services.ErrUserNotFound —
// this package doesn't import internal/services) when the subject has no
// matching local user and the resolver isn't auto-provisioning one; the
// wiring in cmd/server translates between the two.
type ExternalSubjectResolver func(ctx context.Context, sub string, sourceIP string) (userID int64, status string, err error)

// ErrIdentityNotFound is the sentinel an ExternalSubjectResolver must return
// when a WSO2 subject has no matching local user — distinguishes
// "reject, not provisioned" (401) from a genuine resolver failure (503).
var ErrIdentityNotFound = errors.New("identity not found")

// wso2Resolver backs IdentityMiddleware. Set once at startup via
// SetIdentityResolver — injected rather than imported directly
// (internal/services is not imported by this package at all) for the same
// reason PermissionResolver below is injected: internal/services pulls in
// go-duckdb (cgo) via duckdb.go, and this package needs to stay buildable/
// vettable/testable without that dependency.
var wso2Resolver ExternalSubjectResolver

// SetIdentityResolver wires IdentityMiddleware's real dependency. Call once
// at startup (cmd/server, which already imports internal/services for
// NewAuthMiddleware).
func SetIdentityResolver(resolver ExternalSubjectResolver) {
	wso2Resolver = resolver
}

// IdentityMiddleware resolves the current request's user ID and stores it
// in the gin context under ContextKeyUserID.
//
// KNOWN, DELIBERATE, TEMPORARY SECURITY GAP: identity here is trusted from
// the client-supplied X-User-Subject header, not cryptographically
// verified. The header carries the WSO2 "sub" claim the frontend decoded
// (unverified) from its id_token — see
// uid-dp-velocity-engine-control-plane-frontend's src/services/apiClient.js.
// There is currently no server-side proof this header wasn't forged; only
// the RBAC permission check downstream (RequirePermission) is a real
// access-control boundary.
//
// This mirrors fraud-investigation-system's (Prahari) documented approach —
// see that repo's auth/README.md — adopted for the same reason Prahari
// adopted it: this backend's previous implementation (real RS256 signature
// verification against WSO2's JWKS, resolvable via git history) could not
// reach https://sso.uidai.net.in/oauth2/jwks from this cluster's backend
// pod network in prod (TLS handshake timeout, then EOF — a network-path
// problem, not a code or credentials bug). Revisit once that reachability
// is fixed; the JWKS-based version is recoverable from git history if
// needed.
//
// Resolves the trusted "sub" to a local users.id via wso2Resolver. Whether
// an unrecognized subject is rejected or auto-provisioned depends entirely
// on which resolver cmd/server wired in — this middleware doesn't know or
// care which; see services.GetOrProvisionUserByExternalSubject for the
// JIT-provisioning behavior actually wired in today, and its own doc comment
// for the compensating controls that make auto-provisioning against this
// unverified header defensible.
func IdentityMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if wso2Resolver == nil {
			// Deployment wiring bug (SetIdentityResolver was never called),
			// not a request error.
			slog.Error("IdentityMiddleware used before SetIdentityResolver was called")
			c.AbortWithStatusJSON(http.StatusNotImplemented, gin.H{
				"detail": "Authentication is not yet configured on this deployment",
			})
			return
		}

		sub := strings.TrimSpace(c.GetHeader("X-User-Subject"))
		if sub == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"detail": "Missing X-User-Subject header",
			})
			return
		}

		userID, status, err := wso2Resolver(c.Request.Context(), sub, c.ClientIP())
		if err != nil {
			if errors.Is(err, ErrIdentityNotFound) {
				slog.Warn("Identity rejected: external_subject not found in local users table", "received_sub", sub)
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
					"detail": "User not authorized: no local account for this identity",
				})
				return
			}
			slog.Error("Failed to resolve WSO2 subject to local user", "error", err)
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"detail": "Authorization service temporarily unavailable",
			})
			return
		}
		if status != "ACTIVE" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"detail": "Account is disabled"})
			return
		}

		c.Set(ContextKeyUserID, userID)
		c.Next()
	}
}

// PermissionResolver resolves a user's roles and flat permission set. Matches
// services.GetUserPermissions's signature — injected rather than imported
// directly so RequirePermission is unit-testable against a fake resolver
// without a real MySQL connection.
type PermissionResolver func(ctx context.Context, userID int64) (roles []string, perms map[string]bool, err error)

// LedTeamsResolver resolves which team IDs a user leads. Matches
// services.GetLedTeamIDs's signature — injected for the same reason
// PermissionResolver is (keeps this package free of internal/services'
// go-duckdb/cgo dependency). Used by RequireAdminAccess to decide whether a
// non-iam:manage user still belongs in the Admin Panel as a team lead.
type LedTeamsResolver func(ctx context.Context, userID int64) ([]int64, error)

// AuthMiddleware holds the resolvers used to build RequirePermission/
// RequireAdminAccess guards. Construct once at startup with
// services.GetUserPermissions and services.GetLedTeamIDs.
type AuthMiddleware struct {
	Resolver         PermissionResolver
	LedTeamsResolver LedTeamsResolver
}

// NewAuthMiddleware constructs an AuthMiddleware bound to the given resolvers.
func NewAuthMiddleware(resolver PermissionResolver, ledTeamsResolver LedTeamsResolver) *AuthMiddleware {
	return &AuthMiddleware{Resolver: resolver, LedTeamsResolver: ledTeamsResolver}
}

// RequirePermission returns middleware that aborts with 403 unless the
// current request's user has the given resource:action permission. Must run
// after IdentityMiddleware (it reads ContextKeyUserID, set there).
//
// Reusable across any route: RequirePermission("rules", "publish"),
// RequirePermission("rules", "delete"), RequirePermission("iam", "manage"), ...
func (m *AuthMiddleware) RequirePermission(resource, action string) gin.HandlerFunc {
	required := resource + ":" + action
	return func(c *gin.Context) {
		uidRaw, exists := c.Get(ContextKeyUserID)
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"detail": "Authentication required"})
			return
		}
		userID, ok := uidRaw.(int64)
		if !ok {
			// Defensive: only IdentityMiddleware should ever set this key.
			slog.Error("auth_user_id in context has unexpected type", "value", uidRaw)
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"detail": "Internal authorization error"})
			return
		}

		_, perms, err := m.Resolver(c.Request.Context(), userID)
		if err != nil {
			slog.Error("Permission resolution failed", "user_id", userID, "required", required, "error", err)
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"detail": "Authorization service temporarily unavailable",
			})
			return
		}

		if !perms[required] {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"detail": "Insufficient permissions: requires " + required,
			})
			return
		}

		c.Next()
	}
}

// RequireAdminAccess gates the /admin/* route group: full access via
// iam:manage (SUPER_ADMIN today), or scoped access via leading at least one
// team. This only answers "does this user belong in the Admin Panel at
// all" — the fine-grained scoping (which users/teams/audit entries a
// non-iam:manage actor can actually see or mutate) happens in
// internal/services/admin.go and internal/handlers/admin.go, since it's
// data-dependent (which team) in a way a static per-route guard can't express.
func (m *AuthMiddleware) RequireAdminAccess() gin.HandlerFunc {
	return func(c *gin.Context) {
		uidRaw, exists := c.Get(ContextKeyUserID)
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"detail": "Authentication required"})
			return
		}
		userID, ok := uidRaw.(int64)
		if !ok {
			slog.Error("auth_user_id in context has unexpected type", "value", uidRaw)
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"detail": "Internal authorization error"})
			return
		}

		_, perms, err := m.Resolver(c.Request.Context(), userID)
		if err != nil {
			slog.Error("Permission resolution failed", "user_id", userID, "error", err)
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"detail": "Authorization service temporarily unavailable",
			})
			return
		}
		if perms["iam:manage"] {
			c.Next()
			return
		}

		led, err := m.LedTeamsResolver(c.Request.Context(), userID)
		if err != nil {
			slog.Error("Team-lead resolution failed", "user_id", userID, "error", err)
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"detail": "Authorization service temporarily unavailable",
			})
			return
		}
		if len(led) == 0 {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"detail": "Insufficient permissions: requires iam:manage or team leadership",
			})
			return
		}

		c.Next()
	}
}
