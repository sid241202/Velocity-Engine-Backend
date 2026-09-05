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

// ExternalSubjectResolver resolves a WSO2 "sub" claim to a local user ID and
// status. Must return ErrIdentityNotFound (not services.ErrUserNotFound —
// this package doesn't import internal/services) when the subject has no
// matching local user; the wiring in cmd/server translates between the two.
type ExternalSubjectResolver func(ctx context.Context, sub string) (userID int64, status string, err error)

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
// Resolves the trusted "sub" to a local users.id via wso2Resolver. An
// unrecognized subject is rejected (401), not auto-provisioned — matching
// the deny-until-provisioned model confirmed in the demo-wso2 plan's
// reference-backend research; admins still create users/user_roles rows
// manually.
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

		userID, status, err := wso2Resolver(c.Request.Context(), sub)
        if err != nil {
            if errors.Is(err, ErrIdentityNotFound) {
                // ADD THIS LOG LINE:
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

// AuthMiddleware holds the permission resolver used to build RequirePermission
// guards. Construct once at startup with services.GetUserPermissions.
type AuthMiddleware struct {
	Resolver PermissionResolver
}

// NewAuthMiddleware constructs an AuthMiddleware bound to the given resolver.
func NewAuthMiddleware(resolver PermissionResolver) *AuthMiddleware {
	return &AuthMiddleware{Resolver: resolver}
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
