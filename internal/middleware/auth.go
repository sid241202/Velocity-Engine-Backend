// Package middleware provides the authorization layer for the control plane:
// resolving the current request's identity and enforcing per-endpoint
// permission requirements against the RBAC store.
package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"

	"velocity-engine-control-plane-backend-go/internal/config"

	"github.com/gin-gonic/gin"
)

// ContextKeyUserID is the gin.Context key IdentityMiddleware sets and
// RequirePermission reads. Exported so handlers (e.g. GET /me) can read the
// resolved identity directly.
const ContextKeyUserID = "auth_user_id"

// IdentityMiddleware resolves the current request's user ID and stores it in
// the gin context under ContextKeyUserID.
//
// ══════════════════════════════════════════════════════════════════════════
// TEMPORARY PRE-WSO2 IDENTITY SHIM.
//
// Real authentication (WSO2/OIDC token validation, extracting the 'sub'
// claim and mapping it to users.external_subject) is explicitly out of scope
// for this task — "ignore the WSO2/OAuth phase for now, assume the user is
// already authenticated." Until that lands, identity is resolved from a
// plain X-Debug-User-Id header, and ONLY when config.AuthDevMode is true.
//
// This is not a real auth mechanism and must be replaced by JWT/session
// validation before any non-development deployment. config.go already
// panics at boot if ENV=prod and AuthDevMode is left enabled — that's the
// hard safety net; this comment is so nobody mistakes this function for real
// auth when reading it in isolation.
// ══════════════════════════════════════════════════════════════════════════
func IdentityMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !config.AuthDevMode {
			// Correctly 501, not 401: there is currently no identity
			// resolution mechanism at all (real auth isn't built yet), as
			// opposed to "you sent a request but didn't authenticate."
			c.AbortWithStatusJSON(http.StatusNotImplemented, gin.H{
				"detail": "Authentication is not yet configured on this deployment",
			})
			return
		}

		raw := c.GetHeader("X-Debug-User-Id")
		if raw == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"detail": "Missing X-Debug-User-Id (development identity header)",
			})
			return
		}
		userID, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"detail": "Invalid X-Debug-User-Id"})
			return
		}

		c.Set(ContextKeyUserID, userID)
		c.Next()
	}
}

// PermissionResolver resolves a user's roles and flat permission set. Matches
// services.GetUserPermissions's signature — injected rather than imported
// directly so RequirePermission is unit-testable against a fake resolver
// rather than the real one.
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
