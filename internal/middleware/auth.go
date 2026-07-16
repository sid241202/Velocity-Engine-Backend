// Package middleware provides the authorization layer for the control plane:
// resolving the current request's identity and enforcing per-endpoint
// permission requirements against the RBAC store.
package middleware

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"velocity-engine-control-plane-backend-go/internal/config"

	"github.com/gin-gonic/gin"
)

// ContextKeyUserID is the gin.Context key IdentityMiddleware sets and
// RequirePermission reads. Exported so handlers (e.g. GET /me) can read the
// resolved identity directly.
const ContextKeyUserID = "auth_user_id"

// WSO2TokenValidator validates a raw bearer token against WSO2's JWKS and
// returns its "sub" claim. Matches services.ValidateWSO2Token's signature.
type WSO2TokenValidator func(ctx context.Context, rawToken string) (sub string, err error)

// ExternalSubjectResolver resolves a WSO2 "sub" claim to a local user ID and
// status. Must return ErrIdentityNotFound (not services.ErrUserNotFound —
// this package doesn't import internal/services) when the subject has no
// matching local user; the wiring in cmd/server translates between the two.
type ExternalSubjectResolver func(ctx context.Context, sub string) (userID int64, status string, err error)

// ErrIdentityNotFound is the sentinel an ExternalSubjectResolver must return
// when a WSO2 subject has no matching local user — distinguishes
// "reject, not provisioned" (401) from a genuine resolver failure (503).
var ErrIdentityNotFound = errors.New("identity not found")

// wso2Validator/wso2Resolver back the "wso2" AuthMode path. Set once at
// startup via SetWSO2Dependencies — injected rather than imported directly
// (internal/services is not imported by this package at all) for the same
// reason PermissionResolver below is injected: internal/services pulls in
// go-duckdb (cgo) via duckdb.go, and this package needs to stay buildable/
// vettable/testable without that dependency.
var (
	wso2Validator WSO2TokenValidator
	wso2Resolver  ExternalSubjectResolver
)

// SetWSO2Dependencies wires the "wso2" AuthMode path. Call once at startup
// (cmd/server, which already imports internal/services for
// NewAuthMiddleware) — a no-op if AuthMode is "dev".
func SetWSO2Dependencies(validator WSO2TokenValidator, resolver ExternalSubjectResolver) {
	wso2Validator = validator
	wso2Resolver = resolver
}

// IdentityMiddleware resolves the current request's user ID and stores it in
// the gin context under ContextKeyUserID. Branches on config.AuthMode:
//
//   - "wso2": real identity. Validates the Authorization: Bearer token against
//     WSO2's JWKS (wso2Validator — real signature verification, not a
//     decode-only check), then resolves the token's "sub" claim to a local
//     users.id via wso2Resolver. An unrecognized subject is rejected (401),
//     not auto-provisioned — matching the deny-until-provisioned model
//     confirmed in the demo-wso2 plan's reference-backend research; admins
//     still create users/user_roles rows manually, same as before.
//   - "dev": the TEMPORARY pre-WSO2 identity shim — a plain X-Debug-User-Id
//     header, gated by config.AuthDevMode (which config.go's boot-time panic
//     already refuses to allow when ENV=prod). Kept, deliberately, as a local
//     dev/demo fallback for when a live WSO2 instance isn't reachable.
//
// Any other/unset AuthMode is a deployment misconfiguration, not a request
// error — respond 501, not 401.
func IdentityMiddleware() gin.HandlerFunc {
	switch config.AuthMode {
	case "wso2":
		return wso2IdentityMiddleware()
	case "dev":
		return devIdentityMiddleware()
	default:
		return func(c *gin.Context) {
			c.AbortWithStatusJSON(http.StatusNotImplemented, gin.H{
				"detail": "Authentication is not yet configured on this deployment",
			})
		}
	}
}

// devIdentityMiddleware is the TEMPORARY pre-WSO2 identity shim described
// above. Not a real auth mechanism — see IdentityMiddleware's doc comment.
func devIdentityMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !config.AuthDevMode {
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

// wso2IdentityMiddleware validates a real WSO2-issued bearer token and
// resolves its subject to a local user. See IdentityMiddleware's doc comment.
func wso2IdentityMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if wso2Validator == nil || wso2Resolver == nil {
			// Deployment wiring bug (SetWSO2Dependencies was never called),
			// not a request error — respond the same way as an unconfigured
			// AuthMode rather than panicking on a nil function call.
			slog.Error("AuthMode=wso2 but SetWSO2Dependencies was never called")
			c.AbortWithStatusJSON(http.StatusNotImplemented, gin.H{
				"detail": "Authentication is not yet configured on this deployment",
			})
			return
		}

		header := c.GetHeader("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(header, prefix) || len(header) <= len(prefix) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"detail": "Missing or malformed Authorization: Bearer <token> header",
			})
			return
		}
		rawToken := strings.TrimPrefix(header, prefix)

		sub, err := wso2Validator(c.Request.Context(), rawToken)
		if err != nil {
			slog.Warn("WSO2 token validation failed", "error", err)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"detail": "Invalid or expired token"})
			return
		}

		userID, status, err := wso2Resolver(c.Request.Context(), sub)
		if err != nil {
			if errors.Is(err, ErrIdentityNotFound) {
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
