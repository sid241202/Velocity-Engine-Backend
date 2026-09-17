package handlers

import (
	"errors"
	"net/http"
	"strconv"

	"velocity-engine-control-plane-backend-go/internal/middleware"
	"velocity-engine-control-plane-backend-go/internal/models"
	"velocity-engine-control-plane-backend-go/internal/services"

	"github.com/gin-gonic/gin"
)

// AdminHandler serves the Admin Panel's /admin/* routes. Route-level access
// (RequirePermission("iam", "manage")) fully gates this group — SUPER_ADMIN
// only — so every handler below is a thin call-and-respond wrapper with no
// further per-target scoping to apply.
type AdminHandler struct{}

func NewAdminHandler() *AdminHandler { return &AdminHandler{} }

// actorID reads the identity IdentityMiddleware resolved earlier in the
// chain. Every route in this file runs after IdentityMiddleware +
// RequirePermission("iam", "manage"), so this should never actually miss in
// production — the type-assertion failure path is defensive, matching
// RequirePermission's own handling of the same context key.
func actorID(c *gin.Context) (int64, bool) {
	uidRaw, exists := c.Get(middleware.ContextKeyUserID)
	if !exists {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"detail": "Authentication required"})
		return 0, false
	}
	userID, ok := uidRaw.(int64)
	if !ok {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"detail": "Internal authorization error"})
		return 0, false
	}
	return userID, true
}

func (h *AdminHandler) ListUsers(c *gin.Context) {
	users, err := services.ListUsers(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Failed to list users"})
		return
	}
	c.JSON(http.StatusOK, users)
}

func (h *AdminHandler) ListRoles(c *gin.Context) {
	roles, err := services.ListRoles(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Failed to list roles"})
		return
	}
	c.JSON(http.StatusOK, roles)
}

func (h *AdminHandler) ListAuditLog(c *gin.Context) {
	entries, err := services.ListAuditLog(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Failed to list audit log"})
		return
	}
	c.JSON(http.StatusOK, entries)
}

func (h *AdminHandler) UpdateUser(c *gin.Context) {
	uid, ok := actorID(c)
	if !ok {
		return
	}
	targetID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid user id"})
		return
	}

	var req models.AdminUserPatch
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request body: " + err.Error()})
		return
	}

	updated, err := services.UpdateUser(c.Request.Context(), uid, targetID, services.UserPatch{
		Role: req.Role, Status: req.Status,
	})
	writeAdminMutationResult(c, updated, err)
}

// writeAdminMutationResult maps every sentinel error the admin service layer
// can return to its HTTP status — the single place that mapping lives, so
// each handler above stays a thin call-and-respond wrapper.
func writeAdminMutationResult(c *gin.Context, result interface{}, err error) {
	if err == nil {
		c.JSON(http.StatusOK, result)
		return
	}
	switch {
	case errors.Is(err, services.ErrUserNotFound):
		c.JSON(http.StatusNotFound, gin.H{"detail": err.Error()})
	case errors.Is(err, services.ErrCannotEditSelf), errors.Is(err, services.ErrLastActiveSuperAdmin):
		c.JSON(http.StatusForbidden, gin.H{"detail": err.Error()})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Admin Panel action failed"})
	}
}
