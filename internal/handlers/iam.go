package handlers

import (
	"net/http"

	"velocity-engine-control-plane-backend-go/internal/middleware"
	"velocity-engine-control-plane-backend-go/internal/models"
	"velocity-engine-control-plane-backend-go/internal/services"

	"github.com/gin-gonic/gin"
)

// IAMHandler handles identity/authorization endpoints.
type IAMHandler struct{}

// NewIAMHandler creates a new IAMHandler.
func NewIAMHandler() *IAMHandler {
	return &IAMHandler{}
}

// Me handles GET /me — returns the current user's roles and flat permission
// list. The frontend calls this once at bootstrap to hydrate its
// authorization context (which UI elements to show/hide); the backend
// independently re-checks every permission server-side regardless of what
// this response says, via RequirePermission on each protected route.
func (h *IAMHandler) Me(c *gin.Context) {
	uidRaw, exists := c.Get(middleware.ContextKeyUserID)
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"detail": "Authentication required"})
		return
	}
	userID, ok := uidRaw.(int64)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Internal authorization error"})
		return
	}

	roles, permSet, err := services.GetUserPermissions(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Authorization service temporarily unavailable"})
		return
	}

	permList := make([]string, 0, len(permSet))
	for k := range permSet {
		permList = append(permList, k)
	}

	c.JSON(http.StatusOK, models.MeResponse{
		UserID:      userID,
		Roles:       roles,
		Permissions: permList,
	})
}
