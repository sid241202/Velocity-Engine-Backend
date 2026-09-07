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
// (middleware.RequireAdminAccess: iam:manage OR any team leadership) only
// answers "does this caller belong here at all" — every handler below still
// resolves the caller's own scope (isSuperAdmin / led team IDs) itself and
// asks internal/services/admin.go to enforce or apply it, since that scoping
// is data-dependent per caller and per target, not a static route gate.
type AdminHandler struct{}

func NewAdminHandler() *AdminHandler { return &AdminHandler{} }

// actorID reads the identity IdentityMiddleware resolved earlier in the
// chain. Every route in this file runs after IdentityMiddleware +
// RequireAdminAccess, so this should never actually miss in production —
// the type-assertion failure path is defensive, matching RequirePermission's
// own handling of the same context key.
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

// actorScope resolves whether the caller has full (iam:manage) access, and
// if not, which teams they lead — the two pieces of context every list/
// mutate handler below needs to decide what to show or allow.
func actorScope(c *gin.Context, userID int64) (isSuperAdmin bool, ledTeamIDs []int64, ok bool) {
	_, perms, err := services.GetUserPermissions(c.Request.Context(), userID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"detail": "Authorization service temporarily unavailable"})
		return false, nil, false
	}
	if perms["iam:manage"] {
		return true, nil, true
	}
	led, err := services.GetLedTeamIDs(c.Request.Context(), userID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"detail": "Authorization service temporarily unavailable"})
		return false, nil, false
	}
	return false, led, true
}

// scopedTeamIDsFor returns nil (unrestricted) for a super admin, or the
// caller's led teams PLUS the MISC default team for a lead — matching
// exactly the scope services.UpdateUser enforces ("your team(s) + the
// unassigned pool"). Used as the filter for ListUsers/ListTeams/
// ListAuditLog; without folding MISC in here too, a lead's user list would
// silently exclude the very unassigned users they're supposed to be able to
// claim into their team.
func scopedTeamIDsFor(c *gin.Context, isSuperAdmin bool, ledTeamIDs []int64) ([]int64, bool) {
	if isSuperAdmin {
		return nil, true
	}
	miscID, err := services.GetDefaultTeamID(c.Request.Context())
	if err != nil {
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"detail": "Authorization service temporarily unavailable"})
		return nil, false
	}
	return append(append([]int64{}, ledTeamIDs...), miscID), true
}

func (h *AdminHandler) ListUsers(c *gin.Context) {
	uid, ok := actorID(c)
	if !ok {
		return
	}
	isSuperAdmin, led, ok := actorScope(c, uid)
	if !ok {
		return
	}
	scope, ok := scopedTeamIDsFor(c, isSuperAdmin, led)
	if !ok {
		return
	}
	users, err := services.ListUsers(c.Request.Context(), scope)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Failed to list users"})
		return
	}
	c.JSON(http.StatusOK, users)
}

func (h *AdminHandler) ListTeams(c *gin.Context) {
	uid, ok := actorID(c)
	if !ok {
		return
	}
	isSuperAdmin, led, ok := actorScope(c, uid)
	if !ok {
		return
	}
	scope, ok := scopedTeamIDsFor(c, isSuperAdmin, led)
	if !ok {
		return
	}
	teams, err := services.ListTeams(c.Request.Context(), scope)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Failed to list teams"})
		return
	}
	c.JSON(http.StatusOK, teams)
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
	uid, ok := actorID(c)
	if !ok {
		return
	}
	isSuperAdmin, led, ok := actorScope(c, uid)
	if !ok {
		return
	}
	scope, ok := scopedTeamIDsFor(c, isSuperAdmin, led)
	if !ok {
		return
	}
	entries, err := services.ListAuditLog(c.Request.Context(), scope)
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
		Role: req.Role, TeamID: req.TeamID, Status: req.Status,
	})
	writeAdminMutationResult(c, updated, err)
}

func (h *AdminHandler) CreateTeam(c *gin.Context) {
	uid, ok := actorID(c)
	if !ok {
		return
	}
	var req models.AdminCreateTeamRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request body: " + err.Error()})
		return
	}
	team, err := services.CreateTeam(c.Request.Context(), uid, req.Name, req.Description)
	writeAdminMutationResult(c, team, err)
}

func (h *AdminHandler) GrantTeamLead(c *gin.Context) {
	uid, ok := actorID(c)
	if !ok {
		return
	}
	teamID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid team id"})
		return
	}
	var req models.AdminGrantLeadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request body: " + err.Error()})
		return
	}
	err = services.GrantTeamLead(c.Request.Context(), uid, teamID, req.UserID)
	writeAdminMutationResult(c, gin.H{"ok": true}, err)
}

func (h *AdminHandler) RevokeTeamLead(c *gin.Context) {
	uid, ok := actorID(c)
	if !ok {
		return
	}
	teamID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid team id"})
		return
	}
	userID, err := strconv.ParseInt(c.Param("userId"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid user id"})
		return
	}
	err = services.RevokeTeamLead(c.Request.Context(), uid, teamID, userID)
	writeAdminMutationResult(c, gin.H{"ok": true}, err)
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
	case errors.Is(err, services.ErrUserNotFound), errors.Is(err, services.ErrTeamNotFound):
		c.JSON(http.StatusNotFound, gin.H{"detail": err.Error()})
	case errors.Is(err, services.ErrCannotEditSelf), errors.Is(err, services.ErrForbiddenScope), errors.Is(err, services.ErrLastActiveSuperAdmin):
		c.JSON(http.StatusForbidden, gin.H{"detail": err.Error()})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "Admin Panel action failed"})
	}
}
