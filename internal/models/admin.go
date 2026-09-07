package models

// AdminUserView is the Admin Panel's joined view of a user — unlike the raw
// User table-mirror in rbac.go, this includes the user's single resolved
// role (via user_roles -> roles) and team_id, exactly the shape
// src/components/AdminPanel/UserTable.jsx on the frontend expects.
type AdminUserView struct {
	ID          int64  `json:"id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
	Role        string `json:"role"`
	TeamID      *int64 `json:"team_id"`
	Status      string `json:"status"`
}

// AdminTeam is a team plus which user IDs currently lead it (team_leads
// rows) — combined here because the frontend always needs both together.
type AdminTeam struct {
	ID          int64   `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	IsDefault   bool    `json:"is_default"`
	LeadUserIDs []int64 `json:"lead_user_ids"`
}

// AdminRoleView is a role plus its flat "resource:action" permission list —
// the Role Reference panel's plain-language source of truth, resolved live
// from role_permissions rather than hardcoded, so it can't drift from
// whatever the seed data actually says.
type AdminRoleView struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Permissions []string `json:"permissions"`
}

// AdminUserPatch is the request body for PATCH /admin/users/:id. TeamID is a
// pointer so JSON `null` (SUPER_ADMIN, who has no team) is distinguishable
// from a real team id — every field is required regardless, matching what
// the frontend's adminApi.js always sends together.
type AdminUserPatch struct {
	Role   string `json:"role" binding:"required"`
	TeamID *int64 `json:"team_id"`
	Status string `json:"status" binding:"required"`
}

// AdminCreateTeamRequest is the request body for POST /admin/teams.
type AdminCreateTeamRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
}

// AdminGrantLeadRequest is the request body for POST /admin/teams/:id/leads.
type AdminGrantLeadRequest struct {
	UserID int64 `json:"user_id" binding:"required"`
}
