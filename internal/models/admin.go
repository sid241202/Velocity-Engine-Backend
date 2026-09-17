package models

// AdminUserView is the Admin Panel's joined view of a user — unlike the raw
// User table-mirror in rbac.go, this includes the user's single resolved
// role (via user_roles -> roles), exactly the shape
// src/components/AdminPanel/UserTable.jsx on the frontend expects.
type AdminUserView struct {
	ID          int64  `json:"id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
	Role        string `json:"role"`
	Status      string `json:"status"`
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

// AdminUserPatch is the request body for PATCH /admin/users/:id.
type AdminUserPatch struct {
	Role   string `json:"role" binding:"required"`
	Status string `json:"status" binding:"required"`
}
