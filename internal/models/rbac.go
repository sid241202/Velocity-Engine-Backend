package models

import "time"

// User mirrors the `users` table.
type User struct {
	ID              int64     `json:"id"`
	ExternalSubject string    `json:"external_subject"`
	Email           string    `json:"email"`
	DisplayName     string    `json:"display_name"`
	Status          string    `json:"status"` // ACTIVE | DISABLED
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// Role mirrors the `roles` table.
type Role struct {
	ID          int32     `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	IsSystem    bool      `json:"is_system"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Permission mirrors the `permissions` table.
type Permission struct {
	ID          int32  `json:"id"`
	Resource    string `json:"resource"`
	Action      string `json:"action"`
	Description string `json:"description,omitempty"`
}

// Key returns the canonical "resource:action" string used as the map key
// throughout the RBAC service and middleware.
func (p Permission) Key() string { return p.Resource + ":" + p.Action }

// AuditLogEntry mirrors the `audit_log` table.
type AuditLogEntry struct {
	ID          int64                  `json:"id"`
	ActorUserID *int64                 `json:"actor_user_id,omitempty"`
	Action      string                 `json:"action"`
	TargetType  string                 `json:"target_type"`
	TargetID    string                 `json:"target_id"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
}

// MeResponse is returned by GET /me — the frontend's one-time authorization
// context hydration payload.
type MeResponse struct {
	UserID      int64    `json:"user_id"`
	Roles       []string `json:"roles"`
	Permissions []string `json:"permissions"`  // flat "resource:action" strings
	LedTeamIDs  []int64  `json:"led_team_ids"` // teams this user leads (team_leads rows) — see internal/services/admin.go
}
