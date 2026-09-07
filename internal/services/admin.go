// Admin Panel service layer: user/team/role/audit-log listing and the
// team-scoped mutations (role/team/status changes, team creation, lead
// grant/revoke). This is the real authorization boundary for all of it —
// internal/handlers/admin.go is a thin JSON-in/JSON-out wrapper, and the
// frontend's own scoping (hiding controls a lead shouldn't see) is a UX
// convenience only, same posture as the rest of this codebase's RBAC.
package services

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velocity-engine-control-plane-backend-go/internal/metrics"
	"velocity-engine-control-plane-backend-go/internal/models"
)

var (
	// ErrCannotEditSelf/ErrForbiddenScope/ErrLastActiveSuperAdmin are all
	// mapped to 403 by the handler — distinct sentinels so each can carry
	// its own message without the handler needing to inspect error text.
	ErrCannotEditSelf       = errors.New("cannot edit your own account through the admin panel")
	ErrForbiddenScope       = errors.New("target is outside your administrative scope")
	ErrLastActiveSuperAdmin = errors.New("cannot demote or disable the last active super admin")
	ErrTeamNotFound         = errors.New("team not found")
)

// UserPatch is PATCH /admin/users/:id's decoded body. TeamID nil means
// SUPER_ADMIN (no team); every field is otherwise required, matching what
// the frontend's adminApi.js always sends together.
type UserPatch struct {
	Role   string
	TeamID *int64
	Status string
}

// adminUserRow is the internal (role, team_id, status) shape used by the
// scope-checking logic below — a small subset of AdminUserView.
type adminUserRow struct {
	ID     int64
	Role   string
	TeamID *int64
	Status string
}

func getAdminUserRow(ctx context.Context, db *sql.DB, userID int64) (*adminUserRow, error) {
	row := &adminUserRow{ID: userID}
	err := db.QueryRowContext(ctx, `
		SELECT u.team_id, u.status, COALESCE(r.name, '')
		FROM users u
		LEFT JOIN user_roles ur ON ur.user_id = u.id
		LEFT JOIN roles r ON r.id = ur.role_id
		WHERE u.id = ?
	`, userID).Scan(&row.TeamID, &row.Status, &row.Role)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrUserNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to look up user: %w", err)
	}
	return row, nil
}

func getDefaultTeamID(ctx context.Context, db *sql.DB) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx, "SELECT id FROM teams WHERE is_default = TRUE LIMIT 1").Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to resolve default team: %w", err)
	}
	return id, nil
}

// GetDefaultTeamID returns the id of the seeded default (MISC) team.
// Exported for internal/handlers/admin.go, which needs it to fold MISC into
// a non-super-admin actor's scope for ListUsers/ListTeams/ListAuditLog —
// the same "your team(s) + the unassigned pool" scope UpdateUser enforces.
func GetDefaultTeamID(ctx context.Context) (int64, error) {
	db, err := getMySQLDB()
	if err != nil {
		return 0, fmt.Errorf("mysql unavailable: %w", err)
	}
	return getDefaultTeamID(ctx, db)
}

func countActiveSuperAdmins(ctx context.Context, db *sql.DB) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM users u
		JOIN user_roles ur ON ur.user_id = u.id
		JOIN roles r ON r.id = ur.role_id
		WHERE r.name = 'SUPER_ADMIN' AND u.status = 'ACTIVE'
	`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count active super admins: %w", err)
	}
	return count, nil
}

func containsInt64(haystack []int64, needle int64) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}

// ListUsers returns every user, or (when scopedTeamIDs is non-nil) only
// users whose team_id is in that set — the real, server-side half of "a
// team lead sees their team + the unassigned pool," not just the frontend
// choosing not to render the rest.
func ListUsers(ctx context.Context, scopedTeamIDs []int64) ([]models.AdminUserView, error) {
	start := time.Now()
	users, err := listUsersImpl(ctx, scopedTeamIDs)
	metrics.MySQLQueryDuration.WithLabelValues("admin_list_users").Observe(time.Since(start).Seconds())
	return users, err
}

func listUsersImpl(ctx context.Context, scopedTeamIDs []int64) ([]models.AdminUserView, error) {
	db, err := getMySQLDB()
	if err != nil {
		return nil, fmt.Errorf("mysql unavailable: %w", err)
	}

	query := `
		SELECT u.id, u.email, u.display_name, u.team_id, u.status, COALESCE(r.name, '')
		FROM users u
		LEFT JOIN user_roles ur ON ur.user_id = u.id
		LEFT JOIN roles r ON r.id = ur.role_id`
	args := []interface{}{}
	if scopedTeamIDs != nil {
		placeholders := make([]string, len(scopedTeamIDs))
		for i, id := range scopedTeamIDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		if len(placeholders) == 0 {
			return []models.AdminUserView{}, nil // scoped to nothing — a lead who somehow leads no team
		}
		query += " WHERE u.team_id IN (" + strings.Join(placeholders, ",") + ")"
	}
	query += " ORDER BY u.id"

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
	}
	defer rows.Close()

	users := make([]models.AdminUserView, 0)
	for rows.Next() {
		var u models.AdminUserView
		if err := rows.Scan(&u.ID, &u.Email, &u.DisplayName, &u.TeamID, &u.Status, &u.Role); err != nil {
			return nil, fmt.Errorf("failed to scan user row: %w", err)
		}
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return users, nil
}

// ListTeams returns every team, or (scopedTeamIDs non-nil) only the given
// ones — same server-side scoping rationale as ListUsers.
func ListTeams(ctx context.Context, scopedTeamIDs []int64) ([]models.AdminTeam, error) {
	start := time.Now()
	teams, err := listTeamsImpl(ctx, scopedTeamIDs)
	metrics.MySQLQueryDuration.WithLabelValues("admin_list_teams").Observe(time.Since(start).Seconds())
	return teams, err
}

func listTeamsImpl(ctx context.Context, scopedTeamIDs []int64) ([]models.AdminTeam, error) {
	db, err := getMySQLDB()
	if err != nil {
		return nil, fmt.Errorf("mysql unavailable: %w", err)
	}

	query := "SELECT id, name, description, is_default FROM teams"
	args := []interface{}{}
	if scopedTeamIDs != nil {
		placeholders := make([]string, len(scopedTeamIDs))
		for i, id := range scopedTeamIDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		if len(placeholders) == 0 {
			return []models.AdminTeam{}, nil
		}
		query += " WHERE id IN (" + strings.Join(placeholders, ",") + ")"
	}
	query += " ORDER BY id"

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list teams: %w", err)
	}
	teams := make([]models.AdminTeam, 0)
	teamByID := make(map[int64]*models.AdminTeam)
	for rows.Next() {
		var t models.AdminTeam
		var desc sql.NullString
		if err := rows.Scan(&t.ID, &t.Name, &desc, &t.IsDefault); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan team row: %w", err)
		}
		t.Description = desc.String
		t.LeadUserIDs = []int64{}
		teams = append(teams, t)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	rows.Close()
	for i := range teams {
		teamByID[teams[i].ID] = &teams[i]
	}

	if len(teams) == 0 {
		return teams, nil
	}
	leadRows, err := db.QueryContext(ctx, "SELECT team_id, user_id FROM team_leads")
	if err != nil {
		return nil, fmt.Errorf("failed to list team leads: %w", err)
	}
	defer leadRows.Close()
	for leadRows.Next() {
		var teamID, userID int64
		if err := leadRows.Scan(&teamID, &userID); err != nil {
			return nil, fmt.Errorf("failed to scan team_leads row: %w", err)
		}
		if t, ok := teamByID[teamID]; ok {
			t.LeadUserIDs = append(t.LeadUserIDs, userID)
		}
	}
	if err := leadRows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return teams, nil
}

// ListRoles resolves each role's permission set live from role_permissions
// rather than hardcoding it, so the Role Reference panel can't drift from
// whatever the seed data actually says.
func ListRoles(ctx context.Context) ([]models.AdminRoleView, error) {
	start := time.Now()
	roles, err := listRolesImpl(ctx)
	metrics.MySQLQueryDuration.WithLabelValues("admin_list_roles").Observe(time.Since(start).Seconds())
	return roles, err
}

func listRolesImpl(ctx context.Context) ([]models.AdminRoleView, error) {
	db, err := getMySQLDB()
	if err != nil {
		return nil, fmt.Errorf("mysql unavailable: %w", err)
	}

	rows, err := db.QueryContext(ctx, `
		SELECT r.id, r.name, COALESCE(r.description, ''), p.resource, p.action
		FROM roles r
		LEFT JOIN role_permissions rp ON rp.role_id = r.id
		LEFT JOIN permissions p ON p.id = rp.permission_id
		ORDER BY r.id
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list roles: %w", err)
	}
	defer rows.Close()

	order := make([]string, 0)
	byName := make(map[string]*models.AdminRoleView)
	for rows.Next() {
		var roleID int64
		var name, description string
		var resource, action sql.NullString
		if err := rows.Scan(&roleID, &name, &description, &resource, &action); err != nil {
			return nil, fmt.Errorf("failed to scan role row: %w", err)
		}
		role, ok := byName[name]
		if !ok {
			role = &models.AdminRoleView{Name: name, Description: description, Permissions: []string{}}
			byName[name] = role
			order = append(order, name)
		}
		if resource.Valid && action.Valid {
			role.Permissions = append(role.Permissions, resource.String+":"+action.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	result := make([]models.AdminRoleView, 0, len(order))
	for _, name := range order {
		result = append(result, *byName[name])
	}
	return result, nil
}

// ListAuditLog returns the most recent 200 entries, or (scopedTeamIDs
// non-nil) only entries targeting a user currently in one of those teams —
// a super admin sees everything; a team lead sees only what touched their
// own team's members (including while those members were in MISC).
func ListAuditLog(ctx context.Context, scopedTeamIDs []int64) ([]models.AuditLogEntry, error) {
	start := time.Now()
	entries, err := listAuditLogImpl(ctx, scopedTeamIDs)
	metrics.MySQLQueryDuration.WithLabelValues("admin_list_audit_log").Observe(time.Since(start).Seconds())
	return entries, err
}

func listAuditLogImpl(ctx context.Context, scopedTeamIDs []int64) ([]models.AuditLogEntry, error) {
	db, err := getMySQLDB()
	if err != nil {
		return nil, fmt.Errorf("mysql unavailable: %w", err)
	}

	var rows *sql.Rows
	if scopedTeamIDs == nil {
		rows, err = db.QueryContext(ctx, `
			SELECT id, actor_user_id, action, target_type, target_id, metadata, created_at
			FROM audit_log ORDER BY id DESC LIMIT 200`)
	} else if len(scopedTeamIDs) == 0 {
		return []models.AuditLogEntry{}, nil
	} else {
		placeholders := make([]string, len(scopedTeamIDs))
		args := make([]interface{}, len(scopedTeamIDs))
		for i, id := range scopedTeamIDs {
			placeholders[i] = "?"
			args[i] = id
		}
		query := `
			SELECT a.id, a.actor_user_id, a.action, a.target_type, a.target_id, a.metadata, a.created_at
			FROM audit_log a
			JOIN users tu ON a.target_type = 'user' AND a.target_id = CAST(tu.id AS CHAR)
			WHERE tu.team_id IN (` + strings.Join(placeholders, ",") + `)
			ORDER BY a.id DESC LIMIT 200`
		rows, err = db.QueryContext(ctx, query, args...)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list audit log: %w", err)
	}
	defer rows.Close()

	entries := make([]models.AuditLogEntry, 0)
	for rows.Next() {
		var e models.AuditLogEntry
		var metaJSON sql.NullString
		if err := rows.Scan(&e.ID, &e.ActorUserID, &e.Action, &e.TargetType, &e.TargetID, &metaJSON, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan audit log row: %w", err)
		}
		if metaJSON.Valid && metaJSON.String != "" {
			_ = json.Unmarshal([]byte(metaJSON.String), &e.Metadata)
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return entries, nil
}

// UpdateUser applies a role/team/status change, enforcing every scope rule
// agreed in the design: an actor with iam:manage can do anything; a team
// lead can only touch a user currently in their own team(s) or MISC, can
// never grant SUPER_ADMIN, and can only move a user between their own
// team(s) and MISC. Self-edits and demoting/disabling the last active
// SUPER_ADMIN are blocked for everyone, including a super admin.
func UpdateUser(ctx context.Context, actorID, targetID int64, patch UserPatch) (*models.AdminUserView, error) {
	start := time.Now()
	user, err := updateUserImpl(ctx, actorID, targetID, patch)
	metrics.MySQLQueryDuration.WithLabelValues("admin_update_user").Observe(time.Since(start).Seconds())
	// One "user_update" action label regardless of which of role/team/status
	// actually changed — a single PATCH can change more than one, so there's
	// no single sub-action to attribute a forbidden/error outcome to before
	// the scope checks even run. Which fields changed on a successful update
	// is already captured precisely by the distinct audit_log rows written
	// below (role.assign / team.assign / user.status_change).
	recordMutationMetric(err, "user_update")
	return user, err
}

func recordMutationMetric(err error, action string) {
	switch {
	case err == nil:
		metrics.IAMAdminMutationsTotal.WithLabelValues(action, "success").Inc()
	case errors.Is(err, ErrCannotEditSelf) || errors.Is(err, ErrForbiddenScope) || errors.Is(err, ErrLastActiveSuperAdmin):
		metrics.IAMAdminMutationsTotal.WithLabelValues(action, "forbidden").Inc()
	default:
		metrics.IAMAdminMutationsTotal.WithLabelValues(action, "error").Inc()
	}
}

func updateUserImpl(ctx context.Context, actorID, targetID int64, patch UserPatch) (*models.AdminUserView, error) {
	db, err := getMySQLDB()
	if err != nil {
		return nil, fmt.Errorf("mysql unavailable: %w", err)
	}

	actor, err := getAdminUserRow(ctx, db, actorID)
	if err != nil {
		return nil, err
	}
	target, err := getAdminUserRow(ctx, db, targetID)
	if err != nil {
		return nil, err
	}
	if targetID == actorID {
		return nil, ErrCannotEditSelf
	}
	// Note: given today's data model (isSuperAdmin bypass requires the actor
	// themselves to be an active SUPER_ADMIN), the only way to ever reach a
	// target that IS the sole active SUPER_ADMIN is for actor and target to
	// be the same person — already caught by the self-edit check just above.
	// isLastActiveSuperAdmin below is still checked independently (belt and
	// suspenders against a future change to either guard), but expect it to
	// never actually fire ahead of ErrCannotEditSelf in practice.

	isSuperAdmin := actor.Role == "SUPER_ADMIN"
	var ledTeamIDs []int64
	if !isSuperAdmin {
		ledTeamIDs, err = GetLedTeamIDs(ctx, actorID)
		if err != nil {
			return nil, err
		}
		if len(ledTeamIDs) == 0 {
			return nil, ErrForbiddenScope
		}
	}

	// Scope check runs BEFORE the last-active-super-admin check below,
	// deliberately: a lead attempting to touch a target outside their scope
	// must get the same ErrForbiddenScope regardless of what that target
	// actually is, rather than a more specific "that's the last active super
	// admin" response leaking a fact about a user they have no business
	// looking at. Since SUPER_ADMIN always has team_id = NULL, this scope
	// check alone already excludes every SUPER_ADMIN target for a non-super-
	// admin actor — isLastActiveSuperAdmin below is therefore only ever
	// reachable for an actor who is themselves a super admin.
	if !isSuperAdmin {
		miscTeamID, err := getDefaultTeamID(ctx, db)
		if err != nil {
			return nil, err
		}
		scopedTeamIDs := append(append([]int64{}, ledTeamIDs...), miscTeamID)

		targetTeamID := int64(-1)
		if target.TeamID != nil {
			targetTeamID = *target.TeamID
		}
		if !containsInt64(scopedTeamIDs, targetTeamID) {
			return nil, ErrForbiddenScope
		}
		if patch.Role == "SUPER_ADMIN" {
			return nil, ErrForbiddenScope
		}
		if patch.TeamID == nil || !containsInt64(scopedTeamIDs, *patch.TeamID) {
			return nil, ErrForbiddenScope
		}
	}

	activeSuperAdmins, err := countActiveSuperAdmins(ctx, db)
	if err != nil {
		return nil, err
	}
	isLastActiveSuperAdmin := target.Role == "SUPER_ADMIN" && target.Status == "ACTIVE" && activeSuperAdmins <= 1
	if isLastActiveSuperAdmin && (patch.Role != "SUPER_ADMIN" || patch.Status != "ACTIVE") {
		return nil, ErrLastActiveSuperAdmin
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin update transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	if patch.Role != target.Role {
		var roleID int64
		if err := tx.QueryRowContext(ctx, "SELECT id FROM roles WHERE name = ?", patch.Role).Scan(&roleID); err != nil {
			return nil, fmt.Errorf("unknown role %q: %w", patch.Role, err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO user_roles (user_id, role_id, assigned_by) VALUES (?, ?, ?)
			ON DUPLICATE KEY UPDATE role_id = VALUES(role_id), assigned_at = CURRENT_TIMESTAMP, assigned_by = VALUES(assigned_by)
		`, targetID, roleID, actorID); err != nil {
			return nil, fmt.Errorf("failed to assign role: %w", err)
		}
	}
	targetTeamChanged := (target.TeamID == nil) != (patch.TeamID == nil) ||
		(target.TeamID != nil && patch.TeamID != nil && *target.TeamID != *patch.TeamID)
	if targetTeamChanged {
		if _, err := tx.ExecContext(ctx, "UPDATE users SET team_id = ? WHERE id = ?", patch.TeamID, targetID); err != nil {
			return nil, fmt.Errorf("failed to update team: %w", err)
		}
	}
	if patch.Status != target.Status {
		if _, err := tx.ExecContext(ctx, "UPDATE users SET status = ? WHERE id = ?", patch.Status, targetID); err != nil {
			return nil, fmt.Errorf("failed to update status: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit update transaction: %w", err)
	}

	if patch.Role != target.Role {
		_ = RecordAuditEvent(ctx, &actorID, "role.assign", "user", fmt.Sprint(targetID), map[string]interface{}{"from": target.Role, "to": patch.Role})
	}
	if targetTeamChanged {
		_ = RecordAuditEvent(ctx, &actorID, "team.assign", "user", fmt.Sprint(targetID), map[string]interface{}{"from": target.TeamID, "to": patch.TeamID})
	}
	if patch.Status != target.Status {
		_ = RecordAuditEvent(ctx, &actorID, "user.status_change", "user", fmt.Sprint(targetID), map[string]interface{}{"from": target.Status, "to": patch.Status})
	}
	// Active invalidation — the mutation just committed, so the target's
	// next permission check must see it immediately, not after the TTL.
	InvalidateUserPermissions(targetID)

	return &models.AdminUserView{ID: targetID, Role: patch.Role, TeamID: patch.TeamID, Status: patch.Status}, nil
}

// CreateTeam is SUPER_ADMIN-only, enforced at the route (RequirePermission
// "iam","manage") — not re-checked here, same convention as every other
// RequirePermission-gated mutation in this codebase.
func CreateTeam(ctx context.Context, actorID int64, name, description string) (*models.AdminTeam, error) {
	start := time.Now()
	team, err := createTeamImpl(ctx, actorID, name, description)
	metrics.MySQLQueryDuration.WithLabelValues("admin_create_team").Observe(time.Since(start).Seconds())
	recordMutationMetric(err, "team_create")
	return team, err
}

func createTeamImpl(ctx context.Context, actorID int64, name, description string) (*models.AdminTeam, error) {
	db, err := getMySQLDB()
	if err != nil {
		return nil, fmt.Errorf("mysql unavailable: %w", err)
	}
	res, err := db.ExecContext(ctx, "INSERT INTO teams (name, description, is_default) VALUES (?, ?, FALSE)", name, description)
	if err != nil {
		return nil, fmt.Errorf("failed to create team: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("failed to read new team id: %w", err)
	}
	_ = RecordAuditEvent(ctx, &actorID, "team.create", "team", fmt.Sprint(id), map[string]interface{}{"name": name})
	return &models.AdminTeam{ID: id, Name: name, Description: description, IsDefault: false, LeadUserIDs: []int64{}}, nil
}

// GrantTeamLead is SUPER_ADMIN-only (route-gated, see CreateTeam's comment).
// Requires the target already be a member of the team they're being made
// lead of — leadership without membership would be a confusing state (a
// lead who can't see their own team's users, since scoping is by team_id
// membership, not the lead grant itself). Idempotent: granting a lead who
// already leads the team is a no-op, not an error.
func GrantTeamLead(ctx context.Context, actorID, teamID, targetUserID int64) error {
	start := time.Now()
	err := grantTeamLeadImpl(ctx, actorID, teamID, targetUserID)
	metrics.MySQLQueryDuration.WithLabelValues("admin_grant_team_lead").Observe(time.Since(start).Seconds())
	recordMutationMetric(err, "lead_grant")
	return err
}

func grantTeamLeadImpl(ctx context.Context, actorID, teamID, targetUserID int64) error {
	db, err := getMySQLDB()
	if err != nil {
		return fmt.Errorf("mysql unavailable: %w", err)
	}
	target, err := getAdminUserRow(ctx, db, targetUserID)
	if err != nil {
		return err
	}
	if target.TeamID == nil || *target.TeamID != teamID {
		return fmt.Errorf("%w: user must belong to the team before being made its lead", ErrForbiddenScope)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT IGNORE INTO team_leads (team_id, user_id, granted_by) VALUES (?, ?, ?)
	`, teamID, targetUserID, actorID); err != nil {
		return fmt.Errorf("failed to grant team lead: %w", err)
	}
	var teamName string
	_ = db.QueryRowContext(ctx, "SELECT name FROM teams WHERE id = ?", teamID).Scan(&teamName)
	_ = RecordAuditEvent(ctx, &actorID, "team.lead_grant", "user", fmt.Sprint(targetUserID), map[string]interface{}{"team": teamName})
	return nil
}

// RevokeTeamLead is SUPER_ADMIN-only (route-gated). A no-op (not an error)
// if the user wasn't a lead of that team.
func RevokeTeamLead(ctx context.Context, actorID, teamID, targetUserID int64) error {
	start := time.Now()
	err := revokeTeamLeadImpl(ctx, actorID, teamID, targetUserID)
	metrics.MySQLQueryDuration.WithLabelValues("admin_revoke_team_lead").Observe(time.Since(start).Seconds())
	recordMutationMetric(err, "lead_revoke")
	return err
}

func revokeTeamLeadImpl(ctx context.Context, actorID, teamID, targetUserID int64) error {
	db, err := getMySQLDB()
	if err != nil {
		return fmt.Errorf("mysql unavailable: %w", err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM team_leads WHERE team_id = ? AND user_id = ?", teamID, targetUserID); err != nil {
		return fmt.Errorf("failed to revoke team lead: %w", err)
	}
	var teamName string
	_ = db.QueryRowContext(ctx, "SELECT name FROM teams WHERE id = ?", teamID).Scan(&teamName)
	_ = RecordAuditEvent(ctx, &actorID, "team.lead_revoke", "user", fmt.Sprint(targetUserID), map[string]interface{}{"team": teamName})
	return nil
}
