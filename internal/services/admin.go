// Admin Panel service layer: user/role/audit-log listing and user mutation
// (role/status changes). This is the real authorization boundary for all of
// it — internal/handlers/admin.go is a thin JSON-in/JSON-out wrapper, and
// the frontend's own gating is a UX convenience only, same posture as the
// rest of this codebase's RBAC. Every route in this group is gated on
// iam:manage alone (see internal/handlers/admin.go / cmd/server/main.go) —
// there is no narrower scope to enforce below that.
package services

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"velocity-engine-control-plane-backend-go/internal/metrics"
	"velocity-engine-control-plane-backend-go/internal/models"
)

var (
	// ErrCannotEditSelf/ErrLastActiveSuperAdmin are both mapped to 403 by the
	// handler — distinct sentinels so each can carry its own message without
	// the handler needing to inspect error text.
	ErrCannotEditSelf       = errors.New("cannot edit your own account through the admin panel")
	ErrLastActiveSuperAdmin = errors.New("cannot demote or disable the last active super admin")
)

// UserPatch is PATCH /admin/users/:id's decoded body.
type UserPatch struct {
	Role   string
	Status string
}

// adminUserRow is the internal (role, status) shape used by the mutation
// logic below — a small subset of AdminUserView.
type adminUserRow struct {
	ID     int64
	Role   string
	Status string
}

func getAdminUserRow(ctx context.Context, db *sql.DB, userID int64) (*adminUserRow, error) {
	row := &adminUserRow{ID: userID}
	err := db.QueryRowContext(ctx, `
		SELECT u.status, COALESCE(r.name, '')
		FROM users u
		LEFT JOIN user_roles ur ON ur.user_id = u.id
		LEFT JOIN roles r ON r.id = ur.role_id
		WHERE u.id = ?
	`, userID).Scan(&row.Status, &row.Role)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrUserNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to look up user: %w", err)
	}
	return row, nil
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

// ListUsers returns every user.
func ListUsers(ctx context.Context) ([]models.AdminUserView, error) {
	start := time.Now()
	users, err := listUsersImpl(ctx)
	metrics.MySQLQueryDuration.WithLabelValues("admin_list_users").Observe(time.Since(start).Seconds())
	return users, err
}

func listUsersImpl(ctx context.Context) ([]models.AdminUserView, error) {
	db, err := getMySQLDB()
	if err != nil {
		return nil, fmt.Errorf("mysql unavailable: %w", err)
	}

	rows, err := db.QueryContext(ctx, `
		SELECT u.id, u.email, u.display_name, u.status, COALESCE(r.name, '')
		FROM users u
		LEFT JOIN user_roles ur ON ur.user_id = u.id
		LEFT JOIN roles r ON r.id = ur.role_id
		ORDER BY u.id`)
	if err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
	}
	defer rows.Close()

	users := make([]models.AdminUserView, 0)
	for rows.Next() {
		var u models.AdminUserView
		if err := rows.Scan(&u.ID, &u.Email, &u.DisplayName, &u.Status, &u.Role); err != nil {
			return nil, fmt.Errorf("failed to scan user row: %w", err)
		}
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return users, nil
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

// ListAuditLog returns the most recent 200 entries.
func ListAuditLog(ctx context.Context) ([]models.AuditLogEntry, error) {
	start := time.Now()
	entries, err := listAuditLogImpl(ctx)
	metrics.MySQLQueryDuration.WithLabelValues("admin_list_audit_log").Observe(time.Since(start).Seconds())
	return entries, err
}

func listAuditLogImpl(ctx context.Context) ([]models.AuditLogEntry, error) {
	db, err := getMySQLDB()
	if err != nil {
		return nil, fmt.Errorf("mysql unavailable: %w", err)
	}

	rows, err := db.QueryContext(ctx, `
		SELECT id, actor_user_id, action, target_type, target_id, metadata, created_at
		FROM audit_log ORDER BY id DESC LIMIT 200`)
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

// UpdateUser applies a role/status change. Self-edits and demoting/disabling
// the last active SUPER_ADMIN are blocked for everyone, including a super
// admin. Every caller here has already been route-gated on iam:manage (see
// internal/handlers/admin.go) — there is no narrower per-target scope to
// enforce beyond that.
func UpdateUser(ctx context.Context, actorID, targetID int64, patch UserPatch) (*models.AdminUserView, error) {
	start := time.Now()
	user, err := updateUserImpl(ctx, actorID, targetID, patch)
	metrics.MySQLQueryDuration.WithLabelValues("admin_update_user").Observe(time.Since(start).Seconds())
	// One "user_update" action label regardless of which of role/status
	// actually changed — a single PATCH can change more than one, so there's
	// no single sub-action to attribute a forbidden/error outcome to before
	// the checks below even run. Which fields changed on a successful update
	// is already captured precisely by the distinct audit_log rows written
	// below (role.assign / user.status_change).
	recordMutationMetric(err, "user_update")
	return user, err
}

func recordMutationMetric(err error, action string) {
	switch {
	case err == nil:
		metrics.IAMAdminMutationsTotal.WithLabelValues(action, "success").Inc()
	case errors.Is(err, ErrCannotEditSelf) || errors.Is(err, ErrLastActiveSuperAdmin):
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

	target, err := getAdminUserRow(ctx, db, targetID)
	if err != nil {
		return nil, err
	}
	if targetID == actorID {
		return nil, ErrCannotEditSelf
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
	if patch.Status != target.Status {
		_ = RecordAuditEvent(ctx, &actorID, "user.status_change", "user", fmt.Sprint(targetID), map[string]interface{}{"from": target.Status, "to": patch.Status})
	}
	// Active invalidation — the mutation just committed, so the target's
	// next permission check must see it immediately, not after the TTL.
	InvalidateUserPermissions(targetID)

	return &models.AdminUserView{ID: targetID, Role: patch.Role, Status: patch.Status}, nil
}
