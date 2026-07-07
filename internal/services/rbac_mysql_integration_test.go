//go:build mysql_integration

// Integration test against a REAL MySQL instance — proves the actual DDL in
// internal/migrations/mysql/0001_init_rbac.sql is valid MySQL and that the
// real queries in fetchUserPermissionsFromDB / RunMySQLMigrations work
// end-to-end, not just the pure-Go cache logic covered by rbac_test.go.
//
// Opt-in via build tag so `go test ./...` never requires MySQL to be
// present (and can't fail in a CI that doesn't have it wired up). Run with:
//
//	MYSQL_HOST=... MYSQL_PORT=... MYSQL_USER=... MYSQL_PASSWORD=... MYSQL_DATABASE=... \
//	  go test -tags mysql_integration -v ./internal/services/... -run MySQLIntegration
package services

import (
	"context"
	"testing"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
)

func TestMySQLIntegration_MigrationsAndPermissionResolution(t *testing.T) {
	ResetMySQLConn()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := RunMySQLMigrations(ctx); err != nil {
		t.Fatalf("RunMySQLMigrations failed against real MySQL: %v", err)
	}

	// Re-running must be a no-op (idempotency / schema_migrations tracking).
	if err := RunMySQLMigrations(ctx); err != nil {
		t.Fatalf("RunMySQLMigrations (second run) failed: %v", err)
	}

	db, err := getMySQLDB()
	if err != nil {
		t.Fatalf("getMySQLDB failed: %v", err)
	}

	// Seed data assertions — the four approved roles and ten permissions
	// from the migration must be present exactly as designed.
	var roleCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM roles WHERE is_system = TRUE").Scan(&roleCount); err != nil {
		t.Fatalf("failed to count seeded roles: %v", err)
	}
	if roleCount != 4 {
		t.Fatalf("expected 4 seeded system roles, got %d", roleCount)
	}

	var permCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM permissions").Scan(&permCount); err != nil {
		t.Fatalf("failed to count seeded permissions: %v", err)
	}
	if permCount != 10 {
		t.Fatalf("expected 10 seeded permissions, got %d", permCount)
	}

	// SUPER_ADMIN must have all 10 permissions (CROSS JOIN in the seed).
	var superAdminPermCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM role_permissions rp
		JOIN roles r ON r.id = rp.role_id
		WHERE r.name = 'SUPER_ADMIN'
	`).Scan(&superAdminPermCount); err != nil {
		t.Fatalf("failed to count SUPER_ADMIN permissions: %v", err)
	}
	if superAdminPermCount != 10 {
		t.Fatalf("expected SUPER_ADMIN to have all 10 permissions, got %d", superAdminPermCount)
	}

	// RULE_EDITOR must NOT have rules:publish or rules:delete (maker-checker split).
	var editorHasPublish int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM role_permissions rp
		JOIN roles r ON r.id = rp.role_id
		JOIN permissions p ON p.id = rp.permission_id
		WHERE r.name = 'RULE_EDITOR' AND p.resource = 'rules' AND p.action IN ('publish','delete')
	`).Scan(&editorHasPublish); err != nil {
		t.Fatalf("failed to check RULE_EDITOR permissions: %v", err)
	}
	if editorHasPublish != 0 {
		t.Fatalf("expected RULE_EDITOR to have neither rules:publish nor rules:delete, found %d matching grants", editorHasPublish)
	}

	// End-to-end permission resolution through fetchUserPermissionsFromDB
	// (the real DB path, not a swapped fetchFunc) against a throwaway test user.
	res, err := db.ExecContext(ctx, `
		INSERT INTO users (external_subject, email, display_name, status)
		VALUES (?, ?, ?, 'ACTIVE')
	`, "integration-test-subject", "integration-test@example.invalid", "Integration Test User")
	if err != nil {
		t.Fatalf("failed to insert test user: %v", err)
	}
	userID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("failed to get inserted user id: %v", err)
	}
	t.Cleanup(func() {
		db.ExecContext(context.Background(), "DELETE FROM users WHERE id = ?", userID)
	})

	var roleID int32
	if err := db.QueryRowContext(ctx, "SELECT id FROM roles WHERE name = 'RULE_MANAGER'").Scan(&roleID); err != nil {
		t.Fatalf("failed to find RULE_MANAGER role id: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO user_roles (user_id, role_id) VALUES (?, ?)", userID, roleID); err != nil {
		t.Fatalf("failed to assign role: %v", err)
	}

	roles, perms, err := fetchUserPermissionsFromDB(ctx, userID)
	if err != nil {
		t.Fatalf("fetchUserPermissionsFromDB failed: %v", err)
	}
	if len(roles) != 1 || roles[0] != "RULE_MANAGER" {
		t.Fatalf("expected roles=[RULE_MANAGER], got %v", roles)
	}
	if !perms["rules:publish"] || !perms["rules:delete"] || perms["iam:manage"] {
		t.Fatalf("RULE_MANAGER permission set looks wrong: %v", perms)
	}

	// GetUserPermissions (the cached path) must agree with the direct fetch.
	InvalidateUserPermissions(userID)
	cachedRoles, cachedPerms, err := GetUserPermissions(ctx, userID)
	if err != nil {
		t.Fatalf("GetUserPermissions failed: %v", err)
	}
	if len(cachedRoles) != 1 || !cachedPerms["rules:publish"] {
		t.Fatalf("GetUserPermissions result mismatch: roles=%v perms=%v", cachedRoles, cachedPerms)
	}

	// Disabled user must resolve to ErrUserDisabled, not an empty-but-ok set.
	if _, err := db.ExecContext(ctx, "UPDATE users SET status = 'DISABLED' WHERE id = ?", userID); err != nil {
		t.Fatalf("failed to disable test user: %v", err)
	}
	InvalidateUserPermissions(userID)
	if _, _, err := GetUserPermissions(ctx, userID); err == nil {
		t.Fatal("expected an error for a disabled user, got nil")
	}

	// RecordAuditEvent must insert a real, readable row.
	if err := RecordAuditEvent(ctx, &userID, "test.event", "user", "0", map[string]interface{}{"note": "integration test"}); err != nil {
		t.Fatalf("RecordAuditEvent failed: %v", err)
	}
	var auditCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit_log WHERE action = 'test.event'").Scan(&auditCount); err != nil {
		t.Fatalf("failed to verify audit_log row: %v", err)
	}
	if auditCount < 1 {
		t.Fatal("expected at least one audit_log row for the recorded event")
	}

	t.Logf("MySQL integration test passed against %s:%d/%s", config.MySQLHost, config.MySQLPort, config.MySQLDatabase)
}
