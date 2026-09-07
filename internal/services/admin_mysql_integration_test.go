//go:build mysql_integration

// Integration test against a REAL MySQL instance for the multi-team RBAC
// extension (0003_add_teams.sql) — JIT auto-provisioning, the per-IP rate
// limit, and UpdateUser's team-scoped enforcement. Same opt-in convention as
// rbac_mysql_integration_test.go: apply 0001+0002+0003 to the target
// database yourself first, then:
//
//	MYSQL_HOST=... MYSQL_PORT=... MYSQL_USER=... MYSQL_PASSWORD=... MYSQL_DATABASE=... \
//	  go test -tags mysql_integration -v ./internal/services/... -run TestAdminMySQLIntegration
package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
)

func TestAdminMySQLIntegration_JITProvisioningAndRateLimit(t *testing.T) {
	ResetMySQLConn()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := VerifyMySQLSchema(ctx); err != nil {
		t.Fatalf("VerifyMySQLSchema failed — apply 0001/0002/0003 migrations first: %v", err)
	}

	db, err := getMySQLDB()
	if err != nil {
		t.Fatalf("getMySQLDB failed: %v", err)
	}

	origLimit := config.JITProvisionRateLimitPerMinute
	config.JITProvisionRateLimitPerMinute = 2
	t.Cleanup(func() { config.JITProvisionRateLimitPerMinute = origLimit })
	// Reset the package-level rate-limit window so this test doesn't inherit
	// counts from another test/run sharing the same process.
	jitRateMu.Lock()
	jitRateWindowStart = time.Time{}
	jitRateCounts = make(map[string]int)
	jitRateMu.Unlock()

	const ip = "198.51.100.7" // TEST-NET-2, distinct per test to avoid cross-test rate-limit bleed
	sub1 := "integration-jit-subject-1"
	sub2 := "integration-jit-subject-2"
	sub3 := "integration-jit-subject-3"
	t.Cleanup(func() {
		for _, s := range []string{sub1, sub2, sub3} {
			db.ExecContext(context.Background(), "DELETE FROM users WHERE external_subject = ?", s)
		}
	})

	// First two provisions within the limit must succeed and land in MISC
	// with READ_ONLY_ANALYST.
	for _, sub := range []string{sub1, sub2} {
		userID, status, err := GetOrProvisionUserByExternalSubject(ctx, sub, ip)
		if err != nil {
			t.Fatalf("expected provisioning to succeed for %s, got: %v", sub, err)
		}
		if status != "ACTIVE" {
			t.Fatalf("expected new user to be ACTIVE, got %s", status)
		}
		var teamName, roleName string
		if err := db.QueryRowContext(ctx, `
			SELECT t.name, r.name FROM users u
			JOIN teams t ON t.id = u.team_id
			JOIN user_roles ur ON ur.user_id = u.id
			JOIN roles r ON r.id = ur.role_id
			WHERE u.id = ?
		`, userID).Scan(&teamName, &roleName); err != nil {
			t.Fatalf("failed to look up provisioned user's team/role: %v", err)
		}
		if teamName != "MISC" || roleName != "READ_ONLY_ANALYST" {
			t.Fatalf("expected MISC/READ_ONLY_ANALYST, got team=%s role=%s", teamName, roleName)
		}
		var auditCount int
		db.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit_log WHERE action='user.jit_provision' AND target_id=?", userID).Scan(&auditCount)
		if auditCount < 1 {
			t.Fatalf("expected a user.jit_provision audit_log row for user %d", userID)
		}
	}

	// Third distinct subject, same source IP, exceeds the 2/minute limit —
	// must be rejected as ErrUserNotFound (IdentityMiddleware maps this to a
	// 401, same as the pre-JIT deny-until-provisioned behavior), not silently
	// provisioned.
	if _, _, err := GetOrProvisionUserByExternalSubject(ctx, sub3, ip); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("expected the 3rd provisioning attempt from the same IP to be rate-limited (ErrUserNotFound), got: %v", err)
	}
	var sub3Count int
	db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users WHERE external_subject = ?", sub3).Scan(&sub3Count)
	if sub3Count != 0 {
		t.Fatal("rate-limited attempt must not have created a user row")
	}

	// A lookup for an already-provisioned subject must always succeed
	// regardless of rate-limit state (it's a lookup, not a new provision).
	if _, _, err := GetOrProvisionUserByExternalSubject(ctx, sub1, ip); err != nil {
		t.Fatalf("expected lookup of an already-provisioned subject to succeed despite rate limit, got: %v", err)
	}
}

func TestAdminMySQLIntegration_UpdateUserEnforcesTeamScope(t *testing.T) {
	ResetMySQLConn()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := VerifyMySQLSchema(ctx); err != nil {
		t.Fatalf("VerifyMySQLSchema failed — apply 0001/0002/0003 migrations first: %v", err)
	}
	db, err := getMySQLDB()
	if err != nil {
		t.Fatalf("getMySQLDB failed: %v", err)
	}

	var teamAID, teamBID, roleEditorID, roleAnalystID, roleManagerID int64
	res, err := db.ExecContext(ctx, "INSERT INTO teams (name) VALUES (?)", "IntegrationTeamA")
	if err != nil {
		t.Fatalf("failed to create team A: %v", err)
	}
	teamAID, _ = res.LastInsertId()
	res, err = db.ExecContext(ctx, "INSERT INTO teams (name) VALUES (?)", "IntegrationTeamB")
	if err != nil {
		t.Fatalf("failed to create team B: %v", err)
	}
	teamBID, _ = res.LastInsertId()
	db.QueryRowContext(ctx, "SELECT id FROM roles WHERE name = 'RULE_EDITOR'").Scan(&roleEditorID)
	db.QueryRowContext(ctx, "SELECT id FROM roles WHERE name = 'READ_ONLY_ANALYST'").Scan(&roleAnalystID)
	db.QueryRowContext(ctx, "SELECT id FROM roles WHERE name = 'RULE_MANAGER'").Scan(&roleManagerID)

	insertUser := func(sub string, teamID int64, roleID int64) int64 {
		res, err := db.ExecContext(ctx, "INSERT INTO users (external_subject, email, display_name, status, team_id) VALUES (?, ?, ?, 'ACTIVE', ?)",
			sub, sub+"@example.invalid", sub, teamID)
		if err != nil {
			t.Fatalf("failed to insert user %s: %v", sub, err)
		}
		id, _ := res.LastInsertId()
		if _, err := db.ExecContext(ctx, "INSERT INTO user_roles (user_id, role_id) VALUES (?, ?)", id, roleID); err != nil {
			t.Fatalf("failed to assign role to %s: %v", sub, err)
		}
		return id
	}

	leadID := insertUser("integration-lead-a", teamAID, roleManagerID)
	teammateID := insertUser("integration-teammate-a", teamAID, roleEditorID)
	outsiderID := insertUser("integration-outsider-b", teamBID, roleAnalystID)

	if _, err := db.ExecContext(ctx, "INSERT INTO team_leads (team_id, user_id) VALUES (?, ?)", teamAID, leadID); err != nil {
		t.Fatalf("failed to grant team lead: %v", err)
	}

	t.Cleanup(func() {
		for _, id := range []int64{leadID, teammateID, outsiderID} {
			db.ExecContext(context.Background(), "DELETE FROM users WHERE id = ?", id)
		}
		db.ExecContext(context.Background(), "DELETE FROM teams WHERE id IN (?, ?)", teamAID, teamBID)
	})

	// The lead can edit their own teammate.
	if _, err := UpdateUser(ctx, leadID, teammateID, UserPatch{Role: "RULE_MANAGER", TeamID: &teamAID, Status: "ACTIVE"}); err != nil {
		t.Fatalf("expected lead to be able to update their own teammate, got: %v", err)
	}

	// The lead cannot touch a user in a different team.
	if _, err := UpdateUser(ctx, leadID, outsiderID, UserPatch{Role: "RULE_EDITOR", TeamID: &teamAID, Status: "ACTIVE"}); !errors.Is(err, ErrForbiddenScope) {
		t.Fatalf("expected ErrForbiddenScope for an out-of-team target, got: %v", err)
	}

	// The lead cannot grant SUPER_ADMIN even to their own teammate.
	if _, err := UpdateUser(ctx, leadID, teammateID, UserPatch{Role: "SUPER_ADMIN", TeamID: nil, Status: "ACTIVE"}); !errors.Is(err, ErrForbiddenScope) {
		t.Fatalf("expected ErrForbiddenScope when a lead attempts to grant SUPER_ADMIN, got: %v", err)
	}

	// The lead cannot move their teammate to a team they don't lead.
	if _, err := UpdateUser(ctx, leadID, teammateID, UserPatch{Role: "RULE_EDITOR", TeamID: &teamBID, Status: "ACTIVE"}); !errors.Is(err, ErrForbiddenScope) {
		t.Fatalf("expected ErrForbiddenScope when moving a teammate to a non-led team, got: %v", err)
	}

	// Self-edit is blocked even for the lead acting on themselves.
	if _, err := UpdateUser(ctx, leadID, leadID, UserPatch{Role: "RULE_MANAGER", TeamID: &teamAID, Status: "ACTIVE"}); !errors.Is(err, ErrCannotEditSelf) {
		t.Fatalf("expected ErrCannotEditSelf, got: %v", err)
	}

	// Cache invalidation: the teammate's cached permissions must reflect the
	// role change from the very first update above, with no manual wait.
	InvalidateUserPermissions(teammateID) // ensure a clean baseline before re-asserting
	_, perms, err := GetUserPermissions(ctx, teammateID)
	if err != nil {
		t.Fatalf("GetUserPermissions failed: %v", err)
	}
	if !perms["rules:publish"] {
		t.Fatalf("expected teammate's permissions to reflect RULE_MANAGER after the update, got: %v", perms)
	}
}
