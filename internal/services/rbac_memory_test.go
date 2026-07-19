// Replaces rbac_mysql_integration_test.go, which proved the MySQL DDL/seed
// data and fetchUserPermissionsFromDB worked end-to-end against a real
// MySQL instance. This branch has no MySQL anymore — these tests cover the
// same guarantees (seed-data shape, per-role grants, and end-to-end
// permission resolution for a user id) against the in-memory
// demoUsers/rolePermissions store in rbac.go instead. No build tag or
// external dependency needed: `go test ./...` always exercises this.
package services

import (
	"context"
	"errors"
	"testing"
)

func TestDemoRBACSeed_FourRolesTenPermissions(t *testing.T) {
	if len(rolePermissions) != 4 {
		t.Fatalf("expected 4 seeded roles, got %d: %v", len(rolePermissions), rolePermissions)
	}

	all := make(map[string]bool)
	for _, perms := range rolePermissions {
		for k := range perms {
			all[k] = true
		}
	}
	if len(all) != 10 {
		t.Fatalf("expected 10 distinct resource:action permission keys across all roles, got %d: %v", len(all), all)
	}
}

func TestDemoRBACSeed_SuperAdminHasAllPermissions(t *testing.T) {
	perms := rolePermissions["SUPER_ADMIN"]
	if len(perms) != 10 {
		t.Fatalf("expected SUPER_ADMIN to have all 10 permissions, got %d: %v", len(perms), perms)
	}
}

func TestDemoRBACSeed_RuleEditorCannotPublishOrDelete(t *testing.T) {
	perms := rolePermissions["RULE_EDITOR"]
	if perms["rules:publish"] || perms["rules:delete"] {
		t.Fatalf("expected RULE_EDITOR to have neither rules:publish nor rules:delete, got %v", perms)
	}
	// but must still be able to draft/edit rules
	if !perms["rules:create"] || !perms["rules:read"] || !perms["rules:update"] {
		t.Fatalf("expected RULE_EDITOR to have rules:create/read/update, got %v", perms)
	}
}

func TestFetchUserPermissionsFromMemory_SeededUser(t *testing.T) {
	// Seeded as RULE_MANAGER — see rbac.go's demoUsers.
	roles, perms, err := fetchUserPermissionsFromMemory(context.Background(), 2)
	if err != nil {
		t.Fatalf("fetchUserPermissionsFromMemory failed: %v", err)
	}
	if len(roles) != 1 || roles[0] != "RULE_MANAGER" {
		t.Fatalf("expected roles=[RULE_MANAGER], got %v", roles)
	}
	if !perms["rules:publish"] || !perms["rules:delete"] || perms["iam:manage"] {
		t.Fatalf("RULE_MANAGER permission set looks wrong: %v", perms)
	}
}

func TestFetchUserPermissionsFromMemory_UnknownUser(t *testing.T) {
	_, _, err := fetchUserPermissionsFromMemory(context.Background(), 999999)
	if !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("expected ErrUserNotFound for an unseeded user id, got %v", err)
	}
}

func TestFetchUserPermissionsFromMemory_DisabledUser(t *testing.T) {
	// Temporarily disable a seeded user rather than requiring a permanent
	// disabled seed entry — restored via t.Cleanup, matching the
	// save/restore pattern rbac_test.go already uses for fetchFunc/config.
	const userID = 4
	orig := demoUsers[userID]
	demoUsers[userID] = demoUser{role: orig.role, status: "DISABLED"}
	t.Cleanup(func() { demoUsers[userID] = orig })

	_, _, err := fetchUserPermissionsFromMemory(context.Background(), userID)
	if !errors.Is(err, ErrUserDisabled) {
		t.Fatalf("expected ErrUserDisabled, got %v", err)
	}
}

func TestGetUserPermissions_EndToEndAgreesWithDirectFetch(t *testing.T) {
	resetRBACCacheForTest(t)
	withTTLs(t, 300, 1800)

	const userID = 3 // seeded as RULE_EDITOR
	InvalidateUserPermissions(userID)

	roles, perms, err := GetUserPermissions(context.Background(), userID)
	if err != nil {
		t.Fatalf("GetUserPermissions failed: %v", err)
	}
	if len(roles) != 1 || roles[0] != "RULE_EDITOR" {
		t.Fatalf("expected roles=[RULE_EDITOR], got %v", roles)
	}
	if !perms["rules:create"] || perms["rules:publish"] {
		t.Fatalf("cached GetUserPermissions result mismatch for RULE_EDITOR: %v", perms)
	}

	directRoles, directPerms, err := fetchUserPermissionsFromMemory(context.Background(), userID)
	if err != nil {
		t.Fatalf("fetchUserPermissionsFromMemory failed: %v", err)
	}
	if len(directRoles) != len(roles) || directRoles[0] != roles[0] {
		t.Fatalf("cached and direct roles disagree: cached=%v direct=%v", roles, directRoles)
	}
	if len(directPerms) != len(perms) {
		t.Fatalf("cached and direct permission sets disagree in size: cached=%v direct=%v", perms, directPerms)
	}
}
