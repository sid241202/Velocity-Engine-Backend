-- Multi-team RBAC extension for the Velocity Engine Control Plane Admin Panel.
-- Adds `teams` + `team_leads` and a nullable `users.team_id`. See the design
-- discussion (Admin Panel + team-based RBAC) for the full reasoning; this
-- file implements exactly what was agreed there.
--
-- Run this manually with a mysql client against the target database — the
-- backend never creates, alters, or seeds this schema itself (same
-- convention as 0001_init_rbac.sql / 0002_init_rules_store.sql). At startup
-- it only verifies these tables/columns exist (internal/services/mysql.go,
-- VerifyMySQLSchema) and fails Admin Panel routes closed if they don't.
--
-- Design notes:
--   * Team leadership is a `team_leads` join table, NOT a new role. A 5th
--     "TEAM_LEAD" role would conflict with user_roles.user_id being a
--     PRIMARY KEY (one role per user, enforced by the DB) — a person who
--     leads a team and is also e.g. RULE_MANAGER couldn't hold both if
--     leadership were a role slot instead of an orthogonal capability.
--   * `teams.is_default` marks exactly one seeded row (MISC) as the landing
--     team for every new/JIT-provisioned user. NULL team_id is reserved for
--     SUPER_ADMIN only (who sits above all teams) — enforced at the
--     application level (internal/services/admin.go), since MySQL 8 CHECK
--     constraints can't reference another table's rows.
--   * users.team_id uses ON DELETE RESTRICT, not SET NULL: deleting a team
--     that still has members should fail loudly and force an explicit
--     reassignment first, not silently dump people into the NULL/no-team
--     state that's meant to be SUPER_ADMIN-only.
--   * No uniqueness constraint forces "exactly one lead per team" — multiple
--     leads (a backup, a transition period) are allowed; enforcing exactly
--     one would cost real complexity for no real benefit.

CREATE TABLE teams (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(128) NOT NULL,
    description VARCHAR(255) NULL,
    is_default  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_teams_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE users
    ADD COLUMN team_id INT UNSIGNED NULL AFTER status,
    ADD CONSTRAINT fk_users_team FOREIGN KEY (team_id)
        REFERENCES teams(id) ON DELETE RESTRICT;

CREATE TABLE team_leads (
    team_id     INT UNSIGNED NOT NULL,
    user_id     BIGINT UNSIGNED NOT NULL,
    granted_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    granted_by  BIGINT UNSIGNED NULL,
    PRIMARY KEY (team_id, user_id),
    KEY idx_team_leads_user (user_id),
    CONSTRAINT fk_team_leads_team       FOREIGN KEY (team_id)    REFERENCES teams(id) ON DELETE CASCADE,
    CONSTRAINT fk_team_leads_user       FOREIGN KEY (user_id)    REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT fk_team_leads_granted_by FOREIGN KEY (granted_by) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Seed data: the default "unassigned" team ────────────────────────────────

INSERT INTO teams (name, description, is_default) VALUES
    ('MISC', 'Default team for users not yet assigned to a specific team.', TRUE);

-- ── Backfill: every existing user needs a team_id now that the column
--    exists. SUPER_ADMIN stays NULL (sits above all teams); everyone else
--    goes to MISC until a super admin or team lead assigns them elsewhere.
UPDATE users u
LEFT JOIN user_roles ur ON ur.user_id = u.id
LEFT JOIN roles r ON r.id = ur.role_id
SET u.team_id = (SELECT id FROM teams WHERE is_default = TRUE LIMIT 1)
WHERE r.name IS NULL OR r.name <> 'SUPER_ADMIN';

-- ── Assigning/reassigning a user's team (manual reference, until the Admin
--    Panel's own endpoints exist to do this — see internal/handlers/admin.go):
--
-- UPDATE users SET team_id = (SELECT id FROM teams WHERE name = 'Team Alpha') WHERE id = ?;
--
-- -- Granting a user as a team's lead (idempotent — a second grant is a no-op,
-- -- not an error, matching internal/services/admin.go's GrantTeamLead):
-- INSERT IGNORE INTO team_leads (team_id, user_id, granted_by)
-- VALUES ((SELECT id FROM teams WHERE name = 'Team Alpha'), ?, ?);
--
-- Note: role/team/status changes made through the Admin Panel take effect
-- immediately (the mutation actively evicts that user's cached permission
-- set — see internal/services/rbac.go's InvalidateUserPermissions). A
-- change made by hand-editing these tables directly is NOT actively
-- invalidated — it takes effect within RBAC_PERMISSION_CACHE_TTL_SECONDS
-- (default 300s), same as before this migration.
