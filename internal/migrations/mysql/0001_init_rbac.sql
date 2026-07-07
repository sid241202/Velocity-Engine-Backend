-- RBAC schema for the Velocity Engine Control Plane.
-- Approved design: Users / Roles / Permissions / User_Roles / Role_Permissions
-- + Audit Log. See project RBAC design notes for the role/permission matrix
-- this seed data implements.
--
-- Run this manually with a mysql client against the target database — the
-- backend never creates, alters, or seeds this schema itself. At startup it
-- only verifies these tables exist (internal/services/mysql.go,
-- VerifyMySQLSchema) and fails RBAC-protected routes closed if they don't.

CREATE TABLE users (
    id                BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    external_subject  VARCHAR(255) NOT NULL,   -- WSO2/OIDC 'sub' claim — hook point for the future auth phase
    email             VARCHAR(255) NOT NULL,
    display_name      VARCHAR(255) NOT NULL,
    status            ENUM('ACTIVE','DISABLED') NOT NULL DEFAULT 'ACTIVE',
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_users_external_subject (external_subject),
    UNIQUE KEY uq_users_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE roles (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(64) NOT NULL,
    description VARCHAR(255) NULL,
    is_system   BOOLEAN NOT NULL DEFAULT FALSE,   -- seeded roles; application should block deletion of these
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_roles_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE permissions (
    id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    resource    VARCHAR(64) NOT NULL,    -- e.g. 'rules', 'live_analysis', 'historical_analysis', 'iam'
    action      VARCHAR(64) NOT NULL,    -- e.g. 'create','read','update','delete','publish','execute','manage'
    description VARCHAR(255) NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_permissions_resource_action (resource, action)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE user_roles (
    user_id     BIGINT UNSIGNED NOT NULL,
    role_id     INT UNSIGNED NOT NULL,
    assigned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    assigned_by BIGINT UNSIGNED NULL,     -- who granted this — nullable for system-seeded assignments
    PRIMARY KEY (user_id, role_id),
    KEY idx_user_roles_role_id (role_id),  -- supports "who has role X" without a full scan
    CONSTRAINT fk_user_roles_user        FOREIGN KEY (user_id)     REFERENCES users(id)  ON DELETE CASCADE,
    CONSTRAINT fk_user_roles_role        FOREIGN KEY (role_id)     REFERENCES roles(id)  ON DELETE CASCADE,
    CONSTRAINT fk_user_roles_assigned_by FOREIGN KEY (assigned_by) REFERENCES users(id)  ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE role_permissions (
    role_id       INT UNSIGNED NOT NULL,
    permission_id INT UNSIGNED NOT NULL,
    granted_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (role_id, permission_id),
    KEY idx_role_permissions_permission_id (permission_id),
    CONSTRAINT fk_role_permissions_role       FOREIGN KEY (role_id)       REFERENCES roles(id)       ON DELETE CASCADE,
    CONSTRAINT fk_role_permissions_permission FOREIGN KEY (permission_id) REFERENCES permissions(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE audit_log (
    id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    actor_user_id BIGINT UNSIGNED NULL,       -- NULL for system-initiated actions
    action        VARCHAR(64) NOT NULL,        -- e.g. 'role.assign', 'role.revoke', 'user.disable'
    target_type   VARCHAR(64) NOT NULL,        -- e.g. 'user', 'role', 'permission'
    target_id     VARCHAR(64) NOT NULL,
    metadata      JSON NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_audit_log_actor (actor_user_id),
    KEY idx_audit_log_target (target_type, target_id),
    KEY idx_audit_log_created_at (created_at),
    CONSTRAINT fk_audit_log_actor FOREIGN KEY (actor_user_id) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Seed data: the four approved roles and ten permissions ─────────────────

INSERT INTO roles (name, description, is_system) VALUES
    ('SUPER_ADMIN',       'Full system access, including user and role administration.', TRUE),
    ('RULE_MANAGER',      'Full rule lifecycle (create/edit/publish/delete) and full analysis read access.', TRUE),
    ('RULE_EDITOR',       'Can draft and edit rules but cannot publish or delete them.', TRUE),
    ('READ_ONLY_ANALYST', 'Read-only access to all analysis views; no rule mutation rights.', TRUE);

INSERT INTO permissions (resource, action, description) VALUES
    ('rules', 'create',  'Create a new rule (draft).'),
    ('rules', 'read',    'View rule definitions.'),
    ('rules', 'update',  'Edit an existing rule.'),
    ('rules', 'delete',  'Delete a rule.'),
    ('rules', 'publish', 'Publish a rule to ACTIVE / change its production status.'),
    ('live_analysis', 'read', 'View the Live Analysis panel.'),
    ('aggregated_analysis', 'read', 'View the Analytics panel (ClickHouse-backed).'),
    ('historical_analysis', 'read', 'View historical replay results.'),
    ('historical_analysis', 'execute', 'Run a historical replay / breakdown query.'),
    ('iam', 'manage', 'Manage users, roles, and permission assignments.');

INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r CROSS JOIN permissions p WHERE r.name = 'SUPER_ADMIN';

INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r JOIN permissions p
WHERE r.name = 'RULE_MANAGER' AND p.resource <> 'iam';

INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r JOIN permissions p
WHERE r.name = 'RULE_EDITOR' AND (
    (p.resource = 'rules' AND p.action IN ('create','read','update'))
    OR p.resource IN ('live_analysis','aggregated_analysis','historical_analysis')
);

INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r JOIN permissions p
WHERE r.name = 'READ_ONLY_ANALYST' AND (
    (p.resource = 'rules' AND p.action = 'read')
    OR p.resource IN ('live_analysis','aggregated_analysis','historical_analysis')
);
