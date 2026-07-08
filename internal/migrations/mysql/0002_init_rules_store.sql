-- Rule persistence schema for the Velocity Engine Control Plane.
-- Replaces the earlier CSV-file-based rule store (internal/store's
-- csvstore.go/types.go, removed) — MySQL is now the durable source of truth
-- for rule definitions, so the backend can reconstruct its in-memory rule
-- list after a restart instead of starting empty every time.
--
-- Run this manually with a mysql client against the target database — the
-- backend never creates, alters, or seeds this schema itself. At startup it
-- only verifies this table exists (internal/services/mysql.go,
-- VerifyMySQLSchema) and reloads active rules from it
-- (internal/services/rule_store.go, LoadActiveRules).

-- Design note: one table, not five. The earlier CSV design split a rule
-- across 5 files (rules / window_configs / sink_configs / aggregation_specs
-- / breach_conditions) and, in the process, never captured the rule's
-- filter conditions at all — a real gap. Rather than repeat that risk with
-- a hand-maintained set of normalized tables (which would also need a
-- recursive structure for the filter AND/OR tree), rule_payload below holds
-- the complete rule as JSON, exactly as accepted by the API — nothing about
-- its shape can be silently dropped, because there's no per-field mapping
-- to forget a field from. The other columns are denormalized, queryable
-- conveniences for browsing/filtering without parsing JSON; rule_payload is
-- the only column actually needed to reconstruct a rule.
CREATE TABLE rules (
    id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    rule_id       VARCHAR(128) NOT NULL,
    version       INT UNSIGNED NOT NULL,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,   -- the current version for this rule_id; startup reload filters on this
    name          VARCHAR(255) NOT NULL,
    description   VARCHAR(500) NULL,               -- human-readable summary, auto-generated from the rule's aggregations/window
    status        VARCHAR(32) NOT NULL,             -- DRAFT | ACTIVE | PAUSED | DELETED
    severity      VARCHAR(32) NOT NULL,
    rule_payload  JSON NOT NULL,                     -- the complete rule (metadata, routing, filters, grouping, windowing,
                                                       -- aggregations, thresholds, sinks) — see design note above
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_rules_rule_id_version (rule_id, version),
    KEY idx_rules_rule_id_active (rule_id, is_active),  -- "current version of rule X" / "all active rules" (startup reload)
    KEY idx_rules_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Versioning semantics, enforced by internal/services/rule_store.go, not by
-- the schema itself (MySQL has no partial/conditional unique index, so "at
-- most one is_active=TRUE row per rule_id" is an application-maintained
-- invariant, always changed inside a transaction):
--
--   * A NEW VERSION (an INSERT) is created only when the rule's actual
--     definition changes — i.e. a full edit via PUT /rules/:id. The
--     previously-active row for that rule_id is deactivated in the same
--     transaction as the new one is inserted.
--   * Publishing (DRAFT -> ACTIVE), pausing/resuming (ACTIVE <-> PAUSED),
--     and deleting (-> DELETED) are status changes to the CURRENT version —
--     applied as an UPDATE to the active row in place, no new version row.
--     Deleting also flips is_active to FALSE (a DELETED row is a tombstone,
--     it should never be reloaded into the running rule set).
--
-- This means version history only ever tracks genuine content changes,
-- which is the more useful kind of history to keep ("when did the rule's
-- logic change") rather than every operational status flip.
