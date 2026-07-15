-- Rule persistence schema for the Velocity Engine Control Plane.
-- Replaces the earlier CSV-file-based rule store (internal/store's
-- csvstore.go/types.go, removed) — MySQL is now the durable source of truth
-- for rule definitions, so the backend can reconstruct its in-memory rule
-- list after a restart instead of starting empty every time.
--
-- Run this manually with a mysql client against the target database — the
-- backend never creates, alters, or seeds this schema itself. At startup it
-- only verifies these tables exist (internal/services/mysql.go,
-- VerifyMySQLSchema) and reloads active rules from them
-- (internal/services/rule_store.go, LoadActiveRules).
--
-- Design: five tables, normalized 1:1 with the five CSV files this
-- replaces (rules.csv, window_configs.csv, sink_configs.csv,
-- aggregation_specs.csv, breach_conditions.csv), scoped one row (or row
-- group) per rule version — no cross-rule reuse of aggregation
-- types/sources/breach conditions as independent catalogs; that's a
-- larger reusable-catalog redesign intentionally deferred to a v2 pass.
-- The one addition with no CSV precedent is `rules.filters`: the rule's
-- AND/OR filter condition tree was never captured anywhere before this.
-- It's stored as a single JSON column even though everything else here is
-- normalized — it's a recursive tree, always read/written as a whole
-- unit, never queried by its internal structure, so a self-referencing
-- adjacency-list table would add real reconstruction complexity (recursive
-- queries or multi-row app-side tree-walking) for no corresponding benefit.

CREATE TABLE rules (
    id                    BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    rule_id               VARCHAR(128) NOT NULL,
    version               INT UNSIGNED NOT NULL,
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,   -- current version for this rule_id; startup reload filters on this
    name                  VARCHAR(255) NOT NULL,           -- rule_metadata.rule_name (the real name, not a synthesized one)
    description           VARCHAR(500) NULL,               -- auto-generated human-readable summary
    status                VARCHAR(32) NOT NULL,             -- DRAFT | ACTIVE | PAUSED | DELETED
    severity              VARCHAR(32) NOT NULL,
    source_topic          VARCHAR(255) NOT NULL,             -- execution_routing.target_source_topic
    entity_name           VARCHAR(128) NULL,                  -- grouping.entity_name
    anomaly_entity_field  VARCHAR(128) NULL,                   -- grouping.anomaly_entity_field
    grouping_keys         JSON NULL,                             -- grouping.keys, e.g. ["uid"]
    filters               JSON NULL,                               -- the full filter AND/OR tree — see design note above
    penalty_ttl_seconds   INT NOT NULL DEFAULT 0,                    -- rule_metadata.penalty_ttl_seconds; lives here (not sink_configs) now that CSV parity no longer applies
    active_guard          VARCHAR(128) GENERATED ALWAYS AS (IF(is_active, rule_id, NULL)) VIRTUAL,  -- DB-enforced "at most one active row per rule_id" — see uq_rules_one_active_per_rule below
    created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_rules_rule_id_version (rule_id, version),
    UNIQUE KEY uq_rules_one_active_per_rule (active_guard),
    KEY idx_rules_active_rule_id (is_active, rule_id),
    KEY idx_rules_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- One row per rule version — rule_row_id is both the primary key and the
-- foreign key, enforcing the 1:1 relationship. Mirrors window_configs.csv.
CREATE TABLE window_configs (
    rule_row_id           BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    window_type           VARCHAR(32) NOT NULL,
    window_size_ms        BIGINT NOT NULL,
    slide_ms              BIGINT NOT NULL DEFAULT 0,
    time_mode              VARCHAR(32) NOT NULL,
    timestamp_field         VARCHAR(128) NULL,
    timestamp_format          VARCHAR(32) NULL,
    allowed_lateness_ms         BIGINT NOT NULL DEFAULT 0,
    alignment_offset_ms          BIGINT NOT NULL DEFAULT 0,
    use_kafka_timestamp            BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT fk_window_configs_rule FOREIGN KEY (rule_row_id) REFERENCES rules(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- One row per rule version. Mirrors sink_configs.csv, minus penalty_ttl_seconds
-- (moved to `rules` — it's a rule_metadata field, not a sink toggle; kept here
-- originally only for 1:1 CSV parity, which no longer applies).
CREATE TABLE sink_configs (
    rule_row_id                 BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    agg_sink_enabled            BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_sink_enabled        BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_store_sink_enabled  BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT fk_sink_configs_rule FOREIGN KEY (rule_row_id) REFERENCES rules(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Many rows per rule version (up to 3, per the rule builder's existing
-- limit). Mirrors aggregation_specs.csv.
CREATE TABLE aggregation_specs (
    id                   BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    rule_row_id          BIGINT UNSIGNED NOT NULL,
    alias                VARCHAR(128) NOT NULL,
    agg_function         VARCHAR(32) NOT NULL,
    source_field         VARCHAR(255) NOT NULL,
    is_high_cardinality  BOOLEAN NOT NULL DEFAULT FALSE,
    spec_order           INT NOT NULL,
    CONSTRAINT fk_aggregation_specs_rule FOREIGN KEY (rule_row_id) REFERENCES rules(id) ON DELETE CASCADE,
    KEY idx_aggregation_specs_rule (rule_row_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- One row per rule version (the JEXL threshold expression). Mirrors
-- breach_conditions.csv.
CREATE TABLE breach_conditions (
    id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    rule_row_id  BIGINT UNSIGNED NOT NULL,
    expression   TEXT NOT NULL,
    CONSTRAINT fk_breach_conditions_rule FOREIGN KEY (rule_row_id) REFERENCES rules(id) ON DELETE CASCADE,
    KEY idx_breach_conditions_rule (rule_row_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Versioning semantics, enforced by internal/services/rule_store.go, and
-- backstopped by the schema itself: MySQL 8 has no direct partial/conditional
-- unique index, but `rules.active_guard` (a generated column that's NULL for
-- every non-active row and equal to rule_id for the active one) plus
-- uq_rules_one_active_per_rule gets the same effect — "at most one
-- is_active=TRUE row per rule_id" is a real database constraint, not just an
-- application-maintained invariant. The application still always changes
-- is_active inside a transaction (below) — the constraint is a backstop
-- against a bug or a race in that logic, not a substitute for it:
--
--   * A NEW VERSION (a fresh row in `rules` plus its four side-table rows)
--     is created only when the rule's actual definition changes — a full
--     edit via PUT /rules/:id. The previously-active `rules` row (and its
--     now-superseded side-table rows) is deactivated, not deleted, in the
--     same transaction — full version history is preserved.
--   * Publishing (DRAFT -> ACTIVE), pausing/resuming (ACTIVE <-> PAUSED),
--     and deleting (-> DELETED) are status changes to the CURRENT version —
--     an UPDATE to `rules.status` (and `is_active` on delete) only. The
--     four side tables never change on a status transition, since the
--     rule's definition doesn't change — only `rules` needs touching.
--     Deleting flips is_active to FALSE (a DELETED row is a tombstone, it
--     should never be reloaded into the running rule set).
