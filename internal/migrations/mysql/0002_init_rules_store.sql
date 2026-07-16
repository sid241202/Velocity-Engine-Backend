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
    created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_rules_rule_id_version (rule_id, version),
    KEY idx_rules_rule_id_active (rule_id, is_active),
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

-- One row per rule version. penalty_ttl_seconds is kept here rather than
-- on `rules`, matching the old sink_configs.csv's placement exactly even
-- though it's conceptually a rule_metadata field — preserving 1:1 CSV
-- parity rather than silently relocating it.
CREATE TABLE sink_configs (
    rule_row_id                 BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    agg_sink_enabled            BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_sink_enabled        BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_store_sink_enabled  BOOLEAN NOT NULL DEFAULT FALSE,
    penalty_ttl_seconds         INT NOT NULL DEFAULT 0,
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

-- Versioning semantics, enforced by internal/services/rule_store.go, not by
-- the schema itself (MySQL has no partial/conditional unique index, so "at
-- most one is_active=TRUE row per rule_id" is an application-maintained
-- invariant, always changed inside a transaction):
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
