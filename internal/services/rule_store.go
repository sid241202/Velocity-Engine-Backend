// Rule persistence — MySQL-backed replacement for the earlier CSV-file rule
// store (internal/store's csvstore.go/types.go, removed). See
// internal/migrations/mysql/0002_init_rules_store.sql for the schema and the
// versioning-semantics note there, which the two write functions below
// implement.
package services

import (
	"context"
	"encoding/json"
	"fmt"

	"velocity-engine-control-plane-backend-go/internal/models"
	"velocity-engine-control-plane-backend-go/internal/store"
)

// SaveNewRuleVersion persists a new version of a rule: deactivates whatever
// was previously active for this rule_id (a no-op if this is the first
// version — there's nothing to deactivate yet) and inserts the new version
// as active, in one transaction. Call this only when the rule's actual
// definition has changed (create, or a full edit via PUT /rules/:id) — for
// operational status changes to an existing version (publish, pause/resume,
// delete), use UpdateRuleStatusInPlace instead; see the migration file's
// versioning-semantics note for why these are handled differently.
func SaveNewRuleVersion(ctx context.Context, rule *models.VelocityRule, version int) error {
	db, err := getMySQLDB()
	if err != nil {
		return fmt.Errorf("mysql unavailable: %w", err)
	}

	payload, err := json.Marshal(rule)
	if err != nil {
		return fmt.Errorf("failed to serialize rule: %w", err)
	}

	ruleID := rule.RuleMetadata.RuleID
	description := store.GenerateRuleDescription(rule)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // no-op once committed below

	if _, err := tx.ExecContext(ctx,
		"UPDATE rules SET is_active = FALSE WHERE rule_id = ? AND is_active = TRUE",
		ruleID,
	); err != nil {
		return fmt.Errorf("failed to deactivate previous rule version: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO rules (rule_id, version, is_active, name, description, status, severity, rule_payload)
		VALUES (?, ?, TRUE, ?, ?, ?, ?, ?)
	`, ruleID, version, rule.RuleMetadata.RuleName, description, rule.RuleMetadata.Status, rule.RuleMetadata.SeverityLevel, payload); err != nil {
		return fmt.Errorf("failed to insert new rule version: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit rule version: %w", err)
	}
	return nil
}

// UpdateRuleStatusInPlace updates the currently-active row for a rule_id —
// no new version is created, since the rule's actual definition hasn't
// changed, only its operational status (DRAFT->ACTIVE on publish,
// ACTIVE<->PAUSED, or ->DELETED). rulePayload is the full, re-serialized
// rule reflecting the new status (callers already have this, since they
// mutate the same in-memory copy that gets published to Kafka). If
// deactivate is true, is_active is also flipped to FALSE in the same
// statement — used for delete, so the tombstone row is naturally excluded
// from LoadActiveRules without a separate status filter.
func UpdateRuleStatusInPlace(ctx context.Context, ruleID, status string, rulePayload []byte, deactivate bool) error {
	db, err := getMySQLDB()
	if err != nil {
		return fmt.Errorf("mysql unavailable: %w", err)
	}

	query := "UPDATE rules SET status = ?, rule_payload = ?"
	args := []interface{}{status, rulePayload}
	if deactivate {
		query += ", is_active = FALSE"
	}
	query += " WHERE rule_id = ? AND is_active = TRUE"
	args = append(args, ruleID)

	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to update rule status: %w", err)
	}
	return nil
}

// LoadActiveRules reconstructs the in-memory rule map from MySQL — this is
// what lets the backend survive a restart without losing its rule list.
// IsPublished is derived from status rather than stored as its own column:
// a rule has been published at least once iff its status is ACTIVE or
// PAUSED (DRAFT means never published; DELETED rows are excluded by the
// is_active filter and never reach here at all).
func LoadActiveRules(ctx context.Context) (map[string]*models.RuleRecord, error) {
	db, err := getMySQLDB()
	if err != nil {
		return nil, fmt.Errorf("mysql unavailable: %w", err)
	}

	rows, err := db.QueryContext(ctx,
		"SELECT rule_id, version, name, status, rule_payload FROM rules WHERE is_active = TRUE",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query active rules: %w", err)
	}
	defer rows.Close()

	result := make(map[string]*models.RuleRecord)
	for rows.Next() {
		var ruleID, name, status string
		var version int
		var payload []byte
		if err := rows.Scan(&ruleID, &version, &name, &status, &payload); err != nil {
			return nil, fmt.Errorf("failed to scan rule row: %w", err)
		}
		result[ruleID] = &models.RuleRecord{
			ID:          ruleID,
			Name:        name,
			Status:      status,
			RulePayload: json.RawMessage(payload),
			IsPublished: status == "ACTIVE" || status == "PAUSED",
			Version:     version,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return result, nil
}
