// Rule persistence — MySQL-backed replacement for the earlier CSV-file rule
// store (internal/store's csvstore.go/types.go, removed). See
// internal/migrations/mysql/0002_init_rules_store.sql for the schema — five
// tables normalized 1:1 with the five CSV files this replaces — and the
// versioning-semantics note there, which the two write functions below
// implement.
package services

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"velocity-engine-control-plane-backend-go/internal/models"
	"velocity-engine-control-plane-backend-go/internal/store"
)

// SaveNewRuleVersion persists a new version of a rule across all five
// tables: deactivates whatever was previously active for this rule_id (a
// no-op if this is the first version — there's nothing to deactivate yet)
// and inserts the new version's rows as active, in one transaction. Call
// this only when the rule's actual definition has changed (create, or a
// full edit via PUT /rules/:id) — for operational status changes to an
// existing version (publish, pause/resume, delete), use
// UpdateRuleStatusInPlace instead; see the migration file's
// versioning-semantics note for why these are handled differently.
func SaveNewRuleVersion(ctx context.Context, rule *models.VelocityRule, version int) error {
	db, err := getMySQLDB()
	if err != nil {
		return fmt.Errorf("mysql unavailable: %w", err)
	}

	ruleID := rule.RuleMetadata.RuleID
	description := store.GenerateRuleDescription(rule)

	groupingKeys, err := json.Marshal(rule.Grouping.Keys)
	if err != nil {
		return fmt.Errorf("failed to serialize grouping keys: %w", err)
	}

	var filtersJSON interface{}
	if rule.Filters != nil {
		b, err := json.Marshal(rule.Filters)
		if err != nil {
			return fmt.Errorf("failed to serialize filters: %w", err)
		}
		filtersJSON = b
	}

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

	res, err := tx.ExecContext(ctx, `
		INSERT INTO rules (rule_id, version, is_active, name, description, status, severity,
			source_topic, entity_name, anomaly_entity_field, grouping_keys, filters)
		VALUES (?, ?, TRUE, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, ruleID, version, rule.RuleMetadata.RuleName, description, rule.RuleMetadata.Status, rule.RuleMetadata.SeverityLevel,
		rule.ExecutionRouting.TargetSourceTopic, rule.Grouping.EntityName, rule.Grouping.AnomalyEntityField, groupingKeys, filtersJSON)
	if err != nil {
		return fmt.Errorf("failed to insert new rule version: %w", err)
	}
	ruleRowID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to get new rule row id: %w", err)
	}

	w := rule.Windowing
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO window_configs (rule_row_id, window_type, window_size_ms, slide_ms, time_mode,
			timestamp_field, timestamp_format, allowed_lateness_ms, alignment_offset_ms, use_kafka_timestamp)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, ruleRowID, w.Type, w.SizeMs, w.SlideMs, w.TimeType, w.TimestampField, w.TimestampFormat,
		w.AllowedLatenessMs, w.AlignmentOffsetMs, w.UseKafkaTimestamp); err != nil {
		return fmt.Errorf("failed to insert window config: %w", err)
	}

	var aggOn, anomalyOn, storeOn bool
	if rule.Sinks != nil {
		aggOn = rule.Sinks.AggSinkEnabled
		anomalyOn = rule.Sinks.AnomalySinkEnabled
		storeOn = rule.Sinks.AnomalyStoreSinkEnabled
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO sink_configs (rule_row_id, agg_sink_enabled, anomaly_sink_enabled, anomaly_store_sink_enabled, penalty_ttl_seconds)
		VALUES (?, ?, ?, ?, ?)
	`, ruleRowID, aggOn, anomalyOn, storeOn, rule.RuleMetadata.PenaltyTTLSeconds); err != nil {
		return fmt.Errorf("failed to insert sink config: %w", err)
	}

	for i, agg := range rule.Aggregations {
		isHighCard := agg.CardinalityHint == "HIGH"
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO aggregation_specs (rule_row_id, alias, agg_function, source_field, is_high_cardinality, spec_order)
			VALUES (?, ?, ?, ?, ?, ?)
		`, ruleRowID, agg.Alias, agg.Function, agg.Field, isHighCard, i+1); err != nil {
			return fmt.Errorf("failed to insert aggregation spec %d: %w", i, err)
		}
	}

	if expr := rule.HavingThresholds.Expression; expr != "" {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO breach_conditions (rule_row_id, expression) VALUES (?, ?)
		`, ruleRowID, expr); err != nil {
			return fmt.Errorf("failed to insert breach condition: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit rule version: %w", err)
	}
	return nil
}

// UpdateRuleStatusInPlace updates the currently-active `rules` row's status
// for a rule_id — no new version, and none of the other four tables are
// touched, since an operational status change (DRAFT->ACTIVE on publish,
// ACTIVE<->PAUSED, or ->DELETED) never changes the rule's actual definition.
// If deactivate is true, is_active is also flipped to FALSE in the same
// statement — used for delete, so the tombstone row is naturally excluded
// from LoadActiveRules without a separate status filter.
func UpdateRuleStatusInPlace(ctx context.Context, ruleID, status string, deactivate bool) error {
	db, err := getMySQLDB()
	if err != nil {
		return fmt.Errorf("mysql unavailable: %w", err)
	}

	query := "UPDATE rules SET status = ?"
	if deactivate {
		query += ", is_active = FALSE"
	}
	query += " WHERE rule_id = ? AND is_active = TRUE"

	if _, err := db.ExecContext(ctx, query, status, ruleID); err != nil {
		return fmt.Errorf("failed to update rule status: %w", err)
	}
	return nil
}

// LoadActiveRules reconstructs the in-memory rule map from MySQL — this is
// what lets the backend survive a restart without losing its rule list.
// Reassembles a full VelocityRule per active rule from all five tables,
// batching the four side-table queries with a single WHERE rule_row_id IN
// (...) each (5 queries total, regardless of how many rules exist) rather
// than one round trip per rule per table. IsPublished is derived from
// status rather than stored as its own column: a rule has been published
// at least once iff its status is ACTIVE or PAUSED (DRAFT means never
// published; DELETED rows are excluded by the is_active filter and never
// reach here at all).
func LoadActiveRules(ctx context.Context) (map[string]*models.RuleRecord, error) {
	db, err := getMySQLDB()
	if err != nil {
		return nil, fmt.Errorf("mysql unavailable: %w", err)
	}

	type ruleRow struct {
		rowID              int64
		ruleID             string
		version            int
		name               string
		description        string
		status             string
		severity           string
		sourceTopic        string
		entityName         string
		anomalyEntityField string
		groupingKeys       []byte
		filters            []byte
	}

	rows, err := db.QueryContext(ctx, `
		SELECT id, rule_id, version, name, description, status, severity,
			source_topic, entity_name, anomaly_entity_field, grouping_keys, filters
		FROM rules WHERE is_active = TRUE
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query active rules: %w", err)
	}
	var ruleRows []ruleRow
	for rows.Next() {
		var r ruleRow
		var entityName, anomalyEntityField sql.NullString
		var groupingKeys, filters sql.RawBytes
		if err := rows.Scan(&r.rowID, &r.ruleID, &r.version, &r.name, &r.description, &r.status, &r.severity,
			&r.sourceTopic, &entityName, &anomalyEntityField, &groupingKeys, &filters); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan rule row: %w", err)
		}
		r.entityName = entityName.String
		r.anomalyEntityField = anomalyEntityField.String
		if groupingKeys != nil {
			r.groupingKeys = append([]byte(nil), groupingKeys...)
		}
		if filters != nil {
			r.filters = append([]byte(nil), filters...)
		}
		ruleRows = append(ruleRows, r)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return nil, fmt.Errorf("rows iteration error: %w", rowsErr)
	}
	if len(ruleRows) == 0 {
		return map[string]*models.RuleRecord{}, nil
	}

	rowIDs := make([]interface{}, len(ruleRows))
	for i, r := range ruleRows {
		rowIDs[i] = r.rowID
	}
	inClause := "(" + strings.TrimSuffix(strings.Repeat("?,", len(rowIDs)), ",") + ")"

	windows := make(map[int64]models.WindowingConfig, len(ruleRows))
	wRows, err := db.QueryContext(ctx, `
		SELECT rule_row_id, window_type, window_size_ms, slide_ms, time_mode,
			timestamp_field, timestamp_format, allowed_lateness_ms, alignment_offset_ms, use_kafka_timestamp
		FROM window_configs WHERE rule_row_id IN `+inClause, rowIDs...)
	if err != nil {
		return nil, fmt.Errorf("failed to query window configs: %w", err)
	}
	for wRows.Next() {
		var rowID int64
		var w models.WindowingConfig
		var timestampField, timestampFormat sql.NullString
		if err := wRows.Scan(&rowID, &w.Type, &w.SizeMs, &w.SlideMs, &w.TimeType,
			&timestampField, &timestampFormat, &w.AllowedLatenessMs, &w.AlignmentOffsetMs, &w.UseKafkaTimestamp); err != nil {
			wRows.Close()
			return nil, fmt.Errorf("failed to scan window config: %w", err)
		}
		w.TimestampField = timestampField.String
		w.TimestampFormat = timestampFormat.String
		windows[rowID] = w
	}
	wRowsErr := wRows.Err()
	wRows.Close()
	if wRowsErr != nil {
		return nil, fmt.Errorf("window_configs rows iteration error: %w", wRowsErr)
	}

	type sinkRow struct {
		sinks             models.SinkConfig
		penaltyTTLSeconds int
	}
	sinks := make(map[int64]sinkRow, len(ruleRows))
	sRows, err := db.QueryContext(ctx, `
		SELECT rule_row_id, agg_sink_enabled, anomaly_sink_enabled, anomaly_store_sink_enabled, penalty_ttl_seconds
		FROM sink_configs WHERE rule_row_id IN `+inClause, rowIDs...)
	if err != nil {
		return nil, fmt.Errorf("failed to query sink configs: %w", err)
	}
	for sRows.Next() {
		var rowID int64
		var s sinkRow
		if err := sRows.Scan(&rowID, &s.sinks.AggSinkEnabled, &s.sinks.AnomalySinkEnabled, &s.sinks.AnomalyStoreSinkEnabled, &s.penaltyTTLSeconds); err != nil {
			sRows.Close()
			return nil, fmt.Errorf("failed to scan sink config: %w", err)
		}
		sinks[rowID] = s
	}
	sRowsErr := sRows.Err()
	sRows.Close()
	if sRowsErr != nil {
		return nil, fmt.Errorf("sink_configs rows iteration error: %w", sRowsErr)
	}

	aggs := make(map[int64][]models.AggregationSpec, len(ruleRows))
	aRows, err := db.QueryContext(ctx, `
		SELECT rule_row_id, alias, agg_function, source_field, is_high_cardinality
		FROM aggregation_specs WHERE rule_row_id IN `+inClause+` ORDER BY rule_row_id, spec_order`, rowIDs...)
	if err != nil {
		return nil, fmt.Errorf("failed to query aggregation specs: %w", err)
	}
	for aRows.Next() {
		var rowID int64
		var a models.AggregationSpec
		var isHighCard bool
		if err := aRows.Scan(&rowID, &a.Alias, &a.Function, &a.Field, &isHighCard); err != nil {
			aRows.Close()
			return nil, fmt.Errorf("failed to scan aggregation spec: %w", err)
		}
		if isHighCard {
			a.CardinalityHint = "HIGH"
		} else {
			a.CardinalityHint = "LOW"
		}
		aggs[rowID] = append(aggs[rowID], a)
	}
	aRowsErr := aRows.Err()
	aRows.Close()
	if aRowsErr != nil {
		return nil, fmt.Errorf("aggregation_specs rows iteration error: %w", aRowsErr)
	}

	breaches := make(map[int64]string, len(ruleRows))
	bRows, err := db.QueryContext(ctx, `
		SELECT rule_row_id, expression FROM breach_conditions WHERE rule_row_id IN `+inClause, rowIDs...)
	if err != nil {
		return nil, fmt.Errorf("failed to query breach conditions: %w", err)
	}
	for bRows.Next() {
		var rowID int64
		var expr string
		if err := bRows.Scan(&rowID, &expr); err != nil {
			bRows.Close()
			return nil, fmt.Errorf("failed to scan breach condition: %w", err)
		}
		breaches[rowID] = expr
	}
	bRowsErr := bRows.Err()
	bRows.Close()
	if bRowsErr != nil {
		return nil, fmt.Errorf("breach_conditions rows iteration error: %w", bRowsErr)
	}

	result := make(map[string]*models.RuleRecord, len(ruleRows))
	for _, r := range ruleRows {
		var groupingKeys []string
		if r.groupingKeys != nil {
			if err := json.Unmarshal(r.groupingKeys, &groupingKeys); err != nil {
				return nil, fmt.Errorf("failed to parse grouping_keys for rule %s: %w", r.ruleID, err)
			}
		}
		var filters *models.FilterNode
		if r.filters != nil {
			filters = &models.FilterNode{}
			if err := json.Unmarshal(r.filters, filters); err != nil {
				return nil, fmt.Errorf("failed to parse filters for rule %s: %w", r.ruleID, err)
			}
		}
		s := sinks[r.rowID]

		rule := models.VelocityRule{
			RuleMetadata: models.RuleMetadata{
				RuleID:            r.ruleID,
				RuleName:          r.name,
				Status:            r.status,
				SeverityLevel:     r.severity,
				PenaltyTTLSeconds: s.penaltyTTLSeconds,
			},
			ExecutionRouting: models.ExecutionRouting{TargetSourceTopic: r.sourceTopic},
			Filters:          filters,
			Grouping: models.GroupingConfig{
				Keys:               groupingKeys,
				EntityName:         r.entityName,
				AnomalyEntityField: r.anomalyEntityField,
			},
			Windowing:        windows[r.rowID],
			Aggregations:     aggs[r.rowID],
			HavingThresholds: models.HavingThresholds{Expression: breaches[r.rowID]},
			Sinks:            &s.sinks,
		}

		payload, err := json.Marshal(rule)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize reconstructed rule %s: %w", r.ruleID, err)
		}

		result[r.ruleID] = &models.RuleRecord{
			ID:          r.ruleID,
			Name:        r.name,
			Status:      r.status,
			RulePayload: json.RawMessage(payload),
			IsPublished: r.status == "ACTIVE" || r.status == "PAUSED",
			Version:     r.version,
		}
	}
	return result, nil
}
