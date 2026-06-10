package models

import "encoding/json"

// RuleMetadata matches Python RuleMetadata Pydantic model exactly.
type RuleMetadata struct {
	RuleID            string `json:"rule_id"`
	RuleName          string `json:"rule_name"`
	Status            string `json:"status"`
	SeverityLevel     string `json:"severity_level"`
	PenaltyTTLSeconds int    `json:"penalty_ttl_seconds"`
}

// ExecutionRouting matches Python ExecutionRouting Pydantic model exactly.
type ExecutionRouting struct {
	TargetCluster     string `json:"target_cluster"`
	TargetSourceTopic string `json:"target_source_topic"`
}

// FilterNode is a recursive tree structure for AND/OR filter groups.
// Matches Python FilterNode Pydantic model exactly.
type FilterNode struct {
	Type       string       `json:"type"`
	Logic      *string      `json:"logic,omitempty"`
	Conditions []FilterNode `json:"conditions,omitempty"`
	Field      *string      `json:"field,omitempty"`
	Operator   *string      `json:"operator,omitempty"`
	Value      interface{}  `json:"value,omitempty"`
}

// GroupingConfig matches Python GroupingConfig Pydantic model exactly.
type GroupingConfig struct {
	Keys []string `json:"keys"`
}

// WindowingConfig matches Python WindowingConfig Pydantic model exactly.
type WindowingConfig struct {
	Type              string `json:"type"`
	TimeType          string `json:"time_type"`
	TimestampField    string `json:"timestamp_field"`
	TimestampFormat   string `json:"timestamp_format"`
	UseKafkaTimestamp bool   `json:"use_kafka_timestamp"`
	SizeMs            int64  `json:"size_ms"`
	SlideMs           int64  `json:"slide_ms"`
	AllowedLatenessMs int64  `json:"allowed_lateness_ms"`
	AlignmentOffsetMs int64  `json:"alignment_offset_ms"`
}

// AggregationSpec matches Python AggregationSpec Pydantic model exactly.
type AggregationSpec struct {
	Alias           string `json:"alias"`
	Field           string `json:"field"`
	Function        string `json:"function"`
	CardinalityHint string `json:"cardinality_hint"`
}

// HavingThresholds matches Python HavingThresholds Pydantic model exactly.
type HavingThresholds struct {
	Expression string `json:"expression"`
}

// VelocityRule is the top-level rule structure.
// Matches Python VelocityRule Pydantic model exactly.
type VelocityRule struct {
	RuleMetadata     RuleMetadata     `json:"rule_metadata"`
	ExecutionRouting ExecutionRouting  `json:"execution_routing"`
	Filters          *FilterNode      `json:"filters"`
	Grouping         GroupingConfig   `json:"grouping"`
	Windowing        WindowingConfig  `json:"windowing"`
	Aggregations     []AggregationSpec `json:"aggregations"`
	HavingThresholds HavingThresholds `json:"having_thresholds"`
}

// RuleRecord is the in-memory storage record for a rule.
type RuleRecord struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Status      string          `json:"status"`
	RulePayload json.RawMessage `json:"rule_payload"`
	IsPublished bool            `json:"is_published"`
}
