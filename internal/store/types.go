package store

// RuleRow maps to rules.csv — one row per rule version.
type RuleRow struct {
	RuleID       string
	Version      int
	IsActive     bool
	Name         string
	Description  string
	Status       string
	Severity     string
	EntityName   string
	GroupingKeys string // JSON array string
	CreatedAt    string // ISO-8601
	UpdatedAt    string // ISO-8601
}

// WindowConfigRow maps to window_configs.csv — one row per rule version.
type WindowConfigRow struct {
	RuleID             string
	Version            int
	WindowType         string
	WindowSizeMs       int64
	SlideMs            int64
	TimeMode           string
	TimestampField     string
	TimestampFormat    string
	AllowedLatenessMs  int64
	AlignmentOffsetMs  int64
	UseKafkaTimestamp  bool
}

// SinkConfigRow maps to sink_configs.csv — one row per rule version.
type SinkConfigRow struct {
	RuleID                  string
	Version                 int
	AggSinkEnabled          bool
	AnomalySinkEnabled      bool
	AnomalyStoreSinkEnabled bool
	PenaltyTTLSeconds       int
}

// AggSpecRow maps to aggregation_specs.csv — up to 3 rows per rule version.
type AggSpecRow struct {
	ID              string
	RuleID          string
	Version         int
	Alias           string
	AggFunction     string
	SourceField     string
	IsHighCard      bool
	SpecOrder       int
}

// BreachCondRow maps to breach_conditions.csv — N rows per rule version.
type BreachCondRow struct {
	ID             string
	RuleID         string
	Version        int
	Expression     string // The JEXL expression (stored as-is for now)
}
