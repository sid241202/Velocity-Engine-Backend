package store

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/models"
)

const chanBuffer = 1024

// writeReq is a generic write request holding a row of strings.
type writeReq struct {
	row []string
}

// csvWriter owns a single CSV file and drains a channel into it.
type csvWriter struct {
	ch     chan writeReq
	done   chan struct{}
}

func newCsvWriter(path string, header []string) (*csvWriter, error) {
	needHeader := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		needHeader = true
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("csvWriter open %s: %w", path, err)
	}

	w := &csvWriter{
		ch:   make(chan writeReq, chanBuffer),
		done: make(chan struct{}),
	}

	go func() {
		defer f.Close()
		defer close(w.done)
		cw := csv.NewWriter(f)
		if needHeader {
			_ = cw.Write(header)
			cw.Flush()
		}
		for req := range w.ch {
			if err := cw.Write(req.row); err != nil {
				slog.Error("csvWriter write error", "file", path, "error", err)
			}
			cw.Flush()
			if err := cw.Error(); err != nil {
				slog.Error("csvWriter flush error", "file", path, "error", err)
			}
		}
	}()

	return w, nil
}

func (w *csvWriter) send(row []string) {
	select {
	case w.ch <- writeReq{row: row}:
	default:
		slog.Warn("csvWriter channel full — row dropped")
	}
}

func (w *csvWriter) close() {
	close(w.ch)
	<-w.done
}

// CSVStore manages 5 CSV files — one per logical table.
type CSVStore struct {
	rules      *csvWriter
	windows    *csvWriter
	sinks      *csvWriter
	aggSpecs   *csvWriter
	breach     *csvWriter
	mu         sync.Mutex // protects version tracking
}

var rulesHeader = []string{"rule_id", "version", "is_active", "name", "description", "status", "severity", "entity_name", "grouping_keys", "created_at", "updated_at"}
var windowsHeader = []string{"rule_id", "version", "window_type", "window_size_ms", "slide_ms", "time_mode", "timestamp_field", "timestamp_format", "allowed_lateness_ms", "alignment_offset_ms", "use_kafka_timestamp"}
var sinksHeader = []string{"rule_id", "version", "agg_sink_enabled", "anomaly_sink_enabled", "anomaly_store_sink_enabled", "penalty_ttl_seconds"}
var aggSpecsHeader = []string{"id", "rule_id", "version", "alias", "agg_function", "source_field", "is_high_cardinality", "spec_order"}
var breachHeader = []string{"id", "rule_id", "version", "expression"}

// Open initialises the CSVStore, creating all 5 files under dir.
func Open(dir string) (*CSVStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("csvstore mkdir %s: %w", dir, err)
	}

	make := func(name string, header []string) (*csvWriter, error) {
		return newCsvWriter(filepath.Join(dir, name), header)
	}

	r, err := make("rules.csv", rulesHeader)
	if err != nil {
		return nil, err
	}
	w, err := make("window_configs.csv", windowsHeader)
	if err != nil {
		return nil, err
	}
	s, err := make("sink_configs.csv", sinksHeader)
	if err != nil {
		return nil, err
	}
	a, err := make("aggregation_specs.csv", aggSpecsHeader)
	if err != nil {
		return nil, err
	}
	b, err := make("breach_conditions.csv", breachHeader)
	if err != nil {
		return nil, err
	}

	slog.Info("CSVStore opened", "dir", dir)
	return &CSVStore{rules: r, windows: w, sinks: s, aggSpecs: a, breach: b}, nil
}

// Close gracefully drains all channels and closes all files.
func (s *CSVStore) Close() {
	s.rules.close()
	s.windows.close()
	s.sinks.close()
	s.aggSpecs.close()
	s.breach.close()
	slog.Info("CSVStore closed")
}

// WriteRule writes a new version of a rule across all 5 CSV files.
// If version > 1, it also writes a deactivation row for the previous version.
func (s *CSVStore) WriteRule(rule *models.VelocityRule, version int, isCreate bool) {
	now := time.Now().UTC().Format(time.RFC3339)
	name := GenerateRuleName(rule)
	desc := GenerateRuleDescription(rule)
	ruleID := rule.RuleMetadata.RuleID

	// Deactivate the previous version in rules.csv (append a false row)
	if !isCreate && version > 1 {
		s.rules.send([]string{
			ruleID, strconv.Itoa(version - 1), "false",
			name, desc,
			rule.RuleMetadata.Status, rule.RuleMetadata.SeverityLevel,
			rule.Grouping.EntityName, marshalKeys(rule.Grouping.Keys),
			now, now,
		})
	}

	// Write new active version to rules.csv
	s.rules.send([]string{
		ruleID, strconv.Itoa(version), "true",
		name, desc,
		rule.RuleMetadata.Status, rule.RuleMetadata.SeverityLevel,
		rule.Grouping.EntityName, marshalKeys(rule.Grouping.Keys),
		now, now,
	})

	// window_configs.csv
	w := rule.Windowing
	s.windows.send([]string{
		ruleID, strconv.Itoa(version),
		w.Type,
		strconv.FormatInt(w.SizeMs, 10),
		strconv.FormatInt(w.SlideMs, 10),
		w.TimeType,
		w.TimestampField,
		w.TimestampFormat,
		strconv.FormatInt(w.AllowedLatenessMs, 10),
		strconv.FormatInt(w.AlignmentOffsetMs, 10),
		strconv.FormatBool(w.UseKafkaTimestamp),
	})

	// sink_configs.csv
	var aggOn, anomalyOn, redisOn bool
	var penaltyTTL int
	if rule.Sinks != nil {
		aggOn = rule.Sinks.AggSinkEnabled
		anomalyOn = rule.Sinks.AnomalySinkEnabled
		redisOn = rule.Sinks.AnomalyStoreSinkEnabled
	}
	penaltyTTL = rule.RuleMetadata.PenaltyTTLSeconds
	s.sinks.send([]string{
		ruleID, strconv.Itoa(version),
		strconv.FormatBool(aggOn),
		strconv.FormatBool(anomalyOn),
		strconv.FormatBool(redisOn),
		strconv.Itoa(penaltyTTL),
	})

	// aggregation_specs.csv — one row per agg (up to 3)
	for i, agg := range rule.Aggregations {
		isHighCard := agg.CardinalityHint == "HIGH"
		s.aggSpecs.send([]string{
			newID(), ruleID, strconv.Itoa(version),
			agg.Alias, agg.Function, agg.Field,
			strconv.FormatBool(isHighCard),
			strconv.Itoa(i + 1),
		})
	}

	// breach_conditions.csv — one row for the JEXL expression
	if rule.HavingThresholds.Expression != "" {
		s.breach.send([]string{
			newID(), ruleID, strconv.Itoa(version),
			rule.HavingThresholds.Expression,
		})
	}
}

func marshalKeys(keys []string) string {
	b, _ := json.Marshal(keys)
	return string(b)
}

func newID() string {
	// Simple time-based UUID-ish ID; replace with uuid lib if available
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
