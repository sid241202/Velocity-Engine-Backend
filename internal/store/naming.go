package store

import (
	"fmt"
	"strings"

	"velocity-engine-control-plane-backend-go/internal/models"
)

// GenerateRuleName produces a unique, human-readable rule name from the rule payload.
// Format: {AGG_SUMMARY}__{ENTITY}__{WINDOW_TYPE}_{WINDOW_SIZE}__{SHORT_ID}
// Example: SUM_amount+COUNT_DISTINCT_ip__userId__SLD_5m_1m__a3f9c1b2
func GenerateRuleName(rule *models.VelocityRule) string {
	parts := make([]string, 0, len(rule.Aggregations))
	for _, agg := range rule.Aggregations {
		fieldShort := lastPathSegment(agg.Field)
		if agg.Function == "COUNT_DISTINCT" {
			parts = append(parts, fmt.Sprintf("COUNT_DISTINCT_%s", fieldShort))
		} else {
			parts = append(parts, fmt.Sprintf("%s_%s", agg.Function, fieldShort))
		}
	}
	aggSummary := strings.Join(parts, "+")
	if aggSummary == "" {
		aggSummary = "RULE"
	}

	entity := lastPathSegment(rule.Grouping.EntityName)
	if entity == "" {
		if len(rule.Grouping.Keys) > 0 {
			entity = lastPathSegment(rule.Grouping.Keys[0])
		}
		if entity == "" || entity == "__GLOBAL__" {
			entity = "GLOBAL"
		}
	}

	windowPart := formatWindowPart(rule.Windowing)

	shortID := rule.RuleMetadata.RuleID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}

	return fmt.Sprintf("%s__%s__%s__%s", aggSummary, entity, windowPart, shortID)
}

// GenerateRuleDescription produces a human-readable description for the UI.
func GenerateRuleDescription(rule *models.VelocityRule) string {
	aggDescs := make([]string, 0, len(rule.Aggregations))
	for _, agg := range rule.Aggregations {
		field := lastPathSegment(agg.Field)
		switch agg.Function {
		case "COUNT":
			aggDescs = append(aggDescs, fmt.Sprintf("count of %s", field))
		case "COUNT_DISTINCT":
			aggDescs = append(aggDescs, fmt.Sprintf("count of distinct %s", field))
		case "SUM":
			aggDescs = append(aggDescs, fmt.Sprintf("sum of %s", field))
		case "AVG":
			aggDescs = append(aggDescs, fmt.Sprintf("average of %s", field))
		case "MIN":
			aggDescs = append(aggDescs, fmt.Sprintf("min of %s", field))
		case "MAX":
			aggDescs = append(aggDescs, fmt.Sprintf("max of %s", field))
		default:
			aggDescs = append(aggDescs, fmt.Sprintf("%s of %s", strings.ToLower(agg.Function), field))
		}
	}

	var windowDesc string
	w := rule.Windowing
	if w.Type == "SLIDING" {
		windowDesc = fmt.Sprintf("%s sliding window (slide: %s)", humanMs(w.SizeMs), humanMs(w.SlideMs))
	} else {
		windowDesc = fmt.Sprintf("%s tumbling window", humanMs(w.SizeMs))
	}

	entity := lastPathSegment(rule.Grouping.EntityName)
	if entity == "" {
		entity = "(global)"
	}

	return fmt.Sprintf("Computes %s over a %s, monitoring %s.",
		strings.Join(aggDescs, ", "), windowDesc, entity)
}

func lastPathSegment(field string) string {
	if field == "" {
		return ""
	}
	parts := strings.Split(field, ".")
	return parts[len(parts)-1]
}

func formatWindowPart(w models.WindowingConfig) string {
	size := humanMs(w.SizeMs)
	if w.Type == "SLIDING" {
		slide := humanMs(w.SlideMs)
		return fmt.Sprintf("SLD_%s_%s", size, slide)
	}
	return fmt.Sprintf("TBL_%s", size)
}

func humanMs(ms int64) string {
	if ms <= 0 {
		return "0ms"
	}
	if ms < 1000 {
		return fmt.Sprintf("%dms", ms)
	}
	sec := ms / 1000
	if sec < 60 {
		return fmt.Sprintf("%ds", sec)
	}
	min := sec / 60
	if min < 60 {
		remSec := sec % 60
		if remSec == 0 {
			return fmt.Sprintf("%dm", min)
		}
		return fmt.Sprintf("%dm%ds", min, remSec)
	}
	hr := min / 60
	remMin := min % 60
	if remMin == 0 {
		return fmt.Sprintf("%dh", hr)
	}
	return fmt.Sprintf("%dh%dm", hr, remMin)
}
