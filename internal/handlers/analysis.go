package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"velocity-engine-control-plane-backend-go/internal/models"
	"velocity-engine-control-plane-backend-go/internal/services"

	"github.com/gin-gonic/gin"
)

// respondHistoricalError maps a historical-query service error to the right HTTP
// status: ErrHistoricalEngineBusy is a transient capacity condition (the single
// DuckDB slot was occupied) and must be retryable (503), not a 500. Everything
// else is an internal error. The real error is logged by the caller; the client
// only sees a generic, safe message.
func respondHistoricalError(c *gin.Context, err error) {
	if errors.Is(err, services.ErrHistoricalEngineBusy) {
		c.Header("Retry-After", "5")
		c.JSON(http.StatusServiceUnavailable, gin.H{"detail": "The historical query engine is busy. Please retry in a few seconds."})
		return
	}
	c.JSON(http.StatusInternalServerError, gin.H{"detail": "Internal server error"})
}

// AnalysisHandler handles live-analysis, agg-analysis, historical-test, and historical-analysis endpoints.
type AnalysisHandler struct {
	liveStore    *services.LiveStore
	anomalyStore *services.AnomalyStore
}

// NewAnalysisHandler creates a new AnalysisHandler.
func NewAnalysisHandler(ls *services.LiveStore, as *services.AnomalyStore) *AnalysisHandler {
	return &AnalysisHandler{liveStore: ls, anomalyStore: as}
}

// LiveAnalysis handles GET /rules/live-analysis
func (h *AnalysisHandler) LiveAnalysis(c *gin.Context) {
	ruleIDsParam := c.Query("rule_ids")
	hoursStr := c.DefaultQuery("hours", "24")

	hours, err := strconv.Atoi(hoursStr)
	if err != nil || hours <= 0 || hours > 168 {
		hours = 24
	}
	_ = hours // hours is used for LiveStore time-based filtering in future, currently GetAll returns all data

	var ids []string
	for _, r := range strings.Split(ruleIDsParam, ",") {
		trimmed := strings.TrimSpace(r)
		if trimmed != "" {
			ids = append(ids, trimmed)
		}
	}

	results := h.liveStore.GetAll(ids)
	c.JSON(http.StatusOK, gin.H{"results": results})
}

// AnomalyAnalysis handles GET /rules/anomaly-analysis
// Returns recent anomaly events from the in-memory ring buffer (fed from Kafka anomaly topic).
// Query params:
//   - rule_ids: comma-separated list of rule IDs to filter by (empty = all)
//   - limit:    max number of anomaly events to return (default 100, max 500)
func (h *AnalysisHandler) AnomalyAnalysis(c *gin.Context) {
	ruleIDsParam := c.Query("rule_ids")
	limitStr := c.DefaultQuery("limit", "100")

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 || limit > 500 {
		limit = 100
	}

	var ids []string
	for _, r := range strings.Split(ruleIDsParam, ",") {
		trimmed := strings.TrimSpace(r)
		if trimmed != "" {
			ids = append(ids, trimmed)
		}
	}

	if h.anomalyStore == nil {
		c.JSON(http.StatusOK, gin.H{"results": []interface{}{}, "count": 0})
		return
	}

	var result []map[string]interface{}
	if len(ids) == 0 {
		result = h.anomalyStore.GetRecent("", limit)
	} else {
		for _, id := range ids {
			events := h.anomalyStore.GetRecent(id, limit)
			result = append(result, events...)
		}
	}

	if result == nil {
		result = []map[string]interface{}{}
	}

	slog.Info("AnomalyAnalysis request", "rule_ids", ids, "limit", limit, "returned", len(result))
	c.JSON(http.StatusOK, gin.H{
		"results": result,
		"count":   len(result),
	})
}


func (h *AnalysisHandler) AggAnalysis(c *gin.Context) {
	ruleIDsParam := c.Query("rule_ids")
	// URL-decode timestamps: the frontend sends encodeURIComponent("YYYY-MM-DD HH:MM:SS")
	// so the space becomes %20 and must be decoded before passing to parseDateTimeBestEffort.
	startTSRaw, _ := url.QueryUnescape(c.Query("start_ts"))
	endTSRaw, _ := url.QueryUnescape(c.Query("end_ts"))
	startTS := strings.TrimSpace(startTSRaw)
	endTS := strings.TrimSpace(endTSRaw)

	var ids []string
	for _, r := range strings.Split(ruleIDsParam, ",") {
		trimmed := strings.TrimSpace(r)
		if trimmed != "" {
			ids = append(ids, trimmed)
		}
	}

	// Input validation: require at least one rule ID and valid time range
	if len(ids) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Please select at least one rule to analyze."})
		return
	}
	if startTS == "" || endTS == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Start and end timestamps are required."})
		return
	}

	slog.Info("AggAnalysis request", "rule_ids", ids, "start_ts", startTS, "end_ts", endTS)
	results, err := services.GetAggResults(c.Request.Context(), ids, startTS, endTS)
	if err != nil {
		slog.Error("Failed to get agg results", "error", err)
		// Return user-friendly message — real error is already logged above
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to query aggregated analytics. Please try again or contact support."})
		return
	}

	c.JSON(http.StatusOK, gin.H{"results": results})
}

// HistoricalTest handles POST /rules/historical-test
func (h *AnalysisHandler) HistoricalTest(c *gin.Context) {
	var rule models.VelocityRule
	if err := c.ShouldBindJSON(&rule); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid request body: %s", err.Error())})
		return
	}

	daysBackStr := c.DefaultQuery("days_back", "7")
	daysBack, err := strconv.Atoi(daysBackStr)
	if err != nil || daysBack <= 0 || daysBack > 90 {
		daysBack = 7
	}

	// IST timezone: UTC+5:30
	ist := time.FixedZone("IST", 5*60*60+30*60)
	endDt := time.Now().In(ist)
	// Strip timezone to match Python datetime.now(IST).replace(tzinfo=None)
	endDtNaive := time.Date(endDt.Year(), endDt.Month(), endDt.Day(),
		endDt.Hour(), endDt.Minute(), endDt.Second(), 0, time.UTC)
	startDtNaive := endDtNaive.Add(-time.Duration(daysBack) * 24 * time.Hour)

	// Convert rule to map[string]interface{} for DuckDB worker
	ruleJSON, err := json.Marshal(rule)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to serialize rule"})
		return
	}
	var ruleDict map[string]interface{}
	if err := json.Unmarshal(ruleJSON, &ruleDict); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to parse rule"})
		return
	}

	results, err := services.RunHistoricalAnalysis(
		c.Request.Context(),
		ruleDict,
		startDtNaive.Format("2006-01-02T15:04:05"),
		endDtNaive.Format("2006-01-02T15:04:05"),
	)
	if err != nil {
		slog.Error("Historical test failed", "error", err)
		respondHistoricalError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"results": results,
	})
}

// HistoricalAnalysis handles POST /rules/historical-analysis
func (h *AnalysisHandler) HistoricalAnalysis(c *gin.Context) {
	var rule models.VelocityRule
	if err := c.ShouldBindJSON(&rule); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid request body: %s", err.Error())})
		return
	}

	// URL-decode timestamps: the frontend sends encodeURIComponent("YYYY-MM-DD HH:MM:SS")
	// The Go backend's parseIST() expects "YYYY-MM-DD HH:MM:SS" (space-separated, IST naive).
	startTSRaw, _ := url.QueryUnescape(c.Query("start_ts"))
	endTSRaw, _ := url.QueryUnescape(c.Query("end_ts"))
	startTS := strings.TrimSpace(startTSRaw)
	endTS := strings.TrimSpace(endTSRaw)

	if startTS == "" || endTS == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "start_ts and end_ts are required query parameters"})
		return
	}

	slog.Info("HistoricalAnalysis request", "start_ts", startTS, "end_ts", endTS)

	// Convert rule to map[string]interface{} for DuckDB worker
	ruleJSON, err := json.Marshal(rule)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to serialize rule"})
		return
	}
	var ruleDict map[string]interface{}
	if err := json.Unmarshal(ruleJSON, &ruleDict); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to parse rule"})
		return
	}

	results, err := services.RunHistoricalAnalysis(c.Request.Context(), ruleDict, startTS, endTS)
	if err != nil {
		slog.Error("Historical analysis failed", "error", err)
		respondHistoricalError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"results": results,
	})
}

// HistoricalBreakdown handles POST /rules/historical-breakdown
// Returns forensic drill-down breakdowns (modality mix, auth outcome,
// geographic hotspot, fingerprint match-score histogram) over the same
// matched rows RunHistoricalAnalysis would use for this rule/time range.
func (h *AnalysisHandler) HistoricalBreakdown(c *gin.Context) {
	var rule models.VelocityRule
	if err := c.ShouldBindJSON(&rule); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid request body: %s", err.Error())})
		return
	}

	startTSRaw, _ := url.QueryUnescape(c.Query("start_ts"))
	endTSRaw, _ := url.QueryUnescape(c.Query("end_ts"))
	startTS := strings.TrimSpace(startTSRaw)
	endTS := strings.TrimSpace(endTSRaw)

	if startTS == "" || endTS == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "start_ts and end_ts are required query parameters"})
		return
	}

	ruleJSON, err := json.Marshal(rule)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to serialize rule"})
		return
	}
	var ruleDict map[string]interface{}
	if err := json.Unmarshal(ruleJSON, &ruleDict); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to parse rule"})
		return
	}

	slog.Info("HistoricalBreakdown request", "start_ts", startTS, "end_ts", endTS)

	result, err := services.RunHistoricalBreakdown(c.Request.Context(), ruleDict, startTS, endTS)
	if err != nil {
		slog.Error("Historical breakdown failed", "error", err)
		respondHistoricalError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"result": result,
	})
}
