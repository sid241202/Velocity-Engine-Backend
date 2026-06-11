package handlers

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"velocity-engine-control-plane-backend-go/internal/models"
	"velocity-engine-control-plane-backend-go/internal/services"

	"github.com/gin-gonic/gin"
)

// AnalysisHandler handles live-analysis, agg-analysis, historical-test, and historical-analysis endpoints.
type AnalysisHandler struct {
	liveStore *services.LiveStore
}

// NewAnalysisHandler creates a new AnalysisHandler.
func NewAnalysisHandler(ls *services.LiveStore) *AnalysisHandler {
	return &AnalysisHandler{liveStore: ls}
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

// AggAnalysis handles GET /rules/agg-analysis
func (h *AnalysisHandler) AggAnalysis(c *gin.Context) {
	ruleIDsParam := c.Query("rule_ids")
	startTS := c.Query("start_ts")
	endTS := c.Query("end_ts")

	var ids []string
	for _, r := range strings.Split(ruleIDsParam, ",") {
		trimmed := strings.TrimSpace(r)
		if trimmed != "" {
			ids = append(ids, trimmed)
		}
	}

	results, err := services.GetAggResults(ids, startTS, endTS)
	if err != nil {
		slog.Error("Failed to get agg results", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
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
		ruleDict,
		startDtNaive.Format("2006-01-02T15:04:05"),
		endDtNaive.Format("2006-01-02T15:04:05"),
	)
	if err != nil {
		slog.Error("Historical test failed", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Internal server error"})
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

	startTS := c.Query("start_ts")
	endTS := c.Query("end_ts")

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

	results, err := services.RunHistoricalAnalysis(ruleDict, startTS, endTS)
	if err != nil {
		slog.Error("Historical analysis failed", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Internal server error"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"results": results,
	})
}
