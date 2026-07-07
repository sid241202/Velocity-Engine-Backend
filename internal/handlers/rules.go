package handlers

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"velocity-engine-control-plane-backend-go/internal/models"
	"velocity-engine-control-plane-backend-go/internal/services"
	"velocity-engine-control-plane-backend-go/internal/store"

	"github.com/gin-gonic/gin"
)

// RulesHandler handles all rule CRUD operations.
type RulesHandler struct {
	mu        sync.RWMutex
	rulesDB   map[string]*models.RuleRecord
	liveStore *services.LiveStore
	wsManager *services.WSManager
	csvStore  *store.CSVStore
}

// NewRulesHandler creates a new RulesHandler.
func NewRulesHandler(ls *services.LiveStore, wm *services.WSManager, cs *store.CSVStore) *RulesHandler {
	return &RulesHandler{
		rulesDB:   make(map[string]*models.RuleRecord),
		liveStore: ls,
		wsManager: wm,
		csvStore:  cs,
	}
}

// ReadRoot handles GET /
func (h *RulesHandler) ReadRoot(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"service": "Velocity Engine Control Plane",
	})
}

// Health handles GET /health (liveness probe — is the process alive?)
// mysql is informational only — it does not affect the "healthy" status,
// since most of this backend's functionality does not depend on MySQL/RBAC.
func (h *RulesHandler) Health(c *gin.Context) {
	mysqlOK, mysqlReason := services.IsMySQLReady()
	mysqlStatus := "ok"
	if !mysqlOK {
		mysqlStatus = mysqlReason
	}
	c.JSON(http.StatusOK, gin.H{
		"status":         "healthy",
		"live_store":     h.liveStore.Stats(),
		"ws_connections": h.wsManager.ConnectionCount(),
		"mysql":          mysqlStatus,
	})
}

// Readyz handles GET /readyz (readiness probe — are all dependencies available?)
// Returns 200 only when ClickHouse is reachable. Returns 503 otherwise.
func (h *RulesHandler) Readyz(c *gin.Context) {
	ready, reason := services.IsReady()
	if ready {
		c.JSON(http.StatusOK, gin.H{
			"status":     "ready",
			"clickhouse": "ok",
			"live_store": h.liveStore.Stats(),
		})
	} else {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "not_ready",
			"reason": reason,
		})
	}
}

// CreateRule handles POST /rules
func (h *RulesHandler) CreateRule(c *gin.Context) {
	var rule models.VelocityRule
	if err := c.ShouldBindJSON(&rule); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid request body: %s", err.Error())})
		return
	}

	ruleID := rule.RuleMetadata.RuleID

	h.mu.Lock()
	_, exists := h.rulesDB[ruleID]
	if exists {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Rule with this ID already exists"})
		return
	}

	// Input validation
	// Auto-default rule_name to rule_id if not provided
	if strings.TrimSpace(rule.RuleMetadata.RuleName) == "" {
		rule.RuleMetadata.RuleName = ruleID
	}
	isNoWindowing := strings.EqualFold(rule.Windowing.Type, "NONE")
	if !isNoWindowing {
		if len(rule.Aggregations) == 0 {
			h.mu.Unlock()
			c.JSON(http.StatusBadRequest, gin.H{"detail": "aggregations list must not be empty"})
			return
		}
		if rule.Windowing.SizeMs <= 0 {
			h.mu.Unlock()
			c.JSON(http.StatusBadRequest, gin.H{"detail": "windowing.size_ms must be > 0"})
			return
		}
		if rule.Windowing.SlideMs <= 0 {
			h.mu.Unlock()
			c.JSON(http.StatusBadRequest, gin.H{"detail": "windowing.slide_ms must be > 0"})
			return
		}
		if rule.Windowing.Type == "SLIDING" && rule.Windowing.SlideMs > rule.Windowing.SizeMs {
			h.mu.Unlock()
			c.JSON(http.StatusBadRequest, gin.H{"detail": "windowing.slide_ms must not exceed windowing.size_ms for SLIDING windows"})
			return
		}
	}

	// Force DRAFT status on creation
	rule.RuleMetadata.Status = "DRAFT"

	rulePayload, err := json.Marshal(rule)
	if err != nil {
		h.mu.Unlock()
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to serialize rule"})
		return
	}

	record := &models.RuleRecord{
		ID:          ruleID,
		Name:        rule.RuleMetadata.RuleName,
		Status:      "DRAFT",
		RulePayload: rulePayload,
		IsPublished: false,
		Version:     1,
	}

	h.rulesDB[ruleID] = record
	h.mu.Unlock()

	// Persist to CSV store (fire-and-forget via channels)
	if h.csvStore != nil {
		h.csvStore.WriteRule(&rule, 1, true)
	}

	c.JSON(http.StatusOK, rule)
}

// ListRules handles GET /rules
func (h *RulesHandler) ListRules(c *gin.Context) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var rules []models.VelocityRule
	for _, r := range h.rulesDB {
		var rule models.VelocityRule
		if err := json.Unmarshal(r.RulePayload, &rule); err != nil {
			slog.Error("Failed to load rule", "id", r.ID, "error", err)
			continue
		}
		rules = append(rules, rule)
	}

	// Return empty array instead of null
	if rules == nil {
		rules = []models.VelocityRule{}
	}

	c.JSON(http.StatusOK, rules)
}

// GetRule handles GET /rules/:rule_id
func (h *RulesHandler) GetRule(c *gin.Context) {
	ruleID := c.Param("rule_id")

	h.mu.RLock()
	record, ok := h.rulesDB[ruleID]
	h.mu.RUnlock()

	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Rule not found"})
		return
	}

	var rule models.VelocityRule
	if err := json.Unmarshal(record.RulePayload, &rule); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to deserialize rule"})
		return
	}

	c.JSON(http.StatusOK, rule)
}

// PublishRule handles POST /rules/:rule_id/prod
func (h *RulesHandler) PublishRule(c *gin.Context) {
	ruleID := c.Param("rule_id")

	h.mu.RLock()
	record, ok := h.rulesDB[ruleID]
	if !ok {
		h.mu.RUnlock()
		c.JSON(http.StatusNotFound, gin.H{"detail": "Rule not found"})
		return
	}

	var ruleDict map[string]interface{}
	if err := json.Unmarshal(record.RulePayload, &ruleDict); err != nil {
		h.mu.RUnlock()
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to deserialize rule"})
		return
	}
	h.mu.RUnlock()

	// Force ACTIVE
	if rm, ok := ruleDict["rule_metadata"].(map[string]interface{}); ok {
		rm["status"] = "ACTIVE"
	}

	// Publish OUTSIDE the lock
	success := services.PublishRule(ruleDict)
	if !success {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to publish rule to Kafka"})
		return
	}

	// Re-acquire lock to update state
	h.mu.Lock()
	record.IsPublished = true
	record.Status = "ACTIVE"
	newPayload, err := json.Marshal(ruleDict)
	if err != nil {
		h.mu.Unlock()
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Internal server error"})
		return
	}
	record.RulePayload = newPayload
	h.mu.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Rule %s moved to PROD (ACTIVE)", ruleID),
	})
}

// StatusUpdateRequest matches the Python StatusUpdateRequest Pydantic model.
type StatusUpdateRequest struct {
	Status string `json:"status"`
}

// UpdateRuleStatus handles POST /rules/:rule_id/status
func (h *RulesHandler) UpdateRuleStatus(c *gin.Context) {
	ruleID := c.Param("rule_id")

	var req StatusUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid request body"})
		return
	}

	if req.Status != "ACTIVE" && req.Status != "PAUSED" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Invalid status. Use ACTIVE or PAUSED."})
		return
	}

	h.mu.RLock()
	record, ok := h.rulesDB[ruleID]
	if !ok {
		h.mu.RUnlock()
		c.JSON(http.StatusNotFound, gin.H{"detail": "Rule not found"})
		return
	}

	var ruleDict map[string]interface{}
	if err := json.Unmarshal(record.RulePayload, &ruleDict); err != nil {
		h.mu.RUnlock()
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to deserialize rule"})
		return
	}
	h.mu.RUnlock()

	if rm, ok := ruleDict["rule_metadata"].(map[string]interface{}); ok {
		rm["status"] = req.Status
	}

	success := services.PublishRule(ruleDict)
	if !success {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to publish status update to Kafka"})
		return
	}

	h.mu.Lock()
	record.Status = req.Status
	newPayload, err := json.Marshal(ruleDict)
	if err != nil {
		h.mu.Unlock()
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Internal server error"})
		return
	}
	record.RulePayload = newPayload
	h.mu.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Rule %s status updated to %s", ruleID, req.Status),
	})
}

// DeleteRule handles DELETE /rules/:rule_id
func (h *RulesHandler) DeleteRule(c *gin.Context) {
	ruleID := c.Param("rule_id")

	h.mu.RLock()
	record, ok := h.rulesDB[ruleID]
	if !ok {
		h.mu.RUnlock()
		c.JSON(http.StatusNotFound, gin.H{"detail": "Rule not found"})
		return
	}
	wasPublished := record.IsPublished

	var ruleDict map[string]interface{}
	if err := json.Unmarshal(record.RulePayload, &ruleDict); err != nil {
		h.mu.RUnlock()
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to deserialize rule"})
		return
	}
	h.mu.RUnlock()

	// If it was ever published, tell Flink to delete its state (outside lock)
	if wasPublished {
		if rm, ok := ruleDict["rule_metadata"].(map[string]interface{}); ok {
			rm["status"] = "DELETED"
		}
		success := services.PublishRule(ruleDict)
		if !success {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to publish DELETED tombstone to Kafka. Safety abort."})
			return
		}
	}

	// Re-acquire lock to remove from memory
	h.mu.Lock()
	if rm, ok := ruleDict["rule_metadata"].(map[string]interface{}); ok {
		rm["status"] = "DELETED"
	}
	newPayload, _ := json.Marshal(ruleDict)
	newVersion := record.Version + 1

	// Write tombstone to CSV
	var ruleToSave models.VelocityRule
	json.Unmarshal(newPayload, &ruleToSave)
	if h.csvStore != nil {
		h.csvStore.WriteRule(&ruleToSave, newVersion, false)
	}

	delete(h.rulesDB, ruleID)
	h.mu.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Rule %s removed from Flink and permanently deleted", ruleID),
	})
}

// LiveResults handles GET /rules/:rule_id/live-results
func (h *RulesHandler) LiveResults(c *gin.Context) {
	ruleID := c.Param("rule_id")

	limit := 100
	if l, ok := c.GetQuery("limit"); ok {
		if _, err := fmt.Sscanf(l, "%d", &limit); err != nil {
			limit = 100
		}
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	results, err := services.GetLiveResults(c.Request.Context(), ruleID, limit)
	if err != nil {
		slog.Error("Failed to get live results", "rule_id", ruleID, "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"rule_id": ruleID,
		"results": results,
	})
}

// UpdateRule handles PUT /rules/:rule_id
// Accepts a full updated VelocityRule payload.
// Only allowed when rule is in DRAFT or PAUSED status.
func (h *RulesHandler) UpdateRule(c *gin.Context) {
	ruleID := c.Param("rule_id")

	var rule models.VelocityRule
	if err := c.ShouldBindJSON(&rule); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid request body: %s", err.Error())})
		return
	}

	// rule_id in payload must match URL param
	if rule.RuleMetadata.RuleID != ruleID {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "rule_id in payload must match URL parameter"})
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	record, ok := h.rulesDB[ruleID]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Rule not found"})
		return
	}

	// Only editable in DRAFT or PAUSED state
	if record.Status != "DRAFT" && record.Status != "PAUSED" {
		c.JSON(http.StatusConflict, gin.H{"detail": fmt.Sprintf("Rule is %s. Pause the rule before editing.", record.Status)})
		return
	}

	// Validations (same as create)
	if strings.TrimSpace(rule.RuleMetadata.RuleName) == "" {
		rule.RuleMetadata.RuleName = ruleID
	}
	isNoWindowing := strings.EqualFold(rule.Windowing.Type, "NONE")
	if !isNoWindowing {
		if len(rule.Aggregations) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "aggregations list must not be empty"})
			return
		}
		if rule.Windowing.SizeMs <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"detail": "windowing.size_ms must be > 0"})
			return
		}
	}

	// Force back to DRAFT on edit
	rule.RuleMetadata.Status = "DRAFT"

	newVersion := record.Version + 1

	rulePayload, err := json.Marshal(rule)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to serialize rule"})
		return
	}

	record.RulePayload = rulePayload
	record.Status = "DRAFT"
	record.Name = rule.RuleMetadata.RuleName
	record.Version = newVersion
	// IsPublished stays as-is (rule was previously published, still tracks history)

	// Persist to CSV store
	if h.csvStore != nil {
		h.csvStore.WriteRule(&rule, newVersion, false)
	}

	slog.Info("Rule updated", "rule_id", ruleID, "version", newVersion)
	c.JSON(http.StatusOK, rule)
}
