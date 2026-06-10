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

	"github.com/gin-gonic/gin"
)

// RulesHandler handles all rule CRUD operations.
type RulesHandler struct {
	mu        sync.RWMutex
	rulesDB   map[string]*models.RuleRecord
	liveStore *services.LiveStore
	wsManager *services.WSManager
}

// NewRulesHandler creates a new RulesHandler.
func NewRulesHandler(ls *services.LiveStore, wm *services.WSManager) *RulesHandler {
	return &RulesHandler{
		rulesDB:   make(map[string]*models.RuleRecord),
		liveStore: ls,
		wsManager: wm,
	}
}

// ReadRoot handles GET /
func (h *RulesHandler) ReadRoot(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"service": "Velocity Engine Control Plane",
	})
}

// Health handles GET /health
func (h *RulesHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":         "healthy",
		"live_store":     h.liveStore.Stats(),
		"ws_connections": h.wsManager.ConnectionCount(),
	})
}

// CreateRule handles POST /rules
func (h *RulesHandler) CreateRule(c *gin.Context) {
	var rule models.VelocityRule
	if err := c.ShouldBindJSON(&rule); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": fmt.Sprintf("Invalid request body: %s", err.Error())})
		return
	}

	ruleID := rule.RuleMetadata.RuleID

	h.mu.RLock()
	_, exists := h.rulesDB[ruleID]
	h.mu.RUnlock()

	if exists {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Rule with this ID already exists"})
		return
	}

	// Input validation
	if strings.TrimSpace(rule.RuleMetadata.RuleName) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "rule_metadata.rule_name must not be empty"})
		return
	}
	if len(rule.Aggregations) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "aggregations list must not be empty"})
		return
	}
	if rule.Windowing.SizeMs <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "windowing.size_ms must be > 0"})
		return
	}
	if rule.Windowing.SlideMs <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "windowing.slide_ms must be > 0"})
		return
	}
	if rule.Windowing.Type == "SLIDING" && rule.Windowing.SlideMs > rule.Windowing.SizeMs {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "windowing.slide_ms must not exceed windowing.size_ms for SLIDING windows"})
		return
	}

	// Force DRAFT status on creation
	rule.RuleMetadata.Status = "DRAFT"

	rulePayload, err := json.Marshal(rule)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to serialize rule"})
		return
	}

	record := &models.RuleRecord{
		ID:          ruleID,
		Name:        rule.RuleMetadata.RuleName,
		Status:      "DRAFT",
		RulePayload: rulePayload,
		IsPublished: false,
	}

	h.mu.Lock()
	h.rulesDB[ruleID] = record
	h.mu.Unlock()

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

	h.mu.Lock()
	defer h.mu.Unlock()

	record, ok := h.rulesDB[ruleID]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Rule not found"})
		return
	}

	var ruleDict map[string]interface{}
	if err := json.Unmarshal(record.RulePayload, &ruleDict); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to deserialize rule"})
		return
	}

	// Force ACTIVE
	if rm, ok := ruleDict["rule_metadata"].(map[string]interface{}); ok {
		rm["status"] = "ACTIVE"
	}

	success := services.PublishRule(ruleDict)
	if success {
		record.IsPublished = true
		record.Status = "ACTIVE"
		newPayload, _ := json.Marshal(ruleDict)
		record.RulePayload = newPayload
		c.JSON(http.StatusOK, gin.H{
			"status":  "success",
			"message": fmt.Sprintf("Rule %s moved to PROD (ACTIVE)", ruleID),
		})
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to publish rule to Kafka"})
	}
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

	h.mu.Lock()
	defer h.mu.Unlock()

	record, ok := h.rulesDB[ruleID]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Rule not found"})
		return
	}

	var ruleDict map[string]interface{}
	if err := json.Unmarshal(record.RulePayload, &ruleDict); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to deserialize rule"})
		return
	}

	if rm, ok := ruleDict["rule_metadata"].(map[string]interface{}); ok {
		rm["status"] = req.Status
	}

	success := services.PublishRule(ruleDict)
	if success {
		record.Status = req.Status
		newPayload, _ := json.Marshal(ruleDict)
		record.RulePayload = newPayload
		c.JSON(http.StatusOK, gin.H{
			"status":  "success",
			"message": fmt.Sprintf("Rule %s status updated to %s", ruleID, req.Status),
		})
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to publish status update to Kafka"})
	}
}

// DeleteRule handles DELETE /rules/:rule_id
func (h *RulesHandler) DeleteRule(c *gin.Context) {
	ruleID := c.Param("rule_id")

	h.mu.Lock()
	defer h.mu.Unlock()

	record, ok := h.rulesDB[ruleID]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"detail": "Rule not found"})
		return
	}

	var ruleDict map[string]interface{}
	if err := json.Unmarshal(record.RulePayload, &ruleDict); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to deserialize rule"})
		return
	}

	// If it was ever published, tell Flink to delete its state
	if record.IsPublished {
		if rm, ok := ruleDict["rule_metadata"].(map[string]interface{}); ok {
			rm["status"] = "DELETED"
		}
		success := services.PublishRule(ruleDict)
		if !success {
			c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to publish DELETED tombstone to Kafka. Safety abort."})
			return
		}
	}

	// Move rule back to DRAFT status (keep in DB for re-publishing)
	if rm, ok := ruleDict["rule_metadata"].(map[string]interface{}); ok {
		rm["status"] = "DRAFT"
	}
	record.Status = "DRAFT"
	record.IsPublished = false
	newPayload, _ := json.Marshal(ruleDict)
	record.RulePayload = newPayload

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Rule %s removed from Flink and moved back to DRAFT", ruleID),
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

	results, err := services.GetLiveResults(ruleID, limit)
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
