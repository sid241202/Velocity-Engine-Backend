package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
	"strings"

	"velocity-engine-control-plane-backend-go/internal/services"
	"velocity-engine-control-plane-backend-go/internal/config"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true // Not a CORS request
		}
		if config.CORSOrigins == "*" {
			return true
		}
		for _, o := range strings.Split(config.CORSOrigins, ",") {
			if strings.TrimSpace(o) == origin {
				return true
			}
		}
		return false
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// WSHandler handles WebSocket endpoints.
type WSHandler struct {
	liveStore    *services.LiveStore
	wsManager    *services.WSManager
	anomalyStore *services.AnomalyStore
	anomalyWSMgr *services.WSManager
}

// NewWSHandler creates a new WebSocket handler.
func NewWSHandler(ls *services.LiveStore, wm *services.WSManager, as *services.AnomalyStore, awm *services.WSManager) *WSHandler {
	return &WSHandler{
		liveStore:    ls,
		wsManager:    wm,
		anomalyStore: as,
		anomalyWSMgr: awm,
	}
}

// LiveResultsWS handles WS /ws/live-results/:rule_id
// On connect: accept, then loop every 1 second polling ClickHouse and sending JSON.
func (h *WSHandler) LiveResultsWS(c *gin.Context) {
	ruleID := c.Param("rule_id")

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		slog.Error("WebSocket upgrade failed", "error", err)
		return
	}
	defer conn.Close()

	slog.Info("WebSocket live-results connected", "rule_id", ruleID)

	// Start a goroutine to read and discard messages (required for close detection)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			slog.Info("Client disconnected from live-results", "rule_id", ruleID)
			return
		case <-ticker.C:
			results, err := services.GetLiveResults(ruleID, 100)
			if err != nil {
				slog.Error("Failed to get live results for WebSocket", "rule_id", ruleID, "error", err)
				continue
			}

			msg, err := json.Marshal(map[string]interface{}{
				"rule_id": ruleID,
				"results": results,
			})
			if err != nil {
				slog.Error("Failed to marshal live results", "error", err)
				continue
			}

			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				slog.Error("Failed to send live results", "rule_id", ruleID, "error", err)
				return
			}
		}
	}
}

// LiveAnalysisWS handles WS /ws/live-analysis
// On connect: accept. Wait for subscribe message. Send bootstrap snapshot.
// Then heartbeat at configured interval.
func (h *WSHandler) LiveAnalysisWS(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		slog.Error("WebSocket upgrade failed", "error", err)
		return
	}
	defer func() {
		h.wsManager.Disconnect(conn)
		conn.Close()
	}()

	slog.Info("WebSocket live-analysis connected")

	heartbeatInterval := time.Duration(h.wsManager.HeartbeatInterval()) * time.Second

	for {
		// Set read deadline for heartbeat timeout
		conn.SetReadDeadline(time.Now().Add(heartbeatInterval))

		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				slog.Info("WebSocket live-analysis disconnected normally")
				return
			}
			// Check if it's a timeout (which means we should send heartbeat)
			if netErr, ok := err.(*websocket.CloseError); ok {
				slog.Info("WebSocket closed", "code", netErr.Code)
				return
			}
			// Timeout — send heartbeat
			if isTimeout(err) {
				heartbeatMsg, _ := json.Marshal(map[string]string{"type": "heartbeat"})
				if writeErr := h.wsManager.WriteToConn(conn, websocket.TextMessage, heartbeatMsg); writeErr != nil {
					slog.Error("Failed to send heartbeat", "error", writeErr)
					return
				}
				continue
			}
			slog.Error("WebSocket read error", "error", err)
			return
		}

		// Parse the message
		var msg map[string]interface{}
		if err := json.Unmarshal(message, &msg); err != nil {
			slog.Warn("Invalid WebSocket message", "error", err)
			continue
		}

		if msg["type"] == "subscribe" {
			ruleIDsRaw, _ := msg["rule_ids"].([]interface{})
			var ruleIDs []string
			for _, id := range ruleIDsRaw {
				if s, ok := id.(string); ok {
					ruleIDs = append(ruleIDs, s)
				}
			}

			h.wsManager.Connect(conn, ruleIDs)

			// Send bootstrap snapshot
			snapshot := h.liveStore.GetAll(ruleIDs)
			bootstrapMsg, _ := json.Marshal(map[string]interface{}{
				"type": "bootstrap",
				"data": snapshot,
			})
			if err := h.wsManager.WriteToConn(conn, websocket.TextMessage, bootstrapMsg); err != nil {
				slog.Error("Failed to send bootstrap", "error", err)
				return
			}
		}
	}
}

// isTimeout checks if an error is a timeout error.
func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	// net.Error with Timeout() is the standard way
	type timeoutError interface {
		Timeout() bool
	}
	if te, ok := err.(timeoutError); ok {
		return te.Timeout()
	}
	return false
}

// AnomalyAnalysisWS handles WS /ws/anomaly-analysis
// On connect: accept. Wait for subscribe message {type:"subscribe",rule_ids:[...]}.
// Sends bootstrap snapshot of recent anomaly events, then pushes new ones in real-time.
func (h *WSHandler) AnomalyAnalysisWS(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		slog.Error("AnomalyAnalysisWS upgrade failed", "error", err)
		return
	}
	defer func() {
		h.anomalyWSMgr.Disconnect(conn)
		conn.Close()
	}()

	slog.Info("WebSocket anomaly-analysis connected")

	heartbeatInterval := time.Duration(h.anomalyWSMgr.HeartbeatInterval()) * time.Second

	for {
		conn.SetReadDeadline(time.Now().Add(heartbeatInterval))
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				slog.Info("AnomalyAnalysisWS disconnected normally")
				return
			}
			if isTimeout(err) {
				heartbeatMsg, _ := json.Marshal(map[string]string{"type": "heartbeat"})
				if writeErr := h.anomalyWSMgr.WriteToConn(conn, websocket.TextMessage, heartbeatMsg); writeErr != nil {
					slog.Error("AnomalyAnalysisWS heartbeat failed", "error", writeErr)
					return
				}
				continue
			}
			slog.Error("AnomalyAnalysisWS read error", "error", err)
			return
		}

		var msg map[string]interface{}
		if err := json.Unmarshal(message, &msg); err != nil {
			slog.Warn("Invalid AnomalyAnalysisWS message", "error", err)
			continue
		}

		if msg["type"] == "subscribe" {
			ruleIDsRaw, _ := msg["rule_ids"].([]interface{})
			var ruleIDs []string
			for _, id := range ruleIDsRaw {
				if s, ok := id.(string); ok {
					ruleIDs = append(ruleIDs, s)
				}
			}

			h.anomalyWSMgr.Connect(conn, ruleIDs)

			// Send bootstrap snapshot of recent anomaly events
			var recentAnomalies []map[string]interface{}
			if len(ruleIDs) == 0 {
				recentAnomalies = h.anomalyStore.GetRecent("", 200)
			} else {
				for _, rid := range ruleIDs {
					recentAnomalies = append(recentAnomalies, h.anomalyStore.GetRecent(rid, 50)...)
				}
			}
			bootstrapMsg, _ := json.Marshal(map[string]interface{}{
				"type": "bootstrap",
				"data": recentAnomalies,
			})
			if err := h.anomalyWSMgr.WriteToConn(conn, websocket.TextMessage, bootstrapMsg); err != nil {
				slog.Error("AnomalyAnalysisWS bootstrap send failed", "error", err)
				return
			}
		}
	}
}
