package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
	"velocity-engine-control-plane-backend-go/internal/services"

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

	extendDeadline := startReadLiveness(conn)

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			// Every read error is terminal for this connection — see
			// startReadLiveness. Never loop back into ReadMessage here.
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				slog.Info("WebSocket live-analysis disconnected normally")
			} else if closeErr, ok := err.(*websocket.CloseError); ok {
				slog.Info("WebSocket closed", "code", closeErr.Code)
			} else {
				slog.Info("WebSocket live-analysis read ended", "error", err)
			}
			return
		}
		extendDeadline()

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

// startReadLiveness configures a connection's read side for ping/pong
// liveness and returns a function that pushes the read deadline forward.
//
// This replaces a read loop that treated a read timeout as "send an
// application-level heartbeat and keep reading". That could not work:
// gorilla/websocket treats ANY read error, a deadline timeout included, as
// terminal for the connection's read side — every subsequent ReadMessage
// returns the same cached error immediately instead of blocking. The loop
// therefore spun at full CPU sending heartbeats until gorilla's own
// repeated-read guard fired ("repeated read on failed websocket
// connection") and panicked the handler, killing the connection. In
// practice that meant every Live Stream socket died roughly 30 seconds
// after the client's last inbound message — the client has nothing to send
// after its initial subscribe — and then reconnected, which is a large part
// of the disconnects seen in the logs.
//
// Liveness now uses the protocol's own mechanism: WSManager's writePump
// sends a ping frame every WS_HEARTBEAT_INTERVAL, the client's WebSocket
// stack answers with a pong automatically (browsers do this with no
// application code), and each pong extends the read deadline. A read
// timeout now means what it should — nothing came back for two whole
// intervals — and the connection is closed rather than spun on.
func startReadLiveness(conn *websocket.Conn) func() {
	interval := time.Duration(config.WSHeartbeatSec) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}
	// Two intervals of slack, so a single dropped or delayed pong doesn't
	// tear down a healthy connection.
	wait := 2 * interval
	extend := func() { conn.SetReadDeadline(time.Now().Add(wait)) }
	extend()
	conn.SetPongHandler(func(string) error {
		extend()
		return nil
	})
	return extend
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

	extendDeadline := startReadLiveness(conn)

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				slog.Info("AnomalyAnalysisWS disconnected normally")
			} else {
				slog.Info("AnomalyAnalysisWS read ended", "error", err)
			}
			return
		}
		extendDeadline()

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
