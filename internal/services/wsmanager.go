package services

import (
	"encoding/json"
	"log/slog"
	"sync"

	"velocity-engine-control-plane-backend-go/internal/config"

	"github.com/gorilla/websocket"
)

// wsConn wraps a websocket.Conn with a per-connection write mutex.
// gorilla/websocket supports only one concurrent writer per connection.
type wsConn struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (wc *wsConn) WriteMessage(msgType int, data []byte) error {
	wc.writeMu.Lock()
	defer wc.writeMu.Unlock()
	return wc.conn.WriteMessage(msgType, data)
}

// WSManager manages WebSocket connections subscribed to rule IDs.
// Thread-safe with sync.RWMutex.
type WSManager struct {
	mu    sync.RWMutex
	conns map[*websocket.Conn]*wsEntry // conn -> entry with ruleIDs and write mutex
}

// wsEntry holds per-connection state.
type wsEntry struct {
	ruleIDs map[string]bool
	writeMu sync.Mutex
}

// NewWSManager creates a new WebSocket manager.
func NewWSManager() *WSManager {
	return &WSManager{
		conns: make(map[*websocket.Conn]*wsEntry),
	}
}

// Connect registers a WebSocket connection with subscribed rule IDs.
func (m *WSManager) Connect(conn *websocket.Conn, ruleIDs []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idSet := make(map[string]bool, len(ruleIDs))
	for _, id := range ruleIDs {
		idSet[id] = true
	}
	if entry, ok := m.conns[conn]; ok {
		// Update existing subscription
		entry.ruleIDs = idSet
	} else {
		m.conns[conn] = &wsEntry{ruleIDs: idSet}
	}
	slog.Info("WebSocket connected", "subscribed_rules", len(ruleIDs))
}

// Disconnect removes a WebSocket connection.
func (m *WSManager) Disconnect(conn *websocket.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conns, conn)
	slog.Info("WebSocket disconnected")
}

// WriteToConn safely writes a message to a specific connection using its write mutex.
func (m *WSManager) WriteToConn(conn *websocket.Conn, msgType int, data []byte) error {
	m.mu.RLock()
	entry, ok := m.conns[conn]
	m.mu.RUnlock()
	if !ok {
		return nil // connection was removed
	}
	entry.writeMu.Lock()
	defer entry.writeMu.Unlock()
	return conn.WriteMessage(msgType, data)
}

// Broadcast sends a delta message to all connections subscribed to the given rule ID.
func (m *WSManager) Broadcast(ruleID string, row map[string]interface{}) {
	msg, err := json.Marshal(map[string]interface{}{
		"type":    "delta",
		"rule_id": ruleID,
		"row":     row,
	})
	if err != nil {
		slog.Error("Failed to marshal broadcast message", "error", err)
		return
	}

	m.mu.RLock()
	type target struct {
		conn  *websocket.Conn
		entry *wsEntry
	}
	var targets []target
	for conn, entry := range m.conns {
		if entry.ruleIDs[ruleID] {
			targets = append(targets, target{conn, entry})
		}
	}
	m.mu.RUnlock()

	for _, t := range targets {
		t.entry.writeMu.Lock()
		err := t.conn.WriteMessage(websocket.TextMessage, msg)
		t.entry.writeMu.Unlock()
		if err != nil {
			slog.Error("Failed to send broadcast to WebSocket", "error", err)
			m.Disconnect(t.conn)
		}
	}
}

// ConnectionCount returns the number of active connections.
func (m *WSManager) ConnectionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}

// HeartbeatInterval returns the configured heartbeat interval in seconds.
func (m *WSManager) HeartbeatInterval() int {
	return config.WSHeartbeatSec
}
