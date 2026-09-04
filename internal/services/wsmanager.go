package services

import (
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"

	"github.com/gorilla/websocket"
)

// outboxSize bounds how many pending messages a single connection can have
// queued before Broadcast/WriteToConn start dropping messages for it
// instead of blocking the caller. Broadcast's caller is the Kafka consumer
// goroutine, so a slow/stalled client must never be able to stall message
// consumption for every other subscriber.
const outboxSize = 64

// writeDeadline bounds how long a single WriteMessage call may block on the
// network once the writer goroutine picks a message off the outbox — a
// backstop for a connection that accepts writes but drains them slowly.
const writeDeadline = 10 * time.Second

// errOutboxFull is returned by WriteToConn when a connection's outbox is
// already saturated — the client can't keep up even with a heartbeat.
var errOutboxFull = errors.New("websocket outbox full")

// WSManager manages WebSocket connections subscribed to rule IDs.
// Thread-safe with sync.RWMutex.
type WSManager struct {
	mu    sync.RWMutex
	conns map[*websocket.Conn]*wsEntry
}

// wsEntry holds per-connection state: which rule IDs it's subscribed to,
// and a bounded outbox drained by a dedicated writer goroutine (started in
// Connect, stopped in Disconnect) so this connection has exactly one
// writer for its socket's lifetime (gorilla/websocket allows only one
// concurrent writer) and so a slow/stalled client's socket write can never
// block the caller of Broadcast/WriteToConn. A full outbox means the
// client can't keep up — the new message is dropped rather than queued
// without bound (OOM) or blocking the sender.
type wsEntry struct {
	ruleIDs map[string]bool
	outbox  chan []byte
	closed  chan struct{}
	// done is closed by writePump right before it returns (any exit path) —
	// the signal a caller needs before it's safe to write to this
	// connection itself. gorilla/websocket permits only one concurrent
	// writer per connection; writePump is normally that writer, so anyone
	// else touching the connection (see CloseAll) must wait for done first
	// rather than assume closing `closed` was enough on its own.
	done chan struct{}
	once sync.Once
}

func newWsEntry(ruleIDs map[string]bool) *wsEntry {
	return &wsEntry{
		ruleIDs: ruleIDs,
		outbox:  make(chan []byte, outboxSize),
		closed:  make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// enqueue queues data for the connection's writer goroutine, dropping it
// instead of blocking if the connection's outbox is already full.
func (e *wsEntry) enqueue(data []byte) bool {
	select {
	case e.outbox <- data:
		return true
	case <-e.closed:
		return false
	default:
		return false // outbox full — client too slow, drop this message
	}
}

// stop signals the writer goroutine to exit. Safe to call more than once.
func (e *wsEntry) stop() {
	e.once.Do(func() { close(e.closed) })
}

// NewWSManager creates a new WebSocket manager.
func NewWSManager() *WSManager {
	return &WSManager{
		conns: make(map[*websocket.Conn]*wsEntry),
	}
}

// writePump is the only goroutine that ever calls conn.WriteMessage for
// this connection, draining entry.outbox until Disconnect closes
// entry.closed. On a write failure (including hitting writeDeadline) it
// tears the connection down itself rather than waiting for the read
// loop's own heartbeat timeout to notice.
func (m *WSManager) writePump(conn *websocket.Conn, entry *wsEntry) {
	defer close(entry.done)
	for {
		select {
		case data := <-entry.outbox:
			conn.SetWriteDeadline(time.Now().Add(writeDeadline))
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				slog.Error("WebSocket write failed — disconnecting", "error", err)
				m.Disconnect(conn)
				conn.Close()
				return
			}
		case <-entry.closed:
			return
		}
	}
}

// Connect registers a WebSocket connection with subscribed rule IDs and
// starts its dedicated writer goroutine. A no-op on the writer if this
// connection is already registered — only its subscription is updated.
func (m *WSManager) Connect(conn *websocket.Conn, ruleIDs []string) {
	idSet := make(map[string]bool, len(ruleIDs))
	for _, id := range ruleIDs {
		idSet[id] = true
	}

	m.mu.Lock()
	entry, exists := m.conns[conn]
	if exists {
		entry.ruleIDs = idSet
	} else {
		entry = newWsEntry(idSet)
		m.conns[conn] = entry
	}
	m.mu.Unlock()

	if !exists {
		go m.writePump(conn, entry)
	}
	slog.Info("WebSocket connected", "subscribed_rules", len(ruleIDs))
}

// Disconnect removes a WebSocket connection and stops its writer goroutine.
// Safe to call more than once for the same connection.
func (m *WSManager) Disconnect(conn *websocket.Conn) {
	m.mu.Lock()
	entry, ok := m.conns[conn]
	delete(m.conns, conn)
	m.mu.Unlock()
	if ok {
		entry.stop()
	}
	slog.Info("WebSocket disconnected")
}

// WriteToConn queues a message for a specific connection's writer
// goroutine. msgType is accepted for signature compatibility with callers
// but the writer always sends websocket.TextMessage — the only type any
// caller in this codebase ever uses. Returns errOutboxFull if the
// connection can't even accept a heartbeat right now.
func (m *WSManager) WriteToConn(conn *websocket.Conn, msgType int, data []byte) error {
	m.mu.RLock()
	entry, ok := m.conns[conn]
	m.mu.RUnlock()
	if !ok {
		return nil // connection was removed; nothing to do
	}
	if !entry.enqueue(data) {
		return errOutboxFull
	}
	return nil
}

// Broadcast sends a delta message to all connections subscribed to the
// given rule ID. Never blocks on a slow client's socket — this is called
// directly from the Kafka consumer's processing goroutine, so a stalled
// client here must never be able to stall message consumption for every
// other subscriber.
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
	var targets []*wsEntry
	for _, entry := range m.conns {
		if entry.ruleIDs[ruleID] {
			targets = append(targets, entry)
		}
	}
	m.mu.RUnlock()

	for _, entry := range targets {
		entry.enqueue(msg)
	}
}

// closeAllWriterWait bounds how long CloseAll waits for a connection's
// writePump to exit before giving up on sending that one connection a
// close frame — a backstop, not an expected case.
const closeAllWriterWait = 2 * time.Second

// CloseAll sends every connected client a real WebSocket close frame, then
// disconnects it. Used during process shutdown: net/http's Server.Shutdown
// does not track or wait for hijacked connections — which is exactly what
// every WebSocket connection is once upgraded (see gorilla/websocket's
// Upgrader.Upgrade, which calls Hijack) — so without this, an active
// connection would simply die when the process exits, with no close frame
// ever reaching the client. CloseAll gives each client an explicit,
// immediate signal to reconnect instead.
//
// This hands write ownership of each conn from its writePump goroutine to
// this call: it signals the entry closed (stopping writePump from pulling
// more off the outbox) and waits for entry.done before writing the close
// frame itself, since gorilla/websocket permits only one concurrent writer
// per connection and writePump is normally that writer.
func (m *WSManager) CloseAll() {
	m.mu.RLock()
	entries := make(map[*websocket.Conn]*wsEntry, len(m.conns))
	for conn, entry := range m.conns {
		entries[conn] = entry
	}
	m.mu.RUnlock()

	closeMsg := websocket.FormatCloseMessage(websocket.CloseServiceRestart, "server shutting down")
	for conn, entry := range entries {
		entry.stop()
		select {
		case <-entry.done:
			conn.SetWriteDeadline(time.Now().Add(writeDeadline))
			conn.WriteMessage(websocket.CloseMessage, closeMsg)
		case <-time.After(closeAllWriterWait):
			slog.Warn("WebSocket writer did not stop in time — closing without a close frame")
		}
		m.Disconnect(conn)
		conn.Close()
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
