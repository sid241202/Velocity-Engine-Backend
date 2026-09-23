package services

import (
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
	"velocity-engine-control-plane-backend-go/internal/metrics"

	"github.com/gorilla/websocket"
)

// outboxSize bounds how many pending messages a single connection can have
// queued before Broadcast/WriteToConn start dropping messages for it
// instead of blocking the caller. A slow/stalled client must never be able
// to stall message consumption for every other subscriber.
//
// Each slot now holds a whole flush window's worth of coalesced rows (see
// the batching notes on Broadcast), not a single row, so the same depth
// absorbs far more real traffic than it did when every Kafka message got
// its own slot. Configurable via WS_OUTBOX_SIZE.
var outboxSize = config.WSOutboxSize

// writeDeadline bounds how long a single WriteMessage call may block on the
// network once the writer goroutine picks a message off the outbox — a
// backstop for a connection that accepts writes but drains them slowly.
// Configurable via WS_WRITE_DEADLINE_SECONDS.
var writeDeadline = time.Duration(config.WSWriteDeadlineSec) * time.Second

// ErrOutboxFull is returned by WriteToConn when a connection's outbox is
// already saturated. Exported so callers (internal/handlers/websocket.go's
// heartbeat senders) can distinguish this specific, often-benign condition
// — the outbox is full of real data, which is itself proof the connection
// is alive — from a genuine write/connection failure, which is not benign
// and should still disconnect. Do not treat this as equivalent to a dead
// connection.
var ErrOutboxFull = errors.New("websocket outbox full")

// WSManager manages WebSocket connections subscribed to rule IDs.
// Thread-safe with sync.RWMutex.
type WSManager struct {
	mu    sync.RWMutex
	conns map[*websocket.Conn]*wsEntry
	// byRule indexes connections by the rule IDs they subscribe to, so a
	// broadcast for one rule touches only that rule's subscribers. Broadcast
	// previously scanned every connected client on every single event, which
	// made fan-out cost O(events x connections) — the largest single
	// amplifier of both client-side stutter and outbox-full drops. Kept
	// exactly in step with conns under the same lock.
	byRule map[string]map[*websocket.Conn]*wsEntry
	// name labels this manager's metrics ("live" or "anomaly" — see
	// cmd/server/main.go's two NewWSManager calls). Fixed at 2 values.
	name string

	// ── Batched fan-out ──────────────────────────────────────────────────
	// Updates accumulate per rule and are flushed on a ticker as ONE message
	// per rule, instead of one frame (plus one json.Marshal, plus one
	// connection scan) per Kafka message. Under peak ingest that collapses
	// thousands of tiny frames per second into a handful of consolidated
	// ones, which is what the client can actually render.
	batchMu sync.Mutex
	pending map[string]*ruleBatch

	stopFlush chan struct{}
	flushDone chan struct{}
	stopOnce  sync.Once
}

// ruleBatch accumulates one rule's updates between flushes, coalescing by
// (groupKey, windowStart) the same way LiveStore.Add and the frontend's own
// delta buffer do. Flink early-fires a window repeatedly as it fills, so
// within a 250ms flush window the same window can be updated several times —
// only the latest state of each is worth sending. order preserves arrival
// order of distinct windows so the client sees a stable sequence.
type ruleBatch struct {
	order []string
	rows  map[string]map[string]interface{}
}

func newRuleBatch() *ruleBatch {
	return &ruleBatch{rows: make(map[string]map[string]interface{})}
}

// put adds or coalesces a row, reporting whether it had to drop the oldest
// pending row to stay within the batch cap.
func (b *ruleBatch) put(key string, row map[string]interface{}, maxRows int) (dropped bool) {
	if existing, ok := b.rows[key]; ok {
		// Same window updated again before the flush — replace in place,
		// keeping its original position in order.
		if shouldReplace(existing, row) {
			b.rows[key] = row
		}
		return false
	}
	if maxRows > 0 && len(b.order) >= maxRows {
		// Genuinely distinct rows beyond the cap: drop the oldest so the
		// freshest state still gets through, rather than stalling ingestion
		// or letting one flush grow without bound.
		oldest := b.order[0]
		b.order = b.order[1:]
		delete(b.rows, oldest)
		dropped = true
	}
	b.order = append(b.order, key)
	b.rows[key] = row
	return dropped
}

// drain returns the batch's rows in arrival order.
func (b *ruleBatch) drain() []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(b.order))
	for _, k := range b.order {
		if row, ok := b.rows[k]; ok {
			out = append(out, row)
		}
	}
	return out
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

// NewWSManager creates a new WebSocket manager. name labels this manager's
// metrics (e.g. "live" or "anomaly").
func NewWSManager(name string) *WSManager {
	m := &WSManager{
		conns:     make(map[*websocket.Conn]*wsEntry),
		byRule:    make(map[string]map[*websocket.Conn]*wsEntry),
		pending:   make(map[string]*ruleBatch),
		name:      name,
		stopFlush: make(chan struct{}),
		flushDone: make(chan struct{}),
	}
	go m.runFlusher()
	return m
}

// runFlusher drives the batched fan-out on a fixed interval
// (WS_BROADCAST_INTERVAL_MS, default 250ms — deliberately matching the
// frontend's own delta-flush cadence in LiveAnalysis.jsx).
func (m *WSManager) runFlusher() {
	defer close(m.flushDone)
	interval := time.Duration(config.WSBroadcastIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.flush()
		case <-m.stopFlush:
			m.flush() // don't strand whatever accumulated since the last tick
			return
		}
	}
}

// StopFlusher stops the batching goroutine after one final flush. Safe to
// call more than once.
func (m *WSManager) StopFlusher() {
	m.stopOnce.Do(func() {
		close(m.stopFlush)
		<-m.flushDone
	})
}

// writePump is the only goroutine that ever calls conn.WriteMessage for
// this connection, draining entry.outbox until Disconnect closes
// entry.closed. On a write failure (including hitting writeDeadline) it
// tears the connection down itself rather than waiting for the read
// loop's own heartbeat timeout to notice.
func (m *WSManager) writePump(conn *websocket.Conn, entry *wsEntry) {
	defer close(entry.done)

	// Liveness is driven from here, the goroutine that owns the socket's
	// write side, rather than from the read loop. Every tick sends a real
	// protocol-level ping frame, which browsers answer with a pong
	// automatically — that pong is what keeps the read side's deadline
	// rolling forward (see internal/handlers/websocket.go).
	interval := time.Duration(config.WSHeartbeatSec) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ping := time.NewTicker(interval)
	defer ping.Stop()

	fail := func(err error, what string) {
		slog.Error("WebSocket write failed — disconnecting", "error", err, "kind", what)
		m.Disconnect(conn)
		conn.Close()
	}

	for {
		select {
		case data := <-entry.outbox:
			conn.SetWriteDeadline(time.Now().Add(writeDeadline))
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				fail(err, "data")
				return
			}
		case <-ping.C:
			// The ping is written directly rather than queued, so a
			// connection that is merely busy can never miss its liveness
			// signal just because its outbox is backed up with real data.
			conn.SetWriteDeadline(time.Now().Add(writeDeadline))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				fail(err, "ping")
				return
			}
			// The application-level heartbeat is kept for any client that
			// watches for it, but it goes through the outbox so it still
			// respects backpressure — a full outbox is real data in flight,
			// which is itself proof of liveness, so skipping this one is
			// fine (the ping above already went out regardless).
			if !entry.enqueue(heartbeatFrame) {
				metrics.WebSocketHeartbeatSkippedTotal.WithLabelValues(m.name).Inc()
			}
		case <-entry.closed:
			return
		}
	}
}

// heartbeatFrame is the application-level heartbeat payload, marshalled once
// at startup rather than per tick per connection.
var heartbeatFrame = []byte(`{"type":"heartbeat"}`)

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
		// Re-subscribing: drop this connection out of its old rules' buckets
		// before indexing it under the new ones. LiveAnalysis.jsx sends a
		// fresh subscribe whenever the selected rule changes, so this is a
		// routine path, not an edge case.
		for rid := range entry.ruleIDs {
			m.unindexLocked(rid, conn)
		}
		entry.ruleIDs = idSet
	} else {
		entry = newWsEntry(idSet)
		m.conns[conn] = entry
	}
	for rid := range idSet {
		bucket := m.byRule[rid]
		if bucket == nil {
			bucket = make(map[*websocket.Conn]*wsEntry)
			m.byRule[rid] = bucket
		}
		bucket[conn] = entry
	}
	m.mu.Unlock()

	if !exists {
		go m.writePump(conn, entry)
		metrics.WebSocketActiveConnections.WithLabelValues(m.name).Inc()
	}
	slog.Info("WebSocket connected", "subscribed_rules", len(ruleIDs))
}

// Disconnect removes a WebSocket connection and stops its writer goroutine.
// Safe to call more than once for the same connection.
func (m *WSManager) Disconnect(conn *websocket.Conn) {
	m.mu.Lock()
	entry, ok := m.conns[conn]
	delete(m.conns, conn)
	if ok {
		for rid := range entry.ruleIDs {
			m.unindexLocked(rid, conn)
		}
	}
	m.mu.Unlock()
	if ok {
		entry.stop()
		metrics.WebSocketActiveConnections.WithLabelValues(m.name).Dec()
	}
	slog.Info("WebSocket disconnected")
}

// WriteToConn queues a message for a specific connection's writer
// goroutine. msgType is accepted for signature compatibility with callers
// but the writer always sends websocket.TextMessage — the only type any
// caller in this codebase ever uses. Returns ErrOutboxFull if the
// connection can't even accept a heartbeat right now.
func (m *WSManager) WriteToConn(conn *websocket.Conn, msgType int, data []byte) error {
	m.mu.RLock()
	entry, ok := m.conns[conn]
	m.mu.RUnlock()
	if !ok {
		return nil // connection was removed; nothing to do
	}
	if !entry.enqueue(data) {
		return ErrOutboxFull
	}
	return nil
}

// unindexLocked removes one connection from one rule's bucket, dropping the
// bucket entirely once empty so byRule can't grow without bound as rules come
// and go. Must be called with m.mu held (write).
func (m *WSManager) unindexLocked(ruleID string, conn *websocket.Conn) {
	bucket := m.byRule[ruleID]
	if bucket == nil {
		return
	}
	delete(bucket, conn)
	if len(bucket) == 0 {
		delete(m.byRule, ruleID)
	}
}

// Broadcast queues one row for delivery to every connection subscribed to
// the given rule ID. It no longer writes to any socket itself: the row is
// coalesced into that rule's pending batch and sent by runFlusher on the
// next tick as part of a single consolidated message.
//
// This is what the fix hinges on. Previously every Kafka message meant a
// fresh json.Marshal, a linear scan of every connected client, and a
// separate WebSocket frame — at ~1,000+ msg/sec that is what saturated the
// per-connection outbox (websocket_dropped_messages_total), pushed slow
// writes past the write deadline into forced disconnects, and flooded the
// browser with more updates than it could render. Now that cost is paid
// once per rule per flush interval regardless of ingest rate.
//
// Never blocks the caller on a slow client — the batch lock is held only
// for a map write.
func (m *WSManager) Broadcast(ruleID string, row map[string]interface{}) {
	key := rowCompositeKey(row)

	m.batchMu.Lock()
	batch := m.pending[ruleID]
	if batch == nil {
		batch = newRuleBatch()
		m.pending[ruleID] = batch
	}
	dropped := batch.put(key, row, config.WSBroadcastMaxBatchRows)
	m.batchMu.Unlock()

	if dropped {
		metrics.WebSocketDroppedTotal.WithLabelValues(m.name, "batch_overflow").Inc()
	}
}

// flush marshals each rule's accumulated rows into one message and hands it
// to the subscribed connections' writer goroutines.
func (m *WSManager) flush() {
	m.batchMu.Lock()
	if len(m.pending) == 0 {
		m.batchMu.Unlock()
		return
	}
	batches := m.pending
	m.pending = make(map[string]*ruleBatch, len(batches))
	m.batchMu.Unlock()

	start := time.Now()
	defer func() {
		metrics.WebSocketBroadcastDuration.WithLabelValues(m.name).Observe(time.Since(start).Seconds())
	}()

	for ruleID, batch := range batches {
		rows := batch.drain()
		if len(rows) == 0 {
			continue
		}

		// Snapshot just this rule's subscribers — O(subscribers to this
		// rule), not O(all connections).
		m.mu.RLock()
		bucket := m.byRule[ruleID]
		if len(bucket) == 0 {
			m.mu.RUnlock()
			continue // nobody is watching this rule; drop the batch
		}
		targets := make([]*wsEntry, 0, len(bucket))
		for _, entry := range bucket {
			targets = append(targets, entry)
		}
		m.mu.RUnlock()

		// Marshal once per rule per flush, not once per row.
		msg, err := json.Marshal(map[string]interface{}{
			"type":    "delta_batch",
			"rule_id": ruleID,
			"rows":    rows,
		})
		if err != nil {
			slog.Error("Failed to marshal broadcast batch", "error", err, "rule_id", ruleID)
			continue
		}

		for _, entry := range targets {
			if !entry.enqueue(msg) {
				metrics.WebSocketDroppedTotal.WithLabelValues(m.name, "outbox_full").Inc()
			}
		}
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
	// Stop batching first (with a final flush) so anything already
	// accumulated reaches clients before their sockets are torn down.
	m.StopFlusher()

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
