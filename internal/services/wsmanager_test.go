package services

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var testUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// newTestWSServer starts an httptest server that upgrades every request to a
// WebSocket and hands the resulting server-side *websocket.Conn to onConnect.
func newTestWSServer(t *testing.T, onConnect func(*websocket.Conn)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		onConnect(conn)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newTestWSManager builds a WSManager and stops its background flusher when
// the test ends, so a test binary running dozens of these doesn't leak a
// ticker goroutine per manager.
func newTestWSManager(t *testing.T) *WSManager {
	t.Helper()
	m := NewWSManager("test")
	t.Cleanup(m.StopFlusher)
	return m
}

// readBatchRows reads one broadcast message and returns its rows. Fan-out is
// batched now (see WSManager.flush), so one message carries a whole flush
// window's worth of rows rather than exactly one row.
func readBatchRows(t *testing.T, c *websocket.Conn) (string, []map[string]interface{}) {
	t.Helper()
	_, data, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("client did not receive a broadcast message: %v", err)
	}
	var msg struct {
		Type   string                   `json:"type"`
		RuleID string                   `json:"rule_id"`
		Rows   []map[string]interface{} `json:"rows"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("received message is not valid JSON: %v", err)
	}
	if msg.Type != "delta_batch" {
		t.Fatalf(`expected type "delta_batch", got %q`, msg.Type)
	}
	return msg.RuleID, msg.Rows
}

// dialTestWS connects a client-side WebSocket to the given httptest server.
func dialTestWS(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// TestWSManager_BroadcastDeliversToSubscriber verifies the basic end-to-end
// path: Connect a real (httptest-backed) connection, Broadcast a row for its
// subscribed rule ID, and confirm the client actually receives a "delta"
// message shaped the way LiveAnalysis.jsx/consumer.go expect.
func TestWSManager_BroadcastDeliversToSubscriber(t *testing.T) {
	m := newTestWSManager(t)
	var serverConn *websocket.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		serverConn = conn
		m.Connect(conn, []string{"R1"})
		wg.Done()
	})
	client := dialTestWS(t, srv)
	wg.Wait()
	defer m.Disconnect(serverConn)

	m.Broadcast("R1", map[string]interface{}{"groupKey": "g1", "aggResult": map[string]interface{}{"count": 5}})

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	ruleID, rows := readBatchRows(t, client)
	if ruleID != "R1" {
		t.Fatalf(`expected rule_id "R1", got %v`, ruleID)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row in the batch, got %d", len(rows))
	}
	if rows[0]["groupKey"] != "g1" {
		t.Fatalf(`expected groupKey "g1", got %v`, rows[0]["groupKey"])
	}
}

// TestWSManager_BroadcastCoalescesRepeatedWindowUpdates covers the batching
// behaviour that makes the fan-out fix work: Flink early-fires the same
// window repeatedly as it fills, so several Broadcast calls for one
// (groupKey, windowStart) inside a single flush interval must collapse into
// ONE row carrying the latest value — not N rows, and not N messages.
func TestWSManager_BroadcastCoalescesRepeatedWindowUpdates(t *testing.T) {
	m := newTestWSManager(t)
	var serverConn *websocket.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		serverConn = conn
		m.Connect(conn, []string{"R1"})
		wg.Done()
	})
	client := dialTestWS(t, srv)
	wg.Wait()
	defer m.Disconnect(serverConn)

	// 100 updates to the SAME window, plus one distinct window.
	for i := 0; i < 100; i++ {
		m.Broadcast("R1", map[string]interface{}{
			"groupKey": "g1", "windowStart": "2026-09-23 10:00:00",
			"count": float64(i), "isFinal": false,
		})
	}
	m.Broadcast("R1", map[string]interface{}{
		"groupKey": "g2", "windowStart": "2026-09-23 10:00:00", "count": float64(1),
	})

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, rows := readBatchRows(t, client)
	if len(rows) != 2 {
		t.Fatalf("expected 101 broadcasts to coalesce into 2 rows (2 distinct windows), got %d", len(rows))
	}
	var g1 map[string]interface{}
	for _, r := range rows {
		if r["groupKey"] == "g1" {
			g1 = r
		}
	}
	if g1 == nil {
		t.Fatal("coalesced batch is missing the g1 window")
	}
	if got := g1["count"].(float64); got != 99 {
		t.Fatalf("coalesced row must carry the LATEST value (99), got %v", got)
	}
}

// TestWSManager_BatchNeverRegressesFinalToPartial verifies the ordering
// backstop also applies to the outgoing batch: once a window's authoritative
// final row is pending, a late early-fire partial for the same window must
// not overwrite it before the flush.
func TestWSManager_BatchNeverRegressesFinalToPartial(t *testing.T) {
	b := newRuleBatch()
	final := map[string]interface{}{"groupKey": "g1", "windowStart": "w", "count": float64(10), "isFinal": true}
	partial := map[string]interface{}{"groupKey": "g1", "windowStart": "w", "count": float64(4), "isFinal": false}

	b.put(rowCompositeKey(final), final, 100)
	b.put(rowCompositeKey(partial), partial, 100)

	rows := b.drain()
	if len(rows) != 1 {
		t.Fatalf("expected 1 coalesced row, got %d", len(rows))
	}
	if got := rows[0]["count"].(float64); got != 10 {
		t.Fatalf("a late partial overwrote the final row: expected count 10, got %v", got)
	}
}

// TestWSManager_ByRuleIndexStaysInStepWithConns verifies the per-rule
// connection index is maintained on every path that mutates subscriptions —
// a stale entry there would either leak memory or broadcast a rule's data to
// a connection that has since unsubscribed from it.
func TestWSManager_ByRuleIndexStaysInStepWithConns(t *testing.T) {
	m := newTestWSManager(t)
	var serverConn *websocket.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		serverConn = conn
		wg.Done()
	})
	_ = dialTestWS(t, srv)
	wg.Wait()

	m.Connect(serverConn, []string{"R1"})
	m.mu.RLock()
	_, inR1 := m.byRule["R1"][serverConn]
	m.mu.RUnlock()
	if !inR1 {
		t.Fatal("connection missing from byRule[R1] after Connect")
	}

	m.Connect(serverConn, []string{"R2"}) // re-subscribe
	m.mu.RLock()
	_, stillR1 := m.byRule["R1"]
	_, inR2 := m.byRule["R2"][serverConn]
	m.mu.RUnlock()
	if stillR1 {
		t.Fatal("byRule still holds an empty R1 bucket after re-subscribing away from it")
	}
	if !inR2 {
		t.Fatal("connection missing from byRule[R2] after re-subscribe")
	}

	m.Disconnect(serverConn)
	m.mu.RLock()
	leftover := len(m.byRule)
	m.mu.RUnlock()
	if leftover != 0 {
		t.Fatalf("byRule leaked %d bucket(s) after Disconnect", leftover)
	}
}

// TestWSManager_BroadcastDoesNotDeliverToUnsubscribed verifies a connection
// subscribed to a different rule ID does not receive the broadcast.
func TestWSManager_BroadcastDoesNotDeliverToUnsubscribed(t *testing.T) {
	m := newTestWSManager(t)
	var serverConn *websocket.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		serverConn = conn
		m.Connect(conn, []string{"OTHER_RULE"})
		wg.Done()
	})
	client := dialTestWS(t, srv)
	wg.Wait()
	defer m.Disconnect(serverConn)

	m.Broadcast("R1", map[string]interface{}{"groupKey": "g1"})

	client.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, _, err := client.ReadMessage(); err == nil {
		t.Fatal("expected no message for an unsubscribed rule ID, but received one")
	}
}

// TestWSManager_MessageOrderingPreservedPerConnection verifies that multiple
// Broadcast calls to the same connection arrive at the client in the same
// order they were sent — the outbox is a single channel drained by exactly
// one writer goroutine, so FIFO ordering per connection must hold even
// though delivery itself is best-effort (see the "no loss" test below for
// what is deliberately NOT guaranteed).
func TestWSManager_MessageOrderingPreservedPerConnection(t *testing.T) {
	m := newTestWSManager(t)
	var serverConn *websocket.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		serverConn = conn
		m.Connect(conn, []string{"R1"})
		wg.Done()
	})
	client := dialTestWS(t, srv)
	wg.Wait()
	defer m.Disconnect(serverConn)

	// Distinct windows, so nothing coalesces and all 50 must arrive —
	// spread across however many flush batches the ticker produces.
	const n = 50
	for i := 0; i < n; i++ {
		m.Broadcast("R1", map[string]interface{}{
			"groupKey": "g1", "windowStart": fmt.Sprintf("w%03d", i), "seq": float64(i),
		})
	}

	client.SetReadDeadline(time.Now().Add(3 * time.Second))
	var seqs []int
	for len(seqs) < n {
		_, rows := readBatchRows(t, client)
		for _, r := range rows {
			seqs = append(seqs, int(r["seq"].(float64)))
		}
	}
	if len(seqs) != n {
		t.Fatalf("expected exactly %d rows across all batches, got %d", n, len(seqs))
	}
	for i, got := range seqs {
		if got != i {
			t.Fatalf("out-of-order delivery at position %d: expected seq %d, got %d", i, i, got)
		}
	}
}

// TestWSManager_ConnectReusesEntryOnResubscribe is the regression test for
// the exact bug this rewrite must not reintroduce: gorilla/websocket allows
// only one concurrent writer per connection, so Connect must never spawn a
// second writePump goroutine for a connection that's already registered —
// re-subscribing (a client sending a new {type:"subscribe"} with different
// rule_ids on an existing socket) must update the existing entry in place.
func TestWSManager_ConnectReusesEntryOnResubscribe(t *testing.T) {
	m := newTestWSManager(t)
	var serverConn *websocket.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		serverConn = conn
		wg.Done()
	})
	_ = dialTestWS(t, srv)
	wg.Wait()

	m.Connect(serverConn, []string{"R1"})
	m.mu.RLock()
	first := m.conns[serverConn]
	m.mu.RUnlock()

	m.Connect(serverConn, []string{"R2"}) // simulates a re-subscribe
	m.mu.RLock()
	second := m.conns[serverConn]
	m.mu.RUnlock()
	defer m.Disconnect(serverConn)

	if first != second {
		t.Fatal("Connect on an already-registered connection replaced the entry instead of reusing it — would spawn a duplicate writePump goroutine")
	}
	if !second.ruleIDs["R2"] || second.ruleIDs["R1"] {
		t.Fatalf("expected subscription updated to {R2}, got %v", second.ruleIDs)
	}
}

// TestWSManager_DisconnectIsIdempotent verifies Disconnect can be called
// more than once for the same connection without panicking — both the
// read-loop's deferred Disconnect and the writer goroutine's self-triggered
// Disconnect (on a write failure) can race to call this for the same conn.
func TestWSManager_DisconnectIsIdempotent(t *testing.T) {
	m := newTestWSManager(t)
	var serverConn *websocket.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		serverConn = conn
		m.Connect(conn, []string{"R1"})
		wg.Done()
	})
	_ = dialTestWS(t, srv)
	wg.Wait()

	m.Disconnect(serverConn)
	m.Disconnect(serverConn) // must not panic (sync.Once + delete-on-missing-key are both idempotent)

	m.mu.RLock()
	_, stillPresent := m.conns[serverConn]
	m.mu.RUnlock()
	if stillPresent {
		t.Fatal("connection still present in the manager after Disconnect")
	}
}

// TestWSManager_EnqueueNeverBlocksAndDropsWhenFull is a direct, deterministic
// test of the actual bug this rewrite fixes: a bounded outbox with a
// non-blocking enqueue. It talks to wsEntry directly (white-box, no writer
// goroutine draining it) so the outcome doesn't depend on network/OS timing —
// filling the outbox past capacity must never block the caller, and once
// full, the message is dropped (returns false) rather than queued unbounded.
func TestWSManager_EnqueueNeverBlocksAndDropsWhenFull(t *testing.T) {
	e := newWsEntry(map[string]bool{"R1": true})
	// No writePump started — nothing ever drains e.outbox.

	done := make(chan bool, outboxSize+10)
	start := time.Now()
	for i := 0; i < outboxSize+10; i++ {
		done <- e.enqueue([]byte("msg"))
	}
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("enqueue blocked instead of dropping when the outbox filled up — took %v for %d calls", elapsed, outboxSize+10)
	}
	close(done)

	accepted := 0
	for ok := range done {
		if ok {
			accepted++
		}
	}
	if accepted != outboxSize {
		t.Fatalf("expected exactly outboxSize (%d) messages accepted before drops began, got %d", outboxSize, accepted)
	}
}

// TestWSManager_BroadcastNeverBlocksCallerOnSlowSubscriber reproduces the
// original bug's exact shape end-to-end: one subscriber whose writer can't
// keep up must never delay Broadcast for other subscribers or for the
// caller (the Kafka consumer's own processing goroutine in production).
// Simulated by never reading on the slow client, so its outbox saturates
// and its writePump eventually blocks on the real network write (bounded by
// writeDeadline) — Broadcast itself must still return immediately regardless.
func TestWSManager_BroadcastNeverBlocksCallerOnSlowSubscriber(t *testing.T) {
	m := newTestWSManager(t)
	var mu sync.Mutex
	var slowConn, fastConn *websocket.Conn
	var wg sync.WaitGroup
	wg.Add(2)
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		defer wg.Done()
		mu.Lock()
		defer mu.Unlock()
		if slowConn == nil {
			slowConn = conn
			m.Connect(conn, []string{"R1"})
			return
		}
		fastConn = conn
		m.Connect(conn, []string{"R1"})
	})
	slowClient := dialTestWS(t, srv)
	fastClient := dialTestWS(t, srv)
	wg.Wait()
	defer m.Disconnect(slowConn)
	defer m.Disconnect(fastConn)

	// Drain the fast client concurrently so it never backs up. Batching
	// means the message count no longer tracks the broadcast count, so this
	// drains until the connection is closed rather than for a fixed count.
	fastDone := make(chan struct{})
	go func() {
		defer close(fastDone)
		for {
			fastClient.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, _, err := fastClient.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// slowClient never reads, so its outbox (and eventually its OS socket
	// buffer) saturates. Broadcast must not slow down regardless. Distinct
	// windows so the batcher can't collapse the load away.
	const n = 20000
	start := time.Now()
	for i := 0; i < n; i++ {
		m.Broadcast("R1", map[string]interface{}{
			"groupKey": "g1", "windowStart": fmt.Sprintf("w%06d", i), "seq": float64(i),
		})
	}
	elapsed := time.Since(start)
	if elapsed > 3*time.Second {
		t.Fatalf("Broadcast was blocked by a slow/non-reading subscriber — took %v for %d calls (writeDeadline is %v)", elapsed, n, writeDeadline)
	}

	fastClient.Close()
	<-fastDone
	_ = slowClient
}

// TestWSManager_ConcurrentStress exercises Connect/Broadcast/WriteToConn/
// Disconnect from many goroutines at once against a shared WSManager and a
// pool of real connections, opening and closing connections throughout.
// Run with `go test -race` — the race detector is the actual assertion here;
// the test itself only needs to keep all code paths busy concurrently.
func TestWSManager_ConcurrentStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}
	m := newTestWSManager(t)

	const numConns = 4
	const numWorkers = 4
	// Fixed per-worker op count, not a wall-clock duration: bounds total
	// work (and log volume — Connect/Disconnect both log) deterministically
	// regardless of how much -race's per-access instrumentation slows down
	// each iteration, instead of scaling iteration count with however much
	// of a time budget happens to be available on a given run/machine.
	const itersPerWorker = 25

	var conns []*websocket.Conn
	var connsMu sync.Mutex
	var connWG sync.WaitGroup
	connWG.Add(numConns)
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		m.Connect(conn, []string{"R1", "R2"})
		connsMu.Lock()
		conns = append(conns, conn)
		connsMu.Unlock()
		connWG.Done()
		// Block here for the connection's lifetime, same as the real
		// LiveAnalysisWS/AnomalyAnalysisWS handlers — nothing to read since
		// this test's clients never send, but exiting early would let the
		// httptest server tear the hijacked connection down mid-test.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})

	var clients []*websocket.Conn
	for i := 0; i < numConns; i++ {
		clients = append(clients, dialTestWS(t, srv))
	}
	connWG.Wait()

	// Drain every client's inbox for the test's duration so a real client
	// reading normally doesn't look like a stalled/backpressured one to the
	// server — this test's job is to exercise the four operations
	// concurrently, not to (re-)prove the already-covered slow-subscriber
	// backpressure behavior, and an undrained client here would make
	// server-side writes fail/reconnect in a tight, uninformative loop.
	var drainWG sync.WaitGroup
	for _, c := range clients {
		drainWG.Add(1)
		go func(c *websocket.Conn) {
			defer drainWG.Done()
			for {
				if _, _, err := c.ReadMessage(); err != nil {
					return
				}
			}
		}(c)
	}

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			connsMu.Lock()
			conn := conns[id%len(conns)]
			connsMu.Unlock()
			for i := 0; i < itersPerWorker; i++ {
				switch id % 3 {
				case 0:
					m.Broadcast("R1", map[string]interface{}{"worker": id, "i": i})
				case 1:
					m.WriteToConn(conn, websocket.TextMessage, []byte(`{"type":"heartbeat"}`))
				case 2:
					m.Connect(conn, []string{"R1", "R2"}) // simulate re-subscribe churn
				}
			}
		}(w)
	}
	wg.Wait()

	for _, c := range clients {
		c.Close()
	}
	drainWG.Wait()
	for _, c := range conns {
		m.Disconnect(c)
	}
}

// TestWSManager_CloseAllSendsCloseFrameAndDisconnects verifies the shutdown
// path: every connected client receives a real WebSocket close frame (not
// just a raw TCP drop) and is removed from the manager. This is what
// cmd/server/main.go calls during SIGTERM handling, since srv.Shutdown
// never sees these hijacked connections at all.
func TestWSManager_CloseAllSendsCloseFrameAndDisconnects(t *testing.T) {
	m := newTestWSManager(t)
	var serverConn *websocket.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		serverConn = conn
		m.Connect(conn, []string{"R1"})
		wg.Done()
	})
	client := dialTestWS(t, srv)
	wg.Wait()

	m.CloseAll()

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := client.ReadMessage()
	closeErr, ok := err.(*websocket.CloseError)
	if !ok {
		t.Fatalf("expected a *websocket.CloseError from CloseAll, got %v (%T)", err, err)
	}
	if closeErr.Code != websocket.CloseServiceRestart {
		t.Fatalf("expected close code %d (CloseServiceRestart), got %d", websocket.CloseServiceRestart, closeErr.Code)
	}

	if m.ConnectionCount() != 0 {
		t.Fatal("connection still registered in the manager after CloseAll")
	}
	_ = serverConn
}

// TestWSManager_CloseAllConcurrentWithBroadcast is the race-detector
// assertion for CloseAll's writer handoff: while CloseAll is closing
// connections, a concurrent Broadcast must never race with CloseAll's own
// direct conn.WriteMessage call — CloseAll only takes over writing to a
// connection after confirming (via entry.done) that connection's writePump
// has actually exited, and Broadcast only ever enqueues onto a channel, so
// this must be race-free. Run with `go test -race`.
func TestWSManager_CloseAllConcurrentWithBroadcast(t *testing.T) {
	m := newTestWSManager(t)
	const numConns = 4
	var conns []*websocket.Conn
	var connsMu sync.Mutex
	var connWG sync.WaitGroup
	connWG.Add(numConns)
	srv := newTestWSServer(t, func(conn *websocket.Conn) {
		m.Connect(conn, []string{"R1"})
		connsMu.Lock()
		conns = append(conns, conn)
		connsMu.Unlock()
		connWG.Done()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	var clients []*websocket.Conn
	for i := 0; i < numConns; i++ {
		clients = append(clients, dialTestWS(t, srv))
	}
	connWG.Wait()

	var drainWG sync.WaitGroup
	for _, c := range clients {
		drainWG.Add(1)
		go func(c *websocket.Conn) {
			defer drainWG.Done()
			for {
				if _, _, err := c.ReadMessage(); err != nil {
					return
				}
			}
		}(c)
	}

	var broadcastWG sync.WaitGroup
	stop := make(chan struct{})
	broadcastWG.Add(1)
	go func() {
		defer broadcastWG.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				m.Broadcast("R1", map[string]interface{}{"i": i})
			}
		}
	}()

	time.Sleep(20 * time.Millisecond) // let a few broadcasts land first
	m.CloseAll()
	close(stop)
	broadcastWG.Wait()

	for _, c := range clients {
		c.Close()
	}
	drainWG.Wait()

	if m.ConnectionCount() != 0 {
		t.Fatal("connections still registered after CloseAll")
	}
}
