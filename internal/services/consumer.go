package services

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
	"velocity-engine-control-plane-backend-go/internal/metrics"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ResultsConsumer subscribes to the Kafka results topic and fans out to:
//   - LiveStore (in-memory ring buffer)
//   - WSManager (WebSocket push to subscribed clients)
type ResultsConsumer struct {
	cancel    context.CancelFunc
	done      chan struct{}
	liveStore *LiveStore
	wsManager *WSManager
	// pool drains raw message bytes off the read loop. See workerPool's doc
	// comment in msgpool.go for why the read loop no longer processes
	// messages itself.
	pool *workerPool
}

// NewResultsConsumer creates a new results consumer.
func NewResultsConsumer(ls *LiveStore, wm *WSManager) *ResultsConsumer {
	return &ResultsConsumer{liveStore: ls, wsManager: wm, done: make(chan struct{})}
}

// Start begins consuming in a background goroutine.
func (rc *ResultsConsumer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	rc.cancel = cancel
	rc.pool = newWorkerPool(config.ResultsTopic, config.ConsumerWorkers, config.ConsumerQueueSize, rc.processMessage)
	go func() {
		defer close(rc.done)
		rc.run(ctx)
		// Drain whatever is still queued before declaring the consumer
		// stopped — the read loop has exited by now, so nothing else can
		// submit.
		rc.pool.stop()
	}()
	slog.Info("Results consumer started",
		"topic", config.ResultsTopic,
		"workers", len(rc.pool.queues))
}

// stopWait bounds how long Stop blocks for the consumer goroutine to
// actually exit before giving up — a backstop so a stuck run() can't hang
// process shutdown forever.
const stopWait = 5 * time.Second

// Stop signals the consumer goroutine to stop and waits (bounded by
// stopWait) for it to actually call Close on its Kafka client. This matters
// during graceful shutdown: a clean Close sends a LeaveGroupRequest so the
// broker rebalances the group immediately, instead of only noticing this
// member's absence after session.timeout.ms — the process exiting before
// that Close happens would turn what should be a clean departure into an
// unclean one.
func (rc *ResultsConsumer) Stop() {
	if rc.cancel != nil {
		rc.cancel()
	}
	select {
	case <-rc.done:
	case <-time.After(stopWait):
		slog.Warn("Results consumer did not stop within timeout during shutdown")
	}
	slog.Info("Results consumer stopped")
}

func (rc *ResultsConsumer) run(ctx context.Context) {
	backoff := 1 * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// One shared group ID for the topic — deliberately NOT suffixed with
		// the pod name. The old per-pod suffix put every pod in its own
		// consumer group, so each pod would receive a full copy of every
		// partition instead of sharing the topic between them: at 2 pods,
		// twice the ingest work and twice the WebSocket fan-out, with both
		// pods holding the same rows. That is a landmine rather than a bug
		// today (replicas: 1), but it costs nothing to remove now, and it
		// would be an unpleasant surprise the first time anyone scaled the
		// deployment up. Group membership is identified by Kafka's own
		// member id, so restarts do not accumulate stale groups.
		groupID := config.ResultsConsumerGroup

		cfg := &kafka.ConfigMap{
			"bootstrap.servers":  config.KafkaBrokers,
			"group.id":           groupID,
			"auto.offset.reset":  "latest",
			"enable.auto.commit": true,
			"session.timeout.ms": 30000,
		}
		for k, v := range kafkaTuning() {
			cfg.SetKey(k, v)
		}
		c, err := kafka.NewConsumer(cfg)
		if err != nil {
			slog.Error("Failed to create Kafka results consumer — will retry", "error", err, "backoff", backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			if backoff < 30*time.Second {
				backoff = time.Duration(float64(backoff) * 1.5)
			}
			continue
		}

		if err := c.SubscribeTopics([]string{config.ResultsTopic}, nil); err != nil {
			slog.Error("Failed to subscribe to results topic — will retry", "error", err)
			c.Close()
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			continue
		}

		backoff = 1 * time.Second // reset on successful connect
		slog.Info("Results consumer subscribed", "topic", config.ResultsTopic)

		for {
			select {
			case <-ctx.Done():
				c.Close()
				return
			default:
			}

			msg, err := c.ReadMessage(1000) // 1s timeout
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				slog.Error("Results consumer read error — reconnecting", "error", err)
				break // inner loop → reconnect outer loop
			}

			metrics.KafkaMessagesConsumedTotal.WithLabelValues(config.ResultsTopic).Inc()
			// consumer_group label is the configured base group name, not
			// the per-pod-suffixed groupID this consumer actually joined
			// Kafka with — using the per-pod id would make this label's
			// cardinality grow with every pod restart/rescale.
			if _, high, wmErr := c.GetWatermarkOffsets(*msg.TopicPartition.Topic, msg.TopicPartition.Partition); wmErr == nil {
				lag := high - int64(msg.TopicPartition.Offset) - 1
				if lag < 0 {
					lag = 0
				}
				metrics.KafkaConsumerLag.WithLabelValues(config.ResultsTopic, config.ResultsConsumerGroup).Set(float64(lag))
			}

			// Hand off to the worker pool, keyed by the record key so all
			// updates for one key stay ordered on one worker.
			if !rc.pool.submit(ctx, msg.Key, msg.Value) {
				c.Close()
				return // context cancelled while waiting for queue space
			}
		}
		c.Close()
	}
}

func (rc *ResultsConsumer) processMessage(value []byte) {
	var row map[string]interface{}
	if err := json.Unmarshal(value, &row); err != nil {
		slog.Error("Failed to unmarshal results message", "error", err)
		return
	}

	metrics.ObserveConsumeDelay(config.ResultsTopic, row)

	// ── Schema normalization ──────────────────────────────────────────────────
	// New Flink schema: aggResult is a pre-serialized JSON string — parse it for richer display
	if aggStr, ok := row["aggResult"].(string); ok {
		var aggMap interface{}
		if err := json.Unmarshal([]byte(aggStr), &aggMap); err == nil {
			row["aggResult"] = aggMap
		}
	}
	// Backward compat: old schema used aggregationResults
	if aggStr, ok := row["aggregationResults"].(string); ok {
		var aggMap interface{}
		if err := json.Unmarshal([]byte(aggStr), &aggMap); err == nil {
			row["aggregationResults"] = aggMap
		}
	}

	// Add event_type discriminator for frontend routing
	row["event_type"] = "agg"

	// Normalize: Flink AggregationResult uses "id" but downstream code expects "ruleId".
	if id, ok := row["id"].(string); ok && id != "" {
		if _, hasRuleId := row["ruleId"]; !hasRuleId {
			row["ruleId"] = id
		}
	}

	// Normalize groupKey: new schema sends "groupKey" field explicitly.
	// Fall back to "entityValue" for backward compatibility with old Flink versions.
	if _, hasGK := row["groupKey"]; !hasGK {
		if ev, ok := row["entityValue"].(string); ok && ev != "" {
			row["groupKey"] = ev
		}
	}

	// Normalize windowStart / windowEnd from Flink schema to the field names
	// the frontend and LiveStore expect.
	if _, hasWS := row["windowStart"]; !hasWS {
		if ws := row["windowStart"]; ws != nil {
			row["windowStart"] = ws
		}
	}

	// ── thresholdBreached normalization ───────────────────────────────────────
	// Flink serializes boolean as JSON true/false. The frontend's isBreached()
	// checks for: row.thresholdBreached === true || row.thresholdBreached === 1.
	// We ensure thresholdBreached is always present in the row sent to LiveStore
	// and the WebSocket so the frontend can rely on it unconditionally.
	//
	// Jackson serializes Java boolean false as JSON false → Go JSON → bool false.
	// Jackson serializes Java boolean true  as JSON true  → Go JSON → bool true.
	// We keep it as-is; the frontend handles both bool and int (1/0).
	if _, hasBreached := row["thresholdBreached"]; !hasBreached {
		// Row predates the thresholdBreached field (old Flink version).
		// Default to false — the frontend will show no breach for these rows
		// which is correct since we cannot re-evaluate without the rule definition.
		row["thresholdBreached"] = false
	}

	// ── isFinal normalization ──────────────────────────────────────────────
	// New Flink schema tags each row true (authoritative, end-of-window) or
	// false (early-fire partial preview). Rows predating this field (old
	// Flink version) are always the settled end-of-window row, so default
	// to true rather than leaving the frontend to guess.
	if _, hasFinal := row["isFinal"]; !hasFinal {
		row["isFinal"] = true
	}

	rc.liveStore.Add(row)

	// New schema uses "id" as rule identifier; old schema used "ruleId"
	ruleID, _ := row["ruleId"].(string)
	if ruleID == "" {
		ruleID, _ = row["id"].(string)
	}
	if ruleID == "" {
		ruleID = "unknown"
	}
	rc.wsManager.Broadcast(ruleID, row)
}
