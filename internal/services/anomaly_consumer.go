package services

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
	"velocity-engine-control-plane-backend-go/internal/metrics"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// AnomalyStore holds a bounded ring-buffer of recent anomaly events in memory.
// It is written by AnomalyConsumer and read by the anomaly WebSocket handler.
type AnomalyStore struct {
	mu     sync.RWMutex
	events []map[string]interface{}
	maxLen int
}

func NewAnomalyStore(maxLen int) *AnomalyStore {
	return &AnomalyStore{events: make([]map[string]interface{}, 0, min(maxLen, 1000)), maxLen: maxLen}
}

func (as *AnomalyStore) Add(event map[string]interface{}) {
	as.mu.Lock()
	defer as.mu.Unlock()
	as.events = append(as.events, event)
	if len(as.events) > as.maxLen {
		// Copy to a new slice to release the old backing array
		trimmed := make([]map[string]interface{}, as.maxLen)
		copy(trimmed, as.events[len(as.events)-as.maxLen:])
		as.events = trimmed
	}
}

// GetRecent returns up to n most-recent anomaly events, optionally filtered by ruleID (empty = all).
func (as *AnomalyStore) GetRecent(ruleID string, n int) []map[string]interface{} {
	as.mu.RLock()
	defer as.mu.RUnlock()
	var result []map[string]interface{}
	for i := len(as.events) - 1; i >= 0 && len(result) < n; i-- {
		evt := as.events[i]
		if ruleID != "" {
			// Filter on the normalized "ruleId" (processAnomaly guarantees it's
			// always populated by the time an event reaches Add — copied from
			// "id" if not already present), not the raw "id" field: an event
			// whose source schema uses "ruleId" directly without an "id" field
			// would otherwise never match here, even though the WebSocket
			// broadcast path for the same event correctly resolves its rule ID
			// via the same ruleId-then-id fallback.
			if id, _ := evt["ruleId"].(string); id != ruleID {
				continue
			}
		}
		result = append(result, evt)
	}
	// Reverse to chronological order
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

// AnomalyConsumer subscribes to the anomaly Kafka topic and fans out to:
//   - AnomalyStore (in-memory ring buffer)
//   - AnomalyWSManager (WebSocket push to subscribed clients)
type AnomalyConsumer struct {
	cancel       context.CancelFunc
	done         chan struct{}
	anomalyStore *AnomalyStore
	anomalyWSMgr *WSManager
	// pool drains raw message bytes off the read loop — same decoupling as
	// ResultsConsumer, see workerPool in msgpool.go.
	pool *workerPool
}

func NewAnomalyConsumer(store *AnomalyStore, wm *WSManager) *AnomalyConsumer {
	return &AnomalyConsumer{anomalyStore: store, anomalyWSMgr: wm, done: make(chan struct{})}
}

func (ac *AnomalyConsumer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	ac.cancel = cancel
	ac.pool = newWorkerPool(config.AnomalyTopic, config.ConsumerWorkers, config.ConsumerQueueSize, ac.processAnomaly)
	go func() {
		defer close(ac.done)
		ac.run(ctx)
		ac.pool.stop()
	}()
	slog.Info("Anomaly consumer started",
		"topic", config.AnomalyTopic,
		"workers", len(ac.pool.queues))
}

// Stop signals the consumer goroutine to stop and waits (bounded by
// stopWait, defined in consumer.go) for it to actually close its Kafka
// client — see ResultsConsumer.Stop's doc comment for why this matters for
// a clean, immediate LeaveGroupRequest during shutdown.
func (ac *AnomalyConsumer) Stop() {
	if ac.cancel != nil {
		ac.cancel()
	}
	select {
	case <-ac.done:
	case <-time.After(stopWait):
		slog.Warn("Anomaly consumer did not stop within timeout during shutdown")
	}
	slog.Info("Anomaly consumer stopped")
}

func (ac *AnomalyConsumer) run(ctx context.Context) {
	backoff := 1 * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Shared group ID per topic, not per pod — see the equivalent
		// comment in consumer.go's run() for why the pod-name suffix was
		// removed.
		groupID := config.AnomalyConsumerGroup

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
			slog.Error("Failed to create anomaly Kafka consumer", "error", err)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}

		if err := c.SubscribeTopics([]string{config.AnomalyTopic}, nil); err != nil {
			slog.Error("Failed to subscribe to anomaly topic", "error", err)
			c.Close()
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			continue
		}

		backoff = 1 * time.Second // reset on successful connect
		slog.Info("Anomaly consumer subscribed", "topic", config.AnomalyTopic)

		for {
			select {
			case <-ctx.Done():
				c.Close()
				return
			default:
			}

			msg, err := c.ReadMessage(1000)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				slog.Error("Anomaly consumer read error", "error", err)
				break // inner loop → reconnect
			}

			metrics.KafkaMessagesConsumedTotal.WithLabelValues(config.AnomalyTopic).Inc()
			if _, high, wmErr := c.GetWatermarkOffsets(*msg.TopicPartition.Topic, msg.TopicPartition.Partition); wmErr == nil {
				lag := high - int64(msg.TopicPartition.Offset) - 1
				if lag < 0 {
					lag = 0
				}
				metrics.KafkaConsumerLag.WithLabelValues(config.AnomalyTopic, config.AnomalyConsumerGroup).Set(float64(lag))
			}

			if !ac.pool.submit(ctx, msg.Key, msg.Value) {
				c.Close()
				return // context cancelled while waiting for queue space
			}
		}
		c.Close()
	}
}

func (ac *AnomalyConsumer) processAnomaly(value []byte) {
	var evt map[string]interface{}
	if err := json.Unmarshal(value, &evt); err != nil {
		slog.Error("Failed to unmarshal anomaly event", "error", err)
		return
	}

	metrics.ObserveConsumeDelay(config.AnomalyTopic, evt)

	// Add event_type discriminator so WS clients can distinguish from agg events
	evt["event_type"] = "anomaly"

	// Normalize: Flink AnomalyEvent uses "id" but downstream expects "ruleId"
	if id, ok := evt["id"].(string); ok && id != "" {
		if _, hasRuleId := evt["ruleId"]; !hasRuleId {
			evt["ruleId"] = id
		}
	}

	ac.anomalyStore.Add(evt)

	ruleID, _ := evt["ruleId"].(string)
	if ruleID == "" {
		ruleID, _ = evt["id"].(string)
	}
	if ruleID == "" {
		ruleID = "unknown"
	}
	ac.anomalyWSMgr.Broadcast(ruleID, evt)
}
