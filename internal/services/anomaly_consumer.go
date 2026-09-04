package services

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"

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
	cancel        context.CancelFunc
	done          chan struct{}
	anomalyStore  *AnomalyStore
	anomalyWSMgr  *WSManager
}

func NewAnomalyConsumer(store *AnomalyStore, wm *WSManager) *AnomalyConsumer {
	return &AnomalyConsumer{anomalyStore: store, anomalyWSMgr: wm, done: make(chan struct{})}
}

func (ac *AnomalyConsumer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	ac.cancel = cancel
	go func() {
		defer close(ac.done)
		ac.run(ctx)
	}()
	slog.Info("Anomaly consumer started", "topic", config.AnomalyTopic)
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

		// Use POD_NAME (set by Kubernetes downward API) for a stable group ID per pod.
		// This prevents stale consumer group accumulation on restarts.
		podName := os.Getenv("POD_NAME")
		if podName == "" {
			hostname, _ := os.Hostname()
			podName = hostname
			slog.Warn("POD_NAME env var not set — using hostname for anomaly consumer group ID")
		}
		groupID := config.AnomalyConsumerGroup + "-" + podName

		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  config.KafkaBrokers,
			"group.id":           groupID,
			"auto.offset.reset":  "latest",
			"enable.auto.commit": true,
			"session.timeout.ms": 30000,
		})
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

			ac.processAnomaly(msg.Value)
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
