package services

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// AnomalyStore holds a bounded ring-buffer of recent anomaly events in memory.
// It is written by AnomalyConsumer and read by the anomaly WebSocket handler.
type AnomalyStore struct {
	mu     chan struct{}
	events []map[string]interface{}
	maxLen int
}

func NewAnomalyStore(maxLen int) *AnomalyStore {
	mu := make(chan struct{}, 1)
	mu <- struct{}{}
	return &AnomalyStore{mu: mu, events: make([]map[string]interface{}, 0, maxLen), maxLen: maxLen}
}

func (as *AnomalyStore) Add(event map[string]interface{}) {
	<-as.mu
	defer func() { as.mu <- struct{}{} }()
	as.events = append(as.events, event)
	if len(as.events) > as.maxLen {
		as.events = as.events[len(as.events)-as.maxLen:]
	}
}

// GetRecent returns up to n most-recent anomaly events, optionally filtered by ruleID (empty = all).
func (as *AnomalyStore) GetRecent(ruleID string, n int) []map[string]interface{} {
	<-as.mu
	defer func() { as.mu <- struct{}{} }()
	var result []map[string]interface{}
	for i := len(as.events) - 1; i >= 0 && len(result) < n; i-- {
		evt := as.events[i]
		if ruleID != "" {
			if id, _ := evt["id"].(string); id != ruleID {
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
	anomalyStore  *AnomalyStore
	anomalyWSMgr  *WSManager
}

func NewAnomalyConsumer(store *AnomalyStore, wm *WSManager) *AnomalyConsumer {
	return &AnomalyConsumer{anomalyStore: store, anomalyWSMgr: wm}
}

func (ac *AnomalyConsumer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	ac.cancel = cancel
	go ac.run(ctx)
	slog.Info("Anomaly consumer started", "topic", config.AnomalyTopic)
}

func (ac *AnomalyConsumer) Stop() {
	if ac.cancel != nil {
		ac.cancel()
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

		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  config.KafkaBrokers,
			"group.id":           config.AnomalyConsumerGroup,
			"auto.offset.reset":  "latest",
			"enable.auto.commit": true,
			"session.timeout.ms": 30000,
		})
		if err != nil {
			slog.Error("Failed to create anomaly Kafka consumer", "error", err)
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}

		if err := c.SubscribeTopics([]string{config.AnomalyTopic}, nil); err != nil {
			slog.Error("Failed to subscribe to anomaly topic", "error", err)
			c.Close()
			time.Sleep(backoff)
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

	ac.anomalyStore.Add(evt)

	ruleID, _ := evt["id"].(string)
	if ruleID == "" {
		ruleID = "unknown"
	}
	ac.anomalyWSMgr.Broadcast(ruleID, evt)
}
