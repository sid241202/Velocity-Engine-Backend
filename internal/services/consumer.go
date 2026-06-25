package services

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ResultsConsumer subscribes to the Kafka results topic and fans out to:
//   - LiveStore (in-memory ring buffer)
//   - WSManager (WebSocket push to subscribed clients)
type ResultsConsumer struct {
	cancel    context.CancelFunc
	liveStore *LiveStore
	wsManager *WSManager
}

// NewResultsConsumer creates a new results consumer.
func NewResultsConsumer(ls *LiveStore, wm *WSManager) *ResultsConsumer {
	return &ResultsConsumer{liveStore: ls, wsManager: wm}
}

// Start begins consuming in a background goroutine.
func (rc *ResultsConsumer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	rc.cancel = cancel
	go rc.run(ctx)
	slog.Info("Results consumer started", "topic", config.ResultsTopic)
}

// Stop signals the consumer goroutine to stop.
func (rc *ResultsConsumer) Stop() {
	if rc.cancel != nil {
		rc.cancel()
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

		// Use hostname-appended group ID so each pod gets ALL messages (broadcast pattern).
		// With a static group ID, Kafka distributes partitions across pods — each sees only
		// a subset of messages, which breaks LiveStore completeness and WebSocket broadcast.
		hostname, _ := os.Hostname()
		groupID := config.ResultsConsumerGroup + "-" + hostname

		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  config.KafkaBrokers,
			"group.id":           groupID,
			"auto.offset.reset":  "latest",
			"enable.auto.commit": true,
			"session.timeout.ms": 30000,
		})
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
			rc.processMessage(msg.Value)
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

	// New schema: aggResult is a pre-serialized JSON string — parse it for richer display
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

	// Normalize: Flink AggregationResult uses "id" but downstream code (ClickHouse queries,
	// LiveStore, WebSocket) expects "ruleId". Copy id → ruleId for consistency.
	if id, ok := row["id"].(string); ok && id != "" {
		if _, hasRuleId := row["ruleId"]; !hasRuleId {
			row["ruleId"] = id
		}
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
