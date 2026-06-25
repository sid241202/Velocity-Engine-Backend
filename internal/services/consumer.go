package services

import (
	"context"
	"encoding/json"
	"log/slog"

	"velocity-engine-control-plane-backend-go/internal/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ResultsConsumer consumes Kafka results messages in a background goroutine.
type ResultsConsumer struct {
	cancel    context.CancelFunc
	liveStore *LiveStore
	wsManager *WSManager
}

// NewResultsConsumer creates a new results consumer.
func NewResultsConsumer(ls *LiveStore, wm *WSManager) *ResultsConsumer {
	return &ResultsConsumer{
		liveStore: ls,
		wsManager: wm,
	}
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
	consumerGroup := config.ResultsConsumerGroup
	slog.Info("Starting Kafka consumer", "group.id", consumerGroup)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  config.KafkaBrokers,
		"group.id":           consumerGroup,
		"auto.offset.reset":  "latest",
		"enable.auto.commit": true,
		"session.timeout.ms": 30000,
	})
	if err != nil {
		slog.Error("Failed to create Kafka consumer", "error", err)
		return
	}
	defer func() {
		if cerr := c.Close(); cerr != nil {
			slog.Error("Error closing Kafka consumer", "error", cerr)
		}
	}()

	if err := c.SubscribeTopics([]string{config.ResultsTopic}, nil); err != nil {
		slog.Error("Failed to subscribe to topic", "error", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := c.ReadMessage(1000) // 1s timeout
		if err != nil {
			// Timeout is expected, not an error
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				continue
			}
			slog.Error("Kafka consumer error", "error", err)
			continue
		}

		rc.processMessage(msg.Value)
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

	rc.liveStore.Add(row)

	// New schema uses "id" as the rule identifier; old schema used "ruleId"
	ruleID, _ := row["id"].(string)
	if ruleID == "" {
		ruleID, _ = row["ruleId"].(string)
	}
	if ruleID == "" {
		ruleID = "unknown"
	}
	rc.wsManager.Broadcast(ruleID, row)
}
