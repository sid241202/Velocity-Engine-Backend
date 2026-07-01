package services

import (
	"encoding/json"
	"log/slog"
	"sync"

	"velocity-engine-control-plane-backend-go/internal/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	producer    *kafka.Producer
	producerMu  sync.Mutex
)

// getProducer returns the singleton Kafka producer, creating it if necessary.
// Unlike sync.Once, this retries on failure so transient broker unavailability
// at startup doesn't permanently break publishing.
func getProducer() (*kafka.Producer, error) {
	producerMu.Lock()
	defer producerMu.Unlock()

	if producer != nil {
		return producer, nil
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.KafkaBrokers,
		"acks":              "all",
		"retries":           3,
		"linger.ms":         10,
		"compression.type":  "lz4",
	})
	if err != nil {
		slog.Error("Failed to create Kafka producer", "error", err)
		return nil, err
	}
	producer = p
	slog.Info("Kafka producer initialized", "brokers", config.KafkaBrokers)
	return producer, nil
}

// PublishRule serializes and publishes a rule dict to the rules Kafka topic.
// Returns true on success, false on failure.
func PublishRule(ruleDict map[string]interface{}) bool {
	p, err := getProducer()
	if err != nil {
		slog.Error("Kafka producer not available", "error", err)
		return false
	}

	// Extract rule_id for key
	rm, ok := ruleDict["rule_metadata"].(map[string]interface{})
	if !ok {
		slog.Error("rule_metadata missing or invalid")
		return false
	}
	ruleID, ok := rm["rule_id"].(string)
	if !ok {
		slog.Error("rule_id missing or invalid")
		return false
	}

	value, err := json.Marshal(ruleDict)
	if err != nil {
		slog.Error("Failed to marshal rule for Kafka", "error", err)
		return false
	}

	topic := config.RulesTopic
	deliveryChan := make(chan kafka.Event, 1)

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(ruleID),
		Value:          value,
	}, deliveryChan)
	if err != nil {
		slog.Error("Failed to produce message to Kafka", "error", err)
		return false
	}

	// Wait for delivery report with 10s timeout
	remaining := p.Flush(10 * 1000)
	if remaining > 0 {
		slog.Error("Kafka flush timed out", "remaining", remaining)
		return false
	}

	e := <-deliveryChan
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			slog.Error("Kafka delivery failed", "error", ev.TopicPartition.Error)
			return false
		}
		slog.Info("Message delivered to Kafka",
			"topic", *ev.TopicPartition.Topic,
			"partition", ev.TopicPartition.Partition,
		)
		return true
	case kafka.Error:
		slog.Error("Kafka producer error on delivery", "code", ev.Code(), "error", ev)
		return false
	default:
		slog.Error("Unexpected Kafka delivery event type", "event", e)
		return false
	}
}

// CloseProducer flushes and closes the Kafka producer.
func CloseProducer() {
	producerMu.Lock()
	defer producerMu.Unlock()
	if producer != nil {
		producer.Flush(5 * 1000)
		producer.Close()
		producer = nil
		slog.Info("Kafka producer closed")
	}
}
