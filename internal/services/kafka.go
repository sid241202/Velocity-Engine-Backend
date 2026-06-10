package services

import (
	"encoding/json"
	"log/slog"
	"sync"

	"velocity-engine-control-plane-backend-go/internal/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	producer     *kafka.Producer
	producerOnce sync.Once
	producerErr  error
)

// initProducer creates the singleton Kafka producer.
func initProducer() {
	producerOnce.Do(func() {
		p, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": config.KafkaBrokers,
			"acks":              "all",
			"retries":           3,
			"linger.ms":         10,
			"compression.type":  "lz4",
		})
		if err != nil {
			slog.Error("Failed to create Kafka producer", "error", err)
			producerErr = err
			return
		}
		producer = p
		slog.Info("Kafka producer initialized", "brokers", config.KafkaBrokers)
	})
}

// PublishRule serializes and publishes a rule dict to the rules Kafka topic.
// Returns true on success, false on failure.
func PublishRule(ruleDict map[string]interface{}) bool {
	initProducer()
	if producerErr != nil || producer == nil {
		slog.Error("Kafka producer not available", "error", producerErr)
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

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(ruleID),
		Value:          value,
	}, deliveryChan)
	if err != nil {
		slog.Error("Failed to produce message to Kafka", "error", err)
		return false
	}

	// Wait for delivery report with 10s timeout
	remaining := producer.Flush(10 * 1000)
	if remaining > 0 {
		slog.Error("Kafka flush timed out", "remaining", remaining)
		return false
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		slog.Error("Kafka delivery failed", "error", m.TopicPartition.Error)
		return false
	}

	slog.Info("Message delivered to Kafka",
		"topic", *m.TopicPartition.Topic,
		"partition", m.TopicPartition.Partition,
	)
	return true
}

// CloseProducer flushes and closes the Kafka producer.
func CloseProducer() {
	if producer != nil {
		producer.Flush(5 * 1000)
		producer.Close()
		slog.Info("Kafka producer closed")
	}
}
