package config

import (
	"os"
	"strconv"
)

var (
	// Server
	ServerHost  = getEnv("SERVER_HOST", "0.0.0.0")
	ServerPort  = getEnv("SERVER_PORT", "8000")
	LogLevel    = getEnv("LOG_LEVEL", "INFO")
	CORSOrigins = getEnv("CORS_ORIGINS", "*")

	// Kafka
	KafkaBrokers         = getEnv("KAFKA_BROKERS", "localhost:9092")
	RulesTopic           = getEnv("RULES_TOPIC", "DE.AUTH.VELOCITY_ENGINE.RULES")
	ResultsTopic         = getEnv("RESULTS_TOPIC", "DE.AUTH.VELOCITY_ENGINE.RESULTS")
	ResultsConsumerGroup = getEnv("RESULTS_CONSUMER_GROUP", "velocity-cp-results")

	// ClickHouse
	ClickHouseHost     = getEnv("CLICKHOUSE_HOST", "localhost")
	ClickHousePort     = getEnvInt("CLICKHOUSE_PORT", 8123)
	ClickHouseUser     = getEnv("CLICKHOUSE_USER", "default")
	ClickHousePassword = getEnv("CLICKHOUSE_PASSWORD", "")
	ClickHouseDB       = getEnv("CLICKHOUSE_DB", "auth_analytics")
	ClickHouseTable    = getEnv("CLICKHOUSE_TABLE", "velocity_rule_results")

	// Iceberg / S3
	IcebergURI      = getEnv("ICEBERG_URI", "thrift://localhost:9083")
	S3Endpoint      = getEnv("S3_ENDPOINT", "http://localhost:9000")
	S3AccessKey     = getEnv("S3_ACCESS_KEY", "admin")
	S3SecretKey     = getEnv("S3_SECRET_KEY", "password")
	IcebergTable    = getEnv("ICEBERG_TABLE", "stream_auth.auth_txn_raw_union_v1")
	IcebergS3Path   = getEnv("ICEBERG_S3_PATH", "s3://warehouse/stream_auth.db/auth_txn_raw_union_v1")

	// LiveStore
	LiveStoreMaxRows = getEnvInt("LIVE_STORE_MAX_ROWS", 50000)
	LiveStoreHours   = getEnvInt("LIVE_STORE_HOURS", 24)
	WSHeartbeatSec   = getEnvInt("WS_HEARTBEAT_INTERVAL", 30)
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
