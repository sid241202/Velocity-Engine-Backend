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
	ResultsTopic          = getEnv("RESULTS_TOPIC", "DE.AUTH.VELOCITY_ENGINE.RESULTS")
	ResultsConsumerGroup  = getEnv("RESULTS_CONSUMER_GROUP", "velocity-cp-results")
	AnomalyTopic          = getEnv("ANOMALY_TOPIC", "DE.AUTH.VELOCITY_ENGINE.ANOMALIES")
	AnomalyConsumerGroup  = getEnv("ANOMALY_CONSUMER_GROUP", "velocity-cp-anomalies")

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
	S3AccessKey     = getEnv("S3_ACCESS_KEY", "")
	S3SecretKey     = getEnv("S3_SECRET_KEY", "")
	IcebergTable    = getEnv("ICEBERG_TABLE", "stream_auth.auth_txn_raw_union_v1")
	IcebergS3Path   = getEnv("ICEBERG_S3_PATH", "s3://warehouse/stream_auth.db/auth_txn_raw_union_v1")

	// LiveStore
	LiveStoreMaxRows = getEnvInt("LIVE_STORE_MAX_ROWS", 50000)
	LiveStoreHours   = getEnvInt("LIVE_STORE_HOURS", 24)
	WSHeartbeatSec   = getEnvInt("WS_HEARTBEAT_INTERVAL", 30)

	// MySQL (RBAC store)
	MySQLHost         = getEnv("MYSQL_HOST", "localhost")
	MySQLPort         = getEnvInt("MYSQL_PORT", 3306)
	MySQLUser         = getEnv("MYSQL_USER", "root")
	MySQLPassword     = getEnv("MYSQL_PASSWORD", "")
	MySQLDatabase     = getEnv("MYSQL_DATABASE", "velocity_engine_iam")
	MySQLMaxOpenConns = getEnvInt("MYSQL_MAX_OPEN_CONNS", 20)
	MySQLMaxIdleConns = getEnvInt("MYSQL_MAX_IDLE_CONNS", 10)

	// RBAC
	// RBACPermissionCacheTTLSeconds: how long a resolved (roles, permissions) set
	// is served from memory before re-querying MySQL. See RBACPermissionMaxStaleSeconds
	// below for the fail-tolerant fallback bound during a MySQL outage.
	RBACPermissionCacheTTLSeconds = getEnvInt("RBAC_PERMISSION_CACHE_TTL_SECONDS", 300)
	// RBACPermissionMaxStaleSeconds bounds how long a cached permission set may
	// keep being served past its TTL if MySQL is unreachable when a refresh is
	// attempted. This is a deliberate availability/security tradeoff: a brief
	// MySQL blip shouldn't lock every user out of the whole control plane, but
	// serving a since-revoked permission set indefinitely is a real security
	// exposure — so staleness is tolerated only up to this bound, after which
	// authorization checks fail closed (503) instead of trusting old data forever.
	RBACPermissionMaxStaleSeconds = getEnvInt("RBAC_PERMISSION_MAX_STALE_SECONDS", 1800)
	// AuthDevMode enables the TEMPORARY pre-WSO2 identity shim (X-Debug-User-Id
	// header) used until real token validation is implemented. See
	// internal/middleware/auth.go. Guarded below: refuses to boot with this on
	// in a prod environment, mirroring the existing S3 key check in init().
	AuthDevMode = getEnv("AUTH_DEV_MODE", "true") == "true"
)

func init() {
	if S3AccessKey == "" || S3SecretKey == "" {
		// Log warning or panic if necessary in prod, but for tests it might be empty
		// Slog isn't initialized yet, so just check. In a real prod setup, panic here.
		if os.Getenv("ENV") == "prod" {
			panic("S3_ACCESS_KEY and S3_SECRET_KEY are required")
		}
	}
	if AuthDevMode && os.Getenv("ENV") == "prod" {
		// AuthDevMode bypasses real identity verification (see
		// internal/middleware/auth.go) — it must never be reachable in prod.
		panic("AUTH_DEV_MODE must be false (or unset) when ENV=prod — it is a pre-WSO2 development identity bypass")
	}
}

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
