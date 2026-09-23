package config

import (
	"os"
	"strconv"
)

var (
	// Server
	ServerHost = getEnv("SERVER_HOST", "0.0.0.0")
	ServerPort = getEnv("SERVER_PORT", "8000")
	LogLevel   = getEnv("LOG_LEVEL", "INFO")
	// CORSOrigins: comma-separated allow-list (see cmd/server/main.go's CORS
	// wiring). Defaults to this app's actual staging frontend origin (raw
	// IP:port, no DNS name — see appConfig.js's authConfig comment for the
	// same address) rather than "*", now that a real origin is known for
	// this branch; override via CORS_ORIGINS for any other environment.
	CORSOrigins = getEnv("CORS_ORIGINS", "http://10.10.79.27:32515")

	// Kafka
	KafkaBrokers         = getEnv("KAFKA_BROKERS", "localhost:9092")
	RulesTopic           = getEnv("RULES_TOPIC", "DE.AUTH.VELOCITY_ENGINE.RULES")
	ResultsTopic         = getEnv("RESULTS_TOPIC", "DE.AUTH.VELOCITY_ENGINE.RESULTS")
	ResultsConsumerGroup = getEnv("RESULTS_CONSUMER_GROUP", "velocity-cp-results")
	AnomalyTopic         = getEnv("ANOMALY_TOPIC", "DE.AUTH.VELOCITY_ENGINE.ANOMALIES")
	AnomalyConsumerGroup = getEnv("ANOMALY_CONSUMER_GROUP", "velocity-cp-anomalies")

	// ClickHouse
	ClickHouseHost     = getEnv("CLICKHOUSE_HOST", "localhost")
	ClickHousePort     = getEnvInt("CLICKHOUSE_PORT", 8123)
	ClickHouseUser     = getEnv("CLICKHOUSE_USER", "default")
	ClickHousePassword = getEnv("CLICKHOUSE_PASSWORD", "")
	ClickHouseDB       = getEnv("CLICKHOUSE_DB", "auth_analytics")
	ClickHouseTable    = getEnv("CLICKHOUSE_TABLE", "velocity_rule_results")

	// Iceberg / S3
	IcebergURI    = getEnv("ICEBERG_URI", "thrift://localhost:9083")
	S3Endpoint    = getEnv("S3_ENDPOINT", "http://localhost:9000")
	S3AccessKey   = getEnv("S3_ACCESS_KEY", "")
	S3SecretKey   = getEnv("S3_SECRET_KEY", "")
	IcebergTable  = getEnv("ICEBERG_TABLE", "stream_auth.auth_txn_raw_union_v1")
	IcebergS3Path = getEnv("ICEBERG_S3_PATH", "s3://warehouse/stream_auth.db/auth_txn_raw_union_v1")

	// LiveStore
	// LiveStoreMaxRowsPerRule caps rows held for ONE rule. It replaces the
	// former global LIVE_STORE_MAX_ROWS cap, which was shared across every
	// rule and evicted from whichever rule happened to hold the most rows —
	// so a single busy rule's traffic could shrink or empty a completely
	// different rule's snapshot, which is exactly what made the frontend's
	// live chart appear to "flash to a different graph" on reconnect (the
	// bootstrap it got back no longer matched what it had a moment before).
	// Eviction is now strictly per rule: exceeding this cap can only ever
	// drop that same rule's own oldest rows. Total store size is therefore
	// bounded by (number of active rules x this value), with the
	// LIVE_STORE_HOURS time prune keeping it far below that in practice.
	//
	// NOTE for operators: LIVE_STORE_MAX_ROWS is no longer read. Its old
	// production value (750000) was a whole-store budget; setting that same
	// number here would mean 750000 rows *per rule*. Size this against a
	// single rule's peak window instead — see resources/Gitea files.txt.
	LiveStoreMaxRowsPerRule = getEnvInt("LIVE_STORE_MAX_ROWS_PER_RULE", 25000)
	// LiveStoreHours bounds both the startup bootstrap-from-ClickHouse pull
	// (main.go) and the ongoing background pruning in
	// internal/services/livestore.go's pruneStale — it is the actual,
	// continuously-enforced retention window for everything GetAll can ever
	// return (the "hours" query param on GET /rules/live-analysis is parsed
	// but not yet wired to anything narrower — see that handler). Matches the
	// frontend's own LIVE_WINDOW_HOURS in LiveAnalysis.jsx; keep both in sync
	// if either changes; changing only one of them still "works" but the
	// unmatched side won't get the memory/bandwidth benefit of the smaller
	// window.
	LiveStoreHours = getEnvInt("LIVE_STORE_HOURS", 1)
	WSHeartbeatSec = getEnvInt("WS_HEARTBEAT_INTERVAL", 30)

	// ── Kafka consumer concurrency ───────────────────────────────────────
	// The results/anomaly consumers used to read a message and process it
	// (JSON unmarshal + LiveStore write + WebSocket fan-out) inline on the
	// single goroutine that owns the Kafka client, so end-to-end throughput
	// was capped at one core no matter how many the pod was given. The read
	// loop now only hands raw message bytes to a bounded queue drained by a
	// worker pool.
	//
	// ConsumerWorkers: pool size. 0 (the default) means runtime.GOMAXPROCS,
	// i.e. the pod's CPU limit. Dispatch is key-affine (see
	// consumer.go's workerFor), so per-key ordering survives the pool.
	ConsumerWorkers = getEnvInt("CONSUMER_WORKERS", 0)
	// ConsumerQueueSize: depth of the hand-off queue per consumer. This is
	// the burst absorber between Kafka reads and processing. When it fills,
	// the read loop blocks rather than dropping messages — backpressure
	// surfaces as consumer lag (kafka_consumer_lag), which is visible and
	// recoverable, instead of as silent data loss.
	ConsumerQueueSize = getEnvInt("CONSUMER_QUEUE_SIZE", 16384)
	// KafkaFetchMinBytes / KafkaFetchWaitMaxMs tune librdkafka's fetch
	// batching. The defaults (1 byte / 500ms) make the broker answer almost
	// every fetch immediately with whatever is available, which at ~1,000+
	// msg/sec means a very high fetch rate for small payloads. Waiting for a
	// modest batch trades a few ms of latency for markedly less syscall and
	// broker overhead.
	KafkaFetchMinBytes  = getEnvInt("KAFKA_FETCH_MIN_BYTES", 65536)
	KafkaFetchWaitMaxMs = getEnvInt("KAFKA_FETCH_WAIT_MAX_MS", 100)
	// KafkaQueuedMaxMessagesKb bounds librdkafka's own internal prefetch
	// buffer (per partition), in kilobytes.
	KafkaQueuedMaxMessagesKb = getEnvInt("KAFKA_QUEUED_MAX_MESSAGES_KB", 65536)

	// ── WebSocket fan-out ────────────────────────────────────────────────
	// WSBroadcastIntervalMs: how often accumulated per-rule updates are
	// flushed to subscribers as ONE consolidated message, instead of one
	// WebSocket frame (and one json.Marshal, and one full scan of every
	// connection) per Kafka message. Deliberately mirrors the frontend's own
	// 250ms delta-flush window in src/components/LiveAnalysis.jsx so both
	// sides coalesce on the same cadence — sending faster than the client
	// re-renders only buys dropped messages and stutter.
	WSBroadcastIntervalMs = getEnvInt("WS_BROADCAST_INTERVAL_MS", 250)
	// WSBroadcastMaxBatchRows bounds how many rows one rule's pending batch
	// may hold between flushes. Updates coalesce by (groupKey, windowStart)
	// first, so this is only reached by genuinely distinct rows.
	WSBroadcastMaxBatchRows = getEnvInt("WS_BROADCAST_MAX_BATCH_ROWS", 5000)
	// WSOutboxSize: per-connection pending-message queue depth. Was a
	// hardcoded 4096 in wsmanager.go. Batching means one slot now carries a
	// whole flush window's worth of rows rather than a single row, so this
	// buys far more real headroom than the same number did before.
	WSOutboxSize = getEnvInt("WS_OUTBOX_SIZE", 4096)
	// WSWriteDeadlineSec: how long a single socket write may block before
	// the connection is considered stalled and torn down. Was hardcoded at
	// 10s in wsmanager.go.
	WSWriteDeadlineSec = getEnvInt("WS_WRITE_DEADLINE_SECONDS", 10)

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

	// JITProvisionRateLimitPerMinute bounds how many new users a single
	// source IP may trigger auto-provisioning for per minute (see
	// internal/services/rbac.go's GetOrProvisionUserByExternalSubject). A
	// compensating control for JIT auto-provisioning's real exposure: the
	// backend's identity layer trusts the client-supplied X-User-Subject
	// header without cryptographic verification (see IdentityMiddleware's
	// doc comment), so without this bound, auto-creating a user for every
	// unrecognized subject would let anyone who can reach this backend mint
	// unlimited accounts just by varying the header value.
	JITProvisionRateLimitPerMinute = getEnvInt("JIT_PROVISION_RATE_LIMIT_PER_MINUTE", 5)
)

func init() {
	if S3AccessKey == "" || S3SecretKey == "" {
		// Log warning or panic if necessary in prod, but for tests it might be empty
		// Slog isn't initialized yet, so just check. In a real prod setup, panic here.
		if os.Getenv("ENV") == "prod" {
			panic("S3_ACCESS_KEY and S3_SECRET_KEY are required")
		}
	}
	if ResultsConsumerGroup == AnomalyConsumerGroup {
		// Each consumer's actual group.id (see consumer.go/anomaly_consumer.go)
		// is this value plus "-<pod name>" — identical here means the results
		// and anomaly consumers join Kafka as two members of the exact same
		// consumer group despite subscribing to different topics. That's not
		// a correctness issue for partition assignment (Kafka's classic
		// assignors split each topic's partitions only among the members
		// subscribed to it), but it couples their rebalance lifecycles:
		// every join/leave/session-timeout in either consumer triggers a
		// group-wide rebalance for both, so a transient hiccup in one topic's
		// consumption can pause the other's too. RESULTS and ANOMALIES are
		// independent data flows with no reason to share this. Panic here
		// rather than let it silently cause intermittent, hard-to-diagnose
		// consumption stalls.
		panic("RESULTS_CONSUMER_GROUP and ANOMALY_CONSUMER_GROUP must be distinct")
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
