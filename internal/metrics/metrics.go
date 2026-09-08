// Package metrics defines this backend's Prometheus metrics on a dedicated
// registry (not prometheus.DefaultRegisterer, so /metrics exposes exactly
// this process's own metrics plus the standard Go/process collectors — never
// anything a third-party dependency registers globally on the default one).
//
// This package has no dependency on internal/services (which pulls in
// go-duckdb/cgo via duckdb.go) so it can be imported from internal/middleware
// without reintroducing that dependency there.
package metrics

import (
	"database/sql"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Registry is the single registry this backend's /metrics endpoint serves.
var Registry = prometheus.NewRegistry()

// wideBuckets spans sub-10ms CRUD calls up to 900s — the historical-query
// ceiling shared with the frontend's nginx proxy_read_timeout and this
// server's own http.Server.WriteTimeout (see cmd/server/main.go). Reused for
// any histogram that might see either fast or slow calls (HTTP requests
// include both fast RBAC/CRUD routes and the slow historical-* routes;
// DuckDB historical queries are always slow).
var wideBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
	10, 30, 60, 120, 300, 600, 900,
}

var (
	HTTPRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests, by method, route template, and status code.",
	}, []string{"method", "route", "status"})

	HTTPRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request duration in seconds, by method and route template.",
		Buckets: wideBuckets,
	}, []string{"method", "route"})

	KafkaMessagesConsumedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_messages_consumed_total",
		Help: "Total Kafka messages consumed, by topic.",
	}, []string{"topic"})

	// KafkaConsumerLag is computed per consumed message from the message's
	// own partition watermark (GetWatermarkOffsets), not from a separate
	// admin-client poll — cheap (librdkafka caches watermarks) and avoids a
	// second connection just for lag. consumer_group is always the
	// *configured* base group name (config.ResultsConsumerGroup /
	// config.AnomalyConsumerGroup), never the per-pod-suffixed group id
	// consumer.go/anomaly_consumer.go actually join Kafka with — using the
	// per-pod id here would make this label's cardinality grow with every
	// pod restart/rescale instead of staying fixed at 2.
	KafkaConsumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Approximate consumer lag (partition high watermark minus last consumed offset), by topic and consumer group.",
	}, []string{"topic", "consumer_group"})

	// WebSocketActiveConnections / Duration / Dropped all carry a "stream"
	// label — "live" or "anomaly", the two independent WSManager instances
	// this backend runs (see cmd/server/main.go) — fixed at 2 values.
	WebSocketActiveConnections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "websocket_active_connections",
		Help: "Current active WebSocket connections, by stream.",
	}, []string{"stream"})

	WebSocketBroadcastDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "websocket_broadcast_duration_seconds",
		Help:    "Duration of a single Broadcast() fan-out call, by stream.",
		Buckets: prometheus.DefBuckets,
	}, []string{"stream"})

	// WebSocketDroppedTotal's "reason" is a single closed value today
	// ("outbox_full" — WSManager's only drop path) but kept as a label so a
	// future second drop reason doesn't require a metric rename.
	WebSocketDroppedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "websocket_dropped_messages_total",
		Help: "Messages dropped because a connection's outbox was full, by stream and reason.",
	}, []string{"stream", "reason"})

	// WebSocketHeartbeatSkippedTotal counts a heartbeat tick that couldn't be
	// enqueued because the outbox was already full of real data — a
	// near-miss, not a disconnect (see internal/handlers/websocket.go, which
	// deliberately keeps the connection open in this case: outbox-full from
	// real traffic is proof of liveness, not a dead connection). A rising
	// rate here means connections are running close to outboxSize under
	// current load — worth watching alongside websocket_dropped_messages_total,
	// which is the actual data-loss signal this metric is not.
	WebSocketHeartbeatSkippedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "websocket_heartbeat_skipped_total",
		Help: "Heartbeat ticks skipped because the outbox was already full of real data, by stream.",
	}, []string{"stream"})

	// MySQLOptionalTableMissing is set to 1 the moment this backend detects a
	// given optional table or column doesn't exist (MySQL error 1146 or
	// 1054 — see isMissingSchemaError in internal/services/mysql.go) and
	// degrades the corresponding feature gracefully instead of failing
	// closed — see internal/services/rbac.go's GetLedTeamIDs/
	// provisionUserByExternalSubject and rule_store.go's
	// SaveNewRuleVersion/LoadActiveRules. A dashboard should treat any
	// non-zero value here as "this environment's schema is behind what the
	// code expects" — worth fixing by provisioning the named migration, not
	// a steady-state condition to leave alone indefinitely. Label values are
	// a small, fixed, code-controlled set (table names, or "users.team_id"
	// for the one tolerated column) — never derived from user input — so
	// this carries no unbounded-cardinality risk.
	MySQLOptionalTableMissing = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mysql_optional_table_missing",
		Help: "1 if this optional table/column was found missing and its feature is running in degraded/tolerant mode, by table.",
	}, []string{"table"})

	// ClickHouseQueryDuration/ErrorsTotal's query_type is one of exactly 3
	// named queries this backend issues (live_results, live_results_multi,
	// agg_results) — see internal/services/clickhouse.go.
	ClickHouseQueryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "clickhouse_query_duration_seconds",
		Help:    "ClickHouse query duration in seconds, by query type.",
		Buckets: prometheus.DefBuckets,
	}, []string{"query_type"})

	// error_class is "unavailable" (connection-level, matches
	// services.ErrClickHouseUnavailable — retryable/503) or "query_error"
	// (everything else) — never a raw error string.
	ClickHouseErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_errors_total",
		Help: "ClickHouse query errors, by query type and error class (unavailable|query_error).",
	}, []string{"query_type", "error_class"})

	// DuckDBQueryDuration's query_type is historical_analysis or
	// historical_breakdown (the two DuckDB/Iceberg entry points in
	// internal/services/duckdb.go). Wide buckets: these queries are
	// legitimately slow, up to the 900s ceiling.
	DuckDBQueryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "duckdb_historical_query_duration_seconds",
		Help:    "DuckDB/Iceberg historical query duration in seconds, by query type.",
		Buckets: wideBuckets,
	}, []string{"query_type"})

	// error_class is "busy" (services.ErrHistoricalEngineBusy — the single
	// query slot was occupied, retryable/503) or "query_error".
	DuckDBErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "duckdb_errors_total",
		Help: "DuckDB/Iceberg historical query errors, by query type and error class (busy|query_error).",
	}, []string{"query_type", "error_class"})

	// MySQLQueryDuration covers MySQL calls NOT already covered by the more
	// specific RBACResolutionDuration below (which times the identical
	// fetchUserPermissionsFromDB call — instrumenting it twice under two
	// metric names would just duplicate the same number). operation is one
	// of: get_user_by_external_subject, verify_schema, record_audit_event.
	MySQLQueryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mysql_query_duration_seconds",
		Help:    "MySQL query duration in seconds, by operation.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	RBACCacheHitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rbac_permission_cache_hits_total",
		Help: "RBAC permission cache hits (served without a MySQL round trip).",
	})
	RBACCacheMissesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rbac_permission_cache_misses_total",
		Help: "RBAC permission cache misses — expired, absent, or a stale-serve fallback attempt.",
	})
	RBACResolutionDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "rbac_permission_resolution_duration_seconds",
		Help:    "Duration of a MySQL-backed RBAC permission resolution (the cache-miss path).",
		Buckets: prometheus.DefBuckets,
	})

	// IAMJITProvisionedUsersTotal / IAMJITProvisionRateLimitedTotal cover the
	// two outcomes of GetOrProvisionUserByExternalSubject
	// (internal/services/rbac.go) on an unrecognized WSO2 subject: a new
	// user actually created, or the attempt rejected by the per-source-IP
	// rate limit (config.JITProvisionRateLimitPerMinute) — the compensating
	// control for auto-provisioning against an identity header that isn't
	// cryptographically verified. Neither is visible via the generic
	// http_requests_total, since both happen inside GET /me's identity
	// resolution, not as their own route.
	IAMJITProvisionedUsersTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "iam_jit_provisioned_users_total",
		Help: "Total users auto-provisioned on first WSO2 login with no existing local account (JIT provisioning).",
	})
	IAMJITProvisionRateLimitedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "iam_jit_provision_rate_limited_total",
		Help: "JIT-provisioning attempts rejected by the per-source-IP rate limit.",
	})

	// IAMAdminMutationsTotal covers every Admin Panel mutation by action and
	// result. action is one of 4 fixed values (user_update — role/team/
	// status changes, which of the three actually changed is captured
	// precisely in the resulting audit_log row instead of here — team_create,
	// lead_grant, lead_revoke); result is one of 3 (success, forbidden,
	// error) — bounded cardinality, matches this project's existing
	// label-design discipline. Distinct from the generic
	// http_requests_total{route="/admin/users/:id",...}: that can't tell a
	// successful update apart from a scope-forbidden one, both of which
	// return non-5xx-shaped outcomes at the HTTP layer today (200 vs 403).
	IAMAdminMutationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "iam_admin_mutations_total",
		Help: "Admin Panel mutations, by action and result (success|forbidden|error).",
	}, []string{"action", "result"})

	// RBACCacheInvalidationsTotal counts explicit evictions triggered by an
	// Admin Panel mutation (InvalidateUserPermissions) — the mechanism that
	// makes a role/team/status change effective immediately instead of
	// waiting out RBAC_PERMISSION_CACHE_TTL_SECONDS. Complements
	// RBACCacheHitsTotal/MissesTotal: a healthy system should show this
	// firing roughly once per real Admin Panel mutation, not continuously.
	RBACCacheInvalidationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "rbac_permission_cache_invalidations_total",
		Help: "Explicit RBAC permission cache invalidations triggered by an Admin Panel role/team/status change.",
	})

	// BackendConsumeDelay is the last checkpoint of the Kafka-to-Redis
	// per-stage latency chain (see the observability plan): time from
	// Flink's producedAt timestamp (TimeUtils.currentIstString() at
	// emission — see RuleEvaluatorFunction.java) to this backend consuming
	// the message. topic is exactly 2 values (results, anomalies).
	BackendConsumeDelay = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "velocity_backend_consume_delay_seconds",
		Help:    "Time from Flink's producedAt timestamp to this backend consuming the Kafka message, by topic.",
		Buckets: prometheus.DefBuckets,
	}, []string{"topic"})
)

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		HTTPRequestsTotal, HTTPRequestDuration,
		KafkaMessagesConsumedTotal, KafkaConsumerLag,
		WebSocketActiveConnections, WebSocketBroadcastDuration, WebSocketDroppedTotal,
		WebSocketHeartbeatSkippedTotal, MySQLOptionalTableMissing,
		ClickHouseQueryDuration, ClickHouseErrorsTotal,
		DuckDBQueryDuration, DuckDBErrorsTotal,
		MySQLQueryDuration,
		RBACCacheHitsTotal, RBACCacheMissesTotal, RBACResolutionDuration,
		IAMJITProvisionedUsersTotal, IAMJITProvisionRateLimitedTotal,
		IAMAdminMutationsTotal, RBACCacheInvalidationsTotal,
		BackendConsumeDelay,
	)
}

// RegisterLiveStoreCollectors wires the LiveStore's current row count and
// cumulative dropped-row count as collectors read live at scrape time,
// rather than duplicating that state via separate increment call-sites.
// Call once at startup with the running LiveStore's TotalRows/DroppedRowsTotal
// methods.
func RegisterLiveStoreCollectors(rows func() int, droppedTotal func() float64) {
	Registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "livestore_rows",
			Help: "Current row count held in the in-memory LiveStore.",
		}, func() float64 { return float64(rows()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "livestore_evictions_total",
			Help: "Cumulative rows evicted from the LiveStore due to the row-count cap.",
		}, droppedTotal),
	)
}

// RegisterMySQLPoolCollectors wires database/sql's native pool stats
// (sql.DB.Stats()) as collectors read live at scrape time. Call once at
// startup with services.MySQLPoolStats, which returns a zero value if the
// pool hasn't been initialized yet (safe to register before first use).
func RegisterMySQLPoolCollectors(stats func() sql.DBStats) {
	Registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "mysql_pool_open_connections",
			Help: "Open MySQL connections (in use + idle).",
		}, func() float64 { return float64(stats().OpenConnections) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "mysql_pool_in_use",
			Help: "MySQL connections currently in use.",
		}, func() float64 { return float64(stats().InUse) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "mysql_pool_idle",
			Help: "Idle MySQL connections in the pool.",
		}, func() float64 { return float64(stats().Idle) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "mysql_pool_wait_count_total",
			Help: "Cumulative number of connections waited for because none was free (sql.DBStats.WaitCount).",
		}, func() float64 { return float64(stats().WaitCount) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "mysql_pool_wait_duration_seconds_total",
			Help: "Cumulative time spent waiting for a free connection (sql.DBStats.WaitDuration).",
		}, func() float64 { return stats().WaitDuration.Seconds() }),
	)
}

// istZone matches livestore.go's istZoneLS: Flink writes naive IST wall-clock
// strings with no timezone marker (TimeUtils.IST_FMT), so parsing must apply
// the +05:30 offset explicitly rather than assume UTC.
var istZone = time.FixedZone("IST", 5*60*60+30*60)

// parseProducedAt parses Flink's "producedAt" field: a naive IST
// "2006-01-02 15:04:05" string set at emission time by
// TimeUtils.currentIstString() in RuleEvaluatorFunction.java.
func parseProducedAt(v string) (time.Time, bool) {
	if v == "" {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("2006-01-02 15:04:05", v, istZone)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// ObserveConsumeDelay records BackendConsumeDelay from a consumed message's
// "producedAt" field, if present and parseable. Silently a no-op otherwise —
// a missing/malformed field must never break message processing.
func ObserveConsumeDelay(topic string, row map[string]interface{}) {
	producedAt, _ := row["producedAt"].(string)
	t, ok := parseProducedAt(producedAt)
	if !ok {
		return
	}
	delay := time.Since(t).Seconds()
	if delay < 0 {
		delay = 0
	}
	BackendConsumeDelay.WithLabelValues(topic).Observe(delay)
}
