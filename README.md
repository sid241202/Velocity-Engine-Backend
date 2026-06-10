# Velocity Engine Control Plane — Backend (Go)

## Overview
Go 1.23 backend for the Velocity Engine Control Plane. Drop-in replacement for the Python backend — exposes the **exact same HTTP and WebSocket API contract**. The existing React frontend works unchanged.

## Tech Stack
- Go 1.23, Gin (HTTP), Gorilla WebSocket
- confluent-kafka-go (producer + consumer)
- clickhouse-go (HTTP driver)
- go-duckdb + Iceberg extension (historical analysis)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_HOST` | `0.0.0.0` | Server bind address |
| `SERVER_PORT` | `8000` | Server listen port |
| `LOG_LEVEL` | `INFO` | Log level (DEBUG sets Gin to debug mode) |
| `CORS_ORIGINS` | `*` | Allowed CORS origins (comma-separated) |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `RULES_TOPIC` | `DE.AUTH.VELOCITY_ENGINE.RULES` | Kafka topic for publishing rules to Flink |
| `RESULTS_TOPIC` | `DE.AUTH.VELOCITY_ENGINE.RESULTS` | Kafka topic for consuming results from Flink |
| `RESULTS_CONSUMER_GROUP` | `velocity-cp-results` | Kafka consumer group base name |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse server hostname |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port |
| `CLICKHOUSE_USER` | `default` | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | _(empty)_ | ClickHouse password |
| `CLICKHOUSE_DB` | `auth_analytics` | ClickHouse database name |
| `CLICKHOUSE_TABLE` | `velocity_rule_results` | ClickHouse table name for rule results |
| `ICEBERG_S3_PATH` | `s3://warehouse/stream_auth.db/auth_txn_raw_union_v1` | S3 path to Iceberg table (for DuckDB iceberg_scan) |
| `S3_ENDPOINT` | `http://localhost:9000` | S3/MinIO endpoint |
| `S3_ACCESS_KEY` | `admin` | S3 access key |
| `S3_SECRET_KEY` | `password` | S3 secret key |
| `LIVE_STORE_MAX_ROWS` | `50000` | Max rows in the in-memory LiveStore |
| `LIVE_STORE_HOURS` | `24` | Hours of data to retain in LiveStore |
| `WS_HEARTBEAT_INTERVAL` | `30` | WebSocket heartbeat interval in seconds |

## Build & Run

### Local
```bash
go mod tidy
CGO_ENABLED=1 go build -o server ./cmd/server
./server
```

### Docker
```bash
docker build -t velocity-backend-go:$(cat VERSION) .
docker run -p 8000:8000 --env-file .env velocity-backend-go:$(cat VERSION)
```

## Key Differences from Python Backend
1. **DuckDB/Iceberg**: Uses DuckDB's native `iceberg_scan()` extension instead of PyIceberg. Requires `ICEBERG_S3_PATH` instead of `ICEBERG_URI`.
2. **Performance**: Compiled binary, goroutines instead of asyncio. Significantly lower memory footprint.
3. **CGO**: Required for confluent-kafka-go and go-duckdb. Docker build handles this.

## GitOps Deployment
1. Push code to Bitbucket/Gitea
2. Jenkins pipeline builds Go binary + Docker image and pushes to Harbor
3. Jenkins updates `k8s/deployment.yaml` with new image tag
4. ArgoCD detects manifest change and syncs to K8s cluster
