.PHONY: help up down restart logs ps \
        kafka-up clickhouse-up monitoring-up generator-up \
        simulate stream-start stream-stop ch-query

# ── Default ────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  FMCG Real-Time Analytics Platform"
	@echo "  ─────────────────────────────────"
	@echo "  make up              Start all services"
	@echo "  make down            Stop all services"
	@echo "  make restart         Restart all services"
	@echo "  make logs            Tail all logs"
	@echo "  make ps              Show running containers"
	@echo ""
	@echo "  make kafka-up        Start Kafka stack only"
	@echo "  make clickhouse-up   Start ClickHouse only"
	@echo "  make monitoring-up   Start Grafana + Prometheus"
	@echo "  make generator-up    Start event generator"
	@echo ""
	@echo "  make simulate N=500  Send N one-shot events"
	@echo "  make stream-start    Start 1,000 tx/s stream"
	@echo "  make stream-stop     Stop stream"
	@echo "  make ch-query Q=...  Run ClickHouse query"
	@echo ""

# ── Copy env ───────────────────────────────────────────────────────────────────
env:
	@if not exist .env (copy .env.example .env && echo ".env created") else (echo ".env already exists")

# ── Full stack ─────────────────────────────────────────────────────────────────
up: env
	docker compose up -d --build

down:
	docker compose down -v

restart:
	docker compose restart

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

# ── Individual stacks ──────────────────────────────────────────────────────────
kafka-up:
	docker compose -f services/kafka/docker-compose.yml up -d

clickhouse-up:
	docker compose -f services/clickhouse/docker-compose.yml up -d

monitoring-up:
	docker compose -f services/monitoring/docker-compose.yml up -d

generator-up:
	docker compose -f services/generator/docker-compose.yml up -d --build

# ── Generator API shortcuts ────────────────────────────────────────────────────
N ?= 1000
simulate:
	curl -s -X POST "http://localhost:8000/api/v1/simulate?count=$(N)" | python -m json.tool

stream-start:
	curl -s -X POST "http://localhost:8000/api/v1/stream/start?tps=1000" | python -m json.tool

stream-stop:
	curl -s -X POST "http://localhost:8000/api/v1/stream/stop" | python -m json.tool

stream-status:
	curl -s "http://localhost:8000/api/v1/stream/status" | python -m json.tool

# ── ClickHouse shortcuts ───────────────────────────────────────────────────────
Q ?= SELECT count() FROM pos_transactions
ch-query:
	docker exec fmcg-clickhouse clickhouse-client --query "$(Q)"

ch-row-count:
	docker exec fmcg-clickhouse clickhouse-client --query "SELECT count() FROM pos_transactions"

ch-latest:
	docker exec fmcg-clickhouse clickhouse-client --query \
		"SELECT * FROM pos_transactions ORDER BY timestamp DESC LIMIT 5 FORMAT PrettyCompact"

ch-benchmark:
	@echo "=== Benchmark: SUM revenue by region (raw MergeTree) ==="
	docker exec fmcg-clickhouse clickhouse-client --time \
		--query "SELECT region, sum(total_amount) FROM pos_transactions GROUP BY region"
	@echo "=== Benchmark: SUM revenue by region (Materialized View) ==="
	docker exec fmcg-clickhouse clickhouse-client --time \
		--query "SELECT region, sumMerge(revenue) FROM pos_hourly_agg GROUP BY region"
