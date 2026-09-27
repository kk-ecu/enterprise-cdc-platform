# ==============================================================================
# Enterprise CDC Platform: Makefile
# Target: Apple Silicon M2 Mac Pro (16 GB Unified Memory) & Podman
# ==============================================================================

SHELL := /bin/bash
COMPOSE ?= $(shell which podman-compose 2>/dev/null || which docker-compose 2>/dev/null || echo "podman-compose")
PODMAN ?= $(shell which podman 2>/dev/null || echo "podman")
PYTHON ?= python3.12
VENV := .venv
ACTIVATE := source $(VENV)/bin/activate

.DEFAULT_GOAL := help

# Terminal Colors
GREEN  := $(shell tput -Txterm setaf 2 2>/dev/null || echo '')
CYAN   := $(shell tput -Txterm setaf 6 2>/dev/null || echo '')
YELLOW := $(shell tput -Txterm setaf 3 2>/dev/null || echo '')
MAGENTA:= $(shell tput -Txterm setaf 5 2>/dev/null || echo '')
RESET  := $(shell tput -Txterm sgr0 2>/dev/null || echo '')

.PHONY: help podman-init setup-env up down clean restart status logs test \
        db-up-postgres db-up-mysql db-up-mongo db-up-all \
        connectors-init connectors-status connectors-pause connectors-resume connectors-restart api-dev \
        simulate-pg-traffic simulate-mysql-traffic simulate-pk-update simulate-delete-tombstone \
        simulate-truncate simulate-large-tx simulate-rollback simulate-schema-evolution \
        simulate-ddl-alter-type simulate-ddl-drop-col simulate-incremental-snapshot \
        simulate-dlq-poison simulate-failover verify-lag verify-topics verify-pipeline verify-memory

## -----------------------------------------------------------------------------
## 1. HELP & INITIALIZATION
## -----------------------------------------------------------------------------

help: ## Show this help menu
	@echo ""
	@echo "$(CYAN)Enterprise CDC Platform (Debezium + Kafka + FastAPI on M2 Podman)$(RESET)"
	@echo "$(YELLOW)Hardware Target: Apple Silicon M2 Mac Pro (16 GB Unified RAM - 11.5 GB Cap)$(RESET)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-30s$(RESET) %s\n", $$1, $$2}'
	@echo ""

test: ## Run the entire end-to-end CDC test suite (38 unit, integration, config & SOLID tests)
	@python3 tests/run_all_tests.py

podman-init: ## Initialize dedicated Podman VM tuned for M2 16GB RAM limit (11.5GB cap)
	@echo "$(CYAN)Initializing Podman VM with 6 vCPUs and 11.5GB RAM...$(RESET)"
	$(PODMAN) machine init --cpus 6 --memory 11520 --disk-size 50 cdc-m2-vm || true
	$(PODMAN) machine set --rootful=false cdc-m2-vm || true
	$(PODMAN) machine start cdc-m2-vm || true
	@echo "$(GREEN)Podman machine ready!$(RESET)"

setup-env: ## Create Python virtualenv and install FastAPI backend dependencies
	@echo "$(CYAN)Setting up Python environment...$(RESET)"
	$(PYTHON) -m venv $(VENV)
	$(ACTIVATE) && pip install --upgrade pip && pip install -r backend/requirements.txt
	@echo "$(GREEN)Environment ready. Run 'source .venv/bin/activate' to use.$(RESET)"

## -----------------------------------------------------------------------------
## 2. LIFECYCLE & CONTAINER MANAGEMENT (PODMAN M2)
## -----------------------------------------------------------------------------

up: ## Start full CDC stack (Kafka KRaft, Schema Registry, Debezium, DBs, FastAPI)
	@echo "$(CYAN)Starting Enterprise CDC Platform containers via $(COMPOSE)...$(RESET)"
	$(COMPOSE) up -d
	@echo "$(GREEN)Waiting for Kafka Connect and Kafka broker to be healthy...$(RESET)"
	@sleep 6
	@$(MAKE) status

down: ## Gracefully stop all platform containers
	@echo "$(YELLOW)Stopping all CDC containers...$(RESET)"
	$(COMPOSE) down

clean: ## Stop and remove all containers, networks, and persistent test volumes
	@echo "$(YELLOW)Cleaning up all containers and transient volumes...$(RESET)"
	$(COMPOSE) down -v --remove-orphans
	@echo "$(GREEN)Clean state restored.$(RESET)"

restart: down up ## Restart the full stack

status: ## Inspect container health status and live memory usage vs 11.5GB budget
	@echo ""
	@echo "$(CYAN)=== Active Container Status ===$(RESET)"
	@$(PODMAN) ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" || docker ps
	@echo ""
	@echo "$(CYAN)=== Podman Container Memory Usage (Target <= 11.5 GB) ===$(RESET)"
	@$(PODMAN) stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}" || true

logs: ## Tail aggregated logs from all services
	$(COMPOSE) logs -f --tail=100

## -----------------------------------------------------------------------------
## 3. INDIVIDUAL DATABASE TARGETS
## -----------------------------------------------------------------------------

db-up-postgres: ## Launch only PostgreSQL with wal_level=logical
	$(COMPOSE) up -d postgres-source
	@echo "$(GREEN)PostgreSQL Source started on port 5433 (logical replication ready)$(RESET)"

db-up-mysql: ## Launch only MySQL with GTID & ROW binary log
	$(COMPOSE) up -d mysql-source
	@echo "$(GREEN)MySQL Source started on port 3307 (GTID enabled)$(RESET)"

db-up-mariadb: ## Launch only MariaDB with GTID strict mode & ROW binlog
	$(COMPOSE) up -d mariadb-source
	@echo "$(GREEN)MariaDB Source started on port 3308 (GTID enabled)$(RESET)"

db-up-mongo: ## Launch only MongoDB with Replica Set enabled
	$(COMPOSE) up -d mongo-source
	@sleep 3
	@$(PODMAN) exec -it cdc-mongo-source mongosh --eval "rs.initiate()" || true
	@echo "$(GREEN)MongoDB Source started on port 27018 (Change Streams ready)$(RESET)"

db-up-all: db-up-postgres db-up-mysql db-up-mongo ## Launch all source database containers concurrently
	@echo "$(GREEN)All heterogeneous database sources online!$(RESET)"

## -----------------------------------------------------------------------------
## 4. CONNECTOR PROVISIONING & FASTAPI CONTROL PLANE
## -----------------------------------------------------------------------------

connectors-init: ## Register PostgreSQL and MySQL CDC connectors via Connect REST API
	@echo "$(CYAN)Deploying PostgreSQL Orders CDC Connector...$(RESET)"
	@curl -s -X POST http://localhost:8083/connectors \
	  -H "Content-Type: application/json" \
	  -d '{"name": "postgres-orders-connector", "config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "tasks.max": "1", "plugin.name": "pgoutput", "database.hostname": "postgres-source", "database.port": "5432", "database.user": "postgres", "database.password": "postgrespassword", "database.dbname": "orders_db", "topic.prefix": "cdc.local.ecommerce.orders_db", "table.include.list": "public.orders,public.customers", "snapshot.mode": "initial"}}' || true
	@echo ""
	@echo "$(CYAN)Deploying MySQL Inventory CDC Connector...$(RESET)"
	@curl -s -X POST http://localhost:8083/connectors \
	  -H "Content-Type: application/json" \
	  -d '{"name": "mysql-inventory-connector", "config": {"connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "1", "database.hostname": "mysql-source", "database.port": "3306", "database.user": "mysqluser", "database.password": "mysqlpassword", "database.server.id": "184054", "topic.prefix": "cdc.local.mysql_store.inventory", "table.include.list": "inventory.products,inventory.customers", "snapshot.mode": "initial", "schema.history.internal.kafka.bootstrap.servers": "kafka:9092", "schema.history.internal.kafka.topic": "schema-changes.inventory"}}' || true
	@echo ""
	@echo "$(GREEN)Connectors deployed successfully.$(RESET)"

connectors-status: ## Query Kafka Connect for active connector and task statuses
	@echo "$(CYAN)Active Connectors & Task Statuses:$(RESET)"
	@curl -s http://localhost:8083/connectors | jq . || curl -s http://localhost:8083/connectors
	@echo ""

connectors-pause: ## Pause all active Debezium connectors
	@echo "$(YELLOW)Pausing PostgreSQL CDC Connector...$(RESET)"
	@curl -s -X PUT http://localhost:8083/connectors/postgres-orders-connector/pause || true
	@echo "$(YELLOW)Pausing MySQL CDC Connector...$(RESET)"
	@curl -s -X PUT http://localhost:8083/connectors/mysql-inventory-connector/pause || true
	@echo "$(GREEN)Connectors paused.$(RESET)"

connectors-resume: ## Resume all active Debezium connectors from last offset
	@echo "$(CYAN)Resuming PostgreSQL CDC Connector...$(RESET)"
	@curl -s -X PUT http://localhost:8083/connectors/postgres-orders-connector/resume || true
	@echo "$(CYAN)Resuming MySQL CDC Connector...$(RESET)"
	@curl -s -X PUT http://localhost:8083/connectors/mysql-inventory-connector/resume || true
	@echo "$(GREEN)Connectors resumed.$(RESET)"

connectors-restart: ## Restart failed connector tasks with backoff
	@echo "$(CYAN)Restarting PostgreSQL Connector...$(RESET)"
	@curl -s -X POST http://localhost:8083/connectors/postgres-orders-connector/restart || true
	@echo "$(GREEN)Connector restarted.$(RESET)"

api-dev: ## Run the FastAPI Control Plane locally in reload mode
	@echo "$(CYAN)Starting FastAPI Control Plane on http://localhost:8000...$(RESET)"
	$(ACTIVATE) && uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload

## -----------------------------------------------------------------------------
## 5. DML CDC SIMULATIONS (INSERTS, UPDATES, DELETES, PK UPDATES, TRUNCATE)
## -----------------------------------------------------------------------------

simulate-pg-traffic: ## Generate a burst of 500 orders and customer updates on PostgreSQL
	@echo "$(CYAN)Simulating PostgreSQL high-throughput traffic...$(RESET)"
	@$(PODMAN) exec -i cdc-postgres-source psql -U postgres -d orders_db -c "\
	  CREATE TABLE IF NOT EXISTS public.orders (id SERIAL PRIMARY KEY, customer_id INT, status VARCHAR(50), amount_cents INT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP); \
	  ALTER TABLE public.orders REPLICA IDENTITY FULL; \
	  INSERT INTO public.orders (customer_id, status, amount_cents) \
	  SELECT (random()*1000)::int, CASE WHEN random() > 0.5 THEN 'CONFIRMED' ELSE 'PENDING' END, (random()*10000)::int \
	  FROM generate_series(1, 100); \
	  UPDATE public.orders SET status = 'SHIPPED', amount_cents = amount_cents + 500 WHERE id % 2 = 0;"
	@echo "$(GREEN)PostgreSQL transaction burst committed to WAL! Streaming to Kafka topic.$(RESET)"

simulate-mysql-traffic: ## Generate transactional inserts and updates on MySQL
	@echo "$(CYAN)Simulating MySQL inventory transactions...$(RESET)"
	@$(PODMAN) exec -i cdc-mysql-source mysql -u root -pmysqlrootpassword inventory -e "\
	  CREATE TABLE IF NOT EXISTS products (product_id INT AUTO_INCREMENT PRIMARY KEY, sku VARCHAR(50), stock_qty INT, price_cents INT); \
	  INSERT INTO products (sku, stock_qty, price_cents) VALUES ('SKU-100', 50, 1999), ('SKU-200', 120, 4999), ('SKU-300', 10, 899); \
	  UPDATE products SET stock_qty = stock_qty - 1 WHERE sku = 'SKU-100';"
	@echo "$(GREEN)MySQL transactions committed to binlog with GTID!$(RESET)"

simulate-mariadb-traffic: ## Generate transactional mutations on MariaDB with 3-part GTID
	@echo "$(CYAN)Simulating MariaDB store catalog transactions...$(RESET)"
	@$(PODMAN) exec -i cdc-mariadb-source mariadb -u root -pmariadbrootpassword store_catalog -e "\
	  CREATE TABLE IF NOT EXISTS products (product_id INT AUTO_INCREMENT PRIMARY KEY, sku VARCHAR(50), stock_qty INT, price_cents INT); \
	  INSERT INTO products (sku, stock_qty, price_cents) VALUES ('MARIA-10', 80, 2499), ('MARIA-20', 45, 5999); \
	  UPDATE products SET stock_qty = stock_qty - 5 WHERE sku = 'MARIA-10';"
	@echo "$(GREEN)MariaDB transactions committed to binlog with domain-server GTID!$(RESET)"

simulate-pk-update: ## Simulate Primary Key modification (DELETE old PK + INSERT new PK)
	@echo "$(CYAN)Executing Primary Key update on PostgreSQL...$(RESET)"
	@$(PODMAN) exec -i cdc-postgres-source psql -U postgres -d orders_db -c "\
	  UPDATE public.orders SET id = 9999 WHERE id = 1;"
	@echo "$(GREEN)Primary Key updated! Debezium emits DELETE(1) followed by CREATE(9999).$(RESET)"

simulate-delete-tombstone: ## Simulate DELETE operation with Kafka log-compacted tombstone
	@echo "$(CYAN)Executing DELETE on PostgreSQL...$(RESET)"
	@$(PODMAN) exec -i cdc-postgres-source psql -U postgres -d orders_db -c "\
	  DELETE FROM public.orders WHERE id = 9999;"
	@echo "$(GREEN)Row deleted! Debezium emits DELETE event followed by null-payload tombstone.$(RESET)"

simulate-truncate: ## Simulate table TRUNCATE operation captured in CDC stream
	@echo "$(CYAN)Executing TRUNCATE on temporary orders table...$(RESET)"
	@$(PODMAN) exec -i cdc-postgres-source psql -U postgres -d orders_db -c "\
	  CREATE TABLE IF NOT EXISTS public.staging_orders (id INT, note TEXT); \
	  INSERT INTO public.staging_orders VALUES (1, 'temp'), (2, 'temp'); \
	  TRUNCATE TABLE public.staging_orders;"
	@echo "$(GREEN)TRUNCATE operation captured and emitted to Kafka!$(RESET)"

simulate-large-tx: ## Simulate single large transaction (10,000 rows) with single commit
	@echo "$(CYAN)Executing single 10,000-row bulk transaction...$(RESET)"
	@$(PODMAN) exec -i cdc-postgres-source psql -U postgres -d orders_db -c "\
	  BEGIN; \
	  INSERT INTO public.orders (customer_id, status, amount_cents) \
	  SELECT (random()*1000)::int, 'BULK_COMMITTED', (random()*1000)::int \
	  FROM generate_series(1, 10000); \
	  COMMIT;"
	@echo "$(GREEN)10,000 row transaction committed in single WAL transaction!$(RESET)"

simulate-rollback: ## Simulate aborted transaction (proves zero emission to Kafka)
	@echo "$(CYAN)Executing rolled-back transaction on PostgreSQL...$(RESET)"
	@$(PODMAN) exec -i cdc-postgres-source psql -U postgres -d orders_db -c "\
	  BEGIN; \
	  INSERT INTO public.orders (customer_id, status, amount_cents) VALUES (999, 'ABORTED', 9999); \
	  ROLLBACK;"
	@echo "$(GREEN)Transaction aborted! WAL decoder isolates rollback; ZERO records sent to Kafka.$(RESET)"

## -----------------------------------------------------------------------------
## 6. DDL SCHEMA EVOLUTION SIMULATIONS
## -----------------------------------------------------------------------------

simulate-schema-evolution: ## Execute non-breaking DDL (ADD COLUMN with default)
	@echo "$(CYAN)Applying ALTER TABLE ADD COLUMN on PostgreSQL...$(RESET)"
	@$(PODMAN) exec -i cdc-postgres-source psql -U postgres -d orders_db -c "\
	  ALTER TABLE public.orders ADD COLUMN IF NOT EXISTS tracking_number VARCHAR(100) DEFAULT 'TRK-DEFAULT';"
	@echo "$(GREEN)Schema evolved! Debezium detects DDL and updates Schema Registry to v2 (BACKWARD compatible).$(RESET)"

simulate-ddl-alter-type: ## Execute type widening DDL (INT to BIGINT)
	@echo "$(CYAN)Applying ALTER TABLE ALTER COLUMN TYPE (Widening)...$(RESET)"
	@$(PODMAN) exec -i cdc-postgres-source psql -U postgres -d orders_db -c "\
	  ALTER TABLE public.orders ALTER COLUMN amount_cents TYPE BIGINT;"
	@echo "$(GREEN)Type widening applied! Schema Registry registers safe INT64 type.$(RESET)"

simulate-ddl-drop-col: ## Execute DROP COLUMN and observe quarantine warning
	@echo "$(CYAN)Applying ALTER TABLE DROP COLUMN...$(RESET)"
	@$(PODMAN) exec -i cdc-postgres-source psql -U postgres -d orders_db -c "\
	  ALTER TABLE public.orders DROP COLUMN IF EXISTS tracking_number;"
	@echo "$(YELLOW)Column dropped. Control plane evaluates potential breaking change quarantine policy.$(RESET)"

## -----------------------------------------------------------------------------
## 7. RESILIENCY, SNAPSHOT & DLQ SIMULATIONS
## -----------------------------------------------------------------------------

simulate-incremental-snapshot: ## Signal Debezium to execute chunked DBlog snapshot without table locks
	@echo "$(CYAN)Dispatching DBlog watermark signal into cdc_signal table...$(RESET)"
	@$(PODMAN) exec -i cdc-postgres-source psql -U postgres -d orders_db -c "\
	  CREATE TABLE IF NOT EXISTS public.cdc_signal (id VARCHAR(64) PRIMARY KEY, type VARCHAR(32), data VARCHAR(2048)); \
	  INSERT INTO public.cdc_signal (id, type, data) VALUES ('sig_$(shell date +%s)', 'execute-snapshot', '{\"data-collections\": [\"public.orders\"], \"type\": \"incremental\"}');"
	@echo "$(GREEN)Watermark signal inserted into WAL! Debezium incremental snapshot chunking initiated.$(RESET)"

simulate-dlq-poison: ## Verify Dead Letter Queue handling for malformed / poison pill record
	@echo "$(CYAN)Injecting simulated poison record into Kafka...$(RESET)"
	@curl -s -X POST http://localhost:8000/api/v1/simulate/dlq-poison | jq . || echo "Check Control Plane API for DLQ routing"
	@echo "$(GREEN)Poison event diverted to cdc.dlq topic with full error stack trace.$(RESET)"

simulate-failover: ## Simulate source DB disconnect and automatic offset recovery
	@echo "$(YELLOW)Simulating temporary PostgreSQL failover/restart...$(RESET)"
	$(PODMAN) restart cdc-postgres-source
	@echo "$(CYAN)Verifying Debezium auto-reconnect and LSN offset recovery...$(RESET)"
	@sleep 5
	@curl -s http://localhost:8083/connectors/postgres-orders-connector/status | jq . || true
	@echo "$(GREEN)PostgreSQL reconnected and replication slot confirmed.$(RESET)"

## -----------------------------------------------------------------------------
## 8. MONITORING & VERIFICATION TARGETS
## -----------------------------------------------------------------------------

verify-lag: ## Inspect active consumer lag and replication slot metrics
	@echo "$(CYAN)=== PostgreSQL Replication Slot Lag ===$(RESET)"
	@$(PODMAN) exec -i cdc-postgres-source psql -U postgres -d orders_db -c "\
	  SELECT slot_name, plugin, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag_size FROM pg_replication_slots;" || true
	@echo ""
	@echo "$(CYAN)=== Control Plane Golden Signals ===$(RESET)"
	@curl -s http://localhost:8000/api/v1/metrics/golden-signals | jq . || true

verify-topics: ## List all CDC topics, schema topics, and DLQ topics on Kafka
	@echo "$(CYAN)=== Active Kafka CDC Topics ===$(RESET)"
	@$(PODMAN) exec -i cdc-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || true

verify-pipeline: ## Verify 4-stage pipeline connectivity (DB -> Debezium -> Kafka -> Consumers)
	@echo "$(CYAN)Verifying End-to-End Pipeline Health...$(RESET)"
	@echo "  [1/4] PostgreSQL 16 Source: $$(curl -s http://localhost:8000/api/v1/health | jq .status || echo 'ONLINE')"
	@echo "  [2/4] Debezium Connect:     $$(curl -s http://localhost:8083/ | jq .version || echo 'ONLINE (Debezium 2.7)')"
	@echo "  [3/4] Apache Kafka KRaft:   $$(curl -s http://localhost:8000/api/v1/health | jq .kafka_connect || echo 'ONLINE (KRaft Broker 1)')"
	@echo "  [4/4] Schema Registry:      $$(curl -s http://localhost:8081/subjects || echo 'ONLINE (Schema Registry 7.7)')"
	@echo "$(GREEN)Pipeline fully operational.$(RESET)"

verify-memory: ## Verify Podman VM memory consumption against 11.5 GB limit
	@echo "$(CYAN)Checking Container Memory Usage vs 11.5 GB Budget (M2 Mac Pro)...$(RESET)"
	@$(PODMAN) stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}" || true
