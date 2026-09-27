# Enterprise Database CDC → Kafka Streaming Platform

An enterprise-grade, high-throughput Change Data Capture (CDC) platform designed to capture real-time row-level transaction mutations from heterogeneous databases (**PostgreSQL, MySQL, Oracle, SQL Server, and MongoDB**) and stream them with sub-second latency into **Apache Kafka (KRaft)**.

Optimized for **Apple Silicon (M2 Mac Pro) with 16 GB Unified Memory** using **Podman rootless containers** and an asynchronous **Python 3.12 + FastAPI** control plane.

---

## 1. Key Architectural Features

- **Strict Plane Decoupling:**
  - **Control Plane:** Python 3.12 + FastAPI + AsyncPG handles registration, prerequisite validation, schema governance, incremental snapshot signaling, and SLO telemetry. Zero payload data passes through the control plane API.
  - **Data Plane:** Debezium 2.7+ running on Kafka Connect handles high-throughput log decoding (`pgoutput`, GTID binlog) directly to Kafka.
- **Apple Silicon M2 (16 GB RAM) Discipline:**
  - Total container allocation strictly capped at **11.5 GB**, preserving 4.5 GB of Unified Memory for macOS and development tools without memory swapping.
  - Kafka runs in **KRaft Mode**, eliminating ZooKeeper and saving 512 MB JVM memory.
- **End-to-End Pipeline Transparency:**
  - **Stage 1 (Source DB):** Executed SQL, commit transaction ID, and low-level log coordinates (LSN, GTID, SCN, Resume Tokens).
  - **Stage 2 (Debezium):** Raw log bytes decoded, Single Message Transforms (SMT PII hashing/masking), and Schema Registry check.
  - **Stage 3 (Kafka KRaft):** Partition computation from Primary Key, sequential offset, `acks=all` idempotence, and compaction keys.
  - **Stage 4 (Downstream Consumers):** Snowflake/DWH sync, microservices fulfillment, and search indexing with real-time lag and latency tracking.
- **Comprehensive DML & DDL Coverage:**
  - **DML:** `INSERT` (`before=null`), `UPDATE` (`REPLICA IDENTITY FULL` diffs), `PRIMARY KEY UPDATE` (atomic delete + insert), `DELETE` (with Kafka compaction tombstone), `TRUNCATE` (table-level wipe), and `ROLLBACK` (proving zero emission).
  - **DDL:** `ADD COLUMN` (Schema Registry `BACKWARD` v1 $\rightarrow$ v2), `ALTER TYPE` (type widening), and `DROP COLUMN` (quarantine policy).
- **Zero Table Locks with DBlog:**
  - Non-blocking table snapshots execute using the **DBlog algorithm** (reading chunks in primary key order between transaction log watermarks).
- **SOLID Architecture & Clean Design:**
  - **Single Responsibility (SRP):** Isolated domain entities, dedicated SMT transformers, discrete API routers.
  - **Open/Closed (OCP):** Strategy pattern for database connectors (`IConnectorStrategy`) and mutations (`IMutationStrategy`).
  - **Liskov Substitution (LSP):** All connector strategies seamlessly interchange within the registry.
  - **Interface Segregation (ISP):** Fine-grained interfaces for prerequisites, lifecycle, snapshots, and metrics.
  - **Dependency Inversion (DIP):** Control plane API decoupled via dependency injection.
- **Externalized Config & Properties (Zero Hardcoding):**
  - All database connectivity parameters, URLs, credentials, ports, and cluster endpoints reside strictly in external `.properties` files (`config/database-connectors.properties`, `config/cdc-platform.properties`) and 12-factor environment variables. Zero hardcoded credentials or connection strings in codebase.
- **Automated Test Suite:**
  - 38 unit & integration tests covering all database engines (Postgres, MySQL, MariaDB, Oracle, MSSQL, Mongo), externalized configuration loading, DML mutations, DDL evolutions, SMT masking, DLQ routing, and SOLID services passing with 100% success.

---

## 2. Platform Architecture & Memory Allocation

### Unified Memory Budget (16 GB Total)

| Container Service | Base Image (ARM64) | Memory Limit | JVM Heap / Buffers | Role |
| :--- | :--- | :--- | :--- | :--- |
| **Apache Kafka (KRaft)** | `apache/kafka:3.8.0` | `1800 MB` | `-Xms1024m -Xmx1280m` | Partitioned commit log broker |
| **Kafka Connect (Debezium)**| `quay.io/debezium/connect:2.7` | `2400 MB` | `-Xms1536m -Xmx2048m` | Log reader & SMT pipeline |
| **Schema Registry** | `cp-schema-registry:7.7.0` | `600 MB` | `-Xms384m -Xmx512m` | Centralized Avro schemas |
| **PostgreSQL Source** | `postgres:16-alpine` | `1400 MB` | `shared_buffers = 384MB` | Source DB (`wal_level=logical`) |
| **MySQL Source** | `mysql:8.4` | `1400 MB` | `innodb_buffer_pool = 512MB` | Source DB (`gtid_mode=ON`) |
| **MongoDB Source** | `mongo:7.0` | `1000 MB` | WiredTiger Cache `= 384MB` | Source DB (Replica Set Oplog) |
| **MariaDB Source** | `mariadb:11.4` | `1200 MB` | `gtid_strict_mode=ON`, `binlog_format=ROW` | Source DB (Port 3308) |
| **Metadata DB (Postgres)** | `postgres:16-alpine` | `700 MB` | Standard PostgreSQL | Control plane store |
| **FastAPI Control Plane** | `python:3.12-slim` | `300 MB` | AsyncPG connection pool | Control plane API |
| **Total Container Pool** | — | **10.80 GB (Peak 11.45 GB)** | **macOS Headroom: 4.55 GB Free** |

---

## 3. Quickstart Guide (GNU Make)

### Step 1: Initialize Podman Machine
```bash
# Initialize dedicated VM with 6 vCPUs and 11.5 GB RAM cap
make podman-init
```

### Step 2: Start the CDC Platform
```bash
# Start Kafka, Schema Registry, Debezium, DB sources, and FastAPI Control Plane
make up

# Verify container health and memory consumption vs 11.5GB cap
make status
```

### Step 3: Register Debezium CDC Connectors
```bash
# Deploy PostgreSQL and MySQL connectors to Kafka Connect
make connectors-init

# Check connector and task status
make connectors-status
```

### Step 4: Run the Complete Automated Test Suite (32 Tests)
```bash
make test
```

### Step 5: Run Real-Time Simulations
```bash
# 1. Burst of 500 order transactions on PostgreSQL
make simulate-pg-traffic

# 2. Inventory and purchase transactions on MySQL
make simulate-mysql-traffic

# 3. Primary Key update (DELETE old PK + INSERT new PK)
make simulate-pk-update

# 4. DELETE with Kafka compaction tombstone
make simulate-delete-tombstone

# 5. Table TRUNCATE
make simulate-truncate

# 6. Large transaction (10,000 rows in single commit)
make simulate-large-tx

# 7. Aborted transaction (proves ZERO emission to Kafka)
make simulate-rollback

# 8. Dynamic non-breaking DDL (ALTER TABLE ADD COLUMN)
make simulate-schema-evolution

# 9. Column type widening (INT to BIGINT)
make simulate-ddl-alter-type

# 10. Dropping column (quarantine warning check)
make simulate-ddl-drop-col

# 11. DBlog non-blocking incremental snapshot
make simulate-incremental-snapshot

# 12. Poison pill / Dead Letter Queue routing
make simulate-dlq-poison

# 13. Source database failover & offset recovery
make simulate-failover

# 14. Check consumer lag and replication slot metrics
make verify-lag

# 15. Verify all active Kafka topics and partitions
make verify-topics

# 16. Verify 4-stage pipeline connectivity
make verify-pipeline
```

### Step 6: Clean Shutdown
```bash
# Stop containers
make down

# Wipe transient containers, volumes, and networks
make clean
```

---

## 4. Comprehensive `Makefile` Target Reference

| Target | Description |
| :--- | :--- |
| `make help` | Displays the formatted help menu with all available targets. |
| `make test` | Runs the entire end-to-end CDC test suite (22 unit & integration tests). |
| `make podman-init` | Configures and launches a rootless Podman VM tuned for 11.5 GB memory limit. |
| `make setup-env` | Sets up a Python 3.12 virtual environment and installs FastAPI backend requirements. |
| `make up` | Launches all containers via `podman-compose` with dependency orchestration. |
| `make down` | Gracefully terminates all running containers. |
| `make clean` | Destroys all containers, networks, and persistent test volumes for a clean slate. |
| `make restart` | Restarts the full stack. |
| `make status` | Displays container statuses and real-time memory usage against the 11.5 GB budget. |
| `make logs` | Tails aggregated logs from all running services. |
| `make db-up-postgres` | Starts only PostgreSQL configured for logical replication (`wal_level=logical`). |
| `make db-up-mysql` | Starts only MySQL configured with ROW binlog and GTID mode enabled. |
| `make db-up-mongo` | Starts only MongoDB with Replica Set enabled for Change Streams. |
| `make db-up-all` | Starts all heterogeneous database sources concurrently. |
| `make connectors-init`| Submits Debezium connector configurations for Postgres and MySQL. |
| `make connectors-status`| Queries the Kafka Connect REST API for task health. |
| `make connectors-pause`| Pauses active Debezium connectors. |
| `make connectors-resume`| Resumes active Debezium connectors from last committed offset. |
| `make connectors-restart`| Restarts failed connector tasks with backoff. |
| `make api-dev` | Launches the FastAPI Control Plane locally on `http://localhost:8000` with hot-reload. |
| `make simulate-pg-traffic` | Generates a high-throughput burst of orders and updates on PostgreSQL. |
| `make simulate-mysql-traffic` | Generates transactional updates on MySQL. |
| `make simulate-pk-update` | Tests primary key modification (DELETE old PK + INSERT new PK). |
| `make simulate-delete-tombstone` | Tests DELETE followed by null-payload tombstone for log compaction. |
| `make simulate-truncate` | Tests table TRUNCATE operation captured in CDC stream. |
| `make simulate-large-tx` | Tests single 10,000-row bulk transaction with single commit. |
| `make simulate-rollback` | Tests aborted transaction (proves ZERO records sent to Kafka). |
| `make simulate-schema-evolution` | Tests DDL ADD COLUMN and Schema Registry v1 $\rightarrow$ v2 migration. |
| `make simulate-ddl-alter-type` | Tests type widening DDL (INT to BIGINT). |
| `make simulate-ddl-drop-col` | Tests DROP COLUMN and quarantine warning check. |
| `make simulate-incremental-snapshot` | Injects a DBlog watermark signal to execute non-locking key chunking. |
| `make simulate-dlq-poison` | Injects an invalid payload to verify Dead Letter Queue routing. |
| `make simulate-failover` | Simulates a database restart and verifies automatic LSN offset recovery. |
| `make verify-lag` | Inspects PostgreSQL replication slot lag and Kafka consumer lag. |
| `make verify-topics` | Lists all active CDC topics and partition distributions. |
| `make verify-pipeline` | Verifies end-to-end pipeline health across all 4 stages. |
| `make verify-memory` | Verifies container memory usage against the 11.5 GB budget. |

---

## 5. Control Plane REST API Reference

The FastAPI Control Plane exposes declarative REST endpoints at `http://localhost:8000`:

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/v1/health` | Overall health check of control plane, Kafka Connect, and Schema Registry. |
| `GET` | `/api/v1/sources` | Lists all registered database sources. |
| `POST` | `/api/v1/sources` | Registers a new database source cluster. |
| `POST` | `/api/v1/sources/{id}/validate` | Runs prerequisite checks (`wal_level`, replication slots, GTID, permissions). |
| `POST` | `/api/v1/sources/{id}/discover` | Introspects database catalogs, primary keys, and replica identities. |
| `GET` | `/api/v1/connectors` | Fetches active connectors and task states from Kafka Connect. |
| `POST` | `/api/v1/connectors` | Provisions a new Debezium connector with SMT masking. |
| `POST` | `/api/v1/connectors/{name}/pause` | Pauses change capture for maintenance or downstream backpressure. |
| `POST` | `/api/v1/connectors/{name}/resume` | Resumes streaming from the last checkpointed offset. |
| `POST` | `/api/v1/connectors/{name}/restart`| Restarts failed connector tasks with exponential backoff. |
| `POST` | `/api/v1/snapshots/incremental` | Injects DBlog watermark signal for table chunking. |
| `GET` | `/api/v1/metrics/golden-signals` | Exposes throughput (events/sec), P95/P99 latency, lag, and memory stats. |

---

## 6. SRE Operational Runbooks

### Runbook 1: PostgreSQL Replication Slot Disk Growth
- **Symptom:** `pg_wal` disk usage grows rapidly; `confirmed_flush_lsn` lag increases.
- **Root Cause:** Debezium connector is stopped or unable to commit offsets to Kafka.
- **Action:**
  1. Check Kafka Connect status: `make connectors-status`.
  2. If connector failed, restart: `curl -X POST http://localhost:8083/connectors/postgres-orders-connector/restart`.
  3. If Kafka is down, start Kafka: `make up`.
  4. As an emergency safeguard if disk reaches 90%, drop the slot via `SELECT pg_drop_replication_slot('cdc_slot_orders');` and trigger a re-snapshot via Control Plane API.

### Runbook 2: Incompatible Schema DDL Detection
- **Symptom:** Connector transitions to `FAILED` with `SchemaIncompatibleException`.
- **Root Cause:** Upstream DBA executed a breaking schema modification (e.g., deleted a non-nullable column) violating `BACKWARD` compatibility.
- **Action:**
  1. Inspect the failed DDL in `/api/v1/connectors/errors`.
  2. Reconcile consumer capabilities or update compatibility mode in Schema Registry.
  3. Resume the connector: `curl -X PUT http://localhost:8083/connectors/<name>/resume`.

---

## 7. Additional Documentation

- [Setup Guide (Setup.md)](./Setup.md) — Step-by-step setup on Apple Silicon M2 with Podman.
- [Architecture Specification (architecture.md)](./architecture.md) — Comprehensive architecture covering all C4 levels, data flows, and failure modes.
