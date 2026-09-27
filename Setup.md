# Setup Guide: Enterprise CDC Platform on Podman (M2 Mac Pro 16 GB RAM)

This guide provides end-to-end setup instructions for deploying and running the **Enterprise Change Data Capture (CDC) to Apache Kafka Platform** on an **Apple Silicon (M2 Mac Pro) with 16 GB Unified Memory**, using **Podman rootless containers** and an asynchronous **Python 3.12 + FastAPI** control plane.

---

## 1. System Requirements & Hardware Constraints

| Component | Specification |
| :--- | :--- |
| **Host Machine** | Apple Silicon M2 Mac Pro / MacBook Pro (ARM64) |
| **Host Memory** | 16 GB Unified RAM |
| **Container Engine** | Podman 5.0+ (Rootless with QEMU/Apple Hypervisor VM) |
| **Container Memory Cap** | **11.5 GB allocated to Podman VM**, leaving 4.5 GB for macOS and developer tools |
| **Python Runtime** | Python 3.12+ (or Python 3.10+) |
| **Build Automation** | GNU Make (`make`) |

---

## 2. Podman Machine Initialization for M2 Mac Pro

Standard Docker Desktop or unconstrained container engines can easily exhaust a 16 GB Mac. Follow these exact sizing parameters to avoid macOS memory swapping or out-of-memory (OOM) killer invocations:

```bash
# 1. Stop existing default machine (if any)
podman machine stop
podman machine rm -f podman-machine-default 2>/dev/null || true

# 2. Initialize a dedicated VM with 6 vCPUs, 11520 MB RAM (11.25 GB), and 50 GB disk
podman machine init \
  --cpus 6 \
  --memory 11520 \
  --disk-size 50 \
  cdc-m2-vm

# 3. Set as default and start the VM
podman machine set --rootful=false cdc-m2-vm
podman machine start cdc-m2-vm

# 4. Verify machine resources
podman machine info
```

*(Alternatively, run `make podman-init` directly).*

---

## 3. Host Dependencies Installation

Install necessary CLI utilities using Homebrew:

```bash
brew install podman podman-compose python@3.12 kcat jq
```

* `podman`: Daemonless rootless container engine.
* `podman-compose`: Compose specification runner for Podman.
* `python@3.12`: Host runtime for FastAPI control plane and test runner.
* `kcat` (formerly `kafkacat`): Real-time Kafka topic inspector.
* `jq`: JSON processor for CLI scripting.

---

## 4. Python Virtual Environment Setup

Create an isolated virtual environment for the FastAPI control plane and test suite:

```bash
# Create and activate virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Upgrade pip and install dependencies
pip install --upgrade pip
pip install -r backend/requirements.txt
```

*(Alternatively, run `make setup-env`).*

---

## 5. Podman Resource Allocation Breakdown (16 GB Unified RAM)

The services in `podman-compose.yml` have explicit memory limits (`mem_limit`) and JVM heap reservations configured:

| Container Service | Base Image (ARM64) | Max Memory Limit | JVM Heap / Buffer Configuration | Role |
| :--- | :--- | :--- | :--- | :--- |
| **Apache Kafka (KRaft)** | `apache/kafka:3.8.0` | `1800 MB` | `-Xms1024m -Xmx1280m` | Partitioned commit log broker |
| **Kafka Connect (Debezium)** | `quay.io/debezium/connect:2.7` | `2400 MB` | `-Xms1536m -Xmx2048m` (G1GC) | Log decoding & SMT transforms |
| **Schema Registry** | `cp-schema-registry:7.7.0` | `600 MB` | `-Xms384m -Xmx512m` | Confluent Avro schemas |
| **PostgreSQL Source** | `postgres:16-alpine` | `1400 MB` | `shared_buffers = 384MB`, `wal_level = logical` | Source DB (`REPLICA IDENTITY FULL`) |
| **MySQL Source** | `mysql:8.4` | `1400 MB` | `innodb_buffer_pool = 512MB`, `binlog_format = ROW` | Source DB (`gtid_mode=ON`) |
| **MariaDB Source** | `mariadb:11.4` | `1200 MB` | `innodb_buffer_pool = 384MB`, `binlog_format = ROW` | Source DB (`gtid_strict_mode=ON`, port 3308) |
| **MongoDB Source** | `mongo:7.0` | `1000 MB` | WiredTiger Cache `= 384MB` | Source DB (Replica Set Oplog) |
| **Metadata DB (Postgres)** | `postgres:16-alpine` | `700 MB` | Standard PostgreSQL | Control plane store |
| **FastAPI Control Plane** | `python:3.12-slim` | `300 MB` | 2 Uvicorn async workers (~120MB idle) | Control plane API & discovery |
| **Total Container Pool** | — | **10.80 GB (Peak 11.45 GB)** | **Host Headroom: 4.55 GB Free (Zero Paging on M2 16GB)** |

---

## 6. Starting & Operating the Platform via Make

The included `Makefile` provides complete one-command operations:

```bash
# 1. Start all infrastructure containers (Kafka, Connect, Registry, DB sources)
make up

# 2. Check health status and live memory usage vs 11.5 GB budget
make status

# 3. Provision and register all Debezium CDC connectors
make connectors-init

# 4. Verify connector and task status
make connectors-status

# 5. Run the complete automated test suite (38 unit, integration, config & SOLID tests)
make test

# 6. Start the FastAPI Control Plane locally
make api-dev
```

---

## 7. Verifying CDC Data Pipeline End-to-End

### 7.1 Inspecting Active Kafka Topics
```bash
# List all topics created by the Debezium engine
podman exec -it cdc-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### 7.2 Consuming Real-Time CDC Events with `kcat`
```bash
# Tail live PostgreSQL order changes
kcat -b localhost:9092 -t cdc.local.ecommerce.orders_db.public.orders -C -e -q | jq .

# Tail live MySQL customer changes
kcat -b localhost:9092 -t cdc.local.mysql_store.inventory.products -C -e -q | jq .
```

---

## 8. Running Simulation Workloads

Test CDC under load, failure injection, schema changes, and edge cases:

```bash
# 1. High-throughput PostgreSQL traffic (inserts, updates, deletes)
make simulate-pg-traffic

# 2. MySQL transactional traffic (orders & inventory balance)
make simulate-mysql-traffic

# 3. Primary Key update (DELETE old PK + INSERT new PK)
make simulate-pk-update

# 4. DELETE operation followed by Kafka compaction tombstone
make simulate-delete-tombstone

# 5. Table TRUNCATE operation
make simulate-truncate

# 6. Bulk transaction (10,000 rows in single commit)
make simulate-large-tx

# 7. Aborted transaction (proves ZERO emission to Kafka)
make simulate-rollback

# 8. Dynamic Schema Evolution (ALTER TABLE ADD COLUMN with Schema Registry validation)
make simulate-schema-evolution

# 9. Column type widening (INT to BIGINT)
make simulate-ddl-alter-type

# 10. Dropping column and checking quarantine policy
make simulate-ddl-drop-col

# 11. DBlog Incremental Snapshot (read chunked keys between log watermarks)
make simulate-incremental-snapshot

# 12. Poison Pill / Dead Letter Queue (DLQ) validation
make simulate-dlq-poison

# 13. Source database failover & offset recovery
make simulate-failover

# 14. Check consumer lag across all topics
make verify-lag

# 15. Check memory consumption against the 11.5 GB budget
make verify-memory
```

---

## 9. Clean Shutdown & Tear Down

```bash
# Stop containers gracefully
make down

# Complete teardown (wipes test volumes and transient data)
make clean
```

---

## 10. Externalized Configuration & Properties Management (Zero Hardcoding)

All database credentials, ports, cluster URLs, and replication parameters are externalized according to 12-factor principles:

| Configuration File | Scope & Contents | Precedence Order |
| :--- | :--- | :--- |
| **`config/database-connectors.properties`** | Hostnames, internal/external ports, credentials, DB names, connection URLs, topic prefixes for PostgreSQL, MySQL, MariaDB, MongoDB, Oracle, SQL Server | File Level (Overridable via ENV) |
| **`config/cdc-platform.properties`** | Kafka KRaft broker list, Debezium worker IDs, Schema Registry URLs, SMT salt/fields, DLQ topics, SLO targets | File Level (Overridable via ENV) |
| **`config/platform-config.json`** | Typed JSON schema used by microservices and UI dashboard | Fallback Data Matrix |
| **`.env.example` / OS Environment** | `POSTGRES_SOURCE_URL`, `MARIADB_SOURCE_URL`, `MYSQL_SOURCE_URL`, `KAFKA_BOOTSTRAP_SERVERS` | **Highest Precedence (Runtime Override)** |

To override any property at runtime, specify standard 12-factor environment variables:
```bash
export MARIADB_SOURCE_HOST="mariadb-prod-cluster.internal"
export MARIADB_SOURCE_PORT=3306
export MARIADB_SOURCE_PASSWORD="secure-vault-password"
python3 -m unittest tests/test_configuration_properties.py
```

