# Enterprise CDC Platform: End-to-End Architecture Specification

**Document Version:** 1.0.0  
**Classification:** Enterprise Platform Architecture  
**Author:** Principal CDC & Distributed Systems Architect  
**Runtime:** Apple Silicon M2 Mac Pro (16 GB Unified Memory), Podman Rootless, Python 3.12 + FastAPI Control Plane, Debezium 2.7+ & Apache Kafka (KRaft) Data Plane

---

## Table of Contents
1. [Executive Summary & Core Philosophy](#1-executive-summary--core-philosophy)
2. [C4 Architecture Models](#2-c4-architecture-models)
   - [2.1 Level 1: System Context Diagram](#21-level-1-system-context-diagram)
   - [2.2 Level 2: Container Diagram](#22-level-2-container-diagram)
   - [2.3 Level 3: Component Diagram (FastAPI & Debezium SMT Pipeline)](#23-level-3-component-diagram)
   - [2.4 Level 4: Deployment Diagram (Podman Rootless on M2 ARM64)](#24-level-4-deployment-diagram)
3. [Heterogeneous Database CDC Engine Flows](#3-heterogeneous-database-cdc-engine-flows)
   - [3.1 PostgreSQL (WAL / pgoutput / LSN)](#31-postgresql-cdc-data-flow)
   - [3.2 MySQL (Binary Log / GTID)](#32-mysql-cdc-data-flow)
   - [3.3 Oracle (Redo Log / LogMiner / SCN)](#33-oracle-cdc-data-flow)
   - [3.4 SQL Server (Change Data Capture / LSN)](#34-sql-server-cdc-data-flow)
   - [3.5 MongoDB (Replica Set Change Streams / Resume Tokens)](#35-mongodb-cdc-data-flow)
4. [Incremental Snapshot Architecture (DBlog Algorithm)](#4-incremental-snapshot-architecture-dblog-algorithm)
5. [Kafka Topic Strategy & Schema Governance](#5-kafka-topic-strategy--schema-governance)
6. [Canonical Event Contract](#6-canonical-event-contract)
7. [Failure Recovery & Resiliency Flows](#7-failure-recovery--resiliency-flows)
8. [Security & RBAC Architecture](#8-security--rbac-architecture)
9. [Non-Functional Requirements (NFR) Matrix](#9-non-functional-requirements-nfr-matrix)

---

## 1. Executive Summary & Core Philosophy

The **Enterprise CDC Platform** is an event-driven data capture platform engineered to decouple heterogeneous database mutations from operational queries, streaming transaction logs with sub-second latency into Apache Kafka.

```
┌────────────────────────────────────────────────────────────────────────┐
│               CDC CONTROL PLANE (Python 3.12 + FastAPI)                │
│  • Prerequisite Discovery • Health & Lag Monitoring • Schema Governance│
│  • Incremental Snapshot Triggers • RBAC & Multi-Tenant Quotas • Audit  │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Declarative REST / Config Sync
                                    ▼
┌──────────────────┐      ┌──────────────────────────────────────────────┐
│ Heterogeneous DB │      │     CDC DATA PLANE (Debezium + Kafka Connect) │
│ Sources:         ├─────►│  Log Mining: pgoutput / binlog / redo log     │
│ PG, MySQL, ORA,  │(WAL) │  SMT Masking • Wire-Format Avro Serialization │
│ MSSQL, MongoDB   │      │  At-Least-Once Offset Checkpoint Guarantee   │
└──────────────────┘      └──────────────────────┬───────────────────────┘
                                                 │ Idempotent Producer
                                                 ▼
                          ┌──────────────────────────────────────────────┐
                          │   APACHE KAFKA CLUSTER (KRaft Consensus)     │
                          │  Compacted Topics • Confluent Schema Registry │
                          └──────────────────────┬───────────────────────┘
                                                 │ Sub-Second Streaming
                                                 ▼
                               [ Data Lake / Microservices / Elastic / DWH ]
```

### Core Tenets
1. **Zero Log Parser Reinvention:** Low-level transaction log decoding (PostgreSQL WAL, MySQL binlog, Oracle redo, SQL Server transaction log, MongoDB oplog) is delegated entirely to **Debezium on Kafka Connect**.
2. **Strict Plane Separation:** The **Control Plane** (Python 3.12 + FastAPI + AsyncPG) is completely isolated from high-volume data payloads. It handles registration, prerequisite verification, schema policies, ad-hoc snapshot signaling, and health telemetry.
3. **M2 Mac Pro 16 GB Memory Optimization:** All services run within **Podman rootless containers** strictly budgeted to stay under **11.5 GB of Unified Memory**, allowing zero-paging execution alongside macOS host workloads.
4. **End-to-End Pipeline Transparency:** Full tracing across all 4 stages: Source Database $\rightarrow$ Debezium SMT Engine $\rightarrow$ Kafka KRaft Broker $\rightarrow$ Downstream Consumers.
5. **SOLID Principles & Modular Architecture:** Strict adherence across both control plane microservices and data pipelines:
   - **S (Single Responsibility):** Domain models represent entities only; SMT transforms handle PII/diffing; Routers handle HTTP contracts only.
   - **O (Open/Closed):** Extensible `IConnectorStrategy` registry allows adding new engines (e.g. Cassandra, TiDB) without modifying existing strategies.
   - **L (Liskov Substitution):** All database connector strategies implement `IConnectorStrategy` and can be substituted transparently into the Kafka Connect provisioning registry.
   - **I (Interface Segregation):** Fine-grained interfaces (`IPrerequisiteChecker`, `IConnectorManager`, `ISnapshotCoordinator`, `ISMTTransformer`, `ISchemaRegistryValidator`, `IMetricsProvider`).
   - **D (Dependency Inversion):** FastAPI routers depend strictly on abstract protocols injected via `backend/app/api/dependencies.py`.

### 1.1 The 4-Stage Transparent Event Pipeline

```text
┌─────────────────────────┐      ┌─────────────────────────┐      ┌─────────────────────────┐      ┌─────────────────────────┐
│ STAGE 1: SOURCE DB      │      │ STAGE 2: DEBEZIUM       │      │ STAGE 3: KAFKA KRAFT    │      │ STAGE 4: DOWNSTREAM     │
│                         │      │                         │      │                         │      │                         │
│ • Executed SQL/Mutation ├─────►│ • Raw Log Segment Read  ├─────►│ • Destination Topic     ├─────►│ • Snowflake / DWH Sync  │
│ • Commit Tx ID & Time   │(WAL) │ • SMT Masking (PII)     │(Net) │ • Partition & Offset    │(Sub) │ • Microservices ACK     │
│ • LSN / GTID / SCN Pos  │      │ • Schema Registry Check │      │ • Idempotent Producer   │      │ • Search Indexer ACK    │
│ • Pre- vs Post-Tuple    │      │ • Canonical Wire Header │      │ • Log Compaction Key    │      │ • E2E Latency Tracking  │
└─────────────────────────┘      └─────────────────────────┘      └─────────────────────────┘      └─────────────────────────┘
```

---

## 2. C4 Architecture Models

### 2.1 Level 1: System Context Diagram

```mermaid
C4Context
    title System Context Diagram - Enterprise CDC Platform

    Person(data_engineer, "Data / SRE Engineer", "Registers sources, sets masking policies, triggers resyncs and incremental snapshots")
    System_Ext(source_dbs, "Source Databases", "PostgreSQL, MySQL, Oracle, SQL Server, MongoDB (Primary & Replica clusters)")
    
    Enterprise_Boundary(b0, "Enterprise CDC Platform Boundary") {
        System(control_plane, "CDC Control Plane", "Python FastAPI async service managing connector lifecycles, catalog discovery, RBAC, and audit logs")
        System(data_plane, "CDC Data Plane", "Debezium 2.7 engine on Kafka Connect streaming decoded mutations to Kafka")
    }
    
    System_Ext(schema_reg, "Confluent Schema Registry", "Centralized Avro and JSON Schema compatibility governance")
    System_Ext(downstream, "Downstream Consumers", "Snowflake, BigQuery, Real-Time Microservices, Elasticsearch, Vector Stores")

    Rel(data_engineer, control_plane, "Manages topology, triggers snapshots, inspects lag", "HTTPS / REST / Web UI")
    Rel(control_plane, source_dbs, "Discovers schemas, inspects WAL/binlog prerequisites", "JDBC / AsyncPG (Port 5432, 3306)")
    Rel(control_plane, data_plane, "Provisions and rebalances connectors, sends signal watermarks", "REST API (Port 8083)")
    Rel(source_dbs, data_plane, "Streams binary transaction logs", "Replication TCP Protocol")
    Rel(data_plane, schema_reg, "Registers schema evolutions & validates BACKWARD compatibility", "HTTP REST (Port 8081)")
    Rel(data_plane, downstream, "Emits partitioned, ordered change events", "Kafka Wire Protocol (Port 9092)")
```

---

### 2.2 Level 2: Container Diagram

```mermaid
C4Container
    title Container Diagram - Enterprise CDC Platform (Podman ARM64)

    Container(web_ui, "CDC Web Console", "React 19, Tailwind CSS", "Web frontend for real-time monitoring, catalog exploration, and control")
    Container(fastapi_app, "Control Plane API", "Python 3.12, FastAPI, AsyncPG, Pydantic v2", "Asynchronous control plane for source discovery, connector state machine, and metrics")
    ContainerDb(meta_db, "Metadata Database", "PostgreSQL 16 (Alpine ARM64)", "Persistent store for source registrations, connector manifests, RBAC, and audit logs")
    
    Container(connect_cluster, "CDC Engine (Kafka Connect)", "Debezium 2.7, OpenJDK 21 (ARM64)", "Distributed workers parsing WAL/binlog, applying SMT masking, emitting Avro records")
    Container(kafka_broker, "Apache Kafka Broker", "Kafka 3.8+ (KRaft Quorum Mode)", "Durable commit log broker, log compaction, exactly-once transactional coordination")
    Container(schema_registry, "Schema Registry", "Confluent Schema Registry 7.7", "Enforces schema compatibility (BACKWARD/FULL) and serves schema IDs")

    Rel(web_ui, fastapi_app, "Dispatches management actions", "HTTPS / JSON")
    Rel(fastapi_app, meta_db, "Reads & writes configuration and audit state", "AsyncPG (Port 5432)")
    Rel(fastapi_app, connect_cluster, "Deploys, pauses, restarts connectors", "HTTP REST (Port 8083)")
    Rel(connect_cluster, kafka_broker, "Publishes CDC records and checkpoints offsets", "Kafka TCP (Port 9092)")
    Rel(connect_cluster, schema_registry, "Registers schemas and fetches wire schema IDs", "HTTP (Port 8081)")
```

---

### 2.3 Level 3: Component Diagram

```mermaid
C4Component
    title Component Diagram - FastAPI Control Plane & Kafka Connect Pipeline

    Container_Boundary(cp_boundary, "FastAPI Control Plane") {
        Component(api_router, "API Routers", "FastAPI APIRouter", "REST endpoints for /sources, /connectors, /snapshots, /metrics")
        Component(discovery_engine, "Discovery Engine", "Async Catalog Introspector", "Inspects WAL levels, replication slots, GTID status, and table schemas")
        Component(state_machine, "Connector State Machine", "Lifecycle Orchestrator", "Transitions connector states: REGISTERED -> VALIDATED -> RUNNING -> PAUSED")
        Component(signal_manager, "Signal Manager", "Watermark Injector", "Writes DBlog signaling events into source tables to trigger incremental snapshots")
        Component(audit_service, "Audit & Security Service", "JWT & Policy Evaluator", "Enforces RBAC and appends immutable audit records")
    }

    Container_Boundary(dp_boundary, "Debezium / Kafka Connect Worker") {
        Component(log_reader, "Log Reader Task", "pgoutput / Binlog Reader", "Maintains active replication connection; reads raw transaction segments")
        Component(event_buffer, "Bounded Event Buffer", "BlockingQueue (2048 records)", "In-memory buffer with strict backpressure to prevent JVM OOM")
        Component(smt_chain, "SMT Transformation Chain", "Kafka Connect SMTs", "Applies PII masking, canonical header enrichment, and regex topic routing")
        Component(converter, "Avro/JSON Converter", "Schema Registry Converter", "Prepends 5-byte magic wire header and serializes payload")
        Component(producer, "Idempotent Producer", "KafkaProducer (acks=all)", "Publishes records to Kafka with transactional sequence IDs")
    }

    Rel(api_router, state_machine, "Triggers actions")
    Rel(state_machine, discovery_engine, "Runs prerequisite checks")
    Rel(api_router, signal_manager, "Triggers snapshot")
    Rel(state_machine, log_reader, "Deploys connector task config via Connect REST")
    Rel(log_reader, event_buffer, "Pushes raw records")
    Rel(event_buffer, smt_chain, "Polls record batches")
    Rel(smt_chain, converter, "Passes transformed tuples")
    Rel(converter, producer, "Dispatches binary payloads")
```

---

### 2.4 Level 4: Deployment Diagram (Podman Rootless on macOS M2)

```mermaid
deployment
    title Deployment Diagram - Apple Silicon M2 (16 GB Unified Memory)

    deploymentNode(m2_host, "macOS Host: Apple Silicon M2 (16 GB RAM)", "Physical Machine") {
        deploymentNode(podman_vm, "Podman QEMU/Hypervisor Machine (11.5 GB RAM Cap)", "Virtual Machine") {
            deploymentNode(bridge_net, "Isolated Podman Network: cdc-enterprise-net", "Container Bridge") {
                node(c_fastapi, "cdc-control-plane", "Python 3.12 (Mem: 300MB)")
                node(c_meta_db, "cdc-metadata-db", "PostgreSQL 16 (Mem: 700MB)")
                node(c_connect, "cdc-debezium-connect", "OpenJDK 21 (Mem: 2400MB)")
                node(c_kafka, "cdc-kafka-broker", "KRaft Broker (Mem: 1800MB)")
                node(c_schema, "cdc-schema-registry", "Schema Registry (Mem: 600MB)")
                node(c_pg_src, "cdc-postgres-source", "Postgres 16 WAL (Mem: 1400MB)")
                node(c_my_src, "cdc-mysql-source", "MySQL 8.4 GTID (Mem: 1400MB)")
                node(c_mg_src, "cdc-mongo-source", "MongoDB 7.0 RS (Mem: 1000MB)")
            }
        }
        node(host_tools, "macOS User Space", "Free RAM: ~4.5 GB (Zero Paging)")
    }
```

---

## 3. Heterogeneous Database CDC Engine Flows

### 3.1 PostgreSQL CDC Data Flow

```mermaid
sequenceDiagram
    autonumber
    participant App as App Writes
    participant PG as PostgreSQL (WAL Engine)
    participant Slot as Replication Slot (pgoutput)
    participant Deb as Debezium Connector
    participant SMT as SMT Masking Chain
    participant SR as Schema Registry
    participant K as Kafka Broker (Topic)

    App->>PG: INSERT / UPDATE orders (Replica Identity: FULL)
    PG->>PG: Append to WAL Segment (Write Ahead Log)
    PG->>Slot: Decode WAL records into logical tuples (pgoutput)
    Deb->>Slot: Stream changes via logical replication protocol
    Slot-->>Deb: Raw tuple: LSN 0/16B2D88, TX_ID 9481, Before/After image
    Deb->>SMT: Evaluate PII rules (Mask credit_card, hash email)
    SMT->>SR: Verify schema version & compatibility
    SR-->>SMT: Schema OK (ID: 42)
    SMT->>K: Send record (Key: order_id, acks=all, enable.idempotence=true)
    K-->>Deb: Ack received (Partition 2, Offset 10429)
    Deb->>Slot: Advance confirmed_flush_lsn (Safe to reclaim WAL disk space)
```

---

### 3.2 MySQL CDC Data Flow

```mermaid
sequenceDiagram
    autonumber
    participant Client as Application
    participant MySQL as MySQL 8.4 Server
    participant Binlog as Binary Log (ROW + GTID)
    participant Deb as Debezium MySQL Connector
    participant K as Kafka Broker

    Client->>MySQL: UPDATE customers SET balance = balance - 100
    MySQL->>Binlog: Write GTID event: 24da01e2-b892-11e7-8b0f-0242ac110002:149
    MySQL->>Binlog: Write Table Map & Update Rows Event (Full Row Image)
    Deb->>MySQL: Send COM_BINLOG_DUMP_GTID from last known GTID set
    MySQL-->>Deb: Stream Binlog binary events
    Deb->>Deb: Decode rows into Canonical Event Envelope
    Deb->>K: Publish to cdc.local.mysql_store.inventory.customers
    K-->>Deb: Broker Commit Ack
    Deb->>Deb: Commit GTID set to internal connect-offsets topic
```

---

### 3.3 Oracle CDC Data Flow

```mermaid
sequenceDiagram
    autonumber
    participant Oracle as Oracle 19c / 21c (PDB)
    participant Redo as Online & Archive Redo Logs
    participant Deb as Debezium Oracle Connector
    participant K as Kafka Broker

    Oracle->>Redo: Append Redo Entry with Supplemental Log Data (All Columns)
    Oracle->>Oracle: Advance System Change Number (SCN: 18934812)
    Deb->>Oracle: Query LogMiner dictionary (or XStream Inbound Server)
    Oracle-->>Deb: Mined DML transaction record correlated across RAC threads
    Deb->>K: Emit canonical event with position: { type: "SCN", value: "18934812" }
    K-->>Deb: Ack
```

---

### 3.4 SQL Server CDC Data Flow

```mermaid
sequenceDiagram
    autonumber
    participant App as Application
    participant MSSQL as SQL Server Database
    participant Agent as SQL Server CDC Capture Job
    participant CT as Change Table (cdc.dbo_orders_CT)
    participant Deb as Debezium SQL Server Connector
    participant K as Kafka Broker

    App->>MSSQL: INSERT INTO orders VALUES (...)
    MSSQL->>Agent: Scan transaction log for captured tables
    Agent->>CT: Insert row with __$operation (2=Insert), __$start_lsn
    Deb->>CT: Query cdc.fn_cdc_get_all_changes_... from last LSN
    CT-->>Deb: Return change batch
    Deb->>K: Publish records with LSN coordinates
    K-->>Deb: Ack
```

---

### 3.5 MongoDB CDC Data Flow

```mermaid
sequenceDiagram
    autonumber
    participant App as Client
    participant Mongo as MongoDB Replica Set Primary
    participant Oplog as local.oplog.rs
    participant Deb as Debezium MongoDB Connector
    participant K as Kafka Broker

    App->>Mongo: db.orders.updateOne({_id: 1}, {$set: {status: "SHIPPED"}})
    Mongo->>Oplog: Append oplog entry with resume token
    Deb->>Mongo: Open Change Stream cursor: watch() with resumeToken
    Mongo-->>Deb: Stream change notification (OperationType: update)
    Deb->>K: Publish record with position.type="RESUME_TOKEN"
    K-->>Deb: Ack
```

---

### 3.6 MariaDB CDC Data Flow (GTID Strict Mode)

```mermaid
sequenceDiagram
    autonumber
    participant App as Client Application
    participant MariaDB as MariaDB 11.4 Source (Port 3308)
    participant Binlog as MariaDB Binlog (format=ROW)
    participant Deb as Debezium MariaDB/MySQL Connector
    participant K as Kafka Broker (KRaft 9092)

    App->>MariaDB: INSERT INTO store.inventory (sku, stock, price) VALUES ('SKU-99', 450, 19.99)
    MariaDB->>Binlog: Assign MariaDB GTID (domain-server-seq: 0-1-101)
    MariaDB->>Binlog: Write Table_map and WRITE_ROWS event (ROW image)
    Deb->>MariaDB: Connect with COM_BINLOG_DUMP_GTID using MariaDB GTID format
    MariaDB-->>Deb: Stream binary log events
    Deb->>Deb: Decode into Canonical CDC Envelope with position: { type: "GTID", value: "0-1-101" }
    Deb->>K: Publish to cdc.local.mariadb_orders.store.inventory
    K-->>Deb: In-Sync Replica Ack (acks=all)
    Deb->>Deb: Checkpoint MariaDB GTID state to connect-offsets topic
```

---

## 4. Incremental Snapshot Architecture (DBlog Algorithm)

Large enterprise tables cannot be snapshotted with exclusive or shared table locks without causing downtime. The platform implements the **DBlog watermark signaling algorithm**:

```mermaid
sequenceDiagram
    autonumber
    participant CP as Control Plane API
    participant Src as Source Database
    participant Log as Transaction Log (WAL)
    participant Deb as Debezium Worker
    participant K as Kafka Topic

    CP->>Src: 1. Send signal: INSERT INTO cdc_signal (id, type, data) VALUES ('s1', 'execute-snapshot', '{"data-collections":["public.orders"]}')
    Src->>Log: Signal written into WAL stream
    Deb->>Log: Detect 'snapshot-window-open' watermark in log
    Deb->>Src: 2. Query chunk: SELECT * FROM orders WHERE id >= 1000 AND id < 2000 ORDER BY id ASC (Non-locking query)
    Src-->>Deb: Return chunk rows
    Deb->>Deb: Buffer chunk records in memory
    Deb->>Log: Detect 'snapshot-window-close' watermark in log
    Deb->>Deb: 3. Reconcile: If WAL contains an UPDATE/DELETE for an ID in the chunk, the WAL event takes precedence!
    Deb->>K: 4. Emit deduplicated chunk records as eventType='READ'
    Deb->>CP: Snapshot progress: Table 45% complete, 15,000 rows/sec
```

---

## 5. Kafka Topic Strategy & Schema Governance

### Topic Naming Structure
$$\mathbf{cdc}.\langle\text{env}\rangle.\langle\text{system}\rangle.\langle\text{database}\rangle.\langle\text{schema}\rangle.\langle\text{table}\rangle$$
* Example: `cdc.prod.ecommerce.orders_db.public.orders`

### Partitioning & Compaction
* **Partition Key:** Source primary key (e.g. `order_id`), guaranteeing **strict in-order processing** per record.
* **Cleanup Policy:** `compact,delete`
  * Log compaction guarantees the latest record state is never purged.
  * Time-based retention (e.g. 7 days) preserves the audit history of intermediate mutations.
* **Schema Evolution:** Enforces `BACKWARD` compatibility via Schema Registry. Incompatible DDL changes (e.g., dropping non-nullable columns) trigger automatic quarantine into a Dead Letter Queue (`cdc.dlq.*`).

---

## 6. Canonical Event Contract

Every change event emitted across any database source is normalized into a strict canonical envelope:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "eventId": "evt_01J8F3K9M7Q8R4STVWXYZ01234",
  "eventType": "UPDATE",
  "source": {
    "type": "postgres",
    "cluster": "prod-useast1-db-cluster",
    "database": "ecommerce_platform",
    "schema": "public",
    "table": "orders"
  },
  "transaction": {
    "id": "tx_94810294",
    "timestamp": "2026-09-25T09:48:11.234Z",
    "order": 1
  },
  "position": {
    "type": "LSN",
    "value": "0/16B2D88"
  },
  "key": {
    "order_id": "ord_8849102"
  },
  "before": {
    "order_id": "ord_8849102",
    "customer_id": "cust_1029",
    "status": "PENDING",
    "amount_cents": 14999
  },
  "after": {
    "order_id": "ord_8849102",
    "customer_id": "cust_1029",
    "status": "CONFIRMED",
    "amount_cents": 14999
  },
  "metadata": {
    "connector": "debezium-pg-orders",
    "snapshot": false,
    "sourceTimestamp": "2026-09-25T09:48:11.234Z",
    "captureTimestamp": "2026-09-25T09:48:11.298Z",
    "schemaVersion": "2.1.0",
    "schemaId": 42
  }
}
```

---

## 7. Failure Recovery & Resiliency Flows

```mermaid
flowchart TD
    A[Failure Detected] --> B{Failure Category}
    
    B -->|Database Primary Crash| C[Standby Promoted to Primary]
    C --> D[Control Plane Re-points Connector & Validates Slot]
    D --> E[Resume Streaming from Last Committed LSN/GTID]

    B -->|Kafka Broker Unavailable| F[Connector Buffer Fills Up]
    F --> G[Backpressure Stops Log Ingestion]
    G --> H[Exponential Jittered Retries]
    H -->|Broker Restored| I[Drain Buffer & Commit Offsets]

    B -->|Poison Pill / Serialization Error| J[SMT / Converter Error Handler]
    J --> K[Wrap with Error Stack Trace & Original Payload]
    K --> L[Route to Dead Letter Queue: cdc.dlq.*]
    L --> M[Operator Reviews DLQ via Control Plane API]
```

---

## 8. Security & RBAC Architecture

1. **Role-Based Access Control (RBAC):**
   * `PlatformAdmin`: Full administrative control, connector provisioning, and quota configuration.
   * `CDCOperator`: Pause/resume connectors, trigger incremental snapshots, and replay DLQ events.
   * `Developer`: Read schemas, inspect lag metrics, and query discovered catalogs.
   * `Auditor`: Immutable audit log inspector.
2. **In-Flight Data Masking:** Single Message Transforms (SMTs) evaluate hashing/redaction rules on the connector worker before events reach the Kafka broker network.
3. **Transport Security:** TLS 1.3 enforced on all external endpoints; SASL/SCRAM-SHA-512 authentication on Kafka broker listeners.

---

## 9. Non-Functional Requirements (NFR) Matrix

| Category | NFR Metric | Architectural Guarantee |
| :--- | :--- | :--- |
| **Availability** | $\ge 99.9\%$ | Distributed Kafka Connect task rebalancing with automatic failover. |
| **End-to-End Latency** | P95 $< 3$ seconds, P99 $< 15$ seconds | In-memory log decoding without intermediate disk spooling. |
| **Throughput** | $25,000+$ events/sec per worker | Optimized batch sizes (2048 records) and linger time (10ms). |
| **Data Loss Guarantee** | Zero unacknowledged data loss | `acks=all`, persistent replication slots, and idempotent Kafka producers. |
| **Host Memory Budget** | Strict $\le 11.5$ GB container ceiling | Explicit `mem_limit` and JVM heap caps tuned for Apple Silicon M2 16 GB. |
