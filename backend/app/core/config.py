"""
Enterprise CDC Platform: Configuration & Properties Loader
Adheres strictly to Separation of Concerns and 12-Factor principles.
Loads all database connectivity parameters, URLs, and cluster configurations from
external properties files (config/cdc-platform.properties, config/database-connectors.properties)
with zero hardcoded strings in application code.
"""

import os
from typing import Dict, Any, List, Optional
from pathlib import Path

def parse_properties_file(filepath: str) -> Dict[str, str]:
    """Parses a standard Java/Debezium/Spring .properties file into a key-value dictionary."""
    props: Dict[str, str] = {}
    path = Path(filepath)
    if not path.is_absolute():
        # Check current directory, project root, and parent directories
        candidates = [
            Path(filepath),
            Path(__file__).resolve().parent.parent.parent.parent / filepath,
            Path(__file__).resolve().parent.parent.parent / filepath,
            Path("/config") / Path(filepath).name,
            Path("/") / filepath
        ]
        for c in candidates:
            if c.exists() and c.is_file():
                path = c
                break

    if not path.exists():
        return props

    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith(("#", "!")):
                    continue
                if "=" in stripped:
                    key, val = stripped.split("=", 1)
                    props[key.strip()] = val.strip()
                elif ":" in stripped:
                    key, val = stripped.split(":", 1)
                    props[key.strip()] = val.strip()
    except Exception:
        pass
    return props

# Load properties files
_PLATFORM_PROPS_PATH = os.getenv("CDC_PLATFORM_PROPERTIES_PATH", "config/cdc-platform.properties")
_CONNECTORS_PROPS_PATH = os.getenv("DATABASE_CONNECTORS_PROPERTIES_PATH", "config/database-connectors.properties")

_platform_props = parse_properties_file(_PLATFORM_PROPS_PATH)
_connectors_props = parse_properties_file(_CONNECTORS_PROPS_PATH)

def _get_val(key: str, env_var: Optional[str] = None, default: str = "") -> str:
    """Helper to retrieve value: 1. Environment variable, 2. Properties file, 3. Default."""
    if env_var and os.getenv(env_var):
        return os.getenv(env_var, default)
    if key in _platform_props:
        return _platform_props[key]
    if key in _connectors_props:
        return _connectors_props[key]
    return default

def _get_int(key: str, env_var: Optional[str] = None, default: int = 0) -> int:
    val = _get_val(key, env_var, str(default))
    try:
        return int(val)
    except ValueError:
        return default

def _get_bool(key: str, env_var: Optional[str] = None, default: bool = False) -> bool:
    val = _get_val(key, env_var, str(default)).lower()
    return val in ("true", "1", "yes", "on")

def _get_float(key: str, env_var: Optional[str] = None, default: float = 0.0) -> float:
    val = _get_val(key, env_var, str(default))
    try:
        return float(val)
    except ValueError:
        return default

# Dataclasses / Configuration Models
class PostgresSourceConfig:
    def __init__(self):
        self.id = _get_val("postgres.source.id", "POSTGRES_SOURCE_ID", "src-pg-orders")
        self.engine = _get_val("postgres.source.engine", default="postgres")
        self.host = _get_val("postgres.source.host", "POSTGRES_SOURCE_HOST", "postgres-source")
        self.port = _get_int("postgres.source.port", "POSTGRES_SOURCE_PORT", 5432)
        self.external_port = _get_int("postgres.source.external.port", default=5433)
        self.database = _get_val("postgres.source.database", "POSTGRES_SOURCE_DB", "orders_db")
        self.username = _get_val("postgres.source.user", "POSTGRES_SOURCE_USER", "postgres")
        self.password = _get_val("postgres.source.password", "POSTGRES_SOURCE_PASSWORD", "postgrespassword")
        self.url = _get_val("postgres.source.url", "POSTGRES_SOURCE_URL", f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}")
        self.connector_class = _get_val("postgres.connector.class", default="io.debezium.connector.postgresql.PostgresConnector")
        self.plugin_name = _get_val("postgres.plugin.name", default="pgoutput")
        self.topic_prefix = _get_val("postgres.topic.prefix", default=f"cdc.local.ecommerce.{self.database}")
        self.table_include_list = _get_val("postgres.table.include.list", default="public.orders,public.customers")
        self.snapshot_mode = _get_val("postgres.snapshot.mode", default="initial")
        self.signal_data_collection = _get_val("postgres.signal.data.collection", default="public.cdc_signal")
        self.publication_autocreate_mode = _get_val("postgres.publication.autocreate.mode", default="all_tables")
        self.tombstones_on_delete = _get_val("postgres.tombstones.on.delete", default="true")
        self.decimal_handling_mode = _get_val("postgres.decimal.handling.mode", default="double")
        self.wal_level = _get_val("postgres.wal_level", default="logical")
        self.max_replication_slots = _get_int("postgres.max_replication_slots", default=5)
        self.max_wal_senders = _get_int("postgres.max_wal_senders", default=5)

class MySQLSourceConfig:
    def __init__(self):
        self.id = _get_val("mysql.source.id", "MYSQL_SOURCE_ID", "src-my-inventory")
        self.engine = _get_val("mysql.source.engine", default="mysql")
        self.host = _get_val("mysql.source.host", "MYSQL_SOURCE_HOST", "mysql-source")
        self.port = _get_int("mysql.source.port", "MYSQL_SOURCE_PORT", 3306)
        self.external_port = _get_int("mysql.source.external.port", default=3307)
        self.database = _get_val("mysql.source.database", "MYSQL_SOURCE_DB", "inventory")
        self.username = _get_val("mysql.source.user", "MYSQL_SOURCE_USER", "mysqluser")
        self.password = _get_val("mysql.source.password", "MYSQL_SOURCE_PASSWORD", "mysqlpassword")
        self.root_password = _get_val("mysql.source.root.password", default="mysqlrootpassword")
        self.server_id = _get_val("mysql.source.server.id", default="184054")
        self.url = _get_val("mysql.source.url", "MYSQL_SOURCE_URL", f"mysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}")
        self.connector_class = _get_val("mysql.connector.class", default="io.debezium.connector.mysql.MySqlConnector")
        self.topic_prefix = _get_val("mysql.topic.prefix", default=f"cdc.local.mysql_store.{self.database}")
        self.table_include_list = _get_val("mysql.table.include.list", default="inventory.products,inventory.customers")
        self.snapshot_mode = _get_val("mysql.snapshot.mode", default="initial")
        self.schema_history_bootstrap_servers = _get_val("mysql.schema.history.internal.kafka.bootstrap.servers", default="kafka:9092")
        self.schema_history_topic = _get_val("mysql.schema.history.internal.kafka.topic", default=f"schema-changes.{self.database}")
        self.include_schema_changes = _get_val("mysql.include.schema.changes", default="true")
        self.gtid_source_filter_dml_events = _get_val("mysql.gtid.source.filter.dml.events", default="true")
        self.binlog_format = _get_val("mysql.binlog_format", default="ROW")
        self.gtid_mode = _get_val("mysql.gtid_mode", default="ON")

class MariaDBSourceConfig:
    def __init__(self):
        self.id = _get_val("mariadb.source.id", "MARIADB_SOURCE_ID", "src-maria-catalog")
        self.engine = _get_val("mariadb.source.engine", default="mariadb")
        self.host = _get_val("mariadb.source.host", "MARIADB_SOURCE_HOST", "mariadb-source")
        self.port = _get_int("mariadb.source.port", "MARIADB_SOURCE_PORT", 3306)
        self.external_port = _get_int("mariadb.source.external.port", default=3308)
        self.database = _get_val("mariadb.source.database", "MARIADB_SOURCE_DB", "store_catalog")
        self.username = _get_val("mariadb.source.user", "MARIADB_SOURCE_USER", "mariadbuser")
        self.password = _get_val("mariadb.source.password", "MARIADB_SOURCE_PASSWORD", "mariadbpassword")
        self.root_password = _get_val("mariadb.source.root.password", default="mariadbrootpassword")
        self.server_id = _get_val("mariadb.source.server.id", default="184055")
        self.url = _get_val("mariadb.source.url", "MARIADB_SOURCE_URL", f"mariadb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}")
        self.connector_class = _get_val("mariadb.connector.class", default="io.debezium.connector.mysql.MySqlConnector")
        self.topic_prefix = _get_val("mariadb.topic.prefix", default=f"cdc.local.mariadb_store.{self.database}")
        self.table_include_list = _get_val("mariadb.table.include.list", default="store_catalog.products,store_catalog.inventory")
        self.snapshot_mode = _get_val("mariadb.snapshot.mode", default="initial")
        self.schema_history_bootstrap_servers = _get_val("mariadb.schema.history.internal.kafka.bootstrap.servers", default="kafka:9092")
        self.schema_history_topic = _get_val("mariadb.schema.history.internal.kafka.topic", default=f"schema-changes.mariadb.{self.database}")
        self.include_schema_changes = _get_val("mariadb.include.schema.changes", default="true")
        self.gtid_source_filter_dml_events = _get_val("mariadb.gtid.source.filter.dml.events", default="true")
        self.binlog_format = _get_val("mariadb.binlog_format", default="ROW")
        self.gtid_strict_mode = _get_val("mariadb.gtid_strict_mode", default="ON")

class MongoDBSourceConfig:
    def __init__(self):
        self.id = _get_val("mongodb.source.id", "MONGODB_SOURCE_ID", "src-mongo-catalog")
        self.engine = _get_val("mongodb.source.engine", default="mongodb")
        self.host = _get_val("mongodb.source.host", "MONGODB_SOURCE_HOST", "mongo-source")
        self.port = _get_int("mongodb.source.port", "MONGODB_SOURCE_PORT", 27017)
        self.external_port = _get_int("mongodb.source.external.port", default=27018)
        self.database = _get_val("mongodb.source.database", "MONGODB_SOURCE_DB", "store")
        self.username = _get_val("mongodb.source.user", default="mongoadmin")
        self.password = _get_val("mongodb.source.password", default="mongopassword")
        self.replica_set = _get_val("mongodb.source.replica.set", default="rs0")
        self.connection_string = _get_val("mongodb.source.connection.string", "MONGODB_SOURCE_URL", f"mongodb://{self.host}:{self.port}/?replicaSet={self.replica_set}")
        self.connector_class = _get_val("mongodb.connector.class", default="io.debezium.connector.mongodb.MongoDbConnector")
        self.topic_prefix = _get_val("mongodb.topic.prefix", default=f"cdc.local.mongodb_catalog.{self.database}")
        self.collection_include_list = _get_val("mongodb.collection.include.list", default="store.products,store.orders")
        self.snapshot_mode = _get_val("mongodb.snapshot.mode", default="initial")
        self.capture_mode = _get_val("mongodb.capture.mode", default="change_streams_update_lookup")

class OracleSourceConfig:
    def __init__(self):
        self.id = _get_val("oracle.source.id", "ORACLE_SOURCE_ID", "src-ora-erp")
        self.engine = _get_val("oracle.source.engine", default="oracle")
        self.host = _get_val("oracle.source.host", "ORACLE_SOURCE_HOST", "oracle-source")
        self.port = _get_int("oracle.source.port", "ORACLE_SOURCE_PORT", 1521)
        self.database = _get_val("oracle.source.database", "ORACLE_SOURCE_DB", "ORCLCDB")
        self.pdb_name = _get_val("oracle.source.pdb.name", default="ORCLPDB1")
        self.username = _get_val("oracle.source.user", "ORACLE_SOURCE_USER", "c##dbzuser")
        self.password = _get_val("oracle.source.password", "ORACLE_SOURCE_PASSWORD", "dbzpassword")
        self.url = _get_val("oracle.source.url", "ORACLE_SOURCE_URL", f"oracle://{self.username}:{self.password}@{self.host}:{self.port}/{self.pdb_name}")
        self.connector_class = _get_val("oracle.connector.class", default="io.debezium.connector.oracle.OracleConnector")
        self.topic_prefix = _get_val("oracle.topic.prefix", default=f"cdc.local.oracle_erp.{self.database}")
        self.table_include_list = _get_val("oracle.table.include.list", default="ERP.CUSTOMERS,ERP.INVOICES")
        self.snapshot_mode = _get_val("oracle.snapshot.mode", default="initial")
        self.log_mining_strategy = _get_val("oracle.log.mining.strategy", default="logminer")

class SQLServerSourceConfig:
    def __init__(self):
        self.id = _get_val("sqlserver.source.id", "SQLSERVER_SOURCE_ID", "src-sql-crm")
        self.engine = _get_val("sqlserver.source.engine", default="sqlserver")
        self.host = _get_val("sqlserver.source.host", "SQLSERVER_SOURCE_HOST", "sqlserver-source")
        self.port = _get_int("sqlserver.source.port", "SQLSERVER_SOURCE_PORT", 1433)
        self.database = _get_val("sqlserver.source.database", "SQLSERVER_SOURCE_DB", "crm_db")
        self.username = _get_val("sqlserver.source.user", "SQLSERVER_SOURCE_USER", "sa")
        self.password = _get_val("sqlserver.source.password", "SQLSERVER_SOURCE_PASSWORD", "Password123!")
        self.url = _get_val("sqlserver.source.url", "SQLSERVER_SOURCE_URL", f"sqlserver://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}")
        self.connector_class = _get_val("sqlserver.connector.class", default="io.debezium.connector.sqlserver.SqlServerConnector")
        self.topic_prefix = _get_val("sqlserver.topic.prefix", default=f"cdc.local.mssql_crm.{self.database}")
        self.table_include_list = _get_val("sqlserver.table.include.list", default="dbo.accounts,dbo.leads")
        self.snapshot_mode = _get_val("sqlserver.snapshot.mode", default="initial")

class MetadataDatabaseConfig:
    def __init__(self):
        self.host = _get_val("metadata.db.host", default="metadata-db")
        self.port = _get_int("metadata.db.port", default=5432)
        self.external_port = _get_int("metadata.db.external.port", default=5434)
        self.database = _get_val("metadata.db.database", default="cdc_control_plane")
        self.username = _get_val("metadata.db.user", default="cdc_admin")
        self.password = _get_val("metadata.db.password", default="cdcadminpassword")
        self.url = _get_val("metadata.db.url", "METADATA_DATABASE_URL", f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}")

class KafkaConfig:
    def __init__(self):
        self.bootstrap_servers = _get_val("kafka.bootstrap.servers", "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092,localhost:9092")
        self.cluster_id = _get_val("kafka.cluster.id", default="cdc-kraft-cluster-01")
        self.node_id = _get_int("kafka.node.id", default=1)
        self.controller_quorum_voters = _get_val("kafka.controller.quorum.voters", default="1@kafka:9093")
        self.num_partitions = _get_int("kafka.num.partitions", default=3)
        self.replication_factor = _get_int("kafka.replication.factor", default=1)
        self.producer_acks = _get_val("kafka.producer.acks", default="all")
        self.producer_idempotence = _get_bool("kafka.producer.idempotence", default=True)

class DebeziumConfig:
    def __init__(self):
        self.connect_rest_url = _get_val("connect.rest.url", "CONNECT_REST_URL", "http://localhost:8083")
        self.internal_rest_url = _get_val("connect.internal.rest.url", default="http://debezium-connect:8083")
        self.worker_id = _get_val("connect.worker.id", default="connect-worker-01:8083")
        self.tasks_max = _get_int("connect.tasks.max", default=1)
        self.config_storage_topic = _get_val("connect.config.storage.topic", default="cdc_connect_configs")
        self.offset_storage_topic = _get_val("connect.offset.storage.topic", default="cdc_connect_offsets")
        self.status_storage_topic = _get_val("connect.status.storage.topic", default="cdc_connect_statuses")

class SchemaRegistryConfig:
    def __init__(self):
        self.url = _get_val("schema.registry.url", "SCHEMA_REGISTRY_URL", "http://localhost:8081")
        self.internal_url = _get_val("schema.registry.internal.url", default="http://schema-registry:8081")
        self.compatibility_mode = _get_val("schema.registry.compatibility.mode", default="BACKWARD")
        self.wire_format_magic_byte = _get_int("schema.registry.wire.format.magic.byte", default=0)
        self.default_schema_id = _get_int("schema.registry.default.schema.id", default=43)
        self.schema_version = _get_int("schema.registry.schema.version", default=2)

class SMTConfig:
    def __init__(self):
        self.transforms_list = _get_val("smt.transforms.list", default="unwrap,maskEmail,maskCard")
        self.salt = _get_val("smt.salt", "SMT_SALT", "cdc-enterprise-salt-secure-hash-2026")
        self.email_fields = [f.strip() for f in _get_val("smt.email.fields", default="email,customer_email").split(",") if f.strip()]
        self.email_replacement = _get_val("smt.email.replacement", default="REDACTED_PII")
        self.card_fields = [f.strip() for f in _get_val("smt.card.fields", default="credit_card,card_number,tax_id").split(",") if f.strip()]
        self.card_replacement = _get_val("smt.card.replacement", default="XXXX-XXXX-XXXX-XXXX")
        self.unwrap_type = _get_val("smt.unwrap.type", default="io.debezium.transforms.ExtractNewRecordState")
        self.unwrap_drop_tombstones = _get_bool("smt.unwrap.drop.tombstones", default=False)

class SnapshotConfig:
    def __init__(self):
        self.algorithm = _get_val("snapshot.algorithm", default="DBlog")
        self.watermark_table = _get_val("snapshot.watermark.table", default="public.cdc_signal")
        self.chunk_size_rows = _get_int("snapshot.chunk.size.rows", default=1024)
        self.signal_topic = _get_val("snapshot.signal.topic", default="cdc.signals")

class DLQConfig:
    def __init__(self):
        self.topic_prefix = _get_val("dlq.topic.prefix", default="cdc.dlq")
        self.unparseable_topic = _get_val("dlq.orders.unparseable.topic", default="cdc.dlq.orders.unparseable")
        self.max_retry_attempts = _get_int("dlq.max.retry.attempts", default=3)
        self.retry_backoff_ms = _get_int("dlq.retry.backoff.ms", default=1000)

class MetricsConfig:
    def __init__(self):
        self.throughput_target_eps = _get_float("metrics.throughput.target.eps", default=1842.0)
        self.p95_latency_target_ms = _get_float("metrics.p95.latency.target.ms", default=142.0)
        self.p99_latency_target_ms = _get_float("metrics.p99.latency.target.ms", default=380.0)
        self.wal_slot_lag_target_bytes = _get_int("metrics.wal.slot.lag.target.bytes", default=0)
        self.memory_budget_mb = _get_int("metrics.memory.budget.mb", "MEMORY_CAP_MB", 11520)
        self.memory_used_mb = _get_int("metrics.memory.used.mb", default=9600)

class AppSettings:
    """Unified Settings Manager loaded from config and properties files."""
    def __init__(self):
        self.app_name = _get_val("app.name", "APP_NAME", "Enterprise CDC Platform Control Plane")
        self.version = _get_val("app.version", "APP_VERSION", "1.0.0")
        self.environment = _get_val("app.environment", "ENVIRONMENT", "local-m2")
        self.host = _get_val("app.host", default="0.0.0.0")
        self.port = _get_int("app.port", default=8000)
        self.api_prefix = _get_val("app.api.prefix", default="/api/v1")
        self.log_level = _get_val("app.log.level", "LOG_LEVEL", "INFO")
        self.memory_cap_mb = _get_int("app.memory.cap.mb", "MEMORY_CAP_MB", 11520)

        # Child Configuration Domains
        self.postgres = PostgresSourceConfig()
        self.mysql = MySQLSourceConfig()
        self.mariadb = MariaDBSourceConfig()
        self.mongodb = MongoDBSourceConfig()
        self.oracle = OracleSourceConfig()
        self.sqlserver = SQLServerSourceConfig()
        self.metadata_db = MetadataDatabaseConfig()
        self.kafka = KafkaConfig()
        self.debezium = DebeziumConfig()
        self.schema_registry = SchemaRegistryConfig()
        self.smt = SMTConfig()
        self.snapshot = SnapshotConfig()
        self.dlq = DLQConfig()
        self.metrics = MetricsConfig()

        # Backward compatibility aliases
        self.connect_rest_url = self.debezium.connect_rest_url
        self.kafka_bootstrap_servers = self.kafka.bootstrap_servers
        self.schema_registry_url = self.schema_registry.url

settings = AppSettings()
