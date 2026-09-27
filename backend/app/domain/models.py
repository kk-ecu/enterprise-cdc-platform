"""
Enterprise CDC Platform: Domain Models
Defines pure data transfer objects, event envelopes, and configurations.
Adheres to Single Responsibility: Only data validation and schema definitions.
Gracefully supports Pydantic when available, or standard dataclasses.
"""

from typing import List, Dict, Any, Optional
from enum import Enum

try:
    from pydantic import BaseModel, Field
except ImportError:
    # Fallback to standard library dataclass representation if pydantic is not yet installed in host environment
    from dataclasses import dataclass, field

    def Field(default=..., **kwargs):
        if default is ...:
            return field()
        return field(default=default)

    class BaseModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

        def dict(self):
            return self.__dict__

class DatabaseType(str, Enum):
    POSTGRES = "postgres"
    MYSQL = "mysql"
    MARIADB = "mariadb"
    ORACLE = "oracle"
    SQLSERVER = "sqlserver"
    MONGODB = "mongodb"

class OperationType(str, Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    PK_UPDATE = "PK_UPDATE"
    TRUNCATE = "TRUNCATE"
    ROLLBACK = "ROLLBACK"
    DDL_ADD_COL = "DDL_ADD_COL"
    DDL_ALTER_TYPE = "DDL_ALTER_TYPE"
    DDL_DROP_COL = "DDL_DROP_COL"

class SourceRegistration(BaseModel):
    id: str
    type: DatabaseType
    host: str
    port: int
    database: str
    username: str
    environment: str = "local-m2"

    def __init__(self, id: str, type: DatabaseType, host: str, port: int, database: str, username: str, environment: str = "local-m2", **kwargs):
        self.id = id
        self.type = type
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.environment = environment

class ConnectorConfig(BaseModel):
    name: str
    source_type: DatabaseType
    database: str
    table_include_list: str
    snapshot_mode: str = "initial"
    mask_pii: bool = True

    def __init__(self, name: str, source_type: DatabaseType, database: str, table_include_list: str, snapshot_mode: str = "initial", mask_pii: bool = True, **kwargs):
        self.name = name
        self.source_type = source_type
        self.database = database
        self.table_include_list = table_include_list
        self.snapshot_mode = snapshot_mode
        self.mask_pii = mask_pii

class IncrementalSnapshotRequest(BaseModel):
    connector_name: str
    collection: str
    additional_condition: Optional[str] = None

    def __init__(self, connector_name: str, collection: str, additional_condition: Optional[str] = None, **kwargs):
        self.connector_name = connector_name
        self.collection = collection
        self.additional_condition = additional_condition

class GoldenSignalsMetrics(BaseModel):
    timestamp: str
    throughput_eps: float
    p95_latency_ms: float
    p99_latency_ms: float
    wal_slot_lag_bytes: int
    memory_used_mb: int
    memory_budget_mb: int
    dlq_poison_count: int
    active_connectors_count: int

    def __init__(self, timestamp: str, throughput_eps: float, p95_latency_ms: float, p99_latency_ms: float, wal_slot_lag_bytes: int, memory_used_mb: int, memory_budget_mb: int, dlq_poison_count: int, active_connectors_count: int, **kwargs):
        self.timestamp = timestamp
        self.throughput_eps = throughput_eps
        self.p95_latency_ms = p95_latency_ms
        self.p99_latency_ms = p99_latency_ms
        self.wal_slot_lag_bytes = wal_slot_lag_bytes
        self.memory_used_mb = memory_used_mb
        self.memory_budget_mb = memory_budget_mb
        self.dlq_poison_count = dlq_poison_count
        self.active_connectors_count = active_connectors_count

class CDCCanonicalEvent(BaseModel):
    event_id: str
    timestamp: str
    source_type: DatabaseType
    operation: OperationType
    log_position: Dict[str, str]
    topic: str
    partition: int
    offset: int
    schema_id: int
    schema_version: int
    key: Dict[str, Any]
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    diff_fields: List[str] = []
    status: str
