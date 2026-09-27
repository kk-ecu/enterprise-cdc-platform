"""
Enterprise CDC Platform: Service Interfaces & Protocols
Defines fine-grained interfaces adhering to Interface Segregation & Dependency Inversion.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from backend.app.domain.models import (
    SourceRegistration,
    ConnectorConfig,
    IncrementalSnapshotRequest,
    GoldenSignalsMetrics,
    DatabaseType,
    OperationType,
    CDCCanonicalEvent
)

class IPrerequisiteChecker(ABC):
    """Checks database engine prerequisites (WAL level, binlog format, etc.)"""
    @abstractmethod
    async def verify_prerequisites(self, source: SourceRegistration) -> Dict[str, Any]:
        pass

class IConnectorStrategy(ABC):
    """Open/Closed & Liskov Substitution: Strategy for generating engine-specific Debezium JSON config"""
    @property
    @abstractmethod
    def supported_type(self) -> DatabaseType:
        pass

    @abstractmethod
    def build_debezium_config(self, config: ConnectorConfig) -> Dict[str, Any]:
        pass

class IConnectorManager(ABC):
    """Manages connector lifecycle on Debezium Kafka Connect"""
    @abstractmethod
    async def list_connectors(self) -> List[str]:
        pass

    @abstractmethod
    async def create_or_update_connector(self, config: ConnectorConfig) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def pause_connector(self, name: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def resume_connector(self, name: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def restart_connector(self, name: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def get_connector_status(self, name: str) -> Dict[str, Any]:
        pass

class ISnapshotCoordinator(ABC):
    """Coordinates DBlog watermark-based incremental snapshots without table locks"""
    @abstractmethod
    async def trigger_incremental_snapshot(self, req: IncrementalSnapshotRequest) -> Dict[str, Any]:
        pass

class ISMTTransformer(ABC):
    """Applies in-flight Single Message Transforms (PII masking, routing, key extraction)"""
    @abstractmethod
    def transform(self, record: Dict[str, Any]) -> Dict[str, Any]:
        pass

class ISchemaRegistryValidator(ABC):
    """Governs schema evolution, schema ID assignment, and backward compatibility"""
    @abstractmethod
    async def validate_schema_evolution(self, subject: str, new_schema: Dict[str, Any]) -> Dict[str, Any]:
        pass

class IMetricsProvider(ABC):
    """Collects golden signals (latency, throughput, replication lag, memory consumption)"""
    @abstractmethod
    async def collect_golden_signals(self) -> GoldenSignalsMetrics:
        pass
