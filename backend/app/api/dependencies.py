"""
Enterprise CDC Platform: FastAPI Dependencies
Wires Dependency Inversion Principle (DIP) via Dependency Injection.
"""

from backend.app.services.interfaces import (
    IPrerequisiteChecker,
    IConnectorManager,
    ISnapshotCoordinator,
    ISMTTransformer,
    ISchemaRegistryValidator,
    IMetricsProvider
)
from backend.app.services.prerequisite_service import DatabasePrerequisiteChecker
from backend.app.services.connector_manager import DebeziumConnectorManager
from backend.app.services.snapshot_service import DBlogSnapshotCoordinator
from backend.app.services.smt_service import SMTTransformer
from backend.app.services.schema_registry_service import SchemaRegistryValidator
from backend.app.services.metrics_service import GoldenSignalsMetricsService

# Singletons / Factories
_prerequisite_checker = DatabasePrerequisiteChecker()
_connector_manager = DebeziumConnectorManager()
_snapshot_coordinator = DBlogSnapshotCoordinator()
_smt_transformer = SMTTransformer()
_schema_validator = SchemaRegistryValidator()
_metrics_provider = GoldenSignalsMetricsService()

def get_prerequisite_checker() -> IPrerequisiteChecker:
    return _prerequisite_checker

def get_connector_manager() -> IConnectorManager:
    return _connector_manager

def get_snapshot_coordinator() -> ISnapshotCoordinator:
    return _snapshot_coordinator

def get_smt_transformer() -> ISMTTransformer:
    return _smt_transformer

def get_schema_validator() -> ISchemaRegistryValidator:
    return _schema_validator

def get_metrics_provider() -> IMetricsProvider:
    return _metrics_provider
