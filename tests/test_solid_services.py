"""
SOLID Principles & Service Layer Unit Tests
Validates:
- Single Responsibility & Open/Closed in ConnectorStrategyRegistry
- Liskov Substitution across all 5 database connector strategies
- Interface Segregation in SMT, Prerequisite, and Snapshot services
- Dependency Inversion via dependency injection
"""

import unittest
import asyncio
from backend.app.domain.models import DatabaseType, ConnectorConfig, IncrementalSnapshotRequest
from backend.app.services.connectors.strategies import (
    ConnectorStrategyRegistry,
    PostgresConnectorStrategy,
    MySQLConnectorStrategy,
    MongoDBConnectorStrategy,
    OracleConnectorStrategy,
    SQLServerConnectorStrategy
)
from backend.app.services.smt_service import SMTTransformer
from backend.app.services.prerequisite_service import DatabasePrerequisiteChecker
from backend.app.services.snapshot_service import DBlogSnapshotCoordinator
from backend.app.services.schema_registry_service import SchemaRegistryValidator
from backend.app.services.metrics_service import GoldenSignalsMetricsService

class TestSOLIDServices(unittest.TestCase):
    def setUp(self):
        self.registry = ConnectorStrategyRegistry()
        self.smt = SMTTransformer()
        self.prereqs = DatabasePrerequisiteChecker()
        self.snapshots = DBlogSnapshotCoordinator()
        self.schema_validator = SchemaRegistryValidator()
        self.metrics_svc = GoldenSignalsMetricsService()

    def test_open_closed_and_liskov_connector_strategies(self):
        """Verify all connector strategies conform to IConnectorStrategy (Liskov Substitution)"""
        strategies = [
            (DatabaseType.POSTGRES, "io.debezium.connector.postgresql.PostgresConnector"),
            (DatabaseType.MYSQL, "io.debezium.connector.mysql.MySqlConnector"),
            (DatabaseType.MONGODB, "io.debezium.connector.mongodb.MongoDbConnector"),
            (DatabaseType.ORACLE, "io.debezium.connector.oracle.OracleConnector"),
            (DatabaseType.SQLSERVER, "io.debezium.connector.sqlserver.SqlServerConnector"),
        ]

        for db_type, expected_class in strategies:
            strategy = self.registry.get_strategy(db_type)
            self.assertEqual(strategy.supported_type, db_type)
            config = ConnectorConfig(
                name=f"test-{db_type.value}-connector",
                source_type=db_type,
                database="testdb",
                table_include_list="test_table",
                snapshot_mode="initial",
                mask_pii=True
            )
            built = strategy.build_debezium_config(config)
            self.assertEqual(built["connector.class"], expected_class)
            self.assertIn("topic.prefix", built)

    def test_smt_pii_masking_and_diff_calculation(self):
        """Verify SMT transformation hashes PII, redacts cards, and computes diffs"""
        record = {
            "before": {"id": 100, "status": "PENDING", "amount": 500},
            "after": {
                "id": 100,
                "status": "COMPLETED",
                "amount": 750,
                "customer_email": "alice@corp.internal",
                "credit_card": "4111999988881234"
            }
        }
        transformed = self.smt.transform(record)
        after = transformed["after"]
        # PII email hashed
        self.assertNotEqual(after["customer_email"], "alice@corp.internal")
        self.assertEqual(len(after["customer_email"]), 16)
        # Credit card redacted with last 4
        self.assertEqual(after["credit_card"], "XXXX-XXXX-XXXX-1234")
        # Diff calculated
        self.assertIn("status", transformed["diff_fields"])
        self.assertIn("amount", transformed["diff_fields"])

    def test_schema_registry_wire_header_format(self):
        """Verify Confluent Schema Registry 5-byte magic wire header format"""
        header = self.smt.generate_schema_registry_wire_header(42)
        self.assertEqual(len(header), 5)
        self.assertEqual(header[0], 0x00) # Magic byte
        schema_id = int.from_bytes(header[1:5], byteorder="big")
        self.assertEqual(schema_id, 42)

    def test_dblog_incremental_snapshot_signal(self):
        """Verify DBlog snapshot coordinator generates valid watermark signal"""
        req = IncrementalSnapshotRequest(
            connector_name="postgres-orders-connector",
            collection="public.orders",
            additional_condition="amount_cents > 1000"
        )
        res = asyncio.run(self.snapshots.trigger_incremental_snapshot(req))
        self.assertEqual(res["status"], "SIGNAL_DISPATCHED")
        self.assertEqual(res["target_connector"], "postgres-orders-connector")
        self.assertEqual(res["watermark_table"], "public.cdc_signal")
        self.assertIn("sig_", res["signal_id"])

    def test_schema_evolution_backward_compatibility(self):
        """Verify Schema Registry validator detects breaking vs non-breaking changes"""
        # Safe addition
        safe_diff = {"added_fields": [{"name": "loyalty_score", "type": "int", "default": 0}]}
        res_safe = asyncio.run(self.schema_validator.validate_schema_evolution("orders-value", safe_diff))
        self.assertTrue(res_safe["compatible"])
        self.assertEqual(res_safe["policy_action"], "AUTO_REGISTER")

        # Breaking drop column without default
        breaking_diff = {"dropped_fields": ["required_tenant_id"]}
        res_breaking = asyncio.run(self.schema_validator.validate_schema_evolution("orders-value", breaking_diff))
        self.assertFalse(res_breaking["compatible"])
        self.assertEqual(res_breaking["policy_action"], "QUARANTINE_DLQ")

    def test_metrics_service_golden_signals(self):
        """Verify golden signals SLO collection respects 11.5 GB memory cap"""
        metrics = asyncio.run(self.metrics_svc.collect_golden_signals())
        self.assertGreater(metrics.throughput_eps, 0)
        self.assertLess(metrics.p95_latency_ms, 1000) # sub-second
        self.assertLessEqual(metrics.memory_used_mb, metrics.memory_budget_mb)
        self.assertEqual(metrics.memory_budget_mb, 11520) # M2 Mac Pro budget

if __name__ == "__main__":
    unittest.main()
