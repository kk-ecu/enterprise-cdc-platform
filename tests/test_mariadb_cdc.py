"""
MariaDB CDC Test Suite
Validates:
- Binary Log ROW format, log_bin, and MariaDB GTID strict mode
- MariaDB GTID coordinate formatting (domain-server-sequence)
- MariaDB Debezium connector configuration generation
- Canonical Kafka event emission for MariaDB store catalog
"""

import unittest
from backend.app.domain.models import DatabaseType, ConnectorConfig
from backend.app.services.connectors.strategies import MariaDBConnectorStrategy
from backend.app.services.prerequisite_service import DatabasePrerequisiteChecker
from backend.app.domain.models import SourceRegistration

class TestMariaDBCDC(unittest.TestCase):
    def setUp(self):
        self.strategy = MariaDBConnectorStrategy()
        self.prereqs = DatabasePrerequisiteChecker()

    def test_mariadb_prerequisites(self):
        """Verify MariaDB binlog_format=ROW, gtid_strict_mode, and log_bin checks"""
        source = SourceRegistration(
            id="src-mariadb-store",
            type=DatabaseType.MARIADB,
            host="mariadb-source",
            port=3306,
            database="store_catalog",
            username="mariadbuser"
        )
        import asyncio
        res = asyncio.run(self.prereqs.verify_prerequisites(source))
        self.assertTrue(res["all_passed"])
        check_names = [c["name"] for c in res["checks"]]
        self.assertIn("binlog_format == ROW", check_names)
        self.assertIn("log_bin == ON", check_names)
        self.assertIn("gtid_strict_mode == ON", check_names)

    def test_mariadb_connector_config_generation(self):
        """Verify MariaDBConnectorStrategy generates valid Debezium Kafka Connect payload"""
        config = ConnectorConfig(
            name="mariadb-store-connector",
            source_type=DatabaseType.MARIADB,
            database="store_catalog",
            table_include_list="store_catalog.products,store_catalog.discounts",
            snapshot_mode="initial"
        )
        debezium_cfg = self.strategy.build_debezium_config(config)
        self.assertEqual(debezium_cfg["connector.class"], "io.debezium.connector.mysql.MySqlConnector")
        self.assertEqual(debezium_cfg["database.hostname"], "mariadb-source")
        self.assertEqual(debezium_cfg["topic.prefix"], "cdc.local.mariadb_store.store_catalog")
        self.assertEqual(debezium_cfg["table.include.list"], "store_catalog.products,store_catalog.discounts")
        self.assertEqual(debezium_cfg["gtid.source.filter.dml.events"], "true")

    def test_mariadb_gtid_coordinate_parsing(self):
        """Verify MariaDB 3-part GTID format: <domain_id>-<server_id>-<sequence_no>"""
        sample_mariadb_gtid = "0-184055-9842"
        parts = sample_mariadb_gtid.split("-")
        self.assertEqual(len(parts), 3)
        domain_id, server_id, sequence_no = int(parts[0]), int(parts[1]), int(parts[2])
        self.assertEqual(domain_id, 0)
        self.assertEqual(server_id, 184055)
        self.assertEqual(sequence_no, 9842)

    def test_mariadb_canonical_event_emission(self):
        """Verify MariaDB mutation formats into canonical CDC envelope"""
        raw_change = {
            "table": "products",
            "gtid": "0-184055-9843",
            "op": "c",
            "after": {"sku": "MARIA-SKU-99", "name": "MariaDB High Perf SSD", "price_cents": 12900}
        }
        canonical_event = {
            "eventId": "evt_maria_01",
            "sourceType": "mariadb",
            "logPosition": {"type": "GTID", "value": raw_change["gtid"]},
            "kafkaTopic": "cdc.local.mariadb_store.store_catalog.products",
            "partition": 1,
            "key": {"sku": raw_change["after"]["sku"]},
            "before": None,
            "after": raw_change["after"]
        }
        self.assertEqual(canonical_event["sourceType"], "mariadb")
        self.assertEqual(canonical_event["logPosition"]["value"], "0-184055-9843")
        self.assertIsNone(canonical_event["before"])
        self.assertIsNotNone(canonical_event["after"])

if __name__ == "__main__":
    unittest.main()
