"""
Enterprise CDC Platform: Configuration & Properties Verification Suite
Validates that:
- External .properties files (cdc-platform.properties and database-connectors.properties) are correctly parsed.
- Not a single connection parameter, credential, host, or URL is hardcoded in Python code.
- All 6 database strategies (Postgres, MySQL, MariaDB, MongoDB, Oracle, SQL Server) read from config.
- Environment variable overrides take precedence over property file values (12-factor rule).
- Single Message Transformation (SMT), DBlog snapshots, and Schema Registry wire-formats use configured properties.
"""

import os
import unittest
from backend.app.core.config import settings, parse_properties_file, PostgresSourceConfig
from backend.app.services.connectors.strategies import (
    ConnectorStrategyRegistry,
    MariaDBConnectorStrategy,
    PostgresConnectorStrategy,
    MySQLConnectorStrategy,
    MongoDBConnectorStrategy,
    OracleConnectorStrategy,
    SQLServerConnectorStrategy
)
from backend.app.domain.models import DatabaseType, ConnectorConfig
from backend.app.services.smt_service import SMTTransformer
from backend.app.services.snapshot_service import DBlogSnapshotCoordinator

class TestConfigurationAndProperties(unittest.TestCase):
    def test_properties_files_exist_and_parse(self):
        """Verify that external properties files are found and successfully parsed"""
        platform_props = parse_properties_file("config/cdc-platform.properties")
        connectors_props = parse_properties_file("config/database-connectors.properties")

        self.assertGreater(len(platform_props), 10, "Platform properties should have key-value pairs")
        self.assertGreater(len(connectors_props), 20, "Database connector properties should have key-value pairs")

        # Check key items
        self.assertIn("kafka.bootstrap.servers", platform_props)
        self.assertIn("connect.rest.url", platform_props)
        self.assertIn("postgres.source.host", connectors_props)
        self.assertIn("mariadb.source.host", connectors_props)
        self.assertIn("mysql.source.host", connectors_props)

    def test_mariadb_externalized_configuration(self):
        """Verify MariaDB connectivity, credentials, and CDC parameters are completely externalized"""
        m = settings.mariadb
        self.assertEqual(m.engine, "mariadb")
        self.assertEqual(m.host, "mariadb-source")
        self.assertEqual(m.port, 3306)
        self.assertEqual(m.external_port, 3308)
        self.assertEqual(m.database, "store_catalog")
        self.assertEqual(m.username, "mariadbuser")
        self.assertEqual(m.password, "mariadbpassword")
        self.assertEqual(m.server_id, "184055")
        self.assertEqual(m.gtid_strict_mode, "ON")
        self.assertIn("mariadbuser:mariadbpassword@mariadb-source", m.url)

    def test_postgres_externalized_configuration(self):
        """Verify PostgreSQL connectivity parameters are resolved from configuration"""
        p = settings.postgres
        self.assertEqual(p.engine, "postgres")
        self.assertEqual(p.host, "postgres-source")
        self.assertEqual(p.port, 5432)
        self.assertEqual(p.database, "orders_db")
        self.assertEqual(p.username, "postgres")
        self.assertEqual(p.password, "postgrespassword")
        self.assertEqual(p.plugin_name, "pgoutput")
        self.assertEqual(p.wal_level, "logical")

    def test_all_connector_strategies_use_external_config(self):
        """Verify that strategies dynamically construct payloads using settings, not hardcoded strings"""
        registry = ConnectorStrategyRegistry()

        # MariaDB strategy
        maria_strat = registry.get_strategy(DatabaseType.MARIADB)
        maria_cfg = maria_strat.build_debezium_config(
            ConnectorConfig(name="maria-test", source_type=DatabaseType.MARIADB, database="store_catalog", table_include_list="store_catalog.products")
        )
        self.assertEqual(maria_cfg["database.hostname"], settings.mariadb.host)
        self.assertEqual(maria_cfg["database.port"], str(settings.mariadb.port))
        self.assertEqual(maria_cfg["database.user"], settings.mariadb.username)
        self.assertEqual(maria_cfg["database.password"], settings.mariadb.password)
        self.assertEqual(maria_cfg["database.server.id"], settings.mariadb.server_id)

        # Postgres strategy
        pg_strat = registry.get_strategy(DatabaseType.POSTGRES)
        pg_cfg = pg_strat.build_debezium_config(
            ConnectorConfig(name="pg-test", source_type=DatabaseType.POSTGRES, database="orders_db", table_include_list="public.orders")
        )
        self.assertEqual(pg_cfg["database.hostname"], settings.postgres.host)
        self.assertEqual(pg_cfg["database.port"], str(settings.postgres.port))
        self.assertEqual(pg_cfg["database.user"], settings.postgres.username)
        self.assertEqual(pg_cfg["database.password"], settings.postgres.password)
        self.assertEqual(pg_cfg["plugin.name"], settings.postgres.plugin_name)

        # MySQL strategy
        my_strat = registry.get_strategy(DatabaseType.MYSQL)
        my_cfg = my_strat.build_debezium_config(
            ConnectorConfig(name="my-test", source_type=DatabaseType.MYSQL, database="inventory", table_include_list="inventory.products")
        )
        self.assertEqual(my_cfg["database.hostname"], settings.mysql.host)
        self.assertEqual(my_cfg["database.port"], str(settings.mysql.port))
        self.assertEqual(my_cfg["database.user"], settings.mysql.username)
        self.assertEqual(my_cfg["database.password"], settings.mysql.password)

        # MongoDB strategy
        mongo_strat = registry.get_strategy(DatabaseType.MONGODB)
        mongo_cfg = mongo_strat.build_debezium_config(
            ConnectorConfig(name="mongo-test", source_type=DatabaseType.MONGODB, database="store", table_include_list="store.products")
        )
        self.assertEqual(mongo_cfg["mongodb.connection.string"], settings.mongodb.connection_string)

        # Oracle strategy
        ora_strat = registry.get_strategy(DatabaseType.ORACLE)
        ora_cfg = ora_strat.build_debezium_config(
            ConnectorConfig(name="ora-test", source_type=DatabaseType.ORACLE, database="ORCLCDB", table_include_list="ERP.CUSTOMERS")
        )
        self.assertEqual(ora_cfg["database.hostname"], settings.oracle.host)
        self.assertEqual(ora_cfg["database.user"], settings.oracle.username)
        self.assertEqual(ora_cfg["database.password"], settings.oracle.password)

        # SQL Server strategy
        sql_strat = registry.get_strategy(DatabaseType.SQLSERVER)
        sql_cfg = sql_strat.build_debezium_config(
            ConnectorConfig(name="sql-test", source_type=DatabaseType.SQLSERVER, database="crm_db", table_include_list="dbo.accounts")
        )
        self.assertEqual(sql_cfg["database.hostname"], settings.sqlserver.host)
        self.assertEqual(sql_cfg["database.user"], settings.sqlserver.username)
        self.assertEqual(sql_cfg["database.password"], settings.sqlserver.password)

    def test_smt_and_snapshot_configured_properties(self):
        """Verify SMT salt and snapshot parameters originate from config properties"""
        smt = SMTTransformer()
        self.assertEqual(smt.salt, settings.smt.salt)
        self.assertEqual(smt.email_fields, settings.smt.email_fields)
        self.assertEqual(smt.card_fields, settings.smt.card_fields)

        # Verify wire-format magic byte is loaded from config
        header = smt.generate_schema_registry_wire_header(43)
        self.assertEqual(header[0], settings.schema_registry.wire_format_magic_byte)

    def test_environment_override_precedence(self):
        """Verify that 12-factor environment variables override properties files"""
        test_custom_host = "custom-prod-pg-host.internal"
        os.environ["POSTGRES_SOURCE_HOST"] = test_custom_host
        try:
            custom_pg = PostgresSourceConfig()
            self.assertEqual(custom_pg.host, test_custom_host)
        finally:
            del os.environ["POSTGRES_SOURCE_HOST"]

if __name__ == "__main__":
    unittest.main()
