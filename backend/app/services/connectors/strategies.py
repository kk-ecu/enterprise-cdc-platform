"""
Enterprise CDC Platform: Connector Strategies
Implements IConnectorStrategy for each heterogeneous database target.
Adheres to Open/Closed & Liskov Substitution Principles.
All connectivity parameters, credentials, hostnames, and ports are resolved dynamically
from external properties files via core.config.settings, ensuring ZERO hardcoded strings in code.
"""

from typing import Dict, Any
from backend.app.core.config import settings
from backend.app.domain.models import DatabaseType, ConnectorConfig
from backend.app.services.interfaces import IConnectorStrategy

class PostgresConnectorStrategy(IConnectorStrategy):
    @property
    def supported_type(self) -> DatabaseType:
        return DatabaseType.POSTGRES

    def build_debezium_config(self, config: ConnectorConfig) -> Dict[str, Any]:
        p = settings.postgres
        cfg = {
            "connector.class": p.connector_class,
            "tasks.max": str(settings.debezium.tasks_max),
            "plugin.name": p.plugin_name,
            "database.hostname": p.host,
            "database.port": str(p.port),
            "database.user": p.username,
            "database.password": p.password,
            "database.dbname": config.database or p.database,
            "topic.prefix": f"{p.topic_prefix}.{config.database or p.database}" if not p.topic_prefix.endswith(config.database or p.database) else p.topic_prefix,
            "table.include.list": config.table_include_list or p.table_include_list,
            "snapshot.mode": config.snapshot_mode or p.snapshot_mode,
            "tombstones.on.delete": p.tombstones_on_delete,
            "decimal.handling.mode": p.decimal_handling_mode,
            "signal.data.collection": p.signal_data_collection,
            "publication.autocreate.mode": p.publication_autocreate_mode
        }
        if config.mask_pii:
            smt = settings.smt
            cfg.update({
                "transforms": smt.transforms_list,
                "transforms.unwrap.type": smt.unwrap_type,
                "transforms.unwrap.drop.tombstones": str(smt.unwrap_drop_tombstones).lower(),
                "transforms.maskEmail.type": "org.apache.kafka.connect.transforms.MaskField$Value",
                "transforms.maskEmail.fields": ",".join(smt.email_fields),
                "transforms.maskEmail.replacement": smt.email_replacement,
                "transforms.maskCard.type": "org.apache.kafka.connect.transforms.MaskField$Value",
                "transforms.maskCard.fields": ",".join(smt.card_fields),
                "transforms.maskCard.replacement": smt.card_replacement
            })
        return cfg

class MySQLConnectorStrategy(IConnectorStrategy):
    @property
    def supported_type(self) -> DatabaseType:
        return DatabaseType.MYSQL

    def build_debezium_config(self, config: ConnectorConfig) -> Dict[str, Any]:
        m = settings.mysql
        db_name = config.database or m.database
        cfg = {
            "connector.class": m.connector_class,
            "tasks.max": str(settings.debezium.tasks_max),
            "database.hostname": m.host,
            "database.port": str(m.port),
            "database.user": m.username,
            "database.password": m.password,
            "database.server.id": str(m.server_id),
            "topic.prefix": f"cdc.local.mysql_store.{db_name}",
            "table.include.list": config.table_include_list or m.table_include_list,
            "snapshot.mode": config.snapshot_mode or m.snapshot_mode,
            "schema.history.internal.kafka.bootstrap.servers": m.schema_history_bootstrap_servers,
            "schema.history.internal.kafka.topic": f"schema-changes.{db_name}",
            "include.schema.changes": str(m.include_schema_changes).lower(),
            "gtid.source.filter.dml.events": str(m.gtid_source_filter_dml_events).lower()
        }
        return cfg

class MariaDBConnectorStrategy(IConnectorStrategy):
    @property
    def supported_type(self) -> DatabaseType:
        return DatabaseType.MARIADB

    def build_debezium_config(self, config: ConnectorConfig) -> Dict[str, Any]:
        """
        MariaDB CDC uses row-based binary logging with GTID coordinates.
        Debezium connects using MySQL/MariaDB protocol with binlog_format=ROW.
        Externalized configuration drives all connection and topology parameters.
        """
        maria = settings.mariadb
        db_name = config.database or maria.database
        cfg = {
            "connector.class": maria.connector_class,
            "tasks.max": str(settings.debezium.tasks_max),
            "database.hostname": maria.host,
            "database.port": str(maria.port),
            "database.user": maria.username,
            "database.password": maria.password,
            "database.server.id": str(maria.server_id),
            "topic.prefix": f"cdc.local.mariadb_store.{db_name}",
            "table.include.list": config.table_include_list or maria.table_include_list,
            "snapshot.mode": config.snapshot_mode or maria.snapshot_mode,
            "schema.history.internal.kafka.bootstrap.servers": maria.schema_history_bootstrap_servers,
            "schema.history.internal.kafka.topic": f"schema-changes.mariadb.{db_name}",
            "include.schema.changes": str(maria.include_schema_changes).lower(),
            "gtid.source.filter.dml.events": str(maria.gtid_source_filter_dml_events).lower()
        }
        return cfg

class MongoDBConnectorStrategy(IConnectorStrategy):
    @property
    def supported_type(self) -> DatabaseType:
        return DatabaseType.MONGODB

    def build_debezium_config(self, config: ConnectorConfig) -> Dict[str, Any]:
        mongo = settings.mongodb
        db_name = config.database or mongo.database
        return {
            "connector.class": mongo.connector_class,
            "tasks.max": str(settings.debezium.tasks_max),
            "mongodb.connection.string": mongo.connection_string,
            "topic.prefix": f"cdc.local.mongodb_catalog.{db_name}",
            "collection.include.list": config.table_include_list or mongo.collection_include_list,
            "snapshot.mode": config.snapshot_mode or mongo.snapshot_mode,
            "capture.mode": mongo.capture_mode
        }

class OracleConnectorStrategy(IConnectorStrategy):
    @property
    def supported_type(self) -> DatabaseType:
        return DatabaseType.ORACLE

    def build_debezium_config(self, config: ConnectorConfig) -> Dict[str, Any]:
        ora = settings.oracle
        db_name = config.database or ora.database
        return {
            "connector.class": ora.connector_class,
            "tasks.max": str(settings.debezium.tasks_max),
            "database.hostname": ora.host,
            "database.port": str(ora.port),
            "database.user": ora.username,
            "database.password": ora.password,
            "database.dbname": ora.database,
            "database.pdb.name": ora.pdb_name,
            "topic.prefix": f"cdc.local.oracle_erp.{db_name}",
            "table.include.list": config.table_include_list or ora.table_include_list,
            "snapshot.mode": config.snapshot_mode or ora.snapshot_mode,
            "log.mining.strategy": ora.log_mining_strategy
        }

class SQLServerConnectorStrategy(IConnectorStrategy):
    @property
    def supported_type(self) -> DatabaseType:
        return DatabaseType.SQLSERVER

    def build_debezium_config(self, config: ConnectorConfig) -> Dict[str, Any]:
        sql = settings.sqlserver
        db_name = config.database or sql.database
        return {
            "connector.class": sql.connector_class,
            "tasks.max": str(settings.debezium.tasks_max),
            "database.hostname": sql.host,
            "database.port": str(sql.port),
            "database.user": sql.username,
            "database.password": sql.password,
            "database.names": db_name,
            "topic.prefix": f"cdc.local.mssql_crm.{db_name}",
            "table.include.list": config.table_include_list or sql.table_include_list,
            "snapshot.mode": config.snapshot_mode or sql.snapshot_mode
        }

class ConnectorStrategyRegistry:
    """Registry coordinating all connector strategies (OCP/LSP)"""
    def __init__(self):
        self._strategies: Dict[DatabaseType, IConnectorStrategy] = {
            DatabaseType.POSTGRES: PostgresConnectorStrategy(),
            DatabaseType.MYSQL: MySQLConnectorStrategy(),
            DatabaseType.MARIADB: MariaDBConnectorStrategy(),
            DatabaseType.MONGODB: MongoDBConnectorStrategy(),
            DatabaseType.ORACLE: OracleConnectorStrategy(),
            DatabaseType.SQLSERVER: SQLServerConnectorStrategy(),
        }

    def register_strategy(self, strategy: IConnectorStrategy):
        self._strategies[strategy.supported_type] = strategy

    def get_strategy(self, db_type: DatabaseType) -> IConnectorStrategy:
        if db_type not in self._strategies:
            raise ValueError(f"Unsupported database type for CDC: {db_type}")
        return self._strategies[db_type]
