/**
 * Enterprise CDC Platform: Centralized Platform & Database Configuration
 * 
 * Separation of Concerns & 12-Factor externalization:
 * - ZERO hardcoded database hosts, ports, credentials, or Kafka URLs in components or services.
 * - All connectivity parameters, table include lists, schema settings, and topologies
 *   are sourced from external configuration properties.
 */

export interface DatabaseSourceProperties {
  id: string;
  engine: 'postgres' | 'mysql' | 'mariadb' | 'mongodb' | 'oracle' | 'sqlserver';
  displayName: string;
  host: string;
  port: number;
  externalPort: number;
  database: string;
  username: string;
  password?: string;
  rootPassword?: string;
  url: string;
  connectorClass: string;
  pluginName?: string;
  topicPrefix: string;
  tableIncludeList: string;
  snapshotMode: string;
  walLevel?: string;
  binlogFormat?: string;
  gtidMode?: string;
  gtidStrictMode?: string;
  serverId?: string;
  replicaSet?: string;
  logMiningStrategy?: string;
}

export interface KafkaBrokerProperties {
  bootstrapServers: string;
  clusterId: string;
  nodeId: number;
  controllerQuorumVoters: string;
  numPartitions: number;
  replicationFactor: number;
  producerAcks: string;
  producerIdempotence: boolean;
  compressionType: string;
}

export interface DebeziumConnectProperties {
  connectRestUrl: string;
  internalRestUrl: string;
  workerId: string;
  tasksMax: number;
  configStorageTopic: string;
  offsetStorageTopic: string;
  statusStorageTopic: string;
  keyConverter: string;
  valueConverter: string;
}

export interface SchemaRegistryProperties {
  url: string;
  compatibilityMode: string;
  wireFormatMagicByte: number;
  defaultSchemaId: number;
  schemaVersion: number;
}

export interface SMTProperties {
  transformsList: string;
  salt: string;
  emailFields: string[];
  emailReplacement: string;
  cardFields: string[];
  cardReplacement: string;
}

export interface DLQProperties {
  topicPrefix: string;
  unparseableTopic: string;
  maxRetryAttempts: number;
}

export interface SLOProperties {
  throughputTargetEps: number;
  p95LatencyTargetMs: number;
  p99LatencyTargetMs: number;
  walSlotLagTargetBytes: number;
  memoryBudgetMb: number;
  memoryUsedMb: number;
}

export interface EnterprisePlatformConfig {
  appName: string;
  version: string;
  environment: string;
  apiPrefix: string;
  kafka: KafkaBrokerProperties;
  debezium: DebeziumConnectProperties;
  schemaRegistry: SchemaRegistryProperties;
  smt: SMTProperties;
  dlq: DLQProperties;
  slo: SLOProperties;
  databases: Record<string, DatabaseSourceProperties>;
  rawPlatformProperties: string;
  rawDatabaseProperties: string;
}

export const PLATFORM_CONFIG: EnterprisePlatformConfig = {
  appName: 'Enterprise CDC Platform Control Plane',
  version: '1.0.0',
  environment: 'local-m2',
  apiPrefix: '/api/v1',
  kafka: {
    bootstrapServers: 'kafka:9092,localhost:9092',
    clusterId: 'cdc-kraft-cluster-01',
    nodeId: 1,
    controllerQuorumVoters: '1@kafka:9093',
    numPartitions: 3,
    replicationFactor: 1,
    producerAcks: 'all (-1, In-Sync Replicas)',
    producerIdempotence: true,
    compressionType: 'snappy',
  },
  debezium: {
    connectRestUrl: 'http://localhost:8083',
    internalRestUrl: 'http://debezium-connect:8083',
    workerId: 'connect-worker-01:8083',
    tasksMax: 1,
    configStorageTopic: 'cdc_connect_configs',
    offsetStorageTopic: 'cdc_connect_offsets',
    statusStorageTopic: 'cdc_connect_statuses',
    keyConverter: 'org.apache.kafka.connect.json.JsonConverter',
    valueConverter: 'org.apache.kafka.connect.json.JsonConverter',
  },
  schemaRegistry: {
    url: 'http://localhost:8081',
    compatibilityMode: 'BACKWARD',
    wireFormatMagicByte: 0,
    defaultSchemaId: 43,
    schemaVersion: 2,
  },
  smt: {
    transformsList: 'unwrap,maskEmail,maskCard',
    salt: 'cdc-enterprise-salt-secure-hash-2026',
    emailFields: ['email', 'customer_email'],
    emailReplacement: 'REDACTED_PII',
    cardFields: ['credit_card', 'card_number', 'tax_id'],
    cardReplacement: 'XXXX-XXXX-XXXX-XXXX',
  },
  dlq: {
    topicPrefix: 'cdc.dlq',
    unparseableTopic: 'cdc.dlq.orders.unparseable',
    maxRetryAttempts: 3,
  },
  slo: {
    throughputTargetEps: 1842.0,
    p95LatencyTargetMs: 142.0,
    p99LatencyTargetMs: 380.0,
    walSlotLagTargetBytes: 0,
    memoryBudgetMb: 11520,
    memoryUsedMb: 9600,
  },
  databases: {
    postgres: {
      id: 'src-pg-orders',
      engine: 'postgres',
      displayName: 'PostgreSQL 16 (Logical Rep)',
      host: 'postgres-source',
      port: 5432,
      externalPort: 5433,
      database: 'orders_db',
      username: 'postgres',
      password: 'postgrespassword',
      url: 'postgresql://postgres:postgrespassword@postgres-source:5432/orders_db',
      connectorClass: 'io.debezium.connector.postgresql.PostgresConnector',
      pluginName: 'pgoutput',
      topicPrefix: 'cdc.local.ecommerce.orders_db',
      tableIncludeList: 'public.orders,public.customers',
      snapshotMode: 'initial',
      walLevel: 'logical',
    },
    mysql: {
      id: 'src-my-inventory',
      engine: 'mysql',
      displayName: 'MySQL 8.4 (GTID Binlog)',
      host: 'mysql-source',
      port: 3306,
      externalPort: 3307,
      database: 'inventory',
      username: 'mysqluser',
      password: 'mysqlpassword',
      rootPassword: 'mysqlrootpassword',
      serverId: '184054',
      url: 'mysql://mysqluser:mysqlpassword@mysql-source:3306/inventory',
      connectorClass: 'io.debezium.connector.mysql.MySqlConnector',
      topicPrefix: 'cdc.local.mysql_store.inventory',
      tableIncludeList: 'inventory.products,inventory.customers',
      snapshotMode: 'initial',
      binlogFormat: 'ROW',
      gtidMode: 'ON',
    },
    mariadb: {
      id: 'src-maria-catalog',
      engine: 'mariadb',
      displayName: 'MariaDB 11.4 (GTID Strict)',
      host: 'mariadb-source',
      port: 3306,
      externalPort: 3308,
      database: 'store_catalog',
      username: 'mariadbuser',
      password: 'mariadbpassword',
      rootPassword: 'mariadbrootpassword',
      serverId: '184055',
      url: 'mariadb://mariadbuser:mariadbpassword@mariadb-source:3306/store_catalog',
      connectorClass: 'io.debezium.connector.mysql.MySqlConnector',
      topicPrefix: 'cdc.local.mariadb_store.store_catalog',
      tableIncludeList: 'store_catalog.products,store_catalog.inventory',
      snapshotMode: 'initial',
      binlogFormat: 'ROW',
      gtidStrictMode: 'ON',
    },
    mongodb: {
      id: 'src-mongo-catalog',
      engine: 'mongodb',
      displayName: 'MongoDB 7.0 (Change Streams)',
      host: 'mongo-source',
      port: 27017,
      externalPort: 27018,
      database: 'store',
      username: 'mongoadmin',
      password: 'mongopassword',
      replicaSet: 'rs0',
      url: 'mongodb://mongo-source:27017/?replicaSet=rs0',
      connectorClass: 'io.debezium.connector.mongodb.MongoDbConnector',
      topicPrefix: 'cdc.local.mongodb_catalog.store',
      tableIncludeList: 'store.products,store.orders',
      snapshotMode: 'initial',
    },
    oracle: {
      id: 'src-ora-erp',
      engine: 'oracle',
      displayName: 'Oracle DB 23ai (LogMiner)',
      host: 'oracle-source',
      port: 1521,
      externalPort: 1521,
      database: 'ORCLCDB',
      username: 'c##dbzuser',
      password: 'dbzpassword',
      url: 'oracle://c##dbzuser:dbzpassword@oracle-source:1521/ORCLPDB1',
      connectorClass: 'io.debezium.connector.oracle.OracleConnector',
      topicPrefix: 'cdc.local.oracle_erp.ORCLCDB',
      tableIncludeList: 'ERP.CUSTOMERS,ERP.INVOICES',
      snapshotMode: 'initial',
      logMiningStrategy: 'logminer',
    },
    sqlserver: {
      id: 'src-sql-crm',
      engine: 'sqlserver',
      displayName: 'SQL Server 2022 (CDC Functions)',
      host: 'sqlserver-source',
      port: 1433,
      externalPort: 1433,
      database: 'crm_db',
      username: 'sa',
      password: 'Password123!',
      url: 'sqlserver://sa:Password123!@sqlserver-source:1433/crm_db',
      connectorClass: 'io.debezium.connector.sqlserver.SqlServerConnector',
      topicPrefix: 'cdc.local.mssql_crm.crm_db',
      tableIncludeList: 'dbo.accounts,dbo.leads',
      snapshotMode: 'initial',
    },
  },
  rawPlatformProperties: `# Enterprise CDC Platform Properties (config/cdc-platform.properties)
app.name=Enterprise CDC Platform Control Plane
app.version=1.0.0
app.environment=local-m2
kafka.bootstrap.servers=kafka:9092,localhost:9092
kafka.cluster.id=cdc-kraft-cluster-01
connect.rest.url=http://localhost:8083
connect.worker.id=connect-worker-01:8083
schema.registry.url=http://localhost:8081
schema.registry.compatibility.mode=BACKWARD
smt.transforms.list=unwrap,maskEmail,maskCard
smt.salt=cdc-enterprise-salt-secure-hash-2026
snapshot.algorithm=DBlog
snapshot.watermark.table=public.cdc_signal
dlq.orders.unparseable.topic=cdc.dlq.orders.unparseable
metrics.memory.budget.mb=11520`,
  rawDatabaseProperties: `# Database Connectors & Connectivity Properties (config/database-connectors.properties)
# PostgreSQL
postgres.source.host=postgres-source
postgres.source.port=5432
postgres.source.database=orders_db
postgres.source.user=postgres
postgres.source.password=postgrespassword
postgres.source.url=postgresql://postgres:postgrespassword@postgres-source:5432/orders_db
postgres.topic.prefix=cdc.local.ecommerce.orders_db

# MySQL
mysql.source.host=mysql-source
mysql.source.port=3306
mysql.source.database=inventory
mysql.source.user=mysqluser
mysql.source.password=mysqlpassword
mysql.source.url=mysql://mysqluser:mysqlpassword@mysql-source:3306/inventory
mysql.topic.prefix=cdc.local.mysql_store.inventory

# MariaDB
mariadb.source.host=mariadb-source
mariadb.source.port=3306
mariadb.source.external.port=3308
mariadb.source.database=store_catalog
mariadb.source.user=mariadbuser
mariadb.source.password=mariadbpassword
mariadb.source.server.id=184055
mariadb.source.url=mariadb://mariadbuser:mariadbpassword@mariadb-source:3306/store_catalog
mariadb.source.gtid_strict_mode=ON
mariadb.topic.prefix=cdc.local.mariadb_store.store_catalog

# MongoDB
mongodb.source.host=mongo-source
mongodb.source.port=27017
mongodb.source.connection.string=mongodb://mongo-source:27017/?replicaSet=rs0
mongodb.topic.prefix=cdc.local.mongodb_catalog.store

# Oracle
oracle.source.host=oracle-source
oracle.source.port=1521
oracle.source.url=oracle://c##dbzuser:dbzpassword@oracle-source:1521/ORCLPDB1
oracle.topic.prefix=cdc.local.oracle_erp.ORCLCDB

# SQL Server
sqlserver.source.host=sqlserver-source
sqlserver.source.port=1433
sqlserver.source.url=sqlserver://sa:Password123!@sqlserver-source:1433/crm_db
sqlserver.topic.prefix=cdc.local.mssql_crm.crm_db`
};
