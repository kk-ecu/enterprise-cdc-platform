/**
 * Enterprise CDC Platform: Simulation Service
 * Adheres to SOLID:
 * - Single Responsibility: Generating valid CDC events and metadata.
 * - Open/Closed: Extensible generator strategies.
 * All connectivity parameters, Kafka topics, worker IDs, and cluster URLs
 * are dynamically resolved from PLATFORM_CONFIG (external properties).
 */

import { CDCEventRecord, DatabaseEngine, CDCOperationType } from '../types/cdc';
import { PLATFORM_CONFIG } from '../config/platformConfig';

export interface IMutationStrategy {
  generate(db: DatabaseEngine, currentOffset: number): CDCEventRecord;
}

function getDatabaseConfig(db: DatabaseEngine) {
  return PLATFORM_CONFIG.databases[db] || PLATFORM_CONFIG.databases.postgres;
}

class InsertMutationStrategy implements IMutationStrategy {
  generate(db: DatabaseEngine, currentOffset: number): CDCEventRecord {
    const dbConfig = getDatabaseConfig(db);
    const orderId = Math.floor(1000 + Math.random() * 9000);
    const eventId = `evt_${Date.now().toString().slice(-6)}`;
    const timeStr = new Date().toLocaleTimeString();

    let logCoordType: 'LSN' | 'GTID' | 'SCN' | 'RESUME_TOKEN' = 'LSN';
    let logCoordValue = `0/16B${Math.floor(1000 + Math.random() * 8000).toString(16).toUpperCase()}`;

    if (db === 'mysql') {
      logCoordType = 'GTID';
      logCoordValue = `3e11fa47-71ca-11e1-9e33:410${Math.floor(Math.random() * 90)}`;
    } else if (db === 'mariadb') {
      logCoordType = 'GTID';
      logCoordValue = `0-1-${Math.floor(1000 + Math.random() * 9000)}`;
    } else if (db === 'mongodb') {
      logCoordType = 'RESUME_TOKEN';
      logCoordValue = `8264A7E0000000012B0229296E04`;
    } else if (db === 'oracle') {
      logCoordType = 'SCN';
      logCoordValue = `1948294721`;
    }

    const topic = `${dbConfig.topicPrefix}.${dbConfig.tableIncludeList.split(',')[0]}`;

    return {
      id: eventId,
      timestamp: timeStr,
      sourceType: db,
      operationType: 'INSERT',
      category: 'DML',
      sqlStatement: `INSERT INTO ${dbConfig.tableIncludeList.split(',')[0]} (id, customer_id, status, amount_cents) VALUES (${orderId}, 942, 'PENDING', 24900);`,
      logPosition: { 
        type: logCoordType, 
        value: logCoordValue 
      },
      kafkaTopic: topic,
      partition: orderId % PLATFORM_CONFIG.kafka.numPartitions,
      offset: currentOffset + 1,
      schemaVersion: PLATFORM_CONFIG.schemaRegistry.schemaVersion,
      schemaId: PLATFORM_CONFIG.schemaRegistry.defaultSchemaId,
      key: { id: orderId },
      before: null,
      after: { id: orderId, customer_id: 942, status: 'PENDING', amount_cents: 24900, priority_level: 'STANDARD' },
      status: 'EMITTED_TO_KAFKA',
      debeziumExtraction: {
        workerId: PLATFORM_CONFIG.debezium.workerId,
        pluginUsed: dbConfig.pluginName || (db === 'mariadb' ? 'mariadb-binlog (gtid_strict_mode)' : dbConfig.connectorClass),
        rawLogBytes: 284,
        transformsApplied: ['ByLogicalTableRouter', 'MaskField (PII Redaction)', 'ExtractNewRecordState'],
        maskedFields: PLATFORM_CONFIG.smt.emailFields.map(f => `${f} (SHA-256)`).concat(['tax_id (Redacted)']),
        schemaRegistryCheck: `Schema Valid (Schema ID ${PLATFORM_CONFIG.schemaRegistry.defaultSchemaId})`
      },
      kafkaIngestion: {
        brokerCluster: PLATFORM_CONFIG.kafka.clusterId,
        producerAcks: PLATFORM_CONFIG.kafka.producerAcks,
        idempotentSeq: 4131,
        wireFormatHeader: `0x00 0x00 0x00 0x00 0x${PLATFORM_CONFIG.schemaRegistry.defaultSchemaId.toString(16).toUpperCase()}`,
        compactionKey: `{"id": ${orderId}}`,
        flushAckLatencyMs: 11
      },
      consumerProcessing: {
        dwhSync: { consumerGroup: 'snowflake-cdc-ingest', status: 'PROCESSED', lag: 0, latencyMs: 36 },
        microservice: { consumerGroup: 'order-fulfillment-service', status: 'PROCESSED', lag: 0, latencyMs: 14 },
        searchIndexer: { consumerGroup: 'elasticsearch-sync', status: 'PROCESSED', lag: 0, latencyMs: 19 },
        totalEndToEndLatencyMs: 80
      }
    };
  }
}

class UpdateMutationStrategy implements IMutationStrategy {
  generate(db: DatabaseEngine, currentOffset: number): CDCEventRecord {
    const dbConfig = getDatabaseConfig(db);
    const orderId = 1001;
    const eventId = `evt_${Date.now().toString().slice(-6)}`;
    const timeStr = new Date().toLocaleTimeString();
    const newStatus = ['CONFIRMED', 'SHIPPED', 'DELIVERED', 'REFUNDED'][Math.floor(Math.random() * 4)];
    const newAmount = Math.floor(15000 + Math.random() * 5000);
    const topic = `${dbConfig.topicPrefix}.${dbConfig.tableIncludeList.split(',')[0]}`;

    return {
      id: eventId,
      timestamp: timeStr,
      sourceType: db,
      operationType: 'UPDATE',
      category: 'DML',
      sqlStatement: `UPDATE ${dbConfig.tableIncludeList.split(',')[0]} SET status = '${newStatus}', amount_cents = ${newAmount} WHERE id = ${orderId};`,
      logPosition: { 
        type: db === 'mysql' || db === 'mariadb' ? 'GTID' : 'LSN', 
        value: db === 'mariadb' ? `0-1-1002` : `0/16B${Math.floor(1000 + Math.random() * 8000).toString(16).toUpperCase()}` 
      },
      kafkaTopic: topic,
      partition: 2,
      offset: currentOffset + 1,
      schemaVersion: PLATFORM_CONFIG.schemaRegistry.schemaVersion,
      schemaId: PLATFORM_CONFIG.schemaRegistry.defaultSchemaId,
      key: { id: orderId },
      before: { id: orderId, customer_id: 88, status: 'PENDING', amount_cents: 14999 },
      after: { id: orderId, customer_id: 88, status: newStatus, amount_cents: newAmount },
      diffFields: ['status', 'amount_cents'],
      status: 'EMITTED_TO_KAFKA',
      debeziumExtraction: {
        workerId: PLATFORM_CONFIG.debezium.workerId,
        pluginUsed: `${dbConfig.pluginName || 'connector'} (REPLICA IDENTITY FULL - Complete Before Image Captured)`,
        rawLogBytes: 312,
        transformsApplied: ['ByLogicalTableRouter', 'ExtractNewRecordState'],
        maskedFields: [],
        schemaRegistryCheck: `Schema Valid (Schema ID ${PLATFORM_CONFIG.schemaRegistry.defaultSchemaId})`
      },
      kafkaIngestion: {
        brokerCluster: PLATFORM_CONFIG.kafka.clusterId,
        producerAcks: PLATFORM_CONFIG.kafka.producerAcks,
        idempotentSeq: 4132,
        wireFormatHeader: `0x00 0x00 0x00 0x00 0x${PLATFORM_CONFIG.schemaRegistry.defaultSchemaId.toString(16).toUpperCase()}`,
        compactionKey: `{"id": ${orderId}}`,
        flushAckLatencyMs: 13
      },
      consumerProcessing: {
        dwhSync: { consumerGroup: 'snowflake-cdc-ingest', status: 'PROCESSED', lag: 0, latencyMs: 40 },
        microservice: { consumerGroup: 'order-fulfillment-service', status: 'PROCESSED', lag: 0, latencyMs: 15 },
        searchIndexer: { consumerGroup: 'elasticsearch-sync', status: 'PROCESSED', lag: 0, latencyMs: 22 },
        totalEndToEndLatencyMs: 90
      }
    };
  }
}

class PKUpdateMutationStrategy implements IMutationStrategy {
  generate(db: DatabaseEngine, currentOffset: number): CDCEventRecord {
    const dbConfig = getDatabaseConfig(db);
    const eventId = `evt_${Date.now().toString().slice(-6)}`;
    const timeStr = new Date().toLocaleTimeString();
    const topic = `${dbConfig.topicPrefix}.${dbConfig.tableIncludeList.split(',')[0]}`;

    return {
      id: eventId,
      timestamp: timeStr,
      sourceType: db,
      operationType: 'PK_UPDATE',
      category: 'DML',
      sqlStatement: `UPDATE ${dbConfig.tableIncludeList.split(',')[0]} SET id = 9999 WHERE id = 1001; -- Atomic Delete old PK + Insert new PK`,
      logPosition: { type: 'LSN', value: '0/16B8900' },
      kafkaTopic: topic,
      partition: 0,
      offset: currentOffset + 2,
      schemaVersion: PLATFORM_CONFIG.schemaRegistry.schemaVersion,
      schemaId: PLATFORM_CONFIG.schemaRegistry.defaultSchemaId,
      key: { id: 9999, _old_id: 1001 },
      before: { id: 1001, customer_id: 88, status: 'CONFIRMED' },
      after: { id: 9999, customer_id: 88, status: 'CONFIRMED' },
      diffFields: ['id'],
      status: 'EMITTED_TO_KAFKA',
      debeziumExtraction: {
        workerId: PLATFORM_CONFIG.debezium.workerId,
        pluginUsed: `${dbConfig.pluginName || 'connector'} (Key change split to DELETE + INSERT sequence)`,
        rawLogBytes: 420,
        transformsApplied: ['ExtractNewRecordState'],
        maskedFields: [],
        schemaRegistryCheck: 'Schema Valid'
      },
      kafkaIngestion: {
        brokerCluster: PLATFORM_CONFIG.kafka.clusterId,
        producerAcks: PLATFORM_CONFIG.kafka.producerAcks,
        idempotentSeq: 4133,
        wireFormatHeader: `0x00 0x00 0x00 0x00 0x${PLATFORM_CONFIG.schemaRegistry.defaultSchemaId.toString(16).toUpperCase()}`,
        compactionKey: '{"id": 9999}',
        flushAckLatencyMs: 14
      },
      consumerProcessing: {
        dwhSync: { consumerGroup: 'snowflake-cdc-ingest', status: 'PROCESSED', lag: 0, latencyMs: 44 },
        microservice: { consumerGroup: 'order-fulfillment-service', status: 'PROCESSED', lag: 0, latencyMs: 18 },
        searchIndexer: { consumerGroup: 'elasticsearch-sync', status: 'PROCESSED', lag: 0, latencyMs: 25 },
        totalEndToEndLatencyMs: 101
      }
    };
  }
}

class DeleteMutationStrategy implements IMutationStrategy {
  generate(db: DatabaseEngine, currentOffset: number): CDCEventRecord {
    const dbConfig = getDatabaseConfig(db);
    const eventId = `evt_${Date.now().toString().slice(-6)}`;
    const timeStr = new Date().toLocaleTimeString();
    const topic = `${dbConfig.topicPrefix}.${dbConfig.tableIncludeList.split(',')[0]}`;

    return {
      id: eventId,
      timestamp: timeStr,
      sourceType: db,
      operationType: 'DELETE',
      category: 'DML',
      sqlStatement: `DELETE FROM ${dbConfig.tableIncludeList.split(',')[0]} WHERE id = 1001; -- Emits DELETE event + Kafka Tombstone`,
      logPosition: { type: 'LSN', value: '0/16B9000' },
      kafkaTopic: topic,
      partition: 2,
      offset: currentOffset + 1,
      schemaVersion: PLATFORM_CONFIG.schemaRegistry.schemaVersion,
      schemaId: PLATFORM_CONFIG.schemaRegistry.defaultSchemaId,
      key: { id: 1001 },
      before: { id: 1001, customer_id: 88, status: 'CANCELLED' },
      after: null,
      status: 'EMITTED_TO_KAFKA',
      debeziumExtraction: {
        workerId: PLATFORM_CONFIG.debezium.workerId,
        pluginUsed: dbConfig.pluginName || 'connector',
        rawLogBytes: 198,
        transformsApplied: ['tombstones.on.delete=true'],
        maskedFields: [],
        schemaRegistryCheck: 'Schema Valid'
      },
      kafkaIngestion: {
        brokerCluster: PLATFORM_CONFIG.kafka.clusterId,
        producerAcks: PLATFORM_CONFIG.kafka.producerAcks,
        idempotentSeq: 4134,
        wireFormatHeader: `0x00 0x00 0x00 0x00 0x${PLATFORM_CONFIG.schemaRegistry.defaultSchemaId.toString(16).toUpperCase()}`,
        compactionKey: '{"id": 1001}',
        flushAckLatencyMs: 10
      },
      consumerProcessing: {
        dwhSync: { consumerGroup: 'snowflake-cdc-ingest', status: 'PROCESSED', lag: 0, latencyMs: 34 },
        microservice: { consumerGroup: 'order-fulfillment-service', status: 'PROCESSED', lag: 0, latencyMs: 12 },
        searchIndexer: { consumerGroup: 'elasticsearch-sync', status: 'PROCESSED (Document Removed)', lag: 0, latencyMs: 16 },
        totalEndToEndLatencyMs: 72
      }
    };
  }
}

class DDLAddColumnStrategy implements IMutationStrategy {
  generate(db: DatabaseEngine, currentOffset: number): CDCEventRecord {
    const dbConfig = getDatabaseConfig(db);
    const eventId = `evt_${Date.now().toString().slice(-6)}`;
    const timeStr = new Date().toLocaleTimeString();
    const newSchemaId = PLATFORM_CONFIG.schemaRegistry.defaultSchemaId + 1;
    const newVersion = PLATFORM_CONFIG.schemaRegistry.schemaVersion + 1;

    return {
      id: eventId,
      timestamp: timeStr,
      sourceType: db,
      operationType: 'DDL_ADD_COL',
      category: 'DDL',
      sqlStatement: `ALTER TABLE ${dbConfig.tableIncludeList.split(',')[0]} ADD COLUMN loyalty_tier VARCHAR(20) DEFAULT 'BRONZE';`,
      logPosition: { type: 'LSN', value: '0/16BC100' },
      kafkaTopic: '_schemas',
      partition: 0,
      offset: 108,
      schemaVersion: newVersion,
      schemaId: newSchemaId,
      key: { subject: `${dbConfig.tableIncludeList.split(',')[0]}-value` },
      before: { version: PLATFORM_CONFIG.schemaRegistry.schemaVersion, fields: ['id', 'customer_id', 'status', 'amount_cents', 'priority_level'] },
      after: { version: newVersion, fields: ['id', 'customer_id', 'status', 'amount_cents', 'priority_level', 'loyalty_tier'], compatibility: PLATFORM_CONFIG.schemaRegistry.compatibilityMode },
      status: 'SCHEMA_EVOLVED',
      debeziumExtraction: {
        workerId: PLATFORM_CONFIG.debezium.workerId,
        pluginUsed: `${dbConfig.pluginName || 'connector'} (Schema DDL Detected)`,
        rawLogBytes: 210,
        transformsApplied: ['SchemaEvolutionInterceptor'],
        maskedFields: [],
        schemaRegistryCheck: `${PLATFORM_CONFIG.schemaRegistry.compatibilityMode} Compatible (Auto-Registered New Schema ID ${newSchemaId})`
      },
      kafkaIngestion: {
        brokerCluster: PLATFORM_CONFIG.kafka.clusterId,
        producerAcks: PLATFORM_CONFIG.kafka.producerAcks,
        idempotentSeq: 4137,
        wireFormatHeader: `0x00 0x00 0x00 0x00 0x${newSchemaId.toString(16).toUpperCase()} (Schema ID ${newSchemaId})`,
        compactionKey: `${dbConfig.tableIncludeList.split(',')[0]}-value`,
        flushAckLatencyMs: 12
      },
      consumerProcessing: {
        dwhSync: { consumerGroup: 'snowflake-cdc-ingest', status: 'PROCESSED (Schema Migrated)', lag: 0, latencyMs: 38 },
        microservice: { consumerGroup: 'order-fulfillment-service', status: 'PROCESSED', lag: 0, latencyMs: 14 },
        searchIndexer: { consumerGroup: 'elasticsearch-sync', status: 'PROCESSED (Mapping Updated)', lag: 0, latencyMs: 22 },
        totalEndToEndLatencyMs: 86
      }
    };
  }
}

class RollbackStrategy implements IMutationStrategy {
  generate(db: DatabaseEngine, currentOffset: number): CDCEventRecord {
    const dbConfig = getDatabaseConfig(db);
    const eventId = `evt_${Date.now().toString().slice(-6)}`;
    const timeStr = new Date().toLocaleTimeString();
    return {
      id: eventId,
      timestamp: timeStr,
      sourceType: db,
      operationType: 'ROLLBACK',
      category: 'DML',
      sqlStatement: `BEGIN; INSERT INTO ${dbConfig.tableIncludeList.split(',')[0]} VALUES (999, 'INVALID'); ROLLBACK; -- Aborted Transaction`,
      logPosition: { type: 'LSN', value: '0/16BB000 (Aborted in WAL)' },
      kafkaTopic: 'NONE (Filtered at WAL Decoding Layer)',
      partition: -1,
      offset: -1,
      schemaVersion: 0,
      schemaId: 0,
      key: { tx_id: 'aborted_703' },
      before: null,
      after: null,
      status: 'ROLLED_BACK_ZERO_EMISSION',
      debeziumExtraction: {
        workerId: PLATFORM_CONFIG.debezium.workerId,
        pluginUsed: `${dbConfig.pluginName || 'connector'} (Logical Decoding filtered out uncommitted txn)`,
        rawLogBytes: 80,
        transformsApplied: [],
        maskedFields: [],
        schemaRegistryCheck: 'N/A'
      },
      kafkaIngestion: {
        brokerCluster: 'N/A (Zero Producer Dispatches)',
        producerAcks: 'N/A',
        idempotentSeq: 0,
        wireFormatHeader: 'None',
        compactionKey: 'None',
        flushAckLatencyMs: 0
      },
      consumerProcessing: {
        dwhSync: { consumerGroup: 'snowflake-cdc-ingest', status: 'SKIPPED', lag: 0, latencyMs: 0 },
        microservice: { consumerGroup: 'order-fulfillment-service', status: 'SKIPPED', lag: 0, latencyMs: 0 },
        searchIndexer: { consumerGroup: 'elasticsearch-sync', status: 'SKIPPED', lag: 0, latencyMs: 0 },
        totalEndToEndLatencyMs: 0
      }
    };
  }
}

class TruncateStrategy implements IMutationStrategy {
  generate(db: DatabaseEngine, currentOffset: number): CDCEventRecord {
    const dbConfig = getDatabaseConfig(db);
    const eventId = `evt_${Date.now().toString().slice(-6)}`;
    const timeStr = new Date().toLocaleTimeString();
    return {
      id: eventId,
      timestamp: timeStr,
      sourceType: db,
      operationType: 'TRUNCATE',
      category: 'DML',
      sqlStatement: `TRUNCATE TABLE public.staging_orders; -- Table wipe on ${dbConfig.displayName}`,
      logPosition: { type: 'LSN', value: '0/16B9500' },
      kafkaTopic: `${dbConfig.topicPrefix}.public.staging_orders`,
      partition: 0,
      offset: currentOffset + 1,
      schemaVersion: 1,
      schemaId: 22,
      key: { table: 'staging_orders' },
      before: { total_records_purged: 45000 },
      after: null,
      status: 'EMITTED_TO_KAFKA',
      debeziumExtraction: {
        workerId: PLATFORM_CONFIG.debezium.workerId,
        pluginUsed: `${dbConfig.pluginName || 'connector'} (Truncate publication listener)`,
        rawLogBytes: 94,
        transformsApplied: ['TruncateEventExtractor'],
        maskedFields: [],
        schemaRegistryCheck: 'Schema Valid'
      },
      kafkaIngestion: {
        brokerCluster: PLATFORM_CONFIG.kafka.clusterId,
        producerAcks: PLATFORM_CONFIG.kafka.producerAcks,
        idempotentSeq: 4135,
        wireFormatHeader: '0x00 0x00 0x00 0x00 0x16',
        compactionKey: 'staging_orders',
        flushAckLatencyMs: 9
      },
      consumerProcessing: {
        dwhSync: { consumerGroup: 'snowflake-cdc-ingest', status: 'PROCESSED (Table Truncated)', lag: 0, latencyMs: 50 },
        microservice: { consumerGroup: 'order-fulfillment-service', status: 'SKIPPED', lag: 0, latencyMs: 2 },
        searchIndexer: { consumerGroup: 'elasticsearch-sync', status: 'PROCESSED (Index Cleared)', lag: 0, latencyMs: 25 },
        totalEndToEndLatencyMs: 86
      }
    };
  }
}

export class CDCMutationFactory {
  private static strategies: Record<string, IMutationStrategy> = {
    INSERT: new InsertMutationStrategy(),
    UPDATE: new UpdateMutationStrategy(),
    PK_UPDATE: new PKUpdateMutationStrategy(),
    DELETE: new DeleteMutationStrategy(),
    DDL_ADD_COL: new DDLAddColumnStrategy(),
    ROLLBACK: new RollbackStrategy(),
    TRUNCATE: new TruncateStrategy(),
  };

  public static createEvent(
    op: CDCOperationType,
    db: DatabaseEngine,
    currentOffset: number
  ): CDCEventRecord {
    const strategy = this.strategies[op] || this.strategies['UPDATE'];
    return strategy.generate(db, currentOffset);
  }
}
