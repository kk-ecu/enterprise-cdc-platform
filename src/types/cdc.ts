/**
 * Enterprise CDC Platform: Domain Type Definitions
 * Adheres to Single Responsibility Principle: isolated pure types & interfaces.
 */

export type DatabaseEngine = 'postgres' | 'mysql' | 'mariadb' | 'oracle' | 'sqlserver' | 'mongodb';

export type CDCOperationType =
  | 'INSERT'
  | 'UPDATE'
  | 'DELETE'
  | 'PK_UPDATE'
  | 'TRUNCATE'
  | 'LARGE_TX'
  | 'ROLLBACK'
  | 'DDL_ADD_COL'
  | 'DDL_ALTER_TYPE'
  | 'DDL_DROP_COL';

export type LogPositionType = 'LSN' | 'GTID' | 'SCN' | 'RESUME_TOKEN';

export interface CDCEventRecord {
  id: string;
  timestamp: string;
  sourceType: DatabaseEngine;
  operationType: CDCOperationType;
  category: 'DML' | 'DDL';
  sqlStatement: string;
  logPosition: { type: LogPositionType; value: string };
  kafkaTopic: string;
  partition: number;
  offset: number;
  schemaVersion: number;
  schemaId: number;
  key: Record<string, any>;
  before: Record<string, any> | null;
  after: Record<string, any> | null;
  diffFields?: string[];
  status: 'EMITTED_TO_KAFKA' | 'SCHEMA_EVOLVED' | 'ROLLED_BACK_ZERO_EMISSION' | 'DLQ_QUARANTINE';
  debeziumExtraction: {
    workerId: string;
    pluginUsed: string;
    rawLogBytes: number;
    transformsApplied: string[];
    maskedFields: string[];
    schemaRegistryCheck: string;
  };
  kafkaIngestion: {
    brokerCluster: string;
    producerAcks: string;
    idempotentSeq: number;
    wireFormatHeader: string;
    compactionKey: string;
    flushAckLatencyMs: number;
  };
  consumerProcessing: {
    dwhSync: { consumerGroup: string; status: string; lag: number; latencyMs: number };
    microservice: { consumerGroup: string; status: string; lag: number; latencyMs: number };
    searchIndexer: { consumerGroup: string; status: string; lag: number; latencyMs: number };
    totalEndToEndLatencyMs: number;
  };
}

export interface PlatformStats {
  totalEvents: number;
  inserts: number;
  updates: number;
  deletes: number;
  truncates: number;
  pkUpdates: number;
  schemaEvolutions: number;
  eps: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
  lsnLagBytes: number;
  memoryUsedMb: number;
  memoryBudgetMb: number;
  dlqCount: number;
}
