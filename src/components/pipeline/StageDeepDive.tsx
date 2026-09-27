import React from 'react';
import { Check } from 'lucide-react';
import { CDCEventRecord } from '../../types/cdc';

interface Props {
  selectedEvent: CDCEventRecord;
  activeStageViewer: 1 | 2 | 3 | 4;
  setActiveStageViewer: (stage: 1 | 2 | 3 | 4) => void;
}

export const StageDeepDive: React.FC<Props> = ({
  selectedEvent,
  activeStageViewer,
  setActiveStageViewer,
}) => {
  return (
    <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-4">
      <div className="flex items-center justify-between border-b border-slate-800 pb-3">
        <div className="flex items-center gap-3">
          <span className="text-xs font-bold text-slate-400 uppercase tracking-wider">Deep Dive Stage Inspection:</span>
          <div className="flex gap-1.5">
            {[
              { num: 1, label: 'Stage 1: Source DB Mutation' },
              { num: 2, label: 'Stage 2: Debezium Decoding & SMT' },
              { num: 3, label: 'Stage 3: Kafka Message Details' },
              { num: 4, label: 'Stage 4: Downstream Consumer Acks' },
            ].map((st) => (
              <button
                key={st.num}
                onClick={() => setActiveStageViewer(st.num as any)}
                className={`px-3 py-1 rounded text-xs font-medium transition ${
                  activeStageViewer === st.num
                    ? 'bg-cyan-500 text-slate-950 font-bold'
                    : 'bg-slate-800 text-slate-400 hover:text-white'
                }`}
              >
                {st.label}
              </button>
            ))}
          </div>
        </div>

        <div className="text-xs text-slate-400 font-mono">
          Event ID: <strong className="text-cyan-400">{selectedEvent.id}</strong>
        </div>
      </div>

      {/* View 1: Database Mutation */}
      {activeStageViewer === 1 && (
        <div className="space-y-3 text-xs">
          <div className="p-3 rounded bg-slate-950 border border-slate-800 font-mono text-cyan-300 text-xs">
            <span className="text-slate-500 select-none">SQL &gt; </span>
            {selectedEvent.sqlStatement}
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="p-3.5 rounded bg-slate-950 border border-slate-800 space-y-2">
              <div className="font-semibold text-amber-400 flex items-center justify-between">
                <span>BEFORE TUPLE (From Replication Log)</span>
                <span className="text-[10px] text-slate-500 font-mono">REPLICA IDENTITY FULL</span>
              </div>
              <pre className="font-mono text-[11px] text-slate-300 overflow-x-auto p-2 bg-slate-900 rounded">
                {selectedEvent.before ? JSON.stringify(selectedEvent.before, null, 2) : 'null (New Row Created)'}
              </pre>
            </div>

            <div className="p-3.5 rounded bg-slate-950 border border-slate-800 space-y-2">
              <div className="font-semibold text-emerald-400 flex items-center justify-between">
                <span>AFTER TUPLE (Committed State in Log)</span>
                <span className="text-[10px] text-emerald-400 font-mono">Committed LSN {selectedEvent.logPosition.value}</span>
              </div>
              <pre className="font-mono text-[11px] text-slate-300 overflow-x-auto p-2 bg-slate-900 rounded">
                {selectedEvent.after ? JSON.stringify(selectedEvent.after, null, 2) : 'null (Row Deleted / Tombstone)'}
              </pre>
            </div>
          </div>
        </div>
      )}

      {/* View 2: Debezium Decoding & SMT */}
      {activeStageViewer === 2 && (
        <div className="space-y-3 text-xs">
          <div className="p-3.5 rounded bg-slate-950 border border-slate-800 space-y-2 font-mono text-[11px]">
            <div className="text-cyan-400 font-bold text-xs">Debezium Connector Extraction Details:</div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-slate-300 pt-1">
              <div>&bull; Connector Worker: <span className="text-white">{selectedEvent.debeziumExtraction.workerId}</span></div>
              <div>&bull; Decoding Plugin: <span className="text-white">{selectedEvent.debeziumExtraction.pluginUsed}</span></div>
              <div>&bull; Raw Log Segment Read: <span className="text-cyan-300">{selectedEvent.debeziumExtraction.rawLogBytes} bytes</span></div>
              <div>&bull; Schema Registry Status: <span className="text-purple-300">{selectedEvent.debeziumExtraction.schemaRegistryCheck}</span></div>
            </div>
          </div>

          <div className="p-3.5 rounded bg-slate-950 border border-slate-800 space-y-2 font-mono text-[11px]">
            <div className="text-blue-400 font-bold text-xs">Single Message Transforms (SMT) Executed In-Flight:</div>
            <div className="space-y-1 text-slate-300">
              {selectedEvent.debeziumExtraction.transformsApplied.map((tr, idx) => (
                <div key={idx} className="flex items-center gap-2">
                  <Check className="w-3.5 h-3.5 text-emerald-400" />
                  <span>{tr}</span>
                </div>
              ))}
            </div>
            {selectedEvent.debeziumExtraction.maskedFields.length > 0 && (
              <div className="pt-2 text-[11px] text-amber-300">
                PII Fields Masked/Hashed before Kafka publish: {selectedEvent.debeziumExtraction.maskedFields.join(', ')}
              </div>
            )}
          </div>
        </div>
      )}

      {/* View 3: Kafka Message Details */}
      {activeStageViewer === 3 && (
        <div className="space-y-3 text-xs">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <div className="p-3 rounded bg-slate-950 border border-slate-800">
              <div className="text-[10px] text-slate-400">Destination Topic</div>
              <div className="text-xs font-mono font-bold text-emerald-400 mt-0.5 truncate">{selectedEvent.kafkaTopic}</div>
            </div>
            <div className="p-3 rounded bg-slate-950 border border-slate-800">
              <div className="text-[10px] text-slate-400">Partition & Offset</div>
              <div className="text-xs font-mono font-bold text-white mt-0.5">Partition {selectedEvent.partition} &bull; Offset #{selectedEvent.offset}</div>
            </div>
            <div className="p-3 rounded bg-slate-950 border border-slate-800">
              <div className="text-[10px] text-slate-400">Producer Guarantee</div>
              <div className="text-xs font-mono font-bold text-cyan-400 mt-0.5">{selectedEvent.kafkaIngestion.producerAcks}</div>
            </div>
            <div className="p-3 rounded bg-slate-950 border border-slate-800">
              <div className="text-[10px] text-slate-400">Flush Ack Latency</div>
              <div className="text-xs font-mono font-bold text-purple-400 mt-0.5">{selectedEvent.kafkaIngestion.flushAckLatencyMs} ms</div>
            </div>
          </div>

          <div className="p-3.5 rounded bg-slate-950 border border-slate-800 space-y-2">
            <div className="text-[11px] font-mono text-slate-400 flex items-center justify-between">
              <span>Serialized Kafka Message Payload (Canonical Wire-Format)</span>
              <span className="text-purple-400">Wire Header: {selectedEvent.kafkaIngestion.wireFormatHeader}</span>
            </div>
            <pre className="p-3 rounded bg-slate-900 border border-slate-800 font-mono text-[11px] text-slate-300 overflow-x-auto">
{JSON.stringify({
  eventId: selectedEvent.id,
  eventType: selectedEvent.operationType,
  source: { type: selectedEvent.sourceType, topic: selectedEvent.kafkaTopic },
  position: selectedEvent.logPosition,
  key: selectedEvent.key,
  before: selectedEvent.before,
  after: selectedEvent.after,
  metadata: {
    schemaId: selectedEvent.schemaId,
    schemaVersion: selectedEvent.schemaVersion,
    timestamp: selectedEvent.timestamp
  }
}, null, 2)}
            </pre>
          </div>
        </div>
      )}

      {/* View 4: Downstream Consumers */}
      {activeStageViewer === 4 && (
        <div className="space-y-3 text-xs">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div className="p-3.5 rounded bg-slate-950 border border-slate-800 space-y-1.5">
              <div className="flex items-center justify-between">
                <strong className="text-white text-xs">1. Data Warehouse / Data Lake</strong>
                <span className="text-[10px] font-mono text-emerald-400 bg-emerald-950 px-1.5 py-0.5 rounded">ACK</span>
              </div>
              <div className="text-slate-400 text-[11px] font-mono">Consumer: {selectedEvent.consumerProcessing.dwhSync.consumerGroup}</div>
              <div className="text-slate-400 text-[11px] font-mono">Latency: <span className="text-cyan-400">{selectedEvent.consumerProcessing.dwhSync.latencyMs} ms</span></div>
              <div className="text-slate-400 text-[11px] font-mono">Consumer Lag: <span className="text-emerald-400">{selectedEvent.consumerProcessing.dwhSync.lag} records</span></div>
            </div>

            <div className="p-3.5 rounded bg-slate-950 border border-slate-800 space-y-1.5">
              <div className="flex items-center justify-between">
                <strong className="text-white text-xs">2. Order Microservice</strong>
                <span className="text-[10px] font-mono text-emerald-400 bg-emerald-950 px-1.5 py-0.5 rounded">ACK</span>
              </div>
              <div className="text-slate-400 text-[11px] font-mono">Consumer: {selectedEvent.consumerProcessing.microservice.consumerGroup}</div>
              <div className="text-slate-400 text-[11px] font-mono">Latency: <span className="text-cyan-400">{selectedEvent.consumerProcessing.microservice.latencyMs} ms</span></div>
              <div className="text-slate-400 text-[11px] font-mono">Consumer Lag: <span className="text-emerald-400">{selectedEvent.consumerProcessing.microservice.lag} records</span></div>
            </div>

            <div className="p-3.5 rounded bg-slate-950 border border-slate-800 space-y-1.5">
              <div className="flex items-center justify-between">
                <strong className="text-white text-xs">3. Search & Vector Indexer</strong>
                <span className="text-[10px] font-mono text-emerald-400 bg-emerald-950 px-1.5 py-0.5 rounded">ACK</span>
              </div>
              <div className="text-slate-400 text-[11px] font-mono">Consumer: {selectedEvent.consumerProcessing.searchIndexer.consumerGroup}</div>
              <div className="text-slate-400 text-[11px] font-mono">Latency: <span className="text-cyan-400">{selectedEvent.consumerProcessing.searchIndexer.latencyMs} ms</span></div>
              <div className="text-slate-400 text-[11px] font-mono">Consumer Lag: <span className="text-emerald-400">{selectedEvent.consumerProcessing.searchIndexer.lag} records</span></div>
            </div>
          </div>

          <div className="p-3 rounded bg-slate-950 border border-slate-800 font-mono text-[11px] flex items-center justify-between">
            <span>Idempotent Deduplication Check: <strong className="text-emerald-400">PASSED (Unique eventId verified)</strong></span>
            <span>Total End-to-End Latency: <strong className="text-cyan-400">{selectedEvent.consumerProcessing.totalEndToEndLatencyMs} ms</strong></span>
          </div>
        </div>
      )}
    </div>
  );
};
