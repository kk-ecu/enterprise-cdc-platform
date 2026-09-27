import React from 'react';
import { Database, Activity, Server, CheckCheck, ArrowRight } from 'lucide-react';
import { CDCEventRecord } from '../../types/cdc';

interface Props {
  selectedEvent: CDCEventRecord;
  activeStageViewer: 1 | 2 | 3 | 4;
  setActiveStageViewer: (stage: 1 | 2 | 3 | 4) => void;
}

export const PipelineFlowVisualizer: React.FC<Props> = ({
  selectedEvent,
  activeStageViewer,
  setActiveStageViewer,
}) => {
  return (
    <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
      {/* STAGE 1: SOURCE DB */}
      <div
        onClick={() => setActiveStageViewer(1)}
        className={`p-4 rounded-xl border cursor-pointer transition relative ${
          activeStageViewer === 1
            ? 'bg-slate-900 border-cyan-500 shadow-lg shadow-cyan-500/10 ring-1 ring-cyan-500'
            : 'bg-slate-900/60 border-slate-800 hover:border-slate-700'
        }`}
      >
        <div className="flex items-center justify-between mb-2">
          <span className="text-[10px] font-mono uppercase tracking-wider text-cyan-400 font-bold bg-cyan-950/80 px-2 py-0.5 rounded border border-cyan-800">
            STAGE 1: SOURCE DB
          </span>
          <Database className="w-4 h-4 text-cyan-400" />
        </div>
        <h3 className="font-bold text-white text-sm">1. Database Transaction</h3>
        <p className="text-[11px] text-slate-400 mt-1 line-clamp-2">
          {selectedEvent.sqlStatement}
        </p>
        <div className="mt-3 pt-2 border-t border-slate-800 text-[11px] font-mono space-y-1">
          <div className="text-slate-300 flex justify-between">
            <span>Engine:</span> <strong className="text-white">{selectedEvent.sourceType.toUpperCase()}</strong>
          </div>
          <div className="text-slate-300 flex justify-between">
            <span>{selectedEvent.logPosition.type}:</span> <strong className="text-cyan-300">{selectedEvent.logPosition.value}</strong>
          </div>
          <div className="text-slate-300 flex justify-between">
            <span>Committed:</span> <strong className="text-emerald-400">YES (In WAL)</strong>
          </div>
        </div>
        <div className="hidden md:block absolute -right-3 top-1/2 -translate-y-1/2 z-10">
          <div className="w-6 h-6 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center text-slate-400">
            <ArrowRight className="w-3 h-3 text-cyan-400" />
          </div>
        </div>
      </div>

      {/* STAGE 2: DEBEZIUM */}
      <div
        onClick={() => setActiveStageViewer(2)}
        className={`p-4 rounded-xl border cursor-pointer transition relative ${
          activeStageViewer === 2
            ? 'bg-slate-900 border-blue-500 shadow-lg shadow-blue-500/10 ring-1 ring-blue-500'
            : 'bg-slate-900/60 border-slate-800 hover:border-slate-700'
        }`}
      >
        <div className="flex items-center justify-between mb-2">
          <span className="text-[10px] font-mono uppercase tracking-wider text-blue-400 font-bold bg-blue-950/80 px-2 py-0.5 rounded border border-blue-800">
            STAGE 2: DEBEZIUM
          </span>
          <Activity className="w-4 h-4 text-blue-400" />
        </div>
        <h3 className="font-bold text-white text-sm">2. Log Capture & SMT</h3>
        <p className="text-[11px] text-slate-400 mt-1">
          {selectedEvent.debeziumExtraction.pluginUsed}
        </p>
        <div className="mt-3 pt-2 border-t border-slate-800 text-[11px] font-mono space-y-1">
          <div className="text-slate-300 flex justify-between">
            <span>Decoded:</span> <strong className="text-blue-300">{selectedEvent.debeziumExtraction.rawLogBytes} bytes</strong>
          </div>
          <div className="text-slate-300 flex justify-between">
            <span>SMT Masking:</span> <strong className="text-emerald-400">Active (PII Hashed)</strong>
          </div>
          <div className="text-slate-300 flex justify-between">
            <span>Schema Reg:</span> <strong className="text-purple-300">Schema ID {selectedEvent.schemaId}</strong>
          </div>
        </div>
        <div className="hidden md:block absolute -right-3 top-1/2 -translate-y-1/2 z-10">
          <div className="w-6 h-6 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center text-slate-400">
            <ArrowRight className="w-3 h-3 text-blue-400" />
          </div>
        </div>
      </div>

      {/* STAGE 3: KAFKA */}
      <div
        onClick={() => setActiveStageViewer(3)}
        className={`p-4 rounded-xl border cursor-pointer transition relative ${
          activeStageViewer === 3
            ? 'bg-slate-900 border-purple-500 shadow-lg shadow-purple-500/10 ring-1 ring-purple-500'
            : 'bg-slate-900/60 border-slate-800 hover:border-slate-700'
        }`}
      >
        <div className="flex items-center justify-between mb-2">
          <span className="text-[10px] font-mono uppercase tracking-wider text-purple-400 font-bold bg-purple-950/80 px-2 py-0.5 rounded border border-purple-800">
            STAGE 3: KAFKA
          </span>
          <Server className="w-4 h-4 text-purple-400" />
        </div>
        <h3 className="font-bold text-white text-sm">3. Kafka KRaft Topic</h3>
        <p className="text-[11px] text-slate-400 mt-1 truncate">
          {selectedEvent.kafkaTopic}
        </p>
        <div className="mt-3 pt-2 border-t border-slate-800 text-[11px] font-mono space-y-1">
          <div className="text-slate-300 flex justify-between">
            <span>Partition:</span> <strong className="text-white">{selectedEvent.partition >= 0 ? `P-${selectedEvent.partition}` : 'N/A'}</strong>
          </div>
          <div className="text-slate-300 flex justify-between">
            <span>Offset:</span> <strong className="text-cyan-300">#{selectedEvent.offset >= 0 ? selectedEvent.offset : 'N/A'}</strong>
          </div>
          <div className="text-slate-300 flex justify-between">
            <span>Producer Ack:</span> <strong className="text-emerald-400">acks=all (Idempotent)</strong>
          </div>
        </div>
        <div className="hidden md:block absolute -right-3 top-1/2 -translate-y-1/2 z-10">
          <div className="w-6 h-6 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center text-slate-400">
            <ArrowRight className="w-3 h-3 text-purple-400" />
          </div>
        </div>
      </div>

      {/* STAGE 4: CONSUMERS */}
      <div
        onClick={() => setActiveStageViewer(4)}
        className={`p-4 rounded-xl border cursor-pointer transition ${
          activeStageViewer === 4
            ? 'bg-slate-900 border-emerald-500 shadow-lg shadow-emerald-500/10 ring-1 ring-emerald-500'
            : 'bg-slate-900/60 border-slate-800 hover:border-slate-700'
        }`}
      >
        <div className="flex items-center justify-between mb-2">
          <span className="text-[10px] font-mono uppercase tracking-wider text-emerald-400 font-bold bg-emerald-950/80 px-2 py-0.5 rounded border border-emerald-800">
            STAGE 4: CONSUMERS
          </span>
          <CheckCheck className="w-4 h-4 text-emerald-400" />
        </div>
        <h3 className="font-bold text-white text-sm">4. Consumer Processing</h3>
        <p className="text-[11px] text-slate-400 mt-1">
          DWH &bull; Microservices &bull; Elastic
        </p>
        <div className="mt-3 pt-2 border-t border-slate-800 text-[11px] font-mono space-y-1">
          <div className="text-slate-300 flex justify-between">
            <span>Snowflake Sync:</span> <strong className="text-emerald-400">{selectedEvent.consumerProcessing.dwhSync.status}</strong>
          </div>
          <div className="text-slate-300 flex justify-between">
            <span>Microservice:</span> <strong className="text-emerald-400">{selectedEvent.consumerProcessing.microservice.status}</strong>
          </div>
          <div className="text-slate-300 flex justify-between">
            <span>E2E Latency:</span> <strong className="text-cyan-300">{selectedEvent.consumerProcessing.totalEndToEndLatencyMs} ms</strong>
          </div>
        </div>
      </div>
    </div>
  );
};
