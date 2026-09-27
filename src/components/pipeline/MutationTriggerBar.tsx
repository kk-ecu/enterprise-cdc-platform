import React from 'react';
import { Workflow, Pause, Play } from 'lucide-react';
import { DatabaseEngine, CDCOperationType, CDCEventRecord } from '../../types/cdc';

interface Props {
  selectedDb: DatabaseEngine;
  setSelectedDb: (db: DatabaseEngine) => void;
  autoStreamActive: boolean;
  setAutoStreamActive: (active: boolean) => void;
  onTriggerOperation: (op: CDCOperationType) => void;
  selectedEvent: CDCEventRecord;
}

export const MutationTriggerBar: React.FC<Props> = ({
  selectedDb,
  setSelectedDb,
  autoStreamActive,
  setAutoStreamActive,
  onTriggerOperation,
  selectedEvent,
}) => {
  return (
    <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-4 shadow-xl">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-3 border-b border-slate-800 pb-3">
        <div>
          <div className="flex items-center gap-2">
            <Workflow className="w-5 h-5 text-cyan-400" />
            <h2 className="text-base font-bold text-white">
              Transparent End-to-End CDC Event Pipeline Flow
            </h2>
          </div>
          <p className="text-xs text-slate-400 mt-0.5">
            Full transparency across all 4 stages: database transaction logs, Debezium decoding with SMT, Kafka topic ingestion, and downstream consumer acknowledgments.
          </p>
        </div>

        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1.5 bg-slate-950 px-3 py-1.5 rounded-lg border border-slate-800 text-xs">
            <span className="text-slate-400">Auto Stream:</span>
            <button
              onClick={() => setAutoStreamActive(!autoStreamActive)}
              className={`flex items-center gap-1 px-2.5 py-1 rounded font-bold transition ${
                autoStreamActive
                  ? 'bg-emerald-500 text-slate-950'
                  : 'bg-slate-800 text-slate-400 hover:text-white'
              }`}
            >
              {autoStreamActive ? <Pause className="w-3 h-3" /> : <Play className="w-3 h-3" />}
              {autoStreamActive ? 'STREAMING ON' : 'PAUSED'}
            </button>
          </div>

          {/* Engine Selector */}
          <div className="flex items-center gap-1 bg-slate-950 p-1 rounded-lg border border-slate-800">
            {(['postgres', 'mysql', 'mariadb', 'oracle', 'sqlserver', 'mongodb'] as const).map((db) => (
              <button
                key={db}
                onClick={() => setSelectedDb(db)}
                className={`px-2 py-0.5 rounded text-[11px] font-mono transition ${
                  selectedDb === db
                    ? 'bg-cyan-500 text-slate-950 font-bold'
                    : 'text-slate-400 hover:text-white'
                }`}
              >
                {db === 'postgres' ? 'Postgres' : db === 'mysql' ? 'MySQL' : db === 'mariadb' ? 'MariaDB' : db === 'oracle' ? 'Oracle' : db === 'sqlserver' ? 'MSSQL' : 'Mongo'}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Action Buttons for DML and DDL */}
      <div className="flex flex-wrap items-center justify-between gap-2 pt-1">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-xs font-semibold text-slate-400">Trigger Mutation:</span>
          <button
            onClick={() => onTriggerOperation('UPDATE')}
            className="px-2.5 py-1 rounded bg-blue-950/70 hover:bg-blue-900 text-blue-300 border border-blue-800 text-xs font-medium flex items-center gap-1"
          >
            <span>↻ UPDATE Status</span>
          </button>
          <button
            onClick={() => onTriggerOperation('INSERT')}
            className="px-2.5 py-1 rounded bg-emerald-950/70 hover:bg-emerald-900 text-emerald-300 border border-emerald-800 text-xs font-medium flex items-center gap-1"
          >
            <span>+ INSERT Order</span>
          </button>
          <button
            onClick={() => onTriggerOperation('PK_UPDATE')}
            className="px-2.5 py-1 rounded bg-purple-950/70 hover:bg-purple-900 text-purple-300 border border-purple-800 text-xs font-medium flex items-center gap-1"
          >
            <span>⇋ Primary Key Update</span>
          </button>
          <button
            onClick={() => onTriggerOperation('DELETE')}
            className="px-2.5 py-1 rounded bg-amber-950/70 hover:bg-amber-900 text-amber-300 border border-amber-800 text-xs font-medium flex items-center gap-1"
          >
            <span>✕ DELETE + Tombstone</span>
          </button>
          <button
            onClick={() => onTriggerOperation('DDL_ADD_COL')}
            className="px-2.5 py-1 rounded bg-purple-950/70 hover:bg-purple-900 text-purple-300 border border-purple-800 text-xs font-medium flex items-center gap-1"
          >
            <span>+ ALTER TABLE ADD COLUMN</span>
          </button>
          <button
            onClick={() => onTriggerOperation('TRUNCATE')}
            className="px-2.5 py-1 rounded bg-red-950/70 hover:bg-red-900 text-red-300 border border-red-800 text-xs font-medium flex items-center gap-1"
          >
            <span>⚠ TRUNCATE</span>
          </button>
          <button
            onClick={() => onTriggerOperation('ROLLBACK')}
            className="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 border border-slate-700 text-xs font-medium flex items-center gap-1"
          >
            <span>↺ Rollback Tx</span>
          </button>
        </div>

        <div className="text-[11px] text-slate-400 font-mono">
          Currently Tracking: <strong className="text-cyan-400">{selectedEvent.id}</strong> ({selectedEvent.operationType})
        </div>
      </div>
    </div>
  );
};
