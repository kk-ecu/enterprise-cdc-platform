import React from 'react';
import { PlatformStats } from '../../types/cdc';

interface Props {
  stats: PlatformStats;
}

export const LiveTelemetryBar: React.FC<Props> = ({ stats }) => {
  return (
    <section className="bg-slate-900 border-b border-slate-800 px-6 py-3">
      <div className="max-w-7xl mx-auto grid grid-cols-2 sm:grid-cols-4 md:grid-cols-7 gap-3">
        <div className="p-2.5 rounded-md bg-slate-950 border border-slate-800">
          <div className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">Total CDC Events</div>
          <div className="text-base font-bold text-white font-mono mt-0.5">{stats.totalEvents.toLocaleString()}</div>
          <div className="text-[10px] text-cyan-400 flex items-center gap-1">Kafka Partitioned Log</div>
        </div>

        <div className="p-2.5 rounded-md bg-slate-950 border border-slate-800">
          <div className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">Inserts (CREATE)</div>
          <div className="text-base font-bold text-emerald-400 font-mono mt-0.5">{stats.inserts.toLocaleString()}</div>
          <div className="text-[10px] text-slate-500 font-mono">before = null</div>
        </div>

        <div className="p-2.5 rounded-md bg-slate-950 border border-slate-800">
          <div className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">Updates (FULL DIFF)</div>
          <div className="text-base font-bold text-blue-400 font-mono mt-0.5">{stats.updates.toLocaleString()}</div>
          <div className="text-[10px] text-slate-500 font-mono">Replica Identity FULL</div>
        </div>

        <div className="p-2.5 rounded-md bg-slate-950 border border-slate-800">
          <div className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">Deletes & Tombstone</div>
          <div className="text-base font-bold text-amber-400 font-mono mt-0.5">{stats.deletes.toLocaleString()}</div>
          <div className="text-[10px] text-slate-500 font-mono">Log Compaction Clean</div>
        </div>

        <div className="p-2.5 rounded-md bg-slate-950 border border-slate-800">
          <div className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">DDL Schema Evolutions</div>
          <div className="text-base font-bold text-purple-400 font-mono mt-0.5">{stats.schemaEvolutions}</div>
          <div className="text-[10px] text-purple-400 font-mono">Schema Reg BACKWARD</div>
        </div>

        <div className="p-2.5 rounded-md bg-slate-950 border border-slate-800">
          <div className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">P95 / P99 Latency</div>
          <div className="text-base font-bold text-white font-mono mt-0.5">{stats.p95LatencyMs} / {stats.p99LatencyMs} ms</div>
          <div className="text-[10px] text-emerald-400 font-mono">Sub-Second Target</div>
        </div>

        <div className="p-2.5 rounded-md bg-slate-950 border border-slate-800">
          <div className="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">WAL Slot Lag</div>
          <div className="text-base font-bold text-emerald-400 font-mono mt-0.5">0 Bytes</div>
          <div className="text-[10px] text-slate-500 font-mono">Flush LSN Confirmed</div>
        </div>
      </div>
    </section>
  );
};
