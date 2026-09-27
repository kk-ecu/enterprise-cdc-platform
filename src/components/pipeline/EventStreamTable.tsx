import React from 'react';
import { Activity, CheckCircle2 } from 'lucide-react';
import { CDCEventRecord } from '../../types/cdc';

interface Props {
  events: CDCEventRecord[];
  selectedEventId: string;
  setSelectedEventId: (id: string) => void;
  filterOp: string;
  setFilterOp: (f: string) => void;
}

export const EventStreamTable: React.FC<Props> = ({
  events,
  selectedEventId,
  setSelectedEventId,
  filterOp,
  setFilterOp,
}) => {
  const filteredEvents = events.filter((e) => {
    if (filterOp === 'ALL') return true;
    if (filterOp === 'DML') return e.category === 'DML';
    if (filterOp === 'DDL') return e.category === 'DDL';
    return e.operationType === filterOp;
  });

  return (
    <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-3">
      <div className="flex items-center justify-between border-b border-slate-800 pb-2">
        <div className="flex items-center gap-2">
          <Activity className="w-4 h-4 text-cyan-400" />
          <h3 className="font-semibold text-white text-xs">Real-Time Event Stream History</h3>
        </div>

        <div className="flex items-center gap-2">
          <div className="flex gap-1 text-[10px]">
            {['ALL', 'DML', 'DDL'].map((f) => (
              <button
                key={f}
                onClick={() => setFilterOp(f)}
                className={`px-2 py-0.5 rounded font-mono ${
                  filterOp === f ? 'bg-cyan-500 text-slate-950 font-bold' : 'bg-slate-800 text-slate-400'
                }`}
              >
                {f}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-xs text-left">
          <thead className="bg-slate-950 text-slate-400 font-mono text-[11px] border-b border-slate-800">
            <tr>
              <th className="p-2.5">Event ID & Time</th>
              <th className="p-2.5">Engine</th>
              <th className="p-2.5">Op Type</th>
              <th className="p-2.5">Executed SQL Statement</th>
              <th className="p-2.5">Log Position</th>
              <th className="p-2.5">Kafka Topic & Partition</th>
              <th className="p-2.5">Pipeline Status</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/60 font-mono text-[11px]">
            {filteredEvents.map((evt) => {
              const isSelected = evt.id === selectedEventId;
              return (
                <tr
                  key={evt.id}
                  onClick={() => setSelectedEventId(evt.id)}
                  className={`cursor-pointer transition ${
                    isSelected ? 'bg-cyan-950/40 text-cyan-200' : 'hover:bg-slate-800/40 text-slate-300'
                  }`}
                >
                  <td className="p-2.5 font-bold text-white whitespace-nowrap">
                    {evt.id} <span className="text-[10px] text-slate-500 font-normal">({evt.timestamp})</span>
                  </td>
                  <td className="p-2.5 text-slate-400 uppercase">{evt.sourceType}</td>
                  <td className="p-2.5">
                    <span
                      className={`text-[10px] font-bold px-1.5 py-0.5 rounded ${
                        evt.operationType === 'INSERT'
                          ? 'bg-emerald-950 text-emerald-400 border border-emerald-800'
                          : evt.operationType === 'UPDATE'
                          ? 'bg-blue-950 text-blue-400 border border-blue-800'
                          : evt.operationType === 'DELETE'
                          ? 'bg-amber-950 text-amber-400 border border-amber-800'
                          : evt.operationType.startsWith('DDL')
                          ? 'bg-purple-950 text-purple-400 border border-purple-800'
                          : 'bg-red-950 text-red-400'
                      }`}
                    >
                      {evt.operationType}
                    </span>
                  </td>
                  <td className="p-2.5 font-sans truncate max-w-xs">{evt.sqlStatement}</td>
                  <td className="p-2.5 text-cyan-400">{evt.logPosition.value}</td>
                  <td className="p-2.5 text-slate-400 truncate max-w-[200px]">{evt.kafkaTopic} (P-{evt.partition})</td>
                  <td className="p-2.5">
                    <span className="text-[10px] text-emerald-400 font-bold flex items-center gap-1">
                      <CheckCircle2 className="w-3 h-3" /> {evt.status}
                    </span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};
