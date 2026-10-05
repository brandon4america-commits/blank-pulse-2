import React from 'react';
import EdgeConsensusCacheEngine from './components/EdgeConsensusCacheEngine';
import DirecttoBlock4MBStreamPipeline from './components/DirecttoBlock4MBStreamPipeline';
import AtomicFsyncHardwareCommitWatchdog from './components/AtomicFsyncHardwareCommitWatchdog';
import HybridISOIngestionPartitioningMatrix from './components/HybridISOIngestionPartitioningMatrix';
import RealtimeRawIOProgressStreamer from './components/RealtimeRawIOProgressStreamer';

export default function App() {
  return (
    <div className="min-h-screen bg-slate-950 text-slate-200 p-8 font-sans">
      <header className="max-w-6xl mx-auto mb-10 pb-6 border-b border-slate-800">
        <div className="inline-block px-3 py-1 bg-indigo-500/10 border border-indigo-500/20 text-indigo-400 text-xs font-mono font-bold rounded-full mb-3">
          Bare-Metal Inception Era
        </div>
        <h1 className="text-4xl font-black text-white tracking-tight">PulseFS: Raw Block Provisioner</h1>
        <p className="text-slate-400 mt-2 max-w-2xl text-sm leading-relaxed">Mutated into a bare-metal raw block streaming architecture inspired by low-level device image flashing. Bridges the micro-kernel foundation with direct unbuffered 4MB block I/O pipelining, hardware fsync synchronization watchdogs, and live byte-stream telemetry.</p>
      </header>

      <main className="max-w-6xl mx-auto grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        <EdgeConsensusCacheEngine />
        <DirecttoBlock4MBStreamPipeline />
        <AtomicFsyncHardwareCommitWatchdog />
        <HybridISOIngestionPartitioningMatrix />
        <RealtimeRawIOProgressStreamer />
      </main>
    </div>
  );
}
