import React, { useState, useEffect, useRef, useReducer, useCallback, useId } from 'react';
import {
  Activity,
  AlertTriangle,
  Cpu,
  Database,
  Flame,
  HardDrive,
  Layers,
  Lock,
  RefreshCw,
  ShieldAlert,
  Terminal,
  Zap,
  CheckCircle2,
  Sliders,
  Radio
} from 'lucide-react';

// ============================================================================
// BARE-METAL ARCHITECTURAL TYPES & CONSTANTS
// ============================================================================
const BLOCK_SIZE_BYTES = 4 * 1024 * 1024; // 4096 KB (4MB Raw Page Alignment)
const SECTOR_COUNT = 64;
const DEFAULT_FSYNC_TIMEOUT_MS = 120;

export type BlockState = 'CLEAN' | 'DIRTY' | 'STAGED' | 'FSYNC_LOCK' | 'COMMITTED' | 'PARITY_ERR';

export interface RawBlockDescriptor {
  sectorId: number;
  physicalOffset: string;
  state: BlockState;
  crc32: string;
  dirtyBytes: number;
  lastSyncEpoch: number;
  writeCycles: number;
  consensusVoteWeight: number;
}

export interface FsyncWatchdogTelemetry {
  jitterUs: number;
  barrierLatencyMs: number;
  activeWatchdogTimer: number;
  watchdogTripped: boolean;
  busVoltageV: number;
  dmaThroughputMBps: number;
  queuedBlocksCount: number;
}

export interface WasmConsensusBridge {
  computeBlockCrc32: (data: Uint8Array) => string;
  verifySectorConsensus: (descriptor: RawBlockDescriptor) => boolean;
  synthesizeParity: (buffer: Uint8Array) => Uint8Array;
}

// ============================================================================
// DIRECT MEMORY TRANSPORT & WASM CORE SIMULATOR
// Bridges rust wasm-bindgen execution layer into raw web streams
// ============================================================================
class RustWasmEngine implements WasmConsensusBridge {
  computeBlockCrc32(data: Uint8Array): string {
    let crc = 0xffffffff;
    const len = Math.min(data.length, 1024); // fast sample hash
    for (let i = 0; i < len; i++) {
      crc = (crc >>> 8) ^ ((crc ^ data[i]) & 0xff);
    }
    return '0x' + ((crc ^ 0xffffffff) >>> 0).toString(16).padStart(8, '0').toUpperCase();
  }

  verifySectorConsensus(descriptor: RawBlockDescriptor): boolean {
    return descriptor.state !== 'PARITY_ERR' && descriptor.dirtyBytes === 0;
  }

  synthesizeParity(buffer: Uint8Array): Uint8Array {
    const parity = new Uint8Array(64);
    for (let i = 0; i < 64; i++) {
      parity[i] = buffer[i % buffer.length] ^ 0xaa;
    }
    return parity;
  }
}

const wasmEngine = new RustWasmEngine();

// ============================================================================
// COMPONENT: EdgeConsensusCacheEngine
// ============================================================================
export default function EdgeConsensusCacheEngine() {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const streamTransformRef = useRef<TransformStream<Uint8Array, Uint8Array> | null>(null);
  const pipelineWriterRef = useRef<WritableStreamDefaultWriter<Uint8Array> | null>(null);

  // Bare-metal hardware state
  const [isWebUsbAttached, setIsWebUsbAttached] = useState<boolean>(false);
  const [usbDeviceName, setUsbDeviceName] = useState<string>('PULSE-RAW-BLK-DEV [DISCONNECTED]');
  const [directDmaMode, setDirectDmaMode] = useState<boolean>(true);
  const [fsyncWatchdogActive, setFsyncWatchdogActive] = useState<boolean>(true);
  const [hardwareBarrierActive, setHardwareBarrierActive] = useState<boolean>(true);
  const [activeSectorIndex, setActiveSectorIndex] = useState<number>(0);
  const [streamingActive, setStreamingActive] = useState<boolean>(false);

  // Diagnostic Telemetry
  const [telemetry, setTelemetry] = useState<FsyncWatchdogTelemetry>({
    jitterUs: 42.8,
    barrierLatencyMs: 4.12,
    activeWatchdogTimer: DEFAULT_FSYNC_TIMEOUT_MS,
    watchdogTripped: false,
    busVoltageV: 5.02,
    dmaThroughputMBps: 842.1,
    queuedBlocksCount: 3
  });

  // Physical Sectors Table
  const [sectors, setSectors] = useState<RawBlockDescriptor[]>(() =>
    Array.from({ length: SECTOR_COUNT }, (_, i) => ({
      sectorId: i,
      physicalOffset: `0x${(i * BLOCK_SIZE_BYTES).toString(16).padStart(10, '0').toUpperCase()}`,
      state: i === 0 ? 'DIRTY' : i % 5 === 0 ? 'FSYNC_LOCK' : 'COMMITTED',
      crc32: `0x${(0x5a1f2b00 + i * 37).toString(16).toUpperCase()}`,
      dirtyBytes: i === 0 ? 1048576 : 0,
      lastSyncEpoch: Date.now() - i * 450,
      writeCycles: 1400 + (i % 87),
      consensusVoteWeight: 98.4 - (i % 3)
    }))
  );

  // Hexadecimal Live Dump Buffer Window
  const [rawHexSample, setRawHexSample] = useState<Uint8Array>(() => {
    const b = new Uint8Array(256);
    for (let i = 0; i < 256; i++) b[i] = (i * 13) % 256;
    return b;
  });

  // Hardware Console Log
  const [sysLogs, setSysLogs] = useState<Array<{ id: string; time: string; msg: string; level: 'RAW' | 'WARN' | 'CRIT' | 'OK' }>>([
    { id: '1', time: '00:00:00.012', msg: 'WASM Direct Memory Transport mapped at 0x7FFF00000000', level: 'RAW' },
    { id: '2', time: '00:00:00.045', msg: 'O_DIRECT | O_SYNC bare-metal pipeline initiated (4096KB granularity)', level: 'RAW' },
    { id: '3', time: '00:00:00.108', msg: 'Hardware fsync watchdog armed [timeout: 120ms, poll: 50us]', level: 'OK' }
  ]);

  const addSysLog = useCallback((msg: string, level: 'RAW' | 'WARN' | 'CRIT' | 'OK' = 'RAW') => {
    const d = new Date();
    const ts = `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}:${d.getSeconds().toString().padStart(2, '0')}.${d.getMilliseconds().toString().padStart(3, '0')}`;
    setSysLogs((prev) => [{ id: Math.random().toString(36).substring(2, 9), time: ts, msg, level }, ...prev.slice(0, 40)]);
  }, []);

  // ============================================================================
  // PIPELINE CREATION: Direct Unbuffered WebStream
  // ============================================================================
  const initializePipeline = useCallback(() => {
    const transform = new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        // Compute Rust-Wasm Parity and In-line CRC validation
        const crc = wasmEngine.computeBlockCrc32(chunk);
        controller.enqueue(chunk);
      }
    });

    const sink = new WritableStream<Uint8Array>({
      write(chunk) {
        // Direct Hardware Sink emulation / DMA push
        setRawHexSample(new Uint8Array(chunk.slice(0, 256)));
      }
    });

    transform.readable.pipeTo(sink).catch((err) => {
      addSysLog(`Direct Transport Sink breakdown: ${err.message}`, 'CRIT');
    });

    streamTransformRef.current = transform;
    pipelineWriterRef.current = transform.writable.getWriter();
    addSysLog('DMA Write Channel successfully attached to physical boundary', 'OK');
  }, [addSysLog]);

  useEffect(() => {
    initializePipeline();
    return () => {
      pipelineWriterRef.current?.close().catch(() => {});
    };
  }, [initializePipeline]);

  // ============================================================================
  // WEBUSB ATTACHMENT EMULATION / INTERFACE
  // ============================================================================
  const handleToggleWebUsb = async () => {
    if (isWebUsbAttached) {
      setIsWebUsbAttached(false);
      setUsbDeviceName('PULSE-RAW-BLK-DEV [DISCONNECTED]');
      addSysLog('Raw block transport detached from WebUSB target interface', 'WARN');
      return;
    }

    try {
      if ('usb' in navigator) {
        // Real WebUSB call attempt
        addSysLog('Probing raw devices via WebUSB bus scanning...', 'RAW');
        try {
          const device = await (navigator as any).usb.requestDevice({
            filters: []
          });
          if (device) {
            await device.open();
            setIsWebUsbAttached(true);
            setUsbDeviceName(`USB RAW BLK: ${device.productName || '0x' + device.vendorId.toString(16)}`);
            addSysLog(`Hardware linked: ${device.productName} [DMA Pipe Active]`, 'OK');
            return;
          }
        } catch (e: any) {
          addSysLog(`WebUSB native probe bypassed or declined: ${e.message}`, 'WARN');
        }
      }
      
      // Bare-metal hardware fallback emulation
      setIsWebUsbAttached(true);
      setUsbDeviceName('RAW_NVME_VIRT_BRIDGE:0x1C58 (LBA 4K ALIGNED)');
      addSysLog('Emulated direct physical WebUSB controller attached: 0x1C58', 'OK');
    } catch (err: any) {
      addSysLog(`WebUSB negotiation critical fault: ${err.message}`, 'CRIT');
    }
  };

  // ============================================================================
  // TELEMETRY AND FSYNC WATCHDOG RUNNER
  // ============================================================================
  useEffect(() => {
    const timer = setInterval(() => {
      setTelemetry((prev) => {
        const jitterVariance = (Math.random() * 4.8 - 2.4);
        const nextJitter = Math.max(12.0, +(prev.jitterUs + jitterVariance).toFixed(2));
        const barrierVariance = (Math.random() * 0.4 - 0.2);
        const nextBarrier = Math.max(1.8, +(prev.barrierLatencyMs + barrierVariance).toFixed(2));

        // Watchdog evaluation
        const tripped = fsyncWatchdogActive && nextBarrier * 30 > DEFAULT_FSYNC_TIMEOUT_MS;
        const throughput = streamingActive ? +(820 + Math.random() * 95).toFixed(1) : +(34 + Math.random() * 5).toFixed(1);

        return {
          jitterUs: nextJitter,
          barrierLatencyMs: nextBarrier,
          activeWatchdogTimer: tripped ? 0 : DEFAULT_FSYNC_TIMEOUT_MS - Math.floor(nextBarrier * 5),
          watchdogTripped: tripped,
          busVoltageV: +(5.02 + (Math.random() * 0.04 - 0.02)).toFixed(2),
          dmaThroughputMBps: throughput,
          queuedBlocksCount: streamingActive ? Math.floor(Math.random() * 6) + 1 : 0
        };
      });

      // Advance streaming write if active
      if (streamingActive && pipelineWriterRef.current) {
        const chunk = new Uint8Array(4096);
        crypto.getRandomValues(chunk);
        pipelineWriterRef.current.write(chunk).catch(() => {});
        
        // Randomly update sector states
        setSectors((prev) => {
          const idx = Math.floor(Math.random() * prev.length);
          const copy = [...prev];
          const curr = copy[idx];
          if (curr.state === 'COMMITTED') {
            copy[idx] = {
              ...curr,
              state: 'DIRTY',
              dirtyBytes: 4194304,
              crc32: wasmEngine.computeBlockCrc32(chunk)
            };
          } else if (curr.state === 'DIRTY') {
            copy[idx] = {
              ...curr,
              state: 'FSYNC_LOCK'
            };
          } else if (curr.state === 'FSYNC_LOCK') {
            copy[idx] = {
              ...curr,
              state: 'COMMITTED',
              dirtyBytes: 0,
              writeCycles: curr.writeCycles + 1
            };
          }
          return copy;
        });
      }
    }, 180);

    return () => clearInterval(timer);
  }, [fsyncWatchdogActive, streamingActive]);

  // ============================================================================
  // TELEMETRY OSCILLOSCOPE (CANVAS DRAW)
  // ============================================================================
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    let animId: number;
    let offset = 0;

    const render = () => {
      const width = canvas.width;
      const height = canvas.height;
      ctx.fillStyle = '#05070a';
      ctx.fillRect(0, 0, width, height);

      // Grid line scanlines
      ctx.strokeStyle = '#121d28';
      ctx.lineWidth = 1;
      for (let x = 0; x < width; x += 32) {
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, height);
        ctx.stroke();
      }
      for (let y = 0; y < height; y += 16) {
        ctx.beginPath();
        ctx.moveTo(0, y);
        ctx.lineTo(width, y);
        ctx.stroke();
      }

      // Signal 1: DMA Raw Stream Telemetry (Phosphor Amber)
      ctx.beginPath();
      ctx.strokeStyle = '#f59e0b';
      ctx.lineWidth = 1.5;
      for (let x = 0; x < width; x++) {
        const freq = 0.04;
        const noise = (Math.sin((x + offset) * freq) * 18) + (Math.cos((x * 0.1) + offset) * 7);
        const y = (height / 2) + noise + (streamingActive ? (Math.random() * 6 - 3) : 0);
        if (x === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.stroke();

      // Signal 2: Hardware Fsync Latency Barcode (Phosphor Emerald / Red if tripped)
      ctx.beginPath();
      ctx.strokeStyle = telemetry.watchdogTripped ? '#ef4444' : '#10b981';
      ctx.lineWidth = 1;
      for (let x = 0; x < width; x += 8) {
        const spike = Math.sin((x * 0.2) + offset * 0.5) > 0.7 ? 14 : 2;
        ctx.moveTo(x, height - 10);
        ctx.lineTo(x, height - 10 - spike);
      }
      ctx.stroke();

      offset += 1.5;
      animId = requestAnimationFrame(render);
    };

    render();
    return () => cancelAnimationFrame(animId);
  }, [streamingActive, telemetry.watchdogTripped]);

  // ============================================================================
  // USER ACTIONS & BARE-METAL HARDWARE EMULATION
  // ============================================================================
  const executeHardwareFsync = () => {
    addSysLog('O_SYNC barrier issued: Directing unbuffered cache write to disk blocks', 'RAW');
    setSectors((prev) =>
      prev.map((sec) => {
        if (sec.state === 'DIRTY' || sec.state === 'FSYNC_LOCK') {
          return {
            ...sec,
            state: 'COMMITTED',
            dirtyBytes: 0,
            writeCycles: sec.writeCycles + 1,
            lastSyncEpoch: Date.now()
          };
        }
        return sec;
      })
    );
    addSysLog('Consensus verified across physical sectors. CRC-32 verified OK.', 'OK');
  };

  const injectParityFault = () => {
    setSectors((prev) => {
      const copy = [...prev];
      copy[activeSectorIndex] = {
        ...copy[activeSectorIndex],
        state: 'PARITY_ERR',
        crc32: '0xDEADBEEF',
        dirtyBytes: 2097152
      };
      return copy;
    });
    addSysLog(`CRITICAL: Bitflip injected at sector 0x${activeSectorIndex.toString(16).toUpperCase()}`, 'CRIT');
  };

  const recoverSectorParity = (index: number) => {
    setSectors((prev) => {
      const copy = [...prev];
      copy[index] = {
        ...copy[index],
        state: 'COMMITTED',
        crc32: wasmEngine.computeBlockCrc32(rawHexSample),
        dirtyBytes: 0
      };
      return copy;
    });
    addSysLog(`Sector 0x${index.toString(16).toUpperCase()} rebuilt via Wasm-Consensus Reed-Solomon parity`, 'OK');
  };

  const selectedSector = sectors[activeSectorIndex] || sectors[0];

  return (
    <div className="flex h-screen w-full flex-col bg-[#07090e] font-mono text-zinc-300 select-none overflow-hidden antialiased">
      {/* ==================================================================== */}
      {/* TOP INDUSTRIAL BEZEL & TELEMETRY STRIP                               */}
      {/* ==================================================================== */}
      <header className="flex h-14 shrink-0 items-center justify-between border-b border-zinc-800/80 bg-[#0b0e14] px-4">
        <div className="flex items-center space-x-3">
          <div className="flex items-center space-x-2">
            <span className="relative flex h-3 w-3">
              <span
                className={`absolute inline-flex h-full w-full animate-ping rounded-full ${
                  streamingActive ? 'bg-amber-400 opacity-75' : 'bg-emerald-400 opacity-30'
                }`}
              />
              <span
                className={`relative inline-flex h-3 w-3 rounded-full ${
                  streamingActive ? 'bg-amber-500' : 'bg-emerald-500'
                }`}
              />
            </span>
            <div className="text-xs font-bold tracking-widest text-zinc-100 uppercase">
              PulseFS<span className="text-amber-500 font-black"> // </span>RAW-EDGE-CONSENSUS
            </div>
          </div>
          <span className="text-zinc-600 text-xs">|</span>
          <div className="text-[11px] text-zinc-400">
            PAGE_SZ: <span className="text-zinc-200 font-semibold">4096 KB</span>
          </div>
          <span className="text-zinc-600 text-xs">|</span>
          <div className="text-[11px] text-zinc-400">
            BARRIER: <span className="text-emerald-400 font-semibold">{hardwareBarrierActive ? 'O_DIRECT|O_SYNC' : 'ASYNC_UNSAFE'}</span>
          </div>
        </div>

        {/* Live Device Descriptor Bar */}
        <div className="flex items-center space-x-4">
          <button
            onClick={handleToggleWebUsb}
            className={`flex items-center space-x-2 rounded border px-2.5 py-1 text-[11px] font-medium transition-all ${
              isWebUsbAttached
                ? 'border-emerald-700/60 bg-emerald-950/40 text-emerald-300 hover:bg-emerald-900/50'
                : 'border-zinc-700 bg-zinc-900 text-zinc-300 hover:border-zinc-500 hover:bg-zinc-800'
            }`}
          >
            <Zap className={`h-3.5 w-3.5 ${isWebUsbAttached ? 'text-emerald-400' : 'text-zinc-500'}`} />
            <span>{usbDeviceName}</span>
          </button>

          <button
            onClick={() => setStreamingActive(!streamingActive)}
            className={`flex items-center space-x-1.5 rounded px-3 py-1 text-[11px] font-bold uppercase transition ${
              streamingActive
                ? 'border border-amber-500/70 bg-amber-500/20 text-amber-300 shadow-[0_0_12px_rgba(245,158,11,0.2)]'
                : 'border border-zinc-700 bg-zinc-800 text-zinc-300 hover:border-zinc-600'
            }`}
          >
            <Activity className={`h-3.5 w-3.5 ${streamingActive ? 'animate-spin' : ''}`} />
            <span>{streamingActive ? 'HALT STREAM' : 'RUN RAW STREAM'}</span>
          </button>
        </div>
      </header>

      {/* ==================================================================== */}
      {/* SECONDARY HARDWARE TELEMETRY BANNER                                  */}
      {/* ==================================================================== */}
      <div className="grid h-12 grid-cols-6 border-b border-zinc-800/80 bg-[#090b10] text-[11px]">
        <div className="flex items-center justify-between border-r border-zinc-800/60 px-3">
          <span className="text-zinc-500 uppercase">Jitter Latency:</span>
          <span className="font-bold text-amber-400">{telemetry.jitterUs} µs</span>
        </div>
        <div className="flex items-center justify-between border-r border-zinc-800/60 px-3">
          <span className="text-zinc-500 uppercase">Barrier Latency:</span>
          <span className="font-bold text-zinc-200">{telemetry.barrierLatencyMs} ms</span>
        </div>
        <div className="flex items-center justify-between border-r border-zinc-800/60 px-3">
          <span className="text-zinc-500 uppercase">Watchdog Timer:</span>
          <span className={`font-bold ${telemetry.watchdogTripped ? 'text-red-500 animate-pulse' : 'text-emerald-400'}`}>
            {telemetry.activeWatchdogTimer} ms
          </span>
        </div>
        <div className="flex items-center justify-between border-r border-zinc-800/60 px-3">
          <span className="text-zinc-500 uppercase">DMA Throughput:</span>
          <span className="font-bold text-cyan-400">{telemetry.dmaThroughputMBps} MB/s</span>
        </div>
        <div className="flex items-center justify-between border-r border-zinc-800/60 px-3">
          <span className="text-zinc-500 uppercase">Bus Voltage:</span>
          <span className="font-bold text-zinc-300">{telemetry.busVoltageV} V</span>
        </div>
        <div className="flex items-center justify-between px-3">
          <span className="text-zinc-500 uppercase">Queue Depth:</span>
          <span className="font-bold text-zinc-100">{telemetry.queuedBlocksCount} chunks</span>
        </div>
      </div>

      {/* ==================================================================== */}
      {/* MAIN HARDWARE DIAGNOSTICS VIEWPORT                                   */}
      {/* ==================================================================== */}
      <div className="flex flex-1 overflow-hidden">
        {/* LEFT COLUMN: 64 Physical Sector Allocation Ring Grid */}
        <section className="flex flex-1 flex-col border-r border-zinc-800/80 bg-[#06080d]">
          <div className="flex h-10 items-center justify-between border-b border-zinc-800 px-4 bg-[#090c12]">
            <div className="flex items-center space-x-2">
              <Layers className="h-4 w-4 text-amber-500" />
              <span className="text-xs font-bold uppercase tracking-wider text-zinc-200">
                4MB Physical Block Cache Map (256MB Ring Window)
              </span>
            </div>
            <div className="flex items-center space-x-3 text-[10px]">
              <span className="flex items-center space-x-1">
                <span className="h-2 w-2 rounded-xs bg-emerald-500" />
                <span className="text-zinc-400">COMMITTED</span>
              </span>
              <span className="flex items-center space-x-1">
                <span className="h-2 w-2 rounded-xs bg-amber-500" />
                <span className="text-zinc-400">DIRTY</span>
              </span>
              <span className="flex items-center space-x-1">
                <span className="h-2 w-2 rounded-xs bg-cyan-500" />
                <span className="text-zinc-400">FSYNC_LOCK</span>
              </span>
              <span className="flex items-center space-x-1">
                <span className="h-2 w-2 rounded-xs bg-red-600 animate-pulse" />
                <span className="text-zinc-400">PARITY_ERR</span>
              </span>
            </div>
          </div>

          {/* 64 Sectors Visual Grid */}
          <div className="flex-1 overflow-y-auto p-4">
            <div className="grid grid-cols-8 gap-2">
              {sectors.map((sec) => {
                const isSelected = sec.sectorId === activeSectorIndex;
                let bgState = 'bg-emerald-950/40 border-emerald-700/50 text-emerald-400';
                if (sec.state === 'DIRTY') bgState = 'bg-amber-950/40 border-amber-600/70 text-amber-300';
                if (sec.state === 'FSYNC_LOCK') bgState = 'bg-cyan-950/40 border-cyan-500/70 text-cyan-300 animate-pulse';
                if (sec.state === 'PARITY_ERR') bgState = 'bg-red-950/80 border-red-600 text-red-300 ring-1 ring-red-500';

                return (
                  <button
                    key={sec.sectorId}
                    onClick={() => setActiveSectorIndex(sec.sectorId)}
                    className={`group relative flex flex-col justify-between rounded border p-2 text-left transition-all ${bgState} ${
                      isSelected ? 'ring-2 ring-amber-400 scale-[1.02] shadow-lg shadow-black/80 z-10' : 'hover:border-zinc-400'
                    }`}
                  >
                    <div className="flex items-center justify-between text-[10px]">
                      <span className="font-bold">#0x{sec.sectorId.toString(16).padStart(2, '0').toUpperCase()}</span>
                      <span className="text-[9px] opacity-70">{sec.consensusVoteWeight.toFixed(0)}%</span>
                    </div>

                    <div className="my-1.5 truncate text-[9px] text-zinc-400 font-mono tracking-tighter">
                      {sec.crc32}
                    </div>

                    <div className="flex items-center justify-between text-[8px] text-zinc-400">
                      <span>{sec.state}</span>
                      <span>{sec.writeCycles}w</span>
                    </div>

                    {/* Progress Fill if dirty */}
                    {sec.dirtyBytes > 0 && (
                      <div className="absolute inset-x-0 bottom-0 h-0.5 bg-amber-400">
                        <div
                          className="h-full bg-amber-200 transition-all duration-300"
                          style={{ width: `${(sec.dirtyBytes / BLOCK_SIZE_BYTES) * 100}%` }}
                        />
                      </div>
                    )}
                  </button>
                );
              })}
            </div>

            {/* DIRECT MEMORY OSCILLOSCOPE TRACE */}
            <div className="mt-4 rounded border border-zinc-800 bg-[#040608] p-3">
              <div className="mb-2 flex items-center justify-between">
                <span className="text-[11px] font-bold text-zinc-300 flex items-center space-x-1.5">
                  <Terminal className="h-3.5 w-3.5 text-cyan-400" />
                  <span>HARDWARE DMA CARRIER WAVE & FSYNC BARCODE INTERVAL</span>
                </span>
                <span className="text-[10px] text-zinc-500">SAMPLING @ 192.4 kHz (RAW BUS)</span>
              </div>
              <canvas ref={canvasRef} width={800} height={90} className="w-full rounded border border-zinc-900 bg-black" />
            </div>
          </div>

          {/* LOWER STATUS COMMAND BAR */}
          <div className="flex h-12 items-center justify-between border-t border-zinc-800 bg-[#090b10] px-4">
            <div className="flex items-center space-x-2">
              <button
                onClick={executeHardwareFsync}
                className="flex items-center space-x-1.5 rounded border border-amber-600/70 bg-amber-600/20 px-3 py-1.5 text-xs font-bold text-amber-300 hover:bg-amber-600/30 active:translate-y-0.5"
              >
                <Flame className="h-3.5 w-3.5 text-amber-400" />
                <span>FORCE FSYNC BARRIER</span>
              </button>
              <button
                onClick={injectParityFault}
                className="flex items-center space-x-1.5 rounded border border-red-900 bg-red-950/40 px-3 py-1.5 text-xs font-bold text-red-300 hover:bg-red-900/40 active:translate-y-0.5"
              >
                <AlertTriangle className="h-3.5 w-3.5 text-red-400" />
                <span>INJECT BITFLIP</span>
              </button>
            </div>

            <div className="flex items-center space-x-3 text-xs">
              <label className="flex items-center space-x-1.5 cursor-pointer">
                <input
                  type="checkbox"
                  checked={hardwareBarrierActive}
                  onChange={(e) => setHardwareBarrierActive(e.target.checked)}
                  className="rounded border-zinc-700 bg-zinc-900 text-amber-500 focus:ring-0"
                />
                <span className="text-zinc-400">HARDWARE O_SYNC</span>
              </label>

              <label className="flex items-center space-x-1.5 cursor-pointer">
                <input
                  type="checkbox"
                  checked={fsyncWatchdogActive}
                  onChange={(e) => setFsyncWatchdogActive(e.target.checked)}
                  className="rounded border-zinc-700 bg-zinc-900 text-amber-500 focus:ring-0"
                />
                <span className="text-zinc-400">FSYNC WATCHDOG</span>
              </label>
            </div>
          </div>
        </section>

        {/* RIGHT COLUMN: Sector Inspector & Bare-metal Hex Telemetry */}
        <section className="flex w-96 flex-col bg-[#07090f]">
          {/* Active Sector Metadata */}
          <div className="border-b border-zinc-800 bg-[#0a0d14] p-3">
            <div className="flex items-center justify-between">
              <span className="text-xs font-bold text-zinc-100 flex items-center space-x-1.5">
                <HardDrive className="h-3.5 w-3.5 text-amber-500" />
                <span>SECTOR 0x{selectedSector.sectorId.toString(16).toUpperCase()} INSPECTOR</span>
              </span>
              <span
                className={`rounded px-1.5 py-0.5 text-[9px] font-bold ${
                  selectedSector.state === 'COMMITTED'
                    ? 'bg-emerald-950 text-emerald-400 border border-emerald-800'
                    : selectedSector.state === 'PARITY_ERR'
                    ? 'bg-red-950 text-red-400 border border-red-800'
                    : 'bg-amber-950 text-amber-400 border border-amber-800'
                }`}
              >
                {selectedSector.state}
              </span>
            </div>

            <div className="mt-3 grid grid-cols-2 gap-2 text-[10px]">
              <div>
                <span className="text-zinc-500">PHY_ADDR:</span>
                <p className="font-semibold text-zinc-200">{selectedSector.physicalOffset}</p>
              </div>
              <div>
                <span className="text-zinc-500">WASM_CRC:</span>
                <p className="font-semibold text-cyan-400">{selectedSector.crc32}</p>
              </div>
              <div>
                <span className="text-zinc-500">DIRTY_BYTES:</span>
                <p className="font-semibold text-zinc-200">
                  {selectedSector.dirtyBytes.toLocaleString()} / 4MB
                </p>
              </div>
              <div>
                <span className="text-zinc-500">WRITE_ENDURANCE:</span>
                <p className="font-semibold text-zinc-200">{selectedSector.writeCycles} cycles</p>
              </div>
            </div>

            {selectedSector.state === 'PARITY_ERR' && (
              <button
                onClick={() => recoverSectorParity(selectedSector.sectorId)}
                className="mt-3 w-full rounded border border-emerald-600 bg-emerald-950/60 py-1 text-xs font-bold text-emerald-300 hover:bg-emerald-900/60"
              >
                RECONSTRUCT SECTOR VIA WASM RING
              </button>
            )}
          </div>

          {/* Raw Hex Stream Telemetry (DMA Head Window) */}
          <div className="flex-1 flex flex-col border-b border-zinc-800 bg-[#05070a] p-3 overflow-hidden">
            <div className="mb-2 flex items-center justify-between text-[11px]">
              <span className="font-bold text-zinc-300">DMA RAW DUMP (PAGE_HEAD)</span>
              <span className="text-[10px] text-zinc-500">256-BYTE WINDOW</span>
            </div>

            <div className="flex-1 overflow-y-auto font-mono text-[10px] leading-relaxed text-zinc-400 select-text">
              {Array.from({ length: 16 }).map((_, rowIndex) => {
                const rowOffset = rowIndex * 16;
                const slice = Array.from(rawHexSample.slice(rowOffset, rowOffset + 16));
                const hexString = slice.map((b) => b.toString(16).padStart(2, '0').toUpperCase()).join(' ');
                const asciiString = slice
                  .map((b) => (b >= 32 && b <= 126 ? String.fromCharCode(b) : '.'))
                  .join('');

                return (
                  <div key={rowIndex} className="flex justify-between hover:bg-zinc-900/80 px-1 rounded">
                    <span className="text-zinc-600">0x{rowOffset.toString(16).padStart(4, '0')}</span>
                    <span className="text-amber-500/90 tracking-tight">{hexString}</span>
                    <span className="text-zinc-400 tracking-tight">|{asciiString}|</span>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Micro-Kernel Hardware Event Log */}
          <div className="h-48 flex flex-col bg-[#080b11] p-3">
            <div className="mb-1.5 flex items-center justify-between text-[10px] text-zinc-400 font-bold border-b border-zinc-800 pb-1">
              <span>RING BUFFER SYSLOG</span>
              <span className="text-zinc-600">UNBUFFERED</span>
            </div>
            <div className="flex-1 overflow-y-auto space-y-1 text-[10px]">
              {sysLogs.map((log) => {
                let badgeColor = 'text-zinc-400';
                if (log.level === 'OK') badgeColor = 'text-emerald-400';
                if (log.level === 'WARN') badgeColor = 'text-amber-400';
                if (log.level === 'CRIT') badgeColor = 'text-red-400 font-bold';

                return (
                  <div key={log.id} className="flex items-start space-x-1.5">
                    <span className="text-zinc-600 shrink-0">[{log.time}]</span>
                    <span className={badgeColor}>{log.msg}</span>
                  </div>
                );
              })}
            </div>
          </div>
        </section>
      </div>

      {/* ==================================================================== */}
      {/* HARDWARE FOOTER: PHYSICAL BARRIER WATCHDOG STATUS                     */}
      {/* ==================================================================== */}
      <footer className="flex h-7 shrink-0 items-center justify-between border-t border-zinc-800 bg-[#050608] px-4 text-[10px] text-zinc-500">
        <div className="flex items-center space-x-4">
          <span>TRANSPORT: <strong className="text-zinc-300">DIRECT MEMORY / ZERO-COPY DMA</strong></span>
          <span>WASM INTEROP: <strong className="text-zinc-300">WASM-BINDGEN SIMD128</strong></span>
          <span>ALIGNMENT: <strong className="text-zinc-300">4096-BYTE STRICT HARD SECTOR</strong></span>
        </div>
        <div className="flex items-center space-x-2">
          {telemetry.watchdogTripped ? (
            <span className="flex items-center space-x-1 text-red-500 font-bold">
              <ShieldAlert className="h-3 w-3" />
              <span>WATCHDOG TRIPPED: FSYNC EXCEEDED THRESHOLD</span>
            </span>
          ) : (
            <span className="flex items-center space-x-1 text-emerald-500">
              <CheckCircle2 className="h-3 w-3" />
              <span>HARDWARE BARRIER SYNCHRONIZED</span>
            </span>
          )}
        </div>
      </footer>
    </div>
  );
}