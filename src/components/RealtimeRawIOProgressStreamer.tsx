import React, {
  useState,
  useEffect,
  useRef,
  useCallback,
  useMemo,
  useTransition,
} from "react";

// ============================================================================
// BARE-METAL TYPES & WASM DIRECT MEMORY INTERFACES
// ============================================================================

export type BlockState =
  | "STANDBY"
  | "STREAMING"
  | "SYNC_WAIT"
  | "COMMITTED"
  | "CORRUPT";

export interface BlockTelemetry {
  readonly id: number;
  readonly lba: number;
  readonly sizeBytes: number;
  status: BlockState;
  crc32: string;
  latencyMicros: number;
  committedAt: number | null;
}

export interface HardwareWatchdogState {
  fsyncTimeoutArmed: boolean;
  heartbeatDeadlineMs: number;
  uncommittedBytes: number;
  controllerTemperatureC: number;
  voltageRailV: number;
  dmaLockEngaged: boolean;
}

export interface IOPerfMetrics {
  currentThroughputMBs: number;
  instantIOPS: number;
  averageLatencyUs: number;
  jitterUs: number;
  totalTransferredBytes: bigint;
  droppedPackets: number;
}

// Simulated Wasm memory linear layout pointer
interface WasmMemoryBridge {
  buffer: ArrayBuffer;
  ringBufferPtr: number;
  capacityBlocks: number;
  writePointer: number;
  syncPointer: number;
}

// ============================================================================
// CONSTANTS (Bare-Metal 4MiB Chunks, Ring Sizes, Timing)
// ============================================================================

const BLOCK_SIZE_BYTES = 4 * 1024 * 1024; // Strict 4MB aligned pages
const TOTAL_PIPELINE_BLOCKS = 32; // Active circular ring buffer
const TOTAL_STORAGE_CAPACITY_BYTES =
  BigInt(BLOCK_SIZE_BYTES) * BigInt(TOTAL_PIPELINE_BLOCKS);
const CRC32_TABLE = new Uint32Array(256);

// Precompute CRC32 Table for client-side raw data stream integrity
for (let i = 0; i < 256; i++) {
  let c = i;
  for (let j = 0; j < 8; j++) {
    c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
  }
  CRC32_TABLE[i] = c;
}

function computeCrc32(buffer: Uint8Array, offset: number, length: number): string {
  let crc = 0xffffffff;
  for (let i = offset; i < offset + length; i++) {
    crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ buffer[i]) & 0xff];
  }
  return ((crc ^ 0xffffffff) >>> 0).toString(16).toUpperCase().padStart(8, "0");
}

// ============================================================================
// COMPONENT IMPLEMENTATION
// ============================================================================

export default function RawIOProgressStreamer(): React.ReactElement {
  const [, startTransition] = useTransition();

  // Low-level Hardware Stream States
  const [isStreaming, setIsStreaming] = useState<boolean>(false);
  const [isWasmHydrated, setIsWasmHydrated] = useState<boolean>(false);
  const [selectedBlockIdx, setSelectedBlockIdx] = useState<number | null>(0);
  const [activeTab, setActiveTab] = useState<"MATRIX" | "HEX_DUMP" | "WATCHDOG">("MATRIX");

  // Telemetry Aggregates
  const [metrics, setMetrics] = useState<IOPerfMetrics>({
    currentThroughputMBs: 0,
    instantIOPS: 0,
    averageLatencyUs: 142.4,
    jitterUs: 18.2,
    totalTransferredBytes: 0n,
    droppedPackets: 0,
  });

  const [watchdog, setWatchdog] = useState<HardwareWatchdogState>({
    fsyncTimeoutArmed: true,
    heartbeatDeadlineMs: 50,
    uncommittedBytes: 0,
    controllerTemperatureC: 48.2,
    voltageRailV: 3.31,
    dmaLockEngaged: true,
  });

  // Physical Block Grid Registry
  const [blocks, setBlocks] = useState<BlockTelemetry[]>(() =>
    Array.from({ length: TOTAL_PIPELINE_BLOCKS }, (_, i) => ({
      id: i,
      lba: i * 8192, // 4MB = 8192 * 512b sectors
      sizeBytes: BLOCK_SIZE_BYTES,
      status: "STANDBY",
      crc32: "00000000",
      latencyMicros: 0,
      committedAt: null,
    }))
  );

  // Direct Memory Ring Buffer references
  const wasmBridgeRef = useRef<WasmMemoryBridge | null>(null);
  const streamAbortControllerRef = useRef<AbortController | null>(null);
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const telemetryHistoryRef = useRef<number[]>(new Array(120).fill(0));
  const hexDumpBufferRef = useRef<Uint8Array>(new Uint8Array(256));

  // Initialize Bare-metal Wasm Linear Memory Buffer Mock
  useEffect(() => {
    try {
      const memoryCapacity = BLOCK_SIZE_BYTES * 2; // Double buffered staging pipeline
      const buffer = new ArrayBuffer(memoryCapacity);
      wasmBridgeRef.current = {
        buffer,
        ringBufferPtr: 0,
        capacityBlocks: TOTAL_PIPELINE_BLOCKS,
        writePointer: 0,
        syncPointer: 0,
      };

      // Seed initial inspection block
      const seedArray = new Uint8Array(buffer, 0, 256);
      for (let i = 0; i < 256; i++) {
        seedArray[i] = Math.floor(Math.random() * 256);
      }
      hexDumpBufferRef.current = seedArray;

      setIsWasmHydrated(true);
    } catch {
      setIsWasmHydrated(false);
    }

    return () => {
      if (streamAbortControllerRef.current) {
        streamAbortControllerRef.current.abort();
      }
    };
  }, []);

  // Oscilloscope Canvas Render Loop for Microsecond-level Jitter Telemetry
  useEffect(() => {
    let animFrameId: number;
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const render = () => {
      const w = canvas.width;
      const h = canvas.height;
      ctx.fillStyle = "#050706";
      ctx.fillRect(0, 0, w, h);

      // Grid lines
      ctx.strokeStyle = "rgba(16, 185, 129, 0.08)";
      ctx.lineWidth = 1;
      ctx.beginPath();
      for (let x = 0; x < w; x += 20) {
        ctx.moveTo(x, 0);
        ctx.lineTo(x, h);
      }
      for (let y = 0; y < h; y += 15) {
        ctx.moveTo(0, y);
        ctx.lineTo(w, y);
      }
      ctx.stroke();

      // Plot Throughput waveform
      const history = telemetryHistoryRef.current;
      ctx.strokeStyle = "#10b981";
      ctx.lineWidth = 1.5;
      ctx.shadowColor = "rgba(16, 185, 129, 0.8)";
      ctx.shadowBlur = 6;
      ctx.beginPath();

      const sliceWidth = w / (history.length - 1);
      for (let i = 0; i < history.length; i++) {
        const val = history[i]; // Normalized 0 - 1000 MB/s
        const y = h - (val / 1200) * (h - 8) - 4;
        if (i === 0) ctx.moveTo(0, y);
        else ctx.lineTo(i * sliceWidth, y);
      }
      ctx.stroke();
      ctx.shadowBlur = 0;

      animFrameId = requestAnimationFrame(render);
    };

    animFrameId = requestAnimationFrame(render);
    return () => cancelAnimationFrame(animFrameId);
  }, []);

  // Core Hardware Streaming Engine
  const startHardwareStream = useCallback(() => {
    if (isStreaming) return;

    const abortController = new AbortController();
    streamAbortControllerRef.current = abortController;
    setIsStreaming(true);

    let activeBlockCursor = 0;
    let accumulatedBytes = metrics.totalTransferredBytes;

    const workerInterval = window.setInterval(() => {
      if (abortController.signal.aborted) {
        clearInterval(workerInterval);
        return;
      }

      const baseIOPS = 2800 + Math.floor(Math.random() * 450);
      const latency = 110 + Math.random() * 40;
      const jitter = Math.random() * 12;
      const throughputMBs = +(latency > 140 ? 780 + Math.random() * 60 : 960 + Math.random() * 110).toFixed(2);

      telemetryHistoryRef.current.push(throughputMBs);
      telemetryHistoryRef.current.shift();

      accumulatedBytes += BigInt(BLOCK_SIZE_BYTES);

      // Mutate ring buffer states
      startTransition(() => {
        setBlocks((prev) => {
          const next = [...prev];
          const curr = { ...next[activeBlockCursor] };

          // Advance previous committed to clean
          const prevIdx = (activeBlockCursor - 1 + TOTAL_PIPELINE_BLOCKS) % TOTAL_PIPELINE_BLOCKS;
          next[prevIdx] = {
            ...next[prevIdx],
            status: "COMMITTED",
            committedAt: Date.now(),
          };

          // Mark current active chunk
          const simulatedPayload = new Uint8Array(32);
          crypto.getRandomValues(simulatedPayload);
          curr.status = "STREAMING";
          curr.crc32 = computeCrc32(simulatedPayload, 0, 32);
          curr.latencyMicros = +(latency * 10).toFixed(1);
          next[activeBlockCursor] = curr;

          return next;
        });

        setMetrics({
          currentThroughputMBs: throughputMBs,
          instantIOPS: baseIOPS,
          averageLatencyUs: +latency.toFixed(1),
          jitterUs: +jitter.toFixed(1),
          totalTransferredBytes: accumulatedBytes,
          droppedPackets: Math.random() > 0.98 ? 1 : 0,
        });

        setWatchdog((prev) => ({
          ...prev,
          uncommittedBytes: (activeBlockCursor + 1) * BLOCK_SIZE_BYTES,
          controllerTemperatureC: +(48.0 + Math.random() * 2.8).toFixed(1),
          voltageRailV: +(3.3 + (Math.random() - 0.5) * 0.04).toFixed(3),
        }));
      });

      activeBlockCursor = (activeBlockCursor + 1) % TOTAL_PIPELINE_BLOCKS;
    }, 180);
  }, [isStreaming, metrics.totalTransferredBytes]);

  const haltHardwareStream = useCallback(() => {
    if (streamAbortControllerRef.current) {
      streamAbortControllerRef.current.abort();
      streamAbortControllerRef.current = null;
    }
    setIsStreaming(false);
    setMetrics((prev) => ({
      ...prev,
      currentThroughputMBs: 0,
      instantIOPS: 0,
    }));
  }, []);

  const forceHardwareFsync = useCallback(() => {
    setBlocks((prev) =>
      prev.map((blk) => ({
        ...blk,
        status: blk.status === "STREAMING" ? "COMMITTED" : blk.status,
        committedAt: Date.now(),
      }))
    );
    setWatchdog((prev) => ({
      ...prev,
      uncommittedBytes: 0,
    }));
  }, []);

  // Format Large Bytes
  const formattedStorageProgress = useMemo(() => {
    const rawVal = Number(metrics.totalTransferredBytes) / 1024 / 1024;
    return rawVal.toLocaleString("en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }, [metrics.totalTransferredBytes]);

  return (
    <div className="flex flex-col w-full min-h-screen bg-[#070908] text-[#a4b3a8] font-mono select-none p-4 md:p-6 antialiased">
      {/* ================= TOP TELEMETRY RACK BAR ================= */}
      <header className="border border-[#1f2d24] bg-[#0c120e]/90 p-4 mb-4 backdrop-blur-md relative overflow-hidden">
        <div className="absolute top-0 left-0 w-1.5 h-full bg-[#10b981]" />
        
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center space-x-3">
            <div className="relative">
              <span className={`w-3 h-3 block rounded-full ${isStreaming ? "bg-emerald-400 animate-ping" : "bg-emerald-800"}`} />
              <span className={`w-3 h-3 block rounded-full absolute top-0 left-0 ${isStreaming ? "bg-emerald-500 shadow-[0_0_10px_#10b981]" : "bg-emerald-900"}`} />
            </div>
            <div>
              <div className="flex items-center space-x-2">
                <h1 className="text-emerald-400 text-sm tracking-widest font-bold uppercase">
                  PulseFS Raw Block Provisioner
                </h1>
                <span className="text-[10px] px-1.5 py-0.5 border border-emerald-900 bg-emerald-950/40 text-emerald-400 rounded-xs">
                  DMA DIRECT
                </span>
                <span className="text-[10px] px-1.5 py-0.5 border border-zinc-800 bg-zinc-900/60 text-zinc-400 font-mono">
                  WASM MEM 0x7FFF00
                </span>
              </div>
              <p className="text-xs text-[#526357] mt-0.5">
                4MiB Aligned Unbuffered Direct Block Pipeline | FSYNC Watchdog Active
              </p>
            </div>
          </div>

          {/* Core Hardware Action Buttons */}
          <div className="flex items-center space-x-2.5">
            {!isStreaming ? (
              <button
                type="button"
                onClick={startHardwareStream}
                className="cursor-pointer border border-[#10b981] bg-[#10b981]/15 hover:bg-[#10b981]/25 text-emerald-400 px-3.5 py-1.5 text-xs font-bold uppercase tracking-wider transition-colors duration-100 flex items-center space-x-2 shadow-[0_0_12px_rgba(16,185,129,0.15)] active:translate-y-0.5"
              >
                <svg className="w-3.5 h-3.5 fill-current" viewBox="0 0 24 24">
                  <path d="M8 5v14l11-7z" />
                </svg>
                <span>Engage Direct Stream</span>
              </button>
            ) : (
              <button
                type="button"
                onClick={haltHardwareStream}
                className="cursor-pointer border border-rose-500/60 bg-rose-950/20 hover:bg-rose-950/40 text-rose-400 px-3.5 py-1.5 text-xs font-bold uppercase tracking-wider transition-colors duration-100 flex items-center space-x-2 shadow-[0_0_12px_rgba(244,63,94,0.15)] active:translate-y-0.5"
              >
                <svg className="w-3.5 h-3.5 fill-current" viewBox="0 0 24 24">
                  <path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z" />
                </svg>
                <span>Halt Pipeline</span>
              </button>
            )}

            <button
              type="button"
              onClick={forceHardwareFsync}
              className="cursor-pointer border border-[#2b3d30] bg-[#121c15] hover:border-emerald-700 hover:text-emerald-300 text-zinc-300 px-3 py-1.5 text-xs uppercase tracking-wider transition-all duration-100 active:scale-95"
            >
              Hardware FSYNC
            </button>
          </div>
        </div>
      </header>

      {/* ================= PRIMARY TELEMETRY GAUGES ================= */}
      <section className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-2.5 mb-4">
        {/* Metric 1 */}
        <div className="border border-[#1a251e] bg-[#0b100d] p-3 flex flex-col justify-between">
          <span className="text-[10px] text-[#55695d] uppercase tracking-wider font-semibold">
            Raw Throughput
          </span>
          <div className="mt-1 flex items-baseline justify-between">
            <span className="text-xl md:text-2xl font-bold tracking-tight text-emerald-400">
              {metrics.currentThroughputMBs.toFixed(1)}
            </span>
            <span className="text-xs text-[#526357] font-semibold">MB/s</span>
          </div>
          <div className="w-full bg-[#141b16] h-1 mt-2 overflow-hidden">
            <div
              className="bg-emerald-500 h-full transition-all duration-200"
              style={{ width: `${Math.min(100, (metrics.currentThroughputMBs / 1200) * 100)}%` }}
            />
          </div>
        </div>

        {/* Metric 2 */}
        <div className="border border-[#1a251e] bg-[#0b100d] p-3 flex flex-col justify-between">
          <span className="text-[10px] text-[#55695d] uppercase tracking-wider font-semibold">
            Instant IOPS
          </span>
          <div className="mt-1 flex items-baseline justify-between">
            <span className="text-xl md:text-2xl font-bold tracking-tight text-emerald-400">
              {metrics.instantIOPS.toLocaleString()}
            </span>
            <span className="text-xs text-[#526357]">4K_EQ</span>
          </div>
          <div className="w-full bg-[#141b16] h-1 mt-2 overflow-hidden">
            <div
              className="bg-emerald-500 h-full transition-all duration-200"
              style={{ width: `${Math.min(100, (metrics.instantIOPS / 4000) * 100)}%` }}
            />
          </div>
        </div>

        {/* Metric 3 */}
        <div className="border border-[#1a251e] bg-[#0b100d] p-3 flex flex-col justify-between">
          <span className="text-[10px] text-[#55695d] uppercase tracking-wider font-semibold">
            I/O Jitter (99.9th)
          </span>
          <div className="mt-1 flex items-baseline justify-between">
            <span className="text-xl md:text-2xl font-bold tracking-tight text-amber-400">
              {metrics.jitterUs.toFixed(1)}
            </span>
            <span className="text-xs text-[#526357]">μs</span>
          </div>
          <div className="w-full bg-[#141b16] h-1 mt-2 overflow-hidden">
            <div
              className="bg-amber-500 h-full transition-all duration-200"
              style={{ width: `${Math.min(100, (metrics.jitterUs / 30) * 100)}%` }}
            />
          </div>
        </div>

        {/* Metric 4 */}
        <div className="border border-[#1a251e] bg-[#0b100d] p-3 flex flex-col justify-between">
          <span className="text-[10px] text-[#55695d] uppercase tracking-wider font-semibold">
            Uncommitted Dirty
          </span>
          <div className="mt-1 flex items-baseline justify-between">
            <span className="text-xl md:text-2xl font-bold tracking-tight text-zinc-200">
              {(watchdog.uncommittedBytes / (1024 * 1024)).toFixed(0)}
            </span>
            <span className="text-xs text-[#526357]">MiB / RAW</span>
          </div>
          <div className="w-full bg-[#141b16] h-1 mt-2 overflow-hidden">
            <div
              className="bg-zinc-400 h-full transition-all duration-200"
              style={{ width: `${Math.min(100, (watchdog.uncommittedBytes / (TOTAL_PIPELINE_BLOCKS * BLOCK_SIZE_BYTES)) * 100)}%` }}
            />
          </div>
        </div>

        {/* Metric 5 */}
        <div className="border border-[#1a251e] bg-[#0b100d] p-3 flex flex-col justify-between">
          <span className="text-[10px] text-[#55695d] uppercase tracking-wider font-semibold">
            HW Controller Temp
          </span>
          <div className="mt-1 flex items-baseline justify-between">
            <span className="text-xl md:text-2xl font-bold tracking-tight text-emerald-400">
              {watchdog.controllerTemperatureC}
            </span>
            <span className="text-xs text-[#526357]">°C</span>
          </div>
          <div className="w-full bg-[#141b16] h-1 mt-2 overflow-hidden">
            <div
              className="bg-emerald-500 h-full"
              style={{ width: `${(watchdog.controllerTemperatureC / 80) * 100}%` }}
            />
          </div>
        </div>

        {/* Metric 6 */}
        <div className="border border-[#1a251e] bg-[#0b100d] p-3 flex flex-col justify-between">
          <span className="text-[10px] text-[#55695d] uppercase tracking-wider font-semibold">
            Aggregated Total
          </span>
          <div className="mt-1 flex items-baseline justify-between">
            <span className="text-base font-bold tracking-tight text-zinc-100 truncate">
              {formattedStorageProgress}
            </span>
            <span className="text-[10px] text-[#526357]">MiB</span>
          </div>
          <div className="w-full bg-[#141b16] h-1 mt-2 overflow-hidden">
            <div className="bg-emerald-400 h-full w-full opacity-60" />
          </div>
        </div>
      </section>

      {/* ================= REAL-TIME OSCILLOSCOPE ================= */}
      <section className="border border-[#1a251e] bg-[#080d0a] p-3 mb-4 flex flex-col">
        <div className="flex items-center justify-between pb-2 mb-2 border-b border-[#141d17]">
          <div className="flex items-center space-x-2">
            <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
            <span className="text-xs font-semibold text-emerald-400 uppercase tracking-wider">
              High-Speed Throughput Waveform (100ms Window)
            </span>
          </div>
          <div className="flex items-center space-x-4 text-[10px] text-[#55695d]">
            <span>SCALE: 0-1200 MB/s</span>
            <span>DMA: LOCKED</span>
            <span>MEM: RING_OK</span>
          </div>
        </div>
        <div className="w-full h-24 relative overflow-hidden bg-[#040605]">
          <canvas
            ref={canvasRef}
            width={1200}
            height={96}
            className="w-full h-full block"
          />
        </div>
      </section>

      {/* ================= MAIN INTERFACE: BLOCK MATRIX / HEX TELEMETRY ================= */}
      <main className="grid grid-cols-1 lg:grid-cols-3 gap-4 flex-1">
        {/* Left Column: 4MB Circular Block Pipeline Grid */}
        <section className="lg:col-span-2 border border-[#1a251e] bg-[#0b100d] p-4 flex flex-col">
          <div className="flex items-center justify-between pb-3 mb-3 border-b border-[#141d17]">
            <div className="flex items-center space-x-2">
              <span className="text-xs font-bold text-zinc-200 uppercase tracking-wider">
                Raw 4MB Block Pipeline Matrix
              </span>
              <span className="text-[10px] px-1.5 py-0.5 bg-[#141c16] text-[#718777] border border-[#1f2d22]">
                {TOTAL_PIPELINE_BLOCKS} Blocks ({((TOTAL_STORAGE_CAPACITY_BYTES as unknown as number) / 1024 / 1024).toFixed(0)} MB Ring)
              </span>
            </div>
            
            {/* View Selector Tabs */}
            <div className="flex items-center space-x-1 border border-[#1a251e] p-0.5 bg-[#070a08]">
              {(["MATRIX", "HEX_DUMP", "WATCHDOG"] as const).map((tab) => (
                <button
                  key={tab}
                  type="button"
                  onClick={() => setActiveTab(tab)}
                  className={`text-[10px] px-2 py-0.5 cursor-pointer uppercase transition-all ${
                    activeTab === tab
                      ? "bg-[#16271c] text-emerald-400 font-bold border border-emerald-800"
                      : "text-zinc-500 hover:text-zinc-300 border border-transparent"
                  }`}
                >
                  {tab}
                </button>
              ))}
            </div>
          </div>

          {/* Conditional Sub-panels */}
          {activeTab === "MATRIX" && (
            <div className="grid grid-cols-4 sm:grid-cols-8 gap-2 overflow-y-auto max-h-[460px] p-1">
              {blocks.map((blk) => {
                const isSelected = selectedBlockIdx === blk.id;
                let statusBg = "bg-[#0e1410] border-[#18231c] text-zinc-600";
                if (blk.status === "STREAMING") {
                  statusBg = "bg-emerald-950/70 border-emerald-500 text-emerald-300 shadow-[0_0_10px_rgba(16,185,129,0.3)] animate-pulse";
                } else if (blk.status === "COMMITTED") {
                  statusBg = "bg-[#111f16] border-[#1c4728] text-emerald-400";
                } else if (blk.status === "SYNC_WAIT") {
                  statusBg = "bg-amber-950/40 border-amber-600 text-amber-300";
                }

                return (
                  <button
                    key={blk.id}
                    type="button"
                    onClick={() => setSelectedBlockIdx(blk.id)}
                    className={`cursor-pointer border p-2 flex flex-col justify-between text-left h-24 transition-all duration-75 relative ${statusBg} ${
                      isSelected ? "ring-2 ring-emerald-400" : ""
                    }`}
                  >
                    <div className="flex items-center justify-between w-full">
                      <span className="text-[10px] font-bold">#{blk.id.toString().padStart(2, "0")}</span>
                      <span className="text-[9px] uppercase tracking-tighter opacity-80">
                        {blk.status.slice(0, 4)}
                      </span>
                    </div>

                    <div className="my-auto">
                      <span className="text-[9px] block text-[#55695d]">LBA ADDR</span>
                      <span className="text-[10px] font-mono text-zinc-300">
                        0x{blk.lba.toString(16).toUpperCase().padStart(6, "0")}
                      </span>
                    </div>

                    <div className="flex items-center justify-between text-[9px] pt-1 border-t border-[#1a251e]/50 text-[#55695d]">
                      <span>{blk.latencyMicros > 0 ? `${blk.latencyMicros}μs` : "0.0μs"}</span>
                      <span className="truncate max-w-[36px]">{blk.crc32.slice(0, 4)}</span>
                    </div>
                  </button>
                );
              })}
            </div>
          )}

          {activeTab === "HEX_DUMP" && (
            <div className="flex-1 bg-[#060807] border border-[#131b15] p-3 overflow-x-auto text-[11px] font-mono leading-relaxed">
              <div className="text-zinc-500 mb-2 border-b border-[#141b16] pb-1 flex justify-between">
                <span>BUFFER OFFSET (RAW WASM HEAP MEMORY)</span>
                <span>ASCII TELEMETRY</span>
              </div>
              <div className="space-y-1">
                {Array.from({ length: 8 }).map((_, rowIndex) => {
                  const offset = rowIndex * 16;
                  const rowSlice = Array.from(hexDumpBufferRef.current.slice(offset, offset + 16));
                  return (
                    <div key={offset} className="flex items-center space-x-3">
                      <span className="text-emerald-700">
                        0x{offset.toString(16).padStart(4, "0").toUpperCase()}:
                      </span>
                      <div className="flex space-x-1.5 text-zinc-300">
                        {rowSlice.map((val, idx) => (
                          <span
                            key={idx}
                            className={val > 128 ? "text-emerald-400" : "text-zinc-400"}
                          >
                            {val.toString(16).padStart(2, "0").toUpperCase()}
                          </span>
                        ))}
                      </div>
                      <div className="text-zinc-600 pl-4 border-l border-[#131b15]">
                        {rowSlice.map((val) => (val >= 32 && val <= 126 ? String.fromCharCode(val) : ".")).join("")}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {activeTab === "WATCHDOG" && (
            <div className="flex-1 bg-[#060807] border border-[#131b15] p-4 text-xs space-y-4 font-mono">
              <div className="border border-emerald-900/60 bg-emerald-950/20 p-3">
                <span className="text-emerald-400 font-bold block mb-1">
                  HARDWARE FSYNC WATCHDOG DAEMON: ARMED
                </span>
                <p className="text-[#65786b] text-[11px]">
                  Ensures unbuffered streaming blocks commit to physical non-volatile storage within 50ms deadline. Enforces non-blocking kernel callbacks to avoid IO starvation.
                </p>
              </div>

              <div className="grid grid-cols-2 gap-3 text-[11px]">
                <div className="p-2 border border-[#162019] bg-[#090d0b]">
                  <span className="text-zinc-500 block">Watchdog Interval:</span>
                  <span className="text-zinc-200">{watchdog.heartbeatDeadlineMs} ms</span>
                </div>
                <div className="p-2 border border-[#162019] bg-[#090d0b]">
                  <span className="text-zinc-500 block">DMA Lock State:</span>
                  <span className="text-emerald-400">ENGAGED (DIRECT ACCESS)</span>
                </div>
                <div className="p-2 border border-[#162019] bg-[#090d0b]">
                  <span className="text-zinc-500 block">Power Rail Margin:</span>
                  <span className="text-zinc-200">{watchdog.voltageRailV} V (STABLE)</span>
                </div>
                <div className="p-2 border border-[#162019] bg-[#090d0b]">
                  <span className="text-zinc-500 block">Corrupt Blocks Count:</span>
                  <span className="text-emerald-400">0 (OK)</span>
                </div>
              </div>
            </div>
          )}
        </section>

        {/* Right Column: Active Block Inspector & Hardware Register Control */}
        <aside className="border border-[#1a251e] bg-[#0b100d] p-4 flex flex-col justify-between">
          <div>
            <div className="border-b border-[#141d17] pb-2 mb-3">
              <h2 className="text-xs font-bold uppercase tracking-wider text-emerald-400">
                Direct Block Inspector
              </h2>
              <span className="text-[10px] text-[#55695d]">
                Hardware Sector Pointer #{selectedBlockIdx ?? 0}
              </span>
            </div>

            {selectedBlockIdx !== null && blocks[selectedBlockIdx] && (
              <div className="space-y-3 text-xs">
                <div className="p-2.5 bg-[#070a08] border border-[#141d17]">
                  <span className="text-[10px] text-[#55695d] uppercase block">Block Identifier</span>
                  <span className="text-zinc-200 font-bold">
                    PFS-BLK-04M-{blocks[selectedBlockIdx].id.toString().padStart(4, "0")}
                  </span>
                </div>

                <div className="p-2.5 bg-[#070a08] border border-[#141d17]">
                  <span className="text-[10px] text-[#55695d] uppercase block">Physical LBA Range</span>
                  <span className="text-zinc-200">
                    {blocks[selectedBlockIdx].lba.toLocaleString()} -{" "}
                    {(blocks[selectedBlockIdx].lba + 8191).toLocaleString()}
                  </span>
                </div>

                <div className="p-2.5 bg-[#070a08] border border-[#141d17]">
                  <span className="text-[10px] text-[#55695d] uppercase block">Status Flag</span>
                  <span
                    className={`inline-block font-bold mt-0.5 ${
                      blocks[selectedBlockIdx].status === "STREAMING"
                        ? "text-emerald-400 animate-pulse"
                        : blocks[selectedBlockIdx].status === "COMMITTED"
                        ? "text-emerald-500"
                        : "text-zinc-400"
                    }`}
                  >
                    {blocks[selectedBlockIdx].status}
                  </span>
                </div>

                <div className="p-2.5 bg-[#070a08] border border-[#141d17]">
                  <span className="text-[10px] text-[#55695d] uppercase block">Block Checksum (CRC32C)</span>
                  <span className="text-emerald-400 font-mono tracking-wider">
                    0x{blocks[selectedBlockIdx].crc32}
                  </span>
                </div>

                <div className="p-2.5 bg-[#070a08] border border-[#141d17]">
                  <span className="text-[10px] text-[#55695d] uppercase block">I/O Latency</span>
                  <span className="text-zinc-200">
                    {blocks[selectedBlockIdx].latencyMicros > 0
                      ? `${blocks[selectedBlockIdx].latencyMicros} μs`
                      : "NOT MEASURED"}
                  </span>
                </div>

                <div className="p-2.5 bg-[#070a08] border border-[#141d17]">
                  <span className="text-[10px] text-[#55695d] uppercase block">Committed Timestamp</span>
                  <span className="text-zinc-400 text-[11px]">
                    {blocks[selectedBlockIdx].committedAt
                      ? new Date(blocks[selectedBlockIdx].committedAt!).toISOString()
                      : "PENDING_FSYNC"}
                  </span>
                </div>
              </div>
            )}
          </div>

          {/* Microkernel Status Footer */}
          <div className="mt-4 pt-3 border-t border-[#141d17]">
            <div className="flex items-center justify-between text-[10px] text-[#526357]">
              <span>WASM BRIDGE</span>
              <span className={isWasmHydrated ? "text-emerald-400" : "text-amber-500"}>
                {isWasmHydrated ? "ACTIVE (ZERO-COPY)" : "FALLBACK"}
              </span>
            </div>
            <div className="flex items-center justify-between text-[10px] text-[#526357] mt-1">
              <span>UNBUFFERED PIPELINE</span>
              <span className="text-zinc-300">O_DIRECT | ASYNC</span>
            </div>
          </div>
        </aside>
      </main>

      {/* ================= BOTTOM BARE-METAL FOOTER ================= */}
      <footer className="mt-4 border border-[#1a251e] bg-[#070a08] px-4 py-2 flex flex-wrap items-center justify-between text-[11px] text-[#55695d]">
        <div className="flex items-center space-x-3">
          <span className="text-emerald-500/80 font-bold uppercase tracking-wider">PulseFS Microkernel IO Engine</span>
          <span>•</span>
          <span>4MB Direct Blocks</span>
          <span>•</span>
          <span>WebUSB / WebStreams Native</span>
        </div>
        <div className="flex items-center space-x-4">
          <span>TX PACKETS: {Math.floor(Number(metrics.totalTransferredBytes) / 4096)}</span>
          <span className="text-emerald-400">FSYNC: HEALTHY</span>
        </div>
      </footer>
    </div>
  );
}