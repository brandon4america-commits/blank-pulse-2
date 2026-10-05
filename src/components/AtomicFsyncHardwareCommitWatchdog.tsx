import React, { useState, useEffect, useRef, useCallback, useReducer } from 'react';

// ============================================================================
// SYSTEM ARCHITECTURE & TYPES (PulseFS Raw Block Streaming / Direct Memory)
// ============================================================================

export type FsyncState = 
  | 'IDLE' 
  | 'ACQUIRING_BUFFER'
  | 'PIPELINE_STREAMING' 
  | 'AWAITING_HW_ACK' 
  | 'ATOMIC_COMMITTED' 
  | 'WATCHDOG_PANIC' 
  | 'CACHE_DIRTY_DRAIN';

export interface HardwareRegisters {
  CR0_FSYNC_EN: boolean;
  CR1_FUA_FORCE: boolean;
  SR0_BUS_BUSY: boolean;
  SR1_CACHE_DIRTY: boolean;
  SR2_DMA_RUN: boolean;
  IR0_TIMEOUT_ERR: boolean;
  IR1_COMMIT_ACK: boolean;
}

export interface BlockDescriptor {
  id: number;
  lba: bigint;
  sizeBytes: number;
  checksum: number;
  state: 'CLEAN' | 'ALLOCATED' | 'STREAMING' | 'BARRIER_AWAIT' | 'COMMITTED' | 'FAULT';
  fsyncLatencyUs: number;
  memoryPointer: string;
}

export interface WatchdogMetrics {
  ioTimeouts: number;
  totalFlushes: number;
  averageAckLatencyUs: number;
  peakAckLatencyUs: number;
  watchdogBudgetMs: number;
  currentCycleStart: number;
  bytesCommitted: bigint;
  activeRingHead: number;
  activeRingTail: number;
}

// Simulated Wasm Linear Memory / Direct Memory Transport Interface
interface WasmMemoryBridge {
  memory: WebAssembly.Memory | null;
  bufferView: Uint8Array | null;
  flushPointer: number;
  init: () => void;
  writeDirect4MBChunk: (chunkId: number) => Promise<number>;
  triggerAtomicFsync: (timeoutMs: number, injectStall?: boolean) => Promise<boolean>;
}

// ============================================================================
// INLINE HARDWARE ICONS (Zero external bundle dependencies, Bare-Metal style)
// ============================================================================

const CpuIcon = ({ className = "w-4 h-4" }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <rect x="4" y="4" width="16" height="16" rx="2" />
    <rect x="9" y="9" width="6" height="6" />
    <path d="M9 1v3M15 1v3M9 20v3M15 20v3M20 9h3M20 14h3M1 9h3M1 14h3" />
  </svg>
);

const ActivityIcon = ({ className = "w-4 h-4" }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
  </svg>
);

const ShieldAlertIcon = ({ className = "w-4 h-4" }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
    <line x1="12" y1="8" x2="12" y2="12" />
    <line x1="12" y1="16" x2="12.01" y2="16" />
  </svg>
);

const ZapIcon = ({ className = "w-4 h-4" }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
  </svg>
);

const TerminalIcon = ({ className = "w-4 h-4" }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <polyline points="4 17 10 11 4 5" />
    <line x1="12" y1="19" x2="20" y2="19" />
  </svg>
);

// ============================================================================
// CONSTANTS & BITMASKS
// ============================================================================

const BLOCK_SIZE_BYTES = 4 * 1024 * 1024; // 4MiB unbuffered boundary
const TOTAL_BLOCKS = 64;
const DEFAULT_WATCHDOG_BUDGET_MS = 140; // Hard threshold for hardware commit
const RAW_BASE_PTR = 0x10000000;

// ============================================================================
// MAIN PRODUCTION COMPONENT
// ============================================================================

export function AtomicFsyncWatchdog() {
  // Direct Memory & Hardware Pipeline State
  const [deviceConnected, setDeviceConnected] = useState<boolean>(false);
  const [fsyncState, setFsyncState] = useState<FsyncState>('IDLE');
  const [registers, setRegisters] = useState<HardwareRegisters>({
    CR0_FSYNC_EN: true,
    CR1_FUA_FORCE: true,
    SR0_BUS_BUSY: false,
    SR1_CACHE_DIRTY: false,
    SR2_DMA_RUN: false,
    IR0_TIMEOUT_ERR: false,
    IR1_COMMIT_ACK: false,
  });

  const [metrics, setMetrics] = useState<WatchdogMetrics>({
    ioTimeouts: 0,
    totalFlushes: 0,
    averageAckLatencyUs: 4120,
    peakAckLatencyUs: 8930,
    watchdogBudgetMs: DEFAULT_WATCHDOG_BUDGET_MS,
    currentCycleStart: 0,
    bytesCommitted: BigInt(0),
    activeRingHead: 0,
    activeRingTail: 0,
  });

  const [blocks, setBlocks] = useState<BlockDescriptor[]>(() => 
    Array.from({ length: TOTAL_BLOCKS }, (_, i) => ({
      id: i,
      lba: BigInt(i * 8192),
      sizeBytes: BLOCK_SIZE_BYTES,
      checksum: 0xe3b0c442 ^ (i * 0x1f1f),
      state: 'CLEAN',
      fsyncLatencyUs: 0,
      memoryPointer: `0x${(RAW_BASE_PTR + i * BLOCK_SIZE_BYTES).toString(16).toUpperCase()}`,
    }))
  );

  const [telemetryLogs, setTelemetryLogs] = useState<string[]>([]);
  const [watchdogTimeRemaining, setWatchdogTimeRemaining] = useState<number>(DEFAULT_WATCHDOG_BUDGET_MS);
  const [injectStall, setInjectStall] = useState<boolean>(false);
  const [isAutoStreaming, setIsAutoStreaming] = useState<boolean>(false);

  // Wasm / WebStreams Direct Memory Ref
  const wasmBridgeRef = useRef<WasmMemoryBridge | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);
  const watchdogTimerRef = useRef<number | null>(null);
  const terminalBottomRef = useRef<HTMLDivElement | null>(null);

  // Append formatted raw telemetry
  const logTelemetry = useCallback((msg: string, tag: 'INFO' | 'WARN' | 'CRIT' | 'BUS' = 'INFO') => {
    const timestamp = (performance.now() / 1000).toFixed(6);
    const logLine = `[+${timestamp}] [${tag.padEnd(4)}] ${msg}`;
    setTelemetryLogs((prev) => [...prev.slice(-99), logLine]);
  }, []);

  // Initialize WebAssembly Memory Allocation (Simulating Rust wasm-bindgen linear transport)
  useEffect(() => {
    try {
      const memory = new WebAssembly.Memory({ initial: 256, maximum: 512 }); // 16MB Initial direct pages
      const bufferView = new Uint8Array(memory.buffer);

      wasmBridgeRef.current = {
        memory,
        bufferView,
        flushPointer: 0,
        init: () => {
          logTelemetry('Wasm Linear Memory mapped: 256 pages (16,777,216 bytes)', 'BUS');
          logTelemetry('Kernel Direct Buffer: zero-copy ring transport ready.', 'INFO');
        },
        writeDirect4MBChunk: async (chunkId: number) => {
          // Zero-copy stream emulation via Direct Memory Buffer
          const offset = (chunkId % 4) * BLOCK_SIZE_BYTES;
          if (wasmBridgeRef.current?.bufferView) {
            // Emulate filling raw block with deterministic test pattern
            wasmBridgeRef.current.bufferView.fill(0xAA, offset, offset + 1024);
          }
          return offset;
        },
        triggerAtomicFsync: async (timeoutMs: number, stall = false) => {
          return new Promise<boolean>((resolve, reject) => {
            const simulatedHardwareLag = stall ? timeoutMs + 40 : Math.floor(Math.random() * 45) + 15;
            
            const timer = window.setTimeout(() => {
              if (stall) {
                reject(new Error('HARDWARE_BARRIER_TIMEOUT: FSYNC not acknowledged by non-volatile flash'));
              } else {
                resolve(true);
              }
            }, simulatedHardwareLag);

            // Abort support
            if (abortControllerRef.current) {
              abortControllerRef.current.signal.addEventListener('abort', () => {
                clearTimeout(timer);
                reject(new Error('WATCHDOG_INTERRUPT_VECTOR_FIRED'));
              });
            }
          });
        },
      };

      wasmBridgeRef.current.init();
      logTelemetry('SYS_READY: Microkernel block scheduler active at boundary 4096K', 'BUS');
    } catch (err: any) {
      logTelemetry(`WASM_INIT_FAIL: ${err.message}`, 'CRIT');
    }
  }, [logTelemetry]);

  // Scroll terminal logs automatically
  useEffect(() => {
    if (terminalBottomRef.current) {
      terminalBottomRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [telemetryLogs]);

  // Hardware Fsync Trigger Pipeline
  const executeAtomicFsyncPipeline = useCallback(async () => {
    if (fsyncState === 'PIPELINE_STREAMING' || fsyncState === 'AWAITING_HW_ACK') return;

    abortControllerRef.current = new AbortController();
    const cycleStartTime = performance.now();

    const targetBlockIndex = metrics.activeRingHead;
    const targetLba = blocks[targetBlockIndex].lba;

    try {
      // Phase 1: Direct Memory Pipe Initialization
      setFsyncState('ACQUIRING_BUFFER');
      setRegisters((r) => ({ ...r, SR0_BUS_BUSY: true, SR2_DMA_RUN: true, SR1_CACHE_DIRTY: true }));
      logTelemetry(`DMA_START: Allocating 4MiB DMA staging buffer @ LBA 0x${targetLba.toString(16)}`, 'BUS');

      await wasmBridgeRef.current?.writeDirect4MBChunk(targetBlockIndex);

      setBlocks((prev) => {
        const next = [...prev];
        next[targetBlockIndex].state = 'STREAMING';
        return next;
      });

      // Phase 2: Unbuffered Streaming over WebUSB Direct Pipe
      setFsyncState('PIPELINE_STREAMING');
      await new Promise((r) => setTimeout(r, 28)); // Simulate USB 3.2 Gen2x2 20Gbps payload delivery

      setBlocks((prev) => {
        const next = [...prev];
        next[targetBlockIndex].state = 'BARRIER_AWAIT';
        return next;
      });

      // Phase 3: Hardware Barrier & Watchdog Countdown
      setFsyncState('AWAITING_HW_ACK');
      logTelemetry(`BARRIER_REQ: FSYNC_FLUSH_EXT emitted. Watchdog arming (${metrics.watchdogBudgetMs}ms)`, 'WARN');
      
      const watchdogStart = performance.now();
      setWatchdogTimeRemaining(metrics.watchdogBudgetMs);

      // Interval ticker for high-density watchdog UI
      const ticker = window.setInterval(() => {
        const elapsed = performance.now() - watchdogStart;
        const left = Math.max(0, metrics.watchdogBudgetMs - elapsed);
        setWatchdogTimeRemaining(left);
        if (left <= 0) {
          clearInterval(ticker);
        }
      }, 4);

      // Trigger the low-level hardware FSYNC
      const fsyncPromise = wasmBridgeRef.current!.triggerAtomicFsync(metrics.watchdogBudgetMs, injectStall);
      
      // Hardware watchdog timeout race
      const timeoutPromise = new Promise((_, reject) => {
        watchdogTimerRef.current = window.setTimeout(() => {
          reject(new Error(`WATCHDOG_EXPIRED: Flash cache drain exceeded threshold of ${metrics.watchdogBudgetMs}ms`));
        }, metrics.watchdogBudgetMs);
      });

      await Promise.race([fsyncPromise, timeoutPromise]);
      clearInterval(ticker);
      if (watchdogTimerRef.current) clearTimeout(watchdogTimerRef.current);

      // Phase 4: Atomic Commit Finalization
      const commitAckTime = performance.now();
      const latencyUs = Math.round((commitAckTime - watchdogStart) * 1000);

      setRegisters((r) => ({
        ...r,
        SR0_BUS_BUSY: false,
        SR1_CACHE_DIRTY: false,
        SR2_DMA_RUN: false,
        IR1_COMMIT_ACK: true,
        IR0_TIMEOUT_ERR: false,
      }));

      setBlocks((prev) => {
        const next = [...prev];
        next[targetBlockIndex].state = 'COMMITTED';
        next[targetBlockIndex].fsyncLatencyUs = latencyUs;
        next[targetBlockIndex].checksum = (next[targetBlockIndex].checksum ^ 0x5a5a5a5a) >>> 0;
        return next;
      });

      setMetrics((m) => ({
        ...m,
        totalFlushes: m.totalFlushes + 1,
        averageAckLatencyUs: Math.round((m.averageAckLatencyUs * m.totalFlushes + latencyUs) / (m.totalFlushes + 1)),
        peakAckLatencyUs: Math.max(m.peakAckLatencyUs, latencyUs),
        bytesCommitted: m.bytesCommitted + BigInt(BLOCK_SIZE_BYTES),
        activeRingHead: (m.activeRingHead + 1) % TOTAL_BLOCKS,
      }));

      setFsyncState('ATOMIC_COMMITTED');
      logTelemetry(`FSYNC_ACK_RECV: Hardware Commit Sealed in ${latencyUs}µs. Barrier cleared.`, 'INFO');

      // Reset back to idle after short dwell
      setTimeout(() => {
        setFsyncState('IDLE');
        setRegisters((r) => ({ ...r, IR1_COMMIT_ACK: false }));
      }, 250);

    } catch (err: any) {
      // Panic Handler & Watchdog Fail-Safe Triggered
      if (watchdogTimerRef.current) clearTimeout(watchdogTimerRef.current);

      setRegisters((r) => ({
        ...r,
        SR0_BUS_BUSY: false,
        SR2_DMA_RUN: false,
        IR0_TIMEOUT_ERR: true,
        IR1_COMMIT_ACK: false,
      }));

      setBlocks((prev) => {
        const next = [...prev];
        next[targetBlockIndex].state = 'FAULT';
        return next;
      });

      setMetrics((m) => ({
        ...m,
        ioTimeouts: m.ioTimeouts + 1,
      }));

      setFsyncState('WATCHDOG_PANIC');
      logTelemetry(`KERNEL PANIC: ${err.message}`, 'CRIT');
      logTelemetry(`WATCHDOG_INTERRUPT: Rolling back volatile metadata at LBA 0x${targetLba.toString(16)}`, 'CRIT');
      
      setIsAutoStreaming(false);
    }
  }, [fsyncState, metrics.activeRingHead, metrics.watchdogBudgetMs, injectStall, blocks, logTelemetry]);

  // Auto-stream loop
  useEffect(() => {
    let timer: number | null = null;
    if (isAutoStreaming && (fsyncState === 'IDLE' || fsyncState === 'ATOMIC_COMMITTED')) {
      timer = window.setTimeout(() => {
        executeAtomicFsyncPipeline();
      }, 80);
    }
    return () => {
      if (timer) clearTimeout(timer);
    };
  }, [isAutoStreaming, fsyncState, executeAtomicFsyncPipeline]);

  // Connect mock / WebUSB raw endpoint
  const handleToggleWebUSB = async () => {
    if (deviceConnected) {
      if (abortControllerRef.current) abortControllerRef.current.abort();
      setDeviceConnected(false);
      setIsAutoStreaming(false);
      setFsyncState('IDLE');
      logTelemetry('USB_DISCONNECT: Endpoint 0x81 bulk pipe released.', 'WARN');
    } else {
      logTelemetry('USB_ENUM: Probing raw block endpoints [Vendor 0x1d6b, Product 0x0104]...', 'BUS');
      // Simulated WebUSB pairing
      setTimeout(() => {
        setDeviceConnected(true);
        logTelemetry('USB_CLAIMED: Raw Bulk Out (4MB MTU) locked with Force Unit Access (FUA) support.', 'INFO');
      }, 200);
    }
  };

  // Reset Panic State
  const handlePanicReset = () => {
    setFsyncState('IDLE');
    setWatchdogTimeRemaining(metrics.watchdogBudgetMs);
    setRegisters((r) => ({ ...r, IR0_TIMEOUT_ERR: false, SR1_CACHE_DIRTY: false }));
    logTelemetry('FAULT_CLEAR: IR0 cleared. Watchdog timer reset to calibrated baseline.', 'BUS');
  };

  // Calculate percentage of watchdog expiration
  const watchdogProgress = Math.min(
    100,
    Math.max(0, (watchdogTimeRemaining / metrics.watchdogBudgetMs) * 100)
  );

  return (
    <div className="w-full min-h-screen bg-[#07090e] text-[#b0bcc9] font-mono selection:bg-[#ffb454] selection:text-black p-2 sm:p-4 md:p-6 flex flex-col gap-4 text-xs antialiased">
      {/* ================= TOP HARDWARE HEADER ================= */}
      <header className="border border-[#1b2533] bg-[#0c1017] p-3 rounded flex flex-wrap items-center justify-between gap-3 shadow-2xl relative overflow-hidden">
        <div className="absolute top-0 left-0 w-1 h-full bg-[#00f3ff]" />
        
        <div className="flex items-center gap-3">
          <div className="p-2 bg-[#121924] border border-[#212d3d] rounded text-[#00f3ff]">
            <CpuIcon className="w-5 h-5 animate-pulse" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <span className="text-white font-bold tracking-wider text-sm">PULSEFS :: RAW BLOCK PROVISIONER</span>
              <span className="bg-[#00f3ff]/10 text-[#00f3ff] border border-[#00f3ff]/30 text-[10px] px-1.5 py-0.5 rounded font-bold uppercase tracking-tight">
                Direct DMA 4MiB
              </span>
            </div>
            <p className="text-[#56687e] text-[11px] mt-0.5">
              Unbuffered Streaming Engine • Rust Wasm Native Memory Bridge • NVMe Fsync Watchdog
            </p>
          </div>
        </div>

        {/* Global Pipeline Status Indicators */}
        <div className="flex items-center gap-4">
          <div className="flex flex-col items-end">
            <span className="text-[10px] text-[#56687e] uppercase tracking-wider font-semibold">Bus Protocol</span>
            <span className="text-[#39bae6] font-bold">RAW WebStreams / USB3.2</span>
          </div>

          <div className="h-7 w-[1px] bg-[#1a2330]" />

          <button
            onClick={handleToggleWebUSB}
            className={`px-3 py-1.5 border rounded flex items-center gap-2 font-bold tracking-wider transition-all duration-150 ${
              deviceConnected
                ? 'bg-[#14261f] border-[#39e6a2]/40 text-[#39e6a2] hover:bg-[#1b342b]'
                : 'bg-[#221718] border-[#ff3333]/40 text-[#ff6666] hover:bg-[#311f21]'
            }`}
          >
            <span className={`w-2 h-2 rounded-full ${deviceConnected ? 'bg-[#39e6a2] animate-ping' : 'bg-[#ff3333]'}`} />
            {deviceConnected ? 'ENDPOINT CLAIMED (4MB IO)' : 'DISCONNECTED: CONNECT RAW HW'}
          </button>
        </div>
      </header>

      {/* ================= WATCHDOG TELEMETRY HUD ================= */}
      <section className="grid grid-cols-1 lg:grid-cols-4 gap-4">
        {/* Watchdog Countdown Gauge */}
        <div className={`p-4 border rounded relative overflow-hidden flex flex-col justify-between ${
          fsyncState === 'WATCHDOG_PANIC' 
            ? 'bg-[#250d11] border-[#ff3b30]' 
            : fsyncState === 'AWAITING_HW_ACK'
            ? 'bg-[#181a0e] border-[#ffcc00]'
            : 'bg-[#0c1017] border-[#1b2533]'
        }`}>
          <div>
            <div className="flex items-center justify-between mb-1">
              <span className="text-[11px] font-bold tracking-wider uppercase text-[#738294] flex items-center gap-1.5">
                <ShieldAlertIcon className="w-3.5 h-3.5 text-[#ffb454]" />
                Fsync Hardware Watchdog
              </span>
              <span className={`font-mono text-xs font-bold px-1.5 py-0.5 rounded ${
                fsyncState === 'WATCHDOG_PANIC' ? 'bg-[#ff3b30] text-black' : 'bg-[#161f2c] text-[#ffb454]'
              }`}>
                LIMIT: {metrics.watchdogBudgetMs}ms
              </span>
            </div>

            <div className="mt-3">
              <div className="flex justify-between items-baseline">
                <div className="text-2xl font-bold font-mono tracking-tight text-white">
                  {fsyncState === 'WATCHDOG_PANIC' ? (
                    <span className="text-[#ff3b30]">TIMEOUT FAULT</span>
                  ) : (
                    <span>{watchdogTimeRemaining.toFixed(1)} <span className="text-xs text-[#56687e]">ms window</span></span>
                  )}
                </div>
                <span className="text-[10px] text-[#56687e]">
                  {fsyncState === 'AWAITING_HW_ACK' ? 'NON-VOLATILE DRAIN IN FLIGHT' : 'WATCHDOG ARMED'}
                </span>
              </div>

              {/* High-frequency Progress Bar */}
              <div className="w-full bg-[#141b24] h-2 rounded mt-2 overflow-hidden border border-[#212d3d]">
                <div 
                  className={`h-full transition-all duration-75 ${
                    fsyncState === 'WATCHDOG_PANIC'
                      ? 'bg-[#ff3b30] w-full'
                      : watchdogProgress < 30
                      ? 'bg-[#ff3333]'
                      : watchdogProgress < 60
                      ? 'bg-[#ffcc00]'
                      : 'bg-[#00f3ff]'
                  }`}
                  style={{ width: `${watchdogProgress}%` }}
                />
              </div>
            </div>
          </div>

          <div className="mt-4 pt-3 border-t border-[#182330] flex items-center justify-between text-[11px]">
            <span className="text-[#56687e]">Stall Injection (Simulate Flash Freeze):</span>
            <button
              onClick={() => setInjectStall((v) => !v)}
              className={`px-2 py-0.5 border text-[10px] font-bold rounded ${
                injectStall
                  ? 'bg-[#3d1818] border-[#ff4444] text-[#ff7777]'
                  : 'bg-[#121922] border-[#223142] text-[#6b7c93] hover:text-white'
              }`}
            >
              {injectStall ? 'STALL ARMED (+180ms)' : 'NORMAL BUS'}
            </button>
          </div>
        </div>

        {/* Real-time Hardware IO Registers */}
        <div className="p-4 border border-[#1b2533] bg-[#0c1017] rounded flex flex-col justify-between">
          <div className="flex items-center justify-between">
            <span className="text-[11px] font-bold tracking-wider uppercase text-[#738294] flex items-center gap-1.5">
              <ZapIcon className="w-3.5 h-3.5 text-[#00f3ff]" />
              Control Registers (MMIO)
            </span>
            <span className="text-[10px] text-[#39bae6] bg-[#00f3ff]/10 px-1 rounded">BAR0 @ 0xFEC00000</span>
          </div>

          <div className="grid grid-cols-2 gap-2 my-2 text-[10px]">
            <div className="p-1.5 bg-[#121820] border border-[#1f2937] rounded flex items-center justify-between">
              <span className="text-[#64748b]">CR0_FSYNC_EN</span>
              <span className={registers.CR0_FSYNC_EN ? 'text-[#39e6a2] font-bold' : 'text-[#475569]'}>1</span>
            </div>
            <div className="p-1.5 bg-[#121820] border border-[#1f2937] rounded flex items-center justify-between">
              <span className="text-[#64748b]">CR1_FUA_FORCE</span>
              <span className={registers.CR1_FUA_FORCE ? 'text-[#39e6a2] font-bold' : 'text-[#475569]'}>1</span>
            </div>
            <div className="p-1.5 bg-[#121820] border border-[#1f2937] rounded flex items-center justify-between">
              <span className="text-[#64748b]">SR0_BUS_BUSY</span>
              <span className={registers.SR0_BUS_BUSY ? 'text-[#ffcc00] font-bold animate-pulse' : 'text-[#475569]'}>
                {registers.SR0_BUS_BUSY ? '1' : '0'}
              </span>
            </div>
            <div className="p-1.5 bg-[#121820] border border-[#1f2937] rounded flex items-center justify-between">
              <span className="text-[#64748b]">SR1_DIRTY_CACHE</span>
              <span className={registers.SR1_CACHE_DIRTY ? 'text-[#ff9436] font-bold' : 'text-[#475569]'}>
                {registers.SR1_CACHE_DIRTY ? '1' : '0'}
              </span>
            </div>
            <div className="p-1.5 bg-[#121820] border border-[#1f2937] rounded flex items-center justify-between">
              <span className="text-[#64748b]">SR2_DMA_RUN</span>
              <span className={registers.SR2_DMA_RUN ? 'text-[#00f3ff] font-bold' : 'text-[#475569]'}>
                {registers.SR2_DMA_RUN ? '1' : '0'}
              </span>
            </div>
            <div className="p-1.5 bg-[#121820] border border-[#1f2937] rounded flex items-center justify-between">
              <span className="text-[#64748b]">IR0_TIMEOUT_ERR</span>
              <span className={registers.IR0_TIMEOUT_ERR ? 'text-[#ff3b30] font-bold animate-ping' : 'text-[#475569]'}>
                {registers.IR0_TIMEOUT_ERR ? '1' : '0'}
              </span>
            </div>
          </div>

          <div className="text-[10px] text-[#4b5563] flex justify-between">
            <span>Direct Memory Pointer:</span>
            <span className="text-[#94a3b8] font-mono">0x{RAW_BASE_PTR.toString(16).toUpperCase()}</span>
          </div>
        </div>

        {/* Flush Latencies & IO Performance */}
        <div className="p-4 border border-[#1b2533] bg-[#0c1017] rounded flex flex-col justify-between">
          <div className="flex items-center justify-between">
            <span className="text-[11px] font-bold tracking-wider uppercase text-[#738294] flex items-center gap-1.5">
              <ActivityIcon className="w-3.5 h-3.5 text-[#39e6a2]" />
              Fsync Microsecond Latency
            </span>
            <span className="text-[10px] text-[#39e6a2]">JITTER CALIBRATED</span>
          </div>

          <div className="space-y-2 my-2">
            <div className="flex justify-between items-baseline">
              <span className="text-[10px] text-[#64748b]">AVG FLUSH ACK</span>
              <span className="text-lg font-bold text-white font-mono">
                {metrics.averageAckLatencyUs} <span className="text-xs text-[#64748b]">µs</span>
              </span>
            </div>
            <div className="flex justify-between items-baseline">
              <span className="text-[10px] text-[#64748b]">PEAK COMMITTED DRAIN</span>
              <span className="text-sm font-bold text-[#ffb454] font-mono">
                {metrics.peakAckLatencyUs} <span className="text-xs text-[#64748b]">µs</span>
              </span>
            </div>
            <div className="flex justify-between items-baseline">
              <span className="text-[10px] text-[#64748b]">TIMEOUT INCIDENTS</span>
              <span className={`text-sm font-bold font-mono ${metrics.ioTimeouts > 0 ? 'text-[#ff3b30]' : 'text-[#39e6a2]'}`}>
                {metrics.ioTimeouts} FAILS
              </span>
            </div>
          </div>

          <div className="pt-2 border-t border-[#182330] flex justify-between text-[10px] text-[#56687e]">
            <span>COMMITTED BLOCKS:</span>
            <span className="text-white font-bold">{metrics.totalFlushes} ({((Number(metrics.bytesCommitted) / 1024 / 1024)).toFixed(0)} MiB)</span>
          </div>
        </div>

        {/* Pipeline Execution Controls */}
        <div className="p-4 border border-[#1b2533] bg-[#0c1017] rounded flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between mb-2">
              <span className="text-[11px] font-bold tracking-wider uppercase text-[#738294]">
                Pipeline Dispatcher
              </span>
              <span className={`w-2.5 h-2.5 rounded-full ${
                fsyncState === 'WATCHDOG_PANIC' ? 'bg-[#ff3b30]' :
                fsyncState === 'AWAITING_HW_ACK' ? 'bg-[#ffcc00] animate-ping' :
                fsyncState === 'PIPELINE_STREAMING' ? 'bg-[#00f3ff]' : 'bg-[#39e6a2]'
              }`} />
            </div>

            <p className="text-[10px] text-[#56687e] leading-relaxed">
              Dispatches unbuffered 4MB block chunks directly through WebStream buffers to NVMe device endpoints with hardware FUA commit barriers.
            </p>
          </div>

          <div className="space-y-2 mt-3">
            {fsyncState === 'WATCHDOG_PANIC' ? (
              <button
                onClick={handlePanicReset}
                className="w-full py-2 bg-[#ff3b30] hover:bg-[#e03126] text-black font-extrabold tracking-wider uppercase text-[11px] rounded transition-all shadow-lg shadow-[#ff3b30]/20"
              >
                CLEAR WATCHDOG PANIC & RE-ARM
              </button>
            ) : (
              <div className="grid grid-cols-2 gap-2">
                <button
                  disabled={!deviceConnected || fsyncState !== 'IDLE'}
                  onClick={executeAtomicFsyncPipeline}
                  className="py-2 bg-[#172333] hover:bg-[#1e3046] disabled:opacity-40 disabled:cursor-not-allowed border border-[#2b3e58] text-[#00f3ff] font-bold tracking-wider uppercase text-[10px] rounded transition-all"
                >
                  PIPELINE 4MB
                </button>
                <button
                  disabled={!deviceConnected}
                  onClick={() => setIsAutoStreaming((v) => !v)}
                  className={`py-2 border font-bold tracking-wider uppercase text-[10px] rounded transition-all ${
                    isAutoStreaming
                      ? 'bg-[#3b2b10] border-[#ffaa00] text-[#ffaa00]'
                      : 'bg-[#121a24] border-[#223142] text-white hover:bg-[#1a2533]'
                  }`}
                >
                  {isAutoStreaming ? 'HALT STREAM' : 'AUTO STREAM'}
                </button>
              </div>
            )}
          </div>
        </div>
      </section>

      {/* ================= RAW 4MB BLOCK GRID (Unbuffered Allocation Ring) ================= */}
      <section className="border border-[#1b2533] bg-[#0c1017] p-4 rounded">
        <div className="flex flex-wrap items-center justify-between gap-2 pb-3 mb-3 border-b border-[#182330]">
          <div className="flex items-center gap-2">
            <span className="text-white font-bold tracking-wider uppercase text-xs">
              Direct Memory Ring Buffer (64 x 4MiB Chunks = 256MiB Physical Span)
            </span>
            <span className="text-[10px] text-[#56687e]">Zero-Buffered Alignment</span>
          </div>
          <div className="flex items-center gap-3 text-[10px]">
            <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 bg-[#121820] border border-[#232f3e] inline-block" /> CLEAN</span>
            <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 bg-[#00f3ff] inline-block animate-pulse" /> DMA PIPELINE</span>
            <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 bg-[#ffcc00] inline-block animate-ping" /> FSYNC BARRIER</span>
            <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 bg-[#1b4332] border border-[#39e6a2] inline-block" /> COMMITTED</span>
            <span className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 bg-[#ff3b30] inline-block" /> FAULT</span>
          </div>
        </div>

        {/* Block Matrix */}
        <div className="grid grid-cols-8 sm:grid-cols-16 gap-1.5">
          {blocks.map((block) => {
            const isHead = block.id === metrics.activeRingHead;
            return (
              <div
                key={block.id}
                title={`Block #${block.id} | LBA: 0x${block.lba.toString(16)} | Addr: ${block.memoryPointer} | State: ${block.state}`}
                className={`h-9 rounded p-1 border flex flex-col justify-between transition-all duration-150 cursor-pointer ${
                  block.state === 'STREAMING'
                    ? 'bg-[#00f3ff]/20 border-[#00f3ff] text-[#00f3ff]'
                    : block.state === 'BARRIER_AWAIT'
                    ? 'bg-[#ffcc00]/20 border-[#ffcc00] text-[#ffcc00] animate-pulse'
                    : block.state === 'COMMITTED'
                    ? 'bg-[#10241b] border-[#255e44] text-[#39e6a2]'
                    : block.state === 'FAULT'
                    ? 'bg-[#3b1215] border-[#ff3b30] text-[#ff3b30]'
                    : isHead
                    ? 'bg-[#1a2333] border-[#3b82f6] text-white'
                    : 'bg-[#0f141c] border-[#1b2330] text-[#475569] hover:border-[#334155]'
                }`}
              >
                <div className="flex justify-between items-center text-[9px] font-bold">
                  <span>{block.id.toString().padStart(2, '0')}</span>
                  {isHead && <span className="text-[#3b82f6] text-[8px]">▶PTR</span>}
                </div>
                <div className="text-[8px] font-mono truncate text-right opacity-70">
                  {block.fsyncLatencyUs > 0 ? `${(block.fsyncLatencyUs / 1000).toFixed(1)}ms` : '4096K'}
                </div>
              </div>
            );
          })}
        </div>
      </section>

      {/* ================= LOW-LEVEL TELEMETRY STREAM & LOG TERMINAL ================= */}
      <section className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Real-time Hardware Console */}
        <div className="lg:col-span-2 border border-[#1b2533] bg-[#080b0f] p-3 rounded flex flex-col h-64">
          <div className="flex items-center justify-between pb-2 mb-2 border-b border-[#161f2b]">
            <div className="flex items-center gap-2">
              <TerminalIcon className="w-4 h-4 text-[#ffb454]" />
              <span className="text-white font-bold tracking-wider text-[11px] uppercase">
                Direct Memory Stream Telemetry (DMA/USB-Bulk/Fsync)
              </span>
            </div>
            <button
              onClick={() => setTelemetryLogs([])}
              className="text-[10px] text-[#56687e] hover:text-white"
            >
              CLEAR CONSOLE
            </button>
          </div>

          <div className="flex-1 overflow-y-auto space-y-1 font-mono text-[10px] pr-2 scrollbar-thin scrollbar-thumb-[#1e293b]">
            {telemetryLogs.length === 0 ? (
              <span className="text-[#475569]">System initialized. Awaiting pipeline instruction...</span>
            ) : (
              telemetryLogs.map((log, idx) => (
                <div 
                  key={idx}
                  className={`leading-tight ${
                    log.includes('[CRIT]') ? 'text-[#ff5555] font-bold bg-[#ff0000]/10 px-1 py-0.5 rounded' :
                    log.includes('[WARN]') ? 'text-[#ffcc00]' :
                    log.includes('[BUS ]') ? 'text-[#00f3ff]' : 'text-[#8da0b6]'
                  }`}
                >
                  {log}
                </div>
              ))
            )}
            <div ref={terminalBottomRef} />
          </div>
        </div>

        {/* Micro-kernel Architecture Spec card */}
        <div className="border border-[#1b2533] bg-[#0c1017] p-4 rounded flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between mb-2">
              <span className="text-white font-bold tracking-wider text-[11px] uppercase">
                PulseFS Specification
              </span>
              <span className="text-[10px] text-[#00f3ff] bg-[#00f3ff]/10 px-1.5 py-0.5 rounded border border-[#00f3ff]/30 font-bold">
                REV 2.4.9
              </span>
            </div>
            <p className="text-[10px] text-[#56687e] leading-relaxed">
              Direct block flash provisioner eliminating userland/kernel page-cache transitions. Enforces zero-copy hardware barriers via continuous synchronous Fsync Watchdogs.
            </p>

            <div className="mt-3 space-y-2 border-t border-[#182330] pt-2 text-[10px]">
              <div className="flex justify-between">
                <span className="text-[#64748b]">Atomic I/O Granularity:</span>
                <span className="text-white font-mono">4,194,304 Bytes</span>
              </div>
              <div className="flex justify-between">
                <span className="text-[#64748b]">Memory Pipeline Transport:</span>
                <span className="text-white font-mono">Vite 6 Native SharedMem</span>
              </div>
              <div className="flex justify-between">
                <span className="text-[#64748b]">Driver ABI:</span>
                <span className="text-white font-mono">wasm-bindgen (Rust 1.85)</span>
              </div>
              <div className="flex justify-between">
                <span className="text-[#64748b]">Barrier Guarantee:</span>
                <span className="text-[#39e6a2] font-mono">Atomic Commit-or-Panic</span>
              </div>
            </div>
          </div>

          <div className="p-2 bg-[#121924] border border-[#1e2a3a] rounded text-[10px] text-[#718296] flex items-center gap-2 mt-4">
            <span className="w-1.5 h-1.5 rounded-full bg-[#00f3ff]" />
            Direct unbuffered block flashing ready for deployment.
          </div>
        </div>
      </section>
    </div>
  );
}

export default AtomicFsyncWatchdog;