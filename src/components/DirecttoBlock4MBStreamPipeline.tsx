import React, { useState, useEffect, useCallback, useRef, useTransition, useId } from 'react';

// ============================================================================
// SYSTEM DEFINITIONS & DIRECT MEMORY HARDWARE CONSTANTS
// ============================================================================

export const BLOCK_SIZE_BYTES = 4 * 1024 * 1024; // 4MB Direct-to-Block allocation
export const SECTOR_SIZE = 512;
export const SECTORS_PER_BLOCK = BLOCK_SIZE_BYTES / SECTOR_SIZE; // 8192 sectors
export const RING_BUFFER_SLOTS = 4; // 16MB total inflight pipeline
export const FSYNC_TIMEOUT_THRESHOLD_US = 25000; // 25ms watchdog panic limit

export type StreamState = 
  | 'IDLE' 
  | 'USB_NEGOTIATING' 
  | 'DMA_MAPPED' 
  | 'STREAMING_PIPELINE' 
  | 'FSYNC_BARRIER' 
  | 'COMMITTED' 
  | 'BUS_FAULT';

export interface RingBufferSlot {
  slotId: number;
  ptr: number; // WASM linear memory address
  lbaStart: bigint;
  status: 'EMPTY' | 'FILL_STAGING' | 'DMA_TRANSFER' | 'AWAIT_FSYNC' | 'RETIRED';
  bytesWritten: number;
  crc32: number;
  fsyncDurationUs: number;
}

export interface HardwareTelemetry {
  lbaTarget: bigint;
  blocksWritten: number;
  activeThroughputMBps: number;
  fsyncWatchdogTimeUs: number;
  backpressureRatio: number; // 0.0 - 1.0
  crcMismatchCount: number;
  unbufferedIoActive: boolean;
  busVoltageMv: number;
  busCurrentMa: number;
}

export interface WasmMicrokernelBridge {
  memoryBuffer: SharedArrayBuffer | ArrayBuffer;
  allocateRingSlots: (slots: number, sizeBytes: number) => number[];
  computeCrc32Block: (ptr: number, length: number) => number;
  commitFsyncBarrier: (slot: number) => Promise<{ latencyUs: number; ok: boolean }>;
  pollWatchdogRegister: () => { watchdogTripped: boolean; cycleTimeUs: number };
}

// Simulated Wasm & WebUSB Raw Transport Engine
class BareMetalTransportEngine implements WasmMicrokernelBridge {
  public memoryBuffer: ArrayBuffer;
  private ringPointers: number[] = [];

  constructor() {
    this.memoryBuffer = new ArrayBuffer(BLOCK_SIZE_BYTES * RING_BUFFER_SLOTS);
  }

  allocateRingSlots(slots: number, sizeBytes: number): number[] {
    this.ringPointers = [];
    for (let i = 0; i < slots; i++) {
      this.ringPointers.push(i * sizeBytes);
    }
    return this.ringPointers;
  }

  computeCrc32Block(ptr: number, length: number): number {
    // Fast CRC32-C (Castagnoli) mock simulation over mapped pointer
    const view = new DataView(this.memoryBuffer, ptr, Math.min(length, 1024));
    let crc = 0xFFFFFFFF;
    for (let i = 0; i < view.byteLength; i += 4) {
      crc = (crc ^ view.getUint32(i, true)) >>> 0;
    }
    return crc ^ 0xFFFFFFFF;
  }

  async commitFsyncBarrier(slot: number): Promise<{ latencyUs: number; ok: boolean }> {
    const start = performance.now();
    // Hardware JEDEC write-cache flush & physical storage barrier
    const simulatedHardwareDelay = 1.2 + Math.random() * 4.8; 
    await new Promise(r => setTimeout(r, simulatedHardwareDelay));
    const durationUs = Math.round((performance.now() - start) * 1000);
    return {
      latencyUs: durationUs,
      ok: durationUs < FSYNC_TIMEOUT_THRESHOLD_US
    };
  }

  pollWatchdogRegister(): { watchdogTripped: boolean; cycleTimeUs: number } {
    const cycle = Math.floor(800 + Math.random() * 600);
    return {
      watchdogTripped: cycle > FSYNC_TIMEOUT_THRESHOLD_US,
      cycleTimeUs: cycle
    };
  }
}

// ============================================================================
// MAIN COMPONENT: DIRECT-TO-BLOCK 4MB STREAM PIPELINE
// ============================================================================

export default function DirectToBlockPipeline() {
  const componentId = useId();
  const [isPending, startTransition] = useTransition();

  // Low-level hardware pipeline state
  const [streamState, setStreamState] = useState<StreamState>('IDLE');
  const [deviceDescriptor, setDeviceDescriptor] = useState<{
    vendor: string;
    product: string;
    endpointOut: number;
    maxPacketSize: number;
    rawLbaMax: bigint;
  } | null>(null);

  const [ringSlots, setRingSlots] = useState<RingBufferSlot[]>([
    { slotId: 0, ptr: 0x000000, lbaStart: 0n, status: 'EMPTY', bytesWritten: 0, crc32: 0, fsyncDurationUs: 0 },
    { slotId: 1, ptr: 0x400000, lbaStart: 0n, status: 'EMPTY', bytesWritten: 0, crc32: 0, fsyncDurationUs: 0 },
    { slotId: 2, ptr: 0x800000, lbaStart: 0n, status: 'EMPTY', bytesWritten: 0, crc32: 0, fsyncDurationUs: 0 },
    { slotId: 3, ptr: 0xC00000, lbaStart: 0n, status: 'EMPTY', bytesWritten: 0, crc32: 0, fsyncDurationUs: 0 },
  ]);

  const [telemetry, setTelemetry] = useState<HardwareTelemetry>({
    lbaTarget: 0x000000001000n,
    blocksWritten: 0,
    activeThroughputMBps: 0.0,
    fsyncWatchdogTimeUs: 1120,
    backpressureRatio: 0.02,
    crcMismatchCount: 0,
    unbufferedIoActive: true,
    busVoltageMv: 5040,
    busCurrentMa: 890
  });

  const [logTail, setLogTail] = useState<Array<{ id: number; timestamp: string; channel: string; msg: string }>>([]);

  // Hardware Engine and WebStream references
  const engineRef = useRef<BareMetalTransportEngine | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);
  const logCounterRef = useRef<number>(0);
  const streamPumpRef = useRef<number | null>(null);

  // Append atomic telemetric log
  const pushLog = useCallback((channel: 'KERNEL' | 'DMA' | 'FSYNC' | 'RAWIO', msg: string) => {
    const timestamp = (performance.now() / 1000).toFixed(4).padStart(9, '0');
    setLogTail(prev => [
      { id: ++logCounterRef.current, timestamp, channel, msg },
      ...prev.slice(0, 48)
    ]);
  }, []);

  // Initialize Native Bare-Metal Bridge
  useEffect(() => {
    const engine = new BareMetalTransportEngine();
    const ptrs = engine.allocateRingSlots(RING_BUFFER_SLOTS, BLOCK_SIZE_BYTES);
    engineRef.current = engine;

    setRingSlots(slots => slots.map((s, idx) => ({
      ...s,
      ptr: ptrs[idx]
    })));

    pushLog('KERNEL', 'PulseFS v0.98.4 Microkernel init. Memory mapped: 16777216 bytes (Direct DMA).');
    pushLog('RAWIO', 'CR0_DIRECT_IO = 0x01. Operating in Unbuffered Non-Volatile IO mode.');

    return () => {
      if (streamPumpRef.current) cancelAnimationFrame(streamPumpRef.current);
    };
  }, [pushLog]);

  // Direct Hardware Handshake simulation (WebUSB Interface / Raw Block target)
  const handleAttachDevice = async () => {
    setStreamState('USB_NEGOTIATING');
    pushLog('DMA', 'Acquiring USB Bulk-Out Interface (EP 0x02)...');

    // Simulate raw USB/NVMe Endpoint Claim
    setTimeout(() => {
      setDeviceDescriptor({
        vendor: 'PULSE_SYS_RAW_IO',
        product: 'BLK-PROV-EMMC-UFS4',
        endpointOut: 0x02,
        maxPacketSize: 1024,
        rawLbaMax: 0x000000FFFFFFFFn
      });
      setStreamState('DMA_MAPPED');
      pushLog('DMA', 'Endpoint claimed. Direct Scatter-Gather Ring initialized.');
    }, 850);
  };

  // 4MB Streaming Pipeline Trigger
  const handleStartPipeline = () => {
    if (!engineRef.current || streamState !== 'DMA_MAPPED') return;

    abortControllerRef.current = new AbortController();
    setStreamState('STREAMING_PIPELINE');
    pushLog('RAWIO', `Dispatching 4MB stream pipeline to LBA [0x${telemetry.lbaTarget.toString(16).toUpperCase()}]`);

    let currentLba = telemetry.lbaTarget;
    let written = 0;
    let activeSlotIndex = 0;
    let lastStamp = performance.now();
    let bytesInWindow = 0;

    const streamLoop = async () => {
      if (abortControllerRef.current?.signal.aborted) {
        return;
      }

      const slot = activeSlotIndex;
      activeSlotIndex = (activeSlotIndex + 1) % RING_BUFFER_SLOTS;

      // 1. Stage Direct-to-Block memory chunk
      setRingSlots(prev => prev.map((s, i) => i === slot ? {
        ...s,
        status: 'FILL_STAGING',
        lbaStart: currentLba,
        bytesWritten: 0
      } : s));

      // Simulate microsecond fill of 4MB unbuffered chunk
      await new Promise(r => setTimeout(r, 16));

      // 2. Hardware CRC Computation via Direct WASM Linear Memory
      const engine = engineRef.current!;
      const crc = engine.computeCrc32Block(slot * BLOCK_SIZE_BYTES, BLOCK_SIZE_BYTES);

      setRingSlots(prev => prev.map((s, i) => i === slot ? {
        ...s,
        status: 'DMA_TRANSFER',
        bytesWritten: BLOCK_SIZE_BYTES,
        crc32: crc
      } : s));

      // 3. Physical Commit + Watchdog Hardware Barrier
      setStreamState('FSYNC_BARRIER');
      const fsyncRes = await engine.commitFsyncBarrier(slot);

      if (!fsyncRes.ok) {
        setStreamState('BUS_FAULT');
        pushLog('FSYNC', `PANIC: Hardware Watchdog Barrier Timed Out: ${fsyncRes.latencyUs}µs > ${FSYNC_TIMEOUT_THRESHOLD_US}µs`);
        return;
      }

      // Slot is retired, flushed to raw substrate
      currentLba += BigInt(SECTORS_PER_BLOCK);
      written++;
      bytesInWindow += BLOCK_SIZE_BYTES;

      const now = performance.now();
      const elapsed = (now - lastStamp) / 1000;
      let curMbps = telemetry.activeThroughputMBps;

      if (elapsed >= 0.25) {
        curMbps = (bytesInWindow / (1024 * 1024)) / elapsed;
        bytesInWindow = 0;
        lastStamp = now;
      }

      const watchdog = engine.pollWatchdogRegister();

      startTransition(() => {
        setRingSlots(prev => prev.map((s, i) => i === slot ? {
          ...s,
          status: 'RETIRED',
          fsyncDurationUs: fsyncRes.latencyUs
        } : s));

        setTelemetry(prev => ({
          ...prev,
          lbaTarget: currentLba,
          blocksWritten: written,
          activeThroughputMBps: parseFloat(curMbps.toFixed(2)),
          fsyncWatchdogTimeUs: fsyncRes.latencyUs,
          backpressureRatio: parseFloat((0.02 + Math.random() * 0.08).toFixed(3)),
          busVoltageMv: Math.floor(5020 + Math.random() * 40),
          busCurrentMa: Math.floor(920 + Math.random() * 140)
        }));

        setStreamState('STREAMING_PIPELINE');
      });

      if (written % 4 === 0) {
        pushLog('FSYNC', `HARDWARE BARRIER OK: Slot #${slot} committed at LBA 0x${currentLba.toString(16).toUpperCase()} in ${fsyncRes.latencyUs}µs`);
      }

      // Chain next frame
      streamPumpRef.current = requestAnimationFrame(() => {
        streamLoop();
      });
    };

    streamLoop();
  };

  const handleAbortPipeline = () => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }
    if (streamPumpRef.current) {
      cancelAnimationFrame(streamPumpRef.current);
    }
    setStreamState('DMA_MAPPED');
    pushLog('RAWIO', 'Pipeline streaming halted by host operator. Storage interface held in safe state.');
  };

  const handleEmergencyBusReset = () => {
    if (streamPumpRef.current) cancelAnimationFrame(streamPumpRef.current);
    setStreamState('IDLE');
    setDeviceDescriptor(null);
    setTelemetry(prev => ({
      ...prev,
      activeThroughputMBps: 0.0,
      blocksWritten: 0,
      lbaTarget: 0x000000001000n
    }));
    setRingSlots(prev => prev.map(s => ({ ...s, status: 'EMPTY', bytesWritten: 0, crc32: 0, fsyncDurationUs: 0 })));
    pushLog('KERNEL', 'BUS RESET: Hardware lines cycled, DMA ring buffers purged.');
  };

  return (
    <div className="w-full max-w-7xl mx-auto bg-zinc-950 text-zinc-300 font-mono border border-zinc-800 rounded-none shadow-2xl selection:bg-emerald-950 selection:text-emerald-300">
      
      {/* TOP DECK: Microkernel Titlebar & Realtime Hardware Telemetry */}
      <header className="border-b border-zinc-800/90 bg-zinc-900/60 p-3 flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center space-x-3">
          <div className="relative flex items-center justify-center w-5 h-5">
            <span className={`absolute inline-flex h-full w-full rounded-full opacity-75 animate-ping ${
              streamState === 'STREAMING_PIPELINE' ? 'bg-emerald-500' :
              streamState === 'FSYNC_BARRIER' ? 'bg-amber-500' :
              streamState === 'BUS_FAULT' ? 'bg-rose-500' : 'bg-zinc-600'
            }`} />
            <span className={`relative inline-flex rounded-full h-3 w-3 ${
              streamState === 'STREAMING_PIPELINE' ? 'bg-emerald-400' :
              streamState === 'FSYNC_BARRIER' ? 'bg-amber-400' :
              streamState === 'BUS_FAULT' ? 'bg-rose-400' : 'bg-zinc-500'
            }`} />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <span className="font-extrabold text-sm tracking-widest text-zinc-100 uppercase">
                PulseFS // Direct-to-Block Stream
              </span>
              <span className="text-[10px] px-1.5 py-0.5 rounded bg-zinc-800 text-zinc-400 border border-zinc-700">
                RAW_O_DIRECT 4MB
              </span>
            </div>
            <div className="text-[10px] text-zinc-500 tracking-tight">
              TARGET_LBA: 0x{telemetry.lbaTarget.toString(16).padStart(16, '0').toUpperCase()} • SECTOR_SIZE: {SECTOR_SIZE}B
            </div>
          </div>
        </div>

        {/* Global Pipeline Status Pill */}
        <div className="flex items-center gap-4">
          <div className="flex flex-col text-right">
            <span className="text-[10px] text-zinc-500 tracking-wider">PIPELINE STATE</span>
            <span className={`text-xs font-bold tracking-widest ${
              streamState === 'STREAMING_PIPELINE' ? 'text-emerald-400 animate-pulse' :
              streamState === 'FSYNC_BARRIER' ? 'text-amber-400' :
              streamState === 'BUS_FAULT' ? 'text-rose-500' : 'text-zinc-400'
            }`}>
              {streamState}
            </span>
          </div>

          <div className="h-7 w-[1px] bg-zinc-800" />

          {/* Core Hardware Metrics Quick Display */}
          <div className="flex gap-4 text-xs">
            <div>
              <div className="text-[9px] text-zinc-500">THROUGHPUT</div>
              <div className="font-bold text-emerald-400">
                {telemetry.activeThroughputMBps.toFixed(1)} <span className="text-[9px] text-zinc-500">MB/s</span>
              </div>
            </div>
            <div>
              <div className="text-[9px] text-zinc-500">FSYNC WATCHDOG</div>
              <div className={`font-bold ${telemetry.fsyncWatchdogTimeUs > 15000 ? 'text-rose-400' : 'text-zinc-200'}`}>
                {telemetry.fsyncWatchdogTimeUs} <span className="text-[9px] text-zinc-500">µs</span>
              </div>
            </div>
            <div>
              <div className="text-[9px] text-zinc-500">POWER DRAW</div>
              <div className="font-bold text-zinc-200">
                {(telemetry.busVoltageMv / 1000).toFixed(2)}V / {telemetry.busCurrentMa}mA
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* SUB-HEADER CONTROLS & FLIGHT REGISTERS */}
      <section className="p-3 bg-zinc-950/80 border-b border-zinc-800 flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          {deviceDescriptor === null ? (
            <button
              onClick={handleAttachDevice}
              disabled={streamState === 'USB_NEGOTIATING'}
              className="px-3 py-1.5 text-xs font-semibold bg-emerald-600 hover:bg-emerald-500 text-zinc-950 border border-emerald-400 shadow transition-all active:scale-[0.98] disabled:opacity-50"
            >
              ATTACH RAW USB-STORAGE
            </button>
          ) : streamState === 'STREAMING_PIPELINE' || streamState === 'FSYNC_BARRIER' ? (
            <button
              onClick={handleAbortPipeline}
              className="px-3 py-1.5 text-xs font-semibold bg-amber-600 hover:bg-amber-500 text-zinc-950 border border-amber-400 shadow transition-all active:scale-[0.98]"
            >
              HALT STREAM PIPELINE
            </button>
          ) : (
            <button
              onClick={handleStartPipeline}
              className="px-3 py-1.5 text-xs font-semibold bg-emerald-500 hover:bg-emerald-400 text-zinc-950 border border-emerald-300 shadow transition-all active:scale-[0.98]"
            >
              DISPATCH 4MB STREAM
            </button>
          )}

          <button
            onClick={handleEmergencyBusReset}
            className="px-3 py-1.5 text-xs font-semibold bg-zinc-800 hover:bg-zinc-700 text-rose-300 border border-zinc-700 active:scale-[0.98]"
          >
            SYS_BUS_RESET
          </button>
        </div>

        {/* Register Telemetry Flags */}
        <div className="flex items-center gap-2 text-[10px]">
          <div className="px-2 py-1 bg-zinc-900 border border-zinc-800 flex items-center gap-1.5">
            <span className="text-zinc-500">BACKPRESSURE:</span>
            <span className="text-emerald-400 font-bold">{(telemetry.backpressureRatio * 100).toFixed(1)}%</span>
          </div>
          <div className="px-2 py-1 bg-zinc-900 border border-zinc-800 flex items-center gap-1.5">
            <span className="text-zinc-500">COMMITTED:</span>
            <span className="text-zinc-200 font-bold">{telemetry.blocksWritten * 4} MB</span>
            <span className="text-zinc-500">({telemetry.blocksWritten} BLK)</span>
          </div>
          <div className="px-2 py-1 bg-zinc-900 border border-zinc-800 flex items-center gap-1.5">
            <span className="text-zinc-500">WATCHDOG FAULTS:</span>
            <span className={telemetry.crcMismatchCount > 0 ? "text-rose-400" : "text-zinc-400"}>
              {telemetry.crcMismatchCount}
            </span>
          </div>
        </div>
      </section>

      {/* CORE PIPELINE ARCHITECTURE: 4MB RING BUFFER MATRIX */}
      <main className="p-4 grid grid-cols-1 lg:grid-cols-12 gap-4">
        
        {/* Left Column: 4-Slot DMA In-Flight Ring Matrix (8 Cols) */}
        <div className="lg:col-span-8 space-y-4">
          <div className="flex items-center justify-between border-b border-zinc-800 pb-2">
            <div className="text-xs font-bold text-zinc-400 tracking-wider flex items-center gap-2">
              <svg className="w-3.5 h-3.5 text-emerald-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <rect x="2" y="2" width="20" height="8" rx="1" />
                <rect x="2" y="14" width="20" height="8" rx="1" />
                <line x1="6" y1="6" x2="6" y2="6.01" />
                <line x1="6" y1="18" x2="6" y2="18.01" />
              </svg>
              <span>DIRECT DMA RING BUFFER PIPELINE (16MB IN-FLIGHT)</span>
            </div>
            <div className="text-[10px] text-zinc-500">
              ALLOCATION CHUNK: <span className="text-zinc-300">4,194,304 BYTES</span>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {ringSlots.map((slot) => {
              const isSlotActive = slot.status === 'DMA_TRANSFER' || slot.status === 'FILL_STAGING';
              const isSyncing = slot.status === 'AWAIT_FSYNC' || (streamState === 'FSYNC_BARRIER' && isSlotActive);

              return (
                <div
                  key={slot.slotId}
                  className={`p-3 border transition-colors relative overflow-hidden ${
                    isSlotActive 
                      ? 'bg-zinc-900/90 border-emerald-500/80 shadow-lg shadow-emerald-950/40' 
                      : isSyncing
                      ? 'bg-zinc-900/90 border-amber-500/80 shadow-lg shadow-amber-950/40'
                      : slot.status === 'RETIRED'
                      ? 'bg-zinc-900/40 border-zinc-800'
                      : 'bg-zinc-950 border-zinc-900'
                  }`}
                >
                  {/* Visual Progress Bar for Slot DMA Filling */}
                  <div
                    className={`absolute bottom-0 left-0 h-0.5 transition-all duration-150 ${
                      isSlotActive ? 'bg-emerald-400 w-full' : isSyncing ? 'bg-amber-400 w-full' : 'w-0'
                    }`}
                  />

                  <div className="flex items-center justify-between text-xs mb-2">
                    <span className="font-bold text-zinc-200">SLOT #{slot.slotId.toString().padStart(2, '0')}</span>
                    <span className={`text-[10px] px-1.5 py-0.5 border ${
                      slot.status === 'DMA_TRANSFER' ? 'bg-emerald-950/80 text-emerald-300 border-emerald-700 animate-pulse' :
                      slot.status === 'FILL_STAGING' ? 'bg-blue-950/80 text-blue-300 border-blue-700' :
                      slot.status === 'RETIRED' ? 'bg-zinc-800 text-zinc-400 border-zinc-700' :
                      'bg-zinc-900 text-zinc-600 border-zinc-800'
                    }`}>
                      {slot.status}
                    </span>
                  </div>

                  <div className="space-y-1 text-[11px]">
                    <div className="flex justify-between">
                      <span className="text-zinc-500">LINEAR PTR:</span>
                      <span className="text-zinc-300">0x{slot.ptr.toString(16).padStart(8, '0').toUpperCase()}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-zinc-500">START LBA:</span>
                      <span className="text-zinc-300">0x{slot.lbaStart.toString(16).toUpperCase()}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-zinc-500">PAYLOAD SIZE:</span>
                      <span className="text-zinc-300">{(slot.bytesWritten / (1024 * 1024)).toFixed(2)} MB / 4.00 MB</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-zinc-500">CRC32-C CHK:</span>
                      <span className="text-emerald-400 font-mono">
                        {slot.crc32 !== 0 ? `0x${slot.crc32.toString(16).padStart(8, '0').toUpperCase()}` : '0x00000000'}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-zinc-500">FSYNC BARRIER:</span>
                      <span className={slot.fsyncDurationUs > 10000 ? "text-amber-400 font-bold" : "text-zinc-400"}>
                        {slot.fsyncDurationUs > 0 ? `${slot.fsyncDurationUs} µs` : 'WAIT_BARRIER'}
                      </span>
                    </div>
                  </div>

                  {/* Micro Sector Map Strip (Representing 8192 sectors within 4MB) */}
                  <div className="mt-3 pt-2 border-t border-zinc-800/80">
                    <div className="text-[9px] text-zinc-500 mb-1 flex justify-between">
                      <span>SECTOR DENSITY ({SECTORS_PER_BLOCK} LBA)</span>
                      <span>{slot.status === 'RETIRED' ? 'COMMITTED' : slot.status === 'EMPTY' ? 'EMPTY' : 'TRANSFERRING'}</span>
                    </div>
                    <div className="h-1.5 w-full bg-zinc-800 rounded-none overflow-hidden flex">
                      {Array.from({ length: 16 }).map((_, barIdx) => (
                        <div
                          key={barIdx}
                          className={`h-full flex-1 border-r border-zinc-950 ${
                            slot.status === 'RETIRED' ? 'bg-emerald-500/80' :
                            slot.status === 'DMA_TRANSFER' ? 'bg-emerald-400 animate-pulse' :
                            slot.status === 'FILL_STAGING' ? 'bg-blue-400/80' :
                            'bg-zinc-800'
                          }`}
                        />
                      ))}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>

          {/* Bare-Metal Streaming Telemetry Strip */}
          <div className="bg-zinc-900/50 border border-zinc-800 p-3 space-y-3">
            <div className="text-xs font-bold text-zinc-400 flex items-center justify-between">
              <span>HARDWARE FSYNC & WATCHDOG MONITORING</span>
              <span className="text-[10px] text-emerald-400">JEDEC NON-VOLATILE SPEC COMPLIANT</span>
            </div>

            <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-center text-xs">
              <div className="p-2 bg-zinc-950 border border-zinc-800">
                <div className="text-[9px] text-zinc-500 uppercase">IO Timeout Limit</div>
                <div className="font-bold text-zinc-200 mt-0.5">{FSYNC_TIMEOUT_THRESHOLD_US / 1000} ms</div>
              </div>
              <div className="p-2 bg-zinc-950 border border-zinc-800">
                <div className="text-[9px] text-zinc-500 uppercase">Direct Bus IOPS</div>
                <div className="font-bold text-emerald-400 mt-0.5">
                  {Math.round((telemetry.activeThroughputMBps * 1024 * 1024) / (SECTOR_SIZE * 8))} IOPS
                </div>
              </div>
              <div className="p-2 bg-zinc-950 border border-zinc-800">
                <div className="text-[9px] text-zinc-500 uppercase">Ring Backpressure</div>
                <div className="font-bold text-zinc-300 mt-0.5">{(telemetry.backpressureRatio * 100).toFixed(1)}%</div>
              </div>
              <div className="p-2 bg-zinc-950 border border-zinc-800">
                <div className="text-[9px] text-zinc-500 uppercase">Physical LBA End</div>
                <div className="font-bold text-zinc-300 mt-0.5">
                  0x{telemetry.lbaTarget.toString(16).slice(-6).toUpperCase()}
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Right Column: Microkernel Telemetry Stream Log (4 Cols) */}
        <div className="lg:col-span-4 flex flex-col h-[480px] bg-zinc-950 border border-zinc-800">
          <div className="p-2.5 bg-zinc-900 border-b border-zinc-800 flex items-center justify-between">
            <span className="text-xs font-bold text-zinc-300 flex items-center gap-1.5">
              <span className="inline-block w-2 h-2 bg-emerald-500 rounded-none" />
              LIVE TELEMETRY BUS
            </span>
            <span className="text-[10px] text-zinc-500">RAW PIPE LOGS</span>
          </div>

          <div className="flex-1 p-2 overflow-y-auto space-y-1 font-mono text-[10px] select-text">
            {logTail.length === 0 ? (
              <div className="text-zinc-600 italic p-4 text-center">Pipeline stream inactive. Hardware standby.</div>
            ) : (
              logTail.map(log => (
                <div key={log.id} className="leading-tight flex gap-1.5">
                  <span className="text-zinc-600 select-none">{log.timestamp}</span>
                  <span className={`px-1 py-0 rounded text-[9px] font-bold ${
                    log.channel === 'KERNEL' ? 'bg-zinc-800 text-zinc-300' :
                    log.channel === 'FSYNC' ? 'bg-amber-950/80 text-amber-300' :
                    log.channel === 'DMA' ? 'bg-blue-950/80 text-blue-300' :
                    'bg-emerald-950/80 text-emerald-300'
                  }`}>
                    {log.channel}
                  </span>
                  <span className="text-zinc-300 break-all">{log.msg}</span>
                </div>
              ))
            )}
          </div>

          {/* Micro Terminal Command Bar */}
          <div className="p-2 border-t border-zinc-800 bg-zinc-900/60 flex items-center justify-between text-[10px] text-zinc-400">
            <span className="flex items-center gap-1">
              <span className="text-emerald-500 font-bold">&gt;</span>
              <span>DEV_ENDPOINT_OUT: {deviceDescriptor ? `EP_${deviceDescriptor.endpointOut}` : 'NONE'}</span>
            </span>
            <span className="text-zinc-500">SHARED_WASM_MEM: 16MB</span>
          </div>
        </div>

      </main>

      {/* FOOTER BARRIER STATS */}
      <footer className="p-2.5 bg-zinc-900/40 border-t border-zinc-800 text-[10px] flex flex-wrap items-center justify-between text-zinc-500">
        <div className="flex items-center space-x-4">
          <span>HOST TRANSPORT: <strong className="text-zinc-300">Raw WebStreams API / Vite 6 Direct Memory</strong></span>
          <span>PIPELINE ENGINE: <strong className="text-zinc-300">Rust Wasm-Bindgen SIMD Accelerated</strong></span>
        </div>
        <div>
          <span>CRC32 CASTAGNOLI HARDWARE ENFORCED • ZERO-COPY KERNEL PROVISIONING</span>
        </div>
      </footer>

    </div>
  );
}