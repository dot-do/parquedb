/**
 * Worker Thread Compaction for MergeTree Engine
 *
 * Offloads CPU-intensive Parquet encoding during compaction to a Worker thread.
 * The main thread sends data to encode, the worker returns the Parquet buffer.
 * This keeps the main thread responsive for reads/writes during compaction.
 *
 * Falls back to in-process compaction when Worker threads are unavailable.
 *
 * The encoding functions (encodeDataToParquet, encodeRelsToParquet,
 * encodeEventsToParquet) are exported as standalone functions so they can
 * be tested directly without requiring worker thread instantiation.
 */

import { Worker } from 'node:worker_threads'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { DataLine, RelLine } from './types'

// =============================================================================
// Message Types
// =============================================================================

/** Message sent from main thread to worker */
export interface CompactionRequest {
  type: 'encode-data' | 'encode-rels' | 'encode-events'
  id: string
  payload: unknown
}

/** Message sent from worker back to main thread */
export interface CompactionResult {
  type: 'encoded'
  id: string
  buffer: ArrayBuffer
  error?: string
}

// =============================================================================
// Standalone Encoding Functions
// =============================================================================

/** System fields stored as dedicated Parquet columns for DataLine */
const DATA_SYSTEM_FIELDS = new Set(['$id', '$op', '$v', '$ts'])

/**
 * Encode an array of DataLine entities into a Parquet buffer.
 *
 * Sorts by $id for deterministic output, separates system fields from
 * data fields, and packs remaining fields into a $data JSON column.
 */
export async function encodeDataToParquet(
  data: Array<{ $id: string; $op: string; $v: number; $ts: number; [key: string]: unknown }>,
): Promise<ArrayBuffer> {
  const { parquetWriteBuffer } = await import('hyparquet-writer')

  const sorted = [...data].sort((a, b) => (a.$id < b.$id ? -1 : a.$id > b.$id ? 1 : 0))

  const ids: string[] = []
  const ops: string[] = []
  const versions: number[] = []
  const timestamps: number[] = []
  const dataJsons: string[] = []

  for (const entity of sorted) {
    ids.push(entity.$id)
    ops.push(entity.$op)
    versions.push(entity.$v)
    timestamps.push(entity.$ts + 0.0) // ensure DOUBLE (not INT32) via float coercion
    const dataFields: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(entity)) {
      if (!DATA_SYSTEM_FIELDS.has(key)) {
        dataFields[key] = value
      }
    }
    dataJsons.push(JSON.stringify(dataFields))
  }

  return parquetWriteBuffer({
    columnData: [
      { name: '$id', data: ids },
      { name: '$op', data: ops },
      { name: '$v', data: versions },
      { name: '$ts', data: timestamps, type: 'DOUBLE' as const },
      { name: '$data', data: dataJsons },
    ],
  })
}

/**
 * Encode an array of RelLine relationships into a Parquet buffer.
 */
export async function encodeRelsToParquet(
  rels: Array<{ $op: string; $ts: number; f: string; p: string; r: string; t: string }>,
): Promise<ArrayBuffer> {
  const { parquetWriteBuffer } = await import('hyparquet-writer')

  return parquetWriteBuffer({
    columnData: [
      { name: '$op', data: rels.map(r => r.$op) },
      { name: '$ts', data: rels.map(r => r.$ts + 0.0), type: 'DOUBLE' as const },
      { name: 'f', data: rels.map(r => r.f) },
      { name: 'p', data: rels.map(r => r.p) },
      { name: 'r', data: rels.map(r => r.r) },
      { name: 't', data: rels.map(r => r.t) },
    ],
  })
}

/**
 * Encode an array of event records into a Parquet buffer.
 */
export async function encodeEventsToParquet(
  events: Array<Record<string, unknown>>,
): Promise<ArrayBuffer> {
  const { parquetWriteBuffer } = await import('hyparquet-writer')

  return parquetWriteBuffer({
    columnData: [
      { name: 'id', data: events.map(e => (e.id as string) ?? '') },
      { name: 'ts', data: events.map(e => ((e.ts as number) ?? 0) + 0.0), type: 'DOUBLE' as const },
      { name: 'op', data: events.map(e => (e.op as string) ?? '') },
      { name: 'ns', data: events.map(e => (e.ns as string) ?? '') },
      { name: 'eid', data: events.map(e => (e.eid as string) ?? '') },
      { name: 'before', data: events.map(e => e.before ? JSON.stringify(e.before) : '') },
      { name: 'after', data: events.map(e => e.after ? JSON.stringify(e.after) : '') },
      { name: 'actor', data: events.map(e => (e.actor as string) ?? '') },
    ],
  })
}

// =============================================================================
// CompactionWorker
// =============================================================================

/**
 * CompactionWorker manages a background worker thread for Parquet encoding.
 *
 * Usage:
 *   const worker = new CompactionWorker()
 *   await worker.start()
 *   const buffer = await worker.encodeData(dataLines)
 *   await worker.stop()
 *
 * If worker threads are unavailable (e.g., in Cloudflare Workers), the
 * start() method sets ready=false and callers should fall back to the
 * standalone encoding functions (encodeDataToParquet, etc.) directly.
 */
export class CompactionWorker {
  private worker: Worker | null = null
  private pending = new Map<string, { resolve: (buf: ArrayBuffer) => void; reject: (err: Error) => void }>()
  private requestCounter = 0
  private _ready = false

  /** Start the worker thread */
  async start(): Promise<void> {
    if (this.worker) return

    const workerPath = join(dirname(fileURLToPath(import.meta.url)), 'compaction-worker.worker.js')

    try {
      this.worker = new Worker(workerPath)
      this.worker.on('message', (msg: CompactionResult) => {
        const pending = this.pending.get(msg.id)
        if (!pending) return
        this.pending.delete(msg.id)
        if (msg.error) {
          pending.reject(new Error(msg.error))
        } else {
          pending.resolve(msg.buffer)
        }
      })
      this.worker.on('error', (err) => {
        // Reject all pending requests on worker error
        for (const [, pending] of this.pending) {
          pending.reject(err)
        }
        this.pending.clear()
      })
      this._ready = true
    } catch {
      // Worker threads not available, fall back to in-process encoding
      this._ready = false
    }
  }

  /** Check if the worker is ready */
  get ready(): boolean {
    return this._ready && this.worker !== null
  }

  /** Encode data lines to Parquet buffer in worker thread */
  async encodeData(
    data: Array<{ $id: string; $op: string; $v: number; $ts: number; [key: string]: unknown }>,
  ): Promise<ArrayBuffer> {
    return this.sendRequest({ type: 'encode-data', id: this.nextId(), payload: data })
  }

  /** Encode relationship lines to Parquet buffer in worker thread */
  async encodeRels(
    rels: Array<{ $op: string; $ts: number; f: string; p: string; r: string; t: string }>,
  ): Promise<ArrayBuffer> {
    return this.sendRequest({ type: 'encode-rels', id: this.nextId(), payload: rels })
  }

  /** Encode event records to Parquet buffer in worker thread */
  async encodeEvents(events: Array<Record<string, unknown>>): Promise<ArrayBuffer> {
    return this.sendRequest({ type: 'encode-events', id: this.nextId(), payload: events })
  }

  /** Stop the worker thread */
  async stop(): Promise<void> {
    if (this.worker) {
      await this.worker.terminate()
      this.worker = null
      this._ready = false
    }
  }

  private nextId(): string {
    return `req-${++this.requestCounter}`
  }

  private sendRequest(request: CompactionRequest): Promise<ArrayBuffer> {
    if (!this.worker) {
      return Promise.reject(new Error('Worker not started'))
    }
    return new Promise((resolve, reject) => {
      this.pending.set(request.id, { resolve, reject })
      this.worker!.postMessage(request)
    })
  }
}
