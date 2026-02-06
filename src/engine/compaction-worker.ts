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
 * encodeEventsToParquet) are re-exported from parquet-encoders.ts which is
 * the single source of truth. This module re-exports them for backward
 * compatibility.
 */

import { Worker } from 'node:worker_threads'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { DataLine, RelLine } from './types'
import type { AnyEventLine } from './merge-events'

// =============================================================================
// Re-export encoding functions from the canonical source
// =============================================================================

export { encodeDataToParquet, encodeRelsToParquet, encodeEventsToParquet } from './parquet-encoders'

// =============================================================================
// Message Types
// =============================================================================

/** Message sent from main thread to worker (discriminated union on `type`) */
export type CompactionRequest =
  | { type: 'encode-data'; id: string; payload: DataLine[] }
  | { type: 'encode-rels'; id: string; payload: RelLine[] }
  | { type: 'encode-events'; id: string; payload: AnyEventLine[] }

/** Message sent from worker back to main thread */
export interface CompactionResult {
  type: 'encoded'
  id: string
  buffer: ArrayBuffer
  error?: string
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
  async encodeData(data: DataLine[]): Promise<ArrayBuffer> {
    return this.sendRequest({ type: 'encode-data', id: this.nextId(), payload: data })
  }

  /** Encode relationship lines to Parquet buffer in worker thread */
  async encodeRels(rels: RelLine[]): Promise<ArrayBuffer> {
    return this.sendRequest({ type: 'encode-rels', id: this.nextId(), payload: rels })
  }

  /** Encode event records to Parquet buffer in worker thread */
  async encodeEvents(events: Array<AnyEventLine>): Promise<ArrayBuffer> {
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
    const worker = this.worker
    return new Promise((resolve, reject) => {
      this.pending.set(request.id, { resolve, reject })
      worker.postMessage(request)
    })
  }
}
