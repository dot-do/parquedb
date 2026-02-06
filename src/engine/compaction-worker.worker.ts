/**
 * Worker Thread Script for Parquet Encoding
 *
 * Receives data from the main thread and encodes it as Parquet buffers.
 * Runs in a separate thread to avoid blocking the main event loop.
 *
 * Uses the same standalone encoding functions exported from
 * compaction-worker.ts to ensure consistent encoding between
 * in-process and worker thread paths.
 */

import { parentPort } from 'node:worker_threads'
import type { CompactionRequest, CompactionResult } from './compaction-worker'
import { encodeDataToParquet, encodeRelsToParquet, encodeEventsToParquet } from './compaction-worker'

if (parentPort) {
  parentPort.on('message', async (msg: CompactionRequest) => {
    try {
      let buffer: ArrayBuffer

      switch (msg.type) {
        case 'encode-data': {
          const data = msg.payload as Array<{ $id: string; $op: string; $v: number; $ts: number; [key: string]: unknown }>
          buffer = await encodeDataToParquet(data)
          break
        }

        case 'encode-rels': {
          const rels = msg.payload as Array<{ $op: string; $ts: number; f: string; p: string; r: string; t: string }>
          buffer = await encodeRelsToParquet(rels)
          break
        }

        case 'encode-events': {
          const events = msg.payload as Array<Record<string, unknown>>
          buffer = await encodeEventsToParquet(events)
          break
        }

        default:
          throw new Error(`Unknown message type: ${msg.type}`)
      }

      const result: CompactionResult = { type: 'encoded', id: msg.id, buffer }
      parentPort!.postMessage(result, [buffer]) // Transfer buffer for zero-copy
    } catch (err) {
      const result: CompactionResult = {
        type: 'encoded',
        id: msg.id,
        buffer: new ArrayBuffer(0),
        error: String(err),
      }
      parentPort!.postMessage(result)
    }
  })
}
