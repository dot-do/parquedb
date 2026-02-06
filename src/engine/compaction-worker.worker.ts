/**
 * Worker Thread Script for Parquet Encoding
 *
 * Receives data from the main thread and encodes it as Parquet buffers.
 * Runs in a separate thread to avoid blocking the main event loop.
 *
 * Imports encoding functions directly from parquet-encoders.ts (the canonical
 * source) to avoid pulling in unnecessary node:worker_threads dependencies
 * through compaction-worker.ts.
 */

import { parentPort } from 'node:worker_threads'
import type { CompactionRequest, CompactionResult } from './compaction-worker'
import { encodeDataToParquet, encodeRelsToParquet, encodeEventsToParquet } from './parquet-encoders'

if (parentPort) {
  parentPort.on('message', async (msg: CompactionRequest) => {
    try {
      let buffer: ArrayBuffer

      switch (msg.type) {
        case 'encode-data': {
          // msg.payload is narrowed to DataLine[] by the discriminated union
          buffer = await encodeDataToParquet(msg.payload)
          break
        }

        case 'encode-rels': {
          // msg.payload is narrowed to RelLine[] by the discriminated union
          buffer = await encodeRelsToParquet(msg.payload)
          break
        }

        case 'encode-events': {
          // msg.payload is narrowed to AnyEventLine[] by the discriminated union
          buffer = await encodeEventsToParquet(msg.payload)
          break
        }

        default:
          throw new Error(`Unknown message type: ${(msg as { type: string }).type}`)
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
