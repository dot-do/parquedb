/**
 * Parquet Encoding Functions for MergeTree Engine
 *
 * Standalone encoding functions for data, rels, and events.
 * These are safe to import from any runtime (Node.js, Cloudflare Workers)
 * because they have no node:-prefixed imports at the top level.
 *
 * Originally lived in compaction-worker.ts but extracted here to avoid
 * pulling in node:worker_threads when imported from Workers-side code
 * (e.g., DOCompactor).
 */

import type { DataLine, RelLine } from './types'
import type { AnyEventLine } from './merge-events'
import { DATA_SYSTEM_FIELDS } from './utils'

// =============================================================================
// Standalone Encoding Functions
// =============================================================================

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
  events: Array<AnyEventLine>,
): Promise<ArrayBuffer> {
  const { parquetWriteBuffer } = await import('hyparquet-writer')

  return parquetWriteBuffer({
    columnData: [
      { name: 'id', data: events.map(e => e.id ?? '') },
      { name: 'ts', data: events.map(e => (e.ts ?? 0) + 0.0), type: 'DOUBLE' as const },
      { name: 'op', data: events.map(e => e.op ?? '') },
      { name: 'ns', data: events.map(e => e.ns ?? '') },
      { name: 'eid', data: events.map(e => 'eid' in e ? e.eid : '') },
      { name: 'before', data: events.map(e => 'before' in e && e.before ? JSON.stringify(e.before) : '') },
      { name: 'after', data: events.map(e => 'after' in e && e.after ? JSON.stringify(e.after) : '') },
      { name: 'actor', data: events.map(e => ('actor' in e ? (e.actor as string) : '') ?? '') },
    ],
  })
}
