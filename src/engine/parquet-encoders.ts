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
 *
 * The $data column uses Parquet VARIANT type, which stores semi-structured
 * data in an efficient binary format with automatic columnar compression.
 * hyparquet auto-decodes VARIANT columns to JS objects on read, so no
 * manual JSON.parse is needed.
 *
 * Benefits:
 * - hyparquet auto-decodes $data to JS objects (no manual JSON.parse needed)
 * - Analytics tools recognize the column as structured VARIANT data
 * - Columnar compression applies to the binary VARIANT encoding
 * - Predicate pushdown possible via shredded fields (future per-collection config)
 * - Native support for mixed types, nested objects, arrays, nulls
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
 * data fields, and packs remaining fields into a $data VARIANT column.
 *
 * The $data column uses Parquet's VARIANT type (a group with metadata
 * and value binary sub-columns). hyparquet auto-decodes VARIANT columns
 * to JS objects on read. No manual JSON.parse is needed.
 *
 * System fields ($id, $op, $v, $ts) are stored as dedicated typed columns.
 * All other entity fields are stored in the $data VARIANT column.
 */
export async function encodeDataToParquet(
  data: Array<{ $id: string; $op: string; $v: number; $ts: number; [key: string]: unknown }>,
): Promise<ArrayBuffer> {
  const { parquetWriteBuffer, createVariantColumn, autoSchemaElement } = await import('hyparquet-writer')

  const sorted = [...data].sort((a, b) => (a.$id < b.$id ? -1 : a.$id > b.$id ? 1 : 0))

  const ids: string[] = []
  const ops: string[] = []
  const versions: number[] = []
  const timestamps: number[] = []
  const dataObjects: Record<string, unknown>[] = []

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
    dataObjects.push(dataFields)
  }

  // Create VARIANT column for $data (schema + encoded binary data)
  const variantCol = createVariantColumn('$data', dataObjects)

  // Build the full schema: root + system columns + VARIANT $data group
  const schema = [
    // Root schema element
    {
      name: 'root',
      num_children: 5, // $id, $op, $v, $ts, $data
    },
    // System field schemas (auto-detected from data)
    autoSchemaElement('$id', ids),
    autoSchemaElement('$op', ops),
    autoSchemaElement('$v', versions),
    { name: '$ts', type: 'DOUBLE' as const, repetition_type: 'REQUIRED' as const },
    // VARIANT $data schema elements (group + metadata + value)
    ...variantCol.schema,
  ]

  return parquetWriteBuffer({
    schema,
    columnData: [
      { name: '$id', data: ids },
      { name: '$op', data: ops },
      { name: '$v', data: versions },
      { name: '$ts', data: timestamps },
      { name: '$data', data: variantCol.data },
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
