/**
 * Shared R2 Parquet Utilities
 *
 * Common helpers for reading Parquet files from R2 and decoding rows into
 * typed engine line objects. Extracted to eliminate duplication across:
 * - do-compactor.ts (DOCompactor)
 * - do-read-path.ts (DOReadPath)
 *
 * Row decoding functions are also used by parquet-adapter.ts (Node.js path).
 */

import type { DataLine, RelLine, EventLine } from './types'
import { toNumber } from './utils'
import { parseDataField } from './parquet-data-utils'

// =============================================================================
// R2 Parquet reader
// =============================================================================

/**
 * Read a Parquet file from R2 and decode rows using hyparquet.
 * Returns an empty array if the key does not exist.
 */
export async function readParquetFromR2(
  bucket: R2Bucket,
  key: string,
): Promise<Record<string, unknown>[]> {
  const obj = await bucket.get(key)
  if (!obj) return []

  const buffer = await obj.arrayBuffer()
  if (buffer.byteLength === 0) return []

  const { parquetReadObjects } = await import('hyparquet')
  const asyncBuffer = {
    byteLength: buffer.byteLength,
    slice: async (start: number, end?: number) =>
      buffer.slice(start, end ?? buffer.byteLength),
  }

  return (await parquetReadObjects({ file: asyncBuffer })) as Record<string, unknown>[]
}

// =============================================================================
// Row decoders
// =============================================================================

/**
 * Decode raw Parquet rows into DataLine objects.
 *
 * System fields ($id, $op, $v, $ts) are extracted as typed columns.
 * The $data column is parsed via parseDataField which handles VARIANT binary,
 * JSON objects (auto-decoded by hyparquet), and legacy JSON strings.
 */
export function decodeDataRows(rows: Record<string, unknown>[]): DataLine[] {
  return rows.map((row) => {
    const dataFields = parseDataField(row.$data)
    return {
      ...dataFields,
      $id: row.$id as string,
      $op: row.$op as DataLine['$op'],
      $v: toNumber(row.$v),
      $ts: toNumber(row.$ts),
    }
  })
}

/**
 * Decode raw Parquet rows into RelLine objects.
 */
export function decodeRelRows(rows: Record<string, unknown>[]): RelLine[] {
  return rows.map((row) => ({
    $op: row.$op as RelLine['$op'],
    $ts: toNumber(row.$ts),
    f: row.f as string,
    p: row.p as string,
    r: row.r as string,
    t: row.t as string,
  }))
}

/**
 * Decode raw Parquet event rows into EventLine records.
 *
 * Handles optional before/after JSON fields and actor field.
 * Logs warnings for corrupted JSON but does not throw.
 */
export function decodeEventRows(rows: Record<string, unknown>[]): EventLine[] {
  return rows.map((row) => {
    const event: EventLine = {
      id: row.id as string,
      ts: toNumber(row.ts),
      op: row.op as EventLine['op'],
      ns: row.ns as string,
      eid: row.eid as string,
    }

    const before = row.before as string | undefined
    if (before && before !== '') {
      try {
        event.before = JSON.parse(before) as Record<string, unknown>
      } catch {
        console.warn(`[r2-parquet-utils] Skipping corrupted before JSON for event ${event.id}: ${before.slice(0, 100)}`)
      }
    }

    const after = row.after as string | undefined
    if (after && after !== '') {
      try {
        event.after = JSON.parse(after) as Record<string, unknown>
      } catch {
        console.warn(`[r2-parquet-utils] Skipping corrupted after JSON for event ${event.id}: ${after.slice(0, 100)}`)
      }
    }

    const actor = row.actor as string | undefined
    if (actor && actor !== '') {
      event.actor = actor
    }

    return event
  })
}
