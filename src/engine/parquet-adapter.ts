/**
 * Parquet Storage Adapter for ParqueDB's Hybrid MergeTree Engine
 *
 * Implements the FullStorageAdapter interface using real Parquet files via
 * hyparquet (reader) and hyparquet-writer (writer). This adapter stores
 * compacted data in columnar Parquet format rather than JSON, providing
 * better compression and enabling predicate pushdown for queries.
 *
 * Column layouts:
 * - Data files: $id (string), $op (string), $v (number), $ts (number), $data (string/JSON)
 * - Rels files: $op (string), $ts (number), f (string), p (string), r (string), t (string)
 * - Events files: id (string), ts (number), op (string), ns (string), eid (string),
 *                 before (string/JSON), after (string/JSON), actor (string)
 */

import { readFile, writeFile, mkdir } from 'node:fs/promises'
import { dirname } from 'node:path'
import type { FullStorageAdapter } from './storage-adapters'
import type { DataLine, RelLine } from './types'

// =============================================================================
// Helpers
// =============================================================================

/** System fields stored as dedicated Parquet columns for DataLine */
const DATA_SYSTEM_FIELDS = new Set(['$id', '$op', '$v', '$ts'])

/**
 * Coerce a value to number. Handles BigInt (legacy INT64 files) and number (DOUBLE).
 */
function toNumber(value: unknown): number {
  if (typeof value === 'number') return value
  if (typeof value === 'bigint') return Number(value)
  return 0
}

/**
 * Create an AsyncBuffer wrapper around a Node.js Buffer for hyparquet.
 * hyparquet requires an object with byteLength and an async slice method.
 */
function toAsyncBuffer(buffer: Buffer): { byteLength: number; slice: (start: number, end?: number) => Promise<ArrayBuffer> } {
  return {
    byteLength: buffer.byteLength,
    slice: async (start: number, end?: number) => {
      const sliced = buffer.slice(start, end ?? buffer.byteLength)
      return sliced.buffer.slice(sliced.byteOffset, sliced.byteOffset + sliced.byteLength)
    },
  }
}

/**
 * Ensure the parent directory for a file path exists.
 */
async function ensureDir(path: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true })
}

// =============================================================================
// ParquetStorageAdapter
// =============================================================================

/**
 * Storage adapter that reads/writes Parquet files using hyparquet/hyparquet-writer.
 *
 * Data entities are stored with system fields ($id, $op, $v, $ts) as typed
 * columns and all remaining fields packed into a $data JSON string column.
 * This provides efficient columnar access to system fields while preserving
 * arbitrary entity data.
 *
 * Relationships and events are stored with direct column mappings matching
 * their respective schemas.
 */
export class ParquetStorageAdapter implements FullStorageAdapter {
  // ---------------------------------------------------------------------------
  // Data operations
  // ---------------------------------------------------------------------------

  async readData(path: string): Promise<DataLine[]> {
    let fileData: Buffer
    try {
      fileData = await readFile(path)
    } catch {
      return []
    }

    if (fileData.byteLength === 0) {
      return []
    }

    const { parquetReadObjects } = await import('hyparquet')
    const asyncBuffer = toAsyncBuffer(fileData)

    const rows = await parquetReadObjects({ file: asyncBuffer }) as Array<{
      $id: string
      $op: string
      $v: number
      $ts: number
      $data: string
    }>

    return rows.map(row => {
      const dataFields = row.$data ? JSON.parse(row.$data) as Record<string, unknown> : {}
      return {
        ...dataFields,
        $id: row.$id,
        $op: row.$op as DataLine['$op'],
        $v: toNumber(row.$v),
        $ts: toNumber(row.$ts),
      }
    })
  }

  async writeData(path: string, data: DataLine[]): Promise<void> {
    await ensureDir(path)

    // Sort by $id for deterministic output and efficient lookups
    const sorted = [...data].sort((a, b) => (a.$id < b.$id ? -1 : a.$id > b.$id ? 1 : 0))

    // Separate system fields from data fields
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

      // Pack remaining fields into $data JSON
      const dataFields: Record<string, unknown> = {}
      for (const [key, value] of Object.entries(entity)) {
        if (!DATA_SYSTEM_FIELDS.has(key)) {
          dataFields[key] = value
        }
      }
      dataJsons.push(JSON.stringify(dataFields))
    }

    const columnData = [
      { name: '$id', data: ids },
      { name: '$op', data: ops },
      { name: '$v', data: versions },
      { name: '$ts', data: timestamps, type: 'DOUBLE' as const },
      { name: '$data', data: dataJsons },
    ]

    const { parquetWriteBuffer } = await import('hyparquet-writer')
    const buffer = parquetWriteBuffer({ columnData })
    await writeFile(path, new Uint8Array(buffer))
  }

  // ---------------------------------------------------------------------------
  // Rel operations
  // ---------------------------------------------------------------------------

  async readRels(path: string): Promise<RelLine[]> {
    let fileData: Buffer
    try {
      fileData = await readFile(path)
    } catch {
      return []
    }

    if (fileData.byteLength === 0) {
      return []
    }

    const { parquetReadObjects } = await import('hyparquet')
    const asyncBuffer = toAsyncBuffer(fileData)

    const rows = await parquetReadObjects({ file: asyncBuffer }) as Array<{
      $op: string
      $ts: number
      f: string
      p: string
      r: string
      t: string
    }>

    return rows.map(row => ({
      $op: row.$op as RelLine['$op'],
      $ts: toNumber(row.$ts),
      f: row.f,
      p: row.p,
      r: row.r,
      t: row.t,
    }))
  }

  async writeRels(path: string, data: RelLine[]): Promise<void> {
    await ensureDir(path)

    const columnData = [
      { name: '$op', data: data.map(r => r.$op) },
      { name: '$ts', data: data.map(r => r.$ts + 0.0), type: 'DOUBLE' as const },
      { name: 'f', data: data.map(r => r.f) },
      { name: 'p', data: data.map(r => r.p) },
      { name: 'r', data: data.map(r => r.r) },
      { name: 't', data: data.map(r => r.t) },
    ]

    const { parquetWriteBuffer } = await import('hyparquet-writer')
    const buffer = parquetWriteBuffer({ columnData })
    await writeFile(path, new Uint8Array(buffer))
  }

  // ---------------------------------------------------------------------------
  // Event operations
  // ---------------------------------------------------------------------------

  async readEvents(path: string): Promise<Record<string, unknown>[]> {
    let fileData: Buffer
    try {
      fileData = await readFile(path)
    } catch {
      return []
    }

    if (fileData.byteLength === 0) {
      return []
    }

    const { parquetReadObjects } = await import('hyparquet')
    const asyncBuffer = toAsyncBuffer(fileData)

    const rows = await parquetReadObjects({ file: asyncBuffer }) as Array<{
      id: string
      ts: number
      op: string
      ns: string
      eid: string
      before: string
      after: string
      actor: string
    }>

    return rows.map(row => {
      const event: Record<string, unknown> = {
        id: row.id,
        ts: toNumber(row.ts),
        op: row.op,
        ns: row.ns,
        eid: row.eid,
      }

      if (row.before) {
        event.before = JSON.parse(row.before)
      }
      if (row.after) {
        event.after = JSON.parse(row.after)
      }
      if (row.actor) {
        event.actor = row.actor
      }

      return event
    })
  }

  async writeEvents(path: string, data: Record<string, unknown>[]): Promise<void> {
    await ensureDir(path)

    const columnData = [
      { name: 'id', data: data.map(e => (e.id as string) ?? '') },
      { name: 'ts', data: data.map(e => ((e.ts as number) ?? 0) + 0.0), type: 'DOUBLE' as const },
      { name: 'op', data: data.map(e => (e.op as string) ?? '') },
      { name: 'ns', data: data.map(e => (e.ns as string) ?? '') },
      { name: 'eid', data: data.map(e => (e.eid as string) ?? '') },
      { name: 'before', data: data.map(e => e.before ? JSON.stringify(e.before) : '') },
      { name: 'after', data: data.map(e => e.after ? JSON.stringify(e.after) : '') },
      { name: 'actor', data: data.map(e => (e.actor as string) ?? '') },
    ]

    const { parquetWriteBuffer } = await import('hyparquet-writer')
    const buffer = parquetWriteBuffer({ columnData })
    await writeFile(path, new Uint8Array(buffer))
  }
}
