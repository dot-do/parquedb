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
 *
 * Encoding is delegated to parquet-encoders.ts (single source of truth).
 */

import { readFile, writeFile, mkdir } from 'node:fs/promises'
import { dirname } from 'node:path'
import type { FullStorageAdapter } from './storage-adapters'
import type { DataLine, RelLine, EventLine } from './types'
import type { AnyEventLine } from './merge-events'
import { encodeDataToParquet, encodeRelsToParquet, encodeEventsToParquet } from './parquet-encoders'
import { toNumber } from './utils'
import { parseDataField } from './parquet-data-utils'

// =============================================================================
// Helpers
// =============================================================================

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
 * columns and all remaining fields packed into a $data column. The $data
 * column uses Parquet's JSON converted type, which hyparquet auto-decodes
 * to JS objects on read. For backward compatibility with older files that
 * used plain UTF8 strings, the reader also handles string $data via
 * JSON.parse fallback.
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
    } catch (error: unknown) {
      if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'ENOENT') {
        return []
      }
      throw error
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
      $data: unknown
    }>

    return rows.map(row => {
      const dataFields = parseDataField(row.$data)
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
    const buffer = await encodeDataToParquet(data)
    await writeFile(path, new Uint8Array(buffer))
  }

  // ---------------------------------------------------------------------------
  // Rel operations
  // ---------------------------------------------------------------------------

  async readRels(path: string): Promise<RelLine[]> {
    let fileData: Buffer
    try {
      fileData = await readFile(path)
    } catch (error: unknown) {
      if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'ENOENT') {
        return []
      }
      throw error
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
    const buffer = await encodeRelsToParquet(data)
    await writeFile(path, new Uint8Array(buffer))
  }

  // ---------------------------------------------------------------------------
  // Event operations
  // ---------------------------------------------------------------------------

  async readEvents(path: string): Promise<AnyEventLine[]> {
    let fileData: Buffer
    try {
      fileData = await readFile(path)
    } catch (error: unknown) {
      if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'ENOENT') {
        return []
      }
      throw error
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
      const event: EventLine = {
        id: row.id,
        ts: toNumber(row.ts),
        op: row.op as EventLine['op'],
        ns: row.ns,
        eid: row.eid,
      }

      if (row.before) {
        try {
          event.before = JSON.parse(row.before) as Record<string, unknown>
        } catch {
          console.warn(`[parquet-adapter] Skipping corrupted before JSON for event ${row.id}: ${row.before.slice(0, 100)}`)
        }
      }
      if (row.after) {
        try {
          event.after = JSON.parse(row.after) as Record<string, unknown>
        } catch {
          console.warn(`[parquet-adapter] Skipping corrupted after JSON for event ${row.id}: ${row.after.slice(0, 100)}`)
        }
      }
      if (row.actor) {
        event.actor = row.actor
      }

      return event
    })
  }

  async writeEvents(path: string, data: AnyEventLine[]): Promise<void> {
    await ensureDir(path)
    const buffer = await encodeEventsToParquet(data)
    await writeFile(path, new Uint8Array(buffer))
  }
}
