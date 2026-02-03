/**
 * ParqueDB Tiny Export (~15KB target)
 *
 * A minimal read-only Parquet query function for maximum tree-shakeability.
 * Designed for applications that only need to read and filter Parquet data.
 *
 * Features:
 * - Direct buffer input (ArrayBuffer)
 * - Simple filter evaluation
 * - Streaming row iteration
 * - Column projection
 *
 * Excludes:
 * - Write operations
 * - Storage backends
 * - Relationships
 * - Collections
 * - Update operators
 * - All other ParqueDB features
 *
 * @packageDocumentation
 * @module parquedb/tiny
 */

import { parquetReadObjects, parquetMetadataAsync } from 'hyparquet'
import type { FileMetaData } from 'hyparquet'
import { createSafeRegex } from '../utils/safe-regex'

// =============================================================================
// Types
// =============================================================================

/**
 * Simple filter operators for tiny export
 */
export interface TinyFilter {
  [field: string]: unknown | TinyOperator
}

/**
 * Filter operators
 */
export interface TinyOperator {
  $eq?: unknown
  $ne?: unknown
  $gt?: number | string | Date
  $gte?: number | string | Date
  $lt?: number | string | Date
  $lte?: number | string | Date
  $in?: unknown[]
  $nin?: unknown[]
  $exists?: boolean
  $regex?: string | RegExp
}

/**
 * Query options for parquetQuery
 */
export interface QueryOptions {
  /** Columns to read (default: all) */
  columns?: string[]
  /** Row groups to read (default: all) */
  rowGroups?: number[]
  /** Maximum rows to return */
  limit?: number
  /** Rows to skip */
  offset?: number
}

/**
 * Row type - generic record
 */
export type Row = Record<string, unknown>

/**
 * Parquet metadata (simplified)
 */
export interface TinyParquetMetadata {
  /** Number of rows in file */
  numRows: number
  /** Number of row groups */
  numRowGroups: number
  /** Column names */
  columns: string[]
  /** File version */
  version: number
}

/**
 * AsyncBuffer interface for hyparquet
 */
interface AsyncBuffer {
  byteLength: number
  slice(start: number, end?: number): Promise<ArrayBuffer>
}

// =============================================================================
// Core Functions
// =============================================================================

/**
 * Create an AsyncBuffer from an ArrayBuffer
 */
function createBufferAdapter(buffer: ArrayBuffer): AsyncBuffer {
  return {
    byteLength: buffer.byteLength,
    slice(start: number, end?: number): Promise<ArrayBuffer> {
      const actualEnd = end ?? buffer.byteLength
      return Promise.resolve(buffer.slice(start, actualEnd))
    },
  }
}

/**
 * Get value at nested path using dot notation
 */
function getNestedValue(obj: Row, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) return undefined
    if (typeof current !== 'object') return undefined
    current = (current as Row)[part]
  }

  return current
}

/**
 * Check deep equality of two values
 */
function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true
  if (a === null || a === undefined) return b === null || b === undefined

  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime()
  }

  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false
    return a.every((v, i) => deepEqual(v, b[i]))
  }

  if (typeof a === 'object' && typeof b === 'object' && a !== null && b !== null) {
    const aObj = a as Row
    const bObj = b as Row
    const aKeys = Object.keys(aObj)
    const bKeys = Object.keys(bObj)
    if (aKeys.length !== bKeys.length) return false
    return aKeys.every(k => deepEqual(aObj[k], bObj[k]))
  }

  return false
}

/**
 * Compare two values for ordering
 */
function compareValues(a: unknown, b: unknown): number {
  if (a === null || a === undefined) {
    return b === null || b === undefined ? 0 : -1
  }
  if (b === null || b === undefined) return 1

  if (typeof a === 'number' && typeof b === 'number') return a - b
  if (typeof a === 'string' && typeof b === 'string') {
    if (a < b) return -1
    if (a > b) return 1
    return 0
  }
  if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime()

  return String(a).localeCompare(String(b))
}

/**
 * Check if a value matches a condition
 */
function matchesCondition(value: unknown, condition: unknown): boolean {
  // Null condition matches null/undefined
  if (condition === null) {
    return value === null || value === undefined
  }

  // Undefined condition always matches
  if (condition === undefined) {
    return true
  }

  // Check if condition is an operator object
  if (typeof condition === 'object' && condition !== null && !Array.isArray(condition) && !(condition instanceof Date)) {
    const ops = condition as TinyOperator
    const keys = Object.keys(ops)

    if (keys.some(k => k.startsWith('$'))) {
      // Operator object
      for (const [op, opValue] of Object.entries(ops)) {
        switch (op) {
          case '$eq':
            if (!deepEqual(value, opValue)) return false
            break

          case '$ne':
            if (deepEqual(value, opValue)) return false
            break

          case '$gt':
            if (value === null || value === undefined) return false
            if (compareValues(value, opValue) <= 0) return false
            break

          case '$gte':
            if (value === null || value === undefined) return false
            if (compareValues(value, opValue) < 0) return false
            break

          case '$lt':
            if (value === null || value === undefined) return false
            if (compareValues(value, opValue) >= 0) return false
            break

          case '$lte':
            if (value === null || value === undefined) return false
            if (compareValues(value, opValue) > 0) return false
            break

          case '$in':
            if (!Array.isArray(opValue)) return false
            if (!opValue.some(v => deepEqual(value, v))) return false
            break

          case '$nin':
            if (!Array.isArray(opValue)) return false
            if (opValue.some(v => deepEqual(value, v))) return false
            break

          case '$exists':
            if (opValue === true && value === undefined) return false
            if (opValue === false && value !== undefined) return false
            break

          case '$regex': {
            if (typeof value !== 'string') return false
            const pattern = createSafeRegex(opValue as string | RegExp)
            if (!pattern.test(value)) return false
            break
          }

          default:
            // Unknown operator - ignore
            break
        }
      }
      return true
    }
  }

  // Direct equality
  return deepEqual(value, condition)
}

/**
 * Check if a row matches a filter
 */
export function matchesFilter(row: Row, filter: TinyFilter): boolean {
  if (!filter || Object.keys(filter).length === 0) {
    return true
  }

  for (const [field, condition] of Object.entries(filter)) {
    // Skip logical operators (not supported in tiny)
    if (field.startsWith('$')) continue

    const fieldValue = getNestedValue(row, field)
    if (!matchesCondition(fieldValue, condition)) {
      return false
    }
  }

  return true
}

/**
 * Query a Parquet buffer and return matching rows
 *
 * @param buffer - ArrayBuffer containing Parquet file data
 * @param filter - Optional filter to apply
 * @param options - Query options (columns, limit, offset)
 * @returns Array of matching rows
 *
 * @example
 * ```typescript
 * import { parquetQuery } from 'parquedb/tiny'
 *
 * const buffer = await fetch('/data.parquet').then(r => r.arrayBuffer())
 *
 * // Read all rows
 * const allRows = await parquetQuery(buffer)
 *
 * // Filter by field
 * const filtered = await parquetQuery(buffer, { status: 'active' })
 *
 * // With operators
 * const expensive = await parquetQuery(buffer, { price: { $gt: 100 } })
 *
 * // With projection and limit
 * const limited = await parquetQuery(buffer, { category: 'books' }, {
 *   columns: ['title', 'price'],
 *   limit: 10
 * })
 * ```
 */
export async function parquetQuery(
  buffer: ArrayBuffer,
  filter?: TinyFilter,
  options?: QueryOptions
): Promise<Row[]> {
  const asyncBuffer = createBufferAdapter(buffer)

  // Build read options
  const readOptions: { file: AsyncBuffer; columns?: string[]; rowGroups?: number[] } = {
    file: asyncBuffer,
  }

  if (options?.columns?.length) {
    readOptions.columns = options.columns
  }

  if (options?.rowGroups?.length) {
    readOptions.rowGroups = options.rowGroups
  }

  // Read data
  let rows = (await parquetReadObjects(readOptions)) as Row[]

  // Apply filter
  if (filter && Object.keys(filter).length > 0) {
    rows = rows.filter(row => matchesFilter(row, filter))
  }

  // Apply offset and limit
  if (options?.offset !== undefined || options?.limit !== undefined) {
    const start = options?.offset ?? 0
    const end = options?.limit !== undefined ? start + options.limit : undefined
    rows = rows.slice(start, end)
  }

  return rows
}

/**
 * Stream rows from a Parquet buffer with optional filtering
 *
 * @param buffer - ArrayBuffer containing Parquet file data
 * @param filter - Optional filter to apply
 * @param options - Query options
 * @yields Individual rows
 *
 * @example
 * ```typescript
 * import { parquetStream } from 'parquedb/tiny'
 *
 * const buffer = await fetch('/large-data.parquet').then(r => r.arrayBuffer())
 *
 * for await (const row of parquetStream(buffer, { active: true })) {
 *   console.log(row)
 * }
 * ```
 */
export async function* parquetStream(
  buffer: ArrayBuffer,
  filter?: TinyFilter,
  options?: QueryOptions
): AsyncGenerator<Row, void, unknown> {
  const asyncBuffer = createBufferAdapter(buffer)
  const metadata = await parquetMetadataAsync(asyncBuffer)

  const rowGroups = options?.rowGroups ?? Array.from(
    { length: metadata.row_groups?.length ?? 0 },
    (_, i) => i
  )

  let rowCount = 0
  const offset = options?.offset ?? 0
  const limit = options?.limit

  for (const groupIndex of rowGroups) {
    if (limit !== undefined && rowCount - offset >= limit) break

    const readOptions: { file: AsyncBuffer; columns?: string[]; rowGroups: number[] } = {
      file: asyncBuffer,
      rowGroups: [groupIndex],
    }

    if (options?.columns?.length) {
      readOptions.columns = options.columns
    }

    const rows = (await parquetReadObjects(readOptions)) as Row[]

    for (const row of rows) {
      // Skip until offset
      if (rowCount < offset) {
        rowCount++
        continue
      }

      // Check limit
      if (limit !== undefined && rowCount - offset >= limit) {
        return
      }

      // Apply filter
      if (filter && Object.keys(filter).length > 0) {
        if (!matchesFilter(row, filter)) {
          continue
        }
      }

      yield row
      rowCount++
    }
  }
}

/**
 * Get metadata from a Parquet buffer
 *
 * @param buffer - ArrayBuffer containing Parquet file data
 * @returns Parquet metadata
 *
 * @example
 * ```typescript
 * import { parquetMetadata } from 'parquedb/tiny'
 *
 * const buffer = await fetch('/data.parquet').then(r => r.arrayBuffer())
 * const meta = await parquetMetadata(buffer)
 *
 * console.log(`Rows: ${meta.numRows}`)
 * console.log(`Columns: ${meta.columns.join(', ')}`)
 * ```
 */
export async function parquetMetadata(buffer: ArrayBuffer): Promise<TinyParquetMetadata> {
  const asyncBuffer = createBufferAdapter(buffer)
  const metadata = await parquetMetadataAsync(asyncBuffer) as FileMetaData

  // Extract column names from schema
  const columns: string[] = []
  if (metadata.schema) {
    for (const element of metadata.schema) {
      if (element.name && element.name !== 'schema' && element.num_children === undefined) {
        columns.push(element.name)
      }
    }
  }

  return {
    numRows: Number(metadata.num_rows ?? 0),
    numRowGroups: metadata.row_groups?.length ?? 0,
    columns,
    version: metadata.version ?? 1,
  }
}

/**
 * Count rows matching a filter
 *
 * @param buffer - ArrayBuffer containing Parquet file data
 * @param filter - Optional filter to apply
 * @returns Number of matching rows
 *
 * @example
 * ```typescript
 * import { parquetCount } from 'parquedb/tiny'
 *
 * const buffer = await fetch('/data.parquet').then(r => r.arrayBuffer())
 * const total = await parquetCount(buffer)
 * const active = await parquetCount(buffer, { status: 'active' })
 * ```
 */
export async function parquetCount(buffer: ArrayBuffer, filter?: TinyFilter): Promise<number> {
  if (!filter || Object.keys(filter).length === 0) {
    const metadata = await parquetMetadata(buffer)
    return metadata.numRows
  }

  const rows = await parquetQuery(buffer, filter)
  return rows.length
}

// =============================================================================
// Version
// =============================================================================

export const VERSION = '0.1.0'
export const EXPORT_TYPE = 'tiny' as const
