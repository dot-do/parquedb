/**
 * Snippet-optimized query implementation
 *
 * Designed to run within Cloudflare Snippet constraints:
 * - <5ms CPU time
 * - <32MB memory
 * - ≤5 fetch subrequests
 */

import { parquetMetadataAsync, parquetRead } from 'hyparquet'
import { compressors } from '../../src/parquet/compression'
import type { PartitionManifest } from './partition'

/** Async file interface for range requests */
export interface AsyncFile {
  byteLength: number
  slice(start: number, end?: number): Promise<ArrayBuffer>
}

/** Query result with metrics */
export interface QueryResult<T> {
  data: T | null
  metrics: {
    cpuTimeMs: number
    fetchCount: number
    bytesRead: number
    rowGroupsScanned: number
    rowGroupsSkipped: number
  }
}

/**
 * Create an AsyncFile from a fetch function
 *
 * @example
 * ```typescript
 * const file = createAsyncFile(
 *   'https://example.com/data.parquet',
 *   async (url, start, end) => {
 *     const response = await fetch(url, {
 *       headers: { Range: `bytes=${start}-${end - 1}` }
 *     })
 *     return response.arrayBuffer()
 *   }
 * )
 * ```
 */
export function createAsyncFile(
  url: string,
  fileSize: number,
  fetchRange: (url: string, start: number, end: number) => Promise<ArrayBuffer>
): AsyncFile {
  let fetchCount = 0
  let bytesRead = 0

  return {
    byteLength: fileSize,
    async slice(start: number, end?: number): Promise<ArrayBuffer> {
      fetchCount++
      const actualEnd = end ?? fileSize
      const buffer = await fetchRange(url, start, actualEnd)
      bytesRead += buffer.byteLength
      return buffer
    },
    // Expose metrics
    get _metrics() {
      return { fetchCount, bytesRead }
    },
  } as AsyncFile & { _metrics: { fetchCount: number; bytesRead: number } }
}

/**
 * Point lookup by ID using partition manifest and row group statistics
 *
 * @example
 * ```typescript
 * const result = await getById('user-123', manifest, async (file) => {
 *   const url = `https://cdn.example.com/data/by-id/${file}`
 *   const response = await fetch(url)
 *   const size = parseInt(response.headers.get('content-length') || '0')
 *   return createAsyncFile(url, size, fetchRange)
 * })
 * ```
 */
export async function getById<T = Record<string, unknown>>(
  id: string,
  manifest: PartitionManifest,
  getFile: (fileName: string) => Promise<AsyncFile>
): Promise<QueryResult<T>> {
  const startTime = performance.now()
  let fetchCount = 0
  let bytesRead = 0
  let rowGroupsScanned = 0
  let rowGroupsSkipped = 0

  // 1. Find partition containing this ID
  const partition = manifest.partitions.find(
    p => id >= p.minKey && id <= p.maxKey
  )

  if (!partition) {
    return {
      data: null,
      metrics: {
        cpuTimeMs: performance.now() - startTime,
        fetchCount: 0,
        bytesRead: 0,
        rowGroupsScanned: 0,
        rowGroupsSkipped: 0,
      },
    }
  }

  // 2. Get file handle
  const file = await getFile(partition.file)

  // 3. Read metadata
  const metadata = await parquetMetadataAsync(file)
  fetchCount++ // metadata fetch

  // 4. Find row group containing this ID using statistics
  let targetRowGroup = -1
  let rowStart = 0

  for (let i = 0; i < metadata.row_groups.length; i++) {
    const rg = metadata.row_groups[i]
    const idCol = rg.columns.find(c =>
      c.meta_data?.path_in_schema?.includes('$id')
    )

    if (idCol?.meta_data?.statistics) {
      const stats = idCol.meta_data.statistics
      const minId = decodeStatValue(stats.min_value)
      const maxId = decodeStatValue(stats.max_value)

      if (id >= minId && id <= maxId) {
        targetRowGroup = i
        break
      } else {
        rowGroupsSkipped++
      }
    } else {
      // No statistics, have to check this row group
      targetRowGroup = i
      break
    }

    rowStart += Number(rg.num_rows)
  }

  if (targetRowGroup === -1) {
    return {
      data: null,
      metrics: {
        cpuTimeMs: performance.now() - startTime,
        fetchCount,
        bytesRead,
        rowGroupsScanned,
        rowGroupsSkipped,
      },
    }
  }

  // 5. Read only the target row group
  const rgRows = Number(metadata.row_groups[targetRowGroup].num_rows)
  let result: T | null = null

  await parquetRead({
    file,
    compressors,
    rowStart,
    rowEnd: rowStart + rgRows,
    onComplete: (columns: unknown[][]) => {
      rowGroupsScanned++
      fetchCount++ // data fetch

      // Find the row with matching ID
      const schema = metadata.schema.slice(1) // Skip root
      const idColIndex = schema.findIndex(s => s.name === '$id')

      if (idColIndex === -1) return

      const idColumn = columns[idColIndex] as string[]
      const rowIndex = idColumn.findIndex(v => v === id)

      if (rowIndex === -1) return

      // Build result object
      const obj: Record<string, unknown> = {}
      for (let c = 0; c < schema.length; c++) {
        const colName = schema[c].name
        let value = (columns[c] as unknown[])[rowIndex]

        // Parse JSON fields back to objects
        if (typeof value === 'string' && (value.startsWith('{') || value.startsWith('['))) {
          try {
            value = JSON.parse(value)
          } catch {
            // Keep as string
          }
        }

        obj[colName] = value
      }

      result = obj as T
    },
  })

  // Get file metrics if available
  const fileMetrics = (file as AsyncFile & { _metrics?: { fetchCount: number; bytesRead: number } })._metrics
  if (fileMetrics) {
    fetchCount = fileMetrics.fetchCount
    bytesRead = fileMetrics.bytesRead
  }

  return {
    data: result,
    metrics: {
      cpuTimeMs: performance.now() - startTime,
      fetchCount,
      bytesRead,
      rowGroupsScanned,
      rowGroupsSkipped,
    },
  }
}

/**
 * Range query with predicate pushdown
 */
export async function findByRange<T = Record<string, unknown>>(
  field: string,
  minValue: string | number,
  maxValue: string | number,
  manifest: PartitionManifest,
  getFile: (fileName: string) => Promise<AsyncFile>,
  options: { limit?: number } = {}
): Promise<QueryResult<T[]>> {
  const startTime = performance.now()
  const results: T[] = []
  let fetchCount = 0
  let bytesRead = 0
  let rowGroupsScanned = 0
  let rowGroupsSkipped = 0

  const limit = options.limit ?? 100

  // Find partitions that might contain matching rows
  const matchingPartitions = manifest.partitions.filter(p => {
    // If manifest is sorted by this field, we can filter partitions
    if (manifest.sortKey === field) {
      return !(String(maxValue) < p.minKey || String(minValue) > p.maxKey)
    }
    // Otherwise, check all partitions
    return true
  })

  for (const partition of matchingPartitions) {
    if (results.length >= limit) break

    const file = await getFile(partition.file)
    const metadata = await parquetMetadataAsync(file)
    fetchCount++

    // Find row groups that might match
    let rowStart = 0
    for (let i = 0; i < metadata.row_groups.length; i++) {
      if (results.length >= limit) break

      const rg = metadata.row_groups[i]
      const col = rg.columns.find(c =>
        c.meta_data?.path_in_schema?.includes(field)
      )

      // Check statistics to potentially skip
      let shouldRead = true
      if (col?.meta_data?.statistics) {
        const stats = col.meta_data.statistics
        const rgMin = decodeStatValue(stats.min_value)
        const rgMax = decodeStatValue(stats.max_value)

        if (String(maxValue) < rgMin || String(minValue) > rgMax) {
          shouldRead = false
          rowGroupsSkipped++
        }
      }

      if (shouldRead) {
        const rgRows = Number(rg.num_rows)

        await parquetRead({
          file,
          compressors,
          rowStart,
          rowEnd: rowStart + rgRows,
          onComplete: (columns: unknown[][]) => {
            rowGroupsScanned++
            fetchCount++

            const schema = metadata.schema.slice(1)
            const fieldIndex = schema.findIndex(s => s.name === field)
            if (fieldIndex === -1) return

            const fieldColumn = columns[fieldIndex] as (string | number)[]

            for (let r = 0; r < fieldColumn.length && results.length < limit; r++) {
              const value = fieldColumn[r]
              if (value >= minValue && value <= maxValue) {
                // Build result object
                const obj: Record<string, unknown> = {}
                for (let c = 0; c < schema.length; c++) {
                  let val = (columns[c] as unknown[])[r]
                  if (typeof val === 'string' && (val.startsWith('{') || val.startsWith('['))) {
                    try { val = JSON.parse(val) } catch { /* keep as string */ }
                  }
                  obj[schema[c].name] = val
                }
                results.push(obj as T)
              }
            }
          },
        })
      }

      rowStart += Number(rg.num_rows)
    }
  }

  return {
    data: results,
    metrics: {
      cpuTimeMs: performance.now() - startTime,
      fetchCount,
      bytesRead,
      rowGroupsScanned,
      rowGroupsSkipped,
    },
  }
}

/**
 * Decode Parquet statistics value
 */
function decodeStatValue(value: unknown): string {
  if (value instanceof Uint8Array || ArrayBuffer.isView(value)) {
    return new TextDecoder().decode(value as Uint8Array)
  }
  if (typeof value === 'bigint') {
    return String(value)
  }
  return String(value ?? '')
}

/**
 * Create a query client for a specific data path
 */
export function createQueryClient(
  basePath: string,
  fetchFn: typeof fetch = fetch
) {
  let manifest: PartitionManifest | null = null

  async function loadManifest(): Promise<PartitionManifest> {
    if (manifest) return manifest

    const response = await fetchFn(`${basePath}/manifest.json`)
    manifest = await response.json()
    return manifest!
  }

  async function getFile(fileName: string): Promise<AsyncFile> {
    const url = `${basePath}/${fileName}`

    // HEAD request to get file size
    const headResponse = await fetchFn(url, { method: 'HEAD' })
    const size = parseInt(headResponse.headers.get('content-length') || '0')

    return createAsyncFile(url, size, async (u, start, end) => {
      const response = await fetchFn(u, {
        headers: { Range: `bytes=${start}-${end - 1}` },
      })
      return response.arrayBuffer()
    })
  }

  return {
    async get<T>(id: string): Promise<QueryResult<T>> {
      const m = await loadManifest()
      return getById<T>(id, m, getFile)
    },

    async find<T>(
      field: string,
      minValue: string | number,
      maxValue: string | number,
      options?: { limit?: number }
    ): Promise<QueryResult<T[]>> {
      const m = await loadManifest()
      return findByRange<T>(field, minValue, maxValue, m, getFile, options)
    },
  }
}
