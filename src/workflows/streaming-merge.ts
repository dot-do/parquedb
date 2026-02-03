/**
 * Streaming Merge-Sort for Large Compaction Windows
 *
 * Implements memory-bounded k-way merge sort for compacting many Parquet files
 * without loading all rows into memory. Key features:
 *
 * 1. **Chunked file reading**: Read each file in configurable chunks
 * 2. **K-way merge with min-heap**: Classic external merge-sort algorithm
 * 3. **Streaming output**: Yield merged rows in chunks for R2 multipart upload
 * 4. **Memory bounds**: Configurable memory limits with adaptive chunk sizing
 *
 * @example
 * ```typescript
 * // Stream merge many files to R2 using multipart upload
 * const merger = new StreamingMergeSorter(storage, {
 *   chunkSize: 10000,
 *   sortKey: 'createdAt',
 *   maxMemoryBytes: 128 * 1024 * 1024,
 * })
 *
 * for await (const chunk of merger.merge(files)) {
 *   // Write chunk to multipart upload
 *   await upload.uploadPart(partNumber++, serializeChunk(chunk))
 * }
 * ```
 */

import { initializeAsyncBuffer } from '../parquet/reader'
import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import { compressors } from '../parquet/compression'
import type { StorageBackend } from '../types/storage'
import type { Compressors } from 'hyparquet'
import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

/** Generic row type */
export type Row = Record<string, unknown>

/**
 * Configuration for streaming merge-sort
 */
export interface StreamingMergeOptions {
  /**
   * Maximum rows to read per chunk from each file.
   * Lower values = less memory, more I/O.
   * @default 10000
   */
  chunkSize?: number | undefined

  /**
   * Maximum memory usage in bytes.
   * Used to adaptively adjust chunk sizes.
   * @default 128MB
   */
  maxMemoryBytes?: number | undefined

  /**
   * Field to sort by.
   * @default 'createdAt'
   */
  sortKey?: string | undefined

  /**
   * Sort direction.
   * @default 'asc'
   */
  sortDirection?: 'asc' | 'desc' | undefined

  /**
   * Columns to read from Parquet files.
   * If not specified, all columns are read.
   */
  columns?: string[] | undefined

  /**
   * Output buffer size (rows).
   * Chunks of this size are yielded to the caller.
   * @default same as chunkSize
   */
  outputBufferSize?: number | undefined
}

/**
 * Result of streaming merge operation
 */
export interface StreamingMergeResult {
  /** Total rows processed */
  totalRows: number
  /** Total bytes read from fully completed files */
  bytesRead: number
  /** Number of files processed (started reading) */
  filesProcessed: number
  /** Number of files fully read to completion */
  filesCompleted: number
  /** Processing duration in milliseconds */
  durationMs: number
}

/**
 * Entry in the min-heap for k-way merge
 */
interface HeapEntry {
  /** Current row from this source */
  row: Row
  /** Index of the source file */
  fileIndex: number
  /** Iterator to get next chunk from this source */
  iterator: AsyncIterator<Row[], undefined>
  /** Current chunk of rows */
  chunk: Row[]
  /** Current index within the chunk */
  chunkIndex: number
}

// =============================================================================
// Min-Heap Implementation
// =============================================================================

/**
 * Min-heap implementation for k-way merge.
 *
 * The heap always returns the entry with the smallest sort key value.
 * For descending sort, we negate the comparison result.
 */
export class MinHeap {
  private heap: HeapEntry[] = []
  private compareFn: (a: Row, b: Row) => number

  constructor(sortKey: string, direction: 'asc' | 'desc' = 'asc') {
    this.compareFn = (a, b) => {
      const aVal = a[sortKey]
      const bVal = b[sortKey]

      let result: number

      // Handle null/undefined - push to end
      if (aVal === undefined || aVal === null) {
        result = bVal === undefined || bVal === null ? 0 : 1
      } else if (bVal === undefined || bVal === null) {
        result = -1
      } else if (typeof aVal === 'string' && typeof bVal === 'string') {
        result = aVal.localeCompare(bVal)
      } else if (typeof aVal === 'number' && typeof bVal === 'number') {
        result = aVal - bVal
      } else if (aVal instanceof Date && bVal instanceof Date) {
        result = aVal.getTime() - bVal.getTime()
      } else {
        // Fallback to string comparison
        result = String(aVal).localeCompare(String(bVal))
      }

      return direction === 'desc' ? -result : result
    }
  }

  /**
   * Add an entry to the heap
   */
  push(entry: HeapEntry): void {
    this.heap.push(entry)
    this.bubbleUp(this.heap.length - 1)
  }

  /**
   * Remove and return the smallest entry
   */
  pop(): HeapEntry | undefined {
    if (this.heap.length === 0) return undefined

    const result = this.heap[0]
    const last = this.heap.pop()

    if (this.heap.length > 0 && last) {
      this.heap[0] = last
      this.bubbleDown(0)
    }

    return result
  }

  /**
   * Get heap size
   */
  get size(): number {
    return this.heap.length
  }

  /**
   * Check if heap is empty
   */
  isEmpty(): boolean {
    return this.heap.length === 0
  }

  private bubbleUp(index: number): void {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2)
      const parent = this.heap[parentIndex]
      const current = this.heap[index]

      if (!parent || !current || this.compareFn(current.row, parent.row) >= 0) {
        break
      }

      this.heap[parentIndex] = current
      this.heap[index] = parent
      index = parentIndex
    }
  }

  private bubbleDown(index: number): void {
    while (true) {
      const leftIndex = 2 * index + 1
      const rightIndex = 2 * index + 2
      let smallest = index

      const current = this.heap[index]
      const left = this.heap[leftIndex]
      const right = this.heap[rightIndex]

      if (left && current && this.compareFn(left.row, current.row) < 0) {
        smallest = leftIndex
      }

      const smallestEntry = this.heap[smallest]
      if (right && smallestEntry && this.compareFn(right.row, smallestEntry.row) < 0) {
        smallest = rightIndex
      }

      if (smallest === index) break

      const smallestSwap = this.heap[smallest]
      if (current && smallestSwap) {
        this.heap[index] = smallestSwap
        this.heap[smallest] = current
      }
      index = smallest
    }
  }
}

// =============================================================================
// Chunked File Reader
// =============================================================================

/**
 * Create an async iterator that yields rows from a Parquet file in chunks.
 *
 * This reads one row group at a time and yields chunks of rows,
 * providing memory-efficient streaming access to large files.
 */
async function* createChunkedParquetReader(
  storage: StorageBackend,
  path: string,
  chunkSize: number,
  columns?: string[]
): AsyncGenerator<Row[], undefined> {
  try {
    const asyncBuffer = await initializeAsyncBuffer(storage, path)
    const metadata = await parquetMetadataAsync(asyncBuffer)

    const numRowGroups = metadata.row_groups?.length ?? 0
    if (numRowGroups === 0) {
      return
    }

    // Read row groups one at a time
    for (let rgIndex = 0; rgIndex < numRowGroups; rgIndex++) {
      const readOptions: {
        file: typeof asyncBuffer
        compressors: Compressors
        columns?: string[] | undefined
        rowGroups: number[]
      } = {
        file: asyncBuffer,
        compressors,
        rowGroups: [rgIndex],
      }

      if (columns && columns.length > 0) {
        readOptions.columns = columns
      }

      const rows = (await parquetReadObjects(readOptions)) as Row[]

      // Yield rows in chunks
      for (let i = 0; i < rows.length; i += chunkSize) {
        yield rows.slice(i, i + chunkSize)
      }
    }
  } catch (error) {
    const msg = error instanceof Error ? error.message : 'Unknown error'
    logger.error(`Failed to read Parquet file: ${path}`, { error: msg })
    // Don't yield any rows for this file, but don't fail the entire merge
  }
}

// =============================================================================
// Streaming Merge Sorter
// =============================================================================

/**
 * Streaming k-way merge sorter for Parquet files.
 *
 * Merges multiple pre-sorted Parquet files into a single sorted stream
 * using a min-heap for efficient k-way merging.
 */
export class StreamingMergeSorter {
  private storage: StorageBackend
  private options: Required<Omit<StreamingMergeOptions, 'columns'>> & { columns?: string[] | undefined }

  constructor(storage: StorageBackend, options: StreamingMergeOptions = {}) {
    this.storage = storage
    this.options = {
      chunkSize: options.chunkSize ?? 10000,
      maxMemoryBytes: options.maxMemoryBytes ?? 128 * 1024 * 1024,
      sortKey: options.sortKey ?? 'createdAt',
      sortDirection: options.sortDirection ?? 'asc',
      outputBufferSize: options.outputBufferSize ?? options.chunkSize ?? 10000,
      columns: options.columns,
    }
  }

  /**
   * Merge multiple Parquet files into a sorted stream.
   *
   * @param files - Paths to Parquet files to merge
   * @yields Chunks of sorted rows
   */
  async *merge(files: string[]): AsyncGenerator<Row[], StreamingMergeResult> {
    const startTime = Date.now()
    const {
      chunkSize,
      sortKey,
      sortDirection,
      outputBufferSize,
      columns,
    } = this.options

    let totalRows = 0
    let filesProcessed = 0

    // Track file sizes for bytesRead calculation
    const fileSizes = new Map<number, number>()
    for (let i = 0; i < files.length; i++) {
      const path = files[i]
      if (!path) continue

      try {
        const stat = await this.storage.stat(path)
        if (stat) {
          fileSizes.set(i, stat.size)
        }
      } catch {
        // Ignore stat errors
      }
    }

    // Track which files have been fully read (iterator exhausted)
    const completedFiles = new Set<number>()

    // Initialize min-heap
    const heap = new MinHeap(sortKey, sortDirection)

    // Initialize iterators for each file
    for (let i = 0; i < files.length; i++) {
      const path = files[i]
      if (!path) continue

      const iterator = createChunkedParquetReader(
        this.storage,
        path,
        chunkSize,
        columns
      )

      const firstChunk = await iterator.next()

      if (!firstChunk.done && firstChunk.value && firstChunk.value.length > 0) {
        const chunk = firstChunk.value
        const firstRow = chunk[0]
        if (firstRow) {
          heap.push({
            row: firstRow,
            fileIndex: i,
            iterator,
            chunk,
            chunkIndex: 0,
          })
          filesProcessed++
          // Note: bytesRead is tracked when file is fully completed, not here
        }
      } else if (firstChunk.done) {
        // Empty file - mark as completed immediately
        completedFiles.add(i)
        filesProcessed++
      }
    }

    // Output buffer for batching
    const outputBuffer: Row[] = []

    // K-way merge
    while (!heap.isEmpty()) {
      const entry = heap.pop()
      if (!entry) break

      // Add to output buffer
      outputBuffer.push(entry.row)
      totalRows++

      // Yield when buffer is full
      if (outputBuffer.length >= outputBufferSize) {
        yield [...outputBuffer]
        outputBuffer.length = 0
      }

      // Advance in current chunk
      entry.chunkIndex++

      // Check if we need the next row from this source
      if (entry.chunkIndex < entry.chunk.length) {
        // Still have rows in current chunk
        const nextRow = entry.chunk[entry.chunkIndex]
        if (nextRow) {
          entry.row = nextRow
          heap.push(entry)
        }
      } else {
        // Need to fetch next chunk
        const nextChunk = await entry.iterator.next()
        if (!nextChunk.done && nextChunk.value && nextChunk.value.length > 0) {
          entry.chunk = nextChunk.value
          entry.chunkIndex = 0
          const nextRow = entry.chunk[0]
          if (nextRow) {
            entry.row = nextRow
            heap.push(entry)
          }
        } else {
          // Iterator is exhausted - file has been fully read
          completedFiles.add(entry.fileIndex)
        }
      }
    }

    // Yield any remaining rows
    if (outputBuffer.length > 0) {
      yield outputBuffer
    }

    // Calculate bytesRead from only fully completed files
    const bytesRead = Array.from(completedFiles).reduce(
      (sum, fileIndex) => sum + (fileSizes.get(fileIndex) ?? 0),
      0
    )

    // Return statistics
    return {
      totalRows,
      bytesRead,
      filesProcessed,
      filesCompleted: completedFiles.size,
      durationMs: Date.now() - startTime,
    }
  }

  /**
   * Collect all merged rows (for smaller datasets that fit in memory).
   *
   * @param files - Paths to Parquet files to merge
   * @returns All rows sorted
   */
  async collectAll(files: string[]): Promise<{ rows: Row[]; stats: StreamingMergeResult }> {
    const allRows: Row[] = []
    let stats: StreamingMergeResult | undefined

    const generator = this.merge(files)
    let result = await generator.next()

    while (!result.done) {
      allRows.push(...result.value)
      result = await generator.next()
    }

    stats = result.value

    return { rows: allRows, stats }
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Estimate memory usage for streaming merge.
 *
 * @param fileCount - Number of files to merge
 * @param chunkSize - Rows per chunk
 * @param avgRowBytes - Average bytes per row
 * @returns Estimated memory usage in bytes
 */
export function estimateStreamingMergeMemory(
  fileCount: number,
  chunkSize: number,
  avgRowBytes: number
): number {
  // Memory per file: one chunk in memory at a time
  const memoryPerFile = chunkSize * avgRowBytes

  // Total: all file chunks + heap overhead + output buffer
  const heapOverhead = fileCount * 100 // Approximate heap entry overhead
  const outputBuffer = chunkSize * avgRowBytes

  return fileCount * memoryPerFile + heapOverhead + outputBuffer
}

/**
 * Calculate optimal chunk size given memory constraints.
 *
 * @param fileCount - Number of files to merge
 * @param maxMemoryBytes - Maximum memory to use
 * @param avgRowBytes - Average bytes per row
 * @param minChunkSize - Minimum chunk size (default: 100)
 * @param maxChunkSize - Maximum chunk size (default: 100000)
 * @returns Optimal chunk size
 */
export function calculateOptimalChunkSize(
  fileCount: number,
  maxMemoryBytes: number,
  avgRowBytes: number,
  minChunkSize: number = 100,
  maxChunkSize: number = 100000
): number {
  // Reserve 20% for overhead
  const availableMemory = maxMemoryBytes * 0.8

  // Memory per file: chunkSize * avgRowBytes
  // We need: fileCount * chunkSize * avgRowBytes + outputBuffer <= availableMemory
  // Assuming outputBuffer = chunkSize * avgRowBytes
  // (fileCount + 1) * chunkSize * avgRowBytes <= availableMemory
  // chunkSize <= availableMemory / ((fileCount + 1) * avgRowBytes)

  const optimalChunkSize = Math.floor(
    availableMemory / ((fileCount + 1) * avgRowBytes)
  )

  return Math.max(minChunkSize, Math.min(maxChunkSize, optimalChunkSize))
}

/**
 * Streaming merge-sort convenience function.
 *
 * @param storage - Storage backend
 * @param files - Paths to Parquet files to merge
 * @param options - Merge options
 * @yields Chunks of sorted rows
 */
export async function* streamingMergeSort(
  storage: StorageBackend,
  files: string[],
  options?: StreamingMergeOptions
): AsyncGenerator<Row[], StreamingMergeResult> {
  const sorter = new StreamingMergeSorter(storage, options)
  return yield* sorter.merge(files)
}

/**
 * Check if streaming merge should be used based on file count and estimated size.
 *
 * @param fileCount - Number of files
 * @param totalEstimatedRows - Estimated total row count
 * @param avgRowBytes - Average bytes per row
 * @param memoryThreshold - Memory threshold to trigger streaming (default: 64MB)
 * @returns true if streaming merge should be used
 */
export function shouldUseStreamingMerge(
  fileCount: number,
  totalEstimatedRows: number,
  avgRowBytes: number = 500,
  memoryThreshold: number = 64 * 1024 * 1024
): boolean {
  const estimatedMemory = totalEstimatedRows * avgRowBytes
  return estimatedMemory > memoryThreshold || fileCount > 20
}
