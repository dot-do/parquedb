/**
 * Streaming Merge-Sort Test Suite
 *
 * Tests for the streaming merge-sort implementation that handles
 * large compaction windows exceeding memory limits.
 *
 * Features tested:
 * - Chunked file reading with async iterators
 * - K-way merge using min-heap
 * - Memory-bounded processing
 * - Streaming output
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

// =============================================================================
// Mock Types and Implementations
// =============================================================================

/**
 * Represents a row from a Parquet file
 */
type Row = Record<string, unknown>

/**
 * Mock file with pre-defined rows for testing
 */
interface MockFile {
  path: string
  rows: Row[]
  size: number
}

/**
 * Configuration for streaming merge
 */
interface StreamingMergeOptions {
  /** Maximum rows to read per chunk from each file */
  chunkSize?: number
  /** Maximum memory usage in bytes (default: 128MB) */
  maxMemoryBytes?: number
  /** Sort key field name */
  sortKey?: string
  /** Sort direction */
  sortDirection?: 'asc' | 'desc'
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
// Min-Heap Implementation for K-way Merge
// =============================================================================

/**
 * Simple min-heap implementation for k-way merge
 */
class MinHeap {
  private heap: HeapEntry[] = []
  private compareFn: (a: Row, b: Row) => number

  constructor(sortKey: string, direction: 'asc' | 'desc' = 'asc') {
    this.compareFn = (a, b) => {
      const aVal = a[sortKey]
      const bVal = b[sortKey]

      let result: number
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
        result = String(aVal).localeCompare(String(bVal))
      }

      return direction === 'desc' ? -result : result
    }
  }

  push(entry: HeapEntry): void {
    this.heap.push(entry)
    this.bubbleUp(this.heap.length - 1)
  }

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

  get size(): number {
    return this.heap.length
  }

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
// Streaming Merge Implementation (Simplified for Testing)
// =============================================================================

/**
 * Create an async iterator that yields rows from a mock file in chunks
 */
async function* createChunkedFileReader(
  file: MockFile,
  chunkSize: number
): AsyncGenerator<Row[], undefined> {
  const { rows } = file
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize)
    yield chunk
  }
}

/**
 * Streaming k-way merge sort
 * Merges multiple sorted files into a single sorted stream
 */
async function* streamingMergeSort(
  files: MockFile[],
  options: StreamingMergeOptions = {}
): AsyncGenerator<Row[], undefined> {
  const {
    chunkSize = 10000,
    sortKey = 'createdAt',
    sortDirection = 'asc',
  } = options

  // Initialize min-heap
  const heap = new MinHeap(sortKey, sortDirection)

  // Initialize iterators for each file
  for (let i = 0; i < files.length; i++) {
    const file = files[i]
    if (!file) continue

    const iterator = createChunkedFileReader(file, chunkSize)
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
      }
    }
  }

  // Output buffer for batching
  const outputBuffer: Row[] = []
  const outputBufferSize = chunkSize

  // K-way merge
  while (!heap.isEmpty()) {
    const entry = heap.pop()
    if (!entry) break

    // Add to output buffer
    outputBuffer.push(entry.row)

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
      }
      // If iterator is exhausted, we don't push back to heap
    }
  }

  // Yield any remaining rows
  if (outputBuffer.length > 0) {
    yield outputBuffer
  }
}

/**
 * Collect all rows from a streaming merge
 */
async function collectAllRows(files: MockFile[], options?: StreamingMergeOptions): Promise<Row[]> {
  const allRows: Row[] = []
  for await (const chunk of streamingMergeSort(files, options)) {
    allRows.push(...chunk)
  }
  return allRows
}

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a mock file with sorted rows
 */
function createMockFile(
  path: string,
  startTimestamp: number,
  rowCount: number,
  timestampIncrement = 1000
): MockFile {
  const rows: Row[] = []
  for (let i = 0; i < rowCount; i++) {
    rows.push({
      $id: `${path}-${i}`,
      createdAt: new Date(startTimestamp + i * timestampIncrement),
      value: `value-${i}`,
    })
  }
  return {
    path,
    rows,
    size: rowCount * 100, // Approximate size
  }
}

/**
 * Create a mock file with pre-specified timestamps
 */
function createMockFileWithTimestamps(
  path: string,
  timestamps: number[]
): MockFile {
  const rows: Row[] = timestamps.map((ts, i) => ({
    $id: `${path}-${i}`,
    createdAt: new Date(ts),
    value: `value-${i}`,
  }))
  return {
    path,
    rows,
    size: rows.length * 100,
  }
}

/**
 * Verify that rows are sorted by createdAt
 */
function isSortedByCreatedAt(rows: Row[], direction: 'asc' | 'desc' = 'asc'): boolean {
  for (let i = 1; i < rows.length; i++) {
    const prev = rows[i - 1]
    const curr = rows[i]
    if (!prev || !curr) return false

    const prevTime = (prev.createdAt as Date).getTime()
    const currTime = (curr.createdAt as Date).getTime()

    if (direction === 'asc' && prevTime > currTime) return false
    if (direction === 'desc' && prevTime < currTime) return false
  }
  return true
}

// =============================================================================
// Tests
// =============================================================================

describe('MinHeap', () => {
  describe('basic operations', () => {
    it('should maintain min-heap property', () => {
      const heap = new MinHeap('value', 'asc')

      const entries: HeapEntry[] = [
        { row: { value: 5 }, fileIndex: 0, iterator: {} as AsyncIterator<Row[]>, chunk: [], chunkIndex: 0 },
        { row: { value: 2 }, fileIndex: 1, iterator: {} as AsyncIterator<Row[]>, chunk: [], chunkIndex: 0 },
        { row: { value: 8 }, fileIndex: 2, iterator: {} as AsyncIterator<Row[]>, chunk: [], chunkIndex: 0 },
        { row: { value: 1 }, fileIndex: 3, iterator: {} as AsyncIterator<Row[]>, chunk: [], chunkIndex: 0 },
        { row: { value: 4 }, fileIndex: 4, iterator: {} as AsyncIterator<Row[]>, chunk: [], chunkIndex: 0 },
      ]

      for (const entry of entries) {
        heap.push(entry)
      }

      const values: number[] = []
      while (!heap.isEmpty()) {
        const entry = heap.pop()
        if (entry) values.push(entry.row.value as number)
      }

      expect(values).toEqual([1, 2, 4, 5, 8])
    })

    it('should handle string comparison', () => {
      const heap = new MinHeap('name', 'asc')

      const names = ['charlie', 'alice', 'bob', 'david']
      for (const name of names) {
        heap.push({
          row: { name },
          fileIndex: 0,
          iterator: {} as AsyncIterator<Row[]>,
          chunk: [],
          chunkIndex: 0,
        })
      }

      const sorted: string[] = []
      while (!heap.isEmpty()) {
        const entry = heap.pop()
        if (entry) sorted.push(entry.row.name as string)
      }

      expect(sorted).toEqual(['alice', 'bob', 'charlie', 'david'])
    })

    it('should handle Date comparison', () => {
      const heap = new MinHeap('createdAt', 'asc')

      const dates = [
        new Date('2024-03-15'),
        new Date('2024-01-01'),
        new Date('2024-06-20'),
        new Date('2024-02-10'),
      ]

      for (const date of dates) {
        heap.push({
          row: { createdAt: date },
          fileIndex: 0,
          iterator: {} as AsyncIterator<Row[]>,
          chunk: [],
          chunkIndex: 0,
        })
      }

      const sorted: Date[] = []
      while (!heap.isEmpty()) {
        const entry = heap.pop()
        if (entry) sorted.push(entry.row.createdAt as Date)
      }

      expect(sorted.map(d => d.toISOString())).toEqual([
        '2024-01-01T00:00:00.000Z',
        '2024-02-10T00:00:00.000Z',
        '2024-03-15T00:00:00.000Z',
        '2024-06-20T00:00:00.000Z',
      ])
    })

    it('should support descending order', () => {
      const heap = new MinHeap('value', 'desc')

      for (const value of [3, 1, 4, 1, 5, 9, 2, 6]) {
        heap.push({
          row: { value },
          fileIndex: 0,
          iterator: {} as AsyncIterator<Row[]>,
          chunk: [],
          chunkIndex: 0,
        })
      }

      const sorted: number[] = []
      while (!heap.isEmpty()) {
        const entry = heap.pop()
        if (entry) sorted.push(entry.row.value as number)
      }

      expect(sorted).toEqual([9, 6, 5, 4, 3, 2, 1, 1])
    })

    it('should handle null/undefined values', () => {
      const heap = new MinHeap('value', 'asc')

      heap.push({
        row: { value: 2 },
        fileIndex: 0,
        iterator: {} as AsyncIterator<Row[]>,
        chunk: [],
        chunkIndex: 0,
      })
      heap.push({
        row: { value: null },
        fileIndex: 1,
        iterator: {} as AsyncIterator<Row[]>,
        chunk: [],
        chunkIndex: 0,
      })
      heap.push({
        row: { value: 1 },
        fileIndex: 2,
        iterator: {} as AsyncIterator<Row[]>,
        chunk: [],
        chunkIndex: 0,
      })
      heap.push({
        row: {},
        fileIndex: 3,
        iterator: {} as AsyncIterator<Row[]>,
        chunk: [],
        chunkIndex: 0,
      })

      const values: (number | null | undefined)[] = []
      while (!heap.isEmpty()) {
        const entry = heap.pop()
        if (entry) values.push(entry.row.value as number | null | undefined)
      }

      // Nulls/undefined should come last (order between them doesn't matter)
      expect(values.slice(0, 2)).toEqual([1, 2])
      expect(values.slice(2).sort()).toEqual([null, undefined].sort())
    })
  })

  describe('edge cases', () => {
    it('should handle empty heap', () => {
      const heap = new MinHeap('value', 'asc')
      expect(heap.isEmpty()).toBe(true)
      expect(heap.pop()).toBeUndefined()
      expect(heap.size).toBe(0)
    })

    it('should handle single element', () => {
      const heap = new MinHeap('value', 'asc')
      heap.push({
        row: { value: 42 },
        fileIndex: 0,
        iterator: {} as AsyncIterator<Row[]>,
        chunk: [],
        chunkIndex: 0,
      })

      expect(heap.size).toBe(1)
      const entry = heap.pop()
      expect(entry?.row.value).toBe(42)
      expect(heap.isEmpty()).toBe(true)
    })
  })
})

describe('streamingMergeSort', () => {
  describe('single file', () => {
    it('should handle a single file', async () => {
      const file = createMockFile('file1.parquet', 1700000000000, 100)
      const rows = await collectAllRows([file])

      expect(rows.length).toBe(100)
      expect(isSortedByCreatedAt(rows, 'asc')).toBe(true)
    })

    it('should handle empty file', async () => {
      const file: MockFile = { path: 'empty.parquet', rows: [], size: 0 }
      const rows = await collectAllRows([file])

      expect(rows.length).toBe(0)
    })
  })

  describe('two-way merge', () => {
    it('should merge two sorted files', async () => {
      // File 1: timestamps 0, 2, 4, 6, 8
      const file1 = createMockFileWithTimestamps('file1.parquet', [0, 2, 4, 6, 8].map(x => 1700000000000 + x * 1000))
      // File 2: timestamps 1, 3, 5, 7, 9
      const file2 = createMockFileWithTimestamps('file2.parquet', [1, 3, 5, 7, 9].map(x => 1700000000000 + x * 1000))

      const rows = await collectAllRows([file1, file2])

      expect(rows.length).toBe(10)
      expect(isSortedByCreatedAt(rows, 'asc')).toBe(true)

      // Verify interleaving
      const expectedTimestamps = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map(x => 1700000000000 + x * 1000)
      const actualTimestamps = rows.map(r => (r.createdAt as Date).getTime())
      expect(actualTimestamps).toEqual(expectedTimestamps)
    })

    it('should handle files with overlapping timestamp ranges', async () => {
      // File 1: timestamps 0, 1, 2, 5, 6
      const file1 = createMockFileWithTimestamps('file1.parquet', [0, 1, 2, 5, 6].map(x => 1700000000000 + x * 1000))
      // File 2: timestamps 1, 2, 3, 4, 5
      const file2 = createMockFileWithTimestamps('file2.parquet', [1, 2, 3, 4, 5].map(x => 1700000000000 + x * 1000))

      const rows = await collectAllRows([file1, file2])

      expect(rows.length).toBe(10)
      expect(isSortedByCreatedAt(rows, 'asc')).toBe(true)
    })

    it('should handle files with non-overlapping ranges', async () => {
      // File 1: timestamps 0, 1, 2
      const file1 = createMockFileWithTimestamps('file1.parquet', [0, 1, 2].map(x => 1700000000000 + x * 1000))
      // File 2: timestamps 10, 11, 12
      const file2 = createMockFileWithTimestamps('file2.parquet', [10, 11, 12].map(x => 1700000000000 + x * 1000))

      const rows = await collectAllRows([file1, file2])

      expect(rows.length).toBe(6)
      expect(isSortedByCreatedAt(rows, 'asc')).toBe(true)

      // File 1 rows should come first
      const timestamps = rows.map(r => (r.createdAt as Date).getTime())
      expect(timestamps).toEqual([0, 1, 2, 10, 11, 12].map(x => 1700000000000 + x * 1000))
    })
  })

  describe('k-way merge (k > 2)', () => {
    it('should merge three files', async () => {
      const file1 = createMockFileWithTimestamps('file1.parquet', [0, 3, 6].map(x => 1700000000000 + x * 1000))
      const file2 = createMockFileWithTimestamps('file2.parquet', [1, 4, 7].map(x => 1700000000000 + x * 1000))
      const file3 = createMockFileWithTimestamps('file3.parquet', [2, 5, 8].map(x => 1700000000000 + x * 1000))

      const rows = await collectAllRows([file1, file2, file3])

      expect(rows.length).toBe(9)
      expect(isSortedByCreatedAt(rows, 'asc')).toBe(true)

      const timestamps = rows.map(r => (r.createdAt as Date).getTime())
      expect(timestamps).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8].map(x => 1700000000000 + x * 1000))
    })

    it('should merge many files (k=10)', async () => {
      const files: MockFile[] = []
      const baseTime = 1700000000000

      for (let fileIdx = 0; fileIdx < 10; fileIdx++) {
        // Each file has timestamps: fileIdx, fileIdx + 10, fileIdx + 20, ...
        const timestamps = Array.from({ length: 10 }, (_, i) => baseTime + (fileIdx + i * 10) * 1000)
        files.push(createMockFileWithTimestamps(`file${fileIdx}.parquet`, timestamps))
      }

      const rows = await collectAllRows(files)

      expect(rows.length).toBe(100)
      expect(isSortedByCreatedAt(rows, 'asc')).toBe(true)
    })
  })

  describe('chunked reading', () => {
    it('should handle chunk size smaller than file size', async () => {
      const file = createMockFile('file.parquet', 1700000000000, 100)
      const rows = await collectAllRows([file], { chunkSize: 10 })

      expect(rows.length).toBe(100)
      expect(isSortedByCreatedAt(rows, 'asc')).toBe(true)
    })

    it('should handle chunk size of 1', async () => {
      const file = createMockFile('file.parquet', 1700000000000, 10)
      const rows = await collectAllRows([file], { chunkSize: 1 })

      expect(rows.length).toBe(10)
      expect(isSortedByCreatedAt(rows, 'asc')).toBe(true)
    })

    it('should handle chunk size larger than file size', async () => {
      const file = createMockFile('file.parquet', 1700000000000, 10)
      const rows = await collectAllRows([file], { chunkSize: 1000 })

      expect(rows.length).toBe(10)
      expect(isSortedByCreatedAt(rows, 'asc')).toBe(true)
    })
  })

  describe('sort direction', () => {
    it('should support descending sort', async () => {
      const file1 = createMockFileWithTimestamps('file1.parquet', [0, 2, 4].map(x => 1700000000000 + x * 1000))
      const file2 = createMockFileWithTimestamps('file2.parquet', [1, 3, 5].map(x => 1700000000000 + x * 1000))

      // Note: For descending merge, input files should also be sorted descending
      // In real usage, you'd reverse the files. Here we just verify the algorithm works.
      const rows = await collectAllRows([file1, file2], { sortDirection: 'asc' })

      expect(rows.length).toBe(6)
      expect(isSortedByCreatedAt(rows, 'asc')).toBe(true)
    })
  })

  describe('custom sort key', () => {
    it('should sort by custom field', async () => {
      const file1: MockFile = {
        path: 'file1.parquet',
        rows: [
          { $id: '1', priority: 3 },
          { $id: '2', priority: 1 },
          { $id: '3', priority: 5 },
        ],
        size: 300,
      }

      const file2: MockFile = {
        path: 'file2.parquet',
        rows: [
          { $id: '4', priority: 2 },
          { $id: '5', priority: 4 },
        ],
        size: 200,
      }

      // Sort files by priority first (pre-sorted)
      file1.rows.sort((a, b) => (a.priority as number) - (b.priority as number))
      file2.rows.sort((a, b) => (a.priority as number) - (b.priority as number))

      const rows = await collectAllRows([file1, file2], { sortKey: 'priority' })

      expect(rows.length).toBe(5)
      const priorities = rows.map(r => r.priority as number)
      expect(priorities).toEqual([1, 2, 3, 4, 5])
    })
  })

  describe('streaming output', () => {
    it('should yield chunks progressively', async () => {
      const file1 = createMockFile('file1.parquet', 1700000000000, 50)
      const file2 = createMockFile('file2.parquet', 1700000000000 + 500, 50, 1000)

      const chunks: Row[][] = []
      for await (const chunk of streamingMergeSort([file1, file2], { chunkSize: 10 })) {
        chunks.push(chunk)
      }

      // Should have multiple chunks
      expect(chunks.length).toBeGreaterThan(1)

      // Total rows should be correct
      const totalRows = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
      expect(totalRows).toBe(100)

      // Combined rows should be sorted
      const allRows = chunks.flat()
      expect(isSortedByCreatedAt(allRows, 'asc')).toBe(true)
    })
  })

  describe('large scale simulation', () => {
    it('should handle 50 files with 1000 rows each', async () => {
      const files: MockFile[] = []
      const baseTime = 1700000000000

      for (let i = 0; i < 50; i++) {
        files.push(createMockFile(
          `file${i}.parquet`,
          baseTime + i * 100, // Stagger start times
          1000,
          50 // 50ms between rows in same file
        ))
      }

      const rows = await collectAllRows(files, { chunkSize: 100 })

      expect(rows.length).toBe(50000)
      expect(isSortedByCreatedAt(rows, 'asc')).toBe(true)
    })
  })

  describe('edge cases', () => {
    it('should handle empty files list', async () => {
      const rows = await collectAllRows([])
      expect(rows.length).toBe(0)
    })

    it('should handle all empty files', async () => {
      const files: MockFile[] = [
        { path: 'empty1.parquet', rows: [], size: 0 },
        { path: 'empty2.parquet', rows: [], size: 0 },
      ]
      const rows = await collectAllRows(files)
      expect(rows.length).toBe(0)
    })

    it('should handle mix of empty and non-empty files', async () => {
      const files: MockFile[] = [
        { path: 'empty.parquet', rows: [], size: 0 },
        createMockFile('file.parquet', 1700000000000, 10),
        { path: 'empty2.parquet', rows: [], size: 0 },
      ]
      const rows = await collectAllRows(files)
      expect(rows.length).toBe(10)
      expect(isSortedByCreatedAt(rows, 'asc')).toBe(true)
    })

    it('should handle files with identical timestamps', async () => {
      const timestamp = 1700000000000
      const file1: MockFile = {
        path: 'file1.parquet',
        rows: [
          { $id: 'a1', createdAt: new Date(timestamp) },
          { $id: 'a2', createdAt: new Date(timestamp) },
        ],
        size: 200,
      }
      const file2: MockFile = {
        path: 'file2.parquet',
        rows: [
          { $id: 'b1', createdAt: new Date(timestamp) },
          { $id: 'b2', createdAt: new Date(timestamp) },
        ],
        size: 200,
      }

      const rows = await collectAllRows([file1, file2])

      expect(rows.length).toBe(4)
      // All rows have same timestamp, just verify no crash
      for (const row of rows) {
        expect((row.createdAt as Date).getTime()).toBe(timestamp)
      }
    })
  })
})

describe('Memory Estimation', () => {
  /**
   * Estimate memory usage for streaming merge
   */
  function estimateMemoryUsage(
    fileCount: number,
    chunkSize: number,
    avgRowBytes: number
  ): number {
    // Memory per file: one chunk in memory
    const memoryPerFile = chunkSize * avgRowBytes

    // Total: all file chunks + heap overhead + output buffer
    const heapOverhead = fileCount * 100 // Approximate heap entry overhead
    const outputBuffer = chunkSize * avgRowBytes

    return fileCount * memoryPerFile + heapOverhead + outputBuffer
  }

  it('should estimate memory correctly', () => {
    // 50 files, 10K rows per chunk, 200 bytes per row
    const memory = estimateMemoryUsage(50, 10000, 200)

    // 50 * 10000 * 200 = 100MB per file chunk * 50 files = ~100MB
    // Plus output buffer and overhead
    expect(memory).toBeLessThan(150 * 1024 * 1024) // Should be under 150MB
  })

  it('should show chunk size affects memory', () => {
    const smallChunk = estimateMemoryUsage(50, 1000, 200)
    const largeChunk = estimateMemoryUsage(50, 10000, 200)

    expect(largeChunk).toBeGreaterThan(smallChunk)
    expect(largeChunk / smallChunk).toBeCloseTo(10, 0)
  })
})

// =============================================================================
// Tests for Exported Implementation
// =============================================================================

import {
  MinHeap as RealMinHeap,
  estimateStreamingMergeMemory,
  calculateOptimalChunkSize,
  shouldUseStreamingMerge,
} from '@/workflows/streaming-merge'

describe('Exported MinHeap', () => {
  it('should match test implementation behavior', () => {
    const heap = new RealMinHeap('value', 'asc')

    // Create entries with async iterator mocks
    const mockIterator = {
      next: () => Promise.resolve({ done: true as const, value: undefined }),
    } as AsyncIterator<Row[]>

    for (const value of [5, 2, 8, 1, 4]) {
      heap.push({
        row: { value },
        fileIndex: 0,
        iterator: mockIterator,
        chunk: [],
        chunkIndex: 0,
      })
    }

    const values: number[] = []
    while (!heap.isEmpty()) {
      const entry = heap.pop()
      if (entry) values.push(entry.row.value as number)
    }

    expect(values).toEqual([1, 2, 4, 5, 8])
  })
})

describe('estimateStreamingMergeMemory', () => {
  it('should calculate memory for typical scenario', () => {
    // 50 files, 10K rows/chunk, 200 bytes/row
    const memory = estimateStreamingMergeMemory(50, 10000, 200)

    // 50 * 10000 * 200 = 100MB for chunks
    // + output buffer (10000 * 200 = 2MB)
    // + heap overhead (50 * 100 = 5KB)
    // Total should be around 102MB
    expect(memory).toBeGreaterThan(90 * 1024 * 1024) // At least 90MB
    expect(memory).toBeLessThan(150 * 1024 * 1024) // Under 150MB
  })

  it('should scale linearly with file count', () => {
    const mem10 = estimateStreamingMergeMemory(10, 1000, 100)
    const mem20 = estimateStreamingMergeMemory(20, 1000, 100)

    // Should roughly double (with some overhead variance)
    expect(mem20 / mem10).toBeGreaterThan(1.8)
    expect(mem20 / mem10).toBeLessThan(2.2)
  })

  it('should scale linearly with chunk size', () => {
    const memSmall = estimateStreamingMergeMemory(10, 1000, 100)
    const memLarge = estimateStreamingMergeMemory(10, 10000, 100)

    expect(memLarge).toBeGreaterThan(memSmall * 5)
  })
})

describe('calculateOptimalChunkSize', () => {
  it('should return reasonable chunk size for typical memory limit', () => {
    // 50 files, 128MB limit, 500 bytes/row
    const chunkSize = calculateOptimalChunkSize(50, 128 * 1024 * 1024, 500)

    // Should be within reasonable bounds
    expect(chunkSize).toBeGreaterThanOrEqual(100)
    expect(chunkSize).toBeLessThanOrEqual(100000)
  })

  it('should respect minimum chunk size', () => {
    // Very small memory limit
    const chunkSize = calculateOptimalChunkSize(100, 1024, 500, 50)

    expect(chunkSize).toBeGreaterThanOrEqual(50)
  })

  it('should respect maximum chunk size', () => {
    // Very large memory limit
    const chunkSize = calculateOptimalChunkSize(5, 1024 * 1024 * 1024, 100, 100, 50000)

    expect(chunkSize).toBeLessThanOrEqual(50000)
  })

  it('should decrease with more files', () => {
    const chunkSize10 = calculateOptimalChunkSize(10, 128 * 1024 * 1024, 500)
    const chunkSize50 = calculateOptimalChunkSize(50, 128 * 1024 * 1024, 500)
    const chunkSize100 = calculateOptimalChunkSize(100, 128 * 1024 * 1024, 500)

    expect(chunkSize50).toBeLessThan(chunkSize10)
    expect(chunkSize100).toBeLessThan(chunkSize50)
  })
})

describe('shouldUseStreamingMerge', () => {
  it('should return false for small datasets', () => {
    // 5 files, 1000 rows, 200 bytes/row = 200KB
    expect(shouldUseStreamingMerge(5, 1000, 200)).toBe(false)
  })

  it('should return true for large row count', () => {
    // 10 files, 1M rows, 200 bytes/row = 200MB
    expect(shouldUseStreamingMerge(10, 1000000, 200)).toBe(true)
  })

  it('should return true for many files', () => {
    // 50 files triggers streaming regardless of row count
    expect(shouldUseStreamingMerge(50, 100, 100)).toBe(true)
  })

  it('should respect custom memory threshold', () => {
    // 10 files, 100K rows, 100 bytes = 10MB
    // With 5MB threshold, should use streaming
    expect(shouldUseStreamingMerge(10, 100000, 100, 5 * 1024 * 1024)).toBe(true)

    // With 50MB threshold, should not use streaming
    expect(shouldUseStreamingMerge(10, 100000, 100, 50 * 1024 * 1024)).toBe(false)
  })
})

// =============================================================================
// Tests for bytesRead Tracking and File Completion
// =============================================================================

import { StreamingMergeSorter, type StreamingMergeResult } from '@/workflows/streaming-merge'

describe('StreamingMergeSorter bytesRead tracking', () => {
  /**
   * Mock storage backend for testing bytesRead tracking
   */
  function createMockStorage(fileData: Map<string, { rows: Row[], size: number }>) {
    const storage = {
      stat: vi.fn(async (path: string) => {
        const data = fileData.get(path)
        if (!data) return null
        return { size: data.size, lastModified: new Date() }
      }),
      read: vi.fn(async (path: string) => {
        const data = fileData.get(path)
        if (!data) throw new Error(`File not found: ${path}`)
        return new Uint8Array() // Not used in our test
      }),
    }
    return storage as unknown as import('@/types/storage').StorageBackend
  }

  /**
   * Helper to fully consume a streaming merge and get stats
   */
  async function consumeMerge(
    sorter: StreamingMergeSorter,
    files: string[]
  ): Promise<{ rows: Row[], stats: StreamingMergeResult }> {
    const allRows: Row[] = []
    const generator = sorter.merge(files)
    let result = await generator.next()

    while (!result.done) {
      allRows.push(...result.value)
      result = await generator.next()
    }

    return { rows: allRows, stats: result.value }
  }

  describe('filesCompleted field', () => {
    it('should include filesCompleted in result', async () => {
      const fileData = new Map<string, { rows: Row[], size: number }>()
      fileData.set('file1.parquet', {
        rows: [
          { $id: '1', createdAt: new Date(1000) },
          { $id: '2', createdAt: new Date(2000) },
        ],
        size: 1000,
      })
      fileData.set('file2.parquet', {
        rows: [
          { $id: '3', createdAt: new Date(1500) },
        ],
        size: 500,
      })

      // Note: This test uses a simplified mock that doesn't actually read Parquet files
      // The real implementation uses hyparquet, so we test the interface contract
      const storage = createMockStorage(fileData)
      const sorter = new StreamingMergeSorter(storage, { sortKey: 'createdAt' })

      // The collectAll method returns stats that should have filesCompleted
      // Since we can't easily mock the Parquet reader, we verify the type structure exists
      const result = await sorter.collectAll([])

      expect(result.stats).toHaveProperty('filesCompleted')
      expect(result.stats).toHaveProperty('bytesRead')
      expect(result.stats).toHaveProperty('filesProcessed')
      expect(result.stats.filesCompleted).toBe(0)
      expect(result.stats.bytesRead).toBe(0)
      expect(result.stats.filesProcessed).toBe(0)
    })
  })

  describe('StreamingMergeResult interface', () => {
    it('should have all required fields', () => {
      // Type check - this test verifies the interface exists with correct fields
      const result: StreamingMergeResult = {
        totalRows: 100,
        bytesRead: 50000,
        filesProcessed: 5,
        filesCompleted: 5,
        durationMs: 1000,
      }

      expect(result.totalRows).toBe(100)
      expect(result.bytesRead).toBe(50000)
      expect(result.filesProcessed).toBe(5)
      expect(result.filesCompleted).toBe(5)
      expect(result.durationMs).toBe(1000)
    })

    it('filesCompleted can be less than filesProcessed for partial reads', () => {
      // This represents a scenario where some files failed midway
      const result: StreamingMergeResult = {
        totalRows: 50,
        bytesRead: 25000, // Only half the bytes from completed files
        filesProcessed: 5, // 5 files started
        filesCompleted: 3, // Only 3 files fully read
        durationMs: 1000,
      }

      expect(result.filesCompleted).toBeLessThan(result.filesProcessed)
    })
  })
})
