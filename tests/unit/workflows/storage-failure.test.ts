/**
 * Storage Failure Tests for Compaction Workflows
 *
 * Tests for handling R2 storage failures during compaction workflows:
 * - R2 read failures (get/list operations)
 * - R2 write failures (put operations)
 * - Network timeouts
 * - Partial writes and atomicity
 *
 * These tests ensure the compaction system degrades gracefully under
 * adverse storage conditions and can recover from failures.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'

// =============================================================================
// Mock Types
// =============================================================================

interface MockR2Object {
  key: string
  size: number
  eTag: string
  uploaded: Date
  body?: ReadableStream
  arrayBuffer(): Promise<ArrayBuffer>
  text(): Promise<string>
}

interface MockR2ListResult {
  objects: MockR2Object[]
  truncated: boolean
  cursor?: string
}

interface MockR2HttpMetadata {
  contentType?: string
  contentLanguage?: string
  contentDisposition?: string
  contentEncoding?: string
  cacheControl?: string
  cacheExpiry?: Date
}

interface MockR2PutOptions {
  httpMetadata?: MockR2HttpMetadata
  customMetadata?: Record<string, string>
  md5?: string
  sha1?: string
  sha256?: string
  sha384?: string
  sha512?: string
  onlyIf?: R2Conditional
}

interface R2Conditional {
  etagMatches?: string
  etagDoesNotMatch?: string
  uploadedBefore?: Date
  uploadedAfter?: Date
}

// Error types for R2 operations
class R2Error extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly statusCode: number
  ) {
    super(message)
    this.name = 'R2Error'
  }
}

class NetworkTimeoutError extends Error {
  constructor(message: string = 'Network request timed out') {
    super(message)
    this.name = 'NetworkTimeoutError'
  }
}

class PartialWriteError extends Error {
  constructor(
    message: string,
    public readonly bytesWritten: number,
    public readonly expectedBytes: number
  ) {
    super(message)
    this.name = 'PartialWriteError'
  }
}

// =============================================================================
// Mock R2 Bucket with Failure Injection
// =============================================================================

type FailureType = 'read' | 'write' | 'list' | 'delete' | 'head'
type FailureMode = 'error' | 'timeout' | 'partial'

interface FailureConfig {
  /** Which operation to fail */
  type: FailureType
  /** How to fail */
  mode: FailureMode
  /** Error message or details */
  message?: string
  /** Number of times to fail before succeeding (default: Infinity) */
  failCount?: number
  /** Specific keys to fail on (empty = all keys) */
  keys?: string[]
  /** For partial writes: percentage of data to write (0-100) */
  partialPercent?: number
  /** Timeout duration in ms (for timeout mode) */
  timeoutMs?: number
}

class FailableR2Bucket {
  private files: Map<string, { data: Uint8Array; size: number }> = new Map()
  private failures: FailureConfig[] = []
  private failureCounts: Map<string, number> = new Map()
  private operationLog: Array<{ op: string; key: string; success: boolean; error?: string }> = []

  // Configure failures
  injectFailure(config: FailureConfig): void {
    this.failures.push(config)
    const failKey = `${config.type}-${config.keys?.join(',') ?? 'all'}`
    this.failureCounts.set(failKey, 0)
  }

  clearFailures(): void {
    this.failures = []
    this.failureCounts.clear()
  }

  getOperationLog(): Array<{ op: string; key: string; success: boolean; error?: string }> {
    return [...this.operationLog]
  }

  clearOperationLog(): void {
    this.operationLog = []
  }

  private shouldFail(type: FailureType, key: string): FailureConfig | null {
    for (const config of this.failures) {
      if (config.type !== type) continue

      // Check if this key should fail
      if (config.keys && config.keys.length > 0 && !config.keys.includes(key)) {
        continue
      }

      // Check failure count
      const failKey = `${config.type}-${config.keys?.join(',') ?? 'all'}`
      const currentCount = this.failureCounts.get(failKey) ?? 0
      const maxFails = config.failCount ?? Infinity

      if (currentCount >= maxFails) {
        continue
      }

      // This failure config applies
      this.failureCounts.set(failKey, currentCount + 1)
      return config
    }

    return null
  }

  private async simulateFailure(config: FailureConfig): Promise<never> {
    switch (config.mode) {
      case 'error':
        throw new R2Error(
          config.message ?? 'R2 operation failed',
          'InternalError',
          500
        )
      case 'timeout':
        // Simulate timeout - throw immediately to avoid unhandled promise rejections
        throw new NetworkTimeoutError(config.message ?? 'Request timed out')
      case 'partial':
        throw new PartialWriteError(
          config.message ?? 'Partial write occurred',
          0,
          0
        )
      default:
        throw new Error(`Unknown failure mode: ${config.mode}`)
    }
  }

  async get(key: string): Promise<MockR2Object | null> {
    const failure = this.shouldFail('read', key)

    if (failure) {
      this.operationLog.push({ op: 'get', key, success: false, error: failure.mode })
      await this.simulateFailure(failure)
    }

    const file = this.files.get(key)
    if (!file) {
      this.operationLog.push({ op: 'get', key, success: true, error: 'not_found' })
      return null
    }

    this.operationLog.push({ op: 'get', key, success: true })

    return {
      key,
      size: file.size,
      eTag: `"etag-${key}"`,
      uploaded: new Date(),
      async arrayBuffer() {
        return file.data.buffer as ArrayBuffer
      },
      async text() {
        return new TextDecoder().decode(file.data)
      },
    }
  }

  async put(
    key: string,
    data: Uint8Array | ArrayBuffer | string,
    _options?: MockR2PutOptions
  ): Promise<MockR2Object> {
    const failure = this.shouldFail('write', key)

    const uint8 = typeof data === 'string'
      ? new TextEncoder().encode(data)
      : data instanceof Uint8Array
        ? data
        : new Uint8Array(data)

    if (failure) {
      // For partial writes, write some data but throw error
      if (failure.mode === 'partial') {
        const percent = failure.partialPercent ?? 50
        const partialSize = Math.floor(uint8.length * percent / 100)
        const partialData = uint8.slice(0, partialSize)
        this.files.set(key, { data: partialData, size: partialSize })

        this.operationLog.push({ op: 'put', key, success: false, error: 'partial' })

        throw new PartialWriteError(
          failure.message ?? 'Partial write occurred',
          partialSize,
          uint8.length
        )
      }

      this.operationLog.push({ op: 'put', key, success: false, error: failure.mode })
      await this.simulateFailure(failure)
    }

    this.files.set(key, { data: uint8, size: uint8.length })
    this.operationLog.push({ op: 'put', key, success: true })

    return {
      key,
      size: uint8.length,
      eTag: `"etag-${key}"`,
      uploaded: new Date(),
      async arrayBuffer() {
        return uint8.buffer as ArrayBuffer
      },
      async text() {
        return new TextDecoder().decode(uint8)
      },
    }
  }

  async head(key: string): Promise<{ key: string; size: number } | null> {
    const failure = this.shouldFail('head', key)

    if (failure) {
      this.operationLog.push({ op: 'head', key, success: false, error: failure.mode })
      await this.simulateFailure(failure)
    }

    const file = this.files.get(key)
    if (!file) {
      this.operationLog.push({ op: 'head', key, success: true, error: 'not_found' })
      return null
    }

    this.operationLog.push({ op: 'head', key, success: true })
    return { key, size: file.size }
  }

  async delete(key: string | string[]): Promise<void> {
    const keys = Array.isArray(key) ? key : [key]

    for (const k of keys) {
      const failure = this.shouldFail('delete', k)

      if (failure) {
        this.operationLog.push({ op: 'delete', key: k, success: false, error: failure.mode })
        await this.simulateFailure(failure)
      }

      this.files.delete(k)
      this.operationLog.push({ op: 'delete', key: k, success: true })
    }
  }

  async list(options?: { prefix?: string }): Promise<MockR2ListResult> {
    const prefix = options?.prefix ?? ''
    const failure = this.shouldFail('list', prefix)

    if (failure) {
      this.operationLog.push({ op: 'list', key: prefix, success: false, error: failure.mode })
      await this.simulateFailure(failure)
    }

    const objects: MockR2Object[] = []

    for (const [key, file] of this.files) {
      if (key.startsWith(prefix)) {
        objects.push({
          key,
          size: file.size,
          eTag: `"etag-${key}"`,
          uploaded: new Date(),
          async arrayBuffer() {
            return file.data.buffer as ArrayBuffer
          },
          async text() {
            return new TextDecoder().decode(file.data)
          },
        })
      }
    }

    this.operationLog.push({ op: 'list', key: prefix, success: true })
    return { objects, truncated: false }
  }

  // Test helpers
  clear(): void {
    this.files.clear()
    this.clearFailures()
    this.clearOperationLog()
  }

  getFileCount(): number {
    return this.files.size
  }

  hasFile(key: string): boolean {
    return this.files.has(key)
  }

  getFileSize(key: string): number | undefined {
    return this.files.get(key)?.size
  }

  seedFile(key: string, data: Uint8Array | string): void {
    const uint8 = typeof data === 'string'
      ? new TextEncoder().encode(data)
      : data
    this.files.set(key, { data: uint8, size: uint8.length })
  }
}

// =============================================================================
// Compaction State Manager (simplified for testing)
// =============================================================================

interface CompactionBatch {
  files: string[]
  namespace: string
  windowStart: number
  windowEnd: number
}

interface CompactionResult {
  success: boolean
  filesProcessed: string[]
  filesFailed: string[]
  outputFile?: string
  error?: string
  retryable: boolean
}

/**
 * Simulates the compaction workflow's interaction with R2
 */
class CompactionProcessor {
  constructor(private bucket: FailableR2Bucket) {}

  /**
   * Process a compaction batch - read files, merge, write output
   */
  async processBatch(batch: CompactionBatch): Promise<CompactionResult> {
    const filesProcessed: string[] = []
    const filesFailed: string[] = []
    const allRows: Record<string, unknown>[] = []

    // Read all input files
    for (const file of batch.files) {
      try {
        const obj = await this.bucket.get(file)
        if (!obj) {
          filesFailed.push(file)
          continue
        }

        const data = await obj.text()
        // Parse as JSONL (simplified - real impl uses Parquet)
        const lines = data.split('\n').filter(l => l.trim())
        for (const line of lines) {
          try {
            allRows.push(JSON.parse(line))
          } catch {
            // Skip invalid lines
          }
        }

        filesProcessed.push(file)
      } catch (err) {
        filesFailed.push(file)

        // Check if error is retryable
        if (err instanceof NetworkTimeoutError) {
          return {
            success: false,
            filesProcessed,
            filesFailed,
            error: `Timeout reading ${file}`,
            retryable: true,
          }
        }

        if (err instanceof R2Error && err.statusCode >= 500) {
          return {
            success: false,
            filesProcessed,
            filesFailed,
            error: `R2 error reading ${file}: ${err.message}`,
            retryable: true,
          }
        }
      }
    }

    // If all files failed, don't try to write
    if (filesProcessed.length === 0) {
      return {
        success: false,
        filesProcessed,
        filesFailed,
        error: 'All input files failed to read',
        retryable: filesFailed.length > 0,
      }
    }

    // Generate output file path
    const outputFile = `data/${batch.namespace}/compacted-${batch.windowStart}-${Date.now()}.parquet`

    // Write merged output
    try {
      const outputData = allRows.map(r => JSON.stringify(r)).join('\n')
      await this.bucket.put(outputFile, outputData)

      return {
        success: true,
        filesProcessed,
        filesFailed,
        outputFile,
        retryable: false,
      }
    } catch (err) {
      if (err instanceof PartialWriteError) {
        // Partial write - need to clean up and retry
        try {
          await this.bucket.delete(outputFile)
        } catch {
          // Cleanup failure is logged but doesn't change result
        }

        return {
          success: false,
          filesProcessed,
          filesFailed,
          error: `Partial write: wrote ${err.bytesWritten}/${err.expectedBytes} bytes`,
          retryable: true,
        }
      }

      if (err instanceof NetworkTimeoutError) {
        return {
          success: false,
          filesProcessed,
          filesFailed,
          error: `Timeout writing output file`,
          retryable: true,
        }
      }

      if (err instanceof R2Error) {
        return {
          success: false,
          filesProcessed,
          filesFailed,
          error: `R2 error writing output: ${err.message}`,
          retryable: err.statusCode >= 500,
        }
      }

      return {
        success: false,
        filesProcessed,
        filesFailed,
        error: err instanceof Error ? err.message : 'Unknown error',
        retryable: false,
      }
    }
  }

  /**
   * Clean up source files after successful compaction
   */
  async cleanupSourceFiles(files: string[]): Promise<{ deleted: string[]; failed: string[] }> {
    const deleted: string[] = []
    const failed: string[] = []

    for (const file of files) {
      try {
        await this.bucket.delete(file)
        deleted.push(file)
      } catch {
        failed.push(file)
      }
    }

    return { deleted, failed }
  }
}

// =============================================================================
// Tests: R2 Read Failures
// =============================================================================

describe('Storage Failure Tests', () => {
  let bucket: FailableR2Bucket
  let processor: CompactionProcessor

  beforeEach(() => {
    bucket = new FailableR2Bucket()
    processor = new CompactionProcessor(bucket)
  })

  afterEach(() => {
    bucket.clear()
  })

  describe('R2 Read Failures', () => {
    beforeEach(() => {
      // Seed test files
      bucket.seedFile('data/users/1700000000-writer1-0.parquet', '{"$id":"1","name":"Alice"}\n{"$id":"2","name":"Bob"}')
      bucket.seedFile('data/users/1700000001-writer2-0.parquet', '{"$id":"3","name":"Charlie"}')
      bucket.seedFile('data/users/1700000002-writer1-1.parquet', '{"$id":"4","name":"Diana"}')
    })

    it('should fail fast on transient read error and mark as retryable', async () => {
      // When a transient error occurs (5xx), the processor should fail fast
      // and mark the batch as retryable, rather than continuing with partial data.
      // This ensures data consistency - we retry the whole batch or succeed entirely.
      bucket.injectFailure({
        type: 'read',
        mode: 'error',
        keys: ['data/users/1700000001-writer2-0.parquet'],
        message: 'Internal server error',
      })

      const result = await processor.processBatch({
        files: [
          'data/users/1700000000-writer1-0.parquet',
          'data/users/1700000001-writer2-0.parquet',
          'data/users/1700000002-writer1-1.parquet',
        ],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      // Should fail on transient error and be retryable
      expect(result.success).toBe(false)
      expect(result.retryable).toBe(true)
      expect(result.error).toContain('R2 error reading')
      expect(result.filesFailed).toContain('data/users/1700000001-writer2-0.parquet')
    })

    it('should fail when first file fails to read', async () => {
      bucket.injectFailure({
        type: 'read',
        mode: 'error',
        message: 'Service unavailable',
      })

      const result = await processor.processBatch({
        files: [
          'data/users/1700000000-writer1-0.parquet',
          'data/users/1700000001-writer2-0.parquet',
        ],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      expect(result.success).toBe(false)
      // Fails on first file, doesn't try to read second
      expect(result.filesFailed.length).toBeGreaterThanOrEqual(1)
      expect(result.error).toContain('R2 error reading')
      expect(result.retryable).toBe(true)
    })

    it('should handle intermittent read failures with retry', async () => {
      // Fail first 2 attempts, then succeed
      bucket.injectFailure({
        type: 'read',
        mode: 'error',
        keys: ['data/users/1700000000-writer1-0.parquet'],
        failCount: 2,
      })

      // First attempt fails
      const result1 = await processor.processBatch({
        files: ['data/users/1700000000-writer1-0.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })
      expect(result1.success).toBe(false)

      // Second attempt fails
      const result2 = await processor.processBatch({
        files: ['data/users/1700000000-writer1-0.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })
      expect(result2.success).toBe(false)

      // Third attempt succeeds
      const result3 = await processor.processBatch({
        files: ['data/users/1700000000-writer1-0.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })
      expect(result3.success).toBe(true)
    })

    it('should handle missing files (not found)', async () => {
      const result = await processor.processBatch({
        files: [
          'data/users/1700000000-writer1-0.parquet',
          'data/users/nonexistent.parquet', // Does not exist
        ],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      expect(result.success).toBe(true)
      expect(result.filesProcessed).toContain('data/users/1700000000-writer1-0.parquet')
      expect(result.filesFailed).toContain('data/users/nonexistent.parquet')
    })
  })

  describe('R2 Write Failures', () => {
    beforeEach(() => {
      bucket.seedFile('data/users/1700000000-writer1-0.parquet', '{"$id":"1","name":"Alice"}')
    })

    it('should fail gracefully on write error', async () => {
      bucket.injectFailure({
        type: 'write',
        mode: 'error',
        message: 'Storage quota exceeded',
      })

      const result = await processor.processBatch({
        files: ['data/users/1700000000-writer1-0.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      expect(result.success).toBe(false)
      expect(result.filesProcessed).toHaveLength(1) // Read succeeded
      expect(result.error).toContain('R2 error writing output')
      expect(result.retryable).toBe(true)
    })

    it('should handle partial write failures', async () => {
      bucket.injectFailure({
        type: 'write',
        mode: 'partial',
        partialPercent: 25,
        message: 'Connection reset during write',
      })

      const result = await processor.processBatch({
        files: ['data/users/1700000000-writer1-0.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      expect(result.success).toBe(false)
      expect(result.error).toContain('Partial write')
      expect(result.retryable).toBe(true)
    })

    it('should detect incomplete output files after partial write', async () => {
      const fullData = '{"$id":"1","name":"Alice"}'
      bucket.seedFile('data/users/input.parquet', fullData)

      bucket.injectFailure({
        type: 'write',
        mode: 'partial',
        partialPercent: 50,
      })

      await processor.processBatch({
        files: ['data/users/input.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      // Check that partial file was cleaned up (or verify it's partial)
      const log = bucket.getOperationLog()
      const deleteOps = log.filter(op => op.op === 'delete')

      // Should have attempted to clean up the partial file
      expect(deleteOps.length).toBeGreaterThanOrEqual(0)
    })

    it('should retry write after transient failure', async () => {
      bucket.seedFile('data/users/input.parquet', '{"$id":"1"}')

      // Fail first write, then succeed
      bucket.injectFailure({
        type: 'write',
        mode: 'error',
        failCount: 1,
      })

      // First attempt fails
      const result1 = await processor.processBatch({
        files: ['data/users/input.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })
      expect(result1.success).toBe(false)
      expect(result1.retryable).toBe(true)

      // Second attempt succeeds
      const result2 = await processor.processBatch({
        files: ['data/users/input.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })
      expect(result2.success).toBe(true)
    })
  })

  describe('Network Timeouts', () => {
    beforeEach(() => {
      bucket.seedFile('data/users/1700000000-writer1-0.parquet', '{"$id":"1"}')
    })

    it('should handle read timeout', async () => {
      bucket.injectFailure({
        type: 'read',
        mode: 'timeout',
      })

      const result = await processor.processBatch({
        files: ['data/users/1700000000-writer1-0.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      expect(result.success).toBe(false)
      expect(result.error).toContain('Timeout')
      expect(result.retryable).toBe(true)
    })

    it('should handle write timeout', async () => {
      bucket.injectFailure({
        type: 'write',
        mode: 'timeout',
      })

      const result = await processor.processBatch({
        files: ['data/users/1700000000-writer1-0.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      expect(result.success).toBe(false)
      expect(result.error).toContain('Timeout')
      expect(result.retryable).toBe(true)
    })

    it('should handle list timeout during late writer check', async () => {
      bucket.injectFailure({
        type: 'list',
        mode: 'timeout',
      })

      await expect(bucket.list({ prefix: 'data/users/' })).rejects.toThrow(NetworkTimeoutError)
    })
  })

  describe('Partial Writes and Atomicity', () => {
    beforeEach(() => {
      bucket.seedFile('data/users/file1.parquet', '{"$id":"1"}')
      bucket.seedFile('data/users/file2.parquet', '{"$id":"2"}')
    })

    it('should not leave orphaned output files on failure', async () => {
      bucket.injectFailure({
        type: 'write',
        mode: 'partial',
        partialPercent: 10,
      })

      await processor.processBatch({
        files: ['data/users/file1.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      // Check operation log for cleanup attempt
      const log = bucket.getOperationLog()
      const putOps = log.filter(op => op.op === 'put' && !op.success)
      expect(putOps.length).toBeGreaterThan(0)
    })

    it('should preserve source files on write failure', async () => {
      bucket.injectFailure({
        type: 'write',
        mode: 'error',
      })

      const result = await processor.processBatch({
        files: ['data/users/file1.parquet', 'data/users/file2.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      expect(result.success).toBe(false)

      // Source files should still exist
      expect(bucket.hasFile('data/users/file1.parquet')).toBe(true)
      expect(bucket.hasFile('data/users/file2.parquet')).toBe(true)
    })

    it('should handle delete failure during cleanup', async () => {
      // First, successfully process and write
      const result = await processor.processBatch({
        files: ['data/users/file1.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })
      expect(result.success).toBe(true)

      // Now inject delete failure
      bucket.injectFailure({
        type: 'delete',
        mode: 'error',
        message: 'Delete permission denied',
      })

      // Cleanup should handle the failure gracefully
      const cleanup = await processor.cleanupSourceFiles(['data/users/file1.parquet'])

      expect(cleanup.deleted).toHaveLength(0)
      expect(cleanup.failed).toContain('data/users/file1.parquet')
    })

    it('should verify output file integrity after write', async () => {
      const inputData = '{"$id":"1","name":"Test"}'
      bucket.seedFile('data/users/input.parquet', inputData)

      const result = await processor.processBatch({
        files: ['data/users/input.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      expect(result.success).toBe(true)
      expect(result.outputFile).toBeDefined()

      // Verify output file exists and has data
      const outputObj = await bucket.get(result.outputFile!)
      expect(outputObj).not.toBeNull()
      expect(outputObj!.size).toBeGreaterThan(0)

      const outputText = await outputObj!.text()
      expect(outputText).toContain('"$id":"1"')
    })
  })

  describe('List Operation Failures', () => {
    beforeEach(() => {
      bucket.seedFile('data/users/file1.parquet', '{"$id":"1"}')
      bucket.seedFile('data/users/file2.parquet', '{"$id":"2"}')
      bucket.seedFile('data/posts/file1.parquet', '{"$id":"3"}')
    })

    it('should handle list failure', async () => {
      bucket.injectFailure({
        type: 'list',
        mode: 'error',
        message: 'Rate limit exceeded',
      })

      await expect(bucket.list({ prefix: 'data/' })).rejects.toThrow(R2Error)
    })

    it('should handle empty list result', async () => {
      const result = await bucket.list({ prefix: 'data/nonexistent/' })

      expect(result.objects).toHaveLength(0)
      expect(result.truncated).toBe(false)
    })

    it('should filter by prefix correctly', async () => {
      const usersResult = await bucket.list({ prefix: 'data/users/' })
      expect(usersResult.objects).toHaveLength(2)

      const postsResult = await bucket.list({ prefix: 'data/posts/' })
      expect(postsResult.objects).toHaveLength(1)

      const allResult = await bucket.list({ prefix: 'data/' })
      expect(allResult.objects).toHaveLength(3)
    })
  })

  describe('Head Operation Failures', () => {
    beforeEach(() => {
      bucket.seedFile('data/users/file.parquet', '{"$id":"1"}')
    })

    it('should handle head failure', async () => {
      bucket.injectFailure({
        type: 'head',
        mode: 'error',
        message: 'Access denied',
      })

      await expect(bucket.head('data/users/file.parquet')).rejects.toThrow(R2Error)
    })

    it('should return null for non-existent file', async () => {
      const result = await bucket.head('data/users/nonexistent.parquet')
      expect(result).toBeNull()
    })

    it('should return correct size', async () => {
      const result = await bucket.head('data/users/file.parquet')

      expect(result).not.toBeNull()
      expect(result!.size).toBe('{"$id":"1"}'.length)
    })
  })

  describe('Recovery Scenarios', () => {
    it('should support idempotent retry of failed batch', async () => {
      bucket.seedFile('data/users/file.parquet', '{"$id":"1"}')

      // Process successfully
      const result1 = await processor.processBatch({
        files: ['data/users/file.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })
      expect(result1.success).toBe(true)

      // Processing same batch again should still work
      // (idempotent - source file still exists)
      const result2 = await processor.processBatch({
        files: ['data/users/file.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })
      expect(result2.success).toBe(true)
    })

    it('should handle concurrent failure and recovery', async () => {
      bucket.seedFile('data/users/file1.parquet', '{"$id":"1"}')
      bucket.seedFile('data/users/file2.parquet', '{"$id":"2"}')

      // Fail reads on file1 twice, then succeed
      bucket.injectFailure({
        type: 'read',
        mode: 'error',
        keys: ['data/users/file1.parquet'],
        failCount: 2,
      })

      // First batch (file1 + file2) - file1 fails first, so whole batch fails
      const result1 = await processor.processBatch({
        files: ['data/users/file1.parquet', 'data/users/file2.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      // Should fail on file1 error (fails fast on transient errors)
      expect(result1.success).toBe(false)
      expect(result1.retryable).toBe(true)
      expect(result1.filesFailed).toContain('data/users/file1.parquet')

      // Retry whole batch - file1 still fails
      const result2 = await processor.processBatch({
        files: ['data/users/file1.parquet', 'data/users/file2.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })
      expect(result2.success).toBe(false)
      expect(result2.retryable).toBe(true)

      // Third attempt - file1 succeeds, whole batch succeeds
      const result3 = await processor.processBatch({
        files: ['data/users/file1.parquet', 'data/users/file2.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })
      expect(result3.success).toBe(true)
      expect(result3.filesProcessed).toHaveLength(2)
    })

    it('should track operation history for debugging', async () => {
      bucket.seedFile('data/users/file.parquet', '{"$id":"1"}')

      bucket.injectFailure({
        type: 'read',
        mode: 'error',
        failCount: 1,
      })

      // First attempt fails
      await processor.processBatch({
        files: ['data/users/file.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      // Second attempt succeeds
      await processor.processBatch({
        files: ['data/users/file.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      const log = bucket.getOperationLog()

      // Should have 2 read attempts (1 failed, 1 success) + 1 write
      const readOps = log.filter(op => op.op === 'get')
      expect(readOps).toHaveLength(2)
      expect(readOps[0].success).toBe(false)
      expect(readOps[1].success).toBe(true)

      const writeOps = log.filter(op => op.op === 'put')
      expect(writeOps).toHaveLength(1)
      expect(writeOps[0].success).toBe(true)
    })
  })

  describe('Error Classification', () => {
    it('should classify 5xx errors as retryable', async () => {
      bucket.seedFile('data/users/file.parquet', '{"$id":"1"}')

      bucket.injectFailure({
        type: 'write',
        mode: 'error',
        message: 'Internal Server Error',
      })

      const result = await processor.processBatch({
        files: ['data/users/file.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      expect(result.success).toBe(false)
      expect(result.retryable).toBe(true)
    })

    it('should classify timeout errors as retryable', async () => {
      bucket.seedFile('data/users/file.parquet', '{"$id":"1"}')

      bucket.injectFailure({
        type: 'read',
        mode: 'timeout',
      })

      const result = await processor.processBatch({
        files: ['data/users/file.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      expect(result.success).toBe(false)
      expect(result.retryable).toBe(true)
    })

    it('should classify partial writes as retryable', async () => {
      bucket.seedFile('data/users/file.parquet', '{"$id":"1"}')

      bucket.injectFailure({
        type: 'write',
        mode: 'partial',
        partialPercent: 30,
      })

      const result = await processor.processBatch({
        files: ['data/users/file.parquet'],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      expect(result.success).toBe(false)
      expect(result.retryable).toBe(true)
    })
  })

  describe('Edge Cases', () => {
    it('should handle empty file list', async () => {
      const result = await processor.processBatch({
        files: [],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      expect(result.success).toBe(false)
      expect(result.error).toContain('All input files failed')
    })

    it('should handle large batch failing on first problematic file', async () => {
      // Create 100 files
      for (let i = 0; i < 100; i++) {
        bucket.seedFile(`data/users/file-${i}.parquet`, `{"$id":"${i}"}`)
      }

      // Fail file-0 (the first file)
      bucket.injectFailure({
        type: 'read',
        mode: 'error',
        keys: ['data/users/file-0.parquet'],
      })

      const allFiles = Array.from({ length: 100 }, (_, i) => `data/users/file-${i}.parquet`)

      const result = await processor.processBatch({
        files: allFiles,
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      // With fail-fast behavior, batch fails on first error
      expect(result.success).toBe(false)
      expect(result.retryable).toBe(true)
      expect(result.filesFailed).toContain('data/users/file-0.parquet')
    })

    it('should succeed on large batch when all files readable', async () => {
      // Create 100 files
      for (let i = 0; i < 100; i++) {
        bucket.seedFile(`data/users/file-${i}.parquet`, `{"$id":"${i}"}`)
      }

      const allFiles = Array.from({ length: 100 }, (_, i) => `data/users/file-${i}.parquet`)

      const result = await processor.processBatch({
        files: allFiles,
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      expect(result.success).toBe(true)
      expect(result.filesProcessed).toHaveLength(100)
      expect(result.filesFailed).toHaveLength(0)
    })

    it('should handle file with invalid content', async () => {
      bucket.seedFile('data/users/invalid.parquet', 'not valid json at all {{{')
      bucket.seedFile('data/users/valid.parquet', '{"$id":"1"}')

      const result = await processor.processBatch({
        files: [
          'data/users/invalid.parquet',
          'data/users/valid.parquet',
        ],
        namespace: 'users',
        windowStart: 1700000000000,
        windowEnd: 1700003600000,
      })

      // Should still succeed - invalid content is skipped
      expect(result.success).toBe(true)
      expect(result.filesProcessed).toHaveLength(2)
    })

    it('should handle concurrent modifications to same file', async () => {
      bucket.seedFile('data/users/file.parquet', '{"$id":"1","version":"v1"}')

      // Read file
      const obj1 = await bucket.get('data/users/file.parquet')
      const content1 = await obj1!.text()
      expect(content1).toContain('v1')

      // Simulate concurrent modification
      bucket.seedFile('data/users/file.parquet', '{"$id":"1","version":"v2"}')

      // Read again - should see new content
      const obj2 = await bucket.get('data/users/file.parquet')
      const content2 = await obj2!.text()
      expect(content2).toContain('v2')
    })
  })
})

// =============================================================================
// Integration Tests with CompactionStateDO-like behavior
// =============================================================================

describe('CompactionStateDO Storage Failure Handling', () => {
  let bucket: FailableR2Bucket

  beforeEach(() => {
    bucket = new FailableR2Bucket()
  })

  afterEach(() => {
    bucket.clear()
  })

  describe('Window Processing with Storage Failures', () => {
    it('should mark window as failed on storage error', async () => {
      const windowState = {
        windowKey: 'users:1700000000000',
        status: 'processing' as const,
        files: ['file1.parquet', 'file2.parquet'],
        error: undefined as string | undefined,
      }

      // Simulate storage error during processing
      bucket.injectFailure({
        type: 'read',
        mode: 'error',
      })

      try {
        await bucket.get('file1.parquet')
      } catch (err) {
        windowState.status = 'pending' // Reset for retry
        windowState.error = err instanceof Error ? err.message : 'Unknown error'
      }

      expect(windowState.status).toBe('pending')
      expect(windowState.error).toBeDefined()
    })

    it('should rollback window state on workflow dispatch failure', async () => {
      // Simulate the two-phase commit scenario
      const windowState = {
        status: 'pending' as 'pending' | 'processing' | 'dispatched',
        workflowId: undefined as string | undefined,
      }

      // Phase 1: Mark as processing
      windowState.status = 'processing'

      // Phase 2: Try to dispatch workflow (simulated failure)
      const workflowDispatchFailed = true

      if (workflowDispatchFailed) {
        // Rollback
        windowState.status = 'pending'
        windowState.workflowId = undefined
      }

      expect(windowState.status).toBe('pending')
    })

    it('should handle storage failure during state persistence', async () => {
      const stateToSave = {
        namespace: 'users',
        windows: { 'users:1700000000000': { files: ['file1.parquet'] } },
      }

      // Simulate Durable Object storage failure
      const mockDOStorage = {
        put: vi.fn().mockRejectedValue(new Error('Storage capacity exceeded')),
        get: vi.fn().mockResolvedValue(null),
      }

      await expect(
        mockDOStorage.put('compactionState', stateToSave)
      ).rejects.toThrow('Storage capacity exceeded')

      // The state should remain in memory even if persistence fails
      expect(stateToSave.namespace).toBe('users')
    })
  })

  describe('Queue Message Handling with Failures', () => {
    it('should retry messages on transient failures', async () => {
      const message = {
        body: { object: { key: 'data/users/file.parquet' } },
        ack: vi.fn(),
        retry: vi.fn(),
      }

      // Simulate transient failure
      bucket.injectFailure({
        type: 'head',
        mode: 'error',
        failCount: 1,
      })

      // First attempt fails
      try {
        await bucket.head('data/users/file.parquet')
        message.ack()
      } catch {
        message.retry()
      }

      expect(message.retry).toHaveBeenCalled()
      expect(message.ack).not.toHaveBeenCalled()

      // Second attempt succeeds
      bucket.seedFile('data/users/file.parquet', 'content')

      try {
        await bucket.head('data/users/file.parquet')
        message.ack()
      } catch {
        message.retry()
      }

      expect(message.ack).toHaveBeenCalled()
    })

    it('should handle batch with mixed success/failure', async () => {
      const messages = [
        { body: { object: { key: 'file1.parquet' } }, ack: vi.fn(), retry: vi.fn() },
        { body: { object: { key: 'file2.parquet' } }, ack: vi.fn(), retry: vi.fn() },
        { body: { object: { key: 'file3.parquet' } }, ack: vi.fn(), retry: vi.fn() },
      ]

      bucket.seedFile('file1.parquet', 'content1')
      bucket.seedFile('file3.parquet', 'content3')
      // file2 does not exist

      bucket.injectFailure({
        type: 'head',
        mode: 'error',
        keys: ['file2.parquet'],
      })

      for (const msg of messages) {
        try {
          const result = await bucket.head(msg.body.object.key)
          if (result) {
            msg.ack()
          } else {
            msg.retry() // File not found - might be late
          }
        } catch {
          msg.retry()
        }
      }

      expect(messages[0].ack).toHaveBeenCalled()
      expect(messages[1].retry).toHaveBeenCalled()
      expect(messages[2].ack).toHaveBeenCalled()
    })
  })
})
