/**
 * R2Backend Append Race Condition Tests
 *
 * Tests to verify and fix race conditions in the append operation.
 *
 * Issue: parquedb-cpl9.4
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { R2Backend, R2OperationError } from '../../../src/storage/R2Backend'
import type {
  R2Bucket,
  R2Object,
  R2ObjectBody,
  R2PutOptions,
} from '../../../src/storage/types/r2'

// =============================================================================
// Mock R2 Bucket Factory
// =============================================================================

interface MockBucketState {
  storage: Map<string, { data: Uint8Array; etag: string; version: number }>
  getCallCount: number
  putCallCount: number
  operationLog: Array<{ op: string; key: string; timestamp: number }>
}

/**
 * Creates a mock R2Bucket that simulates real R2 behavior including
 * ETags and conditional writes.
 */
function createMockBucket(options?: {
  /** Inject artificial delays to simulate network latency */
  readDelay?: number
  writeDelay?: number
  /** Callback to intercept operations for testing race conditions */
  onOperation?: (op: string, key: string, state: MockBucketState) => Promise<void>
}): { bucket: R2Bucket; state: MockBucketState } {
  const state: MockBucketState = {
    storage: new Map(),
    getCallCount: 0,
    putCallCount: 0,
    operationLog: [],
  }

  const generateEtag = (version: number) => `etag-v${version}`

  const bucket: R2Bucket = {
    async get(key: string): Promise<R2ObjectBody | null> {
      state.getCallCount++
      state.operationLog.push({ op: 'get', key, timestamp: Date.now() })

      if (options?.onOperation) {
        await options.onOperation('get', key, state)
      }

      if (options?.readDelay) {
        // Simulate network latency with minimal delay for test purposes
        await Promise.resolve()
      }

      const entry = state.storage.get(key)
      if (!entry) {
        return null
      }

      const { data, etag, version } = entry
      return {
        key,
        version: `v${version}`,
        size: data.length,
        etag,
        httpEtag: `"${etag}"`,
        uploaded: new Date(),
        storageClass: 'Standard',
        checksums: {},
        writeHttpMetadata: () => {},
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(data)
            controller.close()
          },
        }),
        bodyUsed: false,
        arrayBuffer: async () => data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength),
        text: async () => new TextDecoder().decode(data),
        json: async () => JSON.parse(new TextDecoder().decode(data)),
        blob: async () => new Blob([data]),
      }
    },

    async head(key: string): Promise<R2Object | null> {
      const entry = state.storage.get(key)
      if (!entry) {
        return null
      }

      const { data, etag, version } = entry
      return {
        key,
        version: `v${version}`,
        size: data.length,
        etag,
        httpEtag: `"${etag}"`,
        uploaded: new Date(),
        storageClass: 'Standard',
        checksums: {},
        writeHttpMetadata: () => {},
      }
    },

    async put(key: string, value: any, putOptions?: R2PutOptions): Promise<R2Object | null> {
      state.putCallCount++
      state.operationLog.push({ op: 'put', key, timestamp: Date.now() })

      if (options?.onOperation) {
        await options.onOperation('put', key, state)
      }

      if (options?.writeDelay) {
        // Simulate network latency with minimal delay for test purposes
        await Promise.resolve()
      }

      // Convert value to Uint8Array
      let data: Uint8Array
      if (value instanceof Uint8Array) {
        data = value
      } else if (value instanceof ArrayBuffer) {
        data = new Uint8Array(value)
      } else if (typeof value === 'string') {
        data = new TextEncoder().encode(value)
      } else {
        data = new Uint8Array(0)
      }

      // Handle conditional writes
      if (putOptions?.onlyIf && !(putOptions.onlyIf instanceof Headers)) {
        const cond = putOptions.onlyIf
        const existing = state.storage.get(key)

        // etagMatches: only succeed if current ETag matches expected
        if (cond.etagMatches) {
          if (!existing || existing.etag !== cond.etagMatches) {
            // Precondition failed
            return null
          }
        }

        // etagDoesNotMatch: only succeed if current ETag doesn't match
        // Special case: '*' means "only if object doesn't exist"
        if (cond.etagDoesNotMatch) {
          if (cond.etagDoesNotMatch === '*') {
            if (existing) {
              // Object exists, precondition failed
              return null
            }
          } else if (existing && existing.etag === cond.etagDoesNotMatch) {
            // ETag matches what we don't want, precondition failed
            return null
          }
        }
      }

      // Write the data
      const existing = state.storage.get(key)
      const newVersion = existing ? existing.version + 1 : 1
      const newEtag = generateEtag(newVersion)

      state.storage.set(key, {
        data: new Uint8Array(data), // Copy to avoid mutation
        etag: newEtag,
        version: newVersion,
      })

      return {
        key,
        version: `v${newVersion}`,
        size: data.length,
        etag: newEtag,
        httpEtag: `"${newEtag}"`,
        uploaded: new Date(),
        storageClass: 'Standard',
        checksums: {},
        writeHttpMetadata: () => {},
      }
    },

    async delete(keys: string | string[]): Promise<void> {
      const keyArray = Array.isArray(keys) ? keys : [keys]
      for (const key of keyArray) {
        state.storage.delete(key)
      }
    },

    async list(): Promise<any> {
      return { objects: [], truncated: false, delimitedPrefixes: [] }
    },

    async createMultipartUpload(): Promise<any> {
      throw new Error('Not implemented')
    },

    resumeMultipartUpload(): any {
      throw new Error('Not implemented')
    },
  }

  return { bucket, state }
}

// =============================================================================
// Helper Functions
// =============================================================================

function concatUint8Arrays(...arrays: Uint8Array[]): Uint8Array {
  const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0)
  const result = new Uint8Array(totalLength)
  let offset = 0
  for (const arr of arrays) {
    result.set(arr, offset)
    offset += arr.length
  }
  return result
}

// =============================================================================
// Tests
// =============================================================================

describe('R2Backend append race condition', () => {
  describe('basic append functionality', () => {
    it('should append to an existing file', async () => {
      const { bucket, state } = createMockBucket()
      const backend = new R2Backend(bucket)

      // Write initial data
      await backend.write('test.bin', new Uint8Array([1, 2, 3]))

      // Append more data
      await backend.append('test.bin', new Uint8Array([4, 5, 6]))

      // Verify result
      const result = await backend.read('test.bin')
      expect(result).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6]))
    })

    it('should create file if it does not exist', async () => {
      const { bucket } = createMockBucket()
      const backend = new R2Backend(bucket)

      await backend.append('new.bin', new Uint8Array([7, 8, 9]))

      const result = await backend.read('new.bin')
      expect(result).toEqual(new Uint8Array([7, 8, 9]))
    })
  })

  describe('concurrent append to existing file', () => {
    it('should preserve all data when two appends race on existing file', async () => {
      // This test simulates two concurrent appends to the same file
      // Both should succeed and all data should be preserved

      // Track gets specifically for append operations (not initial write)
      let appendGetCount = 0
      let getsReady: (() => void) | null = null
      let waitForGets: Promise<void> | null = null

      const { bucket } = createMockBucket()

      // Store original methods
      const originalGet = bucket.get.bind(bucket)
      const originalPut = bucket.put.bind(bucket)

      // Track when both appends have read the initial state
      waitForGets = new Promise<void>(resolve => {
        getsReady = resolve
      })

      bucket.get = async (key: string) => {
        appendGetCount++
        const result = await originalGet(key)

        // After second append's get, signal both are ready
        if (appendGetCount >= 2 && getsReady) {
          getsReady()
          getsReady = null
        }

        // First get waits for second get to happen
        if (appendGetCount === 1 && waitForGets) {
          await waitForGets
        }

        return result
      }

      const backend = new R2Backend(bucket)

      // Initial data using direct put to avoid the mock
      await originalPut('race.bin', new Uint8Array([1, 2, 3]))

      // Start two concurrent appends
      const append1 = backend.append('race.bin', new Uint8Array([4]))
      const append2 = backend.append('race.bin', new Uint8Array([5]))

      // Both should complete successfully
      await Promise.all([append1, append2])

      // Final result should contain original data plus both appends
      const result = await backend.read('race.bin')

      // The exact order of 4 and 5 depends on which append won the race,
      // but both must be present
      expect(result.length).toBe(5)
      expect(result.slice(0, 3)).toEqual(new Uint8Array([1, 2, 3]))

      // Both 4 and 5 must be present
      const resultArray = Array.from(result)
      expect(resultArray).toContain(4)
      expect(resultArray).toContain(5)
    })

    it('should handle rapid sequential appends correctly', async () => {
      const { bucket } = createMockBucket()
      const backend = new R2Backend(bucket)

      // Start with empty file
      await backend.write('seq.bin', new Uint8Array([0]))

      // Rapidly append 10 values
      const appends = []
      for (let i = 1; i <= 10; i++) {
        appends.push(backend.append('seq.bin', new Uint8Array([i])))
      }

      await Promise.all(appends)

      const result = await backend.read('seq.bin')

      // All values 0-10 must be present
      expect(result.length).toBe(11)
      const resultSet = new Set(Array.from(result))
      for (let i = 0; i <= 10; i++) {
        expect(resultSet.has(i)).toBe(true)
      }
    })
  })

  describe('concurrent append to non-existent file', () => {
    it('should handle two appends racing to create the same file', async () => {
      // Both appends try to create the file at the same time
      // One should win, the other should retry and append to the winner's data

      const { bucket } = createMockBucket()

      // Store original methods
      const originalGet = bucket.get.bind(bucket)

      // Track gets and synchronize them
      let getCount = 0
      let secondGetReady: (() => void) | null = null
      const waitForSecondGet = new Promise<void>(resolve => {
        secondGetReady = resolve
      })

      bucket.get = async (key: string) => {
        getCount++
        const result = await originalGet(key)

        if (getCount === 1) {
          // First get waits for second get
          await waitForSecondGet
        } else if (getCount === 2 && secondGetReady) {
          // Second get signals that both are ready
          secondGetReady()
        }

        return result
      }

      const backend = new R2Backend(bucket)

      // Start two concurrent appends to non-existent file
      const append1 = backend.append('newrace.bin', new Uint8Array([1, 2]))
      const append2 = backend.append('newrace.bin', new Uint8Array([3, 4]))

      // Both should succeed
      await Promise.all([append1, append2])

      // Final result should contain both appends
      const result = await backend.read('newrace.bin')

      // Both [1,2] and [3,4] must be present, order depends on race winner
      expect(result.length).toBe(4)
      const resultArray = Array.from(result)
      expect(resultArray).toContain(1)
      expect(resultArray).toContain(2)
      expect(resultArray).toContain(3)
      expect(resultArray).toContain(4)
    })
  })

  describe('retry exhaustion', () => {
    it('should throw after MAX_RETRIES when constantly racing', async () => {
      // Simulate a pathological case where every put fails due to races
      let putAttempts = 0

      const { bucket } = createMockBucket()

      // Override put to always fail with precondition failure
      const originalPut = bucket.put.bind(bucket)
      bucket.put = async (key: string, value: any, options?: R2PutOptions) => {
        putAttempts++
        if (options?.onlyIf) {
          // Always fail conditional writes to simulate constant racing
          return null
        }
        return originalPut(key, value, options)
      }

      const backend = new R2Backend(bucket)

      // Initial data
      await backend.write('exhaust.bin', new Uint8Array([1, 2, 3]))

      // This append should fail after MAX_RETRIES (10)
      await expect(
        backend.append('exhaust.bin', new Uint8Array([4, 5, 6]))
      ).rejects.toThrow(R2OperationError)

      // Should have attempted 10 times (after the initial successful write)
      // Initial write = 1, then 10 failed conditional puts = 11 total
      expect(putAttempts).toBe(11)
    }, 15000) // Increase timeout due to exponential backoff delays
  })

  describe('error handling during append', () => {
    it('should handle network errors during get gracefully', async () => {
      const { bucket } = createMockBucket()

      // Make get throw an error
      bucket.get = async () => {
        throw new Error('Network error')
      }

      const backend = new R2Backend(bucket)

      await expect(
        backend.append('error.bin', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow(R2OperationError)
    })

    it('should handle network errors during put and retry', async () => {
      // This tests that transient put errors are retried
      let putAttempts = 0

      const { bucket, state } = createMockBucket()

      const originalPut = bucket.put.bind(bucket)
      bucket.put = async (key: string, value: any, options?: R2PutOptions) => {
        putAttempts++
        if (putAttempts < 2) {
          throw new Error('Transient network error')
        }
        return originalPut(key, value, options)
      }

      const backend = new R2Backend(bucket)

      // This should succeed after retry
      // BUG: Current implementation doesn't retry on exceptions, only on null results
      await backend.append('retry.bin', new Uint8Array([1, 2, 3]))

      const result = await backend.read('retry.bin')
      expect(result).toEqual(new Uint8Array([1, 2, 3]))
    })
  })

  describe('data integrity', () => {
    it('should not lose data during concurrent modifications', async () => {
      // Stress test: many concurrent appends
      const { bucket } = createMockBucket()
      const backend = new R2Backend(bucket)

      // Start with initial data
      await backend.write('stress.bin', new Uint8Array([0]))

      // Launch many concurrent appends
      const numAppends = 20
      const appends = []
      for (let i = 1; i <= numAppends; i++) {
        appends.push(backend.append('stress.bin', new Uint8Array([i])))
      }

      // All should complete
      await Promise.all(appends)

      // Read final result
      const result = await backend.read('stress.bin')

      // Should have all values 0 through numAppends
      expect(result.length).toBe(numAppends + 1)

      const resultSet = new Set(Array.from(result))
      for (let i = 0; i <= numAppends; i++) {
        expect(resultSet.has(i)).toBe(true)
      }
    })

    it('should maintain append ordering within a single thread', async () => {
      const { bucket } = createMockBucket()
      const backend = new R2Backend(bucket)

      // Sequential appends from one "thread"
      await backend.write('order.bin', new Uint8Array([1]))
      await backend.append('order.bin', new Uint8Array([2]))
      await backend.append('order.bin', new Uint8Array([3]))
      await backend.append('order.bin', new Uint8Array([4]))

      const result = await backend.read('order.bin')
      expect(result).toEqual(new Uint8Array([1, 2, 3, 4]))
    })
  })
})
