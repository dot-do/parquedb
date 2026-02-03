/**
 * R2Backend readRange Tests
 *
 * Tests for the R2Backend's readRange method to ensure it correctly
 * uses exclusive end semantics as specified in the StorageBackend interface.
 *
 * R2 API uses offset + length for range reads:
 * - range: { offset: start, length: numBytes }
 *
 * Our interface uses exclusive end:
 * - readRange(path, start, end) where end is exclusive
 *
 * So readRange(path, 0, 5) should read 5 bytes starting at offset 0
 * which translates to R2: { offset: 0, length: 5 }
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { R2Backend } from '../../../src/storage/R2Backend'
import type { R2Bucket, R2Object, R2ObjectBody, R2GetOptions } from '../../../src/storage/types/r2'

/**
 * Create a mock R2Bucket for testing
 */
function createMockR2Bucket(): R2Bucket {
  // Test data: 10 bytes [0,1,2,3,4,5,6,7,8,9]
  const testData = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

  const mockBucket: R2Bucket = {
    get: vi.fn(async (_key: string, options?: R2GetOptions): Promise<R2ObjectBody | null> => {
      // Extract range from options
      const range = options?.range
      let data: Uint8Array

      if (range && 'offset' in range && 'length' in range) {
        const { offset, length } = range
        data = testData.slice(offset, offset + length)
      } else {
        data = testData
      }

      const mockBody: R2ObjectBody = {
        key: 'test.bin',
        size: data.length,
        etag: 'mock-etag',
        httpEtag: '"mock-etag"',
        uploaded: new Date(),
        httpMetadata: {},
        customMetadata: {},
        arrayBuffer: async () => data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength),
        text: async () => new TextDecoder().decode(data),
        json: async () => ({}),
        blob: async () => new Blob([data]),
        body: null as unknown as ReadableStream,
        bodyUsed: false,
        checksums: {},
        writeHttpMetadata: () => {},
      }

      return mockBody
    }),
    head: vi.fn(async (_key: string) => ({
      key: 'test.bin',
      size: testData.length,
      etag: 'mock-etag',
      httpEtag: '"mock-etag"',
      uploaded: new Date(),
      httpMetadata: {},
      customMetadata: {},
      checksums: {},
      writeHttpMetadata: () => {},
    })),
    put: vi.fn(),
    delete: vi.fn(),
    list: vi.fn(async () => ({
      objects: [],
      truncated: false,
      cursor: undefined,
      delimitedPrefixes: [],
    })),
    createMultipartUpload: vi.fn(),
  }

  return mockBucket
}

describe('R2Backend readRange', () => {
  let mockBucket: R2Bucket
  let backend: R2Backend

  beforeEach(() => {
    mockBucket = createMockR2Bucket()
    backend = new R2Backend(mockBucket)
  })

  describe('exclusive end position semantics', () => {
    it('readRange(0, 5) should read 5 bytes (offset=0, length=5)', async () => {
      const result = await backend.readRange('test.bin', 0, 5)

      expect(mockBucket.get).toHaveBeenCalledWith('test.bin', {
        range: { offset: 0, length: 5 },
      })

      expect(result.length).toBe(5)
      expect(Array.from(result)).toEqual([0, 1, 2, 3, 4])
    })

    it('readRange(2, 6) should read 4 bytes (offset=2, length=4)', async () => {
      const result = await backend.readRange('test.bin', 2, 6)

      expect(mockBucket.get).toHaveBeenCalledWith('test.bin', {
        range: { offset: 2, length: 4 },
      })

      expect(result.length).toBe(4)
      expect(Array.from(result)).toEqual([2, 3, 4, 5])
    })

    it('readRange(5, 6) (single byte) should read 1 byte (offset=5, length=1)', async () => {
      const result = await backend.readRange('test.bin', 5, 6)

      expect(mockBucket.get).toHaveBeenCalledWith('test.bin', {
        range: { offset: 5, length: 1 },
      })

      expect(result.length).toBe(1)
      expect(Array.from(result)).toEqual([5])
    })

    it('readRange(0, 10) should read all 10 bytes (offset=0, length=10)', async () => {
      const result = await backend.readRange('test.bin', 0, 10)

      expect(mockBucket.get).toHaveBeenCalledWith('test.bin', {
        range: { offset: 0, length: 10 },
      })

      expect(result.length).toBe(10)
      expect(Array.from(result)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    })

    it('readRange(9, 10) should read last byte (offset=9, length=1)', async () => {
      const result = await backend.readRange('test.bin', 9, 10)

      expect(mockBucket.get).toHaveBeenCalledWith('test.bin', {
        range: { offset: 9, length: 1 },
      })

      expect(result.length).toBe(1)
      expect(Array.from(result)).toEqual([9])
    })
  })

  describe('edge cases', () => {
    it('readRange(5, 5) (empty range) should request 0 bytes', async () => {
      const result = await backend.readRange('test.bin', 5, 5)

      // With length=0, R2 will return empty content
      expect(mockBucket.get).toHaveBeenCalledWith('test.bin', {
        range: { offset: 5, length: 0 },
      })

      expect(result.length).toBe(0)
    })
  })

  describe('consistency with Array.slice semantics', () => {
    it('should match Array.slice behavior for various ranges', async () => {
      const original = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

      const testCases = [
        [0, 5],
        [0, 10],
        [2, 8],
        [0, 1],
        [9, 10],
      ]

      for (const [start, end] of testCases) {
        const result = await backend.readRange('test.bin', start, end)
        const sliceResult = original.slice(start, end)

        expect(Array.from(result)).toEqual(sliceResult)
      }
    })
  })
})
