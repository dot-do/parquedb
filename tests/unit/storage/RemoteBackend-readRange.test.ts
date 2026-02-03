/**
 * RemoteBackend readRange Tests
 *
 * Tests for the RemoteBackend's readRange method to ensure it correctly
 * converts exclusive end positions to HTTP Range header format (which uses
 * inclusive end positions).
 *
 * HTTP Range header: bytes=start-end (inclusive)
 * Our interface:     readRange(path, start, end) where end is exclusive
 *
 * Example: To read bytes [0,1,2,3,4] (5 bytes starting at 0):
 *   - Our API: readRange(path, 0, 5)
 *   - HTTP:    Range: bytes=0-4
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { RemoteBackend } from '../../../src/storage/RemoteBackend'

describe('RemoteBackend readRange', () => {
  let mockFetch: ReturnType<typeof vi.fn>
  let backend: RemoteBackend

  beforeEach(() => {
    mockFetch = vi.fn()
    backend = new RemoteBackend({
      baseUrl: 'https://example.com/db/test',
      fetch: mockFetch,
    })
  })

  describe('exclusive end position conversion to HTTP Range', () => {
    it('readRange(0, 5) should use Range: bytes=0-4 (inclusive)', async () => {
      // Create test data: 10 bytes [0,1,2,3,4,5,6,7,8,9]
      const testData = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 206,
        arrayBuffer: async () => testData.slice(0, 5).buffer,
      })

      const result = await backend.readRange('test.bin', 0, 5)

      // Verify the Range header was set correctly
      expect(mockFetch).toHaveBeenCalledWith(
        'https://example.com/db/test/test.bin',
        expect.objectContaining({
          headers: expect.objectContaining({
            Range: 'bytes=0-4', // 0-4 inclusive = 5 bytes = [0,1,2,3,4]
          }),
        })
      )

      expect(result.length).toBe(5)
      expect(Array.from(result)).toEqual([0, 1, 2, 3, 4])
    })

    it('readRange(2, 6) should use Range: bytes=2-5 (inclusive)', async () => {
      const testData = new Uint8Array([2, 3, 4, 5])

      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 206,
        arrayBuffer: async () => testData.buffer,
      })

      await backend.readRange('test.bin', 2, 6)

      expect(mockFetch).toHaveBeenCalledWith(
        'https://example.com/db/test/test.bin',
        expect.objectContaining({
          headers: expect.objectContaining({
            Range: 'bytes=2-5', // 2-5 inclusive = 4 bytes = [2,3,4,5]
          }),
        })
      )
    })

    it('readRange(5, 6) (single byte) should use Range: bytes=5-5', async () => {
      const testData = new Uint8Array([5])

      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 206,
        arrayBuffer: async () => testData.buffer,
      })

      await backend.readRange('test.bin', 5, 6)

      expect(mockFetch).toHaveBeenCalledWith(
        'https://example.com/db/test/test.bin',
        expect.objectContaining({
          headers: expect.objectContaining({
            Range: 'bytes=5-5', // Single byte at position 5
          }),
        })
      )
    })

    it('readRange(0, 10) should use Range: bytes=0-9', async () => {
      const testData = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 206,
        arrayBuffer: async () => testData.buffer,
      })

      await backend.readRange('test.bin', 0, 10)

      expect(mockFetch).toHaveBeenCalledWith(
        'https://example.com/db/test/test.bin',
        expect.objectContaining({
          headers: expect.objectContaining({
            Range: 'bytes=0-9',
          }),
        })
      )
    })
  })

  describe('edge cases', () => {
    it('readRange(0, 0) (empty range) should handle gracefully', async () => {
      // Empty range - this is an edge case
      // Implementation could either not make a request or make one with an empty response
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 206,
        arrayBuffer: async () => new ArrayBuffer(0),
      })

      const result = await backend.readRange('test.bin', 0, 0)
      expect(result.length).toBe(0)
    })

    it('should handle negative start (suffix range) correctly', async () => {
      const testData = new Uint8Array([7, 8, 9])

      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 206,
        arrayBuffer: async () => testData.buffer,
      })

      await backend.readRange('test.bin', -3, -1)

      // Negative ranges in HTTP: bytes=-3 means last 3 bytes
      expect(mockFetch).toHaveBeenCalledWith(
        'https://example.com/db/test/test.bin',
        expect.objectContaining({
          headers: expect.objectContaining({
            Range: 'bytes=-3',
          }),
        })
      )
    })
  })
})
