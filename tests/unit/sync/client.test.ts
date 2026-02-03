/**
 * SyncClient Tests
 *
 * Tests for retry logic with exponential backoff in SyncClient.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { SyncClient, createSyncClient } from '../../../src/sync/client'

// Mock fetch globally
const originalFetch = globalThis.fetch

describe('SyncClient', () => {
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(() => {
    mockFetch = vi.fn()
    globalThis.fetch = mockFetch
    vi.useFakeTimers()
  })

  afterEach(() => {
    globalThis.fetch = originalFetch
    vi.useRealTimers()
  })

  describe('retry with exponential backoff', () => {
    it('should succeed on first attempt without retry', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve({ id: 'db-123' }),
      })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      const promise = client.lookupDatabase('user', 'slug')
      await vi.runAllTimersAsync()
      const result = await promise

      expect(result).toEqual({ id: 'db-123' })
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should retry on network error and succeed', async () => {
      // First call fails with network error
      mockFetch
        .mockRejectedValueOnce(new TypeError('Failed to fetch'))
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ id: 'db-123' }),
        })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      const promise = client.lookupDatabase('user', 'slug')
      await vi.runAllTimersAsync()
      const result = await promise

      expect(result).toEqual({ id: 'db-123' })
      expect(mockFetch).toHaveBeenCalledTimes(2)
    })

    it('should retry on HTTP 429 (rate limited) and succeed', async () => {
      mockFetch
        .mockResolvedValueOnce({
          ok: false,
          status: 429,
          statusText: 'Too Many Requests',
        })
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ id: 'db-123' }),
        })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      const promise = client.lookupDatabase('user', 'slug')
      await vi.runAllTimersAsync()
      const result = await promise

      expect(result).toEqual({ id: 'db-123' })
      expect(mockFetch).toHaveBeenCalledTimes(2)
    })

    it('should retry on HTTP 503 (service unavailable) and succeed', async () => {
      mockFetch
        .mockResolvedValueOnce({
          ok: false,
          status: 503,
          statusText: 'Service Unavailable',
        })
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ id: 'db-123' }),
        })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      const promise = client.lookupDatabase('user', 'slug')
      await vi.runAllTimersAsync()
      const result = await promise

      expect(result).toEqual({ id: 'db-123' })
      expect(mockFetch).toHaveBeenCalledTimes(2)
    })

    it('should retry with exponential backoff delays (100ms, 200ms, 400ms)', async () => {
      // Track when each call happens
      const callTimes: number[] = []

      mockFetch.mockImplementation(() => {
        callTimes.push(Date.now())
        if (callTimes.length < 4) {
          return Promise.reject(new TypeError('Failed to fetch'))
        }
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () => Promise.resolve({ id: 'db-123' }),
        })
      })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      const promise = client.lookupDatabase('user', 'slug')
      await vi.runAllTimersAsync()
      const result = await promise

      expect(result).toEqual({ id: 'db-123' })
      expect(mockFetch).toHaveBeenCalledTimes(4)

      // Verify exponential backoff delays
      // Attempt 1: immediate
      // Attempt 2: after 100ms
      // Attempt 3: after 200ms
      // Attempt 4: after 400ms
      expect(callTimes[1] - callTimes[0]).toBe(100)
      expect(callTimes[2] - callTimes[1]).toBe(200)
      expect(callTimes[3] - callTimes[2]).toBe(400)
    })

    it('should throw after max retries exhausted', async () => {
      mockFetch.mockRejectedValue(new TypeError('Failed to fetch'))

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      const promise = client.lookupDatabase('user', 'slug')
      await vi.runAllTimersAsync()

      // lookupDatabase catches errors and returns null
      const result = await promise
      expect(result).toBeNull()

      // Should have tried 4 times (initial + 3 retries)
      expect(mockFetch).toHaveBeenCalledTimes(4)
    })

    it('should not retry on non-retryable HTTP status (400)', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 400,
        statusText: 'Bad Request',
        text: () => Promise.resolve('Invalid request'),
      })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      // Use try/catch instead to properly handle the rejection
      let error: Error | undefined
      try {
        await client.getUploadUrls('db-123', [{ path: 'test.parquet', size: 100 }])
      } catch (e) {
        error = e as Error
      }

      expect(error).toBeDefined()
      expect(error?.message).toContain('Failed to get upload URLs')
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should not retry on AbortError (timeout)', async () => {
      const abortError = new Error('Aborted')
      abortError.name = 'AbortError'
      mockFetch.mockRejectedValueOnce(abortError)

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
        timeout: 100,
      })

      const promise = client.lookupDatabase('user', 'slug')
      await vi.runAllTimersAsync()

      // lookupDatabase catches errors and returns null
      const result = await promise
      expect(result).toBeNull()

      // Should not retry on abort
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })
  })

  describe('uploadFile retry', () => {
    it('should retry upload on network error', async () => {
      mockFetch
        .mockRejectedValueOnce(new TypeError('Failed to fetch'))
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
        })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      const promise = client.uploadFile(
        {
          path: 'test.parquet',
          url: 'https://presigned.example.com/upload',
          headers: { 'Content-Type': 'application/octet-stream' },
          expiresAt: '2024-12-31T23:59:59Z',
        },
        new Uint8Array([1, 2, 3])
      )
      await vi.runAllTimersAsync()
      await promise

      expect(mockFetch).toHaveBeenCalledTimes(2)
    })

    it('should retry upload on HTTP 503', async () => {
      mockFetch
        .mockResolvedValueOnce({
          ok: false,
          status: 503,
          statusText: 'Service Unavailable',
        })
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
        })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      const promise = client.uploadFile(
        {
          path: 'test.parquet',
          url: 'https://presigned.example.com/upload',
          headers: {},
          expiresAt: '2024-12-31T23:59:59Z',
        },
        new Uint8Array([1, 2, 3])
      )
      await vi.runAllTimersAsync()
      await promise

      expect(mockFetch).toHaveBeenCalledTimes(2)
    })
  })

  describe('downloadFile retry', () => {
    it('should retry download on network error', async () => {
      mockFetch
        .mockRejectedValueOnce(new TypeError('Failed to fetch'))
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
          arrayBuffer: () => Promise.resolve(new ArrayBuffer(3)),
        })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      const promise = client.downloadFile({
        path: 'test.parquet',
        url: 'https://presigned.example.com/download',
        expiresAt: '2024-12-31T23:59:59Z',
      })
      await vi.runAllTimersAsync()
      const result = await promise

      expect(result).toBeInstanceOf(Uint8Array)
      expect(mockFetch).toHaveBeenCalledTimes(2)
    })

    it('should retry download on HTTP 502', async () => {
      mockFetch
        .mockResolvedValueOnce({
          ok: false,
          status: 502,
          statusText: 'Bad Gateway',
        })
        .mockResolvedValueOnce({
          ok: true,
          status: 200,
          arrayBuffer: () => Promise.resolve(new ArrayBuffer(3)),
        })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      const promise = client.downloadFile({
        path: 'test.parquet',
        url: 'https://presigned.example.com/download',
        expiresAt: '2024-12-31T23:59:59Z',
      })
      await vi.runAllTimersAsync()
      const result = await promise

      expect(result).toBeInstanceOf(Uint8Array)
      expect(mockFetch).toHaveBeenCalledTimes(2)
    })
  })

  describe('createSyncClient factory', () => {
    it('should create a SyncClient instance', () => {
      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      expect(client).toBeInstanceOf(SyncClient)
    })
  })
})
