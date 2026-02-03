/**
 * SyncClient Tests
 *
 * Tests for SyncClient behavior.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { SyncClient, createSyncClient } from '../../../src/sync/client'
import {
  createMockFetch,
  createJsonResponse,
  createErrorResponse,
  installMockFetch,
  restoreGlobalFetch,
  type MockFetch,
} from '../../mocks'

// Complete mock DatabaseInfo for tests
const mockDatabaseInfo = {
  id: 'db-123',
  name: 'test-db',
  owner: 'user',
  slug: 'slug',
  visibility: 'public' as const,
  bucket: 'test-bucket',
}

describe('SyncClient', () => {
  let mockFetch: MockFetch

  beforeEach(() => {
    mockFetch = createMockFetch()
    installMockFetch(mockFetch)
  })

  afterEach(() => {
    restoreGlobalFetch()
  })

  describe('lookupDatabase', () => {
    it('should return database info on success', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.resolve(mockDatabaseInfo),
      })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      const result = await client.lookupDatabase('user', 'slug')

      expect(result).toEqual(mockDatabaseInfo)
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should return null on network error', async () => {
      mockFetch.mockRejectedValueOnce(new TypeError('Failed to fetch'))

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      const result = await client.lookupDatabase('user', 'slug')

      expect(result).toBeNull()
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should return null on HTTP error', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
        statusText: 'Not Found',
      })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      const result = await client.lookupDatabase('user', 'slug')

      expect(result).toBeNull()
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should return null on timeout', async () => {
      const abortError = new Error('Aborted')
      abortError.name = 'AbortError'
      mockFetch.mockRejectedValueOnce(abortError)

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
        timeout: 100,
      })

      const result = await client.lookupDatabase('user', 'slug')

      expect(result).toBeNull()
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })
  })

  describe('getUploadUrls', () => {
    it('should throw on HTTP error', async () => {
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

      await expect(
        client.getUploadUrls('db-123', [{ path: 'test.parquet', size: 100 }])
      ).rejects.toThrow('Failed to get upload URLs')

      expect(mockFetch).toHaveBeenCalledTimes(1)
    })
  })

  describe('uploadFile', () => {
    it('should throw on network error', async () => {
      mockFetch.mockRejectedValueOnce(new TypeError('Failed to fetch'))

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      await expect(
        client.uploadFile(
          {
            path: 'test.parquet',
            url: 'https://presigned.example.com/upload',
            headers: { 'Content-Type': 'application/octet-stream' },
            expiresAt: '2024-12-31T23:59:59Z',
          },
          new Uint8Array([1, 2, 3])
        )
      ).rejects.toThrow('Failed to fetch')

      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should throw on HTTP error', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 503,
        statusText: 'Service Unavailable',
      })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      await expect(
        client.uploadFile(
          {
            path: 'test.parquet',
            url: 'https://presigned.example.com/upload',
            headers: {},
            expiresAt: '2024-12-31T23:59:59Z',
          },
          new Uint8Array([1, 2, 3])
        )
      ).rejects.toThrow('Upload failed')

      expect(mockFetch).toHaveBeenCalledTimes(1)
    })
  })

  describe('downloadFile', () => {
    it('should throw on network error', async () => {
      mockFetch.mockRejectedValueOnce(new TypeError('Failed to fetch'))

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      await expect(
        client.downloadFile({
          path: 'test.parquet',
          url: 'https://presigned.example.com/download',
          expiresAt: '2024-12-31T23:59:59Z',
        })
      ).rejects.toThrow('Failed to fetch')

      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should throw on HTTP error', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 502,
        statusText: 'Bad Gateway',
      })

      const client = createSyncClient({
        baseUrl: 'https://api.example.com',
        token: 'test-token',
      })

      await expect(
        client.downloadFile({
          path: 'test.parquet',
          url: 'https://presigned.example.com/download',
          expiresAt: '2024-12-31T23:59:59Z',
        })
      ).rejects.toThrow('Download failed')

      expect(mockFetch).toHaveBeenCalledTimes(1)
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
