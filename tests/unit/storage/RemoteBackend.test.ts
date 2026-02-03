/**
 * RemoteBackend Unit Tests
 *
 * Tests for the read-only HTTP storage backend that provides access to
 * remote ParqueDB databases.
 *
 * Coverage includes:
 * - Read operations (read, readRange, stat, exists, list)
 * - Error handling (404, 401, 403, network errors)
 * - Authentication (token, custom headers)
 * - Timeout handling
 * - Write operations (throws - read-only backend)
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { RemoteBackend, createRemoteBackend } from '../../../src/storage/RemoteBackend'
import { NotFoundError, PermissionDeniedError, NetworkError } from '../../../src/storage/errors'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a mock fetch function
 */
function createMockFetch() {
  return vi.fn<Parameters<typeof fetch>, ReturnType<typeof fetch>>()
}

/**
 * Create a mock Response object
 */
function createMockResponse(options: {
  status?: number
  statusText?: string
  ok?: boolean
  body?: Uint8Array | Record<string, unknown>
  headers?: Record<string, string>
}): Response {
  const {
    status = 200,
    statusText = 'OK',
    ok = status >= 200 && status < 300,
    body = new Uint8Array(0),
    headers = {},
  } = options

  const headersInstance = new Headers()
  for (const [key, value] of Object.entries(headers)) {
    headersInstance.set(key, value)
  }

  return {
    ok,
    status,
    statusText,
    headers: headersInstance,
    arrayBuffer: async () => {
      if (body instanceof Uint8Array) {
        return body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength)
      }
      return new TextEncoder().encode(JSON.stringify(body)).buffer
    },
    json: async () => {
      if (body instanceof Uint8Array) {
        return JSON.parse(new TextDecoder().decode(body))
      }
      return body
    },
  } as Response
}

/**
 * Create a RemoteBackend with mock fetch
 */
function createTestBackend(options: {
  baseUrl?: string
  token?: string
  headers?: Record<string, string>
  timeout?: number
  mockFetch?: ReturnType<typeof createMockFetch>
}) {
  const mockFetch = options.mockFetch ?? createMockFetch()
  const backend = new RemoteBackend({
    baseUrl: options.baseUrl ?? 'https://parque.db/db/test/dataset',
    token: options.token,
    headers: options.headers,
    timeout: options.timeout,
    fetch: mockFetch,
  })
  return { backend, mockFetch }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('RemoteBackend', () => {
  let mockFetch: ReturnType<typeof createMockFetch>
  let backend: RemoteBackend

  beforeEach(() => {
    const result = createTestBackend({})
    mockFetch = result.mockFetch
    backend = result.backend
  })

  // ===========================================================================
  // Constructor & Type
  // ===========================================================================

  describe('constructor and type', () => {
    it('should have type "remote"', () => {
      expect(backend.type).toBe('remote')
    })

    it('should normalize baseUrl by removing trailing slash', () => {
      const { backend: b, mockFetch: mf } = createTestBackend({
        baseUrl: 'https://example.com/db/test/',
      })

      mf.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: new Uint8Array([1, 2, 3]),
      }))

      b.read('file.txt')

      // URL should not have double slashes
      expect(mf).toHaveBeenCalledWith(
        'https://example.com/db/test/file.txt',
        expect.anything()
      )
    })
  })

  // ===========================================================================
  // read() Tests
  // ===========================================================================

  describe('read(path)', () => {
    it('should make GET request with correct URL', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: new Uint8Array([1, 2, 3, 4, 5]),
      }))

      await backend.read('data/posts/data.parquet')

      expect(mockFetch).toHaveBeenCalledWith(
        'https://parque.db/db/test/dataset/data/posts/data.parquet',
        expect.objectContaining({
          headers: expect.any(Object),
        })
      )
    })

    it('should return file contents as Uint8Array', async () => {
      const expectedData = new Uint8Array([10, 20, 30, 40, 50])
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: expectedData,
      }))

      const result = await backend.read('test.bin')

      expect(result).toBeInstanceOf(Uint8Array)
      expect(Array.from(result)).toEqual(Array.from(expectedData))
    })

    it('should throw PathTraversalError for path with leading slash', async () => {
      // Paths starting with / are rejected for security
      await expect(backend.read('/data/file.txt')).rejects.toThrow('Path traversal')
    })

    it('should throw PathTraversalError for path with ..', async () => {
      await expect(backend.read('data/../../../etc/passwd')).rejects.toThrow('Path traversal')
    })

    it('should throw PathTraversalError for path with //', async () => {
      await expect(backend.read('data//file.txt')).rejects.toThrow('Path traversal')
    })

    it('should throw NotFoundError on 404', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 404,
        statusText: 'Not Found',
        ok: false,
      }))

      await expect(backend.read('nonexistent.txt')).rejects.toThrow(NotFoundError)
    })

    it('should include path in NotFoundError', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 404,
        statusText: 'Not Found',
        ok: false,
      }))

      try {
        await backend.read('nonexistent.txt')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(NotFoundError)
        expect((error as NotFoundError).path).toBe('nonexistent.txt')
      }
    })

    it('should throw PermissionDeniedError on 401', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 401,
        statusText: 'Unauthorized',
        ok: false,
      }))

      await expect(backend.read('private.txt')).rejects.toThrow(PermissionDeniedError)
    })

    it('should throw PermissionDeniedError on 403', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 403,
        statusText: 'Forbidden',
        ok: false,
      }))

      await expect(backend.read('forbidden.txt')).rejects.toThrow(PermissionDeniedError)
    })

    it('should throw NetworkError on other HTTP errors', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 500,
        statusText: 'Internal Server Error',
        ok: false,
      }))

      await expect(backend.read('error.txt')).rejects.toThrow(NetworkError)
    })
  })

  // ===========================================================================
  // readRange() Tests
  // ===========================================================================

  describe('readRange(path, start, end)', () => {
    it('should add Range header for positive range', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 206,
        body: new Uint8Array([0, 1, 2, 3, 4]),
      }))

      await backend.readRange('test.bin', 0, 5)

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            Range: 'bytes=0-4', // Exclusive end converted to inclusive
          }),
        })
      )
    })

    it('should handle negative start (suffix range)', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 206,
        body: new Uint8Array([7, 8, 9]),
      }))

      await backend.readRange('test.bin', -3, -1)

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            Range: 'bytes=-3',
          }),
        })
      )
    })

    it('should handle negative end by fetching stat first', async () => {
      // First call is HEAD for stat
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        headers: {
          'Content-Length': '100',
          'Last-Modified': 'Mon, 01 Jan 2024 00:00:00 GMT',
        },
      }))

      // Second call is GET with Range
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 206,
        body: new Uint8Array([90, 91, 92, 93, 94, 95, 96, 97, 98]),
      }))

      await backend.readRange('test.bin', 0, -1)

      // The range should be 0 to (100 + -1 - 1) = 98 (inclusive)
      expect(mockFetch).toHaveBeenLastCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            Range: 'bytes=0-98',
          }),
        })
      )
    })

    it('should return empty Uint8Array for empty range (start >= end)', async () => {
      // No fetch should be made for empty range
      const result = await backend.readRange('test.bin', 5, 5)

      expect(result).toBeInstanceOf(Uint8Array)
      expect(result.length).toBe(0)
      expect(mockFetch).not.toHaveBeenCalled()
    })

    it('should return empty Uint8Array when start > end', async () => {
      const result = await backend.readRange('test.bin', 10, 5)

      expect(result.length).toBe(0)
      expect(mockFetch).not.toHaveBeenCalled()
    })

    it('should throw NotFoundError on 404', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 404,
        ok: false,
      }))

      await expect(backend.readRange('nonexistent.txt', 0, 10)).rejects.toThrow(NotFoundError)
    })

    it('should throw NotFoundError when stat fails for negative end', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 404,
        ok: false,
      }))

      await expect(backend.readRange('nonexistent.txt', 0, -1)).rejects.toThrow(NotFoundError)
    })

    it('should accept 200 response (some servers return 200 instead of 206)', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: new Uint8Array([0, 1, 2]),
      }))

      const result = await backend.readRange('test.bin', 0, 3)

      expect(result.length).toBe(3)
    })
  })

  // ===========================================================================
  // stat() Tests
  // ===========================================================================

  describe('stat(path)', () => {
    it('should make HEAD request', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        headers: {
          'Content-Length': '1024',
          'Last-Modified': 'Mon, 01 Jan 2024 12:00:00 GMT',
          'ETag': '"abc123"',
          'Content-Type': 'application/octet-stream',
        },
      }))

      await backend.stat('test.bin')

      expect(mockFetch).toHaveBeenCalledWith(
        'https://parque.db/db/test/dataset/test.bin',
        expect.objectContaining({
          method: 'HEAD',
        })
      )
    })

    it('should return correct file info from headers', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        headers: {
          'Content-Length': '2048',
          'Last-Modified': 'Tue, 15 Feb 2024 10:30:00 GMT',
          'ETag': '"def456"',
          'Content-Type': 'application/parquet',
        },
      }))

      const stat = await backend.stat('data.parquet')

      expect(stat).not.toBeNull()
      expect(stat!.path).toBe('data.parquet')
      expect(stat!.size).toBe(2048)
      expect(stat!.isDirectory).toBe(false)
      expect(stat!.etag).toBe('"def456"')
      expect(stat!.contentType).toBe('application/parquet')
      expect(stat!.mtime).toBeInstanceOf(Date)
    })

    it('should return null for 404', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 404,
        ok: false,
      }))

      const stat = await backend.stat('nonexistent.txt')

      expect(stat).toBeNull()
    })

    it('should throw on 401/403', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 401,
        ok: false,
      }))

      await expect(backend.stat('private.txt')).rejects.toThrow(PermissionDeniedError)
    })

    it('should cache stat results', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        headers: {
          'Content-Length': '1000',
          'Last-Modified': 'Mon, 01 Jan 2024 00:00:00 GMT',
        },
      }))

      // First call
      await backend.stat('cached.txt')
      // Second call should use cache
      await backend.stat('cached.txt')

      // Only one fetch call should be made
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should clear cache with clearCache()', async () => {
      mockFetch.mockResolvedValue(createMockResponse({
        status: 200,
        headers: {
          'Content-Length': '1000',
          'Last-Modified': 'Mon, 01 Jan 2024 00:00:00 GMT',
        },
      }))

      await backend.stat('cached.txt')
      backend.clearCache()
      await backend.stat('cached.txt')

      expect(mockFetch).toHaveBeenCalledTimes(2)
    })
  })

  // ===========================================================================
  // exists() Tests
  // ===========================================================================

  describe('exists(path)', () => {
    it('should return true for 200', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        headers: {
          'Content-Length': '100',
          'Last-Modified': 'Mon, 01 Jan 2024 00:00:00 GMT',
        },
      }))

      const exists = await backend.exists('existing.txt')

      expect(exists).toBe(true)
    })

    it('should return false for 404', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 404,
        ok: false,
      }))

      const exists = await backend.exists('nonexistent.txt')

      expect(exists).toBe(false)
    })
  })

  // ===========================================================================
  // list() Tests
  // ===========================================================================

  describe('list(prefix, options)', () => {
    it('should fetch _meta/manifest.json', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: {
          files: {
            'data/posts/data.parquet': { path: 'data/posts/data.parquet' },
            'data/users/data.parquet': { path: 'data/users/data.parquet' },
          },
        },
      }))

      await backend.list('data/')

      expect(mockFetch).toHaveBeenCalledWith(
        'https://parque.db/db/test/dataset/_meta/manifest.json',
        expect.anything()
      )
    })

    it('should filter files by prefix', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: {
          files: {
            'data/posts/data.parquet': { path: 'data/posts/data.parquet' },
            'data/users/data.parquet': { path: 'data/users/data.parquet' },
            'rels/forward/posts.parquet': { path: 'rels/forward/posts.parquet' },
          },
        },
      }))

      const result = await backend.list('data/')

      expect(result.files).toHaveLength(2)
      expect(result.files).toContain('data/posts/data.parquet')
      expect(result.files).toContain('data/users/data.parquet')
      expect(result.hasMore).toBe(false)
    })

    it('should apply limit option', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: {
          files: {
            'a.txt': { path: 'a.txt' },
            'b.txt': { path: 'b.txt' },
            'c.txt': { path: 'c.txt' },
          },
        },
      }))

      const result = await backend.list('', { limit: 2 })

      expect(result.files).toHaveLength(2)
      expect(result.hasMore).toBe(true)
    })

    it('should return empty list when manifest not found', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 404,
        ok: false,
      }))

      const result = await backend.list('data/')

      expect(result.files).toEqual([])
      expect(result.hasMore).toBe(false)
    })

    it('should return empty list on fetch error', async () => {
      mockFetch.mockRejectedValueOnce(new Error('Network error'))

      const result = await backend.list('data/')

      expect(result.files).toEqual([])
      expect(result.hasMore).toBe(false)
    })
  })

  // ===========================================================================
  // Write Operations (Not Supported)
  // ===========================================================================

  describe('write operations (not supported)', () => {
    it('write() should throw', async () => {
      await expect(backend.write('test.txt', new Uint8Array([1, 2, 3]))).rejects.toThrow(
        'RemoteBackend is read-only'
      )
    })

    it('writeAtomic() should throw', async () => {
      await expect(backend.writeAtomic('test.txt', new Uint8Array([1, 2, 3]))).rejects.toThrow(
        'RemoteBackend is read-only'
      )
    })

    it('append() should throw', async () => {
      await expect(backend.append('test.txt', new Uint8Array([1, 2, 3]))).rejects.toThrow(
        'RemoteBackend is read-only'
      )
    })

    it('writeConditional() should throw', async () => {
      await expect(
        backend.writeConditional('test.txt', new Uint8Array([1, 2, 3]), null)
      ).rejects.toThrow('RemoteBackend is read-only')
    })
  })

  // ===========================================================================
  // Delete Operations (Not Supported)
  // ===========================================================================

  describe('delete operations (not supported)', () => {
    it('delete() should throw', async () => {
      await expect(backend.delete('test.txt')).rejects.toThrow('RemoteBackend is read-only')
    })

    it('deletePrefix() should throw', async () => {
      await expect(backend.deletePrefix('data/')).rejects.toThrow('RemoteBackend is read-only')
    })
  })

  // ===========================================================================
  // Directory Operations (Not Supported)
  // ===========================================================================

  describe('directory operations (not supported)', () => {
    it('mkdir() should throw', async () => {
      await expect(backend.mkdir('test/dir')).rejects.toThrow('RemoteBackend is read-only')
    })

    it('rmdir() should throw', async () => {
      await expect(backend.rmdir('test/dir')).rejects.toThrow('RemoteBackend is read-only')
    })

    it('copy() should throw', async () => {
      await expect(backend.copy('src.txt', 'dst.txt')).rejects.toThrow('RemoteBackend is read-only')
    })

    it('move() should throw', async () => {
      await expect(backend.move('src.txt', 'dst.txt')).rejects.toThrow('RemoteBackend is read-only')
    })
  })

  // ===========================================================================
  // Authentication
  // ===========================================================================

  describe('authentication', () => {
    it('should include Authorization header when token is provided', async () => {
      const { backend: authBackend, mockFetch: authMockFetch } = createTestBackend({
        token: 'my-secret-token',
      })

      authMockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: new Uint8Array([1, 2, 3]),
      }))

      await authBackend.read('private.txt')

      expect(authMockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: 'Bearer my-secret-token',
          }),
        })
      )
    })

    it('should not include Authorization header when token is not provided', async () => {
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: new Uint8Array([1, 2, 3]),
      }))

      await backend.read('public.txt')

      const callHeaders = mockFetch.mock.calls[0]?.[1]?.headers as Record<string, string>
      expect(callHeaders.Authorization).toBeUndefined()
    })

    it('should allow updating token with setToken()', async () => {
      mockFetch.mockResolvedValue(createMockResponse({
        status: 200,
        body: new Uint8Array([1, 2, 3]),
      }))

      // First request without token
      await backend.read('test.txt')
      const firstCallHeaders = mockFetch.mock.calls[0]?.[1]?.headers as Record<string, string>
      expect(firstCallHeaders.Authorization).toBeUndefined()

      // Set token
      backend.setToken('new-token')

      // Second request with token
      await backend.read('test.txt')
      const secondCallHeaders = mockFetch.mock.calls[1]?.[1]?.headers as Record<string, string>
      expect(secondCallHeaders.Authorization).toBe('Bearer new-token')
    })

    it('should clear cache when token is updated', async () => {
      mockFetch.mockResolvedValue(createMockResponse({
        status: 200,
        headers: {
          'Content-Length': '100',
          'Last-Modified': 'Mon, 01 Jan 2024 00:00:00 GMT',
        },
      }))

      await backend.stat('test.txt')
      await backend.stat('test.txt') // cached
      expect(mockFetch).toHaveBeenCalledTimes(1)

      backend.setToken('new-token')

      await backend.stat('test.txt') // cache cleared
      expect(mockFetch).toHaveBeenCalledTimes(2)
    })
  })

  // ===========================================================================
  // Custom Headers
  // ===========================================================================

  describe('custom headers', () => {
    it('should include custom headers in requests', async () => {
      const { backend: customBackend, mockFetch: customMockFetch } = createTestBackend({
        headers: {
          'X-Custom-Header': 'custom-value',
          'X-Another-Header': 'another-value',
        },
      })

      customMockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: new Uint8Array([1, 2, 3]),
      }))

      await customBackend.read('test.txt')

      expect(customMockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            'X-Custom-Header': 'custom-value',
            'X-Another-Header': 'another-value',
          }),
        })
      )
    })

    it('should merge custom headers with token header', async () => {
      const { backend: customBackend, mockFetch: customMockFetch } = createTestBackend({
        token: 'my-token',
        headers: {
          'X-Custom-Header': 'custom-value',
        },
      })

      customMockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: new Uint8Array([1, 2, 3]),
      }))

      await customBackend.read('test.txt')

      expect(customMockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            'X-Custom-Header': 'custom-value',
            Authorization: 'Bearer my-token',
          }),
        })
      )
    })
  })

  // ===========================================================================
  // Timeout
  // ===========================================================================

  describe('timeout', () => {
    it('should use default timeout of 30000ms', async () => {
      // We can't easily test the actual timeout behavior, but we can verify
      // that an AbortSignal is passed to fetch
      mockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: new Uint8Array([1, 2, 3]),
      }))

      await backend.read('test.txt')

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          signal: expect.any(AbortSignal),
        })
      )
    })

    it('should throw NetworkError on timeout', async () => {
      const { backend: timeoutBackend, mockFetch: timeoutMockFetch } = createTestBackend({
        timeout: 100, // 100ms timeout
      })

      // Simulate timeout by rejecting with AbortError
      const abortError = new Error('The operation was aborted')
      abortError.name = 'AbortError'
      timeoutMockFetch.mockRejectedValueOnce(abortError)

      try {
        await timeoutBackend.read('slow.txt')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(NetworkError)
        expect((error as NetworkError).message).toContain('timeout')
      }
    })

    it('should use custom timeout when provided', async () => {
      const { backend: customTimeoutBackend, mockFetch: customTimeoutMockFetch } = createTestBackend(
        {
          timeout: 5000,
        }
      )

      const abortError = new Error('The operation was aborted')
      abortError.name = 'AbortError'
      customTimeoutMockFetch.mockRejectedValueOnce(abortError)

      try {
        await customTimeoutBackend.read('test.txt')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(NetworkError)
        expect((error as NetworkError).message).toContain('5000ms')
      }
    })
  })

  // ===========================================================================
  // Network Errors
  // ===========================================================================

  describe('network errors', () => {
    it('should wrap fetch errors in NetworkError', async () => {
      mockFetch.mockRejectedValueOnce(new Error('Failed to fetch'))

      await expect(backend.read('test.txt')).rejects.toThrow(NetworkError)
    })

    it('should preserve error message in NetworkError', async () => {
      mockFetch.mockRejectedValueOnce(new Error('DNS resolution failed'))

      try {
        await backend.read('test.txt')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(NetworkError)
        expect((error as NetworkError).message).toBe('DNS resolution failed')
      }
    })

    it('should handle non-Error thrown values', async () => {
      mockFetch.mockRejectedValueOnce('string error')

      try {
        await backend.read('test.txt')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(NetworkError)
        expect((error as NetworkError).message).toBe('Network error')
      }
    })
  })

  // ===========================================================================
  // Factory Function
  // ===========================================================================

  describe('createRemoteBackend()', () => {
    it('should create backend with correct baseUrl', () => {
      const factoryMockFetch = createMockFetch()
      const factoryBackend = createRemoteBackend('username/my-dataset', {
        fetch: factoryMockFetch,
      })

      factoryMockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: new Uint8Array([1, 2, 3]),
      }))

      factoryBackend.read('test.txt')

      expect(factoryMockFetch).toHaveBeenCalledWith(
        'https://parque.db/db/username/my-dataset/test.txt',
        expect.anything()
      )
    })

    it('should pass through other options', () => {
      const factoryMockFetch = createMockFetch()
      const factoryBackend = createRemoteBackend('username/my-dataset', {
        token: 'my-token',
        headers: { 'X-Custom': 'value' },
        timeout: 5000,
        fetch: factoryMockFetch,
      })

      factoryMockFetch.mockResolvedValueOnce(createMockResponse({
        status: 200,
        body: new Uint8Array([1, 2, 3]),
      }))

      factoryBackend.read('test.txt')

      expect(factoryMockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: 'Bearer my-token',
            'X-Custom': 'value',
          }),
        })
      )
    })
  })
})
