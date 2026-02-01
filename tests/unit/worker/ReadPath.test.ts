/**
 * ReadPath Tests
 *
 * Tests for the ReadPath component that provides cached R2 reads for ParqueDB.
 * Implements the read side of CQRS architecture with Cache API support.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  ReadPath,
  NotFoundError,
  ReadError,
  type CacheStats,
  type ReadOptions,
} from '@/worker/ReadPath'
import { DEFAULT_CACHE_CONFIG, type CacheConfig } from '@/worker/CacheStrategy'
import type { R2Bucket, R2Object, R2ObjectBody, R2Objects, R2ListOptions } from '@/storage/types/r2'

// =============================================================================
// Mock R2 Bucket
// =============================================================================

/**
 * Create a mock R2Object for head operations
 */
function createMockR2Object(key: string, size: number, etag: string = 'test-etag'): R2Object {
  return {
    key,
    version: 'v1',
    size,
    etag,
    httpEtag: `"${etag}"`,
    uploaded: new Date(),
    storageClass: 'Standard' as const,
    checksums: {},
    writeHttpMetadata: vi.fn(),
  }
}

/**
 * Create a mock R2ObjectBody for get operations
 */
function createMockR2ObjectBody(
  key: string,
  data: Uint8Array,
  etag: string = 'test-etag'
): R2ObjectBody {
  const arrayBuffer = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)

  // Safely try to parse as JSON, return null if not valid JSON
  function safeJsonParse(str: string): unknown {
    try {
      return JSON.parse(str)
    } catch {
      return null
    }
  }

  return {
    key,
    version: 'v1',
    size: data.byteLength,
    etag,
    httpEtag: `"${etag}"`,
    uploaded: new Date(),
    storageClass: 'Standard' as const,
    checksums: {},
    writeHttpMetadata: vi.fn(),
    body: new ReadableStream({
      start(controller) {
        controller.enqueue(data)
        controller.close()
      },
    }),
    bodyUsed: false,
    arrayBuffer: vi.fn().mockResolvedValue(arrayBuffer),
    text: vi.fn().mockResolvedValue(new TextDecoder().decode(data)),
    json: vi.fn().mockResolvedValue(safeJsonParse(new TextDecoder().decode(data))),
    blob: vi.fn().mockResolvedValue(new Blob([data])),
  }
}

/**
 * Create a mock R2 bucket
 */
function createMockR2Bucket(
  files: Map<string, Uint8Array> = new Map()
): R2Bucket & { _files: Map<string, Uint8Array> } {
  return {
    _files: files,

    async get(key: string, options?: { range?: { offset: number; length: number } }): Promise<R2ObjectBody | null> {
      const data = files.get(key)
      if (!data) return null

      let resultData = data
      if (options?.range) {
        const { offset, length } = options.range
        resultData = data.slice(offset, offset + length)
      }

      return createMockR2ObjectBody(key, resultData)
    },

    async head(key: string): Promise<R2Object | null> {
      const data = files.get(key)
      if (!data) return null
      return createMockR2Object(key, data.byteLength)
    },

    async put(_key: string, _value: unknown): Promise<R2Object | null> {
      throw new Error('put not implemented in mock')
    },

    async delete(_keys: string | string[]): Promise<void> {
      throw new Error('delete not implemented in mock')
    },

    async list(options?: R2ListOptions): Promise<R2Objects> {
      const prefix = options?.prefix || ''
      const objects: R2Object[] = []

      for (const [key, data] of files.entries()) {
        if (key.startsWith(prefix)) {
          objects.push(createMockR2Object(key, data.byteLength))
        }
      }

      return {
        objects,
        truncated: false,
        delimitedPrefixes: [],
      }
    },

    createMultipartUpload: vi.fn(),
    resumeMultipartUpload: vi.fn(),
  }
}

// =============================================================================
// Mock Cache API
// =============================================================================

/**
 * Create a mock Cache API
 */
function createMockCache(): Cache & { _store: Map<string, Response> } {
  const store = new Map<string, Response>()

  return {
    _store: store,

    async match(request: RequestInfo): Promise<Response | undefined> {
      const url = request instanceof Request ? request.url : request
      const cached = store.get(url)
      // Return a clone so the body can be read multiple times
      return cached?.clone()
    },

    async put(request: RequestInfo, response: Response): Promise<void> {
      const url = request instanceof Request ? request.url : request
      store.set(url, response.clone())
    },

    async delete(request: RequestInfo): Promise<boolean> {
      const url = request instanceof Request ? request.url : request
      return store.delete(url)
    },

    async add(_request: RequestInfo): Promise<void> {
      throw new Error('add not implemented')
    },

    async addAll(_requests: RequestInfo[]): Promise<void> {
      throw new Error('addAll not implemented')
    },

    async keys(): Promise<readonly Request[]> {
      return Array.from(store.keys()).map((url) => new Request(url))
    },

    async matchAll(): Promise<readonly Response[]> {
      return Array.from(store.values())
    },
  }
}

// =============================================================================
// Test Helper Functions
// =============================================================================

/**
 * Create test data as Uint8Array
 */
function createTestData(content: string): Uint8Array {
  return new TextEncoder().encode(content)
}

/**
 * Create a mock cached response with proper headers
 */
function createCachedResponse(data: Uint8Array, options: { maxAge?: number; date?: Date } = {}): Response {
  const { maxAge = 60, date = new Date() } = options
  return new Response(data, {
    headers: {
      'Cache-Control': `max-age=${maxAge}`,
      ETag: 'cached-etag',
      'Content-Length': data.byteLength.toString(),
      Date: date.toUTCString(),
    },
  })
}

// =============================================================================
// Test Suites
// =============================================================================

describe('ReadPath', () => {
  let bucket: ReturnType<typeof createMockR2Bucket>
  let cache: ReturnType<typeof createMockCache>
  let readPath: ReadPath

  beforeEach(() => {
    bucket = createMockR2Bucket()
    cache = createMockCache()
    readPath = new ReadPath(bucket as unknown as R2Bucket, cache as unknown as Cache)
  })

  afterEach(() => {
    vi.clearAllMocks()
  })

  // ===========================================================================
  // Error Classes Tests
  // ===========================================================================

  describe('Error Classes', () => {
    describe('NotFoundError', () => {
      it('should create error with path', () => {
        const error = new NotFoundError('data/posts/data.parquet')

        expect(error).toBeInstanceOf(Error)
        expect(error.name).toBe('NotFoundError')
        expect(error.path).toBe('data/posts/data.parquet')
        expect(error.message).toBe('Object not found: data/posts/data.parquet')
      })

      it('should be catchable as Error', () => {
        const error = new NotFoundError('test/path')

        expect(error instanceof Error).toBe(true)
        expect(error instanceof NotFoundError).toBe(true)
      })
    })

    describe('ReadError', () => {
      it('should create error with message and path', () => {
        const error = new ReadError('Invalid range', 'data/file.parquet')

        expect(error).toBeInstanceOf(Error)
        expect(error.name).toBe('ReadError')
        expect(error.path).toBe('data/file.parquet')
        expect(error.message).toBe('Invalid range')
        expect(error.cause).toBeUndefined()
      })

      it('should create error with cause', () => {
        const cause = new Error('Original error')
        const error = new ReadError('Read failed', 'data/file.parquet', cause)

        expect(error.cause).toBe(cause)
      })
    })
  })

  // ===========================================================================
  // readParquet Tests
  // ===========================================================================

  describe('readParquet', () => {
    it('should read file from R2 when not cached', async () => {
      const testData = createTestData('test parquet content')
      bucket._files.set('data/posts/data.parquet', testData)

      const result = await readPath.readParquet('data/posts/data.parquet')

      expect(result).toEqual(testData)
    })

    it('should return cached data on second read', async () => {
      const testData = createTestData('cached parquet content')
      bucket._files.set('data/posts/data.parquet', testData)

      // First read - populates cache
      await readPath.readParquet('data/posts/data.parquet')

      // Second read - should use cache
      const result = await readPath.readParquet('data/posts/data.parquet')

      expect(result).toEqual(testData)
    })

    it('should throw NotFoundError for missing file', async () => {
      await expect(readPath.readParquet('nonexistent/file.parquet')).rejects.toThrow(NotFoundError)
      await expect(readPath.readParquet('nonexistent/file.parquet')).rejects.toThrow(
        'Object not found: nonexistent/file.parquet'
      )
    })

    it('should skip cache when skipCache option is true', async () => {
      const testData = createTestData('test data')
      bucket._files.set('data/posts/data.parquet', testData)

      // Populate cache with stale data
      const staleData = createTestData('stale cached data')
      await cache.put(
        new Request('https://parquedb/data/posts/data.parquet'),
        createCachedResponse(staleData)
      )

      // Read with skipCache
      const result = await readPath.readParquet('data/posts/data.parquet', { skipCache: true })

      expect(result).toEqual(testData)
    })

    it('should update stats on cache hit', async () => {
      const testData = createTestData('test data')
      bucket._files.set('test.parquet', testData)

      // First read - cache miss
      await readPath.readParquet('test.parquet')

      // Second read - cache hit
      await readPath.readParquet('test.parquet')

      const stats = readPath.getStats()
      expect(stats.hits).toBe(1)
      expect(stats.misses).toBe(1)
    })

    it('should update stats on cache miss', async () => {
      const testData = createTestData('test data')
      bucket._files.set('test.parquet', testData)

      await readPath.readParquet('test.parquet')

      const stats = readPath.getStats()
      expect(stats.misses).toBe(1)
      expect(stats.fetchedBytes).toBe(testData.byteLength)
    })

    it('should use custom TTL when provided', async () => {
      const testData = createTestData('test data')
      bucket._files.set('test.parquet', testData)

      const cacheSpy = vi.spyOn(cache, 'put')

      await readPath.readParquet('test.parquet', { ttl: 120 })

      expect(cacheSpy).toHaveBeenCalled()
      const [, response] = cacheSpy.mock.calls[0]
      expect(response.headers.get('Cache-Control')).toContain('max-age=120')
    })

    it('should use different TTL for different content types', async () => {
      const testData = createTestData('test data')
      bucket._files.set('test.parquet', testData)

      const cacheSpy = vi.spyOn(cache, 'put')

      // Test with metadata type
      await readPath.readParquet('test.parquet', { type: 'metadata' })

      expect(cacheSpy).toHaveBeenCalled()
      const [, response] = cacheSpy.mock.calls[0]
      expect(response.headers.get('Cache-Control')).toContain('max-age=300')
    })

    it('should trigger background revalidation when cache is stale', async () => {
      const testData = createTestData('test data')
      bucket._files.set('test.parquet', testData)

      // Pre-populate cache with stale response
      const staleDate = new Date(Date.now() - 100000) // 100 seconds ago
      await cache.put(
        new Request('https://parquedb/test.parquet'),
        new Response(testData, {
          headers: {
            'Cache-Control': 'max-age=60, stale-while-revalidate=60',
            Date: staleDate.toUTCString(),
            ETag: 'old-etag',
          },
        })
      )

      const bucketGetSpy = vi.spyOn(bucket, 'get')

      // Read should return cached data
      const result = await readPath.readParquet('test.parquet')

      expect(result).toEqual(testData)

      // Wait a bit for background revalidation
      await new Promise((resolve) => setTimeout(resolve, 50))

      // Background revalidation should have triggered
      expect(bucketGetSpy).toHaveBeenCalled()
    })
  })

  // ===========================================================================
  // readRange Tests
  // ===========================================================================

  describe('readRange', () => {
    it('should read byte range from file', async () => {
      const testData = createTestData('0123456789ABCDEF')
      bucket._files.set('data.parquet', testData)

      const result = await readPath.readRange('data.parquet', 4, 8)

      expect(result).toEqual(createTestData('4567'))
      expect(result.byteLength).toBe(4)
    })

    it('should throw NotFoundError for missing file', async () => {
      await expect(readPath.readRange('nonexistent.parquet', 0, 10)).rejects.toThrow(NotFoundError)
    })

    it('should throw ReadError for invalid range (negative start)', async () => {
      bucket._files.set('data.parquet', createTestData('test'))

      await expect(readPath.readRange('data.parquet', -1, 10)).rejects.toThrow(ReadError)
      await expect(readPath.readRange('data.parquet', -1, 10)).rejects.toThrow('Invalid range: -1-10')
    })

    it('should throw ReadError when end <= start', async () => {
      bucket._files.set('data.parquet', createTestData('test'))

      await expect(readPath.readRange('data.parquet', 10, 5)).rejects.toThrow(ReadError)
      await expect(readPath.readRange('data.parquet', 10, 10)).rejects.toThrow(ReadError)
    })

    it('should update fetchedBytes stat', async () => {
      bucket._files.set('data.parquet', createTestData('0123456789'))

      await readPath.readRange('data.parquet', 0, 5)

      const stats = readPath.getStats()
      expect(stats.fetchedBytes).toBe(5)
    })
  })

  // ===========================================================================
  // readRangeCached Tests
  // ===========================================================================

  describe('readRangeCached', () => {
    it('should read range and cache with suffix key', async () => {
      const testData = createTestData('0123456789ABCDEF')
      bucket._files.set('data.parquet', testData)

      const result = await readPath.readRangeCached('data.parquet', 0, 4, 'header')

      expect(result).toEqual(createTestData('0123'))
    })

    it('should return cached range on second read', async () => {
      const testData = createTestData('0123456789ABCDEF')
      bucket._files.set('data.parquet', testData)

      // First read - populates cache
      await readPath.readRangeCached('data.parquet', 8, 16, 'footer')

      const bucketGetSpy = vi.spyOn(bucket, 'get')

      // Second read - should use cache
      const result = await readPath.readRangeCached('data.parquet', 8, 16, 'footer')

      expect(result).toEqual(createTestData('89ABCDEF'))
      expect(bucketGetSpy).not.toHaveBeenCalled()
    })

    it('should use different cache keys for different suffixes', async () => {
      const testData = createTestData('0123456789ABCDEF')
      bucket._files.set('data.parquet', testData)

      // Cache header range
      await readPath.readRangeCached('data.parquet', 0, 4, 'header')

      // Cache footer range
      await readPath.readRangeCached('data.parquet', 12, 16, 'footer')

      // Both should be in cache
      const headerCacheKey = new Request('https://parquedb/data.parquet#header')
      const footerCacheKey = new Request('https://parquedb/data.parquet#footer')

      expect(await cache.match(headerCacheKey)).toBeDefined()
      expect(await cache.match(footerCacheKey)).toBeDefined()
    })

    it('should update cache stats correctly', async () => {
      const testData = createTestData('0123456789ABCDEF')
      bucket._files.set('data.parquet', testData)

      // First read - miss
      await readPath.readRangeCached('data.parquet', 0, 4, 'test')

      // Second read - hit
      await readPath.readRangeCached('data.parquet', 0, 4, 'test')

      const stats = readPath.getStats()
      expect(stats.misses).toBe(1)
      expect(stats.hits).toBe(1)
    })
  })

  // ===========================================================================
  // readParquetFooter Tests
  // ===========================================================================

  describe('readParquetFooter', () => {
    it('should read last 8 bytes of file', async () => {
      const content = 'x'.repeat(100) + 'FOOTER00'
      const testData = createTestData(content)
      bucket._files.set('data.parquet', testData)

      const footer = await readPath.readParquetFooter('data.parquet')

      expect(new TextDecoder().decode(footer)).toBe('FOOTER00')
      expect(footer.byteLength).toBe(8)
    })

    it('should throw NotFoundError for missing file', async () => {
      await expect(readPath.readParquetFooter('nonexistent.parquet')).rejects.toThrow(NotFoundError)
    })

    it('should cache footer read', async () => {
      const content = 'x'.repeat(100) + 'FOOTER00'
      bucket._files.set('data.parquet', createTestData(content))

      // First read
      await readPath.readParquetFooter('data.parquet')

      const bucketGetSpy = vi.spyOn(bucket, 'get')

      // Second read should use cache
      await readPath.readParquetFooter('data.parquet')

      expect(bucketGetSpy).not.toHaveBeenCalled()
    })
  })

  // ===========================================================================
  // readParquetMetadata Tests
  // ===========================================================================

  describe('readParquetMetadata', () => {
    it('should read metadata section before footer', async () => {
      const metadata = 'METADATA'
      const footer = 'FOOT1234'
      const padding = 'x'.repeat(100)
      const content = padding + metadata + footer
      bucket._files.set('data.parquet', createTestData(content))

      const result = await readPath.readParquetMetadata('data.parquet', metadata.length)

      expect(new TextDecoder().decode(result)).toBe(metadata)
    })

    it('should throw NotFoundError for missing file', async () => {
      await expect(readPath.readParquetMetadata('nonexistent.parquet', 100)).rejects.toThrow(
        NotFoundError
      )
    })

    it('should cache metadata read', async () => {
      const content = 'x'.repeat(100) + 'METADATA' + 'FOOT1234'
      bucket._files.set('data.parquet', createTestData(content))

      // First read
      await readPath.readParquetMetadata('data.parquet', 8)

      const bucketGetSpy = vi.spyOn(bucket, 'get')

      // Second read should use cache
      await readPath.readParquetMetadata('data.parquet', 8)

      expect(bucketGetSpy).not.toHaveBeenCalled()
    })
  })

  // ===========================================================================
  // Cache Invalidation Tests
  // ===========================================================================

  describe('invalidate', () => {
    it('should invalidate cache for specified paths', async () => {
      const testData = createTestData('test data')
      bucket._files.set('data/posts/data.parquet', testData)

      // Populate cache
      await readPath.readParquet('data/posts/data.parquet')

      // Invalidate
      await readPath.invalidate(['data/posts/data.parquet'])

      // Cache should be empty
      const cacheKey = new Request('https://parquedb/data/posts/data.parquet')
      expect(await cache.match(cacheKey)).toBeUndefined()
    })

    it('should invalidate multiple paths', async () => {
      bucket._files.set('file1.parquet', createTestData('data1'))
      bucket._files.set('file2.parquet', createTestData('data2'))

      await readPath.readParquet('file1.parquet')
      await readPath.readParquet('file2.parquet')

      await readPath.invalidate(['file1.parquet', 'file2.parquet'])

      expect(await cache.match(new Request('https://parquedb/file1.parquet'))).toBeUndefined()
      expect(await cache.match(new Request('https://parquedb/file2.parquet'))).toBeUndefined()
    })

    it('should invalidate cached ranges (footer, metadata)', async () => {
      const content = 'x'.repeat(100) + 'METADATA' + 'FOOT1234'
      bucket._files.set('data.parquet', createTestData(content))

      // Populate caches
      await readPath.readParquetFooter('data.parquet')
      await readPath.readParquetMetadata('data.parquet', 8)

      // Invalidate
      await readPath.invalidate(['data.parquet'])

      // All cached ranges should be invalidated
      expect(await cache.match(new Request('https://parquedb/data.parquet#footer'))).toBeUndefined()
      expect(await cache.match(new Request('https://parquedb/data.parquet#metadata'))).toBeUndefined()
    })
  })

  describe('invalidateNamespace', () => {
    it('should invalidate all known paths for namespace', async () => {
      bucket._files.set('data/posts/data.parquet', createTestData('data'))
      bucket._files.set('indexes/bloom/posts.bloom', createTestData('bloom'))
      bucket._files.set('rels/forward/posts.parquet', createTestData('forward'))
      bucket._files.set('rels/reverse/posts.parquet', createTestData('reverse'))

      // Populate cache for namespace paths
      await readPath.readParquet('data/posts/data.parquet')

      // Invalidate namespace
      await readPath.invalidateNamespace('posts')

      // Verify cache entries are invalidated
      expect(await cache.match(new Request('https://parquedb/data/posts/data.parquet'))).toBeUndefined()
    })
  })

  // ===========================================================================
  // Metadata Operations Tests
  // ===========================================================================

  describe('exists', () => {
    it('should return true for existing file', async () => {
      bucket._files.set('data.parquet', createTestData('test'))

      const result = await readPath.exists('data.parquet')

      expect(result).toBe(true)
    })

    it('should return false for missing file', async () => {
      const result = await readPath.exists('nonexistent.parquet')

      expect(result).toBe(false)
    })
  })

  describe('getMetadata', () => {
    it('should return metadata for existing file', async () => {
      const testData = createTestData('test content')
      bucket._files.set('data.parquet', testData)

      const metadata = await readPath.getMetadata('data.parquet')

      expect(metadata).not.toBeNull()
      expect(metadata!.key).toBe('data.parquet')
      expect(metadata!.size).toBe(testData.byteLength)
    })

    it('should return null for missing file', async () => {
      const metadata = await readPath.getMetadata('nonexistent.parquet')

      expect(metadata).toBeNull()
    })
  })

  describe('list', () => {
    it('should list files with prefix', async () => {
      bucket._files.set('data/posts/data.parquet', createTestData('a'))
      bucket._files.set('data/posts/index.parquet', createTestData('b'))
      bucket._files.set('data/users/data.parquet', createTestData('c'))

      const result = await readPath.list('data/posts')

      expect(result.objects.length).toBe(2)
      expect(result.objects.map((o) => o.key)).toContain('data/posts/data.parquet')
      expect(result.objects.map((o) => o.key)).toContain('data/posts/index.parquet')
    })

    it('should return empty list for no matches', async () => {
      bucket._files.set('other/data.parquet', createTestData('data'))

      const result = await readPath.list('data/')

      expect(result.objects.length).toBe(0)
    })
  })

  // ===========================================================================
  // Stats and Monitoring Tests
  // ===========================================================================

  describe('getStats', () => {
    it('should return initial stats', () => {
      const stats = readPath.getStats()

      expect(stats.hits).toBe(0)
      expect(stats.misses).toBe(0)
      expect(stats.hitRatio).toBe(0)
      expect(stats.cachedBytes).toBe(0)
      expect(stats.fetchedBytes).toBe(0)
    })

    it('should track cache hits and misses', async () => {
      bucket._files.set('test.parquet', createTestData('test data'))

      // Miss
      await readPath.readParquet('test.parquet')

      // Hit
      await readPath.readParquet('test.parquet')

      const stats = readPath.getStats()
      expect(stats.hits).toBe(1)
      expect(stats.misses).toBe(1)
      expect(stats.hitRatio).toBe(0.5)
    })

    it('should track bytes fetched and cached', async () => {
      const testData = createTestData('test data here')
      bucket._files.set('test.parquet', testData)

      // Miss - bytes fetched from R2
      await readPath.readParquet('test.parquet')

      let stats = readPath.getStats()
      expect(stats.fetchedBytes).toBe(testData.byteLength)
      expect(stats.cachedBytes).toBe(0)

      // Hit - bytes served from cache
      await readPath.readParquet('test.parquet')

      stats = readPath.getStats()
      expect(stats.cachedBytes).toBe(testData.byteLength)
    })

    it('should return a copy of stats', () => {
      const stats1 = readPath.getStats()
      const stats2 = readPath.getStats()

      expect(stats1).not.toBe(stats2)
      expect(stats1).toEqual(stats2)
    })
  })

  describe('resetStats', () => {
    it('should reset all stats to zero', async () => {
      bucket._files.set('test.parquet', createTestData('test data'))

      await readPath.readParquet('test.parquet')
      await readPath.readParquet('test.parquet')

      let stats = readPath.getStats()
      expect(stats.hits).toBeGreaterThan(0)
      expect(stats.misses).toBeGreaterThan(0)

      readPath.resetStats()

      stats = readPath.getStats()
      expect(stats.hits).toBe(0)
      expect(stats.misses).toBe(0)
      expect(stats.hitRatio).toBe(0)
      expect(stats.cachedBytes).toBe(0)
      expect(stats.fetchedBytes).toBe(0)
    })
  })

  // ===========================================================================
  // Configuration Tests
  // ===========================================================================

  describe('configuration', () => {
    it('should use default config when none provided', async () => {
      const testData = createTestData('test')
      bucket._files.set('test.parquet', testData)

      const cacheSpy = vi.spyOn(cache, 'put')

      await readPath.readParquet('test.parquet')

      expect(cacheSpy).toHaveBeenCalled()
      const [, response] = cacheSpy.mock.calls[0]
      // Default dataTtl is 60
      expect(response.headers.get('Cache-Control')).toContain('max-age=60')
    })

    it('should use custom config', async () => {
      const customConfig: CacheConfig = {
        dataTtl: 120,
        metadataTtl: 600,
        bloomTtl: 1200,
        staleWhileRevalidate: false,
      }

      const customReadPath = new ReadPath(
        bucket as unknown as R2Bucket,
        cache as unknown as Cache,
        customConfig
      )

      const testData = createTestData('test')
      bucket._files.set('custom.parquet', testData)

      const cacheSpy = vi.spyOn(cache, 'put')

      await customReadPath.readParquet('custom.parquet')

      expect(cacheSpy).toHaveBeenCalled()
      const [, response] = cacheSpy.mock.calls[0]
      expect(response.headers.get('Cache-Control')).toContain('max-age=120')
      expect(response.headers.get('Cache-Control')).not.toContain('stale-while-revalidate')
    })

    it('should use correct TTL for metadata type', async () => {
      const testData = createTestData('test')
      bucket._files.set('meta.parquet', testData)

      const cacheSpy = vi.spyOn(cache, 'put')

      await readPath.readParquet('meta.parquet', { type: 'metadata' })

      expect(cacheSpy).toHaveBeenCalled()
      const [, response] = cacheSpy.mock.calls[0]
      // Default metadataTtl is 300
      expect(response.headers.get('Cache-Control')).toContain('max-age=300')
    })

    it('should use correct TTL for bloom type', async () => {
      const testData = createTestData('test')
      bucket._files.set('bloom.parquet', testData)

      const cacheSpy = vi.spyOn(cache, 'put')

      await readPath.readParquet('bloom.parquet', { type: 'bloom' })

      expect(cacheSpy).toHaveBeenCalled()
      const [, response] = cacheSpy.mock.calls[0]
      // Default bloomTtl is 600
      expect(response.headers.get('Cache-Control')).toContain('max-age=600')
    })
  })

  // ===========================================================================
  // Cache Key Generation Tests
  // ===========================================================================

  describe('cache key generation', () => {
    it('should create cache keys with correct prefix', async () => {
      const testData = createTestData('test')
      bucket._files.set('data/posts/data.parquet', testData)

      await readPath.readParquet('data/posts/data.parquet')

      const cacheKey = new Request('https://parquedb/data/posts/data.parquet')
      const cached = await cache.match(cacheKey)

      expect(cached).toBeDefined()
    })

    it('should handle special characters in path', async () => {
      const testData = createTestData('test')
      bucket._files.set('data/my-namespace/file_v2.parquet', testData)

      await readPath.readParquet('data/my-namespace/file_v2.parquet')

      const cacheKey = new Request('https://parquedb/data/my-namespace/file_v2.parquet')
      const cached = await cache.match(cacheKey)

      expect(cached).toBeDefined()
    })
  })

  // ===========================================================================
  // Hit Ratio Calculation Tests
  // ===========================================================================

  describe('hit ratio calculation', () => {
    it('should calculate correct hit ratio', async () => {
      bucket._files.set('file1.parquet', createTestData('data1'))
      bucket._files.set('file2.parquet', createTestData('data2'))

      // 2 misses
      await readPath.readParquet('file1.parquet')
      await readPath.readParquet('file2.parquet')

      // 3 hits
      await readPath.readParquet('file1.parquet')
      await readPath.readParquet('file1.parquet')
      await readPath.readParquet('file2.parquet')

      const stats = readPath.getStats()
      expect(stats.hits).toBe(3)
      expect(stats.misses).toBe(2)
      expect(stats.hitRatio).toBe(0.6) // 3 / 5
    })

    it('should handle zero total requests', () => {
      const stats = readPath.getStats()
      expect(stats.hitRatio).toBe(0)
    })
  })

  // ===========================================================================
  // Stale-While-Revalidate Tests
  // ===========================================================================

  describe('stale-while-revalidate behavior', () => {
    it('should add stale-while-revalidate header when enabled', async () => {
      const testData = createTestData('test')
      bucket._files.set('test.parquet', testData)

      const cacheSpy = vi.spyOn(cache, 'put')

      await readPath.readParquet('test.parquet')

      expect(cacheSpy).toHaveBeenCalled()
      const [, response] = cacheSpy.mock.calls[0]
      expect(response.headers.get('Cache-Control')).toContain('stale-while-revalidate')
    })

    it('should not add stale-while-revalidate header when disabled', async () => {
      const configWithoutSWR: CacheConfig = {
        ...DEFAULT_CACHE_CONFIG,
        staleWhileRevalidate: false,
      }

      const readPathNoSWR = new ReadPath(
        bucket as unknown as R2Bucket,
        cache as unknown as Cache,
        configWithoutSWR
      )

      const testData = createTestData('test')
      bucket._files.set('test.parquet', testData)

      const cacheSpy = vi.spyOn(cache, 'put')

      await readPathNoSWR.readParquet('test.parquet')

      expect(cacheSpy).toHaveBeenCalled()
      const [, response] = cacheSpy.mock.calls[0]
      expect(response.headers.get('Cache-Control')).not.toContain('stale-while-revalidate')
    })
  })
})
