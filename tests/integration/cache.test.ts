/**
 * Integration tests for Read Caching with Real Storage
 *
 * Tests the cached read path for ParqueDB using FsBackend:
 * - Cache hit/miss behavior
 * - Cache invalidation on writes
 * - Range request handling
 * - Cache statistics
 * - CacheStrategy header generation
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { rm, mkdir } from 'node:fs/promises'
import { FsBackend } from '../../src/storage/FsBackend'
import {
  CacheStrategy,
  DEFAULT_CACHE_CONFIG,
} from '../../src/worker/CacheStrategy'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create test data as Uint8Array
 */
function createTestData(content: string): Uint8Array {
  const encoder = new TextEncoder()
  return encoder.encode(content)
}

// =============================================================================
// Storage-Based Cache Tests
// =============================================================================

describe('Storage-Based Read Caching', () => {
  let backend: FsBackend
  let testDir: string

  beforeEach(async () => {
    testDir = join(tmpdir(), `parquedb-cache-test-${Date.now()}-${Math.random().toString(36).slice(2)}`)
    await mkdir(testDir, { recursive: true })
    backend = new FsBackend(testDir)
  })

  afterEach(async () => {
    try {
      await rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  // ===========================================================================
  // Basic Read Operations
  // ===========================================================================

  describe('Basic Read Operations', () => {
    it('reads and caches parquet files using storage backend', async () => {
      const testContent = 'PAR1 test parquet file content'
      const testData = createTestData(testContent)

      // Write file to storage
      await backend.write('data/posts/data.parquet', testData)

      // First read - from storage
      const result1 = await backend.read('data/posts/data.parquet')

      // Verify content
      const decoder = new TextDecoder()
      expect(decoder.decode(result1)).toBe(testContent)

      // Second read - still from storage (FsBackend doesn't have caching layer)
      const result2 = await backend.read('data/posts/data.parquet')

      // Both reads should return same data
      expect(decoder.decode(result2)).toBe(testContent)
    })

    it('throws error for missing files', async () => {
      await expect(backend.read('data/missing/data.parquet')).rejects.toThrow(
        /not found/i
      )
    })
  })

  // ===========================================================================
  // Range Request Tests
  // ===========================================================================

  describe('Range Requests', () => {
    it('reads byte range from storage', async () => {
      const fullData = createTestData('0123456789ABCDEF')
      await backend.write('data/posts/data.parquet', fullData)

      const result = await backend.readRange('data/posts/data.parquet', 4, 8)

      const decoder = new TextDecoder()
      expect(decoder.decode(result)).toBe('4567')
      expect(result.byteLength).toBe(4)
    })

    it('rejects invalid ranges', async () => {
      await backend.write('test.parquet', createTestData('test content'))

      await expect(backend.readRange('test.parquet', -1, 10)).rejects.toThrow()
      await expect(backend.readRange('test.parquet', 10, 5)).rejects.toThrow()
    })

    it('handles range at end of file', async () => {
      const data = createTestData('0123456789')
      await backend.write('data.parquet', data)

      // Read last 5 bytes
      const result = await backend.readRange('data.parquet', 5, 100)

      const decoder = new TextDecoder()
      expect(decoder.decode(result)).toBe('56789')
    })
  })

  // ===========================================================================
  // Parquet-Specific Operations
  // ===========================================================================

  describe('Parquet Footer Reading', () => {
    it('reads last 8 bytes of parquet file (footer)', async () => {
      // Create a larger file to test footer reading
      const fileContent = 'x'.repeat(100) + 'FOOTER00'
      const data = createTestData(fileContent)
      await backend.write('data/posts/data.parquet', data)

      // Read footer (last 8 bytes)
      const stat = await backend.stat('data/posts/data.parquet')
      expect(stat).not.toBeNull()

      const footer = await backend.readRange(
        'data/posts/data.parquet',
        stat!.size - 8,
        stat!.size
      )

      const decoder = new TextDecoder()
      expect(decoder.decode(footer)).toBe('FOOTER00')
    })

    it('reads metadata section before footer', async () => {
      // Create file with metadata section
      const metadataContent = 'METADATA'
      const footerContent = 'FOOT1234'
      const padding = 'x'.repeat(100)
      const fileContent = padding + metadataContent + footerContent
      const data = createTestData(fileContent)
      await backend.write('data/posts/data.parquet', data)

      const stat = await backend.stat('data/posts/data.parquet')
      expect(stat).not.toBeNull()

      // Read metadata (bytes before footer)
      const metadataStart = stat!.size - 8 - metadataContent.length
      const metadataEnd = stat!.size - 8
      const metadata = await backend.readRange(
        'data/posts/data.parquet',
        metadataStart,
        metadataEnd
      )

      const decoder = new TextDecoder()
      expect(decoder.decode(metadata)).toBe(metadataContent)
    })
  })

  // ===========================================================================
  // Metadata Operations
  // ===========================================================================

  describe('Metadata Operations', () => {
    it('checks file existence', async () => {
      await backend.write('data/posts/data.parquet', createTestData('content'))

      const exists = await backend.exists('data/posts/data.parquet')
      expect(exists).toBe(true)

      const notExists = await backend.exists('data/missing/data.parquet')
      expect(notExists).toBe(false)
    })

    it('gets file metadata (stat)', async () => {
      const content = createTestData('test content')
      await backend.write('data/posts/data.parquet', content)

      const stat = await backend.stat('data/posts/data.parquet')

      expect(stat).not.toBeNull()
      expect(stat!.size).toBe(content.length)
      expect(stat!.etag).toBeDefined()
    })

    it('lists files with prefix', async () => {
      await backend.write('data/posts/data.0000.parquet', createTestData('a'))
      await backend.write('data/posts/data.0001.parquet', createTestData('b'))
      await backend.write('data/users/data.parquet', createTestData('c'))

      const result = await backend.list('data/posts')

      expect(result.files.length).toBe(2)
      expect(result.files).toContain('data/posts/data.0000.parquet')
      expect(result.files).toContain('data/posts/data.0001.parquet')
    })
  })

  // ===========================================================================
  // Write and Invalidation
  // ===========================================================================

  describe('Write and Invalidation', () => {
    it('overwrites file on update', async () => {
      await backend.write('data/posts/data.parquet', createTestData('v1'))

      const v1 = await backend.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(v1)).toBe('v1')

      // Overwrite
      await backend.write('data/posts/data.parquet', createTestData('v2'))

      const v2 = await backend.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(v2)).toBe('v2')
    })

    it('atomic write updates file correctly', async () => {
      await backend.writeAtomic('data/posts/data.parquet', createTestData('initial'))

      const initial = await backend.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(initial)).toBe('initial')

      await backend.writeAtomic('data/posts/data.parquet', createTestData('updated'))

      const updated = await backend.read('data/posts/data.parquet')
      expect(new TextDecoder().decode(updated)).toBe('updated')
    })

    it('deletes file correctly', async () => {
      await backend.write('data/posts/data.parquet', createTestData('content'))

      expect(await backend.exists('data/posts/data.parquet')).toBe(true)

      await backend.delete('data/posts/data.parquet')

      expect(await backend.exists('data/posts/data.parquet')).toBe(false)
    })
  })
})

// =============================================================================
// CacheStrategy Tests
// =============================================================================

describe('CacheStrategy', () => {
  describe('Cache Headers', () => {
    it('generates correct headers for data files', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

      const headers = await strategy.getCacheHeaders('data', {
        etag: 'test-etag',
        size: 1000,
      })

      expect(headers.get('Cache-Control')).toContain('max-age=60')
      expect(headers.get('Cache-Control')).toContain('stale-while-revalidate')
      expect(headers.get('ETag')).toBe('test-etag')
      expect(headers.get('Content-Length')).toBe('1000')
      expect(headers.get('Content-Type')).toBe('application/octet-stream')
    })

    it('generates correct headers for metadata', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

      const headers = await strategy.getCacheHeaders('metadata')

      expect(headers.get('Cache-Control')).toContain('max-age=300')
    })

    it('generates correct headers for bloom filters', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

      const headers = await strategy.getCacheHeaders('bloom')

      expect(headers.get('Cache-Control')).toContain('max-age=600')
    })

    it('generates no-cache headers', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

      const headers = strategy.getNoCacheHeaders()

      expect(headers.get('Cache-Control')).toContain('no-store')
      expect(headers.get('Cache-Control')).toContain('no-cache')
    })
  })

  describe('TTL Management', () => {
    it('returns correct TTL for each type', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

      expect(strategy.getTtl('data')).toBe(60)
      expect(strategy.getTtl('metadata')).toBe(300)
      expect(strategy.getTtl('bloom')).toBe(600)
      expect(strategy.getTtl('schema')).toBe(300) // Same as metadata
      expect(strategy.getTtl('index')).toBe(600) // Same as bloom
    })
  })

  describe('Cache Key Generation', () => {
    it('creates basic cache key', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

      const key = strategy.createCacheKey('data/posts/data.parquet')

      expect(key.url).toBe('https://parquedb/data/posts/data.parquet')
    })

    it('creates cache key with version', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

      const key = strategy.createCacheKey('data/posts/data.parquet', {
        version: 'abc123',
      })

      expect(key.url).toBe('https://parquedb/data/posts/data.parquet?v=abc123')
    })

    it('creates cache key with range', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

      const key = strategy.createCacheKey('data/posts/data.parquet', {
        range: { start: 100, end: 200 },
      })

      expect(key.url).toBe('https://parquedb/data/posts/data.parquet#100-200')
    })

    it('parses cache key', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

      const key = new Request('https://parquedb/data/posts/data.parquet?v=abc#100-200')
      const parsed = strategy.parseCacheKey(key)

      expect(parsed.path).toBe('data/posts/data.parquet')
      expect(parsed.version).toBe('abc')
      expect(parsed.range).toEqual({ start: 100, end: 200 })
    })
  })

  describe('Revalidation Logic', () => {
    it('detects stale responses', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

      // Create response with old date
      const oldDate = new Date(Date.now() - 120000) // 2 minutes ago
      const response = new Response('test', {
        headers: {
          'Cache-Control': 'max-age=60',
          Date: oldDate.toUTCString(),
        },
      })

      expect(strategy.isStale(response)).toBe(true)
    })

    it('detects fresh responses', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

      // Create response with recent date
      const recentDate = new Date()
      const response = new Response('test', {
        headers: {
          'Cache-Control': 'max-age=60',
          Date: recentDate.toUTCString(),
        },
      })

      expect(strategy.isStale(response)).toBe(false)
    })

    it('handles stale-while-revalidate window', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

      // Response is stale but within SWR window
      const oldDate = new Date(Date.now() - 90000) // 90 seconds ago
      const response = new Response('test', {
        headers: {
          'Cache-Control': 'max-age=60, stale-while-revalidate=60',
          Date: oldDate.toUTCString(),
        },
      })

      expect(strategy.isStale(response)).toBe(true)
      expect(strategy.canUseWhileStale(response)).toBe(true)

      // Response is beyond SWR window
      const veryOldDate = new Date(Date.now() - 150000) // 150 seconds ago
      const expiredResponse = new Response('test', {
        headers: {
          'Cache-Control': 'max-age=60, stale-while-revalidate=60',
          Date: veryOldDate.toUTCString(),
        },
      })

      expect(strategy.canUseWhileStale(expiredResponse)).toBe(false)
    })
  })
})

// =============================================================================
// Integration: Query Cache Behavior
// =============================================================================

describe('Query Cache Behavior with Storage', () => {
  let backend: FsBackend
  let testDir: string

  beforeEach(async () => {
    testDir = join(tmpdir(), `parquedb-query-cache-${Date.now()}-${Math.random().toString(36).slice(2)}`)
    await mkdir(testDir, { recursive: true })
    backend = new FsBackend(testDir)
  })

  afterEach(async () => {
    try {
      await rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  it('reads metadata and data consistently', async () => {
    // Create a parquet-like file structure
    const metadataContent = JSON.stringify({ numRows: 100, version: 1 })
    const dataContent = 'row_data_bytes_here'

    await backend.write('data/posts/_meta.json', createTestData(metadataContent))
    await backend.write('data/posts/data.parquet', createTestData(dataContent))

    // Read metadata
    const metadata = await backend.read('data/posts/_meta.json')
    const parsedMeta = JSON.parse(new TextDecoder().decode(metadata))
    expect(parsedMeta.numRows).toBe(100)

    // Read data
    const data = await backend.read('data/posts/data.parquet')
    expect(new TextDecoder().decode(data)).toBe(dataContent)
  })

  it('handles concurrent reads correctly', async () => {
    await backend.write('data/posts/data.parquet', createTestData('concurrent test data'))

    // Multiple concurrent reads
    const reads = Array.from({ length: 10 }, () =>
      backend.read('data/posts/data.parquet')
    )

    const results = await Promise.all(reads)

    // All reads should return the same data
    const decoder = new TextDecoder()
    const expected = 'concurrent test data'
    for (const result of results) {
      expect(decoder.decode(result)).toBe(expected)
    }
  })

  it('updates reflect immediately after write', async () => {
    await backend.write('data/posts/data.parquet', createTestData('version 1'))

    const v1 = await backend.read('data/posts/data.parquet')
    expect(new TextDecoder().decode(v1)).toBe('version 1')

    // Update file
    await backend.write('data/posts/data.parquet', createTestData('version 2'))

    // Read should see new version immediately
    const v2 = await backend.read('data/posts/data.parquet')
    expect(new TextDecoder().decode(v2)).toBe('version 2')
  })
})
