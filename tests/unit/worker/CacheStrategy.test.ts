/**
 * CacheStrategy Tests
 *
 * Tests for the CacheStrategy component that manages cache configuration
 * and header generation for different content types in Worker environments.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  CacheStrategy,
  CacheConfig,
  DEFAULT_CACHE_CONFIG,
  READ_HEAVY_CACHE_CONFIG,
  WRITE_HEAVY_CACHE_CONFIG,
  NO_CACHE_CONFIG,
  createCacheStrategy,
  getContentTypeFromPath,
  type CacheContentType,
  type AdvancedCacheConfig,
} from '@/worker/CacheStrategy'

// =============================================================================
// Test Suites
// =============================================================================

describe('CacheStrategy', () => {
  // ===========================================================================
  // Configuration Tests
  // ===========================================================================

  describe('configuration', () => {
    it('should use default config values', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)

      expect(strategy.getTtl('data')).toBe(60)
      expect(strategy.getTtl('metadata')).toBe(300)
      expect(strategy.getTtl('bloom')).toBe(600)
    })

    it('should use read-heavy config values', () => {
      const strategy = new CacheStrategy(READ_HEAVY_CACHE_CONFIG)

      expect(strategy.getTtl('data')).toBe(300)
      expect(strategy.getTtl('metadata')).toBe(900)
      expect(strategy.getTtl('bloom')).toBe(1800)
    })

    it('should use write-heavy config values', () => {
      const strategy = new CacheStrategy(WRITE_HEAVY_CACHE_CONFIG)

      expect(strategy.getTtl('data')).toBe(15)
      expect(strategy.getTtl('metadata')).toBe(60)
      expect(strategy.getTtl('bloom')).toBe(120)
    })

    it('should use no-cache config values', () => {
      const strategy = new CacheStrategy(NO_CACHE_CONFIG)

      expect(strategy.getTtl('data')).toBe(0)
      expect(strategy.getTtl('metadata')).toBe(0)
      expect(strategy.getTtl('bloom')).toBe(0)
    })

    it('should accept custom config', () => {
      const customConfig: CacheConfig = {
        dataTtl: 120,
        metadataTtl: 600,
        bloomTtl: 1200,
        staleWhileRevalidate: false,
      }

      const strategy = new CacheStrategy(customConfig)

      expect(strategy.getTtl('data')).toBe(120)
      expect(strategy.getTtl('metadata')).toBe(600)
      expect(strategy.getTtl('bloom')).toBe(1200)
    })

    it('should accept advanced config with overrides', () => {
      const advancedConfig: AdvancedCacheConfig = {
        defaults: DEFAULT_CACHE_CONFIG,
        overrides: {
          data: { ttl: 30 },
          bloom: { ttl: 3600, staleWhileRevalidate: false },
        },
      }

      const strategy = new CacheStrategy(advancedConfig)

      expect(strategy.getTtl('data')).toBe(30)
      expect(strategy.getTtl('metadata')).toBe(300) // Uses default
      expect(strategy.getTtl('bloom')).toBe(3600)
    })
  })

  // ===========================================================================
  // TTL Management Tests
  // ===========================================================================

  describe('getTtl', () => {
    it('should return correct TTL for data type', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      expect(strategy.getTtl('data')).toBe(60)
    })

    it('should return correct TTL for metadata type', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      expect(strategy.getTtl('metadata')).toBe(300)
    })

    it('should return correct TTL for schema type (uses metadata TTL)', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      expect(strategy.getTtl('schema')).toBe(300)
    })

    it('should return correct TTL for bloom type', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      expect(strategy.getTtl('bloom')).toBe(600)
    })

    it('should return correct TTL for index type (uses bloom TTL)', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      expect(strategy.getTtl('index')).toBe(600)
    })

    it('should prefer override TTL when specified', () => {
      const advancedConfig: AdvancedCacheConfig = {
        defaults: DEFAULT_CACHE_CONFIG,
        overrides: {
          index: { ttl: 1800, staleWhileRevalidate: true, priority: 'high', cacheRanges: true },
        },
      }

      const strategy = new CacheStrategy(advancedConfig)
      expect(strategy.getTtl('index')).toBe(1800)
    })
  })

  // ===========================================================================
  // Header Generation Tests
  // ===========================================================================

  describe('getCacheHeaders', () => {
    it('should generate correct Cache-Control header for data', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const headers = await strategy.getCacheHeaders('data')

      expect(headers.get('Cache-Control')).toContain('public')
      expect(headers.get('Cache-Control')).toContain('max-age=60')
      expect(headers.get('Cache-Control')).toContain('stale-while-revalidate=60')
    })

    it('should generate correct Cache-Control header for metadata', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const headers = await strategy.getCacheHeaders('metadata')

      expect(headers.get('Cache-Control')).toContain('max-age=300')
      expect(headers.get('Cache-Control')).toContain('stale-while-revalidate=300')
    })

    it('should omit stale-while-revalidate when disabled', async () => {
      const strategy = new CacheStrategy(WRITE_HEAVY_CACHE_CONFIG)
      const headers = await strategy.getCacheHeaders('data')

      expect(headers.get('Cache-Control')).not.toContain('stale-while-revalidate')
    })

    it('should set correct Content-Type for data', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const headers = await strategy.getCacheHeaders('data')

      expect(headers.get('Content-Type')).toBe('application/octet-stream')
    })

    it('should set correct Content-Type for metadata', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const headers = await strategy.getCacheHeaders('metadata')

      expect(headers.get('Content-Type')).toBe('application/json')
    })

    it('should set correct Content-Type for schema', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const headers = await strategy.getCacheHeaders('schema')

      expect(headers.get('Content-Type')).toBe('application/json')
    })

    it('should set correct Content-Type for bloom', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const headers = await strategy.getCacheHeaders('bloom')

      expect(headers.get('Content-Type')).toBe('application/octet-stream')
    })

    it('should set correct Content-Type for index', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const headers = await strategy.getCacheHeaders('index')

      expect(headers.get('Content-Type')).toBe('application/octet-stream')
    })

    it('should include ETag when provided', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const headers = await strategy.getCacheHeaders('data', { etag: '"abc123"' })

      expect(headers.get('ETag')).toBe('"abc123"')
    })

    it('should include Content-Length when provided', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const headers = await strategy.getCacheHeaders('data', { size: 1024 })

      expect(headers.get('Content-Length')).toBe('1024')
    })

    it('should include custom X-ParqueDB headers', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const headers = await strategy.getCacheHeaders('bloom')

      expect(headers.get('X-ParqueDB-Cache-Type')).toBe('bloom')
      expect(headers.get('X-ParqueDB-Cache-TTL')).toBe('600')
    })
  })

  describe('getNoCacheHeaders', () => {
    it('should generate correct no-cache headers', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const headers = strategy.getNoCacheHeaders()

      expect(headers.get('Cache-Control')).toBe('no-store, no-cache, must-revalidate')
      expect(headers.get('Pragma')).toBe('no-cache')
    })
  })

  // ===========================================================================
  // Revalidation Tests
  // ===========================================================================

  describe('shouldRevalidate', () => {
    it('should return true when no Cache-Control header', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const response = new Response(null, { headers: {} })

      expect(await strategy.shouldRevalidate(response)).toBe(true)
    })

    it('should return true when no max-age in Cache-Control', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const response = new Response(null, {
        headers: { 'Cache-Control': 'public' },
      })

      expect(await strategy.shouldRevalidate(response)).toBe(true)
    })

    it('should return false when response is fresh (age < 80% of max-age)', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const response = new Response(null, {
        headers: {
          'Cache-Control': 'max-age=100',
          Age: '50', // 50% of max-age
        },
      })

      expect(await strategy.shouldRevalidate(response)).toBe(false)
    })

    it('should return true when response is stale (age > 80% of max-age)', async () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const response = new Response(null, {
        headers: {
          'Cache-Control': 'max-age=100',
          Age: '85', // 85% of max-age
        },
      })

      expect(await strategy.shouldRevalidate(response)).toBe(true)
    })
  })

  describe('isStale', () => {
    it('should return true when no Cache-Control header', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const response = new Response(null, { headers: {} })

      expect(strategy.isStale(response)).toBe(true)
    })

    it('should return false when age is less than max-age', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const response = new Response(null, {
        headers: {
          'Cache-Control': 'max-age=100',
          Age: '50',
        },
      })

      expect(strategy.isStale(response)).toBe(false)
    })

    it('should return true when age exceeds max-age', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const response = new Response(null, {
        headers: {
          'Cache-Control': 'max-age=100',
          Age: '150',
        },
      })

      expect(strategy.isStale(response)).toBe(true)
    })
  })

  describe('canUseWhileStale', () => {
    it('should return true when not stale', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const response = new Response(null, {
        headers: {
          'Cache-Control': 'max-age=100',
          Age: '50',
        },
      })

      expect(strategy.canUseWhileStale(response)).toBe(true)
    })

    it('should return false when no stale-while-revalidate', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const response = new Response(null, {
        headers: {
          'Cache-Control': 'max-age=100',
          Age: '150',
        },
      })

      expect(strategy.canUseWhileStale(response)).toBe(false)
    })

    it('should return true when within stale-while-revalidate window', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const response = new Response(null, {
        headers: {
          'Cache-Control': 'max-age=100, stale-while-revalidate=50',
          Age: '120', // Within swr window
        },
      })

      expect(strategy.canUseWhileStale(response)).toBe(true)
    })

    it('should return false when past stale-while-revalidate window', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const response = new Response(null, {
        headers: {
          'Cache-Control': 'max-age=100, stale-while-revalidate=50',
          Age: '200', // Past swr window
        },
      })

      expect(strategy.canUseWhileStale(response)).toBe(false)
    })
  })

  // ===========================================================================
  // Cache Key Generation Tests
  // ===========================================================================

  describe('createCacheKey', () => {
    it('should create basic cache key from path', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const key = strategy.createCacheKey('dataset/data.parquet')

      expect(key.url).toBe('https://parquedb/dataset/data.parquet')
    })

    it('should include version parameter when provided', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const key = strategy.createCacheKey('dataset/data.parquet', { version: 'v2' })

      expect(key.url).toBe('https://parquedb/dataset/data.parquet?v=v2')
    })

    it('should include range hash when provided', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const key = strategy.createCacheKey('dataset/data.parquet', { range: { start: 0, end: 1024 } })

      expect(key.url).toBe('https://parquedb/dataset/data.parquet#0-1024')
    })

    it('should include both version and range', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const key = strategy.createCacheKey('dataset/data.parquet', {
        version: 'v3',
        range: { start: 1000, end: 2000 },
      })

      expect(key.url).toBe('https://parquedb/dataset/data.parquet?v=v3#1000-2000')
    })
  })

  describe('parseCacheKey', () => {
    it('should parse basic path', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const key = new Request('https://parquedb/dataset/data.parquet')

      const parsed = strategy.parseCacheKey(key)

      expect(parsed.path).toBe('dataset/data.parquet')
      expect(parsed.version).toBeUndefined()
      expect(parsed.range).toBeUndefined()
    })

    it('should parse version parameter', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const key = new Request('https://parquedb/dataset/data.parquet?v=v2')

      const parsed = strategy.parseCacheKey(key)

      expect(parsed.path).toBe('dataset/data.parquet')
      expect(parsed.version).toBe('v2')
    })

    it('should parse range hash', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const key = new Request('https://parquedb/dataset/data.parquet#0-1024')

      const parsed = strategy.parseCacheKey(key)

      expect(parsed.path).toBe('dataset/data.parquet')
      expect(parsed.range).toEqual({ start: 0, end: 1024 })
    })

    it('should parse both version and range', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const key = new Request('https://parquedb/dataset/data.parquet?v=v3#1000-2000')

      const parsed = strategy.parseCacheKey(key)

      expect(parsed.path).toBe('dataset/data.parquet')
      expect(parsed.version).toBe('v3')
      expect(parsed.range).toEqual({ start: 1000, end: 2000 })
    })

    it('should handle invalid range gracefully', () => {
      const strategy = new CacheStrategy(DEFAULT_CACHE_CONFIG)
      const key = new Request('https://parquedb/dataset/data.parquet#invalid')

      const parsed = strategy.parseCacheKey(key)

      expect(parsed.path).toBe('dataset/data.parquet')
      expect(parsed.range).toBeUndefined()
    })
  })
})

// =============================================================================
// Utility Function Tests
// =============================================================================

describe('createCacheStrategy', () => {
  it('should use default values when no env provided', () => {
    const strategy = createCacheStrategy()

    expect(strategy.getTtl('data')).toBe(60)
    expect(strategy.getTtl('metadata')).toBe(300)
    expect(strategy.getTtl('bloom')).toBe(600)
  })

  it('should use env values when provided', () => {
    const strategy = createCacheStrategy({
      CACHE_DATA_TTL: '120',
      CACHE_METADATA_TTL: '600',
      CACHE_BLOOM_TTL: '1200',
    })

    expect(strategy.getTtl('data')).toBe(120)
    expect(strategy.getTtl('metadata')).toBe(600)
    expect(strategy.getTtl('bloom')).toBe(1200)
  })

  it('should disable stale-while-revalidate when env is false', async () => {
    const strategy = createCacheStrategy({
      CACHE_STALE_WHILE_REVALIDATE: 'false',
    })

    const headers = await strategy.getCacheHeaders('data')
    expect(headers.get('Cache-Control')).not.toContain('stale-while-revalidate')
  })

  it('should enable stale-while-revalidate by default', async () => {
    const strategy = createCacheStrategy({})

    const headers = await strategy.getCacheHeaders('data')
    expect(headers.get('Cache-Control')).toContain('stale-while-revalidate')
  })

  it('should use default TTL for missing env values', () => {
    const strategy = createCacheStrategy({
      CACHE_DATA_TTL: '90',
      // metadata and bloom TTLs not provided
    })

    expect(strategy.getTtl('data')).toBe(90)
    expect(strategy.getTtl('metadata')).toBe(300) // default
    expect(strategy.getTtl('bloom')).toBe(600) // default
  })
})

describe('getContentTypeFromPath', () => {
  it('should identify data.parquet as data type', () => {
    expect(getContentTypeFromPath('dataset/data.parquet')).toBe('data')
  })

  it('should identify any .parquet file as data type', () => {
    expect(getContentTypeFromPath('dataset/posts.parquet')).toBe('data')
    expect(getContentTypeFromPath('ns/collection.parquet')).toBe('data')
  })

  it('should identify .bloom files as bloom type', () => {
    expect(getContentTypeFromPath('dataset/posts.bloom')).toBe('bloom')
    expect(getContentTypeFromPath('indexes/bloom/ns.bloom')).toBe('bloom')
  })

  it('should identify /indexes/ paths as index type', () => {
    expect(getContentTypeFromPath('dataset/indexes/hash.idx')).toBe('index')
    expect(getContentTypeFromPath('dataset/indexes/secondary/titleType.hash.idx')).toBe('index')
    expect(getContentTypeFromPath('ns/indexes/fts/name.json')).toBe('index')
  })

  it('should identify _meta/ paths as schema type', () => {
    expect(getContentTypeFromPath('dataset/_meta/schema.json')).toBe('schema')
  })

  it('should identify schema paths as schema type', () => {
    expect(getContentTypeFromPath('dataset/schema.json')).toBe('schema')
  })

  it('should default to metadata for unknown paths', () => {
    expect(getContentTypeFromPath('dataset/something.json')).toBe('metadata')
    expect(getContentTypeFromPath('dataset/config.yaml')).toBe('metadata')
  })
})

// =============================================================================
// Preset Configuration Tests
// =============================================================================

describe('preset configurations', () => {
  describe('DEFAULT_CACHE_CONFIG', () => {
    it('should have balanced TTL values', () => {
      expect(DEFAULT_CACHE_CONFIG.dataTtl).toBe(60)
      expect(DEFAULT_CACHE_CONFIG.metadataTtl).toBe(300)
      expect(DEFAULT_CACHE_CONFIG.bloomTtl).toBe(600)
      expect(DEFAULT_CACHE_CONFIG.staleWhileRevalidate).toBe(true)
    })
  })

  describe('READ_HEAVY_CACHE_CONFIG', () => {
    it('should have longer TTL values for read-heavy workloads', () => {
      expect(READ_HEAVY_CACHE_CONFIG.dataTtl).toBe(300)
      expect(READ_HEAVY_CACHE_CONFIG.metadataTtl).toBe(900)
      expect(READ_HEAVY_CACHE_CONFIG.bloomTtl).toBe(1800)
      expect(READ_HEAVY_CACHE_CONFIG.staleWhileRevalidate).toBe(true)
    })

    it('should have higher TTL than default', () => {
      expect(READ_HEAVY_CACHE_CONFIG.dataTtl).toBeGreaterThan(DEFAULT_CACHE_CONFIG.dataTtl)
      expect(READ_HEAVY_CACHE_CONFIG.metadataTtl).toBeGreaterThan(DEFAULT_CACHE_CONFIG.metadataTtl)
      expect(READ_HEAVY_CACHE_CONFIG.bloomTtl).toBeGreaterThan(DEFAULT_CACHE_CONFIG.bloomTtl)
    })
  })

  describe('WRITE_HEAVY_CACHE_CONFIG', () => {
    it('should have shorter TTL values for write-heavy workloads', () => {
      expect(WRITE_HEAVY_CACHE_CONFIG.dataTtl).toBe(15)
      expect(WRITE_HEAVY_CACHE_CONFIG.metadataTtl).toBe(60)
      expect(WRITE_HEAVY_CACHE_CONFIG.bloomTtl).toBe(120)
      expect(WRITE_HEAVY_CACHE_CONFIG.staleWhileRevalidate).toBe(false)
    })

    it('should have lower TTL than default', () => {
      expect(WRITE_HEAVY_CACHE_CONFIG.dataTtl).toBeLessThan(DEFAULT_CACHE_CONFIG.dataTtl)
      expect(WRITE_HEAVY_CACHE_CONFIG.metadataTtl).toBeLessThan(DEFAULT_CACHE_CONFIG.metadataTtl)
      expect(WRITE_HEAVY_CACHE_CONFIG.bloomTtl).toBeLessThan(DEFAULT_CACHE_CONFIG.bloomTtl)
    })

    it('should disable stale-while-revalidate', () => {
      expect(WRITE_HEAVY_CACHE_CONFIG.staleWhileRevalidate).toBe(false)
    })
  })

  describe('NO_CACHE_CONFIG', () => {
    it('should have zero TTL values', () => {
      expect(NO_CACHE_CONFIG.dataTtl).toBe(0)
      expect(NO_CACHE_CONFIG.metadataTtl).toBe(0)
      expect(NO_CACHE_CONFIG.bloomTtl).toBe(0)
      expect(NO_CACHE_CONFIG.staleWhileRevalidate).toBe(false)
    })
  })
})
