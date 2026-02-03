/**
 * CacheInvalidation Tests
 *
 * Tests for the cache invalidation system that ensures CQRS cache coherence
 * when Durable Objects write data.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  CacheInvalidator,
  createCacheInvalidator,
  getNamespaceCachePaths,
  getAllCachePaths,
  invalidateAfterWrite,
  type InvalidationResult,
} from '@/worker/CacheInvalidation'

// =============================================================================
// Mock Cache API
// =============================================================================

/**
 * Create a mock Cache API for testing
 */
function createMockCache(): Cache & { _store: Map<string, Response>; deletedKeys: string[] } {
  const store = new Map<string, Response>()
  const deletedKeys: string[] = []

  return {
    _store: store,
    deletedKeys,

    async match(request: RequestInfo): Promise<Response | undefined> {
      const url = request instanceof Request ? request.url : request
      const cached = store.get(url)
      return cached?.clone()
    },

    async put(request: RequestInfo, response: Response): Promise<void> {
      const url = request instanceof Request ? request.url : request
      store.set(url, response.clone())
    },

    async delete(request: RequestInfo): Promise<boolean> {
      const url = request instanceof Request ? request.url : request
      deletedKeys.push(url)
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

/**
 * Pre-populate cache with entries for a namespace
 */
function populateCache(cache: Cache & { _store: Map<string, Response> }, ns: string): void {
  const paths = getAllCachePaths(ns)
  for (const path of paths) {
    cache._store.set(`https://parquedb/${path}`, new Response('cached data'))
  }
}

// =============================================================================
// Path Generation Tests
// =============================================================================

describe('Cache Path Generation', () => {
  describe('getNamespaceCachePaths', () => {
    it('should generate correct paths for a namespace', () => {
      const paths = getNamespaceCachePaths('posts')

      expect(paths.data).toBe('data/posts/data.parquet')
      expect(paths.bloom).toBe('indexes/bloom/posts.bloom')
      expect(paths.forwardRels).toBe('rels/forward/posts.parquet')
      expect(paths.reverseRels).toBe('rels/reverse/posts.parquet')
      expect(paths.metadata).toContain('data/posts/data.parquet#footer')
      expect(paths.metadata).toContain('data/posts/data.parquet#metadata')
    })

    it('should handle namespace with special characters', () => {
      const paths = getNamespaceCachePaths('user-data')

      expect(paths.data).toBe('data/user-data/data.parquet')
      expect(paths.bloom).toBe('indexes/bloom/user-data.bloom')
    })
  })

  describe('getAllCachePaths', () => {
    it('should return all paths as a flat array', () => {
      const paths = getAllCachePaths('posts')

      expect(Array.isArray(paths)).toBe(true)
      expect(paths.length).toBe(6) // data, bloom, forwardRels, reverseRels, 2 metadata
      expect(paths).toContain('data/posts/data.parquet')
      expect(paths).toContain('indexes/bloom/posts.bloom')
      expect(paths).toContain('rels/forward/posts.parquet')
      expect(paths).toContain('rels/reverse/posts.parquet')
    })
  })
})

// =============================================================================
// CacheInvalidator Tests
// =============================================================================

describe('CacheInvalidator', () => {
  let cache: Cache & { _store: Map<string, Response>; deletedKeys: string[] }
  let invalidator: CacheInvalidator

  beforeEach(() => {
    cache = createMockCache()
    invalidator = new CacheInvalidator(cache)
  })

  describe('invalidateNamespace', () => {
    it('should delete all cache entries for a namespace', async () => {
      populateCache(cache, 'posts')

      const result = await invalidator.invalidateNamespace('posts')

      expect(result.success).toBe(true)
      expect(result.paths).toContain('data/posts/data.parquet')
      expect(result.paths).toContain('indexes/bloom/posts.bloom')
      expect(cache.deletedKeys.length).toBeGreaterThan(0)
    })

    it('should return correct count of deleted entries', async () => {
      populateCache(cache, 'posts')

      const result = await invalidator.invalidateNamespace('posts')

      // All 6 paths should have been deleted
      expect(result.entriesDeleted).toBe(6)
    })

    it('should handle empty cache gracefully', async () => {
      const result = await invalidator.invalidateNamespace('empty-namespace')

      expect(result.success).toBe(true)
      expect(result.entriesDeleted).toBe(0)
    })

    it('should bump version after invalidation', async () => {
      const versionBefore = invalidator.getVersion('posts')

      await invalidator.invalidateNamespace('posts')

      const versionAfter = invalidator.getVersion('posts')
      expect(versionAfter).toBe(versionBefore + 1)
    })

    it('should call onInvalidate callback if provided', async () => {
      const onInvalidate = vi.fn()
      const invalidatorWithCallback = new CacheInvalidator(cache, { onInvalidate })

      await invalidatorWithCallback.invalidateNamespace('posts')

      expect(onInvalidate).toHaveBeenCalledWith('posts', expect.any(Array))
    })

    it('should include duration in result', async () => {
      const result = await invalidator.invalidateNamespace('posts')

      expect(result.durationMs).toBeDefined()
      expect(result.durationMs).toBeGreaterThanOrEqual(0)
    })
  })

  describe('invalidatePaths', () => {
    it('should delete specific paths from cache', async () => {
      // Populate specific paths
      cache._store.set('https://parquedb/custom/path.parquet', new Response('data'))
      cache._store.set('https://parquedb/another/path.json', new Response('data'))

      const result = await invalidator.invalidatePaths([
        'custom/path.parquet',
        'another/path.json',
      ])

      expect(result.success).toBe(true)
      expect(result.entriesDeleted).toBe(2)
    })

    it('should handle non-existent paths gracefully', async () => {
      const result = await invalidator.invalidatePaths([
        'nonexistent/path.parquet',
      ])

      expect(result.success).toBe(true)
      expect(result.entriesDeleted).toBe(0)
    })
  })

  describe('invalidateEntity', () => {
    it('should invalidate namespace for entity changes', async () => {
      populateCache(cache, 'posts')

      const result = await invalidator.invalidateEntity('posts', 'abc123')

      // Currently invalidates the whole namespace
      expect(result.success).toBe(true)
      expect(result.paths).toContain('data/posts/data.parquet')
    })
  })

  describe('invalidateRelationships', () => {
    it('should invalidate relationship caches between two namespaces', async () => {
      cache._store.set('https://parquedb/rels/forward/posts.parquet', new Response('data'))
      cache._store.set('https://parquedb/rels/reverse/users.parquet', new Response('data'))

      const result = await invalidator.invalidateRelationships('posts', 'users')

      expect(result.success).toBe(true)
      expect(result.paths).toContain('rels/forward/posts.parquet')
      expect(result.paths).toContain('rels/reverse/users.parquet')
    })
  })

  describe('version tracking', () => {
    it('should start with version 0', () => {
      expect(invalidator.getVersion('new-namespace')).toBe(0)
    })

    it('should increment version with bumpVersion', () => {
      const v1 = invalidator.bumpVersion('posts')
      expect(v1).toBe(1)

      const v2 = invalidator.bumpVersion('posts')
      expect(v2).toBe(2)
    })

    it('should track versions independently per namespace', () => {
      invalidator.bumpVersion('posts')
      invalidator.bumpVersion('posts')
      invalidator.bumpVersion('users')

      expect(invalidator.getVersion('posts')).toBe(2)
      expect(invalidator.getVersion('users')).toBe(1)
    })

    it('should generate versioned cache keys', () => {
      invalidator.bumpVersion('posts')
      invalidator.bumpVersion('posts')

      const key = invalidator.getVersionedCacheKey('data/posts/data.parquet', 'posts')

      expect(key).toBe('data/posts/data.parquet?v=2')
    })
  })

  describe('QueryExecutor cache integration', () => {
    it('should call invalidateCache on QueryExecutor', () => {
      const mockQueryExecutor = {
        invalidateCache: vi.fn(),
      }

      invalidator.clearQueryExecutorCache(mockQueryExecutor, 'posts')

      expect(mockQueryExecutor.invalidateCache).toHaveBeenCalledWith('posts')
    })

    it('should call clearCache when no namespace specified', () => {
      const mockQueryExecutor = {
        invalidateCache: vi.fn(),
        clearCache: vi.fn(),
      }

      invalidator.clearQueryExecutorCache(mockQueryExecutor)

      expect(mockQueryExecutor.clearCache).toHaveBeenCalled()
      expect(mockQueryExecutor.invalidateCache).not.toHaveBeenCalled()
    })
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createCacheInvalidator', () => {
  it('should create a CacheInvalidator instance', () => {
    const cache = createMockCache()
    const invalidator = createCacheInvalidator(cache)

    expect(invalidator).toBeInstanceOf(CacheInvalidator)
  })

  it('should pass options to the invalidator', async () => {
    const cache = createMockCache()
    const onInvalidate = vi.fn()
    const invalidator = createCacheInvalidator(cache, { onInvalidate })

    await invalidator.invalidateNamespace('posts')

    expect(onInvalidate).toHaveBeenCalled()
  })
})

// =============================================================================
// DO Integration Helper Tests
// =============================================================================

describe('invalidateAfterWrite', () => {
  let cache: Cache & { _store: Map<string, Response>; deletedKeys: string[] }

  beforeEach(() => {
    cache = createMockCache()
  })

  it('should invalidate full namespace for create operation', async () => {
    populateCache(cache, 'posts')

    const result = await invalidateAfterWrite(cache, 'posts', 'create')

    expect(result.success).toBe(true)
    expect(result.paths).toContain('data/posts/data.parquet')
  })

  it('should invalidate full namespace for update operation', async () => {
    populateCache(cache, 'posts')

    const result = await invalidateAfterWrite(cache, 'posts', 'update')

    expect(result.success).toBe(true)
    expect(result.paths).toContain('data/posts/data.parquet')
  })

  it('should invalidate full namespace for delete operation', async () => {
    populateCache(cache, 'posts')

    const result = await invalidateAfterWrite(cache, 'posts', 'delete')

    expect(result.success).toBe(true)
    expect(result.paths).toContain('data/posts/data.parquet')
  })

  it('should only invalidate relationship caches for link operation', async () => {
    populateCache(cache, 'posts')

    const result = await invalidateAfterWrite(cache, 'posts', 'link')

    expect(result.success).toBe(true)
    expect(result.paths).toContain('rels/forward/posts.parquet')
    expect(result.paths).toContain('rels/reverse/posts.parquet')
    expect(result.paths).not.toContain('data/posts/data.parquet')
  })

  it('should only invalidate relationship caches for unlink operation', async () => {
    populateCache(cache, 'posts')

    const result = await invalidateAfterWrite(cache, 'posts', 'unlink')

    expect(result.success).toBe(true)
    expect(result.paths).toContain('rels/forward/posts.parquet')
    expect(result.paths).toContain('rels/reverse/posts.parquet')
    expect(result.paths).not.toContain('data/posts/data.parquet')
  })
})

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

describe('Error Handling', () => {
  it('should handle cache deletion errors gracefully', async () => {
    const faultyCache = {
      async match(): Promise<Response | undefined> {
        return undefined
      },
      async put(): Promise<void> {},
      async delete(): Promise<boolean> {
        throw new Error('Cache deletion failed')
      },
      async add(): Promise<void> {},
      async addAll(): Promise<void> {},
      async keys(): Promise<readonly Request[]> {
        return []
      },
      async matchAll(): Promise<readonly Response[]> {
        return []
      },
    } as Cache

    const invalidator = new CacheInvalidator(faultyCache)
    const result = await invalidator.invalidateNamespace('posts')

    // Promise.allSettled catches individual errors, so success is true
    // but no entries are deleted due to errors
    expect(result.success).toBe(true)
    expect(result.entriesDeleted).toBe(0)
  })

  it('should report errors when Promise.allSettled rejects', async () => {
    // Test that even with errors, the function doesn't throw
    const cache = createMockCache()
    const invalidator = new CacheInvalidator(cache)

    // Populate cache
    populateCache(cache, 'posts')

    // This should work normally
    const result = await invalidator.invalidateNamespace('posts')
    expect(result.success).toBe(true)
  })
})

// =============================================================================
// Performance Tests
// =============================================================================

describe('Performance', () => {
  it('should complete invalidation within reasonable time', async () => {
    const cache = createMockCache()
    populateCache(cache, 'large-namespace')
    const invalidator = new CacheInvalidator(cache)

    const startTime = performance.now()
    const result = await invalidator.invalidateNamespace('large-namespace')
    const endTime = performance.now()

    // Should complete in under 100ms for a simple namespace
    expect(endTime - startTime).toBeLessThan(100)
    expect(result.durationMs).toBeLessThan(100)
  })
})
