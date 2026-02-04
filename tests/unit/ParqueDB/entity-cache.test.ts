/**
 * Entity Cache Tests
 *
 * Tests for the LRU-based entity cache with configurable size limits.
 * Verifies that entities are properly evicted when the cache exceeds its limits.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueDB } from '../../../src/ParqueDB'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  getEntityStore,
  configureEntityStore,
  getEntityCacheStats,
  LRUEntityCache,
  DEFAULT_MAX_ENTITIES,
  clearGlobalState,
} from '../../../src/ParqueDB/store'

// =============================================================================
// LRUEntityCache Unit Tests
// =============================================================================

describe('LRUEntityCache', () => {
  describe('basic Map interface', () => {
    it('should implement Map interface correctly', () => {
      const cache = new LRUEntityCache({ maxEntities: 100 })

      // Test set/get
      const entity = { $id: 'test/1', $type: 'Test', name: 'Entity 1' } as any
      cache.set('test/1', entity)
      expect(cache.get('test/1')).toBe(entity)

      // Test has
      expect(cache.has('test/1')).toBe(true)
      expect(cache.has('test/2')).toBe(false)

      // Test size
      expect(cache.size).toBe(1)

      // Test delete
      expect(cache.delete('test/1')).toBe(true)
      expect(cache.has('test/1')).toBe(false)
      expect(cache.size).toBe(0)

      // Test clear
      cache.set('test/1', entity)
      cache.set('test/2', { $id: 'test/2', $type: 'Test', name: 'Entity 2' } as any)
      expect(cache.size).toBe(2)
      cache.clear()
      expect(cache.size).toBe(0)
    })

    it('should iterate over entries correctly', () => {
      const cache = new LRUEntityCache({ maxEntities: 100 })

      const entities = [
        { $id: 'test/1', $type: 'Test', name: 'Entity 1' },
        { $id: 'test/2', $type: 'Test', name: 'Entity 2' },
        { $id: 'test/3', $type: 'Test', name: 'Entity 3' },
      ] as any[]

      entities.forEach(e => cache.set(e.$id, e))

      // Test entries()
      const entriesArray = Array.from(cache.entries())
      expect(entriesArray.length).toBe(3)

      // Test keys()
      const keysArray = Array.from(cache.keys())
      expect(keysArray.length).toBe(3)
      expect(keysArray).toContain('test/1')
      expect(keysArray).toContain('test/2')
      expect(keysArray).toContain('test/3')

      // Test values()
      const valuesArray = Array.from(cache.values())
      expect(valuesArray.length).toBe(3)

      // Test forEach
      const forEachResults: string[] = []
      cache.forEach((value, key) => {
        forEachResults.push(key)
      })
      expect(forEachResults.length).toBe(3)
    })

    it('should have correct Symbol.toStringTag', () => {
      const cache = new LRUEntityCache({ maxEntities: 100 })
      expect(cache[Symbol.toStringTag]).toBe('LRUEntityCache')
    })
  })

  describe('LRU eviction', () => {
    it('should evict least recently used entities when limit is reached', () => {
      const evictedKeys: string[] = []
      const cache = new LRUEntityCache({
        maxEntities: 3,
        onEvict: (key) => evictedKeys.push(key),
      })

      // Add 3 entities (at capacity)
      cache.set('test/1', { $id: 'test/1', $type: 'Test', name: 'Entity 1' } as any)
      cache.set('test/2', { $id: 'test/2', $type: 'Test', name: 'Entity 2' } as any)
      cache.set('test/3', { $id: 'test/3', $type: 'Test', name: 'Entity 3' } as any)
      expect(cache.size).toBe(3)
      expect(evictedKeys.length).toBe(0)

      // Add 4th entity, should evict test/1 (LRU)
      cache.set('test/4', { $id: 'test/4', $type: 'Test', name: 'Entity 4' } as any)
      expect(cache.size).toBe(3)
      expect(evictedKeys).toContain('test/1')
      expect(cache.has('test/1')).toBe(false)
      expect(cache.has('test/4')).toBe(true)
    })

    it('should update LRU order on get', () => {
      const evictedKeys: string[] = []
      const cache = new LRUEntityCache({
        maxEntities: 3,
        onEvict: (key) => evictedKeys.push(key),
      })

      // Add 3 entities
      cache.set('test/1', { $id: 'test/1', $type: 'Test', name: 'Entity 1' } as any)
      cache.set('test/2', { $id: 'test/2', $type: 'Test', name: 'Entity 2' } as any)
      cache.set('test/3', { $id: 'test/3', $type: 'Test', name: 'Entity 3' } as any)

      // Access test/1 to make it recently used
      cache.get('test/1')

      // Add test/4, should evict test/2 (now LRU)
      cache.set('test/4', { $id: 'test/4', $type: 'Test', name: 'Entity 4' } as any)
      expect(evictedKeys).toContain('test/2')
      expect(cache.has('test/1')).toBe(true) // Not evicted because we accessed it
      expect(cache.has('test/2')).toBe(false) // Evicted
    })

    it('should update LRU order on set (update existing)', () => {
      const evictedKeys: string[] = []
      const cache = new LRUEntityCache({
        maxEntities: 3,
        onEvict: (key) => evictedKeys.push(key),
      })

      // Add 3 entities
      cache.set('test/1', { $id: 'test/1', $type: 'Test', name: 'Entity 1' } as any)
      cache.set('test/2', { $id: 'test/2', $type: 'Test', name: 'Entity 2' } as any)
      cache.set('test/3', { $id: 'test/3', $type: 'Test', name: 'Entity 3' } as any)

      // Update test/1 to make it recently used
      cache.set('test/1', { $id: 'test/1', $type: 'Test', name: 'Entity 1 Updated' } as any)

      // Add test/4, should evict test/2 (now LRU)
      cache.set('test/4', { $id: 'test/4', $type: 'Test', name: 'Entity 4' } as any)
      expect(evictedKeys).toContain('test/2')
      expect(cache.has('test/1')).toBe(true)
    })
  })

  describe('cache stats', () => {
    it('should track hits and misses', () => {
      const cache = new LRUEntityCache({ maxEntities: 100 })
      cache.set('test/1', { $id: 'test/1', $type: 'Test', name: 'Entity 1' } as any)

      // Miss
      cache.get('test/2')
      let stats = cache.getStats()
      expect(stats.misses).toBe(1)
      expect(stats.hits).toBe(0)

      // Hit
      cache.get('test/1')
      stats = cache.getStats()
      expect(stats.hits).toBe(1)
      expect(stats.misses).toBe(1)
      expect(stats.hitRate).toBeCloseTo(0.5)
    })

    it('should track evictions', () => {
      const cache = new LRUEntityCache({ maxEntities: 2 })
      cache.set('test/1', { $id: 'test/1', $type: 'Test', name: 'Entity 1' } as any)
      cache.set('test/2', { $id: 'test/2', $type: 'Test', name: 'Entity 2' } as any)
      cache.set('test/3', { $id: 'test/3', $type: 'Test', name: 'Entity 3' } as any)

      const stats = cache.getStats()
      expect(stats.evictions).toBe(1)
    })

    it('should report correct maxEntries', () => {
      const cache = new LRUEntityCache({ maxEntities: 500 })
      const stats = cache.getStats()
      expect(stats.maxEntries).toBe(500)
    })
  })

  describe('invalidateByPrefix', () => {
    it('should invalidate all entries with matching prefix', () => {
      const cache = new LRUEntityCache({ maxEntities: 100 })
      cache.set('posts/1', { $id: 'posts/1', $type: 'Post', name: 'Post 1' } as any)
      cache.set('posts/2', { $id: 'posts/2', $type: 'Post', name: 'Post 2' } as any)
      cache.set('users/1', { $id: 'users/1', $type: 'User', name: 'User 1' } as any)

      const invalidated = cache.invalidateByPrefix('posts/')
      expect(invalidated).toBe(2)
      expect(cache.has('posts/1')).toBe(false)
      expect(cache.has('posts/2')).toBe(false)
      expect(cache.has('users/1')).toBe(true)
    })
  })

  describe('unlimited cache', () => {
    it('should not evict when maxEntities is 0', () => {
      const cache = new LRUEntityCache({ maxEntities: 0 })

      // Add many entities
      for (let i = 0; i < 100; i++) {
        cache.set(`test/${i}`, { $id: `test/${i}`, $type: 'Test', name: `Entity ${i}` } as any)
      }

      expect(cache.size).toBe(100)
      const stats = cache.getStats()
      expect(stats.evictions).toBe(0)
    })
  })
})

// =============================================================================
// Integration Tests with ParqueDB
// =============================================================================

describe('ParqueDB Entity Cache Integration', () => {
  let storage: MemoryBackend
  let db: ParqueDB

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  afterEach(() => {
    if (db) {
      db.dispose()
    }
  })

  describe('default configuration', () => {
    it('should use default max entities when not configured', () => {
      db = new ParqueDB({ storage })
      const stats = db.getCacheStats()
      expect(stats).toBeDefined()
      expect(stats!.maxEntries).toBe(DEFAULT_MAX_ENTITIES)
    })
  })

  describe('custom cache size', () => {
    it('should respect maxCacheSize configuration', () => {
      db = new ParqueDB({ storage, maxCacheSize: 5 })
      const stats = db.getCacheStats()
      expect(stats).toBeDefined()
      expect(stats!.maxEntries).toBe(5)
    })

    it('should evict entities when cache limit is reached', async () => {
      const evictedIds: string[] = []
      db = new ParqueDB({
        storage,
        maxCacheSize: 3,
        onCacheEvict: (key) => evictedIds.push(key),
      })

      // Create 4 entities (limit is 3)
      await db.create('posts', { $type: 'Post', name: 'Post 1' })
      await db.create('posts', { $type: 'Post', name: 'Post 2' })
      await db.create('posts', { $type: 'Post', name: 'Post 3' })
      await db.create('posts', { $type: 'Post', name: 'Post 4' })

      const stats = db.getCacheStats()
      expect(stats!.size).toBe(3)
      expect(stats!.evictions).toBeGreaterThanOrEqual(1)
      expect(evictedIds.length).toBeGreaterThanOrEqual(1)
    })

    it('should call onCacheEvict with evicted entity', async () => {
      const evictedEntities: Array<{ key: string; entity: any }> = []
      db = new ParqueDB({
        storage,
        maxCacheSize: 2,
        onCacheEvict: (key, entity) => evictedEntities.push({ key, entity }),
      })

      // Create 3 entities
      const entity1 = await db.create('posts', { $type: 'Post', name: 'Post 1' })
      await db.create('posts', { $type: 'Post', name: 'Post 2' })
      await db.create('posts', { $type: 'Post', name: 'Post 3' })

      // First entity should have been evicted
      expect(evictedEntities.length).toBeGreaterThanOrEqual(1)
      expect(evictedEntities[0]!.key).toBe(entity1.$id)
      expect(evictedEntities[0]!.entity.name).toBe('Post 1')
    })
  })

  describe('cache stats', () => {
    it('should track cache hits and misses', async () => {
      db = new ParqueDB({ storage, maxCacheSize: 100 })

      const entity = await db.create('posts', { $type: 'Post', name: 'Test Post' })

      // Hit: get existing entity
      await db.get('posts', entity.$id)

      // Miss: get non-existent entity
      await db.get('posts', 'nonexistent')

      const stats = db.getCacheStats()
      expect(stats!.hits).toBeGreaterThanOrEqual(1)
      expect(stats!.misses).toBeGreaterThanOrEqual(1)
    })

    it('should return undefined for getCacheStats when store not initialized', () => {
      // Create a fresh storage without a DB
      const freshStorage = new MemoryBackend()
      const stats = getEntityCacheStats(freshStorage)
      expect(stats).toBeUndefined()
    })
  })

  describe('configureEntityStore', () => {
    it('should allow configuring cache before ParqueDB initialization', () => {
      configureEntityStore(storage, { maxEntities: 50 })
      db = new ParqueDB({ storage })

      const stats = db.getCacheStats()
      expect(stats!.maxEntries).toBe(50)
    })

    it('should migrate entities when reconfiguring cache', async () => {
      // Start with large cache
      db = new ParqueDB({ storage, maxCacheSize: 100 })

      // Create some entities
      await db.create('posts', { $type: 'Post', name: 'Post 1' })
      await db.create('posts', { $type: 'Post', name: 'Post 2' })
      await db.create('posts', { $type: 'Post', name: 'Post 3' })

      let stats = db.getCacheStats()
      expect(stats!.size).toBe(3)

      // Reconfigure with smaller limit - this should trigger migration and eviction
      configureEntityStore(storage, { maxEntities: 2 })

      // Get fresh stats (the internal cache reference may have changed)
      stats = getEntityCacheStats(storage)
      expect(stats!.size).toBe(2)
      expect(stats!.maxEntries).toBe(2)
    })
  })

  describe('getEntityStore direct access', () => {
    it('should return LRU cache that implements Map interface', () => {
      db = new ParqueDB({ storage })
      const store = getEntityStore(storage)

      // Should be a Map-like object
      expect(typeof store.get).toBe('function')
      expect(typeof store.set).toBe('function')
      expect(typeof store.has).toBe('function')
      expect(typeof store.delete).toBe('function')
      expect(typeof store.clear).toBe('function')
      expect(typeof store.size).toBe('number')
    })

    it('should pass config to getEntityStore when creating new store', () => {
      const freshStorage = new MemoryBackend()
      const store = getEntityStore(freshStorage, { maxEntities: 25 })

      // Verify it's an LRUEntityCache with the right config
      expect(store).toBeInstanceOf(LRUEntityCache)
      const stats = (store as LRUEntityCache).getStats()
      expect(stats.maxEntries).toBe(25)
    })
  })

  describe('clearGlobalState', () => {
    it('should clear cache and allow fresh configuration', async () => {
      db = new ParqueDB({ storage, maxCacheSize: 10 })
      await db.create('posts', { $type: 'Post', name: 'Post 1' })

      let stats = db.getCacheStats()
      expect(stats!.size).toBe(1)

      // Clear all state
      clearGlobalState(storage)

      // Cache stats should be unavailable now
      stats = getEntityCacheStats(storage)
      expect(stats).toBeUndefined()

      // Can create new DB with different config
      const db2 = new ParqueDB({ storage, maxCacheSize: 5 })
      const newStats = db2.getCacheStats()
      expect(newStats!.maxEntries).toBe(5)
      expect(newStats!.size).toBe(0)
      db2.dispose()
    })
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Entity Cache Edge Cases', () => {
  it('should handle rapid create/delete cycles', async () => {
    const storage = new MemoryBackend()
    const db = new ParqueDB({ storage, maxCacheSize: 5 })

    // Create and delete many entities
    for (let i = 0; i < 20; i++) {
      const entity = await db.create('posts', { $type: 'Post', name: `Post ${i}` })
      await db.delete('posts', entity.$id)
    }

    const stats = db.getCacheStats()
    // All entities were deleted, so cache should be empty
    expect(stats!.size).toBe(0)

    db.dispose()
  })

  it('should handle concurrent access patterns', async () => {
    const storage = new MemoryBackend()
    const db = new ParqueDB({ storage, maxCacheSize: 10 })

    // Create entities concurrently
    const createPromises = Array.from({ length: 20 }, (_, i) =>
      db.create('posts', { $type: 'Post', name: `Post ${i}` })
    )

    await Promise.all(createPromises)

    const stats = db.getCacheStats()
    expect(stats!.size).toBe(10) // Should be at max capacity
    expect(stats!.evictions).toBe(10) // 20 created - 10 capacity = 10 evicted

    db.dispose()
  })

  it('should handle updates to evicted entities gracefully', async () => {
    const storage = new MemoryBackend()
    const evictedIds: string[] = []
    const db = new ParqueDB({
      storage,
      maxCacheSize: 2,
      onCacheEvict: (key) => evictedIds.push(key),
    })

    // Create 3 entities (first will be evicted)
    const entity1 = await db.create('posts', { $type: 'Post', name: 'Post 1' })
    await db.create('posts', { $type: 'Post', name: 'Post 2' })
    await db.create('posts', { $type: 'Post', name: 'Post 3' })

    expect(evictedIds).toContain(entity1.$id)

    // Try to update the evicted entity - should fail gracefully
    const updated = await db.update('posts', entity1.$id, { $set: { name: 'Updated Post 1' } })

    // Update returns null for non-existent entity in cache
    // (Note: in real usage, the entity would be persisted to storage and could be retrieved)
    expect(updated).toBeNull()

    db.dispose()
  })

  it('should maintain correct size after many operations', async () => {
    const storage = new MemoryBackend()
    const db = new ParqueDB({ storage, maxCacheSize: 5 })

    // Mix of creates, updates, deletes
    const entities = []
    for (let i = 0; i < 10; i++) {
      entities.push(await db.create('posts', { $type: 'Post', name: `Post ${i}` }))
    }

    // Update some
    for (let i = 0; i < 5; i++) {
      await db.update('posts', entities[i + 5]!.$id, { $set: { name: `Updated ${i}` } })
    }

    // Delete some
    for (let i = 0; i < 3; i++) {
      await db.delete('posts', entities[i + 5]!.$id)
    }

    const stats = db.getCacheStats()
    // Cache should never exceed maxEntries
    expect(stats!.size).toBeLessThanOrEqual(5)

    db.dispose()
  })
})
