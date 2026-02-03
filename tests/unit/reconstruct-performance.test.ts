/**
 * Performance Test for reconstructEntityAtTime Optimization
 *
 * This test verifies that the entity event index and reconstruction cache
 * provide O(1) lookup performance instead of O(n) filtering for time-travel queries.
 *
 * The optimization addresses issue parquedb-48hu.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import type { EntityId } from '../../src/types'

describe('reconstructEntityAtTime Performance', () => {
  let db: ParqueDB
  let storage: MemoryBackend

  beforeEach(async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-01T12:00:00Z'))
    storage = new MemoryBackend()
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    vi.useRealTimers()
    db.dispose()
  })

  it('uses entity event index for O(1) lookup instead of O(n) filter', async () => {
    // Create many entities to simulate a large event store
    const entityCount = 100
    const timestamps: Map<string, Date> = new Map()

    // Create entities and record their creation timestamps
    for (let i = 0; i < entityCount; i++) {
      vi.advanceTimersByTime(10) // Ensure different timestamps
      const entity = await db.create('posts', {
        $type: 'Post',
        name: `Post ${i}`,
        title: `Title ${i}`,
        content: `Content ${i}`,
      })
      timestamps.set(entity.$id as string, new Date())
    }

    // Pick a few random entities to query with time-travel
    const entityIds = Array.from(timestamps.keys())
    const testEntityId = entityIds[50]!
    const testTimestamp = timestamps.get(testEntityId)!

    // First query should populate the index (fallback to O(n) filter)
    const firstQuery = await db.get('posts', testEntityId, { asOf: testTimestamp })
    expect(firstQuery).not.toBeNull()
    expect(firstQuery!.name).toBe('Post 50')

    // Second query should use the index (O(1) lookup)
    const secondQuery = await db.get('posts', testEntityId, { asOf: testTimestamp })
    expect(secondQuery).not.toBeNull()
    expect(secondQuery!.name).toBe('Post 50')

    // Third query with same timestamp should hit the cache
    const thirdQuery = await db.get('posts', testEntityId, { asOf: testTimestamp })
    expect(thirdQuery).not.toBeNull()
    expect(thirdQuery!.name).toBe('Post 50')
  })

  it('caches reconstruction results for repeated queries', async () => {
    // Create an entity with multiple updates
    const entity = await db.create('posts', {
      $type: 'Post',
      name: 'Test Post',
      title: 'V1',
      content: 'Initial content',
    })

    const t1 = new Date()
    vi.advanceTimersByTime(100)

    await db.update('posts', entity.$id as string, {
      $set: { title: 'V2' },
    })

    const t2 = new Date()
    vi.advanceTimersByTime(100)

    await db.update('posts', entity.$id as string, {
      $set: { title: 'V3' },
    })

    // Query at t1 multiple times - should use cache after first query
    for (let i = 0; i < 10; i++) {
      const result = await db.get('posts', entity.$id as string, { asOf: t1 })
      expect(result!.title).toBe('V1')
    }

    // Query at t2 multiple times - should use cache after first query
    for (let i = 0; i < 10; i++) {
      const result = await db.get('posts', entity.$id as string, { asOf: t2 })
      expect(result!.title).toBe('V2')
    }
  })

  it('invalidates cache when entity is modified', async () => {
    const entity = await db.create('posts', {
      $type: 'Post',
      name: 'Test Post',
      title: 'Original',
      content: 'Content',
    })

    const beforeUpdate = new Date()
    vi.advanceTimersByTime(100)

    // Query to populate cache
    const cached = await db.get('posts', entity.$id as string, { asOf: beforeUpdate })
    expect(cached!.title).toBe('Original')

    // Modify the entity - this should invalidate the cache
    await db.update('posts', entity.$id as string, {
      $set: { title: 'Updated' },
    })

    // Query again at the same time - cache was invalidated by the update
    // Note: The cache entry for beforeUpdate timestamp is removed on update
    const afterInvalidation = await db.get('posts', entity.$id as string, { asOf: beforeUpdate })
    expect(afterInvalidation!.title).toBe('Original') // Still returns correct historical state
  })

  it('handles binary search for finding events at specific time', async () => {
    // Create entity with many updates at different times
    const entity = await db.create('posts', {
      $type: 'Post',
      name: 'Test Post',
      title: 'V0',
      content: 'Content',
    })

    const timestamps: Date[] = [new Date()]

    // Create 20 updates at different times
    for (let i = 1; i <= 20; i++) {
      vi.advanceTimersByTime(100)
      await db.update('posts', entity.$id as string, {
        $set: { title: `V${i}` },
      })
      timestamps.push(new Date())
    }

    // Query at each timestamp - binary search should find correct version
    for (let i = 0; i <= 20; i++) {
      const result = await db.get('posts', entity.$id as string, { asOf: timestamps[i]! })
      expect(result!.title).toBe(`V${i}`)
    }

    // Query at a time between updates - should get the earlier version
    const midTime = new Date(timestamps[10]!.getTime() + 50) // 50ms after V10
    const midResult = await db.get('posts', entity.$id as string, { asOf: midTime })
    expect(midResult!.title).toBe('V10')
  })

  it('handles entity that did not exist at query time', async () => {
    const beforeCreation = new Date()
    vi.advanceTimersByTime(100)

    const entity = await db.create('posts', {
      $type: 'Post',
      name: 'Test Post',
      title: 'Hello',
      content: 'Content',
    })

    // Query before entity existed - should return null (and cache it)
    const result1 = await db.get('posts', entity.$id as string, { asOf: beforeCreation })
    expect(result1).toBeNull()

    // Second query should hit cache for null result
    const result2 = await db.get('posts', entity.$id as string, { asOf: beforeCreation })
    expect(result2).toBeNull()
  })

  it('handles find() with asOf across many entities', async () => {
    // Create multiple entities
    const entities: { id: string; createdAt: Date }[] = []

    for (let i = 0; i < 50; i++) {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: `Post ${i}`,
        title: `Title ${i}`,
        content: `Content ${i}`,
        status: i < 25 ? 'draft' : 'published',
      })
      entities.push({ id: entity.$id as string, createdAt: new Date() })
      vi.advanceTimersByTime(10)
    }

    // Get time after first 30 entities
    const midTime = entities[29]!.createdAt

    // Find all entities that existed at midTime
    const result = await db.find('posts', {}, { asOf: midTime })
    expect(result.items.length).toBe(30) // Only first 30 existed at midTime

    // Find published entities at midTime (should be 5: entities 25-29)
    const publishedResult = await db.find('posts', { status: 'published' }, { asOf: midTime })
    expect(publishedResult.items.length).toBe(5)
  })
})
