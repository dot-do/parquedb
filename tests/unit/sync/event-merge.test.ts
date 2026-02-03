/**
 * Tests for Event-Based Merge Engine
 *
 * This test suite validates the core merge engine that merges divergent
 * event streams with automatic resolution of commutative operations.
 */

import { describe, it, expect } from 'vitest'
import { mergeEventStreams } from '../../../src/sync/event-merge'
import type { Event } from '../../../src/types/entity'
import { generateULID } from '../../../src/utils/random'

// =============================================================================
// Test Helpers
// =============================================================================

function createEvent(
  target: string,
  op: 'CREATE' | 'UPDATE' | 'DELETE' | 'REL_CREATE' | 'REL_DELETE',
  ts: number,
  before?: unknown,
  after?: unknown,
  metadata?: Record<string, unknown>
): Event {
  return {
    id: generateULID(),
    ts,
    op,
    target,
    before: before as any,
    after: after as any,
    actor: 'test',
    metadata
  }
}

// =============================================================================
// Basic Merge Tests
// =============================================================================

describe('Event Merge - Basic Cases', () => {
  it('should merge with no overlapping entities', async () => {
    const baseEvents: Event[] = []

    const ourEvents: Event[] = [
      createEvent('users:u1', 'CREATE', 1000, undefined, { name: 'Alice' })
    ]

    const theirEvents: Event[] = [
      createEvent('users:u2', 'CREATE', 1000, undefined, { name: 'Bob' })
    ]

    const result = await mergeEventStreams(baseEvents, ourEvents, theirEvents)

    expect(result.success).toBe(true)
    expect(result.conflicts).toHaveLength(0)
    expect(result.mergedEvents).toHaveLength(2)
  })

  it('should merge events only in our branch', async () => {
    const baseEvents: Event[] = []

    const ourEvents: Event[] = [
      createEvent('users:u1', 'CREATE', 1000, undefined, { name: 'Alice' }),
      createEvent('users:u1', 'UPDATE', 1100, { name: 'Alice' }, { name: 'Alice Updated' })
    ]

    const theirEvents: Event[] = []

    const result = await mergeEventStreams(baseEvents, ourEvents, theirEvents)

    expect(result.success).toBe(true)
    expect(result.conflicts).toHaveLength(0)
    expect(result.mergedEvents).toHaveLength(2)
  })

  it('should merge events only in their branch', async () => {
    const baseEvents: Event[] = []

    const ourEvents: Event[] = []

    const theirEvents: Event[] = [
      createEvent('users:u1', 'CREATE', 1000, undefined, { name: 'Bob' }),
      createEvent('users:u1', 'UPDATE', 1100, { name: 'Bob' }, { name: 'Bob Updated' })
    ]

    const result = await mergeEventStreams(baseEvents, ourEvents, theirEvents)

    expect(result.success).toBe(true)
    expect(result.conflicts).toHaveLength(0)
    expect(result.mergedEvents).toHaveLength(2)
  })

  it('should handle empty event streams', async () => {
    const result = await mergeEventStreams([], [], [])

    expect(result.success).toBe(true)
    expect(result.conflicts).toHaveLength(0)
    expect(result.mergedEvents).toHaveLength(0)
  })
})

// =============================================================================
// Commutative Operations Tests
// =============================================================================

describe('Event Merge - Commutative Operations', () => {
  it('should auto-merge $inc operations on same field', async () => {
    const baseEvents: Event[] = [
      createEvent('posts:p1', 'CREATE', 900, undefined, { views: 10 })
    ]

    const ourEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000, { views: 10 }, { views: 15, _ops: { $inc: { views: 5 } } })
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000, { views: 10 }, { views: 13, _ops: { $inc: { views: 3 } } })
    ]

    const result = await mergeEventStreams(baseEvents, ourEvents, theirEvents)

    // $inc operations should auto-merge (they're commutative)
    expect(result.success).toBe(true)
    expect(result.conflicts).toHaveLength(0)
    expect(result.autoMerged.length).toBeGreaterThan(0)
  })

  it('should auto-merge updates on different fields', async () => {
    const baseEvents: Event[] = [
      createEvent('posts:p1', 'CREATE', 900, undefined, { title: 'Old', views: 10 })
    ]

    const ourEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000,
        { title: 'Old', views: 10 },
        { title: 'New Title', views: 10, _ops: { $set: { title: 'New Title' } } }
      )
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000,
        { title: 'Old', views: 10 },
        { title: 'Old', views: 15, _ops: { $inc: { views: 5 } } }
      )
    ]

    const result = await mergeEventStreams(baseEvents, ourEvents, theirEvents)

    // Different fields should not conflict
    expect(result.success).toBe(true)
    expect(result.conflicts).toHaveLength(0)
  })
})

// =============================================================================
// Conflict Detection Tests
// =============================================================================

describe('Event Merge - Conflict Detection', () => {
  it('should detect concurrent update conflict', async () => {
    const baseEvents: Event[] = [
      createEvent('posts:p1', 'CREATE', 900, undefined, { status: 'draft' })
    ]

    const ourEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000,
        { status: 'draft' },
        { status: 'published' }
      )
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000,
        { status: 'draft' },
        { status: 'archived' }
      )
    ]

    const result = await mergeEventStreams(baseEvents, ourEvents, theirEvents)

    // Different values on same field = conflict
    expect(result.success).toBe(false)
    expect(result.conflicts.length).toBeGreaterThan(0)
  })

  it('should detect DELETE vs UPDATE conflict', async () => {
    const baseEvents: Event[] = [
      createEvent('posts:p1', 'CREATE', 900, undefined, { title: 'Post' })
    ]

    const ourEvents: Event[] = [
      createEvent('posts:p1', 'DELETE', 1000, { title: 'Post' }, undefined)
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000,
        { title: 'Post' },
        { title: 'Updated Post' }
      )
    ]

    const result = await mergeEventStreams(baseEvents, ourEvents, theirEvents)

    expect(result.success).toBe(false)
    expect(result.conflicts.length).toBeGreaterThan(0)
    expect(result.conflicts[0]!.type).toBe('delete_update')
  })

  it('should detect CREATE conflict for same entity', async () => {
    const baseEvents: Event[] = []

    const ourEvents: Event[] = [
      createEvent('posts:p1', 'CREATE', 1000, undefined, { title: 'Our Post' })
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p1', 'CREATE', 1000, undefined, { title: 'Their Post' })
    ]

    const result = await mergeEventStreams(baseEvents, ourEvents, theirEvents)

    expect(result.success).toBe(false)
    expect(result.conflicts.length).toBeGreaterThan(0)
    expect(result.conflicts[0]!.type).toBe('create_create')
  })
})

// =============================================================================
// Resolution Tests
// =============================================================================

describe('Event Merge - Conflict Resolution', () => {
  it('should auto-resolve with "ours" strategy', async () => {
    const baseEvents: Event[] = [
      createEvent('posts:p1', 'CREATE', 900, undefined, { status: 'draft' })
    ]

    const ourEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000,
        { status: 'draft' },
        { status: 'published' }
      )
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000,
        { status: 'draft' },
        { status: 'archived' }
      )
    ]

    const result = await mergeEventStreams(baseEvents, ourEvents, theirEvents, {
      resolutionStrategy: 'ours'
    })

    // Should resolve using "ours" strategy
    expect(result.conflicts).toHaveLength(0)
    expect(result.resolved.length).toBeGreaterThan(0)
    expect(result.resolved[0]!.strategy).toBe('ours')
  })

  it('should auto-resolve with "latest" strategy', async () => {
    const baseEvents: Event[] = [
      createEvent('posts:p1', 'CREATE', 900, undefined, { status: 'draft' })
    ]

    const ourEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1000,
        { status: 'draft' },
        { status: 'published' }
      )
    ]

    const theirEvents: Event[] = [
      createEvent('posts:p1', 'UPDATE', 1100,
        { status: 'draft' },
        { status: 'archived' }
      )
    ]

    const result = await mergeEventStreams(baseEvents, ourEvents, theirEvents, {
      resolutionStrategy: 'latest'
    })

    // Should resolve using "latest" strategy (their event is newer)
    expect(result.conflicts).toHaveLength(0)
    expect(result.resolved.length).toBeGreaterThan(0)
    expect(result.resolved[0]!.strategy).toBe('latest')
    expect(result.resolved[0]!.resolvedValue).toBe('archived')
  })
})
