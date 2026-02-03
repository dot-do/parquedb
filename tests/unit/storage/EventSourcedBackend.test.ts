/**
 * EventSourcedBackend Tests
 *
 * Tests for the unified event-sourced storage abstraction that provides
 * consistent behavior across Node.js and Workers environments.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  EventSourcedBackend,
  withEventSourcing,
  MemoryBackend,
} from '../../../src/storage'
import type { Event, EventOp, Variant } from '../../../src/types'

// =============================================================================
// Test Helpers
// =============================================================================

function createEvent(
  ns: string,
  id: string,
  op: EventOp,
  before?: Variant,
  after?: Variant
): Event {
  return {
    id: `evt_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    ts: Date.now(),
    op,
    target: `${ns}:${id}`,
    before,
    after,
    actor: 'test/user',
  }
}

// =============================================================================
// Tests
// =============================================================================

describe('EventSourcedBackend', () => {
  let memoryBackend: MemoryBackend
  let eventSourced: EventSourcedBackend

  beforeEach(() => {
    memoryBackend = new MemoryBackend()
    eventSourced = new EventSourcedBackend(memoryBackend, {
      maxBufferedEvents: 10,
      autoSnapshotThreshold: 5,
      maxCachedEntities: 100,
      cacheTtlMs: 60000,
    })
  })

  describe('appendEvent', () => {
    it('should append CREATE event', async () => {
      const event = createEvent('users', 'abc', 'CREATE', undefined, {
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
      })

      await eventSourced.appendEvent(event)

      // Verify event was stored
      const events = await eventSourced.getEntityEvents('users', 'abc')
      expect(events).toHaveLength(1)
      expect(events[0].op).toBe('CREATE')
    })

    it('should append UPDATE event', async () => {
      // First create
      const createEvent1 = createEvent('users', 'abc', 'CREATE', undefined, {
        $type: 'User',
        name: 'Alice',
      })
      await eventSourced.appendEvent(createEvent1)

      // Then update
      const updateEvent = createEvent(
        'users',
        'abc',
        'UPDATE',
        { $type: 'User', name: 'Alice' },
        { $type: 'User', name: 'Alice Updated' }
      )
      await eventSourced.appendEvent(updateEvent)

      const events = await eventSourced.getEntityEvents('users', 'abc')
      expect(events).toHaveLength(2)
      expect(events[1].op).toBe('UPDATE')
    })

    it('should append DELETE event', async () => {
      // Create then delete
      const createEvent1 = createEvent('users', 'abc', 'CREATE', undefined, {
        $type: 'User',
        name: 'Alice',
      })
      await eventSourced.appendEvent(createEvent1)

      const deleteEvent = createEvent('users', 'abc', 'DELETE', {
        $type: 'User',
        name: 'Alice',
      })
      await eventSourced.appendEvent(deleteEvent)

      const events = await eventSourced.getEntityEvents('users', 'abc')
      expect(events).toHaveLength(2)
      expect(events[1].op).toBe('DELETE')
    })
  })

  describe('getEntityEvents', () => {
    it('should return events for specific entity only', async () => {
      // Create two entities
      await eventSourced.appendEvent(
        createEvent('users', 'abc', 'CREATE', undefined, { $type: 'User', name: 'Alice' })
      )
      await eventSourced.appendEvent(
        createEvent('users', 'def', 'CREATE', undefined, { $type: 'User', name: 'Bob' })
      )

      const aliceEvents = await eventSourced.getEntityEvents('users', 'abc')
      expect(aliceEvents).toHaveLength(1)
      expect((aliceEvents[0].after as { name: string }).name).toBe('Alice')

      const bobEvents = await eventSourced.getEntityEvents('users', 'def')
      expect(bobEvents).toHaveLength(1)
      expect((bobEvents[0].after as { name: string }).name).toBe('Bob')
    })

    it('should return empty array for non-existent entity', async () => {
      const events = await eventSourced.getEntityEvents('users', 'nonexistent')
      expect(events).toHaveLength(0)
    })
  })

  describe('reconstructEntity', () => {
    it('should reconstruct entity from CREATE event', async () => {
      await eventSourced.appendEvent(
        createEvent('users', 'abc', 'CREATE', undefined, {
          $type: 'User',
          name: 'Alice',
          email: 'alice@example.com',
        })
      )

      const entity = await eventSourced.reconstructEntity('users', 'abc')

      expect(entity).not.toBeNull()
      expect(entity!.$id).toBe('users/abc')
      expect(entity!.$type).toBe('User')
      expect(entity!.name).toBe('Alice')
      expect((entity as { email: string }).email).toBe('alice@example.com')
    })

    it('should reconstruct entity with updates applied', async () => {
      // Create
      await eventSourced.appendEvent(
        createEvent('users', 'abc', 'CREATE', undefined, {
          $type: 'User',
          name: 'Alice',
          score: 100,
        })
      )

      // Update
      await eventSourced.appendEvent(
        createEvent(
          'users',
          'abc',
          'UPDATE',
          { $type: 'User', name: 'Alice', score: 100 },
          { name: 'Alice Updated', score: 150 }
        )
      )

      const entity = await eventSourced.reconstructEntity('users', 'abc')

      expect(entity).not.toBeNull()
      expect(entity!.name).toBe('Alice Updated')
      expect((entity as { score: number }).score).toBe(150)
      expect(entity!.version).toBe(2) // Version incremented by update
    })

    it('should return deleted entity with deletedAt set', async () => {
      // Create then delete
      await eventSourced.appendEvent(
        createEvent('users', 'abc', 'CREATE', undefined, {
          $type: 'User',
          name: 'Alice',
        })
      )
      await eventSourced.appendEvent(
        createEvent('users', 'abc', 'DELETE', { $type: 'User', name: 'Alice' })
      )

      const entity = await eventSourced.reconstructEntity('users', 'abc')

      expect(entity).not.toBeNull()
      expect(entity!.deletedAt).toBeDefined()
    })

    it('should return null for non-existent entity', async () => {
      const entity = await eventSourced.reconstructEntity('users', 'nonexistent')
      expect(entity).toBeNull()
    })

    it('should cache reconstructed entities', async () => {
      await eventSourced.appendEvent(
        createEvent('users', 'abc', 'CREATE', undefined, {
          $type: 'User',
          name: 'Alice',
        })
      )

      // First reconstruction
      const entity1 = await eventSourced.reconstructEntity('users', 'abc')

      // Second reconstruction (should use cache)
      const entity2 = await eventSourced.reconstructEntity('users', 'abc')

      expect(entity1).toEqual(entity2)
    })
  })

  describe('snapshots', () => {
    it('should create snapshot for entity', async () => {
      await eventSourced.appendEvent(
        createEvent('users', 'abc', 'CREATE', undefined, {
          $type: 'User',
          name: 'Alice',
        })
      )

      const entity = await eventSourced.reconstructEntity('users', 'abc')
      await eventSourced.createSnapshot('users', 'abc', entity!, 1)

      const snapshot = await eventSourced.getLatestSnapshot('users', 'abc')
      expect(snapshot).not.toBeNull()
      expect(snapshot!.seq).toBe(1)
      expect(snapshot!.state.name).toBe('Alice')
    })

    it('should reconstruct from snapshot plus events', async () => {
      // Create entity
      await eventSourced.appendEvent(
        createEvent('users', 'abc', 'CREATE', undefined, {
          $type: 'User',
          name: 'Alice',
          score: 0,
        })
      )

      // Flush events to storage before snapshot
      await eventSourced.flush()

      // Create snapshot
      const entity = await eventSourced.reconstructEntity('users', 'abc')
      await eventSourced.createSnapshot('users', 'abc', entity!, 1)

      // Add more events after snapshot
      await eventSourced.appendEvent(
        createEvent(
          'users',
          'abc',
          'UPDATE',
          { score: 0 },
          { score: 100 }
        )
      )

      // Flush events to storage before clearing
      await eventSourced.flush()

      // Clear cache to force reconstruction from storage
      eventSourced.clear()

      // Reconstruct should use snapshot + replay new events
      const reconstructed = await eventSourced.reconstructEntity('users', 'abc')
      expect(reconstructed).not.toBeNull()
      expect((reconstructed as { score: number }).score).toBe(100)
    })
  })

  describe('flush', () => {
    it('should flush buffered events to storage', async () => {
      // Add several events
      for (let i = 0; i < 5; i++) {
        await eventSourced.appendEvent(
          createEvent('users', `user${i}`, 'CREATE', undefined, {
            $type: 'User',
            name: `User ${i}`,
          })
        )
      }

      // Flush to storage
      await eventSourced.flush()

      // Create new EventSourcedBackend with same storage
      const newEventSourced = new EventSourcedBackend(memoryBackend)

      // Should be able to read flushed events
      const events = await newEventSourced.getEntityEvents('users', 'user0')
      expect(events).toHaveLength(1)
    })

    it('should auto-flush when buffer threshold reached', async () => {
      // Config has maxBufferedEvents: 10
      // Add 15 events to trigger auto-flush
      for (let i = 0; i < 15; i++) {
        await eventSourced.appendEvent(
          createEvent('users', `user${i}`, 'CREATE', undefined, {
            $type: 'User',
            name: `User ${i}`,
          })
        )
      }

      // Events should be readable from new instance
      const newEventSourced = new EventSourcedBackend(memoryBackend)
      const events = await newEventSourced.getEntityEvents('users', 'user0')
      expect(events.length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('sequences', () => {
    it('should track sequence numbers per namespace', () => {
      expect(eventSourced.getSequence('users')).toBe(1)
      expect(eventSourced.getSequence('posts')).toBe(1)

      eventSourced.nextSequence('users')
      eventSourced.nextSequence('users')

      expect(eventSourced.getSequence('users')).toBe(3)
      expect(eventSourced.getSequence('posts')).toBe(1)
    })

    it('should persist sequences across flush', async () => {
      eventSourced.nextSequence('users')
      eventSourced.nextSequence('users')
      eventSourced.nextSequence('posts')

      await eventSourced.flush()

      // Create new instance with same storage
      const newEventSourced = new EventSourcedBackend(memoryBackend)

      // Need to initialize by calling any method
      await newEventSourced.getEntityEvents('users', 'test')

      expect(newEventSourced.getSequence('users')).toBe(3)
      expect(newEventSourced.getSequence('posts')).toBe(2)
    })
  })

  describe('withEventSourcing factory', () => {
    it('should create EventSourcedBackend wrapper', () => {
      const storage = withEventSourcing(new MemoryBackend())
      expect(storage).toBeInstanceOf(EventSourcedBackend)
    })

    it('should accept configuration options', () => {
      const storage = withEventSourcing(new MemoryBackend(), {
        autoSnapshotThreshold: 50,
        maxCachedEntities: 500,
      })
      expect(storage).toBeInstanceOf(EventSourcedBackend)
    })
  })

  describe('dispose', () => {
    it('should flush and clear on dispose', async () => {
      await eventSourced.appendEvent(
        createEvent('users', 'abc', 'CREATE', undefined, {
          $type: 'User',
          name: 'Alice',
        })
      )

      await eventSourced.dispose()

      // Verify events were flushed
      const newEventSourced = new EventSourcedBackend(memoryBackend)
      const events = await newEventSourced.getEntityEvents('users', 'abc')
      expect(events).toHaveLength(1)
    })
  })
})

describe('EventSourcedBackend - Unified Behavior', () => {
  it('should provide same behavior as ParqueDBDO event sourcing', async () => {
    const storage = withEventSourcing(new MemoryBackend())

    // Simulate the ParqueDBDO pattern:
    // 1. Create entity by appending CREATE event
    await storage.appendEvent({
      id: 'evt1',
      ts: Date.now(),
      op: 'CREATE',
      target: 'users:abc123',
      after: {
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
      },
      actor: 'system/anonymous',
    })

    // 2. Update entity by appending UPDATE event
    await storage.appendEvent({
      id: 'evt2',
      ts: Date.now() + 1,
      op: 'UPDATE',
      target: 'users:abc123',
      before: { name: 'Alice' },
      after: { name: 'Alice Smith', email: 'alice.smith@example.com' },
      actor: 'users/admin',
    })

    // 3. Reconstruct entity from events (like getEntityFromEvents in DO)
    const entity = await storage.reconstructEntity('users', 'abc123')

    // Verify the entity state matches what ParqueDBDO would produce
    expect(entity).not.toBeNull()
    expect(entity!.$type).toBe('User')
    expect(entity!.name).toBe('Alice Smith')
    expect((entity as { email: string }).email).toBe('alice.smith@example.com')
    expect(entity!.version).toBe(2)
  })

  it('should handle soft delete like ParqueDBDO', async () => {
    const storage = withEventSourcing(new MemoryBackend())

    // Create
    await storage.appendEvent({
      id: 'evt1',
      ts: Date.now(),
      op: 'CREATE',
      target: 'posts:xyz',
      after: { $type: 'Post', name: 'My Post', content: 'Hello' },
    })

    // Soft delete
    await storage.appendEvent({
      id: 'evt2',
      ts: Date.now() + 1,
      op: 'DELETE',
      target: 'posts:xyz',
      before: { $type: 'Post', name: 'My Post', content: 'Hello' },
      actor: 'users/admin',
    })

    const entity = await storage.reconstructEntity('posts', 'xyz')

    // Entity should exist but be marked as deleted
    expect(entity).not.toBeNull()
    expect(entity!.deletedAt).toBeDefined()
    expect(entity!.deletedBy).toBe('users/admin')
  })
})
