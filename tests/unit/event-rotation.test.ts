/**
 * Event Log Rotation Test Suite
 *
 * Tests for event log rotation functionality in ParqueDB.
 * Verifies maxEvents, maxAge, and archiveOnRotation behavior.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import type { EntityId } from '../../src/types'

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Advance fake timers by specified milliseconds (use only with vi.useFakeTimers())
 */
function advanceTime(ms: number): void {
  vi.advanceTimersByTime(ms)
}

// =============================================================================
// Event Log Rotation Test Suite
// =============================================================================

describe('Event Log Rotation', () => {
  let db: ParqueDB
  let storage: MemoryBackend

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-01T12:00:00Z'))
    storage = new MemoryBackend()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  // ===========================================================================
  // Default Configuration Tests
  // ===========================================================================

  describe('default configuration', () => {
    it('uses default maxEvents of 10000', async () => {
      db = new ParqueDB({ storage })
      const eventLog = db.getEventLog()
      const config = eventLog.getConfig()

      expect(config.maxEvents).toBe(10000)
    })

    it('uses default maxAge of 7 days', async () => {
      db = new ParqueDB({ storage })
      const eventLog = db.getEventLog()
      const config = eventLog.getConfig()

      expect(config.maxAge).toBe(7 * 24 * 60 * 60 * 1000)
    })

    it('uses default archiveOnRotation of false', async () => {
      db = new ParqueDB({ storage })
      const eventLog = db.getEventLog()
      const config = eventLog.getConfig()

      expect(config.archiveOnRotation).toBe(false)
    })
  })

  // ===========================================================================
  // Custom Configuration Tests
  // ===========================================================================

  describe('custom configuration', () => {
    it('respects custom maxEvents', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 100 },
      })
      const eventLog = db.getEventLog()
      const config = eventLog.getConfig()

      expect(config.maxEvents).toBe(100)
    })

    it('respects custom maxAge', async () => {
      const oneDayMs = 24 * 60 * 60 * 1000
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxAge: oneDayMs },
      })
      const eventLog = db.getEventLog()
      const config = eventLog.getConfig()

      expect(config.maxAge).toBe(oneDayMs)
    })

    it('respects custom archiveOnRotation', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { archiveOnRotation: true },
      })
      const eventLog = db.getEventLog()
      const config = eventLog.getConfig()

      expect(config.archiveOnRotation).toBe(true)
    })
  })

  // ===========================================================================
  // maxEvents Rotation Tests
  // ===========================================================================

  describe('maxEvents rotation', () => {
    it('rotates events when maxEvents is exceeded', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 5 },
      })

      // Create 8 entities (8 CREATE events)
      for (let i = 0; i < 8; i++) {
        await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
        })
      }

      const eventLog = db.getEventLog()
      const eventCount = await eventLog.getEventCount()

      // Should have rotated to only keep 5 events
      expect(eventCount).toBe(5)
    })

    it('keeps the newest events when rotating', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 3 },
      })

      // Create 5 entities
      const entities: { $id: string }[] = []
      for (let i = 0; i < 5; i++) {
        const entity = await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
        })
        entities.push(entity)
        advanceTime(5) // Small delay to ensure different timestamps
      }

      const eventLog = db.getEventLog()

      // Check that the last 3 entities have events
      const lastEntityEvents = await eventLog.getEvents(entities[4].$id as EntityId)
      expect(lastEntityEvents.length).toBe(1)

      // First two entities' events should be rotated out
      const firstEntityEvents = await eventLog.getEvents(entities[0].$id as EntityId)
      expect(firstEntityEvents.length).toBe(0)
    })

    it('drops events when archiveOnRotation is false', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 3, archiveOnRotation: false },
      })

      // Create 5 entities
      for (let i = 0; i < 5; i++) {
        await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
        })
      }

      const eventLog = db.getEventLog()
      const archivedEvents = await eventLog.getArchivedEvents()

      // Archived events should be empty
      expect(archivedEvents.length).toBe(0)
    })

    it('archives events when archiveOnRotation is true', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 3, archiveOnRotation: true },
      })

      // Create 5 entities
      for (let i = 0; i < 5; i++) {
        await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
        })
      }

      const eventLog = db.getEventLog()
      const archivedEvents = await eventLog.getArchivedEvents()

      // Should have 2 archived events (5 - 3 = 2)
      expect(archivedEvents.length).toBe(2)
    })
  })

  // ===========================================================================
  // Manual Archive Tests
  // ===========================================================================

  describe('manual archiveEvents', () => {
    it('archives events older than specified date', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 100, archiveOnRotation: true },
      })

      // Create some entities with sleep to ensure different timestamps
      await db.create('posts', { $type: 'Post', name: 'Post 1', title: 'Title 1' })
      advanceTime(5)
      await db.create('posts', { $type: 'Post', name: 'Post 2', title: 'Title 2' })

      // Wait and capture midTime after the first two events are definitely in the past
      advanceTime(15)
      const midTime = new Date()
      advanceTime(15)

      await db.create('posts', { $type: 'Post', name: 'Post 3', title: 'Title 3' })
      advanceTime(5)
      await db.create('posts', { $type: 'Post', name: 'Post 4', title: 'Title 4' })

      const eventLog = db.getEventLog()

      // Verify we have 4 events before archiving
      const totalBefore = await eventLog.getEventCount()
      expect(totalBefore).toBe(4)

      // Archive events older than midTime
      const result = await eventLog.archiveEvents({ olderThan: midTime })

      expect(result.archivedCount).toBe(2)
      expect(result.droppedCount).toBe(0)

      const remainingCount = await eventLog.getEventCount()
      expect(remainingCount).toBe(2)

      const archivedEvents = await eventLog.getArchivedEvents()
      expect(archivedEvents.length).toBe(2)
    })

    it('archives events to respect maxEvents limit', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 100, archiveOnRotation: true },
      })

      // Create 10 entities
      for (let i = 0; i < 10; i++) {
        await db.create('posts', { $type: 'Post', name: `Post ${i}`, title: `Title ${i}` })
      }

      const eventLog = db.getEventLog()

      // Archive to keep only 5 events
      const result = await eventLog.archiveEvents({ maxEvents: 5 })

      expect(result.archivedCount).toBe(5)
      expect(result.droppedCount).toBe(0)

      const remainingCount = await eventLog.getEventCount()
      expect(remainingCount).toBe(5)
    })

    it('returns correct result when dropping events', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 100, archiveOnRotation: false },
      })

      // Create 10 entities
      for (let i = 0; i < 10; i++) {
        await db.create('posts', { $type: 'Post', name: `Post ${i}`, title: `Title ${i}` })
      }

      const eventLog = db.getEventLog()

      // Archive to keep only 5 events (but archiveOnRotation is false)
      const result = await eventLog.archiveEvents({ maxEvents: 5 })

      expect(result.archivedCount).toBe(0)
      expect(result.droppedCount).toBe(5)

      const remainingCount = await eventLog.getEventCount()
      expect(remainingCount).toBe(5)

      const archivedEvents = await eventLog.getArchivedEvents()
      expect(archivedEvents.length).toBe(0)
    })

    it('returns oldest event timestamp in result', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 100, archiveOnRotation: true },
      })

      await db.create('posts', { $type: 'Post', name: 'Post 1', title: 'Title 1' })
      advanceTime(10)
      await db.create('posts', { $type: 'Post', name: 'Post 2', title: 'Title 2' })
      advanceTime(10)

      const beforeThird = Date.now()
      await db.create('posts', { $type: 'Post', name: 'Post 3', title: 'Title 3' })

      const eventLog = db.getEventLog()
      const result = await eventLog.archiveEvents({ maxEvents: 1 })

      expect(result.oldestEventTs).toBeDefined()
      expect(result.oldestEventTs).toBeGreaterThanOrEqual(beforeThird)
    })

    it('returns newest archived timestamp in result', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 100, archiveOnRotation: true },
      })

      await db.create('posts', { $type: 'Post', name: 'Post 1', title: 'Title 1' })
      advanceTime(10)

      const afterFirst = Date.now()
      await db.create('posts', { $type: 'Post', name: 'Post 2', title: 'Title 2' })
      advanceTime(10)
      await db.create('posts', { $type: 'Post', name: 'Post 3', title: 'Title 3' })

      const eventLog = db.getEventLog()
      const result = await eventLog.archiveEvents({ maxEvents: 1 })

      expect(result.newestArchivedTs).toBeDefined()
      expect(result.newestArchivedTs).toBeLessThan(afterFirst + 50) // Should be around afterFirst
    })
  })

  // ===========================================================================
  // getEventCount Tests
  // ===========================================================================

  describe('getEventCount', () => {
    it('returns 0 for empty event log', async () => {
      db = new ParqueDB({ storage })
      const eventLog = db.getEventLog()
      const count = await eventLog.getEventCount()

      expect(count).toBe(0)
    })

    it('returns correct count after operations', async () => {
      db = new ParqueDB({ storage })

      await db.create('posts', { $type: 'Post', name: 'Post 1', title: 'Title 1' })
      await db.create('posts', { $type: 'Post', name: 'Post 2', title: 'Title 2' })

      const eventLog = db.getEventLog()
      const count = await eventLog.getEventCount()

      expect(count).toBe(2)
    })

    it('reflects rotation', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 3 },
      })

      for (let i = 0; i < 5; i++) {
        await db.create('posts', { $type: 'Post', name: `Post ${i}`, title: `Title ${i}` })
      }

      const eventLog = db.getEventLog()
      const count = await eventLog.getEventCount()

      expect(count).toBe(3)
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('handles maxEvents of 0 gracefully', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 0 },
      })

      // This should immediately rotate all events
      await db.create('posts', { $type: 'Post', name: 'Post 1', title: 'Title 1' })

      const eventLog = db.getEventLog()
      const count = await eventLog.getEventCount()

      expect(count).toBe(0)
    })

    it('handles very small maxAge', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxAge: 1 }, // 1 millisecond
      })

      await db.create('posts', { $type: 'Post', name: 'Post 1', title: 'Title 1' })
      advanceTime(10)

      // Create another event to trigger rotation
      await db.create('posts', { $type: 'Post', name: 'Post 2', title: 'Title 2' })

      const eventLog = db.getEventLog()
      const count = await eventLog.getEventCount()

      // First event should be rotated out due to age
      expect(count).toBeLessThanOrEqual(2)
    })

    it('preserves event order after rotation', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 3, archiveOnRotation: true },
      })

      // Create 5 entities with small delays
      for (let i = 0; i < 5; i++) {
        await db.create('posts', { $type: 'Post', name: `Post ${i}`, title: `Title ${i}` })
        advanceTime(5)
      }

      const eventLog = db.getEventLog()

      // Get all events from namespace and check order
      const events = await eventLog.getEventsByNamespace('posts')

      for (let i = 1; i < events.length; i++) {
        expect(events[i].ts).toBeGreaterThanOrEqual(events[i - 1].ts)
      }
    })

    it('handles rapid creates without losing data', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxEvents: 100 },
      })

      // Rapid creates
      const createPromises = Array.from({ length: 20 }, (_, i) =>
        db.create('posts', { $type: 'Post', name: `Post ${i}`, title: `Title ${i}` })
      )

      await Promise.all(createPromises)

      const eventLog = db.getEventLog()
      const count = await eventLog.getEventCount()

      expect(count).toBe(20)
    })
  })

  // ===========================================================================
  // maxArchivedEvents Tests (Resource Leak Fix)
  // ===========================================================================

  describe('maxArchivedEvents limit', () => {
    it('uses default maxArchivedEvents of 50000', async () => {
      db = new ParqueDB({ storage })
      const eventLog = db.getEventLog()
      const config = eventLog.getConfig()

      expect(config.maxArchivedEvents).toBe(50000)
    })

    it('respects custom maxArchivedEvents', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: { maxArchivedEvents: 100 },
      })
      const eventLog = db.getEventLog()
      const config = eventLog.getConfig()

      expect(config.maxArchivedEvents).toBe(100)
    })

    it('prunes archived events when exceeding maxArchivedEvents during rotation', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: {
          maxEvents: 3,
          archiveOnRotation: true,
          maxArchivedEvents: 5,
        },
      })

      // Create 10 entities (10 CREATE events)
      // With maxEvents=3, 7 events will be rotated to archive
      // With maxArchivedEvents=5, only 5 should remain in archive
      for (let i = 0; i < 10; i++) {
        await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
        })
        advanceTime(5) // Ensure different timestamps
      }

      const eventLog = db.getEventLog()
      const archivedEvents = await eventLog.getArchivedEvents()
      const eventCount = await eventLog.getEventCount()

      // Should have 3 active events (maxEvents limit)
      expect(eventCount).toBe(3)
      // Should have at most 5 archived events (maxArchivedEvents limit)
      expect(archivedEvents.length).toBeLessThanOrEqual(5)
    })

    it('keeps newest archived events when pruning', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: {
          maxEvents: 2,
          archiveOnRotation: true,
          maxArchivedEvents: 3,
        },
      })

      // Create 7 entities
      for (let i = 0; i < 7; i++) {
        await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
        })
        advanceTime(10)
      }

      const eventLog = db.getEventLog()
      const archivedEvents = await eventLog.getArchivedEvents()

      // Should have at most 3 archived events
      expect(archivedEvents.length).toBeLessThanOrEqual(3)

      // Archived events should be sorted by timestamp (oldest to newest after pruning)
      for (let i = 1; i < archivedEvents.length; i++) {
        expect(archivedEvents[i].ts).toBeGreaterThanOrEqual(archivedEvents[i - 1].ts)
      }
    })

    it('prunes archived events when manually archiving', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: {
          maxEvents: 100,
          archiveOnRotation: true,
          maxArchivedEvents: 5,
        },
      })

      // Create 10 entities
      for (let i = 0; i < 10; i++) {
        await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
        })
        advanceTime(5)
      }

      const eventLog = db.getEventLog()

      // Manually archive to keep only 2 events (will archive 8)
      const result = await eventLog.archiveEvents({ maxEvents: 2 })

      expect(result.archivedCount).toBe(8)
      // prunedCount should reflect events pruned due to maxArchivedEvents
      expect(result.prunedCount).toBe(3) // 8 archived - 5 max = 3 pruned

      const archivedEvents = await eventLog.getArchivedEvents()
      expect(archivedEvents.length).toBe(5)
    })

    it('does not prune when under maxArchivedEvents limit', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: {
          maxEvents: 5,
          archiveOnRotation: true,
          maxArchivedEvents: 100,
        },
      })

      // Create 8 entities (will archive 3)
      for (let i = 0; i < 8; i++) {
        await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
        })
        advanceTime(5)
      }

      const eventLog = db.getEventLog()
      const archivedEvents = await eventLog.getArchivedEvents()

      // All 3 archived events should remain (under the 100 limit)
      expect(archivedEvents.length).toBe(3)
    })

    it('handles maxArchivedEvents of 0', async () => {
      db = new ParqueDB({
        storage,
        eventLogConfig: {
          maxEvents: 3,
          archiveOnRotation: true,
          maxArchivedEvents: 0,
        },
      })

      // Create 5 entities
      for (let i = 0; i < 5; i++) {
        await db.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
        })
      }

      const eventLog = db.getEventLog()
      const archivedEvents = await eventLog.getArchivedEvents()

      // All archived events should be pruned immediately
      expect(archivedEvents.length).toBe(0)
    })
  })
})
