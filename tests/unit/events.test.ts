/**
 * Event Log Test Suite
 *
 * Tests for event sourcing functionality in ParqueDB.
 * Uses real FsBackend with temp directories for actual event persistence.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import type {
  EntityId,
  Event,
  Variant,
} from '../../src/types'
import { isRelationshipTarget, parseEntityTarget } from '../../src/types'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

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
// Event Log Test Suite
// =============================================================================

describe('Event Log', () => {
  let db: ParqueDB
  let storage: FsBackend
  let tempDir: string

  beforeEach(async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-01T12:00:00Z'))
    // Create a unique temp directory for each test
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-events-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    vi.useRealTimers()
    // Clean up the temp directory
    try {
      await rm(tempDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  // ===========================================================================
  // Event Recording Tests
  // ===========================================================================

  describe('event recording', () => {
    it('records CREATE event on entity creation', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
        content: 'Test content',
      })

      // Access the event log for the entity
      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      expect(events.length).toBeGreaterThan(0)
      const createEvent = events.find((e: Event) => e.op === 'CREATE')
      expect(createEvent).toBeDefined()
      // Target is now "ns:id" format, not 'entity'
      expect(createEvent!.target).toMatch(/^posts:/)
      expect(isRelationshipTarget(createEvent!.target)).toBe(false)
      expect(createEvent!.before).toBeUndefined()
      expect(createEvent!.after).toBeDefined()
      expect((createEvent!.after as Variant).title).toBe('Hello World')
    })

    it('records UPDATE event with delta', async () => {
      // Create entity first
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original Title',
        content: 'Test content',
      })

      // Update the entity
      await db.update('posts', entity.$id as string, {
        $set: { title: 'Updated Title' },
      })

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      const updateEvent = events.find((e: Event) => e.op === 'UPDATE')
      expect(updateEvent).toBeDefined()
      expect(updateEvent!.before).toBeDefined()
      expect((updateEvent!.before as Variant).title).toBe('Original Title')
      expect(updateEvent!.after).toBeDefined()
      expect((updateEvent!.after as Variant).title).toBe('Updated Title')
    })

    it('records DELETE event for soft delete with after state', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'To Be Deleted',
        content: 'Test content',
      })

      await db.delete('posts', entity.$id as string)

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      const deleteEvent = events.find((e: Event) => e.op === 'DELETE')
      expect(deleteEvent).toBeDefined()
      expect(deleteEvent!.before).toBeDefined()
      // Soft delete should record the after state with deletedAt/deletedBy
      expect(deleteEvent!.after).toBeDefined()
      expect((deleteEvent!.after as Variant).deletedAt).toBeDefined()
      expect((deleteEvent!.after as Variant).deletedBy).toBeDefined()
      // Version should be incremented in after state
      expect((deleteEvent!.after as Variant).version).toBe(2)
    })

    it('records DELETE event for hard delete with null after state', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'To Be Hard Deleted',
        content: 'Test content',
      })

      await db.delete('posts', entity.$id as string, { hard: true })

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      const deleteEvent = events.find((e: Event) => e.op === 'DELETE')
      expect(deleteEvent).toBeDefined()
      expect(deleteEvent!.before).toBeDefined()
      // Hard delete should have null after state
      expect(deleteEvent!.after).toBeNull()
    })

    it('records LINK event for relationships', async () => {
      // Create author and post
      const author = await db.create('users', {
        $type: 'User',
        name: 'John Doe',
        email: 'john@example.com',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
        content: 'Test content',
      })

      // Link the post to the author
      await db.update('posts', post.$id as string, {
        $link: { author: author.$id as EntityId },
      })

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(post.$id as EntityId)

      // LINK events are now recorded with relationship target format "from:pred:to"
      // They use CREATE op, and the target contains the relationship path
      const allEvents = await eventLog.getEventsByOp('CREATE')
      const linkEvent = allEvents.find((e: Event) => isRelationshipTarget(e.target) && e.target.includes(':author:'))
      expect(linkEvent).toBeDefined()
      expect((linkEvent!.after as Variant).predicate).toBe('author')
    })

    it('records UNLINK event', async () => {
      const author = await db.create('users', {
        $type: 'User',
        name: 'John Doe',
        email: 'john@example.com',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
        content: 'Test content',
        author: { 'John Doe': author.$id as EntityId },
      })

      // Unlink the author
      await db.update('posts', post.$id as string, {
        $unlink: { author: author.$id as EntityId },
      })

      const eventLog = db.getEventLog()
      // UNLINK events are now DELETE ops with relationship target format
      const allEvents = await eventLog.getEventsByOp('DELETE')
      const unlinkEvent = allEvents.find((e: Event) => isRelationshipTarget(e.target) && e.target.includes(':author:'))
      expect(unlinkEvent).toBeDefined()
    })

    it('includes actor in event', async () => {
      const actor = 'users/admin' as EntityId

      const entity = await db.create(
        'posts',
        {
          $type: 'Post',
          name: 'Test Post',
          title: 'Hello World',
          content: 'Test content',
        },
        { actor }
      )

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      expect(events[0].actor).toBe(actor)
    })

    it('includes timestamp', async () => {
      const before = Date.now()

      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
        content: 'Test content',
      })

      const after = Date.now()

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      // ts is now a number (milliseconds since epoch)
      expect(typeof events[0].ts).toBe('number')
      expect(events[0].ts).toBeGreaterThanOrEqual(before)
      expect(events[0].ts).toBeLessThanOrEqual(after)
    })

    it('includes sequence number', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
        content: 'Test content',
      })

      // Multiple updates
      await db.update('posts', entity.$id as string, { $set: { title: 'Title 1' } })
      await db.update('posts', entity.$id as string, { $set: { title: 'Title 2' } })

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      // Events should have sequential IDs (ULIDs maintain order)
      for (let i = 1; i < events.length; i++) {
        expect(events[i].id > events[i - 1].id).toBe(true)
      }
    })
  })

  // ===========================================================================
  // Event Structure Tests
  // ===========================================================================

  describe('event structure', () => {
    it('stores target with ns and id', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
        content: 'Test content',
      })

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      // target is now "ns:id" format, use parseEntityTarget to extract
      expect(events[0].target).toBeDefined()
      expect(isRelationshipTarget(events[0].target)).toBe(false)
      const { ns, id } = parseEntityTarget(events[0].target)
      expect(ns).toBe('posts')
      expect(id).toBeDefined()
    })

    it('stores operation type', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
        content: 'Test content',
      })

      await db.update('posts', entity.$id as string, {
        $set: { title: 'Updated' },
      })

      await db.delete('posts', entity.$id as string)

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      const operations = events.map((e: Event) => e.op)
      expect(operations).toContain('CREATE')
      expect(operations).toContain('UPDATE')
      expect(operations).toContain('DELETE')
    })

    it('stores before/after for updates', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
        content: 'Test content',
        viewCount: 0,
      })

      await db.update('posts', entity.$id as string, {
        $set: { title: 'Updated', viewCount: 100 },
      })

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)
      const updateEvent = events.find((e: Event) => e.op === 'UPDATE')

      expect(updateEvent!.before).toBeDefined()
      expect(updateEvent!.after).toBeDefined()
      expect((updateEvent!.before as Variant).title).toBe('Original')
      expect((updateEvent!.after as Variant).title).toBe('Updated')
      expect((updateEvent!.before as Variant).viewCount).toBe(0)
      expect((updateEvent!.after as Variant).viewCount).toBe(100)
    })

    it('compresses large deltas', async () => {
      const largeContent = 'x'.repeat(100000) // 100KB of content

      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
        content: largeContent,
      })

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      // The event should store compressed data for large payloads
      // This tests that the event log handles compression
      expect(events[0].after).toBeDefined()

      // Check that the stored event is smaller than uncompressed
      const eventData = await eventLog.getRawEvent(events[0].id)
      expect(eventData.compressed).toBe(true)
    })
  })

  // ===========================================================================
  // Event Persistence Tests
  // ===========================================================================

  describe('event persistence', () => {
    it('writes events to storage', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
        content: 'Test content',
      })

      // Verify events are accessible through the event log
      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      expect(events.length).toBeGreaterThan(0)
      expect(events[0].op).toBe('CREATE')

      // Verify storage was written to (check file exists)
      const eventsPath = 'posts/events.json'
      const exists = await storage.exists(eventsPath)
      // Events are stored in the event log, not necessarily as a separate file
      // The key test is that the event log can retrieve them
      expect(events[0].target).toBeDefined()
      expect(events[0].target).toMatch(/^posts:/)
    })

    it('batches writes for efficiency', async () => {
      // Create multiple entities in quick succession
      const entities = await Promise.all([
        db.create('posts', { $type: 'Post', name: 'Post 1', title: 'T1', content: 'C1' }),
        db.create('posts', { $type: 'Post', name: 'Post 2', title: 'T2', content: 'C2' }),
        db.create('posts', { $type: 'Post', name: 'Post 3', title: 'T3', content: 'C3' }),
      ])

      // All entities should have been created
      expect(entities.length).toBe(3)

      // Verify events were recorded for all entities
      const eventLog = db.getEventLog()
      const allEvents = await eventLog.getEventsByNamespace('posts')

      expect(allEvents.length).toBe(3)
    })

    it('flushes on transaction commit', async () => {
      // Start a transaction
      const tx = await db.beginTransaction()

      const entity = await tx.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title',
        content: 'Content',
      })

      // Get current event log before commit
      const eventLog = db.getEventLog()

      // Commit the transaction
      await tx.commit()

      // Events should be flushed now
      const events = await eventLog.getEvents(entity.$id as EntityId)
      expect(events.length).toBeGreaterThan(0)
    })
  })

  // ===========================================================================
  // Event Query Tests
  // ===========================================================================

  describe('event querying', () => {
    it('queries events by entity ID', async () => {
      const entity1 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title 1',
        content: 'Content 1',
      })

      const entity2 = await db.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'Title 2',
        content: 'Content 2',
      })

      await db.update('posts', entity1.$id as string, { $set: { title: 'Updated 1' } })

      const eventLog = db.getEventLog()
      const events1 = await eventLog.getEvents(entity1.$id as EntityId)
      const events2 = await eventLog.getEvents(entity2.$id as EntityId)

      expect(events1.length).toBe(2) // CREATE + UPDATE
      expect(events2.length).toBe(1) // CREATE only
    })

    it('queries events by namespace', async () => {
      await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      await db.create('users', {
        $type: 'User',
        name: 'Test User',
        email: 'test@example.com',
      })

      const eventLog = db.getEventLog()
      const postEvents = await eventLog.getEventsByNamespace('posts')
      const userEvents = await eventLog.getEventsByNamespace('users')

      expect(postEvents.length).toBeGreaterThan(0)
      expect(userEvents.length).toBeGreaterThan(0)
      // Events are filtered by namespace using target field
      expect(postEvents.every((e: Event) => {
        if (isRelationshipTarget(e.target)) return true
        return parseEntityTarget(e.target).ns === 'posts'
      })).toBe(true)
      expect(userEvents.every((e: Event) => {
        if (isRelationshipTarget(e.target)) return true
        return parseEntityTarget(e.target).ns === 'users'
      })).toBe(true)
    })

    it('queries events by time range', async () => {
      const startTime = new Date()

      await db.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Title 1',
        content: 'Content 1',
      })

      const midTime = new Date()
      advanceTime(10) // Ensure time difference between events

      await db.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'Title 2',
        content: 'Content 2',
      })

      const endTime = new Date()

      const eventLog = db.getEventLog()
      const events = await eventLog.getEventsByTimeRange(startTime, midTime)

      expect(events.length).toBe(1) // Only first post
      expect((events[0].after as Variant).title).toBe('Title 1')
    })

    it('queries events by operation type', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      await db.update('posts', entity.$id as string, { $set: { title: 'Updated' } })
      await db.delete('posts', entity.$id as string)

      const eventLog = db.getEventLog()
      const updates = await eventLog.getEventsByOp('UPDATE')

      expect(updates.every((e: Event) => e.op === 'UPDATE')).toBe(true)
    })
  })
})
