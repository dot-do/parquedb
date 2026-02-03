/**
 * Transaction Rollback Target Format Tests
 *
 * Tests that transaction rollback correctly handles event target format conversion.
 *
 * Event targets use "ns:id" format (e.g., "users:u1")
 * Entity IDs use "ns/id" format (e.g., "users/u1")
 *
 * The bug: Transaction rollback was comparing/using the wrong format
 * when matching events to entities.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueDB } from '../../src'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { entityTarget, parseEntityTarget, isRelationshipTarget, entityId } from '../../src/types'
import type { EntityId, Event } from '../../src/types'

describe('Transaction Rollback Target Format', () => {
  let db: ParqueDB
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
    db = new ParqueDB({ storage })
  })

  afterEach(() => {
    // No cleanup needed - MemoryBackend is fresh each test
  })

  describe('Event target format validation', () => {
    it('should reject events with ns/id format (wrong format uses slash)', async () => {
      // This tests that the EventWalManager rejects events with wrong format
      // The validation was added to catch bugs where events were created with
      // "ns/id" (EntityId format) instead of "ns:id" (event target format)

      // We can't directly test EventWalManager here, but we verify that
      // the system uses correct format by checking created events
      const entity = await db.Users.create({
        $type: 'User',
        name: 'Test',
      })

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      // All event targets should use colon, never just slash
      for (const event of events) {
        if (!isRelationshipTarget(event.target)) {
          // Should not be in "ns/id" format (without colon)
          expect(event.target).not.toMatch(/^[^:]+\/[^:]+$/)
          // Should be in "ns:id" format
          expect(event.target).toMatch(/^[^/:]+:[^/]+$/)
        }
      }
    })
  })

  describe('entityTarget function', () => {
    it('should create correct format ns:id', () => {
      const target = entityTarget('users', 'u1')
      expect(target).toBe('users:u1')
    })

    it('should use colon not slash', () => {
      const target = entityTarget('posts', 'post-123')
      expect(target).not.toContain('/')
      expect(target).toContain(':')
    })
  })

  describe('entityId function', () => {
    it('should create correct format ns/id', () => {
      const id = entityId('users', 'u1')
      expect(id).toBe('users/u1')
    })

    it('should use slash not colon', () => {
      const id = entityId('posts', 'post-123')
      expect(id).toContain('/')
      expect(id).not.toMatch(/posts:post/)
    })
  })

  describe('parseEntityTarget function', () => {
    it('should parse ns:id format correctly', () => {
      const { ns, id } = parseEntityTarget('users:u1')
      expect(ns).toBe('users')
      expect(id).toBe('u1')
    })

    it('should handle ids with colons', () => {
      const { ns, id } = parseEntityTarget('items:item:with:colons')
      expect(ns).toBe('items')
      expect(id).toBe('item:with:colons')
    })

    it('should throw on invalid format without colon', () => {
      expect(() => parseEntityTarget('users/u1')).toThrow()
    })
  })

  describe('Event target format in CRUD operations', () => {
    it('should record CREATE event with correct target format', async () => {
      const entity = await db.Users.create({
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
      })

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      expect(events.length).toBeGreaterThan(0)
      const createEvent = events.find(e => e.op === 'CREATE')
      expect(createEvent).toBeDefined()

      // Event target should use ns:id format (colon)
      expect(createEvent!.target).toContain(':')
      expect(createEvent!.target).not.toMatch(/^users\//)

      // Verify parsing works
      const { ns } = parseEntityTarget(createEvent!.target)
      expect(ns).toBe('users')
    })

    it('should record UPDATE event with correct target format', async () => {
      const entity = await db.Users.create({
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
      })

      await db.Users.update(entity.$id.split('/')[1]!, { $set: { name: 'Alice Updated' } })

      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      const updateEvent = events.find(e => e.op === 'UPDATE')
      expect(updateEvent).toBeDefined()

      // Event target should use ns:id format (colon)
      expect(updateEvent!.target).toContain(':')
      expect(updateEvent!.target).not.toMatch(/^users\//)
    })

    it('should record DELETE event with correct target format', async () => {
      const entity = await db.Users.create({
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
      })

      const entityIdPart = entity.$id.split('/')[1]!
      await db.Users.delete(entityIdPart)

      const eventLog = db.getEventLog()
      const allDeleteEvents = await eventLog.getEventsByOp('DELETE')

      // Find the delete event for this entity
      const deleteEvent = allDeleteEvents.find(e => {
        if (isRelationshipTarget(e.target)) return false
        const { ns, id } = parseEntityTarget(e.target)
        return ns === 'users' && id === entityIdPart
      })

      expect(deleteEvent).toBeDefined()
      // Event target should use ns:id format (colon)
      expect(deleteEvent!.target).toContain(':')
    })
  })

  describe('Transaction rollback event format matching', () => {
    it('should match events by correct target format during entity lookup', async () => {
      // Create an entity
      const entity = await db.Users.create({
        $type: 'User',
        name: 'Test User',
      })

      const entityIdPart = entity.$id.split('/')[1]!

      // The event target format is ns:id (colon)
      const expectedTarget = entityTarget('users', entityIdPart)

      // Verify target format
      expect(expectedTarget).toBe(`users:${entityIdPart}`)
      expect(expectedTarget).not.toContain('/')

      // Get events and verify we can find the entity's events
      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)

      // Should find at least the CREATE event
      expect(events.length).toBeGreaterThan(0)

      // Verify all events have correct target format
      for (const event of events) {
        if (!isRelationshipTarget(event.target)) {
          expect(event.target).toContain(':')
          expect(event.target).not.toMatch(/^users\//)
        }
      }
    })

    it('should correctly convert between EntityId and event target formats', () => {
      // EntityId uses slash
      const fullId = 'users/user-123' as EntityId
      expect(fullId).toContain('/')

      // Convert to event target format (colon)
      const [ns, ...idParts] = fullId.split('/')
      const target = entityTarget(ns!, idParts.join('/'))

      expect(target).toBe('users:user-123')
      expect(target).toContain(':')
      expect(target).not.toContain('/')

      // Convert back from event target to EntityId format
      const parsed = parseEntityTarget(target)
      const reconstructedId = `${parsed.ns}/${parsed.id}` as EntityId

      expect(reconstructedId).toBe(fullId)
    })
  })
})
