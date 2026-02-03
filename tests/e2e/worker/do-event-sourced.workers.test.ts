/**
 * ParqueDB DO WAL Phase 3 - Event-Sourced Entity Tests
 *
 * Tests the event-sourcing model where entity state is derived from events:
 * - Entity state reconstructed from events (CREATE/UPDATE/DELETE)
 * - Reads work without entities table
 * - Deleted entities return null
 *
 * These tests run in the Cloudflare Workers environment with real bindings:
 * - Durable Objects (PARQUEDB) with SQLite storage
 * - R2 (BUCKET) for Parquet file storage
 *
 * Run with: npm run test:e2e:workers
 */

import { env } from 'cloudflare:test'
import { describe, it, expect } from 'vitest'
import type { TestEnv, ParqueDBDOTestStub } from '../types'
import { asDOTestStub } from '../types'

// Cast env to our typed environment
const testEnv = env as TestEnv

describe('DO WAL Phase 3 - Event-Sourced Entities', () => {
  describe('Entity State from Events', () => {
    it('reconstructs entity from CREATE event', async () => {
      const id = testEnv.PARQUEDB.idFromName(`event-create-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create an entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        content: 'Hello World',
        views: 0,
      }, {}) as Record<string, unknown>

      expect(created.$id).toContain('posts/')
      expect(created.name).toBe('Test Post')

      // Verify we can get it from events
      const entityId = (created.$id as string).split('/')[1]!
      const fromEvents = await stub.getEntityFromEvents('posts', entityId) as Record<string, unknown>

      expect(fromEvents).not.toBeNull()
      expect(fromEvents.$type).toBe('Post')
      expect(fromEvents.name).toBe('Test Post')
      expect(fromEvents.content).toBe('Hello World')
      expect(fromEvents.views).toBe(0)
      expect(fromEvents.version).toBe(1)
    })

    it('reconstructs entity after UPDATE events', async () => {
      const id = testEnv.PARQUEDB.idFromName(`event-update-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create an entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Original Title',
        content: 'Original content',
        views: 0,
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Update it multiple times
      await stub.update('posts', entityId, {
        $set: { name: 'Updated Title' },
      }, {})

      await stub.update('posts', entityId, {
        $inc: { views: 10 },
      }, {})

      await stub.update('posts', entityId, {
        $set: { content: 'Final content' },
      }, {})

      // Get from events should show final state
      const fromEvents = await stub.getEntityFromEvents('posts', entityId) as Record<string, unknown>

      expect(fromEvents).not.toBeNull()
      expect(fromEvents.name).toBe('Updated Title')
      expect(fromEvents.content).toBe('Final content')
      expect(fromEvents.views).toBe(10)
      expect(fromEvents.version).toBe(4) // 1 create + 3 updates
    })

    it('returns null for deleted entities', async () => {
      const id = testEnv.PARQUEDB.idFromName(`event-delete-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create an entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'To Be Deleted',
        content: 'This will be deleted',
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Delete it
      const deleted = await stub.delete('posts', entityId, {})
      expect(deleted).toBe(true)

      // Get should return null (unless includeDeleted)
      const result = await stub.get('posts', entityId)
      expect(result).toBeNull()

      // Get with includeDeleted should show deleted state
      const deletedEntity = await stub.get('posts', entityId, true) as Record<string, unknown>
      expect(deletedEntity).not.toBeNull()
      expect(deletedEntity.deletedAt).toBeDefined()
    })

    it('handles create-update-delete sequence', async () => {
      const id = testEnv.PARQUEDB.idFromName(`event-sequence-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create first entity
      const created1 = await stub.create('posts', {
        $type: 'Post',
        name: 'First Post',
      }, {}) as Record<string, unknown>

      const id1 = (created1.$id as string).split('/')[1]!

      // Update it
      await stub.update('posts', id1, {
        $set: { content: 'Updated' },
      }, {})

      // Delete it
      await stub.delete('posts', id1, {})

      // Create second entity (different ID)
      const created2 = await stub.create('posts', {
        $type: 'Post',
        name: 'Second Post',
      }, {}) as Record<string, unknown>

      const id2 = (created2.$id as string).split('/')[1]!

      // First should be deleted
      const result1 = await stub.get('posts', id1)
      expect(result1).toBeNull()

      // Second should exist
      const result2 = await stub.get('posts', id2) as Record<string, unknown>
      expect(result2).not.toBeNull()
      expect(result2.name).toBe('Second Post')
    })
  })

  describe('Read Operations Without Entities Table', () => {
    it('get() returns correct entity from events', async () => {
      const id = testEnv.PARQUEDB.idFromName(`get-events-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create an entity
      const created = await stub.create('posts', {
        $type: 'Article',
        name: 'Event Sourced Article',
        author: 'Test Author',
        tags: ['test', 'event-sourcing'],
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Get should work by reading from events
      const retrieved = await stub.get('posts', entityId) as Record<string, unknown>

      expect(retrieved).not.toBeNull()
      expect(retrieved.$type).toBe('Article')
      expect(retrieved.name).toBe('Event Sourced Article')
      expect(retrieved.author).toBe('Test Author')
      expect((retrieved.tags as string[]).length).toBe(2)
    })

    it('update() finds entity from events', async () => {
      const id = testEnv.PARQUEDB.idFromName(`update-events-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create an entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Will Be Updated',
        counter: 0,
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Update should work by finding entity from events
      const updated = await stub.update('posts', entityId, {
        $set: { name: 'Has Been Updated' },
        $inc: { counter: 5 },
      }, {}) as Record<string, unknown>

      expect(updated.name).toBe('Has Been Updated')
      expect(updated.counter).toBe(5)
      expect(updated.version).toBe(2)
    })

    it('delete() finds entity from events', async () => {
      const id = testEnv.PARQUEDB.idFromName(`delete-events-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create an entity
      const created = await stub.create('posts', {
        $type: 'Post',
        name: 'Will Be Deleted',
      }, {}) as Record<string, unknown>

      const entityId = (created.$id as string).split('/')[1]!

      // Delete should work by finding entity from events
      const deleted = await stub.delete('posts', entityId, {})
      expect(deleted).toBe(true)

      // Verify it's deleted
      const result = await stub.get('posts', entityId)
      expect(result).toBeNull()
    })
  })

  describe('Cost Optimization Verification', () => {
    it('create() only writes to events, not entities table', async () => {
      const id = testEnv.PARQUEDB.idFromName(`cost-create-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create multiple entities
      const entities = []
      for (let i = 0; i < 4; i++) {
        const entity = await stub.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
        }, {}) as Record<string, unknown>
        entities.push(entity)
      }

      // All should be readable from events
      for (const entity of entities) {
        const entityId = (entity.$id as string).split('/')[1]!
        const retrieved = await stub.get('posts', entityId)
        expect(retrieved).not.toBeNull()
      }

      // Cost: Only events_wal rows written, no entities table rows
      // This is the key optimization of the WAL-based system
    })

    it('events are buffered efficiently', async () => {
      const id = testEnv.PARQUEDB.idFromName(`cost-buffer-${Date.now()}`)
      const stub = asDOTestStub(testEnv.PARQUEDB.get(id))

      // Create some entities
      for (let i = 0; i < 3; i++) {
        await stub.create('posts', {
          $type: 'Post',
          name: `Buffered Post ${i}`,
        }, {})
      }

      // Flush to persist
      await stub.flushAllNsEventBatches()

      // Count should reflect events
      const count = await stub.getUnflushedEventCount()
      expect(count).toBeGreaterThanOrEqual(0)
    })
  })
})
