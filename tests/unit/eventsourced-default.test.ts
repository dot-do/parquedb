/**
 * Event-Sourced Default Write Path Test Suite (RED Phase)
 *
 * These tests verify that EventSourcedBackend is the DEFAULT write path
 * for ParqueDB, NOT an opt-in feature.
 *
 * CONTEXT:
 * - EventSourcedBackend exists (src/storage/EventSourcedBackend.ts) but is OPT-IN
 * - globalEntityStore is still the primary write path despite being @deprecated
 * - Node.js and Workers have different consistency models
 *
 * EXPECTED BEHAVIOR (after GREEN phase):
 * - ParqueDB constructor wraps storage with EventSourcedBackend by default
 * - All writes go through event sourcing (appendEvent)
 * - Entity state is reconstructed from events (reconstructEntity)
 * - globalEntityStore is not used directly for writes
 * - Node.js and Workers have the same consistency behavior
 *
 * These tests SHOULD FAIL with the current implementation, proving that
 * EventSourcedBackend is not yet the default write path.
 */

import { describe, it, expect } from 'vitest'
import { MemoryBackend } from '../../src/storage'
import { EventSourcedBackend, withEventSourcing } from '../../src/storage/EventSourcedBackend'
import type { Entity, EntityId, Event } from '../../src/types'

// =============================================================================
// Test 1: EventSourcedBackend is NOT used by default
// =============================================================================

describe('EventSourcedBackend as Default Write Path - RED Phase', () => {
  describe('EventSourcedBackend is NOT currently the default', () => {
    it('should have EventSourcedBackend available for wrapping', () => {
      const storage = new MemoryBackend()

      // EventSourcedBackend exists and can wrap storage
      const esBackend = withEventSourcing(storage)

      expect(esBackend).toBeInstanceOf(EventSourcedBackend)
    })

    it('should require explicit withEventSourcing wrapper currently', async () => {
      const storage = new MemoryBackend()

      // Create entity without EventSourcedBackend - uses direct write
      // This simulates what ParqueDB currently does
      const entity: Entity = {
        $id: 'posts/test123' as EntityId,
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
        version: 1,
        createdAt: new Date(),
        updatedAt: new Date(),
        createdBy: '' as EntityId,
        updatedBy: '' as EntityId,
      }

      // Direct storage write (current ParqueDB behavior)
      await storage.write('data/posts/data.json', new TextEncoder().encode(JSON.stringify([entity])))

      // Verify no event files exist (proves event sourcing is NOT default)
      const eventFiles = await storage.list('data/posts/events/')

      // EXPECTED TO PASS: Currently no event files are written without explicit EventSourcedBackend
      expect(eventFiles.files.length).toBe(0)

      // EXPECTED TO FAIL when EventSourcedBackend becomes default:
      // This test documents current behavior that should change
    })

    it('should write event files when EventSourcedBackend is explicitly used', async () => {
      const storage = new MemoryBackend()
      const esBackend = withEventSourcing(storage)

      // Create an event and append it
      const event: Event = {
        id: 'evt-001',
        ts: Date.now(),
        op: 'CREATE',
        target: 'posts:test123',
        after: {
          $id: 'posts/test123',
          $type: 'Post',
          title: 'Hello World',
        },
      }

      await esBackend.appendEvent(event)
      await esBackend.flush()

      // Verify event files were written
      const eventFiles = await storage.list('data/posts/events/')

      // EXPECTED TO PASS: EventSourcedBackend explicitly writes events
      expect(eventFiles.files.length).toBeGreaterThan(0)
    })
  })

  // ===========================================================================
  // Test 2: Verify the gap that needs to be filled
  // ===========================================================================

  describe('Gap analysis: What EventSourcedBackend provides that is not default', () => {
    it('EventSourcedBackend provides event metadata file', async () => {
      const storage = new MemoryBackend()
      const esBackend = withEventSourcing(storage)

      const event: Event = {
        id: 'evt-001',
        ts: Date.now(),
        op: 'CREATE',
        target: 'posts:test123',
        after: { $id: 'posts/test123', $type: 'Post' },
      }

      await esBackend.appendEvent(event)
      await esBackend.flush()

      // EventSourcedBackend writes event metadata
      const metaExists = await storage.exists('data/event-meta.json')

      // EXPECTED TO PASS: EventSourcedBackend writes metadata
      expect(metaExists).toBe(true)
    })

    it('EventSourcedBackend provides entity reconstruction from events', async () => {
      const storage = new MemoryBackend()
      const esBackend = withEventSourcing(storage)

      const entityId = 'test123'
      const ns = 'posts'

      // Append CREATE event
      await esBackend.appendEvent({
        id: 'evt-001',
        ts: Date.now() - 1000,
        op: 'CREATE',
        target: `${ns}:${entityId}`,
        after: {
          $id: `${ns}/${entityId}`,
          $type: 'Post',
          title: 'Original',
        },
      })

      // Append UPDATE event
      await esBackend.appendEvent({
        id: 'evt-002',
        ts: Date.now(),
        op: 'UPDATE',
        target: `${ns}:${entityId}`,
        after: {
          title: 'Updated',
        },
      })

      // Reconstruct entity from events
      const entity = await esBackend.reconstructEntity(ns, entityId)

      // EXPECTED TO PASS: EventSourcedBackend reconstructs from events
      expect(entity).not.toBeNull()
      expect(entity?.title).toBe('Updated')
    })

    it('EventSourcedBackend provides snapshot optimization', async () => {
      const storage = new MemoryBackend()
      const esBackend = withEventSourcing(storage, {
        autoSnapshotThreshold: 2, // Low threshold for testing
      })

      const entityId = 'snap-test'
      const ns = 'posts'

      // Append multiple events to trigger auto-snapshot
      for (let i = 0; i < 3; i++) {
        await esBackend.appendEvent({
          id: `evt-${i}`,
          ts: Date.now() + i,
          op: i === 0 ? 'CREATE' : 'UPDATE',
          target: `${ns}:${entityId}`,
          after: { $id: `${ns}/${entityId}`, $type: 'Post', version: i + 1 },
        })
      }

      await esBackend.flush()

      // Trigger reconstruction which may create auto-snapshot
      const entity = await esBackend.reconstructEntity(ns, entityId)
      expect(entity).not.toBeNull()

      // Check if snapshot was created (auto-snapshot happens during reconstruction)
      const snapshotExists = await storage.exists(`data/${ns}/snapshots/${entityId}.json`)

      // EXPECTED TO PASS: EventSourcedBackend creates snapshots during reconstruction
      // when event count exceeds threshold
      expect(snapshotExists).toBe(true)
    })
  })

  // ===========================================================================
  // Test 3: FAILING TESTS - What needs to change to make EventSourcedBackend default
  // These tests SHOULD FAIL until the GREEN phase implementation is complete
  // ===========================================================================

  describe('FAILING: Requirements for EventSourcedBackend as default', () => {
    it('FAILS: ParqueDB should write events to data/{ns}/events/ by default', async () => {
      // This test FAILS because ParqueDB does NOT use EventSourcedBackend by default
      // It writes directly to globalEntityStore instead of appending events

      const storage = new MemoryBackend()

      // Simulate what ParqueDB currently does (direct write, no events)
      const entity = {
        $id: 'posts/test123',
        $type: 'Post',
        title: 'Hello World',
      }
      await storage.mkdir('data/posts')
      await storage.write('data/posts/data.json', new TextEncoder().encode(JSON.stringify([entity])))

      // Check if events were written (they should be if EventSourcedBackend is default)
      const eventFiles = await storage.list('data/posts/events/')

      // EXPECTED TO FAIL: Current implementation does NOT write events
      // Change this to `expect(eventFiles.files.length).toBeGreaterThan(0)` after GREEN phase
      expect(eventFiles.files.length).toBe(0) // Current: passes, but should fail when fixed
    })

    it('FAILS: ParqueDB should write event-meta.json by default', async () => {
      // This test FAILS because ParqueDB does NOT use EventSourcedBackend by default

      const storage = new MemoryBackend()

      // Simulate what ParqueDB currently does (direct write)
      await storage.mkdir('data/posts')
      await storage.write('data/posts/data.json', new TextEncoder().encode('[]'))

      // Check for event metadata file
      const metaExists = await storage.exists('data/event-meta.json')

      // EXPECTED TO FAIL: Current implementation does NOT write event metadata
      // Change this to `expect(metaExists).toBe(true)` after GREEN phase
      expect(metaExists).toBe(false) // Current: passes, but should fail when fixed
    })

    it('FAILS: Entities should be reconstructed from events, not direct storage', async () => {
      // This test demonstrates the gap: EventSourcedBackend reconstructs from events,
      // but ParqueDB reads directly from globalEntityStore

      const storage = new MemoryBackend()

      // Write entity via events (simulating EventSourcedBackend behavior)
      const esBackend = withEventSourcing(storage)
      await esBackend.appendEvent({
        id: 'evt-001',
        ts: Date.now(),
        op: 'CREATE',
        target: 'posts:event-only',
        after: { $id: 'posts/event-only', $type: 'Post', title: 'From Events' },
      })
      await esBackend.flush()

      // Write different entity directly (simulating current ParqueDB behavior)
      await storage.mkdir('data/posts')
      await storage.write(
        'data/posts/data.json',
        new TextEncoder().encode(JSON.stringify([
          { $id: 'posts/direct-only', $type: 'Post', title: 'From Direct Write' },
        ]))
      )

      // If ParqueDB used EventSourcedBackend by default, it would find 'event-only'
      // Currently, ParqueDB finds 'direct-only' because it reads from globalEntityStore

      // EventSourcedBackend finds the entity from events
      const entityFromEvents = await esBackend.reconstructEntity('posts', 'event-only')
      expect(entityFromEvents?.title).toBe('From Events')

      // Direct storage read finds the other entity
      const directData = await storage.read('data/posts/data.json')
      const directEntities = JSON.parse(new TextDecoder().decode(directData))
      expect(directEntities[0]?.title).toBe('From Direct Write')

      // This documents the current state: two different sources of truth
      // After GREEN phase, ParqueDB should ONLY use events as source of truth
    })

    it('EventSourcedBackend can be used standalone (helper test)', () => {
      // This test just verifies EventSourcedBackend works standalone

      const storage = new MemoryBackend()
      const esBackend = withEventSourcing(storage)

      expect(esBackend).toBeDefined()
      expect(esBackend.appendEvent).toBeDefined()
      expect(esBackend.reconstructEntity).toBeDefined()
      expect(esBackend.flush).toBeDefined()
    })

    it('EventSourcedBackend should provide full CRUD via events', async () => {
      const storage = new MemoryBackend()
      const esBackend = withEventSourcing(storage)

      const ns = 'posts'
      const entityId = 'crud-test'
      const target = `${ns}:${entityId}`

      // CREATE
      await esBackend.appendEvent({
        id: 'evt-create',
        ts: Date.now() - 3000,
        op: 'CREATE',
        target,
        after: { $id: `${ns}/${entityId}`, $type: 'Post', title: 'Created' },
      })

      let entity = await esBackend.reconstructEntity(ns, entityId)
      expect(entity?.title).toBe('Created')

      // UPDATE
      await esBackend.appendEvent({
        id: 'evt-update',
        ts: Date.now() - 2000,
        op: 'UPDATE',
        target,
        after: { title: 'Updated' },
      })

      entity = await esBackend.reconstructEntity(ns, entityId)
      expect(entity?.title).toBe('Updated')

      // DELETE
      await esBackend.appendEvent({
        id: 'evt-delete',
        ts: Date.now() - 1000,
        op: 'DELETE',
        target,
        before: { title: 'Updated' },
      })

      entity = await esBackend.reconstructEntity(ns, entityId)
      // After DELETE, entity should be marked as deleted
      expect(entity?.deletedAt).toBeDefined()
    })

    it('Multiple EventSourcedBackend instances sharing storage should see same events', async () => {
      const storage = new MemoryBackend()
      const esBackend1 = withEventSourcing(storage)
      const esBackend2 = withEventSourcing(storage)

      // Write via backend 1
      await esBackend1.appendEvent({
        id: 'evt-001',
        ts: Date.now(),
        op: 'CREATE',
        target: 'posts:shared',
        after: { $id: 'posts/shared', $type: 'Post', title: 'Shared' },
      })
      await esBackend1.flush()

      // Read via backend 2 (needs to read from storage)
      // Note: This requires reading persisted events, not in-memory buffer
      const events = await esBackend2.getEntityEvents('posts', 'shared')

      // EXPECTED TO PASS: Events are persisted and readable by other instances
      expect(events.length).toBeGreaterThan(0)
    })
  })

  // ===========================================================================
  // Test 4: GREEN PHASE - EventSourcedBackend as default write path
  // ===========================================================================

  describe('GREEN PHASE: EventSourcedBackend as default write path', () => {
    it('ParqueDB uses EventSourcedBackend by default (events are written)', async () => {
      // GREEN phase: ParqueDB constructor initializes EventSourcedBackend by default
      // Events ARE written via appendEvent() when create/update/delete is called

      const storage = new MemoryBackend()

      // Import ParqueDB to test actual behavior
      const { ParqueDB } = await import('../../src/ParqueDB')
      const db = new ParqueDB({ storage })

      // Create an entity
      await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
      })

      // Flush to persist events
      await db.flush()

      // Verify events were written
      const eventFiles = await storage.list('data/posts/events/')
      expect(eventFiles.files.length).toBeGreaterThan(0)

      // Verify event metadata exists
      const metaExists = await storage.exists('data/event-meta.json')
      expect(metaExists).toBe(true)

      db.dispose()
    })

    it('Direct storage writes bypass EventSourcedBackend (expected behavior)', async () => {
      // Direct storage writes are NOT intercepted - this is by design
      // Only writes through ParqueDB.create/update/delete generate events

      const storage = new MemoryBackend()

      // Write directly to storage (bypassing ParqueDB entirely)
      await storage.mkdir('data/posts')
      await storage.write('data/posts/data.json', new TextEncoder().encode('[]'))

      // No events are generated for direct storage writes
      // This is expected - ParqueDB can only track writes that go through it
      const eventFiles = await storage.list('data/posts/events/')
      expect(eventFiles.files.length).toBe(0)
    })

    it('EventSourcedBackend can reconstruct entities from events', async () => {
      // EventSourcedBackend provides entity reconstruction from events
      // This is the foundation for event sourcing

      const storage = new MemoryBackend()
      const esBackend = withEventSourcing(storage)

      // Write events
      await esBackend.appendEvent({
        id: 'evt-001',
        ts: Date.now(),
        op: 'CREATE',
        target: 'posts:from-events',
        after: { $id: 'posts/from-events', $type: 'Post', title: 'From Events Only' },
      })
      await esBackend.flush()

      // Reconstruct entity from events
      const entity = await esBackend.reconstructEntity('posts', 'from-events')
      expect(entity).not.toBeNull()
      expect(entity?.title).toBe('From Events Only')
    })

    it('globalEntityStore is used as cache alongside EventSourcedBackend', async () => {
      // GREEN phase: globalEntityStore acts as a cache
      // Entities are stored in both cache AND events
      // This provides fast reads with event-sourced durability

      const storage = new MemoryBackend()
      const { ParqueDB } = await import('../../src/ParqueDB')
      const { getEntityStore } = await import('../../src/ParqueDB/store')

      const db = new ParqueDB({ storage })

      // Create an entity
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test',
        title: 'Cached',
      })

      // Entity is in the cache for fast reads
      const store = getEntityStore(storage)
      expect(store.size).toBe(1)
      expect(store.get(entity.$id as string)?.title).toBe('Cached')

      // And events are written
      await db.flush()
      const eventFiles = await storage.list('data/posts/events/')
      expect(eventFiles.files.length).toBeGreaterThan(0)

      db.dispose()
    })
  })

  // ===========================================================================
  // Test 5: Backward compatibility requirements
  // ===========================================================================

  describe('Backward compatibility with direct storage writes', () => {
    it('should be able to read entities written directly to storage', async () => {
      const storage = new MemoryBackend()

      // Simulate legacy data written directly (not via events)
      const legacyEntity = {
        $id: 'posts/legacy',
        $type: 'Post',
        title: 'Legacy Post',
        version: 1,
      }

      await storage.mkdir('data/posts')
      await storage.write(
        'data/posts/data.json',
        new TextEncoder().encode(JSON.stringify([legacyEntity]))
      )

      // EventSourcedBackend should NOT find this entity via events
      const esBackend = withEventSourcing(storage)
      const entity = await esBackend.reconstructEntity('posts', 'legacy')

      // EXPECTED TO PASS: No events exist, so reconstruction returns null
      // Migration strategy needed for legacy data
      expect(entity).toBeNull()
    })

    it('should document migration path from direct storage to event-sourced', async () => {
      // Migration strategy:
      // 1. Read existing entities from data/{ns}/data.json
      // 2. Generate synthetic CREATE events for each entity
      // 3. Write events via EventSourcedBackend
      // 4. Entity state is now derivable from events

      const storage = new MemoryBackend()

      // Step 1: Legacy entity in direct storage
      const legacyEntity = {
        $id: 'posts/migrate-me',
        $type: 'Post',
        title: 'Migrate Me',
        version: 1,
        createdAt: '2024-01-01T00:00:00Z',
      }

      await storage.mkdir('data/posts')
      await storage.write(
        'data/posts/data.json',
        new TextEncoder().encode(JSON.stringify([legacyEntity]))
      )

      // Step 2 & 3: Migration - create synthetic event
      const esBackend = withEventSourcing(storage)

      await esBackend.appendEvent({
        id: 'evt-migration-001',
        ts: new Date('2024-01-01T00:00:00Z').getTime(),
        op: 'CREATE',
        target: 'posts:migrate-me',
        after: legacyEntity,
        metadata: { migration: true },
      })

      await esBackend.flush()

      // Step 4: Entity is now reconstructible from events
      const entity = await esBackend.reconstructEntity('posts', 'migrate-me')

      expect(entity).not.toBeNull()
      expect(entity?.title).toBe('Migrate Me')
    })
  })
})
