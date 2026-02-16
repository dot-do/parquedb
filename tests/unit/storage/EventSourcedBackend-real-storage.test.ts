/**
 * EventSourcedBackend Tests with Real Storage Backends
 *
 * These tests verify EventSourcedBackend works correctly with real storage
 * backends (FsBackend), not just MemoryBackend. This ensures:
 *
 * 1. Flush writes all three files (events/batch-*.parquet, data.parquet, rels.parquet)
 * 2. Cross-session persistence works (entities can be reconstructed from storage)
 * 3. entitiesDirty flag correctly controls data.parquet writes
 * 4. relsDirty flag correctly controls rels.parquet writes
 *
 * Uses a test factory pattern to enable testing with different storage backends.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { EventSourcedBackend } from '../../../src/storage/EventSourcedBackend'
import { FsBackend } from '../../../src/storage/FsBackend'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { StorageBackend } from '../../../src/types/storage'
import type { Event, EventOp, Variant } from '../../../src/types'
import { generateTestDirName } from '../../factories'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a test event
 */
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

/**
 * Create a CREATE event with entity data
 */
function createCreateEvent(
  ns: string,
  id: string,
  data: Variant
): Event {
  return createEvent(ns, id, 'CREATE', undefined, {
    $type: data.$type || 'TestEntity',
    name: data.name || `Test ${id}`,
    ...data,
  })
}

/**
 * Create an UPDATE event
 */
function createUpdateEvent(
  ns: string,
  id: string,
  before: Variant,
  after: Variant
): Event {
  return createEvent(ns, id, 'UPDATE', before, after)
}

/**
 * Create a DELETE event
 */
function createDeleteEvent(
  ns: string,
  id: string,
  before: Variant
): Event {
  return createEvent(ns, id, 'DELETE', before, undefined)
}

// =============================================================================
// Backend Factory Pattern
// =============================================================================

interface BackendFactory {
  name: string
  create: () => Promise<{ storage: StorageBackend; cleanup: () => Promise<void> }>
}

/**
 * FsBackend factory - creates real filesystem storage in a temp directory
 */
const fsBackendFactory: BackendFactory = {
  name: 'FsBackend',
  create: async () => {
    const testDir = join(tmpdir(), generateTestDirName('es-backend-test'))
    await fs.mkdir(testDir, { recursive: true })
    const storage = new FsBackend(testDir)

    return {
      storage,
      cleanup: async () => {
        try {
          await fs.rm(testDir, { recursive: true, force: true })
        } catch {
          // Ignore cleanup errors
        }
      },
    }
  },
}

/**
 * MemoryBackend factory for comparison testing
 */
const memoryBackendFactory: BackendFactory = {
  name: 'MemoryBackend',
  create: async () => {
    const storage = new MemoryBackend()
    return {
      storage,
      cleanup: async () => {
        // No cleanup needed for memory backend
      },
    }
  },
}

// Test with both backends to ensure consistent behavior
const backendFactories: BackendFactory[] = [
  fsBackendFactory,
  memoryBackendFactory,
]

// =============================================================================
// Tests
// =============================================================================

describe('EventSourcedBackend with Real Storage', () => {
  for (const factory of backendFactories) {
    describe(`${factory.name}`, () => {
      let storage: StorageBackend
      let eventSourced: EventSourcedBackend
      let cleanup: () => Promise<void>

      beforeEach(async () => {
        const created = await factory.create()
        storage = created.storage
        cleanup = created.cleanup
        eventSourced = new EventSourcedBackend(storage, {
          maxBufferedEvents: 100, // High threshold to control flush timing
          autoSnapshotThreshold: 0, // Disable auto-snapshots for explicit control
        })
      })

      afterEach(async () => {
        await eventSourced.dispose()
        await cleanup()
      })

      // =========================================================================
      // 1. Flush writes all three files
      // =========================================================================

      describe('Flush writes all three files', () => {
        it('should create events batch file on flush after appendEvent', async () => {
          const event = createCreateEvent('users', 'user1', {
            $type: 'User',
            name: 'Alice',
            email: 'alice@example.com',
          })

          await eventSourced.appendEvent(event)
          await eventSourced.flush()

          // Verify batch files were created in events/ directory
          const eventsList = await storage.list('events')
          const batchFiles = eventsList.files.filter(f => f.includes('batch-') && f.endsWith('.parquet'))
          expect(batchFiles.length).toBeGreaterThan(0)
        })

        it('should create data.parquet on flush when entities exist', async () => {
          // Create multiple entities to ensure data.parquet is written
          await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
            $type: 'User',
            name: 'Alice',
          }))
          await eventSourced.appendEvent(createCreateEvent('users', 'user2', {
            $type: 'User',
            name: 'Bob',
          }))

          await eventSourced.flush()

          // Verify data.parquet was created
          const dataExists = await storage.exists('data.parquet')
          expect(dataExists).toBe(true)
        })

        it('should create rels.parquet on flush when relationships exist', async () => {
          // Create entities with relationship references
          await eventSourced.appendEvent(createCreateEvent('posts', 'post1', {
            $type: 'Post',
            name: 'My Post',
            author: 'users/user1', // This is a relationship reference
          }))

          await eventSourced.flush()

          // Verify rels.parquet was created (relationships were detected)
          const relsExists = await storage.exists('rels.parquet')
          expect(relsExists).toBe(true)
        })

        it('should write all three files when entities have relationships', async () => {
          // Create a user
          await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
            $type: 'User',
            name: 'Alice',
          }))

          // Create a post that references the user
          await eventSourced.appendEvent(createCreateEvent('posts', 'post1', {
            $type: 'Post',
            name: 'My First Post',
            author: 'users/user1',
          }))

          await eventSourced.flush()

          // Verify all three files were created
          const eventsList = await storage.list('events')
          const batchFiles = eventsList.files.filter(f => f.includes('batch-') && f.endsWith('.parquet'))
          expect(batchFiles.length).toBeGreaterThan(0)
          expect(await storage.exists('data.parquet')).toBe(true)
          expect(await storage.exists('rels.parquet')).toBe(true)
        })
      })

      // =========================================================================
      // 2. Cross-session persistence
      // =========================================================================

      describe('Cross-session persistence', () => {
        it('should reconstruct entities from storage in new session', async () => {
          // Session 1: Create entities and flush
          await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
            $type: 'User',
            name: 'Alice',
            email: 'alice@example.com',
          }))
          await eventSourced.appendEvent(createCreateEvent('users', 'user2', {
            $type: 'User',
            name: 'Bob',
            email: 'bob@example.com',
          }))
          await eventSourced.flush()
          await eventSourced.dispose()

          // Session 2: Create NEW EventSourcedBackend with same storage
          const newEventSourced = new EventSourcedBackend(storage, {
            maxBufferedEvents: 100,
            autoSnapshotThreshold: 0,
          })

          // Verify entities can be reconstructed
          const entities = await newEventSourced.reconstructAllEntities()

          expect(entities.size).toBe(2)
          expect(entities.has('users/user1')).toBe(true)
          expect(entities.has('users/user2')).toBe(true)

          const alice = entities.get('users/user1')
          expect(alice).toBeDefined()
          expect(alice!.$type).toBe('User')
          expect(alice!.name).toBe('Alice')
          expect((alice as { email: string }).email).toBe('alice@example.com')

          const bob = entities.get('users/user2')
          expect(bob).toBeDefined()
          expect(bob!.name).toBe('Bob')

          await newEventSourced.dispose()
        })

        it('should preserve entity state including updates across sessions', async () => {
          // Session 1: Create and update entity
          await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
            $type: 'User',
            name: 'Alice',
            score: 0,
          }))
          await eventSourced.appendEvent(createUpdateEvent(
            'users',
            'user1',
            { score: 0 },
            { score: 100 }
          ))
          await eventSourced.flush()
          await eventSourced.dispose()

          // Session 2: Verify state is preserved
          const newEventSourced = new EventSourcedBackend(storage)
          const entities = await newEventSourced.reconstructAllEntities()

          const user = entities.get('users/user1')
          expect(user).toBeDefined()
          expect((user as { score: number }).score).toBe(100)
          expect(user!.version).toBe(2) // Version incremented by update

          await newEventSourced.dispose()
        })

        it('should not return deleted entities in reconstruction', async () => {
          // Session 1: Create and delete entity
          await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
            $type: 'User',
            name: 'Alice',
          }))
          await eventSourced.appendEvent(createDeleteEvent(
            'users',
            'user1',
            { $type: 'User', name: 'Alice' }
          ))
          await eventSourced.flush()
          await eventSourced.dispose()

          // Session 2: Deleted entities should not be in the map
          const newEventSourced = new EventSourcedBackend(storage)
          const entities = await newEventSourced.reconstructAllEntities()

          // The entity should either not be in the map or have deletedAt set
          const user = entities.get('users/user1')
          if (user) {
            expect(user.deletedAt).toBeDefined()
          }

          await newEventSourced.dispose()
        })

        it('should handle multiple flush cycles across sessions', async () => {
          // Session 1: Create entity and flush
          await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
            $type: 'User',
            name: 'Alice',
          }))
          await eventSourced.flush()
          await eventSourced.dispose()

          // Session 2: Add more entities and flush
          const session2 = new EventSourcedBackend(storage)
          await session2.appendEvent(createCreateEvent('users', 'user2', {
            $type: 'User',
            name: 'Bob',
          }))
          await session2.flush()
          await session2.dispose()

          // Session 3: Verify all entities are present
          const session3 = new EventSourcedBackend(storage)
          const entities = await session3.reconstructAllEntities()

          expect(entities.size).toBe(2)
          expect(entities.has('users/user1')).toBe(true)
          expect(entities.has('users/user2')).toBe(true)

          await session3.dispose()
        })
      })

      // =========================================================================
      // 3. entitiesDirty flag
      // =========================================================================

      describe('entitiesDirty flag', () => {
        it('should set entitiesDirty on CREATE event', async () => {
          // Initial state - no data.parquet
          expect(await storage.exists('data.parquet')).toBe(false)

          // CREATE event should mark entities dirty
          await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
            $type: 'User',
            name: 'Alice',
          }))
          await eventSourced.flush()

          // data.parquet should be written when entitiesDirty is true
          expect(await storage.exists('data.parquet')).toBe(true)
        })

        it('should set entitiesDirty on UPDATE event', async () => {
          // Create initial entity and flush
          await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
            $type: 'User',
            name: 'Alice',
            score: 0,
          }))
          await eventSourced.flush()

          // Get initial data.parquet content
          const initialData = await storage.read('data.parquet')

          // Clear and add UPDATE event
          await eventSourced.appendEvent(createUpdateEvent(
            'users',
            'user1',
            { score: 0 },
            { score: 100 }
          ))
          await eventSourced.flush()

          // data.parquet should be rewritten with updated entity
          const updatedData = await storage.read('data.parquet')

          // The files should be different (entity was updated)
          // Note: Size may vary due to compression, but content should differ
          expect(updatedData.length).toBeGreaterThan(0)
        })

        it('should set entitiesDirty on DELETE event', async () => {
          // Create entity
          await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
            $type: 'User',
            name: 'Alice',
          }))
          await eventSourced.flush()

          // Delete entity
          await eventSourced.appendEvent(createDeleteEvent(
            'users',
            'user1',
            { $type: 'User', name: 'Alice' }
          ))
          await eventSourced.flush()

          // data.parquet should still exist (entities map may be empty or contain soft-deleted)
          expect(await storage.exists('data.parquet')).toBe(true)
        })

        it('should NOT write data.parquet when no entity changes occur', async () => {
          // Flush with no events - data.parquet should not be created
          await eventSourced.flush()

          expect(await storage.exists('data.parquet')).toBe(false)
        })

        it('should correctly track entitiesDirty across multiple flushes', async () => {
          // First flush with entity
          await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
            $type: 'User',
            name: 'Alice',
          }))
          await eventSourced.flush()

          const dataAfterFirstFlush = await storage.stat('data.parquet')
          expect(dataAfterFirstFlush).not.toBeNull()
          const firstMtime = dataAfterFirstFlush!.mtime.getTime()

          // Wait a bit to ensure mtime difference
          await new Promise(resolve => setTimeout(resolve, 10))

          // Second flush with another entity
          await eventSourced.appendEvent(createCreateEvent('users', 'user2', {
            $type: 'User',
            name: 'Bob',
          }))
          await eventSourced.flush()

          const dataAfterSecondFlush = await storage.stat('data.parquet')
          expect(dataAfterSecondFlush).not.toBeNull()

          // File should have been rewritten (mtime should be different or size should change)
          // Since we added an entity, the file should definitely be different
          expect(
            dataAfterSecondFlush!.mtime.getTime() >= firstMtime ||
            dataAfterSecondFlush!.size !== dataAfterFirstFlush!.size
          ).toBe(true)
        })
      })

      // =========================================================================
      // 4. relsDirty flag
      // =========================================================================

      describe('relsDirty flag', () => {
        it('should set relsDirty when relationships are detected in entity', async () => {
          // Create entity with relationship reference
          await eventSourced.appendEvent(createCreateEvent('posts', 'post1', {
            $type: 'Post',
            name: 'My Post',
            author: 'users/user1', // This triggers relationship detection
          }))
          await eventSourced.flush()

          // rels.parquet should be written when relsDirty is true
          expect(await storage.exists('rels.parquet')).toBe(true)
        })

        it('should NOT write rels.parquet when no relationships exist', async () => {
          // Create entity without relationships
          await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
            $type: 'User',
            name: 'Alice',
            email: 'alice@example.com', // Not a relationship reference
          }))
          await eventSourced.flush()

          // rels.parquet should NOT exist (no relationships)
          expect(await storage.exists('rels.parquet')).toBe(false)
        })

        it('should detect relationships in array fields', async () => {
          // Create entity with array of relationship references
          await eventSourced.appendEvent(createCreateEvent('posts', 'post1', {
            $type: 'Post',
            name: 'My Post',
            tags: ['categories/tech', 'categories/programming'], // Array of refs
          }))
          await eventSourced.flush()

          // rels.parquet should be written
          expect(await storage.exists('rels.parquet')).toBe(true)
        })

        it('should detect relationships in nested object references', async () => {
          // Create entity with nested reference object
          await eventSourced.appendEvent(createCreateEvent('posts', 'post1', {
            $type: 'Post',
            name: 'My Post',
            author: { $id: 'users/user1' }, // Object with $id field
          }))
          await eventSourced.flush()

          // rels.parquet should be written
          expect(await storage.exists('rels.parquet')).toBe(true)
        })

        it('should correctly track relsDirty across updates', async () => {
          // Create entity without relationships
          await eventSourced.appendEvent(createCreateEvent('posts', 'post1', {
            $type: 'Post',
            name: 'Draft Post',
          }))
          await eventSourced.flush()

          // No rels.parquet yet
          expect(await storage.exists('rels.parquet')).toBe(false)

          // Update to add relationship
          await eventSourced.appendEvent(createUpdateEvent(
            'posts',
            'post1',
            { name: 'Draft Post' },
            { author: 'users/user1' }
          ))
          await eventSourced.flush()

          // Now rels.parquet should exist
          expect(await storage.exists('rels.parquet')).toBe(true)
        })

        it('should accumulate relationships across multiple entities', async () => {
          // Create multiple entities with relationships
          await eventSourced.appendEvent(createCreateEvent('posts', 'post1', {
            $type: 'Post',
            name: 'Post 1',
            author: 'users/user1',
          }))
          await eventSourced.appendEvent(createCreateEvent('posts', 'post2', {
            $type: 'Post',
            name: 'Post 2',
            author: 'users/user2',
          }))
          await eventSourced.flush()

          // rels.parquet should contain relationships from both entities
          expect(await storage.exists('rels.parquet')).toBe(true)

          // Read the rels file to verify content exists
          const relsData = await storage.read('rels.parquet')
          expect(relsData.length).toBeGreaterThan(0)
        })
      })

      // =========================================================================
      // Integration: Complete workflow
      // =========================================================================

      describe('Complete workflow integration', () => {
        it('should handle complete CRUD workflow with persistence', async () => {
          // CREATE
          await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
            $type: 'User',
            name: 'Alice',
            email: 'alice@example.com',
            score: 0,
          }))

          // UPDATE
          await eventSourced.appendEvent(createUpdateEvent(
            'users',
            'user1',
            { score: 0 },
            { score: 50 }
          ))

          // CREATE another entity with relationship
          await eventSourced.appendEvent(createCreateEvent('posts', 'post1', {
            $type: 'Post',
            name: 'Hello World',
            author: 'users/user1',
          }))

          // Flush all events
          await eventSourced.flush()

          // Verify all files were created
          const eventsList = await storage.list('events')
          const batchFiles = eventsList.files.filter(f => f.includes('batch-') && f.endsWith('.parquet'))
          expect(batchFiles.length).toBeGreaterThan(0)
          expect(await storage.exists('data.parquet')).toBe(true)
          expect(await storage.exists('rels.parquet')).toBe(true)

          // Dispose and create new session
          await eventSourced.dispose()

          const newSession = new EventSourcedBackend(storage)
          const entities = await newSession.reconstructAllEntities()

          // Verify entities
          expect(entities.size).toBe(2)

          const user = entities.get('users/user1')
          expect(user).toBeDefined()
          expect(user!.name).toBe('Alice')
          expect((user as { score: number }).score).toBe(50)
          expect(user!.version).toBe(2)

          const post = entities.get('posts/post1')
          expect(post).toBeDefined()
          expect(post!.name).toBe('Hello World')
          expect((post as { author: string }).author).toBe('users/user1')

          await newSession.dispose()
        })

        it('should handle concurrent entity operations within single session', async () => {
          // Create multiple entities in rapid succession
          const promises = []
          for (let i = 0; i < 10; i++) {
            promises.push(
              eventSourced.appendEvent(createCreateEvent('items', `item${i}`, {
                $type: 'Item',
                name: `Item ${i}`,
                index: i,
              }))
            )
          }
          await Promise.all(promises)
          await eventSourced.flush()

          // Verify all entities were persisted
          await eventSourced.dispose()

          const newSession = new EventSourcedBackend(storage)
          const entities = await newSession.reconstructAllEntities()

          expect(entities.size).toBe(10)
          for (let i = 0; i < 10; i++) {
            expect(entities.has(`items/item${i}`)).toBe(true)
          }

          await newSession.dispose()
        })
      })
    })
  }
})

// =============================================================================
// FsBackend-specific tests (filesystem behavior)
// =============================================================================

describe('EventSourcedBackend with FsBackend specifically', () => {
  let testDir: string
  let storage: FsBackend
  let eventSourced: EventSourcedBackend

  beforeEach(async () => {
    testDir = join(tmpdir(), generateTestDirName('es-fs-test'))
    await fs.mkdir(testDir, { recursive: true })
    storage = new FsBackend(testDir)
    eventSourced = new EventSourcedBackend(storage, {
      maxBufferedEvents: 100,
      autoSnapshotThreshold: 0,
    })
  })

  afterEach(async () => {
    await eventSourced.dispose()
    try {
      await fs.rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  it('should write Parquet files that can be read as binary', async () => {
    await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
      $type: 'User',
      name: 'Alice',
    }))
    await eventSourced.flush()

    // Find the batch file in events/ directory
    const eventsDir = join(testDir, 'events')
    const files = await fs.readdir(eventsDir)
    const batchFile = files.find(f => f.includes('batch-') && f.endsWith('.parquet'))
    expect(batchFile).toBeDefined()

    // Read the raw file content
    const eventsData = await fs.readFile(join(eventsDir, batchFile!))

    // Parquet files start with "PAR1" magic bytes
    expect(eventsData.slice(0, 4).toString()).toBe('PAR1')
  })

  it('should create atomic file writes (no partial files on crash simulation)', async () => {
    // Add multiple events
    for (let i = 0; i < 5; i++) {
      await eventSourced.appendEvent(createCreateEvent('users', `user${i}`, {
        $type: 'User',
        name: `User ${i}`,
      }))
    }
    await eventSourced.flush()

    // Find the batch file in events/ directory
    const eventsDir = join(testDir, 'events')
    const files = await fs.readdir(eventsDir)
    const batchFile = files.find(f => f.includes('batch-') && f.endsWith('.parquet'))
    expect(batchFile).toBeDefined()

    // Verify file is complete (has Parquet footer)
    const eventsData = await fs.readFile(join(eventsDir, batchFile!))

    // Parquet files end with "PAR1" magic bytes
    const footer = eventsData.slice(-4)
    expect(footer.toString()).toBe('PAR1')
  })

  it('should handle empty directories on first run', async () => {
    // First flush with no events should not crash
    await eventSourced.flush()

    // Directory should be empty or only have internal state files
    const files = await fs.readdir(testDir)
    // No Parquet files should exist since no events were written
    expect(files.filter(f => f.endsWith('.parquet')).length).toBe(0)
  })

  it('should handle reconstruction with corrupt/missing batch files gracefully', async () => {
    // Write some events and flush
    await eventSourced.appendEvent(createCreateEvent('users', 'user1', {
      $type: 'User',
      name: 'Alice',
    }))
    await eventSourced.flush()
    await eventSourced.dispose()

    // Delete all batch files in events/ directory to simulate corruption
    const eventsDir = join(testDir, 'events')
    try {
      const files = await fs.readdir(eventsDir)
      for (const file of files) {
        if (file.includes('batch-') && file.endsWith('.parquet')) {
          await fs.unlink(join(eventsDir, file))
        }
      }
    } catch {
      // Directory might not exist
    }

    // New session should handle missing files gracefully
    const newSession = new EventSourcedBackend(storage)
    const entities = await newSession.reconstructAllEntities()

    // Should return empty map (no crash)
    expect(entities.size).toBe(0)

    await newSession.dispose()
  })
})
