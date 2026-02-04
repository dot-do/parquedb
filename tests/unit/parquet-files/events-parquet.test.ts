/**
 * Events Parquet File Creation and Contents Verification Tests
 *
 * Comprehensive tests for events.parquet file:
 * - File creation on disposeAsync()
 * - Schema verification (id, ts, op, target, before, after, actor, metadata)
 * - Event order verification (by timestamp)
 * - CREATE events: null before, populated after
 * - UPDATE events: before and after populated
 * - DELETE events: populated before, null after
 * - Event replay/reconstruction from events.parquet
 * - Audit fields stored in the after variant
 *
 * Uses test factory pattern to run SAME tests against FsBackend and R2Backend.
 */

import { describe, it, expect, beforeEach, afterEach, afterAll } from 'vitest'
import { ParqueDB } from '../../../src/ParqueDB'
import { FsBackend } from '../../../src/storage/FsBackend'
import { mkdtemp, rm, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { parquetRead, parquetMetadataAsync } from 'hyparquet'
import { compressors } from '../../../src/parquet/compression'
import { decodeVariant } from '../../../src/parquet/variant'
import type { StorageBackend } from '../../../src/types/storage'
import {
  createTestFsBackend,
  createTestR2Backend,
  hasR2Credentials,
  cleanupTestStorage,
} from '../../helpers/storage'

// =============================================================================
// Types
// =============================================================================

interface EventRow {
  id: string
  ts: bigint | number
  op: string
  target: string
  before: string | null
  after: string | null
  actor: string | null
  metadata: string | null
}

interface ParsedEvent {
  id: string
  ts: number
  op: string
  target: string
  before: Record<string, unknown> | null
  after: Record<string, unknown> | null
  actor: string | null
  metadata: Record<string, unknown> | null
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Decode a base64-encoded Variant string to an object
 */
function decodeVariantString(base64: string | null): Record<string, unknown> | null {
  if (!base64) return null
  const bytes = Uint8Array.from(atob(base64), (c) => c.charCodeAt(0))
  return decodeVariant(bytes) as Record<string, unknown>
}

/**
 * Create an async buffer for hyparquet from a file path
 */
async function createAsyncBuffer(filePath: string) {
  const data = await readFile(filePath)
  return {
    byteLength: data.length,
    slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
      const sliced = data.slice(start, end ?? data.length)
      const buffer = new ArrayBuffer(sliced.byteLength)
      new Uint8Array(buffer).set(sliced)
      return buffer
    },
  }
}

/**
 * Read events from a parquet file and parse them
 */
async function readEventsParquet(filePath: string): Promise<ParsedEvent[]> {
  const asyncBuffer = await createAsyncBuffer(filePath)

  let rows: unknown[][] = []
  await parquetRead({
    file: asyncBuffer,
    compressors,
    onComplete: (data: unknown[][]) => {
      rows = data
    },
  })

  // Get column names from metadata
  const metadata = await parquetMetadataAsync(asyncBuffer)
  const columnNames = (metadata.schema as Array<{ name?: string }>)
    .filter((s) => s.name && s.name !== 'root')
    .map((s) => s.name!)

  // Convert rows to objects
  return rows.map((row) => {
    const obj: Record<string, unknown> = {}
    columnNames.forEach((name, idx) => {
      obj[name] = row[idx]
    })

    const eventRow = obj as EventRow

    return {
      id: eventRow.id,
      ts: typeof eventRow.ts === 'bigint' ? Number(eventRow.ts) : eventRow.ts,
      op: eventRow.op,
      target: eventRow.target,
      before: decodeVariantString(eventRow.before),
      after: decodeVariantString(eventRow.after),
      actor: eventRow.actor,
      metadata: decodeVariantString(eventRow.metadata),
    }
  })
}

/**
 * Get schema column names from a parquet file
 */
async function getParquetSchemaColumns(filePath: string): Promise<string[]> {
  const asyncBuffer = await createAsyncBuffer(filePath)
  const metadata = await parquetMetadataAsync(asyncBuffer)
  return (metadata.schema as Array<{ name?: string }>)
    .filter((s) => s.name && s.name !== 'root')
    .map((s) => s.name!)
}

// =============================================================================
// Test Factory
// =============================================================================

interface BackendFactory {
  name: string
  createBackend: () => Promise<StorageBackend>
  getEventsPath: (backend: StorageBackend) => string
  shouldSkip?: boolean
}

/**
 * Factory for FsBackend tests
 */
function createFsBackendFactory(): BackendFactory {
  let tempDir: string

  return {
    name: 'FsBackend',
    createBackend: async () => {
      tempDir = await mkdtemp(join(tmpdir(), 'parquedb-events-test-'))
      return new FsBackend(tempDir)
    },
    getEventsPath: () => join(tempDir, 'events.parquet'),
    shouldSkip: false,
  }
}

/**
 * Factory for R2Backend tests (only runs if credentials available)
 */
function createR2BackendFactory(): BackendFactory {
  let r2Backend: StorageBackend | null = null

  return {
    name: 'R2Backend',
    createBackend: async () => {
      r2Backend = await createTestR2Backend()
      return r2Backend
    },
    getEventsPath: () => {
      // R2Backend doesn't have a local path - we need to read via the backend
      throw new Error('R2Backend requires reading via storage backend, not filesystem')
    },
    shouldSkip: !hasR2Credentials(),
  }
}

/**
 * Run the complete test suite against a backend factory
 */
function runEventsParquetTests(factory: BackendFactory) {
  describe(`Events Parquet Tests (${factory.name})`, () => {
    let storage: StorageBackend
    let db: ParqueDB
    let eventsPath: string

    // For R2, we need a different approach to read the parquet file
    async function readEventsFromStorage(): Promise<ParsedEvent[]> {
      if (factory.name === 'R2Backend') {
        // Read via storage backend
        const data = await storage.read('events.parquet')
        const asyncBuffer = {
          byteLength: data.length,
          slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
            const sliced = data.slice(start, end ?? data.length)
            const buffer = new ArrayBuffer(sliced.byteLength)
            new Uint8Array(buffer).set(sliced)
            return buffer
          },
        }

        let rows: unknown[][] = []
        await parquetRead({
          file: asyncBuffer,
          compressors,
          onComplete: (d: unknown[][]) => {
            rows = d
          },
        })

        const metadata = await parquetMetadataAsync(asyncBuffer)
        const columnNames = (metadata.schema as Array<{ name?: string }>)
          .filter((s) => s.name && s.name !== 'root')
          .map((s) => s.name!)

        return rows.map((row) => {
          const obj: Record<string, unknown> = {}
          columnNames.forEach((name, idx) => {
            obj[name] = row[idx]
          })

          const eventRow = obj as EventRow

          return {
            id: eventRow.id,
            ts: typeof eventRow.ts === 'bigint' ? Number(eventRow.ts) : eventRow.ts,
            op: eventRow.op,
            target: eventRow.target,
            before: decodeVariantString(eventRow.before),
            after: decodeVariantString(eventRow.after),
            actor: eventRow.actor,
            metadata: decodeVariantString(eventRow.metadata),
          }
        })
      } else {
        return readEventsParquet(eventsPath)
      }
    }

    async function getSchemaColumns(): Promise<string[]> {
      if (factory.name === 'R2Backend') {
        const data = await storage.read('events.parquet')
        const asyncBuffer = {
          byteLength: data.length,
          slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
            const sliced = data.slice(start, end ?? data.length)
            const buffer = new ArrayBuffer(sliced.byteLength)
            new Uint8Array(buffer).set(sliced)
            return buffer
          },
        }
        const metadata = await parquetMetadataAsync(asyncBuffer)
        return (metadata.schema as Array<{ name?: string }>)
          .filter((s) => s.name && s.name !== 'root')
          .map((s) => s.name!)
      } else {
        return getParquetSchemaColumns(eventsPath)
      }
    }

    beforeEach(async () => {
      storage = await factory.createBackend()
      db = new ParqueDB({ storage })
      try {
        eventsPath = factory.getEventsPath(storage)
      } catch {
        // R2Backend doesn't have a local path
        eventsPath = ''
      }
    })

    afterEach(async () => {
      await db.disposeAsync()
      if (factory.name === 'FsBackend') {
        try {
          await rm((storage as FsBackend).rootPath, { recursive: true, force: true })
        } catch {
          // Ignore cleanup errors
        }
      }
    })

    // =========================================================================
    // Schema Verification Tests
    // =========================================================================

    describe('Schema Verification', () => {
      it('should create events.parquet with correct schema columns', async () => {
        // Create an entity to generate events
        await db.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          title: 'Hello World',
        })

        await db.disposeAsync()

        // Verify file exists
        const exists = await storage.exists('events.parquet')
        expect(exists).toBe(true)

        // Get schema columns
        const columns = await getSchemaColumns()

        // Verify all required columns are present
        expect(columns).toContain('id')
        expect(columns).toContain('ts')
        expect(columns).toContain('op')
        expect(columns).toContain('target')
        expect(columns).toContain('before')
        expect(columns).toContain('after')
        expect(columns).toContain('actor')
        expect(columns).toContain('metadata')

        // Verify audit fields are NOT separate columns (they're in the after variant)
        expect(columns).not.toContain('createdAt')
        expect(columns).not.toContain('createdBy')
        expect(columns).not.toContain('updatedAt')
        expect(columns).not.toContain('updatedBy')
        expect(columns).not.toContain('version')
      })
    })

    // =========================================================================
    // CREATE Event Tests
    // =========================================================================

    describe('CREATE Events', () => {
      it('should have null before and populated after for CREATE events', async () => {
        const entity = await db.create('posts', {
          $type: 'Post',
          name: 'New Post',
          title: 'Test Title',
          content: 'Test Content',
        })

        await db.disposeAsync()

        const events = await readEventsFromStorage()
        const createEvent = events.find((e) => e.op === 'CREATE')

        expect(createEvent).toBeDefined()
        expect(createEvent!.before).toBeNull()
        expect(createEvent!.after).not.toBeNull()

        // Verify after contains entity data
        expect(createEvent!.after!.$id).toBe(entity.$id)
        expect(createEvent!.after!.$type).toBe('Post')
        expect(createEvent!.after!.name).toBe('New Post')
        expect(createEvent!.after!.title).toBe('Test Title')
        expect(createEvent!.after!.content).toBe('Test Content')
      })

      it('should include audit fields in the after variant for CREATE', async () => {
        await db.create('posts', {
          $type: 'Post',
          name: 'Audit Test Post',
          title: 'Test',
        })

        await db.disposeAsync()

        const events = await readEventsFromStorage()
        const createEvent = events.find((e) => e.op === 'CREATE')

        expect(createEvent).toBeDefined()
        expect(createEvent!.after).not.toBeNull()

        // Verify audit fields are in the after variant
        expect(createEvent!.after!.createdAt).toBeDefined()
        expect(createEvent!.after!.createdBy).toBeDefined()
        expect(createEvent!.after!.updatedAt).toBeDefined()
        expect(createEvent!.after!.updatedBy).toBeDefined()
        expect(createEvent!.after!.version).toBe(1)
      })

      it('should have correct target format for entity events', async () => {
        const entity = await db.create('users', {
          $type: 'User',
          name: 'John Doe',
          email: 'john@example.com',
        })

        await db.disposeAsync()

        const events = await readEventsFromStorage()
        const createEvent = events.find((e) => e.op === 'CREATE')

        expect(createEvent).toBeDefined()
        // Target format should be "ns:id"
        const [ns, id] = createEvent!.target.split(':')
        expect(ns).toBe('users')
        expect(id).toBe(entity.$id.split('/')[1]) // Extract local ID from full entity ID
      })
    })

    // =========================================================================
    // UPDATE Event Tests
    // =========================================================================

    describe('UPDATE Events', () => {
      it('should have both before and after populated for UPDATE events', async () => {
        const entity = await db.create('posts', {
          $type: 'Post',
          name: 'Original Post',
          title: 'Original Title',
          content: 'Original Content',
        })

        await db.update('posts', entity.$id, {
          $set: { title: 'Updated Title', content: 'Updated Content' },
        })

        await db.disposeAsync()

        const events = await readEventsFromStorage()
        const updateEvent = events.find((e) => e.op === 'UPDATE')

        expect(updateEvent).toBeDefined()
        expect(updateEvent!.before).not.toBeNull()
        expect(updateEvent!.after).not.toBeNull()

        // Verify before contains original state
        expect(updateEvent!.before!.title).toBe('Original Title')
        expect(updateEvent!.before!.content).toBe('Original Content')

        // Verify after contains updated state
        expect(updateEvent!.after!.title).toBe('Updated Title')
        expect(updateEvent!.after!.content).toBe('Updated Content')
      })

      it('should increment version in after variant for UPDATE', async () => {
        const entity = await db.create('posts', {
          $type: 'Post',
          name: 'Version Test',
          title: 'Test',
        })

        await db.update('posts', entity.$id, {
          $set: { title: 'Updated' },
        })

        await db.disposeAsync()

        const events = await readEventsFromStorage()
        const createEvent = events.find((e) => e.op === 'CREATE')
        const updateEvent = events.find((e) => e.op === 'UPDATE')

        expect(createEvent!.after!.version).toBe(1)
        expect(updateEvent!.before!.version).toBe(1)
        expect(updateEvent!.after!.version).toBe(2)
      })

      it('should track multiple updates correctly', async () => {
        const entity = await db.create('posts', {
          $type: 'Post',
          name: 'Multi Update',
          counter: 0,
        })

        await db.update('posts', entity.$id, { $set: { counter: 1 } })
        await db.update('posts', entity.$id, { $set: { counter: 2 } })
        await db.update('posts', entity.$id, { $set: { counter: 3 } })

        await db.disposeAsync()

        const events = await readEventsFromStorage()
        const updateEvents = events.filter((e) => e.op === 'UPDATE')

        expect(updateEvents.length).toBe(3)

        // Verify each update's before/after chain
        expect(updateEvents[0]!.before!.counter).toBe(0)
        expect(updateEvents[0]!.after!.counter).toBe(1)

        expect(updateEvents[1]!.before!.counter).toBe(1)
        expect(updateEvents[1]!.after!.counter).toBe(2)

        expect(updateEvents[2]!.before!.counter).toBe(2)
        expect(updateEvents[2]!.after!.counter).toBe(3)
      })
    })

    // =========================================================================
    // DELETE Event Tests
    // =========================================================================

    describe('DELETE Events', () => {
      it('should have populated before and null after for DELETE events (hard delete)', async () => {
        const entity = await db.create('posts', {
          $type: 'Post',
          name: 'Delete Test',
          title: 'To Be Deleted',
        })

        await db.delete('posts', entity.$id, { hard: true })

        await db.disposeAsync()

        const events = await readEventsFromStorage()
        const deleteEvent = events.find((e) => e.op === 'DELETE')

        expect(deleteEvent).toBeDefined()
        expect(deleteEvent!.before).not.toBeNull()
        expect(deleteEvent!.after).toBeNull()

        // Verify before contains the entity state before deletion
        expect(deleteEvent!.before!.name).toBe('Delete Test')
        expect(deleteEvent!.before!.title).toBe('To Be Deleted')
      })

      it('should preserve entity state in before for soft delete', async () => {
        const entity = await db.create('posts', {
          $type: 'Post',
          name: 'Soft Delete Test',
          title: 'Soft Deleted',
          customField: 'preserved',
        })

        await db.delete('posts', entity.$id) // Soft delete by default

        await db.disposeAsync()

        const events = await readEventsFromStorage()
        const deleteEvent = events.find((e) => e.op === 'DELETE')

        expect(deleteEvent).toBeDefined()
        expect(deleteEvent!.before).not.toBeNull()
        expect(deleteEvent!.before!.customField).toBe('preserved')

        // For soft delete, after might contain the deleted state with deletedAt
        if (deleteEvent!.after) {
          expect(deleteEvent!.after.deletedAt).toBeDefined()
        }
      })
    })

    // =========================================================================
    // Event Order Verification
    // =========================================================================

    describe('Event Order Verification', () => {
      it('should store events in timestamp order', async () => {
        // Create multiple entities with slight delays to ensure different timestamps
        await db.create('posts', { $type: 'Post', name: 'Post 1', order: 1 })
        await db.create('posts', { $type: 'Post', name: 'Post 2', order: 2 })
        await db.create('posts', { $type: 'Post', name: 'Post 3', order: 3 })

        await db.disposeAsync()

        const events = await readEventsFromStorage()
        const createEvents = events.filter((e) => e.op === 'CREATE')

        expect(createEvents.length).toBe(3)

        // Verify events are in ascending timestamp order
        for (let i = 1; i < createEvents.length; i++) {
          expect(createEvents[i]!.ts).toBeGreaterThanOrEqual(createEvents[i - 1]!.ts)
        }
      })

      it('should maintain correct order for mixed operations', async () => {
        const entity = await db.create('posts', { $type: 'Post', name: 'Mixed Ops', value: 'initial' })
        await db.update('posts', entity.$id, { $set: { value: 'updated' } })
        await db.delete('posts', entity.$id, { hard: true })

        await db.disposeAsync()

        const events = await readEventsFromStorage()

        // Filter events for this entity
        const entityId = entity.$id.split('/')[1]
        const entityEvents = events.filter((e) => e.target.includes(entityId!))

        expect(entityEvents.length).toBe(3)

        // Verify order: CREATE -> UPDATE -> DELETE
        expect(entityEvents[0]!.op).toBe('CREATE')
        expect(entityEvents[1]!.op).toBe('UPDATE')
        expect(entityEvents[2]!.op).toBe('DELETE')

        // Verify timestamps are in order
        expect(entityEvents[1]!.ts).toBeGreaterThanOrEqual(entityEvents[0]!.ts)
        expect(entityEvents[2]!.ts).toBeGreaterThanOrEqual(entityEvents[1]!.ts)
      })

      it('should have unique event IDs (ULIDs)', async () => {
        await db.create('posts', { $type: 'Post', name: 'Post 1' })
        await db.create('posts', { $type: 'Post', name: 'Post 2' })
        await db.create('posts', { $type: 'Post', name: 'Post 3' })

        await db.disposeAsync()

        const events = await readEventsFromStorage()
        const eventIds = events.map((e) => e.id)

        // All IDs should be unique
        const uniqueIds = new Set(eventIds)
        expect(uniqueIds.size).toBe(eventIds.length)

        // ULIDs should be sortable strings (lexicographically ordered by time)
        const sortedIds = [...eventIds].sort()
        expect(sortedIds).toEqual(eventIds)
      })
    })

    // =========================================================================
    // Event Replay / Reconstruction Tests
    // =========================================================================

    describe('Event Replay and Reconstruction', () => {
      it('should reconstruct entity state from events', async () => {
        // Create entity
        const entity = await db.create('posts', {
          $type: 'Post',
          name: 'Replay Test',
          title: 'Original',
          views: 0,
        })

        // Update multiple times
        await db.update('posts', entity.$id, { $set: { title: 'First Update', views: 10 } })
        await db.update('posts', entity.$id, { $set: { views: 20 } })
        await db.update('posts', entity.$id, { $set: { title: 'Final Title', views: 30 } })

        await db.disposeAsync()

        // Read events and replay
        const events = await readEventsFromStorage()
        const entityId = entity.$id.split('/')[1]
        const entityEvents = events
          .filter((e) => e.target.includes(entityId!))
          .sort((a, b) => a.ts - b.ts)

        // Replay events to reconstruct final state
        let reconstructedState: Record<string, unknown> | null = null

        for (const event of entityEvents) {
          if (event.op === 'CREATE' || event.op === 'UPDATE') {
            reconstructedState = event.after
          } else if (event.op === 'DELETE') {
            reconstructedState = null
          }
        }

        // Verify reconstructed state matches expected final state
        expect(reconstructedState).not.toBeNull()
        expect(reconstructedState!.title).toBe('Final Title')
        expect(reconstructedState!.views).toBe(30)
        expect(reconstructedState!.name).toBe('Replay Test')
      })

      it('should reconstruct state at any point in time', async () => {
        const entity = await db.create('posts', {
          $type: 'Post',
          name: 'Time Travel',
          status: 'draft',
        })

        await db.update('posts', entity.$id, { $set: { status: 'review' } })
        await db.update('posts', entity.$id, { $set: { status: 'published' } })

        await db.disposeAsync()

        const events = await readEventsFromStorage()
        const entityId = entity.$id.split('/')[1]
        const entityEvents = events
          .filter((e) => e.target.includes(entityId!))
          .sort((a, b) => a.ts - b.ts)

        expect(entityEvents.length).toBe(3)

        // Reconstruct state after CREATE
        expect(entityEvents[0]!.after!.status).toBe('draft')

        // Reconstruct state after first UPDATE
        expect(entityEvents[1]!.after!.status).toBe('review')

        // Reconstruct state after second UPDATE
        expect(entityEvents[2]!.after!.status).toBe('published')

        // Verify before states for updates
        expect(entityEvents[1]!.before!.status).toBe('draft')
        expect(entityEvents[2]!.before!.status).toBe('review')
      })

      it('should handle entity deletion in replay', async () => {
        const entity = await db.create('posts', {
          $type: 'Post',
          name: 'Delete Replay',
          content: 'Will be deleted',
        })

        await db.delete('posts', entity.$id, { hard: true })

        await db.disposeAsync()

        const events = await readEventsFromStorage()
        const entityId = entity.$id.split('/')[1]
        const entityEvents = events
          .filter((e) => e.target.includes(entityId!))
          .sort((a, b) => a.ts - b.ts)

        // Replay to final state
        let finalState: Record<string, unknown> | null = null
        for (const event of entityEvents) {
          if (event.op === 'CREATE' || event.op === 'UPDATE') {
            finalState = event.after
          } else if (event.op === 'DELETE') {
            finalState = null
          }
        }

        // Entity should be null after hard delete
        expect(finalState).toBeNull()

        // But we can reconstruct state before deletion
        const createEvent = entityEvents.find((e) => e.op === 'CREATE')
        expect(createEvent!.after!.content).toBe('Will be deleted')
      })
    })

    // =========================================================================
    // Multiple Namespaces Test
    // =========================================================================

    describe('Multiple Namespaces', () => {
      it('should correctly store events for multiple namespaces', async () => {
        await db.create('posts', { $type: 'Post', name: 'Post 1' })
        await db.create('users', { $type: 'User', name: 'User 1' })
        await db.create('comments', { $type: 'Comment', name: 'Comment 1' })

        await db.disposeAsync()

        const events = await readEventsFromStorage()

        // All events should be in a single events.parquet file
        expect(events.length).toBe(3)

        // Verify different namespaces
        const targets = events.map((e) => e.target.split(':')[0])
        expect(targets).toContain('posts')
        expect(targets).toContain('users')
        expect(targets).toContain('comments')
      })
    })

    // =========================================================================
    // Actor and Metadata Tests
    // =========================================================================

    describe('Actor and Metadata', () => {
      it('should store actor information when provided', async () => {
        // Note: Actor is typically set via context/session, testing the storage mechanism
        await db.create('posts', { $type: 'Post', name: 'Actor Test' })

        await db.disposeAsync()

        const events = await readEventsFromStorage()
        const createEvent = events[0]

        // Actor may be null or a default value depending on implementation
        expect('actor' in createEvent!).toBe(true)
      })
    })

    // =========================================================================
    // Cross-Process Persistence Test
    // =========================================================================

    describe('Cross-Process Persistence', () => {
      it('should persist events that can be read by a new ParqueDB instance', async () => {
        // Create entity with first instance
        const entity = await db.create('posts', {
          $type: 'Post',
          name: 'Persist Test',
          title: 'Cross Process',
        })

        const entityId = entity.$id

        await db.disposeAsync()

        // Create new ParqueDB instance with same storage
        const db2 = new ParqueDB({ storage })

        // Read entity via get (which reconstructs from events)
        const found = await db2.get('posts', entityId)

        expect(found).not.toBeNull()
        expect(found!.name).toBe('Persist Test')
        expect(found!.title).toBe('Cross Process')

        await db2.disposeAsync()
      })
    })
  })
}

// =============================================================================
// Run Tests
// =============================================================================

// Always register cleanup after all tests
afterAll(async () => {
  await cleanupTestStorage()
})

// Run tests for FsBackend
runEventsParquetTests(createFsBackendFactory())

// Run tests for R2Backend (only if credentials are available)
describe.skipIf(!hasR2Credentials())('R2Backend Events Parquet Tests', () => {
  runEventsParquetTests(createR2BackendFactory())
})
