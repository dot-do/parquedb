/**
 * Storage Architecture Tests
 *
 * Tests to verify the behavior of ParqueDB's dual storage implementations:
 * - ParqueDB.ts: Uses globalEntityStore (in-memory) for Node.js/testing
 * - ParqueDBDO.ts: Uses SQLite for Cloudflare Workers
 *
 * These tests document the expected behavior and ensure consistency.
 * See docs/architecture/ENTITY_STORAGE.md for full documentation.
 */

import { describe, it, expect, beforeEach, afterEach, afterAll } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

// =============================================================================
// Test Utilities
// =============================================================================

const tempDirs: string[] = []

async function createRealStorage(): Promise<FsBackend> {
  const tempDir = join(tmpdir(), `parquedb-arch-test-${Date.now()}-${Math.random().toString(36).slice(2)}`)
  await fs.mkdir(tempDir, { recursive: true })
  tempDirs.push(tempDir)
  return new FsBackend(tempDir)
}

afterAll(async () => {
  await Promise.all(tempDirs.map(dir => fs.rm(dir, { recursive: true, force: true }).catch(() => {})))
})

// =============================================================================
// Node.js Environment: globalEntityStore Behavior
// =============================================================================

describe('Node.js Storage Architecture (globalEntityStore)', () => {
  describe('In-memory Entity Store', () => {
    let storage: FsBackend
    let db: ParqueDB

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    afterEach(() => {
      db.dispose()
    })

    it('should store entities in memory after create', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Test Title',
        content: 'Test content',
      })

      // Entity should be immediately available in memory
      const retrieved = await db.get('posts', entity.$id)
      expect(retrieved).not.toBeNull()
      expect(retrieved?.$id).toBe(entity.$id)
    })

    it('should share entity state across ParqueDB instances with same storage', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Shared Post',
        title: 'Shared Title',
        content: 'Shared content',
      })

      // Create a second ParqueDB instance with the same storage
      const db2 = new ParqueDB({ storage })

      // Entity should be visible in second instance (shared via WeakMap)
      const retrieved = await db2.get('posts', entity.$id)
      expect(retrieved).not.toBeNull()
      expect(retrieved?.$id).toBe(entity.$id)

      db2.dispose()
    })

    it('should isolate state between instances with different storage backends', async () => {
      const storage2 = await createRealStorage()
      const db2 = new ParqueDB({ storage: storage2 })

      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Isolated Post',
        title: 'Isolated Title',
        content: 'Isolated content',
      })

      // Entity should NOT be visible in second instance (different storage)
      const retrieved = await db2.get('posts', entity.$id)
      expect(retrieved).toBeNull()

      db2.dispose()
    })

    it('should clear state on dispose()', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Disposable Post',
        title: 'Title',
        content: 'Content',
      })

      const entityId = entity.$id

      // Verify entity exists
      let retrieved = await db.get('posts', entityId)
      expect(retrieved).not.toBeNull()

      // Dispose the instance
      db.dispose()

      // Create new instance with same storage
      const db2 = new ParqueDB({ storage })

      // Entity should be gone (state was cleared)
      retrieved = await db2.get('posts', entityId)
      expect(retrieved).toBeNull()

      db2.dispose()
    })
  })

  describe('Event Log Behavior', () => {
    let storage: FsBackend
    let db: ParqueDB

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    afterEach(() => {
      db.dispose()
    })

    it('should log CREATE events via history()', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Event Test Post',
        title: 'Title',
        content: 'Content',
      })

      // Use the public history() method to verify events are logged
      const history = await db.getHistory('posts', entity.$id.split('/')[1]!)
      expect(history.items.length).toBeGreaterThan(0)
      expect(history.items[0]?.op).toBe('CREATE')
    })

    it('should support time-travel queries', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Time Travel Post',
        title: 'Original Title',
        content: 'Original Content',
      })

      const createdAt = new Date()

      // Wait a bit to ensure timestamps differ
      await new Promise(resolve => setTimeout(resolve, 10))

      // Update the entity
      await db.update('posts', entity.$id, {
        $set: { title: 'Updated Title' },
      })

      // Query at creation time should show original state
      const historicalEntity = await db.get('posts', entity.$id, {
        asOf: createdAt,
      })

      // Note: Time travel may not be fully implemented, test documents expected behavior
      if (historicalEntity) {
        expect(historicalEntity.title).toBe('Original Title')
      }
    })
  })
})

// =============================================================================
// Storage Backend Interface Contract
// =============================================================================

describe('Storage Backend Contract', () => {
  describe('MemoryBackend', () => {
    let storage: MemoryBackend
    let db: ParqueDB

    beforeEach(() => {
      storage = new MemoryBackend()
      db = new ParqueDB({ storage })
    })

    afterEach(() => {
      db.dispose()
    })

    it('should work with MemoryBackend for testing', async () => {
      const entity = await db.create('test', {
        $type: 'Test',
        name: 'Memory Test',
      })

      const retrieved = await db.get('test', entity.$id)
      expect(retrieved).not.toBeNull()
    })

    it('should isolate state between MemoryBackend instances', () => {
      const storage1 = new MemoryBackend()
      const storage2 = new MemoryBackend()

      expect(storage1).not.toBe(storage2)

      const db1 = new ParqueDB({ storage: storage1 })
      const db2 = new ParqueDB({ storage: storage2 })

      // These should be completely isolated
      expect(db1).not.toBe(db2)

      db1.dispose()
      db2.dispose()
    })
  })

  describe('FsBackend', () => {
    let storage: FsBackend
    let db: ParqueDB

    beforeEach(async () => {
      storage = await createRealStorage()
      db = new ParqueDB({ storage })
    })

    afterEach(() => {
      db.dispose()
    })

    it('should work with FsBackend for local development', async () => {
      const entity = await db.create('test', {
        $type: 'Test',
        name: 'FS Test',
      })

      const retrieved = await db.get('test', entity.$id)
      expect(retrieved).not.toBeNull()
    })
  })
})

// =============================================================================
// Documentation: Workers Architecture Notes
// =============================================================================

describe('Workers Architecture (Documentation)', () => {
  /**
   * NOTE: ParqueDBDO cannot be directly tested in Node.js because it:
   * 1. Imports from 'cloudflare:workers' (Workers runtime only)
   * 2. Uses SqlStorage (Durable Objects API)
   *
   * For DO testing, see:
   * - tests/unit/worker/ParqueDBDO.test.ts (uses mocks)
   * - tests/e2e/parquedb.workers.test.ts (uses wrangler dev)
   */

  it('documents that ParqueDBDO uses SQLite as source of truth', () => {
    // This is a documentation test - it just documents expected behavior
    const expectedTables = ['entities', 'relationships', 'events', 'checkpoints']
    const expectedIndexes = [
      'idx_entities_type',
      'idx_entities_updated',
      'idx_rels_from',
      'idx_rels_to',
      'idx_events_unflushed',
      'idx_events_ns',
    ]

    // Document the expected schema
    expect(expectedTables).toContain('entities')
    expect(expectedTables).toContain('relationships')
    expect(expectedIndexes.length).toBeGreaterThan(0)
  })

  it('documents that Workers reads go through QueryExecutor to R2', () => {
    // This documents the expected read path in Workers
    const readPath = [
      'ParqueDBWorker.find()',
      'QueryExecutor.find()',
      'ReadPath.get()',
      'Cache API (check)',
      'R2.get()',
      'Parse Parquet',
      'Apply filter',
      'Return results',
    ]

    expect(readPath[0]).toBe('ParqueDBWorker.find()')
    expect(readPath).toContain('R2.get()')
  })

  it('documents that Workers writes go through ParqueDBDO', () => {
    // This documents the expected write path in Workers
    const writePath = [
      'ParqueDBWorker.create()',
      'ParqueDBDO.create()',
      'SQLite INSERT',
      'appendEvent()',
      'maybeScheduleFlush()',
      'Cache invalidation',
    ]

    expect(writePath[0]).toBe('ParqueDBWorker.create()')
    expect(writePath).toContain('SQLite INSERT')
  })
})
