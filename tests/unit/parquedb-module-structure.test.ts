/**
 * ParqueDB Module Structure Tests (RED Phase)
 *
 * These tests verify that the ParqueDB module has been properly reorganized
 * into focused, modular files for maintainability. The goal is to split
 * large files (>1000 lines) into smaller, focused modules (<500 lines each).
 *
 * Expected structure after reorganization:
 * - src/ParqueDB/read-path.ts    - Read operations (find, get, query)
 * - src/ParqueDB/write-path.ts   - Write operations (create, update, delete)
 * - src/ParqueDB/snapshots.ts    - Snapshot logic (already exists, verify exports)
 *
 * This follows TDD RED-GREEN-REFACTOR:
 * - RED:    Tests fail (files don't exist or structure is wrong)
 * - GREEN:  Create/reorganize files to pass tests
 * - REFACTOR: Clean up while keeping tests green
 */

import { describe, it, expect, afterEach, afterAll } from 'vitest'
import * as fs from 'node:fs'
import * as path from 'node:path'

// =============================================================================
// Test Constants
// =============================================================================

const PARQUEDB_DIR = path.resolve(__dirname, '../../src/ParqueDB')
const MAX_FILE_LINES = 500

// Expected modular files after reorganization
const EXPECTED_MODULES = {
  'read-path.ts': {
    description: 'Read operations module',
    expectedExports: [
      'findEntities',
      'getEntity',
      'queryEntities',
    ],
  },
  'write-path.ts': {
    description: 'Write operations module',
    expectedExports: [
      'createEntity',
      'updateEntity',
      'deleteEntity',
      'deleteManyEntities',
      'restoreEntity',
    ],
  },
  'snapshots.ts': {
    description: 'Snapshot management module',
    expectedExports: [
      'SnapshotManagerImpl',
      'createSnapshotManager',
      'SnapshotContext',
    ],
  },
}

// Files that should be under the line limit after reorganization
const FILES_TO_CHECK_SIZE = [
  'read-path.ts',
  'write-path.ts',
  'snapshots.ts',
  'core.ts',
  'entity-operations.ts',
  'event-operations.ts',
  'events.ts',
]

// Track resources for cleanup
let cleanupFns: Array<() => Promise<void>> = []

afterEach(async () => {
  // Run all registered cleanup functions
  for (const cleanup of cleanupFns) {
    try {
      await cleanup()
    } catch {
      // Ignore cleanup errors
    }
  }
  cleanupFns = []
})

afterAll(async () => {
  // Final cleanup
  for (const cleanup of cleanupFns) {
    try {
      await cleanup()
    } catch {
      // Ignore cleanup errors
    }
  }
})

// =============================================================================
// Helper Functions
// =============================================================================

function getFileLineCount(filePath: string): number {
  if (!fs.existsSync(filePath)) {
    return -1 // File doesn't exist
  }
  const content = fs.readFileSync(filePath, 'utf-8')
  return content.split('\n').length
}

function fileExists(filename: string): boolean {
  const filePath = path.join(PARQUEDB_DIR, filename)
  return fs.existsSync(filePath)
}

async function getModuleExports(filename: string): Promise<string[]> {
  const filePath = path.join(PARQUEDB_DIR, filename)
  if (!fs.existsSync(filePath)) {
    return []
  }

  try {
    // Dynamic import to get actual exports
    const modulePath = `../../src/ParqueDB/${filename.replace('.ts', '')}`
    const module = await import(modulePath)
    return Object.keys(module)
  } catch {
    return []
  }
}

function getExportsFromSource(filename: string): string[] {
  const filePath = path.join(PARQUEDB_DIR, filename)
  if (!fs.existsSync(filePath)) {
    return []
  }

  const content = fs.readFileSync(filePath, 'utf-8')
  const exports: string[] = []

  // Match export function/class/const/interface/type declarations
  const exportRegex = /export\s+(?:async\s+)?(?:function|class|const|interface|type)\s+(\w+)/g
  let match
  while ((match = exportRegex.exec(content)) !== null) {
    if (match[1]) {
      exports.push(match[1])
    }
  }

  // Match named exports like: export { foo, bar }
  const namedExportRegex = /export\s*\{([^}]+)\}/g
  while ((match = namedExportRegex.exec(content)) !== null) {
    if (match[1]) {
      const names = match[1].split(',').map((s) => s.trim().split(/\s+as\s+/)[0].trim())
      exports.push(...names.filter((n) => n && !n.startsWith('type ')))
    }
  }

  return [...new Set(exports)]
}

// =============================================================================
// Tests: File Structure Verification
// =============================================================================

describe('ParqueDB Module Structure', () => {
  describe('Required files exist', () => {
    it('should have read-path.ts for read operations', () => {
      expect(
        fileExists('read-path.ts'),
        'read-path.ts should exist in src/ParqueDB/'
      ).toBe(true)
    })

    it('should have write-path.ts for write operations', () => {
      expect(
        fileExists('write-path.ts'),
        'write-path.ts should exist in src/ParqueDB/'
      ).toBe(true)
    })

    it('should have snapshots.ts for snapshot logic', () => {
      expect(
        fileExists('snapshots.ts'),
        'snapshots.ts should exist in src/ParqueDB/'
      ).toBe(true)
    })
  })

  describe('File size limits', () => {
    for (const filename of FILES_TO_CHECK_SIZE) {
      it(`${filename} should be under ${MAX_FILE_LINES} lines`, () => {
        const filePath = path.join(PARQUEDB_DIR, filename)
        const lineCount = getFileLineCount(filePath)

        if (lineCount === -1) {
          // File doesn't exist yet - this will be caught by other tests
          expect(lineCount).toBeGreaterThan(0)
          return
        }

        expect(
          lineCount,
          `${filename} has ${lineCount} lines, should be under ${MAX_FILE_LINES}`
        ).toBeLessThanOrEqual(MAX_FILE_LINES)
      })
    }
  })

  describe('Module exports', () => {
    describe('read-path.ts exports', () => {
      const expectedExports = EXPECTED_MODULES['read-path.ts'].expectedExports

      for (const exportName of expectedExports) {
        it(`should export ${exportName}`, () => {
          const exports = getExportsFromSource('read-path.ts')
          expect(
            exports,
            `read-path.ts should export ${exportName}`
          ).toContain(exportName)
        })
      }
    })

    describe('write-path.ts exports', () => {
      const expectedExports = EXPECTED_MODULES['write-path.ts'].expectedExports

      for (const exportName of expectedExports) {
        it(`should export ${exportName}`, () => {
          const exports = getExportsFromSource('write-path.ts')
          expect(
            exports,
            `write-path.ts should export ${exportName}`
          ).toContain(exportName)
        })
      }
    })

    describe('snapshots.ts exports', () => {
      const expectedExports = EXPECTED_MODULES['snapshots.ts'].expectedExports

      for (const exportName of expectedExports) {
        it(`should export ${exportName}`, () => {
          const exports = getExportsFromSource('snapshots.ts')
          expect(
            exports,
            `snapshots.ts should export ${exportName}`
          ).toContain(exportName)
        })
      }
    })
  })
})

// =============================================================================
// Tests: Public API Compatibility
// =============================================================================

describe('ParqueDB Public API Compatibility', () => {
  it('should export ParqueDBImpl from ParqueDB/core', async () => {
    const { ParqueDBImpl } = await import('../../src/ParqueDB/core')
    expect(ParqueDBImpl).toBeDefined()
    expect(typeof ParqueDBImpl).toBe('function')
  })

  it('should export SnapshotManagerImpl from ParqueDB/snapshots', async () => {
    const { SnapshotManagerImpl } = await import('../../src/ParqueDB/snapshots')
    expect(SnapshotManagerImpl).toBeDefined()
    expect(typeof SnapshotManagerImpl).toBe('function')
  })

  it('should export CollectionImpl from ParqueDB/collection', async () => {
    const { CollectionImpl } = await import('../../src/ParqueDB/collection')
    expect(CollectionImpl).toBeDefined()
    expect(typeof CollectionImpl).toBe('function')
  })

  it('should export entity operation helpers from ParqueDB/index', async () => {
    const module = await import('../../src/ParqueDB/index')

    expect(module.deriveTypeFromNamespace).toBeDefined()
    expect(typeof module.deriveTypeFromNamespace).toBe('function')

    expect(module.validateFieldType).toBeDefined()
    expect(typeof module.validateFieldType).toBe('function')

    expect(module.applySchemaDefaults).toBeDefined()
    expect(typeof module.applySchemaDefaults).toBe('function')
  })

  it('should export event operation helpers from ParqueDB/index', async () => {
    const module = await import('../../src/ParqueDB/index')

    expect(module.recordEvent).toBeDefined()
    expect(typeof module.recordEvent).toBe('function')

    expect(module.reconstructEntityAtTime).toBeDefined()
    expect(typeof module.reconstructEntityAtTime).toBe('function')

    expect(module.getEntityHistory).toBeDefined()
    expect(typeof module.getEntityHistory).toBe('function')
  })

  it('should export relationship operation helpers from ParqueDB/index', async () => {
    const module = await import('../../src/ParqueDB/index')

    expect(module.indexRelationshipsForEntity).toBeDefined()
    expect(typeof module.indexRelationshipsForEntity).toBe('function')

    expect(module.hydrateEntity).toBeDefined()
    expect(typeof module.hydrateEntity).toBe('function')

    expect(module.getRelatedEntities).toBeDefined()
    expect(typeof module.getRelatedEntities).toBe('function')
  })

  it('should export validation helpers from ParqueDB/index', async () => {
    const module = await import('../../src/ParqueDB/index')

    expect(module.validateNamespace).toBeDefined()
    expect(typeof module.validateNamespace).toBe('function')

    expect(module.validateFilter).toBeDefined()
    expect(typeof module.validateFilter).toBe('function')

    expect(module.normalizeNamespace).toBeDefined()
    expect(typeof module.normalizeNamespace).toBe('function')
  })

  it('should export store utilities from ParqueDB/index', async () => {
    const module = await import('../../src/ParqueDB/index')

    expect(module.getEntityStore).toBeDefined()
    expect(typeof module.getEntityStore).toBe('function')

    expect(module.getEventStore).toBeDefined()
    expect(typeof module.getEventStore).toBe('function')

    expect(module.clearGlobalState).toBeDefined()
    expect(typeof module.clearGlobalState).toBe('function')
  })
})

// =============================================================================
// Tests: Functional Integration After Reorganization
// =============================================================================

describe('ParqueDB Functional Tests After Reorganization', () => {
  it('should be able to instantiate ParqueDBImpl', async () => {
    const { ParqueDBImpl } = await import('../../src/ParqueDB/core')
    const { MemoryBackend } = await import('../../src/storage')

    const storage = new MemoryBackend()
    const db = new ParqueDBImpl({ storage })

    expect(db).toBeInstanceOf(ParqueDBImpl)

    // Register cleanup
    cleanupFns.push(async () => {
      await db.disposeAsync()
    })
  })

  it('should be able to create and retrieve entities', async () => {
    const { ParqueDBImpl } = await import('../../src/ParqueDB/core')
    const { clearGlobalState } = await import('../../src/ParqueDB/store')
    const { MemoryBackend } = await import('../../src/storage')

    const storage = new MemoryBackend()
    clearGlobalState(storage)

    const db = new ParqueDBImpl({ storage })

    // Register cleanup
    cleanupFns.push(async () => {
      await db.disposeAsync()
      clearGlobalState(storage)
    })

    // Create an entity
    const created = await db.create('users', {
      name: 'Test User',
      email: 'test@example.com',
    })

    expect(created).toBeDefined()
    expect(created.$id).toBeDefined()
    expect(created.name).toBe('Test User')

    // Retrieve the entity
    const retrieved = await db.get('users', created.$id)
    expect(retrieved).toBeDefined()
    expect(retrieved?.name).toBe('Test User')
  })

  it('should be able to find entities with filters', async () => {
    const { ParqueDBImpl } = await import('../../src/ParqueDB/core')
    const { clearGlobalState } = await import('../../src/ParqueDB/store')
    const { MemoryBackend } = await import('../../src/storage')

    const storage = new MemoryBackend()
    clearGlobalState(storage)

    const db = new ParqueDBImpl({ storage })

    // Register cleanup
    cleanupFns.push(async () => {
      await db.disposeAsync()
      clearGlobalState(storage)
    })

    // Create multiple entities
    await db.create('posts', { title: 'Post 1', status: 'published' })
    await db.create('posts', { title: 'Post 2', status: 'draft' })
    await db.create('posts', { title: 'Post 3', status: 'published' })

    // Find with filter
    const published = await db.find('posts', { status: 'published' })

    expect(published.data).toHaveLength(2)
    expect(published.data.every((p) => p.status === 'published')).toBe(true)
  })

  it('should be able to update entities', async () => {
    const { ParqueDBImpl } = await import('../../src/ParqueDB/core')
    const { clearGlobalState } = await import('../../src/ParqueDB/store')
    const { MemoryBackend } = await import('../../src/storage')

    const storage = new MemoryBackend()
    clearGlobalState(storage)

    const db = new ParqueDBImpl({ storage })

    // Register cleanup
    cleanupFns.push(async () => {
      await db.disposeAsync()
      clearGlobalState(storage)
    })

    // Create an entity
    const created = await db.create('users', {
      name: 'Original Name',
      count: 0,
    })

    // Update with $set
    const updated = await db.update('users', created.$id, {
      $set: { name: 'Updated Name' },
      $inc: { count: 1 },
    })

    expect(updated.name).toBe('Updated Name')
    expect(updated.count).toBe(1)
  })

  it('should be able to delete entities', async () => {
    const { ParqueDBImpl } = await import('../../src/ParqueDB/core')
    const { clearGlobalState } = await import('../../src/ParqueDB/store')
    const { MemoryBackend } = await import('../../src/storage')

    const storage = new MemoryBackend()
    clearGlobalState(storage)

    const db = new ParqueDBImpl({ storage })

    // Register cleanup
    cleanupFns.push(async () => {
      await db.disposeAsync()
      clearGlobalState(storage)
    })

    // Create an entity
    const created = await db.create('users', { name: 'To Delete' })

    // Delete it
    const deleteResult = await db.delete('users', created.$id)
    expect(deleteResult.deletedCount).toBe(1)

    // Verify it's gone
    const retrieved = await db.get('users', created.$id)
    expect(retrieved).toBeNull()
  })

  it('should be able to create and use snapshots', async () => {
    const { ParqueDBImpl } = await import('../../src/ParqueDB/core')
    const { clearGlobalState } = await import('../../src/ParqueDB/store')
    const { MemoryBackend } = await import('../../src/storage')

    const storage = new MemoryBackend()
    clearGlobalState(storage)

    const db = new ParqueDBImpl({ storage })

    // Register cleanup
    cleanupFns.push(async () => {
      await db.disposeAsync()
      clearGlobalState(storage)
    })

    // Create an entity
    const created = await db.create('users', { name: 'Snapshot User' })

    // Create a snapshot
    const snapshotManager = db.getSnapshotManager()
    const snapshot = await snapshotManager.createSnapshot()

    expect(snapshot).toBeDefined()
    expect(snapshot.id).toBeDefined()
    expect(snapshot.timestamp).toBeDefined()

    // Verify we can retrieve entity state at snapshot
    const entityAtSnapshot = await snapshotManager.getEntityAtSnapshot(
      created.$id,
      snapshot.id
    )
    expect(entityAtSnapshot?.name).toBe('Snapshot User')
  })
})
