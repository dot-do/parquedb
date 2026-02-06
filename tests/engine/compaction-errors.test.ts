/**
 * Compaction Error Path Test Suite
 *
 * Tests defensive behavior when things go wrong during compaction:
 *
 * 1. **Storage write failures**: Verifies that writeData()/writeRels()/writeEvents()
 *    errors propagate correctly and .compacting files are preserved for recovery.
 *
 * 2. **Engine error paths**: Verifies correct behavior for update/delete on
 *    non-existent entities, duplicate creates, and double deletes.
 *
 * These tests ensure the system fails loudly (no silent data loss) and preserves
 * state for recovery when compaction is interrupted by storage errors.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mkdtemp, writeFile, readFile, rm, access } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { compactDataTable } from '@/engine/compactor'
import type { StorageAdapter } from '@/engine/compactor'
import { compactRelationships } from '@/engine/compactor-rels'
import type { RelStorageAdapter } from '@/engine/compactor-rels'
import { compactEvents } from '@/engine/compactor-events'
import type { EventStorageAdapter } from '@/engine/compactor-events'
import { needsRecovery } from '@/engine/rotation'
import { ParqueEngine } from '@/engine/engine'
import type { DataLine, RelLine } from '@/engine/types'

// =============================================================================
// Helpers
// =============================================================================

let tempDir: string

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'compaction-errors-test-'))
})

afterEach(async () => {
  await rm(tempDir, { recursive: true, force: true })
})

/** Check if a file exists */
async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path)
    return true
  } catch {
    return false
  }
}

/** Write JSONL lines to a file */
async function writeJsonlFile(path: string, lines: Record<string, unknown>[]): Promise<void> {
  const content = lines.map(l => JSON.stringify(l)).join('\n') + '\n'
  await writeFile(path, content, 'utf-8')
}

/** Helper to create a DataLine with defaults */
function makeLine(overrides: Partial<DataLine> & { $id: string }): DataLine {
  return {
    $op: 'c',
    $v: 1,
    $ts: Date.now(),
    ...overrides,
  }
}

// =============================================================================
// Mock rotation for compactor tests
// =============================================================================

vi.mock('@/engine/rotation', async () => {
  const actual = await vi.importActual<typeof import('@/engine/rotation')>('@/engine/rotation')
  return {
    ...actual,
    rotate: vi.fn(),
    cleanup: vi.fn(),
  }
})

import { rotate, cleanup } from '@/engine/rotation'

// =============================================================================
// 1. Compaction Failure Tests - Data
// =============================================================================

describe('compactDataTable - writeData failure', () => {
  it('throws when writeData fails (does not silently swallow the error)', async () => {
    const compactingPath = join(tempDir, 'users.jsonl.compacting')

    // Create a .compacting file with valid JSONL data
    await writeJsonlFile(compactingPath, [
      makeLine({ $id: 'u1', name: 'Alice' }),
      makeLine({ $id: 'u2', name: 'Bob' }),
    ])

    // Configure rotation mock to return the compacting path
    vi.mocked(rotate).mockResolvedValue(compactingPath)
    vi.mocked(cleanup).mockResolvedValue(undefined)

    // Create a storage adapter that throws on writeData
    const failingStorage: StorageAdapter = {
      readData: async () => [],
      writeData: async () => {
        throw new Error('R2 PUT failed: network timeout')
      },
    }

    // compactDataTable should propagate the error, not swallow it
    await expect(
      compactDataTable(tempDir, 'users', failingStorage),
    ).rejects.toThrow('R2 PUT failed: network timeout')
  })

  it('does NOT call cleanup when writeData fails', async () => {
    const compactingPath = join(tempDir, 'users.jsonl.compacting')

    await writeJsonlFile(compactingPath, [
      makeLine({ $id: 'u1', name: 'Alice' }),
    ])

    vi.mocked(rotate).mockResolvedValue(compactingPath)
    vi.mocked(cleanup).mockResolvedValue(undefined)

    const failingStorage: StorageAdapter = {
      readData: async () => [],
      writeData: async () => {
        throw new Error('disk full')
      },
    }

    await expect(
      compactDataTable(tempDir, 'users', failingStorage),
    ).rejects.toThrow('disk full')

    // cleanup should NOT have been called since writeData failed before reaching it
    // (compactDataTable does not have try/catch, so the error propagates before cleanup)
    expect(cleanup).not.toHaveBeenCalled()
  })
})

// =============================================================================
// 2. Compaction Failure Tests - Relationships
// =============================================================================

describe('compactRelationships - writeRels failure', () => {
  it('throws when writeRels fails', async () => {
    const compactingPath = join(tempDir, 'rels.jsonl.compacting')

    // Write valid rel mutations to the compacting file
    const relLines: RelLine[] = [
      { $op: 'l', $ts: Date.now(), f: 'u1', p: 'author', r: 'posts', t: 'p1' },
    ]
    await writeJsonlFile(compactingPath, relLines as unknown as Record<string, unknown>[])

    vi.mocked(rotate).mockResolvedValue(compactingPath)
    vi.mocked(cleanup).mockResolvedValue(undefined)

    const failingStorage: RelStorageAdapter = {
      readRels: async () => [],
      writeRels: async () => {
        throw new Error('S3 PutObject failed: access denied')
      },
    }

    await expect(
      compactRelationships(tempDir, failingStorage),
    ).rejects.toThrow('S3 PutObject failed: access denied')
  })

  it('preserves .compacting file on writeRels failure (for recovery)', async () => {
    const compactingPath = join(tempDir, 'rels.jsonl.compacting')

    const relLines: RelLine[] = [
      { $op: 'l', $ts: Date.now(), f: 'u1', p: 'author', r: 'posts', t: 'p1' },
    ]
    await writeJsonlFile(compactingPath, relLines as unknown as Record<string, unknown>[])

    vi.mocked(rotate).mockResolvedValue(compactingPath)
    vi.mocked(cleanup).mockResolvedValue(undefined)

    const failingStorage: RelStorageAdapter = {
      readRels: async () => [],
      writeRels: async () => {
        throw new Error('write error')
      },
    }

    await expect(
      compactRelationships(tempDir, failingStorage),
    ).rejects.toThrow('write error')

    // cleanup should NOT have been called (error in try block, catch re-throws)
    expect(cleanup).not.toHaveBeenCalled()

    // The .compacting file should still be on disk (not cleaned up)
    expect(await fileExists(compactingPath)).toBe(true)
  })
})

// =============================================================================
// 3. Compaction Failure Tests - Events
// =============================================================================

describe('compactEvents - writeEvents failure', () => {
  it('throws when writeEvents fails', async () => {
    const compactingPath = join(tempDir, 'events.jsonl.compacting')

    await writeJsonlFile(compactingPath, [
      { id: 'e1', ts: Date.now(), op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' } },
    ])

    vi.mocked(rotate).mockResolvedValue(compactingPath)
    vi.mocked(cleanup).mockResolvedValue(undefined)

    const failingStorage: EventStorageAdapter = {
      readEvents: async () => [],
      writeEvents: async () => {
        throw new Error('R2 write quota exceeded')
      },
    }

    await expect(
      compactEvents(tempDir, failingStorage),
    ).rejects.toThrow('R2 write quota exceeded')
  })

  it('preserves .compacting file on writeEvents failure (for recovery)', async () => {
    const compactingPath = join(tempDir, 'events.jsonl.compacting')

    await writeJsonlFile(compactingPath, [
      { id: 'e1', ts: Date.now(), op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' } },
    ])

    vi.mocked(rotate).mockResolvedValue(compactingPath)
    vi.mocked(cleanup).mockResolvedValue(undefined)

    const failingStorage: EventStorageAdapter = {
      readEvents: async () => [],
      writeEvents: async () => {
        throw new Error('storage error')
      },
    }

    await expect(
      compactEvents(tempDir, failingStorage),
    ).rejects.toThrow('storage error')

    // cleanup should NOT be called on error
    expect(cleanup).not.toHaveBeenCalled()

    // .compacting file should still exist for recovery
    expect(await fileExists(compactingPath)).toBe(true)
  })
})

// =============================================================================
// 4. needsRecovery detects leftover .compacting files
// =============================================================================

describe('needsRecovery with .compacting files', () => {
  // Restore real rotation module for these tests
  beforeEach(() => {
    vi.restoreAllMocks()
  })

  it('returns true when a .compacting file exists', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')
    const compactingPath = jsonlPath + '.compacting'

    // Simulate a leftover .compacting file from a failed compaction
    await writeFile(compactingPath, 'leftover data', 'utf-8')

    const result = await needsRecovery(jsonlPath)
    expect(result).toBe(true)
  })

  it('returns false when no .compacting file exists', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')

    const result = await needsRecovery(jsonlPath)
    expect(result).toBe(false)
  })
})

// =============================================================================
// 5. Engine Error Path Tests
// =============================================================================

describe('ParqueEngine - error paths', () => {
  let engine: ParqueEngine

  beforeEach(async () => {
    // Restore real mocks for engine tests (engine uses real rotation internally)
    vi.restoreAllMocks()
    engine = new ParqueEngine({ dataDir: tempDir })
  })

  afterEach(async () => {
    await engine.close()
  })

  // ---------------------------------------------------------------------------
  // update() on non-existent entity
  // ---------------------------------------------------------------------------

  describe('engine.update() on non-existent entity', () => {
    it('throws "Entity not found" for a non-existent entity', async () => {
      await expect(
        engine.update('users', 'nonexistent-id', { $set: { name: 'Ghost' } }),
      ).rejects.toThrow('Entity not found: users/nonexistent-id')
    })

    it('throws "Entity not found" for a non-existent table', async () => {
      await expect(
        engine.update('nonexistent_table', 'some-id', { $set: { name: 'Ghost' } }),
      ).rejects.toThrow('Entity not found: nonexistent_table/some-id')
    })
  })

  // ---------------------------------------------------------------------------
  // create() with duplicate $id
  // ---------------------------------------------------------------------------

  describe('engine.create() with duplicate $id', () => {
    it('second create with same $id overwrites the first (append-only model)', async () => {
      // In an append-only model, a second create with the same $id is just
      // another mutation that will win during merge (higher $v or $ts)
      const first = await engine.create('users', { $id: 'dup-1', name: 'Alice', age: 30 })
      expect(first.$id).toBe('dup-1')
      expect(first.name).toBe('Alice')

      const second = await engine.create('users', { $id: 'dup-1', name: 'Alice v2', age: 31 })
      expect(second.$id).toBe('dup-1')
      expect(second.name).toBe('Alice v2')

      // The buffer should reflect the latest write
      const found = await engine.get('users', 'dup-1')
      expect(found).not.toBeNull()
      expect(found!.name).toBe('Alice v2')
      expect(found!.age).toBe(31)
    })

    it('find returns only one entity for duplicate $id (deduped by buffer)', async () => {
      await engine.create('users', { $id: 'dup-2', name: 'First' })
      await engine.create('users', { $id: 'dup-2', name: 'Second' })

      const results = await engine.find('users')
      // Buffer deduplicates by $id, so only one entity should be returned
      expect(results).toHaveLength(1)
      expect(results[0].name).toBe('Second')
    })
  })

  // ---------------------------------------------------------------------------
  // delete() twice on same entity (double-delete idempotency)
  // ---------------------------------------------------------------------------

  describe('engine.delete() on already-deleted entity', () => {
    it('throws "Entity not found" on second delete (entity is a tombstone)', async () => {
      const created = await engine.create('users', { name: 'Alice' })

      // First delete succeeds
      await engine.delete('users', created.$id)

      // Second delete should throw because the entity is now a tombstone
      await expect(
        engine.delete('users', created.$id),
      ).rejects.toThrow(`Entity not found: users/${created.$id}`)
    })

    it('entity is not findable after first delete', async () => {
      const created = await engine.create('users', { name: 'Alice' })
      await engine.delete('users', created.$id)

      const found = await engine.get('users', created.$id)
      expect(found).toBeNull()

      const results = await engine.find('users')
      expect(results).toHaveLength(0)
    })
  })

  // ---------------------------------------------------------------------------
  // update() on a deleted entity
  // ---------------------------------------------------------------------------

  describe('engine.update() on a deleted entity', () => {
    it('throws "Entity not found" when updating a tombstoned entity', async () => {
      const created = await engine.create('users', { name: 'Alice' })
      await engine.delete('users', created.$id)

      await expect(
        engine.update('users', created.$id, { $set: { name: 'Alice Resurrected' } }),
      ).rejects.toThrow(`Entity not found: users/${created.$id}`)
    })
  })

  // ---------------------------------------------------------------------------
  // delete() on a never-existed entity
  // ---------------------------------------------------------------------------

  describe('engine.delete() on a never-created entity', () => {
    it('throws "Entity not found" for a completely unknown entity', async () => {
      await expect(
        engine.delete('users', 'never-existed-id'),
      ).rejects.toThrow('Entity not found: users/never-existed-id')
    })
  })
})

// =============================================================================
// 6. Compaction read failure tests
// =============================================================================

describe('compactDataTable - readData failure', () => {
  it('throws when readData fails (error during reading existing data)', async () => {
    const compactingPath = join(tempDir, 'users.jsonl.compacting')

    await writeJsonlFile(compactingPath, [
      makeLine({ $id: 'u1', name: 'Alice' }),
    ])

    vi.mocked(rotate).mockResolvedValue(compactingPath)
    vi.mocked(cleanup).mockResolvedValue(undefined)

    const failingStorage: StorageAdapter = {
      readData: async () => {
        throw new Error('R2 GET failed: object not found')
      },
      writeData: async () => {},
    }

    await expect(
      compactDataTable(tempDir, 'users', failingStorage),
    ).rejects.toThrow('R2 GET failed: object not found')
  })
})

describe('compactRelationships - readRels failure', () => {
  it('throws when readRels fails', async () => {
    const compactingPath = join(tempDir, 'rels.jsonl.compacting')

    const relLines: RelLine[] = [
      { $op: 'l', $ts: Date.now(), f: 'u1', p: 'author', r: 'posts', t: 'p1' },
    ]
    await writeJsonlFile(compactingPath, relLines as unknown as Record<string, unknown>[])

    vi.mocked(rotate).mockResolvedValue(compactingPath)
    vi.mocked(cleanup).mockResolvedValue(undefined)

    const failingStorage: RelStorageAdapter = {
      readRels: async () => {
        throw new Error('R2 GET failed')
      },
      writeRels: async () => {},
    }

    await expect(
      compactRelationships(tempDir, failingStorage),
    ).rejects.toThrow('R2 GET failed')
  })
})
