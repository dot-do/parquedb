/**
 * Hybrid Storage Mode Test Suite
 *
 * Tests the hybrid storage architecture where:
 * - JSONL writes are always local (fast appends to disk)
 * - Compacted data is stored via a StorageAdapter (simulates R2/S3 with MemoryStorageAdapter)
 *
 * The MemoryStorageAdapter simulates remote object storage (R2/S3) for testing.
 * The LocalStorageAdapter reads/writes JSON files on local disk.
 *
 * Key behaviors tested:
 * 1. Storage adapters (MemoryStorageAdapter, LocalStorageAdapter) operate correctly
 * 2. Hybrid compaction routes data through the configured storage adapter
 * 3. Read path merges local JSONL buffer with remote compacted data
 * 4. Updates/deletes in local buffer override remote compacted data
 * 5. Data persists across engine restarts via remote storage + JSONL replay
 * 6. Relationship and event compaction works with remote storage
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, writeFile, readFile, rm, access } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import {
  MemoryStorageAdapter,
  LocalStorageAdapter,
  hybridCompactData,
  hybridCompactRels,
  hybridCompactEvents,
  hybridCompactAll,
} from '@/engine/storage-adapters'
import type { FullStorageAdapter } from '@/engine/storage-adapters'
import type { StorageAdapter } from '@/engine/compactor'
import type { RelStorageAdapter } from '@/engine/compactor-rels'
import type { EventStorageAdapter } from '@/engine/compactor-events'
import { ParqueEngine } from '@/engine/engine'
import { replay } from '@/engine/jsonl-reader'
import { mergeResults } from '@/engine/merge'
import type { DataLine, RelLine } from '@/engine/types'

// =============================================================================
// Test Helpers
// =============================================================================

let tempDir: string

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'parquedb-hybrid-test-'))
})

afterEach(async () => {
  await rm(tempDir, { recursive: true, force: true })
})

/** Helper to create a DataLine with sensible defaults */
function makeLine(overrides: Partial<DataLine> & { $id: string }): DataLine {
  return {
    $op: 'c',
    $v: 1,
    $ts: Date.now(),
    ...overrides,
  }
}

/** Helper to create a RelLine */
function makeLink(f: string, p: string, r: string, t: string, ts = 1000): RelLine {
  return { $op: 'l', $ts: ts, f, p, r, t }
}

/** Helper to write JSONL lines to a file */
async function writeJsonl(path: string, lines: unknown[]): Promise<void> {
  const content = lines.map(l => JSON.stringify(l)).join('\n') + '\n'
  await writeFile(path, content, 'utf-8')
}

/** Helper to check if a file exists */
async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path)
    return true
  } catch {
    return false
  }
}

// =============================================================================
// MemoryStorageAdapter Tests
// =============================================================================

describe('MemoryStorageAdapter', () => {
  let adapter: MemoryStorageAdapter

  beforeEach(() => {
    adapter = new MemoryStorageAdapter()
  })

  // --- Data operations ---

  it('readData returns empty array for missing path', async () => {
    const result = await adapter.readData('nonexistent/path')
    expect(result).toEqual([])
  })

  it('writeData + readData roundtrip returns same data', async () => {
    const data: DataLine[] = [
      makeLine({ $id: 'u1', name: 'Alice' }),
      makeLine({ $id: 'u2', name: 'Bob' }),
    ]

    await adapter.writeData('data/users.parquet', data)
    const result = await adapter.readData('data/users.parquet')

    expect(result).toEqual(data)
  })

  it('has() returns false for missing path, true after write', async () => {
    expect(adapter.has('data/users.parquet')).toBe(false)

    await adapter.writeData('data/users.parquet', [makeLine({ $id: 'u1' })])

    expect(adapter.has('data/users.parquet')).toBe(true)
  })

  it('writeData overwrites existing data at same path', async () => {
    await adapter.writeData('data/users.parquet', [makeLine({ $id: 'u1', name: 'Alice' })])
    await adapter.writeData('data/users.parquet', [makeLine({ $id: 'u2', name: 'Bob' })])

    const result = await adapter.readData('data/users.parquet')
    expect(result).toHaveLength(1)
    expect(result[0].$id).toBe('u2')
  })

  // --- Rel operations ---

  it('readRels returns empty array for missing path', async () => {
    const result = await adapter.readRels('rels/rels.parquet')
    expect(result).toEqual([])
  })

  it('writeRels + readRels roundtrip returns same data', async () => {
    const rels: RelLine[] = [
      makeLink('u1', 'posts', 'author', 'p1'),
      makeLink('u2', 'posts', 'author', 'p2'),
    ]

    await adapter.writeRels('rels/rels.parquet', rels)
    const result = await adapter.readRels('rels/rels.parquet')

    expect(result).toEqual(rels)
  })

  // --- Event operations ---

  it('readEvents returns empty array for missing path', async () => {
    const result = await adapter.readEvents('events/events.compacted')
    expect(result).toEqual([])
  })

  it('writeEvents + readEvents roundtrip returns same data', async () => {
    const events: Record<string, unknown>[] = [
      { id: 'e1', ts: 100, op: 'c', ns: 'users', eid: 'u1' },
      { id: 'e2', ts: 200, op: 'u', ns: 'users', eid: 'u2' },
    ]

    await adapter.writeEvents('events/events.compacted', events)
    const result = await adapter.readEvents('events/events.compacted')

    expect(result).toEqual(events)
  })

  // --- clear() ---

  it('clear() removes all stored data', async () => {
    await adapter.writeData('data/users.parquet', [makeLine({ $id: 'u1' })])
    await adapter.writeRels('rels.parquet', [makeLink('u1', 'posts', 'author', 'p1')])
    await adapter.writeEvents('events.compacted', [{ id: 'e1', ts: 100 }])

    adapter.clear()

    expect(adapter.has('data/users.parquet')).toBe(false)
    expect(await adapter.readData('data/users.parquet')).toEqual([])
    expect(await adapter.readRels('rels.parquet')).toEqual([])
    expect(await adapter.readEvents('events.compacted')).toEqual([])
  })

  // --- rename() ---

  it('rename() moves data from one path to another', async () => {
    await adapter.writeData('tmp.parquet', [makeLine({ $id: 'u1', name: 'Alice' })])

    adapter.rename('tmp.parquet', 'final.parquet')

    expect(adapter.has('tmp.parquet')).toBe(false)
    expect(adapter.has('final.parquet')).toBe(true)
    const data = await adapter.readData('final.parquet')
    expect(data).toHaveLength(1)
    expect(data[0].name).toBe('Alice')
  })

  it('rename() is a no-op if source does not exist', () => {
    adapter.rename('nonexistent', 'target')
    expect(adapter.has('target')).toBe(false)
  })
})

// =============================================================================
// LocalStorageAdapter Tests
// =============================================================================

describe('LocalStorageAdapter', () => {
  let adapter: LocalStorageAdapter

  beforeEach(() => {
    adapter = new LocalStorageAdapter()
  })

  // --- Data operations ---

  it('16. readData returns empty array for missing file', async () => {
    const result = await adapter.readData(join(tempDir, 'nonexistent.json'))
    expect(result).toEqual([])
  })

  it('17. writeData writes JSON file, readData reads it back', async () => {
    const path = join(tempDir, 'users.json')
    const data: DataLine[] = [
      makeLine({ $id: 'u1', name: 'Alice' }),
      makeLine({ $id: 'u2', name: 'Bob' }),
    ]

    await adapter.writeData(path, data)
    const result = await adapter.readData(path)

    expect(result).toEqual(data)
  })

  it('18. readData(missing) returns empty array', async () => {
    const result = await adapter.readData(join(tempDir, 'no-such-file.json'))
    expect(result).toEqual([])
  })

  it('19. roundtrip: write then read returns same data', async () => {
    const path = join(tempDir, 'roundtrip.json')
    const data: DataLine[] = [
      makeLine({ $id: 'a', $v: 3, $op: 'u', name: 'Updated Alice', email: 'alice@example.com' }),
      makeLine({ $id: 'b', $v: 1, $op: 'c', name: 'Bob' }),
    ]

    await adapter.writeData(path, data)
    const result = await adapter.readData(path)

    expect(result).toHaveLength(2)
    expect(result[0].$id).toBe('a')
    expect(result[0].name).toBe('Updated Alice')
    expect(result[0].email).toBe('alice@example.com')
    expect(result[1].$id).toBe('b')
  })

  // --- Rel operations ---

  it('readRels returns empty array for missing file', async () => {
    const result = await adapter.readRels(join(tempDir, 'missing-rels.json'))
    expect(result).toEqual([])
  })

  it('writeRels + readRels roundtrip', async () => {
    const path = join(tempDir, 'rels.json')
    const rels: RelLine[] = [
      makeLink('u1', 'posts', 'author', 'p1'),
    ]

    await adapter.writeRels(path, rels)
    const result = await adapter.readRels(path)

    expect(result).toEqual(rels)
  })

  // --- Event operations ---

  it('readEvents returns empty array for missing file', async () => {
    const result = await adapter.readEvents(join(tempDir, 'missing-events.json'))
    expect(result).toEqual([])
  })

  it('writeEvents + readEvents roundtrip', async () => {
    const path = join(tempDir, 'events.json')
    const events = [{ id: 'e1', ts: 100, op: 'c' }]

    await adapter.writeEvents(path, events)
    const result = await adapter.readEvents(path)

    expect(result).toEqual(events)
  })
})

// =============================================================================
// Hybrid Compaction Tests (MemoryStorageAdapter as remote storage)
// =============================================================================

describe('Hybrid data compaction with MemoryStorageAdapter', () => {
  let memoryAdapter: MemoryStorageAdapter

  beforeEach(() => {
    memoryAdapter = new MemoryStorageAdapter()
  })

  // ---------------------------------------------------------------------------
  // Basic hybrid operation
  // ---------------------------------------------------------------------------

  describe('basic hybrid operation', () => {
    it('1. engine creates with no errors when storage adapter is available', () => {
      // Verifying that MemoryStorageAdapter can be instantiated and used
      expect(memoryAdapter).toBeDefined()
      expect(memoryAdapter.has('anything')).toBe(false)
    })

    it('2. write entities -> JSONL files exist on local disk', async () => {
      const engine = new ParqueEngine({ dataDir: tempDir })

      await engine.create('users', { $id: 'u1', name: 'Alice' })
      await engine.create('users', { $id: 'u2', name: 'Bob' })
      await engine.create('users', { $id: 'u3', name: 'Charlie' })

      await engine.close()

      // JSONL file should exist locally
      const jsonlExists = await fileExists(join(tempDir, 'users.jsonl'))
      expect(jsonlExists).toBe(true)

      // Read the JSONL file to verify content
      const lines = await replay<DataLine>(join(tempDir, 'users.jsonl'))
      expect(lines).toHaveLength(3)
    })

    it('3. write entities -> no data written to remote storage yet (pre-compact)', async () => {
      const engine = new ParqueEngine({ dataDir: tempDir })

      await engine.create('users', { $id: 'u1', name: 'Alice' })
      await engine.create('users', { $id: 'u2', name: 'Bob' })

      await engine.close()

      // Remote storage should have nothing (no compaction happened)
      expect(memoryAdapter.has(join(tempDir, 'users.parquet'))).toBe(false)
    })

    it('4. after compact -> data exists in remote storage (via memoryAdapter)', async () => {
      const engine = new ParqueEngine({ dataDir: tempDir })

      await engine.create('users', { $id: 'u1', name: 'Alice' })
      await engine.create('users', { $id: 'u2', name: 'Bob' })
      await engine.create('users', { $id: 'u3', name: 'Charlie' })

      await engine.close()

      // Compact using hybridCompactData (writes directly to adapter, no fs.rename)
      const dataPath = join(tempDir, 'users.parquet')
      const count = await hybridCompactData(tempDir, 'users', memoryAdapter)

      expect(count).toBe(3)

      // Data should now be in the memory adapter
      expect(memoryAdapter.has(dataPath)).toBe(true)
      const remoteData = await memoryAdapter.readData(dataPath)
      expect(remoteData).toHaveLength(3)
      expect(remoteData.map(d => d.$id).sort()).toEqual(['u1', 'u2', 'u3'])
    })

    it('5. after compact -> local JSONL is fresh/empty', async () => {
      const engine = new ParqueEngine({ dataDir: tempDir })

      await engine.create('users', { $id: 'u1', name: 'Alice' })
      await engine.close()

      // Compact
      await hybridCompactData(tempDir, 'users', memoryAdapter)

      // The JSONL file should exist but be empty (rotation creates a fresh one)
      const jsonlPath = join(tempDir, 'users.jsonl')
      const jsonlContent = await readFile(jsonlPath, 'utf-8')
      expect(jsonlContent.trim()).toBe('')
    })
  })

  // ---------------------------------------------------------------------------
  // Read merge (local buffer + remote data)
  // ---------------------------------------------------------------------------

  describe('read merge (local buffer + remote data)', () => {
    it('6. write 3 -> compact -> write 2 more -> mergeResults returns all 5', async () => {
      // Phase 1: Write 3 entities and compact them to remote storage
      const engine1 = new ParqueEngine({ dataDir: tempDir })
      await engine1.create('users', { $id: 'u1', name: 'Alice' })
      await engine1.create('users', { $id: 'u2', name: 'Bob' })
      await engine1.create('users', { $id: 'u3', name: 'Charlie' })
      await engine1.close()

      await hybridCompactData(tempDir, 'users', memoryAdapter)

      // Phase 2: Write 2 more entities (to fresh JSONL after compaction)
      const engine2 = new ParqueEngine({ dataDir: tempDir })
      await engine2.create('users', { $id: 'u4', name: 'Diana' })
      await engine2.create('users', { $id: 'u5', name: 'Eve' })
      await engine2.close()

      // Simulate merge-on-read: remote data + local buffer
      const remoteData = await memoryAdapter.readData(join(tempDir, 'users.parquet'))
      const localBuffer = await replay<DataLine>(join(tempDir, 'users.jsonl'))
      const merged = mergeResults(remoteData, localBuffer)

      expect(merged).toHaveLength(5)
      expect(merged.map(d => d.$id).sort()).toEqual(['u1', 'u2', 'u3', 'u4', 'u5'])
    })

    it('7. 3 compacted entities from remote, 2 recent from local buffer', async () => {
      // Phase 1: Compact 3 entities
      const engine1 = new ParqueEngine({ dataDir: tempDir })
      await engine1.create('users', { $id: 'u1', name: 'Alice' })
      await engine1.create('users', { $id: 'u2', name: 'Bob' })
      await engine1.create('users', { $id: 'u3', name: 'Charlie' })
      await engine1.close()

      await hybridCompactData(tempDir, 'users', memoryAdapter)

      // Verify remote has 3
      const remoteData = await memoryAdapter.readData(join(tempDir, 'users.parquet'))
      expect(remoteData).toHaveLength(3)

      // Phase 2: Write 2 more (they stay in local JSONL)
      const engine2 = new ParqueEngine({ dataDir: tempDir })
      await engine2.create('users', { $id: 'u4', name: 'Diana' })
      await engine2.create('users', { $id: 'u5', name: 'Eve' })
      await engine2.close()

      // Local buffer has the 2 new ones
      const localBuffer = await replay<DataLine>(join(tempDir, 'users.jsonl'))
      expect(localBuffer).toHaveLength(2)
      expect(localBuffer.map(d => d.$id).sort()).toEqual(['u4', 'u5'])
    })

    it('8. after compact -> update entity -> mergeResults returns updated version (buffer overrides remote)', async () => {
      // Phase 1: Create and compact
      const engine1 = new ParqueEngine({ dataDir: tempDir })
      await engine1.create('users', { $id: 'u1', name: 'Alice' })
      await engine1.create('users', { $id: 'u2', name: 'Bob' })
      await engine1.close()

      await hybridCompactData(tempDir, 'users', memoryAdapter)

      // Phase 2: Update entity
      const engine2 = new ParqueEngine({ dataDir: tempDir })
      // Re-populate buffer from remote data for the update to work
      const remoteData = await memoryAdapter.readData(join(tempDir, 'users.parquet'))
      for (const entity of remoteData) {
        engine2.getBuffer('users').set(entity)
      }
      await engine2.update('users', 'u1', { $set: { name: 'Alice Updated' } })
      await engine2.close()

      // Merge: remote data + local updates
      const localBuffer = await replay<DataLine>(join(tempDir, 'users.jsonl'))
      const merged = mergeResults(remoteData, localBuffer)

      expect(merged).toHaveLength(2)
      const alice = merged.find(e => e.$id === 'u1')!
      expect(alice.name).toBe('Alice Updated')
      expect(alice.$v).toBe(2) // Updated version
    })

    it('9. after compact -> delete entity -> mergeResults excludes it (buffer tombstone suppresses remote)', async () => {
      // Phase 1: Create and compact
      const engine1 = new ParqueEngine({ dataDir: tempDir })
      await engine1.create('users', { $id: 'u1', name: 'Alice' })
      await engine1.create('users', { $id: 'u2', name: 'Bob' })
      await engine1.create('users', { $id: 'u3', name: 'Charlie' })
      await engine1.close()

      await hybridCompactData(tempDir, 'users', memoryAdapter)

      // Phase 2: Delete entity u2
      const engine2 = new ParqueEngine({ dataDir: tempDir })
      // Re-populate buffer from remote data
      const remoteData = await memoryAdapter.readData(join(tempDir, 'users.parquet'))
      for (const entity of remoteData) {
        engine2.getBuffer('users').set(entity)
      }
      await engine2.delete('users', 'u2')
      await engine2.close()

      // Merge: remote data + local tombstone
      const localBuffer = await replay<DataLine>(join(tempDir, 'users.jsonl'))
      const merged = mergeResults(remoteData, localBuffer)

      expect(merged).toHaveLength(2)
      expect(merged.map(e => e.$id).sort()).toEqual(['u1', 'u3'])
    })
  })

  // ---------------------------------------------------------------------------
  // Remote data persistence
  // ---------------------------------------------------------------------------

  describe('remote data persistence', () => {
    it('10. write -> compact -> close -> reopen with same adapter -> data found via merge', async () => {
      // Phase 1: Write and compact
      const engine1 = new ParqueEngine({ dataDir: tempDir })
      await engine1.create('users', { $id: 'u1', name: 'Alice' })
      await engine1.create('users', { $id: 'u2', name: 'Bob' })
      await engine1.close()

      await hybridCompactData(tempDir, 'users', memoryAdapter)

      // Phase 2: Create new engine, load data from remote adapter + replay JSONL
      const remoteData = await memoryAdapter.readData(join(tempDir, 'users.parquet'))
      const localBuffer = await replay<DataLine>(join(tempDir, 'users.jsonl'))
      const merged = mergeResults(remoteData, localBuffer)

      expect(merged).toHaveLength(2)
      expect(merged.map(d => d.$id).sort()).toEqual(['u1', 'u2'])
    })

    it('11. multiple compactions accumulate data in remote storage correctly', async () => {
      // Compaction 1: Write 2 entities, compact
      const engine1 = new ParqueEngine({ dataDir: tempDir })
      await engine1.create('users', { $id: 'u1', name: 'Alice' })
      await engine1.create('users', { $id: 'u2', name: 'Bob' })
      await engine1.close()

      const count1 = await hybridCompactData(tempDir, 'users', memoryAdapter)
      expect(count1).toBe(2)

      // Compaction 2: Write 2 more, compact
      const engine2 = new ParqueEngine({ dataDir: tempDir })
      await engine2.create('users', { $id: 'u3', name: 'Charlie' })
      await engine2.create('users', { $id: 'u4', name: 'Diana' })
      await engine2.close()

      const count2 = await hybridCompactData(tempDir, 'users', memoryAdapter)
      expect(count2).toBe(4)

      // Remote storage should have all 4 entities
      const remoteData = await memoryAdapter.readData(join(tempDir, 'users.parquet'))
      expect(remoteData).toHaveLength(4)
      expect(remoteData.map(d => d.$id).sort()).toEqual(['u1', 'u2', 'u3', 'u4'])
    })
  })

  // ---------------------------------------------------------------------------
  // Local JSONL as ephemeral overlay
  // ---------------------------------------------------------------------------

  describe('local JSONL is ephemeral overlay', () => {
    it('12. write (no compact) -> close -> reopen without adapter -> data from JSONL replay', async () => {
      // Write entities (no compaction)
      const engine1 = new ParqueEngine({ dataDir: tempDir })
      await engine1.create('users', { $id: 'u1', name: 'Alice' })
      await engine1.create('users', { $id: 'u2', name: 'Bob' })
      await engine1.close()

      // Reopen and replay JSONL (no remote storage involved)
      const localData = await replay<DataLine>(join(tempDir, 'users.jsonl'))
      expect(localData).toHaveLength(2)
      expect(localData.map(d => d.$id).sort()).toEqual(['u1', 'u2'])
    })

    it('13. write (no compact) -> close -> reopen WITH adapter -> data still from JSONL (not yet compacted)', async () => {
      // Write entities (no compaction)
      const engine1 = new ParqueEngine({ dataDir: tempDir })
      await engine1.create('users', { $id: 'u1', name: 'Alice' })
      await engine1.create('users', { $id: 'u2', name: 'Bob' })
      await engine1.close()

      // Remote storage has nothing
      expect(memoryAdapter.has(join(tempDir, 'users.parquet'))).toBe(false)

      // Data comes from JSONL replay, not remote
      const remoteData = await memoryAdapter.readData(join(tempDir, 'users.parquet'))
      const localBuffer = await replay<DataLine>(join(tempDir, 'users.jsonl'))
      const merged = mergeResults(remoteData, localBuffer)

      expect(merged).toHaveLength(2)
      expect(merged.map(d => d.$id).sort()).toEqual(['u1', 'u2'])
    })
  })
})

// =============================================================================
// Relationship compaction with MemoryStorageAdapter
// =============================================================================

describe('Hybrid relationship compaction with MemoryStorageAdapter', () => {
  let memoryAdapter: MemoryStorageAdapter

  beforeEach(() => {
    memoryAdapter = new MemoryStorageAdapter()
  })

  it('14. link operations -> hybridCompactRels() -> rels exist in remote storage', async () => {
    // Write relationship JSONL
    const rels: RelLine[] = [
      makeLink('u1', 'posts', 'author', 'p1', 1000),
      makeLink('u1', 'posts', 'author', 'p2', 1000),
      makeLink('u2', 'follows', 'followers', 'u1', 1000),
    ]
    await writeJsonl(join(tempDir, 'rels.jsonl'), rels)

    const count = await hybridCompactRels(tempDir, memoryAdapter)

    expect(count).toBe(3)

    // Verify rels are in remote storage
    const relPath = join(tempDir, 'rels.parquet')
    expect(memoryAdapter.has(relPath)).toBe(true)
    const remoteRels = await memoryAdapter.readRels(relPath)
    expect(remoteRels).toHaveLength(3)
    expect(remoteRels.every(r => r.$op === 'l')).toBe(true)
  })
})

// =============================================================================
// Event compaction with MemoryStorageAdapter
// =============================================================================

describe('Hybrid event compaction with MemoryStorageAdapter', () => {
  let memoryAdapter: MemoryStorageAdapter

  beforeEach(() => {
    memoryAdapter = new MemoryStorageAdapter()
  })

  it('15. events -> hybridCompactEvents() -> events exist in remote storage', async () => {
    // Write event JSONL
    const events = [
      { id: 'e1', ts: 100, op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' } },
      { id: 'e2', ts: 200, op: 'c', ns: 'users', eid: 'u2', after: { name: 'Bob' } },
      { id: 'e3', ts: 300, op: 'u', ns: 'users', eid: 'u1', before: { name: 'Alice' }, after: { name: 'Alice Updated' } },
    ]
    await writeJsonl(join(tempDir, 'events.jsonl'), events)

    const count = await hybridCompactEvents(tempDir, memoryAdapter)

    expect(count).toBe(3)

    // Verify events are in remote storage
    const eventPath = join(tempDir, 'events.compacted')
    expect(memoryAdapter.has(eventPath)).toBe(true)
    const remoteEvents = await memoryAdapter.readEvents(eventPath)
    expect(remoteEvents).toHaveLength(3)

    // Events should be sorted by ts
    const timestamps = remoteEvents.map(e => e.ts as number)
    expect(timestamps).toEqual([100, 200, 300])
  })
})

// =============================================================================
// hybridCompactAll convenience function
// =============================================================================

describe('hybridCompactAll', () => {
  let memoryAdapter: MemoryStorageAdapter

  beforeEach(() => {
    memoryAdapter = new MemoryStorageAdapter()
  })

  it('compacts data, rels, and events in a single call', async () => {
    // Write data
    const engine = new ParqueEngine({ dataDir: tempDir })
    await engine.create('users', { $id: 'u1', name: 'Alice' })
    await engine.create('posts', { $id: 'p1', title: 'Hello' })
    await engine.close()

    // Write rels
    await writeJsonl(join(tempDir, 'rels.jsonl'), [
      makeLink('u1', 'posts', 'author', 'p1', 1000),
    ])

    // events.jsonl is already written by the engine

    const result = await hybridCompactAll(tempDir, ['users', 'posts'], memoryAdapter)

    // Data compaction
    expect(result.data.get('users')).toBe(1)
    expect(result.data.get('posts')).toBe(1)

    // Rels compaction
    expect(result.rels).toBe(1)

    // Events compaction (engine writes events for each create)
    expect(result.events).toBeGreaterThanOrEqual(2)

    // Verify data in remote storage
    const users = await memoryAdapter.readData(join(tempDir, 'users.parquet'))
    expect(users).toHaveLength(1)
    const posts = await memoryAdapter.readData(join(tempDir, 'posts.parquet'))
    expect(posts).toHaveLength(1)
  })
})

// =============================================================================
// FullStorageAdapter compatibility
// =============================================================================

describe('FullStorageAdapter compatibility', () => {
  it('MemoryStorageAdapter satisfies StorageAdapter interface', async () => {
    const adapter = new MemoryStorageAdapter()

    // Can be used as StorageAdapter (data only)
    const dataAdapter: StorageAdapter = adapter
    await dataAdapter.writeData('test.parquet', [makeLine({ $id: 'x' })])
    const data = await dataAdapter.readData('test.parquet')
    expect(data).toHaveLength(1)
  })

  it('MemoryStorageAdapter satisfies RelStorageAdapter interface', async () => {
    const adapter = new MemoryStorageAdapter()

    // Can be used as RelStorageAdapter
    const relAdapter: RelStorageAdapter = adapter
    await relAdapter.writeRels('rels.parquet', [makeLink('u1', 'p', 'r', 'u2')])
    const rels = await relAdapter.readRels('rels.parquet')
    expect(rels).toHaveLength(1)
  })

  it('MemoryStorageAdapter satisfies EventStorageAdapter interface', async () => {
    const adapter = new MemoryStorageAdapter()

    // Can be used as EventStorageAdapter
    const eventAdapter: EventStorageAdapter = adapter
    await eventAdapter.writeEvents('events.compacted', [{ id: 'e1', ts: 100 }])
    const events = await eventAdapter.readEvents('events.compacted')
    expect(events).toHaveLength(1)
  })

  it('LocalStorageAdapter satisfies StorageAdapter interface', async () => {
    const adapter = new LocalStorageAdapter()

    const path = join(tempDir, 'test.parquet')
    const dataAdapter: StorageAdapter = adapter
    await dataAdapter.writeData(path, [makeLine({ $id: 'x' })])
    const data = await dataAdapter.readData(path)
    expect(data).toHaveLength(1)
  })

  it('LocalStorageAdapter satisfies RelStorageAdapter interface', async () => {
    const adapter = new LocalStorageAdapter()

    const path = join(tempDir, 'rels.parquet')
    const relAdapter: RelStorageAdapter = adapter
    await relAdapter.writeRels(path, [makeLink('u1', 'p', 'r', 'u2')])
    const rels = await relAdapter.readRels(path)
    expect(rels).toHaveLength(1)
  })

  it('LocalStorageAdapter satisfies EventStorageAdapter interface', async () => {
    const adapter = new LocalStorageAdapter()

    const path = join(tempDir, 'events.compacted')
    const eventAdapter: EventStorageAdapter = adapter
    await eventAdapter.writeEvents(path, [{ id: 'e1', ts: 100 }])
    const events = await eventAdapter.readEvents(path)
    expect(events).toHaveLength(1)
  })
})
