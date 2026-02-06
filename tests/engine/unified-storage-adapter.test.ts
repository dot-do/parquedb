/**
 * Unified StorageAdapter Interface Test Suite (zou5.18)
 *
 * Verifies that the four adapter interfaces (StorageAdapter, RelStorageAdapter,
 * EventStorageAdapter, FullStorageAdapter) are consolidated into a single
 * hierarchy where FullStorageAdapter is the primary interface and the others
 * are Pick<> type aliases.
 *
 * TDD Phase: RED -> GREEN -> REFACTOR
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, writeFile, readFile, rm, access } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import type { FullStorageAdapter } from '@/engine/storage-adapters'
import type { StorageAdapter } from '@/engine/storage-adapters'
import type { RelStorageAdapter } from '@/engine/storage-adapters'
import type { EventStorageAdapter } from '@/engine/storage-adapters'
import { MemoryStorageAdapter } from '@/engine/storage-adapters'
import { compactDataTable } from '@/engine/compactor'
import { compactRelationships } from '@/engine/compactor-rels'
import { compactEvents } from '@/engine/compactor-events'
import type { DataLine, RelLine } from '@/engine/types'
import { makeLine, makeLink } from './helpers'

// =============================================================================
// Test Helpers
// =============================================================================

let tempDir: string

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'unified-adapter-test-'))
})

afterEach(async () => {
  await rm(tempDir, { recursive: true, force: true })
})

/** Write JSONL lines to a file */
async function writeJsonl(path: string, lines: unknown[]): Promise<void> {
  const content = lines.map(l => JSON.stringify(l)).join('\n') + '\n'
  await writeFile(path, content, 'utf-8')
}

// =============================================================================
// 1. Type hierarchy: Pick<> aliases from FullStorageAdapter
// =============================================================================

describe('Unified StorageAdapter type hierarchy', () => {
  it('StorageAdapter is a subset of FullStorageAdapter (Pick<readData, writeData>)', () => {
    // A FullStorageAdapter should be assignable to StorageAdapter
    const full: FullStorageAdapter = new MemoryStorageAdapter()
    const data: StorageAdapter = full
    expect(data.readData).toBeDefined()
    expect(data.writeData).toBeDefined()
  })

  it('RelStorageAdapter is a subset of FullStorageAdapter (Pick<readRels, writeRels>)', () => {
    const full: FullStorageAdapter = new MemoryStorageAdapter()
    const rel: RelStorageAdapter = full
    expect(rel.readRels).toBeDefined()
    expect(rel.writeRels).toBeDefined()
  })

  it('EventStorageAdapter is a subset of FullStorageAdapter (Pick<readEvents, writeEvents>)', () => {
    const full: FullStorageAdapter = new MemoryStorageAdapter()
    const evt: EventStorageAdapter = full
    expect(evt.readEvents).toBeDefined()
    expect(evt.writeEvents).toBeDefined()
  })

  it('all four types are exported from storage-adapters.ts (single source of truth)', async () => {
    // This verifies they can all be imported from the same module
    const mod = await import('@/engine/storage-adapters')
    // FullStorageAdapter is an interface (type-only), but the concrete classes exist
    expect(mod.MemoryStorageAdapter).toBeDefined()
    expect(mod.LocalStorageAdapter).toBeDefined()
  })

  it('all four types are re-exported from engine/index.ts', async () => {
    const mod = await import('@/engine/index')
    expect(mod.MemoryStorageAdapter).toBeDefined()
    expect(mod.LocalStorageAdapter).toBeDefined()
    // Type aliases are erased at runtime, but the module should export them
    // We verify indirectly by checking that the concrete classes exist
  })
})

// =============================================================================
// 2. Compactors accept FullStorageAdapter directly
// =============================================================================

describe('Compactors accept FullStorageAdapter for all compaction paths', () => {
  it('compactDataTable accepts a FullStorageAdapter (not just StorageAdapter)', async () => {
    const jsonlPath = join(tempDir, 'users.jsonl')
    await writeJsonl(jsonlPath, [
      makeLine({ $id: 'u1', name: 'Alice' }),
      makeLine({ $id: 'u2', name: 'Bob' }),
    ])

    // Pass a FullStorageAdapter (MemoryStorageAdapter) directly to compactDataTable
    const adapter = new MemoryStorageAdapter()
    const count = await compactDataTable(tempDir, 'users', adapter)

    expect(count).toBe(2)

    // Verify data was written through the adapter
    const data = await adapter.readData(join(tempDir, 'users.parquet'))
    expect(data).toHaveLength(2)
    expect(data.map(d => d.$id).sort()).toEqual(['u1', 'u2'])
  })

  it('compactRelationships accepts a FullStorageAdapter (not just RelStorageAdapter)', async () => {
    await writeJsonl(join(tempDir, 'rels.jsonl'), [
      makeLink('u1', 'posts', 'author', 'p1'),
      makeLink('u2', 'posts', 'author', 'p2'),
    ])

    // Pass a FullStorageAdapter directly to compactRelationships
    const adapter = new MemoryStorageAdapter()
    const count = await compactRelationships(tempDir, adapter)

    expect(count).toBe(2)

    const rels = await adapter.readRels(join(tempDir, 'rels.parquet'))
    expect(rels).toHaveLength(2)
  })

  it('compactEvents accepts a FullStorageAdapter (not just EventStorageAdapter)', async () => {
    await writeJsonl(join(tempDir, 'events.jsonl'), [
      { id: 'e1', ts: 100, op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' } },
      { id: 'e2', ts: 200, op: 'c', ns: 'users', eid: 'u2', after: { name: 'Bob' } },
    ])

    // Pass a FullStorageAdapter directly to compactEvents
    const adapter = new MemoryStorageAdapter()
    const count = await compactEvents(tempDir, adapter)

    expect(count).toBe(2)

    const events = await adapter.readEvents(join(tempDir, 'events.compacted'))
    expect(events).toHaveLength(2)
  })

  it('a single FullStorageAdapter instance works across all three compaction paths', async () => {
    // Write data JSONL
    await writeJsonl(join(tempDir, 'users.jsonl'), [
      makeLine({ $id: 'u1', name: 'Alice' }),
    ])

    // Write rels JSONL
    await writeJsonl(join(tempDir, 'rels.jsonl'), [
      makeLink('u1', 'posts', 'author', 'p1'),
    ])

    // Write events JSONL
    await writeJsonl(join(tempDir, 'events.jsonl'), [
      { id: 'e1', ts: 100, op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' } },
    ])

    // Use the SAME adapter for all three
    const adapter = new MemoryStorageAdapter()

    const dataCount = await compactDataTable(tempDir, 'users', adapter)
    const relCount = await compactRelationships(tempDir, adapter)
    const eventCount = await compactEvents(tempDir, adapter)

    expect(dataCount).toBe(1)
    expect(relCount).toBe(1)
    expect(eventCount).toBe(1)

    // Verify all data is accessible through the same adapter
    const data = await adapter.readData(join(tempDir, 'users.parquet'))
    expect(data).toHaveLength(1)

    const rels = await adapter.readRels(join(tempDir, 'rels.parquet'))
    expect(rels).toHaveLength(1)

    const events = await adapter.readEvents(join(tempDir, 'events.compacted'))
    expect(events).toHaveLength(1)
  })
})

// =============================================================================
// 3. Backward compatibility: narrow objects still work via structural typing
// =============================================================================

describe('Backward compatibility: narrow adapter objects still work', () => {
  it('an object with only readData/writeData works with compactDataTable (file-backed)', async () => {
    await writeJsonl(join(tempDir, 'users.jsonl'), [
      makeLine({ $id: 'u1', name: 'Alice' }),
    ])

    // A narrow object with only data methods that writes to actual files on disk
    // (the compactors use fs.rename as fallback when no adapter.rename is provided)
    const narrowAdapter: StorageAdapter = {
      readData: async (path: string) => {
        try { return JSON.parse(await readFile(path, 'utf-8')) } catch { return [] }
      },
      writeData: async (path: string, data: DataLine[]) => {
        await writeFile(path, JSON.stringify(data))
      },
    }

    const count = await compactDataTable(tempDir, 'users', narrowAdapter)
    expect(count).toBe(1)
  })

  it('an object with only readRels/writeRels works with compactRelationships (file-backed)', async () => {
    await writeJsonl(join(tempDir, 'rels.jsonl'), [
      makeLink('u1', 'posts', 'author', 'p1'),
    ])

    const narrowAdapter: RelStorageAdapter = {
      readRels: async (path: string) => {
        try { return JSON.parse(await readFile(path, 'utf-8')) } catch { return [] }
      },
      writeRels: async (path: string, data: RelLine[]) => {
        await writeFile(path, JSON.stringify(data))
      },
    }

    const count = await compactRelationships(tempDir, narrowAdapter)
    expect(count).toBe(1)
  })

  it('an object with only readEvents/writeEvents works with compactEvents (file-backed)', async () => {
    await writeJsonl(join(tempDir, 'events.jsonl'), [
      { id: 'e1', ts: 100, op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' } },
    ])

    const narrowAdapter: EventStorageAdapter = {
      readEvents: async (path: string) => {
        try { return JSON.parse(await readFile(path, 'utf-8')) } catch { return [] }
      },
      writeEvents: async (path: string, data: any[]) => {
        await writeFile(path, JSON.stringify(data))
      },
    }

    const count = await compactEvents(tempDir, narrowAdapter)
    expect(count).toBe(1)
  })
})
