/**
 * ParquetStorageAdapter Test Suite
 *
 * Tests the Parquet-based storage adapter for the MergeTree engine.
 * Verifies that data, relationships, and events can be written to and
 * read from real Parquet files with correct column encoding and JSON
 * packing/unpacking for entity data fields.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParquetStorageAdapter } from '@/engine/parquet-adapter'
import { mergeResults } from '@/engine/merge'
import type { DataLine, RelLine } from '@/engine/types'
import { makeLine, makeLink, makeUnlink } from './helpers'

// =============================================================================
// Test Helpers
// =============================================================================

let tempDir: string
let adapter: ParquetStorageAdapter

beforeEach(async () => {
  tempDir = await mkdtemp(join(tmpdir(), 'parquet-adapter-test-'))
  adapter = new ParquetStorageAdapter()
})

afterEach(async () => {
  await rm(tempDir, { recursive: true, force: true })
})

// =============================================================================
// Data Tests
// =============================================================================

describe('ParquetStorageAdapter - Data', () => {
  it('1. writeData + readData roundtrip - simple entities', async () => {
    const path = join(tempDir, 'users.parquet')
    const data: DataLine[] = [
      makeLine({ $id: 'u1', name: 'Alice' }),
      makeLine({ $id: 'u2', name: 'Bob' }),
      makeLine({ $id: 'u3', name: 'Charlie' }),
    ]

    await adapter.writeData(path, data)
    const result = await adapter.readData(path)

    expect(result).toHaveLength(3)
    expect(result.map(r => r.$id).sort()).toEqual(['u1', 'u2', 'u3'])
    expect(result.find(r => r.$id === 'u1')!.name).toBe('Alice')
    expect(result.find(r => r.$id === 'u2')!.name).toBe('Bob')
    expect(result.find(r => r.$id === 'u3')!.name).toBe('Charlie')
  })

  it('2. writeData + readData roundtrip - nested entity data in fields', async () => {
    const path = join(tempDir, 'users.parquet')
    const data: DataLine[] = [
      makeLine({
        $id: 'u1',
        name: 'Alice',
        profile: { bio: 'Developer', location: { city: 'SF', state: 'CA' } },
        tags: ['admin', 'staff'],
        metadata: { score: 42, active: true },
      }),
    ]

    await adapter.writeData(path, data)
    const result = await adapter.readData(path)

    expect(result).toHaveLength(1)
    const entity = result[0]
    expect(entity.name).toBe('Alice')
    expect(entity.profile).toEqual({ bio: 'Developer', location: { city: 'SF', state: 'CA' } })
    expect(entity.tags).toEqual(['admin', 'staff'])
    expect(entity.metadata).toEqual({ score: 42, active: true })
  })

  it('3. readData on missing file returns []', async () => {
    const result = await adapter.readData(join(tempDir, 'nonexistent.parquet'))
    expect(result).toEqual([])
  })

  it('4. writeData with empty array writes valid file that reads back empty', async () => {
    const path = join(tempDir, 'empty.parquet')

    await adapter.writeData(path, [])
    const result = await adapter.readData(path)

    expect(result).toEqual([])
  })

  it('5. System fields ($id, $op, $v, $ts) preserved exactly', async () => {
    const path = join(tempDir, 'system-fields.parquet')
    const ts = 1738857600000
    const data: DataLine[] = [
      { $id: 'entity-001', $op: 'c', $v: 1, $ts: ts, name: 'Test' },
      { $id: 'entity-002', $op: 'u', $v: 3, $ts: ts + 1000, name: 'Updated' },
      { $id: 'entity-003', $op: 'd', $v: 2, $ts: ts + 2000 },
    ]

    await adapter.writeData(path, data)
    const result = await adapter.readData(path)

    // Results are sorted by $id
    const e1 = result.find(r => r.$id === 'entity-001')!
    expect(e1.$op).toBe('c')
    expect(e1.$v).toBe(1)
    expect(e1.$ts).toBe(ts)

    const e2 = result.find(r => r.$id === 'entity-002')!
    expect(e2.$op).toBe('u')
    expect(e2.$v).toBe(3)
    expect(e2.$ts).toBe(ts + 1000)

    const e3 = result.find(r => r.$id === 'entity-003')!
    expect(e3.$op).toBe('d')
    expect(e3.$v).toBe(2)
    expect(e3.$ts).toBe(ts + 2000)
  })

  it('6. Entity data fields preserved through $data JSON column', async () => {
    const path = join(tempDir, 'data-fields.parquet')
    const data: DataLine[] = [
      makeLine({
        $id: 'u1',
        name: 'Alice',
        email: 'alice@example.com',
        age: 30,
        active: true,
        roles: ['admin', 'user'],
        settings: { theme: 'dark', lang: 'en' },
      }),
    ]

    await adapter.writeData(path, data)
    const result = await adapter.readData(path)

    expect(result).toHaveLength(1)
    const entity = result[0]
    expect(entity.name).toBe('Alice')
    expect(entity.email).toBe('alice@example.com')
    expect(entity.age).toBe(30)
    expect(entity.active).toBe(true)
    expect(entity.roles).toEqual(['admin', 'user'])
    expect(entity.settings).toEqual({ theme: 'dark', lang: 'en' })
  })

  it('7. Data sorted by $id in output', async () => {
    const path = join(tempDir, 'sorted.parquet')
    const data: DataLine[] = [
      makeLine({ $id: 'charlie' }),
      makeLine({ $id: 'alice' }),
      makeLine({ $id: 'bob' }),
      makeLine({ $id: 'diana' }),
    ]

    await adapter.writeData(path, data)
    const result = await adapter.readData(path)

    expect(result.map(r => r.$id)).toEqual(['alice', 'bob', 'charlie', 'diana'])
  })
})

// =============================================================================
// Rels Tests
// =============================================================================

describe('ParquetStorageAdapter - Rels', () => {
  it('8. writeRels + readRels roundtrip', async () => {
    const path = join(tempDir, 'rels.parquet')
    const rels: RelLine[] = [
      makeLink('u1', 'posts', 'author', 'p1'),
      makeLink('u1', 'posts', 'author', 'p2'),
      makeLink('u2', 'follows', 'followers', 'u1'),
    ]

    await adapter.writeRels(path, rels)
    const result = await adapter.readRels(path)

    expect(result).toHaveLength(3)
    expect(result[0]).toEqual(rels[0])
    expect(result[1]).toEqual(rels[1])
    expect(result[2]).toEqual(rels[2])
  })

  it('9. Rels roundtrip with both link and unlink ops', async () => {
    const path = join(tempDir, 'rels-mixed.parquet')
    const rels: RelLine[] = [
      makeLink('u1', 'posts', 'author', 'p1', 1000),
      makeUnlink('u1', 'posts', 'author', 'p2', 2000),
      makeLink('u2', 'follows', 'followers', 'u3', 3000),
      makeUnlink('u3', 'follows', 'followers', 'u2', 4000),
    ]

    await adapter.writeRels(path, rels)
    const result = await adapter.readRels(path)

    expect(result).toHaveLength(4)

    // Verify link ops
    const links = result.filter(r => r.$op === 'l')
    expect(links).toHaveLength(2)

    // Verify unlink ops
    const unlinks = result.filter(r => r.$op === 'u')
    expect(unlinks).toHaveLength(2)

    // Verify exact values
    expect(result[0].$op).toBe('l')
    expect(result[0].$ts).toBe(1000)
    expect(result[0].f).toBe('u1')
    expect(result[0].p).toBe('posts')
    expect(result[0].r).toBe('author')
    expect(result[0].t).toBe('p1')

    expect(result[1].$op).toBe('u')
    expect(result[1].$ts).toBe(2000)
  })

  it('10. readRels on missing file returns []', async () => {
    const result = await adapter.readRels(join(tempDir, 'nonexistent-rels.parquet'))
    expect(result).toEqual([])
  })
})

// =============================================================================
// Events Tests
// =============================================================================

describe('ParquetStorageAdapter - Events', () => {
  it('11. writeEvents + readEvents roundtrip', async () => {
    const path = join(tempDir, 'events.parquet')
    const events: Record<string, unknown>[] = [
      { id: 'e1', ts: 100, op: 'c', ns: 'users', eid: 'u1', after: { name: 'Alice' }, actor: 'system' },
      { id: 'e2', ts: 200, op: 'c', ns: 'users', eid: 'u2', after: { name: 'Bob' }, actor: 'admin' },
      { id: 'e3', ts: 300, op: 'u', ns: 'users', eid: 'u1', before: { name: 'Alice' }, after: { name: 'Alice Updated' }, actor: 'system' },
    ]

    await adapter.writeEvents(path, events)
    const result = await adapter.readEvents(path)

    expect(result).toHaveLength(3)
    expect(result[0].id).toBe('e1')
    expect(result[0].ts).toBe(100)
    expect(result[0].op).toBe('c')
    expect(result[0].ns).toBe('users')
    expect(result[0].eid).toBe('u1')
    expect(result[0].actor).toBe('system')
  })

  it('12. Events with before/after preserved through JSON', async () => {
    const path = join(tempDir, 'events-json.parquet')
    const events: Record<string, unknown>[] = [
      {
        id: 'e1',
        ts: 100,
        op: 'c',
        ns: 'users',
        eid: 'u1',
        after: { name: 'Alice', email: 'alice@a.co', tags: ['admin'] },
      },
      {
        id: 'e2',
        ts: 200,
        op: 'u',
        ns: 'users',
        eid: 'u1',
        before: { name: 'Alice', email: 'alice@a.co', tags: ['admin'] },
        after: { name: 'Alice Smith', email: 'alice@a.co', tags: ['admin', 'staff'] },
        actor: 'user:u1',
      },
      {
        id: 'e3',
        ts: 300,
        op: 'd',
        ns: 'users',
        eid: 'u1',
        before: { name: 'Alice Smith', email: 'alice@a.co', tags: ['admin', 'staff'] },
      },
    ]

    await adapter.writeEvents(path, events)
    const result = await adapter.readEvents(path)

    expect(result).toHaveLength(3)

    // Event 1: create (after only)
    expect(result[0].after).toEqual({ name: 'Alice', email: 'alice@a.co', tags: ['admin'] })
    expect(result[0].before).toBeUndefined()

    // Event 2: update (both before and after)
    expect(result[1].before).toEqual({ name: 'Alice', email: 'alice@a.co', tags: ['admin'] })
    expect(result[1].after).toEqual({ name: 'Alice Smith', email: 'alice@a.co', tags: ['admin', 'staff'] })
    expect(result[1].actor).toBe('user:u1')

    // Event 3: delete (before only)
    expect(result[2].before).toEqual({ name: 'Alice Smith', email: 'alice@a.co', tags: ['admin', 'staff'] })
    expect(result[2].after).toBeUndefined()
  })

  it('13. readEvents on missing file returns []', async () => {
    const result = await adapter.readEvents(join(tempDir, 'nonexistent-events.parquet'))
    expect(result).toEqual([])
  })
})

// =============================================================================
// Integration with compactDataTable pipeline
// =============================================================================

describe('ParquetStorageAdapter - Integration with compaction pipeline', () => {
  it('14. Integration with compactDataTable pipeline (mergeResults)', async () => {
    const dataPath = join(tempDir, 'users.parquet')

    // Phase 1: Write initial data (simulating first compaction)
    const initial: DataLine[] = [
      makeLine({ $id: 'u1', $v: 1, $op: 'c', $ts: 1000, name: 'Alice' }),
      makeLine({ $id: 'u2', $v: 1, $op: 'c', $ts: 1000, name: 'Bob' }),
      makeLine({ $id: 'u3', $v: 1, $op: 'c', $ts: 1000, name: 'Charlie' }),
    ]
    await adapter.writeData(dataPath, initial)

    // Phase 2: Read back existing data
    const existing = await adapter.readData(dataPath)
    expect(existing).toHaveLength(3)

    // Phase 3: Simulate JSONL buffer with updates and new entities
    const bufferData: DataLine[] = [
      makeLine({ $id: 'u1', $v: 2, $op: 'u', $ts: 2000, name: 'Alice Updated' }),
      makeLine({ $id: 'u3', $v: 2, $op: 'd', $ts: 2000 }),
      makeLine({ $id: 'u4', $v: 1, $op: 'c', $ts: 2000, name: 'Diana' }),
    ]

    // Phase 4: Merge using ReplacingMergeTree semantics
    const merged = mergeResults(existing, bufferData)

    // Phase 5: Write merged result back to Parquet
    await adapter.writeData(dataPath, merged)

    // Phase 6: Read final result
    const final = await adapter.readData(dataPath)

    // Expectations:
    // u1 was updated (v2 overrides v1)
    // u2 is unchanged
    // u3 was deleted (tombstone removes it)
    // u4 is new
    expect(final).toHaveLength(3)
    expect(final.map(e => e.$id)).toEqual(['u1', 'u2', 'u4'])

    // Verify u1 was updated
    const alice = final.find(e => e.$id === 'u1')!
    expect(alice.name).toBe('Alice Updated')
    expect(alice.$v).toBe(2)
    expect(alice.$op).toBe('u')

    // Verify u2 is unchanged
    const bob = final.find(e => e.$id === 'u2')!
    expect(bob.name).toBe('Bob')
    expect(bob.$v).toBe(1)

    // Verify u4 is new
    const diana = final.find(e => e.$id === 'u4')!
    expect(diana.name).toBe('Diana')
    expect(diana.$v).toBe(1)
  })
})

// =============================================================================
// Error Handling Tests
// =============================================================================

import { writeFile } from 'node:fs/promises'

describe('ParquetStorageAdapter - Error handling', () => {
  it('readData returns [] for missing file (ENOENT)', async () => {
    const result = await adapter.readData(join(tempDir, 'does-not-exist.parquet'))
    expect(result).toEqual([])
  })

  it('readData throws for corrupt parquet file', async () => {
    const corruptPath = join(tempDir, 'corrupt.parquet')
    await writeFile(corruptPath, 'this is not a valid parquet file')
    await expect(adapter.readData(corruptPath)).rejects.toThrow()
  })

  it('readRels returns [] for missing file (ENOENT)', async () => {
    const result = await adapter.readRels(join(tempDir, 'does-not-exist-rels.parquet'))
    expect(result).toEqual([])
  })

  it('readRels throws for corrupt parquet file', async () => {
    const corruptPath = join(tempDir, 'corrupt-rels.parquet')
    await writeFile(corruptPath, 'not a parquet file at all')
    await expect(adapter.readRels(corruptPath)).rejects.toThrow()
  })

  it('readEvents returns [] for missing file (ENOENT)', async () => {
    const result = await adapter.readEvents(join(tempDir, 'does-not-exist-events.parquet'))
    expect(result).toEqual([])
  })

  it('readEvents throws for corrupt parquet file', async () => {
    const corruptPath = join(tempDir, 'corrupt-events.parquet')
    await writeFile(corruptPath, 'garbage parquet data')
    await expect(adapter.readEvents(corruptPath)).rejects.toThrow()
  })
})
