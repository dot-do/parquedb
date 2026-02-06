/**
 * ParqueEngine Bulk Operations Test Suite
 *
 * Tests createMany, large-scale compaction, and concurrent write scenarios.
 * Each test uses a fresh temp directory and engine instance.
 */

import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParqueEngine } from '@/engine/engine'
import { mergeResults } from '@/engine/merge'
import type { StorageAdapter } from '@/engine/compactor'
import { replay } from '@/engine/jsonl-reader'
import type { DataLine, EventLine } from '@/engine/types'

// =============================================================================
// Helpers
// =============================================================================

let engine: ParqueEngine
let dataDir: string

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'parquedb-bulk-'))
  engine = new ParqueEngine({ dataDir })
})

afterEach(async () => {
  await engine.close()
  await rm(dataDir, { recursive: true, force: true })
})

/** Parse JSONL file into array of objects */
async function readJsonl<T>(filePath: string): Promise<T[]> {
  const content = await readFile(filePath, 'utf-8')
  return content
    .split('\n')
    .filter(line => line.trim() !== '')
    .map(line => JSON.parse(line) as T)
}

/** Read a JSON file, returning [] on ENOENT */
async function readJsonFile<T>(path: string): Promise<T[]> {
  try {
    const content = await readFile(path, 'utf-8')
    return JSON.parse(content)
  } catch {
    return []
  }
}

/** Write typed array as JSON file */
async function writeJsonFile<T>(path: string, data: T[]): Promise<void> {
  await writeFile(path, JSON.stringify(data))
}

/** JSON-based StorageAdapter for compaction tests */
function createDataStorage(): StorageAdapter {
  return {
    readData: (path: string) => readJsonFile<DataLine>(path),
    writeData: (path: string, data: DataLine[]) => writeJsonFile(path, data),
  }
}

/** Create a DataLine with sensible defaults */
function makeLine(overrides: Partial<DataLine> & { $id: string }): DataLine {
  return {
    $op: 'c',
    $v: 1,
    $ts: Date.now(),
    ...overrides,
  }
}

// =============================================================================
// createMany
// =============================================================================

describe('Bulk: createMany', () => {
  it('1. createMany with 100 entities -> find returns all 100', async () => {
    const items = Array.from({ length: 100 }, (_, i) => ({
      name: `User-${i}`,
      index: i,
    }))

    const results = await engine.createMany('users', items)

    expect(results).toHaveLength(100)

    const found = await engine.find('users')
    expect(found).toHaveLength(100)

    // Verify each entity is present
    const names = found.map(r => r.name).sort()
    for (let i = 0; i < 100; i++) {
      expect(names).toContain(`User-${i}`)
    }
  })

  it('2. createMany with 1000 entities -> find returns all 1000', async () => {
    const items = Array.from({ length: 1000 }, (_, i) => ({
      name: `User-${String(i).padStart(4, '0')}`,
      index: i,
    }))

    const results = await engine.createMany('users', items)
    expect(results).toHaveLength(1000)

    const found = await engine.find('users')
    expect(found).toHaveLength(1000)
  })

  it('3. createMany -> compact -> find returns all (verifying compaction works at scale)', async () => {
    const items = Array.from({ length: 100 }, (_, i) => ({
      name: `User-${i}`,
      index: i,
    }))

    await engine.createMany('users', items)
    await engine.close()

    // Simulate compaction: rotate JSONL, merge, write
    const storage = createDataStorage()
    const jsonlPath = join(dataDir, 'users.jsonl')
    const compactingPath = jsonlPath + '.compacting'
    const dataPath = join(dataDir, 'users.parquet')

    const { rename: fsRename } = await import('node:fs/promises')
    await fsRename(jsonlPath, compactingPath)
    await writeFile(jsonlPath, '', 'utf-8')

    const jsonlData = await replay<DataLine>(compactingPath)
    const existing = await storage.readData(dataPath)
    const merged = mergeResults(existing, jsonlData)

    expect(merged).toHaveLength(100)

    // Write compacted data
    await storage.writeData(dataPath, merged)

    // Verify compacted data
    const compacted = await readJsonFile<DataLine>(dataPath)
    expect(compacted).toHaveLength(100)
  })
})

// =============================================================================
// createMany JSONL integrity
// =============================================================================

describe('Bulk: createMany JSONL integrity', () => {
  it('all data lines are written to JSONL', async () => {
    const items = Array.from({ length: 50 }, (_, i) => ({
      name: `User-${i}`,
    }))

    await engine.createMany('users', items)

    const dataLines = await readJsonl<DataLine>(join(dataDir, 'users.jsonl'))
    expect(dataLines).toHaveLength(50)
  })

  it('all event lines are written to events.jsonl', async () => {
    const items = Array.from({ length: 50 }, (_, i) => ({
      name: `User-${i}`,
    }))

    await engine.createMany('users', items)

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))
    expect(events).toHaveLength(50)
    for (const event of events) {
      expect(event.op).toBe('c')
      expect(event.ns).toBe('users')
    }
  })

  it('each entity gets a unique ID', async () => {
    const items = Array.from({ length: 200 }, (_, i) => ({
      name: `User-${i}`,
    }))

    const results = await engine.createMany('users', items)

    const ids = results.map(r => r.$id)
    expect(new Set(ids).size).toBe(200)
  })

  it('all entities have $v=1 and $op="c"', async () => {
    const items = Array.from({ length: 100 }, (_, i) => ({
      name: `User-${i}`,
    }))

    const results = await engine.createMany('users', items)

    for (const result of results) {
      expect(result.$v).toBe(1)
      expect(result.$op).toBe('c')
    }
  })
})

// =============================================================================
// Large Compaction
// =============================================================================

describe('Bulk: Large Compaction', () => {
  it('4. create 1000 entities -> compact -> compacted file has 1000 entries', async () => {
    // Create compacted data directly (simulating previous compaction)
    const compactedData: DataLine[] = Array.from({ length: 1000 }, (_, i) =>
      makeLine({
        $id: `u-${String(i).padStart(4, '0')}`,
        $v: 1,
        name: `User-${i}`,
      })
    )

    // Simulate: put 1000 in JSONL, merge with empty
    const merged = mergeResults([], compactedData)

    expect(merged).toHaveLength(1000)
    // Output should be sorted by $id
    for (let i = 1; i < merged.length; i++) {
      expect(merged[i].$id >= merged[i - 1].$id).toBe(true)
    }
  })

  it('5. 1000 compacted + 100 new in JSONL -> merge returns 1100', () => {
    const compacted: DataLine[] = Array.from({ length: 1000 }, (_, i) =>
      makeLine({
        $id: `u-${String(i).padStart(4, '0')}`,
        $v: 1,
        name: `User-${i}`,
      })
    )

    const buffer: DataLine[] = Array.from({ length: 100 }, (_, i) =>
      makeLine({
        $id: `n-${String(i).padStart(4, '0')}`,
        $v: 1,
        name: `New-${i}`,
      })
    )

    const merged = mergeResults(compacted, buffer)
    expect(merged).toHaveLength(1100)
  })

  it('6. 1000 compacted + 50 updates + 50 deletes -> merge returns 950 correct entities', () => {
    const compacted: DataLine[] = Array.from({ length: 1000 }, (_, i) =>
      makeLine({
        $id: `u-${String(i).padStart(4, '0')}`,
        $v: 1,
        name: `User-${i}`,
      })
    )

    // 50 updates (IDs 0-49)
    const updates: DataLine[] = Array.from({ length: 50 }, (_, i) =>
      makeLine({
        $id: `u-${String(i).padStart(4, '0')}`,
        $v: 2,
        $op: 'u',
        name: `Updated-${i}`,
      })
    )

    // 50 deletes (IDs 50-99)
    const deletes: DataLine[] = Array.from({ length: 50 }, (_, i) =>
      makeLine({
        $id: `u-${String(i + 50).padStart(4, '0')}`,
        $v: 2,
        $op: 'd',
      })
    )

    const buffer = [...updates, ...deletes]
    const merged = mergeResults(compacted, buffer)

    expect(merged).toHaveLength(950)

    // Verify updated entities have new data
    const updated = merged.filter(e => e.name && (e.name as string).startsWith('Updated-'))
    expect(updated).toHaveLength(50)

    // Verify deleted entities are not present
    for (let i = 50; i < 100; i++) {
      const id = `u-${String(i).padStart(4, '0')}`
      expect(merged.find(e => e.$id === id)).toBeUndefined()
    }

    // Verify untouched entities are still present
    const untouched = merged.filter(e => e.name && (e.name as string).startsWith('User-'))
    expect(untouched).toHaveLength(900) // 1000 - 50 updated - 50 deleted
  })
})

// =============================================================================
// Concurrent Writes
// =============================================================================

describe('Bulk: Concurrent Writes', () => {
  it('7. 100 parallel creates (Promise.all) -> all succeed, find returns all 100', async () => {
    const promises = Array.from({ length: 100 }, (_, i) =>
      engine.create('users', { name: `User-${i}`, index: i })
    )

    const results = await Promise.all(promises)
    expect(results).toHaveLength(100)

    // All should have unique IDs
    const ids = results.map(r => r.$id)
    expect(new Set(ids).size).toBe(100)

    // Find should return all 100
    const found = await engine.find('users')
    expect(found).toHaveLength(100)
  })

  it('8. parallel creates + parallel updates -> final state is consistent', async () => {
    // Phase 1: Create 50 entities
    const createPromises = Array.from({ length: 50 }, (_, i) =>
      engine.create('users', { $id: `u${i}`, name: `User-${i}`, score: 0 })
    )
    await Promise.all(createPromises)

    // Phase 2: Update all 50 in parallel
    const updatePromises = Array.from({ length: 50 }, (_, i) =>
      engine.update('users', `u${i}`, { $set: { score: 100 } })
    )
    await Promise.all(updatePromises)

    // Verify: all 50 should be updated
    const found = await engine.find('users')
    expect(found).toHaveLength(50)
    for (const entity of found) {
      expect(entity.score).toBe(100)
      expect(entity.$v).toBe(2)
    }
  })
})

// =============================================================================
// createMany + Updates + Deletes
// =============================================================================

describe('Bulk: createMany + Updates + Deletes', () => {
  it('createMany then delete some -> count is correct', async () => {
    const items = Array.from({ length: 20 }, (_, i) => ({
      $id: `u${i}`,
      name: `User-${i}`,
    }))

    await engine.createMany('users', items)
    expect(await engine.count('users')).toBe(20)

    // Delete 5 entities
    for (let i = 0; i < 5; i++) {
      await engine.delete('users', `u${i}`)
    }

    expect(await engine.count('users')).toBe(15)

    // Verify the deleted ones are gone
    for (let i = 0; i < 5; i++) {
      const result = await engine.get('users', `u${i}`)
      expect(result).toBeNull()
    }

    // Verify the remaining ones are present
    for (let i = 5; i < 20; i++) {
      const result = await engine.get('users', `u${i}`)
      expect(result).not.toBeNull()
      expect(result!.name).toBe(`User-${i}`)
    }
  })

  it('createMany then update all -> all have new data', async () => {
    const items = Array.from({ length: 30 }, (_, i) => ({
      $id: `u${i}`,
      name: `User-${i}`,
      score: 0,
    }))

    await engine.createMany('users', items)

    for (let i = 0; i < 30; i++) {
      await engine.update('users', `u${i}`, { $inc: { score: i * 10 } })
    }

    for (let i = 0; i < 30; i++) {
      const entity = await engine.get('users', `u${i}`)
      expect(entity).not.toBeNull()
      expect(entity!.score).toBe(i * 10)
      expect(entity!.$v).toBe(2)
    }
  })
})

// =============================================================================
// Merge at Scale
// =============================================================================

describe('Bulk: Merge at Scale', () => {
  it('large merge preserves all non-deleted entities', () => {
    // 5000 entities in parquet
    const parquet: DataLine[] = Array.from({ length: 5000 }, (_, i) =>
      makeLine({
        $id: `u-${String(i).padStart(5, '0')}`,
        $v: 1,
        name: `User-${i}`,
      })
    )

    // 200 updates + 100 deletes + 300 new inserts in buffer
    const updates: DataLine[] = Array.from({ length: 200 }, (_, i) =>
      makeLine({
        $id: `u-${String(i).padStart(5, '0')}`,
        $v: 2,
        $op: 'u',
        name: `Updated-${i}`,
      })
    )

    const deletes: DataLine[] = Array.from({ length: 100 }, (_, i) =>
      makeLine({
        $id: `u-${String(i + 200).padStart(5, '0')}`,
        $v: 2,
        $op: 'd',
      })
    )

    const inserts: DataLine[] = Array.from({ length: 300 }, (_, i) =>
      makeLine({
        $id: `n-${String(i).padStart(5, '0')}`,
        $v: 1,
        name: `New-${i}`,
      })
    )

    const buffer = [...updates, ...deletes, ...inserts]
    const merged = mergeResults(parquet, buffer)

    // 5000 - 100 deletes + 300 inserts = 5200
    expect(merged).toHaveLength(5200)

    // Check updates took effect
    for (let i = 0; i < 200; i++) {
      const id = `u-${String(i).padStart(5, '0')}`
      const entity = merged.find(e => e.$id === id)
      expect(entity).toBeDefined()
      expect(entity!.name).toBe(`Updated-${i}`)
      expect(entity!.$v).toBe(2)
    }

    // Check deletes are excluded
    for (let i = 200; i < 300; i++) {
      const id = `u-${String(i).padStart(5, '0')}`
      const entity = merged.find(e => e.$id === id)
      expect(entity).toBeUndefined()
    }

    // Check inserts are present
    for (let i = 0; i < 300; i++) {
      const id = `n-${String(i).padStart(5, '0')}`
      const entity = merged.find(e => e.$id === id)
      expect(entity).toBeDefined()
      expect(entity!.name).toBe(`New-${i}`)
    }
  })

  it('merged output is always sorted by $id', () => {
    const parquet: DataLine[] = Array.from({ length: 100 }, (_, i) =>
      makeLine({
        $id: `z-${String(i).padStart(4, '0')}`,
        $v: 1,
        name: `Z-${i}`,
      })
    )

    const buffer: DataLine[] = Array.from({ length: 100 }, (_, i) =>
      makeLine({
        $id: `a-${String(i).padStart(4, '0')}`,
        $v: 1,
        name: `A-${i}`,
      })
    )

    const merged = mergeResults(parquet, buffer)
    expect(merged).toHaveLength(200)

    // Verify sorted
    for (let i = 1; i < merged.length; i++) {
      expect(merged[i].$id >= merged[i - 1].$id).toBe(true)
    }

    // All 'a-' IDs should come before 'z-' IDs
    expect(merged[0].$id.startsWith('a-')).toBe(true)
    expect(merged[merged.length - 1].$id.startsWith('z-')).toBe(true)
  })
})

// =============================================================================
// Edge Cases
// =============================================================================

describe('Bulk: Edge Cases', () => {
  it('createMany with empty array returns empty array', async () => {
    const results = await engine.createMany('users', [])
    expect(results).toEqual([])
  })

  it('createMany with single item works like create', async () => {
    const results = await engine.createMany('users', [{ name: 'Alice' }])

    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Alice')
    expect(results[0].$op).toBe('c')
    expect(results[0].$v).toBe(1)

    const found = await engine.find('users')
    expect(found).toHaveLength(1)
  })

  it('createMany preserves provided $id values', async () => {
    const results = await engine.createMany('users', [
      { $id: 'custom-1', name: 'Alice' },
      { $id: 'custom-2', name: 'Bob' },
    ])

    expect(results[0].$id).toBe('custom-1')
    expect(results[1].$id).toBe('custom-2')
  })

  it('merging empty parquet with empty buffer returns empty', () => {
    const merged = mergeResults([], [])
    expect(merged).toEqual([])
  })

  it('merging empty parquet with buffer returns buffer (minus tombstones)', () => {
    const buffer: DataLine[] = [
      makeLine({ $id: 'u1', name: 'Alice' }),
      makeLine({ $id: 'u2', $op: 'd', $v: 2 }),
    ]

    const merged = mergeResults([], buffer)
    expect(merged).toHaveLength(1)
    expect(merged[0].$id).toBe('u1')
  })

  it('merging parquet with empty buffer returns parquet', () => {
    const parquet: DataLine[] = [
      makeLine({ $id: 'u1', name: 'Alice' }),
      makeLine({ $id: 'u2', name: 'Bob' }),
    ]

    const merged = mergeResults(parquet, [])
    expect(merged).toHaveLength(2)
  })
})
