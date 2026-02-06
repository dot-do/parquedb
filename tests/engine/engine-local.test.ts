/**
 * ParqueEngine Local Storage Mode Test Suite
 *
 * Tests the full engine lifecycle: startup (replay JSONL), compact, shutdown.
 * Validates that data persists across engine restarts via JSONL replay,
 * compaction merges JSONL into data files, and recovery handles interrupted
 * compactions.
 *
 * Each test uses a fresh temp directory and new ParqueEngine instances.
 */

import { mkdtemp, rm, writeFile, readFile, access } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParqueEngine } from '@/engine/engine'
import type { DataLine } from '@/engine/types'

let dataDir: string

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'parquedb-local-test-'))
})

afterEach(async () => {
  await rm(dataDir, { recursive: true, force: true })
})

// =============================================================================
// Helper: check if a file exists
// =============================================================================

async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path)
    return true
  } catch {
    return false
  }
}

// =============================================================================
// Startup Replay
// =============================================================================

describe('ParqueEngine - startup replay', () => {
  it('1. replays JSONL on init: write 3 entities, close, reopen -> find returns 3', async () => {
    // First engine: write 3 entities
    const engine1 = new ParqueEngine({ dataDir })
    await engine1.create('users', { name: 'Alice', age: 30 })
    await engine1.create('users', { name: 'Bob', age: 25 })
    await engine1.create('users', { name: 'Charlie', age: 35 })
    await engine1.close()

    // Second engine: replay from JSONL
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const results = await engine2.find('users')
    expect(results).toHaveLength(3)
    expect(results.map(r => r.name).sort()).toEqual(['Alice', 'Bob', 'Charlie'])

    await engine2.close()
  })

  it('2. replayed buffer contains correct entity data', async () => {
    const engine1 = new ParqueEngine({ dataDir })
    const created = await engine1.create('users', { name: 'Alice', age: 30, email: 'alice@test.com' })
    await engine1.close()

    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const entity = await engine2.get('users', created.$id)
    expect(entity).not.toBeNull()
    expect(entity!.name).toBe('Alice')
    expect(entity!.age).toBe(30)
    expect(entity!.email).toBe('alice@test.com')
    expect(entity!.$id).toBe(created.$id)

    await engine2.close()
  })

  it('3. replay handles updates: write, update, close, reopen -> entity has updated values', async () => {
    const engine1 = new ParqueEngine({ dataDir })
    const created = await engine1.create('users', { name: 'Alice', age: 30 })
    await engine1.update('users', created.$id, { $set: { name: 'Alice Smith', age: 31 } })
    await engine1.close()

    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const entity = await engine2.get('users', created.$id)
    expect(entity).not.toBeNull()
    expect(entity!.name).toBe('Alice Smith')
    expect(entity!.age).toBe(31)
    expect(entity!.$v).toBe(2)

    await engine2.close()
  })

  it('4. replay handles deletes: write, delete, close, reopen -> entity not found', async () => {
    const engine1 = new ParqueEngine({ dataDir })
    const created = await engine1.create('users', { name: 'Alice' })
    await engine1.delete('users', created.$id)
    await engine1.close()

    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const entity = await engine2.get('users', created.$id)
    expect(entity).toBeNull()

    const results = await engine2.find('users')
    expect(results).toHaveLength(0)

    await engine2.close()
  })

  it('5. startup with empty dataDir works (no JSONL files)', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.init()

    const results = await engine.find('users')
    expect(results).toHaveLength(0)

    await engine.close()
  })

  it('replays multiple tables independently', async () => {
    const engine1 = new ParqueEngine({ dataDir })
    await engine1.create('users', { name: 'Alice' })
    await engine1.create('posts', { title: 'Hello World' })
    await engine1.create('posts', { title: 'Second Post' })
    await engine1.close()

    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const users = await engine2.find('users')
    const posts = await engine2.find('posts')
    expect(users).toHaveLength(1)
    expect(posts).toHaveLength(2)

    await engine2.close()
  })
})

// =============================================================================
// Compact
// =============================================================================

describe('ParqueEngine - compact', () => {
  it('6. compact("users") compacts users.jsonl into a data file, returns entity count', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice' })
    await engine.create('users', { name: 'Bob' })
    await engine.create('users', { name: 'Charlie' })

    const count = await engine.compact('users')
    expect(count).toBe(3)

    // Data file should exist
    const dataPath = join(dataDir, 'users.parquet')
    expect(await fileExists(dataPath)).toBe(true)

    await engine.close()
  })

  it('7. after compact, users.jsonl is fresh (empty or minimal)', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice' })
    await engine.create('users', { name: 'Bob' })

    await engine.compact('users')

    // The JSONL file should exist but be empty (rotation creates a fresh one)
    const jsonlPath = join(dataDir, 'users.jsonl')
    const content = await readFile(jsonlPath, 'utf-8')
    expect(content.trim()).toBe('')

    await engine.close()
  })

  it('8. compact + close + reopen: find() still returns correct data', async () => {
    const engine1 = new ParqueEngine({ dataDir })
    await engine1.create('users', { name: 'Alice', age: 30 })
    await engine1.create('users', { name: 'Bob', age: 25 })
    await engine1.compact('users')
    await engine1.close()

    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const results = await engine2.find('users')
    expect(results).toHaveLength(2)
    expect(results.map(r => r.name).sort()).toEqual(['Alice', 'Bob'])

    await engine2.close()
  })

  it('9. compactAll() compacts all tables + rels + events', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice' })
    await engine.create('posts', { title: 'Hello' })
    await engine.create('comments', { body: 'Nice' })

    await engine.compactAll()

    // All data files should exist
    expect(await fileExists(join(dataDir, 'users.parquet'))).toBe(true)
    expect(await fileExists(join(dataDir, 'posts.parquet'))).toBe(true)
    expect(await fileExists(join(dataDir, 'comments.parquet'))).toBe(true)

    await engine.close()
  })

  it('compact returns null when table has no JSONL', async () => {
    const engine = new ParqueEngine({ dataDir })
    const count = await engine.compact('nonexistent')
    expect(count).toBeNull()

    await engine.close()
  })
})

// =============================================================================
// Compact with Storage Adapter
// =============================================================================

describe('ParqueEngine - compact with storage adapter', () => {
  it('10. compact uses Parquet adapter for local mode', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice' })
    await engine.create('users', { name: 'Bob' })

    await engine.compact('users')

    // Should be a real Parquet file with PAR1 magic bytes
    const dataPath = join(dataDir, 'users.parquet')
    const fileData = await readFile(dataPath)
    expect(fileData[0]).toBe(0x50) // P
    expect(fileData[1]).toBe(0x41) // A
    expect(fileData[2]).toBe(0x52) // R
    expect(fileData[3]).toBe(0x31) // 1

    // Verify data can be read back via the engine
    const results = await engine.find('users')
    expect(results).toHaveLength(2)
    expect(results.every(e => e.$id && e.name)).toBe(true)

    await engine.close()
  })
})

// =============================================================================
// Write -> Compact -> Write -> Read
// =============================================================================

describe('ParqueEngine - write/compact/write/read lifecycle', () => {
  it('11. create 3 -> compact -> create 2 more -> find returns all 5', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice' })
    await engine.create('users', { name: 'Bob' })
    await engine.create('users', { name: 'Charlie' })

    await engine.compact('users')

    await engine.create('users', { name: 'Diana' })
    await engine.create('users', { name: 'Eve' })

    const results = await engine.find('users')
    expect(results).toHaveLength(5)
    expect(results.map(r => r.name).sort()).toEqual(['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'])

    await engine.close()
  })

  it('12. create -> compact -> update -> find returns updated version', async () => {
    const engine = new ParqueEngine({ dataDir })
    const created = await engine.create('users', { name: 'Alice', age: 30 })

    await engine.compact('users')

    await engine.update('users', created.$id, { $set: { name: 'Alice Smith', age: 31 } })

    const entity = await engine.get('users', created.$id)
    expect(entity).not.toBeNull()
    expect(entity!.name).toBe('Alice Smith')
    expect(entity!.age).toBe(31)

    await engine.close()
  })

  it('13. create -> compact -> delete -> find returns empty', async () => {
    const engine = new ParqueEngine({ dataDir })
    const created = await engine.create('users', { name: 'Alice' })

    await engine.compact('users')

    await engine.delete('users', created.$id)

    const results = await engine.find('users')
    expect(results).toHaveLength(0)

    await engine.close()
  })

  it('write -> compact -> write -> compact -> reopen: all data intact', async () => {
    const engine1 = new ParqueEngine({ dataDir })
    await engine1.create('users', { name: 'Alice' })
    await engine1.create('users', { name: 'Bob' })
    await engine1.compact('users')

    await engine1.create('users', { name: 'Charlie' })
    await engine1.compact('users')

    await engine1.close()

    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const results = await engine2.find('users')
    expect(results).toHaveLength(3)
    expect(results.map(r => r.name).sort()).toEqual(['Alice', 'Bob', 'Charlie'])

    await engine2.close()
  })

  it('compact -> write -> close -> reopen: data from both compacted file and JSONL', async () => {
    const engine1 = new ParqueEngine({ dataDir })
    await engine1.create('users', { name: 'Alice' })
    await engine1.compact('users')

    // Write after compact (goes to fresh JSONL)
    await engine1.create('users', { name: 'Bob' })
    await engine1.close()

    // Reopen: should merge compacted data + JSONL replay
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const results = await engine2.find('users')
    expect(results).toHaveLength(2)
    expect(results.map(r => r.name).sort()).toEqual(['Alice', 'Bob'])

    await engine2.close()
  })
})

// =============================================================================
// Recovery
// =============================================================================

describe('ParqueEngine - recovery', () => {
  it('14. .compacting file on startup triggers recovery', async () => {
    // Simulate an interrupted compaction: write data to .compacting file
    const jsonlPath = join(dataDir, 'users.jsonl')
    const compactingPath = jsonlPath + '.compacting'

    // Write some data to the compacting file (simulating interrupted compaction)
    const lines = [
      JSON.stringify({ $id: 'u1', $op: 'c', $v: 1, $ts: Date.now(), name: 'Alice' }),
      JSON.stringify({ $id: 'u2', $op: 'c', $v: 1, $ts: Date.now(), name: 'Bob' }),
    ].join('\n') + '\n'
    await writeFile(compactingPath, lines, 'utf-8')

    // Also write an empty JSONL file (as rotation would leave behind)
    await writeFile(jsonlPath, '', 'utf-8')

    const engine = new ParqueEngine({ dataDir })
    await engine.init()

    // After recovery, the .compacting file should be gone
    expect(await fileExists(compactingPath)).toBe(false)

    // The data should be available
    const results = await engine.find('users')
    expect(results).toHaveLength(2)

    await engine.close()
  })

  it('15. after recovery, data is consistent', async () => {
    // Simulate: existing data file + interrupted compaction with updates
    const dataPath = join(dataDir, 'users.parquet')
    const jsonlPath = join(dataDir, 'users.jsonl')
    const compactingPath = jsonlPath + '.compacting'

    // Write existing data file as real Parquet
    const { ParquetStorageAdapter } = await import('@/engine/parquet-adapter')
    const adapter = new ParquetStorageAdapter()
    const existingData: DataLine[] = [
      { $id: 'u1', $op: 'c', $v: 1, $ts: Date.now() - 1000, name: 'Alice' },
      { $id: 'u2', $op: 'c', $v: 1, $ts: Date.now() - 1000, name: 'Bob' },
    ]
    await adapter.writeData(dataPath, existingData)

    // Compacting file has an update and a new entity
    const compactingLines = [
      JSON.stringify({ $id: 'u1', $op: 'u', $v: 2, $ts: Date.now(), name: 'Alice Updated' }),
      JSON.stringify({ $id: 'u3', $op: 'c', $v: 1, $ts: Date.now(), name: 'Charlie' }),
    ].join('\n') + '\n'
    await writeFile(compactingPath, compactingLines, 'utf-8')

    // Empty JSONL (rotation leaves a fresh one)
    await writeFile(jsonlPath, '', 'utf-8')

    const engine = new ParqueEngine({ dataDir })
    await engine.init()

    // After recovery: u1 should be updated, u2 untouched, u3 new
    const results = await engine.find('users')
    expect(results).toHaveLength(3)

    const alice = await engine.get('users', 'u1')
    expect(alice).not.toBeNull()
    expect(alice!.name).toBe('Alice Updated')

    const bob = await engine.get('users', 'u2')
    expect(bob).not.toBeNull()
    expect(bob!.name).toBe('Bob')

    const charlie = await engine.get('users', 'u3')
    expect(charlie).not.toBeNull()
    expect(charlie!.name).toBe('Charlie')

    await engine.close()
  })
})

// =============================================================================
// Compaction Threshold
// =============================================================================

describe('ParqueEngine - compaction thresholds', () => {
  it('16. compact with maxBytes only compacts if threshold exceeded', async () => {
    const engine = new ParqueEngine({ dataDir })
    // Write a small amount of data
    await engine.create('users', { name: 'Alice' })

    // High threshold: should NOT compact
    const count = await engine.compact('users', { maxBytes: 1_000_000 })
    expect(count).toBeNull()

    // Verify JSONL is still intact (not rotated)
    const jsonlPath = join(dataDir, 'users.jsonl')
    const content = await readFile(jsonlPath, 'utf-8')
    expect(content.trim().length).toBeGreaterThan(0)

    await engine.close()
  })

  it('17. autoCompact triggers compaction after threshold writes', async () => {
    const engine = new ParqueEngine({
      dataDir,
      autoCompact: { maxLines: 5 },
    })

    // Write 6 entities (exceeds maxLines: 5)
    for (let i = 0; i < 6; i++) {
      await engine.create('users', { name: `User-${i}` })
    }

    // Give auto-compact a moment to complete (it may be async)
    await new Promise(resolve => setTimeout(resolve, 50))

    // Data file should exist from auto-compaction
    const dataPath = join(dataDir, 'users.parquet')
    expect(await fileExists(dataPath)).toBe(true)

    // All 6 entities should still be accessible
    const results = await engine.find('users')
    expect(results).toHaveLength(6)

    await engine.close()
  })

  it('compact with maxLines only compacts if line threshold exceeded', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice' })

    // High line threshold: should NOT compact
    const count = await engine.compact('users', { maxLines: 1000 })
    expect(count).toBeNull()

    await engine.close()
  })

  it('compact without options always compacts (no threshold check)', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice' })

    // No options: should always compact
    const count = await engine.compact('users')
    expect(count).toBe(1)

    await engine.close()
  })
})

// =============================================================================
// Init is idempotent
// =============================================================================

describe('ParqueEngine - init behavior', () => {
  it('init can be called on a fresh engine with no data', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.init()
    await engine.init() // double init should not error

    const results = await engine.find('users')
    expect(results).toHaveLength(0)

    await engine.close()
  })

  it('writes work correctly after init', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.init()

    const created = await engine.create('users', { name: 'Alice' })
    const found = await engine.get('users', created.$id)
    expect(found).not.toBeNull()
    expect(found!.name).toBe('Alice')

    await engine.close()
  })
})
