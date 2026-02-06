/**
 * ParqueEngine + ParquetStorageAdapter Integration Tests
 *
 * Verifies that the engine produces real Parquet files (with PAR1 magic bytes)
 * when compacting, and that data round-trips correctly through the Parquet
 * format across engine restarts.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParqueEngine } from '@/engine/engine'

let dataDir: string

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'parquedb-parquet-integration-'))
})

afterEach(async () => {
  await rm(dataDir, { recursive: true, force: true })
})

// =============================================================================
// PAR1 Magic Bytes Verification
// =============================================================================

/** PAR1 magic bytes: 0x50 0x41 0x52 0x31 */
const PAR1_MAGIC = Buffer.from([0x50, 0x41, 0x52, 0x31])

/**
 * Check whether a file starts with the PAR1 magic bytes,
 * confirming it is a real Parquet file.
 */
async function hasParquetMagic(path: string): Promise<boolean> {
  const data = await readFile(path)
  if (data.byteLength < 4) return false
  return data[0] === 0x50 && data[1] === 0x41 && data[2] === 0x52 && data[3] === 0x31
}

// =============================================================================
// Tests
// =============================================================================

describe('ParqueEngine - Parquet format integration', () => {
  it('1. compact produces files with PAR1 magic bytes', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice', age: 30 })
    await engine.create('users', { name: 'Bob', age: 25 })
    await engine.create('users', { name: 'Charlie', age: 35 })

    await engine.compact('users')

    const dataPath = join(dataDir, 'users.parquet')
    expect(await hasParquetMagic(dataPath)).toBe(true)

    await engine.close()
  })

  it('2. compacted file also ends with PAR1 magic bytes (Parquet footer)', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice' })
    await engine.compact('users')

    const dataPath = join(dataDir, 'users.parquet')
    const data = await readFile(dataPath)

    // Parquet files end with PAR1 magic bytes as well
    const tail = data.slice(data.byteLength - 4)
    expect(Buffer.from(tail).equals(PAR1_MAGIC)).toBe(true)

    await engine.close()
  })

  it('3. compact + reopen: data round-trips correctly through Parquet', async () => {
    const engine1 = new ParqueEngine({ dataDir })
    await engine1.create('users', { name: 'Alice', age: 30, email: 'alice@test.com' })
    await engine1.create('users', { name: 'Bob', age: 25, tags: ['admin', 'user'] })
    await engine1.create('users', { name: 'Charlie', age: 35, settings: { theme: 'dark' } })

    await engine1.compact('users')
    await engine1.close()

    // Reopen engine and verify data
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const results = await engine2.find('users')
    expect(results).toHaveLength(3)
    expect(results.map(r => r.name).sort()).toEqual(['Alice', 'Bob', 'Charlie'])

    // Verify nested data preserved
    const alice = results.find(r => r.name === 'Alice')!
    expect(alice.age).toBe(30)
    expect(alice.email).toBe('alice@test.com')

    const bob = results.find(r => r.name === 'Bob')!
    expect(bob.tags).toEqual(['admin', 'user'])

    const charlie = results.find(r => r.name === 'Charlie')!
    expect(charlie.settings).toEqual({ theme: 'dark' })

    await engine2.close()
  })

  it('4. write -> compact -> write -> compact -> reopen: multiple compactions work', async () => {
    const engine1 = new ParqueEngine({ dataDir })

    // First batch
    await engine1.create('users', { name: 'Alice' })
    await engine1.create('users', { name: 'Bob' })
    await engine1.compact('users')

    // Verify first compaction produced Parquet
    expect(await hasParquetMagic(join(dataDir, 'users.parquet'))).toBe(true)

    // Second batch
    await engine1.create('users', { name: 'Charlie' })
    await engine1.compact('users')

    // Verify second compaction still produces Parquet
    expect(await hasParquetMagic(join(dataDir, 'users.parquet'))).toBe(true)

    await engine1.close()

    // Reopen and verify all data
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const results = await engine2.find('users')
    expect(results).toHaveLength(3)
    expect(results.map(r => r.name).sort()).toEqual(['Alice', 'Bob', 'Charlie'])

    await engine2.close()
  })

  it('5. update then compact preserves updated values in Parquet', async () => {
    const engine1 = new ParqueEngine({ dataDir })
    const created = await engine1.create('users', { name: 'Alice', age: 30 })
    await engine1.update('users', created.$id, { $set: { name: 'Alice Smith', age: 31 } })
    await engine1.compact('users')
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

  it('6. delete then compact removes entity from Parquet output', async () => {
    const engine1 = new ParqueEngine({ dataDir })
    const alice = await engine1.create('users', { name: 'Alice' })
    await engine1.create('users', { name: 'Bob' })
    await engine1.delete('users', alice.$id)
    await engine1.compact('users')
    await engine1.close()

    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const results = await engine2.find('users')
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Bob')

    await engine2.close()
  })

  it('7. compactAll produces Parquet files for all tables', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice' })
    await engine.create('posts', { title: 'Hello' })
    await engine.create('comments', { body: 'Nice' })

    await engine.compactAll()

    expect(await hasParquetMagic(join(dataDir, 'users.parquet'))).toBe(true)
    expect(await hasParquetMagic(join(dataDir, 'posts.parquet'))).toBe(true)
    expect(await hasParquetMagic(join(dataDir, 'comments.parquet'))).toBe(true)

    await engine.close()
  })

  it('8. compact + write (to JSONL) + reopen merges Parquet + JSONL correctly', async () => {
    const engine1 = new ParqueEngine({ dataDir })
    await engine1.create('users', { name: 'Alice' })
    await engine1.compact('users')

    // Write after compact goes to fresh JSONL
    await engine1.create('users', { name: 'Bob' })
    await engine1.close()

    // Reopen: should merge Parquet data + JSONL replay
    const engine2 = new ParqueEngine({ dataDir })
    await engine2.init()

    const results = await engine2.find('users')
    expect(results).toHaveLength(2)
    expect(results.map(r => r.name).sort()).toEqual(['Alice', 'Bob'])

    await engine2.close()
  })

  it('9. auto-compact produces real Parquet files', async () => {
    const engine = new ParqueEngine({
      dataDir,
      autoCompact: { maxLines: 3 },
    })

    // Write 4 entities (exceeds maxLines: 3)
    for (let i = 0; i < 4; i++) {
      await engine.create('users', { name: `User-${i}` })
    }

    // Wait for auto-compact to complete
    await new Promise(resolve => setTimeout(resolve, 50))

    // The compacted file should be real Parquet
    expect(await hasParquetMagic(join(dataDir, 'users.parquet'))).toBe(true)

    // All entities should still be accessible
    const results = await engine.find('users')
    expect(results).toHaveLength(4)

    await engine.close()
  })

  it('10. compacted Parquet file is NOT valid JSON (proves it is binary Parquet)', async () => {
    const engine = new ParqueEngine({ dataDir })
    await engine.create('users', { name: 'Alice' })
    await engine.compact('users')

    const dataPath = join(dataDir, 'users.parquet')
    const content = await readFile(dataPath, 'utf-8')

    // It should NOT be parseable as JSON (proving it is real Parquet, not JSON)
    expect(() => JSON.parse(content)).toThrow()

    await engine.close()
  })
})
