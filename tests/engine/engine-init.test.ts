/**
 * ParqueEngine init() Table Discovery Tests
 *
 * Validates that init() correctly discovers only actual data table files
 * and excludes system files (rels.parquet, events.parquet), temporary files
 * (*.tmp), and compacting artifacts (*.compacting) from table discovery.
 */

import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParqueEngine } from '@/engine/engine'
import { ParquetStorageAdapter } from '@/engine/parquet-adapter'
import type { DataLine } from '@/engine/types'

let dataDir: string

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'parquedb-init-test-'))
})

afterEach(async () => {
  await rm(dataDir, { recursive: true, force: true })
})

// =============================================================================
// Helper: create a real Parquet file using ParquetStorageAdapter
// =============================================================================

const parquetAdapter = new ParquetStorageAdapter()

async function writeRealParquet(filename: string, data: DataLine[] = []): Promise<void> {
  await parquetAdapter.writeData(join(dataDir, filename), data)
}

async function writeFakeJsonl(filename: string, lines: DataLine[] = []): Promise<void> {
  const content = lines.map(l => JSON.stringify(l)).join('\n') + (lines.length > 0 ? '\n' : '')
  await writeFile(join(dataDir, filename), content, 'utf-8')
}

// =============================================================================
// Table Discovery from .parquet files
// =============================================================================

describe('ParqueEngine - init() table discovery', () => {
  it('discovers actual data tables from .parquet files', async () => {
    // Set up: users.parquet is a legitimate data table
    const userData: DataLine[] = [
      { $id: 'u1', $op: 'c', $v: 1, $ts: Date.now(), name: 'Alice' },
    ]
    await writeRealParquet('users.parquet', userData)

    const engine = new ParqueEngine({ dataDir })
    await engine.init()

    const results = await engine.find('users')
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Alice')

    await engine.close()
  })

  it('excludes rels.parquet from table discovery', async () => {
    // rels.parquet is a system file, not a data table
    await writeRealParquet('rels.parquet', [])
    await writeRealParquet('users.parquet', [
      { $id: 'u1', $op: 'c', $v: 1, $ts: Date.now(), name: 'Alice' },
    ])

    const engine = new ParqueEngine({ dataDir })
    await engine.init()

    const tables = engine.tables
    expect(tables).toContain('users')
    expect(tables).not.toContain('rels')

    await engine.close()
  })

  it('excludes events.parquet from table discovery', async () => {
    // events.parquet is a system file (CDC event log), not a data table
    await writeRealParquet('events.parquet', [])
    await writeRealParquet('users.parquet', [
      { $id: 'u1', $op: 'c', $v: 1, $ts: Date.now(), name: 'Alice' },
    ])

    const engine = new ParqueEngine({ dataDir })
    await engine.init()

    const tables = engine.tables
    expect(tables).toContain('users')
    expect(tables).not.toContain('events')

    await engine.close()
  })

  it('excludes .tmp files from table discovery', async () => {
    // .tmp files are transient and should not be treated as tables
    await writeFile(join(dataDir, 'users.parquet.tmp'), '[]', 'utf-8')
    await writeFile(join(dataDir, 'something.tmp'), '[]', 'utf-8')

    const engine = new ParqueEngine({ dataDir })
    await engine.init()

    const tables = engine.tables
    expect(tables).toHaveLength(0)

    await engine.close()
  })

  it('excludes .compacting files from table discovery as parquet files', async () => {
    // .compacting files are mid-compaction artifacts and should not create phantom tables
    await writeFile(join(dataDir, 'users.parquet.compacting'), '[]', 'utf-8')

    const engine = new ParqueEngine({ dataDir })
    await engine.init()

    const tables = engine.tables
    // Should not create a table named "users.parquet" from "users.parquet.compacting"
    expect(tables).not.toContain('users.parquet')

    await engine.close()
  })

  it('only includes legitimate data tables when mixed with system and temp files', async () => {
    // Mix of real data tables and files that should be excluded
    await writeRealParquet('users.parquet', [
      { $id: 'u1', $op: 'c', $v: 1, $ts: Date.now(), name: 'Alice' },
    ])
    await writeRealParquet('posts.parquet', [
      { $id: 'p1', $op: 'c', $v: 1, $ts: Date.now(), title: 'Hello' },
    ])
    await writeRealParquet('rels.parquet', [])
    await writeRealParquet('events.parquet', [])
    await writeFile(join(dataDir, 'temp.parquet.tmp'), '[]', 'utf-8')
    await writeFile(join(dataDir, 'data.tmp'), '[]', 'utf-8')
    await writeFile(join(dataDir, 'something.compacting'), '[]', 'utf-8')

    const engine = new ParqueEngine({ dataDir })
    await engine.init()

    const tables = engine.tables
    expect(tables.sort()).toEqual(['posts', 'users'])

    const users = await engine.find('users')
    expect(users).toHaveLength(1)

    const posts = await engine.find('posts')
    expect(posts).toHaveLength(1)

    await engine.close()
  })
})

// =============================================================================
// Table Discovery from .jsonl files
// =============================================================================

describe('ParqueEngine - init() JSONL table discovery', () => {
  it('excludes events.jsonl from table discovery', async () => {
    await writeFakeJsonl('events.jsonl', [])
    await writeFakeJsonl('users.jsonl', [
      { $id: 'u1', $op: 'c', $v: 1, $ts: Date.now(), name: 'Alice' },
    ])

    const engine = new ParqueEngine({ dataDir })
    await engine.init()

    const tables = engine.tables
    expect(tables).toContain('users')
    expect(tables).not.toContain('events')

    await engine.close()
  })

  it('excludes rels.jsonl from table discovery', async () => {
    await writeFakeJsonl('rels.jsonl', [])
    await writeFakeJsonl('users.jsonl', [
      { $id: 'u1', $op: 'c', $v: 1, $ts: Date.now(), name: 'Alice' },
    ])

    const engine = new ParqueEngine({ dataDir })
    await engine.init()

    const tables = engine.tables
    expect(tables).toContain('users')
    expect(tables).not.toContain('rels')

    await engine.close()
  })
})
