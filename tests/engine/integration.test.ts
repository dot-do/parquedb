/**
 * ParqueEngine Full Integration Test Suite
 *
 * End-to-end tests that exercise the full lifecycle across all MergeTree
 * components: write path, read path, compaction, schema, and relationships.
 *
 * Each test uses a fresh temp directory and engine instance.
 * Compaction tests use the compactor functions directly with a JSON-based
 * StorageAdapter since the engine does not have compact() wired yet.
 */

import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ParqueEngine } from '@/engine/engine'
import { mergeResults } from '@/engine/merge'
import { compactDataTable } from '@/engine/compactor'
import type { StorageAdapter } from '@/engine/compactor'
import { compactRelationships } from '@/engine/compactor-rels'
import type { RelStorageAdapter } from '@/engine/compactor-rels'
import { RelationshipBuffer } from '@/engine/rel-buffer'
import { SchemaRegistry } from '@/engine/schema'
import { replay } from '@/engine/jsonl-reader'
import { JsonlWriter } from '@/engine/jsonl-writer'
import type { DataLine, EventLine, RelLine } from '@/engine/types'

// =============================================================================
// Helpers
// =============================================================================

let engine: ParqueEngine
let dataDir: string

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'parquedb-integration-'))
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

/** Read a JSON file into typed array, returning [] on ENOENT */
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

/** Write DataLines as JSONL file */
async function writeJsonlFile(path: string, lines: Record<string, unknown>[]): Promise<void> {
  const content = lines.map(l => JSON.stringify(l)).join('\n') + '\n'
  await writeFile(path, content, 'utf-8')
}

/** JSON-based StorageAdapter for data compaction tests */
function createDataStorage(): StorageAdapter {
  return {
    readData: (path: string) => readJsonFile<DataLine>(path),
    writeData: (path: string, data: DataLine[]) => writeJsonFile(path, data),
  }
}

/** JSON-based RelStorageAdapter for relationship compaction tests */
function createRelStorage(): RelStorageAdapter {
  return {
    readRels: (path: string) => readJsonFile<RelLine>(path),
    writeRels: (path: string, data: RelLine[]) => writeJsonFile(path, data),
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
// Basic CRUD Lifecycle
// =============================================================================

describe('Integration: Basic CRUD Lifecycle', () => {
  it('1. create -> find -> returns entity', async () => {
    const created = await engine.create('users', { name: 'Alice', age: 30 })

    const results = await engine.find('users')
    expect(results).toHaveLength(1)
    expect(results[0].$id).toBe(created.$id)
    expect(results[0].name).toBe('Alice')
    expect(results[0].age).toBe(30)
  })

  it('2. create -> update -> find -> returns updated entity', async () => {
    const created = await engine.create('users', { name: 'Alice', age: 30 })

    await engine.update('users', created.$id, { $set: { name: 'Alice Smith', age: 31 } })

    const results = await engine.find('users')
    expect(results).toHaveLength(1)
    expect(results[0].$id).toBe(created.$id)
    expect(results[0].name).toBe('Alice Smith')
    expect(results[0].age).toBe(31)
    expect(results[0].$v).toBe(2)
  })

  it('3. create -> delete -> find -> returns empty', async () => {
    const created = await engine.create('users', { name: 'Alice' })

    await engine.delete('users', created.$id)

    const results = await engine.find('users')
    expect(results).toHaveLength(0)
  })

  it('4. create multiple -> find with filter -> returns matching subset', async () => {
    await engine.create('users', { name: 'Alice', role: 'admin' })
    await engine.create('users', { name: 'Bob', role: 'user' })
    await engine.create('users', { name: 'Charlie', role: 'admin' })
    await engine.create('users', { name: 'Diana', role: 'user' })

    const admins = await engine.find('users', { role: 'admin' })
    expect(admins).toHaveLength(2)
    const names = admins.map(r => r.name)
    expect(names).toContain('Alice')
    expect(names).toContain('Charlie')
  })

  it('5. create -> get by ID -> returns entity', async () => {
    const created = await engine.create('users', { name: 'Alice', email: 'alice@test.com' })

    const result = await engine.get('users', created.$id)
    expect(result).not.toBeNull()
    expect(result!.$id).toBe(created.$id)
    expect(result!.name).toBe('Alice')
    expect(result!.email).toBe('alice@test.com')
  })

  it('6. create -> getMany -> returns entities in order', async () => {
    const a = await engine.create('users', { $id: 'u1', name: 'Alice' })
    const b = await engine.create('users', { $id: 'u2', name: 'Bob' })
    const c = await engine.create('users', { $id: 'u3', name: 'Charlie' })

    const results = await engine.getMany('users', ['u3', 'u1', 'u2'])
    expect(results).toHaveLength(3)
    expect(results[0]!.name).toBe('Charlie')
    expect(results[1]!.name).toBe('Alice')
    expect(results[2]!.name).toBe('Bob')
  })
})

// =============================================================================
// Write -> Compact -> Read Lifecycle
// =============================================================================

describe('Integration: Write -> Compact -> Read Lifecycle', () => {
  it('7. create 5 entities in JSONL -> compactDataTable -> all 5 in compacted file', async () => {
    // Write 5 entities via the engine
    for (let i = 0; i < 5; i++) {
      await engine.create('users', { $id: `u${i}`, name: `User-${i}` })
    }
    await engine.close()

    // Now compact directly
    const storage = createDataStorage()

    // The engine wrote to users.jsonl; simulate rotation by renaming
    const jsonlPath = join(dataDir, 'users.jsonl')
    const compactingPath = jsonlPath + '.compacting'
    const { rename: fsRename } = await import('node:fs/promises')
    await fsRename(jsonlPath, compactingPath)
    await writeFile(jsonlPath, '', 'utf-8')

    // Read JSONL content, merge with empty existing data
    const jsonlData = await replay<DataLine>(compactingPath)
    const existing = await storage.readData(join(dataDir, 'users.parquet'))
    const merged = mergeResults(existing, jsonlData)

    expect(merged).toHaveLength(5)
    const ids = merged.map(e => e.$id).sort()
    expect(ids).toEqual(['u0', 'u1', 'u2', 'u3', 'u4'])
  })

  it('8. create 3 -> compact -> create 2 more -> merge -> all 5 present', async () => {
    // Phase 1: create 3 entities
    const compactedData: DataLine[] = [
      makeLine({ $id: 'u1', name: 'Alice' }),
      makeLine({ $id: 'u2', name: 'Bob' }),
      makeLine({ $id: 'u3', name: 'Charlie' }),
    ]

    // Phase 2: new entities in buffer
    const bufferData: DataLine[] = [
      makeLine({ $id: 'u4', name: 'Diana' }),
      makeLine({ $id: 'u5', name: 'Eve' }),
    ]

    const merged = mergeResults(compactedData, bufferData)
    expect(merged).toHaveLength(5)
    expect(merged.map(e => e.$id).sort()).toEqual(['u1', 'u2', 'u3', 'u4', 'u5'])
  })

  it('9. create -> update -> compact -> compacted file has updated version', async () => {
    // Compacted data
    const compactedData: DataLine[] = [
      makeLine({ $id: 'u1', $v: 1, name: 'Alice' }),
    ]

    // Buffer has an update
    const bufferData: DataLine[] = [
      makeLine({ $id: 'u1', $v: 2, $op: 'u', name: 'Alice Updated' }),
    ]

    const merged = mergeResults(compactedData, bufferData)
    expect(merged).toHaveLength(1)
    expect(merged[0].name).toBe('Alice Updated')
    expect(merged[0].$v).toBe(2)
  })

  it('10. create -> delete -> compact -> entity not in compacted file', async () => {
    // Compacted data
    const compactedData: DataLine[] = [
      makeLine({ $id: 'u1', $v: 1, name: 'Alice' }),
      makeLine({ $id: 'u2', $v: 1, name: 'Bob' }),
    ]

    // Buffer has a delete
    const bufferData: DataLine[] = [
      makeLine({ $id: 'u1', $v: 2, $op: 'd' }),
    ]

    const merged = mergeResults(compactedData, bufferData)
    expect(merged).toHaveLength(1)
    expect(merged[0].$id).toBe('u2')
  })

  it('11. create -> compact -> update in JSONL -> merge returns updated version (buffer wins)', async () => {
    // Simulate: entity in compacted parquet
    const parquetResults: DataLine[] = [
      makeLine({ $id: 'u1', $v: 1, name: 'Alice' }),
    ]

    // Buffer has an update with higher version
    const bufferResults: DataLine[] = [
      makeLine({ $id: 'u1', $v: 2, $op: 'u', name: 'Alice Updated' }),
    ]

    const merged = mergeResults(parquetResults, bufferResults)
    expect(merged).toHaveLength(1)
    expect(merged[0].name).toBe('Alice Updated')
    expect(merged[0].$v).toBe(2)
  })

  it('12. create -> compact -> delete in JSONL -> merge excludes entity', async () => {
    const parquetResults: DataLine[] = [
      makeLine({ $id: 'u1', $v: 1, name: 'Alice' }),
      makeLine({ $id: 'u2', $v: 1, name: 'Bob' }),
    ]

    const bufferResults: DataLine[] = [
      makeLine({ $id: 'u2', $v: 2, $op: 'd' }),
    ]

    const merged = mergeResults(parquetResults, bufferResults)
    expect(merged).toHaveLength(1)
    expect(merged[0].$id).toBe('u1')
    expect(merged[0].name).toBe('Alice')
  })
})

// =============================================================================
// Multi-Table
// =============================================================================

describe('Integration: Multi-Table', () => {
  it('13. create users and posts in separate tables -> each has its own JSONL file', async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice' })
    await engine.create('users', { $id: 'u2', name: 'Bob' })
    await engine.create('posts', { $id: 'p1', title: 'Hello World' })
    await engine.create('posts', { $id: 'p2', title: 'Second Post' })
    await engine.create('posts', { $id: 'p3', title: 'Third Post' })

    // Verify separate JSONL files
    const userLines = await readJsonl<DataLine>(join(dataDir, 'users.jsonl'))
    expect(userLines).toHaveLength(2)

    const postLines = await readJsonl<DataLine>(join(dataDir, 'posts.jsonl'))
    expect(postLines).toHaveLength(3)
  })

  it('14. compact users -> does not affect posts JSONL', async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice' })
    await engine.create('posts', { $id: 'p1', title: 'Hello' })
    await engine.close()

    // Manually compact users
    const storage = createDataStorage()
    const usersJsonl = join(dataDir, 'users.jsonl')
    const compactingPath = usersJsonl + '.compacting'
    const { rename: fsRename } = await import('node:fs/promises')
    await fsRename(usersJsonl, compactingPath)
    await writeFile(usersJsonl, '', 'utf-8')

    const jsonlData = await replay<DataLine>(compactingPath)
    await storage.writeData(join(dataDir, 'users.parquet'), jsonlData)

    // Posts JSONL should still be intact
    const postLines = await readJsonl<DataLine>(join(dataDir, 'posts.jsonl'))
    expect(postLines).toHaveLength(1)
    expect(postLines[0].title).toBe('Hello')
  })

  it('15. find on each table returns correct entities', async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice' })
    await engine.create('users', { $id: 'u2', name: 'Bob' })
    await engine.create('posts', { $id: 'p1', title: 'Hello' })

    const users = await engine.find('users')
    expect(users).toHaveLength(2)
    expect(users.map(u => u.name).sort()).toEqual(['Alice', 'Bob'])

    const posts = await engine.find('posts')
    expect(posts).toHaveLength(1)
    expect(posts[0].title).toBe('Hello')
  })
})

// =============================================================================
// Relationship Lifecycle
// =============================================================================

describe('Integration: Relationship Lifecycle', () => {
  it('16. create RelationshipBuffer -> link -> getForward returns target', () => {
    const rels = new RelationshipBuffer()

    rels.link({ f: 'u1', p: 'posts', r: 'author', t: 'p1', $ts: Date.now() })

    const targets = rels.getForward('u1', 'posts')
    expect(targets).toEqual(['p1'])
  })

  it('17. link -> unlink -> getForward returns empty', () => {
    const rels = new RelationshipBuffer()

    rels.link({ f: 'u1', p: 'posts', r: 'author', t: 'p1', $ts: Date.now() })
    rels.unlink({ f: 'u1', p: 'posts', t: 'p1', $ts: Date.now() })

    const targets = rels.getForward('u1', 'posts')
    expect(targets).toEqual([])
  })

  it('18. multiple links -> getReverse returns all sources', () => {
    const rels = new RelationshipBuffer()

    const ts = Date.now()
    rels.link({ f: 'u1', p: 'likes', r: 'likedBy', t: 'p1', $ts: ts })
    rels.link({ f: 'u2', p: 'likes', r: 'likedBy', t: 'p1', $ts: ts })
    rels.link({ f: 'u3', p: 'likes', r: 'likedBy', t: 'p1', $ts: ts })

    const sources = rels.getReverse('p1', 'likedBy')
    expect(sources.sort()).toEqual(['u1', 'u2', 'u3'])
  })

  it('19. link/unlink operations -> compact rels -> compacted file is correct', async () => {
    const relStorage = createRelStorage()

    // Write relationship mutations to rels.jsonl
    const relsJsonlPath = join(dataDir, 'rels.jsonl')
    const writer = new JsonlWriter(relsJsonlPath)

    const ts = Date.now()
    await writer.append({ $op: 'l', $ts: ts, f: 'u1', p: 'posts', r: 'author', t: 'p1' })
    await writer.append({ $op: 'l', $ts: ts, f: 'u1', p: 'posts', r: 'author', t: 'p2' })
    await writer.append({ $op: 'l', $ts: ts, f: 'u2', p: 'posts', r: 'author', t: 'p3' })
    // Unlink one relationship
    await writer.append({ $op: 'u', $ts: ts + 1, f: 'u1', p: 'posts', r: 'author', t: 'p2' })
    await writer.close()

    // Compact
    const count = await compactRelationships(dataDir, relStorage)

    expect(count).toBe(2) // p1 + p3 survive, p2 was unlinked
    const compacted = await readJsonFile<RelLine>(join(dataDir, 'rels.parquet'))
    expect(compacted).toHaveLength(2)

    const pairs = compacted.map(r => `${r.f}->${r.t}`)
    expect(pairs).toContain('u1->p1')
    expect(pairs).toContain('u2->p3')
    expect(pairs).not.toContain('u1->p2')
  })
})

// =============================================================================
// Schema Lifecycle
// =============================================================================

describe('Integration: Schema Lifecycle', () => {
  it('20. define schema -> registry.get() returns schema', () => {
    const registry = new SchemaRegistry()

    registry.define('users', { name: 'string', email: 'string', age: 'int' })

    const schema = registry.get('users')
    expect(schema).toEqual({ name: 'string', email: 'string', age: 'int' })
  })

  it('21. define -> evolve (add field) -> get() returns updated schema', () => {
    const registry = new SchemaRegistry()

    registry.define('users', { name: 'string', email: 'string' })
    registry.evolve('users', { added: ['role'] })

    const schema = registry.get('users')
    expect(schema).toEqual({ name: 'string', email: 'string', role: 'string' })
  })

  it('22. define at t=1000 -> evolve at t=2000 -> getAt(t=1500) returns original', () => {
    const registry = new SchemaRegistry()

    // Use replayEvent to set specific timestamps
    registry.replayEvent({
      id: 'ev1',
      ts: 1000,
      op: 's',
      ns: 'users',
      schema: { name: 'string', email: 'string' },
    })

    registry.replayEvent({
      id: 'ev2',
      ts: 2000,
      op: 's',
      ns: 'users',
      schema: { name: 'string', email: 'string', role: 'string' },
      migration: { added: ['role'] },
    })

    // At t=1500, only the first schema should be visible
    const schemaAt1500 = registry.getAt('users', 1500)
    expect(schemaAt1500).toEqual({ name: 'string', email: 'string' })

    // At t=2500, the evolved schema should be visible
    const schemaAt2500 = registry.getAt('users', 2500)
    expect(schemaAt2500).toEqual({ name: 'string', email: 'string', role: 'string' })
  })
})

// =============================================================================
// CRUD + Filter Combinations
// =============================================================================

describe('Integration: CRUD + Filter Combinations', () => {
  it('create, update some, delete some -> find with filter returns correct subset', async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice', active: true })
    await engine.create('users', { $id: 'u2', name: 'Bob', active: true })
    await engine.create('users', { $id: 'u3', name: 'Charlie', active: true })
    await engine.create('users', { $id: 'u4', name: 'Diana', active: true })

    // Deactivate Bob
    await engine.update('users', 'u2', { $set: { active: false } })

    // Delete Charlie
    await engine.delete('users', 'u3')

    const activeUsers = await engine.find('users', { active: true })
    expect(activeUsers).toHaveLength(2)
    const names = activeUsers.map(u => u.name).sort()
    expect(names).toEqual(['Alice', 'Diana'])
  })

  it('findOne returns the first match', async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice', role: 'admin' })
    await engine.create('users', { $id: 'u2', name: 'Bob', role: 'user' })

    const result = await engine.findOne('users', { role: 'admin' })
    expect(result).not.toBeNull()
    expect(result!.name).toBe('Alice')
  })

  it('findOne returns null when no match', async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice', role: 'admin' })

    const result = await engine.findOne('users', { role: 'superadmin' })
    expect(result).toBeNull()
  })

  it('count returns correct number after mixed operations', async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice' })
    await engine.create('users', { $id: 'u2', name: 'Bob' })
    await engine.create('users', { $id: 'u3', name: 'Charlie' })

    expect(await engine.count('users')).toBe(3)

    await engine.delete('users', 'u2')

    expect(await engine.count('users')).toBe(2)
  })

  it('get returns null for deleted entity', async () => {
    const created = await engine.create('users', { name: 'Alice' })
    await engine.delete('users', created.$id)

    const result = await engine.get('users', created.$id)
    expect(result).toBeNull()
  })

  it('getMany returns null for deleted entities', async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice' })
    await engine.create('users', { $id: 'u2', name: 'Bob' })
    await engine.create('users', { $id: 'u3', name: 'Charlie' })

    await engine.delete('users', 'u2')

    const results = await engine.getMany('users', ['u1', 'u2', 'u3'])
    expect(results[0]!.name).toBe('Alice')
    expect(results[1]).toBeNull()
    expect(results[2]!.name).toBe('Charlie')
  })
})

// =============================================================================
// Sort / Skip / Limit
// =============================================================================

describe('Integration: Sort / Skip / Limit', () => {
  beforeEach(async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice', age: 30 })
    await engine.create('users', { $id: 'u2', name: 'Bob', age: 25 })
    await engine.create('users', { $id: 'u3', name: 'Charlie', age: 35 })
    await engine.create('users', { $id: 'u4', name: 'Diana', age: 28 })
  })

  it('find with sort ascending', async () => {
    const results = await engine.find('users', {}, { sort: { age: 1 } })
    expect(results.map(r => r.name)).toEqual(['Bob', 'Diana', 'Alice', 'Charlie'])
  })

  it('find with sort descending', async () => {
    const results = await engine.find('users', {}, { sort: { age: -1 } })
    expect(results.map(r => r.name)).toEqual(['Charlie', 'Alice', 'Diana', 'Bob'])
  })

  it('find with limit', async () => {
    const results = await engine.find('users', {}, { sort: { age: 1 }, limit: 2 })
    expect(results).toHaveLength(2)
    expect(results.map(r => r.name)).toEqual(['Bob', 'Diana'])
  })

  it('find with skip', async () => {
    const results = await engine.find('users', {}, { sort: { age: 1 }, skip: 2 })
    expect(results).toHaveLength(2)
    expect(results.map(r => r.name)).toEqual(['Alice', 'Charlie'])
  })

  it('find with skip + limit', async () => {
    const results = await engine.find('users', {}, { sort: { age: 1 }, skip: 1, limit: 2 })
    expect(results).toHaveLength(2)
    expect(results.map(r => r.name)).toEqual(['Diana', 'Alice'])
  })
})

// =============================================================================
// Logical Operators ($or / $and)
// =============================================================================

describe('Integration: Logical Operators', () => {
  beforeEach(async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice', age: 30, role: 'admin' })
    await engine.create('users', { $id: 'u2', name: 'Bob', age: 25, role: 'user' })
    await engine.create('users', { $id: 'u3', name: 'Charlie', age: 35, role: 'admin' })
    await engine.create('users', { $id: 'u4', name: 'Diana', age: 28, role: 'user' })
  })

  it('$or matches entities matching any sub-filter', async () => {
    const results = await engine.find('users', {
      $or: [{ name: 'Alice' }, { name: 'Diana' }],
    })
    expect(results).toHaveLength(2)
    expect(results.map(r => r.name).sort()).toEqual(['Alice', 'Diana'])
  })

  it('$and matches entities matching all sub-filters', async () => {
    const results = await engine.find('users', {
      $and: [{ role: 'admin' }, { age: { $gt: 30 } }],
    })
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Charlie')
  })
})

// =============================================================================
// Merge Semantics Detailed
// =============================================================================

describe('Integration: Merge Semantics', () => {
  it('higher version in buffer wins over lower version in parquet', () => {
    const parquet: DataLine[] = [
      makeLine({ $id: 'u1', $v: 1, name: 'Alice' }),
    ]
    const buffer: DataLine[] = [
      makeLine({ $id: 'u1', $v: 3, $op: 'u', name: 'Alice v3' }),
    ]

    const merged = mergeResults(parquet, buffer)
    expect(merged).toHaveLength(1)
    expect(merged[0].name).toBe('Alice v3')
    expect(merged[0].$v).toBe(3)
  })

  it('equal version => buffer wins (tie-breaking)', () => {
    const parquet: DataLine[] = [
      makeLine({ $id: 'u1', $v: 1, name: 'Alice Parquet' }),
    ]
    const buffer: DataLine[] = [
      makeLine({ $id: 'u1', $v: 1, name: 'Alice Buffer' }),
    ]

    const merged = mergeResults(parquet, buffer)
    expect(merged).toHaveLength(1)
    expect(merged[0].name).toBe('Alice Buffer')
  })

  it('merged results are sorted by $id', () => {
    const parquet: DataLine[] = [
      makeLine({ $id: 'charlie', $v: 1, name: 'Charlie' }),
      makeLine({ $id: 'alice', $v: 1, name: 'Alice' }),
    ]
    const buffer: DataLine[] = [
      makeLine({ $id: 'bob', $v: 1, name: 'Bob' }),
    ]

    const merged = mergeResults(parquet, buffer)
    expect(merged.map(e => e.$id)).toEqual(['alice', 'bob', 'charlie'])
  })

  it('tombstones in buffer suppress parquet entities', () => {
    const parquet: DataLine[] = [
      makeLine({ $id: 'u1', $v: 1, name: 'Alice' }),
      makeLine({ $id: 'u2', $v: 1, name: 'Bob' }),
      makeLine({ $id: 'u3', $v: 1, name: 'Charlie' }),
    ]
    const buffer: DataLine[] = [
      makeLine({ $id: 'u1', $v: 2, $op: 'd' }),
      makeLine({ $id: 'u3', $v: 2, $op: 'd' }),
    ]

    const merged = mergeResults(parquet, buffer)
    expect(merged).toHaveLength(1)
    expect(merged[0].$id).toBe('u2')
  })
})

// =============================================================================
// Engine JSONL + Events Integrity
// =============================================================================

describe('Integration: JSONL + Events Integrity', () => {
  it('each CRUD operation produces matching data and event lines', async () => {
    const created = await engine.create('users', { name: 'Alice' })
    await engine.update('users', created.$id, { $set: { name: 'Alice Updated' } })
    await engine.delete('users', created.$id)

    // Read data JSONL
    const dataLines = await readJsonl<DataLine>(join(dataDir, 'users.jsonl'))
    expect(dataLines).toHaveLength(3)
    expect(dataLines[0].$op).toBe('c')
    expect(dataLines[1].$op).toBe('u')
    expect(dataLines[2].$op).toBe('d')

    // Read events JSONL
    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))
    expect(events).toHaveLength(3)
    expect(events[0].op).toBe('c')
    expect(events[0].after).toEqual({ name: 'Alice' })
    expect(events[0].before).toBeUndefined()

    expect(events[1].op).toBe('u')
    expect(events[1].before).toEqual({ name: 'Alice' })
    expect(events[1].after).toEqual({ name: 'Alice Updated' })

    expect(events[2].op).toBe('d')
    expect(events[2].before).toEqual({ name: 'Alice Updated' })
    expect(events[2].after).toBeUndefined()
  })

  it('events reference correct entity IDs', async () => {
    const alice = await engine.create('users', { name: 'Alice' })
    const bob = await engine.create('users', { name: 'Bob' })

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))
    expect(events).toHaveLength(2)
    expect(events[0].eid).toBe(alice.$id)
    expect(events[1].eid).toBe(bob.$id)
  })

  it('multi-table events all go to shared events.jsonl', async () => {
    await engine.create('users', { $id: 'u1', name: 'Alice' })
    await engine.create('posts', { $id: 'p1', title: 'Hello' })

    const events = await readJsonl<EventLine>(join(dataDir, 'events.jsonl'))
    expect(events).toHaveLength(2)
    expect(events[0].ns).toBe('users')
    expect(events[1].ns).toBe('posts')
  })
})
