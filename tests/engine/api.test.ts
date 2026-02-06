/**
 * Wave 6.2: Barrel Export & Engine Accessor Tests
 *
 * Validates that the @/engine barrel export re-exports all public symbols
 * and that ParqueEngine accessors (tables, dir, getBuffer) work correctly.
 */

import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

// =============================================================================
// Barrel Imports
// =============================================================================

import {
  // Core engine
  ParqueEngine,
  // Buffers
  TableBuffer,
  RelationshipBuffer,
  // Schema
  SchemaRegistry,
  // Merge
  mergeResults,
  // Storage adapters
  LocalStorageAdapter,
  MemoryStorageAdapter,
  // JSONL
  JsonlWriter,
  replay,
  replayInto,
  // JSONL serialization
  serializeLine,
  deserializeLine,
  isDataLine,
  isRelLine,
  // Compaction
  compactDataTable,
  compactRelationships,
  compactEvents,
} from '@/engine'

// =============================================================================
// Barrel Import Tests
// =============================================================================

describe('Barrel imports from @/engine', () => {
  it('exports ParqueEngine as a class', () => {
    expect(ParqueEngine).toBeDefined()
    expect(typeof ParqueEngine).toBe('function')
    const engine = new ParqueEngine({ dataDir: '/tmp/test' })
    expect(engine).toBeInstanceOf(ParqueEngine)
  })

  it('exports TableBuffer, RelationshipBuffer, SchemaRegistry as classes', () => {
    expect(TableBuffer).toBeDefined()
    expect(typeof TableBuffer).toBe('function')
    expect(new TableBuffer()).toBeInstanceOf(TableBuffer)

    expect(RelationshipBuffer).toBeDefined()
    expect(typeof RelationshipBuffer).toBe('function')
    expect(new RelationshipBuffer()).toBeInstanceOf(RelationshipBuffer)

    expect(SchemaRegistry).toBeDefined()
    expect(typeof SchemaRegistry).toBe('function')
    expect(new SchemaRegistry()).toBeInstanceOf(SchemaRegistry)
  })

  it('exports mergeResults as a function', () => {
    expect(mergeResults).toBeDefined()
    expect(typeof mergeResults).toBe('function')
  })

  it('exports LocalStorageAdapter and MemoryStorageAdapter as classes', () => {
    expect(LocalStorageAdapter).toBeDefined()
    expect(typeof LocalStorageAdapter).toBe('function')
    expect(new LocalStorageAdapter()).toBeInstanceOf(LocalStorageAdapter)

    expect(MemoryStorageAdapter).toBeDefined()
    expect(typeof MemoryStorageAdapter).toBe('function')
    expect(new MemoryStorageAdapter()).toBeInstanceOf(MemoryStorageAdapter)
  })

  it('exports JsonlWriter, replay, replayInto', () => {
    expect(JsonlWriter).toBeDefined()
    expect(typeof JsonlWriter).toBe('function')

    expect(replay).toBeDefined()
    expect(typeof replay).toBe('function')

    expect(replayInto).toBeDefined()
    expect(typeof replayInto).toBe('function')
  })

  it('exports serializeLine, deserializeLine, isDataLine, isRelLine', () => {
    expect(serializeLine).toBeDefined()
    expect(typeof serializeLine).toBe('function')

    expect(deserializeLine).toBeDefined()
    expect(typeof deserializeLine).toBe('function')

    expect(isDataLine).toBeDefined()
    expect(typeof isDataLine).toBe('function')

    expect(isRelLine).toBeDefined()
    expect(typeof isRelLine).toBe('function')
  })

  it('exports compactDataTable, compactRelationships, compactEvents as functions', () => {
    expect(compactDataTable).toBeDefined()
    expect(typeof compactDataTable).toBe('function')

    expect(compactRelationships).toBeDefined()
    expect(typeof compactRelationships).toBe('function')

    expect(compactEvents).toBeDefined()
    expect(typeof compactEvents).toBe('function')
  })
})

// =============================================================================
// Engine Accessor Tests
// =============================================================================

describe('ParqueEngine accessors', () => {
  let engine: ParqueEngine
  let dataDir: string

  beforeEach(async () => {
    dataDir = await mkdtemp(join(tmpdir(), 'parquedb-api-test-'))
    engine = new ParqueEngine({ dataDir })
  })

  afterEach(async () => {
    await engine.close()
    await rm(dataDir, { recursive: true, force: true })
  })

  it('engine.tables returns empty array initially', () => {
    expect(engine.tables).toEqual([])
  })

  it('engine.tables returns table names after creates', async () => {
    await engine.create('users', { name: 'Alice' })
    await engine.create('posts', { title: 'Hello' })

    const tables = engine.tables
    expect(tables).toHaveLength(2)
    expect(tables).toContain('users')
    expect(tables).toContain('posts')
  })

  it('engine.dir returns the configured dataDir', () => {
    expect(engine.dir).toBe(dataDir)
  })

  it('engine.getBuffer(table) returns a TableBuffer instance', () => {
    const buffer = engine.getBuffer('users')
    expect(buffer).toBeInstanceOf(TableBuffer)
  })
})
