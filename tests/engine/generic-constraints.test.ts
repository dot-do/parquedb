/**
 * Generic Constraint Type Tests
 *
 * Verifies that generic type parameters on JSONL reader functions,
 * SQLite WAL replayUnflushed, and readParquetFromR2 are properly
 * constrained to `Record<string, unknown>`.
 *
 * Uses vitest's `expectTypeOf` for compile-time type assertions.
 * These tests validate type safety without executing runtime logic.
 */

import { describe, it, expectTypeOf } from 'vitest'
import { replay, replayInto, replayRange } from '@/engine/jsonl-reader'
import type { SqliteWal } from '@/engine/sqlite-wal'
import type { DataLine, RelLine, EventLine, SchemaLine } from '@/engine/types'

// =============================================================================
// replay<T>() constraints
// =============================================================================

describe('replay<T>() generic constraints', () => {
  it('accepts DataLine (extends Record<string, unknown>)', () => {
    expectTypeOf(replay<DataLine>).toBeFunction()
    expectTypeOf(replay<DataLine>).returns.resolves.toEqualTypeOf<DataLine[]>()
  })

  it('accepts RelLine (extends Record<string, unknown>)', () => {
    expectTypeOf(replay<RelLine>).toBeFunction()
    expectTypeOf(replay<RelLine>).returns.resolves.toEqualTypeOf<RelLine[]>()
  })

  it('accepts EventLine (extends Record<string, unknown>)', () => {
    expectTypeOf(replay<EventLine>).toBeFunction()
    expectTypeOf(replay<EventLine>).returns.resolves.toEqualTypeOf<EventLine[]>()
  })

  it('accepts SchemaLine (extends Record<string, unknown>)', () => {
    expectTypeOf(replay<SchemaLine>).toBeFunction()
    expectTypeOf(replay<SchemaLine>).returns.resolves.toEqualTypeOf<SchemaLine[]>()
  })

  it('accepts Record<string, unknown> (default)', () => {
    expectTypeOf(replay).toBeFunction()
    expectTypeOf(replay).returns.resolves.toEqualTypeOf<Record<string, unknown>[]>()
  })

  it('rejects string (does not extend Record<string, unknown>)', () => {
    // @ts-expect-error - string does not satisfy Record<string, unknown> constraint
    replay<string>
  })

  it('rejects number (does not extend Record<string, unknown>)', () => {
    // @ts-expect-error - number does not satisfy Record<string, unknown> constraint
    replay<number>
  })

  it('rejects boolean (does not extend Record<string, unknown>)', () => {
    // @ts-expect-error - boolean does not satisfy Record<string, unknown> constraint
    replay<boolean>
  })

  it('rejects string[] (does not extend Record<string, unknown>)', () => {
    // @ts-expect-error - string[] does not satisfy Record<string, unknown> constraint
    replay<string[]>
  })
})

// =============================================================================
// replayInto<T>() constraints
// =============================================================================

describe('replayInto<T>() generic constraints', () => {
  it('accepts DataLine (extends Record<string, unknown>)', () => {
    expectTypeOf(replayInto<DataLine>).toBeFunction()
  })

  it('accepts RelLine (extends Record<string, unknown>)', () => {
    expectTypeOf(replayInto<RelLine>).toBeFunction()
  })

  it('accepts EventLine (extends Record<string, unknown>)', () => {
    expectTypeOf(replayInto<EventLine>).toBeFunction()
  })

  it('rejects string (does not extend Record<string, unknown>)', () => {
    // @ts-expect-error - string does not satisfy Record<string, unknown> constraint
    replayInto<string>
  })

  it('rejects number (does not extend Record<string, unknown>)', () => {
    // @ts-expect-error - number does not satisfy Record<string, unknown> constraint
    replayInto<number>
  })
})

// =============================================================================
// replayRange<T>() constraints
// =============================================================================

describe('replayRange<T>() generic constraints', () => {
  it('accepts DataLine (extends Record<string, unknown>)', () => {
    expectTypeOf(replayRange<DataLine>).toBeFunction()
    expectTypeOf(replayRange<DataLine>).returns.resolves.toEqualTypeOf<DataLine[]>()
  })

  it('accepts RelLine (extends Record<string, unknown>)', () => {
    expectTypeOf(replayRange<RelLine>).toBeFunction()
  })

  it('accepts EventLine (extends Record<string, unknown>)', () => {
    expectTypeOf(replayRange<EventLine>).toBeFunction()
  })

  it('rejects string (does not extend Record<string, unknown>)', () => {
    // @ts-expect-error - string does not satisfy Record<string, unknown> constraint
    replayRange<string>
  })

  it('rejects number (does not extend Record<string, unknown>)', () => {
    // @ts-expect-error - number does not satisfy Record<string, unknown> constraint
    replayRange<number>
  })
})

// =============================================================================
// SqliteWal.replayUnflushed<T>() constraints
// =============================================================================

describe('SqliteWal.replayUnflushed<T>() generic constraints', () => {
  // Helper to test the constraint on SqliteWal.replayUnflushed.
  // We use a function that accepts a SqliteWal instance and calls
  // replayUnflushed with the type parameter, avoiding esbuild
  // parse issues with advanced type-level generic instantiation.

  it('accepts DataLine (extends Record<string, unknown>)', () => {
    const fn = (wal: SqliteWal) => wal.replayUnflushed<DataLine>('test')
    expectTypeOf(fn).returns.toEqualTypeOf<DataLine[]>()
  })

  it('accepts RelLine (extends Record<string, unknown>)', () => {
    const fn = (wal: SqliteWal) => wal.replayUnflushed<RelLine>('rels')
    expectTypeOf(fn).returns.toEqualTypeOf<RelLine[]>()
  })

  it('accepts EventLine (extends Record<string, unknown>)', () => {
    const fn = (wal: SqliteWal) => wal.replayUnflushed<EventLine>('events')
    expectTypeOf(fn).returns.toEqualTypeOf<EventLine[]>()
  })

  it('accepts Record<string, unknown> (default)', () => {
    const fn = (wal: SqliteWal) => wal.replayUnflushed('test')
    expectTypeOf(fn).returns.toEqualTypeOf<Record<string, unknown>[]>()
  })

  it('rejects string (does not extend Record<string, unknown>)', () => {
    // @ts-expect-error - string does not satisfy Record<string, unknown> constraint
    const fn = (wal: SqliteWal) => wal.replayUnflushed<string>('test')
  })

  it('rejects number (does not extend Record<string, unknown>)', () => {
    // @ts-expect-error - number does not satisfy Record<string, unknown> constraint
    const fn = (wal: SqliteWal) => wal.replayUnflushed<number>('test')
  })
})
