/**
 * Tests for src/engine/sort.ts
 *
 * Extracted from engine.ts: sortEntities() is a pure function that sorts
 * DataLine arrays by a sort specification (field -> 1 ascending / -1 descending).
 */

import { describe, it, expect } from 'vitest'
import { sortEntities } from '../../src/engine/sort'
import type { DataLine } from '../../src/engine/types'

// =============================================================================
// Helpers
// =============================================================================

function makeLine(id: string, fields: Record<string, unknown> = {}): DataLine {
  return { $id: id, $op: 'c', $v: 1, $ts: 1000, ...fields }
}

// =============================================================================
// Single-field sort
// =============================================================================

describe('sortEntities — single field', () => {
  it('sorts ascending by a string field', () => {
    const entities = [
      makeLine('3', { name: 'Charlie' }),
      makeLine('1', { name: 'Alice' }),
      makeLine('2', { name: 'Bob' }),
    ]
    const sorted = sortEntities(entities, { name: 1 })
    expect(sorted.map(e => e.name)).toEqual(['Alice', 'Bob', 'Charlie'])
  })

  it('sorts descending by a string field', () => {
    const entities = [
      makeLine('1', { name: 'Alice' }),
      makeLine('3', { name: 'Charlie' }),
      makeLine('2', { name: 'Bob' }),
    ]
    const sorted = sortEntities(entities, { name: -1 })
    expect(sorted.map(e => e.name)).toEqual(['Charlie', 'Bob', 'Alice'])
  })

  it('sorts ascending by a numeric field', () => {
    const entities = [
      makeLine('3', { age: 30 }),
      makeLine('1', { age: 10 }),
      makeLine('2', { age: 20 }),
    ]
    const sorted = sortEntities(entities, { age: 1 })
    expect(sorted.map(e => (e as Record<string, unknown>).age)).toEqual([10, 20, 30])
  })

  it('sorts descending by a numeric field', () => {
    const entities = [
      makeLine('1', { age: 10 }),
      makeLine('3', { age: 30 }),
      makeLine('2', { age: 20 }),
    ]
    const sorted = sortEntities(entities, { age: -1 })
    expect(sorted.map(e => (e as Record<string, unknown>).age)).toEqual([30, 20, 10])
  })
})

// =============================================================================
// null / undefined handling
// =============================================================================

describe('sortEntities — null/undefined values', () => {
  it('puts null/undefined values before defined values (ascending)', () => {
    const entities = [
      makeLine('2', { name: 'Bob' }),
      makeLine('3', { name: null }),
      makeLine('1', { name: 'Alice' }),
    ]
    const sorted = sortEntities(entities, { name: 1 })
    expect(sorted.map(e => e.name)).toEqual([null, 'Alice', 'Bob'])
  })

  it('puts missing fields before defined values (ascending)', () => {
    const entities = [
      makeLine('2', { name: 'Bob' }),
      makeLine('3', {}),  // name is undefined
      makeLine('1', { name: 'Alice' }),
    ]
    const sorted = sortEntities(entities, { name: 1 })
    const names = sorted.map(e => e.name)
    expect(names[0]).toBeUndefined()
    expect(names[1]).toBe('Alice')
    expect(names[2]).toBe('Bob')
  })

  it('puts null/undefined values after defined values (descending)', () => {
    const entities = [
      makeLine('3', { name: null }),
      makeLine('1', { name: 'Alice' }),
      makeLine('2', { name: 'Bob' }),
    ]
    const sorted = sortEntities(entities, { name: -1 })
    // descending: defined values first, then null
    expect(sorted.map(e => e.name)).toEqual(['Bob', 'Alice', null])
  })
})

// =============================================================================
// Multi-field sort (tiebreakers)
// =============================================================================

describe('sortEntities — multi-field tiebreakers', () => {
  it('uses second field as tiebreaker', () => {
    const entities = [
      makeLine('1', { status: 'active', name: 'Charlie' }),
      makeLine('2', { status: 'active', name: 'Alice' }),
      makeLine('3', { status: 'inactive', name: 'Bob' }),
    ]
    const sorted = sortEntities(entities, { status: 1, name: 1 })
    expect(sorted.map(e => e.$id)).toEqual(['2', '1', '3'])
  })

  it('supports mixed ascending/descending in multi-field', () => {
    const entities = [
      makeLine('1', { group: 'A', score: 10 }),
      makeLine('2', { group: 'A', score: 20 }),
      makeLine('3', { group: 'B', score: 5 }),
    ]
    // group ascending, score descending
    const sorted = sortEntities(entities, { group: 1, score: -1 })
    expect(sorted.map(e => e.$id)).toEqual(['2', '1', '3'])
  })
})

// =============================================================================
// Nested field sort (dot notation)
// =============================================================================

describe('sortEntities — nested field paths', () => {
  it('sorts by a nested field using dot notation', () => {
    const entities = [
      makeLine('1', { address: { city: 'NYC' } }),
      makeLine('2', { address: { city: 'LA' } }),
      makeLine('3', { address: { city: 'SF' } }),
    ]
    const sorted = sortEntities(entities, { 'address.city': 1 })
    expect(sorted.map(e => e.$id)).toEqual(['2', '1', '3'])
  })
})

// =============================================================================
// Edge cases
// =============================================================================

describe('sortEntities — edge cases', () => {
  it('returns a new array (does not mutate input)', () => {
    const entities = [
      makeLine('2', { name: 'Bob' }),
      makeLine('1', { name: 'Alice' }),
    ]
    const sorted = sortEntities(entities, { name: 1 })
    expect(sorted).not.toBe(entities)
    expect(entities[0].$id).toBe('2') // original unchanged
  })

  it('handles empty array', () => {
    const sorted = sortEntities([], { name: 1 })
    expect(sorted).toEqual([])
  })

  it('handles single element', () => {
    const entities = [makeLine('1', { name: 'Alice' })]
    const sorted = sortEntities(entities, { name: 1 })
    expect(sorted).toEqual(entities)
  })

  it('preserves order when all values are equal', () => {
    const entities = [
      makeLine('1', { name: 'Same' }),
      makeLine('2', { name: 'Same' }),
      makeLine('3', { name: 'Same' }),
    ]
    const sorted = sortEntities(entities, { name: 1 })
    expect(sorted.map(e => e.$id)).toEqual(['1', '2', '3'])
  })
})
