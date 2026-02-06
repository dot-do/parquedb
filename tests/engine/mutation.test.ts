/**
 * Tests for src/engine/mutation.ts
 *
 * Extracted from engine.ts: applyUpdate() and extractData() are pure functions
 * that handle update operator application and system field stripping.
 */

import { describe, it, expect } from 'vitest'
import { applyUpdate, extractData } from '../../src/engine/mutation'
import type { DataLine } from '../../src/engine/types'

// =============================================================================
// extractData
// =============================================================================

describe('extractData', () => {
  it('returns only non-system fields', () => {
    const entity: DataLine = {
      $id: '001',
      $op: 'c',
      $v: 1,
      $ts: 1000,
      name: 'Alice',
      email: 'alice@example.com',
    }
    const data = extractData(entity)
    expect(data).toEqual({ name: 'Alice', email: 'alice@example.com' })
  })

  it('returns empty object for tombstone (system fields only)', () => {
    const entity: DataLine = {
      $id: '001',
      $op: 'd',
      $v: 2,
      $ts: 2000,
    }
    const data = extractData(entity)
    expect(data).toEqual({})
  })

  it('preserves nested objects in data fields', () => {
    const entity: DataLine = {
      $id: '001',
      $op: 'c',
      $v: 1,
      $ts: 1000,
      address: { city: 'NYC', zip: '10001' },
    }
    const data = extractData(entity)
    expect(data).toEqual({ address: { city: 'NYC', zip: '10001' } })
  })

  it('preserves null and false values', () => {
    const entity: DataLine = {
      $id: '001',
      $op: 'c',
      $v: 1,
      $ts: 1000,
      name: null,
      active: false,
      count: 0,
    }
    const data = extractData(entity)
    expect(data).toEqual({ name: null, active: false, count: 0 })
  })

  it('uses $data field when available and non-empty', () => {
    const entity: DataLine = {
      $id: '001',
      $op: 'c',
      $v: 1,
      $ts: 1000,
      $data: { name: 'Alice', email: 'alice@example.com' },
      name: 'Alice',
      email: 'alice@example.com',
    }
    const data = extractData(entity)
    expect(data).toEqual({ name: 'Alice', email: 'alice@example.com' })
  })

  it('falls back to flat fields when $data is empty', () => {
    const entity: DataLine = {
      $id: '001',
      $op: 'c',
      $v: 1,
      $ts: 1000,
      $data: {},
      name: 'Alice',
    }
    const data = extractData(entity)
    expect(data).toEqual({ name: 'Alice' })
  })
})

// =============================================================================
// applyUpdate — $set
// =============================================================================

describe('applyUpdate — $set', () => {
  it('sets new fields on the entity', () => {
    const entity: DataLine = { $id: '001', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' }
    const result = applyUpdate(entity, { $set: { email: 'alice@example.com' } })
    expect(result.name).toBe('Alice')
    expect((result as Record<string, unknown>).email).toBe('alice@example.com')
  })

  it('overwrites existing fields', () => {
    const entity: DataLine = { $id: '001', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' }
    const result = applyUpdate(entity, { $set: { name: 'Bob' } })
    expect(result.name).toBe('Bob')
  })

  it('does not modify system fields via $set', () => {
    const entity: DataLine = { $id: '001', $op: 'c', $v: 1, $ts: 1000 }
    const result = applyUpdate(entity, { $set: { $id: 'HACKED', $op: 'x', $v: 999, $ts: 0 } })
    expect(result.$id).toBe('001')
    expect(result.$op).toBe('c')
    expect(result.$v).toBe(1)
    expect(result.$ts).toBe(1000)
  })

  it('does not mutate the original entity', () => {
    const entity: DataLine = { $id: '001', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' }
    applyUpdate(entity, { $set: { name: 'Bob' } })
    expect(entity.name).toBe('Alice')
  })
})

// =============================================================================
// applyUpdate — $inc
// =============================================================================

describe('applyUpdate — $inc', () => {
  it('increments existing numeric fields', () => {
    const entity: DataLine = { $id: '001', $op: 'c', $v: 1, $ts: 1000, views: 10 }
    const result = applyUpdate(entity, { $inc: { views: 5 } })
    expect((result as Record<string, unknown>).views).toBe(15)
  })

  it('defaults missing fields to 0 before incrementing', () => {
    const entity: DataLine = { $id: '001', $op: 'c', $v: 1, $ts: 1000 }
    const result = applyUpdate(entity, { $inc: { count: 3 } })
    expect((result as Record<string, unknown>).count).toBe(3)
  })

  it('handles negative increments (decrement)', () => {
    const entity: DataLine = { $id: '001', $op: 'c', $v: 1, $ts: 1000, balance: 100 }
    const result = applyUpdate(entity, { $inc: { balance: -25 } })
    expect((result as Record<string, unknown>).balance).toBe(75)
  })

  it('treats non-numeric existing values as 0', () => {
    const entity: DataLine = { $id: '001', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' }
    const result = applyUpdate(entity, { $inc: { name: 1 } })
    expect((result as Record<string, unknown>).name).toBe(1)
  })

  it('does not modify system fields via $inc', () => {
    const entity: DataLine = { $id: '001', $op: 'c', $v: 1, $ts: 1000 }
    const result = applyUpdate(entity, { $inc: { $v: 100, $ts: 999 } })
    expect(result.$v).toBe(1)
    expect(result.$ts).toBe(1000)
  })
})

// =============================================================================
// applyUpdate — $unset
// =============================================================================

describe('applyUpdate — $unset', () => {
  it('removes fields from the entity', () => {
    const entity: DataLine = { $id: '001', $op: 'c', $v: 1, $ts: 1000, name: 'Alice', email: 'a@b.co' }
    const result = applyUpdate(entity, { $unset: { email: true } })
    expect((result as Record<string, unknown>).email).toBeUndefined()
    expect(result.name).toBe('Alice')
  })

  it('is a no-op for non-existent fields', () => {
    const entity: DataLine = { $id: '001', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' }
    const result = applyUpdate(entity, { $unset: { missing: true } })
    expect(result.name).toBe('Alice')
  })

  it('does not remove system fields via $unset', () => {
    const entity: DataLine = { $id: '001', $op: 'c', $v: 1, $ts: 1000 }
    const result = applyUpdate(entity, { $unset: { $id: true, $op: true, $v: true, $ts: true } })
    expect(result.$id).toBe('001')
    expect(result.$op).toBe('c')
    expect(result.$v).toBe(1)
    expect(result.$ts).toBe(1000)
  })
})

// =============================================================================
// applyUpdate — combined operators
// =============================================================================

describe('applyUpdate — combined operators', () => {
  it('applies $set, $inc, and $unset together', () => {
    const entity: DataLine = {
      $id: '001', $op: 'c', $v: 1, $ts: 1000,
      name: 'Alice', views: 10, temp: 'remove-me',
    }
    const result = applyUpdate(entity, {
      $set: { name: 'Bob' },
      $inc: { views: 1 },
      $unset: { temp: true },
    })
    expect(result.name).toBe('Bob')
    expect((result as Record<string, unknown>).views).toBe(11)
    expect((result as Record<string, unknown>).temp).toBeUndefined()
  })

  it('returns a clone when no operators are provided', () => {
    const entity: DataLine = {
      $id: '001', $op: 'c', $v: 1, $ts: 1000,
      $data: { name: 'Alice' }, name: 'Alice',
    }
    const result = applyUpdate(entity, {})
    expect(result).toEqual(entity)
    expect(result).not.toBe(entity) // different object reference
    expect(result.$data).not.toBe(entity.$data) // $data is also cloned
  })

  it('returns a clone with $data populated from flat fields for legacy entities', () => {
    const entity: DataLine = { $id: '001', $op: 'c', $v: 1, $ts: 1000, name: 'Alice' }
    const result = applyUpdate(entity, {})
    expect(result.$data).toEqual({ name: 'Alice' })
    expect(result).not.toBe(entity)
  })
})
