import { describe, it, expect } from 'vitest'
import { toNumber, DATA_SYSTEM_FIELDS } from '@/engine/utils'

/**
 * Shared Engine Utils Test Suite
 *
 * Tests the utility helpers shared across engine modules:
 * - parquet-adapter.ts (Parquet storage adapter)
 * - do-compactor.ts (Durable Object compactor)
 * - do-read-path.ts (DO merge-on-read path)
 * - parquet-encoders.ts (Parquet encoding)
 * - engine.ts (core engine)
 */

describe('toNumber', () => {
  it('returns number as-is', () => {
    expect(toNumber(42)).toBe(42)
    expect(toNumber(0)).toBe(0)
    expect(toNumber(-1.5)).toBe(-1.5)
    expect(toNumber(Infinity)).toBe(Infinity)
    expect(toNumber(NaN)).toBeNaN()
  })

  it('converts BigInt to number', () => {
    expect(toNumber(BigInt(100))).toBe(100)
    expect(toNumber(BigInt(0))).toBe(0)
    expect(toNumber(BigInt(-42))).toBe(-42)
    expect(toNumber(BigInt(Number.MAX_SAFE_INTEGER))).toBe(Number.MAX_SAFE_INTEGER)
  })

  it('returns 0 for string input', () => {
    expect(toNumber('hello')).toBe(0)
    expect(toNumber('42')).toBe(0)
    expect(toNumber('')).toBe(0)
  })

  it('returns 0 for null/undefined', () => {
    expect(toNumber(null)).toBe(0)
    expect(toNumber(undefined)).toBe(0)
  })

  it('returns 0 for other types', () => {
    expect(toNumber(true)).toBe(0)
    expect(toNumber(false)).toBe(0)
    expect(toNumber({})).toBe(0)
    expect(toNumber([])).toBe(0)
    expect(toNumber(Symbol('x'))).toBe(0)
  })
})

describe('DATA_SYSTEM_FIELDS', () => {
  it('contains exactly the five system fields', () => {
    expect(DATA_SYSTEM_FIELDS.has('$id')).toBe(true)
    expect(DATA_SYSTEM_FIELDS.has('$op')).toBe(true)
    expect(DATA_SYSTEM_FIELDS.has('$v')).toBe(true)
    expect(DATA_SYSTEM_FIELDS.has('$ts')).toBe(true)
    expect(DATA_SYSTEM_FIELDS.has('$data')).toBe(true)
    expect(DATA_SYSTEM_FIELDS.size).toBe(5)
  })

  it('does not contain user data fields', () => {
    expect(DATA_SYSTEM_FIELDS.has('name')).toBe(false)
    expect(DATA_SYSTEM_FIELDS.has('email')).toBe(false)
    expect(DATA_SYSTEM_FIELDS.has('$type')).toBe(false)
  })
})
