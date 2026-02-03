/**
 * Tests for Simple Query Execution
 *
 * Tests the shared query execution logic used by MV refresh and query executor.
 */

import { describe, test, expect } from 'vitest'
import { executeSimpleQuery, applyProjection, type SimpleQuery } from '../../../src/query/simple-query'

// =============================================================================
// Test Data
// =============================================================================

interface TestUser {
  $id: string
  name: string
  age: number
  status: string
  email?: string
}

const testUsers: TestUser[] = [
  { $id: '1', name: 'Alice', age: 30, status: 'active', email: 'alice@test.com' },
  { $id: '2', name: 'Bob', age: 25, status: 'inactive' },
  { $id: '3', name: 'Charlie', age: 35, status: 'active', email: 'charlie@test.com' },
  { $id: '4', name: 'Diana', age: 28, status: 'pending' },
  { $id: '5', name: 'Eve', age: 22, status: 'active' },
]

// =============================================================================
// applyProjection Tests
// =============================================================================

describe('applyProjection', () => {
  describe('inclusion mode', () => {
    test('includes only specified fields', () => {
      const result = applyProjection(testUsers, { $id: 1, name: 1 })

      expect(result).toHaveLength(5)
      expect(result[0]).toEqual({ $id: '1', name: 'Alice' })
      expect(result[0]).not.toHaveProperty('age')
      expect(result[0]).not.toHaveProperty('status')
    })

    test('handles boolean true for inclusion', () => {
      const result = applyProjection(testUsers, { $id: true, name: true })

      expect(result[0]).toEqual({ $id: '1', name: 'Alice' })
    })

    test('handles missing fields gracefully', () => {
      const result = applyProjection(testUsers, { $id: 1, nonexistent: 1 })

      expect(result[0]).toEqual({ $id: '1' })
      expect(result[0]).not.toHaveProperty('nonexistent')
    })
  })

  describe('exclusion mode', () => {
    test('excludes specified fields', () => {
      const result = applyProjection(testUsers, { email: 0 })

      expect(result[0]).toEqual({ $id: '1', name: 'Alice', age: 30, status: 'active' })
      expect(result[0]).not.toHaveProperty('email')
    })

    test('handles boolean false for exclusion', () => {
      const result = applyProjection(testUsers, { email: false, age: false })

      expect(result[0]).toEqual({ $id: '1', name: 'Alice', status: 'active' })
    })
  })

  describe('edge cases', () => {
    test('empty projection returns original data', () => {
      const result = applyProjection(testUsers, {})

      expect(result).toEqual(testUsers)
    })

    test('preserves array length', () => {
      const result = applyProjection(testUsers, { $id: 1 })

      expect(result).toHaveLength(testUsers.length)
    })
  })
})

// =============================================================================
// executeSimpleQuery Tests
// =============================================================================

describe('executeSimpleQuery', () => {
  describe('filter', () => {
    test('filters by equality', () => {
      const result = executeSimpleQuery(testUsers, {
        filter: { status: 'active' },
      })

      expect(result).toHaveLength(3)
      expect(result.every(u => u.status === 'active')).toBe(true)
    })

    test('filters by comparison operator', () => {
      const result = executeSimpleQuery(testUsers, {
        filter: { age: { $gte: 30 } },
      })

      expect(result).toHaveLength(2)
      expect(result.map(u => u.name)).toEqual(['Alice', 'Charlie'])
    })

    test('filters by $in operator', () => {
      const result = executeSimpleQuery(testUsers, {
        filter: { status: { $in: ['active', 'pending'] } },
      })

      expect(result).toHaveLength(4)
    })

    test('empty filter returns all data', () => {
      const result = executeSimpleQuery(testUsers, {
        filter: {},
      })

      expect(result).toHaveLength(5)
    })
  })

  describe('projection', () => {
    test('applies projection to filtered data', () => {
      const result = executeSimpleQuery(testUsers, {
        filter: { status: 'active' },
        project: { $id: 1, name: 1 },
      })

      expect(result).toHaveLength(3)
      expect(result[0]).toEqual({ $id: '1', name: 'Alice' })
    })
  })

  describe('sort', () => {
    test('sorts by single field ascending', () => {
      const result = executeSimpleQuery(testUsers, {
        sort: { age: 1 },
      })

      expect(result.map(u => u.age)).toEqual([22, 25, 28, 30, 35])
    })

    test('sorts by single field descending', () => {
      const result = executeSimpleQuery(testUsers, {
        sort: { age: -1 },
      })

      expect(result.map(u => u.age)).toEqual([35, 30, 28, 25, 22])
    })

    test('sorts by multiple fields', () => {
      const data = [
        { $id: '1', status: 'active', name: 'Charlie' },
        { $id: '2', status: 'active', name: 'Alice' },
        { $id: '3', status: 'inactive', name: 'Bob' },
      ]

      const result = executeSimpleQuery(data, {
        sort: { status: 1, name: 1 },
      })

      expect(result.map(u => u.name)).toEqual(['Alice', 'Charlie', 'Bob'])
    })

    test('does not mutate original data', () => {
      const original = [...testUsers]
      executeSimpleQuery(testUsers, {
        sort: { age: -1 },
      })

      expect(testUsers).toEqual(original)
    })
  })

  describe('combined operations', () => {
    test('applies filter, project, and sort in correct order', () => {
      const result = executeSimpleQuery(testUsers, {
        filter: { status: 'active' },
        project: { $id: 1, name: 1, age: 1 },
        sort: { age: -1 },
      })

      expect(result).toHaveLength(3)
      expect(result).toEqual([
        { $id: '3', name: 'Charlie', age: 35 },
        { $id: '1', name: 'Alice', age: 30 },
        { $id: '5', name: 'Eve', age: 22 },
      ])
    })

    test('empty query returns original data unchanged', () => {
      const result = executeSimpleQuery(testUsers, {})

      expect(result).toEqual(testUsers)
    })
  })

  describe('null/undefined handling', () => {
    test('handles null values in filter', () => {
      const data = [
        { $id: '1', name: 'Alice', email: null },
        { $id: '2', name: 'Bob', email: 'bob@test.com' },
        { $id: '3', name: 'Charlie' }, // email is undefined
      ]

      const result = executeSimpleQuery(data, {
        filter: { email: null },
      })

      // Both null and undefined should match
      expect(result).toHaveLength(2)
      expect(result.map(u => u.name)).toEqual(['Alice', 'Charlie'])
    })

    test('handles null values in sort', () => {
      const data = [
        { $id: '1', name: 'Alice', score: 100 },
        { $id: '2', name: 'Bob', score: null },
        { $id: '3', name: 'Charlie', score: 50 },
        { $id: '4', name: 'Diana' }, // score is undefined
      ]

      const result = executeSimpleQuery(data, {
        sort: { score: 1 },
      })

      // Non-null values should be sorted first, nulls last
      expect(result.map(u => u.score)).toEqual([50, 100, null, undefined])
    })
  })
})

// =============================================================================
// Type Safety Tests
// =============================================================================

describe('type safety', () => {
  test('preserves record type', () => {
    const result = executeSimpleQuery(testUsers, {
      filter: { status: 'active' },
    })

    // TypeScript should infer correct type
    const first = result[0]
    if (first) {
      expect(typeof first.$id).toBe('string')
      expect(typeof first.name).toBe('string')
      expect(typeof first.age).toBe('number')
    }
  })

  test('works with generic records', () => {
    const data: Record<string, unknown>[] = [
      { id: 1, value: 'a' },
      { id: 2, value: 'b' },
    ]

    const result = executeSimpleQuery(data, {
      filter: { id: 1 },
    })

    expect(result).toHaveLength(1)
    expect(result[0]).toEqual({ id: 1, value: 'a' })
  })
})
