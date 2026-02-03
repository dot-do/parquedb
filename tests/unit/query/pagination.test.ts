/**
 * Tests for pagination utilities
 *
 * Verifies the shared pagination logic used by ParqueDB find operations.
 */

import { describe, it, expect } from 'vitest'
import { applyPagination, extractPaginationOptions } from '@/query/pagination'
import type { FindOptions } from '@/types'

describe('applyPagination', () => {
  // Sample data for testing
  const items = [
    { $id: '1', name: 'Alice', age: 30 },
    { $id: '2', name: 'Bob', age: 25 },
    { $id: '3', name: 'Charlie', age: 35 },
    { $id: '4', name: 'Diana', age: 28 },
    { $id: '5', name: 'Eve', age: 32 },
  ]

  describe('sorting', () => {
    it('should sort by field ascending', () => {
      const result = applyPagination([...items], { sort: { name: 1 } })
      expect(result.items.map(i => i.name)).toEqual([
        'Alice',
        'Bob',
        'Charlie',
        'Diana',
        'Eve',
      ])
    })

    it('should sort by field descending', () => {
      const result = applyPagination([...items], { sort: { name: -1 } })
      expect(result.items.map(i => i.name)).toEqual([
        'Eve',
        'Diana',
        'Charlie',
        'Bob',
        'Alice',
      ])
    })

    it('should sort by numeric field', () => {
      const result = applyPagination([...items], { sort: { age: 1 } })
      expect(result.items.map(i => i.age)).toEqual([25, 28, 30, 32, 35])
    })

    it('should sort using string direction "asc"', () => {
      const result = applyPagination([...items], { sort: { age: 'asc' } })
      expect(result.items.map(i => i.age)).toEqual([25, 28, 30, 32, 35])
    })

    it('should sort using string direction "desc"', () => {
      const result = applyPagination([...items], { sort: { age: 'desc' } })
      expect(result.items.map(i => i.age)).toEqual([35, 32, 30, 28, 25])
    })
  })

  describe('limit', () => {
    it('should limit results', () => {
      const result = applyPagination([...items], { limit: 3 })
      expect(result.items).toHaveLength(3)
      expect(result.hasMore).toBe(true)
      expect(result.total).toBe(5)
    })

    it('should not have hasMore when limit exceeds items', () => {
      const result = applyPagination([...items], { limit: 10 })
      expect(result.items).toHaveLength(5)
      expect(result.hasMore).toBe(false)
    })

    it('should set nextCursor when hasMore is true', () => {
      const result = applyPagination([...items], { limit: 2 })
      expect(result.nextCursor).toBe('2')
    })
  })

  describe('skip/offset', () => {
    it('should skip items', () => {
      const result = applyPagination([...items], { skip: 2 })
      expect(result.items).toHaveLength(3)
      expect(result.items[0].$id).toBe('3')
    })

    it('should combine skip and limit', () => {
      const result = applyPagination([...items], { skip: 1, limit: 2 })
      expect(result.items).toHaveLength(2)
      expect(result.items.map(i => i.$id)).toEqual(['2', '3'])
      expect(result.hasMore).toBe(true)
    })
  })

  describe('cursor-based pagination', () => {
    it('should start after cursor', () => {
      const result = applyPagination([...items], { cursor: '2' })
      expect(result.items).toHaveLength(3)
      expect(result.items[0].$id).toBe('3')
    })

    it('should return empty when cursor not found', () => {
      const result = applyPagination([...items], { cursor: 'nonexistent' })
      expect(result.items).toHaveLength(0)
    })

    it('should combine cursor and limit', () => {
      const result = applyPagination([...items], { cursor: '1', limit: 2 })
      expect(result.items).toHaveLength(2)
      expect(result.items.map(i => i.$id)).toEqual(['2', '3'])
      expect(result.hasMore).toBe(true)
    })
  })

  describe('total count', () => {
    it('should return total count before pagination', () => {
      const result = applyPagination([...items], { limit: 2 })
      expect(result.total).toBe(5)
    })
  })

  describe('empty input', () => {
    it('should handle empty array', () => {
      const result = applyPagination([], { limit: 10 })
      expect(result.items).toHaveLength(0)
      expect(result.hasMore).toBe(false)
      expect(result.total).toBe(0)
    })

    it('should handle no options', () => {
      const result = applyPagination([...items])
      expect(result.items).toHaveLength(5)
      expect(result.hasMore).toBe(false)
      expect(result.total).toBe(5)
    })
  })
})

describe('extractPaginationOptions', () => {
  it('should extract pagination fields from FindOptions', () => {
    const findOptions: FindOptions = {
      sort: { name: 1 },
      limit: 10,
      skip: 5,
      cursor: 'abc',
      // Other FindOptions fields that should be ignored
      filter: { status: 'active' },
      project: { name: 1 },
      includeDeleted: true,
    }

    const result = extractPaginationOptions(findOptions)
    expect(result).toEqual({
      sort: { name: 1 },
      limit: 10,
      skip: 5,
      cursor: 'abc',
    })
  })

  it('should handle undefined options', () => {
    const result = extractPaginationOptions(undefined)
    expect(result).toEqual({
      sort: undefined,
      limit: undefined,
      skip: undefined,
      cursor: undefined,
    })
  })

  it('should handle partial options', () => {
    const result = extractPaginationOptions({ limit: 20 })
    expect(result.limit).toBe(20)
    expect(result.sort).toBeUndefined()
  })
})
