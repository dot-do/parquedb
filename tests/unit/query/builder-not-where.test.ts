/**
 * QueryBuilder notWhere() Method Tests
 *
 * Tests for the notWhere() method which provides a fluent API
 * for building queries with field-level $not operators.
 */

import { describe, it, expect } from 'vitest'
import { QueryBuilder } from '../../../src/query/builder'
import { matchesFilter } from '../../../src/query/filter'

describe('QueryBuilder.notWhere()', () => {
  describe('basic usage', () => {
    it('creates filter with field-level $not for comparison operators', () => {
      const builder = new QueryBuilder()
      const { filter } = builder.notWhere('score', 'gt', 100).build()

      expect(filter).toEqual({ score: { $not: { $gt: 100 } } })
    })

    it('creates filter with field-level $not for $eq', () => {
      const builder = new QueryBuilder()
      const { filter } = builder.notWhere('status', 'eq', 'inactive').build()

      expect(filter).toEqual({ status: { $not: { $eq: 'inactive' } } })
    })

    it('creates filter with field-level $not for $in', () => {
      const builder = new QueryBuilder()
      const { filter } = builder.notWhere('category', 'in', ['draft', 'archived']).build()

      expect(filter).toEqual({ category: { $not: { $in: ['draft', 'archived'] } } })
    })

    it('creates filter with field-level $not for $regex', () => {
      const builder = new QueryBuilder()
      const { filter } = builder.notWhere('name', 'regex', '^admin').build()

      expect(filter).toEqual({ name: { $not: { $regex: '^admin' } } })
    })

    it('creates filter with field-level $not for $startsWith', () => {
      const builder = new QueryBuilder()
      const { filter } = builder.notWhere('url', 'startsWith', 'http://').build()

      expect(filter).toEqual({ url: { $not: { $startsWith: 'http://' } } })
    })
  })

  describe('combining with where()', () => {
    it('combines notWhere with regular where conditions', () => {
      const builder = new QueryBuilder()
        .where('status', 'eq', 'active')
        .notWhere('score', 'lt', 50)

      const { filter } = builder.build()

      expect(filter).toEqual({
        status: { $eq: 'active' },
        score: { $not: { $lt: 50 } },
      })
    })

    it('combines multiple notWhere conditions', () => {
      const builder = new QueryBuilder()
        .notWhere('score', 'lt', 0)
        .notWhere('name', 'regex', '^test')

      const { filter } = builder.build()

      // When multiple fields, they are merged into a single filter
      expect(filter).toEqual({
        score: { $not: { $lt: 0 } },
        name: { $not: { $regex: '^test' } },
      })
    })

    it('combines where, andWhere, and notWhere', () => {
      const builder = new QueryBuilder()
        .where('type', 'eq', 'user')
        .andWhere('active', 'eq', true)
        .notWhere('role', 'in', ['banned', 'suspended'])

      const { filter } = builder.build()

      expect(filter).toEqual({
        type: { $eq: 'user' },
        active: { $eq: true },
        role: { $not: { $in: ['banned', 'suspended'] } },
      })
    })
  })

  describe('duplicate field handling', () => {
    it('uses $and when same field has multiple conditions including notWhere', () => {
      const builder = new QueryBuilder()
        .where('score', 'gte', 0)
        .notWhere('score', 'gt', 100)

      const { filter } = builder.build()

      // Multiple conditions on same field should use $and
      expect(filter).toEqual({
        $and: [
          { score: { $gte: 0 } },
          { score: { $not: { $gt: 100 } } },
        ],
      })
    })
  })

  describe('filter evaluation', () => {
    it('correctly filters data with notWhere', () => {
      const builder = new QueryBuilder()
        .where('status', 'eq', 'active')
        .notWhere('score', 'gt', 100)

      const { filter } = builder.build()

      const data = [
        { id: 1, status: 'active', score: 50 },
        { id: 2, status: 'active', score: 150 },
        { id: 3, status: 'inactive', score: 50 },
        { id: 4, status: 'active', score: 100 },
      ]

      const results = data.filter(d => matchesFilter(d, filter))
      expect(results.map(r => r.id)).toEqual([1, 4])
    })

    it('correctly filters with regex notWhere', () => {
      const builder = new QueryBuilder()
        .notWhere('email', 'endsWith', '@internal.com')

      const { filter } = builder.build()

      const data = [
        { name: 'Alice', email: 'alice@gmail.com' },
        { name: 'Bob', email: 'bob@internal.com' },
        { name: 'Charlie', email: 'charlie@company.com' },
      ]

      const results = data.filter(d => matchesFilter(d, filter))
      expect(results.map(r => r.name)).toEqual(['Alice', 'Charlie'])
    })
  })

  describe('operator alias support', () => {
    it('supports symbolic operators with notWhere', () => {
      const builder1 = new QueryBuilder().notWhere('score', '>', 100)
      const builder2 = new QueryBuilder().notWhere('score', 'gt', 100)

      expect(builder1.build().filter).toEqual(builder2.build().filter)
    })

    it('supports = operator alias', () => {
      const builder = new QueryBuilder().notWhere('status', '=', 'banned')
      const { filter } = builder.build()

      expect(filter).toEqual({ status: { $not: { $eq: 'banned' } } })
    })

    it('supports != operator alias', () => {
      const builder = new QueryBuilder().notWhere('status', '!=', 'active')
      const { filter } = builder.build()

      expect(filter).toEqual({ status: { $not: { $ne: 'active' } } })
    })
  })

  describe('validation', () => {
    it('throws error for invalid operator', () => {
      const builder = new QueryBuilder()
      // @ts-expect-error - testing invalid operator
      expect(() => builder.notWhere('field', 'invalid', 'value')).toThrow('Invalid operator: invalid')
    })
  })

  describe('clone()', () => {
    it('preserves notWhere conditions when cloning', () => {
      const builder = new QueryBuilder()
        .where('type', 'eq', 'post')
        .notWhere('status', 'eq', 'deleted')

      const cloned = builder.clone()
      cloned.where('featured', 'eq', true)

      // Original should not have featured condition
      expect(builder.build().filter).toEqual({
        type: { $eq: 'post' },
        status: { $not: { $eq: 'deleted' } },
      })

      // Cloned should have all conditions
      expect(cloned.build().filter).toEqual({
        type: { $eq: 'post' },
        status: { $not: { $eq: 'deleted' } },
        featured: { $eq: true },
      })
    })
  })

  describe('chaining', () => {
    it('supports method chaining', () => {
      const { filter, options } = new QueryBuilder()
        .where('type', 'eq', 'product')
        .notWhere('price', 'gte', 1000)
        .notWhere('category', 'in', ['clearance', 'discontinued'])
        .orderBy('price', 'asc')
        .limit(10)
        .build()

      expect(filter).toEqual({
        type: { $eq: 'product' },
        price: { $not: { $gte: 1000 } },
        category: { $not: { $in: ['clearance', 'discontinued'] } },
      })
      expect(options.sort).toEqual({ price: 'asc' })
      expect(options.limit).toBe(10)
    })
  })
})
