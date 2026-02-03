/**
 * QueryBuilder Tests
 *
 * Tests for the fluent query builder pattern for ParqueDB collections.
 *
 * The QueryBuilder provides a chainable API for constructing queries:
 * - where/andWhere/orWhere for filter conditions
 * - orderBy for sorting
 * - limit/offset for pagination
 * - select for field projection
 * - build() to get the filter and options
 * - find() to execute the query
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { QueryBuilder } from '../../../src/query/builder'
import { Collection, clearGlobalStorage } from '../../../src/collection'
import type { Filter } from '../../../src/types/filter'
import type { FindOptions } from '../../../src/types/options'

describe('QueryBuilder', () => {
  // =============================================================================
  // Basic Construction
  // =============================================================================

  describe('construction', () => {
    it('creates an empty QueryBuilder', () => {
      const builder = new QueryBuilder()
      const { filter, options } = builder.build()
      expect(filter).toEqual({})
      expect(options).toEqual({})
    })

    it('can be created with an initial collection reference', () => {
      const collection = new Collection('posts')
      const builder = new QueryBuilder(collection)
      expect(builder).toBeInstanceOf(QueryBuilder)
    })
  })

  // =============================================================================
  // where() - Basic Conditions
  // =============================================================================

  describe('where()', () => {
    describe('comparison operators', () => {
      it('builds $eq filter with "eq" operator', () => {
        const builder = new QueryBuilder()
          .where('status', 'eq', 'published')
        const { filter } = builder.build()
        expect(filter).toEqual({ status: { $eq: 'published' } })
      })

      it('builds $eq filter with "=" operator', () => {
        const builder = new QueryBuilder()
          .where('status', '=', 'published')
        const { filter } = builder.build()
        expect(filter).toEqual({ status: { $eq: 'published' } })
      })

      it('builds $ne filter with "ne" operator', () => {
        const builder = new QueryBuilder()
          .where('status', 'ne', 'deleted')
        const { filter } = builder.build()
        expect(filter).toEqual({ status: { $ne: 'deleted' } })
      })

      it('builds $ne filter with "!=" operator', () => {
        const builder = new QueryBuilder()
          .where('status', '!=', 'deleted')
        const { filter } = builder.build()
        expect(filter).toEqual({ status: { $ne: 'deleted' } })
      })

      it('builds $gt filter with "gt" operator', () => {
        const builder = new QueryBuilder()
          .where('score', 'gt', 100)
        const { filter } = builder.build()
        expect(filter).toEqual({ score: { $gt: 100 } })
      })

      it('builds $gt filter with ">" operator', () => {
        const builder = new QueryBuilder()
          .where('score', '>', 100)
        const { filter } = builder.build()
        expect(filter).toEqual({ score: { $gt: 100 } })
      })

      it('builds $gte filter with "gte" operator', () => {
        const builder = new QueryBuilder()
          .where('score', 'gte', 100)
        const { filter } = builder.build()
        expect(filter).toEqual({ score: { $gte: 100 } })
      })

      it('builds $gte filter with ">=" operator', () => {
        const builder = new QueryBuilder()
          .where('score', '>=', 100)
        const { filter } = builder.build()
        expect(filter).toEqual({ score: { $gte: 100 } })
      })

      it('builds $lt filter with "lt" operator', () => {
        const builder = new QueryBuilder()
          .where('score', 'lt', 50)
        const { filter } = builder.build()
        expect(filter).toEqual({ score: { $lt: 50 } })
      })

      it('builds $lt filter with "<" operator', () => {
        const builder = new QueryBuilder()
          .where('score', '<', 50)
        const { filter } = builder.build()
        expect(filter).toEqual({ score: { $lt: 50 } })
      })

      it('builds $lte filter with "lte" operator', () => {
        const builder = new QueryBuilder()
          .where('score', 'lte', 50)
        const { filter } = builder.build()
        expect(filter).toEqual({ score: { $lte: 50 } })
      })

      it('builds $lte filter with "<=" operator', () => {
        const builder = new QueryBuilder()
          .where('score', '<=', 50)
        const { filter } = builder.build()
        expect(filter).toEqual({ score: { $lte: 50 } })
      })

      it('builds $in filter with "in" operator', () => {
        const builder = new QueryBuilder()
          .where('status', 'in', ['draft', 'published'])
        const { filter } = builder.build()
        expect(filter).toEqual({ status: { $in: ['draft', 'published'] } })
      })

      it('builds $nin filter with "nin" operator', () => {
        const builder = new QueryBuilder()
          .where('status', 'nin', ['deleted', 'archived'])
        const { filter } = builder.build()
        expect(filter).toEqual({ status: { $nin: ['deleted', 'archived'] } })
      })
    })

    describe('string operators', () => {
      it('builds $regex filter with "regex" operator', () => {
        const builder = new QueryBuilder()
          .where('title', 'regex', '^Hello')
        const { filter } = builder.build()
        expect(filter).toEqual({ title: { $regex: '^Hello' } })
      })

      it('builds $startsWith filter', () => {
        const builder = new QueryBuilder()
          .where('title', 'startsWith', 'Hello')
        const { filter } = builder.build()
        expect(filter).toEqual({ title: { $startsWith: 'Hello' } })
      })

      it('builds $endsWith filter', () => {
        const builder = new QueryBuilder()
          .where('title', 'endsWith', 'World')
        const { filter } = builder.build()
        expect(filter).toEqual({ title: { $endsWith: 'World' } })
      })

      it('builds $contains filter', () => {
        const builder = new QueryBuilder()
          .where('content', 'contains', 'parquet')
        const { filter } = builder.build()
        expect(filter).toEqual({ content: { $contains: 'parquet' } })
      })
    })

    describe('existence operators', () => {
      it('builds $exists filter with "exists" operator', () => {
        const builder = new QueryBuilder()
          .where('deletedAt', 'exists', false)
        const { filter } = builder.build()
        expect(filter).toEqual({ deletedAt: { $exists: false } })
      })
    })

    describe('nested field access', () => {
      it('handles dot notation for nested fields', () => {
        const builder = new QueryBuilder()
          .where('user.profile.age', 'gte', 18)
        const { filter } = builder.build()
        expect(filter).toEqual({ 'user.profile.age': { $gte: 18 } })
      })
    })

    describe('date values', () => {
      it('handles Date objects', () => {
        const date = new Date('2024-06-15')
        const builder = new QueryBuilder()
          .where('createdAt', 'gte', date)
        const { filter } = builder.build()
        expect(filter).toEqual({ createdAt: { $gte: date } })
      })
    })
  })

  // =============================================================================
  // andWhere() - Chaining Conditions with AND
  // =============================================================================

  describe('andWhere()', () => {
    it('combines multiple conditions with implicit AND', () => {
      const builder = new QueryBuilder()
        .where('status', 'eq', 'published')
        .andWhere('featured', 'eq', true)
      const { filter } = builder.build()
      expect(filter).toEqual({
        status: { $eq: 'published' },
        featured: { $eq: true }
      })
    })

    it('chains multiple andWhere calls', () => {
      const builder = new QueryBuilder()
        .where('status', 'eq', 'published')
        .andWhere('score', 'gte', 80)
        .andWhere('views', 'gt', 1000)
      const { filter } = builder.build()
      expect(filter).toEqual({
        status: { $eq: 'published' },
        score: { $gte: 80 },
        views: { $gt: 1000 }
      })
    })

    it('handles same field with different operators', () => {
      const builder = new QueryBuilder()
        .where('score', 'gte', 50)
        .andWhere('score', 'lte', 100)
      const { filter } = builder.build()
      // Should use $and when same field has multiple conditions
      expect(filter).toEqual({
        $and: [
          { score: { $gte: 50 } },
          { score: { $lte: 100 } }
        ]
      })
    })
  })

  // =============================================================================
  // orWhere() - Chaining Conditions with OR
  // =============================================================================

  describe('orWhere()', () => {
    it('combines conditions with OR', () => {
      const builder = new QueryBuilder()
        .where('status', 'eq', 'published')
        .orWhere('featured', 'eq', true)
      const { filter } = builder.build()
      expect(filter).toEqual({
        $or: [
          { status: { $eq: 'published' } },
          { featured: { $eq: true } }
        ]
      })
    })

    it('chains multiple orWhere calls', () => {
      const builder = new QueryBuilder()
        .where('status', 'eq', 'published')
        .orWhere('status', 'eq', 'featured')
        .orWhere('status', 'eq', 'pinned')
      const { filter } = builder.build()
      expect(filter).toEqual({
        $or: [
          { status: { $eq: 'published' } },
          { status: { $eq: 'featured' } },
          { status: { $eq: 'pinned' } }
        ]
      })
    })
  })

  // =============================================================================
  // Mixed AND/OR Conditions
  // =============================================================================

  describe('mixed AND/OR conditions', () => {
    it('handles AND then OR', () => {
      const builder = new QueryBuilder()
        .where('type', 'eq', 'article')
        .andWhere('published', 'eq', true)
        .orWhere('featured', 'eq', true)
      const { filter } = builder.build()
      // (type=article AND published=true) OR (featured=true)
      expect(filter).toEqual({
        $or: [
          {
            type: { $eq: 'article' },
            published: { $eq: true }
          },
          { featured: { $eq: true } }
        ]
      })
    })
  })

  // =============================================================================
  // orderBy() - Sorting
  // =============================================================================

  describe('orderBy()', () => {
    it('adds ascending sort by default', () => {
      const builder = new QueryBuilder()
        .orderBy('createdAt')
      const { options } = builder.build()
      expect(options.sort).toEqual({ createdAt: 'asc' })
    })

    it('adds explicit ascending sort', () => {
      const builder = new QueryBuilder()
        .orderBy('createdAt', 'asc')
      const { options } = builder.build()
      expect(options.sort).toEqual({ createdAt: 'asc' })
    })

    it('adds descending sort', () => {
      const builder = new QueryBuilder()
        .orderBy('createdAt', 'desc')
      const { options } = builder.build()
      expect(options.sort).toEqual({ createdAt: 'desc' })
    })

    it('chains multiple orderBy calls', () => {
      const builder = new QueryBuilder()
        .orderBy('status', 'asc')
        .orderBy('createdAt', 'desc')
      const { options } = builder.build()
      expect(options.sort).toEqual({
        status: 'asc',
        createdAt: 'desc'
      })
    })

    it('handles nested field sorting', () => {
      const builder = new QueryBuilder()
        .orderBy('author.name', 'asc')
      const { options } = builder.build()
      expect(options.sort).toEqual({ 'author.name': 'asc' })
    })
  })

  // =============================================================================
  // limit() - Limiting Results
  // =============================================================================

  describe('limit()', () => {
    it('sets the limit option', () => {
      const builder = new QueryBuilder()
        .limit(20)
      const { options } = builder.build()
      expect(options.limit).toBe(20)
    })

    it('overrides previous limit', () => {
      const builder = new QueryBuilder()
        .limit(20)
        .limit(10)
      const { options } = builder.build()
      expect(options.limit).toBe(10)
    })

    it('handles limit of 0', () => {
      const builder = new QueryBuilder()
        .limit(0)
      const { options } = builder.build()
      expect(options.limit).toBe(0)
    })
  })

  // =============================================================================
  // offset() - Skipping Results
  // =============================================================================

  describe('offset()', () => {
    it('sets the skip option', () => {
      const builder = new QueryBuilder()
        .offset(10)
      const { options } = builder.build()
      expect(options.skip).toBe(10)
    })

    it('overrides previous offset', () => {
      const builder = new QueryBuilder()
        .offset(10)
        .offset(20)
      const { options } = builder.build()
      expect(options.skip).toBe(20)
    })

    it('handles offset of 0', () => {
      const builder = new QueryBuilder()
        .offset(0)
      const { options } = builder.build()
      expect(options.skip).toBe(0)
    })
  })

  // =============================================================================
  // skip() - Alias for offset
  // =============================================================================

  describe('skip()', () => {
    it('is an alias for offset()', () => {
      const builder = new QueryBuilder()
        .skip(15)
      const { options } = builder.build()
      expect(options.skip).toBe(15)
    })
  })

  // =============================================================================
  // select() - Field Projection
  // =============================================================================

  describe('select()', () => {
    it('sets the project option with field array', () => {
      const builder = new QueryBuilder()
        .select(['title', 'content', 'author'])
      const { options } = builder.build()
      expect(options.project).toEqual({
        title: 1,
        content: 1,
        author: 1
      })
    })

    it('handles empty field array', () => {
      const builder = new QueryBuilder()
        .select([])
      const { options } = builder.build()
      expect(options.project).toEqual({})
    })

    it('handles single field', () => {
      const builder = new QueryBuilder()
        .select(['title'])
      const { options } = builder.build()
      expect(options.project).toEqual({ title: 1 })
    })

    it('handles nested field selection', () => {
      const builder = new QueryBuilder()
        .select(['title', 'author.name', 'author.email'])
      const { options } = builder.build()
      expect(options.project).toEqual({
        title: 1,
        'author.name': 1,
        'author.email': 1
      })
    })
  })

  // =============================================================================
  // build() - Generating Filter and Options
  // =============================================================================

  describe('build()', () => {
    it('returns filter and options object', () => {
      const builder = new QueryBuilder()
        .where('status', 'eq', 'published')
        .orderBy('createdAt', 'desc')
        .limit(10)
        .offset(5)
        .select(['title', 'content'])

      const { filter, options } = builder.build()

      expect(filter).toEqual({ status: { $eq: 'published' } })
      expect(options).toEqual({
        sort: { createdAt: 'desc' },
        limit: 10,
        skip: 5,
        project: { title: 1, content: 1 }
      })
    })

    it('returns empty objects for unset values', () => {
      const builder = new QueryBuilder()
      const { filter, options } = builder.build()
      expect(filter).toEqual({})
      expect(options).toEqual({})
    })

    it('can be called multiple times with same result', () => {
      const builder = new QueryBuilder()
        .where('status', 'eq', 'published')

      const result1 = builder.build()
      const result2 = builder.build()

      expect(result1).toEqual(result2)
    })
  })

  // =============================================================================
  // find() - Query Execution
  // =============================================================================

  describe('find()', () => {
    beforeEach(() => {
      clearGlobalStorage()
    })

    it('executes the query against the collection', async () => {
      const collection = new Collection<{ title: string; status: string }>('posts')

      // Create test data
      await collection.create({ $type: 'Post', name: 'Post 1', title: 'First', status: 'published' })
      await collection.create({ $type: 'Post', name: 'Post 2', title: 'Second', status: 'draft' })
      await collection.create({ $type: 'Post', name: 'Post 3', title: 'Third', status: 'published' })

      const builder = new QueryBuilder(collection)
        .where('status', 'eq', 'published')

      const results = await builder.find()

      expect(results).toHaveLength(2)
      expect(results.every(r => (r as any).status === 'published')).toBe(true)
    })

    it('throws error if no collection is set', async () => {
      const builder = new QueryBuilder()
        .where('status', 'eq', 'published')

      await expect(builder.find()).rejects.toThrow('No collection set')
    })

    it('applies all query options', async () => {
      const collection = new Collection<{ title: string; score: number }>('posts')

      // Create test data
      await collection.create({ $type: 'Post', name: 'A', title: 'A', score: 100 })
      await collection.create({ $type: 'Post', name: 'B', title: 'B', score: 80 })
      await collection.create({ $type: 'Post', name: 'C', title: 'C', score: 90 })
      await collection.create({ $type: 'Post', name: 'D', title: 'D', score: 70 })
      await collection.create({ $type: 'Post', name: 'E', title: 'E', score: 60 })

      const builder = new QueryBuilder(collection)
        .where('score', 'gte', 70)
        .orderBy('score', 'desc')
        .limit(3)

      const results = await builder.find()

      expect(results).toHaveLength(3)
      expect(results.map(r => (r as any).title)).toEqual(['A', 'C', 'B'])
    })
  })

  // =============================================================================
  // findOne() - Single Result Query
  // =============================================================================

  describe('findOne()', () => {
    beforeEach(() => {
      clearGlobalStorage()
    })

    it('returns single matching result', async () => {
      const collection = new Collection<{ title: string; status: string }>('posts')

      await collection.create({ $type: 'Post', name: 'Post 1', title: 'First', status: 'published' })
      await collection.create({ $type: 'Post', name: 'Post 2', title: 'Second', status: 'draft' })

      const builder = new QueryBuilder(collection)
        .where('status', 'eq', 'published')

      const result = await builder.findOne()

      expect(result).not.toBeNull()
      expect((result as any).status).toBe('published')
    })

    it('returns null when no match', async () => {
      const collection = new Collection<{ title: string; status: string }>('posts')

      await collection.create({ $type: 'Post', name: 'Post 1', title: 'First', status: 'draft' })

      const builder = new QueryBuilder(collection)
        .where('status', 'eq', 'published')

      const result = await builder.findOne()

      expect(result).toBeNull()
    })
  })

  // =============================================================================
  // count() - Counting Results
  // =============================================================================

  describe('count()', () => {
    beforeEach(() => {
      clearGlobalStorage()
    })

    it('returns count of matching documents', async () => {
      const collection = new Collection<{ status: string }>('posts')

      await collection.create({ $type: 'Post', name: 'Post 1', status: 'published' })
      await collection.create({ $type: 'Post', name: 'Post 2', status: 'draft' })
      await collection.create({ $type: 'Post', name: 'Post 3', status: 'published' })

      const builder = new QueryBuilder(collection)
        .where('status', 'eq', 'published')

      const count = await builder.count()

      expect(count).toBe(2)
    })
  })

  // =============================================================================
  // Complex Query Scenarios
  // =============================================================================

  describe('complex query scenarios', () => {
    beforeEach(() => {
      clearGlobalStorage()
    })

    it('builds a complex real-world query', async () => {
      const collection = new Collection<{
        title: string
        status: string
        score: number
        category: string
      }>('articles')

      await collection.create({ $type: 'Article', name: 'A1', title: 'AI in 2024', status: 'published', score: 95, category: 'tech' })
      await collection.create({ $type: 'Article', name: 'A2', title: 'Cloud Computing', status: 'published', score: 85, category: 'tech' })
      await collection.create({ $type: 'Article', name: 'A3', title: 'Draft Article', status: 'draft', score: 75, category: 'tech' })
      await collection.create({ $type: 'Article', name: 'A4', title: 'Old Tech', status: 'published', score: 60, category: 'tech' })
      await collection.create({ $type: 'Article', name: 'A5', title: 'Food Blog', status: 'published', score: 90, category: 'lifestyle' })

      const builder = new QueryBuilder(collection)
        .where('status', 'eq', 'published')
        .andWhere('category', 'eq', 'tech')
        .andWhere('score', 'gte', 80)
        .orderBy('score', 'desc')
        .limit(10)
        .select(['title', 'score'])

      const results = await builder.find()

      expect(results).toHaveLength(2)
      expect(results.map(r => (r as any).title)).toEqual(['AI in 2024', 'Cloud Computing'])
    })

    it('handles pagination with offset and limit', async () => {
      const collection = new Collection<{ title: string; order: number }>('items')

      for (let i = 1; i <= 10; i++) {
        await collection.create({ $type: 'Item', name: `Item ${i}`, title: `Item ${i}`, order: i })
      }

      const page2 = await new QueryBuilder(collection)
        .orderBy('order', 'asc')
        .offset(3)
        .limit(3)
        .find()

      expect(page2).toHaveLength(3)
      expect(page2.map(r => (r as any).order)).toEqual([4, 5, 6])
    })
  })

  // =============================================================================
  // Method Chaining Returns this
  // =============================================================================

  describe('method chaining', () => {
    it('all methods return the builder instance', () => {
      const builder = new QueryBuilder()

      expect(builder.where('a', 'eq', 1)).toBe(builder)
      expect(builder.andWhere('b', 'eq', 2)).toBe(builder)
      expect(builder.orWhere('c', 'eq', 3)).toBe(builder)
      expect(builder.orderBy('d')).toBe(builder)
      expect(builder.limit(10)).toBe(builder)
      expect(builder.offset(5)).toBe(builder)
      expect(builder.skip(5)).toBe(builder)
      expect(builder.select(['a'])).toBe(builder)
    })
  })

  // =============================================================================
  // Clone / Copy Builder
  // =============================================================================

  describe('clone()', () => {
    it('creates an independent copy of the builder', () => {
      const original = new QueryBuilder()
        .where('status', 'eq', 'published')
        .limit(10)

      const cloned = original.clone()
        .andWhere('featured', 'eq', true)
        .limit(5)

      const originalResult = original.build()
      const clonedResult = cloned.build()

      expect(originalResult.filter).toEqual({ status: { $eq: 'published' } })
      expect(originalResult.options.limit).toBe(10)

      expect(clonedResult.filter).toEqual({
        status: { $eq: 'published' },
        featured: { $eq: true }
      })
      expect(clonedResult.options.limit).toBe(5)
    })
  })

  // =============================================================================
  // Error Handling
  // =============================================================================

  describe('error handling', () => {
    it('throws for invalid operator', () => {
      const builder = new QueryBuilder()
      expect(() => builder.where('field', 'invalid' as any, 'value')).toThrow('Invalid operator: invalid')
    })

    it('throws for negative limit', () => {
      const builder = new QueryBuilder()
      expect(() => builder.limit(-1)).toThrow('Limit cannot be negative')
    })

    it('throws for negative offset', () => {
      const builder = new QueryBuilder()
      expect(() => builder.offset(-1)).toThrow('Offset cannot be negative')
    })
  })

  // =============================================================================
  // Collection.builder() Integration
  // =============================================================================

  describe('Collection.builder() integration', () => {
    beforeEach(() => {
      clearGlobalStorage()
    })

    it('returns a QueryBuilder bound to the collection', async () => {
      const collection = new Collection<{ title: string; status: string }>('posts')

      await collection.create({ $type: 'Post', name: 'Post 1', title: 'Hello', status: 'published' })
      await collection.create({ $type: 'Post', name: 'Post 2', title: 'World', status: 'draft' })

      const results = await collection.builder()
        .where('status', 'eq', 'published')
        .find()

      expect(results).toHaveLength(1)
      expect((results[0] as any).title).toBe('Hello')
    })

    it('works with full query chain', async () => {
      const collection = new Collection<{ title: string; score: number }>('articles')

      await collection.create({ $type: 'Article', name: 'A', title: 'A', score: 90 })
      await collection.create({ $type: 'Article', name: 'B', title: 'B', score: 85 })
      await collection.create({ $type: 'Article', name: 'C', title: 'C', score: 80 })
      await collection.create({ $type: 'Article', name: 'D', title: 'D', score: 75 })
      await collection.create({ $type: 'Article', name: 'E', title: 'E', score: 70 })

      const results = await collection.builder()
        .where('score', 'gte', 75)
        .orderBy('score', 'desc')
        .limit(3)
        .select(['title', 'score'])
        .find()

      expect(results).toHaveLength(3)
      expect(results.map(r => (r as any).title)).toEqual(['A', 'B', 'C'])
    })
  })

  // =============================================================================
  // Array Operators
  // =============================================================================

  describe('array operators', () => {
    describe('whereAll()', () => {
      it('builds $all filter for array contains all', () => {
        const builder = new QueryBuilder()
          .whereAll('tags', ['tech', 'database'])
        const { filter } = builder.build()
        expect(filter).toEqual({ tags: { $all: ['tech', 'database'] } })
      })

      it('handles single element array', () => {
        const builder = new QueryBuilder()
          .whereAll('tags', ['tech'])
        const { filter } = builder.build()
        expect(filter).toEqual({ tags: { $all: ['tech'] } })
      })

      it('handles empty array', () => {
        const builder = new QueryBuilder()
          .whereAll('tags', [])
        const { filter } = builder.build()
        expect(filter).toEqual({ tags: { $all: [] } })
      })

      it('chains with other conditions', () => {
        const builder = new QueryBuilder()
          .where('status', 'eq', 'published')
          .whereAll('tags', ['tech', 'database'])
        const { filter } = builder.build()
        expect(filter).toEqual({
          status: { $eq: 'published' },
          tags: { $all: ['tech', 'database'] }
        })
      })

      it('returns this for method chaining', () => {
        const builder = new QueryBuilder()
        expect(builder.whereAll('tags', ['tech'])).toBe(builder)
      })
    })

    describe('whereElemMatch()', () => {
      it('builds $elemMatch filter for array element matching', () => {
        const builder = new QueryBuilder()
          .whereElemMatch('comments', { score: { $gt: 10 } })
        const { filter } = builder.build()
        expect(filter).toEqual({ comments: { $elemMatch: { score: { $gt: 10 } } } })
      })

      it('handles complex nested filters', () => {
        const builder = new QueryBuilder()
          .whereElemMatch('items', {
            quantity: { $gte: 5 },
            price: { $lt: 100 }
          })
        const { filter } = builder.build()
        expect(filter).toEqual({
          items: {
            $elemMatch: {
              quantity: { $gte: 5 },
              price: { $lt: 100 }
            }
          }
        })
      })

      it('handles simple equality in elemMatch', () => {
        const builder = new QueryBuilder()
          .whereElemMatch('users', { role: 'admin' })
        const { filter } = builder.build()
        expect(filter).toEqual({ users: { $elemMatch: { role: 'admin' } } })
      })

      it('chains with other conditions', () => {
        const builder = new QueryBuilder()
          .where('status', 'eq', 'active')
          .whereElemMatch('comments', { approved: true })
        const { filter } = builder.build()
        expect(filter).toEqual({
          status: { $eq: 'active' },
          comments: { $elemMatch: { approved: true } }
        })
      })

      it('returns this for method chaining', () => {
        const builder = new QueryBuilder()
        expect(builder.whereElemMatch('items', { active: true })).toBe(builder)
      })
    })

    describe('whereSize()', () => {
      it('builds $size filter for array length', () => {
        const builder = new QueryBuilder()
          .whereSize('tags', 3)
        const { filter } = builder.build()
        expect(filter).toEqual({ tags: { $size: 3 } })
      })

      it('handles size of 0', () => {
        const builder = new QueryBuilder()
          .whereSize('items', 0)
        const { filter } = builder.build()
        expect(filter).toEqual({ items: { $size: 0 } })
      })

      it('handles size of 1', () => {
        const builder = new QueryBuilder()
          .whereSize('roles', 1)
        const { filter } = builder.build()
        expect(filter).toEqual({ roles: { $size: 1 } })
      })

      it('chains with other conditions', () => {
        const builder = new QueryBuilder()
          .where('status', 'eq', 'active')
          .whereSize('tags', 5)
        const { filter } = builder.build()
        expect(filter).toEqual({
          status: { $eq: 'active' },
          tags: { $size: 5 }
        })
      })

      it('throws for negative size', () => {
        const builder = new QueryBuilder()
        expect(() => builder.whereSize('tags', -1)).toThrow('Size cannot be negative')
      })

      it('returns this for method chaining', () => {
        const builder = new QueryBuilder()
        expect(builder.whereSize('tags', 3)).toBe(builder)
      })
    })

    describe('combined array operators', () => {
      it('combines multiple array operators on different fields', () => {
        const builder = new QueryBuilder()
          .whereAll('tags', ['tech'])
          .whereSize('comments', 5)
        const { filter } = builder.build()
        expect(filter).toEqual({
          tags: { $all: ['tech'] },
          comments: { $size: 5 }
        })
      })

      it('works in a complete query chain', () => {
        const builder = new QueryBuilder()
          .where('status', 'eq', 'published')
          .whereAll('tags', ['tech', 'database'])
          .whereSize('authors', 2)
          .orderBy('createdAt', 'desc')
          .limit(10)

        const { filter, options } = builder.build()

        expect(filter).toEqual({
          status: { $eq: 'published' },
          tags: { $all: ['tech', 'database'] },
          authors: { $size: 2 }
        })
        expect(options).toEqual({
          sort: { createdAt: 'desc' },
          limit: 10
        })
      })
    })
  })
})
