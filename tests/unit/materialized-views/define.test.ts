/**
 * Tests for defineView() and defineCollection() API
 *
 * Tests for creating and validating materialized view and collection definitions.
 */

import { describe, it, expect } from 'vitest'
import {
  defineView,
  defineCollection,
  MVDefinitionError,
  CollectionDefinitionError,
  parseSchema,
} from '../../../src/materialized-views/define'

// =============================================================================
// defineView Tests
// =============================================================================

describe('defineView', () => {
  describe('basic view creation', () => {
    it('should create a simple view with $from', () => {
      const view = defineView({
        $from: 'posts',
      })

      expect(view.$from).toBe('posts')
      expect(view.$refresh?.mode).toBe('streaming') // default
    })

    it('should create a view with $filter', () => {
      const view = defineView({
        $from: 'posts',
        $filter: { status: 'published' },
      })

      expect(view.$filter).toEqual({ status: 'published' })
    })

    it('should create a view with $expand', () => {
      const view = defineView({
        $from: 'Order',
        $expand: ['customer', 'items.product'],
      })

      expect(view.$expand).toEqual(['customer', 'items.product'])
    })

    it('should create a view with $groupBy and $compute', () => {
      const view = defineView({
        $from: 'Order',
        $groupBy: [{ date: '$createdAt' }, 'status'],
        $compute: {
          orderCount: { $count: '*' },
          revenue: { $sum: 'total' },
        },
      })

      expect(view.$groupBy).toEqual([{ date: '$createdAt' }, 'status'])
      expect(view.$compute).toEqual({
        orderCount: { $count: '*' },
        revenue: { $sum: 'total' },
      })
    })
  })

  describe('refresh mode options', () => {
    it('should default to streaming refresh mode', () => {
      const view = defineView({
        $from: 'posts',
      })

      expect(view.$refresh?.mode).toBe('streaming')
    })

    it('should accept scheduled refresh with cron', () => {
      const view = defineView({
        $from: 'posts',
        $refresh: { mode: 'scheduled', schedule: '0 * * * *' },
      })

      expect(view.$refresh?.mode).toBe('scheduled')
      expect(view.$refresh?.schedule).toBe('0 * * * *')
    })

    it('should accept manual refresh mode', () => {
      const view = defineView({
        $from: 'posts',
        $refresh: { mode: 'manual' },
      })

      expect(view.$refresh?.mode).toBe('manual')
    })
  })

  describe('$from validation', () => {
    it('should throw MISSING_FROM for undefined $from', () => {
      expect(() => defineView({
        // @ts-expect-error - Testing runtime behavior with missing $from
        $filter: { status: 'active' },
      })).toThrow(MVDefinitionError)
    })

    it('should throw INVALID_FROM for empty string $from', () => {
      expect(() => defineView({
        $from: '',
      })).toThrow(MVDefinitionError)

      try {
        defineView({ $from: '' })
      } catch (error) {
        expect(error).toBeInstanceOf(MVDefinitionError)
        expect((error as MVDefinitionError).code).toBe('INVALID_FROM')
        expect((error as MVDefinitionError).field).toBe('$from')
      }
    })

    it('should throw INVALID_FROM for non-string $from', () => {
      expect(() => defineView({
        // @ts-expect-error - Testing runtime behavior with invalid $from type
        $from: 123,
      })).toThrow(MVDefinitionError)
    })
  })

  describe('$expand validation', () => {
    it('should throw INVALID_EXPAND for non-array $expand', () => {
      expect(() => defineView({
        $from: 'posts',
        // @ts-expect-error - Testing runtime behavior with invalid $expand type
        $expand: 'customer',
      })).toThrow(MVDefinitionError)

      try {
        // @ts-expect-error
        defineView({ $from: 'posts', $expand: 'customer' })
      } catch (error) {
        expect((error as MVDefinitionError).code).toBe('INVALID_EXPAND')
        expect((error as MVDefinitionError).field).toBe('$expand')
      }
    })

    it('should throw INVALID_EXPAND for array with non-string elements', () => {
      expect(() => defineView({
        $from: 'posts',
        // @ts-expect-error - Testing runtime behavior with invalid $expand elements
        $expand: ['customer', 123],
      })).toThrow(MVDefinitionError)
    })

    it('should throw INVALID_EXPAND for array with empty strings', () => {
      expect(() => defineView({
        $from: 'posts',
        $expand: ['customer', ''],
      })).toThrow(MVDefinitionError)
    })

    it('should accept valid $expand array', () => {
      const view = defineView({
        $from: 'Order',
        $expand: ['customer', 'items.product'],
      })
      expect(view.$expand).toEqual(['customer', 'items.product'])
    })
  })

  describe('$filter validation', () => {
    it('should throw INVALID_FILTER for non-object $filter', () => {
      expect(() => defineView({
        $from: 'posts',
        // @ts-expect-error - Testing runtime behavior with invalid $filter type
        $filter: 'status = published',
      })).toThrow(MVDefinitionError)

      try {
        // @ts-expect-error
        defineView({ $from: 'posts', $filter: 'invalid' })
      } catch (error) {
        expect((error as MVDefinitionError).code).toBe('INVALID_FILTER')
        expect((error as MVDefinitionError).field).toBe('$filter')
      }
    })

    it('should throw INVALID_FILTER for filter with unknown operator', () => {
      expect(() => defineView({
        $from: 'posts',
        $filter: {
          // @ts-expect-error - Testing runtime behavior with unknown operator
          $unknownOp: 'value',
        },
      })).toThrow(MVDefinitionError)
    })

    it('should accept valid filter with known operators', () => {
      const view = defineView({
        $from: 'posts',
        $filter: {
          status: { $eq: 'published' },
          views: { $gt: 100 },
        },
      })
      expect(view.$filter).toBeDefined()
    })

    it('should accept valid filter with logical operators', () => {
      const view = defineView({
        $from: 'posts',
        $filter: {
          $or: [
            { status: 'published' },
            { featured: true },
          ],
        },
      })
      expect(view.$filter?.$or).toHaveLength(2)
    })
  })

  describe('$groupBy validation', () => {
    it('should throw INVALID_GROUP_BY for non-array $groupBy', () => {
      expect(() => defineView({
        $from: 'posts',
        // @ts-expect-error - Testing runtime behavior with invalid $groupBy type
        $groupBy: 'status',
      })).toThrow(MVDefinitionError)

      try {
        // @ts-expect-error
        defineView({ $from: 'posts', $groupBy: 'status' })
      } catch (error) {
        expect((error as MVDefinitionError).code).toBe('INVALID_GROUP_BY')
        expect((error as MVDefinitionError).field).toBe('$groupBy')
      }
    })

    it('should throw INVALID_GROUP_BY for array with invalid elements', () => {
      expect(() => defineView({
        $from: 'posts',
        // @ts-expect-error - Testing runtime behavior with invalid $groupBy elements
        $groupBy: ['status', 123],
      })).toThrow(MVDefinitionError)
    })

    it('should accept $groupBy with string elements', () => {
      const view = defineView({
        $from: 'posts',
        $groupBy: ['status', 'author'],
      })
      expect(view.$groupBy).toEqual(['status', 'author'])
    })

    it('should accept $groupBy with object elements', () => {
      const view = defineView({
        $from: 'posts',
        $groupBy: [{ date: '$createdAt' }],
      })
      expect(view.$groupBy).toEqual([{ date: '$createdAt' }])
    })
  })

  describe('$compute validation', () => {
    it('should throw INVALID_COMPUTE for non-object $compute', () => {
      expect(() => defineView({
        $from: 'posts',
        // @ts-expect-error - Testing runtime behavior with invalid $compute type
        $compute: ['count'],
      })).toThrow(MVDefinitionError)

      try {
        // @ts-expect-error
        defineView({ $from: 'posts', $compute: 'count' })
      } catch (error) {
        expect((error as MVDefinitionError).code).toBe('INVALID_COMPUTE')
        expect((error as MVDefinitionError).field).toBe('$compute')
      }
    })

    it('should throw INVALID_COMPUTE for compute with invalid aggregate expression', () => {
      expect(() => defineView({
        $from: 'posts',
        $compute: {
          // @ts-expect-error - Testing runtime behavior with invalid aggregate
          count: 'count(*)',
        },
      })).toThrow(MVDefinitionError)
    })

    it('should throw INVALID_COMPUTE for compute with unknown aggregate function', () => {
      expect(() => defineView({
        $from: 'posts',
        $compute: {
          // @ts-expect-error - Testing runtime behavior with unknown aggregate
          count: { $unknown: '*' },
        },
      })).toThrow(MVDefinitionError)
    })

    it('should accept valid $compute with $count', () => {
      const view = defineView({
        $from: 'posts',
        $compute: {
          count: { $count: '*' },
        },
      })
      expect(view.$compute).toEqual({ count: { $count: '*' } })
    })

    it('should accept valid $compute with $sum', () => {
      const view = defineView({
        $from: 'posts',
        $compute: {
          totalViews: { $sum: 'views' },
        },
      })
      expect(view.$compute).toEqual({ totalViews: { $sum: 'views' } })
    })

    it('should accept valid $compute with $avg, $min, $max', () => {
      const view = defineView({
        $from: 'posts',
        $compute: {
          avgViews: { $avg: 'views' },
          minViews: { $min: 'views' },
          maxViews: { $max: 'views' },
        },
      })
      expect(view.$compute?.avgViews).toEqual({ $avg: 'views' })
    })

    it('should accept $compute with conditional expression', () => {
      const view = defineView({
        $from: 'posts',
        $compute: {
          publishedCount: {
            $sum: {
              $cond: [{ status: 'published' }, 1, 0],
            },
          },
        },
      })
      expect(view.$compute?.publishedCount).toBeDefined()
    })
  })

  describe('$refresh validation', () => {
    it('should throw INVALID_REFRESH_MODE for unknown mode', () => {
      expect(() => defineView({
        $from: 'posts',
        // @ts-expect-error - Testing runtime behavior with invalid refresh mode
        $refresh: { mode: 'realtime' },
      })).toThrow(MVDefinitionError)
    })

    it('should throw MISSING_SCHEDULE for scheduled mode without schedule', () => {
      expect(() => defineView({
        $from: 'posts',
        $refresh: { mode: 'scheduled' },
      })).toThrow(MVDefinitionError)

      try {
        defineView({ $from: 'posts', $refresh: { mode: 'scheduled' } })
      } catch (error) {
        expect((error as MVDefinitionError).code).toBe('MISSING_SCHEDULE')
      }
    })

    it('should throw INVALID_SCHEDULE for invalid cron expression', () => {
      expect(() => defineView({
        $from: 'posts',
        $refresh: { mode: 'scheduled', schedule: 'invalid-cron' },
      })).toThrow(MVDefinitionError)
    })

    it('should accept valid cron expression', () => {
      const view = defineView({
        $from: 'posts',
        $refresh: { mode: 'scheduled', schedule: '0 * * * *' },
      })
      expect(view.$refresh?.schedule).toBe('0 * * * *')
    })
  })
})

// =============================================================================
// defineCollection Tests
// =============================================================================

describe('defineCollection', () => {
  describe('basic collection creation', () => {
    it('should create a simple collection with type and fields', () => {
      const collection = defineCollection('User', {
        name: 'string!',
        email: 'string!',
        age: 'int?',
      })

      expect(collection.$type).toBe('User')
      expect(collection.name).toBe('string!')
      expect(collection.email).toBe('string!')
      expect(collection.age).toBe('int?')
    })

    it('should create a collection with ingest source', () => {
      const collection = defineCollection('AIRequest', {
        modelId: 'string!',
        tokens: 'int?',
      }, 'ai-sdk')

      expect(collection.$type).toBe('AIRequest')
      expect(collection.$ingest).toBe('ai-sdk')
    })
  })

  describe('type name validation', () => {
    it('should throw INVALID_TYPE for empty type', () => {
      expect(() => defineCollection('', {
        name: 'string!',
      })).toThrow(CollectionDefinitionError)

      try {
        defineCollection('', { name: 'string!' })
      } catch (error) {
        expect(error).toBeInstanceOf(CollectionDefinitionError)
        expect((error as CollectionDefinitionError).code).toBe('INVALID_TYPE')
        expect((error as CollectionDefinitionError).field).toBe('type')
      }
    })

    it('should throw INVALID_TYPE for type starting with underscore', () => {
      expect(() => defineCollection('_Reserved', {
        name: 'string!',
      })).toThrow(CollectionDefinitionError)
    })

    it('should throw INVALID_TYPE for type with invalid characters', () => {
      expect(() => defineCollection('User-Profile', {
        name: 'string!',
      })).toThrow(CollectionDefinitionError)
    })

    it('should throw INVALID_TYPE for type starting with number', () => {
      expect(() => defineCollection('123User', {
        name: 'string!',
      })).toThrow(CollectionDefinitionError)
    })

    it('should accept valid type name', () => {
      const collection = defineCollection('UserProfile', {
        name: 'string!',
      })
      expect(collection.$type).toBe('UserProfile')
    })

    it('should accept type name with underscores', () => {
      const collection = defineCollection('User_Profile', {
        name: 'string!',
      })
      expect(collection.$type).toBe('User_Profile')
    })
  })

  describe('fields validation', () => {
    it('should throw INVALID_FIELDS for null fields', () => {
      expect(() => defineCollection('User',
        // @ts-expect-error - Testing runtime behavior with null fields
        null
      )).toThrow(CollectionDefinitionError)
    })

    it('should throw RESERVED_FIELD_NAME for reserved field names', () => {
      expect(() => defineCollection('User', {
        $type: 'SomeType',
        name: 'string!',
      })).toThrow(CollectionDefinitionError)

      try {
        defineCollection('User', { $from: 'Source', name: 'string!' })
      } catch (error) {
        expect((error as CollectionDefinitionError).code).toBe('RESERVED_FIELD_NAME')
        expect((error as CollectionDefinitionError).field).toBe('$from')
      }
    })
  })

  describe('field type validation', () => {
    it('should throw INVALID_FIELD_TYPE for invalid type string', () => {
      expect(() => defineCollection('User', {
        name: 'invalidtype!',
      })).toThrow(CollectionDefinitionError)

      try {
        defineCollection('User', { name: 'notavalidtype' })
      } catch (error) {
        expect((error as CollectionDefinitionError).code).toBe('INVALID_FIELD_TYPE')
        expect((error as CollectionDefinitionError).field).toBe('name')
      }
    })

    it('should accept valid primitive types', () => {
      const collection = defineCollection('User', {
        name: 'string!',
        age: 'int?',
        active: 'boolean',
        score: 'float',
        created: 'timestamp!',
      })
      expect(collection.name).toBe('string!')
      expect(collection.age).toBe('int?')
    })

    it('should accept relationship strings', () => {
      const collection = defineCollection('Post', {
        title: 'string!',
        author: '-> User.posts',
      })
      expect(collection.author).toBe('-> User.posts')
    })

    it('should accept object field definitions with valid type', () => {
      const collection = defineCollection('User', {
        name: { type: 'string!', index: true },
      })
      expect(collection.name).toEqual({ type: 'string!', index: true })
    })

    it('should throw INVALID_FIELD_TYPE for object field with invalid type', () => {
      expect(() => defineCollection('User', {
        name: { type: 'invalidtype!' },
      })).toThrow(CollectionDefinitionError)
    })
  })

  describe('ingest source validation', () => {
    it('should throw INVALID_INGEST_SOURCE for empty string', () => {
      expect(() => defineCollection('User', {
        name: 'string!',
      }, '')).toThrow(CollectionDefinitionError)
    })

    it('should accept valid ingest sources', () => {
      const collection = defineCollection('Events', {
        type: 'string!',
      }, 'tail')
      expect(collection.$ingest).toBe('tail')
    })
  })
})

// =============================================================================
// MVDefinitionError Tests
// =============================================================================

describe('MVDefinitionError', () => {
  it('should have correct error properties', () => {
    const error = new MVDefinitionError('INVALID_FROM', '$from', 'Test message')

    expect(error.name).toBe('MVDefinitionError')
    expect(error.code).toBe('INVALID_FROM')
    expect(error.field).toBe('$from')
    expect(error.message).toBe('Test message')
  })
})

// =============================================================================
// CollectionDefinitionError Tests
// =============================================================================

describe('CollectionDefinitionError', () => {
  it('should have correct error properties', () => {
    const error = new CollectionDefinitionError('INVALID_TYPE', 'type', 'Test message')

    expect(error.name).toBe('CollectionDefinitionError')
    expect(error.code).toBe('INVALID_TYPE')
    expect(error.field).toBe('type')
    expect(error.message).toBe('Test message')
  })
})

// =============================================================================
// parseSchema Tests
// =============================================================================

describe('parseSchema', () => {
  describe('with valid inputs', () => {
    it('should parse collections correctly', () => {
      const schema = {
        Customer: { name: 'string!' },
        Order: { total: 'int!' },
      }
      const parsed = parseSchema(schema)

      expect(parsed.collections.size).toBe(2)
      expect(parsed.collections.has('Customer')).toBe(true)
      expect(parsed.collections.has('Order')).toBe(true)
      expect(parsed.streamCollections.size).toBe(0)
      expect(parsed.materializedViews.size).toBe(0)
    })

    it('should parse materialized views correctly', () => {
      const schema = {
        Customer: { name: 'string!' },
        ActiveCustomers: { $from: 'Customer', $filter: { active: true } },
      }
      const parsed = parseSchema(schema)

      expect(parsed.collections.size).toBe(1)
      expect(parsed.materializedViews.size).toBe(1)
      expect(parsed.materializedViews.has('ActiveCustomers')).toBe(true)
    })

    it('should parse stream collections correctly', () => {
      const schema = {
        TailEvents: { $type: 'TailEvent', $ingest: 'tail', outcome: 'string!' },
      }
      const parsed = parseSchema(schema)

      expect(parsed.streamCollections.size).toBe(1)
      expect(parsed.streamCollections.has('TailEvents')).toBe(true)
    })
  })

  describe('with invalid inputs', () => {
    it('should skip null entries', () => {
      const schema = {
        Customer: { name: 'string!' },
        // @ts-expect-error - Testing runtime behavior with invalid input
        NullEntry: null,
      }
      const parsed = parseSchema(schema)

      expect(parsed.collections.size).toBe(1)
      expect(parsed.collections.has('Customer')).toBe(true)
      expect(parsed.collections.has('NullEntry')).toBe(false)
    })

    it('should skip non-object entries', () => {
      const schema = {
        Customer: { name: 'string!' },
        // @ts-expect-error - Testing runtime behavior with invalid input
        StringEntry: 'not an object',
        // @ts-expect-error - Testing runtime behavior with invalid input
        NumberEntry: 123,
      }
      const parsed = parseSchema(schema)

      expect(parsed.collections.size).toBe(1)
    })

    it('should skip MVs with non-string $from', () => {
      const schema = {
        Customer: { name: 'string!' },
        // @ts-expect-error - Testing runtime behavior with invalid input
        BadMV: { $from: 123 },
      }
      const parsed = parseSchema(schema)

      expect(parsed.materializedViews.size).toBe(0)
      expect(parsed.collections.size).toBe(2)
    })

    it('should skip stream collections with non-string $ingest', () => {
      const schema = {
        // @ts-expect-error - Testing runtime behavior with invalid input
        BadStream: { $type: 'Test', $ingest: 123 },
      }
      const parsed = parseSchema(schema)

      expect(parsed.streamCollections.size).toBe(0)
      expect(parsed.collections.size).toBe(1)
    })
  })
})
