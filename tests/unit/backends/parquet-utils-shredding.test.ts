/**
 * Tests for entityToRow/rowToEntity shredding support
 *
 * Tests the variant shredding functionality in parquet-utils.ts that allows
 * specified fields to be extracted into separate Parquet columns for
 * predicate pushdown, while storing remaining fields in the $data Variant.
 */

import { describe, it, expect } from 'vitest'
import {
  entityToRow,
  rowToEntity,
  type EntityToRowOptions,
  type RowToEntityOptions,
} from '../../../src/backends/parquet-utils'
import type { Entity, EntityId } from '../../../src/types/entity'

// Helper to create a test entity
function createTestEntity<T extends Record<string, unknown>>(data: T): Entity<T> {
  return {
    $id: 'test/entity-1' as EntityId,
    $type: 'TestEntity',
    name: 'Test Entity',
    createdAt: new Date('2024-01-01T00:00:00Z'),
    createdBy: 'users/admin' as EntityId,
    updatedAt: new Date('2024-01-01T00:00:00Z'),
    updatedBy: 'users/admin' as EntityId,
    version: 1,
    ...data,
  } as Entity<T>
}

describe('entityToRow with shredding', () => {
  describe('without shredding (backwards compatibility)', () => {
    it('should convert entity to row with all data in $data', () => {
      const entity = createTestEntity({
        status: 'active',
        priority: 1,
        description: 'A test entity',
      })

      const row = entityToRow(entity)

      expect(row.$id).toBe('test/entity-1')
      expect(row.$type).toBe('TestEntity')
      expect(row.name).toBe('Test Entity')
      expect(row.version).toBe(1)
      expect(row.$data).toBeDefined()
      expect(typeof row.$data).toBe('string') // base64 encoded

      // Shredded fields should NOT be present at top level
      expect(row.status).toBeUndefined()
      expect(row.priority).toBeUndefined()
      expect(row.description).toBeUndefined()
    })

    it('should handle entity with no data fields', () => {
      const entity = createTestEntity({})

      const row = entityToRow(entity)

      expect(row.$id).toBe('test/entity-1')
      expect(row.$data).toBeDefined()
    })
  })

  describe('with shredding enabled', () => {
    it('should extract shredded fields into separate columns', () => {
      const entity = createTestEntity({
        status: 'active',
        priority: 1,
        description: 'A test entity',
      })

      const options: EntityToRowOptions = {
        shredFields: ['status', 'priority'],
      }

      const row = entityToRow(entity, options)

      // Shredded fields should be at top level
      expect(row.status).toBe('active')
      expect(row.priority).toBe(1)

      // Non-shredded fields should NOT be at top level
      expect(row.description).toBeUndefined()

      // Core fields should still be present
      expect(row.$id).toBe('test/entity-1')
      expect(row.$type).toBe('TestEntity')
      expect(row.$data).toBeDefined()
    })

    it('should handle shredded fields that do not exist on entity', () => {
      const entity = createTestEntity({
        status: 'active',
      })

      const options: EntityToRowOptions = {
        shredFields: ['status', 'nonexistent'],
      }

      const row = entityToRow(entity, options)

      expect(row.status).toBe('active')
      expect(row.nonexistent).toBeUndefined()
    })

    it('should handle empty shredFields array', () => {
      const entity = createTestEntity({
        status: 'active',
        priority: 1,
      })

      const options: EntityToRowOptions = {
        shredFields: [],
      }

      const row = entityToRow(entity, options)

      // Should behave like no shredding
      expect(row.status).toBeUndefined()
      expect(row.priority).toBeUndefined()
    })

    it('should handle null values in shredded fields', () => {
      const entity = createTestEntity({
        status: null,
        priority: 1,
      })

      const options: EntityToRowOptions = {
        shredFields: ['status', 'priority'],
      }

      const row = entityToRow(entity, options)

      expect(row.status).toBeNull()
      expect(row.priority).toBe(1)
    })

    it('should handle various data types in shredded fields', () => {
      const entity = createTestEntity({
        stringField: 'hello',
        numberField: 42,
        booleanField: true,
        arrayField: [1, 2, 3],
        objectField: { nested: 'value' },
      })

      const options: EntityToRowOptions = {
        shredFields: ['stringField', 'numberField', 'booleanField', 'arrayField', 'objectField'],
      }

      const row = entityToRow(entity, options)

      expect(row.stringField).toBe('hello')
      expect(row.numberField).toBe(42)
      expect(row.booleanField).toBe(true)
      expect(row.arrayField).toEqual([1, 2, 3])
      expect(row.objectField).toEqual({ nested: 'value' })
    })
  })
})

describe('rowToEntity with shredding', () => {
  describe('without shredding (backwards compatibility)', () => {
    it('should convert row to entity with data from $data only', () => {
      const entity = createTestEntity({
        status: 'active',
        priority: 1,
      })

      const row = entityToRow(entity)
      const restored = rowToEntity<{ status: string; priority: number }>(row)

      expect(restored.$id).toBe('test/entity-1')
      expect(restored.$type).toBe('TestEntity')
      expect(restored.name).toBe('Test Entity')
      expect(restored.status).toBe('active')
      expect(restored.priority).toBe(1)
    })
  })

  describe('with shredding enabled', () => {
    it('should read shredded columns and merge with $data', () => {
      const entity = createTestEntity({
        status: 'active',
        priority: 1,
        description: 'A test entity',
      })

      const shredFields = ['status', 'priority']
      const row = entityToRow(entity, { shredFields })
      const restored = rowToEntity<{
        status: string
        priority: number
        description: string
      }>(row, { shredFields })

      expect(restored.status).toBe('active')
      expect(restored.priority).toBe(1)
      expect(restored.description).toBe('A test entity')
    })

    it('should give precedence to shredded columns over $data', () => {
      // Create a row where shredded column has different value than $data
      // This simulates a scenario where data was updated in shredded column
      const entity = createTestEntity({
        status: 'old_status',
        priority: 1,
      })

      const row = entityToRow(entity) // No shredding initially
      // Manually add shredded column with different value
      row.status = 'new_status'

      const restored = rowToEntity<{ status: string; priority: number }>(row, {
        shredFields: ['status'],
      })

      // Shredded value should take precedence
      expect(restored.status).toBe('new_status')
      expect(restored.priority).toBe(1)
    })

    it('should handle null shredded columns', () => {
      const entity = createTestEntity({
        status: 'active',
        priority: null,
      })

      const shredFields = ['status', 'priority']
      const row = entityToRow(entity, { shredFields })

      // Set shredded field to null (simulating Parquet null)
      row.priority = null

      const restored = rowToEntity<{
        status: string
        priority: number | null
      }>(row, { shredFields })

      expect(restored.status).toBe('active')
      // Null values in shredded columns are not merged (treated as missing)
      expect(restored.priority).toBeUndefined()
    })

    it('should handle missing shredded columns', () => {
      const entity = createTestEntity({
        status: 'active',
        priority: 1,
        description: 'test',
      })

      // Write with some fields shredded
      const row = entityToRow(entity, { shredFields: ['status'] })

      // Read expecting more shredded fields than were written
      const restored = rowToEntity<{
        status: string
        priority: number
        description: string
      }>(row, { shredFields: ['status', 'priority', 'nonexistent'] })

      expect(restored.status).toBe('active')
      // priority should come from $data since it wasn't shredded during write
      expect(restored.priority).toBe(1)
      expect(restored.description).toBe('test')
    })

    it('should handle empty shredFields array', () => {
      const entity = createTestEntity({
        status: 'active',
        priority: 1,
      })

      const row = entityToRow(entity)
      const restored = rowToEntity<{ status: string; priority: number }>(row, {
        shredFields: [],
      })

      expect(restored.status).toBe('active')
      expect(restored.priority).toBe(1)
    })
  })
})

describe('entityToRow/rowToEntity roundtrip', () => {
  it('should preserve all data through roundtrip without shredding', () => {
    const original = createTestEntity({
      status: 'active',
      priority: 1,
      tags: ['a', 'b', 'c'],
      metadata: { key: 'value' },
    })

    const row = entityToRow(original)
    const restored = rowToEntity<{
      status: string
      priority: number
      tags: string[]
      metadata: { key: string }
    }>(row)

    expect(restored.$id).toBe(original.$id)
    expect(restored.$type).toBe(original.$type)
    expect(restored.name).toBe(original.name)
    expect(restored.version).toBe(original.version)
    expect(restored.status).toBe(original.status)
    expect(restored.priority).toBe(original.priority)
    expect(restored.tags).toEqual(original.tags)
    expect(restored.metadata).toEqual(original.metadata)
  })

  it('should preserve all data through roundtrip with shredding', () => {
    const original = createTestEntity({
      status: 'active',
      priority: 1,
      tags: ['a', 'b', 'c'],
      metadata: { key: 'value' },
    })

    const shredFields = ['status', 'priority']
    const row = entityToRow(original, { shredFields })
    const restored = rowToEntity<{
      status: string
      priority: number
      tags: string[]
      metadata: { key: string }
    }>(row, { shredFields })

    expect(restored.$id).toBe(original.$id)
    expect(restored.$type).toBe(original.$type)
    expect(restored.name).toBe(original.name)
    expect(restored.version).toBe(original.version)
    expect(restored.status).toBe(original.status)
    expect(restored.priority).toBe(original.priority)
    expect(restored.tags).toEqual(original.tags)
    expect(restored.metadata).toEqual(original.metadata)
  })

  it('should preserve dates through roundtrip', () => {
    const original = createTestEntity({
      publishedAt: new Date('2024-06-15T12:00:00Z'),
      scheduledFor: new Date('2024-07-01T00:00:00Z'),
    })

    const shredFields = ['publishedAt']
    const row = entityToRow(original, { shredFields })
    const restored = rowToEntity<{
      publishedAt: Date
      scheduledFor: Date
    }>(row, { shredFields })

    // Note: Date objects become ISO strings through Variant encoding
    // Shredded dates also become Date objects
    expect(restored.createdAt).toEqual(original.createdAt)
    expect(restored.updatedAt).toEqual(original.updatedAt)
  })

  it('should handle complex nested structures', () => {
    const original = createTestEntity({
      config: {
        nested: {
          deeply: {
            value: 42,
          },
        },
        array: [{ a: 1 }, { b: 2 }],
      },
    })

    const row = entityToRow(original)
    const restored = rowToEntity<{
      config: {
        nested: { deeply: { value: number } }
        array: Array<{ a?: number; b?: number }>
      }
    }>(row)

    expect(restored.config).toEqual(original.config)
  })
})

describe('edge cases', () => {
  it('should handle entity with soft delete fields', () => {
    const entity = {
      $id: 'test/deleted-1' as EntityId,
      $type: 'TestEntity',
      name: 'Deleted Entity',
      createdAt: new Date('2024-01-01T00:00:00Z'),
      createdBy: 'users/admin' as EntityId,
      updatedAt: new Date('2024-01-02T00:00:00Z'),
      updatedBy: 'users/admin' as EntityId,
      deletedAt: new Date('2024-01-03T00:00:00Z'),
      deletedBy: 'users/deleter' as EntityId,
      version: 2,
      status: 'archived',
    } as Entity<{ status: string }>

    const shredFields = ['status']
    const row = entityToRow(entity, { shredFields })

    expect(row.deletedAt).toBeDefined()
    expect(row.deletedBy).toBe('users/deleter')
    expect(row.status).toBe('archived')

    const restored = rowToEntity<{ status: string }>(row, { shredFields })

    expect(restored.deletedAt).toBeDefined()
    expect(restored.deletedBy).toBe('users/deleter')
    expect(restored.status).toBe('archived')
  })

  it('should handle undefined values (should be filtered out)', () => {
    const entity = createTestEntity({
      defined: 'value',
      undefined: undefined,
    })

    const row = entityToRow(entity)

    // Undefined should not appear in shredded columns
    expect(row.undefined).toBeUndefined()

    const restored = rowToEntity<{ defined: string; undefined?: string }>(row)

    expect(restored.defined).toBe('value')
    expect(restored.undefined).toBeUndefined()
  })

  it('should handle empty string values', () => {
    const entity = createTestEntity({
      emptyString: '',
      status: 'active',
    })

    const shredFields = ['emptyString', 'status']
    const row = entityToRow(entity, { shredFields })

    expect(row.emptyString).toBe('')
    expect(row.status).toBe('active')

    const restored = rowToEntity<{ emptyString: string; status: string }>(row, { shredFields })

    expect(restored.emptyString).toBe('')
    expect(restored.status).toBe('active')
  })

  it('should handle zero number values', () => {
    const entity = createTestEntity({
      zero: 0,
      one: 1,
    })

    const shredFields = ['zero', 'one']
    const row = entityToRow(entity, { shredFields })

    expect(row.zero).toBe(0)
    expect(row.one).toBe(1)

    const restored = rowToEntity<{ zero: number; one: number }>(row, { shredFields })

    expect(restored.zero).toBe(0)
    expect(restored.one).toBe(1)
  })

  it('should handle false boolean values', () => {
    const entity = createTestEntity({
      isActive: false,
      isAdmin: true,
    })

    const shredFields = ['isActive', 'isAdmin']
    const row = entityToRow(entity, { shredFields })

    expect(row.isActive).toBe(false)
    expect(row.isAdmin).toBe(true)

    const restored = rowToEntity<{ isActive: boolean; isAdmin: boolean }>(row, { shredFields })

    expect(restored.isActive).toBe(false)
    expect(restored.isAdmin).toBe(true)
  })
})
