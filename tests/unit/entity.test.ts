/**
 * Entity Unit Tests
 *
 * Fast, isolated tests for entity-related functions with mocks.
 * These tests should run quickly and not require external resources.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  entityId,
  parseEntityId,
  parseRelation,
  parseFieldType,
  isRelationString,
} from '../../src/types'
import {
  createTestEntity,
  createEntityId as createTestEntityId,
  createAuditFields,
  createCreateInput,
  createTestSchema,
  resetIdCounter,
} from '../factories'
import type { EntityId } from '../../src/types'

describe('Entity ID Functions', () => {
  describe('entityId', () => {
    it('should create a valid EntityId from namespace and id', () => {
      const id = entityId('posts', 'my-post-123')
      expect(id).toBe('posts/my-post-123')
      expect(id).toBeEntityId()
    })

    it('should handle special characters in id', () => {
      const id = entityId('users', 'user@example.com')
      expect(id).toBe('users/user@example.com')
    })

    it('should handle numeric ids', () => {
      const id = entityId('orders', '12345')
      expect(id).toBe('orders/12345')
    })

    it('should handle UUIDs', () => {
      const uuid = '550e8400-e29b-41d4-a716-446655440000'
      const id = entityId('items', uuid)
      expect(id).toBe(`items/${uuid}`)
    })
  })

  describe('parseEntityId', () => {
    it('should parse a simple EntityId', () => {
      const id = 'posts/my-post' as EntityId
      const result = parseEntityId(id)
      expect(result.ns).toBe('posts')
      expect(result.id).toBe('my-post')
    })

    it('should handle ids with slashes', () => {
      const id = 'files/path/to/file.txt' as EntityId
      const result = parseEntityId(id)
      expect(result.ns).toBe('files')
      expect(result.id).toBe('path/to/file.txt')
    })

    it('should handle namespace only (edge case)', () => {
      const id = 'ns/' as EntityId
      const result = parseEntityId(id)
      expect(result.ns).toBe('ns')
      expect(result.id).toBe('')
    })
  })
})

describe('Schema Parsing Functions', () => {
  describe('parseRelation', () => {
    it('should parse forward relation: -> User.posts', () => {
      const result = parseRelation('-> User.posts')
      expect(result).not.toBeNull()
      expect(result!.toType).toBe('User')
      expect(result!.reverse).toBe('posts')
      expect(result!.direction).toBe('forward')
      expect(result!.mode).toBe('exact')
      expect(result!.isArray).toBe(false)
    })

    it('should parse forward relation with array: -> Category.posts[]', () => {
      const result = parseRelation('-> Category.posts[]')
      expect(result).not.toBeNull()
      expect(result!.toType).toBe('Category')
      expect(result!.reverse).toBe('posts')
      expect(result!.isArray).toBe(true)
    })

    it('should parse backward relation: <- Comment.post[]', () => {
      const result = parseRelation('<- Comment.post[]')
      expect(result).not.toBeNull()
      expect(result!.fromType).toBe('Comment')
      expect(result!.fromField).toBe('post')
      expect(result!.direction).toBe('backward')
      expect(result!.isArray).toBe(true)
    })

    it('should parse fuzzy forward relation: ~> Topic', () => {
      const result = parseRelation('~> Topic')
      expect(result).not.toBeNull()
      expect(result!.toType).toBe('Topic')
      expect(result!.direction).toBe('forward')
      expect(result!.mode).toBe('fuzzy')
    })

    it('should parse fuzzy backward relation: <~ Source', () => {
      const result = parseRelation('<~ Source')
      expect(result).not.toBeNull()
      expect(result!.fromType).toBe('Source')
      expect(result!.direction).toBe('backward')
      expect(result!.mode).toBe('fuzzy')
    })

    it('should return null for invalid relation', () => {
      expect(parseRelation('string!')).toBeNull()
      expect(parseRelation('invalid')).toBeNull()
      expect(parseRelation('')).toBeNull()
    })
  })

  describe('parseFieldType', () => {
    it('should parse simple type', () => {
      const result = parseFieldType('string')
      expect(result.type).toBe('string')
      expect(result.required).toBe(false)
      expect(result.isArray).toBe(false)
    })

    it('should parse required type', () => {
      const result = parseFieldType('string!')
      expect(result.type).toBe('string')
      expect(result.required).toBe(true)
    })

    it('should parse optional type', () => {
      const result = parseFieldType('string?')
      expect(result.type).toBe('string')
      expect(result.required).toBe(false)
    })

    it('should parse array type', () => {
      const result = parseFieldType('string[]')
      expect(result.type).toBe('string')
      expect(result.isArray).toBe(true)
      expect(result.required).toBe(false)
    })

    it('should parse required array type', () => {
      const result = parseFieldType('string[]!')
      expect(result.type).toBe('string')
      expect(result.isArray).toBe(true)
      expect(result.required).toBe(true)
    })

    it('should parse type with default value', () => {
      const result = parseFieldType("string = 'default'")
      expect(result.type).toBe('string')
      expect(result.default).toBe("'default'")
    })

    it('should parse parametric types', () => {
      const result = parseFieldType('decimal(10,2)')
      expect(result.type).toBe('decimal(10,2)')
    })
  })

  describe('isRelationString', () => {
    it('should return true for relation strings', () => {
      expect(isRelationString('-> User.posts')).toBe(true)
      expect(isRelationString('<- Comment.post')).toBe(true)
      expect(isRelationString('~> Topic')).toBe(true)
      expect(isRelationString('<~ Source')).toBe(true)
    })

    it('should return false for non-relation strings', () => {
      expect(isRelationString('string')).toBe(false)
      expect(isRelationString('string!')).toBe(false)
      expect(isRelationString('User')).toBe(false)
      expect(isRelationString('')).toBe(false)
    })
  })
})

describe('Test Factories', () => {
  beforeEach(() => {
    resetIdCounter()
  })

  describe('createTestEntity', () => {
    it('should create a valid entity with defaults', () => {
      const entity = createTestEntity()
      expect(entity).toBeValidEntity()
      expect(entity.$type).toBe('TestEntity')
      expect(entity.version).toBe(1)
    })

    it('should accept overrides', () => {
      const entity = createTestEntity({
        $type: 'Post',
        name: 'My Post',
        data: { title: 'Hello World' },
      })

      expect(entity.$type).toBe('Post')
      expect(entity.name).toBe('My Post')
      expect(entity.title).toBe('Hello World')
    })

    it('should create entities with proper audit fields', () => {
      const entity = createTestEntity()
      expect(entity).toHaveAuditFields()
      expect(entity.createdAt).toBeInstanceOf(Date)
      expect(entity.updatedAt).toBeInstanceOf(Date)
    })
  })

  describe('createAuditFields', () => {
    it('should create default audit fields', () => {
      const fields = createAuditFields()
      expect(fields.createdAt).toBeInstanceOf(Date)
      expect(fields.updatedAt).toBeInstanceOf(Date)
      expect(fields.createdBy).toBeEntityId()
      expect(fields.updatedBy).toBeEntityId()
      expect(fields.version).toBe(1)
    })

    it('should accept overrides', () => {
      const customDate = new Date('2024-01-01')
      const fields = createAuditFields({
        createdAt: customDate,
        version: 5,
      })

      expect(fields.createdAt).toBe(customDate)
      expect(fields.version).toBe(5)
    })
  })

  describe('createTestSchema', () => {
    it('should create a valid schema', () => {
      const schema = createTestSchema()
      expect(schema).toHaveProperty('TestEntity')
      expect(schema.TestEntity.name).toBe('string!')
    })

    it('should merge with overrides', () => {
      const schema = createTestSchema({
        User: {
          name: 'string!',
          email: { type: 'email!', index: 'unique' },
        },
      })

      expect(schema).toHaveProperty('TestEntity')
      expect(schema).toHaveProperty('User')
      expect(schema.User.email).toEqual({ type: 'email!', index: 'unique' })
    })
  })
})

describe('Custom Matchers', () => {
  describe('toBeEntityId', () => {
    it('should pass for valid EntityIds', () => {
      expect('users/123').toBeEntityId()
      expect('posts/my-post').toBeEntityId()
      expect('files/path/to/file').toBeEntityId()
    })

    it('should fail for invalid EntityIds', () => {
      expect(() => expect('invalid').toBeEntityId()).toThrow()
      expect(() => expect('').toBeEntityId()).toThrow()
      expect(() => expect(123).toBeEntityId()).toThrow()
    })
  })

  describe('toBeValidEntity', () => {
    it('should pass for valid entities', () => {
      const entity = createTestEntity()
      expect(entity).toBeValidEntity()
    })

    it('should fail for incomplete entities', () => {
      expect(() => expect({ name: 'Test' }).toBeValidEntity()).toThrow()
      expect(() => expect(null).toBeValidEntity()).toThrow()
    })
  })

  describe('toMatchFilter', () => {
    it('should match simple equality filters', () => {
      const entity = createTestEntity({ data: { status: 'published' } })
      expect(entity).toMatchFilter({ status: 'published' })
    })

    it('should match type filter', () => {
      const entity = createTestEntity({ $type: 'Post' })
      expect(entity).toMatchFilter({ $type: 'Post' })
    })
  })
})
