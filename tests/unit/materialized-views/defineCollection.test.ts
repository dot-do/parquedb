/**
 * Tests for defineCollection() API
 *
 * Tests for creating and validating collection definitions with input validation.
 */

import { describe, it, expect } from 'vitest'
import {
  defineCollection,
  CollectionDefinitionError,
} from '../../../src/materialized-views/define'

describe('defineCollection', () => {
  describe('valid inputs', () => {
    it('should create a basic collection', () => {
      const collection = defineCollection('User', {
        name: 'string!',
        email: 'string!',
      })

      expect(collection.$type).toBe('User')
      expect(collection.name).toBe('string!')
      expect(collection.email).toBe('string!')
    })

    it('should create a collection with ingest source', () => {
      const collection = defineCollection('AIRequest', {
        modelId: 'string!',
        tokens: 'int?',
      }, 'ai-sdk')

      expect(collection.$type).toBe('AIRequest')
      expect(collection.$ingest).toBe('ai-sdk')
    })

    it('should accept type names with underscores', () => {
      const collection = defineCollection('User_Profile', {
        name: 'string!',
      })

      expect(collection.$type).toBe('User_Profile')
    })

    it('should accept type names with numbers', () => {
      const collection = defineCollection('Data2024', {
        value: 'int!',
      })

      expect(collection.$type).toBe('Data2024')
    })
  })

  describe('type name validation', () => {
    it('should reject empty type name', () => {
      expect(() => defineCollection('', { name: 'string!' })).toThrow(CollectionDefinitionError)
    })

    it('should reject type name starting with underscore', () => {
      expect(() => defineCollection('_Private', { name: 'string!' })).toThrow(CollectionDefinitionError)
    })

    it('should reject type name starting with number', () => {
      expect(() => defineCollection('123User', { name: 'string!' })).toThrow(CollectionDefinitionError)
    })

    it('should reject type name with special characters', () => {
      expect(() => defineCollection('User-Profile', { name: 'string!' })).toThrow(CollectionDefinitionError)
    })

    it('should include error code in type validation error', () => {
      try {
        defineCollection('', { name: 'string!' })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(CollectionDefinitionError)
        expect((error as CollectionDefinitionError).code).toBe('INVALID_TYPE')
        expect((error as CollectionDefinitionError).field).toBe('type')
      }
    })
  })

  describe('fields validation', () => {
    it('should reject null fields', () => {
      expect(() => defineCollection('User', null as any)).toThrow(CollectionDefinitionError)
    })

    it('should reject non-object fields', () => {
      expect(() => defineCollection('User', 'invalid' as any)).toThrow(CollectionDefinitionError)
    })

    it('should reject reserved field name $type', () => {
      expect(() => defineCollection('User', { $type: 'string!' })).toThrow(CollectionDefinitionError)
    })

    it('should reject reserved field name $ingest', () => {
      expect(() => defineCollection('User', { $ingest: 'string!' })).toThrow(CollectionDefinitionError)
    })

    it('should reject reserved field name $from', () => {
      expect(() => defineCollection('User', { $from: 'string!' })).toThrow(CollectionDefinitionError)
    })

    it('should include error code in reserved field error', () => {
      try {
        defineCollection('User', { $from: 'string!' })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(CollectionDefinitionError)
        expect((error as CollectionDefinitionError).code).toBe('RESERVED_FIELD_NAME')
        expect((error as CollectionDefinitionError).field).toBe('$from')
      }
    })
  })

  describe('ingest source validation', () => {
    it('should allow undefined ingest source', () => {
      const collection = defineCollection('User', { name: 'string!' })
      expect(collection.$ingest).toBeUndefined()
    })

    it('should reject empty ingest source', () => {
      expect(() => defineCollection('User', { name: 'string!' }, '')).toThrow(CollectionDefinitionError)
    })

    it('should reject non-string ingest source', () => {
      expect(() => defineCollection('User', { name: 'string!' }, 123 as any)).toThrow(CollectionDefinitionError)
    })

    it('should include error code in ingest validation error', () => {
      try {
        defineCollection('User', { name: 'string!' }, '')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(CollectionDefinitionError)
        expect((error as CollectionDefinitionError).code).toBe('INVALID_INGEST_SOURCE')
        expect((error as CollectionDefinitionError).field).toBe('ingestSource')
      }
    })

    it('should accept custom ingest source strings', () => {
      const collection = defineCollection('Custom', { value: 'int!' }, 'my-custom-source')
      expect(collection.$ingest).toBe('my-custom-source')
    })
  })

  describe('field type validation', () => {
    it('should accept valid primitive types', () => {
      const collection = defineCollection('User', {
        name: 'string!',
        age: 'int?',
        score: 'number',
        isActive: 'boolean!',
        createdAt: 'datetime',
        data: 'json',
      })

      expect(collection.$type).toBe('User')
      expect(collection.name).toBe('string!')
      expect(collection.age).toBe('int?')
    })

    it('should accept valid array types', () => {
      const collection = defineCollection('User', {
        tags: 'string[]',
        scores: 'int[]!',
      })

      expect(collection.tags).toBe('string[]')
      expect(collection.scores).toBe('int[]!')
    })

    it('should accept valid parametric types', () => {
      const collection = defineCollection('User', {
        price: 'decimal(10,2)',
        shortName: 'varchar(50)',
        embedding: 'vector(1536)',
        status: 'enum(draft,published,archived)',
      })

      expect(collection.price).toBe('decimal(10,2)')
      expect(collection.status).toBe('enum(draft,published,archived)')
    })

    it('should accept relationship strings', () => {
      const collection = defineCollection('Post', {
        title: 'string!',
        author: '-> User.posts',
        comments: '<- Comment.post[]',
      })

      expect(collection.author).toBe('-> User.posts')
      expect(collection.comments).toBe('<- Comment.post[]')
    })

    it('should accept object field definitions with valid types', () => {
      const collection = defineCollection('User', {
        email: { type: 'email!', index: 'unique' },
        bio: { type: 'text', default: '' },
      })

      expect(collection.email).toEqual({ type: 'email!', index: 'unique' })
    })

    it('should reject invalid primitive type', () => {
      expect(() => defineCollection('User', {
        name: 'invalidtype!',
      })).toThrow(CollectionDefinitionError)
    })

    it('should reject typo in type name', () => {
      expect(() => defineCollection('User', {
        name: 'strng!', // typo
      })).toThrow(CollectionDefinitionError)
    })

    it('should reject invalid parametric type syntax', () => {
      expect(() => defineCollection('User', {
        price: 'decimal()', // missing parameters
      })).toThrow(CollectionDefinitionError)
    })

    it('should reject random strings as field types', () => {
      expect(() => defineCollection('User', {
        name: 'hello world',
      })).toThrow(CollectionDefinitionError)
    })

    it('should reject invalid type in object field definition', () => {
      expect(() => defineCollection('User', {
        email: { type: 'notavalidtype!' },
      })).toThrow(CollectionDefinitionError)
    })

    it('should include error code in field type validation error', () => {
      try {
        defineCollection('User', { name: 'invalidtype!' })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(CollectionDefinitionError)
        expect((error as CollectionDefinitionError).code).toBe('INVALID_FIELD_TYPE')
        expect((error as CollectionDefinitionError).field).toBe('name')
      }
    })

    it('should provide descriptive error message for invalid field type', () => {
      try {
        defineCollection('User', { status: 'notreal' })
        expect.fail('Should have thrown')
      } catch (error) {
        const err = error as CollectionDefinitionError
        expect(err.message).toContain('notreal')
        expect(err.message).toContain('status')
        expect(err.message).toContain('Expected a valid type')
      }
    })

    it('should accept fuzzy relationship strings', () => {
      const collection = defineCollection('Post', {
        title: 'string!',
        relatedTopics: '~> Topic',
        similarPosts: '<~ Post',
      })

      expect(collection.relatedTopics).toBe('~> Topic')
      expect(collection.similarPosts).toBe('<~ Post')
    })

    it('should accept default values in field types', () => {
      const collection = defineCollection('User', {
        status: "string = 'active'",
        count: 'int = 0',
      })

      expect(collection.status).toBe("string = 'active'")
      expect(collection.count).toBe('int = 0')
    })
  })

  describe('CollectionDefinitionError', () => {
    it('should have correct error name', () => {
      try {
        defineCollection('', { name: 'string!' })
        expect.fail('Should have thrown')
      } catch (error) {
        expect((error as CollectionDefinitionError).name).toBe('CollectionDefinitionError')
      }
    })

    it('should have descriptive message', () => {
      try {
        defineCollection('_Reserved', { name: 'string!' })
        expect.fail('Should have thrown')
      } catch (error) {
        expect((error as CollectionDefinitionError).message).toContain('underscore')
      }
    })
  })
})
