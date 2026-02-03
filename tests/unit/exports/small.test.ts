/**
 * Tests for parquedb/small export
 *
 * Verifies that:
 * - Core exports are available
 * - Basic CRUD operations work
 * - Filter matching works
 * - Update operators work
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  // Core classes
  ParqueDB,
  Collection,
  clearGlobalStorage,

  // Entity types
  type Entity,
  type EntityId,
  type CreateInput,
  entityId,
  parseEntityId,
  isValidEntityId,

  // Filter types
  type Filter,
  matchesFilter,
  createPredicate,

  // Update types
  type UpdateInput,
  type Update,

  // Options
  type FindOptions,
  type CreateOptions,

  // Storage
  MemoryBackend,
  type StorageBackend,

  // Mutation operators
  applyOperators,
  getField,
  setField,

  // Utils
  deepEqual,
  compareValues,
  getNestedValue,
  generateId,

  // Errors
  ParqueDBError,
  ErrorCode,
  isParqueDBError,

  // Version
  VERSION,
  EXPORT_TYPE,
} from '../../../src/exports/small'

describe('parquedb/small export', () => {
  describe('export verification', () => {
    it('exports VERSION and EXPORT_TYPE', () => {
      expect(VERSION).toBe('0.1.0')
      expect(EXPORT_TYPE).toBe('small')
    })

    it('exports ParqueDB class', () => {
      expect(ParqueDB).toBeDefined()
      expect(typeof ParqueDB).toBe('function')
    })

    it('exports Collection class', () => {
      expect(Collection).toBeDefined()
      expect(typeof Collection).toBe('function')
    })

    it('exports MemoryBackend class', () => {
      expect(MemoryBackend).toBeDefined()
      expect(typeof MemoryBackend).toBe('function')
    })

    it('exports clearGlobalStorage function', () => {
      expect(clearGlobalStorage).toBeDefined()
      expect(typeof clearGlobalStorage).toBe('function')
    })
  })

  describe('entity helpers', () => {
    it('entityId creates valid EntityId', () => {
      const id = entityId('users', 'alice')
      expect(id).toBe('users/alice')
    })

    it('parseEntityId extracts ns and id', () => {
      const { ns, id } = parseEntityId('posts/my-post' as EntityId)
      expect(ns).toBe('posts')
      expect(id).toBe('my-post')
    })

    it('isValidEntityId validates format', () => {
      expect(isValidEntityId('users/alice')).toBe(true)
      expect(isValidEntityId('invalid')).toBe(false)
      expect(isValidEntityId('/invalid')).toBe(false)
    })
  })

  describe('filter matching', () => {
    const entity = {
      $id: 'posts/1' as EntityId,
      $type: 'Post',
      name: 'Hello',
      title: 'Hello World',
      status: 'published',
      views: 100,
      tags: ['tech', 'db'],
      createdAt: new Date('2024-01-01'),
      createdBy: 'users/alice' as EntityId,
      updatedAt: new Date('2024-01-02'),
      updatedBy: 'users/alice' as EntityId,
      version: 1,
    }

    it('matches simple equality', () => {
      expect(matchesFilter(entity, { status: 'published' })).toBe(true)
      expect(matchesFilter(entity, { status: 'draft' })).toBe(false)
    })

    it('matches $eq operator', () => {
      expect(matchesFilter(entity, { views: { $eq: 100 } })).toBe(true)
      expect(matchesFilter(entity, { views: { $eq: 50 } })).toBe(false)
    })

    it('matches $ne operator', () => {
      expect(matchesFilter(entity, { status: { $ne: 'draft' } })).toBe(true)
      expect(matchesFilter(entity, { status: { $ne: 'published' } })).toBe(false)
    })

    it('matches comparison operators', () => {
      expect(matchesFilter(entity, { views: { $gt: 50 } })).toBe(true)
      expect(matchesFilter(entity, { views: { $gt: 100 } })).toBe(false)
      expect(matchesFilter(entity, { views: { $gte: 100 } })).toBe(true)
      expect(matchesFilter(entity, { views: { $lt: 150 } })).toBe(true)
      expect(matchesFilter(entity, { views: { $lte: 100 } })).toBe(true)
    })

    it('matches $in operator', () => {
      expect(matchesFilter(entity, { status: { $in: ['published', 'draft'] } })).toBe(true)
      expect(matchesFilter(entity, { status: { $in: ['draft', 'archived'] } })).toBe(false)
    })

    it('matches $nin operator', () => {
      expect(matchesFilter(entity, { status: { $nin: ['draft', 'archived'] } })).toBe(true)
      expect(matchesFilter(entity, { status: { $nin: ['published', 'draft'] } })).toBe(false)
    })

    it('matches $exists operator', () => {
      expect(matchesFilter(entity, { views: { $exists: true } })).toBe(true)
      expect(matchesFilter(entity, { nonexistent: { $exists: false } })).toBe(true)
    })

    it('creates predicate function', () => {
      const isPublished = createPredicate({ status: 'published' })
      expect(isPublished(entity)).toBe(true)
      expect(isPublished({ ...entity, status: 'draft' })).toBe(false)
    })
  })

  describe('update operators', () => {
    it('applies $set operator', () => {
      const doc = { name: 'old', count: 1 }
      const result = applyOperators(doc, { $set: { name: 'new' } }, { isInsert: false })
      expect(result.document.name).toBe('new')
      expect(result.document.count).toBe(1)
    })

    it('applies $unset operator', () => {
      const doc = { name: 'test', extra: 'value' }
      const result = applyOperators(doc, { $unset: { extra: '' } }, { isInsert: false })
      expect(result.document.extra).toBeUndefined()
      expect(result.document.name).toBe('test')
    })

    it('applies $inc operator', () => {
      const doc = { count: 5 }
      const result = applyOperators(doc, { $inc: { count: 3 } }, { isInsert: false })
      expect(result.document.count).toBe(8)
    })

    it('applies $push operator', () => {
      const doc = { tags: ['a', 'b'] }
      const result = applyOperators(doc, { $push: { tags: 'c' } }, { isInsert: false })
      expect(result.document.tags).toEqual(['a', 'b', 'c'])
    })

    it('applies $pull operator', () => {
      const doc = { tags: ['a', 'b', 'c'] }
      const result = applyOperators(doc, { $pull: { tags: 'b' } }, { isInsert: false })
      expect(result.document.tags).toEqual(['a', 'c'])
    })
  })

  describe('utility functions', () => {
    it('deepEqual compares values correctly', () => {
      expect(deepEqual({ a: 1 }, { a: 1 })).toBe(true)
      expect(deepEqual({ a: 1 }, { a: 2 })).toBe(false)
      expect(deepEqual([1, 2, 3], [1, 2, 3])).toBe(true)
      expect(deepEqual(null, undefined)).toBe(true)
    })

    it('compareValues orders values correctly', () => {
      expect(compareValues(1, 2)).toBeLessThan(0)
      expect(compareValues(2, 1)).toBeGreaterThan(0)
      expect(compareValues(1, 1)).toBe(0)
      expect(compareValues('a', 'b')).toBeLessThan(0)
      expect(compareValues(null, 1)).toBeLessThan(0)
    })

    it('getNestedValue extracts nested paths', () => {
      const obj = { a: { b: { c: 42 } } }
      expect(getNestedValue(obj, 'a.b.c')).toBe(42)
      expect(getNestedValue(obj, 'a.b')).toEqual({ c: 42 })
      expect(getNestedValue(obj, 'x.y.z')).toBeUndefined()
    })

    it('generateId creates unique ids', () => {
      const id1 = generateId()
      const id2 = generateId()
      expect(id1).toBeDefined()
      expect(id1).not.toBe(id2)
    })
  })

  describe('Collection basic operations', () => {
    let collection: Collection<{ title: string; status: string; views?: number }>

    beforeEach(() => {
      clearGlobalStorage()
      collection = new Collection('posts')
    })

    it('creates entity', async () => {
      const entity = await collection.create({
        $type: 'Post',
        name: 'test-post',
        title: 'Test Post',
        status: 'draft',
      })

      expect(entity.$id).toMatch(/^posts\//)
      expect(entity.$type).toBe('Post')
      expect(entity.title).toBe('Test Post')
      expect(entity.version).toBe(1)
    })

    it('gets entity by id', async () => {
      const created = await collection.create({
        $type: 'Post',
        name: 'test',
        title: 'Test',
        status: 'published',
      })

      const fetched = await collection.get(created.$id)
      expect(fetched.$id).toBe(created.$id)
      expect(fetched.title).toBe('Test')
    })

    it('finds entities with filter', async () => {
      await collection.create({ $type: 'Post', name: 'post1', title: 'Post 1', status: 'published' })
      await collection.create({ $type: 'Post', name: 'post2', title: 'Post 2', status: 'draft' })
      await collection.create({ $type: 'Post', name: 'post3', title: 'Post 3', status: 'published' })

      const published = await collection.find({ status: 'published' })
      expect(published).toHaveLength(2)
    })

    it('updates entity', async () => {
      const created = await collection.create({
        $type: 'Post',
        name: 'test',
        title: 'Original',
        status: 'draft',
      })

      await collection.update(created.$id, { $set: { status: 'published' } })
      const updated = await collection.get(created.$id)
      expect(updated.status).toBe('published')
      expect(updated.version).toBe(2)
    })

    it('deletes entity', async () => {
      const created = await collection.create({
        $type: 'Post',
        name: 'test',
        title: 'Test',
        status: 'draft',
      })

      const result = await collection.delete(created.$id)
      expect(result.deletedCount).toBe(1)

      // Soft deleted - should throw when trying to get
      await expect(collection.get(created.$id)).rejects.toThrow()
    })
  })

  describe('MemoryBackend', () => {
    let backend: StorageBackend

    beforeEach(() => {
      backend = new MemoryBackend()
    })

    it('writes and reads data', async () => {
      const data = new TextEncoder().encode('hello world')
      await backend.write('test.txt', data)

      const read = await backend.read('test.txt')
      expect(new TextDecoder().decode(read)).toBe('hello world')
    })

    it('checks existence', async () => {
      expect(await backend.exists('missing.txt')).toBe(false)

      await backend.write('exists.txt', new Uint8Array([1, 2, 3]))
      expect(await backend.exists('exists.txt')).toBe(true)
    })

    it('lists files', async () => {
      await backend.write('data/file1.txt', new Uint8Array([1]))
      await backend.write('data/file2.txt', new Uint8Array([2]))
      await backend.write('other/file3.txt', new Uint8Array([3]))

      const result = await backend.list('data/')
      expect(result.files).toHaveLength(2)
      expect(result.files).toContain('data/file1.txt')
      expect(result.files).toContain('data/file2.txt')
    })

    it('deletes files', async () => {
      await backend.write('to-delete.txt', new Uint8Array([1]))
      expect(await backend.exists('to-delete.txt')).toBe(true)

      await backend.delete('to-delete.txt')
      expect(await backend.exists('to-delete.txt')).toBe(false)
    })
  })

  describe('error handling', () => {
    it('ParqueDBError is exported', () => {
      const error = new ParqueDBError('test error', ErrorCode.VALIDATION_FAILED)
      expect(error.message).toBe('test error')
      expect(error.code).toBe(ErrorCode.VALIDATION_FAILED)
    })

    it('isParqueDBError type guard works', () => {
      const pqError = new ParqueDBError('test', ErrorCode.INTERNAL)
      const regularError = new Error('test')

      expect(isParqueDBError(pqError)).toBe(true)
      expect(isParqueDBError(regularError)).toBe(false)
    })
  })
})
