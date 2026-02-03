/**
 * Update Operation Tests
 *
 * Tests for the update operation module.
 */

import { describe, it, expect } from 'vitest'
import {
  executeUpdate,
  createMutationContext,
  MutationOperationError,
  MutationErrorCodes,
} from '../../../src/mutation'
import { VersionConflictError } from '../../../src/mutation/update'
import type { Entity, EntityId, Schema } from '../../../src/types'

// =============================================================================
// Test Helpers
// =============================================================================

function createTestEntity(overrides: Partial<Entity> = {}): Entity {
  return {
    $id: 'posts/test-123' as EntityId,
    $type: 'Post',
    name: 'Test Post',
    title: 'Hello World',
    content: 'Original content',
    viewCount: 10,
    tags: ['a', 'b'],
    createdAt: new Date('2024-01-01T00:00:00Z'),
    createdBy: 'users/creator' as EntityId,
    updatedAt: new Date('2024-01-01T00:00:00Z'),
    updatedBy: 'users/creator' as EntityId,
    version: 1,
    ...overrides,
  } as Entity
}

// =============================================================================
// Execute Update Tests
// =============================================================================

describe('executeUpdate', () => {
  const context = createMutationContext('posts', {
    actor: 'users/updater' as EntityId,
    timestamp: new Date('2024-01-15T00:00:00Z'),
  })

  describe('$set operator', () => {
    it('sets scalar fields', () => {
      const entity = createTestEntity()
      const result = executeUpdate(context, 'posts/test-123', { $set: { title: 'Updated Title' } }, entity)

      expect(result.entity?.title).toBe('Updated Title')
      expect(result.modified).toBe(true)
      expect(result.upserted).toBe(false)
    })

    it('sets multiple fields', () => {
      const entity = createTestEntity()
      const result = executeUpdate(context, 'posts/test-123', {
        $set: { title: 'New Title', content: 'New Content' },
      }, entity)

      expect(result.entity?.title).toBe('New Title')
      expect(result.entity?.content).toBe('New Content')
    })

    it('sets nested fields with dot notation', () => {
      const entity = createTestEntity({ metadata: { views: 100 } })
      const result = executeUpdate(context, 'posts/test-123', {
        $set: { 'metadata.likes': 50 },
      }, entity)

      expect((result.entity as any).metadata.likes).toBe(50)
      expect((result.entity as any).metadata.views).toBe(100)
    })

    it('creates nested objects if they do not exist', () => {
      const entity = createTestEntity()
      const result = executeUpdate(context, 'posts/test-123', {
        $set: { 'stats.views': 100 },
      }, entity)

      expect((result.entity as any).stats.views).toBe(100)
    })
  })

  describe('$inc operator', () => {
    it('increments numeric fields', () => {
      const entity = createTestEntity({ viewCount: 10 })
      const result = executeUpdate(context, 'posts/test-123', { $inc: { viewCount: 5 } }, entity)

      expect(result.entity?.viewCount).toBe(15)
    })

    it('decrements with negative values', () => {
      const entity = createTestEntity({ viewCount: 10 })
      const result = executeUpdate(context, 'posts/test-123', { $inc: { viewCount: -3 } }, entity)

      expect(result.entity?.viewCount).toBe(7)
    })

    it('creates field if it does not exist', () => {
      const entity = createTestEntity()
      delete (entity as any).viewCount
      const result = executeUpdate(context, 'posts/test-123', { $inc: { viewCount: 1 } }, entity)

      expect(result.entity?.viewCount).toBe(1)
    })

    it('increments multiple fields', () => {
      const entity = createTestEntity({ viewCount: 10, likeCount: 5 })
      const result = executeUpdate(context, 'posts/test-123', {
        $inc: { viewCount: 1, likeCount: 2 },
      }, entity)

      expect(result.entity?.viewCount).toBe(11)
      expect((result.entity as any).likeCount).toBe(7)
    })
  })

  describe('$push operator', () => {
    it('appends to array', () => {
      const entity = createTestEntity({ tags: ['a', 'b'] })
      const result = executeUpdate(context, 'posts/test-123', { $push: { tags: 'c' } }, entity)

      expect(result.entity?.tags).toEqual(['a', 'b', 'c'])
    })

    it('creates array if it does not exist', () => {
      const entity = createTestEntity()
      delete (entity as any).tags
      const result = executeUpdate(context, 'posts/test-123', { $push: { tags: 'first' } }, entity)

      expect(result.entity?.tags).toEqual(['first'])
    })

    it('supports $each modifier', () => {
      const entity = createTestEntity({ tags: ['a'] })
      const result = executeUpdate(context, 'posts/test-123', {
        $push: { tags: { $each: ['b', 'c', 'd'] } },
      }, entity)

      expect(result.entity?.tags).toEqual(['a', 'b', 'c', 'd'])
    })

    it('supports $position modifier', () => {
      const entity = createTestEntity({ tags: ['first', 'last'] })
      const result = executeUpdate(context, 'posts/test-123', {
        $push: { tags: { $each: ['middle'], $position: 1 } },
      }, entity)

      expect(result.entity?.tags).toEqual(['first', 'middle', 'last'])
    })

    it('supports $slice modifier', () => {
      const entity = createTestEntity({ scores: [1, 2] })
      const result = executeUpdate(context, 'posts/test-123', {
        $push: { scores: { $each: [3, 4, 5], $slice: 3 } },
      }, entity)

      expect((result.entity as any).scores).toEqual([1, 2, 3])
    })
  })

  describe('$pull operator', () => {
    it('removes matching elements', () => {
      const entity = createTestEntity({ tags: ['a', 'b', 'c', 'b'] })
      const result = executeUpdate(context, 'posts/test-123', { $pull: { tags: 'b' } }, entity)

      expect(result.entity?.tags).toEqual(['a', 'c'])
    })

    it('supports comparison operators', () => {
      const entity = createTestEntity({ scores: [10, 25, 50, 75, 100] })
      const result = executeUpdate(context, 'posts/test-123', {
        $pull: { scores: { $lt: 30 } },
      }, entity)

      expect((result.entity as any).scores).toEqual([50, 75, 100])
    })

    it('supports object matching', () => {
      const entity = createTestEntity({
        items: [{ id: 1, active: true }, { id: 2, active: false }],
      })
      const result = executeUpdate(context, 'posts/test-123', {
        $pull: { items: { active: false } },
      }, entity)

      expect((result.entity as any).items).toEqual([{ id: 1, active: true }])
    })
  })

  describe('update with filters', () => {
    it('updates entity matching context namespace', () => {
      const entity = createTestEntity()
      const result = executeUpdate(context, 'posts/test-123', { $set: { title: 'Updated' } }, entity)

      expect(result.modified).toBe(true)
      expect(result.entity?.title).toBe('Updated')
    })

    it('returns null for non-existent entity without upsert', () => {
      const result = executeUpdate(context, 'posts/test-123', { $set: { title: 'Updated' } }, undefined)

      expect(result.entity).toBeNull()
      expect(result.modified).toBe(false)
      expect(result.upserted).toBe(false)
    })
  })

  describe('upsert operations', () => {
    it('creates entity when upsert is true and entity does not exist', () => {
      const result = executeUpdate(
        context,
        'posts/new-post',
        { $set: { title: 'New Post' } },
        undefined,
        { upsert: true }
      )

      expect(result.entity).toBeDefined()
      expect(result.entity?.$id).toBe('posts/new-post')
      expect(result.entity?.title).toBe('New Post')
      expect(result.upserted).toBe(true)
    })

    it('sets audit fields on upsert', () => {
      const result = executeUpdate(
        context,
        'posts/new-post',
        { $set: { title: 'New Post' } },
        undefined,
        { upsert: true }
      )

      expect(result.entity?.createdAt).toBeDefined()
      expect(result.entity?.createdBy).toBe('users/updater')
      expect(result.entity?.updatedAt).toEqual(context.timestamp)
      expect(result.entity?.updatedBy).toBe('users/updater')
    })

    it('sets version to 1 on upsert', () => {
      const result = executeUpdate(
        context,
        'posts/new-post',
        { $set: { title: 'New Post' } },
        undefined,
        { upsert: true }
      )

      expect(result.entity?.version).toBe(1)
    })

    it('generates CREATE event for upsert', () => {
      const result = executeUpdate(
        context,
        'posts/new-post',
        { $set: { title: 'New Post' } },
        undefined,
        { upsert: true }
      )

      expect(result.events[0].op).toBe('CREATE')
      expect(result.events[0].before).toBeNull()
      expect(result.events[0].after).toBeDefined()
    })

    it('does not upsert when entity exists', () => {
      const entity = createTestEntity()
      const result = executeUpdate(
        context,
        'posts/test-123',
        { $set: { title: 'Updated' } },
        entity,
        { upsert: true }
      )

      expect(result.upserted).toBe(false)
      expect(result.events[0].op).toBe('UPDATE')
    })

    it('applies $setOnInsert only on upsert', () => {
      const result = executeUpdate(
        context,
        'posts/new-post',
        {
          $set: { title: 'New Post' },
          $setOnInsert: { defaultValue: 100 },
        },
        undefined,
        { upsert: true }
      )

      expect((result.entity as any).defaultValue).toBe(100)
    })

    it('ignores $setOnInsert on regular update', () => {
      const entity = createTestEntity()
      const result = executeUpdate(
        context,
        'posts/test-123',
        {
          $set: { title: 'Updated' },
          $setOnInsert: { defaultValue: 100 },
        },
        entity,
        { upsert: true }
      )

      expect((result.entity as any).defaultValue).toBeUndefined()
    })
  })

  describe('optimistic concurrency', () => {
    it('succeeds when expectedVersion matches', () => {
      const entity = createTestEntity({ version: 3 })
      const result = executeUpdate(
        context,
        'posts/test-123',
        { $set: { title: 'Updated' } },
        entity,
        { expectedVersion: 3 }
      )

      expect(result.modified).toBe(true)
    })

    it('throws VersionConflictError when expectedVersion does not match', () => {
      const entity = createTestEntity({ version: 3 })

      expect(() =>
        executeUpdate(
          context,
          'posts/test-123',
          { $set: { title: 'Updated' } },
          entity,
          { expectedVersion: 2 }
        )
      ).toThrow(VersionConflictError)
    })

    it('throws VersionConflictError with correct values', () => {
      const entity = createTestEntity({ version: 5 })

      try {
        executeUpdate(
          context,
          'posts/test-123',
          { $set: { title: 'Updated' } },
          entity,
          { expectedVersion: 3 }
        )
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionConflictError)
        expect((error as VersionConflictError).expectedVersion).toBe(3)
        expect((error as VersionConflictError).actualVersion).toBe(5)
      }
    })

    it('throws VersionConflictError for non-existent entity with expectedVersion > 1', () => {
      expect(() =>
        executeUpdate(
          context,
          'posts/test-123',
          { $set: { title: 'Updated' } },
          undefined,
          { expectedVersion: 5, upsert: true }
        )
      ).toThrow(VersionConflictError)
    })

    it('increments version on update', () => {
      const entity = createTestEntity({ version: 3 })
      const result = executeUpdate(context, 'posts/test-123', { $set: { title: 'Updated' } }, entity)

      expect(result.entity?.version).toBe(4)
    })
  })

  describe('metadata updates', () => {
    it('updates updatedAt timestamp', () => {
      const entity = createTestEntity()
      const result = executeUpdate(context, 'posts/test-123', { $set: { title: 'Updated' } }, entity)

      expect(result.entity?.updatedAt).toEqual(context.timestamp)
    })

    it('updates updatedBy actor', () => {
      const entity = createTestEntity()
      const result = executeUpdate(context, 'posts/test-123', { $set: { title: 'Updated' } }, entity)

      expect(result.entity?.updatedBy).toBe('users/updater')
    })

    it('preserves createdAt and createdBy', () => {
      const entity = createTestEntity({
        createdAt: new Date('2020-01-01T00:00:00Z'),
        createdBy: 'users/original-creator' as EntityId,
      })
      const result = executeUpdate(context, 'posts/test-123', { $set: { title: 'Updated' } }, entity)

      expect(result.entity?.createdAt).toEqual(new Date('2020-01-01T00:00:00Z'))
      expect(result.entity?.createdBy).toBe('users/original-creator')
    })
  })

  describe('event generation', () => {
    it('generates UPDATE event', () => {
      const entity = createTestEntity()
      const result = executeUpdate(context, 'posts/test-123', { $set: { title: 'Updated' } }, entity)

      expect(result.events).toHaveLength(1)
      expect(result.events[0].op).toBe('UPDATE')
      expect(result.events[0].actor).toBe('users/updater')
      expect(result.events[0].timestamp).toEqual(context.timestamp)
    })

    it('captures before state in event', () => {
      const entity = createTestEntity({ title: 'Original Title' })
      const result = executeUpdate(context, 'posts/test-123', { $set: { title: 'Updated' } }, entity)

      expect(result.events[0].before).toMatchObject({
        title: 'Original Title',
      })
    })

    it('captures after state in event', () => {
      const entity = createTestEntity()
      const result = executeUpdate(context, 'posts/test-123', { $set: { title: 'Updated Title' } }, entity)

      expect(result.events[0].after).toMatchObject({
        title: 'Updated Title',
      })
    })
  })

  describe('returnDocument option', () => {
    it('returns after state by default', () => {
      const entity = createTestEntity({ title: 'Original' })
      const result = executeUpdate(context, 'posts/test-123', { $set: { title: 'Updated' } }, entity)

      expect(result.entity?.title).toBe('Updated')
    })

    it('returns after state when returnDocument is "after"', () => {
      const entity = createTestEntity({ title: 'Original' })
      const result = executeUpdate(
        context,
        'posts/test-123',
        { $set: { title: 'Updated' } },
        entity,
        { returnDocument: 'after' }
      )

      expect(result.entity?.title).toBe('Updated')
    })

    it('returns before state when returnDocument is "before"', () => {
      const entity = createTestEntity({ title: 'Original' })
      const result = executeUpdate(
        context,
        'posts/test-123',
        { $set: { title: 'Updated' } },
        entity,
        { returnDocument: 'before' }
      )

      expect(result.entity?.title).toBe('Original')
    })

    it('returns null for before state on upsert', () => {
      const result = executeUpdate(
        context,
        'posts/new-post',
        { $set: { title: 'New' } },
        undefined,
        { upsert: true, returnDocument: 'before' }
      )

      expect(result.entity).toBeNull()
    })
  })

  describe('combined operators', () => {
    it('applies multiple operators in sequence', () => {
      const entity = createTestEntity({
        title: 'Original',
        viewCount: 10,
        tags: ['a', 'b'],
      })
      const result = executeUpdate(context, 'posts/test-123', {
        $set: { title: 'Updated' },
        $inc: { viewCount: 5 },
        $push: { tags: 'c' },
      }, entity)

      expect(result.entity?.title).toBe('Updated')
      expect(result.entity?.viewCount).toBe(15)
      expect(result.entity?.tags).toEqual(['a', 'b', 'c'])
    })
  })

  describe('validation', () => {
    it('throws on invalid operators', () => {
      const entity = createTestEntity()

      expect(() =>
        executeUpdate(context, 'posts/test-123', { $invalid: { title: 'Test' } } as any, entity)
      ).toThrow()
    })

    it('throws on conflicting operators for same field', () => {
      const entity = createTestEntity()

      expect(() =>
        executeUpdate(context, 'posts/test-123', {
          $set: { count: 5 },
          $inc: { count: 1 },
        }, entity)
      ).toThrow(/Conflicting/)
    })
  })
})

// =============================================================================
// Version Conflict Error Tests
// =============================================================================

describe('VersionConflictError', () => {
  it('has correct name', () => {
    const error = new VersionConflictError(3, 5)
    expect(error.name).toBe('VersionConflictError')
  })

  it('has correct message', () => {
    const error = new VersionConflictError(3, 5)
    expect(error.message).toContain('expected 3')
    expect(error.message).toContain('got 5')
  })

  it('stores version values', () => {
    const error = new VersionConflictError(3, 5)
    expect(error.expectedVersion).toBe(3)
    expect(error.actualVersion).toBe(5)
  })

  it('handles undefined actual version', () => {
    const error = new VersionConflictError(3, undefined)
    expect(error.actualVersion).toBeUndefined()
    expect(error.message).toContain('got undefined')
  })
})
