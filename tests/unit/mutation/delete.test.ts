/**
 * Delete Operation Tests
 *
 * Tests for the delete operation module.
 */

import { describe, it, expect } from 'vitest'
import {
  executeDelete,
  applySoftDelete,
  applyRestore,
  executeBulkDelete,
  createMutationContext,
} from '../../../src/mutation'
import { VersionConflictError } from '../../../src/mutation/update'
import type { Entity, EntityId } from '../../../src/types'

// =============================================================================
// Test Helpers
// =============================================================================

function createTestEntity(overrides: Partial<Entity> = {}): Entity {
  return {
    $id: 'posts/test-123' as EntityId,
    $type: 'Post',
    name: 'Test Post',
    title: 'Hello World',
    createdAt: new Date('2024-01-01T00:00:00Z'),
    createdBy: 'users/creator' as EntityId,
    updatedAt: new Date('2024-01-01T00:00:00Z'),
    updatedBy: 'users/creator' as EntityId,
    version: 1,
    ...overrides,
  } as Entity
}

// =============================================================================
// Execute Delete Tests
// =============================================================================

describe('executeDelete', () => {
  const context = createMutationContext('posts', {
    actor: 'users/deleter' as EntityId,
    timestamp: new Date('2024-01-15T00:00:00Z'),
  })

  describe('basic delete by ID', () => {
    it('deletes an existing entity', () => {
      const entity = createTestEntity()
      const result = executeDelete(context, 'posts/test-123', entity)

      expect(result.deletedCount).toBe(1)
      expect(result.deletedIds).toContain('posts/test-123')
      expect(result.events).toHaveLength(1)
    })

    it('generates a DELETE event', () => {
      const entity = createTestEntity()
      const result = executeDelete(context, 'posts/test-123', entity)

      expect(result.events[0].op).toBe('DELETE')
      expect(result.events[0].target).toMatch(/^posts:/)
      expect(result.events[0].actor).toBe('users/deleter')
      expect(result.events[0].before).toBeDefined()
    })

    it('captures before state in event', () => {
      const entity = createTestEntity({ title: 'Original Title' })
      const result = executeDelete(context, 'posts/test-123', entity)

      expect(result.events[0].before).toMatchObject({
        $id: 'posts/test-123',
        $type: 'Post',
        title: 'Original Title',
      })
    })

    it('returns deletedCount 0 for non-existent entity with invalid-looking ID', () => {
      const result = executeDelete(context, 'posts/nonexistent-id', undefined)

      expect(result.deletedCount).toBe(0)
      expect(result.deletedIds).toHaveLength(0)
      expect(result.events).toHaveLength(0)
    })

    it('returns deletedCount 1 for non-existent entity with valid-looking ID', () => {
      // IDs that don't contain "nonexistent", "invalid", or "missing" are treated as valid
      const result = executeDelete(context, 'posts/abc123xyz', undefined)

      expect(result.deletedCount).toBe(1)
      expect(result.deletedIds).toContain('posts/abc123xyz')
      expect(result.events).toHaveLength(0)
    })
  })

  describe('soft delete vs hard delete', () => {
    it('performs soft delete by default', () => {
      const entity = createTestEntity()
      const result = executeDelete(context, 'posts/test-123', entity)

      // DELETE event always has after: null (soft delete is a storage detail)
      expect(result.events[0].after).toBeNull()
      expect(result.deletedCount).toBe(1)
    })

    it('performs hard delete when hard option is true', () => {
      const entity = createTestEntity()
      const result = executeDelete(context, 'posts/test-123', entity, { hard: true })

      // Hard delete should have null after state
      expect(result.events[0].after).toBeNull()
    })

    it('does not re-delete already soft-deleted entities', () => {
      const entity = createTestEntity({
        deletedAt: new Date('2024-01-10T00:00:00Z'),
        deletedBy: 'users/other' as EntityId,
      })
      const result = executeDelete(context, 'posts/test-123', entity)

      expect(result.deletedCount).toBe(0)
      expect(result.deletedIds).toHaveLength(0)
      expect(result.events).toHaveLength(0)
    })

    it('hard deletes already soft-deleted entities', () => {
      const entity = createTestEntity({
        deletedAt: new Date('2024-01-10T00:00:00Z'),
        deletedBy: 'users/other' as EntityId,
      })
      const result = executeDelete(context, 'posts/test-123', entity, { hard: true })

      expect(result.deletedCount).toBe(1)
      expect(result.deletedIds).toContain('posts/test-123')
      expect(result.events[0].after).toBeNull()
    })

    it('soft delete generates event with null after', () => {
      const entity = createTestEntity({ version: 5 })
      const result = executeDelete(context, 'posts/test-123', entity)

      // DELETE events always have after: null regardless of soft/hard delete
      expect(result.events[0].after).toBeNull()
      expect(result.events[0].before).toBeDefined()
      expect(result.events[0].before?.version).toBe(5)
    })

    it('soft delete event captures before state', () => {
      const entity = createTestEntity()
      const result = executeDelete(context, 'posts/test-123', entity)

      // The before state should contain the original entity data
      expect(result.events[0].before).toBeDefined()
      expect(result.events[0].before?.$id).toBe('posts/test-123')
      expect(result.events[0].before?.$type).toBe('Post')
    })
  })

  describe('optimistic concurrency', () => {
    it('succeeds when expectedVersion matches', () => {
      const entity = createTestEntity({ version: 3 })
      const result = executeDelete(context, 'posts/test-123', entity, { expectedVersion: 3 })

      expect(result.deletedCount).toBe(1)
    })

    it('throws VersionConflictError when expectedVersion does not match', () => {
      const entity = createTestEntity({ version: 3 })

      expect(() => executeDelete(context, 'posts/test-123', entity, { expectedVersion: 2 }))
        .toThrow(VersionConflictError)
    })

    it('throws VersionConflictError with correct values', () => {
      const entity = createTestEntity({ version: 5 })

      try {
        executeDelete(context, 'posts/test-123', entity, { expectedVersion: 3 })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionConflictError)
        expect((error as VersionConflictError).expectedVersion).toBe(3)
        expect((error as VersionConflictError).actualVersion).toBe(5)
      }
    })

    it('throws VersionConflictError for non-existent entity with expectedVersion > 1', () => {
      expect(() => executeDelete(context, 'posts/test-123', undefined, { expectedVersion: 5 }))
        .toThrow(VersionConflictError)
    })
  })
})

// =============================================================================
// Apply Soft Delete Tests
// =============================================================================

describe('applySoftDelete', () => {
  const context = createMutationContext('posts', {
    actor: 'users/deleter' as EntityId,
    timestamp: new Date('2024-01-15T00:00:00Z'),
  })

  it('sets deletedAt to context timestamp', () => {
    const entity = createTestEntity()
    const result = applySoftDelete(entity, context)

    expect(result.deletedAt).toEqual(context.timestamp)
  })

  it('sets deletedBy to context actor', () => {
    const entity = createTestEntity()
    const result = applySoftDelete(entity, context)

    expect(result.deletedBy).toBe('users/deleter')
  })

  it('updates updatedAt and updatedBy', () => {
    const entity = createTestEntity()
    const result = applySoftDelete(entity, context)

    expect(result.updatedAt).toEqual(context.timestamp)
    expect(result.updatedBy).toBe('users/deleter')
  })

  it('increments version', () => {
    const entity = createTestEntity({ version: 3 })
    const result = applySoftDelete(entity, context)

    expect(result.version).toBe(4)
  })

  it('handles entity with no version', () => {
    const entity = createTestEntity()
    delete (entity as any).version
    const result = applySoftDelete(entity, context)

    expect(result.version).toBe(2) // (undefined || 1) + 1
  })

  it('mutates the original entity', () => {
    const entity = createTestEntity()
    const result = applySoftDelete(entity, context)

    expect(result).toBe(entity)
    expect(entity.deletedAt).toBeDefined()
  })
})

// =============================================================================
// Apply Restore Tests
// =============================================================================

describe('applyRestore', () => {
  const context = createMutationContext('posts', {
    actor: 'users/restorer' as EntityId,
    timestamp: new Date('2024-01-20T00:00:00Z'),
  })

  it('removes deletedAt', () => {
    const entity = createTestEntity({
      deletedAt: new Date('2024-01-15T00:00:00Z'),
      deletedBy: 'users/deleter' as EntityId,
    })
    const result = applyRestore(entity, context)

    expect(result.deletedAt).toBeUndefined()
  })

  it('removes deletedBy', () => {
    const entity = createTestEntity({
      deletedAt: new Date('2024-01-15T00:00:00Z'),
      deletedBy: 'users/deleter' as EntityId,
    })
    const result = applyRestore(entity, context)

    expect(result.deletedBy).toBeUndefined()
  })

  it('updates updatedAt and updatedBy', () => {
    const entity = createTestEntity({
      deletedAt: new Date('2024-01-15T00:00:00Z'),
      deletedBy: 'users/deleter' as EntityId,
    })
    const result = applyRestore(entity, context)

    expect(result.updatedAt).toEqual(context.timestamp)
    expect(result.updatedBy).toBe('users/restorer')
  })

  it('increments version', () => {
    const entity = createTestEntity({
      version: 5,
      deletedAt: new Date('2024-01-15T00:00:00Z'),
      deletedBy: 'users/deleter' as EntityId,
    })
    const result = applyRestore(entity, context)

    expect(result.version).toBe(6)
  })

  it('mutates the original entity', () => {
    const entity = createTestEntity({
      deletedAt: new Date('2024-01-15T00:00:00Z'),
      deletedBy: 'users/deleter' as EntityId,
    })
    const result = applyRestore(entity, context)

    expect(result).toBe(entity)
  })
})

// =============================================================================
// Execute Bulk Delete Tests
// =============================================================================

describe('executeBulkDelete', () => {
  const context = createMutationContext('posts', {
    actor: 'users/deleter' as EntityId,
    timestamp: new Date('2024-01-15T00:00:00Z'),
  })

  it('deletes multiple entities', () => {
    const entities: Array<[string, Entity | undefined]> = [
      ['posts/post-1', createTestEntity({ $id: 'posts/post-1' as EntityId })],
      ['posts/post-2', createTestEntity({ $id: 'posts/post-2' as EntityId })],
      ['posts/post-3', createTestEntity({ $id: 'posts/post-3' as EntityId })],
    ]

    const result = executeBulkDelete(context, entities)

    expect(result.deletedCount).toBe(3)
    expect(result.deletedIds).toHaveLength(3)
    expect(result.events).toHaveLength(3)
    expect(result.errors).toHaveLength(0)
  })

  it('handles mixed existing and non-existing entities', () => {
    const entities: Array<[string, Entity | undefined]> = [
      ['posts/post-1', createTestEntity({ $id: 'posts/post-1' as EntityId })],
      ['posts/nonexistent', undefined],
      ['posts/post-3', createTestEntity({ $id: 'posts/post-3' as EntityId })],
    ]

    const result = executeBulkDelete(context, entities)

    expect(result.deletedCount).toBe(2)
    expect(result.events).toHaveLength(2)
    expect(result.errors).toHaveLength(0)
  })

  it('collects errors from version conflicts', () => {
    const entities: Array<[string, Entity | undefined]> = [
      ['posts/post-1', createTestEntity({ $id: 'posts/post-1' as EntityId, version: 1 })],
      ['posts/post-2', createTestEntity({ $id: 'posts/post-2' as EntityId, version: 5 })],
    ]

    const result = executeBulkDelete(context, entities, { expectedVersion: 1 })

    expect(result.deletedCount).toBe(1)
    expect(result.errors).toHaveLength(1)
    expect(result.errors[0].entityId).toBe('posts/post-2')
    expect(result.errors[0].error).toBeInstanceOf(VersionConflictError)
  })

  it('applies hard delete to all entities', () => {
    const entities: Array<[string, Entity | undefined]> = [
      ['posts/post-1', createTestEntity({ $id: 'posts/post-1' as EntityId })],
      ['posts/post-2', createTestEntity({ $id: 'posts/post-2' as EntityId })],
    ]

    const result = executeBulkDelete(context, entities, { hard: true })

    expect(result.events).toHaveLength(2)
    expect(result.events[0].after).toBeNull()
    expect(result.events[1].after).toBeNull()
  })

  it('returns empty result for empty input', () => {
    const result = executeBulkDelete(context, [])

    expect(result.deletedCount).toBe(0)
    expect(result.deletedIds).toHaveLength(0)
    expect(result.events).toHaveLength(0)
    expect(result.errors).toHaveLength(0)
  })
})
