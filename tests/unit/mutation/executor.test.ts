/**
 * Mutation Executor Tests
 *
 * Tests for the MutationExecutor class.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  MutationExecutor,
  VersionConflictError,
  type EntityStore,
  type MutationEvent,
} from '../../../src/mutation'
import type { Entity, EntityId, CreateInput, UpdateInput, Schema } from '../../../src/types'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a simple in-memory entity store for testing
 */
function createTestStore(): EntityStore & { data: Map<string, Entity> } {
  const data = new Map<string, Entity>()
  return {
    data,
    get: (id: string) => data.get(id),
    set: (id: string, entity: Entity) => { data.set(id, entity) },
    delete: (id: string) => data.delete(id),
    has: (id: string) => data.has(id),
    forEach: (callback) => data.forEach(callback),
  }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('MutationExecutor', () => {
  let executor: MutationExecutor
  let store: EntityStore & { data: Map<string, Entity> }

  beforeEach(() => {
    executor = new MutationExecutor({
      defaultActor: 'users/test' as EntityId,
    })
    store = createTestStore()
  })

  // ===========================================================================
  // Create Tests
  // ===========================================================================

  describe('create', () => {
    it('creates a new entity', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
      }, store)

      expect(entity.$id).toMatch(/^posts\//)
      expect(entity.$type).toBe('Post')
      expect(entity.name).toBe('Test Post')
      expect(entity.title).toBe('Hello World')
      expect(entity.version).toBe(1)
      expect(entity.createdAt).toBeInstanceOf(Date)
      expect(entity.updatedAt).toBeInstanceOf(Date)
    })

    it('stores entity in the store', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      expect(store.has(entity.$id as string)).toBe(true)
      expect(store.get(entity.$id as string)).toEqual(entity)
    })

    it('sets actor from options', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store, { actor: 'users/admin' as EntityId })

      expect(entity.createdBy).toBe('users/admin')
      expect(entity.updatedBy).toBe('users/admin')
    })

    it('validates required fields', async () => {
      await expect(
        executor.create('posts', {
          name: 'No Type',
        } as CreateInput, store)
      ).rejects.toThrow()

      await expect(
        executor.create('posts', {
          $type: 'Post',
        } as CreateInput, store)
      ).rejects.toThrow()
    })

    it('skips validation with skipValidation option', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        // Missing name, but should not throw with skipValidation
      } as CreateInput, store, { skipValidation: true })

      expect(entity.$type).toBe('Post')
    })
  })

  // ===========================================================================
  // Update Tests
  // ===========================================================================

  describe('update', () => {
    it('updates an existing entity', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original Title',
      }, store)

      const updated = await executor.update('posts', entity.$id as string, {
        $set: { title: 'Updated Title' },
      }, store)

      expect(updated).not.toBeNull()
      expect(updated!.title).toBe('Updated Title')
      expect(updated!.version).toBe(2)
    })

    it('returns null for non-existent entity', async () => {
      const result = await executor.update('posts', 'posts/nonexistent', {
        $set: { title: 'New Title' },
      }, store)

      expect(result).toBeNull()
    })

    it('supports upsert option', async () => {
      const result = await executor.update('posts', 'posts/new-id', {
        $set: { title: 'Upserted Title' },
      }, store, { upsert: true })

      expect(result).not.toBeNull()
      expect(result!.title).toBe('Upserted Title')
      expect(store.has('posts/new-id')).toBe(true)
    })

    it('applies $inc operator', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        viewCount: 10,
      }, store)

      const updated = await executor.update('posts', entity.$id as string, {
        $inc: { viewCount: 5 },
      }, store)

      expect(updated!.viewCount).toBe(15)
    })

    it('applies $push operator', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        tags: ['initial'],
      }, store)

      const updated = await executor.update('posts', entity.$id as string, {
        $push: { tags: 'new-tag' },
      }, store)

      expect(updated!.tags).toEqual(['initial', 'new-tag'])
    })

    it('applies $unset operator', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        description: 'To be removed',
      }, store)

      const updated = await executor.update('posts', entity.$id as string, {
        $unset: { description: '' },
      }, store)

      expect(updated!.description).toBeUndefined()
    })

    it('supports returnDocument: before', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
      }, store)

      const before = await executor.update('posts', entity.$id as string, {
        $set: { title: 'Updated' },
      }, store, { returnDocument: 'before' })

      expect(before!.title).toBe('Original')
      expect(before!.version).toBe(1)

      // But the store should have the updated version
      const current = store.get(entity.$id as string)
      expect(current!.title).toBe('Updated')
    })

    it('supports returnDocument: after', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
      }, store)

      const after = await executor.update('posts', entity.$id as string, {
        $set: { title: 'Updated' },
      }, store, { returnDocument: 'after' })

      expect(after!.title).toBe('Updated')
      expect(after!.version).toBe(2)
    })
  })

  // ===========================================================================
  // Optimistic Concurrency Tests
  // ===========================================================================

  describe('optimistic concurrency', () => {
    it('succeeds when version matches', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      const updated = await executor.update('posts', entity.$id as string, {
        $set: { title: 'Updated' },
      }, store, { expectedVersion: 1 })

      expect(updated!.version).toBe(2)
    })

    it('throws VersionConflictError when version mismatch', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      // First update succeeds
      await executor.update('posts', entity.$id as string, {
        $set: { title: 'Update 1' },
      }, store)

      // Second update with stale version fails
      await expect(
        executor.update('posts', entity.$id as string, {
          $set: { title: 'Update 2' },
        }, store, { expectedVersion: 1 })
      ).rejects.toThrow(VersionConflictError)
    })

    it('VersionConflictError has expected properties', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      await executor.update('posts', entity.$id as string, {
        $set: { title: 'Update 1' },
      }, store)

      try {
        await executor.update('posts', entity.$id as string, {
          $set: { title: 'Update 2' },
        }, store, { expectedVersion: 1 })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionConflictError)
        expect((error as VersionConflictError).expectedVersion).toBe(1)
        expect((error as VersionConflictError).actualVersion).toBe(2)
      }
    })
  })

  // ===========================================================================
  // Delete Tests
  // ===========================================================================

  describe('delete', () => {
    it('soft deletes an entity', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      const result = await executor.delete('posts', entity.$id as string, store)

      expect(result.deletedCount).toBe(1)

      const deleted = store.get(entity.$id as string)
      expect(deleted).toBeDefined()
      expect(deleted!.deletedAt).toBeInstanceOf(Date)
    })

    it('hard deletes an entity', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      const result = await executor.delete('posts', entity.$id as string, store, { hard: true })

      expect(result.deletedCount).toBe(1)
      expect(store.has(entity.$id as string)).toBe(false)
    })

    it('returns 0 for already soft-deleted entity', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      await executor.delete('posts', entity.$id as string, store)
      const result = await executor.delete('posts', entity.$id as string, store)

      expect(result.deletedCount).toBe(0)
    })

    it('returns 0 for non-existent entity with invalid ID', async () => {
      const result = await executor.delete('posts', 'posts/nonexistent', store)
      expect(result.deletedCount).toBe(0)
    })
  })

  // ===========================================================================
  // Restore Tests
  // ===========================================================================

  describe('restore', () => {
    it('restores a soft-deleted entity', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      await executor.delete('posts', entity.$id as string, store)

      const restored = await executor.restore('posts', entity.$id as string, store)

      expect(restored).not.toBeNull()
      expect(restored!.deletedAt).toBeUndefined()
      expect(restored!.deletedBy).toBeUndefined()
    })

    it('returns null for non-existent entity', async () => {
      const result = await executor.restore('posts', 'posts/nonexistent', store)
      expect(result).toBeNull()
    })

    it('returns entity unchanged if not deleted', async () => {
      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      const restored = await executor.restore('posts', entity.$id as string, store)

      expect(restored!.$id).toBe(entity.$id)
      expect(restored!.version).toBe(1) // Not incremented
    })
  })

  // ===========================================================================
  // Hooks Tests
  // ===========================================================================

  describe('hooks', () => {
    it('calls preMutation hooks', async () => {
      const preMutationHook = vi.fn()
      executor.registerHooks({
        preMutation: [preMutationHook],
      })

      await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      expect(preMutationHook).toHaveBeenCalledTimes(1)
      expect(preMutationHook.mock.calls[0][1]).toBe('create')
    })

    it('calls postMutation hooks with events', async () => {
      const postMutationHook = vi.fn()
      executor.registerHooks({
        postMutation: [postMutationHook],
      })

      await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      expect(postMutationHook).toHaveBeenCalledTimes(1)
      expect(postMutationHook.mock.calls[0][1]).toBe('create')
      expect(postMutationHook.mock.calls[0][3]).toBeInstanceOf(Array) // events
    })

    it('calls preCreate and postCreate hooks', async () => {
      const preCreateHook = vi.fn()
      const postCreateHook = vi.fn()

      executor.registerHooks({
        preCreate: [preCreateHook],
        postCreate: [postCreateHook],
      })

      await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      expect(preCreateHook).toHaveBeenCalledTimes(1)
      expect(postCreateHook).toHaveBeenCalledTimes(1)
    })

    it('calls preUpdate and postUpdate hooks', async () => {
      const preUpdateHook = vi.fn()
      const postUpdateHook = vi.fn()

      executor.registerHooks({
        preUpdate: [preUpdateHook],
        postUpdate: [postUpdateHook],
      })

      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      await executor.update('posts', entity.$id as string, {
        $set: { title: 'Updated' },
      }, store)

      expect(preUpdateHook).toHaveBeenCalledTimes(1)
      expect(postUpdateHook).toHaveBeenCalledTimes(1)
    })

    it('calls preDelete and postDelete hooks', async () => {
      const preDeleteHook = vi.fn()
      const postDeleteHook = vi.fn()

      executor.registerHooks({
        preDelete: [preDeleteHook],
        postDelete: [postDeleteHook],
      })

      const entity = await executor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      await executor.delete('posts', entity.$id as string, store)

      expect(preDeleteHook).toHaveBeenCalledTimes(1)
      expect(postDeleteHook).toHaveBeenCalledTimes(1)
    })
  })

  // ===========================================================================
  // Event Recording Tests
  // ===========================================================================

  describe('event recording', () => {
    it('records events through recordEvent callback', async () => {
      const recordedEvents: MutationEvent[] = []
      const recordingExecutor = new MutationExecutor({
        defaultActor: 'users/test' as EntityId,
        recordEvent: async (event) => {
          recordedEvents.push(event)
        },
      })

      await recordingExecutor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      expect(recordedEvents).toHaveLength(1)
      expect(recordedEvents[0].op).toBe('CREATE')
      expect(recordedEvents[0].target).toMatch(/^posts:/)
    })

    it('records UPDATE event', async () => {
      const recordedEvents: MutationEvent[] = []
      const recordingExecutor = new MutationExecutor({
        defaultActor: 'users/test' as EntityId,
        recordEvent: async (event) => {
          recordedEvents.push(event)
        },
      })

      const entity = await recordingExecutor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      await recordingExecutor.update('posts', entity.$id as string, {
        $set: { title: 'Updated' },
      }, store)

      expect(recordedEvents).toHaveLength(2)
      expect(recordedEvents[1].op).toBe('UPDATE')
      expect(recordedEvents[1].before).not.toBeNull()
      expect(recordedEvents[1].after).not.toBeNull()
    })

    it('records DELETE event', async () => {
      const recordedEvents: MutationEvent[] = []
      const recordingExecutor = new MutationExecutor({
        defaultActor: 'users/test' as EntityId,
        recordEvent: async (event) => {
          recordedEvents.push(event)
        },
      })

      const entity = await recordingExecutor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      }, store)

      await recordingExecutor.delete('posts', entity.$id as string, store)

      expect(recordedEvents).toHaveLength(2)
      expect(recordedEvents[1].op).toBe('DELETE')
      expect(recordedEvents[1].before).not.toBeNull()
      expect(recordedEvents[1].after).toBeNull()
    })
  })

  // ===========================================================================
  // Schema Tests
  // ===========================================================================

  describe('schema integration', () => {
    it('applies schema defaults', async () => {
      const schema: Schema = {
        Post: {
          $ns: 'posts',
          title: 'string!',
          status: 'string = "draft"',
          viewCount: 'number = 0',
        },
      }

      const schemaExecutor = new MutationExecutor({
        schema,
        defaultActor: 'users/test' as EntityId,
      })

      const entity = await schemaExecutor.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello',
      }, store)

      expect(entity.status).toBe('draft')
      expect(entity.viewCount).toBe(0)
    })
  })
})
