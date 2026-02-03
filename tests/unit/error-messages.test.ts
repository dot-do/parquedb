/**
 * Tests for improved error messages with context
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { ParqueDBImpl } from '../../src/ParqueDB/core'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import {
  VersionConflictError,
  EntityNotFoundError,
  ValidationError,
  RelationshipError,
  EventError,
} from '../../src/ParqueDB/types'
import type { Schema } from '../../src/types'

describe('Error Messages with Context', () => {
  let db: ParqueDBImpl
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
    db = new ParqueDBImpl({ storage })
  })

  describe('ValidationError', () => {
    it('should include field name and namespace for missing required fields', async () => {
      const schema: Schema = {
        User: {
          email: 'string!',
          name: 'string',
        },
      }
      db.registerSchema(schema)

      try {
        await db.create('users', {
          $type: 'User',
          name: 'John Doe',
          // email is missing (required)
        }, { skipValidation: false })
        expect.fail('Should have thrown an error')
      } catch (error) {
        // Note: SchemaValidator throws SchemaValidationError, which is different from our ValidationError
        // Our ValidationError is used in the core validation logic
        expect((error as Error).message).toContain('email')
        expect((error as Error).message).toContain('required')
      }
    })

    it('should include type information for type mismatches', async () => {
      const schema: Schema = {
        User: {
          age: 'number',
        },
      }
      db.registerSchema(schema)

      try {
        await db.create('users', {
          $type: 'User',
          age: 'thirty', // should be number
        }, { skipValidation: false })
        expect.fail('Should have thrown an error')
      } catch (error) {
        // SchemaValidator handles type validation
        expect((error as Error).message).toContain('age')
      }
    })

    it('should provide context for invalid relationship formats', async () => {
      const schema: Schema = {
        Post: {
          title: 'string',
          author: 'string', // Simple string field for testing
        },
      }
      db.registerSchema(schema)

      try {
        await db.create('posts', {
          $type: 'Post',
          author: {
            'Invalid Format': 'no-slash-here',
          },
        }, { skipValidation: false })
        expect.fail('Should have thrown an error')
      } catch (error) {
        // Validation should catch type mismatch
        expect((error as Error).message).toContain('author')
      }
    })
  })

  describe('VersionConflictError', () => {
    it('should include namespace and entity ID in update conflicts', async () => {
      const entity = await db.create('posts', {
        title: 'Test Post',
      })

      const id = entity.$id.split('/')[1]

      try {
        await db.update('posts', id, { $set: { title: 'Updated' } }, { expectedVersion: 999 })
        expect.fail('Should have thrown VersionConflictError')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionConflictError)
        const err = error as VersionConflictError
        expect(err.expectedVersion).toBe(999)
        expect(err.actualVersion).toBe(1)
        expect(err.namespace).toBe('posts')
        expect(err.entityId).toBe(id)
        expect(err.message).toContain('posts')
        expect(err.message).toContain(id)
        expect(err.message).toContain('999')
        expect(err.message).toContain('1')
      }
    })

    it('should include namespace and entity ID in delete conflicts', async () => {
      const entity = await db.create('posts', {
        title: 'Test Post',
      })

      const id = entity.$id.split('/')[1]

      try {
        await db.delete('posts', id, { expectedVersion: 5 })
        expect.fail('Should have thrown VersionConflictError')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionConflictError)
        const err = error as VersionConflictError
        expect(err.expectedVersion).toBe(5)
        expect(err.actualVersion).toBe(1)
        expect(err.namespace).toBe('posts')
        expect(err.entityId).toBe(id)
      }
    })

    it('should handle non-existent entity with expected version > 1', async () => {
      try {
        await db.update('posts', 'nonexistent123', { $set: { title: 'Updated' } }, { expectedVersion: 2 })
        expect.fail('Should have thrown VersionConflictError')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionConflictError)
        const err = error as VersionConflictError
        expect(err.expectedVersion).toBe(2)
        expect(err.actualVersion).toBeUndefined()
        expect(err.namespace).toBe('posts')
        expect(err.entityId).toBe('nonexistent123')
      }
    })
  })

  describe('EntityNotFoundError', () => {
    it('should include namespace and entity ID', async () => {
      try {
        const snapshotManager = db.getSnapshotManager()
        await snapshotManager.createSnapshot('users/abc123' as any)
        expect.fail('Should have thrown EntityNotFoundError')
      } catch (error) {
        expect(error).toBeInstanceOf(EntityNotFoundError)
        const err = error as EntityNotFoundError
        expect(err.namespace).toBe('users')
        expect(err.entityId).toBe('abc123')
        expect(err.message).toBe('Entity not found: users/abc123')
      }
    })
  })

  describe('RelationshipError', () => {
    it('should include context for undefined relationships in schema', async () => {
      const schema: Schema = {
        Post: {
          title: 'string',
        },
      }
      db.registerSchema(schema)

      const post = await db.create('posts', {
        $type: 'Post',
        title: 'Test',
      })

      const user = await db.create('users', {
        name: 'Alice',
      })

      const postId = post.$id.split('/')[1]

      try {
        await db.update('posts', postId, {
          $link: {
            author: user.$id, // 'author' not defined in schema
          },
        })
        expect.fail('Should have thrown RelationshipError')
      } catch (error) {
        expect(error).toBeInstanceOf(RelationshipError)
        const err = error as RelationshipError
        expect(err.operation).toBe('Link')
        expect(err.namespace).toBe('Post')
        expect(err.relationshipName).toBe('author')
        expect(err.message).toContain('Link')
        expect(err.message).toContain('Post')
        expect(err.message).toContain('author')
        expect(err.message).toContain('not defined')
      }
    })

    it('should include target entity ID for non-existent targets', async () => {
      // Skip schema registration to avoid schema parsing issues
      // Test relationship errors directly
      const post = await db.create('posts', {
        $type: 'Post',
        title: 'Test',
      })

      const postId = post.$id.split('/')[1]

      try {
        await db.update('posts', postId, {
          $link: {
            author: 'users/nonexistent',
          },
        })
        expect.fail('Should have thrown RelationshipError')
      } catch (error) {
        expect(error).toBeInstanceOf(RelationshipError)
        const err = error as RelationshipError
        expect(err.operation).toBe('Link')
        expect(err.namespace).toBe('Post')
        expect(err.relationshipName).toBe('author')
        expect(err.targetId).toBe('users/nonexistent')
        expect(err.message).toContain('users/nonexistent')
        expect(err.message).toContain('does not exist')
      }
    })

    it('should include context when linking to deleted entity', async () => {
      // Skip schema registration to avoid schema parsing issues
      const user = await db.create('users', { name: 'Alice' })
      await db.delete('users', user.$id.split('/')[1])

      const post = await db.create('posts', {
        $type: 'Post',
        title: 'Test',
      })

      const postId = post.$id.split('/')[1]

      try {
        await db.update('posts', postId, {
          $link: {
            author: user.$id,
          },
        })
        expect.fail('Should have thrown RelationshipError')
      } catch (error) {
        expect(error).toBeInstanceOf(RelationshipError)
        const err = error as RelationshipError
        expect(err.operation).toBe('Link')
        expect(err.targetId).toBe(user.$id)
        expect(err.message).toContain('deleted')
      }
    })
  })

  describe('EventError', () => {
    it('should include event ID for missing events', async () => {
      const eventLog = db.getEventLog()

      try {
        await eventLog.getRawEvent('nonexistent-event-id')
        expect.fail('Should have thrown EventError')
      } catch (error) {
        expect(error).toBeInstanceOf(EventError)
        const err = error as EventError
        expect(err.operation).toBe('Get event')
        expect(err.eventId).toBe('nonexistent-event-id')
        expect(err.message).toContain('nonexistent-event-id')
        expect(err.message).toContain('Event not found')
      }
    })

    it('should include snapshot ID for missing snapshots', async () => {
      const snapshotManager = db.getSnapshotManager()

      try {
        await snapshotManager.getRawSnapshot('nonexistent-snapshot')
        expect.fail('Should have thrown EventError')
      } catch (error) {
        expect(error).toBeInstanceOf(EventError)
        const err = error as EventError
        expect(err.operation).toBe('Get snapshot')
        expect(err.snapshotId).toBe('nonexistent-snapshot')
        expect(err.message).toContain('nonexistent-snapshot')
        expect(err.message).toContain('Snapshot not found')
      }
    })

    it('should include entity ID for time-travel errors', async () => {
      const entity = await db.create('posts', { title: 'Test' })
      const futureTime = new Date(Date.now() + 1000000)

      try {
        await db.revert(entity.$id, futureTime)
        expect.fail('Should have thrown EventError')
      } catch (error) {
        expect(error).toBeInstanceOf(EventError)
        const err = error as EventError
        expect(err.operation).toBe('Revert entity')
        expect(err.entityId).toBe(entity.$id)
        expect(err.message).toContain('future')
      }
    })

    it('should provide context when entity did not exist at target time', async () => {
      const entity = await db.create('posts', { title: 'Test' })
      const pastTime = new Date(Date.now() - 1000000)

      try {
        await db.revert(entity.$id, pastTime)
        expect.fail('Should have thrown EventError')
      } catch (error) {
        expect(error).toBeInstanceOf(EventError)
        const err = error as EventError
        expect(err.operation).toBe('Revert entity')
        expect(err.entityId).toBe(entity.$id)
        expect(err.message).toContain('did not exist')
      }
    })

    it('should include entity ID when creating snapshot of deleted entity', async () => {
      const entity = await db.create('posts', { title: 'Test' })
      await db.delete('posts', entity.$id.split('/')[1])

      const snapshotManager = db.getSnapshotManager()

      try {
        await snapshotManager.createSnapshot(entity.$id)
        expect.fail('Should have thrown EventError')
      } catch (error) {
        expect(error).toBeInstanceOf(EventError)
        const err = error as EventError
        expect(err.operation).toBe('Create snapshot')
        expect(err.entityId).toBe(entity.$id)
        expect(err.message).toContain('deleted')
      }
    })
  })

  describe('Error message readability', () => {
    it('should produce human-readable version conflict messages', async () => {
      const entity = await db.create('posts', { title: 'Test' })
      const id = entity.$id.split('/')[1]

      try {
        await db.update('posts', id, { $set: { title: 'Updated' } }, { expectedVersion: 3 })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionConflictError)
        // Expected: "Version conflict: expected 3, got 1 for posts/abc123"
        expect((error as Error).message).toMatch(/Version conflict: expected 3, got 1 for posts\/\w+/)
      }
    })

    it('should produce human-readable validation messages', async () => {
      const schema: Schema = {
        User: {
          email: 'string!',
        },
      }
      db.registerSchema(schema)

      try {
        await db.create('users', {
          $type: 'User',
          name: 'Test',
        }, { skipValidation: false })
        expect.fail('Should have thrown')
      } catch (error) {
        // Schema validator will handle this
        expect((error as Error).message).toContain('email')
      }
    })

    it('should produce human-readable relationship error messages', async () => {
      const post = await db.create('posts', {
        $type: 'Post',
        title: 'Test',
      })

      const postId = post.$id.split('/')[1]

      try {
        await db.update('posts', postId, {
          $link: {
            author: 'users/missing123',
          },
        })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(RelationshipError)
        // Expected: "Link failed for Post/xyz relationship 'author' with target users/missing123: Target entity does not exist"
        expect((error as Error).message).toContain('Link failed')
        expect((error as Error).message).toContain('Post')
        expect((error as Error).message).toContain('author')
        expect((error as Error).message).toContain('users/missing123')
      }
    })
  })
})
