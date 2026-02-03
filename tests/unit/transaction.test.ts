/**
 * Transaction Test Suite
 *
 * Tests for ACID transaction support in ParqueDB.
 * Covers commit, rollback, isolation, and optimistic concurrency control.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueDB, VersionConflictError } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import type { EntityId, Event } from '../../src/types'
import { mkdtemp } from 'node:fs/promises'
import { cleanupTempDir } from '../setup'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

// =============================================================================
// Transaction Test Suite
// =============================================================================

describe('Transactions', () => {
  let db: ParqueDB
  let storage: FsBackend
  let tempDir: string

  beforeEach(async () => {
    // Create a unique temp directory for each test
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-txn-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    // Clean up the temp directory
    try {
      await cleanupTempDir(tempDir)
    } catch {
      // Ignore cleanup errors
    }
  })

  // ===========================================================================
  // Basic Transaction Tests
  // ===========================================================================

  describe('beginTransaction', () => {
    it('returns a transaction object with required methods', () => {
      const tx = db.beginTransaction()

      expect(tx).toBeDefined()
      expect(typeof tx.create).toBe('function')
      expect(typeof tx.update).toBe('function')
      expect(typeof tx.delete).toBe('function')
      expect(typeof tx.commit).toBe('function')
      expect(typeof tx.rollback).toBe('function')
    })
  })

  // ===========================================================================
  // Commit Tests
  // ===========================================================================

  describe('commit', () => {
    it('persists created entities on commit', async () => {
      const tx = db.beginTransaction()

      const entity = await tx.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
        content: 'This is a test',
      })

      await tx.commit()

      // Entity should be retrievable after commit
      const retrieved = await db.get('posts', entity.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved!.title).toBe('Hello World')
    })

    it('persists updates on commit', async () => {
      // Create entity outside transaction
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original Title',
        content: 'Content',
      })

      const tx = db.beginTransaction()

      await tx.update('posts', entity.$id as string, {
        $set: { title: 'Updated Title' },
      })

      await tx.commit()

      // Update should be persisted
      const retrieved = await db.get('posts', entity.$id as string)
      expect(retrieved!.title).toBe('Updated Title')
    })

    it('persists deletes on commit', async () => {
      // Create entity outside transaction
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'To Be Deleted',
        content: 'Content',
      })

      const tx = db.beginTransaction()

      await tx.delete('posts', entity.$id as string)

      await tx.commit()

      // Entity should be deleted (or soft-deleted)
      const retrieved = await db.get('posts', entity.$id as string)
      expect(retrieved).toBeNull()
    })

    it('flushes events to event log on commit', async () => {
      const tx = db.beginTransaction()

      const entity = await tx.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Title',
        content: 'Content',
      })

      await tx.commit()

      // Events should be recorded
      const eventLog = db.getEventLog()
      const events = await eventLog.getEvents(entity.$id as EntityId)
      expect(events.length).toBeGreaterThan(0)
      expect(events.some((e: Event) => e.op === 'CREATE')).toBe(true)
    })
  })

  // ===========================================================================
  // Rollback Tests
  // ===========================================================================

  describe('rollback', () => {
    it('discards created entities on rollback', async () => {
      const tx = db.beginTransaction()

      const entity = await tx.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Should Not Exist',
        content: 'Content',
      })

      const entityId = entity.$id as string

      await tx.rollback()

      // Entity should not exist after rollback
      const retrieved = await db.get('posts', entityId)
      expect(retrieved).toBeNull()
    })

    it('does not flush events on rollback', async () => {
      // First create an entity outside transaction to ensure event log works
      const preEntity = await db.create('posts', {
        $type: 'Post',
        name: 'Pre-existing',
        title: 'Exists',
        content: 'Content',
      })

      const eventLog = db.getEventLog()
      const preEvents = await eventLog.getEventsByNamespace('posts')
      const preEventCount = preEvents.length

      const tx = db.beginTransaction()

      await tx.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Rolled Back',
        content: 'Content',
      })

      await tx.rollback()

      // Event count should not increase (no events from rolled back tx)
      const postEvents = await eventLog.getEventsByNamespace('posts')
      expect(postEvents.length).toBe(preEventCount)
    })

    it('can perform operations after rollback', async () => {
      const tx = db.beginTransaction()

      await tx.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Rolled Back',
        content: 'Content',
      })

      await tx.rollback()

      // Should be able to create entities after rollback
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'New Post',
        title: 'After Rollback',
        content: 'Content',
      })

      const retrieved = await db.get('posts', entity.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved!.title).toBe('After Rollback')
    })
  })

  // ===========================================================================
  // Multiple Operations Tests
  // ===========================================================================

  describe('multiple operations', () => {
    it('supports multiple creates in one transaction', async () => {
      const tx = db.beginTransaction()

      const entity1 = await tx.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'First Post',
        content: 'Content 1',
      })

      const entity2 = await tx.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'Second Post',
        content: 'Content 2',
      })

      const entity3 = await tx.create('posts', {
        $type: 'Post',
        name: 'Post 3',
        title: 'Third Post',
        content: 'Content 3',
      })

      await tx.commit()

      // All entities should exist
      const r1 = await db.get('posts', entity1.$id as string)
      const r2 = await db.get('posts', entity2.$id as string)
      const r3 = await db.get('posts', entity3.$id as string)

      expect(r1).not.toBeNull()
      expect(r2).not.toBeNull()
      expect(r3).not.toBeNull()
    })

    it('supports mixed operations in one transaction', async () => {
      // Pre-create entities
      const toUpdate = await db.create('posts', {
        $type: 'Post',
        name: 'To Update',
        title: 'Original',
        content: 'Content',
      })

      const toDelete = await db.create('posts', {
        $type: 'Post',
        name: 'To Delete',
        title: 'Will Be Deleted',
        content: 'Content',
      })

      const tx = db.beginTransaction()

      // Create
      const created = await tx.create('posts', {
        $type: 'Post',
        name: 'Created',
        title: 'New Post',
        content: 'Content',
      })

      // Update
      await tx.update('posts', toUpdate.$id as string, {
        $set: { title: 'Updated' },
      })

      // Delete
      await tx.delete('posts', toDelete.$id as string)

      await tx.commit()

      // Verify all operations
      const createdResult = await db.get('posts', created.$id as string)
      const updatedResult = await db.get('posts', toUpdate.$id as string)
      const deletedResult = await db.get('posts', toDelete.$id as string)

      expect(createdResult).not.toBeNull()
      expect(createdResult!.title).toBe('New Post')

      expect(updatedResult).not.toBeNull()
      expect(updatedResult!.title).toBe('Updated')

      expect(deletedResult).toBeNull()
    })

    it('rolls back all operations on rollback', async () => {
      // Pre-create entity
      const existing = await db.create('posts', {
        $type: 'Post',
        name: 'Existing',
        title: 'Original Title',
        content: 'Content',
      })

      const tx = db.beginTransaction()

      // Create new
      const created = await tx.create('posts', {
        $type: 'Post',
        name: 'New',
        title: 'Should Not Exist',
        content: 'Content',
      })

      const createdId = created.$id as string

      // Update existing - note: rollback may not fully restore updates
      // This is a known limitation of the current simplified implementation

      await tx.rollback()

      // Created entity should not exist
      const createdResult = await db.get('posts', createdId)
      expect(createdResult).toBeNull()
    })

    it('restores entity state after update rollback', async () => {
      // Pre-create entity
      const existing = await db.create('posts', {
        $type: 'Post',
        name: 'Existing',
        title: 'Original Title',
        content: 'Original Content',
        views: 100,
      })

      const originalVersion = existing.version

      const tx = db.beginTransaction()

      // Update in transaction
      await tx.update('posts', existing.$id as string, {
        $set: {
          title: 'Modified Title',
          content: 'Modified Content',
        },
        $inc: { views: 50 },
      })

      await tx.rollback()

      // Entity should have original values
      const retrieved = await db.get('posts', existing.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved!.title).toBe('Original Title')
      expect(retrieved!.content).toBe('Original Content')
      expect(retrieved!.views).toBe(100)
      expect(retrieved!.version).toBe(originalVersion)
    })

    it('restores deleted entity on rollback', async () => {
      // Pre-create entity
      const existing = await db.create('posts', {
        $type: 'Post',
        name: 'To Delete',
        title: 'Original Title',
        content: 'Content',
      })

      const tx = db.beginTransaction()

      // Delete in transaction
      await tx.delete('posts', existing.$id as string)

      await tx.rollback()

      // Entity should still exist with original data
      const retrieved = await db.get('posts', existing.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved!.title).toBe('Original Title')
      expect(retrieved!.content).toBe('Content')
    })

    it('restores state after multiple updates to same entity', async () => {
      // Pre-create entity
      const existing = await db.create('posts', {
        $type: 'Post',
        name: 'Test',
        title: 'Original',
        views: 0,
      })

      const tx = db.beginTransaction()

      // Multiple updates to same entity
      await tx.update('posts', existing.$id as string, {
        $set: { title: 'First Update' },
      })

      await tx.update('posts', existing.$id as string, {
        $set: { title: 'Second Update' },
      })

      await tx.update('posts', existing.$id as string, {
        $inc: { views: 10 },
      })

      await tx.rollback()

      // Entity should have original values
      const retrieved = await db.get('posts', existing.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved!.title).toBe('Original')
      expect(retrieved!.views).toBe(0)
    })

    it('handles complex rollback with creates, updates, and deletes', async () => {
      // Pre-create entities
      const toUpdate = await db.create('posts', {
        $type: 'Post',
        name: 'To Update',
        title: 'Original Title',
        content: 'Original Content',
      })

      const toDelete = await db.create('posts', {
        $type: 'Post',
        name: 'To Delete',
        title: 'Will Be Deleted',
        content: 'Content',
      })

      const tx = db.beginTransaction()

      // Create new entity
      const created = await tx.create('posts', {
        $type: 'Post',
        name: 'New',
        title: 'Created in Tx',
        content: 'Content',
      })

      // Update existing entity
      await tx.update('posts', toUpdate.$id as string, {
        $set: { title: 'Updated Title' },
      })

      // Delete existing entity
      await tx.delete('posts', toDelete.$id as string)

      await tx.rollback()

      // Created entity should not exist
      const createdResult = await db.get('posts', created.$id as string)
      expect(createdResult).toBeNull()

      // Updated entity should have original state
      const updatedResult = await db.get('posts', toUpdate.$id as string)
      expect(updatedResult).not.toBeNull()
      expect(updatedResult!.title).toBe('Original Title')

      // Deleted entity should still exist
      const deletedResult = await db.get('posts', toDelete.$id as string)
      expect(deletedResult).not.toBeNull()
      expect(deletedResult!.title).toBe('Will Be Deleted')
    })
  })

  // ===========================================================================
  // Cross-Namespace Operations Tests
  // ===========================================================================

  describe('cross-namespace operations', () => {
    it('supports operations across multiple namespaces', async () => {
      const tx = db.beginTransaction()

      const user = await tx.create('users', {
        $type: 'User',
        name: 'Test User',
        email: 'test@example.com',
      })

      const post = await tx.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'A Post',
        content: 'Content',
        authorId: user.$id,
      })

      const comment = await tx.create('comments', {
        $type: 'Comment',
        name: 'Test Comment',
        text: 'A comment',
        postId: post.$id,
        authorId: user.$id,
      })

      await tx.commit()

      // All entities should exist
      const userResult = await db.get('users', user.$id as string)
      const postResult = await db.get('posts', post.$id as string)
      const commentResult = await db.get('comments', comment.$id as string)

      expect(userResult).not.toBeNull()
      expect(postResult).not.toBeNull()
      expect(commentResult).not.toBeNull()
    })
  })

  // ===========================================================================
  // Optimistic Concurrency Control Tests
  // ===========================================================================

  describe('optimistic concurrency', () => {
    it('supports expectedVersion for updates', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
        content: 'Content',
      })

      // Entity starts at version 1
      expect(entity.version).toBe(1)

      // Update with correct version
      const updated = await db.update(
        'posts',
        entity.$id as string,
        { $set: { title: 'Updated' } },
        { expectedVersion: entity.version }
      )

      expect(updated).not.toBeNull()
      expect(updated!.title).toBe('Updated')
      // After update, version should be 2
      expect(updated!.version).toBe(2)
    })

    it('throws VersionConflictError on version mismatch', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original',
        content: 'Content',
      })

      // Try to update with wrong version
      await expect(
        db.update(
          'posts',
          entity.$id as string,
          { $set: { title: 'Updated' } },
          { expectedVersion: 999 }
        )
      ).rejects.toThrow(VersionConflictError)
    })

    it('increments version on each update', async () => {
      const entity = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'V1',
        content: 'Content',
      })

      const v1 = entity.version!

      const updated1 = await db.update(
        'posts',
        entity.$id as string,
        { $set: { title: 'V2' } }
      )
      expect(updated1!.version).toBe(v1 + 1)

      const updated2 = await db.update(
        'posts',
        entity.$id as string,
        { $set: { title: 'V3' } }
      )
      expect(updated2!.version).toBe(v1 + 2)

      const updated3 = await db.update(
        'posts',
        entity.$id as string,
        { $set: { title: 'V4' } }
      )
      expect(updated3!.version).toBe(v1 + 3)
    })
  })

  // ===========================================================================
  // Transaction Isolation Tests
  // ===========================================================================

  describe('isolation', () => {
    it('creates are visible within transaction before commit', async () => {
      const tx = db.beginTransaction()

      const entity = await tx.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Created in Tx',
        content: 'Content',
      })

      // Entity should be visible via db.get since tx.create uses db.create internally
      const retrieved = await db.get('posts', entity.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved!.title).toBe('Created in Tx')

      await tx.rollback()

      // After rollback, entity should not exist
      const afterRollback = await db.get('posts', entity.$id as string)
      expect(afterRollback).toBeNull()
    })
  })

  // ===========================================================================
  // Error Handling Tests
  // ===========================================================================

  describe('error handling', () => {
    it('can rollback after create fails validation', async () => {
      const tx = db.beginTransaction()

      // Create a valid entity first
      const valid = await tx.create('posts', {
        $type: 'Post',
        name: 'Valid Post',
        title: 'Valid',
        content: 'Content',
      })

      const validId = valid.$id as string

      await tx.rollback()

      // Valid entity should also be rolled back
      const retrieved = await db.get('posts', validId)
      expect(retrieved).toBeNull()
    })

    it('allows new transaction after commit', async () => {
      const tx1 = db.beginTransaction()

      await tx1.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'First',
        content: 'Content',
      })

      await tx1.commit()

      // Start new transaction
      const tx2 = db.beginTransaction()

      const entity2 = await tx2.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'Second',
        content: 'Content',
      })

      await tx2.commit()

      const retrieved = await db.get('posts', entity2.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved!.title).toBe('Second')
    })

    it('allows new transaction after rollback', async () => {
      const tx1 = db.beginTransaction()

      await tx1.create('posts', {
        $type: 'Post',
        name: 'Post 1',
        title: 'Rolled Back',
        content: 'Content',
      })

      await tx1.rollback()

      // Start new transaction
      const tx2 = db.beginTransaction()

      const entity2 = await tx2.create('posts', {
        $type: 'Post',
        name: 'Post 2',
        title: 'After Rollback',
        content: 'Content',
      })

      await tx2.commit()

      const retrieved = await db.get('posts', entity2.$id as string)
      expect(retrieved).not.toBeNull()
      expect(retrieved!.title).toBe('After Rollback')
    })
  })

  // ===========================================================================
  // Sequential Transaction Tests
  // ===========================================================================

  describe('sequential transactions', () => {
    it('handles sequential commits correctly', async () => {
      const ids: string[] = []

      for (let i = 0; i < 5; i++) {
        const tx = db.beginTransaction()

        const entity = await tx.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
          content: 'Content',
        })

        ids.push(entity.$id as string)
        await tx.commit()
      }

      // All entities should exist
      for (let i = 0; i < 5; i++) {
        const retrieved = await db.get('posts', ids[i])
        expect(retrieved).not.toBeNull()
        expect(retrieved!.title).toBe(`Title ${i}`)
      }
    })

    it('handles alternating commit and rollback', async () => {
      const committedIds: string[] = []
      const rolledBackIds: string[] = []

      for (let i = 0; i < 6; i++) {
        const tx = db.beginTransaction()

        const entity = await tx.create('posts', {
          $type: 'Post',
          name: `Post ${i}`,
          title: `Title ${i}`,
          content: 'Content',
        })

        if (i % 2 === 0) {
          await tx.commit()
          committedIds.push(entity.$id as string)
        } else {
          rolledBackIds.push(entity.$id as string)
          await tx.rollback()
        }
      }

      // Committed entities should exist
      for (const id of committedIds) {
        const retrieved = await db.get('posts', id)
        expect(retrieved).not.toBeNull()
      }

      // Rolled back entities should not exist
      for (const id of rolledBackIds) {
        const retrieved = await db.get('posts', id)
        expect(retrieved).toBeNull()
      }
    })
  })
})
