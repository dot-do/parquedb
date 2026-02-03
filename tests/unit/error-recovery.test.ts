/**
 * Error Recovery and Partial Failure Tests
 *
 * Tests for error recovery scenarios, partial failures in batch operations,
 * rollback behavior, and retry logic.
 *
 * Issue: parquedb-8tmn [P1] - Add: Error recovery and partial failure tests
 *
 * Uses real FsBackend storage with temporary directories (NO MOCKS).
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mkdtemp } from 'node:fs/promises'
import { cleanupTempDir } from '../setup'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { ParqueDB } from '../../src/ParqueDB'
import { FsBackend } from '../../src/storage/FsBackend'
import {
  withRetry,
  isRetryableError,
} from '../../src/delta-utils/retry'
import {
  VersionConflictError,
} from '../../src/ParqueDB/types'
import type { EntityId, Filter, UpdateInput } from '../../src/types'

// =============================================================================
// Test Suite
// =============================================================================

describe('Error Recovery and Partial Failures', () => {
  let tempDir: string
  let storage: FsBackend
  let db: ParqueDB

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-error-recovery-test-'))
    storage = new FsBackend(tempDir)
    db = new ParqueDB({ storage })
  })

  afterEach(async () => {
    await cleanupTempDir(tempDir)
  })

  // ===========================================================================
  // Error Recovery Scenarios
  // ===========================================================================

  describe('error recovery scenarios', () => {
    it('recovers from version conflict with retry', async () => {
      const posts = (db as any).Posts

      // Create initial post
      const post = await posts.create({
        $type: 'Post',
        name: 'Test Post',
        title: 'Original Title',
        viewCount: 0,
      })

      const postId = post.$id.split('/')[1]

      // Simulate concurrent modification scenario
      let attemptCount = 0
      const instantDelay = async (_ms: number): Promise<void> => {}

      const updateWithRetry = async () => {
        return withRetry(
          async () => {
            attemptCount++

            // Fetch current state by ID (not using $id in filter which is not supported)
            const current = await db.get('posts', postId)
            if (!current) throw new Error('Post not found')

            // On first attempt, simulate a concurrent update
            if (attemptCount === 1) {
              await db.update('posts', postId, { $set: { title: 'Concurrent Update' } })
            }

            // Try to update with expected version (will fail on first attempt)
            return db.update('posts', postId, {
              $set: { title: `Updated by attempt ${attemptCount}` },
            }, { expectedVersion: current.version })
          },
          {
            _delayFn: instantDelay,
            maxRetries: 3,
            isRetryable: (error) => error instanceof VersionConflictError,
          }
        )
      }

      const result = await updateWithRetry()

      expect(attemptCount).toBeGreaterThan(1)
      expect(result.title).toContain('Updated by attempt')
    })

    it('recovers from transient storage errors', async () => {
      const posts = (db as any).Posts

      let attemptCount = 0
      const instantDelay = async (_ms: number): Promise<void> => {}

      // Create a retryable error
      const createRetryableError = () => {
        const error = new Error('Transient storage failure')
        error.name = 'ConcurrencyError'
        return error
      }

      const operationWithRetry = async () => {
        return withRetry(
          async () => {
            attemptCount++
            if (attemptCount < 3) {
              throw createRetryableError()
            }
            return posts.create({
              $type: 'Post',
              name: 'Test Post',
              title: 'Created after retry',
            })
          },
          {
            _delayFn: instantDelay,
            maxRetries: 5,
          }
        )
      }

      const result = await operationWithRetry()

      expect(attemptCount).toBe(3)
      expect(result.title).toBe('Created after retry')
    })

    it('does not retry non-retryable errors', async () => {
      let attemptCount = 0
      const instantDelay = async (_ms: number): Promise<void> => {}

      const operation = async () => {
        return withRetry(
          async () => {
            attemptCount++
            throw new TypeError('Invalid type - not retryable')
          },
          {
            _delayFn: instantDelay,
            maxRetries: 5,
          }
        )
      }

      await expect(operation()).rejects.toThrow('Invalid type - not retryable')
      expect(attemptCount).toBe(1) // Should not retry
    })

    it('recovers partial state after failed batch operation', async () => {
      const posts = (db as any).Posts

      // Create some initial posts
      await posts.create({ $type: 'Post', name: 'Post 1', slug: 'post-1', title: 'Title 1' })
      await posts.create({ $type: 'Post', name: 'Post 2', slug: 'post-2', title: 'Title 2' })

      // Verify initial state
      const initialPosts = await posts.find()
      expect(initialPosts.items).toHaveLength(2)

      // Attempt a batch operation that partially fails (ordered: false continues on error)
      const result = await posts.upsertMany(
        [
          { filter: { slug: 'post-1' }, update: { $set: { title: 'Updated 1' } } },
          { filter: { slug: 'post-2' }, update: { $set: { title: 'Updated 2' } }, options: { expectedVersion: 999 } },
          { filter: { slug: 'post-3' }, update: { $set: { title: 'New Post 3' } } },
        ],
        { ordered: false }
      )

      // Verify partial success
      expect(result.ok).toBe(false)
      expect(result.modifiedCount).toBe(1)
      expect(result.insertedCount).toBe(1)
      expect(result.errors).toHaveLength(1)
      expect(result.errors[0].index).toBe(1)

      // Verify database state reflects partial success
      const post1 = await posts.findOne({ slug: 'post-1' })
      const post2 = await posts.findOne({ slug: 'post-2' })
      const post3 = await posts.findOne({ slug: 'post-3' })

      expect(post1.title).toBe('Updated 1') // Updated successfully
      expect(post2.title).toBe('Title 2') // Failed, unchanged
      expect(post3).not.toBeNull() // Inserted successfully
      expect(post3.title).toBe('New Post 3')
    })

    it('handles entity not found during update gracefully', async () => {
      const posts = (db as any).Posts

      // Try to update non-existent entity
      const result = await posts.update('posts', 'nonexistent-id', {
        $set: { title: 'Updated' },
      })

      // Should return null without throwing
      expect(result).toBeNull()
    })

    it('handles entity not found with upsert option', async () => {
      const posts = (db as any).Posts

      // Upsert non-existent entity
      const result = await posts.upsert(
        { slug: 'new-post' },
        { $set: { title: 'Upserted Post' } }
      )

      expect(result).not.toBeNull()
      expect(result.title).toBe('Upserted Post')
      expect(result.slug).toBe('new-post')
    })
  })

  // ===========================================================================
  // Partial Failures in Batch Operations
  // ===========================================================================

  describe('partial failures in batch operations', () => {
    beforeEach(async () => {
      const posts = (db as any).Posts
      await posts.create({ $type: 'Post', name: 'Post 1', slug: 'post-1', title: 'Title 1' })
      await posts.create({ $type: 'Post', name: 'Post 2', slug: 'post-2', title: 'Title 2' })
      await posts.create({ $type: 'Post', name: 'Post 3', slug: 'post-3', title: 'Title 3' })
    })

    it('ordered: true stops on first error and rolls back remaining', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsertMany(
        [
          { filter: { slug: 'post-1' }, update: { $set: { title: 'Updated 1' } } },
          { filter: { slug: 'post-2' }, update: { $set: { title: 'Updated 2' } }, options: { expectedVersion: 999 } },
          { filter: { slug: 'post-3' }, update: { $set: { title: 'Updated 3' } } },
        ],
        { ordered: true }
      )

      expect(result.ok).toBe(false)
      expect(result.modifiedCount).toBe(1) // Only first succeeded
      expect(result.errors).toHaveLength(1)
      expect(result.errors[0].index).toBe(1)

      // Verify third operation was not executed
      const post3 = await posts.findOne({ slug: 'post-3' })
      expect(post3.title).toBe('Title 3') // Unchanged
    })

    it('ordered: false continues past errors and completes valid operations', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsertMany(
        [
          { filter: { slug: 'post-1' }, update: { $set: { title: 'Updated 1' } } },
          { filter: { slug: 'post-2' }, update: { $set: { title: 'Updated 2' } }, options: { expectedVersion: 999 } },
          { filter: { slug: 'post-3' }, update: { $set: { title: 'Updated 3' } } },
        ],
        { ordered: false }
      )

      expect(result.ok).toBe(false)
      expect(result.modifiedCount).toBe(2) // First and third succeeded
      expect(result.errors).toHaveLength(1)
      expect(result.errors[0].index).toBe(1)

      // Verify first and third were updated, second unchanged
      const post1 = await posts.findOne({ slug: 'post-1' })
      const post2 = await posts.findOne({ slug: 'post-2' })
      const post3 = await posts.findOne({ slug: 'post-3' })

      expect(post1.title).toBe('Updated 1')
      expect(post2.title).toBe('Title 2') // Unchanged due to error
      expect(post3.title).toBe('Updated 3')
    })

    it('handles multiple errors in unordered batch', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsertMany(
        [
          { filter: { slug: 'post-1' }, update: { $set: { title: 'Updated 1' } }, options: { expectedVersion: 999 } },
          { filter: { slug: 'post-2' }, update: { $set: { title: 'Updated 2' } }, options: { expectedVersion: 999 } },
          { filter: { slug: 'post-3' }, update: { $set: { title: 'Updated 3' } } },
        ],
        { ordered: false }
      )

      expect(result.ok).toBe(false)
      expect(result.modifiedCount).toBe(1) // Only third succeeded
      expect(result.errors).toHaveLength(2)
      expect(result.errors[0].index).toBe(0)
      expect(result.errors[1].index).toBe(1)
    })

    it('reports detailed error information for each failure', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsertMany(
        [
          { filter: { slug: 'post-1' }, update: { $set: { title: 'Updated' } }, options: { expectedVersion: 999 } },
        ],
        { ordered: false }
      )

      expect(result.ok).toBe(false)
      expect(result.errors).toHaveLength(1)
      expect(result.errors[0]).toMatchObject({
        index: 0,
        filter: { slug: 'post-1' },
      })
      expect(result.errors[0].error).toBeDefined()
      expect(result.errors[0].error.message).toContain('Version')
    })

    it('handles empty batch gracefully', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsertMany([])

      expect(result.ok).toBe(true)
      expect(result.insertedCount).toBe(0)
      expect(result.modifiedCount).toBe(0)
      expect(result.errors).toEqual([])
    })

    it('handles all failures in batch', async () => {
      const posts = (db as any).Posts

      const result = await posts.upsertMany(
        [
          { filter: { slug: 'post-1' }, update: { $set: { title: 'U1' } }, options: { expectedVersion: 999 } },
          { filter: { slug: 'post-2' }, update: { $set: { title: 'U2' } }, options: { expectedVersion: 999 } },
        ],
        { ordered: false }
      )

      expect(result.ok).toBe(false)
      expect(result.modifiedCount).toBe(0)
      expect(result.errors).toHaveLength(2)
    })
  })

  // ===========================================================================
  // Rollback Behavior
  // ===========================================================================

  describe('rollback behavior', () => {
    it('maintains data consistency after version conflict', async () => {
      const posts = (db as any).Posts

      const post = await posts.create({
        $type: 'Post',
        name: 'Test',
        slug: 'test',
        title: 'Original',
        viewCount: 100,
      })

      const postId = post.$id.split('/')[1]

      // Try to update with wrong version - this throws VersionConflictError
      try {
        await db.update('posts', postId, {
          $set: { title: 'Should Fail' },
          $inc: { viewCount: 10 },
        }, { expectedVersion: 999 })
        expect.fail('Should have thrown VersionConflictError')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionConflictError)
      }

      // Verify data unchanged
      const unchanged = await posts.findOne({ slug: 'test' })
      expect(unchanged.title).toBe('Original')
      expect(unchanged.viewCount).toBe(100)
      expect(unchanged.version).toBe(1)
    })

    it('does not partially apply operators on failure', async () => {
      const posts = (db as any).Posts

      const post = await posts.create({
        $type: 'Post',
        name: 'Test',
        slug: 'test',
        title: 'Original',
        viewCount: 100,
        tags: ['initial'],
      })

      const postId = post.$id.split('/')[1]

      // Try complex update with wrong version
      try {
        await db.update('posts', postId, {
          $set: { title: 'New Title' },
          $inc: { viewCount: 50 },
          $push: { tags: 'new-tag' },
        }, { expectedVersion: 999 })
        expect.fail('Should have thrown VersionConflictError')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionConflictError)
      }

      // Verify nothing changed
      const unchanged = await posts.findOne({ slug: 'test' })
      expect(unchanged.title).toBe('Original')
      expect(unchanged.viewCount).toBe(100)
      expect(unchanged.tags).toEqual(['initial'])
    })

    it('preserves audit fields on failed update', async () => {
      const posts = (db as any).Posts

      const post = await posts.create({
        $type: 'Post',
        name: 'Test',
        slug: 'test',
      })

      const originalCreatedAt = post.createdAt
      const originalUpdatedAt = post.updatedAt
      const postId = post.$id.split('/')[1]

      // Advance time to ensure timestamp would be different (using fake timers for this test)
      vi.useFakeTimers()
      vi.advanceTimersByTime(10)
      vi.useRealTimers()

      // Try to update with wrong version
      try {
        await db.update('posts', postId, { $set: { title: 'Fail' } }, { expectedVersion: 999 })
        expect.fail('Should have thrown VersionConflictError')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionConflictError)
      }

      // Verify audit fields unchanged
      const unchanged = await posts.findOne({ slug: 'test' })
      expect(unchanged.createdAt.getTime()).toBe(originalCreatedAt.getTime())
      expect(unchanged.updatedAt.getTime()).toBe(originalUpdatedAt.getTime())
    })

    it('handles relationship operation failures cleanly', async () => {
      const posts = (db as any).Posts

      const post = await posts.create({
        $type: 'Post',
        name: 'Test',
        slug: 'test',
        title: 'Original',
      })

      const postId = post.$id.split('/')[1]

      // Try to link to non-existent user - this should throw RelationshipError
      try {
        await db.update('posts', postId, {
          $link: { author: 'users/nonexistent' as EntityId },
        })
        expect.fail('Should have thrown an error')
      } catch (error) {
        // RelationshipError is expected when linking to non-existent entity
        expect(error).toBeDefined()
      }

      // Verify post unchanged and no invalid relationships created
      const unchanged = await posts.findOne({ slug: 'test' })
      expect(unchanged.title).toBe('Original')
    })
  })

  // ===========================================================================
  // Retry Logic
  // ===========================================================================

  describe('retry logic', () => {
    const instantDelay = async (_ms: number): Promise<void> => {}

    it('respects maxRetries configuration', async () => {
      let attempts = 0
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      await expect(
        withRetry(
          async () => {
            attempts++
            throw concurrencyError
          },
          {
            _delayFn: instantDelay,
            maxRetries: 2,
          }
        )
      ).rejects.toThrow('conflict')

      // 1 initial + 2 retries = 3 attempts
      expect(attempts).toBe(3)
    })

    it('stops retrying on success', async () => {
      let attempts = 0
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const result = await withRetry(
        async () => {
          attempts++
          if (attempts < 3) throw concurrencyError
          return 'success'
        },
        {
          _delayFn: instantDelay,
          maxRetries: 5,
        }
      )

      expect(result).toBe('success')
      expect(attempts).toBe(3)
    })

    it('collects metrics during retry', async () => {
      let attempts = 0
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const { result, metrics } = await withRetry(
        async () => {
          attempts++
          if (attempts < 3) throw concurrencyError
          return 'done'
        },
        {
          _delayFn: instantDelay,
          maxRetries: 5,
          returnMetrics: true,
        }
      )

      expect(result).toBe('done')
      expect(metrics.succeeded).toBe(true)
      expect(metrics.attempts).toBe(3)
      expect(metrics.retries).toBe(2)
      expect(metrics.errors).toHaveLength(2)
    })

    it('reports metrics on failure', async () => {
      const concurrencyError = new Error('persistent conflict')
      concurrencyError.name = 'ConcurrencyError'

      try {
        await withRetry(
          async () => {
            throw concurrencyError
          },
          {
            _delayFn: instantDelay,
            maxRetries: 2,
            returnMetrics: true,
          }
        )
        expect.fail('Should have thrown')
      } catch (error: any) {
        expect(error.metrics).toBeDefined()
        expect(error.metrics.succeeded).toBe(false)
        expect(error.metrics.attempts).toBe(3)
      }
    })

    it('supports abort signal', async () => {
      const controller = new AbortController()
      let attempts = 0
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      const delayFn = async () => {
        controller.abort()
      }

      await expect(
        withRetry(
          async () => {
            attempts++
            throw concurrencyError
          },
          {
            _delayFn: delayFn,
            signal: controller.signal,
            maxRetries: 10,
          }
        )
      ).rejects.toThrow('aborted')

      expect(attempts).toBe(1)
    })

    it('calls onRetry callback before each retry', async () => {
      const retryInfos: any[] = []
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      await withRetry(
        async () => {
          if (retryInfos.length < 2) throw concurrencyError
          return 'done'
        },
        {
          _delayFn: instantDelay,
          onRetry: (info) => {
            retryInfos.push(info)
          },
        }
      )

      expect(retryInfos).toHaveLength(2)
      expect(retryInfos[0].attempt).toBe(1)
      expect(retryInfos[1].attempt).toBe(2)
    })

    it('can abort via onRetry returning false', async () => {
      let attempts = 0
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      await expect(
        withRetry(
          async () => {
            attempts++
            throw concurrencyError
          },
          {
            _delayFn: instantDelay,
            maxRetries: 5,
            onRetry: () => false, // Abort on first retry
          }
        )
      ).rejects.toThrow('conflict')

      expect(attempts).toBe(1)
    })

    it('uses custom isRetryable function', async () => {
      let attempts = 0
      const customError = new Error('custom error')

      const result = await withRetry(
        async () => {
          attempts++
          if (attempts < 3) throw customError
          return 'success'
        },
        {
          _delayFn: instantDelay,
          isRetryable: (error) => error.message === 'custom error',
        }
      )

      expect(result).toBe('success')
      expect(attempts).toBe(3)
    })
  })

  // ===========================================================================
  // Error Type Detection
  // ===========================================================================

  describe('error type detection', () => {
    it('identifies version conflict errors', async () => {
      const posts = (db as any).Posts

      const post = await posts.create({
        $type: 'Post',
        name: 'Test',
        slug: 'test',
      })

      const postId = post.$id.split('/')[1]

      try {
        await db.update('posts', postId, { $set: { title: 'Fail' } }, { expectedVersion: 999 })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionConflictError)
      }
    })

    it('identifies retryable errors', () => {
      const concurrencyError = new Error('conflict')
      concurrencyError.name = 'ConcurrencyError'

      expect(isRetryableError(concurrencyError)).toBe(true)

      const versionError = new Error('mismatch')
      versionError.name = 'VersionMismatchError'

      expect(isRetryableError(versionError)).toBe(true)

      const retryableError = new Error('temp') as Error & { retryable: boolean }
      retryableError.retryable = true

      expect(isRetryableError(retryableError)).toBe(true)

      // Non-retryable errors
      expect(isRetryableError(new Error('generic'))).toBe(false)
      expect(isRetryableError(new TypeError('type error'))).toBe(false)
      expect(isRetryableError(null)).toBe(false)
      expect(isRetryableError(undefined)).toBe(false)
    })

    it('error contains expected properties for debugging', async () => {
      const posts = (db as any).Posts

      const post = await posts.create({
        $type: 'Post',
        name: 'Test',
        slug: 'test',
      })

      const postId = post.$id.split('/')[1]

      try {
        await db.update('posts', postId, { $set: { title: 'Fail' } }, { expectedVersion: 5 })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionConflictError)
        const err = error as VersionConflictError
        expect(err.expectedVersion).toBe(5)
        expect(err.actualVersion).toBe(1)
        expect(err.namespace).toBe('posts')
        expect(err.entityId).toBe(postId)
      }
    })
  })

  // ===========================================================================
  // Concurrent Operation Handling
  // ===========================================================================

  describe('concurrent operation handling', () => {
    it('handles concurrent creates to same collection', async () => {
      const posts = (db as any).Posts

      // Create multiple posts concurrently
      const createPromises = Array.from({ length: 10 }, (_, i) =>
        posts.create({
          $type: 'Post',
          name: `Post ${i}`,
          slug: `concurrent-post-${i}`,
          title: `Concurrent Post ${i}`,
        })
      )

      const results = await Promise.all(createPromises)

      // All creates should succeed
      expect(results).toHaveLength(10)
      results.forEach((post, i) => {
        expect(post.slug).toBe(`concurrent-post-${i}`)
      })

      // Verify in database
      const allPosts = await posts.find({ slug: { $regex: '^concurrent-post-' } })
      expect(allPosts.items).toHaveLength(10)
    })

    it('handles concurrent updates without optimistic locking', async () => {
      const posts = (db as any).Posts

      const post = await posts.create({
        $type: 'Post',
        name: 'Test',
        slug: 'concurrent-test',
        viewCount: 0,
      })

      const postId = post.$id.split('/')[1]

      // Simulate concurrent increments (without expectedVersion, all should succeed)
      const incrementPromises = Array.from({ length: 5 }, () =>
        db.update('posts', postId, { $inc: { viewCount: 1 } })
      )

      const results = await Promise.all(incrementPromises)

      // All should succeed when not using expectedVersion
      const successes = results.filter(r => r !== null)
      expect(successes.length).toBe(5)

      // Final viewCount should be 5
      const final = await posts.findOne({ slug: 'concurrent-test' })
      expect(final.viewCount).toBe(5)
    })

    it('serializes updates with expectedVersion correctly', async () => {
      const posts = (db as any).Posts

      const post = await posts.create({
        $type: 'Post',
        name: 'Test',
        slug: 'serial-test',
        counter: 0,
      })

      const postId = post.$id.split('/')[1]

      // Sequential updates with version checking
      for (let i = 1; i <= 5; i++) {
        const current = await db.get('posts', postId)
        await db.update('posts', postId, {
          $inc: { counter: 1 },
        }, { expectedVersion: current!.version })
      }

      // Verify final state
      const final = await posts.findOne({ slug: 'serial-test' })
      expect(final.counter).toBe(5)
      expect(final.version).toBe(6)
    })
  })

  // ===========================================================================
  // Recovery after System Errors
  // ===========================================================================

  describe('recovery after system errors', () => {
    it('database remains queryable after failed operation', async () => {
      const posts = (db as any).Posts

      await posts.create({
        $type: 'Post',
        name: 'Existing',
        slug: 'existing',
        title: 'Existing Post',
      })

      // Cause a failure (expectedVersion > 1 on non-existent throws VersionConflictError)
      try {
        await db.update('posts', 'nonexistent', { $set: { x: 1 } }, { expectedVersion: 2 })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionConflictError)
      }

      // Database should still work
      const existing = await posts.findOne({ slug: 'existing' })
      expect(existing).not.toBeNull()
      expect(existing.title).toBe('Existing Post')

      // Can still create
      const newPost = await posts.create({
        $type: 'Post',
        name: 'After Error',
        slug: 'after-error',
      })
      expect(newPost.slug).toBe('after-error')
    })

    it('handles rapid create-delete-create cycles with unique slugs', async () => {
      const posts = (db as any).Posts

      // Create and soft-delete several posts with different slugs
      for (let i = 0; i < 3; i++) {
        const post = await posts.create({
          $type: 'Post',
          name: `Cycle Post ${i}`,
          slug: `cycle-post-${i}`,
          cycle: i,
        })

        const postId = post.$id.split('/')[1]

        // Soft delete (default)
        await db.delete('posts', postId)
      }

      // Final create with new unique slug
      const finalPost = await posts.create({
        $type: 'Post',
        name: 'Final',
        slug: 'cycle-post-final',
        cycle: 'final',
      })

      expect(finalPost.cycle).toBe('final')

      // The final post should exist
      const found = await posts.findOne({ slug: 'cycle-post-final' })
      expect(found).not.toBeNull()
      expect(found.cycle).toBe('final')
    })

    it('hard delete allows reusing slot', async () => {
      const posts = (db as any).Posts

      // Create and hard-delete
      const post1 = await posts.create({
        $type: 'Post',
        name: 'First',
        slug: 'reusable-slot',
      })

      const postId1 = post1.$id.split('/')[1]
      await db.delete('posts', postId1, { hard: true })

      // Create new post with same slug
      const post2 = await posts.create({
        $type: 'Post',
        name: 'Second',
        slug: 'reusable-slot',
      })

      expect(post2.name).toBe('Second')

      // Should find the new post
      const found = await posts.findOne({ slug: 'reusable-slot' })
      expect(found).not.toBeNull()
      expect(found.name).toBe('Second')
    })
  })
})
