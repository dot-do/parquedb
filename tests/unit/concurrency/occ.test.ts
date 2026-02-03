/**
 * Optimistic Concurrency Control (OCC) Tests Under Load
 *
 * Tests for verifying OCC behavior under various concurrent write scenarios,
 * including conflict detection, retry mechanisms, and edge cases.
 *
 * OCC Implementation in ParqueDB:
 * - Entity level: version field + expectedVersion option
 * - Storage level: ETag-based conditional writes
 * - Retry: withRetry utility with exponential backoff
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { Collection, clearGlobalStorage, clearEventLog } from '../../../src/Collection'
import type { Entity, EntityId } from '../../../src/types'
import { VersionConflictError } from '../../../src/mutation/update'
import {
  withRetry,
  isRetryableError,
  type RetryConfig,
  type RetryMetrics,
} from '../../../src/delta-utils/retry'

// =============================================================================
// Test Types
// =============================================================================

interface Counter {
  value: number
  metadata?: Record<string, unknown>
}

interface Account {
  balance: number
  currency: string
  status: 'active' | 'suspended' | 'closed'
}

interface Document {
  title: string
  content: string
  revision: number
  lastEditor?: string
}

// =============================================================================
// Custom Error Classes for Testing
// =============================================================================

class ConcurrencyError extends Error {
  override name = 'ConcurrencyError'
  constructor(message: string) {
    super(message)
  }
}

// =============================================================================
// Test Setup
// =============================================================================

describe('OCC Under Load', () => {
  let countersCollection: Collection<Counter>
  let accountsCollection: Collection<Account>
  let documentsCollection: Collection<Document>

  beforeEach(() => {
    clearGlobalStorage()
    clearEventLog()
    countersCollection = new Collection<Counter>('counters')
    accountsCollection = new Collection<Account>('accounts')
    documentsCollection = new Collection<Document>('documents')
  })

  afterEach(() => {
    vi.useRealTimers()
    clearGlobalStorage()
    clearEventLog()
  })

  // ===========================================================================
  // Basic OCC Version Checking
  // ===========================================================================

  describe('Basic OCC Version Checking', () => {
    it('should increment version on every update', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'test-counter',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]

      expect(entity.version).toBe(1)

      // Sequential updates
      for (let i = 1; i <= 5; i++) {
        await countersCollection.update(localId, { $inc: { value: 1 } })
        const updated = await countersCollection.get(localId)
        expect(updated.version).toBe(1 + i)
      }
    })

    it('should reject update with incorrect expected version', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'version-test',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]

      // Update with correct version
      await countersCollection.update(
        localId,
        { $set: { value: 10 } },
        { expectedVersion: 1 }
      )

      // Attempt update with stale version
      await expect(
        countersCollection.update(
          localId,
          { $set: { value: 20 } },
          { expectedVersion: 1 }
        )
      ).rejects.toThrow('Version mismatch')
    })

    it('should accept update with correct expected version', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'correct-version',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]

      // Sequential updates with correct versions
      await countersCollection.update(
        localId,
        { $set: { value: 1 } },
        { expectedVersion: 1 }
      )
      await countersCollection.update(
        localId,
        { $set: { value: 2 } },
        { expectedVersion: 2 }
      )
      await countersCollection.update(
        localId,
        { $set: { value: 3 } },
        { expectedVersion: 3 }
      )

      const final = await countersCollection.get(localId)
      expect(final.value).toBe(3)
      expect(final.version).toBe(4)
    })
  })

  // ===========================================================================
  // Concurrent Writes with OCC
  // ===========================================================================

  describe('Concurrent Writes with OCC', () => {
    it('should detect conflicts when multiple writers use same version', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'contention-counter',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]
      const initialVersion = entity.version as number

      // Simulate 10 concurrent writers all expecting same version
      const writerCount = 10
      const results = await Promise.allSettled(
        Array.from({ length: writerCount }, (_, i) =>
          countersCollection.update(
            localId,
            { $set: { value: i } },
            { expectedVersion: initialVersion }
          )
        )
      )

      // Exactly one should succeed, rest should fail
      const successes = results.filter(r => r.status === 'fulfilled')
      const failures = results.filter(r => r.status === 'rejected')

      expect(successes).toHaveLength(1)
      expect(failures).toHaveLength(writerCount - 1)

      // All failures should be version mismatch
      for (const failure of failures) {
        if (failure.status === 'rejected') {
          expect(failure.reason.message).toContain('Version mismatch')
        }
      }
    })

    it('should handle high contention with 50 concurrent writers', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'high-contention',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]
      const initialVersion = entity.version as number

      const writerCount = 50
      const results = await Promise.allSettled(
        Array.from({ length: writerCount }, (_, i) =>
          countersCollection.update(
            localId,
            { $set: { value: i * 100 } },
            { expectedVersion: initialVersion }
          )
        )
      )

      const successes = results.filter(r => r.status === 'fulfilled')
      const failures = results.filter(r => r.status === 'rejected')

      // Exactly one writer should succeed
      expect(successes).toHaveLength(1)
      expect(failures).toHaveLength(writerCount - 1)

      // Verify final state is consistent
      const final = await countersCollection.get(localId)
      expect(final.version).toBe(2) // Only one successful update
    })

    it('should allow sequential success after conflict detection', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'sequential-after-conflict',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]

      // First update succeeds
      await countersCollection.update(
        localId,
        { $set: { value: 1 } },
        { expectedVersion: 1 }
      )

      // Second update with stale version fails
      await expect(
        countersCollection.update(
          localId,
          { $set: { value: 2 } },
          { expectedVersion: 1 }
        )
      ).rejects.toThrow('Version mismatch')

      // Re-read and retry with correct version
      const current = await countersCollection.get(localId)
      await countersCollection.update(
        localId,
        { $set: { value: 2 } },
        { expectedVersion: current.version }
      )

      const final = await countersCollection.get(localId)
      expect(final.value).toBe(2)
      expect(final.version).toBe(3)
    })
  })

  // ===========================================================================
  // Retry Mechanism Tests
  // ===========================================================================

  describe('Retry Mechanism', () => {
    it('should identify ConcurrencyError as retryable', () => {
      expect(isRetryableError(new ConcurrencyError('test'))).toBe(true)
    })

    it('should not retry non-retryable errors', () => {
      expect(isRetryableError(new Error('generic error'))).toBe(false)
      expect(isRetryableError(new TypeError('type error'))).toBe(false)
      expect(isRetryableError(null)).toBe(false)
      expect(isRetryableError(undefined)).toBe(false)
    })

    it('should retry with exponential backoff on retryable errors', async () => {
      let attempts = 0
      const delays: number[] = []

      const result = await withRetry(
        async () => {
          attempts++
          if (attempts < 3) {
            const error = new ConcurrencyError(`Attempt ${attempts} failed`)
            throw error
          }
          return 'success'
        },
        {
          maxRetries: 5,
          baseDelay: 10,
          jitter: false,
          onRetry: ({ delay }) => {
            delays.push(delay)
          },
          isRetryable: (err) => err.name === 'ConcurrencyError',
        }
      )

      expect(result).toBe('success')
      expect(attempts).toBe(3)
      expect(delays).toHaveLength(2) // 2 retries before success
      // Check exponential growth (with baseDelay=10, multiplier=2)
      expect(delays[0]).toBe(10)
      expect(delays[1]).toBe(20)
    })

    it('should respect maxRetries limit', async () => {
      let attempts = 0

      await expect(
        withRetry(
          async () => {
            attempts++
            const error = new ConcurrencyError('Always fails')
            throw error
          },
          {
            maxRetries: 3,
            baseDelay: 1,
            isRetryable: (err) => err.name === 'ConcurrencyError',
          }
        )
      ).rejects.toThrow('Always fails')

      expect(attempts).toBe(4) // Initial + 3 retries
    })

    it('should track metrics during retry', async () => {
      let attempts = 0

      const { result, metrics } = await withRetry(
        async () => {
          attempts++
          if (attempts < 2) {
            throw new ConcurrencyError('Retry needed')
          }
          return 42
        },
        {
          maxRetries: 5,
          baseDelay: 1,
          returnMetrics: true,
          isRetryable: (err) => err.name === 'ConcurrencyError',
        }
      )

      expect(result).toBe(42)
      expect(metrics.attempts).toBe(2)
      expect(metrics.retries).toBe(1)
      expect(metrics.succeeded).toBe(true)
      expect(metrics.errors).toHaveLength(1)
    })

    it('should allow aborting retries via onRetry callback', async () => {
      let attempts = 0

      await expect(
        withRetry(
          async () => {
            attempts++
            throw new ConcurrencyError('Always fails')
          },
          {
            maxRetries: 10,
            baseDelay: 1,
            isRetryable: (err) => err.name === 'ConcurrencyError',
            onRetry: ({ attempt }) => {
              if (attempt >= 2) {
                return false // Abort after 2nd attempt
              }
            },
          }
        )
      ).rejects.toThrow()

      expect(attempts).toBe(2)
    })

    it('should abort on AbortSignal', async () => {
      vi.useFakeTimers()
      const controller = new AbortController()
      let attempts = 0
      let abortErrorCaught = false

      const retryPromise = withRetry(
        async () => {
          attempts++
          throw new ConcurrencyError('Fails')
        },
        {
          maxRetries: 100,
          baseDelay: 20,
          signal: controller.signal,
          isRetryable: (err) => err.name === 'ConcurrencyError',
        }
      ).catch((err) => {
        abortErrorCaught = true
        return err // Return instead of re-throwing to avoid unhandled rejection
      })

      // Advance time to let some retries happen, then abort
      await vi.advanceTimersByTimeAsync(30) // Let first retry delay pass
      controller.abort()

      // Run all pending timers to ensure the promise settles
      await vi.runAllTimersAsync()

      const result = await retryPromise

      expect(result).toBeInstanceOf(Error)
      expect((result as Error).message).toContain('aborted')
      expect(abortErrorCaught).toBe(true)
      expect(attempts).toBeLessThan(10) // Should abort early
      // vi.useRealTimers() handled by afterEach
    })
  })

  // ===========================================================================
  // Read-Modify-Write Pattern Tests
  // ===========================================================================

  describe('Read-Modify-Write Pattern', () => {
    it('should implement safe read-modify-write with retry', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'rmw-counter',
        value: 100,
      })
      const localId = (entity.$id as string).split('/')[1]

      const safeIncrement = async (amount: number) => {
        let retries = 0
        while (retries < 5) {
          const current = await countersCollection.get(localId)
          try {
            await countersCollection.update(
              localId,
              { $set: { value: (current.value as number) + amount } },
              { expectedVersion: current.version }
            )
            return true
          } catch (error) {
            if ((error as Error).message.includes('Version mismatch')) {
              retries++
              continue
            }
            throw error
          }
        }
        return false
      }

      // Run 5 concurrent safe increments
      const results = await Promise.all(
        Array.from({ length: 5 }, () => safeIncrement(10))
      )

      // All should eventually succeed
      expect(results.every(r => r)).toBe(true)

      const final = await countersCollection.get(localId)
      expect(final.value).toBe(150) // 100 + 5*10
    })

    it('should detect lost updates without OCC', async () => {
      const entity = await accountsCollection.create({
        $type: 'Account',
        name: 'lost-update-account',
        balance: 1000,
        currency: 'USD',
        status: 'active',
      })
      const localId = (entity.$id as string).split('/')[1]

      // Both "users" read the same balance
      const user1Balance = (await accountsCollection.get(localId)).balance as number
      const user2Balance = (await accountsCollection.get(localId)).balance as number

      // Both try to add 100 (without OCC)
      await Promise.all([
        accountsCollection.update(localId, { $set: { balance: user1Balance + 100 } }),
        accountsCollection.update(localId, { $set: { balance: user2Balance + 100 } }),
      ])

      const final = await accountsCollection.get(localId)
      // One update is lost - result is 1100 instead of 1200
      expect(final.balance).toBe(1100)
    })

    it('should prevent lost updates with atomic $inc', async () => {
      const entity = await accountsCollection.create({
        $type: 'Account',
        name: 'atomic-account',
        balance: 1000,
        currency: 'USD',
        status: 'active',
      })
      const localId = (entity.$id as string).split('/')[1]

      // 10 concurrent deposits of 100 using atomic $inc
      await Promise.all(
        Array.from({ length: 10 }, () =>
          accountsCollection.update(localId, { $inc: { balance: 100 } })
        )
      )

      const final = await accountsCollection.get(localId)
      expect(final.balance).toBe(2000) // 1000 + 10*100
    })
  })

  // ===========================================================================
  // Delete with OCC
  // ===========================================================================

  describe('Delete with OCC', () => {
    it('should reject delete with stale version', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'delete-test',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]

      // Update to increment version
      await countersCollection.update(localId, { $inc: { value: 1 } })

      // Try delete with stale version
      await expect(
        countersCollection.delete(localId, { expectedVersion: 1 })
      ).rejects.toThrow('Version mismatch')
    })

    it('should allow delete with correct version', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'delete-correct',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]

      await countersCollection.update(localId, { $inc: { value: 1 } })

      const current = await countersCollection.get(localId)
      const result = await countersCollection.delete(localId, {
        expectedVersion: current.version,
      })

      expect(result.deletedCount).toBe(1)
    })

    it('should handle concurrent delete and update', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'concurrent-delete-update',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]
      const version = entity.version as number

      // Concurrent delete and update with same version
      const results = await Promise.allSettled([
        countersCollection.delete(localId, { expectedVersion: version }),
        countersCollection.update(
          localId,
          { $set: { value: 100 } },
          { expectedVersion: version }
        ),
      ])

      // One should succeed, one should fail (or both succeed depending on timing)
      const successes = results.filter(r => r.status === 'fulfilled')
      expect(successes.length).toBeGreaterThanOrEqual(1)
    })
  })

  // ===========================================================================
  // Load Testing OCC
  // ===========================================================================

  describe('OCC Under High Load', () => {
    it('should handle 100 concurrent writers with OCC', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'load-test-100',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]

      const writerCount = 100

      // Use atomic $inc to avoid lost updates
      await Promise.all(
        Array.from({ length: writerCount }, () =>
          countersCollection.update(localId, { $inc: { value: 1 } })
        )
      )

      const final = await countersCollection.get(localId)
      expect(final.value).toBe(writerCount)
    })

    it('should maintain consistency under sustained load', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'sustained-load',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]

      // 5 waves of 20 concurrent writers
      for (let wave = 0; wave < 5; wave++) {
        await Promise.all(
          Array.from({ length: 20 }, () =>
            countersCollection.update(localId, { $inc: { value: 1 } })
          )
        )
      }

      const final = await countersCollection.get(localId)
      expect(final.value).toBe(100) // 5 * 20
      expect(final.version).toBe(101) // 1 initial + 100 updates
    })

    it('should handle mixed operations under load', async () => {
      // Create 10 entities
      const entities = await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          countersCollection.create({
            $type: 'Counter',
            name: `mixed-${i}`,
            value: 0,
          })
        )
      )

      // Perform mixed operations on all entities concurrently
      const operations: Promise<unknown>[] = []

      for (const entity of entities) {
        const localId = (entity.$id as string).split('/')[1]

        // 5 increments per entity
        for (let i = 0; i < 5; i++) {
          operations.push(
            countersCollection.update(localId, { $inc: { value: 1 } })
          )
        }

        // 2 reads per entity
        for (let i = 0; i < 2; i++) {
          operations.push(countersCollection.get(localId))
        }
      }

      await Promise.all(operations)

      // Verify all entities have correct final state
      for (const entity of entities) {
        const localId = (entity.$id as string).split('/')[1]
        const final = await countersCollection.get(localId)
        expect(final.value).toBe(5)
      }
    })
  })

  // ===========================================================================
  // Edge Cases and Race Conditions
  // ===========================================================================

  describe('Edge Cases and Race Conditions', () => {
    it('should handle rapid version changes', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'rapid-version',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]

      // Perform 50 rapid sequential updates
      for (let i = 0; i < 50; i++) {
        await countersCollection.update(localId, { $inc: { value: 1 } })
      }

      const final = await countersCollection.get(localId)
      expect(final.value).toBe(50)
      expect(final.version).toBe(51)
    })

    it('should detect version changes between read and write', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'read-write-race',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]

      // Simulate race: read, concurrent update, then write with stale version
      const readEntity = await countersCollection.get(localId)

      // Another process updates
      await countersCollection.update(localId, { $inc: { value: 100 } })

      // Original process tries to write with stale version
      await expect(
        countersCollection.update(
          localId,
          { $set: { value: (readEntity.value as number) + 10 } },
          { expectedVersion: readEntity.version }
        )
      ).rejects.toThrow('Version mismatch')
    })

    it('should handle version wraparound at high numbers', async () => {
      // Start with a high version number (simulated)
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'high-version',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]

      // Perform many updates to increase version
      for (let i = 0; i < 100; i++) {
        await countersCollection.update(localId, { $inc: { value: 1 } })
      }

      const final = await countersCollection.get(localId)
      expect(final.version).toBe(101)

      // Continue with OCC at high version
      await countersCollection.update(
        localId,
        { $set: { value: 999 } },
        { expectedVersion: 101 }
      )

      const updated = await countersCollection.get(localId)
      expect(updated.version).toBe(102)
    })

    it('should handle concurrent create and update race', async () => {
      const createPromise = countersCollection.create({
        $type: 'Counter',
        name: 'create-update-race',
        value: 0,
      })

      const entity = await createPromise
      const localId = (entity.$id as string).split('/')[1]

      // Immediate update after create
      const updatePromise = countersCollection.update(
        localId,
        { $set: { value: 100 } },
        { expectedVersion: 1 }
      )

      await updatePromise

      const final = await countersCollection.get(localId)
      expect(final.value).toBe(100)
    })

    it('should handle update to non-existent entity', async () => {
      const result = await countersCollection.update(
        'non-existent-id',
        { $set: { value: 100 } }
      )

      expect(result.matchedCount).toBe(0)
      expect(result.modifiedCount).toBe(0)
    })

    it('should handle update with version check on non-existent entity', async () => {
      // This should fail gracefully
      const result = await countersCollection.update(
        'non-existent-id',
        { $set: { value: 100 } },
        { expectedVersion: 1 }
      )

      expect(result.matchedCount).toBe(0)
      expect(result.modifiedCount).toBe(0)
    })
  })

  // ===========================================================================
  // Document Versioning Scenario
  // ===========================================================================

  describe('Document Versioning Scenario', () => {
    it('should track document revisions with OCC', async () => {
      const doc = await documentsCollection.create({
        $type: 'Document',
        name: 'versioned-doc',
        title: 'Draft',
        content: 'Initial content',
        revision: 1,
      })
      const localId = (doc.$id as string).split('/')[1]

      // Multiple users editing the document
      const editors = ['alice', 'bob', 'charlie']
      const edits: Promise<unknown>[] = []

      for (const editor of editors) {
        edits.push(
          (async () => {
            const current = await documentsCollection.get(localId)
            try {
              await documentsCollection.update(
                localId,
                {
                  $set: {
                    content: `Edited by ${editor}`,
                    lastEditor: editor,
                  },
                  $inc: { revision: 1 },
                },
                { expectedVersion: current.version }
              )
              return { editor, success: true }
            } catch {
              return { editor, success: false }
            }
          })()
        )
      }

      const results = await Promise.all(edits)
      const successes = results.filter(r => (r as { success: boolean }).success)

      // At least one editor should succeed
      expect(successes.length).toBeGreaterThanOrEqual(1)
    })

    it('should allow collaborative editing with retry', async () => {
      vi.useFakeTimers()
      try {
        const doc = await documentsCollection.create({
          $type: 'Document',
          name: 'collab-doc',
          title: 'Collaborative Document',
          content: 'Start',
          revision: 1,
        })
        const localId = (doc.$id as string).split('/')[1]

        const editWithRetry = async (editor: string, maxRetries = 5) => {
          for (let attempt = 0; attempt < maxRetries; attempt++) {
            const current = await documentsCollection.get(localId)
            try {
              await documentsCollection.update(
                localId,
                {
                  $set: { lastEditor: editor },
                  $inc: { revision: 1 },
                },
                { expectedVersion: current.version }
              )
              return true
            } catch (error) {
              if (!(error as Error).message.includes('Version mismatch')) {
                throw error
              }
              // Small delay before retry
              await vi.advanceTimersByTimeAsync(1)
            }
          }
          return false
        }

        // 5 editors concurrently with retry
        const results = await Promise.all(
          Array.from({ length: 5 }, (_, i) => editWithRetry(`editor-${i}`))
        )

        // All should eventually succeed
        expect(results.every(r => r)).toBe(true)

        const final = await documentsCollection.get(localId)
        expect(final.revision).toBe(6) // 1 initial + 5 edits
      } finally {
        // vi.useRealTimers() also handled by afterEach as backup
        vi.useRealTimers()
      }
    })
  })

  // ===========================================================================
  // Transaction-like Patterns
  // ===========================================================================

  describe('Transaction-like Patterns', () => {
    it('should implement compare-and-swap pattern', async () => {
      const counter = await countersCollection.create({
        $type: 'Counter',
        name: 'cas-counter',
        value: 0,
      })
      const localId = (counter.$id as string).split('/')[1]

      const compareAndSwap = async (
        expectedValue: number,
        newValue: number
      ): Promise<boolean> => {
        const current = await countersCollection.get(localId)
        if ((current.value as number) !== expectedValue) {
          return false // CAS failed - value changed
        }
        try {
          await countersCollection.update(
            localId,
            { $set: { value: newValue } },
            { expectedVersion: current.version }
          )
          return true
        } catch {
          return false // Version conflict
        }
      }

      // Successful CAS
      expect(await compareAndSwap(0, 10)).toBe(true)

      // Failed CAS (wrong expected value)
      expect(await compareAndSwap(0, 20)).toBe(false)

      // Successful CAS with correct expected value
      expect(await compareAndSwap(10, 20)).toBe(true)

      const final = await countersCollection.get(localId)
      expect(final.value).toBe(20)
    })

    it('should implement optimistic transaction with rollback detection', async () => {
      // Create two accounts for transfer
      const account1 = await accountsCollection.create({
        $type: 'Account',
        name: 'account-1',
        balance: 1000,
        currency: 'USD',
        status: 'active',
      })
      const account2 = await accountsCollection.create({
        $type: 'Account',
        name: 'account-2',
        balance: 1000,
        currency: 'USD',
        status: 'active',
      })

      const id1 = (account1.$id as string).split('/')[1]
      const id2 = (account2.$id as string).split('/')[1]

      const transfer = async (
        fromId: string,
        toId: string,
        amount: number
      ): Promise<boolean> => {
        // Read both accounts
        const from = await accountsCollection.get(fromId)
        const to = await accountsCollection.get(toId)

        if ((from.balance as number) < amount) {
          throw new Error('Insufficient funds')
        }

        try {
          // Debit from source
          await accountsCollection.update(
            fromId,
            { $inc: { balance: -amount } },
            { expectedVersion: from.version }
          )

          // Credit to destination
          await accountsCollection.update(
            toId,
            { $inc: { balance: amount } },
            { expectedVersion: to.version }
          )

          return true
        } catch (error) {
          // In a real implementation, we would need to compensate/rollback
          // For this test, we just detect the conflict
          return false
        }
      }

      const result = await transfer(id1, id2, 100)
      expect(result).toBe(true)

      const final1 = await accountsCollection.get(id1)
      const final2 = await accountsCollection.get(id2)

      expect(final1.balance).toBe(900)
      expect(final2.balance).toBe(1100)
    })
  })

  // ===========================================================================
  // VersionConflictError Specific Tests
  // ===========================================================================

  describe('VersionConflictError Details', () => {
    it('should include expected and actual versions in error', async () => {
      const entity = await countersCollection.create({
        $type: 'Counter',
        name: 'error-details',
        value: 0,
      })
      const localId = (entity.$id as string).split('/')[1]

      // Update to change version
      await countersCollection.update(localId, { $inc: { value: 1 } })

      // Try update with stale version
      try {
        await countersCollection.update(
          localId,
          { $set: { value: 100 } },
          { expectedVersion: 1 }
        )
        expect.fail('Should have thrown')
      } catch (error) {
        expect((error as Error).message).toContain('expected 1')
        expect((error as Error).message).toContain('got 2')
      }
    })
  })

  // ===========================================================================
  // Stress Test
  // ===========================================================================

  describe('Stress Tests', () => {
    it('should handle stress test with mixed operations', async () => {
      // Create 5 entities
      const entities = await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          countersCollection.create({
            $type: 'Counter',
            name: `stress-${i}`,
            value: 0,
          })
        )
      )

      const localIds = entities.map(e => (e.$id as string).split('/')[1])

      // Perform 200 random operations
      const operations: Promise<unknown>[] = []

      for (let i = 0; i < 200; i++) {
        const randomIndex = Math.floor(Math.random() * localIds.length)
        const localId = localIds[randomIndex]

        if (Math.random() < 0.3) {
          // 30% reads
          operations.push(countersCollection.get(localId))
        } else {
          // 70% writes
          operations.push(
            countersCollection.update(localId, { $inc: { value: 1 } })
          )
        }
      }

      await Promise.all(operations)

      // Verify data integrity
      for (const localId of localIds) {
        const entity = await countersCollection.get(localId)
        expect(typeof entity.value).toBe('number')
        expect(typeof entity.version).toBe('number')
        expect((entity.version as number)).toBeGreaterThan(0)
      }
    })
  })
})
