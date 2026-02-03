/**
 * Unit tests for distributed locking mechanism
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  DistributedLock,
  acquireLock,
  releaseLock,
  withLock,
  isLockHeld,
  LockAcquisitionError,
  LockExpiredError,
  LOCK_PATH,
} from '../../../src/sync/distributed-lock'

// =============================================================================
// Test Utilities
// =============================================================================

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

// =============================================================================
// DistributedLock Tests
// =============================================================================

describe('DistributedLock', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('acquireLock', () => {
    it('acquires lock when not held', async () => {
      const lock = await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
      })

      expect(lock).toBeDefined()
      expect(lock.owner).toBe('process-1')
      expect(lock.resource).toBe('merge')
      expect(lock.acquiredAt).toBeDefined()
      expect(lock.expiresAt).toBeGreaterThan(lock.acquiredAt)
    })

    it('throws when lock already held by another owner', async () => {
      await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
      })

      await expect(
        acquireLock(storage, {
          owner: 'process-2',
          resource: 'merge',
        })
      ).rejects.toThrow(LockAcquisitionError)
    })

    it('allows same owner to re-acquire (reentrant)', async () => {
      const lock1 = await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
      })

      const lock2 = await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
      })

      expect(lock2).toBeDefined()
      expect(lock2.owner).toBe('process-1')
    })

    it('respects custom TTL', async () => {
      const lock = await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
        ttlMs: 5000,
      })

      expect(lock.expiresAt - lock.acquiredAt).toBe(5000)
    })

    it('acquires lock after previous lock expires', async () => {
      // Acquire lock with short TTL
      await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
        ttlMs: 1000,
      })

      // Fast-forward time past TTL
      vi.advanceTimersByTime(1500)

      // Another process should now be able to acquire
      const lock = await acquireLock(storage, {
        owner: 'process-2',
        resource: 'merge',
      })

      expect(lock.owner).toBe('process-2')
    })

    it('supports multiple independent resources', async () => {
      const lock1 = await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge:main',
      })

      const lock2 = await acquireLock(storage, {
        owner: 'process-2',
        resource: 'merge:feature',
      })

      expect(lock1.resource).toBe('merge:main')
      expect(lock2.resource).toBe('merge:feature')
    })

    it('retries acquisition with backoff', async () => {
      // Use real timers for this test
      vi.useRealTimers()

      // First acquire by process-1 with short TTL
      await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
        ttlMs: 50, // Very short TTL
      })

      // Small delay to simulate time passing
      await delay(60)

      // Process-2 tries - lock should have expired
      const lock = await acquireLock(storage, {
        owner: 'process-2',
        resource: 'merge',
        maxRetries: 3,
        retryDelayMs: 10,
      })

      expect(lock.owner).toBe('process-2')

      // Restore fake timers
      vi.useFakeTimers()
    })

    it('fails after max retries', async () => {
      // Use real timers for this test
      vi.useRealTimers()

      // First acquire by process-1 with long TTL
      await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
        ttlMs: 60000,
      })

      // Process-2 tries with limited retries and short delays
      await expect(
        acquireLock(storage, {
          owner: 'process-2',
          resource: 'merge',
          maxRetries: 2,
          retryDelayMs: 10, // Short delay for fast test
        })
      ).rejects.toThrow(LockAcquisitionError)

      // Restore fake timers
      vi.useFakeTimers()
    })
  })

  describe('releaseLock', () => {
    it('releases lock held by owner', async () => {
      const lock = await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
      })

      await releaseLock(storage, lock)

      // Another process should now be able to acquire
      const newLock = await acquireLock(storage, {
        owner: 'process-2',
        resource: 'merge',
      })

      expect(newLock.owner).toBe('process-2')
    })

    it('throws when releasing lock not owned', async () => {
      await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
      })

      const fakeLock: DistributedLock = {
        owner: 'process-2',
        resource: 'merge',
        acquiredAt: Date.now(),
        expiresAt: Date.now() + 30000,
        version: 'wrong-version',
      }

      await expect(releaseLock(storage, fakeLock)).rejects.toThrow()
    })

    it('succeeds silently when lock does not exist', async () => {
      const fakeLock: DistributedLock = {
        owner: 'process-1',
        resource: 'merge',
        acquiredAt: Date.now(),
        expiresAt: Date.now() + 30000,
        version: 'nonexistent',
      }

      // Should not throw
      await releaseLock(storage, fakeLock)
    })
  })

  describe('isLockHeld', () => {
    it('returns false when no lock exists', async () => {
      const held = await isLockHeld(storage, 'merge')
      expect(held).toBe(false)
    })

    it('returns true when lock is held', async () => {
      await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
      })

      const held = await isLockHeld(storage, 'merge')
      expect(held).toBe(true)
    })

    it('returns false when lock is expired', async () => {
      await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
        ttlMs: 1000,
      })

      vi.advanceTimersByTime(1500)

      const held = await isLockHeld(storage, 'merge')
      expect(held).toBe(false)
    })
  })

  describe('withLock', () => {
    it('executes operation with lock held', async () => {
      let executed = false

      await withLock(
        storage,
        { owner: 'process-1', resource: 'merge' },
        async () => {
          executed = true
          const held = await isLockHeld(storage, 'merge')
          expect(held).toBe(true)
        }
      )

      expect(executed).toBe(true)
    })

    it('releases lock after operation completes', async () => {
      await withLock(
        storage,
        { owner: 'process-1', resource: 'merge' },
        async () => {
          // Do nothing
        }
      )

      const held = await isLockHeld(storage, 'merge')
      expect(held).toBe(false)
    })

    it('releases lock when operation throws', async () => {
      await expect(
        withLock(
          storage,
          { owner: 'process-1', resource: 'merge' },
          async () => {
            throw new Error('Operation failed')
          }
        )
      ).rejects.toThrow('Operation failed')

      const held = await isLockHeld(storage, 'merge')
      expect(held).toBe(false)
    })

    it('returns operation result', async () => {
      const result = await withLock(
        storage,
        { owner: 'process-1', resource: 'merge' },
        async () => {
          return { success: true, data: 42 }
        }
      )

      expect(result).toEqual({ success: true, data: 42 })
    })

    it('prevents concurrent operations on same resource', async () => {
      vi.useRealTimers() // Need real timers for actual concurrency test

      const results: string[] = []

      const op1 = withLock(
        storage,
        { owner: 'process-1', resource: 'merge' },
        async () => {
          results.push('op1-start')
          await delay(50)
          results.push('op1-end')
        }
      )

      // Small delay to ensure op1 starts first
      await delay(10)

      const op2Promise = withLock(
        storage,
        { owner: 'process-2', resource: 'merge', maxRetries: 10, retryDelayMs: 20 },
        async () => {
          results.push('op2-start')
          await delay(10)
          results.push('op2-end')
        }
      )

      await Promise.all([op1, op2Promise])

      // op2 should only start after op1 completes
      expect(results.indexOf('op1-end')).toBeLessThan(results.indexOf('op2-start'))

      vi.useFakeTimers() // Restore fake timers for other tests
    })
  })

  describe('Lock Renewal', () => {
    it('supports lock renewal before expiry', async () => {
      const lock = await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
        ttlMs: 5000,
      })

      vi.advanceTimersByTime(3000)

      // Renew the lock
      const renewed = await acquireLock(storage, {
        owner: 'process-1',
        resource: 'merge',
        ttlMs: 5000,
      })

      expect(renewed.expiresAt).toBeGreaterThan(lock.expiresAt)
    })
  })

  describe('Lock Path Convention', () => {
    it('uses standard lock path format', () => {
      expect(LOCK_PATH('merge')).toBe('_locks/merge.lock')
      expect(LOCK_PATH('merge:main')).toBe('_locks/merge:main.lock')
    })
  })
})

// =============================================================================
// Integration with Merge Operations
// =============================================================================

describe('DistributedLock Integration', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('generates unique lock IDs for different processes', async () => {
    const lock1 = await acquireLock(storage, {
      owner: 'worker-1',
      resource: 'merge',
    })

    await releaseLock(storage, lock1)

    const lock2 = await acquireLock(storage, {
      owner: 'worker-2',
      resource: 'merge',
    })

    // Versions should be unique
    expect(lock1.version).not.toBe(lock2.version)
  })
})
