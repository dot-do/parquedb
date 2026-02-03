/**
 * Unit Tests for Distributed Locking
 *
 * Tests the lock mechanism for preventing concurrent merge operations.
 * Covers:
 * - Lock acquisition and release
 * - Lock expiry and timeout
 * - Concurrent lock attempts
 * - Lock extension
 * - Force release for recovery
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  createLockManager,
  StorageLockManager,
  withLock,
  LockAcquisitionError,
  LockExpiredError,
  type LockManager,
  type Lock,
  type LockState,
  type LockResource,
  LOCK_DIR,
  DEFAULT_LOCK_TIMEOUT,
  DEFAULT_WAIT_TIMEOUT,
} from '../../../src/sync/lock'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import type { StorageBackend } from '../../../src/types/storage'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Advance fake timers by specified milliseconds (use only with vi.useFakeTimers())
 * Uses async version to allow pending promises to resolve
 */
async function advanceTime(ms: number): Promise<void> {
  await vi.advanceTimersByTimeAsync(ms)
}

// =============================================================================
// Test Setup
// =============================================================================

describe('Distributed Locking', () => {
  let storage: MemoryBackend
  let lockManager: LockManager

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-01T12:00:00Z'))
    storage = new MemoryBackend()
    lockManager = createLockManager(storage)
  })

  afterEach(async () => {
    // Clean up any locks
    await lockManager.forceRelease('merge')
    await lockManager.forceRelease('commit')
    await lockManager.forceRelease('sync')
    vi.useRealTimers()
  })

  // ===========================================================================
  // Basic Lock Acquisition
  // ===========================================================================

  describe('Basic Lock Acquisition', () => {
    it('should acquire a lock on an unlocked resource', async () => {
      const result = await lockManager.tryAcquire('merge')

      expect(result.acquired).toBe(true)
      expect(result.lock).toBeDefined()
      expect(result.lock!.resource).toBe('merge')
      expect(result.lock!.isValid()).toBe(true)

      await result.lock!.release()
    })

    it('should fail to acquire lock on already locked resource', async () => {
      // First lock
      const first = await lockManager.tryAcquire('merge')
      expect(first.acquired).toBe(true)

      // Second attempt should fail
      const second = await lockManager.tryAcquire('merge')
      expect(second.acquired).toBe(false)
      expect(second.currentHolder).toBeDefined()
      expect(second.currentHolder!.resource).toBe('merge')

      await first.lock!.release()
    })

    it('should allow acquiring different resources simultaneously', async () => {
      const mergeLock = await lockManager.tryAcquire('merge')
      const commitLock = await lockManager.tryAcquire('commit')
      const syncLock = await lockManager.tryAcquire('sync')

      expect(mergeLock.acquired).toBe(true)
      expect(commitLock.acquired).toBe(true)
      expect(syncLock.acquired).toBe(true)

      await mergeLock.lock!.release()
      await commitLock.lock!.release()
      await syncLock.lock!.release()
    })

    it('should track namespace-scoped locks independently', async () => {
      const ns1Lock = await lockManager.tryAcquire('namespace:users')
      const ns2Lock = await lockManager.tryAcquire('namespace:posts')

      expect(ns1Lock.acquired).toBe(true)
      expect(ns2Lock.acquired).toBe(true)

      // Same namespace should be blocked
      const ns1Second = await lockManager.tryAcquire('namespace:users')
      expect(ns1Second.acquired).toBe(false)

      await ns1Lock.lock!.release()
      await ns2Lock.lock!.release()
    })
  })

  // ===========================================================================
  // Lock Release
  // ===========================================================================

  describe('Lock Release', () => {
    it('should release a held lock', async () => {
      const result = await lockManager.tryAcquire('merge')
      expect(result.acquired).toBe(true)

      await result.lock!.release()

      // Should now be able to acquire
      const second = await lockManager.tryAcquire('merge')
      expect(second.acquired).toBe(true)
      await second.lock!.release()
    })

    it('should be idempotent (multiple releases are safe)', async () => {
      const result = await lockManager.tryAcquire('merge')
      expect(result.acquired).toBe(true)

      // Release multiple times
      await result.lock!.release()
      await result.lock!.release()
      await result.lock!.release()

      // Should still work
      const second = await lockManager.tryAcquire('merge')
      expect(second.acquired).toBe(true)
      await second.lock!.release()
    })

    it('should mark lock as invalid after release', async () => {
      const result = await lockManager.tryAcquire('merge')
      expect(result.lock!.isValid()).toBe(true)

      await result.lock!.release()

      expect(result.lock!.isValid()).toBe(false)
    })
  })

  // ===========================================================================
  // Lock Expiry
  // ===========================================================================

  describe('Lock Expiry', () => {
    it('should expire locks after timeout', async () => {
      // Acquire with very short timeout
      const result = await lockManager.tryAcquire('merge', {
        timeout: 50, // 50ms
      })
      expect(result.acquired).toBe(true)

      // Wait for expiry using fake timers
      await advanceTime(100)

      // Should now be acquirable by another process
      const second = await lockManager.tryAcquire('merge')
      expect(second.acquired).toBe(true)
      await second.lock!.release()
    })

    it('should report lock as invalid after expiry', async () => {
      const result = await lockManager.tryAcquire('merge', {
        timeout: 50,
      })
      expect(result.lock!.isValid()).toBe(true)

      await advanceTime(100)

      expect(result.lock!.isValid()).toBe(false)
    })

    it('should clean up expired locks automatically', async () => {
      // Acquire with short timeout
      await lockManager.tryAcquire('merge', { timeout: 50 })

      await advanceTime(100)

      // isLocked should return null for expired lock
      const state = await lockManager.isLocked('merge')
      expect(state).toBeNull()
    })
  })

  // ===========================================================================
  // Lock Extension
  // ===========================================================================

  describe('Lock Extension', () => {
    it('should extend a held lock', async () => {
      const result = await lockManager.tryAcquire('merge', {
        timeout: 100,
      })
      expect(result.acquired).toBe(true)

      const originalState = result.lock!.getState()
      const originalExpiry = new Date(originalState.expiresAt).getTime()

      // Extend by 1 second
      const extended = await result.lock!.extend(1000)
      expect(extended).toBe(true)

      const newState = result.lock!.getState()
      const newExpiry = new Date(newState.expiresAt).getTime()

      expect(newExpiry).toBeGreaterThan(originalExpiry)

      await result.lock!.release()
    })

    it('should fail to extend a released lock', async () => {
      const result = await lockManager.tryAcquire('merge')
      await result.lock!.release()

      const extended = await result.lock!.extend(1000)
      expect(extended).toBe(false)
    })

    it('should fail to extend a lock held by another holder', async () => {
      // First holder acquires
      const manager1 = createLockManager(storage, 'holder-1')
      const lock1 = await manager1.tryAcquire('merge')
      expect(lock1.acquired).toBe(true)

      // First holder releases
      await lock1.lock!.release()

      // Second holder acquires
      const manager2 = createLockManager(storage, 'holder-2')
      const lock2 = await manager2.tryAcquire('merge')
      expect(lock2.acquired).toBe(true)

      // First lock's extend should fail (different holder now owns it)
      const extended = await lock1.lock!.extend(1000)
      expect(extended).toBe(false)

      await lock2.lock!.release()
    })
  })

  // ===========================================================================
  // Waiting for Lock
  // ===========================================================================

  describe('Waiting for Lock', () => {
    it('should wait and acquire when lock becomes available', async () => {
      // First holder acquires with short timeout
      const first = await lockManager.tryAcquire('merge', {
        timeout: 100,
      })
      expect(first.acquired).toBe(true)

      // Start second holder waiting for lock
      const secondPromise = lockManager.acquire('merge', {
        waitTimeout: 500,
        retryInterval: 50,
      })

      // Advance time to expire first lock and allow second to acquire
      await advanceTime(150)

      const second = await secondPromise
      expect(second.acquired).toBe(true)

      await second.lock!.release()
    })

    it('should fail if lock not available within wait timeout', async () => {
      // First holder acquires with long timeout
      const first = await lockManager.tryAcquire('merge', {
        timeout: 10000,
      })
      expect(first.acquired).toBe(true)

      // Start second holder waiting for lock
      const secondPromise = lockManager.acquire('merge', {
        waitTimeout: 100,
        retryInterval: 20,
      })

      // Advance time past wait timeout
      await advanceTime(150)

      const second = await secondPromise
      expect(second.acquired).toBe(false)
      expect(second.currentHolder).toBeDefined()

      await first.lock!.release()
    })
  })

  // ===========================================================================
  // Force Release
  // ===========================================================================

  describe('Force Release', () => {
    it('should force release a held lock', async () => {
      const result = await lockManager.tryAcquire('merge')
      expect(result.acquired).toBe(true)

      // Force release
      const released = await lockManager.forceRelease('merge')
      expect(released).toBe(true)

      // Should now be acquirable
      const second = await lockManager.tryAcquire('merge')
      expect(second.acquired).toBe(true)
      await second.lock!.release()
    })

    it('should return false when force releasing unlocked resource', async () => {
      const released = await lockManager.forceRelease('merge')
      expect(released).toBe(false)
    })
  })

  // ===========================================================================
  // List Locks
  // ===========================================================================

  describe('List Locks', () => {
    it('should list all held locks', async () => {
      await lockManager.tryAcquire('merge')
      await lockManager.tryAcquire('commit')
      await lockManager.tryAcquire('sync')

      const locks = await lockManager.listLocks()
      expect(locks.length).toBe(3)

      const resources = locks.map(l => l.resource)
      expect(resources).toContain('merge')
      expect(resources).toContain('commit')
      expect(resources).toContain('sync')

      await lockManager.forceRelease('merge')
      await lockManager.forceRelease('commit')
      await lockManager.forceRelease('sync')
    })

    it('should not list expired locks', async () => {
      await lockManager.tryAcquire('merge', { timeout: 50 })
      await lockManager.tryAcquire('commit', { timeout: 1000 })

      await advanceTime(100)

      const locks = await lockManager.listLocks()
      expect(locks.length).toBe(1)
      expect(locks[0]!.resource).toBe('commit')

      await lockManager.forceRelease('commit')
    })
  })

  // ===========================================================================
  // isLocked
  // ===========================================================================

  describe('isLocked', () => {
    it('should return lock state for locked resource', async () => {
      const result = await lockManager.tryAcquire('merge')
      expect(result.acquired).toBe(true)

      const state = await lockManager.isLocked('merge')
      expect(state).not.toBeNull()
      expect(state!.resource).toBe('merge')
      expect(state!.holder).toBeDefined()

      await result.lock!.release()
    })

    it('should return null for unlocked resource', async () => {
      const state = await lockManager.isLocked('merge')
      expect(state).toBeNull()
    })
  })

  // ===========================================================================
  // withLock Helper
  // ===========================================================================

  describe('withLock Helper', () => {
    it('should execute function while holding lock', async () => {
      let executed = false

      const result = await withLock(lockManager, 'merge', async () => {
        // Verify lock is held
        const state = await lockManager.isLocked('merge')
        expect(state).not.toBeNull()
        executed = true
        return 'success'
      })

      expect(executed).toBe(true)
      expect(result).toBe('success')

      // Lock should be released
      const state = await lockManager.isLocked('merge')
      expect(state).toBeNull()
    })

    it('should release lock even if function throws', async () => {
      await expect(
        withLock(lockManager, 'merge', async () => {
          throw new Error('Test error')
        })
      ).rejects.toThrow('Test error')

      // Lock should still be released
      const state = await lockManager.isLocked('merge')
      expect(state).toBeNull()
    })

    it('should throw LockAcquisitionError if lock unavailable', async () => {
      // Hold the lock
      const first = await lockManager.tryAcquire('merge', { timeout: 10000 })

      // Create the promise and attach the rejection handler immediately
      // to prevent unhandled rejection
      const lockPromise = withLock(
        lockManager,
        'merge',
        async () => 'should not execute',
        { waitTimeout: 100 }
      )

      // Attach error handler first to avoid unhandled rejection
      const resultPromise = lockPromise.then(
        () => { throw new Error('Expected rejection but got resolution') },
        (error) => error
      )

      // Advance time to trigger the timeout
      await advanceTime(150)

      // Now get the error
      const error = await resultPromise
      expect(error).toBeInstanceOf(LockAcquisitionError)

      await first.lock!.release()
    })
  })

  // ===========================================================================
  // Concurrent Lock Attempts
  // ===========================================================================

  describe('Concurrent Lock Attempts', () => {
    it('should handle multiple concurrent lock attempts', async () => {
      const attempts = 10
      const results: { acquired: boolean; index: number }[] = []

      // Fire all attempts simultaneously
      const promises = Array.from({ length: attempts }, (_, i) =>
        lockManager.tryAcquire('merge').then(result => ({
          acquired: result.acquired,
          index: i,
        }))
      )

      const all = await Promise.all(promises)
      results.push(...all)

      // Exactly one should succeed
      const successes = results.filter(r => r.acquired)
      expect(successes.length).toBe(1)

      await lockManager.forceRelease('merge')
    })

    it('should serialize operations with withLock', async () => {
      const operations = 5
      const order: number[] = []

      // Start all operations - they will queue up waiting for the lock
      const promises = Array.from({ length: operations }, (_, i) =>
        withLock(lockManager, 'merge', async () => {
          // Record which operation ran
          order.push(i)
        }, { waitTimeout: 5000, retryInterval: 50 })
      )

      // Advance time incrementally to allow each operation to complete
      // Each operation needs time to acquire the lock and complete
      for (let i = 0; i < operations * 2; i++) {
        await advanceTime(100)
      }

      await Promise.all(promises)

      // All operations should have completed
      expect(order.length).toBe(operations)
    })
  })

  // ===========================================================================
  // Lock State
  // ===========================================================================

  describe('Lock State', () => {
    it('should include correct metadata in lock state', async () => {
      const result = await lockManager.tryAcquire('merge', {
        metadata: { operation: 'test-merge', user: 'test-user' },
      })
      expect(result.acquired).toBe(true)

      const state = result.lock!.getState()
      expect(state.resource).toBe('merge')
      expect(state.holder).toBeDefined()
      expect(state.acquiredAt).toBeDefined()
      expect(state.expiresAt).toBeDefined()
      expect(state.metadata).toEqual({ operation: 'test-merge', user: 'test-user' })

      await result.lock!.release()
    })

    it('should persist lock state to storage', async () => {
      const result = await lockManager.tryAcquire('merge')
      expect(result.acquired).toBe(true)

      // Verify file exists in storage
      const lockPath = `${LOCK_DIR}/merge.lock`
      const exists = await storage.exists(lockPath)
      expect(exists).toBe(true)

      // Read and verify content
      const data = await storage.read(lockPath)
      const state = JSON.parse(new TextDecoder().decode(data)) as LockState
      expect(state.resource).toBe('merge')

      await result.lock!.release()
    })
  })

  // ===========================================================================
  // Multiple Lock Managers
  // ===========================================================================

  describe('Multiple Lock Managers', () => {
    it('should coordinate between different lock manager instances', async () => {
      const manager1 = createLockManager(storage, 'process-1')
      const manager2 = createLockManager(storage, 'process-2')

      // Manager 1 acquires
      const lock1 = await manager1.tryAcquire('merge')
      expect(lock1.acquired).toBe(true)

      // Manager 2 should fail
      const lock2 = await manager2.tryAcquire('merge')
      expect(lock2.acquired).toBe(false)
      expect(lock2.currentHolder?.holder).toBe('process-1')

      // Manager 1 releases
      await lock1.lock!.release()

      // Manager 2 should now succeed
      const lock3 = await manager2.tryAcquire('merge')
      expect(lock3.acquired).toBe(true)
      await lock3.lock!.release()
    })

    it('should show correct holder when lock is contested', async () => {
      const manager1 = createLockManager(storage, 'process-1')
      const manager2 = createLockManager(storage, 'process-2')

      // Manager 1 acquires
      await manager1.tryAcquire('merge')

      // Manager 2 checks status
      const state = await manager2.isLocked('merge')
      expect(state).not.toBeNull()
      expect(state!.holder).toBe('process-1')

      await manager1.forceRelease('merge')
    })
  })
})
