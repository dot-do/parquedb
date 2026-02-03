/**
 * Unit tests for distributed locking mechanism
 *
 * Tests the StorageLockManager from src/sync/lock.ts which provides
 * distributed locking for merge operations to prevent concurrent
 * modifications from corrupting data.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  StorageLockManager,
  createLockManager,
  withLock,
  LockAcquisitionError,
  DEFAULT_LOCK_TIMEOUT,
  DEFAULT_WAIT_TIMEOUT,
  DEFAULT_RETRY_INTERVAL,
  LOCK_DIR,
  type LockResource,
  type LockState,
  type Lock,
  type LockManager,
} from '../../../src/sync/lock'

// =============================================================================
// Test Utilities
// =============================================================================

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

// =============================================================================
// StorageLockManager Tests
// =============================================================================

describe('StorageLockManager', () => {
  let storage: MemoryBackend
  let lockManager: LockManager

  beforeEach(() => {
    storage = new MemoryBackend()
    lockManager = createLockManager(storage)
  })

  describe('tryAcquire', () => {
    it('acquires lock when not held', async () => {
      const result = await lockManager.tryAcquire('merge')

      expect(result.acquired).toBe(true)
      expect(result.lock).toBeDefined()
      expect(result.lock!.resource).toBe('merge')
    })

    it('fails when lock already held by another holder', async () => {
      const manager1 = createLockManager(storage, 'process-1')
      const manager2 = createLockManager(storage, 'process-2')

      // First process acquires lock
      const result1 = await manager1.tryAcquire('merge')
      expect(result1.acquired).toBe(true)

      // Second process tries to acquire same lock
      const result2 = await manager2.tryAcquire('merge')
      expect(result2.acquired).toBe(false)
      expect(result2.currentHolder).toBeDefined()
      expect(result2.currentHolder!.holder).toBe('process-1')
    })

    it('supports multiple independent resources', async () => {
      const result1 = await lockManager.tryAcquire('merge')
      const result2 = await lockManager.tryAcquire('commit')
      const result3 = await lockManager.tryAcquire('sync')

      expect(result1.acquired).toBe(true)
      expect(result2.acquired).toBe(true)
      expect(result3.acquired).toBe(true)
    })

    it('respects custom timeout', async () => {
      const result = await lockManager.tryAcquire('merge', {
        timeout: 5000,
      })

      expect(result.acquired).toBe(true)
      const state = result.lock!.getState()
      const acquiredAt = new Date(state.acquiredAt).getTime()
      const expiresAt = new Date(state.expiresAt).getTime()
      expect(expiresAt - acquiredAt).toBe(5000)
    })

    it('supports namespace locks', async () => {
      const result = await lockManager.tryAcquire('namespace:users')

      expect(result.acquired).toBe(true)
      expect(result.lock!.resource).toBe('namespace:users')
    })
  })

  describe('acquire', () => {
    it('acquires lock immediately when available', async () => {
      const result = await lockManager.acquire('merge')

      expect(result.acquired).toBe(true)
      expect(result.lock).toBeDefined()
    })

    it('waits and retries when lock is held', async () => {
      const manager1 = createLockManager(storage, 'process-1')
      const manager2 = createLockManager(storage, 'process-2')

      // First process acquires lock with short timeout
      const result1 = await manager1.acquire('merge', {
        timeout: 100, // Lock expires after 100ms
      })
      expect(result1.acquired).toBe(true)

      // Second process waits for lock
      const acquirePromise = manager2.acquire('merge', {
        waitTimeout: 200,
        retryInterval: 50,
      })

      // Wait for first lock to expire
      await delay(150)

      const result2 = await acquirePromise
      expect(result2.acquired).toBe(true)
    })

    it('fails after wait timeout', async () => {
      const manager1 = createLockManager(storage, 'process-1')
      const manager2 = createLockManager(storage, 'process-2')

      // First process acquires lock with long timeout
      await manager1.acquire('merge', {
        timeout: 10000,
      })

      // Second process times out waiting
      const result2 = await manager2.acquire('merge', {
        waitTimeout: 100,
        retryInterval: 20,
      })

      expect(result2.acquired).toBe(false)
      expect(result2.currentHolder).toBeDefined()
    })
  })

  describe('Lock.release', () => {
    it('releases lock held by holder', async () => {
      const result = await lockManager.tryAcquire('merge')
      expect(result.acquired).toBe(true)

      await result.lock!.release()

      // Lock should now be available
      const result2 = await lockManager.tryAcquire('merge')
      expect(result2.acquired).toBe(true)
    })

    it('is idempotent (can call release multiple times)', async () => {
      const result = await lockManager.tryAcquire('merge')
      expect(result.acquired).toBe(true)

      await result.lock!.release()
      await result.lock!.release() // Should not throw
    })
  })

  describe('Lock.isValid', () => {
    it('returns true when lock is held and not expired', async () => {
      const result = await lockManager.tryAcquire('merge', {
        timeout: 10000,
      })
      expect(result.acquired).toBe(true)

      expect(result.lock!.isValid()).toBe(true)
    })

    it('returns false when lock is released', async () => {
      const result = await lockManager.tryAcquire('merge')
      expect(result.acquired).toBe(true)

      await result.lock!.release()

      expect(result.lock!.isValid()).toBe(false)
    })

    it('returns false when lock is expired', async () => {
      const result = await lockManager.tryAcquire('merge', {
        timeout: 50, // Very short timeout
      })
      expect(result.acquired).toBe(true)

      await delay(100) // Wait for expiry

      expect(result.lock!.isValid()).toBe(false)
    })
  })

  describe('Lock.extend', () => {
    it('extends lock expiry time', async () => {
      const result = await lockManager.tryAcquire('merge', {
        timeout: 1000,
      })
      expect(result.acquired).toBe(true)

      const originalExpiry = new Date(result.lock!.getState().expiresAt).getTime()

      await delay(100)

      const extended = await result.lock!.extend(5000)
      expect(extended).toBe(true)

      const newExpiry = new Date(result.lock!.getState().expiresAt).getTime()
      expect(newExpiry).toBeGreaterThan(originalExpiry)
    })

    it('fails if lock was released', async () => {
      const result = await lockManager.tryAcquire('merge')
      expect(result.acquired).toBe(true)

      await result.lock!.release()

      const extended = await result.lock!.extend(5000)
      expect(extended).toBe(false)
    })
  })

  describe('Lock.getState', () => {
    it('returns current lock state', async () => {
      const result = await lockManager.tryAcquire('merge', {
        holder: 'test-holder',
        metadata: { key: 'value' },
      })
      expect(result.acquired).toBe(true)

      const state = result.lock!.getState()

      expect(state.resource).toBe('merge')
      expect(state.holder).toBe('test-holder')
      expect(state.acquiredAt).toBeDefined()
      expect(state.expiresAt).toBeDefined()
      expect(state.metadata).toEqual({ key: 'value' })
    })
  })

  describe('isLocked', () => {
    it('returns null when no lock exists', async () => {
      const state = await lockManager.isLocked('merge')
      expect(state).toBeNull()
    })

    it('returns lock state when lock is held', async () => {
      await lockManager.tryAcquire('merge')

      const state = await lockManager.isLocked('merge')
      expect(state).not.toBeNull()
      expect(state!.resource).toBe('merge')
    })

    it('returns null when lock is expired', async () => {
      await lockManager.tryAcquire('merge', {
        timeout: 50,
      })

      await delay(100)

      const state = await lockManager.isLocked('merge')
      expect(state).toBeNull()
    })
  })

  describe('forceRelease', () => {
    it('releases lock regardless of holder', async () => {
      const manager1 = createLockManager(storage, 'process-1')

      await manager1.tryAcquire('merge')

      // Force release from another manager
      const released = await lockManager.forceRelease('merge')
      expect(released).toBe(true)

      // Lock should now be available
      const result = await lockManager.tryAcquire('merge')
      expect(result.acquired).toBe(true)
    })

    it('returns false when no lock exists', async () => {
      // forceRelease returns false if there was no lock to release
      const released = await lockManager.forceRelease('merge')
      expect(released).toBe(false)
    })
  })

  describe('listLocks', () => {
    it('returns empty list when no locks', async () => {
      const locks = await lockManager.listLocks()
      expect(locks).toEqual([])
    })

    it('returns all held locks', async () => {
      await lockManager.tryAcquire('merge')
      await lockManager.tryAcquire('commit')
      await lockManager.tryAcquire('sync')

      const locks = await lockManager.listLocks()

      expect(locks).toHaveLength(3)
      const resources = locks.map(l => l.resource)
      expect(resources).toContain('merge')
      expect(resources).toContain('commit')
      expect(resources).toContain('sync')
    })

    it('excludes expired locks', async () => {
      await lockManager.tryAcquire('merge', { timeout: 50 })
      await lockManager.tryAcquire('commit', { timeout: 10000 })

      await delay(100)

      const locks = await lockManager.listLocks()

      expect(locks).toHaveLength(1)
      expect(locks[0]!.resource).toBe('commit')
    })
  })
})

// =============================================================================
// withLock Helper Tests
// =============================================================================

describe('withLock', () => {
  let storage: MemoryBackend
  let lockManager: LockManager

  beforeEach(() => {
    storage = new MemoryBackend()
    lockManager = createLockManager(storage)
  })

  it('executes operation with lock held', async () => {
    let executed = false

    await withLock(lockManager, 'merge', async () => {
      executed = true
      const state = await lockManager.isLocked('merge')
      expect(state).not.toBeNull()
    })

    expect(executed).toBe(true)
  })

  it('releases lock after operation completes', async () => {
    await withLock(lockManager, 'merge', async () => {
      // Do nothing
    })

    const state = await lockManager.isLocked('merge')
    expect(state).toBeNull()
  })

  it('releases lock when operation throws', async () => {
    await expect(
      withLock(lockManager, 'merge', async () => {
        throw new Error('Operation failed')
      })
    ).rejects.toThrow('Operation failed')

    const state = await lockManager.isLocked('merge')
    expect(state).toBeNull()
  })

  it('returns operation result', async () => {
    const result = await withLock(lockManager, 'merge', async () => {
      return { success: true, data: 42 }
    })

    expect(result).toEqual({ success: true, data: 42 })
  })

  it('throws LockAcquisitionError when lock cannot be acquired', async () => {
    const manager1 = createLockManager(storage, 'process-1')
    const manager2 = createLockManager(storage, 'process-2')

    // Acquire lock with first manager
    await manager1.acquire('merge', { timeout: 10000 })

    // Try withLock with second manager
    await expect(
      withLock(manager2, 'merge', async () => {
        return 'should not reach here'
      }, { waitTimeout: 50 })
    ).rejects.toThrow(LockAcquisitionError)
  })

  it('prevents concurrent operations on same resource', async () => {
    const results: string[] = []
    const manager1 = createLockManager(storage, 'process-1')
    const manager2 = createLockManager(storage, 'process-2')

    const op1 = withLock(manager1, 'merge', async () => {
      results.push('op1-start')
      await delay(50)
      results.push('op1-end')
    })

    // Small delay to ensure op1 starts first
    await delay(10)

    const op2 = withLock(
      manager2,
      'merge',
      async () => {
        results.push('op2-start')
        await delay(10)
        results.push('op2-end')
      },
      { waitTimeout: 200, retryInterval: 20 }
    )

    await Promise.all([op1, op2])

    // op2 should only start after op1 completes
    expect(results.indexOf('op1-end')).toBeLessThan(results.indexOf('op2-start'))
  })
})

// =============================================================================
// Constants Tests
// =============================================================================

describe('Lock Constants', () => {
  it('has reasonable default timeout', () => {
    expect(DEFAULT_LOCK_TIMEOUT).toBe(30_000) // 30 seconds
  })

  it('has reasonable default wait timeout', () => {
    expect(DEFAULT_WAIT_TIMEOUT).toBe(5_000) // 5 seconds
  })

  it('has reasonable default retry interval', () => {
    expect(DEFAULT_RETRY_INTERVAL).toBe(100) // 100ms
  })

  it('uses standard lock directory', () => {
    expect(LOCK_DIR).toBe('_meta/locks')
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('Lock Integration', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  it('generates unique holder IDs', async () => {
    const manager1 = createLockManager(storage)
    const manager2 = createLockManager(storage)

    const result1 = await manager1.tryAcquire('merge')
    const state1 = result1.lock!.getState()
    await result1.lock!.release()

    const result2 = await manager2.tryAcquire('merge')
    const state2 = result2.lock!.getState()

    // Holder IDs should be unique
    expect(state1.holder).not.toBe(state2.holder)
  })

  it('locks stored in storage backend', async () => {
    await lockManager.tryAcquire('merge')

    // Verify lock file exists in storage
    const exists = await storage.exists('_meta/locks/merge.lock')
    expect(exists).toBe(true)
  })

  it('lock files are valid JSON', async () => {
    await lockManager.tryAcquire('merge')

    const data = await storage.read('_meta/locks/merge.lock')
    const text = new TextDecoder().decode(data)

    expect(() => JSON.parse(text)).not.toThrow()

    const parsed = JSON.parse(text)
    expect(parsed.resource).toBe('merge')
    expect(parsed.holder).toBeDefined()
    expect(parsed.acquiredAt).toBeDefined()
    expect(parsed.expiresAt).toBeDefined()
  })

  // Variable reused from outer scope - need to create local
  let lockManager: LockManager

  beforeEach(() => {
    storage = new MemoryBackend()
    lockManager = createLockManager(storage)
  })
})
