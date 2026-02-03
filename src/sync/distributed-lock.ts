/**
 * Distributed Locking for ParqueDB
 *
 * Provides a distributed lock mechanism for coordinating concurrent merge
 * operations across multiple processes or workers. Uses storage backend's
 * conditional write capability for atomic lock acquisition.
 *
 * Design:
 * - Lock files stored at `_locks/{resource}.lock`
 * - Lock data includes owner, resource, timestamps, and version
 * - Uses ETag-based conditional writes for atomic operations
 * - TTL-based expiration prevents deadlocks from crashed processes
 * - Supports retry with exponential backoff
 *
 * @example
 * ```typescript
 * // Acquire lock for merge operation
 * const lock = await acquireLock(storage, {
 *   owner: 'worker-123',
 *   resource: 'merge:main',
 *   ttlMs: 30000,
 * })
 *
 * try {
 *   // Perform merge...
 * } finally {
 *   await releaseLock(storage, lock)
 * }
 *
 * // Or use withLock for automatic cleanup
 * const result = await withLock(storage, { owner: 'worker-123', resource: 'merge:main' }, async () => {
 *   return await performMerge()
 * })
 * ```
 */

import type { StorageBackend } from '../types/storage'
import { ETagMismatchError } from '../storage/errors'

// =============================================================================
// Types
// =============================================================================

/**
 * Represents an acquired distributed lock
 */
export interface DistributedLock {
  /** Process/worker ID that owns the lock */
  owner: string

  /** Resource being locked (e.g., 'merge:main') */
  resource: string

  /** Timestamp when lock was acquired */
  acquiredAt: number

  /** Timestamp when lock expires */
  expiresAt: number

  /** Version/ETag of the lock file for safe release */
  version: string
}

/**
 * Options for acquiring a lock
 */
export interface LockOptions {
  /** Process/worker ID that will own the lock */
  owner: string

  /** Resource to lock (e.g., 'merge', 'merge:main') */
  resource: string

  /** Time-to-live in milliseconds (default: 30000ms = 30 seconds) */
  ttlMs?: number

  /** Maximum number of acquisition retries (default: 0 = no retry) */
  maxRetries?: number

  /** Delay between retries in milliseconds (default: 1000ms) */
  retryDelayMs?: number
}

/**
 * Internal lock data stored in the lock file
 */
interface LockData {
  owner: string
  resource: string
  acquiredAt: number
  expiresAt: number
}

// =============================================================================
// Constants
// =============================================================================

/** Default lock TTL (30 seconds) */
const DEFAULT_TTL_MS = 30_000

/** Default retry delay (1 second) */
const DEFAULT_RETRY_DELAY_MS = 1_000

/**
 * Get the storage path for a lock resource
 */
export function LOCK_PATH(resource: string): string {
  return `_locks/${resource}.lock`
}

// =============================================================================
// Errors
// =============================================================================

/**
 * Error thrown when lock acquisition fails
 */
export class LockAcquisitionError extends Error {
  override readonly name = 'LockAcquisitionError'

  constructor(
    public readonly resource: string,
    public readonly currentOwner: string,
    message?: string
  ) {
    super(
      message ||
        `Failed to acquire lock for resource '${resource}': currently held by '${currentOwner}'`
    )
    Object.setPrototypeOf(this, LockAcquisitionError.prototype)
  }
}

/**
 * Error thrown when a lock has expired during operation
 */
export class LockExpiredError extends Error {
  override readonly name = 'LockExpiredError'

  constructor(
    public readonly resource: string,
    public readonly owner: string
  ) {
    super(`Lock for resource '${resource}' owned by '${owner}' has expired`)
    Object.setPrototypeOf(this, LockExpiredError.prototype)
  }
}

// =============================================================================
// Core Lock Functions
// =============================================================================

/**
 * Generate a unique version ID for lock tracking
 */
function generateVersion(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 10)
  return `${timestamp}-${random}`
}

/**
 * Acquire a distributed lock
 *
 * @param storage - Storage backend to use
 * @param options - Lock options
 * @returns Acquired lock object
 * @throws LockAcquisitionError if lock cannot be acquired
 *
 * @example
 * ```typescript
 * const lock = await acquireLock(storage, {
 *   owner: 'worker-123',
 *   resource: 'merge:main',
 *   ttlMs: 30000,
 *   maxRetries: 3,
 * })
 * ```
 */
export async function acquireLock(
  storage: StorageBackend,
  options: LockOptions
): Promise<DistributedLock> {
  const {
    owner,
    resource,
    ttlMs = DEFAULT_TTL_MS,
    maxRetries = 0,
    retryDelayMs = DEFAULT_RETRY_DELAY_MS,
  } = options

  const lockPath = LOCK_PATH(resource)

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    // Check existing lock
    const existingLock = await readLockFile(storage, lockPath)

    if (existingLock) {
      const now = Date.now()

      // Check if lock is expired
      if (existingLock.expiresAt <= now) {
        // Lock expired, try to take over
        const newLock = await tryAcquire(storage, lockPath, owner, resource, ttlMs, existingLock)
        if (newLock) {
          return newLock
        }
        // Failed to take over (another process got it), continue retry loop
      } else if (existingLock.owner === owner) {
        // Same owner - renew the lock (reentrant)
        const renewedLock = await tryAcquire(storage, lockPath, owner, resource, ttlMs, existingLock)
        if (renewedLock) {
          return renewedLock
        }
        // Failed to renew (someone else took it), continue retry loop
      } else {
        // Lock held by another owner
        if (attempt < maxRetries) {
          await sleep(retryDelayMs)
          continue
        }
        throw new LockAcquisitionError(resource, existingLock.owner)
      }
    } else {
      // No existing lock, try to create one
      const newLock = await tryCreateLock(storage, lockPath, owner, resource, ttlMs)
      if (newLock) {
        return newLock
      }
      // Failed to create (another process created one), continue retry loop
    }

    // If we reach here, we need to retry
    if (attempt < maxRetries) {
      await sleep(retryDelayMs)
    }
  }

  // Final attempt failed, check who owns the lock now
  const currentLock = await readLockFile(storage, lockPath)
  throw new LockAcquisitionError(
    resource,
    currentLock?.owner || 'unknown'
  )
}

/**
 * Try to create a new lock file atomically
 */
async function tryCreateLock(
  storage: StorageBackend,
  lockPath: string,
  owner: string,
  resource: string,
  ttlMs: number
): Promise<DistributedLock | null> {
  const now = Date.now()
  const lockData: LockData = {
    owner,
    resource,
    acquiredAt: now,
    expiresAt: now + ttlMs,
  }

  try {
    // Use writeConditional with null expectedVersion to create only if not exists
    const result = await storage.writeConditional(
      lockPath,
      new TextEncoder().encode(JSON.stringify(lockData, null, 2)),
      null // Only create if doesn't exist
    )

    return {
      ...lockData,
      version: result.etag,
    }
  } catch (error) {
    if (error instanceof ETagMismatchError) {
      // Lock already exists (created by another process)
      return null
    }
    // Check for other storage errors that indicate conditional write failure
    if (error instanceof Error && error.message.includes('etag')) {
      return null
    }
    throw error
  }
}

/**
 * Try to acquire lock by updating existing lock file
 */
async function tryAcquire(
  storage: StorageBackend,
  lockPath: string,
  owner: string,
  resource: string,
  ttlMs: number,
  existingLock: { version: string }
): Promise<DistributedLock | null> {
  const now = Date.now()
  const lockData: LockData = {
    owner,
    resource,
    acquiredAt: now,
    expiresAt: now + ttlMs,
  }

  try {
    // Use writeConditional with existing version for atomic update
    const result = await storage.writeConditional(
      lockPath,
      new TextEncoder().encode(JSON.stringify(lockData, null, 2)),
      existingLock.version
    )

    return {
      ...lockData,
      version: result.etag,
    }
  } catch (error) {
    if (error instanceof ETagMismatchError) {
      // Lock was modified by another process
      return null
    }
    // Check for other storage errors that indicate conditional write failure
    if (error instanceof Error && error.message.includes('etag')) {
      return null
    }
    throw error
  }
}

/**
 * Read lock file from storage
 */
async function readLockFile(
  storage: StorageBackend,
  lockPath: string
): Promise<(LockData & { version: string }) | null> {
  try {
    const exists = await storage.exists(lockPath)
    if (!exists) {
      return null
    }

    const data = await storage.read(lockPath)
    const stat = await storage.stat(lockPath)
    const lockData = JSON.parse(new TextDecoder().decode(data)) as LockData

    return {
      ...lockData,
      version: stat?.etag || '',
    }
  } catch (error) {
    // If file doesn't exist or is corrupted, treat as no lock
    return null
  }
}

/**
 * Release a distributed lock
 *
 * @param storage - Storage backend to use
 * @param lock - Lock to release (must be the lock returned by acquireLock)
 *
 * @example
 * ```typescript
 * const lock = await acquireLock(storage, { owner: 'worker-123', resource: 'merge' })
 * try {
 *   // ... do work
 * } finally {
 *   await releaseLock(storage, lock)
 * }
 * ```
 */
export async function releaseLock(
  storage: StorageBackend,
  lock: DistributedLock
): Promise<void> {
  const lockPath = LOCK_PATH(lock.resource)

  try {
    // Verify we still own the lock before deleting
    const currentLock = await readLockFile(storage, lockPath)

    if (!currentLock) {
      // Lock doesn't exist - already released or expired
      return
    }

    // Only release if we own it and version matches
    if (currentLock.owner !== lock.owner) {
      throw new Error(
        `Cannot release lock: owned by '${currentLock.owner}', not '${lock.owner}'`
      )
    }

    if (currentLock.version !== lock.version) {
      throw new Error(
        `Cannot release lock: version mismatch (expected ${lock.version}, got ${currentLock.version})`
      )
    }

    await storage.delete(lockPath)
  } catch (error) {
    // If lock doesn't exist, that's fine
    if (error instanceof Error && error.message.includes('not found')) {
      return
    }
    throw error
  }
}

/**
 * Check if a lock is currently held for a resource
 *
 * @param storage - Storage backend to use
 * @param resource - Resource to check
 * @returns true if lock is held and not expired
 */
export async function isLockHeld(
  storage: StorageBackend,
  resource: string
): Promise<boolean> {
  const lockPath = LOCK_PATH(resource)
  const lock = await readLockFile(storage, lockPath)

  if (!lock) {
    return false
  }

  // Check if lock is expired
  return lock.expiresAt > Date.now()
}

/**
 * Execute an operation while holding a lock
 *
 * Automatically acquires the lock before executing the operation and releases
 * it after completion (even if the operation throws).
 *
 * @param storage - Storage backend to use
 * @param options - Lock options
 * @param operation - Async operation to execute while holding the lock
 * @returns Result of the operation
 *
 * @example
 * ```typescript
 * const result = await withLock(
 *   storage,
 *   { owner: 'worker-123', resource: 'merge:main' },
 *   async () => {
 *     return await performMergeOperation()
 *   }
 * )
 * ```
 */
export async function withLock<T>(
  storage: StorageBackend,
  options: LockOptions,
  operation: () => Promise<T>
): Promise<T> {
  const lock = await acquireLock(storage, options)

  try {
    return await operation()
  } finally {
    await releaseLock(storage, lock)
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Sleep for a given number of milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}
