/**
 * Distributed Locking for ParqueDB
 *
 * Provides advisory locking for merge operations to prevent concurrent
 * modifications from corrupting data. Works across:
 * - CLI (filesystem-based lock files)
 * - Workers (Durable Object-based locks)
 *
 * Lock Design:
 * - Advisory locks (check before operation, release after)
 * - Timeout/expiry for crash recovery
 * - Works with the storage backend abstraction
 *
 * @example
 * ```typescript
 * const lock = await lockManager.acquire('merge', { timeout: 30000 })
 * try {
 *   // Perform merge operation
 * } finally {
 *   await lock.release()
 * }
 * ```
 */

import type { StorageBackend } from '../types/storage'

// =============================================================================
// Types
// =============================================================================

/**
 * Lock resource identifiers
 */
export type LockResource =
  | 'merge'           // Branch merge operations
  | 'commit'          // Commit creation
  | 'sync'            // Push/pull sync operations
  | 'compact'         // Compaction operations
  | `namespace:${string}` // Per-namespace locks

/**
 * Lock state stored in storage
 */
export interface LockState {
  /** Lock resource identifier */
  resource: LockResource

  /** Lock holder identifier (process ID, worker ID, etc.) */
  holder: string

  /** Timestamp when lock was acquired (ISO string) */
  acquiredAt: string

  /** Timestamp when lock expires (ISO string) */
  expiresAt: string

  /** Optional metadata about the lock holder */
  metadata?: Record<string, unknown> | undefined
}

/**
 * Options for acquiring a lock
 */
export interface LockAcquireOptions {
  /** Lock timeout in milliseconds (default: 30000 = 30 seconds) */
  timeout?: number | undefined

  /** How long to wait to acquire the lock in milliseconds (default: 5000 = 5 seconds) */
  waitTimeout?: number | undefined

  /** Retry interval when waiting in milliseconds (default: 100) */
  retryInterval?: number | undefined

  /** Lock holder identifier (auto-generated if not provided) */
  holder?: string | undefined

  /** Optional metadata to store with the lock */
  metadata?: Record<string, unknown> | undefined
}

/**
 * Result of a lock acquisition attempt
 */
export interface LockAcquireResult {
  /** Whether the lock was acquired */
  acquired: boolean

  /** The lock handle if acquired */
  lock?: Lock | undefined

  /** If not acquired, information about the current lock holder */
  currentHolder?: LockState | undefined
}

/**
 * A held lock that can be released
 */
export interface Lock {
  /** The resource this lock protects */
  resource: LockResource

  /** Release the lock */
  release(): Promise<void>

  /** Check if the lock is still valid (not expired) */
  isValid(): boolean

  /** Extend the lock timeout */
  extend(additionalMs: number): Promise<boolean>

  /** Get the lock state */
  getState(): LockState
}

/**
 * Lock manager interface
 *
 * Implementations:
 * - FsLockManager: File-based locks for CLI
 * - DOLockManager: Durable Object-based locks for Workers
 */
export interface LockManager {
  /**
   * Acquire a lock on a resource
   *
   * @param resource - Resource to lock
   * @param options - Acquisition options
   * @returns Lock acquisition result
   */
  acquire(resource: LockResource, options?: LockAcquireOptions): Promise<LockAcquireResult>

  /**
   * Try to acquire a lock without waiting
   *
   * @param resource - Resource to lock
   * @param options - Acquisition options (waitTimeout is ignored)
   * @returns Lock acquisition result
   */
  tryAcquire(resource: LockResource, options?: LockAcquireOptions): Promise<LockAcquireResult>

  /**
   * Check if a resource is currently locked
   *
   * @param resource - Resource to check
   * @returns Current lock state if locked, null otherwise
   */
  isLocked(resource: LockResource): Promise<LockState | null>

  /**
   * Force release a lock (for admin/recovery purposes)
   *
   * @param resource - Resource to unlock
   * @returns Whether a lock was released
   */
  forceRelease(resource: LockResource): Promise<boolean>

  /**
   * List all currently held locks
   */
  listLocks(): Promise<LockState[]>
}

// =============================================================================
// Constants
// =============================================================================

/** Default lock timeout in milliseconds (30 seconds) */
export const DEFAULT_LOCK_TIMEOUT = 30_000

/** Default wait timeout in milliseconds (5 seconds) */
export const DEFAULT_WAIT_TIMEOUT = 5_000

/** Default retry interval in milliseconds */
export const DEFAULT_RETRY_INTERVAL = 100

/** Lock file directory */
export const LOCK_DIR = '_meta/locks'

/** Lock file extension */
export const LOCK_EXT = '.lock'

// =============================================================================
// Storage-Based Lock Manager
// =============================================================================

/**
 * Storage-based lock manager
 *
 * Uses the storage backend to implement distributed locking.
 * Works with any StorageBackend (FsBackend, R2Backend, MemoryBackend, etc.)
 *
 * Lock files are stored at `_meta/locks/{resource}.lock` and contain
 * JSON-encoded LockState.
 */
export class StorageLockManager implements LockManager {
  private readonly storage: StorageBackend
  private readonly holderId: string

  constructor(storage: StorageBackend, holderId?: string) {
    this.storage = storage
    this.holderId = holderId ?? generateHolderId()
  }

  /**
   * Get the path for a lock file
   */
  private getLockPath(resource: LockResource): string {
    // Sanitize resource name for file path
    const safeName = resource.replace(/[^a-zA-Z0-9_-]/g, '_')
    return `${LOCK_DIR}/${safeName}${LOCK_EXT}`
  }

  /**
   * Read current lock state from storage
   */
  private async readLockState(resource: LockResource): Promise<LockState | null> {
    const path = this.getLockPath(resource)

    try {
      const exists = await this.storage.exists(path)
      if (!exists) return null

      const data = await this.storage.read(path)
      const state = JSON.parse(new TextDecoder().decode(data)) as LockState

      // Check if lock has expired
      const expiresAt = new Date(state.expiresAt).getTime()
      if (Date.now() > expiresAt) {
        // Lock has expired - clean it up
        await this.storage.delete(path).catch(() => {
          // Ignore delete errors
        })
        return null
      }

      return state
    } catch {
      return null
    }
  }

  /**
   * Write lock state to storage
   */
  private async writeLockState(state: LockState): Promise<boolean> {
    const path = this.getLockPath(state.resource)

    try {
      // Ensure lock directory exists
      await this.storage.mkdir(LOCK_DIR).catch(() => {
        // Directory might already exist
      })

      const data = new TextEncoder().encode(JSON.stringify(state, null, 2))

      // Try to write atomically with "if none match" to prevent race conditions
      // If the storage backend supports conditional writes
      try {
        await this.storage.writeConditional(path, data, null)
        return true
      } catch (error) {
        // Check if this is a "file already exists" type error
        if (isAlreadyExistsError(error)) {
          return false
        }
        throw error
      }
    } catch {
      return false
    }
  }

  /**
   * Release a lock by deleting the lock file
   */
  private async releaseLock(resource: LockResource, expectedHolder: string): Promise<boolean> {
    const path = this.getLockPath(resource)

    try {
      // Verify we still hold the lock before releasing
      const current = await this.readLockState(resource)
      if (!current || current.holder !== expectedHolder) {
        return false
      }

      await this.storage.delete(path)
      return true
    } catch {
      return false
    }
  }

  async acquire(
    resource: LockResource,
    options: LockAcquireOptions = {}
  ): Promise<LockAcquireResult> {
    const waitTimeout = options.waitTimeout ?? DEFAULT_WAIT_TIMEOUT
    const retryInterval = options.retryInterval ?? DEFAULT_RETRY_INTERVAL
    const holder = options.holder ?? this.holderId

    const deadline = Date.now() + waitTimeout

    while (Date.now() < deadline) {
      const result = await this.tryAcquire(resource, { ...options, holder })
      if (result.acquired) {
        return result
      }

      // Wait before retrying
      await sleep(retryInterval)
    }

    // Failed to acquire within wait timeout
    const currentHolder = await this.readLockState(resource)
    return {
      acquired: false,
      currentHolder: currentHolder ?? undefined,
    }
  }

  async tryAcquire(
    resource: LockResource,
    options: LockAcquireOptions = {}
  ): Promise<LockAcquireResult> {
    const timeout = options.timeout ?? DEFAULT_LOCK_TIMEOUT
    const holder = options.holder ?? this.holderId

    // First, check if there's an existing lock and clean up if expired
    // This is purely for cleanup - the actual atomicity comes from writeConditional
    const existing = await this.readLockState(resource)
    if (existing) {
      // readLockState already cleaned up expired locks, so if we get here
      // the lock is still valid
      return {
        acquired: false,
        currentHolder: existing,
      }
    }

    const now = new Date()
    const expiresAt = new Date(now.getTime() + timeout)

    const state: LockState = {
      resource,
      holder,
      acquiredAt: now.toISOString(),
      expiresAt: expiresAt.toISOString(),
      metadata: options.metadata,
    }

    // Try to acquire atomically using conditional write
    // This is the ONLY place where we check-and-write atomically
    const acquired = await this.writeLockState(state)

    if (!acquired) {
      // Another process grabbed it first (race condition handled correctly)
      const currentHolder = await this.readLockState(resource)
      return {
        acquired: false,
        currentHolder: currentHolder ?? undefined,
      }
    }

    // Create lock handle
    const lock = this.createLock(state)

    return {
      acquired: true,
      lock,
    }
  }

  async isLocked(resource: LockResource): Promise<LockState | null> {
    return this.readLockState(resource)
  }

  async forceRelease(resource: LockResource): Promise<boolean> {
    const path = this.getLockPath(resource)
    try {
      const exists = await this.storage.exists(path)
      if (!exists) return false
      await this.storage.delete(path)
      return true
    } catch {
      return false
    }
  }

  async listLocks(): Promise<LockState[]> {
    const locks: LockState[] = []

    try {
      const result = await this.storage.list(LOCK_DIR)

      for (const file of result.files) {
        if (file.endsWith(LOCK_EXT)) {
          try {
            const data = await this.storage.read(file)
            const state = JSON.parse(new TextDecoder().decode(data)) as LockState

            // Check if not expired
            const expiresAt = new Date(state.expiresAt).getTime()
            if (Date.now() <= expiresAt) {
              locks.push(state)
            }
          } catch {
            // Skip invalid lock files
          }
        }
      }
    } catch {
      // Lock directory might not exist yet
    }

    return locks
  }

  /**
   * Create a Lock handle from a LockState
   */
  private createLock(state: LockState): Lock {
    const storage = this.storage
    const manager = this

    let currentState = state
    let released = false

    return {
      resource: state.resource,

      async release(): Promise<void> {
        if (released) return
        await manager.releaseLock(currentState.resource, currentState.holder)
        released = true
      },

      isValid(): boolean {
        if (released) return false
        const expiresAt = new Date(currentState.expiresAt).getTime()
        return Date.now() <= expiresAt
      },

      async extend(additionalMs: number): Promise<boolean> {
        if (released) return false

        const path = manager.getLockPath(currentState.resource)

        try {
          // Get current etag for conditional write
          const stat = await storage.stat(path)
          if (!stat || !stat.etag) {
            return false
          }

          // Read current state
          const data = await storage.read(path)
          const stored = JSON.parse(new TextDecoder().decode(data)) as LockState

          // Verify we still hold it
          if (stored.holder !== currentState.holder) {
            return false
          }

          // Extend expiry
          const newExpiresAt = new Date(Date.now() + additionalMs)
          const newState: LockState = {
            ...stored,
            expiresAt: newExpiresAt.toISOString(),
          }

          // Write updated state with conditional write to prevent TOCTOU race
          const newData = new TextEncoder().encode(JSON.stringify(newState, null, 2))
          await storage.writeConditional(path, newData, stat.etag)

          currentState = newState
          return true
        } catch {
          // Conditional write failed (etag mismatch) or other error
          return false
        }
      },

      getState(): LockState {
        return { ...currentState }
      },
    }
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Generate a unique holder ID
 */
function generateHolderId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 10)
  const pid = typeof process !== 'undefined' ? process.pid : 0
  return `${timestamp}-${random}-${pid}`
}

/**
 * Sleep for a given number of milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * Check if an error indicates the file already exists or a conditional write failed
 * because the file was created by another process (race condition)
 */
function isAlreadyExistsError(error: unknown): boolean {
  if (!(error instanceof Error)) return false

  const message = error.message.toLowerCase()
  const name = error.name.toLowerCase()

  // Check for ETagMismatchError - this is what writeConditional throws
  // when expectedVersion is null but the file already exists
  if (name === 'etagmismatcherror' || name.includes('etag')) {
    return true
  }

  // Check for StorageError with ETAG_MISMATCH code
  if ('code' in error && (error as { code: string }).code === 'ETAG_MISMATCH') {
    return true
  }

  return (
    message.includes('already exists') ||
    message.includes('eexist') ||
    message.includes('precondition') ||
    message.includes('etag mismatch') ||
    name.includes('exist') ||
    (error as NodeJS.ErrnoException).code === 'EEXIST'
  )
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a lock manager for the given storage backend
 *
 * @param storage - Storage backend to use for locks
 * @param holderId - Optional holder ID (auto-generated if not provided)
 * @returns A LockManager instance
 */
export function createLockManager(
  storage: StorageBackend,
  holderId?: string
): LockManager {
  return new StorageLockManager(storage, holderId)
}

// =============================================================================
// Lock Helpers
// =============================================================================

/**
 * Execute a function while holding a lock
 *
 * Automatically acquires the lock before executing and releases after,
 * even if the function throws an error.
 *
 * @param manager - Lock manager to use
 * @param resource - Resource to lock
 * @param fn - Function to execute while holding the lock
 * @param options - Lock acquisition options
 * @returns The result of the function
 * @throws If the lock cannot be acquired or if the function throws
 *
 * @example
 * ```typescript
 * const result = await withLock(lockManager, 'merge', async () => {
 *   return await performMerge()
 * })
 * ```
 */
export async function withLock<T>(
  manager: LockManager,
  resource: LockResource,
  fn: () => Promise<T>,
  options?: LockAcquireOptions
): Promise<T> {
  const result = await manager.acquire(resource, options)

  if (!result.acquired) {
    const holderInfo = result.currentHolder
      ? ` (held by ${result.currentHolder.holder} since ${result.currentHolder.acquiredAt})`
      : ''
    throw new LockAcquisitionError(resource, holderInfo)
  }

  try {
    return await fn()
  } finally {
    await result.lock!.release()
  }
}

/**
 * Error thrown when a lock cannot be acquired
 */
export class LockAcquisitionError extends Error {
  readonly name = 'LockAcquisitionError'

  constructor(
    public readonly resource: LockResource,
    details: string = ''
  ) {
    super(`Failed to acquire lock on '${resource}'${details}`)
  }
}

/**
 * Error thrown when an operation is attempted on an expired lock
 */
export class LockExpiredError extends Error {
  readonly name = 'LockExpiredError'

  constructor(public readonly resource: LockResource) {
    super(`Lock on '${resource}' has expired`)
  }
}
