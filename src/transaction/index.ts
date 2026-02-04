/**
 * Unified Transaction Abstraction for ParqueDB
 *
 * This module provides a consistent transaction interface across all ParqueDB
 * components including the database layer, storage backends, and integrations.
 *
 * Key Features:
 * - Unified Transaction interface with commit/rollback semantics
 * - TransactionManager for managing transaction lifecycle
 * - withTransaction() helper for automatic commit/rollback handling
 * - Support for nested transactions via savepoints
 * - Rollback tracking with operation snapshots
 *
 * @example
 * ```typescript
 * // Using withTransaction helper
 * const result = await withTransaction(db, async (tx) => {
 *   await tx.create('users', { name: 'Alice' })
 *   await tx.create('posts', { title: 'Hello', authorId: 'users/alice' })
 *   return { success: true }
 * })
 *
 * // Manual transaction management
 * const manager = new TransactionManager()
 * const tx = manager.begin({ timeout: 5000 })
 * try {
 *   await doWork(tx)
 *   await tx.commit()
 * } catch (error) {
 *   await tx.rollback()
 *   throw error
 * }
 * ```
 */

import {
  DEFAULT_TIMEOUT_MS,
  DEFAULT_TRANSACTION_RETRY_DELAY,
  DEFAULT_MAX_RETRIES,
} from '../constants'
import { logger } from '../utils/logger'

// =============================================================================
// Core Types
// =============================================================================

/**
 * Transaction status
 */
export type TransactionStatus =
  | 'pending'      // Transaction is active
  | 'committed'    // Transaction was successfully committed
  | 'rolled_back'  // Transaction was rolled back
  | 'failed'       // Transaction failed (error during commit/rollback)

/**
 * Transaction isolation level
 *
 * Note: ParqueDB implements optimistic concurrency control. These levels
 * affect conflict detection behavior rather than locking.
 */
export type IsolationLevel =
  | 'read_uncommitted'  // Sees uncommitted changes from other transactions
  | 'read_committed'    // Only sees committed changes (default)
  | 'repeatable_read'   // Snapshot isolation - sees consistent snapshot
  | 'serializable'      // Full isolation with conflict detection

/**
 * Options for beginning a transaction
 */
export interface TransactionOptions {
  /** Optional transaction ID (auto-generated if not provided) */
  id?: string | undefined

  /** Timeout in milliseconds (default: 30000) */
  timeout?: number | undefined

  /** Isolation level (default: 'read_committed') */
  isolation?: IsolationLevel | undefined

  /** Whether to allow nested transactions via savepoints */
  allowNested?: boolean | undefined

  /** Actor performing the transaction */
  actor?: string | undefined

  /** Custom metadata to attach to the transaction */
  metadata?: Record<string, unknown> | undefined
}

/**
 * Operation recorded in a transaction for rollback
 */
export interface TransactionOperation<T = unknown> {
  /** Operation type */
  type: 'create' | 'update' | 'delete' | 'read' | 'write' | 'custom'

  /** Target namespace or path */
  target: string

  /** Entity or resource ID */
  id?: string | undefined

  /** State before the operation (for rollback) */
  beforeState?: T | undefined

  /** State after the operation */
  afterState?: T | undefined

  /** Operation timestamp */
  timestamp: Date

  /** Sequence number within transaction */
  sequence: number
}

/**
 * Savepoint for nested transactions
 */
export interface Savepoint {
  /** Savepoint name */
  name: string

  /** Operation sequence at savepoint creation */
  sequence: number

  /** Timestamp */
  createdAt: Date
}

// =============================================================================
// Transaction Interface
// =============================================================================

/**
 * Unified Transaction interface
 *
 * This interface provides a consistent contract for transactions across
 * all ParqueDB components. Implementations may support different subsets
 * of functionality based on their capabilities.
 */
export interface Transaction<TContext = unknown> {
  /** Unique transaction identifier */
  readonly id: string

  /** Current transaction status */
  readonly status: TransactionStatus

  /** When the transaction was started */
  readonly startedAt: Date

  /** Transaction context (database, storage, etc.) */
  readonly context: TContext

  /**
   * Commit the transaction
   *
   * Persists all operations performed within the transaction.
   * After commit, the transaction cannot be used for further operations.
   *
   * @throws TransactionError if commit fails
   */
  commit(): Promise<void>

  /**
   * Rollback the transaction
   *
   * Undoes all operations performed within the transaction.
   * After rollback, the transaction cannot be used for further operations.
   *
   * @throws TransactionError if rollback fails
   */
  rollback(): Promise<void>

  /**
   * Create a savepoint for partial rollback
   *
   * Savepoints allow rolling back to a specific point within a transaction
   * without rolling back the entire transaction.
   *
   * @param name - Savepoint name
   * @throws TransactionError if savepoints not supported
   */
  savepoint?(name: string): Promise<void>

  /**
   * Rollback to a savepoint
   *
   * Undoes all operations after the named savepoint.
   *
   * @param name - Savepoint name
   * @throws TransactionError if savepoint not found
   */
  rollbackToSavepoint?(name: string): Promise<void>

  /**
   * Release a savepoint
   *
   * Removes the savepoint without rolling back.
   *
   * @param name - Savepoint name
   */
  releaseSavepoint?(name: string): Promise<void>

  /**
   * Check if the transaction is active
   */
  isActive(): boolean

  /**
   * Get all operations in the transaction
   */
  getOperations(): TransactionOperation[]

  /**
   * Record an operation for rollback tracking
   */
  recordOperation(op: Omit<TransactionOperation, 'timestamp' | 'sequence'>): void
}

// =============================================================================
// Database Transaction Interface
// =============================================================================

/**
 * Database-level transaction with entity operations
 *
 * Extends the base Transaction interface with CRUD operations
 * for ParqueDB entities.
 */
export interface DatabaseTransaction extends Transaction {
  /**
   * Create an entity within the transaction
   */
  create<T = Record<string, unknown>>(
    namespace: string,
    data: T,
    options?: { actor?: string | undefined } | undefined
  ): Promise<T & { $id: string }>

  /**
   * Update an entity within the transaction
   */
  update<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    update: Record<string, unknown>,
    options?: { actor?: string | undefined; expectedVersion?: number | undefined } | undefined
  ): Promise<T | null>

  /**
   * Delete an entity within the transaction
   */
  delete(
    namespace: string,
    id: string,
    options?: { actor?: string | undefined; soft?: boolean | undefined } | undefined
  ): Promise<{ deletedCount: number }>

  /**
   * Get an entity within the transaction
   *
   * Returns the in-transaction state, which may differ from committed state.
   */
  get<T = Record<string, unknown>>(
    namespace: string,
    id: string
  ): Promise<T | null>
}

// =============================================================================
// Storage Transaction Interface
// =============================================================================

/**
 * Storage-level transaction for file operations
 *
 * Extends the base Transaction interface with file I/O operations.
 */
export interface StorageTransaction extends Transaction {
  /**
   * Read file within transaction
   */
  read(path: string): Promise<Uint8Array>

  /**
   * Write file within transaction
   */
  write(path: string, data: Uint8Array): Promise<void>

  /**
   * Delete file within transaction
   */
  delete(path: string): Promise<void>

  /**
   * Check if file exists within transaction
   */
  exists?(path: string): Promise<boolean>
}

// =============================================================================
// Transaction Manager
// =============================================================================

/**
 * Transaction manager state
 */
interface TransactionState {
  transaction: Transaction
  options: TransactionOptions
  operations: TransactionOperation[]
  savepoints: Savepoint[]
  timeoutHandle?: ReturnType<typeof setTimeout> | undefined
}

/**
 * Transaction Manager
 *
 * Manages transaction lifecycle including creation, tracking, and cleanup.
 * Supports concurrent transactions and automatic timeout handling.
 */
export class TransactionManager<TContext = unknown> {
  private transactions: Map<string, TransactionState> = new Map()
  private idCounter: number = 0

  /**
   * Default transaction options
   */
  readonly defaults: Required<Omit<TransactionOptions, 'id' | 'metadata'>> = {
    timeout: DEFAULT_TIMEOUT_MS,
    isolation: 'read_committed',
    allowNested: false,
    actor: 'system',
  }

  /**
   * Generate a unique transaction ID
   */
  private generateId(): string {
    const timestamp = Date.now()
    const random = Math.random().toString(36).substring(2, 11)
    return `txn_${++this.idCounter}_${timestamp}_${random}`
  }

  /**
   * Begin a new transaction
   *
   * @param context - Transaction context (database, storage, etc.)
   * @param options - Transaction options
   * @returns Transaction instance
   */
  begin(context: TContext, options: TransactionOptions = {}): Transaction<TContext> {
    const id = options.id ?? this.generateId()
    const startedAt = new Date()

    const mergedOptions: TransactionOptions = {
      ...this.defaults,
      ...options,
      id,
    }

    const operations: TransactionOperation[] = []
    const savepoints: Savepoint[] = []
    let status: TransactionStatus = 'pending'
    let sequenceCounter = 0

    const self = this

    const transaction: Transaction<TContext> = {
      id,
      get status() { return status },
      startedAt,
      context,

      async commit(): Promise<void> {
        if (status !== 'pending') {
          throw new TransactionError(
            `Cannot commit transaction in '${status}' status`,
            'INVALID_STATE',
            id
          )
        }

        try {
          // Clear timeout
          const state = self.transactions.get(id)
          if (state?.timeoutHandle) {
            clearTimeout(state.timeoutHandle)
          }

          status = 'committed'
          self.transactions.delete(id)
        } catch (error) {
          status = 'failed'
          throw new TransactionError(
            `Failed to commit transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
            'COMMIT_FAILED',
            id,
            error instanceof Error ? error : undefined
          )
        }
      },

      async rollback(): Promise<void> {
        if (status !== 'pending') {
          throw new TransactionError(
            `Cannot rollback transaction in '${status}' status`,
            'INVALID_STATE',
            id
          )
        }

        try {
          // Clear timeout
          const state = self.transactions.get(id)
          if (state?.timeoutHandle) {
            clearTimeout(state.timeoutHandle)
          }

          status = 'rolled_back'
          self.transactions.delete(id)
        } catch (error) {
          status = 'failed'
          throw new TransactionError(
            `Failed to rollback transaction: ${error instanceof Error ? error.message : 'Unknown error'}`,
            'ROLLBACK_FAILED',
            id,
            error instanceof Error ? error : undefined
          )
        }
      },

      async savepoint(name: string): Promise<void> {
        if (status !== 'pending') {
          throw new TransactionError(
            'Cannot create savepoint on inactive transaction',
            'INVALID_STATE',
            id
          )
        }

        if (savepoints.some(sp => sp.name === name)) {
          throw new TransactionError(
            `Savepoint '${name}' already exists`,
            'SAVEPOINT_EXISTS',
            id
          )
        }

        savepoints.push({
          name,
          sequence: sequenceCounter,
          createdAt: new Date(),
        })
      },

      async rollbackToSavepoint(name: string): Promise<void> {
        if (status !== 'pending') {
          throw new TransactionError(
            'Cannot rollback to savepoint on inactive transaction',
            'INVALID_STATE',
            id
          )
        }

        const savepointIndex = savepoints.findIndex(sp => sp.name === name)
        if (savepointIndex === -1) {
          throw new TransactionError(
            `Savepoint '${name}' not found`,
            'SAVEPOINT_NOT_FOUND',
            id
          )
        }

        const savepoint = savepoints[savepointIndex]!

        // Remove operations after savepoint
        while (operations.length > 0 && operations[operations.length - 1]!.sequence > savepoint.sequence) {
          operations.pop()
        }

        // Remove savepoints after this one
        savepoints.splice(savepointIndex + 1)
      },

      async releaseSavepoint(name: string): Promise<void> {
        const index = savepoints.findIndex(sp => sp.name === name)
        if (index !== -1) {
          savepoints.splice(index, 1)
        }
      },

      isActive(): boolean {
        return status === 'pending'
      },

      getOperations(): TransactionOperation[] {
        return [...operations]
      },

      recordOperation(op: Omit<TransactionOperation, 'timestamp' | 'sequence'>): void {
        if (status !== 'pending') {
          throw new TransactionError(
            'Cannot record operation on inactive transaction',
            'INVALID_STATE',
            id
          )
        }

        operations.push({
          ...op,
          timestamp: new Date(),
          sequence: ++sequenceCounter,
        })
      },
    }

    // Set up timeout
    let timeoutHandle: ReturnType<typeof setTimeout> | undefined
    if (mergedOptions.timeout && mergedOptions.timeout > 0) {
      timeoutHandle = setTimeout(async () => {
        if (status === 'pending') {
          try {
            await transaction.rollback()
          } catch (rollbackError) {
            // Log rollback errors on timeout - these indicate potential state inconsistency
            // The error is caught to prevent unhandled rejection, but logged for debugging
            logger.warn(`[TransactionManager] Rollback on timeout failed for transaction ${id}:`, rollbackError)
          }
        }
      }, mergedOptions.timeout)
    }

    // Store transaction state
    this.transactions.set(id, {
      transaction,
      options: mergedOptions,
      operations,
      savepoints,
      timeoutHandle,
    })

    return transaction
  }

  /**
   * Get an active transaction by ID
   */
  get(id: string): Transaction<TContext> | undefined {
    return this.transactions.get(id)?.transaction
  }

  /**
   * Check if a transaction is active
   */
  isActive(id: string): boolean {
    const state = this.transactions.get(id)
    return state?.transaction.isActive() ?? false
  }

  /**
   * Get all active transactions
   */
  getActiveTransactions(): Transaction<TContext>[] {
    return Array.from(this.transactions.values())
      .filter(state => state.transaction.isActive())
      .map(state => state.transaction)
  }

  /**
   * Get the number of active transactions
   */
  getActiveCount(): number {
    return this.getActiveTransactions().length
  }

  /**
   * Clean up stale transactions (for debugging/maintenance)
   *
   * @param maxAge - Maximum age in milliseconds
   * @returns Number of transactions cleaned up
   */
  cleanup(maxAge: number = 5 * 60 * 1000): number {
    const cutoff = Date.now() - maxAge
    let cleaned = 0

    for (const [id, state] of this.transactions.entries()) {
      if (state.transaction.startedAt.getTime() < cutoff) {
        if (state.timeoutHandle) {
          clearTimeout(state.timeoutHandle)
        }
        this.transactions.delete(id)
        cleaned++
      }
    }

    return cleaned
  }

  /**
   * Force cleanup all transactions (for shutdown)
   */
  async shutdown(): Promise<void> {
    for (const state of this.transactions.values()) {
      if (state.timeoutHandle) {
        clearTimeout(state.timeoutHandle)
      }
      if (state.transaction.isActive()) {
        try {
          await state.transaction.rollback()
        } catch {
          // Ignore errors during shutdown
        }
      }
    }
    this.transactions.clear()
  }
}

// =============================================================================
// withTransaction Helper
// =============================================================================

/**
 * Context provider interface for withTransaction
 */
export interface TransactionProvider<_TContext, TTx extends Transaction = Transaction> {
  beginTransaction(options?: TransactionOptions): TTx
}

/**
 * Execute a function within a transaction
 *
 * Automatically commits on success or rolls back on error.
 * This is the recommended way to use transactions in ParqueDB.
 *
 * @param provider - Object with beginTransaction method (e.g., ParqueDB instance)
 * @param fn - Function to execute within the transaction
 * @param options - Transaction options
 * @returns Result of the function
 * @throws The original error if the function throws (after rollback)
 *
 * @example
 * ```typescript
 * const result = await withTransaction(db, async (tx) => {
 *   const user = await tx.create('users', { name: 'Alice' })
 *   const post = await tx.create('posts', { title: 'Hello', authorId: user.$id })
 *   return { user, post }
 * })
 * ```
 */
export async function withTransaction<TContext, TTx extends Transaction, TResult>(
  provider: TransactionProvider<TContext, TTx>,
  fn: (tx: TTx) => Promise<TResult>,
  options?: TransactionOptions
): Promise<TResult> {
  const tx = provider.beginTransaction(options)

  try {
    const result = await fn(tx)
    await tx.commit()
    return result
  } catch (error) {
    try {
      await tx.rollback()
    } catch (rollbackError) {
      // Log rollback error but throw original error
      logger.error('Transaction rollback failed:', rollbackError)
    }
    throw error
  }
}

/**
 * Execute a function within a transaction with retry on conflict
 *
 * Useful for optimistic concurrency control scenarios where
 * transactions may fail due to version conflicts.
 *
 * @param provider - Object with beginTransaction method
 * @param fn - Function to execute within the transaction
 * @param options - Options including retry configuration
 * @returns Result of the function
 * @throws The last error if all retries fail
 *
 * @example
 * ```typescript
 * const result = await withRetry(db, async (tx) => {
 *   const user = await tx.get('users', 'users/123')
 *   await tx.update('users', 'users/123', {
 *     $inc: { balance: -100 }
 *   }, { expectedVersion: user.version })
 *   return user
 * }, { maxRetries: 3, retryDelay: 100 })
 * ```
 */
export async function withRetry<TContext, TTx extends Transaction, TResult>(
  provider: TransactionProvider<TContext, TTx>,
  fn: (tx: TTx) => Promise<TResult>,
  options: TransactionOptions & {
    maxRetries?: number | undefined
    retryDelay?: number | undefined
    shouldRetry?: ((error: unknown) => boolean) | undefined
  } = {}
): Promise<TResult> {
  const {
    maxRetries = DEFAULT_MAX_RETRIES,
    retryDelay = DEFAULT_TRANSACTION_RETRY_DELAY,
    shouldRetry = isRetryableError,
    ...txOptions
  } = options

  let lastError: unknown
  let attempts = 0

  while (attempts <= maxRetries) {
    try {
      return await withTransaction(provider, fn, txOptions)
    } catch (error) {
      lastError = error
      attempts++

      if (attempts > maxRetries || !shouldRetry(error)) {
        throw error
      }

      // Exponential backoff with jitter
      const delay = retryDelay * Math.pow(2, attempts - 1) * (0.5 + Math.random() * 0.5)
      await new Promise(resolve => setTimeout(resolve, delay))
    }
  }

  throw lastError
}

/**
 * Default function to determine if an error is retryable
 */
function isRetryableError(error: unknown): boolean {
  if (error instanceof TransactionError) {
    return error.code === 'VERSION_CONFLICT' || error.code === 'COMMIT_CONFLICT'
  }

  // Check for version conflict errors from other sources
  if (error instanceof Error) {
    const message = error.message.toLowerCase()
    return (
      message.includes('version conflict') ||
      message.includes('version mismatch') ||
      message.includes('optimistic locking') ||
      message.includes('concurrent modification')
    )
  }

  return false
}

// =============================================================================
// Transaction Errors
// =============================================================================

/**
 * Error codes for transaction operations
 */
export type TransactionErrorCode =
  | 'INVALID_STATE'      // Transaction in invalid state for operation
  | 'COMMIT_FAILED'      // Commit operation failed
  | 'COMMIT_CONFLICT'    // Commit failed due to concurrent modification
  | 'ROLLBACK_FAILED'    // Rollback operation failed
  | 'TIMEOUT'            // Transaction timed out
  | 'SAVEPOINT_EXISTS'   // Savepoint with name already exists
  | 'SAVEPOINT_NOT_FOUND'// Savepoint not found
  | 'VERSION_CONFLICT'   // Optimistic concurrency conflict
  | 'NESTED_NOT_ALLOWED' // Nested transactions not allowed
  | 'UNKNOWN'            // Unknown error

/**
 * Transaction-specific error class
 */
export class TransactionError extends Error {
  readonly code: TransactionErrorCode
  readonly transactionId?: string | undefined
  override readonly cause?: Error | undefined

  constructor(
    message: string,
    code: TransactionErrorCode = 'UNKNOWN',
    transactionId?: string,
    cause?: Error
  ) {
    super(message)
    this.name = 'TransactionError'
    this.code = code
    this.transactionId = transactionId
    this.cause = cause

    // Maintain proper stack trace in V8 environments
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, TransactionError)
    }
  }

  /**
   * Create from another error
   */
  static fromError(
    error: Error,
    code: TransactionErrorCode = 'UNKNOWN',
    transactionId?: string
  ): TransactionError {
    const txError = new TransactionError(error.message, code, transactionId, error)
    if (error.stack) txError.stack = error.stack
    return txError
  }

  /**
   * Check if an error is a TransactionError
   */
  static isTransactionError(error: unknown): error is TransactionError {
    return error instanceof TransactionError
  }

  /**
   * Check if an error is a specific type
   */
  static isCode(error: unknown, code: TransactionErrorCode): boolean {
    return TransactionError.isTransactionError(error) && error.code === code
  }
}

// =============================================================================
// Type Guards and Utilities
// =============================================================================

/**
 * Check if an object implements the Transaction interface
 */
export function isTransaction(obj: unknown): obj is Transaction {
  if (!obj || typeof obj !== 'object') return false

  const tx = obj as Record<string, unknown>
  return (
    typeof tx.id === 'string' &&
    typeof tx.commit === 'function' &&
    typeof tx.rollback === 'function' &&
    typeof tx.isActive === 'function'
  )
}

/**
 * Check if a transaction supports savepoints
 */
export function supportsSavepoints(tx: Transaction): tx is Transaction & {
  savepoint: (name: string) => Promise<void>
  rollbackToSavepoint: (name: string) => Promise<void>
} {
  return typeof tx.savepoint === 'function' && typeof tx.rollbackToSavepoint === 'function'
}

/**
 * Check if an object is a DatabaseTransaction
 */
export function isDatabaseTransaction(tx: Transaction): tx is DatabaseTransaction {
  const dbTx = tx as DatabaseTransaction
  return (
    typeof dbTx.create === 'function' &&
    typeof dbTx.update === 'function' &&
    typeof dbTx.delete === 'function'
  )
}

/**
 * Check if an object is a StorageTransaction
 */
export function isStorageTransaction(tx: Transaction): tx is StorageTransaction {
  const storageTx = tx as StorageTransaction
  return (
    typeof storageTx.read === 'function' &&
    typeof storageTx.write === 'function'
  )
}

// =============================================================================
// Re-exports for Compatibility
// =============================================================================

// Re-export types that may be used externally
export type {
  TransactionOptions as BeginTransactionOptions,
  TransactionOperation as TxOperation,
}
