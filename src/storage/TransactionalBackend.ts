/**
 * TransactionalBackend - Storage backend wrapper with transaction support
 *
 * Wraps any StorageBackend implementation to provide ACID-like transactions
 * with read isolation, buffered writes, and rollback capability.
 *
 * Transaction semantics:
 * - Reads within a transaction see committed state + any writes made in the transaction
 * - Writes are buffered and only applied on commit
 * - Deletes are buffered and only applied on commit
 * - Rollback discards all pending changes
 * - Commit applies all changes atomically with automatic rollback on failure
 *
 * Atomicity guarantee:
 * - Before applying changes, original file states are captured (snapshot)
 * - If any operation fails during commit, all successfully applied changes are rolled back
 * - The storage is restored to its original pre-commit state on failure
 *
 * Limitations:
 * - No multi-transaction isolation (reads see globally committed state)
 * - No deadlock detection or timeout on transactions
 * - Rollback during commit failure is best-effort (cannot guarantee if storage itself fails)
 */

import type {
  StorageBackend,
  TransactionalBackend as ITransactionalBackend,
  Transaction,
  FileStat,
  ListOptions,
  ListResult,
  WriteOptions,
  WriteResult,
  RmdirOptions,
} from '../types/storage'
import { generateEtag } from './utils'
import { NotFoundError } from './errors'

// =============================================================================
// Transaction State
// =============================================================================

/** Pending write operation */
interface PendingWrite {
  type: 'write'
  data: Uint8Array
  options?: WriteOptions
}

/** Pending delete operation */
interface PendingDelete {
  type: 'delete'
}

/** Union of pending operations */
type PendingOperation = PendingWrite | PendingDelete

/** Snapshot of original file state before commit */
interface OriginalState {
  /** Path to the file */
  path: string
  /** Original data (null if file didn't exist) */
  data: Uint8Array | null
}

/** Transaction state */
interface TransactionState {
  /** Unique transaction ID */
  id: string

  /** Pending operations keyed by path */
  pending: Map<string, PendingOperation>

  /** Whether transaction is still active */
  active: boolean

  /** Timestamp when transaction started */
  startedAt: Date
}

// =============================================================================
// Transaction Implementation
// =============================================================================

/**
 * Transaction handle implementation
 *
 * Buffers all operations and applies them on commit.
 */
class TransactionImpl implements Transaction {
  readonly id: string

  constructor(
    private readonly backend: StorageBackend,
    private readonly state: TransactionState,
    private readonly onComplete: (txId: string) => void
  ) {
    this.id = state.id
  }

  /**
   * Ensure transaction is still active
   */
  private ensureActive(): void {
    if (!this.state.active) {
      throw new TransactionError(this.id, 'Transaction is no longer active')
    }
  }

  /**
   * Read within transaction
   *
   * If the path has a pending write, return that data.
   * If the path has a pending delete, throw NotFoundError.
   * Otherwise, read from the underlying backend.
   */
  async read(path: string): Promise<Uint8Array> {
    this.ensureActive()

    const pending = this.state.pending.get(path)

    if (pending) {
      if (pending.type === 'delete') {
        throw new NotFoundError(path)
      }
      // Return a copy of the pending write data
      return new Uint8Array(pending.data)
    }

    // Read from underlying backend
    return this.backend.read(path)
  }

  /**
   * Buffer a write operation
   */
  async write(path: string, data: Uint8Array): Promise<void> {
    this.ensureActive()

    this.state.pending.set(path, {
      type: 'write',
      data: new Uint8Array(data), // Copy to prevent external mutation
    })
  }

  /**
   * Buffer a delete operation
   */
  async delete(path: string): Promise<void> {
    this.ensureActive()

    this.state.pending.set(path, {
      type: 'delete',
    })
  }

  /**
   * Commit all pending operations atomically
   *
   * Applies all writes and deletes to the underlying backend.
   * Order: deletes first, then writes (to handle overwrite semantics).
   *
   * ATOMICITY GUARANTEE:
   * - Before applying any changes, captures original state of all affected files
   * - If any operation fails, rolls back all successfully applied changes
   * - On rollback failure, throws with both original and rollback errors
   */
  async commit(): Promise<void> {
    this.ensureActive()

    // Collect deletes and writes
    const deletes: string[] = []
    const writes: Array<{ path: string; data: Uint8Array; options?: WriteOptions }> = []

    this.state.pending.forEach((op, path) => {
      if (op.type === 'delete') {
        deletes.push(path)
      } else {
        writes.push({ path, data: op.data, options: op.options })
      }
    })

    // Phase 1: Capture original state of all affected files
    const originalStates: OriginalState[] = []
    const allPaths = new Set([...deletes, ...writes.map((w) => w.path)])

    for (const path of allPaths) {
      try {
        const data = await this.backend.read(path)
        originalStates.push({ path, data })
      } catch (error) {
        // File doesn't exist - record that it was absent
        if (error instanceof NotFoundError) {
          originalStates.push({ path, data: null })
        } else {
          // Unexpected error reading file - cannot guarantee atomicity
          this.state.active = false
          this.state.pending.clear()
          this.onComplete(this.id)
          throw new TransactionCommitError(this.id, [
            error instanceof Error ? error : new Error(String(error)),
          ])
        }
      }
    }

    // Phase 2: Apply all operations, tracking what was successfully applied
    const appliedOperations: Array<{ type: 'write' | 'delete'; path: string }> = []
    let commitError: Error | null = null

    // Apply deletes first
    for (const path of deletes) {
      try {
        await this.backend.delete(path)
        appliedOperations.push({ type: 'delete', path })
      } catch (error) {
        // Ignore not found errors on delete (file already doesn't exist)
        if (!(error instanceof NotFoundError)) {
          commitError = error instanceof Error ? error : new Error(String(error))
          break
        }
        // Still track it as applied since the goal (file not existing) is achieved
        appliedOperations.push({ type: 'delete', path })
      }
    }

    // Apply writes (if no error yet)
    if (!commitError) {
      for (const { path, data, options } of writes) {
        try {
          await this.backend.write(path, data, options)
          appliedOperations.push({ type: 'write', path })
        } catch (error) {
          commitError = error instanceof Error ? error : new Error(String(error))
          break
        }
      }
    }

    // Phase 3: If there was an error, rollback all applied changes
    if (commitError) {
      const rollbackErrors: Error[] = []

      // Rollback in reverse order
      for (let i = appliedOperations.length - 1; i >= 0; i--) {
        const op = appliedOperations[i]
        const original = originalStates.find((s) => s.path === op.path)

        if (!original) {
          // This shouldn't happen, but handle gracefully
          continue
        }

        try {
          if (original.data === null) {
            // File didn't exist before - delete it
            await this.backend.delete(op.path)
          } else {
            // File existed - restore original content
            await this.backend.write(op.path, original.data)
          }
        } catch (rollbackError) {
          // Track rollback errors but continue trying to rollback other files
          rollbackErrors.push(
            rollbackError instanceof Error ? rollbackError : new Error(String(rollbackError))
          )
        }
      }

      // Mark transaction as complete (even on failure)
      this.state.active = false
      this.state.pending.clear()
      this.onComplete(this.id)

      // Throw with all errors
      if (rollbackErrors.length > 0) {
        throw new TransactionCommitError(this.id, [
          commitError,
          new Error(
            `Rollback also failed with ${rollbackErrors.length} error(s): ${rollbackErrors.map((e) => e.message).join('; ')}`
          ),
        ])
      }
      throw new TransactionCommitError(this.id, [commitError])
    }

    // Phase 4: Success - mark transaction as complete
    this.state.active = false
    this.state.pending.clear()
    this.onComplete(this.id)
  }

  /**
   * Rollback all pending operations
   *
   * Discards all buffered changes without applying them.
   */
  async rollback(): Promise<void> {
    this.ensureActive()

    this.state.pending.clear()
    this.state.active = false
    this.onComplete(this.id)
  }
}

// =============================================================================
// TransactionalBackend Implementation
// =============================================================================

/**
 * TransactionalBackend wraps a StorageBackend to add transaction support
 *
 * @example
 * ```typescript
 * const backend = new MemoryBackend()
 * const txBackend = new TransactionalBackend(backend)
 *
 * const tx = await txBackend.beginTransaction()
 * await tx.write('file.txt', data)
 * await tx.commit()
 * ```
 */
export class TransactionalBackend implements ITransactionalBackend {
  readonly type: string

  /** Active transactions */
  private transactions = new Map<string, TransactionState>()

  /** Counter for generating transaction IDs */
  private txCounter = 0

  constructor(private readonly backend: StorageBackend) {
    this.type = `transactional:${backend.type}`
  }

  /**
   * Get the underlying backend
   */
  get inner(): StorageBackend {
    return this.backend
  }

  /**
   * Generate a unique transaction ID
   */
  private generateTxId(): string {
    return `tx-${++this.txCounter}-${Date.now().toString(36)}`
  }

  /**
   * Handle transaction completion (commit or rollback)
   */
  private onTransactionComplete = (txId: string): void => {
    this.transactions.delete(txId)
  }

  // =========================================================================
  // Transaction Management
  // =========================================================================

  /**
   * Begin a new transaction
   */
  async beginTransaction(): Promise<Transaction> {
    const id = this.generateTxId()

    const state: TransactionState = {
      id,
      pending: new Map(),
      active: true,
      startedAt: new Date(),
    }

    this.transactions.set(id, state)

    return new TransactionImpl(this.backend, state, this.onTransactionComplete)
  }

  /**
   * Get count of active transactions (useful for monitoring)
   */
  get activeTransactionCount(): number {
    return this.transactions.size
  }

  // =========================================================================
  // Pass-through Read Operations
  // =========================================================================

  async read(path: string): Promise<Uint8Array> {
    return this.backend.read(path)
  }

  async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
    return this.backend.readRange(path, start, end)
  }

  async exists(path: string): Promise<boolean> {
    return this.backend.exists(path)
  }

  async stat(path: string): Promise<FileStat | null> {
    return this.backend.stat(path)
  }

  async list(prefix: string, options?: ListOptions): Promise<ListResult> {
    return this.backend.list(prefix, options)
  }

  // =========================================================================
  // Pass-through Write Operations
  // =========================================================================

  async write(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    return this.backend.write(path, data, options)
  }

  async writeAtomic(path: string, data: Uint8Array, options?: WriteOptions): Promise<WriteResult> {
    return this.backend.writeAtomic(path, data, options)
  }

  async append(path: string, data: Uint8Array): Promise<void> {
    return this.backend.append(path, data)
  }

  async delete(path: string): Promise<boolean> {
    return this.backend.delete(path)
  }

  async deletePrefix(prefix: string): Promise<number> {
    return this.backend.deletePrefix(prefix)
  }

  // =========================================================================
  // Pass-through Directory Operations
  // =========================================================================

  async mkdir(path: string): Promise<void> {
    return this.backend.mkdir(path)
  }

  async rmdir(path: string, options?: RmdirOptions): Promise<void> {
    return this.backend.rmdir(path, options)
  }

  // =========================================================================
  // Pass-through Atomic Operations
  // =========================================================================

  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null,
    options?: WriteOptions
  ): Promise<WriteResult> {
    return this.backend.writeConditional(path, data, expectedVersion, options)
  }

  async copy(source: string, dest: string): Promise<void> {
    return this.backend.copy(source, dest)
  }

  async move(source: string, dest: string): Promise<void> {
    return this.backend.move(source, dest)
  }
}

// =============================================================================
// Error Classes
// =============================================================================

/**
 * Error thrown when a transaction operation fails
 */
export class TransactionError extends Error {
  constructor(
    public readonly transactionId: string,
    message: string
  ) {
    super(`Transaction ${transactionId}: ${message}`)
    this.name = 'TransactionError'
  }
}

/**
 * Error thrown when commit fails
 */
export class TransactionCommitError extends TransactionError {
  constructor(
    transactionId: string,
    public readonly errors: Error[]
  ) {
    super(
      transactionId,
      `Commit failed with ${errors.length} error(s): ${errors.map((e) => e.message).join('; ')}`
    )
    this.name = 'TransactionCommitError'
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Wrap a storage backend with transaction support
 *
 * @example
 * ```typescript
 * const backend = new MemoryBackend()
 * const txBackend = withTransactions(backend)
 *
 * const tx = await txBackend.beginTransaction()
 * await tx.write('file.txt', data)
 * await tx.commit()
 * ```
 */
export function withTransactions(backend: StorageBackend): TransactionalBackend {
  // Don't double-wrap
  if (backend instanceof TransactionalBackend) {
    return backend
  }
  return new TransactionalBackend(backend)
}

/**
 * Execute a function within a transaction, auto-committing on success or rolling back on error
 *
 * @example
 * ```typescript
 * const result = await runInTransaction(txBackend, async (tx) => {
 *   await tx.write('file1.txt', data1)
 *   await tx.write('file2.txt', data2)
 *   return 'success'
 * })
 * ```
 */
export async function runInTransaction<T>(
  backend: ITransactionalBackend,
  fn: (tx: Transaction) => Promise<T>
): Promise<T> {
  const tx = await backend.beginTransaction()

  try {
    const result = await fn(tx)
    await tx.commit()
    return result
  } catch (error) {
    await tx.rollback()
    throw error
  }
}
