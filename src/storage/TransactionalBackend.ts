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
// Configuration
// =============================================================================

// Import from centralized constants
// Note: Using local constants for now to maintain existing API compatibility
// These can be unified with central constants in a future refactor

/** Default maximum number of operations per transaction */
const DEFAULT_MAX_TRANSACTION_OPERATIONS = 10000 // Matches constants.ts DEFAULT_MAX_EVENTS

/** Default maximum total bytes per transaction (100MB) */
const DEFAULT_MAX_TRANSACTION_BYTES = 100 * 1024 * 1024

/** Options for configuring TransactionalBackend */
export interface TransactionalBackendOptions {
  /**
   * Maximum number of operations (writes + deletes) allowed per transaction.
   * Prevents memory pressure from extremely large transactions.
   * @default 10000
   */
  maxTransactionOperations?: number | undefined

  /**
   * Maximum total bytes of write data allowed per transaction.
   * Prevents memory exhaustion from large payloads.
   * @default 104857600 (100MB)
   */
  maxTransactionBytes?: number | undefined
}

// =============================================================================
// Transaction State
// =============================================================================

/** Pending write operation */
interface PendingWrite {
  type: 'write'
  data: Uint8Array
  options?: WriteOptions | undefined
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

/** Transaction limits configuration */
interface TransactionLimits {
  /** Maximum number of operations allowed */
  maxOperations: number

  /** Maximum bytes allowed */
  maxBytes: number
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

  /** Current total bytes of pending write data */
  currentBytes: number

  /** Transaction limits */
  limits: TransactionLimits
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
   * Check if adding an operation would exceed transaction limits
   */
  private checkLimits(newBytes: number): void {
    const { limits, pending, currentBytes } = this.state

    // Check operation count limit
    // Note: We check after incrementing because this is a new operation
    if (pending.size >= limits.maxOperations) {
      throw new TransactionTooLargeError(
        this.id,
        'operations',
        limits.maxOperations,
        pending.size + 1
      )
    }

    // Check bytes limit
    const projectedBytes = currentBytes + newBytes
    if (projectedBytes > limits.maxBytes) {
      throw new TransactionTooLargeError(this.id, 'bytes', limits.maxBytes, projectedBytes)
    }
  }

  /**
   * Buffer a write operation
   */
  async write(path: string, data: Uint8Array): Promise<void> {
    this.ensureActive()

    // Get the existing operation for this path (if any)
    const existing = this.state.pending.get(path)
    const existingBytes = existing?.type === 'write' ? existing.data.length : 0

    // Calculate net new bytes (new data minus any existing data for this path)
    const netNewBytes = data.length - existingBytes

    // Check limits before adding the operation
    // Only check operation count if this is a new path
    if (!existing) {
      this.checkLimits(netNewBytes)
    } else if (netNewBytes > 0) {
      // Existing path, just check bytes limit
      const projectedBytes = this.state.currentBytes + netNewBytes
      if (projectedBytes > this.state.limits.maxBytes) {
        throw new TransactionTooLargeError(
          this.id,
          'bytes',
          this.state.limits.maxBytes,
          projectedBytes
        )
      }
    }

    // Update bytes tracking
    this.state.currentBytes += netNewBytes

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

    // Get the existing operation for this path (if any)
    const existing = this.state.pending.get(path)

    // Check operation count limit only if this is a new path
    if (!existing) {
      if (this.state.pending.size >= this.state.limits.maxOperations) {
        throw new TransactionTooLargeError(
          this.id,
          'operations',
          this.state.limits.maxOperations,
          this.state.pending.size + 1
        )
      }
    }

    // If replacing a write with a delete, reclaim the bytes
    if (existing?.type === 'write') {
      this.state.currentBytes -= existing.data.length
    }

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
    const writes: Array<{ path: string; data: Uint8Array; options?: WriteOptions | undefined }> = []

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

  /** Transaction limits configuration */
  private readonly limits: TransactionLimits

  constructor(
    private readonly backend: StorageBackend,
    options?: TransactionalBackendOptions
  ) {
    this.type = `transactional:${backend.type}`
    this.limits = {
      maxOperations: options?.maxTransactionOperations ?? DEFAULT_MAX_TRANSACTION_OPERATIONS,
      maxBytes: options?.maxTransactionBytes ?? DEFAULT_MAX_TRANSACTION_BYTES,
    }
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
      currentBytes: 0,
      limits: this.limits,
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

/**
 * Error thrown when a transaction exceeds configured size limits
 */
export class TransactionTooLargeError extends TransactionError {
  constructor(
    transactionId: string,
    public readonly limitType: 'operations' | 'bytes',
    public readonly limit: number,
    public readonly actual: number
  ) {
    const limitDescription =
      limitType === 'operations'
        ? `operation count limit of ${limit.toLocaleString()}`
        : `size limit of ${formatBytes(limit)}`
    const actualDescription =
      limitType === 'operations' ? actual.toLocaleString() : formatBytes(actual)

    super(transactionId, `Transaction exceeds ${limitDescription} (attempted: ${actualDescription})`)
    this.name = 'TransactionTooLargeError'
  }
}

/**
 * Format bytes as human-readable string
 */
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} bytes`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`
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
 *
 * @example With custom limits
 * ```typescript
 * const txBackend = withTransactions(backend, {
 *   maxTransactionOperations: 5000,
 *   maxTransactionBytes: 50 * 1024 * 1024 // 50MB
 * })
 * ```
 */
export function withTransactions(
  backend: StorageBackend,
  options?: TransactionalBackendOptions
): TransactionalBackend {
  // Don't double-wrap
  if (backend instanceof TransactionalBackend) {
    return backend
  }
  return new TransactionalBackend(backend, options)
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
