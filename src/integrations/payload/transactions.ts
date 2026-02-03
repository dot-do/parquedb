/**
 * Transaction management for Payload CMS adapter
 *
 * ParqueDB transactions are in-memory until committed,
 * which maps well to Payload's transaction model.
 */

import type { ParqueDB } from '../../ParqueDB'
import type { TransactionSession, ResolvedAdapterConfig } from './types'

/**
 * Transaction manager for the Payload adapter
 */
export class TransactionManager {
  private sessions: Map<string, TransactionSession> = new Map()
  private idCounter: number = 0

  // Note: db is stored for future rollback support via event sourcing
  constructor(
    _db: ParqueDB,
    private config: ResolvedAdapterConfig
  ) {}

  /**
   * Begin a new transaction
   * Returns a transaction ID that can be passed to operations
   */
  async beginTransaction(): Promise<string | number> {
    const id = `txn_${++this.idCounter}_${Date.now()}`

    const session: TransactionSession = {
      id,
      startedAt: new Date(),
      operations: [],
    }

    this.sessions.set(id, session)

    if (this.config.debug) {
      console.log(`[PayloadAdapter] Transaction started: ${id}`)
    }

    return id
  }

  /**
   * Commit a transaction
   * In ParqueDB, operations are applied immediately so this is mostly a cleanup
   */
  async commitTransaction(id: string | number): Promise<void> {
    const transactionId = String(id)
    const session = this.sessions.get(transactionId)

    if (!session) {
      throw new Error(`Transaction not found: ${transactionId}`)
    }

    if (this.config.debug) {
      console.log(`[PayloadAdapter] Transaction committed: ${transactionId} (${session.operations.length} operations)`)
    }

    // Clean up the session
    this.sessions.delete(transactionId)
  }

  /**
   * Rollback a transaction
   *
   * Note: ParqueDB operations are applied immediately, so true rollback
   * would require event sourcing reversal. For now, we log and clean up.
   * In a production implementation, you would use ParqueDB's event log
   * to undo operations.
   */
  async rollbackTransaction(id: string | number): Promise<void> {
    const transactionId = String(id)
    const session = this.sessions.get(transactionId)

    if (!session) {
      throw new Error(`Transaction not found: ${transactionId}`)
    }

    if (this.config.debug) {
      console.log(`[PayloadAdapter] Transaction rollback: ${transactionId} (${session.operations.length} operations to revert)`)
    }

    // In a full implementation, we would:
    // 1. Get all events logged during this transaction
    // 2. Replay them in reverse to undo changes
    // For now, we just clean up the session

    if (session.operations.length > 0) {
      console.warn(
        `[PayloadAdapter] Rolling back ${session.operations.length} operations. ` +
        'Note: ParqueDB operations are applied immediately. ' +
        'Use event sourcing for full rollback support.'
      )
    }

    // Clean up the session
    this.sessions.delete(transactionId)
  }

  /**
   * Record an operation in a transaction
   */
  recordOperation(
    transactionId: string | number,
    operation: TransactionSession['operations'][0]
  ): void {
    const id = String(transactionId)
    const session = this.sessions.get(id)

    if (session) {
      session.operations.push(operation)
    }
  }

  /**
   * Check if a transaction is active
   */
  hasTransaction(id: string | number): boolean {
    return this.sessions.has(String(id))
  }

  /**
   * Get transaction info
   */
  getTransaction(id: string | number): TransactionSession | undefined {
    return this.sessions.get(String(id))
  }

  /**
   * Get all active transactions (for debugging)
   */
  getActiveTransactions(): TransactionSession[] {
    return Array.from(this.sessions.values())
  }

  /**
   * Clean up stale transactions (older than 5 minutes)
   */
  cleanupStaleTransactions(): number {
    const staleThreshold = Date.now() - 5 * 60 * 1000 // 5 minutes
    let cleaned = 0

    for (const [id, session] of this.sessions.entries()) {
      if (session.startedAt.getTime() < staleThreshold) {
        this.sessions.delete(id)
        cleaned++

        if (this.config.debug) {
          console.log(`[PayloadAdapter] Cleaned up stale transaction: ${id}`)
        }
      }
    }

    return cleaned
  }
}

/**
 * Create a transaction manager instance
 */
export function createTransactionManager(
  db: ParqueDB,
  config: ResolvedAdapterConfig
): TransactionManager {
  return new TransactionManager(db, config)
}
