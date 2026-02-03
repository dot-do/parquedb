/**
 * Relationship WAL (Write-Ahead Log) Manager
 *
 * Handles event batching and WAL operations for relationship events.
 * Similar to EventWalManager but specialized for relationship operations.
 *
 * Includes backpressure handling to prevent unbounded memory growth under
 * sustained write load.
 */

import type { Event } from '../../types'
import type { EventBuffer, BackpressureConfig, BackpressureState } from './types'
import { DEFAULT_BACKPRESSURE_CONFIG, BackpressureTimeoutError } from './types'
import { deserializeEventBatch, EVENT_BATCH_COUNT_THRESHOLD, EVENT_BATCH_SIZE_THRESHOLD } from './event-wal'

/**
 * Relationship WAL Manager
 *
 * Manages event buffering and WAL operations for relationship events.
 * Includes backpressure handling to prevent memory exhaustion.
 */
export class RelationshipWalManager {
  /** Relationship event buffers per namespace */
  private relEventBuffers: Map<string, EventBuffer> = new Map()

  // Backpressure state
  private backpressureConfig: BackpressureConfig
  private backpressureActive: boolean = false
  private backpressureWaiters: Array<{ resolve: () => void; reject: (err: Error) => void }> = []
  private pendingFlushCount: number = 0

  // Statistics
  private backpressureEvents: number = 0
  private totalWaitTimeMs: number = 0
  private lastBackpressureAt: number | null = null

  constructor(
    private sql: SqlStorage,
    private counters: Map<string, number>,
    backpressureConfig?: Partial<BackpressureConfig>
  ) {
    this.backpressureConfig = {
      ...DEFAULT_BACKPRESSURE_CONFIG,
      ...backpressureConfig,
    }
  }

  // ===========================================================================
  // Backpressure Configuration
  // ===========================================================================

  /**
   * Get current backpressure configuration
   */
  getBackpressureConfig(): BackpressureConfig {
    return { ...this.backpressureConfig }
  }

  /**
   * Update backpressure configuration
   */
  setBackpressureConfig(config: Partial<BackpressureConfig>): void {
    this.backpressureConfig = {
      ...this.backpressureConfig,
      ...config,
    }
  }

  /**
   * Get current backpressure state
   */
  getBackpressureState(): BackpressureState {
    return {
      active: this.backpressureActive,
      currentBufferSizeBytes: this.getTotalBufferSizeBytes(),
      currentEventCount: this.getTotalEventCount(),
      pendingFlushCount: this.pendingFlushCount,
      backpressureEvents: this.backpressureEvents,
      totalWaitTimeMs: this.totalWaitTimeMs,
      lastBackpressureAt: this.lastBackpressureAt,
    }
  }

  /**
   * Reset backpressure statistics
   */
  resetBackpressureStats(): void {
    this.backpressureEvents = 0
    this.totalWaitTimeMs = 0
    this.lastBackpressureAt = null
  }

  /**
   * Force release backpressure (emergency release)
   */
  forceReleaseBackpressure(): void {
    this.backpressureActive = false
    this.releaseWaiters()
  }

  // ===========================================================================
  // Backpressure Internal Methods
  // ===========================================================================

  /**
   * Get total buffer size across all namespace buffers
   */
  private getTotalBufferSizeBytes(): number {
    let total = 0
    for (const buffer of this.relEventBuffers.values()) {
      total += buffer.sizeBytes
    }
    return total
  }

  /**
   * Get total event count across all namespace buffers
   */
  private getTotalEventCount(): number {
    let total = 0
    for (const buffer of this.relEventBuffers.values()) {
      total += buffer.events.length
    }
    return total
  }

  /**
   * Check if backpressure should be activated
   */
  private shouldActivateBackpressure(): boolean {
    const totalBytes = this.getTotalBufferSizeBytes()
    const totalEvents = this.getTotalEventCount()

    return (
      totalBytes >= this.backpressureConfig.maxBufferSizeBytes ||
      totalEvents >= this.backpressureConfig.maxBufferEventCount ||
      this.pendingFlushCount >= this.backpressureConfig.maxPendingFlushes
    )
  }

  /**
   * Check if backpressure should be released
   */
  private shouldReleaseBackpressure(): boolean {
    const totalBytes = this.getTotalBufferSizeBytes()
    const totalEvents = this.getTotalEventCount()
    const threshold = this.backpressureConfig.releaseThreshold

    return (
      totalBytes < this.backpressureConfig.maxBufferSizeBytes * threshold &&
      totalEvents < this.backpressureConfig.maxBufferEventCount * threshold &&
      this.pendingFlushCount < this.backpressureConfig.maxPendingFlushes * threshold
    )
  }

  /**
   * Check and update backpressure state
   */
  private checkBackpressure(): void {
    if (!this.backpressureActive && this.shouldActivateBackpressure()) {
      this.backpressureActive = true
      this.backpressureEvents++
      this.lastBackpressureAt = Date.now()
    } else if (this.backpressureActive && this.shouldReleaseBackpressure()) {
      this.backpressureActive = false
      this.releaseWaiters()
    }
  }

  /**
   * Release all waiting operations
   */
  private releaseWaiters(): void {
    const waiters = this.backpressureWaiters
    this.backpressureWaiters = []
    for (const waiter of waiters) {
      waiter.resolve()
    }
  }

  /**
   * Wait for backpressure to be released
   */
  private async waitForBackpressure(): Promise<void> {
    if (!this.backpressureActive) return

    const startTime = Date.now()
    const timeoutMs = this.backpressureConfig.timeoutMs

    return new Promise<void>((resolve, reject) => {
      const waiter = { resolve, reject }
      this.backpressureWaiters.push(waiter)

      // Set timeout if not infinite
      if (timeoutMs !== Infinity) {
        setTimeout(() => {
          const index = this.backpressureWaiters.indexOf(waiter)
          if (index >= 0) {
            this.backpressureWaiters.splice(index, 1)
            this.totalWaitTimeMs += Date.now() - startTime
            reject(new BackpressureTimeoutError(timeoutMs, this.getBackpressureState()))
          }
        }, timeoutMs)
      }
    }).finally(() => {
      this.totalWaitTimeMs += Date.now() - startTime
    })
  }

  // ===========================================================================
  // Relationship Event Buffering
  // ===========================================================================

  /**
   * Append a relationship event with namespace-based batching
   *
   * @param ns - Namespace for the event
   * @param event - Event without ID
   * @returns Event ID
   */
  async appendRelEvent(ns: string, event: Omit<Event, 'id'>): Promise<string> {
    // Wait for backpressure if active
    await this.waitForBackpressure()

    // Get or create buffer for this namespace
    let buffer = this.relEventBuffers.get(ns)
    if (!buffer) {
      const relCounterKey = `rel:${ns}`
      const seq = this.counters.get(relCounterKey) || 1
      buffer = { events: [], firstSeq: seq, lastSeq: seq, sizeBytes: 0 }
      this.relEventBuffers.set(ns, buffer)
    }

    // Generate event ID using sequence
    const eventId = `rel_${buffer.lastSeq}`
    const fullEvent: Event = { ...event, id: eventId }

    buffer.events.push(fullEvent)
    buffer.lastSeq++
    this.counters.set(`rel:${ns}`, buffer.lastSeq)

    // Estimate size
    const eventJson = JSON.stringify(fullEvent)
    buffer.sizeBytes += eventJson.length

    // Check if backpressure should be activated after adding
    this.checkBackpressure()

    // Check if we should flush
    if (
      buffer.events.length >= EVENT_BATCH_COUNT_THRESHOLD ||
      buffer.sizeBytes >= EVENT_BATCH_SIZE_THRESHOLD
    ) {
      await this.flushRelEventBatch(ns)
    }

    return eventId
  }

  /**
   * Flush buffered relationship events for a namespace to rels_wal
   */
  async flushRelEventBatch(ns: string): Promise<void> {
    const buffer = this.relEventBuffers.get(ns)
    if (!buffer || buffer.events.length === 0) return

    this.pendingFlushCount++

    try {
      // Serialize events to blob
      const json = JSON.stringify(buffer.events)
      const data = new TextEncoder().encode(json)
      const now = new Date().toISOString()

      this.sql.exec(
        `INSERT INTO rels_wal (ns, first_seq, last_seq, events, created_at)
         VALUES (?, ?, ?, ?, ?)`,
        ns,
        buffer.firstSeq,
        buffer.lastSeq - 1, // last_seq is inclusive
        data,
        now
      )

      // Reset buffer for next batch
      this.relEventBuffers.set(ns, {
        events: [],
        firstSeq: buffer.lastSeq,
        lastSeq: buffer.lastSeq,
        sizeBytes: 0,
      })
    } finally {
      this.pendingFlushCount--
      // Check if backpressure can be released
      this.checkBackpressure()
    }
  }

  /**
   * Flush all namespace relationship event buffers
   */
  async flushAllRelEventBatches(): Promise<void> {
    for (const ns of this.relEventBuffers.keys()) {
      await this.flushRelEventBatch(ns)
    }
  }

  /**
   * Get unflushed WAL relationship event count for a namespace
   */
  async getUnflushedRelEventCount(ns: string): Promise<number> {
    interface WalRow extends Record<string, SqlStorageValue> {
      events: ArrayBuffer
    }

    const rows = [...this.sql.exec<WalRow>(
      `SELECT events FROM rels_wal WHERE ns = ?`,
      ns
    )]

    let count = 0
    for (const row of rows) {
      const events = deserializeEventBatch(row.events)
      count += events.length
    }

    // Add buffered events not yet written
    const buffer = this.relEventBuffers.get(ns)
    count += buffer?.events.length ?? 0

    return count
  }

  /**
   * Get total unflushed WAL relationship event count across all namespaces
   */
  async getTotalUnflushedRelEventCount(): Promise<number> {
    interface WalRow extends Record<string, SqlStorageValue> {
      events: ArrayBuffer
    }

    const rows = [...this.sql.exec<WalRow>(`SELECT events FROM rels_wal`)]

    let total = 0
    for (const row of rows) {
      const events = deserializeEventBatch(row.events)
      total += events.length
    }

    // Add all buffered events
    for (const buffer of this.relEventBuffers.values()) {
      total += buffer.events.length
    }

    return total
  }

  /**
   * Get unflushed WAL batch count for relationships
   */
  async getUnflushedRelBatchCount(ns?: string): Promise<number> {
    let query = 'SELECT COUNT(*) as count FROM rels_wal'
    const params: unknown[] = []

    if (ns) {
      query += ' WHERE ns = ?'
      params.push(ns)
    }

    const rows = [...this.sql.exec<{ count: number }>(query, ...params)]
    return rows[0]?.count ?? 0
  }

  /**
   * Read all unflushed relationship WAL events for a namespace
   */
  async readUnflushedRelEvents(ns: string): Promise<Event[]> {
    const allEvents: Event[] = []

    interface WalRow extends Record<string, SqlStorageValue> {
      id: number
      events: ArrayBuffer
      first_seq: number
      last_seq: number
    }

    const rows = [...this.sql.exec<WalRow>(
      `SELECT id, events, first_seq, last_seq FROM rels_wal WHERE ns = ? ORDER BY first_seq ASC`,
      ns
    )]

    for (const row of rows) {
      const batchEvents = deserializeEventBatch(row.events)
      allEvents.push(...batchEvents)
    }

    // Add buffer events
    const buffer = this.relEventBuffers.get(ns)
    if (buffer) {
      allEvents.push(...buffer.events)
    }

    return allEvents
  }

  /**
   * Delete relationship WAL batches for a namespace (after archiving to R2)
   */
  async deleteRelWalBatches(ns: string, upToSeq: number): Promise<void> {
    this.sql.exec(
      `DELETE FROM rels_wal WHERE ns = ? AND last_seq <= ?`,
      ns,
      upToSeq
    )
  }

  /**
   * Get relationship sequence counter for a namespace
   */
  getRelSequenceCounter(ns: string): number {
    const relCounterKey = `rel:${ns}`
    return this.counters.get(relCounterKey) || 1
  }

  /**
   * Get relationship buffer state for a namespace (for testing)
   */
  getRelBufferState(ns: string): {
    eventCount: number
    firstSeq: number
    lastSeq: number
    sizeBytes: number
  } | null {
    const buffer = this.relEventBuffers.get(ns)
    if (!buffer) return null
    return {
      eventCount: buffer.events.length,
      firstSeq: buffer.firstSeq,
      lastSeq: buffer.lastSeq,
      sizeBytes: buffer.sizeBytes,
    }
  }

  /**
   * Get all relationship event buffers (for transaction snapshots)
   */
  getRelEventBuffers(): Map<string, EventBuffer> {
    return this.relEventBuffers
  }

  /**
   * Set relationship event buffers (for transaction rollback)
   */
  setRelEventBuffers(buffers: Map<string, EventBuffer>): void {
    this.relEventBuffers = buffers
    // Check backpressure after restoring buffers
    this.checkBackpressure()
  }
}
