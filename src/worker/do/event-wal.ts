/**
 * Event WAL (Write-Ahead Log) Manager
 *
 * Handles event batching and WAL operations for entity events.
 * Events are buffered in memory and flushed as batches to reduce SQLite row costs.
 *
 * Includes backpressure handling to prevent unbounded memory growth under
 * sustained write load.
 */

import type { Event } from '../../types'
import type { EventBuffer, BackpressureConfig, BackpressureState } from './types'
import { DEFAULT_BACKPRESSURE_CONFIG, BackpressureTimeoutError } from './types'
import Sqids from 'sqids'

/** WAL batch thresholds */
export const EVENT_BATCH_COUNT_THRESHOLD = 100
export const EVENT_BATCH_SIZE_THRESHOLD = 64 * 1024 // 64KB

// Initialize Sqids for short ID generation
const sqids = new Sqids()

/**
 * Event WAL Manager
 *
 * Manages event buffering and WAL operations for entity events.
 * Includes backpressure handling to prevent memory exhaustion.
 */
export class EventWalManager {
  /** Event buffers per namespace for WAL batching with sequence tracking */
  private nsEventBuffers: Map<string, EventBuffer> = new Map()

  /**
   * Legacy event buffer for events with pre-assigned IDs.
   * Used by ParqueDBDO.getEntityFromEvents and transaction snapshots.
   * Events stored here use the "ns:id" target format (e.g., "users:u1").
   */
  private eventBuffer: Event[] = []

  /** Size tracking for the legacy event buffer */
  private eventBufferSize: number = 0

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
    let total = this.eventBufferSize
    for (const buffer of this.nsEventBuffers.values()) {
      total += buffer.sizeBytes
    }
    return total
  }

  /**
   * Get total event count across all namespace buffers
   */
  private getTotalEventCount(): number {
    let total = this.eventBuffer.length
    for (const buffer of this.nsEventBuffers.values()) {
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
  // Legacy Event Buffer (for events with pre-assigned IDs)
  // ===========================================================================

  /**
   * Append an event with a pre-assigned ID to the legacy buffer.
   *
   * This is used by ParqueDBDO operations that need to store events with
   * specific IDs (e.g., ULIDs). The event target must use the correct
   * "ns:id" format (e.g., "users:u1") created by entityTarget().
   *
   * @param event - Complete event including ID and target in "ns:id" format
   */
  async appendEvent(event: Event): Promise<void> {
    // Wait for backpressure if active
    await this.waitForBackpressure()

    // Validate that the target uses the correct format (ns:id, not ns/id)
    // Entity targets should contain a colon, not a slash
    if (event.target.includes('/') && !event.target.includes(':')) {
      throw new Error(
        `Invalid event target format: "${event.target}". ` +
          'Expected "ns:id" format (e.g., "users:u1"). ' +
          'Use entityTarget(ns, id) to create the correct format.'
      )
    }

    this.eventBuffer.push(event)
    this.eventBufferSize += JSON.stringify(event).length

    // Check if backpressure should be activated after adding
    this.checkBackpressure()
  }

  /**
   * Get the legacy event buffer.
   *
   * Returns a shallow copy to prevent external mutation.
   * Events in this buffer have targets in "ns:id" format.
   */
  getEventBuffer(): Event[] {
    return [...this.eventBuffer]
  }

  /**
   * Get the size of the legacy event buffer in bytes.
   */
  getEventBufferSize(): number {
    return this.eventBufferSize
  }

  /**
   * Set the legacy event buffer (for transaction rollback).
   *
   * This restores the event buffer to a previous state captured
   * in a transaction snapshot. All events must have targets in
   * the correct "ns:id" format.
   *
   * @param events - Events to restore (should use "ns:id" target format)
   * @param size - Size in bytes to restore
   */
  setEventBuffer(events: Event[], size: number): void {
    this.eventBuffer = events
    this.eventBufferSize = size
  }

  /**
   * Clear the legacy event buffer.
   */
  clearEventBuffer(): void {
    this.eventBuffer = []
    this.eventBufferSize = 0
  }

  // ===========================================================================
  // Namespace-based Event Buffering
  // ===========================================================================

  /**
   * Append an event with namespace-based sequence tracking
   *
   * @param ns - Namespace for the event
   * @param event - Event without ID (ID will be generated)
   * @returns Event ID generated with Sqids
   */
  async appendEventWithSeq(ns: string, event: Omit<Event, 'id'>): Promise<string> {
    // Wait for backpressure if active
    await this.waitForBackpressure()

    // Get or create buffer for this namespace
    let buffer = this.nsEventBuffers.get(ns)
    if (!buffer) {
      // Initialize with current counter
      const seq = this.counters.get(ns) || 1
      buffer = { events: [], firstSeq: seq, lastSeq: seq, sizeBytes: 0 }
      this.nsEventBuffers.set(ns, buffer)
    }

    // Generate event ID using Sqids with current sequence
    const eventId = sqids.encode([buffer.lastSeq])
    const fullEvent: Event = { ...event, id: eventId }

    buffer.events.push(fullEvent)
    buffer.lastSeq++
    this.counters.set(ns, buffer.lastSeq)

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
      await this.flushNsEventBatch(ns)
    }

    return eventId
  }

  /**
   * Flush buffered events for a specific namespace to events_wal
   */
  async flushNsEventBatch(ns: string): Promise<void> {
    const buffer = this.nsEventBuffers.get(ns)
    if (!buffer || buffer.events.length === 0) return

    this.pendingFlushCount++

    try {
      // Serialize events to blob
      const json = JSON.stringify(buffer.events)
      const data = new TextEncoder().encode(json)
      const now = new Date().toISOString()

      this.sql.exec(
        `INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at)
         VALUES (?, ?, ?, ?, ?)`,
        ns,
        buffer.firstSeq,
        buffer.lastSeq - 1, // last_seq is inclusive
        data,
        now
      )

      // Reset buffer for next batch
      this.nsEventBuffers.set(ns, {
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
   * Flush all namespace event buffers
   */
  async flushAllNsEventBatches(): Promise<void> {
    for (const ns of this.nsEventBuffers.keys()) {
      await this.flushNsEventBatch(ns)
    }
  }

  /**
   * Get unflushed WAL event count for a specific namespace
   */
  async getUnflushedWalEventCount(ns: string): Promise<number> {
    // Count events in WAL batches
    const rows = [...this.sql.exec<{ total: number }>(
      `SELECT SUM(last_seq - first_seq + 1) as total FROM events_wal WHERE ns = ?`,
      ns
    )]

    const walCount = rows[0]?.total ?? 0

    // Add buffered events not yet written
    const buffer = this.nsEventBuffers.get(ns)
    const bufferCount = buffer?.events.length ?? 0

    return walCount + bufferCount
  }

  /**
   * Get total unflushed WAL event count across all namespaces
   */
  async getTotalUnflushedWalEventCount(): Promise<number> {
    // Count events in WAL batches
    const rows = [...this.sql.exec<{ total: number }>(
      `SELECT SUM(last_seq - first_seq + 1) as total FROM events_wal`
    )]

    let total = rows[0]?.total ?? 0

    // Add all buffered events
    for (const buffer of this.nsEventBuffers.values()) {
      total += buffer.events.length
    }

    return total
  }

  /**
   * Get unflushed WAL batch count
   */
  async getUnflushedWalBatchCount(): Promise<number> {
    const rows = [...this.sql.exec<{ count: number }>(
      'SELECT COUNT(*) as count FROM events_wal'
    )]

    return rows[0]?.count ?? 0
  }

  /**
   * Read all unflushed WAL events for a namespace
   */
  async readUnflushedWalEvents(ns: string): Promise<Event[]> {
    const allEvents: Event[] = []

    interface WalRow extends Record<string, SqlStorageValue> {
      id: number
      events: ArrayBuffer
      first_seq: number
      last_seq: number
    }

    const rows = [...this.sql.exec<WalRow>(
      `SELECT id, events, first_seq, last_seq
       FROM events_wal
       WHERE ns = ?
       ORDER BY first_seq ASC`,
      ns
    )]

    for (const row of rows) {
      const batchEvents = deserializeEventBatch(row.events)
      allEvents.push(...batchEvents)
    }

    // Add buffer events
    const buffer = this.nsEventBuffers.get(ns)
    if (buffer) {
      allEvents.push(...buffer.events)
    }

    return allEvents
  }

  /**
   * Delete WAL batches for a namespace (after archiving to R2)
   */
  async deleteWalBatches(ns: string, upToSeq: number): Promise<void> {
    this.sql.exec(
      `DELETE FROM events_wal WHERE ns = ? AND last_seq <= ?`,
      ns,
      upToSeq
    )
  }

  /**
   * Get current sequence counter for a namespace
   */
  getSequenceCounter(ns: string): number {
    return this.counters.get(ns) || 1
  }

  /**
   * Get buffer state for a namespace (for testing)
   */
  getNsBufferState(ns: string): {
    eventCount: number
    firstSeq: number
    lastSeq: number
    sizeBytes: number
  } | null {
    const buffer = this.nsEventBuffers.get(ns)
    if (!buffer) return null
    return {
      eventCount: buffer.events.length,
      firstSeq: buffer.firstSeq,
      lastSeq: buffer.lastSeq,
      sizeBytes: buffer.sizeBytes,
    }
  }

  /**
   * Get all namespace event buffers (for transaction snapshots)
   */
  getNsEventBuffers(): Map<string, EventBuffer> {
    return this.nsEventBuffers
  }

  /**
   * Set namespace event buffers (for transaction rollback)
   */
  setNsEventBuffers(buffers: Map<string, EventBuffer>): void {
    this.nsEventBuffers = buffers
  }

  /**
   * Get unflushed event count (from events_wal + buffers)
   */
  async getUnflushedEventCount(): Promise<number> {
    return this.getTotalUnflushedWalEventCount()
  }

  /**
   * Read all unflushed events (from events_wal + buffers)
   */
  async readUnflushedEvents(): Promise<Event[]> {
    const allEvents: Event[] = []

    interface WalRow extends Record<string, SqlStorageValue> {
      events: ArrayBuffer
    }

    const rows = [...this.sql.exec<WalRow>(
      `SELECT events FROM events_wal ORDER BY id ASC`
    )]

    for (const row of rows) {
      const batchEvents = deserializeEventBatch(row.events)
      allEvents.push(...batchEvents)
    }

    // Add events from all namespace buffers
    for (const buffer of this.nsEventBuffers.values()) {
      allEvents.push(...buffer.events)
    }

    return allEvents
  }
}

/**
 * Deserialize a batch blob back to events
 */
export function deserializeEventBatch(batch: Uint8Array | ArrayBuffer): Event[] {
  if (!batch) return []

  let data: Uint8Array
  if (batch instanceof Uint8Array) {
    data = batch
  } else if (batch instanceof ArrayBuffer) {
    data = new Uint8Array(batch)
  } else if (ArrayBuffer.isView(batch)) {
    data = new Uint8Array((batch as ArrayBufferView).buffer)
  } else {
    // Assume it's already a buffer-like object
    data = new Uint8Array(batch as ArrayBuffer)
  }

  const json = new TextDecoder().decode(data)
  try {
    return JSON.parse(json) as Event[]
  } catch {
    // Invalid JSON in event batch - return empty array
    return []
  }
}
