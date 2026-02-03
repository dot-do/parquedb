/**
 * Event WAL (Write-Ahead Log) Manager
 *
 * Handles event batching and WAL operations for entity events.
 * Events are buffered in memory and flushed as batches to reduce SQLite row costs.
 */

import type { Event } from '../../types'
import type { EventBuffer } from './types'
import Sqids from 'sqids'
import {
  EVENT_BATCH_COUNT_THRESHOLD,
  EVENT_BATCH_SIZE_THRESHOLD,
} from '../../constants'

// Re-export for backwards compatibility
export { EVENT_BATCH_COUNT_THRESHOLD, EVENT_BATCH_SIZE_THRESHOLD }

// Initialize Sqids for short ID generation
const sqids = new Sqids()

/**
 * Event WAL Manager
 *
 * Manages event buffering and WAL operations for entity events.
 */
export class EventWalManager {
  /** Legacy event buffer for WAL batching */
  private eventBuffer: Event[] = []

  /** Approximate size of buffered events in bytes */
  private eventBufferSize = 0

  /** Event buffers per namespace for WAL batching with sequence tracking */
  private nsEventBuffers: Map<string, EventBuffer> = new Map()

  constructor(
    private sql: SqlStorage,
    private counters: Map<string, number>
  ) {}

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

  // ===========================================================================
  // Legacy Event Batching
  // ===========================================================================

  /**
   * Append an event to the legacy buffer
   */
  async appendEvent(event: Event): Promise<void> {
    this.eventBuffer.push(event)

    // Estimate size
    const eventJson = JSON.stringify(event)
    this.eventBufferSize += eventJson.length

    // Check if we should flush
    if (
      this.eventBuffer.length >= EVENT_BATCH_COUNT_THRESHOLD ||
      this.eventBufferSize >= EVENT_BATCH_SIZE_THRESHOLD
    ) {
      await this.flushEventBatch()
    }
  }

  /**
   * Flush legacy buffered events as a single batch row
   */
  async flushEventBatch(): Promise<void> {
    if (this.eventBuffer.length === 0) return

    const events = this.eventBuffer
    const minTs = Math.min(...events.map(e => e.ts))
    const maxTs = Math.max(...events.map(e => e.ts))

    // Serialize events to blob
    const json = JSON.stringify(events)
    const data = new TextEncoder().encode(json)

    this.sql.exec(
      `INSERT INTO event_batches (batch, min_ts, max_ts, event_count, flushed)
       VALUES (?, ?, ?, ?, 0)`,
      data,
      minTs,
      maxTs,
      events.length
    )

    // Clear buffer
    this.eventBuffer = []
    this.eventBufferSize = 0
  }

  /**
   * Get unflushed event count (from batches + buffer)
   */
  async getUnflushedEventCount(): Promise<number> {
    // Count events in unflushed batches
    const rows = [...this.sql.exec<{ total: number }>(
      'SELECT SUM(event_count) as total FROM event_batches WHERE flushed = 0'
    )]

    const batchCount = rows[0]?.total ?? 0

    // Add buffered events not yet written
    return batchCount + this.eventBuffer.length
  }

  /**
   * Get unflushed batch count
   */
  async getUnflushedBatchCount(): Promise<number> {
    const rows = [...this.sql.exec<{ count: number }>(
      'SELECT COUNT(*) as count FROM event_batches WHERE flushed = 0'
    )]

    return rows[0]?.count ?? 0
  }

  /**
   * Read all unflushed events (from batches + buffer)
   */
  async readUnflushedEvents(): Promise<Event[]> {
    const allEvents: Event[] = []

    interface EventBatchRow extends Record<string, SqlStorageValue> {
      id: number
      batch: ArrayBuffer
      min_ts: number
      max_ts: number
      event_count: number
    }

    const rows = [...this.sql.exec<EventBatchRow>(
      `SELECT id, batch, min_ts, max_ts, event_count
       FROM event_batches
       WHERE flushed = 0
       ORDER BY min_ts ASC`
    )]

    for (const row of rows) {
      const batchEvents = deserializeEventBatch(row.batch)
      allEvents.push(...batchEvents)
    }

    // Add buffer events
    allEvents.push(...this.eventBuffer)

    return allEvents
  }

  /**
   * Mark event batches as flushed
   */
  async markEventBatchesFlushed(batchIds: number[]): Promise<void> {
    if (batchIds.length === 0) return

    const placeholders = batchIds.map(() => '?').join(',')
    this.sql.exec(
      `UPDATE event_batches SET flushed = 1 WHERE id IN (${placeholders})`,
      ...batchIds
    )
  }

  /**
   * Get the legacy event buffer (for transaction snapshots)
   */
  getEventBuffer(): Event[] {
    return this.eventBuffer
  }

  /**
   * Get the legacy event buffer size
   */
  getEventBufferSize(): number {
    return this.eventBufferSize
  }

  /**
   * Set the legacy event buffer (for transaction rollback)
   */
  setEventBuffer(events: Event[], size: number): void {
    this.eventBuffer = events
    this.eventBufferSize = size
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
