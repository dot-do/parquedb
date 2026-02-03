/**
 * Relationship WAL (Write-Ahead Log) Manager
 *
 * Handles event batching and WAL operations for relationship events.
 * Similar to EventWalManager but specialized for relationship operations.
 */

import type { Event } from '../../types'
import type { EventBuffer } from './types'
import { deserializeEventBatch, EVENT_BATCH_COUNT_THRESHOLD, EVENT_BATCH_SIZE_THRESHOLD } from './event-wal'

/**
 * Relationship WAL Manager
 *
 * Manages event buffering and WAL operations for relationship events.
 */
export class RelationshipWalManager {
  /** Relationship event buffers per namespace */
  private relEventBuffers: Map<string, EventBuffer> = new Map()

  constructor(
    private sql: SqlStorage,
    private counters: Map<string, number>
  ) {}

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
  }
}
