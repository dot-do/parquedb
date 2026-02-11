/**
 * Parquet Flush Manager
 *
 * Handles flushing events from WAL to Parquet files in R2.
 */

import type { Event } from '../../types'
import type { FlushConfig } from './types'
import { generateULID } from './ulid'
import { deserializeEventBatch } from './event-wal'
import {
  DEFAULT_FLUSH_MIN_EVENTS,
  DEFAULT_FLUSH_MAX_INTERVAL_MS,
  DEFAULT_MAX_EVENTS,
  DEFAULT_FLUSH_ROW_GROUP_SIZE,
} from '../../constants'

/**
 * Default flush configuration
 */
export const DEFAULT_FLUSH_CONFIG: FlushConfig = {
  minEvents: DEFAULT_FLUSH_MIN_EVENTS,
  maxInterval: DEFAULT_FLUSH_MAX_INTERVAL_MS,
  maxEvents: DEFAULT_MAX_EVENTS,
  rowGroupSize: DEFAULT_FLUSH_ROW_GROUP_SIZE,
}

/**
 * Parquet Flush Manager
 *
 * Coordinates flushing of event batches to Parquet files in R2.
 */
export class FlushManager {
  /** Flush configuration */
  private flushConfig: FlushConfig

  /** Whether a flush alarm is set */
  private flushAlarmSet = false

  constructor(
    private sql: SqlStorage,
    private bucket: R2Bucket,
    private ctx: DurableObjectState,
    flushConfig?: Partial<FlushConfig>
  ) {
    this.flushConfig = { ...DEFAULT_FLUSH_CONFIG, ...flushConfig }
  }

  /**
   * Flush events to Parquet and store in R2
   */
  async flushToParquet(): Promise<void> {
    // Get unflushed batches
    interface FlushEventBatchRow extends Record<string, SqlStorageValue> {
      id: number
      batch: ArrayBuffer
      min_ts: number
      max_ts: number
      event_count: number
    }

    const batches = [...this.sql.exec<FlushEventBatchRow>(
      `SELECT id, batch, min_ts, max_ts, event_count
       FROM event_batches
       WHERE flushed = 0
       ORDER BY min_ts ASC`
    )]

    // Collect all events and count
    let totalCount = 0
    const allEvents: Event[] = []
    const batchIds: number[] = []

    for (const batch of batches) {
      const events = deserializeEventBatch(batch.batch)
      allEvents.push(...events)
      totalCount += events.length
      batchIds.push(batch.id)

      if (totalCount >= this.flushConfig.maxEvents) break
    }

    if (totalCount === 0) {
      return
    }

    const firstEvent = allEvents[0]!
    const lastEvent = allEvents[allEvents.length - 1]!
    const checkpointId = generateULID()

    // Generate parquet path
    const date = new Date(firstEvent.ts)
    const datePath = `${date.getUTCFullYear()}/${String(date.getUTCMonth() + 1).padStart(2, '0')}/${String(date.getUTCDate()).padStart(2, '0')}`
    const parquetPath = `events/archive/${datePath}/${checkpointId}.parquet`

    // Convert events to columnar format for Parquet
    const columnData = [
      { name: 'id', data: allEvents.map(e => e.id) },
      { name: 'ts', data: allEvents.map(e => e.ts) },
      { name: 'op', data: allEvents.map(e => e.op) },
      { name: 'target', data: allEvents.map(e => e.target) },
      { name: 'before', data: allEvents.map(e => e.before ? JSON.stringify(e.before) : null) },
      { name: 'after', data: allEvents.map(e => e.after ? JSON.stringify(e.after) : null) },
      { name: 'actor', data: allEvents.map(e => e.actor ?? null) },
      { name: 'metadata', data: allEvents.map(e => e.metadata ? JSON.stringify(e.metadata) : null) },
    ]

    // Write Parquet file to R2 using hyparquet-writer
    try {
      const { parquetWriteBuffer } = await import('hyparquet-writer')
      const parquetBuffer = parquetWriteBuffer({ columnData })
      await this.bucket.put(parquetPath, parquetBuffer)
    } catch (error: unknown) {
      // If Parquet writing fails, log and re-throw to prevent marking batches as flushed
      const message = error instanceof Error ? error.message : 'Unknown error'
      throw new Error(`Failed to write Parquet file to R2: ${message}`)
    }

    // Mark batches as flushed
    if (batchIds.length > 0) {
      const placeholders = batchIds.map(() => '?').join(',')
      this.sql.exec(
        `UPDATE event_batches SET flushed = 1 WHERE id IN (${placeholders})`,
        ...batchIds
      )
    }

    // Record checkpoint
    this.sql.exec(
      `INSERT INTO checkpoints (id, created_at, event_count, first_event_id, last_event_id, parquet_path)
       VALUES (?, ?, ?, ?, ?, ?)`,
      checkpointId,
      new Date().toISOString(),
      allEvents.length,
      firstEvent.id,
      lastEvent.id,
      parquetPath
    )
  }

  /**
   * Schedule a flush if conditions are met
   */
  async maybeScheduleFlush(unflushedEventCount: number): Promise<void> {
    if (this.flushAlarmSet) return

    if (unflushedEventCount >= this.flushConfig.maxEvents) {
      // Flush immediately if we hit max events
      await this.flushToParquet()
    } else if (unflushedEventCount > 0) {
      // Always schedule flush for ANY unflushed events — data MUST reach R2
      await this.ctx.storage.setAlarm(Date.now() + this.flushConfig.maxInterval)
      this.flushAlarmSet = true
    }
  }

  /**
   * Reset the alarm flag (called from alarm handler)
   */
  resetAlarmFlag(): void {
    this.flushAlarmSet = false
  }

  /**
   * Check if alarm is set
   */
  isAlarmSet(): boolean {
    return this.flushAlarmSet
  }

  /**
   * Get flush configuration
   */
  getFlushConfig(): FlushConfig {
    return { ...this.flushConfig }
  }

  /**
   * Update flush configuration
   */
  setFlushConfig(config: Partial<FlushConfig>): void {
    this.flushConfig = { ...this.flushConfig, ...config }
  }
}
