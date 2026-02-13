/**
 * WAL Cost Optimizer
 *
 * Provides additional cost optimizations for the WAL:
 * 1. Compaction - Merge small WAL batches to reduce SQLite row count
 * 2. Pruning - Delete old flushed batches with configurable retention
 * 3. Adaptive Thresholds - Dynamically adjust batch sizes based on load
 * 4. Compression - Compress event blobs to reduce storage costs
 *
 * Cost Model:
 * - DO SQLite is 4.5x more expensive than R2 writes
 * - Each SQLite row has a fixed overhead cost
 * - Reducing row count is the primary cost optimization lever
 */

import { deflateSync, inflateSync } from 'fflate'
import type { Event } from '../../types'
import {
  WAL_COMPACTION_MIN_BATCHES,
  WAL_COMPACTION_TARGET_EVENTS,
  WAL_MAX_BLOB_SIZE,
  WAL_FLUSHED_RETENTION_MS,
  WAL_ADAPTIVE_WINDOW_MS,
  WAL_MIN_BATCH_THRESHOLD,
  WAL_MAX_BATCH_THRESHOLD,
  COMPRESSION_SAVINGS_THRESHOLD,
} from '../../constants'

// =============================================================================
// Configuration
// =============================================================================

export interface WalOptimizerConfig {
  /** Enable compression for event blobs (default: true) */
  enableCompression: boolean

  /** Minimum batch size before compaction is triggered (default: 10 batches) */
  compactionMinBatches: number

  /** Target batch size after compaction (default: 1000 events) */
  compactionTargetEvents: number

  /** Maximum blob size before splitting (default: 256KB) */
  maxBlobSize: number

  /** Retention period for flushed batches in ms (default: 7 days) */
  flushedRetentionMs: number

  /** Enable adaptive thresholds (default: true) */
  enableAdaptiveThresholds: boolean

  /** Window size for adaptive threshold calculation in ms (default: 1 minute) */
  adaptiveWindowMs: number

  /** Minimum batch threshold (default: 10) */
  minBatchThreshold: number

  /** Maximum batch threshold (default: 1000) */
  maxBatchThreshold: number
}

export const DEFAULT_WAL_OPTIMIZER_CONFIG: WalOptimizerConfig = {
  enableCompression: true,
  compactionMinBatches: WAL_COMPACTION_MIN_BATCHES,
  compactionTargetEvents: WAL_COMPACTION_TARGET_EVENTS,
  maxBlobSize: WAL_MAX_BLOB_SIZE,
  flushedRetentionMs: WAL_FLUSHED_RETENTION_MS,
  enableAdaptiveThresholds: true,
  adaptiveWindowMs: WAL_ADAPTIVE_WINDOW_MS,
  minBatchThreshold: WAL_MIN_BATCH_THRESHOLD,
  maxBatchThreshold: WAL_MAX_BATCH_THRESHOLD,
}

// =============================================================================
// Compression
// =============================================================================

/**
 * Compress events to a blob for storage
 * Uses fflate for fast DEFLATE compression
 */
export function compressEvents(events: Event[], enableCompression = true): Uint8Array {
  const json = JSON.stringify(events)
  const data = new TextEncoder().encode(json)

  if (!enableCompression) {
    return data
  }

  try {
    // Use fflate for fast compression
    const compressed = deflateSync(data, { level: 6 })

    // Only use compression if it actually saves space
    if (compressed.length < data.length * COMPRESSION_SAVINGS_THRESHOLD) {
      // Prefix with magic byte 0x01 to indicate compression
      const result = new Uint8Array(compressed.length + 1)
      result[0] = 0x01
      result.set(compressed, 1)
      return result
    }
  } catch {
    // Fall back to uncompressed on error
  }

  // Prefix with magic byte 0x00 to indicate no compression
  const result = new Uint8Array(data.length + 1)
  result[0] = 0x00
  result.set(data, 1)
  return result
}

/**
 * Decompress events from a blob
 */
export function decompressEvents(blob: Uint8Array | ArrayBuffer): Event[] {
  let data: Uint8Array
  if (blob instanceof ArrayBuffer) {
    data = new Uint8Array(blob)
  } else {
    data = blob
  }

  if (data.length === 0) {
    return []
  }

  // Check magic byte
  const isCompressed = data[0] === 0x01
  const payload = data.slice(1)

  let jsonData: Uint8Array
  if (isCompressed) {
    try {
      jsonData = inflateSync(payload)
    } catch {
      // Try to parse as legacy uncompressed format
      jsonData = data
    }
  } else if (data[0] === 0x00) {
    jsonData = payload
  } else {
    // Legacy format without magic byte
    jsonData = data
  }

  const json = new TextDecoder().decode(jsonData)
  try {
    return JSON.parse(json) as Event[]
  } catch {
    return []
  }
}

/**
 * Calculate compression ratio for diagnostics
 */
export function calculateCompressionRatio(events: Event[]): {
  originalSize: number
  compressedSize: number
  ratio: number
} {
  const json = JSON.stringify(events)
  const original = new TextEncoder().encode(json)
  const compressed = compressEvents(events, true)

  return {
    originalSize: original.length,
    compressedSize: compressed.length,
    ratio: compressed.length / original.length,
  }
}

// =============================================================================
// WAL Optimizer Class
// =============================================================================

export class WalOptimizer {
  private config: WalOptimizerConfig
  private sql: SqlStorage

  // Adaptive threshold tracking
  private eventTimestamps: number[] = []
  private currentBatchThreshold: number

  constructor(sql: SqlStorage, config: Partial<WalOptimizerConfig> = {}) {
    this.config = { ...DEFAULT_WAL_OPTIMIZER_CONFIG, ...config }
    this.sql = sql
    this.currentBatchThreshold = 100 // Default starting threshold
  }

  // ===========================================================================
  // Compaction
  // ===========================================================================

  /**
   * Compact small WAL batches into larger ones
   *
   * This reduces the number of SQLite rows by merging batches.
   * Cost reduction: N small batches -> 1 large batch = (N-1) rows saved
   *
   * @param ns - Namespace to compact (undefined for all)
   * @returns Number of rows saved by compaction
   */
  async compactWalBatches(ns?: string): Promise<{ rowsSaved: number; batchesMerged: number }> {
    interface WalBatchRow extends Record<string, SqlStorageValue> {
      id: number
      ns: string
      first_seq: number
      last_seq: number
      events: ArrayBuffer
    }

    // Get all unflushed batches
    let query = `SELECT id, ns, first_seq, last_seq, events
                 FROM events_wal
                 ORDER BY ns, first_seq ASC`
    const params: unknown[] = []

    if (ns) {
      query = `SELECT id, ns, first_seq, last_seq, events
               FROM events_wal
               WHERE ns = ?
               ORDER BY first_seq ASC`
      params.push(ns)
    }

    const rows = [...this.sql.exec<WalBatchRow>(query, ...params)]

    if (rows.length < this.config.compactionMinBatches) {
      return { rowsSaved: 0, batchesMerged: 0 }
    }

    // Group by namespace
    const byNamespace = new Map<string, WalBatchRow[]>()
    for (const row of rows) {
      const existing = byNamespace.get(row.ns) ?? []
      existing.push(row)
      byNamespace.set(row.ns, existing)
    }

    let totalRowsSaved = 0
    let totalBatchesMerged = 0

    // Compact each namespace
    for (const [nsKey, batches] of byNamespace) {
      if (batches.length < 2) continue

      const result = await this.compactNamespaceBatches(nsKey, batches)
      totalRowsSaved += result.rowsSaved
      totalBatchesMerged += result.batchesMerged
    }

    return { rowsSaved: totalRowsSaved, batchesMerged: totalBatchesMerged }
  }

  /**
   * Compact batches for a single namespace
   */
  private async compactNamespaceBatches(
    ns: string,
    batches: Array<{
      id: number
      ns: string
      first_seq: number
      last_seq: number
      events: ArrayBuffer
    }>
  ): Promise<{ rowsSaved: number; batchesMerged: number }> {
    // Collect all events
    const allEvents: Event[] = []
    const batchIds: number[] = []
    let minSeq = Infinity
    let maxSeq = -Infinity

    for (const batch of batches) {
      const events = decompressEvents(new Uint8Array(batch.events))
      allEvents.push(...events)
      batchIds.push(batch.id)
      minSeq = Math.min(minSeq, batch.first_seq)
      maxSeq = Math.max(maxSeq, batch.last_seq)
    }

    if (allEvents.length === 0) {
      return { rowsSaved: 0, batchesMerged: 0 }
    }

    // Split into target-sized batches
    const newBatches: Array<{ events: Event[]; firstSeq: number; lastSeq: number }> = []
    for (let i = 0; i < allEvents.length; i += this.config.compactionTargetEvents) {
      const chunk = allEvents.slice(i, i + this.config.compactionTargetEvents)
      const batchFirstSeq = minSeq + i
      const batchLastSeq = Math.min(minSeq + i + chunk.length - 1, maxSeq)
      newBatches.push({
        events: chunk,
        firstSeq: batchFirstSeq,
        lastSeq: batchLastSeq,
      })
    }

    // Only compact if we're reducing row count
    if (newBatches.length >= batches.length) {
      return { rowsSaved: 0, batchesMerged: 0 }
    }

    // Delete old batches (batched to avoid SQLite parameter limit)
    const BATCH_SIZE = 99 // Cloudflare DO SQLite max 100 params per statement
    for (let i = 0; i < batchIds.length; i += BATCH_SIZE) {
      const batch = batchIds.slice(i, i + BATCH_SIZE)
      const placeholders = batch.map(() => '?').join(',')
      this.sql.exec(`DELETE FROM events_wal WHERE id IN (${placeholders})`, ...batch)
    }

    // Insert compacted batches
    const now = new Date().toISOString()
    for (const batch of newBatches) {
      const blob = compressEvents(batch.events, this.config.enableCompression)
      this.sql.exec(
        `INSERT INTO events_wal (ns, first_seq, last_seq, events, created_at)
         VALUES (?, ?, ?, ?, ?)`,
        ns,
        batch.firstSeq,
        batch.lastSeq,
        blob,
        now
      )
    }

    const rowsSaved = batches.length - newBatches.length
    return { rowsSaved, batchesMerged: batches.length }
  }

  // ===========================================================================
  // Pruning
  // ===========================================================================

  /**
   * Prune old flushed batches
   *
   * Deletes event_batches that have been flushed to R2 and are older
   * than the retention period.
   *
   * @returns Number of batches deleted
   */
  async pruneFlushedBatches(): Promise<number> {
    const cutoffTime = Date.now() - this.config.flushedRetentionMs

    // Delete flushed batches older than retention period
    interface DeletedRow extends Record<string, SqlStorageValue> {
      count: number
    }

    // First count how many will be deleted
    const countRows = [...this.sql.exec<DeletedRow>(
      `SELECT COUNT(*) as count FROM event_batches
       WHERE flushed = 1 AND max_ts < ?`,
      cutoffTime
    )]

    const deleteCount = countRows[0]?.count ?? 0

    if (deleteCount > 0) {
      this.sql.exec(
        `DELETE FROM event_batches WHERE flushed = 1 AND max_ts < ?`,
        cutoffTime
      )
    }

    return deleteCount
  }

  /**
   * Prune old WAL batches for a specific namespace
   *
   * @param ns - Namespace to prune
   * @param olderThanTs - Delete batches with created_at before this timestamp
   * @returns Number of batches deleted
   */
  async pruneWalBatches(ns: string, olderThanTs: number): Promise<number> {
    const cutoffDate = new Date(olderThanTs).toISOString()

    interface DeletedRow extends Record<string, SqlStorageValue> {
      count: number
    }

    const countRows = [...this.sql.exec<DeletedRow>(
      `SELECT COUNT(*) as count FROM events_wal
       WHERE ns = ? AND created_at < ?`,
      ns,
      cutoffDate
    )]

    const deleteCount = countRows[0]?.count ?? 0

    if (deleteCount > 0) {
      this.sql.exec(
        `DELETE FROM events_wal WHERE ns = ? AND created_at < ?`,
        ns,
        cutoffDate
      )
    }

    return deleteCount
  }

  // ===========================================================================
  // Adaptive Thresholds
  // ===========================================================================

  /**
   * Record an event timestamp for adaptive threshold calculation
   */
  recordEvent(): void {
    if (!this.config.enableAdaptiveThresholds) return

    const now = Date.now()
    this.eventTimestamps.push(now)

    // Clean old timestamps outside the window
    const cutoff = now - this.config.adaptiveWindowMs
    this.eventTimestamps = this.eventTimestamps.filter(ts => ts >= cutoff)
  }

  /**
   * Calculate the adaptive batch threshold based on recent event rate
   *
   * High event rate -> larger batches (more events per row)
   * Low event rate -> smaller batches (faster flush for freshness)
   *
   * @returns Recommended batch threshold
   */
  calculateAdaptiveThreshold(): number {
    if (!this.config.enableAdaptiveThresholds) {
      return 100 // Default
    }

    const now = Date.now()
    const cutoff = now - this.config.adaptiveWindowMs
    const recentEvents = this.eventTimestamps.filter(ts => ts >= cutoff)
    const eventsPerMinute = recentEvents.length

    // Calculate threshold based on events per minute
    // Higher rate = higher threshold (batch more to reduce rows)
    // Lower rate = lower threshold (flush faster for freshness)
    let threshold: number

    if (eventsPerMinute > 1000) {
      // Very high rate: use maximum batching
      threshold = this.config.maxBatchThreshold
    } else if (eventsPerMinute > 100) {
      // High rate: scale linearly
      const ratio = (eventsPerMinute - 100) / 900
      threshold = Math.floor(
        this.config.minBatchThreshold +
          ratio * (this.config.maxBatchThreshold - this.config.minBatchThreshold)
      )
    } else if (eventsPerMinute > 10) {
      // Moderate rate: use moderate batching
      const ratio = (eventsPerMinute - 10) / 90
      threshold = Math.floor(this.config.minBatchThreshold + ratio * 90)
    } else {
      // Low rate: use minimum batching for freshness
      threshold = this.config.minBatchThreshold
    }

    this.currentBatchThreshold = threshold
    return threshold
  }

  /**
   * Get current adaptive batch threshold
   */
  getCurrentBatchThreshold(): number {
    return this.currentBatchThreshold
  }

  // ===========================================================================
  // Cost Analysis
  // ===========================================================================

  /**
   * Calculate current WAL cost metrics
   *
   * @returns Cost analysis report
   */
  async analyzeCosts(): Promise<{
    totalBatches: number
    totalEvents: number
    avgEventsPerBatch: number
    totalBlobSize: number
    estimatedCostSavings: {
      vsPerEventStorage: number
      percentage: number
    }
    compressionStats: {
      enabled: boolean
      avgRatio: number
    }
    recommendations: string[]
  }> {
    interface BatchStatsRow extends Record<string, SqlStorageValue> {
      batch_count: number
      total_events: number
    }

    interface WalStatsRow extends Record<string, SqlStorageValue> {
      batch_count: number
    }

    // Get event_batches stats
    const batchStats = [...this.sql.exec<BatchStatsRow>(
      `SELECT COUNT(*) as batch_count, COALESCE(SUM(event_count), 0) as total_events
       FROM event_batches WHERE flushed = 0`
    )]

    const batchCount = batchStats[0]?.batch_count ?? 0
    const batchEventCount = Number(batchStats[0]?.total_events ?? 0)

    // Get events_wal stats
    const walStats = [...this.sql.exec<WalStatsRow>(
      `SELECT COUNT(*) as batch_count FROM events_wal`
    )]

    const walBatchCount = walStats[0]?.batch_count ?? 0

    // Estimate WAL events (need to read blobs)
    let walEventCount = 0
    let totalBlobSize = 0
    let compressionRatios: number[] = []

    interface WalBlobRow extends Record<string, SqlStorageValue> {
      events: ArrayBuffer
    }

    const walBlobs = [...this.sql.exec<WalBlobRow>(`SELECT events FROM events_wal`)]
    for (const row of walBlobs) {
      const blob = new Uint8Array(row.events)
      totalBlobSize += blob.length
      const events = decompressEvents(blob)
      walEventCount += events.length

      // Calculate compression ratio
      if (events.length > 0) {
        const { ratio } = calculateCompressionRatio(events)
        compressionRatios.push(ratio)
      }
    }

    const totalBatches = batchCount + walBatchCount
    const totalEvents = batchEventCount + walEventCount
    const avgEventsPerBatch = totalBatches > 0 ? totalEvents / totalBatches : 0

    // Cost savings calculation
    // Without batching: totalEvents rows
    // With batching: totalBatches rows
    const rowsSavedByBatching = totalEvents - totalBatches
    const percentageSavings = totalEvents > 0 ? (rowsSavedByBatching / totalEvents) * 100 : 0

    // Compression stats
    const avgCompressionRatio =
      compressionRatios.length > 0
        ? compressionRatios.reduce((a, b) => a + b, 0) / compressionRatios.length
        : 1

    // Recommendations
    const recommendations: string[] = []

    if (avgEventsPerBatch < 50 && totalBatches > 10) {
      recommendations.push(
        'Consider increasing batch threshold - low events per batch indicates frequent flushes'
      )
    }

    if (totalBatches > this.config.compactionMinBatches) {
      recommendations.push(`Run compaction - ${totalBatches} batches can be merged`)
    }

    if (!this.config.enableCompression && avgCompressionRatio < 0.7) {
      recommendations.push(
        `Enable compression - estimated ${((1 - avgCompressionRatio) * 100).toFixed(0)}% size reduction available`
      )
    }

    if (batchCount > 100) {
      recommendations.push(
        'Many unflushed batches - consider triggering a flush to R2'
      )
    }

    return {
      totalBatches,
      totalEvents,
      avgEventsPerBatch,
      totalBlobSize,
      estimatedCostSavings: {
        vsPerEventStorage: rowsSavedByBatching,
        percentage: percentageSavings,
      },
      compressionStats: {
        enabled: this.config.enableCompression,
        avgRatio: avgCompressionRatio,
      },
      recommendations,
    }
  }

  // ===========================================================================
  // Vacuum
  // ===========================================================================

  /**
   * Run full WAL optimization cycle
   *
   * This combines compaction, pruning, and adaptive threshold calculation
   * into a single operation for convenience.
   *
   * @returns Optimization results
   */
  async optimize(): Promise<{
    compaction: { rowsSaved: number; batchesMerged: number }
    pruning: { batchesDeleted: number }
    adaptiveThreshold: number
    costAnalysis: Awaited<ReturnType<WalOptimizer['analyzeCosts']>>
  }> {
    // Run compaction
    const compaction = await this.compactWalBatches()

    // Run pruning
    const pruning = {
      batchesDeleted: await this.pruneFlushedBatches(),
    }

    // Calculate adaptive threshold
    const adaptiveThreshold = this.calculateAdaptiveThreshold()

    // Analyze costs
    const costAnalysis = await this.analyzeCosts()

    return {
      compaction,
      pruning,
      adaptiveThreshold,
      costAnalysis,
    }
  }

  // ===========================================================================
  // Configuration
  // ===========================================================================

  /**
   * Update optimizer configuration
   */
  setConfig(config: Partial<WalOptimizerConfig>): void {
    this.config = { ...this.config, ...config }
  }

  /**
   * Get current configuration
   */
  getConfig(): WalOptimizerConfig {
    return { ...this.config }
  }
}
