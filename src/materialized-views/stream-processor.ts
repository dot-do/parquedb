/**
 * StreamProcessor for Local Materialized View Processing
 *
 * A generic stream processor for Node.js environments that handles:
 * - Batching: Accumulates records until batch size or timeout
 * - Flushing: Writes batches to Parquet files via StorageBackend
 * - Backpressure: Pauses ingestion when write buffer is full
 * - Recovery: Handles failures gracefully with retry logic
 * - Persistence: WAL, DLQ persistence, and checkpointing for crash recovery
 *
 * This is designed for local AI observability use cases where streaming
 * data (AI SDK logs, evalite traces, etc.) needs to be materialized
 * to Parquet files efficiently.
 *
 * @example
 * ```typescript
 * const processor = new StreamProcessor<AIRequest>({
 *   name: 'ai-requests',
 *   storage,
 *   outputPath: '_views/ai_requests/data',
 *   batchSize: 100,
 *   flushIntervalMs: 5000,
 *   schema: aiRequestSchema,
 *   persistence: { enabled: true }, // Enable crash recovery
 * })
 *
 * await processor.start() // Recovers any pending records from WAL/DLQ
 *
 * // Push records as they arrive
 * processor.push({ model: 'gpt-4', prompt: '...', tokens: 150 })
 *
 * // Or use async iterator
 * for await (const request of aiSdkStream) {
 *   processor.push(request)
 * }
 *
 * await processor.stop() // Flushes remaining records
 * ```
 */

import type { StorageBackend, WriteResult } from '../types/storage'
import type { ParquetSchema } from '../parquet/types'
import { ParquetWriter } from '../parquet/writer'
import {
  StreamPersistence,
  createStreamPersistence,
  type StreamCheckpoint,
  type RecoveryResult,
  type PersistenceStats,
} from './stream-persistence'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for StreamProcessor
 */
export interface StreamProcessorConfig<T> {
  /** Unique name for this processor (used for file naming) */
  name: string

  /** Storage backend for writing Parquet files */
  storage: StorageBackend

  /** Base path for output files (e.g., '_views/ai_requests/data') */
  outputPath: string

  /** Parquet schema for the records */
  schema: ParquetSchema

  /**
   * Number of records to batch before writing
   * @default 1000
   */
  batchSize?: number

  /**
   * Maximum time (ms) to wait before flushing a partial batch
   * @default 5000
   */
  flushIntervalMs?: number

  /**
   * Maximum number of pending writes before applying backpressure
   * @default 3
   */
  maxPendingWrites?: number

  /**
   * Maximum in-memory buffer size (records) before applying backpressure
   * @default 10000
   */
  maxBufferSize?: number

  /**
   * Row group size for Parquet files
   * @default 5000
   */
  rowGroupSize?: number

  /**
   * Compression codec for Parquet files
   * @default 'lz4'
   */
  compression?: 'none' | 'snappy' | 'gzip' | 'lz4' | 'zstd'

  /**
   * Transform function to apply to each record before writing
   */
  transform?: (record: T) => Record<string, unknown>

  /**
   * Callback when a batch is successfully written
   */
  onBatchWritten?: (result: BatchWriteResult) => void

  /**
   * Callback when an error occurs
   */
  onError?: (error: Error, context: ErrorContext<T>) => void

  /**
   * Callback specifically for write failures after all retries exhausted.
   * This receives the full failed batch with all records for replay capability.
   * Required when writeFailureBehavior is 'callback'.
   */
  onWriteError?: (failedBatch: FailedBatch<T>) => void | Promise<void>

  /**
   * Behavior when a write fails after all retries.
   * - 'silent': Drop records silently (legacy, not recommended)
   * - 'queue': Add to dead-letter queue (access via getDeadLetterQueue())
   * - 'throw': Throw WriteFailureError (stops processing)
   * - 'callback': Call onWriteError (required when using this option)
   * @default 'queue'
   */
  writeFailureBehavior?: WriteFailureBehavior

  /**
   * Maximum size of the dead-letter queue before applying backpressure
   * @default 1000
   */
  maxDeadLetterQueueSize?: number

  /**
   * Retry configuration for failed writes
   */
  retry?: RetryConfig

  /**
   * Persistence configuration for crash recovery
   *
   * When enabled, the processor will:
   * - Log records to WAL before writing to Parquet
   * - Persist failed batches to disk (surviving crashes)
   * - Checkpoint stream position for incremental recovery
   *
   * @default { enabled: false }
   */
  persistence?: PersistenceConfig
}

/**
 * Retry configuration
 */
export interface RetryConfig {
  /** Maximum retry attempts */
  maxAttempts?: number

  /** Initial delay between retries (ms) */
  initialDelayMs?: number

  /** Maximum delay between retries (ms) */
  maxDelayMs?: number

  /** Multiplier for exponential backoff */
  backoffMultiplier?: number
}

/**
 * Persistence configuration for crash recovery
 */
export interface PersistenceConfig {
  /**
   * Enable persistence (WAL, DLQ persistence, checkpointing)
   * @default false
   */
  enabled: boolean

  /**
   * Maximum WAL segment size in bytes before rotation
   * @default 10MB
   */
  maxWalSegmentSize?: number

  /**
   * Maximum age of WAL segments before archiving (ms)
   * @default 1 hour
   */
  maxWalSegmentAge?: number

  /**
   * How often to save checkpoints (in batches)
   * @default 10 (save checkpoint every 10 batches)
   */
  checkpointInterval?: number

  /**
   * Whether to sync WAL writes immediately (durability vs performance)
   * @default true
   */
  syncWal?: boolean

  /**
   * Callback when recovery completes
   */
  onRecovery?: (result: RecoveryResult) => void
}

/**
 * A failed batch that couldn't be written after all retries
 */
export interface FailedBatch<T> {
  /** Records that failed to write */
  records: T[]

  /** Batch number */
  batchNumber: number

  /** File path that was attempted */
  filePath: string

  /** The error that caused the failure */
  error: Error

  /** Timestamp when failure occurred */
  failedAt: number

  /** Number of retry attempts made */
  attempts: number
}

/**
 * Behavior when a write fails after all retries
 */
export type WriteFailureBehavior =
  | 'silent' // Legacy behavior: silently drop (not recommended)
  | 'queue' // Add to dead-letter queue for later replay
  | 'throw' // Throw exception (stops processing)
  | 'callback' // Require onWriteError callback to handle

/**
 * Result of a batch write operation
 */
export interface BatchWriteResult {
  /** Number of records written */
  recordCount: number

  /** Path to the written file */
  filePath: string

  /** Write result from storage backend */
  writeResult: WriteResult

  /** Duration of the write operation (ms) */
  durationMs: number

  /** Sequence number of this batch */
  batchNumber: number
}

/**
 * Context for error callbacks
 */
export interface ErrorContext<T> {
  /** Phase where error occurred */
  phase: 'transform' | 'write' | 'flush'

  /** Records involved (if available) */
  records?: T[]

  /** Batch number (if applicable) */
  batchNumber?: number

  /** File path (if applicable) */
  filePath?: string

  /** Number of retry attempts made (for write phase) */
  attempts?: number
}

/**
 * Error thrown when write fails and writeFailureBehavior is 'throw'
 */
export class WriteFailureError<T> extends Error {
  readonly failedBatch: FailedBatch<T>

  constructor(failedBatch: FailedBatch<T>) {
    super(
      `Failed to write batch ${failedBatch.batchNumber} after ${failedBatch.attempts} attempts: ${failedBatch.error.message}`
    )
    this.name = 'WriteFailureError'
    this.failedBatch = failedBatch
  }
}

/**
 * Statistics for the stream processor
 */
export interface StreamProcessorStats {
  /** Total records received */
  recordsReceived: number

  /** Total records written to storage */
  recordsWritten: number

  /** Total batches written */
  batchesWritten: number

  /** Total failed batches */
  failedBatches: number

  /** Current buffer size */
  bufferSize: number

  /** Current pending writes */
  pendingWrites: number

  /** Total bytes written */
  bytesWritten: number

  /** Number of times backpressure was applied */
  backpressureEvents: number

  /** Average batch write duration (ms) */
  avgBatchDurationMs: number

  /** Processor start time */
  startedAt: number | null

  /** Last record received time */
  lastRecordAt: number | null

  /** Last successful write time */
  lastWriteAt: number | null

  /** Records recovered from WAL on startup */
  recordsRecovered: number

  /** DLQ entries recovered on startup */
  dlqEntriesRecovered: number

  /** Last checkpoint sequence (if using persistence) */
  lastCheckpointSequence?: string | number
}

/**
 * Processor state
 */
export type ProcessorState = 'idle' | 'running' | 'stopping' | 'stopped' | 'error'

// =============================================================================
// Default Configuration
// =============================================================================

const DEFAULT_CONFIG = {
  batchSize: 1000,
  flushIntervalMs: 5000,
  maxPendingWrites: 3,
  maxBufferSize: 10000,
  rowGroupSize: 5000,
  compression: 'lz4' as const,
  writeFailureBehavior: 'queue' as WriteFailureBehavior,
  maxDeadLetterQueueSize: 1000,
  retry: {
    maxAttempts: 3,
    initialDelayMs: 100,
    maxDelayMs: 5000,
    backoffMultiplier: 2,
  },
  persistence: {
    enabled: false,
    maxWalSegmentSize: 10 * 1024 * 1024, // 10MB
    maxWalSegmentAge: 60 * 60 * 1000, // 1 hour
    checkpointInterval: 10, // Save checkpoint every 10 batches
    syncWal: true,
  },
}

// =============================================================================
// StreamProcessor Class
// =============================================================================

/**
 * Stream processor for materializing streaming data to Parquet files.
 *
 * Handles batching, flushing, and backpressure for efficient writes
 * in Node.js environments.
 */
export class StreamProcessor<T extends Record<string, unknown> = Record<string, unknown>> {
  private config: Required<
    Omit<StreamProcessorConfig<T>, 'transform' | 'onBatchWritten' | 'onError' | 'onWriteError'>
  > & {
    transform?: StreamProcessorConfig<T>['transform']
    onBatchWritten?: StreamProcessorConfig<T>['onBatchWritten']
    onError?: StreamProcessorConfig<T>['onError']
    onWriteError?: StreamProcessorConfig<T>['onWriteError']
    persistence: Required<Omit<PersistenceConfig, 'onRecovery'>> & { onRecovery?: PersistenceConfig['onRecovery'] }
  }

  private state: ProcessorState = 'idle'
  private buffer: T[] = []
  private pendingWrites = 0
  private batchCounter = 0
  private flushTimer: ReturnType<typeof setTimeout> | null = null
  private writer: ParquetWriter

  // Backpressure management
  private backpressurePromise: Promise<void> | null = null
  private backpressureResolve: (() => void) | null = null

  // Dead-letter queue for failed batches
  private deadLetterQueue: FailedBatch<T>[] = []

  // Persistence layer for crash recovery
  private persistence: StreamPersistence<T> | null = null
  private batchesSinceCheckpoint = 0

  // Statistics
  private stats: StreamProcessorStats = this.createEmptyStats()
  private batchDurations: number[] = []

  constructor(config: StreamProcessorConfig<T>) {
    // Validate configuration
    if (config.writeFailureBehavior === 'callback' && !config.onWriteError) {
      throw new Error("onWriteError callback is required when writeFailureBehavior is 'callback'")
    }

    this.config = {
      ...DEFAULT_CONFIG,
      ...config,
      retry: {
        ...DEFAULT_CONFIG.retry,
        ...config.retry,
      },
      persistence: {
        ...DEFAULT_CONFIG.persistence,
        ...config.persistence,
      },
    }

    this.writer = new ParquetWriter(this.config.storage, {
      rowGroupSize: this.config.rowGroupSize,
      compression: this.config.compression,
    })

    // Initialize persistence if enabled
    if (this.config.persistence.enabled) {
      this.persistence = createStreamPersistence<T>({
        name: this.config.name,
        storage: this.config.storage,
        basePath: this.config.outputPath,
        maxWalSegmentSize: this.config.persistence.maxWalSegmentSize,
        maxWalSegmentAge: this.config.persistence.maxWalSegmentAge,
        syncWal: this.config.persistence.syncWal,
      })
    }
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Start the processor
   *
   * When persistence is enabled, this will:
   * 1. Recover any pending records from WAL
   * 2. Recover any failed batches from DLQ
   * 3. Load the last checkpoint
   * 4. Re-process recovered records
   */
  async start(): Promise<void> {
    if (this.state === 'running') {
      return
    }

    if (this.state === 'stopping') {
      throw new Error('Cannot start while stopping')
    }

    // Perform recovery if persistence is enabled
    if (this.persistence) {
      const recoveryResult = await this.persistence.recover()

      // Update stats with recovery information
      this.stats.recordsRecovered = recoveryResult.pendingRecords.length
      this.stats.dlqEntriesRecovered = recoveryResult.dlqEntriesRecovered

      // Restore checkpoint info
      if (recoveryResult.checkpoint) {
        this.stats.lastCheckpointSequence = recoveryResult.checkpoint.sequence
        this.batchCounter = recoveryResult.checkpoint.lastBatchNumber
      }

      // Add recovered records to buffer for re-processing
      if (recoveryResult.pendingRecords.length > 0) {
        this.buffer.push(...recoveryResult.pendingRecords)
        this.stats.bufferSize = this.buffer.length
      }

      // Restore DLQ entries
      if (recoveryResult.failedBatches.length > 0) {
        this.deadLetterQueue.push(...recoveryResult.failedBatches)
      }

      // Call recovery callback if provided
      if (this.config.persistence.onRecovery) {
        this.config.persistence.onRecovery(recoveryResult)
      }

      // Log recovery status
      if (!recoveryResult.cleanRecovery) {
        console.log(
          `[StreamProcessor:${this.config.name}] Recovery completed: ` +
            `${recoveryResult.walEntriesRecovered} WAL entries, ` +
            `${recoveryResult.dlqEntriesRecovered} DLQ entries, ` +
            `checkpoint: ${recoveryResult.checkpoint?.sequence ?? 'none'}`
        )
      }
    }

    this.state = 'running'
    this.stats.startedAt = Date.now()
    this.startFlushTimer()
  }

  /**
   * Stop the processor gracefully
   *
   * Flushes any remaining buffered records before stopping.
   * When persistence is enabled, saves a final checkpoint.
   */
  async stop(): Promise<void> {
    if (this.state !== 'running') {
      return
    }

    this.state = 'stopping'
    this.stopFlushTimer()

    // Flush remaining records
    if (this.buffer.length > 0) {
      await this.flush()
    }

    // Wait for pending writes to complete
    while (this.pendingWrites > 0) {
      await new Promise((resolve) => setTimeout(resolve, 50))
    }

    // Save final checkpoint if persistence is enabled
    if (this.persistence) {
      await this.saveCheckpoint()
    }

    this.state = 'stopped'
  }

  /**
   * Push a single record for processing
   *
   * @returns Promise that resolves when the record is buffered (may wait for backpressure)
   */
  async push(record: T): Promise<void> {
    if (this.state !== 'running') {
      throw new Error(`StreamProcessor is not running (state: ${this.state})`)
    }

    // Wait for backpressure to clear
    await this.waitForBackpressure()

    this.buffer.push(record)
    this.stats.recordsReceived++
    this.stats.bufferSize = this.buffer.length
    this.stats.lastRecordAt = Date.now()

    // Check if we should apply backpressure
    this.checkBackpressure()

    // Check if we should flush
    if (this.buffer.length >= this.config.batchSize) {
      await this.triggerFlush()
    }
  }

  /**
   * Push multiple records for processing
   */
  async pushMany(records: T[]): Promise<void> {
    for (const record of records) {
      await this.push(record)
    }
  }

  /**
   * Force flush all buffered records immediately
   */
  async flush(): Promise<void> {
    if (this.buffer.length === 0) {
      return
    }

    await this.writeBatch()
  }

  /**
   * Get current processor state
   */
  getState(): ProcessorState {
    return this.state
  }

  /**
   * Check if the processor is running
   */
  isRunning(): boolean {
    return this.state === 'running'
  }

  /**
   * Get current statistics
   */
  getStats(): StreamProcessorStats {
    // Calculate average batch duration
    if (this.batchDurations.length > 0) {
      const sum = this.batchDurations.reduce((a, b) => a + b, 0)
      this.stats.avgBatchDurationMs = sum / this.batchDurations.length
    }

    return { ...this.stats }
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.stats = this.createEmptyStats()
    this.stats.startedAt = this.state === 'running' ? Date.now() : null
    this.batchDurations = []
  }

  /**
   * Get the processor name
   */
  getName(): string {
    return this.config.name
  }

  /**
   * Check if persistence is enabled
   */
  isPersistenceEnabled(): boolean {
    return this.persistence !== null
  }

  /**
   * Get persistence statistics
   *
   * Returns information about WAL, DLQ, and checkpoint state.
   * Only available if persistence is enabled.
   */
  async getPersistenceStats(): Promise<PersistenceStats | null> {
    if (!this.persistence) return null
    return this.persistence.getStats()
  }

  /**
   * Manually save a checkpoint
   *
   * This is useful for ensuring state is persisted before a graceful shutdown
   * or at critical points in processing.
   */
  async checkpoint(metadata?: Record<string, unknown>): Promise<void> {
    if (!this.persistence) {
      throw new Error('Persistence is not enabled')
    }

    await this.persistence.saveCheckpoint({
      timestamp: Date.now(),
      lastBatchNumber: this.batchCounter,
      recordsProcessed: this.stats.recordsWritten,
      metadata: {
        ...metadata,
        bufferSize: this.buffer.length,
        pendingWrites: this.pendingWrites,
        dlqSize: this.deadLetterQueue.length,
      },
    })
    this.stats.lastCheckpointSequence = this.batchCounter
  }

  /**
   * Get the last checkpoint
   *
   * Returns the most recently saved checkpoint, or null if none exists.
   */
  async getCheckpoint(): Promise<StreamCheckpoint | null> {
    if (!this.persistence) return null
    return this.persistence.loadCheckpoint()
  }

  /**
   * Clean up old WAL archive files
   *
   * @param maxAgeMs Maximum age of archives to keep (default: 24 hours)
   * @returns Number of files cleaned up
   */
  async cleanupWALArchives(maxAgeMs?: number): Promise<number> {
    if (!this.persistence) return 0
    return this.persistence.cleanupWALArchives(maxAgeMs)
  }

  /**
   * Get the dead-letter queue containing failed batches
   *
   * Failed batches are added to this queue when writeFailureBehavior is 'queue'.
   * Each entry contains all records from the failed batch along with metadata
   * for replay purposes.
   */
  getDeadLetterQueue(): ReadonlyArray<FailedBatch<T>> {
    return this.deadLetterQueue
  }

  /**
   * Get the number of failed batches in the dead-letter queue
   */
  getDeadLetterQueueSize(): number {
    return this.deadLetterQueue.length
  }

  /**
   * Clear the dead-letter queue
   *
   * Use this after you've processed/replayed the failed batches.
   * Returns the cleared batches for final processing.
   * Also clears persisted DLQ entries if persistence is enabled.
   */
  async clearDeadLetterQueue(): Promise<FailedBatch<T>[]> {
    const batches = this.deadLetterQueue
    this.deadLetterQueue = []

    // Clear persisted DLQ entries
    if (this.persistence) {
      try {
        await this.persistence.clearDLQ()
      } catch (error) {
        console.warn(`[StreamProcessor:${this.config.name}] Failed to clear persisted DLQ:`, error)
      }
    }

    return batches
  }

  /**
   * Retry all batches in the dead-letter queue
   *
   * Attempts to write each failed batch again. Successfully written batches
   * are removed from the queue. Returns the number of batches successfully retried.
   */
  async retryDeadLetterQueue(): Promise<number> {
    if (this.deadLetterQueue.length === 0) {
      return 0
    }

    const batchesToRetry = [...this.deadLetterQueue]
    this.deadLetterQueue = []
    let successCount = 0

    for (const failedBatch of batchesToRetry) {
      try {
        await this.writeWithRetry(
          failedBatch.filePath,
          failedBatch.records as Record<string, unknown>[],
          failedBatch.batchNumber
        )
        successCount++
        this.stats.recordsWritten += failedBatch.records.length
        this.stats.batchesWritten++

        // Remove from persisted DLQ on success
        if (this.persistence) {
          try {
            await this.persistence.updateDLQEntry(failedBatch, true)
          } catch {
            // Best-effort cleanup
          }
        }
      } catch (err) {
        // Still failed, add back to DLQ
        const updatedBatch: FailedBatch<T> = {
          ...failedBatch,
          error: err instanceof Error ? err : new Error(String(err)),
          failedAt: Date.now(),
          attempts: failedBatch.attempts + this.config.retry.maxAttempts,
        }
        this.deadLetterQueue.push(updatedBatch)

        // Update persisted DLQ entry
        if (this.persistence) {
          try {
            await this.persistence.updateDLQEntry(failedBatch, false)
          } catch {
            // Best-effort update
          }
        }
      }
    }

    return successCount
  }

  // ===========================================================================
  // Internal Methods
  // ===========================================================================

  /**
   * Create empty stats object
   */
  private createEmptyStats(): StreamProcessorStats {
    return {
      recordsReceived: 0,
      recordsWritten: 0,
      batchesWritten: 0,
      failedBatches: 0,
      bufferSize: 0,
      pendingWrites: 0,
      bytesWritten: 0,
      backpressureEvents: 0,
      avgBatchDurationMs: 0,
      startedAt: null,
      lastRecordAt: null,
      lastWriteAt: null,
      recordsRecovered: 0,
      dlqEntriesRecovered: 0,
    }
  }

  /**
   * Save a checkpoint to persistence layer
   */
  private async saveCheckpoint(): Promise<void> {
    if (!this.persistence) return

    try {
      await this.persistence.saveCheckpoint({
        timestamp: Date.now(),
        lastBatchNumber: this.batchCounter,
        recordsProcessed: this.stats.recordsWritten,
        metadata: {
          bufferSize: this.buffer.length,
          pendingWrites: this.pendingWrites,
          dlqSize: this.deadLetterQueue.length,
        },
      })
      this.stats.lastCheckpointSequence = this.batchCounter
    } catch (error) {
      console.warn(`[StreamProcessor:${this.config.name}] Failed to save checkpoint:`, error)
    }
  }

  /**
   * Start the flush timer
   */
  private startFlushTimer(): void {
    if (this.flushTimer) {
      return
    }

    this.flushTimer = setInterval(() => {
      if (this.buffer.length > 0 && this.state === 'running') {
        this.triggerFlush().catch((err) => {
          this.emitError(err instanceof Error ? err : new Error(String(err)), {
            phase: 'flush',
          })
        })
      }
    }, this.config.flushIntervalMs)
  }

  /**
   * Stop the flush timer
   */
  private stopFlushTimer(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer)
      this.flushTimer = null
    }
  }

  /**
   * Trigger a flush (non-blocking, manages pending writes)
   */
  private async triggerFlush(): Promise<void> {
    // Don't trigger if we're at max pending writes
    if (this.pendingWrites >= this.config.maxPendingWrites) {
      return
    }

    await this.writeBatch()
  }

  /**
   * Write a batch of records to storage
   */
  private async writeBatch(): Promise<void> {
    if (this.buffer.length === 0) {
      return
    }

    // Snapshot and clear buffer
    const records = this.buffer.splice(0, this.config.batchSize)
    this.stats.bufferSize = this.buffer.length
    this.batchCounter++
    const batchNumber = this.batchCounter

    this.pendingWrites++
    this.stats.pendingWrites = this.pendingWrites

    const startTime = Date.now()
    const filePath = this.getBatchFilePath(batchNumber)

    // Log to WAL before writing (if persistence enabled)
    let walEntryId: string | undefined
    if (this.persistence) {
      try {
        const walEntry = await this.persistence.logWAL(records, batchNumber, filePath)
        walEntryId = walEntry.id
      } catch (walError) {
        // WAL write failed - continue without WAL protection
        console.warn(`[StreamProcessor:${this.config.name}] WAL write failed:`, walError)
      }
    }

    try {
      // Transform records if needed
      const transformedRecords = records.map((record) => {
        if (this.config.transform) {
          try {
            return this.config.transform(record)
          } catch (err) {
            this.emitError(err instanceof Error ? err : new Error(String(err)), {
              phase: 'transform',
              records: [record],
              batchNumber,
            })
            return record as Record<string, unknown>
          }
        }
        return record as Record<string, unknown>
      })

      // Write with retry
      const writeResult = await this.writeWithRetry(filePath, transformedRecords, batchNumber)

      // Commit WAL entry on success
      if (this.persistence && walEntryId) {
        try {
          await this.persistence.commitWAL(walEntryId)
        } catch (commitError) {
          // WAL commit failed - not critical since data was written
          console.warn(`[StreamProcessor:${this.config.name}] WAL commit failed:`, commitError)
        }
      }

      const durationMs = Date.now() - startTime

      // Update stats
      this.stats.recordsWritten += records.length
      this.stats.batchesWritten++
      this.stats.bytesWritten += writeResult.size
      this.stats.lastWriteAt = Date.now()
      this.batchDurations.push(durationMs)

      // Keep only last 100 timing samples
      if (this.batchDurations.length > 100) {
        this.batchDurations.shift()
      }

      // Save checkpoint periodically
      this.batchesSinceCheckpoint++
      if (this.persistence && this.batchesSinceCheckpoint >= this.config.persistence.checkpointInterval) {
        await this.saveCheckpoint()
        this.batchesSinceCheckpoint = 0
      }

      // Emit callback
      if (this.config.onBatchWritten) {
        this.config.onBatchWritten({
          recordCount: records.length,
          filePath,
          writeResult,
          durationMs,
          batchNumber,
        })
      }
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err))
      this.stats.failedBatches++

      // Mark WAL entry as failed
      if (this.persistence && walEntryId) {
        try {
          await this.persistence.failWAL(walEntryId, error)
        } catch {
          // Best-effort WAL update
        }
      }

      // Create failed batch info for logging and DLQ
      const failedBatch: FailedBatch<T> = {
        records,
        batchNumber,
        filePath,
        error,
        failedAt: Date.now(),
        attempts: this.config.retry.maxAttempts,
      }

      // Log detailed info for replay capability
      console.error(`[StreamProcessor:${this.config.name}] Write failure after ${failedBatch.attempts} attempts:`, {
        batchNumber,
        filePath,
        recordCount: records.length,
        error: error.message,
        firstRecordId:
          (records[0] as Record<string, unknown>)?.id ?? (records[0] as Record<string, unknown>)?.$id,
        failedAt: new Date(failedBatch.failedAt).toISOString(),
      })

      // Handle failure based on configured behavior
      await this.handleWriteFailure(failedBatch)

      // Always emit error for backwards compatibility
      this.emitError(error, {
        phase: 'write',
        records,
        batchNumber,
        filePath,
        attempts: this.config.retry.maxAttempts,
      })
    } finally {
      this.pendingWrites--
      this.stats.pendingWrites = this.pendingWrites

      // Release backpressure if applicable
      this.releaseBackpressure()
    }
  }

  /**
   * Handle a write failure based on configured behavior
   */
  private async handleWriteFailure(failedBatch: FailedBatch<T>): Promise<void> {
    switch (this.config.writeFailureBehavior) {
      case 'silent':
        // Legacy behavior: silently drop records (not recommended)
        // Records are already removed from buffer and will be lost
        break

      case 'queue':
        // Add to dead-letter queue for later replay
        this.deadLetterQueue.push(failedBatch)

        // Persist to disk if persistence enabled (survives crashes)
        if (this.persistence) {
          try {
            await this.persistence.persistDLQ(failedBatch)
          } catch (persistError) {
            console.error(
              `[StreamProcessor:${this.config.name}] Failed to persist DLQ entry to disk:`,
              persistError
            )
          }
        }

        // Apply backpressure if DLQ is getting full
        if (this.deadLetterQueue.length >= this.config.maxDeadLetterQueueSize) {
          console.warn(
            `[StreamProcessor:${this.config.name}] Dead-letter queue is full (${this.deadLetterQueue.length} batches). ` +
              `Consider processing failed batches with retryDeadLetterQueue() or clearDeadLetterQueue().`
          )
        }
        break

      case 'throw':
        // Throw exception to stop processing
        throw new WriteFailureError(failedBatch)

      case 'callback':
        // Call the onWriteError callback (required for this behavior)
        if (this.config.onWriteError) {
          try {
            await this.config.onWriteError(failedBatch)
          } catch (callbackErr) {
            // Log but don't fail on callback errors
            console.error(`[StreamProcessor:${this.config.name}] onWriteError callback threw:`, callbackErr)
          }
        }
        break

      default: {
        // Exhaustiveness check
        const _exhaustive: never = this.config.writeFailureBehavior
        throw new Error(`Unknown writeFailureBehavior: ${_exhaustive}`)
      }
    }
  }

  /**
   * Write to storage with retry logic
   */
  private async writeWithRetry(
    filePath: string,
    records: Record<string, unknown>[],
    batchNumber: number
  ): Promise<WriteResult> {
    const { maxAttempts, initialDelayMs, maxDelayMs, backoffMultiplier } = this.config.retry
    let lastError: Error | null = null
    let delay = initialDelayMs

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        await this.writer.write(filePath, records, this.config.schema)

        // Get file size from stat
        const stat = await this.config.storage.stat(filePath)
        return {
          etag: `batch-${batchNumber}`,
          size: stat?.size ?? 0,
        }
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err))

        if (attempt < maxAttempts - 1) {
          await new Promise((resolve) => setTimeout(resolve, delay))
          delay = Math.min(delay * backoffMultiplier, maxDelayMs)
        }
      }
    }

    throw lastError
  }

  /**
   * Get the file path for a batch
   */
  private getBatchFilePath(batchNumber: number): string {
    const paddedNumber = batchNumber.toString().padStart(8, '0')
    const timestamp = Date.now()
    return `${this.config.outputPath}/${this.config.name}.${paddedNumber}.${timestamp}.parquet`
  }

  /**
   * Check if backpressure should be applied
   */
  private checkBackpressure(): void {
    const shouldApplyBackpressure =
      this.buffer.length >= this.config.maxBufferSize || this.pendingWrites >= this.config.maxPendingWrites

    if (shouldApplyBackpressure && !this.backpressurePromise) {
      this.stats.backpressureEvents++
      this.backpressurePromise = new Promise((resolve) => {
        this.backpressureResolve = resolve
      })
    }
  }

  /**
   * Wait for backpressure to clear
   */
  private async waitForBackpressure(): Promise<void> {
    if (this.backpressurePromise) {
      await this.backpressurePromise
    }
  }

  /**
   * Release backpressure if conditions are met
   */
  private releaseBackpressure(): void {
    const shouldRelease =
      this.buffer.length < this.config.maxBufferSize * 0.8 && this.pendingWrites < this.config.maxPendingWrites

    if (shouldRelease && this.backpressureResolve) {
      this.backpressureResolve()
      this.backpressurePromise = null
      this.backpressureResolve = null
    }
  }

  /**
   * Emit an error
   */
  private emitError(error: Error, context: ErrorContext<T>): void {
    if (this.config.onError) {
      try {
        this.config.onError(error, context)
      } catch {
        // Ignore errors in error handler
      }
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a new StreamProcessor instance
 */
export function createStreamProcessor<T extends Record<string, unknown>>(
  config: StreamProcessorConfig<T>
): StreamProcessor<T> {
  return new StreamProcessor(config)
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Create a pass-through async iterator that also pushes to a StreamProcessor
 *
 * @example
 * ```typescript
 * const processor = createStreamProcessor({ ... })
 * await processor.start()
 *
 * const teedStream = teeToProcessor(sourceIterator, processor)
 * for await (const item of teedStream) {
 *   // Process item in pipeline AND it gets written to Parquet
 * }
 *
 * await processor.stop()
 * ```
 */
export async function* teeToProcessor<T extends Record<string, unknown>>(
  source: AsyncIterable<T>,
  processor: StreamProcessor<T>
): AsyncIterable<T> {
  for await (const item of source) {
    await processor.push(item)
    yield item
  }
}

/**
 * Drain an async iterator through a StreamProcessor
 *
 * @example
 * ```typescript
 * const processor = createStreamProcessor({ ... })
 * await processor.start()
 *
 * await drainToProcessor(sourceIterator, processor)
 *
 * await processor.stop()
 * ```
 */
export async function drainToProcessor<T extends Record<string, unknown>>(
  source: AsyncIterable<T>,
  processor: StreamProcessor<T>
): Promise<void> {
  for await (const item of source) {
    await processor.push(item)
  }
}

/**
 * Create a WritableStream that pushes to a StreamProcessor
 *
 * Useful for integrating with Web Streams API.
 */
export function createProcessorSink<T extends Record<string, unknown>>(
  processor: StreamProcessor<T>
): WritableStream<T> {
  return new WritableStream({
    async write(chunk) {
      await processor.push(chunk)
    },
  })
}
