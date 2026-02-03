/**
 * Compaction Queue Consumer
 *
 * A Cloudflare Queue consumer that receives R2 object-create events and
 * processes raw event files into Parquet segments.
 *
 * Architecture (Option D - Event-Driven Compaction):
 * ```
 * TailDO → writes raw events to R2
 *              ↓ object-create notification
 *        Queue → Compaction Worker (observable via tail)
 *              ↓ writes Parquet segments
 *        R2 → object-create notification
 *              ↓
 *        Queue → MV refresh / downstream
 * ```
 *
 * Benefits:
 * - Fully event-driven, no polling
 * - Observable via tail workers (unlike DO internals)
 * - Scales horizontally with queue consumers
 * - Decoupled from TailDO for reliability
 *
 * Dead Letter Queue (DLQ) Support:
 * - Messages that fail processing are retried up to MAX_RETRIES (default: 3)
 * - Messages exceeding max retries are NOT retried - Cloudflare sends them to DLQ
 * - DLQ must be configured in wrangler.toml with `dead_letter_queue` setting
 * - Parse errors and malformed data are acked (not retried) to avoid blocking
 * - Each message has an `attempts` property tracking delivery attempts (starts at 1)
 *
 * @see https://developers.cloudflare.com/r2/buckets/event-notifications/
 * @see https://developers.cloudflare.com/queues/
 * @see https://developers.cloudflare.com/queues/configuration/batching-retries/
 */

import { WorkerLogsMV, createWorkerLogsMV, type TailEvent, type TailItem } from '../streaming/worker-logs'
import { DEFAULT_MAX_RETRIES } from '../constants'
import { logger } from '../utils/logger'
import { R2Backend } from '../storage/R2Backend'
import type { StorageBackend } from '../types/storage'
import type { R2Bucket as InternalR2Bucket } from '../storage/types/r2'
import { toR2Bucket } from '../utils/type-utils'
import type { ValidatedTraceItem } from './tail-validation'

// =============================================================================
// Types
// =============================================================================

/**
 * Environment bindings for the Compaction Consumer
 */
export interface CompactionConsumerEnv {
  /** R2 bucket for reading raw events and writing Parquet files */
  LOGS_BUCKET: R2Bucket

  /** Optional: Prefix for raw event files (default: "raw-events") */
  RAW_EVENTS_PREFIX?: string

  /** Optional: Prefix for Parquet log files (default: "logs/workers") */
  PARQUET_PREFIX?: string

  /** Optional: Flush threshold for WorkerLogsMV (default: 1000) */
  FLUSH_THRESHOLD?: string

  /** Optional: Compression codec (default: "lz4") */
  COMPRESSION?: 'none' | 'snappy' | 'gzip' | 'lz4' | 'zstd'

  /** Optional: Queue for downstream notifications (MV refresh, etc.) */
  DOWNSTREAM_QUEUE?: Queue<DownstreamMessage>

  /**
   * Optional: Maximum retry attempts before message goes to DLQ (default: 3)
   * Should match the `max_retries` setting in wrangler.toml queue consumer config
   */
  MAX_RETRIES?: string
}

/**
 * R2 event notification message from queue
 *
 * @see https://developers.cloudflare.com/r2/buckets/event-notifications/#payload-format
 */
export interface R2EventNotification {
  /** Account ID */
  account: string

  /** Bucket name */
  bucket: string

  /** Object key (path) */
  object: {
    /** Object key */
    key: string
    /** Object size in bytes */
    size: number
    /** Object ETag */
    eTag: string
  }

  /** Event type */
  eventType: 'object-create' | 'object-delete'

  /** Event time in ISO 8601 format */
  eventTime: string

  /** Copy source (for copy operations) */
  copySource?: {
    bucket: string
    object: string
  }
}

/**
 * Raw events file format (NDJSON)
 */
export interface RawEventsFile {
  /** TailDO instance ID that created this file */
  doId: string
  /** Timestamp when the file was created */
  createdAt: number
  /** Batch sequence number (for ordering) */
  batchSeq: number
  /** Array of validated trace items */
  events: ValidatedTraceItem[]
}

/**
 * Message sent to downstream queue after Parquet file is written
 */
export interface DownstreamMessage {
  /** Event type */
  type: 'parquet-written'

  /** Path to the Parquet file */
  parquetPath: string

  /** Number of records in the file */
  recordCount: number

  /** File size in bytes */
  sizeBytes: number

  /** Source raw event files that were processed */
  sourceFiles: string[]

  /** Timestamp when processing completed */
  processedAt: number
}

/**
 * Processing result for a single raw events file
 */
export interface ProcessingResult {
  /** Original file key */
  sourceKey: string
  /** Number of events processed */
  eventsProcessed: number
  /** Whether processing succeeded */
  success: boolean
  /** Error message if failed */
  error?: string
}

/**
 * Batch processing result
 */
export interface BatchResult {
  /** Total messages in the batch */
  totalMessages: number
  /** Successfully processed */
  succeeded: number
  /** Failed to process */
  failed: number
  /** Messages retried (will be redelivered) */
  retried: number
  /** Messages sent to dead letter queue (exceeded max retries or non-retryable) */
  deadLettered: number
  /** Individual results */
  results: ProcessingResult[]
  /** Parquet files written */
  parquetFilesWritten: string[]
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Check if a file is a raw events file based on path
 */
function isRawEventsFile(key: string, prefix: string): boolean {
  return key.startsWith(prefix) && key.endsWith('.ndjson')
}

/**
 * Parse raw events file from NDJSON content
 */
function parseRawEventsFile(content: string): RawEventsFile {
  const lines = content.trim().split('\n').filter(Boolean)
  if (lines.length === 0) {
    throw new Error('Empty raw events file')
  }

  // First line contains metadata
  const firstLine = lines[0]!
  const metadata = JSON.parse(firstLine) as Omit<RawEventsFile, 'events'>

  // Remaining lines are events (NDJSON format)
  const events: ValidatedTraceItem[] = []
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i]!
    try {
      events.push(JSON.parse(line) as ValidatedTraceItem)
    } catch {
      logger.warn(`[CompactionConsumer] Skipping malformed event line ${i}`)
    }
  }

  return {
    ...metadata,
    events,
  }
}

/**
 * Convert ValidatedTraceItem to TailItem for WorkerLogsMV
 */
function validatedItemToTailItem(item: ValidatedTraceItem): TailItem {
  return {
    scriptName: item.scriptName ?? 'unknown',
    outcome: item.outcome as TailItem['outcome'],
    eventTimestamp: item.eventTimestamp ?? Date.now(),
    event: item.event ? {
      request: item.event.request ? {
        method: item.event.request.method,
        url: item.event.request.url,
        headers: item.event.request.headers,
        cf: item.event.request.cf,
      } : undefined!,
      response: item.event.response,
    } : null,
    logs: item.logs.map(log => ({
      timestamp: log.timestamp,
      level: log.level as TailItem['logs'][number]['level'],
      message: [log.message],
    })),
    exceptions: item.exceptions,
  }
}

// =============================================================================
// Compaction Consumer Class
// =============================================================================

/**
 * Compaction Consumer for processing raw event files into Parquet
 */
export class CompactionConsumer {
  private storage: StorageBackend
  private mv: WorkerLogsMV
  private rawEventsPrefix: string
  private env: CompactionConsumerEnv
  private maxRetries: number

  constructor(env: CompactionConsumerEnv) {
    this.env = env
    this.rawEventsPrefix = env.RAW_EVENTS_PREFIX || 'raw-events'
    this.maxRetries = env.MAX_RETRIES ? parseInt(env.MAX_RETRIES, 10) : DEFAULT_MAX_RETRIES

    // Create R2 storage backend
    const bucket = toR2Bucket<InternalR2Bucket>(env.LOGS_BUCKET)
    this.storage = new R2Backend(bucket)

    // Create WorkerLogsMV instance (no periodic flushing - we flush after each batch)
    const parquetPrefix = env.PARQUET_PREFIX || 'logs/workers'
    const flushThreshold = env.FLUSH_THRESHOLD ? parseInt(env.FLUSH_THRESHOLD, 10) : 1000
    const compression = env.COMPRESSION || 'lz4'

    this.mv = createWorkerLogsMV({
      storage: this.storage,
      datasetPath: parquetPrefix,
      flushThreshold,
      flushIntervalMs: 0, // Disable periodic flushing
      compression,
    })
  }

  /**
   * Process a batch of queue messages
   *
   * Each message contains an R2 event notification for a raw events file.
   *
   * DLQ Handling:
   * - Messages have an `attempts` property (starts at 1) tracking delivery attempts
   * - If attempts >= maxRetries, the message is NOT retried (will go to DLQ via Cloudflare)
   * - Non-retryable errors (parse errors, malformed data) are acked (not sent to DLQ)
   * - Transient errors trigger retry() if attempts < maxRetries
   */
  async processBatch(messages: Message<R2EventNotification>[]): Promise<BatchResult> {
    const result: BatchResult = {
      totalMessages: messages.length,
      succeeded: 0,
      failed: 0,
      retried: 0,
      deadLettered: 0,
      results: [],
      parquetFilesWritten: [],
    }

    // Process each message
    for (const message of messages) {
      const notification = message.body
      const key = notification.object.key
      const attempts = message.attempts ?? 1

      // Skip non-raw-events files
      if (!isRawEventsFile(key, this.rawEventsPrefix)) {
        logger.info(`[CompactionConsumer] Skipping non-raw-events file: ${key}`)
        message.ack()
        continue
      }

      // Skip delete events
      if (notification.eventType === 'object-delete') {
        logger.info(`[CompactionConsumer] Skipping delete event: ${key}`)
        message.ack()
        continue
      }

      try {
        const processingResult = await this.processFile(key)
        result.results.push(processingResult)

        if (processingResult.success) {
          result.succeeded++
          message.ack()
        } else {
          result.failed++
          // Check if this is a non-retryable error (parse/malformed errors won't improve with retry)
          const isNonRetryable =
            processingResult.error?.includes('parse') ||
            processingResult.error?.includes('malformed') ||
            processingResult.error?.includes('Empty raw events file')

          if (isNonRetryable) {
            // Non-retryable: ack to prevent blocking, log for visibility
            logger.warn(
              `[CompactionConsumer] Non-retryable error for ${key} (attempt ${attempts}): ${processingResult.error}. ` +
                `Message acknowledged (will not go to DLQ).`
            )
            message.ack()
          } else if (attempts >= this.maxRetries) {
            // Max retries reached: don't call retry(), let Cloudflare send to DLQ
            logger.error(
              `[CompactionConsumer] Max retries (${this.maxRetries}) reached for ${key}. ` +
                `Error: ${processingResult.error}. Message will be sent to DLQ.`
            )
            result.deadLettered++
            // Don't ack, don't retry - Cloudflare will send to DLQ
          } else {
            // Transient error with retries remaining: schedule retry
            logger.warn(
              `[CompactionConsumer] Retrying ${key} (attempt ${attempts}/${this.maxRetries}): ${processingResult.error}`
            )
            message.retry()
            result.retried++
          }
        }
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error)
        logger.error(`[CompactionConsumer] Error processing ${key} (attempt ${attempts}):`, errorMessage)

        result.failed++
        result.results.push({
          sourceKey: key,
          eventsProcessed: 0,
          success: false,
          error: errorMessage,
        })

        // Check if max retries reached
        if (attempts >= this.maxRetries) {
          logger.error(
            `[CompactionConsumer] Max retries (${this.maxRetries}) reached for ${key}. ` +
              `Error: ${errorMessage}. Message will be sent to DLQ.`
          )
          result.deadLettered++
          // Don't ack, don't retry - Cloudflare will send to DLQ
        } else {
          // Retry on transient errors
          logger.warn(
            `[CompactionConsumer] Retrying ${key} (attempt ${attempts}/${this.maxRetries}): ${errorMessage}`
          )
          message.retry()
          result.retried++
        }
      }
    }

    // Flush any buffered records to Parquet
    const statsBefore = this.mv.getStats()
    if (statsBefore.bufferSize > 0) {
      await this.mv.flush()
      const statsAfter = this.mv.getStats()

      // Track written files
      if (statsAfter.filesCreated > statsBefore.filesCreated) {
        logger.info(`[CompactionConsumer] Flushed ${statsBefore.bufferSize} records to Parquet`)
      }

      // Send downstream notification if configured
      if (this.env.DOWNSTREAM_QUEUE && statsAfter.filesCreated > statsBefore.filesCreated) {
        await this.sendDownstreamNotification(
          result.results.filter(r => r.success).map(r => r.sourceKey),
          statsBefore.bufferSize,
          statsAfter.bytesWritten - statsBefore.bytesWritten
        )
      }
    }

    return result
  }

  /**
   * Process a single raw events file
   */
  private async processFile(key: string): Promise<ProcessingResult> {
    logger.info(`[CompactionConsumer] Processing ${key}`)

    // Read the raw events file
    const data = await this.storage.read(key)
    const content = new TextDecoder().decode(data)

    // Parse the file
    const rawFile = parseRawEventsFile(content)

    if (rawFile.events.length === 0) {
      return {
        sourceKey: key,
        eventsProcessed: 0,
        success: true,
      }
    }

    // Convert to TailItems and ingest
    const traces: TailItem[] = rawFile.events.map(validatedItemToTailItem)
    const tailEvent: TailEvent = {
      type: 'tail',
      traces,
    }

    await this.mv.ingestTailEvent(tailEvent)

    // Optionally delete the raw file after successful processing
    // await this.storage.delete(key)

    return {
      sourceKey: key,
      eventsProcessed: rawFile.events.length,
      success: true,
    }
  }

  /**
   * Send notification to downstream queue
   */
  private async sendDownstreamNotification(
    sourceFiles: string[],
    recordCount: number,
    sizeBytes: number
  ): Promise<void> {
    if (!this.env.DOWNSTREAM_QUEUE) return

    const parquetPrefix = this.env.PARQUET_PREFIX || 'logs/workers'

    // Generate approximate parquet path (actual path is time-based)
    const now = new Date()
    const year = now.getUTCFullYear()
    const month = String(now.getUTCMonth() + 1).padStart(2, '0')
    const day = String(now.getUTCDate()).padStart(2, '0')
    const hour = String(now.getUTCHours()).padStart(2, '0')
    const parquetPath = `${parquetPrefix}/year=${year}/month=${month}/day=${day}/hour=${hour}/`

    const message: DownstreamMessage = {
      type: 'parquet-written',
      parquetPath,
      recordCount,
      sizeBytes,
      sourceFiles,
      processedAt: Date.now(),
    }

    await this.env.DOWNSTREAM_QUEUE.send(message)
    logger.info(`[CompactionConsumer] Sent downstream notification for ${recordCount} records`)
  }

  /**
   * Get current MV statistics
   */
  getStats() {
    return this.mv.getStats()
  }
}

// =============================================================================
// Queue Consumer Export
// =============================================================================

/**
 * Create a queue consumer handler
 *
 * @example
 * ```typescript
 * export default {
 *   async queue(batch: MessageBatch<R2EventNotification>, env: CompactionConsumerEnv) {
 *     const consumer = new CompactionConsumer(env)
 *     const result = await consumer.processBatch(batch.messages)
 *     console.log(`Processed ${result.succeeded}/${result.totalMessages} messages`)
 *   }
 * }
 * ```
 */
export function createCompactionConsumer(env: CompactionConsumerEnv): CompactionConsumer {
  return new CompactionConsumer(env)
}

/**
 * Default export for use as a Cloudflare Worker with queue consumer
 *
 * @example wrangler.toml:
 * ```toml
 * name = "parquedb-compaction"
 * main = "src/worker/compaction-consumer.ts"
 *
 * [[queues.consumers]]
 * queue = "parquedb-raw-events"
 * max_batch_size = 100
 * max_batch_timeout = 30
 *
 * [[r2_buckets]]
 * binding = "LOGS_BUCKET"
 * bucket_name = "parquedb-logs"
 * ```
 */
export default {
  async queue(
    batch: MessageBatch<R2EventNotification>,
    env: CompactionConsumerEnv,
    _ctx: ExecutionContext
  ): Promise<void> {
    logger.info(`[CompactionConsumer] Processing batch of ${batch.messages.length} messages`)

    const consumer = createCompactionConsumer(env)
    // Copy messages to a mutable array since batch.messages is readonly
    const messages = [...batch.messages]
    const result = await consumer.processBatch(messages)

    // Log summary with DLQ statistics
    const summary = [
      `succeeded=${result.succeeded}`,
      `failed=${result.failed}`,
      `retried=${result.retried}`,
      `deadLettered=${result.deadLettered}`,
    ].join(', ')

    if (result.deadLettered > 0) {
      logger.warn(`[CompactionConsumer] Batch complete with DLQ messages: ${summary}`)
    } else {
      logger.info(`[CompactionConsumer] Batch complete: ${summary}`)
    }
  },
}
