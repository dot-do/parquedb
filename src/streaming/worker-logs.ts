/**
 * Worker Logs Materialized View
 *
 * A materialized view for storing and querying Cloudflare Worker logs.
 * Designed to ingest logs from Tail Workers and store them efficiently
 * in Parquet format for later analysis.
 *
 * Features:
 * - Structured log schema with request/response metadata
 * - Efficient columnar storage with compression
 * - Timestamp-based partitioning for time-range queries
 * - Statistics tracking for monitoring
 *
 * @example
 * ```typescript
 * // Create the MV
 * const workerLogs = createWorkerLogsMV({
 *   storage: myStorage,
 *   datasetPath: 'logs/workers',
 *   flushThreshold: 1000,
 *   flushIntervalMs: 30000,
 * })
 *
 * // Ingest logs from Tail Worker
 * await workerLogs.ingestTailEvent(tailEvent)
 *
 * // Flush to Parquet
 * await workerLogs.flush()
 * ```
 */

import type { StorageBackend } from '../types/storage'
import type { ParquetSchema } from '../parquet/types'

// =============================================================================
// Tail Worker Types (from Cloudflare Workers Runtime)
// =============================================================================

/**
 * Outcome of a traced Worker execution
 */
export type WorkerOutcome =
  | 'unknown'
  | 'ok'
  | 'exception'
  | 'exceededCpu'
  | 'exceededMemory'
  | 'scriptNotFound'
  | 'canceled'
  | 'responseStreamDisconnected'

/**
 * Log level for console output
 */
export type LogLevel = 'debug' | 'info' | 'log' | 'warn' | 'error'

/**
 * A log message from console.log/warn/error etc.
 */
export interface TailLog {
  /** Timestamp when the log was recorded (ms since epoch) */
  timestamp: number
  /** Log level */
  level: LogLevel
  /** Log message arguments (serialized) */
  message: unknown[]
}

/**
 * An exception that occurred during Worker execution
 */
export interface TailException {
  /** Timestamp when the exception occurred (ms since epoch) */
  timestamp: number
  /** Exception name/type */
  name: string
  /** Exception message */
  message: string
}

/**
 * HTTP request information from a traced Worker
 */
export interface TailRequest {
  /** HTTP method */
  method: string
  /** Request URL */
  url: string
  /** Request headers */
  headers: Record<string, string>
  /** Cloudflare-specific request properties */
  cf?: Record<string, unknown>
}

/**
 * HTTP response information from a traced Worker
 */
export interface TailResponse {
  /** HTTP status code */
  status: number
}

/**
 * Fetch event info for HTTP-triggered Workers
 */
export interface FetchEventInfo {
  /** Request details */
  request: TailRequest
  /** Response details */
  response?: TailResponse
}

/**
 * A single trace item from a Tail Worker
 */
export interface TailItem {
  /** Name of the Worker script */
  scriptName: string
  /** Event info (for fetch handlers) */
  event: FetchEventInfo | null
  /** Timestamp when the event started (ms since epoch) */
  eventTimestamp: number
  /** Console logs from the Worker */
  logs: TailLog[]
  /** Exceptions that occurred */
  exceptions: TailException[]
  /** Outcome of the Worker execution */
  outcome: WorkerOutcome
}

/**
 * Tail event passed to the tail() handler
 */
export interface TailEvent {
  /** Event type */
  type: 'tail'
  /** Array of trace items */
  traces: TailItem[]
}

// =============================================================================
// Worker Log Record Types
// =============================================================================

/**
 * Flattened log record for Parquet storage
 *
 * Each log message becomes a separate record for efficient querying.
 */
export interface WorkerLogRecord {
  /** Unique log ID (ULID) */
  id: string
  /** Worker script name */
  scriptName: string
  /** Event timestamp (ms since epoch) */
  eventTimestamp: number
  /** Log timestamp (ms since epoch) */
  timestamp: number
  /** Log level */
  level: LogLevel
  /** Serialized log message (JSON) */
  message: string
  /** HTTP method (if fetch event) */
  httpMethod: string | null
  /** Request URL (if fetch event) */
  httpUrl: string | null
  /** Response status (if fetch event) */
  httpStatus: number | null
  /** Worker outcome */
  outcome: WorkerOutcome
  /** Whether this log is from an exception */
  isException: boolean
  /** Exception name (if isException) */
  exceptionName: string | null
  /** Cloudflare colo (from cf properties) */
  colo: string | null
  /** Client country (from cf properties) */
  country: string | null
  /** Request ID correlation */
  requestId: string | null
}

/**
 * Parquet schema for worker logs
 */
export const WORKER_LOGS_SCHEMA: ParquetSchema = {
  id: { type: 'BYTE_ARRAY', optional: false },
  scriptName: { type: 'BYTE_ARRAY', optional: false },
  eventTimestamp: { type: 'INT64', optional: false },
  timestamp: { type: 'INT64', optional: false },
  level: { type: 'BYTE_ARRAY', optional: false },
  message: { type: 'BYTE_ARRAY', optional: false },
  httpMethod: { type: 'BYTE_ARRAY', optional: true },
  httpUrl: { type: 'BYTE_ARRAY', optional: true },
  httpStatus: { type: 'INT32', optional: true },
  outcome: { type: 'BYTE_ARRAY', optional: false },
  isException: { type: 'BOOLEAN', optional: false },
  exceptionName: { type: 'BYTE_ARRAY', optional: true },
  colo: { type: 'BYTE_ARRAY', optional: true },
  country: { type: 'BYTE_ARRAY', optional: true },
  requestId: { type: 'BYTE_ARRAY', optional: true },
}

// =============================================================================
// Statistics Types
// =============================================================================

/**
 * Statistics for the WorkerLogs MV
 */
export interface WorkerLogsStats {
  /** Total log records ingested */
  recordsIngested: number
  /** Total log records written to Parquet */
  recordsWritten: number
  /** Total Parquet files created */
  filesCreated: number
  /** Total bytes written */
  bytesWritten: number
  /** Records by log level */
  byLevel: Record<LogLevel, number>
  /** Records by outcome */
  byOutcome: Record<WorkerOutcome, number>
  /** Records by script name */
  byScript: Record<string, number>
  /** Number of exceptions recorded */
  exceptions: number
  /** Number of flushes performed */
  flushCount: number
  /** Last flush timestamp */
  lastFlushAt: number | null
  /** Current buffer size */
  bufferSize: number
}

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * Configuration for the WorkerLogs MV
 */
export interface WorkerLogsMVConfig {
  /** Storage backend for writing Parquet files */
  storage: StorageBackend
  /** Base path for log files (e.g., 'logs/workers') */
  datasetPath: string
  /** Number of records to buffer before flushing (default: 1000) */
  flushThreshold?: number
  /** Maximum time to buffer records in ms (default: 30000) */
  flushIntervalMs?: number
  /** Compression codec for Parquet (default: 'lz4') */
  compression?: 'none' | 'snappy' | 'gzip' | 'lz4' | 'zstd'
  /** Target row group size (default: 5000) */
  rowGroupSize?: number
  /** ID generator function (default: uses ULID) */
  generateId?: () => string
}

/**
 * Default configuration values
 */
const DEFAULT_CONFIG = {
  flushThreshold: 1000,
  flushIntervalMs: 30000,
  compression: 'lz4' as const,
  rowGroupSize: 5000,
}

// =============================================================================
// ULID Generator (simplified)
// =============================================================================

/**
 * Generate a ULID-like ID for log records
 *
 * Format: timestamp (10 chars) + random (16 chars)
 * This provides time-ordered unique IDs.
 */
function generateULID(): string {
  const timestamp = Date.now()
  const timeStr = timestamp.toString(36).padStart(10, '0')
  const random = Array.from({ length: 16 }, () =>
    Math.floor(Math.random() * 36).toString(36)
  ).join('')
  return timeStr + random
}

// =============================================================================
// WorkerLogsMV Class
// =============================================================================

/**
 * Materialized View for Worker Logs
 *
 * Buffers log records in memory and periodically flushes them to Parquet
 * files for efficient storage and querying.
 */
export class WorkerLogsMV {
  private storage: StorageBackend
  private datasetPath: string
  private flushThreshold: number
  private flushIntervalMs: number
  private compression: 'none' | 'snappy' | 'gzip' | 'lz4' | 'zstd'
  private rowGroupSize: number
  private generateId: () => string

  private buffer: WorkerLogRecord[] = []
  private flushTimer: ReturnType<typeof setTimeout> | null = null
  private flushPromise: Promise<void> | null = null
  private running = false

  private stats: WorkerLogsStats = this.createEmptyStats()

  constructor(config: WorkerLogsMVConfig) {
    this.storage = config.storage
    this.datasetPath = config.datasetPath.replace(/\/$/, '') // Remove trailing slash
    this.flushThreshold = config.flushThreshold ?? DEFAULT_CONFIG.flushThreshold
    this.flushIntervalMs = config.flushIntervalMs ?? DEFAULT_CONFIG.flushIntervalMs
    this.compression = config.compression ?? DEFAULT_CONFIG.compression
    this.rowGroupSize = config.rowGroupSize ?? DEFAULT_CONFIG.rowGroupSize
    this.generateId = config.generateId ?? generateULID
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Start the MV (enables periodic flushing)
   */
  start(): void {
    if (this.running) return

    this.running = true
    this.startFlushTimer()
  }

  /**
   * Stop the MV and flush remaining records
   */
  async stop(): Promise<void> {
    if (!this.running) return

    this.running = false
    this.stopFlushTimer()

    // Flush remaining records
    await this.flush()
  }

  /**
   * Check if the MV is running
   */
  isRunning(): boolean {
    return this.running
  }

  /**
   * Ingest a Tail Worker event
   *
   * Converts TailItems to WorkerLogRecords and buffers them.
   *
   * @param event - Tail event from Cloudflare
   */
  async ingestTailEvent(event: TailEvent): Promise<void> {
    for (const trace of event.traces) {
      await this.ingestTailItem(trace)
    }

    // Check if we should flush
    await this.maybeFlush()
  }

  /**
   * Ingest a single TailItem
   *
   * @param item - Single trace item from a Tail event
   */
  async ingestTailItem(item: TailItem): Promise<void> {
    const records = this.tailItemToRecords(item)
    this.buffer.push(...records)
    this.stats.bufferSize = this.buffer.length

    // Update stats
    for (const record of records) {
      this.stats.recordsIngested++
      this.stats.byLevel[record.level] = (this.stats.byLevel[record.level] || 0) + 1
      this.stats.byOutcome[record.outcome] = (this.stats.byOutcome[record.outcome] || 0) + 1
      this.stats.byScript[record.scriptName] = (this.stats.byScript[record.scriptName] || 0) + 1
      if (record.isException) {
        this.stats.exceptions++
      }
    }

    // Check if we should flush
    await this.maybeFlush()
  }

  /**
   * Ingest raw log records directly
   *
   * @param records - Array of WorkerLogRecord to ingest
   */
  async ingestRecords(records: WorkerLogRecord[]): Promise<void> {
    this.buffer.push(...records)
    this.stats.bufferSize = this.buffer.length

    // Update stats
    for (const record of records) {
      this.stats.recordsIngested++
      this.stats.byLevel[record.level] = (this.stats.byLevel[record.level] || 0) + 1
      this.stats.byOutcome[record.outcome] = (this.stats.byOutcome[record.outcome] || 0) + 1
      this.stats.byScript[record.scriptName] = (this.stats.byScript[record.scriptName] || 0) + 1
      if (record.isException) {
        this.stats.exceptions++
      }
    }

    // Check if we should flush
    await this.maybeFlush()
  }

  /**
   * Flush buffered records to Parquet
   */
  async flush(): Promise<void> {
    // Wait for any in-flight flush
    if (this.flushPromise) {
      await this.flushPromise
    }

    if (this.buffer.length === 0) {
      return
    }

    this.flushPromise = this.doFlush()
    try {
      await this.flushPromise
    } finally {
      this.flushPromise = null
    }
  }

  /**
   * Get current statistics
   */
  getStats(): WorkerLogsStats {
    return { ...this.stats }
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.stats = this.createEmptyStats()
    this.stats.bufferSize = this.buffer.length
  }

  /**
   * Get current buffer contents (for testing/debugging)
   */
  getBuffer(): WorkerLogRecord[] {
    return [...this.buffer]
  }

  // ===========================================================================
  // Internal Methods
  // ===========================================================================

  /**
   * Create empty stats object
   */
  private createEmptyStats(): WorkerLogsStats {
    return {
      recordsIngested: 0,
      recordsWritten: 0,
      filesCreated: 0,
      bytesWritten: 0,
      byLevel: {
        debug: 0,
        info: 0,
        log: 0,
        warn: 0,
        error: 0,
      },
      byOutcome: {
        unknown: 0,
        ok: 0,
        exception: 0,
        exceededCpu: 0,
        exceededMemory: 0,
        scriptNotFound: 0,
        canceled: 0,
        responseStreamDisconnected: 0,
      },
      byScript: {},
      exceptions: 0,
      flushCount: 0,
      lastFlushAt: null,
      bufferSize: 0,
    }
  }

  /**
   * Convert a TailItem to WorkerLogRecords
   */
  private tailItemToRecords(item: TailItem): WorkerLogRecord[] {
    const records: WorkerLogRecord[] = []

    // Extract common fields
    const httpMethod = item.event?.request?.method ?? null
    const httpUrl = item.event?.request?.url ?? null
    const httpStatus = item.event?.response?.status ?? null
    const colo = item.event?.request?.cf?.colo as string ?? null
    const country = item.event?.request?.cf?.country as string ?? null
    const requestId = item.event?.request?.headers?.['cf-ray'] ?? null

    // Convert console logs
    for (const log of item.logs) {
      records.push({
        id: this.generateId(),
        scriptName: item.scriptName,
        eventTimestamp: item.eventTimestamp,
        timestamp: log.timestamp,
        level: log.level,
        message: this.serializeMessage(log.message),
        httpMethod,
        httpUrl,
        httpStatus,
        outcome: item.outcome,
        isException: false,
        exceptionName: null,
        colo,
        country,
        requestId,
      })
    }

    // Convert exceptions
    for (const exc of item.exceptions) {
      records.push({
        id: this.generateId(),
        scriptName: item.scriptName,
        eventTimestamp: item.eventTimestamp,
        timestamp: exc.timestamp,
        level: 'error',
        message: `${exc.name}: ${exc.message}`,
        httpMethod,
        httpUrl,
        httpStatus,
        outcome: item.outcome,
        isException: true,
        exceptionName: exc.name,
        colo,
        country,
        requestId,
      })
    }

    // If no logs or exceptions, create a synthetic record for the event
    if (records.length === 0) {
      records.push({
        id: this.generateId(),
        scriptName: item.scriptName,
        eventTimestamp: item.eventTimestamp,
        timestamp: item.eventTimestamp,
        level: item.outcome === 'ok' ? 'info' : 'error',
        message: `Worker execution: ${item.outcome}`,
        httpMethod,
        httpUrl,
        httpStatus,
        outcome: item.outcome,
        isException: false,
        exceptionName: null,
        colo,
        country,
        requestId,
      })
    }

    return records
  }

  /**
   * Serialize log message to JSON string
   */
  private serializeMessage(message: unknown[]): string {
    try {
      if (message.length === 1) {
        const val = message[0]
        if (typeof val === 'string') {
          return val
        }
        return JSON.stringify(val)
      }
      return JSON.stringify(message)
    } catch {
      return String(message)
    }
  }

  /**
   * Check if we should flush based on threshold
   */
  private async maybeFlush(): Promise<void> {
    if (this.buffer.length >= this.flushThreshold) {
      await this.flush()
    }
  }

  /**
   * Start the periodic flush timer
   */
  private startFlushTimer(): void {
    if (this.flushTimer) return

    this.flushTimer = setInterval(() => {
      if (this.buffer.length > 0 && !this.flushPromise) {
        this.flush().catch((err) => {
          console.error('[WorkerLogsMV] Periodic flush failed:', err)
        })
      }
    }, this.flushIntervalMs)
  }

  /**
   * Stop the periodic flush timer
   */
  private stopFlushTimer(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer)
      this.flushTimer = null
    }
  }

  /**
   * Perform the actual flush to Parquet
   */
  private async doFlush(): Promise<void> {
    if (this.buffer.length === 0) return

    // Snapshot and clear buffer
    const records = this.buffer
    this.buffer = []
    this.stats.bufferSize = 0

    // Generate file path with timestamp partitioning
    const now = new Date()
    const year = now.getUTCFullYear()
    const month = String(now.getUTCMonth() + 1).padStart(2, '0')
    const day = String(now.getUTCDate()).padStart(2, '0')
    const hour = String(now.getUTCHours()).padStart(2, '0')
    const timestamp = Date.now()

    const filePath = `${this.datasetPath}/year=${year}/month=${month}/day=${day}/hour=${hour}/logs-${timestamp}.parquet`

    try {
      // Convert records to columnar format
      const columns = this.recordsToColumns(records)

      // Write to Parquet using storage backend
      const buffer = await this.buildParquetBuffer(columns)
      const result = await this.storage.writeAtomic(filePath, buffer, {
        contentType: 'application/vnd.apache.parquet',
      })

      // Update stats
      this.stats.recordsWritten += records.length
      this.stats.filesCreated++
      this.stats.bytesWritten += result.size
      this.stats.flushCount++
      this.stats.lastFlushAt = Date.now()
    } catch (error) {
      // Put records back in buffer on failure
      this.buffer = records.concat(this.buffer)
      this.stats.bufferSize = this.buffer.length
      throw error
    }
  }

  /**
   * Convert records to columnar format
   */
  private recordsToColumns(records: WorkerLogRecord[]): Record<string, unknown[]> {
    const columns: Record<string, unknown[]> = {}

    // Initialize columns
    for (const colName of Object.keys(WORKER_LOGS_SCHEMA)) {
      columns[colName] = []
    }

    // Fill columns
    for (const record of records) {
      columns['id']!.push(record.id)
      columns['scriptName']!.push(record.scriptName)
      columns['eventTimestamp']!.push(record.eventTimestamp)
      columns['timestamp']!.push(record.timestamp)
      columns['level']!.push(record.level)
      columns['message']!.push(record.message)
      columns['httpMethod']!.push(record.httpMethod)
      columns['httpUrl']!.push(record.httpUrl)
      columns['httpStatus']!.push(record.httpStatus)
      columns['outcome']!.push(record.outcome)
      columns['isException']!.push(record.isException)
      columns['exceptionName']!.push(record.exceptionName)
      columns['colo']!.push(record.colo)
      columns['country']!.push(record.country)
      columns['requestId']!.push(record.requestId)
    }

    return columns
  }

  /**
   * Build Parquet buffer from columnar data
   */
  private async buildParquetBuffer(columns: Record<string, unknown[]>): Promise<Uint8Array> {
    try {
      // Try to use hyparquet-writer
      const { parquetWriteBuffer } = await import('hyparquet-writer')
      const { writeCompressors } = await import('../parquet/compression')

      const columnData = Object.entries(columns).map(([name, data]) => ({
        name,
        data,
        columnIndex: true,
        offsetIndex: true,
      }))

      const writeOptions: Record<string, unknown> = {
        columnData,
        statistics: true,
        rowGroupSize: this.rowGroupSize,
      }

      // Set compression
      if (this.compression !== 'none') {
        const codecMap: Record<string, string> = {
          snappy: 'SNAPPY',
          gzip: 'GZIP',
          lz4: 'LZ4',
          zstd: 'ZSTD',
        }
        writeOptions.codec = codecMap[this.compression]
        writeOptions.compressors = writeCompressors
      }

      const result = parquetWriteBuffer(writeOptions as Parameters<typeof parquetWriteBuffer>[0])
      return new Uint8Array(result)
    } catch {
      // Fallback to JSON-based format
      return this.buildFallbackBuffer(columns)
    }
  }

  /**
   * Build fallback buffer when hyparquet-writer is not available
   */
  private buildFallbackBuffer(columns: Record<string, unknown[]>): Uint8Array {
    const data = {
      schema: WORKER_LOGS_SCHEMA,
      columns,
      metadata: {
        compression: this.compression,
        rowGroupSize: this.rowGroupSize,
      },
    }

    const jsonStr = JSON.stringify(data)
    const encoder = new TextEncoder()
    const jsonBytes = encoder.encode(jsonStr)

    // Wrap with Parquet magic bytes
    const MAGIC = new Uint8Array([0x50, 0x41, 0x52, 0x31]) // 'PAR1'
    const buffer = new Uint8Array(MAGIC.length * 2 + jsonBytes.length)
    buffer.set(MAGIC, 0)
    buffer.set(jsonBytes, MAGIC.length)
    buffer.set(MAGIC, MAGIC.length + jsonBytes.length)

    return buffer
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a new WorkerLogsMV instance
 */
export function createWorkerLogsMV(config: WorkerLogsMVConfig): WorkerLogsMV {
  return new WorkerLogsMV(config)
}

/**
 * Create a WorkerLogsMV handler for use with StreamingRefreshEngine
 *
 * This adapter allows the WorkerLogsMV to be registered as an MV handler
 * that processes CDC events from ParqueDB.
 *
 * @param mv - The WorkerLogsMV instance
 * @param sourceNamespace - The namespace to watch for log events
 */
export function createWorkerLogsMVHandler(
  mv: WorkerLogsMV,
  sourceNamespace = 'worker_logs'
) {
  return {
    name: 'WorkerLogs',
    sourceNamespaces: [sourceNamespace],
    async process(events: import('../types/entity').Event[]): Promise<void> {
      // Convert CDC events to WorkerLogRecords
      const records: WorkerLogRecord[] = []

      for (const event of events) {
        if (event.op === 'CREATE' && event.after) {
          // Assume event.after contains a WorkerLogRecord-like structure
          const data = event.after as Partial<WorkerLogRecord>
          if (data.scriptName && data.timestamp && data.level) {
            records.push({
              id: data.id ?? generateULID(),
              scriptName: data.scriptName,
              eventTimestamp: data.eventTimestamp ?? data.timestamp,
              timestamp: data.timestamp,
              level: data.level,
              message: data.message ?? '',
              httpMethod: data.httpMethod ?? null,
              httpUrl: data.httpUrl ?? null,
              httpStatus: data.httpStatus ?? null,
              outcome: data.outcome ?? 'unknown',
              isException: data.isException ?? false,
              exceptionName: data.exceptionName ?? null,
              colo: data.colo ?? null,
              country: data.country ?? null,
              requestId: data.requestId ?? null,
            })
          }
        }
      }

      if (records.length > 0) {
        await mv.ingestRecords(records)
      }
    },
  }
}
