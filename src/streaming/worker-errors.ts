/**
 * Worker Errors Materialized View
 *
 * A streaming MV that extracts, categorizes, and indexes errors from
 * worker log events. Provides real-time error aggregation and analytics.
 *
 * Features:
 * - Error categorization (network, timeout, validation, etc.)
 * - Severity classification (critical, error, warning, info)
 * - Indexing by error type, worker, path, and time
 * - Rolling statistics computation
 * - Parquet persistence with buffered writes
 *
 * @example
 * ```typescript
 * // Create the MV with Parquet persistence
 * const workerErrors = createWorkerErrorsMV({
 *   storage: myStorage,
 *   datasetPath: 'errors/workers',
 *   flushThreshold: 500,
 *   flushIntervalMs: 30000,
 * })
 *
 * // Start the MV (enables periodic flushing)
 * workerErrors.start()
 *
 * // Process events
 * await workerErrors.process(events)
 *
 * // Stop and flush remaining errors
 * await workerErrors.stop()
 * ```
 */

import type { Event } from '../types/entity'
import type { StorageBackend } from '../types/storage'
import type { ParquetSchema } from '../parquet/types'
import { logger } from '../utils/logger'
import {
  type MVHandler,
  type WorkerError,
  type WorkerErrorStats,
  type ErrorCategory,
  type ErrorSeverity,
  type ErrorPattern,
  classifyError,
  severityFromStatus,
  categoryFromStatus,
  DEFAULT_ERROR_PATTERNS,
} from './types'

// =============================================================================
// Parquet Schema for Worker Errors
// =============================================================================

/**
 * Parquet schema for worker errors
 */
export const WORKER_ERRORS_SCHEMA: ParquetSchema = {
  id: { type: 'BYTE_ARRAY', optional: false },
  ts: { type: 'INT64', optional: false },
  category: { type: 'BYTE_ARRAY', optional: false },
  severity: { type: 'BYTE_ARRAY', optional: false },
  code: { type: 'BYTE_ARRAY', optional: false },
  message: { type: 'BYTE_ARRAY', optional: false },
  stack: { type: 'BYTE_ARRAY', optional: true },
  path: { type: 'BYTE_ARRAY', optional: true },
  method: { type: 'BYTE_ARRAY', optional: true },
  requestId: { type: 'BYTE_ARRAY', optional: true },
  workerName: { type: 'BYTE_ARRAY', optional: true },
  colo: { type: 'BYTE_ARRAY', optional: true },
  metadata: { type: 'BYTE_ARRAY', optional: true }, // JSON-encoded
}

// =============================================================================
// Statistics Types
// =============================================================================

/**
 * Extended statistics for the WorkerErrors MV with persistence info
 */
export interface WorkerErrorsStatsExtended extends WorkerErrorStats {
  /** Total error records written to Parquet */
  recordsWritten: number
  /** Total Parquet files created */
  filesCreated: number
  /** Total bytes written to Parquet */
  bytesWritten: number
  /** Number of flushes performed */
  flushCount: number
  /** Last flush timestamp */
  lastFlushAt: number | null
  /** Current buffer size */
  bufferSize: number
}

// =============================================================================
// Worker Errors MV Configuration
// =============================================================================

/**
 * Configuration for the WorkerErrors MV
 */
export interface WorkerErrorsConfig {
  /** Custom error patterns for classification (merged with defaults) */
  errorPatterns?: ErrorPattern[]
  /** Maximum errors to retain in memory (default: 10000) */
  maxErrors?: number
  /** Window size for rolling stats in ms (default: 60000 = 1 minute) */
  statsWindowMs?: number
  /** Namespaces to listen for error events (default: ['logs', 'errors', 'workers']) */
  sourceNamespaces?: string[]
  /** Storage backend for writing Parquet files (optional - enables persistence) */
  storage?: StorageBackend
  /** Base path for error files (e.g., 'errors/workers') - required if storage is set */
  datasetPath?: string
  /** Number of records to buffer before flushing (default: 500) */
  flushThreshold?: number
  /** Maximum time to buffer records in ms (default: 30000) */
  flushIntervalMs?: number
  /** Compression codec for Parquet (default: 'lz4') */
  compression?: 'none' | 'snappy' | 'gzip' | 'lz4' | 'zstd'
  /** Target row group size (default: 5000) */
  rowGroupSize?: number
}

const DEFAULT_CONFIG = {
  errorPatterns: [] as ErrorPattern[],
  maxErrors: 10000,
  statsWindowMs: 60000,
  sourceNamespaces: ['logs', 'errors', 'workers'],
  flushThreshold: 500,
  flushIntervalMs: 30000,
  compression: 'lz4' as const,
  rowGroupSize: 5000,
}

// =============================================================================
// Worker Errors MV Implementation
// =============================================================================

/**
 * Materialized View for Worker Errors
 *
 * Processes log/error events and maintains:
 * - Categorized error list
 * - Index by error type
 * - Rolling statistics
 * - Parquet persistence (when storage is configured)
 *
 * Buffers error records in memory and periodically flushes them to Parquet
 * files for efficient storage and querying.
 */
export class WorkerErrorsMV implements MVHandler {
  readonly name = 'WorkerErrors'
  readonly sourceNamespaces: string[]

  private readonly config: WorkerErrorsConfig
  private readonly errorPatterns: ErrorPattern[]

  // Parquet persistence
  private readonly storage?: StorageBackend
  private readonly datasetPath?: string
  private readonly flushThreshold: number
  private readonly flushIntervalMs: number
  private readonly compression: 'none' | 'snappy' | 'gzip' | 'lz4' | 'zstd'
  private readonly rowGroupSize: number

  // Buffer for Parquet writes
  private buffer: WorkerError[] = []
  private flushTimer: ReturnType<typeof setTimeout> | null = null
  private flushPromise: Promise<void> | null = null
  private running = false

  // In-memory error storage (for querying)
  private errors: WorkerError[] = []

  // Indexes for fast lookup
  private byCategory: Map<ErrorCategory, Set<string>> = new Map()
  private bySeverity: Map<ErrorSeverity, Set<string>> = new Map()
  private byCode: Map<string, Set<string>> = new Map()
  private byWorker: Map<string, Set<string>> = new Map()
  private byPath: Map<string, Set<string>> = new Map()
  private byId: Map<string, WorkerError> = new Map()

  // Extended stats for persistence tracking
  private persistenceStats = {
    recordsWritten: 0,
    filesCreated: 0,
    bytesWritten: 0,
    flushCount: 0,
    lastFlushAt: null as number | null,
    bufferSize: 0,
  }

  constructor(config: WorkerErrorsConfig = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config }
    this.sourceNamespaces = this.config.sourceNamespaces ?? DEFAULT_CONFIG.sourceNamespaces
    this.errorPatterns = [
      ...DEFAULT_ERROR_PATTERNS,
      ...(this.config.errorPatterns ?? []),
    ]

    // Parquet persistence configuration
    this.storage = config.storage
    this.datasetPath = config.datasetPath?.replace(/\/$/, '') // Remove trailing slash
    this.flushThreshold = config.flushThreshold ?? DEFAULT_CONFIG.flushThreshold
    this.flushIntervalMs = config.flushIntervalMs ?? DEFAULT_CONFIG.flushIntervalMs
    this.compression = config.compression ?? DEFAULT_CONFIG.compression
    this.rowGroupSize = config.rowGroupSize ?? DEFAULT_CONFIG.rowGroupSize

    // Initialize category and severity maps
    const categories: ErrorCategory[] = [
      'network', 'timeout', 'validation', 'authentication',
      'storage', 'query', 'internal', 'rate_limit', 'unknown',
    ]
    const severities: ErrorSeverity[] = ['critical', 'error', 'warning', 'info']

    for (const cat of categories) {
      this.byCategory.set(cat, new Set())
    }
    for (const sev of severities) {
      this.bySeverity.set(sev, new Set())
    }
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  /**
   * Start the MV (enables periodic flushing if storage is configured)
   */
  start(): void {
    if (this.running) return

    this.running = true
    if (this.storage && this.datasetPath) {
      this.startFlushTimer()
    }
  }

  /**
   * Stop the MV and flush remaining records
   */
  async stop(): Promise<void> {
    if (!this.running) return

    this.running = false
    this.stopFlushTimer()

    // Flush remaining records
    if (this.storage && this.datasetPath) {
      await this.flush()
    }
  }

  /**
   * Check if the MV is running
   */
  isRunning(): boolean {
    return this.running
  }

  /**
   * Check if Parquet persistence is enabled
   */
  isPersistenceEnabled(): boolean {
    return !!(this.storage && this.datasetPath)
  }

  // ===========================================================================
  // MVHandler Implementation
  // ===========================================================================

  /**
   * Process a batch of events
   */
  async process(events: Event[]): Promise<void> {
    for (const event of events) {
      // Only process error-like events
      if (!this.isErrorEvent(event)) continue

      const workerError = this.extractError(event)
      if (workerError) {
        this.addError(workerError)
      }
    }

    // Check if we should flush to Parquet
    if (this.storage && this.datasetPath) {
      await this.maybeFlush()
    }
  }

  // ===========================================================================
  // Query Methods
  // ===========================================================================

  /**
   * Get all errors (most recent first)
   */
  getErrors(limit?: number): WorkerError[] {
    const result = [...this.errors].reverse()
    return limit ? result.slice(0, limit) : result
  }

  /**
   * Get errors by category
   */
  getErrorsByCategory(category: ErrorCategory, limit?: number): WorkerError[] {
    const ids = this.byCategory.get(category) ?? new Set()
    return this.getErrorsById(ids, limit)
  }

  /**
   * Get errors by severity
   */
  getErrorsBySeverity(severity: ErrorSeverity, limit?: number): WorkerError[] {
    const ids = this.bySeverity.get(severity) ?? new Set()
    return this.getErrorsById(ids, limit)
  }

  /**
   * Get errors by error code
   */
  getErrorsByCode(code: string, limit?: number): WorkerError[] {
    const ids = this.byCode.get(code) ?? new Set()
    return this.getErrorsById(ids, limit)
  }

  /**
   * Get errors by worker name
   */
  getErrorsByWorker(workerName: string, limit?: number): WorkerError[] {
    const ids = this.byWorker.get(workerName) ?? new Set()
    return this.getErrorsById(ids, limit)
  }

  /**
   * Get errors by request path
   */
  getErrorsByPath(path: string, limit?: number): WorkerError[] {
    const ids = this.byPath.get(path) ?? new Set()
    return this.getErrorsById(ids, limit)
  }

  /**
   * Get errors within a time range
   */
  getErrorsInRange(startTs: number, endTs: number): WorkerError[] {
    return this.errors.filter(e => e.ts >= startTs && e.ts <= endTs)
  }

  /**
   * Get error by ID
   */
  getError(id: string): WorkerError | undefined {
    return this.byId.get(id)
  }

  /**
   * Get aggregated statistics
   */
  getStats(): WorkerErrorStats {
    const now = Date.now()
    const windowStart = now - (this.config.statsWindowMs ?? DEFAULT_CONFIG.statsWindowMs)

    // Count by category
    const byCategory: Record<ErrorCategory, number> = {
      network: 0,
      timeout: 0,
      validation: 0,
      authentication: 0,
      storage: 0,
      query: 0,
      internal: 0,
      rate_limit: 0,
      unknown: 0,
    }
    for (const [category, ids] of this.byCategory) {
      byCategory[category] = ids.size
    }

    // Count by severity
    const bySeverity: Record<ErrorSeverity, number> = {
      critical: 0,
      error: 0,
      warning: 0,
      info: 0,
    }
    for (const [severity, ids] of this.bySeverity) {
      bySeverity[severity] = ids.size
    }

    // Count by code
    const byCode: Record<string, number> = {}
    for (const [code, ids] of this.byCode) {
      byCode[code] = ids.size
    }

    // Count by worker
    const byWorker: Record<string, number> = {}
    for (const [worker, ids] of this.byWorker) {
      byWorker[worker] = ids.size
    }

    // Calculate error rate (errors in the window)
    const errorsInWindow = this.errors.filter(e => e.ts >= windowStart).length
    const statsWindowMs = this.config.statsWindowMs ?? DEFAULT_CONFIG.statsWindowMs
    const windowMinutes = statsWindowMs / 60000
    const errorRatePerMinute = errorsInWindow / windowMinutes

    // Time range
    const timeRange = {
      start: this.errors.length > 0 ? this.errors[0]!.ts : now,
      end: this.errors.length > 0 ? this.errors[this.errors.length - 1]!.ts : now,
    }

    return {
      totalErrors: this.errors.length,
      byCategory,
      bySeverity,
      byCode,
      byWorker,
      errorRatePerMinute,
      timeRange,
    }
  }

  /**
   * Get extended statistics including persistence info
   */
  getExtendedStats(): WorkerErrorsStatsExtended {
    return {
      ...this.getStats(),
      ...this.persistenceStats,
      bufferSize: this.buffer.length,
    }
  }

  /**
   * Flush buffered errors to Parquet
   *
   * @throws Error if storage is not configured
   */
  async flush(): Promise<void> {
    if (!this.storage || !this.datasetPath) {
      return // No-op if persistence is not enabled
    }

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
   * Get current buffer contents (for testing/debugging)
   */
  getBuffer(): WorkerError[] {
    return [...this.buffer]
  }

  /**
   * Get error count
   */
  count(): number {
    return this.errors.length
  }

  /**
   * Clear all errors (in-memory only, does not affect persisted Parquet files)
   */
  clear(): void {
    this.errors = []
    this.buffer = []
    this.byId.clear()
    for (const set of this.byCategory.values()) set.clear()
    for (const set of this.bySeverity.values()) set.clear()
    this.byCode.clear()
    this.byWorker.clear()
    this.byPath.clear()
    this.persistenceStats.bufferSize = 0
  }

  /**
   * Reset statistics
   */
  resetStats(): void {
    this.persistenceStats = {
      recordsWritten: 0,
      filesCreated: 0,
      bytesWritten: 0,
      flushCount: 0,
      lastFlushAt: null,
      bufferSize: this.buffer.length,
    }
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  /**
   * Check if an event represents an error
   */
  private isErrorEvent(event: Event): boolean {
    // Look for error indicators in the event
    if (event.after) {
      const after = event.after as Record<string, unknown>
      // Check for error-like fields
      if (after.error || after.errorCode || after.errorMessage) return true
      if (after.status && typeof after.status === 'number' && after.status >= 400) return true
      if (after.level === 'error' || after.level === 'fatal') return true
      if (after.exception || after.stack) return true
    }
    return false
  }

  /**
   * Extract a WorkerError from an event
   */
  private extractError(event: Event): WorkerError | null {
    const after = event.after as Record<string, unknown> | undefined
    if (!after) return null

    // Extract error message
    const message = this.extractMessage(after)
    if (!message) return null

    // Extract error code
    const code = this.extractCode(after)

    // Classify the error
    const classification = classifyError(message, code, this.errorPatterns)

    // Override from HTTP status if available
    const status = after.status as number | undefined
    if (status && status >= 400) {
      classification.severity = severityFromStatus(status)
      classification.category = categoryFromStatus(status)
    }

    return {
      id: event.id,
      ts: event.ts,
      category: classification.category,
      severity: classification.severity,
      code: classification.code,
      message,
      stack: after.stack as string | undefined,
      path: after.path as string | undefined,
      method: after.method as string | undefined,
      requestId: (after.requestId ?? (after.cf as Record<string, unknown> | undefined)?.rayId) as string | undefined,
      workerName: after.workerName as string | undefined,
      colo: (after.colo ?? (after.cf as Record<string, unknown> | undefined)?.colo) as string | undefined,
      metadata: this.extractMetadata(after),
    }
  }

  /**
   * Extract error message from event data
   */
  private extractMessage(data: Record<string, unknown>): string | null {
    // Try various common error message fields
    if (typeof data.errorMessage === 'string') return data.errorMessage
    if (typeof data.message === 'string') return data.message
    if (data.error && typeof data.error === 'object') {
      const err = data.error as Record<string, unknown>
      if (typeof err.message === 'string') return err.message
    }
    if (typeof data.error === 'string') return data.error
    if (data.exception && typeof data.exception === 'object') {
      const ex = data.exception as Record<string, unknown>
      if (typeof ex.message === 'string') return ex.message
    }
    return null
  }

  /**
   * Extract error code from event data
   */
  private extractCode(data: Record<string, unknown>): string | undefined {
    if (typeof data.errorCode === 'string') return data.errorCode
    if (typeof data.code === 'string') return data.code
    if (data.error && typeof data.error === 'object') {
      const err = data.error as Record<string, unknown>
      if (typeof err.code === 'string') return err.code
    }
    if (typeof data.status === 'number') return `HTTP_${data.status}`
    return undefined
  }

  /**
   * Extract additional metadata from event data
   */
  private extractMetadata(data: Record<string, unknown>): Record<string, unknown> | undefined {
    const metadata: Record<string, unknown> = {}

    // Extract Cloudflare-specific fields
    if (data.cf && typeof data.cf === 'object') {
      metadata.cf = data.cf
    }

    // Extract timing information
    if (typeof data.duration === 'number') metadata.duration = data.duration
    if (typeof data.cpuTime === 'number') metadata.cpuTime = data.cpuTime

    // Extract user agent
    if (typeof data.userAgent === 'string') metadata.userAgent = data.userAgent

    // Extract query/filter that caused the error
    if (data.query) metadata.query = data.query
    if (data.filter) metadata.filter = data.filter

    return Object.keys(metadata).length > 0 ? metadata : undefined
  }

  /**
   * Add an error to storage and indexes
   */
  private addError(error: WorkerError): void {
    // Check for duplicate
    if (this.byId.has(error.id)) return

    // Enforce max size for in-memory storage
    const maxErrors = this.config.maxErrors ?? DEFAULT_CONFIG.maxErrors
    while (this.errors.length >= maxErrors) {
      const oldest = this.errors.shift()
      if (oldest) {
        this.removeFromIndexes(oldest)
      }
    }

    // Add to in-memory storage
    this.errors.push(error)
    this.byId.set(error.id, error)

    // Add to Parquet buffer if persistence is enabled
    if (this.storage && this.datasetPath) {
      this.buffer.push(error)
      this.persistenceStats.bufferSize = this.buffer.length
    }

    // Add to indexes
    this.byCategory.get(error.category)?.add(error.id)
    this.bySeverity.get(error.severity)?.add(error.id)

    if (!this.byCode.has(error.code)) {
      this.byCode.set(error.code, new Set())
    }
    this.byCode.get(error.code)!.add(error.id)

    if (error.workerName) {
      if (!this.byWorker.has(error.workerName)) {
        this.byWorker.set(error.workerName, new Set())
      }
      this.byWorker.get(error.workerName)!.add(error.id)
    }

    if (error.path) {
      if (!this.byPath.has(error.path)) {
        this.byPath.set(error.path, new Set())
      }
      this.byPath.get(error.path)!.add(error.id)
    }
  }

  /**
   * Remove an error from indexes
   */
  private removeFromIndexes(error: WorkerError): void {
    this.byId.delete(error.id)
    this.byCategory.get(error.category)?.delete(error.id)
    this.bySeverity.get(error.severity)?.delete(error.id)
    this.byCode.get(error.code)?.delete(error.id)
    if (error.workerName) {
      this.byWorker.get(error.workerName)?.delete(error.id)
    }
    if (error.path) {
      this.byPath.get(error.path)?.delete(error.id)
    }
  }

  /**
   * Get errors by a set of IDs
   */
  private getErrorsById(ids: Set<string>, limit?: number): WorkerError[] {
    const result: WorkerError[] = []
    // Iterate in reverse for most recent first
    for (let i = this.errors.length - 1; i >= 0 && (!limit || result.length < limit); i--) {
      const error = this.errors[i]!
      if (ids.has(error.id)) {
        result.push(error)
      }
    }
    return result
  }

  // ===========================================================================
  // Parquet Persistence Methods
  // ===========================================================================

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
          logger.error('[WorkerErrorsMV] Periodic flush failed:', err)
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
    if (!this.storage || !this.datasetPath) return
    if (this.buffer.length === 0) return

    // Snapshot and clear buffer
    const errors = this.buffer
    this.buffer = []
    this.persistenceStats.bufferSize = 0

    // Generate file path with timestamp partitioning
    const now = new Date()
    const year = now.getUTCFullYear()
    const month = String(now.getUTCMonth() + 1).padStart(2, '0')
    const day = String(now.getUTCDate()).padStart(2, '0')
    const hour = String(now.getUTCHours()).padStart(2, '0')
    const timestamp = Date.now()

    const filePath = `${this.datasetPath}/year=${year}/month=${month}/day=${day}/hour=${hour}/errors-${timestamp}.parquet`

    try {
      // Convert errors to columnar format
      const columns = this.errorsToColumns(errors)

      // Write to Parquet using storage backend
      const buffer = await this.buildParquetBuffer(columns)
      const result = await this.storage.writeAtomic(filePath, buffer, {
        contentType: 'application/vnd.apache.parquet',
      })

      // Update stats
      this.persistenceStats.recordsWritten += errors.length
      this.persistenceStats.filesCreated++
      this.persistenceStats.bytesWritten += result.size
      this.persistenceStats.flushCount++
      this.persistenceStats.lastFlushAt = Date.now()
    } catch (error) {
      // Put errors back in buffer on failure
      this.buffer = errors.concat(this.buffer)
      this.persistenceStats.bufferSize = this.buffer.length
      throw error
    }
  }

  /**
   * Convert errors to columnar format
   */
  private errorsToColumns(errors: WorkerError[]): Record<string, unknown[]> {
    const columns: Record<string, unknown[]> = {}

    // Initialize columns
    for (const colName of Object.keys(WORKER_ERRORS_SCHEMA)) {
      columns[colName] = []
    }

    // Fill columns
    for (const error of errors) {
      columns['id']!.push(error.id)
      columns['ts']!.push(error.ts)
      columns['category']!.push(error.category)
      columns['severity']!.push(error.severity)
      columns['code']!.push(error.code)
      columns['message']!.push(error.message)
      columns['stack']!.push(error.stack ?? null)
      columns['path']!.push(error.path ?? null)
      columns['method']!.push(error.method ?? null)
      columns['requestId']!.push(error.requestId ?? null)
      columns['workerName']!.push(error.workerName ?? null)
      columns['colo']!.push(error.colo ?? null)
      columns['metadata']!.push(error.metadata ? JSON.stringify(error.metadata) : null)
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
      schema: WORKER_ERRORS_SCHEMA,
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

/**
 * Create a new WorkerErrors MV instance
 */
export function createWorkerErrorsMV(config?: WorkerErrorsConfig): WorkerErrorsMV {
  return new WorkerErrorsMV(config)
}
