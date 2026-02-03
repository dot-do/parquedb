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
 */

import type { Event } from '../types/entity'
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
}

const DEFAULT_CONFIG: Required<WorkerErrorsConfig> = {
  errorPatterns: [],
  maxErrors: 10000,
  statsWindowMs: 60000,
  sourceNamespaces: ['logs', 'errors', 'workers'],
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
 */
export class WorkerErrorsMV implements MVHandler {
  readonly name = 'WorkerErrors'
  readonly sourceNamespaces: string[]

  private readonly config: Required<WorkerErrorsConfig>
  private readonly errorPatterns: ErrorPattern[]

  // Error storage
  private errors: WorkerError[] = []

  // Indexes for fast lookup
  private byCategory: Map<ErrorCategory, Set<string>> = new Map()
  private bySeverity: Map<ErrorSeverity, Set<string>> = new Map()
  private byCode: Map<string, Set<string>> = new Map()
  private byWorker: Map<string, Set<string>> = new Map()
  private byPath: Map<string, Set<string>> = new Map()
  private byId: Map<string, WorkerError> = new Map()

  constructor(config: WorkerErrorsConfig = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config }
    this.sourceNamespaces = this.config.sourceNamespaces
    this.errorPatterns = [
      ...DEFAULT_ERROR_PATTERNS,
      ...this.config.errorPatterns,
    ]

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
    const windowStart = now - this.config.statsWindowMs

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
    const windowMinutes = this.config.statsWindowMs / 60000
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
   * Get error count
   */
  count(): number {
    return this.errors.length
  }

  /**
   * Clear all errors
   */
  clear(): void {
    this.errors = []
    this.byId.clear()
    for (const set of this.byCategory.values()) set.clear()
    for (const set of this.bySeverity.values()) set.clear()
    this.byCode.clear()
    this.byWorker.clear()
    this.byPath.clear()
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
      requestId: (after.requestId ?? after.cf?.rayId) as string | undefined,
      workerName: after.workerName as string | undefined,
      colo: (after.colo ?? after.cf?.colo) as string | undefined,
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

    // Enforce max size
    while (this.errors.length >= this.config.maxErrors) {
      const oldest = this.errors.shift()
      if (oldest) {
        this.removeFromIndexes(oldest)
      }
    }

    // Add to storage
    this.errors.push(error)
    this.byId.set(error.id, error)

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
}

/**
 * Create a new WorkerErrors MV instance
 */
export function createWorkerErrorsMV(config?: WorkerErrorsConfig): WorkerErrorsMV {
  return new WorkerErrorsMV(config)
}
