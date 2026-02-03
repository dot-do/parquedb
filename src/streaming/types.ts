/**
 * Streaming Types for ParqueDB Materialized Views
 *
 * Defines types for streaming MVs that process events in real-time.
 */

import type { Event, EventOp } from '../types/entity'

// =============================================================================
// Worker Error Types
// =============================================================================

/**
 * Error severity levels for categorization
 */
export type ErrorSeverity = 'critical' | 'error' | 'warning' | 'info'

/**
 * Error category for grouping similar errors
 */
export type ErrorCategory =
  | 'network'        // Network/connectivity errors
  | 'timeout'        // Timeout errors
  | 'validation'     // Input validation errors
  | 'authentication' // Auth/permission errors
  | 'storage'        // Storage/R2 errors
  | 'query'          // Query execution errors
  | 'internal'       // Internal/unexpected errors
  | 'rate_limit'     // Rate limiting errors
  | 'unknown'        // Uncategorized errors

/**
 * Parsed worker error with categorization
 */
export interface WorkerError {
  /** Unique error ID */
  id: string
  /** Timestamp of the error */
  ts: number
  /** Error category */
  category: ErrorCategory
  /** Error severity */
  severity: ErrorSeverity
  /** Error code (from ParqueDB error codes or HTTP status) */
  code: string
  /** Error message */
  message: string
  /** Stack trace (if available) */
  stack?: string
  /** Request path that caused the error */
  path?: string
  /** HTTP method */
  method?: string
  /** Request ID for correlation */
  requestId?: string
  /** Worker script name */
  workerName?: string
  /** Datacenter/colo location */
  colo?: string
  /** Additional metadata */
  metadata?: Record<string, unknown>
}

/**
 * Worker error statistics
 */
export interface WorkerErrorStats {
  /** Total error count */
  totalErrors: number
  /** Errors by category */
  byCategory: Record<ErrorCategory, number>
  /** Errors by severity */
  bySeverity: Record<ErrorSeverity, number>
  /** Errors by code */
  byCode: Record<string, number>
  /** Errors by worker */
  byWorker: Record<string, number>
  /** Error rate per minute (rolling) */
  errorRatePerMinute: number
  /** Time range covered */
  timeRange: {
    start: number
    end: number
  }
}

// =============================================================================
// MV Handler Interface
// =============================================================================

/**
 * Handler for processing events in a materialized view
 */
export interface MVHandler {
  /** Unique name of the MV */
  name: string
  /** Source namespaces this MV listens to */
  sourceNamespaces: string[]
  /** Process a batch of events */
  process(events: Event[]): Promise<void>
}

// =============================================================================
// Streaming Engine Types
// =============================================================================

/**
 * Configuration for the streaming refresh engine
 */
export interface StreamingRefreshConfig {
  /** Number of events to batch before processing (default: 10) */
  batchSize?: number
  /** Maximum time to wait before flushing a batch in ms (default: 100) */
  batchTimeoutMs?: number
  /** Maximum buffer size before applying backpressure (default: 100) */
  maxBufferSize?: number
}

/**
 * Statistics for the streaming engine
 */
export interface StreamingStats {
  /** Total events received */
  eventsReceived: number
  /** Total events processed successfully */
  eventsProcessed: number
  /** Total batches processed */
  batchesProcessed: number
  /** Failed batches */
  failedBatches: number
  /** Backpressure events (when buffer was full) */
  backpressureEvents: number
  /** Events by operation type */
  eventsByOp: Partial<Record<EventOp, number>>
}

// =============================================================================
// Error Classification
// =============================================================================

/**
 * Pattern for matching errors to categories
 */
export interface ErrorPattern {
  /** Regex pattern to match error message */
  pattern: RegExp
  /** Category to assign */
  category: ErrorCategory
  /** Severity to assign */
  severity: ErrorSeverity
  /** Optional code override */
  code?: string
}

/**
 * Default error patterns for classification
 *
 * NOTE: Order matters! More specific patterns should come before general ones.
 * For example, "Invalid filter" should match query before validation matches "Invalid".
 */
export const DEFAULT_ERROR_PATTERNS: ErrorPattern[] = [
  // Network errors
  { pattern: /network|ECONNREFUSED|ENOTFOUND|ETIMEDOUT|fetch failed/i, category: 'network', severity: 'error' },
  { pattern: /socket hang up|connection reset/i, category: 'network', severity: 'error' },

  // Timeout errors
  { pattern: /timeout|timed out|deadline exceeded/i, category: 'timeout', severity: 'warning' },

  // Authentication errors (before validation to catch "invalid token")
  { pattern: /unauthorized|forbidden|permission denied|auth.*fail/i, category: 'authentication', severity: 'error' },
  { pattern: /token.*expired|invalid.*token/i, category: 'authentication', severity: 'error' },

  // Query errors (before validation to catch "Invalid filter")
  { pattern: /query|filter|sort|projection|aggregat/i, category: 'query', severity: 'warning' },

  // Validation errors
  { pattern: /validation|required field|missing.*param/i, category: 'validation', severity: 'warning' },
  { pattern: /schema.*mismatch|type.*error/i, category: 'validation', severity: 'warning' },
  { pattern: /\binvalid\b(?!.*token)/i, category: 'validation', severity: 'warning' },

  // Storage errors
  { pattern: /storage|R2|bucket|file not found|quota exceeded/i, category: 'storage', severity: 'error' },
  { pattern: /ENOENT|EACCES|ENOSPC/i, category: 'storage', severity: 'error' },

  // Rate limit errors
  { pattern: /rate.*limit|too many requests|429/i, category: 'rate_limit', severity: 'warning' },

  // Internal errors (should be last, catch-all patterns)
  { pattern: /internal.*error|unexpected|assertion|invariant/i, category: 'internal', severity: 'critical' },
]

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Classify an error message into a category and severity
 */
export function classifyError(
  message: string,
  code?: string,
  patterns: ErrorPattern[] = DEFAULT_ERROR_PATTERNS
): { category: ErrorCategory; severity: ErrorSeverity; code: string } {
  for (const pattern of patterns) {
    if (pattern.pattern.test(message)) {
      return {
        category: pattern.category,
        severity: pattern.severity,
        code: pattern.code ?? code ?? pattern.category.toUpperCase(),
      }
    }
  }

  // Default to unknown category
  return {
    category: 'unknown',
    severity: 'error',
    code: code ?? 'UNKNOWN',
  }
}

/**
 * Determine severity from HTTP status code
 */
export function severityFromStatus(status: number): ErrorSeverity {
  if (status >= 500) return 'critical'
  if (status >= 400) return 'warning'
  return 'info'
}

/**
 * Determine category from HTTP status code
 */
export function categoryFromStatus(status: number): ErrorCategory {
  switch (status) {
    case 400:
      return 'validation'
    case 401:
    case 403:
      return 'authentication'
    case 404:
      return 'storage'
    case 408:
      return 'timeout'
    case 429:
      return 'rate_limit'
    case 500:
    case 502:
    case 503:
    case 504:
      return 'internal'
    default:
      return 'unknown'
  }
}
