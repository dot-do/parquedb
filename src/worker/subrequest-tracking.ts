/**
 * Fetch Subrequest Tracking
 *
 * Utilities for extracting and counting fetch subrequests from Tail Worker events.
 * Fetch subrequests appear in the diagnosticsChannelEvents array when a Worker
 * makes outbound HTTP requests using the Fetch API.
 *
 * This is important for:
 * - Monitoring Snippets compliance (5 subrequest limit)
 * - Debugging Worker performance
 * - Understanding Worker dependencies
 *
 * @see https://developers.cloudflare.com/workers/observability/logs/tail-workers/
 * @see https://developers.cloudflare.com/workers/platform/limits/
 */

import type { ValidatedTraceItem } from './tail-validation'
import { WORKERS_PAID_SUBREQUEST_LIMIT as IMPORTED_WORKERS_PAID_SUBREQUEST_LIMIT } from '../constants'

// =============================================================================
// Types
// =============================================================================

/**
 * Diagnostics channel event from Cloudflare Workers
 *
 * These events are emitted when certain operations occur within a Worker,
 * including fetch subrequests, KV operations, and other binding calls.
 */
export interface DiagnosticsChannelEvent {
  /** Channel name identifying the event type (e.g., 'fetch', 'kv', 'cache') */
  channel: string
  /** Timestamp when the event occurred (ms since epoch) */
  timestamp: number
  /** Event-specific message data */
  message: Record<string, unknown> | null
}

/**
 * Extracted fetch subrequest information
 */
export interface FetchSubrequest {
  /** URL of the subrequest */
  url?: string | undefined
  /** HTTP method used */
  method?: string | undefined
  /** Response status code */
  status?: number | undefined
  /** Duration of the request in milliseconds */
  duration?: number | undefined
  /** Timestamp when the request was made */
  timestamp: number
}

/**
 * Summary statistics for subrequests in a trace
 */
export interface SubrequestSummary {
  /** Total number of fetch subrequests */
  fetchCount: number
  /** Total duration of all subrequests */
  totalDurationMs: number
  /** Average duration per subrequest */
  avgDurationMs: number
  /** URLs called (deduplicated) */
  uniqueUrls: string[]
  /** Whether the trace exceeded Snippets limits (5 subrequests) */
  exceedsSnippetsLimit: boolean
}

// =============================================================================
// Constants
// =============================================================================

/**
 * Snippets subrequest limit
 * @see https://developers.cloudflare.com/workers/platform/limits/
 */
export const SNIPPETS_SUBREQUEST_LIMIT = 5

/**
 * Workers Free plan subrequest limit
 */
export const WORKERS_FREE_SUBREQUEST_LIMIT = 50

/**
 * Workers Paid plan subrequest limit
 */
export const WORKERS_PAID_SUBREQUEST_LIMIT = IMPORTED_WORKERS_PAID_SUBREQUEST_LIMIT

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Check if a value is a valid diagnostics channel event
 */
function isDiagnosticsChannelEvent(value: unknown): value is DiagnosticsChannelEvent {
  if (!value || typeof value !== 'object') {
    return false
  }
  const obj = value as Record<string, unknown>
  return typeof obj.channel === 'string' && typeof obj.timestamp === 'number'
}

/**
 * Check if a diagnostics channel event is a fetch event
 */
function isFetchEvent(event: DiagnosticsChannelEvent): boolean {
  return event.channel === 'fetch'
}

// =============================================================================
// Core Functions
// =============================================================================

/**
 * Count the number of fetch subrequests in a trace item
 *
 * Iterates through diagnosticsChannelEvents and counts events with channel 'fetch'.
 *
 * @param item - Validated trace item from a Tail Worker
 * @returns Number of fetch subrequests
 *
 * @example
 * ```typescript
 * const count = countFetchSubrequests(traceItem)
 * if (count > SNIPPETS_SUBREQUEST_LIMIT) {
 *   console.warn('Exceeded Snippets subrequest limit!')
 * }
 * ```
 */
export function countFetchSubrequests(item: ValidatedTraceItem): number {
  if (!Array.isArray(item.diagnosticsChannelEvents)) {
    return 0
  }

  let count = 0
  for (const event of item.diagnosticsChannelEvents) {
    if (isDiagnosticsChannelEvent(event) && isFetchEvent(event)) {
      count++
    }
  }

  return count
}

/**
 * Extract detailed information about fetch subrequests
 *
 * Parses diagnosticsChannelEvents to extract URL, method, status, and duration
 * for each fetch subrequest.
 *
 * @param item - Validated trace item from a Tail Worker
 * @returns Array of fetch subrequest details
 *
 * @example
 * ```typescript
 * const subrequests = extractFetchSubrequests(traceItem)
 * for (const req of subrequests) {
 *   console.log(`${req.method} ${req.url} - ${req.duration}ms`)
 * }
 * ```
 */
export function extractFetchSubrequests(item: ValidatedTraceItem): FetchSubrequest[] {
  if (!Array.isArray(item.diagnosticsChannelEvents)) {
    return []
  }

  const subrequests: FetchSubrequest[] = []

  for (const event of item.diagnosticsChannelEvents) {
    if (!isDiagnosticsChannelEvent(event) || !isFetchEvent(event)) {
      continue
    }

    const message = event.message as Record<string, unknown> | null
    const subrequest: FetchSubrequest = {
      timestamp: event.timestamp,
    }

    if (message) {
      if (typeof message.url === 'string') {
        subrequest.url = message.url
      }
      if (typeof message.method === 'string') {
        subrequest.method = message.method
      }
      if (typeof message.status === 'number') {
        subrequest.status = message.status
      }
      if (typeof message.duration === 'number') {
        subrequest.duration = message.duration
      }
    }

    subrequests.push(subrequest)
  }

  return subrequests
}

/**
 * Get a summary of subrequest statistics for a trace item
 *
 * Provides aggregate information useful for monitoring and compliance checks.
 *
 * @param item - Validated trace item from a Tail Worker
 * @returns Summary statistics for subrequests
 *
 * @example
 * ```typescript
 * const summary = getSubrequestSummary(traceItem)
 * console.log(`Fetch count: ${summary.fetchCount}`)
 * console.log(`Avg duration: ${summary.avgDurationMs}ms`)
 * if (summary.exceedsSnippetsLimit) {
 *   console.warn('Worker exceeds Snippets limits!')
 * }
 * ```
 */
export function getSubrequestSummary(item: ValidatedTraceItem): SubrequestSummary {
  const subrequests = extractFetchSubrequests(item)

  let totalDurationMs = 0
  const urls = new Set<string>()

  for (const req of subrequests) {
    if (req.duration !== undefined) {
      totalDurationMs += req.duration
    }
    if (req.url) {
      urls.add(req.url)
    }
  }

  const fetchCount = subrequests.length

  return {
    fetchCount,
    totalDurationMs,
    avgDurationMs: fetchCount > 0 ? totalDurationMs / fetchCount : 0,
    uniqueUrls: Array.from(urls),
    exceedsSnippetsLimit: fetchCount > SNIPPETS_SUBREQUEST_LIMIT,
  }
}

/**
 * Check if a trace item is compliant with Snippets limits
 *
 * Snippets have stricter limits than regular Workers:
 * - 5ms CPU time
 * - 5 subrequests
 * - 32KB memory
 *
 * @param item - Validated trace item from a Tail Worker
 * @returns true if the item is within Snippets limits
 */
export function isSnippetsCompliant(item: ValidatedTraceItem): boolean {
  const fetchCount = countFetchSubrequests(item)
  return fetchCount <= SNIPPETS_SUBREQUEST_LIMIT
}

/**
 * Count fetch subrequests from an unknown array of diagnostics channel events
 *
 * This is useful when working with raw TailItem data before it has been
 * validated as a ValidatedTraceItem.
 *
 * @param diagnosticsChannelEvents - Array of unknown diagnostics channel events
 * @returns Number of fetch subrequests
 */
export function countFetchSubrequestsFromUnknown(diagnosticsChannelEvents: unknown[] | undefined): number {
  if (!Array.isArray(diagnosticsChannelEvents)) {
    return 0
  }

  let count = 0
  for (const event of diagnosticsChannelEvents) {
    if (isDiagnosticsChannelEvent(event) && isFetchEvent(event)) {
      count++
    }
  }

  return count
}
