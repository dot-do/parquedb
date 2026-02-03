/**
 * Streaming Refresh Engine
 *
 * This module re-exports the canonical implementation from materialized-views/streaming.
 * The full-featured implementation includes:
 * - Event buffering and batching for efficiency
 * - Backpressure handling with warnings
 * - Error isolation (one MV failure doesn't affect others)
 * - Retry logic with exponential backoff
 * - Comprehensive statistics and monitoring
 *
 * @example
 * ```typescript
 * import { createStreamingRefreshEngine } from 'parquedb/streaming'
 *
 * const engine = createStreamingRefreshEngine({
 *   batchSize: 100,
 *   batchTimeoutMs: 500,
 * })
 *
 * engine.registerMV({
 *   name: 'OrderAnalytics',
 *   sourceNamespaces: ['orders', 'products'],
 *   async process(events) {
 *     // Update MV based on events
 *   }
 * })
 *
 * await engine.start()
 * await engine.processEvent(event)
 * ```
 */

// Re-export the canonical implementation from materialized-views
export {
  StreamingRefreshEngine,
  createStreamingRefreshEngine,
} from '../materialized-views/streaming'

// Re-export types from materialized-views
export type {
  StreamingRefreshConfig,
  MVHandler,
  StreamingStats,
  ErrorHandler,
  WarningHandler,
} from '../materialized-views/streaming'
