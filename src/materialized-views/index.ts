/**
 * Materialized Views module for ParqueDB
 *
 * Provides:
 * - Streaming refresh: Real-time MV updates triggered by CDC events
 * - Scheduled refresh: Periodic MV rebuilds using DO alarms
 * - Manual refresh: On-demand MV rebuilds
 * - Staleness detection: Track and detect stale views
 * - Incremental refresh: Efficient delta-based updates
 *
 * File organization (consolidated):
 * - types.ts: Core types, IngestSource types, cron re-exports
 * - define.ts: defineView(), parseSchema(), cycle detection, validation
 * - scheduler.ts: MVScheduler, cron validation
 * - stream-processor.ts: StreamProcessor, StreamPersistence (WAL/DLQ/checkpointing)
 * - streaming.ts: StreamingRefreshEngine (CDC events)
 * - staleness.ts: StalenessDetector, lineage tracking
 * - aggregations.ts: Aggregate computation, groupBy, having
 * - refresh.ts: Full refresh implementation
 * - incremental.ts: IncrementalRefresher (delta-based updates)
 * - storage.ts: MVStorageManager (view persistence)
 * - version-providers.ts: SourceVersionProvider implementations
 * - write-path-integration.ts: MVEventEmitter, ParqueDB hooks
 */

// Core types - types.ts is the canonical source for shared types
export * from './types'

// Define module - exports defineView which shadows types.ts version
// Use explicit exports to avoid conflict
export {
  MVDefinitionError,
  parseSchema,
  detectMVCycles,
  validateSchema,
  validateNameStrict,
  type ParsedSchema,
  // Note: defineView and DefineViewInput are also in types.ts - use types.ts version
} from './define'

// Storage module
export * from './storage'

// Streaming module
export * from './streaming'

// Staleness module - has MVLineage that conflicts with types.ts
// Exclude MVLineage (use types.ts version)
export {
  StalenessDetector,
  createEmptyLineage,
  serializeLineage,
  deserializeLineage,
  type SourceVersion,
  type SourceVersionProvider,
  type StalenessState,
  type SourceStaleness,
  type StalenessMetrics,
} from './staleness'

// Aggregations module - has AggregateExpr and GroupBySpec that conflict with types.ts
// Types.ts has simpler versions; use aggregations.ts for implementation details
export {
  computeAggregate,
  computeGroupKey,
  executeMVAggregation,
  isTimeGrouping,
  isBucketGrouping,
  type CountExpr,
  type SumExpr,
  type AvgExpr,
  type MinExpr,
  type MaxExpr,
  type FirstExpr,
  type LastExpr,
  type GroupByField,
  type TimeGrouping,
  type BucketGrouping,
  type MVAggregationResult,
  type Document,
} from './aggregations'

// Refresh module
export * from './refresh'

// Scheduler module - has RetryConfig that conflicts with stream-processor
export {
  MVScheduler,
  createMVScheduler,
  DEFAULT_RETRY_CONFIG,
  type MVSchedulerConfig,
  type SchedulerStats,
} from './scheduler'

// Incremental module - has lineage functions that conflict with staleness
// Exclude the conflicting lineage helpers, use staleness.ts versions
export {
  IncrementalRefresher,
  createIncrementalRefresher,
  type IncrementalLineage,
  type IncrementalRefreshResult,
  type IncrementalRefreshOptions,
  type SourceDelta,
} from './incremental'

// Stream processor module - includes StreamPersistence (consolidated from stream-persistence.ts)
// Has RetryConfig that conflicts with scheduler - let stream-processor's version be exported
export * from './stream-processor'

// Version providers module
export * from './version-providers'

// Write path integration module
export * from './write-path-integration'
