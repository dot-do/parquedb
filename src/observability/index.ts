/**
 * Observability Module for ParqueDB
 *
 * Re-exports all observability types, hooks, and utilities.
 *
 * @module observability
 */

export {
  // Context types
  type HookContext,
  type QueryContext,
  type MutationContext,
  type StorageContext,

  // Result types
  type QueryResult,
  type MutationResult,
  type StorageResult,

  // Hook interfaces
  type QueryHook,
  type MutationHook,
  type StorageHook,
  type ObservabilityHook,

  // Metrics types
  type OperationMetrics,
  type AggregatedMetrics,

  // Classes
  HookRegistry,
  MetricsCollector,

  // Utility functions
  generateOperationId,
  createQueryContext,
  createMutationContext,
  createStorageContext,

  // Global instance
  globalHookRegistry,
} from './hooks'

// Re-export retention module
export {
  RetentionManager,
  createRetentionManager,
  type RetentionPolicy,
  type TieredRetentionPolicies,
  type RetentionManagerConfig,
  type ResolvedRetentionConfig,
  type CleanupProgress,
  type CleanupResult,
  type ScheduleOptions,
  type CleanupScheduler,
} from './retention'

// Re-export compaction observability module
export {
  // Types
  type CompactionMetrics,
  type MetricDataPoint,
  type MetricTimeSeries,
  type AggregatedMetrics as CompactionAggregatedMetrics,
  type HealthIndicator,
  type DashboardConfig,
  type PrometheusMetricType,
  type PrometheusMetric,
  type AnalyticsEngineDataPoint,

  // Constants
  DEFAULT_DASHBOARD_CONFIG,
  PROMETHEUS_METRICS,
  ANALYTICS_ENGINE_SCHEMA,

  // Metrics functions
  emitCompactionMetrics,
  emitToAnalyticsEngine,
  getLatestMetrics,
  getAllLatestMetrics,
  getMetricTimeSeries,
  getAggregatedMetrics,
  clearMetricsStore,
  exportPrometheusMetrics,
  exportJsonTimeSeries,

  // Dashboard functions
  evaluateHealth,
  evaluateAggregatedHealth,
  generateDashboardHtml,
} from './compaction'
