/**
 * Compaction Observability
 *
 * Monitoring infrastructure for the compaction system:
 * - Metrics emission and collection
 * - Dashboard HTML generation
 * - Prometheus export
 * - JSON time-series export
 */

// Types
export type {
  CompactionMetrics,
  MetricDataPoint,
  MetricTimeSeries,
  AggregatedMetrics,
  HealthIndicator,
  DashboardConfig,
  PrometheusMetricType,
  PrometheusMetric,
  AnalyticsEngineDataPoint,
} from './types'

export {
  DEFAULT_DASHBOARD_CONFIG,
  PROMETHEUS_METRICS,
  ANALYTICS_ENGINE_SCHEMA,
} from './types'

// Metrics
export {
  emitCompactionMetrics,
  emitToAnalyticsEngine,
  getLatestMetrics,
  getAllLatestMetrics,
  getMetricTimeSeries,
  getAggregatedMetrics,
  clearMetricsStore,
  exportPrometheusMetrics,
  exportJsonTimeSeries,
} from './metrics'

// Dashboard
export {
  evaluateHealth,
  evaluateAggregatedHealth,
  generateDashboardHtml,
} from './dashboard'
