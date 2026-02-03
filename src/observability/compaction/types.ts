/**
 * Compaction Monitoring Types
 *
 * Type definitions for compaction system observability.
 */

// =============================================================================
// Metrics Types
// =============================================================================

/** Compaction metrics emitted on each state update */
export interface CompactionMetrics {
  /** Namespace this metric belongs to */
  namespace: string
  /** Timestamp of the metric */
  timestamp: number
  /** Number of windows pending compaction */
  windows_pending: number
  /** Number of windows currently being processed */
  windows_processing: number
  /** Number of windows dispatched to workflows */
  windows_dispatched: number
  /** Total files pending across all windows */
  files_pending: number
  /** Age in ms of the oldest pending window */
  oldest_window_age_ms: number
  /** Number of known writers */
  known_writers: number
  /** Number of currently active writers */
  active_writers: number
  /** Total bytes pending compaction */
  bytes_pending: number
  /** Number of windows stuck in processing (timeout) */
  windows_stuck: number
}

/** Time-series data point for charting */
export interface MetricDataPoint {
  timestamp: number
  value: number
}

/** Time-series for a specific metric */
export interface MetricTimeSeries {
  metric: string
  namespace: string
  data: MetricDataPoint[]
}

/** Aggregated metrics across all namespaces */
export interface AggregatedMetrics {
  /** Total windows pending across all namespaces */
  total_windows_pending: number
  /** Total windows processing */
  total_windows_processing: number
  /** Total windows dispatched */
  total_windows_dispatched: number
  /** Total files pending */
  total_files_pending: number
  /** Total bytes pending */
  total_bytes_pending: number
  /** Number of namespaces being tracked */
  namespace_count: number
  /** Per-namespace breakdown */
  by_namespace: Record<string, CompactionMetrics>
}

// =============================================================================
// Dashboard Types
// =============================================================================

/** Health indicator for dashboard display */
export type HealthIndicator = 'healthy' | 'degraded' | 'unhealthy'

/** Dashboard configuration */
export interface DashboardConfig {
  /** Auto-refresh interval in seconds */
  refreshIntervalSeconds: number
  /** Maximum data points to show in charts */
  maxDataPoints: number
  /** Thresholds for health indicators */
  thresholds: {
    /** Max pending windows before degraded */
    pendingWindowsDegraded: number
    /** Max pending windows before unhealthy */
    pendingWindowsUnhealthy: number
    /** Max window age (hours) before degraded */
    windowAgeDegradedHours: number
    /** Max window age (hours) before unhealthy */
    windowAgeUnhealthyHours: number
    /** Any stuck windows triggers unhealthy */
    stuckWindowsUnhealthy: number
  }
}

/** Default dashboard configuration */
export const DEFAULT_DASHBOARD_CONFIG: DashboardConfig = {
  refreshIntervalSeconds: 30,
  maxDataPoints: 60, // 30 minutes at 30-second intervals
  thresholds: {
    pendingWindowsDegraded: 10,
    pendingWindowsUnhealthy: 50,
    windowAgeDegradedHours: 2,
    windowAgeUnhealthyHours: 6,
    stuckWindowsUnhealthy: 1,
  },
}

// =============================================================================
// Prometheus Export Types
// =============================================================================

/** Prometheus metric type */
export type PrometheusMetricType = 'counter' | 'gauge' | 'histogram' | 'summary'

/** Prometheus metric definition */
export interface PrometheusMetric {
  name: string
  help: string
  type: PrometheusMetricType
  labels?: string[] | undefined
}

/** Compaction metrics for Prometheus export */
export const PROMETHEUS_METRICS: PrometheusMetric[] = [
  {
    name: 'parquedb_compaction_windows_pending',
    help: 'Number of compaction windows pending',
    type: 'gauge',
    labels: ['namespace'],
  },
  {
    name: 'parquedb_compaction_windows_processing',
    help: 'Number of compaction windows being processed',
    type: 'gauge',
    labels: ['namespace'],
  },
  {
    name: 'parquedb_compaction_windows_dispatched',
    help: 'Number of compaction windows dispatched to workflows',
    type: 'gauge',
    labels: ['namespace'],
  },
  {
    name: 'parquedb_compaction_files_pending',
    help: 'Total files pending compaction',
    type: 'gauge',
    labels: ['namespace'],
  },
  {
    name: 'parquedb_compaction_bytes_pending',
    help: 'Total bytes pending compaction',
    type: 'gauge',
    labels: ['namespace'],
  },
  {
    name: 'parquedb_compaction_oldest_window_age_seconds',
    help: 'Age of oldest pending window in seconds',
    type: 'gauge',
    labels: ['namespace'],
  },
  {
    name: 'parquedb_compaction_known_writers',
    help: 'Number of known writers',
    type: 'gauge',
    labels: ['namespace'],
  },
  {
    name: 'parquedb_compaction_active_writers',
    help: 'Number of currently active writers',
    type: 'gauge',
    labels: ['namespace'],
  },
  {
    name: 'parquedb_compaction_windows_stuck',
    help: 'Number of windows stuck in processing',
    type: 'gauge',
    labels: ['namespace'],
  },
]

// =============================================================================
// Analytics Engine Types (Cloudflare)
// =============================================================================

/** Analytics Engine data point for Workers Analytics Engine */
export interface AnalyticsEngineDataPoint {
  /** Index fields for grouping (strings, max 32) */
  indexes?: string[] | undefined
  /** Blob fields for additional metadata (strings, max 32) */
  blobs?: string[] | undefined
  /** Double fields for numeric values (numbers, max 20) */
  doubles?: number[] | undefined
}

/** Mapping of metric names to their blob/double indexes */
export const ANALYTICS_ENGINE_SCHEMA = {
  indexes: {
    0: 'namespace',
  },
  blobs: {
    0: 'metric_name',
  },
  doubles: {
    0: 'metric_value',
    1: 'timestamp',
  },
} as const
