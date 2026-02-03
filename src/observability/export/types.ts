/**
 * Observability Export Types
 *
 * Type definitions for dashboard and export APIs supporting:
 * - Prometheus metrics endpoints
 * - OpenTelemetry trace/metric export
 * - WebSocket/SSE streaming
 * - JSON export for custom integrations
 * - Grafana-compatible query interface
 *
 * @module observability/export/types
 */

// =============================================================================
// Export Format Types
// =============================================================================

/**
 * Supported export formats
 */
export type ExportFormat =
  | 'prometheus'
  | 'opentelemetry'
  | 'json'
  | 'csv'
  | 'grafana'

/**
 * Time range for export queries
 */
export interface TimeRange {
  /** Start time (inclusive) */
  from: Date
  /** End time (exclusive) */
  to: Date
}

/**
 * Resolution/step for time-series data
 */
export type Resolution = '1m' | '5m' | '15m' | '1h' | '6h' | '1d' | '7d' | '30d'

// =============================================================================
// Prometheus Types
// =============================================================================

/**
 * Prometheus metric type
 */
export type PrometheusMetricType = 'counter' | 'gauge' | 'histogram' | 'summary'

/**
 * Prometheus metric definition
 */
export interface PrometheusMetricDef {
  name: string
  help: string
  type: PrometheusMetricType
  labels?: string[]
}

/**
 * Prometheus metric with labels and value
 */
export interface PrometheusMetricValue {
  name: string
  labels: Record<string, string>
  value: number
  timestamp?: number
}

/**
 * Prometheus histogram bucket
 */
export interface PrometheusHistogramBucket {
  le: number | '+Inf'
  count: number
}

/**
 * Prometheus histogram metric
 */
export interface PrometheusHistogram {
  name: string
  labels: Record<string, string>
  buckets: PrometheusHistogramBucket[]
  sum: number
  count: number
}

// =============================================================================
// OpenTelemetry Types
// =============================================================================

/**
 * OpenTelemetry resource attributes
 */
export interface OTelResourceAttributes {
  'service.name': string
  'service.version'?: string
  'service.namespace'?: string
  'deployment.environment'?: string
  [key: string]: string | number | boolean | undefined
}

/**
 * OpenTelemetry metric data point
 */
export interface OTelDataPoint {
  attributes: Record<string, string | number | boolean>
  startTimeUnixNano: bigint | number
  timeUnixNano: bigint | number
  value: number | { count: number; sum: number; bucketCounts: number[] }
}

/**
 * OpenTelemetry metric
 */
export interface OTelMetric {
  name: string
  description: string
  unit: string
  data: {
    dataPoints: OTelDataPoint[]
    aggregationTemporality?: 'AGGREGATION_TEMPORALITY_DELTA' | 'AGGREGATION_TEMPORALITY_CUMULATIVE'
    isMonotonic?: boolean
  }
}

/**
 * OpenTelemetry metrics export payload
 */
export interface OTelMetricsPayload {
  resourceMetrics: Array<{
    resource: {
      attributes: Array<{ key: string; value: { stringValue?: string; intValue?: number; boolValue?: boolean } }>
    }
    scopeMetrics: Array<{
      scope: { name: string; version?: string }
      metrics: OTelMetric[]
    }>
  }>
}

/**
 * OpenTelemetry span
 */
export interface OTelSpan {
  traceId: string
  spanId: string
  parentSpanId?: string
  name: string
  kind: 'SPAN_KIND_INTERNAL' | 'SPAN_KIND_SERVER' | 'SPAN_KIND_CLIENT' | 'SPAN_KIND_PRODUCER' | 'SPAN_KIND_CONSUMER'
  startTimeUnixNano: bigint | number
  endTimeUnixNano: bigint | number
  attributes: Array<{ key: string; value: { stringValue?: string; intValue?: number; boolValue?: boolean } }>
  status?: { code: 'STATUS_CODE_OK' | 'STATUS_CODE_ERROR'; message?: string }
  events?: Array<{
    timeUnixNano: bigint | number
    name: string
    attributes?: Array<{ key: string; value: { stringValue?: string; intValue?: number; boolValue?: boolean } }>
  }>
}

/**
 * OpenTelemetry trace export payload
 */
export interface OTelTracePayload {
  resourceSpans: Array<{
    resource: {
      attributes: Array<{ key: string; value: { stringValue?: string; intValue?: number; boolValue?: boolean } }>
    }
    scopeSpans: Array<{
      scope: { name: string; version?: string }
      spans: OTelSpan[]
    }>
  }>
}

// =============================================================================
// Grafana Types
// =============================================================================

/**
 * Grafana query request
 */
export interface GrafanaQueryRequest {
  /** Query targets */
  targets: Array<{
    /** Reference ID for the query */
    refId: string
    /** Metric name or query expression */
    target: string
    /** Query type */
    type?: 'timeseries' | 'table'
    /** Format */
    format?: 'time_series' | 'table'
    /** Additional payload */
    payload?: Record<string, unknown>
  }>
  /** Time range */
  range: {
    from: string
    to: string
    raw: { from: string; to: string }
  }
  /** Interval in milliseconds */
  intervalMs: number
  /** Maximum data points */
  maxDataPoints: number
  /** Scoped vars */
  scopedVars?: Record<string, { text: string; value: string }>
}

/**
 * Grafana time series response
 */
export interface GrafanaTimeSeriesResponse {
  target: string
  datapoints: Array<[number, number]> // [value, timestamp]
  refId?: string
}

/**
 * Grafana table response
 */
export interface GrafanaTableResponse {
  columns: Array<{ text: string; type?: string }>
  rows: unknown[][]
  type: 'table'
  refId?: string
}

/**
 * Grafana query response
 */
export type GrafanaQueryResponse = GrafanaTimeSeriesResponse | GrafanaTableResponse

/**
 * Grafana annotation request
 */
export interface GrafanaAnnotationRequest {
  range: { from: string; to: string }
  annotation: {
    name: string
    datasource: string
    enable: boolean
    iconColor?: string
    query?: string
  }
}

/**
 * Grafana annotation
 */
export interface GrafanaAnnotation {
  time: number
  timeEnd?: number
  title: string
  text?: string
  tags?: string[]
}

/**
 * Grafana variable request (for template variables)
 */
export interface GrafanaVariableRequest {
  payload: {
    target: string
    variables?: Record<string, { text: string; value: string }>
  }
  range: { from: string; to: string }
}

/**
 * Grafana variable response
 */
export type GrafanaVariableResponse = Array<{ __text: string; __value: string }>

// =============================================================================
// Streaming Types
// =============================================================================

/**
 * SSE event type
 */
export type SSEEventType = 'metric' | 'alert' | 'heartbeat' | 'error'

/**
 * SSE metric event
 */
export interface SSEMetricEvent {
  type: 'metric'
  timestamp: number
  namespace: string
  metrics: Record<string, number>
}

/**
 * SSE alert event
 */
export interface SSEAlertEvent {
  type: 'alert'
  timestamp: number
  severity: 'info' | 'warning' | 'critical'
  namespace?: string
  title: string
  message: string
  metadata?: Record<string, unknown>
}

/**
 * SSE heartbeat event
 */
export interface SSEHeartbeatEvent {
  type: 'heartbeat'
  timestamp: number
}

/**
 * SSE error event
 */
export interface SSEErrorEvent {
  type: 'error'
  timestamp: number
  error: string
  code?: string
}

/**
 * SSE event
 */
export type SSEEvent = SSEMetricEvent | SSEAlertEvent | SSEHeartbeatEvent | SSEErrorEvent

/**
 * WebSocket message type
 */
export type WSMessageType = 'subscribe' | 'unsubscribe' | 'metric' | 'alert' | 'error' | 'ack'

/**
 * WebSocket subscribe message
 */
export interface WSSubscribeMessage {
  type: 'subscribe'
  id: string
  namespaces?: string[]
  metrics?: string[]
  interval?: number
}

/**
 * WebSocket unsubscribe message
 */
export interface WSUnsubscribeMessage {
  type: 'unsubscribe'
  id: string
}

/**
 * WebSocket metric message
 */
export interface WSMetricMessage {
  type: 'metric'
  subscriptionId: string
  timestamp: number
  namespace: string
  metrics: Record<string, number>
}

/**
 * WebSocket alert message
 */
export interface WSAlertMessage {
  type: 'alert'
  timestamp: number
  severity: 'info' | 'warning' | 'critical'
  namespace?: string
  title: string
  message: string
}

/**
 * WebSocket error message
 */
export interface WSErrorMessage {
  type: 'error'
  error: string
  code?: string
}

/**
 * WebSocket ack message
 */
export interface WSAckMessage {
  type: 'ack'
  id: string
  status: 'subscribed' | 'unsubscribed' | 'error'
  message?: string
}

/**
 * WebSocket message
 */
export type WSMessage =
  | WSSubscribeMessage
  | WSUnsubscribeMessage
  | WSMetricMessage
  | WSAlertMessage
  | WSErrorMessage
  | WSAckMessage

// =============================================================================
// JSON Export Types
// =============================================================================

/**
 * JSON export options
 */
export interface JSONExportOptions {
  /** Namespaces to include (all if not specified) */
  namespaces?: string[]
  /** Metrics to include (all if not specified) */
  metrics?: string[]
  /** Time range */
  timeRange?: TimeRange
  /** Resolution for time-series data */
  resolution?: Resolution
  /** Maximum data points per series */
  maxDataPoints?: number
  /** Include metadata */
  includeMetadata?: boolean
  /** Pretty print JSON */
  pretty?: boolean
}

/**
 * JSON metric series
 */
export interface JSONMetricSeries {
  metric: string
  namespace: string
  labels?: Record<string, string>
  dataPoints: Array<{
    timestamp: number
    value: number
  }>
}

/**
 * JSON export payload
 */
export interface JSONExportPayload {
  timestamp: number
  version: string
  exportedAt: string
  options: JSONExportOptions
  namespaces: Record<string, {
    latest?: Record<string, number>
    timeSeries?: Record<string, JSONMetricSeries>
    metadata?: Record<string, unknown>
  }>
}

// =============================================================================
// CSV Export Types
// =============================================================================

/**
 * CSV export options
 */
export interface CSVExportOptions {
  /** Namespaces to include */
  namespaces?: string[]
  /** Metrics to include */
  metrics?: string[]
  /** Time range */
  timeRange?: TimeRange
  /** Include header row */
  includeHeader?: boolean
  /** Delimiter (default: comma) */
  delimiter?: ',' | '\t' | ';'
  /** Quote character (default: double quote) */
  quoteChar?: '"' | "'"
}

// =============================================================================
// Dashboard API Types
// =============================================================================

/**
 * Dashboard configuration
 */
export interface DashboardConfig {
  /** Dashboard title */
  title: string
  /** Refresh interval in seconds */
  refreshInterval: number
  /** Namespaces to monitor */
  namespaces: string[]
  /** Health thresholds */
  thresholds?: {
    degraded?: Record<string, number>
    unhealthy?: Record<string, number>
  }
  /** Time range for charts */
  timeRange?: TimeRange
  /** Chart resolution */
  resolution?: Resolution
}

/**
 * Dashboard panel data
 */
export interface DashboardPanelData {
  id: string
  title: string
  type: 'gauge' | 'timeseries' | 'stat' | 'table' | 'alert-list'
  data: unknown
  lastUpdated: number
}

/**
 * Dashboard snapshot
 */
export interface DashboardSnapshot {
  config: DashboardConfig
  panels: DashboardPanelData[]
  health: 'healthy' | 'degraded' | 'unhealthy'
  timestamp: number
}

// =============================================================================
// API Endpoint Types
// =============================================================================

/**
 * Export API endpoint configuration
 */
export interface ExportAPIConfig {
  /** Base path for export endpoints */
  basePath: string
  /** Enable Prometheus endpoint */
  enablePrometheus: boolean
  /** Enable OpenTelemetry endpoint */
  enableOpenTelemetry: boolean
  /** Enable Grafana-compatible endpoint */
  enableGrafana: boolean
  /** Enable JSON export endpoint */
  enableJSON: boolean
  /** Enable CSV export endpoint */
  enableCSV: boolean
  /** Enable SSE streaming endpoint */
  enableSSE: boolean
  /** Enable WebSocket streaming endpoint */
  enableWebSocket: boolean
  /** CORS configuration */
  cors?: {
    origins: string[]
    methods: string[]
    headers: string[]
  }
  /** Authentication configuration */
  auth?: {
    type: 'none' | 'bearer' | 'basic' | 'api-key'
    /** Header name for API key auth */
    apiKeyHeader?: string
  }
}

/**
 * Default export API configuration
 */
export const DEFAULT_EXPORT_API_CONFIG: ExportAPIConfig = {
  basePath: '/api/observability',
  enablePrometheus: true,
  enableOpenTelemetry: true,
  enableGrafana: true,
  enableJSON: true,
  enableCSV: true,
  enableSSE: true,
  enableWebSocket: true,
  cors: {
    origins: ['*'],
    methods: ['GET', 'POST', 'OPTIONS'],
    headers: ['Content-Type', 'Authorization', 'X-API-Key'],
  },
}
