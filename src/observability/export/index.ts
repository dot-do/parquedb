/**
 * Observability Export Module
 *
 * Provides comprehensive export APIs for observability data:
 * - Prometheus metrics endpoint (/metrics)
 * - OpenTelemetry trace/metric export (OTLP/JSON)
 * - WebSocket/SSE streaming for dashboards
 * - JSON export API for custom integrations
 * - CSV export for data analysis
 * - Grafana-compatible query interface
 *
 * @example
 * ```typescript
 * import {
 *   exportAIUsageToPrometheus,
 *   exportAIUsageToOTLP,
 *   handleAIUsageQuery,
 *   createSSEStream,
 *   exportAIUsageToJSON,
 *   exportAIUsageToCSV,
 * } from 'parquedb/observability/export'
 *
 * // Prometheus endpoint
 * app.get('/metrics', async (req, res) => {
 *   const aggregates = await usageMV.getUsage()
 *   const prometheus = exportAIUsageToPrometheus(aggregates)
 *   res.type('text/plain').send(prometheus)
 * })
 *
 * // OpenTelemetry endpoint
 * app.post('/v1/metrics', async (req, res) => {
 *   const aggregates = await usageMV.getUsage()
 *   const otlp = exportAIUsageToOTLP(aggregates)
 *   res.json(otlp)
 * })
 *
 * // Grafana data source
 * app.post('/grafana/query', async (req, res) => {
 *   const aggregates = await usageMV.getUsage()
 *   const response = handleAIUsageQuery(req.body, aggregates)
 *   res.json(response)
 * })
 *
 * // SSE streaming
 * app.get('/stream', async (req, res) => {
 *   const stream = createSSEStream(getMetrics)
 *   return createSSEResponse(stream)
 * })
 *
 * // JSON export
 * app.get('/export/json', async (req, res) => {
 *   const aggregates = await usageMV.getUsage()
 *   const json = exportAIUsageToJSON(aggregates)
 *   res.json(json)
 * })
 *
 * // CSV export
 * app.get('/export/csv', async (req, res) => {
 *   const aggregates = await usageMV.getUsage()
 *   const csv = exportAIUsageToCSV(aggregates)
 *   return createCSVResponse(csv, 'ai-usage.csv')
 * })
 * ```
 *
 * @module observability/export
 */

// =============================================================================
// Types
// =============================================================================

export type {
  // Export formats
  ExportFormat,
  TimeRange,
  Resolution,

  // Prometheus types
  PrometheusMetricType,
  PrometheusMetricDef,
  PrometheusMetricValue,
  PrometheusHistogramBucket,
  PrometheusHistogram,

  // OpenTelemetry types
  OTelResourceAttributes,
  OTelDataPoint,
  OTelMetric,
  OTelMetricsPayload,
  OTelSpan,
  OTelTracePayload,

  // Grafana types
  GrafanaQueryRequest,
  GrafanaQueryResponse,
  GrafanaTimeSeriesResponse,
  GrafanaTableResponse,
  GrafanaAnnotationRequest,
  GrafanaAnnotation,
  GrafanaVariableRequest,
  GrafanaVariableResponse,

  // Streaming types
  SSEEventType,
  SSEEvent,
  SSEMetricEvent,
  SSEAlertEvent,
  SSEHeartbeatEvent,
  SSEErrorEvent,
  WSMessageType,
  WSMessage,
  WSSubscribeMessage,
  WSUnsubscribeMessage,
  WSMetricMessage,
  WSAlertMessage,
  WSErrorMessage,
  WSAckMessage,

  // JSON/CSV types
  JSONExportOptions,
  JSONExportPayload,
  JSONMetricSeries,
  CSVExportOptions,

  // Dashboard types
  DashboardConfig,
  DashboardPanelData,
  DashboardSnapshot,

  // API config types
  ExportAPIConfig,
} from './types'

// Constants
export { DEFAULT_EXPORT_API_CONFIG } from './types'

// =============================================================================
// Prometheus Export
// =============================================================================

export {
  AI_PROMETHEUS_METRICS,
  DEFAULT_LATENCY_BUCKETS,
  exportAIUsageToPrometheus,
  exportAIStatsToPrometheus,
  exportCompactionToPrometheus,
  combinePrometheusExports,
} from './prometheus'

// =============================================================================
// OpenTelemetry Export
// =============================================================================

export {
  exportAIUsageToOTLP,
  exportCompactionToOTLP,
  exportAIRequestsToOTLPTraces,
  mergeOTLPMetrics,
  mergeOTLPTraces,
} from './opentelemetry'

// =============================================================================
// Grafana Export
// =============================================================================

export {
  AI_GRAFANA_METRICS,
  COMPACTION_GRAFANA_METRICS,
  type AIGrafanaMetric,
  type CompactionGrafanaMetric,
  type GrafanaMetric,
  parseGrafanaTimeRange,
  handleGrafanaSearch,
  handleAIUsageQuery,
  handleCompactionQuery,
  handleAnnotationsQuery,
  handleTagKeys,
  handleTagValues,
  handleVariableQuery,
} from './grafana'

// =============================================================================
// Streaming Export (SSE/WebSocket)
// =============================================================================

export {
  // SSE functions
  formatSSEEvent,
  createSSEMetricEvent,
  createSSEAlertEvent,
  createSSEHeartbeat,
  createSSEError,
  compactionMetricsToSSE,
  aiUsageToSSE,
  createSSEResponse,
  createSSEStream,
  type SSEStreamConfig,

  // WebSocket functions
  parseWSMessage,
  formatWSMessage,
  createWSMetricMessage,
  createWSAlertMessage,
  createWSAckMessage,
  handleWSSubscribe,
  handleWSUnsubscribe,
  handleWSMessage,
  cleanupWSConnection,
  type WSSubscription,
  type WSConnectionState,
} from './streaming'

// =============================================================================
// JSON/CSV Export
// =============================================================================

export {
  // JSON export
  exportAIUsageToJSON,
  exportCompactionToJSON,
  exportAIRequestsToJSON,

  // CSV export
  exportAIUsageToCSV,
  exportAIRequestsToCSV,
  exportCompactionToCSV,

  // Response helpers
  createCSVResponse,
  createJSONResponse,
} from './json-csv'
