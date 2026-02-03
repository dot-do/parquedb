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

// Re-export AI observability module (includes anomaly detection and pricing service)
export {
  // AI Usage MV
  AIUsageMV,
  createAIUsageMV,
  DEFAULT_MODEL_PRICING,
  type ModelPricing,
  type PricingProvider,
  type TokenUsage,
  type AIRequest,
  type TimeGranularity,
  type AIUsageAggregate,
  type AIUsageSummary,
  type AIUsageMVConfig,
  type ResolvedAIUsageMVConfig,
  type AIUsageQueryOptions,
  type RefreshResult,

  // AI Requests MV
  AIRequestsMV,
  createAIRequestsMV,
  generateRequestId,
  type AIRequestType,
  type AIProvider,
  type AIRequestStatus,
  type AIRequestRecord,
  type RecordAIRequestInput,
  type AIRequestsQueryOptions,
  type AIRequestsStats,
  type AIRequestsMVConfig,
  type ResolvedAIRequestsMVConfig,
  type AIRequestsCleanupResult,

  // Generated Content MV
  GeneratedContentMV,
  createGeneratedContentMV,
  generateContentId,
  hashContent,
  type GeneratedContentType,
  type ContentClassification,
  type FinishReason,
  type GeneratedContentRecord,
  type RecordContentInput,
  type ContentQueryOptions,
  type ContentStats,
  type GeneratedContentMVConfig,
  type ResolvedContentMVConfig,
  type ContentCleanupResult,

  // Model Pricing Service - Auto-updating pricing with API refresh, caching, and enterprise overrides
  ModelPricingService,
  createModelPricingService,
  getDefaultPricingService,
  resetDefaultPricingService,
  type PricingSource,
  type PricingWithMetadata,
  type PricingCache,
  type PricingFetchResult,
  type ModelPricingServiceConfig,
  type ResolvedPricingServiceConfig,
  type PricingServiceStatus,

  // Anomaly Detection
  AnomalyDetector,
  createAnomalyDetector,
  createAnomalyDetectorWithWebhook,
  createObservationFromMetrics,
  DEFAULT_ANOMALY_THRESHOLDS,
  type AnomalySeverity,
  type AnomalyType,
  type AnomalyEvent,
  type WindowStats,
  type AnomalyObservation,
  type AnomalyThresholds,
  type AnomalyDetectorConfig,
  type ResolvedAnomalyDetectorConfig,
  type AnomalyDetectorStats,

  // Rate Limit Metrics - Token/cost rate limiting awareness for AI workloads
  RateLimitMetrics,
  createRateLimitMetrics,
  createRateLimitMetricsWithWebhook,
  DEFAULT_RATE_LIMIT_THRESHOLDS,
  type RateLimitAlertSeverity,
  type RateLimitMetricType,
  type ThresholdConfig,
  type RateLimitThresholds,
  type RateLimitAlert,
  type RateLimitObservation,
  type RateSnapshot,
  type AggregatedRateSnapshot,
  type RateLimitStats,
  type RateLimitMetricsConfig,
  type ResolvedRateLimitMetricsConfig,
} from './ai'

// Re-export export/dashboard module for observability data export
export {
  // Types
  type ExportFormat,
  type TimeRange,
  type Resolution,

  // Prometheus types and functions
  type PrometheusMetricDef,
  type PrometheusMetricValue,
  type PrometheusHistogramBucket,
  type PrometheusHistogram,
  AI_PROMETHEUS_METRICS,
  DEFAULT_LATENCY_BUCKETS,
  exportAIUsageToPrometheus,
  exportAIStatsToPrometheus,
  exportCompactionToPrometheus,
  combinePrometheusExports,

  // OpenTelemetry types and functions
  type OTelResourceAttributes,
  type OTelDataPoint,
  type OTelMetric,
  type OTelMetricsPayload,
  type OTelSpan,
  type OTelTracePayload,
  exportAIUsageToOTLP,
  exportCompactionToOTLP,
  exportAIRequestsToOTLPTraces,
  mergeOTLPMetrics,
  mergeOTLPTraces,

  // Grafana types and functions
  type GrafanaQueryRequest,
  type GrafanaQueryResponse,
  type GrafanaTimeSeriesResponse,
  type GrafanaTableResponse,
  type GrafanaAnnotationRequest,
  type GrafanaAnnotation,
  type GrafanaVariableRequest,
  type GrafanaVariableResponse,
  type AIGrafanaMetric,
  type CompactionGrafanaMetric,
  type GrafanaMetric,
  AI_GRAFANA_METRICS,
  COMPACTION_GRAFANA_METRICS,
  parseGrafanaTimeRange,
  handleGrafanaSearch,
  handleAIUsageQuery,
  handleCompactionQuery,
  handleAnnotationsQuery,
  handleTagKeys,
  handleTagValues,
  handleVariableQuery,

  // Streaming types and functions (SSE/WebSocket)
  type SSEEventType,
  type SSEEvent,
  type SSEMetricEvent,
  type SSEAlertEvent,
  type SSEHeartbeatEvent,
  type SSEErrorEvent,
  type WSMessageType,
  type WSMessage,
  type WSSubscribeMessage,
  type WSUnsubscribeMessage,
  type WSMetricMessage,
  type WSAlertMessage,
  type WSErrorMessage,
  type WSAckMessage,
  type SSEStreamConfig,
  type WSSubscription,
  type WSConnectionState,
  formatSSEEvent,
  createSSEMetricEvent,
  createSSEAlertEvent,
  createSSEHeartbeat,
  createSSEError,
  compactionMetricsToSSE,
  aiUsageToSSE,
  createSSEResponse,
  createSSEStream,
  parseWSMessage,
  formatWSMessage,
  createWSMetricMessage,
  createWSAlertMessage,
  createWSAckMessage,
  handleWSSubscribe,
  handleWSUnsubscribe,
  handleWSMessage,
  cleanupWSConnection,

  // JSON/CSV export types and functions
  type JSONExportOptions,
  type JSONExportPayload,
  type JSONMetricSeries,
  type CSVExportOptions,
  exportAIUsageToJSON,
  exportCompactionToJSON,
  exportAIRequestsToJSON,
  exportAIUsageToCSV,
  exportAIRequestsToCSV,
  exportCompactionToCSV,
  createCSVResponse,
  createJSONResponse,

  // Dashboard/API types
  type DashboardPanelData,
  type DashboardSnapshot,
  type ExportAPIConfig,
  DEFAULT_EXPORT_API_CONFIG,
} from './export'
