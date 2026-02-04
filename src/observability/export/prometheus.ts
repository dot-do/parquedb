/**
 * Prometheus Export
 *
 * Functions for exporting observability data in Prometheus text format.
 * Supports both AI observability MVs and compaction metrics.
 *
 * @module observability/export/prometheus
 */

import type {
  PrometheusMetricDef,
  PrometheusHistogramBucket,
} from './types'
import type { AIUsageAggregate } from '../ai/types'
import type { CompactionMetrics } from '../compaction/types'

// =============================================================================
// Constants
// =============================================================================

/**
 * AI observability metrics definitions
 */
export const AI_PROMETHEUS_METRICS: PrometheusMetricDef[] = [
  // Request metrics
  {
    name: 'parquedb_ai_requests_total',
    help: 'Total number of AI API requests',
    type: 'counter',
    labels: ['model', 'provider', 'status'],
  },
  {
    name: 'parquedb_ai_requests_cached_total',
    help: 'Total number of cached AI API responses',
    type: 'counter',
    labels: ['model', 'provider'],
  },

  // Token metrics
  {
    name: 'parquedb_ai_tokens_total',
    help: 'Total tokens processed',
    type: 'counter',
    labels: ['model', 'provider', 'type'],
  },

  // Cost metrics
  {
    name: 'parquedb_ai_cost_dollars_total',
    help: 'Total estimated cost in USD',
    type: 'counter',
    labels: ['model', 'provider'],
  },

  // Latency metrics
  {
    name: 'parquedb_ai_request_duration_milliseconds',
    help: 'AI request duration in milliseconds',
    type: 'histogram',
    labels: ['model', 'provider'],
  },

  // Error rate gauge
  {
    name: 'parquedb_ai_error_rate',
    help: 'Current error rate (0-1)',
    type: 'gauge',
    labels: ['model', 'provider'],
  },

  // Cache hit ratio gauge
  {
    name: 'parquedb_ai_cache_hit_ratio',
    help: 'Current cache hit ratio (0-1)',
    type: 'gauge',
    labels: ['model', 'provider'],
  },
]

/**
 * Default histogram buckets for latency (in ms)
 */
export const DEFAULT_LATENCY_BUCKETS = [50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000]

// =============================================================================
// Prometheus Formatters
// =============================================================================

/**
 * Format a Prometheus metric line
 */
function formatMetricLine(
  name: string,
  labels: Record<string, string>,
  value: number,
  timestamp?: number
): string {
  const labelStr = Object.entries(labels)
    .map(([k, v]) => `${k}="${escapeLabel(v)}"`)
    .join(',')

  const labelPart = labelStr ? `{${labelStr}}` : ''
  const timestampPart = timestamp ? ` ${timestamp}` : ''

  return `${name}${labelPart} ${value}${timestampPart}`
}

/**
 * Escape label value for Prometheus format
 */
function escapeLabel(value: string): string {
  return value
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\n/g, '\\n')
}

/**
 * Format a histogram in Prometheus format
 * @internal Reserved for future use
 */
export function formatHistogram(
  name: string,
  labels: Record<string, string>,
  buckets: PrometheusHistogramBucket[],
  sum: number,
  count: number,
  timestamp?: number
): string[] {
  const lines: string[] = []
  let cumulativeCount = 0

  for (const bucket of buckets) {
    cumulativeCount += bucket.count
    const bucketLabels = { ...labels, le: String(bucket.le) }
    lines.push(formatMetricLine(`${name}_bucket`, bucketLabels, cumulativeCount, timestamp))
  }

  lines.push(formatMetricLine(`${name}_sum`, labels, sum, timestamp))
  lines.push(formatMetricLine(`${name}_count`, labels, count, timestamp))

  return lines
}

/**
 * Create histogram buckets from latency values
 * @internal Reserved for future use
 */
export function createHistogramBuckets(
  latencies: number[],
  bucketBoundaries: number[] = DEFAULT_LATENCY_BUCKETS
): PrometheusHistogramBucket[] {
  const buckets: PrometheusHistogramBucket[] = []

  for (const le of bucketBoundaries) {
    const count = latencies.filter(l => l <= le).length
    buckets.push({ le, count })
  }

  // Add +Inf bucket
  buckets.push({ le: '+Inf', count: latencies.length })

  return buckets
}

// =============================================================================
// Export Functions
// =============================================================================

/**
 * Export AI usage aggregates to Prometheus format
 *
 * @param aggregates - Array of usage aggregates
 * @param options - Export options
 * @returns Prometheus text format
 */
export function exportAIUsageToPrometheus(
  aggregates: AIUsageAggregate[],
  options: { timestamp?: number | undefined; includeMetadata?: boolean | undefined } = {}
): string {
  const lines: string[] = []
  const timestamp = options.timestamp ?? Date.now()

  // Group by model and provider for aggregation
  const byModelProvider = new Map<string, AIUsageAggregate[]>()
  for (const agg of aggregates) {
    const key = `${agg.modelId}:${agg.providerId}`
    const existing = byModelProvider.get(key) ?? []
    existing.push(agg)
    byModelProvider.set(key, existing)
  }

  // Request totals
  lines.push('# HELP parquedb_ai_requests_total Total number of AI API requests')
  lines.push('# TYPE parquedb_ai_requests_total counter')
  for (const [key, aggs] of byModelProvider) {
    const [model, provider] = key.split(':')
    // Note: total not used directly, but computed separately as success + errors
    const success = aggs.reduce((sum, a) => sum + a.successCount, 0)
    const errors = aggs.reduce((sum, a) => sum + a.errorCount, 0)

    lines.push(formatMetricLine('parquedb_ai_requests_total', { model: model!, provider: provider!, status: 'success' }, success, timestamp))
    lines.push(formatMetricLine('parquedb_ai_requests_total', { model: model!, provider: provider!, status: 'error' }, errors, timestamp))
  }

  // Cached totals
  lines.push('# HELP parquedb_ai_requests_cached_total Total number of cached AI API responses')
  lines.push('# TYPE parquedb_ai_requests_cached_total counter')
  for (const [key, aggs] of byModelProvider) {
    const [model, provider] = key.split(':')
    const cached = aggs.reduce((sum, a) => sum + a.cachedCount, 0)
    lines.push(formatMetricLine('parquedb_ai_requests_cached_total', { model: model!, provider: provider! }, cached, timestamp))
  }

  // Token totals
  lines.push('# HELP parquedb_ai_tokens_total Total tokens processed')
  lines.push('# TYPE parquedb_ai_tokens_total counter')
  for (const [key, aggs] of byModelProvider) {
    const [model, provider] = key.split(':')
    const prompt = aggs.reduce((sum, a) => sum + a.totalPromptTokens, 0)
    const completion = aggs.reduce((sum, a) => sum + a.totalCompletionTokens, 0)

    lines.push(formatMetricLine('parquedb_ai_tokens_total', { model: model!, provider: provider!, type: 'prompt' }, prompt, timestamp))
    lines.push(formatMetricLine('parquedb_ai_tokens_total', { model: model!, provider: provider!, type: 'completion' }, completion, timestamp))
  }

  // Cost totals
  lines.push('# HELP parquedb_ai_cost_dollars_total Total estimated cost in USD')
  lines.push('# TYPE parquedb_ai_cost_dollars_total counter')
  for (const [key, aggs] of byModelProvider) {
    const [model, provider] = key.split(':')
    const cost = aggs.reduce((sum, a) => sum + a.estimatedTotalCost, 0)
    lines.push(formatMetricLine('parquedb_ai_cost_dollars_total', { model: model!, provider: provider! }, cost, timestamp))
  }

  // Latency metrics
  lines.push('# HELP parquedb_ai_request_duration_milliseconds AI request duration in milliseconds')
  lines.push('# TYPE parquedb_ai_request_duration_milliseconds gauge')
  for (const [key, aggs] of byModelProvider) {
    const [model, provider] = key.split(':')
    const avgLatency = aggs.reduce((sum, a) => sum + a.avgLatencyMs * a.requestCount, 0) /
      Math.max(1, aggs.reduce((sum, a) => sum + a.requestCount, 0))
    lines.push(formatMetricLine('parquedb_ai_request_duration_milliseconds', { model: model!, provider: provider!, quantile: 'avg' }, avgLatency, timestamp))

    // Add percentiles if available
    const lastAgg = aggs[aggs.length - 1]
    if (lastAgg?.p50LatencyMs !== undefined) {
      lines.push(formatMetricLine('parquedb_ai_request_duration_milliseconds', { model: model!, provider: provider!, quantile: '0.5' }, lastAgg.p50LatencyMs, timestamp))
    }
    if (lastAgg?.p90LatencyMs !== undefined) {
      lines.push(formatMetricLine('parquedb_ai_request_duration_milliseconds', { model: model!, provider: provider!, quantile: '0.9' }, lastAgg.p90LatencyMs, timestamp))
    }
    if (lastAgg?.p95LatencyMs !== undefined) {
      lines.push(formatMetricLine('parquedb_ai_request_duration_milliseconds', { model: model!, provider: provider!, quantile: '0.95' }, lastAgg.p95LatencyMs, timestamp))
    }
    if (lastAgg?.p99LatencyMs !== undefined) {
      lines.push(formatMetricLine('parquedb_ai_request_duration_milliseconds', { model: model!, provider: provider!, quantile: '0.99' }, lastAgg.p99LatencyMs, timestamp))
    }
  }

  // Error rate gauge
  lines.push('# HELP parquedb_ai_error_rate Current error rate (0-1)')
  lines.push('# TYPE parquedb_ai_error_rate gauge')
  for (const [key, aggs] of byModelProvider) {
    const [model, provider] = key.split(':')
    const total = aggs.reduce((sum, a) => sum + a.requestCount, 0)
    const errors = aggs.reduce((sum, a) => sum + a.errorCount, 0)
    const errorRate = total > 0 ? errors / total : 0
    lines.push(formatMetricLine('parquedb_ai_error_rate', { model: model!, provider: provider! }, errorRate, timestamp))
  }

  return lines.join('\n')
}

/**
 * Export AI requests stats to Prometheus format
 *
 * @param stats - AI requests statistics
 * @param options - Export options
 * @returns Prometheus text format
 */
export function exportAIStatsToPrometheus(
  stats: AIRequestsStats,
  options: { timestamp?: number | undefined } = {}
): string {
  const lines: string[] = []
  const timestamp = options.timestamp ?? Date.now()

  // Summary metrics
  lines.push('# HELP parquedb_ai_requests_total Total number of AI API requests')
  lines.push('# TYPE parquedb_ai_requests_total counter')
  lines.push(formatMetricLine('parquedb_ai_requests_total', { status: 'total' }, stats.totalRequests, timestamp))
  lines.push(formatMetricLine('parquedb_ai_requests_total', { status: 'success' }, stats.successCount, timestamp))
  lines.push(formatMetricLine('parquedb_ai_requests_total', { status: 'error' }, stats.errorCount, timestamp))

  // By model
  for (const [model, data] of Object.entries(stats.byModel)) {
    lines.push(formatMetricLine('parquedb_ai_requests_total', { model, status: 'all' }, data.count, timestamp))
    lines.push(formatMetricLine('parquedb_ai_cost_dollars_total', { model }, data.cost, timestamp))
    lines.push(formatMetricLine('parquedb_ai_request_duration_milliseconds', { model, quantile: 'avg' }, data.avgLatency, timestamp))
  }

  // By provider
  for (const [provider, data] of Object.entries(stats.byProvider)) {
    lines.push(formatMetricLine('parquedb_ai_requests_total', { provider, status: 'all' }, data.count, timestamp))
    lines.push(formatMetricLine('parquedb_ai_cost_dollars_total', { provider }, data.cost, timestamp))
    lines.push(formatMetricLine('parquedb_ai_request_duration_milliseconds', { provider, quantile: 'avg' }, data.avgLatency, timestamp))
  }

  // Token totals
  lines.push('# HELP parquedb_ai_tokens_total Total tokens processed')
  lines.push('# TYPE parquedb_ai_tokens_total counter')
  lines.push(formatMetricLine('parquedb_ai_tokens_total', { type: 'prompt' }, stats.tokens.totalPromptTokens, timestamp))
  lines.push(formatMetricLine('parquedb_ai_tokens_total', { type: 'completion' }, stats.tokens.totalCompletionTokens, timestamp))
  lines.push(formatMetricLine('parquedb_ai_tokens_total', { type: 'total' }, stats.tokens.totalTokens, timestamp))

  // Cost metrics
  lines.push('# HELP parquedb_ai_cost_dollars_total Total estimated cost in USD')
  lines.push('# TYPE parquedb_ai_cost_dollars_total counter')
  lines.push(formatMetricLine('parquedb_ai_cost_dollars_total', {}, stats.cost.totalCost, timestamp))
  lines.push(formatMetricLine('parquedb_ai_cache_savings_dollars_total', {}, stats.cost.cacheSavings, timestamp))

  // Latency metrics
  lines.push('# HELP parquedb_ai_request_duration_milliseconds AI request duration in milliseconds')
  lines.push('# TYPE parquedb_ai_request_duration_milliseconds summary')
  lines.push(formatMetricLine('parquedb_ai_request_duration_milliseconds', { quantile: '0.5' }, stats.latency.p50, timestamp))
  lines.push(formatMetricLine('parquedb_ai_request_duration_milliseconds', { quantile: '0.95' }, stats.latency.p95, timestamp))
  lines.push(formatMetricLine('parquedb_ai_request_duration_milliseconds', { quantile: '0.99' }, stats.latency.p99, timestamp))
  lines.push(formatMetricLine('parquedb_ai_request_duration_milliseconds_min', {}, stats.latency.min, timestamp))
  lines.push(formatMetricLine('parquedb_ai_request_duration_milliseconds_max', {}, stats.latency.max, timestamp))
  lines.push(formatMetricLine('parquedb_ai_request_duration_milliseconds_avg', {}, stats.latency.avg, timestamp))

  // Ratios
  lines.push('# HELP parquedb_ai_error_rate Current error rate (0-1)')
  lines.push('# TYPE parquedb_ai_error_rate gauge')
  lines.push(formatMetricLine('parquedb_ai_error_rate', {}, stats.errorRate, timestamp))

  lines.push('# HELP parquedb_ai_cache_hit_ratio Current cache hit ratio (0-1)')
  lines.push('# TYPE parquedb_ai_cache_hit_ratio gauge')
  lines.push(formatMetricLine('parquedb_ai_cache_hit_ratio', {}, stats.cacheHitRatio, timestamp))

  return lines.join('\n')
}

/**
 * Export compaction metrics to Prometheus format
 *
 * @param metrics - Map of namespace to compaction metrics
 * @param options - Export options
 * @returns Prometheus text format
 */
export function exportCompactionToPrometheus(
  metrics: Map<string, CompactionMetrics> | CompactionMetrics[],
  options: { timestamp?: number | undefined; namespaces?: string[] | undefined } = {}
): string {
  const lines: string[] = []
  const timestamp = options.timestamp ?? Date.now()

  // Convert array to map if needed
  const metricsMap = Array.isArray(metrics)
    ? new Map(metrics.map(m => [m.namespace, m]))
    : metrics

  // Filter namespaces if specified
  const filteredMetrics = options.namespaces
    ? new Map(Array.from(metricsMap.entries()).filter(([ns]) => options.namespaces!.includes(ns)))
    : metricsMap

  // Windows pending
  lines.push('# HELP parquedb_compaction_windows_pending Number of compaction windows pending')
  lines.push('# TYPE parquedb_compaction_windows_pending gauge')
  for (const [namespace, m] of filteredMetrics) {
    lines.push(formatMetricLine('parquedb_compaction_windows_pending', { namespace }, m.windows_pending, timestamp))
  }

  // Windows processing
  lines.push('# HELP parquedb_compaction_windows_processing Number of compaction windows being processed')
  lines.push('# TYPE parquedb_compaction_windows_processing gauge')
  for (const [namespace, m] of filteredMetrics) {
    lines.push(formatMetricLine('parquedb_compaction_windows_processing', { namespace }, m.windows_processing, timestamp))
  }

  // Windows dispatched
  lines.push('# HELP parquedb_compaction_windows_dispatched Number of compaction windows dispatched')
  lines.push('# TYPE parquedb_compaction_windows_dispatched gauge')
  for (const [namespace, m] of filteredMetrics) {
    lines.push(formatMetricLine('parquedb_compaction_windows_dispatched', { namespace }, m.windows_dispatched, timestamp))
  }

  // Files pending
  lines.push('# HELP parquedb_compaction_files_pending Total files pending compaction')
  lines.push('# TYPE parquedb_compaction_files_pending gauge')
  for (const [namespace, m] of filteredMetrics) {
    lines.push(formatMetricLine('parquedb_compaction_files_pending', { namespace }, m.files_pending, timestamp))
  }

  // Bytes pending
  lines.push('# HELP parquedb_compaction_bytes_pending Total bytes pending compaction')
  lines.push('# TYPE parquedb_compaction_bytes_pending gauge')
  for (const [namespace, m] of filteredMetrics) {
    lines.push(formatMetricLine('parquedb_compaction_bytes_pending', { namespace }, m.bytes_pending, timestamp))
  }

  // Oldest window age (in seconds)
  lines.push('# HELP parquedb_compaction_oldest_window_age_seconds Age of oldest pending window in seconds')
  lines.push('# TYPE parquedb_compaction_oldest_window_age_seconds gauge')
  for (const [namespace, m] of filteredMetrics) {
    const ageSeconds = Math.round(m.oldest_window_age_ms / 1000)
    lines.push(formatMetricLine('parquedb_compaction_oldest_window_age_seconds', { namespace }, ageSeconds, timestamp))
  }

  // Writers
  lines.push('# HELP parquedb_compaction_known_writers Number of known writers')
  lines.push('# TYPE parquedb_compaction_known_writers gauge')
  for (const [namespace, m] of filteredMetrics) {
    lines.push(formatMetricLine('parquedb_compaction_known_writers', { namespace }, m.known_writers, timestamp))
  }

  lines.push('# HELP parquedb_compaction_active_writers Number of currently active writers')
  lines.push('# TYPE parquedb_compaction_active_writers gauge')
  for (const [namespace, m] of filteredMetrics) {
    lines.push(formatMetricLine('parquedb_compaction_active_writers', { namespace }, m.active_writers, timestamp))
  }

  // Stuck windows
  lines.push('# HELP parquedb_compaction_windows_stuck Number of windows stuck in processing')
  lines.push('# TYPE parquedb_compaction_windows_stuck gauge')
  for (const [namespace, m] of filteredMetrics) {
    lines.push(formatMetricLine('parquedb_compaction_windows_stuck', { namespace }, m.windows_stuck, timestamp))
  }

  return lines.join('\n')
}

/**
 * Combine multiple Prometheus exports
 */
export function combinePrometheusExports(...exports: string[]): string {
  return exports.filter(Boolean).join('\n\n')
}
