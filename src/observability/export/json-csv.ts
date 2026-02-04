/**
 * JSON/CSV Export
 *
 * Functions for exporting observability data in JSON and CSV formats.
 * Provides flexible export options for custom integrations.
 *
 * @module observability/export/json-csv
 */

import type {
  JSONExportOptions,
  JSONExportPayload,
  JSONMetricSeries,
  CSVExportOptions,
} from './types'
import type { AIUsageAggregate, AIUsageSummary } from '../ai/types'
import type { AIRequestRecord } from '../ai/AIRequestsMV'
import type { CompactionMetrics, MetricTimeSeries } from '../compaction/types'

// =============================================================================
// Constants
// =============================================================================

const EXPORT_VERSION = '1.0.0'

// =============================================================================
// JSON Export Functions
// =============================================================================

/**
 * Export AI usage data to JSON format
 */
export function exportAIUsageToJSON(
  aggregates: AIUsageAggregate[],
  _summary?: AIUsageSummary,
  options: JSONExportOptions = {}
): JSONExportPayload {
  const now = Date.now()

  // Group by model/provider
  const namespaces: Record<string, {
    latest?: Record<string, number> | undefined
    timeSeries?: Record<string, JSONMetricSeries> | undefined
    metadata?: Record<string, unknown> | undefined
  }> = {}

  // Apply time range filter
  let filteredAggregates = aggregates
  if (options.timeRange) {
    filteredAggregates = aggregates.filter(agg => {
      const aggTime = new Date(agg.dateKey).getTime()
      return aggTime >= options.timeRange!.from.getTime() &&
             aggTime <= options.timeRange!.to.getTime()
    })
  }

  // Group by model/provider
  const byModelProvider = new Map<string, AIUsageAggregate[]>()
  for (const agg of filteredAggregates) {
    const namespace = `${agg.modelId}/${agg.providerId}`

    // Apply namespace filter
    if (options.namespaces && !options.namespaces.includes(namespace)) {
      continue
    }

    const existing = byModelProvider.get(namespace) ?? []
    existing.push(agg)
    byModelProvider.set(namespace, existing)
  }

  // Build namespace data
  for (const [namespace, aggs] of byModelProvider) {
    const sortedAggs = [...aggs].sort((a, b) =>
      new Date(a.dateKey).getTime() - new Date(b.dateKey).getTime()
    )

    const lastAgg = sortedAggs[sortedAggs.length - 1]!

    // Latest metrics
    const latest: Record<string, number> = {
      requestCount: lastAgg.requestCount,
      successCount: lastAgg.successCount,
      errorCount: lastAgg.errorCount,
      cachedCount: lastAgg.cachedCount,
      totalPromptTokens: lastAgg.totalPromptTokens,
      totalCompletionTokens: lastAgg.totalCompletionTokens,
      totalTokens: lastAgg.totalTokens,
      estimatedTotalCost: lastAgg.estimatedTotalCost,
      avgLatencyMs: lastAgg.avgLatencyMs,
      errorRate: lastAgg.requestCount > 0 ? lastAgg.errorCount / lastAgg.requestCount : 0,
    }

    // Time series data
    const timeSeries: Record<string, JSONMetricSeries> = {}
    const metricNames = options.metrics ?? [
      'requestCount',
      'successCount',
      'errorCount',
      'totalTokens',
      'estimatedTotalCost',
      'avgLatencyMs',
    ]

    for (const metric of metricNames) {
      const dataPoints = sortedAggs
        .slice(-(options.maxDataPoints ?? 100))
        .map(agg => ({
          timestamp: new Date(agg.dateKey).getTime(),
          value: getAIAggregateMetricValue(metric, agg),
        }))

      timeSeries[metric] = {
        metric,
        namespace,
        dataPoints,
      }
    }

    namespaces[namespace] = {
      latest,
      timeSeries,
    }

    // Add metadata if requested
    if (options.includeMetadata) {
      namespaces[namespace].metadata = {
        modelId: lastAgg.modelId,
        providerId: lastAgg.providerId,
        granularity: lastAgg.granularity,
        firstSeen: sortedAggs[0]?.createdAt,
        lastUpdated: lastAgg.updatedAt,
      }
    }
  }

  return {
    timestamp: now,
    version: EXPORT_VERSION,
    exportedAt: new Date(now).toISOString(),
    options,
    namespaces,
  }
}

/**
 * Get metric value from AI aggregate
 */
function getAIAggregateMetricValue(metric: string, agg: AIUsageAggregate): number {
  switch (metric) {
    case 'requestCount':
      return agg.requestCount
    case 'successCount':
      return agg.successCount
    case 'errorCount':
      return agg.errorCount
    case 'cachedCount':
      return agg.cachedCount
    case 'totalPromptTokens':
      return agg.totalPromptTokens
    case 'totalCompletionTokens':
      return agg.totalCompletionTokens
    case 'totalTokens':
      return agg.totalTokens
    case 'estimatedInputCost':
      return agg.estimatedInputCost
    case 'estimatedOutputCost':
      return agg.estimatedOutputCost
    case 'estimatedTotalCost':
      return agg.estimatedTotalCost
    case 'avgLatencyMs':
      return agg.avgLatencyMs
    case 'minLatencyMs':
      return agg.minLatencyMs === Infinity ? 0 : agg.minLatencyMs
    case 'maxLatencyMs':
      return agg.maxLatencyMs
    case 'p50LatencyMs':
      return agg.p50LatencyMs ?? agg.avgLatencyMs
    case 'p90LatencyMs':
      return agg.p90LatencyMs ?? agg.avgLatencyMs
    case 'p95LatencyMs':
      return agg.p95LatencyMs ?? agg.maxLatencyMs
    case 'p99LatencyMs':
      return agg.p99LatencyMs ?? agg.maxLatencyMs
    case 'errorRate':
      return agg.requestCount > 0 ? agg.errorCount / agg.requestCount : 0
    case 'cacheHitRatio':
      return agg.requestCount > 0 ? agg.cachedCount / agg.requestCount : 0
    default:
      return 0
  }
}

/**
 * Export compaction metrics to JSON format
 */
export function exportCompactionToJSON(
  latestMetrics: Map<string, CompactionMetrics>,
  timeSeries?: Map<string, Map<string, MetricTimeSeries>>,
  options: JSONExportOptions = {}
): JSONExportPayload {
  const now = Date.now()

  const namespaces: Record<string, {
    latest?: Record<string, number> | undefined
    timeSeries?: Record<string, JSONMetricSeries> | undefined
    metadata?: Record<string, unknown> | undefined
  }> = {}

  for (const [namespace, metrics] of latestMetrics) {
    // Apply namespace filter
    if (options.namespaces && !options.namespaces.includes(namespace)) {
      continue
    }

    // Latest metrics
    const latest: Record<string, number> = {
      windows_pending: metrics.windows_pending,
      windows_processing: metrics.windows_processing,
      windows_dispatched: metrics.windows_dispatched,
      windows_stuck: metrics.windows_stuck,
      files_pending: metrics.files_pending,
      bytes_pending: metrics.bytes_pending,
      oldest_window_age_ms: metrics.oldest_window_age_ms,
      known_writers: metrics.known_writers,
      active_writers: metrics.active_writers,
    }

    namespaces[namespace] = { latest }

    // Time series data
    if (timeSeries) {
      const nsTimeSeries = timeSeries.get(namespace)
      if (nsTimeSeries) {
        const jsonTimeSeries: Record<string, JSONMetricSeries> = {}

        const metricNames = options.metrics ?? [
          'windows_pending',
          'windows_processing',
          'files_pending',
          'bytes_pending',
          'oldest_window_age_ms',
        ]

        for (const metric of metricNames) {
          const series = nsTimeSeries.get(metric)
          if (series) {
            let dataPoints = series.data.map(p => ({
              timestamp: p.timestamp,
              value: p.value,
            }))

            // Apply time range filter
            if (options.timeRange) {
              dataPoints = dataPoints.filter(p =>
                p.timestamp >= options.timeRange!.from.getTime() &&
                p.timestamp <= options.timeRange!.to.getTime()
              )
            }

            // Apply max data points limit
            if (options.maxDataPoints && dataPoints.length > options.maxDataPoints) {
              dataPoints = dataPoints.slice(-options.maxDataPoints)
            }

            jsonTimeSeries[metric] = {
              metric,
              namespace,
              dataPoints,
            }
          }
        }

        namespaces[namespace].timeSeries = jsonTimeSeries
      }
    }

    // Add metadata if requested
    if (options.includeMetadata) {
      namespaces[namespace].metadata = {
        lastUpdated: new Date(metrics.timestamp).toISOString(),
      }
    }
  }

  return {
    timestamp: now,
    version: EXPORT_VERSION,
    exportedAt: new Date(now).toISOString(),
    options,
    namespaces,
  }
}

/**
 * Export AI requests to JSON format
 */
export function exportAIRequestsToJSON(
  requests: AIRequestRecord[],
  options: JSONExportOptions = {}
): {
  timestamp: number
  version: string
  exportedAt: string
  count: number
  requests: AIRequestRecord[]
} {
  const now = Date.now()

  // Apply time range filter
  let filteredRequests = requests
  if (options.timeRange) {
    filteredRequests = requests.filter(r => {
      const reqTime = r.timestamp.getTime()
      return reqTime >= options.timeRange!.from.getTime() &&
             reqTime <= options.timeRange!.to.getTime()
    })
  }

  // Apply max data points limit
  if (options.maxDataPoints && filteredRequests.length > options.maxDataPoints) {
    filteredRequests = filteredRequests.slice(-options.maxDataPoints)
  }

  return {
    timestamp: now,
    version: EXPORT_VERSION,
    exportedAt: new Date(now).toISOString(),
    count: filteredRequests.length,
    requests: filteredRequests,
  }
}

// =============================================================================
// CSV Export Functions
// =============================================================================

/**
 * CSV row type
 */
type CSVRow = (string | number | boolean | null | undefined)[]

/**
 * Escape CSV value
 */
function escapeCSVValue(
  value: string | number | boolean | null | undefined,
  delimiter: string,
  quoteChar: string
): string {
  if (value === null || value === undefined) {
    return ''
  }

  const strValue = String(value)

  // Check if we need to quote
  const needsQuoting =
    strValue.includes(delimiter) ||
    strValue.includes(quoteChar) ||
    strValue.includes('\n') ||
    strValue.includes('\r')

  if (needsQuoting) {
    // Escape quotes by doubling them
    const escaped = strValue.replace(new RegExp(quoteChar, 'g'), quoteChar + quoteChar)
    return `${quoteChar}${escaped}${quoteChar}`
  }

  return strValue
}

/**
 * Format CSV row
 */
function formatCSVRow(
  row: CSVRow,
  options: CSVExportOptions
): string {
  const delimiter = options.delimiter ?? ','
  const quoteChar = options.quoteChar ?? '"'

  return row
    .map(value => escapeCSVValue(value, delimiter, quoteChar))
    .join(delimiter)
}

/**
 * Export AI usage aggregates to CSV format
 */
export function exportAIUsageToCSV(
  aggregates: AIUsageAggregate[],
  options: CSVExportOptions = {}
): string {
  const rows: string[] = []
  const delimiter = options.delimiter ?? ','

  // Header row
  if (options.includeHeader !== false) {
    const headers = [
      'dateKey',
      'modelId',
      'providerId',
      'granularity',
      'requestCount',
      'successCount',
      'errorCount',
      'cachedCount',
      'totalPromptTokens',
      'totalCompletionTokens',
      'totalTokens',
      'estimatedInputCost',
      'estimatedOutputCost',
      'estimatedTotalCost',
      'avgLatencyMs',
      'minLatencyMs',
      'maxLatencyMs',
      'errorRate',
    ]
    rows.push(headers.join(delimiter))
  }

  // Apply time range filter
  let filteredAggregates = aggregates
  if (options.timeRange) {
    filteredAggregates = aggregates.filter(agg => {
      const aggTime = new Date(agg.dateKey).getTime()
      return aggTime >= options.timeRange!.from.getTime() &&
             aggTime <= options.timeRange!.to.getTime()
    })
  }

  // Data rows
  for (const agg of filteredAggregates) {
    const row: CSVRow = [
      agg.dateKey,
      agg.modelId,
      agg.providerId,
      agg.granularity,
      agg.requestCount,
      agg.successCount,
      agg.errorCount,
      agg.cachedCount,
      agg.totalPromptTokens,
      agg.totalCompletionTokens,
      agg.totalTokens,
      agg.estimatedInputCost,
      agg.estimatedOutputCost,
      agg.estimatedTotalCost,
      agg.avgLatencyMs,
      agg.minLatencyMs === Infinity ? 0 : agg.minLatencyMs,
      agg.maxLatencyMs,
      agg.requestCount > 0 ? agg.errorCount / agg.requestCount : 0,
    ]
    rows.push(formatCSVRow(row, options))
  }

  return rows.join('\n')
}

/**
 * Export AI requests to CSV format
 */
export function exportAIRequestsToCSV(
  requests: AIRequestRecord[],
  options: CSVExportOptions = {}
): string {
  const rows: string[] = []
  const delimiter = options.delimiter ?? ','

  // Header row
  if (options.includeHeader !== false) {
    const headers = [
      'requestId',
      'timestamp',
      'modelId',
      'providerId',
      'requestType',
      'status',
      'latencyMs',
      'promptTokens',
      'completionTokens',
      'totalTokens',
      'estimatedCost',
      'cached',
      'finishReason',
      'error',
      'errorCode',
      'userId',
      'appId',
      'environment',
    ]
    rows.push(headers.join(delimiter))
  }

  // Apply time range filter
  let filteredRequests = requests
  if (options.timeRange) {
    filteredRequests = requests.filter(r => {
      const reqTime = r.timestamp.getTime()
      return reqTime >= options.timeRange!.from.getTime() &&
             reqTime <= options.timeRange!.to.getTime()
    })
  }

  // Data rows
  for (const req of filteredRequests) {
    const row: CSVRow = [
      req.requestId,
      req.timestamp.toISOString(),
      req.modelId,
      req.providerId,
      req.requestType,
      req.status,
      req.latencyMs,
      req.promptTokens,
      req.completionTokens,
      req.totalTokens,
      req.estimatedCost,
      req.cached,
      req.finishReason,
      req.error,
      req.errorCode,
      req.userId,
      req.appId,
      req.environment,
    ]
    rows.push(formatCSVRow(row, options))
  }

  return rows.join('\n')
}

/**
 * Export compaction metrics to CSV format
 */
export function exportCompactionToCSV(
  metrics: Map<string, CompactionMetrics> | CompactionMetrics[],
  options: CSVExportOptions = {}
): string {
  const rows: string[] = []
  const delimiter = options.delimiter ?? ','

  // Convert array to map if needed
  const metricsMap = Array.isArray(metrics)
    ? new Map(metrics.map(m => [m.namespace, m]))
    : metrics

  // Header row
  if (options.includeHeader !== false) {
    const headers = [
      'namespace',
      'timestamp',
      'windows_pending',
      'windows_processing',
      'windows_dispatched',
      'windows_stuck',
      'files_pending',
      'bytes_pending',
      'oldest_window_age_ms',
      'known_writers',
      'active_writers',
    ]
    rows.push(headers.join(delimiter))
  }

  // Data rows
  for (const [namespace, m] of metricsMap) {
    // Apply namespace filter
    if (options.namespaces && !options.namespaces.includes(namespace)) {
      continue
    }

    const row: CSVRow = [
      namespace,
      new Date(m.timestamp).toISOString(),
      m.windows_pending,
      m.windows_processing,
      m.windows_dispatched,
      m.windows_stuck,
      m.files_pending,
      m.bytes_pending,
      m.oldest_window_age_ms,
      m.known_writers,
      m.active_writers,
    ]
    rows.push(formatCSVRow(row, options))
  }

  return rows.join('\n')
}

/**
 * Create CSV response with appropriate headers
 */
export function createCSVResponse(
  csv: string,
  filename: string = 'export.csv'
): Response {
  return new Response(csv, {
    headers: {
      'Content-Type': 'text/csv; charset=utf-8',
      'Content-Disposition': `attachment; filename="${filename}"`,
      'Access-Control-Allow-Origin': '*',
    },
  })
}

/**
 * Create JSON response with appropriate headers
 */
export function createJSONResponse(
  data: unknown,
  pretty: boolean = false
): Response {
  const body = pretty ? JSON.stringify(data, null, 2) : JSON.stringify(data)
  return new Response(body, {
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
    },
  })
}
