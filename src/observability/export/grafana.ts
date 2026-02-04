/**
 * Grafana Export
 *
 * Functions for providing Grafana-compatible data source endpoints.
 * Implements the Grafana SimpleJSON datasource protocol.
 *
 * @see https://grafana.com/grafana/plugins/grafana-simple-json-datasource/
 *
 * @module observability/export/grafana
 */

import type {
  GrafanaQueryRequest,
  GrafanaQueryResponse,
  GrafanaTimeSeriesResponse,
  GrafanaTableResponse,
  GrafanaAnnotationRequest,
  GrafanaAnnotation,
  GrafanaVariableRequest,
  GrafanaVariableResponse,
  TimeRange,
} from './types'
import type { AIUsageAggregate } from '../ai/types'
import type { CompactionMetrics, MetricTimeSeries } from '../compaction/types'

// =============================================================================
// Available Metrics
// =============================================================================

/**
 * Available AI metrics for Grafana
 */
export const AI_GRAFANA_METRICS = [
  'ai.requests.total',
  'ai.requests.success',
  'ai.requests.error',
  'ai.requests.cached',
  'ai.tokens.prompt',
  'ai.tokens.completion',
  'ai.tokens.total',
  'ai.cost.total',
  'ai.cost.input',
  'ai.cost.output',
  'ai.latency.avg',
  'ai.latency.p50',
  'ai.latency.p95',
  'ai.latency.p99',
  'ai.latency.min',
  'ai.latency.max',
  'ai.error_rate',
  'ai.cache_hit_ratio',
] as const

/**
 * Available compaction metrics for Grafana
 */
export const COMPACTION_GRAFANA_METRICS = [
  'compaction.windows.pending',
  'compaction.windows.processing',
  'compaction.windows.dispatched',
  'compaction.windows.stuck',
  'compaction.files.pending',
  'compaction.bytes.pending',
  'compaction.oldest_window_age',
  'compaction.known_writers',
  'compaction.active_writers',
] as const

export type AIGrafanaMetric = (typeof AI_GRAFANA_METRICS)[number]
export type CompactionGrafanaMetric = (typeof COMPACTION_GRAFANA_METRICS)[number]
export type GrafanaMetric = AIGrafanaMetric | CompactionGrafanaMetric

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Parse Grafana time range to Date objects
 */
export function parseGrafanaTimeRange(range: { from: string; to: string }): TimeRange {
  return {
    from: new Date(range.from),
    to: new Date(range.to),
  }
}

/**
 * Format timestamp for Grafana (milliseconds)
 * @internal Reserved for future use
 */
export function toGrafanaTimestamp(date: Date): number {
  return date.getTime()
}

/**
 * Parse metric target with optional filters
 * e.g., "ai.requests.total{model=gpt-4,provider=openai}"
 */
function parseMetricTarget(target: string): {
  metric: string
  filters: Record<string, string>
} {
  const match = target.match(/^([^{]+)(?:\{([^}]+)\})?$/)
  if (!match) {
    return { metric: target, filters: {} }
  }

  const metric = match[1]!
  const filterStr = match[2]

  const filters: Record<string, string> = {}
  if (filterStr) {
    for (const pair of filterStr.split(',')) {
      const [key, value] = pair.split('=')
      if (key && value) {
        filters[key.trim()] = value.trim()
      }
    }
  }

  return { metric, filters }
}

// =============================================================================
// Query Handlers
// =============================================================================

/**
 * Handle Grafana search request (returns available metrics)
 */
export function handleGrafanaSearch(): string[] {
  return [...AI_GRAFANA_METRICS, ...COMPACTION_GRAFANA_METRICS]
}

/**
 * Handle Grafana query request for AI usage data
 */
export function handleAIUsageQuery(
  request: GrafanaQueryRequest,
  aggregates: AIUsageAggregate[],
  _timeSeries?: Map<string, MetricTimeSeries>
): GrafanaQueryResponse[] {
  const responses: GrafanaQueryResponse[] = []
  const timeRange = parseGrafanaTimeRange(request.range)

  // Filter aggregates by time range
  const filteredAggregates = aggregates.filter(agg => {
    const aggDate = new Date(agg.dateKey)
    return aggDate >= timeRange.from && aggDate <= timeRange.to
  })

  for (const target of request.targets) {
    const { metric, filters } = parseMetricTarget(target.target)

    // Apply filters
    let filtered = filteredAggregates
    if (filters.model) {
      filtered = filtered.filter(a => a.modelId === filters.model)
    }
    if (filters.provider) {
      filtered = filtered.filter(a => a.providerId === filters.provider)
    }

    const format = target.format ?? target.type ?? 'time_series'

    if (format === 'table') {
      // Table format
      const tableResponse = createAIUsageTableResponse(metric, filtered, target.refId)
      responses.push(tableResponse)
    } else {
      // Time series format
      const timeSeriesResponse = createAIUsageTimeSeriesResponse(
        metric,
        filtered,
        target.refId,
        filters
      )
      responses.push(timeSeriesResponse)
    }
  }

  return responses
}

/**
 * Create time series response for AI usage metrics
 */
function createAIUsageTimeSeriesResponse(
  metric: string,
  aggregates: AIUsageAggregate[],
  refId?: string,
  filters?: Record<string, string>
): GrafanaTimeSeriesResponse {
  // Group by model/provider if no filter specified
  const grouped = new Map<string, Array<[number, number]>>()

  for (const agg of aggregates) {
    const key = filters?.model || filters?.provider
      ? metric
      : `${metric}{model=${agg.modelId},provider=${agg.providerId}}`

    const existing = grouped.get(key) ?? []
    const timestamp = new Date(agg.dateKey).getTime()
    const value = getAIMetricValue(metric, agg)

    existing.push([value, timestamp])
    grouped.set(key, existing)
  }

  // If multiple groups, create first one (for simplicity)
  // In a full implementation, would return multiple series
  const [targetName, datapoints] = Array.from(grouped.entries())[0] ?? [metric, []]

  // Sort by timestamp
  datapoints.sort((a, b) => a[1] - b[1])

  return {
    target: targetName,
    datapoints,
    refId,
  }
}

/**
 * Create table response for AI usage metrics
 */
function createAIUsageTableResponse(
  metric: string,
  aggregates: AIUsageAggregate[],
  refId?: string
): GrafanaTableResponse {
  const columns = [
    { text: 'Time', type: 'time' },
    { text: 'Model', type: 'string' },
    { text: 'Provider', type: 'string' },
    { text: 'Value', type: 'number' },
  ]

  const rows = aggregates.map(agg => [
    new Date(agg.dateKey).getTime(),
    agg.modelId,
    agg.providerId,
    getAIMetricValue(metric, agg),
  ])

  return {
    columns,
    rows,
    type: 'table',
    refId,
  }
}

/**
 * Get AI metric value from aggregate
 */
function getAIMetricValue(metric: string, agg: AIUsageAggregate): number {
  switch (metric) {
    case 'ai.requests.total':
      return agg.requestCount
    case 'ai.requests.success':
      return agg.successCount
    case 'ai.requests.error':
      return agg.errorCount
    case 'ai.requests.cached':
      return agg.cachedCount
    case 'ai.tokens.prompt':
      return agg.totalPromptTokens
    case 'ai.tokens.completion':
      return agg.totalCompletionTokens
    case 'ai.tokens.total':
      return agg.totalTokens
    case 'ai.cost.total':
      return agg.estimatedTotalCost
    case 'ai.cost.input':
      return agg.estimatedInputCost
    case 'ai.cost.output':
      return agg.estimatedOutputCost
    case 'ai.latency.avg':
      return agg.avgLatencyMs
    case 'ai.latency.p50':
      return agg.p50LatencyMs ?? agg.avgLatencyMs
    case 'ai.latency.p95':
      return agg.p95LatencyMs ?? agg.maxLatencyMs
    case 'ai.latency.p99':
      return agg.p99LatencyMs ?? agg.maxLatencyMs
    case 'ai.latency.min':
      return agg.minLatencyMs === Infinity ? 0 : agg.minLatencyMs
    case 'ai.latency.max':
      return agg.maxLatencyMs
    case 'ai.error_rate':
      return agg.requestCount > 0 ? agg.errorCount / agg.requestCount : 0
    case 'ai.cache_hit_ratio':
      return agg.requestCount > 0 ? agg.cachedCount / agg.requestCount : 0
    default:
      return 0
  }
}

/**
 * Handle Grafana query request for compaction data
 */
export function handleCompactionQuery(
  request: GrafanaQueryRequest,
  latestMetrics: Map<string, CompactionMetrics>,
  timeSeries?: Map<string, Map<string, MetricTimeSeries>>
): GrafanaQueryResponse[] {
  const responses: GrafanaQueryResponse[] = []
  const timeRange = parseGrafanaTimeRange(request.range)

  for (const target of request.targets) {
    const { metric, filters } = parseMetricTarget(target.target)

    const format = target.format ?? target.type ?? 'time_series'

    if (format === 'table') {
      // Table format
      const tableResponse = createCompactionTableResponse(metric, latestMetrics, filters, target.refId)
      responses.push(tableResponse)
    } else {
      // Time series format
      if (timeSeries) {
        const timeSeriesResponse = createCompactionTimeSeriesResponse(
          metric,
          timeSeries,
          filters,
          timeRange,
          target.refId
        )
        responses.push(timeSeriesResponse)
      } else {
        // No time series data, return latest as single point
        const datapoints: Array<[number, number]> = []
        for (const [ns, m] of latestMetrics) {
          if (!filters.namespace || filters.namespace === ns) {
            const value = getCompactionMetricValue(metric, m)
            datapoints.push([value, m.timestamp])
          }
        }
        responses.push({
          target: metric,
          datapoints,
          refId: target.refId,
        })
      }
    }
  }

  return responses
}

/**
 * Create time series response for compaction metrics
 */
function createCompactionTimeSeriesResponse(
  metric: string,
  timeSeries: Map<string, Map<string, MetricTimeSeries>>,
  filters: Record<string, string>,
  timeRange: TimeRange,
  refId?: string
): GrafanaTimeSeriesResponse {
  const datapoints: Array<[number, number]> = []

  // Map Grafana metric name to internal metric name
  const internalMetricName = metric.replace('compaction.', '').replace(/\./g, '_')

  for (const [namespace, nsTimeSeries] of timeSeries) {
    if (filters.namespace && filters.namespace !== namespace) {
      continue
    }

    const series = nsTimeSeries.get(internalMetricName)
    if (series) {
      for (const point of series.data) {
        if (point.timestamp >= timeRange.from.getTime() && point.timestamp <= timeRange.to.getTime()) {
          datapoints.push([point.value, point.timestamp])
        }
      }
    }
  }

  // Sort by timestamp
  datapoints.sort((a, b) => a[1] - b[1])

  const targetName = filters.namespace ? `${metric}{namespace=${filters.namespace}}` : metric

  return {
    target: targetName,
    datapoints,
    refId,
  }
}

/**
 * Create table response for compaction metrics
 */
function createCompactionTableResponse(
  metric: string,
  latestMetrics: Map<string, CompactionMetrics>,
  filters: Record<string, string>,
  refId?: string
): GrafanaTableResponse {
  const columns = [
    { text: 'Time', type: 'time' },
    { text: 'Namespace', type: 'string' },
    { text: 'Value', type: 'number' },
  ]

  const rows: unknown[][] = []
  for (const [namespace, m] of latestMetrics) {
    if (filters.namespace && filters.namespace !== namespace) {
      continue
    }
    rows.push([m.timestamp, namespace, getCompactionMetricValue(metric, m)])
  }

  return {
    columns,
    rows,
    type: 'table',
    refId,
  }
}

/**
 * Get compaction metric value from metrics object
 */
function getCompactionMetricValue(metric: string, m: CompactionMetrics): number {
  switch (metric) {
    case 'compaction.windows.pending':
      return m.windows_pending
    case 'compaction.windows.processing':
      return m.windows_processing
    case 'compaction.windows.dispatched':
      return m.windows_dispatched
    case 'compaction.windows.stuck':
      return m.windows_stuck
    case 'compaction.files.pending':
      return m.files_pending
    case 'compaction.bytes.pending':
      return m.bytes_pending
    case 'compaction.oldest_window_age':
      return m.oldest_window_age_ms / 1000 // Return in seconds
    case 'compaction.known_writers':
      return m.known_writers
    case 'compaction.active_writers':
      return m.active_writers
    default:
      return 0
  }
}

/**
 * Handle Grafana annotations request
 */
export function handleAnnotationsQuery(
  request: GrafanaAnnotationRequest,
  alerts?: Array<{
    timestamp: Date
    severity: string
    title: string
    message?: string | undefined
    namespace?: string | undefined
  }>
): GrafanaAnnotation[] {
  if (!alerts) {
    return []
  }

  const timeRange = parseGrafanaTimeRange(request.range)
  const query = request.annotation.query?.toLowerCase()

  return alerts
    .filter(alert => {
      const time = alert.timestamp.getTime()
      const inRange = time >= timeRange.from.getTime() && time <= timeRange.to.getTime()
      const matchesQuery = !query || alert.severity.toLowerCase().includes(query)
      return inRange && matchesQuery
    })
    .map(alert => ({
      time: alert.timestamp.getTime(),
      title: alert.title,
      text: alert.message,
      tags: [alert.severity, alert.namespace].filter(Boolean) as string[],
    }))
}

/**
 * Handle Grafana tag-keys request (for ad-hoc filters)
 */
export function handleTagKeys(): Array<{ type: string; text: string }> {
  return [
    { type: 'string', text: 'model' },
    { type: 'string', text: 'provider' },
    { type: 'string', text: 'namespace' },
    { type: 'string', text: 'status' },
  ]
}

/**
 * Handle Grafana tag-values request (for ad-hoc filters)
 */
export function handleTagValues(
  key: string,
  aggregates: AIUsageAggregate[],
  compactionMetrics: Map<string, CompactionMetrics>
): Array<{ text: string }> {
  const values = new Set<string>()

  switch (key) {
    case 'model':
      for (const agg of aggregates) {
        values.add(agg.modelId)
      }
      break
    case 'provider':
      for (const agg of aggregates) {
        values.add(agg.providerId)
      }
      break
    case 'namespace':
      for (const ns of compactionMetrics.keys()) {
        values.add(ns)
      }
      break
    case 'status':
      values.add('success')
      values.add('error')
      values.add('cached')
      break
  }

  return Array.from(values).map(text => ({ text }))
}

/**
 * Handle Grafana variable request
 */
export function handleVariableQuery(
  request: GrafanaVariableRequest,
  aggregates: AIUsageAggregate[],
  compactionMetrics: Map<string, CompactionMetrics>
): GrafanaVariableResponse {
  const target = request.payload.target.toLowerCase()

  if (target === 'models' || target === 'model') {
    const models = new Set(aggregates.map(a => a.modelId))
    return Array.from(models).map(m => ({ __text: m, __value: m }))
  }

  if (target === 'providers' || target === 'provider') {
    const providers = new Set(aggregates.map(a => a.providerId))
    return Array.from(providers).map(p => ({ __text: p, __value: p }))
  }

  if (target === 'namespaces' || target === 'namespace') {
    return Array.from(compactionMetrics.keys()).map(ns => ({ __text: ns, __value: ns }))
  }

  if (target === 'metrics') {
    return handleGrafanaSearch().map(m => ({ __text: m, __value: m }))
  }

  return []
}
