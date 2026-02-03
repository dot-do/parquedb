/**
 * OpenTelemetry Export
 *
 * Functions for exporting observability data in OpenTelemetry Protocol (OTLP) format.
 * Supports both metrics and traces export compatible with OTLP/JSON.
 *
 * @module observability/export/opentelemetry
 */

import type {
  OTelResourceAttributes,
  OTelDataPoint,
  OTelMetric,
  OTelMetricsPayload,
  OTelSpan,
  OTelTracePayload,
} from './types'
import type { AIUsageAggregate } from '../ai/types'
import type { AIRequestRecord } from '../ai/AIRequestsMV'
import type { CompactionMetrics } from '../compaction/types'

// =============================================================================
// Constants
// =============================================================================

const SERVICE_NAME = 'parquedb'
const SERVICE_VERSION = '1.0.0'
const INSTRUMENTATION_SCOPE = 'parquedb.observability'

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Convert resource attributes to OTLP format
 */
function resourceAttributesToOTLP(
  attrs: OTelResourceAttributes
): Array<{ key: string; value: { stringValue?: string; intValue?: number; boolValue?: boolean } }> {
  return Object.entries(attrs)
    .filter(([, v]) => v !== undefined)
    .map(([key, value]) => {
      if (typeof value === 'string') {
        return { key, value: { stringValue: value } }
      } else if (typeof value === 'number') {
        return { key, value: { intValue: value } }
      } else if (typeof value === 'boolean') {
        return { key, value: { boolValue: value } }
      }
      return { key, value: { stringValue: String(value) } }
    })
}

/**
 * Convert attributes map to OTLP format
 */
function attributesToOTLP(
  attrs: Record<string, string | number | boolean>
): Array<{ key: string; value: { stringValue?: string; intValue?: number; boolValue?: boolean } }> {
  return Object.entries(attrs).map(([key, value]) => {
    if (typeof value === 'string') {
      return { key, value: { stringValue: value } }
    } else if (typeof value === 'number') {
      return { key, value: { intValue: value } }
    } else if (typeof value === 'boolean') {
      return { key, value: { boolValue: value } }
    }
    return { key, value: { stringValue: String(value) } }
  })
}

/**
 * Generate a random trace ID (32 hex chars)
 */
function generateTraceId(): string {
  const bytes = new Uint8Array(16)
  crypto.getRandomValues(bytes)
  return Array.from(bytes)
    .map(b => b.toString(16).padStart(2, '0'))
    .join('')
}

/**
 * Generate a random span ID (16 hex chars)
 */
function generateSpanId(): string {
  const bytes = new Uint8Array(8)
  crypto.getRandomValues(bytes)
  return Array.from(bytes)
    .map(b => b.toString(16).padStart(2, '0'))
    .join('')
}

/**
 * Convert Date to Unix nanoseconds
 */
function dateToUnixNano(date: Date): number {
  return date.getTime() * 1_000_000
}

// =============================================================================
// Metrics Export
// =============================================================================

/**
 * Export AI usage aggregates to OpenTelemetry metrics format
 *
 * @param aggregates - Array of usage aggregates
 * @param options - Export options
 * @returns OTLP metrics payload
 */
export function exportAIUsageToOTLP(
  aggregates: AIUsageAggregate[],
  options: {
    resourceAttributes?: Partial<OTelResourceAttributes>
    environment?: string
  } = {}
): OTelMetricsPayload {
  const resourceAttrs: OTelResourceAttributes = {
    'service.name': SERVICE_NAME,
    'service.version': SERVICE_VERSION,
    'deployment.environment': options.environment ?? 'production',
    ...options.resourceAttributes,
  }

  const metrics: OTelMetric[] = []
  const now = Date.now() * 1_000_000 // Convert to nanoseconds

  // Group by model and provider
  const byModelProvider = new Map<string, AIUsageAggregate[]>()
  for (const agg of aggregates) {
    const key = `${agg.modelId}:${agg.providerId}`
    const existing = byModelProvider.get(key) ?? []
    existing.push(agg)
    byModelProvider.set(key, existing)
  }

  // Request count metric
  const requestCountDataPoints: OTelDataPoint[] = []
  for (const [, aggs] of byModelProvider) {
    const lastAgg = aggs[aggs.length - 1]!
    requestCountDataPoints.push({
      attributes: {
        model: lastAgg.modelId,
        provider: lastAgg.providerId,
      },
      startTimeUnixNano: dateToUnixNano(lastAgg.createdAt),
      timeUnixNano: now,
      value: aggs.reduce((sum, a) => sum + a.requestCount, 0),
    })
  }

  metrics.push({
    name: 'parquedb.ai.requests',
    description: 'Total number of AI API requests',
    unit: '{request}',
    data: {
      dataPoints: requestCountDataPoints,
      aggregationTemporality: 'AGGREGATION_TEMPORALITY_CUMULATIVE',
      isMonotonic: true,
    },
  })

  // Token count metric
  const tokenDataPoints: OTelDataPoint[] = []
  for (const [, aggs] of byModelProvider) {
    const lastAgg = aggs[aggs.length - 1]!
    tokenDataPoints.push({
      attributes: {
        model: lastAgg.modelId,
        provider: lastAgg.providerId,
        token_type: 'prompt',
      },
      startTimeUnixNano: dateToUnixNano(lastAgg.createdAt),
      timeUnixNano: now,
      value: aggs.reduce((sum, a) => sum + a.totalPromptTokens, 0),
    })
    tokenDataPoints.push({
      attributes: {
        model: lastAgg.modelId,
        provider: lastAgg.providerId,
        token_type: 'completion',
      },
      startTimeUnixNano: dateToUnixNano(lastAgg.createdAt),
      timeUnixNano: now,
      value: aggs.reduce((sum, a) => sum + a.totalCompletionTokens, 0),
    })
  }

  metrics.push({
    name: 'parquedb.ai.tokens',
    description: 'Total tokens processed',
    unit: '{token}',
    data: {
      dataPoints: tokenDataPoints,
      aggregationTemporality: 'AGGREGATION_TEMPORALITY_CUMULATIVE',
      isMonotonic: true,
    },
  })

  // Cost metric
  const costDataPoints: OTelDataPoint[] = []
  for (const [, aggs] of byModelProvider) {
    const lastAgg = aggs[aggs.length - 1]!
    costDataPoints.push({
      attributes: {
        model: lastAgg.modelId,
        provider: lastAgg.providerId,
      },
      startTimeUnixNano: dateToUnixNano(lastAgg.createdAt),
      timeUnixNano: now,
      value: aggs.reduce((sum, a) => sum + a.estimatedTotalCost, 0),
    })
  }

  metrics.push({
    name: 'parquedb.ai.cost',
    description: 'Total estimated cost in USD',
    unit: 'USD',
    data: {
      dataPoints: costDataPoints,
      aggregationTemporality: 'AGGREGATION_TEMPORALITY_CUMULATIVE',
      isMonotonic: true,
    },
  })

  // Latency histogram (using exponential buckets)
  const latencyDataPoints: OTelDataPoint[] = []
  for (const [, aggs] of byModelProvider) {
    const lastAgg = aggs[aggs.length - 1]!
    const totalRequests = aggs.reduce((sum, a) => sum + a.requestCount, 0)
    const totalLatency = aggs.reduce((sum, a) => sum + a.totalLatencyMs, 0)

    latencyDataPoints.push({
      attributes: {
        model: lastAgg.modelId,
        provider: lastAgg.providerId,
      },
      startTimeUnixNano: dateToUnixNano(lastAgg.createdAt),
      timeUnixNano: now,
      value: {
        count: totalRequests,
        sum: totalLatency,
        bucketCounts: [], // Would need actual latency samples for proper histogram
      },
    })
  }

  metrics.push({
    name: 'parquedb.ai.request_duration',
    description: 'AI request duration in milliseconds',
    unit: 'ms',
    data: {
      dataPoints: latencyDataPoints,
      aggregationTemporality: 'AGGREGATION_TEMPORALITY_CUMULATIVE',
    },
  })

  // Error rate gauge
  const errorRateDataPoints: OTelDataPoint[] = []
  for (const [, aggs] of byModelProvider) {
    const lastAgg = aggs[aggs.length - 1]!
    const total = aggs.reduce((sum, a) => sum + a.requestCount, 0)
    const errors = aggs.reduce((sum, a) => sum + a.errorCount, 0)
    const errorRate = total > 0 ? errors / total : 0

    errorRateDataPoints.push({
      attributes: {
        model: lastAgg.modelId,
        provider: lastAgg.providerId,
      },
      startTimeUnixNano: now,
      timeUnixNano: now,
      value: errorRate,
    })
  }

  metrics.push({
    name: 'parquedb.ai.error_rate',
    description: 'Current error rate (0-1)',
    unit: '1',
    data: {
      dataPoints: errorRateDataPoints,
    },
  })

  return {
    resourceMetrics: [
      {
        resource: {
          attributes: resourceAttributesToOTLP(resourceAttrs),
        },
        scopeMetrics: [
          {
            scope: { name: INSTRUMENTATION_SCOPE, version: SERVICE_VERSION },
            metrics,
          },
        ],
      },
    ],
  }
}

/**
 * Export compaction metrics to OpenTelemetry format
 *
 * @param metrics - Map of namespace to compaction metrics
 * @param options - Export options
 * @returns OTLP metrics payload
 */
export function exportCompactionToOTLP(
  metrics: Map<string, CompactionMetrics> | CompactionMetrics[],
  options: {
    resourceAttributes?: Partial<OTelResourceAttributes>
    environment?: string
  } = {}
): OTelMetricsPayload {
  const resourceAttrs: OTelResourceAttributes = {
    'service.name': SERVICE_NAME,
    'service.version': SERVICE_VERSION,
    'deployment.environment': options.environment ?? 'production',
    ...options.resourceAttributes,
  }

  const metricsMap = Array.isArray(metrics)
    ? new Map(metrics.map(m => [m.namespace, m]))
    : metrics

  const otlpMetrics: OTelMetric[] = []
  const now = Date.now() * 1_000_000

  // Windows pending
  const windowsPendingDataPoints: OTelDataPoint[] = []
  for (const [namespace, m] of metricsMap) {
    windowsPendingDataPoints.push({
      attributes: { namespace },
      startTimeUnixNano: now,
      timeUnixNano: now,
      value: m.windows_pending,
    })
  }

  otlpMetrics.push({
    name: 'parquedb.compaction.windows_pending',
    description: 'Number of compaction windows pending',
    unit: '{window}',
    data: { dataPoints: windowsPendingDataPoints },
  })

  // Windows processing
  const windowsProcessingDataPoints: OTelDataPoint[] = []
  for (const [namespace, m] of metricsMap) {
    windowsProcessingDataPoints.push({
      attributes: { namespace },
      startTimeUnixNano: now,
      timeUnixNano: now,
      value: m.windows_processing,
    })
  }

  otlpMetrics.push({
    name: 'parquedb.compaction.windows_processing',
    description: 'Number of compaction windows being processed',
    unit: '{window}',
    data: { dataPoints: windowsProcessingDataPoints },
  })

  // Files pending
  const filesPendingDataPoints: OTelDataPoint[] = []
  for (const [namespace, m] of metricsMap) {
    filesPendingDataPoints.push({
      attributes: { namespace },
      startTimeUnixNano: now,
      timeUnixNano: now,
      value: m.files_pending,
    })
  }

  otlpMetrics.push({
    name: 'parquedb.compaction.files_pending',
    description: 'Total files pending compaction',
    unit: '{file}',
    data: { dataPoints: filesPendingDataPoints },
  })

  // Bytes pending
  const bytesPendingDataPoints: OTelDataPoint[] = []
  for (const [namespace, m] of metricsMap) {
    bytesPendingDataPoints.push({
      attributes: { namespace },
      startTimeUnixNano: now,
      timeUnixNano: now,
      value: m.bytes_pending,
    })
  }

  otlpMetrics.push({
    name: 'parquedb.compaction.bytes_pending',
    description: 'Total bytes pending compaction',
    unit: 'By',
    data: { dataPoints: bytesPendingDataPoints },
  })

  // Windows stuck
  const windowsStuckDataPoints: OTelDataPoint[] = []
  for (const [namespace, m] of metricsMap) {
    windowsStuckDataPoints.push({
      attributes: { namespace },
      startTimeUnixNano: now,
      timeUnixNano: now,
      value: m.windows_stuck,
    })
  }

  otlpMetrics.push({
    name: 'parquedb.compaction.windows_stuck',
    description: 'Number of windows stuck in processing',
    unit: '{window}',
    data: { dataPoints: windowsStuckDataPoints },
  })

  return {
    resourceMetrics: [
      {
        resource: {
          attributes: resourceAttributesToOTLP(resourceAttrs),
        },
        scopeMetrics: [
          {
            scope: { name: INSTRUMENTATION_SCOPE, version: SERVICE_VERSION },
            metrics: otlpMetrics,
          },
        ],
      },
    ],
  }
}

// =============================================================================
// Trace Export
// =============================================================================

/**
 * Export AI requests as OpenTelemetry traces
 *
 * Each AI request becomes a span with relevant attributes.
 *
 * @param requests - Array of AI request records
 * @param options - Export options
 * @returns OTLP trace payload
 */
export function exportAIRequestsToOTLPTraces(
  requests: AIRequestRecord[],
  options: {
    resourceAttributes?: Partial<OTelResourceAttributes>
    environment?: string
    parentSpanId?: string
    traceId?: string
  } = {}
): OTelTracePayload {
  const resourceAttrs: OTelResourceAttributes = {
    'service.name': SERVICE_NAME,
    'service.version': SERVICE_VERSION,
    'deployment.environment': options.environment ?? 'production',
    ...options.resourceAttributes,
  }

  const traceId = options.traceId ?? generateTraceId()
  const spans: OTelSpan[] = []

  for (const req of requests) {
    const startTime = dateToUnixNano(req.timestamp)
    const endTime = startTime + (req.latencyMs * 1_000_000)

    const attributes: Record<string, string | number | boolean> = {
      'ai.model': req.modelId,
      'ai.provider': req.providerId,
      'ai.request_type': req.requestType,
      'ai.tokens.prompt': req.promptTokens,
      'ai.tokens.completion': req.completionTokens,
      'ai.tokens.total': req.totalTokens,
      'ai.cost.estimated': req.estimatedCost,
      'ai.cached': req.cached,
    }

    if (req.finishReason) {
      attributes['ai.finish_reason'] = req.finishReason
    }
    if (req.temperature !== undefined) {
      attributes['ai.temperature'] = req.temperature
    }
    if (req.maxTokens !== undefined) {
      attributes['ai.max_tokens'] = req.maxTokens
    }
    if (req.userId) {
      attributes['user.id'] = req.userId
    }
    if (req.appId) {
      attributes['app.id'] = req.appId
    }
    if (req.environment) {
      attributes['deployment.environment'] = req.environment
    }

    const span: OTelSpan = {
      traceId,
      spanId: generateSpanId(),
      parentSpanId: options.parentSpanId,
      name: `ai.${req.requestType}`,
      kind: 'SPAN_KIND_CLIENT',
      startTimeUnixNano: startTime,
      endTimeUnixNano: endTime,
      attributes: attributesToOTLP(attributes),
      status: req.status === 'success'
        ? { code: 'STATUS_CODE_OK' }
        : { code: 'STATUS_CODE_ERROR', message: req.error },
    }

    // Add error event if applicable
    if (req.status === 'error' && req.error) {
      span.events = [
        {
          timeUnixNano: endTime,
          name: 'exception',
          attributes: attributesToOTLP({
            'exception.type': req.errorCode ?? 'Error',
            'exception.message': req.error,
          }),
        },
      ]
    }

    spans.push(span)
  }

  return {
    resourceSpans: [
      {
        resource: {
          attributes: resourceAttributesToOTLP(resourceAttrs),
        },
        scopeSpans: [
          {
            scope: { name: INSTRUMENTATION_SCOPE, version: SERVICE_VERSION },
            spans,
          },
        ],
      },
    ],
  }
}

/**
 * Merge multiple OTLP payloads
 */
export function mergeOTLPMetrics(...payloads: OTelMetricsPayload[]): OTelMetricsPayload {
  const allResourceMetrics = payloads.flatMap(p => p.resourceMetrics)
  return { resourceMetrics: allResourceMetrics }
}

/**
 * Merge multiple OTLP trace payloads
 */
export function mergeOTLPTraces(...payloads: OTelTracePayload[]): OTelTracePayload {
  const allResourceSpans = payloads.flatMap(p => p.resourceSpans)
  return { resourceSpans: allResourceSpans }
}
