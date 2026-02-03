/**
 * Prometheus Metrics Endpoint Handler
 *
 * Provides a /metrics endpoint that returns metrics in Prometheus text format.
 * Integrates with the global PrometheusMetrics instance.
 *
 * @module worker/handlers/metrics
 */

import { getGlobalMetrics, exportMetrics } from '../../observability/prometheus'
import { getGlobalTelemetry } from '../../observability/telemetry'
import {
  exportCompactionToPrometheus,
  exportAIUsageToPrometheus,
  combinePrometheusExports,
} from '../../observability/export/prometheus'
import { getAllLatestMetrics } from '../../observability/compaction'
import type { HandlerContext } from './types'

/**
 * Prometheus text format content type
 */
export const PROMETHEUS_CONTENT_TYPE = 'text/plain; version=0.0.4; charset=utf-8'

/**
 * Handle the /metrics endpoint
 *
 * Returns all collected metrics in Prometheus exposition format.
 * Includes:
 * - Core ParqueDB metrics (requests, entities, storage, cache)
 * - Telemetry metrics (write throughput, consistency lag)
 * - Compaction metrics (if available)
 * - AI observability metrics (if available)
 *
 * @param _context - Handler context (optional, for consistency with other handlers)
 * @returns Response with Prometheus-formatted metrics
 */
export function handleMetrics(_context?: HandlerContext): Response {
  const outputs: string[] = []

  // 1. Export core Prometheus metrics
  try {
    const coreMetrics = exportMetrics()
    if (coreMetrics.trim()) {
      outputs.push(coreMetrics)
    }
  } catch {
    // Ignore errors, continue with other metrics
  }

  // 2. Export telemetry metrics (write throughput, cache, consistency lag)
  try {
    const telemetry = getGlobalTelemetry()
    const telemetryMetrics = telemetry.exportPrometheus()
    if (telemetryMetrics.trim()) {
      outputs.push(telemetryMetrics)
    }
  } catch {
    // Ignore errors, continue with other metrics
  }

  // 3. Export compaction metrics
  try {
    const compactionMetricsMap = getAllLatestMetrics()
    if (compactionMetricsMap.size > 0) {
      const compactionMetrics = exportCompactionToPrometheus(compactionMetricsMap)
      if (compactionMetrics.trim()) {
        outputs.push(compactionMetrics)
      }
    }
  } catch {
    // Ignore errors, continue with other metrics
  }

  // Combine all metric outputs
  const combined = combinePrometheusExports(...outputs)

  return new Response(combined, {
    status: 200,
    headers: {
      'Content-Type': PROMETHEUS_CONTENT_TYPE,
      'Cache-Control': 'no-cache, no-store, must-revalidate',
    },
  })
}

/**
 * Handle metrics with additional AI usage data
 *
 * @param _context - Handler context
 * @param aiAggregates - Optional AI usage aggregates to include
 * @returns Response with Prometheus-formatted metrics
 */
export function handleMetricsWithAI(
  _context: HandlerContext | undefined,
  aiAggregates?: Array<{
    modelId: string
    providerId: string
    requestCount: number
    successCount: number
    errorCount: number
    cachedCount: number
    totalPromptTokens: number
    totalCompletionTokens: number
    estimatedTotalCost: number
    avgLatencyMs: number
    p50LatencyMs?: number | undefined
    p90LatencyMs?: number | undefined
    p95LatencyMs?: number | undefined
    p99LatencyMs?: number | undefined
  }>
): Response {
  const outputs: string[] = []

  // Export core metrics
  try {
    const coreMetrics = exportMetrics()
    if (coreMetrics.trim()) {
      outputs.push(coreMetrics)
    }
  } catch {
    // Ignore
  }

  // Export telemetry metrics
  try {
    const telemetry = getGlobalTelemetry()
    const telemetryMetrics = telemetry.exportPrometheus()
    if (telemetryMetrics.trim()) {
      outputs.push(telemetryMetrics)
    }
  } catch {
    // Ignore
  }

  // Export compaction metrics
  try {
    const compactionMetricsMap = getAllLatestMetrics()
    if (compactionMetricsMap.size > 0) {
      const compactionMetrics = exportCompactionToPrometheus(compactionMetricsMap)
      if (compactionMetrics.trim()) {
        outputs.push(compactionMetrics)
      }
    }
  } catch {
    // Ignore
  }

  // Export AI usage metrics if provided
  if (aiAggregates && aiAggregates.length > 0) {
    try {
      // Type cast to match expected interface (with required fields for export)
      const fullAggregates = aiAggregates.map((agg) => ({
        $id: `${agg.modelId}-${agg.providerId}`,
        $type: 'AIUsage' as const,
        name: `${agg.modelId}/${agg.providerId}`,
        modelId: agg.modelId,
        providerId: agg.providerId,
        dateKey: new Date().toISOString().split('T')[0],
        granularity: 'day' as const,
        requestCount: agg.requestCount,
        successCount: agg.successCount,
        errorCount: agg.errorCount,
        cachedCount: agg.cachedCount,
        generateCount: 0,
        streamCount: 0,
        totalPromptTokens: agg.totalPromptTokens,
        totalCompletionTokens: agg.totalCompletionTokens,
        totalTokens: agg.totalPromptTokens + agg.totalCompletionTokens,
        avgTokensPerRequest: agg.requestCount > 0 ? (agg.totalPromptTokens + agg.totalCompletionTokens) / agg.requestCount : 0,
        totalLatencyMs: agg.avgLatencyMs * agg.requestCount,
        avgLatencyMs: agg.avgLatencyMs,
        minLatencyMs: 0,
        maxLatencyMs: 0,
        p50LatencyMs: agg.p50LatencyMs,
        p90LatencyMs: agg.p90LatencyMs,
        p95LatencyMs: agg.p95LatencyMs,
        p99LatencyMs: agg.p99LatencyMs,
        estimatedInputCost: 0,
        estimatedOutputCost: 0,
        estimatedTotalCost: agg.estimatedTotalCost,
        createdAt: new Date(),
        updatedAt: new Date(),
        version: 1,
      }))
      const aiMetrics = exportAIUsageToPrometheus(fullAggregates)
      if (aiMetrics.trim()) {
        outputs.push(aiMetrics)
      }
    } catch {
      // Ignore
    }
  }

  const combined = combinePrometheusExports(...outputs)

  return new Response(combined, {
    status: 200,
    headers: {
      'Content-Type': PROMETHEUS_CONTENT_TYPE,
      'Cache-Control': 'no-cache, no-store, must-revalidate',
    },
  })
}

/**
 * Record a request in the global metrics
 *
 * Helper function to be called from the main worker to track requests.
 *
 * @param method - HTTP method
 * @param namespace - Target namespace (or 'unknown')
 * @param status - Response status code
 * @param durationSeconds - Request duration in seconds
 */
export function recordRequest(
  method: string,
  namespace: string,
  status: number,
  durationSeconds: number
): void {
  const metrics = getGlobalMetrics()

  // Record request count
  metrics.increment('requests_total', {
    method,
    namespace,
    status: String(status),
  })

  // Record request duration
  metrics.observe('request_duration_seconds', durationSeconds, {
    method,
    namespace,
  })
}

/**
 * Record a cache access in the global metrics
 *
 * @param cache - Cache identifier (e.g., 'query', 'metadata', 'bloom')
 * @param hit - Whether this was a cache hit
 */
export function recordCacheAccess(cache: string, hit: boolean): void {
  const metrics = getGlobalMetrics()
  if (hit) {
    metrics.increment('cache_hits_total', { cache })
  } else {
    metrics.increment('cache_misses_total', { cache })
  }
}

/**
 * Record a write operation in the global metrics
 *
 * @param namespace - Target namespace
 * @param operation - Operation type (create, update, delete)
 */
export function recordWriteOperation(namespace: string, operation: string): void {
  const metrics = getGlobalMetrics()
  metrics.increment('write_operations_total', { namespace, operation })
}

/**
 * Record a read operation in the global metrics
 *
 * @param namespace - Target namespace
 * @param operation - Operation type (find, get, count)
 */
export function recordReadOperation(namespace: string, operation: string): void {
  const metrics = getGlobalMetrics()
  metrics.increment('read_operations_total', { namespace, operation })
}

/**
 * Update entity count gauge
 *
 * @param namespace - Namespace
 * @param count - Entity count
 */
export function setEntityCount(namespace: string, count: number): void {
  const metrics = getGlobalMetrics()
  metrics.set('entities_total', count, { namespace })
}

/**
 * Update storage size gauge
 *
 * @param namespace - Namespace
 * @param type - Storage type (data, index, events)
 * @param bytes - Size in bytes
 */
export function setStorageSize(namespace: string, type: string, bytes: number): void {
  const metrics = getGlobalMetrics()
  metrics.set('storage_bytes', bytes, { namespace, type })
}

/**
 * Record a compaction run
 *
 * @param namespace - Namespace that was compacted
 * @param status - Compaction status (success, failure)
 */
export function recordCompactionRun(namespace: string, status: 'success' | 'failure'): void {
  const metrics = getGlobalMetrics()
  metrics.increment('compaction_runs_total', { namespace, status })
}

/**
 * Record an error
 *
 * @param type - Error type (e.g., 'validation', 'storage', 'timeout')
 * @param namespace - Namespace where error occurred (or 'unknown')
 */
export function recordError(type: string, namespace: string): void {
  const metrics = getGlobalMetrics()
  metrics.increment('errors_total', { type, namespace })
}
