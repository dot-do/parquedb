/**
 * Compaction Route Handlers
 *
 * Handles /compaction/* routes for event-driven compaction monitoring.
 */

import type { RouteHandlerContext } from '../route-registry'
import { buildErrorResponse } from '../responses'

/**
 * Handle GET /compaction/status - Get compaction status for namespace(s)
 *
 * Query parameters:
 * - namespace: Get status for a specific namespace (recommended)
 * - namespaces: Comma-separated list of namespaces to aggregate
 */
export async function handleCompactionStatus(ctx: RouteHandlerContext): Promise<Response> {
  const { request, url, env, startTime } = ctx

  if (!env.COMPACTION_STATE) {
    return buildErrorResponse(request, new Error('Compaction State DO not available'), 500, startTime)
  }

  // Capture reference after null check for use in callbacks
  const compactionState = env.COMPACTION_STATE

  const namespaceParam = url.searchParams.get('namespace')
  const namespacesParam = url.searchParams.get('namespaces')

  // Single namespace query - direct to its sharded DO
  if (namespaceParam) {
    const id = compactionState.idFromName(namespaceParam)
    const stub = compactionState.get(id)
    return stub.fetch(new Request(new URL('/status', request.url).toString()))
  }

  // Multiple namespaces query - aggregate from multiple DOs
  if (namespacesParam) {
    const namespaces = namespacesParam.split(',').map(ns => ns.trim()).filter(Boolean)
    if (namespaces.length === 0) {
      return buildErrorResponse(
        request,
        new Error('namespaces parameter must contain at least one namespace'),
        400,
        startTime
      )
    }

    // Query all namespace DOs in parallel
    const results = await Promise.all(
      namespaces.map(async (namespace) => {
        const id = compactionState.idFromName(namespace)
        const stub = compactionState.get(id)
        try {
          const response = await stub.fetch(new Request(new URL('/status', request.url).toString()))
          const data = await response.json() as Record<string, unknown>
          return { namespace, ...data }
        } catch (err) {
          return { namespace, error: err instanceof Error ? err.message : 'Unknown error' }
        }
      })
    )

    // Aggregate statistics
    const aggregated = {
      namespaces: results,
      summary: {
        totalNamespaces: results.length,
        totalActiveWindows: results.reduce((sum, r) => {
          const windows = (r as { activeWindows?: number | undefined }).activeWindows ?? 0
          return sum + windows
        }, 0),
        totalKnownWriters: [...new Set(results.flatMap(r => {
          const writers = (r as { knownWriters?: string[] | undefined }).knownWriters ?? []
          return writers
        }))],
      },
    }

    return new Response(JSON.stringify(aggregated, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  // No namespace specified - return usage instructions
  return new Response(JSON.stringify({
    message: 'CompactionStateDO is sharded by namespace. Please specify a namespace parameter.',
    usage: {
      single: '/compaction/status?namespace=posts',
      multiple: '/compaction/status?namespaces=posts,comments,users',
    },
    note: 'Each namespace has its own CompactionStateDO instance for scalability.',
  }, null, 2), {
    status: 400,
    headers: { 'Content-Type': 'application/json' },
  })
}

/**
 * Handle GET /compaction/health - Aggregated health check for alerting/monitoring
 *
 * Query parameters:
 * - namespaces: Comma-separated list of namespaces to check (required)
 * - maxPendingWindows: Threshold for degraded status (default: 10)
 * - maxWindowAgeHours: Threshold for degraded status (default: 2)
 */
export async function handleCompactionHealth(ctx: RouteHandlerContext): Promise<Response> {
  const { request, url, env, startTime } = ctx

  if (!env.COMPACTION_STATE) {
    return buildErrorResponse(request, new Error('Compaction State DO not available'), 500, startTime)
  }

  // Capture reference after null check for use in callbacks
  const compactionState = env.COMPACTION_STATE

  const namespacesParam = url.searchParams.get('namespaces')
  if (!namespacesParam) {
    return new Response(JSON.stringify({
      error: 'namespaces parameter is required',
      usage: '/compaction/health?namespaces=users,posts,comments',
      optional: {
        maxPendingWindows: 'Threshold for degraded status (default: 10)',
        maxWindowAgeHours: 'Threshold for degraded status (default: 2)',
      },
    }, null, 2), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    })
  }

  const namespaces = namespacesParam.split(',').map(ns => ns.trim()).filter(Boolean)
  if (namespaces.length === 0) {
    return buildErrorResponse(
      request,
      new Error('namespaces parameter must contain at least one namespace'),
      400,
      startTime
    )
  }

  // Parse optional config parameters
  const maxPendingWindows = parseInt(url.searchParams.get('maxPendingWindows') ?? '10', 10)
  const maxWindowAgeHours = parseFloat(url.searchParams.get('maxWindowAgeHours') ?? '2')
  const healthConfig = { maxPendingWindows, maxWindowAgeHours }

  // Import health evaluation functions
  const {
    evaluateNamespaceHealth,
    aggregateHealthStatus,
    isCompactionStatusResponse,
  } = await import('../../workflows/compaction-queue-consumer')

  type NamespaceHealth = import('../../workflows/compaction-queue-consumer').NamespaceHealth

  // Query all namespace DOs in parallel
  const namespaceHealthMap: Record<string, NamespaceHealth> = {}

  await Promise.all(
    namespaces.map(async (namespace) => {
      const id = compactionState.idFromName(namespace)
      const stub = compactionState.get(id)
      try {
        const response = await stub.fetch(new Request(new URL('/status', request.url).toString()))
        const data = await response.json()

        if (isCompactionStatusResponse(data)) {
          namespaceHealthMap[namespace] = evaluateNamespaceHealth(namespace, data, healthConfig)
        } else {
          // Namespace has no data yet - treat as healthy
          namespaceHealthMap[namespace] = {
            namespace,
            status: 'healthy',
            metrics: {
              activeWindows: 0,
              oldestWindowAge: 0,
              totalPendingFiles: 0,
              windowsStuckInProcessing: 0,
            },
            issues: [],
          }
        }
      } catch (err) {
        // Error querying namespace - mark as unhealthy
        namespaceHealthMap[namespace] = {
          namespace,
          status: 'unhealthy',
          metrics: {
            activeWindows: 0,
            oldestWindowAge: 0,
            totalPendingFiles: 0,
            windowsStuckInProcessing: 0,
          },
          issues: [`Failed to query status: ${err instanceof Error ? err.message : 'Unknown error'}`],
        }
      }
    })
  )

  // Aggregate health status
  const healthResponse = aggregateHealthStatus(namespaceHealthMap)

  // Return appropriate HTTP status code based on health
  const httpStatus = healthResponse.status === 'healthy' ? 200 : healthResponse.status === 'degraded' ? 200 : 503

  return new Response(JSON.stringify(healthResponse, null, 2), {
    status: httpStatus,
    headers: { 'Content-Type': 'application/json' },
  })
}

/**
 * Handle GET /compaction/dashboard - HTML monitoring page
 *
 * Query parameters:
 * - namespaces: Comma-separated list of namespaces to monitor (required)
 */
export async function handleCompactionDashboard(ctx: RouteHandlerContext): Promise<Response> {
  const { request, url, baseUrl, startTime } = ctx

  const { generateDashboardHtml } = await import('../../observability/compaction')

  const namespacesParam = url.searchParams.get('namespaces')
  if (!namespacesParam) {
    return new Response(
      JSON.stringify(
        {
          error: 'namespaces parameter is required',
          usage: '/compaction/dashboard?namespaces=users,posts,comments',
        },
        null,
        2
      ),
      {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      }
    )
  }

  const namespaces = namespacesParam
    .split(',')
    .map((ns) => ns.trim())
    .filter(Boolean)
  if (namespaces.length === 0) {
    return buildErrorResponse(
      request,
      new Error('namespaces parameter must contain at least one namespace'),
      400,
      startTime
    )
  }

  const html = generateDashboardHtml(baseUrl, namespaces)
  return new Response(html, {
    headers: { 'Content-Type': 'text/html; charset=utf-8' },
  })
}

/**
 * Handle GET /compaction/metrics - Prometheus format export
 *
 * Query parameters:
 * - namespaces: Optional comma-separated list of namespaces to include
 */
export async function handleCompactionMetrics(ctx: RouteHandlerContext): Promise<Response> {
  const { url } = ctx

  const { exportPrometheusMetrics } = await import('../../observability/compaction')

  const namespacesParam = url.searchParams.get('namespaces')
  const namespaces = namespacesParam
    ? namespacesParam
        .split(',')
        .map((ns) => ns.trim())
        .filter(Boolean)
    : undefined

  const prometheusOutput = exportPrometheusMetrics(namespaces)
  return new Response(prometheusOutput, {
    headers: {
      'Content-Type': 'text/plain; version=0.0.4; charset=utf-8',
    },
  })
}

/**
 * Handle GET /compaction/metrics/json - JSON time-series export
 *
 * Query parameters:
 * - namespaces: Optional comma-separated list of namespaces to include
 * - since: Optional Unix timestamp (ms) to filter data points from
 * - limit: Optional max data points per series (default: 100)
 */
export async function handleCompactionMetricsJson(ctx: RouteHandlerContext): Promise<Response> {
  const { url } = ctx

  const { exportJsonTimeSeries } = await import('../../observability/compaction')

  const namespacesParam = url.searchParams.get('namespaces')
  const namespaces = namespacesParam
    ? namespacesParam
        .split(',')
        .map((ns) => ns.trim())
        .filter(Boolean)
    : undefined

  const sinceParam = url.searchParams.get('since')
  const since = sinceParam ? parseInt(sinceParam, 10) : undefined

  const limitParam = url.searchParams.get('limit')
  const limit = limitParam ? parseInt(limitParam, 10) : 100

  const jsonData = exportJsonTimeSeries(namespaces, since, limit)
  return new Response(JSON.stringify(jsonData, null, 2), {
    headers: { 'Content-Type': 'application/json' },
  })
}
