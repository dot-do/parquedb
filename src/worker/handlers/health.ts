/**
 * Health Check Handler
 *
 * Provides health check endpoints with support for:
 * - Basic health check (quick, no dependencies)
 * - Deep health check (verifies R2 connectivity, read/write paths)
 *
 * Usage:
 *   GET /health         - Basic health check (fast)
 *   GET /health?deep=true - Deep health check (R2 verification)
 */

import { buildResponse } from '../responses'
import type { HandlerContext } from './types'
import { runDeepHealthChecks } from './health-checks'

/** Extended context with optional bucket for deep checks */
export interface HealthHandlerContext extends HandlerContext {
  /** R2 bucket for deep health checks (optional) */
  bucket?: R2Bucket | undefined
}

/**
 * Handle health check route (/health)
 *
 * Basic mode (default): Returns static health status, very fast.
 * Deep mode (?deep=true): Verifies R2 connectivity, read/write paths.
 */
export async function handleHealth(
  context: HealthHandlerContext
): Promise<Response> {
  const { request, baseUrl, startTime, url, bucket } = context

  // Check if deep health check is requested
  const deep = url?.searchParams.get('deep') === 'true'

  if (deep && bucket) {
    // Run deep health checks
    const deepResult = await runDeepHealthChecks(bucket, { timeoutMs: 10000 })

    // Determine HTTP status based on health
    const httpStatus =
      deepResult.status === 'healthy' ? 200 :
      deepResult.status === 'degraded' ? 200 :
      503

    return new Response(JSON.stringify({
      api: {
        status: deepResult.status,
        mode: 'deep',
        uptime: 'ok',
        storage: 'r2',
        compute: 'durable-objects',
      },
      checks: deepResult.checks,
      latency: {
        total: `${deepResult.latencyMs.total.toFixed(0)}ms`,
        r2: deepResult.latencyMs.r2 ? `${deepResult.latencyMs.r2.toFixed(0)}ms` : undefined,
        read: deepResult.latencyMs.read ? `${deepResult.latencyMs.read.toFixed(0)}ms` : undefined,
        write: deepResult.latencyMs.write ? `${deepResult.latencyMs.write.toFixed(0)}ms` : undefined,
      },
      timestamp: deepResult.timestamp,
      links: {
        home: baseUrl,
        datasets: `${baseUrl}/datasets`,
      },
    }, null, 2), {
      status: httpStatus,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Access-Control-Allow-Origin': '*',
      },
    })
  }

  if (deep && !bucket) {
    // Deep check requested but no bucket available
    return new Response(JSON.stringify({
      api: {
        status: 'degraded',
        mode: 'deep',
        error: 'R2 bucket not available for deep health check',
        uptime: 'ok',
        storage: 'r2',
        compute: 'durable-objects',
      },
      links: {
        home: baseUrl,
        datasets: `${baseUrl}/datasets`,
      },
    }, null, 2), {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Access-Control-Allow-Origin': '*',
      },
    })
  }

  // Basic health check (fast path, no I/O)
  return buildResponse(request, {
    api: {
      status: 'healthy',
      mode: 'basic',
      uptime: 'ok',
      storage: 'r2',
      compute: 'durable-objects',
    },
    links: {
      home: baseUrl,
      datasets: `${baseUrl}/datasets`,
    },
  }, startTime)
}

/**
 * Synchronous basic health check (for backwards compatibility)
 *
 * @deprecated Use handleHealth instead for deep check support
 */
export function handleHealthBasic(context: HandlerContext): Response {
  const { request, baseUrl, startTime } = context

  return buildResponse(request, {
    api: {
      status: 'healthy',
      mode: 'basic',
      uptime: 'ok',
      storage: 'r2',
      compute: 'durable-objects',
    },
    links: {
      home: baseUrl,
      datasets: `${baseUrl}/datasets`,
    },
  }, startTime)
}
