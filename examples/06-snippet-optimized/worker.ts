/**
 * Example Cloudflare Worker/Snippet for ParqueDB queries
 *
 * This demonstrates querying Parquet data from static assets
 * within Cloudflare Snippet constraints.
 *
 * Deploy as a Snippet for FREE hosting, or as a Worker for more features.
 */

import { createQueryClient } from './query'
import type { QueryResult } from './query'

// Types for your data
interface User {
  $id: string
  $type: string
  name: string
  email: string
  createdAt: string
}

// Configuration
const DATA_BASE_URL = 'https://your-domain.com/data/users/by-id'

// Create query client
const db = createQueryClient(DATA_BASE_URL)

// Request handler
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // Track timing
    const requestStart = Date.now()
    const cpuStart = performance.now()

    try {
      let result: QueryResult<unknown>

      // Route: GET /users/:id
      if (path.startsWith('/users/') && request.method === 'GET') {
        const id = path.slice('/users/'.length)
        result = await db.get<User>(id)

        // Log metrics (in production, send to your logging service)
        logMetrics(env, ctx, {
          path,
          method: 'GET',
          operation: 'getById',
          ...result.metrics,
          wallTimeMs: Date.now() - requestStart,
        })

        if (!result.data) {
          return new Response('Not found', { status: 404 })
        }

        return Response.json(result.data, {
          headers: {
            'X-CPU-Time-Ms': result.metrics.cpuTimeMs.toFixed(2),
            'X-Fetch-Count': String(result.metrics.fetchCount),
            'X-Row-Groups-Scanned': String(result.metrics.rowGroupsScanned),
            'X-Row-Groups-Skipped': String(result.metrics.rowGroupsSkipped),
          },
        })
      }

      // Route: GET /users?createdAfter=...&createdBefore=...
      if (path === '/users' && request.method === 'GET') {
        const minDate = url.searchParams.get('createdAfter') || '1970-01-01'
        const maxDate = url.searchParams.get('createdBefore') || '2100-01-01'
        const limit = parseInt(url.searchParams.get('limit') || '10')

        result = await db.find<User>('createdAt', minDate, maxDate, { limit })

        logMetrics(env, ctx, {
          path,
          method: 'GET',
          operation: 'findByRange',
          ...result.metrics,
          wallTimeMs: Date.now() - requestStart,
        })

        return Response.json(result.data, {
          headers: {
            'X-CPU-Time-Ms': result.metrics.cpuTimeMs.toFixed(2),
            'X-Fetch-Count': String(result.metrics.fetchCount),
            'X-Row-Groups-Scanned': String(result.metrics.rowGroupsScanned),
            'X-Row-Groups-Skipped': String(result.metrics.rowGroupsSkipped),
          },
        })
      }

      // Health check
      if (path === '/health') {
        return Response.json({
          status: 'ok',
          cpuTimeMs: performance.now() - cpuStart,
        })
      }

      return new Response('Not found', { status: 404 })
    } catch (error) {
      console.error('Query error:', error)

      logMetrics(env, ctx, {
        path,
        method: request.method,
        operation: 'error',
        error: String(error),
        cpuTimeMs: performance.now() - cpuStart,
        wallTimeMs: Date.now() - requestStart,
        fetchCount: 0,
        bytesRead: 0,
        rowGroupsScanned: 0,
        rowGroupsSkipped: 0,
      })

      return new Response('Internal error', { status: 500 })
    }
  },
}

// Environment bindings
interface Env {
  // R2 bucket for logging (optional)
  LOGS?: R2Bucket
  // KV for caching manifests (optional)
  CACHE?: KVNamespace
}

// Metrics logging
interface Metrics {
  path: string
  method: string
  operation: string
  cpuTimeMs: number
  wallTimeMs: number
  fetchCount: number
  bytesRead: number
  rowGroupsScanned: number
  rowGroupsSkipped: number
  error?: string
}

function logMetrics(env: Env, ctx: ExecutionContext, metrics: Metrics) {
  // Console log for wrangler tail
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    ...metrics,
  }))

  // Optionally write to R2 (non-blocking)
  if (env.LOGS) {
    const key = `logs/${new Date().toISOString().slice(0, 10)}/${Date.now()}-${Math.random().toString(36).slice(2)}.json`

    ctx.waitUntil(
      env.LOGS.put(key, JSON.stringify({
        timestamp: new Date().toISOString(),
        ...metrics,
      }))
    )
  }
}

/**
 * Static assets version for Cloudflare Pages/Snippets
 *
 * When using static assets, the data files are served from the same domain.
 */
export function createStaticAssetsHandler(assetsPath: string = '/data') {
  const db = createQueryClient(assetsPath)

  return {
    async fetch(request: Request): Promise<Response> {
      const url = new URL(request.url)
      const id = url.searchParams.get('id')

      if (!id) {
        return new Response('Missing id parameter', { status: 400 })
      }

      const result = await db.get(id)

      if (!result.data) {
        return new Response('Not found', { status: 404 })
      }

      return Response.json({
        data: result.data,
        metrics: result.metrics,
      })
    },
  }
}
