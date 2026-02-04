/**
 * Response Helpers for ParqueDB Worker
 *
 * Provides utilities for building HTTP responses with:
 * - Server-Timing headers
 * - Cloudflare request metadata
 * - Consistent JSON structure
 */

// =============================================================================
// Types
// =============================================================================

export interface CfProperties {
  colo?: string | undefined
  country?: string | undefined
  city?: string | undefined
  region?: string | undefined
  timezone?: string | undefined
  latitude?: string | undefined
  longitude?: string | undefined
  asn?: number | undefined
  asOrganization?: string | undefined
  httpProtocol?: string | undefined
}

export interface StorageStats {
  cdnHits: number
  primaryHits: number
  edgeHits: number
  cacheHits: number
  totalReads: number
  usingCdn: boolean
  usingEdge: boolean
}

// =============================================================================
// Timing Context
// =============================================================================

/**
 * Timing context for Server-Timing headers
 */
export interface TimingContext {
  startTime: number
  marks: Map<string, number>
  durations: Map<string, number>
}

/**
 * Create a new timing context
 */
export function createTimingContext(): TimingContext {
  return {
    startTime: performance.now(),
    marks: new Map(),
    durations: new Map(),
  }
}

/**
 * Mark a point in time for later measurement
 */
export function markTiming(ctx: TimingContext, name: string): void {
  ctx.marks.set(name, performance.now())
}

/**
 * Measure duration from a start mark to now
 */
export function measureTiming(ctx: TimingContext, name: string, startMark?: string): void {
  const start = startMark ? ctx.marks.get(startMark) : ctx.startTime
  if (start !== undefined) {
    ctx.durations.set(name, performance.now() - start)
  }
}

/**
 * Build Server-Timing header value from timing context
 */
export function buildServerTimingHeader(ctx: TimingContext): string {
  const parts: string[] = []

  // Add total time
  const total = performance.now() - ctx.startTime
  parts.push(`total;dur=${total.toFixed(1)}`)

  // Add individual durations
  for (const [name, dur] of ctx.durations) {
    parts.push(`${name};dur=${dur.toFixed(1)}`)
  }

  return parts.join(', ')
}

// =============================================================================
// Response Data Types
// =============================================================================

export interface ResponseData {
  api: Record<string, unknown>
  links: Record<string, string>
  data?: unknown | undefined
  items?: unknown[] | undefined
  stats?: Record<string, unknown> | undefined
  relationships?: Record<string, unknown> | undefined
}

// =============================================================================
// Response Builders
// =============================================================================

/**
 * Build a successful JSON response with Cloudflare metadata
 */
export function buildResponse(
  request: Request,
  data: ResponseData,
  timing?: TimingContext | number,
  storageStats?: StorageStats
): Response {
  const cf = (request.cf || {}) as CfProperties

  // Handle both old startTime number and new TimingContext
  const startTime = typeof timing === 'number' ? timing : timing?.startTime
  const timingCtx = typeof timing === 'object' ? timing : undefined

  // Calculate latency
  const latency = startTime ? Math.round(performance.now() - startTime) : undefined

  // Format timestamp in user's timezone
  const now = new Date()
  const requestedAt = cf.timezone
    ? now.toLocaleString('en-US', { timeZone: cf.timezone, hour12: false }).replace(',', '')
    : now.toISOString()

  // Get IP and ray from headers
  const ip = request.headers.get('cf-connecting-ip') || request.headers.get('x-forwarded-for')?.split(',')[0]
  const rayId = request.headers.get('cf-ray')?.split('-')[0]
  const ray = rayId && cf.colo ? `${rayId}-${cf.colo}` : rayId

  // Build timing info for response body
  const timingInfo: Record<string, string> | undefined = timingCtx ? {} : undefined
  if (timingCtx && timingInfo) {
    for (const [name, dur] of timingCtx.durations) {
      timingInfo[name] = `${dur.toFixed(0)}ms`
    }
  }

  const response = {
    api: data.api,
    links: data.links,
    ...(data.data !== undefined ? { data: data.data } : {}),
    ...(data.relationships !== undefined ? { relationships: data.relationships } : {}),
    ...(data.items !== undefined ? { items: data.items } : {}),
    ...(data.stats !== undefined ? { stats: data.stats } : {}),
    user: {
      ip,
      ray,
      colo: cf.colo,
      country: cf.country,
      city: cf.city,
      region: cf.region,
      timezone: cf.timezone,
      requestedAt,
      ...(latency !== undefined ? { latency: `${latency}ms` } : {}),
      ...(timingInfo && Object.keys(timingInfo).length > 0 ? { timing: timingInfo } : {}),
      ...(storageStats?.totalReads || storageStats?.cacheHits ? {
        storage: {
          cacheHits: storageStats.cacheHits,
          edgeHits: storageStats.edgeHits,
          cdnHits: storageStats.cdnHits,
          primaryHits: storageStats.primaryHits,
          totalReads: storageStats.totalReads,
          usingEdge: storageStats.usingEdge,
          usingCdn: storageStats.usingCdn,
        }
      } : {}),
    },
  }

  // Build headers
  const headers: Record<string, string> = {
    'Access-Control-Allow-Origin': '*',
    'Cache-Control': 'public, max-age=60',
  }

  // Add Server-Timing header if we have timing context
  if (timingCtx) {
    headers['Server-Timing'] = buildServerTimingHeader(timingCtx)
  }

  return Response.json(response, { headers })
}

/**
 * Extended error interface for errors with additional metadata
 */
export interface ExtendedError extends Error {
  code?: string | undefined
  hint?: string | undefined
}

/**
 * Build an error response with Cloudflare metadata
 */
export function buildErrorResponse(
  request: Request,
  error: Error | ExtendedError,
  status: number = 500,
  startTime?: number
): Response {
  const cf = (request.cf || {}) as CfProperties

  // Calculate latency
  const latency = startTime ? Math.round(performance.now() - startTime) : undefined

  // Format timestamp in user's timezone
  const now = new Date()
  const requestedAt = cf.timezone
    ? now.toLocaleString('en-US', { timeZone: cf.timezone, hour12: false }).replace(',', '')
    : now.toISOString()

  // Get IP and ray from headers
  const ip = request.headers.get('cf-connecting-ip') || request.headers.get('x-forwarded-for')?.split(',')[0]
  const rayId = request.headers.get('cf-ray')?.split('-')[0]
  const ray = rayId && cf.colo ? `${rayId}-${cf.colo}` : rayId

  // Extract extended error properties if present
  const extError = error as ExtendedError

  // Derive base URL from request for full URL links
  const base = new URL(request.url).origin

  return Response.json({
    api: {
      error: true,
      ...(extError.code ? { code: extError.code } : {}),
      message: error.message,
      ...(extError.hint ? { hint: extError.hint } : {}),
      status,
    },
    links: {
      home: base,
      datasets: `${base}/datasets`,
    },
    user: {
      ip,
      ray,
      colo: cf.colo,
      country: cf.country,
      city: cf.city,
      region: cf.region,
      timezone: cf.timezone,
      requestedAt,
      ...(latency !== undefined ? { latency: `${latency}ms` } : {}),
    },
  }, {
    status,
    headers: {
      'Access-Control-Allow-Origin': '*',
    },
  })
}

/**
 * Build a CORS preflight response
 *
 * Includes X-Requested-With header support for CSRF protection.
 * Clients must send this header with mutations to pass CSRF validation.
 */
export function buildCorsPreflightResponse(): Response {
  return new Response(null, {
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, X-Requested-With, X-CSRF-Token, Authorization',
    },
  })
}
