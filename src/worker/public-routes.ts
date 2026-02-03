/**
 * Public Database Routes
 *
 * HTTP routes for accessing public and unlisted databases.
 * Supports Range requests for efficient Parquet file reading.
 *
 * Routes:
 * - GET /api/public - List public databases
 * - GET /api/db/:owner/:slug - Database metadata
 * - GET /api/db/:owner/:slug/:collection - Query collection
 * - GET /db/:owner/:slug/* - Raw file access with Range support
 *
 * Security:
 * - Rate limiting on all public endpoints to prevent DoS and enumeration attacks
 * - CORS headers for cross-origin access control
 * - Visibility checks for database access
 */

import type { Env } from '../types/worker'
import { type DatabaseIndexDO, type DatabaseInfo } from './DatabaseIndexDO'
import { allowsAnonymousRead } from '../types/visibility'
import { getDOStub } from '../utils/type-utils'
import {
  type RateLimitDO,
  type RateLimitResult,
  getClientId,
  buildRateLimitHeaders,
  buildRateLimitResponse,
} from './RateLimitDO'
import { MissingBucketError } from './r2-errors'
import { extractBearerToken, verifyOwnership } from './jwt-utils'

/**
 * Get the database index stub for a user
 */
function getDatabaseIndex(env: Env, owner: string): DatabaseIndexDO | null {
  if (!env.DATABASE_INDEX) return null
  const indexId = env.DATABASE_INDEX.idFromName(`user:${owner}`)
  return getDOStub<DatabaseIndexDO>(env.DATABASE_INDEX, indexId)
}

// =============================================================================
// CORS Headers
// =============================================================================

/**
 * CORS Security Policy:
 *
 * PUBLIC_CORS_HEADERS: Used for public database endpoints (/api/public, /api/db/:owner/:slug, /db/:owner/:slug/*).
 * These endpoints are designed for anonymous read access to public/unlisted databases.
 * - Origin: '*' allows any site to embed public database content (intentional for data sharing)
 * - Methods: Read-only (GET, HEAD, OPTIONS) - no mutations allowed via public routes
 * - Headers: Range is needed for efficient Parquet partial reads; no Authorization needed for public data
 * - Security: Visibility checks are enforced server-side; only public/unlisted data is accessible without auth
 *
 * AUTHENTICATED_CORS_HEADERS: Used for endpoints requiring authentication.
 * - Authorization header is allowed for Bearer token authentication
 * - Used when accessing private databases or performing mutations
 *
 * Note: Even with permissive CORS, all access control is enforced server-side via visibility checks
 * and token validation. CORS only controls browser-based cross-origin requests.
 */
const PUBLIC_CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Range',
  'Access-Control-Expose-Headers': 'Content-Range, Content-Length, ETag, Accept-Ranges',
  'Access-Control-Max-Age': '86400',
}

const AUTHENTICATED_CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Range, Authorization',
  'Access-Control-Expose-Headers': 'Content-Range, Content-Length, ETag, Accept-Ranges',
  'Access-Control-Max-Age': '86400',
}

/**
 * Add public CORS headers to response (no Authorization header needed)
 */
function addPublicCorsHeaders(response: Response): Response {
  const headers = new Headers(response.headers)
  for (const [key, value] of Object.entries(PUBLIC_CORS_HEADERS)) {
    headers.set(key, value)
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  })
}

/**
 * Add authenticated CORS headers to response (includes Authorization header)
 */
function addAuthenticatedCorsHeaders(response: Response): Response {
  const headers = new Headers(response.headers)
  for (const [key, value] of Object.entries(AUTHENTICATED_CORS_HEADERS)) {
    headers.set(key, value)
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  })
}

/**
 * Add rate limit headers to response
 */
function addRateLimitHeaders(response: Response, rateLimitResult: RateLimitResult): Response {
  const headers = new Headers(response.headers)
  const rateLimitHeaders = buildRateLimitHeaders(rateLimitResult)
  for (const [key, value] of Object.entries(rateLimitHeaders)) {
    headers.set(key, value)
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  })
}

// =============================================================================
// Rate Limiting
// =============================================================================

/**
 * Endpoint type mapping for rate limiting
 * Maps URL patterns to rate limit categories
 */
type EndpointType = 'public' | 'database' | 'query' | 'file'

/**
 * Get endpoint type from path for rate limiting
 */
function getEndpointType(path: string): EndpointType {
  if (path === '/api/public') return 'public'
  if (path.match(/^\/api\/db\/[^/]+\/[^/]+$/)) return 'database'
  if (path.match(/^\/api\/db\/[^/]+\/[^/]+\/[^/]+$/)) return 'query'
  if (path.startsWith('/db/')) return 'file'
  return 'public' // Default fallback
}

/**
 * Check rate limit for a request
 *
 * @param request - Incoming request
 * @param env - Worker environment
 * @param endpointType - Type of endpoint for rate limit category
 * @returns Rate limit result or null if rate limiting is not configured
 */
async function checkRateLimit(
  request: Request,
  env: Env,
  endpointType: EndpointType
): Promise<RateLimitResult | null> {
  // Rate limiting requires RATE_LIMITER binding
  if (!env.RATE_LIMITER) {
    return null
  }

  const clientId = getClientId(request)
  const rateLimitId = env.RATE_LIMITER.idFromName(clientId)
  const limiter = env.RATE_LIMITER.get(rateLimitId) as unknown as RateLimitDO

  try {
    return await limiter.checkLimit(endpointType)
  } catch (error) {
    // If rate limiting fails, log but allow the request through
    console.error('[RateLimit] Error checking rate limit:', error)
    return null
  }
}

// =============================================================================
// Route Handlers
// =============================================================================

/**
 * Handle public database routes
 * Returns null if route doesn't match
 *
 * Rate limiting is applied to all public endpoints to prevent:
 * - DoS attacks that could overwhelm the database
 * - Database enumeration through brute-force requests
 * - Resource exhaustion from excessive file reads
 */
export async function handlePublicRoutes(
  request: Request,
  env: Env,
  path: string,
  _baseUrl: string
): Promise<Response | null> {
  // Handle CORS preflight for public routes
  // Public routes use PUBLIC_CORS_HEADERS (no Authorization header needed for anonymous access)
  if (request.method === 'OPTIONS') {
    if (path.startsWith('/api/public') || path.startsWith('/api/db/') || path.startsWith('/db/')) {
      return new Response(null, {
        status: 204,
        headers: PUBLIC_CORS_HEADERS,
      })
    }
  }

  // Check if this is a public route that needs rate limiting
  const isPublicRoute =
    path === '/api/public' ||
    path.startsWith('/api/db/') ||
    path.startsWith('/db/')

  if (!isPublicRoute) {
    return null
  }

  // Apply rate limiting
  const endpointType = getEndpointType(path)
  const rateLimitResult = await checkRateLimit(request, env, endpointType)

  // If rate limited, return 429 response
  if (rateLimitResult && !rateLimitResult.allowed) {
    return addPublicCorsHeaders(buildRateLimitResponse(rateLimitResult))
  }

  // Helper to add rate limit headers to response
  const withRateLimitHeaders = (response: Response): Response => {
    if (rateLimitResult) {
      return addRateLimitHeaders(response, rateLimitResult)
    }
    return response
  }

  // GET /api/public - List public databases (no auth needed)
  if (path === '/api/public' && request.method === 'GET') {
    const response = await handleListPublic(request, env)
    return withRateLimitHeaders(addPublicCorsHeaders(response))
  }

  // GET /api/db/:owner/:slug - Database metadata
  // Uses authenticated CORS since private databases may require Authorization header
  const dbMetaMatch = path.match(/^\/api\/db\/([^/]+)\/([^/]+)$/)
  if (dbMetaMatch && request.method === 'GET') {
    const [, owner, slug] = dbMetaMatch
    const response = await handleDatabaseMeta(request, env, owner!, slug!)
    return withRateLimitHeaders(addAuthenticatedCorsHeaders(response))
  }

  // GET /api/db/:owner/:slug/:collection - Query collection
  // Uses authenticated CORS since private databases may require Authorization header
  const collectionMatch = path.match(/^\/api\/db\/([^/]+)\/([^/]+)\/([^/]+)$/)
  if (collectionMatch && request.method === 'GET') {
    const [, owner, slug, collection] = collectionMatch
    const response = await handleCollectionQuery(request, env, owner!, slug!, collection!)
    return withRateLimitHeaders(addAuthenticatedCorsHeaders(response))
  }

  // GET /db/:owner/:slug/* - Raw file access
  // Uses authenticated CORS since private databases may require Authorization header
  const rawFileMatch = path.match(/^\/db\/([^/]+)\/([^/]+)\/(.+)$/)
  if (rawFileMatch && (request.method === 'GET' || request.method === 'HEAD')) {
    const [, owner, slug, filePath] = rawFileMatch
    const response = await handleRawFileAccess(request, env, owner!, slug!, filePath!)
    return withRateLimitHeaders(addAuthenticatedCorsHeaders(response))
  }

  return null
}

// =============================================================================
// Handler Implementations
// =============================================================================

/**
 * List all public (discoverable) databases
 */
async function handleListPublic(
  request: Request,
  env: Env
): Promise<Response> {
  try {
    // Get query parameters
    const url = new URL(request.url)
    const limit = parseInt(url.searchParams.get('limit') ?? '50', 10)
    const offset = parseInt(url.searchParams.get('offset') ?? '0', 10)

    // For now, we need a way to aggregate public databases across all users
    // This would typically require a global index or aggregation service
    // For MVP, return empty list or use a known list of public database owners

    // TODO: Implement global public database index
    // For now, return a placeholder response
    const databases: DatabaseInfo[] = []

    return Response.json({
      databases: databases.slice(offset, offset + limit),
      total: databases.length,
      hasMore: offset + limit < databases.length,
    })
  } catch (error) {
    return Response.json(
      { error: error instanceof Error ? error.message : 'Failed to list public databases' },
      { status: 500 }
    )
  }
}

/**
 * Get database metadata by owner/slug
 */
async function handleDatabaseMeta(
  request: Request,
  env: Env,
  owner: string,
  slug: string
): Promise<Response> {
  try {
    // Get the user's database index
    const index = getDatabaseIndex(env, owner)
    if (!index) {
      return Response.json(
        { error: 'Database index not configured' },
        { status: 503 }
      )
    }

    const database = await index.getBySlug(owner, slug)

    if (!database) {
      return Response.json({ error: 'Database not found' }, { status: 404 })
    }

    // Check visibility permissions
    const token = extractBearerToken(request)
    const isOwner = await verifyOwnership(token, owner, env)

    if (!allowsAnonymousRead(database.visibility) && !isOwner) {
      return Response.json(
        { error: 'Authentication required' },
        { status: 401 }
      )
    }

    // Return database info (exclude sensitive fields for non-owners)
    const publicInfo = {
      id: database.id,
      name: database.name,
      description: database.description,
      owner: database.owner,
      slug: database.slug,
      visibility: database.visibility,
      collectionCount: database.collectionCount,
      entityCount: database.entityCount,
      createdAt: database.createdAt,
    }

    return Response.json(publicInfo)
  } catch (error) {
    return Response.json(
      { error: error instanceof Error ? error.message : 'Failed to get database' },
      { status: 500 }
    )
  }
}

/**
 * Query a collection in a public database
 */
async function handleCollectionQuery(
  request: Request,
  env: Env,
  owner: string,
  slug: string,
  collection: string
): Promise<Response> {
  try {
    // Get database info
    const index = getDatabaseIndex(env, owner)
    if (!index) {
      return Response.json(
        { error: 'Database index not configured' },
        { status: 503 }
      )
    }

    const database = await index.getBySlug(owner, slug)
    if (!database) {
      return Response.json({ error: 'Database not found' }, { status: 404 })
    }

    // Check visibility
    const token = extractBearerToken(request)
    const isOwner = await verifyOwnership(token, owner, env)

    if (!allowsAnonymousRead(database.visibility) && !isOwner) {
      return Response.json(
        { error: 'Authentication required' },
        { status: 401 }
      )
    }

    // Parse query parameters (for future query implementation)
    const url = new URL(request.url)
    const _filter = url.searchParams.get('filter')
    const _limit = parseInt(url.searchParams.get('limit') ?? '100', 10)
    const _offset = parseInt(url.searchParams.get('offset') ?? '0', 10)
    void _filter; void _limit; void _offset // Will be used in future implementation

    // TODO: Execute query against the database's R2 bucket
    // For now, return placeholder
    return Response.json({
      items: [],
      total: 0,
      hasMore: false,
      collection,
      database: `${owner}/${slug}`,
    })
  } catch (error) {
    return Response.json(
      { error: error instanceof Error ? error.message : 'Query failed' },
      { status: 500 }
    )
  }
}

/**
 * Raw file access with Range request support
 *
 * Critical for efficient Parquet reading where clients need to:
 * 1. Read footer (last 8 bytes) to get footer length
 * 2. Read footer to get metadata
 * 3. Read specific row groups based on predicate pushdown
 */
async function handleRawFileAccess(
  request: Request,
  env: Env,
  owner: string,
  slug: string,
  filePath: string
): Promise<Response> {
  try {
    // Validate R2 bucket is configured
    if (!env.BUCKET) {
      throw new MissingBucketError('BUCKET', 'Required for file access.')
    }

    // Get database info
    const index = getDatabaseIndex(env, owner)
    if (!index) {
      return new Response('Service Unavailable', { status: 503 })
    }

    const database = await index.getBySlug(owner, slug)
    if (!database) {
      return new Response('Not Found', { status: 404 })
    }

    // Check visibility
    const token = extractBearerToken(request)
    const isOwner = await verifyOwnership(token, owner, env)

    if (!allowsAnonymousRead(database.visibility) && !isOwner) {
      return new Response('Unauthorized', { status: 401 })
    }

    // Build the full path in R2
    const r2Path = database.prefix
      ? `${database.prefix}/${filePath}`
      : filePath

    // Get the R2 object
    const bucket = env.BUCKET

    // Parse Range header
    const rangeHeader = request.headers.get('Range')

    // Fetch from R2
    let object: R2Object | R2ObjectBody | null
    if (request.method === 'HEAD') {
      object = await bucket.head(r2Path)
    } else if (rangeHeader) {
      const range = parseRangeHeader(rangeHeader)
      if (range) {
        // Use the range option for partial reads
        object = await bucket.get(r2Path, { range } as Parameters<typeof bucket.get>[1])
      } else {
        object = await bucket.get(r2Path)
      }
    } else {
      object = await bucket.get(r2Path)
    }

    if (!object) {
      return new Response('Not Found', { status: 404 })
    }

    // Build response headers
    const headers = new Headers()
    headers.set('Content-Type', object.httpMetadata?.contentType ?? 'application/octet-stream')
    headers.set('ETag', object.etag)
    headers.set('Accept-Ranges', 'bytes')

    if (object.size !== undefined) {
      headers.set('Content-Length', object.size.toString())
    }

    // Handle Range response
    if (rangeHeader && 'body' in object && object.range) {
      const r2Range = object.range as { offset: number; length: number }
      const contentLength = r2Range.length
      const start = r2Range.offset
      const end = start + contentLength - 1
      const total = object.size

      headers.set('Content-Length', contentLength.toString())
      headers.set('Content-Range', `bytes ${start}-${end}/${total}`)

      // Cast body to ReadableStream for Response constructor
      const body = (object as { body: ReadableStream }).body
      return new Response(body, {
        status: 206,
        headers,
      })
    }

    // Full response
    if (request.method === 'HEAD') {
      return new Response(null, {
        status: 200,
        headers,
      })
    }

    // Cast body to ReadableStream for Response constructor
    const body = 'body' in object ? (object as { body: ReadableStream }).body : null
    return new Response(body, {
      status: 200,
      headers,
    })
  } catch (error) {
    console.error('Raw file access error:', error)
    return new Response('Internal Server Error', { status: 500 })
  }
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Parse Range header into R2 range options
 */
function parseRangeHeader(header: string): R2Range | null {
  // Format: bytes=start-end or bytes=-suffix or bytes=start-
  const match = header.match(/^bytes=(-?\d*)-(-?\d*)$/)
  if (!match) return null

  const startStr = match[1] ?? ''
  const endStr = match[2] ?? ''

  // Suffix range: bytes=-500 (last 500 bytes)
  if (startStr === '' && endStr !== '') {
    return { suffix: parseInt(endStr, 10) }
  }

  // Range from start: bytes=100-
  if (startStr !== '' && endStr === '') {
    return { offset: parseInt(startStr, 10) }
  }

  // Full range: bytes=100-200
  if (startStr !== '' && endStr !== '') {
    const offset = parseInt(startStr, 10)
    const end = parseInt(endStr, 10)
    return { offset, length: end - offset + 1 }
  }

  return null
}

// =============================================================================
// R2 Types (for Range requests)
// =============================================================================

interface R2Range {
  offset?: number
  length?: number
  suffix?: number
}
