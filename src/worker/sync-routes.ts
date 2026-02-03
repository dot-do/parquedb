/**
 * Sync API Routes
 *
 * Provides endpoints for CLI push/pull/sync operations:
 * - POST /api/sync/register - Register a database
 * - POST /api/sync/upload-urls - Get presigned upload URLs
 * - POST /api/sync/download-urls - Get presigned download URLs
 * - GET /api/sync/manifest/:databaseId - Get manifest
 * - PUT /api/sync/manifest/:databaseId - Update manifest
 */

import type { Env } from '../types/worker'
import { type DatabaseInfo, getUserDatabaseIndex } from './DatabaseIndexDO'
import { DEFAULT_VISIBILITY } from '../types/visibility'
import type { Visibility } from '../types/visibility'
import type { SyncManifest } from '../sync/manifest'
import { MissingBucketError, handleBucketError } from './r2-errors'
import { extractBearerToken, verifyJWT } from './jwt-utils'
import {
  signUploadToken,
  signDownloadToken,
  verifyUploadToken,
  verifyDownloadToken,
  type TokenPayload,
} from './sync-token'

// Re-export token functions for backwards compatibility and testing
export { signUploadToken, signDownloadToken, verifyUploadToken, verifyDownloadToken, type TokenPayload }

// =============================================================================
// Types
// =============================================================================

interface RegisterRequest {
  name: string
  visibility?: Visibility
  slug?: string
  owner: string
}

interface UploadUrlsRequest {
  databaseId: string
  files: Array<{
    path: string
    size: number
    contentType?: string
  }>
}

interface DownloadUrlsRequest {
  databaseId: string
  paths: string[]
}

// =============================================================================
// CORS Headers
// =============================================================================

const SYNC_CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
}

function addCorsHeaders(response: Response): Response {
  const headers = new Headers(response.headers)
  for (const [key, value] of Object.entries(SYNC_CORS_HEADERS)) {
    headers.set(key, value)
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  })
}

// =============================================================================
// Route Handler
// =============================================================================

/**
 * Handle sync API routes
 * Returns null if route doesn't match
 */
export async function handleSyncRoutes(
  request: Request,
  env: Env,
  path: string
): Promise<Response | null> {
  // Handle CORS preflight
  if (request.method === 'OPTIONS' && path.startsWith('/api/sync/')) {
    return new Response(null, {
      status: 204,
      headers: SYNC_CORS_HEADERS,
    })
  }

  // All sync routes require authentication
  const token = extractBearerToken(request)
  if (!token) {
    return addCorsHeaders(Response.json(
      { error: 'Authentication required' },
      { status: 401 }
    ))
  }

  // Verify token with JWKS and extract user info
  const verifyResult = await verifyJWT(token, env)
  if (!verifyResult.valid || !verifyResult.user) {
    return addCorsHeaders(Response.json(
      { error: verifyResult.error ?? 'Invalid token' },
      { status: 401 }
    ))
  }
  const user: UserInfo = {
    id: verifyResult.user.id,
    username: verifyResult.user.username,
  }

  // POST /api/sync/register - Register a database
  if (path === '/api/sync/register' && request.method === 'POST') {
    return addCorsHeaders(await handleRegister(request, env, user))
  }

  // POST /api/sync/upload-urls - Get presigned upload URLs
  if (path === '/api/sync/upload-urls' && request.method === 'POST') {
    return addCorsHeaders(await handleUploadUrls(request, env, user))
  }

  // POST /api/sync/download-urls - Get presigned download URLs
  if (path === '/api/sync/download-urls' && request.method === 'POST') {
    return addCorsHeaders(await handleDownloadUrls(request, env, user))
  }

  // GET/PUT /api/sync/manifest/:databaseId
  const manifestMatch = path.match(/^\/api\/sync\/manifest\/([^/]+)$/)
  if (manifestMatch) {
    const databaseId = manifestMatch[1]!
    if (request.method === 'GET') {
      return addCorsHeaders(await handleGetManifest(env, user, databaseId))
    }
    if (request.method === 'PUT') {
      return addCorsHeaders(await handleUpdateManifest(request, env, user, databaseId))
    }
  }

  return null
}

// =============================================================================
// Handler Implementations
// =============================================================================

/**
 * Register a new database for sync
 */
async function handleRegister(
  request: Request,
  env: Env,
  user: UserInfo
): Promise<Response> {
  try {
    const body = await request.json() as RegisterRequest

    if (!body.name) {
      return Response.json({ error: 'Name is required' }, { status: 400 })
    }

    // Verify ownership if owner specified
    if (body.owner && body.owner !== user.username && body.owner !== user.id) {
      return Response.json(
        { error: 'Cannot register database for another user' },
        { status: 403 }
      )
    }

    const owner = body.owner ?? user.username ?? user.id

    // Get the user's database index
    const index = getUserDatabaseIndex(env as { DATABASE_INDEX: Parameters<typeof getUserDatabaseIndex>[0]['DATABASE_INDEX'] }, user.id)

    // Register the database
    // Note: We use 'parquedb' as the default bucket name since R2 bucket bindings
    // are configured via wrangler.toml and the bucket name isn't exposed at runtime
    const database = await index.register(
      {
        name: body.name,
        bucket: 'parquedb',
        prefix: `${owner}/${body.slug ?? body.name}`,
        visibility: body.visibility ?? DEFAULT_VISIBILITY,
        slug: body.slug,
        owner,
      },
      user.id
    )

    return Response.json({
      id: database.id,
      bucket: database.bucket,
      prefix: database.prefix,
    }, { status: 201 })
  } catch (error) {
    console.error('[handleRegister] Error:', error)
    return Response.json(
      { error: error instanceof Error ? error.message : 'Registration failed' },
      { status: 500 }
    )
  }
}

/**
 * Generate presigned URLs for uploading files
 *
 * Note: Cloudflare R2 supports presigned URLs via the S3 API.
 * For Workers without S3 credentials, we use a different approach:
 * we generate signed URLs that route through our worker.
 */
async function handleUploadUrls(
  request: Request,
  env: Env,
  user: UserInfo
): Promise<Response> {
  try {
    const body = await request.json() as UploadUrlsRequest

    if (!body.databaseId || !body.files?.length) {
      return Response.json(
        { error: 'databaseId and files are required' },
        { status: 400 }
      )
    }

    // Verify user owns the database
    const database = await verifyDatabaseOwnership(env, user, body.databaseId)
    if (!database) {
      return Response.json(
        { error: 'Database not found or access denied' },
        { status: 404 }
      )
    }

    // Validate SYNC_SECRET is configured
    if (!env.SYNC_SECRET) {
      return Response.json(
        { error: 'SYNC_SECRET is not configured. Contact administrator.' },
        { status: 500 }
      )
    }

    // Generate signed upload URLs
    // For simplicity, we generate worker-proxied URLs with signed tokens
    const baseUrl = new URL(request.url).origin
    const expiresAt = new Date(Date.now() + 3600 * 1000).toISOString() // 1 hour

    const urls = await Promise.all(body.files.map(async file => {
      const uploadPath = database.prefix
        ? `${database.prefix}/${file.path}`
        : file.path

      // Create a signed token for this upload using HMAC-SHA256
      const uploadToken = await signUploadToken({
        databaseId: body.databaseId,
        path: file.path,
        userId: user.id,
        expiresAt,
      }, env)

      return {
        path: file.path,
        url: `${baseUrl}/api/sync/upload/${body.databaseId}/${encodeURIComponent(file.path)}`,
        headers: {
          'Content-Type': file.contentType ?? 'application/octet-stream',
          'X-Upload-Token': uploadToken,
        },
        expiresAt,
      }
    }))

    return Response.json({ urls })
  } catch (error) {
    console.error('[handleUploadUrls] Error:', error)
    return Response.json(
      { error: error instanceof Error ? error.message : 'Failed to generate upload URLs' },
      { status: 500 }
    )
  }
}

/**
 * Generate presigned URLs for downloading files
 */
async function handleDownloadUrls(
  request: Request,
  env: Env,
  user: UserInfo
): Promise<Response> {
  try {
    const body = await request.json() as DownloadUrlsRequest

    if (!body.databaseId || !body.paths?.length) {
      return Response.json(
        { error: 'databaseId and paths are required' },
        { status: 400 }
      )
    }

    // Verify user has access to the database
    const database = await verifyDatabaseAccess(env, user, body.databaseId)
    if (!database) {
      return Response.json(
        { error: 'Database not found or access denied' },
        { status: 404 }
      )
    }

    // Validate SYNC_SECRET is configured
    if (!env.SYNC_SECRET) {
      return Response.json(
        { error: 'SYNC_SECRET is not configured. Contact administrator.' },
        { status: 500 }
      )
    }

    // Generate download URLs
    const baseUrl = new URL(request.url).origin
    const expiresAt = new Date(Date.now() + 3600 * 1000).toISOString() // 1 hour

    const urls = await Promise.all(body.paths.map(async path => {
      const downloadToken = await signDownloadToken({
        databaseId: body.databaseId,
        path,
        userId: user.id,
        expiresAt,
      }, env)

      return {
        path,
        url: `${baseUrl}/api/sync/download/${body.databaseId}/${encodeURIComponent(path)}?token=${downloadToken}`,
        expiresAt,
      }
    }))

    return Response.json({ urls })
  } catch (error) {
    console.error('[handleDownloadUrls] Error:', error)
    return Response.json(
      { error: error instanceof Error ? error.message : 'Failed to generate download URLs' },
      { status: 500 }
    )
  }
}

/**
 * Get the manifest for a database
 */
async function handleGetManifest(
  env: Env,
  user: UserInfo,
  databaseId: string
): Promise<Response> {
  try {
    // Validate R2 bucket is configured
    if (!env.BUCKET) {
      throw new MissingBucketError('BUCKET', 'Required for manifest operations.')
    }

    // Verify user has access
    const database = await verifyDatabaseAccess(env, user, databaseId)
    if (!database) {
      return Response.json(
        { error: 'Database not found or access denied' },
        { status: 404 }
      )
    }

    // Read manifest from R2
    const manifestPath = database.prefix
      ? `${database.prefix}/_meta/manifest.json`
      : '_meta/manifest.json'

    const object = await env.BUCKET.get(manifestPath)
    if (!object) {
      return Response.json({ error: 'Manifest not found' }, { status: 404 })
    }

    const manifestText = await object.text()
    const manifest = JSON.parse(manifestText) as SyncManifest

    return Response.json(manifest)
  } catch (error) {
    console.error('[handleGetManifest] Error:', error)
    return Response.json(
      { error: error instanceof Error ? error.message : 'Failed to get manifest' },
      { status: 500 }
    )
  }
}

/**
 * Update the manifest for a database
 */
async function handleUpdateManifest(
  request: Request,
  env: Env,
  user: UserInfo,
  databaseId: string
): Promise<Response> {
  try {
    // Validate R2 bucket is configured
    if (!env.BUCKET) {
      throw new MissingBucketError('BUCKET', 'Required for manifest operations.')
    }

    // Verify user owns the database
    const database = await verifyDatabaseOwnership(env, user, databaseId)
    if (!database) {
      return Response.json(
        { error: 'Database not found or access denied' },
        { status: 404 }
      )
    }

    const manifest = await request.json() as SyncManifest

    // Write manifest to R2
    const manifestPath = database.prefix
      ? `${database.prefix}/_meta/manifest.json`
      : '_meta/manifest.json'

    await env.BUCKET.put(manifestPath, JSON.stringify(manifest, null, 2), {
      httpMetadata: { contentType: 'application/json' },
    })

    return Response.json({ success: true })
  } catch (error) {
    console.error('[handleUpdateManifest] Error:', error)
    return Response.json(
      { error: error instanceof Error ? error.message : 'Failed to update manifest' },
      { status: 500 }
    )
  }
}

// =============================================================================
// Upload/Download Handlers (called via presigned URLs)
// =============================================================================

/**
 * Handle file upload via signed URL
 */
export async function handleUpload(
  request: Request,
  env: Env,
  databaseId: string,
  filePath: string
): Promise<Response> {
  try {
    // Validate R2 bucket is configured
    if (!env.BUCKET) {
      const error = new MissingBucketError('BUCKET', 'Required for file uploads.')
      const bucketErrorResponse = handleBucketError(error)
      if (bucketErrorResponse) {
        return addCorsHeaders(bucketErrorResponse)
      }
    }

    // Verify upload token
    const uploadToken = request.headers.get('X-Upload-Token')
    if (!uploadToken) {
      return addCorsHeaders(Response.json(
        { error: 'Upload token required' },
        { status: 401 }
      ))
    }

    // Validate SYNC_SECRET is configured for token verification
    if (!env.SYNC_SECRET) {
      return addCorsHeaders(Response.json(
        { error: 'SYNC_SECRET is not configured. Contact administrator.' },
        { status: 500 }
      ))
    }

    const tokenData = await verifyUploadToken(uploadToken, env)
    if (!tokenData || tokenData.databaseId !== databaseId || tokenData.path !== filePath) {
      return addCorsHeaders(Response.json(
        { error: 'Invalid or expired upload token' },
        { status: 403 }
      ))
    }

    // Get database info
    const index = getUserDatabaseIndex(env as { DATABASE_INDEX: Parameters<typeof getUserDatabaseIndex>[0]['DATABASE_INDEX'] }, tokenData.userId)
    const database = await index.get(databaseId)
    if (!database) {
      return addCorsHeaders(Response.json(
        { error: 'Database not found' },
        { status: 404 }
      ))
    }

    // Upload to R2
    const fullPath = database.prefix
      ? `${database.prefix}/${filePath}`
      : filePath

    const contentType = request.headers.get('Content-Type') ?? 'application/octet-stream'
    const body = await request.arrayBuffer()

    await env.BUCKET!.put(fullPath, body, {
      httpMetadata: { contentType },
    })

    return addCorsHeaders(Response.json({ success: true, path: filePath }))
  } catch (error) {
    console.error('[handleUpload] Error:', error)
    return addCorsHeaders(Response.json(
      { error: error instanceof Error ? error.message : 'Upload failed' },
      { status: 500 }
    ))
  }
}

/**
 * Handle file download via signed URL
 */
export async function handleDownload(
  request: Request,
  env: Env,
  databaseId: string,
  filePath: string
): Promise<Response> {
  try {
    // Validate R2 bucket is configured
    if (!env.BUCKET) {
      const error = new MissingBucketError('BUCKET', 'Required for file downloads.')
      const bucketErrorResponse = handleBucketError(error)
      if (bucketErrorResponse) {
        return addCorsHeaders(bucketErrorResponse)
      }
    }

    // Verify download token
    const url = new URL(request.url)
    const downloadToken = url.searchParams.get('token')
    if (!downloadToken) {
      return addCorsHeaders(Response.json(
        { error: 'Download token required' },
        { status: 401 }
      ))
    }

    // Validate SYNC_SECRET is configured for token verification
    if (!env.SYNC_SECRET) {
      return addCorsHeaders(Response.json(
        { error: 'SYNC_SECRET is not configured. Contact administrator.' },
        { status: 500 }
      ))
    }

    const tokenData = await verifyDownloadToken(downloadToken, env)
    if (!tokenData || tokenData.databaseId !== databaseId || tokenData.path !== filePath) {
      return addCorsHeaders(Response.json(
        { error: 'Invalid or expired download token' },
        { status: 403 }
      ))
    }

    // Get database info
    const index = getUserDatabaseIndex(env as { DATABASE_INDEX: Parameters<typeof getUserDatabaseIndex>[0]['DATABASE_INDEX'] }, tokenData.userId)
    const database = await index.get(databaseId)
    if (!database) {
      return addCorsHeaders(Response.json(
        { error: 'Database not found' },
        { status: 404 }
      ))
    }

    // Download from R2
    const fullPath = database.prefix
      ? `${database.prefix}/${filePath}`
      : filePath

    const object = await env.BUCKET!.get(fullPath)
    if (!object) {
      return addCorsHeaders(Response.json(
        { error: 'File not found' },
        { status: 404 }
      ))
    }

    const headers = new Headers()
    headers.set('Content-Type', object.httpMetadata?.contentType ?? 'application/octet-stream')
    headers.set('Content-Length', object.size.toString())
    headers.set('ETag', object.etag)

    // Add CORS headers
    for (const [key, value] of Object.entries(SYNC_CORS_HEADERS)) {
      headers.set(key, value)
    }

    return new Response(object.body as ReadableStream, { headers })
  } catch (error) {
    console.error('[handleDownload] Error:', error)
    return addCorsHeaders(Response.json(
      { error: error instanceof Error ? error.message : 'Download failed' },
      { status: 500 }
    ))
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

interface UserInfo {
  id: string
  username?: string
}

/**
 * Verify user owns the database
 */
async function verifyDatabaseOwnership(
  env: Env,
  user: UserInfo,
  databaseId: string
): Promise<DatabaseInfo | null> {
  try {
    const index = getUserDatabaseIndex(env as { DATABASE_INDEX: Parameters<typeof getUserDatabaseIndex>[0]['DATABASE_INDEX'] }, user.id)
    const database = await index.get(databaseId)
    return database
  } catch {
    return null
  }
}

/**
 * Verify user has access to database (read access)
 */
async function verifyDatabaseAccess(
  env: Env,
  user: UserInfo,
  databaseId: string
): Promise<DatabaseInfo | null> {
  // For now, just check ownership
  // TODO: Add support for shared databases
  return verifyDatabaseOwnership(env, user, databaseId)
}

// =============================================================================
// Token Signing (HMAC-SHA256 based tokens)
// =============================================================================

/**
 * Token payload for upload/download signed URLs
 * @internal Exported for testing purposes
 */
export interface TokenPayload {
  databaseId: string
  path: string
  userId: string
  expiresAt: string
}
