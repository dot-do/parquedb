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
import { logger } from '../utils/logger'
import { SECONDS_PER_DAY, SYNC_TOKEN_URL_EXPIRY_MS } from '../constants'

// Re-export token functions for backwards compatibility and testing
export { signUploadToken, signDownloadToken, verifyUploadToken, verifyDownloadToken, type TokenPayload }

// =============================================================================
// URL Parameter Validation
// =============================================================================

/**
 * Error thrown when URL parameter validation fails
 */
export class InvalidUrlParameterError extends Error {
  override readonly name = 'InvalidUrlParameterError'
  readonly parameter: string
  readonly value: string

  constructor(message: string, parameter: string, value: string) {
    super(message)
    Object.setPrototypeOf(this, InvalidUrlParameterError.prototype)
    this.parameter = parameter
    this.value = value
  }
}

/**
 * Characters that are dangerous in URL parameters
 * - Null byte: Can truncate strings in some systems
 * - Line breaks: Could be used for log injection or header injection
 */
const DANGEROUS_CHARS = ['\0', '\n', '\r']

/**
 * URL-encoded dangerous characters to check before decoding
 */
const ENCODED_DANGEROUS_PATTERNS = [
  '%00',      // Null byte
  '%0a', '%0A', // Line feed
  '%0d', '%0D', // Carriage return
]

/**
 * Patterns indicating path traversal attempts
 * These are checked both before and after URL decoding
 */
const PATH_TRAVERSAL_PATTERNS = [
  '../',    // Unix path traversal
  '..\\',   // Windows path traversal
  '..',     // Generic traversal (at path boundaries)
  '%2e%2e', // URL-encoded ..
  '%2E%2E', // URL-encoded .. (uppercase)
]

/**
 * Check if a value contains dangerous characters
 */
function hasDangerousChars(value: string): boolean {
  return DANGEROUS_CHARS.some(char => value.includes(char))
}

/**
 * Check if a value contains URL-encoded dangerous characters
 */
function hasEncodedDangerousChars(value: string): boolean {
  const lowerValue = value.toLowerCase()
  return ENCODED_DANGEROUS_PATTERNS.some(pattern =>
    lowerValue.includes(pattern.toLowerCase())
  )
}

/**
 * Recursively decode a URL-encoded string up to a maximum depth
 * This handles double-encoding, triple-encoding, etc.
 */
function fullyDecode(value: string, maxDepth: number = 3): string {
  let current = value
  for (let i = 0; i < maxDepth; i++) {
    try {
      const decoded = decodeURIComponent(current)
      if (decoded === current) {
        // No more encoding to decode
        break
      }
      current = decoded
    } catch {
      // Invalid encoding, stop decoding
      break
    }
  }
  return current
}

/**
 * Check if a value contains path traversal patterns
 * Checks the raw value for encoded traversal and decoded value for actual traversal
 */
function hasPathTraversal(value: string): boolean {
  // Check for encoded traversal patterns in the raw value
  const lowerValue = value.toLowerCase()
  if (PATH_TRAVERSAL_PATTERNS.some(p => lowerValue.includes(p.toLowerCase()))) {
    return true
  }

  // Fully decode (handles double/triple encoding) and check again
  try {
    const fullyDecoded = fullyDecode(value)
    if (fullyDecoded !== value) {
      // Value was encoded, check fully decoded version
      const normalizedDecoded = fullyDecoded.replace(/\\/g, '/')
      const parts = normalizedDecoded.split('/')
      if (parts.some(part => part === '..')) {
        return true
      }
    }
  } catch {
    // If decoding fails, the value might be malformed - still check raw value
  }

  // Check raw value with normalized separators
  const normalized = value.replace(/\\/g, '/')
  const parts = normalized.split('/')
  return parts.some(part => part === '..')
}

/**
 * Validate a URL-derived parameter for security issues
 *
 * This function checks for:
 * 1. Null bytes and other dangerous characters
 * 2. URL-encoded dangerous characters
 * 3. Path traversal attempts (../, ..\)
 * 4. Double-encoded path traversal
 *
 * @param value - The parameter value to validate
 * @param paramName - The parameter name for error messages
 * @throws InvalidUrlParameterError if validation fails
 */
export function validateUrlParameter(value: string, paramName: string): void {
  // Empty values are invalid
  if (!value || value.trim() === '') {
    throw new InvalidUrlParameterError(
      `${paramName} cannot be empty`,
      paramName,
      value
    )
  }

  // Check for URL-encoded dangerous characters in raw value
  if (hasEncodedDangerousChars(value)) {
    throw new InvalidUrlParameterError(
      `${paramName} contains dangerous encoded characters`,
      paramName,
      value
    )
  }

  // Check for path traversal in raw and decoded value
  if (hasPathTraversal(value)) {
    throw new InvalidUrlParameterError(
      `${paramName} contains path traversal sequence`,
      paramName,
      value
    )
  }

  // Try to decode and check the decoded value
  let decoded: string
  try {
    decoded = decodeURIComponent(value)
  } catch {
    throw new InvalidUrlParameterError(
      `${paramName} contains invalid URL encoding`,
      paramName,
      value
    )
  }

  // Check decoded value for dangerous characters
  if (hasDangerousChars(decoded)) {
    throw new InvalidUrlParameterError(
      `${paramName} contains dangerous characters`,
      paramName,
      value
    )
  }
}

/**
 * Validate a database ID parameter
 * Database IDs should be alphanumeric with limited special characters
 */
export function validateDatabaseId(databaseId: string): void {
  validateUrlParameter(databaseId, 'databaseId')

  // Database IDs should match a safe pattern (alphanumeric, hyphens, underscores)
  // This prevents any path manipulation even if they pass basic traversal checks
  const safeIdPattern = /^[a-zA-Z0-9_-]+$/
  if (!safeIdPattern.test(databaseId)) {
    throw new InvalidUrlParameterError(
      'databaseId must contain only alphanumeric characters, hyphens, and underscores',
      'databaseId',
      databaseId
    )
  }
}

/**
 * Validate a file path parameter
 * File paths can contain slashes but must not contain traversal sequences
 */
export function validateFilePath(filePath: string): void {
  validateUrlParameter(filePath, 'filePath')

  // Decode the path for additional validation
  let decoded: string
  try {
    decoded = decodeURIComponent(filePath)
  } catch {
    throw new InvalidUrlParameterError(
      'filePath contains invalid URL encoding',
      'filePath',
      filePath
    )
  }

  // File paths should not start with a slash (they're relative paths)
  if (decoded.startsWith('/') || decoded.startsWith('\\')) {
    throw new InvalidUrlParameterError(
      'filePath must be a relative path (cannot start with / or \\)',
      'filePath',
      filePath
    )
  }

  // Validate each path segment
  const segments = decoded.replace(/\\/g, '/').split('/')
  for (const segment of segments) {
    if (segment === '..') {
      throw new InvalidUrlParameterError(
        'filePath contains path traversal sequence',
        'filePath',
        filePath
      )
    }
    if (segment === '.') {
      // Single dots are allowed but unusual - skip them
      continue
    }
    // Check for null bytes in segment
    if (segment.includes('\0')) {
      throw new InvalidUrlParameterError(
        'filePath contains null byte',
        'filePath',
        filePath
      )
    }
  }
}

// =============================================================================
// Types
// =============================================================================

interface RegisterRequest {
  name: string
  visibility?: Visibility | undefined
  slug?: string | undefined
  owner: string
}

interface UploadUrlsRequest {
  databaseId: string
  files: Array<{
    path: string
    size: number
    contentType?: string | undefined
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
  'Access-Control-Max-Age': String(SECONDS_PER_DAY),
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

    // Validate database ID to prevent path traversal attacks
    try {
      validateDatabaseId(databaseId)
    } catch (error) {
      if (error instanceof InvalidUrlParameterError) {
        return addCorsHeaders(Response.json(
          { error: error.message },
          { status: 400 }
        ))
      }
      throw error
    }

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
    logger.error('[handleRegister] Error:', error)
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

    // Validate databaseId to prevent injection attacks
    try {
      validateDatabaseId(body.databaseId)
    } catch (error) {
      if (error instanceof InvalidUrlParameterError) {
        return Response.json(
          { error: error.message },
          { status: 400 }
        )
      }
      throw error
    }

    // Validate all file paths to prevent path traversal
    for (const file of body.files) {
      try {
        validateFilePath(file.path)
      } catch (error) {
        if (error instanceof InvalidUrlParameterError) {
          return Response.json(
            { error: `Invalid file path '${file.path}': ${error.message}` },
            { status: 400 }
          )
        }
        throw error
      }
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
    const expiresAt = new Date(Date.now() + SYNC_TOKEN_URL_EXPIRY_MS).toISOString() // 1 hour

    const urls = await Promise.all(body.files.map(async file => {
      const _uploadPath = database.prefix
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
    logger.error('[handleUploadUrls] Error:', error)
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

    // Validate databaseId to prevent injection attacks
    try {
      validateDatabaseId(body.databaseId)
    } catch (error) {
      if (error instanceof InvalidUrlParameterError) {
        return Response.json(
          { error: error.message },
          { status: 400 }
        )
      }
      throw error
    }

    // Validate all file paths to prevent path traversal
    for (const path of body.paths) {
      try {
        validateFilePath(path)
      } catch (error) {
        if (error instanceof InvalidUrlParameterError) {
          return Response.json(
            { error: `Invalid file path '${path}': ${error.message}` },
            { status: 400 }
          )
        }
        throw error
      }
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
    const expiresAt = new Date(Date.now() + SYNC_TOKEN_URL_EXPIRY_MS).toISOString() // 1 hour

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
    logger.error('[handleDownloadUrls] Error:', error)
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
    logger.error('[handleGetManifest] Error:', error)
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
    logger.error('[handleUpdateManifest] Error:', error)
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
    // Validate URL-derived parameters to prevent path traversal attacks
    try {
      validateDatabaseId(databaseId)
      validateFilePath(filePath)
    } catch (error) {
      if (error instanceof InvalidUrlParameterError) {
        return addCorsHeaders(Response.json(
          { error: error.message },
          { status: 400 }
        ))
      }
      throw error
    }

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
    logger.error('[handleUpload] Error:', error)
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
    // Validate URL-derived parameters to prevent path traversal attacks
    try {
      validateDatabaseId(databaseId)
      validateFilePath(filePath)
    } catch (error) {
      if (error instanceof InvalidUrlParameterError) {
        return addCorsHeaders(Response.json(
          { error: error.message },
          { status: 400 }
        ))
      }
      throw error
    }

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
    logger.error('[handleDownload] Error:', error)
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
  username?: string | undefined
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
  // Currently only ownership is checked. Shared database access would require:
  // 1. A sharing/permissions model in the database metadata
  // 2. Checking if the user has been granted access
  // For now, only owners can access their databases.
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
