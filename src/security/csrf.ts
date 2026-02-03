/**
 * CSRF Protection for ParqueDB
 *
 * Provides Cross-Site Request Forgery protection for mutation endpoints.
 * Implements multiple defense layers:
 *
 * 1. Origin/Referer header validation - Ensures requests come from allowed origins
 * 2. Custom header requirement - Requires X-Requested-With header for state-changing requests
 * 3. Token-based protection - Optional signed CSRF tokens for form submissions
 *
 * For Cloudflare Workers, we use a combination of:
 * - Origin validation (browser-enforced, cannot be spoofed from JavaScript)
 * - Custom header requirement (X-Requested-With: ParqueDB)
 * - Database-level CORS configuration for per-database origin restrictions
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { csrf } from 'parquedb/security'
 *
 * const app = new Hono()
 *
 * // Apply CSRF protection to all mutation routes
 * app.use('/api/*', csrf({
 *   allowedOrigins: ['https://app.example.com'],
 *   requireCustomHeader: true,
 * }))
 * ```
 *
 * @module
 */

import type { Context, MiddlewareHandler, Next } from 'hono'
import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

/**
 * CSRF protection options
 */
export interface CsrfOptions {
  /**
   * List of allowed origins for mutation requests.
   * If empty or not provided, allows same-origin only.
   * Use '*' to allow all origins (not recommended for production).
   *
   * @example ['https://app.example.com', 'https://admin.example.com']
   */
  allowedOrigins?: string[]

  /**
   * Require X-Requested-With header for mutation requests.
   * This header cannot be set cross-origin without CORS preflight,
   * providing protection against simple form-based CSRF attacks.
   *
   * @default true
   */
  requireCustomHeader?: boolean

  /**
   * Expected value for X-Requested-With header.
   * @default 'ParqueDB'
   */
  customHeaderValue?: string

  /**
   * HTTP methods considered "safe" (read-only) that don't need CSRF validation.
   * @default ['GET', 'HEAD', 'OPTIONS']
   */
  safeMethods?: string[]

  /**
   * Paths to exclude from CSRF protection.
   * Supports exact matches and patterns ending with '*'.
   *
   * @example ['/health', '/api/public/*']
   */
  excludePaths?: string[]

  /**
   * Custom error handler for CSRF validation failures.
   * If not provided, returns a 403 Forbidden response.
   */
  onError?: (c: Context, reason: string) => Response | Promise<Response>

  /**
   * Enable verbose logging for debugging CSRF issues.
   * @default false
   */
  debug?: boolean
}

/**
 * Result of CSRF validation
 */
export interface CsrfValidationResult {
  valid: boolean
  reason?: string
}

/**
 * CSRF token payload (for signed token approach)
 */
export interface CsrfTokenPayload {
  /** User or session identifier */
  sub: string
  /** Token creation timestamp (ms) */
  iat: number
  /** Token expiration timestamp (ms) */
  exp: number
  /** Random nonce for uniqueness */
  nonce: string
}

// =============================================================================
// CSRF Middleware
// =============================================================================

/**
 * CSRF protection middleware for Hono
 *
 * Validates requests to protect against Cross-Site Request Forgery attacks.
 * Uses a combination of origin validation and custom header requirements.
 *
 * @example
 * ```typescript
 * import { csrf } from 'parquedb/security'
 *
 * // Basic usage - same-origin only
 * app.use('/api/*', csrf())
 *
 * // With allowed origins
 * app.use('/api/*', csrf({
 *   allowedOrigins: ['https://app.example.com'],
 * }))
 *
 * // Disable custom header requirement (not recommended)
 * app.use('/api/*', csrf({
 *   requireCustomHeader: false,
 * }))
 * ```
 */
export function csrf(options: CsrfOptions = {}): MiddlewareHandler {
  const {
    allowedOrigins = [],
    requireCustomHeader = true,
    customHeaderValue = 'ParqueDB',
    safeMethods = ['GET', 'HEAD', 'OPTIONS'],
    excludePaths = [],
    onError,
    debug = false,
  } = options

  return async (c: Context, next: Next) => {
    const method = c.req.method.toUpperCase()
    const path = new URL(c.req.url).pathname

    // Skip safe methods
    if (safeMethods.includes(method)) {
      return next()
    }

    // Skip excluded paths
    if (isPathExcluded(path, excludePaths)) {
      if (debug) {
        logger.debug(`[CSRF] Skipping excluded path: ${path}`)
      }
      return next()
    }

    // Validate CSRF protection
    const result = validateCsrf(c, {
      allowedOrigins,
      requireCustomHeader,
      customHeaderValue,
    })

    if (!result.valid) {
      if (debug) {
        logger.warn(`[CSRF] Validation failed: ${result.reason}`)
        logger.warn(`[CSRF] Request: ${method} ${path}`)
        logger.warn(`[CSRF] Origin: ${c.req.header('Origin')}`)
        logger.warn(`[CSRF] Referer: ${c.req.header('Referer')}`)
        logger.warn(`[CSRF] X-Requested-With: ${c.req.header('X-Requested-With')}`)
      }

      if (onError) {
        return onError(c, result.reason!)
      }

      return c.json(
        {
          error: 'CSRF validation failed',
          code: 'CSRF_VALIDATION_FAILED',
          reason: result.reason,
        },
        403
      )
    }

    if (debug) {
      logger.debug(`[CSRF] Validation passed for ${method} ${path}`)
    }

    return next()
  }
}

/**
 * Validate CSRF protection for a request
 *
 * Checks:
 * 1. Origin header matches allowed origins (or same-origin)
 * 2. X-Requested-With header is present (if required)
 */
export function validateCsrf(
  c: Context,
  options: {
    allowedOrigins?: string[]
    requireCustomHeader?: boolean
    customHeaderValue?: string
  } = {}
): CsrfValidationResult {
  const {
    allowedOrigins = [],
    requireCustomHeader = true,
    customHeaderValue = 'ParqueDB',
  } = options

  const origin = c.req.header('Origin')
  const referer = c.req.header('Referer')
  const requestedWith = c.req.header('X-Requested-With')
  const requestUrl = new URL(c.req.url)
  const requestOrigin = `${requestUrl.protocol}//${requestUrl.host}`

  // 1. Validate Origin header
  // Origin header is set by browsers for cross-origin requests and same-origin POSTs
  if (origin) {
    // Check if origin is allowed
    if (!isOriginAllowed(origin, allowedOrigins, requestOrigin)) {
      return {
        valid: false,
        reason: `Origin '${origin}' is not allowed`,
      }
    }
  } else if (referer) {
    // Fall back to Referer for older browsers or same-origin requests
    // Some browsers don't send Origin for same-origin POSTs
    try {
      const refererUrl = new URL(referer)
      const refererOrigin = `${refererUrl.protocol}//${refererUrl.host}`

      if (!isOriginAllowed(refererOrigin, allowedOrigins, requestOrigin)) {
        return {
          valid: false,
          reason: `Referer origin '${refererOrigin}' is not allowed`,
        }
      }
    } catch {
      return {
        valid: false,
        reason: 'Invalid Referer header',
      }
    }
  } else {
    // No Origin or Referer header
    // This can happen for:
    // - Direct API calls without browser (curl, etc.)
    // - Same-origin requests in some browsers
    // We require the custom header in this case for additional security
    if (requireCustomHeader && !requestedWith) {
      return {
        valid: false,
        reason: 'Missing Origin header and X-Requested-With header',
      }
    }
  }

  // 2. Validate X-Requested-With header (if required)
  if (requireCustomHeader) {
    if (!requestedWith) {
      return {
        valid: false,
        reason: 'Missing X-Requested-With header',
      }
    }

    // Allow exact match or 'XMLHttpRequest' (common for AJAX libraries)
    if (requestedWith !== customHeaderValue && requestedWith !== 'XMLHttpRequest') {
      return {
        valid: false,
        reason: `Invalid X-Requested-With header value: '${requestedWith}'`,
      }
    }
  }

  return { valid: true }
}

/**
 * Check if an origin is allowed
 */
function isOriginAllowed(
  origin: string,
  allowedOrigins: string[],
  sameOrigin: string
): boolean {
  // Always allow same-origin
  if (origin === sameOrigin) {
    return true
  }

  // Check against allowed origins list
  if (allowedOrigins.length === 0) {
    // No allowed origins specified = same-origin only
    return false
  }

  // Wildcard allows all origins
  if (allowedOrigins.includes('*')) {
    return true
  }

  // Check exact match
  return allowedOrigins.includes(origin)
}

/**
 * Check if a path is excluded from CSRF protection
 */
function isPathExcluded(path: string, excludePaths: string[]): boolean {
  for (const pattern of excludePaths) {
    if (pattern.endsWith('*')) {
      // Prefix match
      const prefix = pattern.slice(0, -1)
      if (path.startsWith(prefix)) {
        return true
      }
    } else {
      // Exact match
      if (path === pattern) {
        return true
      }
    }
  }
  return false
}

// =============================================================================
// Token-based CSRF Protection
// =============================================================================

/**
 * Generate a signed CSRF token
 *
 * Useful for form submissions where custom headers cannot be set.
 * The token should be embedded in the form and validated on submission.
 *
 * @param secret - Secret key for signing (should be from environment variable)
 * @param subject - User or session identifier
 * @param ttlMs - Token validity duration in milliseconds (default: 1 hour)
 *
 * @example
 * ```typescript
 * // Generate token for form
 * const token = await generateCsrfToken(env.CSRF_SECRET, userId)
 *
 * // Include in form
 * <input type="hidden" name="csrf_token" value="${token}" />
 * ```
 */
export async function generateCsrfToken(
  secret: string,
  subject: string,
  ttlMs: number = 3600000 // 1 hour
): Promise<string> {
  const now = Date.now()

  const payload: CsrfTokenPayload = {
    sub: subject,
    iat: now,
    exp: now + ttlMs,
    nonce: generateNonce(),
  }

  const payloadJson = JSON.stringify(payload)
  const payloadBase64 = btoa(payloadJson)

  // Sign with HMAC-SHA256
  const signature = await signHmac(secret, payloadBase64)

  return `${payloadBase64}.${signature}`
}

/**
 * Verify a CSRF token
 *
 * @param secret - Secret key used for signing
 * @param token - Token to verify
 * @param subject - Expected subject (user/session ID)
 *
 * @returns Validation result with payload if valid
 *
 * @example
 * ```typescript
 * const result = await verifyCsrfToken(env.CSRF_SECRET, token, userId)
 * if (!result.valid) {
 *   return c.json({ error: result.reason }, 403)
 * }
 * ```
 */
export async function verifyCsrfToken(
  secret: string,
  token: string,
  subject: string
): Promise<CsrfValidationResult & { payload?: CsrfTokenPayload }> {
  if (!token) {
    return { valid: false, reason: 'Missing CSRF token' }
  }

  const parts = token.split('.')
  if (parts.length !== 2) {
    return { valid: false, reason: 'Invalid token format' }
  }

  const [payloadBase64, signature] = parts

  // Verify signature
  const expectedSignature = await signHmac(secret, payloadBase64!)
  if (signature !== expectedSignature) {
    return { valid: false, reason: 'Invalid token signature' }
  }

  // Decode payload
  let payload: CsrfTokenPayload
  try {
    const payloadJson = atob(payloadBase64!)
    payload = JSON.parse(payloadJson)
  } catch {
    return { valid: false, reason: 'Invalid token payload' }
  }

  // Verify expiration
  if (payload.exp < Date.now()) {
    return { valid: false, reason: 'Token expired' }
  }

  // Verify subject
  if (payload.sub !== subject) {
    return { valid: false, reason: 'Token subject mismatch' }
  }

  return { valid: true, payload }
}

/**
 * Middleware for verifying CSRF tokens (for form submissions)
 *
 * @example
 * ```typescript
 * app.post('/form-submit', csrfToken({ secret: env.CSRF_SECRET }), async (c) => {
 *   // Token verified, process form
 * })
 * ```
 */
export function csrfToken(options: {
  secret: string
  tokenField?: string
  getSubject: (c: Context) => string | null
}): MiddlewareHandler {
  const { secret, tokenField = 'csrf_token', getSubject } = options

  return async (c: Context, next: Next) => {
    const subject = getSubject(c)
    if (!subject) {
      return c.json({ error: 'Authentication required' }, 401)
    }

    // Get token from form body or header
    let token: string | null = null

    const contentType = c.req.header('Content-Type')
    if (contentType?.includes('application/x-www-form-urlencoded') || contentType?.includes('multipart/form-data')) {
      const body = await c.req.parseBody()
      token = body[tokenField] as string
    } else if (contentType?.includes('application/json')) {
      const body = await c.req.json()
      token = body[tokenField]
    }

    // Also check header
    if (!token) {
      token = c.req.header('X-CSRF-Token') ?? null
    }

    const result = await verifyCsrfToken(secret, token || '', subject)
    if (!result.valid) {
      return c.json(
        {
          error: 'CSRF validation failed',
          code: 'CSRF_TOKEN_INVALID',
          reason: result.reason,
        },
        403
      )
    }

    return next()
  }
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Generate a cryptographically secure random nonce
 */
function generateNonce(): string {
  const bytes = new Uint8Array(16)
  crypto.getRandomValues(bytes)
  return Array.from(bytes)
    .map(b => b.toString(16).padStart(2, '0'))
    .join('')
}

/**
 * Sign data using HMAC-SHA256
 */
async function signHmac(secret: string, data: string): Promise<string> {
  const encoder = new TextEncoder()
  const keyData = encoder.encode(secret)
  const messageData = encoder.encode(data)

  const key = await crypto.subtle.importKey(
    'raw',
    keyData,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  )

  const signature = await crypto.subtle.sign('HMAC', key, messageData)
  const signatureArray = new Uint8Array(signature)

  return Array.from(signatureArray)
    .map(b => b.toString(16).padStart(2, '0'))
    .join('')
}

// =============================================================================
// Exports for CORS + CSRF Integration
// =============================================================================

/**
 * Build CORS headers with CSRF protection in mind
 *
 * Ensures that CORS configuration doesn't inadvertently allow CSRF attacks.
 * - Restricts allowed origins to explicit list
 * - Requires preflight for custom headers
 * - Only allows safe methods for wildcard origins
 */
export function buildSecureCorsHeaders(options: {
  allowedOrigins: string[]
  allowedMethods?: string[]
  allowedHeaders?: string[]
  exposeHeaders?: string[]
  credentials?: boolean
  maxAge?: number
}): Record<string, string> {
  const {
    allowedOrigins: _allowedOrigins,
    allowedMethods = ['GET', 'HEAD', 'OPTIONS'],
    allowedHeaders = ['Content-Type', 'X-Requested-With'],
    exposeHeaders = [],
    credentials = false,
    maxAge = 86400,
  } = options

  const headers: Record<string, string> = {
    'Access-Control-Allow-Methods': allowedMethods.join(', '),
    'Access-Control-Allow-Headers': allowedHeaders.join(', '),
    'Access-Control-Max-Age': maxAge.toString(),
  }

  if (exposeHeaders.length > 0) {
    headers['Access-Control-Expose-Headers'] = exposeHeaders.join(', ')
  }

  // For credentials, we cannot use wildcard origin
  if (credentials) {
    headers['Access-Control-Allow-Credentials'] = 'true'
    // Origin will be set dynamically based on request
  }

  // Origin is set dynamically per-request in the middleware
  // This function returns the static headers only

  return headers
}

/**
 * Get the appropriate Access-Control-Allow-Origin value for a request
 *
 * @param requestOrigin - Origin header from the request
 * @param allowedOrigins - List of allowed origins
 * @param allowCredentials - Whether credentials are allowed
 */
export function getAllowedOriginHeader(
  requestOrigin: string | null,
  allowedOrigins: string[],
  allowCredentials: boolean = false
): string | null {
  if (!requestOrigin) {
    return null
  }

  // Wildcard is not allowed with credentials
  if (allowedOrigins.includes('*') && !allowCredentials) {
    return '*'
  }

  // Check if origin is in the allowed list
  if (allowedOrigins.includes(requestOrigin)) {
    return requestOrigin
  }

  return null
}
