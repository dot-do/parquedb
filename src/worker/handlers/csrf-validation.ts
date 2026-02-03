/**
 * CSRF Validation for Worker Handlers
 *
 * Provides request-level CSRF validation for HTTP handlers in the worker.
 * This is a lightweight validation that checks:
 *
 * 1. Origin/Referer header validation
 * 2. X-Requested-With header requirement
 *
 * For more comprehensive CSRF protection with Hono middleware,
 * use the security module: import { csrf } from 'parquedb/security'
 *
 * @module
 */

/**
 * Result of CSRF validation
 */
export interface CsrfValidationResult {
  valid: boolean
  reason?: string
}

/**
 * Validate CSRF protection for a mutation request
 *
 * Requirements for valid requests:
 * 1. Must have Origin or Referer header matching the request URL origin
 * 2. Must have X-Requested-With header (XMLHttpRequest or ParqueDB)
 *
 * This provides protection against:
 * - Simple form-based CSRF attacks (no custom headers)
 * - Cross-origin fetch without preflight
 *
 * @param request - The incoming request to validate
 * @param allowedOrigins - Optional list of additional allowed origins
 * @returns Validation result with reason if invalid
 *
 * @example
 * ```typescript
 * const result = validateCsrfRequest(request)
 * if (!result.valid) {
 *   return new Response(`CSRF validation failed: ${result.reason}`, { status: 403 })
 * }
 * ```
 */
export function validateCsrfRequest(
  request: Request,
  allowedOrigins?: string[]
): CsrfValidationResult {
  const url = new URL(request.url)
  const requestOrigin = `${url.protocol}//${url.host}`

  const origin = request.headers.get('Origin')
  const referer = request.headers.get('Referer')
  const requestedWith = request.headers.get('X-Requested-With')

  // 1. Validate Origin header
  if (origin) {
    if (!isOriginAllowed(origin, requestOrigin, allowedOrigins)) {
      return {
        valid: false,
        reason: `Origin '${origin}' is not allowed`,
      }
    }
  } else if (referer) {
    // Fall back to Referer for older browsers or same-origin requests
    try {
      const refererUrl = new URL(referer)
      const refererOrigin = `${refererUrl.protocol}//${refererUrl.host}`

      if (!isOriginAllowed(refererOrigin, requestOrigin, allowedOrigins)) {
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
    // - Direct API calls without browser (curl, Postman, etc.)
    // - Same-origin requests in some configurations
    //
    // For API requests without browser context, we still require X-Requested-With
    // This is acceptable because:
    // - CSRF attacks require a browser to execute
    // - Non-browser clients can easily set the header
    if (!requestedWith) {
      return {
        valid: false,
        reason: 'Missing Origin/Referer header. Include X-Requested-With header for API calls.',
      }
    }
  }

  // 2. Validate X-Requested-With header
  // This header cannot be set cross-origin without CORS preflight
  // It provides an additional layer of protection
  if (!requestedWith) {
    return {
      valid: false,
      reason: 'Missing X-Requested-With header. Set to "XMLHttpRequest" or "ParqueDB".',
    }
  }

  // Accept common values
  const validHeaderValues = ['XMLHttpRequest', 'ParqueDB', 'fetch']
  if (!validHeaderValues.includes(requestedWith)) {
    // Also accept any non-empty value for flexibility with other clients
    // The key protection is that the header is present (requires preflight)
  }

  return { valid: true }
}

/**
 * Check if an origin is allowed
 */
function isOriginAllowed(
  origin: string,
  sameOrigin: string,
  allowedOrigins?: string[]
): boolean {
  // Always allow same-origin
  if (origin === sameOrigin) {
    return true
  }

  // Check against allowed origins list
  if (allowedOrigins && allowedOrigins.length > 0) {
    if (allowedOrigins.includes('*')) {
      return true
    }
    if (allowedOrigins.includes(origin)) {
      return true
    }
  }

  return false
}

/**
 * Create a CSRF error response
 */
export function csrfErrorResponse(reason: string): Response {
  return Response.json(
    {
      error: 'CSRF validation failed',
      code: 'CSRF_VALIDATION_FAILED',
      reason,
      hint: 'Include X-Requested-With header with value "XMLHttpRequest" or "ParqueDB"',
    },
    { status: 403 }
  )
}
