/**
 * CORS (Cross-Origin Resource Sharing) Utilities
 *
 * Centralized CORS handling for all worker routes.
 *
 * Security Policy:
 * - BASIC_CORS_HEADERS: Simple Access-Control-Allow-Origin for JSON responses
 * - PUBLIC_CORS_HEADERS: For public database endpoints (read-only, no auth needed)
 * - AUTHENTICATED_CORS_HEADERS: For endpoints requiring Authorization header
 * - SYNC_CORS_HEADERS: For sync API with full CRUD operations
 * - PREFLIGHT_CORS_HEADERS: Full headers for OPTIONS preflight with CSRF support
 *
 * Note: Even with permissive CORS, all access control is enforced server-side via
 * visibility checks and token validation. CORS only controls browser-based
 * cross-origin requests.
 */

import { SECONDS_PER_DAY } from "../constants";

// =============================================================================
// CORS Header Constants
// =============================================================================

/**
 * Basic CORS header - just allows any origin
 * Used for simple JSON responses that don't need method/header control
 */
export const BASIC_CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
} as const;

/**
 * CORS headers for public database endpoints (/api/public, /api/db/:owner/:slug, /db/:owner/:slug/*)
 * These endpoints are designed for anonymous read access to public/unlisted databases.
 * - Origin: '*' allows any site to embed public database content (intentional for data sharing)
 * - Methods: Read-only (GET, HEAD, OPTIONS) - no mutations allowed via public routes
 * - Headers: Range is needed for efficient Parquet partial reads; no Authorization needed for public data
 * - Security: Visibility checks are enforced server-side; only public/unlisted data is accessible without auth
 */
export const PUBLIC_CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Range",
  "Access-Control-Expose-Headers":
    "Content-Range, Content-Length, ETag, Accept-Ranges",
  "Access-Control-Max-Age": String(SECONDS_PER_DAY),
} as const;

/**
 * CORS headers for endpoints requiring authentication
 * - Authorization header is allowed for Bearer token authentication
 * - Used when accessing private databases or performing authenticated operations
 */
export const AUTHENTICATED_CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Range, Authorization",
  "Access-Control-Expose-Headers":
    "Content-Range, Content-Length, ETag, Accept-Ranges",
  "Access-Control-Max-Age": String(SECONDS_PER_DAY),
} as const;

/**
 * CORS headers for sync API endpoints
 * Full CRUD operations with Authorization support
 */
export const SYNC_CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Max-Age": String(SECONDS_PER_DAY),
} as const;

/**
 * CORS headers for preflight responses (OPTIONS requests)
 * Includes X-Requested-With header support for CSRF protection.
 * Clients must send this header with mutations to pass CSRF validation.
 */
export const PREFLIGHT_CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PATCH, DELETE, OPTIONS",
  "Access-Control-Allow-Headers":
    "Content-Type, X-Requested-With, X-CSRF-Token, Authorization",
} as const;

// =============================================================================
// CORS Response Builders
// =============================================================================

/**
 * Build a CORS preflight response for OPTIONS requests
 *
 * @returns Response with 204 status and full CORS headers
 */
export function buildCorsPreflightResponse(): Response {
  return new Response(null, {
    status: 204,
    headers: PREFLIGHT_CORS_HEADERS,
  });
}

/**
 * Build a preflight response with custom CORS headers
 *
 * @param headers - Custom CORS headers to use
 * @returns Response with 204 status and provided headers
 */
export function buildPreflightResponse(
  headers: Record<string, string>,
): Response {
  return new Response(null, {
    status: 204,
    headers,
  });
}

// =============================================================================
// CORS Header Helpers
// =============================================================================

/**
 * Add CORS headers to an existing response
 *
 * @param response - Original response
 * @param corsHeaders - CORS headers to add
 * @returns New response with CORS headers
 */
export function addCorsHeaders(
  response: Response,
  corsHeaders: Record<string, string> = BASIC_CORS_HEADERS,
): Response {
  const headers = new Headers(response.headers);
  for (const [key, value] of Object.entries(corsHeaders)) {
    headers.set(key, value);
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

/**
 * Add public CORS headers to response (no Authorization header needed)
 *
 * @param response - Original response
 * @returns New response with public CORS headers
 */
export function addPublicCorsHeaders(response: Response): Response {
  return addCorsHeaders(response, PUBLIC_CORS_HEADERS);
}

/**
 * Add authenticated CORS headers to response (includes Authorization header)
 *
 * @param response - Original response
 * @returns New response with authenticated CORS headers
 */
export function addAuthenticatedCorsHeaders(response: Response): Response {
  return addCorsHeaders(response, AUTHENTICATED_CORS_HEADERS);
}

/**
 * Add sync API CORS headers to response
 *
 * @param response - Original response
 * @returns New response with sync CORS headers
 */
export function addSyncCorsHeaders(response: Response): Response {
  return addCorsHeaders(response, SYNC_CORS_HEADERS);
}

// =============================================================================
// CORS Middleware
// =============================================================================

/**
 * Handle CORS for a request
 *
 * If the request is an OPTIONS preflight, returns a preflight response.
 * Otherwise, returns null to continue processing.
 *
 * @param request - Incoming request
 * @param corsHeaders - Optional custom CORS headers (defaults to PREFLIGHT_CORS_HEADERS)
 * @returns Preflight response for OPTIONS, null otherwise
 */
export function handleCors(
  request: Request,
  corsHeaders: Record<string, string> = PREFLIGHT_CORS_HEADERS,
): Response | null {
  if (request.method === "OPTIONS") {
    return buildPreflightResponse(corsHeaders);
  }
  return null;
}

/**
 * Handle CORS for public routes
 *
 * @param request - Incoming request
 * @returns Preflight response for OPTIONS, null otherwise
 */
export function handlePublicCors(request: Request): Response | null {
  return handleCors(request, PUBLIC_CORS_HEADERS);
}

/**
 * Handle CORS for authenticated routes
 *
 * @param request - Incoming request
 * @returns Preflight response for OPTIONS, null otherwise
 */
export function handleAuthenticatedCors(request: Request): Response | null {
  return handleCors(request, AUTHENTICATED_CORS_HEADERS);
}

/**
 * Handle CORS for sync API routes
 *
 * @param request - Incoming request
 * @returns Preflight response for OPTIONS, null otherwise
 */
export function handleSyncCors(request: Request): Response | null {
  return handleCors(request, SYNC_CORS_HEADERS);
}
