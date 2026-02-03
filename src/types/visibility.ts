/**
 * Visibility Types for ParqueDB
 *
 * Controls access to databases and collections:
 * - public: Discoverable and accessible by anyone
 * - unlisted: Accessible with direct link, not discoverable
 * - private: Requires authentication
 */

/**
 * Visibility level for databases and collections
 *
 * | Value     | Discoverable | Anonymous Read | Requires Auth |
 * |-----------|-------------|----------------|---------------|
 * | public    | Yes         | Yes            | No            |
 * | unlisted  | No          | Yes (with link)| No            |
 * | private   | No          | No             | Yes           |
 */
export type Visibility = 'public' | 'unlisted' | 'private'

/**
 * Default visibility for new databases and collections
 * Private by default for security
 */
export const DEFAULT_VISIBILITY: Visibility = 'private'

/**
 * All valid visibility values
 */
export const VISIBILITY_VALUES: readonly Visibility[] = ['public', 'unlisted', 'private'] as const

/**
 * Check if a value is a valid visibility
 */
export function isValidVisibility(value: unknown): value is Visibility {
  return typeof value === 'string' && VISIBILITY_VALUES.includes(value as Visibility)
}

/**
 * Parse visibility from string, returning default if invalid
 */
export function parseVisibility(value: string | undefined): Visibility {
  if (value && isValidVisibility(value)) {
    return value
  }
  return DEFAULT_VISIBILITY
}

/**
 * Check if visibility allows anonymous read access
 */
export function allowsAnonymousRead(visibility: Visibility): boolean {
  return visibility === 'public' || visibility === 'unlisted'
}

/**
 * Check if visibility allows discovery (listing)
 */
export function allowsDiscovery(visibility: Visibility): boolean {
  return visibility === 'public'
}

// =============================================================================
// CORS Configuration
// =============================================================================

/**
 * CORS configuration for database access
 *
 * Controls Cross-Origin Resource Sharing headers for public/unlisted databases.
 * Private databases always require authentication and ignore CORS settings.
 *
 * @example
 * ```typescript
 * defineConfig({
 *   visibility: 'public',
 *   cors: {
 *     origins: ['https://myapp.com', 'https://admin.myapp.com'],
 *     methods: ['GET', 'HEAD'],
 *     maxAge: 86400
 *   }
 * })
 * ```
 */
export interface CorsConfig {
  /**
   * Allowed origins
   * - `'*'` allows any origin (default for public databases)
   * - Array of specific origins for restricted access
   * - `false` disables CORS entirely
   */
  origins?: '*' | string[] | false

  /**
   * Allowed HTTP methods
   * @default ['GET', 'HEAD', 'OPTIONS']
   */
  methods?: string[]

  /**
   * Headers the client is allowed to send
   * @default ['Content-Type', 'Range']
   */
  allowedHeaders?: string[]

  /**
   * Headers exposed to the client
   * @default ['Content-Range', 'Content-Length', 'ETag', 'Accept-Ranges']
   */
  exposedHeaders?: string[]

  /**
   * How long preflight results can be cached (in seconds)
   * @default 86400 (24 hours)
   */
  maxAge?: number

  /**
   * Allow credentials (cookies, authorization headers)
   * Note: Cannot be true when origins is '*'
   * @default false
   */
  credentials?: boolean
}

/**
 * Default CORS configuration for public databases
 */
export const DEFAULT_CORS_CONFIG: CorsConfig = {
  origins: '*',
  methods: ['GET', 'HEAD', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Range'],
  exposedHeaders: ['Content-Range', 'Content-Length', 'ETag', 'Accept-Ranges'],
  maxAge: 86400,
  credentials: false,
}

/**
 * Build CORS headers from configuration
 */
export function buildCorsHeaders(
  config: CorsConfig,
  requestOrigin?: string | null
): Record<string, string> {
  const headers: Record<string, string> = {}

  if (config.origins === false) {
    // CORS disabled
    return headers
  }

  // Access-Control-Allow-Origin
  if (config.origins === '*') {
    headers['Access-Control-Allow-Origin'] = '*'
  } else if (config.origins && requestOrigin) {
    // Check if request origin is in allowed list
    if (config.origins.includes(requestOrigin)) {
      headers['Access-Control-Allow-Origin'] = requestOrigin
      headers['Vary'] = 'Origin'
    }
  }

  // Only add other headers if origin is allowed
  if (headers['Access-Control-Allow-Origin']) {
    if (config.methods?.length) {
      headers['Access-Control-Allow-Methods'] = config.methods.join(', ')
    }

    if (config.allowedHeaders?.length) {
      headers['Access-Control-Allow-Headers'] = config.allowedHeaders.join(', ')
    }

    if (config.exposedHeaders?.length) {
      headers['Access-Control-Expose-Headers'] = config.exposedHeaders.join(', ')
    }

    if (config.maxAge !== undefined) {
      headers['Access-Control-Max-Age'] = config.maxAge.toString()
    }

    if (config.credentials) {
      headers['Access-Control-Allow-Credentials'] = 'true'
    }
  }

  return headers
}
