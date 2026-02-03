/**
 * MCP Server Authentication
 *
 * Provides authentication support for the ParqueDB MCP server.
 * Supports multiple authentication strategies:
 * - API Key authentication (simple, suitable for local development)
 * - Bearer token authentication (OAuth 2.1 compatible)
 * - Custom token verification (for integration with existing auth systems)
 *
 * @example
 * ```typescript
 * import { createParqueDBMCPServer, createApiKeyAuthenticator } from 'parquedb/mcp'
 *
 * // Simple API key authentication
 * const authenticator = createApiKeyAuthenticator({
 *   apiKeys: new Map([
 *     ['my-secret-key', { clientId: 'client-1', scopes: ['read', 'write'] }]
 *   ])
 * })
 *
 * const server = createParqueDBMCPServer(db, {
 *   auth: {
 *     authenticator,
 *     required: true, // Reject unauthenticated requests
 *   }
 * })
 * ```
 */

import type { AuthInfo as MCPAuthInfo } from '@modelcontextprotocol/sdk/server/auth/types.js'

/**
 * Authentication info for ParqueDB MCP server.
 * Extends the MCP SDK's AuthInfo with ParqueDB-specific fields.
 */
export interface AuthInfo extends MCPAuthInfo {
  /**
   * The access token (may be redacted for security)
   */
  token: string

  /**
   * The client ID associated with this token
   */
  clientId: string

  /**
   * Scopes/permissions granted to this token
   */
  scopes: string[]

  /**
   * When the token expires (Unix timestamp in seconds)
   */
  expiresAt?: number | undefined

  /**
   * The resource server URL this token is valid for (RFC 8707)
   */
  resource?: URL | undefined

  /**
   * Additional custom data attached to the auth info
   */
  extra?: Record<string, unknown> | undefined
}

/**
 * Result of an authentication attempt
 */
export type AuthResult =
  | { success: true; authInfo: AuthInfo }
  | { success: false; error: AuthError }

/**
 * Authentication error details
 */
export interface AuthError {
  /**
   * Error code for programmatic handling
   */
  code: AuthErrorCode

  /**
   * Human-readable error message
   */
  message: string

  /**
   * HTTP status code to return (default: 401)
   */
  statusCode?: number | undefined
}

/**
 * Standard authentication error codes
 */
export type AuthErrorCode =
  | 'missing_token'
  | 'invalid_token'
  | 'expired_token'
  | 'insufficient_scope'
  | 'invalid_client'
  | 'server_error'

/**
 * Interface for token authenticators.
 * Implement this interface to create custom authentication strategies.
 */
export interface TokenAuthenticator {
  /**
   * Authenticate a token and return auth info or an error.
   *
   * @param token - The token to authenticate (without "Bearer " prefix)
   * @param context - Optional context about the request
   * @returns Auth result with info or error
   */
  authenticate(token: string, context?: AuthContext): Promise<AuthResult>

  /**
   * Optional: Refresh/extend a token's validity
   */
  refresh?(authInfo: AuthInfo): Promise<AuthResult>

  /**
   * Optional: Revoke a token
   */
  revoke?(token: string): Promise<void>
}

/**
 * Context information for authentication
 */
export interface AuthContext {
  /**
   * The MCP operation being performed
   */
  operation?: string | undefined

  /**
   * Required scopes for this operation
   */
  requiredScopes?: string[] | undefined

  /**
   * IP address of the client (if available)
   */
  clientIp?: string | undefined

  /**
   * User agent of the client (if available)
   */
  userAgent?: string | undefined
}

/**
 * Configuration for API key authentication
 */
export interface ApiKeyAuthConfig {
  /**
   * Map of API keys to their associated auth info.
   * Key is the API key string, value contains client info.
   */
  apiKeys: Map<string, ApiKeyEntry>

  /**
   * Hash algorithm for comparing keys (default: none - direct comparison)
   * Use 'sha256' for production to avoid timing attacks
   */
  hashAlgorithm?: 'none' | 'sha256' | undefined

  /**
   * Default scopes to grant if not specified in the key entry
   */
  defaultScopes?: string[] | undefined

  /**
   * Optional key expiration check
   */
  checkExpiration?: boolean | undefined
}

/**
 * Entry for a single API key
 */
export interface ApiKeyEntry {
  /**
   * Client ID associated with this key
   */
  clientId: string

  /**
   * Scopes/permissions granted to this key
   */
  scopes?: string[] | undefined

  /**
   * When this key expires (Unix timestamp in seconds)
   */
  expiresAt?: number | undefined

  /**
   * Human-readable name/description for this key
   */
  name?: string | undefined

  /**
   * Whether this key is currently active
   */
  active?: boolean | undefined

  /**
   * Additional metadata
   */
  metadata?: Record<string, unknown> | undefined
}

/**
 * Creates an API key authenticator.
 *
 * This is a simple authenticator suitable for development and
 * controlled environments. For production with external clients,
 * consider using OAuth-based authentication.
 *
 * @param config - API key configuration
 * @returns Token authenticator instance
 *
 * @example
 * ```typescript
 * const authenticator = createApiKeyAuthenticator({
 *   apiKeys: new Map([
 *     ['dev-key-123', { clientId: 'dev-client', scopes: ['read'] }],
 *     ['admin-key-456', { clientId: 'admin', scopes: ['read', 'write', 'delete'] }],
 *   ]),
 *   checkExpiration: true,
 * })
 * ```
 */
export function createApiKeyAuthenticator(config: ApiKeyAuthConfig): TokenAuthenticator {
  const { apiKeys, hashAlgorithm = 'none', defaultScopes = [], checkExpiration = false } = config

  return {
    async authenticate(token: string, context?: AuthContext): Promise<AuthResult> {
      if (!token || token.trim().length === 0) {
        return {
          success: false,
          error: {
            code: 'missing_token',
            message: 'Authentication token is required',
            statusCode: 401,
          },
        }
      }

      // Normalize token
      const normalizedToken = token.trim()

      // Look up the key
      let entry: ApiKeyEntry | undefined

      if (hashAlgorithm === 'sha256') {
        // Use constant-time comparison with hashed keys
        const hashedToken = await hashToken(normalizedToken)
        for (const [key, value] of apiKeys) {
          const hashedKey = await hashToken(key)
          if (constantTimeCompare(hashedToken, hashedKey)) {
            entry = value
            break
          }
        }
      } else {
        entry = apiKeys.get(normalizedToken)
      }

      if (!entry) {
        return {
          success: false,
          error: {
            code: 'invalid_token',
            message: 'Invalid API key',
            statusCode: 401,
          },
        }
      }

      // Check if key is active
      if (entry.active === false) {
        return {
          success: false,
          error: {
            code: 'invalid_token',
            message: 'API key has been deactivated',
            statusCode: 401,
          },
        }
      }

      // Check expiration
      if (checkExpiration && entry.expiresAt) {
        const now = Math.floor(Date.now() / 1000)
        if (now >= entry.expiresAt) {
          return {
            success: false,
            error: {
              code: 'expired_token',
              message: 'API key has expired',
              statusCode: 401,
            },
          }
        }
      }

      // Check required scopes
      const grantedScopes = entry.scopes ?? defaultScopes
      if (context?.requiredScopes && context.requiredScopes.length > 0) {
        const missingScopes = context.requiredScopes.filter(
          (scope) => !grantedScopes.includes(scope)
        )
        if (missingScopes.length > 0) {
          return {
            success: false,
            error: {
              code: 'insufficient_scope',
              message: `Missing required scopes: ${missingScopes.join(', ')}`,
              statusCode: 403,
            },
          }
        }
      }

      // Success - return auth info
      return {
        success: true,
        authInfo: {
          token: redactToken(normalizedToken),
          clientId: entry.clientId,
          scopes: grantedScopes,
          expiresAt: entry.expiresAt,
          extra: {
            name: entry.name,
            ...entry.metadata,
          },
        },
      }
    },

    async revoke(token: string): Promise<void> {
      // For API key auth, we can mark the key as inactive
      const entry = apiKeys.get(token)
      if (entry) {
        entry.active = false
      }
    },
  }
}

/**
 * Configuration for custom token verification
 */
export interface CustomAuthConfig {
  /**
   * Function to verify a token and return auth info
   */
  verify: (token: string, context?: AuthContext) => Promise<AuthInfo | null>

  /**
   * Optional function to refresh a token
   */
  refresh?: ((authInfo: AuthInfo) => Promise<AuthInfo | null>) | undefined

  /**
   * Optional function to revoke a token
   */
  revoke?: ((token: string) => Promise<void>) | undefined
}

/**
 * Creates a custom token authenticator.
 *
 * Use this when you need to integrate with an existing
 * authentication system (e.g., JWT validation, external auth service).
 *
 * @param config - Custom auth configuration
 * @returns Token authenticator instance
 *
 * @example
 * ```typescript
 * import jwt from 'jsonwebtoken'
 *
 * const authenticator = createCustomAuthenticator({
 *   verify: async (token) => {
 *     try {
 *       const decoded = jwt.verify(token, process.env.JWT_SECRET!)
 *       return {
 *         token: redactToken(token),
 *         clientId: decoded.sub,
 *         scopes: decoded.scopes ?? [],
 *         expiresAt: decoded.exp,
 *       }
 *     } catch {
 *       return null
 *     }
 *   },
 * })
 * ```
 */
export function createCustomAuthenticator(config: CustomAuthConfig): TokenAuthenticator {
  return {
    async authenticate(token: string, context?: AuthContext): Promise<AuthResult> {
      if (!token || token.trim().length === 0) {
        return {
          success: false,
          error: {
            code: 'missing_token',
            message: 'Authentication token is required',
            statusCode: 401,
          },
        }
      }

      try {
        const authInfo = await config.verify(token, context)

        if (!authInfo) {
          return {
            success: false,
            error: {
              code: 'invalid_token',
              message: 'Token verification failed',
              statusCode: 401,
            },
          }
        }

        // Check required scopes if specified in context
        if (context?.requiredScopes && context.requiredScopes.length > 0) {
          const missingScopes = context.requiredScopes.filter(
            (scope) => !authInfo.scopes.includes(scope)
          )
          if (missingScopes.length > 0) {
            return {
              success: false,
              error: {
                code: 'insufficient_scope',
                message: `Missing required scopes: ${missingScopes.join(', ')}`,
                statusCode: 403,
              },
            }
          }
        }

        // Check expiration
        if (authInfo.expiresAt) {
          const now = Math.floor(Date.now() / 1000)
          if (now >= authInfo.expiresAt) {
            return {
              success: false,
              error: {
                code: 'expired_token',
                message: 'Token has expired',
                statusCode: 401,
              },
            }
          }
        }

        return { success: true, authInfo }
      } catch (error) {
        return {
          success: false,
          error: {
            code: 'server_error',
            message: error instanceof Error ? error.message : 'Authentication error',
            statusCode: 500,
          },
        }
      }
    },

    async refresh(authInfo: AuthInfo): Promise<AuthResult> {
      if (!config.refresh) {
        return {
          success: false,
          error: {
            code: 'server_error',
            message: 'Token refresh not supported',
            statusCode: 501,
          },
        }
      }

      try {
        const newAuthInfo = await config.refresh(authInfo)
        if (!newAuthInfo) {
          return {
            success: false,
            error: {
              code: 'invalid_token',
              message: 'Token refresh failed',
              statusCode: 401,
            },
          }
        }
        return { success: true, authInfo: newAuthInfo }
      } catch (error) {
        return {
          success: false,
          error: {
            code: 'server_error',
            message: error instanceof Error ? error.message : 'Token refresh error',
            statusCode: 500,
          },
        }
      }
    },

    async revoke(token: string): Promise<void> {
      if (config.revoke) {
        await config.revoke(token)
      }
    },
  }
}

/**
 * Scope definitions for ParqueDB operations
 */
export const PARQUEDB_SCOPES = {
  /** Read entities */
  READ: 'parquedb:read',
  /** Create entities */
  WRITE: 'parquedb:write',
  /** Update entities */
  UPDATE: 'parquedb:update',
  /** Delete entities */
  DELETE: 'parquedb:delete',
  /** List collections */
  LIST: 'parquedb:list',
  /** Semantic search */
  SEARCH: 'parquedb:search',
  /** Aggregation operations */
  AGGREGATE: 'parquedb:aggregate',
  /** Full access */
  ADMIN: 'parquedb:admin',
} as const

/**
 * Map of MCP tools to their required scopes
 */
export const TOOL_SCOPE_REQUIREMENTS: Record<string, string[]> = {
  parquedb_find: [PARQUEDB_SCOPES.READ],
  parquedb_get: [PARQUEDB_SCOPES.READ],
  parquedb_create: [PARQUEDB_SCOPES.WRITE],
  parquedb_update: [PARQUEDB_SCOPES.UPDATE],
  parquedb_delete: [PARQUEDB_SCOPES.DELETE],
  parquedb_count: [PARQUEDB_SCOPES.READ],
  parquedb_aggregate: [PARQUEDB_SCOPES.AGGREGATE],
  parquedb_list_collections: [PARQUEDB_SCOPES.LIST],
  parquedb_semantic_search: [PARQUEDB_SCOPES.SEARCH],
}

/**
 * Check if the given scopes satisfy the required scopes.
 * The 'parquedb:admin' scope grants access to all operations.
 */
export function hasRequiredScopes(grantedScopes: string[], requiredScopes: string[]): boolean {
  // Admin scope grants all access
  if (grantedScopes.includes(PARQUEDB_SCOPES.ADMIN)) {
    return true
  }

  // Check if all required scopes are present
  return requiredScopes.every((scope) => grantedScopes.includes(scope))
}

/**
 * Get required scopes for a tool
 */
export function getToolScopes(toolName: string): string[] {
  return TOOL_SCOPE_REQUIREMENTS[toolName] ?? [PARQUEDB_SCOPES.READ]
}

// Utility functions

/**
 * Hash a token using SHA-256
 */
async function hashToken(token: string): Promise<string> {
  // Use Web Crypto API (available in Node.js and browsers)
  const encoder = new TextEncoder()
  const data = encoder.encode(token)

  // Check if crypto.subtle is available (Node.js 18+, browsers)
  if (typeof globalThis.crypto?.subtle?.digest === 'function') {
    const hashBuffer = await globalThis.crypto.subtle.digest('SHA-256', data)
    const hashArray = Array.from(new Uint8Array(hashBuffer))
    return hashArray.map((b) => b.toString(16).padStart(2, '0')).join('')
  }

  // Fallback for Node.js without Web Crypto
  const crypto = await import('crypto')
  return crypto.createHash('sha256').update(token).digest('hex')
}

/**
 * Constant-time string comparison to prevent timing attacks
 */
function constantTimeCompare(a: string, b: string): boolean {
  if (a.length !== b.length) {
    return false
  }

  let result = 0
  for (let i = 0; i < a.length; i++) {
    result |= a.charCodeAt(i) ^ b.charCodeAt(i)
  }

  return result === 0
}

/**
 * Redact a token for logging purposes (show first 4 and last 4 chars)
 */
export function redactToken(token: string): string {
  if (token.length <= 12) {
    return '***'
  }
  return `${token.slice(0, 4)}...${token.slice(-4)}`
}

/**
 * Parse a Bearer token from an Authorization header
 */
export function parseBearerToken(authHeader: string | null | undefined): string | null {
  if (!authHeader) {
    return null
  }

  const parts = authHeader.split(' ')
  if (parts.length !== 2 || parts[0]?.toLowerCase() !== 'bearer') {
    return null
  }

  return parts[1] ?? null
}

/**
 * Authentication error class
 */
export class AuthenticationError extends Error {
  public readonly code: AuthErrorCode
  public readonly statusCode: number

  constructor(error: AuthError) {
    super(error.message)
    this.name = 'AuthenticationError'
    this.code = error.code
    this.statusCode = error.statusCode ?? 401
  }
}
