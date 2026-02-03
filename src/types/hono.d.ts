/**
 * Hono type extensions for ParqueDB
 *
 * This module augments Hono's ContextVariableMap to provide proper typing
 * for context variables set by ParqueDB middleware, eliminating the need
 * for (c as any) casts throughout the codebase.
 *
 * Variables provided by middleware:
 * - `user`: AuthUser from auth middleware (set by auth())
 * - `actor`: EntityId for mutations (set by auth())
 * - `token`: Raw JWT token string (set by auth())
 * - `databaseContext`: Database context from databaseContextMiddleware
 * - `cookieDatabaseId`: Database ID from cookie
 * - `database`: Legacy database context (deprecated)
 */

import type { EntityId } from './entity'
import type { StorageBackend } from './storage'

/**
 * Authenticated user from oauth.do
 * Re-declared here to avoid circular imports with auth.ts
 */
export interface HonoAuthUser {
  id: string
  email?: string | undefined
  firstName?: string | undefined
  lastName?: string | undefined
  profilePictureUrl?: string | undefined
  organizationId?: string | undefined
  roles?: string[] | undefined
  permissions?: string[] | undefined
}

/**
 * Database info from context
 * This mirrors the DatabaseInfo type from worker/DatabaseIndexDO.ts
 * to ensure type compatibility with the actual implementation.
 */
export interface HonoDatabaseInfo {
  /** Unique database ID */
  id: string
  /** Human-readable name */
  name: string
  /** Description */
  description?: string | undefined
  /** R2 bucket name */
  bucket: string
  /** Path prefix within bucket */
  prefix?: string | undefined
  /** When database was created */
  createdAt: Date
  /** Who created the database */
  createdBy: EntityId
  /** Last access time */
  lastAccessedAt?: Date | undefined
  /** Estimated size in bytes */
  sizeBytes?: number | undefined
  /** Number of collections */
  collectionCount?: number | undefined
  /** Number of entities */
  entityCount?: number | undefined
  /** Database schema version */
  schemaVersion?: number | undefined
  /** Custom metadata */
  metadata?: Record<string, unknown> | undefined
  /** Visibility level for the database */
  visibility: string
  /** URL-friendly slug for public access */
  slug?: string | undefined
  /** Owner username */
  owner?: string | undefined
}

/**
 * Database context data from middleware
 */
export interface HonoDatabaseContextData {
  databaseId: string
  database: HonoDatabaseInfo
  storage: StorageBackend
  basePath: string
}

/**
 * Legacy database context (deprecated)
 */
export interface HonoDatabaseContext {
  database: HonoDatabaseInfo
  storage: StorageBackend
  userId: EntityId
  basePath: string
}

declare module 'hono' {
  interface ContextVariableMap {
    /**
     * Authenticated user from auth middleware
     * Set by auth() middleware from parquedb/hono
     */
    user: HonoAuthUser | null

    /**
     * Actor entity ID for mutations (e.g., "users/abc123")
     * Set by auth() middleware, passed to mutations for audit fields
     */
    actor: EntityId | null

    /**
     * Raw JWT token from Authorization header
     * Set by auth() middleware
     */
    token: string | null

    /**
     * Database context from databaseContextMiddleware
     * Contains database info, storage backend, and base path
     */
    databaseContext: HonoDatabaseContextData | null

    /**
     * Database ID from cookie (for fallback navigation)
     * Set by databaseContextMiddleware
     */
    cookieDatabaseId: string | null

    /**
     * Legacy database context (deprecated)
     * Use databaseContext instead
     */
    database: HonoDatabaseContext | null
  }
}
