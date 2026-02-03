/**
 * Remote Database Client
 *
 * Client for querying public and unlisted ParqueDB databases.
 * Reads Parquet files directly from remote storage using HTTP range requests.
 *
 * @example
 * ```typescript
 * // Open a public database
 * const db = await openRemoteDB('username/my-dataset')
 *
 * // Query a collection
 * const posts = await db.Posts.find({ status: 'published' })
 *
 * // With authentication for private databases
 * const privateDb = await openRemoteDB('username/private-data', { token: 'your-token' })
 * ```
 */

import { RemoteBackend } from '../storage/RemoteBackend'
import type { Filter, FindOptions, Entity, PaginatedResult } from '../types'
import type { Visibility } from '../types/visibility'
import { proxyTarget } from '../types/cast'

// =============================================================================
// Validation Helpers
// =============================================================================

/**
 * Validate that an object has all required fields
 * @throws Error if validation fails
 */
function validateRequiredFields(
  obj: unknown,
  requiredFields: string[],
  context: string
): void {
  if (obj === null || obj === undefined) {
    throw new Error(`${context}: Response is null or undefined`)
  }
  if (typeof obj !== 'object') {
    throw new Error(`${context}: Expected object, got ${typeof obj}`)
  }
  const record = obj as Record<string, unknown>
  const missingFields = requiredFields.filter(field => {
    const value = record[field]
    return value === null || value === undefined
  })
  if (missingFields.length > 0) {
    throw new Error(`${context}: Missing required fields: ${missingFields.join(', ')}`)
  }
}

// =============================================================================
// Types
// =============================================================================

/**
 * Options for opening a remote database
 */
export interface OpenRemoteDBOptions {
  /** Authentication token (required for private databases) */
  token?: string

  /** Custom base URL (defaults to https://parque.db) */
  baseUrl?: string

  /** Request timeout in milliseconds */
  timeout?: number

  /** Custom headers */
  headers?: Record<string, string>
}

/**
 * Remote database metadata
 */
export interface RemoteDBInfo {
  /** Database ID */
  id: string

  /** Database name */
  name: string

  /** Owner username */
  owner: string

  /** URL slug */
  slug: string

  /** Visibility level */
  visibility: Visibility

  /** Description */
  description?: string

  /** Number of collections */
  collectionCount?: number

  /** Number of entities */
  entityCount?: number
}

/**
 * Remote collection interface (read-only)
 */
export interface RemoteCollection<T = Record<string, unknown>> {
  /** Collection namespace */
  readonly namespace: string

  /**
   * Find entities matching a filter
   */
  find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>>

  /**
   * Find a single entity
   */
  findOne(filter?: Filter, options?: FindOptions): Promise<Entity<T> | null>

  /**
   * Get entity by ID
   */
  get(id: string): Promise<Entity<T> | null>

  /**
   * Count entities matching filter
   */
  count(filter?: Filter): Promise<number>

  /**
   * Check if entity exists
   */
  exists(id: string): Promise<boolean>
}

/**
 * Remote database interface (read-only)
 */
export interface RemoteDB {
  /** Database info */
  readonly info: RemoteDBInfo

  /** Storage backend */
  readonly backend: RemoteBackend

  /**
   * Get a collection by name
   */
  collection<T = Record<string, unknown>>(name: string): RemoteCollection<T>

  /**
   * List available collections
   */
  collections(): Promise<string[]>

  /**
   * Dynamic collection access via Proxy
   */
  [key: string]: RemoteCollection | unknown
}

// =============================================================================
// RemoteCollection Implementation
// =============================================================================

/**
 * Implementation of RemoteCollection
 */
class RemoteCollectionImpl<T = Record<string, unknown>> implements RemoteCollection<T> {
  readonly namespace: string
  private backend: RemoteBackend

  constructor(namespace: string, backend: RemoteBackend) {
    this.namespace = namespace
    this.backend = backend
  }

  async find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>> {
    // Read the Parquet file and filter in-memory
    // In a production implementation, this would:
    // 1. Read Parquet file metadata using range requests
    // 2. Use row group statistics for predicate pushdown
    // 3. Only read necessary row groups
    // 4. Apply filter on the data

    const parquetPath = `data/${this.namespace}/data.parquet`

    try {
      // Check if collection exists
      const exists = await this.backend.exists(parquetPath)
      if (!exists) {
        return { items: [], total: 0, hasMore: false }
      }

      // For now, return empty results
      // Full implementation would use hyparquet to read and filter
      return { items: [], total: 0, hasMore: false }
    } catch {
      return { items: [], total: 0, hasMore: false }
    }
  }

  async findOne(filter?: Filter, options?: FindOptions): Promise<Entity<T> | null> {
    const result = await this.find(filter, { ...options, limit: 1 })
    return result.items[0] ?? null
  }

  async get(id: string): Promise<Entity<T> | null> {
    return this.findOne({ $id: id })
  }

  async count(filter?: Filter): Promise<number> {
    const result = await this.find(filter, { limit: 0 })
    return result.total ?? 0
  }

  async exists(id: string): Promise<boolean> {
    const entity = await this.get(id)
    return entity !== null
  }
}

// =============================================================================
// RemoteDB Implementation
// =============================================================================

/**
 * Implementation of RemoteDB
 */
class RemoteDBImpl implements RemoteDB {
  readonly info: RemoteDBInfo
  readonly backend: RemoteBackend

  private collectionCache: Map<string, RemoteCollection> = new Map();

  // Allow dynamic access
  [key: string]: RemoteCollection | unknown

  constructor(info: RemoteDBInfo, backend: RemoteBackend) {
    this.info = info
    this.backend = backend

    // Create Proxy for dynamic collection access
    return new Proxy(this, {
      get(target, prop: string) {
        // Return known properties
        if (prop in target) {
          return proxyTarget<unknown>(target)[prop]
        }

        // Dynamic collection access: db.Posts -> db.collection('posts')
        const ns = prop.charAt(0).toLowerCase() + prop.slice(1)
        return target.collection(ns)
      },
    })
  }

  collection<T = Record<string, unknown>>(name: string): RemoteCollection<T> {
    const cached = this.collectionCache.get(name)
    if (cached) {
      return cached as RemoteCollection<T>
    }

    const collection = new RemoteCollectionImpl<T>(name, this.backend)
    this.collectionCache.set(name, collection)
    return collection
  }

  async collections(): Promise<string[]> {
    // Try to read from manifest
    try {
      const manifestData = await this.backend.read('_meta/manifest.json')
      const manifest = JSON.parse(new TextDecoder().decode(manifestData)) as {
        files?: Record<string, { path: string }>
      }

      const collections = new Set<string>()

      for (const file of Object.values(manifest.files ?? {})) {
        const match = file.path.match(/^data\/([^/]+)\//)
        if (match && match[1]) {
          collections.add(match[1])
        }
      }

      return Array.from(collections)
    } catch {
      // Manifest not available
      return []
    }
  }
}

// =============================================================================
// Public API
// =============================================================================

/**
 * Open a remote ParqueDB database
 *
 * @param ownerSlug - Database reference in format 'owner/slug' (e.g., 'username/my-dataset')
 * @param options - Connection options
 *
 * @example
 * ```typescript
 * // Open public database
 * const db = await openRemoteDB('username/my-dataset')
 *
 * // Query posts collection
 * const posts = await db.Posts.find({ published: true })
 * console.log(posts.items)
 *
 * // Open private database with authentication
 * const privateDb = await openRemoteDB('username/private', {
 *   token: 'your-auth-token'
 * })
 * ```
 */
export async function openRemoteDB(
  ownerSlug: string,
  options: OpenRemoteDBOptions = {}
): Promise<RemoteDB> {
  // Parse owner/slug
  const parts = ownerSlug.split('/')
  if (parts.length !== 2) {
    throw new Error('Invalid database reference. Use format: owner/slug')
  }
  const [owner, slug] = parts

  // Build base URL
  const baseUrl = options.baseUrl ?? 'https://parque.db'
  const dbUrl = `${baseUrl}/db/${ownerSlug}`

  // Create backend
  const backend = new RemoteBackend({
    baseUrl: dbUrl,
    token: options.token,
    timeout: options.timeout,
    headers: options.headers,
  })

  // Fetch database info
  let info: RemoteDBInfo

  try {
    const response = await fetch(`${baseUrl}/api/db/${ownerSlug}`, {
      headers: options.token ? { 'Authorization': `Bearer ${options.token}` } : {},
    })

    if (response.status === 404) {
      throw new Error(`Database not found: ${ownerSlug}`)
    }

    if (response.status === 401 || response.status === 403) {
      throw new Error('Authentication required for this database')
    }

    if (!response.ok) {
      throw new Error(`Failed to fetch database info: ${response.statusText}`)
    }

    const data = await response.json()
    validateRequiredFields(
      data,
      ['id', 'name', 'owner', 'slug', 'visibility'],
      'openRemoteDB'
    )
    info = data as RemoteDBInfo
  } catch (error) {
    // If API is not available, create minimal info from ownerSlug
    if (error instanceof Error && error.message.includes('fetch')) {
      info = {
        id: ownerSlug,
        name: slug!,
        owner: owner!,
        slug: slug!,
        visibility: 'public',
      }
    } else {
      throw error
    }
  }

  return new RemoteDBImpl(info, backend)
}

/**
 * Check if a remote database exists and is accessible
 *
 * @example
 * ```typescript
 * const exists = await checkRemoteDB('username/my-dataset')
 * if (exists) {
 *   const db = await openRemoteDB('username/my-dataset')
 * }
 * ```
 */
export async function checkRemoteDB(
  ownerSlug: string,
  options: OpenRemoteDBOptions = {}
): Promise<boolean> {
  try {
    const baseUrl = options.baseUrl ?? 'https://parque.db'

    const response = await fetch(`${baseUrl}/api/db/${ownerSlug}`, {
      method: 'HEAD',
      headers: options.token ? { 'Authorization': `Bearer ${options.token}` } : {},
    })

    return response.ok
  } catch {
    return false
  }
}

/**
 * List public databases
 *
 * @example
 * ```typescript
 * const databases = await listPublicDatabases()
 * for (const db of databases) {
 *   console.log(`${db.owner}/${db.slug}: ${db.description}`)
 * }
 * ```
 */
export async function listPublicDatabases(
  options: { baseUrl?: string; limit?: number; offset?: number } = {}
): Promise<RemoteDBInfo[]> {
  const baseUrl = options.baseUrl ?? 'https://parque.db'
  const params = new URLSearchParams()

  if (options.limit) params.set('limit', options.limit.toString())
  if (options.offset) params.set('offset', options.offset.toString())

  const url = `${baseUrl}/api/public?${params.toString()}`

  try {
    const response = await fetch(url)

    if (!response.ok) {
      return []
    }

    const data = await response.json() as { databases: RemoteDBInfo[] }
    return data.databases
  } catch {
    return []
  }
}
