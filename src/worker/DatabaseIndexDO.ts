/**
 * DatabaseIndex Durable Object
 *
 * Tracks all databases owned by a user. Each user gets their own DatabaseIndex DO
 * that maintains an index of their databases with metadata.
 *
 * Features:
 * - List all databases for a user
 * - Track metadata (name, created, last accessed, size)
 * - Integrate with oauth.do for user identity
 * - Support database creation/deletion
 *
 * @example
 * ```typescript
 * // Get user's database index
 * const indexId = env.DATABASE_INDEX.idFromName(userId)
 * const index = env.DATABASE_INDEX.get(indexId)
 *
 * // List databases
 * const databases = await index.list()
 *
 * // Register a new database
 * await index.register({ name: 'my-app', bucket: 'r2-bucket' })
 * ```
 */

import { DurableObject } from 'cloudflare:workers'
import type { EntityId } from '../types'
import type { Visibility } from '../types/visibility'
import { DEFAULT_VISIBILITY, isValidVisibility } from '../types/visibility'

// =============================================================================
// Types
// =============================================================================

/**
 * Database metadata stored in the index
 */
export interface DatabaseInfo {
  /** Unique database ID */
  id: string
  /** Human-readable name */
  name: string
  /** Description */
  description?: string
  /** R2 bucket name */
  bucket: string
  /** Path prefix within bucket */
  prefix?: string
  /** When database was created */
  createdAt: Date
  /** Who created the database */
  createdBy: EntityId
  /** Last access time */
  lastAccessedAt?: Date
  /** Estimated size in bytes */
  sizeBytes?: number
  /** Number of collections */
  collectionCount?: number
  /** Number of entities */
  entityCount?: number
  /** Database schema version */
  schemaVersion?: number
  /** Custom metadata */
  metadata?: Record<string, unknown>
  /**
   * Visibility level for the database
   * - 'public': Discoverable and accessible by anyone
   * - 'unlisted': Accessible with direct link, not discoverable
   * - 'private': Requires authentication (default)
   */
  visibility: Visibility
  /** URL-friendly slug for public access (e.g., 'my-dataset') */
  slug?: string
  /** Owner username (for public URL: username/slug) */
  owner?: string
}

/**
 * Options for registering a database
 */
export interface RegisterDatabaseOptions {
  /** Human-readable name */
  name: string
  /** Description */
  description?: string
  /** R2 bucket name */
  bucket: string
  /** Path prefix within bucket */
  prefix?: string
  /** Custom metadata */
  metadata?: Record<string, unknown>
  /** Visibility level (default: 'private') */
  visibility?: Visibility
  /** URL-friendly slug for public access */
  slug?: string
  /** Owner username */
  owner?: string
}

/**
 * Options for updating database info
 */
export interface UpdateDatabaseOptions {
  /** New name */
  name?: string
  /** New description */
  description?: string
  /** Update stats */
  stats?: {
    sizeBytes?: number
    collectionCount?: number
    entityCount?: number
  }
  /** Custom metadata to merge */
  metadata?: Record<string, unknown>
  /** New visibility level */
  visibility?: Visibility
  /** New URL-friendly slug */
  slug?: string
}

/**
 * Bindings expected by DatabaseIndexDO
 */
export interface DatabaseIndexEnv {
  /** Optional: R2 bucket for storing index backups */
  INDEX_BACKUP?: R2Bucket
}

// =============================================================================
// DatabaseIndex Durable Object
// =============================================================================

/**
 * Durable Object that maintains an index of databases for a user
 *
 * The DO ID should be derived from the user ID:
 * ```typescript
 * const indexId = env.DATABASE_INDEX.idFromName(`user:${userId}`)
 * ```
 */
export class DatabaseIndexDO extends DurableObject<DatabaseIndexEnv> {
  private sql: SqlStorage

  constructor(ctx: DurableObjectState, env: DatabaseIndexEnv) {
    super(ctx, env)
    this.sql = ctx.storage.sql
    this.initSchema()
  }

  /**
   * Initialize SQLite schema
   */
  private initSchema(): void {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS databases (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        bucket TEXT NOT NULL,
        prefix TEXT,
        created_at INTEGER NOT NULL,
        created_by TEXT NOT NULL,
        last_accessed_at INTEGER,
        size_bytes INTEGER DEFAULT 0,
        collection_count INTEGER DEFAULT 0,
        entity_count INTEGER DEFAULT 0,
        schema_version INTEGER DEFAULT 1,
        metadata TEXT,
        deleted_at INTEGER,
        visibility TEXT DEFAULT 'private',
        slug TEXT,
        owner TEXT
      );

      CREATE INDEX IF NOT EXISTS idx_databases_name ON databases(name);
      CREATE INDEX IF NOT EXISTS idx_databases_created ON databases(created_at);
      CREATE INDEX IF NOT EXISTS idx_databases_accessed ON databases(last_accessed_at);
      CREATE INDEX IF NOT EXISTS idx_databases_visibility ON databases(visibility);
      CREATE UNIQUE INDEX IF NOT EXISTS idx_databases_owner_slug ON databases(owner, slug) WHERE slug IS NOT NULL AND deleted_at IS NULL;
    `)

    // Migration: add columns if they don't exist (for existing databases)
    this.migrateSchema()
  }

  /**
   * Migrate schema for existing databases
   */
  private migrateSchema(): void {
    // Check if visibility column exists by attempting a query
    try {
      this.sql.exec(`SELECT visibility FROM databases LIMIT 1`)
    } catch {
      // Column doesn't exist, add it
      this.sql.exec(`ALTER TABLE databases ADD COLUMN visibility TEXT DEFAULT 'private'`)
      this.sql.exec(`ALTER TABLE databases ADD COLUMN slug TEXT`)
      this.sql.exec(`ALTER TABLE databases ADD COLUMN owner TEXT`)
      this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_databases_visibility ON databases(visibility)`)
      this.sql.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_databases_owner_slug ON databases(owner, slug) WHERE slug IS NOT NULL AND deleted_at IS NULL`)
    }
  }

  /**
   * List all databases for this user
   */
  async list(): Promise<DatabaseInfo[]> {
    const rows = this.sql.exec(`
      SELECT * FROM databases
      WHERE deleted_at IS NULL
      ORDER BY last_accessed_at DESC, created_at DESC
    `).toArray()

    return rows.map((row) => this.rowToDatabase(row))
  }

  /**
   * Get a specific database by ID
   */
  async get(id: string): Promise<DatabaseInfo | null> {
    const rows = this.sql.exec(`
      SELECT * FROM databases
      WHERE id = ? AND deleted_at IS NULL
    `, id).toArray()

    if (rows.length === 0) return null
    return this.rowToDatabase(rows[0]!)
  }

  /**
   * Get a database by name
   */
  async getByName(name: string): Promise<DatabaseInfo | null> {
    const rows = this.sql.exec(`
      SELECT * FROM databases
      WHERE name = ? AND deleted_at IS NULL
    `, name).toArray()

    if (rows.length === 0) return null
    return this.rowToDatabase(rows[0]!)
  }

  /**
   * Get a database by owner and slug (for public URL access)
   *
   * @example
   * ```typescript
   * // Find database at parque.db/username/my-dataset
   * const db = await index.getBySlug('username', 'my-dataset')
   * ```
   */
  async getBySlug(owner: string, slug: string): Promise<DatabaseInfo | null> {
    const rows = this.sql.exec(`
      SELECT * FROM databases
      WHERE owner = ? AND slug = ? AND deleted_at IS NULL
    `, owner, slug).toArray()

    if (rows.length === 0) return null
    return this.rowToDatabase(rows[0]!)
  }

  /**
   * List all public (discoverable) databases
   * Used for public database directory
   */
  async listPublic(): Promise<DatabaseInfo[]> {
    const rows = this.sql.exec(`
      SELECT * FROM databases
      WHERE visibility = 'public' AND deleted_at IS NULL
      ORDER BY last_accessed_at DESC, created_at DESC
    `).toArray()

    return rows.map((row) => this.rowToDatabase(row))
  }

  /**
   * List databases by visibility level
   */
  async listByVisibility(visibility: Visibility): Promise<DatabaseInfo[]> {
    const rows = this.sql.exec(`
      SELECT * FROM databases
      WHERE visibility = ? AND deleted_at IS NULL
      ORDER BY last_accessed_at DESC, created_at DESC
    `, visibility).toArray()

    return rows.map((row) => this.rowToDatabase(row))
  }

  /**
   * Set visibility and optionally slug for a database
   *
   * @example
   * ```typescript
   * // Make database public with a custom slug
   * await index.setVisibility(dbId, 'public', 'my-dataset')
   * ```
   */
  async setVisibility(
    id: string,
    visibility: Visibility,
    slug?: string
  ): Promise<DatabaseInfo | null> {
    return this.update(id, { visibility, slug })
  }

  /**
   * Register a new database
   */
  async register(
    options: RegisterDatabaseOptions,
    actor: EntityId
  ): Promise<DatabaseInfo> {
    const id = this.generateId()
    const now = Date.now()
    const visibility = options.visibility ?? DEFAULT_VISIBILITY

    // Validate slug if provided
    if (options.slug && !this.isValidSlug(options.slug)) {
      throw new Error('Invalid slug: must be lowercase alphanumeric with hyphens')
    }

    // Check for duplicate slug within owner
    if (options.slug && options.owner) {
      const existing = await this.getBySlug(options.owner, options.slug)
      if (existing) {
        throw new Error(`Slug '${options.slug}' already exists for owner '${options.owner}'`)
      }
    }

    this.sql.exec(`
      INSERT INTO databases (id, name, description, bucket, prefix, created_at, created_by, metadata, visibility, slug, owner)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
      id,
      options.name,
      options.description ?? null,
      options.bucket,
      options.prefix ?? null,
      now,
      actor,
      options.metadata ? JSON.stringify(options.metadata) : null,
      visibility,
      options.slug ?? null,
      options.owner ?? null
    )

    return {
      id,
      name: options.name,
      description: options.description,
      bucket: options.bucket,
      prefix: options.prefix,
      createdAt: new Date(now),
      createdBy: actor,
      metadata: options.metadata,
      visibility,
      slug: options.slug,
      owner: options.owner,
    }
  }

  /**
   * Update database info
   */
  async update(id: string, options: UpdateDatabaseOptions): Promise<DatabaseInfo | null> {
    const db = await this.get(id)
    if (!db) return null

    const updates: string[] = []
    const values: unknown[] = []

    if (options.name !== undefined) {
      updates.push('name = ?')
      values.push(options.name)
    }

    if (options.description !== undefined) {
      updates.push('description = ?')
      values.push(options.description)
    }

    if (options.stats?.sizeBytes !== undefined) {
      updates.push('size_bytes = ?')
      values.push(options.stats.sizeBytes)
    }

    if (options.stats?.collectionCount !== undefined) {
      updates.push('collection_count = ?')
      values.push(options.stats.collectionCount)
    }

    if (options.stats?.entityCount !== undefined) {
      updates.push('entity_count = ?')
      values.push(options.stats.entityCount)
    }

    if (options.metadata !== undefined) {
      const merged = { ...db.metadata, ...options.metadata }
      updates.push('metadata = ?')
      values.push(JSON.stringify(merged))
    }

    if (options.visibility !== undefined) {
      if (!isValidVisibility(options.visibility)) {
        throw new Error(`Invalid visibility: ${options.visibility}`)
      }
      updates.push('visibility = ?')
      values.push(options.visibility)
    }

    if (options.slug !== undefined) {
      if (options.slug && !this.isValidSlug(options.slug)) {
        throw new Error('Invalid slug: must be lowercase alphanumeric with hyphens')
      }
      // Check for duplicate slug within owner
      if (options.slug && db.owner) {
        const existing = await this.getBySlug(db.owner, options.slug)
        if (existing && existing.id !== id) {
          throw new Error(`Slug '${options.slug}' already exists for owner '${db.owner}'`)
        }
      }
      updates.push('slug = ?')
      values.push(options.slug || null)
    }

    if (updates.length === 0) {
      return db
    }

    values.push(id)
    this.sql.exec(`
      UPDATE databases
      SET ${updates.join(', ')}
      WHERE id = ?
    `, ...values)

    return this.get(id)
  }

  /**
   * Record database access (updates last_accessed_at)
   */
  async recordAccess(id: string): Promise<void> {
    this.sql.exec(`
      UPDATE databases
      SET last_accessed_at = ?
      WHERE id = ? AND deleted_at IS NULL
    `, Date.now(), id)
  }

  /**
   * Soft delete a database from the index
   * Note: This does NOT delete the actual data in R2
   */
  async unregister(id: string): Promise<boolean> {
    const result = this.sql.exec(`
      UPDATE databases
      SET deleted_at = ?
      WHERE id = ? AND deleted_at IS NULL
    `, Date.now(), id)

    return result.rowsWritten > 0
  }

  /**
   * Permanently delete a database from the index
   */
  async purge(id: string): Promise<boolean> {
    const result = this.sql.exec(`
      DELETE FROM databases WHERE id = ?
    `, id)

    return result.rowsWritten > 0
  }

  /**
   * Get statistics for this user's databases
   */
  async stats(): Promise<{
    totalDatabases: number
    totalSizeBytes: number
    totalEntities: number
    oldestDatabase?: Date
    newestDatabase?: Date
    mostRecentAccess?: Date
  }> {
    const rows = this.sql.exec(`
      SELECT
        COUNT(*) as total_databases,
        COALESCE(SUM(size_bytes), 0) as total_size_bytes,
        COALESCE(SUM(entity_count), 0) as total_entities,
        MIN(created_at) as oldest,
        MAX(created_at) as newest,
        MAX(last_accessed_at) as most_recent
      FROM databases
      WHERE deleted_at IS NULL
    `).toArray()

    const row = rows[0] as Record<string, unknown>

    return {
      totalDatabases: (row.total_databases as number) ?? 0,
      totalSizeBytes: (row.total_size_bytes as number) ?? 0,
      totalEntities: (row.total_entities as number) ?? 0,
      oldestDatabase: row.oldest ? new Date(row.oldest as number) : undefined,
      newestDatabase: row.newest ? new Date(row.newest as number) : undefined,
      mostRecentAccess: row.most_recent ? new Date(row.most_recent as number) : undefined,
    }
  }

  /**
   * Search databases by name
   */
  async search(query: string): Promise<DatabaseInfo[]> {
    const rows = this.sql.exec(`
      SELECT * FROM databases
      WHERE name LIKE ? AND deleted_at IS NULL
      ORDER BY name
    `, `%${query}%`).toArray()

    return rows.map((row) => this.rowToDatabase(row))
  }

  // =============================================================================
  // HTTP Handler (for Worker integration)
  // =============================================================================

  /**
   * Handle HTTP requests
   */
  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    try {
      // GET /databases - list all databases
      if (request.method === 'GET' && path === '/databases') {
        const databases = await this.list()
        return Response.json({ databases })
      }

      // GET /databases/:id - get specific database
      if (request.method === 'GET' && path.startsWith('/databases/')) {
        const id = path.slice('/databases/'.length)
        const database = await this.get(id)
        if (!database) {
          return Response.json({ error: 'Database not found' }, { status: 404 })
        }
        return Response.json(database)
      }

      // POST /databases - register new database
      if (request.method === 'POST' && path === '/databases') {
        const body = await request.json() as RegisterDatabaseOptions & { actor: EntityId }
        const { actor, ...options } = body
        if (!actor) {
          return Response.json({ error: 'Actor required' }, { status: 400 })
        }
        const database = await this.register(options, actor)
        return Response.json(database, { status: 201 })
      }

      // PATCH /databases/:id - update database
      if (request.method === 'PATCH' && path.startsWith('/databases/')) {
        const id = path.slice('/databases/'.length)
        const options = await request.json() as UpdateDatabaseOptions
        const database = await this.update(id, options)
        if (!database) {
          return Response.json({ error: 'Database not found' }, { status: 404 })
        }
        return Response.json(database)
      }

      // DELETE /databases/:id - unregister database
      if (request.method === 'DELETE' && path.startsWith('/databases/')) {
        const id = path.slice('/databases/'.length)
        const deleted = await this.unregister(id)
        if (!deleted) {
          return Response.json({ error: 'Database not found' }, { status: 404 })
        }
        return Response.json({ deleted: true })
      }

      // POST /databases/:id/access - record access
      if (request.method === 'POST' && path.endsWith('/access')) {
        const id = path.slice('/databases/'.length, -'/access'.length)
        await this.recordAccess(id)
        return Response.json({ success: true })
      }

      // GET /stats - get user stats
      if (request.method === 'GET' && path === '/stats') {
        const stats = await this.stats()
        return Response.json(stats)
      }

      // GET /search?q=... - search databases
      if (request.method === 'GET' && path === '/search') {
        const query = url.searchParams.get('q') ?? ''
        const databases = await this.search(query)
        return Response.json({ databases })
      }

      // GET /public - list public databases
      if (request.method === 'GET' && path === '/public') {
        const databases = await this.listPublic()
        return Response.json({ databases })
      }

      // GET /databases/by-slug/:owner/:slug - get by owner/slug
      if (request.method === 'GET' && path.startsWith('/databases/by-slug/')) {
        const parts = path.slice('/databases/by-slug/'.length).split('/')
        if (parts.length !== 2) {
          return Response.json({ error: 'Invalid path: expected /databases/by-slug/:owner/:slug' }, { status: 400 })
        }
        const [owner, slug] = parts
        const database = await this.getBySlug(owner!, slug!)
        if (!database) {
          return Response.json({ error: 'Database not found' }, { status: 404 })
        }
        return Response.json(database)
      }

      // PUT /databases/:id/visibility - set visibility
      if (request.method === 'PUT' && path.endsWith('/visibility')) {
        const id = path.slice('/databases/'.length, -'/visibility'.length)
        const body = await request.json() as { visibility: Visibility; slug?: string }
        const database = await this.setVisibility(id, body.visibility, body.slug)
        if (!database) {
          return Response.json({ error: 'Database not found' }, { status: 404 })
        }
        return Response.json(database)
      }

      return Response.json({ error: 'Not found' }, { status: 404 })
    } catch (error) {
      console.error('[DatabaseIndexDO] Error:', error)
      return Response.json(
        { error: error instanceof Error ? error.message : 'Internal error' },
        { status: 500 }
      )
    }
  }

  // =============================================================================
  // Helpers
  // =============================================================================

  private generateId(): string {
    // Simple short ID using timestamp + random
    const timestamp = Date.now().toString(36)
    const random = Math.random().toString(36).slice(2, 8)
    return `db_${timestamp}${random}`
  }

  private rowToDatabase(row: Record<string, unknown>): DatabaseInfo {
    const visibility = row.visibility as string | undefined
    return {
      id: row.id as string,
      name: row.name as string,
      description: row.description as string | undefined,
      bucket: row.bucket as string,
      prefix: row.prefix as string | undefined,
      createdAt: new Date(row.created_at as number),
      createdBy: row.created_by as EntityId,
      lastAccessedAt: row.last_accessed_at
        ? new Date(row.last_accessed_at as number)
        : undefined,
      sizeBytes: row.size_bytes as number | undefined,
      collectionCount: row.collection_count as number | undefined,
      entityCount: row.entity_count as number | undefined,
      schemaVersion: row.schema_version as number | undefined,
      metadata: row.metadata
        ? JSON.parse(row.metadata as string)
        : undefined,
      visibility: isValidVisibility(visibility) ? visibility : DEFAULT_VISIBILITY,
      slug: row.slug as string | undefined,
      owner: row.owner as string | undefined,
    }
  }

  /**
   * Validate slug format
   * Must be lowercase alphanumeric with hyphens, 3-64 characters
   */
  private isValidSlug(slug: string): boolean {
    return /^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$/.test(slug) || /^[a-z0-9]{1,3}$/.test(slug)
  }
}

// =============================================================================
// Helper for Worker Integration
// =============================================================================

/**
 * Get the DatabaseIndex for a user
 *
 * @example
 * ```typescript
 * const index = getUserDatabaseIndex(env, userId)
 * const databases = await index.list()
 * ```
 */
export function getUserDatabaseIndex(
  env: { DATABASE_INDEX: DurableObjectNamespace<DatabaseIndexDO> },
  userId: string
): DurableObjectStub<DatabaseIndexDO> {
  const id = env.DATABASE_INDEX.idFromName(`user:${userId}`)
  return env.DATABASE_INDEX.get(id)
}
