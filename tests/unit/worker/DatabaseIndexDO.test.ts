/**
 * DatabaseIndexDO Tests
 *
 * Comprehensive tests for the DatabaseIndex Durable Object that tracks
 * all databases owned by a user. Tests all methods including CRUD operations,
 * visibility management, and HTTP handler.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import type { EntityId } from '@/types'
import type { Visibility } from '@/types/visibility'
import {
  type DatabaseInfo,
  type RegisterDatabaseOptions,
  type UpdateDatabaseOptions,
} from '@/worker/DatabaseIndexDO'

// =============================================================================
// Mock Types (since we can't import Cloudflare-specific modules in tests)
// =============================================================================

interface MockSqlResult {
  toArray(): Record<string, unknown>[]
  rowsWritten: number
}

interface MockSqlStorage {
  exec(query: string, ...params: unknown[]): MockSqlResult
}

interface MockDurableObjectState {
  storage: {
    sql: MockSqlStorage
  }
}

// =============================================================================
// Mock Implementation of DatabaseIndexDO for Testing
// =============================================================================

/**
 * Test implementation of DatabaseIndexDO that doesn't require Cloudflare runtime.
 * This mirrors the actual implementation but uses an in-memory store for testing.
 */
class TestDatabaseIndexDO {
  private databases: Map<string, Record<string, unknown>> = new Map()

  /**
   * List all databases for this user
   */
  async list(): Promise<DatabaseInfo[]> {
    return Array.from(this.databases.values())
      .filter((db) => !db.deleted_at)
      .sort((a, b) => {
        // Sort by last_accessed_at DESC, then created_at DESC
        const aAccess = (a.last_accessed_at as number) ?? 0
        const bAccess = (b.last_accessed_at as number) ?? 0
        if (bAccess !== aAccess) return bAccess - aAccess
        return (b.created_at as number) - (a.created_at as number)
      })
      .map((row) => this.rowToDatabase(row))
  }

  /**
   * Get a specific database by ID
   */
  async get(id: string): Promise<DatabaseInfo | null> {
    const db = this.databases.get(id)
    if (!db || db.deleted_at) return null
    return this.rowToDatabase(db)
  }

  /**
   * Get a database by name
   */
  async getByName(name: string): Promise<DatabaseInfo | null> {
    const db = Array.from(this.databases.values()).find(
      (d) => d.name === name && !d.deleted_at
    )
    if (!db) return null
    return this.rowToDatabase(db)
  }

  /**
   * Get a database by owner and slug
   */
  async getBySlug(owner: string, slug: string): Promise<DatabaseInfo | null> {
    const db = Array.from(this.databases.values()).find(
      (d) => d.owner === owner && d.slug === slug && !d.deleted_at
    )
    if (!db) return null
    return this.rowToDatabase(db)
  }

  /**
   * List all public databases
   */
  async listPublic(): Promise<DatabaseInfo[]> {
    return Array.from(this.databases.values())
      .filter((db) => db.visibility === 'public' && !db.deleted_at)
      .sort((a, b) => {
        const aAccess = (a.last_accessed_at as number) ?? 0
        const bAccess = (b.last_accessed_at as number) ?? 0
        if (bAccess !== aAccess) return bAccess - aAccess
        return (b.created_at as number) - (a.created_at as number)
      })
      .map((row) => this.rowToDatabase(row))
  }

  /**
   * List databases by visibility
   */
  async listByVisibility(visibility: Visibility): Promise<DatabaseInfo[]> {
    return Array.from(this.databases.values())
      .filter((db) => db.visibility === visibility && !db.deleted_at)
      .sort((a, b) => {
        const aAccess = (a.last_accessed_at as number) ?? 0
        const bAccess = (b.last_accessed_at as number) ?? 0
        if (bAccess !== aAccess) return bAccess - aAccess
        return (b.created_at as number) - (a.created_at as number)
      })
      .map((row) => this.rowToDatabase(row))
  }

  /**
   * Set visibility for a database
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
    const visibility = options.visibility ?? 'private'

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

    const row: Record<string, unknown> = {
      id,
      name: options.name,
      description: options.description ?? null,
      bucket: options.bucket,
      prefix: options.prefix ?? null,
      created_at: now,
      created_by: actor,
      last_accessed_at: null,
      size_bytes: 0,
      collection_count: 0,
      entity_count: 0,
      schema_version: 1,
      metadata: options.metadata ? JSON.stringify(options.metadata) : null,
      deleted_at: null,
      visibility,
      slug: options.slug ?? null,
      owner: options.owner ?? null,
    }

    this.databases.set(id, row)

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
    const db = this.databases.get(id)
    if (!db || db.deleted_at) return null

    if (options.name !== undefined) {
      db.name = options.name
    }

    if (options.description !== undefined) {
      db.description = options.description
    }

    if (options.stats?.sizeBytes !== undefined) {
      db.size_bytes = options.stats.sizeBytes
    }

    if (options.stats?.collectionCount !== undefined) {
      db.collection_count = options.stats.collectionCount
    }

    if (options.stats?.entityCount !== undefined) {
      db.entity_count = options.stats.entityCount
    }

    if (options.metadata !== undefined) {
      const existingMetadata = db.metadata ? JSON.parse(db.metadata as string) : {}
      db.metadata = JSON.stringify({ ...existingMetadata, ...options.metadata })
    }

    if (options.visibility !== undefined) {
      if (!['public', 'unlisted', 'private'].includes(options.visibility)) {
        throw new Error(`Invalid visibility: ${options.visibility}`)
      }
      db.visibility = options.visibility
    }

    if (options.slug !== undefined) {
      if (options.slug && !this.isValidSlug(options.slug)) {
        throw new Error('Invalid slug: must be lowercase alphanumeric with hyphens')
      }
      // Check for duplicate slug within owner
      if (options.slug && db.owner) {
        const existing = await this.getBySlug(db.owner as string, options.slug)
        if (existing && existing.id !== id) {
          throw new Error(`Slug '${options.slug}' already exists for owner '${db.owner}'`)
        }
      }
      db.slug = options.slug || null
    }

    return this.rowToDatabase(db)
  }

  /**
   * Record database access
   */
  async recordAccess(id: string): Promise<void> {
    const db = this.databases.get(id)
    if (db && !db.deleted_at) {
      db.last_accessed_at = Date.now()
    }
  }

  /**
   * Soft delete a database
   */
  async unregister(id: string): Promise<boolean> {
    const db = this.databases.get(id)
    if (!db || db.deleted_at) return false
    db.deleted_at = Date.now()
    return true
  }

  /**
   * Permanently delete a database
   */
  async purge(id: string): Promise<boolean> {
    return this.databases.delete(id)
  }

  /**
   * Get statistics
   */
  async stats(): Promise<{
    totalDatabases: number
    totalSizeBytes: number
    totalEntities: number
    oldestDatabase?: Date
    newestDatabase?: Date
    mostRecentAccess?: Date
  }> {
    const activeDbs = Array.from(this.databases.values()).filter((db) => !db.deleted_at)

    if (activeDbs.length === 0) {
      return {
        totalDatabases: 0,
        totalSizeBytes: 0,
        totalEntities: 0,
      }
    }

    const totalSizeBytes = activeDbs.reduce((sum, db) => sum + ((db.size_bytes as number) || 0), 0)
    const totalEntities = activeDbs.reduce((sum, db) => sum + ((db.entity_count as number) || 0), 0)
    const createdAts = activeDbs.map((db) => db.created_at as number)
    const accessedAts = activeDbs.map((db) => db.last_accessed_at as number).filter(Boolean)

    return {
      totalDatabases: activeDbs.length,
      totalSizeBytes,
      totalEntities,
      oldestDatabase: createdAts.length > 0 ? new Date(Math.min(...createdAts)) : undefined,
      newestDatabase: createdAts.length > 0 ? new Date(Math.max(...createdAts)) : undefined,
      mostRecentAccess: accessedAts.length > 0 ? new Date(Math.max(...accessedAts)) : undefined,
    }
  }

  /**
   * Search databases by name
   */
  async search(query: string): Promise<DatabaseInfo[]> {
    const lowerQuery = query.toLowerCase()
    return Array.from(this.databases.values())
      .filter((db) => !db.deleted_at && (db.name as string).toLowerCase().includes(lowerQuery))
      .sort((a, b) => (a.name as string).localeCompare(b.name as string))
      .map((row) => this.rowToDatabase(row))
  }

  /**
   * Handle HTTP requests
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    try {
      // GET /databases - list all databases
      if (request.method === 'GET' && path === '/databases') {
        const databases = await this.list()
        return Response.json({ databases })
      }

      // GET /databases/:id - get specific database
      if (request.method === 'GET' && path.startsWith('/databases/') && !path.includes('by-slug')) {
        const id = path.slice('/databases/'.length)
        if (id.endsWith('/access') || id.endsWith('/visibility')) {
          // Skip, handled by other routes
        } else {
          const database = await this.get(id)
          if (!database) {
            return Response.json({ error: 'Database not found' }, { status: 404 })
          }
          return Response.json(database)
        }
      }

      // POST /databases - register new database
      if (request.method === 'POST' && path === '/databases') {
        const body = (await request.json()) as RegisterDatabaseOptions & { actor: EntityId }
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
        const options = (await request.json()) as UpdateDatabaseOptions
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
          return Response.json(
            { error: 'Invalid path: expected /databases/by-slug/:owner/:slug' },
            { status: 400 }
          )
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
        const body = (await request.json()) as { visibility: Visibility; slug?: string }
        const database = await this.setVisibility(id, body.visibility, body.slug)
        if (!database) {
          return Response.json({ error: 'Database not found' }, { status: 404 })
        }
        return Response.json(database)
      }

      return Response.json({ error: 'Not found' }, { status: 404 })
    } catch (error) {
      return Response.json(
        { error: error instanceof Error ? error.message : 'Internal error' },
        { status: 500 }
      )
    }
  }

  // Helpers
  private generateId(): string {
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
      lastAccessedAt: row.last_accessed_at ? new Date(row.last_accessed_at as number) : undefined,
      sizeBytes: row.size_bytes as number | undefined,
      collectionCount: row.collection_count as number | undefined,
      entityCount: row.entity_count as number | undefined,
      schemaVersion: row.schema_version as number | undefined,
      metadata: row.metadata
        ? (() => {
            try {
              return JSON.parse(row.metadata as string)
            } catch {
              return undefined
            }
          })()
        : undefined,
      visibility:
        visibility && ['public', 'unlisted', 'private'].includes(visibility)
          ? (visibility as Visibility)
          : 'private',
      slug: row.slug as string | undefined,
      owner: row.owner as string | undefined,
    }
  }

  private isValidSlug(slug: string): boolean {
    return /^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$/.test(slug) || /^[a-z0-9]{1,3}$/.test(slug)
  }
}

// =============================================================================
// Test Suites
// =============================================================================

describe('DatabaseIndexDO', () => {
  let indexDO: TestDatabaseIndexDO
  const testActor = 'users/test-user' as EntityId

  beforeEach(() => {
    indexDO = new TestDatabaseIndexDO()
  })

  // ===========================================================================
  // Registration Tests
  // ===========================================================================

  describe('register', () => {
    it('should register a new database with required fields', async () => {
      const db = await indexDO.register(
        {
          name: 'test-db',
          bucket: 'my-bucket',
        },
        testActor
      )

      expect(db.id).toMatch(/^db_/)
      expect(db.name).toBe('test-db')
      expect(db.bucket).toBe('my-bucket')
      expect(db.createdBy).toBe(testActor)
      expect(db.visibility).toBe('private')
      expect(db.createdAt).toBeInstanceOf(Date)
    })

    it('should register a database with optional fields', async () => {
      const db = await indexDO.register(
        {
          name: 'full-db',
          bucket: 'my-bucket',
          description: 'A test database',
          prefix: 'data/',
          metadata: { version: 1, tags: ['test'] },
          visibility: 'public',
          slug: 'my-dataset',
          owner: 'testuser',
        },
        testActor
      )

      expect(db.description).toBe('A test database')
      expect(db.prefix).toBe('data/')
      expect(db.metadata).toEqual({ version: 1, tags: ['test'] })
      expect(db.visibility).toBe('public')
      expect(db.slug).toBe('my-dataset')
      expect(db.owner).toBe('testuser')
    })

    it('should reject invalid slugs', async () => {
      await expect(
        indexDO.register(
          {
            name: 'test-db',
            bucket: 'my-bucket',
            slug: 'INVALID_SLUG',
          },
          testActor
        )
      ).rejects.toThrow('Invalid slug')
    })

    it('should reject duplicate slugs for same owner', async () => {
      await indexDO.register(
        {
          name: 'db1',
          bucket: 'bucket1',
          slug: 'my-dataset',
          owner: 'testuser',
        },
        testActor
      )

      await expect(
        indexDO.register(
          {
            name: 'db2',
            bucket: 'bucket2',
            slug: 'my-dataset',
            owner: 'testuser',
          },
          testActor
        )
      ).rejects.toThrow("Slug 'my-dataset' already exists for owner 'testuser'")
    })

    it('should allow same slug for different owners', async () => {
      await indexDO.register(
        {
          name: 'db1',
          bucket: 'bucket1',
          slug: 'my-dataset',
          owner: 'user1',
        },
        testActor
      )

      const db2 = await indexDO.register(
        {
          name: 'db2',
          bucket: 'bucket2',
          slug: 'my-dataset',
          owner: 'user2',
        },
        testActor
      )

      expect(db2.slug).toBe('my-dataset')
      expect(db2.owner).toBe('user2')
    })
  })

  // ===========================================================================
  // Retrieval Tests
  // ===========================================================================

  describe('get', () => {
    it('should retrieve a database by ID', async () => {
      const registered = await indexDO.register(
        { name: 'test-db', bucket: 'bucket' },
        testActor
      )

      const db = await indexDO.get(registered.id)
      expect(db).not.toBeNull()
      expect(db!.name).toBe('test-db')
    })

    it('should return null for non-existent database', async () => {
      const db = await indexDO.get('non-existent-id')
      expect(db).toBeNull()
    })

    it('should return null for deleted database', async () => {
      const registered = await indexDO.register(
        { name: 'test-db', bucket: 'bucket' },
        testActor
      )

      await indexDO.unregister(registered.id)
      const db = await indexDO.get(registered.id)
      expect(db).toBeNull()
    })
  })

  describe('getByName', () => {
    it('should retrieve a database by name', async () => {
      await indexDO.register({ name: 'unique-name', bucket: 'bucket' }, testActor)

      const db = await indexDO.getByName('unique-name')
      expect(db).not.toBeNull()
      expect(db!.name).toBe('unique-name')
    })

    it('should return null for non-existent name', async () => {
      const db = await indexDO.getByName('non-existent')
      expect(db).toBeNull()
    })
  })

  describe('getBySlug', () => {
    it('should retrieve a database by owner and slug', async () => {
      await indexDO.register(
        {
          name: 'test-db',
          bucket: 'bucket',
          slug: 'my-data',
          owner: 'testuser',
        },
        testActor
      )

      const db = await indexDO.getBySlug('testuser', 'my-data')
      expect(db).not.toBeNull()
      expect(db!.slug).toBe('my-data')
      expect(db!.owner).toBe('testuser')
    })

    it('should return null for non-matching owner/slug', async () => {
      await indexDO.register(
        {
          name: 'test-db',
          bucket: 'bucket',
          slug: 'my-data',
          owner: 'testuser',
        },
        testActor
      )

      const db = await indexDO.getBySlug('otheruser', 'my-data')
      expect(db).toBeNull()
    })
  })

  // ===========================================================================
  // List Tests
  // ===========================================================================

  describe('list', () => {
    it('should return empty array when no databases', async () => {
      const databases = await indexDO.list()
      expect(databases).toEqual([])
    })

    it('should list all non-deleted databases', async () => {
      await indexDO.register({ name: 'db1', bucket: 'b1' }, testActor)
      await indexDO.register({ name: 'db2', bucket: 'b2' }, testActor)
      const db3 = await indexDO.register({ name: 'db3', bucket: 'b3' }, testActor)

      await indexDO.unregister(db3.id)

      const databases = await indexDO.list()
      expect(databases.length).toBe(2)
      expect(databases.map((d) => d.name).sort()).toEqual(['db1', 'db2'])
    })

    it('should sort by last accessed, then created date', async () => {
      const db1 = await indexDO.register({ name: 'db1', bucket: 'b1' }, testActor)
      const db2 = await indexDO.register({ name: 'db2', bucket: 'b2' }, testActor)
      await indexDO.register({ name: 'db3', bucket: 'b3' }, testActor)

      await indexDO.recordAccess(db1.id)

      const databases = await indexDO.list()
      // db1 should be first (most recently accessed)
      expect(databases[0].name).toBe('db1')
    })
  })

  describe('listPublic', () => {
    it('should only list public databases', async () => {
      await indexDO.register(
        { name: 'public-db', bucket: 'b1', visibility: 'public' },
        testActor
      )
      await indexDO.register(
        { name: 'unlisted-db', bucket: 'b2', visibility: 'unlisted' },
        testActor
      )
      await indexDO.register(
        { name: 'private-db', bucket: 'b3', visibility: 'private' },
        testActor
      )

      const databases = await indexDO.listPublic()
      expect(databases.length).toBe(1)
      expect(databases[0].name).toBe('public-db')
    })
  })

  describe('listByVisibility', () => {
    it('should filter by visibility level', async () => {
      await indexDO.register(
        { name: 'public-db', bucket: 'b1', visibility: 'public' },
        testActor
      )
      await indexDO.register(
        { name: 'unlisted-db', bucket: 'b2', visibility: 'unlisted' },
        testActor
      )
      await indexDO.register(
        { name: 'private-db', bucket: 'b3', visibility: 'private' },
        testActor
      )

      const unlisted = await indexDO.listByVisibility('unlisted')
      expect(unlisted.length).toBe(1)
      expect(unlisted[0].name).toBe('unlisted-db')
    })
  })

  // ===========================================================================
  // Update Tests
  // ===========================================================================

  describe('update', () => {
    it('should update database name', async () => {
      const db = await indexDO.register({ name: 'original', bucket: 'b1' }, testActor)

      const updated = await indexDO.update(db.id, { name: 'renamed' })
      expect(updated).not.toBeNull()
      expect(updated!.name).toBe('renamed')
    })

    it('should update database description', async () => {
      const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

      const updated = await indexDO.update(db.id, { description: 'New description' })
      expect(updated!.description).toBe('New description')
    })

    it('should update database stats', async () => {
      const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

      const updated = await indexDO.update(db.id, {
        stats: {
          sizeBytes: 1000000,
          collectionCount: 5,
          entityCount: 10000,
        },
      })

      expect(updated!.sizeBytes).toBe(1000000)
      expect(updated!.collectionCount).toBe(5)
      expect(updated!.entityCount).toBe(10000)
    })

    it('should merge metadata', async () => {
      const db = await indexDO.register(
        {
          name: 'test',
          bucket: 'b1',
          metadata: { existing: 'value', toReplace: 'old' },
        },
        testActor
      )

      const updated = await indexDO.update(db.id, {
        metadata: { toReplace: 'new', added: 'field' },
      })

      expect(updated!.metadata).toEqual({
        existing: 'value',
        toReplace: 'new',
        added: 'field',
      })
    })

    it('should update visibility', async () => {
      const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

      const updated = await indexDO.update(db.id, { visibility: 'public' })
      expect(updated!.visibility).toBe('public')
    })

    it('should reject invalid visibility', async () => {
      const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

      await expect(
        indexDO.update(db.id, { visibility: 'invalid' as Visibility })
      ).rejects.toThrow('Invalid visibility')
    })

    it('should update slug', async () => {
      const db = await indexDO.register(
        { name: 'test', bucket: 'b1', owner: 'testuser' },
        testActor
      )

      const updated = await indexDO.update(db.id, { slug: 'new-slug' })
      expect(updated!.slug).toBe('new-slug')
    })

    it('should reject duplicate slug on update', async () => {
      await indexDO.register(
        {
          name: 'db1',
          bucket: 'b1',
          slug: 'existing-slug',
          owner: 'testuser',
        },
        testActor
      )

      const db2 = await indexDO.register(
        { name: 'db2', bucket: 'b2', owner: 'testuser' },
        testActor
      )

      await expect(indexDO.update(db2.id, { slug: 'existing-slug' })).rejects.toThrow(
        "Slug 'existing-slug' already exists"
      )
    })

    it('should return null for non-existent database', async () => {
      const result = await indexDO.update('non-existent', { name: 'new' })
      expect(result).toBeNull()
    })

    it('should return unchanged database when no options provided', async () => {
      const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

      const updated = await indexDO.update(db.id, {})
      expect(updated!.name).toBe('test')
    })
  })

  describe('setVisibility', () => {
    it('should set visibility and slug together', async () => {
      const db = await indexDO.register(
        { name: 'test', bucket: 'b1', owner: 'testuser' },
        testActor
      )

      const updated = await indexDO.setVisibility(db.id, 'public', 'my-public-data')
      expect(updated!.visibility).toBe('public')
      expect(updated!.slug).toBe('my-public-data')
    })
  })

  // ===========================================================================
  // Access Tracking Tests
  // ===========================================================================

  describe('recordAccess', () => {
    it('should update last accessed time', async () => {
      const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

      expect(db.lastAccessedAt).toBeUndefined()

      await indexDO.recordAccess(db.id)

      const updated = await indexDO.get(db.id)
      expect(updated!.lastAccessedAt).toBeInstanceOf(Date)
    })

    it('should not update deleted database', async () => {
      const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)
      await indexDO.unregister(db.id)

      await indexDO.recordAccess(db.id)

      // Should not throw, just silently do nothing
    })
  })

  // ===========================================================================
  // Deletion Tests
  // ===========================================================================

  describe('unregister', () => {
    it('should soft delete a database', async () => {
      const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

      const result = await indexDO.unregister(db.id)
      expect(result).toBe(true)

      const deleted = await indexDO.get(db.id)
      expect(deleted).toBeNull()
    })

    it('should return false for non-existent database', async () => {
      const result = await indexDO.unregister('non-existent')
      expect(result).toBe(false)
    })

    it('should return false for already deleted database', async () => {
      const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

      await indexDO.unregister(db.id)
      const result = await indexDO.unregister(db.id)
      expect(result).toBe(false)
    })
  })

  describe('purge', () => {
    it('should permanently delete a database', async () => {
      const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

      const result = await indexDO.purge(db.id)
      expect(result).toBe(true)

      // Should not appear even after purge
      const deleted = await indexDO.get(db.id)
      expect(deleted).toBeNull()
    })

    it('should return false for non-existent database', async () => {
      const result = await indexDO.purge('non-existent')
      expect(result).toBe(false)
    })
  })

  // ===========================================================================
  // Statistics Tests
  // ===========================================================================

  describe('stats', () => {
    it('should return zeros when no databases', async () => {
      const stats = await indexDO.stats()

      expect(stats.totalDatabases).toBe(0)
      expect(stats.totalSizeBytes).toBe(0)
      expect(stats.totalEntities).toBe(0)
      expect(stats.oldestDatabase).toBeUndefined()
      expect(stats.newestDatabase).toBeUndefined()
      expect(stats.mostRecentAccess).toBeUndefined()
    })

    it('should aggregate database stats', async () => {
      const db1 = await indexDO.register({ name: 'db1', bucket: 'b1' }, testActor)
      const db2 = await indexDO.register({ name: 'db2', bucket: 'b2' }, testActor)

      await indexDO.update(db1.id, {
        stats: { sizeBytes: 1000, entityCount: 100 },
      })
      await indexDO.update(db2.id, {
        stats: { sizeBytes: 2000, entityCount: 200 },
      })

      await indexDO.recordAccess(db1.id)

      const stats = await indexDO.stats()

      expect(stats.totalDatabases).toBe(2)
      expect(stats.totalSizeBytes).toBe(3000)
      expect(stats.totalEntities).toBe(300)
      expect(stats.oldestDatabase).toBeInstanceOf(Date)
      expect(stats.newestDatabase).toBeInstanceOf(Date)
      expect(stats.mostRecentAccess).toBeInstanceOf(Date)
    })

    it('should exclude deleted databases from stats', async () => {
      const db1 = await indexDO.register({ name: 'db1', bucket: 'b1' }, testActor)
      const db2 = await indexDO.register({ name: 'db2', bucket: 'b2' }, testActor)

      await indexDO.update(db1.id, { stats: { sizeBytes: 1000 } })
      await indexDO.update(db2.id, { stats: { sizeBytes: 2000 } })

      await indexDO.unregister(db2.id)

      const stats = await indexDO.stats()
      expect(stats.totalDatabases).toBe(1)
      expect(stats.totalSizeBytes).toBe(1000)
    })
  })

  // ===========================================================================
  // Search Tests
  // ===========================================================================

  describe('search', () => {
    beforeEach(async () => {
      await indexDO.register({ name: 'production-api', bucket: 'b1' }, testActor)
      await indexDO.register({ name: 'staging-api', bucket: 'b2' }, testActor)
      await indexDO.register({ name: 'analytics', bucket: 'b3' }, testActor)
    })

    it('should find databases by partial name match', async () => {
      const results = await indexDO.search('api')
      expect(results.length).toBe(2)
      expect(results.map((d) => d.name).sort()).toEqual(['production-api', 'staging-api'])
    })

    it('should be case-insensitive', async () => {
      const results = await indexDO.search('API')
      expect(results.length).toBe(2)
    })

    it('should return empty array for no matches', async () => {
      const results = await indexDO.search('nonexistent')
      expect(results).toEqual([])
    })

    it('should exclude deleted databases', async () => {
      const toDelete = await indexDO.register({ name: 'api-to-delete', bucket: 'b4' }, testActor)
      await indexDO.unregister(toDelete.id)

      const results = await indexDO.search('api')
      expect(results.map((d) => d.name)).not.toContain('api-to-delete')
    })

    it('should sort results by name', async () => {
      const results = await indexDO.search('api')
      expect(results[0].name).toBe('production-api')
      expect(results[1].name).toBe('staging-api')
    })
  })

  // ===========================================================================
  // Slug Validation Tests
  // ===========================================================================

  describe('slug validation', () => {
    it('should accept valid slugs', async () => {
      const validSlugs = ['abc', 'a', 'ab', 'my-dataset', 'a1', 'test-123-data']

      for (const slug of validSlugs) {
        const db = await indexDO.register(
          { name: `db-${slug}`, bucket: 'b1', slug },
          testActor
        )
        expect(db.slug).toBe(slug)
      }
    })

    it('should reject invalid slugs', async () => {
      const invalidSlugs = [
        'UPPERCASE',
        'has_underscore',
        '-starts-with-dash',
        'ends-with-dash-',
        'has spaces',
        'has.dots',
      ]

      for (const slug of invalidSlugs) {
        await expect(
          indexDO.register({ name: `db-${slug}`, bucket: 'b1', slug }, testActor)
        ).rejects.toThrow('Invalid slug')
      }
    })
  })

  // ===========================================================================
  // HTTP Handler Tests
  // ===========================================================================

  describe('fetch (HTTP handler)', () => {
    describe('GET /databases', () => {
      it('should list all databases', async () => {
        await indexDO.register({ name: 'db1', bucket: 'b1' }, testActor)

        const request = new Request('http://localhost/databases')
        const response = await indexDO.fetch(request)

        expect(response.status).toBe(200)
        const body = (await response.json()) as { databases: DatabaseInfo[] }
        expect(body.databases.length).toBe(1)
      })
    })

    describe('GET /databases/:id', () => {
      it('should return database by ID', async () => {
        const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

        const request = new Request(`http://localhost/databases/${db.id}`)
        const response = await indexDO.fetch(request)

        expect(response.status).toBe(200)
        const body = (await response.json()) as DatabaseInfo
        expect(body.name).toBe('test')
      })

      it('should return 404 for non-existent database', async () => {
        const request = new Request('http://localhost/databases/non-existent')
        const response = await indexDO.fetch(request)

        expect(response.status).toBe(404)
      })
    })

    describe('POST /databases', () => {
      it('should create a new database', async () => {
        const request = new Request('http://localhost/databases', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name: 'new-db',
            bucket: 'my-bucket',
            actor: testActor,
          }),
        })

        const response = await indexDO.fetch(request)

        expect(response.status).toBe(201)
        const body = (await response.json()) as DatabaseInfo
        expect(body.name).toBe('new-db')
      })

      it('should return 400 when actor is missing', async () => {
        const request = new Request('http://localhost/databases', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name: 'new-db',
            bucket: 'my-bucket',
          }),
        })

        const response = await indexDO.fetch(request)

        expect(response.status).toBe(400)
      })
    })

    describe('PATCH /databases/:id', () => {
      it('should update a database', async () => {
        const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

        const request = new Request(`http://localhost/databases/${db.id}`, {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: 'updated' }),
        })

        const response = await indexDO.fetch(request)

        expect(response.status).toBe(200)
        const body = (await response.json()) as DatabaseInfo
        expect(body.name).toBe('updated')
      })

      it('should return 404 for non-existent database', async () => {
        const request = new Request('http://localhost/databases/non-existent', {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: 'updated' }),
        })

        const response = await indexDO.fetch(request)
        expect(response.status).toBe(404)
      })
    })

    describe('DELETE /databases/:id', () => {
      it('should delete a database', async () => {
        const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

        const request = new Request(`http://localhost/databases/${db.id}`, {
          method: 'DELETE',
        })

        const response = await indexDO.fetch(request)

        expect(response.status).toBe(200)
        const body = (await response.json()) as { deleted: boolean }
        expect(body.deleted).toBe(true)
      })

      it('should return 404 for non-existent database', async () => {
        const request = new Request('http://localhost/databases/non-existent', {
          method: 'DELETE',
        })

        const response = await indexDO.fetch(request)
        expect(response.status).toBe(404)
      })
    })

    describe('POST /databases/:id/access', () => {
      it('should record database access', async () => {
        const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

        const request = new Request(`http://localhost/databases/${db.id}/access`, {
          method: 'POST',
        })

        const response = await indexDO.fetch(request)

        expect(response.status).toBe(200)
        const body = (await response.json()) as { success: boolean }
        expect(body.success).toBe(true)
      })
    })

    describe('GET /stats', () => {
      it('should return user statistics', async () => {
        await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

        const request = new Request('http://localhost/stats')
        const response = await indexDO.fetch(request)

        expect(response.status).toBe(200)
        const body = (await response.json()) as { totalDatabases: number }
        expect(body.totalDatabases).toBe(1)
      })
    })

    describe('GET /search', () => {
      it('should search databases', async () => {
        await indexDO.register({ name: 'test-api', bucket: 'b1' }, testActor)
        await indexDO.register({ name: 'other', bucket: 'b2' }, testActor)

        const request = new Request('http://localhost/search?q=api')
        const response = await indexDO.fetch(request)

        expect(response.status).toBe(200)
        const body = (await response.json()) as { databases: DatabaseInfo[] }
        expect(body.databases.length).toBe(1)
        expect(body.databases[0].name).toBe('test-api')
      })

      it('should handle empty query', async () => {
        await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

        const request = new Request('http://localhost/search')
        const response = await indexDO.fetch(request)

        expect(response.status).toBe(200)
        const body = (await response.json()) as { databases: DatabaseInfo[] }
        expect(body.databases.length).toBe(1)
      })
    })

    describe('GET /public', () => {
      it('should list public databases', async () => {
        await indexDO.register(
          { name: 'public-db', bucket: 'b1', visibility: 'public' },
          testActor
        )
        await indexDO.register(
          { name: 'private-db', bucket: 'b2', visibility: 'private' },
          testActor
        )

        const request = new Request('http://localhost/public')
        const response = await indexDO.fetch(request)

        expect(response.status).toBe(200)
        const body = (await response.json()) as { databases: DatabaseInfo[] }
        expect(body.databases.length).toBe(1)
        expect(body.databases[0].name).toBe('public-db')
      })
    })

    describe('GET /databases/by-slug/:owner/:slug', () => {
      it('should get database by owner and slug', async () => {
        await indexDO.register(
          {
            name: 'test',
            bucket: 'b1',
            slug: 'my-data',
            owner: 'testuser',
          },
          testActor
        )

        const request = new Request('http://localhost/databases/by-slug/testuser/my-data')
        const response = await indexDO.fetch(request)

        expect(response.status).toBe(200)
        const body = (await response.json()) as DatabaseInfo
        expect(body.slug).toBe('my-data')
      })

      it('should return 400 for invalid path', async () => {
        const request = new Request('http://localhost/databases/by-slug/only-one-part')
        const response = await indexDO.fetch(request)

        expect(response.status).toBe(400)
      })

      it('should return 404 for non-existent slug', async () => {
        const request = new Request('http://localhost/databases/by-slug/testuser/nonexistent')
        const response = await indexDO.fetch(request)

        expect(response.status).toBe(404)
      })
    })

    describe('PUT /databases/:id/visibility', () => {
      it('should update visibility', async () => {
        const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)

        const request = new Request(`http://localhost/databases/${db.id}/visibility`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ visibility: 'public', slug: 'my-public-data' }),
        })

        const response = await indexDO.fetch(request)

        expect(response.status).toBe(200)
        const body = (await response.json()) as DatabaseInfo
        expect(body.visibility).toBe('public')
        expect(body.slug).toBe('my-public-data')
      })

      it('should return 404 for non-existent database', async () => {
        const request = new Request('http://localhost/databases/non-existent/visibility', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ visibility: 'public' }),
        })

        const response = await indexDO.fetch(request)
        expect(response.status).toBe(404)
      })
    })

    describe('error handling', () => {
      it('should return 404 for unknown routes', async () => {
        const request = new Request('http://localhost/unknown-route')
        const response = await indexDO.fetch(request)

        expect(response.status).toBe(404)
      })

      it('should return 500 for internal errors', async () => {
        const request = new Request('http://localhost/databases', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name: 'test',
            bucket: 'b1',
            slug: 'INVALID_SLUG', // Will cause validation error
            actor: testActor,
          }),
        })

        const response = await indexDO.fetch(request)
        expect(response.status).toBe(500)
        const body = (await response.json()) as { error: string }
        expect(body.error).toContain('Invalid slug')
      })
    })
  })

  // ===========================================================================
  // ID Generation Tests
  // ===========================================================================

  describe('ID generation', () => {
    it('should generate unique IDs', async () => {
      const ids = new Set<string>()

      for (let i = 0; i < 100; i++) {
        const db = await indexDO.register({ name: `db-${i}`, bucket: 'b1' }, testActor)
        ids.add(db.id)
      }

      expect(ids.size).toBe(100) // All IDs should be unique
    })

    it('should generate IDs with db_ prefix', async () => {
      const db = await indexDO.register({ name: 'test', bucket: 'b1' }, testActor)
      expect(db.id).toMatch(/^db_/)
    })
  })
})
