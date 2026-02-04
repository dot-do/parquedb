/**
 * Public Routes Tests
 *
 * Tests for the public database routes including:
 * - CORS handling (OPTIONS preflight, public and authenticated headers)
 * - Rate limiting integration
 * - Database metadata endpoint
 * - Collection query endpoint
 * - Raw file access with Range headers
 * - Visibility checks and authentication
 * - JWT token decoding and ownership verification
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

// Mock jwt-utils to avoid real JWKS verification in tests
vi.mock('@/worker/jwt-utils', async (importOriginal) => {
  const original = await importOriginal<typeof import('@/worker/jwt-utils')>()
  return {
    ...original,
    extractBearerToken: original.extractBearerToken,
    verifyOwnership: vi.fn(async (token: string | null, owner: string) => {
      if (!token) return false
      // Decode the mock JWT payload (second segment, base64url-encoded)
      try {
        const parts = token.split('.')
        if (parts.length !== 3) return false
        const payloadPart = parts[1]!
        // Restore base64 padding
        const base64 = payloadPart.replace(/-/g, '+').replace(/_/g, '/')
        const padding = '='.repeat((4 - base64.length % 4) % 4)
        const payload = JSON.parse(atob(base64 + padding))
        const normalizedOwner = owner.toLowerCase()
        if (typeof payload.sub === 'string' && payload.sub.toLowerCase() === normalizedOwner) return true
        if (typeof payload.username === 'string' && payload.username.toLowerCase() === normalizedOwner) return true
        if (typeof payload.preferred_username === 'string' && payload.preferred_username.toLowerCase() === normalizedOwner) return true
        return false
      } catch {
        return false
      }
    }),
  }
})

import { handlePublicRoutes } from '@/worker/public-routes'
import type { Env } from '@/types/worker'
import type { DatabaseInfo } from '@/worker/DatabaseIndexDO'
import type { RateLimitResult } from '@/worker/rate-limit-utils'
import { setLogger, consoleLogger, noopLogger } from '@/utils/logger'

// =============================================================================
// Mock Helpers
// =============================================================================

/**
 * Create a minimal mock environment
 */
function createMockEnv(overrides: Partial<Env> = {}): Env {
  return {
    PARQUEDB: {} as DurableObjectNamespace,
    BUCKET: createMockBucket(),
    ...overrides,
  } as Env
}

/**
 * Create a mock R2 bucket
 */
function createMockBucket(overrides: Partial<R2Bucket> = {}): R2Bucket {
  return {
    head: vi.fn().mockResolvedValue(null),
    get: vi.fn().mockResolvedValue(null),
    put: vi.fn().mockResolvedValue(null),
    delete: vi.fn().mockResolvedValue(undefined),
    list: vi.fn().mockResolvedValue({ objects: [], truncated: false }),
    createMultipartUpload: vi.fn(),
    resumeMultipartUpload: vi.fn(),
    ...overrides,
  } as unknown as R2Bucket
}

/**
 * Create a mock DatabaseIndexDO stub
 */
function createMockDatabaseIndex(databases: Map<string, DatabaseInfo> = new Map()) {
  return {
    getBySlug: vi.fn(async (owner: string, slug: string) => {
      return databases.get(`${owner}/${slug}`) || null
    }),
    list: vi.fn().mockResolvedValue([...databases.values()]),
  }
}

/**
 * Create a mock rate limiter DO stub
 */
function createMockRateLimiter(result: RateLimitResult = {
  allowed: true,
  remaining: 99,
  resetAt: Date.now() + 60000,
  limit: 100,
  current: 1,
}) {
  return {
    checkLimit: vi.fn().mockResolvedValue(result),
    getStatus: vi.fn().mockResolvedValue(result),
    reset: vi.fn().mockResolvedValue(undefined),
  }
}

/**
 * Create a mock DurableObjectNamespace
 */
function createMockDONamespace<T>(stub: T) {
  return {
    idFromName: vi.fn().mockReturnValue({ toString: () => 'mock-id' }),
    get: vi.fn().mockReturnValue(stub),
    newUniqueId: vi.fn().mockReturnValue({ toString: () => 'unique-id' }),
  } as unknown as DurableObjectNamespace
}

/**
 * Create a sample database info
 */
function createDatabaseInfo(overrides: Partial<DatabaseInfo> = {}): DatabaseInfo {
  return {
    id: 'db-123',
    name: 'Test Database',
    description: 'A test database',
    bucket: 'test-bucket',
    prefix: 'data',
    createdAt: new Date('2024-01-01'),
    createdBy: 'user/testuser' as any,
    visibility: 'public',
    slug: 'test-db',
    owner: 'testuser',
    collectionCount: 5,
    entityCount: 1000,
    ...overrides,
  }
}

/**
 * Create a mock R2 object
 */
function createMockR2Object(content: ArrayBuffer | null, metadata: Partial<R2Object> = {}): R2ObjectBody | R2Object | null {
  if (content === null) return null

  return {
    key: 'test-file.parquet',
    version: 'v1',
    size: content.byteLength,
    etag: '"mock-etag"',
    httpEtag: '"mock-etag"',
    checksums: {},
    uploaded: new Date(),
    httpMetadata: { contentType: 'application/octet-stream' },
    customMetadata: {},
    body: new ReadableStream({
      start(controller) {
        controller.enqueue(new Uint8Array(content))
        controller.close()
      },
    }),
    bodyUsed: false,
    arrayBuffer: () => Promise.resolve(content),
    text: () => Promise.resolve(new TextDecoder().decode(content)),
    json: () => Promise.resolve(JSON.parse(new TextDecoder().decode(content))),
    blob: () => Promise.resolve(new Blob([content])),
    writeHttpMetadata: vi.fn(),
    ...metadata,
  } as R2ObjectBody
}

/**
 * Encode a JWT token (for testing)
 */
function encodeJwt(payload: Record<string, unknown>): string {
  const header = { alg: 'HS256', typ: 'JWT' }
  const encodeBase64Url = (data: string) => {
    const base64 = btoa(data)
    return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
  }
  const headerPart = encodeBase64Url(JSON.stringify(header))
  const payloadPart = encodeBase64Url(JSON.stringify(payload))
  const signature = encodeBase64Url('mock-signature')
  return `${headerPart}.${payloadPart}.${signature}`
}

// =============================================================================
// CORS Handling Tests
// =============================================================================

describe('Public Routes - CORS Handling', () => {
  describe('OPTIONS preflight requests', () => {
    it('should return 204 with CORS headers for /api/public', async () => {
      const request = new Request('https://api.parquedb.com/api/public', {
        method: 'OPTIONS',
      })
      const env = createMockEnv()

      const response = await handlePublicRoutes(request, env, '/api/public', 'https://api.parquedb.com')

      expect(response).not.toBeNull()
      expect(response!.status).toBe(204)
      expect(response!.headers.get('Access-Control-Allow-Origin')).toBe('*')
      expect(response!.headers.get('Access-Control-Allow-Methods')).toBe('GET, HEAD, OPTIONS')
      expect(response!.headers.get('Access-Control-Allow-Headers')).toBe('Content-Type, Range')
      expect(response!.headers.get('Access-Control-Max-Age')).toBe('86400')
    })

    it('should return 204 with CORS headers for /api/db/:owner/:slug', async () => {
      const request = new Request('https://api.parquedb.com/api/db/testuser/mydb', {
        method: 'OPTIONS',
      })
      const env = createMockEnv()

      const response = await handlePublicRoutes(request, env, '/api/db/testuser/mydb', 'https://api.parquedb.com')

      expect(response).not.toBeNull()
      expect(response!.status).toBe(204)
      expect(response!.headers.get('Access-Control-Allow-Origin')).toBe('*')
    })

    it('should return 204 with CORS headers for /db/:owner/:slug/*', async () => {
      const request = new Request('https://api.parquedb.com/db/testuser/mydb/data.parquet', {
        method: 'OPTIONS',
      })
      const env = createMockEnv()

      const response = await handlePublicRoutes(request, env, '/db/testuser/mydb/data.parquet', 'https://api.parquedb.com')

      expect(response).not.toBeNull()
      expect(response!.status).toBe(204)
    })

    it('should not handle OPTIONS for non-public routes', async () => {
      const request = new Request('https://api.parquedb.com/other/path', {
        method: 'OPTIONS',
      })
      const env = createMockEnv()

      const response = await handlePublicRoutes(request, env, '/other/path', 'https://api.parquedb.com')

      expect(response).toBeNull()
    })
  })

  describe('Response CORS headers', () => {
    it('should add public CORS headers to /api/public response', async () => {
      const request = new Request('https://api.parquedb.com/api/public')
      const env = createMockEnv()

      const response = await handlePublicRoutes(request, env, '/api/public', 'https://api.parquedb.com')

      expect(response).not.toBeNull()
      expect(response!.headers.get('Access-Control-Allow-Origin')).toBe('*')
      expect(response!.headers.get('Access-Control-Allow-Methods')).toBe('GET, HEAD, OPTIONS')
      expect(response!.headers.get('Access-Control-Expose-Headers')).toBe('Content-Range, Content-Length, ETag, Accept-Ranges')
    })

    it('should add authenticated CORS headers (with Authorization) to /api/db/:owner/:slug response', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('testuser/mydb', createDatabaseInfo({ owner: 'testuser', slug: 'mydb' }))

      const mockIndex = createMockDatabaseIndex(databases)
      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
      })

      const request = new Request('https://api.parquedb.com/api/db/testuser/mydb')

      const response = await handlePublicRoutes(request, env, '/api/db/testuser/mydb', 'https://api.parquedb.com')

      expect(response).not.toBeNull()
      expect(response!.headers.get('Access-Control-Allow-Headers')).toBe('Content-Type, Range, Authorization')
    })
  })
})

// =============================================================================
// Rate Limiting Tests
// =============================================================================

describe('Public Routes - Rate Limiting', () => {
  it('should return 429 when rate limit is exceeded', async () => {
    const rateLimitResult: RateLimitResult = {
      allowed: false,
      remaining: 0,
      resetAt: Date.now() + 30000,
      limit: 100,
      current: 100,
    }
    const mockRateLimiter = createMockRateLimiter(rateLimitResult)
    const env = createMockEnv({
      RATE_LIMITER: createMockDONamespace(mockRateLimiter),
    })

    const request = new Request('https://api.parquedb.com/api/public')

    const response = await handlePublicRoutes(request, env, '/api/public', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(429)

    const body = await response!.json() as { error: string; message: string }
    expect(body.error).toBe('Too Many Requests')
    expect(body.message).toContain('Rate limit exceeded')
  })

  it('should include rate limit headers when allowed', async () => {
    const rateLimitResult: RateLimitResult = {
      allowed: true,
      remaining: 50,
      resetAt: Date.now() + 60000,
      limit: 100,
      current: 50,
    }
    const mockRateLimiter = createMockRateLimiter(rateLimitResult)
    const env = createMockEnv({
      RATE_LIMITER: createMockDONamespace(mockRateLimiter),
    })

    const request = new Request('https://api.parquedb.com/api/public')

    const response = await handlePublicRoutes(request, env, '/api/public', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.headers.get('X-RateLimit-Limit')).toBe('100')
    expect(response!.headers.get('X-RateLimit-Remaining')).toBe('50')
    expect(response!.headers.get('X-RateLimit-Reset')).toBeDefined()
  })

  it('should proceed without rate limiting when RATE_LIMITER is not configured', async () => {
    const env = createMockEnv()
    const request = new Request('https://api.parquedb.com/api/public')

    const response = await handlePublicRoutes(request, env, '/api/public', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(200)
    // Should not have rate limit headers
    expect(response!.headers.get('X-RateLimit-Limit')).toBeNull()
  })

  it('should proceed when rate limiter throws an error', async () => {
    const mockRateLimiter = {
      checkLimit: vi.fn().mockRejectedValue(new Error('Rate limiter unavailable')),
    }
    const env = createMockEnv({
      RATE_LIMITER: createMockDONamespace(mockRateLimiter),
    })

    // Enable console logger so logger.error calls console.error
    setLogger(consoleLogger)
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    const request = new Request('https://api.parquedb.com/api/public')

    const response = await handlePublicRoutes(request, env, '/api/public', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(200)
    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining('[RateLimit]'), expect.any(Error))

    errorSpy.mockRestore()
    setLogger(noopLogger)
  })

  it('should use correct endpoint type for rate limiting', async () => {
    const mockRateLimiter = createMockRateLimiter()
    const env = createMockEnv({
      RATE_LIMITER: createMockDONamespace(mockRateLimiter),
    })

    // Test /api/public
    await handlePublicRoutes(
      new Request('https://api.parquedb.com/api/public'),
      env,
      '/api/public',
      'https://api.parquedb.com'
    )
    expect(mockRateLimiter.checkLimit).toHaveBeenCalledWith('public')

    // Test /api/db/:owner/:slug
    mockRateLimiter.checkLimit.mockClear()
    const databases = new Map<string, DatabaseInfo>()
    databases.set('testuser/mydb', createDatabaseInfo())
    const mockIndex = createMockDatabaseIndex(databases)
    const envWithIndex = createMockEnv({
      RATE_LIMITER: createMockDONamespace(mockRateLimiter),
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })
    await handlePublicRoutes(
      new Request('https://api.parquedb.com/api/db/testuser/mydb'),
      envWithIndex,
      '/api/db/testuser/mydb',
      'https://api.parquedb.com'
    )
    expect(mockRateLimiter.checkLimit).toHaveBeenCalledWith('database')

    // Test /api/db/:owner/:slug/:collection
    mockRateLimiter.checkLimit.mockClear()
    await handlePublicRoutes(
      new Request('https://api.parquedb.com/api/db/testuser/mydb/posts'),
      envWithIndex,
      '/api/db/testuser/mydb/posts',
      'https://api.parquedb.com'
    )
    expect(mockRateLimiter.checkLimit).toHaveBeenCalledWith('query')

    // Test /db/:owner/:slug/*
    mockRateLimiter.checkLimit.mockClear()
    await handlePublicRoutes(
      new Request('https://api.parquedb.com/db/testuser/mydb/data.parquet'),
      envWithIndex,
      '/db/testuser/mydb/data.parquet',
      'https://api.parquedb.com'
    )
    expect(mockRateLimiter.checkLimit).toHaveBeenCalledWith('file')
  })
})

// =============================================================================
// /api/public - List Public Databases
// =============================================================================

describe('Public Routes - /api/public', () => {
  it('should return empty list with pagination info', async () => {
    const request = new Request('https://api.parquedb.com/api/public')
    const env = createMockEnv()

    const response = await handlePublicRoutes(request, env, '/api/public', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(200)

    const body = await response!.json() as {
      databases: unknown[]
      total: number
      hasMore: boolean
    }
    expect(body.databases).toEqual([])
    expect(body.total).toBe(0)
    expect(body.hasMore).toBe(false)
  })

  it('should respect limit and offset query parameters', async () => {
    const request = new Request('https://api.parquedb.com/api/public?limit=10&offset=5')
    const env = createMockEnv()

    const response = await handlePublicRoutes(request, env, '/api/public', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(200)
  })

  it('should not handle non-GET requests', async () => {
    const request = new Request('https://api.parquedb.com/api/public', {
      method: 'POST',
    })
    const env = createMockEnv()

    const response = await handlePublicRoutes(request, env, '/api/public', 'https://api.parquedb.com')

    // Non-GET is not handled by this endpoint (after rate limiting check)
    expect(response).toBeNull()
  })
})

// =============================================================================
// /api/db/:owner/:slug - Database Metadata
// =============================================================================

describe('Public Routes - /api/db/:owner/:slug', () => {
  it('should return 503 when DATABASE_INDEX is not configured', async () => {
    const request = new Request('https://api.parquedb.com/api/db/testuser/mydb')
    const env = createMockEnv() // No DATABASE_INDEX

    const response = await handlePublicRoutes(request, env, '/api/db/testuser/mydb', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(503)

    const body = await response!.json() as { error: string }
    expect(body.error).toBe('Database index not configured')
  })

  it('should return 404 when database is not found', async () => {
    const mockIndex = createMockDatabaseIndex()
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    const request = new Request('https://api.parquedb.com/api/db/testuser/nonexistent')

    const response = await handlePublicRoutes(request, env, '/api/db/testuser/nonexistent', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(404)

    const body = await response!.json() as { error: string }
    expect(body.error).toBe('Database not found')
  })

  it('should return database metadata for public database', async () => {
    const databases = new Map<string, DatabaseInfo>()
    const dbInfo = createDatabaseInfo({
      id: 'db-456',
      name: 'Public Dataset',
      description: 'A public dataset',
      owner: 'testuser',
      slug: 'public-ds',
      visibility: 'public',
      collectionCount: 3,
      entityCount: 500,
    })
    databases.set('testuser/public-ds', dbInfo)

    const mockIndex = createMockDatabaseIndex(databases)
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    const request = new Request('https://api.parquedb.com/api/db/testuser/public-ds')

    const response = await handlePublicRoutes(request, env, '/api/db/testuser/public-ds', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(200)

    const body = await response!.json() as Partial<DatabaseInfo>
    expect(body.id).toBe('db-456')
    expect(body.name).toBe('Public Dataset')
    expect(body.description).toBe('A public dataset')
    expect(body.owner).toBe('testuser')
    expect(body.slug).toBe('public-ds')
    expect(body.visibility).toBe('public')
    expect(body.collectionCount).toBe(3)
    expect(body.entityCount).toBe(500)
  })

  it('should return database metadata for unlisted database', async () => {
    const databases = new Map<string, DatabaseInfo>()
    databases.set('testuser/unlisted-db', createDatabaseInfo({
      owner: 'testuser',
      slug: 'unlisted-db',
      visibility: 'unlisted',
    }))

    const mockIndex = createMockDatabaseIndex(databases)
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    const request = new Request('https://api.parquedb.com/api/db/testuser/unlisted-db')

    const response = await handlePublicRoutes(request, env, '/api/db/testuser/unlisted-db', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(200)
  })

  it('should return 401 for private database without auth', async () => {
    const databases = new Map<string, DatabaseInfo>()
    databases.set('testuser/private-db', createDatabaseInfo({
      owner: 'testuser',
      slug: 'private-db',
      visibility: 'private',
    }))

    const mockIndex = createMockDatabaseIndex(databases)
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    const request = new Request('https://api.parquedb.com/api/db/testuser/private-db')

    const response = await handlePublicRoutes(request, env, '/api/db/testuser/private-db', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(401)

    const body = await response!.json() as { error: string }
    expect(body.error).toBe('Authentication required')
  })

  it('should allow owner to access private database', async () => {
    const databases = new Map<string, DatabaseInfo>()
    databases.set('testuser/private-db', createDatabaseInfo({
      owner: 'testuser',
      slug: 'private-db',
      visibility: 'private',
    }))

    const mockIndex = createMockDatabaseIndex(databases)
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    // Create a JWT with the owner's identity
    const token = encodeJwt({ sub: 'testuser' })
    const request = new Request('https://api.parquedb.com/api/db/testuser/private-db', {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    })

    const response = await handlePublicRoutes(request, env, '/api/db/testuser/private-db', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(200)
  })

  it('should handle errors gracefully', async () => {
    const mockIndex = {
      getBySlug: vi.fn().mockRejectedValue(new Error('Database error')),
    }
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    const request = new Request('https://api.parquedb.com/api/db/testuser/mydb')

    const response = await handlePublicRoutes(request, env, '/api/db/testuser/mydb', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(500)

    const body = await response!.json() as { error: string }
    expect(body.error).toBe('Database error')
  })
})

// =============================================================================
// /api/db/:owner/:slug/:collection - Collection Query
// =============================================================================

describe('Public Routes - /api/db/:owner/:slug/:collection', () => {
  it('should return 503 when DATABASE_INDEX is not configured', async () => {
    const request = new Request('https://api.parquedb.com/api/db/testuser/mydb/posts')
    const env = createMockEnv()

    const response = await handlePublicRoutes(request, env, '/api/db/testuser/mydb/posts', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(503)
  })

  it('should return 404 when database is not found', async () => {
    const mockIndex = createMockDatabaseIndex()
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    const request = new Request('https://api.parquedb.com/api/db/testuser/nonexistent/posts')

    const response = await handlePublicRoutes(request, env, '/api/db/testuser/nonexistent/posts', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(404)
  })

  it('should return 401 for private database collection without auth', async () => {
    const databases = new Map<string, DatabaseInfo>()
    databases.set('testuser/private-db', createDatabaseInfo({
      owner: 'testuser',
      slug: 'private-db',
      visibility: 'private',
    }))

    const mockIndex = createMockDatabaseIndex(databases)
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    const request = new Request('https://api.parquedb.com/api/db/testuser/private-db/posts')

    const response = await handlePublicRoutes(request, env, '/api/db/testuser/private-db/posts', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(401)
  })

  it('should return collection data for public database', async () => {
    const databases = new Map<string, DatabaseInfo>()
    databases.set('testuser/public-db', createDatabaseInfo({
      owner: 'testuser',
      slug: 'public-db',
      visibility: 'public',
    }))

    const mockIndex = createMockDatabaseIndex(databases)
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    const request = new Request('https://api.parquedb.com/api/db/testuser/public-db/posts')

    const response = await handlePublicRoutes(request, env, '/api/db/testuser/public-db/posts', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(200)

    const body = await response!.json() as {
      items: unknown[]
      total: number
      hasMore: boolean
      collection: string
      database: string
    }
    expect(body.items).toEqual([])
    expect(body.total).toBe(0)
    expect(body.hasMore).toBe(false)
    expect(body.collection).toBe('posts')
    expect(body.database).toBe('testuser/public-db')
  })

  it('should parse query parameters', async () => {
    const databases = new Map<string, DatabaseInfo>()
    databases.set('testuser/public-db', createDatabaseInfo({
      owner: 'testuser',
      slug: 'public-db',
      visibility: 'public',
    }))

    const mockIndex = createMockDatabaseIndex(databases)
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    const request = new Request('https://api.parquedb.com/api/db/testuser/public-db/posts?filter={"status":"published"}&limit=50&offset=10')

    const response = await handlePublicRoutes(request, env, '/api/db/testuser/public-db/posts', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(200)
  })

  it('should handle errors gracefully', async () => {
    const mockIndex = {
      getBySlug: vi.fn().mockRejectedValue(new Error('Query failed')),
    }
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    const request = new Request('https://api.parquedb.com/api/db/testuser/mydb/posts')

    const response = await handlePublicRoutes(request, env, '/api/db/testuser/mydb/posts', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(500)
  })
})

// =============================================================================
// /db/:owner/:slug/* - Raw File Access
// =============================================================================

describe('Public Routes - /db/:owner/:slug/* (Raw File Access)', () => {
  it('should return 503 when DATABASE_INDEX is not configured', async () => {
    const request = new Request('https://api.parquedb.com/db/testuser/mydb/data.parquet')
    const env = createMockEnv()

    const response = await handlePublicRoutes(request, env, '/db/testuser/mydb/data.parquet', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(503)
  })

  it('should return 404 when database is not found', async () => {
    const mockIndex = createMockDatabaseIndex()
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    const request = new Request('https://api.parquedb.com/db/testuser/nonexistent/data.parquet')

    const response = await handlePublicRoutes(request, env, '/db/testuser/nonexistent/data.parquet', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(404)
  })

  it('should return 401 for private database without auth', async () => {
    const databases = new Map<string, DatabaseInfo>()
    databases.set('testuser/private-db', createDatabaseInfo({
      owner: 'testuser',
      slug: 'private-db',
      visibility: 'private',
    }))

    const mockIndex = createMockDatabaseIndex(databases)
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    const request = new Request('https://api.parquedb.com/db/testuser/private-db/data.parquet')

    const response = await handlePublicRoutes(request, env, '/db/testuser/private-db/data.parquet', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(401)
  })

  it('should return 404 when file is not found', async () => {
    const databases = new Map<string, DatabaseInfo>()
    databases.set('testuser/public-db', createDatabaseInfo({
      owner: 'testuser',
      slug: 'public-db',
      visibility: 'public',
      prefix: 'mydata',
    }))

    const mockIndex = createMockDatabaseIndex(databases)
    const mockBucket = createMockBucket({
      get: vi.fn().mockResolvedValue(null),
      head: vi.fn().mockResolvedValue(null),
    })

    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
      BUCKET: mockBucket,
    })

    const request = new Request('https://api.parquedb.com/db/testuser/public-db/nonexistent.parquet')

    const response = await handlePublicRoutes(request, env, '/db/testuser/public-db/nonexistent.parquet', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(404)
    expect(mockBucket.get).toHaveBeenCalledWith('mydata/nonexistent.parquet')
  })

  it('should return file content for public database', async () => {
    const databases = new Map<string, DatabaseInfo>()
    databases.set('testuser/public-db', createDatabaseInfo({
      owner: 'testuser',
      slug: 'public-db',
      visibility: 'public',
      prefix: 'data',
    }))

    const fileContent = new TextEncoder().encode('PAR1...')
    const mockObject = createMockR2Object(fileContent.buffer as ArrayBuffer, {
      size: fileContent.byteLength,
    })

    const mockIndex = createMockDatabaseIndex(databases)
    const mockBucket = createMockBucket({
      get: vi.fn().mockResolvedValue(mockObject),
    })

    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
      BUCKET: mockBucket,
    })

    const request = new Request('https://api.parquedb.com/db/testuser/public-db/entities.parquet')

    const response = await handlePublicRoutes(request, env, '/db/testuser/public-db/entities.parquet', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(200)
    expect(response!.headers.get('ETag')).toBe('"mock-etag"')
    expect(response!.headers.get('Accept-Ranges')).toBe('bytes')
    expect(mockBucket.get).toHaveBeenCalledWith('data/entities.parquet')
  })

  it('should handle file without prefix', async () => {
    const databases = new Map<string, DatabaseInfo>()
    databases.set('testuser/public-db', createDatabaseInfo({
      owner: 'testuser',
      slug: 'public-db',
      visibility: 'public',
      prefix: undefined, // No prefix
    }))

    const fileContent = new TextEncoder().encode('data')
    const mockObject = createMockR2Object(fileContent.buffer as ArrayBuffer)

    const mockIndex = createMockDatabaseIndex(databases)
    const mockBucket = createMockBucket({
      get: vi.fn().mockResolvedValue(mockObject),
    })

    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
      BUCKET: mockBucket,
    })

    const request = new Request('https://api.parquedb.com/db/testuser/public-db/entities.parquet')

    await handlePublicRoutes(request, env, '/db/testuser/public-db/entities.parquet', 'https://api.parquedb.com')

    // Without prefix, the path should be used directly
    expect(mockBucket.get).toHaveBeenCalledWith('entities.parquet')
  })

  describe('HEAD requests', () => {
    it('should handle HEAD requests', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('testuser/public-db', createDatabaseInfo({
        owner: 'testuser',
        slug: 'public-db',
        visibility: 'public',
      }))

      const mockObject = {
        key: 'entities.parquet',
        size: 1000,
        etag: '"etag-123"',
        httpMetadata: { contentType: 'application/octet-stream' },
      }

      const mockIndex = createMockDatabaseIndex(databases)
      const mockBucket = createMockBucket({
        head: vi.fn().mockResolvedValue(mockObject),
      })

      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
        BUCKET: mockBucket,
      })

      const request = new Request('https://api.parquedb.com/db/testuser/public-db/entities.parquet', {
        method: 'HEAD',
      })

      const response = await handlePublicRoutes(request, env, '/db/testuser/public-db/entities.parquet', 'https://api.parquedb.com')

      expect(response).not.toBeNull()
      expect(response!.status).toBe(200)
      expect(response!.body).toBeNull()
      expect(mockBucket.head).toHaveBeenCalled()
    })
  })

  describe('Range requests', () => {
    it('should handle full range request (bytes=start-end)', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('testuser/public-db', createDatabaseInfo({
        owner: 'testuser',
        slug: 'public-db',
        visibility: 'public',
      }))

      const fullContent = new Uint8Array(1000)
      fullContent.fill(65) // Fill with 'A'

      const rangeContent = fullContent.slice(100, 201) // bytes 100-200

      const mockObject = {
        ...createMockR2Object(rangeContent.buffer as ArrayBuffer),
        size: 1000,
        range: { offset: 100, length: 101 },
      }

      const mockIndex = createMockDatabaseIndex(databases)
      const mockBucket = createMockBucket({
        get: vi.fn().mockResolvedValue(mockObject),
      })

      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
        BUCKET: mockBucket,
      })

      const request = new Request('https://api.parquedb.com/db/testuser/public-db/data.parquet', {
        headers: {
          Range: 'bytes=100-200',
        },
      })

      const response = await handlePublicRoutes(request, env, '/db/testuser/public-db/data.parquet', 'https://api.parquedb.com')

      expect(response).not.toBeNull()
      expect(response!.status).toBe(206)
      expect(response!.headers.get('Content-Range')).toBe('bytes 100-200/1000')
      expect(response!.headers.get('Content-Length')).toBe('101')

      // Verify the range option was passed to bucket.get
      expect(mockBucket.get).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ range: { offset: 100, length: 101 } })
      )
    })

    it('should handle suffix range request (bytes=-suffix)', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('testuser/public-db', createDatabaseInfo({
        owner: 'testuser',
        slug: 'public-db',
        visibility: 'public',
      }))

      const mockObject = createMockR2Object(new ArrayBuffer(100))
      const mockIndex = createMockDatabaseIndex(databases)
      const mockBucket = createMockBucket({
        get: vi.fn().mockResolvedValue(mockObject),
      })

      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
        BUCKET: mockBucket,
      })

      const request = new Request('https://api.parquedb.com/db/testuser/public-db/data.parquet', {
        headers: {
          Range: 'bytes=-100',
        },
      })

      await handlePublicRoutes(request, env, '/db/testuser/public-db/data.parquet', 'https://api.parquedb.com')

      expect(mockBucket.get).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ range: { suffix: 100 } })
      )
    })

    it('should handle open-ended range request (bytes=start-)', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('testuser/public-db', createDatabaseInfo({
        owner: 'testuser',
        slug: 'public-db',
        visibility: 'public',
      }))

      const mockObject = createMockR2Object(new ArrayBuffer(100))
      const mockIndex = createMockDatabaseIndex(databases)
      const mockBucket = createMockBucket({
        get: vi.fn().mockResolvedValue(mockObject),
      })

      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
        BUCKET: mockBucket,
      })

      const request = new Request('https://api.parquedb.com/db/testuser/public-db/data.parquet', {
        headers: {
          Range: 'bytes=500-',
        },
      })

      await handlePublicRoutes(request, env, '/db/testuser/public-db/data.parquet', 'https://api.parquedb.com')

      expect(mockBucket.get).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ range: { offset: 500 } })
      )
    })

    it('should ignore invalid Range header', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('testuser/public-db', createDatabaseInfo({
        owner: 'testuser',
        slug: 'public-db',
        visibility: 'public',
      }))

      const mockObject = createMockR2Object(new ArrayBuffer(100))
      const mockIndex = createMockDatabaseIndex(databases)
      const mockBucket = createMockBucket({
        get: vi.fn().mockResolvedValue(mockObject),
      })

      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
        BUCKET: mockBucket,
      })

      const request = new Request('https://api.parquedb.com/db/testuser/public-db/data.parquet', {
        headers: {
          Range: 'invalid-range',
        },
      })

      const response = await handlePublicRoutes(request, env, '/db/testuser/public-db/data.parquet', 'https://api.parquedb.com')

      expect(response).not.toBeNull()
      expect(response!.status).toBe(200) // Should return full file
      expect(mockBucket.get).toHaveBeenCalledWith(expect.any(String)) // No range option
    })
  })

  it('should handle errors gracefully', async () => {
    const databases = new Map<string, DatabaseInfo>()
    databases.set('testuser/public-db', createDatabaseInfo({
      owner: 'testuser',
      slug: 'public-db',
      visibility: 'public',
    }))

    const mockIndex = createMockDatabaseIndex(databases)
    const mockBucket = createMockBucket({
      get: vi.fn().mockRejectedValue(new Error('R2 error')),
    })

    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
      BUCKET: mockBucket,
    })

    // Suppress console.error for this test
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    const request = new Request('https://api.parquedb.com/db/testuser/public-db/data.parquet')

    const response = await handlePublicRoutes(request, env, '/db/testuser/public-db/data.parquet', 'https://api.parquedb.com')

    expect(response).not.toBeNull()
    expect(response!.status).toBe(500)

    errorSpy.mockRestore()
  })
})

// =============================================================================
// JWT Token and Ownership Verification
// =============================================================================

describe('Public Routes - JWT Token Verification', () => {
  describe('Token extraction', () => {
    it('should extract Bearer token from Authorization header', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('testuser/private-db', createDatabaseInfo({
        owner: 'testuser',
        slug: 'private-db',
        visibility: 'private',
      }))

      const mockIndex = createMockDatabaseIndex(databases)
      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
      })

      // Valid token with matching owner
      const token = encodeJwt({ sub: 'testuser' })
      const request = new Request('https://api.parquedb.com/api/db/testuser/private-db', {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      })

      const response = await handlePublicRoutes(request, env, '/api/db/testuser/private-db', 'https://api.parquedb.com')

      expect(response!.status).toBe(200)
    })

    it('should reject non-Bearer authorization', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('testuser/private-db', createDatabaseInfo({
        owner: 'testuser',
        slug: 'private-db',
        visibility: 'private',
      }))

      const mockIndex = createMockDatabaseIndex(databases)
      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
      })

      const request = new Request('https://api.parquedb.com/api/db/testuser/private-db', {
        headers: {
          Authorization: 'Basic dXNlcjpwYXNz',
        },
      })

      const response = await handlePublicRoutes(request, env, '/api/db/testuser/private-db', 'https://api.parquedb.com')

      expect(response!.status).toBe(401)
    })
  })

  describe('JWT payload decoding', () => {
    it('should decode standard JWT with sub claim', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('alice/private-db', createDatabaseInfo({
        owner: 'alice',
        slug: 'private-db',
        visibility: 'private',
      }))

      const mockIndex = createMockDatabaseIndex(databases)
      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
      })

      const token = encodeJwt({ sub: 'alice' })
      const request = new Request('https://api.parquedb.com/api/db/alice/private-db', {
        headers: { Authorization: `Bearer ${token}` },
      })

      const response = await handlePublicRoutes(request, env, '/api/db/alice/private-db', 'https://api.parquedb.com')

      expect(response!.status).toBe(200)
    })

    it('should decode JWT with username claim', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('bob/private-db', createDatabaseInfo({
        owner: 'bob',
        slug: 'private-db',
        visibility: 'private',
      }))

      const mockIndex = createMockDatabaseIndex(databases)
      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
      })

      const token = encodeJwt({ username: 'bob' })
      const request = new Request('https://api.parquedb.com/api/db/bob/private-db', {
        headers: { Authorization: `Bearer ${token}` },
      })

      const response = await handlePublicRoutes(request, env, '/api/db/bob/private-db', 'https://api.parquedb.com')

      expect(response!.status).toBe(200)
    })

    it('should decode JWT with preferred_username claim (OIDC)', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('carol/private-db', createDatabaseInfo({
        owner: 'carol',
        slug: 'private-db',
        visibility: 'private',
      }))

      const mockIndex = createMockDatabaseIndex(databases)
      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
      })

      const token = encodeJwt({ preferred_username: 'carol' })
      const request = new Request('https://api.parquedb.com/api/db/carol/private-db', {
        headers: { Authorization: `Bearer ${token}` },
      })

      const response = await handlePublicRoutes(request, env, '/api/db/carol/private-db', 'https://api.parquedb.com')

      expect(response!.status).toBe(200)
    })

    it('should handle case-insensitive owner comparison', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('TestUser/private-db', createDatabaseInfo({
        owner: 'TestUser',
        slug: 'private-db',
        visibility: 'private',
      }))

      const mockIndex = createMockDatabaseIndex(databases)
      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
      })

      // Token has lowercase, owner is mixed case
      const token = encodeJwt({ sub: 'testuser' })
      const request = new Request('https://api.parquedb.com/api/db/TestUser/private-db', {
        headers: { Authorization: `Bearer ${token}` },
      })

      const response = await handlePublicRoutes(request, env, '/api/db/TestUser/private-db', 'https://api.parquedb.com')

      expect(response!.status).toBe(200)
    })

    it('should reject invalid JWT format', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('testuser/private-db', createDatabaseInfo({
        owner: 'testuser',
        slug: 'private-db',
        visibility: 'private',
      }))

      const mockIndex = createMockDatabaseIndex(databases)
      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
      })

      const request = new Request('https://api.parquedb.com/api/db/testuser/private-db', {
        headers: { Authorization: 'Bearer invalid.jwt' },
      })

      const response = await handlePublicRoutes(request, env, '/api/db/testuser/private-db', 'https://api.parquedb.com')

      expect(response!.status).toBe(401)
    })

    it('should reject JWT with no matching identity claim', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('testuser/private-db', createDatabaseInfo({
        owner: 'testuser',
        slug: 'private-db',
        visibility: 'private',
      }))

      const mockIndex = createMockDatabaseIndex(databases)
      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
      })

      // Token has different user
      const token = encodeJwt({ sub: 'otheruser' })
      const request = new Request('https://api.parquedb.com/api/db/testuser/private-db', {
        headers: { Authorization: `Bearer ${token}` },
      })

      const response = await handlePublicRoutes(request, env, '/api/db/testuser/private-db', 'https://api.parquedb.com')

      expect(response!.status).toBe(401)
    })

    it('should handle JWT with invalid base64 payload', async () => {
      const databases = new Map<string, DatabaseInfo>()
      databases.set('testuser/private-db', createDatabaseInfo({
        owner: 'testuser',
        slug: 'private-db',
        visibility: 'private',
      }))

      const mockIndex = createMockDatabaseIndex(databases)
      const env = createMockEnv({
        DATABASE_INDEX: createMockDONamespace(mockIndex),
      })

      // Create a token with invalid base64 in payload
      const request = new Request('https://api.parquedb.com/api/db/testuser/private-db', {
        headers: { Authorization: 'Bearer eyJhbGciOiJIUzI1NiJ9.!!!invalid!!!.signature' },
      })

      const response = await handlePublicRoutes(request, env, '/api/db/testuser/private-db', 'https://api.parquedb.com')

      expect(response!.status).toBe(401)
    })
  })
})

// =============================================================================
// Non-Matching Routes
// =============================================================================

describe('Public Routes - Non-Matching Routes', () => {
  it('should return null for unmatched routes', async () => {
    const env = createMockEnv()

    const testCases = [
      '/api/other',
      '/other/path',
      '/api/v2/public',
      '/databases/testuser/mydb',
    ]

    for (const path of testCases) {
      const request = new Request(`https://api.parquedb.com${path}`)
      const response = await handlePublicRoutes(request, env, path, 'https://api.parquedb.com')
      expect(response).toBeNull()
    }
  })

  it('should not handle POST/PUT/DELETE for database routes', async () => {
    const databases = new Map<string, DatabaseInfo>()
    databases.set('testuser/public-db', createDatabaseInfo({
      owner: 'testuser',
      slug: 'public-db',
      visibility: 'public',
    }))

    const mockIndex = createMockDatabaseIndex(databases)
    const env = createMockEnv({
      DATABASE_INDEX: createMockDONamespace(mockIndex),
    })

    // POST to /api/db/:owner/:slug should not be handled
    const postRequest = new Request('https://api.parquedb.com/api/db/testuser/public-db', {
      method: 'POST',
    })
    const postResponse = await handlePublicRoutes(postRequest, env, '/api/db/testuser/public-db', 'https://api.parquedb.com')
    expect(postResponse).toBeNull()

    // DELETE to /db/:owner/:slug/* should not be handled
    const deleteRequest = new Request('https://api.parquedb.com/db/testuser/public-db/file.parquet', {
      method: 'DELETE',
    })
    const deleteResponse = await handlePublicRoutes(deleteRequest, env, '/db/testuser/public-db/file.parquet', 'https://api.parquedb.com')
    expect(deleteResponse).toBeNull()
  })
})
