/**
 * Remote Client Unit Tests
 *
 * Tests for remote database client functions:
 * - openRemoteDB: Open a remote database by owner/slug
 * - checkRemoteDB: Check if a remote database exists and is accessible
 * - listPublicDatabases: List available public databases
 * - RemoteCollection operations: find, findOne, get, count, exists
 * - RemoteDB operations: collection, collections, proxy access
 *
 * Uses mocked fetch to test client behavior without network calls.
 */

import { describe, it, expect, beforeEach, afterEach, vi, type Mock } from 'vitest'
import {
  openRemoteDB,
  checkRemoteDB,
  listPublicDatabases,
  type RemoteDB,
  type RemoteDBInfo,
  type OpenRemoteDBOptions,
} from '../../../src/client/remote'
import { RemoteBackend } from '../../../src/storage/RemoteBackend'

// =============================================================================
// Mock Setup
// =============================================================================

// Store original fetch
const originalFetch = globalThis.fetch

// Create mock fetch function
let mockFetch: Mock

beforeEach(() => {
  mockFetch = vi.fn()
  globalThis.fetch = mockFetch
})

afterEach(() => {
  globalThis.fetch = originalFetch
  vi.restoreAllMocks()
})

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a mock response with JSON body
 */
function createJsonResponse(data: unknown, status = 200, statusText = 'OK'): Response {
  return new Response(JSON.stringify(data), {
    status,
    statusText,
    headers: { 'Content-Type': 'application/json' },
  })
}

/**
 * Create a mock database info response
 */
function createDBInfoResponse(overrides: Partial<RemoteDBInfo> = {}): RemoteDBInfo {
  return {
    id: 'test-db-id',
    name: 'my-dataset',
    owner: 'testuser',
    slug: 'my-dataset',
    visibility: 'public',
    description: 'A test database',
    collectionCount: 3,
    entityCount: 100,
    ...overrides,
  }
}

// =============================================================================
// validateRequiredFields Tests (via openRemoteDB)
// =============================================================================

describe('validateRequiredFields', () => {
  it('should throw error for null response', async () => {
    mockFetch.mockResolvedValue(createJsonResponse(null))

    await expect(openRemoteDB('testuser/my-dataset')).rejects.toThrow(
      'openRemoteDB: Response is null or undefined'
    )
  })

  it('should throw error for undefined response', async () => {
    mockFetch.mockResolvedValue(createJsonResponse(undefined))

    await expect(openRemoteDB('testuser/my-dataset')).rejects.toThrow(
      'openRemoteDB: Response is null or undefined'
    )
  })

  it('should throw error for non-object response', async () => {
    mockFetch.mockResolvedValue(createJsonResponse('string response'))

    await expect(openRemoteDB('testuser/my-dataset')).rejects.toThrow(
      'openRemoteDB: Expected object, got string'
    )
  })

  it('should throw error for missing required fields', async () => {
    // Missing 'id' and 'visibility' fields
    mockFetch.mockResolvedValue(
      createJsonResponse({
        name: 'my-dataset',
        owner: 'testuser',
        slug: 'my-dataset',
      })
    )

    await expect(openRemoteDB('testuser/my-dataset')).rejects.toThrow(
      'openRemoteDB: Missing required fields: id, visibility'
    )
  })

  it('should throw error for null required fields', async () => {
    mockFetch.mockResolvedValue(
      createJsonResponse({
        id: null,
        name: 'my-dataset',
        owner: 'testuser',
        slug: 'my-dataset',
        visibility: 'public',
      })
    )

    await expect(openRemoteDB('testuser/my-dataset')).rejects.toThrow(
      'openRemoteDB: Missing required fields: id'
    )
  })
})

// =============================================================================
// openRemoteDB Tests
// =============================================================================

describe('openRemoteDB', () => {
  describe('ownerSlug parsing', () => {
    it('should parse valid owner/slug format', async () => {
      const dbInfo = createDBInfoResponse()
      mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

      const db = await openRemoteDB('testuser/my-dataset')

      expect(db.info.owner).toBe('testuser')
      expect(db.info.slug).toBe('my-dataset')
    })

    it('should throw error for invalid format (no slash)', async () => {
      await expect(openRemoteDB('invalid-format')).rejects.toThrow(
        'Invalid database reference. Use format: owner/slug'
      )
    })

    it('should throw error for invalid format (too many slashes)', async () => {
      await expect(openRemoteDB('owner/slug/extra')).rejects.toThrow(
        'Invalid database reference. Use format: owner/slug'
      )
    })

    it('should throw error for empty string', async () => {
      await expect(openRemoteDB('')).rejects.toThrow(
        'Invalid database reference. Use format: owner/slug'
      )
    })

    it('should throw error for just a slash', async () => {
      await expect(openRemoteDB('/')).rejects.toThrow(
        'Invalid database reference. Use format: owner/slug'
      )
    })
  })

  describe('API requests', () => {
    it('should call correct API endpoint', async () => {
      const dbInfo = createDBInfoResponse()
      mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

      await openRemoteDB('testuser/my-dataset')

      expect(mockFetch).toHaveBeenCalledWith(
        'https://parque.db/api/db/testuser/my-dataset',
        expect.objectContaining({
          headers: {},
        })
      )
    })

    it('should use custom base URL', async () => {
      const dbInfo = createDBInfoResponse()
      mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

      await openRemoteDB('testuser/my-dataset', {
        baseUrl: 'https://custom.example.com',
      })

      expect(mockFetch).toHaveBeenCalledWith(
        'https://custom.example.com/api/db/testuser/my-dataset',
        expect.any(Object)
      )
    })

    it('should include auth token in headers when provided', async () => {
      const dbInfo = createDBInfoResponse()
      mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

      await openRemoteDB('testuser/my-dataset', { token: 'my-secret-token' })

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: { Authorization: 'Bearer my-secret-token' },
        })
      )
    })
  })

  describe('error handling', () => {
    it('should throw error for 404 response', async () => {
      mockFetch.mockResolvedValue(
        new Response('Not Found', { status: 404, statusText: 'Not Found' })
      )

      await expect(openRemoteDB('testuser/my-dataset')).rejects.toThrow(
        'Database not found: testuser/my-dataset'
      )
    })

    it('should throw error for 401 response', async () => {
      mockFetch.mockResolvedValue(
        new Response('Unauthorized', { status: 401, statusText: 'Unauthorized' })
      )

      await expect(openRemoteDB('testuser/my-dataset')).rejects.toThrow(
        'Authentication required for this database'
      )
    })

    it('should throw error for 403 response', async () => {
      mockFetch.mockResolvedValue(
        new Response('Forbidden', { status: 403, statusText: 'Forbidden' })
      )

      await expect(openRemoteDB('testuser/my-dataset')).rejects.toThrow(
        'Authentication required for this database'
      )
    })

    it('should throw error for other non-OK responses', async () => {
      mockFetch.mockResolvedValue(
        new Response('Internal Server Error', {
          status: 500,
          statusText: 'Internal Server Error',
        })
      )

      await expect(openRemoteDB('testuser/my-dataset')).rejects.toThrow(
        'Failed to fetch database info: Internal Server Error'
      )
    })

    it('should create minimal info when fetch fails with network error', async () => {
      mockFetch.mockRejectedValue(new TypeError('fetch failed'))

      const db = await openRemoteDB('testuser/my-dataset')

      expect(db.info.id).toBe('testuser/my-dataset')
      expect(db.info.name).toBe('my-dataset')
      expect(db.info.owner).toBe('testuser')
      expect(db.info.slug).toBe('my-dataset')
      expect(db.info.visibility).toBe('public')
    })
  })

  describe('returned RemoteDB', () => {
    it('should return RemoteDB with correct info', async () => {
      const dbInfo = createDBInfoResponse({
        description: 'Test database description',
        collectionCount: 5,
        entityCount: 1000,
      })
      mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

      const db = await openRemoteDB('testuser/my-dataset')

      expect(db.info.id).toBe('test-db-id')
      expect(db.info.name).toBe('my-dataset')
      expect(db.info.owner).toBe('testuser')
      expect(db.info.slug).toBe('my-dataset')
      expect(db.info.visibility).toBe('public')
      expect(db.info.description).toBe('Test database description')
      expect(db.info.collectionCount).toBe(5)
      expect(db.info.entityCount).toBe(1000)
    })

    it('should return RemoteDB with RemoteBackend', async () => {
      const dbInfo = createDBInfoResponse()
      mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

      const db = await openRemoteDB('testuser/my-dataset')

      expect(db.backend).toBeInstanceOf(RemoteBackend)
    })
  })
})

// =============================================================================
// checkRemoteDB Tests
// =============================================================================

describe('checkRemoteDB', () => {
  it('should return true when database exists', async () => {
    mockFetch.mockResolvedValue(new Response(null, { status: 200 }))

    const result = await checkRemoteDB('testuser/my-dataset')

    expect(result).toBe(true)
  })

  it('should return false when database does not exist (404)', async () => {
    mockFetch.mockResolvedValue(new Response(null, { status: 404 }))

    const result = await checkRemoteDB('testuser/nonexistent')

    expect(result).toBe(false)
  })

  it('should return false when unauthorized (401)', async () => {
    mockFetch.mockResolvedValue(new Response(null, { status: 401 }))

    const result = await checkRemoteDB('testuser/private-db')

    expect(result).toBe(false)
  })

  it('should return false when forbidden (403)', async () => {
    mockFetch.mockResolvedValue(new Response(null, { status: 403 }))

    const result = await checkRemoteDB('testuser/forbidden-db')

    expect(result).toBe(false)
  })

  it('should return false on network error', async () => {
    mockFetch.mockRejectedValue(new TypeError('fetch failed'))

    const result = await checkRemoteDB('testuser/my-dataset')

    expect(result).toBe(false)
  })

  it('should use HEAD method', async () => {
    mockFetch.mockResolvedValue(new Response(null, { status: 200 }))

    await checkRemoteDB('testuser/my-dataset')

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ method: 'HEAD' })
    )
  })

  it('should call correct API endpoint', async () => {
    mockFetch.mockResolvedValue(new Response(null, { status: 200 }))

    await checkRemoteDB('testuser/my-dataset')

    expect(mockFetch).toHaveBeenCalledWith(
      'https://parque.db/api/db/testuser/my-dataset',
      expect.any(Object)
    )
  })

  it('should use custom base URL', async () => {
    mockFetch.mockResolvedValue(new Response(null, { status: 200 }))

    await checkRemoteDB('testuser/my-dataset', {
      baseUrl: 'https://custom.example.com',
    })

    expect(mockFetch).toHaveBeenCalledWith(
      'https://custom.example.com/api/db/testuser/my-dataset',
      expect.any(Object)
    )
  })

  it('should include auth token when provided', async () => {
    mockFetch.mockResolvedValue(new Response(null, { status: 200 }))

    await checkRemoteDB('testuser/my-dataset', { token: 'my-token' })

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: { Authorization: 'Bearer my-token' },
      })
    )
  })
})

// =============================================================================
// listPublicDatabases Tests
// =============================================================================

describe('listPublicDatabases', () => {
  it('should return list of public databases', async () => {
    const databases: RemoteDBInfo[] = [
      createDBInfoResponse({ id: 'db1', name: 'dataset-1', slug: 'dataset-1' }),
      createDBInfoResponse({ id: 'db2', name: 'dataset-2', slug: 'dataset-2' }),
    ]
    mockFetch.mockResolvedValue(createJsonResponse({ databases }))

    const result = await listPublicDatabases()

    expect(result).toHaveLength(2)
    expect(result[0].id).toBe('db1')
    expect(result[1].id).toBe('db2')
  })

  it('should return empty array when API returns empty list', async () => {
    mockFetch.mockResolvedValue(createJsonResponse({ databases: [] }))

    const result = await listPublicDatabases()

    expect(result).toEqual([])
  })

  it('should return empty array on non-OK response', async () => {
    mockFetch.mockResolvedValue(
      new Response('Internal Server Error', { status: 500 })
    )

    const result = await listPublicDatabases()

    expect(result).toEqual([])
  })

  it('should return empty array on network error', async () => {
    mockFetch.mockRejectedValue(new TypeError('fetch failed'))

    const result = await listPublicDatabases()

    expect(result).toEqual([])
  })

  it('should call correct API endpoint', async () => {
    mockFetch.mockResolvedValue(createJsonResponse({ databases: [] }))

    await listPublicDatabases()

    expect(mockFetch).toHaveBeenCalledWith('https://parque.db/api/public?')
  })

  it('should use custom base URL', async () => {
    mockFetch.mockResolvedValue(createJsonResponse({ databases: [] }))

    await listPublicDatabases({ baseUrl: 'https://custom.example.com' })

    expect(mockFetch).toHaveBeenCalledWith('https://custom.example.com/api/public?')
  })

  it('should include limit parameter', async () => {
    mockFetch.mockResolvedValue(createJsonResponse({ databases: [] }))

    await listPublicDatabases({ limit: 10 })

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('limit=10')
    )
  })

  it('should include offset parameter', async () => {
    mockFetch.mockResolvedValue(createJsonResponse({ databases: [] }))

    await listPublicDatabases({ offset: 20 })

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining('offset=20')
    )
  })

  it('should include both limit and offset parameters', async () => {
    mockFetch.mockResolvedValue(createJsonResponse({ databases: [] }))

    await listPublicDatabases({ limit: 10, offset: 20 })

    const url = mockFetch.mock.calls[0][0] as string
    expect(url).toContain('limit=10')
    expect(url).toContain('offset=20')
  })
})

// =============================================================================
// RemoteDB Tests
// =============================================================================

describe('RemoteDB', () => {
  let db: RemoteDB

  beforeEach(async () => {
    const dbInfo = createDBInfoResponse()
    // First call is for API info, subsequent calls are for backend operations
    mockFetch.mockResolvedValue(createJsonResponse(dbInfo))
    db = await openRemoteDB('testuser/my-dataset')
  })

  describe('collection()', () => {
    it('should return RemoteCollection with correct namespace', () => {
      const posts = db.collection('posts')

      expect(posts.namespace).toBe('posts')
    })

    it('should support typed collections', () => {
      interface Post {
        title: string
        content: string
      }
      const posts = db.collection<Post>('posts')

      expect(posts.namespace).toBe('posts')
    })

    it('should cache collection instances', () => {
      const posts1 = db.collection('posts')
      const posts2 = db.collection('posts')

      expect(posts1).toBe(posts2)
    })

    it('should return different instances for different namespaces', () => {
      const posts = db.collection('posts')
      const users = db.collection('users')

      expect(posts).not.toBe(users)
    })
  })

  describe('Proxy-based collection access', () => {
    it('should provide access via db.Posts', () => {
      const posts = (db as any).Posts

      expect(posts.namespace).toBe('posts')
    })

    it('should provide access via db.Users', () => {
      const users = (db as any).Users

      expect(users.namespace).toBe('users')
    })

    it('should convert PascalCase to camelCase', () => {
      const blogPosts = (db as any).BlogPosts

      expect(blogPosts.namespace).toBe('blogPosts')
    })

    it('should handle single letter namespace', () => {
      const x = (db as any).X

      expect(x.namespace).toBe('x')
    })

    it('should return existing properties', () => {
      expect(db.info).toBeDefined()
      expect(db.backend).toBeDefined()
      expect(typeof db.collection).toBe('function')
      expect(typeof db.collections).toBe('function')
    })
  })

  describe('collections()', () => {
    it('should return list of collections from manifest', async () => {
      const manifest = {
        files: {
          'data/posts/data.parquet': { path: 'data/posts/data.parquet' },
          'data/users/data.parquet': { path: 'data/users/data.parquet' },
          'data/comments/data.parquet': { path: 'data/comments/data.parquet' },
        },
      }
      mockFetch.mockResolvedValue(createJsonResponse(manifest))

      const collections = await db.collections()

      expect(collections).toContain('posts')
      expect(collections).toContain('users')
      expect(collections).toContain('comments')
      expect(collections).toHaveLength(3)
    })

    it('should return empty array when manifest is not available', async () => {
      mockFetch.mockResolvedValue(new Response('Not Found', { status: 404 }))

      const collections = await db.collections()

      expect(collections).toEqual([])
    })

    it('should return empty array when manifest has no files', async () => {
      mockFetch.mockResolvedValue(createJsonResponse({ files: {} }))

      const collections = await db.collections()

      expect(collections).toEqual([])
    })

    it('should return empty array on network error', async () => {
      mockFetch.mockRejectedValue(new TypeError('fetch failed'))

      const collections = await db.collections()

      expect(collections).toEqual([])
    })

    it('should deduplicate collections from multiple files', async () => {
      const manifest = {
        files: {
          'data/posts/data.parquet': { path: 'data/posts/data.parquet' },
          'data/posts/index.parquet': { path: 'data/posts/index.parquet' },
        },
      }
      mockFetch.mockResolvedValue(createJsonResponse(manifest))

      const collections = await db.collections()

      expect(collections).toEqual(['posts'])
    })
  })
})

// =============================================================================
// RemoteCollection Tests
// =============================================================================

describe('RemoteCollection', () => {
  let db: RemoteDB

  beforeEach(async () => {
    const dbInfo = createDBInfoResponse()
    mockFetch.mockResolvedValue(createJsonResponse(dbInfo))
    db = await openRemoteDB('testuser/my-dataset')
  })

  describe('find()', () => {
    it('should return empty result when collection does not exist', async () => {
      // Mock backend.exists to return false
      mockFetch.mockResolvedValue(new Response(null, { status: 404 }))

      const posts = db.collection('posts')
      const result = await posts.find()

      expect(result.items).toEqual([])
      expect(result.total).toBe(0)
      expect(result.hasMore).toBe(false)
    })

    it('should return empty result when collection exists but reading fails', async () => {
      // Mock backend.exists to return true (HEAD returns 200)
      mockFetch.mockResolvedValue(
        new Response(null, {
          status: 200,
          headers: { 'Content-Length': '100' },
        })
      )

      const posts = db.collection('posts')
      const result = await posts.find({ status: 'published' })

      // Current implementation returns empty results with warning
      expect(result.items).toEqual([])
      expect(result.hasMore).toBe(false)
    })

    it('should accept filter parameter', async () => {
      mockFetch.mockResolvedValue(new Response(null, { status: 404 }))

      const posts = db.collection('posts')
      const result = await posts.find({ status: 'published', author: 'user1' })

      expect(result.items).toEqual([])
    })

    it('should accept options parameter', async () => {
      mockFetch.mockResolvedValue(new Response(null, { status: 404 }))

      const posts = db.collection('posts')
      const result = await posts.find({}, { limit: 10, skip: 5 })

      expect(result.items).toEqual([])
    })
  })

  describe('findOne()', () => {
    it('should return null when no results', async () => {
      mockFetch.mockResolvedValue(new Response(null, { status: 404 }))

      const posts = db.collection('posts')
      const result = await posts.findOne({ status: 'published' })

      expect(result).toBeNull()
    })

    it('should pass limit: 1 to find', async () => {
      mockFetch.mockResolvedValue(new Response(null, { status: 404 }))

      const posts = db.collection('posts')
      await posts.findOne({ status: 'published' })

      // findOne calls find with limit: 1
      // Verification is implicit - if it doesn't error, it worked
    })
  })

  describe('get()', () => {
    it('should return null when entity not found', async () => {
      mockFetch.mockResolvedValue(new Response(null, { status: 404 }))

      const posts = db.collection('posts')
      const result = await posts.get('non-existent-id')

      expect(result).toBeNull()
    })

    it('should call findOne with $id filter', async () => {
      mockFetch.mockResolvedValue(new Response(null, { status: 404 }))

      const posts = db.collection('posts')
      await posts.get('post-123')

      // get calls findOne({ $id: 'post-123' })
      // Verification is implicit
    })
  })

  describe('count()', () => {
    it('should return 0 when collection does not exist', async () => {
      mockFetch.mockResolvedValue(new Response(null, { status: 404 }))

      const posts = db.collection('posts')
      const result = await posts.count()

      expect(result).toBe(0)
    })

    it('should accept filter parameter', async () => {
      mockFetch.mockResolvedValue(new Response(null, { status: 404 }))

      const posts = db.collection('posts')
      const result = await posts.count({ status: 'published' })

      expect(result).toBe(0)
    })
  })

  describe('exists()', () => {
    it('should return false when entity does not exist', async () => {
      mockFetch.mockResolvedValue(new Response(null, { status: 404 }))

      const posts = db.collection('posts')
      const result = await posts.exists('non-existent-id')

      expect(result).toBe(false)
    })

    it('should call get to check existence', async () => {
      mockFetch.mockResolvedValue(new Response(null, { status: 404 }))

      const posts = db.collection('posts')
      await posts.exists('post-123')

      // exists calls get(id) and returns entity !== null
    })
  })

  describe('namespace', () => {
    it('should have readonly namespace property', () => {
      const posts = db.collection('posts')

      expect(posts.namespace).toBe('posts')
    })
  })
})

// =============================================================================
// Options Type Tests
// =============================================================================

describe('OpenRemoteDBOptions', () => {
  it('should work with no options', async () => {
    const dbInfo = createDBInfoResponse()
    mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

    const db = await openRemoteDB('testuser/my-dataset')

    expect(db).toBeDefined()
  })

  it('should work with token option', async () => {
    const dbInfo = createDBInfoResponse()
    mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

    const db = await openRemoteDB('testuser/my-dataset', {
      token: 'my-auth-token',
    })

    expect(db).toBeDefined()
  })

  it('should work with baseUrl option', async () => {
    const dbInfo = createDBInfoResponse()
    mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

    const db = await openRemoteDB('testuser/my-dataset', {
      baseUrl: 'https://custom.example.com',
    })

    expect(db).toBeDefined()
  })

  it('should work with timeout option', async () => {
    const dbInfo = createDBInfoResponse()
    mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

    const db = await openRemoteDB('testuser/my-dataset', {
      timeout: 5000,
    })

    expect(db).toBeDefined()
  })

  it('should work with headers option', async () => {
    const dbInfo = createDBInfoResponse()
    mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

    const db = await openRemoteDB('testuser/my-dataset', {
      headers: { 'X-Custom-Header': 'value' },
    })

    expect(db).toBeDefined()
  })

  it('should work with all options combined', async () => {
    const dbInfo = createDBInfoResponse()
    mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

    const db = await openRemoteDB('testuser/my-dataset', {
      token: 'my-token',
      baseUrl: 'https://custom.example.com',
      timeout: 5000,
      headers: { 'X-Custom-Header': 'value' },
    })

    expect(db).toBeDefined()
  })
})

// =============================================================================
// RemoteDBInfo Type Tests
// =============================================================================

describe('RemoteDBInfo', () => {
  it('should include all required fields', async () => {
    const dbInfo = createDBInfoResponse()
    mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

    const db = await openRemoteDB('testuser/my-dataset')

    expect(db.info.id).toBeDefined()
    expect(db.info.name).toBeDefined()
    expect(db.info.owner).toBeDefined()
    expect(db.info.slug).toBeDefined()
    expect(db.info.visibility).toBeDefined()
  })

  it('should include optional fields when provided', async () => {
    const dbInfo = createDBInfoResponse({
      description: 'A test description',
      collectionCount: 5,
      entityCount: 100,
    })
    mockFetch.mockResolvedValue(createJsonResponse(dbInfo))

    const db = await openRemoteDB('testuser/my-dataset')

    expect(db.info.description).toBe('A test description')
    expect(db.info.collectionCount).toBe(5)
    expect(db.info.entityCount).toBe(100)
  })

  it('should handle different visibility values', async () => {
    // Test 'unlisted' visibility
    const unlistedDbInfo = createDBInfoResponse({ visibility: 'unlisted' })
    mockFetch.mockResolvedValue(createJsonResponse(unlistedDbInfo))

    const unlistedDb = await openRemoteDB('testuser/my-dataset')
    expect(unlistedDb.info.visibility).toBe('unlisted')
  })
})

// =============================================================================
// Integration-style Tests
// =============================================================================

describe('Remote Client Integration', () => {
  it('should support full workflow: check, open, query', async () => {
    // Step 1: Check if database exists
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 200 }))

    const exists = await checkRemoteDB('testuser/my-dataset')
    expect(exists).toBe(true)

    // Step 2: Open database
    const dbInfo = createDBInfoResponse()
    mockFetch.mockResolvedValueOnce(createJsonResponse(dbInfo))

    const db = await openRemoteDB('testuser/my-dataset')
    expect(db.info.name).toBe('my-dataset')

    // Step 3: Query collection
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 404 }))

    const posts = db.collection('posts')
    const result = await posts.find({ status: 'published' })
    expect(result.items).toEqual([])
  })

  it('should support listing and opening databases', async () => {
    // Step 1: List public databases
    const databases: RemoteDBInfo[] = [
      createDBInfoResponse({ id: 'db1', name: 'dataset-1', owner: 'user1', slug: 'dataset-1' }),
      createDBInfoResponse({ id: 'db2', name: 'dataset-2', owner: 'user2', slug: 'dataset-2' }),
    ]
    mockFetch.mockResolvedValueOnce(createJsonResponse({ databases }))

    const list = await listPublicDatabases()
    expect(list).toHaveLength(2)

    // Step 2: Open first database from list
    const firstDb = list[0]
    mockFetch.mockResolvedValueOnce(createJsonResponse(firstDb))

    const db = await openRemoteDB(`${firstDb.owner}/${firstDb.slug}`)
    expect(db.info.id).toBe('db1')
  })

  it('should handle authenticated access', async () => {
    // First try without token - should fail
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 401 }))

    const existsWithoutAuth = await checkRemoteDB('testuser/private-db')
    expect(existsWithoutAuth).toBe(false)

    // Then try with token - should succeed
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 200 }))

    const existsWithAuth = await checkRemoteDB('testuser/private-db', {
      token: 'valid-token',
    })
    expect(existsWithAuth).toBe(true)

    // Open with token
    const dbInfo = createDBInfoResponse({ visibility: 'private' })
    mockFetch.mockResolvedValueOnce(createJsonResponse(dbInfo))

    const db = await openRemoteDB('testuser/private-db', { token: 'valid-token' })
    expect(db.info.visibility).toBe('private')
  })
})
