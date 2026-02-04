/**
 * Entity Handler Tests
 *
 * Tests for the /datasets/:dataset/:collection/:id entity detail endpoint.
 * Tests CRUD operations, caching, error handling, and relationship grouping.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { handleEntityDetail } from '../../../../src/worker/handlers/entity'
import {
  createMockWorker,
  createMockHandlerContext,
  createMockCaches,
} from '../../../mocks'
import type { MockHandlerContext } from '../../../mocks/worker'

// Mock the Cloudflare caches API
const mockCachesObj = createMockCaches()
// @ts-expect-error - mocking global caches API
globalThis.caches = mockCachesObj

describe('Entity Handler', () => {
  let mockWorker: ReturnType<typeof createMockWorker>
  let mockContext: MockHandlerContext

  beforeEach(() => {
    vi.clearAllMocks()
    mockCachesObj.open.mockClear()
    mockCachesObj.default.match.mockClear()
    mockCachesObj.default.put.mockClear()
    mockCachesObj.default.match.mockResolvedValue(null) // No cache hit by default

    mockWorker = createMockWorker()
  })

  // ===========================================================================
  // Happy Path Tests
  // ===========================================================================

  describe('GET /datasets/:dataset/:collection/:id - happy path', () => {
    it('should return entity when found', async () => {
      const mockEntity = {
        $id: 'occupations/11-1011',
        $type: 'occupation',
        name: 'Chief Executive',
        description: 'Determine and formulate policies',
      }

      mockWorker.find.mockResolvedValue({
        items: [mockEntity],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/onet-graph/occupations/11-1011',
        { worker: mockWorker }
      )

      const response = await handleEntityDetail(
        mockContext as any,
        'onet-graph',
        'occupations',
        '11-1011'
      )

      expect(response.status).toBe(200)
      const body = (await response.json()) as Record<string, unknown>

      expect(body.api).toEqual(
        expect.objectContaining({
          resource: 'entity',
          dataset: 'onet-graph',
          collection: 'occupations',
          id: '11-1011',
          type: 'occupation',
        })
      )

      expect(body.data).toEqual(
        expect.objectContaining({
          $type: 'occupation',
          name: 'Chief Executive',
          description: 'Determine and formulate policies',
        })
      )
    })

    it('should call worker.find with correct filter', async () => {
      mockWorker.find.mockResolvedValue({
        items: [{ $id: 'users/u1', $type: 'user', name: 'Test' }],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/myapp/users/u1',
        { worker: mockWorker }
      )

      await handleEntityDetail(mockContext as any, 'myapp', 'users', 'u1')

      expect(mockWorker.find).toHaveBeenCalledWith(
        'myapp',
        { $id: 'users/u1' },
        { limit: 1 }
      )
    })

    it('should include self link in response', async () => {
      mockWorker.find.mockResolvedValue({
        items: [{ $id: 'posts/p1', $type: 'post', name: 'Hello' }],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/blog/posts/p1',
        { worker: mockWorker }
      )

      const response = await handleEntityDetail(
        mockContext as any,
        'blog',
        'posts',
        'p1'
      )
      const body = (await response.json()) as Record<string, unknown>

      const links = body.links as Record<string, string>
      expect(links.self).toBe('https://api.example.com/datasets/blog/posts/p1')
      expect(links.collection).toBe('https://api.example.com/datasets/blog/posts')
      expect(links.dataset).toBe('https://api.example.com/datasets/blog')
      expect(links.home).toBe('https://api.example.com')
    })

    it('should URL encode entity IDs with special characters', async () => {
      const entityId = '2.C.2.b'
      mockWorker.find.mockResolvedValue({
        items: [{ $id: `knowledge/${entityId}`, $type: 'knowledge', name: 'Test Knowledge' }],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        `https://api.parquedb.com/datasets/onet/knowledge/${encodeURIComponent(entityId)}`,
        { worker: mockWorker }
      )

      const response = await handleEntityDetail(
        mockContext as any,
        'onet',
        'knowledge',
        entityId
      )
      const body = (await response.json()) as Record<string, unknown>

      const links = body.links as Record<string, string>
      expect(links.self).toContain(encodeURIComponent(entityId))
    })
  })

  // ===========================================================================
  // Relationship Grouping Tests
  // ===========================================================================

  describe('relationship grouping', () => {
    it('should group relationships by predicate', async () => {
      mockWorker.find.mockResolvedValue({
        items: [{ $id: 'occupations/11-1011', $type: 'occupation', name: 'CEO' }],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockResolvedValue([
        {
          to_ns: 'skills',
          to_id: 'critical-thinking',
          to_name: 'Critical Thinking',
          to_type: 'skill',
          predicate: 'skills',
          importance: 85,
          level: 4,
          matchMode: null,
          similarity: null,
        },
        {
          to_ns: 'skills',
          to_id: 'active-listening',
          to_name: 'Active Listening',
          to_type: 'skill',
          predicate: 'skills',
          importance: 72,
          level: 3,
          matchMode: null,
          similarity: null,
        },
        {
          to_ns: 'abilities',
          to_id: 'oral-comprehension',
          to_name: 'Oral Comprehension',
          to_type: 'ability',
          predicate: 'abilities',
          importance: 90,
          level: null,
          matchMode: null,
          similarity: null,
        },
      ])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/onet-graph/occupations/11-1011',
        { worker: mockWorker }
      )

      const response = await handleEntityDetail(
        mockContext as any,
        'onet-graph',
        'occupations',
        '11-1011'
      )
      const body = (await response.json()) as Record<string, unknown>

      expect(body.relationships).toBeDefined()
      const rels = body.relationships as Record<string, Record<string, string>>

      // Check skills predicate exists
      expect(rels.skills).toBeDefined()
      expect(rels.skills['Critical Thinking']).toBeDefined()
      expect(rels.skills['Active Listening']).toBeDefined()

      // Check abilities predicate exists
      expect(rels.abilities).toBeDefined()
      expect(rels.abilities['Oral Comprehension']).toBeDefined()
    })

    it('should sort relationship items by importance descending', async () => {
      mockWorker.find.mockResolvedValue({
        items: [{ $id: 'occupations/test', $type: 'occupation', name: 'Test' }],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockResolvedValue([
        {
          to_ns: 'skills',
          to_id: 'skill-low',
          to_name: 'Low Priority Skill',
          to_type: 'skill',
          predicate: 'skills',
          importance: 30,
          level: null,
          matchMode: null,
          similarity: null,
        },
        {
          to_ns: 'skills',
          to_id: 'skill-high',
          to_name: 'High Priority Skill',
          to_type: 'skill',
          predicate: 'skills',
          importance: 95,
          level: null,
          matchMode: null,
          similarity: null,
        },
        {
          to_ns: 'skills',
          to_id: 'skill-mid',
          to_name: 'Mid Priority Skill',
          to_type: 'skill',
          predicate: 'skills',
          importance: 60,
          level: null,
          matchMode: null,
          similarity: null,
        },
      ])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/test/occupations/test?arrays',
        { worker: mockWorker }
      )

      const response = await handleEntityDetail(
        mockContext as any,
        'test',
        'occupations',
        'test'
      )
      const body = (await response.json()) as Record<string, unknown>

      const rels = body.relationships as Record<
        string,
        { items: Array<{ name: string; importance: number }> }
      >
      const skillItems = rels.skills.items

      // Should be sorted by importance descending
      expect(skillItems[0]?.importance).toBe(95)
      expect(skillItems[1]?.importance).toBe(60)
      expect(skillItems[2]?.importance).toBe(30)
    })

    it('should return undefined relationships when entity has no relationships', async () => {
      mockWorker.find.mockResolvedValue({
        items: [{ $id: 'posts/p1', $type: 'post', name: 'Lonely Post' }],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/blog/posts/p1',
        { worker: mockWorker }
      )

      const response = await handleEntityDetail(
        mockContext as any,
        'blog',
        'posts',
        'p1'
      )
      const body = (await response.json()) as Record<string, unknown>

      expect(body.relationships).toBeUndefined()
    })

    it('should return arrays format when ?arrays is set', async () => {
      mockWorker.find.mockResolvedValue({
        items: [{ $id: 'occupations/test', $type: 'occupation', name: 'Test' }],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockResolvedValue([
        {
          to_ns: 'skills',
          to_id: 'skill-1',
          to_name: 'Skill One',
          to_type: 'skill',
          predicate: 'skills',
          importance: 50,
          level: null,
          matchMode: null,
          similarity: null,
        },
      ])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/onet/occupations/test?arrays',
        { worker: mockWorker }
      )

      const response = await handleEntityDetail(
        mockContext as any,
        'onet',
        'occupations',
        'test'
      )
      const body = (await response.json()) as Record<string, unknown>

      const rels = body.relationships as Record<
        string,
        { count: number; href: string; items: unknown[] }
      >
      expect(rels.skills.count).toBe(1)
      expect(rels.skills.href).toContain('/skills')
      expect(Array.isArray(rels.skills.items)).toBe(true)
    })
  })

  // ===========================================================================
  // Error Cases (404, 500)
  // ===========================================================================

  describe('error cases', () => {
    it('should return 404 when entity is not found', async () => {
      mockWorker.find.mockResolvedValue({
        items: [],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/myapp/users/nonexistent',
        { worker: mockWorker }
      )

      const response = await handleEntityDetail(
        mockContext as any,
        'myapp',
        'users',
        'nonexistent'
      )

      expect(response.status).toBe(404)
      const body = (await response.json()) as Record<string, unknown>
      expect(body.error).toBeDefined()
    })

    it('should return 404 when parquet file is missing (File not found error)', async () => {
      mockWorker.find.mockRejectedValue(
        new Error('File not found: myapp/users.parquet')
      )
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/myapp/users/u1',
        { worker: mockWorker }
      )

      const response = await handleEntityDetail(
        mockContext as any,
        'myapp',
        'users',
        'u1'
      )

      expect(response.status).toBe(404)
      const body = (await response.json()) as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.code).toBe('DATASET_NOT_FOUND')
      expect(api.message).toContain('Dataset file not found')
    })

    it('should return 404 when relationship file is missing', async () => {
      mockWorker.find.mockResolvedValue({
        items: [{ $id: 'users/u1', $type: 'user', name: 'Test' }],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockRejectedValue(
        new Error('File not found: myapp/rels.parquet')
      )

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/myapp/users/u1',
        { worker: mockWorker }
      )

      const response = await handleEntityDetail(
        mockContext as any,
        'myapp',
        'users',
        'u1'
      )

      expect(response.status).toBe(404)
      const body = (await response.json()) as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.code).toBe('DATASET_NOT_FOUND')
    })

    it('should re-throw non file-not-found errors', async () => {
      mockWorker.find.mockRejectedValue(new Error('Database connection failed'))
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/myapp/users/u1',
        { worker: mockWorker }
      )

      await expect(
        handleEntityDetail(mockContext as any, 'myapp', 'users', 'u1')
      ).rejects.toThrow('Database connection failed')
    })
  })

  // ===========================================================================
  // Cache Behavior Tests
  // ===========================================================================

  describe('caching behavior', () => {
    it('should check cache before fetching data', async () => {
      mockWorker.find.mockResolvedValue({
        items: [{ $id: 'users/u1', $type: 'user', name: 'Test' }],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/myapp/users/u1',
        { worker: mockWorker }
      )

      await handleEntityDetail(mockContext as any, 'myapp', 'users', 'u1')

      expect(mockCachesObj.open).toHaveBeenCalledWith('parquedb-responses')
      expect(mockCachesObj.default.match).toHaveBeenCalled()
    })

    it('should return cached response when available', async () => {
      const cachedData = {
        api: { resource: 'entity', id: 'u1' },
        links: { self: '/users/u1' },
        data: { $id: 'users/u1', name: 'Cached User' },
        relationships: undefined,
      }

      mockCachesObj.default.match.mockResolvedValue(
        Response.json(cachedData)
      )

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/myapp/users/u1',
        { worker: mockWorker }
      )

      const response = await handleEntityDetail(
        mockContext as any,
        'myapp',
        'users',
        'u1'
      )

      expect(response.status).toBe(200)
      // Worker should not be called when cache hits
      expect(mockWorker.find).not.toHaveBeenCalled()
      expect(mockWorker.getRelationships).not.toHaveBeenCalled()
    })

    it('should store response in cache after successful fetch', async () => {
      mockWorker.find.mockResolvedValue({
        items: [{ $id: 'users/u1', $type: 'user', name: 'New User' }],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/myapp/users/u1',
        { worker: mockWorker }
      )

      await handleEntityDetail(mockContext as any, 'myapp', 'users', 'u1')

      // Check that waitUntil was called to cache the response
      expect(mockContext.ctx.waitUntil).toHaveBeenCalled()
    })
  })

  // ===========================================================================
  // Parallel Fetch Tests
  // ===========================================================================

  describe('parallel fetch behavior', () => {
    it('should fetch entity and relationships in parallel', async () => {
      let findResolved = false
      let relsResolved = false

      mockWorker.find.mockImplementation(async () => {
        // Simulate some delay
        await new Promise((resolve) => setTimeout(resolve, 10))
        findResolved = true
        return {
          items: [{ $id: 'users/u1', $type: 'user', name: 'Test' }],
          hasMore: false,
          stats: {},
        }
      })

      mockWorker.getRelationships.mockImplementation(async () => {
        // This should start before find resolves
        expect(findResolved).toBe(false)
        await new Promise((resolve) => setTimeout(resolve, 5))
        relsResolved = true
        return []
      })

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/myapp/users/u1',
        { worker: mockWorker }
      )

      await handleEntityDetail(mockContext as any, 'myapp', 'users', 'u1')

      expect(findResolved).toBe(true)
      expect(relsResolved).toBe(true)
    })
  })

  // ===========================================================================
  // Input Validation Tests
  // ===========================================================================

  describe('input validation', () => {
    it('should handle entity IDs with slashes', async () => {
      mockWorker.find.mockResolvedValue({
        items: [{ $id: 'knowledge/2.C.2.b/subsection', $type: 'knowledge', name: 'Test' }],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/onet/knowledge/2.C.2.b%2Fsubsection',
        { worker: mockWorker }
      )

      const response = await handleEntityDetail(
        mockContext as any,
        'onet',
        'knowledge',
        '2.C.2.b/subsection'
      )

      expect(response.status).toBe(200)
      expect(mockWorker.find).toHaveBeenCalledWith(
        'onet',
        { $id: 'knowledge/2.C.2.b/subsection' },
        { limit: 1 }
      )
    })

    it('should handle dataset IDs with hyphens', async () => {
      mockWorker.find.mockResolvedValue({
        items: [{ $id: 'items/i1', $type: 'item', name: 'Test' }],
        hasMore: false,
        stats: {},
      })
      mockWorker.getRelationships.mockResolvedValue([])

      mockContext = createMockHandlerContext(
        'https://api.parquedb.com/datasets/my-cool-dataset/items/i1',
        { worker: mockWorker }
      )

      const response = await handleEntityDetail(
        mockContext as any,
        'my-cool-dataset',
        'items',
        'i1'
      )

      expect(response.status).toBe(200)
      expect(mockWorker.find).toHaveBeenCalledWith(
        'my-cool-dataset',
        expect.any(Object),
        expect.any(Object)
      )
    })
  })
})
