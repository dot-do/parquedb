/**
 * Worker HTTP Handlers Tests
 *
 * Integration tests for worker route handlers using mock Request/Response objects.
 * Tests routing utilities, health, root, dataset, ns, and entity handlers.
 */

import { describe, it, expect, vi, beforeEach, beforeAll } from 'vitest'
import {
  parseQueryFilter,
  parseQueryOptions,
  QueryParamError,
  RoutePatterns,
  matchRoute,
} from '@/worker/routing'
import { handleHealth } from '@/worker/handlers/health'
import { handleRoot } from '@/worker/handlers/root'
import {
  handleDatasetsList,
  handleDatasetDetail,
  handleCollectionList,
} from '@/worker/handlers/datasets'
import { handleNsRoute } from '@/worker/handlers/ns'
import { handleRelationshipTraversal } from '@/worker/handlers/relationships'
import { handleEntityDetail } from '@/worker/handlers/entity'
import { DATASETS } from '@/worker/datasets'
import type { HandlerContext } from '@/worker/handlers/types'
import { createMockWorker, createMockCaches, createMockExecutionContext } from '../../mocks'

// Mock the Cloudflare caches API for Node.js environment
const mockCachesObj = createMockCaches()
// @ts-expect-error - mocking global caches API
globalThis.caches = mockCachesObj

// =============================================================================
// Mock Helpers
// =============================================================================

/**
 * Create a HandlerContext for testing
 */
function createContext(
  urlStr: string,
  options: {
    method?: string
    body?: unknown
    worker?: ReturnType<typeof createMockWorker>
  } = {}
): HandlerContext {
  const method = options.method || 'GET'
  const url = new URL(urlStr)
  const baseUrl = `${url.protocol}//${url.host}`
  const path = url.pathname

  // Build headers - include CSRF headers for mutation methods
  const headers: Record<string, string> = {}
  if (['POST', 'PATCH', 'PUT', 'DELETE'].includes(method)) {
    headers['X-Requested-With'] = 'XMLHttpRequest'
    headers['Origin'] = baseUrl
  }

  const requestInit: RequestInit = { method }
  if (options.body) {
    requestInit.body = JSON.stringify(options.body)
    headers['Content-Type'] = 'application/json'
  }
  if (Object.keys(headers).length > 0) {
    requestInit.headers = headers
  }

  const request = new Request(urlStr, requestInit)

  return {
    request,
    url,
    baseUrl,
    path,
    worker: (options.worker || createMockWorker()) as unknown as HandlerContext['worker'],
    startTime: performance.now(),
    ctx: createMockExecutionContext() as unknown as ExecutionContext,
  }
}

// =============================================================================
// Routing Utilities
// =============================================================================

describe('Routing Utilities', () => {
  // ===========================================================================
  // parseQueryFilter
  // ===========================================================================

  describe('parseQueryFilter', () => {
    it('should return empty filter when no filter param', () => {
      const params = new URLSearchParams()
      const filter = parseQueryFilter(params)
      expect(filter).toEqual({})
    })

    it('should parse valid JSON filter', () => {
      const params = new URLSearchParams({
        filter: '{"status":"published"}',
      })
      const filter = parseQueryFilter(params)
      expect(filter).toEqual({ status: 'published' })
    })

    it('should parse filter with operators', () => {
      const params = new URLSearchParams({
        filter: '{"score":{"$gte":100},"status":{"$in":["published","featured"]}}',
      })
      const filter = parseQueryFilter(params)
      expect(filter).toEqual({
        score: { $gte: 100 },
        status: { $in: ['published', 'featured'] },
      })
    })

    it('should throw QueryParamError for invalid JSON', () => {
      const params = new URLSearchParams({
        filter: 'not-valid-json',
      })
      expect(() => parseQueryFilter(params)).toThrow(QueryParamError)
      expect(() => parseQueryFilter(params)).toThrow('Invalid filter: must be valid JSON')
    })

    it('should parse filter with logical operators', () => {
      const params = new URLSearchParams({
        filter: '{"$or":[{"status":"published"},{"featured":true}]}',
      })
      const filter = parseQueryFilter(params)
      expect(filter).toEqual({
        $or: [{ status: 'published' }, { featured: true }],
      })
    })
  })

  // ===========================================================================
  // parseQueryOptions - sort
  // ===========================================================================

  describe('parseQueryOptions - sort', () => {
    it('should return empty options when no params', () => {
      const params = new URLSearchParams()
      const options = parseQueryOptions(params)
      expect(options).toEqual({})
    })

    it('should parse JSON sort', () => {
      const params = new URLSearchParams({
        sort: '{"createdAt":-1}',
      })
      const options = parseQueryOptions(params)
      expect(options.sort).toEqual({ createdAt: -1 })
    })

    it('should parse simple sort format field:asc', () => {
      const params = new URLSearchParams({
        sort: 'name:asc',
      })
      const options = parseQueryOptions(params)
      expect(options.sort).toEqual({ name: 1 })
    })

    it('should parse simple sort format field:desc', () => {
      const params = new URLSearchParams({
        sort: 'createdAt:desc',
      })
      const options = parseQueryOptions(params)
      expect(options.sort).toEqual({ createdAt: -1 })
    })

    it('should parse multiple sort fields', () => {
      const params = new URLSearchParams({
        sort: 'status:asc,createdAt:desc',
      })
      const options = parseQueryOptions(params)
      expect(options.sort).toEqual({ status: 1, createdAt: -1 })
    })

    it('should default to asc when direction is missing', () => {
      const params = new URLSearchParams({
        sort: 'name',
      })
      const options = parseQueryOptions(params)
      expect(options.sort).toEqual({ name: 1 })
    })
  })

  // ===========================================================================
  // parseQueryOptions - project
  // ===========================================================================

  describe('parseQueryOptions - project', () => {
    it('should parse JSON projection', () => {
      const params = new URLSearchParams({
        project: '{"title":1,"content":1}',
      })
      const options = parseQueryOptions(params)
      expect(options.project).toEqual({ title: 1, content: 1 })
    })

    it('should parse simple inclusion projection', () => {
      const params = new URLSearchParams({
        project: 'title,content,author',
      })
      const options = parseQueryOptions(params)
      expect(options.project).toEqual({ title: 1, content: 1, author: 1 })
    })

    it('should parse simple exclusion projection with dash prefix', () => {
      const params = new URLSearchParams({
        project: '-password,-secret',
      })
      const options = parseQueryOptions(params)
      expect(options.project).toEqual({ password: 0, secret: 0 })
    })

    it('should parse mixed inclusion and exclusion', () => {
      const params = new URLSearchParams({
        project: 'name,title,-internal',
      })
      const options = parseQueryOptions(params)
      expect(options.project).toEqual({ name: 1, title: 1, internal: 0 })
    })
  })

  // ===========================================================================
  // parseQueryOptions - limit, skip, cursor
  // ===========================================================================

  describe('parseQueryOptions - pagination', () => {
    it('should parse limit', () => {
      const params = new URLSearchParams({ limit: '50' })
      const options = parseQueryOptions(params)
      expect(options.limit).toBe(50)
    })

    it('should parse skip', () => {
      const params = new URLSearchParams({ skip: '20' })
      const options = parseQueryOptions(params)
      expect(options.skip).toBe(20)
    })

    it('should parse cursor', () => {
      const params = new URLSearchParams({ cursor: 'abc123' })
      const options = parseQueryOptions(params)
      expect(options.cursor).toBe('abc123')
    })

    it('should parse all pagination options together', () => {
      const params = new URLSearchParams({
        limit: '10',
        skip: '5',
        cursor: 'xyz',
      })
      const options = parseQueryOptions(params)
      expect(options.limit).toBe(10)
      expect(options.skip).toBe(5)
      expect(options.cursor).toBe('xyz')
    })
  })

  // ===========================================================================
  // RoutePatterns and matchRoute
  // ===========================================================================

  describe('RoutePatterns', () => {
    it('should match /datasets/:dataset', () => {
      const result = matchRoute<[string]>('/datasets/imdb', RoutePatterns.dataset)
      expect(result).toEqual(['imdb'])
    })

    it('should not match /datasets', () => {
      const result = matchRoute<[string]>('/datasets', RoutePatterns.dataset)
      expect(result).toBeNull()
    })

    it('should match /datasets/:dataset/:collection', () => {
      const result = matchRoute<[string, string]>('/datasets/imdb/titles', RoutePatterns.collection)
      expect(result).toEqual(['imdb', 'titles'])
    })

    it('should match /datasets/:dataset/:collection/:id', () => {
      const result = matchRoute<[string, string, string]>('/datasets/imdb/titles/tt0000001', RoutePatterns.entity)
      expect(result).toEqual(['imdb', 'titles', 'tt0000001'])
    })

    it('should match /datasets/:dataset/:collection/:id/:predicate', () => {
      const result = matchRoute<[string, string, string, string]>(
        '/datasets/onet-graph/occupations/11-1011/skills',
        RoutePatterns.relationship
      )
      expect(result).toEqual(['onet-graph', 'occupations', '11-1011', 'skills'])
    })

    it('should match /ns/:namespace', () => {
      const result = matchRoute<[string, string | undefined]>('/ns/posts', RoutePatterns.ns)
      expect(result).not.toBeNull()
      expect(result![0]).toBe('posts')
      expect(result![1]).toBeUndefined()
    })

    it('should match /ns/:namespace/:id', () => {
      const result = matchRoute<[string, string]>('/ns/posts/abc123', RoutePatterns.ns)
      expect(result).toEqual(['posts', 'abc123'])
    })

    it('should not match unrelated paths', () => {
      expect(matchRoute('/health', RoutePatterns.dataset)).toBeNull()
      expect(matchRoute('/unknown/path', RoutePatterns.ns)).toBeNull()
      expect(matchRoute('/', RoutePatterns.entity)).toBeNull()
    })
  })
})

// =============================================================================
// Health Handler
// =============================================================================

describe('handleHealth', () => {
  it('should return 200 with healthy status', () => {
    const context = createContext('https://test.parquedb.com/health')
    const response = handleHealth(context)

    expect(response.status).toBe(200)
  })

  it('should include api status in response body', async () => {
    const context = createContext('https://test.parquedb.com/health')
    const response = handleHealth(context)
    const body = await response.json() as Record<string, unknown>

    expect(body.api).toBeDefined()
    const api = body.api as Record<string, unknown>
    expect(api.status).toBe('healthy')
    expect(api.uptime).toBe('ok')
    expect(api.storage).toBe('r2')
    expect(api.compute).toBe('durable-objects')
  })

  it('should include links in response', async () => {
    const context = createContext('https://test.parquedb.com/health')
    const response = handleHealth(context)
    const body = await response.json() as Record<string, unknown>

    const links = body.links as Record<string, string>
    expect(links.home).toBe('https://test.parquedb.com')
    expect(links.datasets).toBe('https://test.parquedb.com/datasets')
  })

  it('should include CORS header', () => {
    const context = createContext('https://test.parquedb.com/health')
    const response = handleHealth(context)

    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })
})

// =============================================================================
// Root Handler
// =============================================================================

describe('handleRoot', () => {
  it('should return 200', () => {
    const context = createContext('https://test.parquedb.com/')
    const response = handleRoot(context)

    expect(response.status).toBe(200)
  })

  it('should include API info in response', async () => {
    const context = createContext('https://test.parquedb.com/')
    const response = handleRoot(context)
    const body = await response.json() as Record<string, unknown>

    const api = body.api as Record<string, unknown>
    expect(api.name).toBe('ParqueDB')
    expect(api.version).toBeDefined()
    expect(api.description).toBeDefined()
  })

  it('should include navigation links', async () => {
    const context = createContext('https://test.parquedb.com/')
    const response = handleRoot(context)
    const body = await response.json() as Record<string, unknown>

    const links = body.links as Record<string, string>
    expect(links.self).toBe('https://test.parquedb.com')
    expect(links.datasets).toBe('https://test.parquedb.com/datasets')
    expect(links.health).toBe('https://test.parquedb.com/health')
  })

  it('should include CORS header', () => {
    const context = createContext('https://test.parquedb.com/')
    const response = handleRoot(context)

    expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
  })
})

// =============================================================================
// Dataset Handlers
// =============================================================================

describe('Dataset Handlers', () => {
  // ===========================================================================
  // handleDatasetsList
  // ===========================================================================

  describe('handleDatasetsList', () => {
    it('should return 200 with list of datasets', async () => {
      const context = createContext('https://test.parquedb.com/datasets')
      const response = handleDatasetsList(context)

      expect(response.status).toBe(200)

      const body = await response.json() as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.resource).toBe('datasets')
      expect(api.count).toBeGreaterThan(0)
    })

    it('should include items array with all datasets', async () => {
      const context = createContext('https://test.parquedb.com/datasets')
      const response = handleDatasetsList(context)
      const body = await response.json() as Record<string, unknown>

      const items = body.items as Array<Record<string, unknown>>
      expect(items.length).toBe(Object.keys(DATASETS).length)

      // Each item should have an id and href
      for (const item of items) {
        expect(item.id).toBeDefined()
        expect(item.href).toBeDefined()
        expect(typeof item.href).toBe('string')
        expect((item.href as string).startsWith('https://test.parquedb.com/datasets/')).toBe(true)
      }
    })

    it('should include links to each dataset', async () => {
      const context = createContext('https://test.parquedb.com/datasets')
      const response = handleDatasetsList(context)
      const body = await response.json() as Record<string, unknown>

      const links = body.links as Record<string, string>
      expect(links.self).toBe('https://test.parquedb.com/datasets')

      // Should have a link for each dataset
      for (const key of Object.keys(DATASETS)) {
        expect(links[key]).toBe(`https://test.parquedb.com/datasets/${key}`)
      }
    })
  })

  // ===========================================================================
  // handleDatasetDetail
  // ===========================================================================

  describe('handleDatasetDetail', () => {
    it('should return 200 for known dataset', async () => {
      const context = createContext('https://test.parquedb.com/datasets/imdb')
      const response = handleDatasetDetail(context, 'imdb')

      expect(response.status).toBe(200)

      const body = await response.json() as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.resource).toBe('dataset')
      expect(api.id).toBe('imdb')
      expect(api.name).toBe('IMDB')
    })

    it('should return 404 for unknown dataset', async () => {
      const context = createContext('https://test.parquedb.com/datasets/nonexistent')
      const response = handleDatasetDetail(context, 'nonexistent')

      expect(response.status).toBe(404)

      const body = await response.json() as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.error).toBe(true)
      expect(api.message).toContain('not found')
    })

    it('should include collection links for known dataset', async () => {
      const context = createContext('https://test.parquedb.com/datasets/imdb')
      const response = handleDatasetDetail(context, 'imdb')
      const body = await response.json() as Record<string, unknown>

      const links = body.links as Record<string, string>
      expect(links.self).toBe('https://test.parquedb.com/datasets/imdb')
      expect(links.home).toBe('https://test.parquedb.com')
      expect(links.datasets).toBe('https://test.parquedb.com/datasets')

      // Should have links for each collection
      const dataset = DATASETS['imdb']!
      for (const col of dataset.collections) {
        expect(links[col]).toBe(`https://test.parquedb.com/datasets/imdb/${col}`)
      }
    })

    it('should include collections data in response body', async () => {
      const context = createContext('https://test.parquedb.com/datasets/imdb')
      const response = handleDatasetDetail(context, 'imdb')
      const body = await response.json() as Record<string, unknown>

      const data = body.data as Record<string, unknown>
      expect(data.collections).toBeDefined()

      // Default format is object map
      const collections = data.collections as Record<string, string>
      const dataset = DATASETS['imdb']!
      for (const col of dataset.collections) {
        expect(collections[col]).toBe(`https://test.parquedb.com/datasets/imdb/${col}`)
      }
    })

    it('should return arrays format when ?arrays is set', async () => {
      const context = createContext('https://test.parquedb.com/datasets/imdb?arrays')
      const response = handleDatasetDetail(context, 'imdb')
      const body = await response.json() as Record<string, unknown>

      const data = body.data as Record<string, unknown>
      const collections = data.collections as Array<Record<string, string>>
      expect(Array.isArray(collections)).toBe(true)
      expect(collections[0]).toHaveProperty('name')
      expect(collections[0]).toHaveProperty('href')
    })
  })

  // ===========================================================================
  // handleCollectionList
  // ===========================================================================

  describe('handleCollectionList', () => {
    it('should return 404 for unknown dataset', async () => {
      const context = createContext('https://test.parquedb.com/datasets/nonexistent/titles')
      const response = await handleCollectionList(context, 'nonexistent', 'titles')

      expect(response.status).toBe(404)
    })

    it('should call worker.find with correct namespace', async () => {
      const worker = createMockWorker()
      const context = createContext(
        'https://test.parquedb.com/datasets/imdb/titles',
        { worker }
      )

      await handleCollectionList(context, 'imdb', 'titles')

      expect(worker.find).toHaveBeenCalledWith(
        'imdb/titles',
        expect.any(Object),
        expect.objectContaining({ limit: 20 })
      )
    })

    it('should pass filter from query params to worker.find', async () => {
      const worker = createMockWorker()
      const context = createContext(
        'https://test.parquedb.com/datasets/imdb/titles?filter={"titleType":"movie"}',
        { worker }
      )

      await handleCollectionList(context, 'imdb', 'titles')

      expect(worker.find).toHaveBeenCalledWith(
        'imdb/titles',
        { titleType: 'movie' },
        expect.any(Object)
      )
    })

    it('should pass limit from query params', async () => {
      const worker = createMockWorker()
      const context = createContext(
        'https://test.parquedb.com/datasets/imdb/titles?limit=50',
        { worker }
      )

      await handleCollectionList(context, 'imdb', 'titles')

      expect(worker.find).toHaveBeenCalledWith(
        'imdb/titles',
        expect.any(Object),
        expect.objectContaining({ limit: 50 })
      )
    })

    it('should return 200 with items', async () => {
      const worker = createMockWorker({
        find: vi.fn().mockResolvedValue({
          items: [
            { $id: 'titles/tt0000001', $type: 'title', name: 'Carmencita' },
          ],
          stats: {},
          hasMore: false,
        }),
      })
      const context = createContext(
        'https://test.parquedb.com/datasets/imdb/titles',
        { worker }
      )

      const response = await handleCollectionList(context, 'imdb', 'titles')

      expect(response.status).toBe(200)
      const body = await response.json() as Record<string, unknown>
      const items = body.items as unknown[]
      expect(items.length).toBe(1)
    })

    it('should include next link when hasMore is true', async () => {
      const worker = createMockWorker({
        find: vi.fn().mockResolvedValue({
          items: [{ $id: 'titles/tt0000001', name: 'Test' }],
          stats: {},
          hasMore: true,
        }),
      })
      const context = createContext(
        'https://test.parquedb.com/datasets/imdb/titles',
        { worker }
      )

      const response = await handleCollectionList(context, 'imdb', 'titles')
      const body = await response.json() as Record<string, unknown>
      const links = body.links as Record<string, string>

      expect(links.next).toBeDefined()
      expect(links.next).toContain('skip=')
    })
  })
})

// =============================================================================
// NS (Legacy) Handler
// =============================================================================

describe('handleNsRoute', () => {
  // ===========================================================================
  // GET operations
  // ===========================================================================

  describe('GET /ns/:namespace', () => {
    it('should call worker.find for namespace listing', async () => {
      const worker = createMockWorker()
      const context = createContext('https://test.parquedb.com/ns/posts', { worker })

      await handleNsRoute(context, 'posts')

      expect(worker.find).toHaveBeenCalledWith('posts', expect.any(Object), expect.any(Object))
    })

    it('should return items array', async () => {
      const worker = createMockWorker({
        find: vi.fn().mockResolvedValue({
          items: [{ $id: 'posts/1', name: 'Hello' }],
          stats: {},
          hasMore: false,
        }),
      })
      const context = createContext('https://test.parquedb.com/ns/posts', { worker })

      const response = await handleNsRoute(context, 'posts')
      expect(response.status).toBe(200)

      const body = await response.json() as Record<string, unknown>
      expect(body.items).toBeDefined()
      const items = body.items as unknown[]
      expect(items.length).toBe(1)
    })
  })

  describe('GET /ns/:namespace/:id', () => {
    it('should return entity when found', async () => {
      const mockEntity = { $id: 'posts/1', name: 'Hello World', $type: 'post' }
      const worker = createMockWorker({
        get: vi.fn().mockResolvedValue(mockEntity),
      })
      const context = createContext('https://test.parquedb.com/ns/posts/1', { worker })

      const response = await handleNsRoute(context, 'posts', '1')

      expect(response.status).toBe(200)
      const body = await response.json() as Record<string, unknown>
      expect(body.data).toEqual(mockEntity)
    })

    it('should return 404 for unknown entity', async () => {
      const worker = createMockWorker({
        get: vi.fn().mockResolvedValue(null),
      })
      const context = createContext('https://test.parquedb.com/ns/posts/nonexistent', { worker })

      const response = await handleNsRoute(context, 'posts', 'nonexistent')

      expect(response.status).toBe(404)
      const body = await response.json() as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.error).toBe(true)
      expect(api.message).toContain('not found')
    })
  })

  // ===========================================================================
  // POST operations
  // ===========================================================================

  describe('POST /ns/:namespace', () => {
    it('should create entity and return 201', async () => {
      const created = { $id: 'posts/new1', name: 'New Post', type: 'post' }
      const worker = createMockWorker({
        create: vi.fn().mockResolvedValue(created),
      })
      const context = createContext('https://test.parquedb.com/ns/posts', {
        method: 'POST',
        body: { name: 'New Post', type: 'post' },
        worker,
      })

      const response = await handleNsRoute(context, 'posts')

      expect(response.status).toBe(201)
      const body = await response.json()
      expect(body).toEqual(created)
      expect(worker.create).toHaveBeenCalledWith('posts', { name: 'New Post', type: 'post' })
    })

    it('should warn but still create when type is missing', async () => {
      const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
      const worker = createMockWorker()
      const context = createContext('https://test.parquedb.com/ns/posts', {
        method: 'POST',
        body: { name: 'Test Post' },
        worker,
      })

      const response = await handleNsRoute(context, 'posts')

      expect(response.status).toBe(201)
      expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('missing type field'))
      expect(worker.create).toHaveBeenCalled()
      warnSpy.mockRestore()
    })

    it('should warn but still create when name is missing', async () => {
      const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
      const worker = createMockWorker()
      const context = createContext('https://test.parquedb.com/ns/posts', {
        method: 'POST',
        body: { type: 'post' },
        worker,
      })

      const response = await handleNsRoute(context, 'posts')

      expect(response.status).toBe(201)
      expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('missing name field'))
      expect(worker.create).toHaveBeenCalled()
      warnSpy.mockRestore()
    })

    it('should throw QueryParamError for invalid JSON body', async () => {
      const request = new Request('https://test.parquedb.com/ns/posts', {
        method: 'POST',
        body: 'not json',
        headers: {
          'Content-Type': 'text/plain',
          'X-Requested-With': 'XMLHttpRequest',
          'Origin': 'https://test.parquedb.com',
        },
      })
      const url = new URL(request.url)
      const context: HandlerContext = {
        request,
        url,
        baseUrl: `${url.protocol}//${url.host}`,
        path: url.pathname,
        worker: createMockWorker() as unknown as HandlerContext['worker'],
        startTime: performance.now(),
        ctx: { waitUntil: vi.fn(), passThroughOnException: vi.fn() } as unknown as ExecutionContext,
      }

      await expect(handleNsRoute(context, 'posts')).rejects.toThrow('Invalid JSON body')
    })
  })

  // ===========================================================================
  // PATCH operations
  // ===========================================================================

  describe('PATCH /ns/:namespace/:id', () => {
    it('should return 400 when no ID provided', async () => {
      const worker = createMockWorker()
      const context = createContext('https://test.parquedb.com/ns/posts', {
        method: 'PATCH',
        body: { $set: { name: 'Updated' } },
        worker,
      })

      const response = await handleNsRoute(context, 'posts')

      expect(response.status).toBe(400)
      const body = await response.json() as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.message).toContain('ID required')
    })

    it('should update entity when ID is provided', async () => {
      const updateResult = { matched: 1, modified: 1 }
      const worker = createMockWorker({
        update: vi.fn().mockResolvedValue(updateResult),
      })
      const context = createContext('https://test.parquedb.com/ns/posts/1', {
        method: 'PATCH',
        body: { $set: { name: 'Updated Title' } },
        worker,
      })

      const response = await handleNsRoute(context, 'posts', '1')

      expect(response.status).toBe(200)
      expect(worker.update).toHaveBeenCalledWith('posts', '1', { $set: { name: 'Updated Title' } })
    })

    it('should pass through update body without operator validation', async () => {
      const worker = createMockWorker()
      const context = createContext('https://test.parquedb.com/ns/posts/1', {
        method: 'PATCH',
        body: { name: 'Updated Title' },
        worker,
      })

      const response = await handleNsRoute(context, 'posts', '1')

      expect(response.status).toBe(200)
      expect(worker.update).toHaveBeenCalledWith('posts', '1', { name: 'Updated Title' })
    })

    it('should pass through empty update body', async () => {
      const worker = createMockWorker()
      const context = createContext('https://test.parquedb.com/ns/posts/1', {
        method: 'PATCH',
        body: {},
        worker,
      })

      const response = await handleNsRoute(context, 'posts', '1')

      expect(response.status).toBe(200)
      expect(worker.update).toHaveBeenCalledWith('posts', '1', {})
    })
  })

  // ===========================================================================
  // DELETE operations
  // ===========================================================================

  describe('DELETE /ns/:namespace/:id', () => {
    it('should return 400 when no ID provided', async () => {
      const worker = createMockWorker()
      const context = createContext('https://test.parquedb.com/ns/posts', {
        method: 'DELETE',
        worker,
      })

      const response = await handleNsRoute(context, 'posts')

      expect(response.status).toBe(400)
      const body = await response.json() as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.message).toContain('ID required')
    })

    it('should delete entity when ID is provided', async () => {
      const deleteResult = { deleted: 1 }
      const worker = createMockWorker({
        delete: vi.fn().mockResolvedValue(deleteResult),
      })
      const context = createContext('https://test.parquedb.com/ns/posts/1', {
        method: 'DELETE',
        worker,
      })

      const response = await handleNsRoute(context, 'posts', '1')

      expect(response.status).toBe(200)
      expect(worker.delete).toHaveBeenCalledWith('posts', '1')
    })
  })

  // ===========================================================================
  // Method not allowed
  // ===========================================================================

  describe('unsupported methods', () => {
    it('should return 405 for PUT', async () => {
      const context = createContext('https://test.parquedb.com/ns/posts', {
        method: 'PUT',
      })

      const response = await handleNsRoute(context, 'posts')

      expect(response.status).toBe(405)
    })
  })
})

// =============================================================================
// Relationship Handler
// =============================================================================

describe('handleRelationshipTraversal', () => {
  it('should return relationship items', async () => {
    const worker = createMockWorker({
      getRelationships: vi.fn().mockResolvedValue([
        {
          to_ns: 'skills',
          to_id: 'critical-thinking',
          to_name: 'Critical Thinking',
          to_type: 'skill',
          predicate: 'skills',
          importance: 85,
          level: 4,
        },
        {
          to_ns: 'skills',
          to_id: 'active-listening',
          to_name: 'Active Listening',
          to_type: 'skill',
          predicate: 'skills',
          importance: 72,
          level: 3,
        },
      ]),
      getStorageStats: vi.fn().mockReturnValue({
        cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
      }),
    })

    const context = createContext(
      'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011/skills',
      { worker }
    )

    const response = await handleRelationshipTraversal(
      context, 'onet-graph', 'occupations', '11-1011', 'skills'
    )

    expect(response.status).toBe(200)
    const body = await response.json() as Record<string, unknown>

    const api = body.api as Record<string, unknown>
    expect(api.resource).toBe('relationships')
    expect(api.predicate).toBe('skills')
    expect(api.count).toBe(2)

    const items = body.items as Array<Record<string, unknown>>
    expect(items.length).toBe(2)

    // Should be sorted by importance descending
    expect(items[0]!.name).toBe('Critical Thinking')
    expect(items[0]!.importance).toBe(85)
    expect(items[1]!.name).toBe('Active Listening')
    expect(items[1]!.importance).toBe(72)
  })

  it('should return empty items when no relationships', async () => {
    const worker = createMockWorker({
      getRelationships: vi.fn().mockResolvedValue([]),
      getStorageStats: vi.fn().mockReturnValue({
        cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
      }),
    })

    const context = createContext(
      'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011/skills',
      { worker }
    )

    const response = await handleRelationshipTraversal(
      context, 'onet-graph', 'occupations', '11-1011', 'skills'
    )

    expect(response.status).toBe(200)
    const body = await response.json() as Record<string, unknown>
    const items = body.items as unknown[]
    expect(items.length).toBe(0)
    const api = body.api as Record<string, unknown>
    expect(api.count).toBe(0)
  })

  it('should paginate results with limit and skip', async () => {
    // Create 5 relationships
    const rels = Array.from({ length: 5 }, (_, i) => ({
      to_ns: 'skills',
      to_id: `skill-${i}`,
      to_name: `Skill ${i}`,
      to_type: 'skill',
      predicate: 'skills',
      importance: 100 - i * 10,
      level: null,
    }))

    const worker = createMockWorker({
      getRelationships: vi.fn().mockResolvedValue(rels),
      getStorageStats: vi.fn().mockReturnValue({
        cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
      }),
    })

    const context = createContext(
      'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011/skills?limit=2&skip=1',
      { worker }
    )

    const response = await handleRelationshipTraversal(
      context, 'onet-graph', 'occupations', '11-1011', 'skills'
    )

    const body = await response.json() as Record<string, unknown>
    const items = body.items as unknown[]
    // 5 items sorted by importance, skip 1, limit 2
    expect(items.length).toBe(2)
    const api = body.api as Record<string, unknown>
    // count should be total (5), not paginated count
    expect(api.count).toBe(5)
  })

  it('should throw error for invalid limit (non-numeric)', async () => {
    const worker = createMockWorker({
      getRelationships: vi.fn().mockResolvedValue([]),
      getStorageStats: vi.fn().mockReturnValue({
        cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
      }),
    })

    const context = createContext(
      'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011/skills?limit=abc',
      { worker }
    )

    await expect(
      handleRelationshipTraversal(context, 'onet-graph', 'occupations', '11-1011', 'skills')
    ).rejects.toThrow('Invalid limit: must be a valid integer')
  })

  it('should throw error for negative skip', async () => {
    const worker = createMockWorker({
      getRelationships: vi.fn().mockResolvedValue([]),
      getStorageStats: vi.fn().mockReturnValue({
        cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
      }),
    })

    const context = createContext(
      'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011/skills?skip=-10',
      { worker }
    )

    await expect(
      handleRelationshipTraversal(context, 'onet-graph', 'occupations', '11-1011', 'skills')
    ).rejects.toThrow('Invalid skip: must be non-negative')
  })

  it('should throw error for limit exceeding maximum', async () => {
    const worker = createMockWorker({
      getRelationships: vi.fn().mockResolvedValue([]),
      getStorageStats: vi.fn().mockReturnValue({
        cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
      }),
    })

    const context = createContext(
      'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011/skills?limit=10000',
      { worker }
    )

    await expect(
      handleRelationshipTraversal(context, 'onet-graph', 'occupations', '11-1011', 'skills')
    ).rejects.toThrow('Invalid limit: cannot exceed 1000')
  })

  it('should pass predicate filter to worker.getRelationships', async () => {
    const worker = createMockWorker({
      getRelationships: vi.fn().mockResolvedValue([]),
      getStorageStats: vi.fn().mockReturnValue({
        cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
      }),
    })

    const context = createContext(
      'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011/abilities',
      { worker }
    )

    await handleRelationshipTraversal(
      context, 'onet-graph', 'occupations', '11-1011', 'abilities'
    )

    expect(worker.getRelationships).toHaveBeenCalledWith('onet-graph', '11-1011', 'abilities', {
      matchMode: undefined,
      minSimilarity: undefined,
      maxSimilarity: undefined,
    })
  })

  it('should include correct links in response', async () => {
    const worker = createMockWorker({
      getRelationships: vi.fn().mockResolvedValue([]),
      getStorageStats: vi.fn().mockReturnValue({
        cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
      }),
    })

    const context = createContext(
      'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011/skills',
      { worker }
    )

    const response = await handleRelationshipTraversal(
      context, 'onet-graph', 'occupations', '11-1011', 'skills'
    )

    const body = await response.json() as Record<string, unknown>
    const links = body.links as Record<string, string>
    expect(links.self).toContain('/datasets/onet-graph/occupations/')
    expect(links.self).toContain('/skills')
    expect(links.entity).toContain('/datasets/onet-graph/occupations/')
  })

  it('should return 404 with proper error format when parquet file is missing', async () => {
    const worker = createMockWorker({
      getRelationships: vi.fn().mockRejectedValue(
        new Error('File not found: onet-graph/rels.parquet')
      ),
      getStorageStats: vi.fn().mockReturnValue({
        cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
      }),
    })

    const context = createContext(
      'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011/skills',
      { worker }
    )

    const response = await handleRelationshipTraversal(
      context, 'onet-graph', 'occupations', '11-1011', 'skills'
    )

    expect(response.status).toBe(404)
    const body = await response.json() as Record<string, unknown>
    const api = body.api as Record<string, unknown>
    expect(api.error).toBe(true)
    expect(api.code).toBe('DATASET_NOT_FOUND')
    expect(api.message).toContain('Dataset file not found')
    expect(api.hint).toBe('This collection may not have been uploaded yet.')
  })
})

// =============================================================================
// File Not Found Error Handling (404 instead of 500)
// =============================================================================

describe('File Not Found Error Handling', () => {
  describe('handleCollectionList', () => {
    it('should return 404 when parquet file is missing', async () => {
      const worker = createMockWorker({
        find: vi.fn().mockRejectedValue(
          new Error('File not found: imdb/titles.parquet')
        ),
      })
      const context = createContext(
        'https://test.parquedb.com/datasets/imdb/titles',
        { worker }
      )

      const response = await handleCollectionList(context, 'imdb', 'titles')

      expect(response.status).toBe(404)
      const body = await response.json() as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.error).toBe(true)
      expect(api.code).toBe('DATASET_NOT_FOUND')
      expect(api.message).toBe('Dataset file not found: imdb/titles.parquet')
      expect(api.hint).toBe('This collection may not have been uploaded yet.')
    })

    it('should re-throw non file-not-found errors', async () => {
      const worker = createMockWorker({
        find: vi.fn().mockRejectedValue(new Error('Some other error')),
      })
      const context = createContext(
        'https://test.parquedb.com/datasets/imdb/titles',
        { worker }
      )

      await expect(handleCollectionList(context, 'imdb', 'titles')).rejects.toThrow('Some other error')
    })

    it('should extract file path from error message', async () => {
      const worker = createMockWorker({
        find: vi.fn().mockRejectedValue(
          new Error('File not found: onet-graph/occupations.parquet')
        ),
      })
      const context = createContext(
        'https://test.parquedb.com/datasets/onet-graph/occupations',
        { worker }
      )

      const response = await handleCollectionList(context, 'onet-graph', 'occupations')

      expect(response.status).toBe(404)
      const body = await response.json() as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.message).toBe('Dataset file not found: onet-graph/occupations.parquet')
    })

    it('should use default path when file path cannot be extracted', async () => {
      const worker = createMockWorker({
        find: vi.fn().mockRejectedValue(
          new Error('File not found')
        ),
      })
      const context = createContext(
        'https://test.parquedb.com/datasets/imdb/titles',
        { worker }
      )

      const response = await handleCollectionList(context, 'imdb', 'titles')

      expect(response.status).toBe(404)
      const body = await response.json() as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      // Should fallback to the default path constructed from namespace
      expect(api.message).toBe('Dataset file not found: imdb/titles.parquet')
    })
  })

  describe('handleRelationshipTraversal', () => {
    it('should return 404 when rels parquet file is missing', async () => {
      const worker = createMockWorker({
        getRelationships: vi.fn().mockRejectedValue(
          new Error('File not found: onet-graph/rels.parquet')
        ),
        getStorageStats: vi.fn().mockReturnValue({
          cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
        }),
      })
      const context = createContext(
        'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011/skills',
        { worker }
      )

      const response = await handleRelationshipTraversal(
        context, 'onet-graph', 'occupations', '11-1011', 'skills'
      )

      expect(response.status).toBe(404)
      const body = await response.json() as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.error).toBe(true)
      expect(api.code).toBe('DATASET_NOT_FOUND')
      expect(api.message).toBe('Dataset file not found: onet-graph/rels.parquet')
      expect(api.hint).toBe('This collection may not have been uploaded yet.')
    })

    it('should re-throw non file-not-found errors', async () => {
      const worker = createMockWorker({
        getRelationships: vi.fn().mockRejectedValue(new Error('Connection timeout')),
        getStorageStats: vi.fn().mockReturnValue({
          cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
        }),
      })
      const context = createContext(
        'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011/skills',
        { worker }
      )

      await expect(
        handleRelationshipTraversal(context, 'onet-graph', 'occupations', '11-1011', 'skills')
      ).rejects.toThrow('Connection timeout')
    })
  })

  describe('handleEntityDetail', () => {
    it('should return 404 when parquet file is missing', async () => {
      const worker = createMockWorker({
        find: vi.fn().mockRejectedValue(
          new Error('File not found: onet-graph/occupations.parquet')
        ),
        getRelationships: vi.fn().mockResolvedValue([]),
        getStorageStats: vi.fn().mockReturnValue({
          cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
        }),
      })
      const context = createContext(
        'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011',
        { worker }
      )

      const response = await handleEntityDetail(
        context, 'onet-graph', 'occupations', '11-1011'
      )

      expect(response.status).toBe(404)
      const body = await response.json() as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.error).toBe(true)
      expect(api.code).toBe('DATASET_NOT_FOUND')
      expect(api.message).toBe('Dataset file not found: onet-graph/occupations.parquet')
      expect(api.hint).toBe('This collection may not have been uploaded yet.')
    })

    it('should re-throw non file-not-found errors', async () => {
      const worker = createMockWorker({
        find: vi.fn().mockRejectedValue(new Error('Database connection failed')),
        getRelationships: vi.fn().mockResolvedValue([]),
        getStorageStats: vi.fn().mockReturnValue({
          cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
        }),
      })
      const context = createContext(
        'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011',
        { worker }
      )

      await expect(
        handleEntityDetail(context, 'onet-graph', 'occupations', '11-1011')
      ).rejects.toThrow('Database connection failed')
    })

    it('should return 404 when relationship file is missing', async () => {
      const worker = createMockWorker({
        find: vi.fn().mockResolvedValue({ items: [{ $id: 'occupations/11-1011', name: 'Test' }], hasMore: false, stats: {} }),
        getRelationships: vi.fn().mockRejectedValue(
          new Error('File not found: onet-graph/rels.parquet')
        ),
        getStorageStats: vi.fn().mockReturnValue({
          cdnHits: 0, primaryHits: 0, edgeHits: 0, cacheHits: 0, totalReads: 0, usingCdn: false, usingEdge: false,
        }),
      })
      const context = createContext(
        'https://test.parquedb.com/datasets/onet-graph/occupations/11-1011',
        { worker }
      )

      const response = await handleEntityDetail(
        context, 'onet-graph', 'occupations', '11-1011'
      )

      expect(response.status).toBe(404)
      const body = await response.json() as Record<string, unknown>
      const api = body.api as Record<string, unknown>
      expect(api.error).toBe(true)
      expect(api.code).toBe('DATASET_NOT_FOUND')
      expect(api.message).toBe('Dataset file not found: onet-graph/rels.parquet')
    })
  })
})
