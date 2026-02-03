/**
 * Search Client Unit Tests
 *
 * Comprehensive tests for the ParqueDB Search client module.
 * Tests cover:
 * - Proxy-based client API (search.imdb, search.onet, etc.)
 * - createSearchClient factory with custom datasets
 * - Full-text search queries
 * - Vector search
 * - Hybrid search
 * - Suggest/autocomplete
 * - Filter options
 * - Error handling (network errors, invalid responses)
 * - URL building logic
 * - Type safety (compile-time checks)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  // Default client (proxy-based)
  search,

  // Client factory for custom datasets
  createSearchClient,

  // Standalone functions (tree-shakable)
  query,
  suggest,
  vectorSearch,
  hybridSearch,

  // Built-in schemas
  IMDBSchema,
  ONETSchema,
  UNSPSCSchema,

  // Types
  type SearchResult,
  type SuggestResult,
  type SearchOptions,
  type SearchClientConfig,
  type DatasetClient,
  type IMDBTitle,
  type IMDBFilters,
  type ONETOccupation,
  type ONETFilters,
  type UNSPSCCode,
  type UNSPSCFilters,
} from '../../../src/search/client'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a mock search response
 */
function createMockSearchResponse<T>(data: T[], options?: {
  total?: number
  limit?: number
  offset?: number
  facets?: Record<string, { value: string; count: number }[]>
  stats?: Record<string, { min: number; max: number; avg: number }>
  timing?: Record<string, number>
  didYouMean?: string
}): SearchResult<T> {
  return {
    data,
    total: options?.total ?? data.length,
    limit: options?.limit ?? 20,
    offset: options?.offset ?? 0,
    facets: options?.facets,
    stats: options?.stats,
    timing: options?.timing,
    didYouMean: options?.didYouMean,
  }
}

/**
 * Create a mock suggest response
 */
function createMockSuggestResponse(suggestions: string[], query: string): SuggestResult {
  return { suggestions, query }
}

/**
 * Sample IMDB data for testing
 */
const sampleIMDBData: IMDBTitle[] = [
  {
    tconst: 'tt0133093',
    titleType: 'movie',
    primaryTitle: 'The Matrix',
    originalTitle: 'The Matrix',
    isAdult: false,
    startYear: 1999,
    endYear: null,
    runtimeMinutes: 136,
    genres: ['Action', 'Sci-Fi'],
  },
  {
    tconst: 'tt0234215',
    titleType: 'movie',
    primaryTitle: 'The Matrix Reloaded',
    originalTitle: 'The Matrix Reloaded',
    isAdult: false,
    startYear: 2003,
    endYear: null,
    runtimeMinutes: 138,
    genres: ['Action', 'Sci-Fi'],
  },
]

/**
 * Sample ONET data for testing
 */
const sampleONETData: ONETOccupation[] = [
  {
    code: '15-1251.00',
    title: 'Computer Programmers',
    description: 'Create, modify, and test the code and scripts that allow computer applications to run.',
  },
  {
    code: '15-1252.00',
    title: 'Software Developers',
    description: 'Research, design, and develop computer and network software.',
  },
]

/**
 * Sample UNSPSC data for testing
 */
const sampleUNSPSCData: UNSPSCCode[] = [
  {
    commodityCode: '43211501',
    commodityTitle: 'Desktop computers',
    classCode: '432115',
    classTitle: 'Computers',
    familyCode: '4321',
    familyTitle: 'Computer equipment',
    segmentCode: '43',
    segmentTitle: 'Information technology broadcasting and telecommunications',
  },
]

// Store original fetch
const originalFetch = globalThis.fetch

// =============================================================================
// Setup and Teardown
// =============================================================================

describe('Search Client', () => {
  let mockFetch: ReturnType<typeof vi.fn>

  beforeEach(() => {
    mockFetch = vi.fn()
    globalThis.fetch = mockFetch
  })

  afterEach(() => {
    globalThis.fetch = originalFetch
    vi.restoreAllMocks()
  })

  // ===========================================================================
  // Proxy-based Client API Tests
  // ===========================================================================

  describe('Proxy-based client API', () => {
    describe('search.imdb', () => {
      it('performs full-text search', async () => {
        const mockResponse = createMockSearchResponse(sampleIMDBData)
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => mockResponse,
        })

        const result = await search.imdb('matrix')

        expect(mockFetch).toHaveBeenCalledTimes(1)
        expect(result.data).toHaveLength(2)
        expect(result.data[0].primaryTitle).toBe('The Matrix')
      })

      it('builds correct URL for search query', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('matrix')

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('imdb')
        expect(calledUrl).toContain('q=matrix')
      })

      it('supports browse mode without query', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse(sampleIMDBData),
        })

        const result = await search.imdb({ limit: 10 })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('limit=10')
        expect(calledUrl).not.toContain('q=')
        expect(result.data).toHaveLength(2)
      })

      it('supports filters with search query', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([sampleIMDBData[0]]),
        })

        await search.imdb('matrix', { type: 'movie', year_gte: 1999 })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('q=matrix')
        expect(calledUrl).toContain('type=movie')
        expect(calledUrl).toContain('year_gte=1999')
      })
    })

    describe('search.onet', () => {
      it('performs full-text search on occupations', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse(sampleONETData),
        })

        const result = await search.onet('software')

        expect(result.data).toHaveLength(2)
        expect(result.data[1].title).toBe('Software Developers')
      })

      it('supports code filter', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([sampleONETData[0]]),
        })

        await search.onet('programmer', { code: '15-1251.00' })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('code=15-1251.00')
      })
    })

    describe('search.unspsc', () => {
      it('performs full-text search on commodity codes', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse(sampleUNSPSCData),
        })

        const result = await search.unspsc('computer')

        expect(result.data).toHaveLength(1)
        expect(result.data[0].commodityTitle).toBe('Desktop computers')
      })

      it('supports segment filter', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse(sampleUNSPSCData),
        })

        await search.unspsc('desktop', { segment: '43' })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('segment=43')
      })
    })

    describe('Proxy access for any dataset', () => {
      it('creates client for any dataset name via proxy', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        // Access a built-in dataset
        const result = await search.imdb('test')

        expect(mockFetch).toHaveBeenCalled()
        expect(result.data).toEqual([])
      })
    })
  })

  // ===========================================================================
  // createSearchClient Factory Tests
  // ===========================================================================

  describe('createSearchClient factory', () => {
    it('creates client with default base URL', async () => {
      const client = createSearchClient<{
        products: [{ id: string; name: string }, { category?: string }]
      }>()

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([{ id: '1', name: 'Product' }]),
      })

      await client.products('test')

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('cdn.workers.do/search-v10')
    })

    it('creates client with custom base URL', async () => {
      const client = createSearchClient<{
        products: [{ id: string }, {}]
      }>({ baseUrl: 'https://api.example.com/search' })

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await client.products('test')

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('api.example.com/search')
    })

    it('supports multiple custom datasets', async () => {
      interface Product {
        id: string
        name: string
        price: number
      }
      interface ProductFilters {
        category?: string | undefined
        price_gte?: number | undefined
        price_lte?: number | undefined
      }

      interface Category {
        id: string
        name: string
      }
      interface CategoryFilters {
        parent?: string | undefined
      }

      const client = createSearchClient<{
        products: [Product, ProductFilters]
        categories: [Category, CategoryFilters]
      }>({ baseUrl: 'https://api.example.com' })

      // Test products dataset
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([{ id: '1', name: 'Laptop', price: 999 }]),
      })

      const products = await client.products('laptop', { category: 'electronics', price_lte: 1000 })
      expect(products.data[0].name).toBe('Laptop')

      // Test categories dataset
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([{ id: 'cat1', name: 'Electronics' }]),
      })

      const categories = await client.categories('electronics', { parent: 'root' })
      expect(categories.data[0].name).toBe('Electronics')
    })

    it('handles trailing slash in base URL', async () => {
      const clientWithSlash = createSearchClient<{
        test: [{ id: string }, {}]
      }>({ baseUrl: 'https://api.example.com/search/' })

      const clientWithoutSlash = createSearchClient<{
        test: [{ id: string }, {}]
      }>({ baseUrl: 'https://api.example.com/search' })

      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await clientWithSlash.test('query')
      await clientWithoutSlash.test('query')

      // Both should produce valid URLs
      const url1 = mockFetch.mock.calls[0][0]
      const url2 = mockFetch.mock.calls[1][0]

      expect(url1).toContain('test')
      expect(url2).toContain('test')
    })
  })

  // ===========================================================================
  // Full-text Search Tests
  // ===========================================================================

  describe('Full-text search queries', () => {
    it('sends query as q parameter', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('science fiction movies')

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('q=science+fiction+movies')
    })

    it('handles special characters in query', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('movie & tv shows')

      const calledUrl = mockFetch.mock.calls[0][0]
      // URL should have encoded the special characters
      expect(calledUrl).toContain('q=')
      expect(mockFetch).toHaveBeenCalled()
    })

    it('returns facets when requested', async () => {
      const mockResponse = createMockSearchResponse(sampleIMDBData, {
        facets: {
          titleType: [
            { value: 'movie', count: 100 },
            { value: 'tvSeries', count: 50 },
          ],
        },
      })

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      })

      const result = await search.imdb('action', { facets: ['titleType'] })

      expect(result.facets).toBeDefined()
      expect(result.facets?.titleType).toHaveLength(2)
    })

    it('returns stats when requested', async () => {
      const mockResponse = createMockSearchResponse(sampleIMDBData, {
        stats: {
          startYear: { min: 1990, max: 2023, avg: 2010 },
        },
      })

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      })

      const result = await search.imdb('drama', { stats: ['startYear'] })

      expect(result.stats).toBeDefined()
      expect(result.stats?.startYear.min).toBe(1990)
    })

    it('returns didYouMean suggestions', async () => {
      const mockResponse = createMockSearchResponse([], {
        didYouMean: 'matrix',
      })

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      })

      const result = await search.imdb('matirx')

      expect(result.didYouMean).toBe('matrix')
    })

    it('returns timing information when requested', async () => {
      const mockResponse = createMockSearchResponse(sampleIMDBData, {
        timing: {
          search: 15,
          facets: 5,
          total: 20,
        },
      })

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      })

      const result = await search.imdb('action', { timing: true })

      expect(result.timing).toBeDefined()
      expect(result.timing?.total).toBe(20)
    })
  })

  // ===========================================================================
  // Vector Search Tests
  // ===========================================================================

  describe('Vector search', () => {
    const mockEmbedding = new Array(768).fill(0.1)

    it('performs vector search with embedding', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse(sampleIMDBData),
      })

      const result = await search.imdb.vector(mockEmbedding)

      expect(result.data).toHaveLength(2)
      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('mode=vector')
    })

    it('includes vector parameter in URL', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb.vector(mockEmbedding)

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('vector=')
    })

    it('supports limit and offset options', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb.vector(mockEmbedding, { limit: 5, offset: 10 })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('limit=5')
      expect(calledUrl).toContain('offset=10')
    })

    it('supports sorting options', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb.vector(mockEmbedding, { sort: '_score' })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('sort=_score')
    })
  })

  // ===========================================================================
  // Hybrid Search Tests
  // ===========================================================================

  describe('Hybrid search', () => {
    const mockEmbedding = new Array(768).fill(0.1)

    it('combines text query and vector search', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse(sampleIMDBData),
      })

      const result = await search.imdb.hybrid('action movies', mockEmbedding)

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('q=action+movies')
      expect(calledUrl).toContain('vector=')
      expect(calledUrl).toContain('mode=hybrid')
      expect(result.data).toHaveLength(2)
    })

    it('supports fts_weight option', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb.hybrid('sci-fi', mockEmbedding, { fts_weight: 0.7 })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('fts_weight=0.7')
    })

    it('supports filters in hybrid search', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb.hybrid('love story', mockEmbedding, { type: 'movie', year_gte: 2000 })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('type=movie')
      expect(calledUrl).toContain('year_gte=2000')
    })

    it('supports pagination in hybrid search', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb.hybrid('comedy', mockEmbedding, { limit: 20, offset: 40 })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('limit=20')
      expect(calledUrl).toContain('offset=40')
    })
  })

  // ===========================================================================
  // Suggest/Autocomplete Tests
  // ===========================================================================

  describe('Suggest/autocomplete', () => {
    it('returns suggestions for prefix', async () => {
      const mockResponse = createMockSuggestResponse(
        ['matrix', 'matilda', 'matinee'],
        'mat'
      )

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      })

      const result = await search.imdb.suggest('mat')

      expect(result.suggestions).toHaveLength(3)
      expect(result.suggestions).toContain('matrix')
      expect(result.query).toBe('mat')
    })

    it('builds correct URL for suggest endpoint', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSuggestResponse([], 'test'),
      })

      await search.imdb.suggest('test')

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('suggest/imdb')
      expect(calledUrl).toContain('q=test')
    })

    it('handles empty suggestions', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSuggestResponse([], 'xyz'),
      })

      const result = await search.imdb.suggest('xyz')

      expect(result.suggestions).toEqual([])
    })

    it('works with different datasets', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSuggestResponse(['software developer', 'software engineer'], 'soft'),
      })

      const result = await search.onet.suggest('soft')

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('suggest/onet')
      expect(result.suggestions).toContain('software developer')
    })
  })

  // ===========================================================================
  // Filter Options Tests
  // ===========================================================================

  describe('Filter options', () => {
    describe('IMDB filters', () => {
      it('supports type filter as string', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('action', { type: 'movie' })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('type=movie')
      })

      it('supports type filter as array', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('drama', { type: ['movie', 'tvSeries'] })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('type=movie%2CtvSeries')
      })

      it('supports year_gte filter', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('comedy', { year_gte: 2020 })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('year_gte=2020')
      })

      it('supports year_lte filter', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('classic', { year_lte: 1970 })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('year_lte=1970')
      })

      it('supports runtime filters', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('short film', { runtime_gte: 60, runtime_lte: 120 })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('runtime_gte=60')
        expect(calledUrl).toContain('runtime_lte=120')
      })

      it('supports combining multiple filters', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('thriller', {
          type: 'movie',
          year_gte: 2010,
          year_lte: 2020,
          runtime_gte: 90,
        })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('type=movie')
        expect(calledUrl).toContain('year_gte=2010')
        expect(calledUrl).toContain('year_lte=2020')
        expect(calledUrl).toContain('runtime_gte=90')
      })
    })

    describe('Search options', () => {
      it('supports limit option', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([], { limit: 50 }),
        })

        await search.imdb('test', { limit: 50 })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('limit=50')
      })

      it('supports offset option', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([], { offset: 100 }),
        })

        await search.imdb('test', { offset: 100 })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('offset=100')
      })

      it('supports sort option', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('test', { sort: 'startYear:desc' })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('sort=startYear%3Adesc')
      })

      it('supports facets option', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('test', { facets: ['titleType', 'genres'] })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('facets=titleType%2Cgenres')
      })

      it('supports stats option', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('test', { stats: ['startYear', 'runtimeMinutes'] })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('stats=startYear%2CruntimeMinutes')
      })

      it('supports timing option', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('test', { timing: true })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('timing=true')
      })

      it('supports highlight option', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('test', { highlight: true })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('highlight=true')
      })
    })

    describe('Filter value handling', () => {
      it('ignores undefined filter values', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('test', { type: undefined, year_gte: 2000 })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).not.toContain('type=')
        expect(calledUrl).toContain('year_gte=2000')
      })

      it('ignores null filter values', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await search.imdb('test', { type: null as any, year_gte: 2000 })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).not.toContain('type=')
        expect(calledUrl).toContain('year_gte=2000')
      })
    })
  })

  // ===========================================================================
  // Error Handling Tests
  // ===========================================================================

  describe('Error handling', () => {
    describe('Network errors', () => {
      it('throws on network failure', async () => {
        mockFetch.mockRejectedValueOnce(new Error('Network error'))

        await expect(search.imdb('test')).rejects.toThrow('Network error')
      })

      it('throws on timeout', async () => {
        mockFetch.mockRejectedValueOnce(new Error('Timeout'))

        await expect(search.imdb('test')).rejects.toThrow('Timeout')
      })

      it('propagates fetch errors', async () => {
        const customError = new Error('DNS resolution failed')
        mockFetch.mockRejectedValueOnce(customError)

        await expect(search.imdb('test')).rejects.toBe(customError)
      })
    })

    describe('HTTP errors', () => {
      it('throws on 400 Bad Request', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: false,
          status: 400,
        })

        await expect(search.imdb('test')).rejects.toThrow('Search error: 400')
      })

      it('throws on 401 Unauthorized', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: false,
          status: 401,
        })

        await expect(search.imdb('test')).rejects.toThrow('Search error: 401')
      })

      it('throws on 403 Forbidden', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: false,
          status: 403,
        })

        await expect(search.imdb('test')).rejects.toThrow('Search error: 403')
      })

      it('throws on 404 Not Found', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: false,
          status: 404,
        })

        await expect(search.imdb('test')).rejects.toThrow('Search error: 404')
      })

      it('throws on 429 Too Many Requests', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: false,
          status: 429,
        })

        await expect(search.imdb('test')).rejects.toThrow('Search error: 429')
      })

      it('throws on 500 Internal Server Error', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: false,
          status: 500,
        })

        await expect(search.imdb('test')).rejects.toThrow('Search error: 500')
      })

      it('throws on 502 Bad Gateway', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: false,
          status: 502,
        })

        await expect(search.imdb('test')).rejects.toThrow('Search error: 502')
      })

      it('throws on 503 Service Unavailable', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: false,
          status: 503,
        })

        await expect(search.imdb('test')).rejects.toThrow('Search error: 503')
      })
    })

    describe('Invalid response handling', () => {
      it('throws on invalid JSON response', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => {
            throw new SyntaxError('Invalid JSON')
          },
        })

        await expect(search.imdb('test')).rejects.toThrow()
      })

      it('handles empty response body', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => null,
        })

        const result = await search.imdb('test')
        expect(result).toBeNull()
      })
    })

    describe('Error handling in suggest', () => {
      it('throws on network failure in suggest', async () => {
        mockFetch.mockRejectedValueOnce(new Error('Network error'))

        await expect(search.imdb.suggest('test')).rejects.toThrow('Network error')
      })

      it('throws on HTTP error in suggest', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: false,
          status: 500,
        })

        await expect(search.imdb.suggest('test')).rejects.toThrow('Search error: 500')
      })
    })

    describe('Error handling in vector search', () => {
      it('throws on network failure in vector search', async () => {
        mockFetch.mockRejectedValueOnce(new Error('Network error'))

        await expect(search.imdb.vector([0.1, 0.2])).rejects.toThrow('Network error')
      })

      it('throws on HTTP error in vector search', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: false,
          status: 500,
        })

        await expect(search.imdb.vector([0.1, 0.2])).rejects.toThrow('Search error: 500')
      })
    })

    describe('Error handling in hybrid search', () => {
      it('throws on network failure in hybrid search', async () => {
        mockFetch.mockRejectedValueOnce(new Error('Network error'))

        await expect(search.imdb.hybrid('test', [0.1, 0.2])).rejects.toThrow('Network error')
      })

      it('throws on HTTP error in hybrid search', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: false,
          status: 500,
        })

        await expect(search.imdb.hybrid('test', [0.1, 0.2])).rejects.toThrow('Search error: 500')
      })
    })
  })

  // ===========================================================================
  // URL Building Logic Tests
  // ===========================================================================

  describe('URL building logic', () => {
    it('correctly builds base URL with dataset path', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test')

      const calledUrl = mockFetch.mock.calls[0][0]
      const url = new URL(calledUrl)
      expect(url.pathname).toContain('imdb')
    })

    it('correctly encodes query parameters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test query')

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('q=test+query')
    })

    it('correctly handles array parameters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test', { facets: ['a', 'b', 'c'] })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('facets=a%2Cb%2Cc')
    })

    it('correctly handles boolean parameters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test', { timing: true, highlight: false })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('timing=true')
      expect(calledUrl).toContain('highlight=false')
    })

    it('correctly handles number parameters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test', { limit: 100, offset: 50 })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('limit=100')
      expect(calledUrl).toContain('offset=50')
    })

    it('builds correct suggest URL', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSuggestResponse([], 'test'),
      })

      await search.imdb.suggest('test')

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('suggest/imdb')
    })

    it('preserves base URL structure', async () => {
      const client = createSearchClient<{
        test: [{ id: string }, {}]
      }>({ baseUrl: 'https://api.example.com/v2/search' })

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await client.test('query')

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('api.example.com')
      expect(calledUrl).toContain('test')
    })
  })

  // ===========================================================================
  // Standalone Functions Tests (Tree-Shakable)
  // ===========================================================================

  describe('Standalone functions (tree-shakable)', () => {
    describe('query()', () => {
      it('performs search on specified dataset', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse(sampleIMDBData),
        })

        const result = await query<IMDBTitle>('imdb', 'matrix')

        expect(result.data).toHaveLength(2)
        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('imdb')
        expect(calledUrl).toContain('q=matrix')
      })

      it('supports filters', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await query('imdb', 'test', { limit: 10, offset: 20 })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('limit=10')
        expect(calledUrl).toContain('offset=20')
      })

      it('supports custom base URL', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await query('products', 'laptop', {}, 'https://custom.api.com/search')

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('custom.api.com/search')
      })
    })

    describe('suggest()', () => {
      it('performs autocomplete on specified dataset', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSuggestResponse(['matrix'], 'mat'),
        })

        const result = await suggest('imdb', 'mat')

        expect(result.suggestions).toContain('matrix')
        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('suggest/imdb')
      })

      it('supports custom base URL', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSuggestResponse([], 'test'),
        })

        await suggest('products', 'lap', 'https://custom.api.com/search')

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('custom.api.com/search')
      })
    })

    describe('vectorSearch()', () => {
      it('performs vector search on specified dataset', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse(sampleIMDBData),
        })

        const result = await vectorSearch<IMDBTitle>('imdb', [0.1, 0.2, 0.3])

        expect(result.data).toHaveLength(2)
        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('mode=vector')
      })

      it('supports options', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await vectorSearch('imdb', [0.1], { limit: 5 })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('limit=5')
      })

      it('supports custom base URL', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await vectorSearch('products', [0.1], {}, 'https://custom.api.com/search')

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('custom.api.com/search')
      })
    })

    describe('hybridSearch()', () => {
      it('performs hybrid search on specified dataset', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse(sampleIMDBData),
        })

        const result = await hybridSearch<IMDBTitle>('imdb', 'action', [0.1, 0.2, 0.3])

        expect(result.data).toHaveLength(2)
        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('mode=hybrid')
        expect(calledUrl).toContain('q=action')
      })

      it('supports fts_weight option', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await hybridSearch('imdb', 'test', [0.1], { fts_weight: 0.8 })

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('fts_weight=0.8')
      })

      it('supports custom base URL', async () => {
        mockFetch.mockResolvedValueOnce({
          ok: true,
          json: async () => createMockSearchResponse([]),
        })

        await hybridSearch('products', 'laptop', [0.1], {}, 'https://custom.api.com/search')

        const calledUrl = mockFetch.mock.calls[0][0]
        expect(calledUrl).toContain('custom.api.com/search')
      })
    })
  })

  // ===========================================================================
  // Type Safety Tests (Compile-time checks)
  // ===========================================================================

  describe('Type safety', () => {
    // These tests verify type correctness at compile-time
    // If the code compiles, the types are correct

    it('search.imdb returns typed IMDBTitle results', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse(sampleIMDBData),
      })

      const result = await search.imdb('matrix')

      // Type should be SearchResult<IMDBTitle>
      const firstItem: IMDBTitle = result.data[0]
      expect(firstItem.tconst).toBeDefined()
      expect(firstItem.primaryTitle).toBeDefined()
      expect(firstItem.genres).toBeInstanceOf(Array)
    })

    it('search.onet returns typed ONETOccupation results', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse(sampleONETData),
      })

      const result = await search.onet('programmer')

      // Type should be SearchResult<ONETOccupation>
      const firstItem: ONETOccupation = result.data[0]
      expect(firstItem.code).toBeDefined()
      expect(firstItem.title).toBeDefined()
      expect(firstItem.description).toBeDefined()
    })

    it('search.unspsc returns typed UNSPSCCode results', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse(sampleUNSPSCData),
      })

      const result = await search.unspsc('computer')

      // Type should be SearchResult<UNSPSCCode>
      const firstItem: UNSPSCCode = result.data[0]
      expect(firstItem.commodityCode).toBeDefined()
      expect(firstItem.segmentCode).toBeDefined()
    })

    it('createSearchClient provides typed access to custom datasets', async () => {
      interface CustomProduct {
        id: string
        name: string
        price: number
      }

      interface CustomFilters {
        minPrice?: number | undefined
        maxPrice?: number | undefined
      }

      const client = createSearchClient<{
        products: [CustomProduct, CustomFilters]
      }>()

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([{ id: '1', name: 'Test', price: 99 }]),
      })

      const result = await client.products('test', { minPrice: 50 })

      // Type should be SearchResult<CustomProduct>
      const firstItem: CustomProduct = result.data[0]
      expect(firstItem.id).toBeDefined()
      expect(firstItem.name).toBeDefined()
      expect(typeof firstItem.price).toBe('number')
    })

    it('IMDBFilters type-checks filter properties', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      // This should compile without errors
      const filters: IMDBFilters = {
        type: 'movie',
        year_gte: 2000,
        year_lte: 2020,
        runtime_gte: 90,
        runtime_lte: 180,
      }

      await search.imdb('test', filters)
      expect(mockFetch).toHaveBeenCalled()
    })

    it('SearchOptions type-checks common options', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      const options: SearchOptions = {
        limit: 20,
        offset: 0,
        sort: 'startYear:desc',
        facets: ['titleType'],
        stats: ['startYear'],
        timing: true,
        highlight: true,
      }

      await search.imdb('test', options)
      expect(mockFetch).toHaveBeenCalled()
    })

    it('DatasetClient interface is correctly implemented', async () => {
      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      // search.imdb should implement DatasetClient<IMDBTitle, IMDBFilters>
      const client: DatasetClient<IMDBTitle, IMDBFilters> = search.imdb

      // Test callable signature
      await client('matrix')
      await client({ type: 'movie' })

      // Test methods
      await client.suggest('mat')
      await client.vector([0.1, 0.2])
      await client.hybrid('action', [0.1, 0.2])

      expect(mockFetch).toHaveBeenCalledTimes(5)
    })
  })

  // ===========================================================================
  // Built-in Schema Tests
  // ===========================================================================

  describe('Built-in schemas', () => {
    describe('IMDBSchema', () => {
      it('defines all expected fields', () => {
        expect(IMDBSchema.tconst).toBeDefined()
        expect(IMDBSchema.titleType).toBeDefined()
        expect(IMDBSchema.primaryTitle).toBeDefined()
        expect(IMDBSchema.originalTitle).toBeDefined()
        expect(IMDBSchema.isAdult).toBeDefined()
        expect(IMDBSchema.startYear).toBeDefined()
        expect(IMDBSchema.endYear).toBeDefined()
        expect(IMDBSchema.runtimeMinutes).toBeDefined()
        expect(IMDBSchema.genres).toBeDefined()
      })

      it('has correct field types', () => {
        expect(IMDBSchema.tconst.type).toBe('keyword')
        expect(IMDBSchema.titleType.type).toBe('keyword')
        expect(IMDBSchema.primaryTitle.type).toBe('text')
        expect(IMDBSchema.isAdult.type).toBe('boolean')
        expect(IMDBSchema.startYear.type).toBe('number')
        expect(IMDBSchema.genres.type).toBe('array')
      })

      it('marks filterable fields correctly', () => {
        expect(IMDBSchema.tconst.filterable).toBe(true)
        expect(IMDBSchema.titleType.filterable).toBe(true)
        expect(IMDBSchema.isAdult.filterable).toBe(true)
        expect(IMDBSchema.startYear.filterable).toBe(true)
      })

      it('marks facetable fields correctly', () => {
        expect(IMDBSchema.titleType.facetable).toBe(true)
        expect(IMDBSchema.startYear.facetable).toBe(true)
        expect(IMDBSchema.genres.facetable).toBe(true)
      })

      it('marks sortable fields correctly', () => {
        expect(IMDBSchema.startYear.sortable).toBe(true)
        expect(IMDBSchema.runtimeMinutes.sortable).toBe(true)
      })
    })

    describe('ONETSchema', () => {
      it('defines all expected fields', () => {
        expect(ONETSchema.code).toBeDefined()
        expect(ONETSchema.title).toBeDefined()
        expect(ONETSchema.description).toBeDefined()
      })

      it('has correct field types', () => {
        expect(ONETSchema.code.type).toBe('keyword')
        expect(ONETSchema.title.type).toBe('text')
        expect(ONETSchema.description.type).toBe('text')
      })

      it('marks filterable fields correctly', () => {
        expect(ONETSchema.code.filterable).toBe(true)
      })
    })

    describe('UNSPSCSchema', () => {
      it('defines all expected fields', () => {
        expect(UNSPSCSchema.commodityCode).toBeDefined()
        expect(UNSPSCSchema.commodityTitle).toBeDefined()
        expect(UNSPSCSchema.classCode).toBeDefined()
        expect(UNSPSCSchema.classTitle).toBeDefined()
        expect(UNSPSCSchema.familyCode).toBeDefined()
        expect(UNSPSCSchema.familyTitle).toBeDefined()
        expect(UNSPSCSchema.segmentCode).toBeDefined()
        expect(UNSPSCSchema.segmentTitle).toBeDefined()
      })

      it('has correct field types', () => {
        expect(UNSPSCSchema.commodityCode.type).toBe('keyword')
        expect(UNSPSCSchema.commodityTitle.type).toBe('text')
        expect(UNSPSCSchema.classCode.type).toBe('keyword')
        expect(UNSPSCSchema.segmentCode.type).toBe('keyword')
      })

      it('marks filterable fields correctly', () => {
        expect(UNSPSCSchema.commodityCode.filterable).toBe(true)
        expect(UNSPSCSchema.classCode.filterable).toBe(true)
        expect(UNSPSCSchema.familyCode.filterable).toBe(true)
        expect(UNSPSCSchema.segmentCode.filterable).toBe(true)
      })
    })
  })

  // ===========================================================================
  // Response Structure Tests
  // ===========================================================================

  describe('Response structure', () => {
    it('SearchResult has required fields', async () => {
      const mockResponse: SearchResult<IMDBTitle> = {
        data: sampleIMDBData,
        total: 100,
        limit: 20,
        offset: 0,
      }

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      })

      const result = await search.imdb('matrix')

      expect(result.data).toBeDefined()
      expect(result.total).toBeDefined()
      expect(result.limit).toBeDefined()
      expect(result.offset).toBeDefined()
    })

    it('SearchResult includes optional fields when present', async () => {
      const mockResponse: SearchResult<IMDBTitle> = {
        data: sampleIMDBData,
        total: 100,
        limit: 20,
        offset: 0,
        facets: { titleType: [{ value: 'movie', count: 50 }] },
        stats: { startYear: { min: 1990, max: 2023, avg: 2010 } },
        timing: { total: 15 },
        didYouMean: 'suggestion',
      }

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      })

      const result = await search.imdb('test')

      expect(result.facets).toBeDefined()
      expect(result.stats).toBeDefined()
      expect(result.timing).toBeDefined()
      expect(result.didYouMean).toBeDefined()
    })

    it('SuggestResult has required fields', async () => {
      const mockResponse: SuggestResult = {
        suggestions: ['matrix', 'matilda'],
        query: 'mat',
      }

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      })

      const result = await search.imdb.suggest('mat')

      expect(result.suggestions).toBeDefined()
      expect(result.query).toBeDefined()
    })
  })

  // ===========================================================================
  // Edge Cases Tests
  // ===========================================================================

  describe('Edge cases', () => {
    it('handles empty search results', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      const result = await search.imdb('nonexistent query xyz123')

      expect(result.data).toEqual([])
      expect(result.total).toBe(0)
    })

    it('handles empty query string', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse(sampleIMDBData),
      })

      const result = await search.imdb('')

      expect(mockFetch).toHaveBeenCalled()
      // Empty query string is still passed but may result in browse mode
      // The important thing is the request is made successfully
      expect(result.data).toHaveLength(2)
    })

    it('handles very long query strings', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      const longQuery = 'a'.repeat(1000)
      await search.imdb(longQuery)

      expect(mockFetch).toHaveBeenCalled()
    })

    it('handles large embedding vectors', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      const largeEmbedding = new Array(1536).fill(0.1) // OpenAI ada-002 size
      await search.imdb.vector(largeEmbedding)

      expect(mockFetch).toHaveBeenCalled()
    })

    it('handles zero limit', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test', { limit: 0 })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('limit=0')
    })

    it('handles large offset', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test', { offset: 1000000 })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('offset=1000000')
    })

    it('handles Unicode characters in query', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('\u4e2d\u6587\u7535\u5f71') // Chinese characters

      expect(mockFetch).toHaveBeenCalled()
    })

    it('handles emoji in query', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('love movie')

      expect(mockFetch).toHaveBeenCalled()
    })

    it('handles negative numbers in filters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test', { year_gte: -500, year_lte: 0 })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('year_gte=-500')
      expect(calledUrl).toContain('year_lte=0')
    })

    it('handles decimal numbers in filters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test', { runtime_gte: 1.5, runtime_lte: 2.5 })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('runtime_gte=1.5')
      expect(calledUrl).toContain('runtime_lte=2.5')
    })

    it('handles empty array filter values', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test', { type: [] })

      const calledUrl = mockFetch.mock.calls[0][0]
      // Empty array should still be included as empty string
      expect(calledUrl).toContain('type=')
    })

    it('handles whitespace-only query', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('   ')

      expect(mockFetch).toHaveBeenCalled()
    })

    it('handles query with newlines', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('first line\nsecond line')

      expect(mockFetch).toHaveBeenCalled()
    })

    it('handles query with tabs', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('word1\tword2')

      expect(mockFetch).toHaveBeenCalled()
    })
  })

  // ===========================================================================
  // Range Filter Object Tests
  // ===========================================================================

  describe('Range filter objects', () => {
    it('handles object range filter with gte and lte', async () => {
      const client = createSearchClient<{
        products: [{ id: string; price: number }, { price?: { gte?: number; lte?: number } }]
      }>()

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await client.products('laptop', { price: { gte: 500, lte: 1500 } })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('price_gte=500')
      expect(calledUrl).toContain('price_lte=1500')
    })

    it('handles object range filter with only gte', async () => {
      const client = createSearchClient<{
        products: [{ id: string; price: number }, { price?: { gte?: number; lte?: number } }]
      }>()

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await client.products('laptop', { price: { gte: 500 } })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('price_gte=500')
      expect(calledUrl).not.toContain('price_lte')
    })

    it('handles object range filter with only lte', async () => {
      const client = createSearchClient<{
        products: [{ id: string; price: number }, { price?: { gte?: number; lte?: number } }]
      }>()

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await client.products('laptop', { price: { lte: 1000 } })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).not.toContain('price_gte')
      expect(calledUrl).toContain('price_lte=1000')
    })

    it('handles object range filter with undefined values', async () => {
      const client = createSearchClient<{
        products: [{ id: string; price: number }, { price?: { gte?: number; lte?: number } }]
      }>()

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await client.products('laptop', { price: { gte: 500, lte: undefined } })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('price_gte=500')
      expect(calledUrl).not.toContain('price_lte')
    })

    it('handles empty object range filter', async () => {
      const client = createSearchClient<{
        products: [{ id: string; price: number }, { price?: { gte?: number; lte?: number } }]
      }>()

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await client.products('laptop', { price: {} })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).not.toContain('price_gte')
      expect(calledUrl).not.toContain('price_lte')
    })

    it('handles multiple range filters', async () => {
      const client = createSearchClient<{
        products: [
          { id: string; price: number; rating: number },
          { price?: { gte?: number; lte?: number }; rating?: { gte?: number; lte?: number } }
        ]
      }>()

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await client.products('laptop', {
        price: { gte: 500, lte: 1500 },
        rating: { gte: 4 },
      })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('price_gte=500')
      expect(calledUrl).toContain('price_lte=1500')
      expect(calledUrl).toContain('rating_gte=4')
    })
  })

  // ===========================================================================
  // ONET and UNSPSC Advanced Filter Tests
  // ===========================================================================

  describe('ONET filters', () => {
    it('supports array of codes', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse(sampleONETData),
      })

      await search.onet('developer', { code: ['15-1251.00', '15-1252.00'] })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('code=15-1251.00%2C15-1252.00')
    })

    it('handles browse mode without query', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse(sampleONETData),
      })

      const result = await search.onet({ limit: 10 })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('limit=10')
      expect(result.data).toHaveLength(2)
    })
  })

  describe('UNSPSC filters', () => {
    it('supports multiple hierarchy filters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse(sampleUNSPSCData),
      })

      await search.unspsc('computer', { segment: '43', family: '4321' })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('segment=43')
      expect(calledUrl).toContain('family=4321')
    })

    it('supports array of segments', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.unspsc('equipment', { segment: ['43', '44'] })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('segment=43%2C44')
    })

    it('supports class and commodity filters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse(sampleUNSPSCData),
      })

      await search.unspsc('desktop', { class: '432115', commodity: '43211501' })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('class=432115')
      expect(calledUrl).toContain('commodity=43211501')
    })

    it('handles browse mode without query', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse(sampleUNSPSCData),
      })

      const result = await search.unspsc({ segment: '43', limit: 50 })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('segment=43')
      expect(calledUrl).toContain('limit=50')
      expect(result.data).toHaveLength(1)
    })
  })

  // ===========================================================================
  // Concurrent Request Tests
  // ===========================================================================

  describe('Concurrent requests', () => {
    it('handles multiple concurrent searches', async () => {
      vi.useFakeTimers()
      try {
        mockFetch.mockImplementation(async (url: string) => {
          // Simulate slight delay using fake timers
          await vi.advanceTimersByTimeAsync(10)
          return {
            ok: true,
            json: async () => createMockSearchResponse([]),
          }
        })

        const results = await Promise.all([
          search.imdb('matrix'),
          search.onet('developer'),
          search.unspsc('computer'),
        ])

        expect(mockFetch).toHaveBeenCalledTimes(3)
        expect(results).toHaveLength(3)
        results.forEach(result => {
          expect(result.data).toEqual([])
        })
      } finally {
        vi.useRealTimers()
      }
    })

    it('handles concurrent suggest and search', async () => {
      mockFetch.mockImplementation(async (url: string) => {
        if (url.includes('suggest')) {
          return {
            ok: true,
            json: async () => createMockSuggestResponse(['matrix'], 'mat'),
          }
        }
        return {
          ok: true,
          json: async () => createMockSearchResponse(sampleIMDBData),
        }
      })

      const [searchResult, suggestResult] = await Promise.all([
        search.imdb('matrix'),
        search.imdb.suggest('mat'),
      ])

      expect(searchResult.data).toHaveLength(2)
      expect(suggestResult.suggestions).toContain('matrix')
    })

    it('handles concurrent vector and hybrid searches', async () => {
      const mockEmbedding = [0.1, 0.2, 0.3]

      mockFetch.mockImplementation(async () => ({
        ok: true,
        json: async () => createMockSearchResponse(sampleIMDBData),
      }))

      const [vectorResult, hybridResult] = await Promise.all([
        search.imdb.vector(mockEmbedding),
        search.imdb.hybrid('action', mockEmbedding),
      ])

      expect(mockFetch).toHaveBeenCalledTimes(2)
      expect(vectorResult.data).toHaveLength(2)
      expect(hybridResult.data).toHaveLength(2)
    })

    it('isolates errors between concurrent requests', async () => {
      let callCount = 0
      mockFetch.mockImplementation(async () => {
        callCount++
        if (callCount === 2) {
          return { ok: false, status: 500 }
        }
        return {
          ok: true,
          json: async () => createMockSearchResponse([]),
        }
      })

      const results = await Promise.allSettled([
        search.imdb('query1'),
        search.imdb('query2'),
        search.imdb('query3'),
      ])

      expect(results[0].status).toBe('fulfilled')
      expect(results[1].status).toBe('rejected')
      expect(results[2].status).toBe('fulfilled')
    })
  })

  // ===========================================================================
  // Client Reuse Tests
  // ===========================================================================

  describe('Client reuse', () => {
    it('allows multiple calls to the same client', async () => {
      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('query1')
      await search.imdb('query2')
      await search.imdb('query3')

      expect(mockFetch).toHaveBeenCalledTimes(3)
    })

    it('maintains separate state for different custom clients', async () => {
      const client1 = createSearchClient<{
        test: [{ id: string }, {}]
      }>({ baseUrl: 'https://api1.example.com' })

      const client2 = createSearchClient<{
        test: [{ id: string }, {}]
      }>({ baseUrl: 'https://api2.example.com' })

      mockFetch.mockResolvedValue({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await client1.test('query')
      await client2.test('query')

      expect(mockFetch.mock.calls[0][0]).toContain('api1.example.com')
      expect(mockFetch.mock.calls[1][0]).toContain('api2.example.com')
    })
  })

  // ===========================================================================
  // Parameter Serialization Tests
  // ===========================================================================

  describe('Parameter serialization', () => {
    it('serializes string values correctly', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test', { type: 'movie' })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('type=movie')
    })

    it('serializes number values as strings', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test', { limit: 100 })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('limit=100')
    })

    it('serializes boolean values as strings', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test', { timing: true })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('timing=true')
    })

    it('serializes arrays with comma separator', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test', { facets: ['type', 'year', 'genre'] })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('facets=type%2Cyear%2Cgenre')
    })

    it('handles special characters in array values', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => createMockSearchResponse([]),
      })

      await search.imdb('test', { type: ['movie', 'tv-series'] })

      const calledUrl = mockFetch.mock.calls[0][0]
      expect(calledUrl).toContain('type=movie%2Ctv-series')
    })
  })
})
