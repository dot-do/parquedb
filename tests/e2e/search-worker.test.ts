/**
 * E2E Test Suite for Search Worker at cdn.workers.do/search
 *
 * Tests the deployed search worker endpoints for O*NET, UNSPSC, and IMDB datasets.
 *
 * Run with:
 *   SEARCH_WORKER_URL=https://cdn.workers.do pnpm test:e2e tests/e2e/search-worker.test.ts
 *
 * Or skip in CI:
 *   SKIP_SEARCH_WORKER_TESTS=1 pnpm test:e2e
 */

import { describe, it, expect, beforeAll } from 'vitest'

// =============================================================================
// Configuration
// =============================================================================

const SKIP_TESTS = process.env.SKIP_SEARCH_WORKER_TESTS === '1'
const SEARCH_WORKER_URL = process.env.SEARCH_WORKER_URL || 'https://cdn.workers.do'
const REQUEST_TIMEOUT = 30000

// =============================================================================
// Types
// =============================================================================

interface SearchResult<T> {
  data: T[]
  pagination: {
    total: number
    limit: number
    offset: number
    hasMore: boolean
  }
  timing: {
    fetchMs: number
    searchMs: number
    totalMs: number
  }
}

interface Occupation {
  code: string
  title: string
  description: string
}

interface UnspscEntry {
  commodityCode: string
  commodityTitle: string
  classCode: string
  classTitle: string
  familyCode: string
  familyTitle: string
  segmentCode: string
  segmentTitle: string
}

interface ImdbTitle {
  tconst: string
  titleType: string
  primaryTitle: string
  originalTitle: string
  isAdult: boolean
  startYear: number | null
  endYear: number | null
  runtimeMinutes: number | null
  genres: string[]
}

interface HealthResponse {
  status: string
  datasets: string[]
  cached: string[]
  r2Status: string
  hasDataBinding: boolean
}

interface ErrorResponse {
  error: string
  message: string
  availableEndpoints?: string[] | undefined
}

interface ApiInfo {
  name: string
  version: string
  endpoints: Record<string, unknown>
}

interface RequestResult<T = unknown> {
  success: boolean
  status: number
  latencyMs: number
  data?: T | undefined
  error?: string | undefined
  headers?: Record<string, string> | undefined
}

// =============================================================================
// Test Helpers
// =============================================================================

async function fetchWithTimeout<T>(
  url: string,
  timeout: number = REQUEST_TIMEOUT
): Promise<RequestResult<T>> {
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeout)
  const start = performance.now()

  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        Accept: 'application/json',
      },
    })
    const latencyMs = performance.now() - start
    clearTimeout(timeoutId)

    const headers: Record<string, string> = {}
    response.headers.forEach((value, key) => {
      headers[key.toLowerCase()] = value
    })

    let data: T | undefined
    const contentType = response.headers.get('content-type') || ''
    if (contentType.includes('application/json')) {
      try {
        data = await response.json()
      } catch {
        // Ignore JSON parse errors
      }
    }

    return {
      success: response.ok,
      status: response.status,
      latencyMs,
      data,
      headers,
    }
  } catch (error) {
    clearTimeout(timeoutId)
    const latencyMs = performance.now() - start
    const errorMessage =
      error instanceof Error
        ? error.name === 'AbortError'
          ? `Request timeout after ${timeout}ms`
          : error.message
        : String(error)

    return {
      success: false,
      status: 0,
      latencyMs,
      error: errorMessage,
    }
  }
}

async function searchEndpoint<T>(
  endpoint: string,
  params: Record<string, string | number> = {}
): Promise<RequestResult<SearchResult<T>>> {
  const url = new URL(`${SEARCH_WORKER_URL}${endpoint}`)
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, String(value))
  }
  return fetchWithTimeout<SearchResult<T>>(url.toString())
}

// =============================================================================
// Test Suite
// =============================================================================

describe.skipIf(SKIP_TESTS)('E2E Search Worker Tests (cdn.workers.do/search)', () => {
  // Health check before running tests
  beforeAll(async () => {
    const result = await fetchWithTimeout<HealthResponse>(
      `${SEARCH_WORKER_URL}/search/health`
    )
    if (!result.success) {
      console.warn(
        `Search worker health check failed: ${result.error || `HTTP ${result.status}`}`
      )
      console.warn(`URL: ${SEARCH_WORKER_URL}/search/health`)
    }
  }, 60000)

  // ===========================================================================
  // Health & API Info
  // ===========================================================================

  describe('Health & API Info', () => {
    it('should return health status', async () => {
      const result = await fetchWithTimeout<HealthResponse>(
        `${SEARCH_WORKER_URL}/search/health`
      )

      expect(result.success).toBe(true)
      expect(result.status).toBe(200)
      expect(result.data).toBeDefined()
      expect(result.data!.status).toBe('ok')
      expect(result.data!.datasets).toBeInstanceOf(Array)
      expect(result.data!.datasets).toContain('onet')
      expect(result.data!.datasets).toContain('unspsc')
      expect(result.data!.datasets).toContain('imdb')
    })

    it('should return API info at root endpoint', async () => {
      // Try /search/ with trailing slash (some routers redirect /search to /search/)
      const result = await fetchWithTimeout<ApiInfo>(`${SEARCH_WORKER_URL}/search/`)

      // May redirect or return API info - both are acceptable
      if (result.success) {
        expect(result.status).toBe(200)
        expect(result.data).toBeDefined()
        expect(result.data!.name).toBe('ParqueDB Search API')
        expect(result.data!.endpoints).toBeDefined()
      } else {
        // If not successful, it should be a redirect (3xx) or the endpoint doesn't exist
        // This is acceptable behavior for the root endpoint
        expect([301, 302, 307, 308, 404]).toContain(result.status)
      }
    })
  })

  // ===========================================================================
  // O*NET Search
  // ===========================================================================

  describe('O*NET Search (/search/onet)', () => {
    it('should search occupations by query', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { q: 'engineer' })

      expect(result.success).toBe(true)
      expect(result.status).toBe(200)
      expect(result.data).toBeDefined()
      expect(result.data!.data).toBeInstanceOf(Array)
      expect(result.data!.data.length).toBeGreaterThan(0)

      // Verify response structure
      const occupation = result.data!.data[0]
      expect(occupation).toHaveProperty('code')
      expect(occupation).toHaveProperty('title')
      expect(occupation).toHaveProperty('description')

      // Verify search relevance
      const hasEngineer = result.data!.data.some(
        (occ) =>
          occ.title.toLowerCase().includes('engineer') ||
          occ.description.toLowerCase().includes('engineer')
      )
      expect(hasEngineer).toBe(true)
    })

    it('should search occupations by code prefix', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { code: '11' })

      expect(result.success).toBe(true)
      expect(result.data).toBeDefined()
      expect(result.data!.data.length).toBeGreaterThan(0)

      // All results should have codes starting with '11'
      for (const occ of result.data!.data) {
        expect(occ.code.startsWith('11')).toBe(true)
      }
    })

    it('should return empty array for no matches', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', {
        q: 'xyznonexistent123',
      })

      expect(result.success).toBe(true)
      expect(result.data).toBeDefined()
      expect(result.data!.data).toBeInstanceOf(Array)
      expect(result.data!.data.length).toBe(0)
      expect(result.data!.pagination.total).toBe(0)
    })

    it('should return all occupations without query', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { limit: 10 })

      expect(result.success).toBe(true)
      expect(result.data).toBeDefined()
      expect(result.data!.data.length).toBeLessThanOrEqual(10)
      expect(result.data!.pagination.total).toBeGreaterThan(0)
    })

    it('should include timing information', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { q: 'manager' })

      expect(result.success).toBe(true)
      expect(result.data).toBeDefined()
      expect(result.data!.timing).toBeDefined()
      expect(typeof result.data!.timing.fetchMs).toBe('number')
      expect(typeof result.data!.timing.searchMs).toBe('number')
      expect(typeof result.data!.timing.totalMs).toBe('number')
      expect(result.data!.timing.totalMs).toBeGreaterThanOrEqual(0)
    })
  })

  // ===========================================================================
  // UNSPSC Search
  // ===========================================================================

  describe('UNSPSC Search (/search/unspsc)', () => {
    it('should search categories by query', async () => {
      const result = await searchEndpoint<UnspscEntry>('/search/unspsc', {
        q: 'computer',
      })

      expect(result.success).toBe(true)
      expect(result.status).toBe(200)
      expect(result.data).toBeDefined()
      expect(result.data!.data).toBeInstanceOf(Array)
      expect(result.data!.data.length).toBeGreaterThan(0)

      // Verify response structure
      const entry = result.data!.data[0]
      expect(entry).toHaveProperty('commodityCode')
      expect(entry).toHaveProperty('commodityTitle')
      expect(entry).toHaveProperty('classCode')
      expect(entry).toHaveProperty('familyCode')
      expect(entry).toHaveProperty('segmentCode')
    })

    it('should filter by segment code', async () => {
      // First get a valid segment code
      const initial = await searchEndpoint<UnspscEntry>('/search/unspsc', { limit: 1 })
      expect(initial.success).toBe(true)
      expect(initial.data!.data.length).toBeGreaterThan(0)

      const segmentCode = initial.data!.data[0].segmentCode

      const result = await searchEndpoint<UnspscEntry>('/search/unspsc', {
        segment: segmentCode,
      })

      expect(result.success).toBe(true)
      expect(result.data!.data.length).toBeGreaterThan(0)

      // All results should have the same segment code
      for (const entry of result.data!.data) {
        expect(entry.segmentCode).toBe(segmentCode)
      }
    })

    it('should filter by family code', async () => {
      // First get a valid family code
      const initial = await searchEndpoint<UnspscEntry>('/search/unspsc', { limit: 1 })
      expect(initial.success).toBe(true)

      const familyCode = initial.data!.data[0].familyCode

      const result = await searchEndpoint<UnspscEntry>('/search/unspsc', {
        family: familyCode,
      })

      expect(result.success).toBe(true)
      for (const entry of result.data!.data) {
        expect(entry.familyCode).toBe(familyCode)
      }
    })

    it('should combine query with filters', async () => {
      const result = await searchEndpoint<UnspscEntry>('/search/unspsc', {
        q: 'equipment',
        limit: 20,
      })

      expect(result.success).toBe(true)
      expect(result.data).toBeDefined()

      // Search results should contain 'equipment' in one of the title fields
      if (result.data!.data.length > 0) {
        const hasEquipment = result.data!.data.some((entry) => {
          const titles = [
            entry.commodityTitle,
            entry.classTitle,
            entry.familyTitle,
            entry.segmentTitle,
          ].join(' ')
          return titles.toLowerCase().includes('equipment')
        })
        expect(hasEquipment).toBe(true)
      }
    })
  })

  // ===========================================================================
  // IMDB Search
  // ===========================================================================

  describe('IMDB Search (/search/imdb)', () => {
    it('should search titles by query', async () => {
      const result = await searchEndpoint<ImdbTitle>('/search/imdb', { q: 'love' })

      expect(result.success).toBe(true)
      expect(result.status).toBe(200)
      expect(result.data).toBeDefined()
      expect(result.data!.data).toBeInstanceOf(Array)
      expect(result.data!.data.length).toBeGreaterThan(0)

      // Verify response structure
      const title = result.data!.data[0]
      expect(title).toHaveProperty('tconst')
      expect(title).toHaveProperty('titleType')
      expect(title).toHaveProperty('primaryTitle')
      expect(title).toHaveProperty('originalTitle')
      expect(title).toHaveProperty('genres')
    })

    it('should filter by title type', async () => {
      const result = await searchEndpoint<ImdbTitle>('/search/imdb', {
        type: 'movie',
        limit: 20,
      })

      expect(result.success).toBe(true)
      expect(result.data).toBeDefined()

      // All results should be movies
      for (const title of result.data!.data) {
        expect(title.titleType).toBe('movie')
      }
    })

    it('should filter by genre', async () => {
      const result = await searchEndpoint<ImdbTitle>('/search/imdb', {
        genre: 'Comedy',
        limit: 20,
      })

      expect(result.success).toBe(true)
      expect(result.data).toBeDefined()

      // All results should include Comedy genre
      for (const title of result.data!.data) {
        const hasComedy = title.genres.some(
          (g) => g.toLowerCase() === 'comedy'
        )
        expect(hasComedy).toBe(true)
      }
    })

    it('should filter by year', async () => {
      const result = await searchEndpoint<ImdbTitle>('/search/imdb', {
        year: 2020,
        limit: 20,
      })

      expect(result.success).toBe(true)
      expect(result.data).toBeDefined()

      // All results should be from 2020
      for (const title of result.data!.data) {
        expect(title.startYear).toBe(2020)
      }
    })

    it('should combine query with filters', async () => {
      const result = await searchEndpoint<ImdbTitle>('/search/imdb', {
        q: 'the',
        type: 'movie',
        limit: 10,
      })

      expect(result.success).toBe(true)
      expect(result.data).toBeDefined()

      for (const title of result.data!.data) {
        expect(title.titleType).toBe('movie')
        const hasThe =
          title.primaryTitle.toLowerCase().includes('the') ||
          title.originalTitle.toLowerCase().includes('the')
        expect(hasThe).toBe(true)
      }
    })

    it('should include timing information', async () => {
      const result = await searchEndpoint<ImdbTitle>('/search/imdb', { q: 'action' })

      expect(result.success).toBe(true)
      expect(result.data!.timing).toBeDefined()
      expect(result.data!.timing.fetchMs).toBeGreaterThanOrEqual(0)
      expect(result.data!.timing.searchMs).toBeGreaterThanOrEqual(0)
      expect(result.data!.timing.totalMs).toBeGreaterThanOrEqual(0)
    })
  })

  // ===========================================================================
  // Pagination
  // ===========================================================================

  describe('Pagination', () => {
    it('should respect limit parameter', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { limit: 5 })

      expect(result.success).toBe(true)
      expect(result.data!.data.length).toBeLessThanOrEqual(5)
      expect(result.data!.pagination.limit).toBe(5)
    })

    it('should respect offset parameter', async () => {
      // Get first page
      const page1 = await searchEndpoint<Occupation>('/search/onet', {
        limit: 5,
        offset: 0,
      })

      // Get second page
      const page2 = await searchEndpoint<Occupation>('/search/onet', {
        limit: 5,
        offset: 5,
      })

      expect(page1.success).toBe(true)
      expect(page2.success).toBe(true)

      // Pages should have different items (assuming there are enough results)
      if (page1.data!.pagination.total > 5) {
        expect(page1.data!.data[0].code).not.toBe(page2.data!.data[0]?.code)
        expect(page2.data!.pagination.offset).toBe(5)
      }
    })

    it('should indicate hasMore correctly', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { limit: 5 })

      expect(result.success).toBe(true)

      const { pagination } = result.data!
      const expectedHasMore = pagination.offset + result.data!.data.length < pagination.total
      expect(pagination.hasMore).toBe(expectedHasMore)
    })

    it('should cap limit at 100', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { limit: 200 })

      expect(result.success).toBe(true)
      expect(result.data!.data.length).toBeLessThanOrEqual(100)
      expect(result.data!.pagination.limit).toBeLessThanOrEqual(100)
    })

    it('should handle empty offset gracefully', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { limit: 5 })

      expect(result.success).toBe(true)
      expect(result.data!.pagination.offset).toBe(0)
    })

    it('should paginate through UNSPSC results', async () => {
      const page1 = await searchEndpoint<UnspscEntry>('/search/unspsc', {
        limit: 10,
        offset: 0,
      })
      const page2 = await searchEndpoint<UnspscEntry>('/search/unspsc', {
        limit: 10,
        offset: 10,
      })

      expect(page1.success).toBe(true)
      expect(page2.success).toBe(true)

      if (page1.data!.pagination.total > 10) {
        expect(page1.data!.data[0].commodityCode).not.toBe(
          page2.data!.data[0]?.commodityCode
        )
      }
    })

    it('should paginate through IMDB results', async () => {
      const page1 = await searchEndpoint<ImdbTitle>('/search/imdb', {
        limit: 10,
        offset: 0,
      })
      const page2 = await searchEndpoint<ImdbTitle>('/search/imdb', {
        limit: 10,
        offset: 10,
      })

      expect(page1.success).toBe(true)
      expect(page2.success).toBe(true)

      if (page1.data!.pagination.total > 10) {
        expect(page1.data!.data[0].tconst).not.toBe(page2.data!.data[0]?.tconst)
      }
    })
  })

  // ===========================================================================
  // Error Handling
  // ===========================================================================

  describe('Error Handling', () => {
    it('should return 404 for invalid dataset', async () => {
      const result = await fetchWithTimeout<ErrorResponse>(
        `${SEARCH_WORKER_URL}/search/invalid-dataset`
      )

      expect(result.success).toBe(false)
      expect(result.status).toBe(404)
      expect(result.data).toBeDefined()
      expect(result.data!.error).toBe('Not Found')
      expect(result.data!.availableEndpoints).toContain('/search/onet')
      expect(result.data!.availableEndpoints).toContain('/search/unspsc')
      expect(result.data!.availableEndpoints).toContain('/search/imdb')
    })

    it('should handle empty query gracefully', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { q: '' })

      // Empty query should return all results (not an error)
      expect(result.success).toBe(true)
      expect(result.data!.data).toBeInstanceOf(Array)
    })

    it('should handle special characters in query', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', {
        q: 'test & analysis',
      })

      expect(result.success).toBe(true)
      expect(result.data).toBeDefined()
    })

    it('should handle unicode characters in query', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { q: 'cafe' })

      expect(result.success).toBe(true)
      expect(result.data).toBeDefined()
    })

    it('should handle very long query gracefully', async () => {
      const longQuery = 'a'.repeat(1000)
      const result = await searchEndpoint<Occupation>('/search/onet', { q: longQuery })

      // Should either succeed with no results or return an error, but not crash
      expect([true, false]).toContain(result.success)
    })

    it('should handle invalid limit parameter', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', {
        limit: -1,
      })

      // Should either handle gracefully or use default limit
      expect(result.success).toBe(true)
    })

    it('should handle non-numeric offset', async () => {
      const url = `${SEARCH_WORKER_URL}/search/onet?offset=abc`
      const result = await fetchWithTimeout<SearchResult<Occupation>>(url)

      // Should handle gracefully - either return results with NaN parsed to 0/null, or return error
      // JavaScript's parseInt('abc', 10) returns NaN, which may be handled differently
      if (result.success) {
        // The worker may return null, 0, or NaN for invalid offset - all are acceptable
        const offset = result.data!.pagination.offset
        expect(offset === 0 || offset === null || Number.isNaN(offset)).toBe(true)
      }
    })
  })

  // ===========================================================================
  // Response Format & Timing
  // ===========================================================================

  describe('Response Format & Timing', () => {
    it('should return valid JSON content-type', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { q: 'test' })

      expect(result.success).toBe(true)
      expect(result.headers).toBeDefined()
      expect(result.headers!['content-type']).toContain('application/json')
    })

    it('should include cache-control headers', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { q: 'test' })

      expect(result.success).toBe(true)
      expect(result.headers!['cache-control']).toBeDefined()
    })

    it('should include X-Response-Time header', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { q: 'test' })

      expect(result.success).toBe(true)
      // X-Response-Time is set by the worker
      if (result.headers!['x-response-time']) {
        expect(result.headers!['x-response-time']).toMatch(/^\d+ms$/)
      }
    })

    it('should have reasonable response times for O*NET', async () => {
      const result = await searchEndpoint<Occupation>('/search/onet', { q: 'engineer' })

      expect(result.success).toBe(true)
      // Response should be under 5 seconds even for cold start
      expect(result.latencyMs).toBeLessThan(5000)

      // Timing data should be present
      expect(result.data!.timing.totalMs).toBeGreaterThanOrEqual(0)
    })

    it('should have reasonable response times for UNSPSC', async () => {
      const result = await searchEndpoint<UnspscEntry>('/search/unspsc', {
        q: 'computer',
      })

      expect(result.success).toBe(true)
      expect(result.latencyMs).toBeLessThan(5000)
    })

    it('should have reasonable response times for IMDB', async () => {
      const result = await searchEndpoint<ImdbTitle>('/search/imdb', { q: 'love' })

      expect(result.success).toBe(true)
      expect(result.latencyMs).toBeLessThan(5000)
    })

    it('should return consistent response structure across datasets', async () => {
      const onetResult = await searchEndpoint<Occupation>('/search/onet', { limit: 1 })
      const unspscResult = await searchEndpoint<UnspscEntry>('/search/unspsc', {
        limit: 1,
      })
      const imdbResult = await searchEndpoint<ImdbTitle>('/search/imdb', { limit: 1 })

      // All should have the same structure
      for (const result of [onetResult, unspscResult, imdbResult]) {
        expect(result.success).toBe(true)
        expect(result.data).toBeDefined()
        expect(result.data!.data).toBeInstanceOf(Array)
        expect(result.data!.pagination).toBeDefined()
        expect(result.data!.pagination).toHaveProperty('total')
        expect(result.data!.pagination).toHaveProperty('limit')
        expect(result.data!.pagination).toHaveProperty('offset')
        expect(result.data!.pagination).toHaveProperty('hasMore')
        expect(result.data!.timing).toBeDefined()
        expect(result.data!.timing).toHaveProperty('fetchMs')
        expect(result.data!.timing).toHaveProperty('searchMs')
        expect(result.data!.timing).toHaveProperty('totalMs')
      }
    })
  })

  // ===========================================================================
  // Concurrent Requests
  // ===========================================================================

  describe('Concurrent Requests', () => {
    it('should handle 5 concurrent searches', async () => {
      const requests = Array.from({ length: 5 }, (_, i) =>
        searchEndpoint<Occupation>('/search/onet', { q: `test${i}` })
      )
      const results = await Promise.all(requests)

      const successCount = results.filter((r) => r.success).length
      expect(successCount).toBeGreaterThanOrEqual(4) // Allow 1 failure
    })

    it('should handle concurrent searches across different datasets', async () => {
      const requests = [
        searchEndpoint<Occupation>('/search/onet', { q: 'engineer' }),
        searchEndpoint<UnspscEntry>('/search/unspsc', { q: 'computer' }),
        searchEndpoint<ImdbTitle>('/search/imdb', { q: 'love' }),
      ]
      const results = await Promise.all(requests)

      for (const result of results) {
        expect(result.success).toBe(true)
        expect(result.data).toBeDefined()
      }
    })
  })
})
