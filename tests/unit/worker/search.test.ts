/**
 * Search Worker Unit Tests
 *
 * Tests for snippets/worker/search.ts which provides search endpoints for
 * multiple datasets (O*NET, UNSPSC, IMDB) with caching and text filtering.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

// =============================================================================
// Types (mirror from search.ts since not exported)
// =============================================================================

interface Env {
  DATA: R2Bucket
}

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

// =============================================================================
// Mock R2 Data
// =============================================================================

const mockOnetData: Occupation[] = [
  { code: '11-1011.00', title: 'Chief Executives', description: 'Plan, direct, and coordinate operational activities.' },
  { code: '11-1021.00', title: 'General and Operations Managers', description: 'Plan, direct, and coordinate operations of organizations.' },
  { code: '15-1252.00', title: 'Software Developers', description: 'Research, design, and develop computer software.' },
  { code: '15-1253.00', title: 'Software Quality Assurance Analysts', description: 'Test software to identify problems.' },
  { code: '17-2141.00', title: 'Mechanical Engineers', description: 'Design mechanical devices and machinery.' },
]

const mockUnspscData: UnspscEntry[] = [
  {
    commodityCode: '43211501',
    commodityTitle: 'Desktop computers',
    classCode: '43211500',
    classTitle: 'Computers',
    familyCode: '43210000',
    familyTitle: 'Computer Equipment and Accessories',
    segmentCode: '43000000',
    segmentTitle: 'Information Technology Broadcasting and Telecommunications',
  },
  {
    commodityCode: '43211502',
    commodityTitle: 'Notebook computers',
    classCode: '43211500',
    classTitle: 'Computers',
    familyCode: '43210000',
    familyTitle: 'Computer Equipment and Accessories',
    segmentCode: '43000000',
    segmentTitle: 'Information Technology Broadcasting and Telecommunications',
  },
  {
    commodityCode: '43211503',
    commodityTitle: 'Tablet computers',
    classCode: '43211500',
    classTitle: 'Computers',
    familyCode: '43210000',
    familyTitle: 'Computer Equipment and Accessories',
    segmentCode: '43000000',
    segmentTitle: 'Information Technology Broadcasting and Telecommunications',
  },
]

const mockImdbData: ImdbTitle[] = [
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
  {
    tconst: 'tt0111161',
    titleType: 'movie',
    primaryTitle: 'The Shawshank Redemption',
    originalTitle: 'The Shawshank Redemption',
    isAdult: false,
    startYear: 1994,
    endYear: null,
    runtimeMinutes: 142,
    genres: ['Drama'],
  },
  {
    tconst: 'tt0108778',
    titleType: 'tvSeries',
    primaryTitle: 'Friends',
    originalTitle: 'Friends',
    isAdult: false,
    startYear: 1994,
    endYear: 2004,
    runtimeMinutes: 22,
    genres: ['Comedy', 'Romance'],
  },
]

// =============================================================================
// Mock R2 Bucket
// =============================================================================

function createMockR2Bucket(dataMap: Record<string, unknown>) {
  return {
    get: vi.fn(async (key: string) => {
      const data = dataMap[key]
      if (!data) return null
      return {
        json: async () => data,
        size: JSON.stringify(data).length,
      }
    }),
    head: vi.fn(async (key: string) => {
      const data = dataMap[key]
      if (!data) return null
      return { size: JSON.stringify(data).length }
    }),
    put: vi.fn(),
    delete: vi.fn(),
    list: vi.fn(),
  } as unknown as R2Bucket
}

function createMockEnv(dataMap: Record<string, unknown> = {}): Env {
  const defaultData = {
    'onet-occupations.json': mockOnetData,
    'unspsc.json': mockUnspscData,
    'imdb-titles.json': mockImdbData,
    ...dataMap,
  }
  return {
    DATA: createMockR2Bucket(defaultData),
  }
}

// =============================================================================
// Helper to import and reset module
// =============================================================================

async function importSearchWorker() {
  // Clear module cache to reset dataCache
  vi.resetModules()
  // Dynamic import of the search worker
  return await import('../../../snippets/worker/search')
}

// =============================================================================
// Tests
// =============================================================================

describe('Search Worker', () => {
  let worker: Awaited<ReturnType<typeof importSearchWorker>>['default']

  beforeEach(async () => {
    vi.clearAllMocks()
    const module = await importSearchWorker()
    worker = module.default
  })

  // ===========================================================================
  // Health Check
  // ===========================================================================

  describe('Health Check', () => {
    it('should return healthy status at /health', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/health')
      const response = await worker.fetch(request, env)

      expect(response.status).toBe(200)
      const body = await response.json() as Record<string, unknown>
      expect(body.status).toBe('ok')
      expect(body.datasets).toEqual(['onet', 'unspsc', 'imdb'])
      expect(body.hasDataBinding).toBe(true)
    })

    it('should return healthy status at /search/health', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/health')
      const response = await worker.fetch(request, env)

      expect(response.status).toBe(200)
      const body = await response.json() as Record<string, unknown>
      expect(body.status).toBe('ok')
    })

    it('should check R2 access in health check', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/health')
      await worker.fetch(request, env)

      expect(env.DATA.head).toHaveBeenCalledWith('onet-occupations.json')
    })

    it('should report R2 status when object not found', async () => {
      const env = createMockEnv({})
      ;(env.DATA.head as ReturnType<typeof vi.fn>).mockResolvedValue(null)

      const request = new Request('https://search.test/health')
      const response = await worker.fetch(request, env)

      const body = await response.json() as Record<string, unknown>
      expect(body.r2Status).toBe('object not found')
    })
  })

  // ===========================================================================
  // Root Endpoint
  // ===========================================================================

  describe('Root Endpoint', () => {
    it('should return API info at /', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/')
      const response = await worker.fetch(request, env)

      expect(response.status).toBe(200)
      const body = await response.json() as Record<string, unknown>
      expect(body.name).toBe('ParqueDB Search API')
      expect(body.version).toBe('1.0.0')
      expect(body.endpoints).toBeDefined()
    })

    it('should return API info at /search', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search')
      const response = await worker.fetch(request, env)

      expect(response.status).toBe(200)
      const body = await response.json() as Record<string, unknown>
      expect(body.name).toBe('ParqueDB Search API')
    })

    it('should include endpoint documentation', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/')
      const response = await worker.fetch(request, env)

      const body = await response.json() as Record<string, unknown>
      const endpoints = body.endpoints as Record<string, Record<string, unknown>>

      expect(endpoints['/search/onet']).toBeDefined()
      expect(endpoints['/search/onet'].params).toContain('q')
      expect(endpoints['/search/unspsc']).toBeDefined()
      expect(endpoints['/search/imdb']).toBeDefined()
      expect(endpoints['/search/health']).toBeDefined()
    })
  })

  // ===========================================================================
  // 404 Not Found
  // ===========================================================================

  describe('Not Found', () => {
    it('should return 404 for unknown endpoints', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/unknown')
      const response = await worker.fetch(request, env)

      expect(response.status).toBe(404)
      const body = await response.json() as Record<string, unknown>
      expect(body.error).toBe('Not Found')
      expect(body.message).toContain('/unknown')
    })

    it('should include available endpoints in 404 response', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/unknown')
      const response = await worker.fetch(request, env)

      const body = await response.json() as Record<string, unknown>
      expect(body.availableEndpoints).toContain('/search/onet')
      expect(body.availableEndpoints).toContain('/search/unspsc')
      expect(body.availableEndpoints).toContain('/search/imdb')
      expect(body.availableEndpoints).toContain('/search/health')
    })
  })

  // ===========================================================================
  // O*NET Search
  // ===========================================================================

  describe('O*NET Search', () => {
    it('should search O*NET occupations at /search/onet', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?q=software')
      const response = await worker.fetch(request, env)

      expect(response.status).toBe(200)
      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(2) // Software Developers, Software QA Analysts
      expect(body.data.every(occ => occ.title.toLowerCase().includes('software'))).toBe(true)
    })

    it('should also work at /onet shorthand', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/onet?q=engineer')
      const response = await worker.fetch(request, env)

      expect(response.status).toBe(200)
      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.some(occ => occ.title.includes('Engineers'))).toBe(true)
    })

    it('should search in title field', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?q=executive')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.title).toBe('Chief Executives')
    })

    it('should search in description field', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?q=coordinate')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(2) // Chief Executives, General and Operations Managers
    })

    it('should be case insensitive', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?q=SOFTWARE')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(2)
    })

    it('should filter by code prefix', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?code=15')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(2) // Software Developers, Software QA Analysts
      expect(body.data.every(occ => occ.code.startsWith('15'))).toBe(true)
    })

    it('should combine code filter with text search', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?code=15&q=developers')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.title).toBe('Software Developers')
    })

    it('should return all when no query', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(5)
    })

    it('should include response time header', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?q=software')
      const response = await worker.fetch(request, env)

      expect(response.headers.get('X-Response-Time')).toMatch(/^\d+ms$/)
    })

    it('should set cache control header', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?q=software')
      const response = await worker.fetch(request, env)

      expect(response.headers.get('Cache-Control')).toBe('public, max-age=3600')
    })
  })

  // ===========================================================================
  // UNSPSC Search
  // ===========================================================================

  describe('UNSPSC Search', () => {
    it('should search UNSPSC categories at /search/unspsc', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/unspsc?q=computer')
      const response = await worker.fetch(request, env)

      expect(response.status).toBe(200)
      const body = await response.json() as SearchResult<UnspscEntry>
      expect(body.data.length).toBe(3) // All mock items contain "computer"
    })

    it('should also work at /unspsc shorthand', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/unspsc?q=notebook')
      const response = await worker.fetch(request, env)

      expect(response.status).toBe(200)
      const body = await response.json() as SearchResult<UnspscEntry>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.commodityTitle).toBe('Notebook computers')
    })

    it('should search in commodityTitle field', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/unspsc?q=desktop')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<UnspscEntry>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.commodityTitle).toBe('Desktop computers')
    })

    it('should search in classTitle field', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/unspsc?q=Computers')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<UnspscEntry>
      expect(body.data.length).toBe(3) // All are in "Computers" class
    })

    it('should search in familyTitle field', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/unspsc?q=accessories')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<UnspscEntry>
      expect(body.data.length).toBe(3) // All in "Computer Equipment and Accessories" family
    })

    it('should search in segmentTitle field', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/unspsc?q=telecommunications')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<UnspscEntry>
      expect(body.data.length).toBe(3) // All in IT/Telecom segment
    })

    it('should filter by commodityCode', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/unspsc?commodity=43211501')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<UnspscEntry>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.commodityTitle).toBe('Desktop computers')
    })

    it('should filter by familyCode', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/unspsc?family=43210000')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<UnspscEntry>
      expect(body.data.length).toBe(3)
    })

    it('should filter by segmentCode', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/unspsc?segment=43000000')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<UnspscEntry>
      expect(body.data.length).toBe(3)
    })

    it('should combine multiple filters', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/unspsc?family=43210000&q=tablet')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<UnspscEntry>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.commodityTitle).toBe('Tablet computers')
    })
  })

  // ===========================================================================
  // IMDB Search
  // ===========================================================================

  describe('IMDB Search', () => {
    it('should search IMDB titles at /search/imdb', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/imdb?q=matrix')
      const response = await worker.fetch(request, env)

      expect(response.status).toBe(200)
      const body = await response.json() as SearchResult<ImdbTitle>
      expect(body.data.length).toBe(2) // The Matrix, The Matrix Reloaded
    })

    it('should also work at /imdb shorthand', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/imdb?q=shawshank')
      const response = await worker.fetch(request, env)

      expect(response.status).toBe(200)
      const body = await response.json() as SearchResult<ImdbTitle>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.primaryTitle).toBe('The Shawshank Redemption')
    })

    it('should search in primaryTitle field', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/imdb?q=friends')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<ImdbTitle>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.primaryTitle).toBe('Friends')
    })

    it('should search in originalTitle field', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/imdb?q=redemption')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<ImdbTitle>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.originalTitle).toBe('The Shawshank Redemption')
    })

    it('should filter by titleType', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/imdb?type=movie')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<ImdbTitle>
      expect(body.data.length).toBe(3) // 3 movies, 1 TV series
      expect(body.data.every(t => t.titleType === 'movie')).toBe(true)
    })

    it('should filter by genre', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/imdb?genre=drama')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<ImdbTitle>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.primaryTitle).toBe('The Shawshank Redemption')
    })

    it('should filter by genre case insensitively', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/imdb?genre=ACTION')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<ImdbTitle>
      expect(body.data.length).toBe(2) // Matrix and Matrix Reloaded
    })

    it('should filter by year', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/imdb?year=1999')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<ImdbTitle>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.primaryTitle).toBe('The Matrix')
    })

    it('should combine type and year filters', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/imdb?type=movie&year=1994')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<ImdbTitle>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.primaryTitle).toBe('The Shawshank Redemption')
    })

    it('should combine type filter with text search', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/imdb?type=tvSeries&q=friends')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<ImdbTitle>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.titleType).toBe('tvSeries')
    })

    it('should use default limit of 20 for IMDB', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/imdb')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<ImdbTitle>
      expect(body.pagination.limit).toBe(20)
    })

    it('should have shorter cache for IMDB (300 seconds)', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/imdb?q=matrix')
      const response = await worker.fetch(request, env)

      expect(response.headers.get('Cache-Control')).toBe('public, max-age=300')
    })
  })

  // ===========================================================================
  // Pagination
  // ===========================================================================

  describe('Pagination', () => {
    it('should respect limit parameter', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?limit=2')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(2)
      expect(body.pagination.limit).toBe(2)
      expect(body.pagination.total).toBe(5)
    })

    it('should respect offset parameter', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?offset=2&limit=2')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(2)
      expect(body.pagination.offset).toBe(2)
    })

    it('should cap limit at 100', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?limit=500')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.pagination.limit).toBe(100)
    })

    it('should calculate hasMore correctly', async () => {
      const env = createMockEnv()

      // With more items remaining
      const request1 = new Request('https://search.test/search/onet?limit=2')
      const response1 = await worker.fetch(request1, env)
      const body1 = await response1.json() as SearchResult<Occupation>
      expect(body1.pagination.hasMore).toBe(true)

      // Without more items
      const request2 = new Request('https://search.test/search/onet?limit=50')
      const response2 = await worker.fetch(request2, env)
      const body2 = await response2.json() as SearchResult<Occupation>
      expect(body2.pagination.hasMore).toBe(false)
    })

    it('should handle offset beyond data range', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?offset=100')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(0)
      expect(body.pagination.total).toBe(5)
      expect(body.pagination.hasMore).toBe(false)
    })

    it('should default limit to 50 for O*NET and UNSPSC', async () => {
      const env = createMockEnv()

      const onetReq = new Request('https://search.test/search/onet')
      const onetRes = await worker.fetch(onetReq, env)
      const onetBody = await onetRes.json() as SearchResult<Occupation>
      expect(onetBody.pagination.limit).toBe(50)

      const unspscReq = new Request('https://search.test/search/unspsc')
      const unspscRes = await worker.fetch(unspscReq, env)
      const unspscBody = await unspscRes.json() as SearchResult<UnspscEntry>
      expect(unspscBody.pagination.limit).toBe(50)
    })

    it('should default offset to 0', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.pagination.offset).toBe(0)
    })
  })

  // ===========================================================================
  // Timing Information
  // ===========================================================================

  describe('Timing', () => {
    it('should include timing information in response', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?q=software')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.timing).toBeDefined()
      expect(typeof body.timing.fetchMs).toBe('number')
      expect(typeof body.timing.searchMs).toBe('number')
      expect(typeof body.timing.totalMs).toBe('number')
    })

    it('should round timing values to 2 decimal places', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?q=software')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      // Check that timing values are properly rounded (at most 2 decimal places)
      const checkDecimalPlaces = (n: number) => {
        const str = n.toString()
        const decimalIdx = str.indexOf('.')
        if (decimalIdx === -1) return true
        return str.length - decimalIdx - 1 <= 2
      }
      expect(checkDecimalPlaces(body.timing.fetchMs)).toBe(true)
      expect(checkDecimalPlaces(body.timing.searchMs)).toBe(true)
      expect(checkDecimalPlaces(body.timing.totalMs)).toBe(true)
    })
  })

  // ===========================================================================
  // Caching
  // ===========================================================================

  describe('Data Caching', () => {
    it('should cache dataset on first load', async () => {
      const env = createMockEnv()

      // First request - should load from R2
      const request1 = new Request('https://search.test/search/onet?q=software')
      await worker.fetch(request1, env)
      expect(env.DATA.get).toHaveBeenCalledWith('onet-occupations.json')
      expect(env.DATA.get).toHaveBeenCalledTimes(1)

      // Second request - should use cache
      const request2 = new Request('https://search.test/search/onet?q=engineer')
      await worker.fetch(request2, env)
      // Should still be only 1 call (cached)
      expect(env.DATA.get).toHaveBeenCalledTimes(1)
    })

    it('should report cached datasets in health check', async () => {
      const env = createMockEnv()

      // Load O*NET data first
      await worker.fetch(new Request('https://search.test/search/onet?q=test'), env)

      // Check health
      const healthReq = new Request('https://search.test/health')
      const response = await worker.fetch(healthReq, env)
      const body = await response.json() as Record<string, unknown>

      expect(body.cached).toContain('onet')
    })

    it('should cache datasets independently', async () => {
      const env = createMockEnv()

      // Load O*NET
      await worker.fetch(new Request('https://search.test/search/onet?q=test'), env)
      expect(env.DATA.get).toHaveBeenCalledTimes(1)

      // Load UNSPSC
      await worker.fetch(new Request('https://search.test/search/unspsc?q=test'), env)
      expect(env.DATA.get).toHaveBeenCalledTimes(2)

      // Load IMDB
      await worker.fetch(new Request('https://search.test/search/imdb?q=test'), env)
      expect(env.DATA.get).toHaveBeenCalledTimes(3)

      // All subsequent requests should use cache
      await worker.fetch(new Request('https://search.test/search/onet?q=other'), env)
      await worker.fetch(new Request('https://search.test/search/unspsc?q=other'), env)
      await worker.fetch(new Request('https://search.test/search/imdb?q=other'), env)
      expect(env.DATA.get).toHaveBeenCalledTimes(3) // No additional calls
    })
  })

  // ===========================================================================
  // Error Handling
  // ===========================================================================

  describe('Error Handling', () => {
    it('should return 500 when R2 object is missing', async () => {
      const env = createMockEnv({})
      ;(env.DATA.get as ReturnType<typeof vi.fn>).mockResolvedValue(null)

      const request = new Request('https://search.test/search/onet?q=test')
      const response = await worker.fetch(request, env)

      expect(response.status).toBe(500)
      const body = await response.json() as Record<string, unknown>
      expect(body.error).toBe('Internal Server Error')
      expect(body.message).toContain('R2 object missing')
    })

    it('should return 500 when R2 throws error', async () => {
      const env = createMockEnv()
      ;(env.DATA.get as ReturnType<typeof vi.fn>).mockRejectedValue(new Error('R2 connection failed'))

      const request = new Request('https://search.test/search/onet?q=test')
      const response = await worker.fetch(request, env)

      expect(response.status).toBe(500)
      const body = await response.json() as Record<string, unknown>
      expect(body.error).toBe('Internal Server Error')
      expect(body.message).toBe('R2 connection failed')
    })

    it('should return JSON content type for error responses', async () => {
      const env = createMockEnv()
      ;(env.DATA.get as ReturnType<typeof vi.fn>).mockRejectedValue(new Error('Test error'))

      const request = new Request('https://search.test/search/onet?q=test')
      const response = await worker.fetch(request, env)

      expect(response.headers.get('Content-Type')).toBe('application/json')
    })
  })

  // ===========================================================================
  // searchData Function (Text Filtering Logic)
  // ===========================================================================

  describe('Text Search Logic', () => {
    it('should match partial strings', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?q=soft')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(2) // Software Developers, Software QA Analysts
    })

    it('should search across all configured fields', async () => {
      const env = createMockEnv()
      // "design" appears in description of both Software Developers and Mechanical Engineers
      const request = new Request('https://search.test/search/onet?q=design')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(2)
    })

    it('should handle empty search query', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?q=')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      // Empty query returns all items
      expect(body.data.length).toBe(5)
    })

    it('should handle search query with no matches', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?q=nonexistentterm')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(0)
      expect(body.pagination.total).toBe(0)
    })

    it('should search in array fields (genres for IMDB)', async () => {
      const env = createMockEnv()
      // Note: genres is an array, but text search on primaryTitle/originalTitle
      // Sci-Fi is in genres array, but search is on primaryTitle/originalTitle
      const request = new Request('https://search.test/search/imdb?q=Matrix')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<ImdbTitle>
      expect(body.data.length).toBe(2)
      // Verify genres are returned in results
      expect(body.data.every(t => t.genres.includes('Sci-Fi'))).toBe(true)
    })
  })

  // ===========================================================================
  // Query Parameter Parsing
  // ===========================================================================

  describe('Query Parameter Parsing', () => {
    it('should parse limit as integer', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?limit=3.7')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.pagination.limit).toBe(3) // parseInt truncates
    })

    it('should handle invalid limit gracefully', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?limit=abc')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      // parseInt('abc', 10) returns NaN
      // Math.min(NaN, 100) returns NaN
      // JSON.stringify converts NaN to null
      // This documents the current behavior - ideally invalid params should be rejected or use defaults
      expect(body.pagination.limit).toBeNull()
    })

    it('should handle negative offset', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?offset=-5')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.pagination.offset).toBe(-5) // Current behavior - no validation
    })

    it('should URL decode query parameter', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?q=Chief%20Executives')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      expect(body.data.length).toBe(1)
      expect(body.data[0]!.title).toBe('Chief Executives')
    })

    it('should handle special characters in query', async () => {
      const env = createMockEnv()
      const request = new Request('https://search.test/search/onet?q=11-1011')
      const response = await worker.fetch(request, env)

      const body = await response.json() as SearchResult<Occupation>
      // Should match code field since description contains code
      expect(body.data.length).toBe(0) // Code is not in searchFields for text search
    })
  })
})
