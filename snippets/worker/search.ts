/**
 * ParqueDB Unified Search Worker
 *
 * A Cloudflare Worker that provides search endpoints for multiple datasets.
 * Designed to measure real CPU time via tail worker.
 *
 * Endpoints:
 * - GET /search/onet?q=engineer        Search O*NET occupations
 * - GET /search/unspsc?q=computer      Search UNSPSC categories
 * - GET /search/imdb?q=matrix          Search IMDB titles
 * - GET /search/health                 Health check
 *
 * Data source: cdn.workers.do/parquedb-benchmarks/snippets/
 */

// =============================================================================
// Types
// =============================================================================

interface Env {
  /** R2 bucket for data access - avoids routing issues */
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

// Dataset schemas
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
// Configuration
// =============================================================================

const DATASETS = {
  onet: {
    key: 'onet-occupations.json',
    searchFields: ['title', 'description'],
  },
  unspsc: {
    key: 'unspsc.json',
    searchFields: ['commodityTitle', 'classTitle', 'familyTitle', 'segmentTitle'],
  },
  imdb: {
    key: 'imdb-titles.json',
    searchFields: ['primaryTitle', 'originalTitle'],
  },
} as const

type DatasetName = keyof typeof DATASETS

// Dataset caches (persists within worker instance)
const dataCache = new Map<DatasetName, unknown[]>()

// =============================================================================
// Data Loading
// =============================================================================

/**
 * Load dataset from R2 with caching
 *
 * Uses direct R2 binding to avoid routing issues with subrequests.
 */
async function loadDataset<T>(name: DatasetName, env: Env): Promise<{ data: T[]; fetchMs: number }> {
  const startFetch = performance.now()

  // Check cache
  const cached = dataCache.get(name)
  if (cached) {
    console.log(`[loadDataset] ${name}: cache hit`)
    return { data: cached as T[], fetchMs: performance.now() - startFetch }
  }

  console.log(`[loadDataset] ${name}: loading from R2, env.DATA exists: ${!!env.DATA}`)

  // Load from R2 directly
  const config = DATASETS[name]
  console.log(`[loadDataset] ${name}: fetching key ${config.key}`)

  const object = await env.DATA.get(config.key)

  if (!object) {
    console.log(`[loadDataset] ${name}: object not found in R2`)
    throw new Error(`R2 object missing: ${name} (key: ${config.key})`)
  }

  console.log(`[loadDataset] ${name}: object found, size ${object.size}`)
  const data = (await object.json()) as T[]
  dataCache.set(name, data)

  console.log(`[loadDataset] ${name}: loaded ${data.length} items`)
  return { data, fetchMs: performance.now() - startFetch }
}

// =============================================================================
// Search Functions
// =============================================================================

/**
 * Generic text search across specified fields
 */
function searchData<T extends Record<string, unknown>>(
  data: T[],
  query: string | null,
  searchFields: readonly string[],
  filters: Record<string, string | null>,
  limit: number,
  offset: number
): { results: T[]; total: number; searchMs: number } {
  const startSearch = performance.now()

  let filtered = data

  // Apply text query
  if (query) {
    const q = query.toLowerCase()
    filtered = filtered.filter((item) =>
      searchFields.some((field) => {
        const value = item[field]
        if (typeof value === 'string') {
          return value.toLowerCase().includes(q)
        }
        if (Array.isArray(value)) {
          return value.some((v) => typeof v === 'string' && v.toLowerCase().includes(q))
        }
        return false
      })
    )
  }

  // Apply additional filters
  for (const [key, value] of Object.entries(filters)) {
    if (value !== null && value !== undefined) {
      filtered = filtered.filter((item) => {
        const itemValue = item[key]
        if (itemValue === undefined) return false

        // Handle numeric comparison
        if (typeof itemValue === 'number') {
          return itemValue === Number(value)
        }

        // Handle string comparison
        if (typeof itemValue === 'string') {
          return itemValue === value || itemValue.startsWith(value)
        }

        return false
      })
    }
  }

  const total = filtered.length
  const results = filtered.slice(offset, offset + limit)

  return {
    results,
    total,
    searchMs: performance.now() - startSearch,
  }
}

// =============================================================================
// Dataset-specific handlers
// =============================================================================

async function handleOnetSearch(params: URLSearchParams, env: Env): Promise<Response> {
  const startTotal = performance.now()

  const q = params.get('q')
  const code = params.get('code')
  const limit = Math.min(parseInt(params.get('limit') ?? '50', 10), 100)
  const offset = parseInt(params.get('offset') ?? '0', 10)

  const { data, fetchMs } = await loadDataset<Occupation>('onet', env)

  // For code lookup, filter by prefix
  let filtered = data
  if (code) {
    filtered = data.filter((occ) => occ.code.startsWith(code))
  }

  const { results, total, searchMs } = searchData(
    filtered,
    q,
    DATASETS.onet.searchFields,
    {},
    limit,
    offset
  )

  const result: SearchResult<Occupation> = {
    data: results,
    pagination: {
      total,
      limit,
      offset,
      hasMore: offset + results.length < total,
    },
    timing: {
      fetchMs: Math.round(fetchMs * 100) / 100,
      searchMs: Math.round(searchMs * 100) / 100,
      totalMs: Math.round((performance.now() - startTotal) * 100) / 100,
    },
  }

  return Response.json(result, {
    headers: {
      'Cache-Control': 'public, max-age=3600',
      'Content-Type': 'application/json',
    },
  })
}

async function handleUnspscSearch(params: URLSearchParams, env: Env): Promise<Response> {
  const startTotal = performance.now()

  const q = params.get('q')
  const commodity = params.get('commodity')
  const family = params.get('family')
  const segment = params.get('segment')
  const limit = Math.min(parseInt(params.get('limit') ?? '50', 10), 100)
  const offset = parseInt(params.get('offset') ?? '0', 10)

  const { data, fetchMs } = await loadDataset<UnspscEntry>('unspsc', env)

  // Apply specific filters
  let filtered = data
  if (commodity) {
    filtered = filtered.filter((e) => e.commodityCode === commodity)
  }
  if (family) {
    filtered = filtered.filter((e) => e.familyCode === family)
  }
  if (segment) {
    filtered = filtered.filter((e) => e.segmentCode === segment)
  }

  const { results, total, searchMs } = searchData(
    filtered,
    q,
    DATASETS.unspsc.searchFields,
    {},
    limit,
    offset
  )

  const result: SearchResult<UnspscEntry> = {
    data: results,
    pagination: {
      total,
      limit,
      offset,
      hasMore: offset + results.length < total,
    },
    timing: {
      fetchMs: Math.round(fetchMs * 100) / 100,
      searchMs: Math.round(searchMs * 100) / 100,
      totalMs: Math.round((performance.now() - startTotal) * 100) / 100,
    },
  }

  return Response.json(result, {
    headers: {
      'Cache-Control': 'public, max-age=3600',
      'Content-Type': 'application/json',
    },
  })
}

async function handleImdbSearch(params: URLSearchParams, env: Env): Promise<Response> {
  const startTotal = performance.now()

  const q = params.get('q')
  const type = params.get('type')
  const genre = params.get('genre')
  const year = params.get('year')
  const limit = Math.min(parseInt(params.get('limit') ?? '20', 10), 100)
  const offset = parseInt(params.get('offset') ?? '0', 10)

  const { data, fetchMs } = await loadDataset<ImdbTitle>('imdb', env)

  // Apply specific filters
  let filtered = data
  if (type) {
    filtered = filtered.filter((t) => t.titleType === type)
  }
  if (genre) {
    const genreLower = genre.toLowerCase()
    filtered = filtered.filter((t) => t.genres.some((g) => g.toLowerCase() === genreLower))
  }
  if (year) {
    const yearNum = parseInt(year, 10)
    filtered = filtered.filter((t) => t.startYear === yearNum)
  }

  const { results, total, searchMs } = searchData(
    filtered,
    q,
    DATASETS.imdb.searchFields,
    {},
    limit,
    offset
  )

  const result: SearchResult<ImdbTitle> = {
    data: results,
    pagination: {
      total,
      limit,
      offset,
      hasMore: offset + results.length < total,
    },
    timing: {
      fetchMs: Math.round(fetchMs * 100) / 100,
      searchMs: Math.round(searchMs * 100) / 100,
      totalMs: Math.round((performance.now() - startTotal) * 100) / 100,
    },
  }

  return Response.json(result, {
    headers: {
      'Cache-Control': 'public, max-age=300',
      'Content-Type': 'application/json',
    },
  })
}

// =============================================================================
// Main Handler
// =============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // Add timing header for observability
    const startTime = performance.now()

    try {
      // Health check
      if (path === '/search/health' || path === '/health') {
        // Test R2 access
        let r2Status = 'unknown'
        try {
          const testObj = await env.DATA.head('onet-occupations.json')
          r2Status = testObj ? `ok (size: ${testObj.size})` : 'object not found'
        } catch (err) {
          r2Status = `error: ${err instanceof Error ? err.message : 'unknown'}`
        }

        return Response.json({
          status: 'ok',
          datasets: Object.keys(DATASETS),
          cached: Array.from(dataCache.keys()),
          r2Status,
          hasDataBinding: !!env.DATA,
        })
      }

      // O*NET search
      if (path === '/search/onet' || path === '/onet') {
        const response = await handleOnetSearch(url.searchParams, env)
        response.headers.set('X-Response-Time', `${Math.round(performance.now() - startTime)}ms`)
        return response
      }

      // UNSPSC search
      if (path === '/search/unspsc' || path === '/unspsc') {
        const response = await handleUnspscSearch(url.searchParams, env)
        response.headers.set('X-Response-Time', `${Math.round(performance.now() - startTime)}ms`)
        return response
      }

      // IMDB search
      if (path === '/search/imdb' || path === '/imdb') {
        const response = await handleImdbSearch(url.searchParams, env)
        response.headers.set('X-Response-Time', `${Math.round(performance.now() - startTime)}ms`)
        return response
      }

      // Root path - show available endpoints
      if (path === '/' || path === '/search' || path === '/search/') {
        return Response.json(
          {
            name: 'ParqueDB Search API',
            version: '1.0.0',
            endpoints: {
              '/search/onet': {
                description: 'Search O*NET occupations',
                params: ['q', 'code', 'limit', 'offset'],
                example: '/search/onet?q=engineer',
              },
              '/search/unspsc': {
                description: 'Search UNSPSC categories',
                params: ['q', 'commodity', 'family', 'segment', 'limit', 'offset'],
                example: '/search/unspsc?q=computer',
              },
              '/search/imdb': {
                description: 'Search IMDB titles',
                params: ['q', 'type', 'genre', 'year', 'limit', 'offset'],
                example: '/search/imdb?q=matrix',
              },
              '/search/health': {
                description: 'Health check',
              },
            },
          },
          {
            headers: {
              'Content-Type': 'application/json',
            },
          }
        )
      }

      // Not found
      return new Response(
        JSON.stringify({
          error: 'Not Found',
          message: `Unknown endpoint: ${path}`,
          availableEndpoints: ['/search/onet', '/search/unspsc', '/search/imdb', '/search/health'],
        }),
        {
          status: 404,
          headers: { 'Content-Type': 'application/json' },
        }
      )
    } catch (error) {
      console.error('Search error:', error)
      return new Response(
        JSON.stringify({
          error: 'Internal Server Error',
          message: error instanceof Error ? error.message : 'Unknown error',
        }),
        {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        }
      )
    }
  },
}
