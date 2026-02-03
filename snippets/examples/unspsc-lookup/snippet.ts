/**
 * UNSPSC Category Lookup Snippet
 *
 * Look up UNSPSC (United Nations Standard Products and Services Code) categories.
 * Provides hierarchical category information from commodity to segment level.
 *
 * Endpoints:
 * - GET /unspsc?q=pet               Search by title (any level)
 * - GET /unspsc?commodity=10111302  Get by commodity code
 * - GET /unspsc?segment=10000000    Get all in segment
 * - GET /unspsc?family=10110000     Get all in family
 * - GET /unspsc/:code               Get single commodity
 * - GET /unspsc/segments            List all segments
 *
 * Data source: O*NET UNSPSC Reference (4,262 categories)
 * Expected JSON at: cdn.workers.do/parquedb-benchmarks/snippets/unspsc.json
 */

// =============================================================================
// Types
// =============================================================================

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

interface Env {
  ASSETS?: Fetcher
}

// =============================================================================
// Configuration
// =============================================================================

const DATA_URL = 'https://cdn.workers.do/parquedb-benchmarks/snippets/unspsc.json'

// Cache for loaded data
let cachedData: UnspscEntry[] | null = null

// =============================================================================
// Handler
// =============================================================================

export default {
  async fetch(request: Request, env?: Env): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // Normalize path (support /search/unspsc)
    const normalizedPath = path.replace(/^\/search/, '')

    // GET /unspsc/:code - Get single commodity by code
    const codeMatch = normalizedPath.match(/^\/unspsc\/(\d{8})$/)
    if (codeMatch) {
      return getCommodity(codeMatch[1], env)
    }

    // GET /unspsc/segments - List all top-level segments
    if (normalizedPath === '/unspsc/segments') {
      return getSegments(env)
    }

    // GET /unspsc - Search/filter UNSPSC codes
    if (normalizedPath === '/unspsc') {
      return searchUnspsc(url.searchParams, env)
    }

    return new Response('Not Found. Try /unspsc?q=pet or /unspsc/segments', { status: 404 })
  },
}

// =============================================================================
// Data Loading
// =============================================================================

/**
 * Load UNSPSC data from JSON
 */
async function loadUnspsc(env?: Env): Promise<UnspscEntry[]> {
  if (cachedData) return cachedData

  const response = env?.ASSETS
    ? await env.ASSETS.fetch(DATA_URL)
    : await fetch(DATA_URL)

  if (!response.ok) {
    throw new Error(`Failed to fetch data: ${response.status}`)
  }

  cachedData = await response.json() as UnspscEntry[]
  return cachedData
}

// =============================================================================
// Search Functions
// =============================================================================

/**
 * Search UNSPSC entries
 */
async function searchUnspsc(
  params: URLSearchParams,
  env?: Env
): Promise<Response> {
  try {
    const q = params.get('q')?.toLowerCase()
    const commodity = params.get('commodity')
    const classCode = params.get('class')
    const family = params.get('family')
    const segment = params.get('segment')
    const limit = Math.min(parseInt(params.get('limit') ?? '50', 10), 100)
    const offset = parseInt(params.get('offset') ?? '0', 10)

    const entries = await loadUnspsc(env)
    let filtered = entries

    // Filter by text query (searches all title fields)
    if (q) {
      filtered = filtered.filter(entry =>
        entry.commodityTitle.toLowerCase().includes(q) ||
        entry.classTitle.toLowerCase().includes(q) ||
        entry.familyTitle.toLowerCase().includes(q) ||
        entry.segmentTitle.toLowerCase().includes(q)
      )
    }

    // Filter by commodity code
    if (commodity) {
      filtered = filtered.filter(entry => entry.commodityCode === commodity)
    }

    // Filter by class code
    if (classCode) {
      filtered = filtered.filter(entry => entry.classCode === classCode)
    }

    // Filter by family code
    if (family) {
      filtered = filtered.filter(entry => entry.familyCode === family)
    }

    // Filter by segment code
    if (segment) {
      filtered = filtered.filter(entry => entry.segmentCode === segment)
    }

    const total = filtered.length
    const paginated = filtered.slice(offset, offset + limit)

    return Response.json({
      data: paginated,
      pagination: {
        total,
        limit,
        offset,
        hasMore: offset + paginated.length < total,
      },
    }, {
      headers: {
        'Cache-Control': 'public, max-age=3600',
        'Content-Type': 'application/json',
      },
    })
  } catch (error) {
    console.error('Search error:', error)
    return new Response('Internal error', { status: 500 })
  }
}

/**
 * Get a single commodity by code
 */
async function getCommodity(code: string, env?: Env): Promise<Response> {
  try {
    const entries = await loadUnspsc(env)
    const entry = entries.find(e => e.commodityCode === code)

    if (!entry) {
      return new Response('Commodity not found', { status: 404 })
    }

    return Response.json(entry, {
      headers: {
        'Cache-Control': 'public, max-age=3600',
        'Content-Type': 'application/json',
      },
    })
  } catch (error) {
    console.error('Lookup error:', error)
    return new Response('Internal error', { status: 500 })
  }
}

/**
 * Get all unique segments (top-level categories)
 */
async function getSegments(env?: Env): Promise<Response> {
  try {
    const entries = await loadUnspsc(env)

    // Extract unique segments
    const segmentMap = new Map<string, { code: string; title: string; count: number }>()

    for (const entry of entries) {
      const existing = segmentMap.get(entry.segmentCode)
      if (existing) {
        existing.count++
      } else {
        segmentMap.set(entry.segmentCode, {
          code: entry.segmentCode,
          title: entry.segmentTitle,
          count: 1,
        })
      }
    }

    const segments = Array.from(segmentMap.values())
      .sort((a, b) => a.code.localeCompare(b.code))

    return Response.json({
      data: segments,
      total: segments.length,
    }, {
      headers: {
        'Cache-Control': 'public, max-age=3600',
        'Content-Type': 'application/json',
      },
    })
  } catch (error) {
    console.error('Segments error:', error)
    return new Response('Internal error', { status: 500 })
  }
}
