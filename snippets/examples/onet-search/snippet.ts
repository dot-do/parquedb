/**
 * O*NET Occupation Search Snippet
 *
 * Search O*NET occupation data by title or code.
 * Uses JSON for reliable edge deployment.
 *
 * Endpoints:
 * - GET /occupations?q=engineer         Search by title
 * - GET /occupations?code=11-1011       Search by SOC code prefix
 * - GET /occupations/:code              Get single occupation
 *
 * Data source: O*NET 29.2 Occupation Data (1,016 occupations)
 * Expected JSON at: cdn.workers.do/parquedb-benchmarks/snippets/onet-occupations.json
 */

// =============================================================================
// Types
// =============================================================================

interface Occupation {
  code: string
  title: string
  description: string
}

interface Env {
  ASSETS?: Fetcher
}

// =============================================================================
// Configuration
// =============================================================================

const DATA_URL = 'https://cdn.workers.do/parquedb-benchmarks/snippets/onet-occupations.json'

// Cache for loaded data (persists within worker instance)
let cachedData: Occupation[] | null = null

// =============================================================================
// Handler
// =============================================================================

export default {
  async fetch(request: Request, env?: Env): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // Support both /occupations and /search/occupations paths
    // Also support /onet as an alias
    const normalizedPath = path
      .replace(/^\/search/, '')
      .replace(/^\/onet/, '/occupations')

    // GET /occupations/:code - Get single occupation
    const codeMatch = normalizedPath.match(/^\/occupations\/(\d{2}-\d{4}(?:\.\d{2})?)$/)
    if (codeMatch) {
      return getOccupation(codeMatch[1], env)
    }

    // GET /occupations - Search occupations
    if (normalizedPath === '/occupations') {
      return searchOccupations(url.searchParams, env)
    }

    return new Response('Not Found. Try /search/occupations?q=engineer', { status: 404 })
  },
}

// =============================================================================
// Data Loading
// =============================================================================

/**
 * Load occupation data from JSON
 */
async function loadOccupations(env?: Env): Promise<Occupation[]> {
  if (cachedData) return cachedData

  const response = env?.ASSETS
    ? await env.ASSETS.fetch(DATA_URL)
    : await fetch(DATA_URL)

  if (!response.ok) {
    throw new Error(`Failed to fetch data: ${response.status}`)
  }

  cachedData = await response.json() as Occupation[]
  return cachedData
}

// =============================================================================
// Search Functions
// =============================================================================

/**
 * Search occupations by query or code prefix
 */
async function searchOccupations(
  params: URLSearchParams,
  env?: Env
): Promise<Response> {
  try {
    const q = params.get('q')?.toLowerCase()
    const codePrefix = params.get('code')
    const limit = Math.min(parseInt(params.get('limit') ?? '50', 10), 100)
    const offset = parseInt(params.get('offset') ?? '0', 10)

    const occupations = await loadOccupations(env)

    let filtered = occupations

    // Filter by query (searches title and description)
    if (q) {
      filtered = filtered.filter(occ =>
        occ.title.toLowerCase().includes(q) ||
        occ.description.toLowerCase().includes(q)
      )
    }

    // Filter by SOC code prefix
    if (codePrefix) {
      filtered = filtered.filter(occ => occ.code.startsWith(codePrefix))
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
 * Get a single occupation by exact code
 */
async function getOccupation(code: string, env?: Env): Promise<Response> {
  try {
    const occupations = await loadOccupations(env)

    // Try exact match first
    let occupation = occupations.find(occ => occ.code === code)

    // If not found and code doesn't have decimal, try with .00
    if (!occupation && !code.includes('.')) {
      occupation = occupations.find(occ => occ.code === `${code}.00`)
    }

    if (!occupation) {
      return new Response('Occupation not found', { status: 404 })
    }

    return Response.json(occupation, {
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
