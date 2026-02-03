/**
 * IMDB Title Search Snippet
 *
 * Search IMDB titles (movies, TV shows, etc.) from JSON data.
 *
 * Endpoints:
 * - GET /titles?q=godfather          Search by title
 * - GET /titles?type=movie           Filter by title type
 * - GET /titles?genre=Drama          Filter by genre
 * - GET /titles?year=2020            Filter by start year
 * - GET /titles/:tconst              Get single title (e.g., tt0068646)
 * - GET /titles/types                List available title types
 *
 * Data source: IMDB title.basics subset (10,000 titles from 2000+)
 * Expected JSON at: cdn.workers.do/parquedb-benchmarks/snippets/imdb-titles.json
 */

// =============================================================================
// Types
// =============================================================================

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

interface Env {
  ASSETS?: Fetcher
}

// =============================================================================
// Configuration
// =============================================================================

const DATA_URL = 'https://cdn.workers.do/parquedb-benchmarks/snippets/imdb-titles.json'

// Cache for loaded data
let cachedData: ImdbTitle[] | null = null

// =============================================================================
// Handler
// =============================================================================

export default {
  async fetch(request: Request, env?: Env): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // Normalize path (support /search/titles, /search/imdb)
    const normalizedPath = path
      .replace(/^\/search/, '')
      .replace(/^\/imdb/, '/titles')

    // GET /titles/:tconst - Get single title
    const tconstMatch = normalizedPath.match(/^\/titles\/(tt\d+)$/)
    if (tconstMatch) {
      return getTitle(tconstMatch[1], env)
    }

    // GET /titles/types - List available title types
    if (normalizedPath === '/titles/types') {
      return getTitleTypes(env)
    }

    // GET /titles - Search titles
    if (normalizedPath === '/titles') {
      return searchTitles(url.searchParams, env)
    }

    return new Response('Not Found. Try /titles?q=godfather or /titles/types', { status: 404 })
  },
}

// =============================================================================
// Data Loading
// =============================================================================

/**
 * Load titles from JSON
 */
async function loadTitles(env?: Env): Promise<ImdbTitle[]> {
  if (cachedData) return cachedData

  const response = env?.ASSETS
    ? await env.ASSETS.fetch(DATA_URL)
    : await fetch(DATA_URL)

  if (!response.ok) {
    throw new Error(`Failed to fetch data: ${response.status}`)
  }

  cachedData = await response.json() as ImdbTitle[]
  return cachedData
}

// =============================================================================
// Search Functions
// =============================================================================

/**
 * Search titles with various filters
 */
async function searchTitles(
  params: URLSearchParams,
  env?: Env
): Promise<Response> {
  try {
    const q = params.get('q')?.toLowerCase()
    const type = params.get('type')
    const genre = params.get('genre')
    const year = params.get('year') ? parseInt(params.get('year')!, 10) : null
    const minYear = params.get('minYear') ? parseInt(params.get('minYear')!, 10) : null
    const maxYear = params.get('maxYear') ? parseInt(params.get('maxYear')!, 10) : null
    const adult = params.get('adult')
    const limit = Math.min(parseInt(params.get('limit') ?? '20', 10), 100)
    const offset = parseInt(params.get('offset') ?? '0', 10)

    const titles = await loadTitles(env)
    let filtered = titles

    // Filter by text query
    if (q) {
      filtered = filtered.filter(title =>
        title.primaryTitle.toLowerCase().includes(q) ||
        title.originalTitle.toLowerCase().includes(q)
      )
    }

    // Filter by title type
    if (type) {
      filtered = filtered.filter(title => title.titleType === type)
    }

    // Filter by genre
    if (genre) {
      const genreLower = genre.toLowerCase()
      filtered = filtered.filter(title =>
        title.genres.some(g => g.toLowerCase() === genreLower)
      )
    }

    // Filter by exact year
    if (year) {
      filtered = filtered.filter(title => title.startYear === year)
    }

    // Filter by year range
    if (minYear) {
      filtered = filtered.filter(title =>
        title.startYear !== null && title.startYear >= minYear
      )
    }
    if (maxYear) {
      filtered = filtered.filter(title =>
        title.startYear !== null && title.startYear <= maxYear
      )
    }

    // Filter by adult content
    if (adult !== null) {
      const showAdult = adult === 'true'
      filtered = filtered.filter(title => title.isAdult === showAdult)
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
        'Cache-Control': 'public, max-age=300',
        'Content-Type': 'application/json',
      },
    })
  } catch (error) {
    console.error('Search error:', error)
    const message = error instanceof Error ? error.message : 'Internal error'
    return new Response(message, { status: 500 })
  }
}

/**
 * Get a single title by tconst
 */
async function getTitle(tconst: string, env?: Env): Promise<Response> {
  try {
    const titles = await loadTitles(env)
    const title = titles.find(t => t.tconst === tconst)

    if (!title) {
      return new Response('Title not found', { status: 404 })
    }

    return Response.json(title, {
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
 * Get available title types
 */
async function getTitleTypes(env?: Env): Promise<Response> {
  try {
    const titles = await loadTitles(env)

    // Count titles by type
    const typeCounts = new Map<string, number>()
    for (const title of titles) {
      typeCounts.set(title.titleType, (typeCounts.get(title.titleType) ?? 0) + 1)
    }

    const types = Array.from(typeCounts.entries())
      .map(([type, count]) => ({ type, count }))
      .sort((a, b) => b.count - a.count)

    return Response.json({
      data: types,
      total: types.length,
    }, {
      headers: {
        'Cache-Control': 'public, max-age=3600',
        'Content-Type': 'application/json',
      },
    })
  } catch (error) {
    console.error('Types error:', error)
    return new Response('Internal error', { status: 500 })
  }
}
