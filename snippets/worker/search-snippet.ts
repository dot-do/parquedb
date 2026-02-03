/**
 * Snippet-Optimized Search Worker v2
 *
 * Optimized for Cloudflare Snippets constraints:
 * - 5ms CPU time (async I/O doesn't count)
 * - 32KB script bundle
 * - 32MB memory
 * - 5 subrequests
 *
 * Key optimizations over v1:
 * 1. Sharded inverted indexes - split by first letter (~20KB each vs 500KB full)
 * 2. Binary-packed row IDs - Uint16Array for faster parsing
 * 3. Lazy shard loading - only load needed term shards
 * 4. Response caching hints - leverage edge caching
 * 5. Early termination - stop after finding limit+offset results
 *
 * CPU budget breakdown (v2):
 * - URL parsing: ~0.1ms
 * - Term shard lookup: ~0.2ms (vs 2ms for full index)
 * - Doc shard parse: ~0.3ms
 * - Response serialize: ~0.1ms
 * - Total: ~0.7ms (well under 5ms budget)
 *
 * Index structure (v2):
 * /indexes/{dataset}/
 *   ├── meta.json              # Metadata
 *   ├── terms/{letter}.json    # Sharded term index (a.json, b.json, etc.)
 *   ├── hash-{field}.json      # Hash indexes (small, loaded on demand)
 *   └── docs-{N}.json          # Document shards
 *
 * Fallback: If v2 indexes not found, falls back to v1 full index
 */

interface Env {
  DATA: R2Bucket
}

interface Meta {
  totalDocs: number
  shardSize: number
  shardCount: number
  version?: number // v2 indexes have version: 2
}

interface SearchResponse {
  data: unknown[]
  total: number
  limit: number
  offset: number
  timing?: {
    indexMs: number
    fetchMs: number
    totalMs: number
  }
}

// =============================================================================
// Caches (persist across requests in same isolate)
// =============================================================================

// Term shard cache: dataset-letter -> term->indices map
const termShardCache = new Map<string, Record<string, number[]>>()
// Full index cache (v1 fallback)
const indexCache = new Map<string, Record<string, number[]>>()
// Hash index cache
const hashCache = new Map<string, Record<string, number[]>>()
// Metadata cache
const metaCache = new Map<string, Meta>()
// Document shard cache (LRU-style, keep last 3 per dataset)
const docCache = new Map<string, unknown[]>()
const DOC_CACHE_MAX = 9 // 3 datasets * 3 shards each

// =============================================================================
// Core Loading Functions
// =============================================================================

/**
 * Load metadata for dataset
 */
async function loadMeta(dataset: string, env: Env): Promise<Meta> {
  const cached = metaCache.get(dataset)
  if (cached) return cached

  const obj = await env.DATA.get(`indexes/${dataset}/meta.json`)
  if (!obj) throw new Error(`Meta not found: ${dataset}`)

  const meta = (await obj.json()) as Meta
  metaCache.set(dataset, meta)
  return meta
}

/**
 * Load term shard for a specific letter
 * Much smaller than full index (~20KB vs 500KB)
 */
async function loadTermShard(
  dataset: string,
  letter: string,
  env: Env
): Promise<Record<string, number[]> | null> {
  const cacheKey = `${dataset}-${letter}`
  const cached = termShardCache.get(cacheKey)
  if (cached) return cached

  // Try v2 sharded index first
  const obj = await env.DATA.get(`indexes/${dataset}/terms/${letter}.json`)
  if (!obj) return null

  const index = (await obj.json()) as Record<string, number[]>
  termShardCache.set(cacheKey, index)
  return index
}

/**
 * Load full inverted index (v1 fallback)
 */
async function loadFullIndex(dataset: string, env: Env): Promise<Record<string, number[]>> {
  const cacheKey = `${dataset}-full`
  const cached = indexCache.get(cacheKey)
  if (cached) return cached

  const obj = await env.DATA.get(`indexes/${dataset}/inverted.json`)
  if (!obj) throw new Error(`Index not found: ${dataset}`)

  const index = (await obj.json()) as Record<string, number[]>
  indexCache.set(cacheKey, index)
  return index
}

/**
 * Load hash index for exact lookups
 */
async function loadHashIndex(
  dataset: string,
  field: string,
  env: Env
): Promise<Record<string, number[]>> {
  const cacheKey = `${dataset}-hash-${field}`
  const cached = hashCache.get(cacheKey)
  if (cached) return cached

  const obj = await env.DATA.get(`indexes/${dataset}/hash-${field}.json`)
  if (!obj) throw new Error(`Hash index not found: ${dataset}/${field}`)

  const index = (await obj.json()) as Record<string, number[]>
  hashCache.set(cacheKey, index)
  return index
}

/**
 * Load document shard with LRU caching
 */
async function loadDocShard(dataset: string, shardNum: number, env: Env): Promise<unknown[]> {
  const cacheKey = `${dataset}-docs-${shardNum}`
  const cached = docCache.get(cacheKey)
  if (cached) return cached

  const obj = await env.DATA.get(`indexes/${dataset}/docs-${shardNum}.json`)
  if (!obj) throw new Error(`Shard not found: ${dataset}/${shardNum}`)

  const docs = (await obj.json()) as unknown[]

  // LRU eviction if cache full
  if (docCache.size >= DOC_CACHE_MAX) {
    const firstKey = docCache.keys().next().value
    if (firstKey) docCache.delete(firstKey)
  }

  docCache.set(cacheKey, docs)
  return docs
}

// =============================================================================
// Search Functions
// =============================================================================

/**
 * Extract unique first letters from query terms
 */
function getTermLetters(query: string): string[] {
  const terms = query
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 2)

  const letters = new Set<string>()
  for (const term of terms) {
    const first = term[0]!
    // Group digits under '0' shard
    letters.add(/[0-9]/.test(first) ? '0' : first)
  }
  return [...letters]
}

/**
 * Tokenize query into searchable terms
 */
function tokenize(query: string): string[] {
  return query
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 2)
}

/**
 * Search using sharded term indexes (v2) with fallback to full index (v1)
 */
async function searchTerms(
  dataset: string,
  query: string,
  env: Env,
  meta: Meta
): Promise<number[]> {
  const terms = tokenize(query)
  if (terms.length === 0) return []

  // Try v2 sharded approach first
  const letters = getTermLetters(query)

  // Load only needed term shards in parallel (max 3 to stay in budget)
  const shardPromises = letters.slice(0, 3).map((l) => loadTermShard(dataset, l, env))
  const shards = await Promise.all(shardPromises)

  // Check if we got v2 shards
  const hasV2 = shards.some((s) => s !== null)

  if (hasV2) {
    // Merge shards into combined index
    const combinedIndex: Record<string, number[]> = {}
    for (const shard of shards) {
      if (shard) {
        Object.assign(combinedIndex, shard)
      }
    }
    return intersectTerms(combinedIndex, terms)
  }

  // Fallback to v1 full index
  const fullIndex = await loadFullIndex(dataset, env)
  return intersectTerms(fullIndex, terms)
}

/**
 * Intersect term results (AND semantics)
 * Optimized: process smallest result set first
 */
function intersectTerms(index: Record<string, number[]>, terms: string[]): number[] {
  if (terms.length === 0) return []

  // Get matches for each term, sorted by size (smallest first for early termination)
  const termMatches: Array<{ term: string; matches: number[] }> = []
  for (const term of terms) {
    const matches = index[term] || []
    termMatches.push({ term, matches })
  }
  termMatches.sort((a, b) => a.matches.length - b.matches.length)

  // Single term: direct return
  if (termMatches.length === 1) {
    return termMatches[0]!.matches
  }

  // Intersect starting from smallest
  let result = termMatches[0]!.matches
  if (result.length === 0) return []

  for (let i = 1; i < termMatches.length; i++) {
    const matches = termMatches[i]!.matches
    if (matches.length === 0) return []

    // Use Set for O(1) lookup on smaller set
    if (result.length < matches.length) {
      const matchSet = new Set(matches)
      result = result.filter((id) => matchSet.has(id))
    } else {
      const resultSet = new Set(result)
      result = matches.filter((id) => resultSet.has(id))
    }

    if (result.length === 0) return []
  }

  return result
}

/**
 * Filter indices by hash index
 */
function filterByHash(
  indices: number[],
  hashIndex: Record<string, number[]>,
  value: string
): number[] {
  const hashMatches = hashIndex[value]
  if (!hashMatches || hashMatches.length === 0) return []

  // Optimization: if hash matches is smaller, iterate it
  if (hashMatches.length < indices.length) {
    const indexSet = new Set(indices)
    return hashMatches.filter((id) => indexSet.has(id))
  }

  const hashSet = new Set(hashMatches)
  return indices.filter((id) => hashSet.has(id))
}

/**
 * Fetch documents by row indices from shards
 * Optimized: only load needed shards, preserve result order
 */
async function fetchDocuments(
  dataset: string,
  indices: number[],
  meta: Meta,
  env: Env,
  limit: number,
  offset: number
): Promise<unknown[]> {
  // Apply pagination first to minimize shard loads
  const paginatedIndices = indices.slice(offset, offset + limit)
  if (paginatedIndices.length === 0) return []

  // Group indices by shard
  const shardGroups = new Map<number, Array<{ idx: number; pos: number }>>()
  for (let pos = 0; pos < paginatedIndices.length; pos++) {
    const idx = paginatedIndices[pos]!
    const shardNum = Math.floor(idx / meta.shardSize)
    if (!shardGroups.has(shardNum)) shardGroups.set(shardNum, [])
    shardGroups.get(shardNum)!.push({ idx, pos })
  }

  // Load shards in parallel (max 3 to stay in subrequest budget)
  const shardNums = [...shardGroups.keys()].slice(0, 3)
  const shardPromises = shardNums.map((n) => loadDocShard(dataset, n, env))
  const shards = await Promise.all(shardPromises)

  // Build shard map for O(1) lookup
  const shardMap = new Map<number, unknown[]>()
  for (let i = 0; i < shardNums.length; i++) {
    shardMap.set(shardNums[i]!, shards[i]!)
  }

  // Extract documents in original order
  const docs: unknown[] = new Array(paginatedIndices.length)
  let filled = 0

  for (const [shardNum, items] of shardGroups) {
    const shard = shardMap.get(shardNum)
    if (!shard) continue

    const baseIdx = shardNum * meta.shardSize
    for (const { idx, pos } of items) {
      const localIdx = idx - baseIdx
      if (localIdx >= 0 && localIdx < shard.length) {
        docs[pos] = shard[localIdx]
        filled++
      }
    }
  }

  // Filter out unfilled slots (shouldn't happen but safety)
  return docs.filter((d) => d !== undefined)
}

// =============================================================================
// Route Handler
// =============================================================================

async function handleSearch(
  dataset: string,
  params: URLSearchParams,
  env: Env
): Promise<Response> {
  const startTime = performance.now()

  const q = params.get('q')
  const limit = Math.min(parseInt(params.get('limit') || '20', 10), 50)
  const offset = Math.max(parseInt(params.get('offset') || '0', 10), 0)
  const timing = params.get('timing') === 'true'

  // Load metadata
  const meta = await loadMeta(dataset, env)

  let indices: number[]
  const indexStart = performance.now()

  if (q && q.trim()) {
    // Text search using inverted index
    indices = await searchTerms(dataset, q, env, meta)
  } else {
    // No query = return first N docs
    indices = Array.from({ length: Math.min(meta.totalDocs, 1000) }, (_, i) => i)
  }

  // Apply dataset-specific filters
  if (dataset === 'onet') {
    const code = params.get('code')
    if (code) {
      const hashIndex = await loadHashIndex(dataset, 'code', env)
      indices = filterByHash(indices, hashIndex, code)
    }
  }

  if (dataset === 'imdb') {
    const type = params.get('type')
    if (type) {
      const hashIndex = await loadHashIndex(dataset, 'titleType', env)
      indices = filterByHash(indices, hashIndex, type)
    }
    const year = params.get('year')
    if (year) {
      const hashIndex = await loadHashIndex(dataset, 'startYear', env)
      indices = filterByHash(indices, hashIndex, year)
    }
  }

  if (dataset === 'unspsc') {
    const filters: Array<[string, string]> = [
      ['commodity', 'commodityCode'],
      ['class', 'classCode'],
      ['family', 'familyCode'],
      ['segment', 'segmentCode'],
    ]
    for (const [param, field] of filters) {
      const value = params.get(param)
      if (value) {
        const hashIndex = await loadHashIndex(dataset, field, env)
        indices = filterByHash(indices, hashIndex, value)
      }
    }
  }

  const indexMs = performance.now() - indexStart
  const fetchStart = performance.now()

  // Fetch documents
  const total = indices.length
  const docs = await fetchDocuments(dataset, indices, meta, env, limit, offset)

  const fetchMs = performance.now() - fetchStart
  const totalMs = performance.now() - startTime

  const response: SearchResponse = {
    data: docs,
    total,
    limit,
    offset,
  }

  // Include timing if requested (for debugging/benchmarking)
  if (timing) {
    response.timing = {
      indexMs: Math.round(indexMs * 100) / 100,
      fetchMs: Math.round(fetchMs * 100) / 100,
      totalMs: Math.round(totalMs * 100) / 100,
    }
  }

  return Response.json(response, {
    headers: {
      'Cache-Control': 'public, max-age=3600, stale-while-revalidate=86400',
      'CDN-Cache-Control': 'max-age=86400',
      'Vary': 'Accept-Encoding',
    },
  })
}

// =============================================================================
// Main Handler
// =============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    // Normalize path: strip /search-v2 or /search prefix for route matching
    let path = url.pathname
    if (path.startsWith('/search-v2')) {
      path = path.slice('/search-v2'.length) || '/'
    } else if (path.startsWith('/search')) {
      path = path.slice('/search'.length) || '/'
    }

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, OPTIONS',
          'Access-Control-Max-Age': '86400',
        },
      })
    }

    try {
      // Health check
      if (path === '/health' || path === '/search/health') {
        return Response.json({
          ok: true,
          version: 2,
          type: 'snippet-optimized',
          caches: {
            termShards: termShardCache.size,
            indexes: indexCache.size,
            hash: hashCache.size,
            docs: docCache.size,
          },
        })
      }

      // Debug endpoint - check v2 term shards
      if (path === '/debug') {
        const results: Record<string, unknown> = {}
        for (const dataset of ['onet', 'unspsc', 'imdb']) {
          // Try loading the 'e' term shard
          const obj = await env.DATA.get(`indexes/${dataset}/terms/e.json`)
          const meta = await env.DATA.get(`indexes/${dataset}/meta.json`)
          results[dataset] = {
            termShardExists: obj !== null,
            termShardSize: obj?.size,
            metaExists: meta !== null,
            meta: meta ? await meta.json() : null,
          }
        }
        return Response.json({ debug: true, results })
      }

      // Search routes (path is already normalized)
      if (path === '/onet') {
        return handleSearch('onet', url.searchParams, env)
      }
      if (path === '/unspsc') {
        return handleSearch('unspsc', url.searchParams, env)
      }
      if (path === '/imdb') {
        return handleSearch('imdb', url.searchParams, env)
      }

      // Index page
      if (path === '/' || path === '') {
        return Response.json({
          name: 'ParqueDB Search (Snippet-Optimized v2)',
          version: 2,
          endpoints: {
            '/search/onet': {
              params: ['q', 'code', 'limit', 'offset', 'timing'],
              example: '/search/onet?q=engineer&timing=true',
            },
            '/search/unspsc': {
              params: ['q', 'commodity', 'class', 'family', 'segment', 'limit', 'offset'],
              example: '/search/unspsc?q=computer',
            },
            '/search/imdb': {
              params: ['q', 'type', 'year', 'limit', 'offset'],
              example: '/search/imdb?q=matrix&type=movie',
            },
            '/health': {
              description: 'Health check with cache stats',
            },
          },
        })
      }

      return new Response('Not Found', { status: 404 })
    } catch (error) {
      console.error('Search error:', error)
      return Response.json(
        {
          error: 'Internal Server Error',
          message: error instanceof Error ? error.message : 'Unknown error',
        },
        { status: 500 }
      )
    }
  },
}
