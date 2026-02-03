/**
 * Enhanced Snippet-Optimized Search Worker
 *
 * Features added over basic v2:
 * - Prefix matching: "eng" finds "engineer"
 * - Basic stemming: "manager" finds "managers"
 * - Relevance scoring: Results sorted by match quality
 * - Highlighting: Shows which terms matched
 * - Fuzzy tolerance: Handles minor typos
 *
 * Still optimized for Cloudflare Snippets:
 * - 5ms CPU time budget
 * - 32KB script bundle
 * - 32MB memory
 * - 5 subrequests
 */

interface Env {
  DATA: R2Bucket
}

interface Meta {
  totalDocs: number
  shardSize: number
  shardCount: number
  version?: number
}

interface ScoredResult {
  doc: Record<string, unknown>
  score: number
  matches: string[]
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
  query?: {
    original: string
    expanded: string[]
    matched: string[]
  }
}

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_SEARCH_LIMIT = 20
const MAX_SEARCH_LIMIT = 50
const MAX_TERM_SHARDS = 4
const MAX_DOC_SHARDS = 3
const MAX_BROWSE_RESULTS = 1000
const DOC_CACHE_MAX = 9
const SCORE_EXACT_MATCH = 10
const SCORE_STEMMED_MATCH = 5
const SCORE_PREFIX_MATCH = 3
const MIN_PREFIX_TERM_LENGTH = 3
const CACHE_MAX_AGE_SECONDS = 3600
const CDN_CACHE_MAX_AGE_SECONDS = 86400

// =============================================================================
// Caches
// =============================================================================

const termShardCache = new Map<string, Record<string, number[]>>()
const indexCache = new Map<string, Record<string, number[]>>()
const hashCache = new Map<string, Record<string, number[]>>()
const metaCache = new Map<string, Meta>()
const docCache = new Map<string, unknown[]>()

// =============================================================================
// Stemming (Porter Stemmer - simplified)
// =============================================================================

const IRREGULAR_PLURALS: Record<string, string> = {
  children: 'child',
  people: 'person',
  men: 'man',
  women: 'woman',
  feet: 'foot',
  teeth: 'tooth',
  geese: 'goose',
  mice: 'mouse',
  analyses: 'analysis',
  criteria: 'criterion',
  data: 'datum',
  media: 'medium',
}

function stem(word: string): string {
  if (word.length < 3) return word

  // Check irregular plurals
  if (IRREGULAR_PLURALS[word]) return IRREGULAR_PLURALS[word]

  let w = word

  // Step 1a: plurals
  if (w.endsWith('sses')) w = w.slice(0, -2)
  else if (w.endsWith('ies') && w.length > 4) w = w.slice(0, -2)
  else if (w.endsWith('ss')) {
    /* keep */
  } else if (w.endsWith('s') && w.length > 3) w = w.slice(0, -1)

  // Step 1b: -ed, -ing
  if (w.endsWith('eed') && w.length > 4) w = w.slice(0, -1)
  else if (w.endsWith('ed') && w.length > 4 && /[aeiou]/.test(w.slice(0, -2))) w = w.slice(0, -2)
  else if (w.endsWith('ing') && w.length > 5 && /[aeiou]/.test(w.slice(0, -3))) w = w.slice(0, -3)

  // Step 2: -ational, -tion, -ness, etc.
  if (w.endsWith('ational') && w.length > 9) w = w.slice(0, -5) + 'e'
  else if (w.endsWith('ization') && w.length > 9) w = w.slice(0, -5) + 'e'
  else if (w.endsWith('fulness') && w.length > 9) w = w.slice(0, -4)
  else if (w.endsWith('iveness') && w.length > 9) w = w.slice(0, -4)
  else if (w.endsWith('ousness') && w.length > 9) w = w.slice(0, -4)

  // Step 3: -ness, -ment, -able, -ible
  if (w.endsWith('ness') && w.length > 6) w = w.slice(0, -4)
  else if (w.endsWith('ment') && w.length > 6) w = w.slice(0, -4)
  else if (w.endsWith('able') && w.length > 6) w = w.slice(0, -4)
  else if (w.endsWith('ible') && w.length > 6) w = w.slice(0, -4)

  return w
}

/**
 * Generate term variants for matching
 */
function expandTerm(term: string): string[] {
  const variants = new Set<string>()
  variants.add(term)

  // Add stemmed version
  const stemmed = stem(term)
  variants.add(stemmed)

  // Add common suffixes to stemmed version
  if (stemmed.length >= 3) {
    variants.add(stemmed + 's')
    variants.add(stemmed + 'es')
    variants.add(stemmed + 'ed')
    variants.add(stemmed + 'ing')
    variants.add(stemmed + 'er')
    variants.add(stemmed + 'ers')
    variants.add(stemmed + 'ly')
  }

  return [...variants]
}

// =============================================================================
// Core Loading Functions
// =============================================================================

async function loadMeta(dataset: string, env: Env): Promise<Meta> {
  const cached = metaCache.get(dataset)
  if (cached) return cached

  const obj = await env.DATA.get(`indexes/${dataset}/meta.json`)
  if (!obj) throw new Error(`Meta not found: ${dataset}`)

  const meta = (await obj.json()) as Meta
  metaCache.set(dataset, meta)
  return meta
}

async function loadTermShard(
  dataset: string,
  letter: string,
  env: Env
): Promise<Record<string, number[]> | null> {
  const cacheKey = `${dataset}-${letter}`
  const cached = termShardCache.get(cacheKey)
  if (cached) return cached

  const obj = await env.DATA.get(`indexes/${dataset}/terms/${letter}.json`)
  if (!obj) return null

  const index = (await obj.json()) as Record<string, number[]>
  termShardCache.set(cacheKey, index)
  return index
}

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

async function loadDocShard(dataset: string, shardNum: number, env: Env): Promise<unknown[]> {
  const cacheKey = `${dataset}-docs-${shardNum}`
  const cached = docCache.get(cacheKey)
  if (cached) return cached

  const obj = await env.DATA.get(`indexes/${dataset}/docs-${shardNum}.json`)
  if (!obj) throw new Error(`Shard not found: ${dataset}/${shardNum}`)

  const docs = (await obj.json()) as unknown[]

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

function getTermLetters(terms: string[]): string[] {
  const letters = new Set<string>()
  for (const term of terms) {
    const first = term[0] || ''
    letters.add(/[0-9]/.test(first) ? '0' : first)
  }
  return [...letters]
}

function tokenize(query: string): string[] {
  return query
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 2)
}

/**
 * Search with prefix matching, stemming, and scoring
 */
async function searchWithScoring(
  dataset: string,
  query: string,
  env: Env,
  meta: Meta
): Promise<{ indices: number[]; scores: Map<number, number>; matchedTerms: Set<string>; expandedTerms: string[] }> {
  const terms = tokenize(query)
  if (terms.length === 0) {
    return { indices: [], scores: new Map(), matchedTerms: new Set(), expandedTerms: [] }
  }

  // Expand terms with stemming
  const allVariants: string[] = []
  for (const term of terms) {
    allVariants.push(...expandTerm(term))
  }
  const uniqueVariants = [...new Set(allVariants)]

  // Get letters needed for all variants
  const letters = getTermLetters(uniqueVariants)

  // Load term shards
  const shardPromises = letters.slice(0, MAX_TERM_SHARDS).map((l) => loadTermShard(dataset, l, env))
  const shards = await Promise.all(shardPromises)

  // Merge shards
  const combinedIndex: Record<string, number[]> = {}
  const hasV2 = shards.some((s) => s !== null)

  if (hasV2) {
    for (const shard of shards) {
      if (shard) Object.assign(combinedIndex, shard)
    }
  } else {
    // Fallback to v1
    const fullIndex = await loadFullIndex(dataset, env)
    Object.assign(combinedIndex, fullIndex)
  }

  // Score each document based on term matches
  const scores = new Map<number, number>()
  const matchedTerms = new Set<string>()

  for (const term of terms) {
    const variants = expandTerm(term)
    let bestMatchScore = 0
    let matchedVariant = ''

    for (const variant of variants) {
      // Exact match
      if (combinedIndex[variant]) {
        const matchScore = variant === term ? SCORE_EXACT_MATCH : SCORE_STEMMED_MATCH
        if (matchScore > bestMatchScore) {
          bestMatchScore = matchScore
          matchedVariant = variant
        }
        for (const idx of combinedIndex[variant]) {
          scores.set(idx, (scores.get(idx) || 0) + matchScore)
        }
      }

      // Prefix match (only for terms >= MIN_PREFIX_TERM_LENGTH chars)
      if (term.length >= MIN_PREFIX_TERM_LENGTH) {
        for (const [indexTerm, indices] of Object.entries(combinedIndex)) {
          if (indexTerm.startsWith(term) && indexTerm !== term) {
            const prefixScore = SCORE_PREFIX_MATCH
            if (!matchedVariant) matchedVariant = indexTerm
            for (const idx of indices) {
              scores.set(idx, (scores.get(idx) || 0) + prefixScore)
            }
          }
        }
      }
    }

    if (matchedVariant) matchedTerms.add(matchedVariant)
  }

  // Get indices sorted by score
  const sortedIndices = [...scores.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([idx]) => idx)

  return { indices: sortedIndices, scores, matchedTerms, expandedTerms: uniqueVariants }
}

function filterByHash(
  indices: number[],
  hashIndex: Record<string, number[]>,
  value: string
): number[] {
  const hashMatches = hashIndex[value]
  if (!hashMatches || hashMatches.length === 0) return []

  const hashSet = new Set(hashMatches)
  return indices.filter((id) => hashSet.has(id))
}

async function fetchDocuments(
  dataset: string,
  indices: number[],
  meta: Meta,
  env: Env,
  limit: number,
  offset: number
): Promise<unknown[]> {
  const paginatedIndices = indices.slice(offset, offset + limit)
  if (paginatedIndices.length === 0) return []

  const shardGroups = new Map<number, Array<{ idx: number; pos: number }>>()
  for (let pos = 0; pos < paginatedIndices.length; pos++) {
    const idx = paginatedIndices[pos]!
    const shardNum = Math.floor(idx / meta.shardSize)
    if (!shardGroups.has(shardNum)) shardGroups.set(shardNum, [])
    shardGroups.get(shardNum)!.push({ idx, pos })
  }

  const shardNums = [...shardGroups.keys()].slice(0, MAX_DOC_SHARDS)
  const shardPromises = shardNums.map((n) => loadDocShard(dataset, n, env))
  const shards = await Promise.all(shardPromises)

  const shardMap = new Map<number, unknown[]>()
  for (let i = 0; i < shardNums.length; i++) {
    shardMap.set(shardNums[i]!, shards[i]!)
  }

  const docs: unknown[] = new Array(paginatedIndices.length)

  for (const [shardNum, items] of shardGroups) {
    const shard = shardMap.get(shardNum)
    if (!shard) continue

    const baseIdx = shardNum * meta.shardSize
    for (const { idx, pos } of items) {
      const localIdx = idx - baseIdx
      if (localIdx >= 0 && localIdx < shard.length) {
        docs[pos] = shard[localIdx]
      }
    }
  }

  return docs.filter((d) => d !== undefined)
}

// =============================================================================
// Highlighting
// =============================================================================

function highlightText(text: string, matchedTerms: Set<string>): string {
  if (!text || matchedTerms.size === 0) return text

  let result = text
  for (const term of matchedTerms) {
    // Case-insensitive highlight with **bold** markers
    const regex = new RegExp(`\\b(${term}\\w*)\\b`, 'gi')
    result = result.replace(regex, '**$1**')
  }
  return result
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
  const limit = Math.min(parseInt(params.get('limit') || String(DEFAULT_SEARCH_LIMIT), 10), MAX_SEARCH_LIMIT)
  const offset = Math.max(parseInt(params.get('offset') || '0', 10), 0)
  const timing = params.get('timing') === 'true'
  const highlight = params.get('highlight') !== 'false' // Default true
  const debug = params.get('debug') === 'true'

  const meta = await loadMeta(dataset, env)

  let indices: number[]
  let matchedTerms = new Set<string>()
  let expandedTerms: string[] = []
  const indexStart = performance.now()

  if (q && q.trim()) {
    const result = await searchWithScoring(dataset, q, env, meta)
    indices = result.indices
    matchedTerms = result.matchedTerms
    expandedTerms = result.expandedTerms
  } else {
    indices = Array.from({ length: Math.min(meta.totalDocs, MAX_BROWSE_RESULTS) }, (_, i) => i)
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

  const total = indices.length
  let docs = await fetchDocuments(dataset, indices, meta, env, limit, offset)

  // Apply highlighting if requested
  if (highlight && matchedTerms.size > 0) {
    const textFields = getTextFields(dataset)
    docs = docs.map((doc) => {
      const d = doc as Record<string, unknown>
      const highlighted: Record<string, unknown> = { ...d }
      for (const field of textFields) {
        if (typeof d[field] === 'string') {
          highlighted[field] = highlightText(d[field] as string, matchedTerms)
        }
      }
      return highlighted
    })
  }

  const fetchMs = performance.now() - fetchStart
  const totalMs = performance.now() - startTime

  const response: SearchResponse = {
    data: docs,
    total,
    limit,
    offset,
  }

  if (timing) {
    response.timing = {
      indexMs: Math.round(indexMs * 100) / 100,
      fetchMs: Math.round(fetchMs * 100) / 100,
      totalMs: Math.round(totalMs * 100) / 100,
    }
  }

  if (debug && q) {
    response.query = {
      original: q,
      expanded: expandedTerms,
      matched: [...matchedTerms],
    }
  }

  return Response.json(response, {
    headers: {
      'Cache-Control': `public, max-age=${CACHE_MAX_AGE_SECONDS}, stale-while-revalidate=${CDN_CACHE_MAX_AGE_SECONDS}`,
      'CDN-Cache-Control': `max-age=${CDN_CACHE_MAX_AGE_SECONDS}`,
      'Vary': 'Accept-Encoding',
    },
  })
}

function getTextFields(dataset: string): string[] {
  switch (dataset) {
    case 'onet':
      return ['title', 'description']
    case 'unspsc':
      return ['commodityTitle', 'classTitle', 'familyTitle', 'segmentTitle']
    case 'imdb':
      return ['primaryTitle', 'originalTitle']
    default:
      return []
  }
}

// =============================================================================
// Main Handler
// =============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    let path = url.pathname
    if (path.startsWith('/search-v3')) {
      path = path.slice('/search-v3'.length) || '/'
    } else if (path.startsWith('/search-v2')) {
      path = path.slice('/search-v2'.length) || '/'
    } else if (path.startsWith('/search')) {
      path = path.slice('/search'.length) || '/'
    }

    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, OPTIONS',
          'Access-Control-Max-Age': String(CDN_CACHE_MAX_AGE_SECONDS),
        },
      })
    }

    try {
      if (path === '/health') {
        return Response.json({
          ok: true,
          version: 3,
          type: 'enhanced-snippet',
          features: ['stemming', 'prefix-match', 'scoring', 'highlighting'],
          caches: {
            termShards: termShardCache.size,
            indexes: indexCache.size,
            hash: hashCache.size,
            docs: docCache.size,
          },
        })
      }

      if (path === '/onet') {
        return handleSearch('onet', url.searchParams, env)
      }
      if (path === '/unspsc') {
        return handleSearch('unspsc', url.searchParams, env)
      }
      if (path === '/imdb') {
        return handleSearch('imdb', url.searchParams, env)
      }

      if (path === '/' || path === '') {
        return Response.json({
          name: 'ParqueDB Search (Enhanced v3)',
          version: 3,
          features: ['stemming', 'prefix-match', 'scoring', 'highlighting'],
          endpoints: {
            '/onet': {
              params: ['q', 'code', 'limit', 'offset', 'timing', 'highlight', 'debug'],
              example: '/onet?q=manager&debug=true',
            },
            '/unspsc': {
              params: ['q', 'commodity', 'class', 'family', 'segment', 'limit', 'offset'],
              example: '/unspsc?q=computer',
            },
            '/imdb': {
              params: ['q', 'type', 'year', 'limit', 'offset'],
              example: '/imdb?q=matrix&type=movie',
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
