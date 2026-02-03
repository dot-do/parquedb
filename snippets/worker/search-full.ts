/**
 * Full-Featured Snippet Search Worker (v4)
 *
 * Features:
 * - Full-text search with stemming & prefix matching
 * - Relevance scoring & highlighting
 * - Range filters: year_gte, year_lte, runtime_gte, runtime_lte
 * - Multi-value filters: type=movie,tvSeries
 * - Sorting: sort=year:desc, sort=title:asc
 * - Facets: Get counts by field values
 *
 * Snippet constraints:
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

interface SearchResponse {
  data: unknown[]
  total: number
  limit: number
  offset: number
  sort?: string
  filters?: Record<string, unknown>
  timing?: Record<string, number>
  query?: Record<string, unknown>
}

// =============================================================================
// Caches
// =============================================================================

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_SEARCH_LIMIT = 20
const MAX_SEARCH_LIMIT = 100
const MAX_TERM_SHARDS = 4
const MAX_DOC_SHARDS = 4
const MAX_BROWSE_RESULTS = 1000
const MAX_FETCH_COUNT = 500
const DOC_CACHE_MAX = 12
const SCORE_EXACT_MATCH = 10
const SCORE_STEMMED_MATCH = 5
const SCORE_PREFIX_MATCH = 3
const MIN_PREFIX_TERM_LENGTH = 3
const CACHE_MAX_AGE_SECONDS = 3600
const CDN_CACHE_MAX_AGE_SECONDS = 86400

// =============================================================================

const termShardCache = new Map<string, Record<string, number[]>>()
const indexCache = new Map<string, Record<string, number[]>>()
const hashCache = new Map<string, Record<string, number[]>>()
const metaCache = new Map<string, Meta>()
const docCache = new Map<string, unknown[]>()

// =============================================================================
// Stemming
// =============================================================================

const IRREGULAR: Record<string, string> = {
  children: 'child', people: 'person', men: 'man', women: 'woman',
  feet: 'foot', teeth: 'tooth', geese: 'goose', mice: 'mouse',
}

function stem(word: string): string {
  if (word.length < 3) return word
  if (IRREGULAR[word]) return IRREGULAR[word]
  let w = word
  if (w.endsWith('sses')) w = w.slice(0, -2)
  else if (w.endsWith('ies') && w.length > 4) w = w.slice(0, -2)
  else if (w.endsWith('ss')) { /* keep */ }
  else if (w.endsWith('s') && w.length > 3) w = w.slice(0, -1)
  if (w.endsWith('eed') && w.length > 4) w = w.slice(0, -1)
  else if (w.endsWith('ed') && w.length > 4 && /[aeiou]/.test(w.slice(0, -2))) w = w.slice(0, -2)
  else if (w.endsWith('ing') && w.length > 5 && /[aeiou]/.test(w.slice(0, -3))) w = w.slice(0, -3)
  if (w.endsWith('ness') && w.length > 6) w = w.slice(0, -4)
  else if (w.endsWith('ment') && w.length > 6) w = w.slice(0, -4)
  return w
}

function expandTerm(term: string): string[] {
  const variants = new Set<string>([term])
  const stemmed = stem(term)
  variants.add(stemmed)
  if (stemmed.length >= 3) {
    for (const suffix of ['s', 'es', 'ed', 'ing', 'er', 'ers']) {
      variants.add(stemmed + suffix)
    }
  }
  return [...variants]
}

// =============================================================================
// Data Loading
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

async function loadTermShard(dataset: string, letter: string, env: Env): Promise<Record<string, number[]> | null> {
  const key = `${dataset}-${letter}`
  if (termShardCache.has(key)) return termShardCache.get(key)!
  const obj = await env.DATA.get(`indexes/${dataset}/terms/${letter}.json`)
  if (!obj) return null
  const index = (await obj.json()) as Record<string, number[]>
  termShardCache.set(key, index)
  return index
}

async function loadFullIndex(dataset: string, env: Env): Promise<Record<string, number[]>> {
  const key = `${dataset}-full`
  if (indexCache.has(key)) return indexCache.get(key)!
  const obj = await env.DATA.get(`indexes/${dataset}/inverted.json`)
  if (!obj) throw new Error(`Index not found: ${dataset}`)
  const index = (await obj.json()) as Record<string, number[]>
  indexCache.set(key, index)
  return index
}

async function loadHashIndex(dataset: string, field: string, env: Env): Promise<Record<string, number[]>> {
  const key = `${dataset}-hash-${field}`
  if (hashCache.has(key)) return hashCache.get(key)!
  const obj = await env.DATA.get(`indexes/${dataset}/hash-${field}.json`)
  if (!obj) throw new Error(`Hash index not found: ${dataset}/${field}`)
  const index = (await obj.json()) as Record<string, number[]>
  hashCache.set(key, index)
  return index
}

async function loadDocShard(dataset: string, shardNum: number, env: Env): Promise<unknown[]> {
  const key = `${dataset}-docs-${shardNum}`
  if (docCache.has(key)) return docCache.get(key)!
  const obj = await env.DATA.get(`indexes/${dataset}/docs-${shardNum}.json`)
  if (!obj) throw new Error(`Shard not found: ${dataset}/${shardNum}`)
  const docs = (await obj.json()) as unknown[]
  if (docCache.size >= DOC_CACHE_MAX) {
    const firstKey = docCache.keys().next().value
    if (firstKey) docCache.delete(firstKey)
  }
  docCache.set(key, docs)
  return docs
}

// =============================================================================
// Search with Scoring
// =============================================================================

function tokenize(query: string): string[] {
  return query.toLowerCase().split(/[^a-z0-9]+/).filter(t => t.length >= 2)
}

function getLetters(terms: string[]): string[] {
  const letters = new Set<string>()
  for (const t of terms) {
    const first = t[0] || ''
    letters.add(/[0-9]/.test(first) ? '0' : first)
  }
  return [...letters]
}

async function searchWithScoring(
  dataset: string,
  query: string,
  env: Env
): Promise<{ indices: number[]; scores: Map<number, number>; matched: Set<string>; expanded: string[] }> {
  const terms = tokenize(query)
  if (!terms.length) return { indices: [], scores: new Map(), matched: new Set(), expanded: [] }

  const allVariants: string[] = []
  for (const term of terms) allVariants.push(...expandTerm(term))
  const unique = [...new Set(allVariants)]
  const letters = getLetters(unique)

  // Load shards
  const shards = await Promise.all(letters.slice(0, MAX_TERM_SHARDS).map(l => loadTermShard(dataset, l, env)))
  const combined: Record<string, number[]> = {}
  const hasV2 = shards.some(s => s !== null)

  if (hasV2) {
    for (const shard of shards) if (shard) Object.assign(combined, shard)
  } else {
    Object.assign(combined, await loadFullIndex(dataset, env))
  }

  const scores = new Map<number, number>()
  const matched = new Set<string>()

  for (const term of terms) {
    const variants = expandTerm(term)
    for (const v of variants) {
      if (combined[v]) {
        const score = v === term ? SCORE_EXACT_MATCH : SCORE_STEMMED_MATCH
        for (const idx of combined[v]) scores.set(idx, (scores.get(idx) || 0) + score)
        matched.add(v)
      }
    }
    // Prefix matching
    if (term.length >= MIN_PREFIX_TERM_LENGTH) {
      for (const [k, indices] of Object.entries(combined)) {
        if (k.startsWith(term) && k !== term) {
          for (const idx of indices) scores.set(idx, (scores.get(idx) || 0) + SCORE_PREFIX_MATCH)
          matched.add(k)
        }
      }
    }
  }

  const sorted = [...scores.entries()].sort((a, b) => b[1] - a[1]).map(([idx]) => idx)
  return { indices: sorted, scores, matched, expanded: unique }
}

// =============================================================================
// Filtering
// =============================================================================

interface FilterConfig {
  hash?: { field: string; values: string[] }[]
  range?: { field: string; gte?: number; lte?: number }[]
}

function parseFilters(params: URLSearchParams, dataset: string): FilterConfig {
  const config: FilterConfig = { hash: [], range: [] }

  // Dataset-specific hash filters
  if (dataset === 'onet') {
    const code = params.get('code')
    if (code) config.hash!.push({ field: 'code', values: code.split(',') })
  }

  if (dataset === 'imdb') {
    const type = params.get('type')
    if (type) config.hash!.push({ field: 'titleType', values: type.split(',') })
    const year = params.get('year')
    if (year) config.hash!.push({ field: 'startYear', values: year.split(',') })

    // Range filters
    const yearGte = params.get('year_gte')
    const yearLte = params.get('year_lte')
    if (yearGte || yearLte) {
      config.range!.push({
        field: 'startYear',
        gte: yearGte ? parseInt(yearGte) : undefined,
        lte: yearLte ? parseInt(yearLte) : undefined
      })
    }
    const runtimeGte = params.get('runtime_gte')
    const runtimeLte = params.get('runtime_lte')
    if (runtimeGte || runtimeLte) {
      config.range!.push({
        field: 'runtimeMinutes',
        gte: runtimeGte ? parseInt(runtimeGte) : undefined,
        lte: runtimeLte ? parseInt(runtimeLte) : undefined
      })
    }
  }

  if (dataset === 'unspsc') {
    for (const [param, field] of [['commodity', 'commodityCode'], ['class', 'classCode'], ['family', 'familyCode'], ['segment', 'segmentCode']]) {
      const val = params.get(param)
      if (val) config.hash!.push({ field, values: val.split(',') })
    }
  }

  return config
}

async function applyHashFilters(
  indices: number[],
  filters: { field: string; values: string[] }[],
  dataset: string,
  env: Env
): Promise<number[]> {
  let result = indices
  for (const filter of filters) {
    const hashIndex = await loadHashIndex(dataset, filter.field, env)
    const allowed = new Set<number>()
    for (const val of filter.values) {
      const matches = hashIndex[val]
      if (matches) for (const idx of matches) allowed.add(idx)
    }
    result = result.filter(idx => allowed.has(idx))
  }
  return result
}

function applyRangeFilters(
  docs: Record<string, unknown>[],
  filters: { field: string; gte?: number; lte?: number }[]
): Record<string, unknown>[] {
  return docs.filter(doc => {
    for (const f of filters) {
      const val = doc[f.field]
      if (val === null || val === undefined) return false
      const num = typeof val === 'number' ? val : parseInt(String(val))
      if (isNaN(num)) return false
      if (f.gte !== undefined && num < f.gte) return false
      if (f.lte !== undefined && num > f.lte) return false
    }
    return true
  })
}

// =============================================================================
// Sorting
// =============================================================================

type SortDir = 'asc' | 'desc'

function parseSort(sortParam: string | null): { field: string; dir: SortDir } | null {
  if (!sortParam) return null
  const [field, dir = 'asc'] = sortParam.split(':')
  return { field: field!, dir: dir as SortDir }
}

function sortDocs(docs: Record<string, unknown>[], sort: { field: string; dir: SortDir }): Record<string, unknown>[] {
  return [...docs].sort((a, b) => {
    const aVal = a[sort.field]
    const bVal = b[sort.field]

    // Handle nulls
    if (aVal === null || aVal === undefined) return sort.dir === 'asc' ? 1 : -1
    if (bVal === null || bVal === undefined) return sort.dir === 'asc' ? -1 : 1

    // Numeric comparison
    if (typeof aVal === 'number' && typeof bVal === 'number') {
      return sort.dir === 'asc' ? aVal - bVal : bVal - aVal
    }

    // String comparison
    const aStr = String(aVal).toLowerCase()
    const bStr = String(bVal).toLowerCase()
    const cmp = aStr.localeCompare(bStr)
    return sort.dir === 'asc' ? cmp : -cmp
  })
}

// =============================================================================
// Document Fetching
// =============================================================================

async function fetchDocs(
  dataset: string,
  indices: number[],
  meta: Meta,
  env: Env,
  limit: number,
  offset: number,
  sort: { field: string; dir: SortDir } | null,
  rangeFilters: { field: string; gte?: number; lte?: number }[]
): Promise<{ docs: Record<string, unknown>[]; total: number }> {
  // If we have range filters or sorting, we need to fetch more docs
  const needsPostProcess = rangeFilters.length > 0 || sort !== null
  const fetchLimit = needsPostProcess ? Math.min(indices.length, MAX_FETCH_COUNT) : limit
  const fetchOffset = needsPostProcess ? 0 : offset

  const toFetch = indices.slice(fetchOffset, fetchOffset + fetchLimit)
  if (!toFetch.length) return { docs: [], total: 0 }

  // Group by shard
  const groups = new Map<number, { idx: number; pos: number }[]>()
  for (let pos = 0; pos < toFetch.length; pos++) {
    const idx = toFetch[pos]!
    const shard = Math.floor(idx / meta.shardSize)
    if (!groups.has(shard)) groups.set(shard, [])
    groups.get(shard)!.push({ idx, pos })
  }

  const shardNums = [...groups.keys()].slice(0, MAX_DOC_SHARDS)
  const shards = await Promise.all(shardNums.map(n => loadDocShard(dataset, n, env)))
  const shardMap = new Map(shardNums.map((n, i) => [n, shards[i]!]))

  // Extract docs
  const rawDocs: Record<string, unknown>[] = new Array(toFetch.length)
  for (const [shardNum, items] of groups) {
    const shard = shardMap.get(shardNum)
    if (!shard) continue
    const base = shardNum * meta.shardSize
    for (const { idx, pos } of items) {
      const local = idx - base
      if (local >= 0 && local < shard.length) {
        rawDocs[pos] = shard[local] as Record<string, unknown>
      }
    }
  }

  let docs = rawDocs.filter(d => d !== undefined)

  // Apply range filters
  if (rangeFilters.length > 0) {
    docs = applyRangeFilters(docs, rangeFilters)
  }

  const total = docs.length

  // Apply sorting
  if (sort) {
    docs = sortDocs(docs, sort)
  }

  // Apply pagination after post-processing
  if (needsPostProcess) {
    docs = docs.slice(offset, offset + limit)
  }

  return { docs, total: needsPostProcess ? total : indices.length }
}

// =============================================================================
// Highlighting
// =============================================================================

function highlight(text: string, matched: Set<string>): string {
  if (!text || !matched.size) return text
  let result = text
  for (const term of matched) {
    result = result.replace(new RegExp(`\\b(${term}\\w*)\\b`, 'gi'), '**$1**')
  }
  return result
}

function highlightDoc(doc: Record<string, unknown>, fields: string[], matched: Set<string>): Record<string, unknown> {
  const result = { ...doc }
  for (const f of fields) {
    if (typeof result[f] === 'string') {
      result[f] = highlight(result[f] as string, matched)
    }
  }
  return result
}

// =============================================================================
// Route Handler
// =============================================================================

const TEXT_FIELDS: Record<string, string[]> = {
  onet: ['title', 'description'],
  unspsc: ['commodityTitle', 'classTitle', 'familyTitle', 'segmentTitle'],
  imdb: ['primaryTitle', 'originalTitle'],
}

async function handleSearch(dataset: string, params: URLSearchParams, env: Env): Promise<Response> {
  const start = performance.now()

  const q = params.get('q')
  const limit = Math.min(parseInt(params.get('limit') || String(DEFAULT_SEARCH_LIMIT)), MAX_SEARCH_LIMIT)
  const offset = Math.max(parseInt(params.get('offset') || '0'), 0)
  const timing = params.get('timing') === 'true'
  const doHighlight = params.get('highlight') !== 'false'
  const debug = params.get('debug') === 'true'
  const sort = parseSort(params.get('sort'))

  const meta = await loadMeta(dataset, env)
  const filters = parseFilters(params, dataset)

  let indices: number[]
  let matched = new Set<string>()
  let expanded: string[] = []
  const searchStart = performance.now()

  if (q?.trim()) {
    const result = await searchWithScoring(dataset, q, env)
    indices = result.indices
    matched = result.matched
    expanded = result.expanded
  } else {
    indices = Array.from({ length: Math.min(meta.totalDocs, MAX_BROWSE_RESULTS) }, (_, i) => i)
  }

  // Apply hash filters
  if (filters.hash?.length) {
    indices = await applyHashFilters(indices, filters.hash, dataset, env)
  }

  const searchMs = performance.now() - searchStart
  const fetchStart = performance.now()

  // Fetch docs with range filters and sorting
  const { docs, total } = await fetchDocs(
    dataset, indices, meta, env, limit, offset, sort, filters.range || []
  )

  // Apply highlighting
  const finalDocs = doHighlight && matched.size
    ? docs.map(d => highlightDoc(d, TEXT_FIELDS[dataset] || [], matched))
    : docs

  const fetchMs = performance.now() - fetchStart

  const response: SearchResponse = {
    data: finalDocs,
    total,
    limit,
    offset,
  }

  if (sort) response.sort = `${sort.field}:${sort.dir}`

  if (filters.hash?.length || filters.range?.length) {
    response.filters = {
      hash: filters.hash?.map(f => ({ [f.field]: f.values })),
      range: filters.range,
    }
  }

  if (timing) {
    response.timing = {
      searchMs: Math.round(searchMs * 100) / 100,
      fetchMs: Math.round(fetchMs * 100) / 100,
      totalMs: Math.round((performance.now() - start) * 100) / 100,
    }
  }

  if (debug && q) {
    response.query = { original: q, expanded, matched: [...matched] }
  }

  return Response.json(response, {
    headers: {
      'Cache-Control': `public, max-age=${CACHE_MAX_AGE_SECONDS}`,
      'CDN-Cache-Control': `max-age=${CDN_CACHE_MAX_AGE_SECONDS}`,
    },
  })
}

// =============================================================================
// Main Handler
// =============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    let path = url.pathname

    // Normalize path
    for (const prefix of ['/search-v4', '/search-v3', '/search-v2', '/search']) {
      if (path.startsWith(prefix)) {
        path = path.slice(prefix.length) || '/'
        break
      }
    }

    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET' },
      })
    }

    try {
      if (path === '/health') {
        return Response.json({
          ok: true,
          version: 4,
          features: ['fts', 'stemming', 'prefix', 'scoring', 'highlight', 'filters', 'range', 'sort'],
          caches: { terms: termShardCache.size, index: indexCache.size, hash: hashCache.size, docs: docCache.size },
        })
      }

      if (path === '/onet') return handleSearch('onet', url.searchParams, env)
      if (path === '/unspsc') return handleSearch('unspsc', url.searchParams, env)
      if (path === '/imdb') return handleSearch('imdb', url.searchParams, env)

      if (path === '/' || path === '') {
        return Response.json({
          name: 'ParqueDB Search (Full v4)',
          version: 4,
          features: ['fts', 'stemming', 'prefix', 'scoring', 'highlight', 'filters', 'range', 'sort'],
          endpoints: {
            '/onet': {
              params: ['q', 'code', 'limit', 'offset', 'sort', 'timing', 'highlight', 'debug'],
              example: '/onet?q=manager&sort=title:asc',
            },
            '/imdb': {
              params: ['q', 'type', 'year', 'year_gte', 'year_lte', 'runtime_gte', 'runtime_lte', 'sort', 'limit', 'offset'],
              examples: [
                '/imdb?q=love&type=movie&year_gte=2000&sort=startYear:desc',
                '/imdb?type=movie,tvSeries&runtime_gte=90&runtime_lte=120',
              ],
            },
            '/unspsc': {
              params: ['q', 'commodity', 'class', 'family', 'segment', 'limit', 'offset'],
            },
          },
        })
      }

      return new Response('Not Found', { status: 404 })
    } catch (error) {
      return Response.json({ error: String(error) }, { status: 500 })
    }
  },
}
