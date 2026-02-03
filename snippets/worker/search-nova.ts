/**
 * Nova Snippet Search Worker (v10)
 *
 * Advanced search with:
 * - TRUE predicate pushdown (filter at index level before scoring)
 * - Vector similarity search (cosine, dot product)
 * - Hybrid search (FTS + vector with RRF fusion)
 * - Multi-index intersection
 */

interface Env { DATA: R2Bucket }
interface Meta {
  totalDocs: number
  shardSize: number
  shardCount: number
  hasVectors?: boolean
  vectorDim?: number
}

interface SearchResponse {
  data: unknown[]
  total: number
  limit: number
  offset: number
  mode?: 'fts' | 'vector' | 'hybrid'
  facets?: Record<string, { value: string; count: number }[]>
  stats?: Record<string, { min: number; max: number; avg: number }>
  timing?: {
    indexIntersectMs?: number
    ftsMs?: number
    vectorMs?: number
    fusionMs?: number
    fetchMs?: number
    totalMs?: number
  }
}

// Caches
const termCache = new Map<string, Record<string, number[]>>()
const hashCache = new Map<string, Record<string, number[]>>()
const metaCache = new Map<string, Meta>()
const docCache = new Map<string, unknown[]>()
const vectorCache = new Map<string, Float32Array[]>()

// Constants
const STOPS = new Set(['the','a','an','and','or','but','in','on','at','to','for','of','with','by','is','are','was','were'])

// Text Processing
function stem(w: string): string {
  if (w.length < 4) return w
  let s = w
  if (s.endsWith('ing') && s.length > 5) s = s.slice(0,-3)
  else if (s.endsWith('ed') && s.length > 4) s = s.slice(0,-2)
  else if (s.endsWith('s') && !s.endsWith('ss') && s.length > 3) s = s.slice(0,-1)
  return s
}

// Data Loading
async function loadMeta(ds: string, env: Env): Promise<Meta> {
  if (metaCache.has(ds)) return metaCache.get(ds)!
  const o = await env.DATA.get(`indexes/${ds}/meta.json`)
  if (!o) throw new Error(`Meta: ${ds}`)
  const m = (await o.json()) as Meta
  metaCache.set(ds, m)
  return m
}

async function loadTermShard(ds: string, l: string, env: Env): Promise<Record<string, number[]> | null> {
  const k = `${ds}-${l}`
  if (termCache.has(k)) return termCache.get(k)!
  const o = await env.DATA.get(`indexes/${ds}/terms/${l}.json`)
  if (!o) return null
  const idx = (await o.json()) as Record<string, number[]>
  termCache.set(k, idx)
  return idx
}

async function loadHash(ds: string, f: string, env: Env): Promise<Record<string, number[]>> {
  const k = `${ds}-hash-${f}`
  if (hashCache.has(k)) return hashCache.get(k)!
  const o = await env.DATA.get(`indexes/${ds}/hash-${f}.json`)
  if (!o) throw new Error(`Hash: ${ds}/${f}`)
  const idx = (await o.json()) as Record<string, number[]>
  hashCache.set(k, idx)
  return idx
}

async function loadDocs(ds: string, n: number, env: Env): Promise<unknown[]> {
  const k = `${ds}-docs-${n}`
  if (docCache.has(k)) return docCache.get(k)!
  const o = await env.DATA.get(`indexes/${ds}/docs-${n}.json`)
  if (!o) throw new Error(`Shard: ${ds}/${n}`)
  const d = (await o.json()) as unknown[]
  if (docCache.size >= 10) docCache.delete(docCache.keys().next().value as string)
  docCache.set(k, d)
  return d
}

async function loadVectors(ds: string, n: number, env: Env): Promise<Float32Array[] | null> {
  const k = `${ds}-vec-${n}`
  if (vectorCache.has(k)) return vectorCache.get(k)!
  const o = await env.DATA.get(`indexes/${ds}/vectors-${n}.json`)
  if (!o) return null
  const raw = (await o.json()) as number[][]
  const vectors = raw.map(v => new Float32Array(v))
  vectorCache.set(k, vectors)
  return vectors
}

// Set Operations for Predicate Pushdown
function intersectSets(sets: Set<number>[]): Set<number> {
  if (sets.length === 0) return new Set()
  if (sets.length === 1) return sets[0]!

  // Start with smallest set for efficiency
  const sorted = [...sets].sort((a, b) => a.size - b.size)
  const result = new Set<number>()
  const smallest = sorted[0]!

  for (const item of smallest) {
    if (sorted.every(s => s.has(item))) {
      result.add(item)
    }
  }
  return result
}

function unionSets(sets: Set<number>[]): Set<number> {
  const result = new Set<number>()
  for (const s of sets) {
    for (const item of s) result.add(item)
  }
  return result
}

// TRUE Predicate Pushdown
// Returns candidate set BEFORE any scoring
interface PredicateResult {
  candidates: Set<number>
  ftsTermSets: Map<string, Set<number>>  // For scoring later
}

async function applyPredicates(
  ds: string,
  terms: string[],
  hashFilters: { field: string; values: string[] }[],
  env: Env
): Promise<PredicateResult> {
  const sets: Set<number>[] = []
  const ftsTermSets = new Map<string, Set<number>>()

  // 1. Load hash filter sets
  for (const filter of hashFilters) {
    const hash = await loadHash(ds, filter.field, env)
    const filterSet = new Set<number>()
    for (const value of filter.values) {
      const indices = hash[value]
      if (indices) {
        for (const i of indices) filterSet.add(i)
      }
    }
    if (filterSet.size > 0) sets.push(filterSet)
  }

  // 2. Load FTS term sets
  if (terms.length > 0) {
    const letters = new Set(terms.map(t => /\d/.test(t[0]!) ? '0' : t[0]!))
    const shards = await Promise.all([...letters].slice(0, 3).map(l => loadTermShard(ds, l, env)))
    const index: Record<string, number[]> = {}
    for (const s of shards) if (s) Object.assign(index, s)

    // Collect term sets for intersection AND scoring
    for (const term of terms) {
      const termSet = new Set<number>()
      const stemmed = stem(term)

      // Exact match
      if (index[term]) {
        for (const i of index[term]!) termSet.add(i)
      }
      // Stemmed match
      if (index[stemmed] && stemmed !== term) {
        for (const i of index[stemmed]!) termSet.add(i)
      }
      // Prefix match (limited)
      let prefixCount = 0
      for (const [k, indices] of Object.entries(index)) {
        if (prefixCount >= 5) break
        if (k.startsWith(term) && k !== term) {
          prefixCount++
          for (const i of indices) termSet.add(i)
        }
      }

      if (termSet.size > 0) {
        sets.push(termSet)
        ftsTermSets.set(term, termSet)
      }
    }
  }

  // 3. Intersect all sets (TRUE predicate pushdown!)
  const candidates = sets.length > 0 ? intersectSets(sets) : new Set<number>()

  return { candidates, ftsTermSets }
}

// FTS Scoring (only on pre-filtered candidates)
function scoreFTS(
  candidates: Set<number>,
  ftsTermSets: Map<string, Set<number>>
): Map<number, number> {
  const scores = new Map<number, number>()

  for (const idx of candidates) {
    let score = 0
    for (const [term, termSet] of ftsTermSets) {
      if (termSet.has(idx)) {
        // Exact match scores higher
        score += 10
      }
    }
    if (score > 0) scores.set(idx, score)
  }

  return scores
}

// Vector Similarity
function cosineSimilarity(a: Float32Array, b: Float32Array): number {
  let dot = 0, normA = 0, normB = 0
  for (let i = 0; i < a.length; i++) {
    dot += a[i]! * b[i]!
    normA += a[i]! * a[i]!
    normB += b[i]! * b[i]!
  }
  return dot / (Math.sqrt(normA) * Math.sqrt(normB) + 1e-8)
}

function dotProduct(a: Float32Array, b: Float32Array): number {
  let dot = 0
  for (let i = 0; i < a.length; i++) {
    dot += a[i]! * b[i]!
  }
  return dot
}

// Vector Search
async function vectorSearch(
  ds: string,
  queryVector: number[],
  candidates: Set<number> | null,
  topK: number,
  metric: 'cosine' | 'dot',
  meta: Meta,
  env: Env
): Promise<Map<number, number>> {
  const qv = new Float32Array(queryVector)
  const scores = new Map<number, number>()
  const similarityFn = metric === 'cosine' ? cosineSimilarity : dotProduct

  // Determine which shards to load
  const shardIndices = candidates
    ? [...new Set([...candidates].map(i => Math.floor(i / meta.shardSize)))]
    : Array.from({ length: Math.min(meta.shardCount, 3) }, (_, i) => i)

  // Load vectors from relevant shards
  for (const shardIdx of shardIndices.slice(0, 3)) {
    const vectors = await loadVectors(ds, shardIdx, env)
    if (!vectors) continue

    const baseIdx = shardIdx * meta.shardSize
    for (let i = 0; i < vectors.length; i++) {
      const docIdx = baseIdx + i

      // Skip if not in candidates (when pre-filtered)
      if (candidates && !candidates.has(docIdx)) continue

      const similarity = similarityFn(qv, vectors[i]!)
      scores.set(docIdx, similarity)
    }
  }

  // Keep top K
  const sorted = [...scores.entries()].sort((a, b) => b[1] - a[1])
  const topScores = new Map<number, number>()
  for (const [idx, score] of sorted.slice(0, topK)) {
    topScores.set(idx, score)
  }

  return topScores
}

// Reciprocal Rank Fusion for Hybrid Search
function reciprocalRankFusion(
  ftsScores: Map<number, number>,
  vectorScores: Map<number, number>,
  k: number = 60,
  ftsWeight: number = 0.5
): Map<number, number> {
  // Convert to rankings
  const ftsRanked = [...ftsScores.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([idx], rank) => ({ idx, rank: rank + 1 }))

  const vectorRanked = [...vectorScores.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([idx], rank) => ({ idx, rank: rank + 1 }))

  // Build rank maps
  const ftsRankMap = new Map(ftsRanked.map(r => [r.idx, r.rank]))
  const vectorRankMap = new Map(vectorRanked.map(r => [r.idx, r.rank]))

  // All unique indices
  const allIndices = new Set([...ftsScores.keys(), ...vectorScores.keys()])

  // Calculate RRF scores
  const rrfScores = new Map<number, number>()
  const vectorWeight = 1 - ftsWeight

  for (const idx of allIndices) {
    let score = 0

    const ftsRank = ftsRankMap.get(idx)
    if (ftsRank !== undefined) {
      score += ftsWeight * (1 / (k + ftsRank))
    }

    const vecRank = vectorRankMap.get(idx)
    if (vecRank !== undefined) {
      score += vectorWeight * (1 / (k + vecRank))
    }

    rrfScores.set(idx, score)
  }

  return rrfScores
}

// Parse filters
interface Filters {
  hash: { field: string; values: string[] }[]
  range: { field: string; gte?: number; lte?: number }[]
}

function parseFilters(p: URLSearchParams, ds: string): Filters {
  const f: Filters = { hash: [], range: [] }

  if (ds === 'imdb') {
    const type = p.get('type')
    if (type) f.hash.push({ field: 'titleType', values: type.split(',') })

    const yearGte = p.get('year_gte'), yearLte = p.get('year_lte')
    if (yearGte || yearLte) {
      f.range.push({
        field: 'startYear',
        gte: yearGte ? +yearGte : undefined,
        lte: yearLte ? +yearLte : undefined
      })
    }
  }

  if (ds === 'onet') {
    const code = p.get('code')
    if (code) f.hash.push({ field: 'code', values: code.split(',') })
  }

  return f
}

// Sorting
function sortDocs(
  docs: Record<string, unknown>[],
  specs: { field: string; dir: 'asc' | 'desc' }[]
): Record<string, unknown>[] {
  if (!specs.length) return docs
  return [...docs].sort((a, b) => {
    for (const { field, dir } of specs) {
      const av = a[field], bv = b[field]
      if (av == bv) continue
      if (av == null) return 1
      if (bv == null) return -1
      const cmp = typeof av === 'number' && typeof bv === 'number'
        ? av - bv
        : String(av).localeCompare(String(bv))
      return dir === 'asc' ? cmp : -cmp
    }
    return 0
  })
}

// Facets
function computeFacets(
  docs: Record<string, unknown>[],
  fields: string[]
): Record<string, { value: string; count: number }[]> {
  const r: Record<string, { value: string; count: number }[]> = {}
  for (const f of fields) {
    const c = new Map<string, number>()
    for (const d of docs.slice(0, 200)) {
      const v = d[f]
      if (Array.isArray(v)) {
        for (const item of v) c.set(String(item), (c.get(String(item)) || 0) + 1)
      } else if (v != null) {
        c.set(String(v), (c.get(String(v)) || 0) + 1)
      }
    }
    r[f] = [...c.entries()].sort((a, b) => b[1] - a[1]).slice(0, 10)
      .map(([value, count]) => ({ value, count }))
  }
  return r
}

// Fetch documents
async function fetchDocs(
  ds: string,
  indices: number[],
  meta: Meta,
  env: Env,
  limit: number,
  offset: number
): Promise<Record<string, unknown>[]> {
  const toFetch = indices.slice(offset, offset + limit)
  if (!toFetch.length) return []

  const groups = new Map<number, { idx: number; pos: number }[]>()
  for (let pos = 0; pos < toFetch.length; pos++) {
    const idx = toFetch[pos]!
    const shard = Math.floor(idx / meta.shardSize)
    if (!groups.has(shard)) groups.set(shard, [])
    groups.get(shard)!.push({ idx, pos })
  }

  const shardNums = [...groups.keys()].slice(0, 3)
  const shards = await Promise.all(shardNums.map(n => loadDocs(ds, n, env)))
  const shardMap = new Map(shardNums.map((n, i) => [n, shards[i]!]))

  const docs: Record<string, unknown>[] = new Array(toFetch.length)
  for (const [shard, items] of groups) {
    const data = shardMap.get(shard)
    if (!data) continue
    const base = shard * meta.shardSize
    for (const { idx, pos } of items) {
      const local = idx - base
      if (local >= 0 && local < data.length) {
        docs[pos] = data[local] as Record<string, unknown>
      }
    }
  }

  return docs.filter(d => d)
}

// Highlighting
const TEXT_FIELDS: Record<string, string[]> = {
  onet: ['title', 'description'],
  unspsc: ['commodityTitle', 'classTitle'],
  imdb: ['primaryTitle', 'originalTitle']
}

function highlight(text: string, terms: string[]): string {
  if (!text || !terms.length) return text
  let r = text
  for (const t of terms) {
    r = r.replace(new RegExp(`\\b(${t}\\w*)`, 'gi'), '**$1**')
  }
  return r
}

// Main Search Handler
async function handleSearch(ds: string, p: URLSearchParams, env: Env): Promise<Response> {
  const timing: Record<string, number> = {}
  const start = performance.now()

  // Parse params
  const q = p.get('q') || ''
  const vectorParam = p.get('vector')  // JSON array: [0.1, 0.2, ...]
  const mode = p.get('mode') as 'fts' | 'vector' | 'hybrid' | null
  const ftsWeight = parseFloat(p.get('fts_weight') || '0.5')
  const metric = (p.get('metric') || 'cosine') as 'cosine' | 'dot'
  const limit = Math.min(+(p.get('limit') || 20), 50)
  const offset = Math.max(+(p.get('offset') || 0), 0)
  const sortParam = p.get('sort')
  const sortSpec = sortParam
    ? sortParam.split(',').map(s => {
        const [f, d = 'asc'] = s.split(':')
        return { field: f!, dir: d as 'asc' | 'desc' }
      })
    : []
  const facetFields = (p.get('facets') || '').split(',').filter(Boolean).slice(0, 3)
  const showTiming = p.get('timing') === 'true'

  const meta = await loadMeta(ds, env)
  const filters = parseFilters(p, ds)

  // Parse query terms
  const terms = q.toLowerCase()
    .split(/\s+/)
    .filter(t => t.length >= 2 && !STOPS.has(t))
    .slice(0, 5)

  // Parse vector
  let queryVector: number[] | null = null
  if (vectorParam) {
    try {
      queryVector = JSON.parse(vectorParam)
    } catch { /* ignore */ }
  }

  // Determine search mode
  let searchMode: 'fts' | 'vector' | 'hybrid' = 'fts'
  if (mode) {
    searchMode = mode
  } else if (queryVector && terms.length > 0) {
    searchMode = 'hybrid'
  } else if (queryVector) {
    searchMode = 'vector'
  }

  // Step 1: Apply predicates (TRUE pushdown!)
  const t0 = performance.now()
  const { candidates, ftsTermSets } = await applyPredicates(ds, terms, filters.hash, env)
  timing.indexIntersectMs = performance.now() - t0

  let finalScores = new Map<number, number>()

  // Step 2: Score based on mode
  if (searchMode === 'fts' || searchMode === 'hybrid') {
    const t1 = performance.now()
    const ftsScores = scoreFTS(candidates, ftsTermSets)
    timing.ftsMs = performance.now() - t1

    if (searchMode === 'fts') {
      finalScores = ftsScores
    } else if (searchMode === 'hybrid' && queryVector) {
      // Vector search on FTS candidates
      const t2 = performance.now()
      const vectorScores = await vectorSearch(
        ds, queryVector, candidates, limit * 2, metric, meta, env
      )
      timing.vectorMs = performance.now() - t2

      // RRF fusion
      const t3 = performance.now()
      finalScores = reciprocalRankFusion(ftsScores, vectorScores, 60, ftsWeight)
      timing.fusionMs = performance.now() - t3
    }
  } else if (searchMode === 'vector' && queryVector) {
    const t2 = performance.now()
    // Vector search, optionally filtered by predicates
    finalScores = await vectorSearch(
      ds, queryVector,
      candidates.size > 0 ? candidates : null,
      limit * 2, metric, meta, env
    )
    timing.vectorMs = performance.now() - t2
  }

  // Step 3: Rank and fetch
  const t4 = performance.now()
  let rankedIndices = [...finalScores.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([idx]) => idx)

  // If no scores (browse mode), use candidates or all docs
  if (rankedIndices.length === 0) {
    if (candidates.size > 0) {
      rankedIndices = [...candidates].slice(0, 500)
    } else {
      rankedIndices = Array.from({ length: Math.min(meta.totalDocs, 500) }, (_, i) => i)
    }
  }

  // Apply range filters (post-fetch for now)
  let docs = await fetchDocs(ds, rankedIndices, meta, env, Math.min(limit + 100, 300), 0)

  // Range filter
  for (const r of filters.range) {
    docs = docs.filter(d => {
      const v = d[r.field]
      if (v == null) return false
      const n = typeof v === 'number' ? v : +v
      if (isNaN(n)) return false
      return (r.gte === undefined || n >= r.gte) && (r.lte === undefined || n <= r.lte)
    })
  }

  const total = docs.length

  // Sort
  if (sortSpec.length) docs = sortDocs(docs, sortSpec)

  // Paginate
  docs = docs.slice(offset, offset + limit)

  // Highlight
  if (terms.length > 0) {
    docs = docs.map(d => {
      const r = { ...d }
      for (const f of TEXT_FIELDS[ds] || []) {
        if (typeof r[f] === 'string') {
          r[f] = highlight(r[f] as string, terms)
        }
      }
      return r
    })
  }

  timing.fetchMs = performance.now() - t4

  // Facets
  const facets = facetFields.length > 0 ? computeFacets(docs, facetFields) : undefined

  timing.totalMs = performance.now() - start

  const response: SearchResponse = {
    data: docs,
    total,
    limit,
    offset,
    mode: searchMode
  }

  if (facets) response.facets = facets
  if (showTiming) response.timing = {
    indexIntersectMs: Math.round(timing.indexIntersectMs! * 100) / 100,
    ftsMs: timing.ftsMs ? Math.round(timing.ftsMs * 100) / 100 : undefined,
    vectorMs: timing.vectorMs ? Math.round(timing.vectorMs * 100) / 100 : undefined,
    fusionMs: timing.fusionMs ? Math.round(timing.fusionMs * 100) / 100 : undefined,
    fetchMs: Math.round(timing.fetchMs! * 100) / 100,
    totalMs: Math.round(timing.totalMs! * 100) / 100
  }

  return Response.json(response, {
    headers: { 'Cache-Control': 'public, max-age=3600' }
  })
}

// Suggest
async function handleSuggest(ds: string, p: URLSearchParams, env: Env): Promise<Response> {
  const q = p.get('q') || ''
  if (q.length < 2) return Response.json({ suggestions: [], query: q })

  const letter = q[0]!
  const shard = await loadTermShard(ds, /\d/.test(letter) ? '0' : letter, env)
  if (!shard) return Response.json({ suggestions: [], query: q })

  const prefix = q.toLowerCase()
  const matches: { term: string; count: number }[] = []

  for (const [term, indices] of Object.entries(shard)) {
    if (term.startsWith(prefix) && term !== prefix) {
      matches.push({ term, count: indices.length })
    }
    if (matches.length >= 100) break
  }

  const suggestions = matches
    .sort((a, b) => b.count - a.count)
    .slice(0, 10)
    .map(m => m.term)

  return Response.json({ suggestions, query: q })
}

// Main
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    let path = url.pathname

    for (const pre of ['/search-v10', '/search-v9', '/search']) {
      if (path.startsWith(pre)) {
        path = path.slice(pre.length) || '/'
        break
      }
    }

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: { 'Access-Control-Allow-Origin': '*' } })
    }

    try {
      if (path === '/health') {
        return Response.json({
          ok: true,
          version: 10,
          features: [
            'fts', 'stemming', 'prefix',
            'predicate_pushdown', 'index_intersection',
            'vector_search', 'cosine', 'dot_product',
            'hybrid_search', 'rrf_fusion',
            'filters', 'range', 'sort',
            'facets', 'highlight', 'suggest'
          ]
        })
      }

      // Suggest
      if (path === '/suggest/onet') return handleSuggest('onet', url.searchParams, env)
      if (path === '/suggest/imdb') return handleSuggest('imdb', url.searchParams, env)
      if (path === '/suggest/unspsc') return handleSuggest('unspsc', url.searchParams, env)

      // Search
      if (path === '/onet') return handleSearch('onet', url.searchParams, env)
      if (path === '/unspsc') return handleSearch('unspsc', url.searchParams, env)
      if (path === '/imdb') return handleSearch('imdb', url.searchParams, env)

      if (path === '/' || path === '') {
        return Response.json({
          name: 'ParqueDB Search (Nova v10)',
          version: 10,
          features: [
            'fts', 'predicate_pushdown', 'vector_search', 'hybrid_search'
          ],
          searchModes: {
            fts: 'Full-text search (default when q= provided)',
            vector: 'Vector similarity (when vector= provided)',
            hybrid: 'Combined FTS + vector with RRF fusion'
          },
          examples: [
            '/imdb?q=love&type=movie (FTS with predicate pushdown)',
            '/imdb?q=love&type=movie&timing=true (show timing breakdown)',
            '/imdb?vector=[0.1,0.2,...]&mode=vector (vector search)',
            '/imdb?q=love&vector=[...]&mode=hybrid&fts_weight=0.7 (hybrid)',
          ]
        })
      }

      return new Response('Not Found', { status: 404 })
    } catch (e) {
      return Response.json({ error: String(e) }, { status: 500 })
    }
  }
}
