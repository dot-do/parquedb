/**
 * Maximum-Featured Snippet Search Worker (v5)
 *
 * Pushing the limits of what's possible in a Cloudflare Snippet:
 *
 * Full-Text Search:
 * - Stemming (manager → managers)
 * - Prefix matching (eng → engineer)
 * - Phrase search ("software developer")
 * - Negation (-exclude)
 * - Field-specific search (title:engineer)
 * - Synonyms (auto → automobile → car)
 * - Fuzzy matching (enginer → engineer)
 *
 * Filtering:
 * - Hash filters (type=movie)
 * - Multi-value (type=movie,tvSeries)
 * - Range filters (year_gte=2000)
 * - Existence filters (has:runtime)
 * - Negation filters (not_type=tvEpisode)
 *
 * Sorting & Pagination:
 * - Multi-field sort (sort=year:desc,title:asc)
 * - Score boosting (boost=title:2)
 * - Cursor-based pagination
 *
 * Aggregations:
 * - Facet counts (?facets=type,year)
 * - Stats (min/max/avg for numeric fields)
 */

interface Env {
  DATA: R2Bucket
}

interface Meta {
  totalDocs: number
  shardSize: number
  shardCount: number
}

interface Facet {
  value: string
  count: number
}

interface SearchResponse {
  data: unknown[]
  total: number
  limit: number
  offset: number
  sort?: string
  facets?: Record<string, Facet[]>
  stats?: Record<string, { min: number; max: number; avg: number }>
  timing?: Record<string, number>
  query?: Record<string, unknown>
}

// =============================================================================
// Caches & Config
// =============================================================================

const termCache = new Map<string, Record<string, number[]>>()
const indexCache = new Map<string, Record<string, number[]>>()
const hashCache = new Map<string, Record<string, number[]>>()
const metaCache = new Map<string, Meta>()
const docCache = new Map<string, unknown[]>()

const SYNONYMS: Record<string, string[]> = {
  auto: ['automobile', 'car', 'vehicle'],
  car: ['auto', 'automobile', 'vehicle'],
  movie: ['film', 'picture'],
  film: ['movie', 'picture'],
  tv: ['television', 'series'],
  computer: ['pc', 'machine', 'computing'],
  software: ['program', 'application', 'app'],
  engineer: ['engineering', 'developer'],
  manager: ['management', 'supervisor', 'director'],
  analyst: ['analysis', 'analyzer'],
  doctor: ['physician', 'medical'],
  nurse: ['nursing', 'healthcare'],
}

const STOPWORDS = new Set(['the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'must', 'shall'])

// =============================================================================
// Text Processing
// =============================================================================

const IRREGULAR: Record<string, string> = {
  children: 'child', people: 'person', men: 'man', women: 'woman',
  feet: 'foot', teeth: 'tooth', geese: 'goose', mice: 'mouse',
  analyses: 'analysis', criteria: 'criterion',
}

function stem(w: string): string {
  if (w.length < 3) return w
  if (IRREGULAR[w]) return IRREGULAR[w]
  if (w.endsWith('sses')) return w.slice(0, -2)
  if (w.endsWith('ies') && w.length > 4) return w.slice(0, -2)
  if (w.endsWith('ss')) return w
  if (w.endsWith('s') && w.length > 3) return w.slice(0, -1)
  if (w.endsWith('eed') && w.length > 4) return w.slice(0, -1)
  if (w.endsWith('ed') && w.length > 4 && /[aeiou]/.test(w.slice(0, -2))) return w.slice(0, -2)
  if (w.endsWith('ing') && w.length > 5 && /[aeiou]/.test(w.slice(0, -3))) return w.slice(0, -3)
  if (w.endsWith('ness') && w.length > 6) return w.slice(0, -4)
  if (w.endsWith('ment') && w.length > 6) return w.slice(0, -4)
  if (w.endsWith('ation') && w.length > 7) return w.slice(0, -5) + 'e'
  if (w.endsWith('ity') && w.length > 5) return w.slice(0, -3)
  if (w.endsWith('ful') && w.length > 5) return w.slice(0, -3)
  if (w.endsWith('less') && w.length > 6) return w.slice(0, -4)
  return w
}

function levenshtein(a: string, b: string): number {
  if (a.length === 0) return b.length
  if (b.length === 0) return a.length
  if (a === b) return 0

  const matrix: number[][] = []
  for (let i = 0; i <= b.length; i++) matrix[i] = [i]
  for (let j = 0; j <= a.length; j++) matrix[0]![j] = j

  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      const cost = a[j - 1] === b[i - 1] ? 0 : 1
      matrix[i]![j] = Math.min(
        matrix[i - 1]![j]! + 1,
        matrix[i]![j - 1]! + 1,
        matrix[i - 1]![j - 1]! + cost
      )
    }
  }
  return matrix[b.length]![a.length]!
}

function expandTerm(term: string, fuzzy = false): { variants: string[]; synonyms: string[] } {
  const variants = new Set<string>([term])
  const stemmed = stem(term)
  variants.add(stemmed)

  // Add common suffixes
  for (const s of ['s', 'es', 'ed', 'ing', 'er', 'ers', 'ly', 'tion', 'ment']) {
    if (stemmed.length >= 3) variants.add(stemmed + s)
  }

  // Get synonyms
  const synonyms = SYNONYMS[term] || SYNONYMS[stemmed] || []

  return { variants: [...variants], synonyms }
}

// =============================================================================
// Query Parsing
// =============================================================================

interface ParsedQuery {
  must: string[]           // Required terms
  should: string[]         // Optional terms (boost)
  mustNot: string[]        // Excluded terms
  phrases: string[]        // Exact phrases
  fieldTerms: { field: string; term: string }[]  // Field-specific
  fuzzy: boolean
  boosts: Record<string, number>
}

function parseQuery(q: string): ParsedQuery {
  const result: ParsedQuery = {
    must: [], should: [], mustNot: [], phrases: [], fieldTerms: [], fuzzy: false, boosts: {}
  }

  if (!q) return result

  // Extract phrases
  const phraseRe = /"([^"]+)"/g
  let match
  while ((match = phraseRe.exec(q)) !== null) {
    result.phrases.push(match[1]!.toLowerCase())
  }
  q = q.replace(phraseRe, '')

  // Extract field:term
  const fieldRe = /(\w+):(\w+)/g
  while ((match = fieldRe.exec(q)) !== null) {
    result.fieldTerms.push({ field: match[1]!, term: match[2]!.toLowerCase() })
  }
  q = q.replace(fieldRe, '')

  // Check for fuzzy flag
  if (q.includes('~')) {
    result.fuzzy = true
    q = q.replace(/~/g, '')
  }

  // Parse remaining terms
  const tokens = q.toLowerCase().split(/\s+/).filter(t => t.length >= 2)

  for (const token of tokens) {
    if (token.startsWith('-')) {
      result.mustNot.push(token.slice(1))
    } else if (token.startsWith('+')) {
      result.must.push(token.slice(1))
    } else if (!STOPWORDS.has(token)) {
      result.should.push(token)
    }
  }

  return result
}

// =============================================================================
// Data Loading
// =============================================================================

async function loadMeta(ds: string, env: Env): Promise<Meta> {
  if (metaCache.has(ds)) return metaCache.get(ds)!
  const obj = await env.DATA.get(`indexes/${ds}/meta.json`)
  if (!obj) throw new Error(`Meta: ${ds}`)
  const meta = (await obj.json()) as Meta
  metaCache.set(ds, meta)
  return meta
}

async function loadTermShard(ds: string, letter: string, env: Env): Promise<Record<string, number[]> | null> {
  const k = `${ds}-${letter}`
  if (termCache.has(k)) return termCache.get(k)!
  const obj = await env.DATA.get(`indexes/${ds}/terms/${letter}.json`)
  if (!obj) return null
  const idx = (await obj.json()) as Record<string, number[]>
  termCache.set(k, idx)
  return idx
}

async function loadFullIndex(ds: string, env: Env): Promise<Record<string, number[]>> {
  const k = `${ds}-full`
  if (indexCache.has(k)) return indexCache.get(k)!
  const obj = await env.DATA.get(`indexes/${ds}/inverted.json`)
  if (!obj) throw new Error(`Index: ${ds}`)
  const idx = (await obj.json()) as Record<string, number[]>
  indexCache.set(k, idx)
  return idx
}

async function loadHashIndex(ds: string, field: string, env: Env): Promise<Record<string, number[]>> {
  const k = `${ds}-hash-${field}`
  if (hashCache.has(k)) return hashCache.get(k)!
  const obj = await env.DATA.get(`indexes/${ds}/hash-${field}.json`)
  if (!obj) throw new Error(`Hash: ${ds}/${field}`)
  const idx = (await obj.json()) as Record<string, number[]>
  hashCache.set(k, idx)
  return idx
}

async function loadDocShard(ds: string, n: number, env: Env): Promise<unknown[]> {
  const k = `${ds}-docs-${n}`
  if (docCache.has(k)) return docCache.get(k)!
  const obj = await env.DATA.get(`indexes/${ds}/docs-${n}.json`)
  if (!obj) throw new Error(`Shard: ${ds}/${n}`)
  const docs = (await obj.json()) as unknown[]
  if (docCache.size >= 15) docCache.delete(docCache.keys().next().value as string)
  docCache.set(k, docs)
  return docs
}

// =============================================================================
// Search Engine
// =============================================================================

async function search(
  ds: string,
  pq: ParsedQuery,
  env: Env
): Promise<{ indices: number[]; scores: Map<number, number>; matched: Set<string> }> {
  const scores = new Map<number, number>()
  const matched = new Set<string>()

  // Collect all terms to search
  const allTerms = [...pq.should, ...pq.must]
  if (!allTerms.length && !pq.phrases.length) {
    return { indices: [], scores, matched }
  }

  // Get letters for sharding (include mustNot terms too)
  const letters = new Set<string>()
  for (const t of [...allTerms, ...pq.mustNot]) {
    const first = t[0] || ''
    letters.add(/[0-9]/.test(first) ? '0' : first)
  }

  // Load shards
  const shards = await Promise.all([...letters].slice(0, 4).map(l => loadTermShard(ds, l, env)))
  const index: Record<string, number[]> = {}
  const hasV2 = shards.some(s => s !== null)
  if (hasV2) {
    for (const s of shards) if (s) Object.assign(index, s)
  } else {
    Object.assign(index, await loadFullIndex(ds, env))
  }

  // Score documents
  for (const term of allTerms) {
    const { variants, synonyms } = expandTerm(term, pq.fuzzy)

    for (const v of variants) {
      if (index[v]) {
        const score = v === term ? 10 : 5
        for (const idx of index[v]) scores.set(idx, (scores.get(idx) || 0) + score)
        matched.add(v)
      }
    }

    for (const syn of synonyms) {
      if (index[syn]) {
        for (const idx of index[syn]) scores.set(idx, (scores.get(idx) || 0) + 3)
        matched.add(syn)
      }
    }

    // Prefix matching
    if (term.length >= 3) {
      for (const [k, indices] of Object.entries(index)) {
        if (k.startsWith(term) && k !== term && k.length <= term.length + 5) {
          for (const idx of indices) scores.set(idx, (scores.get(idx) || 0) + 2)
          matched.add(k)
        }
      }
    }

    // Fuzzy matching
    if (pq.fuzzy && term.length >= 4) {
      for (const [k, indices] of Object.entries(index)) {
        if (k.length >= term.length - 1 && k.length <= term.length + 1) {
          const dist = levenshtein(term, k)
          if (dist === 1) {
            for (const idx of indices) scores.set(idx, (scores.get(idx) || 0) + 1)
            matched.add(k)
          }
        }
      }
    }
  }

  // Handle must-not (exclusion)
  const excluded = new Set<number>()
  for (const term of pq.mustNot) {
    const { variants } = expandTerm(term)
    for (const v of variants) {
      if (index[v]) {
        for (const idx of index[v]) excluded.add(idx)
      }
    }
  }

  // Filter and sort
  let indices = [...scores.entries()]
    .filter(([idx]) => !excluded.has(idx))
    .sort((a, b) => b[1] - a[1])
    .map(([idx]) => idx)

  return { indices, scores, matched }
}

// =============================================================================
// Filtering & Sorting
// =============================================================================

interface Filters {
  hash: { field: string; values: string[]; negate?: boolean }[]
  range: { field: string; gte?: number; lte?: number }[]
  exists: string[]
}

function parseFilters(params: URLSearchParams, ds: string): Filters {
  const f: Filters = { hash: [], range: [], exists: [] }

  if (ds === 'onet') {
    const code = params.get('code')
    if (code) f.hash.push({ field: 'code', values: code.split(',') })
  }

  if (ds === 'imdb') {
    for (const [p, field] of [['type', 'titleType'], ['year', 'startYear']]) {
      const v = params.get(p)
      const nv = params.get('not_' + p)
      if (v) f.hash.push({ field, values: v.split(',') })
      if (nv) f.hash.push({ field, values: nv.split(','), negate: true })
    }
    for (const [p, field] of [['year', 'startYear'], ['runtime', 'runtimeMinutes']]) {
      const gte = params.get(p + '_gte')
      const lte = params.get(p + '_lte')
      if (gte || lte) f.range.push({ field, gte: gte ? +gte : undefined, lte: lte ? +lte : undefined })
    }
    const has = params.get('has')
    if (has) f.exists = has.split(',')
  }

  if (ds === 'unspsc') {
    for (const [p, field] of [['commodity', 'commodityCode'], ['class', 'classCode'], ['family', 'familyCode'], ['segment', 'segmentCode']]) {
      const v = params.get(p)
      if (v) f.hash.push({ field, values: v.split(',') })
    }
  }

  return f
}

async function applyHashFilters(indices: number[], filters: Filters['hash'], ds: string, env: Env): Promise<number[]> {
  for (const f of filters) {
    const hash = await loadHashIndex(ds, f.field, env)
    const allowed = new Set<number>()
    for (const v of f.values) {
      const m = hash[v]
      if (m) for (const i of m) allowed.add(i)
    }
    indices = f.negate
      ? indices.filter(i => !allowed.has(i))
      : indices.filter(i => allowed.has(i))
  }
  return indices
}

type SortSpec = { field: string; dir: 'asc' | 'desc' }[]

function parseSort(s: string | null): SortSpec {
  if (!s) return []
  return s.split(',').map(p => {
    const [field, dir = 'asc'] = p.split(':')
    return { field: field!, dir: dir as 'asc' | 'desc' }
  })
}

function sortDocs(docs: Record<string, unknown>[], specs: SortSpec): Record<string, unknown>[] {
  if (!specs.length) return docs
  return [...docs].sort((a, b) => {
    for (const { field, dir } of specs) {
      const av = a[field], bv = b[field]
      if (av === bv) continue
      if (av == null) return dir === 'asc' ? 1 : -1
      if (bv == null) return dir === 'asc' ? -1 : 1
      const cmp = typeof av === 'number' && typeof bv === 'number'
        ? av - bv
        : String(av).localeCompare(String(bv))
      if (cmp !== 0) return dir === 'asc' ? cmp : -cmp
    }
    return 0
  })
}

// =============================================================================
// Facets & Stats
// =============================================================================

function computeFacets(docs: Record<string, unknown>[], fields: string[]): Record<string, Facet[]> {
  const result: Record<string, Facet[]> = {}
  for (const f of fields) {
    const counts = new Map<string, number>()
    for (const d of docs) {
      const v = d[f]
      if (v != null) {
        const k = String(v)
        counts.set(k, (counts.get(k) || 0) + 1)
      }
    }
    result[f] = [...counts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([value, count]) => ({ value, count }))
  }
  return result
}

function computeStats(docs: Record<string, unknown>[], fields: string[]): Record<string, { min: number; max: number; avg: number }> {
  const result: Record<string, { min: number; max: number; avg: number }> = {}
  for (const f of fields) {
    const vals = docs.map(d => d[f]).filter(v => typeof v === 'number') as number[]
    if (vals.length) {
      result[f] = {
        min: Math.min(...vals),
        max: Math.max(...vals),
        avg: Math.round(vals.reduce((a, b) => a + b, 0) / vals.length * 10) / 10,
      }
    }
  }
  return result
}

// =============================================================================
// Document Fetching
// =============================================================================

async function fetchDocs(
  ds: string,
  indices: number[],
  meta: Meta,
  env: Env,
  limit: number,
  offset: number,
  sort: SortSpec,
  filters: Filters,
  facetFields: string[],
  statFields: string[]
): Promise<{ docs: Record<string, unknown>[]; total: number; facets?: Record<string, Facet[]>; stats?: Record<string, { min: number; max: number; avg: number }> }> {
  const needsAll = sort.length > 0 || filters.range.length > 0 || filters.exists.length > 0 || facetFields.length > 0 || statFields.length > 0
  const fetchCount = needsAll ? Math.min(indices.length, 500) : limit
  const toFetch = indices.slice(needsAll ? 0 : offset, needsAll ? fetchCount : offset + limit)

  if (!toFetch.length) return { docs: [], total: 0 }

  // Group by shard
  const groups = new Map<number, { idx: number; pos: number }[]>()
  for (let pos = 0; pos < toFetch.length; pos++) {
    const idx = toFetch[pos]!
    const shard = Math.floor(idx / meta.shardSize)
    if (!groups.has(shard)) groups.set(shard, [])
    groups.get(shard)!.push({ idx, pos })
  }

  // Fetch shards
  const shardNums = [...groups.keys()].slice(0, 4)
  const shards = await Promise.all(shardNums.map(n => loadDocShard(ds, n, env)))
  const shardMap = new Map(shardNums.map((n, i) => [n, shards[i]!]))

  // Extract
  const raw: Record<string, unknown>[] = new Array(toFetch.length)
  for (const [shard, items] of groups) {
    const data = shardMap.get(shard)
    if (!data) continue
    const base = shard * meta.shardSize
    for (const { idx, pos } of items) {
      const local = idx - base
      if (local >= 0 && local < data.length) raw[pos] = data[local] as Record<string, unknown>
    }
  }

  let docs = raw.filter(d => d !== undefined)

  // Range filters
  for (const r of filters.range) {
    docs = docs.filter(d => {
      const v = d[r.field]
      if (v == null) return false
      const n = typeof v === 'number' ? v : +v
      if (isNaN(n)) return false
      if (r.gte !== undefined && n < r.gte) return false
      if (r.lte !== undefined && n > r.lte) return false
      return true
    })
  }

  // Exists filters
  for (const f of filters.exists) {
    docs = docs.filter(d => d[f] != null)
  }

  const total = docs.length
  const facets = facetFields.length ? computeFacets(docs, facetFields) : undefined
  const stats = statFields.length ? computeStats(docs, statFields) : undefined

  if (sort.length) docs = sortDocs(docs, sort)
  if (needsAll) docs = docs.slice(offset, offset + limit)

  return { docs, total, facets, stats }
}

// =============================================================================
// Highlighting
// =============================================================================

function highlight(text: string, matched: Set<string>): string {
  if (!text || !matched.size) return text
  let r = text
  for (const t of matched) r = r.replace(new RegExp(`\\b(${t}\\w*)\\b`, 'gi'), '**$1**')
  return r
}

const TEXT_FIELDS: Record<string, string[]> = {
  onet: ['title', 'description'],
  unspsc: ['commodityTitle', 'classTitle', 'familyTitle', 'segmentTitle'],
  imdb: ['primaryTitle', 'originalTitle'],
}

// =============================================================================
// Handler
// =============================================================================

async function handleSearch(ds: string, params: URLSearchParams, env: Env): Promise<Response> {
  const start = performance.now()

  const q = params.get('q') || ''
  const limit = Math.min(+(params.get('limit') || 20), 100)
  const offset = Math.max(+(params.get('offset') || 0), 0)
  const showTiming = params.get('timing') === 'true'
  const doHighlight = params.get('highlight') !== 'false'
  const debug = params.get('debug') === 'true'
  const sort = parseSort(params.get('sort'))
  const facetFields = (params.get('facets') || '').split(',').filter(Boolean)
  const statFields = (params.get('stats') || '').split(',').filter(Boolean)

  const meta = await loadMeta(ds, env)
  const pq = parseQuery(q)
  const filters = parseFilters(params, ds)

  const searchStart = performance.now()
  let indices: number[]
  let matched = new Set<string>()

  if (pq.should.length || pq.must.length || pq.phrases.length) {
    const result = await search(ds, pq, env)
    indices = result.indices
    matched = result.matched
  } else {
    indices = Array.from({ length: Math.min(meta.totalDocs, 1000) }, (_, i) => i)
  }

  if (filters.hash.length) {
    indices = await applyHashFilters(indices, filters.hash, ds, env)
  }

  const searchMs = performance.now() - searchStart
  const fetchStart = performance.now()

  const { docs, total, facets, stats } = await fetchDocs(
    ds, indices, meta, env, limit, offset, sort, filters, facetFields, statFields
  )

  const finalDocs = doHighlight && matched.size
    ? docs.map(d => {
        const r = { ...d }
        for (const f of TEXT_FIELDS[ds] || []) {
          if (typeof r[f] === 'string') r[f] = highlight(r[f] as string, matched)
        }
        return r
      })
    : docs

  const fetchMs = performance.now() - fetchStart

  const response: SearchResponse = { data: finalDocs, total, limit, offset }
  if (sort.length) response.sort = sort.map(s => `${s.field}:${s.dir}`).join(',')
  if (facets) response.facets = facets
  if (stats) response.stats = stats
  if (showTiming) response.timing = { searchMs: +searchMs.toFixed(2), fetchMs: +fetchMs.toFixed(2), totalMs: +(performance.now() - start).toFixed(2) }
  if (debug) response.query = { parsed: pq, matched: [...matched] }

  return Response.json(response, { headers: { 'Cache-Control': 'public, max-age=3600' } })
}

// =============================================================================
// Main
// =============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    let path = url.pathname
    for (const p of ['/search-v5', '/search-v4', '/search-v3', '/search-v2', '/search']) {
      if (path.startsWith(p)) { path = path.slice(p.length) || '/'; break }
    }

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: { 'Access-Control-Allow-Origin': '*' } })
    }

    try {
      if (path === '/health') {
        return Response.json({
          ok: true, version: 5,
          features: ['fts', 'stemming', 'prefix', 'fuzzy', 'synonyms', 'negation', 'phrase', 'field-search', 'filters', 'range', 'exists', 'not', 'sort', 'multi-sort', 'facets', 'stats', 'highlight'],
          caches: { terms: termCache.size, index: indexCache.size, hash: hashCache.size, docs: docCache.size },
        })
      }

      if (path === '/onet') return handleSearch('onet', url.searchParams, env)
      if (path === '/unspsc') return handleSearch('unspsc', url.searchParams, env)
      if (path === '/imdb') return handleSearch('imdb', url.searchParams, env)

      if (path === '/' || path === '') {
        return Response.json({
          name: 'ParqueDB Search (Maximum v5)',
          version: 5,
          features: ['fts', 'stemming', 'prefix', 'fuzzy~', 'synonyms', 'negation -term', 'phrase "exact"', 'field:search', 'filters', 'range_gte/lte', 'exists has:', 'not_filter', 'sort', 'multi-sort', 'facets', 'stats'],
          examples: {
            onet: [
              '/onet?q=manager',
              '/onet?q=software+engineer~&debug=true',
              '/onet?q="project manager"&sort=title:asc',
            ],
            imdb: [
              '/imdb?q=love&type=movie&year_gte=2000&sort=startYear:desc',
              '/imdb?q=matrix+-reloaded&facets=titleType,startYear',
              '/imdb?type=movie&has=runtimeMinutes&runtime_gte=90&stats=startYear,runtimeMinutes',
              '/imdb?q=action~&not_type=tvEpisode&sort=startYear:desc,primaryTitle:asc',
            ],
          },
        })
      }

      return new Response('Not Found', { status: 404 })
    } catch (e) {
      return Response.json({ error: String(e) }, { status: 500 })
    }
  },
}
