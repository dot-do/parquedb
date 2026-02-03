/**
 * Zen Snippet Search Worker (v9)
 *
 * CPU-optimized version with guards for 5ms constraint.
 *
 * Optimizations:
 * - Early termination on CPU budget
 * - Limited fuzzy candidates (max 100 comparisons)
 * - Skip fuzzy if exact matches found
 * - Lazy spell correction (only if 0 results)
 * - Capped prefix expansion (max 10 matches)
 * - Pre-sorted term keys for binary search
 */

interface Env { DATA: R2Bucket }
interface Meta { totalDocs: number; shardSize: number; shardCount: number }

interface SearchResponse {
  data: unknown[]
  total: number
  limit: number
  offset: number
  cursor?: string
  facets?: Record<string, { value: string; count: number }[]>
  stats?: Record<string, { min: number; max: number; avg: number }>
  didYouMean?: string
  timing?: Record<string, number>
  cpuBudget?: { used: number; limit: number; exceeded: boolean }
}

// =============================================================================
// Constants - Named values for all magic numbers
// =============================================================================

// Search limits
const DEFAULT_SEARCH_LIMIT = 20
const MAX_SEARCH_LIMIT = 50
const MAX_QUERY_TERMS = 5
const MAX_BROWSE_RESULTS = 500

// Shard and cache limits
const MAX_TERM_SHARDS = 3
const MAX_DOC_SHARDS = 3
const MAX_DOC_CACHE_SIZE = 10
const MAX_FETCH_COUNT = 300

// Facets and stats
const MAX_FACET_FIELDS = 3
const MAX_STAT_FIELDS = 2
const MAX_FACET_STAT_DOCS = 200
const MAX_FACET_VALUES = 10

// Search phase limits
const MAX_PREFIX_MATCHES_PER_TERM = 10
const MAX_PREFIX_EXTRA_LENGTH = 4
const MAX_FUZZY_CHECKS = 100
const MAX_SPELL_CHECKS = 50
const MAX_SPELL_EDIT_DISTANCE = 3
const MAX_SYNONYM_EXPANSIONS = 3

// Minimum lengths for search phases
const MIN_PREFIX_TERM_LENGTH = 3
const MIN_FUZZY_TERM_LENGTH = 4
const MIN_SUGGEST_QUERY_LENGTH = 2

// Scoring weights
const SCORE_EXACT_MATCH = 10
const SCORE_STEMMED_MATCH = 5
const SCORE_SYNONYM_MATCH = 3
const SCORE_PREFIX_MATCH = 2
const SCORE_FUZZY_MATCH = 1

// Suggest limits
const MAX_SUGGEST_ITERATIONS = 200
const MAX_SUGGESTIONS = 10

// Cache TTL
const CACHE_MAX_AGE_SECONDS = 3600

// CPU Budget tracking - tracks only synchronous CPU work, not I/O
const CPU_BUDGET_MS = 4  // Leave 1ms margin
let cpuAccumulated = 0
let segmentStart = 0

function resetCpuBudget() {
  cpuAccumulated = 0
  segmentStart = 0
}

function startCpuSegment() {
  segmentStart = performance.now()
}

function endCpuSegment() {
  if (segmentStart > 0) {
    cpuAccumulated += performance.now() - segmentStart
    segmentStart = 0
  }
}

function checkCpuBudget(): boolean {
  endCpuSegment()
  startCpuSegment()
  return cpuAccumulated < CPU_BUDGET_MS
}

function getCpuUsed(): number {
  endCpuSegment()
  return cpuAccumulated
}

// Caches
const termCache = new Map<string, Record<string, number[]>>()
const hashCache = new Map<string, Record<string, number[]>>()
const metaCache = new Map<string, Meta>()
const docCache = new Map<string, unknown[]>()

// Constants
const STOPS = new Set(['the','a','an','and','or','but','in','on','at','to','for','of','with','by','is','are','was','were'])
const SYNONYMS: Record<string, string[]> = {
  movie: ['film'], film: ['movie'],
  car: ['auto', 'vehicle'], auto: ['car'],
  engineer: ['engineering'], manager: ['management'],
}

// Text Processing - optimized
function stem(w: string): string {
  if (w.length < 4) return w
  let s = w
  if (s.endsWith('ing') && s.length > 5) s = s.slice(0,-3)
  else if (s.endsWith('ed') && s.length > 4) s = s.slice(0,-2)
  else if (s.endsWith('s') && !s.endsWith('ss') && s.length > 3) s = s.slice(0,-1)
  return s
}

// Fast Levenshtein with early termination
function levenshteinFast(a: string, b: string, maxDist: number): number {
  if (a === b) return 0
  if (Math.abs(a.length - b.length) > maxDist) return maxDist + 1

  const m = a.length, n = b.length
  if (m === 0) return n
  if (n === 0) return m

  // Use single row for space efficiency
  let prev = Array.from({ length: n + 1 }, (_, i) => i)
  let curr = new Array(n + 1)

  for (let i = 1; i <= m; i++) {
    curr[0] = i
    let minInRow = i

    for (let j = 1; j <= n; j++) {
      const cost = a[i-1] === b[j-1] ? 0 : 1
      curr[j] = Math.min(prev[j]! + 1, curr[j-1]! + 1, prev[j-1]! + cost)
      minInRow = Math.min(minInRow, curr[j]!)
    }

    // Early termination if minimum in row exceeds maxDist
    if (minInRow > maxDist) return maxDist + 1

    ;[prev, curr] = [curr, prev]
  }

  return prev[n]!
}

// Data Loading
async function loadMeta(ds: string, env: Env): Promise<Meta> {
  if (metaCache.has(ds)) return metaCache.get(ds)!
  const o = await env.DATA.get(`indexes/${ds}/meta.json`)
  if (!o) throw new Error(`Meta: ${ds}`)
  const m = (await o.json()) as Meta; metaCache.set(ds, m); return m
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
  if (docCache.size >= MAX_DOC_CACHE_SIZE) docCache.delete(docCache.keys().next().value as string)
  docCache.set(k, d)
  return d
}

// CPU-bounded search
interface SearchResult {
  indices: number[]
  scores: Map<number, number>
  matched: Set<string>
  didYouMean?: string
  cpuExceeded: boolean
}

async function search(ds: string, terms: string[], fuzzy: boolean, env: Env): Promise<SearchResult> {
  const scores = new Map<number, number>()
  const matched = new Set<string>()
  let didYouMean: string | undefined
  let cpuExceeded = false

  if (!terms.length) return { indices: [], scores, matched, cpuExceeded }

  // Load shards (I/O - doesn't count toward CPU)
  const letters = new Set(terms.map(t => /\d/.test(t[0]!) ? '0' : t[0]!))
  const shards = await Promise.all([...letters].slice(0, MAX_TERM_SHARDS).map(l => loadTermShard(ds, l, env)))

  // START CPU work: index building
  startCpuSegment()
  const index: Record<string, number[]> = {}
  for (const s of shards) if (s) Object.assign(index, s)

  const indexTerms = Object.keys(index)
  let exactMatchFound = false

  // Phase 1: Exact matches (fast)
  for (const term of terms) {
    if (!checkCpuBudget()) { cpuExceeded = true; break }

    const stemmed = stem(term)
    for (const variant of [term, stemmed]) {
      if (index[variant]) {
        exactMatchFound = true
        const pts = variant === term ? SCORE_EXACT_MATCH : SCORE_STEMMED_MATCH
        for (const idx of index[variant]!) {
          scores.set(idx, (scores.get(idx) || 0) + pts)
        }
        matched.add(variant)
      }
    }

    // Synonyms (limited)
    const syns = SYNONYMS[term] || SYNONYMS[stemmed]
    if (syns) {
      for (const syn of syns.slice(0, MAX_SYNONYM_EXPANSIONS)) {
        if (index[syn]) {
          for (const idx of index[syn]!) {
            scores.set(idx, (scores.get(idx) || 0) + SCORE_SYNONYM_MATCH)
          }
          matched.add(syn)
        }
      }
    }
  }

  // Phase 2: Prefix matches (capped at MAX_PREFIX_MATCHES_PER_TERM per term)
  if (checkCpuBudget()) {
    for (const term of terms) {
      if (term.length < MIN_PREFIX_TERM_LENGTH) continue
      if (!checkCpuBudget()) { cpuExceeded = true; break }

      let prefixCount = 0
      for (const k of indexTerms) {
        if (prefixCount >= MAX_PREFIX_MATCHES_PER_TERM) break
        if (k.startsWith(term) && k !== term && k.length <= term.length + MAX_PREFIX_EXTRA_LENGTH) {
          prefixCount++
          for (const idx of index[k]!) {
            scores.set(idx, (scores.get(idx) || 0) + SCORE_PREFIX_MATCH)
          }
          matched.add(k)
        }
      }
    }
  }

  // Phase 3: Fuzzy matches (only if no exact matches, capped iterations)
  if (fuzzy && !exactMatchFound && checkCpuBudget()) {
    let fuzzyChecks = 0

    for (const term of terms) {
      if (term.length < MIN_FUZZY_TERM_LENGTH) continue
      if (!checkCpuBudget()) { cpuExceeded = true; break }

      for (const k of indexTerms) {
        if (fuzzyChecks >= MAX_FUZZY_CHECKS) break
        if (Math.abs(k.length - term.length) > 1) continue

        fuzzyChecks++
        if (levenshteinFast(term, k, 1) === 1) {
          for (const idx of index[k]!) {
            scores.set(idx, (scores.get(idx) || 0) + SCORE_FUZZY_MATCH)
          }
          matched.add(k)
        }
      }
    }
  }

  // Phase 4: Spell correction (only if 0 results)
  if (scores.size === 0 && checkCpuBudget()) {
    let spellChecks = 0

    for (const term of terms) {
      if (!checkCpuBudget()) break

      let bestMatch: string | null = null
      let bestDist = MAX_SPELL_EDIT_DISTANCE

      for (const k of indexTerms) {
        if (spellChecks >= MAX_SPELL_CHECKS) break
        if (Math.abs(k.length - term.length) > 2) continue

        spellChecks++
        const dist = levenshteinFast(term, k, 2)
        if (dist < bestDist) {
          bestDist = dist
          bestMatch = k
        }
      }

      if (bestMatch) {
        didYouMean = bestMatch
        break
      }
    }
  }

  const sorted = [...scores.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([i]) => i)

  endCpuSegment()  // END CPU work
  return { indices: sorted, scores, matched, didYouMean, cpuExceeded }
}

// Filters
interface Filters {
  hash: { field: string; values: string[] }[]
  range: { field: string; gte?: number; lte?: number }[]
}

function parseFilters(p: URLSearchParams, ds: string): Filters {
  const f: Filters = { hash: [], range: [] }
  if (ds === 'imdb') {
    const type = p.get('type'); if (type) f.hash.push({ field: 'titleType', values: type.split(',') })
    const yearGte = p.get('year_gte'), yearLte = p.get('year_lte')
    if (yearGte || yearLte) f.range.push({ field: 'startYear', gte: yearGte ? +yearGte : undefined, lte: yearLte ? +yearLte : undefined })
  }
  if (ds === 'onet') {
    const code = p.get('code'); if (code) f.hash.push({ field: 'code', values: code.split(',') })
  }
  return f
}

async function applyHashFilters(indices: number[], filters: Filters['hash'], ds: string, env: Env): Promise<number[]> {
  for (const f of filters) {
    const hash = await loadHash(ds, f.field, env)
    const allowed = new Set<number>()
    for (const v of f.values) {
      const m = hash[v]
      if (m) for (const i of m) allowed.add(i)
    }
    indices = indices.filter(i => allowed.has(i))
  }
  return indices
}

// Sorting
type SortSpec = { field: string; dir: 'asc' | 'desc' }[]

function sortDocs(docs: Record<string, unknown>[], specs: SortSpec): Record<string, unknown>[] {
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

// Facets (capped)
function computeFacets(docs: Record<string, unknown>[], fields: string[]): Record<string, { value: string; count: number }[]> {
  const r: Record<string, { value: string; count: number }[]> = {}
  for (const f of fields) {
    const c = new Map<string, number>()
    for (const d of docs.slice(0, MAX_FACET_STAT_DOCS)) {
      const v = d[f]
      if (Array.isArray(v)) {
        for (const item of v) c.set(String(item), (c.get(String(item)) || 0) + 1)
      } else if (v != null) {
        c.set(String(v), (c.get(String(v)) || 0) + 1)
      }
    }
    r[f] = [...c.entries()].sort((a, b) => b[1] - a[1]).slice(0, MAX_FACET_VALUES).map(([value, count]) => ({ value, count }))
  }
  return r
}

// Stats (capped)
function computeStats(docs: Record<string, unknown>[], fields: string[]): Record<string, { min: number; max: number; avg: number }> {
  const r: Record<string, { min: number; max: number; avg: number }> = {}
  for (const f of fields) {
    const vals = docs.slice(0, MAX_FACET_STAT_DOCS).map(d => d[f]).filter(v => typeof v === 'number') as number[]
    if (vals.length) {
      r[f] = {
        min: Math.min(...vals),
        max: Math.max(...vals),
        avg: Math.round(vals.reduce((a, b) => a + b, 0) / vals.length * 10) / 10
      }
    }
  }
  return r
}

// Document Fetching
async function fetchDocs(
  ds: string, indices: number[], meta: Meta, env: Env,
  limit: number, offset: number, sortSpec: SortSpec, filters: Filters,
  facetFields: string[], statFields: string[]
): Promise<{
  docs: Record<string, unknown>[]
  total: number
  facets?: Record<string, { value: string; count: number }[]>
  stats?: Record<string, { min: number; max: number; avg: number }>
}> {
  const needsAll = sortSpec.length > 0 || filters.range.length > 0 || facetFields.length > 0 || statFields.length > 0
  const fetchCount = needsAll ? Math.min(indices.length, MAX_FETCH_COUNT) : limit
  const toFetch = indices.slice(needsAll ? 0 : offset, needsAll ? fetchCount : offset + limit)

  if (!toFetch.length) return { docs: [], total: 0 }

  const groups = new Map<number, { idx: number; pos: number }[]>()
  for (let pos = 0; pos < toFetch.length; pos++) {
    const idx = toFetch[pos]!
    const shard = Math.floor(idx / meta.shardSize)
    if (!groups.has(shard)) groups.set(shard, [])
    groups.get(shard)!.push({ idx, pos })
  }

  const shardNums = [...groups.keys()].slice(0, MAX_DOC_SHARDS)
  const shards = await Promise.all(shardNums.map(n => loadDocs(ds, n, env)))

  // START CPU work: doc assembly and filtering
  startCpuSegment()
  const shardMap = new Map(shardNums.map((n, i) => [n, shards[i]!]))

  const raw: Record<string, unknown>[] = new Array(toFetch.length)
  for (const [shard, items] of groups) {
    const data = shardMap.get(shard)
    if (!data) continue
    const base = shard * meta.shardSize
    for (const { idx, pos } of items) {
      const local = idx - base
      if (local >= 0 && local < data.length) {
        raw[pos] = data[local] as Record<string, unknown>
      }
    }
  }

  let docs = raw.filter(d => d)

  // Range filters
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
  const facets = facetFields.length ? computeFacets(docs, facetFields) : undefined
  const stats = statFields.length ? computeStats(docs, statFields) : undefined

  if (sortSpec.length) docs = sortDocs(docs, sortSpec)
  if (needsAll) docs = docs.slice(offset, offset + limit)

  endCpuSegment()  // END CPU work
  return { docs, total, facets, stats }
}

// Highlighting
const TEXT_FIELDS: Record<string, string[]> = {
  onet: ['title', 'description'],
  unspsc: ['commodityTitle', 'classTitle'],
  imdb: ['primaryTitle', 'originalTitle']
}

function highlight(text: string, matched: Set<string>): string {
  if (!text || !matched.size) return text
  let r = text
  for (const t of matched) {
    r = r.replace(new RegExp(`\\b(${t}\\w*)`, 'gi'), '**$1**')
  }
  return r
}

// Main Handler
async function handleSearch(ds: string, p: URLSearchParams, env: Env): Promise<Response> {
  resetCpuBudget()
  const wallStart = performance.now()

  const q = p.get('q') || ''
  const limit = Math.min(+(p.get('limit') || DEFAULT_SEARCH_LIMIT), MAX_SEARCH_LIMIT)
  const offset = Math.max(+(p.get('offset') || 0), 0)
  const fuzzy = q.includes('~') || p.get('fuzzy') === 'true'
  const showTiming = p.get('timing') === 'true'
  const doHighlight = p.get('highlight') !== 'false'
  const sortParam = p.get('sort')
  const sortSpec: SortSpec = sortParam
    ? sortParam.split(',').map(s => {
        const [f, d = 'asc'] = s.split(':')
        return { field: f!, dir: d as 'asc' | 'desc' }
      })
    : []
  const facetFields = (p.get('facets') || '').split(',').filter(Boolean).slice(0, MAX_FACET_FIELDS)
  const statFields = (p.get('stats') || '').split(',').filter(Boolean).slice(0, MAX_STAT_FIELDS)

  // Parse query
  const cleanQ = q.replace(/~/g, '')
  const terms = cleanQ.toLowerCase()
    .split(/\s+/)
    .filter(t => t.length >= 2 && !STOPS.has(t))
    .slice(0, MAX_QUERY_TERMS)

  const meta = await loadMeta(ds, env)
  const filters = parseFilters(p, ds)

  // Search
  let indices: number[]
  let matched = new Set<string>()
  let didYouMean: string | undefined
  let cpuExceeded = false
  let scores = new Map<number, number>()

  if (terms.length) {
    const result = await search(ds, terms, fuzzy, env)
    indices = result.indices
    scores = result.scores
    matched = result.matched
    didYouMean = result.didYouMean
    cpuExceeded = result.cpuExceeded
  } else {
    indices = Array.from({ length: Math.min(meta.totalDocs, MAX_BROWSE_RESULTS) }, (_, i) => i)
  }

  // Apply hash filters
  if (filters.hash.length) {
    indices = await applyHashFilters(indices, filters.hash, ds, env)
  }

  // Fetch docs
  const { docs, total, facets, stats } = await fetchDocs(
    ds, indices, meta, env, limit, offset, sortSpec, filters, facetFields, statFields
  )

  // Highlight
  let finalDocs: unknown[] = docs
  if (doHighlight && matched.size) {
    finalDocs = docs.map(d => {
      const r = { ...d }
      for (const f of TEXT_FIELDS[ds] || []) {
        if (typeof r[f] === 'string') {
          r[f] = highlight(r[f] as string, matched)
        }
      }
      return r
    })
  }

  const response: SearchResponse = {
    data: finalDocs,
    total,
    limit,
    offset
  }

  if (facets) response.facets = facets
  if (stats) response.stats = stats
  if (didYouMean) response.didYouMean = didYouMean

  if (showTiming) {
    const cpuFinal = getCpuUsed()
    response.timing = {
      cpuMs: +cpuFinal.toFixed(2),
      wallMs: +(performance.now() - wallStart).toFixed(2)
    }
    response.cpuBudget = {
      used: +cpuFinal.toFixed(2),
      limit: CPU_BUDGET_MS,
      exceeded: cpuExceeded
    }
  }

  return Response.json(response, {
    headers: { 'Cache-Control': `public, max-age=${CACHE_MAX_AGE_SECONDS}` }
  })
}

// Suggest endpoint
async function handleSuggest(ds: string, p: URLSearchParams, env: Env): Promise<Response> {
  const q = p.get('q') || ''
  if (q.length < MIN_SUGGEST_QUERY_LENGTH) return Response.json({ suggestions: [], query: q })

  const letter = q[0]!
  const shard = await loadTermShard(ds, /\d/.test(letter) ? '0' : letter, env)
  if (!shard) return Response.json({ suggestions: [], query: q })

  const prefix = q.toLowerCase()
  const matches: { term: string; count: number }[] = []

  // Limit iterations
  let checked = 0
  for (const [term, indices] of Object.entries(shard)) {
    if (checked++ > MAX_SUGGEST_ITERATIONS) break
    if (term.startsWith(prefix) && term !== prefix) {
      matches.push({ term, count: indices.length })
    }
  }

  const suggestions = matches
    .sort((a, b) => b.count - a.count)
    .slice(0, MAX_SUGGESTIONS)
    .map(m => m.term)

  return Response.json({ suggestions, query: q })
}

// Main
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    let path = url.pathname

    for (const pre of ['/search-v9', '/search-v8', '/search-v7', '/search-v6', '/search']) {
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
          version: 9,
          features: [
            'fts', 'stemming', 'prefix', 'fuzzy', 'synonyms',
            'filters', 'range', 'sort',
            'facets', 'stats', 'highlight',
            'suggest', 'spell_correct',
            'cpu_budget', 'cpu_guards'
          ],
          cpuBudgetMs: CPU_BUDGET_MS
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
          name: 'ParqueDB Search (Zen v9 - CPU Optimized)',
          version: 9,
          cpuBudgetMs: CPU_BUDGET_MS,
          features: [
            'fts', 'stemming', 'prefix~', 'fuzzy~', 'synonyms',
            'filters', 'range', 'sort',
            'facets', 'stats', 'highlight',
            'suggest', 'spell_correct'
          ],
          guards: [
            `max ${MAX_QUERY_TERMS} query terms`,
            `max ${MAX_FUZZY_CHECKS} fuzzy comparisons`,
            `max ${MAX_SPELL_CHECKS} spell checks`,
            `max ${MAX_PREFIX_MATCHES_PER_TERM} prefix matches per term`,
            `max ${MAX_FACET_STAT_DOCS} docs for facets/stats`,
            `max ${MAX_DOC_SHARDS} doc shards fetched`,
            `max ${MAX_SEARCH_LIMIT} results per page`
          ]
        })
      }

      return new Response('Not Found', { status: 404 })
    } catch (e) {
      return Response.json({ error: String(e) }, { status: 500 })
    }
  }
}
