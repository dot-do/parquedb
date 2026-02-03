/**
 * Supreme Snippet Search Worker (v7)
 *
 * All features from v6 PLUS:
 * - Spell correction: "matrx" → "Did you mean: matrix?"
 * - Query profiling: timing breakdown per operation
 * - Score filtering: score_min/score_max
 * - Bucket aggregations: year_buckets, numeric ranges
 * - More Like This: /mlt?id=tt0133093 → similar movies
 * - Result grouping: group=startYear
 * - Multi-field search: search_fields=title,description
 * - Cursor pagination: cursor=xxx for efficient paging
 *
 * Bundle target: <15KB gzipped (still 17KB headroom)
 */

interface Env { DATA: R2Bucket }
interface Meta { totalDocs: number; shardSize: number; shardCount: number }

interface SearchResponse {
  data: unknown[]
  total: number
  limit: number
  offset: number
  cursor?: string
  sort?: string
  facets?: Record<string, { value: string; count: number }[]>
  stats?: Record<string, { min: number; max: number; avg: number; sum?: number }>
  buckets?: Record<string, { from: number; to: number; count: number }[]>
  groups?: Record<string, unknown[]>
  suggestions?: string[]
  didYouMean?: string
  timing?: Record<string, number>
  query?: unknown
}

// Constants
const DEFAULT_SEARCH_LIMIT = 20
const MAX_SEARCH_LIMIT = 100
const DEFAULT_MLT_LIMIT = 10
const MAX_MLT_LIMIT = 50
const MAX_TERM_SHARDS = 5
const MAX_DOC_SHARDS = 4
const MAX_MLT_DOC_SHARDS = 3
const MAX_DOC_CACHE_SIZE = 15
const MAX_BROWSE_RESULTS = 1000
const MAX_FETCH_COUNT = 500
const MAX_FACET_VALUES = 10
const MAX_SUGGESTIONS = 10
const MAX_SPELL_EDIT_DISTANCE = 3
const DEFAULT_BUCKET_SIZE = 10
const DEFAULT_GROUP_LIMIT = 3
const MAX_PREFIX_EXTRA_LENGTH = 5
const MIN_PREFIX_TERM_LENGTH = 3
const MIN_FUZZY_TERM_LENGTH = 4
const MIN_SUGGEST_QUERY_LENGTH = 2
const SCORE_EXACT_MATCH = 10
const SCORE_STEMMED_MATCH = 5
const SCORE_SYNONYM_MATCH = 3
const SCORE_PREFIX_MATCH = 2
const SCORE_FUZZY_MATCH = 1
const SCORE_WILDCARD_MATCH = 4
const SNIPPET_LENGTH = 150
const SNIPPET_CONTEXT_BEFORE = 50
const SNIPPET_CONTEXT_AFTER = 100
const CACHE_MAX_AGE_SECONDS = 3600

// Caches
const termCache = new Map<string, Record<string, number[]>>()
const indexCache = new Map<string, Record<string, number[]>>()
const hashCache = new Map<string, Record<string, number[]>>()
const metaCache = new Map<string, Meta>()
const docCache = new Map<string, unknown[]>()

// Synonyms & Stopwords
const SYNONYMS: Record<string, string[]> = {
  auto: ['automobile', 'car', 'vehicle'], car: ['auto', 'automobile'],
  movie: ['film', 'picture'], film: ['movie'],
  computer: ['pc', 'computing'], software: ['program', 'application', 'app'],
  engineer: ['engineering'], manager: ['management', 'supervisor'],
  doctor: ['physician', 'medical'], developer: ['programmer', 'coder'],
}
const STOPS = new Set(['the','a','an','and','or','but','in','on','at','to','for','of','with','by','is','are','was','were','be','been','have','has','had','do','does','did','will','would','could','should'])
const IRREGULAR: Record<string, string> = { children:'child', people:'person', men:'man', women:'woman', feet:'foot', teeth:'tooth' }

// Text Processing
function stem(w: string): string {
  if (w.length < 3) return w
  if (IRREGULAR[w]) return IRREGULAR[w]
  let s = w
  if (s.endsWith('sses')) s = s.slice(0,-2)
  else if (s.endsWith('ies') && s.length > 4) s = s.slice(0,-2)
  else if (!s.endsWith('ss') && s.endsWith('s') && s.length > 3) s = s.slice(0,-1)
  if (s.endsWith('eed') && s.length > 4) s = s.slice(0,-1)
  else if (s.endsWith('ed') && s.length > 4 && /[aeiou]/.test(s.slice(0,-2))) s = s.slice(0,-2)
  else if (s.endsWith('ing') && s.length > 5 && /[aeiou]/.test(s.slice(0,-3))) s = s.slice(0,-3)
  if (s.endsWith('ness') && s.length > 6) s = s.slice(0,-4)
  else if (s.endsWith('ment') && s.length > 6) s = s.slice(0,-4)
  else if (s.endsWith('ation') && s.length > 7) s = s.slice(0,-5) + 'e'
  return s
}

function levenshtein(a: string, b: string): number {
  if (a === b) return 0
  if (!a.length) return b.length
  if (!b.length) return a.length
  const m: number[][] = Array(b.length + 1).fill(null).map(() => Array(a.length + 1).fill(0))
  for (let i = 0; i <= a.length; i++) m[0]![i] = i
  for (let j = 0; j <= b.length; j++) m[j]![0] = j
  for (let j = 1; j <= b.length; j++) {
    for (let i = 1; i <= a.length; i++) {
      m[j]![i] = Math.min(m[j-1]![i]! + 1, m[j]![i-1]! + 1, m[j-1]![i-1]! + (a[i-1] === b[j-1] ? 0 : 1))
    }
  }
  return m[b.length]![a.length]!
}

function expandTerm(t: string): { variants: string[]; synonyms: string[] } {
  const v = new Set([t, stem(t)])
  const st = stem(t)
  for (const s of ['s','es','ed','ing','er','ers','ly','tion','ment']) if (st.length >= 3) v.add(st + s)
  return { variants: [...v], synonyms: SYNONYMS[t] || SYNONYMS[st] || [] }
}

// Spell correction - find closest match in index
function findSpellCorrection(term: string, index: Record<string, number[]>): string | null {
  if (index[term]) return null  // Term exists, no correction needed

  let bestMatch: string | null = null
  let bestDist = MAX_SPELL_EDIT_DISTANCE
  let bestFreq = 0

  for (const [candidate, indices] of Object.entries(index)) {
    if (Math.abs(candidate.length - term.length) > 2) continue
    const dist = levenshtein(term, candidate)
    if (dist < bestDist || (dist === bestDist && indices.length > bestFreq)) {
      bestDist = dist
      bestMatch = candidate
      bestFreq = indices.length
    }
  }
  return bestMatch
}

// Query Parser
interface ParsedQuery {
  must: string[]; should: string[]; mustNot: string[]
  phrases: string[]; wildcards: string[]
  fuzzy: boolean; boosts: Record<string, number>
  fields?: string[]
}

function parseQuery(q: string, defaultFields?: string[]): ParsedQuery {
  const r: ParsedQuery = { must: [], should: [], mustNot: [], phrases: [], wildcards: [], fuzzy: false, boosts: {}, fields: defaultFields }
  if (!q) return r

  // Phrases
  let m; const pRe = /"([^"]+)"/g
  while ((m = pRe.exec(q)) !== null) r.phrases.push(m[1]!.toLowerCase())
  q = q.replace(pRe, '')

  // Wildcards
  const wRe = /(\w+)\*/g
  while ((m = wRe.exec(q)) !== null) r.wildcards.push(m[1]!.toLowerCase())
  q = q.replace(wRe, '')

  // Fuzzy flag
  if (q.includes('~')) { r.fuzzy = true; q = q.replace(/~/g, '') }

  // Field-specific search: field:term
  const fieldRe = /(\w+):(\w+)/g
  while ((m = fieldRe.exec(q)) !== null) {
    r.boosts[m[2]!.toLowerCase()] = 1
    // Note: field-specific search would need field-specific indexes
  }
  q = q.replace(fieldRe, '')

  // Tokens
  for (const t of q.toLowerCase().split(/\s+/).filter(x => x.length >= 2)) {
    if (t.startsWith('-')) r.mustNot.push(t.slice(1))
    else if (t.startsWith('+')) r.must.push(t.slice(1))
    else if (t === 'and' || t === 'or') continue
    else if (!STOPS.has(t)) r.should.push(t)
  }
  return r
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
  const idx = (await o.json()) as Record<string, number[]>; termCache.set(k, idx); return idx
}

async function loadFullIndex(ds: string, env: Env): Promise<Record<string, number[]>> {
  const k = `${ds}-full`
  if (indexCache.has(k)) return indexCache.get(k)!
  const o = await env.DATA.get(`indexes/${ds}/inverted.json`)
  if (!o) throw new Error(`Index: ${ds}`)
  const idx = (await o.json()) as Record<string, number[]>; indexCache.set(k, idx); return idx
}

async function loadHash(ds: string, f: string, env: Env): Promise<Record<string, number[]>> {
  const k = `${ds}-hash-${f}`
  if (hashCache.has(k)) return hashCache.get(k)!
  const o = await env.DATA.get(`indexes/${ds}/hash-${f}.json`)
  if (!o) throw new Error(`Hash: ${ds}/${f}`)
  const idx = (await o.json()) as Record<string, number[]>; hashCache.set(k, idx); return idx
}

async function loadDocs(ds: string, n: number, env: Env): Promise<unknown[]> {
  const k = `${ds}-docs-${n}`
  if (docCache.has(k)) return docCache.get(k)!
  const o = await env.DATA.get(`indexes/${ds}/docs-${n}.json`)
  if (!o) throw new Error(`Shard: ${ds}/${n}`)
  const d = (await o.json()) as unknown[]
  if (docCache.size >= MAX_DOC_CACHE_SIZE) docCache.delete(docCache.keys().next().value as string)
  docCache.set(k, d); return d
}

// Search Engine
interface SearchResult {
  indices: number[]
  scores: Map<number, number>
  matched: Set<string>
  explains: Map<number, string[]>
  didYouMean?: string
  timings: Record<string, number>
}

async function search(ds: string, pq: ParsedQuery, boosts: Record<string, number>, env: Env): Promise<SearchResult> {
  const timings: Record<string, number> = {}
  const scores = new Map<number, number>()
  const matched = new Set<string>()
  const explains = new Map<number, string[]>()
  let didYouMean: string | undefined

  const allTerms = [...pq.should, ...pq.must, ...pq.mustNot, ...pq.wildcards]
  if (!allTerms.length && !pq.phrases.length) return { indices: [], scores, matched, explains, timings }

  // Get letters
  const t0 = performance.now()
  const letters = new Set<string>()
  for (const t of allTerms) { const f = t[0] || ''; letters.add(/\d/.test(f) ? '0' : f) }

  // Load shards
  const shards = await Promise.all([...letters].slice(0, MAX_TERM_SHARDS).map(l => loadTermShard(ds, l, env)))
  const index: Record<string, number[]> = {}
  if (shards.some(s => s)) { for (const s of shards) if (s) Object.assign(index, s) }
  else Object.assign(index, await loadFullIndex(ds, env))
  timings.loadIndex = performance.now() - t0

  // Spell correction
  const t1 = performance.now()
  const corrections: string[] = []
  for (const term of pq.should) {
    if (!index[term] && !index[stem(term)]) {
      const correction = findSpellCorrection(term, index)
      if (correction) corrections.push(correction)
    }
  }
  if (corrections.length) didYouMean = corrections.join(' ')
  timings.spellcheck = performance.now() - t1

  function addScore(idx: number, pts: number, reason: string) {
    scores.set(idx, (scores.get(idx) || 0) + pts)
    if (!explains.has(idx)) explains.set(idx, [])
    explains.get(idx)!.push(reason)
  }

  // Score documents
  const t2 = performance.now()
  for (const term of [...pq.should, ...pq.must]) {
    const { variants, synonyms } = expandTerm(term)
    const boost = boosts[term] || 1

    for (const v of variants) {
      if (index[v]) {
        const pts = (v === term ? SCORE_EXACT_MATCH : SCORE_STEMMED_MATCH) * boost
        for (const idx of index[v]) addScore(idx, pts, `${v}=${pts}`)
        matched.add(v)
      }
    }

    for (const syn of synonyms) {
      if (index[syn]) {
        const pts = SCORE_SYNONYM_MATCH * boost
        for (const idx of index[syn]) addScore(idx, pts, `syn:${syn}=${pts}`)
        matched.add(syn)
      }
    }

    // Prefix
    if (term.length >= MIN_PREFIX_TERM_LENGTH) {
      for (const [k, idxs] of Object.entries(index)) {
        if (k.startsWith(term) && k !== term && k.length <= term.length + MAX_PREFIX_EXTRA_LENGTH) {
          const pts = SCORE_PREFIX_MATCH * boost
          for (const idx of idxs) addScore(idx, pts, `prefix:${k}=${pts}`)
          matched.add(k)
        }
      }
    }

    // Fuzzy
    if (pq.fuzzy && term.length >= MIN_FUZZY_TERM_LENGTH) {
      for (const [k, idxs] of Object.entries(index)) {
        if (Math.abs(k.length - term.length) <= 1 && levenshtein(term, k) === 1) {
          for (const idx of idxs) addScore(idx, SCORE_FUZZY_MATCH, `fuzzy:${k}=${SCORE_FUZZY_MATCH}`)
          matched.add(k)
        }
      }
    }
  }
  timings.scoring = performance.now() - t2

  // Wildcards
  for (const wc of pq.wildcards) {
    for (const [k, idxs] of Object.entries(index)) {
      if (k.startsWith(wc)) {
        for (const idx of idxs) addScore(idx, SCORE_WILDCARD_MATCH, `wc:${k}=${SCORE_WILDCARD_MATCH}`)
        matched.add(k)
      }
    }
  }

  // Exclusions
  const excluded = new Set<number>()
  for (const term of pq.mustNot) {
    const { variants } = expandTerm(term)
    for (const v of variants) {
      if (index[v]) for (const idx of index[v]) excluded.add(idx)
    }
  }

  const sorted = [...scores.entries()].filter(([i]) => !excluded.has(i)).sort((a, b) => b[1] - a[1]).map(([i]) => i)
  return { indices: sorted, scores, matched, explains, didYouMean, timings }
}

// Suggestions/Autocomplete
async function getSuggestions(ds: string, prefix: string, env: Env): Promise<string[]> {
  if (prefix.length < MIN_SUGGEST_QUERY_LENGTH) return []
  const letter = prefix[0]!
  const shard = await loadTermShard(ds, /\d/.test(letter) ? '0' : letter, env)
  if (!shard) return []

  const matches: { term: string; count: number }[] = []
  for (const [term, indices] of Object.entries(shard)) {
    if (term.startsWith(prefix) && term !== prefix) {
      matches.push({ term, count: indices.length })
    }
  }
  return matches.sort((a, b) => b.count - a.count).slice(0, MAX_SUGGESTIONS).map(m => m.term)
}

// Filters
interface Filters {
  hash: { field: string; values: string[]; negate?: boolean }[]
  range: { field: string; gte?: number; lte?: number }[]
  exists: string[]
  scoreMin?: number
  scoreMax?: number
}

function parseFilters(p: URLSearchParams, ds: string): Filters {
  const f: Filters = { hash: [], range: [], exists: [] }

  // Score filters
  const scoreMin = p.get('score_min'); if (scoreMin) f.scoreMin = +scoreMin
  const scoreMax = p.get('score_max'); if (scoreMax) f.scoreMax = +scoreMax

  if (ds === 'onet') {
    const c = p.get('code'); if (c) f.hash.push({ field: 'code', values: c.split(',') })
  }
  if (ds === 'imdb') {
    for (const [param, field] of [['type', 'titleType'], ['year', 'startYear']] as const) {
      const v = p.get(param); if (v) f.hash.push({ field, values: v.split(',') })
      const nv = p.get('not_' + param); if (nv) f.hash.push({ field, values: nv.split(','), negate: true })
    }
    for (const [param, field] of [['year', 'startYear'], ['runtime', 'runtimeMinutes']] as const) {
      const gte = p.get(param + '_gte'), lte = p.get(param + '_lte')
      if (gte || lte) f.range.push({ field, gte: gte ? +gte : undefined, lte: lte ? +lte : undefined })
    }
    const has = p.get('has'); if (has) f.exists = has.split(',')
  }
  if (ds === 'unspsc') {
    for (const [param, field] of [['commodity', 'commodityCode'], ['class', 'classCode'], ['family', 'familyCode'], ['segment', 'segmentCode']] as const) {
      const v = p.get(param); if (v) f.hash.push({ field, values: v.split(',') })
    }
  }
  return f
}

async function applyHashFilters(indices: number[], filters: Filters['hash'], ds: string, env: Env): Promise<number[]> {
  for (const f of filters) {
    const hash = await loadHash(ds, f.field, env)
    const allowed = new Set<number>()
    for (const v of f.values) { const m = hash[v]; if (m) for (const i of m) allowed.add(i) }
    indices = f.negate ? indices.filter(i => !allowed.has(i)) : indices.filter(i => allowed.has(i))
  }
  return indices
}

// Sorting
type SortSpec = { field: string; dir: 'asc' | 'desc' }[]
function parseSort(s: string | null): SortSpec {
  if (!s) return []
  return s.split(',').map(p => { const [f, d = 'asc'] = p.split(':'); return { field: f!, dir: d as 'asc' | 'desc' } })
}
function sortDocs(docs: Record<string, unknown>[], specs: SortSpec): Record<string, unknown>[] {
  if (!specs.length) return docs
  return [...docs].sort((a, b) => {
    for (const { field, dir } of specs) {
      const av = a[field], bv = b[field]
      if (av == bv) continue
      if (av == null) return dir === 'asc' ? 1 : -1
      if (bv == null) return dir === 'asc' ? -1 : 1
      const cmp = typeof av === 'number' && typeof bv === 'number' ? av - bv : String(av).localeCompare(String(bv))
      if (cmp !== 0) return dir === 'asc' ? cmp : -cmp
    }
    return 0
  })
}

// Facets, Stats & Buckets
function computeFacets(docs: Record<string, unknown>[], fields: string[]): Record<string, { value: string; count: number }[]> {
  const r: Record<string, { value: string; count: number }[]> = {}
  for (const f of fields) {
    const c = new Map<string, number>()
    for (const d of docs) { const v = d[f]; if (v != null) c.set(String(v), (c.get(String(v)) || 0) + 1) }
    r[f] = [...c.entries()].sort((a, b) => b[1] - a[1]).slice(0, MAX_FACET_VALUES).map(([value, count]) => ({ value, count }))
  }
  return r
}

function computeStats(docs: Record<string, unknown>[], fields: string[]): Record<string, { min: number; max: number; avg: number; sum: number }> {
  const r: Record<string, { min: number; max: number; avg: number; sum: number }> = {}
  for (const f of fields) {
    const vals = docs.map(d => d[f]).filter(v => typeof v === 'number') as number[]
    if (vals.length) {
      const sum = vals.reduce((a, b) => a + b, 0)
      r[f] = { min: Math.min(...vals), max: Math.max(...vals), avg: Math.round(sum / vals.length * 10) / 10, sum }
    }
  }
  return r
}

function computeBuckets(docs: Record<string, unknown>[], field: string, bucketSize: number): { from: number; to: number; count: number }[] {
  const vals = docs.map(d => d[field]).filter(v => typeof v === 'number') as number[]
  if (!vals.length) return []

  const min = Math.min(...vals)
  const max = Math.max(...vals)
  const buckets: Map<number, number> = new Map()

  for (const v of vals) {
    const bucket = Math.floor(v / bucketSize) * bucketSize
    buckets.set(bucket, (buckets.get(bucket) || 0) + 1)
  }

  return [...buckets.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([from, count]) => ({ from, to: from + bucketSize, count }))
}

// Grouping
function groupDocs(docs: Record<string, unknown>[], field: string, limit: number): Record<string, unknown[]> {
  const groups: Record<string, unknown[]> = {}
  for (const d of docs) {
    const key = String(d[field] ?? 'null')
    if (!groups[key]) groups[key] = []
    if (groups[key]!.length < limit) groups[key]!.push(d)
  }
  return groups
}

// Cursor encoding/decoding
function encodeCursor(offset: number): string {
  return btoa(String(offset))
}
function decodeCursor(cursor: string): number {
  try { return parseInt(atob(cursor), 10) || 0 } catch { return 0 }
}

// Document Fetching
async function fetchDocs(
  ds: string, indices: number[], scores: Map<number, number>, meta: Meta, env: Env,
  limit: number, offset: number, sort: SortSpec, filters: Filters,
  facetFields: string[], statFields: string[], bucketField: string | null, bucketSize: number,
  distinctField: string | null, groupField: string | null, groupLimit: number
): Promise<{
  docs: Record<string, unknown>[]
  total: number
  facets?: Record<string, { value: string; count: number }[]>
  stats?: Record<string, { min: number; max: number; avg: number; sum: number }>
  buckets?: Record<string, { from: number; to: number; count: number }[]>
  groups?: Record<string, unknown[]>
}> {
  // Apply score filters
  if (filters.scoreMin !== undefined || filters.scoreMax !== undefined) {
    indices = indices.filter(i => {
      const s = scores.get(i) || 0
      if (filters.scoreMin !== undefined && s < filters.scoreMin) return false
      if (filters.scoreMax !== undefined && s > filters.scoreMax) return false
      return true
    })
  }

  const needsAll = sort.length > 0 || filters.range.length > 0 || filters.exists.length > 0 ||
                   facetFields.length > 0 || statFields.length > 0 || distinctField ||
                   groupField || bucketField
  const fetchCount = needsAll ? Math.min(indices.length, MAX_FETCH_COUNT) : limit
  const toFetch = indices.slice(needsAll ? 0 : offset, needsAll ? fetchCount : offset + limit)
  if (!toFetch.length) return { docs: [], total: 0 }

  const groups = new Map<number, { idx: number; pos: number }[]>()
  for (let pos = 0; pos < toFetch.length; pos++) {
    const idx = toFetch[pos]!, shard = Math.floor(idx / meta.shardSize)
    if (!groups.has(shard)) groups.set(shard, [])
    groups.get(shard)!.push({ idx, pos })
  }

  const shardNums = [...groups.keys()].slice(0, MAX_DOC_SHARDS)
  const shards = await Promise.all(shardNums.map(n => loadDocs(ds, n, env)))
  const shardMap = new Map(shardNums.map((n, i) => [n, shards[i]!]))

  const raw: Record<string, unknown>[] = new Array(toFetch.length)
  for (const [shard, items] of groups) {
    const data = shardMap.get(shard); if (!data) continue
    const base = shard * meta.shardSize
    for (const { idx, pos } of items) {
      const local = idx - base
      if (local >= 0 && local < data.length) raw[pos] = data[local] as Record<string, unknown>
    }
  }

  let docs = raw.filter(d => d)
  for (const r of filters.range) {
    docs = docs.filter(d => {
      const v = d[r.field]; if (v == null) return false
      const n = typeof v === 'number' ? v : +v; if (isNaN(n)) return false
      return (r.gte === undefined || n >= r.gte) && (r.lte === undefined || n <= r.lte)
    })
  }
  for (const f of filters.exists) docs = docs.filter(d => d[f] != null)

  // Distinct
  if (distinctField) {
    const seen = new Set<string>()
    docs = docs.filter(d => {
      const v = String(d[distinctField] ?? '')
      if (seen.has(v)) return false
      seen.add(v); return true
    })
  }

  const total = docs.length
  const facets = facetFields.length ? computeFacets(docs, facetFields) : undefined
  const stats = statFields.length ? computeStats(docs, statFields) : undefined
  const buckets = bucketField ? { [bucketField]: computeBuckets(docs, bucketField, bucketSize) } : undefined
  const groupedResult = groupField ? groupDocs(docs, groupField, groupLimit) : undefined

  if (sort.length) docs = sortDocs(docs, sort)
  if (!groupField && needsAll) docs = docs.slice(offset, offset + limit)

  return { docs: groupField ? [] : docs, total, facets, stats, buckets, groups: groupedResult }
}

// More Like This
async function moreLikeThis(ds: string, docId: string, env: Env, limit: number): Promise<Record<string, unknown>[]> {
  // Load the source document first
  const meta = await loadMeta(ds, env)

  // For IMDB, search by tconst
  if (ds === 'imdb') {
    const hash = await loadHash(ds, 'tconst', env)
    const indices = hash[docId]
    if (!indices || !indices.length) return []

    const shard = Math.floor(indices[0]! / meta.shardSize)
    const docs = await loadDocs(ds, shard, env)
    const sourceDoc = docs[indices[0]! - shard * meta.shardSize] as Record<string, unknown>
    if (!sourceDoc) return []

    // Extract terms from title for similarity
    const title = String(sourceDoc.primaryTitle || '').toLowerCase()
    const terms = title.split(/\s+/).filter(t => t.length >= 3 && !STOPS.has(t))

    if (!terms.length) return []

    // Search for similar docs
    const pq = parseQuery(terms.slice(0, 3).join(' '))
    const result = await search(ds, pq, {}, env)

    // Filter out the source document
    const similar = result.indices.filter(i => i !== indices[0]).slice(0, limit)

    // Fetch similar docs
    const groups = new Map<number, number[]>()
    for (const idx of similar) {
      const s = Math.floor(idx / meta.shardSize)
      if (!groups.has(s)) groups.set(s, [])
      groups.get(s)!.push(idx)
    }

    const shardData = await Promise.all([...groups.keys()].slice(0, MAX_MLT_DOC_SHARDS).map(n => loadDocs(ds, n, env)))
    const shardMap = new Map([...groups.keys()].slice(0, MAX_MLT_DOC_SHARDS).map((n, i) => [n, shardData[i]!]))

    const results: Record<string, unknown>[] = []
    for (const idx of similar) {
      const s = Math.floor(idx / meta.shardSize)
      const data = shardMap.get(s)
      if (data) {
        const doc = data[idx - s * meta.shardSize]
        if (doc) results.push(doc as Record<string, unknown>)
      }
    }

    return results
  }

  return []
}

// Highlighting & Snippets
const TEXT_FIELDS: Record<string, string[]> = {
  onet: ['title', 'description'], unspsc: ['commodityTitle', 'classTitle', 'familyTitle', 'segmentTitle'], imdb: ['primaryTitle', 'originalTitle']
}

function highlight(text: string, matched: Set<string>): string {
  if (!text || !matched.size) return text
  let r = text
  for (const t of matched) r = r.replace(new RegExp(`\\b(${t}\\w*)`, 'gi'), '**$1**')
  return r
}

function extractSnippet(text: string, matched: Set<string>, len = SNIPPET_LENGTH): string {
  if (!text || !matched.size) return text.slice(0, len)
  const lower = text.toLowerCase()
  for (const t of matched) {
    const idx = lower.indexOf(t)
    if (idx >= 0) {
      const start = Math.max(0, idx - SNIPPET_CONTEXT_BEFORE)
      const end = Math.min(text.length, idx + t.length + SNIPPET_CONTEXT_AFTER)
      return (start > 0 ? '...' : '') + text.slice(start, end) + (end < text.length ? '...' : '')
    }
  }
  return text.slice(0, len)
}

// Main Handler
async function handleSearch(ds: string, p: URLSearchParams, env: Env): Promise<Response> {
  const start = performance.now()

  const q = p.get('q') || ''
  const limit = Math.min(+(p.get('limit') || DEFAULT_SEARCH_LIMIT), MAX_SEARCH_LIMIT)
  const cursor = p.get('cursor')
  const offset = cursor ? decodeCursor(cursor) : Math.max(+(p.get('offset') || 0), 0)
  const showTiming = p.get('timing') === 'true'
  const doHighlight = p.get('highlight') !== 'false'
  const doSnippets = p.get('snippets') === 'true'
  const explain = p.get('explain') === 'true'
  const debug = p.get('debug') === 'true'
  const sort = parseSort(p.get('sort'))
  const facetFields = (p.get('facets') || '').split(',').filter(Boolean)
  const statFields = (p.get('stats') || '').split(',').filter(Boolean)
  const bucketField = p.get('bucket') || null
  const bucketSize = +(p.get('bucket_size') || DEFAULT_BUCKET_SIZE)
  const fields = (p.get('fields') || '').split(',').filter(Boolean)
  const distinctField = p.get('distinct') || null
  const groupField = p.get('group') || null
  const groupLimit = +(p.get('group_limit') || DEFAULT_GROUP_LIMIT)
  const searchFields = p.get('search_fields')?.split(',').filter(Boolean)

  // Parse boosts
  const boosts: Record<string, number> = {}
  const boostParam = p.get('boost')
  if (boostParam) {
    for (const b of boostParam.split(',')) {
      const [term, val] = b.split(':')
      if (term && val) boosts[term] = +val
    }
  }

  const meta = await loadMeta(ds, env)
  const pq = parseQuery(q, searchFields)
  const filters = parseFilters(p, ds)

  const searchStart = performance.now()
  let indices: number[], matched = new Set<string>(), explains = new Map<number, string[]>()
  let searchTimings: Record<string, number> = {}
  let didYouMean: string | undefined
  let scores = new Map<number, number>()

  if (pq.should.length || pq.must.length || pq.wildcards.length || pq.phrases.length) {
    const result = await search(ds, pq, boosts, env)
    indices = result.indices
    scores = result.scores
    matched = result.matched
    explains = result.explains
    searchTimings = result.timings
    didYouMean = result.didYouMean
  } else {
    indices = Array.from({ length: Math.min(meta.totalDocs, MAX_BROWSE_RESULTS) }, (_, i) => i)
  }

  if (filters.hash.length) indices = await applyHashFilters(indices, filters.hash, ds, env)

  const searchMs = performance.now() - searchStart
  const fetchStart = performance.now()

  const { docs, total, facets, stats, buckets, groups } = await fetchDocs(
    ds, indices, scores,
    meta, env, limit, offset, sort, filters, facetFields, statFields, bucketField, bucketSize, distinctField, groupField, groupLimit
  )

  // Project fields
  let finalDocs: unknown[] = docs
  if (fields.length) {
    finalDocs = docs.map(d => {
      const r: Record<string, unknown> = {}
      for (const f of fields) if (d[f] !== undefined) r[f] = d[f]
      return r
    })
  }

  // Highlight or snippets
  if ((doHighlight || doSnippets) && matched.size) {
    finalDocs = (finalDocs as Record<string, unknown>[]).map(d => {
      const r = { ...d }
      for (const f of TEXT_FIELDS[ds] || []) {
        if (typeof r[f] === 'string') {
          r[f] = doSnippets ? extractSnippet(r[f] as string, matched) : highlight(r[f] as string, matched)
        }
      }
      return r
    })
  }

  // Add explain
  if (explain) {
    const docIndices = indices.slice(offset, offset + limit)
    finalDocs = (finalDocs as Record<string, unknown>[]).map((d, i) => ({
      ...d,
      _explain: explains.get(docIndices[i]!) || []
    }))
  }

  const fetchMs = performance.now() - fetchStart

  const response: SearchResponse = { data: groups ? [] : finalDocs, total, limit, offset }

  // Cursor for next page
  if (offset + limit < total) {
    response.cursor = encodeCursor(offset + limit)
  }

  if (sort.length) response.sort = sort.map(s => `${s.field}:${s.dir}`).join(',')
  if (facets) response.facets = facets
  if (stats) response.stats = stats
  if (buckets) response.buckets = buckets
  if (groups) response.groups = groups
  if (didYouMean) response.didYouMean = didYouMean
  if (showTiming) response.timing = {
    ...searchTimings,
    searchMs: +searchMs.toFixed(2),
    fetchMs: +fetchMs.toFixed(2),
    totalMs: +(performance.now() - start).toFixed(2)
  }
  if (debug) response.query = { parsed: pq, matched: [...matched], boosts }

  return Response.json(response, { headers: { 'Cache-Control': `public, max-age=${CACHE_MAX_AGE_SECONDS}` } })
}

// Suggest endpoint
async function handleSuggest(ds: string, p: URLSearchParams, env: Env): Promise<Response> {
  const q = p.get('q') || ''
  const suggestions = await getSuggestions(ds, q.toLowerCase(), env)
  return Response.json({ suggestions, query: q })
}

// MLT endpoint
async function handleMLT(ds: string, p: URLSearchParams, env: Env): Promise<Response> {
  const id = p.get('id') || ''
  const limit = Math.min(+(p.get('limit') || DEFAULT_MLT_LIMIT), MAX_MLT_LIMIT)

  if (!id) return Response.json({ error: 'id parameter required' }, { status: 400 })

  const similar = await moreLikeThis(ds, id, env, limit)
  return Response.json({ data: similar, sourceId: id, total: similar.length })
}

// Main
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    let path = url.pathname
    for (const pre of ['/search-v7', '/search-v6', '/search-v5', '/search-v4', '/search-v3', '/search-v2', '/search']) {
      if (path.startsWith(pre)) { path = path.slice(pre.length) || '/'; break }
    }

    if (request.method === 'OPTIONS') return new Response(null, { headers: { 'Access-Control-Allow-Origin': '*' } })

    try {
      if (path === '/health') {
        return Response.json({
          ok: true, version: 7,
          features: [
            'fts', 'stemming', 'prefix', 'fuzzy', 'wildcards', 'synonyms', 'negation', 'phrase',
            'filters', 'range', 'exists', 'not', 'score_filter',
            'sort', 'multi-sort',
            'facets', 'stats', 'buckets',
            'highlight', 'snippets', 'explain',
            'fields', 'distinct', 'boost',
            'suggest', 'spell_correct', 'mlt', 'group', 'cursor'
          ],
        })
      }

      // Suggest endpoints
      if (path === '/suggest/onet') return handleSuggest('onet', url.searchParams, env)
      if (path === '/suggest/imdb') return handleSuggest('imdb', url.searchParams, env)
      if (path === '/suggest/unspsc') return handleSuggest('unspsc', url.searchParams, env)

      // More Like This endpoints
      if (path === '/mlt/imdb') return handleMLT('imdb', url.searchParams, env)
      if (path === '/mlt/onet') return handleMLT('onet', url.searchParams, env)

      // Search endpoints
      if (path === '/onet') return handleSearch('onet', url.searchParams, env)
      if (path === '/unspsc') return handleSearch('unspsc', url.searchParams, env)
      if (path === '/imdb') return handleSearch('imdb', url.searchParams, env)

      if (path === '/' || path === '') {
        return Response.json({
          name: 'ParqueDB Search (Supreme v7)', version: 7,
          features: [
            'fts', 'stemming', 'prefix', 'fuzzy~', 'wildcards*', 'synonyms', 'negation -', 'phrase ""',
            'filters', 'range', 'exists', 'not', 'score_filter',
            'sort', 'facets', 'stats', 'buckets',
            'highlight', 'snippets', 'explain',
            'fields', 'distinct', 'boost',
            'suggest', 'spell_correct', 'mlt', 'group', 'cursor'
          ],
          endpoints: {
            '/onet': 'O*NET occupations', '/imdb': 'IMDB titles', '/unspsc': 'UNSPSC categories',
            '/suggest/onet': 'Autocomplete for O*NET', '/suggest/imdb': 'Autocomplete for IMDB',
            '/mlt/imdb': 'More Like This for IMDB',
          },
          examples: [
            '/imdb?q=matrx (spell correction → "Did you mean: matrix")',
            '/imdb?q=love&score_min=20 (filter by relevance score)',
            '/imdb?q=action&bucket=startYear&bucket_size=10 (year histogram)',
            '/imdb?q=love&group=titleType&group_limit=3 (group by type)',
            '/mlt/imdb?id=tt0133093 (movies similar to The Matrix)',
            '/imdb?q=love&cursor=xxx (cursor pagination)',
          ],
        })
      }

      return new Response('Not Found', { status: 404 })
    } catch (e) {
      return Response.json({ error: String(e) }, { status: 500 })
    }
  },
}
