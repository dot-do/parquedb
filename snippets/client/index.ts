/**
 * ParqueDB Search Client
 *
 * Tiny, typesafe, tree-shakable fetch-based client for the search API.
 *
 * @example
 * ```ts
 * import { createSearchClient } from '@parquedb/search-client'
 *
 * const search = createSearchClient({ baseUrl: 'https://cdn.workers.do/search-v10' })
 *
 * // Full-text search
 * const results = await search.query('imdb', { q: 'matrix', type: 'movie' })
 *
 * // Vector search
 * const similar = await search.vector('imdb', { vector: embedding, limit: 10 })
 *
 * // Hybrid search
 * const hybrid = await search.hybrid('imdb', { q: 'love', vector: embedding })
 *
 * // Autocomplete
 * const suggestions = await search.suggest('imdb', 'mat')
 * ```
 */

// =============================================================================
// Types
// =============================================================================

/** Supported datasets */
export type Dataset = 'imdb' | 'onet' | 'unspsc'

/** Search mode */
export type SearchMode = 'fts' | 'vector' | 'hybrid'

/** Vector similarity metric */
export type VectorMetric = 'cosine' | 'dot'

/** Sort direction */
export type SortDir = 'asc' | 'desc'

/** Sort specification */
export type SortSpec = `${string}:${SortDir}` | string

/** Base search parameters */
export interface BaseSearchParams {
  /** Maximum results to return (default: 20, max: 50) */
  limit?: number
  /** Offset for pagination */
  offset?: number
  /** Sort specification: "field:asc" or "field:desc" */
  sort?: SortSpec
  /** Fields to compute facets for */
  facets?: string[]
  /** Fields to compute stats for */
  stats?: string[]
  /** Include timing breakdown */
  timing?: boolean
}

/** Full-text search parameters */
export interface FTSParams extends BaseSearchParams {
  /** Search query */
  q: string
  /** Enable highlighting (default: true) */
  highlight?: boolean
}

/** Filter parameters for IMDB */
export interface IMDBFilters {
  /** Filter by title type: movie, tvSeries, short, etc. */
  type?: string | string[]
  /** Minimum year */
  year_gte?: number
  /** Maximum year */
  year_lte?: number
}

/** Filter parameters for O*NET */
export interface ONETFilters {
  /** Filter by occupation code */
  code?: string | string[]
}

/** Combined search parameters */
export interface SearchParams extends BaseSearchParams, Partial<IMDBFilters>, Partial<ONETFilters> {
  /** Search query (optional for browse mode) */
  q?: string
}

/** Vector search parameters */
export interface VectorParams extends BaseSearchParams {
  /** Query vector (array of numbers) */
  vector: number[]
  /** Similarity metric (default: cosine) */
  metric?: VectorMetric
}

/** Hybrid search parameters */
export interface HybridParams extends BaseSearchParams {
  /** Search query */
  q: string
  /** Query vector */
  vector: number[]
  /** Weight for FTS vs vector (0-1, default: 0.5) */
  fts_weight?: number
  /** Similarity metric */
  metric?: VectorMetric
}

/** Facet result */
export interface Facet {
  value: string
  count: number
}

/** Stats result */
export interface Stats {
  min: number
  max: number
  avg: number
  sum?: number
  count?: number
  variance?: number
}

/** Timing breakdown */
export interface Timing {
  indexIntersectMs?: number
  ftsMs?: number
  vectorMs?: number
  fusionMs?: number
  fetchMs?: number
  totalMs?: number
}

/** Search response */
export interface SearchResponse<T = unknown> {
  data: T[]
  total: number
  limit: number
  offset: number
  mode?: SearchMode
  facets?: Record<string, Facet[]>
  stats?: Record<string, Stats>
  didYouMean?: string
  timing?: Timing
}

/** Suggest response */
export interface SuggestResponse {
  suggestions: string[]
  query: string
}

/** Health response */
export interface HealthResponse {
  ok: boolean
  version: number
  features: string[]
}

/** Client configuration */
export interface SearchClientConfig {
  /** Base URL of the search API */
  baseUrl: string
  /** Default request options */
  fetchOptions?: RequestInit
  /** Transform response before returning */
  transform?: <T>(response: SearchResponse<T>) => SearchResponse<T>
}

// =============================================================================
// IMDB Types
// =============================================================================

export interface IMDBTitle {
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

// =============================================================================
// O*NET Types
// =============================================================================

export interface ONETOccupation {
  code: string
  title: string
  description: string
}

// =============================================================================
// Client Implementation
// =============================================================================

function buildUrl(base: string, path: string, params: Record<string, unknown>): string {
  const url = new URL(path, base)

  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null) continue

    if (Array.isArray(value)) {
      url.searchParams.set(key, value.join(','))
    } else if (typeof value === 'object') {
      url.searchParams.set(key, JSON.stringify(value))
    } else {
      url.searchParams.set(key, String(value))
    }
  }

  return url.toString()
}

async function fetchJSON<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(url, options)

  if (!response.ok) {
    const error = await response.text().catch(() => 'Unknown error')
    throw new Error(`Search API error: ${response.status} - ${error}`)
  }

  return response.json() as Promise<T>
}

/** Create a search client instance */
export function createSearchClient(config: SearchClientConfig) {
  const { baseUrl, fetchOptions, transform } = config

  async function request<T>(
    path: string,
    params: Record<string, unknown>
  ): Promise<SearchResponse<T>> {
    const url = buildUrl(baseUrl, path, params)
    const response = await fetchJSON<SearchResponse<T>>(url, fetchOptions)
    return transform ? transform(response) : response
  }

  return {
    /**
     * Full-text search with optional filters
     *
     * @example
     * ```ts
     * const results = await client.query('imdb', {
     *   q: 'matrix',
     *   type: 'movie',
     *   year_gte: 2000,
     *   limit: 10
     * })
     * ```
     */
    async query<T = unknown>(
      dataset: Dataset,
      params: SearchParams
    ): Promise<SearchResponse<T>> {
      return request<T>(`/${dataset}`, { ...params, mode: 'fts' })
    },

    /**
     * Vector similarity search
     *
     * @example
     * ```ts
     * const results = await client.vector('imdb', {
     *   vector: [0.1, 0.2, ...],
     *   metric: 'cosine',
     *   limit: 10
     * })
     * ```
     */
    async vector<T = unknown>(
      dataset: Dataset,
      params: VectorParams
    ): Promise<SearchResponse<T>> {
      return request<T>(`/${dataset}`, {
        ...params,
        vector: params.vector,
        mode: 'vector'
      })
    },

    /**
     * Hybrid search combining FTS and vector similarity
     *
     * @example
     * ```ts
     * const results = await client.hybrid('imdb', {
     *   q: 'love story',
     *   vector: embedding,
     *   fts_weight: 0.7
     * })
     * ```
     */
    async hybrid<T = unknown>(
      dataset: Dataset,
      params: HybridParams
    ): Promise<SearchResponse<T>> {
      return request<T>(`/${dataset}`, {
        ...params,
        vector: params.vector,
        mode: 'hybrid'
      })
    },

    /**
     * Autocomplete suggestions
     *
     * @example
     * ```ts
     * const { suggestions } = await client.suggest('imdb', 'mat')
     * // ['matrix', 'matter', 'matthew', ...]
     * ```
     */
    async suggest(dataset: Dataset, prefix: string): Promise<SuggestResponse> {
      const url = buildUrl(baseUrl, `/suggest/${dataset}`, { q: prefix })
      return fetchJSON<SuggestResponse>(url, fetchOptions)
    },

    /**
     * Health check
     */
    async health(): Promise<HealthResponse> {
      const url = buildUrl(baseUrl, '/health', {})
      return fetchJSON<HealthResponse>(url, fetchOptions)
    },

    /**
     * Raw request for advanced use cases
     */
    async raw<T = unknown>(
      path: string,
      params: Record<string, unknown>
    ): Promise<SearchResponse<T>> {
      return request<T>(path, params)
    }
  }
}

// =============================================================================
// Typed Client Factories
// =============================================================================

/** Create a typed client for IMDB searches */
export function createIMDBClient(baseUrl: string, fetchOptions?: RequestInit) {
  const client = createSearchClient({ baseUrl, fetchOptions })

  return {
    async search(params: SearchParams & IMDBFilters) {
      return client.query<IMDBTitle>('imdb', params)
    },

    async vector(params: VectorParams) {
      return client.vector<IMDBTitle>('imdb', params)
    },

    async hybrid(params: HybridParams) {
      return client.hybrid<IMDBTitle>('imdb', params)
    },

    async suggest(prefix: string) {
      return client.suggest('imdb', prefix)
    }
  }
}

/** Create a typed client for O*NET searches */
export function createONETClient(baseUrl: string, fetchOptions?: RequestInit) {
  const client = createSearchClient({ baseUrl, fetchOptions })

  return {
    async search(params: SearchParams & ONETFilters) {
      return client.query<ONETOccupation>('onet', params)
    },

    async vector(params: VectorParams) {
      return client.vector<ONETOccupation>('onet', params)
    },

    async suggest(prefix: string) {
      return client.suggest('onet', prefix)
    }
  }
}

// =============================================================================
// Standalone Functions (Tree-Shakable)
// =============================================================================

const DEFAULT_BASE_URL = 'https://cdn.workers.do/search-v10'

/**
 * One-shot search function (tree-shakable)
 *
 * @example
 * ```ts
 * import { search } from '@parquedb/search-client'
 *
 * const results = await search('imdb', { q: 'matrix' })
 * ```
 */
export async function search<T = unknown>(
  dataset: Dataset,
  params: SearchParams,
  baseUrl = DEFAULT_BASE_URL
): Promise<SearchResponse<T>> {
  const url = buildUrl(baseUrl, `/${dataset}`, params)
  return fetchJSON<SearchResponse<T>>(url)
}

/**
 * One-shot vector search (tree-shakable)
 */
export async function vectorSearch<T = unknown>(
  dataset: Dataset,
  params: VectorParams,
  baseUrl = DEFAULT_BASE_URL
): Promise<SearchResponse<T>> {
  const url = buildUrl(baseUrl, `/${dataset}`, { ...params, mode: 'vector' })
  return fetchJSON<SearchResponse<T>>(url)
}

/**
 * One-shot hybrid search (tree-shakable)
 */
export async function hybridSearch<T = unknown>(
  dataset: Dataset,
  params: HybridParams,
  baseUrl = DEFAULT_BASE_URL
): Promise<SearchResponse<T>> {
  const url = buildUrl(baseUrl, `/${dataset}`, { ...params, mode: 'hybrid' })
  return fetchJSON<SearchResponse<T>>(url)
}

/**
 * One-shot suggest (tree-shakable)
 */
export async function suggest(
  dataset: Dataset,
  prefix: string,
  baseUrl = DEFAULT_BASE_URL
): Promise<SuggestResponse> {
  const url = buildUrl(baseUrl, `/suggest/${dataset}`, { q: prefix })
  return fetchJSON<SuggestResponse>(url)
}

// =============================================================================
// React Hook (Optional - Tree-Shakable)
// =============================================================================

export interface UseSearchOptions<T> {
  /** Initial data */
  initialData?: SearchResponse<T>
  /** Skip initial fetch */
  skip?: boolean
  /** Refetch interval in ms */
  refetchInterval?: number
}

/**
 * React hook for search (requires React)
 *
 * @example
 * ```tsx
 * import { useSearch } from '@parquedb/search-client'
 *
 * function MovieSearch() {
 *   const [query, setQuery] = useState('')
 *   const { data, loading, error } = useSearch('imdb', { q: query })
 *
 *   return (
 *     <div>
 *       <input value={query} onChange={e => setQuery(e.target.value)} />
 *       {loading && <div>Loading...</div>}
 *       {data?.data.map(movie => <div key={movie.tconst}>{movie.primaryTitle}</div>)}
 *     </div>
 *   )
 * }
 * ```
 */
export function useSearch<T = unknown>(
  dataset: Dataset,
  params: SearchParams,
  options: UseSearchOptions<T> = {}
) {
  // This is a placeholder - actual implementation would use React hooks
  // Keeping it here for the type signature
  throw new Error(
    'useSearch requires React. Import from @parquedb/search-client/react instead.'
  )
}

// =============================================================================
// Export Types
// =============================================================================

export type SearchClient = ReturnType<typeof createSearchClient>
export type IMDBClient = ReturnType<typeof createIMDBClient>
export type ONETClient = ReturnType<typeof createONETClient>
