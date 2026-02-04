/**
 * ParqueDB Search Client
 *
 * Tiny, typesafe, tree-shakable, proxy-based search client.
 *
 * @example
 * ```ts
 * import { search } from 'parquedb'
 *
 * // Proxy-based API
 * const movies = await search.imdb('matrix', { type: 'movie' })
 * const jobs = await search.onet('engineer')
 *
 * // With filters
 * const recent = await search.imdb('love', { year_gte: 2020 })
 *
 * // Autocomplete
 * const suggestions = await search.imdb.suggest('mat')
 *
 * // Vector search
 * const similar = await search.imdb.vector(embedding)
 *
 * // Hybrid
 * const hybrid = await search.imdb.hybrid('love', embedding, { fts_weight: 0.7 })
 * ```
 */

// =============================================================================
// Core Types
// =============================================================================

/** Search response */
export interface SearchResult<T> {
  data: T[]
  total: number
  limit: number
  offset: number
  facets?: Record<string, { value: string; count: number }[]> | undefined
  stats?: Record<string, { min: number; max: number; avg: number }> | undefined
  timing?: Record<string, number> | undefined
  didYouMean?: string | undefined
}

/** Suggest response */
export interface SuggestResult {
  suggestions: string[]
  query: string
}

/** Base options for all searches */
export interface SearchOptions {
  limit?: number | undefined
  offset?: number | undefined
  sort?: string | undefined
  facets?: string[] | undefined
  stats?: string[] | undefined
  timing?: boolean | undefined
  highlight?: boolean | undefined
}

// =============================================================================
// Schema Definition Types
// =============================================================================

/** Define a searchable field */
export interface SearchField<_T = unknown> {
  type: 'text' | 'keyword' | 'number' | 'date' | 'boolean' | 'array'
  filterable?: boolean | undefined
  facetable?: boolean | undefined
  sortable?: boolean | undefined
}

/** Schema for a dataset */
export type DatasetSchema = Record<string, SearchField>

/** Extract document type from schema */
export type DocType<S extends DatasetSchema> = {
  [K in keyof S]: S[K]['type'] extends 'text' | 'keyword' ? string
    : S[K]['type'] extends 'number' ? number
    : S[K]['type'] extends 'date' ? string | Date
    : S[K]['type'] extends 'boolean' ? boolean
    : S[K]['type'] extends 'array' ? string[]
    : unknown
}

/** Extract filterable fields from schema */
export type FilterableFields<S extends DatasetSchema> = {
  [K in keyof S as S[K]['filterable'] extends true ? K : never]?:
    S[K]['type'] extends 'text' | 'keyword' ? string | string[]
    : S[K]['type'] extends 'number' ? number | { gte?: number | undefined; lte?: number | undefined }
    : S[K]['type'] extends 'boolean' ? boolean
    : never
}

// =============================================================================
// Built-in Schemas
// =============================================================================

export const IMDBSchema = {
  tconst: { type: 'keyword' as const, filterable: true },
  titleType: { type: 'keyword' as const, filterable: true, facetable: true },
  primaryTitle: { type: 'text' as const },
  originalTitle: { type: 'text' as const },
  isAdult: { type: 'boolean' as const, filterable: true },
  startYear: { type: 'number' as const, filterable: true, sortable: true, facetable: true },
  endYear: { type: 'number' as const, filterable: true },
  runtimeMinutes: { type: 'number' as const, filterable: true, sortable: true },
  genres: { type: 'array' as const, facetable: true },
} satisfies DatasetSchema

export const ONETSchema = {
  code: { type: 'keyword' as const, filterable: true },
  title: { type: 'text' as const },
  description: { type: 'text' as const },
} satisfies DatasetSchema

export const UNSPSCSchema = {
  commodityCode: { type: 'keyword' as const, filterable: true },
  commodityTitle: { type: 'text' as const },
  classCode: { type: 'keyword' as const, filterable: true },
  classTitle: { type: 'text' as const },
  familyCode: { type: 'keyword' as const, filterable: true },
  familyTitle: { type: 'text' as const },
  segmentCode: { type: 'keyword' as const, filterable: true },
  segmentTitle: { type: 'text' as const },
} satisfies DatasetSchema

// =============================================================================
// Document Types
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

export interface ONETOccupation {
  code: string
  title: string
  description: string
}

export interface UNSPSCCode {
  commodityCode: string
  commodityTitle: string
  classCode: string
  classTitle: string
  familyCode: string
  familyTitle: string
  segmentCode: string
  segmentTitle: string
}

// =============================================================================
// Filter Types
// =============================================================================

export interface IMDBFilters {
  type?: string | string[] | undefined
  year_gte?: number | undefined
  year_lte?: number | undefined
  runtime_gte?: number | undefined
  runtime_lte?: number | undefined
}

export interface ONETFilters {
  code?: string | string[] | undefined
}

export interface UNSPSCFilters {
  segment?: string | string[] | undefined
  family?: string | string[] | undefined
  class?: string | string[] | undefined
  commodity?: string | string[] | undefined
}

// =============================================================================
// Dataset Client Interface
// =============================================================================

export interface DatasetClient<T, F = Record<string, unknown>> {
  /** Full-text search */
  (query: string, filters?: F & SearchOptions): Promise<SearchResult<T>>

  /** Browse (no query) */
  (filters?: F & SearchOptions): Promise<SearchResult<T>>

  /** Autocomplete */
  suggest(prefix: string): Promise<SuggestResult>

  /** Vector search */
  vector(embedding: number[], options?: SearchOptions): Promise<SearchResult<T>>

  /** Hybrid search */
  hybrid(query: string, embedding: number[], options?: F & SearchOptions & { fts_weight?: number | undefined }): Promise<SearchResult<T>>
}

// =============================================================================
// Internal Implementation
// =============================================================================

const DEFAULT_BASE_URL = 'https://cdn.workers.do/search-v10'

function buildUrl(base: string, path: string, params: Record<string, unknown>): string {
  const url = new URL(path, base.endsWith('/') ? base : base + '/')

  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null) continue
    if (Array.isArray(value)) {
      url.searchParams.set(key, value.join(','))
    } else if (typeof value === 'object') {
      // Handle range filters like { gte: 2000, lte: 2020 }
      for (const [subKey, subVal] of Object.entries(value)) {
        if (subVal !== undefined) {
          url.searchParams.set(`${key}_${subKey}`, String(subVal))
        }
      }
    } else {
      url.searchParams.set(key, String(value))
    }
  }

  return url.toString()
}

async function fetchJSON<T>(url: string): Promise<T> {
  const res = await fetch(url)
  if (!res.ok) throw new Error(`Search error: ${res.status}`)
  return res.json() as Promise<T>
}

function createDatasetClient<T, F>(
  baseUrl: string,
  dataset: string
): DatasetClient<T, F> {
  const client = async (
    queryOrFilters?: string | (F & SearchOptions),
    maybeFilters?: F & SearchOptions
  ): Promise<SearchResult<T>> => {
    const isQuery = typeof queryOrFilters === 'string'
    const query = isQuery ? queryOrFilters : undefined
    const filters = isQuery ? maybeFilters : queryOrFilters

    const params: Record<string, unknown> = { ...filters }
    if (query) params.q = query

    const url = buildUrl(baseUrl, dataset, params)
    return fetchJSON<SearchResult<T>>(url)
  }

  client.suggest = async (prefix: string): Promise<SuggestResult> => {
    const url = buildUrl(baseUrl, `suggest/${dataset}`, { q: prefix })
    return fetchJSON<SuggestResult>(url)
  }

  client.vector = async (
    embedding: number[],
    options?: SearchOptions
  ): Promise<SearchResult<T>> => {
    const url = buildUrl(baseUrl, dataset, {
      ...options,
      vector: embedding,
      mode: 'vector'
    })
    return fetchJSON<SearchResult<T>>(url)
  }

  client.hybrid = async (
    query: string,
    embedding: number[],
    options?: F & SearchOptions & { fts_weight?: number | undefined }
  ): Promise<SearchResult<T>> => {
    const url = buildUrl(baseUrl, dataset, {
      ...options,
      q: query,
      vector: embedding,
      mode: 'hybrid'
    })
    return fetchJSON<SearchResult<T>>(url)
  }

  return client as DatasetClient<T, F>
}

// =============================================================================
// Search Client Types
// =============================================================================

export interface SearchClientConfig {
  baseUrl?: string | undefined
}

export interface BuiltinSearchClient {
  imdb: DatasetClient<IMDBTitle, IMDBFilters>
  onet: DatasetClient<ONETOccupation, ONETFilters>
  unspsc: DatasetClient<UNSPSCCode, UNSPSCFilters>
}

// =============================================================================
// Create Search Client (Generic)
// =============================================================================

/**
 * Create a custom search client for your own datasets
 *
 * @example
 * ```ts
 * // Define your types
 * interface Product {
 *   id: string
 *   name: string
 *   price: number
 *   category: string
 * }
 *
 * interface ProductFilters {
 *   category?: string
 *   price_gte?: number
 *   price_lte?: number
 * }
 *
 * // Create typed client
 * const search = createSearchClient<{
 *   products: [Product, ProductFilters]
 *   categories: [Category, CategoryFilters]
 * }>({ baseUrl: 'https://api.example.com/search' })
 *
 * // Use with full type safety
 * const { data } = await search.products('laptop', { category: 'electronics' })
 * ```
 */
export function createSearchClient<
  Datasets extends Record<string, [doc: unknown, filters: Record<string, unknown>]>
>(config: SearchClientConfig = {}): {
  [K in keyof Datasets]: DatasetClient<Datasets[K][0], Datasets[K][1]>
} {
  const baseUrl = config.baseUrl ?? DEFAULT_BASE_URL

  return new Proxy({} as Record<string, never>, {
    get(_, dataset: string) {
      return createDatasetClient(baseUrl, dataset)
    }
  }) as { [K in keyof Datasets]: DatasetClient<Datasets[K][0], Datasets[K][1]> }
}

// =============================================================================
// Default Search Client (Proxy-based)
// =============================================================================

/**
 * Default search client with built-in datasets
 *
 * @example
 * ```ts
 * import { search } from 'parquedb'
 *
 * // Simple search
 * const { data } = await search.imdb('matrix')
 *
 * // With filters
 * const movies = await search.imdb('love', { type: 'movie', year_gte: 2000 })
 *
 * // Autocomplete
 * const { suggestions } = await search.imdb.suggest('mat')
 *
 * // Vector search
 * const similar = await search.imdb.vector(embedding)
 * ```
 */
export const search: BuiltinSearchClient = createSearchClient<{
  imdb: [IMDBTitle, IMDBFilters]
  onet: [ONETOccupation, ONETFilters]
  unspsc: [UNSPSCCode, UNSPSCFilters]
}>()

// =============================================================================
// Standalone Functions (Tree-Shakable)
// =============================================================================

/**
 * One-shot search (tree-shakable)
 */
export async function query<T = unknown>(
  dataset: string,
  q: string,
  filters?: Record<string, unknown> & SearchOptions,
  baseUrl = DEFAULT_BASE_URL
): Promise<SearchResult<T>> {
  const url = buildUrl(baseUrl, dataset, { ...filters, q })
  return fetchJSON<SearchResult<T>>(url)
}

/**
 * One-shot suggest (tree-shakable)
 */
export async function suggest(
  dataset: string,
  prefix: string,
  baseUrl = DEFAULT_BASE_URL
): Promise<SuggestResult> {
  const url = buildUrl(baseUrl, `suggest/${dataset}`, { q: prefix })
  return fetchJSON<SuggestResult>(url)
}

/**
 * One-shot vector search (tree-shakable)
 */
export async function vectorSearch<T = unknown>(
  dataset: string,
  embedding: number[],
  options?: SearchOptions,
  baseUrl = DEFAULT_BASE_URL
): Promise<SearchResult<T>> {
  const url = buildUrl(baseUrl, dataset, { ...options, vector: embedding, mode: 'vector' })
  return fetchJSON<SearchResult<T>>(url)
}

/**
 * One-shot hybrid search (tree-shakable)
 */
export async function hybridSearch<T = unknown>(
  dataset: string,
  q: string,
  embedding: number[],
  options?: SearchOptions & { fts_weight?: number | undefined },
  baseUrl = DEFAULT_BASE_URL
): Promise<SearchResult<T>> {
  const url = buildUrl(baseUrl, dataset, { ...options, q, vector: embedding, mode: 'hybrid' })
  return fetchJSON<SearchResult<T>>(url)
}

// =============================================================================
// Type Exports
// =============================================================================

export type { DatasetClient, DatasetSchema, DocType, FilterableFields }
