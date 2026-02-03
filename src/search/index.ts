/**
 * ParqueDB Search - Proxy-based typesafe search client
 *
 * @example
 * ```ts
 * import { search } from 'parquedb'
 *
 * // Proxy API
 * const { data } = await search.imdb('matrix', { type: 'movie' })
 *
 * // Autocomplete
 * const { suggestions } = await search.imdb.suggest('mat')
 *
 * // Custom datasets
 * const mySearch = createSearchClient<{
 *   products: [Product, ProductFilters]
 * }>({ baseUrl: 'https://api.example.com' })
 * ```
 */

export {
  // Default client (proxy-based)
  search,

  // Client factory for custom datasets
  createSearchClient,

  // Standalone functions (tree-shakable)
  query,
  suggest,
  vectorSearch,
  hybridSearch,

  // Built-in schemas
  IMDBSchema,
  ONETSchema,
  UNSPSCSchema,

  // Types
  type SearchResult,
  type SuggestResult,
  type SearchOptions,
  type SearchClientConfig,
  type DatasetClient,
  type DatasetSchema,
  type SearchField,
  type DocType,
  type FilterableFields,

  // Domain types
  type IMDBTitle,
  type IMDBFilters,
  type ONETOccupation,
  type ONETFilters,
  type UNSPSCCode,
  type UNSPSCFilters,
  type BuiltinSearchClient,
} from './client.js'
