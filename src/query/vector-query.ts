/**
 * Vector Query Processing for ParqueDB
 *
 * Handles text-to-embedding conversion for vector similarity queries
 * and normalization of vector filter operators.
 */

import type { Filter } from '../types/filter'
import type { EmbeddingProvider } from '../embeddings/provider'
import { DEFAULT_VECTOR_TOP_K } from '../constants'

// =============================================================================
// Types
// =============================================================================

/**
 * Normalized vector query with embedding vector ready for search
 */
export interface NormalizedVectorQuery {
  /** The query vector */
  vector: number[]
  /** Field to search */
  field: string
  /** Number of results to return */
  topK: number
  /** Minimum score threshold */
  minScore?: number
  /** HNSW efSearch parameter */
  efSearch?: number
}

/**
 * Result of vector filter normalization
 */
export interface VectorFilterNormalizationResult {
  /** The normalized filter with vector ready for search */
  filter: Filter
  /** Whether text was converted to embedding */
  textEmbedded: boolean
  /** The normalized vector query parameters */
  vectorQuery?: NormalizedVectorQuery
}

// =============================================================================
// Vector Filter Extraction
// =============================================================================

/**
 * Extract vector query parameters from a filter
 *
 * Handles both legacy ($near/$k/$field) and new (query/field/topK) formats.
 *
 * @param filter - Filter containing $vector operator
 * @returns Extracted vector query parameters or null if no $vector
 */
export function extractVectorQuery(filter: Filter): {
  query: number[] | string
  field: string
  topK: number
  minScore?: number
  efSearch?: number
} | null {
  if (!filter.$vector) {
    return null
  }

  const vq = filter.$vector

  // Handle new format (query/field/topK)
  if ('query' in vq && vq.query !== undefined) {
    return {
      query: vq.query,
      field: vq.field,
      topK: vq.topK,
      minScore: vq.minScore,
      efSearch: vq.efSearch,
    }
  }

  // Handle legacy format ($near/$k/$field)
  if ('$near' in vq && vq.$near !== undefined) {
    return {
      query: vq.$near,
      field: vq.$field ?? 'embedding',
      topK: vq.$k ?? DEFAULT_VECTOR_TOP_K,
      minScore: vq.$minScore,
      efSearch: vq.efSearch,
    }
  }

  return null
}

/**
 * Check if a vector query contains text that needs embedding
 */
export function isTextVectorQuery(filter: Filter): boolean {
  const vq = extractVectorQuery(filter)
  return vq !== null && typeof vq.query === 'string'
}

// =============================================================================
// Vector Filter Normalization
// =============================================================================

/**
 * Normalize a vector filter, converting text to embedding if needed
 *
 * If the query is a string and an embedding provider is available,
 * the text is converted to an embedding vector.
 *
 * @param filter - Filter to normalize
 * @param embeddingProvider - Optional embedding provider for text conversion
 * @returns Normalized filter with vector ready for search
 *
 * @example
 * ```typescript
 * // Text query with provider
 * const result = await normalizeVectorFilter(
 *   { $vector: { query: 'machine learning', field: 'embedding', topK: 10 } },
 *   myProvider
 * )
 * // result.filter.$vector.query is now a number[]
 *
 * // Already a vector
 * const result = await normalizeVectorFilter(
 *   { $vector: { query: [0.1, 0.2, ...], field: 'embedding', topK: 10 } }
 * )
 * // result.filter unchanged
 * ```
 */
export async function normalizeVectorFilter(
  filter: Filter,
  embeddingProvider?: EmbeddingProvider
): Promise<VectorFilterNormalizationResult> {
  const vq = extractVectorQuery(filter)

  // No vector query in filter
  if (!vq) {
    return { filter, textEmbedded: false }
  }

  // Already a vector
  if (Array.isArray(vq.query)) {
    return {
      filter,
      textEmbedded: false,
      vectorQuery: {
        vector: vq.query,
        field: vq.field,
        topK: vq.topK,
        minScore: vq.minScore,
        efSearch: vq.efSearch,
      },
    }
  }

  // Text query but no embedding provider
  if (!embeddingProvider) {
    throw new Error(
      'Vector query contains text but no embedding provider is configured. ' +
      'Either pass a number[] vector directly, or configure an embedding provider on the database.'
    )
  }

  // Convert text to embedding
  const text = vq.query
  const vector = await embeddingProvider.embed(text, { isQuery: true })

  // Create normalized filter with embedded vector
  // We need to preserve all other filter properties
  const normalizedFilter: Filter = {
    ...filter,
    $vector: {
      query: vector,
      field: vq.field,
      topK: vq.topK,
      minScore: vq.minScore,
      efSearch: vq.efSearch,
      // Preserve legacy format for backward compatibility with query executor
      $near: vector,
      $k: vq.topK,
      $field: vq.field,
      $minScore: vq.minScore,
    },
  }

  return {
    filter: normalizedFilter,
    textEmbedded: true,
    vectorQuery: {
      vector,
      field: vq.field,
      topK: vq.topK,
      minScore: vq.minScore,
      efSearch: vq.efSearch,
    },
  }
}

/**
 * Batch normalize multiple filters with text vector queries
 *
 * More efficient than calling normalizeVectorFilter multiple times
 * as it batches the embedding calls.
 *
 * @param filters - Array of filters to normalize
 * @param embeddingProvider - Embedding provider for text conversion
 * @returns Array of normalized filters
 */
export async function normalizeVectorFilterBatch(
  filters: Filter[],
  embeddingProvider?: EmbeddingProvider
): Promise<VectorFilterNormalizationResult[]> {
  // Collect all text queries
  const textQueries: Array<{ index: number; text: string; vq: ReturnType<typeof extractVectorQuery> }> = []

  for (let i = 0; i < filters.length; i++) {
    const filter = filters[i]!
    const vq = extractVectorQuery(filter)
    if (vq && typeof vq.query === 'string') {
      textQueries.push({ index: i, text: vq.query, vq })
    }
  }

  // If no text queries, return results without API call
  if (textQueries.length === 0) {
    return filters.map(filter => {
      const vq = extractVectorQuery(filter)
      if (!vq) {
        return { filter, textEmbedded: false }
      }
      return {
        filter,
        textEmbedded: false,
        vectorQuery: {
          vector: vq.query as number[],
          field: vq.field,
          topK: vq.topK,
          minScore: vq.minScore,
          efSearch: vq.efSearch,
        },
      }
    })
  }

  // Require provider if there are text queries
  if (!embeddingProvider) {
    throw new Error(
      'Vector queries contain text but no embedding provider is configured. ' +
      'Either pass number[] vectors directly, or configure an embedding provider on the database.'
    )
  }

  // Batch embed all text queries
  const vectors = await embeddingProvider.embedBatch(
    textQueries.map(q => q.text),
    { isQuery: true }
  )

  // Build results
  const results: VectorFilterNormalizationResult[] = []

  for (let i = 0; i < filters.length; i++) {
    const filter = filters[i]!
    const vq = extractVectorQuery(filter)

    if (!vq) {
      results.push({ filter, textEmbedded: false })
      continue
    }

    // Check if this was a text query
    const textQueryIndex = textQueries.findIndex(q => q.index === i)
    if (textQueryIndex !== -1) {
      const vector = vectors[textQueryIndex]!
      const normalizedFilter: Filter = {
        ...filter,
        $vector: {
          query: vector,
          field: vq.field,
          topK: vq.topK,
          minScore: vq.minScore,
          efSearch: vq.efSearch,
          // Legacy format
          $near: vector,
          $k: vq.topK,
          $field: vq.field,
          $minScore: vq.minScore,
        },
      }
      results.push({
        filter: normalizedFilter,
        textEmbedded: true,
        vectorQuery: {
          vector,
          field: vq.field,
          topK: vq.topK,
          minScore: vq.minScore,
          efSearch: vq.efSearch,
        },
      })
    } else {
      // Already a vector
      results.push({
        filter,
        textEmbedded: false,
        vectorQuery: {
          vector: vq.query as number[],
          field: vq.field,
          topK: vq.topK,
          minScore: vq.minScore,
          efSearch: vq.efSearch,
        },
      })
    }
  }

  return results
}
