/**
 * Query-Time Embedding Generation for Vector Search
 *
 * Provides utilities for converting text queries to vector embeddings
 * at query time, enabling semantic search with text input.
 *
 * Features:
 * - Text-to-vector conversion using configurable embedding providers
 * - Optional LRU caching of query embeddings
 * - Support for both text and pre-computed vector queries
 * - Batch embedding for multiple queries
 *
 * @example
 * ```typescript
 * import { createQueryEmbeddingGenerator } from '@/indexes/vector/query-embeddings'
 * import { createWorkersAIProvider } from '@/embeddings/workers-ai'
 *
 * const provider = createWorkersAIProvider(env.AI)
 * const generator = createQueryEmbeddingGenerator(provider, {
 *   cacheEnabled: true,
 *   maxCacheSize: 500
 * })
 *
 * // Embed a text query
 * const vector = await generator.embed('machine learning tutorials')
 *
 * // Or pass through an existing vector
 * const result = await generator.embedOrPassthrough(existingVector)
 * ```
 */

import type { EmbeddingProvider } from '../../embeddings/provider'

// =============================================================================
// Types
// =============================================================================

/**
 * Options for the QueryEmbeddingGenerator
 */
export interface QueryEmbeddingGeneratorOptions {
  /** Enable caching of query embeddings (default: false) */
  cacheEnabled?: boolean | undefined
  /** Maximum number of cached embeddings (default: 1000) */
  maxCacheSize?: number | undefined
  /** TTL for cached embeddings in milliseconds (default: 5 minutes) */
  cacheTtlMs?: number | undefined
  /** Whether to validate vector dimensions when passing through (default: true) */
  validateDimensions?: boolean | undefined
}

/**
 * Cache statistics for monitoring
 */
export interface CacheStats {
  /** Current number of cached entries */
  size: number
  /** Number of cache hits */
  hits: number
  /** Number of cache misses */
  misses: number
  /** Hit rate (hits / total requests) */
  hitRate: number
}

/**
 * Input for preparing a vector query
 */
export interface VectorQueryInput {
  /** Text query or pre-computed vector */
  query: string | number[]
  /** Field containing the vector to search */
  field: string
  /** Number of results to return */
  topK: number
  /** Minimum score threshold */
  minScore?: number | undefined
  /** HNSW ef_search parameter */
  efSearch?: number | undefined
}

/**
 * Prepared vector query ready for execution
 */
export interface PreparedVectorQuery {
  /** The query vector (embedded or passed through) */
  vector: number[]
  /** Field to search */
  field: string
  /** Number of results */
  topK: number
  /** Minimum score */
  minScore?: number | undefined
  /** ef_search parameter */
  efSearch?: number | undefined
  /** Whether the text was embedded */
  textEmbedded: boolean
  /** The original query input */
  originalQuery: string | number[]
}

// =============================================================================
// Cache Entry
// =============================================================================

/**
 * Cached embedding entry with timestamp
 */
interface CacheEntry {
  vector: number[]
  timestamp: number
}

// =============================================================================
// QueryEmbeddingGenerator
// =============================================================================

/**
 * Generates embeddings for text queries at search time
 *
 * Wraps an embedding provider with optional caching and provides
 * utilities for preparing vector search queries from text input.
 */
export class QueryEmbeddingGenerator {
  private cache: Map<string, CacheEntry> = new Map()
  private readonly maxCacheSize: number
  private readonly cacheTtlMs: number
  private readonly _validateDimensions: boolean
  private hits = 0
  private misses = 0

  /**
   * Whether caching is enabled
   */
  readonly cacheEnabled: boolean

  /**
   * The underlying embedding provider
   */
  readonly provider: EmbeddingProvider

  constructor(
    provider: EmbeddingProvider,
    options: QueryEmbeddingGeneratorOptions = {}
  ) {
    this.provider = provider
    this.cacheEnabled = options.cacheEnabled ?? false
    this.maxCacheSize = options.maxCacheSize ?? 1000
    this.cacheTtlMs = options.cacheTtlMs ?? 5 * 60 * 1000 // 5 minutes
    this._validateDimensions = options.validateDimensions ?? true
  }

  /**
   * Get the dimensions of embeddings from the provider
   */
  get dimensions(): number {
    return this.provider.dimensions
  }

  /**
   * Get the current cache size
   */
  get cacheSize(): number {
    return this.cache.size
  }

  /**
   * Embed a text query into a vector
   *
   * @param text - The text query to embed
   * @returns Promise resolving to the embedding vector
   */
  async embed(text: string): Promise<number[]> {
    // Check cache first if enabled
    if (this.cacheEnabled) {
      const cached = this.getFromCache(text)
      if (cached) {
        this.hits++
        return cached
      }
      this.misses++
    }

    // Generate embedding
    const vector = await this.provider.embed(text, { isQuery: true })

    // Cache the result if enabled
    if (this.cacheEnabled) {
      this.setInCache(text, vector)
    }

    return vector
  }

  /**
   * Embed multiple text queries into vectors
   *
   * @param texts - Array of text queries to embed
   * @returns Promise resolving to array of embedding vectors
   */
  async embedBatch(texts: string[]): Promise<number[][]> {
    if (texts.length === 0) {
      return []
    }

    // If caching is disabled, just call the provider
    if (!this.cacheEnabled) {
      return this.provider.embedBatch(texts, { isQuery: true })
    }

    // Check cache for each text
    const results: (number[] | undefined)[] = new Array(texts.length)
    const uncachedTexts: string[] = []
    const uncachedIndices: number[] = []

    for (let i = 0; i < texts.length; i++) {
      const text = texts[i]!
      const cached = this.getFromCache(text)
      if (cached) {
        results[i] = cached
        this.hits++
      } else {
        uncachedTexts.push(text)
        uncachedIndices.push(i)
        this.misses++
      }
    }

    // If all are cached, return immediately
    if (uncachedTexts.length === 0) {
      return results as number[][]
    }

    // Embed uncached texts
    const newVectors = await this.provider.embedBatch(uncachedTexts, { isQuery: true })

    // Merge results and cache new embeddings
    for (let i = 0; i < uncachedIndices.length; i++) {
      const idx = uncachedIndices[i]!
      const vector = newVectors[i]!
      results[idx] = vector
      this.setInCache(uncachedTexts[i]!, vector)
    }

    return results as number[][]
  }

  /**
   * Embed text or pass through an existing vector
   *
   * @param query - Text query or pre-computed vector
   * @returns Promise resolving to the embedding vector
   * @throws Error if vector dimensions don't match and validation is enabled
   */
  async embedOrPassthrough(query: string | number[]): Promise<number[]> {
    // If already a vector, optionally validate and return
    if (Array.isArray(query)) {
      if (this._validateDimensions && query.length !== this.dimensions) {
        throw new Error(
          `Vector dimension mismatch: expected ${this.dimensions}, got ${query.length}`
        )
      }
      return query
    }

    // Embed the text query
    return this.embed(query)
  }

  /**
   * Prepare a vector query from text or vector input
   *
   * @param input - Vector query input with text or vector
   * @returns Promise resolving to a prepared vector query
   */
  async prepareVectorQuery(input: VectorQueryInput): Promise<PreparedVectorQuery> {
    const isTextQuery = typeof input.query === 'string'
    const vector = await this.embedOrPassthrough(input.query)

    return {
      vector,
      field: input.field,
      topK: input.topK,
      minScore: input.minScore,
      efSearch: input.efSearch,
      textEmbedded: isTextQuery,
      originalQuery: input.query,
    }
  }

  /**
   * Clear the embedding cache
   */
  clearCache(): void {
    this.cache.clear()
    this.hits = 0
    this.misses = 0
  }

  /**
   * Get cache statistics
   */
  getCacheStats(): CacheStats {
    const total = this.hits + this.misses
    return {
      size: this.cache.size,
      hits: this.hits,
      misses: this.misses,
      hitRate: total > 0 ? this.hits / total : 0,
    }
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  /**
   * Get a cached embedding if available and not expired
   */
  private getFromCache(text: string): number[] | undefined {
    const entry = this.cache.get(text)
    if (!entry) {
      return undefined
    }

    // Check if expired
    if (Date.now() - entry.timestamp > this.cacheTtlMs) {
      this.cache.delete(text)
      return undefined
    }

    // Move to end for LRU behavior
    this.cache.delete(text)
    this.cache.set(text, entry)

    return entry.vector
  }

  /**
   * Store an embedding in the cache
   */
  private setInCache(text: string, vector: number[]): void {
    // Evict oldest entries if at capacity
    while (this.cache.size >= this.maxCacheSize) {
      const firstKey = this.cache.keys().next().value
      if (firstKey !== undefined) {
        this.cache.delete(firstKey)
      } else {
        break
      }
    }

    this.cache.set(text, {
      vector,
      timestamp: Date.now(),
    })
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a QueryEmbeddingGenerator with the given provider and options
 *
 * @param provider - The embedding provider to use
 * @param options - Generator options
 * @returns A configured QueryEmbeddingGenerator instance
 *
 * @example
 * ```typescript
 * const generator = createQueryEmbeddingGenerator(provider, {
 *   cacheEnabled: true,
 *   maxCacheSize: 500
 * })
 *
 * const vector = await generator.embed('search query')
 * ```
 */
export function createQueryEmbeddingGenerator(
  provider: EmbeddingProvider,
  options?: QueryEmbeddingGeneratorOptions
): QueryEmbeddingGenerator {
  return new QueryEmbeddingGenerator(provider, options)
}
