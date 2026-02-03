/**
 * Embedding Provider Interface for ParqueDB
 *
 * Defines a generic interface for embedding providers that can be used
 * for both document embedding at write time and query embedding at search time.
 *
 * Providers implement this interface to enable automatic text-to-vector conversion
 * for vector similarity searches.
 *
 * @example
 * ```typescript
 * // Using WorkersAI
 * const provider = createWorkersAIProvider(env.AI)
 *
 * // Using AI SDK
 * import { embed } from 'ai'
 * const provider = createAISDKProvider({ embed })
 *
 * // Configure database with provider
 * const db = new ParqueDB({
 *   storage,
 *   embeddingProvider: provider
 * })
 *
 * // Now can query with text
 * await db.Posts.find({
 *   $vector: {
 *     field: 'embedding',
 *     text: 'machine learning tutorials',
 *     topK: 10
 *   }
 * })
 * ```
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Options for embedding generation
 */
export interface EmbedTextOptions {
  /** Model override for this specific embedding */
  model?: string
  /** Whether this is a query embedding (may use different prefixes) */
  isQuery?: boolean
}

/**
 * Embedding result with metadata
 */
export interface EmbeddingResult {
  /** The embedding vector */
  vector: number[]
  /** Dimensions of the vector */
  dimensions: number
  /** Model used for embedding */
  model?: string
}

/**
 * Generic embedding provider interface
 *
 * Implement this interface to add support for different embedding services.
 * The provider is used for:
 * - Query-time embedding: Converting search text to vectors
 * - Document embedding: Converting document fields to vectors on write
 */
export interface EmbeddingProvider {
  /**
   * Generate an embedding for a single text
   *
   * @param text - Text to embed
   * @param options - Embedding options
   * @returns Promise resolving to the embedding vector
   */
  embed(text: string, options?: EmbedTextOptions): Promise<number[]>

  /**
   * Generate embeddings for multiple texts (batch)
   *
   * @param texts - Array of texts to embed
   * @param options - Embedding options
   * @returns Promise resolving to array of embedding vectors
   */
  embedBatch(texts: string[], options?: EmbedTextOptions): Promise<number[][]>

  /**
   * Get the dimensions of embeddings produced by this provider
   */
  readonly dimensions: number

  /**
   * Get the default model identifier
   */
  readonly model: string
}

// =============================================================================
// Query Embedding Cache
// =============================================================================

/**
 * Options for the query embedding cache
 */
export interface QueryEmbeddingCacheOptions {
  /** Maximum number of entries to cache (default: 1000) */
  maxSize?: number
  /** TTL in milliseconds for cache entries (default: 5 minutes) */
  ttlMs?: number
}

/**
 * Cache entry with timestamp
 */
interface CacheEntry {
  vector: number[]
  timestamp: number
}

import { DEFAULT_EMBEDDING_CACHE_SIZE, DEFAULT_EMBEDDING_CACHE_TTL } from '../constants'

/**
 * LRU cache for query embeddings
 *
 * Caches recently used query embeddings to avoid redundant API calls
 * for repeated searches.
 */
export class QueryEmbeddingCache {
  private cache: Map<string, CacheEntry> = new Map()
  private readonly maxSize: number
  private readonly ttlMs: number

  constructor(options: QueryEmbeddingCacheOptions = {}) {
    this.maxSize = options.maxSize ?? DEFAULT_EMBEDDING_CACHE_SIZE
    this.ttlMs = options.ttlMs ?? DEFAULT_EMBEDDING_CACHE_TTL
  }

  /**
   * Generate a cache key from text and options
   */
  private getCacheKey(text: string, model?: string): string {
    return `${model ?? 'default'}:${text}`
  }

  /**
   * Get a cached embedding if available and not expired
   */
  get(text: string, model?: string): number[] | undefined {
    const key = this.getCacheKey(text, model)
    const entry = this.cache.get(key)

    if (!entry) {
      return undefined
    }

    // Check if expired
    if (Date.now() - entry.timestamp > this.ttlMs) {
      this.cache.delete(key)
      return undefined
    }

    // Move to end for LRU behavior (delete and re-add)
    this.cache.delete(key)
    this.cache.set(key, entry)

    return entry.vector
  }

  /**
   * Store an embedding in the cache
   */
  set(text: string, vector: number[], model?: string): void {
    const key = this.getCacheKey(text, model)

    // Evict oldest entries if at capacity
    while (this.cache.size >= this.maxSize) {
      const firstKey = this.cache.keys().next().value
      if (firstKey) {
        this.cache.delete(firstKey)
      } else {
        break
      }
    }

    this.cache.set(key, {
      vector,
      timestamp: Date.now(),
    })
  }

  /**
   * Clear all cached entries
   */
  clear(): void {
    this.cache.clear()
  }

  /**
   * Get current cache size
   */
  get size(): number {
    return this.cache.size
  }

  /**
   * Prune expired entries
   */
  prune(): number {
    const now = Date.now()
    let pruned = 0

    for (const [key, entry] of this.cache) {
      if (now - entry.timestamp > this.ttlMs) {
        this.cache.delete(key)
        pruned++
      }
    }

    return pruned
  }
}

// =============================================================================
// Caching Embedding Provider Wrapper
// =============================================================================

/**
 * Options for the caching embedding provider
 */
export interface CachingEmbeddingProviderOptions {
  /** Cache options */
  cache?: QueryEmbeddingCacheOptions
}

/**
 * Wraps an embedding provider with query caching
 *
 * Only caches query embeddings (when isQuery option is true).
 * Document embeddings are not cached as they are typically unique.
 */
export class CachingEmbeddingProvider implements EmbeddingProvider {
  private queryCache: QueryEmbeddingCache

  constructor(
    private provider: EmbeddingProvider,
    options: CachingEmbeddingProviderOptions = {}
  ) {
    this.queryCache = new QueryEmbeddingCache(options.cache)
  }

  get dimensions(): number {
    return this.provider.dimensions
  }

  get model(): string {
    return this.provider.model
  }

  /**
   * Get the underlying cache for inspection/testing
   */
  get cache(): QueryEmbeddingCache {
    return this.queryCache
  }

  async embed(text: string, options?: EmbedTextOptions): Promise<number[]> {
    // Only use cache for query embeddings
    if (options?.isQuery) {
      const cached = this.queryCache.get(text, options?.model)
      if (cached) {
        return cached
      }
    }

    const vector = await this.provider.embed(text, options)

    // Cache query embeddings
    if (options?.isQuery) {
      this.queryCache.set(text, vector, options?.model)
    }

    return vector
  }

  async embedBatch(texts: string[], options?: EmbedTextOptions): Promise<number[][]> {
    // For batch queries, check cache for each text
    if (options?.isQuery) {
      const results: (number[] | undefined)[] = texts.map(text =>
        this.queryCache.get(text, options?.model)
      )

      // Find texts that need embedding
      const uncachedTexts: string[] = []
      const uncachedIndices: number[] = []

      for (let i = 0; i < texts.length; i++) {
        if (!results[i]) {
          uncachedTexts.push(texts[i]!)
          uncachedIndices.push(i)
        }
      }

      // If all cached, return immediately
      if (uncachedTexts.length === 0) {
        return results as number[][]
      }

      // Embed uncached texts
      const newVectors = await this.provider.embedBatch(uncachedTexts, options)

      // Merge results and cache new embeddings
      for (let i = 0; i < uncachedIndices.length; i++) {
        const idx = uncachedIndices[i]!
        const vector = newVectors[i]!
        results[idx] = vector
        this.queryCache.set(uncachedTexts[i]!, vector, options?.model)
      }

      return results as number[][]
    }

    // For non-query embeddings, just pass through
    return this.provider.embedBatch(texts, options)
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a caching wrapper around an embedding provider
 *
 * @param provider - The underlying embedding provider
 * @param options - Cache options
 * @returns Caching embedding provider
 */
export function withQueryCache(
  provider: EmbeddingProvider,
  options?: CachingEmbeddingProviderOptions
): CachingEmbeddingProvider {
  return new CachingEmbeddingProvider(provider, options)
}
