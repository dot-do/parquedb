/**
 * Cloudflare Workers AI Embeddings Integration
 *
 * Provides embedding generation using Cloudflare Workers AI.
 * Default model: @cf/baai/bge-m3 (1024 dimensions)
 *
 * @example
 * ```typescript
 * const embeddings = new WorkersAIEmbeddings(env.AI)
 *
 * // Single text embedding
 * const vector = await embeddings.embed("Hello, world!")
 *
 * // Batch embedding
 * const vectors = await embeddings.embedBatch(["Hello", "World"])
 * ```
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Cloudflare AI binding interface
 * This matches the AI binding available in Cloudflare Workers
 */
export interface AIBinding {
  run<T = unknown>(
    model: string,
    inputs: Record<string, unknown>
  ): Promise<T>
}

/**
 * Embedding model configuration
 */
export interface EmbeddingModelConfig {
  /** Model identifier */
  model: string
  /** Vector dimensions */
  dimensions: number
  /** Maximum tokens per text */
  maxTokens?: number
  /** Whether to normalize vectors */
  normalize?: boolean
}

/**
 * Embedding options for individual calls
 */
export interface EmbedOptions {
  /** Override the default model */
  model?: string
  /** Return raw embeddings without normalization */
  raw?: boolean
}

/**
 * Response from the AI embedding model
 */
interface EmbeddingResponse {
  shape: number[]
  data: number[][]
}

// =============================================================================
// Constants
// =============================================================================

/**
 * Default embedding model: BGE-M3
 * - Multi-lingual (100+ languages)
 * - Multi-functionality (dense, sparse, multi-vector retrieval)
 * - Multi-granularity (sentence to document)
 * - 1024 dimensions
 */
export const DEFAULT_MODEL = '@cf/baai/bge-m3'
export const DEFAULT_DIMENSIONS = 1024

/**
 * Available embedding models in Cloudflare Workers AI
 */
export const EMBEDDING_MODELS: Record<string, EmbeddingModelConfig> = {
  // BGE-M3: Best for multi-lingual, general purpose
  '@cf/baai/bge-m3': {
    model: '@cf/baai/bge-m3',
    dimensions: 1024,
    maxTokens: 8192,
    normalize: true,
  },
  // BGE-Base: Good balance of speed and quality
  '@cf/baai/bge-base-en-v1.5': {
    model: '@cf/baai/bge-base-en-v1.5',
    dimensions: 768,
    maxTokens: 512,
    normalize: true,
  },
  // BGE-Small: Fastest, lowest memory
  '@cf/baai/bge-small-en-v1.5': {
    model: '@cf/baai/bge-small-en-v1.5',
    dimensions: 384,
    maxTokens: 512,
    normalize: true,
  },
  // BGE-Large: Highest quality
  '@cf/baai/bge-large-en-v1.5': {
    model: '@cf/baai/bge-large-en-v1.5',
    dimensions: 1024,
    maxTokens: 512,
    normalize: true,
  },
}

// =============================================================================
// WorkersAIEmbeddings Class
// =============================================================================

/**
 * Cloudflare Workers AI embeddings provider
 *
 * Generates vector embeddings using Cloudflare's AI models.
 * Supports single and batch embedding generation.
 *
 * @example
 * ```typescript
 * // Initialize with AI binding
 * const embeddings = new WorkersAIEmbeddings(env.AI)
 *
 * // Generate embedding for text
 * const vector = await embeddings.embed("Search query")
 *
 * // Batch embedding for multiple texts
 * const vectors = await embeddings.embedBatch([
 *   "Document 1",
 *   "Document 2",
 *   "Document 3"
 * ])
 *
 * // Use with custom model
 * const smallVector = await embeddings.embed("Text", {
 *   model: '@cf/baai/bge-small-en-v1.5'
 * })
 * ```
 */
export class WorkersAIEmbeddings {
  /** AI binding for model inference */
  private ai: AIBinding

  /** Default model to use */
  private defaultModel: string

  /** Model configuration */
  private config: EmbeddingModelConfig

  /**
   * Create a new WorkersAIEmbeddings instance
   *
   * @param ai - Cloudflare AI binding (env.AI)
   * @param model - Model to use (default: @cf/baai/bge-m3)
   */
  constructor(ai: AIBinding, model: string = DEFAULT_MODEL) {
    this.ai = ai
    this.defaultModel = model
    this.config = EMBEDDING_MODELS[model] ?? {
      model,
      dimensions: DEFAULT_DIMENSIONS,
      normalize: true,
    }
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Generate embedding for a single text
   *
   * @param text - Text to embed
   * @param options - Embedding options
   * @returns Vector embedding as number array
   *
   * @example
   * ```typescript
   * const vector = await embeddings.embed("Hello, world!")
   * console.log(vector.length) // 1024 (for bge-m3)
   * ```
   */
  async embed(text: string, options?: EmbedOptions): Promise<number[]> {
    const model = options?.model ?? this.defaultModel

    const response = await this.ai.run<EmbeddingResponse>(model, {
      text: [text],
    })

    if (!response?.data?.[0]) {
      throw new Error('Failed to generate embedding: empty response')
    }

    const vector = response.data[0]

    // Normalize if configured (and not requesting raw)
    if (this.config.normalize && !options?.raw) {
      return this.normalizeVector(vector)
    }

    return vector
  }

  /**
   * Generate embeddings for multiple texts (batch)
   *
   * @param texts - Array of texts to embed
   * @param options - Embedding options
   * @returns Array of vector embeddings
   *
   * @example
   * ```typescript
   * const vectors = await embeddings.embedBatch([
   *   "First document",
   *   "Second document"
   * ])
   * ```
   */
  async embedBatch(texts: string[], options?: EmbedOptions): Promise<number[][]> {
    if (texts.length === 0) {
      return []
    }

    const model = options?.model ?? this.defaultModel

    const response = await this.ai.run<EmbeddingResponse>(model, {
      text: texts,
    })

    if (!response?.data || response.data.length !== texts.length) {
      throw new Error(
        `Failed to generate embeddings: expected ${texts.length} vectors, got ${response?.data?.length ?? 0}`
      )
    }

    // Normalize if configured
    if (this.config.normalize && !options?.raw) {
      return response.data.map(v => this.normalizeVector(v))
    }

    return response.data
  }

  /**
   * Embed a query text (with query-specific prefix for BGE models)
   *
   * For BGE models, queries should be prefixed for better retrieval performance.
   *
   * @param query - Query text to embed
   * @param options - Embedding options
   * @returns Vector embedding
   */
  async embedQuery(query: string, options?: EmbedOptions): Promise<number[]> {
    // BGE models benefit from query prefix
    const prefixedQuery = this.isBGEModel()
      ? `Represent this sentence for searching relevant passages: ${query}`
      : query

    return this.embed(prefixedQuery, options)
  }

  /**
   * Embed document text (with document-specific prefix for BGE models)
   *
   * For BGE models, documents can be prefixed for better retrieval performance.
   *
   * @param document - Document text to embed
   * @param options - Embedding options
   * @returns Vector embedding
   */
  async embedDocument(document: string, options?: EmbedOptions): Promise<number[]> {
    // BGE-M3 doesn't require prefix for documents
    return this.embed(document, options)
  }

  /**
   * Get the dimensions of the embedding vectors
   */
  get dimensions(): number {
    return this.config.dimensions
  }

  /**
   * Get the current model
   */
  get model(): string {
    return this.defaultModel
  }

  /**
   * Get model configuration
   */
  getModelConfig(model?: string): EmbeddingModelConfig {
    const m = model ?? this.defaultModel
    return EMBEDDING_MODELS[m] ?? this.config
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  /**
   * Normalize a vector to unit length (L2 normalization)
   */
  private normalizeVector(vector: number[]): number[] {
    const magnitude = Math.sqrt(vector.reduce((sum, v) => sum + v * v, 0))
    if (magnitude === 0) return vector
    return vector.map(v => v / magnitude)
  }

  /**
   * Check if current model is a BGE model
   */
  private isBGEModel(): boolean {
    return this.defaultModel.includes('bge')
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a WorkersAIEmbeddings instance
 *
 * @param ai - Cloudflare AI binding
 * @param model - Optional model override
 * @returns WorkersAIEmbeddings instance
 */
export function createEmbeddings(
  ai: AIBinding,
  model: string = DEFAULT_MODEL
): WorkersAIEmbeddings {
  return new WorkersAIEmbeddings(ai, model)
}

/**
 * Get dimensions for a given model
 *
 * @param model - Model identifier
 * @returns Number of dimensions
 */
export function getModelDimensions(model: string = DEFAULT_MODEL): number {
  return EMBEDDING_MODELS[model]?.dimensions ?? DEFAULT_DIMENSIONS
}
