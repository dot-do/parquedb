/**
 * Vercel AI SDK Embeddings Integration for ParqueDB
 *
 * Provides embedding generation using Vercel AI SDK providers (OpenAI, Cohere, etc.)
 * for Node.js environments. This wrapper matches the interface used by Workers AI
 * embeddings to enable seamless switching between providers.
 *
 * NOTE: The AI SDK packages (ai, @ai-sdk/openai, etc.) are optional peer dependencies.
 * You must install them to use this module:
 *   npm install ai @ai-sdk/openai
 *
 * @example
 * ```typescript
 * import { createAISDKEmbeddings } from 'parquedb/embeddings'
 *
 * // OpenAI embeddings
 * const openaiEmbedder = createAISDKEmbeddings({
 *   provider: 'openai',
 *   model: 'text-embedding-3-small',
 *   apiKey: process.env.OPENAI_API_KEY
 * })
 *
 * // Cohere embeddings
 * const cohereEmbedder = createAISDKEmbeddings({
 *   provider: 'cohere',
 *   model: 'embed-english-v3.0',
 *   apiKey: process.env.COHERE_API_KEY
 * })
 *
 * // Generate embedding
 * const vector = await openaiEmbedder.embed('Hello, world!')
 *
 * // Batch embedding
 * const vectors = await openaiEmbedder.embedBatch(['Hello', 'World'])
 * ```
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Common interface for embedding providers
 *
 * This interface is implemented by both WorkersAIEmbeddings and AISDKEmbeddings
 * to allow seamless switching between embedding backends.
 */
export interface EmbeddingProvider {
  /** Generate embedding for a single text */
  embed(text: string): Promise<number[]>
  /** Generate embeddings for multiple texts */
  embedBatch(texts: string[]): Promise<number[][]>
  /** Vector dimensions for this provider/model */
  dimensions: number
}

/**
 * Supported AI SDK providers
 */
export type AISDKProvider =
  | 'openai'
  | 'cohere'
  | 'mistral'
  | 'google'
  | 'amazon'
  | 'voyage'
  | 'custom'

/**
 * Configuration for AI SDK embeddings
 */
export interface AISDKEmbeddingsConfig {
  /** AI provider to use */
  provider: AISDKProvider
  /** Model identifier */
  model: string
  /** API key for the provider */
  apiKey?: string | undefined
  /** Base URL override (for custom providers or self-hosted) */
  baseURL?: string | undefined
  /** Custom embedding function (for custom provider) */
  customEmbedFn?: ((texts: string[]) => Promise<number[][]>) | undefined
  /** Override dimensions (auto-detected if not specified) */
  dimensions?: number | undefined
  /** Whether to normalize vectors to unit length (default: true) */
  normalize?: boolean | undefined
}

/**
 * Options for individual embed calls
 */
export interface AISDKEmbedOptions {
  /** Return raw embeddings without normalization */
  raw?: boolean | undefined
}

/**
 * Model configuration with default dimensions
 */
interface ModelConfig {
  dimensions: number
  maxBatchSize?: number | undefined
}

/**
 * Type for embedding model returned by AI SDK providers
 */
interface EmbeddingModel {
  modelId?: string | undefined
}

/**
 * Type for embedMany result
 */
interface EmbedManyResult {
  embeddings: number[][]
}

/**
 * Type for embedMany function from AI SDK
 */
type EmbedManyFn = (params: { model: EmbeddingModel; values: string[] }) => Promise<EmbedManyResult>

/**
 * Type for AI SDK provider factory
 */
type ProviderFactory = (config: { apiKey?: string | undefined; baseURL?: string | undefined }) => {
  textEmbeddingModel: (model: string) => EmbeddingModel
}

// =============================================================================
// Constants
// =============================================================================

/**
 * Known embedding model configurations
 */
export const AI_SDK_MODELS: Record<string, Record<string, ModelConfig>> = {
  openai: {
    'text-embedding-3-small': { dimensions: 1536, maxBatchSize: 2048 },
    'text-embedding-3-large': { dimensions: 3072, maxBatchSize: 2048 },
    'text-embedding-ada-002': { dimensions: 1536, maxBatchSize: 2048 },
  },
  cohere: {
    'embed-english-v3.0': { dimensions: 1024, maxBatchSize: 96 },
    'embed-multilingual-v3.0': { dimensions: 1024, maxBatchSize: 96 },
    'embed-english-light-v3.0': { dimensions: 384, maxBatchSize: 96 },
    'embed-multilingual-light-v3.0': { dimensions: 384, maxBatchSize: 96 },
  },
  mistral: {
    'mistral-embed': { dimensions: 1024, maxBatchSize: 512 },
  },
  google: {
    'text-embedding-004': { dimensions: 768, maxBatchSize: 100 },
    'text-embedding-preview-0815': { dimensions: 768, maxBatchSize: 100 },
  },
  voyage: {
    'voyage-3': { dimensions: 1024, maxBatchSize: 128 },
    'voyage-3-lite': { dimensions: 512, maxBatchSize: 128 },
    'voyage-code-3': { dimensions: 1024, maxBatchSize: 128 },
  },
  amazon: {
    'amazon.titan-embed-text-v1': { dimensions: 1536, maxBatchSize: 100 },
    'amazon.titan-embed-text-v2:0': { dimensions: 1024, maxBatchSize: 100 },
  },
}

/**
 * Default dimensions when model is not recognized
 */
export const DEFAULT_AI_SDK_DIMENSIONS = 1536

// =============================================================================
// AISDKEmbeddings Class
// =============================================================================

/**
 * Vercel AI SDK embeddings provider
 *
 * Generates vector embeddings using various AI SDK providers.
 * Supports single and batch embedding generation.
 *
 * @example
 * ```typescript
 * // Initialize with OpenAI
 * const embeddings = new AISDKEmbeddings({
 *   provider: 'openai',
 *   model: 'text-embedding-3-small',
 *   apiKey: process.env.OPENAI_API_KEY
 * })
 *
 * // Generate embedding for text
 * const vector = await embeddings.embed("Search query")
 *
 * // Batch embedding for multiple texts
 * const vectors = await embeddings.embedBatch([
 *   "Document 1",
 *   "Document 2"
 * ])
 * ```
 */
export class AISDKEmbeddings implements EmbeddingProvider {
  /** Provider configuration */
  private config: AISDKEmbeddingsConfig

  /** Model configuration */
  private modelConfig: ModelConfig

  /**
   * Create a new AISDKEmbeddings instance
   *
   * @param config - Embedding configuration
   */
  constructor(config: AISDKEmbeddingsConfig) {
    this.config = {
      normalize: true,
      ...config,
    }

    // Get model config with dimensions
    const providerModels = AI_SDK_MODELS[config.provider] ?? {}
    this.modelConfig = providerModels[config.model] ?? {
      dimensions: config.dimensions ?? DEFAULT_AI_SDK_DIMENSIONS,
    }

    // Override dimensions if specified
    if (config.dimensions) {
      this.modelConfig = { ...this.modelConfig, dimensions: config.dimensions }
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
   */
  async embed(text: string, options?: AISDKEmbedOptions): Promise<number[]> {
    const vectors = await this.embedBatch([text], options)
    const vector = vectors[0]
    if (!vector) {
      throw new Error('Failed to generate embedding: empty response')
    }
    return vector
  }

  /**
   * Generate embeddings for multiple texts (batch)
   *
   * @param texts - Array of texts to embed
   * @param options - Embedding options
   * @returns Array of vector embeddings
   */
  async embedBatch(texts: string[], options?: AISDKEmbedOptions): Promise<number[][]> {
    if (texts.length === 0) {
      return []
    }

    // Handle custom provider
    if (this.config.provider === 'custom') {
      if (!this.config.customEmbedFn) {
        throw new Error('Custom provider requires customEmbedFn')
      }
      const vectors = await this.config.customEmbedFn(texts)
      return this.maybeNormalize(vectors, options)
    }

    // Use the appropriate provider
    const vectors = await this.callProvider(texts)
    return this.maybeNormalize(vectors, options)
  }

  /**
   * Embed a query text (same as embed for most providers)
   *
   * @param query - Query text to embed
   * @param options - Embedding options
   * @returns Vector embedding
   */
  async embedQuery(query: string, options?: AISDKEmbedOptions): Promise<number[]> {
    // For Cohere, we could use input_type: 'search_query'
    // For now, treat same as regular embed
    return this.embed(query, options)
  }

  /**
   * Embed document text
   *
   * @param document - Document text to embed
   * @param options - Embedding options
   * @returns Vector embedding
   */
  async embedDocument(document: string, options?: AISDKEmbedOptions): Promise<number[]> {
    // For Cohere, we could use input_type: 'search_document'
    // For now, treat same as regular embed
    return this.embed(document, options)
  }

  /**
   * Get the dimensions of the embedding vectors
   */
  get dimensions(): number {
    return this.modelConfig.dimensions
  }

  /**
   * Get the current model
   */
  get model(): string {
    return this.config.model
  }

  /**
   * Get the current provider
   */
  get provider(): AISDKProvider {
    return this.config.provider
  }

  /**
   * Get model configuration
   */
  getModelConfig(): ModelConfig {
    return this.modelConfig
  }

  // ===========================================================================
  // Provider Implementations
  // ===========================================================================

  /**
   * Call the appropriate provider to generate embeddings
   */
  private async callProvider(texts: string[]): Promise<number[][]> {
    switch (this.config.provider) {
      case 'openai':
        return this.callOpenAI(texts)
      case 'cohere':
        return this.callCohere(texts)
      case 'mistral':
        return this.callMistral(texts)
      case 'google':
        return this.callGoogle(texts)
      case 'voyage':
        return this.callVoyage(texts)
      case 'amazon':
        return this.callAmazon(texts)
      default:
        throw new Error(`Unsupported provider: ${this.config.provider}`)
    }
  }

  /**
   * Call OpenAI embeddings API using AI SDK
   */
  private async callOpenAI(texts: string[]): Promise<number[][]> {
    const { embedMany, providerFactory } = await this.loadAISDK('@ai-sdk/openai', 'openai')

    const provider = providerFactory({
      apiKey: this.config.apiKey,
      baseURL: this.config.baseURL,
    })

    const model = provider.textEmbeddingModel(this.config.model)
    const result = await embedMany({ model, values: texts })

    return result.embeddings
  }

  /**
   * Call Cohere embeddings API using AI SDK
   */
  private async callCohere(texts: string[]): Promise<number[][]> {
    const { embedMany, providerFactory } = await this.loadAISDK('@ai-sdk/cohere', 'cohere')

    const provider = providerFactory({
      apiKey: this.config.apiKey,
      baseURL: this.config.baseURL,
    })

    const model = provider.textEmbeddingModel(this.config.model)
    const result = await embedMany({ model, values: texts })

    return result.embeddings
  }

  /**
   * Call Mistral embeddings API using AI SDK
   */
  private async callMistral(texts: string[]): Promise<number[][]> {
    const { embedMany, providerFactory } = await this.loadAISDK('@ai-sdk/mistral', 'mistral')

    const provider = providerFactory({
      apiKey: this.config.apiKey,
      baseURL: this.config.baseURL,
    })

    const model = provider.textEmbeddingModel(this.config.model)
    const result = await embedMany({ model, values: texts })

    return result.embeddings
  }

  /**
   * Call Google embeddings API using AI SDK
   */
  private async callGoogle(texts: string[]): Promise<number[][]> {
    const { embedMany, providerFactory } = await this.loadAISDK('@ai-sdk/google', 'google')

    const provider = providerFactory({
      apiKey: this.config.apiKey,
      baseURL: this.config.baseURL,
    })

    const model = provider.textEmbeddingModel(this.config.model)
    const result = await embedMany({ model, values: texts })

    return result.embeddings
  }

  /**
   * Call Voyage embeddings API using AI SDK
   * Voyage uses OpenAI-compatible API
   */
  private async callVoyage(texts: string[]): Promise<number[][]> {
    const { embedMany, providerFactory } = await this.loadAISDK('@ai-sdk/openai', 'createOpenAI')

    const provider = providerFactory({
      apiKey: this.config.apiKey,
      baseURL: this.config.baseURL ?? 'https://api.voyageai.com/v1',
    })

    const model = provider.textEmbeddingModel(this.config.model)
    const result = await embedMany({ model, values: texts })

    return result.embeddings
  }

  /**
   * Call Amazon Bedrock embeddings API using AI SDK
   */
  private async callAmazon(texts: string[]): Promise<number[][]> {
    const { embedMany, providerFactory } = await this.loadAISDK('@ai-sdk/amazon-bedrock', 'amazon')

    const provider = providerFactory({
      // Amazon Bedrock uses AWS credentials from environment
    })

    const model = provider.textEmbeddingModel(this.config.model)
    const result = await embedMany({ model, values: texts })

    return result.embeddings
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  /**
   * Dynamically load AI SDK provider module
   *
   * @param packageName - Package to import (e.g., '@ai-sdk/openai')
   * @param exportName - Export name for the provider factory (e.g., 'openai')
   */
  private async loadAISDK(
    packageName: string,
    exportName: string
  ): Promise<{
    embedMany: EmbedManyFn
    providerFactory: ProviderFactory
  }> {
    try {
      // Dynamic imports - these packages are optional peer dependencies
      // Using Function constructor to avoid TypeScript's static import analysis
      // eslint-disable-next-line @typescript-eslint/no-implied-eval
      const dynamicImport = new Function('specifier', 'return import(specifier)') as (
        specifier: string
      ) => Promise<Record<string, unknown>>

      const aiModule = await dynamicImport('ai')
      const providerModule = await dynamicImport(packageName)

      const embedMany = aiModule.embedMany as EmbedManyFn
      const providerFactory = providerModule[exportName] as ProviderFactory

      if (!embedMany || typeof embedMany !== 'function') {
        throw new Error('embedMany function not found in ai package')
      }

      if (!providerFactory || typeof providerFactory !== 'function') {
        throw new Error(`${exportName} function not found in ${packageName}`)
      }

      return { embedMany, providerFactory }
    } catch (error) {
      const err = error as Error
      throw new Error(
        `Failed to load AI SDK provider "${packageName}". ` +
        `Make sure to install it: npm install ai ${packageName}\n` +
        `Original error: ${err.message}`
      )
    }
  }

  /**
   * Normalize vectors if configured
   */
  private maybeNormalize(vectors: number[][], options?: AISDKEmbedOptions): number[][] {
    if (this.config.normalize && !options?.raw) {
      return vectors.map(v => this.normalizeVector(v))
    }
    return vectors
  }

  /**
   * Normalize a vector to unit length (L2 normalization)
   */
  private normalizeVector(vector: number[]): number[] {
    const magnitude = Math.sqrt(vector.reduce((sum, v) => sum + v * v, 0))
    if (magnitude === 0) return vector
    return vector.map(v => v / magnitude)
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an AISDKEmbeddings instance
 *
 * @param config - Embedding configuration
 * @returns AISDKEmbeddings instance
 *
 * @example
 * ```typescript
 * // OpenAI
 * const embedder = createAISDKEmbeddings({
 *   provider: 'openai',
 *   model: 'text-embedding-3-small',
 *   apiKey: process.env.OPENAI_API_KEY
 * })
 *
 * // Cohere
 * const embedder = createAISDKEmbeddings({
 *   provider: 'cohere',
 *   model: 'embed-english-v3.0',
 *   apiKey: process.env.COHERE_API_KEY
 * })
 *
 * // Custom provider
 * const embedder = createAISDKEmbeddings({
 *   provider: 'custom',
 *   model: 'my-model',
 *   dimensions: 768,
 *   customEmbedFn: async (texts) => {
 *     // Your custom embedding logic
 *     return texts.map(() => new Array(768).fill(0))
 *   }
 * })
 * ```
 */
export function createAISDKEmbeddings(config: AISDKEmbeddingsConfig): AISDKEmbeddings {
  return new AISDKEmbeddings(config)
}

/**
 * Get dimensions for a given provider/model combination
 *
 * @param provider - AI provider
 * @param model - Model identifier
 * @returns Number of dimensions
 */
export function getAISDKModelDimensions(provider: AISDKProvider, model: string): number {
  const providerModels = AI_SDK_MODELS[provider]
  if (!providerModels) {
    return DEFAULT_AI_SDK_DIMENSIONS
  }
  return providerModels[model]?.dimensions ?? DEFAULT_AI_SDK_DIMENSIONS
}

/**
 * List available models for a provider
 *
 * @param provider - AI provider
 * @returns Array of model names
 */
export function listAISDKModels(provider: AISDKProvider): string[] {
  const providerModels = AI_SDK_MODELS[provider]
  if (!providerModels) {
    return []
  }
  return Object.keys(providerModels)
}
