/**
 * Embedding Provider Adapters for ParqueDB
 *
 * Factory functions to create EmbeddingProvider instances from various
 * embedding services and SDKs.
 */

import type { EmbeddingProvider, EmbedTextOptions } from './provider'
import type { AIBinding } from './workers-ai'
import {
  WorkersAIEmbeddings,
  DEFAULT_MODEL,
  DEFAULT_DIMENSIONS,
  getModelDimensions,
} from './workers-ai'

// =============================================================================
// Workers AI Adapter
// =============================================================================

/**
 * Options for Workers AI embedding provider
 */
export interface WorkersAIProviderOptions {
  /** Model to use (default: @cf/baai/bge-m3) */
  model?: string
}

/**
 * Create an embedding provider using Cloudflare Workers AI
 *
 * @param ai - Cloudflare AI binding (env.AI)
 * @param options - Provider options
 * @returns EmbeddingProvider instance
 *
 * @example
 * ```typescript
 * const provider = createWorkersAIProvider(env.AI)
 *
 * // With custom model
 * const provider = createWorkersAIProvider(env.AI, {
 *   model: '@cf/baai/bge-small-en-v1.5'
 * })
 * ```
 */
export function createWorkersAIProvider(
  ai: AIBinding,
  options: WorkersAIProviderOptions = {}
): EmbeddingProvider {
  const model = options.model ?? DEFAULT_MODEL
  const embeddings = new WorkersAIEmbeddings(ai, model)

  return {
    async embed(text: string, opts?: EmbedTextOptions): Promise<number[]> {
      // Use query-specific embedding for search queries
      if (opts?.isQuery) {
        return embeddings.embedQuery(text, { model: opts.model })
      }
      return embeddings.embed(text, { model: opts?.model })
    },

    async embedBatch(texts: string[], opts?: EmbedTextOptions): Promise<number[][]> {
      // For batch queries, we don't have query-specific batch method
      // so we use regular batch embedding
      return embeddings.embedBatch(texts, { model: opts?.model })
    },

    get dimensions(): number {
      return getModelDimensions(model)
    },

    get model(): string {
      return model
    },
  }
}

// =============================================================================
// AI SDK Adapter
// =============================================================================

/**
 * AI SDK embed function interface
 * Compatible with Vercel AI SDK's embed function
 */
export interface AISDKEmbedFunction {
  (options: {
    model: unknown
    value: string
  }): Promise<{
    embedding: number[]
  }>
}

/**
 * AI SDK embedMany function interface
 * Compatible with Vercel AI SDK's embedMany function
 */
export interface AISDKEmbedManyFunction {
  (options: {
    model: unknown
    values: string[]
  }): Promise<{
    embeddings: number[][]
  }>
}

/**
 * Options for AI SDK embedding provider
 */
export interface AISDKProviderOptions {
  /** The embed function from AI SDK */
  embed: AISDKEmbedFunction
  /** The embedMany function from AI SDK (optional, falls back to multiple embed calls) */
  embedMany?: AISDKEmbedManyFunction
  /** The model to use */
  model: unknown
  /** Dimensions of the model's embeddings */
  dimensions: number
  /** Model identifier string */
  modelId?: string
}

/**
 * Create an embedding provider using Vercel AI SDK
 *
 * @param options - Provider options including embed function and model
 * @returns EmbeddingProvider instance
 *
 * @example
 * ```typescript
 * import { embed, embedMany } from 'ai'
 * import { openai } from '@ai-sdk/openai'
 *
 * const provider = createAISDKProvider({
 *   embed,
 *   embedMany,
 *   model: openai.embedding('text-embedding-3-small'),
 *   dimensions: 1536,
 *   modelId: 'text-embedding-3-small'
 * })
 * ```
 */
export function createAISDKProvider(options: AISDKProviderOptions): EmbeddingProvider {
  const { embed, embedMany, model, dimensions, modelId } = options

  return {
    async embed(text: string, _opts?: EmbedTextOptions): Promise<number[]> {
      const result = await embed({
        model,
        value: text,
      })
      return result.embedding
    },

    async embedBatch(texts: string[], _opts?: EmbedTextOptions): Promise<number[][]> {
      if (embedMany) {
        const result = await embedMany({
          model,
          values: texts,
        })
        return result.embeddings
      }

      // Fallback to sequential embedding if embedMany not provided
      const results: number[][] = []
      for (const text of texts) {
        const result = await embed({
          model,
          value: text,
        })
        results.push(result.embedding)
      }
      return results
    },

    get dimensions(): number {
      return dimensions
    },

    get model(): string {
      return modelId ?? 'ai-sdk-model'
    },
  }
}

// =============================================================================
// Custom Function Adapter
// =============================================================================

/**
 * Options for custom embedding function provider
 */
export interface CustomProviderOptions {
  /** Function to generate a single embedding */
  embed: (text: string) => Promise<number[]>
  /** Function to generate batch embeddings (optional) */
  embedBatch?: (texts: string[]) => Promise<number[][]>
  /** Dimensions of the embeddings */
  dimensions: number
  /** Model identifier */
  model?: string
}

/**
 * Create an embedding provider from custom functions
 *
 * @param options - Provider options
 * @returns EmbeddingProvider instance
 *
 * @example
 * ```typescript
 * // Using a custom embedding API
 * const provider = createCustomProvider({
 *   embed: async (text) => {
 *     const response = await fetch('/api/embed', {
 *       method: 'POST',
 *       body: JSON.stringify({ text })
 *     })
 *     const { vector } = await response.json()
 *     return vector
 *   },
 *   dimensions: 768,
 *   model: 'custom-model'
 * })
 * ```
 */
export function createCustomProvider(options: CustomProviderOptions): EmbeddingProvider {
  const { embed, embedBatch, dimensions, model } = options

  return {
    async embed(text: string, _opts?: EmbedTextOptions): Promise<number[]> {
      return embed(text)
    },

    async embedBatch(texts: string[], _opts?: EmbedTextOptions): Promise<number[][]> {
      if (embedBatch) {
        return embedBatch(texts)
      }

      // Fallback to sequential embedding
      const results: number[][] = []
      for (const text of texts) {
        results.push(await embed(text))
      }
      return results
    },

    get dimensions(): number {
      return dimensions
    },

    get model(): string {
      return model ?? 'custom'
    },
  }
}

// =============================================================================
// Re-exports for convenience
// =============================================================================

export { DEFAULT_MODEL, DEFAULT_DIMENSIONS }
