/**
 * Embeddings Module for ParqueDB
 *
 * Provides embedding generation for vector similarity search.
 * Supports:
 * - Cloudflare Workers AI (for Workers environments)
 * - Vercel AI SDK (for Node.js environments with OpenAI, Cohere, etc.)
 */

// Workers AI Embeddings (Cloudflare Workers)
export {
  WorkersAIEmbeddings,
  createEmbeddings,
  getModelDimensions,
  DEFAULT_MODEL,
  DEFAULT_DIMENSIONS,
  EMBEDDING_MODELS,
  type AIBinding,
  type EmbeddingModelConfig,
  type EmbedOptions,
} from './workers-ai'

// Vercel AI SDK Embeddings (Node.js)
export {
  AISDKEmbeddings,
  createAISDKEmbeddings,
  getAISDKModelDimensions,
  listAISDKModels,
  AI_SDK_MODELS,
  DEFAULT_AI_SDK_DIMENSIONS,
  type EmbeddingProvider,
  type AISDKProvider,
  type AISDKEmbeddingsConfig,
  type AISDKEmbedOptions,
} from './ai-sdk'

// Auto-Embedding Utilities
export {
  processEmbedOperator,
  autoEmbedFields,
  hasEmbedOperator,
  extractEmbedOperator,
  buildAutoEmbedConfig,
  getNestedValue,
  setNestedValue,
  type AutoEmbedFieldConfig,
  type AutoEmbedConfig,
  type ProcessEmbeddingsOptions,
} from './auto-embed'

// Provider Interface and Caching
export {
  QueryEmbeddingCache,
  CachingEmbeddingProvider,
  withQueryCache,
  type EmbeddingProvider as GenericEmbeddingProvider,
  type EmbedTextOptions,
  type EmbeddingResult,
  type QueryEmbeddingCacheOptions,
  type CachingEmbeddingProviderOptions,
} from './provider'

// Provider Adapters
export {
  createWorkersAIProvider,
  createAISDKProvider,
  createCustomProvider,
  type WorkersAIProviderOptions,
  type AISDKProviderOptions,
  type AISDKEmbedFunction,
  type AISDKEmbedManyFunction,
  type CustomProviderOptions,
} from './adapters'

// Background Embedding Generation
export {
  EmbeddingQueue,
  createEmbeddingQueue,
  configureBackgroundEmbeddings,
  BackgroundEmbeddingConfigBuilder,
  type BackgroundEmbeddingConfig,
  type EmbeddingQueueItem,
  type QueueProcessingResult,
  type QueueStats,
  type EntityLoader,
  type EntityUpdater,
} from './background'
