/**
 * Embeddings Module for ParqueDB
 *
 * Provides embedding generation for vector similarity search.
 * Currently supports Cloudflare Workers AI.
 */

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
