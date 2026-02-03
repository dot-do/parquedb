/**
 * ai-database Integration Module
 *
 * This module exports the ParqueDB adapter for ai-database,
 * allowing ParqueDB to be used as a backend for ai-database applications.
 *
 * @packageDocumentation
 */

export {
  // Main adapter class and factory
  ParqueDBAdapter,
  createParqueDBProvider,

  // Types (re-exported from adapter for convenience)
  type DBProvider,
  type DBProviderExtended,
  type Transaction,
  type DBEvent,
  type DBAction,
  type DBArtifact,
  type ListOptions,
  type SearchOptions,
  type SemanticSearchOptions,
  type HybridSearchOptions,
  type SemanticSearchResult,
  type HybridSearchResult,
  type RelationMetadata,
  type CreateEventOptions,
  type CreateActionOptions,
  type EmbeddingsConfig,
} from './adapter'
