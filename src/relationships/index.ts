/**
 * Relationships Module
 *
 * Provides utilities for working with entity relationships in ParqueDB.
 *
 * @packageDocumentation
 */

export {
  RelationshipBatchLoader,
  createBatchLoader,
  type BatchLoadRequest,
  type BatchLoadResult,
  type BatchLoaderOptions,
  type BatchLoaderDB,
} from './batch-loader'

export {
  type MatchMode,
  type RelationshipMetadata,
  type ShreddedRelationshipFields,
  type RelationshipFilter,
  type RelationshipQueryOptions,
  type RelationshipQueryResult,
  type RelationshipWithMetadata,
  isMatchMode,
  isValidSimilarity,
  extractShreddedFields,
  mergeShreddedFields,
  validateSimilarity,
  validateMatchModeConsistency,
} from './types'
