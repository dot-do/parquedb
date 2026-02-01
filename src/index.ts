/**
 * ParqueDB - A Parquet-based database for Node.js, browsers, and Cloudflare Workers
 *
 * @packageDocumentation
 */

// =============================================================================
// Main Classes
// =============================================================================

export { ParqueDB, type ParqueDBConfig } from './ParqueDB'
export { Collection } from './Collection'

// =============================================================================
// Types
// =============================================================================

export * from './types'

// =============================================================================
// Storage Backends
// =============================================================================

export {
  MemoryBackend,
  FsBackend,
  // R2Backend,
  // FsxBackend,
} from './storage'

// =============================================================================
// Schema
// =============================================================================

export {
  parseSchema,
  parseFieldType,
  parseRelation,
  isRelationString,
} from './schema'

// =============================================================================
// Client (for RPC) - temporarily disabled for Worker build
// =============================================================================

// export {
//   ParqueDBClient,
//   createParqueDBClient,
//   type ParqueDBClientOptions,
// } from './client'

// =============================================================================
// Query Utilities
// =============================================================================

export {
  matchesFilter,
  createPredicate,
} from './query/filter'

export {
  applyUpdate,
} from './query/update'

// =============================================================================
// Indexes
// =============================================================================

export {
  // Index Manager
  IndexManager,
  // Secondary Indexes
  HashIndex,
  SSTIndex,
  // Full-Text Search
  FTSIndex,
  InvertedIndex,
  BM25Scorer,
  tokenize,
  tokenizeQuery,
  porterStem,
  // Key Encoding
  encodeKey,
  decodeKey,
  compareKeys,
  hashKey,
  // Types
  type IndexDefinition,
  type IndexMetadata,
  type IndexStats,
  type IndexLookupResult,
  type RangeQuery,
  type FTSSearchOptions,
  type FTSSearchResult,
} from './indexes'

// =============================================================================
// Events (CDC / Time-Travel)
// =============================================================================

export {
  // Types
  type Event,
  type EventBatch,
  type EventSegment,
  type EventManifest,
  type EventWriterConfig,
  type DatasetConfig,
  type TimeTravelOptions,
  // Utilities
  isRelationshipTarget,
  parseEntityTarget,
  parseRelTarget,
  entityTarget,
  relTarget,
} from './events'

// =============================================================================
// Version
// =============================================================================

export const VERSION = '0.1.0'
