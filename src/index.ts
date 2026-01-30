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
// Version
// =============================================================================

export const VERSION = '0.1.0'
