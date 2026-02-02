/**
 * Index Module Exports
 *
 * ParqueDB secondary indexes and full-text search
 */

// Types
export * from './types'

// Errors
export * from './errors'

// Index Manager
export {
  IndexManager,
  type SelectedIndex,
  type IndexManagerOptions,
  type IndexManagerErrorHandler,
} from './manager'

// Secondary Indexes
export * from './secondary'

// Full-Text Search
export * from './fts'

// Bloom Filters
export * from './bloom'

// Vector Indexes
export * from './vector'

// Compact Encoding
export * from './encoding'
