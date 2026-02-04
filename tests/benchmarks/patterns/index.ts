/**
 * Query Patterns Index
 *
 * Re-exports all dataset-specific query patterns for benchmarking.
 */

// Blog dataset patterns
export {
  // Types
  type Author,
  type Post,
  type Comment,
  type Tag,
  type QueryPattern,
  type LocalQueryPattern,
  type QueryCategory,

  // HTTP patterns (for deployed workers)
  blogPatterns,

  // Local patterns (for FsBackend testing)
  blogLocalPatterns,

  // Test data generation
  generateBlogTestData,

  // Utilities
  getPatternByName,
  getLocalPatternByName,
  getPatternsByCategory,
  getLocalPatternsByCategory,
  blogPatternSummary,
} from './blog'

// Future dataset patterns will be exported here:
// export * from './imdb'
// export * from './onet'
// export * from './unspsc'
// export * from './ecommerce'
