/**
 * Query Patterns Index
 *
 * Re-exports all dataset-specific query patterns for benchmarking.
 */

// =============================================================================
// Imports for allPatterns
// =============================================================================

import { blogPatterns as _blogPatterns } from './blog'
import { imdbPatterns as _imdbPatterns } from './imdb'
import { onetPatterns as _onetPatterns } from './onet'
import { unspscPatterns as _unspscPatterns } from './unspsc'
import { ecommercePatterns as _ecommercePatterns } from './ecommerce'

// =============================================================================
// Blog Dataset Patterns
// =============================================================================

export {
  // Types
  type Author,
  type Post,
  type Comment,
  type Tag,
  type QueryPattern as BlogQueryPattern,
  type LocalQueryPattern,
  type QueryCategory as BlogQueryCategory,

  // HTTP patterns (for deployed workers)
  blogPatterns,

  // Local patterns (for FsBackend testing)
  blogLocalPatterns,

  // Test data generation
  generateBlogTestData,

  // Utilities
  getPatternByName as getBlogPatternByName,
  getLocalPatternByName,
  getPatternsByCategory as getBlogPatternsByCategory,
  getLocalPatternsByCategory,
  blogPatternSummary,
} from './blog'

// =============================================================================
// IMDB Dataset Patterns
// =============================================================================

export {
  type QueryPattern as IMDBQueryPattern,
  type QueryCategory as IMDBQueryCategory,
  imdbPatterns,
  getPatternsByCategory as getIMDBPatternsByCategory,
  getPatternByName as getIMDBPatternByName,
  getPointLookupPatterns,
  getRelationshipPatterns,
  getFtsPatterns,
  getAggregationPatterns,
  getCompoundPatterns,
  getFilteredPatterns,
  patternSummary as imdbPatternSummary,
  BASE_URL as IMDB_BASE_URL,
} from './imdb'

// =============================================================================
// O*NET Dataset Patterns
// =============================================================================

export {
  type QueryPattern as ONetQueryPattern,
  onetPatterns,
} from './onet'

// =============================================================================
// UNSPSC Dataset Patterns
// =============================================================================

export {
  type QueryPattern as UNSPSCQueryPattern,
  type QueryCategory as UNSPSCQueryCategory,
  unspscPatterns,
  getPatternsByCategory as getUNSPSCPatternsByCategory,
} from './unspsc'

// =============================================================================
// E-commerce Dataset Patterns
// =============================================================================

export {
  type QueryPattern as EcommerceQueryPattern,
  type PatternCategory as EcommercePatternCategory,
  type Product,
  type Category,
  type Order,
  type OrderItem,
  type Customer,
  ecommercePatterns,
  getPatternsByCategory as getEcommercePatternsByCategory,
  getPatternsWithinTarget,
  generateSampleProducts,
  generateSampleOrders,
  generateSampleCustomers,
} from './ecommerce'

// =============================================================================
// Unified QueryPattern Type
// =============================================================================

/**
 * Unified query pattern type for all datasets
 */
export interface QueryPattern {
  /** Human-readable pattern name */
  name: string
  /** Query category */
  category: string
  /** Target latency in milliseconds */
  targetMs: number
  /** Description of the pattern */
  description?: string
  /** Query function that returns a Response */
  query: (baseUrl?: string) => Promise<Response>
}

/**
 * All dataset patterns indexed by dataset name
 */
export const allPatterns: Record<string, QueryPattern[]> = {
  imdb: _imdbPatterns as QueryPattern[],
  onet: _onetPatterns as QueryPattern[],
  unspsc: _unspscPatterns as QueryPattern[],
  blog: _blogPatterns as QueryPattern[],
  ecommerce: _ecommercePatterns as QueryPattern[],
}
