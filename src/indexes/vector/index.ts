/**
 * Vector Index Exports for ParqueDB
 *
 * Provides HNSW-based vector similarity search functionality
 * with support for hybrid search (vector + metadata filtering).
 *
 * Memory Management Features:
 * - LRU cache with configurable max nodes and max bytes limits
 * - Product Quantization (PQ) for vector compression
 * - Automatic eviction of least recently used nodes
 */

export { VectorIndex, type VectorIndexMemoryOptions } from './hnsw'
export {
  cosineDistance,
  euclideanDistance,
  euclideanDistanceSquared,
  dotProductDistance,
  getDistanceFunction,
  distanceToScore,
  normalize,
} from './distance'
export { LRUCache, type LRUCacheOptions } from './lru-cache'
export {
  ProductQuantizer,
  type PQCodebook,
  type PQCode,
  type DistanceTable,
} from './product-quantization'

// Re-export hybrid search types for convenience
export type {
  HybridSearchOptions,
  HybridSearchResult,
  HybridSearchStrategy,
} from '../types'
