/**
 * Vector Index Exports for ParqueDB
 *
 * Provides HNSW-based vector similarity search functionality
 * with support for hybrid search (vector + metadata filtering).
 */

export { VectorIndex } from './hnsw'
export {
  cosineDistance,
  euclideanDistance,
  euclideanDistanceSquared,
  dotProductDistance,
  getDistanceFunction,
  distanceToScore,
  normalize,
} from './distance'

// Re-export hybrid search types for convenience
export type {
  HybridSearchOptions,
  HybridSearchResult,
  HybridSearchStrategy,
} from '../types'
