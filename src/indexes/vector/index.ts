/**
 * Vector Index Exports for ParqueDB
 *
 * Provides HNSW-based vector similarity search functionality.
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
