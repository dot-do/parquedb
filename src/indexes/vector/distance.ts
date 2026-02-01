/**
 * Distance Functions for Vector Index
 *
 * Provides distance/similarity metrics for vector search.
 */

/**
 * Cosine distance between two vectors.
 * Returns 0 for identical vectors, 1 for orthogonal, 2 for opposite.
 *
 * cosine_distance = 1 - cosine_similarity
 *
 * @param a - First vector
 * @param b - Second vector
 * @returns Distance (0 = identical, 1 = orthogonal, 2 = opposite)
 */
export function cosineDistance(a: number[], b: number[]): number {
  let dotProduct = 0
  let normA = 0
  let normB = 0

  for (let i = 0; i < a.length; i++) {
    const ai = a[i]!
    const bi = b[i]!
    dotProduct += ai * bi
    normA += ai * ai
    normB += bi * bi
  }

  // Handle zero vectors
  if (normA === 0 || normB === 0) {
    return normA === 0 && normB === 0 ? 0 : 1
  }

  const similarity = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB))
  // Clamp to [-1, 1] to handle floating point errors
  const clampedSimilarity = Math.max(-1, Math.min(1, similarity))

  // Distance = 1 - similarity (range: [0, 2])
  return 1 - clampedSimilarity
}

/**
 * Euclidean distance between two vectors.
 *
 * @param a - First vector
 * @param b - Second vector
 * @returns Euclidean distance (L2 norm)
 */
export function euclideanDistance(a: number[], b: number[]): number {
  let sum = 0

  for (let i = 0; i < a.length; i++) {
    const diff = a[i]! - b[i]!
    sum += diff * diff
  }

  return Math.sqrt(sum)
}

/**
 * Squared Euclidean distance (faster, avoids sqrt).
 *
 * @param a - First vector
 * @param b - Second vector
 * @returns Squared Euclidean distance
 */
export function euclideanDistanceSquared(a: number[], b: number[]): number {
  let sum = 0

  for (let i = 0; i < a.length; i++) {
    const diff = a[i]! - b[i]!
    sum += diff * diff
  }

  return sum
}

/**
 * Dot product distance (negative dot product for use with min-heaps).
 * Higher dot product = more similar, so we negate for distance.
 *
 * @param a - First vector
 * @param b - Second vector
 * @returns Negative dot product
 */
export function dotProductDistance(a: number[], b: number[]): number {
  let dotProduct = 0

  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i]! * b[i]!
  }

  // Return negative because higher dot product means more similar
  return -dotProduct
}

/**
 * Get the distance function for a given metric
 *
 * @param metric - Distance metric name
 * @returns Distance function
 */
export function getDistanceFunction(
  metric: 'cosine' | 'euclidean' | 'dot'
): (a: number[], b: number[]) => number {
  switch (metric) {
    case 'cosine':
      return cosineDistance
    case 'euclidean':
      return euclideanDistance
    case 'dot':
      return dotProductDistance
    default:
      return cosineDistance
  }
}

/**
 * Convert distance to similarity score (0-1 range)
 *
 * @param distance - Distance value
 * @param metric - Distance metric used
 * @returns Similarity score (0 = dissimilar, 1 = identical)
 */
export function distanceToScore(
  distance: number,
  metric: 'cosine' | 'euclidean' | 'dot'
): number {
  switch (metric) {
    case 'cosine':
      // Cosine distance is in [0, 2], similarity is 1 - distance
      return 1 - distance
    case 'euclidean':
      // Euclidean distance can be any positive value
      // Use exponential decay for similarity
      return Math.exp(-distance)
    case 'dot':
      // Dot distance is negative dot product
      // Convert back to dot product and normalize (assumes unit vectors)
      return -distance
    default:
      return 1 - distance
  }
}

/**
 * Normalize a vector to unit length
 *
 * @param v - Vector to normalize
 * @returns Normalized vector
 */
export function normalize(v: number[]): number[] {
  let norm = 0
  for (let i = 0; i < v.length; i++) {
    norm += v[i]! * v[i]!
  }
  norm = Math.sqrt(norm)

  if (norm === 0) {
    return v.slice()
  }

  const result = new Array(v.length)
  for (let i = 0; i < v.length; i++) {
    result[i] = v[i]! / norm
  }

  return result
}
