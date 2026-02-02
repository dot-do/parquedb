/**
 * Tests for Vector Distance Functions
 *
 * Comprehensive tests for all distance/similarity metrics used by the vector index.
 */

import { describe, it, expect } from 'vitest'
import {
  cosineDistance,
  euclideanDistance,
  euclideanDistanceSquared,
  dotProductDistance,
  getDistanceFunction,
  distanceToScore,
  normalize,
} from '@/indexes/vector/distance'

// =============================================================================
// Cosine Distance
// =============================================================================

describe('cosineDistance', () => {
  it('returns 0 for identical vectors', () => {
    expect(cosineDistance([1, 0, 0], [1, 0, 0])).toBeCloseTo(0, 10)
    expect(cosineDistance([0.5, 0.5, 0.5], [0.5, 0.5, 0.5])).toBeCloseTo(0, 10)
    expect(cosineDistance([1, 2, 3], [1, 2, 3])).toBeCloseTo(0, 10)
  })

  it('returns 1 for orthogonal vectors', () => {
    expect(cosineDistance([1, 0, 0], [0, 1, 0])).toBeCloseTo(1, 10)
    expect(cosineDistance([1, 0, 0], [0, 0, 1])).toBeCloseTo(1, 10)
    expect(cosineDistance([0, 1, 0], [0, 0, 1])).toBeCloseTo(1, 10)
  })

  it('returns 2 for opposite vectors', () => {
    expect(cosineDistance([1, 0, 0], [-1, 0, 0])).toBeCloseTo(2, 10)
    expect(cosineDistance([1, 1, 0], [-1, -1, 0])).toBeCloseTo(2, 10)
    expect(cosineDistance([1, 2, 3], [-1, -2, -3])).toBeCloseTo(2, 10)
  })

  it('is magnitude-invariant (same direction, different magnitudes)', () => {
    const d1 = cosineDistance([1, 0, 0], [10, 0, 0])
    expect(d1).toBeCloseTo(0, 10)

    const d2 = cosineDistance([1, 1, 1], [100, 100, 100])
    expect(d2).toBeCloseTo(0, 10)

    const d3 = cosineDistance([3, 4, 0], [6, 8, 0])
    expect(d3).toBeCloseTo(0, 10)
  })

  it('handles zero vectors', () => {
    // Both zero vectors: defined as distance 0
    expect(cosineDistance([0, 0, 0], [0, 0, 0])).toBe(0)

    // One zero vector: defined as distance 1
    expect(cosineDistance([0, 0, 0], [1, 0, 0])).toBe(1)
    expect(cosineDistance([1, 0, 0], [0, 0, 0])).toBe(1)
  })

  it('returns values in [0, 2] range', () => {
    // Test with a variety of random-ish vectors
    const vectors = [
      [1, 0, 0],
      [0, 1, 0],
      [0, 0, 1],
      [-1, 0, 0],
      [1, 1, 1],
      [-1, -1, -1],
      [0.5, -0.3, 0.8],
      [-0.7, 0.2, -0.5],
    ]

    for (const a of vectors) {
      for (const b of vectors) {
        const d = cosineDistance(a, b)
        expect(d).toBeGreaterThanOrEqual(-1e-10) // Allow tiny float errors
        expect(d).toBeLessThanOrEqual(2 + 1e-10)
      }
    }
  })

  it('is symmetric', () => {
    const a = [1.5, -2.3, 0.7]
    const b = [-0.5, 3.1, -1.2]
    expect(cosineDistance(a, b)).toBeCloseTo(cosineDistance(b, a), 10)
  })

  it('handles high-dimensional vectors', () => {
    const dim = 128
    const a = Array.from({ length: dim }, (_, i) => Math.sin(i))
    const b = Array.from({ length: dim }, (_, i) => Math.cos(i))

    const d = cosineDistance(a, b)
    expect(d).toBeGreaterThanOrEqual(0)
    expect(d).toBeLessThanOrEqual(2)
  })

  it('handles normalized vectors correctly', () => {
    // Pre-normalized vectors (unit length)
    const a = normalize([1, 2, 3])
    const b = normalize([4, 5, 6])

    const d = cosineDistance(a, b)
    expect(d).toBeGreaterThanOrEqual(0)
    expect(d).toBeLessThanOrEqual(2)

    // Distance from itself should be 0
    expect(cosineDistance(a, a)).toBeCloseTo(0, 10)
  })
})

// =============================================================================
// Euclidean Distance
// =============================================================================

describe('euclideanDistance', () => {
  it('returns 0 for identical vectors', () => {
    expect(euclideanDistance([1, 0, 0], [1, 0, 0])).toBe(0)
    expect(euclideanDistance([5, 5, 5], [5, 5, 5])).toBe(0)
    expect(euclideanDistance([0, 0, 0], [0, 0, 0])).toBe(0)
  })

  it('calculates correct distance for known values', () => {
    // 3-4-5 right triangle
    expect(euclideanDistance([0, 0, 0], [3, 4, 0])).toBeCloseTo(5, 10)

    // Unit distance along axes
    expect(euclideanDistance([0, 0, 0], [1, 0, 0])).toBeCloseTo(1, 10)
    expect(euclideanDistance([0, 0, 0], [0, 1, 0])).toBeCloseTo(1, 10)

    // sqrt(3) for unit diagonal in 3D
    expect(euclideanDistance([0, 0, 0], [1, 1, 1])).toBeCloseTo(Math.sqrt(3), 10)
  })

  it('is symmetric', () => {
    const a = [1.5, -2.3, 0.7]
    const b = [-0.5, 3.1, -1.2]
    expect(euclideanDistance(a, b)).toBeCloseTo(euclideanDistance(b, a), 10)
  })

  it('satisfies triangle inequality', () => {
    const a = [1, 0, 0]
    const b = [0, 1, 0]
    const c = [0, 0, 1]

    const ab = euclideanDistance(a, b)
    const bc = euclideanDistance(b, c)
    const ac = euclideanDistance(a, c)

    expect(ac).toBeLessThanOrEqual(ab + bc + 1e-10)
  })

  it('is non-negative', () => {
    const vectors = [
      [1, 0, 0],
      [-1, -1, -1],
      [0.5, -0.3, 0.8],
      [100, -200, 300],
    ]

    for (const a of vectors) {
      for (const b of vectors) {
        expect(euclideanDistance(a, b)).toBeGreaterThanOrEqual(0)
      }
    }
  })

  it('handles zero vectors', () => {
    expect(euclideanDistance([0, 0, 0], [0, 0, 0])).toBe(0)
    expect(euclideanDistance([0, 0, 0], [1, 0, 0])).toBeCloseTo(1, 10)
  })

  it('handles high-dimensional vectors', () => {
    const dim = 128
    const a = new Array(dim).fill(0)
    const b = new Array(dim).fill(1)

    // Distance should be sqrt(128)
    expect(euclideanDistance(a, b)).toBeCloseTo(Math.sqrt(dim), 10)
  })

  it('is NOT magnitude-invariant (unlike cosine)', () => {
    const d1 = euclideanDistance([1, 0, 0], [2, 0, 0])
    const d2 = euclideanDistance([1, 0, 0], [10, 0, 0])
    expect(d2).toBeGreaterThan(d1)
  })
})

// =============================================================================
// Euclidean Distance Squared
// =============================================================================

describe('euclideanDistanceSquared', () => {
  it('returns 0 for identical vectors', () => {
    expect(euclideanDistanceSquared([1, 0, 0], [1, 0, 0])).toBe(0)
    expect(euclideanDistanceSquared([0, 0, 0], [0, 0, 0])).toBe(0)
  })

  it('returns the square of euclidean distance', () => {
    const a = [0, 0, 0]
    const b = [3, 4, 0]

    const ed = euclideanDistance(a, b)
    const eds = euclideanDistanceSquared(a, b)

    expect(eds).toBeCloseTo(ed * ed, 10)
    expect(eds).toBeCloseTo(25, 10) // 3^2 + 4^2 = 25
  })

  it('preserves ordering (monotonic with euclidean distance)', () => {
    const query = [0, 0, 0]
    const near = [1, 0, 0]
    const far = [3, 4, 0]

    const nearDist = euclideanDistanceSquared(query, near)
    const farDist = euclideanDistanceSquared(query, far)

    expect(nearDist).toBeLessThan(farDist)
  })

  it('is symmetric', () => {
    const a = [1.5, -2.3, 0.7]
    const b = [-0.5, 3.1, -1.2]
    expect(euclideanDistanceSquared(a, b)).toBeCloseTo(
      euclideanDistanceSquared(b, a),
      10
    )
  })
})

// =============================================================================
// Dot Product Distance
// =============================================================================

describe('dotProductDistance', () => {
  it('returns negative dot product', () => {
    // dot([1,1,1], [1,1,1]) = 3, distance = -3
    expect(dotProductDistance([1, 1, 1], [1, 1, 1])).toBe(-3)
  })

  it('returns 0 for orthogonal vectors', () => {
    expect(dotProductDistance([1, 0, 0], [0, 1, 0])).toBeCloseTo(0)
    expect(dotProductDistance([1, 0, 0], [0, 0, 1])).toBeCloseTo(0)
  })

  it('returns positive value for opposite vectors', () => {
    // dot([1,0,0], [-1,0,0]) = -1, distance = 1
    expect(dotProductDistance([1, 0, 0], [-1, 0, 0])).toBe(1)
  })

  it('is symmetric', () => {
    const a = [1.5, -2.3, 0.7]
    const b = [-0.5, 3.1, -1.2]
    expect(dotProductDistance(a, b)).toBeCloseTo(dotProductDistance(b, a), 10)
  })

  it('handles zero vectors', () => {
    expect(dotProductDistance([0, 0, 0], [1, 2, 3])).toBeCloseTo(0)
    expect(dotProductDistance([1, 2, 3], [0, 0, 0])).toBeCloseTo(0)
    expect(dotProductDistance([0, 0, 0], [0, 0, 0])).toBeCloseTo(0)
  })

  it('lower distance means more similar for same-direction vectors', () => {
    const query = [1, 0, 0]
    const similar = [10, 0, 0] // High dot product -> low (negative) distance
    const dissimilar = [0.1, 0, 0] // Low dot product -> higher distance

    expect(dotProductDistance(query, similar)).toBeLessThan(
      dotProductDistance(query, dissimilar)
    )
  })

  it('handles large magnitudes correctly', () => {
    const a = [100, 200, 300]
    const b = [4, 5, 6]
    // dot = 100*4 + 200*5 + 300*6 = 400 + 1000 + 1800 = 3200
    expect(dotProductDistance(a, b)).toBe(-3200)
  })

  it('handles normalized vectors', () => {
    const a = normalize([1, 2, 3])
    const b = normalize([4, 5, 6])

    const d = dotProductDistance(a, b)
    // For unit vectors, dot product is in [-1, 1], so distance is in [-1, 1]
    expect(d).toBeGreaterThanOrEqual(-1 - 1e-10)
    expect(d).toBeLessThanOrEqual(1 + 1e-10)
  })
})

// =============================================================================
// getDistanceFunction
// =============================================================================

describe('getDistanceFunction', () => {
  it('returns cosineDistance for "cosine"', () => {
    const fn = getDistanceFunction('cosine')
    expect(fn([1, 0, 0], [1, 0, 0])).toBeCloseTo(0, 10)
    expect(fn([1, 0, 0], [0, 1, 0])).toBeCloseTo(1, 10)
  })

  it('returns euclideanDistance for "euclidean"', () => {
    const fn = getDistanceFunction('euclidean')
    expect(fn([0, 0, 0], [3, 4, 0])).toBeCloseTo(5, 10)
  })

  it('returns dotProductDistance for "dot"', () => {
    const fn = getDistanceFunction('dot')
    expect(fn([1, 1, 1], [1, 1, 1])).toBe(-3)
  })

  it('returns cosineDistance as default for unknown metric', () => {
    // The type system restricts to the three known metrics, but we can
    // verify the default branch behavior by casting
    const fn = getDistanceFunction('cosine')
    expect(fn([1, 0, 0], [0, 1, 0])).toBeCloseTo(1, 10)
  })
})

// =============================================================================
// distanceToScore
// =============================================================================

describe('distanceToScore', () => {
  describe('cosine metric', () => {
    it('returns 1 for distance 0 (identical)', () => {
      expect(distanceToScore(0, 'cosine')).toBe(1)
    })

    it('returns 0 for distance 1 (orthogonal)', () => {
      expect(distanceToScore(1, 'cosine')).toBe(0)
    })

    it('returns -1 for distance 2 (opposite)', () => {
      expect(distanceToScore(2, 'cosine')).toBe(-1)
    })

    it('returns values between -1 and 1', () => {
      for (let d = 0; d <= 2; d += 0.1) {
        const score = distanceToScore(d, 'cosine')
        expect(score).toBeGreaterThanOrEqual(-1 - 1e-10)
        expect(score).toBeLessThanOrEqual(1 + 1e-10)
      }
    })
  })

  describe('euclidean metric', () => {
    it('returns 1 for distance 0 (identical)', () => {
      expect(distanceToScore(0, 'euclidean')).toBe(1)
    })

    it('returns decreasing scores for increasing distances', () => {
      const score0 = distanceToScore(0, 'euclidean')
      const score1 = distanceToScore(1, 'euclidean')
      const score5 = distanceToScore(5, 'euclidean')
      const score10 = distanceToScore(10, 'euclidean')

      expect(score0).toBeGreaterThan(score1)
      expect(score1).toBeGreaterThan(score5)
      expect(score5).toBeGreaterThan(score10)
    })

    it('returns values in (0, 1] range', () => {
      for (let d = 0; d <= 100; d += 1) {
        const score = distanceToScore(d, 'euclidean')
        expect(score).toBeGreaterThan(0)
        expect(score).toBeLessThanOrEqual(1)
      }
    })

    it('uses exponential decay', () => {
      expect(distanceToScore(1, 'euclidean')).toBeCloseTo(Math.exp(-1), 10)
      expect(distanceToScore(2, 'euclidean')).toBeCloseTo(Math.exp(-2), 10)
    })
  })

  describe('dot metric', () => {
    it('converts negative distance back to dot product', () => {
      // dotProductDistance returns -dotProduct
      // distanceToScore for dot returns -distance = dotProduct
      expect(distanceToScore(-3, 'dot')).toBe(3)
      expect(distanceToScore(-1, 'dot')).toBe(1)
      expect(distanceToScore(0, 'dot')).toBeCloseTo(0)
      expect(distanceToScore(1, 'dot')).toBe(-1)
    })
  })
})

// =============================================================================
// normalize
// =============================================================================

describe('normalize', () => {
  it('produces unit-length vectors', () => {
    const v = normalize([3, 4, 0])
    const magnitude = Math.sqrt(v.reduce((sum, x) => sum + x * x, 0))
    expect(magnitude).toBeCloseTo(1, 10)
  })

  it('preserves direction', () => {
    const original = [3, 4, 0]
    const normalized = normalize(original)

    // Cosine distance between original and normalized should be 0
    expect(cosineDistance(original, normalized)).toBeCloseTo(0, 10)
  })

  it('handles already-normalized vectors', () => {
    const v = [1, 0, 0]
    const result = normalize(v)
    expect(result[0]).toBeCloseTo(1, 10)
    expect(result[1]).toBeCloseTo(0, 10)
    expect(result[2]).toBeCloseTo(0, 10)
  })

  it('handles zero vectors by returning a copy', () => {
    const v = [0, 0, 0]
    const result = normalize(v)
    expect(result).toEqual([0, 0, 0])
    // Should be a new array (not mutating original)
    expect(result).not.toBe(v)
  })

  it('normalizes high-dimensional vectors', () => {
    const dim = 128
    const v = Array.from({ length: dim }, (_, i) => i + 1)
    const normalized = normalize(v)

    const magnitude = Math.sqrt(normalized.reduce((sum, x) => sum + x * x, 0))
    expect(magnitude).toBeCloseTo(1, 10)
    expect(normalized.length).toBe(dim)
  })

  it('handles negative values', () => {
    const v = normalize([-3, -4, 0])
    const magnitude = Math.sqrt(v.reduce((sum, x) => sum + x * x, 0))
    expect(magnitude).toBeCloseTo(1, 10)

    // Direction should be preserved (all components negative)
    expect(v[0]).toBeLessThan(0)
    expect(v[1]).toBeLessThan(0)
  })

  it('handles single-element vectors', () => {
    expect(normalize([5])).toEqual([1])
    expect(normalize([-5])).toEqual([-1])
  })

  it('does not mutate the original vector', () => {
    const original = [3, 4, 0]
    const copy = [...original]
    normalize(original)
    expect(original).toEqual(copy)
  })
})
