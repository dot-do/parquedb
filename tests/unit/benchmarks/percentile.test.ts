/**
 * Percentile Calculation Tests
 *
 * Tests for bounds checking on percentile array access across benchmark utilities.
 * Ensures proper handling of edge cases:
 * - Empty arrays
 * - Single-element arrays
 * - Out-of-bounds index access (p0, p100)
 */

import { describe, it, expect } from 'vitest'
import { calculateLatencyStats } from '../../../tests/benchmarks/types'
import { calculateStats as calculateBenchmarkStats } from '../../../tests/benchmarks/setup'

// =============================================================================
// LatencyStats Tests (from tests/benchmarks/types.ts)
// =============================================================================

describe('calculateLatencyStats', () => {
  describe('empty array handling', () => {
    it('should return all zeros for empty array', () => {
      const stats = calculateLatencyStats([])

      expect(stats.min).toBe(0)
      expect(stats.max).toBe(0)
      expect(stats.mean).toBe(0)
      expect(stats.median).toBe(0)
      expect(stats.p50).toBe(0)
      expect(stats.p75).toBe(0)
      expect(stats.p90).toBe(0)
      expect(stats.p95).toBe(0)
      expect(stats.p99).toBe(0)
      expect(stats.stdDev).toBe(0)
    })
  })

  describe('single element array', () => {
    it('should return the same value for all percentiles', () => {
      const stats = calculateLatencyStats([42])

      expect(stats.min).toBe(42)
      expect(stats.max).toBe(42)
      expect(stats.mean).toBe(42)
      expect(stats.median).toBe(42)
      expect(stats.p50).toBe(42)
      expect(stats.p95).toBe(42)
      expect(stats.p99).toBe(42)
      expect(stats.stdDev).toBe(0) // No variance with single element
    })
  })

  describe('small arrays (fewer than 100 elements)', () => {
    it('should handle 2-element array', () => {
      const stats = calculateLatencyStats([10, 20])

      expect(stats.min).toBe(10)
      expect(stats.max).toBe(20)
      expect(stats.mean).toBe(15)
      // p95 and p99 should clamp to valid indices
      expect(stats.p95).toBeLessThanOrEqual(20)
      expect(stats.p99).toBeLessThanOrEqual(20)
    })

    it('should handle 5-element array', () => {
      const stats = calculateLatencyStats([1, 2, 3, 4, 5])

      expect(stats.min).toBe(1)
      expect(stats.max).toBe(5)
      expect(stats.mean).toBe(3)
      // All percentiles should be within bounds
      expect(stats.p50).toBeGreaterThanOrEqual(1)
      expect(stats.p50).toBeLessThanOrEqual(5)
      expect(stats.p95).toBeGreaterThanOrEqual(1)
      expect(stats.p95).toBeLessThanOrEqual(5)
      expect(stats.p99).toBeGreaterThanOrEqual(1)
      expect(stats.p99).toBeLessThanOrEqual(5)
    })
  })

  describe('large arrays', () => {
    it('should calculate correct percentiles for 100-element array', () => {
      const data = Array.from({ length: 100 }, (_, i) => i + 1)
      const stats = calculateLatencyStats(data)

      expect(stats.min).toBe(1)
      expect(stats.max).toBe(100)
      expect(stats.mean).toBe(50.5)
      expect(stats.p50).toBeGreaterThanOrEqual(50)
      expect(stats.p50).toBeLessThanOrEqual(51)
      expect(stats.p95).toBeGreaterThanOrEqual(95)
      expect(stats.p99).toBeGreaterThanOrEqual(99)
    })
  })

  describe('unsorted input', () => {
    it('should handle unsorted arrays correctly', () => {
      const stats = calculateLatencyStats([50, 10, 30, 90, 70])

      expect(stats.min).toBe(10)
      expect(stats.max).toBe(90)
    })
  })
})

// =============================================================================
// BenchmarkStats Tests (from tests/benchmarks/setup.ts)
// =============================================================================

describe('calculateStats (BenchmarkStats)', () => {
  describe('empty array handling', () => {
    it('should return all zeros for empty array', () => {
      const stats = calculateBenchmarkStats('test', [])

      expect(stats.name).toBe('test')
      expect(stats.iterations).toBe(0)
      expect(stats.totalTime).toBe(0)
      expect(stats.mean).toBe(0)
      expect(stats.median).toBe(0)
      expect(stats.min).toBe(0)
      expect(stats.max).toBe(0)
      expect(stats.stdDev).toBe(0)
      expect(stats.p95).toBe(0)
      expect(stats.p99).toBe(0)
      expect(stats.opsPerSecond).toBe(0)
    })
  })

  describe('single element array', () => {
    it('should return correct stats for single value', () => {
      const stats = calculateBenchmarkStats('test', [100])

      expect(stats.name).toBe('test')
      expect(stats.iterations).toBe(1)
      expect(stats.totalTime).toBe(100)
      expect(stats.mean).toBe(100)
      expect(stats.median).toBe(100)
      expect(stats.min).toBe(100)
      expect(stats.max).toBe(100)
      expect(stats.p95).toBe(100)
      expect(stats.p99).toBe(100)
      expect(stats.stdDev).toBe(0)
    })
  })

  describe('normal operations', () => {
    it('should calculate correct stats for typical benchmark data', () => {
      const durations = [10, 12, 11, 13, 9, 14, 8, 15, 7, 16]
      const stats = calculateBenchmarkStats('benchmark', durations)

      expect(stats.iterations).toBe(10)
      expect(stats.totalTime).toBe(115) // sum of all
      expect(stats.mean).toBe(11.5)
      expect(stats.min).toBe(7)
      expect(stats.max).toBe(16)
      // p95 and p99 should be within valid range
      expect(stats.p95).toBeGreaterThanOrEqual(7)
      expect(stats.p95).toBeLessThanOrEqual(16)
      expect(stats.p99).toBeGreaterThanOrEqual(7)
      expect(stats.p99).toBeLessThanOrEqual(16)
    })
  })
})

// =============================================================================
// Edge Case Tests for Index Bounds
// =============================================================================

describe('Index Bounds Safety', () => {
  it('should not access negative indices', () => {
    // With very small arrays, the formula (p/100) * n - 1 could be negative
    // for p values like 0 or with n=1
    const stats = calculateLatencyStats([42])

    // All values should be defined (not undefined from array[-1] access)
    expect(stats.p50).toBeDefined()
    expect(stats.p95).toBeDefined()
    expect(stats.p99).toBeDefined()
    expect(typeof stats.p50).toBe('number')
    expect(typeof stats.p95).toBe('number')
    expect(typeof stats.p99).toBe('number')
    expect(Number.isNaN(stats.p50)).toBe(false)
    expect(Number.isNaN(stats.p95)).toBe(false)
    expect(Number.isNaN(stats.p99)).toBe(false)
  })

  it('should not access indices beyond array length', () => {
    // For p=100 on array of length n, index would be n which is out of bounds
    const data = [1, 2, 3]
    const stats = calculateLatencyStats(data)

    // p99 with 3 elements: ceil(0.99 * 3) - 1 = ceil(2.97) - 1 = 3 - 1 = 2
    // This should be valid (index 2 is the last element)
    expect(stats.p99).toBe(3) // Should be the max value

    // Make sure no NaN or undefined values leaked through
    expect(Number.isNaN(stats.p99)).toBe(false)
    expect(stats.p99).toBeLessThanOrEqual(3)
  })

  it('should handle very large arrays without index errors', () => {
    const largeArray = Array.from({ length: 10000 }, (_, i) => i)
    const stats = calculateLatencyStats(largeArray)

    expect(stats.min).toBe(0)
    expect(stats.max).toBe(9999)
    expect(stats.p99).toBeGreaterThanOrEqual(0)
    expect(stats.p99).toBeLessThanOrEqual(9999)
    expect(Number.isNaN(stats.p99)).toBe(false)
  })
})
