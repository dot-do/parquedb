/**
 * Statistics Utilities
 *
 * Functions for calculating latency percentiles and statistics.
 */

import type { LatencyStats } from './types'

/**
 * Calculate percentile statistics from latency measurements
 */
export function calculateStats(latencies: number[]): LatencyStats {
  if (latencies.length === 0) {
    return { p50: 0, p95: 0, p99: 0, avg: 0, min: 0, max: 0, stdDev: 0 }
  }

  const sorted = [...latencies].sort((a, b) => a - b)
  const sum = sorted.reduce((a, b) => a + b, 0)
  const avg = sum / sorted.length

  // Calculate standard deviation
  const squaredDiffs = sorted.map(v => Math.pow(v - avg, 2))
  const avgSquaredDiff = squaredDiffs.reduce((a, b) => a + b, 0) / sorted.length
  const stdDev = Math.sqrt(avgSquaredDiff)

  return {
    p50: getPercentile(sorted, 0.5),
    p95: getPercentile(sorted, 0.95),
    p99: getPercentile(sorted, 0.99),
    avg: Math.round(avg * 100) / 100,
    min: sorted[0] ?? 0,
    max: sorted[sorted.length - 1] ?? 0,
    stdDev: Math.round(stdDev * 100) / 100,
  }
}

/**
 * Get a specific percentile from a sorted array
 */
function getPercentile(sorted: number[], percentile: number): number {
  const index = Math.floor(sorted.length * percentile)
  return sorted[Math.min(index, sorted.length - 1)] ?? 0
}

/**
 * Format latency stats for console output
 */
export function formatStats(stats: LatencyStats, label: string): string {
  return `${label}:
  p50: ${stats.p50}ms
  p95: ${stats.p95}ms
  p99: ${stats.p99}ms
  avg: ${stats.avg}ms
  min: ${stats.min}ms
  max: ${stats.max}ms
  stdDev: ${stats.stdDev}ms`
}

/**
 * Format a single latency value with appropriate precision
 */
export function formatLatency(ms: number): string {
  if (ms < 1) {
    return `${Math.round(ms * 1000) / 1000}ms`
  }
  if (ms < 10) {
    return `${Math.round(ms * 10) / 10}ms`
  }
  return `${Math.round(ms)}ms`
}
