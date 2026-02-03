/**
 * Tests for load-test-compaction.ts utilities
 *
 * These tests verify the core utility functions and data generation
 * used in the compaction load testing suite.
 */

import { describe, it, expect } from 'vitest'

// =============================================================================
// Utility Functions (copied from script for testing)
// =============================================================================

function generateWriterId(): string {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
  let id = ''
  for (let i = 0; i < 8; i++) {
    id += chars.charAt(Math.floor(Math.random() * chars.length))
  }
  return id
}

function generateParquetLikeData(sizeBytes: number): Uint8Array {
  const data = new Uint8Array(sizeBytes)

  // PAR1 magic at start
  data[0] = 0x50 // P
  data[1] = 0x41 // A
  data[2] = 0x52 // R
  data[3] = 0x31 // 1

  // Fill middle with random data
  for (let i = 4; i < sizeBytes - 4; i++) {
    data[i] = Math.floor(Math.random() * 256)
  }

  // PAR1 magic at end
  if (sizeBytes > 7) {
    data[sizeBytes - 4] = 0x50 // P
    data[sizeBytes - 3] = 0x41 // A
    data[sizeBytes - 2] = 0x52 // R
    data[sizeBytes - 1] = 0x31 // 1
  }

  return data
}

function percentile(arr: number[], p: number): number {
  if (arr.length === 0) return 0
  const sorted = [...arr].sort((a, b) => a - b)
  const idx = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, idx)] ?? 0
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)}GB`
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms.toFixed(0)}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  if (ms < 3600000) return `${(ms / 60000).toFixed(1)}m`
  return `${(ms / 3600000).toFixed(1)}h`
}

// =============================================================================
// Tests
// =============================================================================

describe('load-test-compaction utilities', () => {
  describe('generateWriterId', () => {
    it('should generate 8-character IDs', () => {
      const id = generateWriterId()
      expect(id).toHaveLength(8)
    })

    it('should only use lowercase letters and numbers', () => {
      const id = generateWriterId()
      expect(id).toMatch(/^[a-z0-9]{8}$/)
    })

    it('should generate unique IDs', () => {
      const ids = new Set<string>()
      for (let i = 0; i < 100; i++) {
        ids.add(generateWriterId())
      }
      // Allow for small chance of collision in 100 attempts with 36^8 possibilities
      expect(ids.size).toBeGreaterThan(95)
    })
  })

  describe('generateParquetLikeData', () => {
    it('should generate data of the correct size', () => {
      const sizes = [8, 100, 1024, 10240]
      for (const size of sizes) {
        const data = generateParquetLikeData(size)
        expect(data.length).toBe(size)
      }
    })

    it('should have PAR1 magic bytes at start', () => {
      const data = generateParquetLikeData(100)
      expect(data[0]).toBe(0x50) // P
      expect(data[1]).toBe(0x41) // A
      expect(data[2]).toBe(0x52) // R
      expect(data[3]).toBe(0x31) // 1
    })

    it('should have PAR1 magic bytes at end for sizes > 7', () => {
      const data = generateParquetLikeData(100)
      expect(data[96]).toBe(0x50) // P
      expect(data[97]).toBe(0x41) // A
      expect(data[98]).toBe(0x52) // R
      expect(data[99]).toBe(0x31) // 1
    })

    it('should handle minimum size of 8 bytes', () => {
      const data = generateParquetLikeData(8)
      expect(data.length).toBe(8)
      // Start magic
      expect(data[0]).toBe(0x50)
      expect(data[1]).toBe(0x41)
      expect(data[2]).toBe(0x52)
      expect(data[3]).toBe(0x31)
      // End magic
      expect(data[4]).toBe(0x50)
      expect(data[5]).toBe(0x41)
      expect(data[6]).toBe(0x52)
      expect(data[7]).toBe(0x31)
    })
  })

  describe('percentile', () => {
    it('should return 0 for empty array', () => {
      expect(percentile([], 50)).toBe(0)
    })

    it('should calculate p50 correctly', () => {
      const data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      expect(percentile(data, 50)).toBe(5)
    })

    it('should calculate p95 correctly', () => {
      const data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
      expect(percentile(data, 95)).toBe(19)
    })

    it('should calculate p99 correctly', () => {
      const data = Array.from({ length: 100 }, (_, i) => i + 1)
      expect(percentile(data, 99)).toBe(99)
    })

    it('should handle single element', () => {
      expect(percentile([42], 50)).toBe(42)
      expect(percentile([42], 99)).toBe(42)
    })

    it('should handle unsorted input', () => {
      const data = [5, 3, 1, 4, 2]
      expect(percentile(data, 50)).toBe(3)
    })
  })

  describe('formatBytes', () => {
    it('should format bytes correctly', () => {
      expect(formatBytes(0)).toBe('0B')
      expect(formatBytes(100)).toBe('100B')
      expect(formatBytes(1023)).toBe('1023B')
    })

    it('should format kilobytes correctly', () => {
      expect(formatBytes(1024)).toBe('1.0KB')
      expect(formatBytes(2048)).toBe('2.0KB')
      expect(formatBytes(1536)).toBe('1.5KB')
    })

    it('should format megabytes correctly', () => {
      expect(formatBytes(1024 * 1024)).toBe('1.0MB')
      expect(formatBytes(100 * 1024 * 1024)).toBe('100.0MB')
    })

    it('should format gigabytes correctly', () => {
      expect(formatBytes(1024 * 1024 * 1024)).toBe('1.0GB')
      expect(formatBytes(2.5 * 1024 * 1024 * 1024)).toBe('2.5GB')
    })
  })

  describe('formatDuration', () => {
    it('should format milliseconds correctly', () => {
      expect(formatDuration(0)).toBe('0ms')
      expect(formatDuration(500)).toBe('500ms')
      expect(formatDuration(999)).toBe('999ms')
    })

    it('should format seconds correctly', () => {
      expect(formatDuration(1000)).toBe('1.0s')
      expect(formatDuration(5000)).toBe('5.0s')
      expect(formatDuration(30000)).toBe('30.0s')
    })

    it('should format minutes correctly', () => {
      expect(formatDuration(60000)).toBe('1.0m')
      expect(formatDuration(600000)).toBe('10.0m')
      expect(formatDuration(1800000)).toBe('30.0m')
    })

    it('should format hours correctly', () => {
      expect(formatDuration(3600000)).toBe('1.0h')
      expect(formatDuration(7200000)).toBe('2.0h')
    })
  })
})

describe('load-test-compaction scenarios', () => {
  const SCENARIOS: Record<string, {
    durationSec: number
    numNamespaces: number
    filesPerSecond: number
    fileSizeBytes: number
    numWriters: number
  }> = {
    A: {
      durationSec: 600,
      numNamespaces: 10,
      filesPerSecond: 100,
      fileSizeBytes: 1024,
      numWriters: 1,
    },
    B: {
      durationSec: 3600,
      numNamespaces: 100,
      filesPerSecond: 1000,
      fileSizeBytes: 10240,
      numWriters: 10,
    },
    C: {
      durationSec: 1800,
      numNamespaces: 5,
      filesPerSecond: 10,
      fileSizeBytes: 100 * 1024 * 1024,
      numWriters: 1,
    },
    D: {
      durationSec: 600,
      numNamespaces: 1,
      filesPerSecond: 500,
      fileSizeBytes: 1024,
      numWriters: 50,
    },
  }

  describe('Scenario A (baseline)', () => {
    const scenario = SCENARIOS.A!

    it('should have correct configuration', () => {
      expect(scenario.durationSec).toBe(600) // 10 minutes
      expect(scenario.numNamespaces).toBe(10)
      expect(scenario.filesPerSecond).toBe(100)
      expect(scenario.fileSizeBytes).toBe(1024)
      expect(scenario.numWriters).toBe(1)
    })

    it('should produce expected file count', () => {
      const expectedFiles = scenario.filesPerSecond * scenario.durationSec
      expect(expectedFiles).toBe(60000)
    })

    it('should produce expected data volume', () => {
      const expectedBytes = scenario.filesPerSecond * scenario.durationSec * scenario.fileSizeBytes
      expect(expectedBytes).toBe(60000 * 1024) // ~60MB
    })
  })

  describe('Scenario B (high volume)', () => {
    const scenario = SCENARIOS.B!

    it('should have correct configuration', () => {
      expect(scenario.durationSec).toBe(3600) // 1 hour
      expect(scenario.numNamespaces).toBe(100)
      expect(scenario.filesPerSecond).toBe(1000)
      expect(scenario.fileSizeBytes).toBe(10240)
      expect(scenario.numWriters).toBe(10)
    })

    it('should produce expected file count', () => {
      const expectedFiles = scenario.filesPerSecond * scenario.durationSec
      expect(expectedFiles).toBe(3600000) // 3.6M files
    })

    it('should produce expected data volume', () => {
      const expectedBytes = scenario.filesPerSecond * scenario.durationSec * scenario.fileSizeBytes
      expect(expectedBytes).toBe(3600000 * 10240) // ~36GB
    })
  })

  describe('Scenario C (large files)', () => {
    const scenario = SCENARIOS.C!

    it('should have correct configuration', () => {
      expect(scenario.durationSec).toBe(1800) // 30 minutes
      expect(scenario.numNamespaces).toBe(5)
      expect(scenario.filesPerSecond).toBe(10)
      expect(scenario.fileSizeBytes).toBe(100 * 1024 * 1024) // 100MB
      expect(scenario.numWriters).toBe(1)
    })

    it('should produce expected file count', () => {
      const expectedFiles = scenario.filesPerSecond * scenario.durationSec
      expect(expectedFiles).toBe(18000)
    })

    it('should produce expected data volume', () => {
      const expectedBytes = scenario.filesPerSecond * scenario.durationSec * scenario.fileSizeBytes
      expect(expectedBytes).toBe(18000 * 100 * 1024 * 1024) // ~1.8TB
    })
  })

  describe('Scenario D (high concurrency)', () => {
    const scenario = SCENARIOS.D!

    it('should have correct configuration', () => {
      expect(scenario.durationSec).toBe(600) // 10 minutes
      expect(scenario.numNamespaces).toBe(1)
      expect(scenario.filesPerSecond).toBe(500)
      expect(scenario.fileSizeBytes).toBe(1024)
      expect(scenario.numWriters).toBe(50)
    })

    it('should produce expected file count', () => {
      const expectedFiles = scenario.filesPerSecond * scenario.durationSec
      expect(expectedFiles).toBe(300000)
    })

    it('should have high writer-to-namespace ratio', () => {
      expect(scenario.numWriters / scenario.numNamespaces).toBe(50)
    })
  })
})

describe('load-test-compaction metrics aggregation', () => {
  interface HealthMetric {
    timestamp: number
    status: 'healthy' | 'degraded' | 'unhealthy' | 'error'
    totalActiveWindows: number
    oldestWindowAgeMs: number
    totalPendingFiles: number
    windowsStuckInProcessing: number
  }

  interface WriteMetric {
    latencyMs: number
    success: boolean
  }

  function calculateSummary(healthMetrics: HealthMetric[], writeMetrics: WriteMetric[]) {
    const successWrites = writeMetrics.filter(m => m.success)
    const latencies = successWrites.map(m => m.latencyMs)

    return {
      totalFilesWritten: writeMetrics.length,
      totalFilesSucceeded: successWrites.length,
      totalFilesFailed: writeMetrics.length - successWrites.length,
      writeLatencyP50: percentile(latencies, 50),
      writeLatencyP95: percentile(latencies, 95),
      writeLatencyP99: percentile(latencies, 99),
      writeLatencyMax: Math.max(0, ...latencies),
      healthStatusCounts: healthMetrics.reduce((counts, m) => {
        counts[m.status] = (counts[m.status] || 0) + 1
        return counts
      }, {} as Record<string, number>),
      maxActiveWindows: Math.max(0, ...healthMetrics.map(m => m.totalActiveWindows)),
      maxPendingFiles: Math.max(0, ...healthMetrics.map(m => m.totalPendingFiles)),
    }
  }

  it('should calculate write metrics correctly', () => {
    const writeMetrics: WriteMetric[] = [
      { latencyMs: 10, success: true },
      { latencyMs: 20, success: true },
      { latencyMs: 30, success: true },
      { latencyMs: 100, success: false },
      { latencyMs: 50, success: true },
    ]

    const summary = calculateSummary([], writeMetrics)

    expect(summary.totalFilesWritten).toBe(5)
    expect(summary.totalFilesSucceeded).toBe(4)
    expect(summary.totalFilesFailed).toBe(1)
  })

  it('should calculate health status counts correctly', () => {
    const healthMetrics: HealthMetric[] = [
      { timestamp: 1, status: 'healthy', totalActiveWindows: 1, oldestWindowAgeMs: 0, totalPendingFiles: 0, windowsStuckInProcessing: 0 },
      { timestamp: 2, status: 'healthy', totalActiveWindows: 2, oldestWindowAgeMs: 0, totalPendingFiles: 0, windowsStuckInProcessing: 0 },
      { timestamp: 3, status: 'degraded', totalActiveWindows: 5, oldestWindowAgeMs: 0, totalPendingFiles: 0, windowsStuckInProcessing: 0 },
      { timestamp: 4, status: 'healthy', totalActiveWindows: 3, oldestWindowAgeMs: 0, totalPendingFiles: 0, windowsStuckInProcessing: 0 },
    ]

    const summary = calculateSummary(healthMetrics, [])

    expect(summary.healthStatusCounts).toEqual({
      healthy: 3,
      degraded: 1,
    })
  })

  it('should calculate max metrics correctly', () => {
    const healthMetrics: HealthMetric[] = [
      { timestamp: 1, status: 'healthy', totalActiveWindows: 5, oldestWindowAgeMs: 1000, totalPendingFiles: 50, windowsStuckInProcessing: 0 },
      { timestamp: 2, status: 'healthy', totalActiveWindows: 10, oldestWindowAgeMs: 2000, totalPendingFiles: 100, windowsStuckInProcessing: 0 },
      { timestamp: 3, status: 'healthy', totalActiveWindows: 3, oldestWindowAgeMs: 500, totalPendingFiles: 30, windowsStuckInProcessing: 0 },
    ]

    const summary = calculateSummary(healthMetrics, [])

    expect(summary.maxActiveWindows).toBe(10)
    expect(summary.maxPendingFiles).toBe(100)
  })
})
