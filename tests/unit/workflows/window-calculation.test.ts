/**
 * Window Calculation Tests
 *
 * Tests for time window calculations used in compaction workflows.
 * Covers:
 * - windowStart/windowEnd calculation with different window sizes
 * - Window readiness logic (time elapsed, writer quorum)
 * - File grouping into windows
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'

// =============================================================================
// Types
// =============================================================================

interface WindowState {
  windowStart: number
  windowEnd: number
  filesByWriter: Map<string, string[]>
  writers: Set<string>
  lastActivityAt: number
  totalSize: number
}

interface WindowConfig {
  windowSizeMs: number
  minFilesToCompact: number
  maxWaitTimeMs: number
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Calculate window boundaries for a given timestamp
 */
function calculateWindowBoundaries(
  timestamp: number,
  windowSizeMs: number
): { windowStart: number; windowEnd: number } {
  const windowStart = Math.floor(timestamp / windowSizeMs) * windowSizeMs
  const windowEnd = windowStart + windowSizeMs
  return { windowStart, windowEnd }
}

/**
 * Generate a window key from namespace and window start
 */
function generateWindowKey(namespace: string, windowStart: number): string {
  return `${namespace}:${windowStart}`
}

/**
 * Check if a window is ready for compaction
 */
function isWindowReady(
  window: WindowState,
  now: number,
  activeWriters: string[],
  config: WindowConfig
): { ready: boolean; reason: string } {
  // Check if window is too recent
  if (now < window.windowEnd + config.maxWaitTimeMs) {
    return { ready: false, reason: 'Window is too recent' }
  }

  // Count total files
  let totalFiles = 0
  for (const files of window.filesByWriter.values()) {
    totalFiles += files.length
  }

  // Check minimum files threshold
  if (totalFiles < config.minFilesToCompact) {
    return { ready: false, reason: `Not enough files (${totalFiles} < ${config.minFilesToCompact})` }
  }

  // Check writer quorum
  const missingWriters = activeWriters.filter(w => !window.writers.has(w))
  const waitedLongEnough = (now - window.lastActivityAt) > config.maxWaitTimeMs

  if (missingWriters.length > 0 && !waitedLongEnough) {
    return { ready: false, reason: `Waiting for writers: ${missingWriters.join(', ')}` }
  }

  return { ready: true, reason: 'All conditions met' }
}

/**
 * Create a test window state
 */
function createWindowState(overrides: Partial<WindowState> = {}): WindowState {
  return {
    windowStart: 1700000000000,
    windowEnd: 1700003600000,
    filesByWriter: new Map(),
    writers: new Set(),
    lastActivityAt: Date.now(),
    totalSize: 0,
    ...overrides,
  }
}

// =============================================================================
// Window Boundary Calculation Tests
// =============================================================================

describe('Window Calculation - Boundaries', () => {
  describe('1-hour windows (3600000ms)', () => {
    const windowSizeMs = 3600000 // 1 hour

    it('should calculate window containing timestamp', () => {
      const timestamp = 1700001234000 // Some time in the middle of an hour
      const { windowStart, windowEnd } = calculateWindowBoundaries(timestamp, windowSizeMs)

      expect(windowStart).toBeLessThanOrEqual(timestamp)
      expect(windowEnd).toBeGreaterThan(timestamp)
    })

    it('should create windows of exactly 1 hour', () => {
      const timestamp = 1700001234000
      const { windowStart, windowEnd } = calculateWindowBoundaries(timestamp, windowSizeMs)

      expect(windowEnd - windowStart).toBe(windowSizeMs)
    })

    it('should align windows to hour boundaries', () => {
      // 2023-11-14 22:13:20 UTC
      const timestamp = 1700000000000
      const { windowStart } = calculateWindowBoundaries(timestamp, windowSizeMs)

      // Window start should be divisible by window size
      expect(windowStart % windowSizeMs).toBe(0)
    })

    it('should place consecutive timestamps in same window', () => {
      const timestamp1 = 1700001000000
      const timestamp2 = 1700001500000
      const timestamp3 = 1700002000000

      const window1 = calculateWindowBoundaries(timestamp1, windowSizeMs)
      const window2 = calculateWindowBoundaries(timestamp2, windowSizeMs)
      const window3 = calculateWindowBoundaries(timestamp3, windowSizeMs)

      // All within same hour should have same window start
      expect(window1.windowStart).toBe(window2.windowStart)
      expect(window2.windowStart).toBe(window3.windowStart)
    })

    it('should place timestamps across hour boundary in different windows', () => {
      // Use timestamps that are guaranteed to be in different windows
      // First, find a clean hour boundary
      const baseTime = 1700000000000
      const windowStart = Math.floor(baseTime / windowSizeMs) * windowSizeMs
      const timestamp1 = windowStart + windowSizeMs - 1000 // Last second of first window
      const timestamp2 = windowStart + windowSizeMs // First second of next window

      const window1 = calculateWindowBoundaries(timestamp1, windowSizeMs)
      const window2 = calculateWindowBoundaries(timestamp2, windowSizeMs)

      expect(window1.windowStart).not.toBe(window2.windowStart)
      expect(window2.windowStart).toBe(window1.windowEnd)
    })
  })

  describe('30-minute windows (1800000ms)', () => {
    const windowSizeMs = 1800000 // 30 minutes

    it('should create windows of exactly 30 minutes', () => {
      const timestamp = 1700001234000
      const { windowStart, windowEnd } = calculateWindowBoundaries(timestamp, windowSizeMs)

      expect(windowEnd - windowStart).toBe(windowSizeMs)
    })

    it('should create twice as many windows as 1-hour', () => {
      // Two timestamps 45 minutes apart
      const timestamp1 = 1700000000000
      const timestamp2 = 1700002700000 // 45 minutes later

      const window1 = calculateWindowBoundaries(timestamp1, windowSizeMs)
      const window2 = calculateWindowBoundaries(timestamp2, windowSizeMs)

      // Should be in different windows
      expect(window1.windowStart).not.toBe(window2.windowStart)
    })
  })

  describe('15-minute windows (900000ms)', () => {
    const windowSizeMs = 900000 // 15 minutes

    it('should create windows of exactly 15 minutes', () => {
      const timestamp = 1700001234000
      const { windowStart, windowEnd } = calculateWindowBoundaries(timestamp, windowSizeMs)

      expect(windowEnd - windowStart).toBe(windowSizeMs)
    })

    it('should handle more granular time ranges', () => {
      // With 15-minute windows (900000ms), timestamps at these offsets:
      // 0-14:59 = window 1, 15:00-29:59 = window 2, etc.
      // Use a clean base time that's exactly on a 15-minute boundary
      const baseTime = Math.floor(1700000000000 / windowSizeMs) * windowSizeMs

      const timestamps = [
        baseTime,                 // 0 min - window 1
        baseTime + 600000,        // +10 min - window 1 (still in first 15 min)
        baseTime + 900000,        // +15 min - window 2 (new 15 min window)
        baseTime + 1200000,       // +20 min - window 2 (still in second 15 min)
      ]

      const windows = timestamps.map(t => calculateWindowBoundaries(t, windowSizeMs))

      // First two (0 and 10 min) should be in same window
      expect(windows[0].windowStart).toBe(windows[1].windowStart)
      // Third (15 min) should be in different window
      expect(windows[0].windowStart).not.toBe(windows[2].windowStart)
      // Third and fourth (15 and 20 min) should be in same window
      expect(windows[2].windowStart).toBe(windows[3].windowStart)
    })
  })

  describe('edge cases', () => {
    it('should handle timestamp exactly at window boundary', () => {
      const windowSizeMs = 3600000
      const exactBoundary = 1700000000000 - (1700000000000 % windowSizeMs)

      const { windowStart, windowEnd } = calculateWindowBoundaries(exactBoundary, windowSizeMs)

      expect(windowStart).toBe(exactBoundary)
      expect(windowEnd).toBe(exactBoundary + windowSizeMs)
    })

    it('should handle timestamp at end of window (exclusive)', () => {
      const windowSizeMs = 3600000
      const windowStart = 1700000000000 - (1700000000000 % windowSizeMs)
      const atWindowEnd = windowStart + windowSizeMs

      const result = calculateWindowBoundaries(atWindowEnd, windowSizeMs)

      // Should be in the NEXT window
      expect(result.windowStart).toBe(atWindowEnd)
    })

    it('should handle zero timestamp', () => {
      const windowSizeMs = 3600000
      const { windowStart, windowEnd } = calculateWindowBoundaries(0, windowSizeMs)

      expect(windowStart).toBe(0)
      expect(windowEnd).toBe(windowSizeMs)
    })

    it('should handle very large timestamps (year 2100)', () => {
      const windowSizeMs = 3600000
      const farFuture = 4102444800000 // 2100-01-01 00:00:00 UTC

      const { windowStart, windowEnd } = calculateWindowBoundaries(farFuture, windowSizeMs)

      expect(windowStart).toBeLessThanOrEqual(farFuture)
      expect(windowEnd).toBeGreaterThan(farFuture)
      expect(windowEnd - windowStart).toBe(windowSizeMs)
    })
  })
})

// =============================================================================
// Window Key Generation Tests
// =============================================================================

describe('Window Calculation - Key Generation', () => {
  describe('window key format', () => {
    it('should generate key with namespace and windowStart', () => {
      const key = generateWindowKey('users', 1700000000000)

      expect(key).toBe('users:1700000000000')
    })

    it('should generate unique keys for different namespaces', () => {
      const key1 = generateWindowKey('users', 1700000000000)
      const key2 = generateWindowKey('posts', 1700000000000)

      expect(key1).not.toBe(key2)
    })

    it('should generate unique keys for different windows', () => {
      const key1 = generateWindowKey('users', 1700000000000)
      const key2 = generateWindowKey('users', 1700003600000)

      expect(key1).not.toBe(key2)
    })

    it('should handle nested namespace paths', () => {
      const key = generateWindowKey('app/users/archived', 1700000000000)

      expect(key).toBe('app/users/archived:1700000000000')
    })
  })

  describe('key parsing', () => {
    it('should be parseable back to components', () => {
      const namespace = 'users'
      const windowStart = 1700000000000
      const key = generateWindowKey(namespace, windowStart)

      const parts = key.split(':')
      expect(parts[0]).toBe(namespace)
      expect(parseInt(parts[1]!, 10)).toBe(windowStart)
    })

    it('should handle namespace with colons (edge case)', () => {
      // This tests that we split on last colon
      const key = generateWindowKey('app:v2/users', 1700000000000)

      // Split only on LAST colon
      const lastColonIndex = key.lastIndexOf(':')
      const namespace = key.slice(0, lastColonIndex)
      const windowStart = parseInt(key.slice(lastColonIndex + 1), 10)

      expect(namespace).toBe('app:v2/users')
      expect(windowStart).toBe(1700000000000)
    })
  })
})

// =============================================================================
// Window Readiness Tests
// =============================================================================

describe('Window Calculation - Readiness', () => {
  const defaultConfig: WindowConfig = {
    windowSizeMs: 3600000, // 1 hour
    minFilesToCompact: 10,
    maxWaitTimeMs: 300000, // 5 minutes
  }

  describe('time-based readiness', () => {
    it('should not be ready if window just ended', () => {
      const now = Date.now()
      const windowEnd = now - 60000 // 1 minute ago

      const window = createWindowState({
        windowEnd,
        lastActivityAt: now - 30000,
      })

      const result = isWindowReady(window, now, [], defaultConfig)

      expect(result.ready).toBe(false)
      expect(result.reason).toContain('too recent')
    })

    it('should be ready after maxWaitTimeMs has passed', () => {
      const now = Date.now()
      const windowEnd = now - 400000 // 6+ minutes ago (past 5 min wait)

      const window = createWindowState({
        windowEnd,
        lastActivityAt: now - 350000,
        filesByWriter: new Map([
          ['writer1', Array(10).fill('file.parquet')],
        ]),
        writers: new Set(['writer1']),
      })

      const result = isWindowReady(window, now, [], defaultConfig)

      expect(result.ready).toBe(true)
    })

    it('should respect different maxWaitTimeMs values', () => {
      const now = Date.now()
      const windowEnd = now - 120000 // 2 minutes ago

      const window = createWindowState({
        windowEnd,
        lastActivityAt: now - 100000,
        filesByWriter: new Map([
          ['writer1', Array(10).fill('file.parquet')],
        ]),
        writers: new Set(['writer1']),
      })

      // With 5 minute wait - should NOT be ready
      const result1 = isWindowReady(window, now, [], { ...defaultConfig, maxWaitTimeMs: 300000 })
      expect(result1.ready).toBe(false)

      // With 1 minute wait - should be ready
      const result2 = isWindowReady(window, now, [], { ...defaultConfig, maxWaitTimeMs: 60000 })
      expect(result2.ready).toBe(true)
    })
  })

  describe('file count readiness', () => {
    it('should not be ready if below minimum files', () => {
      const now = Date.now()
      const windowEnd = now - 400000

      const window = createWindowState({
        windowEnd,
        lastActivityAt: now - 350000,
        filesByWriter: new Map([
          ['writer1', Array(5).fill('file.parquet')], // Only 5 files
        ]),
        writers: new Set(['writer1']),
      })

      const result = isWindowReady(window, now, [], defaultConfig)

      expect(result.ready).toBe(false)
      expect(result.reason).toContain('Not enough files')
    })

    it('should be ready at exactly minimum files', () => {
      const now = Date.now()
      const windowEnd = now - 400000

      const window = createWindowState({
        windowEnd,
        lastActivityAt: now - 350000,
        filesByWriter: new Map([
          ['writer1', Array(10).fill('file.parquet')], // Exactly 10 files
        ]),
        writers: new Set(['writer1']),
      })

      const result = isWindowReady(window, now, [], defaultConfig)

      expect(result.ready).toBe(true)
    })

    it('should count files across all writers', () => {
      const now = Date.now()
      const windowEnd = now - 400000

      const window = createWindowState({
        windowEnd,
        lastActivityAt: now - 350000,
        filesByWriter: new Map([
          ['writer1', Array(4).fill('file.parquet')],
          ['writer2', Array(4).fill('file.parquet')],
          ['writer3', Array(4).fill('file.parquet')],
        ]),
        writers: new Set(['writer1', 'writer2', 'writer3']),
      })

      // Total: 12 files across 3 writers
      const result = isWindowReady(window, now, [], defaultConfig)

      expect(result.ready).toBe(true)
    })

    it('should respect different minFilesToCompact values', () => {
      const now = Date.now()
      const windowEnd = now - 400000

      const window = createWindowState({
        windowEnd,
        lastActivityAt: now - 350000,
        filesByWriter: new Map([
          ['writer1', Array(5).fill('file.parquet')],
        ]),
        writers: new Set(['writer1']),
      })

      // With min 10 - should NOT be ready
      const result1 = isWindowReady(window, now, [], { ...defaultConfig, minFilesToCompact: 10 })
      expect(result1.ready).toBe(false)

      // With min 5 - should be ready
      const result2 = isWindowReady(window, now, [], { ...defaultConfig, minFilesToCompact: 5 })
      expect(result2.ready).toBe(true)
    })
  })

  describe('writer quorum readiness', () => {
    it('should not be ready if active writers are missing', () => {
      const now = Date.now()
      const windowEnd = now - 400000

      const window = createWindowState({
        windowEnd,
        lastActivityAt: now - 100000, // Recent activity
        filesByWriter: new Map([
          ['writer1', Array(10).fill('file.parquet')],
        ]),
        writers: new Set(['writer1']),
      })

      // writer2 is active but hasn't contributed
      const activeWriters = ['writer1', 'writer2']
      const result = isWindowReady(window, now, activeWriters, defaultConfig)

      expect(result.ready).toBe(false)
      expect(result.reason).toContain('writer2')
    })

    it('should be ready if all active writers contributed', () => {
      const now = Date.now()
      const windowEnd = now - 400000

      const window = createWindowState({
        windowEnd,
        lastActivityAt: now - 350000,
        filesByWriter: new Map([
          ['writer1', Array(5).fill('file.parquet')],
          ['writer2', Array(5).fill('file.parquet')],
        ]),
        writers: new Set(['writer1', 'writer2']),
      })

      const activeWriters = ['writer1', 'writer2']
      const result = isWindowReady(window, now, activeWriters, defaultConfig)

      expect(result.ready).toBe(true)
    })

    it('should be ready after wait timeout even with missing writers', () => {
      const now = Date.now()
      const windowEnd = now - 400000

      const window = createWindowState({
        windowEnd,
        lastActivityAt: now - 400000, // Last activity was long ago
        filesByWriter: new Map([
          ['writer1', Array(10).fill('file.parquet')],
        ]),
        writers: new Set(['writer1']),
      })

      // writer2 is active but waited long enough
      const activeWriters = ['writer1', 'writer2']
      const result = isWindowReady(window, now, activeWriters, defaultConfig)

      expect(result.ready).toBe(true)
    })

    it('should handle empty active writers list', () => {
      const now = Date.now()
      const windowEnd = now - 400000

      const window = createWindowState({
        windowEnd,
        lastActivityAt: now - 350000,
        filesByWriter: new Map([
          ['writer1', Array(10).fill('file.parquet')],
        ]),
        writers: new Set(['writer1']),
      })

      // No active writers - no quorum needed
      const result = isWindowReady(window, now, [], defaultConfig)

      expect(result.ready).toBe(true)
    })

    it('should allow extra writers in window beyond active set', () => {
      const now = Date.now()
      const windowEnd = now - 400000

      const window = createWindowState({
        windowEnd,
        lastActivityAt: now - 350000,
        filesByWriter: new Map([
          ['writer1', Array(5).fill('file.parquet')],
          ['writer2', Array(5).fill('file.parquet')],
          ['writer3', Array(5).fill('file.parquet')], // Extra writer
        ]),
        writers: new Set(['writer1', 'writer2', 'writer3']),
      })

      // Only writer1 and writer2 are "active"
      const activeWriters = ['writer1', 'writer2']
      const result = isWindowReady(window, now, activeWriters, defaultConfig)

      expect(result.ready).toBe(true)
    })
  })
})

// =============================================================================
// File Grouping into Windows Tests
// =============================================================================

describe('Window Calculation - File Grouping', () => {
  const windowSizeMs = 3600000 // 1 hour

  /**
   * Group files into windows based on timestamp
   */
  function groupFilesIntoWindows(
    files: Array<{ namespace: string; writerId: string; timestamp: number; path: string; size: number }>,
    windowSizeMs: number
  ): Map<string, WindowState> {
    const windows = new Map<string, WindowState>()

    for (const file of files) {
      const { windowStart, windowEnd } = calculateWindowBoundaries(file.timestamp, windowSizeMs)
      const windowKey = generateWindowKey(file.namespace, windowStart)

      let window = windows.get(windowKey)
      if (!window) {
        window = createWindowState({
          windowStart,
          windowEnd,
          lastActivityAt: file.timestamp,
        })
        windows.set(windowKey, window)
      }

      // Add file to writer's list
      const writerFiles = window.filesByWriter.get(file.writerId) ?? []
      writerFiles.push(file.path)
      window.filesByWriter.set(file.writerId, writerFiles)
      window.writers.add(file.writerId)
      window.totalSize += file.size
      window.lastActivityAt = Math.max(window.lastActivityAt, file.timestamp)
    }

    return windows
  }

  describe('single namespace grouping', () => {
    it('should group files in same hour to same window', () => {
      const files = [
        { namespace: 'users', writerId: 'writer1', timestamp: 1700000000000, path: 'file1.parquet', size: 1024 },
        { namespace: 'users', writerId: 'writer1', timestamp: 1700001000000, path: 'file2.parquet', size: 1024 },
        { namespace: 'users', writerId: 'writer1', timestamp: 1700002000000, path: 'file3.parquet', size: 1024 },
      ]

      const windows = groupFilesIntoWindows(files, windowSizeMs)

      expect(windows.size).toBe(1)

      const window = windows.values().next().value as WindowState
      expect(window.filesByWriter.get('writer1')).toHaveLength(3)
    })

    it('should separate files into different hour windows', () => {
      const files = [
        { namespace: 'users', writerId: 'writer1', timestamp: 1700000000000, path: 'file1.parquet', size: 1024 },
        { namespace: 'users', writerId: 'writer1', timestamp: 1700003600000, path: 'file2.parquet', size: 1024 }, // +1 hour
        { namespace: 'users', writerId: 'writer1', timestamp: 1700007200000, path: 'file3.parquet', size: 1024 }, // +2 hours
      ]

      const windows = groupFilesIntoWindows(files, windowSizeMs)

      expect(windows.size).toBe(3)
    })
  })

  describe('multi-namespace grouping', () => {
    it('should separate files by namespace', () => {
      const files = [
        { namespace: 'users', writerId: 'writer1', timestamp: 1700000000000, path: 'users/file1.parquet', size: 1024 },
        { namespace: 'posts', writerId: 'writer1', timestamp: 1700000000000, path: 'posts/file1.parquet', size: 1024 },
      ]

      const windows = groupFilesIntoWindows(files, windowSizeMs)

      expect(windows.size).toBe(2)
      expect(windows.has('users:' + calculateWindowBoundaries(1700000000000, windowSizeMs).windowStart)).toBe(true)
      expect(windows.has('posts:' + calculateWindowBoundaries(1700000000000, windowSizeMs).windowStart)).toBe(true)
    })

    it('should handle same namespace across different windows', () => {
      const files = [
        { namespace: 'users', writerId: 'writer1', timestamp: 1700000000000, path: 'file1.parquet', size: 1024 },
        { namespace: 'users', writerId: 'writer1', timestamp: 1700003600000, path: 'file2.parquet', size: 1024 },
        { namespace: 'posts', writerId: 'writer1', timestamp: 1700000000000, path: 'file3.parquet', size: 1024 },
      ]

      const windows = groupFilesIntoWindows(files, windowSizeMs)

      expect(windows.size).toBe(3)
    })
  })

  describe('multi-writer grouping', () => {
    it('should track multiple writers in same window', () => {
      const files = [
        { namespace: 'users', writerId: 'writer1', timestamp: 1700000000000, path: 'w1-file1.parquet', size: 1024 },
        { namespace: 'users', writerId: 'writer2', timestamp: 1700001000000, path: 'w2-file1.parquet', size: 2048 },
        { namespace: 'users', writerId: 'writer3', timestamp: 1700002000000, path: 'w3-file1.parquet', size: 1024 },
      ]

      const windows = groupFilesIntoWindows(files, windowSizeMs)

      expect(windows.size).toBe(1)

      const window = windows.values().next().value as WindowState
      expect(window.writers.size).toBe(3)
      expect(window.writers.has('writer1')).toBe(true)
      expect(window.writers.has('writer2')).toBe(true)
      expect(window.writers.has('writer3')).toBe(true)
    })

    it('should track files by writer correctly', () => {
      const files = [
        { namespace: 'users', writerId: 'writer1', timestamp: 1700000000000, path: 'w1-file1.parquet', size: 1024 },
        { namespace: 'users', writerId: 'writer1', timestamp: 1700001000000, path: 'w1-file2.parquet', size: 1024 },
        { namespace: 'users', writerId: 'writer2', timestamp: 1700000500000, path: 'w2-file1.parquet', size: 2048 },
      ]

      const windows = groupFilesIntoWindows(files, windowSizeMs)
      const window = windows.values().next().value as WindowState

      expect(window.filesByWriter.get('writer1')).toHaveLength(2)
      expect(window.filesByWriter.get('writer2')).toHaveLength(1)
    })
  })

  describe('size accumulation', () => {
    it('should accumulate total size across all files', () => {
      const files = [
        { namespace: 'users', writerId: 'writer1', timestamp: 1700000000000, path: 'file1.parquet', size: 1000 },
        { namespace: 'users', writerId: 'writer1', timestamp: 1700001000000, path: 'file2.parquet', size: 2000 },
        { namespace: 'users', writerId: 'writer2', timestamp: 1700002000000, path: 'file3.parquet', size: 3000 },
      ]

      const windows = groupFilesIntoWindows(files, windowSizeMs)
      const window = windows.values().next().value as WindowState

      expect(window.totalSize).toBe(6000)
    })
  })

  describe('last activity tracking', () => {
    it('should track latest activity timestamp', () => {
      const files = [
        { namespace: 'users', writerId: 'writer1', timestamp: 1700000000000, path: 'file1.parquet', size: 1024 },
        { namespace: 'users', writerId: 'writer1', timestamp: 1700002000000, path: 'file2.parquet', size: 1024 },
        { namespace: 'users', writerId: 'writer1', timestamp: 1700001000000, path: 'file3.parquet', size: 1024 },
      ]

      const windows = groupFilesIntoWindows(files, windowSizeMs)
      const window = windows.values().next().value as WindowState

      // Should be the max timestamp
      expect(window.lastActivityAt).toBe(1700002000000)
    })
  })
})

// =============================================================================
// Active Writer Detection Tests
// =============================================================================

describe('Window Calculation - Active Writer Detection', () => {
  const WRITER_INACTIVE_THRESHOLD_MS = 30 * 60 * 1000 // 30 minutes

  /**
   * Get list of active writers
   */
  function getActiveWriters(
    writerLastSeen: Map<string, number>,
    now: number,
    thresholdMs: number = WRITER_INACTIVE_THRESHOLD_MS
  ): string[] {
    const active: string[] = []
    for (const [writerId, lastSeen] of writerLastSeen) {
      if (now - lastSeen < thresholdMs) {
        active.push(writerId)
      }
    }
    return active
  }

  describe('activity threshold', () => {
    it('should include writers seen recently', () => {
      const now = Date.now()
      const writerLastSeen = new Map([
        ['writer1', now - 1000], // 1 second ago
        ['writer2', now - 60000], // 1 minute ago
        ['writer3', now - 1800000], // 30 minutes ago (at threshold)
      ])

      const active = getActiveWriters(writerLastSeen, now)

      // writer3 is exactly at threshold, should be EXCLUDED (>= not >)
      expect(active).toContain('writer1')
      expect(active).toContain('writer2')
      expect(active).not.toContain('writer3')
    })

    it('should exclude writers not seen for too long', () => {
      const now = Date.now()
      const writerLastSeen = new Map([
        ['writer1', now - 1000], // Active
        ['writer2', now - (WRITER_INACTIVE_THRESHOLD_MS + 1000)], // Inactive
      ])

      const active = getActiveWriters(writerLastSeen, now)

      expect(active).toContain('writer1')
      expect(active).not.toContain('writer2')
    })

    it('should handle empty writer map', () => {
      const now = Date.now()
      const writerLastSeen = new Map<string, number>()

      const active = getActiveWriters(writerLastSeen, now)

      expect(active).toHaveLength(0)
    })

    it('should handle all writers inactive', () => {
      const now = Date.now()
      const oldTime = now - (WRITER_INACTIVE_THRESHOLD_MS + 1000)
      const writerLastSeen = new Map([
        ['writer1', oldTime],
        ['writer2', oldTime],
        ['writer3', oldTime],
      ])

      const active = getActiveWriters(writerLastSeen, now)

      expect(active).toHaveLength(0)
    })
  })

  describe('custom threshold', () => {
    it('should respect custom threshold values', () => {
      const now = Date.now()
      const writerLastSeen = new Map([
        ['writer1', now - 60000], // 1 minute ago
      ])

      // With 30 second threshold - should be inactive
      const active1 = getActiveWriters(writerLastSeen, now, 30000)
      expect(active1).toHaveLength(0)

      // With 2 minute threshold - should be active
      const active2 = getActiveWriters(writerLastSeen, now, 120000)
      expect(active2).toContain('writer1')
    })
  })
})
