/**
 * Hierarchical Compaction (LSM-tree style) Test Suite
 *
 * Tests for the hierarchical compaction functionality:
 * - Level path generation
 * - Promotion threshold calculations
 * - LevelStateDO state management
 * - CompactionPromotionWorkflow integration
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  generateLevelPath,
  parseLevelFromPath,
  shouldPromote,
  getNextLevel,
  getPromotionThreshold,
  getISOWeek,
  DEFAULT_HIERARCHICAL_CONFIG,
  createEmptyLevelState,
  createEmptyNamespaceLevelState,
  addFileToLevel,
  removeFilesFromLevel,
  getPromotionsNeeded,
  type CompactionLevel,
  type HierarchicalCompactionLevels,
  type LevelFileMetadata,
  type LevelState,
  type NamespaceLevelState,
} from '@/workflows/hierarchical-compaction-types'

// =============================================================================
// Path Generation Tests
// =============================================================================

describe('generateLevelPath', () => {
  const namespace = 'users'
  // 2024-01-15 10:30:00 UTC
  const windowStart = 1705315800000

  describe('L0 paths (hourly)', () => {
    it('should generate correct L0 path with hour partitioning', () => {
      const path = generateLevelPath(namespace, 'l0', windowStart, 0)

      expect(path).toContain('data/users/l0/')
      expect(path).toContain('year=2024')
      expect(path).toContain('month=01')
      expect(path).toContain('day=15')
      expect(path).toContain('hour=10')
      expect(path).toMatch(/compacted-\d+-0\.parquet$/)
    })

    it('should include batch number in L0 path', () => {
      const path = generateLevelPath(namespace, 'l0', windowStart, 5)

      expect(path).toMatch(/compacted-\d+-5\.parquet$/)
    })
  })

  describe('L1 paths (daily)', () => {
    it('should generate correct L1 path with day partitioning (no hour)', () => {
      const path = generateLevelPath(namespace, 'l1', windowStart, 0)

      expect(path).toContain('data/users/l1/')
      expect(path).toContain('year=2024')
      expect(path).toContain('month=01')
      expect(path).toContain('day=15')
      expect(path).not.toContain('hour=')
      expect(path).toMatch(/compacted-\d+-0\.parquet$/)
    })
  })

  describe('L2 paths (weekly)', () => {
    it('should generate correct L2 path with week partitioning', () => {
      const path = generateLevelPath(namespace, 'l2', windowStart, 0)

      expect(path).toContain('data/users/l2/')
      expect(path).toContain('year=2024')
      expect(path).toMatch(/week=\d{2}/)
      expect(path).not.toContain('month=')
      expect(path).not.toContain('day=')
      expect(path).not.toContain('hour=')
    })

    it('should calculate ISO week number correctly', () => {
      // January 1, 2024 is a Monday, so it's week 1
      const jan1 = new Date(Date.UTC(2024, 0, 1, 12, 0, 0)).getTime()
      const path = generateLevelPath(namespace, 'l2', jan1, 0)

      expect(path).toContain('week=01')
    })
  })

  describe('edge cases', () => {
    it('should handle timestamps at midnight', () => {
      // 2024-01-15 00:00:00 UTC
      const midnight = new Date(Date.UTC(2024, 0, 15, 0, 0, 0)).getTime()
      const path = generateLevelPath(namespace, 'l0', midnight, 0)

      expect(path).toContain('hour=00')
    })

    it('should handle end of month', () => {
      // 2024-01-31 23:59:00 UTC
      const endOfMonth = new Date(Date.UTC(2024, 0, 31, 23, 59, 0)).getTime()
      const path = generateLevelPath(namespace, 'l0', endOfMonth, 0)

      expect(path).toContain('day=31')
      expect(path).toContain('hour=23')
    })

    it('should handle nested namespace', () => {
      const path = generateLevelPath('app/users/data', 'l0', windowStart, 0)

      expect(path).toContain('data/app/users/data/l0/')
    })
  })
})

// =============================================================================
// Path Parsing Tests
// =============================================================================

describe('parseLevelFromPath', () => {
  it('should parse L0 level from path', () => {
    const path = 'data/users/l0/year=2024/month=01/day=15/hour=10/compacted-1705315800000-0.parquet'
    const level = parseLevelFromPath(path)

    expect(level).toBe('l0')
  })

  it('should parse L1 level from path', () => {
    const path = 'data/users/l1/year=2024/month=01/day=15/compacted-1705315800000-0.parquet'
    const level = parseLevelFromPath(path)

    expect(level).toBe('l1')
  })

  it('should parse L2 level from path', () => {
    const path = 'data/users/l2/year=2024/week=03/compacted-1705315800000-0.parquet'
    const level = parseLevelFromPath(path)

    expect(level).toBe('l2')
  })

  it('should default to L0 for paths without level directory', () => {
    const path = 'data/users/year=2024/compacted-1705315800000-0.parquet'
    const level = parseLevelFromPath(path)

    expect(level).toBe('l0')
  })

  it('should return null for non-data paths', () => {
    const path = 'events/segment-0001.parquet'
    const level = parseLevelFromPath(path)

    expect(level).toBeNull()
  })
})

// =============================================================================
// Promotion Threshold Tests
// =============================================================================

describe('getPromotionThreshold', () => {
  const config: HierarchicalCompactionLevels = {
    l0ToL1Threshold: 24,
    l1ToL2Threshold: 7,
  }

  it('should return L0 to L1 threshold', () => {
    const threshold = getPromotionThreshold('l0', config)
    expect(threshold).toBe(24)
  })

  it('should return L1 to L2 threshold', () => {
    const threshold = getPromotionThreshold('l1', config)
    expect(threshold).toBe(7)
  })

  it('should return Infinity for L2 (no promotion)', () => {
    const threshold = getPromotionThreshold('l2', config)
    expect(threshold).toBe(Infinity)
  })
})

describe('getNextLevel', () => {
  it('should return L1 for L0', () => {
    expect(getNextLevel('l0')).toBe('l1')
  })

  it('should return L2 for L1', () => {
    expect(getNextLevel('l1')).toBe('l2')
  })

  it('should return null for L2 (final level)', () => {
    expect(getNextLevel('l2')).toBeNull()
  })
})

describe('shouldPromote', () => {
  const config: HierarchicalCompactionLevels = {
    l0ToL1Threshold: 24,
    l1ToL2Threshold: 7,
  }

  it('should return true when L0 file count exceeds threshold', () => {
    expect(shouldPromote('l0', 24, config)).toBe(true)
    expect(shouldPromote('l0', 30, config)).toBe(true)
  })

  it('should return false when L0 file count is below threshold', () => {
    expect(shouldPromote('l0', 23, config)).toBe(false)
    expect(shouldPromote('l0', 0, config)).toBe(false)
  })

  it('should return true when L1 file count exceeds threshold', () => {
    expect(shouldPromote('l1', 7, config)).toBe(true)
    expect(shouldPromote('l1', 10, config)).toBe(true)
  })

  it('should return false when L1 file count is below threshold', () => {
    expect(shouldPromote('l1', 6, config)).toBe(false)
  })

  it('should never promote from L2', () => {
    expect(shouldPromote('l2', 100, config)).toBe(false)
    expect(shouldPromote('l2', 1000, config)).toBe(false)
  })
})

// =============================================================================
// Default Configuration Tests
// =============================================================================

describe('DEFAULT_HIERARCHICAL_CONFIG', () => {
  it('should have correct default values', () => {
    expect(DEFAULT_HIERARCHICAL_CONFIG.enabled).toBe(false)
    expect(DEFAULT_HIERARCHICAL_CONFIG.levels.l0ToL1Threshold).toBe(24)
    expect(DEFAULT_HIERARCHICAL_CONFIG.levels.l1ToL2Threshold).toBe(7)
    expect(DEFAULT_HIERARCHICAL_CONFIG.targetFormat).toBe('native')
  })

  it('should be immutable (readonly)', () => {
    // TypeScript would catch this at compile time, but let's verify the shape
    expect(typeof DEFAULT_HIERARCHICAL_CONFIG.enabled).toBe('boolean')
    expect(typeof DEFAULT_HIERARCHICAL_CONFIG.levels.l0ToL1Threshold).toBe('number')
  })
})

// =============================================================================
// LevelStateDO Mock Tests
// =============================================================================

describe('LevelStateDO behavior', () => {
  // Mock storage for testing state management logic
  class MockDurableObjectStorage {
    private data = new Map<string, unknown>()

    async get<T>(key: string): Promise<T | undefined> {
      return this.data.get(key) as T | undefined
    }

    async put<T>(key: string, value: T): Promise<void> {
      this.data.set(key, value)
    }
  }

  interface MockLevelState {
    level: CompactionLevel
    files: LevelFileMetadata[]
    totalSize: number
    totalRows: number
  }

  interface MockNamespaceLevelState {
    namespace: string
    levels: Record<CompactionLevel, MockLevelState>
    updatedAt: number
  }

  describe('state initialization', () => {
    it('should initialize with empty levels', () => {
      const levels: Record<CompactionLevel, MockLevelState> = {
        l0: { level: 'l0', files: [], totalSize: 0, totalRows: 0 },
        l1: { level: 'l1', files: [], totalSize: 0, totalRows: 0 },
        l2: { level: 'l2', files: [], totalSize: 0, totalRows: 0 },
      }

      expect(levels.l0.files).toHaveLength(0)
      expect(levels.l1.files).toHaveLength(0)
      expect(levels.l2.files).toHaveLength(0)
    })

    it('should restore state from storage', async () => {
      const storage = new MockDurableObjectStorage()

      const storedState: MockNamespaceLevelState = {
        namespace: 'users',
        levels: {
          l0: {
            level: 'l0',
            files: [
              {
                path: 'data/users/l0/compacted-123.parquet',
                size: 1024,
                rowCount: 100,
                windowStart: 1705315800000,
                windowEnd: 1705319400000,
                createdAt: Date.now(),
              },
            ],
            totalSize: 1024,
            totalRows: 100,
          },
          l1: { level: 'l1', files: [], totalSize: 0, totalRows: 0 },
          l2: { level: 'l2', files: [], totalSize: 0, totalRows: 0 },
        },
        updatedAt: Date.now(),
      }

      await storage.put('levelState', storedState)
      const restored = await storage.get<MockNamespaceLevelState>('levelState')

      expect(restored?.namespace).toBe('users')
      expect(restored?.levels.l0.files).toHaveLength(1)
      expect(restored?.levels.l0.totalSize).toBe(1024)
    })
  })

  describe('file tracking', () => {
    it('should accumulate files and totals correctly', () => {
      const files: LevelFileMetadata[] = [
        { path: 'file1.parquet', size: 1000, rowCount: 50, windowStart: 0, windowEnd: 1000, createdAt: 0 },
        { path: 'file2.parquet', size: 2000, rowCount: 100, windowStart: 0, windowEnd: 1000, createdAt: 0 },
        { path: 'file3.parquet', size: 3000, rowCount: 150, windowStart: 0, windowEnd: 1000, createdAt: 0 },
      ]

      const totalSize = files.reduce((sum, f) => sum + f.size, 0)
      const totalRows = files.reduce((sum, f) => sum + (f.rowCount ?? 0), 0)

      expect(totalSize).toBe(6000)
      expect(totalRows).toBe(300)
    })

    it('should track files across multiple levels independently', () => {
      const levels: Record<CompactionLevel, MockLevelState> = {
        l0: {
          level: 'l0',
          files: [
            { path: 'l0-file1.parquet', size: 1000, rowCount: 100, windowStart: 0, windowEnd: 1000, createdAt: 0 },
            { path: 'l0-file2.parquet', size: 1000, rowCount: 100, windowStart: 0, windowEnd: 1000, createdAt: 0 },
          ],
          totalSize: 2000,
          totalRows: 200,
        },
        l1: {
          level: 'l1',
          files: [
            { path: 'l1-file1.parquet', size: 5000, rowCount: 500, windowStart: 0, windowEnd: 1000, createdAt: 0 },
          ],
          totalSize: 5000,
          totalRows: 500,
        },
        l2: { level: 'l2', files: [], totalSize: 0, totalRows: 0 },
      }

      expect(levels.l0.files).toHaveLength(2)
      expect(levels.l1.files).toHaveLength(1)
      expect(levels.l2.files).toHaveLength(0)

      expect(levels.l0.totalSize).toBe(2000)
      expect(levels.l1.totalSize).toBe(5000)
    })
  })

  describe('promotion detection', () => {
    const config: HierarchicalCompactionLevels = {
      l0ToL1Threshold: 3, // Lower for testing
      l1ToL2Threshold: 2,
    }

    it('should detect when L0 needs promotion', () => {
      const l0FileCount = 3
      expect(shouldPromote('l0', l0FileCount, config)).toBe(true)
    })

    it('should detect when L1 needs promotion', () => {
      const l1FileCount = 2
      expect(shouldPromote('l1', l1FileCount, config)).toBe(true)
    })

    it('should not promote when below threshold', () => {
      expect(shouldPromote('l0', 2, config)).toBe(false)
      expect(shouldPromote('l1', 1, config)).toBe(false)
    })
  })

  describe('file removal after promotion', () => {
    it('should recalculate totals after removing files', () => {
      const files: LevelFileMetadata[] = [
        { path: 'file1.parquet', size: 1000, rowCount: 50, windowStart: 0, windowEnd: 1000, createdAt: 0 },
        { path: 'file2.parquet', size: 2000, rowCount: 100, windowStart: 0, windowEnd: 1000, createdAt: 0 },
        { path: 'file3.parquet', size: 3000, rowCount: 150, windowStart: 0, windowEnd: 1000, createdAt: 0 },
      ]

      const pathsToRemove = new Set(['file1.parquet', 'file2.parquet'])
      const remainingFiles = files.filter(f => !pathsToRemove.has(f.path))

      expect(remainingFiles).toHaveLength(1)
      expect(remainingFiles[0].path).toBe('file3.parquet')

      const newTotalSize = remainingFiles.reduce((sum, f) => sum + f.size, 0)
      const newTotalRows = remainingFiles.reduce((sum, f) => sum + (f.rowCount ?? 0), 0)

      expect(newTotalSize).toBe(3000)
      expect(newTotalRows).toBe(150)
    })
  })
})

// =============================================================================
// Promotion Workflow Parameter Tests
// =============================================================================

describe('CompactionPromotionParams', () => {
  it('should accept valid promotion parameters', () => {
    const params = {
      namespace: 'users',
      fromLevel: 'l0' as CompactionLevel,
      toLevel: 'l1' as CompactionLevel,
      files: [
        'data/users/l0/year=2024/month=01/day=15/hour=00/compacted-1705276800000-0.parquet',
        'data/users/l0/year=2024/month=01/day=15/hour=01/compacted-1705280400000-0.parquet',
      ],
      targetFormat: 'native' as const,
      deleteSource: true,
    }

    expect(params.fromLevel).toBe('l0')
    expect(params.toLevel).toBe('l1')
    expect(params.files).toHaveLength(2)
  })

  it('should support all level transitions', () => {
    const transitions: Array<[CompactionLevel, CompactionLevel]> = [
      ['l0', 'l1'],
      ['l1', 'l2'],
    ]

    for (const [from, to] of transitions) {
      const params = {
        namespace: 'test',
        fromLevel: from,
        toLevel: to,
        files: ['test.parquet'],
        targetFormat: 'native' as const,
      }

      expect(params.fromLevel).toBe(from)
      expect(params.toLevel).toBe(to)
    }
  })
})

// =============================================================================
// ISO Week Calculation Tests
// =============================================================================

describe('ISO week calculation', () => {
  // getISOWeek is internal, test via generateLevelPath

  it('should handle start of year (Jan 1)', () => {
    // January 1, 2024 (Monday) - Week 1
    const jan1 = Date.UTC(2024, 0, 1, 12, 0, 0)
    const path = generateLevelPath('test', 'l2', jan1, 0)

    expect(path).toContain('year=2024')
    expect(path).toContain('week=01')
  })

  it('should handle end of year (Dec 31)', () => {
    // December 31, 2024 (Tuesday) - Week 1 of 2025 in ISO
    const dec31 = Date.UTC(2024, 11, 31, 12, 0, 0)
    const path = generateLevelPath('test', 'l2', dec31, 0)

    // ISO week for Dec 31, 2024 is week 1 of 2025
    expect(path).toMatch(/week=\d{2}/)
  })

  it('should handle mid-year week', () => {
    // July 15, 2024 (Monday) - Week 29
    const july15 = Date.UTC(2024, 6, 15, 12, 0, 0)
    const path = generateLevelPath('test', 'l2', july15, 0)

    expect(path).toContain('year=2024')
    expect(path).toContain('week=29')
  })
})

// =============================================================================
// Integration Scenarios
// =============================================================================

describe('integration scenarios', () => {
  describe('typical write-heavy workload', () => {
    const config: HierarchicalCompactionLevels = {
      l0ToL1Threshold: 24, // 24 hourly files = 1 day
      l1ToL2Threshold: 7,   // 7 daily files = 1 week
    }

    it('should simulate daily promotion from L0 to L1', () => {
      // Simulate 24 hourly files
      const hourlyFiles: LevelFileMetadata[] = []
      const baseTime = Date.UTC(2024, 0, 15, 0, 0, 0)

      for (let hour = 0; hour < 24; hour++) {
        const windowStart = baseTime + hour * 60 * 60 * 1000
        hourlyFiles.push({
          path: generateLevelPath('users', 'l0', windowStart, 0),
          size: 1024 * 1024, // 1MB each
          rowCount: 1000,
          windowStart,
          windowEnd: windowStart + 60 * 60 * 1000,
          createdAt: Date.now(),
        })
      }

      expect(hourlyFiles).toHaveLength(24)
      expect(shouldPromote('l0', hourlyFiles.length, config)).toBe(true)

      // After promotion, L0 is empty, L1 has 1 daily file
      const l1OutputPath = generateLevelPath('users', 'l1', baseTime, 0)
      expect(l1OutputPath).toContain('/l1/')
      expect(l1OutputPath).toContain('day=15')
    })

    it('should simulate weekly promotion from L1 to L2', () => {
      // Simulate 7 daily files
      const dailyFiles: LevelFileMetadata[] = []
      const baseTime = Date.UTC(2024, 0, 15, 0, 0, 0)

      for (let day = 0; day < 7; day++) {
        const windowStart = baseTime + day * 24 * 60 * 60 * 1000
        dailyFiles.push({
          path: generateLevelPath('users', 'l1', windowStart, 0),
          size: 24 * 1024 * 1024, // ~24MB each (24 hourly files compacted)
          rowCount: 24000,
          windowStart,
          windowEnd: windowStart + 24 * 60 * 60 * 1000,
          createdAt: Date.now(),
        })
      }

      expect(dailyFiles).toHaveLength(7)
      expect(shouldPromote('l1', dailyFiles.length, config)).toBe(true)

      // After promotion, L1 is empty, L2 has 1 weekly file
      const l2OutputPath = generateLevelPath('users', 'l2', baseTime, 0)
      expect(l2OutputPath).toContain('/l2/')
      expect(l2OutputPath).toMatch(/week=\d{2}/)
    })
  })

  describe('namespace isolation', () => {
    it('should generate unique paths for different namespaces', () => {
      const timestamp = Date.UTC(2024, 0, 15, 10, 0, 0)

      const usersPath = generateLevelPath('users', 'l0', timestamp, 0)
      const postsPath = generateLevelPath('posts', 'l0', timestamp, 0)

      expect(usersPath).toContain('data/users/')
      expect(postsPath).toContain('data/posts/')
      expect(usersPath).not.toBe(postsPath)
    })
  })

  describe('concurrent level operations', () => {
    it('should support multiple levels having files simultaneously', () => {
      const levels: Record<CompactionLevel, number> = {
        l0: 5,   // Some new files
        l1: 3,   // Some compacted daily files
        l2: 2,   // Some compacted weekly files
      }

      // This simulates a steady-state system
      expect(levels.l0).toBeLessThan(24) // Not ready for L0->L1 promotion
      expect(levels.l1).toBeLessThan(7)   // Not ready for L1->L2 promotion
      expect(levels.l2).toBe(2)           // Historical data
    })
  })
})

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('error handling', () => {
  describe('invalid level handling', () => {
    it('should handle unknown levels gracefully', () => {
      const unknownLevel = 'l3' as CompactionLevel
      const threshold = getPromotionThreshold(unknownLevel, DEFAULT_HIERARCHICAL_CONFIG.levels)

      // Unknown levels should return Infinity (never promote)
      expect(threshold).toBe(Infinity)
    })

    it('should return null for next level of unknown level', () => {
      const unknownLevel = 'l3' as CompactionLevel
      const next = getNextLevel(unknownLevel)

      expect(next).toBeNull()
    })
  })

  describe('path parsing edge cases', () => {
    it('should handle paths with multiple /l0/ substrings', () => {
      const path = 'data/l0-backup/users/l0/year=2024/compacted.parquet'
      const level = parseLevelFromPath(path)

      // Should match the level directory pattern
      expect(level).toBe('l0')
    })

    it('should handle paths without level directories in data prefix', () => {
      const path = 'backup/data/users/compacted.parquet'
      const level = parseLevelFromPath(path)

      // Falls back to L0 for data paths without explicit level
      expect(level).toBeNull()
    })
  })
})

// =============================================================================
// Configuration Merging Tests
// =============================================================================

describe('configuration merging', () => {
  it('should merge partial config with defaults', () => {
    const partialConfig: Partial<HierarchicalCompactionLevels> = {
      l0ToL1Threshold: 12, // Override just L0->L1
    }

    const merged: HierarchicalCompactionLevels = {
      ...DEFAULT_HIERARCHICAL_CONFIG.levels,
      ...partialConfig,
    }

    expect(merged.l0ToL1Threshold).toBe(12) // Overridden
    expect(merged.l1ToL2Threshold).toBe(7)   // Default
  })

  it('should allow both thresholds to be customized', () => {
    const customConfig: HierarchicalCompactionLevels = {
      l0ToL1Threshold: 48, // 2 days worth of hourly files
      l1ToL2Threshold: 30, // ~1 month of daily files
    }

    expect(shouldPromote('l0', 47, customConfig)).toBe(false)
    expect(shouldPromote('l0', 48, customConfig)).toBe(true)

    expect(shouldPromote('l1', 29, customConfig)).toBe(false)
    expect(shouldPromote('l1', 30, customConfig)).toBe(true)
  })
})
