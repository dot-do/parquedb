/**
 * Tests for RetentionManager - Data Retention and Compaction
 *
 * Tests the retention management functionality including:
 * - Batch cleanup operations
 * - Tiered retention policies
 * - Background scheduling
 * - Progress tracking
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  RetentionManager,
  createRetentionManager,
  type RetentionManagerConfig,
  type TieredRetentionPolicies,
  type CleanupProgress,
} from '../../../../src/observability/retention'

// =============================================================================
// Mock ParqueDB
// =============================================================================

interface MockRecord {
  $id: string
  timestamp: Date
  granularity?: string | undefined
  [key: string]: unknown
}

function createMockCollection() {
  const store: Map<string, MockRecord> = new Map()
  let idCounter = 0

  return {
    store,
    create: vi.fn(async (data: Record<string, unknown>) => {
      const id = `test/${++idCounter}`
      const record = { $id: id, ...data } as MockRecord
      store.set(id, record)
      return record
    }),
    find: vi.fn(async (filter?: Record<string, unknown>, options?: { limit?: number; sort?: Record<string, number> }) => {
      let results = Array.from(store.values())

      // Apply filters
      if (filter) {
        results = results.filter(item => {
          for (const [key, value] of Object.entries(filter)) {
            if (key === 'timestamp' && typeof value === 'object' && value !== null) {
              const ts = item.timestamp
              const filterObj = value as Record<string, unknown>
              if (filterObj.$lt && ts >= (filterObj.$lt as Date)) return false
              if (filterObj.$gte && ts < (filterObj.$gte as Date)) return false
            } else if (key === 'granularity') {
              if (typeof value === 'object' && value !== null) {
                const filterObj = value as Record<string, unknown>
                if (filterObj.$exists === false && item.granularity !== undefined) return false
              } else if (item.granularity !== value) {
                return false
              }
            } else if (typeof value !== 'object' && item[key] !== value) {
              return false
            }
          }
          return true
        })
      }

      // Apply sorting
      if (options?.sort) {
        const [sortKey, sortDir] = Object.entries(options.sort)[0]!
        results.sort((a, b) => {
          const aVal = a[sortKey as keyof MockRecord]
          const bVal = b[sortKey as keyof MockRecord]
          if (aVal instanceof Date && bVal instanceof Date) {
            return sortDir > 0 ? aVal.getTime() - bVal.getTime() : bVal.getTime() - aVal.getTime()
          }
          return 0
        })
      }

      // Apply limit
      if (options?.limit) {
        results = results.slice(0, options.limit)
      }

      return { items: results }
    }),
    count: vi.fn(async (filter?: Record<string, unknown>) => {
      let results = Array.from(store.values())

      if (filter) {
        results = results.filter(item => {
          for (const [key, value] of Object.entries(filter)) {
            if (key === 'timestamp' && typeof value === 'object' && value !== null) {
              const ts = item.timestamp
              const filterObj = value as Record<string, unknown>
              if (filterObj.$lt && ts >= (filterObj.$lt as Date)) return false
              if (filterObj.$gte && ts < (filterObj.$gte as Date)) return false
            } else if (key === 'granularity') {
              if (typeof value === 'object' && value !== null) {
                const filterObj = value as Record<string, unknown>
                if (filterObj.$exists === false && item.granularity !== undefined) return false
              } else if (item.granularity !== value) {
                return false
              }
            } else if (typeof value !== 'object' && item[key] !== value) {
              return false
            }
          }
          return true
        })
      }

      return results.length
    }),
    deleteMany: vi.fn(async (filter: Record<string, unknown>, _options?: { hard?: boolean }) => {
      let toDelete: string[] = []

      for (const [id, item] of store) {
        let matches = true
        for (const [key, value] of Object.entries(filter)) {
          if (key === 'timestamp' && typeof value === 'object' && value !== null) {
            const ts = item.timestamp
            const filterObj = value as Record<string, unknown>
            if (filterObj.$lt && ts >= (filterObj.$lt as Date)) matches = false
            if (filterObj.$gte && ts < (filterObj.$gte as Date)) matches = false
          } else if (key === 'granularity') {
            if (typeof value === 'object' && value !== null) {
              const filterObj = value as Record<string, unknown>
              if (filterObj.$exists === false && item.granularity !== undefined) matches = false
            } else if (item.granularity !== value) {
              matches = false
            }
          } else if (typeof value !== 'object' && item[key] !== value) {
            matches = false
          }
        }
        if (matches) {
          toDelete.push(id)
        }
      }

      for (const id of toDelete) {
        store.delete(id)
      }

      return { deletedCount: toDelete.length }
    }),
  }
}

function createMockDB() {
  const collections: Map<string, ReturnType<typeof createMockCollection>> = new Map()

  const getOrCreateCollection = (name: string) => {
    if (!collections.has(name)) {
      collections.set(name, createMockCollection())
    }
    return collections.get(name)!
  }

  return {
    collections,
    collection: vi.fn((name: string) => getOrCreateCollection(name)),
    deleteMany: vi.fn(async (collectionName: string, filter: Record<string, unknown>, options?: { hard?: boolean }) => {
      const col = getOrCreateCollection(collectionName)
      return col.deleteMany(filter, options)
    }),
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

function addTestRecords(
  collection: ReturnType<typeof createMockCollection>,
  records: Array<{ timestamp: Date; granularity?: string; [key: string]: unknown }>
): void {
  for (const record of records) {
    collection.store.set(`test/${collection.store.size + 1}`, {
      $id: `test/${collection.store.size + 1}`,
      ...record,
    })
  }
}

// =============================================================================
// Constructor Tests
// =============================================================================

describe('RetentionManager', () => {
  describe('constructor', () => {
    it('should create an instance with default config', () => {
      const db = createMockDB() as unknown as Parameters<typeof createRetentionManager>[0]
      const manager = new RetentionManager(db, { collection: 'test_collection' })

      expect(manager).toBeInstanceOf(RetentionManager)
      const config = manager.getConfig()
      expect(config.collection).toBe('test_collection')
      expect(config.timestampField).toBe('timestamp')
      expect(config.granularityField).toBe('granularity')
    })

    it('should accept custom configuration', () => {
      const db = createMockDB() as unknown as Parameters<typeof createRetentionManager>[0]
      const config: RetentionManagerConfig = {
        collection: 'custom_collection',
        policies: {
          hourly: { maxAgeMs: 1000, enabled: true },
          daily: { maxAgeMs: 5000, enabled: true },
        },
        batchSize: 500,
        timestampField: 'createdAt',
        granularityField: 'period',
        debug: true,
      }

      const manager = new RetentionManager(db, config)
      const resolvedConfig = manager.getConfig()

      expect(resolvedConfig.collection).toBe('custom_collection')
      expect(resolvedConfig.batchSize).toBe(500)
      expect(resolvedConfig.timestampField).toBe('createdAt')
      expect(resolvedConfig.granularityField).toBe('period')
      expect(resolvedConfig.debug).toBe(true)
      expect(resolvedConfig.policies.hourly?.maxAgeMs).toBe(1000)
    })

    it('should use maxAgeMs as default policy when no policies provided', () => {
      const db = createMockDB() as unknown as Parameters<typeof createRetentionManager>[0]
      const manager = new RetentionManager(db, {
        collection: 'test',
        maxAgeMs: 86400000, // 1 day
      })

      const config = manager.getConfig()
      expect(config.policies.default?.maxAgeMs).toBe(86400000)
    })
  })

  describe('createRetentionManager factory', () => {
    it('should create a RetentionManager instance', () => {
      const db = createMockDB() as unknown as Parameters<typeof createRetentionManager>[0]
      const manager = createRetentionManager(db, { collection: 'test' })

      expect(manager).toBeInstanceOf(RetentionManager)
    })
  })
})

// =============================================================================
// Cleanup Tests
// =============================================================================

describe('Cleanup Operations', () => {
  let db: ReturnType<typeof createMockDB>
  let manager: RetentionManager

  beforeEach(() => {
    db = createMockDB()
    manager = new RetentionManager(db as unknown as Parameters<typeof createRetentionManager>[0], {
      collection: 'test_records',
      policies: {
        hourly: { maxAgeMs: 1000, enabled: true },  // 1 second
        daily: { maxAgeMs: 5000, enabled: true },   // 5 seconds
        monthly: { maxAgeMs: 10000, enabled: true }, // 10 seconds
        default: { maxAgeMs: 3000, enabled: true },  // 3 seconds
      },
    })
  })

  describe('cleanup', () => {
    it('should delete old records using batch delete', async () => {
      const collection = db.collection('test_records')
      const now = Date.now()

      // Add old records (older than 1 second)
      addTestRecords(collection, [
        { timestamp: new Date(now - 5000), granularity: 'hour' },
        { timestamp: new Date(now - 3000), granularity: 'hour' },
        { timestamp: new Date(now - 2000), granularity: 'hour' },
      ])

      // Add recent records
      addTestRecords(collection, [
        { timestamp: new Date(now - 500), granularity: 'hour' },
        { timestamp: new Date(now - 100), granularity: 'hour' },
      ])

      const result = await manager.cleanup()

      expect(result.success).toBe(true)
      expect(result.deletedCount).toBe(3)
      expect(result.byGranularity.hour).toBe(3)
    })

    it('should respect granularity-specific policies', async () => {
      const collection = db.collection('test_records')
      const now = Date.now()

      // Hourly records - policy: 1 second
      addTestRecords(collection, [
        { timestamp: new Date(now - 2000), granularity: 'hour' }, // Should be deleted
        { timestamp: new Date(now - 500), granularity: 'hour' },  // Should be kept
      ])

      // Daily records - policy: 5 seconds
      addTestRecords(collection, [
        { timestamp: new Date(now - 6000), granularity: 'day' },  // Should be deleted
        { timestamp: new Date(now - 2000), granularity: 'day' },  // Should be kept
      ])

      // Monthly records - policy: 10 seconds
      addTestRecords(collection, [
        { timestamp: new Date(now - 15000), granularity: 'month' }, // Should be deleted
        { timestamp: new Date(now - 5000), granularity: 'month' },  // Should be kept
      ])

      const result = await manager.cleanup()

      expect(result.success).toBe(true)
      expect(result.byGranularity.hour).toBe(1)
      expect(result.byGranularity.day).toBe(1)
      expect(result.byGranularity.month).toBe(1)
      expect(result.deletedCount).toBe(3)
    })

    it('should handle records without granularity using default policy', async () => {
      const collection = db.collection('test_records')
      const now = Date.now()

      // Records without granularity - default policy: 3 seconds
      addTestRecords(collection, [
        { timestamp: new Date(now - 5000) }, // Should be deleted
        { timestamp: new Date(now - 4000) }, // Should be deleted
        { timestamp: new Date(now - 1000) }, // Should be kept
      ])

      const result = await manager.cleanup()

      expect(result.success).toBe(true)
      expect(result.byGranularity.default).toBe(2)
    })

    it('should return zero when no records to delete', async () => {
      const collection = db.collection('test_records')
      const now = Date.now()

      // Add only recent records
      addTestRecords(collection, [
        { timestamp: new Date(now - 100), granularity: 'hour' },
        { timestamp: new Date(now - 200), granularity: 'day' },
      ])

      const result = await manager.cleanup()

      expect(result.success).toBe(true)
      expect(result.deletedCount).toBe(0)
    })

    it('should call progress callback during cleanup', async () => {
      const collection = db.collection('test_records')
      const now = Date.now()

      addTestRecords(collection, [
        { timestamp: new Date(now - 5000), granularity: 'hour' },
        { timestamp: new Date(now - 5000), granularity: 'hour' },
      ])

      const progressCalls: CleanupProgress[] = []
      const result = await manager.cleanup((progress) => {
        progressCalls.push({ ...progress })
      })

      expect(result.success).toBe(true)
      expect(progressCalls.length).toBeGreaterThan(0)
      expect(progressCalls[progressCalls.length - 1]!.phase).toBe('complete')
      expect(progressCalls[progressCalls.length - 1]!.percentage).toBe(100)
    })
  })

  describe('cleanupBefore', () => {
    it('should delete records older than cutoff date', async () => {
      const collection = db.collection('test_records')
      const now = Date.now()

      addTestRecords(collection, [
        { timestamp: new Date(now - 10000) },
        { timestamp: new Date(now - 5000) },
        { timestamp: new Date(now - 1000) },
      ])

      const cutoff = new Date(now - 3000)
      const deletedCount = await manager.cleanupBefore(cutoff)

      expect(deletedCount).toBe(2)
    })
  })
})

// =============================================================================
// Scheduler Tests
// =============================================================================

describe('Cleanup Scheduler', () => {
  let db: ReturnType<typeof createMockDB>
  let manager: RetentionManager

  beforeEach(() => {
    vi.useFakeTimers()
    db = createMockDB()
    manager = new RetentionManager(db as unknown as Parameters<typeof createRetentionManager>[0], {
      collection: 'test_records',
      maxAgeMs: 1000,
    })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('scheduleCleanup', () => {
    it('should schedule periodic cleanup', async () => {
      const completeCalls: any[] = []
      const scheduler = manager.scheduleCleanup({
        intervalMs: 5000,
        onComplete: (result) => completeCalls.push(result),
      })

      expect(scheduler.isRunning()).toBe(true)
      expect(scheduler.nextRunAt()).not.toBeNull()

      // Clean up
      scheduler.stop()
    })

    it('should run immediately when runImmediately is true', async () => {
      const completeCalls: any[] = []
      const scheduler = manager.scheduleCleanup({
        intervalMs: 5000,
        runImmediately: true,
        onComplete: (result) => completeCalls.push(result),
      })

      // Allow the immediate run to complete
      await vi.advanceTimersByTimeAsync(0)

      expect(completeCalls.length).toBe(1)

      scheduler.stop()
    })

    it('should support pause and resume', () => {
      const scheduler = manager.scheduleCleanup({
        intervalMs: 5000,
      })

      expect(scheduler.isRunning()).toBe(true)

      scheduler.pause()
      expect(scheduler.isRunning()).toBe(false)

      scheduler.resume()
      expect(scheduler.isRunning()).toBe(true)

      scheduler.stop()
    })

    it('should support manual trigger', async () => {
      const completeCalls: any[] = []
      const scheduler = manager.scheduleCleanup({
        intervalMs: 60000, // Long interval
        onComplete: (result) => completeCalls.push(result),
      })

      const result = await scheduler.trigger()

      expect(result.success).toBe(true)

      scheduler.stop()
    })

    it('should stop scheduling when stopped', () => {
      const scheduler = manager.scheduleCleanup({
        intervalMs: 5000,
      })

      scheduler.stop()

      expect(scheduler.isRunning()).toBe(false)
      expect(scheduler.nextRunAt()).toBeNull()
    })
  })
})

// =============================================================================
// Statistics Tests
// =============================================================================

describe('Retention Statistics', () => {
  let db: ReturnType<typeof createMockDB>
  let manager: RetentionManager

  beforeEach(() => {
    db = createMockDB()
    manager = new RetentionManager(db as unknown as Parameters<typeof createRetentionManager>[0], {
      collection: 'test_records',
      policies: {
        hourly: { maxAgeMs: 1000, enabled: true },
        daily: { maxAgeMs: 5000, enabled: true },
      },
    })
  })

  describe('getRetentionStats', () => {
    it('should return statistics about data eligible for cleanup', async () => {
      const collection = db.collection('test_records')
      const now = Date.now()

      // Add records
      addTestRecords(collection, [
        { timestamp: new Date(now - 5000), granularity: 'hour' },  // Eligible
        { timestamp: new Date(now - 2000), granularity: 'hour' },  // Eligible
        { timestamp: new Date(now - 100), granularity: 'hour' },   // Not eligible
        { timestamp: new Date(now - 10000), granularity: 'day' },  // Eligible
        { timestamp: new Date(now - 1000), granularity: 'day' },   // Not eligible
      ])

      const stats = await manager.getRetentionStats()

      expect(stats.collection).toBe('test_records')
      expect(stats.totalRecords).toBe(5)
      expect(stats.totalEligibleForDeletion).toBeGreaterThan(0)
      expect(stats.byGranularity.hour).toBeDefined()
      expect(stats.byGranularity.day).toBeDefined()
    })

    it('should return oldest timestamp per granularity', async () => {
      const collection = db.collection('test_records')
      const now = Date.now()
      const oldestTimestamp = new Date(now - 10000)

      addTestRecords(collection, [
        { timestamp: oldestTimestamp, granularity: 'hour' },
        { timestamp: new Date(now - 5000), granularity: 'hour' },
        { timestamp: new Date(now - 1000), granularity: 'hour' },
      ])

      const stats = await manager.getRetentionStats()

      expect(stats.byGranularity.hour.oldestTimestamp?.getTime()).toBe(oldestTimestamp.getTime())
    })
  })
})
