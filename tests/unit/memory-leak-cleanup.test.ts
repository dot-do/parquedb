/**
 * Memory Leak Cleanup Tests (RED)
 *
 * Tests for verifying that global metrics and caches are properly cleaned up
 * when ParqueDB instances are disposed. These tests address the memory leak
 * in globalFireAndForgetMetrics (src/utils/fire-and-forget.ts:246) which
 * accumulates metrics forever without cleanup on disposeAsync().
 *
 * Issue: parquedb-05f6 - Memory leak in globalFireAndForgetMetrics
 *
 * These tests verify:
 * 1. After disposeAsync(), globalFireAndForgetMetrics is cleared
 * 2. Global caches are cleared on dispose
 * 3. No memory growth after repeated create/dispose cycles
 * 4. WeakMap references are released
 *
 * @module tests/unit/memory-leak-cleanup
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { ParqueDB } from '@/ParqueDB'
import {
  globalFireAndForgetMetrics,
  fireAndForget,
} from '@/utils/fire-and-forget'
import {
  getEntityStore,
  getEventStore,
  getArchivedEventStore,
  getSnapshotStore,
  getQueryStatsStore,
  getReverseRelIndex,
  getEntityEventIndex,
  getReconstructionCache,
  clearGlobalState,
} from '@/ParqueDB/store'
import { MemoryBackend } from '@/storage/MemoryBackend'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Wait for pending promises to settle with timeout
 */
async function flushPromises(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 100))
}

/**
 * Track memory usage approximation by counting entries in global stores
 */
function countGlobalStoreEntries(storage: MemoryBackend): number {
  let count = 0

  // Count entity store entries
  const entityStore = getEntityStore(storage)
  count += entityStore.size

  // Count event store entries
  const eventStore = getEventStore(storage)
  count += eventStore.length

  // Count archived event store entries
  const archivedEventStore = getArchivedEventStore(storage)
  count += archivedEventStore.length

  // Count snapshot store entries
  const snapshotStore = getSnapshotStore(storage)
  count += snapshotStore.length

  // Count query stats entries
  const queryStats = getQueryStatsStore(storage)
  count += queryStats.size

  // Count reverse relationship index entries
  const reverseRelIndex = getReverseRelIndex(storage)
  count += reverseRelIndex.size

  // Count entity event index entries
  const entityEventIndex = getEntityEventIndex(storage)
  count += entityEventIndex.size

  // Count reconstruction cache entries
  const reconstructionCache = getReconstructionCache(storage)
  count += reconstructionCache.size

  return count
}

// =============================================================================
// Test Suite: globalFireAndForgetMetrics Cleanup
// =============================================================================

describe('Memory Leak Cleanup', () => {
  // Set reasonable test timeout
  const TEST_TIMEOUT = 5000

  beforeEach(() => {
    // Reset global metrics before each test
    globalFireAndForgetMetrics.reset()
  })

  // ===========================================================================
  // globalFireAndForgetMetrics Cleanup Tests
  // ===========================================================================

  describe('globalFireAndForgetMetrics cleanup on dispose', () => {
    it('should demonstrate that metrics accumulate without explicit reset', { timeout: TEST_TIMEOUT }, async () => {
      // Trigger fire-and-forget operations
      fireAndForget('auto-snapshot', async () => {})
      fireAndForget('periodic-flush', async () => {})
      fireAndForget('cache-cleanup', async () => {})

      await flushPromises()

      // Verify metrics have accumulated
      const metrics = globalFireAndForgetMetrics.getAggregatedMetrics()
      expect(metrics.totalStarted).toBe(3)
      expect(metrics.totalSucceeded).toBe(3)

      // This test passes - it demonstrates that metrics accumulate
      // The next tests verify that disposeAsync SHOULD reset them (but currently doesn't)
    })

    it('should clear globalFireAndForgetMetrics after disposeAsync() - FAILS until cleanup implemented', { timeout: TEST_TIMEOUT }, async () => {
      // This test will FAIL until we implement cleanup in disposeAsync()
      // It demonstrates the expected behavior after the fix

      const storage = new MemoryBackend()
      const db = new ParqueDB({ storage })

      // Trigger fire-and-forget operations
      fireAndForget('auto-snapshot', async () => {})
      fireAndForget('periodic-flush', async () => {})

      await flushPromises()

      // Verify metrics accumulated
      const metricsBefore = globalFireAndForgetMetrics.getAggregatedMetrics()
      expect(metricsBefore.totalStarted).toBeGreaterThan(0)

      // Call disposeAsync which should reset globalFireAndForgetMetrics
      await db.disposeAsync()

      // After dispose, globalFireAndForgetMetrics should be cleared
      const metricsAfter = globalFireAndForgetMetrics.getAggregatedMetrics()
      expect(metricsAfter.totalStarted).toBe(0)
      expect(metricsAfter.totalSucceeded).toBe(0)
      expect(metricsAfter.totalFailed).toBe(0)
      expect(metricsAfter.totalRetries).toBe(0)
    })

    it('should clear metrics for all operation types after disposeAsync() - FAILS until cleanup implemented', { timeout: TEST_TIMEOUT }, async () => {
      const storage = new MemoryBackend()
      const db = new ParqueDB({ storage })

      // Trigger various fire-and-forget operation types
      fireAndForget('auto-snapshot', async () => {})
      fireAndForget('periodic-flush', async () => {})
      fireAndForget('cache-cleanup', async () => {})

      await flushPromises()

      // Verify multiple operation types have metrics
      const autoSnapshotBefore = globalFireAndForgetMetrics.getMetrics('auto-snapshot')
      const periodicFlushBefore = globalFireAndForgetMetrics.getMetrics('periodic-flush')
      const cacheCleanupBefore = globalFireAndForgetMetrics.getMetrics('cache-cleanup')

      expect(autoSnapshotBefore.started).toBe(1)
      expect(periodicFlushBefore.started).toBe(1)
      expect(cacheCleanupBefore.started).toBe(1)

      // Call disposeAsync which should reset metrics
      await db.disposeAsync()

      // All operation type metrics should be cleared
      const autoSnapshotAfter = globalFireAndForgetMetrics.getMetrics('auto-snapshot')
      const periodicFlushAfter = globalFireAndForgetMetrics.getMetrics('periodic-flush')
      const cacheCleanupAfter = globalFireAndForgetMetrics.getMetrics('cache-cleanup')

      expect(autoSnapshotAfter.started).toBe(0)
      expect(periodicFlushAfter.started).toBe(0)
      expect(cacheCleanupAfter.started).toBe(0)
    })
  })

  // ===========================================================================
  // Global Cache Cleanup Tests
  // ===========================================================================

  describe('Global caches cleared on dispose', () => {
    it('should clear all global stores after clearGlobalState()', { timeout: TEST_TIMEOUT }, async () => {
      const storage = new MemoryBackend()

      // Populate global stores directly
      const entityStore = getEntityStore(storage)
      entityStore.set('posts/test-1', { $id: 'posts/test-1', $type: 'Post', name: 'Test' } as never)

      const eventStore = getEventStore(storage)
      eventStore.push({ id: 'evt-1', ts: Date.now(), op: 'CREATE', target: 'posts:test-1' } as never)

      const queryStats = getQueryStatsStore(storage)
      queryStats.set('posts', { totalQueries: 1, snapshotHits: 0, snapshotMisses: 1 })

      // Verify stores have data
      const countBefore = countGlobalStoreEntries(storage)
      expect(countBefore).toBeGreaterThan(0)

      // Call clearGlobalState (this IS called by disposeAsync)
      clearGlobalState(storage)

      // Verify all stores are cleared
      const countAfter = countGlobalStoreEntries(storage)
      expect(countAfter).toBe(0)
    })

    it('should clear entity event index after clearGlobalState()', { timeout: TEST_TIMEOUT }, async () => {
      const storage = new MemoryBackend()

      // Populate entity event index
      const entityEventIndex = getEntityEventIndex(storage)
      entityEventIndex.set('posts:test-1', [
        { id: 'evt-1', ts: Date.now(), op: 'CREATE', target: 'posts:test-1' } as never,
        { id: 'evt-2', ts: Date.now(), op: 'UPDATE', target: 'posts:test-1' } as never,
      ])

      expect(entityEventIndex.size).toBe(1)

      // Call clearGlobalState
      clearGlobalState(storage)

      // Entity event index should be cleared
      const entityEventIndexAfter = getEntityEventIndex(storage)
      expect(entityEventIndexAfter.size).toBe(0)
    })

    it('should clear reconstruction cache after clearGlobalState()', { timeout: TEST_TIMEOUT }, async () => {
      const storage = new MemoryBackend()

      // Populate reconstruction cache
      const reconstructionCache = getReconstructionCache(storage)
      reconstructionCache.set('posts/test-1:12345', {
        entity: { $id: 'posts/test-1', $type: 'Post', name: 'Test' } as never,
        timestamp: Date.now(),
      })

      expect(reconstructionCache.size).toBe(1)

      // Call clearGlobalState
      clearGlobalState(storage)

      // Reconstruction cache should be cleared
      const reconstructionCacheAfter = getReconstructionCache(storage)
      expect(reconstructionCacheAfter.size).toBe(0)
    })

    it('should clear reverse relationship index after clearGlobalState()', { timeout: TEST_TIMEOUT }, async () => {
      const storage = new MemoryBackend()

      // Populate reverse relationship index
      const reverseRelIndex = getReverseRelIndex(storage)
      const targetMap = new Map<string, Set<string>>()
      targetMap.set('posts.author', new Set(['posts/test-1', 'posts/test-2']))
      reverseRelIndex.set('users/test-1', targetMap)

      expect(reverseRelIndex.size).toBe(1)

      // Call clearGlobalState
      clearGlobalState(storage)

      // Reverse relationship index should be cleared
      const reverseRelIndexAfter = getReverseRelIndex(storage)
      expect(reverseRelIndexAfter.size).toBe(0)
    })
  })

  // ===========================================================================
  // Memory Growth Prevention Tests
  // ===========================================================================

  describe('No memory growth after repeated create/dispose cycles', () => {
    it('should not accumulate metrics across multiple dispose cycles - FAILS until cleanup implemented', { timeout: TEST_TIMEOUT }, async () => {
      const cycleCount = 3
      const metricsHistory: number[] = []

      for (let i = 0; i < cycleCount; i++) {
        const storage = new MemoryBackend()
        const db = new ParqueDB({ storage })

        // Do some operations that trigger fire-and-forget
        fireAndForget('auto-snapshot', async () => {})

        await flushPromises()

        // Record metrics before dispose
        const metrics = globalFireAndForgetMetrics.getAggregatedMetrics()
        metricsHistory.push(metrics.totalStarted)

        // Call disposeAsync which should reset metrics
        await db.disposeAsync()
      }

      // Metrics should NOT accumulate across cycles
      // If there's a memory leak, metricsHistory values will keep increasing: [1, 2, 3]
      // After fix, they should be constant: [1, 1, 1]
      const lastMetric = metricsHistory[metricsHistory.length - 1]
      const firstMetric = metricsHistory[0]

      // After dispose, metrics should be reset to 0 or a constant value
      // They should not be N times the first value
      expect(lastMetric).toBeLessThanOrEqual(firstMetric!)
    })

    it('should not accumulate global store entries across multiple create/dispose cycles', { timeout: TEST_TIMEOUT }, async () => {
      const cycleCount = 3
      const storageInstances: MemoryBackend[] = []

      for (let i = 0; i < cycleCount; i++) {
        const storage = new MemoryBackend()

        // Populate some data
        const entityStore = getEntityStore(storage)
        entityStore.set(`posts/test-${i}`, { $id: `posts/test-${i}`, $type: 'Post', name: 'Test' } as never)

        // Keep reference to storage to check for leaks
        storageInstances.push(storage)

        // Call clearGlobalState (what disposeAsync does)
        clearGlobalState(storage)
      }

      // After disposal, all storage instances should have zero entries in global stores
      for (const storage of storageInstances) {
        const count = countGlobalStoreEntries(storage)
        expect(count).toBe(0)
      }
    })

    it('should maintain constant memory footprint with create/update/dispose cycles - FAILS until cleanup implemented', { timeout: TEST_TIMEOUT }, async () => {
      const cycleCount = 3

      // Track that we can repeatedly create and dispose without accumulation
      for (let cycle = 0; cycle < cycleCount; cycle++) {
        const storage = new MemoryBackend()
        const db = new ParqueDB({ storage })

        // Simulate operations
        const entityStore = getEntityStore(storage)
        entityStore.set(`posts/test-${cycle}`, { $id: `posts/test-${cycle}`, $type: 'Post', name: 'Test' } as never)

        fireAndForget('auto-snapshot', async () => {})

        await flushPromises()

        // Call disposeAsync which should reset metrics
        await db.disposeAsync()

        // After dispose, global stores for this storage should be empty
        const count = countGlobalStoreEntries(storage)
        expect(count).toBe(0)
      }

      // Global metrics should also be cleared after the last dispose
      const finalMetrics = globalFireAndForgetMetrics.getAggregatedMetrics()
      expect(finalMetrics.totalStarted).toBe(0)
    })
  })

  // ===========================================================================
  // WeakMap Reference Release Tests
  // ===========================================================================

  describe('WeakMap references released', () => {
    it('should release WeakMap entry when clearGlobalState is called', { timeout: TEST_TIMEOUT }, async () => {
      const storage = new MemoryBackend()

      // Populate data
      const entityStore = getEntityStore(storage)
      entityStore.set('posts/test-1', { $id: 'posts/test-1', $type: 'Post', name: 'Test' } as never)

      // Verify data exists
      const entityStoreBefore = getEntityStore(storage)
      expect(entityStoreBefore.size).toBe(1)

      // Call clearGlobalState
      clearGlobalState(storage)

      // After clear, stores should return fresh empty instances
      const entityStoreAfter = getEntityStore(storage)
      expect(entityStoreAfter.size).toBe(0)
    })

    it('should allow garbage collection after clearGlobalState', { timeout: TEST_TIMEOUT }, async () => {
      // This test verifies that disposed storage backends can be garbage collected

      const disposeStorage = async (): Promise<void> => {
        const storage = new MemoryBackend()

        const entityStore = getEntityStore(storage)
        entityStore.set('posts/test-1', { $id: 'posts/test-1', $type: 'Post', name: 'Test' } as never)

        clearGlobalState(storage)
        // After this function returns, storage should be eligible for GC
      }

      // Run dispose multiple times
      for (let i = 0; i < 3; i++) {
        await disposeStorage()
      }

      // Force GC if available (Node.js with --expose-gc flag)
      if (global.gc) {
        global.gc()
      }

      // This test mainly verifies no crashes or errors occur
      expect(true).toBe(true)
    })
  })

  // ===========================================================================
  // Integration: Full Lifecycle Cleanup
  // ===========================================================================

  describe('Full lifecycle cleanup', () => {
    it('should clean up all resources including metrics - FAILS until cleanup implemented', { timeout: TEST_TIMEOUT }, async () => {
      const storage = new MemoryBackend()
      const db = new ParqueDB({ storage })

      // Simulate full lifecycle
      const entityStore = getEntityStore(storage)
      entityStore.set('posts/test-1', { $id: 'posts/test-1', $type: 'Post', name: 'Test' } as never)

      fireAndForget('auto-snapshot', async () => {})
      fireAndForget('cache-cleanup', async () => {})

      await flushPromises()

      // Call disposeAsync which clears both stores and metrics
      await db.disposeAsync()

      // Verify store cleanup
      const storeCount = countGlobalStoreEntries(storage)
      expect(storeCount).toBe(0)

      // Verify metrics cleanup
      const metrics = globalFireAndForgetMetrics.getAggregatedMetrics()
      expect(metrics.totalStarted).toBe(0)
    })
  })
})
