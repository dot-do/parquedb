/**
 * MV Storage Manager Tests
 *
 * Tests the MVStorageManager class for:
 * - Manifest loading/saving
 * - View CRUD operations
 * - View data storage
 * - View stats tracking
 *
 * Uses MemoryBackend for testing.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  MVStorageManager,
  MVStoragePaths,
  MVNotFoundError,
  MVAlreadyExistsError,
  MV_MANIFEST_VERSION,
} from '../../../src/materialized-views/storage'
import { viewName } from '../../../src/materialized-views/types'
import type { ViewDefinition } from '../../../src/materialized-views/types'

// =============================================================================
// Test Fixtures
// =============================================================================

function createTestViewDefinition(name: string, source = 'users'): ViewDefinition {
  return {
    name: viewName(name),
    source,
    query: { filter: { status: 'active' } },
    options: { refreshMode: 'manual' },
  }
}

function createScheduledViewDefinition(name: string, source = 'orders'): ViewDefinition {
  return {
    name: viewName(name),
    source,
    query: { filter: { status: 'completed' } },
    options: {
      refreshMode: 'scheduled',
      schedule: { cron: '0 * * * *' },
    },
  }
}

function createStreamingViewDefinition(name: string, source = 'events'): ViewDefinition {
  return {
    name: viewName(name),
    source,
    query: {},
    options: {
      refreshMode: 'streaming',
      maxStalenessMs: 5000,
    },
  }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('MVStorageManager', () => {
  let backend: MemoryBackend
  let storage: MVStorageManager

  beforeEach(() => {
    backend = new MemoryBackend()
    storage = new MVStorageManager(backend)
  })

  // ===========================================================================
  // Manifest Operations
  // ===========================================================================

  describe('manifest operations', () => {
    describe('loadManifest()', () => {
      it('should create empty manifest if none exists', async () => {
        const manifest = await storage.loadManifest()

        expect(manifest).toBeDefined()
        expect(manifest.version).toBe(MV_MANIFEST_VERSION)
        expect(manifest.views).toEqual([])
        expect(manifest.updatedAt).toBeDefined()
      })

      it('should load existing manifest from storage', async () => {
        // Create a view first
        await storage.createView(createTestViewDefinition('test-view'))

        // Create new storage manager to test loading
        const storage2 = new MVStorageManager(backend)
        const manifest = await storage2.loadManifest()

        expect(manifest.views).toHaveLength(1)
        expect(manifest.views[0].name).toBe('test-view')
      })

      it('should cache manifest on load', async () => {
        await storage.createView(createTestViewDefinition('test-view'))

        // Load manifest twice
        const manifest1 = await storage.loadManifest()
        const manifest2 = await storage.loadManifest()

        // Should be the same object (cached)
        expect(manifest1).toBe(manifest2)
      })
    })

    describe('saveManifest()', () => {
      it('should save manifest and update cache', async () => {
        const manifest = await storage.loadManifest()

        manifest.views.push({
          name: 'manual-view',
          state: 'pending',
          source: 'test',
          createdAt: new Date().toISOString(),
          metadataPath: '_views/manual-view/metadata.json',
        })

        await storage.saveManifest(manifest)

        // Reload from new manager
        const storage2 = new MVStorageManager(backend)
        const loaded = await storage2.loadManifest()

        expect(loaded.views).toHaveLength(1)
        expect(loaded.views[0].name).toBe('manual-view')
      })

      it('should update updatedAt timestamp', async () => {
        vi.useFakeTimers()
        const manifest1 = await storage.loadManifest()
        const time1 = manifest1.updatedAt

        // Advance time and save again
        vi.advanceTimersByTime(10)
        await storage.saveManifest(manifest1)

        expect(manifest1.updatedAt).not.toBe(time1)
        vi.useRealTimers()
      })
    })

    describe('invalidateManifestCache()', () => {
      it('should clear cached manifest', async () => {
        const manifest1 = await storage.loadManifest()
        storage.invalidateManifestCache()
        const manifest2 = await storage.loadManifest()

        // Should be different objects
        expect(manifest1).not.toBe(manifest2)
      })
    })
  })

  // ===========================================================================
  // View CRUD Operations
  // ===========================================================================

  describe('view CRUD operations', () => {
    describe('createView()', () => {
      it('should create a new view', async () => {
        const definition = createTestViewDefinition('my-view')
        const metadata = await storage.createView(definition)

        expect(metadata.definition.name).toBe('my-view')
        expect(metadata.state).toBe('pending')
        expect(metadata.createdAt).toBeInstanceOf(Date)
        expect(metadata.version).toBe(1)
      })

      it('should add view to manifest', async () => {
        await storage.createView(createTestViewDefinition('my-view'))

        const manifest = await storage.loadManifest()
        expect(manifest.views).toHaveLength(1)
        expect(manifest.views[0].name).toBe('my-view')
        expect(manifest.views[0].source).toBe('users')
      })

      it('should create view directory structure', async () => {
        await storage.createView(createTestViewDefinition('my-view'))

        // Check directories exist
        const metadataExists = await backend.exists(MVStoragePaths.viewMetadata('my-view'))
        expect(metadataExists).toBe(true)
      })

      it('should throw MVAlreadyExistsError for duplicate view', async () => {
        await storage.createView(createTestViewDefinition('duplicate-view'))

        await expect(
          storage.createView(createTestViewDefinition('duplicate-view'))
        ).rejects.toThrow(MVAlreadyExistsError)
      })

      it('should create initial stats file', async () => {
        await storage.createView(createTestViewDefinition('stats-test'))

        const stats = await storage.getViewStats('stats-test')
        expect(stats.totalRefreshes).toBe(0)
        expect(stats.queryCount).toBe(0)
      })
    })

    describe('getViewMetadata()', () => {
      it('should return view metadata', async () => {
        await storage.createView(createTestViewDefinition('get-test'))

        const metadata = await storage.getViewMetadata('get-test')

        expect(metadata.definition.name).toBe('get-test')
        expect(metadata.state).toBe('pending')
      })

      it('should throw MVNotFoundError for non-existent view', async () => {
        await expect(
          storage.getViewMetadata('does-not-exist')
        ).rejects.toThrow(MVNotFoundError)
      })

      it('should parse dates correctly', async () => {
        await storage.createView(createTestViewDefinition('date-test'))

        const metadata = await storage.getViewMetadata('date-test')

        expect(metadata.createdAt).toBeInstanceOf(Date)
      })
    })

    describe('saveViewMetadata()', () => {
      it('should save updated metadata', async () => {
        await storage.createView(createTestViewDefinition('save-test'))
        const metadata = await storage.getViewMetadata('save-test')

        metadata.state = 'ready'
        metadata.documentCount = 100
        await storage.saveViewMetadata('save-test', metadata)

        const updated = await storage.getViewMetadata('save-test')
        expect(updated.state).toBe('ready')
        expect(updated.documentCount).toBe(100)
      })
    })

    describe('updateViewState()', () => {
      it('should update view state', async () => {
        await storage.createView(createTestViewDefinition('state-test'))

        await storage.updateViewState('state-test', 'building')

        const metadata = await storage.getViewMetadata('state-test')
        expect(metadata.state).toBe('building')
      })

      it('should update manifest entry', async () => {
        await storage.createView(createTestViewDefinition('manifest-state-test'))

        await storage.updateViewState('manifest-state-test', 'ready')

        const manifest = await storage.loadManifest()
        const entry = manifest.views.find((v) => v.name === 'manifest-state-test')
        expect(entry?.state).toBe('ready')
      })

      it('should set error message when state is error', async () => {
        await storage.createView(createTestViewDefinition('error-test'))

        await storage.updateViewState('error-test', 'error', 'Something went wrong')

        const metadata = await storage.getViewMetadata('error-test')
        expect(metadata.state).toBe('error')
        expect(metadata.error).toBe('Something went wrong')
      })

      it('should increment version', async () => {
        await storage.createView(createTestViewDefinition('version-test'))

        await storage.updateViewState('version-test', 'ready')

        const metadata = await storage.getViewMetadata('version-test')
        expect(metadata.version).toBe(2)
      })
    })

    describe('deleteView()', () => {
      it('should delete view and return true', async () => {
        await storage.createView(createTestViewDefinition('delete-test'))

        const result = await storage.deleteView('delete-test')

        expect(result).toBe(true)
      })

      it('should remove view from manifest', async () => {
        await storage.createView(createTestViewDefinition('delete-manifest-test'))
        await storage.deleteView('delete-manifest-test')

        const manifest = await storage.loadManifest()
        expect(manifest.views.find((v) => v.name === 'delete-manifest-test')).toBeUndefined()
      })

      it('should return false for non-existent view', async () => {
        const result = await storage.deleteView('does-not-exist')

        expect(result).toBe(false)
      })
    })

    describe('viewExists()', () => {
      it('should return true for existing view', async () => {
        await storage.createView(createTestViewDefinition('exists-test'))

        const exists = await storage.viewExists('exists-test')

        expect(exists).toBe(true)
      })

      it('should return false for non-existent view', async () => {
        const exists = await storage.viewExists('does-not-exist')

        expect(exists).toBe(false)
      })
    })

    describe('listViews()', () => {
      it('should return empty array when no views', async () => {
        const views = await storage.listViews()

        expect(views).toEqual([])
      })

      it('should return all views', async () => {
        await storage.createView(createTestViewDefinition('view-1'))
        await storage.createView(createTestViewDefinition('view-2', 'orders'))
        await storage.createView(createTestViewDefinition('view-3', 'products'))

        const views = await storage.listViews()

        expect(views).toHaveLength(3)
        expect(views.map((v) => v.name)).toContain('view-1')
        expect(views.map((v) => v.name)).toContain('view-2')
        expect(views.map((v) => v.name)).toContain('view-3')
      })

      it('should return a copy, not the original array', async () => {
        await storage.createView(createTestViewDefinition('copy-test'))

        const views1 = await storage.listViews()
        const views2 = await storage.listViews()

        expect(views1).not.toBe(views2)
      })
    })

    describe('getAllViewMetadata()', () => {
      it('should return full metadata for all views', async () => {
        await storage.createView(createTestViewDefinition('meta-1'))
        await storage.createView(createTestViewDefinition('meta-2'))

        const allMetadata = await storage.getAllViewMetadata()

        expect(allMetadata).toHaveLength(2)
        expect(allMetadata[0].definition).toBeDefined()
        expect(allMetadata[1].definition).toBeDefined()
      })
    })
  })

  // ===========================================================================
  // View Stats Operations
  // ===========================================================================

  describe('view stats operations', () => {
    beforeEach(async () => {
      await storage.createView(createTestViewDefinition('stats-view'))
    })

    describe('getViewStats()', () => {
      it('should return default stats for new view', async () => {
        const stats = await storage.getViewStats('stats-view')

        expect(stats.totalRefreshes).toBe(0)
        expect(stats.successfulRefreshes).toBe(0)
        expect(stats.failedRefreshes).toBe(0)
        expect(stats.avgRefreshDurationMs).toBe(0)
        expect(stats.queryCount).toBe(0)
        expect(stats.cacheHitRatio).toBe(0)
      })

      it('should return default stats for non-existent stats file', async () => {
        const stats = await storage.getViewStats('non-existent')

        expect(stats.totalRefreshes).toBe(0)
      })
    })

    describe('saveViewStats()', () => {
      it('should save custom stats', async () => {
        await storage.saveViewStats('stats-view', {
          totalRefreshes: 10,
          successfulRefreshes: 9,
          failedRefreshes: 1,
          avgRefreshDurationMs: 150,
          queryCount: 100,
          cacheHitRatio: 0.8,
        })

        const stats = await storage.getViewStats('stats-view')
        expect(stats.totalRefreshes).toBe(10)
        expect(stats.queryCount).toBe(100)
      })
    })

    describe('recordRefresh()', () => {
      it('should record successful refresh', async () => {
        await storage.recordRefresh('stats-view', true, 100)

        const stats = await storage.getViewStats('stats-view')
        expect(stats.totalRefreshes).toBe(1)
        expect(stats.successfulRefreshes).toBe(1)
        expect(stats.failedRefreshes).toBe(0)
        expect(stats.avgRefreshDurationMs).toBe(100)
      })

      it('should record failed refresh', async () => {
        await storage.recordRefresh('stats-view', false, 50)

        const stats = await storage.getViewStats('stats-view')
        expect(stats.totalRefreshes).toBe(1)
        expect(stats.successfulRefreshes).toBe(0)
        expect(stats.failedRefreshes).toBe(1)
      })

      it('should update metadata last refresh time on success', async () => {
        await storage.recordRefresh('stats-view', true, 100)

        const metadata = await storage.getViewMetadata('stats-view')
        expect(metadata.lastRefreshedAt).toBeInstanceOf(Date)
        expect(metadata.lastRefreshDurationMs).toBe(100)
      })

      it('should update manifest on success', async () => {
        await storage.recordRefresh('stats-view', true, 100)

        const manifest = await storage.loadManifest()
        const entry = manifest.views.find((v) => v.name === 'stats-view')
        expect(entry?.lastRefreshedAt).toBeDefined()
      })

      it('should calculate running average duration', async () => {
        await storage.recordRefresh('stats-view', true, 100)
        await storage.recordRefresh('stats-view', true, 200)

        const stats = await storage.getViewStats('stats-view')
        expect(stats.avgRefreshDurationMs).toBe(150)
      })
    })

    describe('recordQuery()', () => {
      it('should increment query count', async () => {
        await storage.recordQuery('stats-view', true)
        await storage.recordQuery('stats-view', false)

        const stats = await storage.getViewStats('stats-view')
        expect(stats.queryCount).toBe(2)
      })

      it('should update cache hit ratio', async () => {
        // Record 10 cache hits
        for (let i = 0; i < 10; i++) {
          await storage.recordQuery('stats-view', true)
        }

        const stats = await storage.getViewStats('stats-view')
        // Should be close to 1.0 with exponential moving average
        expect(stats.cacheHitRatio).toBeGreaterThan(0.5)
      })
    })
  })

  // ===========================================================================
  // View Data Operations
  // ===========================================================================

  describe('view data operations', () => {
    beforeEach(async () => {
      await storage.createView(createTestViewDefinition('data-view'))
    })

    describe('getDataFilePath()', () => {
      it('should return correct path', () => {
        const path = storage.getDataFilePath('my-view')
        expect(path).toBe('_views/my-view/data/data.parquet')
      })
    })

    describe('getDataShardPath()', () => {
      it('should return correct path with padded shard number', () => {
        expect(storage.getDataShardPath('my-view', 0)).toBe('_views/my-view/data/data.0000.parquet')
        expect(storage.getDataShardPath('my-view', 5)).toBe('_views/my-view/data/data.0005.parquet')
        expect(storage.getDataShardPath('my-view', 123)).toBe('_views/my-view/data/data.0123.parquet')
      })
    })

    describe('writeViewData()', () => {
      it('should write data file', async () => {
        const data = new Uint8Array([0x50, 0x41, 0x52, 0x31]) // PAR1 magic bytes

        await storage.writeViewData('data-view', data)

        const exists = await backend.exists(MVStoragePaths.viewDataFile('data-view'))
        expect(exists).toBe(true)
      })

      it('should return write result with etag', async () => {
        const data = new Uint8Array([1, 2, 3, 4])

        const result = await storage.writeViewData('data-view', data)

        expect(result.etag).toBeDefined()
        expect(result.size).toBe(4)
      })
    })

    describe('writeViewDataShard()', () => {
      it('should write shard file', async () => {
        const data = new Uint8Array([1, 2, 3, 4])

        await storage.writeViewDataShard('data-view', 0, data)

        const exists = await backend.exists(MVStoragePaths.viewDataShard('data-view', 0))
        expect(exists).toBe(true)
      })
    })

    describe('readViewData()', () => {
      it('should read data file', async () => {
        const data = new Uint8Array([1, 2, 3, 4, 5])
        await storage.writeViewData('data-view', data)

        const result = await storage.readViewData('data-view')

        expect(result).toEqual(data)
      })
    })

    describe('readViewDataRange()', () => {
      it('should read partial data', async () => {
        const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        await storage.writeViewData('data-view', data)

        const result = await storage.readViewDataRange('data-view', 2, 6)

        expect(result).toEqual(new Uint8Array([2, 3, 4, 5]))
      })
    })

    describe('viewDataExists()', () => {
      it('should return true when data exists', async () => {
        await storage.writeViewData('data-view', new Uint8Array([1, 2, 3]))

        const exists = await storage.viewDataExists('data-view')

        expect(exists).toBe(true)
      })

      it('should return false when data does not exist', async () => {
        const exists = await storage.viewDataExists('data-view')

        expect(exists).toBe(false)
      })
    })

    describe('getViewDataStat()', () => {
      it('should return file stats', async () => {
        const data = new Uint8Array([1, 2, 3, 4, 5])
        await storage.writeViewData('data-view', data)

        const stat = await storage.getViewDataStat('data-view')

        expect(stat?.size).toBe(5)
        expect(stat?.mtime).toBeInstanceOf(Date)
      })

      it('should return null for non-existent data', async () => {
        const stat = await storage.getViewDataStat('data-view')

        expect(stat).toBeNull()
      })
    })

    describe('listViewDataFiles()', () => {
      it('should list all data files', async () => {
        await storage.writeViewData('data-view', new Uint8Array([1]))
        await storage.writeViewDataShard('data-view', 1, new Uint8Array([2]))
        await storage.writeViewDataShard('data-view', 2, new Uint8Array([3]))

        const files = await storage.listViewDataFiles('data-view')

        expect(files).toHaveLength(3)
      })
    })

    describe('deleteViewData()', () => {
      it('should delete all data files', async () => {
        await storage.writeViewData('data-view', new Uint8Array([1]))
        await storage.writeViewDataShard('data-view', 1, new Uint8Array([2]))

        const count = await storage.deleteViewData('data-view')

        expect(count).toBe(2)

        const files = await storage.listViewDataFiles('data-view')
        expect(files).toHaveLength(0)
      })
    })
  })

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  describe('utility methods', () => {
    describe('getBackend()', () => {
      it('should return the storage backend', () => {
        const result = storage.getBackend()

        expect(result).toBe(backend)
      })
    })

    describe('ensureRootDir()', () => {
      it('should create the _views directory', async () => {
        await storage.ensureRootDir()

        const stat = await backend.stat(MVStoragePaths.root)
        expect(stat?.isDirectory).toBe(true)
      })
    })

    describe('getViewsNeedingRefresh()', () => {
      it('should return empty array when no views', async () => {
        const views = await storage.getViewsNeedingRefresh()

        expect(views).toEqual([])
      })

      it('should not include manual views', async () => {
        await storage.createView(createTestViewDefinition('manual-view'))

        const views = await storage.getViewsNeedingRefresh()

        expect(views).toHaveLength(0)
      })

      it('should include stale views', async () => {
        await storage.createView(createScheduledViewDefinition('scheduled-view'))
        await storage.updateViewState('scheduled-view', 'stale')

        const views = await storage.getViewsNeedingRefresh()

        expect(views).toHaveLength(1)
        expect(views[0].definition.name).toBe('scheduled-view')
      })

      it('should not include disabled views', async () => {
        await storage.createView(createStreamingViewDefinition('disabled-view'))
        await storage.updateViewState('disabled-view', 'disabled')

        const views = await storage.getViewsNeedingRefresh()

        expect(views).toHaveLength(0)
      })

      it('should not include building views', async () => {
        await storage.createView(createStreamingViewDefinition('building-view'))
        await storage.updateViewState('building-view', 'building')

        const views = await storage.getViewsNeedingRefresh()

        expect(views).toHaveLength(0)
      })

      it('should include scheduled views past their refresh time', async () => {
        await storage.createView(createScheduledViewDefinition('past-due-view'))
        const metadata = await storage.getViewMetadata('past-due-view')
        metadata.nextRefreshAt = new Date(Date.now() - 60000) // 1 minute ago
        await storage.saveViewMetadata('past-due-view', metadata)

        const views = await storage.getViewsNeedingRefresh()

        expect(views.some((v) => v.definition.name === 'past-due-view')).toBe(true)
      })
    })

    describe('getViewsBySource()', () => {
      it('should return views for a specific source', async () => {
        await storage.createView(createTestViewDefinition('users-view-1', 'users'))
        await storage.createView(createTestViewDefinition('users-view-2', 'users'))
        await storage.createView(createTestViewDefinition('orders-view', 'orders'))

        const views = await storage.getViewsBySource('users')

        expect(views).toHaveLength(2)
        expect(views.every((v) => v.definition.source === 'users')).toBe(true)
      })

      it('should return empty array for unknown source', async () => {
        await storage.createView(createTestViewDefinition('test-view'))

        const views = await storage.getViewsBySource('unknown')

        expect(views).toEqual([])
      })
    })

    describe('getStreamingViewsForSource()', () => {
      it('should return only streaming views for source', async () => {
        await storage.createView(createTestViewDefinition('manual-view', 'events'))
        await storage.createView(createStreamingViewDefinition('streaming-view', 'events'))
        await storage.createView(createScheduledViewDefinition('scheduled-view', 'events'))

        const views = await storage.getStreamingViewsForSource('events')

        expect(views).toHaveLength(1)
        expect(views[0].definition.name).toBe('streaming-view')
      })

      it('should not include disabled streaming views', async () => {
        await storage.createView(createStreamingViewDefinition('disabled-streaming', 'events'))
        await storage.updateViewState('disabled-streaming', 'disabled')

        const views = await storage.getStreamingViewsForSource('events')

        expect(views).toHaveLength(0)
      })
    })
  })

  // ===========================================================================
  // Error Handling
  // ===========================================================================

  describe('error handling', () => {
    it('MVNotFoundError should have correct properties', () => {
      const error = new MVNotFoundError('test-view')

      expect(error.name).toBe('MVNotFoundError')
      expect(error.code).toBe('MV_NOT_FOUND')
      expect(error.viewName).toBe('test-view')
      expect(error.message).toContain('test-view')
    })

    it('MVAlreadyExistsError should have correct properties', () => {
      const error = new MVAlreadyExistsError('test-view')

      expect(error.name).toBe('MVAlreadyExistsError')
      expect(error.code).toBe('MV_ALREADY_EXISTS')
      expect(error.viewName).toBe('test-view')
      expect(error.message).toContain('test-view')
    })
  })

  // ===========================================================================
  // Integration Tests
  // ===========================================================================

  describe('integration', () => {
    it('should handle full view lifecycle', async () => {
      // Create view
      const definition = createTestViewDefinition('lifecycle-view')
      await storage.createView(definition)

      // Verify created
      expect(await storage.viewExists('lifecycle-view')).toBe(true)

      // Update state to building
      await storage.updateViewState('lifecycle-view', 'building')

      // Write data
      const data = new Uint8Array([1, 2, 3, 4, 5])
      await storage.writeViewData('lifecycle-view', data)

      // Record refresh
      await storage.recordRefresh('lifecycle-view', true, 150)

      // Update state to ready
      await storage.updateViewState('lifecycle-view', 'ready')

      // Record queries
      await storage.recordQuery('lifecycle-view', true)
      await storage.recordQuery('lifecycle-view', true)

      // Verify final state
      const metadata = await storage.getViewMetadata('lifecycle-view')
      expect(metadata.state).toBe('ready')
      expect(metadata.lastRefreshedAt).toBeInstanceOf(Date)

      const stats = await storage.getViewStats('lifecycle-view')
      expect(stats.totalRefreshes).toBe(1)
      expect(stats.queryCount).toBe(2)

      // Delete view
      const deleted = await storage.deleteView('lifecycle-view')
      expect(deleted).toBe(true)
      expect(await storage.viewExists('lifecycle-view')).toBe(false)
    })

    it('should maintain consistency across multiple storage managers', async () => {
      // Create view with first manager
      await storage.createView(createTestViewDefinition('consistency-view'))

      // Access with second manager
      const storage2 = new MVStorageManager(backend)
      const metadata = await storage2.getViewMetadata('consistency-view')
      expect(metadata.definition.name).toBe('consistency-view')

      // Update with second manager
      await storage2.updateViewState('consistency-view', 'ready')

      // Verify with first manager (invalidate cache first)
      storage.invalidateManifestCache()
      const manifest = await storage.loadManifest()
      const entry = manifest.views.find((v) => v.name === 'consistency-view')
      expect(entry?.state).toBe('ready')
    })
  })
})
