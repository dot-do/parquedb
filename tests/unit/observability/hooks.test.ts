/**
 * Tests for Observability Hooks
 *
 * Tests the hook registry, metrics collector, and utility functions.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  HookRegistry,
  MetricsCollector,
  generateOperationId,
  createQueryContext,
  createMutationContext,
  createStorageContext,
  globalHookRegistry,
  type QueryHook,
  type MutationHook,
  type StorageHook,
  type ObservabilityHook,
  type QueryContext,
  type MutationContext,
  type StorageContext,
  type QueryResult,
  type MutationResult,
  type StorageResult,
} from '../../../src/observability'

// =============================================================================
// HookRegistry Tests
// =============================================================================

describe('HookRegistry', () => {
  let registry: HookRegistry

  beforeEach(() => {
    registry = new HookRegistry()
  })

  describe('query hooks', () => {
    it('should register query hooks', () => {
      const hook: QueryHook = {
        onQueryStart: vi.fn(),
        onQueryEnd: vi.fn(),
        onQueryError: vi.fn(),
      }

      registry.registerQueryHook(hook)
      expect(registry.queryHookCount).toBe(1)
    })

    it('should unregister query hooks', () => {
      const hook: QueryHook = {
        onQueryStart: vi.fn(),
      }

      const unregister = registry.registerQueryHook(hook)
      expect(registry.queryHookCount).toBe(1)

      unregister()
      expect(registry.queryHookCount).toBe(0)
    })

    it('should dispatch onQueryStart to all hooks', async () => {
      const hook1 = { onQueryStart: vi.fn() }
      const hook2 = { onQueryStart: vi.fn() }

      registry.registerQueryHook(hook1)
      registry.registerQueryHook(hook2)

      const context = createQueryContext('find', 'posts', { status: 'published' })
      await registry.dispatchQueryStart(context)

      expect(hook1.onQueryStart).toHaveBeenCalledWith(context)
      expect(hook2.onQueryStart).toHaveBeenCalledWith(context)
    })

    it('should dispatch onQueryEnd to all hooks', async () => {
      const hook = { onQueryEnd: vi.fn() }
      registry.registerQueryHook(hook)

      const context = createQueryContext('find', 'posts')
      const result: QueryResult = {
        rowCount: 10,
        durationMs: 50,
        indexUsed: 'status_idx',
      }

      await registry.dispatchQueryEnd(context, result)
      expect(hook.onQueryEnd).toHaveBeenCalledWith(context, result)
    })

    it('should dispatch onQueryError to all hooks', async () => {
      const hook = { onQueryError: vi.fn() }
      registry.registerQueryHook(hook)

      const context = createQueryContext('find', 'posts')
      const error = new Error('Query failed')

      await registry.dispatchQueryError(context, error)
      expect(hook.onQueryError).toHaveBeenCalledWith(context, error)
    })

    it('should handle async hooks', async () => {
      const hook: QueryHook = {
        onQueryStart: vi.fn().mockResolvedValue(undefined),
        onQueryEnd: vi.fn().mockResolvedValue(undefined),
      }

      registry.registerQueryHook(hook)

      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)

      expect(hook.onQueryStart).toHaveBeenCalled()
    })

    it('should skip hooks that do not implement the method', async () => {
      const hook1: QueryHook = { onQueryStart: vi.fn() }
      const hook2: QueryHook = { onQueryEnd: vi.fn() }

      registry.registerQueryHook(hook1)
      registry.registerQueryHook(hook2)

      const context = createQueryContext('find', 'posts')
      await registry.dispatchQueryStart(context)

      expect(hook1.onQueryStart).toHaveBeenCalled()
      // hook2.onQueryEnd should not be called during dispatchQueryStart
    })
  })

  describe('mutation hooks', () => {
    it('should register mutation hooks', () => {
      const hook: MutationHook = {
        onMutationStart: vi.fn(),
        onMutationEnd: vi.fn(),
        onMutationError: vi.fn(),
      }

      registry.registerMutationHook(hook)
      expect(registry.mutationHookCount).toBe(1)
    })

    it('should unregister mutation hooks', () => {
      const hook: MutationHook = {
        onMutationStart: vi.fn(),
      }

      const unregister = registry.registerMutationHook(hook)
      expect(registry.mutationHookCount).toBe(1)

      unregister()
      expect(registry.mutationHookCount).toBe(0)
    })

    it('should dispatch onMutationStart to all hooks', async () => {
      const hook = { onMutationStart: vi.fn() }
      registry.registerMutationHook(hook)

      const context = createMutationContext('create', 'posts', undefined, {
        $type: 'Post',
        name: 'Test',
        title: 'Test Post',
      })

      await registry.dispatchMutationStart(context)
      expect(hook.onMutationStart).toHaveBeenCalledWith(context)
    })

    it('should dispatch onMutationEnd to all hooks', async () => {
      const hook = { onMutationEnd: vi.fn() }
      registry.registerMutationHook(hook)

      const context = createMutationContext('create', 'posts')
      const result: MutationResult = {
        affectedCount: 1,
        generatedIds: ['posts/abc123'],
        durationMs: 25,
        newVersion: 1,
      }

      await registry.dispatchMutationEnd(context, result)
      expect(hook.onMutationEnd).toHaveBeenCalledWith(context, result)
    })

    it('should dispatch onMutationError to all hooks', async () => {
      const hook = { onMutationError: vi.fn() }
      registry.registerMutationHook(hook)

      const context = createMutationContext('update', 'posts', 'posts/123')
      const error = new Error('Entity not found')

      await registry.dispatchMutationError(context, error)
      expect(hook.onMutationError).toHaveBeenCalledWith(context, error)
    })
  })

  describe('storage hooks', () => {
    it('should register storage hooks', () => {
      const hook: StorageHook = {
        onRead: vi.fn(),
        onWrite: vi.fn(),
        onDelete: vi.fn(),
        onStorageError: vi.fn(),
      }

      registry.registerStorageHook(hook)
      expect(registry.storageHookCount).toBe(1)
    })

    it('should unregister storage hooks', () => {
      const hook: StorageHook = {
        onRead: vi.fn(),
      }

      const unregister = registry.registerStorageHook(hook)
      expect(registry.storageHookCount).toBe(1)

      unregister()
      expect(registry.storageHookCount).toBe(0)
    })

    it('should dispatch onRead to all hooks', async () => {
      const hook = { onRead: vi.fn() }
      registry.registerStorageHook(hook)

      const context = createStorageContext('read', 'data/posts/data.parquet')
      const result: StorageResult = {
        bytesTransferred: 1024,
        durationMs: 15,
        etag: 'abc123',
      }

      await registry.dispatchStorageRead(context, result)
      expect(hook.onRead).toHaveBeenCalledWith(context, result)
    })

    it('should dispatch onWrite to all hooks', async () => {
      const hook = { onWrite: vi.fn() }
      registry.registerStorageHook(hook)

      const context = createStorageContext('write', 'data/posts/data.parquet')
      const result: StorageResult = {
        bytesTransferred: 2048,
        durationMs: 30,
        etag: 'def456',
      }

      await registry.dispatchStorageWrite(context, result)
      expect(hook.onWrite).toHaveBeenCalledWith(context, result)
    })

    it('should dispatch onDelete to all hooks', async () => {
      const hook = { onDelete: vi.fn() }
      registry.registerStorageHook(hook)

      const context = createStorageContext('delete', 'data/posts/data.parquet')
      const result: StorageResult = {
        bytesTransferred: 0,
        durationMs: 10,
        fileCount: 1,
      }

      await registry.dispatchStorageDelete(context, result)
      expect(hook.onDelete).toHaveBeenCalledWith(context, result)
    })

    it('should dispatch onStorageError to all hooks', async () => {
      const hook = { onStorageError: vi.fn() }
      registry.registerStorageHook(hook)

      const context = createStorageContext('read', 'data/posts/data.parquet')
      const error = new Error('File not found')

      await registry.dispatchStorageError(context, error)
      expect(hook.onStorageError).toHaveBeenCalledWith(context, error)
    })
  })

  describe('combined hooks', () => {
    it('should register combined observability hook', () => {
      const hook: ObservabilityHook = {
        onQueryStart: vi.fn(),
        onQueryEnd: vi.fn(),
        onMutationStart: vi.fn(),
        onMutationEnd: vi.fn(),
        onRead: vi.fn(),
        onWrite: vi.fn(),
      }

      registry.registerHook(hook)

      expect(registry.queryHookCount).toBe(1)
      expect(registry.mutationHookCount).toBe(1)
      expect(registry.storageHookCount).toBe(1)
    })

    it('should unregister all handlers from combined hook', () => {
      const hook: ObservabilityHook = {
        onQueryStart: vi.fn(),
        onMutationStart: vi.fn(),
        onRead: vi.fn(),
      }

      const unregister = registry.registerHook(hook)

      expect(registry.queryHookCount).toBe(1)
      expect(registry.mutationHookCount).toBe(1)
      expect(registry.storageHookCount).toBe(1)

      unregister()

      expect(registry.queryHookCount).toBe(0)
      expect(registry.mutationHookCount).toBe(0)
      expect(registry.storageHookCount).toBe(0)
    })

    it('should only register relevant handlers', () => {
      const queryOnlyHook: ObservabilityHook = {
        onQueryStart: vi.fn(),
        onQueryEnd: vi.fn(),
      }

      registry.registerHook(queryOnlyHook)

      expect(registry.queryHookCount).toBe(1)
      expect(registry.mutationHookCount).toBe(0)
      expect(registry.storageHookCount).toBe(0)
    })
  })

  describe('clearHooks', () => {
    it('should clear all hooks', () => {
      registry.registerQueryHook({ onQueryStart: vi.fn() })
      registry.registerMutationHook({ onMutationStart: vi.fn() })
      registry.registerStorageHook({ onRead: vi.fn() })

      expect(registry.queryHookCount).toBe(1)
      expect(registry.mutationHookCount).toBe(1)
      expect(registry.storageHookCount).toBe(1)

      registry.clearHooks()

      expect(registry.queryHookCount).toBe(0)
      expect(registry.mutationHookCount).toBe(0)
      expect(registry.storageHookCount).toBe(0)
    })
  })

  describe('hasHooks', () => {
    it('should return false when no hooks registered', () => {
      expect(registry.hasHooks).toBe(false)
    })

    it('should return true when any hook is registered', () => {
      registry.registerQueryHook({ onQueryStart: vi.fn() })
      expect(registry.hasHooks).toBe(true)
    })
  })
})

// =============================================================================
// MetricsCollector Tests
// =============================================================================

describe('MetricsCollector', () => {
  let collector: MetricsCollector

  beforeEach(() => {
    collector = new MetricsCollector()
  })

  describe('query metrics', () => {
    it('should collect query metrics on success', () => {
      const context = createQueryContext('find', 'posts')
      const result: QueryResult = {
        rowCount: 10,
        durationMs: 50,
      }

      collector.onQueryEnd(context, result)

      const metrics = collector.getMetrics()
      expect(metrics.queries.find.count).toBe(1)
      expect(metrics.queries.find.totalDurationMs).toBe(50)
      expect(metrics.queries.find.rowsProcessed).toBe(10)
    })

    it('should collect query metrics on error', () => {
      const context = createQueryContext('find', 'posts')
      const error = new Error('Query failed')

      collector.onQueryError(context, error)

      const metrics = collector.getMetrics()
      expect(metrics.queries.find.errorCount).toBe(1)
      expect(metrics.system.totalErrors).toBe(1)
    })

    it('should track min/max duration', () => {
      const context1 = createQueryContext('find', 'posts')
      const context2 = createQueryContext('find', 'posts')

      collector.onQueryEnd(context1, { rowCount: 5, durationMs: 20 })
      collector.onQueryEnd(context2, { rowCount: 10, durationMs: 100 })

      const metrics = collector.getMetrics()
      expect(metrics.queries.find.minDurationMs).toBe(20)
      expect(metrics.queries.find.maxDurationMs).toBe(100)
    })

    it('should track multiple operation types', () => {
      collector.onQueryEnd(
        createQueryContext('find', 'posts'),
        { rowCount: 10, durationMs: 50 }
      )
      collector.onQueryEnd(
        createQueryContext('findOne', 'posts'),
        { rowCount: 1, durationMs: 20 }
      )
      collector.onQueryEnd(
        createQueryContext('count', 'posts'),
        { rowCount: 100, durationMs: 10 }
      )

      const metrics = collector.getMetrics()
      expect(metrics.queries.find.count).toBe(1)
      expect(metrics.queries.findOne.count).toBe(1)
      expect(metrics.queries.count.count).toBe(1)
      expect(metrics.system.totalOperations).toBe(3)
    })
  })

  describe('mutation metrics', () => {
    it('should collect mutation metrics on success', () => {
      const context = createMutationContext('create', 'posts')
      const result: MutationResult = {
        affectedCount: 1,
        generatedIds: ['posts/abc123'],
        durationMs: 25,
      }

      collector.onMutationEnd(context, result)

      const metrics = collector.getMetrics()
      expect(metrics.mutations.create.count).toBe(1)
      expect(metrics.mutations.create.totalDurationMs).toBe(25)
      expect(metrics.mutations.create.rowsProcessed).toBe(1)
    })

    it('should collect mutation metrics on error', () => {
      const context = createMutationContext('update', 'posts', 'posts/123')
      const error = new Error('Update failed')

      collector.onMutationError(context, error)

      const metrics = collector.getMetrics()
      expect(metrics.mutations.update.errorCount).toBe(1)
    })

    it('should track all mutation types', () => {
      collector.onMutationEnd(
        createMutationContext('create', 'posts'),
        { affectedCount: 1, durationMs: 20 }
      )
      collector.onMutationEnd(
        createMutationContext('update', 'posts', 'posts/1'),
        { affectedCount: 1, durationMs: 15 }
      )
      collector.onMutationEnd(
        createMutationContext('delete', 'posts', 'posts/2'),
        { affectedCount: 1, durationMs: 10 }
      )

      const metrics = collector.getMetrics()
      expect(metrics.mutations.create.count).toBe(1)
      expect(metrics.mutations.update.count).toBe(1)
      expect(metrics.mutations.delete.count).toBe(1)
    })
  })

  describe('storage metrics', () => {
    it('should collect read metrics', () => {
      const context = createStorageContext('read', 'data/posts/data.parquet')
      const result: StorageResult = {
        bytesTransferred: 1024,
        durationMs: 15,
      }

      collector.onRead(context, result)

      const metrics = collector.getMetrics()
      expect(metrics.storage.read.count).toBe(1)
      expect(metrics.storage.read.bytesTransferred).toBe(1024)
    })

    it('should collect write metrics', () => {
      const context = createStorageContext('write', 'data/posts/data.parquet')
      const result: StorageResult = {
        bytesTransferred: 2048,
        durationMs: 30,
      }

      collector.onWrite(context, result)

      const metrics = collector.getMetrics()
      expect(metrics.storage.write.count).toBe(1)
      expect(metrics.storage.write.bytesTransferred).toBe(2048)
    })

    it('should collect delete metrics', () => {
      const context = createStorageContext('delete', 'data/posts/data.parquet')
      const result: StorageResult = {
        bytesTransferred: 0,
        durationMs: 10,
      }

      collector.onDelete(context, result)

      const metrics = collector.getMetrics()
      expect(metrics.storage.delete.count).toBe(1)
    })

    it('should collect storage error metrics', () => {
      const context = createStorageContext('read', 'data/posts/data.parquet')
      const error = new Error('File not found')

      collector.onStorageError(context, error)

      const metrics = collector.getMetrics()
      expect(metrics.storage.read.errorCount).toBe(1)
      expect(metrics.system.totalErrors).toBe(1)
    })

    it('should accumulate bytes transferred', () => {
      collector.onRead(
        createStorageContext('read', 'file1.parquet'),
        { bytesTransferred: 1000, durationMs: 10 }
      )
      collector.onRead(
        createStorageContext('read', 'file2.parquet'),
        { bytesTransferred: 2000, durationMs: 15 }
      )

      const metrics = collector.getMetrics()
      expect(metrics.storage.read.bytesTransferred).toBe(3000)
    })
  })

  describe('system metrics', () => {
    it('should track total operations', () => {
      collector.onQueryEnd(
        createQueryContext('find', 'posts'),
        { rowCount: 10, durationMs: 50 }
      )
      collector.onMutationEnd(
        createMutationContext('create', 'posts'),
        { affectedCount: 1, durationMs: 25 }
      )
      collector.onRead(
        createStorageContext('read', 'file.parquet'),
        { bytesTransferred: 1000, durationMs: 10 }
      )

      const metrics = collector.getMetrics()
      expect(metrics.system.totalOperations).toBe(3)
    })

    it('should track total errors', () => {
      collector.onQueryError(
        createQueryContext('find', 'posts'),
        new Error('Query error')
      )
      collector.onMutationError(
        createMutationContext('create', 'posts'),
        new Error('Mutation error')
      )
      collector.onStorageError(
        createStorageContext('read', 'file.parquet'),
        new Error('Storage error')
      )

      const metrics = collector.getMetrics()
      expect(metrics.system.totalErrors).toBe(3)
    })

    it('should track uptime', async () => {
      await new Promise(resolve => setTimeout(resolve, 10))
      const metrics = collector.getMetrics()
      expect(metrics.system.uptime).toBeGreaterThanOrEqual(10)
    })
  })

  describe('getAverageLatency', () => {
    it('should return average latency for operation type', () => {
      collector.onQueryEnd(
        createQueryContext('find', 'posts'),
        { rowCount: 10, durationMs: 50 }
      )
      collector.onQueryEnd(
        createQueryContext('find', 'posts'),
        { rowCount: 5, durationMs: 100 }
      )

      const avgLatency = collector.getAverageLatency('queries', 'find')
      expect(avgLatency).toBe(75)
    })

    it('should return 0 for no operations', () => {
      const avgLatency = collector.getAverageLatency('queries', 'find')
      expect(avgLatency).toBe(0)
    })
  })

  describe('getErrorRate', () => {
    it('should return error rate for operation type', () => {
      collector.onQueryEnd(
        createQueryContext('find', 'posts'),
        { rowCount: 10, durationMs: 50 }
      )
      collector.onQueryEnd(
        createQueryContext('find', 'posts'),
        { rowCount: 5, durationMs: 30 }
      )
      collector.onQueryEnd(
        createQueryContext('find', 'posts'),
        { rowCount: 3, durationMs: 20 }
      )
      collector.onQueryError(
        createQueryContext('find', 'posts'),
        new Error('Failed')
      )

      // 1 error out of 4 total operations = 0.25
      const errorRate = collector.getErrorRate('queries', 'find')
      expect(errorRate).toBeCloseTo(0.25, 2)
    })

    it('should return 0 for no operations', () => {
      const errorRate = collector.getErrorRate('queries', 'find')
      expect(errorRate).toBe(0)
    })
  })

  describe('reset', () => {
    it('should reset all metrics', () => {
      collector.onQueryEnd(
        createQueryContext('find', 'posts'),
        { rowCount: 10, durationMs: 50 }
      )
      collector.onMutationEnd(
        createMutationContext('create', 'posts'),
        { affectedCount: 1, durationMs: 25 }
      )

      collector.reset()

      const metrics = collector.getMetrics()
      expect(metrics.queries.find.count).toBe(0)
      expect(metrics.mutations.create.count).toBe(0)
      expect(metrics.system.totalOperations).toBe(0)
    })
  })
})

// =============================================================================
// Utility Function Tests
// =============================================================================

describe('Utility Functions', () => {
  describe('generateOperationId', () => {
    it('should generate unique IDs', () => {
      const ids = new Set<string>()
      for (let i = 0; i < 100; i++) {
        ids.add(generateOperationId())
      }
      expect(ids.size).toBe(100)
    })

    it('should generate string IDs', () => {
      const id = generateOperationId()
      expect(typeof id).toBe('string')
      expect(id.length).toBeGreaterThan(0)
    })
  })

  describe('createQueryContext', () => {
    it('should create valid query context', () => {
      const context = createQueryContext('find', 'posts', { status: 'published' }, { limit: 10 })

      expect(context.operationId).toBeDefined()
      expect(context.startTime).toBeDefined()
      expect(context.operationType).toBe('find')
      expect(context.namespace).toBe('posts')
      expect(context.filter).toEqual({ status: 'published' })
      expect(context.options).toEqual({ limit: 10 })
    })

    it('should handle minimal parameters', () => {
      const context = createQueryContext('count')

      expect(context.operationId).toBeDefined()
      expect(context.operationType).toBe('count')
      expect(context.namespace).toBeUndefined()
      expect(context.filter).toBeUndefined()
    })
  })

  describe('createMutationContext', () => {
    it('should create valid mutation context', () => {
      const data = { $type: 'Post', name: 'Test', title: 'Test Post' }
      const context = createMutationContext('create', 'posts', undefined, data)

      expect(context.operationId).toBeDefined()
      expect(context.startTime).toBeDefined()
      expect(context.operationType).toBe('create')
      expect(context.namespace).toBe('posts')
      expect(context.data).toEqual(data)
    })

    it('should handle entity ID for updates', () => {
      const context = createMutationContext('update', 'posts', 'posts/123', { $set: { title: 'Updated' } })

      expect(context.entityId).toBe('posts/123')
      expect(context.operationType).toBe('update')
    })

    it('should handle multiple entity IDs', () => {
      const context = createMutationContext('deleteMany', 'posts', ['posts/1', 'posts/2', 'posts/3'])

      expect(context.entityId).toEqual(['posts/1', 'posts/2', 'posts/3'])
    })
  })

  describe('createStorageContext', () => {
    it('should create valid storage context', () => {
      const context = createStorageContext('read', 'data/posts/data.parquet')

      expect(context.operationId).toBeDefined()
      expect(context.startTime).toBeDefined()
      expect(context.operationType).toBe('read')
      expect(context.path).toBe('data/posts/data.parquet')
    })

    it('should handle range reads', () => {
      const context = createStorageContext('readRange', 'data/posts/data.parquet', { start: 0, end: 1024 })

      expect(context.operationType).toBe('readRange')
      expect(context.range).toEqual({ start: 0, end: 1024 })
    })
  })
})

// =============================================================================
// Global Registry Tests
// =============================================================================

describe('globalHookRegistry', () => {
  beforeEach(() => {
    globalHookRegistry.clearHooks()
  })

  it('should be a HookRegistry instance', () => {
    expect(globalHookRegistry).toBeInstanceOf(HookRegistry)
  })

  it('should persist hooks across imports', () => {
    const hook = { onQueryStart: vi.fn() }
    globalHookRegistry.registerQueryHook(hook)

    expect(globalHookRegistry.queryHookCount).toBe(1)
  })
})

// =============================================================================
// Integration Tests
// =============================================================================

describe('Integration', () => {
  it('should work with MetricsCollector and HookRegistry together', async () => {
    const registry = new HookRegistry()
    const collector = new MetricsCollector()

    registry.registerHook(collector)

    // Simulate a complete query lifecycle
    const queryContext = createQueryContext('find', 'posts', { status: 'published' })
    await registry.dispatchQueryStart(queryContext)
    await registry.dispatchQueryEnd(queryContext, { rowCount: 10, durationMs: 50 })

    // Simulate a mutation
    const mutationContext = createMutationContext('create', 'posts')
    await registry.dispatchMutationStart(mutationContext)
    await registry.dispatchMutationEnd(mutationContext, { affectedCount: 1, durationMs: 25 })

    // Simulate storage operations
    const storageContext = createStorageContext('read', 'data/posts/data.parquet')
    await registry.dispatchStorageRead(storageContext, { bytesTransferred: 1024, durationMs: 15 })

    const metrics = collector.getMetrics()
    expect(metrics.queries.find.count).toBe(1)
    expect(metrics.mutations.create.count).toBe(1)
    expect(metrics.storage.read.count).toBe(1)
    expect(metrics.system.totalOperations).toBe(3)
  })

  it('should handle multiple hooks for the same events', async () => {
    const registry = new HookRegistry()
    const collector1 = new MetricsCollector()
    const collector2 = new MetricsCollector()
    const customHook = {
      onQueryEnd: vi.fn(),
    }

    registry.registerHook(collector1)
    registry.registerHook(collector2)
    registry.registerQueryHook(customHook)

    const context = createQueryContext('find', 'posts')
    await registry.dispatchQueryEnd(context, { rowCount: 5, durationMs: 30 })

    expect(collector1.getMetrics().queries.find.count).toBe(1)
    expect(collector2.getMetrics().queries.find.count).toBe(1)
    expect(customHook.onQueryEnd).toHaveBeenCalled()
  })
})

// =============================================================================
// Observed Storage Backend Tests
// =============================================================================

describe('ObservedBackend', () => {
  let backend: import('../../../src/storage').MemoryBackend
  let observedBackend: import('../../../src/storage').ObservedBackend

  beforeEach(async () => {
    globalHookRegistry.clearHooks()
    const { MemoryBackend, ObservedBackend } = await import('../../../src/storage')
    backend = new MemoryBackend()
    observedBackend = new ObservedBackend(backend)
  })

  it('should dispatch read hook on read', async () => {
    const hook = { onRead: vi.fn() }
    globalHookRegistry.registerStorageHook(hook)

    // Write some data first
    await backend.write('test.txt', new Uint8Array([1, 2, 3]))

    // Read through observed backend
    await observedBackend.read('test.txt')

    expect(hook.onRead).toHaveBeenCalledTimes(1)
    expect(hook.onRead).toHaveBeenCalledWith(
      expect.objectContaining({
        operationType: 'read',
        path: 'test.txt',
      }),
      expect.objectContaining({
        bytesTransferred: 3,
        durationMs: expect.any(Number),
      })
    )
  })

  it('should dispatch write hook on write', async () => {
    const hook = { onWrite: vi.fn() }
    globalHookRegistry.registerStorageHook(hook)

    await observedBackend.write('test.txt', new Uint8Array([1, 2, 3, 4, 5]))

    expect(hook.onWrite).toHaveBeenCalledTimes(1)
    expect(hook.onWrite).toHaveBeenCalledWith(
      expect.objectContaining({
        operationType: 'write',
        path: 'test.txt',
      }),
      expect.objectContaining({
        bytesTransferred: 5,
        durationMs: expect.any(Number),
      })
    )
  })

  it('should dispatch delete hook on delete', async () => {
    const hook = { onDelete: vi.fn() }
    globalHookRegistry.registerStorageHook(hook)

    // Write some data first
    await backend.write('test.txt', new Uint8Array([1, 2, 3]))

    // Delete through observed backend
    await observedBackend.delete('test.txt')

    expect(hook.onDelete).toHaveBeenCalledTimes(1)
    expect(hook.onDelete).toHaveBeenCalledWith(
      expect.objectContaining({
        operationType: 'delete',
        path: 'test.txt',
      }),
      expect.objectContaining({
        durationMs: expect.any(Number),
        fileCount: 1,
      })
    )
  })

  it('should dispatch error hook on read error', async () => {
    const hook = { onStorageError: vi.fn() }
    globalHookRegistry.registerStorageHook(hook)

    // Try to read non-existent file
    await expect(observedBackend.read('nonexistent.txt')).rejects.toThrow()

    expect(hook.onStorageError).toHaveBeenCalledTimes(1)
    expect(hook.onStorageError).toHaveBeenCalledWith(
      expect.objectContaining({
        operationType: 'read',
        path: 'nonexistent.txt',
      }),
      expect.any(Error)
    )
  })

  it('should dispatch read hook for list operation', async () => {
    const hook = { onRead: vi.fn() }
    globalHookRegistry.registerStorageHook(hook)

    // Write some data first
    await backend.write('data/test1.txt', new Uint8Array([1]))
    await backend.write('data/test2.txt', new Uint8Array([2]))

    // List through observed backend
    await observedBackend.list('data/')

    expect(hook.onRead).toHaveBeenCalledTimes(1)
    expect(hook.onRead).toHaveBeenCalledWith(
      expect.objectContaining({
        operationType: 'list',
        path: 'data/',
      }),
      expect.objectContaining({
        fileCount: 2,
      })
    )
  })

  it('should dispatch write hook for append operation', async () => {
    const hook = { onWrite: vi.fn() }
    globalHookRegistry.registerStorageHook(hook)

    await observedBackend.append('log.txt', new Uint8Array([1, 2, 3]))

    expect(hook.onWrite).toHaveBeenCalledTimes(1)
    expect(hook.onWrite).toHaveBeenCalledWith(
      expect.objectContaining({
        operationType: 'append',
        path: 'log.txt',
      }),
      expect.objectContaining({
        bytesTransferred: 3,
      })
    )
  })

  it('should work with withObservability helper', async () => {
    const { withObservability, MemoryBackend } = await import('../../../src/storage')
    const memory = new MemoryBackend()
    const observed = withObservability(memory)

    expect(observed.type).toBe('observed:memory')

    const hook = { onWrite: vi.fn() }
    globalHookRegistry.registerStorageHook(hook)

    await observed.write('test.txt', new Uint8Array([1]))

    expect(hook.onWrite).toHaveBeenCalled()
  })

  it('should not double-wrap an already observed backend', async () => {
    const { withObservability, MemoryBackend, ObservedBackend } = await import('../../../src/storage')
    const memory = new MemoryBackend()
    const observed = withObservability(memory)
    const doubleObserved = withObservability(observed)

    // Should be the same instance
    expect(doubleObserved).toBe(observed)
    expect(doubleObserved.type).toBe('observed:memory')
  })
})
