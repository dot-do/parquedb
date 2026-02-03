/**
 * Tests for WorkerErrors Materialized View
 *
 * Tests cover:
 * - Error extraction from events
 * - Error categorization and classification
 * - Index-based queries
 * - Statistics computation
 * - Memory management (max errors)
 */

import { describe, test, expect, beforeEach } from 'vitest'
import {
  WorkerErrorsMV,
  createWorkerErrorsMV,
} from '../../../src/streaming/worker-errors'
import {
  classifyError,
  severityFromStatus,
  categoryFromStatus,
  DEFAULT_ERROR_PATTERNS,
} from '../../../src/streaming/types'
import type { Event, EventOp } from '../../../src/types/entity'

// =============================================================================
// Test Helpers
// =============================================================================

function createEvent(
  id: string,
  data: Record<string, unknown>,
  op: EventOp = 'CREATE'
): Event {
  return {
    id,
    ts: Date.now(),
    op,
    target: 'logs:' + id,
    after: data,
    actor: 'test:user',
  }
}

function createErrorEvent(
  id: string,
  message: string,
  extra?: Record<string, unknown>
): Event {
  return createEvent(id, {
    errorMessage: message,
    ...extra,
  })
}

// =============================================================================
// Error Classification Tests
// =============================================================================

describe('Error Classification', () => {
  describe('classifyError', () => {
    test('classifies network errors', () => {
      const result = classifyError('fetch failed: ECONNREFUSED')
      expect(result.category).toBe('network')
      expect(result.severity).toBe('error')
    })

    test('classifies timeout errors', () => {
      const result = classifyError('Request timed out after 30s')
      expect(result.category).toBe('timeout')
      expect(result.severity).toBe('warning')
    })

    test('classifies validation errors', () => {
      const result = classifyError('Validation failed: required field missing')
      expect(result.category).toBe('validation')
      expect(result.severity).toBe('warning')
    })

    test('classifies authentication errors', () => {
      const result = classifyError('Unauthorized: token expired')
      expect(result.category).toBe('authentication')
      expect(result.severity).toBe('error')
    })

    test('classifies storage errors', () => {
      const result = classifyError('R2 bucket not found')
      expect(result.category).toBe('storage')
      expect(result.severity).toBe('error')
    })

    test('classifies query errors', () => {
      const result = classifyError('Invalid filter: unknown operator')
      expect(result.category).toBe('query')
      expect(result.severity).toBe('warning')
    })

    test('classifies rate limit errors', () => {
      const result = classifyError('Rate limit exceeded, too many requests')
      expect(result.category).toBe('rate_limit')
      expect(result.severity).toBe('warning')
    })

    test('classifies internal errors', () => {
      const result = classifyError('Internal server error: unexpected state')
      expect(result.category).toBe('internal')
      expect(result.severity).toBe('critical')
    })

    test('returns unknown for unmatched errors', () => {
      const result = classifyError('Something went wrong xyz123')
      expect(result.category).toBe('unknown')
      expect(result.severity).toBe('error')
    })

    test('uses provided code if no pattern match', () => {
      const result = classifyError('xyz123', 'CUSTOM_CODE')
      expect(result.code).toBe('CUSTOM_CODE')
    })
  })

  describe('severityFromStatus', () => {
    test('returns critical for 5xx errors', () => {
      expect(severityFromStatus(500)).toBe('critical')
      expect(severityFromStatus(503)).toBe('critical')
    })

    test('returns warning for 4xx errors', () => {
      expect(severityFromStatus(400)).toBe('warning')
      expect(severityFromStatus(404)).toBe('warning')
    })

    test('returns info for success codes', () => {
      expect(severityFromStatus(200)).toBe('info')
      expect(severityFromStatus(201)).toBe('info')
    })
  })

  describe('categoryFromStatus', () => {
    test('maps 400 to validation', () => {
      expect(categoryFromStatus(400)).toBe('validation')
    })

    test('maps 401/403 to authentication', () => {
      expect(categoryFromStatus(401)).toBe('authentication')
      expect(categoryFromStatus(403)).toBe('authentication')
    })

    test('maps 404 to storage', () => {
      expect(categoryFromStatus(404)).toBe('storage')
    })

    test('maps 408 to timeout', () => {
      expect(categoryFromStatus(408)).toBe('timeout')
    })

    test('maps 429 to rate_limit', () => {
      expect(categoryFromStatus(429)).toBe('rate_limit')
    })

    test('maps 5xx to internal', () => {
      expect(categoryFromStatus(500)).toBe('internal')
      expect(categoryFromStatus(502)).toBe('internal')
      expect(categoryFromStatus(503)).toBe('internal')
      expect(categoryFromStatus(504)).toBe('internal')
    })
  })
})

// =============================================================================
// WorkerErrorsMV Tests
// =============================================================================

describe('WorkerErrorsMV', () => {
  let mv: WorkerErrorsMV

  beforeEach(() => {
    mv = createWorkerErrorsMV()
  })

  describe('Event Processing', () => {
    test('processes error events with errorMessage field', async () => {
      const event = createErrorEvent('e1', 'Connection refused')
      await mv.process([event])

      expect(mv.count()).toBe(1)
      const errors = mv.getErrors()
      expect(errors[0]?.message).toBe('Connection refused')
    })

    test('processes error events with message field and level=error', async () => {
      // Note: just having a message field is not enough - need an error indicator
      const event = createEvent('e1', { message: 'Something failed', level: 'error' })
      await mv.process([event])

      expect(mv.count()).toBe(1)
    })

    test('processes error events with error object', async () => {
      const event = createEvent('e1', {
        error: { message: 'Nested error message', code: 'ERR_CODE' },
      })
      await mv.process([event])

      expect(mv.count()).toBe(1)
      const errors = mv.getErrors()
      expect(errors[0]?.message).toBe('Nested error message')
      expect(errors[0]?.code).toBe('ERR_CODE')
    })

    test('processes error events with status >= 400', async () => {
      const event = createEvent('e1', {
        status: 500,
        message: 'Internal error',
      })
      await mv.process([event])

      expect(mv.count()).toBe(1)
      const errors = mv.getErrors()
      expect(errors[0]?.severity).toBe('critical')
      expect(errors[0]?.category).toBe('internal')
    })

    test('processes error events with level=error', async () => {
      const event = createEvent('e1', {
        level: 'error',
        message: 'Log error message',
      })
      await mv.process([event])

      expect(mv.count()).toBe(1)
    })

    test('ignores non-error events', async () => {
      const event = createEvent('e1', {
        level: 'info',
        message: 'This is just info',
      })
      await mv.process([event])

      expect(mv.count()).toBe(0)
    })

    test('ignores events without error indicators', async () => {
      const event = createEvent('e1', {
        data: { foo: 'bar' },
      })
      await mv.process([event])

      expect(mv.count()).toBe(0)
    })

    test('processes multiple events in batch', async () => {
      const events = [
        createErrorEvent('e1', 'Error 1'),
        createErrorEvent('e2', 'Error 2'),
        createErrorEvent('e3', 'Error 3'),
      ]
      await mv.process(events)

      expect(mv.count()).toBe(3)
    })

    test('deduplicates events by ID', async () => {
      const event1 = createErrorEvent('same-id', 'Error 1')
      const event2 = createErrorEvent('same-id', 'Error 2')

      await mv.process([event1])
      await mv.process([event2])

      expect(mv.count()).toBe(1)
      expect(mv.getErrors()[0]?.message).toBe('Error 1')
    })
  })

  describe('Error Extraction', () => {
    test('extracts path and method', async () => {
      const event = createErrorEvent('e1', 'Not found', {
        path: '/api/users/123',
        method: 'GET',
      })
      await mv.process([event])

      const error = mv.getError('e1')
      expect(error?.path).toBe('/api/users/123')
      expect(error?.method).toBe('GET')
    })

    test('extracts requestId', async () => {
      const event = createErrorEvent('e1', 'Failed', {
        requestId: 'req-123',
      })
      await mv.process([event])

      const error = mv.getError('e1')
      expect(error?.requestId).toBe('req-123')
    })

    test('extracts worker name and colo', async () => {
      const event = createErrorEvent('e1', 'Worker error', {
        workerName: 'my-worker',
        colo: 'SJC',
      })
      await mv.process([event])

      const error = mv.getError('e1')
      expect(error?.workerName).toBe('my-worker')
      expect(error?.colo).toBe('SJC')
    })

    test('extracts stack trace', async () => {
      const event = createErrorEvent('e1', 'Error with stack', {
        stack: 'Error: test\n  at foo.js:10',
      })
      await mv.process([event])

      const error = mv.getError('e1')
      expect(error?.stack).toContain('foo.js:10')
    })

    test('extracts metadata', async () => {
      const event = createErrorEvent('e1', 'Error with metadata', {
        duration: 1500,
        cpuTime: 50,
        userAgent: 'Mozilla/5.0',
        query: { filter: { status: 'active' } },
      })
      await mv.process([event])

      const error = mv.getError('e1')
      expect(error?.metadata?.duration).toBe(1500)
      expect(error?.metadata?.cpuTime).toBe(50)
      expect(error?.metadata?.userAgent).toBe('Mozilla/5.0')
      expect(error?.metadata?.query).toEqual({ filter: { status: 'active' } })
    })
  })

  describe('Querying', () => {
    beforeEach(async () => {
      // Set up test data with various errors
      const events = [
        createErrorEvent('e1', 'Network error: ECONNREFUSED', {
          workerName: 'worker-a',
          path: '/api/users',
        }),
        createErrorEvent('e2', 'Validation failed: missing field', {
          workerName: 'worker-a',
          path: '/api/posts',
        }),
        createErrorEvent('e3', 'Unauthorized: invalid token', {
          workerName: 'worker-b',
          path: '/api/users',
        }),
        createErrorEvent('e4', 'Request timeout after 30s', {
          workerName: 'worker-b',
          path: '/api/heavy',
        }),
        createErrorEvent('e5', 'Internal server error', {
          workerName: 'worker-c',
          path: '/api/admin',
        }),
      ]
      await mv.process(events)
    })

    test('getErrors returns all errors (most recent first)', () => {
      const errors = mv.getErrors()
      expect(errors).toHaveLength(5)
      expect(errors[0]?.id).toBe('e5')
      expect(errors[4]?.id).toBe('e1')
    })

    test('getErrors respects limit', () => {
      const errors = mv.getErrors(2)
      expect(errors).toHaveLength(2)
      expect(errors[0]?.id).toBe('e5')
      expect(errors[1]?.id).toBe('e4')
    })

    test('getErrorsByCategory returns correct errors', () => {
      const networkErrors = mv.getErrorsByCategory('network')
      expect(networkErrors).toHaveLength(1)
      expect(networkErrors[0]?.id).toBe('e1')

      const validationErrors = mv.getErrorsByCategory('validation')
      expect(validationErrors).toHaveLength(1)
      expect(validationErrors[0]?.id).toBe('e2')
    })

    test('getErrorsBySeverity returns correct errors', () => {
      const criticalErrors = mv.getErrorsBySeverity('critical')
      expect(criticalErrors).toHaveLength(1)
      expect(criticalErrors[0]?.id).toBe('e5')

      const warningErrors = mv.getErrorsBySeverity('warning')
      expect(warningErrors).toHaveLength(2) // validation + timeout
    })

    test('getErrorsByWorker returns correct errors', () => {
      const workerAErrors = mv.getErrorsByWorker('worker-a')
      expect(workerAErrors).toHaveLength(2)

      const workerBErrors = mv.getErrorsByWorker('worker-b')
      expect(workerBErrors).toHaveLength(2)

      const workerCErrors = mv.getErrorsByWorker('worker-c')
      expect(workerCErrors).toHaveLength(1)
    })

    test('getErrorsByPath returns correct errors', () => {
      const userErrors = mv.getErrorsByPath('/api/users')
      expect(userErrors).toHaveLength(2)
    })

    test('getError returns single error by ID', () => {
      const error = mv.getError('e3')
      expect(error).toBeDefined()
      expect(error?.message).toContain('Unauthorized')
    })

    test('getError returns undefined for unknown ID', () => {
      const error = mv.getError('unknown')
      expect(error).toBeUndefined()
    })

    test('getErrorsInRange filters by timestamp', async () => {
      const now = Date.now()
      mv.clear()

      // Create events with specific timestamps
      const event1 = createErrorEvent('t1', 'Error 1')
      event1.ts = now - 60000 // 1 minute ago
      const event2 = createErrorEvent('t2', 'Error 2')
      event2.ts = now - 30000 // 30 seconds ago
      const event3 = createErrorEvent('t3', 'Error 3')
      event3.ts = now // now

      await mv.process([event1, event2, event3])

      const recentErrors = mv.getErrorsInRange(now - 45000, now)
      expect(recentErrors).toHaveLength(2)
    })
  })

  describe('Statistics', () => {
    beforeEach(async () => {
      const events = [
        createErrorEvent('s1', 'Network error', { workerName: 'w1' }),
        createErrorEvent('s2', 'Network error', { workerName: 'w1' }),
        createErrorEvent('s3', 'Validation error', { workerName: 'w2' }),
        createErrorEvent('s4', 'Internal error', { workerName: 'w2' }),
        createErrorEvent('s5', 'Timeout error', { workerName: 'w3' }),
      ]
      await mv.process(events)
    })

    test('getStats returns correct totals', () => {
      const stats = mv.getStats()
      expect(stats.totalErrors).toBe(5)
    })

    test('getStats returns correct byCategory counts', () => {
      const stats = mv.getStats()
      expect(stats.byCategory.network).toBe(2)
      expect(stats.byCategory.validation).toBe(1)
      expect(stats.byCategory.internal).toBe(1)
      expect(stats.byCategory.timeout).toBe(1)
    })

    test('getStats returns correct bySeverity counts', () => {
      const stats = mv.getStats()
      expect(stats.bySeverity.error).toBe(2) // network
      expect(stats.bySeverity.warning).toBe(2) // validation + timeout
      expect(stats.bySeverity.critical).toBe(1) // internal
    })

    test('getStats returns correct byWorker counts', () => {
      const stats = mv.getStats()
      expect(stats.byWorker.w1).toBe(2)
      expect(stats.byWorker.w2).toBe(2)
      expect(stats.byWorker.w3).toBe(1)
    })

    test('getStats calculates error rate per minute', () => {
      const stats = mv.getStats()
      // All 5 errors are within the default 1-minute window
      expect(stats.errorRatePerMinute).toBeGreaterThan(0)
    })

    test('getStats includes time range', () => {
      const stats = mv.getStats()
      expect(stats.timeRange.start).toBeLessThanOrEqual(stats.timeRange.end)
    })
  })

  describe('Memory Management', () => {
    test('enforces maxErrors limit', async () => {
      const mv = createWorkerErrorsMV({ maxErrors: 5 })

      const events = Array.from({ length: 10 }, (_, i) =>
        createErrorEvent(`e${i}`, `Error ${i}`)
      )

      await mv.process(events)

      expect(mv.count()).toBe(5)
      // Should have kept the most recent 5
      expect(mv.getError('e5')).toBeDefined()
      expect(mv.getError('e9')).toBeDefined()
      expect(mv.getError('e0')).toBeUndefined()
    })

    test('clear removes all errors', async () => {
      await mv.process([createErrorEvent('e1', 'Error 1')])
      expect(mv.count()).toBe(1)

      mv.clear()

      expect(mv.count()).toBe(0)
      expect(mv.getErrors()).toHaveLength(0)
    })

    test('clear resets all indexes', async () => {
      await mv.process([
        createErrorEvent('e1', 'Network error', { workerName: 'w1' }),
      ])

      mv.clear()

      expect(mv.getErrorsByCategory('network')).toHaveLength(0)
      expect(mv.getErrorsByWorker('w1')).toHaveLength(0)
    })
  })

  describe('Custom Configuration', () => {
    test('accepts custom error patterns', async () => {
      const customMV = createWorkerErrorsMV({
        errorPatterns: [
          {
            pattern: /custom_error_xyz/i,
            category: 'storage',
            severity: 'critical',
          },
        ],
      })

      await customMV.process([
        createErrorEvent('e1', 'custom_error_xyz happened'),
      ])

      const error = customMV.getError('e1')
      expect(error?.category).toBe('storage')
      expect(error?.severity).toBe('critical')
    })

    test('accepts custom source namespaces', () => {
      const customMV = createWorkerErrorsMV({
        sourceNamespaces: ['custom-errors', 'custom-logs'],
      })

      expect(customMV.sourceNamespaces).toEqual(['custom-errors', 'custom-logs'])
    })

    test('accepts custom stats window', async () => {
      const customMV = createWorkerErrorsMV({
        statsWindowMs: 30000, // 30 seconds
      })

      await customMV.process([createErrorEvent('e1', 'Error 1')])
      const stats = customMV.getStats()

      // Error rate should be calculated over 30 seconds
      expect(stats.errorRatePerMinute).toBe(2) // 1 error in 0.5 minutes = 2 per minute
    })
  })

  describe('MVHandler Interface', () => {
    test('has correct name', () => {
      expect(mv.name).toBe('WorkerErrors')
    })

    test('has default source namespaces', () => {
      expect(mv.sourceNamespaces).toEqual(['logs', 'errors', 'workers'])
    })

    test('process returns a promise', async () => {
      const result = mv.process([createErrorEvent('e1', 'Test')])
      expect(result).toBeInstanceOf(Promise)
      await result
    })
  })
})

// =============================================================================
// Integration with StreamingRefreshEngine
// =============================================================================

describe('WorkerErrorsMV Integration', () => {
  test('can be used with streaming engine', async () => {
    const { createStreamingRefreshEngine, createWorkerErrorsMV } = await import('../../../src/streaming')

    const mv = createWorkerErrorsMV()
    const engine = createStreamingRefreshEngine()

    engine.registerMV(mv)
    await engine.start()

    // Simulate an error event
    await engine.processEvent({
      id: 'int-1',
      ts: Date.now(),
      op: 'CREATE',
      target: 'logs:int-1',
      after: { errorMessage: 'Integration test error' },
      actor: 'test',
    })

    await engine.flush()
    await engine.stop()

    expect(mv.count()).toBe(1)
    expect(mv.getError('int-1')?.message).toBe('Integration test error')
  })
})

// =============================================================================
// Parquet Persistence Tests
// =============================================================================

describe('WorkerErrorsMV Parquet Persistence', () => {
  /**
   * Create a mock storage backend for testing
   */
  function createMockStorage() {
    const files = new Map<string, Uint8Array>()
    return {
      files,
      type: 'memory',
      read: async (path: string) => {
        const data = files.get(path)
        if (!data) throw new Error(`File not found: ${path}`)
        return data
      },
      readRange: async (path: string, start: number, end: number) => {
        const data = files.get(path)
        if (!data) throw new Error(`File not found: ${path}`)
        return data.slice(start, end)
      },
      exists: async (path: string) => files.has(path),
      stat: async (path: string) => {
        const data = files.get(path)
        if (!data) return null
        return {
          path,
          size: data.length,
          mtime: new Date(),
          isDirectory: false,
        }
      },
      list: async (prefix: string) => {
        const matchingFiles = Array.from(files.keys()).filter(k => k.startsWith(prefix))
        return { files: matchingFiles, hasMore: false }
      },
      write: async (path: string, data: Uint8Array) => {
        files.set(path, data)
        return { etag: 'test-etag', size: data.length }
      },
      writeAtomic: async (path: string, data: Uint8Array) => {
        files.set(path, data)
        return { etag: 'test-etag', size: data.length }
      },
      append: async () => {},
      delete: async (path: string) => {
        return files.delete(path)
      },
      deletePrefix: async () => 0,
      mkdir: async () => {},
      rmdir: async () => {},
      writeConditional: async (path: string, data: Uint8Array) => {
        files.set(path, data)
        return { etag: 'test-etag', size: data.length }
      },
      copy: async () => {},
      move: async () => {},
    }
  }

  describe('Configuration', () => {
    test('persistence is disabled by default', () => {
      const mv = createWorkerErrorsMV()
      expect(mv.isPersistenceEnabled()).toBe(false)
    })

    test('persistence is enabled when storage is configured', () => {
      const storage = createMockStorage()
      const mv = createWorkerErrorsMV({
        storage: storage as any,
        datasetPath: 'errors/workers',
      })
      expect(mv.isPersistenceEnabled()).toBe(true)
    })

    test('accepts custom flush configuration', () => {
      const storage = createMockStorage()
      const mv = createWorkerErrorsMV({
        storage: storage as any,
        datasetPath: 'errors/workers',
        flushThreshold: 100,
        flushIntervalMs: 5000,
        compression: 'snappy',
        rowGroupSize: 1000,
      })
      expect(mv.isPersistenceEnabled()).toBe(true)
    })
  })

  describe('Lifecycle', () => {
    test('start enables periodic flushing', () => {
      const storage = createMockStorage()
      const mv = createWorkerErrorsMV({
        storage: storage as any,
        datasetPath: 'errors/workers',
      })

      expect(mv.isRunning()).toBe(false)
      mv.start()
      expect(mv.isRunning()).toBe(true)
    })

    test('stop disables periodic flushing', async () => {
      const storage = createMockStorage()
      const mv = createWorkerErrorsMV({
        storage: storage as any,
        datasetPath: 'errors/workers',
      })

      mv.start()
      await mv.stop()
      expect(mv.isRunning()).toBe(false)
    })

    test('stop flushes remaining errors', async () => {
      const storage = createMockStorage()
      const mv = createWorkerErrorsMV({
        storage: storage as any,
        datasetPath: 'errors/workers',
        flushThreshold: 1000, // High threshold so it doesn't auto-flush
      })

      mv.start()

      // Add some errors
      await mv.process([
        createEvent('e1', { errorMessage: 'Error 1' }),
        createEvent('e2', { errorMessage: 'Error 2' }),
      ])

      expect(mv.getBuffer().length).toBe(2)

      await mv.stop()

      expect(mv.getBuffer().length).toBe(0)
      expect(storage.files.size).toBe(1)
    })
  })

  describe('Buffering', () => {
    test('errors are added to buffer when persistence is enabled', async () => {
      const storage = createMockStorage()
      const mv = createWorkerErrorsMV({
        storage: storage as any,
        datasetPath: 'errors/workers',
        flushThreshold: 1000,
      })

      await mv.process([
        createEvent('e1', { errorMessage: 'Error 1' }),
      ])

      expect(mv.getBuffer().length).toBe(1)
      expect(mv.getBuffer()[0]?.id).toBe('e1')
    })

    test('buffer is not used when persistence is disabled', async () => {
      const mv = createWorkerErrorsMV()

      await mv.process([
        createEvent('e1', { errorMessage: 'Error 1' }),
      ])

      expect(mv.getBuffer().length).toBe(0)
    })

    test('flush clears buffer and writes to storage', async () => {
      const storage = createMockStorage()
      const mv = createWorkerErrorsMV({
        storage: storage as any,
        datasetPath: 'errors/workers',
        flushThreshold: 1000,
      })

      await mv.process([
        createEvent('e1', { errorMessage: 'Error 1' }),
        createEvent('e2', { errorMessage: 'Error 2' }),
      ])

      expect(mv.getBuffer().length).toBe(2)

      await mv.flush()

      expect(mv.getBuffer().length).toBe(0)
      expect(storage.files.size).toBe(1)
    })

    test('auto-flushes when threshold is reached', async () => {
      const storage = createMockStorage()
      const mv = createWorkerErrorsMV({
        storage: storage as any,
        datasetPath: 'errors/workers',
        flushThreshold: 3,
      })

      // Add 3 errors to trigger auto-flush
      await mv.process([
        createEvent('e1', { errorMessage: 'Error 1' }),
        createEvent('e2', { errorMessage: 'Error 2' }),
        createEvent('e3', { errorMessage: 'Error 3' }),
      ])

      // Buffer should be cleared after auto-flush
      expect(mv.getBuffer().length).toBe(0)
      expect(storage.files.size).toBe(1)
    })
  })

  describe('Statistics', () => {
    test('getExtendedStats includes persistence info', async () => {
      const storage = createMockStorage()
      const mv = createWorkerErrorsMV({
        storage: storage as any,
        datasetPath: 'errors/workers',
        flushThreshold: 2,
      })

      // Add errors and trigger flush
      await mv.process([
        createEvent('e1', { errorMessage: 'Error 1' }),
        createEvent('e2', { errorMessage: 'Error 2' }),
      ])

      const stats = mv.getExtendedStats()

      expect(stats.recordsWritten).toBe(2)
      expect(stats.filesCreated).toBe(1)
      expect(stats.bytesWritten).toBeGreaterThan(0)
      expect(stats.flushCount).toBe(1)
      expect(stats.lastFlushAt).not.toBeNull()
      expect(stats.bufferSize).toBe(0)
    })

    test('resetStats clears persistence stats', async () => {
      const storage = createMockStorage()
      const mv = createWorkerErrorsMV({
        storage: storage as any,
        datasetPath: 'errors/workers',
        flushThreshold: 2,
      })

      await mv.process([
        createEvent('e1', { errorMessage: 'Error 1' }),
        createEvent('e2', { errorMessage: 'Error 2' }),
      ])

      mv.resetStats()

      const stats = mv.getExtendedStats()
      expect(stats.recordsWritten).toBe(0)
      expect(stats.filesCreated).toBe(0)
      expect(stats.bytesWritten).toBe(0)
      expect(stats.flushCount).toBe(0)
      expect(stats.lastFlushAt).toBeNull()
    })
  })

  describe('File Partitioning', () => {
    test('writes files with time-based partitioning', async () => {
      const storage = createMockStorage()
      const mv = createWorkerErrorsMV({
        storage: storage as any,
        datasetPath: 'errors/workers',
        flushThreshold: 1,
      })

      await mv.process([
        createEvent('e1', { errorMessage: 'Error 1' }),
      ])

      // Check file path format
      const files = Array.from(storage.files.keys())
      expect(files.length).toBe(1)

      const filePath = files[0]!
      // Should match pattern: errors/workers/year=YYYY/month=MM/day=DD/hour=HH/errors-TIMESTAMP.parquet
      expect(filePath).toMatch(/^errors\/workers\/year=\d{4}\/month=\d{2}\/day=\d{2}\/hour=\d{2}\/errors-\d+\.parquet$/)
    })
  })

  describe('Error Handling', () => {
    test('restores buffer on write failure', async () => {
      const storage = createMockStorage()
      // Make writeAtomic fail
      storage.writeAtomic = async () => {
        throw new Error('Write failed')
      }

      const mv = createWorkerErrorsMV({
        storage: storage as any,
        datasetPath: 'errors/workers',
        flushThreshold: 1000,
      })

      await mv.process([
        createEvent('e1', { errorMessage: 'Error 1' }),
      ])

      expect(mv.getBuffer().length).toBe(1)

      await expect(mv.flush()).rejects.toThrow('Write failed')

      // Buffer should be restored
      expect(mv.getBuffer().length).toBe(1)
    })
  })

  describe('Parquet Output', () => {
    test('writes valid Parquet-like data', async () => {
      const storage = createMockStorage()
      const mv = createWorkerErrorsMV({
        storage: storage as any,
        datasetPath: 'errors/workers',
        flushThreshold: 1,
      })

      await mv.process([
        createEvent('e1', { errorMessage: 'Test error', workerName: 'test-worker' }),
      ])

      const files = Array.from(storage.files.entries())
      expect(files.length).toBe(1)

      const [, data] = files[0]!
      expect(data.length).toBeGreaterThan(0)

      // Check for Parquet magic bytes (PAR1)
      expect(data[0]).toBe(0x50) // P
      expect(data[1]).toBe(0x41) // A
      expect(data[2]).toBe(0x52) // R
      expect(data[3]).toBe(0x31) // 1
    })
  })
})
