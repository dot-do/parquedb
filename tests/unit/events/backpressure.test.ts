/**
 * Backpressure Test Suite
 *
 * Tests for the pending events backpressure mechanism in ParqueDB.
 * When the pending events queue exceeds maxPendingEvents, writes should
 * throw a BackpressureError to prevent unbounded memory growth.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { BackpressureError, isBackpressureError, ErrorCode } from '../../../src/errors'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { DEFAULT_EVENT_LOG_CONFIG } from '../../../src/ParqueDB/types'

describe('Pending Events Backpressure', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  // ===========================================================================
  // BackpressureError Class Tests
  // ===========================================================================

  describe('BackpressureError', () => {
    it('creates error with correct properties', () => {
      const error = new BackpressureError(100, 50, {
        operation: 'CREATE',
        namespace: 'posts',
      })

      expect(error).toBeInstanceOf(BackpressureError)
      expect(error.name).toBe('BackpressureError')
      expect(error.code).toBe(ErrorCode.BACKPRESSURE)
      expect(error.currentSize).toBe(100)
      expect(error.maxSize).toBe(50)
      expect(error.operation).toBe('CREATE')
      expect(error.namespace).toBe('posts')
    })

    it('creates error without optional context', () => {
      const error = new BackpressureError(10, 5)

      expect(error.currentSize).toBe(10)
      expect(error.maxSize).toBe(5)
      expect(error.operation).toBeUndefined()
      expect(error.namespace).toBeUndefined()
    })

    it('generates helpful error message', () => {
      const error = new BackpressureError(100, 50, { namespace: 'posts' })

      expect(error.message).toContain('Pending events queue is full')
      expect(error.message).toContain('100/50')
      expect(error.message).toContain('posts')
    })

    it('serializes correctly for RPC', () => {
      const error = new BackpressureError(100, 50, {
        operation: 'UPDATE',
        namespace: 'users',
      })

      const json = error.toJSON()

      expect(json.code).toBe('BACKPRESSURE')
      expect(json.name).toBe('BackpressureError')
      expect(json.context?.currentSize).toBe(100)
      expect(json.context?.maxSize).toBe(50)
      expect(json.context?.operation).toBe('UPDATE')
      expect(json.context?.namespace).toBe('users')
    })
  })

  // ===========================================================================
  // Type Guard Tests
  // ===========================================================================

  describe('isBackpressureError', () => {
    it('returns true for BackpressureError instances', () => {
      const error = new BackpressureError(10, 5)
      expect(isBackpressureError(error)).toBe(true)
    })

    it('returns false for other errors', () => {
      expect(isBackpressureError(new Error('other'))).toBe(false)
      expect(isBackpressureError(new TypeError('type error'))).toBe(false)
    })

    it('returns false for null/undefined', () => {
      expect(isBackpressureError(null)).toBe(false)
      expect(isBackpressureError(undefined)).toBe(false)
    })

    it('returns false for non-errors', () => {
      expect(isBackpressureError('string')).toBe(false)
      expect(isBackpressureError(123)).toBe(false)
      expect(isBackpressureError({})).toBe(false)
    })
  })

  // ===========================================================================
  // Configuration Tests
  // ===========================================================================

  describe('configuration', () => {
    it('default maxPendingEvents is 10000', () => {
      expect(DEFAULT_EVENT_LOG_CONFIG.maxPendingEvents).toBe(10000)
    })
  })

  // ===========================================================================
  // Unit Tests for recordEvent backpressure check
  // ===========================================================================

  describe('recordEvent backpressure', () => {
    it('throws when pendingEvents reaches limit', async () => {
      // Import recordEvent directly to test the backpressure logic
      const { recordEvent } = await import('../../../src/ParqueDB/event-operations')

      // Create a minimal context that simulates being at capacity
      // inTransaction: true prevents scheduleFlush from being called, avoiding async issues
      const mockCtx = {
        eventLogConfig: { maxPendingEvents: 2, maxEvents: 10000, maxAge: 86400000, archiveOnRotation: false, maxArchivedEvents: 50000 },
        pendingEvents: [{ id: '1' }, { id: '2' }] as any[], // Already at limit
        events: [],
        entities: new Map(),
        archivedEvents: [],
        snapshots: [],
        queryStats: new Map(),
        entityEventIndex: new Map(),
        reconstructionCache: new Map(),
        snapshotConfig: {},
        inTransaction: true, // Prevents async flush, keeping test isolated
        flushPromise: null,
        setFlushPromise: () => {},
        setPendingEvents: () => {},
        getSnapshotManager: () => ({ createSnapshot: async () => ({}) }) as any,
        storage: storage,
      }

      expect(() => {
        recordEvent(
          mockCtx as any,
          'CREATE',
          'posts:123',
          null,
          { $id: 'posts/123', $type: 'Post', name: 'Test' } as any
        )
      }).toThrow(BackpressureError)
    })

    it('does not throw when below limit', async () => {
      const { recordEvent } = await import('../../../src/ParqueDB/event-operations')

      const mockCtx = {
        eventLogConfig: { maxPendingEvents: 3, maxEvents: 10000, maxAge: 86400000, archiveOnRotation: false, maxArchivedEvents: 50000 },
        pendingEvents: [{ id: '1' }] as any[], // Below limit
        events: [],
        entities: new Map(),
        archivedEvents: [],
        snapshots: [],
        queryStats: new Map(),
        entityEventIndex: new Map(),
        reconstructionCache: new Map(),
        snapshotConfig: {},
        inTransaction: true, // Prevents async flush, keeping test isolated
        flushPromise: null,
        setFlushPromise: () => {},
        setPendingEvents: () => {},
        getSnapshotManager: () => ({ createSnapshot: async () => ({}) }) as any,
        storage: storage,
      }

      // Should not throw
      expect(() => {
        recordEvent(
          mockCtx as any,
          'CREATE',
          'posts:123',
          null,
          { $id: 'posts/123', $type: 'Post', name: 'Test' } as any
        )
      }).not.toThrow()
    })

    it('does not throw when maxPendingEvents is 0 (disabled)', async () => {
      const { recordEvent } = await import('../../../src/ParqueDB/event-operations')

      const mockCtx = {
        eventLogConfig: { maxPendingEvents: 0, maxEvents: 10000, maxAge: 86400000, archiveOnRotation: false, maxArchivedEvents: 50000 },
        pendingEvents: Array(1000).fill({ id: 'x' }) as any[], // Many events but disabled
        events: [],
        entities: new Map(),
        archivedEvents: [],
        snapshots: [],
        queryStats: new Map(),
        entityEventIndex: new Map(),
        reconstructionCache: new Map(),
        snapshotConfig: {},
        inTransaction: true, // Prevents async flush, keeping test isolated
        flushPromise: null,
        setFlushPromise: () => {},
        setPendingEvents: () => {},
        getSnapshotManager: () => ({ createSnapshot: async () => ({}) }) as any,
        storage: storage,
      }

      // Should not throw even with many pending events
      expect(() => {
        recordEvent(
          mockCtx as any,
          'CREATE',
          'posts:123',
          null,
          { $id: 'posts/123', $type: 'Post', name: 'Test' } as any
        )
      }).not.toThrow()
    })

    it('includes namespace in error context', async () => {
      const { recordEvent } = await import('../../../src/ParqueDB/event-operations')

      const mockCtx = {
        eventLogConfig: { maxPendingEvents: 1, maxEvents: 10000, maxAge: 86400000, archiveOnRotation: false, maxArchivedEvents: 50000 },
        pendingEvents: [{ id: '1' }] as any[],
        events: [],
        entities: new Map(),
        archivedEvents: [],
        snapshots: [],
        queryStats: new Map(),
        entityEventIndex: new Map(),
        reconstructionCache: new Map(),
        snapshotConfig: {},
        inTransaction: true, // Prevents async flush, keeping test isolated
        flushPromise: null,
        setFlushPromise: () => {},
        setPendingEvents: () => {},
        getSnapshotManager: () => ({ createSnapshot: async () => ({}) }) as any,
        storage: storage,
      }

      try {
        recordEvent(
          mockCtx as any,
          'UPDATE',
          'users:456',
          { $id: 'users/456', $type: 'User', name: 'Old' } as any,
          { $id: 'users/456', $type: 'User', name: 'New' } as any
        )
        expect.fail('Should have thrown')
      } catch (error) {
        expect(isBackpressureError(error)).toBe(true)
        const bpError = error as BackpressureError
        expect(bpError.namespace).toBe('users')
        expect(bpError.operation).toBe('UPDATE')
      }
    })
  })
})
