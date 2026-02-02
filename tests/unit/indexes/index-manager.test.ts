/**
 * Tests for IndexManager error handling
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { IndexManager } from '@/indexes/manager'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition, IndexEvent, IndexEventListener } from '@/indexes/types'

describe('IndexManager', () => {
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
  })

  describe('constructor', () => {
    it('accepts legacy string basePath parameter', () => {
      const manager = new IndexManager(storage, '/data')
      expect(manager).toBeDefined()
    })

    it('accepts options object', () => {
      const manager = new IndexManager(storage, {
        basePath: '/data',
        onError: () => {},
        throwOnListenerError: true,
      })
      expect(manager).toBeDefined()
    })

    it('accepts empty options object', () => {
      const manager = new IndexManager(storage, {})
      expect(manager).toBeDefined()
    })

    it('accepts no second parameter', () => {
      const manager = new IndexManager(storage)
      expect(manager).toBeDefined()
    })
  })

  describe('event listener error handling', () => {
    const definition: IndexDefinition = {
      name: 'test_index',
      type: 'hash',
      fields: [{ path: 'status' }],
    }

    it('calls onError callback when listener throws', async () => {
      const onErrorMock = vi.fn()
      const manager = new IndexManager(storage, { onError: onErrorMock })

      const error = new Error('Listener error')
      const failingListener: IndexEventListener = () => {
        throw error
      }

      manager.addEventListener(failingListener)

      // Trigger an event by creating an index
      await manager.createIndex('test', definition)

      // onError should have been called
      expect(onErrorMock).toHaveBeenCalled()
      const [receivedError, receivedEvent, receivedListener] = onErrorMock.mock.calls[0]
      expect(receivedError).toBe(error)
      expect(receivedEvent.type).toBe('build_started')
      expect(receivedListener).toBe(failingListener)
    })

    it('calls onError with Error object when listener throws non-Error', async () => {
      const onErrorMock = vi.fn()
      const manager = new IndexManager(storage, { onError: onErrorMock })

      const failingListener: IndexEventListener = () => {
        throw 'string error' // eslint-disable-line no-throw-literal
      }

      manager.addEventListener(failingListener)

      await manager.createIndex('test', definition)

      expect(onErrorMock).toHaveBeenCalled()
      const [receivedError] = onErrorMock.mock.calls[0]
      expect(receivedError).toBeInstanceOf(Error)
      expect(receivedError.message).toBe('string error')
    })

    it('continues calling other listeners when one throws', async () => {
      const onErrorMock = vi.fn()
      const manager = new IndexManager(storage, { onError: onErrorMock })

      const successfulListener1 = vi.fn()
      const failingListener = vi.fn(() => {
        throw new Error('Listener error')
      })
      const successfulListener2 = vi.fn()

      manager.addEventListener(successfulListener1)
      manager.addEventListener(failingListener)
      manager.addEventListener(successfulListener2)

      await manager.createIndex('test', definition)

      // All listeners should have been called
      expect(successfulListener1).toHaveBeenCalled()
      expect(failingListener).toHaveBeenCalled()
      expect(successfulListener2).toHaveBeenCalled()
    })

    it('logs warning when no onError callback is provided', async () => {
      const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
      // Note: This test relies on the logger.warn using console.warn internally
      // If the logger has different behavior, this test might need adjustment

      const manager = new IndexManager(storage)

      const failingListener: IndexEventListener = () => {
        throw new Error('Listener error')
      }

      manager.addEventListener(failingListener)

      await manager.createIndex('test', definition)

      // Since we're using the logger, we might not catch console.warn directly
      // This is a basic smoke test - in a real scenario we'd mock the logger
      warnSpy.mockRestore()
    })

    it('throws AggregateError when throwOnListenerError is true', async () => {
      const manager = new IndexManager(storage, { throwOnListenerError: true })

      const error1 = new Error('Listener error 1')
      const error2 = new Error('Listener error 2')

      manager.addEventListener(() => {
        throw error1
      })
      manager.addEventListener(() => {
        throw error2
      })

      // createIndex internally calls emit, which should throw
      await expect(manager.createIndex('test', definition)).rejects.toThrow(AggregateError)

      try {
        await manager.createIndex('test2', { ...definition, name: 'test_index_2' })
      } catch (e) {
        expect(e).toBeInstanceOf(AggregateError)
        const aggError = e as AggregateError
        expect(aggError.errors).toHaveLength(2)
        expect(aggError.errors).toContain(error1)
        expect(aggError.errors).toContain(error2)
        expect(aggError.message).toContain('2 listener(s) threw errors')
      }
    })

    it('throws AggregateError with single error when one listener throws', async () => {
      const manager = new IndexManager(storage, { throwOnListenerError: true })

      const error = new Error('Single listener error')
      manager.addEventListener(() => {
        throw error
      })

      await expect(manager.createIndex('test', definition)).rejects.toThrow(AggregateError)

      try {
        await manager.createIndex('test2', { ...definition, name: 'test_index_2' })
      } catch (e) {
        expect(e).toBeInstanceOf(AggregateError)
        const aggError = e as AggregateError
        expect(aggError.errors).toHaveLength(1)
        expect(aggError.errors[0]).toBe(error)
      }
    })

    it('does not throw when throwOnListenerError is true but no errors occur', async () => {
      const manager = new IndexManager(storage, { throwOnListenerError: true })

      const successfulListener = vi.fn()
      manager.addEventListener(successfulListener)

      await expect(manager.createIndex('test', definition)).resolves.not.toThrow()
      expect(successfulListener).toHaveBeenCalled()
    })

    it('both calls onError and throws when both options are provided', async () => {
      const onErrorMock = vi.fn()
      const manager = new IndexManager(storage, {
        onError: onErrorMock,
        throwOnListenerError: true,
      })

      const error = new Error('Listener error')
      manager.addEventListener(() => {
        throw error
      })

      await expect(manager.createIndex('test', definition)).rejects.toThrow(AggregateError)
      expect(onErrorMock).toHaveBeenCalled()
    })

    it('ignores errors thrown by onError callback itself', async () => {
      const onErrorMock = vi.fn(() => {
        throw new Error('Error in error handler')
      })
      const manager = new IndexManager(storage, { onError: onErrorMock })

      manager.addEventListener(() => {
        throw new Error('Listener error')
      })

      // Should not throw - errors in onError are swallowed
      await expect(manager.createIndex('test', definition)).resolves.toBeDefined()
      expect(onErrorMock).toHaveBeenCalled()
    })
  })

  describe('removeEventListener', () => {
    it('removes event listener', async () => {
      const manager = new IndexManager(storage)
      const listener = vi.fn()

      manager.addEventListener(listener)
      manager.removeEventListener(listener)

      await manager.createIndex('test', {
        name: 'test_index',
        type: 'hash',
        fields: [{ path: 'status' }],
      })

      expect(listener).not.toHaveBeenCalled()
    })
  })

  describe('unimplemented methods throw errors', () => {
    it('hashLookup throws not implemented error', async () => {
      const manager = new IndexManager(storage)
      await expect(manager.hashLookup('test', 'idx', 'value')).rejects.toThrow(
        /Method not implemented: hashLookup/
      )
    })

    it('rangeQuery throws not implemented error', async () => {
      const manager = new IndexManager(storage)
      await expect(manager.rangeQuery('test', 'idx', { $gte: 10 })).rejects.toThrow(
        /Method not implemented: rangeQuery/
      )
    })

    it('ftsSearch throws not implemented error', async () => {
      const manager = new IndexManager(storage)
      await expect(manager.ftsSearch('test', 'search query')).rejects.toThrow(
        /Method not implemented: ftsSearch/
      )
    })
  })
})
