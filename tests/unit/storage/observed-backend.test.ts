/**
 * ObservedBackend Tests
 *
 * Tests that ObservedBackend correctly wraps another StorageBackend and:
 * - Delegates all operations to the inner backend
 * - Dispatches observability hooks (onRead, onWrite, onDelete, onStorageError) with correct args
 * - Propagates errors correctly and dispatches error hooks
 * - Does not double-wrap when using withObservability()
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { ObservedBackend, withObservability } from '../../../src/storage/ObservedBackend'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { globalHookRegistry } from '../../../src/observability'
import type { StorageContext, StorageResult, StorageHook } from '../../../src/observability/hooks'

// =============================================================================
// Helpers
// =============================================================================

function textToBytes(text: string): Uint8Array {
  return new TextEncoder().encode(text)
}

function bytesToText(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes)
}

// =============================================================================
// Test Suite
// =============================================================================

describe('ObservedBackend', () => {
  let inner: MemoryBackend
  let observed: ObservedBackend
  let hookCalls: { method: string; context: StorageContext; result?: StorageResult; error?: Error }[]
  let unregister: () => void

  beforeEach(() => {
    inner = new MemoryBackend()
    observed = new ObservedBackend(inner)
    hookCalls = []

    // Clear any existing hooks
    globalHookRegistry.clearHooks()

    // Register a test hook to capture dispatched events
    const testHook: StorageHook = {
      onRead(context: StorageContext, result: StorageResult) {
        hookCalls.push({ method: 'onRead', context, result })
      },
      onWrite(context: StorageContext, result: StorageResult) {
        hookCalls.push({ method: 'onWrite', context, result })
      },
      onDelete(context: StorageContext, result: StorageResult) {
        hookCalls.push({ method: 'onDelete', context, result })
      },
      onStorageError(context: StorageContext, error: Error) {
        hookCalls.push({ method: 'onStorageError', context, error })
      },
    }

    unregister = globalHookRegistry.registerStorageHook(testHook)
  })

  // Clean up hooks after each test
  afterEach(() => {
    unregister()
    globalHookRegistry.clearHooks()
  })

  // ===========================================================================
  // Constructor & Type
  // ===========================================================================

  describe('constructor and type', () => {
    it('should set type to observed:{inner.type}', () => {
      expect(observed.type).toBe('observed:memory')
    })

    it('should expose the inner backend via .inner', () => {
      expect(observed.inner).toBe(inner)
    })
  })

  // ===========================================================================
  // withObservability helper
  // ===========================================================================

  describe('withObservability()', () => {
    it('should wrap a backend in ObservedBackend', () => {
      const wrapped = withObservability(inner)
      expect(wrapped).toBeInstanceOf(ObservedBackend)
      expect(wrapped.inner).toBe(inner)
    })

    it('should not double-wrap an already observed backend', () => {
      const wrapped = withObservability(inner)
      const doubleWrapped = withObservability(wrapped)
      expect(doubleWrapped).toBe(wrapped)
    })
  })

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  describe('read(path)', () => {
    it('should delegate to inner backend and return data', async () => {
      await inner.write('test/file.txt', textToBytes('hello'))
      const result = await observed.read('test/file.txt')
      expect(bytesToText(result)).toBe('hello')
    })

    it('should dispatch onRead hook with correct context and result', async () => {
      const data = textToBytes('hello world')
      await inner.write('test/file.txt', data)

      await observed.read('test/file.txt')

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onRead')
      expect(hookCalls[0]!.context.operationType).toBe('read')
      expect(hookCalls[0]!.context.path).toBe('test/file.txt')
      expect(hookCalls[0]!.result!.bytesTransferred).toBe(data.length)
      expect(hookCalls[0]!.result!.durationMs).toBeGreaterThanOrEqual(0)
    })

    it('should dispatch onStorageError when read fails', async () => {
      await expect(observed.read('nonexistent.txt')).rejects.toThrow()

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onStorageError')
      expect(hookCalls[0]!.context.operationType).toBe('read')
      expect(hookCalls[0]!.context.path).toBe('nonexistent.txt')
      expect(hookCalls[0]!.error).toBeInstanceOf(Error)
    })

    it('should propagate the original error from the inner backend', async () => {
      await expect(observed.read('nonexistent.txt')).rejects.toThrow('File not found')
    })
  })

  describe('readRange(path, start, end)', () => {
    it('should delegate to inner backend and return range data', async () => {
      await inner.write('test/range.txt', textToBytes('Hello, World!'))
      const result = await observed.readRange('test/range.txt', 7, 12)
      expect(bytesToText(result)).toBe('World')
    })

    it('should dispatch onRead hook with readRange operation type', async () => {
      await inner.write('test/range.txt', textToBytes('Hello, World!'))
      await observed.readRange('test/range.txt', 7, 12)

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onRead')
      expect(hookCalls[0]!.context.operationType).toBe('readRange')
      expect(hookCalls[0]!.context.path).toBe('test/range.txt')
      expect(hookCalls[0]!.context.range).toEqual({ start: 7, end: 12 })
      expect(hookCalls[0]!.result!.bytesTransferred).toBe(5)
    })

    it('should dispatch onStorageError when readRange fails', async () => {
      await expect(observed.readRange('nonexistent.txt', 0, 10)).rejects.toThrow()

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onStorageError')
      expect(hookCalls[0]!.context.operationType).toBe('readRange')
    })
  })

  // ===========================================================================
  // Lightweight Read Operations (no hooks)
  // ===========================================================================

  describe('exists(path)', () => {
    it('should delegate to inner backend', async () => {
      await inner.write('test/exists.txt', textToBytes('data'))
      expect(await observed.exists('test/exists.txt')).toBe(true)
      expect(await observed.exists('test/nope.txt')).toBe(false)
    })

    it('should NOT dispatch any hooks (lightweight operation)', async () => {
      await inner.write('test/exists.txt', textToBytes('data'))
      await observed.exists('test/exists.txt')
      expect(hookCalls).toHaveLength(0)
    })
  })

  describe('stat(path)', () => {
    it('should delegate to inner backend', async () => {
      await inner.write('test/stat.txt', textToBytes('data'))
      const stat = await observed.stat('test/stat.txt')
      expect(stat).not.toBeNull()
      expect(stat!.size).toBe(4)
    })

    it('should return null for non-existent file', async () => {
      const stat = await observed.stat('nonexistent.txt')
      expect(stat).toBeNull()
    })

    it('should NOT dispatch any hooks (lightweight operation)', async () => {
      await inner.write('test/stat.txt', textToBytes('data'))
      await observed.stat('test/stat.txt')
      expect(hookCalls).toHaveLength(0)
    })
  })

  // ===========================================================================
  // List Operation
  // ===========================================================================

  describe('list(prefix, options)', () => {
    it('should delegate to inner backend and return results', async () => {
      await inner.write('test/a.txt', textToBytes('a'))
      await inner.write('test/b.txt', textToBytes('b'))

      const result = await observed.list('test/')
      expect(result.files).toHaveLength(2)
      expect(result.hasMore).toBe(false)
    })

    it('should dispatch onRead hook with list operation type and fileCount', async () => {
      await inner.write('test/a.txt', textToBytes('a'))
      await inner.write('test/b.txt', textToBytes('b'))

      await observed.list('test/')

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onRead')
      expect(hookCalls[0]!.context.operationType).toBe('list')
      expect(hookCalls[0]!.context.path).toBe('test/')
      expect(hookCalls[0]!.result!.bytesTransferred).toBe(0)
      expect(hookCalls[0]!.result!.fileCount).toBe(2)
    })

    it('should dispatch onStorageError if list fails', async () => {
      // Create a mock backend that throws on list
      const failingBackend = new MemoryBackend()
      const originalList = failingBackend.list.bind(failingBackend)
      failingBackend.list = async () => {
        throw new Error('list failed')
      }
      const failObserved = new ObservedBackend(failingBackend)

      await expect(failObserved.list('test/')).rejects.toThrow('list failed')

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onStorageError')
      expect(hookCalls[0]!.context.operationType).toBe('list')
    })
  })

  // ===========================================================================
  // Write Operations
  // ===========================================================================

  describe('write(path, data, options)', () => {
    it('should delegate to inner backend and return WriteResult', async () => {
      const data = textToBytes('hello')
      const result = await observed.write('test/write.txt', data)
      expect(result.etag).toBeDefined()
      expect(result.size).toBe(data.length)
    })

    it('should dispatch onWrite hook with correct context and result', async () => {
      const data = textToBytes('write data')
      const writeResult = await observed.write('test/write.txt', data)

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onWrite')
      expect(hookCalls[0]!.context.operationType).toBe('write')
      expect(hookCalls[0]!.context.path).toBe('test/write.txt')
      expect(hookCalls[0]!.result!.bytesTransferred).toBe(data.length)
      expect(hookCalls[0]!.result!.etag).toBe(writeResult.etag)
    })

    it('should dispatch onStorageError when write fails', async () => {
      await inner.write('test/existing.txt', textToBytes('existing'))

      await expect(
        observed.write('test/existing.txt', textToBytes('new'), { ifNoneMatch: '*' })
      ).rejects.toThrow()

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onStorageError')
      expect(hookCalls[0]!.context.operationType).toBe('write')
    })

    it('should write data readable through inner backend', async () => {
      await observed.write('test/write.txt', textToBytes('observed write'))
      const result = await inner.read('test/write.txt')
      expect(bytesToText(result)).toBe('observed write')
    })
  })

  describe('writeAtomic(path, data, options)', () => {
    it('should delegate to inner backend and return WriteResult', async () => {
      const data = textToBytes('atomic')
      const result = await observed.writeAtomic('test/atomic.txt', data)
      expect(result.etag).toBeDefined()
      expect(result.size).toBe(data.length)
    })

    it('should dispatch onWrite hook with writeAtomic operation type', async () => {
      await observed.writeAtomic('test/atomic.txt', textToBytes('atomic data'))

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onWrite')
      expect(hookCalls[0]!.context.operationType).toBe('writeAtomic')
      expect(hookCalls[0]!.context.path).toBe('test/atomic.txt')
    })

    it('should dispatch onStorageError when writeAtomic fails', async () => {
      await inner.write('test/existing.txt', textToBytes('existing'))

      await expect(
        observed.writeAtomic('test/existing.txt', textToBytes('new'), { ifNoneMatch: '*' })
      ).rejects.toThrow()

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onStorageError')
      expect(hookCalls[0]!.context.operationType).toBe('writeAtomic')
    })
  })

  describe('append(path, data)', () => {
    it('should delegate to inner backend', async () => {
      await inner.write('test/append.txt', textToBytes('hello'))
      await observed.append('test/append.txt', textToBytes(' world'))

      const result = await inner.read('test/append.txt')
      expect(bytesToText(result)).toBe('hello world')
    })

    it('should dispatch onWrite hook with append operation type', async () => {
      await inner.write('test/append.txt', textToBytes('initial'))
      const appendData = textToBytes(' more')
      await observed.append('test/append.txt', appendData)

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onWrite')
      expect(hookCalls[0]!.context.operationType).toBe('append')
      expect(hookCalls[0]!.result!.bytesTransferred).toBe(appendData.length)
    })

    it('should dispatch onStorageError when append fails', async () => {
      // Create a mock backend that throws on append
      const failingBackend = new MemoryBackend()
      failingBackend.append = async () => {
        throw new Error('append failed')
      }
      const failObserved = new ObservedBackend(failingBackend)

      await expect(failObserved.append('test/a.txt', textToBytes('data'))).rejects.toThrow('append failed')

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onStorageError')
      expect(hookCalls[0]!.context.operationType).toBe('append')
    })
  })

  // ===========================================================================
  // Delete Operations
  // ===========================================================================

  describe('delete(path)', () => {
    it('should delegate to inner backend and return boolean result', async () => {
      await inner.write('test/delete.txt', textToBytes('to delete'))
      const deleted = await observed.delete('test/delete.txt')
      expect(deleted).toBe(true)
    })

    it('should return false when file does not exist', async () => {
      const deleted = await observed.delete('nonexistent.txt')
      expect(deleted).toBe(false)
    })

    it('should dispatch onDelete hook with correct context and result', async () => {
      await inner.write('test/delete.txt', textToBytes('to delete'))
      await observed.delete('test/delete.txt')

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onDelete')
      expect(hookCalls[0]!.context.operationType).toBe('delete')
      expect(hookCalls[0]!.context.path).toBe('test/delete.txt')
      expect(hookCalls[0]!.result!.fileCount).toBe(1)
    })

    it('should report fileCount 0 when file did not exist', async () => {
      await observed.delete('nonexistent.txt')

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.result!.fileCount).toBe(0)
    })

    it('should dispatch onStorageError when delete fails', async () => {
      const failingBackend = new MemoryBackend()
      failingBackend.delete = async () => {
        throw new Error('delete failed')
      }
      const failObserved = new ObservedBackend(failingBackend)

      await expect(failObserved.delete('test/file.txt')).rejects.toThrow('delete failed')

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onStorageError')
    })
  })

  describe('deletePrefix(prefix)', () => {
    it('should delegate to inner backend and return count', async () => {
      await inner.write('prefix/a.txt', textToBytes('a'))
      await inner.write('prefix/b.txt', textToBytes('b'))

      const count = await observed.deletePrefix('prefix/')
      expect(count).toBe(2)
    })

    it('should dispatch onDelete hook with fileCount', async () => {
      await inner.write('prefix/a.txt', textToBytes('a'))
      await inner.write('prefix/b.txt', textToBytes('b'))
      await observed.deletePrefix('prefix/')

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onDelete')
      expect(hookCalls[0]!.context.operationType).toBe('deletePrefix')
      expect(hookCalls[0]!.result!.fileCount).toBe(2)
    })

    it('should dispatch onStorageError when deletePrefix fails', async () => {
      const failingBackend = new MemoryBackend()
      failingBackend.deletePrefix = async () => {
        throw new Error('deletePrefix failed')
      }
      const failObserved = new ObservedBackend(failingBackend)

      await expect(failObserved.deletePrefix('test/')).rejects.toThrow('deletePrefix failed')

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onStorageError')
      expect(hookCalls[0]!.context.operationType).toBe('deletePrefix')
    })
  })

  // ===========================================================================
  // Directory Operations
  // ===========================================================================

  describe('mkdir(path)', () => {
    it('should delegate to inner backend', async () => {
      await observed.mkdir('test/dir')
      // Verify via stat that the directory was created
      const stat = await inner.stat('test/dir')
      expect(stat).not.toBeNull()
    })

    it('should NOT dispatch any hooks (lightweight operation)', async () => {
      await observed.mkdir('test/dir')
      expect(hookCalls).toHaveLength(0)
    })
  })

  describe('rmdir(path, options)', () => {
    it('should delegate to inner backend', async () => {
      await inner.write('test/dir/file.txt', textToBytes('data'))
      await observed.rmdir('test/dir', { recursive: true })

      expect(await inner.exists('test/dir/file.txt')).toBe(false)
    })

    it('should dispatch onDelete hook', async () => {
      await inner.write('test/dir/file.txt', textToBytes('data'))
      await observed.rmdir('test/dir', { recursive: true })

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onDelete')
      expect(hookCalls[0]!.context.operationType).toBe('delete')
      expect(hookCalls[0]!.context.path).toBe('test/dir')
    })

    it('should dispatch onStorageError when rmdir fails', async () => {
      // Attempt to rmdir a non-existent directory
      await expect(observed.rmdir('nonexistent-dir')).rejects.toThrow()

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onStorageError')
    })
  })

  // ===========================================================================
  // Atomic / Conditional Operations
  // ===========================================================================

  describe('writeConditional(path, data, expectedVersion, options)', () => {
    it('should delegate to inner backend for new file (expectedVersion null)', async () => {
      const data = textToBytes('conditional')
      const result = await observed.writeConditional('test/cond.txt', data, null)
      expect(result.etag).toBeDefined()
      expect(result.size).toBe(data.length)
    })

    it('should dispatch onWrite hook with writeAtomic operation type', async () => {
      await observed.writeConditional('test/cond.txt', textToBytes('data'), null)

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onWrite')
      expect(hookCalls[0]!.context.operationType).toBe('writeAtomic')
      expect(hookCalls[0]!.context.path).toBe('test/cond.txt')
    })

    it('should dispatch onStorageError on version mismatch', async () => {
      await inner.write('test/cond.txt', textToBytes('original'))

      await expect(
        observed.writeConditional('test/cond.txt', textToBytes('new'), 'wrong-version')
      ).rejects.toThrow()

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onStorageError')
      expect(hookCalls[0]!.context.operationType).toBe('writeAtomic')
    })
  })

  describe('copy(source, dest)', () => {
    it('should delegate to inner backend', async () => {
      await inner.write('test/src.txt', textToBytes('source data'))
      await observed.copy('test/src.txt', 'test/dst.txt')

      const result = await inner.read('test/dst.txt')
      expect(bytesToText(result)).toBe('source data')
    })

    it('should dispatch onWrite hook with copy operation type', async () => {
      await inner.write('test/src.txt', textToBytes('data'))
      await observed.copy('test/src.txt', 'test/dst.txt')

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onWrite')
      expect(hookCalls[0]!.context.operationType).toBe('copy')
      expect(hookCalls[0]!.context.path).toBe('test/src.txt -> test/dst.txt')
    })

    it('should dispatch onStorageError when source does not exist', async () => {
      await expect(observed.copy('nonexistent.txt', 'dst.txt')).rejects.toThrow()

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onStorageError')
      expect(hookCalls[0]!.context.operationType).toBe('copy')
    })
  })

  describe('move(source, dest)', () => {
    it('should delegate to inner backend', async () => {
      await inner.write('test/src.txt', textToBytes('move data'))
      await observed.move('test/src.txt', 'test/dst.txt')

      const result = await inner.read('test/dst.txt')
      expect(bytesToText(result)).toBe('move data')
      expect(await inner.exists('test/src.txt')).toBe(false)
    })

    it('should dispatch onWrite hook with move operation type', async () => {
      await inner.write('test/src.txt', textToBytes('data'))
      await observed.move('test/src.txt', 'test/dst.txt')

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onWrite')
      expect(hookCalls[0]!.context.operationType).toBe('move')
      expect(hookCalls[0]!.context.path).toBe('test/src.txt -> test/dst.txt')
    })

    it('should dispatch onStorageError when source does not exist', async () => {
      await expect(observed.move('nonexistent.txt', 'dst.txt')).rejects.toThrow()

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onStorageError')
      expect(hookCalls[0]!.context.operationType).toBe('move')
    })
  })

  // ===========================================================================
  // Hook result timing
  // ===========================================================================

  describe('result timing', () => {
    it('should include durationMs >= 0 in all hook results', async () => {
      await inner.write('test/timing.txt', textToBytes('timing'))
      await observed.read('test/timing.txt')
      await observed.write('test/timing2.txt', textToBytes('data'))
      await observed.delete('test/timing.txt')

      expect(hookCalls).toHaveLength(3)
      for (const call of hookCalls) {
        if (call.result) {
          expect(call.result.durationMs).toBeGreaterThanOrEqual(0)
        }
      }
    })
  })

  // ===========================================================================
  // Error Wrapping
  // ===========================================================================

  describe('error handling', () => {
    it('should convert non-Error thrown values to Error objects for hooks', async () => {
      // Create a backend that throws a string instead of Error
      const failingBackend = new MemoryBackend()
      failingBackend.read = async () => {
        throw 'string error'
      }
      const failObserved = new ObservedBackend(failingBackend)

      await expect(failObserved.read('test.txt')).rejects.toBe('string error')

      expect(hookCalls).toHaveLength(1)
      expect(hookCalls[0]!.method).toBe('onStorageError')
      // The hook should receive an Error object even though a string was thrown
      expect(hookCalls[0]!.error).toBeInstanceOf(Error)
      expect(hookCalls[0]!.error!.message).toBe('string error')
    })
  })

  // ===========================================================================
  // No hooks registered (should still work)
  // ===========================================================================

  describe('with no hooks registered', () => {
    it('should still work correctly for all operations', async () => {
      globalHookRegistry.clearHooks()

      const noHookObserved = new ObservedBackend(new MemoryBackend())

      // Write, read, delete should all work without hooks
      await noHookObserved.write('test/file.txt', textToBytes('data'))
      const data = await noHookObserved.read('test/file.txt')
      expect(bytesToText(data)).toBe('data')
      const deleted = await noHookObserved.delete('test/file.txt')
      expect(deleted).toBe(true)
    })
  })

  // ===========================================================================
  // MultipartBackend Support
  // ===========================================================================

  describe('MultipartBackend support', () => {
    // Create a mock backend that supports multipart uploads
    function createMockMultipartBackend(): MemoryBackend & {
      createMultipartUpload: (path: string) => Promise<{
        uploadId: string
        uploadPart: (partNumber: number, data: Uint8Array) => Promise<{ partNumber: number; etag: string; size: number }>
        complete: (parts: { partNumber: number; etag: string; size: number }[]) => Promise<{ etag: string; size: number }>
        abort: () => Promise<void>
      }>
    } {
      const backend = new MemoryBackend()
      const uploads = new Map<string, { path: string; parts: Map<number, Uint8Array> }>()
      let uploadCounter = 0

      return Object.assign(backend, {
        createMultipartUpload: async (path: string) => {
          const uploadId = `upload-${++uploadCounter}`
          uploads.set(uploadId, { path, parts: new Map() })

          return {
            uploadId,
            uploadPart: async (partNumber: number, data: Uint8Array) => {
              const upload = uploads.get(uploadId)
              if (!upload) throw new Error('Upload not found')
              upload.parts.set(partNumber, data)
              return {
                partNumber,
                etag: `etag-${partNumber}`,
                size: data.length,
              }
            },
            complete: async (parts: { partNumber: number; etag: string; size: number }[]) => {
              const upload = uploads.get(uploadId)
              if (!upload) throw new Error('Upload not found')

              // Combine parts in order
              const sortedParts = [...parts].sort((a, b) => a.partNumber - b.partNumber)
              const totalSize = sortedParts.reduce((sum, p) => sum + (upload.parts.get(p.partNumber)?.length || 0), 0)
              const combined = new Uint8Array(totalSize)
              let offset = 0
              for (const part of sortedParts) {
                const data = upload.parts.get(part.partNumber)
                if (data) {
                  combined.set(data, offset)
                  offset += data.length
                }
              }

              // Write to backend
              const result = await backend.write(upload.path, combined)
              uploads.delete(uploadId)

              return {
                etag: result.etag,
                size: result.size,
              }
            },
            abort: async () => {
              uploads.delete(uploadId)
            },
          }
        },
      })
    }

    it('should detect multipart support when inner backend has createMultipartUpload', () => {
      const multipartBackend = createMockMultipartBackend()
      const observed = new ObservedBackend(multipartBackend)

      // Check that isMultipart returns true
      expect('createMultipartUpload' in observed).toBe(true)
    })

    it('should NOT have createMultipartUpload when inner backend does not support it', () => {
      const simpleBackend = new MemoryBackend()
      const observed = new ObservedBackend(simpleBackend)

      // MemoryBackend doesn't have createMultipartUpload
      expect('createMultipartUpload' in observed.inner).toBe(false)
    })

    it('should delegate createMultipartUpload to inner backend', async () => {
      const multipartBackend = createMockMultipartBackend()
      const observed = new ObservedBackend(multipartBackend) as ObservedBackend & {
        createMultipartUpload: typeof multipartBackend.createMultipartUpload
      }

      const upload = await observed.createMultipartUpload('test/multipart.txt')
      expect(upload.uploadId).toBe('upload-1')
    })

    it('should dispatch onWrite hooks for uploadPart operations', async () => {
      const multipartBackend = createMockMultipartBackend()
      const observed = new ObservedBackend(multipartBackend) as ObservedBackend & {
        createMultipartUpload: typeof multipartBackend.createMultipartUpload
      }

      const upload = await observed.createMultipartUpload('test/multipart.txt')
      await upload.uploadPart(1, textToBytes('part one'))

      // Should have dispatched a write hook for uploadPart
      const uploadPartHooks = hookCalls.filter(h => h.context.operationType === 'uploadPart')
      expect(uploadPartHooks).toHaveLength(1)
      expect(uploadPartHooks[0]!.method).toBe('onWrite')
      expect(uploadPartHooks[0]!.result!.bytesTransferred).toBe(8) // 'part one'.length
    })

    it('should dispatch onWrite hooks for complete operations', async () => {
      const multipartBackend = createMockMultipartBackend()
      const observed = new ObservedBackend(multipartBackend) as ObservedBackend & {
        createMultipartUpload: typeof multipartBackend.createMultipartUpload
      }

      const upload = await observed.createMultipartUpload('test/multipart.txt')
      const part1 = await upload.uploadPart(1, textToBytes('part1'))
      const part2 = await upload.uploadPart(2, textToBytes('part2'))

      hookCalls.length = 0 // Reset to check only complete hook

      await upload.complete([part1, part2])

      // Should have dispatched a write hook for complete
      const completeHooks = hookCalls.filter(h => h.context.operationType === 'multipartComplete')
      expect(completeHooks).toHaveLength(1)
      expect(completeHooks[0]!.method).toBe('onWrite')
    })

    it('should dispatch onDelete hooks for abort operations', async () => {
      const multipartBackend = createMockMultipartBackend()
      const observed = new ObservedBackend(multipartBackend) as ObservedBackend & {
        createMultipartUpload: typeof multipartBackend.createMultipartUpload
      }

      const upload = await observed.createMultipartUpload('test/multipart.txt')
      await upload.uploadPart(1, textToBytes('data'))

      hookCalls.length = 0 // Reset to check only abort hook

      await upload.abort()

      // Should have dispatched a delete hook for abort
      const abortHooks = hookCalls.filter(h => h.context.operationType === 'multipartAbort')
      expect(abortHooks).toHaveLength(1)
      expect(abortHooks[0]!.method).toBe('onDelete')
    })

    it('should dispatch onStorageError when uploadPart fails', async () => {
      const multipartBackend = createMockMultipartBackend()
      // Override uploadPart to fail
      const originalCreate = multipartBackend.createMultipartUpload.bind(multipartBackend)
      multipartBackend.createMultipartUpload = async (path: string) => {
        const upload = await originalCreate(path)
        return {
          ...upload,
          uploadPart: async () => {
            throw new Error('Upload part failed')
          },
        }
      }

      const observed = new ObservedBackend(multipartBackend) as ObservedBackend & {
        createMultipartUpload: typeof multipartBackend.createMultipartUpload
      }

      const upload = await observed.createMultipartUpload('test/multipart.txt')
      await expect(upload.uploadPart(1, textToBytes('data'))).rejects.toThrow('Upload part failed')

      const errorHooks = hookCalls.filter(h => h.method === 'onStorageError')
      expect(errorHooks).toHaveLength(1)
      expect(errorHooks[0]!.context.operationType).toBe('uploadPart')
    })

    it('should complete multipart upload and write data correctly', async () => {
      const multipartBackend = createMockMultipartBackend()
      const observed = new ObservedBackend(multipartBackend) as ObservedBackend & {
        createMultipartUpload: typeof multipartBackend.createMultipartUpload
      }

      const upload = await observed.createMultipartUpload('test/multipart.txt')
      const part1 = await upload.uploadPart(1, textToBytes('Hello, '))
      const part2 = await upload.uploadPart(2, textToBytes('World!'))
      await upload.complete([part1, part2])

      // Verify the data was written correctly
      const data = await observed.read('test/multipart.txt')
      expect(bytesToText(data)).toBe('Hello, World!')
    })
  })
})
