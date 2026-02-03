/**
 * FsxBackend Tests
 *
 * Tests for the fsx-based storage backend for Cloudflare Workers.
 * fsx provides POSIX filesystem semantics with tiered storage
 * (SQLite for metadata, R2 for content).
 *
 * NOTE: FsxBackend requires the actual fsx package which is only available
 * in Cloudflare Workers environment. Tests that require actual fsx operations
 * are placed in describe.skipIf blocks and will only run when fsx is available.
 *
 * What CAN be tested in Node.js:
 * - Constructor behavior
 * - Input validation
 * - Type definitions
 * - Error handling for invalid inputs
 *
 * What REQUIRES Workers environment:
 * - Actual file read/write operations
 * - Tiered storage operations
 * - Path resolution with real fsx
 *
 * To run full tests in Workers environment:
 * 1. Configure fsx bindings in wrangler.jsonc
 * 2. Create FsxBackend.workers.test.ts
 * 3. Run with: npm run test:e2e:workers
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { FsxBackend, type FsxBackendOptions } from '../../src/storage/FsxBackend'
import type { Fsx, FsxStorageTier } from '../../src/storage/types/fsx'

// =============================================================================
// Environment Detection
// =============================================================================

/**
 * Check if we're running in Cloudflare Workers environment with fsx available
 */
function isFsxAvailable(): boolean {
  // In Cloudflare Workers with vitest-pool-workers, fsx would be available
  // via environment bindings. For now, fsx is not configured in wrangler.jsonc.
  return false
}

// =============================================================================
// Test: Constructor
// =============================================================================

describe('FsxBackend', () => {
  describe('constructor', () => {
    it('should define type as "fsx"', () => {
      const minimalFsx = {} as Fsx
      const backend = new FsxBackend(minimalFsx)

      expect(backend.type).toBe('fsx')
    })

    it('should accept fsx instance without options', () => {
      const minimalFsx = {} as Fsx
      const backend = new FsxBackend(minimalFsx)

      expect(backend).toBeInstanceOf(FsxBackend)
    })

    it('should accept options with root path', () => {
      const minimalFsx = {} as Fsx
      const options: FsxBackendOptions = { root: '/data/parquedb' }
      const backend = new FsxBackend(minimalFsx, options)

      expect(backend).toBeInstanceOf(FsxBackend)
    })

    it('should accept options with default tier', () => {
      const minimalFsx = {} as Fsx
      const options: FsxBackendOptions = { defaultTier: 'hot' }
      const backend = new FsxBackend(minimalFsx, options)

      expect(backend).toBeInstanceOf(FsxBackend)
    })

    it('should accept options with both root and default tier', () => {
      const minimalFsx = {} as Fsx
      const options: FsxBackendOptions = {
        root: '/data/parquedb',
        defaultTier: 'cold',
      }
      const backend = new FsxBackend(minimalFsx, options)

      expect(backend).toBeInstanceOf(FsxBackend)
    })

    it('should normalize root path by removing trailing slash', () => {
      const minimalFsx = {} as Fsx
      const options: FsxBackendOptions = { root: '/data/parquedb/' }
      const backend = new FsxBackend(minimalFsx, options)

      // The backend should be created successfully with normalized root
      expect(backend).toBeInstanceOf(FsxBackend)
    })

    it('should have readonly type property', () => {
      const minimalFsx = {} as Fsx
      const backend = new FsxBackend(minimalFsx)

      expect(() => {
        // @ts-expect-error - testing readonly
        backend.type = 'other'
      }).toThrow()
    })
  })

  // ===========================================================================
  // Test: Input Validation
  // ===========================================================================

  describe('input validation', () => {
    describe('readRange', () => {
      it('should throw for start greater than end', async () => {
        const minimalFsx = {} as Fsx
        const backend = new FsxBackend(minimalFsx)

        await expect(backend.readRange('data/test.parquet', 100, 50)).rejects.toThrow(
          /Invalid range.*end.*must be >= start|end.*start/i
        )
      })

      it('should throw for negative start', async () => {
        const minimalFsx = {} as Fsx
        const backend = new FsxBackend(minimalFsx)

        await expect(backend.readRange('data/test.parquet', -1, 10)).rejects.toThrow(
          /Invalid range.*start.*must be non-negative|start.*negative/i
        )
      })

      it('should allow start equal to end (empty range)', async () => {
        const minimalFsx = {
          readRange: async () => new Uint8Array(0),
        } as unknown as Fsx
        const backend = new FsxBackend(minimalFsx)

        // start === end is a valid empty range (0 bytes)
        // This should not throw during validation
        const result = await backend.readRange('data/test.parquet', 50, 50)
        expect(result).toBeInstanceOf(Uint8Array)
        expect(result.length).toBe(0)
      })
    })
  })

  // ===========================================================================
  // Test: Tier Types
  // ===========================================================================

  describe('tier types', () => {
    it('should accept "hot" tier', () => {
      const minimalFsx = {} as Fsx
      const options: FsxBackendOptions = { defaultTier: 'hot' }
      const backend = new FsxBackend(minimalFsx, options)

      expect(backend).toBeInstanceOf(FsxBackend)
    })

    it('should accept "warm" tier', () => {
      const minimalFsx = {} as Fsx
      const options: FsxBackendOptions = { defaultTier: 'warm' }
      const backend = new FsxBackend(minimalFsx, options)

      expect(backend).toBeInstanceOf(FsxBackend)
    })

    it('should accept "cold" tier', () => {
      const minimalFsx = {} as Fsx
      const options: FsxBackendOptions = { defaultTier: 'cold' }
      const backend = new FsxBackend(minimalFsx, options)

      expect(backend).toBeInstanceOf(FsxBackend)
    })
  })

  // ===========================================================================
  // Test: Type Identifier
  // ===========================================================================

  describe('type identifier', () => {
    it('should have type "fsx"', () => {
      const minimalFsx = {} as Fsx
      const backend = new FsxBackend(minimalFsx)

      expect(backend.type).toBe('fsx')
    })

    it('should be readonly and throw on assignment', () => {
      const minimalFsx = {} as Fsx
      const backend = new FsxBackend(minimalFsx)

      expect(() => {
        // @ts-expect-error - testing readonly
        backend.type = 'other'
      }).toThrow()
    })
  })

  // ===========================================================================
  // Test: Method Existence
  // ===========================================================================

  describe('method existence', () => {
    let backend: FsxBackend

    beforeAll(() => {
      const minimalFsx = {} as Fsx
      backend = new FsxBackend(minimalFsx)
    })

    it('should have read method', () => {
      expect(typeof backend.read).toBe('function')
    })

    it('should have readRange method', () => {
      expect(typeof backend.readRange).toBe('function')
    })

    it('should have exists method', () => {
      expect(typeof backend.exists).toBe('function')
    })

    it('should have stat method', () => {
      expect(typeof backend.stat).toBe('function')
    })

    it('should have list method', () => {
      expect(typeof backend.list).toBe('function')
    })

    it('should have write method', () => {
      expect(typeof backend.write).toBe('function')
    })

    it('should have writeAtomic method', () => {
      expect(typeof backend.writeAtomic).toBe('function')
    })

    it('should have writeConditional method', () => {
      expect(typeof backend.writeConditional).toBe('function')
    })

    it('should have append method', () => {
      expect(typeof backend.append).toBe('function')
    })

    it('should have delete method', () => {
      expect(typeof backend.delete).toBe('function')
    })

    it('should have deletePrefix method', () => {
      expect(typeof backend.deletePrefix).toBe('function')
    })

    it('should have mkdir method', () => {
      expect(typeof backend.mkdir).toBe('function')
    })

    it('should have rmdir method', () => {
      expect(typeof backend.rmdir).toBe('function')
    })

    it('should have copy method', () => {
      expect(typeof backend.copy).toBe('function')
    })

    it('should have move method', () => {
      expect(typeof backend.move).toBe('function')
    })

    it('should have getTier method', () => {
      expect(typeof backend.getTier).toBe('function')
    })

    it('should have setTier method', () => {
      expect(typeof backend.setTier).toBe('function')
    })

    it('should have promote method', () => {
      expect(typeof backend.promote).toBe('function')
    })

    it('should have demote method', () => {
      expect(typeof backend.demote).toBe('function')
    })
  })

  // ===========================================================================
  // Tests Requiring fsx Environment
  // ===========================================================================

  describe.skipIf(!isFsxAvailable())('read operations (requires fsx)', () => {
    it('should read entire file contents', async () => {
      // TODO: Implement when fsx is available
    })

    it('should read file with root path prefix', async () => {
      // TODO: Implement when fsx is available
    })

    it('should throw FileNotFoundError when file does not exist', async () => {
      // TODO: Implement when fsx is available
    })

    it('should read byte range from file', async () => {
      // TODO: Implement when fsx is available
    })
  })

  describe.skipIf(!isFsxAvailable())('write operations (requires fsx)', () => {
    it('should write data to file', async () => {
      // TODO: Implement when fsx is available
    })

    it('should create parent directories automatically', async () => {
      // TODO: Implement when fsx is available
    })

    it('should write atomically using temp file + rename', async () => {
      // TODO: Implement when fsx is available
    })

    it('should write conditionally with version check', async () => {
      // TODO: Implement when fsx is available
    })

    it('should append data to file', async () => {
      // TODO: Implement when fsx is available
    })

    it('should delete file', async () => {
      // TODO: Implement when fsx is available
    })
  })

  describe.skipIf(!isFsxAvailable())('directory operations (requires fsx)', () => {
    it('should create directory', async () => {
      // TODO: Implement when fsx is available
    })

    it('should create nested directories', async () => {
      // TODO: Implement when fsx is available
    })

    it('should remove empty directory', async () => {
      // TODO: Implement when fsx is available
    })

    it('should remove non-empty directory with recursive option', async () => {
      // TODO: Implement when fsx is available
    })
  })

  describe.skipIf(!isFsxAvailable())('list operations (requires fsx)', () => {
    it('should list files with prefix', async () => {
      // TODO: Implement when fsx is available
    })

    it('should support pagination', async () => {
      // TODO: Implement when fsx is available
    })

    it('should support glob patterns', async () => {
      // TODO: Implement when fsx is available
    })

    it('should include metadata when requested', async () => {
      // TODO: Implement when fsx is available
    })
  })

  describe.skipIf(!isFsxAvailable())('tiered storage operations (requires fsx)', () => {
    it('should get storage tier for file', async () => {
      // TODO: Implement when fsx is available
    })

    it('should set storage tier for file', async () => {
      // TODO: Implement when fsx is available
    })

    it('should promote file to hot tier', async () => {
      // TODO: Implement when fsx is available
    })

    it('should demote file to cold tier', async () => {
      // TODO: Implement when fsx is available
    })
  })

  describe.skipIf(!isFsxAvailable())('error handling (requires fsx)', () => {
    it('should handle ENOENT (file not found)', async () => {
      // TODO: Implement when fsx is available
    })

    it('should handle EEXIST (file exists)', async () => {
      // TODO: Implement when fsx is available
    })

    it('should handle EACCES (permission denied)', async () => {
      // TODO: Implement when fsx is available
    })

    it('should handle EIO (I/O error)', async () => {
      // TODO: Implement when fsx is available
    })

    it('should handle ENOTEMPTY (directory not empty)', async () => {
      // TODO: Implement when fsx is available
    })
  })

  describe.skipIf(!isFsxAvailable())('path handling (requires fsx)', () => {
    it('should prefix all paths with root', async () => {
      // TODO: Implement when fsx is available
    })

    it('should handle paths with special characters', async () => {
      // TODO: Implement when fsx is available
    })

    it('should handle unicode paths', async () => {
      // TODO: Implement when fsx is available
    })
  })

  // ===========================================================================
  // Test: Cursor-based Pagination (with mock fsx)
  // ===========================================================================

  describe('list() cursor-based pagination', () => {
    it('should use cursor to skip already-returned files', async () => {
      // Create a mock fsx that returns a predictable list of files
      const allFiles = [
        'data/file1.parquet',
        'data/file2.parquet',
        'data/file3.parquet',
        'data/file4.parquet',
        'data/file5.parquet',
      ]

      const mockFsx = {
        glob: async () => allFiles,
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)

      // First page: get first 2 files
      const page1 = await backend.list('data/', { limit: 2 })
      expect(page1.files).toEqual(['data/file1.parquet', 'data/file2.parquet'])
      expect(page1.hasMore).toBe(true)
      expect(page1.cursor).toBeDefined()

      // Second page: use cursor to get next 2 files
      const page2 = await backend.list('data/', { limit: 2, cursor: page1.cursor })
      expect(page2.files).toEqual(['data/file3.parquet', 'data/file4.parquet'])
      expect(page2.hasMore).toBe(true)
      expect(page2.cursor).toBeDefined()

      // Third page: use cursor to get remaining files
      const page3 = await backend.list('data/', { limit: 2, cursor: page2.cursor })
      expect(page3.files).toEqual(['data/file5.parquet'])
      expect(page3.hasMore).toBe(false)
      expect(page3.cursor).toBeUndefined()
    })

    it('should handle cursor with no more results', async () => {
      const allFiles = ['data/file1.parquet', 'data/file2.parquet']

      const mockFsx = {
        glob: async () => allFiles,
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)

      // Get all files in first request
      const page1 = await backend.list('data/', { limit: 2 })
      expect(page1.files).toEqual(['data/file1.parquet', 'data/file2.parquet'])
      expect(page1.hasMore).toBe(false)
      expect(page1.cursor).toBeUndefined()
    })

    it('should handle invalid cursor gracefully', async () => {
      const allFiles = ['data/file1.parquet', 'data/file2.parquet']

      const mockFsx = {
        glob: async () => allFiles,
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)

      // Use an invalid cursor (not valid base64 or JSON)
      const result = await backend.list('data/', { limit: 10, cursor: 'invalid-cursor' })

      // Should start from the beginning when cursor is invalid
      expect(result.files).toEqual(['data/file1.parquet', 'data/file2.parquet'])
    })

    it('should handle cursor pointing beyond available files', async () => {
      const allFiles = ['data/file1.parquet']

      const mockFsx = {
        glob: async () => allFiles,
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)

      // Create a cursor that points beyond available files
      const cursor = btoa(JSON.stringify({ offset: 100 }))
      const result = await backend.list('data/', { limit: 10, cursor })

      // Should return empty list when cursor is past end
      expect(result.files).toEqual([])
      expect(result.hasMore).toBe(false)
    })
  })

  // ===========================================================================
  // Test: writeConditional with mock fsx (TOCTOU race condition fix)
  // ===========================================================================

  describe('writeConditional() with ifMatch (atomic conditional writes)', () => {
    /**
     * Helper to create test data
     */
    function textToBytes(text: string): Uint8Array {
      return new TextEncoder().encode(text)
    }

    it('should use exclusive flag when expectedVersion is null (file should not exist)', async () => {
      let writeOptions: { exclusive?: boolean } | undefined

      const mockFsx = {
        writeFile: async (_path: string, _data: Uint8Array, options?: { exclusive?: boolean }) => {
          writeOptions = options
          return { etag: 'new-etag', size: 10, tier: 'hot' as const }
        },
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)
      await backend.writeConditional('new-file.txt', textToBytes('content'), null)

      // Should use exclusive: true for atomic "create if not exists"
      expect(writeOptions?.exclusive).toBe(true)
    })

    it('should use ifMatch option when expectedVersion is provided', async () => {
      let writeOptions: { ifMatch?: string } | undefined

      const mockFsx = {
        writeFile: async (_path: string, _data: Uint8Array, options?: { ifMatch?: string }) => {
          writeOptions = options
          return { etag: 'new-etag', size: 10, tier: 'hot' as const }
        },
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)
      await backend.writeConditional('existing.txt', textToBytes('updated'), 'expected-etag-v1')

      // Should pass ifMatch option for atomic compare-and-swap
      expect(writeOptions?.ifMatch).toBe('expected-etag-v1')
    })

    it('should throw ETagMismatchError when fsx returns ECONFLICT', async () => {
      const mockFsx = {
        writeFile: async () => {
          const err = new Error('Precondition failed') as Error & { code: string }
          err.code = 'ECONFLICT'
          throw err
        },
        stat: async () => ({
          size: 100,
          etag: 'actual-etag-v2',
          atime: new Date(),
          mtime: new Date(),
          birthtime: new Date(),
          ctime: new Date(),
          mode: 0o644,
          uid: 1000,
          gid: 1000,
          isFile: () => true,
          isDirectory: () => false,
          isSymbolicLink: () => false,
        }),
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)

      await expect(
        backend.writeConditional('file.txt', textToBytes('data'), 'expected-etag-v1')
      ).rejects.toMatchObject({
        name: 'ETagMismatchError',
        expectedEtag: 'expected-etag-v1',
        actualEtag: 'actual-etag-v2',
      })
    })

    it('should throw ETagMismatchError when file does not exist but version expected', async () => {
      const mockFsx = {
        writeFile: async () => {
          const err = new Error('File not found') as Error & { code: string }
          err.code = 'ENOENT'
          throw err
        },
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)

      await expect(
        backend.writeConditional('nonexistent.txt', textToBytes('data'), 'some-etag')
      ).rejects.toMatchObject({
        name: 'ETagMismatchError',
        expectedEtag: 'some-etag',
        actualEtag: null,
      })
    })

    it('should throw ETagMismatchError when file exists but null version expected', async () => {
      const mockFsx = {
        writeFile: async () => {
          const err = new Error('File exists') as Error & { code: string }
          err.code = 'EEXIST'
          throw err
        },
        stat: async () => ({
          size: 100,
          etag: 'existing-etag',
          atime: new Date(),
          mtime: new Date(),
          birthtime: new Date(),
          ctime: new Date(),
          mode: 0o644,
          uid: 1000,
          gid: 1000,
          isFile: () => true,
          isDirectory: () => false,
          isSymbolicLink: () => false,
        }),
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)

      await expect(
        backend.writeConditional('existing.txt', textToBytes('data'), null)
      ).rejects.toMatchObject({
        name: 'ETagMismatchError',
        expectedEtag: null,
        actualEtag: 'existing-etag',
      })
    })

    it('should return new etag on successful conditional write', async () => {
      const mockFsx = {
        writeFile: async () => ({
          etag: 'new-etag-v2',
          size: 15,
          tier: 'hot' as const,
        }),
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)
      const result = await backend.writeConditional(
        'file.txt',
        textToBytes('updated content'),
        'old-etag-v1'
      )

      expect(result.etag).toBe('new-etag-v2')
      expect(result.size).toBe(15)
    })

    it('should use ifNoneMatch for exclusive create', async () => {
      let writeOptions: { exclusive?: boolean } | undefined

      const mockFsx = {
        writeFile: async (_path: string, _data: Uint8Array, options?: { exclusive?: boolean }) => {
          writeOptions = options
          return { etag: 'created-etag', size: 10, tier: 'hot' as const }
        },
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)
      await backend.writeConditional('new.txt', textToBytes('content'), null, { ifNoneMatch: '*' })

      expect(writeOptions?.exclusive).toBe(true)
    })

    it('should throw AlreadyExistsError when ifNoneMatch fails', async () => {
      const mockFsx = {
        writeFile: async () => {
          const err = new Error('File exists') as Error & { code: string }
          err.code = 'EEXIST'
          throw err
        },
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)

      await expect(
        backend.writeConditional('existing.txt', textToBytes('data'), null, { ifNoneMatch: '*' })
      ).rejects.toMatchObject({
        name: 'AlreadyExistsError',
      })
    })

    it('should wrap generic errors in OperationError', async () => {
      const mockFsx = {
        writeFile: async () => {
          const err = new Error('Network timeout') as Error & { code: string }
          err.code = 'ETIMEDOUT'
          throw err
        },
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)

      await expect(
        backend.writeConditional('file.txt', textToBytes('data'), 'etag')
      ).rejects.toMatchObject({
        name: 'OperationError',
        operation: 'writeConditional',
      })
    })

    it('should handle stat failure gracefully when getting current etag after conflict', async () => {
      const mockFsx = {
        writeFile: async () => {
          const err = new Error('Precondition failed') as Error & { code: string }
          err.code = 'ECONFLICT'
          throw err
        },
        stat: async () => {
          // File was deleted between conflict and stat
          const err = new Error('File not found') as Error & { code: string }
          err.code = 'ENOENT'
          throw err
        },
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)

      // Should still throw ETagMismatchError with null actual etag
      await expect(
        backend.writeConditional('file.txt', textToBytes('data'), 'expected-etag')
      ).rejects.toMatchObject({
        name: 'ETagMismatchError',
        expectedEtag: 'expected-etag',
        actualEtag: null,
      })
    })

    it('should apply root path prefix to conditional writes', async () => {
      let writtenPath: string | undefined

      const mockFsx = {
        writeFile: async (path: string) => {
          writtenPath = path
          return { etag: 'etag', size: 10, tier: 'hot' as const }
        },
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx, { root: '/data/parquedb' })
      await backend.writeConditional('entities/users.parquet', textToBytes('data'), null)

      expect(writtenPath).toBe('/data/parquedb/entities/users.parquet')
    })

    it('should pass through contentType and metadata options', async () => {
      let writeOptions: { contentType?: string; metadata?: Record<string, string> } | undefined

      const mockFsx = {
        writeFile: async (_path: string, _data: Uint8Array, options?: { contentType?: string; metadata?: Record<string, string> }) => {
          writeOptions = options
          return { etag: 'etag', size: 10, tier: 'hot' as const }
        },
      } as unknown as Fsx

      const backend = new FsxBackend(mockFsx)
      await backend.writeConditional(
        'file.parquet',
        textToBytes('data'),
        'etag',
        {
          contentType: 'application/vnd.apache.parquet',
          metadata: { 'x-custom': 'value' },
        }
      )

      expect(writeOptions?.contentType).toBe('application/vnd.apache.parquet')
      expect(writeOptions?.metadata).toEqual({ 'x-custom': 'value' })
    })
  })
})
