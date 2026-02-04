/**
 * E2E Tests: Studio Database Lifecycle
 *
 * Tests the database lifecycle with REAL storage backends (no mocks):
 * - Storage read/write operations
 * - File persistence across operations
 * - Directory management
 * - Data integrity verification
 *
 * These tests verify that Studio can work with real storage backends.
 * NOTE: Tests use storage backends directly without full ParqueDB to keep tests fast.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  createTestBackend,
  cleanupBackend,
  writeCorruptedParquetFile,
  type BackendType,
} from './setup'
import type { StorageBackend } from '../../../src/types/storage'
import { NotFoundError, AlreadyExistsError, ETagMismatchError } from '../../../src/storage/errors'

describe('E2E: Studio Database Lifecycle', () => {
  // Test with both backend types
  const backends: BackendType[] = ['memory', 'fs']

  for (const backendType of backends) {
    describe(`with ${backendType} backend`, () => {
      let storage: StorageBackend

      beforeEach(async () => {
        storage = await createTestBackend(backendType)
      })

      afterEach(async () => {
        await cleanupBackend(storage)
      })

      // =========================================================================
      // Storage Initialization
      // =========================================================================

      describe('Storage Initialization', () => {
        it('creates a storage backend of the correct type', () => {
          expect(storage.type).toBe(backendType === 'fs' ? 'fs' : 'memory')
        })

        it('starts with empty storage', async () => {
          const result = await storage.list('', { delimiter: '/' })
          expect(result.files.length).toBe(0)
        })
      })

      // =========================================================================
      // Basic File Operations
      // =========================================================================

      describe('Basic File Operations', () => {
        it('writes and reads a file', async () => {
          const content = new TextEncoder().encode('Hello, World!')
          await storage.write('test.txt', content)

          const result = await storage.read('test.txt')
          expect(new TextDecoder().decode(result)).toBe('Hello, World!')
        })

        it('writes and reads binary data', async () => {
          const binary = new Uint8Array([0x00, 0x01, 0x02, 0xFF, 0xFE])
          await storage.write('binary.dat', binary)

          const result = await storage.read('binary.dat')
          expect(result).toEqual(binary)
        })

        it('checks file existence', async () => {
          expect(await storage.exists('missing.txt')).toBe(false)

          await storage.write('exists.txt', new Uint8Array([1, 2, 3]))
          expect(await storage.exists('exists.txt')).toBe(true)
        })

        it('gets file metadata', async () => {
          const content = new TextEncoder().encode('metadata test')
          await storage.write('metadata.txt', content)

          const stat = await storage.stat('metadata.txt')
          expect(stat).not.toBeNull()
          expect(stat!.size).toBe(content.length)
          expect(stat!.isDirectory).toBe(false)
          expect(stat!.mtime).toBeInstanceOf(Date)
        })

        it('returns null for missing file stat', async () => {
          const stat = await storage.stat('nonexistent.txt')
          expect(stat).toBeNull()
        })

        it('throws NotFoundError for missing file read', async () => {
          await expect(storage.read('nonexistent.txt')).rejects.toThrow(NotFoundError)
        })
      })

      // =========================================================================
      // Directory Operations
      // =========================================================================

      describe('Directory Operations', () => {
        it('writes files to nested directories', async () => {
          const content = new Uint8Array([1, 2, 3])
          await storage.write('a/b/c/deep.txt', content)

          expect(await storage.exists('a/b/c/deep.txt')).toBe(true)

          const result = await storage.read('a/b/c/deep.txt')
          expect(result).toEqual(content)
        })

        it('lists files with prefix', async () => {
          await storage.write('dir/file1.txt', new Uint8Array([1]))
          await storage.write('dir/file2.txt', new Uint8Array([2]))
          await storage.write('other/file3.txt', new Uint8Array([3]))

          const result = await storage.list('dir/')
          expect(result.files).toHaveLength(2)
          expect(result.files.sort()).toEqual(['dir/file1.txt', 'dir/file2.txt'].sort())
        })

        it('lists files with delimiter to get directories', async () => {
          await storage.write('prefix/subdir1/file1.txt', new Uint8Array([1]))
          await storage.write('prefix/subdir2/file2.txt', new Uint8Array([2]))
          await storage.write('prefix/root.txt', new Uint8Array([3]))

          const result = await storage.list('prefix/', { delimiter: '/' })

          // Should have root file directly
          expect(result.files).toContain('prefix/root.txt')

          // Should have subdirectory prefixes
          expect(result.prefixes).toBeDefined()
          expect(result.prefixes?.length).toBe(2)
        })

        it('creates directories', async () => {
          await storage.mkdir('my-dir/nested')

          // Directory should exist (even if empty)
          const stat = await storage.stat('my-dir/nested')
          expect(stat?.isDirectory).toBe(true)
        })

        it('deletes files', async () => {
          await storage.write('deleteme.txt', new Uint8Array([1, 2, 3]))
          expect(await storage.exists('deleteme.txt')).toBe(true)

          const deleted = await storage.delete('deleteme.txt')
          expect(deleted).toBe(true)
          expect(await storage.exists('deleteme.txt')).toBe(false)
        })

        it('returns false when deleting non-existent file', async () => {
          const deleted = await storage.delete('nonexistent.txt')
          expect(deleted).toBe(false)
        })

        it('deletes files by prefix', async () => {
          await storage.write('cleanup/a.txt', new Uint8Array([1]))
          await storage.write('cleanup/b.txt', new Uint8Array([2]))
          await storage.write('cleanup/sub/c.txt', new Uint8Array([3]))
          await storage.write('keep/d.txt', new Uint8Array([4]))

          const count = await storage.deletePrefix('cleanup/')
          expect(count).toBe(3)

          expect(await storage.exists('cleanup/a.txt')).toBe(false)
          expect(await storage.exists('cleanup/b.txt')).toBe(false)
          expect(await storage.exists('cleanup/sub/c.txt')).toBe(false)
          expect(await storage.exists('keep/d.txt')).toBe(true)
        })
      })

      // =========================================================================
      // Conditional Writes
      // =========================================================================

      describe('Conditional Writes', () => {
        it('prevents overwrite with ifNoneMatch', async () => {
          await storage.write('unique.txt', new Uint8Array([1]))

          await expect(
            storage.write('unique.txt', new Uint8Array([2]), { ifNoneMatch: '*' })
          ).rejects.toThrow(AlreadyExistsError)
        })

        it('allows write with ifNoneMatch when file does not exist', async () => {
          await storage.write('newfile.txt', new Uint8Array([1]), { ifNoneMatch: '*' })

          expect(await storage.exists('newfile.txt')).toBe(true)
        })

        it('succeeds conditional write with matching etag', async () => {
          const writeResult = await storage.write('versioned.txt', new Uint8Array([1]))

          await storage.write('versioned.txt', new Uint8Array([2]), {
            ifMatch: writeResult.etag,
          })

          const content = await storage.read('versioned.txt')
          expect(content).toEqual(new Uint8Array([2]))
        })

        it('fails conditional write with mismatched etag', async () => {
          await storage.write('versioned.txt', new Uint8Array([1]))

          await expect(
            storage.write('versioned.txt', new Uint8Array([2]), { ifMatch: 'wrong-etag' })
          ).rejects.toThrow(ETagMismatchError)
        })
      })

      // =========================================================================
      // Range Reads
      // =========================================================================

      describe('Range Reads', () => {
        it('reads byte range from file', async () => {
          const content = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
          await storage.write('ranged.bin', content)

          const range = await storage.readRange('ranged.bin', 2, 5)
          expect(range).toEqual(new Uint8Array([2, 3, 4]))
        })

        it('handles range at end of file', async () => {
          const content = new Uint8Array([0, 1, 2, 3, 4])
          await storage.write('end-range.bin', content)

          const range = await storage.readRange('end-range.bin', 3, 10)
          expect(range).toEqual(new Uint8Array([3, 4]))
        })

        it('returns empty for range past end of file', async () => {
          const content = new Uint8Array([0, 1, 2])
          await storage.write('past-end.bin', content)

          const range = await storage.readRange('past-end.bin', 10, 20)
          expect(range.length).toBe(0)
        })

        it('throws NotFoundError for range read on missing file', async () => {
          await expect(storage.readRange('missing.bin', 0, 10)).rejects.toThrow(NotFoundError)
        })
      })

      // =========================================================================
      // Append Operations
      // =========================================================================

      describe('Append Operations', () => {
        it('appends to existing file', async () => {
          await storage.write('append.txt', new TextEncoder().encode('Hello, '))
          await storage.append('append.txt', new TextEncoder().encode('World!'))

          const content = await storage.read('append.txt')
          expect(new TextDecoder().decode(content)).toBe('Hello, World!')
        })

        it('creates file when appending to non-existent file', async () => {
          await storage.append('new-append.txt', new TextEncoder().encode('Created'))

          const content = await storage.read('new-append.txt')
          expect(new TextDecoder().decode(content)).toBe('Created')
        })

        it('handles multiple appends', async () => {
          await storage.append('multi.txt', new TextEncoder().encode('A'))
          await storage.append('multi.txt', new TextEncoder().encode('B'))
          await storage.append('multi.txt', new TextEncoder().encode('C'))

          const content = await storage.read('multi.txt')
          expect(new TextDecoder().decode(content)).toBe('ABC')
        })
      })

      // =========================================================================
      // Copy and Move Operations
      // =========================================================================

      describe('Copy and Move Operations', () => {
        it('copies file', async () => {
          await storage.write('original.txt', new TextEncoder().encode('Copy me'))

          await storage.copy('original.txt', 'copied.txt')

          expect(await storage.exists('original.txt')).toBe(true)
          expect(await storage.exists('copied.txt')).toBe(true)

          const copiedContent = await storage.read('copied.txt')
          expect(new TextDecoder().decode(copiedContent)).toBe('Copy me')
        })

        it('throws when copying non-existent file', async () => {
          await expect(storage.copy('missing.txt', 'dest.txt')).rejects.toThrow(NotFoundError)
        })

        it('moves file', async () => {
          await storage.write('moveme.txt', new TextEncoder().encode('Move me'))

          await storage.move('moveme.txt', 'moved.txt')

          expect(await storage.exists('moveme.txt')).toBe(false)
          expect(await storage.exists('moved.txt')).toBe(true)

          const movedContent = await storage.read('moved.txt')
          expect(new TextDecoder().decode(movedContent)).toBe('Move me')
        })

        it('throws when moving non-existent file', async () => {
          await expect(storage.move('missing.txt', 'dest.txt')).rejects.toThrow(NotFoundError)
        })
      })

      // =========================================================================
      // Data Integrity
      // =========================================================================

      describe('Data Integrity', () => {
        it('preserves UTF-8 content', async () => {
          const content = 'Hello \u4e16\u754c! \u00e9\u00e0\u00fc \ud83d\ude00'
          await storage.write('unicode.txt', new TextEncoder().encode(content))

          const result = await storage.read('unicode.txt')
          expect(new TextDecoder().decode(result)).toBe(content)
        })

        it('preserves binary data with all byte values', async () => {
          const allBytes = new Uint8Array(256)
          for (let i = 0; i < 256; i++) {
            allBytes[i] = i
          }
          await storage.write('allbytes.bin', allBytes)

          const result = await storage.read('allbytes.bin')
          expect(result).toEqual(allBytes)
        })

        it('handles large files', async () => {
          const size = 1024 * 1024 // 1MB
          const largeData = new Uint8Array(size)
          for (let i = 0; i < size; i++) {
            largeData[i] = i % 256
          }
          await storage.write('large.bin', largeData)

          const result = await storage.read('large.bin')
          expect(result.length).toBe(size)
          expect(result[0]).toBe(0)
          expect(result[255]).toBe(255)
          expect(result[256]).toBe(0)
        })

        it('handles empty files', async () => {
          await storage.write('empty.txt', new Uint8Array(0))

          const result = await storage.read('empty.txt')
          expect(result.length).toBe(0)
        })
      })

      // =========================================================================
      // Pagination
      // =========================================================================

      describe('Pagination', () => {
        beforeEach(async () => {
          // Create 10 files
          for (let i = 0; i < 10; i++) {
            await storage.write(`pages/file${i.toString().padStart(2, '0')}.txt`, new Uint8Array([i]))
          }
        })

        it('supports limit in list', async () => {
          const result = await storage.list('pages/', { limit: 3 })

          expect(result.files).toHaveLength(3)
          expect(result.hasMore).toBe(true)
          expect(result.cursor).toBeDefined()
        })

        it('supports cursor for pagination', async () => {
          const page1 = await storage.list('pages/', { limit: 3 })
          const page2 = await storage.list('pages/', { limit: 3, cursor: page1.cursor })

          expect(page2.files).toHaveLength(3)
          // Pages should not overlap
          for (const file of page2.files) {
            expect(page1.files).not.toContain(file)
          }
        })

        it('indicates when no more pages', async () => {
          const result = await storage.list('pages/', { limit: 20 })

          expect(result.files).toHaveLength(10)
          expect(result.hasMore).toBe(false)
          expect(result.cursor).toBeUndefined()
        })
      })
    })
  }
})
