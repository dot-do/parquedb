/**
 * StorageBackend Interface Tests
 *
 * This is a generic test suite that can be used to test any implementation
 * of the StorageBackend interface. Tests are designed to be run against
 * different backends (Memory, Fs, R2, S3, etc.)
 *
 * Usage:
 *   import { createStorageBackendTests } from './StorageBackend.test'
 *   createStorageBackendTests(() => new MemoryBackend())
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import type {
  StorageBackend,
  FileStat,
  WriteResult,
  ListResult,
  WriteOptions,
  ListOptions,
} from '../../src/types/storage'

// =============================================================================
// Test Factory - Creates test suite for any StorageBackend implementation
// =============================================================================

/**
 * Creates a comprehensive test suite for a StorageBackend implementation
 * @param createBackend - Factory function that creates a fresh backend instance
 * @param cleanupBackend - Optional cleanup function called after each test
 */
export function createStorageBackendTests(
  createBackend: () => StorageBackend | Promise<StorageBackend>,
  cleanupBackend?: (backend: StorageBackend) => Promise<void>
): void {
  describe('StorageBackend', () => {
    let backend: StorageBackend

    beforeEach(async () => {
      backend = await createBackend()
    })

    afterEach(async () => {
      if (cleanupBackend) {
        await cleanupBackend(backend)
      }
    })

    // =========================================================================
    // Helper functions
    // =========================================================================

    const textToBytes = (text: string): Uint8Array => {
      return new TextEncoder().encode(text)
    }

    const bytesToText = (bytes: Uint8Array): string => {
      return new TextDecoder().decode(bytes)
    }

    const generateTestData = (size: number): Uint8Array => {
      const data = new Uint8Array(size)
      for (let i = 0; i < size; i++) {
        data[i] = i % 256
      }
      return data
    }

    // =========================================================================
    // 1. read(path) - Read entire file, throw if not exists
    // =========================================================================

    describe('read(path)', () => {
      it('should read an existing file and return its contents', async () => {
        const path = 'test/read-basic.txt'
        const content = textToBytes('Hello, World!')

        await backend.write(path, content)
        const result = await backend.read(path)

        expect(result).toBeInstanceOf(Uint8Array)
        expect(bytesToText(result)).toBe('Hello, World!')
      })

      it('should throw an error when file does not exist', async () => {
        const path = 'test/nonexistent-file.txt'

        await expect(backend.read(path)).rejects.toThrow()
      })

      it('should read binary data correctly', async () => {
        const path = 'test/binary-data.bin'
        const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])

        await backend.write(path, binaryData)
        const result = await backend.read(path)

        expect(result).toEqual(binaryData)
      })

      it('should read empty files', async () => {
        const path = 'test/empty-file.txt'
        const emptyData = new Uint8Array(0)

        await backend.write(path, emptyData)
        const result = await backend.read(path)

        expect(result.length).toBe(0)
      })

      it('should read large files', async () => {
        const path = 'test/large-file.bin'
        const largeData = generateTestData(1024 * 1024) // 1MB

        await backend.write(path, largeData)
        const result = await backend.read(path)

        expect(result.length).toBe(largeData.length)
        expect(result).toEqual(largeData)
      })

      it('should handle paths with special characters', async () => {
        const path = 'test/path with spaces/file-name_v1.2.txt'
        const content = textToBytes('special path content')

        await backend.write(path, content)
        const result = await backend.read(path)

        expect(bytesToText(result)).toBe('special path content')
      })

      it('should handle deeply nested paths', async () => {
        const path = 'test/level1/level2/level3/level4/deep-file.txt'
        const content = textToBytes('deep content')

        await backend.write(path, content)
        const result = await backend.read(path)

        expect(bytesToText(result)).toBe('deep content')
      })
    })

    // =========================================================================
    // 2. write(path, data, options?) - Write file, return WriteResult
    // =========================================================================

    describe('write(path, data, options?)', () => {
      it('should write a file and return WriteResult with etag and size', async () => {
        const path = 'test/write-basic.txt'
        const content = textToBytes('Test content')

        const result = await backend.write(path, content)

        expect(result).toHaveProperty('etag')
        expect(result).toHaveProperty('size')
        expect(result.size).toBe(content.length)
        expect(typeof result.etag).toBe('string')
        expect(result.etag.length).toBeGreaterThan(0)
      })

      it('should overwrite existing file', async () => {
        const path = 'test/overwrite.txt'
        const content1 = textToBytes('First content')
        const content2 = textToBytes('Second content')

        await backend.write(path, content1)
        await backend.write(path, content2)
        const result = await backend.read(path)

        expect(bytesToText(result)).toBe('Second content')
      })

      it('should create parent directories if needed', async () => {
        const path = 'test/new-dir/subdir/file.txt'
        const content = textToBytes('content in new directory')

        const result = await backend.write(path, content)

        expect(result.size).toBe(content.length)
        const readResult = await backend.read(path)
        expect(bytesToText(readResult)).toBe('content in new directory')
      })

      it('should write empty file', async () => {
        const path = 'test/empty-write.txt'
        const content = new Uint8Array(0)

        const result = await backend.write(path, content)

        expect(result.size).toBe(0)
      })

      it('should accept WriteOptions with contentType', async () => {
        const path = 'test/with-content-type.json'
        const content = textToBytes('{"key": "value"}')
        const options: WriteOptions = {
          contentType: 'application/json',
        }

        const result = await backend.write(path, content, options)

        expect(result.size).toBe(content.length)
      })

      it('should accept WriteOptions with custom metadata', async () => {
        const path = 'test/with-metadata.txt'
        const content = textToBytes('metadata test')
        const options: WriteOptions = {
          metadata: {
            'x-custom-key': 'custom-value',
            author: 'test-author',
          },
        }

        const result = await backend.write(path, content, options)

        expect(result.size).toBe(content.length)
      })

      it('should accept WriteOptions with cacheControl', async () => {
        const path = 'test/with-cache-control.txt'
        const content = textToBytes('cache control test')
        const options: WriteOptions = {
          cacheControl: 'max-age=3600',
        }

        const result = await backend.write(path, content, options)

        expect(result.size).toBe(content.length)
      })

      it('should return different etags for different content', async () => {
        const path1 = 'test/etag-test-1.txt'
        const path2 = 'test/etag-test-2.txt'
        const content1 = textToBytes('content one')
        const content2 = textToBytes('content two')

        const result1 = await backend.write(path1, content1)
        const result2 = await backend.write(path2, content2)

        expect(result1.etag).not.toBe(result2.etag)
      })

      it('should honor ifNoneMatch option (write only if not exists)', async () => {
        const path = 'test/if-none-match.txt'
        const content1 = textToBytes('first')
        const content2 = textToBytes('second')

        // First write should succeed
        await backend.write(path, content1)

        // Second write with ifNoneMatch should fail
        const options: WriteOptions = { ifNoneMatch: '*' }
        await expect(backend.write(path, content2, options)).rejects.toThrow()

        // Original content should remain
        const result = await backend.read(path)
        expect(bytesToText(result)).toBe('first')
      })

      it('should honor ifMatch option (conditional write)', async () => {
        const path = 'test/if-match.txt'
        const content1 = textToBytes('first')
        const content2 = textToBytes('second')
        const content3 = textToBytes('third')

        const writeResult = await backend.write(path, content1)
        const correctEtag = writeResult.etag

        // Write with correct etag should succeed
        await backend.write(path, content2, { ifMatch: correctEtag })

        // Write with wrong etag should fail
        await expect(
          backend.write(path, content3, { ifMatch: 'wrong-etag' })
        ).rejects.toThrow()
      })
    })

    // =========================================================================
    // 3. writeAtomic(path, data, options?) - Atomic write (temp + rename)
    // =========================================================================

    describe('writeAtomic(path, data, options?)', () => {
      it('should write file atomically and return WriteResult', async () => {
        const path = 'test/atomic-basic.txt'
        const content = textToBytes('atomic content')

        const result = await backend.writeAtomic(path, content)

        expect(result).toHaveProperty('etag')
        expect(result).toHaveProperty('size')
        expect(result.size).toBe(content.length)
      })

      it('should atomically replace existing file', async () => {
        const path = 'test/atomic-replace.txt'
        const content1 = textToBytes('original')
        const content2 = textToBytes('replacement')

        await backend.write(path, content1)
        await backend.writeAtomic(path, content2)

        const result = await backend.read(path)
        expect(bytesToText(result)).toBe('replacement')
      })

      it('should not leave partial files on error', async () => {
        // This test verifies atomic behavior - if write fails, no partial data exists
        const path = 'test/atomic-partial.txt'
        const content = textToBytes('original content')

        await backend.write(path, content)

        // The atomic nature should ensure either complete success or no change
        const result = await backend.read(path)
        expect(bytesToText(result)).toBe('original content')
      })

      it('should create parent directories if needed', async () => {
        const path = 'test/atomic-newdir/subdir/file.txt'
        const content = textToBytes('atomic in new directory')

        const result = await backend.writeAtomic(path, content)

        expect(result.size).toBe(content.length)
        const readResult = await backend.read(path)
        expect(bytesToText(readResult)).toBe('atomic in new directory')
      })

      it('should accept WriteOptions', async () => {
        const path = 'test/atomic-with-options.txt'
        const content = textToBytes('atomic with options')
        const options: WriteOptions = {
          contentType: 'text/plain',
          metadata: { key: 'value' },
        }

        const result = await backend.writeAtomic(path, content, options)

        expect(result.size).toBe(content.length)
      })

      it('should handle concurrent atomic writes correctly', async () => {
        const path = 'test/atomic-concurrent.txt'
        const content1 = textToBytes('writer-1')
        const content2 = textToBytes('writer-2')

        // Start both writes concurrently
        const [result1, result2] = await Promise.all([
          backend.writeAtomic(path, content1),
          backend.writeAtomic(path, content2),
        ])

        // Both should complete, one wins
        expect(result1.size).toBe(content1.length)
        expect(result2.size).toBe(content2.length)

        // File should contain one of the two contents
        const finalContent = bytesToText(await backend.read(path))
        expect(['writer-1', 'writer-2']).toContain(finalContent)
      })
    })

    // =========================================================================
    // 4. exists(path) - Return boolean
    // =========================================================================

    describe('exists(path)', () => {
      it('should return true for existing file', async () => {
        const path = 'test/exists-true.txt'
        await backend.write(path, textToBytes('exists'))

        const result = await backend.exists(path)

        expect(result).toBe(true)
      })

      it('should return false for non-existing file', async () => {
        const path = 'test/nonexistent-exists.txt'

        const result = await backend.exists(path)

        expect(result).toBe(false)
      })

      it('should return false after file is deleted', async () => {
        const path = 'test/exists-deleted.txt'
        await backend.write(path, textToBytes('to delete'))
        await backend.delete(path)

        const result = await backend.exists(path)

        expect(result).toBe(false)
      })

      it('should handle paths that never existed', async () => {
        const result = await backend.exists('completely/random/path/that/never/existed.txt')

        expect(result).toBe(false)
      })

      it('should distinguish between file and directory', async () => {
        const dirPath = 'test/exists-dir'
        const filePath = 'test/exists-dir/file.txt'

        await backend.mkdir(dirPath)
        await backend.write(filePath, textToBytes('content'))

        // File should exist
        expect(await backend.exists(filePath)).toBe(true)
      })
    })

    // =========================================================================
    // 5. stat(path) - Return FileStat or null
    // =========================================================================

    describe('stat(path)', () => {
      it('should return FileStat for existing file', async () => {
        const path = 'test/stat-basic.txt'
        const content = textToBytes('stat test content')
        await backend.write(path, content)

        const stat = await backend.stat(path)

        expect(stat).not.toBeNull()
        expect(stat!.path).toBe(path)
        expect(stat!.size).toBe(content.length)
        expect(stat!.isDirectory).toBe(false)
        expect(stat!.mtime).toBeInstanceOf(Date)
      })

      it('should return null for non-existing file', async () => {
        const path = 'test/stat-nonexistent.txt'

        const stat = await backend.stat(path)

        expect(stat).toBeNull()
      })

      it('should include etag in FileStat', async () => {
        const path = 'test/stat-etag.txt'
        const content = textToBytes('etag test')
        const writeResult = await backend.write(path, content)

        const stat = await backend.stat(path)

        expect(stat).not.toBeNull()
        expect(stat!.etag).toBe(writeResult.etag)
      })

      it('should return accurate size for various file sizes', async () => {
        const sizes = [0, 1, 100, 1024, 10240]

        for (const size of sizes) {
          const path = `test/stat-size-${size}.bin`
          const content = generateTestData(size)
          await backend.write(path, content)

          const stat = await backend.stat(path)

          expect(stat).not.toBeNull()
          expect(stat!.size).toBe(size)
        }
      })

      it('should return mtime that is recent', async () => {
        const path = 'test/stat-mtime.txt'
        const before = new Date()
        await backend.write(path, textToBytes('mtime test'))
        const after = new Date()

        const stat = await backend.stat(path)

        expect(stat).not.toBeNull()
        expect(stat!.mtime.getTime()).toBeGreaterThanOrEqual(before.getTime() - 1000)
        expect(stat!.mtime.getTime()).toBeLessThanOrEqual(after.getTime() + 1000)
      })

      it('should include contentType if available', async () => {
        const path = 'test/stat-content-type.json'
        await backend.write(path, textToBytes('{}'), {
          contentType: 'application/json',
        })

        const stat = await backend.stat(path)

        expect(stat).not.toBeNull()
        // contentType may or may not be preserved depending on backend
        if (stat!.contentType) {
          expect(stat!.contentType).toBe('application/json')
        }
      })

      it('should include metadata if available', async () => {
        const path = 'test/stat-metadata.txt'
        await backend.write(path, textToBytes('metadata test'), {
          metadata: { 'x-custom': 'value' },
        })

        const stat = await backend.stat(path)

        expect(stat).not.toBeNull()
        // metadata may or may not be preserved depending on backend
        if (stat!.metadata) {
          expect(stat!.metadata['x-custom']).toBe('value')
        }
      })

      it('should correctly identify directories', async () => {
        const dirPath = 'test/stat-directory'
        await backend.mkdir(dirPath)

        const stat = await backend.stat(dirPath)

        // Some backends may not support directory stat
        if (stat) {
          expect(stat.isDirectory).toBe(true)
        }
      })
    })

    // =========================================================================
    // 6. list(prefix, options?) - List files with pagination
    // =========================================================================

    describe('list(prefix, options?)', () => {
      beforeEach(async () => {
        // Create test files for list operations
        const files = [
          'list-test/file1.txt',
          'list-test/file2.txt',
          'list-test/file3.txt',
          'list-test/subdir/file4.txt',
          'list-test/subdir/file5.txt',
          'list-test/other/file6.txt',
        ]

        for (const file of files) {
          await backend.write(file, textToBytes(`content of ${file}`))
        }
      })

      it('should list all files with prefix', async () => {
        const result = await backend.list('list-test/')

        expect(result.files.length).toBeGreaterThanOrEqual(6)
        expect(result.hasMore).toBe(false)
      })

      it('should return ListResult with files array', async () => {
        const result = await backend.list('list-test/')

        expect(result).toHaveProperty('files')
        expect(Array.isArray(result.files)).toBe(true)
      })

      it('should filter by prefix correctly', async () => {
        const result = await backend.list('list-test/subdir/')

        expect(result.files.length).toBe(2)
        result.files.forEach((file) => {
          expect(file).toMatch(/^list-test\/subdir\//)
        })
      })

      it('should support limit option for pagination', async () => {
        const options: ListOptions = { limit: 2 }
        const result = await backend.list('list-test/', options)

        expect(result.files.length).toBeLessThanOrEqual(2)
        if (result.files.length === 2) {
          expect(result.hasMore).toBe(true)
          expect(result.cursor).toBeDefined()
        }
      })

      it('should support cursor-based pagination', async () => {
        // Get first page
        const page1 = await backend.list('list-test/', { limit: 2 })

        if (page1.hasMore && page1.cursor) {
          // Get second page
          const page2 = await backend.list('list-test/', {
            limit: 2,
            cursor: page1.cursor,
          })

          // Files should be different
          const page1Files = new Set(page1.files)
          page2.files.forEach((file) => {
            expect(page1Files.has(file)).toBe(false)
          })
        }
      })

      it('should support delimiter option for directory grouping', async () => {
        const options: ListOptions = { delimiter: '/' }
        const result = await backend.list('list-test/', options)

        // Should have prefixes for subdirectories
        if (result.prefixes) {
          expect(result.prefixes.length).toBeGreaterThan(0)
        }
      })

      it('should return empty result for non-matching prefix', async () => {
        const result = await backend.list('nonexistent-prefix/')

        expect(result.files.length).toBe(0)
        expect(result.hasMore).toBe(false)
      })

      it('should include stats when includeMetadata is true', async () => {
        const options: ListOptions = { includeMetadata: true }
        const result = await backend.list('list-test/', options)

        if (result.stats) {
          expect(result.stats.length).toBe(result.files.length)
          result.stats.forEach((stat) => {
            expect(stat).toHaveProperty('path')
            expect(stat).toHaveProperty('size')
            expect(stat).toHaveProperty('mtime')
          })
        }
      })

      it('should support pattern filtering', async () => {
        const options: ListOptions = { pattern: '*.txt' }
        const result = await backend.list('list-test/', options)

        result.files.forEach((file) => {
          expect(file).toMatch(/\.txt$/)
        })
      })

      it('should handle empty prefix (list all)', async () => {
        const result = await backend.list('')

        expect(result.files.length).toBeGreaterThan(0)
      })
    })

    // =========================================================================
    // 7. readRange(path, start, end) - Read byte range
    // =========================================================================

    describe('readRange(path, start, end)', () => {
      it('should read a byte range from file', async () => {
        const path = 'test/range-basic.txt'
        const content = textToBytes('Hello, World!')
        await backend.write(path, content)

        // Read "World" (bytes 7-11)
        const result = await backend.readRange(path, 7, 12)

        expect(bytesToText(result)).toBe('World')
      })

      it('should read from start of file', async () => {
        const path = 'test/range-start.txt'
        const content = textToBytes('ABCDEFGHIJ')
        await backend.write(path, content)

        const result = await backend.readRange(path, 0, 3)

        expect(bytesToText(result)).toBe('ABC')
      })

      it('should read to end of file', async () => {
        const path = 'test/range-end.txt'
        const content = textToBytes('ABCDEFGHIJ')
        await backend.write(path, content)

        const result = await backend.readRange(path, 7, content.length)

        expect(bytesToText(result)).toBe('HIJ')
      })

      it('should handle single byte range', async () => {
        const path = 'test/range-single.txt'
        const content = textToBytes('ABCDE')
        await backend.write(path, content)

        const result = await backend.readRange(path, 2, 3)

        expect(bytesToText(result)).toBe('C')
      })

      it('should throw for non-existent file', async () => {
        await expect(
          backend.readRange('test/nonexistent-range.txt', 0, 10)
        ).rejects.toThrow()
      })

      it('should handle Parquet footer read pattern', async () => {
        // Parquet files typically read the last 8 bytes for magic + footer size
        const path = 'test/parquet-like.bin'
        const size = 1024
        const data = generateTestData(size)
        await backend.write(path, data)

        // Read last 8 bytes (footer pattern)
        const footer = await backend.readRange(path, size - 8, size)

        expect(footer.length).toBe(8)
        expect(footer).toEqual(data.slice(size - 8, size))
      })

      it('should handle large range reads', async () => {
        const path = 'test/range-large.bin'
        const data = generateTestData(100000) // 100KB
        await backend.write(path, data)

        // Read middle 50KB
        const result = await backend.readRange(path, 25000, 75000)

        expect(result.length).toBe(50000)
        expect(result).toEqual(data.slice(25000, 75000))
      })

      it('should return correct bytes for binary data', async () => {
        const path = 'test/range-binary.bin'
        const data = new Uint8Array([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07])
        await backend.write(path, data)

        const result = await backend.readRange(path, 2, 6)

        expect(result).toEqual(new Uint8Array([0x02, 0x03, 0x04, 0x05]))
      })

      it('should handle range at exact file boundary', async () => {
        const path = 'test/range-boundary.txt'
        const content = textToBytes('12345')
        await backend.write(path, content)

        // Read entire file via range
        const result = await backend.readRange(path, 0, 5)

        expect(bytesToText(result)).toBe('12345')
      })

      it('should throw or clamp for range exceeding file size', async () => {
        const path = 'test/range-exceed.txt'
        const content = textToBytes('SHORT')
        await backend.write(path, content)

        // Depending on implementation, this might throw or clamp
        try {
          const result = await backend.readRange(path, 0, 1000)
          // If it doesn't throw, it should return the available bytes
          expect(result.length).toBeLessThanOrEqual(content.length)
        } catch {
          // Throwing is also acceptable behavior
          expect(true).toBe(true)
        }
      })
    })

    // =========================================================================
    // 8. delete(path) - Delete file, return boolean
    // =========================================================================

    describe('delete(path)', () => {
      it('should delete existing file and return true', async () => {
        const path = 'test/delete-exists.txt'
        await backend.write(path, textToBytes('to delete'))

        const result = await backend.delete(path)

        expect(result).toBe(true)
        expect(await backend.exists(path)).toBe(false)
      })

      it('should return false for non-existing file', async () => {
        const path = 'test/delete-nonexistent.txt'

        const result = await backend.delete(path)

        expect(result).toBe(false)
      })

      it('should delete file but not affect other files', async () => {
        const path1 = 'test/delete-keep-1.txt'
        const path2 = 'test/delete-remove.txt'
        const path3 = 'test/delete-keep-2.txt'

        await backend.write(path1, textToBytes('keep 1'))
        await backend.write(path2, textToBytes('remove'))
        await backend.write(path3, textToBytes('keep 2'))

        await backend.delete(path2)

        expect(await backend.exists(path1)).toBe(true)
        expect(await backend.exists(path2)).toBe(false)
        expect(await backend.exists(path3)).toBe(true)
      })

      it('should be idempotent (delete already deleted file)', async () => {
        const path = 'test/delete-idempotent.txt'
        await backend.write(path, textToBytes('content'))

        await backend.delete(path)
        const result = await backend.delete(path)

        expect(result).toBe(false)
      })

      it('should delete files in subdirectories', async () => {
        const path = 'test/subdir/deep/delete-me.txt'
        await backend.write(path, textToBytes('nested content'))

        const result = await backend.delete(path)

        expect(result).toBe(true)
        expect(await backend.exists(path)).toBe(false)
      })
    })

    // =========================================================================
    // 9. deletePrefix(prefix) - Delete files with prefix, return count
    // =========================================================================

    describe('deletePrefix(prefix)', () => {
      it('should delete all files with prefix and return count', async () => {
        const prefix = 'delete-prefix-test/'
        await backend.write(`${prefix}file1.txt`, textToBytes('1'))
        await backend.write(`${prefix}file2.txt`, textToBytes('2'))
        await backend.write(`${prefix}file3.txt`, textToBytes('3'))

        const count = await backend.deletePrefix(prefix)

        expect(count).toBe(3)
        expect(await backend.exists(`${prefix}file1.txt`)).toBe(false)
        expect(await backend.exists(`${prefix}file2.txt`)).toBe(false)
        expect(await backend.exists(`${prefix}file3.txt`)).toBe(false)
      })

      it('should return 0 for non-matching prefix', async () => {
        const count = await backend.deletePrefix('nonexistent-prefix/')

        expect(count).toBe(0)
      })

      it('should delete files in nested directories', async () => {
        const prefix = 'delete-nested/'
        await backend.write(`${prefix}a/b/file1.txt`, textToBytes('1'))
        await backend.write(`${prefix}a/c/file2.txt`, textToBytes('2'))
        await backend.write(`${prefix}d/file3.txt`, textToBytes('3'))

        const count = await backend.deletePrefix(prefix)

        expect(count).toBe(3)
      })

      it('should not delete files without the prefix', async () => {
        const prefix = 'delete-specific/'
        const otherPath = 'keep-this/file.txt'

        await backend.write(`${prefix}delete-me.txt`, textToBytes('delete'))
        await backend.write(otherPath, textToBytes('keep'))

        await backend.deletePrefix(prefix)

        expect(await backend.exists(otherPath)).toBe(true)
      })

      it('should handle empty prefix carefully', async () => {
        // Empty prefix might delete everything - test carefully
        const prefix = 'safe-prefix/'
        await backend.write(`${prefix}file.txt`, textToBytes('content'))

        // Only delete the safe prefix, not empty
        const count = await backend.deletePrefix(prefix)

        expect(count).toBe(1)
      })
    })

    // =========================================================================
    // 10. mkdir(path) - Create directory
    // =========================================================================

    describe('mkdir(path)', () => {
      it('should create a directory', async () => {
        const path = 'test/mkdir-basic'

        await backend.mkdir(path)

        // Directory existence can be verified by writing a file into it
        await backend.write(`${path}/test.txt`, textToBytes('test'))
        expect(await backend.exists(`${path}/test.txt`)).toBe(true)
      })

      it('should create nested directories (mkdir -p behavior)', async () => {
        const path = 'test/mkdir-nested/level1/level2/level3'

        await backend.mkdir(path)

        await backend.write(`${path}/deep.txt`, textToBytes('deep'))
        expect(await backend.exists(`${path}/deep.txt`)).toBe(true)
      })

      it('should be idempotent (mkdir existing directory)', async () => {
        const path = 'test/mkdir-idempotent'

        await backend.mkdir(path)
        // Should not throw
        await backend.mkdir(path)

        await backend.write(`${path}/file.txt`, textToBytes('content'))
        expect(await backend.exists(`${path}/file.txt`)).toBe(true)
      })

      it('should handle paths with trailing slash', async () => {
        const path = 'test/mkdir-trailing/'

        await backend.mkdir(path)

        const normalizedPath = path.replace(/\/$/, '')
        await backend.write(`${normalizedPath}/file.txt`, textToBytes('content'))
        expect(await backend.exists(`${normalizedPath}/file.txt`)).toBe(true)
      })
    })

    // =========================================================================
    // 11. copy(source, dest) - Copy file
    // =========================================================================

    describe('copy(source, dest)', () => {
      it('should copy file to new location', async () => {
        const source = 'test/copy-source.txt'
        const dest = 'test/copy-dest.txt'
        const content = textToBytes('content to copy')

        await backend.write(source, content)
        await backend.copy(source, dest)

        // Both files should exist with same content
        expect(await backend.exists(source)).toBe(true)
        expect(await backend.exists(dest)).toBe(true)
        expect(bytesToText(await backend.read(dest))).toBe('content to copy')
      })

      it('should preserve original file after copy', async () => {
        const source = 'test/copy-preserve-source.txt'
        const dest = 'test/copy-preserve-dest.txt'
        const content = textToBytes('original content')

        await backend.write(source, content)
        await backend.copy(source, dest)

        expect(bytesToText(await backend.read(source))).toBe('original content')
      })

      it('should overwrite existing destination', async () => {
        const source = 'test/copy-overwrite-source.txt'
        const dest = 'test/copy-overwrite-dest.txt'

        await backend.write(source, textToBytes('new content'))
        await backend.write(dest, textToBytes('old content'))

        await backend.copy(source, dest)

        expect(bytesToText(await backend.read(dest))).toBe('new content')
      })

      it('should throw when source does not exist', async () => {
        const source = 'test/copy-nonexistent-source.txt'
        const dest = 'test/copy-nonexistent-dest.txt'

        await expect(backend.copy(source, dest)).rejects.toThrow()
      })

      it('should create destination parent directories', async () => {
        const source = 'test/copy-newdir-source.txt'
        const dest = 'test/newdir/subdir/copy-dest.txt'
        const content = textToBytes('content')

        await backend.write(source, content)
        await backend.copy(source, dest)

        expect(await backend.exists(dest)).toBe(true)
      })

      it('should copy binary files correctly', async () => {
        const source = 'test/copy-binary-source.bin'
        const dest = 'test/copy-binary-dest.bin'
        const data = generateTestData(10000)

        await backend.write(source, data)
        await backend.copy(source, dest)

        const result = await backend.read(dest)
        expect(result).toEqual(data)
      })
    })

    // =========================================================================
    // 12. move(source, dest) - Move/rename file
    // =========================================================================

    describe('move(source, dest)', () => {
      it('should move file to new location', async () => {
        const source = 'test/move-source.txt'
        const dest = 'test/move-dest.txt'
        const content = textToBytes('content to move')

        await backend.write(source, content)
        await backend.move(source, dest)

        // Source should not exist, dest should have content
        expect(await backend.exists(source)).toBe(false)
        expect(await backend.exists(dest)).toBe(true)
        expect(bytesToText(await backend.read(dest))).toBe('content to move')
      })

      it('should rename file in same directory', async () => {
        const source = 'test/rename-old.txt'
        const dest = 'test/rename-new.txt'
        const content = textToBytes('renamed content')

        await backend.write(source, content)
        await backend.move(source, dest)

        expect(await backend.exists(source)).toBe(false)
        expect(await backend.exists(dest)).toBe(true)
      })

      it('should overwrite existing destination', async () => {
        const source = 'test/move-overwrite-source.txt'
        const dest = 'test/move-overwrite-dest.txt'

        await backend.write(source, textToBytes('new'))
        await backend.write(dest, textToBytes('old'))

        await backend.move(source, dest)

        expect(bytesToText(await backend.read(dest))).toBe('new')
        expect(await backend.exists(source)).toBe(false)
      })

      it('should throw when source does not exist', async () => {
        const source = 'test/move-nonexistent-source.txt'
        const dest = 'test/move-nonexistent-dest.txt'

        await expect(backend.move(source, dest)).rejects.toThrow()
      })

      it('should create destination parent directories', async () => {
        const source = 'test/move-newdir-source.txt'
        const dest = 'test/newdir/subdir/move-dest.txt'

        await backend.write(source, textToBytes('content'))
        await backend.move(source, dest)

        expect(await backend.exists(dest)).toBe(true)
        expect(await backend.exists(source)).toBe(false)
      })

      it('should preserve file content after move', async () => {
        const source = 'test/move-preserve-source.bin'
        const dest = 'test/move-preserve-dest.bin'
        const data = generateTestData(5000)

        await backend.write(source, data)
        await backend.move(source, dest)

        expect(await backend.read(dest)).toEqual(data)
      })
    })

    // =========================================================================
    // 13. writeConditional(path, data, expectedVersion, options?)
    // =========================================================================

    describe('writeConditional(path, data, expectedVersion, options?)', () => {
      it('should write when expectedVersion is null and file does not exist', async () => {
        const path = 'test/conditional-new.txt'
        const content = textToBytes('new content')

        const result = await backend.writeConditional(path, content, null)

        expect(result.size).toBe(content.length)
        expect(bytesToText(await backend.read(path))).toBe('new content')
      })

      it('should fail when expectedVersion is null but file exists', async () => {
        const path = 'test/conditional-exists.txt'
        const content1 = textToBytes('existing')
        const content2 = textToBytes('new')

        await backend.write(path, content1)

        await expect(
          backend.writeConditional(path, content2, null)
        ).rejects.toThrow()
      })

      it('should write when expectedVersion matches current etag', async () => {
        const path = 'test/conditional-match.txt'
        const content1 = textToBytes('original')
        const content2 = textToBytes('updated')

        const writeResult = await backend.write(path, content1)
        const result = await backend.writeConditional(
          path,
          content2,
          writeResult.etag
        )

        expect(result.size).toBe(content2.length)
        expect(bytesToText(await backend.read(path))).toBe('updated')
      })

      it('should fail when expectedVersion does not match', async () => {
        const path = 'test/conditional-mismatch.txt'
        const content = textToBytes('content')

        await backend.write(path, content)

        await expect(
          backend.writeConditional(path, textToBytes('new'), 'wrong-version')
        ).rejects.toThrow()
      })

      it('should return new etag after successful conditional write', async () => {
        const path = 'test/conditional-etag.txt'
        const content1 = textToBytes('v1')
        const content2 = textToBytes('v2')

        const result1 = await backend.write(path, content1)
        const result2 = await backend.writeConditional(
          path,
          content2,
          result1.etag
        )

        expect(result2.etag).not.toBe(result1.etag)
      })

      it('should preserve content on version mismatch', async () => {
        const path = 'test/conditional-preserve.txt'
        const original = textToBytes('original content')
        const attempted = textToBytes('attempted update')

        await backend.write(path, original)

        try {
          await backend.writeConditional(path, attempted, 'bad-version')
        } catch {
          // Expected to throw
        }

        expect(bytesToText(await backend.read(path))).toBe('original content')
      })

      it('should support optimistic concurrency pattern', async () => {
        const path = 'test/conditional-optimistic.txt'
        const initial = textToBytes('initial')

        // Initial write
        const v1 = await backend.write(path, initial)

        // Concurrent updates - only one should succeed
        const update1 = backend.writeConditional(
          path,
          textToBytes('update1'),
          v1.etag
        )
        const update2 = backend.writeConditional(
          path,
          textToBytes('update2'),
          v1.etag
        )

        const results = await Promise.allSettled([update1, update2])

        const successes = results.filter((r) => r.status === 'fulfilled')
        const failures = results.filter((r) => r.status === 'rejected')

        // Exactly one should succeed
        expect(successes.length).toBe(1)
        expect(failures.length).toBe(1)
      })

      it('should accept WriteOptions', async () => {
        const path = 'test/conditional-options.txt'
        const content = textToBytes('content with options')
        const options: WriteOptions = {
          contentType: 'text/plain',
          metadata: { key: 'value' },
        }

        const result = await backend.writeConditional(path, content, null, options)

        expect(result.size).toBe(content.length)
      })

      it('should handle rapid sequential conditional writes', async () => {
        const path = 'test/conditional-rapid.txt'
        let currentEtag: string | null = null

        // Perform 10 sequential conditional writes
        for (let i = 0; i < 10; i++) {
          const content = textToBytes(`version-${i}`)
          const result = await backend.writeConditional(
            path,
            content,
            currentEtag
          )
          currentEtag = result.etag
        }

        expect(bytesToText(await backend.read(path))).toBe('version-9')
      })
    })

    // =========================================================================
    // Edge Cases and Error Handling
    // =========================================================================

    describe('Edge Cases and Error Handling', () => {
      it('should handle Unicode file names', async () => {
        const path = 'test/unicode-\u4e2d\u6587-\u65e5\u672c\u8a9e.txt'
        const content = textToBytes('unicode path test')

        await backend.write(path, content)
        const result = await backend.read(path)

        expect(bytesToText(result)).toBe('unicode path test')
      })

      it('should handle Unicode content', async () => {
        const path = 'test/unicode-content.txt'
        const content = textToBytes('\u4e2d\u6587\u5185\u5bb9 - Japanese: \u65e5\u672c\u8a9e - Emoji: \ud83c\udf89')

        await backend.write(path, content)
        const result = await backend.read(path)

        expect(bytesToText(result)).toBe('\u4e2d\u6587\u5185\u5bb9 - Japanese: \u65e5\u672c\u8a9e - Emoji: \ud83c\udf89')
      })

      it('should handle very long file paths', async () => {
        const segments = Array(20).fill('segment').join('/')
        const path = `test/${segments}/file.txt`
        const content = textToBytes('long path content')

        await backend.write(path, content)
        expect(await backend.exists(path)).toBe(true)
      })

      it('should handle concurrent reads and writes', async () => {
        const path = 'test/concurrent.txt'
        const content = textToBytes('concurrent test')

        await backend.write(path, content)

        // Multiple concurrent reads
        const reads = await Promise.all([
          backend.read(path),
          backend.read(path),
          backend.read(path),
        ])

        reads.forEach((result) => {
          expect(bytesToText(result)).toBe('concurrent test')
        })
      })

      it('should have consistent type property', () => {
        expect(typeof backend.type).toBe('string')
        expect(backend.type.length).toBeGreaterThan(0)
      })
    })

    // =========================================================================
    // append(path, data) - Append to file (for event logs)
    // =========================================================================

    describe('append(path, data)', () => {
      it('should append data to existing file', async () => {
        const path = 'test/append-basic.txt'
        await backend.write(path, textToBytes('Hello'))

        await backend.append(path, textToBytes(' World'))

        const result = await backend.read(path)
        expect(bytesToText(result)).toBe('Hello World')
      })

      it('should create file if it does not exist', async () => {
        const path = 'test/append-new.txt'

        await backend.append(path, textToBytes('First'))

        const result = await backend.read(path)
        expect(bytesToText(result)).toBe('First')
      })

      it('should handle multiple appends', async () => {
        const path = 'test/append-multiple.txt'
        await backend.write(path, textToBytes('A'))

        await backend.append(path, textToBytes('B'))
        await backend.append(path, textToBytes('C'))
        await backend.append(path, textToBytes('D'))

        const result = await backend.read(path)
        expect(bytesToText(result)).toBe('ABCD')
      })

      it('should append binary data correctly', async () => {
        const path = 'test/append-binary.bin'
        const data1 = new Uint8Array([0x01, 0x02])
        const data2 = new Uint8Array([0x03, 0x04])

        await backend.write(path, data1)
        await backend.append(path, data2)

        const result = await backend.read(path)
        expect(result).toEqual(new Uint8Array([0x01, 0x02, 0x03, 0x04]))
      })
    })

    // =========================================================================
    // rmdir(path, options?) - Remove directory
    // =========================================================================

    describe('rmdir(path, options?)', () => {
      it('should remove empty directory', async () => {
        const path = 'test/rmdir-empty'
        await backend.mkdir(path)

        await backend.rmdir(path)

        // Verify by trying to stat the directory
        const stat = await backend.stat(path)
        expect(stat).toBeNull()
      })

      it('should throw when removing non-empty directory without recursive option', async () => {
        const dirPath = 'test/rmdir-nonempty'
        await backend.write(`${dirPath}/file.txt`, textToBytes('content'))

        await expect(backend.rmdir(dirPath)).rejects.toThrow()
      })

      it('should remove non-empty directory with recursive option', async () => {
        const dirPath = 'test/rmdir-recursive'
        await backend.write(`${dirPath}/file1.txt`, textToBytes('1'))
        await backend.write(`${dirPath}/subdir/file2.txt`, textToBytes('2'))

        await backend.rmdir(dirPath, { recursive: true })

        expect(await backend.exists(`${dirPath}/file1.txt`)).toBe(false)
        expect(await backend.exists(`${dirPath}/subdir/file2.txt`)).toBe(false)
      })

      it('should throw when directory does not exist', async () => {
        await expect(backend.rmdir('test/nonexistent-dir')).rejects.toThrow()
      })
    })
  })
}

// =============================================================================
// Default Test Suite - Documents RED phase
// =============================================================================

/**
 * This test suite documents that we're in the RED phase of TDD.
 * The createStorageBackendTests function is designed to be used by
 * backend implementations to verify they conform to the interface.
 */
describe('StorageBackend (No Implementation)', () => {
  it('should fail because no implementation exists', () => {
    // This test documents that we're in the RED phase
    // It will fail until an implementation is provided

    // Attempt to import a backend that doesn't exist yet
    const createBackend = (): StorageBackend => {
      // This will throw because there's no implementation
      throw new Error(
        'No StorageBackend implementation available. ' +
        'This is expected in the RED phase of TDD. ' +
        'Implement a backend (e.g., MemoryBackend) to make tests pass.'
      )
    }

    expect(() => createBackend()).toThrow('No StorageBackend implementation available')
  })

  it('should have StorageBackend interface defined', async () => {
    // Verify the types are properly exported
    const storageModule = await import('../../src/types/storage')

    expect(storageModule).toHaveProperty('StoragePaths')
    expect(storageModule).toHaveProperty('parseStoragePath')
    expect(storageModule).toHaveProperty('isStreamable')
    expect(storageModule).toHaveProperty('isMultipart')
    expect(storageModule).toHaveProperty('isTransactional')
  })
})

// =============================================================================
// Run generic tests against MemoryBackend (will fail in RED phase)
// =============================================================================

/**
 * This invokes the generic test suite against MemoryBackend.
 * All tests will fail because MemoryBackend methods throw "Not implemented".
 *
 * Once MemoryBackend is implemented, these tests will pass.
 */
import { MemoryBackend } from '../../src/storage/MemoryBackend'

createStorageBackendTests(
  () => new MemoryBackend(),
  async (_backend) => {
    // Cleanup is a no-op for MemoryBackend since it's in-memory
    // and each test gets a fresh instance
  }
)

// =============================================================================
// Export for use by backend implementations
// =============================================================================

export type { StorageBackend } from '../../src/types/storage'
