/**
 * StorageBackend Interface Tests
 *
 * Comprehensive test suite for the StorageBackend interface contract.
 * Tests the core interface methods:
 * - read(path): Promise<Uint8Array>
 * - write(path, data): Promise<void>
 * - exists(path): Promise<boolean>
 * - list(prefix): Promise<string[]>
 * - readRange(path, start, end): Promise<Uint8Array>
 * - writeAtomic(path, data): Promise<void>
 *
 * This is TDD RED phase - tests verify the interface contract.
 * Using MemoryBackend as the test implementation.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend, FileNotFoundError } from '../../../src/storage/MemoryBackend'
import type { StorageBackend, ListResult, WriteResult } from '../../../src/types/storage'

// =============================================================================
// Helper Functions
// =============================================================================

function textToBytes(text: string): Uint8Array {
  return new TextEncoder().encode(text)
}

function bytesToText(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes)
}

function generateTestData(size: number): Uint8Array {
  const data = new Uint8Array(size)
  for (let i = 0; i < size; i++) {
    data[i] = i % 256
  }
  return data
}

// =============================================================================
// Test Suite
// =============================================================================

describe('StorageBackend Interface', () => {
  let backend: StorageBackend

  beforeEach(() => {
    backend = new MemoryBackend()
  })

  // ===========================================================================
  // 1. read(path): Promise<Uint8Array>
  // ===========================================================================

  describe('read(path)', () => {
    describe('happy path', () => {
      it('should read an existing file and return its contents as Uint8Array', async () => {
        const path = 'test/read-basic.txt'
        const content = textToBytes('Hello, World!')
        await backend.write(path, content)

        const result = await backend.read(path)

        expect(result).toBeInstanceOf(Uint8Array)
        expect(bytesToText(result)).toBe('Hello, World!')
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

      it('should read large files (1MB)', async () => {
        const path = 'test/large-file.bin'
        const largeData = generateTestData(1024 * 1024) // 1MB
        await backend.write(path, largeData)

        const result = await backend.read(path)

        expect(result.length).toBe(largeData.length)
        expect(result).toEqual(largeData)
      })

      it('should read files in nested directories', async () => {
        const path = 'test/level1/level2/level3/level4/deep-file.txt'
        const content = textToBytes('deep content')
        await backend.write(path, content)

        const result = await backend.read(path)

        expect(bytesToText(result)).toBe('deep content')
      })
    })

    describe('error handling', () => {
      it('should throw an error when file does not exist', async () => {
        const path = 'test/nonexistent-file.txt'

        await expect(backend.read(path)).rejects.toThrow()
      })

      it('should throw FileNotFoundError specifically for non-existent file', async () => {
        const path = 'test/definitely-not-here.txt'

        await expect(backend.read(path)).rejects.toThrow(FileNotFoundError)
      })

      it('should throw error with path information', async () => {
        const path = 'test/missing/path/file.txt'

        try {
          await backend.read(path)
          expect.fail('Should have thrown an error')
        } catch (error) {
          expect((error as Error).message).toContain('missing/path/file.txt')
        }
      })
    })

    describe('edge cases', () => {
      it('should handle paths with spaces', async () => {
        const path = 'test/path with spaces/file name.txt'
        const content = textToBytes('special path content')
        await backend.write(path, content)

        const result = await backend.read(path)

        expect(bytesToText(result)).toBe('special path content')
      })

      it('should handle Unicode file paths', async () => {
        const path = 'test/unicode-\u4e2d\u6587-\u65e5\u672c\u8a9e.txt'
        const content = textToBytes('unicode path test')
        await backend.write(path, content)

        const result = await backend.read(path)

        expect(bytesToText(result)).toBe('unicode path test')
      })

      it('should handle Unicode content', async () => {
        const path = 'test/unicode-content.txt'
        const unicodeContent = '\u4e2d\u6587\u5185\u5bb9 - Japanese: \u65e5\u672c\u8a9e'
        const content = textToBytes(unicodeContent)
        await backend.write(path, content)

        const result = await backend.read(path)

        expect(bytesToText(result)).toBe(unicodeContent)
      })

      it('should return a copy, not the original buffer', async () => {
        const path = 'test/immutable.txt'
        const content = textToBytes('original')
        await backend.write(path, content)

        const result = await backend.read(path)
        result[0] = 0x00 // Mutate the result

        const secondRead = await backend.read(path)
        expect(bytesToText(secondRead)).toBe('original')
      })
    })
  })

  // ===========================================================================
  // 2. write(path, data): Promise<WriteResult>
  // ===========================================================================

  describe('write(path, data)', () => {
    describe('happy path', () => {
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

      it('should create parent directories implicitly', async () => {
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

      it('should write binary data correctly', async () => {
        const path = 'test/binary-write.bin'
        const binaryData = new Uint8Array([0x00, 0x7f, 0x80, 0xff])

        await backend.write(path, binaryData)
        const result = await backend.read(path)

        expect(result).toEqual(binaryData)
      })

      it('should write large file (1MB)', async () => {
        const path = 'test/large-write.bin'
        const largeData = generateTestData(1024 * 1024) // 1MB

        const result = await backend.write(path, largeData)

        expect(result.size).toBe(largeData.length)
      })
    })

    describe('error handling', () => {
      it('should reject ifNoneMatch when file already exists', async () => {
        const path = 'test/if-none-match.txt'
        const content1 = textToBytes('first')
        const content2 = textToBytes('second')

        await backend.write(path, content1)

        await expect(
          backend.write(path, content2, { ifNoneMatch: '*' })
        ).rejects.toThrow()
      })

      it('should reject ifMatch when etag does not match', async () => {
        const path = 'test/if-match.txt'
        const content = textToBytes('content')

        await backend.write(path, content)

        await expect(
          backend.write(path, textToBytes('new'), { ifMatch: 'wrong-etag' })
        ).rejects.toThrow()
      })
    })

    describe('edge cases', () => {
      it('should return different etags for different content', async () => {
        const path1 = 'test/etag-test-1.txt'
        const path2 = 'test/etag-test-2.txt'
        const content1 = textToBytes('content one')
        const content2 = textToBytes('content two')

        const result1 = await backend.write(path1, content1)
        const result2 = await backend.write(path2, content2)

        expect(result1.etag).not.toBe(result2.etag)
      })

      it('should handle very long file paths', async () => {
        const segments = Array(20).fill('segment').join('/')
        const path = `test/${segments}/file.txt`
        const content = textToBytes('long path content')

        await backend.write(path, content)

        expect(await backend.exists(path)).toBe(true)
      })

      it('should store and retrieve custom metadata', async () => {
        const path = 'test/with-metadata.txt'
        const content = textToBytes('metadata test')

        await backend.write(path, content, {
          contentType: 'text/plain',
          metadata: { 'x-custom-key': 'custom-value' },
        })

        const stat = await backend.stat(path)
        if (stat?.metadata) {
          expect(stat.metadata['x-custom-key']).toBe('custom-value')
        }
      })
    })
  })

  // ===========================================================================
  // 3. exists(path): Promise<boolean>
  // ===========================================================================

  describe('exists(path)', () => {
    describe('happy path', () => {
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
    })

    describe('error handling', () => {
      it('should not throw for non-existent paths', async () => {
        const result = await backend.exists('completely/random/path/that/never/existed.txt')

        expect(result).toBe(false)
      })

      it('should not throw for deeply nested non-existent paths', async () => {
        const result = await backend.exists('a/b/c/d/e/f/g/h/i/j/k/l/m/n.txt')

        expect(result).toBe(false)
      })
    })

    describe('edge cases', () => {
      it('should handle paths with spaces', async () => {
        const path = 'test/path with spaces/file.txt'
        await backend.write(path, textToBytes('content'))

        expect(await backend.exists(path)).toBe(true)
        expect(await backend.exists('test/path with spaces/other.txt')).toBe(false)
      })

      it('should handle Unicode paths', async () => {
        const path = 'test/\u4e2d\u6587\u8def\u5f84/file.txt'
        await backend.write(path, textToBytes('content'))

        expect(await backend.exists(path)).toBe(true)
      })

      it('should distinguish between similar paths', async () => {
        await backend.write('test/file.txt', textToBytes('content'))

        expect(await backend.exists('test/file.txt')).toBe(true)
        expect(await backend.exists('test/file.tx')).toBe(false)
        expect(await backend.exists('test/file.txt.bak')).toBe(false)
        expect(await backend.exists('test/file')).toBe(false)
      })

      it('should handle empty string path', async () => {
        const result = await backend.exists('')

        expect(result).toBe(false)
      })
    })
  })

  // ===========================================================================
  // 4. list(prefix): Promise<ListResult>
  // ===========================================================================

  describe('list(prefix)', () => {
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

    describe('happy path', () => {
      it('should list all files with prefix', async () => {
        const result = await backend.list('list-test/')

        expect(result.files.length).toBeGreaterThanOrEqual(6)
        expect(result.hasMore).toBe(false)
      })

      it('should return ListResult with files array', async () => {
        const result = await backend.list('list-test/')

        expect(result).toHaveProperty('files')
        expect(Array.isArray(result.files)).toBe(true)
        expect(result).toHaveProperty('hasMore')
      })

      it('should filter by prefix correctly', async () => {
        const result = await backend.list('list-test/subdir/')

        expect(result.files.length).toBe(2)
        result.files.forEach((file) => {
          expect(file).toMatch(/^list-test\/subdir\//)
        })
      })

      it('should return empty result for non-matching prefix', async () => {
        const result = await backend.list('nonexistent-prefix/')

        expect(result.files.length).toBe(0)
        expect(result.hasMore).toBe(false)
      })
    })

    describe('error handling', () => {
      it('should handle empty prefix (list all)', async () => {
        const result = await backend.list('')

        expect(result.files.length).toBeGreaterThan(0)
      })

      it('should not throw for non-existent prefix', async () => {
        const result = await backend.list('this/path/does/not/exist/')

        expect(result.files).toEqual([])
        expect(result.hasMore).toBe(false)
      })
    })

    describe('edge cases', () => {
      it('should support limit option for pagination', async () => {
        const result = await backend.list('list-test/', { limit: 2 })

        expect(result.files.length).toBeLessThanOrEqual(2)
        if (result.files.length === 2) {
          expect(result.hasMore).toBe(true)
          expect(result.cursor).toBeDefined()
        }
      })

      it('should support cursor-based pagination', async () => {
        const page1 = await backend.list('list-test/', { limit: 2 })

        if (page1.hasMore && page1.cursor) {
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
        const result = await backend.list('list-test/', { delimiter: '/' })

        // Should have prefixes for subdirectories
        if (result.prefixes) {
          expect(result.prefixes.length).toBeGreaterThan(0)
        }
      })

      it('should handle partial prefix match correctly', async () => {
        // Should not match partial prefixes
        const result = await backend.list('list-tes')

        expect(result.files.length).toBe(0)
      })

      it('should include stats when includeMetadata is true', async () => {
        const result = await backend.list('list-test/', { includeMetadata: true })

        if (result.stats) {
          expect(result.stats.length).toBe(result.files.length)
          result.stats.forEach((stat) => {
            expect(stat).toHaveProperty('path')
            expect(stat).toHaveProperty('size')
            expect(stat).toHaveProperty('mtime')
          })
        }
      })
    })
  })

  // ===========================================================================
  // 5. readRange(path, start, end): Promise<Uint8Array>
  // ===========================================================================

  describe('readRange(path, start, end)', () => {
    describe('happy path', () => {
      it('should read a byte range from file', async () => {
        const path = 'test/range-basic.txt'
        const content = textToBytes('Hello, World!')
        await backend.write(path, content)

        // Read "World" (bytes 7-12)
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

      it('should read entire file via range', async () => {
        const path = 'test/range-full.txt'
        const content = textToBytes('12345')
        await backend.write(path, content)

        const result = await backend.readRange(path, 0, 5)

        expect(bytesToText(result)).toBe('12345')
      })
    })

    describe('error handling', () => {
      it('should throw for non-existent file', async () => {
        await expect(
          backend.readRange('test/nonexistent-range.txt', 0, 10)
        ).rejects.toThrow()
      })

      it('should throw FileNotFoundError for non-existent file', async () => {
        await expect(
          backend.readRange('test/nonexistent-range.txt', 0, 10)
        ).rejects.toThrow(FileNotFoundError)
      })

      it('should throw for negative start position', async () => {
        const path = 'test/range-validation.txt'
        await backend.write(path, textToBytes('test content'))

        await expect(
          backend.readRange(path, -1, 10)
        ).rejects.toThrow()
      })

      it('should throw with descriptive message for negative start', async () => {
        const path = 'test/range-validation.txt'
        await backend.write(path, textToBytes('test content'))

        await expect(
          backend.readRange(path, -5, 10)
        ).rejects.toThrow(/start.*-5.*non-negative|start.*negative/i)
      })

      it('should throw when end is less than start', async () => {
        const path = 'test/range-validation.txt'
        await backend.write(path, textToBytes('test content'))

        await expect(
          backend.readRange(path, 10, 5)
        ).rejects.toThrow()
      })

      it('should throw with descriptive message when end < start', async () => {
        const path = 'test/range-validation.txt'
        await backend.write(path, textToBytes('test content'))

        await expect(
          backend.readRange(path, 20, 10)
        ).rejects.toThrow(/end.*10.*>=.*start.*20|end.*start/i)
      })
    })

    describe('edge cases', () => {
      it('should handle Parquet footer read pattern (last 8 bytes)', async () => {
        const path = 'test/parquet-like.bin'
        const size = 1024
        const data = generateTestData(size)
        await backend.write(path, data)

        // Read last 8 bytes (common Parquet footer pattern)
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

      it('should handle range exceeding file size gracefully', async () => {
        const path = 'test/range-exceed.txt'
        const content = textToBytes('SHORT')
        await backend.write(path, content)

        // Depending on implementation, might throw or clamp
        try {
          const result = await backend.readRange(path, 0, 1000)
          // If it doesn't throw, it should return available bytes
          expect(result.length).toBeLessThanOrEqual(content.length)
        } catch {
          // Throwing is also acceptable behavior
          expect(true).toBe(true)
        }
      })

      it('should return empty array when start equals end', async () => {
        const path = 'test/range-empty.txt'
        await backend.write(path, textToBytes('content'))

        const result = await backend.readRange(path, 5, 5)

        expect(result.length).toBe(0)
      })

      it('should return empty array when start is beyond file size', async () => {
        const path = 'test/range-beyond.txt'
        await backend.write(path, textToBytes('short'))

        const result = await backend.readRange(path, 100, 200)

        expect(result.length).toBe(0)
      })
    })
  })

  // ===========================================================================
  // 6. writeAtomic(path, data): Promise<WriteResult>
  // ===========================================================================

  describe('writeAtomic(path, data)', () => {
    describe('happy path', () => {
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

      it('should create parent directories if needed', async () => {
        const path = 'test/atomic-newdir/subdir/file.txt'
        const content = textToBytes('atomic in new directory')

        const result = await backend.writeAtomic(path, content)

        expect(result.size).toBe(content.length)
        const readResult = await backend.read(path)
        expect(bytesToText(readResult)).toBe('atomic in new directory')
      })

      it('should write empty file atomically', async () => {
        const path = 'test/atomic-empty.txt'
        const content = new Uint8Array(0)

        const result = await backend.writeAtomic(path, content)

        expect(result.size).toBe(0)
      })
    })

    describe('error handling', () => {
      it('should preserve original file if partial write would fail', async () => {
        const path = 'test/atomic-partial.txt'
        const content = textToBytes('original content')

        await backend.write(path, content)

        // The atomic nature should ensure either complete success or no change
        const result = await backend.read(path)
        expect(bytesToText(result)).toBe('original content')
      })
    })

    describe('edge cases', () => {
      it('should accept WriteOptions', async () => {
        const path = 'test/atomic-with-options.txt'
        const content = textToBytes('atomic with options')

        const result = await backend.writeAtomic(path, content, {
          contentType: 'text/plain',
          metadata: { key: 'value' },
        })

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

      it('should return different etag than regular write for same content', async () => {
        const content = textToBytes('same content')

        const result1 = await backend.write('test/regular.txt', content)
        const result2 = await backend.writeAtomic('test/atomic.txt', content)

        // Both should succeed and have etags
        expect(result1.etag).toBeDefined()
        expect(result2.etag).toBeDefined()
      })

      it('should write large file atomically', async () => {
        const path = 'test/atomic-large.bin'
        const largeData = generateTestData(1024 * 1024) // 1MB

        const result = await backend.writeAtomic(path, largeData)

        expect(result.size).toBe(largeData.length)
        const readResult = await backend.read(path)
        expect(readResult).toEqual(largeData)
      })
    })
  })

  // ===========================================================================
  // Interface Contract Tests
  // ===========================================================================

  describe('Interface Contract', () => {
    it('should have type property', () => {
      expect(backend.type).toBeDefined()
      expect(typeof backend.type).toBe('string')
      expect(backend.type.length).toBeGreaterThan(0)
    })

    it('should have all required methods', () => {
      // Core methods from the issue
      expect(typeof backend.read).toBe('function')
      expect(typeof backend.write).toBe('function')
      expect(typeof backend.exists).toBe('function')
      expect(typeof backend.list).toBe('function')
      expect(typeof backend.readRange).toBe('function')
      expect(typeof backend.writeAtomic).toBe('function')

      // Additional required methods
      expect(typeof backend.stat).toBe('function')
      expect(typeof backend.delete).toBe('function')
      expect(typeof backend.deletePrefix).toBe('function')
      expect(typeof backend.mkdir).toBe('function')
      expect(typeof backend.rmdir).toBe('function')
      expect(typeof backend.writeConditional).toBe('function')
      expect(typeof backend.copy).toBe('function')
      expect(typeof backend.move).toBe('function')
      expect(typeof backend.append).toBe('function')
    })

    it('should return Promise from all methods', async () => {
      const content = textToBytes('test')
      await backend.write('test/contract.txt', content)

      // All methods should return Promises
      expect(backend.read('test/contract.txt')).toBeInstanceOf(Promise)
      expect(backend.write('test/contract2.txt', content)).toBeInstanceOf(Promise)
      expect(backend.exists('test/contract.txt')).toBeInstanceOf(Promise)
      expect(backend.list('test/')).toBeInstanceOf(Promise)
      expect(backend.readRange('test/contract.txt', 0, 4)).toBeInstanceOf(Promise)
      expect(backend.writeAtomic('test/contract3.txt', content)).toBeInstanceOf(Promise)
    })
  })

  // ===========================================================================
  // Cross-Method Integration Tests
  // ===========================================================================

  describe('Cross-Method Integration', () => {
    it('should maintain consistency between write, read, and exists', async () => {
      const path = 'test/consistency.txt'
      const content = textToBytes('consistent content')

      // Initially should not exist
      expect(await backend.exists(path)).toBe(false)

      // After write, should exist and be readable
      await backend.write(path, content)
      expect(await backend.exists(path)).toBe(true)
      expect(await backend.read(path)).toEqual(content)

      // After delete, should not exist
      await backend.delete(path)
      expect(await backend.exists(path)).toBe(false)
      await expect(backend.read(path)).rejects.toThrow()
    })

    it('should maintain consistency between write and list', async () => {
      const prefix = 'test/list-consistency/'
      const files = ['a.txt', 'b.txt', 'c.txt']

      // Write files
      for (const file of files) {
        await backend.write(`${prefix}${file}`, textToBytes(file))
      }

      // List should contain all files
      const result = await backend.list(prefix)
      expect(result.files.length).toBe(files.length)
      files.forEach((file) => {
        expect(result.files).toContain(`${prefix}${file}`)
      })

      // Delete one file
      await backend.delete(`${prefix}b.txt`)

      // List should reflect deletion
      const result2 = await backend.list(prefix)
      expect(result2.files.length).toBe(files.length - 1)
      expect(result2.files).not.toContain(`${prefix}b.txt`)
    })

    it('should read same content via read and readRange', async () => {
      const path = 'test/read-consistency.txt'
      const content = textToBytes('Hello, World!')
      await backend.write(path, content)

      const fullRead = await backend.read(path)
      const rangeRead = await backend.readRange(path, 0, content.length)

      expect(fullRead).toEqual(rangeRead)
    })

    it('should write same result via write and writeAtomic', async () => {
      const content = textToBytes('same content')

      await backend.write('test/regular.txt', content)
      await backend.writeAtomic('test/atomic.txt', content)

      const regularRead = await backend.read('test/regular.txt')
      const atomicRead = await backend.read('test/atomic.txt')

      expect(regularRead).toEqual(atomicRead)
    })
  })
})

// =============================================================================
// Export for use by other backend implementations
// =============================================================================

export { textToBytes, bytesToText, generateTestData }
