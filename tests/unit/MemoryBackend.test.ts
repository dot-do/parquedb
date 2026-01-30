/**
 * MemoryBackend Test Suite
 *
 * Comprehensive tests for the in-memory StorageBackend implementation.
 * These tests follow the TDD RED phase - they will fail until implementation is complete.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  MemoryBackend,
  FileNotFoundError,
  VersionMismatchError,
  FileExistsError,
  DirectoryNotEmptyError,
} from '../../src/storage/MemoryBackend'

// Helper to create test data
function createTestData(content: string): Uint8Array {
  return new TextEncoder().encode(content)
}

// Helper to decode test data
function decodeData(data: Uint8Array): string {
  return new TextDecoder().decode(data)
}

describe('MemoryBackend', () => {
  let backend: MemoryBackend

  beforeEach(() => {
    backend = new MemoryBackend()
  })

  // ===========================================================================
  // Basic Properties
  // ===========================================================================

  describe('type property', () => {
    it('should have type "memory"', () => {
      expect(backend.type).toBe('memory')
    })
  })

  // ===========================================================================
  // 1. Basic Read/Write Operations
  // ===========================================================================

  describe('Basic read/write operations', () => {
    it('should write and read a file', async () => {
      const data = createTestData('Hello, World!')
      await backend.write('test.txt', data)

      const result = await backend.read('test.txt')
      expect(decodeData(result)).toBe('Hello, World!')
    })

    it('should write and read binary data', async () => {
      const data = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])
      await backend.write('binary.dat', data)

      const result = await backend.read('binary.dat')
      expect(result).toEqual(data)
    })

    it('should overwrite existing file on write', async () => {
      await backend.write('test.txt', createTestData('First'))
      await backend.write('test.txt', createTestData('Second'))

      const result = await backend.read('test.txt')
      expect(decodeData(result)).toBe('Second')
    })

    it('should throw FileNotFoundError when reading non-existent file', async () => {
      await expect(backend.read('nonexistent.txt')).rejects.toThrow(FileNotFoundError)
    })

    it('should write to nested paths', async () => {
      const data = createTestData('Nested content')
      await backend.write('a/b/c/file.txt', data)

      const result = await backend.read('a/b/c/file.txt')
      expect(decodeData(result)).toBe('Nested content')
    })

    it('should handle empty files', async () => {
      await backend.write('empty.txt', new Uint8Array(0))

      const result = await backend.read('empty.txt')
      expect(result.length).toBe(0)
    })

    it('should write and return WriteResult with etag and size', async () => {
      const data = createTestData('Test content')
      const result = await backend.write('test.txt', data)

      expect(result.size).toBe(data.length)
      expect(result.etag).toBeDefined()
      expect(typeof result.etag).toBe('string')
      expect(result.etag.length).toBeGreaterThan(0)
    })

    it('should generate different etags for different content', async () => {
      const result1 = await backend.write('file1.txt', createTestData('Content A'))
      const result2 = await backend.write('file2.txt', createTestData('Content B'))

      expect(result1.etag).not.toBe(result2.etag)
    })
  })

  // ===========================================================================
  // 2. File Existence Checks
  // ===========================================================================

  describe('File existence checks', () => {
    it('should return false for non-existent file', async () => {
      const exists = await backend.exists('nonexistent.txt')
      expect(exists).toBe(false)
    })

    it('should return true for existing file', async () => {
      await backend.write('test.txt', createTestData('content'))

      const exists = await backend.exists('test.txt')
      expect(exists).toBe(true)
    })

    it('should return false after file is deleted', async () => {
      await backend.write('test.txt', createTestData('content'))
      await backend.delete('test.txt')

      const exists = await backend.exists('test.txt')
      expect(exists).toBe(false)
    })

    it('should correctly check nested paths', async () => {
      await backend.write('a/b/c.txt', createTestData('content'))

      expect(await backend.exists('a/b/c.txt')).toBe(true)
      expect(await backend.exists('a/b/d.txt')).toBe(false)
      expect(await backend.exists('a/b')).toBe(false) // Directories don't "exist" as files
    })
  })

  // ===========================================================================
  // 3. Directory Operations (mkdir, list with delimiter)
  // ===========================================================================

  describe('Directory operations', () => {
    describe('mkdir', () => {
      it('should create directory', async () => {
        await backend.mkdir('testdir')
        // mkdir should succeed without error
      })

      it('should create nested directories', async () => {
        await backend.mkdir('a/b/c')
        // Should create all parent directories
      })

      it('should not error when directory already exists', async () => {
        await backend.mkdir('testdir')
        await backend.mkdir('testdir')
        // Should not throw
      })
    })

    describe('rmdir', () => {
      it('should remove empty directory', async () => {
        await backend.mkdir('testdir')
        await backend.rmdir('testdir')
      })

      it('should throw DirectoryNotEmptyError when directory has files', async () => {
        await backend.write('testdir/file.txt', createTestData('content'))

        await expect(backend.rmdir('testdir')).rejects.toThrow(DirectoryNotEmptyError)
      })

      it('should remove non-empty directory with recursive option', async () => {
        await backend.write('testdir/file.txt', createTestData('content'))
        await backend.write('testdir/sub/file2.txt', createTestData('content'))

        await backend.rmdir('testdir', { recursive: true })

        expect(await backend.exists('testdir/file.txt')).toBe(false)
        expect(await backend.exists('testdir/sub/file2.txt')).toBe(false)
      })
    })

    describe('list with delimiter', () => {
      beforeEach(async () => {
        // Set up a directory structure
        await backend.write('data/users/user1.json', createTestData('{}'))
        await backend.write('data/users/user2.json', createTestData('{}'))
        await backend.write('data/orders/order1.json', createTestData('{}'))
        await backend.write('data/config.json', createTestData('{}'))
        await backend.write('readme.txt', createTestData('readme'))
      })

      it('should list files without delimiter (flat list)', async () => {
        const result = await backend.list('data/')

        expect(result.files).toContain('data/users/user1.json')
        expect(result.files).toContain('data/users/user2.json')
        expect(result.files).toContain('data/orders/order1.json')
        expect(result.files).toContain('data/config.json')
        expect(result.files).not.toContain('readme.txt')
      })

      it('should list with delimiter to show "directories"', async () => {
        const result = await backend.list('data/', { delimiter: '/' })

        expect(result.files).toContain('data/config.json')
        expect(result.prefixes).toBeDefined()
        expect(result.prefixes).toContain('data/users/')
        expect(result.prefixes).toContain('data/orders/')
        // Files in subdirs should not be in files list when using delimiter
        expect(result.files).not.toContain('data/users/user1.json')
      })

      it('should list subdirectory contents with delimiter', async () => {
        const result = await backend.list('data/users/', { delimiter: '/' })

        expect(result.files).toContain('data/users/user1.json')
        expect(result.files).toContain('data/users/user2.json')
        expect(result.prefixes || []).toHaveLength(0)
      })

      it('should return empty result for non-matching prefix', async () => {
        const result = await backend.list('nonexistent/')

        expect(result.files).toHaveLength(0)
        expect(result.hasMore).toBe(false)
      })

      it('should match prefix exactly', async () => {
        // Should not match partial prefixes
        const result = await backend.list('dat')

        expect(result.files).toHaveLength(0)
      })

      it('should handle root listing', async () => {
        const result = await backend.list('', { delimiter: '/' })

        expect(result.prefixes).toContain('data/')
        expect(result.files).toContain('readme.txt')
      })
    })
  })

  // ===========================================================================
  // 4. Byte Range Reads (readRange)
  // ===========================================================================

  describe('Byte range reads (readRange)', () => {
    beforeEach(async () => {
      // Create a file with known content: "0123456789"
      await backend.write('numbers.txt', createTestData('0123456789'))
    })

    it('should read full range', async () => {
      const result = await backend.readRange('numbers.txt', 0, 10)
      expect(decodeData(result)).toBe('0123456789')
    })

    it('should read partial range from start', async () => {
      const result = await backend.readRange('numbers.txt', 0, 5)
      expect(decodeData(result)).toBe('01234')
    })

    it('should read partial range from middle', async () => {
      const result = await backend.readRange('numbers.txt', 3, 7)
      expect(decodeData(result)).toBe('3456')
    })

    it('should read partial range to end', async () => {
      const result = await backend.readRange('numbers.txt', 7, 10)
      expect(decodeData(result)).toBe('789')
    })

    it('should handle single byte read', async () => {
      const result = await backend.readRange('numbers.txt', 5, 6)
      expect(decodeData(result)).toBe('5')
    })

    it('should throw FileNotFoundError for non-existent file', async () => {
      await expect(backend.readRange('nonexistent.txt', 0, 10)).rejects.toThrow(FileNotFoundError)
    })

    it('should handle end beyond file size', async () => {
      const result = await backend.readRange('numbers.txt', 5, 100)
      expect(decodeData(result)).toBe('56789')
    })

    it('should return empty array when start equals end', async () => {
      const result = await backend.readRange('numbers.txt', 5, 5)
      expect(result.length).toBe(0)
    })

    it('should return empty array when start is beyond file size', async () => {
      const result = await backend.readRange('numbers.txt', 100, 200)
      expect(result.length).toBe(0)
    })
  })

  // ===========================================================================
  // 5. Atomic Writes
  // ===========================================================================

  describe('Atomic writes (writeAtomic)', () => {
    it('should write file atomically', async () => {
      const data = createTestData('Atomic content')
      const result = await backend.writeAtomic('atomic.txt', data)

      expect(result.size).toBe(data.length)
      expect(await backend.read('atomic.txt')).toEqual(data)
    })

    it('should overwrite existing file atomically', async () => {
      await backend.write('atomic.txt', createTestData('Original'))
      await backend.writeAtomic('atomic.txt', createTestData('Updated'))

      expect(decodeData(await backend.read('atomic.txt'))).toBe('Updated')
    })

    it('should return WriteResult with etag', async () => {
      const result = await backend.writeAtomic('atomic.txt', createTestData('content'))

      expect(result.etag).toBeDefined()
      expect(result.size).toBeGreaterThan(0)
    })

    it('should handle write options', async () => {
      const result = await backend.writeAtomic('atomic.txt', createTestData('content'), {
        contentType: 'text/plain',
        metadata: { custom: 'value' },
      })

      expect(result.etag).toBeDefined()

      // Verify metadata was stored
      const stat = await backend.stat('atomic.txt')
      expect(stat?.contentType).toBe('text/plain')
      expect(stat?.metadata?.custom).toBe('value')
    })
  })

  // ===========================================================================
  // 6. Conditional Writes with Version Checking
  // ===========================================================================

  describe('Conditional writes (writeConditional)', () => {
    it('should write when expectedVersion is null and file does not exist', async () => {
      const result = await backend.writeConditional(
        'new.txt',
        createTestData('New file'),
        null
      )

      expect(result.etag).toBeDefined()
      expect(decodeData(await backend.read('new.txt'))).toBe('New file')
    })

    it('should fail when expectedVersion is null but file exists', async () => {
      await backend.write('existing.txt', createTestData('Existing'))

      await expect(
        backend.writeConditional('existing.txt', createTestData('Update'), null)
      ).rejects.toThrow(VersionMismatchError)
    })

    it('should write when expectedVersion matches current version', async () => {
      const writeResult = await backend.write('versioned.txt', createTestData('v1'))

      const result = await backend.writeConditional(
        'versioned.txt',
        createTestData('v2'),
        writeResult.etag
      )

      expect(result.etag).toBeDefined()
      expect(result.etag).not.toBe(writeResult.etag)
      expect(decodeData(await backend.read('versioned.txt'))).toBe('v2')
    })

    it('should fail when expectedVersion does not match', async () => {
      await backend.write('versioned.txt', createTestData('v1'))

      await expect(
        backend.writeConditional('versioned.txt', createTestData('v2'), 'wrong-etag')
      ).rejects.toThrow(VersionMismatchError)
    })

    it('should fail when file does not exist but expectedVersion is provided', async () => {
      await expect(
        backend.writeConditional('nonexistent.txt', createTestData('content'), 'some-etag')
      ).rejects.toThrow(VersionMismatchError)
    })

    it('should support ifNoneMatch option to only create if not exists', async () => {
      // First write should succeed
      const result = await backend.write('test.txt', createTestData('content'), {
        ifNoneMatch: '*',
      })
      expect(result.etag).toBeDefined()

      // Second write should fail
      await expect(
        backend.write('test.txt', createTestData('new content'), { ifNoneMatch: '*' })
      ).rejects.toThrow(FileExistsError)
    })

    it('should support ifMatch option for conditional update', async () => {
      const firstWrite = await backend.write('test.txt', createTestData('v1'))

      // Update with correct etag should succeed
      const secondWrite = await backend.write('test.txt', createTestData('v2'), {
        ifMatch: firstWrite.etag,
      })
      expect(secondWrite.etag).toBeDefined()

      // Update with old etag should fail
      await expect(
        backend.write('test.txt', createTestData('v3'), { ifMatch: firstWrite.etag })
      ).rejects.toThrow(VersionMismatchError)
    })
  })

  // ===========================================================================
  // 7. Copy and Move Operations
  // ===========================================================================

  describe('Copy and move operations', () => {
    describe('copy', () => {
      it('should copy file to new location', async () => {
        await backend.write('source.txt', createTestData('Copy me'))

        await backend.copy('source.txt', 'dest.txt')

        expect(decodeData(await backend.read('source.txt'))).toBe('Copy me')
        expect(decodeData(await backend.read('dest.txt'))).toBe('Copy me')
      })

      it('should overwrite destination if exists', async () => {
        await backend.write('source.txt', createTestData('Source content'))
        await backend.write('dest.txt', createTestData('Old dest content'))

        await backend.copy('source.txt', 'dest.txt')

        expect(decodeData(await backend.read('dest.txt'))).toBe('Source content')
      })

      it('should throw FileNotFoundError when source does not exist', async () => {
        await expect(backend.copy('nonexistent.txt', 'dest.txt')).rejects.toThrow(FileNotFoundError)
      })

      it('should copy to nested path', async () => {
        await backend.write('source.txt', createTestData('content'))

        await backend.copy('source.txt', 'a/b/c/dest.txt')

        expect(decodeData(await backend.read('a/b/c/dest.txt'))).toBe('content')
      })

      it('should preserve content after copy', async () => {
        const binaryData = new Uint8Array([0x00, 0xff, 0x7f, 0x80])
        await backend.write('binary.dat', binaryData)

        await backend.copy('binary.dat', 'binary-copy.dat')

        expect(await backend.read('binary-copy.dat')).toEqual(binaryData)
      })
    })

    describe('move', () => {
      it('should move file to new location', async () => {
        await backend.write('source.txt', createTestData('Move me'))

        await backend.move('source.txt', 'dest.txt')

        expect(await backend.exists('source.txt')).toBe(false)
        expect(decodeData(await backend.read('dest.txt'))).toBe('Move me')
      })

      it('should overwrite destination if exists', async () => {
        await backend.write('source.txt', createTestData('Source content'))
        await backend.write('dest.txt', createTestData('Old dest content'))

        await backend.move('source.txt', 'dest.txt')

        expect(await backend.exists('source.txt')).toBe(false)
        expect(decodeData(await backend.read('dest.txt'))).toBe('Source content')
      })

      it('should throw FileNotFoundError when source does not exist', async () => {
        await expect(backend.move('nonexistent.txt', 'dest.txt')).rejects.toThrow(FileNotFoundError)
      })

      it('should move to nested path', async () => {
        await backend.write('source.txt', createTestData('content'))

        await backend.move('source.txt', 'a/b/c/dest.txt')

        expect(await backend.exists('source.txt')).toBe(false)
        expect(decodeData(await backend.read('a/b/c/dest.txt'))).toBe('content')
      })

      it('should effectively rename file', async () => {
        await backend.write('old-name.txt', createTestData('Same content'))

        await backend.move('old-name.txt', 'new-name.txt')

        expect(await backend.exists('old-name.txt')).toBe(false)
        expect(decodeData(await backend.read('new-name.txt'))).toBe('Same content')
      })
    })
  })

  // ===========================================================================
  // 8. Delete and deletePrefix
  // ===========================================================================

  describe('Delete operations', () => {
    describe('delete', () => {
      it('should delete existing file and return true', async () => {
        await backend.write('test.txt', createTestData('content'))

        const result = await backend.delete('test.txt')

        expect(result).toBe(true)
        expect(await backend.exists('test.txt')).toBe(false)
      })

      it('should return false when deleting non-existent file', async () => {
        const result = await backend.delete('nonexistent.txt')
        expect(result).toBe(false)
      })

      it('should delete file from nested path', async () => {
        await backend.write('a/b/c/file.txt', createTestData('content'))

        const result = await backend.delete('a/b/c/file.txt')

        expect(result).toBe(true)
        expect(await backend.exists('a/b/c/file.txt')).toBe(false)
      })

      it('should only delete specified file', async () => {
        await backend.write('file1.txt', createTestData('content1'))
        await backend.write('file2.txt', createTestData('content2'))

        await backend.delete('file1.txt')

        expect(await backend.exists('file1.txt')).toBe(false)
        expect(await backend.exists('file2.txt')).toBe(true)
      })
    })

    describe('deletePrefix', () => {
      beforeEach(async () => {
        await backend.write('data/a.txt', createTestData('a'))
        await backend.write('data/b.txt', createTestData('b'))
        await backend.write('data/sub/c.txt', createTestData('c'))
        await backend.write('other/d.txt', createTestData('d'))
      })

      it('should delete all files with prefix and return count', async () => {
        const count = await backend.deletePrefix('data/')

        expect(count).toBe(3)
        expect(await backend.exists('data/a.txt')).toBe(false)
        expect(await backend.exists('data/b.txt')).toBe(false)
        expect(await backend.exists('data/sub/c.txt')).toBe(false)
        expect(await backend.exists('other/d.txt')).toBe(true)
      })

      it('should return 0 when no files match prefix', async () => {
        const count = await backend.deletePrefix('nonexistent/')

        expect(count).toBe(0)
      })

      it('should delete files in subdirectories', async () => {
        const count = await backend.deletePrefix('data/sub/')

        expect(count).toBe(1)
        expect(await backend.exists('data/a.txt')).toBe(true)
        expect(await backend.exists('data/sub/c.txt')).toBe(false)
      })

      it('should handle empty prefix (delete all)', async () => {
        const count = await backend.deletePrefix('')

        expect(count).toBe(4)
        expect(await backend.exists('data/a.txt')).toBe(false)
        expect(await backend.exists('other/d.txt')).toBe(false)
      })
    })
  })

  // ===========================================================================
  // 9. Stat Returning Correct FileStat
  // ===========================================================================

  describe('Stat operations', () => {
    it('should return null for non-existent file', async () => {
      const stat = await backend.stat('nonexistent.txt')
      expect(stat).toBeNull()
    })

    it('should return correct FileStat for file', async () => {
      const data = createTestData('Hello, World!')
      await backend.write('test.txt', data)

      const stat = await backend.stat('test.txt')

      expect(stat).not.toBeNull()
      expect(stat!.path).toBe('test.txt')
      expect(stat!.size).toBe(data.length)
      expect(stat!.isDirectory).toBe(false)
      expect(stat!.mtime).toBeInstanceOf(Date)
    })

    it('should return etag in FileStat', async () => {
      const writeResult = await backend.write('test.txt', createTestData('content'))

      const stat = await backend.stat('test.txt')

      expect(stat!.etag).toBe(writeResult.etag)
    })

    it('should update mtime on write', async () => {
      await backend.write('test.txt', createTestData('v1'))
      const stat1 = await backend.stat('test.txt')

      // Wait a tiny bit to ensure different timestamp
      await new Promise((resolve) => setTimeout(resolve, 10))

      await backend.write('test.txt', createTestData('v2'))
      const stat2 = await backend.stat('test.txt')

      expect(stat2!.mtime.getTime()).toBeGreaterThanOrEqual(stat1!.mtime.getTime())
    })

    it('should return contentType from write options', async () => {
      await backend.write('test.json', createTestData('{}'), {
        contentType: 'application/json',
      })

      const stat = await backend.stat('test.json')

      expect(stat!.contentType).toBe('application/json')
    })

    it('should return custom metadata from write options', async () => {
      await backend.write('test.txt', createTestData('content'), {
        metadata: { author: 'test', version: '1.0' },
      })

      const stat = await backend.stat('test.txt')

      expect(stat!.metadata).toEqual({ author: 'test', version: '1.0' })
    })

    it('should return correct size after content change', async () => {
      await backend.write('test.txt', createTestData('short'))
      expect((await backend.stat('test.txt'))!.size).toBe(5)

      await backend.write('test.txt', createTestData('much longer content'))
      expect((await backend.stat('test.txt'))!.size).toBe(19)
    })

    it('should handle stat for directory', async () => {
      await backend.mkdir('mydir')

      const stat = await backend.stat('mydir')

      // Could be null or have isDirectory: true depending on implementation
      if (stat !== null) {
        expect(stat.isDirectory).toBe(true)
      }
    })
  })

  // ===========================================================================
  // 10. List Pagination with Cursor
  // ===========================================================================

  describe('List pagination with cursor', () => {
    beforeEach(async () => {
      // Create 25 files for pagination testing
      for (let i = 0; i < 25; i++) {
        await backend.write(`files/file${i.toString().padStart(2, '0')}.txt`, createTestData(`content ${i}`))
      }
    })

    it('should respect limit option', async () => {
      const result = await backend.list('files/', { limit: 10 })

      expect(result.files.length).toBe(10)
      expect(result.hasMore).toBe(true)
    })

    it('should return cursor when there are more results', async () => {
      const result = await backend.list('files/', { limit: 10 })

      expect(result.cursor).toBeDefined()
      expect(result.hasMore).toBe(true)
    })

    it('should return all results when limit is not set', async () => {
      const result = await backend.list('files/')

      expect(result.files.length).toBe(25)
      expect(result.hasMore).toBe(false)
      expect(result.cursor).toBeUndefined()
    })

    it('should continue from cursor', async () => {
      const page1 = await backend.list('files/', { limit: 10 })
      const page2 = await backend.list('files/', { limit: 10, cursor: page1.cursor })

      expect(page2.files.length).toBe(10)
      expect(page2.hasMore).toBe(true)

      // Pages should have different files
      const page1Set = new Set(page1.files)
      const page2Set = new Set(page2.files)
      expect([...page1Set].some((f) => page2Set.has(f))).toBe(false)
    })

    it('should paginate through all results', async () => {
      const allFiles: string[] = []
      let cursor: string | undefined

      do {
        const result = await backend.list('files/', { limit: 10, cursor })
        allFiles.push(...result.files)
        cursor = result.cursor
      } while (cursor)

      expect(allFiles.length).toBe(25)
      // Check all files are unique
      expect(new Set(allFiles).size).toBe(25)
    })

    it('should return hasMore: false on last page', async () => {
      const page1 = await backend.list('files/', { limit: 10 })
      const page2 = await backend.list('files/', { limit: 10, cursor: page1.cursor })
      const page3 = await backend.list('files/', { limit: 10, cursor: page2.cursor })

      expect(page3.files.length).toBe(5) // Only 5 remaining
      expect(page3.hasMore).toBe(false)
      expect(page3.cursor).toBeUndefined()
    })

    it('should handle limit larger than total files', async () => {
      const result = await backend.list('files/', { limit: 100 })

      expect(result.files.length).toBe(25)
      expect(result.hasMore).toBe(false)
    })

    it('should include stats when includeMetadata is true', async () => {
      const result = await backend.list('files/', { limit: 5, includeMetadata: true })

      expect(result.stats).toBeDefined()
      expect(result.stats!.length).toBe(5)
      expect(result.stats![0].path).toBeDefined()
      expect(result.stats![0].size).toBeDefined()
      expect(result.stats![0].mtime).toBeInstanceOf(Date)
    })
  })

  // ===========================================================================
  // 11. Error Handling
  // ===========================================================================

  describe('Error handling', () => {
    it('should throw FileNotFoundError with correct path', async () => {
      try {
        await backend.read('missing/path/file.txt')
        expect.fail('Should have thrown FileNotFoundError')
      } catch (error) {
        expect(error).toBeInstanceOf(FileNotFoundError)
        expect((error as FileNotFoundError).message).toContain('missing/path/file.txt')
      }
    })

    it('should throw VersionMismatchError with expected and actual versions', async () => {
      const writeResult = await backend.write('test.txt', createTestData('content'))

      try {
        await backend.writeConditional('test.txt', createTestData('new'), 'wrong-version')
        expect.fail('Should have thrown VersionMismatchError')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionMismatchError)
        expect((error as VersionMismatchError).message).toContain('wrong-version')
        expect((error as VersionMismatchError).message).toContain(writeResult.etag)
      }
    })

    it('should throw FileExistsError with correct path', async () => {
      await backend.write('existing.txt', createTestData('content'))

      try {
        await backend.write('existing.txt', createTestData('new'), { ifNoneMatch: '*' })
        expect.fail('Should have thrown FileExistsError')
      } catch (error) {
        expect(error).toBeInstanceOf(FileExistsError)
        expect((error as FileExistsError).message).toContain('existing.txt')
      }
    })

    it('should throw DirectoryNotEmptyError with correct path', async () => {
      await backend.write('dir/file.txt', createTestData('content'))

      try {
        await backend.rmdir('dir')
        expect.fail('Should have thrown DirectoryNotEmptyError')
      } catch (error) {
        expect(error).toBeInstanceOf(DirectoryNotEmptyError)
        expect((error as DirectoryNotEmptyError).message).toContain('dir')
      }
    })

    it('should handle concurrent writes gracefully', async () => {
      // Multiple concurrent writes to same file should all succeed (last writer wins)
      const writes = Promise.all([
        backend.write('concurrent.txt', createTestData('write1')),
        backend.write('concurrent.txt', createTestData('write2')),
        backend.write('concurrent.txt', createTestData('write3')),
      ])

      await expect(writes).resolves.toBeDefined()

      // File should exist with one of the contents
      const content = decodeData(await backend.read('concurrent.txt'))
      expect(['write1', 'write2', 'write3']).toContain(content)
    })
  })

  // ===========================================================================
  // Append Operations
  // ===========================================================================

  describe('Append operations', () => {
    it('should append to existing file', async () => {
      await backend.write('log.txt', createTestData('Line 1\n'))
      await backend.append('log.txt', createTestData('Line 2\n'))

      expect(decodeData(await backend.read('log.txt'))).toBe('Line 1\nLine 2\n')
    })

    it('should create file if it does not exist', async () => {
      await backend.append('new-log.txt', createTestData('First line\n'))

      expect(decodeData(await backend.read('new-log.txt'))).toBe('First line\n')
    })

    it('should append multiple times', async () => {
      await backend.append('log.txt', createTestData('1'))
      await backend.append('log.txt', createTestData('2'))
      await backend.append('log.txt', createTestData('3'))

      expect(decodeData(await backend.read('log.txt'))).toBe('123')
    })

    it('should handle binary append', async () => {
      await backend.write('binary.dat', new Uint8Array([0x01, 0x02]))
      await backend.append('binary.dat', new Uint8Array([0x03, 0x04]))

      expect(await backend.read('binary.dat')).toEqual(new Uint8Array([0x01, 0x02, 0x03, 0x04]))
    })
  })

  // ===========================================================================
  // Pattern Matching in List
  // ===========================================================================

  describe('List with pattern matching', () => {
    beforeEach(async () => {
      await backend.write('data/file1.json', createTestData('{}'))
      await backend.write('data/file2.json', createTestData('{}'))
      await backend.write('data/file1.txt', createTestData('text'))
      await backend.write('data/image.png', createTestData('png'))
    })

    it('should filter by pattern (glob)', async () => {
      const result = await backend.list('data/', { pattern: '*.json' })

      expect(result.files).toContain('data/file1.json')
      expect(result.files).toContain('data/file2.json')
      expect(result.files).not.toContain('data/file1.txt')
      expect(result.files).not.toContain('data/image.png')
    })

    it('should filter by pattern with wildcard', async () => {
      const result = await backend.list('data/', { pattern: 'file*' })

      expect(result.files).toContain('data/file1.json')
      expect(result.files).toContain('data/file2.json')
      expect(result.files).toContain('data/file1.txt')
      expect(result.files).not.toContain('data/image.png')
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('Edge cases', () => {
    it('should handle paths with special characters', async () => {
      const path = 'data/file with spaces.txt'
      await backend.write(path, createTestData('content'))

      expect(await backend.exists(path)).toBe(true)
      expect(decodeData(await backend.read(path))).toBe('content')
    })

    it('should handle unicode in paths and content', async () => {
      const path = 'data/fichier-\u00e9\u00e0\u00fc.txt'
      const content = 'Caf\u00e9 \u4e2d\u6587 \ud83c\udf89'
      await backend.write(path, createTestData(content))

      expect(decodeData(await backend.read(path))).toBe(content)
    })

    it('should handle very long paths', async () => {
      const longPath = 'a/'.repeat(100) + 'file.txt'
      await backend.write(longPath, createTestData('content'))

      expect(await backend.exists(longPath)).toBe(true)
    })

    it('should handle large files', async () => {
      // 1MB of data
      const largeData = new Uint8Array(1024 * 1024)
      for (let i = 0; i < largeData.length; i++) {
        largeData[i] = i % 256
      }

      await backend.write('large.bin', largeData)

      const result = await backend.read('large.bin')
      expect(result.length).toBe(largeData.length)
      expect(result[0]).toBe(0)
      expect(result[1024]).toBe(0)
    })

    it('should normalize path separators', async () => {
      // Both forward slashes should work consistently
      await backend.write('a/b/c.txt', createTestData('content'))

      expect(await backend.exists('a/b/c.txt')).toBe(true)
    })

    it('should handle empty prefix in list', async () => {
      await backend.write('file1.txt', createTestData('1'))
      await backend.write('file2.txt', createTestData('2'))

      const result = await backend.list('')

      expect(result.files).toContain('file1.txt')
      expect(result.files).toContain('file2.txt')
    })

    it('should handle trailing slash in list prefix', async () => {
      await backend.write('dir/file.txt', createTestData('content'))

      const result1 = await backend.list('dir/')
      const result2 = await backend.list('dir')

      // Both should work, potentially with same results
      expect(result1.files.length).toBeGreaterThan(0)
    })
  })
})
