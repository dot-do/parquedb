/**
 * FsBackend Tests
 *
 * Tests for the Node.js filesystem implementation of StorageBackend.
 * Uses real temporary directories to test actual filesystem behavior.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm, writeFile, mkdir, readFile, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { FsBackend, PathTraversalError } from '../../src/storage/FsBackend'
import { FileNotFoundError, VersionMismatchError, FileExistsError, DirectoryNotEmptyError } from '../../src/storage/MemoryBackend'

describe('FsBackend', () => {
  let tempDir: string
  let backend: FsBackend

  beforeEach(async () => {
    // Create a unique temp directory for each test
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-test-'))
    backend = new FsBackend(tempDir)
  })

  afterEach(async () => {
    // Clean up temp directory after each test
    await rm(tempDir, { recursive: true, force: true })
  })

  // ===========================================================================
  // Constructor and Type
  // ===========================================================================

  describe('constructor', () => {
    it('should store the root path', () => {
      expect(backend.rootPath).toBe(tempDir)
    })

    it('should have type "fs"', () => {
      expect(backend.type).toBe('fs')
    })
  })

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  describe('read', () => {
    it('should read file contents as Uint8Array', async () => {
      const testData = 'Hello, ParqueDB!'
      await writeFile(join(tempDir, 'test.txt'), testData)

      const result = await backend.read('test.txt')

      expect(result).toBeInstanceOf(Uint8Array)
      expect(new TextDecoder().decode(result)).toBe(testData)
    })

    it('should read binary data correctly', async () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd])
      await writeFile(join(tempDir, 'binary.dat'), binaryData)

      const result = await backend.read('binary.dat')

      expect(result).toEqual(binaryData)
    })

    it('should read files in nested directories', async () => {
      await mkdir(join(tempDir, 'a', 'b', 'c'), { recursive: true })
      await writeFile(join(tempDir, 'a', 'b', 'c', 'nested.txt'), 'nested content')

      const result = await backend.read('a/b/c/nested.txt')

      expect(new TextDecoder().decode(result)).toBe('nested content')
    })

    it('should throw FileNotFoundError for non-existent file', async () => {
      await expect(backend.read('nonexistent.txt')).rejects.toThrow(FileNotFoundError)
    })

    it('should throw FileNotFoundError with correct path', async () => {
      await expect(backend.read('missing/file.txt')).rejects.toThrow('missing/file.txt')
    })

    it('should handle empty files', async () => {
      await writeFile(join(tempDir, 'empty.txt'), '')

      const result = await backend.read('empty.txt')

      expect(result).toBeInstanceOf(Uint8Array)
      expect(result.length).toBe(0)
    })

    it('should read large files (1MB)', async () => {
      const largeData = new Uint8Array(1024 * 1024).fill(0x42)
      await writeFile(join(tempDir, 'large.bin'), largeData)

      const result = await backend.read('large.bin')

      expect(result.length).toBe(largeData.length)
      expect(result[0]).toBe(0x42)
      expect(result[result.length - 1]).toBe(0x42)
    })
  })

  // ===========================================================================
  // Byte Range Reads (for Parquet partial file access)
  // ===========================================================================

  describe('readRange', () => {
    it('should read specified byte range', async () => {
      const data = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
      await writeFile(join(tempDir, 'alphabet.txt'), data)

      // Read bytes 5-9 (FGHIJ)
      const result = await backend.readRange('alphabet.txt', 5, 10)

      expect(new TextDecoder().decode(result)).toBe('FGHIJ')
    })

    it('should read from start when start is 0', async () => {
      const data = '0123456789'
      await writeFile(join(tempDir, 'digits.txt'), data)

      const result = await backend.readRange('digits.txt', 0, 5)

      expect(new TextDecoder().decode(result)).toBe('01234')
    })

    it('should read to end of file when end exceeds file size', async () => {
      const data = 'short'
      await writeFile(join(tempDir, 'short.txt'), data)

      const result = await backend.readRange('short.txt', 2, 1000)

      expect(new TextDecoder().decode(result)).toBe('ort')
    })

    it('should return empty array when start equals end', async () => {
      await writeFile(join(tempDir, 'file.txt'), 'content')

      const result = await backend.readRange('file.txt', 5, 5)

      expect(result.length).toBe(0)
    })

    it('should throw for non-existent file', async () => {
      await expect(backend.readRange('missing.txt', 0, 10)).rejects.toThrow(FileNotFoundError)
    })

    it('should handle reading last few bytes (Parquet footer pattern)', async () => {
      // Parquet files have metadata in the last bytes
      const data = new Uint8Array(1000)
      data.set(new TextEncoder().encode('PAR1'), 996) // Footer magic at end
      await writeFile(join(tempDir, 'data.parquet'), data)

      const result = await backend.readRange('data.parquet', 996, 1000)

      expect(new TextDecoder().decode(result)).toBe('PAR1')
    })

    it('should read binary range correctly', async () => {
      const binary = new Uint8Array([0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77])
      await writeFile(join(tempDir, 'binary.dat'), binary)

      const result = await backend.readRange('binary.dat', 2, 6)

      expect(result).toEqual(new Uint8Array([0x22, 0x33, 0x44, 0x55]))
    })

    it('should throw when start is negative', async () => {
      await writeFile(join(tempDir, 'file.txt'), 'content')

      await expect(backend.readRange('file.txt', -1, 5)).rejects.toThrow()
    })

    it('should throw when start > end', async () => {
      await writeFile(join(tempDir, 'file.txt'), 'content')

      await expect(backend.readRange('file.txt', 10, 5)).rejects.toThrow()
    })
  })

  // ===========================================================================
  // Exists
  // ===========================================================================

  describe('exists', () => {
    it('should return true for existing file', async () => {
      await writeFile(join(tempDir, 'exists.txt'), 'content')

      const result = await backend.exists('exists.txt')

      expect(result).toBe(true)
    })

    it('should return false for non-existent file', async () => {
      const result = await backend.exists('nonexistent.txt')

      expect(result).toBe(false)
    })

    it('should return true for existing directory', async () => {
      await mkdir(join(tempDir, 'subdir'))

      const result = await backend.exists('subdir')

      expect(result).toBe(true)
    })

    it('should return false for non-existent nested path', async () => {
      const result = await backend.exists('a/b/c/deep.txt')

      expect(result).toBe(false)
    })

    it('should work with nested file paths', async () => {
      await mkdir(join(tempDir, 'nested'), { recursive: true })
      await writeFile(join(tempDir, 'nested', 'file.txt'), 'data')

      expect(await backend.exists('nested/file.txt')).toBe(true)
      expect(await backend.exists('nested/other.txt')).toBe(false)
    })
  })

  // ===========================================================================
  // Stat
  // ===========================================================================

  describe('stat', () => {
    it('should return null for non-existent file', async () => {
      const result = await backend.stat('nonexistent.txt')

      expect(result).toBeNull()
    })

    it('should return FileStat with correct size', async () => {
      const data = 'Hello, World!'
      await writeFile(join(tempDir, 'hello.txt'), data)

      const result = await backend.stat('hello.txt')

      expect(result).not.toBeNull()
      expect(result!.size).toBe(data.length)
    })

    it('should return FileStat with mtime as Date', async () => {
      await writeFile(join(tempDir, 'timed.txt'), 'data')

      const result = await backend.stat('timed.txt')

      expect(result).not.toBeNull()
      expect(result!.mtime).toBeInstanceOf(Date)
      // Allow 100ms tolerance for filesystem timing differences
      expect(result!.mtime.getTime()).toBeLessThanOrEqual(Date.now() + 100)
      expect(result!.mtime.getTime()).toBeGreaterThan(Date.now() - 10000)
    })

    it('should return correct path in FileStat', async () => {
      await mkdir(join(tempDir, 'dir'))
      await writeFile(join(tempDir, 'dir', 'file.txt'), 'content')

      const result = await backend.stat('dir/file.txt')

      expect(result).not.toBeNull()
      expect(result!.path).toBe('dir/file.txt')
    })

    it('should set isDirectory to false for files', async () => {
      await writeFile(join(tempDir, 'file.txt'), 'content')

      const result = await backend.stat('file.txt')

      expect(result).not.toBeNull()
      expect(result!.isDirectory).toBe(false)
    })

    it('should set isDirectory to true for directories', async () => {
      await mkdir(join(tempDir, 'mydir'))

      const result = await backend.stat('mydir')

      expect(result).not.toBeNull()
      expect(result!.isDirectory).toBe(true)
    })

    it('should include etag based on mtime', async () => {
      await writeFile(join(tempDir, 'versioned.txt'), 'data')

      const result = await backend.stat('versioned.txt')

      expect(result).not.toBeNull()
      expect(result!.etag).toBeDefined()
      expect(typeof result!.etag).toBe('string')
      expect(result!.etag!.length).toBeGreaterThan(0)
    })

    it('should return different etag after file modification', async () => {
      const filePath = join(tempDir, 'changing.txt')
      await writeFile(filePath, 'initial')

      const stat1 = await backend.stat('changing.txt')

      // Write different content to the file (etag is based on content hash + size + mtime)
      // Using different content length ensures the etag will be different
      await writeFile(filePath, 'modified content with different length')

      const stat2 = await backend.stat('changing.txt')

      expect(stat1!.etag).not.toBe(stat2!.etag)
    })
  })

  // ===========================================================================
  // Write Operations
  // ===========================================================================

  describe('write', () => {
    it('should write data to file', async () => {
      const data = new TextEncoder().encode('Hello, ParqueDB!')

      await backend.write('output.txt', data)

      const content = await readFile(join(tempDir, 'output.txt'), 'utf-8')
      expect(content).toBe('Hello, ParqueDB!')
    })

    it('should return WriteResult with size', async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])

      const result = await backend.write('data.bin', data)

      expect(result.size).toBe(5)
    })

    it('should return WriteResult with etag', async () => {
      const data = new TextEncoder().encode('data')

      const result = await backend.write('file.txt', data)

      expect(result.etag).toBeDefined()
      expect(typeof result.etag).toBe('string')
    })

    it('should create parent directories automatically', async () => {
      const data = new TextEncoder().encode('nested')

      await backend.write('a/b/c/deep.txt', data)

      const content = await readFile(join(tempDir, 'a', 'b', 'c', 'deep.txt'), 'utf-8')
      expect(content).toBe('nested')
    })

    it('should overwrite existing file', async () => {
      await writeFile(join(tempDir, 'existing.txt'), 'old content')
      const newData = new TextEncoder().encode('new content')

      await backend.write('existing.txt', newData)

      const content = await readFile(join(tempDir, 'existing.txt'), 'utf-8')
      expect(content).toBe('new content')
    })

    it('should write binary data correctly', async () => {
      const binary = new Uint8Array([0x00, 0xff, 0x80, 0x7f])

      await backend.write('binary.dat', binary)

      const content = await readFile(join(tempDir, 'binary.dat'))
      expect(new Uint8Array(content)).toEqual(binary)
    })

    it('should write empty file', async () => {
      const empty = new Uint8Array(0)

      const result = await backend.write('empty.txt', empty)

      expect(result.size).toBe(0)
      const stats = await stat(join(tempDir, 'empty.txt'))
      expect(stats.size).toBe(0)
    })

    it('should fail with ifNoneMatch when file exists', async () => {
      await writeFile(join(tempDir, 'existing.txt'), 'content')
      const data = new TextEncoder().encode('new')

      await expect(
        backend.write('existing.txt', data, { ifNoneMatch: '*' })
      ).rejects.toThrow(FileExistsError)
    })

    it('should succeed with ifNoneMatch when file does not exist', async () => {
      const data = new TextEncoder().encode('new')

      const result = await backend.write('new.txt', data, { ifNoneMatch: '*' })

      expect(result.size).toBe(3)
    })
  })

  // ===========================================================================
  // Atomic Writes
  // ===========================================================================

  describe('writeAtomic', () => {
    it('should write data atomically', async () => {
      const data = new TextEncoder().encode('atomic content')

      await backend.writeAtomic('atomic.txt', data)

      const content = await readFile(join(tempDir, 'atomic.txt'), 'utf-8')
      expect(content).toBe('atomic content')
    })

    it('should not leave .tmp file after successful write', async () => {
      const data = new TextEncoder().encode('data')

      await backend.writeAtomic('file.txt', data)

      expect(await backend.exists('file.txt.tmp')).toBe(false)
      expect(await backend.exists('file.txt')).toBe(true)
    })

    it('should create parent directories', async () => {
      const data = new TextEncoder().encode('nested atomic')

      await backend.writeAtomic('x/y/z/atomic.txt', data)

      const content = await readFile(join(tempDir, 'x', 'y', 'z', 'atomic.txt'), 'utf-8')
      expect(content).toBe('nested atomic')
    })

    it('should return WriteResult with etag and size', async () => {
      const data = new Uint8Array([1, 2, 3])

      const result = await backend.writeAtomic('file.bin', data)

      expect(result.size).toBe(3)
      expect(result.etag).toBeDefined()
    })

    it('should atomically replace existing file', async () => {
      // Write initial file
      await writeFile(join(tempDir, 'replace.txt'), 'original')

      // Atomically replace
      const newData = new TextEncoder().encode('replaced')
      await backend.writeAtomic('replace.txt', newData)

      const content = await readFile(join(tempDir, 'replace.txt'), 'utf-8')
      expect(content).toBe('replaced')
    })

    it('should use unique temp file names to avoid conflicts', async () => {
      // Write multiple files concurrently
      const writes = Array.from({ length: 10 }, (_, i) =>
        backend.writeAtomic(`concurrent-${i}.txt`, new TextEncoder().encode(`data-${i}`))
      )

      await Promise.all(writes)

      // All files should be written correctly
      for (let i = 0; i < 10; i++) {
        const content = await readFile(join(tempDir, `concurrent-${i}.txt`), 'utf-8')
        expect(content).toBe(`data-${i}`)
      }
    })
  })

  // ===========================================================================
  // Append
  // ===========================================================================

  describe('append', () => {
    it('should append to existing file', async () => {
      await writeFile(join(tempDir, 'log.txt'), 'line1\n')
      const appendData = new TextEncoder().encode('line2\n')

      await backend.append('log.txt', appendData)

      const content = await readFile(join(tempDir, 'log.txt'), 'utf-8')
      expect(content).toBe('line1\nline2\n')
    })

    it('should create file if it does not exist', async () => {
      const data = new TextEncoder().encode('first line\n')

      await backend.append('newlog.txt', data)

      const content = await readFile(join(tempDir, 'newlog.txt'), 'utf-8')
      expect(content).toBe('first line\n')
    })

    it('should append binary data correctly', async () => {
      await writeFile(join(tempDir, 'binary.dat'), new Uint8Array([1, 2, 3]))
      const appendData = new Uint8Array([4, 5, 6])

      await backend.append('binary.dat', appendData)

      const content = await readFile(join(tempDir, 'binary.dat'))
      expect(new Uint8Array(content)).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6]))
    })

    it('should handle multiple sequential appends', async () => {
      for (let i = 1; i <= 5; i++) {
        await backend.append('sequential.txt', new TextEncoder().encode(`${i}\n`))
      }

      const content = await readFile(join(tempDir, 'sequential.txt'), 'utf-8')
      expect(content).toBe('1\n2\n3\n4\n5\n')
    })

    it('should create parent directories if needed', async () => {
      const data = new TextEncoder().encode('nested append')

      await backend.append('events/log.txt', data)

      const content = await readFile(join(tempDir, 'events', 'log.txt'), 'utf-8')
      expect(content).toBe('nested append')
    })
  })

  // ===========================================================================
  // Delete
  // ===========================================================================

  describe('delete', () => {
    it('should delete existing file and return true', async () => {
      await writeFile(join(tempDir, 'todelete.txt'), 'content')

      const result = await backend.delete('todelete.txt')

      expect(result).toBe(true)
      expect(await backend.exists('todelete.txt')).toBe(false)
    })

    it('should return false for non-existent file', async () => {
      const result = await backend.delete('nonexistent.txt')

      expect(result).toBe(false)
    })

    it('should delete files in nested directories', async () => {
      await mkdir(join(tempDir, 'deep', 'path'), { recursive: true })
      await writeFile(join(tempDir, 'deep', 'path', 'file.txt'), 'data')

      const result = await backend.delete('deep/path/file.txt')

      expect(result).toBe(true)
      expect(await backend.exists('deep/path/file.txt')).toBe(false)
    })

    it('should not delete directories', async () => {
      await mkdir(join(tempDir, 'mydir'))

      // Should either return false or throw, not delete the directory
      const result = await backend.delete('mydir')

      expect(result).toBe(false)
      expect(await backend.exists('mydir')).toBe(true)
    })
  })

  // ===========================================================================
  // Delete Prefix
  // ===========================================================================

  describe('deletePrefix', () => {
    it('should delete all files with given prefix', async () => {
      await mkdir(join(tempDir, 'data'))
      await writeFile(join(tempDir, 'data', 'file1.txt'), 'a')
      await writeFile(join(tempDir, 'data', 'file2.txt'), 'b')
      await writeFile(join(tempDir, 'data', 'file3.txt'), 'c')
      await writeFile(join(tempDir, 'other.txt'), 'd')

      const count = await backend.deletePrefix('data/')

      expect(count).toBe(3)
      expect(await backend.exists('data/file1.txt')).toBe(false)
      expect(await backend.exists('data/file2.txt')).toBe(false)
      expect(await backend.exists('data/file3.txt')).toBe(false)
      expect(await backend.exists('other.txt')).toBe(true)
    })

    it('should return 0 when no files match prefix', async () => {
      await writeFile(join(tempDir, 'file.txt'), 'content')

      const count = await backend.deletePrefix('nonexistent/')

      expect(count).toBe(0)
    })

    it('should delete nested files with prefix', async () => {
      await mkdir(join(tempDir, 'prefix', 'nested', 'deep'), { recursive: true })
      await writeFile(join(tempDir, 'prefix', 'a.txt'), '1')
      await writeFile(join(tempDir, 'prefix', 'nested', 'b.txt'), '2')
      await writeFile(join(tempDir, 'prefix', 'nested', 'deep', 'c.txt'), '3')

      const count = await backend.deletePrefix('prefix/')

      expect(count).toBe(3)
    })

    it('should handle prefix matching correctly', async () => {
      await writeFile(join(tempDir, 'test.txt'), 'a')
      await writeFile(join(tempDir, 'testing.txt'), 'b')
      await writeFile(join(tempDir, 'other.txt'), 'c')

      const count = await backend.deletePrefix('test')

      expect(count).toBe(2) // test.txt and testing.txt
      expect(await backend.exists('other.txt')).toBe(true)
    })
  })

  // ===========================================================================
  // Directory Operations
  // ===========================================================================

  describe('mkdir', () => {
    it('should create directory', async () => {
      await backend.mkdir('newdir')

      const stats = await stat(join(tempDir, 'newdir'))
      expect(stats.isDirectory()).toBe(true)
    })

    it('should create nested directories recursively', async () => {
      await backend.mkdir('a/b/c/d')

      const stats = await stat(join(tempDir, 'a', 'b', 'c', 'd'))
      expect(stats.isDirectory()).toBe(true)
    })

    it('should succeed if directory already exists', async () => {
      await mkdir(join(tempDir, 'existing'))

      await expect(backend.mkdir('existing')).resolves.not.toThrow()
    })

    it('should create intermediate directories', async () => {
      await backend.mkdir('x/y/z')

      expect(await backend.exists('x')).toBe(true)
      expect(await backend.exists('x/y')).toBe(true)
      expect(await backend.exists('x/y/z')).toBe(true)
    })
  })

  describe('rmdir', () => {
    it('should remove empty directory', async () => {
      await mkdir(join(tempDir, 'emptydir'))

      await backend.rmdir('emptydir')

      expect(await backend.exists('emptydir')).toBe(false)
    })

    it('should throw DirectoryNotEmptyError for non-empty directory without recursive', async () => {
      await mkdir(join(tempDir, 'nonempty'))
      await writeFile(join(tempDir, 'nonempty', 'file.txt'), 'content')

      await expect(backend.rmdir('nonempty')).rejects.toThrow(DirectoryNotEmptyError)
    })

    it('should remove non-empty directory with recursive option', async () => {
      await mkdir(join(tempDir, 'full', 'sub'), { recursive: true })
      await writeFile(join(tempDir, 'full', 'a.txt'), 'a')
      await writeFile(join(tempDir, 'full', 'sub', 'b.txt'), 'b')

      await backend.rmdir('full', { recursive: true })

      expect(await backend.exists('full')).toBe(false)
    })

    it('should throw for non-existent directory', async () => {
      await expect(backend.rmdir('nonexistent')).rejects.toThrow()
    })
  })

  // ===========================================================================
  // List Operations
  // ===========================================================================

  describe('list', () => {
    beforeEach(async () => {
      // Set up test directory structure
      await mkdir(join(tempDir, 'data', 'users'), { recursive: true })
      await mkdir(join(tempDir, 'data', 'posts'), { recursive: true })
      await writeFile(join(tempDir, 'data', 'users', 'user1.json'), '{}')
      await writeFile(join(tempDir, 'data', 'users', 'user2.json'), '{}')
      await writeFile(join(tempDir, 'data', 'posts', 'post1.json'), '{}')
      await writeFile(join(tempDir, 'root.txt'), 'root')
    })

    it('should list all files with given prefix', async () => {
      const result = await backend.list('data/users/')

      expect(result.files).toContain('data/users/user1.json')
      expect(result.files).toContain('data/users/user2.json')
      expect(result.files).not.toContain('data/posts/post1.json')
    })

    it('should return hasMore: false when all results fit', async () => {
      const result = await backend.list('data/users/')

      expect(result.hasMore).toBe(false)
    })

    it('should respect limit option', async () => {
      const result = await backend.list('data/', { limit: 1 })

      expect(result.files.length).toBe(1)
      expect(result.hasMore).toBe(true)
    })

    it('should support pagination with cursor', async () => {
      const first = await backend.list('data/', { limit: 2 })
      expect(first.cursor).toBeDefined()

      const second = await backend.list('data/', { limit: 2, cursor: first.cursor })

      // Should return different files
      expect(second.files).not.toEqual(first.files)
    })

    it('should list with delimiter for directory grouping', async () => {
      const result = await backend.list('data/', { delimiter: '/' })

      expect(result.prefixes).toBeDefined()
      expect(result.prefixes).toContain('data/users/')
      expect(result.prefixes).toContain('data/posts/')
      // Should not list individual files when using delimiter
      expect(result.files).not.toContain('data/users/user1.json')
    })

    it('should return empty result for non-matching prefix', async () => {
      const result = await backend.list('nonexistent/')

      expect(result.files).toEqual([])
      expect(result.hasMore).toBe(false)
    })

    it('should list files at root level', async () => {
      const result = await backend.list('')

      expect(result.files).toContain('root.txt')
    })

    it('should include stats when includeMetadata is true', async () => {
      const result = await backend.list('data/users/', { includeMetadata: true })

      expect(result.stats).toBeDefined()
      expect(result.stats!.length).toBe(result.files.length)
      expect(result.stats![0].size).toBeGreaterThanOrEqual(0)
    })

    it('should match files with pattern option', async () => {
      const result = await backend.list('data/', { pattern: '*.json' })

      for (const file of result.files) {
        expect(file.endsWith('.json')).toBe(true)
      }
    })
  })

  // ===========================================================================
  // Copy Operations
  // ===========================================================================

  describe('copy', () => {
    it('should copy file to new location', async () => {
      await writeFile(join(tempDir, 'source.txt'), 'copy me')

      await backend.copy('source.txt', 'dest.txt')

      const sourceContent = await readFile(join(tempDir, 'source.txt'), 'utf-8')
      const destContent = await readFile(join(tempDir, 'dest.txt'), 'utf-8')
      expect(sourceContent).toBe('copy me')
      expect(destContent).toBe('copy me')
    })

    it('should create parent directories for destination', async () => {
      await writeFile(join(tempDir, 'original.txt'), 'data')

      await backend.copy('original.txt', 'new/path/copy.txt')

      const content = await readFile(join(tempDir, 'new', 'path', 'copy.txt'), 'utf-8')
      expect(content).toBe('data')
    })

    it('should overwrite existing destination', async () => {
      await writeFile(join(tempDir, 'src.txt'), 'new')
      await writeFile(join(tempDir, 'dst.txt'), 'old')

      await backend.copy('src.txt', 'dst.txt')

      const content = await readFile(join(tempDir, 'dst.txt'), 'utf-8')
      expect(content).toBe('new')
    })

    it('should throw FileNotFoundError for non-existent source', async () => {
      await expect(backend.copy('missing.txt', 'dest.txt')).rejects.toThrow(FileNotFoundError)
    })

    it('should preserve binary content', async () => {
      const binary = new Uint8Array([0x00, 0xff, 0x80, 0x40])
      await writeFile(join(tempDir, 'binary.dat'), binary)

      await backend.copy('binary.dat', 'binary-copy.dat')

      const content = await readFile(join(tempDir, 'binary-copy.dat'))
      expect(new Uint8Array(content)).toEqual(binary)
    })
  })

  // ===========================================================================
  // Move Operations
  // ===========================================================================

  describe('move', () => {
    it('should move file to new location', async () => {
      await writeFile(join(tempDir, 'tomove.txt'), 'moving')

      await backend.move('tomove.txt', 'moved.txt')

      expect(await backend.exists('tomove.txt')).toBe(false)
      const content = await readFile(join(tempDir, 'moved.txt'), 'utf-8')
      expect(content).toBe('moving')
    })

    it('should create parent directories for destination', async () => {
      await writeFile(join(tempDir, 'src.txt'), 'data')

      await backend.move('src.txt', 'nested/path/dst.txt')

      expect(await backend.exists('src.txt')).toBe(false)
      const content = await readFile(join(tempDir, 'nested', 'path', 'dst.txt'), 'utf-8')
      expect(content).toBe('data')
    })

    it('should overwrite existing destination', async () => {
      await writeFile(join(tempDir, 'source.txt'), 'new content')
      await writeFile(join(tempDir, 'target.txt'), 'old content')

      await backend.move('source.txt', 'target.txt')

      expect(await backend.exists('source.txt')).toBe(false)
      const content = await readFile(join(tempDir, 'target.txt'), 'utf-8')
      expect(content).toBe('new content')
    })

    it('should throw FileNotFoundError for non-existent source', async () => {
      await expect(backend.move('missing.txt', 'dest.txt')).rejects.toThrow(FileNotFoundError)
    })

    it('should rename file in same directory', async () => {
      await writeFile(join(tempDir, 'old-name.txt'), 'content')

      await backend.move('old-name.txt', 'new-name.txt')

      expect(await backend.exists('old-name.txt')).toBe(false)
      expect(await backend.exists('new-name.txt')).toBe(true)
    })
  })

  // ===========================================================================
  // Conditional Writes (using mtime as version)
  // ===========================================================================

  describe('writeConditional', () => {
    it('should write when expectedVersion is null and file does not exist', async () => {
      const data = new TextEncoder().encode('new file')

      const result = await backend.writeConditional('new.txt', data, null)

      expect(result.size).toBe(8)
      const content = await readFile(join(tempDir, 'new.txt'), 'utf-8')
      expect(content).toBe('new file')
    })

    it('should fail when expectedVersion is null but file exists', async () => {
      await writeFile(join(tempDir, 'existing.txt'), 'content')
      const data = new TextEncoder().encode('new')

      await expect(
        backend.writeConditional('existing.txt', data, null)
      ).rejects.toThrow(VersionMismatchError)
    })

    it('should write when expectedVersion matches current etag', async () => {
      await writeFile(join(tempDir, 'versioned.txt'), 'original')
      const currentStat = await backend.stat('versioned.txt')
      const data = new TextEncoder().encode('updated')

      const result = await backend.writeConditional('versioned.txt', data, currentStat!.etag!)

      expect(result.size).toBe(7)
      const content = await readFile(join(tempDir, 'versioned.txt'), 'utf-8')
      expect(content).toBe('updated')
    })

    it('should fail when expectedVersion does not match', async () => {
      await writeFile(join(tempDir, 'versioned.txt'), 'content')
      const data = new TextEncoder().encode('update')

      await expect(
        backend.writeConditional('versioned.txt', data, 'wrong-version')
      ).rejects.toThrow(VersionMismatchError)
    })

    it('should return new etag after successful write', async () => {
      await writeFile(join(tempDir, 'file.txt'), 'initial')
      const stat1 = await backend.stat('file.txt')

      // Write different content to ensure etag changes
      const result = await backend.writeConditional(
        'file.txt',
        new TextEncoder().encode('updated content with different length'),
        stat1!.etag!
      )

      expect(result.etag).not.toBe(stat1!.etag)
    })

    it('should handle concurrent writes with version checking', async () => {
      await writeFile(join(tempDir, 'race.txt'), 'initial')
      const stat1 = await backend.stat('race.txt')

      // First write succeeds
      await backend.writeConditional('race.txt', new TextEncoder().encode('first'), stat1!.etag!)

      // Second write with old version fails
      await expect(
        backend.writeConditional('race.txt', new TextEncoder().encode('second'), stat1!.etag!)
      ).rejects.toThrow(VersionMismatchError)
    })

    it('should handle true parallel concurrent writes with file locking (TOCTOU prevention)', async () => {
      // This test verifies that the TOCTOU race condition is fixed by using file locking.
      // Multiple writes are launched in parallel with the same expected version.
      // Only one should succeed; all others should fail with VersionMismatchError.
      await writeFile(join(tempDir, 'parallel-race.txt'), 'initial')
      const initialStat = await backend.stat('parallel-race.txt')
      const initialEtag = initialStat!.etag!

      // Launch multiple parallel writes all expecting the same initial etag
      const numConcurrentWrites = 10
      const writePromises = Array.from({ length: numConcurrentWrites }, (_, i) =>
        backend.writeConditional(
          'parallel-race.txt',
          new TextEncoder().encode(`writer-${i}`),
          initialEtag
        )
          .then((result) => ({ status: 'fulfilled' as const, value: result, writer: i }))
          .catch((error) => ({ status: 'rejected' as const, error, writer: i }))
      )

      const results = await Promise.all(writePromises)

      // Count successes and failures
      const successes = results.filter(r => r.status === 'fulfilled')
      const failures = results.filter(r => r.status === 'rejected')

      // Exactly one write should succeed
      expect(successes.length).toBe(1)

      // All other writes should fail with VersionMismatchError or concurrent-write error
      expect(failures.length).toBe(numConcurrentWrites - 1)
      for (const failure of failures) {
        expect(failure.status).toBe('rejected')
        if (failure.status === 'rejected') {
          // Either version mismatch or concurrent write detection
          expect(failure.error).toBeDefined()
        }
      }

      // Verify the file contains exactly one winner's content
      const finalContent = await readFile(join(tempDir, 'parallel-race.txt'), 'utf-8')
      const successfulWriter = successes[0]
      if (successfulWriter.status === 'fulfilled') {
        expect(finalContent).toBe(`writer-${successfulWriter.writer}`)
      }
    })

    it('should prevent data corruption with rapid successive writes', async () => {
      // This test verifies that file locking prevents interleaving of writes
      // which would cause data corruption
      await writeFile(join(tempDir, 'rapid.txt'), 'start')
      let currentStat = await backend.stat('rapid.txt')

      // Perform a sequence of writes, each checking version before writing
      for (let i = 0; i < 5; i++) {
        const result = await backend.writeConditional(
          'rapid.txt',
          new TextEncoder().encode(`iteration-${i}`),
          currentStat!.etag!
        )
        // Update our knowledge of current etag for next iteration
        currentStat = await backend.stat('rapid.txt')
        expect(currentStat!.etag).toBe(result.etag)
      }

      const finalContent = await readFile(join(tempDir, 'rapid.txt'), 'utf-8')
      expect(finalContent).toBe('iteration-4')
    })
  })

  // ===========================================================================
  // Path Traversal Prevention (Security)
  // ===========================================================================

  describe('path traversal prevention', () => {
    it('should reject paths with ../', async () => {
      await expect(backend.read('../outside.txt')).rejects.toThrow(PathTraversalError)
    })

    it('should reject paths with /..', async () => {
      await expect(backend.read('subdir/../../../etc/passwd')).rejects.toThrow(PathTraversalError)
    })

    it('should reject absolute paths', async () => {
      await expect(backend.read('/etc/passwd')).rejects.toThrow(PathTraversalError)
    })

    it('should reject paths that resolve outside root', async () => {
      await expect(backend.write('a/b/../../../../../../tmp/evil.txt', new Uint8Array([]))).rejects.toThrow(PathTraversalError)
    })

    it('should reject .. in middle of path', async () => {
      await expect(backend.exists('data/../../../etc/shadow')).rejects.toThrow(PathTraversalError)
    })

    it('should allow safe paths that contain "dot" but not traversal', async () => {
      const data = new TextEncoder().encode('safe')
      await backend.write('file.with.dots.txt', data)

      const content = await readFile(join(tempDir, 'file.with.dots.txt'), 'utf-8')
      expect(content).toBe('safe')
    })

    it('should allow hidden files starting with dot', async () => {
      const data = new TextEncoder().encode('hidden')
      await backend.write('.hidden', data)

      const content = await readFile(join(tempDir, '.hidden'), 'utf-8')
      expect(content).toBe('hidden')
    })

    it('should reject paths with null bytes', async () => {
      await expect(backend.read('file\x00.txt')).rejects.toThrow()
    })

    it('should apply traversal prevention to all methods', async () => {
      const maliciousPath = '../../../etc/passwd'
      const data = new Uint8Array([])

      await expect(backend.read(maliciousPath)).rejects.toThrow(PathTraversalError)
      await expect(backend.readRange(maliciousPath, 0, 10)).rejects.toThrow(PathTraversalError)
      await expect(backend.write(maliciousPath, data)).rejects.toThrow(PathTraversalError)
      await expect(backend.writeAtomic(maliciousPath, data)).rejects.toThrow(PathTraversalError)
      await expect(backend.append(maliciousPath, data)).rejects.toThrow(PathTraversalError)
      await expect(backend.delete(maliciousPath)).rejects.toThrow(PathTraversalError)
      await expect(backend.exists(maliciousPath)).rejects.toThrow(PathTraversalError)
      await expect(backend.stat(maliciousPath)).rejects.toThrow(PathTraversalError)
      await expect(backend.copy(maliciousPath, 'safe.txt')).rejects.toThrow(PathTraversalError)
      await expect(backend.copy('safe.txt', maliciousPath)).rejects.toThrow(PathTraversalError)
      await expect(backend.move(maliciousPath, 'safe.txt')).rejects.toThrow(PathTraversalError)
      await expect(backend.move('safe.txt', maliciousPath)).rejects.toThrow(PathTraversalError)
      await expect(backend.mkdir(maliciousPath)).rejects.toThrow(PathTraversalError)
      await expect(backend.rmdir(maliciousPath)).rejects.toThrow(PathTraversalError)
      await expect(backend.list(maliciousPath)).rejects.toThrow(PathTraversalError)
      await expect(backend.deletePrefix(maliciousPath)).rejects.toThrow(PathTraversalError)
    })

    it('should reject encoded path traversal attempts', async () => {
      // URL-encoded ..
      await expect(backend.read('%2e%2e/outside.txt')).rejects.toThrow(PathTraversalError)
      await expect(backend.read('..%2f..%2f..%2fetc/passwd')).rejects.toThrow(PathTraversalError)
    })
  })

  // ===========================================================================
  // Error Handling
  // ===========================================================================

  describe('error handling', () => {
    it('should throw FileNotFoundError (ENOENT equivalent) for missing files', async () => {
      try {
        await backend.read('nonexistent.txt')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(FileNotFoundError)
        expect((error as Error).message).toContain('nonexistent.txt')
      }
    })

    it('should throw FileExistsError when creating file that exists with ifNoneMatch', async () => {
      await writeFile(join(tempDir, 'exists.txt'), 'content')

      try {
        await backend.write('exists.txt', new TextEncoder().encode('new'), { ifNoneMatch: '*' })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(FileExistsError)
      }
    })

    it('should throw DirectoryNotEmptyError when removing non-empty dir without recursive', async () => {
      await mkdir(join(tempDir, 'nonempty'))
      await writeFile(join(tempDir, 'nonempty', 'file.txt'), 'content')

      try {
        await backend.rmdir('nonempty')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(DirectoryNotEmptyError)
      }
    })

    it('should throw VersionMismatchError on conditional write failure', async () => {
      await writeFile(join(tempDir, 'versioned.txt'), 'content')

      try {
        await backend.writeConditional('versioned.txt', new TextEncoder().encode('new'), 'bad-version')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(VersionMismatchError)
      }
    })

    it('should handle permission errors gracefully', async () => {
      // This test may not work on all systems, but attempts to test permission handling
      // Skip on Windows where chmod behaves differently
      if (process.platform === 'win32') return

      await mkdir(join(tempDir, 'readonly'))
      await writeFile(join(tempDir, 'readonly', 'file.txt'), 'content')

      // Make directory read-only
      const { chmod } = await import('node:fs/promises')
      await chmod(join(tempDir, 'readonly'), 0o444)

      try {
        await backend.write('readonly/newfile.txt', new TextEncoder().encode('test'))
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeDefined()
      } finally {
        // Restore permissions for cleanup
        await chmod(join(tempDir, 'readonly'), 0o755)
      }
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('edge cases', () => {
    it('should handle files with special characters in name', async () => {
      const filename = 'file with spaces & special (chars).txt'
      const data = new TextEncoder().encode('special')

      await backend.write(filename, data)
      const content = await backend.read(filename)

      expect(new TextDecoder().decode(content)).toBe('special')
    })

    it('should handle very long file paths', async () => {
      const longPath = 'a/' + 'b/'.repeat(50) + 'file.txt'
      const data = new TextEncoder().encode('deep')

      await backend.write(longPath, data)
      const content = await backend.read(longPath)

      expect(new TextDecoder().decode(content)).toBe('deep')
    })

    it('should handle unicode in filenames', async () => {
      const filename = '\u6587\u5B57.txt' // Japanese characters
      const data = new TextEncoder().encode('unicode')

      await backend.write(filename, data)
      const content = await backend.read(filename)

      expect(new TextDecoder().decode(content)).toBe('unicode')
    })

    it('should handle empty prefix in list', async () => {
      await writeFile(join(tempDir, 'file1.txt'), 'a')
      await writeFile(join(tempDir, 'file2.txt'), 'b')

      const result = await backend.list('')

      expect(result.files.length).toBeGreaterThanOrEqual(2)
    })

    it('should handle trailing slash in directory paths', async () => {
      await backend.mkdir('dir/')

      expect(await backend.exists('dir')).toBe(true)
      expect(await backend.exists('dir/')).toBe(true)
    })

    it('should normalize paths with double slashes', async () => {
      const data = new TextEncoder().encode('content')
      await backend.write('a//b//c.txt', data)

      const content = await backend.read('a/b/c.txt')
      expect(new TextDecoder().decode(content)).toBe('content')
    })

    it('should handle concurrent read operations', async () => {
      await writeFile(join(tempDir, 'concurrent.txt'), 'concurrent data')

      const reads = Array.from({ length: 100 }, () => backend.read('concurrent.txt'))
      const results = await Promise.all(reads)

      for (const result of results) {
        expect(new TextDecoder().decode(result)).toBe('concurrent data')
      }
    })

    it('should handle concurrent write operations to different files', async () => {
      const writes = Array.from({ length: 50 }, (_, i) =>
        backend.write(`file-${i}.txt`, new TextEncoder().encode(`content-${i}`))
      )

      await Promise.all(writes)

      for (let i = 0; i < 50; i++) {
        const content = await readFile(join(tempDir, `file-${i}.txt`), 'utf-8')
        expect(content).toBe(`content-${i}`)
      }
    })
  })
})
