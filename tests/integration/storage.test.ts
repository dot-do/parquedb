/**
 * Storage Integration Tests
 *
 * Tests with real FsBackend storage - no mocks.
 * These tests verify actual storage behavior and data persistence.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { rm, mkdir } from 'node:fs/promises'
import { FsBackend } from '../../src/storage/FsBackend'
import {
  FileNotFoundError,
  FileExistsError,
  VersionMismatchError,
} from '../../src/storage/MemoryBackend'
import {
  createTestData,
  decodeData,
  createRandomData,
  generateTestDirName,
} from '../factories'
import { BINARY_DATA } from '../fixtures'

describe('FsBackend Integration', () => {
  let backend: FsBackend
  let testDir: string

  beforeEach(async () => {
    // Create a unique temp directory for each test using deterministic naming
    testDir = join(tmpdir(), generateTestDirName())
    await mkdir(testDir, { recursive: true })
    backend = new FsBackend(testDir)
  })

  afterEach(async () => {
    // Clean up temp directory after each test
    try {
      await rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  describe('Read/Write Operations', () => {
    it('should write and read text data', async () => {
      const data = createTestData('Hello, ParqueDB!')
      await backend.write('test.txt', data)

      const result = await backend.read('test.txt')
      expect(decodeData(result)).toBe('Hello, ParqueDB!')
    })

    it('should write and read binary data', async () => {
      const data = BINARY_DATA.binary
      await backend.write('binary.dat', data)

      const result = await backend.read('binary.dat')
      expect(result).toEqual(data)
    })

    it('should write and read large data', async () => {
      const largeData = createRandomData(1024 * 100) // 100KB
      await backend.write('large.bin', largeData)

      const result = await backend.read('large.bin')
      expect(result.length).toBe(largeData.length)
      expect(result).toEqual(largeData)
    })

    it('should handle concurrent writes', async () => {
      const writes = Array.from({ length: 10 }, (_, i) =>
        backend.write(`file${i}.txt`, createTestData(`Content ${i}`))
      )

      await Promise.all(writes)

      // Verify all files exist
      for (let i = 0; i < 10; i++) {
        const exists = await backend.exists(`file${i}.txt`)
        expect(exists).toBe(true)
      }
    })

    it('should throw FileNotFoundError for missing files', async () => {
      await expect(backend.read('missing.txt')).rejects.toThrow(FileNotFoundError)
    })
  })

  describe('File Operations', () => {
    it('should check file existence correctly', async () => {
      expect(await backend.exists('test.txt')).toBe(false)

      await backend.write('test.txt', createTestData('content'))

      expect(await backend.exists('test.txt')).toBe(true)
    })

    it('should delete files', async () => {
      await backend.write('test.txt', createTestData('content'))
      expect(await backend.exists('test.txt')).toBe(true)

      const deleted = await backend.delete('test.txt')
      expect(deleted).toBe(true)
      expect(await backend.exists('test.txt')).toBe(false)
    })

    it('should return false when deleting non-existent file', async () => {
      const deleted = await backend.delete('nonexistent.txt')
      expect(deleted).toBe(false)
    })

    it('should copy files', async () => {
      await backend.write('source.txt', createTestData('Copy me'))

      await backend.copy('source.txt', 'dest.txt')

      expect(await backend.exists('source.txt')).toBe(true)
      expect(await backend.exists('dest.txt')).toBe(true)
      expect(decodeData(await backend.read('dest.txt'))).toBe('Copy me')
    })

    it('should move files', async () => {
      await backend.write('source.txt', createTestData('Move me'))

      await backend.move('source.txt', 'dest.txt')

      expect(await backend.exists('source.txt')).toBe(false)
      expect(await backend.exists('dest.txt')).toBe(true)
      expect(decodeData(await backend.read('dest.txt'))).toBe('Move me')
    })
  })

  describe('Byte Range Reads', () => {
    beforeEach(async () => {
      await backend.write('alphabet.txt', createTestData('ABCDEFGHIJ'))
    })

    it('should read byte ranges', async () => {
      const result = await backend.readRange('alphabet.txt', 2, 5)
      expect(decodeData(result)).toBe('CDE')
    })

    it('should handle range beyond file size', async () => {
      const result = await backend.readRange('alphabet.txt', 8, 100)
      expect(decodeData(result)).toBe('IJ')
    })

    it('should return empty for start beyond file size', async () => {
      const result = await backend.readRange('alphabet.txt', 100, 200)
      expect(result.length).toBe(0)
    })
  })

  describe('Directory Listing', () => {
    beforeEach(async () => {
      await backend.write('data/users/user1.json', createTestData('{}'))
      await backend.write('data/users/user2.json', createTestData('{}'))
      await backend.write('data/posts/post1.json', createTestData('{}'))
      await backend.write('data/config.json', createTestData('{}'))
      await backend.write('readme.txt', createTestData('README'))
    })

    it('should list files with prefix', async () => {
      const result = await backend.list('data')

      expect(result.files).toContain('data/users/user1.json')
      expect(result.files).toContain('data/users/user2.json')
      expect(result.files).toContain('data/posts/post1.json')
      expect(result.files).toContain('data/config.json')
      expect(result.files).not.toContain('readme.txt')
    })

    it('should list with delimiter for directory-like behavior', async () => {
      const result = await backend.list('data', { delimiter: '/' })

      expect(result.files).toContain('data/config.json')
      expect(result.prefixes).toContain('data/users/')
      expect(result.prefixes).toContain('data/posts/')
    })

    it('should paginate results', async () => {
      const page1 = await backend.list('data', { limit: 2 })
      expect(page1.files.length).toBe(2)
      expect(page1.hasMore).toBe(true)

      const page2 = await backend.list('data', { limit: 2, cursor: page1.cursor })
      expect(page2.files.length).toBeLessThanOrEqual(2)
    })
  })

  describe('Conditional Writes', () => {
    it('should write only if file does not exist (ifNoneMatch)', async () => {
      // First write should succeed
      const result = await backend.write('new.txt', createTestData('first'), {
        ifNoneMatch: '*',
      })
      expect(result.etag).toBeDefined()

      // Second write should fail
      await expect(
        backend.write('new.txt', createTestData('second'), { ifNoneMatch: '*' })
      ).rejects.toThrow(FileExistsError)
    })

    it('should write only if etag matches (ifMatch) with writeConditional', async () => {
      const write1 = await backend.write('versioned.txt', createTestData('v1'))

      // Update with correct etag
      const write2 = await backend.writeConditional(
        'versioned.txt',
        createTestData('v2'),
        write1.etag
      )
      expect(write2.etag).not.toBe(write1.etag)

      // Update with old etag should fail
      await expect(
        backend.writeConditional('versioned.txt', createTestData('v3'), write1.etag)
      ).rejects.toThrow(VersionMismatchError)
    })

    it('should support optimistic concurrency with writeConditional', async () => {
      // Create file
      const initial = await backend.write('data.json', createTestData('{"count":0}'))

      // First update with correct etag should succeed
      const update1 = await backend.writeConditional(
        'data.json',
        createTestData('{"count":1}'),
        initial.etag
      )
      expect(update1.etag).not.toBe(initial.etag)

      // Second update with old (stale) etag should fail
      await expect(
        backend.writeConditional(
          'data.json',
          createTestData('{"count":2}'),
          initial.etag // Using the stale etag
        )
      ).rejects.toThrow(VersionMismatchError)

      // Update with new etag should succeed
      const update3 = await backend.writeConditional(
        'data.json',
        createTestData('{"count":3}'),
        update1.etag
      )
      expect(update3.etag).toBeDefined()
    })
  })

  describe('Stat and Metadata', () => {
    it('should return file stats', async () => {
      const data = createTestData('Hello, World!')
      const writeResult = await backend.write('test.txt', data)

      const stat = await backend.stat('test.txt')

      expect(stat).not.toBeNull()
      expect(stat!.path).toBe('test.txt')
      expect(stat!.size).toBe(data.length)
      expect(stat!.etag).toBe(writeResult.etag)
      expect(stat!.mtime).toBeInstanceOf(Date)
    })

    it('should return null for non-existent file', async () => {
      const stat = await backend.stat('nonexistent.txt')
      expect(stat).toBeNull()
    })
  })

  describe('Append Operations', () => {
    it('should append to existing file', async () => {
      await backend.write('log.txt', createTestData('Line 1\n'))
      await backend.append('log.txt', createTestData('Line 2\n'))
      await backend.append('log.txt', createTestData('Line 3\n'))

      const content = decodeData(await backend.read('log.txt'))
      expect(content).toBe('Line 1\nLine 2\nLine 3\n')
    })

    it('should create file if it does not exist', async () => {
      await backend.append('new.txt', createTestData('First line'))

      expect(await backend.exists('new.txt')).toBe(true)
      expect(decodeData(await backend.read('new.txt'))).toBe('First line')
    })
  })

  describe('Delete Prefix', () => {
    beforeEach(async () => {
      await backend.write('data/a.txt', createTestData('a'))
      await backend.write('data/b.txt', createTestData('b'))
      await backend.write('data/sub/c.txt', createTestData('c'))
      await backend.write('other/d.txt', createTestData('d'))
    })

    it('should delete all files with prefix', async () => {
      const count = await backend.deletePrefix('data/')

      expect(count).toBe(3)
      expect(await backend.exists('data/a.txt')).toBe(false)
      expect(await backend.exists('data/b.txt')).toBe(false)
      expect(await backend.exists('data/sub/c.txt')).toBe(false)
      expect(await backend.exists('other/d.txt')).toBe(true)
    })

    it('should return 0 for non-matching prefix', async () => {
      const count = await backend.deletePrefix('nonexistent/')
      expect(count).toBe(0)
    })
  })
})

describe('Storage Backend Cross-Environment', () => {
  // These tests verify storage works consistently across environments

  let backend: FsBackend
  let testDir: string

  beforeEach(async () => {
    testDir = join(tmpdir(), generateTestDirName())
    await mkdir(testDir, { recursive: true })
    backend = new FsBackend(testDir)
  })

  afterEach(async () => {
    try {
      await rm(testDir, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  it('should handle unicode content', async () => {
    const content = 'Hello, World! \u4e2d\u6587 \ud83d\udc4b'
    await backend.write('unicode.txt', createTestData(content))

    const result = decodeData(await backend.read('unicode.txt'))
    expect(result).toBe(content)
  })

  it('should handle paths with special characters', async () => {
    const path = 'data/file with spaces.txt'
    await backend.write(path, createTestData('content'))

    expect(await backend.exists(path)).toBe(true)
  })

  it('should handle deeply nested paths', async () => {
    const deepPath = 'a/b/c/d/e/f/g/h/i/j/file.txt'
    await backend.write(deepPath, createTestData('deep'))

    expect(await backend.exists(deepPath)).toBe(true)
    expect(decodeData(await backend.read(deepPath))).toBe('deep')
  })
})
