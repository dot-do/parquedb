/**
 * Cross-Backend Migration Tests
 *
 * Tests for migrating data between different storage backends.
 * Verifies data integrity and completeness after migration.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { rm, mkdir } from 'node:fs/promises'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { FsBackend } from '../../../src/storage/FsBackend'
import type { StorageBackend, FileStat } from '../../../src/types/storage'
import {
  createTestData,
  decodeData,
  createRandomData,
} from '../../factories'
import { BINARY_DATA } from '../../fixtures'

// =============================================================================
// Migration Utility Functions
// =============================================================================

/**
 * Migrate all files from source to destination backend
 * @returns Number of files migrated
 */
async function migrateBackend(
  source: StorageBackend,
  dest: StorageBackend,
  prefix = ''
): Promise<number> {
  let migratedCount = 0
  let cursor: string | undefined

  do {
    const listResult = await source.list(prefix, { cursor, includeMetadata: true })

    for (const filePath of listResult.files) {
      const data = await source.read(filePath)
      await dest.write(filePath, data)
      migratedCount++
    }

    cursor = listResult.hasMore ? listResult.cursor : undefined
  } while (cursor)

  return migratedCount
}

/**
 * Verify that two backends have identical content
 */
async function verifyBackendsMatch(
  backend1: StorageBackend,
  backend2: StorageBackend,
  prefix = ''
): Promise<{ match: boolean; differences: string[] }> {
  const differences: string[] = []

  // Get all files from both backends
  const files1 = await getAllFiles(backend1, prefix)
  const files2 = await getAllFiles(backend2, prefix)

  // Check for files in backend1 not in backend2
  for (const file of files1) {
    if (!files2.includes(file)) {
      differences.push(`File "${file}" exists in source but not destination`)
    }
  }

  // Check for files in backend2 not in backend1
  for (const file of files2) {
    if (!files1.includes(file)) {
      differences.push(`File "${file}" exists in destination but not source`)
    }
  }

  // Compare content of common files
  const commonFiles = files1.filter(f => files2.includes(f))
  for (const file of commonFiles) {
    const data1 = await backend1.read(file)
    const data2 = await backend2.read(file)

    if (data1.length !== data2.length) {
      differences.push(`File "${file}" has different sizes: ${data1.length} vs ${data2.length}`)
    } else if (!arraysEqual(data1, data2)) {
      differences.push(`File "${file}" has different content`)
    }
  }

  return {
    match: differences.length === 0,
    differences,
  }
}

/**
 * Get all files from a backend
 */
async function getAllFiles(backend: StorageBackend, prefix = ''): Promise<string[]> {
  const files: string[] = []
  let cursor: string | undefined

  do {
    const result = await backend.list(prefix, { cursor })
    files.push(...result.files)
    cursor = result.hasMore ? result.cursor : undefined
  } while (cursor)

  return files.sort()
}

/**
 * Compare two Uint8Arrays for equality
 */
function arraysEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false
  }
  return true
}

// =============================================================================
// Test Suites
// =============================================================================

describe('Cross-Backend Migration', () => {
  let memoryBackend: MemoryBackend
  let fsBackend: FsBackend
  let fsBackend2: FsBackend
  let testDir1: string
  let testDir2: string

  beforeEach(async () => {
    // Create memory backend
    memoryBackend = new MemoryBackend()

    // Create filesystem backends with unique temp directories
    testDir1 = join(tmpdir(), `parquedb-migration-test1-${Date.now()}-${Math.random().toString(36).slice(2)}`)
    testDir2 = join(tmpdir(), `parquedb-migration-test2-${Date.now()}-${Math.random().toString(36).slice(2)}`)
    await mkdir(testDir1, { recursive: true })
    await mkdir(testDir2, { recursive: true })
    fsBackend = new FsBackend(testDir1)
    fsBackend2 = new FsBackend(testDir2)
  })

  afterEach(async () => {
    // Clean up temp directories
    try {
      await rm(testDir1, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
    try {
      await rm(testDir2, { recursive: true, force: true })
    } catch {
      // Ignore cleanup errors
    }
  })

  // ===========================================================================
  // Memory -> FileSystem Migration
  // ===========================================================================

  describe('Memory -> FileSystem Migration', () => {
    it('should migrate a single text file', async () => {
      // Arrange: write data to memory backend
      const content = 'Hello, Migration!'
      await memoryBackend.write('test.txt', createTestData(content))

      // Act: migrate to filesystem
      const count = await migrateBackend(memoryBackend, fsBackend)

      // Assert
      expect(count).toBe(1)
      expect(await fsBackend.exists('test.txt')).toBe(true)
      expect(decodeData(await fsBackend.read('test.txt'))).toBe(content)
    })

    it('should migrate multiple files', async () => {
      // Arrange: write multiple files
      const files = [
        { path: 'file1.txt', content: 'Content 1' },
        { path: 'file2.txt', content: 'Content 2' },
        { path: 'file3.txt', content: 'Content 3' },
      ]

      for (const file of files) {
        await memoryBackend.write(file.path, createTestData(file.content))
      }

      // Act: migrate
      const count = await migrateBackend(memoryBackend, fsBackend)

      // Assert
      expect(count).toBe(files.length)
      for (const file of files) {
        expect(await fsBackend.exists(file.path)).toBe(true)
        expect(decodeData(await fsBackend.read(file.path))).toBe(file.content)
      }
    })

    it('should migrate nested directory structure', async () => {
      // Arrange: create nested structure
      const files = [
        'data/users/user1.json',
        'data/users/user2.json',
        'data/posts/post1.json',
        'data/posts/post2.json',
        'config/settings.json',
        'readme.txt',
      ]

      for (const file of files) {
        await memoryBackend.write(file, createTestData(`content of ${file}`))
      }

      // Act: migrate
      const count = await migrateBackend(memoryBackend, fsBackend)

      // Assert
      expect(count).toBe(files.length)
      for (const file of files) {
        expect(await fsBackend.exists(file)).toBe(true)
        expect(decodeData(await fsBackend.read(file))).toBe(`content of ${file}`)
      }
    })

    it('should migrate binary data correctly', async () => {
      // Arrange: write binary data
      await memoryBackend.write('binary.dat', BINARY_DATA.binary)

      // Act: migrate
      await migrateBackend(memoryBackend, fsBackend)

      // Assert
      const migrated = await fsBackend.read('binary.dat')
      expect(migrated).toEqual(BINARY_DATA.binary)
    })

    it('should migrate large files', async () => {
      // Arrange: create large file (1MB)
      const largeData = createRandomData(1024 * 1024)
      await memoryBackend.write('large.bin', largeData)

      // Act: migrate
      await migrateBackend(memoryBackend, fsBackend)

      // Assert
      const migrated = await fsBackend.read('large.bin')
      expect(migrated.length).toBe(largeData.length)
      expect(migrated).toEqual(largeData)
    })

    it('should migrate empty files', async () => {
      // Arrange
      await memoryBackend.write('empty.txt', BINARY_DATA.empty)

      // Act: migrate
      await migrateBackend(memoryBackend, fsBackend)

      // Assert
      const migrated = await fsBackend.read('empty.txt')
      expect(migrated.length).toBe(0)
    })

    it('should preserve file content integrity', async () => {
      // Arrange: create various test files
      const testFiles = [
        { path: 'text.txt', data: createTestData('Hello, World!') },
        { path: 'json.json', data: BINARY_DATA.json },
        { path: 'binary.bin', data: BINARY_DATA.binary },
        { path: 'parquet.parquet', data: BINARY_DATA.parquet },
      ]

      for (const file of testFiles) {
        await memoryBackend.write(file.path, file.data)
      }

      // Act: migrate
      await migrateBackend(memoryBackend, fsBackend)

      // Assert: verify all files match exactly
      const verification = await verifyBackendsMatch(memoryBackend, fsBackend)
      expect(verification.match).toBe(true)
      expect(verification.differences).toEqual([])
    })
  })

  // ===========================================================================
  // FileSystem -> Memory Migration
  // ===========================================================================

  describe('FileSystem -> Memory Migration', () => {
    it('should migrate from filesystem to memory', async () => {
      // Arrange: write to filesystem
      const content = 'Reverse migration test'
      await fsBackend.write('reverse.txt', createTestData(content))

      // Act: migrate to memory
      const count = await migrateBackend(fsBackend, memoryBackend)

      // Assert
      expect(count).toBe(1)
      expect(await memoryBackend.exists('reverse.txt')).toBe(true)
      expect(decodeData(await memoryBackend.read('reverse.txt'))).toBe(content)
    })

    it('should migrate complex directory structure from filesystem', async () => {
      // Arrange: create directory structure in filesystem
      const files = [
        'data/entities/users.parquet',
        'data/entities/posts.parquet',
        'data/relationships/forward.parquet',
        'data/relationships/reverse.parquet',
        'indexes/fts/users.idx',
        'config/schema.json',
      ]

      for (const file of files) {
        await fsBackend.write(file, createTestData(`data: ${file}`))
      }

      // Act: migrate
      const count = await migrateBackend(fsBackend, memoryBackend)

      // Assert
      expect(count).toBe(files.length)
      const verification = await verifyBackendsMatch(fsBackend, memoryBackend)
      expect(verification.match).toBe(true)
    })
  })

  // ===========================================================================
  // FileSystem -> FileSystem Migration
  // ===========================================================================

  describe('FileSystem -> FileSystem Migration', () => {
    it('should migrate between two filesystem backends', async () => {
      // Arrange: create data in first filesystem
      const files = [
        { path: 'data/file1.txt', content: 'File 1 content' },
        { path: 'data/file2.txt', content: 'File 2 content' },
        { path: 'config/settings.json', content: '{"key": "value"}' },
      ]

      for (const file of files) {
        await fsBackend.write(file.path, createTestData(file.content))
      }

      // Act: migrate to second filesystem
      const count = await migrateBackend(fsBackend, fsBackend2)

      // Assert
      expect(count).toBe(files.length)
      for (const file of files) {
        expect(await fsBackend2.exists(file.path)).toBe(true)
        expect(decodeData(await fsBackend2.read(file.path))).toBe(file.content)
      }
    })

    it('should preserve binary integrity in filesystem-to-filesystem migration', async () => {
      // Arrange: create binary files
      const largeData = createRandomData(100 * 1024) // 100KB
      await fsBackend.write('data/large.bin', largeData)
      await fsBackend.write('data/binary.dat', BINARY_DATA.binary)

      // Act: migrate
      await migrateBackend(fsBackend, fsBackend2)

      // Assert
      const verification = await verifyBackendsMatch(fsBackend, fsBackend2, 'data/')
      expect(verification.match).toBe(true)
    })
  })

  // ===========================================================================
  // Round-Trip Migration
  // ===========================================================================

  describe('Round-Trip Migration', () => {
    it('should preserve data through Memory -> FS -> Memory round-trip', async () => {
      // Arrange: create initial data
      const files = [
        { path: 'text.txt', data: createTestData('Hello, Round-Trip!') },
        { path: 'binary.bin', data: BINARY_DATA.binary },
        { path: 'nested/deep/file.json', data: createTestData('{"test": true}') },
      ]

      for (const file of files) {
        await memoryBackend.write(file.path, file.data)
      }

      // Act: Memory -> FS -> Memory
      await migrateBackend(memoryBackend, fsBackend)
      const memoryBackend2 = new MemoryBackend()
      await migrateBackend(fsBackend, memoryBackend2)

      // Assert: original and final should match
      const verification = await verifyBackendsMatch(memoryBackend, memoryBackend2)
      expect(verification.match).toBe(true)
      expect(verification.differences).toEqual([])
    })

    it('should preserve data through FS -> Memory -> FS round-trip', async () => {
      // Arrange: create initial data in FS
      const files = [
        { path: 'data/entities.parquet', data: createRandomData(10 * 1024) },
        { path: 'data/index.idx', data: createRandomData(5 * 1024) },
        { path: 'meta/schema.json', data: createTestData('{"version": 1}') },
      ]

      for (const file of files) {
        await fsBackend.write(file.path, file.data)
      }

      // Act: FS -> Memory -> FS
      await migrateBackend(fsBackend, memoryBackend)
      await migrateBackend(memoryBackend, fsBackend2)

      // Assert: original and final should match
      const verification = await verifyBackendsMatch(fsBackend, fsBackend2)
      expect(verification.match).toBe(true)
      expect(verification.differences).toEqual([])
    })
  })

  // ===========================================================================
  // Partial/Prefix Migration
  // ===========================================================================

  describe('Partial Migration with Prefix', () => {
    it('should migrate only files matching prefix', async () => {
      // Arrange: create files in different directories
      await memoryBackend.write('data/users/user1.json', createTestData('user1'))
      await memoryBackend.write('data/users/user2.json', createTestData('user2'))
      await memoryBackend.write('data/posts/post1.json', createTestData('post1'))
      await memoryBackend.write('config/settings.json', createTestData('settings'))

      // Act: migrate only data/users/ prefix
      const count = await migrateBackend(memoryBackend, fsBackend, 'data/users/')

      // Assert
      expect(count).toBe(2)
      expect(await fsBackend.exists('data/users/user1.json')).toBe(true)
      expect(await fsBackend.exists('data/users/user2.json')).toBe(true)
      expect(await fsBackend.exists('data/posts/post1.json')).toBe(false)
      expect(await fsBackend.exists('config/settings.json')).toBe(false)
    })
  })

  // ===========================================================================
  // Data Integrity Verification
  // ===========================================================================

  describe('Data Integrity Verification', () => {
    it('should detect missing files after migration', async () => {
      // Arrange: create data in source
      await memoryBackend.write('file1.txt', createTestData('content1'))
      await memoryBackend.write('file2.txt', createTestData('content2'))

      // Manually copy only one file (simulating incomplete migration)
      await fsBackend.write('file1.txt', await memoryBackend.read('file1.txt'))

      // Assert: verification should detect missing file
      const verification = await verifyBackendsMatch(memoryBackend, fsBackend)
      expect(verification.match).toBe(false)
      expect(verification.differences.some(d => d.includes('file2.txt'))).toBe(true)
    })

    it('should detect content differences after migration', async () => {
      // Arrange
      await memoryBackend.write('test.txt', createTestData('original'))
      await fsBackend.write('test.txt', createTestData('modified'))

      // Assert
      const verification = await verifyBackendsMatch(memoryBackend, fsBackend)
      expect(verification.match).toBe(false)
      expect(verification.differences.some(d => d.includes('different content'))).toBe(true)
    })

    it('should detect size differences', async () => {
      // Arrange
      await memoryBackend.write('test.txt', createTestData('short'))
      await fsBackend.write('test.txt', createTestData('this is much longer content'))

      // Assert
      const verification = await verifyBackendsMatch(memoryBackend, fsBackend)
      expect(verification.match).toBe(false)
      expect(verification.differences.some(d => d.includes('different sizes'))).toBe(true)
    })

    it('should detect extra files in destination', async () => {
      // Arrange
      await memoryBackend.write('file1.txt', createTestData('content1'))
      await fsBackend.write('file1.txt', createTestData('content1'))
      await fsBackend.write('extra.txt', createTestData('extra'))

      // Assert
      const verification = await verifyBackendsMatch(memoryBackend, fsBackend)
      expect(verification.match).toBe(false)
      expect(verification.differences.some(d => d.includes('extra.txt'))).toBe(true)
    })
  })

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('Edge Cases', () => {
    it('should handle migration of empty backend', async () => {
      // Act: migrate empty backend
      const count = await migrateBackend(memoryBackend, fsBackend)

      // Assert
      expect(count).toBe(0)
    })

    it('should handle files with special characters in names', async () => {
      // Arrange
      const files = [
        'file with spaces.txt',
        'data/path with spaces/file.txt',
      ]

      for (const file of files) {
        await memoryBackend.write(file, createTestData(`content of ${file}`))
      }

      // Act
      await migrateBackend(memoryBackend, fsBackend)

      // Assert
      for (const file of files) {
        expect(await fsBackend.exists(file)).toBe(true)
      }
    })

    it('should handle very deeply nested paths', async () => {
      // Arrange
      const deepPath = 'a/b/c/d/e/f/g/h/i/j/k/l/m/file.txt'
      await memoryBackend.write(deepPath, createTestData('deep content'))

      // Act
      await migrateBackend(memoryBackend, fsBackend)

      // Assert
      expect(await fsBackend.exists(deepPath)).toBe(true)
      expect(decodeData(await fsBackend.read(deepPath))).toBe('deep content')
    })

    it('should handle unicode file paths', async () => {
      // Arrange
      const unicodePath = 'data/\u4e2d\u6587/file.txt'
      await memoryBackend.write(unicodePath, createTestData('unicode path test'))

      // Act
      await migrateBackend(memoryBackend, fsBackend)

      // Assert
      expect(await fsBackend.exists(unicodePath)).toBe(true)
    })

    it('should handle unicode content', async () => {
      // Arrange
      const unicodeContent = 'Hello \u4e16\u754c! \ud83c\udf0d'
      await memoryBackend.write('unicode.txt', createTestData(unicodeContent))

      // Act
      await migrateBackend(memoryBackend, fsBackend)

      // Assert
      expect(decodeData(await fsBackend.read('unicode.txt'))).toBe(unicodeContent)
    })
  })

  // ===========================================================================
  // Large-Scale Migration
  // ===========================================================================

  describe('Large-Scale Migration', () => {
    it('should migrate many small files efficiently', async () => {
      // Arrange: create 100 small files
      const fileCount = 100
      for (let i = 0; i < fileCount; i++) {
        await memoryBackend.write(`files/file${i}.txt`, createTestData(`content ${i}`))
      }

      // Act
      const count = await migrateBackend(memoryBackend, fsBackend)

      // Assert
      expect(count).toBe(fileCount)
      const verification = await verifyBackendsMatch(memoryBackend, fsBackend, 'files/')
      expect(verification.match).toBe(true)
    })

    it('should migrate mixed file sizes correctly', async () => {
      // Arrange: create files of varying sizes
      const files = [
        { path: 'tiny.txt', size: 10 },
        { path: 'small.txt', size: 1024 },
        { path: 'medium.bin', size: 100 * 1024 },
        { path: 'large.bin', size: 500 * 1024 },
      ]

      for (const file of files) {
        await memoryBackend.write(file.path, createRandomData(file.size))
      }

      // Act
      await migrateBackend(memoryBackend, fsBackend)

      // Assert
      const verification = await verifyBackendsMatch(memoryBackend, fsBackend)
      expect(verification.match).toBe(true)
    })
  })
})

// =============================================================================
// Incremental Migration Tests
// =============================================================================

describe('Incremental Migration', () => {
  let sourceBackend: MemoryBackend
  let destBackend: MemoryBackend

  beforeEach(() => {
    sourceBackend = new MemoryBackend()
    destBackend = new MemoryBackend()
  })

  /**
   * Migrate only new or updated files
   */
  async function incrementalMigrate(
    source: StorageBackend,
    dest: StorageBackend,
    prefix = ''
  ): Promise<{ added: number; updated: number; skipped: number }> {
    let added = 0
    let updated = 0
    let skipped = 0

    const sourceFiles = await getAllFiles(source, prefix)

    for (const file of sourceFiles) {
      const sourceData = await source.read(file)
      const destExists = await dest.exists(file)

      if (!destExists) {
        await dest.write(file, sourceData)
        added++
      } else {
        const destData = await dest.read(file)
        if (!arraysEqual(sourceData, destData)) {
          await dest.write(file, sourceData)
          updated++
        } else {
          skipped++
        }
      }
    }

    return { added, updated, skipped }
  }

  it('should only migrate new files', async () => {
    // Arrange: initial state
    await sourceBackend.write('file1.txt', createTestData('content1'))
    await destBackend.write('file1.txt', createTestData('content1'))

    // Add new file to source
    await sourceBackend.write('file2.txt', createTestData('content2'))

    // Act
    const result = await incrementalMigrate(sourceBackend, destBackend)

    // Assert
    expect(result.added).toBe(1)
    expect(result.updated).toBe(0)
    expect(result.skipped).toBe(1)
  })

  it('should update changed files', async () => {
    // Arrange: initial state
    await sourceBackend.write('file1.txt', createTestData('original'))
    await destBackend.write('file1.txt', createTestData('original'))

    // Update source
    await sourceBackend.write('file1.txt', createTestData('updated'))

    // Act
    const result = await incrementalMigrate(sourceBackend, destBackend)

    // Assert
    expect(result.added).toBe(0)
    expect(result.updated).toBe(1)
    expect(result.skipped).toBe(0)
    expect(decodeData(await destBackend.read('file1.txt'))).toBe('updated')
  })

  it('should skip unchanged files', async () => {
    // Arrange: identical state
    await sourceBackend.write('file1.txt', createTestData('content'))
    await destBackend.write('file1.txt', createTestData('content'))

    // Act
    const result = await incrementalMigrate(sourceBackend, destBackend)

    // Assert
    expect(result.added).toBe(0)
    expect(result.updated).toBe(0)
    expect(result.skipped).toBe(1)
  })
})
