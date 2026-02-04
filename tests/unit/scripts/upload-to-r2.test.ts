/**
 * R2 Upload Script Tests
 *
 * TDD RED Phase: Tests for uploading Parquet files to R2.
 *
 * Test cases:
 * 1. Upload single parquet file to R2
 * 2. Upload directory of parquet files
 * 3. Verify uploaded file matches local file (checksum)
 * 4. Handle upload failures gracefully
 * 5. Skip unchanged files (incremental upload)
 *
 * All tests should FAIL until GREEN phase implements the upload script.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import * as fs from 'node:fs'
import * as path from 'node:path'
import * as crypto from 'node:crypto'
import { createMockR2Bucket, createErrorR2Bucket, type MockR2Bucket } from '../../mocks/r2-bucket'

// =============================================================================
// Mock Module - The upload module that will be created in GREEN phase
// =============================================================================

// Mock the upload module that doesn't exist yet
// These will fail until the module is implemented in GREEN phase
vi.mock('../../../scripts/lib/r2-upload', () => ({
  uploadFileToR2: vi.fn().mockRejectedValue(new Error('Not implemented')),
  uploadDirectoryToR2: vi.fn().mockRejectedValue(new Error('Not implemented')),
  verifyUpload: vi.fn().mockRejectedValue(new Error('Not implemented')),
  syncToR2: vi.fn().mockRejectedValue(new Error('Not implemented')),
  computeFileHash: vi.fn().mockRejectedValue(new Error('Not implemented')),
}))

// Import the mocked module (will be replaced when implemented)
import {
  uploadFileToR2,
  uploadDirectoryToR2,
  verifyUpload,
  syncToR2,
  computeFileHash,
} from '../../../scripts/lib/r2-upload'

// =============================================================================
// Test Fixtures and Helpers
// =============================================================================

/**
 * Create a temporary test directory with sample parquet files
 */
async function createTempTestDirectory(): Promise<string> {
  const { mkdtemp, writeFile, mkdir } = await import('node:fs/promises')
  const { tmpdir } = await import('node:os')

  const tempDir = await mkdtemp(path.join(tmpdir(), 'parquedb-r2-test-'))

  // Create sample parquet file structure
  // data/
  //   collection1/
  //     data.parquet
  //   collection2/
  //     data.parquet
  //     indexes/
  //       bloom.parquet

  const collections = ['users', 'products', 'orders']

  for (const collection of collections) {
    const collectionDir = path.join(tempDir, 'data', collection)
    await mkdir(collectionDir, { recursive: true })

    // Create mock parquet file (just binary data for testing)
    const parquetContent = createMockParquetContent(collection)
    await writeFile(path.join(collectionDir, 'data.parquet'), parquetContent)

    // Create index files for some collections
    if (collection === 'users') {
      const indexDir = path.join(collectionDir, 'indexes')
      await mkdir(indexDir, { recursive: true })
      await writeFile(path.join(indexDir, 'bloom.parquet'), createMockParquetContent('bloom'))
    }
  }

  return tempDir
}

/**
 * Clean up temporary test directory
 */
async function cleanupTempDirectory(tempDir: string): Promise<void> {
  const { rm } = await import('node:fs/promises')
  await rm(tempDir, { recursive: true, force: true })
}

/**
 * Create mock parquet content (binary data that simulates a parquet file)
 */
function createMockParquetContent(name: string): Buffer {
  // Parquet files start with magic bytes "PAR1"
  const magic = Buffer.from('PAR1')
  const content = Buffer.from(`mock-parquet-content-for-${name}-${Date.now()}`)
  // Parquet files also end with magic bytes
  return Buffer.concat([magic, content, magic])
}

/**
 * Compute SHA-256 hash of a buffer
 */
function computeHash(data: Buffer | Uint8Array): string {
  return crypto.createHash('sha256').update(data).digest('hex')
}

// =============================================================================
// Test Suite
// =============================================================================

describe('R2 Upload Script', () => {
  let mockBucket: MockR2Bucket
  let tempDir: string

  beforeEach(async () => {
    // Reset all mocks
    vi.clearAllMocks()

    // Create functional mock R2 bucket
    mockBucket = createMockR2Bucket({ functional: true })

    // Create temp test directory
    tempDir = await createTempTestDirectory()
  })

  afterEach(async () => {
    // Clean up temp directory
    await cleanupTempDirectory(tempDir)
  })

  // ===========================================================================
  // 1. Upload single parquet file to R2
  // ===========================================================================

  describe('uploadFileToR2', () => {
    it('should upload a single parquet file to R2', async () => {
      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')
      const r2Path = 'data/users/data.parquet'

      const result = await uploadFileToR2(mockBucket, localPath, r2Path)

      expect(result.success).toBe(true)
      expect(result.path).toBe(r2Path)
      expect(result.size).toBeGreaterThan(0)
      expect(mockBucket.put).toHaveBeenCalledWith(
        r2Path,
        expect.any(Uint8Array),
        expect.objectContaining({
          contentType: 'application/octet-stream',
        })
      )
    })

    it('should return the uploaded file etag', async () => {
      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')
      const r2Path = 'data/users/data.parquet'

      const result = await uploadFileToR2(mockBucket, localPath, r2Path)

      expect(result.etag).toBeDefined()
      expect(typeof result.etag).toBe('string')
      expect(result.etag.length).toBeGreaterThan(0)
    })

    it('should set custom metadata with local file hash', async () => {
      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')
      const r2Path = 'data/users/data.parquet'
      const localContent = fs.readFileSync(localPath)
      const expectedHash = computeHash(localContent)

      await uploadFileToR2(mockBucket, localPath, r2Path)

      expect(mockBucket.put).toHaveBeenCalledWith(
        r2Path,
        expect.any(Uint8Array),
        expect.objectContaining({
          customMetadata: expect.objectContaining({
            'x-parquedb-sha256': expectedHash,
          }),
        })
      )
    })

    it('should throw error for non-existent local file', async () => {
      const localPath = path.join(tempDir, 'non-existent.parquet')
      const r2Path = 'data/missing/data.parquet'

      await expect(uploadFileToR2(mockBucket, localPath, r2Path)).rejects.toThrow(/ENOENT|not found|does not exist/i)
    })

    it('should throw error for non-parquet files', async () => {
      const { writeFile } = await import('node:fs/promises')
      const localPath = path.join(tempDir, 'test.txt')
      await writeFile(localPath, 'not a parquet file')

      await expect(uploadFileToR2(mockBucket, localPath, 'test.txt')).rejects.toThrow(/parquet/i)
    })
  })

  // ===========================================================================
  // 2. Upload directory of parquet files
  // ===========================================================================

  describe('uploadDirectoryToR2', () => {
    it('should upload all parquet files in a directory', async () => {
      const localDir = path.join(tempDir, 'data')
      const r2Prefix = 'data/'

      const result = await uploadDirectoryToR2(mockBucket, localDir, r2Prefix)

      expect(result.success).toBe(true)
      expect(result.uploaded).toBeGreaterThanOrEqual(3) // At least users, products, orders
      expect(result.failed).toBe(0)
    })

    it('should return list of uploaded files', async () => {
      const localDir = path.join(tempDir, 'data')
      const r2Prefix = 'data/'

      const result = await uploadDirectoryToR2(mockBucket, localDir, r2Prefix)

      expect(result.files).toBeDefined()
      expect(Array.isArray(result.files)).toBe(true)
      expect(result.files.length).toBeGreaterThanOrEqual(3)

      // Check that all expected files are in the list
      const r2Paths = result.files.map((f: { r2Path: string }) => f.r2Path)
      expect(r2Paths).toContain('data/users/data.parquet')
      expect(r2Paths).toContain('data/products/data.parquet')
      expect(r2Paths).toContain('data/orders/data.parquet')
    })

    it('should upload files recursively including subdirectories', async () => {
      const localDir = path.join(tempDir, 'data')
      const r2Prefix = 'data/'

      const result = await uploadDirectoryToR2(mockBucket, localDir, r2Prefix)

      // Should include the bloom index file in users/indexes/
      const r2Paths = result.files.map((f: { r2Path: string }) => f.r2Path)
      expect(r2Paths).toContain('data/users/indexes/bloom.parquet')
    })

    it('should preserve directory structure in R2 paths', async () => {
      const localDir = path.join(tempDir, 'data')
      const r2Prefix = 'datasets/test/'

      const result = await uploadDirectoryToR2(mockBucket, localDir, r2Prefix)

      const r2Paths = result.files.map((f: { r2Path: string }) => f.r2Path)
      expect(r2Paths).toContain('datasets/test/users/data.parquet')
      expect(r2Paths).toContain('datasets/test/users/indexes/bloom.parquet')
    })

    it('should skip non-parquet files', async () => {
      const { writeFile } = await import('node:fs/promises')
      await writeFile(path.join(tempDir, 'data', 'README.md'), '# Test')
      await writeFile(path.join(tempDir, 'data', 'config.json'), '{}')

      const localDir = path.join(tempDir, 'data')
      const r2Prefix = 'data/'

      const result = await uploadDirectoryToR2(mockBucket, localDir, r2Prefix)

      const r2Paths = result.files.map((f: { r2Path: string }) => f.r2Path)
      expect(r2Paths).not.toContain('data/README.md')
      expect(r2Paths).not.toContain('data/config.json')
    })

    it('should throw error for non-existent directory', async () => {
      const localDir = path.join(tempDir, 'non-existent-dir')

      await expect(uploadDirectoryToR2(mockBucket, localDir, 'data/')).rejects.toThrow(/ENOENT|not found|does not exist/i)
    })

    it('should return empty result for directory with no parquet files', async () => {
      const { mkdir, writeFile } = await import('node:fs/promises')
      const emptyDir = path.join(tempDir, 'empty')
      await mkdir(emptyDir)
      await writeFile(path.join(emptyDir, 'test.txt'), 'not parquet')

      const result = await uploadDirectoryToR2(mockBucket, emptyDir, 'empty/')

      expect(result.success).toBe(true)
      expect(result.uploaded).toBe(0)
      expect(result.files).toEqual([])
    })
  })

  // ===========================================================================
  // 3. Verify uploaded file matches local file (checksum)
  // ===========================================================================

  describe('verifyUpload', () => {
    it('should verify uploaded file matches local file by checksum', async () => {
      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')
      const r2Path = 'data/users/data.parquet'
      const localContent = fs.readFileSync(localPath)
      const hash = computeHash(localContent)

      // Pre-populate mock bucket with the file
      await mockBucket.put(r2Path, localContent, {
        customMetadata: { 'x-parquedb-sha256': hash },
      })

      const result = await verifyUpload(mockBucket, localPath, r2Path)

      expect(result.valid).toBe(true)
      expect(result.localHash).toBe(hash)
      expect(result.remoteHash).toBe(hash)
    })

    it('should detect checksum mismatch', async () => {
      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')
      const r2Path = 'data/users/data.parquet'
      const localContent = fs.readFileSync(localPath)
      const localHash = computeHash(localContent)

      // Upload with different content (simulating corruption)
      const corruptContent = Buffer.from('corrupted data')
      const corruptHash = computeHash(corruptContent)
      await mockBucket.put(r2Path, corruptContent, {
        customMetadata: { 'x-parquedb-sha256': corruptHash },
      })

      const result = await verifyUpload(mockBucket, localPath, r2Path)

      expect(result.valid).toBe(false)
      expect(result.localHash).toBe(localHash)
      expect(result.remoteHash).toBe(corruptHash)
      expect(result.localHash).not.toBe(result.remoteHash)
    })

    it('should return false if remote file does not exist', async () => {
      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')
      const r2Path = 'data/non-existent/data.parquet'

      const result = await verifyUpload(mockBucket, localPath, r2Path)

      expect(result.valid).toBe(false)
      expect(result.error).toMatch(/not found|does not exist/i)
    })

    it('should fallback to content comparison if metadata hash is missing', async () => {
      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')
      const r2Path = 'data/users/data.parquet'
      const localContent = fs.readFileSync(localPath)

      // Upload without hash metadata
      await mockBucket.put(r2Path, localContent, {})

      const result = await verifyUpload(mockBucket, localPath, r2Path)

      expect(result.valid).toBe(true)
      expect(result.method).toBe('content-comparison')
    })
  })

  // ===========================================================================
  // 4. Handle upload failures gracefully
  // ===========================================================================

  describe('Error Handling', () => {
    it('should handle R2 network errors gracefully', async () => {
      const errorBucket = createErrorR2Bucket('network')
      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')

      const result = await uploadFileToR2(errorBucket, localPath, 'data/users/data.parquet')

      expect(result.success).toBe(false)
      expect(result.error).toMatch(/network|connection/i)
    })

    it('should handle R2 access denied errors gracefully', async () => {
      const errorBucket = createErrorR2Bucket('accessDenied')
      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')

      const result = await uploadFileToR2(errorBucket, localPath, 'data/users/data.parquet')

      expect(result.success).toBe(false)
      expect(result.error).toMatch(/access|denied|permission/i)
    })

    it('should handle R2 quota exceeded errors gracefully', async () => {
      const errorBucket = createErrorR2Bucket('quota')
      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')

      const result = await uploadFileToR2(errorBucket, localPath, 'data/users/data.parquet')

      expect(result.success).toBe(false)
      expect(result.error).toMatch(/quota|exceeded|limit/i)
    })

    it('should continue uploading other files when one fails', async () => {
      // Create a bucket that fails only for specific files
      const partialFailBucket = createMockR2Bucket({ functional: true })
      let callCount = 0
      partialFailBucket.put = vi.fn().mockImplementation(async (key: string, data: Uint8Array) => {
        callCount++
        if (key.includes('products')) {
          throw new Error('Simulated failure for products')
        }
        return { key, size: data.length, etag: `"etag-${callCount}"` }
      })

      const localDir = path.join(tempDir, 'data')
      const result = await uploadDirectoryToR2(partialFailBucket, localDir, 'data/')

      expect(result.uploaded).toBeGreaterThan(0)
      expect(result.failed).toBeGreaterThan(0)
      expect(result.errors).toContainEqual(
        expect.objectContaining({
          path: expect.stringContaining('products'),
        })
      )
    })

    it('should retry failed uploads with exponential backoff', async () => {
      let attempts = 0
      const retryBucket = createMockR2Bucket({ functional: true })
      retryBucket.put = vi.fn().mockImplementation(async (key: string, data: Uint8Array) => {
        attempts++
        if (attempts < 3) {
          throw new Error('Temporary failure')
        }
        return { key, size: data.length, etag: '"success"' }
      })

      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')
      const result = await uploadFileToR2(retryBucket, localPath, 'data/users/data.parquet', {
        maxRetries: 3,
        retryDelay: 10, // Short delay for tests
      })

      expect(result.success).toBe(true)
      expect(attempts).toBe(3)
    })
  })

  // ===========================================================================
  // 5. Skip unchanged files (incremental upload)
  // ===========================================================================

  describe('syncToR2 (incremental upload)', () => {
    it('should skip files that already exist with matching hash', async () => {
      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')
      const r2Path = 'data/users/data.parquet'
      const localContent = fs.readFileSync(localPath)
      const hash = computeHash(localContent)

      // Pre-populate bucket with the same content
      await mockBucket.put(r2Path, localContent, {
        customMetadata: { 'x-parquedb-sha256': hash },
      })

      const result = await syncToR2(mockBucket, tempDir, 'data/')

      expect(result.skipped).toBeGreaterThan(0)
      expect(result.skippedFiles).toContain(r2Path)
    })

    it('should upload files with different hash', async () => {
      const r2Path = 'data/users/data.parquet'

      // Pre-populate bucket with different content
      const oldContent = createMockParquetContent('old-users')
      const oldHash = computeHash(oldContent)
      await mockBucket.put(r2Path, oldContent, {
        customMetadata: { 'x-parquedb-sha256': oldHash },
      })

      const result = await syncToR2(mockBucket, tempDir, 'data/')

      expect(result.uploaded).toBeGreaterThan(0)
      expect(result.uploadedFiles).toContain(r2Path)
    })

    it('should upload new files that do not exist in R2', async () => {
      // Empty bucket - all files are new

      const result = await syncToR2(mockBucket, tempDir, 'data/')

      expect(result.uploaded).toBeGreaterThanOrEqual(3)
      expect(result.skipped).toBe(0)
    })

    it('should report summary of sync operation', async () => {
      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')
      const localContent = fs.readFileSync(localPath)
      const hash = computeHash(localContent)

      // Pre-populate one file
      await mockBucket.put('data/users/data.parquet', localContent, {
        customMetadata: { 'x-parquedb-sha256': hash },
      })

      const result = await syncToR2(mockBucket, tempDir, 'data/')

      expect(result).toMatchObject({
        total: expect.any(Number),
        uploaded: expect.any(Number),
        skipped: expect.any(Number),
        failed: expect.any(Number),
      })
      expect(result.total).toBe(result.uploaded + result.skipped + result.failed)
    })

    it('should support dry-run mode without uploading', async () => {
      const result = await syncToR2(mockBucket, tempDir, 'data/', { dryRun: true })

      expect(result.dryRun).toBe(true)
      expect(result.wouldUpload).toBeGreaterThan(0)
      expect(mockBucket.put).not.toHaveBeenCalled()
    })

    it('should delete files in R2 that no longer exist locally when deleteOrphans is true', async () => {
      // Pre-populate bucket with a file that does not exist locally
      const orphanPath = 'data/deleted-collection/data.parquet'
      await mockBucket.put(orphanPath, createMockParquetContent('orphan'), {
        customMetadata: { 'x-parquedb-sha256': 'orphan-hash' },
      })

      const result = await syncToR2(mockBucket, tempDir, 'data/', { deleteOrphans: true })

      expect(result.deleted).toBe(1)
      expect(result.deletedFiles).toContain(orphanPath)
      expect(mockBucket.delete).toHaveBeenCalledWith(orphanPath)
    })
  })

  // ===========================================================================
  // Utility Functions
  // ===========================================================================

  describe('computeFileHash', () => {
    it('should compute SHA-256 hash of a file', async () => {
      const localPath = path.join(tempDir, 'data', 'users', 'data.parquet')
      const expectedHash = computeHash(fs.readFileSync(localPath))

      const result = await computeFileHash(localPath)

      expect(result).toBe(expectedHash)
    })

    it('should throw error for non-existent file', async () => {
      const localPath = path.join(tempDir, 'non-existent.parquet')

      await expect(computeFileHash(localPath)).rejects.toThrow(/ENOENT|not found/i)
    })

    it('should produce different hashes for different files', async () => {
      const path1 = path.join(tempDir, 'data', 'users', 'data.parquet')
      const path2 = path.join(tempDir, 'data', 'products', 'data.parquet')

      const hash1 = await computeFileHash(path1)
      const hash2 = await computeFileHash(path2)

      expect(hash1).not.toBe(hash2)
    })
  })
})
