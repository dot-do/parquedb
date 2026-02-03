/**
 * Integration Tests for R2Backend and FsxBackend
 *
 * These tests use mocks that simulate real Cloudflare R2 and fsx bindings.
 * The mocks implement the full interface contracts to ensure the backends
 * work correctly with the underlying storage systems.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { R2Backend, R2NotFoundError, R2ETagMismatchError, R2OperationError } from '../../src/storage/R2Backend'
import { FsxBackend } from '../../src/storage/FsxBackend'
import { NotFoundError, ETagMismatchError, AlreadyExistsError, DirectoryNotEmptyError } from '../../src/storage/errors'
import type { R2Bucket, R2Object, R2ObjectBody, R2Objects, R2MultipartUpload, R2UploadedPart, R2GetOptions, R2PutOptions, R2ListOptions, R2HTTPMetadata, R2MultipartOptions } from '../../src/storage/types/r2'
import type { Fsx, FsxStats, FsxStorageTier, FsxWriteResult, FsxError, FsxErrorCode } from '../../src/storage/types/fsx'
import { createTestData, decodeData, createRandomData } from '../factories'
import { BINARY_DATA } from '../fixtures'
import { generateEtag } from '../../src/storage/utils'

// =============================================================================
// Mock R2Bucket Implementation
// =============================================================================

/**
 * Mock R2Bucket that simulates Cloudflare R2 behavior in-memory
 */
function createMockR2Bucket(): R2Bucket {
  const storage = new Map<string, { data: Uint8Array; metadata: R2Object }>()
  const multipartUploads = new Map<string, { key: string; parts: Map<number, { data: Uint8Array; etag: string }> }>()
  let uploadIdCounter = 0

  function createR2Object(key: string, data: Uint8Array, httpMetadata?: R2HTTPMetadata, customMetadata?: Record<string, string>): R2Object {
    const etag = generateEtag(data)
    return {
      key,
      version: `v${Date.now()}`,
      size: data.length,
      etag,
      httpEtag: `"${etag}"`,
      uploaded: new Date(),
      httpMetadata,
      customMetadata,
      storageClass: 'Standard' as const,
      checksums: {},
      writeHttpMetadata: vi.fn(),
    }
  }

  function createR2ObjectBody(key: string, data: Uint8Array, r2Object: R2Object): R2ObjectBody {
    let bodyUsed = false
    return {
      ...r2Object,
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(data)
          controller.close()
        }
      }),
      bodyUsed,
      async arrayBuffer() {
        bodyUsed = true
        return data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
      },
      async text() {
        bodyUsed = true
        return new TextDecoder().decode(data)
      },
      async json<T>() {
        bodyUsed = true
        return JSON.parse(new TextDecoder().decode(data)) as T
      },
      async blob() {
        bodyUsed = true
        return new Blob([data])
      },
    }
  }

  return {
    async get(key: string, options?: R2GetOptions): Promise<R2ObjectBody | null> {
      const entry = storage.get(key)
      if (!entry) {
        return null
      }

      // Handle conditional get
      if (options?.onlyIf && !(options.onlyIf instanceof Headers)) {
        const cond = options.onlyIf
        if (cond.etagMatches && cond.etagMatches !== entry.metadata.etag) {
          return null
        }
        if (cond.etagDoesNotMatch && cond.etagDoesNotMatch === entry.metadata.etag) {
          return null
        }
      }

      // Handle range requests
      let data = entry.data
      if (options?.range && !(options.range instanceof Headers)) {
        const range = options.range
        const offset = range.offset ?? 0
        const length = range.length ?? (data.length - offset)
        data = data.slice(offset, offset + length)
      }

      return createR2ObjectBody(key, data, entry.metadata)
    },

    async head(key: string): Promise<R2Object | null> {
      const entry = storage.get(key)
      return entry ? entry.metadata : null
    },

    async put(
      key: string,
      value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob | null,
      options?: R2PutOptions
    ): Promise<R2Object | null> {
      // Handle conditional writes
      if (options?.onlyIf && !(options.onlyIf instanceof Headers)) {
        const cond = options.onlyIf
        const existing = storage.get(key)

        if (cond.etagMatches) {
          if (!existing || existing.metadata.etag !== cond.etagMatches) {
            return null // Condition failed
          }
        }

        if (cond.etagDoesNotMatch) {
          if (cond.etagDoesNotMatch === '*' && existing) {
            return null // File exists but we want it to not exist
          }
          if (existing && existing.metadata.etag === cond.etagDoesNotMatch) {
            return null // ETag matches but we want it to not match
          }
        }
      }

      // Convert value to Uint8Array
      let data: Uint8Array
      if (value === null) {
        data = new Uint8Array(0)
      } else if (value instanceof Uint8Array) {
        data = value
      } else if (value instanceof ArrayBuffer) {
        data = new Uint8Array(value)
      } else if (ArrayBuffer.isView(value)) {
        data = new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
      } else if (typeof value === 'string') {
        data = new TextEncoder().encode(value)
      } else if (value instanceof Blob) {
        data = new Uint8Array(await value.arrayBuffer())
      } else if (value instanceof ReadableStream) {
        // Read the entire stream
        const reader = value.getReader()
        const chunks: Uint8Array[] = []
        while (true) {
          const { done, value: chunk } = await reader.read()
          if (done) break
          chunks.push(chunk)
        }
        const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
        data = new Uint8Array(totalLength)
        let offset = 0
        for (const chunk of chunks) {
          data.set(chunk, offset)
          offset += chunk.length
        }
      } else {
        throw new Error('Unsupported value type')
      }

      const httpMetadata = options?.httpMetadata instanceof Headers ? undefined : options?.httpMetadata
      const metadata = createR2Object(key, data, httpMetadata, options?.customMetadata)
      storage.set(key, { data, metadata })
      return metadata
    },

    async delete(keys: string | string[]): Promise<void> {
      const keyArray = Array.isArray(keys) ? keys : [keys]
      for (const key of keyArray) {
        storage.delete(key)
      }
    },

    async list(options?: R2ListOptions): Promise<R2Objects> {
      const prefix = options?.prefix ?? ''
      const limit = options?.limit ?? 1000
      const delimiter = options?.delimiter
      const startAfter = options?.startAfter

      const objects: R2Object[] = []
      const prefixesSet = new Set<string>()

      // Get and sort all matching keys
      const allKeys = Array.from(storage.keys())
        .filter(key => key.startsWith(prefix))
        .filter(key => !startAfter || key > startAfter)
        .sort()

      for (const key of allKeys) {
        if (delimiter) {
          const relativeKey = key.slice(prefix.length)
          const delimIndex = relativeKey.indexOf(delimiter)
          if (delimIndex !== -1) {
            prefixesSet.add(prefix + relativeKey.slice(0, delimIndex + 1))
            continue
          }
        }

        const entry = storage.get(key)!
        objects.push(entry.metadata)

        if (objects.length >= limit) {
          break
        }
      }

      const truncated = objects.length >= limit && allKeys.length > objects.length

      return {
        objects,
        truncated,
        cursor: truncated ? objects[objects.length - 1]?.key : undefined,
        delimitedPrefixes: Array.from(prefixesSet).sort(),
      }
    },

    async createMultipartUpload(key: string, options?: R2MultipartOptions): Promise<R2MultipartUpload> {
      const uploadId = `upload-${++uploadIdCounter}`
      multipartUploads.set(uploadId, { key, parts: new Map() })

      return {
        uploadId,
        key,
        async uploadPart(partNumber: number, value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob): Promise<R2UploadedPart> {
          const upload = multipartUploads.get(uploadId)
          if (!upload) {
            throw new Error('Upload not found')
          }

          // Convert value to Uint8Array
          let data: Uint8Array
          if (value instanceof Uint8Array) {
            data = value
          } else if (value instanceof ArrayBuffer) {
            data = new Uint8Array(value)
          } else if (ArrayBuffer.isView(value)) {
            data = new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
          } else if (typeof value === 'string') {
            data = new TextEncoder().encode(value)
          } else if (value instanceof Blob) {
            data = new Uint8Array(await value.arrayBuffer())
          } else {
            // ReadableStream
            const reader = (value as ReadableStream).getReader()
            const chunks: Uint8Array[] = []
            while (true) {
              const { done, value: chunk } = await reader.read()
              if (done) break
              chunks.push(chunk)
            }
            const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0)
            data = new Uint8Array(totalLength)
            let offset = 0
            for (const chunk of chunks) {
              data.set(chunk, offset)
              offset += chunk.length
            }
          }

          const etag = generateEtag(data)
          upload.parts.set(partNumber, { data, etag })
          return { partNumber, etag }
        },
        async abort(): Promise<void> {
          multipartUploads.delete(uploadId)
        },
        async complete(uploadedParts: R2UploadedPart[]): Promise<R2Object> {
          const upload = multipartUploads.get(uploadId)
          if (!upload) {
            throw new Error('Upload not found')
          }

          // Sort parts by part number and concatenate
          const sortedParts = uploadedParts.sort((a, b) => a.partNumber - b.partNumber)
          const allData: Uint8Array[] = []
          for (const part of sortedParts) {
            const partData = upload.parts.get(part.partNumber)
            if (!partData || partData.etag !== part.etag) {
              throw new Error(`Invalid part: ${part.partNumber}`)
            }
            allData.push(partData.data)
          }

          const totalLength = allData.reduce((sum, d) => sum + d.length, 0)
          const combined = new Uint8Array(totalLength)
          let offset = 0
          for (const d of allData) {
            combined.set(d, offset)
            offset += d.length
          }

          const httpMetadata = options?.httpMetadata instanceof Headers ? undefined : options?.httpMetadata
          const metadata = createR2Object(key, combined, httpMetadata, options?.customMetadata)
          storage.set(key, { data: combined, metadata })
          multipartUploads.delete(uploadId)
          return metadata
        },
      }
    },

    resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload {
      const upload = multipartUploads.get(uploadId)
      if (!upload) {
        throw new Error('Upload not found')
      }
      return {
        uploadId,
        key,
        uploadPart: vi.fn(),
        abort: vi.fn(async () => { multipartUploads.delete(uploadId) }),
        complete: vi.fn(),
      }
    },
  }
}

// =============================================================================
// Mock Fsx Implementation
// =============================================================================

/**
 * Create a mock FsxError
 */
function createFsxError(code: FsxErrorCode, message: string, path?: string): FsxError {
  const error = new Error(message) as FsxError
  error.code = code
  error.path = path
  return error
}

/**
 * Mock Fsx that simulates the fsx filesystem in-memory
 */
function createMockFsx(): Fsx {
  const files = new Map<string, { data: Uint8Array; stats: FsxStats }>()
  const directories = new Set<string>()

  function createStats(path: string, data: Uint8Array, isDir = false, metadata?: Record<string, string>): FsxStats {
    const now = new Date()
    const etag = isDir ? undefined : generateEtag(data)
    return {
      size: data.length,
      atime: now,
      mtime: now,
      birthtime: now,
      ctime: now,
      mode: isDir ? 0o755 : 0o644,
      uid: 1000,
      gid: 1000,
      isFile: () => !isDir,
      isDirectory: () => isDir,
      isSymbolicLink: () => false,
      etag,
      metadata,
      tier: 'hot' as FsxStorageTier,
    }
  }

  function ensureParentDirs(path: string): void {
    const parts = path.split('/').slice(0, -1)
    let current = ''
    for (const part of parts) {
      current = current ? `${current}/${part}` : part
      directories.add(current)
    }
  }

  const fsx: Fsx = {
    async readFile(path: string): Promise<Uint8Array> {
      const entry = files.get(path)
      if (!entry) {
        throw createFsxError('ENOENT', `ENOENT: no such file or directory, open '${path}'`, path)
      }
      return new Uint8Array(entry.data)
    },

    async writeFile(path: string, data: Uint8Array | string, options?: { recursive?: boolean; contentType?: string; metadata?: Record<string, string>; tier?: FsxStorageTier; exclusive?: boolean; ifMatch?: string }): Promise<FsxWriteResult> {
      const dataBytes = typeof data === 'string' ? new TextEncoder().encode(data) : data

      // Handle exclusive (create only if not exists)
      if (options?.exclusive && files.has(path)) {
        throw createFsxError('EEXIST', `EEXIST: file already exists, '${path}'`, path)
      }

      // Handle ifMatch (conditional write based on etag)
      if (options?.ifMatch) {
        const existing = files.get(path)
        if (!existing) {
          throw createFsxError('ENOENT', `ENOENT: no such file or directory, '${path}'`, path)
        }
        if (existing.stats.etag !== options.ifMatch) {
          throw createFsxError('ECONFLICT', `ECONFLICT: etag mismatch, '${path}'`, path)
        }
      }

      if (options?.recursive) {
        ensureParentDirs(path)
      }

      const stats = createStats(path, dataBytes, false, options?.metadata)
      files.set(path, { data: new Uint8Array(dataBytes), stats })

      return {
        etag: stats.etag!,
        size: dataBytes.length,
        tier: options?.tier ?? 'hot',
      }
    },

    async writeFileAtomic(path: string, data: Uint8Array | string, options?: { recursive?: boolean; contentType?: string; metadata?: Record<string, string>; tier?: FsxStorageTier }): Promise<FsxWriteResult> {
      // In mock, same as writeFile
      return fsx.writeFile(path, data, options)
    },

    async appendFile(path: string, data: Uint8Array | string): Promise<void> {
      const dataBytes = typeof data === 'string' ? new TextEncoder().encode(data) : data
      const existing = files.get(path)

      if (existing) {
        const newData = new Uint8Array(existing.data.length + dataBytes.length)
        newData.set(existing.data, 0)
        newData.set(dataBytes, existing.data.length)
        existing.data = newData
        existing.stats = createStats(path, newData)
      } else {
        const stats = createStats(path, dataBytes)
        files.set(path, { data: new Uint8Array(dataBytes), stats })
      }
    },

    async unlink(path: string): Promise<void> {
      if (!files.has(path)) {
        throw createFsxError('ENOENT', `ENOENT: no such file or directory, unlink '${path}'`, path)
      }
      files.delete(path)
    },

    async exists(path: string): Promise<boolean> {
      return files.has(path) || directories.has(path)
    },

    async stat(path: string): Promise<FsxStats> {
      const entry = files.get(path)
      if (entry) {
        return { ...entry.stats }
      }
      if (directories.has(path)) {
        return createStats(path, new Uint8Array(0), true)
      }
      throw createFsxError('ENOENT', `ENOENT: no such file or directory, stat '${path}'`, path)
    },

    async lstat(path: string): Promise<FsxStats> {
      return fsx.stat(path)
    },

    async mkdir(path: string, options?: { recursive?: boolean }): Promise<void> {
      if (options?.recursive) {
        const parts = path.split('/').filter(Boolean)
        let current = ''
        for (const part of parts) {
          current = current ? `${current}/${part}` : part
          directories.add(current)
        }
      } else {
        directories.add(path)
      }
    },

    async rmdir(path: string, options?: { recursive?: boolean }): Promise<void> {
      const pathPrefix = path.endsWith('/') ? path : path + '/'

      // Check if directory has files
      const hasFiles = Array.from(files.keys()).some(f => f.startsWith(pathPrefix))

      if (hasFiles && !options?.recursive) {
        throw createFsxError('ENOTEMPTY', `ENOTEMPTY: directory not empty, '${path}'`, path)
      }

      if (options?.recursive) {
        // Delete all files under this directory
        for (const filePath of Array.from(files.keys())) {
          if (filePath.startsWith(pathPrefix)) {
            files.delete(filePath)
          }
        }
        // Delete all subdirectories
        for (const dir of Array.from(directories)) {
          if (dir.startsWith(path)) {
            directories.delete(dir)
          }
        }
      }

      directories.delete(path)
    },

    async readdir(path: string, options?: { withFileTypes?: boolean }): Promise<string[] | Array<{ name: string; isFile(): boolean; isDirectory(): boolean }>> {
      const pathPrefix = path.endsWith('/') ? path : path + '/'
      const entries: Array<{ name: string; isFile: boolean }> = []
      const seenNames = new Set<string>()

      // Find files directly under this path
      for (const filePath of files.keys()) {
        if (filePath.startsWith(pathPrefix)) {
          const relativePath = filePath.slice(pathPrefix.length)
          const firstPart = relativePath.split('/')[0]
          if (!seenNames.has(firstPart) && !relativePath.includes('/')) {
            entries.push({ name: firstPart, isFile: true })
            seenNames.add(firstPart)
          }
        }
      }

      // Find subdirectories
      for (const dir of directories) {
        if (dir.startsWith(pathPrefix) && dir !== path) {
          const relativePath = dir.slice(pathPrefix.length)
          const firstPart = relativePath.split('/')[0]
          if (!seenNames.has(firstPart)) {
            entries.push({ name: firstPart, isFile: false })
            seenNames.add(firstPart)
          }
        }
      }

      if (options?.withFileTypes) {
        return entries.map(e => ({
          name: e.name,
          isFile: () => e.isFile,
          isDirectory: () => !e.isFile,
        }))
      }

      return entries.map(e => e.name)
    },

    async cp(source: string, dest: string): Promise<void> {
      const entry = files.get(source)
      if (!entry) {
        throw createFsxError('ENOENT', `ENOENT: no such file or directory, '${source}'`, source)
      }
      files.set(dest, { data: new Uint8Array(entry.data), stats: createStats(dest, entry.data) })
    },

    async rename(oldPath: string, newPath: string): Promise<void> {
      const entry = files.get(oldPath)
      if (!entry) {
        throw createFsxError('ENOENT', `ENOENT: no such file or directory, '${oldPath}'`, oldPath)
      }
      files.set(newPath, entry)
      files.delete(oldPath)
    },

    async readRange(path: string, start: number, end: number): Promise<Uint8Array> {
      const entry = files.get(path)
      if (!entry) {
        throw createFsxError('ENOENT', `ENOENT: no such file or directory, '${path}'`, path)
      }
      return entry.data.slice(start, end)
    },

    async glob(pattern: string): Promise<string[]> {
      // Simple glob implementation - just matches prefix
      const prefix = pattern.replace(/\*.*$/, '')
      const results: string[] = []
      for (const filePath of files.keys()) {
        if (filePath.startsWith(prefix)) {
          results.push(filePath)
        }
      }
      return results.sort()
    },

    async access(): Promise<void> {
      // Always succeeds in mock
    },

    async truncate(path: string, length = 0): Promise<void> {
      const entry = files.get(path)
      if (!entry) {
        throw createFsxError('ENOENT', `ENOENT: no such file or directory, '${path}'`, path)
      }
      entry.data = entry.data.slice(0, length)
      entry.stats = createStats(path, entry.data)
    },

    async getTier(path: string): Promise<FsxStorageTier> {
      const entry = files.get(path)
      if (!entry) {
        throw createFsxError('ENOENT', `ENOENT: no such file or directory, '${path}'`, path)
      }
      return entry.stats.tier ?? 'hot'
    },

    async setTier(path: string, tier: FsxStorageTier): Promise<void> {
      const entry = files.get(path)
      if (!entry) {
        throw createFsxError('ENOENT', `ENOENT: no such file or directory, '${path}'`, path)
      }
      entry.stats.tier = tier
    },

    async promote(path: string): Promise<void> {
      await fsx.setTier(path, 'hot')
    },

    async demote(path: string): Promise<void> {
      await fsx.setTier(path, 'cold')
    },

    async storageStats() {
      let totalFiles = 0
      let totalSize = 0
      let hotFiles = 0
      let hotSize = 0
      let warmFiles = 0
      let warmSize = 0
      let coldFiles = 0
      let coldSize = 0

      for (const entry of files.values()) {
        totalFiles++
        totalSize += entry.data.length
        switch (entry.stats.tier) {
          case 'hot':
            hotFiles++
            hotSize += entry.data.length
            break
          case 'warm':
            warmFiles++
            warmSize += entry.data.length
            break
          case 'cold':
            coldFiles++
            coldSize += entry.data.length
            break
        }
      }

      return { totalFiles, totalSize, hotFiles, hotSize, warmFiles, warmSize, coldFiles, coldSize }
    },

    async beginTransaction() {
      return {
        id: `tx-${Date.now()}`,
        readFile: fsx.readFile,
        writeFile: fsx.writeFile as (path: string, data: Uint8Array | string) => Promise<void>,
        unlink: fsx.unlink,
        commit: async () => {},
        rollback: async () => {},
      }
    },
  }

  return fsx
}

// =============================================================================
// R2Backend Integration Tests
// =============================================================================

describe('R2Backend Integration Tests', () => {
  let bucket: R2Bucket
  let backend: R2Backend

  beforeEach(() => {
    bucket = createMockR2Bucket()
    backend = new R2Backend(bucket)
  })

  describe('Basic Read/Write Operations', () => {
    it('should write and read text data', async () => {
      const data = createTestData('Hello, R2!')
      await backend.write('test.txt', data)

      const result = await backend.read('test.txt')
      expect(decodeData(result)).toBe('Hello, R2!')
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

    it('should throw R2NotFoundError for missing files', async () => {
      await expect(backend.read('missing.txt')).rejects.toThrow(R2NotFoundError)
    })

    it('should handle empty files', async () => {
      await backend.write('empty.txt', new Uint8Array(0))
      const result = await backend.read('empty.txt')
      expect(result.length).toBe(0)
    })
  })

  describe('File Existence and Stats', () => {
    it('should check file existence correctly', async () => {
      expect(await backend.exists('test.txt')).toBe(false)
      await backend.write('test.txt', createTestData('content'))
      expect(await backend.exists('test.txt')).toBe(true)
    })

    it('should return file stats', async () => {
      const data = createTestData('Hello, World!')
      const writeResult = await backend.write('test.txt', data)

      const stat = await backend.stat('test.txt')
      expect(stat).not.toBeNull()
      expect(stat!.path).toBe('test.txt')
      expect(stat!.size).toBe(data.length)
      expect(stat!.etag).toBe(writeResult.etag)
      expect(stat!.mtime).toBeInstanceOf(Date)
      expect(stat!.isDirectory).toBe(false)
    })

    it('should return null for non-existent file stat', async () => {
      const stat = await backend.stat('nonexistent.txt')
      expect(stat).toBeNull()
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

    it('should handle range at file start', async () => {
      const result = await backend.readRange('alphabet.txt', 0, 3)
      expect(decodeData(result)).toBe('ABC')
    })

    it('should handle range at file end', async () => {
      const result = await backend.readRange('alphabet.txt', 7, 10)
      expect(decodeData(result)).toBe('HIJ')
    })
  })

  describe('File Deletion', () => {
    it('should delete existing files', async () => {
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

    it('should delete files with prefix', async () => {
      await backend.write('data/a.txt', createTestData('a'))
      await backend.write('data/b.txt', createTestData('b'))
      await backend.write('data/sub/c.txt', createTestData('c'))
      await backend.write('other/d.txt', createTestData('d'))

      const count = await backend.deletePrefix('data/')
      expect(count).toBe(3)
      expect(await backend.exists('data/a.txt')).toBe(false)
      expect(await backend.exists('data/b.txt')).toBe(false)
      expect(await backend.exists('data/sub/c.txt')).toBe(false)
      expect(await backend.exists('other/d.txt')).toBe(true)
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
      const result = await backend.list('data/')

      expect(result.files).toContain('data/users/user1.json')
      expect(result.files).toContain('data/users/user2.json')
      expect(result.files).toContain('data/posts/post1.json')
      expect(result.files).toContain('data/config.json')
      expect(result.files).not.toContain('readme.txt')
    })

    it('should list with delimiter for directory-like behavior', async () => {
      const result = await backend.list('data/', { delimiter: '/' })

      expect(result.files).toContain('data/config.json')
      expect(result.prefixes).toContain('data/users/')
      expect(result.prefixes).toContain('data/posts/')
    })

    it('should paginate results', async () => {
      const page1 = await backend.list('data/', { limit: 2 })
      expect(page1.files.length).toBe(2)
      expect(page1.hasMore).toBe(true)

      const page2 = await backend.list('data/', { limit: 2, cursor: page1.cursor })
      expect(page2.files.length).toBeLessThanOrEqual(2)
    })

    it('should include metadata when requested', async () => {
      const result = await backend.list('data/', { includeMetadata: true, limit: 2 })
      expect(result.stats).toBeDefined()
      expect(result.stats!.length).toBe(2)
      expect(result.stats![0].etag).toBeDefined()
      expect(result.stats![0].size).toBeDefined()
    })
  })

  describe('Conditional Writes', () => {
    it('should write only if file does not exist (ifNoneMatch)', async () => {
      const result = await backend.write('new.txt', createTestData('first'), {
        ifNoneMatch: '*',
      })
      expect(result.etag).toBeDefined()

      // Second write should fail
      await expect(
        backend.write('new.txt', createTestData('second'), { ifNoneMatch: '*' })
      ).rejects.toThrow(R2ETagMismatchError)
    })

    it('should support writeConditional with etag matching', async () => {
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
      ).rejects.toThrow(R2ETagMismatchError)
    })

    it('should support writeConditional for new files (expectedVersion null)', async () => {
      // Should succeed for new file
      const result = await backend.writeConditional(
        'brand-new.txt',
        createTestData('content'),
        null
      )
      expect(result.etag).toBeDefined()

      // Should fail if file already exists
      await expect(
        backend.writeConditional('brand-new.txt', createTestData('content2'), null)
      ).rejects.toThrow(R2ETagMismatchError)
    })
  })

  describe('Copy and Move Operations', () => {
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

    it('should throw when copying non-existent file', async () => {
      await expect(backend.copy('missing.txt', 'dest.txt')).rejects.toThrow(R2NotFoundError)
    })

    it('should throw when moving non-existent file', async () => {
      await expect(backend.move('missing.txt', 'dest.txt')).rejects.toThrow(R2NotFoundError)
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

    it('should create file if it does not exist when appending', async () => {
      await backend.append('new.txt', createTestData('First line'))

      expect(await backend.exists('new.txt')).toBe(true)
      expect(decodeData(await backend.read('new.txt'))).toBe('First line')
    })
  })

  describe('Multipart Upload', () => {
    it('should create and complete multipart upload', async () => {
      const upload = await backend.createMultipartUpload('large-file.bin')
      expect(upload.uploadId).toBeDefined()

      const part1 = await upload.uploadPart(1, createRandomData(1024))
      const part2 = await upload.uploadPart(2, createRandomData(1024))

      const result = await upload.complete([
        { partNumber: 1, etag: part1.etag, size: 1024 },
        { partNumber: 2, etag: part2.etag, size: 1024 },
      ])

      expect(result.size).toBe(2048)
      expect(await backend.exists('large-file.bin')).toBe(true)
    })

    it('should abort multipart upload', async () => {
      const upload = await backend.createMultipartUpload('aborted.bin')
      await upload.uploadPart(1, createRandomData(1024))
      await upload.abort()

      expect(await backend.exists('aborted.bin')).toBe(false)
    })

    it('should support standalone multipart upload methods', async () => {
      const uploadId = await backend.startMultipartUpload('standalone.bin')
      expect(uploadId).toBeDefined()

      const part1 = await backend.uploadPart('standalone.bin', uploadId, 1, createRandomData(512))
      const part2 = await backend.uploadPart('standalone.bin', uploadId, 2, createRandomData(512))

      await backend.completeMultipartUpload('standalone.bin', uploadId, [
        { partNumber: 1, etag: part1.etag },
        { partNumber: 2, etag: part2.etag },
      ])

      expect(await backend.exists('standalone.bin')).toBe(true)
      const data = await backend.read('standalone.bin')
      expect(data.length).toBe(1024)
    })

    it('should abort standalone multipart upload', async () => {
      const uploadId = await backend.startMultipartUpload('to-abort.bin')
      await backend.uploadPart('to-abort.bin', uploadId, 1, createRandomData(512))
      await backend.abortMultipartUpload('to-abort.bin', uploadId)

      expect(backend.activeUploadCount).toBe(0)
    })
  })

  describe('Prefix Support', () => {
    it('should apply prefix to all operations', async () => {
      const prefixedBackend = new R2Backend(bucket, { prefix: 'myapp/' })

      await prefixedBackend.write('data.txt', createTestData('content'))

      // The underlying bucket should have the prefixed key
      expect(await prefixedBackend.exists('data.txt')).toBe(true)
      expect(decodeData(await prefixedBackend.read('data.txt'))).toBe('content')

      // List should return paths without prefix
      const result = await prefixedBackend.list('')
      expect(result.files).toContain('data.txt')
    })
  })

  describe('Streaming Write', () => {
    it('should write small data directly', async () => {
      const data = createTestData('small content')
      const result = await backend.writeStreaming('small.txt', data)

      expect(result.etag).toBeDefined()
      expect(result.size).toBe(data.length)
      expect(decodeData(await backend.read('small.txt'))).toBe('small content')
    })
  })

  describe('Directory Operations', () => {
    it('should handle mkdir as no-op', async () => {
      // R2 doesn't have real directories, so mkdir should be a no-op
      await backend.mkdir('some/nested/path')
      // Should not throw
    })

    it('should handle rmdir with recursive option', async () => {
      await backend.write('dir/file1.txt', createTestData('1'))
      await backend.write('dir/file2.txt', createTestData('2'))

      await backend.rmdir('dir/', { recursive: true })

      expect(await backend.exists('dir/file1.txt')).toBe(false)
      expect(await backend.exists('dir/file2.txt')).toBe(false)
    })
  })
})

// =============================================================================
// FsxBackend Integration Tests
// =============================================================================

describe('FsxBackend Integration Tests', () => {
  let fsx: Fsx
  let backend: FsxBackend

  beforeEach(() => {
    fsx = createMockFsx()
    backend = new FsxBackend(fsx)
  })

  describe('Basic Read/Write Operations', () => {
    it('should write and read text data', async () => {
      const data = createTestData('Hello, Fsx!')
      await backend.write('test.txt', data)

      const result = await backend.read('test.txt')
      expect(decodeData(result)).toBe('Hello, Fsx!')
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

    it('should throw NotFoundError for missing files', async () => {
      await expect(backend.read('missing.txt')).rejects.toThrow(NotFoundError)
    })

    it('should write with recursive option', async () => {
      await backend.write('deep/nested/path/file.txt', createTestData('content'))
      expect(await backend.exists('deep/nested/path/file.txt')).toBe(true)
    })
  })

  describe('File Existence and Stats', () => {
    it('should check file existence correctly', async () => {
      expect(await backend.exists('test.txt')).toBe(false)
      await backend.write('test.txt', createTestData('content'))
      expect(await backend.exists('test.txt')).toBe(true)
    })

    it('should return file stats', async () => {
      const data = createTestData('Hello, World!')
      const writeResult = await backend.write('test.txt', data)

      const stat = await backend.stat('test.txt')
      expect(stat).not.toBeNull()
      expect(stat!.path).toBe('test.txt')
      expect(stat!.size).toBe(data.length)
      expect(stat!.etag).toBe(writeResult.etag)
      expect(stat!.mtime).toBeInstanceOf(Date)
      expect(stat!.isDirectory).toBe(false)
    })

    it('should return null for non-existent file stat', async () => {
      const stat = await backend.stat('nonexistent.txt')
      expect(stat).toBeNull()
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

    it('should handle range at file start', async () => {
      const result = await backend.readRange('alphabet.txt', 0, 3)
      expect(decodeData(result)).toBe('ABC')
    })

    it('should handle range at file end', async () => {
      const result = await backend.readRange('alphabet.txt', 7, 10)
      expect(decodeData(result)).toBe('HIJ')
    })
  })

  describe('File Deletion', () => {
    it('should delete existing files', async () => {
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

    it('should delete files with prefix', async () => {
      await backend.write('data/a.txt', createTestData('a'))
      await backend.write('data/b.txt', createTestData('b'))
      await backend.write('data/sub/c.txt', createTestData('c'))
      await backend.write('other/d.txt', createTestData('d'))

      const count = await backend.deletePrefix('data/')
      expect(count).toBe(3)
      expect(await backend.exists('data/a.txt')).toBe(false)
      expect(await backend.exists('data/b.txt')).toBe(false)
      expect(await backend.exists('data/sub/c.txt')).toBe(false)
      expect(await backend.exists('other/d.txt')).toBe(true)
    })
  })

  describe('Directory Operations', () => {
    it('should create directories', async () => {
      await backend.mkdir('mydir')
      // In fsx, mkdir is tracked
    })

    it('should remove empty directories', async () => {
      await backend.mkdir('emptydir')
      await backend.rmdir('emptydir')
      // Should not throw
    })

    it('should throw when removing non-empty directory without recursive', async () => {
      await backend.write('dir/file.txt', createTestData('content'))
      await expect(backend.rmdir('dir')).rejects.toThrow(DirectoryNotEmptyError)
    })

    it('should remove directory recursively', async () => {
      await backend.write('dir/file1.txt', createTestData('1'))
      await backend.write('dir/file2.txt', createTestData('2'))
      await backend.write('dir/sub/file3.txt', createTestData('3'))

      await backend.rmdir('dir', { recursive: true })

      expect(await backend.exists('dir/file1.txt')).toBe(false)
      expect(await backend.exists('dir/file2.txt')).toBe(false)
      expect(await backend.exists('dir/sub/file3.txt')).toBe(false)
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

    it('should list files with prefix using glob', async () => {
      const result = await backend.list('data/')

      expect(result.files).toContain('data/users/user1.json')
      expect(result.files).toContain('data/users/user2.json')
      expect(result.files).toContain('data/posts/post1.json')
      expect(result.files).toContain('data/config.json')
      expect(result.files).not.toContain('readme.txt')
    })

    it('should list with delimiter for directory-like behavior', async () => {
      const result = await backend.list('data/', { delimiter: '/' })

      expect(result.files).toContain('data/config.json')
      expect(result.prefixes).toContain('data/users/')
      expect(result.prefixes).toContain('data/posts/')
    })

    it('should paginate results', async () => {
      const page1 = await backend.list('data/', { limit: 2 })
      expect(page1.files.length).toBe(2)
      expect(page1.hasMore).toBe(true)

      const page2 = await backend.list('data/', { limit: 2, cursor: page1.cursor })
      expect(page2.files.length).toBeLessThanOrEqual(2)
    })
  })

  describe('Conditional Writes', () => {
    it('should write only if file does not exist using ifNoneMatch', async () => {
      const result = await backend.writeConditional(
        'new.txt',
        createTestData('first'),
        null,
        { ifNoneMatch: '*' }
      )
      expect(result.etag).toBeDefined()

      // Second write should fail (file already exists)
      await expect(
        backend.writeConditional('new.txt', createTestData('second'), null, { ifNoneMatch: '*' })
      ).rejects.toThrow(AlreadyExistsError)
    })

    it('should support writeConditional with etag matching', async () => {
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
      ).rejects.toThrow(ETagMismatchError)
    })

    it('should support writeConditional for new files (expectedVersion null)', async () => {
      // Should succeed for new file
      const result = await backend.writeConditional(
        'brand-new.txt',
        createTestData('content'),
        null
      )
      expect(result.etag).toBeDefined()

      // Should fail if file already exists
      await expect(
        backend.writeConditional('brand-new.txt', createTestData('content2'), null)
      ).rejects.toThrow(ETagMismatchError)
    })
  })

  describe('Copy and Move Operations', () => {
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

    it('should throw when copying non-existent file', async () => {
      await expect(backend.copy('missing.txt', 'dest.txt')).rejects.toThrow(NotFoundError)
    })

    it('should throw when moving non-existent file', async () => {
      await expect(backend.move('missing.txt', 'dest.txt')).rejects.toThrow(NotFoundError)
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

    it('should create file if it does not exist when appending', async () => {
      await backend.append('new.txt', createTestData('First line'))

      expect(await backend.exists('new.txt')).toBe(true)
      expect(decodeData(await backend.read('new.txt'))).toBe('First line')
    })
  })

  describe('Atomic Write', () => {
    it('should write atomically', async () => {
      const data = createTestData('atomic content')
      const result = await backend.writeAtomic('atomic.txt', data)

      expect(result.etag).toBeDefined()
      expect(result.size).toBe(data.length)
      expect(decodeData(await backend.read('atomic.txt'))).toBe('atomic content')
    })
  })

  describe('Root Path Prefix', () => {
    it('should apply root prefix to all operations', async () => {
      const prefixedBackend = new FsxBackend(fsx, { root: 'myapp' })

      await prefixedBackend.write('data.txt', createTestData('content'))

      expect(await prefixedBackend.exists('data.txt')).toBe(true)
      expect(decodeData(await prefixedBackend.read('data.txt'))).toBe('content')
    })
  })

  describe('Tiered Storage Operations', () => {
    it('should get storage tier', async () => {
      await backend.write('file.txt', createTestData('content'))
      const tier = await backend.getTier('file.txt')
      expect(['hot', 'warm', 'cold']).toContain(tier)
    })

    it('should set storage tier', async () => {
      await backend.write('file.txt', createTestData('content'))
      await backend.setTier('file.txt', 'cold')
      const tier = await backend.getTier('file.txt')
      expect(tier).toBe('cold')
    })

    it('should promote file to hot tier', async () => {
      await backend.write('file.txt', createTestData('content'))
      await backend.demote('file.txt')
      await backend.promote('file.txt')
      const tier = await backend.getTier('file.txt')
      expect(tier).toBe('hot')
    })

    it('should demote file to cold tier', async () => {
      await backend.write('file.txt', createTestData('content'))
      await backend.demote('file.txt')
      const tier = await backend.getTier('file.txt')
      expect(tier).toBe('cold')
    })

    it('should throw when getting tier for non-existent file', async () => {
      await expect(backend.getTier('missing.txt')).rejects.toThrow(NotFoundError)
    })

    it('should throw when setting tier for non-existent file', async () => {
      await expect(backend.setTier('missing.txt', 'cold')).rejects.toThrow(NotFoundError)
    })
  })

  describe('Unicode and Special Characters', () => {
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
})

// =============================================================================
// Cross-Backend Compatibility Tests
// =============================================================================

describe('Cross-Backend Compatibility', () => {
  /**
   * These tests verify that R2Backend and FsxBackend behave consistently
   * for the same operations, ensuring code can work with either backend.
   */

  interface BackendFactory {
    name: string
    create: () => { backend: R2Backend | FsxBackend; cleanup?: () => void }
  }

  const backends: BackendFactory[] = [
    {
      name: 'R2Backend',
      create: () => {
        const bucket = createMockR2Bucket()
        return { backend: new R2Backend(bucket) }
      },
    },
    {
      name: 'FsxBackend',
      create: () => {
        const fsx = createMockFsx()
        return { backend: new FsxBackend(fsx) }
      },
    },
  ]

  for (const factory of backends) {
    describe(`${factory.name} - Standard StorageBackend Contract`, () => {
      let backend: R2Backend | FsxBackend

      beforeEach(() => {
        const created = factory.create()
        backend = created.backend
      })

      it('should implement type property', () => {
        expect(backend.type).toBeDefined()
        expect(typeof backend.type).toBe('string')
      })

      it('should implement read/write', async () => {
        const data = createTestData('test data')
        await backend.write('test.txt', data)
        const result = await backend.read('test.txt')
        expect(decodeData(result)).toBe('test data')
      })

      it('should implement readRange', async () => {
        await backend.write('range.txt', createTestData('0123456789'))
        const result = await backend.readRange('range.txt', 3, 7)
        expect(decodeData(result)).toBe('3456')
      })

      it('should implement exists', async () => {
        expect(await backend.exists('missing.txt')).toBe(false)
        await backend.write('exists.txt', createTestData('x'))
        expect(await backend.exists('exists.txt')).toBe(true)
      })

      it('should implement stat', async () => {
        const data = createTestData('stat test')
        await backend.write('stat.txt', data)
        const stat = await backend.stat('stat.txt')

        expect(stat).not.toBeNull()
        expect(stat!.path).toBe('stat.txt')
        expect(stat!.size).toBe(data.length)
        expect(stat!.etag).toBeDefined()
      })

      it('should implement delete', async () => {
        await backend.write('delete.txt', createTestData('x'))
        const deleted = await backend.delete('delete.txt')
        expect(deleted).toBe(true)
        expect(await backend.exists('delete.txt')).toBe(false)
      })

      it('should implement deletePrefix', async () => {
        await backend.write('prefix/a.txt', createTestData('a'))
        await backend.write('prefix/b.txt', createTestData('b'))
        const count = await backend.deletePrefix('prefix/')
        expect(count).toBe(2)
      })

      it('should implement list', async () => {
        await backend.write('list/a.txt', createTestData('a'))
        await backend.write('list/b.txt', createTestData('b'))
        const result = await backend.list('list/')
        expect(result.files.length).toBe(2)
      })

      it('should implement copy', async () => {
        await backend.write('copy-src.txt', createTestData('copy'))
        await backend.copy('copy-src.txt', 'copy-dst.txt')
        expect(await backend.exists('copy-src.txt')).toBe(true)
        expect(await backend.exists('copy-dst.txt')).toBe(true)
      })

      it('should implement move', async () => {
        await backend.write('move-src.txt', createTestData('move'))
        await backend.move('move-src.txt', 'move-dst.txt')
        expect(await backend.exists('move-src.txt')).toBe(false)
        expect(await backend.exists('move-dst.txt')).toBe(true)
      })

      it('should implement append', async () => {
        await backend.write('append.txt', createTestData('a'))
        await backend.append('append.txt', createTestData('b'))
        const result = decodeData(await backend.read('append.txt'))
        expect(result).toBe('ab')
      })

      it('should implement writeConditional', async () => {
        const write1 = await backend.write('cond.txt', createTestData('v1'))
        const write2 = await backend.writeConditional('cond.txt', createTestData('v2'), write1.etag)
        expect(write2.etag).not.toBe(write1.etag)
      })

      it('should implement mkdir and rmdir', async () => {
        await backend.mkdir('testdir')
        await backend.rmdir('testdir')
        // Should not throw
      })

      it('should implement writeAtomic', async () => {
        const result = await backend.writeAtomic('atomic.txt', createTestData('atomic'))
        expect(result.etag).toBeDefined()
      })
    })
  }
})
