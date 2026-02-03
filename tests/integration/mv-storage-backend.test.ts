/**
 * R2/S3 Storage Backend MV Integration Tests
 *
 * Tests materialized view operations with cloud storage backends:
 * - R2Backend (Cloudflare R2)
 * - S3-compatible backends
 *
 * Uses mock R2 bucket to enable testing without real cloud credentials.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { R2Backend, R2OperationError, R2NotFoundError } from '../../src/storage/R2Backend'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import {
  MVStorageManager,
  MVStoragePaths,
  MVNotFoundError,
  MVAlreadyExistsError,
} from '../../src/materialized-views/storage'
import { viewName } from '../../src/materialized-views/types'
import type { ViewDefinition } from '../../src/materialized-views/types'
import type { StorageBackend } from '../../src/types/storage'
import type {
  R2Bucket,
  R2Object,
  R2ObjectBody,
  R2Objects,
  R2MultipartUpload,
  R2UploadedPart,
  R2PutOptions,
  R2GetOptions,
  R2ListOptions,
  R2MultipartOptions,
} from '../../src/storage/types/r2'

// =============================================================================
// Mock R2 Bucket Implementation
// =============================================================================

/**
 * Mock R2Bucket that implements the full R2Bucket interface for testing.
 * This enables testing R2Backend without real Cloudflare R2 credentials.
 */
class MockR2Bucket implements R2Bucket {
  private store = new Map<string, { data: Uint8Array; metadata: Partial<R2Object> }>()
  private uploads = new Map<string, { key: string; parts: Map<number, { data: Uint8Array; etag: string }> }>()

  /** Enable simulating errors for testing error handling */
  public simulateError: { operation?: string; error?: Error } = {}

  private generateEtag(data: Uint8Array): string {
    // Simple hash for testing - not cryptographically secure
    let hash = 0
    for (let i = 0; i < data.length; i++) {
      hash = ((hash << 5) - hash + data[i]) | 0
    }
    return Math.abs(hash).toString(16).padStart(8, '0')
  }

  async get(key: string, options?: R2GetOptions): Promise<R2ObjectBody | null> {
    if (this.simulateError.operation === 'get') {
      throw this.simulateError.error ?? new Error('Simulated get error')
    }

    const entry = this.store.get(key)
    if (!entry) return null

    let data = entry.data

    // Handle range requests
    if (options?.range && typeof options.range === 'object' && 'offset' in options.range) {
      const range = options.range as { offset?: number; length?: number }
      const start = range.offset ?? 0
      const length = range.length ?? (data.length - start)
      data = data.slice(start, start + length)
    }

    return this.createR2ObjectBody(key, data, entry.metadata)
  }

  async head(key: string): Promise<R2Object | null> {
    if (this.simulateError.operation === 'head') {
      throw this.simulateError.error ?? new Error('Simulated head error')
    }

    const entry = this.store.get(key)
    if (!entry) return null

    return this.createR2Object(key, entry.data.length, entry.metadata)
  }

  async put(
    key: string,
    value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob | null,
    options?: R2PutOptions
  ): Promise<R2Object | null> {
    if (this.simulateError.operation === 'put') {
      throw this.simulateError.error ?? new Error('Simulated put error')
    }

    // Handle conditional writes
    if (options?.onlyIf && typeof options.onlyIf === 'object') {
      const conditional = options.onlyIf as { etagMatches?: string; etagDoesNotMatch?: string }
      const existing = this.store.get(key)

      if (conditional.etagMatches) {
        if (!existing || existing.metadata.etag !== conditional.etagMatches) {
          return null // Precondition failed
        }
      }

      if (conditional.etagDoesNotMatch === '*') {
        if (existing) {
          return null // Object exists but shouldn't
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
      const chunks: Uint8Array[] = []
      const reader = value.getReader()
      while (true) {
        const { done, value: chunk } = await reader.read()
        if (done) break
        chunks.push(chunk)
      }
      const totalLength = chunks.reduce((acc, c) => acc + c.length, 0)
      data = new Uint8Array(totalLength)
      let offset = 0
      for (const chunk of chunks) {
        data.set(chunk, offset)
        offset += chunk.length
      }
    } else {
      throw new Error('Unsupported value type')
    }

    const etag = this.generateEtag(data)
    const metadata: Partial<R2Object> = {
      etag,
      httpEtag: `"${etag}"`,
      size: data.length,
      uploaded: new Date(),
      httpMetadata: typeof options?.httpMetadata === 'object' && !(options.httpMetadata instanceof Headers)
        ? options.httpMetadata
        : undefined,
      customMetadata: options?.customMetadata,
    }

    this.store.set(key, { data, metadata })

    return this.createR2Object(key, data.length, metadata)
  }

  async delete(keys: string | string[]): Promise<void> {
    if (this.simulateError.operation === 'delete') {
      throw this.simulateError.error ?? new Error('Simulated delete error')
    }

    const keyArray = Array.isArray(keys) ? keys : [keys]
    for (const key of keyArray) {
      this.store.delete(key)
    }
  }

  async list(options?: R2ListOptions): Promise<R2Objects> {
    if (this.simulateError.operation === 'list') {
      throw this.simulateError.error ?? new Error('Simulated list error')
    }

    const prefix = options?.prefix ?? ''
    const limit = options?.limit ?? 1000
    const delimiter = options?.delimiter

    const allKeys = Array.from(this.store.keys()).filter(k => k.startsWith(prefix)).sort()

    const objects: R2Object[] = []
    const delimitedPrefixes = new Set<string>()

    for (const key of allKeys) {
      if (objects.length >= limit) break

      if (delimiter) {
        const relativePath = key.slice(prefix.length)
        const delimIndex = relativePath.indexOf(delimiter)
        if (delimIndex !== -1) {
          delimitedPrefixes.add(prefix + relativePath.slice(0, delimIndex + 1))
          continue
        }
      }

      const entry = this.store.get(key)!
      objects.push(this.createR2Object(key, entry.data.length, entry.metadata))
    }

    return {
      objects,
      truncated: objects.length === limit,
      cursor: objects.length === limit ? objects[objects.length - 1]?.key : undefined,
      delimitedPrefixes: Array.from(delimitedPrefixes),
    }
  }

  async createMultipartUpload(key: string, options?: R2MultipartOptions): Promise<R2MultipartUpload> {
    if (this.simulateError.operation === 'createMultipartUpload') {
      throw this.simulateError.error ?? new Error('Simulated createMultipartUpload error')
    }

    const uploadId = `upload-${Date.now()}-${Math.random().toString(36).slice(2)}`
    this.uploads.set(uploadId, { key, parts: new Map() })

    const self = this
    return {
      key,
      uploadId,

      async uploadPart(partNumber: number, data: Uint8Array): Promise<R2UploadedPart> {
        const upload = self.uploads.get(uploadId)
        if (!upload) throw new Error('Upload not found')

        const etag = self.generateEtag(data)
        upload.parts.set(partNumber, { data, etag })

        return { partNumber, etag }
      },

      async complete(parts: R2UploadedPart[]): Promise<R2Object> {
        const upload = self.uploads.get(uploadId)
        if (!upload) throw new Error('Upload not found')

        // Sort parts and combine
        const sortedParts = parts.sort((a, b) => a.partNumber - b.partNumber)
        const chunks: Uint8Array[] = []
        for (const part of sortedParts) {
          const storedPart = upload.parts.get(part.partNumber)
          if (storedPart) {
            chunks.push(storedPart.data)
          }
        }

        const totalLength = chunks.reduce((acc, c) => acc + c.length, 0)
        const combined = new Uint8Array(totalLength)
        let offset = 0
        for (const chunk of chunks) {
          combined.set(chunk, offset)
          offset += chunk.length
        }

        // Store the combined result
        const etag = self.generateEtag(combined)
        const metadata: Partial<R2Object> = {
          etag,
          httpEtag: `"${etag}"`,
          size: combined.length,
          uploaded: new Date(),
        }
        self.store.set(key, { data: combined, metadata })
        self.uploads.delete(uploadId)

        return self.createR2Object(key, combined.length, metadata)
      },

      async abort(): Promise<void> {
        self.uploads.delete(uploadId)
      },
    }
  }

  resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload {
    throw new Error('resumeMultipartUpload not implemented in mock')
  }

  // Helper methods

  private createR2Object(key: string, size: number, metadata: Partial<R2Object>): R2Object {
    const etag = metadata.etag ?? this.generateEtag(new Uint8Array(0))
    return {
      key,
      version: 'mock-version',
      size,
      etag,
      httpEtag: metadata.httpEtag ?? `"${etag}"`,
      uploaded: metadata.uploaded ?? new Date(),
      storageClass: 'Standard',
      checksums: {},
      httpMetadata: metadata.httpMetadata,
      customMetadata: metadata.customMetadata,
      writeHttpMetadata: () => {},
    }
  }

  private createR2ObjectBody(key: string, data: Uint8Array, metadata: Partial<R2Object>): R2ObjectBody {
    const r2Object = this.createR2Object(key, data.length, metadata)

    return {
      ...r2Object,
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(data)
          controller.close()
        },
      }),
      bodyUsed: false,
      arrayBuffer: async () => data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength),
      text: async () => new TextDecoder().decode(data),
      json: async () => JSON.parse(new TextDecoder().decode(data)),
      blob: async () => new Blob([data]),
    }
  }

  /** Clear all stored data (for test cleanup) */
  clear(): void {
    this.store.clear()
    this.uploads.clear()
    this.simulateError = {}
  }

  /** Get number of stored objects (for test assertions) */
  size(): number {
    return this.store.size
  }
}

// =============================================================================
// Test Fixtures
// =============================================================================

function createTestViewDefinition(name: string, source = 'users'): ViewDefinition {
  return {
    name: viewName(name),
    source,
    query: { filter: { status: 'active' } },
    options: { refreshMode: 'manual' },
  }
}

function createScheduledViewDefinition(name: string, source = 'orders'): ViewDefinition {
  return {
    name: viewName(name),
    source,
    query: { filter: { status: 'completed' } },
    options: {
      refreshMode: 'scheduled',
      schedule: { cron: '0 * * * *' },
    },
  }
}

function createStreamingViewDefinition(name: string, source = 'events'): ViewDefinition {
  return {
    name: viewName(name),
    source,
    query: {},
    options: {
      refreshMode: 'streaming',
      maxStalenessMs: 5000,
    },
  }
}

// =============================================================================
// Test Suite: R2Backend with MVStorageManager
// =============================================================================

describe('MVStorageManager with R2Backend', () => {
  let mockBucket: MockR2Bucket
  let r2Backend: R2Backend
  let storage: MVStorageManager

  beforeEach(() => {
    mockBucket = new MockR2Bucket()
    r2Backend = new R2Backend(mockBucket)
    storage = new MVStorageManager(r2Backend)
  })

  afterEach(() => {
    mockBucket.clear()
  })

  // ===========================================================================
  // Basic CRUD Operations
  // ===========================================================================

  describe('basic CRUD operations', () => {
    it('should create and retrieve a materialized view', async () => {
      const definition = createTestViewDefinition('r2-test-view')

      const metadata = await storage.createView(definition)

      expect(metadata.definition.name).toBe('r2-test-view')
      expect(metadata.state).toBe('pending')
      expect(metadata.version).toBe(1)

      // Verify we can retrieve it
      const retrieved = await storage.getViewMetadata('r2-test-view')
      expect(retrieved.definition.name).toBe('r2-test-view')
    })

    it('should list all views', async () => {
      await storage.createView(createTestViewDefinition('view-1'))
      await storage.createView(createTestViewDefinition('view-2', 'orders'))
      await storage.createView(createTestViewDefinition('view-3', 'products'))

      const views = await storage.listViews()

      expect(views).toHaveLength(3)
      expect(views.map(v => v.name)).toContain('view-1')
      expect(views.map(v => v.name)).toContain('view-2')
      expect(views.map(v => v.name)).toContain('view-3')
    })

    it('should delete a view', async () => {
      await storage.createView(createTestViewDefinition('delete-me'))
      expect(await storage.viewExists('delete-me')).toBe(true)

      const deleted = await storage.deleteView('delete-me')

      expect(deleted).toBe(true)
      expect(await storage.viewExists('delete-me')).toBe(false)
    })

    it('should update view state', async () => {
      await storage.createView(createTestViewDefinition('state-test'))

      await storage.updateViewState('state-test', 'building')

      const metadata = await storage.getViewMetadata('state-test')
      expect(metadata.state).toBe('building')
    })

    it('should throw MVAlreadyExistsError for duplicate view', async () => {
      await storage.createView(createTestViewDefinition('duplicate'))

      await expect(
        storage.createView(createTestViewDefinition('duplicate'))
      ).rejects.toThrow(MVAlreadyExistsError)
    })

    it('should throw MVNotFoundError for non-existent view', async () => {
      await expect(
        storage.getViewMetadata('does-not-exist')
      ).rejects.toThrow(MVNotFoundError)
    })
  })

  // ===========================================================================
  // View Data Operations
  // ===========================================================================

  describe('view data operations', () => {
    beforeEach(async () => {
      await storage.createView(createTestViewDefinition('data-view'))
    })

    it('should write and read view data', async () => {
      const data = new Uint8Array([0x50, 0x41, 0x52, 0x31, 0x00, 0x01, 0x02, 0x03])

      await storage.writeViewData('data-view', data)

      const result = await storage.readViewData('data-view')
      expect(result).toEqual(data)
    })

    it('should read partial data ranges', async () => {
      const data = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      await storage.writeViewData('data-view', data)

      const result = await storage.readViewDataRange('data-view', 2, 6)

      expect(result).toEqual(new Uint8Array([2, 3, 4, 5]))
    })

    it('should check if view data exists', async () => {
      expect(await storage.viewDataExists('data-view')).toBe(false)

      await storage.writeViewData('data-view', new Uint8Array([1, 2, 3]))

      expect(await storage.viewDataExists('data-view')).toBe(true)
    })

    it('should get view data stats', async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      await storage.writeViewData('data-view', data)

      const stat = await storage.getViewDataStat('data-view')

      expect(stat).not.toBeNull()
      expect(stat?.size).toBe(5)
      expect(stat?.mtime).toBeInstanceOf(Date)
    })

    it('should write and list data shards', async () => {
      await storage.writeViewDataShard('data-view', 0, new Uint8Array([1]))
      await storage.writeViewDataShard('data-view', 1, new Uint8Array([2]))
      await storage.writeViewDataShard('data-view', 2, new Uint8Array([3]))

      const files = await storage.listViewDataFiles('data-view')

      expect(files).toHaveLength(3)
    })

    it('should delete all view data files', async () => {
      await storage.writeViewData('data-view', new Uint8Array([1]))
      await storage.writeViewDataShard('data-view', 1, new Uint8Array([2]))

      const count = await storage.deleteViewData('data-view')

      expect(count).toBe(2)
      expect(await storage.viewDataExists('data-view')).toBe(false)
    })
  })

  // ===========================================================================
  // View Stats Operations
  // ===========================================================================

  describe('view stats operations', () => {
    beforeEach(async () => {
      await storage.createView(createTestViewDefinition('stats-view'))
    })

    it('should record successful refresh', async () => {
      await storage.recordRefresh('stats-view', true, 150)

      const stats = await storage.getViewStats('stats-view')
      expect(stats.totalRefreshes).toBe(1)
      expect(stats.successfulRefreshes).toBe(1)
      expect(stats.failedRefreshes).toBe(0)
      expect(stats.avgRefreshDurationMs).toBe(150)
    })

    it('should record failed refresh', async () => {
      await storage.recordRefresh('stats-view', false, 50)

      const stats = await storage.getViewStats('stats-view')
      expect(stats.totalRefreshes).toBe(1)
      expect(stats.successfulRefreshes).toBe(0)
      expect(stats.failedRefreshes).toBe(1)
    })

    it('should record queries', async () => {
      await storage.recordQuery('stats-view', true)
      await storage.recordQuery('stats-view', false)
      await storage.recordQuery('stats-view', true)

      const stats = await storage.getViewStats('stats-view')
      expect(stats.queryCount).toBe(3)
    })

    it('should calculate running average for refresh duration', async () => {
      await storage.recordRefresh('stats-view', true, 100)
      await storage.recordRefresh('stats-view', true, 200)
      await storage.recordRefresh('stats-view', true, 300)

      const stats = await storage.getViewStats('stats-view')
      expect(stats.avgRefreshDurationMs).toBe(200)
    })
  })

  // ===========================================================================
  // Error Handling
  // ===========================================================================

  describe('error handling', () => {
    it('should handle R2 read errors gracefully', async () => {
      await storage.createView(createTestViewDefinition('error-view'))
      await storage.writeViewData('error-view', new Uint8Array([1, 2, 3]))

      // Simulate an R2 error on get
      mockBucket.simulateError = { operation: 'get', error: new Error('Network error') }

      await expect(storage.readViewData('error-view')).rejects.toThrow()
    })

    it('should handle R2 write errors gracefully', async () => {
      await storage.createView(createTestViewDefinition('write-error-view'))

      // Simulate an R2 error on put
      mockBucket.simulateError = { operation: 'put', error: new Error('Storage full') }

      await expect(
        storage.writeViewData('write-error-view', new Uint8Array([1, 2, 3]))
      ).rejects.toThrow()
    })

    it('should handle R2 list errors gracefully', async () => {
      await storage.createView(createTestViewDefinition('list-error-view'))
      await storage.writeViewData('list-error-view', new Uint8Array([1]))

      // Simulate an R2 error on list
      mockBucket.simulateError = { operation: 'list', error: new Error('Permission denied') }

      await expect(storage.listViewDataFiles('list-error-view')).rejects.toThrow()
    })

    it('should handle R2NotFoundError correctly', async () => {
      const backend = new R2Backend(mockBucket)

      await expect(backend.read('nonexistent-file.parquet')).rejects.toThrow(R2NotFoundError)
    })
  })

  // ===========================================================================
  // Manifest Operations
  // ===========================================================================

  describe('manifest operations', () => {
    it('should persist manifest across storage manager instances', async () => {
      // Create views with first manager
      await storage.createView(createTestViewDefinition('persist-1'))
      await storage.createView(createTestViewDefinition('persist-2'))

      // Create new manager with same backend
      const storage2 = new MVStorageManager(r2Backend)
      const views = await storage2.listViews()

      expect(views).toHaveLength(2)
      expect(views.map(v => v.name)).toContain('persist-1')
      expect(views.map(v => v.name)).toContain('persist-2')
    })

    it('should invalidate manifest cache correctly', async () => {
      await storage.createView(createTestViewDefinition('cache-test'))

      // Access manifest to cache it
      await storage.loadManifest()

      // Invalidate cache
      storage.invalidateManifestCache()

      // Reload should work
      const manifest = await storage.loadManifest()
      expect(manifest.views).toHaveLength(1)
    })
  })

  // ===========================================================================
  // View Lifecycle
  // ===========================================================================

  describe('view lifecycle', () => {
    it('should handle complete view lifecycle', async () => {
      // Create view
      const definition = createTestViewDefinition('lifecycle-view')
      await storage.createView(definition)
      expect(await storage.viewExists('lifecycle-view')).toBe(true)

      // Update to building
      await storage.updateViewState('lifecycle-view', 'building')

      // Write data
      const data = new Uint8Array([0x50, 0x41, 0x52, 0x31]) // PAR1 magic bytes
      await storage.writeViewData('lifecycle-view', data)

      // Record refresh
      await storage.recordRefresh('lifecycle-view', true, 200)

      // Update to ready
      await storage.updateViewState('lifecycle-view', 'ready')

      // Record queries
      await storage.recordQuery('lifecycle-view', true)
      await storage.recordQuery('lifecycle-view', false)

      // Verify final state
      const metadata = await storage.getViewMetadata('lifecycle-view')
      expect(metadata.state).toBe('ready')
      expect(metadata.lastRefreshedAt).toBeInstanceOf(Date)

      const stats = await storage.getViewStats('lifecycle-view')
      expect(stats.totalRefreshes).toBe(1)
      expect(stats.queryCount).toBe(2)

      // Delete view
      const deleted = await storage.deleteView('lifecycle-view')
      expect(deleted).toBe(true)
      expect(await storage.viewExists('lifecycle-view')).toBe(false)
    })
  })
})

// =============================================================================
// Test Suite: R2Backend with Prefix
// =============================================================================

describe('MVStorageManager with R2Backend prefix', () => {
  let mockBucket: MockR2Bucket
  let r2Backend: R2Backend
  let storage: MVStorageManager

  beforeEach(() => {
    mockBucket = new MockR2Bucket()
    // Use prefix to simulate multi-tenant or namespaced storage
    r2Backend = new R2Backend(mockBucket, { prefix: 'tenant-123/' })
    storage = new MVStorageManager(r2Backend)
  })

  afterEach(() => {
    mockBucket.clear()
  })

  it('should store views with prefix', async () => {
    await storage.createView(createTestViewDefinition('prefixed-view'))

    // Verify the manifest is stored with prefix
    const exists = await r2Backend.exists(MVStoragePaths.manifest)
    expect(exists).toBe(true)
  })

  it('should isolate views between different prefixes', async () => {
    // Create view in tenant-123
    await storage.createView(createTestViewDefinition('tenant-view'))

    // Create another backend with different prefix
    const r2Backend2 = new R2Backend(mockBucket, { prefix: 'tenant-456/' })
    const storage2 = new MVStorageManager(r2Backend2)

    // Views should be isolated
    const views1 = await storage.listViews()
    const views2 = await storage2.listViews()

    expect(views1).toHaveLength(1)
    expect(views2).toHaveLength(0)
  })
})

// =============================================================================
// Test Suite: S3-Compatible Backend (using same mock)
// =============================================================================

describe('MVStorageManager with S3-compatible backend', () => {
  // R2 is S3-compatible, so we can use the same MockR2Bucket
  // This tests the abstraction works for any S3-compatible backend

  let mockBucket: MockR2Bucket
  let s3Backend: R2Backend
  let storage: MVStorageManager

  beforeEach(() => {
    mockBucket = new MockR2Bucket()
    // R2Backend works with any S3-compatible bucket
    s3Backend = new R2Backend(mockBucket, { prefix: 's3-test/' })
    storage = new MVStorageManager(s3Backend)
  })

  afterEach(() => {
    mockBucket.clear()
  })

  it('should work with S3-compatible operations', async () => {
    const definition = createTestViewDefinition('s3-view')

    await storage.createView(definition)
    const metadata = await storage.getViewMetadata('s3-view')

    expect(metadata.definition.name).toBe('s3-view')
    expect(metadata.state).toBe('pending')
  })

  it('should handle large view data', async () => {
    await storage.createView(createTestViewDefinition('large-view'))

    // Create large data (100KB)
    const largeData = new Uint8Array(100 * 1024)
    for (let i = 0; i < largeData.length; i++) {
      largeData[i] = i % 256
    }

    await storage.writeViewData('large-view', largeData)

    const readData = await storage.readViewData('large-view')
    expect(readData).toEqual(largeData)
  })

  it('should handle sequential view operations', async () => {
    // MVStorageManager manifest operations are not atomic,
    // so concurrent creates can cause race conditions.
    // Test sequential operations which is the intended usage pattern.
    await storage.createView(createTestViewDefinition('sequential-1'))
    await storage.createView(createTestViewDefinition('sequential-2'))
    await storage.createView(createTestViewDefinition('sequential-3'))

    const views = await storage.listViews()
    expect(views).toHaveLength(3)
  })

  it('should handle concurrent read operations', async () => {
    // Create views first (sequentially)
    await storage.createView(createTestViewDefinition('read-concurrent-1'))
    await storage.createView(createTestViewDefinition('read-concurrent-2'))

    // Write data to views
    await storage.writeViewData('read-concurrent-1', new Uint8Array([1, 2, 3]))
    await storage.writeViewData('read-concurrent-2', new Uint8Array([4, 5, 6]))

    // Concurrent reads should work fine
    const [data1, data2, metadata1, metadata2] = await Promise.all([
      storage.readViewData('read-concurrent-1'),
      storage.readViewData('read-concurrent-2'),
      storage.getViewMetadata('read-concurrent-1'),
      storage.getViewMetadata('read-concurrent-2'),
    ])

    expect(data1).toEqual(new Uint8Array([1, 2, 3]))
    expect(data2).toEqual(new Uint8Array([4, 5, 6]))
    expect(metadata1.definition.name).toBe('read-concurrent-1')
    expect(metadata2.definition.name).toBe('read-concurrent-2')
  })
})

// =============================================================================
// Test Suite: Comparison with MemoryBackend
// =============================================================================

describe('R2Backend vs MemoryBackend MV behavior', () => {
  let mockBucket: MockR2Bucket
  let r2Backend: R2Backend
  let memoryBackend: MemoryBackend
  let r2Storage: MVStorageManager
  let memoryStorage: MVStorageManager

  beforeEach(() => {
    mockBucket = new MockR2Bucket()
    r2Backend = new R2Backend(mockBucket)
    memoryBackend = new MemoryBackend()
    r2Storage = new MVStorageManager(r2Backend)
    memoryStorage = new MVStorageManager(memoryBackend)
  })

  afterEach(() => {
    mockBucket.clear()
  })

  it('should have consistent behavior between backends', async () => {
    const definition = createTestViewDefinition('consistency-view')

    // Create view on both backends
    const r2Metadata = await r2Storage.createView(definition)
    const memoryMetadata = await memoryStorage.createView(definition)

    expect(r2Metadata.definition.name).toBe(memoryMetadata.definition.name)
    expect(r2Metadata.state).toBe(memoryMetadata.state)

    // Write same data to both
    const data = new Uint8Array([1, 2, 3, 4, 5])
    await r2Storage.writeViewData('consistency-view', data)
    await memoryStorage.writeViewData('consistency-view', data)

    // Read and compare
    const r2Data = await r2Storage.readViewData('consistency-view')
    const memoryData = await memoryStorage.readViewData('consistency-view')

    expect(r2Data).toEqual(memoryData)
  })

  it('should have consistent error behavior', async () => {
    // Both should throw MVNotFoundError for non-existent views
    await expect(r2Storage.getViewMetadata('nope')).rejects.toThrow(MVNotFoundError)
    await expect(memoryStorage.getViewMetadata('nope')).rejects.toThrow(MVNotFoundError)

    // Both should throw MVAlreadyExistsError for duplicate views
    await r2Storage.createView(createTestViewDefinition('dup'))
    await memoryStorage.createView(createTestViewDefinition('dup'))

    await expect(r2Storage.createView(createTestViewDefinition('dup'))).rejects.toThrow(MVAlreadyExistsError)
    await expect(memoryStorage.createView(createTestViewDefinition('dup'))).rejects.toThrow(MVAlreadyExistsError)
  })
})

// =============================================================================
// Test Suite: R2Backend Multipart Upload for Large MVs
// =============================================================================

describe('R2Backend multipart upload for large MVs', () => {
  let mockBucket: MockR2Bucket
  let r2Backend: R2Backend
  let storage: MVStorageManager

  beforeEach(() => {
    mockBucket = new MockR2Bucket()
    r2Backend = new R2Backend(mockBucket)
    storage = new MVStorageManager(r2Backend)
  })

  afterEach(() => {
    mockBucket.clear()
  })

  it('should handle multipart upload for very large view data', async () => {
    await storage.createView(createTestViewDefinition('multipart-view'))

    // Create data larger than typical part size (simulating large MV)
    // For testing, we use smaller chunks but the API is the same
    const largeData = new Uint8Array(1024 * 1024) // 1MB
    for (let i = 0; i < largeData.length; i++) {
      largeData[i] = i % 256
    }

    // Use writeStreaming which handles multipart internally
    const result = await r2Backend.writeStreaming(
      storage.getDataFilePath('multipart-view'),
      largeData
    )

    expect(result.size).toBe(largeData.length)
    expect(result.etag).toBeDefined()

    // Verify we can read it back
    const readData = await r2Backend.read(storage.getDataFilePath('multipart-view'))
    expect(readData).toEqual(largeData)
  })
})

// =============================================================================
// Test Suite: Persistence and Recovery
// =============================================================================

describe('R2Backend MV persistence and recovery', () => {
  let mockBucket: MockR2Bucket
  let r2Backend: R2Backend
  let storage: MVStorageManager

  beforeEach(() => {
    mockBucket = new MockR2Bucket()
    r2Backend = new R2Backend(mockBucket)
    storage = new MVStorageManager(r2Backend)
  })

  afterEach(() => {
    mockBucket.clear()
  })

  describe('persistence across restarts', () => {
    it('should persist view definition and recover after manager restart', async () => {
      // Create a view with all options
      const definition = createScheduledViewDefinition('persistent-view')
      await storage.createView(definition)
      await storage.updateViewState('persistent-view', 'ready')
      await storage.recordRefresh('persistent-view', true, 250)

      // Simulate restart by creating new storage manager
      const recoveredStorage = new MVStorageManager(r2Backend)

      // Verify view was recovered
      const metadata = await recoveredStorage.getViewMetadata('persistent-view')
      expect(metadata.definition.name).toBe('persistent-view')
      expect(metadata.definition.source).toBe('orders')
      expect(metadata.definition.options.refreshMode).toBe('scheduled')
      expect(metadata.state).toBe('ready')
      expect(metadata.lastRefreshedAt).toBeInstanceOf(Date)
    })

    it('should persist view data and recover after manager restart', async () => {
      await storage.createView(createTestViewDefinition('data-persist-view'))

      // Write substantial data
      const testData = new Uint8Array(10000)
      for (let i = 0; i < testData.length; i++) {
        testData[i] = (i * 7) % 256 // Deterministic pattern
      }
      await storage.writeViewData('data-persist-view', testData)

      // Simulate restart
      const recoveredStorage = new MVStorageManager(r2Backend)

      // Verify data integrity
      const recoveredData = await recoveredStorage.readViewData('data-persist-view')
      expect(recoveredData.length).toBe(testData.length)
      expect(recoveredData).toEqual(testData)
    })

    it('should persist view stats and recover after manager restart', async () => {
      await storage.createView(createTestViewDefinition('stats-persist-view'))

      // Record multiple refreshes and queries
      await storage.recordRefresh('stats-persist-view', true, 100)
      await storage.recordRefresh('stats-persist-view', true, 200)
      await storage.recordRefresh('stats-persist-view', false, 50)
      await storage.recordQuery('stats-persist-view', true)
      await storage.recordQuery('stats-persist-view', false)
      await storage.recordQuery('stats-persist-view', true)

      // Simulate restart
      const recoveredStorage = new MVStorageManager(r2Backend)

      // Verify stats were recovered
      const stats = await recoveredStorage.getViewStats('stats-persist-view')
      expect(stats.totalRefreshes).toBe(3)
      expect(stats.successfulRefreshes).toBe(2)
      expect(stats.failedRefreshes).toBe(1)
      expect(stats.queryCount).toBe(3)
    })

    it('should persist multiple views and recover all', async () => {
      // Create multiple views of different types
      await storage.createView(createTestViewDefinition('manual-persist'))
      await storage.createView(createScheduledViewDefinition('scheduled-persist'))
      await storage.createView(createStreamingViewDefinition('streaming-persist'))

      // Update their states
      await storage.updateViewState('manual-persist', 'ready')
      await storage.updateViewState('scheduled-persist', 'building')
      await storage.updateViewState('streaming-persist', 'stale')

      // Simulate restart
      const recoveredStorage = new MVStorageManager(r2Backend)

      // Verify all views recovered
      const views = await recoveredStorage.listViews()
      expect(views).toHaveLength(3)

      const manual = await recoveredStorage.getViewMetadata('manual-persist')
      const scheduled = await recoveredStorage.getViewMetadata('scheduled-persist')
      const streaming = await recoveredStorage.getViewMetadata('streaming-persist')

      expect(manual.state).toBe('ready')
      expect(scheduled.state).toBe('building')
      expect(streaming.state).toBe('stale')
    })

    it('should persist sharded view data and recover all shards', async () => {
      await storage.createView(createTestViewDefinition('sharded-persist-view'))

      // Write multiple shards
      const shardCount = 5
      const shardData: Uint8Array[] = []
      for (let i = 0; i < shardCount; i++) {
        const data = new Uint8Array(1000)
        for (let j = 0; j < data.length; j++) {
          data[j] = (i * 37 + j) % 256
        }
        shardData.push(data)
        await storage.writeViewDataShard('sharded-persist-view', i, data)
      }

      // Simulate restart
      const recoveredStorage = new MVStorageManager(r2Backend)

      // Verify all shards recovered
      const files = await recoveredStorage.listViewDataFiles('sharded-persist-view')
      expect(files).toHaveLength(shardCount)

      // Verify data integrity for each shard
      for (let i = 0; i < shardCount; i++) {
        const shardPath = recoveredStorage.getDataShardPath('sharded-persist-view', i)
        const recoveredData = await r2Backend.read(shardPath)
        expect(recoveredData).toEqual(shardData[i])
      }
    })
  })

  describe('recovery from partial failures', () => {
    it('should recover manifest after view creation failure', async () => {
      // Create first view successfully
      await storage.createView(createTestViewDefinition('success-view'))

      // Simulate error during second view creation (after manifest update but before data write)
      mockBucket.simulateError = { operation: 'put', error: new Error('Simulated write failure') }

      // Clear error for manifest writes to work
      const originalPut = mockBucket.simulateError
      try {
        await expect(
          storage.createView(createTestViewDefinition('fail-view'))
        ).rejects.toThrow()
      } finally {
        mockBucket.simulateError = {}
      }

      // Recover with new storage manager
      const recoveredStorage = new MVStorageManager(r2Backend)
      const views = await recoveredStorage.listViews()

      // First view should be intact
      expect(views.some(v => v.name === 'success-view')).toBe(true)
    })

    it('should handle recovery when manifest exists but view metadata is missing', async () => {
      await storage.createView(createTestViewDefinition('orphan-test'))
      await storage.createView(createTestViewDefinition('valid-test'))

      // Simulate corruption: delete one view's metadata but keep it in manifest
      const metadataPath = MVStoragePaths.viewMetadata('orphan-test')
      await r2Backend.delete(metadataPath)

      // Recover with new storage manager
      const recoveredStorage = new MVStorageManager(r2Backend)

      // getAllViewMetadata should skip orphaned views
      const allMetadata = await recoveredStorage.getAllViewMetadata()
      expect(allMetadata).toHaveLength(1)
      expect(allMetadata[0].definition.name).toBe('valid-test')
    })

    it('should recover view state after error state was persisted', async () => {
      await storage.createView(createTestViewDefinition('error-recover-view'))
      await storage.updateViewState('error-recover-view', 'error', 'Previous failure message')

      // Simulate restart
      const recoveredStorage = new MVStorageManager(r2Backend)

      // Verify error state was persisted
      const metadata = await recoveredStorage.getViewMetadata('error-recover-view')
      expect(metadata.state).toBe('error')
      expect(metadata.error).toBe('Previous failure message')

      // Can update to ready after fixing issue
      await recoveredStorage.updateViewState('error-recover-view', 'ready')
      const updated = await recoveredStorage.getViewMetadata('error-recover-view')
      expect(updated.state).toBe('ready')
      expect(updated.error).toBeUndefined()
    })
  })

  describe('data integrity verification', () => {
    it('should maintain data integrity across multiple write/read cycles', async () => {
      await storage.createView(createTestViewDefinition('integrity-view'))

      const originalData = new Uint8Array(5000)
      for (let i = 0; i < originalData.length; i++) {
        originalData[i] = Math.floor(Math.random() * 256)
      }

      // Multiple write/read cycles
      for (let cycle = 0; cycle < 3; cycle++) {
        await storage.writeViewData('integrity-view', originalData)
        const readData = await storage.readViewData('integrity-view')
        expect(readData).toEqual(originalData)
      }
    })

    it('should maintain range read consistency with full read', async () => {
      await storage.createView(createTestViewDefinition('range-integrity-view'))

      const data = new Uint8Array(1000)
      for (let i = 0; i < data.length; i++) {
        data[i] = i % 256
      }
      await storage.writeViewData('range-integrity-view', data)

      // Full read
      const fullData = await storage.readViewData('range-integrity-view')

      // Multiple range reads should match full read
      const range1 = await storage.readViewDataRange('range-integrity-view', 0, 100)
      const range2 = await storage.readViewDataRange('range-integrity-view', 100, 500)
      const range3 = await storage.readViewDataRange('range-integrity-view', 500, 1000)

      expect(range1).toEqual(fullData.slice(0, 100))
      expect(range2).toEqual(fullData.slice(100, 500))
      expect(range3).toEqual(fullData.slice(500, 1000))
    })

    it('should verify ETag changes on data update', async () => {
      await storage.createView(createTestViewDefinition('etag-view'))

      // First write
      const result1 = await storage.writeViewData('etag-view', new Uint8Array([1, 2, 3]))
      const etag1 = result1.etag

      // Second write with different data
      const result2 = await storage.writeViewData('etag-view', new Uint8Array([4, 5, 6, 7]))
      const etag2 = result2.etag

      // ETags should be different
      expect(etag1).not.toBe(etag2)

      // Verify stat returns latest etag
      const stat = await storage.getViewDataStat('etag-view')
      expect(stat).not.toBeNull()
    })
  })
})

// =============================================================================
// Test Suite: Range Read Edge Cases
// =============================================================================

describe('R2Backend MV range read edge cases', () => {
  let mockBucket: MockR2Bucket
  let r2Backend: R2Backend
  let storage: MVStorageManager

  beforeEach(() => {
    mockBucket = new MockR2Bucket()
    r2Backend = new R2Backend(mockBucket)
    storage = new MVStorageManager(r2Backend)
  })

  afterEach(() => {
    mockBucket.clear()
  })

  it('should handle zero-length range read', async () => {
    await storage.createView(createTestViewDefinition('zero-range-view'))
    await storage.writeViewData('zero-range-view', new Uint8Array([1, 2, 3, 4, 5]))

    const result = await storage.readViewDataRange('zero-range-view', 2, 2)
    expect(result.length).toBe(0)
  })

  it('should handle range at start of file', async () => {
    await storage.createView(createTestViewDefinition('start-range-view'))
    const data = new Uint8Array([10, 20, 30, 40, 50])
    await storage.writeViewData('start-range-view', data)

    const result = await storage.readViewDataRange('start-range-view', 0, 3)
    expect(result).toEqual(new Uint8Array([10, 20, 30]))
  })

  it('should handle range at end of file', async () => {
    await storage.createView(createTestViewDefinition('end-range-view'))
    const data = new Uint8Array([10, 20, 30, 40, 50])
    await storage.writeViewData('end-range-view', data)

    const result = await storage.readViewDataRange('end-range-view', 3, 5)
    expect(result).toEqual(new Uint8Array([40, 50]))
  })

  it('should handle single byte range', async () => {
    await storage.createView(createTestViewDefinition('single-byte-view'))
    const data = new Uint8Array([100, 101, 102, 103, 104])
    await storage.writeViewData('single-byte-view', data)

    const result = await storage.readViewDataRange('single-byte-view', 2, 3)
    expect(result).toEqual(new Uint8Array([102]))
  })

  it('should handle full file as range', async () => {
    await storage.createView(createTestViewDefinition('full-range-view'))
    const data = new Uint8Array([1, 2, 3, 4, 5])
    await storage.writeViewData('full-range-view', data)

    const result = await storage.readViewDataRange('full-range-view', 0, 5)
    expect(result).toEqual(data)
  })

  it('should handle large offset range reads', async () => {
    await storage.createView(createTestViewDefinition('large-offset-view'))

    // Create larger data
    const data = new Uint8Array(10000)
    for (let i = 0; i < data.length; i++) {
      data[i] = i % 256
    }
    await storage.writeViewData('large-offset-view', data)

    // Read range near the end
    const result = await storage.readViewDataRange('large-offset-view', 9000, 9500)
    expect(result.length).toBe(500)
    expect(result).toEqual(data.slice(9000, 9500))
  })

  it('should handle multiple sequential range reads', async () => {
    await storage.createView(createTestViewDefinition('sequential-range-view'))

    const data = new Uint8Array(100)
    for (let i = 0; i < data.length; i++) {
      data[i] = i
    }
    await storage.writeViewData('sequential-range-view', data)

    // Simulate Parquet-style sequential reads (footer, row groups, etc.)
    const footer = await storage.readViewDataRange('sequential-range-view', 92, 100)
    const middle = await storage.readViewDataRange('sequential-range-view', 50, 70)
    const header = await storage.readViewDataRange('sequential-range-view', 0, 10)

    expect(footer).toEqual(data.slice(92, 100))
    expect(middle).toEqual(data.slice(50, 70))
    expect(header).toEqual(data.slice(0, 10))
  })
})

// =============================================================================
// Test Suite: Concurrent Write Scenarios
// =============================================================================

describe('R2Backend MV concurrent write scenarios', () => {
  let mockBucket: MockR2Bucket
  let r2Backend: R2Backend
  let storage: MVStorageManager

  beforeEach(() => {
    mockBucket = new MockR2Bucket()
    r2Backend = new R2Backend(mockBucket)
    storage = new MVStorageManager(r2Backend)
  })

  afterEach(() => {
    mockBucket.clear()
  })

  it('should handle concurrent writes to different views', async () => {
    // Create multiple views first (sequentially to avoid manifest races)
    await storage.createView(createTestViewDefinition('concurrent-1'))
    await storage.createView(createTestViewDefinition('concurrent-2'))
    await storage.createView(createTestViewDefinition('concurrent-3'))

    // Write to all views concurrently
    const writes = await Promise.all([
      storage.writeViewData('concurrent-1', new Uint8Array([1, 1, 1])),
      storage.writeViewData('concurrent-2', new Uint8Array([2, 2, 2])),
      storage.writeViewData('concurrent-3', new Uint8Array([3, 3, 3])),
    ])

    expect(writes).toHaveLength(3)
    expect(writes.every(w => w.etag)).toBe(true)

    // Verify all writes succeeded
    const [data1, data2, data3] = await Promise.all([
      storage.readViewData('concurrent-1'),
      storage.readViewData('concurrent-2'),
      storage.readViewData('concurrent-3'),
    ])

    expect(data1).toEqual(new Uint8Array([1, 1, 1]))
    expect(data2).toEqual(new Uint8Array([2, 2, 2]))
    expect(data3).toEqual(new Uint8Array([3, 3, 3]))
  })

  it('should handle concurrent stat updates to different views', async () => {
    await storage.createView(createTestViewDefinition('stat-concurrent-1'))
    await storage.createView(createTestViewDefinition('stat-concurrent-2'))

    // Note: recordRefresh with success=true also updates metadata and manifest,
    // which can cause race conditions when run concurrently due to read-modify-write patterns.
    // For reliable concurrent stats, use sequential operations per view or separate storage instances.
    // This test validates concurrent operations on different views work correctly.

    // Run refreshes sequentially (they update shared manifest)
    await storage.recordRefresh('stat-concurrent-1', true, 100)
    await storage.recordRefresh('stat-concurrent-2', true, 200)

    // Concurrent queries to different views (no manifest interaction)
    await Promise.all([
      storage.recordQuery('stat-concurrent-1', true),
      storage.recordQuery('stat-concurrent-2', false),
    ])

    const stats1 = await storage.getViewStats('stat-concurrent-1')
    const stats2 = await storage.getViewStats('stat-concurrent-2')

    expect(stats1.totalRefreshes).toBe(1)
    expect(stats1.queryCount).toBe(1)
    expect(stats2.totalRefreshes).toBe(1)
    expect(stats2.queryCount).toBe(1)
  })

  it('should handle rapid sequential writes to same view', async () => {
    await storage.createView(createTestViewDefinition('rapid-write-view'))

    // Rapid sequential writes (last write wins)
    for (let i = 0; i < 10; i++) {
      await storage.writeViewData('rapid-write-view', new Uint8Array([i]))
    }

    const finalData = await storage.readViewData('rapid-write-view')
    expect(finalData).toEqual(new Uint8Array([9]))
  })

  it('should handle concurrent metadata and data operations', async () => {
    await storage.createView(createTestViewDefinition('mixed-ops-view'))

    // Write initial data
    await storage.writeViewData('mixed-ops-view', new Uint8Array([0]))

    // Concurrent mixed operations
    await Promise.all([
      storage.updateViewState('mixed-ops-view', 'building'),
      storage.writeViewData('mixed-ops-view', new Uint8Array([1, 2])),
      storage.recordQuery('mixed-ops-view', true),
    ])

    // Verify state
    const metadata = await storage.getViewMetadata('mixed-ops-view')
    expect(metadata.state).toBe('building')

    const data = await storage.readViewData('mixed-ops-view')
    expect(data).toEqual(new Uint8Array([1, 2]))

    const stats = await storage.getViewStats('mixed-ops-view')
    expect(stats.queryCount).toBe(1)
  })

  it('should handle concurrent shard writes', async () => {
    await storage.createView(createTestViewDefinition('shard-concurrent-view'))

    // Concurrent shard writes
    const shardWrites = await Promise.all([
      storage.writeViewDataShard('shard-concurrent-view', 0, new Uint8Array([0, 0])),
      storage.writeViewDataShard('shard-concurrent-view', 1, new Uint8Array([1, 1])),
      storage.writeViewDataShard('shard-concurrent-view', 2, new Uint8Array([2, 2])),
      storage.writeViewDataShard('shard-concurrent-view', 3, new Uint8Array([3, 3])),
    ])

    expect(shardWrites).toHaveLength(4)

    // Verify all shards
    const files = await storage.listViewDataFiles('shard-concurrent-view')
    expect(files).toHaveLength(4)
  })
})
