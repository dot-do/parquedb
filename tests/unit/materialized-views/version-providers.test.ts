/**
 * Production Source Version Providers Test Suite
 *
 * Tests for NativeSourceVersionProvider, IcebergSourceVersionProvider, and
 * DeltaSourceVersionProvider.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  NativeSourceVersionProvider,
  IcebergSourceVersionProvider,
  DeltaSourceVersionProvider,
  createNativeVersionProvider,
  createIcebergVersionProvider,
  createDeltaVersionProvider,
} from '@/materialized-views/version-providers'
import type { StorageBackend, ListResult, FileStat } from '@/types/storage'
import { NotFoundError } from '@/storage/errors'

// =============================================================================
// Mock Storage Backend
// =============================================================================

function createMockStorage(): StorageBackend & {
  _files: Map<string, Uint8Array>
  _setFile: (path: string, data: unknown) => void
} {
  const files = new Map<string, Uint8Array>()

  const storage: StorageBackend & {
    _files: Map<string, Uint8Array>
    _setFile: (path: string, data: unknown) => void
  } = {
    _files: files,
    _setFile(path: string, data: unknown) {
      const str = typeof data === 'string' ? data : JSON.stringify(data)
      files.set(path, new TextEncoder().encode(str))
    },

    async read(path: string): Promise<Uint8Array> {
      const data = files.get(path)
      if (!data) {
        throw new NotFoundError(path)
      }
      return data
    },

    async write(path: string, data: Uint8Array): Promise<void> {
      files.set(path, data)
    },

    async exists(path: string): Promise<boolean> {
      return files.has(path)
    },

    async delete(path: string): Promise<void> {
      files.delete(path)
    },

    async list(prefix: string): Promise<ListResult> {
      const matching: string[] = []
      for (const key of files.keys()) {
        if (key.startsWith(prefix)) {
          matching.push(key)
        }
      }
      return { files: matching.sort() }
    },

    async stat(path: string): Promise<FileStat | null> {
      const data = files.get(path)
      if (!data) return null
      return {
        size: data.length,
        mtime: new Date(),
      }
    },

    async mkdir(): Promise<void> {},

    async writeConditional(
      path: string,
      data: Uint8Array,
      _expectedEtag: string | null
    ): Promise<void> {
      files.set(path, data)
    },
  }

  return storage
}

// =============================================================================
// NativeSourceVersionProvider Tests
// =============================================================================

describe('NativeSourceVersionProvider', () => {
  let storage: ReturnType<typeof createMockStorage>
  let provider: NativeSourceVersionProvider

  beforeEach(() => {
    storage = createMockStorage()
    provider = createNativeVersionProvider({
      storage,
      dataset: 'test-dataset',
    })
  })

  describe('getCurrentVersion', () => {
    it('returns null when no manifest exists', async () => {
      const version = await provider.getCurrentVersion('orders')
      expect(version).toBeNull()
    })

    it('returns version based on manifest state', async () => {
      storage._setFile('test-dataset/events/_manifest.json', {
        version: 1,
        dataset: 'test-dataset',
        segments: [
          {
            seq: 1,
            path: 'events/seg-0001.parquet',
            minTs: 1000,
            maxTs: 2000,
            count: 100,
            sizeBytes: 1024,
            createdAt: 1704067200000,
          },
          {
            seq: 2,
            path: 'events/seg-0002.parquet',
            minTs: 2001,
            maxTs: 3000,
            count: 150,
            sizeBytes: 2048,
            createdAt: 1704067300000,
          },
        ],
        nextSeq: 3,
        totalEvents: 250,
        updatedAt: 1704067300000,
      })

      const version = await provider.getCurrentVersion('orders')

      expect(version).not.toBeNull()
      expect(version?.versionId).toBe('seq-2')
      expect(version?.timestamp).toBe(1704067300000)
      expect(version?.backend).toBe('native')
      expect(version?.lastEventId).toBe('segment-2')
      expect(version?.lastEventTs).toBe(3000)
    })

    it('handles empty manifest with no segments', async () => {
      storage._setFile('test-dataset/events/_manifest.json', {
        version: 1,
        dataset: 'test-dataset',
        segments: [],
        nextSeq: 1,
        totalEvents: 0,
        updatedAt: 1704067200000,
      })

      const version = await provider.getCurrentVersion('orders')

      expect(version).not.toBeNull()
      expect(version?.versionId).toBe('seq-0')
      expect(version?.lastEventId).toBeUndefined()
    })
  })

  describe('getChangeCount', () => {
    beforeEach(() => {
      storage._setFile('test-dataset/events/_manifest.json', {
        version: 1,
        dataset: 'test-dataset',
        segments: [
          { seq: 1, count: 100, minTs: 1000, maxTs: 2000, path: '', sizeBytes: 0, createdAt: 0 },
          { seq: 2, count: 150, minTs: 2001, maxTs: 3000, path: '', sizeBytes: 0, createdAt: 0 },
          { seq: 3, count: 200, minTs: 3001, maxTs: 4000, path: '', sizeBytes: 0, createdAt: 0 },
          { seq: 4, count: 50, minTs: 4001, maxTs: 5000, path: '', sizeBytes: 0, createdAt: 0 },
        ],
        nextSeq: 5,
        totalEvents: 500,
        updatedAt: 1704067400000,
      })
    })

    it('returns sum of events between versions', async () => {
      const count = await provider.getChangeCount(
        'orders',
        { versionId: 'seq-1', timestamp: 1000, backend: 'native' },
        { versionId: 'seq-4', timestamp: 5000, backend: 'native' }
      )

      // Should include segments 2, 3, 4 (after seq-1, up to seq-4)
      expect(count).toBe(150 + 200 + 50)
    })

    it('returns 0 for same versions', async () => {
      const count = await provider.getChangeCount(
        'orders',
        { versionId: 'seq-2', timestamp: 2000, backend: 'native' },
        { versionId: 'seq-2', timestamp: 2000, backend: 'native' }
      )

      expect(count).toBe(0)
    })

    it('returns 0 for invalid version IDs', async () => {
      const count = await provider.getChangeCount(
        'orders',
        { versionId: 'invalid', timestamp: 1000, backend: 'native' },
        { versionId: 'seq-4', timestamp: 5000, backend: 'native' }
      )

      expect(count).toBe(0)
    })
  })

  describe('isVersionValid', () => {
    it('returns false when manifest does not exist', async () => {
      const valid = await provider.isVersionValid('orders', {
        versionId: 'seq-1',
        timestamp: 1000,
        backend: 'native',
      })

      expect(valid).toBe(false)
    })

    it('returns true for valid sequence within range', async () => {
      storage._setFile('test-dataset/events/_manifest.json', {
        version: 1,
        dataset: 'test-dataset',
        segments: [
          { seq: 1, count: 100, minTs: 1000, maxTs: 2000, path: '', sizeBytes: 0, createdAt: 0 },
          { seq: 2, count: 150, minTs: 2001, maxTs: 3000, path: '', sizeBytes: 0, createdAt: 0 },
        ],
        nextSeq: 3,
        totalEvents: 250,
        updatedAt: 1704067300000,
      })

      const valid = await provider.isVersionValid('orders', {
        versionId: 'seq-1',
        timestamp: 1000,
        backend: 'native',
      })

      expect(valid).toBe(true)
    })

    it('returns false for sequence that was compacted', async () => {
      storage._setFile('test-dataset/events/_manifest.json', {
        version: 1,
        dataset: 'test-dataset',
        segments: [
          // Segments 1-3 were compacted away, only 4+ remain
          { seq: 4, count: 50, minTs: 4001, maxTs: 5000, path: '', sizeBytes: 0, createdAt: 0 },
        ],
        compactedThrough: 3000,
        nextSeq: 5,
        totalEvents: 50,
        updatedAt: 1704067400000,
      })

      // Sequence 1 was compacted away
      const valid = await provider.isVersionValid('orders', {
        versionId: 'seq-1',
        timestamp: 1000,
        backend: 'native',
      })

      expect(valid).toBe(false)
    })
  })
})

// =============================================================================
// IcebergSourceVersionProvider Tests
// =============================================================================

describe('IcebergSourceVersionProvider', () => {
  let storage: ReturnType<typeof createMockStorage>
  let provider: IcebergSourceVersionProvider

  beforeEach(() => {
    storage = createMockStorage()
    provider = createIcebergVersionProvider({
      storage,
      warehouse: 'warehouse',
      database: 'test-db',
    })
  })

  describe('getCurrentVersion', () => {
    it('returns null when table does not exist', async () => {
      const version = await provider.getCurrentVersion('orders')
      expect(version).toBeNull()
    })

    it('returns version based on current snapshot ID', async () => {
      const metadataPath = 'warehouse/test-db/orders/metadata/v1.metadata.json'

      // Set version hint
      storage._setFile(
        'warehouse/test-db/orders/metadata/version-hint.text',
        metadataPath
      )

      // Set metadata
      storage._setFile(metadataPath, {
        'current-snapshot-id': 12345678901234,
        'last-updated-ms': 1704067200000,
        'last-sequence-number': 5,
        snapshots: [
          {
            'snapshot-id': 12345678901234,
            'timestamp-ms': 1704067200000,
            summary: {
              operation: 'append',
              'added-records': '100',
              'total-records': '100',
            },
          },
        ],
      })

      const version = await provider.getCurrentVersion('orders')

      expect(version).not.toBeNull()
      expect(version?.versionId).toBe('12345678901234')
      expect(version?.timestamp).toBe(1704067200000)
      expect(version?.backend).toBe('iceberg')
    })

    it('returns empty version for table with no snapshots', async () => {
      const metadataPath = 'warehouse/test-db/orders/metadata/v1.metadata.json'

      storage._setFile(
        'warehouse/test-db/orders/metadata/version-hint.text',
        metadataPath
      )

      storage._setFile(metadataPath, {
        'current-snapshot-id': null,
        'last-updated-ms': 1704067200000,
        snapshots: [],
      })

      const version = await provider.getCurrentVersion('orders')

      expect(version).not.toBeNull()
      expect(version?.versionId).toBe('empty')
      expect(version?.backend).toBe('iceberg')
    })
  })

  describe('getChangeCount', () => {
    beforeEach(() => {
      const metadataPath = 'warehouse/test-db/orders/metadata/v1.metadata.json'

      storage._setFile(
        'warehouse/test-db/orders/metadata/version-hint.text',
        metadataPath
      )

      storage._setFile(metadataPath, {
        'current-snapshot-id': 4,
        'last-updated-ms': 1704067400000,
        snapshots: [
          {
            'snapshot-id': 1,
            'timestamp-ms': 1704067100000,
            summary: { 'added-records': '100', 'deleted-records': '0' },
          },
          {
            'snapshot-id': 2,
            'timestamp-ms': 1704067200000,
            summary: { 'added-records': '50', 'deleted-records': '10' },
          },
          {
            'snapshot-id': 3,
            'timestamp-ms': 1704067300000,
            summary: { 'added-records': '200', 'deleted-records': '5' },
          },
          {
            'snapshot-id': 4,
            'timestamp-ms': 1704067400000,
            summary: { 'added-records': '30', 'deleted-records': '0' },
          },
        ],
      })
    })

    it('sums changes between snapshot versions', async () => {
      const count = await provider.getChangeCount(
        'orders',
        { versionId: '1', timestamp: 1704067100000, backend: 'iceberg' },
        { versionId: '4', timestamp: 1704067400000, backend: 'iceberg' }
      )

      // Snapshots 2, 3, 4: (50+10) + (200+5) + (30+0) = 295
      expect(count).toBe(295)
    })

    it('returns 0 for same versions', async () => {
      const count = await provider.getChangeCount(
        'orders',
        { versionId: '2', timestamp: 1704067200000, backend: 'iceberg' },
        { versionId: '2', timestamp: 1704067200000, backend: 'iceberg' }
      )

      expect(count).toBe(0)
    })
  })

  describe('isVersionValid', () => {
    it('returns false when table does not exist', async () => {
      const valid = await provider.isVersionValid('orders', {
        versionId: '123',
        timestamp: 1000,
        backend: 'iceberg',
      })

      expect(valid).toBe(false)
    })

    it('returns true for existing snapshot', async () => {
      const metadataPath = 'warehouse/test-db/orders/metadata/v1.metadata.json'

      storage._setFile(
        'warehouse/test-db/orders/metadata/version-hint.text',
        metadataPath
      )

      storage._setFile(metadataPath, {
        'current-snapshot-id': 123,
        snapshots: [
          { 'snapshot-id': 123, 'timestamp-ms': 1704067200000 },
        ],
      })

      const valid = await provider.isVersionValid('orders', {
        versionId: '123',
        timestamp: 1704067200000,
        backend: 'iceberg',
      })

      expect(valid).toBe(true)
    })

    it('returns false for expired snapshot', async () => {
      const metadataPath = 'warehouse/test-db/orders/metadata/v1.metadata.json'

      storage._setFile(
        'warehouse/test-db/orders/metadata/version-hint.text',
        metadataPath
      )

      storage._setFile(metadataPath, {
        'current-snapshot-id': 456,
        snapshots: [
          // Snapshot 123 was expired
          { 'snapshot-id': 456, 'timestamp-ms': 1704067300000 },
        ],
      })

      const valid = await provider.isVersionValid('orders', {
        versionId: '123',
        timestamp: 1704067200000,
        backend: 'iceberg',
      })

      expect(valid).toBe(false)
    })

    it('returns true for empty version', async () => {
      const metadataPath = 'warehouse/test-db/orders/metadata/v1.metadata.json'

      storage._setFile(
        'warehouse/test-db/orders/metadata/version-hint.text',
        metadataPath
      )

      storage._setFile(metadataPath, {
        'current-snapshot-id': null,
        snapshots: [],
      })

      const valid = await provider.isVersionValid('orders', {
        versionId: 'empty',
        timestamp: 1704067200000,
        backend: 'iceberg',
      })

      expect(valid).toBe(true)
    })
  })
})

// =============================================================================
// DeltaSourceVersionProvider Tests
// =============================================================================

describe('DeltaSourceVersionProvider', () => {
  let storage: ReturnType<typeof createMockStorage>
  let provider: DeltaSourceVersionProvider

  beforeEach(() => {
    storage = createMockStorage()
    provider = createDeltaVersionProvider({
      storage,
      location: 'warehouse',
    })
  })

  describe('getCurrentVersion', () => {
    it('returns null when table does not exist', async () => {
      const version = await provider.getCurrentVersion('orders')
      expect(version).toBeNull()
    })

    it('returns version from commit files', async () => {
      // Create commit files
      storage._setFile(
        'warehouse/orders/_delta_log/00000000000000000000.json',
        '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}\n' +
        '{"metaData":{"id":"test"}}\n' +
        '{"commitInfo":{"timestamp":1704067100000,"operation":"CREATE TABLE"}}'
      )
      storage._setFile(
        'warehouse/orders/_delta_log/00000000000000000001.json',
        '{"add":{"path":"data.parquet","size":1024}}\n' +
        '{"commitInfo":{"timestamp":1704067200000,"operation":"WRITE"}}'
      )
      storage._setFile(
        'warehouse/orders/_delta_log/00000000000000000002.json',
        '{"add":{"path":"data2.parquet","size":2048}}\n' +
        '{"commitInfo":{"timestamp":1704067300000,"operation":"WRITE"}}'
      )

      const version = await provider.getCurrentVersion('orders')

      expect(version).not.toBeNull()
      expect(version?.versionId).toBe('txn-2')
      expect(version?.timestamp).toBe(1704067300000)
      expect(version?.backend).toBe('delta')
    })

    it('uses checkpoint to find current version faster', async () => {
      // Create checkpoint
      storage._setFile(
        'warehouse/orders/_delta_log/_last_checkpoint',
        JSON.stringify({ version: 10, size: 5 })
      )

      // Create commit files after checkpoint
      storage._setFile(
        'warehouse/orders/_delta_log/00000000000000000011.json',
        '{"commitInfo":{"timestamp":1704067200000,"operation":"WRITE"}}'
      )
      storage._setFile(
        'warehouse/orders/_delta_log/00000000000000000012.json',
        '{"commitInfo":{"timestamp":1704067300000,"operation":"WRITE"}}'
      )

      const version = await provider.getCurrentVersion('orders')

      expect(version?.versionId).toBe('txn-12')
    })
  })

  describe('getChangeCount', () => {
    beforeEach(() => {
      storage._setFile(
        'warehouse/orders/_delta_log/00000000000000000000.json',
        '{"commitInfo":{"timestamp":1704067100000,"operation":"CREATE TABLE"}}'
      )
      storage._setFile(
        'warehouse/orders/_delta_log/00000000000000000001.json',
        '{"commitInfo":{"timestamp":1704067200000,"operation":"WRITE","operationMetrics":{"numOutputRows":"100"}}}'
      )
      storage._setFile(
        'warehouse/orders/_delta_log/00000000000000000002.json',
        '{"commitInfo":{"timestamp":1704067300000,"operation":"WRITE","operationMetrics":{"numOutputRows":"50"}}}'
      )
      storage._setFile(
        'warehouse/orders/_delta_log/00000000000000000003.json',
        '{"commitInfo":{"timestamp":1704067400000,"operation":"DELETE","operationMetrics":{"numOutputRows":"25"}}}'
      )
    })

    it('sums output rows from commits', async () => {
      const count = await provider.getChangeCount(
        'orders',
        { versionId: 'txn-0', timestamp: 1704067100000, backend: 'delta' },
        { versionId: 'txn-3', timestamp: 1704067400000, backend: 'delta' }
      )

      // Commits 1, 2, 3: 100 + 50 + 25 = 175
      expect(count).toBe(175)
    })

    it('returns 0 for same versions', async () => {
      const count = await provider.getChangeCount(
        'orders',
        { versionId: 'txn-1', timestamp: 1704067200000, backend: 'delta' },
        { versionId: 'txn-1', timestamp: 1704067200000, backend: 'delta' }
      )

      expect(count).toBe(0)
    })

    it('estimates changes when metrics are missing', async () => {
      storage._setFile(
        'warehouse/orders/_delta_log/00000000000000000004.json',
        '{"commitInfo":{"timestamp":1704067500000,"operation":"WRITE"}}'
      )

      const count = await provider.getChangeCount(
        'orders',
        { versionId: 'txn-3', timestamp: 1704067400000, backend: 'delta' },
        { versionId: 'txn-4', timestamp: 1704067500000, backend: 'delta' }
      )

      // No metrics, defaults to 1 change per commit
      expect(count).toBe(1)
    })
  })

  describe('isVersionValid', () => {
    it('returns false when table does not exist', async () => {
      const valid = await provider.isVersionValid('orders', {
        versionId: 'txn-0',
        timestamp: 1000,
        backend: 'delta',
      })

      expect(valid).toBe(false)
    })

    it('returns true for existing commit', async () => {
      storage._setFile(
        'warehouse/orders/_delta_log/00000000000000000000.json',
        '{"commitInfo":{"timestamp":1704067100000,"operation":"CREATE TABLE"}}'
      )

      const valid = await provider.isVersionValid('orders', {
        versionId: 'txn-0',
        timestamp: 1704067100000,
        backend: 'delta',
      })

      expect(valid).toBe(true)
    })

    it('returns false for vacuumed commit', async () => {
      // Only commit 5 exists (0-4 were vacuumed)
      storage._setFile(
        'warehouse/orders/_delta_log/00000000000000000005.json',
        '{"commitInfo":{"timestamp":1704067500000,"operation":"WRITE"}}'
      )

      const valid = await provider.isVersionValid('orders', {
        versionId: 'txn-0',
        timestamp: 1704067100000,
        backend: 'delta',
      })

      expect(valid).toBe(false)
    })

    it('returns false for invalid version ID format', async () => {
      const valid = await provider.isVersionValid('orders', {
        versionId: 'invalid',
        timestamp: 1000,
        backend: 'delta',
      })

      expect(valid).toBe(false)
    })
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('Factory functions', () => {
  it('createNativeVersionProvider creates provider instance', () => {
    const storage = createMockStorage()
    const provider = createNativeVersionProvider({
      storage,
      dataset: 'test',
    })

    expect(provider).toBeInstanceOf(NativeSourceVersionProvider)
  })

  it('createIcebergVersionProvider creates provider instance', () => {
    const storage = createMockStorage()
    const provider = createIcebergVersionProvider({
      storage,
      warehouse: 'warehouse',
    })

    expect(provider).toBeInstanceOf(IcebergSourceVersionProvider)
  })

  it('createDeltaVersionProvider creates provider instance', () => {
    const storage = createMockStorage()
    const provider = createDeltaVersionProvider({
      storage,
      location: 'warehouse',
    })

    expect(provider).toBeInstanceOf(DeltaSourceVersionProvider)
  })
})
