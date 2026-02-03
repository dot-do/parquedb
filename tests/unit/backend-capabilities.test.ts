/**
 * Backend Capability Introspection Tests
 *
 * Tests for runtime capability checking of storage and entity backends.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  MemoryBackend,
  getStorageCapabilities,
  hasStorageCapability,
  isStreamable,
  isMultipart,
  isTransactional,
} from '../../src/storage'
import type { StorageBackend, StorageCapabilities } from '../../src/types/storage'
import {
  createIcebergBackend,
  IcebergBackend,
  getEntityBackendCapabilities,
  hasEntityBackendCapability,
  isCompatibleWithEngine,
} from '../../src/backends'
import type { EntityBackend, EntityBackendCapabilities } from '../../src/backends/types'

describe('Storage Backend Capabilities', () => {
  describe('getStorageCapabilities', () => {
    it('should return capabilities for MemoryBackend', () => {
      const backend = new MemoryBackend()
      const caps = getStorageCapabilities(backend)

      expect(caps.type).toBe('memory')
      expect(caps.atomicWrites).toBe(true)
      expect(caps.conditionalWrites).toBe(true)
      expect(caps.rangeReads).toBe(true)
      expect(caps.append).toBe(true)
      expect(caps.realDirectories).toBe(true)
      expect(caps.requiresMkdir).toBe(false)
      expect(caps.efficientList).toBe(true)
      expect(caps.efficientStat).toBe(true)
    })

    it('should detect streaming capability as false for MemoryBackend', () => {
      const backend = new MemoryBackend()
      const caps = getStorageCapabilities(backend)

      expect(caps.streaming).toBe(false)
    })

    it('should detect multipart capability as false for MemoryBackend', () => {
      const backend = new MemoryBackend()
      const caps = getStorageCapabilities(backend)

      expect(caps.multipart).toBe(false)
    })

    it('should detect transactions capability as false for MemoryBackend', () => {
      const backend = new MemoryBackend()
      const caps = getStorageCapabilities(backend)

      expect(caps.transactions).toBe(false)
    })

    it('should return sensible defaults for unknown backend types', () => {
      // Create a minimal mock backend with unknown type
      const mockBackend: StorageBackend = {
        type: 'custom-backend',
        read: async () => new Uint8Array(),
        readRange: async () => new Uint8Array(),
        exists: async () => false,
        stat: async () => null,
        list: async () => ({ files: [], hasMore: false }),
        write: async () => ({ etag: '', size: 0 }),
        writeAtomic: async () => ({ etag: '', size: 0 }),
        append: async () => {},
        delete: async () => false,
        deletePrefix: async () => 0,
        mkdir: async () => {},
        rmdir: async () => {},
        writeConditional: async () => ({ etag: '', size: 0 }),
        copy: async () => {},
        move: async () => {},
      }

      const caps = getStorageCapabilities(mockBackend)

      expect(caps.type).toBe('custom-backend')
      expect(caps.atomicWrites).toBe(true)
      expect(caps.conditionalWrites).toBe(true)
    })

    it('should detect extended capabilities via interface detection', () => {
      // Create a mock backend that implements streaming
      const mockStreamingBackend = {
        type: 'streaming-test',
        read: async () => new Uint8Array(),
        readRange: async () => new Uint8Array(),
        exists: async () => false,
        stat: async () => null,
        list: async () => ({ files: [], hasMore: false }),
        write: async () => ({ etag: '', size: 0 }),
        writeAtomic: async () => ({ etag: '', size: 0 }),
        append: async () => {},
        delete: async () => false,
        deletePrefix: async () => 0,
        mkdir: async () => {},
        rmdir: async () => {},
        writeConditional: async () => ({ etag: '', size: 0 }),
        copy: async () => {},
        move: async () => {},
        // Extended streaming methods
        createReadStream: () => new ReadableStream(),
        createWriteStream: () => new WritableStream(),
      } as unknown as StorageBackend

      const caps = getStorageCapabilities(mockStreamingBackend)

      expect(caps.streaming).toBe(true)
      expect(isStreamable(mockStreamingBackend)).toBe(true)
    })

    it('should detect multipart capability via interface detection', () => {
      const mockMultipartBackend = {
        type: 'multipart-test',
        read: async () => new Uint8Array(),
        readRange: async () => new Uint8Array(),
        exists: async () => false,
        stat: async () => null,
        list: async () => ({ files: [], hasMore: false }),
        write: async () => ({ etag: '', size: 0 }),
        writeAtomic: async () => ({ etag: '', size: 0 }),
        append: async () => {},
        delete: async () => false,
        deletePrefix: async () => 0,
        mkdir: async () => {},
        rmdir: async () => {},
        writeConditional: async () => ({ etag: '', size: 0 }),
        copy: async () => {},
        move: async () => {},
        // Extended multipart method
        createMultipartUpload: async () => ({
          uploadId: 'test',
          uploadPart: async () => ({ partNumber: 1, etag: '', size: 0 }),
          complete: async () => ({ etag: '', size: 0 }),
          abort: async () => {},
        }),
      } as unknown as StorageBackend

      const caps = getStorageCapabilities(mockMultipartBackend)

      expect(caps.multipart).toBe(true)
      expect(isMultipart(mockMultipartBackend)).toBe(true)
    })
  })

  describe('hasStorageCapability', () => {
    it('should return true for supported capabilities', () => {
      const backend = new MemoryBackend()

      expect(hasStorageCapability(backend, 'atomicWrites')).toBe(true)
      expect(hasStorageCapability(backend, 'conditionalWrites')).toBe(true)
      expect(hasStorageCapability(backend, 'rangeReads')).toBe(true)
    })

    it('should return false for unsupported capabilities', () => {
      const backend = new MemoryBackend()

      expect(hasStorageCapability(backend, 'streaming')).toBe(false)
      expect(hasStorageCapability(backend, 'multipart')).toBe(false)
      expect(hasStorageCapability(backend, 'transactions')).toBe(false)
    })
  })

  describe('Type guards', () => {
    it('isStreamable should return false for MemoryBackend', () => {
      const backend = new MemoryBackend()
      expect(isStreamable(backend)).toBe(false)
    })

    it('isMultipart should return false for MemoryBackend', () => {
      const backend = new MemoryBackend()
      expect(isMultipart(backend)).toBe(false)
    })

    it('isTransactional should return false for MemoryBackend', () => {
      const backend = new MemoryBackend()
      expect(isTransactional(backend)).toBe(false)
    })
  })

  describe('Capability profiles for known backends', () => {
    it('should have appropriate profile for r2 type', () => {
      const mockR2Backend = {
        type: 'r2',
        read: async () => new Uint8Array(),
        readRange: async () => new Uint8Array(),
        exists: async () => false,
        stat: async () => null,
        list: async () => ({ files: [], hasMore: false }),
        write: async () => ({ etag: '', size: 0 }),
        writeAtomic: async () => ({ etag: '', size: 0 }),
        append: async () => {},
        delete: async () => false,
        deletePrefix: async () => 0,
        mkdir: async () => {},
        rmdir: async () => {},
        writeConditional: async () => ({ etag: '', size: 0 }),
        copy: async () => {},
        move: async () => {},
      } as StorageBackend

      const caps = getStorageCapabilities(mockR2Backend)

      expect(caps.type).toBe('r2')
      expect(caps.realDirectories).toBe(false)
      expect(caps.requiresMkdir).toBe(false)
      expect(caps.maxFileSize).toBe(5 * 1024 * 1024 * 1024 * 1024) // 5TB
    })

    it('should have appropriate profile for fs type', () => {
      const mockFsBackend = {
        type: 'fs',
        read: async () => new Uint8Array(),
        readRange: async () => new Uint8Array(),
        exists: async () => false,
        stat: async () => null,
        list: async () => ({ files: [], hasMore: false }),
        write: async () => ({ etag: '', size: 0 }),
        writeAtomic: async () => ({ etag: '', size: 0 }),
        append: async () => {},
        delete: async () => false,
        deletePrefix: async () => 0,
        mkdir: async () => {},
        rmdir: async () => {},
        writeConditional: async () => ({ etag: '', size: 0 }),
        copy: async () => {},
        move: async () => {},
      } as StorageBackend

      const caps = getStorageCapabilities(mockFsBackend)

      expect(caps.type).toBe('fs')
      expect(caps.realDirectories).toBe(true)
      expect(caps.requiresMkdir).toBe(true)
    })

    it('should have appropriate profile for do-sqlite type', () => {
      const mockDoSqliteBackend = {
        type: 'do-sqlite',
        read: async () => new Uint8Array(),
        readRange: async () => new Uint8Array(),
        exists: async () => false,
        stat: async () => null,
        list: async () => ({ files: [], hasMore: false }),
        write: async () => ({ etag: '', size: 0 }),
        writeAtomic: async () => ({ etag: '', size: 0 }),
        append: async () => {},
        delete: async () => false,
        deletePrefix: async () => 0,
        mkdir: async () => {},
        rmdir: async () => {},
        writeConditional: async () => ({ etag: '', size: 0 }),
        copy: async () => {},
        move: async () => {},
      } as StorageBackend

      const caps = getStorageCapabilities(mockDoSqliteBackend)

      expect(caps.type).toBe('do-sqlite')
      expect(caps.maxFileSize).toBe(128 * 1024 * 1024) // 128MB
    })

    it('should have appropriate profile for remote (read-only) type', () => {
      const mockRemoteBackend = {
        type: 'remote',
        read: async () => new Uint8Array(),
        readRange: async () => new Uint8Array(),
        exists: async () => false,
        stat: async () => null,
        list: async () => ({ files: [], hasMore: false }),
        write: async () => ({ etag: '', size: 0 }),
        writeAtomic: async () => ({ etag: '', size: 0 }),
        append: async () => {},
        delete: async () => false,
        deletePrefix: async () => 0,
        mkdir: async () => {},
        rmdir: async () => {},
        writeConditional: async () => ({ etag: '', size: 0 }),
        copy: async () => {},
        move: async () => {},
      } as StorageBackend

      const caps = getStorageCapabilities(mockRemoteBackend)

      expect(caps.type).toBe('remote')
      expect(caps.atomicWrites).toBe(false)
      expect(caps.conditionalWrites).toBe(false)
      expect(caps.append).toBe(false)
    })
  })
})

// =============================================================================
// Entity Backend Capabilities
// =============================================================================

describe('Entity Backend Capabilities', () => {
  let storage: MemoryBackend
  let backend: IcebergBackend

  beforeEach(async () => {
    storage = new MemoryBackend()
    backend = createIcebergBackend({
      type: 'iceberg',
      storage,
      warehouse: 'warehouse',
      database: 'testdb',
    })
    await backend.initialize()
  })

  afterEach(async () => {
    await backend.close()
  })

  describe('getEntityBackendCapabilities', () => {
    it('should return capabilities for IcebergBackend', () => {
      const caps = getEntityBackendCapabilities(backend)

      expect(caps.type).toBe('iceberg')
      expect(caps.timeTravel).toBe(true)
      expect(caps.schemaEvolution).toBe(true)
      expect(caps.readOnly).toBe(false)
    })

    it('should detect optional operations for IcebergBackend', () => {
      const caps = getEntityBackendCapabilities(backend)

      // Iceberg should support these optional operations
      expect(caps.snapshots).toBe(true)
      expect(caps.setSchema).toBe(true)
      expect(caps.compact).toBe(true)
      expect(caps.vacuum).toBe(true)
      expect(caps.stats).toBe(true)
    })

    it('should have Iceberg-specific table format features', () => {
      const caps = getEntityBackendCapabilities(backend)

      expect(caps.acidTransactions).toBe(true)
      expect(caps.partitioning).toBe(true)
      expect(caps.columnStatistics).toBe(true)
      expect(caps.mergeOnRead).toBe(true)
      expect(caps.copyOnWrite).toBe(true)
    })

    it('should report external query engine compatibility', () => {
      const caps = getEntityBackendCapabilities(backend)

      expect(caps.externalQueryEngines).toBe(true)
      expect(caps.compatibleEngines).toContain('duckdb')
      expect(caps.compatibleEngines).toContain('spark')
      expect(caps.compatibleEngines).toContain('snowflake')
      expect(caps.compatibleEngines).toContain('trino')
    })
  })

  describe('hasEntityBackendCapability', () => {
    it('should return true for supported capabilities', () => {
      expect(hasEntityBackendCapability(backend, 'timeTravel')).toBe(true)
      expect(hasEntityBackendCapability(backend, 'schemaEvolution')).toBe(true)
      expect(hasEntityBackendCapability(backend, 'acidTransactions')).toBe(true)
    })

    it('should return false for readOnly on writable backend', () => {
      expect(hasEntityBackendCapability(backend, 'readOnly')).toBe(false)
    })
  })

  describe('isCompatibleWithEngine', () => {
    it('should return true for compatible engines', () => {
      expect(isCompatibleWithEngine(backend, 'duckdb')).toBe(true)
      expect(isCompatibleWithEngine(backend, 'DuckDB')).toBe(true) // case insensitive
      expect(isCompatibleWithEngine(backend, 'spark')).toBe(true)
      expect(isCompatibleWithEngine(backend, 'snowflake')).toBe(true)
    })

    it('should return false for incompatible engines', () => {
      expect(isCompatibleWithEngine(backend, 'mysql')).toBe(false)
      expect(isCompatibleWithEngine(backend, 'postgres')).toBe(false)
    })
  })

  describe('Mock backends for coverage', () => {
    it('should handle native backend type', () => {
      const mockNativeBackend: EntityBackend = {
        type: 'native',
        supportsTimeTravel: false,
        supportsSchemaEvolution: false,
        readOnly: false,
        initialize: async () => {},
        close: async () => {},
        get: async () => null,
        find: async () => [],
        count: async () => 0,
        exists: async () => false,
        create: async () => ({ $id: 'test/1', $type: 'Test', name: 'test', version: 1, createdAt: new Date(), updatedAt: new Date() }),
        update: async () => ({ $id: 'test/1', $type: 'Test', name: 'test', version: 1, createdAt: new Date(), updatedAt: new Date() }),
        delete: async () => ({ deleted: true }),
        bulkCreate: async () => [],
        bulkUpdate: async () => ({ matchedCount: 0, modifiedCount: 0 }),
        bulkDelete: async () => ({ deletedCount: 0 }),
        getSchema: async () => null,
        listNamespaces: async () => [],
      }

      const caps = getEntityBackendCapabilities(mockNativeBackend)

      expect(caps.type).toBe('native')
      expect(caps.timeTravel).toBe(false)
      expect(caps.acidTransactions).toBe(false)
      expect(caps.externalQueryEngines).toBe(false)
      expect(caps.compatibleEngines).toEqual([])
    })

    it('should handle delta backend type', () => {
      const mockDeltaBackend: EntityBackend = {
        type: 'delta',
        supportsTimeTravel: true,
        supportsSchemaEvolution: true,
        readOnly: false,
        initialize: async () => {},
        close: async () => {},
        get: async () => null,
        find: async () => [],
        count: async () => 0,
        exists: async () => false,
        create: async () => ({ $id: 'test/1', $type: 'Test', name: 'test', version: 1, createdAt: new Date(), updatedAt: new Date() }),
        update: async () => ({ $id: 'test/1', $type: 'Test', name: 'test', version: 1, createdAt: new Date(), updatedAt: new Date() }),
        delete: async () => ({ deleted: true }),
        bulkCreate: async () => [],
        bulkUpdate: async () => ({ matchedCount: 0, modifiedCount: 0 }),
        bulkDelete: async () => ({ deletedCount: 0 }),
        getSchema: async () => null,
        listNamespaces: async () => [],
        snapshot: async () => mockDeltaBackend,
        listSnapshots: async () => [],
      }

      const caps = getEntityBackendCapabilities(mockDeltaBackend)

      expect(caps.type).toBe('delta')
      expect(caps.timeTravel).toBe(true)
      expect(caps.acidTransactions).toBe(true)
      expect(caps.mergeOnRead).toBe(false) // Delta uses copy-on-write
      expect(caps.copyOnWrite).toBe(true)
      expect(caps.compatibleEngines).toContain('duckdb')
      expect(caps.compatibleEngines).toContain('spark')
      expect(caps.compatibleEngines).toContain('databricks')
    })

    it('should detect missing optional methods', () => {
      const minimalBackend: EntityBackend = {
        type: 'native',
        supportsTimeTravel: false,
        supportsSchemaEvolution: false,
        readOnly: true,
        initialize: async () => {},
        close: async () => {},
        get: async () => null,
        find: async () => [],
        count: async () => 0,
        exists: async () => false,
        create: async () => ({ $id: 'test/1', $type: 'Test', name: 'test', version: 1, createdAt: new Date(), updatedAt: new Date() }),
        update: async () => ({ $id: 'test/1', $type: 'Test', name: 'test', version: 1, createdAt: new Date(), updatedAt: new Date() }),
        delete: async () => ({ deleted: true }),
        bulkCreate: async () => [],
        bulkUpdate: async () => ({ matchedCount: 0, modifiedCount: 0 }),
        bulkDelete: async () => ({ deletedCount: 0 }),
        getSchema: async () => null,
        listNamespaces: async () => [],
        // No optional methods: snapshot, listSnapshots, setSchema, compact, vacuum, stats
      }

      const caps = getEntityBackendCapabilities(minimalBackend)

      expect(caps.snapshots).toBe(false)
      expect(caps.setSchema).toBe(false)
      expect(caps.compact).toBe(false)
      expect(caps.vacuum).toBe(false)
      expect(caps.stats).toBe(false)
      expect(caps.readOnly).toBe(true)
    })
  })
})
