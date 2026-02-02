/**
 * Tests for Apache Iceberg Metadata Integration
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  IcebergMetadataManager,
  createIcebergMetadataManager,
  parqueDBTypeToIceberg,
  icebergTypeToParqueDB,
} from '../../../src/integrations/iceberg'

describe('IcebergMetadataManager', () => {
  let storage: MemoryBackend
  let manager: IcebergMetadataManager

  beforeEach(async () => {
    storage = new MemoryBackend()
    manager = createIcebergMetadataManager(storage, {
      location: '/warehouse/posts',
      namespace: 'default',
      tableName: 'posts',
    })
    await manager.initialize()
  })

  describe('initialization', () => {
    it('should create a new table if it does not exist', async () => {
      const metadataPath = '/warehouse/posts/metadata/v1.metadata.json'
      const exists = await storage.exists(metadataPath)
      expect(exists).toBe(true)
    })

    it('should load existing table metadata', async () => {
      // Create another manager for the same location
      const manager2 = createIcebergMetadataManager(storage, {
        location: '/warehouse/posts',
      })
      await manager2.initialize()

      // Should not throw
      const snapshots = await manager2.listSnapshots()
      expect(snapshots).toBeInstanceOf(Array)
    })
  })

  describe('snapshot operations', () => {
    it('should create a snapshot from data files', async () => {
      const result = await manager.createSnapshot(
        [{ seq: 1, path: '/data/seg-1.parquet', minTs: 1000, maxTs: 2000, count: 100, sizeBytes: 4096, createdAt: Date.now() }],
        [{ path: '/data/seg-1.parquet', format: 'parquet', recordCount: 100, sizeBytes: 4096, partition: {} }]
      )

      expect(result.success).toBe(true)
      expect(result.snapshotId).toBeGreaterThan(0n)
    })

    it('should list snapshots', async () => {
      // Create a snapshot first
      await manager.createSnapshot(
        [],
        [{ path: '/data/file1.parquet', format: 'parquet', recordCount: 50, sizeBytes: 2048, partition: {} }]
      )

      const snapshots = await manager.listSnapshots()
      expect(snapshots.length).toBe(1)
      expect(snapshots[0].summary.addedRecords).toBe(50)
    })

    it('should get snapshot at timestamp', async () => {
      const now = Date.now()
      await manager.createSnapshot(
        [],
        [{ path: '/data/file1.parquet', format: 'parquet', recordCount: 100, sizeBytes: 4096, partition: {} }]
      )

      const snapshot = await manager.getSnapshotAtTimestamp(now + 1000)
      expect(snapshot).not.toBeNull()
      expect(snapshot?.summary.addedRecords).toBe(100)
    })

    it('should return null for timestamp before first snapshot', async () => {
      await manager.createSnapshot(
        [],
        [{ path: '/data/file1.parquet', format: 'parquet', recordCount: 100, sizeBytes: 4096, partition: {} }]
      )

      const snapshot = await manager.getSnapshotAtTimestamp(0)
      expect(snapshot).toBeNull()
    })
  })

  describe('schema operations', () => {
    it('should get current schema', async () => {
      const schema = await manager.getCurrentSchema()
      expect(schema.schemaId).toBe(0)
      expect(schema.fields.length).toBeGreaterThan(0)
      expect(schema.fields.find(f => f.name === '$id')).toBeDefined()
    })
  })

  describe('export for query engines', () => {
    it('should export metadata location', async () => {
      const exportInfo = await manager.exportForQueryEngine()
      expect(exportInfo.metadataPath).toContain('/warehouse/posts/metadata')
      expect(exportInfo.location).toBe('/warehouse/posts')
    })
  })
})

describe('Type Conversion', () => {
  describe('parqueDBTypeToIceberg', () => {
    it('should convert string types', () => {
      expect(parqueDBTypeToIceberg('string')).toBe('string')
    })

    it('should convert number to double', () => {
      expect(parqueDBTypeToIceberg('number')).toBe('double')
    })

    it('should convert integer to long', () => {
      expect(parqueDBTypeToIceberg('integer')).toBe('long')
    })

    it('should convert boolean', () => {
      expect(parqueDBTypeToIceberg('boolean')).toBe('boolean')
    })

    it('should convert datetime to timestamptz', () => {
      expect(parqueDBTypeToIceberg('datetime')).toBe('timestamptz')
    })

    it('should default to string for unknown types', () => {
      expect(parqueDBTypeToIceberg('unknown')).toBe('string')
    })
  })

  describe('icebergTypeToParqueDB', () => {
    it('should convert string types', () => {
      expect(icebergTypeToParqueDB('string')).toBe('string')
    })

    it('should convert long to integer', () => {
      expect(icebergTypeToParqueDB('long')).toBe('integer')
    })

    it('should convert double to number', () => {
      expect(icebergTypeToParqueDB('double')).toBe('number')
    })

    it('should convert timestamptz to datetime', () => {
      expect(icebergTypeToParqueDB('timestamptz')).toBe('datetime')
    })

    it('should convert list type to array', () => {
      expect(icebergTypeToParqueDB({ type: 'list', elementId: 1, element: 'string', elementRequired: true })).toBe('array')
    })

    it('should convert struct type to object', () => {
      expect(icebergTypeToParqueDB({ type: 'struct', fields: [] })).toBe('object')
    })
  })
})
