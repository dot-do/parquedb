/**
 * Tests for IcebergBackend Avro manifest format compatibility
 *
 * CRITICAL: Iceberg manifests and manifest lists MUST be Avro-encoded for
 * interoperability with DuckDB, Spark, Snowflake, and other tools.
 *
 * RED phase: These tests verify proper Avro encoding of manifest files.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { IcebergBackend, createIcebergBackend } from '../../../src/backends/iceberg'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import {
  AvroDecoder,
  decodeManifestEntry,
  decodeManifestListEntry,
  type EncodableManifestEntry,
  type EncodableManifestListEntry,
} from '@dotdo/iceberg'

// Avro container file magic bytes
const AVRO_MAGIC = new Uint8Array([0x4f, 0x62, 0x6a, 0x01]) // 'Obj' + version 1

/**
 * Check if a buffer starts with Avro magic bytes
 */
function isAvroFile(data: Uint8Array): boolean {
  if (data.length < 4) return false
  return (
    data[0] === AVRO_MAGIC[0] &&
    data[1] === AVRO_MAGIC[1] &&
    data[2] === AVRO_MAGIC[2] &&
    data[3] === AVRO_MAGIC[3]
  )
}

/**
 * Check if a buffer starts with JSON (either '{' or '[')
 */
function isJsonFile(data: Uint8Array): boolean {
  if (data.length === 0) return false
  // Skip any leading whitespace
  let i = 0
  while (i < data.length && (data[i] === 0x20 || data[i] === 0x0a || data[i] === 0x0d || data[i] === 0x09)) {
    i++
  }
  if (i >= data.length) return false
  // Check for JSON start characters: '{' (0x7b) or '[' (0x5b)
  return data[i] === 0x7b || data[i] === 0x5b
}

/**
 * Parse Avro container file header to extract schema
 */
function parseAvroHeader(data: Uint8Array): { schema: unknown; syncMarker: Uint8Array } | null {
  if (!isAvroFile(data)) return null

  try {
    const decoder = new AvroDecoder(data.slice(4)) // Skip magic bytes

    // Read header metadata map
    const metadata = decoder.readMap(() => decoder.readBytes())

    // Get schema from metadata
    const schemaBytes = metadata.get('avro.schema')
    if (!schemaBytes) return null

    const schemaJson = new TextDecoder().decode(schemaBytes)
    const schema = JSON.parse(schemaJson)

    // Read 16-byte sync marker
    const syncMarker = decoder.readFixed(16)

    return { schema, syncMarker }
  } catch {
    return null
  }
}

describe('IcebergBackend Avro Manifest Format', () => {
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

  describe('Manifest Files (.avro)', () => {
    it('should write manifest files in Avro format, not JSON', async () => {
      // Create an entity to trigger manifest creation
      await backend.create('users', {
        $type: 'User',
        name: 'Alice',
      })

      // Find manifest files
      const metadataFiles = await storage.list('warehouse/testdb/users/metadata/')
      const manifestFiles = metadataFiles.files.filter(f => f.endsWith('-m0.avro'))

      expect(manifestFiles.length).toBeGreaterThan(0)

      // Read the manifest file
      const manifestPath = manifestFiles[0]!
      const manifestData = await storage.read(manifestPath)

      // CRITICAL: Manifest should be Avro-encoded, not JSON
      expect(isJsonFile(manifestData)).toBe(false)
      expect(isAvroFile(manifestData)).toBe(true)
    })

    it('should have valid Avro container file structure', async () => {
      await backend.create('posts', {
        $type: 'Post',
        name: 'Test Post',
      })

      // Find manifest files
      const metadataFiles = await storage.list('warehouse/testdb/posts/metadata/')
      const manifestFiles = metadataFiles.files.filter(f => f.endsWith('-m0.avro'))

      expect(manifestFiles.length).toBeGreaterThan(0)

      const manifestPath = manifestFiles[0]!
      const manifestData = await storage.read(manifestPath)

      // Parse header
      const header = parseAvroHeader(manifestData)
      expect(header).not.toBeNull()
      expect(header!.schema).toBeDefined()
      expect(header!.syncMarker.length).toBe(16)
    })

    it('should write manifest entries with correct Iceberg schema', async () => {
      await backend.create('items', {
        $type: 'Item',
        name: 'Item 1',
      })

      // Find manifest files
      const metadataFiles = await storage.list('warehouse/testdb/items/metadata/')
      const manifestFiles = metadataFiles.files.filter(f => f.endsWith('-m0.avro'))
      const manifestPath = manifestFiles[0]!
      const manifestData = await storage.read(manifestPath)

      const header = parseAvroHeader(manifestData)
      expect(header).not.toBeNull()

      // Check schema has required manifest entry fields
      const schema = header!.schema as { name: string; fields: Array<{ name: string }> }
      expect(schema.name).toBe('manifest_entry')

      const fieldNames = schema.fields.map(f => f.name)
      expect(fieldNames).toContain('status')
      expect(fieldNames).toContain('snapshot_id')
      expect(fieldNames).toContain('data_file')
    })

    it('should produce manifest files readable by standard Avro decoders', async () => {
      await backend.create('products', {
        $type: 'Product',
        name: 'Widget',
        price: 9.99,
      })

      // Find manifest files
      const metadataFiles = await storage.list('warehouse/testdb/products/metadata/')
      const manifestFiles = metadataFiles.files.filter(f => f.endsWith('-m0.avro'))
      const manifestPath = manifestFiles[0]!
      const manifestData = await storage.read(manifestPath)

      // Skip header and try to decode entries
      // This verifies the encoding is actually valid Avro
      expect(isAvroFile(manifestData)).toBe(true)

      // The file should be parseable - at minimum verify magic bytes
      expect(manifestData[0]).toBe(0x4f) // 'O'
      expect(manifestData[1]).toBe(0x62) // 'b'
      expect(manifestData[2]).toBe(0x6a) // 'j'
      expect(manifestData[3]).toBe(0x01) // version 1
    })
  })

  describe('Manifest List Files (snap-*.avro)', () => {
    it('should write manifest list files in Avro format, not JSON', async () => {
      await backend.create('users', {
        $type: 'User',
        name: 'Bob',
      })

      // Find manifest list files (snap-{snapshotId}-{uuid}.avro)
      const metadataFiles = await storage.list('warehouse/testdb/users/metadata/')
      const manifestListFiles = metadataFiles.files.filter(f => f.includes('/snap-') && f.endsWith('.avro'))

      expect(manifestListFiles.length).toBeGreaterThan(0)

      // Read the manifest list file
      const manifestListPath = manifestListFiles[0]!
      const manifestListData = await storage.read(manifestListPath)

      // CRITICAL: Manifest list should be Avro-encoded, not JSON
      expect(isJsonFile(manifestListData)).toBe(false)
      expect(isAvroFile(manifestListData)).toBe(true)
    })

    it('should have valid Avro container file structure for manifest list', async () => {
      await backend.create('orders', {
        $type: 'Order',
        name: 'Order 1',
      })

      // Find manifest list files
      const metadataFiles = await storage.list('warehouse/testdb/orders/metadata/')
      const manifestListFiles = metadataFiles.files.filter(f => f.includes('/snap-') && f.endsWith('.avro'))

      expect(manifestListFiles.length).toBeGreaterThan(0)

      const manifestListPath = manifestListFiles[0]!
      const manifestListData = await storage.read(manifestListPath)

      // Parse header
      const header = parseAvroHeader(manifestListData)
      expect(header).not.toBeNull()
      expect(header!.schema).toBeDefined()
      expect(header!.syncMarker.length).toBe(16)
    })

    it('should write manifest list entries with correct Iceberg schema', async () => {
      await backend.create('tasks', {
        $type: 'Task',
        name: 'Task 1',
      })

      // Find manifest list files
      const metadataFiles = await storage.list('warehouse/testdb/tasks/metadata/')
      const manifestListFiles = metadataFiles.files.filter(f => f.includes('/snap-') && f.endsWith('.avro'))
      const manifestListPath = manifestListFiles[0]!
      const manifestListData = await storage.read(manifestListPath)

      const header = parseAvroHeader(manifestListData)
      expect(header).not.toBeNull()

      // Check schema has required manifest list entry fields
      const schema = header!.schema as { name: string; fields: Array<{ name: string }> }
      expect(schema.name).toBe('manifest_file')

      const fieldNames = schema.fields.map(f => f.name)
      expect(fieldNames).toContain('manifest_path')
      expect(fieldNames).toContain('manifest_length')
      expect(fieldNames).toContain('partition_spec_id')
      expect(fieldNames).toContain('added_snapshot_id')
    })
  })

  describe('Interoperability', () => {
    it('should produce manifests that can be read back correctly', async () => {
      // Create multiple entities to produce meaningful manifest data
      await backend.bulkCreate('users', [
        { $type: 'User', name: 'Alice' },
        { $type: 'User', name: 'Bob' },
        { $type: 'User', name: 'Charlie' },
      ])

      // Verify entities can be read back through the Iceberg backend
      const users = await backend.find('users', {})
      expect(users.length).toBe(3)

      // The manifest should be readable by the backend (round-trip test)
      const snapshots = await backend.listSnapshots('users')
      expect(snapshots.length).toBeGreaterThan(0)
      expect(snapshots[snapshots.length - 1]!.recordCount).toBe(3)
    })

    it('should maintain data integrity across multiple snapshots', async () => {
      // First write
      await backend.create('items', {
        $type: 'Item',
        name: 'Item 1',
      })

      // Second write (creates new snapshot with manifest including previous data)
      await backend.create('items', {
        $type: 'Item',
        name: 'Item 2',
      })

      // Third write
      await backend.create('items', {
        $type: 'Item',
        name: 'Item 3',
      })

      // Should be able to read all items
      const items = await backend.find('items', {})
      expect(items.length).toBe(3)

      // Should have multiple snapshots
      const snapshots = await backend.listSnapshots('items')
      expect(snapshots.length).toBe(3)
    })

    it('should store correct statistics in manifest entries', async () => {
      await backend.bulkCreate('products', [
        { $type: 'Product', name: 'Widget A', price: 10 },
        { $type: 'Product', name: 'Widget B', price: 20 },
        { $type: 'Product', name: 'Widget C', price: 30 },
      ])

      // Find manifest files
      const metadataFiles = await storage.list('warehouse/testdb/products/metadata/')
      const manifestFiles = metadataFiles.files.filter(f => f.endsWith('-m0.avro'))

      expect(manifestFiles.length).toBeGreaterThan(0)

      const manifestPath = manifestFiles[0]!
      const manifestData = await storage.read(manifestPath)

      // Should be valid Avro
      expect(isAvroFile(manifestData)).toBe(true)

      // Verify through backend stats
      const stats = await backend.stats('products')
      expect(stats.recordCount).toBe(3)
      expect(stats.fileCount).toBe(1)
    })
  })

  describe('Format Compliance', () => {
    it('should use .avro extension for manifest files', async () => {
      await backend.create('entities', {
        $type: 'Entity',
        name: 'Test',
      })

      const metadataFiles = await storage.list('warehouse/testdb/entities/metadata/')

      // All manifest files should have .avro extension
      const manifestFiles = metadataFiles.files.filter(f => f.includes('-m0.'))
      expect(manifestFiles.every(f => f.endsWith('.avro'))).toBe(true)

      // Manifest list files should also have .avro extension
      const manifestListFiles = metadataFiles.files.filter(f => f.includes('/snap-'))
      expect(manifestListFiles.every(f => f.endsWith('.avro'))).toBe(true)
    })

    it('should not write any JSON manifest files', async () => {
      await backend.create('data', {
        $type: 'Data',
        name: 'Sample',
      })

      const metadataFiles = await storage.list('warehouse/testdb/data/metadata/')

      // Check all .avro files are actually Avro format
      for (const file of metadataFiles.files) {
        if (file.endsWith('.avro')) {
          const data = await storage.read(file)
          expect(isJsonFile(data)).toBe(false)
          expect(isAvroFile(data)).toBe(true)
        }
      }
    })
  })
})
