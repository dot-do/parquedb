/**
 * Iceberg Table Format Test Suite
 *
 * Tests the IcebergFormat class which produces Iceberg-compatible table
 * layouts from MergeTree compacted data. Validates metadata generation,
 * snapshot creation, manifest entries, and Parquet data file encoding.
 *
 * These are standard Node.js tests (not workers).
 */

import { describe, it, expect } from 'vitest'
import {
  IcebergFormat,
  type IcebergTableMetadata,
  type IcebergManifestEntry,
} from '@/engine/formats/iceberg'
import { parseDataField } from '@/engine/parquet-data-utils'

// =============================================================================
// Helpers
// =============================================================================

/** Decode a Parquet ArrayBuffer into rows using hyparquet */
async function decodeParquet(buffer: ArrayBuffer): Promise<Array<Record<string, unknown>>> {
  const { parquetReadObjects } = await import('hyparquet')
  const asyncBuffer = {
    byteLength: buffer.byteLength,
    slice: async (start: number, end?: number) => buffer.slice(start, end ?? buffer.byteLength),
  }
  return parquetReadObjects({ file: asyncBuffer }) as Promise<Array<Record<string, unknown>>>
}

/** Create a simple IcebergFormat instance for testing */
function createFormat(basePath = 'tables/users', tableName = 'users'): IcebergFormat {
  return new IcebergFormat({ basePath, tableName })
}

/** Create sample data lines */
function makeSampleData(count = 3): Array<{ $id: string; $op: string; $v: number; $ts: number; [key: string]: unknown }> {
  const data: Array<{ $id: string; $op: string; $v: number; $ts: number; [key: string]: unknown }> = []
  for (let i = 0; i < count; i++) {
    data.push({
      $id: `user-${String(i + 1).padStart(3, '0')}`,
      $op: 'c',
      $v: 1,
      $ts: 1000 + i * 100,
      name: `User ${i + 1}`,
      email: `user${i + 1}@example.com`,
    })
  }
  return data
}

// =============================================================================
// Schema Tests
// =============================================================================

describe('IcebergFormat.getDefaultSchema', () => {
  it('1. returns correct Iceberg schema with all MergeTree fields', () => {
    const format = createFormat()
    const schema = format.getDefaultSchema()

    expect(schema.schemaId).toBe(0)
    expect(schema.type).toBe('struct')
    expect(schema.fields).toHaveLength(5)

    // Verify each field
    expect(schema.fields[0]).toEqual({ id: 1, name: '$id', required: true, type: 'string' })
    expect(schema.fields[1]).toEqual({ id: 2, name: '$op', required: true, type: 'string' })
    expect(schema.fields[2]).toEqual({ id: 3, name: '$v', required: true, type: 'int' })
    expect(schema.fields[3]).toEqual({ id: 4, name: '$ts', required: true, type: 'double' })
    expect(schema.fields[4]).toEqual({ id: 5, name: '$data', required: false, type: 'string' })
  })
})

// =============================================================================
// Table Metadata Tests
// =============================================================================

describe('IcebergFormat.createTableMetadata', () => {
  it('2. generates valid v1 metadata with UUID and location', () => {
    const format = createFormat('tables/users', 'users')
    const metadata = format.createTableMetadata()

    // Has a valid UUID
    expect(metadata.tableUuid).toBeDefined()
    expect(typeof metadata.tableUuid).toBe('string')
    expect(metadata.tableUuid.length).toBeGreaterThan(0)

    // Location matches basePath
    expect(metadata.location).toBe('tables/users')

    // Timestamps are recent
    expect(metadata.lastUpdatedMs).toBeGreaterThan(0)
    expect(metadata.lastUpdatedMs).toBeLessThanOrEqual(Date.now())

    // Empty snapshot state
    expect(metadata.currentSnapshotId).toBe(-1)
    expect(metadata.snapshots).toEqual([])
    expect(metadata.snapshotLog).toEqual([])
    expect(metadata.metadataLog).toEqual([])

    // Properties
    expect(metadata.properties['engine']).toBe('parquedb-mergetree')
    expect(metadata.properties['table.name']).toBe('users')
  })

  it('3. has format-version 2', () => {
    const format = createFormat()
    const metadata = format.createTableMetadata()

    expect(metadata.formatVersion).toBe(2)
  })

  it('4. has correct schema matching getDefaultSchema', () => {
    const format = createFormat()
    const metadata = format.createTableMetadata()

    expect(metadata.schemas).toHaveLength(1)
    expect(metadata.currentSchemaId).toBe(0)

    const schema = metadata.schemas[0]
    const defaultSchema = format.getDefaultSchema()

    expect(schema).toEqual(defaultSchema)

    // Verify lastColumnId matches field count
    expect(metadata.lastColumnId).toBe(5)
  })

  it('5. has unpartitioned default partition spec', () => {
    const format = createFormat()
    const metadata = format.createTableMetadata()

    expect(metadata.partitionSpecs).toHaveLength(1)
    expect(metadata.defaultSpecId).toBe(0)
    expect(metadata.partitionSpecs[0]).toEqual({
      specId: 0,
      fields: [],
    })
    expect(metadata.lastPartitionId).toBe(0)
  })
})

// =============================================================================
// Snapshot Tests
// =============================================================================

describe('IcebergFormat.createSnapshot', () => {
  it('6. with null existing metadata creates first snapshot', async () => {
    const format = createFormat()
    const data = makeSampleData(3)

    const result = await format.createSnapshot(null, data)

    // Metadata should be populated
    expect(result.metadata.formatVersion).toBe(2)
    expect(result.metadata.snapshots).toHaveLength(1)
    expect(result.metadata.currentSnapshotId).toBe(1)

    // Snapshot details
    const snapshot = result.metadata.snapshots[0]
    expect(snapshot.snapshotId).toBe(1)
    expect(snapshot.timestampMs).toBeGreaterThan(0)
    expect(snapshot.summary.operation).toBe('append')
    expect(snapshot.summary.addedDataFiles).toBe(1)
    expect(snapshot.summary.addedRecords).toBe(3)

    // Snapshot log
    expect(result.metadata.snapshotLog).toHaveLength(1)
    expect(result.metadata.snapshotLog[0].snapshotId).toBe(1)
  })

  it('7. encodes data to valid Parquet buffer', async () => {
    const format = createFormat()
    const data = makeSampleData(2)

    const result = await format.createSnapshot(null, data)

    // Data buffer is valid Parquet
    expect(result.dataBuffer).toBeInstanceOf(ArrayBuffer)
    expect(result.dataBuffer.byteLength).toBeGreaterThan(0)

    // Verify PAR1 magic bytes
    const view = new Uint8Array(result.dataBuffer)
    expect(view[0]).toBe(0x50) // P
    expect(view[1]).toBe(0x41) // A
    expect(view[2]).toBe(0x52) // R
    expect(view[3]).toBe(0x31) // 1

    // Verify roundtrip: decode and check data
    const rows = await decodeParquet(result.dataBuffer)
    expect(rows).toHaveLength(2)

    // Data is sorted by $id
    expect(rows[0].$id).toBe('user-001')
    expect(rows[1].$id).toBe('user-002')

    // $data column contains the extra fields (VARIANT, decoded via parseDataField)
    const parsed = parseDataField(rows[0].$data)
    expect(parsed.name).toBe('User 1')
    expect(parsed.email).toBe('user1@example.com')
  })

  it('8. generates manifest with file stats', async () => {
    const format = createFormat('tables/posts', 'posts')
    const data = makeSampleData(5)

    const result = await format.createSnapshot(null, data)

    // Manifest has one entry
    expect(result.manifest).toHaveLength(1)

    const entry = result.manifest[0]
    expect(entry.status).toBe(1) // added
    expect(entry.dataFile.fileFormat).toBe('PARQUET')
    expect(entry.dataFile.recordCount).toBe(5)
    expect(entry.dataFile.fileSizeInBytes).toBe(result.dataBuffer.byteLength)

    // File path points to data directory
    expect(entry.dataFile.filePath).toContain('tables/posts/data/')
    expect(entry.dataFile.filePath.endsWith('.parquet')).toBe(true)

    // Column stats present for all 5 fields
    expect(Object.keys(entry.dataFile.valueCounts)).toHaveLength(5)
    expect(Object.keys(entry.dataFile.columnSizes)).toHaveLength(5)

    // All rows have values for required fields
    expect(entry.dataFile.valueCounts[1]).toBe(5) // $id
    expect(entry.dataFile.valueCounts[2]).toBe(5) // $op
    expect(entry.dataFile.nullValueCounts[1]).toBe(0) // $id has no nulls
    expect(entry.dataFile.nullValueCounts[2]).toBe(0) // $op has no nulls

    // Lower/upper bounds for $id
    expect(entry.dataFile.lowerBounds[1]).toBe('user-001')
    expect(entry.dataFile.upperBounds[1]).toBe('user-005')

    // Lower/upper bounds for $op (all 'c')
    expect(entry.dataFile.lowerBounds[2]).toBe('c')
    expect(entry.dataFile.upperBounds[2]).toBe('c')
  })

  it('9. increments snapshot ID on subsequent calls', async () => {
    const format = createFormat()
    const data1 = makeSampleData(2)
    const data2 = makeSampleData(3)

    // First snapshot
    const result1 = await format.createSnapshot(null, data1)
    expect(result1.metadata.currentSnapshotId).toBe(1)

    // Second snapshot, passing first metadata
    const result2 = await format.createSnapshot(result1.metadata, data2)
    expect(result2.metadata.currentSnapshotId).toBe(2)

    // Verify snapshot IDs
    expect(result2.metadata.snapshots[0].snapshotId).toBe(1)
    expect(result2.metadata.snapshots[1].snapshotId).toBe(2)
  })

  it('10. preserves previous snapshots in metadata', async () => {
    const format = createFormat()

    // Create first snapshot
    const result1 = await format.createSnapshot(null, makeSampleData(2))
    expect(result1.metadata.snapshots).toHaveLength(1)

    // Create second snapshot
    const result2 = await format.createSnapshot(result1.metadata, makeSampleData(3))
    expect(result2.metadata.snapshots).toHaveLength(2)

    // First snapshot is preserved with original data
    const firstSnapshot = result2.metadata.snapshots[0]
    expect(firstSnapshot.snapshotId).toBe(1)
    expect(firstSnapshot.summary.addedRecords).toBe(2)

    // Second snapshot has new data
    const secondSnapshot = result2.metadata.snapshots[1]
    expect(secondSnapshot.snapshotId).toBe(2)
    expect(secondSnapshot.summary.addedRecords).toBe(3)

    // Create third snapshot
    const result3 = await format.createSnapshot(result2.metadata, makeSampleData(1))
    expect(result3.metadata.snapshots).toHaveLength(3)
    expect(result3.metadata.snapshots[2].snapshotId).toBe(3)
    expect(result3.metadata.snapshots[2].summary.addedRecords).toBe(1)
  })

  it('handles empty data array without crashing (parquedb-zou5.2)', async () => {
    const format = createFormat()

    // createSnapshot(null, []) should not crash - core regression test
    const result = await format.createSnapshot(null, [])
    expect(result).toBeDefined()

    // Metadata should be valid
    expect(result.metadata.formatVersion).toBe(2)
    expect(result.metadata.snapshots).toHaveLength(1)
    expect(result.metadata.currentSnapshotId).toBe(1)

    // Snapshot summary should show 0 records
    const snapshot = result.metadata.snapshots[0]
    expect(snapshot.summary.addedRecords).toBe(0)
    expect(snapshot.summary.addedDataFiles).toBe(1)

    // Manifest should exist with 0-record entry
    expect(result.manifest).toHaveLength(1)
    expect(result.manifest[0].dataFile.recordCount).toBe(0)
    expect(result.manifest[0].dataFile.fileSizeInBytes).toBeGreaterThan(0)

    // Column stats should be empty (no data to compute bounds from)
    const entry = result.manifest[0]
    expect(Object.keys(entry.dataFile.lowerBounds)).toHaveLength(0)
    expect(Object.keys(entry.dataFile.upperBounds)).toHaveLength(0)

    // Data buffer should be valid Parquet
    expect(result.dataBuffer).toBeInstanceOf(ArrayBuffer)
    expect(result.dataBuffer.byteLength).toBeGreaterThan(0)

    // All paths should be valid
    expect(result.metadataPath).toContain('metadata/v')
    expect(result.dataPath).toContain('data/')
    expect(result.dataPath.endsWith('.parquet')).toBe(true)
  })

  it('handles empty data array with existing metadata (parquedb-zou5.2)', async () => {
    const format = createFormat()

    // First, create a valid snapshot with data
    const result1 = await format.createSnapshot(null, makeSampleData(3))
    expect(result1.metadata.snapshots).toHaveLength(1)

    // Now create an empty snapshot on top - should not crash
    const result2 = await format.createSnapshot(result1.metadata, [])
    expect(result2).toBeDefined()
    expect(result2.metadata.snapshots).toHaveLength(2)
    expect(result2.metadata.snapshots[1].summary.addedRecords).toBe(0)
    expect(result2.metadata.currentSnapshotId).toBe(2)
  })

  it('11. snapshot summary has correct record counts', async () => {
    const format = createFormat()

    // Single record
    const result1 = await format.createSnapshot(null, makeSampleData(1))
    expect(result1.metadata.snapshots[0].summary.addedRecords).toBe(1)
    expect(result1.metadata.snapshots[0].summary.addedDataFiles).toBe(1)

    // Larger batch
    const result2 = await format.createSnapshot(result1.metadata, makeSampleData(100))
    expect(result2.metadata.snapshots[1].summary.addedRecords).toBe(100)
    expect(result2.metadata.snapshots[1].summary.addedDataFiles).toBe(1)
  })
})

// =============================================================================
// Path Tests
// =============================================================================

describe('IcebergFormat.getPaths', () => {
  it('12. returns correct file paths for a given version', () => {
    const format = createFormat('tables/users', 'users')
    const paths = format.getPaths(1)

    expect(paths.metadata).toBe('tables/users/metadata/v1.metadata.json')
    expect(paths.manifestList).toContain('tables/users/metadata/snap-')
    expect(paths.manifestList.endsWith('.json')).toBe(true)
    expect(paths.manifest).toContain('tables/users/metadata/')
    expect(paths.manifest.endsWith('-m0.json')).toBe(true)
    expect(paths.data).toContain('tables/users/data/')
    expect(paths.data.endsWith('.parquet')).toBe(true)
  })

  it('13. returns different metadata paths for different versions', () => {
    const format = createFormat('tables/orders', 'orders')

    const paths1 = format.getPaths(1)
    const paths2 = format.getPaths(2)
    const paths3 = format.getPaths(3)

    expect(paths1.metadata).toBe('tables/orders/metadata/v1.metadata.json')
    expect(paths2.metadata).toBe('tables/orders/metadata/v2.metadata.json')
    expect(paths3.metadata).toBe('tables/orders/metadata/v3.metadata.json')
  })
})

// =============================================================================
// Snapshot Log Tests
// =============================================================================

describe('IcebergFormat snapshot log', () => {
  it('14. multiple snapshots create proper snapshot log', async () => {
    const format = createFormat()

    const result1 = await format.createSnapshot(null, makeSampleData(1))
    const result2 = await format.createSnapshot(result1.metadata, makeSampleData(2))
    const result3 = await format.createSnapshot(result2.metadata, makeSampleData(3))

    // Snapshot log should have 3 entries
    expect(result3.metadata.snapshotLog).toHaveLength(3)

    // Each entry references its snapshot ID
    expect(result3.metadata.snapshotLog[0].snapshotId).toBe(1)
    expect(result3.metadata.snapshotLog[1].snapshotId).toBe(2)
    expect(result3.metadata.snapshotLog[2].snapshotId).toBe(3)

    // Timestamps are monotonically increasing
    for (let i = 1; i < result3.metadata.snapshotLog.length; i++) {
      expect(result3.metadata.snapshotLog[i].timestampMs).toBeGreaterThanOrEqual(
        result3.metadata.snapshotLog[i - 1].timestampMs,
      )
    }
  })

  it('15. lastSequenceNumber increments with each snapshot', async () => {
    const format = createFormat()

    const result1 = await format.createSnapshot(null, makeSampleData(1))
    expect(result1.metadata.lastSequenceNumber).toBe(1)

    const result2 = await format.createSnapshot(result1.metadata, makeSampleData(1))
    expect(result2.metadata.lastSequenceNumber).toBe(2)

    const result3 = await format.createSnapshot(result2.metadata, makeSampleData(1))
    expect(result3.metadata.lastSequenceNumber).toBe(3)
  })
})

// =============================================================================
// File Artifact Tests
// =============================================================================

describe('IcebergFormat file artifacts', () => {
  it('16. createSnapshot returns consistent paths across metadata and manifest', async () => {
    const format = createFormat('tables/events', 'events')
    const data = makeSampleData(5)

    const result = await format.createSnapshot(null, data)

    // The metadata path should point to the metadata directory
    expect(result.metadataPath).toContain('tables/events/metadata/v')
    expect(result.metadataPath.endsWith('.metadata.json')).toBe(true)

    // Manifest list path should be in metadata directory
    expect(result.manifestListPath).toContain('tables/events/metadata/snap-')
    expect(result.manifestListPath.endsWith('.json')).toBe(true)

    // Manifest path should be in metadata directory
    expect(result.manifestPath).toContain('tables/events/metadata/')
    expect(result.manifestPath.endsWith('-m0.json')).toBe(true)

    // Data path should be in data directory
    expect(result.dataPath).toContain('tables/events/data/')
    expect(result.dataPath.endsWith('.parquet')).toBe(true)

    // The snapshot's manifestList field should match the returned manifestListPath
    const snapshot = result.metadata.snapshots[0]
    expect(snapshot.manifestList).toBe(result.manifestListPath)

    // The manifest entry's filePath should match the returned dataPath
    expect(result.manifest[0].dataFile.filePath).toBe(result.dataPath)
  })
})
