/**
 * Iceberg Table Format for ParqueDB MergeTree Engine
 *
 * Writes compacted data as Iceberg-compatible tables with:
 * - Table metadata (schema, partitions, snapshots)
 * - Manifest lists and manifests (JSON format for simplicity)
 * - Standard Parquet data files
 *
 * Layout:
 *   {basePath}/
 *     metadata/
 *       v1.metadata.json       - Table metadata (schema, partitioning, snapshots)
 *       snap-{id}-{uuid}.json  - Manifest list (list of manifests)
 *       {uuid}-m0.json         - Manifest (list of data files with stats)
 *     data/
 *       00000-0-{uuid}.parquet - Data file
 *
 * Uses JSON for manifest lists and manifests (instead of Avro) to keep
 * dependencies minimal. Data files are standard Parquet produced by
 * encodeDataToParquet.
 */

import { encodeDataToParquet } from '../parquet-encoders'

// =============================================================================
// Configuration
// =============================================================================

export interface IcebergConfig {
  /** Base path for the table (e.g., 'tables/users') */
  basePath: string
  /** Table name */
  tableName: string
  /** Row group size for Parquet files (default: 10000) */
  rowGroupSize?: number
}

// =============================================================================
// Iceberg Metadata Types
// =============================================================================

export interface IcebergSnapshot {
  snapshotId: number
  timestampMs: number
  manifestList: string // path to manifest list
  summary: {
    operation: 'append' | 'overwrite'
    addedDataFiles: number
    addedRecords: number
  }
}

export interface IcebergTableMetadata {
  formatVersion: 2
  tableUuid: string
  location: string
  lastSequenceNumber: number
  lastUpdatedMs: number
  lastColumnId: number
  schemas: IcebergSchema[]
  currentSchemaId: number
  partitionSpecs: IcebergPartitionSpec[]
  defaultSpecId: number
  lastPartitionId: number
  properties: Record<string, string>
  currentSnapshotId: number
  snapshots: IcebergSnapshot[]
  snapshotLog: Array<{ timestampMs: number; snapshotId: number }>
  metadataLog: Array<{ timestampMs: number; metadataFile: string }>
}

export interface IcebergSchema {
  schemaId: number
  type: 'struct'
  fields: IcebergField[]
}

export interface IcebergField {
  id: number
  name: string
  required: boolean
  type: string // 'string', 'long', 'double', 'int'
}

export interface IcebergPartitionSpec {
  specId: number
  fields: Array<{
    sourceId: number
    fieldId: number
    name: string
    transform: string
  }>
}

export interface IcebergManifestEntry {
  status: 1 | 2 // 1 = added, 2 = existing
  dataFile: {
    filePath: string
    fileFormat: 'PARQUET'
    recordCount: number
    fileSizeInBytes: number
    columnSizes: Record<number, number>
    valueCounts: Record<number, number>
    nullValueCounts: Record<number, number>
    lowerBounds: Record<number, string>
    upperBounds: Record<number, string>
  }
}

// =============================================================================
// UUID Generation
// =============================================================================

/** Generate a UUID, falling back to timestamp-based if crypto.randomUUID is unavailable */
function generateUuid(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  // Fallback: timestamp + random hex
  const ts = Date.now().toString(16).padStart(12, '0')
  const rand = Math.random().toString(16).slice(2, 14).padStart(12, '0')
  return `${ts.slice(0, 8)}-${ts.slice(8, 12)}-4${rand.slice(0, 3)}-${rand.slice(3, 7)}-${rand.slice(7, 12)}00000`.slice(0, 36)
}

// =============================================================================
// IcebergFormat
// =============================================================================

export class IcebergFormat {
  private config: IcebergConfig
  private metadataVersion = 0

  constructor(config: IcebergConfig) {
    this.config = config
  }

  /** Generate the default schema for MergeTree data tables */
  getDefaultSchema(): IcebergSchema {
    return {
      schemaId: 0,
      type: 'struct',
      fields: [
        { id: 1, name: '$id', required: true, type: 'string' },
        { id: 2, name: '$op', required: true, type: 'string' },
        { id: 3, name: '$v', required: true, type: 'int' },
        { id: 4, name: '$ts', required: true, type: 'double' },
        { id: 5, name: '$data', required: false, type: 'string' },
      ],
    }
  }

  /** Create initial table metadata (v1.metadata.json) */
  createTableMetadata(): IcebergTableMetadata {
    const now = Date.now()
    const schema = this.getDefaultSchema()

    this.metadataVersion = 1

    return {
      formatVersion: 2,
      tableUuid: generateUuid(),
      location: this.config.basePath,
      lastSequenceNumber: 0,
      lastUpdatedMs: now,
      lastColumnId: schema.fields.length,
      schemas: [schema],
      currentSchemaId: 0,
      partitionSpecs: [
        {
          specId: 0,
          fields: [], // unpartitioned
        },
      ],
      defaultSpecId: 0,
      lastPartitionId: 0,
      properties: {
        'engine': 'parquedb-mergetree',
        'table.name': this.config.tableName,
      },
      currentSnapshotId: -1,
      snapshots: [],
      snapshotLog: [],
      metadataLog: [],
    }
  }

  /**
   * Create a new snapshot from compacted data.
   * Returns all the files that need to be written (metadata, manifest list, manifest, data files).
   */
  async createSnapshot(
    existingMetadata: IcebergTableMetadata | null,
    data: Array<{ $id: string; $op: string; $v: number; $ts: number; [key: string]: unknown }>,
  ): Promise<{
    metadata: IcebergTableMetadata
    metadataPath: string
    manifestListPath: string
    manifestList: IcebergManifestEntry[]
    manifestPath: string
    manifest: IcebergManifestEntry[]
    dataPath: string
    dataBuffer: ArrayBuffer
  }> {
    const now = Date.now()
    const uuid = generateUuid()

    // Encode data to Parquet
    const dataBuffer = await encodeDataToParquet(data)

    // Determine version
    const metadata = existingMetadata ? { ...existingMetadata } : this.createTableMetadata()
    this.metadataVersion = existingMetadata
      ? (existingMetadata.metadataLog.length + existingMetadata.snapshots.length + 1)
      : 1

    const newVersion = this.metadataVersion + 1

    // Generate snapshot ID: increment from last, or start at 1
    const snapshotId = metadata.snapshots.length > 0
      ? Math.max(...metadata.snapshots.map((s) => s.snapshotId)) + 1
      : 1

    // Compute file paths
    const dataPath = `${this.config.basePath}/data/00000-0-${uuid}.parquet`
    const manifestPath = `${this.config.basePath}/metadata/${uuid}-m0.json`
    const manifestListPath = `${this.config.basePath}/metadata/snap-${snapshotId}-${uuid}.json`
    const metadataPath = `${this.config.basePath}/metadata/v${newVersion}.metadata.json`

    // Compute column-level stats from the sorted data
    const columnStats = this.computeColumnStats(data)

    // Create manifest entry for this data file
    const manifestEntry: IcebergManifestEntry = {
      status: 1, // added
      dataFile: {
        filePath: dataPath,
        fileFormat: 'PARQUET',
        recordCount: data.length,
        fileSizeInBytes: dataBuffer.byteLength,
        columnSizes: columnStats.columnSizes,
        valueCounts: columnStats.valueCounts,
        nullValueCounts: columnStats.nullValueCounts,
        lowerBounds: columnStats.lowerBounds,
        upperBounds: columnStats.upperBounds,
      },
    }

    const manifest: IcebergManifestEntry[] = [manifestEntry]
    const manifestList: IcebergManifestEntry[] = [manifestEntry]

    // Build snapshot
    const snapshot: IcebergSnapshot = {
      snapshotId,
      timestampMs: now,
      manifestList: manifestListPath,
      summary: {
        operation: 'append',
        addedDataFiles: 1,
        addedRecords: data.length,
      },
    }

    // Update metadata with new snapshot
    const previousMetadataPath = existingMetadata
      ? `${this.config.basePath}/metadata/v${newVersion - 1}.metadata.json`
      : undefined

    const updatedMetadata: IcebergTableMetadata = {
      ...metadata,
      lastSequenceNumber: metadata.lastSequenceNumber + 1,
      lastUpdatedMs: now,
      currentSnapshotId: snapshotId,
      snapshots: [...metadata.snapshots, snapshot],
      snapshotLog: [...metadata.snapshotLog, { timestampMs: now, snapshotId }],
      metadataLog: previousMetadataPath
        ? [...metadata.metadataLog, { timestampMs: now, metadataFile: previousMetadataPath }]
        : metadata.metadataLog,
    }

    return {
      metadata: updatedMetadata,
      metadataPath,
      manifestListPath,
      manifestList,
      manifestPath,
      manifest,
      dataPath,
      dataBuffer,
    }
  }

  /** Get all file paths for a given metadata version */
  getPaths(version: number): {
    metadata: string
    manifestList: string
    manifest: string
    data: string
  } {
    const uuid = '00000000-0000-0000-0000-000000000000'
    return {
      metadata: `${this.config.basePath}/metadata/v${version}.metadata.json`,
      manifestList: `${this.config.basePath}/metadata/snap-0-${uuid}.json`,
      manifest: `${this.config.basePath}/metadata/${uuid}-m0.json`,
      data: `${this.config.basePath}/data/00000-0-${uuid}.parquet`,
    }
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  /**
   * Compute column-level statistics for manifest entries.
   * Maps field IDs (1-5) to their stats.
   */
  private computeColumnStats(
    data: Array<{ $id: string; $op: string; $v: number; $ts: number; [key: string]: unknown }>,
  ): {
    columnSizes: Record<number, number>
    valueCounts: Record<number, number>
    nullValueCounts: Record<number, number>
    lowerBounds: Record<number, string>
    upperBounds: Record<number, string>
  } {
    const columnSizes: Record<number, number> = {}
    const valueCounts: Record<number, number> = {}
    const nullValueCounts: Record<number, number> = {}
    const lowerBounds: Record<number, string> = {}
    const upperBounds: Record<number, string> = {}

    if (data.length === 0) {
      return { columnSizes, valueCounts, nullValueCounts, lowerBounds, upperBounds }
    }

    // Field mapping: 1=$id, 2=$op, 3=$v, 4=$ts, 5=$data
    const fields: Array<{ id: number; name: string; extract: (row: Record<string, unknown>) => string | null }> = [
      { id: 1, name: '$id', extract: (r) => r.$id as string },
      { id: 2, name: '$op', extract: (r) => r.$op as string },
      { id: 3, name: '$v', extract: (r) => String(r.$v) },
      { id: 4, name: '$ts', extract: (r) => String(r.$ts) },
      {
        id: 5,
        name: '$data',
        extract: (r) => {
          const dataFields: Record<string, unknown> = {}
          for (const [key, value] of Object.entries(r)) {
            if (!['$id', '$op', '$v', '$ts'].includes(key)) {
              dataFields[key] = value
            }
          }
          const json = JSON.stringify(dataFields)
          return json === '{}' ? null : json
        },
      },
    ]

    for (const field of fields) {
      let minVal: string | null = null
      let maxVal: string | null = null
      let nullCount = 0
      let totalSize = 0

      for (const row of data) {
        const val = field.extract(row)
        if (val === null || val === undefined) {
          nullCount++
        } else {
          const strVal = String(val)
          totalSize += strVal.length
          if (minVal === null || strVal < minVal) minVal = strVal
          if (maxVal === null || strVal > maxVal) maxVal = strVal
        }
      }

      columnSizes[field.id] = totalSize
      valueCounts[field.id] = data.length
      nullValueCounts[field.id] = nullCount
      if (minVal !== null) lowerBounds[field.id] = minVal
      if (maxVal !== null) upperBounds[field.id] = maxVal
    }

    return { columnSizes, valueCounts, nullValueCounts, lowerBounds, upperBounds }
  }
}
