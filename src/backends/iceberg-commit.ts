/**
 * Iceberg Commit Utilities
 *
 * Shared utilities for committing data to Iceberg tables.
 * Used by both IcebergBackend and the compaction workflow.
 *
 * These utilities handle the atomic commit protocol:
 * 1. Write data file
 * 2. Create manifest entry
 * 3. Write manifest
 * 4. Write manifest list
 * 5. Create snapshot
 * 6. Update metadata.json atomically via version-hint.text
 */

import {
  // Metadata operations
  readTableMetadata,
  MetadataWriter,
  getSnapshotById,
  // Partition operations
  createUnpartitionedSpec,
  // Manifest operations
  ManifestGenerator,
  ManifestListGenerator,
  // Snapshot building
  SnapshotBuilder,
  generateUUID,
  // Types
  type TableMetadata,
  type IcebergSchema,
  type Snapshot,
  type StorageBackend as IcebergStorageBackend,
} from '@dotdo/iceberg'

import {
  encodeManifestToAvro,
  encodeManifestListToAvro,
  decodeManifestListFromAvroOrJson,
} from './iceberg-avro'

import type { StorageBackend } from '../types/storage'
import { isETagMismatchError, isNotFoundError } from '../storage/errors'
import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for an Iceberg commit operation
 */
export interface IcebergCommitConfig {
  /** Storage backend for file I/O */
  storage: StorageBackend

  /** Location of the Iceberg table (e.g., warehouse/db/table) */
  tableLocation: string

  /** Maximum OCC retries (default: 10) */
  maxRetries?: number

  /** Base backoff in ms for OCC retries (default: 100) */
  baseBackoffMs?: number

  /** Max backoff in ms for OCC retries (default: 5000) */
  maxBackoffMs?: number
}

/**
 * Information about a data file to be added to an Iceberg table
 */
export interface DataFileInfo {
  /** Path to the Parquet file */
  path: string

  /** File size in bytes */
  sizeInBytes: number

  /** Number of records in the file */
  recordCount: number
}

/**
 * Result of an Iceberg commit operation
 */
export interface IcebergCommitResult {
  /** Whether the commit succeeded */
  success: boolean

  /** New snapshot ID (if successful) */
  snapshotId?: number

  /** New sequence number (if successful) */
  sequenceNumber?: number

  /** Path to the new metadata file (if successful) */
  metadataPath?: string

  /** Error message (if failed) */
  error?: string
}

// =============================================================================
// Default Entity Schema
// =============================================================================

/**
 * Create the default Iceberg schema for entity storage
 */
export function createDefaultEntitySchema(): IcebergSchema {
  return {
    'schema-id': 0,
    type: 'struct',
    fields: [
      { id: 1, name: '$id', type: 'string', required: true },
      { id: 2, name: '$type', type: 'string', required: true },
      { id: 3, name: 'name', type: 'string', required: true },
      { id: 4, name: 'createdAt', type: 'timestamptz', required: true },
      { id: 5, name: 'createdBy', type: 'string', required: true },
      { id: 6, name: 'updatedAt', type: 'timestamptz', required: true },
      { id: 7, name: 'updatedBy', type: 'string', required: true },
      { id: 8, name: 'deletedAt', type: 'timestamptz', required: false },
      { id: 9, name: 'deletedBy', type: 'string', required: false },
      { id: 10, name: 'version', type: 'int', required: true },
      { id: 11, name: '$data', type: 'binary', required: false }, // Variant blob
    ],
  }
}

// =============================================================================
// Storage Adapter
// =============================================================================

/**
 * Convert ParqueDB StorageBackend to Iceberg StorageBackend
 */
export function toIcebergStorage(storage: StorageBackend): IcebergStorageBackend {
  return {
    async get(key: string): Promise<Uint8Array | null> {
      try {
        return await storage.read(key)
      } catch (error) {
        // Iceberg storage interface expects null for missing files
        if (!isNotFoundError(error)) {
          throw error
        }
        return null
      }
    },
    async put(key: string, data: Uint8Array): Promise<void> {
      await storage.write(key, data)
    },
    async delete(key: string): Promise<void> {
      await storage.delete(key)
    },
    async list(prefix: string): Promise<string[]> {
      const result = await storage.list(prefix)
      return result.files
    },
    async exists(key: string): Promise<boolean> {
      return storage.exists(key)
    },
  }
}

// =============================================================================
// Iceberg Committer Class
// =============================================================================

/**
 * Handles atomic commits to an Iceberg table.
 *
 * This class provides methods for:
 * - Creating new tables
 * - Appending data files to existing tables
 * - Handling optimistic concurrency control (OCC) with retries
 *
 * @example
 * ```typescript
 * const committer = new IcebergCommitter({
 *   storage: r2Backend,
 *   tableLocation: 'warehouse/db/users',
 * })
 *
 * // Create table if it doesn't exist
 * await committer.ensureTable()
 *
 * // Commit a new data file
 * const result = await committer.commitDataFile({
 *   path: 'warehouse/db/users/data/compacted-123.parquet',
 *   sizeInBytes: 1024000,
 *   recordCount: 5000,
 * })
 * ```
 */
export class IcebergCommitter {
  private storage: StorageBackend
  private tableLocation: string
  private maxRetries: number
  private baseBackoffMs: number
  private maxBackoffMs: number

  // Counter for ensuring unique snapshot IDs within the same millisecond
  private snapshotIdCounter = 0
  private lastSnapshotIdMs = 0

  constructor(config: IcebergCommitConfig) {
    this.storage = config.storage
    this.tableLocation = config.tableLocation
    this.maxRetries = config.maxRetries ?? 10
    this.baseBackoffMs = config.baseBackoffMs ?? 100
    this.maxBackoffMs = config.maxBackoffMs ?? 5000
  }

  // ===========================================================================
  // Public Methods
  // ===========================================================================

  /**
   * Ensure the table exists, creating it if necessary
   */
  async ensureTable(): Promise<TableMetadata> {
    const metadata = await this.getTableMetadata()
    if (metadata) {
      return metadata
    }
    return this.createTable()
  }

  /**
   * Get the current table metadata (or null if table doesn't exist)
   */
  async getTableMetadata(): Promise<TableMetadata | null> {
    const icebergStorage = toIcebergStorage(this.storage)
    try {
      return await readTableMetadata(icebergStorage, this.tableLocation)
    } catch (error) {
      if (!isNotFoundError(error)) {
        throw error
      }
      return null
    }
  }

  /**
   * Commit one or more data files to the table atomically.
   *
   * Uses optimistic concurrency control with retries.
   * On conflict, re-reads the current metadata and retries.
   */
  async commitDataFiles(dataFiles: DataFileInfo[]): Promise<IcebergCommitResult> {
    if (dataFiles.length === 0) {
      return { success: true }
    }

    const versionHintPath = `${this.tableLocation}/metadata/version-hint.text`

    for (let attempt = 0; attempt < this.maxRetries; attempt++) {
      // Apply exponential backoff with jitter between retries
      if (attempt > 0) {
        await this.backoffDelay(attempt)
      }

      try {
        // Get current version hint ETag for OCC
        let expectedVersionHintEtag = await this.getVersionHintEtag(versionHintPath)

        // Ensure table exists
        let metadata = await this.getTableMetadata()
        if (!metadata) {
          metadata = await this.createTable()
          // Re-read the version hint ETag after table creation
          expectedVersionHintEtag = await this.getVersionHintEtag(versionHintPath)
        }

        // Prepare the commit (write manifests, build metadata)
        const commitData = await this.prepareCommit(metadata, dataFiles)

        // Try to commit atomically
        const committed = await this.tryCommit(
          versionHintPath,
          expectedVersionHintEtag,
          commitData.metadataPath
        )

        if (committed) {
          return {
            success: true,
            snapshotId: commitData.snapshotId,
            sequenceNumber: commitData.sequenceNumber,
            metadataPath: commitData.metadataPath,
          }
        }
        // Conflict detected, retry
        logger.debug(`OCC conflict on attempt ${attempt + 1}, retrying...`)
      } catch (error) {
        logger.error('Error during commit attempt', { attempt, error })
        throw error
      }
    }

    return {
      success: false,
      error: `Commit failed after ${this.maxRetries} retries due to concurrent modifications`,
    }
  }

  /**
   * Commit a single data file (convenience method)
   */
  async commitDataFile(dataFile: DataFileInfo): Promise<IcebergCommitResult> {
    return this.commitDataFiles([dataFile])
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  /**
   * Generate a unique snapshot ID
   */
  private generateSnapshotId(): number {
    const now = Date.now()
    if (now === this.lastSnapshotIdMs) {
      this.snapshotIdCounter++
    } else {
      this.lastSnapshotIdMs = now
      this.snapshotIdCounter = 0
    }
    return now * 1000 + this.snapshotIdCounter
  }

  /**
   * Create a new Iceberg table
   */
  private async createTable(): Promise<TableMetadata> {
    const icebergStorage = toIcebergStorage(this.storage)
    const writer = new MetadataWriter(icebergStorage)
    const result = await writer.writeNewTable({
      location: this.tableLocation,
      schema: createDefaultEntitySchema(),
      partitionSpec: createUnpartitionedSpec(),
      properties: {
        'parquedb.version': '1',
        'parquedb.type': 'entity',
      },
    })
    return result.metadata
  }

  /**
   * Get the ETag of version-hint.text for OCC
   */
  private async getVersionHintEtag(versionHintPath: string): Promise<string | null> {
    try {
      const stat = await this.storage.stat(versionHintPath)
      return stat?.etag ?? null
    } catch {
      return null
    }
  }

  /**
   * Prepare a commit by writing manifests and building new metadata
   */
  private async prepareCommit(
    metadata: TableMetadata,
    dataFiles: DataFileInfo[]
  ): Promise<{
    metadataPath: string
    snapshotId: number
    sequenceNumber: number
  }> {
    const sequenceNumber = (metadata['last-sequence-number'] ?? 0) + 1
    const snapshotId = this.generateSnapshotId()

    // Calculate totals
    let totalRecords = 0
    let totalSize = 0
    for (const df of dataFiles) {
      totalRecords += df.recordCount
      totalSize += df.sizeInBytes
    }

    // Step 1: Write manifest for all data files
    const manifestPath = await this.writeManifest(dataFiles, sequenceNumber, snapshotId)

    // Step 2: Write manifest list
    const manifestListPath = await this.writeManifestList(
      metadata,
      manifestPath,
      sequenceNumber,
      snapshotId,
      dataFiles.length
    )

    // Step 3: Build new snapshot
    const newSnapshot = this.buildSnapshot(
      metadata,
      sequenceNumber,
      snapshotId,
      manifestListPath,
      totalRecords,
      totalSize,
      dataFiles.length
    )

    // Step 4: Build new metadata
    const newMetadata = this.buildNewMetadata(metadata, sequenceNumber, snapshotId, newSnapshot)

    // Step 5: Write metadata file
    const metadataPath = await this.writeMetadataFile(sequenceNumber, newMetadata)

    return { metadataPath, snapshotId, sequenceNumber }
  }

  /**
   * Write manifest file and return its path
   */
  private async writeManifest(
    dataFiles: DataFileInfo[],
    sequenceNumber: number,
    snapshotId: number
  ): Promise<string> {
    const manifestGenerator = new ManifestGenerator({ sequenceNumber, snapshotId })

    for (const dataFile of dataFiles) {
      manifestGenerator.addDataFile({
        'file-path': dataFile.path,
        'file-format': 'parquet',
        'record-count': dataFile.recordCount,
        'file-size-in-bytes': dataFile.sizeInBytes,
        partition: {},
      })
    }

    const manifestResult = manifestGenerator.generate()
    const manifestId = generateUUID()
    const manifestPath = `${this.tableLocation}/metadata/${manifestId}-m0.avro`
    const manifestContent = encodeManifestToAvro(manifestResult.entries, sequenceNumber)
    await this.storage.write(manifestPath, manifestContent)

    return manifestPath
  }

  /**
   * Write manifest list including parent manifests and return its path
   */
  private async writeManifestList(
    metadata: TableMetadata,
    newManifestPath: string,
    sequenceNumber: number,
    snapshotId: number,
    addedFilesCount: number
  ): Promise<string> {
    const manifestListGenerator = new ManifestListGenerator({ snapshotId, sequenceNumber })

    // Get manifest content size for stats
    const manifestStat = await this.storage.stat(newManifestPath)
    const manifestSize = manifestStat?.size ?? 0

    // Add the new manifest
    manifestListGenerator.addManifestWithStats(
      newManifestPath,
      manifestSize,
      0, // partition spec ID
      {
        addedFiles: addedFilesCount,
        existingFiles: 0,
        deletedFiles: 0,
        addedRows: 0,
        existingRows: 0,
        deletedRows: 0,
      },
      false // not a delete manifest
    )

    // Include manifests from parent snapshot
    await this.addParentManifests(manifestListGenerator, metadata)

    const manifestListId = generateUUID()
    const manifestListPath = `${this.tableLocation}/metadata/snap-${snapshotId}-${manifestListId}.avro`
    const manifestListContent = encodeManifestListToAvro(manifestListGenerator.getManifests())
    await this.storage.write(manifestListPath, manifestListContent)

    return manifestListPath
  }

  /**
   * Add manifests from parent snapshot to manifest list generator
   */
  private async addParentManifests(
    manifestListGenerator: ManifestListGenerator,
    metadata: TableMetadata
  ): Promise<void> {
    const currentSnapshotId = metadata['current-snapshot-id']
    if (!currentSnapshotId) return

    const parentSnapshot = getSnapshotById(metadata, currentSnapshotId)
    if (!parentSnapshot?.['manifest-list']) return

    try {
      const parentManifestListData = await this.storage.read(parentSnapshot['manifest-list'])
      const parentManifests = decodeManifestListFromAvroOrJson(parentManifestListData)
      for (const manifest of parentManifests) {
        manifestListGenerator.addManifestWithStats(
          manifest['manifest-path'],
          manifest['manifest-length'],
          manifest['partition-spec-id'] ?? 0,
          {
            addedFiles: manifest['added-files-count'] ?? 0,
            existingFiles: manifest['existing-files-count'] ?? 0,
            deletedFiles: manifest['deleted-files-count'] ?? 0,
            addedRows: manifest['added-rows-count'] ?? 0,
            existingRows: manifest['existing-rows-count'] ?? 0,
            deletedRows: manifest['deleted-rows-count'] ?? 0,
          },
          manifest.content === 1 // is delete manifest
        )
      }
    } catch (error) {
      if (!isNotFoundError(error)) {
        throw error
      }
      // Parent manifest list not found is expected for first snapshot
    }
  }

  /**
   * Build a new snapshot with summary stats
   */
  private buildSnapshot(
    metadata: TableMetadata,
    sequenceNumber: number,
    snapshotId: number,
    manifestListPath: string,
    addedRecords: number,
    addedSize: number,
    addedFiles: number
  ): Snapshot {
    const currentSnapshotId = metadata['current-snapshot-id']
    const existingSnapshot = currentSnapshotId ? getSnapshotById(metadata, currentSnapshotId) : undefined
    const existingSummary = existingSnapshot?.summary as Record<string, string> | undefined
    const prevTotalRecords = existingSummary?.['total-records'] ? parseInt(existingSummary['total-records']) : 0
    const prevTotalSize = existingSummary?.['total-files-size'] ? parseInt(existingSummary['total-files-size']) : 0
    const prevTotalFiles = existingSummary?.['total-data-files'] ? parseInt(existingSummary['total-data-files']) : 0

    const snapshotBuilder = new SnapshotBuilder({
      sequenceNumber,
      snapshotId,
      parentSnapshotId: currentSnapshotId ?? undefined,
      manifestListPath,
      operation: 'append',
      schemaId: metadata['current-schema-id'],
    })

    snapshotBuilder.setSummary(
      addedFiles,
      0, // deleted files
      addedRecords,
      0, // deleted records
      addedSize,
      0, // removed size
      prevTotalRecords + addedRecords,
      prevTotalSize + addedSize,
      prevTotalFiles + addedFiles
    )

    return snapshotBuilder.build()
  }

  /**
   * Build new table metadata with the new snapshot
   */
  private buildNewMetadata(
    metadata: TableMetadata,
    sequenceNumber: number,
    snapshotId: number,
    newSnapshot: Snapshot
  ): TableMetadata {
    return {
      ...metadata,
      'last-sequence-number': sequenceNumber,
      'last-updated-ms': Date.now(),
      'current-snapshot-id': snapshotId,
      snapshots: [...metadata.snapshots, newSnapshot],
      'snapshot-log': [
        ...(metadata['snapshot-log'] ?? []),
        { 'timestamp-ms': Date.now(), 'snapshot-id': snapshotId },
      ],
    }
  }

  /**
   * Write metadata file and return its path
   */
  private async writeMetadataFile(
    sequenceNumber: number,
    metadata: TableMetadata
  ): Promise<string> {
    const metadataUuid = generateUUID()
    const metadataPath = `${this.tableLocation}/metadata/${sequenceNumber}-${metadataUuid}.metadata.json`
    const metadataJson = JSON.stringify(metadata, null, 2)
    await this.storage.write(metadataPath, new TextEncoder().encode(metadataJson))
    return metadataPath
  }

  /**
   * Try to commit by atomically updating version-hint.text
   * Returns true if commit succeeded, false if conflict detected
   */
  private async tryCommit(
    versionHintPath: string,
    expectedEtag: string | null,
    metadataPath: string
  ): Promise<boolean> {
    try {
      await this.storage.writeConditional(
        versionHintPath,
        new TextEncoder().encode(metadataPath),
        expectedEtag
      )
      return true
    } catch (error) {
      if (isETagMismatchError(error)) {
        // Conflict - another process updated the table
        // Orphaned files will be cleaned up by vacuum
        return false
      }
      throw error
    }
  }

  /**
   * Apply exponential backoff delay with jitter between retries.
   * This prevents thundering herd effects when multiple writers retry simultaneously.
   */
  private async backoffDelay(retryCount: number): Promise<void> {
    const jitter = Math.random() * this.baseBackoffMs
    const backoffMs = Math.min(
      this.baseBackoffMs * Math.pow(2, retryCount - 1) + jitter,
      this.maxBackoffMs
    )
    await this.sleep(backoffMs)
  }

  /**
   * Sleep for specified milliseconds
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an Iceberg committer for a table
 */
export function createIcebergCommitter(config: IcebergCommitConfig): IcebergCommitter {
  return new IcebergCommitter(config)
}

/**
 * Convenience function to commit data files to an Iceberg table.
 * Creates a committer, ensures the table exists, and commits the files.
 *
 * @example
 * ```typescript
 * const result = await commitToIcebergTable({
 *   storage: r2Backend,
 *   tableLocation: 'warehouse/db/users',
 *   dataFiles: [
 *     { path: 'data/file1.parquet', sizeInBytes: 1024000, recordCount: 5000 },
 *     { path: 'data/file2.parquet', sizeInBytes: 2048000, recordCount: 10000 },
 *   ],
 * })
 * ```
 */
export async function commitToIcebergTable(config: {
  storage: StorageBackend
  tableLocation: string
  dataFiles: DataFileInfo[]
  maxRetries?: number
  baseBackoffMs?: number
  maxBackoffMs?: number
}): Promise<IcebergCommitResult> {
  const committer = new IcebergCommitter({
    storage: config.storage,
    tableLocation: config.tableLocation,
    maxRetries: config.maxRetries,
    baseBackoffMs: config.baseBackoffMs,
    maxBackoffMs: config.maxBackoffMs,
  })

  // Ensure table exists
  await committer.ensureTable()

  // Commit the data files
  return committer.commitDataFiles(config.dataFiles)
}
