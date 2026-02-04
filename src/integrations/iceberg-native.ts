/**
 * Native Apache Iceberg Integration using @dotdo/iceberg
 *
 * This module provides direct integration with the @dotdo/iceberg library
 * for full Apache Iceberg specification compliance.
 *
 * Unlike the base iceberg.ts adapter which provides a simplified interface,
 * this module uses the actual Iceberg primitives for:
 * - Proper Avro manifest encoding
 * - Atomic commits with retry logic
 * - Schema evolution with field ID tracking
 * - Bloom filter generation
 * - Column statistics collection
 *
 * IMPORTANT: This module requires @dotdo/iceberg to be installed.
 * Install it with: pnpm add @dotdo/iceberg
 *
 * @module
 */

import type { StorageBackend } from '../types/storage'
import type { EventSegment } from '../events/types'
import { asDynamicModule, asBuilder } from '../types/cast'
import { logger } from '../utils/logger'

// =============================================================================
// Types (defined locally to avoid import errors when @dotdo/iceberg is not installed)
// =============================================================================

/**
 * Options for native Iceberg metadata management
 */
export interface NativeIcebergOptions {
  /** Table location (e.g., 's3://bucket/warehouse/db/table') */
  location: string
  /** Namespace (database name) */
  namespace?: string | undefined
  /** Custom schema definition */
  schema?: IcebergNativeSchema | undefined
  /** Partition spec */
  partitionSpec?: PartitionSpecDefinition | undefined
  /** Sort order */
  sortOrder?: SortOrderDefinition | undefined
  /** Table properties */
  properties?: Record<string, string> | undefined
  /** Enable bloom filters */
  enableBloomFilters?: boolean | undefined
  /** Commit retry options */
  commitRetry?: {
    maxRetries?: number | undefined
    baseDelayMs?: number | undefined
    maxDelayMs?: number | undefined
  } | undefined
}

/**
 * Schema definition for native Iceberg
 */
export interface IcebergNativeSchema {
  fields: Array<{
    name: string
    type: string
    required?: boolean | undefined
    doc?: string | undefined
  }>
}

/**
 * Partition spec definition
 */
export interface PartitionSpecDefinition {
  fields: Array<{
    sourceColumn: string
    transform: 'identity' | 'year' | 'month' | 'day' | 'hour' | 'bucket' | 'truncate'
    name?: string | undefined
    param?: number | undefined
  }>
}

/**
 * Sort order definition
 */
export interface SortOrderDefinition {
  fields: Array<{
    sourceColumn: string
    direction: 'asc' | 'desc'
    nullOrder: 'nulls-first' | 'nulls-last'
  }>
}

/**
 * Data file with statistics for native Iceberg
 */
export interface NativeDataFile {
  path: string
  format: 'parquet'
  recordCount: number
  fileSizeBytes: number
  partition?: Record<string, unknown> | undefined
  columnSizes?: Record<number, number> | undefined
  valueCounts?: Record<number, number> | undefined
  nullValueCounts?: Record<number, number> | undefined
  nanValueCounts?: Record<number, number> | undefined
  lowerBounds?: Record<number, Uint8Array> | undefined
  upperBounds?: Record<number, Uint8Array> | undefined
  keyMetadata?: Uint8Array | undefined
  splitOffsets?: number[] | undefined
  sortOrderId?: number | undefined
}

/**
 * Commit result from native Iceberg
 */
export interface NativeCommitResult {
  snapshotId: bigint
  sequenceNumber: number
  metadataPath: string
  manifestListPath: string
  success: boolean
  error?: string | undefined
  retryCount?: number | undefined
}

/**
 * Iceberg snapshot (simplified local type)
 */
export interface IcebergSnapshot {
  'snapshot-id': bigint | number | string
  'timestamp-ms': number
  'parent-snapshot-id'?: bigint | number | string | undefined
  'manifest-list': string
  summary?: Record<string, string> | undefined
}

/**
 * Iceberg schema (simplified local type)
 */
export interface IcebergSchemaLocal {
  'schema-id': number
  type: 'struct'
  fields: Array<{
    id: number
    name: string
    required: boolean
    type: string
    doc?: string | undefined
  }>
}

/**
 * Schema evolution operation types
 */
export type SchemaEvolutionOperationType =
  | 'add-column'
  | 'drop-column'
  | 'rename-column'
  | 'update-column-type'
  | 'make-column-optional'
  | 'make-column-required'

export interface SchemaEvolutionOp {
  type: SchemaEvolutionOperationType
  name: string
  newName?: string | undefined
  fieldType?: string | undefined
  newType?: string | undefined
}

// =============================================================================
// Dynamic Import Helper
// =============================================================================

/** Iceberg table metadata structure returned by readTableMetadata */
interface IcebergTableMetadata {
  'table-uuid': string
  'format-version': number
  'current-snapshot-id'?: number | bigint | undefined
  'last-sequence-number'?: number | undefined
  'current-schema-id'?: number | undefined
  schemas?: IcebergSchemaLocal[] | undefined
  snapshots?: IcebergSnapshot[] | undefined
  [key: string]: unknown
}

/** Options for creating a new Iceberg table */
interface CreateTableOptions {
  location: string
  schema: IcebergSchemaLocal
  partitionSpec: { 'spec-id': number; fields: unknown[] }
  properties: Record<string, string>
  dataFiles: unknown[]
}

/** Result of creating a new Iceberg table */
interface CreateTableResult {
  metadataPath: string
  [key: string]: unknown
}

/** Result of a commit operation */
interface CommitResult {
  metadata: IcebergTableMetadata
  metadataPath: string
  retryCount?: number | undefined
}

/** Options for commit retry */
interface CommitOptions {
  maxRetries?: number | undefined
}

interface IcebergLibrary {
  createTableWithSnapshot: (
    storage: NativeIcebergStorageAdapter,
    options: CreateTableOptions
  ) => Promise<CreateTableResult>
  readTableMetadata: (
    storage: NativeIcebergStorageAdapter,
    location: string
  ) => Promise<IcebergTableMetadata>
  getCurrentSnapshot: (metadata: IcebergTableMetadata) => IcebergSnapshot | null
  getSnapshotAtTimestamp: (metadata: IcebergTableMetadata, timestampMs: number) => IcebergSnapshot | null
  getSnapshotById: (metadata: IcebergTableMetadata, snapshotId: bigint) => IcebergSnapshot | null
  SnapshotBuilder: new (options: unknown) => unknown
  TableMetadataBuilder: new (options: unknown) => { build: () => unknown; addSnapshot: (s: unknown) => void; setSchemas: (s: unknown[]) => void; setCurrentSchemaId: (id: number) => void }
  ManifestGenerator: new (options: unknown) => { addDataFile: (f: unknown) => void }
  SchemaEvolutionBuilder: new (schema: unknown) => unknown
  SchemaEvolutionError: new (code: string, message: string) => Error
  SnapshotManager: new (storage: unknown, location: string) => { expireSnapshots: (opts: unknown) => Promise<{ expiredSnapshots: unknown[]; retainedSnapshots: unknown[] }> }
  FileStatsCollector: new (schema: unknown) => { finalize: () => unknown }
  BloomFilter: new (options: { numItems: number; fpp: number }) => { add: (value: unknown) => void }
  createAtomicCommitter: (
    storage: NativeIcebergStorageAdapter,
    location: string,
    options?: CommitOptions | undefined
  ) => { commit: (updateFn: (metadata: IcebergTableMetadata) => Promise<unknown>) => Promise<CommitResult> }
  commitWithCleanup: (
    storage: NativeIcebergStorageAdapter,
    location: string,
    updateFn: (currentMetadata: IcebergTableMetadata) => Promise<unknown>,
    options?: CommitOptions | undefined
  ) => Promise<CommitResult>
  isCommitConflictError: (error: unknown) => boolean
  createDefaultSchema: () => IcebergSchemaLocal
  createUnpartitionedSpec: () => { 'spec-id': number; fields: unknown[] }
  createPartitionSpecBuilder: () => { addField: (f: unknown) => void; build: () => unknown }
}

let icebergLib: IcebergLibrary | null = null

async function loadIcebergLib(): Promise<IcebergLibrary> {
  if (icebergLib) return icebergLib

  try {
    // Dynamic import - will fail if @dotdo/iceberg is not installed
    // Using Function constructor to avoid TypeScript static analysis
    const dynamicImport = new Function('specifier', 'return import(specifier)')
    const lib = await dynamicImport('@dotdo/iceberg')
    icebergLib = asDynamicModule<IcebergLibrary>(lib)
    return icebergLib
  } catch {
    // Intentionally ignored: dynamic import failure means the optional dependency is not installed
    throw new Error(
      '@dotdo/iceberg is not installed. Install it with: pnpm add @dotdo/iceberg\n' +
      'For basic Iceberg metadata support without this dependency, use IcebergMetadataManager instead.'
    )
  }
}

// =============================================================================
// Storage Adapter for @dotdo/iceberg
// =============================================================================

/**
 * Adapts ParqueDB StorageBackend to @dotdo/iceberg StorageBackend interface
 */
export class NativeIcebergStorageAdapter {
  constructor(private storage: StorageBackend) {}

  async read(path: string): Promise<Uint8Array> {
    return this.storage.read(path)
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    await this.storage.write(path, data)
  }

  async writeConditional(
    path: string,
    data: Uint8Array,
    expectedVersion: string | null
  ): Promise<{ etag: string }> {
    const result = await this.storage.writeConditional(path, data, expectedVersion)
    return { etag: result.etag }
  }

  async exists(path: string): Promise<boolean> {
    return this.storage.exists(path)
  }

  async list(prefix: string): Promise<string[]> {
    const result = await this.storage.list(prefix)
    return result.files
  }

  async delete(path: string): Promise<boolean> {
    return this.storage.delete(path)
  }

  async stat(path: string): Promise<{ size: number; etag?: string | undefined } | null> {
    const stat = await this.storage.stat(path)
    return stat ? { size: stat.size, etag: stat.etag } : null
  }
}

// =============================================================================
// Native Iceberg Metadata Manager
// =============================================================================

/**
 * Manages Iceberg metadata using @dotdo/iceberg library.
 *
 * This class provides full Apache Iceberg specification compliance by
 * leveraging the @dotdo/iceberg library for all metadata operations.
 */
export class NativeIcebergMetadataManager {
  private storageAdapter: NativeIcebergStorageAdapter
  private options: Required<NativeIcebergOptions>
  private initialized = false

  constructor(
    storage: StorageBackend,
    options: NativeIcebergOptions
  ) {
    this.storageAdapter = new NativeIcebergStorageAdapter(storage)
    this.options = {
      namespace: 'default',
      schema: { fields: [] },
      partitionSpec: { fields: [] },
      sortOrder: { fields: [] },
      properties: {},
      enableBloomFilters: false,
      commitRetry: {
        maxRetries: 4,
        baseDelayMs: 100,
        maxDelayMs: 10000,
      },
      ...options,
    } as Required<NativeIcebergOptions>
  }

  // ===========================================================================
  // Initialization
  // ===========================================================================

  /**
   * Initialize the Iceberg metadata manager with @dotdo/iceberg
   */
  async initialize(): Promise<void> {
    if (this.initialized) return

    const iceberg = await loadIcebergLib()

    // Check if table exists
    const metadataPath = `${this.options.location}/metadata`
    const versionHintPath = `${metadataPath}/version-hint.text`
    const exists = await this.storageAdapter.exists(versionHintPath)

    if (!exists) {
      // Create new table
      await this.createNewTable(iceberg)
    } else {
      // Load existing table
      await this.loadExistingTable(iceberg)
    }

    this.initialized = true
  }

  /**
   * Create a new Iceberg table
   */
  private async createNewTable(iceberg: IcebergLibrary): Promise<void> {
    const schema = this.buildSchema(iceberg)
    const partitionSpec = this.buildPartitionSpec(iceberg)

    // Use createTableWithSnapshot for atomic table creation
    const result = await iceberg.createTableWithSnapshot(
      this.storageAdapter,
      {
        location: this.options.location,
        schema,
        partitionSpec,
        properties: this.options.properties,
        dataFiles: [],
      }
    )

    logger.info(`Created Iceberg table at ${result.metadataPath}`)
  }

  /**
   * Load existing Iceberg table metadata
   */
  private async loadExistingTable(iceberg: IcebergLibrary): Promise<void> {
    const metadata = await iceberg.readTableMetadata(
      this.storageAdapter,
      this.options.location
    )

    logger.info(`Loaded Iceberg table with ${metadata.snapshots?.length || 0} snapshots`)
  }

  // ===========================================================================
  // Commit Operations
  // ===========================================================================

  /**
   * Commit new data files to the table
   *
   * This performs an atomic commit with conflict detection and retry logic.
   */
  async commitDataFiles(dataFiles: NativeDataFile[]): Promise<NativeCommitResult> {
    const iceberg = await loadIcebergLib()

    // Convert to Iceberg DataFile format
    const icebergDataFiles = dataFiles.map(f => ({
      'file-path': f.path,
      'file-format': 'PARQUET',
      'record-count': f.recordCount,
      'file-size-in-bytes': f.fileSizeBytes,
      partition: f.partition || {},
      'column-sizes': f.columnSizes,
      'value-counts': f.valueCounts,
      'null-value-counts': f.nullValueCounts,
      'nan-value-counts': f.nanValueCounts,
      'lower-bounds': f.lowerBounds,
      'upper-bounds': f.upperBounds,
      'key-metadata': f.keyMetadata,
      'split-offsets': f.splitOffsets,
      'sort-order-id': f.sortOrderId,
    }))

    try {
      const result = await iceberg.commitWithCleanup(
        this.storageAdapter,
        this.options.location,
        async (currentMetadata: { 'last-sequence-number'?: number | undefined; 'current-snapshot-id'?: number | bigint | undefined }) => {
          // Build new snapshot
          const snapshotBuilder = new iceberg.SnapshotBuilder({
            sequenceNumber: (currentMetadata['last-sequence-number'] || 0) + 1,
            snapshotId: BigInt(Date.now()),
            parentSnapshotId: currentMetadata['current-snapshot-id'] !== -1
              ? BigInt(currentMetadata['current-snapshot-id']!)
              : undefined,
            operation: 'append',
          })

          // Generate manifest
          const manifestGenerator = new iceberg.ManifestGenerator({
            sequenceNumber: (currentMetadata['last-sequence-number'] || 0) + 1,
            snapshotId: BigInt(Date.now()),
          })

          for (const file of icebergDataFiles) {
            manifestGenerator.addDataFile(file)
          }

          // Build new metadata
          const builder = new iceberg.TableMetadataBuilder({
            location: this.options.location,
            currentMetadata,
          })

          const snapshot = asBuilder<unknown>(snapshotBuilder).build()
          builder.addSnapshot(snapshot)

          return builder.build()
        },
        {
          maxRetries: this.options.commitRetry.maxRetries,
        }
      )

      return {
        snapshotId: BigInt(result.metadata['current-snapshot-id'] || 0),
        sequenceNumber: result.metadata['last-sequence-number'] || 0,
        metadataPath: result.metadataPath,
        manifestListPath: '', // Would be in the snapshot
        success: true,
        retryCount: result.retryCount,
      }
    } catch (error) {
      if (iceberg.isCommitConflictError(error)) {
        return {
          snapshotId: BigInt(0),
          sequenceNumber: 0,
          metadataPath: '',
          manifestListPath: '',
          success: false,
          error: `Commit conflict: ${(error as Error).message}`,
        }
      }
      throw error
    }
  }

  /**
   * Commit from ParqueDB event segments
   */
  async commitFromEventSegments(segments: EventSegment[]): Promise<NativeCommitResult> {
    const dataFiles: NativeDataFile[] = segments.map(seg => ({
      path: seg.path,
      format: 'parquet',
      recordCount: seg.count,
      fileSizeBytes: seg.sizeBytes,
    }))

    return this.commitDataFiles(dataFiles)
  }

  // ===========================================================================
  // Snapshot Operations
  // ===========================================================================

  /**
   * Get current snapshot
   */
  async getCurrentSnapshot(): Promise<IcebergSnapshot | null> {
    const iceberg = await loadIcebergLib()
    const metadata = await iceberg.readTableMetadata(
      this.storageAdapter,
      this.options.location
    )
    return iceberg.getCurrentSnapshot(metadata) as IcebergSnapshot | null
  }

  /**
   * Get snapshot at timestamp
   */
  async getSnapshotAtTimestamp(timestampMs: number): Promise<IcebergSnapshot | null> {
    const iceberg = await loadIcebergLib()
    const metadata = await iceberg.readTableMetadata(
      this.storageAdapter,
      this.options.location
    )
    return iceberg.getSnapshotAtTimestamp(metadata, timestampMs) as IcebergSnapshot | null
  }

  /**
   * Get snapshot by ID
   */
  async getSnapshotById(snapshotId: bigint): Promise<IcebergSnapshot | null> {
    const iceberg = await loadIcebergLib()
    const metadata = await iceberg.readTableMetadata(
      this.storageAdapter,
      this.options.location
    )
    return iceberg.getSnapshotById(metadata, snapshotId) as IcebergSnapshot | null
  }

  /**
   * List all snapshots
   */
  async listSnapshots(): Promise<IcebergSnapshot[]> {
    const iceberg = await loadIcebergLib()
    const metadata = await iceberg.readTableMetadata(
      this.storageAdapter,
      this.options.location
    )
    return (metadata.snapshots || []) as IcebergSnapshot[]
  }

  /**
   * Expire old snapshots
   */
  async expireSnapshots(options: {
    olderThanMs?: number | undefined
    retainLast?: number | undefined
  }): Promise<{ expired: number; retained: number }> {
    const iceberg = await loadIcebergLib()

    const manager = new iceberg.SnapshotManager(
      this.storageAdapter,
      this.options.location
    )

    const result = await manager.expireSnapshots({
      olderThanMs: options.olderThanMs,
      retainLast: options.retainLast,
    })

    return {
      expired: result.expiredSnapshots.length,
      retained: result.retainedSnapshots.length,
    }
  }

  // ===========================================================================
  // Schema Operations
  // ===========================================================================

  /**
   * Get current schema
   */
  async getCurrentSchema(): Promise<IcebergSchemaLocal> {
    const iceberg = await loadIcebergLib()
    const metadata = await iceberg.readTableMetadata(
      this.storageAdapter,
      this.options.location
    )

    const schemaId = metadata['current-schema-id'] || 0
    const schema = metadata.schemas?.find((s: { 'schema-id': number }) => s['schema-id'] === schemaId)

    if (!schema) {
      throw new Error(`Schema with id ${schemaId} not found`)
    }

    return schema as IcebergSchemaLocal
  }

  /**
   * Evolve schema
   */
  async evolveSchema(
    operations: SchemaEvolutionOp[]
  ): Promise<IcebergSchemaLocal> {
    const iceberg = await loadIcebergLib()
    const currentSchema = await this.getCurrentSchema()

    const builder = new iceberg.SchemaEvolutionBuilder(currentSchema) as {
      addColumn: (name: string, type: string, opts: unknown) => void
      dropColumn: (name: string) => void
      renameColumn: (name: string, newName: string) => void
      updateColumnType: (name: string, newType: string) => void
      makeColumnOptional: (name: string) => void
      makeColumnRequired: (name: string) => void
      build: () => { valid: boolean; errors: string[]; schema?: IcebergSchemaLocal | undefined }
    }

    for (const op of operations) {
      switch (op.type) {
        case 'add-column':
          builder.addColumn(op.name, op.fieldType!, op)
          break
        case 'drop-column':
          builder.dropColumn(op.name)
          break
        case 'rename-column':
          builder.renameColumn(op.name, op.newName!)
          break
        case 'update-column-type':
          builder.updateColumnType(op.name, op.newType!)
          break
        case 'make-column-optional':
          builder.makeColumnOptional(op.name)
          break
        case 'make-column-required':
          builder.makeColumnRequired(op.name)
          break
      }
    }

    const result = builder.build()

    if (!result.valid) {
      throw new iceberg.SchemaEvolutionError(
        'INVALID_OPERATION',
        `Schema evolution failed: ${result.errors.join(', ')}`
      )
    }

    // Commit the schema change
    await this.commitSchemaChange(result.schema!)

    return result.schema!
  }

  /**
   * Commit a schema change to the table
   */
  private async commitSchemaChange(schema: IcebergSchemaLocal): Promise<void> {
    const iceberg = await loadIcebergLib()

    await iceberg.commitWithCleanup(
      this.storageAdapter,
      this.options.location,
      async (currentMetadata: { schemas?: IcebergSchemaLocal[] | undefined }) => {
        const builder = new iceberg.TableMetadataBuilder({
          location: this.options.location,
          currentMetadata,
        })

        // Add the new schema
        const schemas = [...(currentMetadata.schemas || []), schema]
        builder.setSchemas(schemas)
        builder.setCurrentSchemaId(schema['schema-id'])

        return builder.build()
      }
    )
  }

  // ===========================================================================
  // Statistics & Bloom Filters
  // ===========================================================================

  /**
   * Generate column statistics for a data file
   */
  async generateStatistics(
    _dataFilePath: string
  ): Promise<unknown> {
    const iceberg = await loadIcebergLib()
    const schema = await this.getCurrentSchema()

    const collector = new iceberg.FileStatsCollector(schema)

    // Would read Parquet file and collect stats
    // For now, return empty stats
    return collector.finalize()
  }

  /**
   * Create bloom filter for a column
   */
  async createBloomFilter(
    _columnId: number,
    values: unknown[]
  ): Promise<{ add: (value: unknown) => void }> {
    const iceberg = await loadIcebergLib()

    const filter = new iceberg.BloomFilter({
      numItems: values.length,
      fpp: 0.01, // 1% false positive rate
    })

    for (const value of values) {
      if (value !== null && value !== undefined) {
        filter.add(value)
      }
    }

    return filter
  }

  // ===========================================================================
  // Export for Query Engines
  // ===========================================================================

  /**
   * Export table metadata for external query engines (DuckDB, Spark, Trino)
   */
  async exportForQueryEngine(): Promise<{
    metadataLocation: string
    tableUuid: string
    currentSnapshotId: bigint | null
    formatVersion: number
  }> {
    const iceberg = await loadIcebergLib()
    const metadata = await iceberg.readTableMetadata(
      this.storageAdapter,
      this.options.location
    )

    return {
      metadataLocation: `${this.options.location}/metadata`,
      tableUuid: metadata['table-uuid'],
      currentSnapshotId: metadata['current-snapshot-id'] !== -1
        ? BigInt(metadata['current-snapshot-id']!)
        : null,
      formatVersion: metadata['format-version'],
    }
  }

  // ===========================================================================
  // Internal Helpers
  // ===========================================================================

  private buildSchema(iceberg: IcebergLibrary): IcebergSchemaLocal {
    if (this.options.schema.fields.length === 0) {
      // Use default ParqueDB entity schema
      return iceberg.createDefaultSchema()
    }

    return {
      'schema-id': 0,
      type: 'struct',
      fields: this.options.schema.fields.map((f, i) => ({
        id: i + 1,
        name: f.name,
        required: f.required ?? false,
        type: f.type,
        doc: f.doc,
      })),
    }
  }

  private buildPartitionSpec(iceberg: IcebergLibrary): { 'spec-id': number; fields: unknown[] } {
    if (this.options.partitionSpec.fields.length === 0) {
      return iceberg.createUnpartitionedSpec()
    }

    // Use partition spec builder
    const builder = iceberg.createPartitionSpecBuilder() as {
      addField: (f: unknown) => void
      build: () => { 'spec-id': number; fields: unknown[] }
    }

    for (const field of this.options.partitionSpec.fields) {
      builder.addField({
        sourceColumn: field.sourceColumn,
        transform: field.transform,
        name: field.name || field.sourceColumn,
        param: field.param,
      })
    }

    return builder.build()
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a native Iceberg metadata manager
 */
export function createNativeIcebergManager(
  storage: StorageBackend,
  options: NativeIcebergOptions
): NativeIcebergMetadataManager {
  return new NativeIcebergMetadataManager(storage, options)
}

/**
 * Enable native Iceberg metadata for a ParqueDB collection
 */
export async function enableNativeIcebergMetadata(
  storage: StorageBackend,
  namespace: string,
  options: Omit<NativeIcebergOptions, 'namespace'>
): Promise<NativeIcebergMetadataManager> {
  const manager = new NativeIcebergMetadataManager(storage, {
    ...options,
    namespace,
  })

  await manager.initialize()

  return manager
}
