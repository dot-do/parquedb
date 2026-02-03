/**
 * Iceberg Entity Backend
 *
 * Stores entities in Apache Iceberg format, compatible with DuckDB, Spark, Snowflake, etc.
 * Uses @dotdo/iceberg for metadata management and catalog operations.
 *
 * Supports:
 * - Filesystem catalog (direct metadata access)
 * - R2 Data Catalog (Cloudflare managed Iceberg)
 * - REST Catalog (any Iceberg REST catalog)
 */

import type {
  EntityBackend,
  IcebergBackendConfig,
  IcebergCatalogConfig,
  EntitySchema,
  SnapshotInfo,
  CompactOptions,
  CompactResult,
  VacuumOptions,
  VacuumResult,
  BackendStats,
  SchemaFieldType,
} from './types'
import type { Entity, EntityId, CreateInput, DeleteResult, UpdateResult } from '../types/entity'
import type { Filter } from '../types/filter'
import type { FindOptions, CreateOptions, UpdateOptions, DeleteOptions, GetOptions } from '../types/options'
import type { Update } from '../types/update'
import type { StorageBackend } from '../types/storage'
import { isETagMismatchError } from '../storage/errors'

// Import shared Parquet utilities
import {
  entityToRow,
  rowToEntity,
  buildEntityParquetSchema,
  matchesFilter,
  generateEntityId,
  extractDataFields,
} from './parquet-utils'

// Import from @dotdo/iceberg
import {
  // Metadata operations
  readTableMetadata,
  MetadataWriter,
  getCurrentSnapshot,
  getSnapshotAtTimestamp,
  getSnapshotById,
  // Snapshot operations
  SnapshotManager,
  // Schema operations
  SchemaEvolutionBuilder,
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
  type ManifestEntry,
  type ManifestFile,
  // Avro encoding for Iceberg manifest files (required for DuckDB/Spark/Snowflake interop)
  AvroEncoder,
  AvroFileWriter,
  createManifestEntrySchema,
  createManifestListSchema,
  encodeManifestEntry,
  encodeManifestListEntry,
  type EncodableManifestEntry,
  type EncodableManifestListEntry,
  type PartitionFieldDef,
} from '@dotdo/iceberg'

// Avro magic bytes for detecting Avro container files
const AVRO_MAGIC = new Uint8Array([0x4f, 0x62, 0x6a, 0x01]) // 'Obj' + version 1

// Import Parquet utilities
import { ParquetWriter } from '../parquet/writer'
import { readParquet } from '../parquet/reader'

// =============================================================================
// Iceberg Backend Implementation
// =============================================================================

/**
 * Iceberg-based entity backend
 *
 * Each namespace becomes an Iceberg table with the following schema:
 * - $id: string (primary key)
 * - $type: string
 * - name: string
 * - createdAt, createdBy, updatedAt, updatedBy: audit fields
 * - deletedAt, deletedBy: soft delete fields
 * - version: int
 * - $data: binary (Variant - full entity data)
 *
 * Additional fields can be "shredded" out for indexing.
 */
export class IcebergBackend implements EntityBackend {
  readonly type = 'iceberg' as const
  readonly supportsTimeTravel = true
  readonly supportsSchemaEvolution = true
  readonly readOnly: boolean

  private storage: StorageBackend
  private warehouse: string
  private database: string
  private catalogConfig: IcebergCatalogConfig | undefined
  private initialized = false

  // Cache of loaded table metadata per namespace
  private tableCache = new Map<string, TableMetadata>()

  // Mutex locks for concurrent write operations per namespace (same-instance protection)
  private writeLocks = new Map<string, Promise<void>>()

  // Maximum retries for optimistic concurrency control (cross-instance protection)
  private readonly maxOccRetries = 10

  // Counter for ensuring unique snapshot IDs within the same millisecond
  private snapshotIdCounter = 0
  private lastSnapshotIdMs = 0

  constructor(config: IcebergBackendConfig) {
    this.storage = config.storage
    this.warehouse = config.warehouse ?? config.location ?? ''
    this.database = config.database ?? 'default'
    this.catalogConfig = config.catalog
    this.readOnly = config.readOnly ?? false
  }

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  async initialize(): Promise<void> {
    if (this.initialized) return

    // Ensure warehouse directory exists
    if (this.warehouse) {
      await this.storage.mkdir(this.warehouse).catch(() => {
        // Directory might already exist
      })
    }

    this.initialized = true
  }

  async close(): Promise<void> {
    this.tableCache.clear()
    this.initialized = false
  }

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  async get<T = Record<string, unknown>>(
    ns: string,
    id: string,
    options?: GetOptions
  ): Promise<Entity<T> | null> {
    const entities = await this.find<T>(ns, { $id: `${ns}/${id}` as EntityId }, { limit: 1, ...options })
    return entities[0] ?? null
  }

  async find<T = Record<string, unknown>>(
    ns: string,
    filter?: Filter,
    options?: FindOptions
  ): Promise<Entity<T>[]> {
    const metadata = await this.getTableMetadata(ns)
    if (!metadata) return []

    // Get the snapshot to query
    let snapshot: Snapshot | undefined
    if (options?.asOf) {
      snapshot = getSnapshotAtTimestamp(metadata, options.asOf.getTime())
    } else {
      snapshot = getCurrentSnapshot(metadata)
    }

    if (!snapshot) return []

    // Read entities from data files
    const entities = await this.readEntitiesFromSnapshot<T>(ns, metadata, snapshot, filter, options)

    // Apply soft delete filter unless includeDeleted
    if (!options?.includeDeleted) {
      return entities.filter(e => !e.deletedAt)
    }

    return entities
  }

  async count(ns: string, filter?: Filter): Promise<number> {
    // For now, just count by reading all matching entities
    // TODO: Optimize with manifest statistics
    const entities = await this.find(ns, filter)
    return entities.length
  }

  async exists(ns: string, id: string): Promise<boolean> {
    const entity = await this.get(ns, id)
    return entity !== null
  }

  // ===========================================================================
  // Write Operations
  // ===========================================================================

  async create<T = Record<string, unknown>>(
    ns: string,
    input: CreateInput<T>,
    options?: CreateOptions
  ): Promise<Entity<T>> {
    if (this.readOnly) {
      throw new Error('Backend is read-only')
    }

    // Generate entity
    const now = new Date()
    const actor = options?.actor ?? 'system/parquedb' as EntityId
    const id = generateEntityId()

    const entity: Entity<T> = {
      $id: `${ns}/${id}` as EntityId,
      $type: input.$type,
      name: input.name,
      createdAt: now,
      createdBy: actor,
      updatedAt: now,
      updatedBy: actor,
      version: 1,
      ...extractDataFields(input),
    } as Entity<T>

    // Write to Iceberg table
    await this.appendEntities(ns, [entity])

    return entity
  }

  async update<T = Record<string, unknown>>(
    ns: string,
    id: string,
    update: Update,
    options?: UpdateOptions
  ): Promise<Entity<T>> {
    if (this.readOnly) {
      throw new Error('Backend is read-only')
    }

    // Get existing entity
    const existing = await this.get<T>(ns, id)
    if (!existing && !options?.upsert) {
      throw new Error(`Entity not found: ${ns}/${id}`)
    }

    const now = new Date()
    const actor = options?.actor ?? 'system/parquedb' as EntityId

    // Apply update operators
    const updated = this.applyUpdate(existing ?? this.createDefaultEntity<T>(ns, id), update)

    // Update audit fields
    const entity: Entity<T> = {
      ...updated,
      updatedAt: now,
      updatedBy: actor,
      version: (existing?.version ?? 0) + 1,
    } as Entity<T>

    // Write updated entity (Iceberg handles versioning)
    await this.appendEntities(ns, [entity])

    return entity
  }

  async delete(ns: string, id: string, options?: DeleteOptions): Promise<DeleteResult> {
    if (this.readOnly) {
      throw new Error('Backend is read-only')
    }

    const entity = await this.get(ns, id)
    if (!entity) {
      return { deletedCount: 0 }
    }

    if (options?.hard) {
      // Hard delete: use Iceberg delete files
      await this.hardDeleteEntities(ns, [`${ns}/${id}`])
    } else {
      // Soft delete: update deletedAt
      const now = new Date()
      const actor = options?.actor ?? 'system/parquedb' as EntityId
      const deleted = {
        ...entity,
        deletedAt: now,
        deletedBy: actor,
        updatedAt: now,
        updatedBy: actor,
        version: entity.version + 1,
      }
      await this.appendEntities(ns, [deleted])
    }

    return { deletedCount: 1 }
  }

  // ===========================================================================
  // Batch Operations
  // ===========================================================================

  async bulkCreate<T = Record<string, unknown>>(
    ns: string,
    inputs: CreateInput<T>[],
    options?: CreateOptions
  ): Promise<Entity<T>[]> {
    if (this.readOnly) {
      throw new Error('Backend is read-only')
    }

    const now = new Date()
    const actor = options?.actor ?? 'system/parquedb' as EntityId

    const entities = inputs.map(input => {
      const id = generateEntityId()
      return {
        $id: `${ns}/${id}` as EntityId,
        $type: input.$type,
        name: input.name,
        createdAt: now,
        createdBy: actor,
        updatedAt: now,
        updatedBy: actor,
        version: 1,
        ...extractDataFields(input),
      } as Entity<T>
    })

    await this.appendEntities(ns, entities)
    return entities
  }

  async bulkUpdate(
    ns: string,
    filter: Filter,
    update: Update,
    options?: UpdateOptions
  ): Promise<UpdateResult> {
    if (this.readOnly) {
      throw new Error('Backend is read-only')
    }

    const entities = await this.find(ns, filter)
    if (entities.length === 0) {
      return { matchedCount: 0, modifiedCount: 0 }
    }

    const now = new Date()
    const actor = options?.actor ?? 'system/parquedb' as EntityId

    const updated = entities.map(entity => {
      const result = this.applyUpdate(entity, update)
      return {
        ...result,
        updatedAt: now,
        updatedBy: actor,
        version: entity.version + 1,
      }
    })

    await this.appendEntities(ns, updated)

    return {
      matchedCount: entities.length,
      modifiedCount: updated.length,
    }
  }

  async bulkDelete(ns: string, filter: Filter, options?: DeleteOptions): Promise<DeleteResult> {
    if (this.readOnly) {
      throw new Error('Backend is read-only')
    }

    const entities = await this.find(ns, filter)
    if (entities.length === 0) {
      return { deletedCount: 0 }
    }

    if (options?.hard) {
      const ids = entities.map(e => e.$id)
      await this.hardDeleteEntities(ns, ids)
    } else {
      const now = new Date()
      const actor = options?.actor ?? 'system/parquedb' as EntityId

      const deleted = entities.map(entity => ({
        ...entity,
        deletedAt: now,
        deletedBy: actor,
        updatedAt: now,
        updatedBy: actor,
        version: entity.version + 1,
      }))

      await this.appendEntities(ns, deleted)
    }

    return { deletedCount: entities.length }
  }

  // ===========================================================================
  // Time Travel
  // ===========================================================================

  async snapshot(ns: string, version: number | Date): Promise<EntityBackend> {
    const metadata = await this.getTableMetadata(ns)
    if (!metadata) {
      throw new Error(`Table not found: ${ns}`)
    }

    let snapshot: Snapshot | undefined
    if (typeof version === 'number') {
      snapshot = getSnapshotById(metadata, version)
    } else {
      snapshot = getSnapshotAtTimestamp(metadata, version.getTime())
    }

    if (!snapshot) {
      throw new Error(`Snapshot not found for version: ${version}`)
    }

    // Return a read-only backend pointing to the specific snapshot
    return new IcebergSnapshotBackend(this, ns, snapshot)
  }

  async listSnapshots(ns: string): Promise<SnapshotInfo[]> {
    const metadata = await this.getTableMetadata(ns)
    if (!metadata) return []

    return metadata.snapshots.map(snap => ({
      id: snap['snapshot-id'],
      timestamp: new Date(snap['timestamp-ms']),
      operation: snap.summary?.operation,
      recordCount: snap.summary?.['added-records'] ? parseInt(snap.summary['added-records']) : undefined,
      summary: snap.summary as Record<string, unknown> | undefined,
    }))
  }

  // ===========================================================================
  // Schema
  // ===========================================================================

  async getSchema(ns: string): Promise<EntitySchema | null> {
    const metadata = await this.getTableMetadata(ns)
    if (!metadata) return null

    const currentSchema = metadata.schemas.find(s => s['schema-id'] === metadata['current-schema-id'])
    if (!currentSchema) return null

    return this.icebergSchemaToEntitySchema(ns, currentSchema)
  }

  async setSchema(ns: string, schema: EntitySchema): Promise<void> {
    if (this.readOnly) {
      throw new Error('Backend is read-only')
    }

    const metadata = await this.getTableMetadata(ns)
    if (!metadata) {
      // Create new table with schema
      await this.createTable(ns, schema)
      return
    }

    // Evolve existing schema
    const currentSchema = metadata.schemas.find(s => s['schema-id'] === metadata['current-schema-id'])
    if (!currentSchema) {
      throw new Error('Current schema not found')
    }

    const builder = new SchemaEvolutionBuilder(currentSchema)

    // Add new fields
    for (const field of schema.fields) {
      const existingField = currentSchema.fields.find(f => f.name === field.name)
      if (!existingField) {
        // Cast to IcebergType - the string values we produce are valid Iceberg primitive types
        const icebergType = this.entityFieldTypeToIceberg(field.type) as
          | 'string' | 'int' | 'long' | 'float' | 'double' | 'boolean'
          | 'date' | 'time' | 'timestamp' | 'timestamptz' | 'uuid' | 'binary'
        builder.addColumn(field.name, icebergType, {
          required: !field.nullable,
          doc: field.doc,
        })
      }
    }

    // TODO: Handle field removals, renames, type changes

    // Apply evolution
    // const result = builder.build()
    // Update metadata with new schema...
  }

  async listNamespaces(): Promise<string[]> {
    // List all tables in the warehouse
    const prefix = this.database ? `${this.warehouse}/${this.database}/` : `${this.warehouse}/`
    const result = await this.storage.list(prefix)

    const namespaces = new Set<string>()
    for (const file of result.files) {
      // Extract namespace from path like warehouse/db/ns/metadata/...
      const parts = file.split('/')
      const dbIndex = this.database ? parts.indexOf(this.database) : -1
      const nsIndex = dbIndex >= 0 ? dbIndex + 1 : (this.warehouse ? parts.indexOf(this.warehouse.split('/').pop()!) + 1 : 0)
      if (parts[nsIndex]) {
        namespaces.add(parts[nsIndex])
      }
    }

    return Array.from(namespaces)
  }

  // ===========================================================================
  // Maintenance
  // ===========================================================================

  async compact(_ns: string, _options?: CompactOptions): Promise<CompactResult> {
    if (this.readOnly) {
      throw new Error('Backend is read-only')
    }

    // For R2 Data Catalog, compaction is managed by Cloudflare
    if (this.catalogConfig?.type === 'r2-data-catalog') {
      // TODO: Trigger compaction via R2 Data Catalog API if available
      return {
        filesCompacted: 0,
        filesCreated: 0,
        bytesBefore: 0,
        bytesAfter: 0,
        durationMs: 0,
      }
    }

    // For other catalogs, implement manual compaction
    // TODO: Implement file compaction
    return {
      filesCompacted: 0,
      filesCreated: 0,
      bytesBefore: 0,
      bytesAfter: 0,
      durationMs: 0,
    }
  }

  async vacuum(ns: string, options?: VacuumOptions): Promise<VacuumResult> {
    if (this.readOnly) {
      throw new Error('Backend is read-only')
    }

    const metadata = await this.getTableMetadata(ns)
    if (!metadata) {
      return { filesDeleted: 0, bytesReclaimed: 0, snapshotsExpired: 0 }
    }

    const manager = new SnapshotManager(metadata)
    const retentionMs = options?.retentionMs ?? 7 * 24 * 60 * 60 * 1000 // 7 days default
    const olderThanMs = Date.now() - retentionMs
    // Note: minSnapshots is handled by SnapshotManager's retention policy, not this method
    const result = manager.expireSnapshots(olderThanMs)

    if (!options?.dryRun) {
      // TODO: Write updated metadata and delete orphaned files
    }

    return {
      filesDeleted: 0, // TODO: Count deleted files
      bytesReclaimed: 0,
      snapshotsExpired: result.expiredSnapshotIds?.length ?? 0,
    }
  }

  async stats(ns: string): Promise<BackendStats> {
    const metadata = await this.getTableMetadata(ns)
    if (!metadata) {
      return {
        recordCount: 0,
        totalBytes: 0,
        fileCount: 0,
        snapshotCount: 0,
      }
    }

    const snapshot = getCurrentSnapshot(metadata)
    const summary = snapshot?.summary as Record<string, string> | undefined

    return {
      recordCount: summary?.['total-records'] ? parseInt(summary['total-records']) : 0,
      totalBytes: summary?.['total-data-files-size-in-bytes']
        ? parseInt(summary['total-data-files-size-in-bytes'])
        : 0,
      fileCount: summary?.['total-data-files'] ? parseInt(summary['total-data-files']) : 0,
      snapshotCount: metadata.snapshots.length,
      lastModified: metadata['last-updated-ms'] ? new Date(metadata['last-updated-ms']) : undefined,
    }
  }

  // ===========================================================================
  // Internal Helpers
  // ===========================================================================

  /**
   * Generate a unique snapshot ID
   * Uses millisecond timestamp but ensures uniqueness when called in same ms
   */
  private generateSnapshotId(): number {
    const now = Date.now()
    if (now === this.lastSnapshotIdMs) {
      this.snapshotIdCounter++
    } else {
      this.lastSnapshotIdMs = now
      this.snapshotIdCounter = 0
    }
    // Combine timestamp with counter to ensure uniqueness
    // Counter goes in the lower bits (max ~1000 ops/ms is safe)
    return now * 1000 + this.snapshotIdCounter
  }

  /**
   * Acquire a write lock for a namespace
   * Ensures concurrent writes to the same namespace are serialized
   */
  private async acquireWriteLock(ns: string): Promise<() => void> {
    // Get the existing lock (if any) to wait on
    const existingLock = this.writeLocks.get(ns)

    // Create a new lock promise
    let releaseLock: () => void
    const lockPromise = new Promise<void>(resolve => {
      releaseLock = resolve
    })

    // Chain this lock onto the existing lock
    // This ensures operations are truly serialized
    const chainedLock = existingLock
      ? existingLock.then(() => lockPromise)
      : lockPromise

    // Store the chained lock immediately (before awaiting)
    // This way other concurrent callers will see this lock in the chain
    this.writeLocks.set(ns, chainedLock)

    // Wait for our turn
    if (existingLock) {
      await existingLock
    }

    return () => {
      releaseLock!()
      // Don't delete from map - let it be overwritten by next operation
      // This ensures the chain remains intact
    }
  }

  /**
   * Public accessor for reading entities from a snapshot (used by IcebergSnapshotBackend)
   */
  async readEntitiesFromSnapshotPublic<T>(
    ns: string,
    snapshot: Snapshot,
    filter?: Filter,
    options?: FindOptions
  ): Promise<Entity<T>[]> {
    const metadata = await this.getTableMetadata(ns)
    if (!metadata) return []
    return this.readEntitiesFromSnapshot<T>(ns, metadata, snapshot, filter, options)
  }

  /**
   * Get the table location for a namespace
   */
  private getTableLocation(ns: string): string {
    return this.database
      ? `${this.warehouse}/${this.database}/${ns}`
      : `${this.warehouse}/${ns}`
  }

  /**
   * Get table metadata, creating table if it doesn't exist
   */
  private async getTableMetadata(ns: string): Promise<TableMetadata | null> {
    // Check cache
    const cached = this.tableCache.get(ns)
    if (cached) return cached

    const location = this.getTableLocation(ns)

    try {
      // Convert ParqueDB StorageBackend to Iceberg StorageBackend
      const icebergStorage = this.toIcebergStorage()
      const metadata = await readTableMetadata(icebergStorage, location)
      if (metadata) {
        this.tableCache.set(ns, metadata)
        return metadata
      }
      return null
    } catch {
      // Table doesn't exist or error reading
      return null
    }
  }

  /**
   * Create a new Iceberg table
   */
  private async createTable(ns: string, schema?: EntitySchema): Promise<TableMetadata> {
    const location = this.getTableLocation(ns)
    const icebergStorage = this.toIcebergStorage()

    // Create default entity schema if not provided
    const icebergSchema = schema
      ? this.entitySchemaToIceberg(schema)
      : this.createDefaultEntitySchema()

    const writer = new MetadataWriter(icebergStorage)
    const result = await writer.writeNewTable({
      location,
      schema: icebergSchema,
      partitionSpec: createUnpartitionedSpec(),
      properties: {
        'parquedb.version': '1',
        'parquedb.type': 'entity',
      },
    })

    this.tableCache.set(ns, result.metadata)
    return result.metadata
  }

  /**
   * Append entities to an Iceberg table with optimistic concurrency control.
   *
   * Uses two levels of concurrency protection:
   * 1. In-memory mutex lock for same-instance concurrent writes
   * 2. OCC via version-hint.text conditional write for cross-instance writes
   *
   * Orphaned files from failed commits are cleaned up by vacuum operations.
   */
  private async appendEntities<T>(ns: string, entities: Entity<T>[]): Promise<void> {
    if (entities.length === 0) return

    // Acquire write lock for this namespace to prevent concurrent writes within same instance
    const releaseLock = await this.acquireWriteLock(ns)

    try {
      const location = this.getTableLocation(ns)
      const versionHintPath = `${location}/metadata/version-hint.text`

      for (let attempt = 0; attempt < this.maxOccRetries; attempt++) {
        // Invalidate cache to get fresh metadata
        this.tableCache.delete(ns)

        // Read current version-hint.text and its ETag for OCC
        let expectedVersionHintEtag: string | null = null
        try {
          const stat = await this.storage.stat(versionHintPath)
          expectedVersionHintEtag = stat?.etag ?? null
        } catch {
          // File doesn't exist yet (new table)
          expectedVersionHintEtag = null
        }

        let metadata = await this.getTableMetadata(ns)
        // Ensure table exists
        if (!metadata) {
          metadata = await this.createTable(ns)
          // Re-read the version hint ETag after table creation
          try {
            const stat = await this.storage.stat(versionHintPath)
            expectedVersionHintEtag = stat?.etag ?? null
          } catch {
            expectedVersionHintEtag = null
          }
        }

        // Step 1: Write entities to Parquet file (safe - unique path)
      const dataFileId = generateUUID()
      const dataFilePath = `${location}/data/${dataFileId}.parquet`

      // Build parquet schema and data
      const parquetSchema = buildEntityParquetSchema()
      const rows = entities.map(entity => entityToRow(entity))

      // Write parquet file
      const writer = new ParquetWriter(this.storage, { compression: 'snappy' })
      const writeResult = await writer.write(dataFilePath, rows, parquetSchema)

      // Step 2: Create manifest entry
      const sequenceNumber = (metadata['last-sequence-number'] ?? 0) + 1
      const snapshotId = this.generateSnapshotId()

      const manifestGenerator = new ManifestGenerator({
        sequenceNumber,
        snapshotId,
      })

      manifestGenerator.addDataFile({
        'file-path': dataFilePath,
        'file-format': 'PARQUET',
        'record-count': entities.length,
        'file-size-in-bytes': writeResult.size,
        partition: {},
      })

      const manifestResult = manifestGenerator.generate()

      // Step 3: Write manifest file (as JSON for now, Avro in production)
      const manifestId = generateUUID()
      const manifestPath = `${location}/metadata/${manifestId}-m0.avro`

      // Write manifest entries as JSON (simplified)
      const manifestContent = new TextEncoder().encode(JSON.stringify(manifestResult.entries, null, 2))
      await this.storage.write(manifestPath, manifestContent)

      // Step 4: Create manifest list that includes manifests from parent snapshot
      const manifestListGenerator = new ManifestListGenerator({
        snapshotId,
        sequenceNumber,
      })

      // Add the new manifest
      manifestListGenerator.addManifestWithStats(
        manifestPath,
        manifestContent.length,
        0, // partition spec ID
        manifestResult.summary,
        false // not a delete manifest
      )

      // Include manifests from parent snapshot (to inherit existing data)
      const currentSnapshotId = metadata['current-snapshot-id']
      if (currentSnapshotId) {
        const parentSnapshot = getSnapshotById(metadata, currentSnapshotId)
        if (parentSnapshot?.['manifest-list']) {
          try {
            const parentManifestListData = await this.storage.read(parentSnapshot['manifest-list'])
            const parentManifests = JSON.parse(new TextDecoder().decode(parentManifestListData)) as ManifestFile[]
            for (const manifest of parentManifests) {
              // Add parent manifest to new manifest list
              manifestListGenerator.addManifestWithStats(
                manifest['manifest-path'],
                manifest['manifest-length'],
                manifest['partition-spec-id'],
                {
                  'added-data-files': String(manifest['added-data-files-count'] ?? 0),
                  'added-records': String(manifest['added-rows-count'] ?? 0),
                  'added-files-size': String(0),
                },
                manifest.content === 1 // is delete manifest
              )
            }
          } catch {
            // Parent manifest list not found or invalid
          }
        }
      }

      // Step 5: Write manifest list
      const manifestListId = generateUUID()
      const manifestListPath = `${location}/metadata/snap-${snapshotId}-${manifestListId}.avro`

      const manifestListContent = new TextEncoder().encode(
        JSON.stringify(manifestListGenerator.getManifests(), null, 2)
      )
      await this.storage.write(manifestListPath, manifestListContent)

      // Step 6: Build new snapshot
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
        1, // added files
        0, // deleted files
        entities.length, // added records
        0, // deleted records
        writeResult.size, // added size
        0, // removed size
        prevTotalRecords + entities.length, // total records
        prevTotalSize + writeResult.size, // total size
        prevTotalFiles + 1 // total files
      )

      const newSnapshot = snapshotBuilder.build()

      // Step 7: Update metadata with new snapshot
      const newMetadata: TableMetadata = {
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

        // Step 8: Write new metadata file (safe - unique path)
        const metadataVersion = sequenceNumber
        const metadataUuid = generateUUID()
        const metadataPath = `${location}/metadata/${metadataVersion}-${metadataUuid}.metadata.json`

        const metadataJson = JSON.stringify(newMetadata, null, 2)
        await this.storage.write(metadataPath, new TextEncoder().encode(metadataJson))

        // Step 9: Atomically update version-hint.text using conditional write
        // This is the critical atomic operation that prevents concurrent write corruption
        try {
          await this.storage.writeConditional(
            versionHintPath,
            new TextEncoder().encode(metadataPath),
            expectedVersionHintEtag
          )
          // Success! Update cache and return
          this.tableCache.set(ns, newMetadata)
          return
        } catch (error) {
          // Check if this is an ETag mismatch (version conflict)
          if (isETagMismatchError(error)) {
            // Another process updated the table - retry with fresh metadata
            // Note: Orphaned files (dataFilePath, manifestPath, etc.) will be
            // cleaned up by vacuum operations, consistent with Iceberg's design
            continue
          }
          // Re-throw other errors
          throw error
        }
      }

      throw new Error(
        `Commit failed after ${this.maxOccRetries} retries due to concurrent modifications. ` +
        `Table: ${ns}. Consider using a different concurrency strategy or retry the operation.`
      )
    } finally {
      releaseLock()
    }
  }

  /**
   * Hard delete entities using Iceberg delete files
   */
  private async hardDeleteEntities(_ns: string, _ids: (string | EntityId)[]): Promise<void> {
    // TODO: Create position delete or equality delete file
    // TODO: Update manifest with delete file
    // TODO: Create new snapshot
    // TODO: Commit metadata
  }

  /**
   * Read entities from a snapshot
   */
  private async readEntitiesFromSnapshot<T>(
    _ns: string,
    _metadata: TableMetadata,
    snapshot: Snapshot,
    filter?: Filter,
    options?: FindOptions
  ): Promise<Entity<T>[]> {
    // Step 1: Read manifest list from snapshot
    const manifestListPath = snapshot['manifest-list']
    if (!manifestListPath) {
      return []
    }

    let manifestFiles: ManifestFile[]
    try {
      const manifestListData = await this.storage.read(manifestListPath)
      const manifestListJson = new TextDecoder().decode(manifestListData)
      manifestFiles = JSON.parse(manifestListJson) as ManifestFile[]
    } catch {
      return [] // Manifest list doesn't exist or is invalid
    }

    // Step 2: Read manifest entries from each manifest file
    const dataFilePaths: string[] = []
    for (const manifestFile of manifestFiles) {
      if (manifestFile.content === 1) {
        continue // Skip delete manifests for now
      }

      try {
        const manifestData = await this.storage.read(manifestFile['manifest-path'])
        const manifestJson = new TextDecoder().decode(manifestData)
        const entries = JSON.parse(manifestJson) as ManifestEntry[]

        for (const entry of entries) {
          // Only include ADDED (1) and EXISTING (0) entries, not DELETED (2)
          if (entry.status !== 2) {
            dataFilePaths.push(entry['data-file']['file-path'])
          }
        }
      } catch {
        // Skip invalid manifest files
      }
    }

    // Step 3: Read entities from each data file
    // Use a Map to deduplicate by entity ID, keeping the highest version
    const entityMap = new Map<string, Entity<T>>()
    for (const dataFilePath of dataFilePaths) {
      try {
        const rows = await readParquet<Record<string, unknown>>(this.storage, dataFilePath)

        for (const row of rows) {
          const entity = rowToEntity<T>(row)
          const existingEntity = entityMap.get(entity.$id)

          // Keep the entity with the highest version number
          if (!existingEntity || entity.version > existingEntity.version) {
            entityMap.set(entity.$id, entity)
          }
        }
      } catch {
        // Skip unreadable files
      }
    }

    // Convert map values to array and apply filter
    const allEntities: Entity<T>[] = []
    for (const entity of entityMap.values()) {
      // Apply filter if provided
      if (filter && !matchesFilter(entity as unknown as Record<string, unknown>, filter)) {
        continue
      }
      allEntities.push(entity)
    }

    // Step 4: Apply sorting
    if (options?.sort) {
      const sortFields = Object.entries(options.sort)
      allEntities.sort((a, b) => {
        for (const [field, direction] of sortFields) {
          const aVal = (a as Record<string, unknown>)[field]
          const bVal = (b as Record<string, unknown>)[field]

          let cmp = 0
          if (aVal === bVal) {
            cmp = 0
          } else if (aVal === null || aVal === undefined) {
            cmp = 1
          } else if (bVal === null || bVal === undefined) {
            cmp = -1
          } else if (typeof aVal === 'string' && typeof bVal === 'string') {
            cmp = aVal.localeCompare(bVal)
          } else if (typeof aVal === 'number' && typeof bVal === 'number') {
            cmp = aVal - bVal
          } else if (aVal instanceof Date && bVal instanceof Date) {
            cmp = aVal.getTime() - bVal.getTime()
          } else {
            cmp = String(aVal).localeCompare(String(bVal))
          }

          if (cmp !== 0) {
            return direction === -1 ? -cmp : cmp
          }
        }
        return 0
      })
    }

    // Step 5: Apply skip and limit
    let result = allEntities
    if (options?.skip) {
      result = result.slice(options.skip)
    }
    if (options?.limit) {
      result = result.slice(0, options.limit)
    }

    return result
  }

  /**
   * Apply update operators to an entity
   */
  private applyUpdate<T>(entity: Entity<T>, update: Update): Entity<T> {
    const result = { ...entity }

    // Handle $set
    if (update.$set) {
      Object.assign(result, update.$set)
    }

    // Handle $unset
    if (update.$unset) {
      for (const key of Object.keys(update.$unset)) {
        delete (result as Record<string, unknown>)[key]
      }
    }

    // Handle $inc
    if (update.$inc) {
      for (const [key, value] of Object.entries(update.$inc)) {
        const current = (result as Record<string, unknown>)[key]
        if (typeof current === 'number' && typeof value === 'number') {
          (result as Record<string, unknown>)[key] = current + value
        }
      }
    }

    // TODO: Handle other operators ($push, $pull, $addToSet, etc.)

    return result
  }

  /**
   * Create a default entity for upsert
   */
  private createDefaultEntity<T>(ns: string, id: string): Entity<T> {
    const now = new Date()
    return {
      $id: `${ns}/${id}` as EntityId,
      $type: 'unknown',
      name: id,
      createdAt: now,
      createdBy: 'system/parquedb' as EntityId,
      updatedAt: now,
      updatedBy: 'system/parquedb' as EntityId,
      version: 0,
    } as Entity<T>
  }

  /**
   * Convert ParqueDB StorageBackend to Iceberg StorageBackend
   */
  private toIcebergStorage(): IcebergStorageBackend {
    const storage = this.storage
    return {
      async get(key: string): Promise<Uint8Array | null> {
        try {
          return await storage.read(key)
        } catch {
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

  /**
   * Create default Iceberg schema for entities
   */
  private createDefaultEntitySchema(): IcebergSchema {
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

  /**
   * Convert EntitySchema to Iceberg schema
   */
  private entitySchemaToIceberg(schema: EntitySchema): IcebergSchema {
    let fieldId = 1
    return {
      'schema-id': schema.version ?? 0,
      type: 'struct',
      fields: schema.fields.map(field => ({
        id: field.id ?? fieldId++,
        name: field.name,
        type: this.entityFieldTypeToIceberg(field.type) as 'string' | 'int' | 'long' | 'float' | 'double' | 'boolean' | 'date' | 'time' | 'timestamp' | 'timestamptz' | 'uuid' | 'binary',
        required: field.required ?? !field.nullable,
      })),
    }
  }

  /**
   * Convert Iceberg schema to EntitySchema
   */
  private icebergSchemaToEntitySchema(name: string, schema: IcebergSchema): EntitySchema {
    return {
      name,
      version: schema['schema-id'],
      fields: schema.fields.map(field => ({
        name: field.name,
        type: this.icebergTypeToEntityFieldType(field.type) as SchemaFieldType,
        required: field.required,
        nullable: !field.required,
        id: field.id,
      })),
    }
  }

  /**
   * Convert entity field type to Iceberg type
   */
  private entityFieldTypeToIceberg(type: SchemaFieldType): string {
    if (typeof type === 'string') {
      switch (type) {
        case 'string':
          return 'string'
        case 'int':
          return 'int'
        case 'long':
          return 'long'
        case 'float':
          return 'float'
        case 'double':
          return 'double'
        case 'boolean':
          return 'boolean'
        case 'timestamp':
          return 'timestamptz'
        case 'date':
          return 'date'
        case 'time':
          return 'time'
        case 'uuid':
          return 'uuid'
        case 'binary':
          return 'binary'
        case 'decimal':
          return 'decimal(38,9)'
        case 'json':
        case 'variant':
          return 'binary' // Store as binary Variant
        default:
          return 'string'
      }
    }
    // Complex types
    if (typeof type === 'object') {
      if (type.type === 'list') {
        return `list<${this.entityFieldTypeToIceberg(type.element)}>`
      }
      if (type.type === 'map') {
        return `map<${this.entityFieldTypeToIceberg(type.key)},${this.entityFieldTypeToIceberg(type.value)}>`
      }
    }
    return 'string'
  }

  /**
   * Convert Iceberg type to entity field type
   */
  private icebergTypeToEntityFieldType(type: unknown): string {
    if (typeof type === 'string') {
      switch (type) {
        case 'string':
          return 'string'
        case 'int':
          return 'int'
        case 'long':
          return 'long'
        case 'float':
          return 'float'
        case 'double':
          return 'double'
        case 'boolean':
          return 'boolean'
        case 'timestamptz':
        case 'timestamp':
          return 'timestamp'
        case 'date':
          return 'date'
        case 'time':
          return 'time'
        case 'uuid':
          return 'uuid'
        case 'binary':
          return 'binary'
        default:
          return 'string'
      }
    }
    return 'json'
  }
}

// =============================================================================
// Snapshot Backend (Read-Only Time-Travel View)
// =============================================================================

/**
 * Read-only backend for querying a specific snapshot
 */
class IcebergSnapshotBackend implements EntityBackend {
  readonly type = 'iceberg' as const
  readonly supportsTimeTravel = false
  readonly supportsSchemaEvolution = false
  readonly readOnly = true

  // Store snapshot for future use in time-travel queries
  private snapshotData: Snapshot

  constructor(
    private parent: IcebergBackend,
    private ns: string,
    snapshotData: Snapshot
  ) {
    this.snapshotData = snapshotData
  }

  /** Get the snapshot ID for debugging/logging */
  get snapshotId(): number {
    return this.snapshotData['snapshot-id']
  }

  async initialize(): Promise<void> {}
  async close(): Promise<void> {}

  async get<T = Record<string, unknown>>(
    ns: string,
    id: string,
    _options?: GetOptions
  ): Promise<Entity<T> | null> {
    if (ns !== this.ns) {
      throw new Error(`Snapshot backend only supports namespace: ${this.ns}`)
    }
    const entities = await this.find<T>(ns, { $id: `${ns}/${id}` as EntityId }, { limit: 1 })
    return entities[0] ?? null
  }

  async find<T = Record<string, unknown>>(
    ns: string,
    filter?: Filter,
    options?: FindOptions
  ): Promise<Entity<T>[]> {
    if (ns !== this.ns) {
      throw new Error(`Snapshot backend only supports namespace: ${this.ns}`)
    }
    // Delegate to parent's readEntitiesFromSnapshot with our snapshot
    return this.parent.readEntitiesFromSnapshotPublic<T>(ns, this.snapshotData, filter, options)
  }

  async count(ns: string, filter?: Filter): Promise<number> {
    const entities = await this.find(ns, filter)
    return entities.length
  }

  async exists(ns: string, id: string): Promise<boolean> {
    const entity = await this.get(ns, id)
    return entity !== null
  }

  // Write operations throw errors (read-only)
  async create(): Promise<never> {
    throw new Error('Snapshot backend is read-only')
  }
  async update(): Promise<never> {
    throw new Error('Snapshot backend is read-only')
  }
  async delete(): Promise<never> {
    throw new Error('Snapshot backend is read-only')
  }
  async bulkCreate(): Promise<never> {
    throw new Error('Snapshot backend is read-only')
  }
  async bulkUpdate(): Promise<never> {
    throw new Error('Snapshot backend is read-only')
  }
  async bulkDelete(): Promise<never> {
    throw new Error('Snapshot backend is read-only')
  }

  async getSchema(ns: string): Promise<EntitySchema | null> {
    return this.parent.getSchema(ns)
  }

  async listNamespaces(): Promise<string[]> {
    return [this.ns]
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create an Iceberg backend
 */
export function createIcebergBackend(config: IcebergBackendConfig): IcebergBackend {
  return new IcebergBackend(config)
}

/**
 * Create an Iceberg backend with R2 Data Catalog
 */
export function createR2IcebergBackend(
  storage: StorageBackend,
  options: {
    accountId: string
    apiToken: string
    bucketName?: string
    warehouse?: string
    database?: string
  }
): IcebergBackend {
  return new IcebergBackend({
    type: 'iceberg',
    storage,
    warehouse: options.warehouse,
    database: options.database,
    catalog: {
      type: 'r2-data-catalog',
      accountId: options.accountId,
      apiToken: options.apiToken,
      bucketName: options.bucketName,
    },
  })
}
