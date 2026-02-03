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

import {
  ReadOnlyError,
  BackendEntityNotFoundError,
  TableNotFoundError,
  SnapshotNotFoundError,
  InvalidNamespaceError,
  SchemaNotFoundError,
  CommitConflictError,
  WriteLockTimeoutError,
  type EntityBackend,
  type IcebergBackendConfig,
  type IcebergCatalogConfig,
  type EntitySchema,
  type SnapshotInfo,
  type CompactOptions,
  type CompactResult,
  type VacuumOptions,
  type VacuumResult,
  type BackendStats,
  type SchemaFieldType,
} from './types'
import type { Entity, EntityId, CreateInput, DeleteResult, UpdateResult } from '../types/entity'
import type { Filter } from '../types/filter'
import type { FindOptions, CreateOptions, UpdateOptions, DeleteOptions, GetOptions } from '../types/options'
import type { Update } from '../types/update'
import type { StorageBackend } from '../types/storage'
import { isETagMismatchError, isNotFoundError } from '../storage/errors'
import { entityAsRecord } from '../types/cast'

// Import shared Parquet utilities
import {
  entityToRow,
  rowToEntity,
  buildEntityParquetSchema,
  matchesFilter,
  generateEntityId,
  extractDataFields,
} from './parquet-utils'

// Import update operators
import { applyOperators } from '../mutation/operators'

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
  // Delete operations
  EqualityDeleteBuilder,
  DeleteManifestGenerator,
  parseEqualityDeleteFile,
  CONTENT_EQUALITY_DELETES,
  MANIFEST_CONTENT_DELETES,
  // Types
  type TableMetadata,
  type IcebergSchema,
  type Snapshot,
  type StorageBackend as IcebergStorageBackend,
  type ManifestEntry as _ManifestEntry,
  type ManifestFile,
} from '@dotdo/iceberg'

// Import Avro encoding/decoding for Iceberg manifest files (required for DuckDB/Spark/Snowflake interop)
import {
  encodeManifestToAvro,
  encodeManifestListToAvro,
  decodeManifestListFromAvroOrJson,
  decodeManifestFromAvroOrJson,
} from './iceberg-avro'

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

  // Timeout for acquiring write locks (default: 30 seconds)
  private readonly writeLockTimeoutMs: number

  // Counter for ensuring unique snapshot IDs within the same millisecond
  private snapshotIdCounter = 0
  private lastSnapshotIdMs = 0

  constructor(config: IcebergBackendConfig) {
    this.storage = config.storage
    this.warehouse = config.warehouse ?? config.location ?? ''
    this.database = config.database ?? 'default'
    this.catalogConfig = config.catalog
    this.readOnly = config.readOnly ?? false
    this.writeLockTimeoutMs = config.writeLockTimeoutMs ?? 30000
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
    // Wait for all pending write operations to complete
    const pendingLocks = Array.from(this.writeLocks.values())
    await Promise.all(pendingLocks)
    this.writeLocks.clear()
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
    // Count by reading all matching entities.
    // Performance note: Could be optimized by using manifest statistics
    // for unfiltered counts, avoiding full data scan.
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
      throw new ReadOnlyError('create', 'IcebergBackend')
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
      throw new ReadOnlyError('update', 'IcebergBackend')
    }

    // Get existing entity
    const existing = await this.get<T>(ns, id)
    if (!existing && !options?.upsert) {
      throw new BackendEntityNotFoundError(ns, id)
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
      throw new ReadOnlyError('delete', 'IcebergBackend')
    }

    // For hard delete, we need to include soft-deleted entities
    // For soft delete, we only look at non-deleted entities
    const includeDeleted = options?.hard ?? false
    const entity = await this.get(ns, id, { includeDeleted })
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
      throw new ReadOnlyError('bulkCreate', 'IcebergBackend')
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
      throw new ReadOnlyError('bulkUpdate', 'IcebergBackend')
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
      throw new ReadOnlyError('bulkDelete', 'IcebergBackend')
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
      throw new TableNotFoundError(ns, 'IcebergBackend')
    }

    let snapshot: Snapshot | undefined
    if (typeof version === 'number') {
      snapshot = getSnapshotById(metadata, version)
    } else {
      snapshot = getSnapshotAtTimestamp(metadata, version.getTime())
    }

    if (!snapshot) {
      throw new SnapshotNotFoundError(ns, version)
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
      throw new ReadOnlyError('setSchema', 'IcebergBackend')
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
      throw new SchemaNotFoundError(ns, `Current schema not found for table: ${ns}`)
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

    // Note: Field removals, renames, and type changes are more complex operations
    // that require careful migration of existing data. For now, only additions are supported.

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
      throw new ReadOnlyError('compact', 'IcebergBackend')
    }

    // For R2 Data Catalog, compaction is managed by Cloudflare automatically.
    // Manual triggering would require R2 Data Catalog API support.
    if (this.catalogConfig?.type === 'r2-data-catalog') {
      return {
        filesCompacted: 0,
        filesCreated: 0,
        bytesBefore: 0,
        bytesAfter: 0,
        durationMs: 0,
      }
    }

    // Manual compaction would involve:
    // 1. Reading multiple small data files
    // 2. Merging them into fewer larger files
    // 3. Updating the Iceberg metadata
    // For now, return no-op result. Use vacuum() for cleanup.
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
      throw new ReadOnlyError('vacuum', 'IcebergBackend')
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
      // Full vacuum implementation would:
      // 1. Write updated metadata with expired snapshots removed
      // 2. Identify orphaned data files no longer referenced
      // 3. Delete orphaned files from storage
      // For now, only snapshot expiration tracking is implemented
    }

    return {
      filesDeleted: 0, // File deletion not yet implemented
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
   *
   * Includes timeout protection to prevent indefinite blocking if an earlier
   * operation in the chain fails or hangs. On timeout, the stale lock is cleared
   * to allow subsequent operations to proceed.
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

    // Wait for our turn with timeout protection
    if (existingLock) {
      const timeoutPromise = new Promise<never>((_, reject) => {
        setTimeout(() => {
          reject(new WriteLockTimeoutError(ns, this.writeLockTimeoutMs))
        }, this.writeLockTimeoutMs)
      })

      try {
        await Promise.race([existingLock, timeoutPromise])
      } catch (error) {
        // On timeout, clean up the stale lock to unblock subsequent operations
        // We clear it only if our chainedLock is still the current one
        // (another operation may have already replaced it)
        if (this.writeLocks.get(ns) === chainedLock) {
          this.writeLocks.delete(ns)
        }
        // Also release our own lock promise so any waiters can proceed
        releaseLock!()
        throw error
      }
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
    } catch (error) {
      // Table doesn't exist yet - return null to indicate no metadata
      // Propagate other errors (permission, network, corruption)
      if (!isNotFoundError(error)) {
        throw error
      }
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

        // Get current version hint ETag for OCC
        let expectedVersionHintEtag = await this.getVersionHintEtag(versionHintPath)

        // Ensure table exists
        let metadata = await this.getTableMetadata(ns)
        if (!metadata) {
          metadata = await this.createTable(ns)
          // Re-read the version hint ETag after table creation
          expectedVersionHintEtag = await this.getVersionHintEtag(versionHintPath)
        }

        // Write data file, manifests, and build new snapshot
        const commitResult = await this.prepareCommit(location, metadata, entities)

        // Try to commit atomically
        const committed = await this.tryCommit(
          ns,
          versionHintPath,
          expectedVersionHintEtag,
          commitResult.newMetadata,
          commitResult.metadataPath
        )

        if (committed) {
          return
        }
        // Conflict detected, retry with fresh metadata
      }

      throw new CommitConflictError(
        `Commit failed after ${this.maxOccRetries} retries due to concurrent modifications. ` +
        `Consider using a different concurrency strategy or retry the operation.`,
        ns,
        /* version */ -1, // Unknown version after exhausted retries
        this.maxOccRetries
      )
    } finally {
      releaseLock()
    }
  }

  /**
   * Get the ETag of version-hint.text for OCC
   */
  private async getVersionHintEtag(versionHintPath: string): Promise<string | null> {
    try {
      const stat = await this.storage.stat(versionHintPath)
      return stat?.etag ?? null
    } catch {
      // File doesn't exist yet (new table)
      return null
    }
  }

  /**
   * Prepare a commit by writing data file, manifests, and building metadata
   */
  private async prepareCommit<T>(
    location: string,
    metadata: TableMetadata,
    entities: Entity<T>[]
  ): Promise<{
    newMetadata: TableMetadata
    metadataPath: string
  }> {
    // Step 1: Write entities to Parquet file
    const dataFileId = generateUUID()
    const dataFilePath = `${location}/data/${dataFileId}.parquet`
    const parquetSchema = buildEntityParquetSchema()
    const rows = entities.map(entity => entityToRow(entity))
    const writer = new ParquetWriter(this.storage, { compression: 'snappy' })
    const writeResult = await writer.write(dataFilePath, rows, parquetSchema)

    // Step 2: Create and write manifest
    const sequenceNumber = (metadata['last-sequence-number'] ?? 0) + 1
    const snapshotId = this.generateSnapshotId()
    const manifestPath = await this.writeManifest(location, dataFilePath, writeResult.size, entities.length, sequenceNumber, snapshotId)

    // Step 3: Create and write manifest list (including parent manifests)
    const manifestListPath = await this.writeManifestList(location, metadata, manifestPath, sequenceNumber, snapshotId)

    // Step 4: Build new snapshot
    const newSnapshot = this.buildSnapshot(metadata, sequenceNumber, snapshotId, manifestListPath, entities.length, writeResult.size)

    // Step 5: Build new metadata
    const newMetadata = this.buildNewMetadata(metadata, sequenceNumber, snapshotId, newSnapshot)

    // Step 6: Write metadata file
    const metadataPath = await this.writeMetadataFile(location, sequenceNumber, newMetadata)

    return { newMetadata, metadataPath }
  }

  /**
   * Write manifest file and return its path
   */
  private async writeManifest(
    location: string,
    dataFilePath: string,
    fileSize: number,
    recordCount: number,
    sequenceNumber: number,
    snapshotId: number
  ): Promise<string> {
    const manifestGenerator = new ManifestGenerator({ sequenceNumber, snapshotId })
    manifestGenerator.addDataFile({
      'file-path': dataFilePath,
      'file-format': 'parquet',
      'record-count': recordCount,
      'file-size-in-bytes': fileSize,
      partition: {},
    })
    const manifestResult = manifestGenerator.generate()

    const manifestId = generateUUID()
    const manifestPath = `${location}/metadata/${manifestId}-m0.avro`
    const manifestContent = encodeManifestToAvro(manifestResult.entries, sequenceNumber)
    await this.storage.write(manifestPath, manifestContent)

    return manifestPath
  }

  /**
   * Write manifest list including parent manifests and return its path
   */
  private async writeManifestList(
    location: string,
    metadata: TableMetadata,
    newManifestPath: string,
    sequenceNumber: number,
    snapshotId: number
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
      { addedFiles: 1, existingFiles: 0, deletedFiles: 0, addedRows: 0, existingRows: 0, deletedRows: 0 },
      false // not a delete manifest
    )

    // Include manifests from parent snapshot
    await this.addParentManifests(manifestListGenerator, metadata)

    const manifestListId = generateUUID()
    const manifestListPath = `${location}/metadata/snap-${snapshotId}-${manifestListId}.avro`
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
      // Parent manifest list not found is expected (first snapshot)
      // Propagate other errors (permission, network, corruption)
      if (!isNotFoundError(error)) {
        throw error
      }
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
    addedSize: number
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
      1, // added files
      0, // deleted files
      addedRecords,
      0, // deleted records
      addedSize,
      0, // removed size
      prevTotalRecords + addedRecords,
      prevTotalSize + addedSize,
      prevTotalFiles + 1
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
    location: string,
    sequenceNumber: number,
    metadata: TableMetadata
  ): Promise<string> {
    const metadataUuid = generateUUID()
    const metadataPath = `${location}/metadata/${sequenceNumber}-${metadataUuid}.metadata.json`
    const metadataJson = JSON.stringify(metadata, null, 2)
    await this.storage.write(metadataPath, new TextEncoder().encode(metadataJson))
    return metadataPath
  }

  /**
   * Try to commit by atomically updating version-hint.text
   * Returns true if commit succeeded, false if conflict detected
   */
  private async tryCommit(
    ns: string,
    versionHintPath: string,
    expectedEtag: string | null,
    newMetadata: TableMetadata,
    metadataPath: string
  ): Promise<boolean> {
    try {
      await this.storage.writeConditional(
        versionHintPath,
        new TextEncoder().encode(metadataPath),
        expectedEtag
      )
      // Success! Update cache
      this.tableCache.set(ns, newMetadata)
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
   * Hard delete entities using Iceberg equality delete files.
   *
   * Creates an equality delete file that marks entities for deletion by $id.
   * The delete file is tracked in a delete manifest within a new snapshot.
   */
  private async hardDeleteEntities(ns: string, ids: (string | EntityId)[]): Promise<void> {
    if (ids.length === 0) return

    // Acquire write lock for this namespace to prevent concurrent writes within same instance
    const releaseLock = await this.acquireWriteLock(ns)

    try {
      const location = this.getTableLocation(ns)
      const versionHintPath = `${location}/metadata/version-hint.text`

      for (let attempt = 0; attempt < this.maxOccRetries; attempt++) {
        // Invalidate cache to get fresh metadata
        this.tableCache.delete(ns)

        // Get current version hint ETag for OCC
        let expectedVersionHintEtag = await this.getVersionHintEtag(versionHintPath)

        // Ensure table exists
        let metadata = await this.getTableMetadata(ns)
        if (!metadata) {
          // No table means nothing to delete
          return
        }

        // Re-read the version hint ETag after getting metadata
        expectedVersionHintEtag = await this.getVersionHintEtag(versionHintPath)

        // Prepare delete commit
        const commitResult = await this.prepareDeleteCommit(location, metadata, ids)

        // Try to commit atomically
        const committed = await this.tryCommit(
          ns,
          versionHintPath,
          expectedVersionHintEtag,
          commitResult.newMetadata,
          commitResult.metadataPath
        )

        if (committed) {
          return
        }
        // Conflict detected, retry with fresh metadata
      }

      throw new CommitConflictError(
        `Delete commit failed after ${this.maxOccRetries} retries due to concurrent modifications.`,
        ns,
        -1,
        this.maxOccRetries
      )
    } finally {
      releaseLock()
    }
  }

  /**
   * Prepare a delete commit by creating equality delete file, manifests, and building metadata.
   */
  private async prepareDeleteCommit(
    location: string,
    metadata: TableMetadata,
    ids: (string | EntityId)[]
  ): Promise<{
    newMetadata: TableMetadata
    metadataPath: string
  }> {
    const sequenceNumber = (metadata['last-sequence-number'] ?? 0) + 1
    const snapshotId = this.generateSnapshotId()

    // Get the current schema
    const currentSchema = metadata.schemas.find(s => s['schema-id'] === metadata['current-schema-id'])
    if (!currentSchema) {
      throw new Error('Current schema not found')
    }

    // Find the $id field ID in the schema (it's field 1 in our default schema)
    const idField = currentSchema.fields.find(f => f.name === '$id')
    const idFieldId = idField?.id ?? 1

    // Step 1: Create equality delete file using EqualityDeleteBuilder
    const deleteBuilder = new EqualityDeleteBuilder({
      schema: currentSchema,
      equalityFieldIds: [idFieldId],
      sequenceNumber,
      snapshotId,
      outputPrefix: `${location}/data/`,
    })

    // Add each ID to the delete file
    for (const id of ids) {
      deleteBuilder.addDelete({ $id: id })
    }

    const deleteResult = deleteBuilder.build()
    const deleteFileId = generateUUID()
    const deleteFilePath = `${location}/data/${deleteFileId}-delete.parquet`

    // Write the delete file
    await this.storage.write(deleteFilePath, deleteResult.data)

    // Step 2: Create delete manifest using DeleteManifestGenerator
    const deleteManifestGenerator = new DeleteManifestGenerator({
      sequenceNumber,
      snapshotId,
    })

    deleteManifestGenerator.addEqualityDeleteFile({
      'file-path': deleteFilePath,
      'file-format': 'parquet',
      partition: {},
      'record-count': ids.length,
      'file-size-in-bytes': deleteResult.data.byteLength,
      'equality-ids': [idFieldId],
    })

    const deleteManifestResult = deleteManifestGenerator.generate()

    // Write delete manifest
    const deleteManifestId = generateUUID()
    const deleteManifestPath = `${location}/metadata/${deleteManifestId}-m0.avro`
    const deleteManifestContent = this.encodeDeleteManifestToAvro(
      deleteManifestResult.entries,
      sequenceNumber,
      snapshotId
    )
    await this.storage.write(deleteManifestPath, deleteManifestContent)

    // Step 3: Create manifest list that includes both data manifests and delete manifest
    const manifestListGenerator = new ManifestListGenerator({ snapshotId, sequenceNumber })

    // Get manifest content size for stats
    const manifestStat = await this.storage.stat(deleteManifestPath)
    const manifestSize = manifestStat?.size ?? 0

    // Add the new delete manifest
    manifestListGenerator.addManifestWithStats(
      deleteManifestPath,
      manifestSize,
      0, // partition spec ID
      { addedFiles: 1, existingFiles: 0, deletedFiles: 0, addedRows: ids.length, existingRows: 0, deletedRows: 0 },
      true // is delete manifest
    )

    // Include manifests from parent snapshot
    await this.addParentManifests(manifestListGenerator, metadata)

    const manifestListId = generateUUID()
    const manifestListPath = `${location}/metadata/snap-${snapshotId}-${manifestListId}.avro`
    const manifestListContent = encodeManifestListToAvro(manifestListGenerator.getManifests())
    await this.storage.write(manifestListPath, manifestListContent)

    // Step 4: Build new snapshot with 'delete' operation
    const newSnapshot = this.buildDeleteSnapshot(metadata, sequenceNumber, snapshotId, manifestListPath, ids.length)

    // Step 5: Build new metadata
    const newMetadata = this.buildNewMetadata(metadata, sequenceNumber, snapshotId, newSnapshot)

    // Step 6: Write metadata file
    const metadataPath = await this.writeMetadataFile(location, sequenceNumber, newMetadata)

    return { newMetadata, metadataPath }
  }

  /**
   * Build a delete snapshot with summary stats
   */
  private buildDeleteSnapshot(
    metadata: TableMetadata,
    sequenceNumber: number,
    snapshotId: number,
    manifestListPath: string,
    deletedRecords: number
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
      operation: 'delete', // Use delete operation
      schemaId: metadata['current-schema-id'],
    })

    snapshotBuilder.setSummary(
      0, // added files
      0, // deleted files (we're not removing data files, just adding delete file)
      0, // added records
      deletedRecords, // deleted records
      0, // added size
      0, // removed size
      Math.max(0, prevTotalRecords - deletedRecords), // total records (approximate)
      prevTotalSize,
      prevTotalFiles
    )

    return snapshotBuilder.build()
  }

  /**
   * Encode delete manifest entries to Avro format.
   * Delete manifests use content type 1 (MANIFEST_CONTENT_DELETES).
   */
  private encodeDeleteManifestToAvro(
    entries: _ManifestEntry[],
    sequenceNumber: number,
    _snapshotId: number
  ): Uint8Array {
    // For delete manifests, we need to encode with content type indicating deletes
    // The entries already have the correct content field (CONTENT_EQUALITY_DELETES = 2)
    // We use the same Avro encoding but the manifest list entry will have content = 1
    return encodeManifestToAvro(entries, sequenceNumber)
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
      manifestFiles = decodeManifestListFromAvroOrJson(manifestListData)
    } catch (error) {
      // Manifest list not found is expected for empty snapshots
      // Propagate other errors (permission, network, corruption)
      if (!isNotFoundError(error)) {
        throw error
      }
      return []
    }

    // Step 2: Collect data file paths and deleted entity IDs
    const dataFilePaths: string[] = []
    const deletedIds = new Set<string>()

    for (const manifestFile of manifestFiles) {
      if (manifestFile.content === MANIFEST_CONTENT_DELETES) {
        // Process delete manifests to collect deleted entity IDs
        try {
          const manifestData = await this.storage.read(manifestFile['manifest-path'])
          const entries = decodeManifestFromAvroOrJson(manifestData)

          for (const entry of entries) {
            // Only process ADDED (1) and EXISTING (0) delete entries, not DELETED (2)
            if (entry.status !== 2) {
              const deleteFilePath = entry['data-file']['file-path']
              // Check if this is an equality delete file (content type 2)
              const content = entry['data-file'].content
              if (content === CONTENT_EQUALITY_DELETES) {
                // Read and parse the equality delete file
                try {
                  const deleteFileData = await this.storage.read(deleteFilePath)
                  const deleteInfo = parseEqualityDeleteFile(deleteFileData)
                  // Collect all deleted IDs
                  for (const deleteEntry of deleteInfo.entries) {
                    const deletedId = deleteEntry['$id'] as string
                    if (deletedId) {
                      deletedIds.add(deletedId)
                    }
                  }
                } catch (error) {
                  // Skip missing delete files (can happen during time travel)
                  // Propagate other errors (permission, network, corruption)
                  if (!isNotFoundError(error)) {
                    throw error
                  }
                }
              }
            }
          }
        } catch (error) {
          // Skip missing manifest files (can happen during time travel)
          // Propagate other errors (permission, network, corruption)
          if (!isNotFoundError(error)) {
            throw error
          }
        }
      } else {
        // Process data manifests
        try {
          const manifestData = await this.storage.read(manifestFile['manifest-path'])
          const entries = decodeManifestFromAvroOrJson(manifestData)

          for (const entry of entries) {
            // Only include ADDED (1) and EXISTING (0) entries, not DELETED (2)
            if (entry.status !== 2) {
              dataFilePaths.push(entry['data-file']['file-path'])
            }
          }
        } catch (error) {
          // Skip missing manifest files (can happen during time travel)
          // Propagate other errors (permission, network, corruption)
          if (!isNotFoundError(error)) {
            throw error
          }
        }
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

          // Skip entities that have been hard deleted
          if (deletedIds.has(entity.$id)) {
            continue
          }

          const existingEntity = entityMap.get(entity.$id)

          // Keep the entity with the highest version number
          if (!existingEntity || entity.version > existingEntity.version) {
            entityMap.set(entity.$id, entity)
          }
        }
      } catch (error) {
        // Skip missing data files (can happen during time travel or concurrent operations)
        // Propagate other errors (permission, network, corruption)
        if (!isNotFoundError(error)) {
          throw error
        }
      }
    }

    // Convert map values to array and apply filter
    const allEntities: Entity<T>[] = []
    for (const entity of entityMap.values()) {
      // Apply filter if provided
      if (filter && !matchesFilter(entityAsRecord(entity), filter)) {
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
   *
   * Supports all MongoDB-style operators via the mutation/operators module:
   * - Field: $set, $unset, $rename, $setOnInsert
   * - Numeric: $inc, $mul, $min, $max
   * - Array: $push, $pull, $pullAll, $addToSet, $pop
   * - Date: $currentDate
   * - Bitwise: $bit
   */
  private applyUpdate<T>(entity: Entity<T>, update: Update): Entity<T> {
    const result = applyOperators(entity as Record<string, unknown>, update)
    return result.document as Entity<T>
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
        } catch (error) {
          // Iceberg storage interface expects null for missing files
          // Propagate other errors (permission, network, corruption)
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
      throw new InvalidNamespaceError(ns, this.ns)
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
      throw new InvalidNamespaceError(ns, this.ns)
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

  // Write operations throw errors (read-only snapshot view)
  async create(): Promise<never> {
    throw new ReadOnlyError('create', 'IcebergSnapshotBackend')
  }
  async update(): Promise<never> {
    throw new ReadOnlyError('update', 'IcebergSnapshotBackend')
  }
  async delete(): Promise<never> {
    throw new ReadOnlyError('delete', 'IcebergSnapshotBackend')
  }
  async bulkCreate(): Promise<never> {
    throw new ReadOnlyError('bulkCreate', 'IcebergSnapshotBackend')
  }
  async bulkUpdate(): Promise<never> {
    throw new ReadOnlyError('bulkUpdate', 'IcebergSnapshotBackend')
  }
  async bulkDelete(): Promise<never> {
    throw new ReadOnlyError('bulkDelete', 'IcebergSnapshotBackend')
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
