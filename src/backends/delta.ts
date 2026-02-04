/**
 * Delta Lake Entity Backend
 *
 * Stores entities in Delta Lake format with:
 * - Transaction log in `_delta_log/` directory
 * - Commit files: `00000000000000000000.json`, `00000000000000000001.json`, etc.
 * - Each commit contains add/remove actions for Parquet files
 * - `_last_checkpoint` file for optimization
 * - Full time travel support
 */

import type {
  EntityBackend,
  DeltaBackendConfig,
  EntitySchema,
  SnapshotInfo,
  CompactOptions,
  CompactResult,
  VacuumOptions,
  VacuumResult,
  BackendStats,
  SchemaFieldType,
} from './types'
import { CommitConflictError, ReadOnlyError } from './types'
import { AlreadyExistsError, isNotFoundError } from '../storage/errors'
import type { Entity, EntityId, EntityData, CreateInput, DeleteResult, UpdateResult } from '../types/entity'
import type { Filter } from '../types/filter'
import type { FindOptions, CreateOptions, UpdateOptions, DeleteOptions, GetOptions } from '../types/options'
import type { Update } from '../types/update'
import type { StorageBackend } from '../types/storage'

// Import shared Parquet utilities
import {
  entityToRow,
  rowToEntity,
  buildEntityParquetSchema,
  matchesFilter,
  generateEntityId,
  extractDataFields,
} from './parquet-utils'

// Import shared entity utilities
import {
  applyUpdate as applyUpdateUtil,
  createDefaultEntity as createDefaultEntityUtil,
  sortEntities,
  applyPagination,
  generateUUID,
} from './entity-utils'

// Import Parquet utilities
import { ParquetWriter } from '../parquet/writer'
import { readParquet } from '../parquet/reader'

// =============================================================================
// Delta Lake Types
// =============================================================================

/** Delta Lake protocol action */
interface ProtocolAction {
  protocol: {
    minReaderVersion: number
    minWriterVersion: number
  }
}

/** Delta Lake metadata action */
interface MetaDataAction {
  metaData: {
    id: string
    name?: string | undefined
    schemaString: string
    partitionColumns: string[]
    configuration?: Record<string, string> | undefined
    createdTime?: number | undefined
  }
}

/** Delta Lake add action */
interface AddAction {
  add: {
    path: string
    size: number
    modificationTime: number
    dataChange: boolean
    stats?: string | undefined
    partitionValues?: Record<string, string> | undefined
    tags?: Record<string, string> | undefined
  }
}

/** Delta Lake remove action */
interface RemoveAction {
  remove: {
    path: string
    deletionTimestamp?: number | undefined
    dataChange: boolean
    extendedFileMetadata?: boolean | undefined
    partitionValues?: Record<string, string> | undefined
  }
}

/** Delta Lake commit info action */
interface CommitInfoAction {
  commitInfo: {
    timestamp: number
    operation: string
    operationParameters?: Record<string, unknown> | undefined
    readVersion?: number | undefined
    isolationLevel?: string | undefined
    isBlindAppend?: boolean | undefined
    operationMetrics?: Record<string, string> | undefined
    engineInfo?: string | undefined
  }
}

/** Union of all action types */
type DeltaAction = ProtocolAction | MetaDataAction | AddAction | RemoveAction | CommitInfoAction

/** Checkpoint metadata */
interface LastCheckpoint {
  version: number
  size: number
  parts?: number | undefined
}

// =============================================================================
// Delta Backend Implementation
// =============================================================================

/** Checkpoint threshold (create checkpoint every N commits) */
const CHECKPOINT_THRESHOLD = 10

/**
 * Delta Lake entity backend
 *
 * Each namespace becomes a Delta table with the following schema:
 * - $id: string (primary key)
 * - $type: string
 * - name: string
 * - createdAt, createdBy, updatedAt, updatedBy: audit fields
 * - deletedAt, deletedBy: soft delete fields
 * - version: int
 * - $data: string (Base64-encoded Variant for flexible data)
 */
export class DeltaBackend implements EntityBackend {
  readonly type = 'delta' as const
  readonly supportsTimeTravel = true
  readonly supportsSchemaEvolution = true
  readonly readOnly: boolean

  private storage: StorageBackend
  private location: string
  private initialized = false

  // Cache of table versions per namespace
  private versionCache = new Map<string, number>()

  // OCC configuration
  private readonly maxRetries: number
  private readonly baseBackoffMs: number
  private readonly maxBackoffMs: number

  constructor(config: DeltaBackendConfig) {
    this.storage = config.storage
    this.location = config.location ?? ''
    this.readOnly = config.readOnly ?? false

    // OCC defaults
    this.maxRetries = config.maxRetries ?? 10
    this.baseBackoffMs = config.baseBackoffMs ?? 100
    this.maxBackoffMs = config.maxBackoffMs ?? 10000
  }

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  async initialize(): Promise<void> {
    if (this.initialized) return

    // Ensure warehouse directory exists
    if (this.location) {
      await this.storage.mkdir(this.location).catch(() => {
        // Directory might already exist
      })
    }

    this.initialized = true
  }

  async close(): Promise<void> {
    this.versionCache.clear()
    this.initialized = false
  }

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  async get<T extends EntityData = EntityData>(
    ns: string,
    id: string,
    options?: GetOptions
  ): Promise<Entity<T> | null> {
    const entities = await this.find<T>(ns, { $id: `${ns}/${id}` as EntityId }, { limit: 1, ...options })
    return entities[0] ?? null
  }

  async find<T extends EntityData = EntityData>(
    ns: string,
    filter?: Filter,
    options?: FindOptions
  ): Promise<Entity<T>[]> {
    const currentVersion = await this.getCurrentVersion(ns)

    if (currentVersion < 0) {
      // Table doesn't exist
      return []
    }

    // Get the version to query (for time travel)
    let queryVersion = currentVersion
    if (options?.asOf) {
      queryVersion = await this.getVersionAtTimestamp(ns, options.asOf.getTime())
    }

    // Read entities from data files at the specified version
    const entities = await this.readEntitiesAtVersion<T>(ns, queryVersion, filter, options)

    // Apply soft delete filter unless includeDeleted
    if (!options?.includeDeleted) {
      return entities.filter(e => !e.deletedAt)
    }

    return entities
  }

  async count(ns: string, filter?: Filter): Promise<number> {
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

  async create<T extends EntityData = EntityData>(
    ns: string,
    input: CreateInput<T>,
    options?: CreateOptions
  ): Promise<Entity<T>> {
    if (this.readOnly) {
      throw new ReadOnlyError('create', 'DeltaBackend')
    }

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

    await this.appendEntities(ns, [entity], 'WRITE')

    return entity
  }

  async update<T extends EntityData = EntityData>(
    ns: string,
    id: string,
    update: Update,
    options?: UpdateOptions
  ): Promise<Entity<T>> {
    if (this.readOnly) {
      throw new ReadOnlyError('update', 'DeltaBackend')
    }

    // Get existing entity
    const existing = await this.get<T>(ns, id)
    if (!existing && !options?.upsert) {
      throw new Error(`Entity not found: ${ns}/${id}`)
    }

    const now = new Date()
    const actor = options?.actor ?? 'system/parquedb' as EntityId

    // Apply update operators
    const updated = applyUpdateUtil(existing ?? createDefaultEntityUtil<T>(ns, id), update)

    // Update audit fields
    const entity: Entity<T> = {
      ...updated,
      updatedAt: now,
      updatedBy: actor,
      version: (existing?.version ?? 0) + 1,
    } as Entity<T>

    // Write updated entity
    await this.appendEntities(ns, [entity], 'UPDATE')

    return entity
  }

  async delete(ns: string, id: string, options?: DeleteOptions): Promise<DeleteResult> {
    if (this.readOnly) {
      throw new ReadOnlyError('delete', 'DeltaBackend')
    }

    const entity = await this.get(ns, id)
    if (!entity) {
      return { deletedCount: 0 }
    }

    if (options?.hard) {
      // Hard delete: rewrite without the entity
      await this.hardDeleteEntity(ns, `${ns}/${id}`)
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
      await this.appendEntities(ns, [deleted], 'DELETE')
    }

    return { deletedCount: 1 }
  }

  // ===========================================================================
  // Batch Operations
  // ===========================================================================

  async bulkCreate<T extends EntityData = EntityData>(
    ns: string,
    inputs: CreateInput<T>[],
    options?: CreateOptions
  ): Promise<Entity<T>[]> {
    if (this.readOnly) {
      throw new ReadOnlyError('bulkCreate', 'DeltaBackend')
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

    await this.appendEntities(ns, entities, 'WRITE')
    return entities
  }

  async bulkUpdate(
    ns: string,
    filter: Filter,
    update: Update,
    options?: UpdateOptions
  ): Promise<UpdateResult> {
    if (this.readOnly) {
      throw new ReadOnlyError('bulkUpdate', 'DeltaBackend')
    }

    const entities = await this.find(ns, filter)
    if (entities.length === 0) {
      return { matchedCount: 0, modifiedCount: 0 }
    }

    const now = new Date()
    const actor = options?.actor ?? 'system/parquedb' as EntityId

    const updated = entities.map(entity => {
      const result = applyUpdateUtil(entity, update)
      return {
        ...result,
        updatedAt: now,
        updatedBy: actor,
        version: entity.version + 1,
      }
    })

    await this.appendEntities(ns, updated, 'UPDATE')

    return {
      matchedCount: entities.length,
      modifiedCount: updated.length,
    }
  }

  async bulkDelete(ns: string, filter: Filter, options?: DeleteOptions): Promise<DeleteResult> {
    if (this.readOnly) {
      throw new ReadOnlyError('bulkDelete', 'DeltaBackend')
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

      await this.appendEntities(ns, deleted, 'DELETE')
    }

    return { deletedCount: entities.length }
  }

  // ===========================================================================
  // Time Travel
  // ===========================================================================

  async snapshot(ns: string, version: number | Date): Promise<EntityBackend> {
    const currentVersion = await this.getCurrentVersion(ns)
    if (currentVersion < 0) {
      throw new Error(`Table not found: ${ns}`)
    }

    let snapshotVersion: number
    if (typeof version === 'number') {
      snapshotVersion = version
    } else {
      snapshotVersion = await this.getVersionAtTimestamp(ns, version.getTime())
    }

    if (snapshotVersion < 0 || snapshotVersion > currentVersion) {
      throw new Error(`Snapshot not found for version: ${version}`)
    }

    // Return a read-only backend pointing to the specific snapshot
    return new DeltaSnapshotBackend(this, ns, snapshotVersion)
  }

  async listSnapshots(ns: string): Promise<SnapshotInfo[]> {
    const tableLocation = this.getTableLocation(ns)
    const logPath = `${tableLocation}/_delta_log/`

    try {
      const logFiles = await this.storage.list(logPath)
      const commitFiles = logFiles.files
        .filter(f => f.endsWith('.json') && !f.includes('checkpoint'))
        .sort()

      const snapshots: SnapshotInfo[] = []
      for (const commitFile of commitFiles) {
        const versionStr = commitFile.split('/').pop()?.replace('.json', '')
        const version = parseInt(versionStr ?? '0', 10)

        try {
          const commitData = await this.storage.read(commitFile)
          const commitText = new TextDecoder().decode(commitData)
          const actions = this.parseCommitFile(commitText)

          const commitInfo = actions.find((a): a is CommitInfoAction => 'commitInfo' in a)
          const timestamp = commitInfo?.commitInfo.timestamp ?? Date.now()

          snapshots.push({
            id: version,
            timestamp: new Date(timestamp),
            operation: commitInfo?.commitInfo.operation,
          })
        } catch (error) {
          // Skip missing commit files, but propagate other errors (permission, network, etc.)
          if (!isNotFoundError(error)) {
            throw error
          }
        }
      }

      return snapshots
    } catch (error) {
      // Return empty list only for not found errors (table doesn't exist)
      // Propagate other errors (permission, network, etc.)
      if (!isNotFoundError(error)) {
        throw error
      }
      return []
    }
  }

  // ===========================================================================
  // Schema
  // ===========================================================================

  async getSchema(ns: string): Promise<EntitySchema | null> {
    const tableLocation = this.getTableLocation(ns)
    const currentVersion = await this.getCurrentVersion(ns)

    if (currentVersion < 0) {
      return null
    }

    // Read first commit to get schema
    const commitPath = `${tableLocation}/_delta_log/${this.formatVersion(0)}.json`
    try {
      const commitData = await this.storage.read(commitPath)
      const commitText = new TextDecoder().decode(commitData)
      const actions = this.parseCommitFile(commitText)

      const metaData = actions.find((a): a is MetaDataAction => 'metaData' in a)
      if (!metaData) return null

      let schemaObj
      try {
        schemaObj = JSON.parse(metaData.metaData.schemaString)
      } catch (error) {
        // Invalid schema JSON in metadata - this is a data corruption issue
        // Return null for JSON parse errors (schema was corrupted)
        if (error instanceof SyntaxError) {
          return null
        }
        throw error
      }
      return this.deltaSchemaToEntitySchema(ns, schemaObj)
    } catch (error) {
      // Return null only for not found errors (commit file doesn't exist)
      // Propagate other errors (permission, network, etc.)
      if (!isNotFoundError(error)) {
        throw error
      }
      return null
    }
  }

  async setSchema(ns: string, schema: EntitySchema): Promise<void> {
    if (this.readOnly) {
      throw new ReadOnlyError('setSchema', 'DeltaBackend')
    }

    // Schema is set when creating the table
    // For now, just ensure table exists
    const currentVersion = await this.getCurrentVersion(ns)
    if (currentVersion < 0) {
      // Create empty table with schema
      await this.createTable(ns, schema)
    }
  }

  async listNamespaces(): Promise<string[]> {
    const prefix = this.location ? `${this.location}/` : ''
    const result = await this.storage.list(prefix)

    const namespaces = new Set<string>()
    for (const file of result.files) {
      // Extract namespace from path like warehouse/ns/_delta_log/...
      const relativePath = file.slice(prefix.length)
      const parts = relativePath.split('/')
      if (parts.length >= 2 && parts[1] === '_delta_log') {
        namespaces.add(parts[0]!)
      }
    }

    return Array.from(namespaces)
  }

  // ===========================================================================
  // Maintenance
  // ===========================================================================

  async compact(ns: string, options?: CompactOptions): Promise<CompactResult> {
    if (this.readOnly) {
      throw new ReadOnlyError('compact', 'DeltaBackend')
    }

    const startTime = Date.now()
    const compactOptions = this.normalizeCompactOptions(options)
    const tableLocation = this.getTableLocation(ns)

    // Initial check for dry run or no-op cases
    const initialVersion = await this.getCurrentVersion(ns)
    if (initialVersion < 0) {
      return this.emptyCompactResult(startTime)
    }

    // Analyze files to find candidates for compaction (initial check for dry run)
    const initialAnalysis = await this.analyzeFilesForCompaction(ns, tableLocation, initialVersion, compactOptions.minFileSize)

    if (initialAnalysis.smallFiles.length < 2) {
      return {
        filesCompacted: 0,
        filesCreated: 0,
        bytesBefore: initialAnalysis.totalBytes,
        bytesAfter: initialAnalysis.totalBytes,
        durationMs: Date.now() - startTime,
      }
    }

    // Return early for dry run
    if (compactOptions.dryRun) {
      const filesToCompact = initialAnalysis.smallFiles.slice(0, compactOptions.maxFiles)
      const bytesBefore = filesToCompact.reduce((sum, f) => sum + f.size, 0)
      return {
        filesCompacted: filesToCompact.length,
        filesCreated: 1,
        bytesBefore,
        bytesAfter: bytesBefore,
        durationMs: Date.now() - startTime,
      }
    }

    // Perform compaction with OCC retry logic
    let retries = 0
    let dataFilePath: string | null = null
    let lastFilesToCompact: Array<{ path: string; size: number }> = []
    let lastBytesBefore = 0
    let lastBytesAfter = 0

    while (retries <= this.maxRetries) {
      // Invalidate version cache on retry
      if (retries > 0) {
        this.versionCache.delete(ns)
      }

      const currentVersion = await this.getCurrentVersion(ns)
      if (currentVersion < 0) {
        // Table was deleted during compaction
        await this.cleanupOrphanedFile(dataFilePath)
        return this.emptyCompactResult(startTime)
      }

      // Re-analyze files at current version (state may have changed)
      const analysis = await this.analyzeFilesForCompaction(ns, tableLocation, currentVersion, compactOptions.minFileSize)

      if (analysis.smallFiles.length < 2) {
        // No longer need to compact
        await this.cleanupOrphanedFile(dataFilePath)
        return {
          filesCompacted: 0,
          filesCreated: 0,
          bytesBefore: analysis.totalBytes,
          bytesAfter: analysis.totalBytes,
          durationMs: Date.now() - startTime,
        }
      }

      // Select files to compact
      const filesToCompact = analysis.smallFiles.slice(0, compactOptions.maxFiles)
      const bytesBefore = filesToCompact.reduce((sum, f) => sum + f.size, 0)
      lastFilesToCompact = filesToCompact
      lastBytesBefore = bytesBefore

      // Read entities from files to compact
      const entities = await this.readEntitiesFromFiles(tableLocation, filesToCompact)
      if (entities.length === 0) {
        await this.cleanupOrphanedFile(dataFilePath)
        return {
          filesCompacted: 0,
          filesCreated: 0,
          bytesBefore,
          bytesAfter: bytesBefore,
          durationMs: Date.now() - startTime,
        }
      }

      // Write data file (only on first attempt or if file set changed)
      if (!dataFilePath) {
        const dataFileId = generateUUID()
        dataFilePath = `${tableLocation}/${dataFileId}.parquet`
        const parquetSchema = buildEntityParquetSchema()
        const rows = entities.map(entity => entityToRow(entity))
        const writer = new ParquetWriter(this.storage, { compression: 'snappy' })
        const writeResult = await writer.write(dataFilePath, rows, parquetSchema)
        lastBytesAfter = writeResult.size
      }

      // Build compaction commit actions
      const dataFileRelativePath = dataFilePath.slice(tableLocation.length + 1)
      const actions = this.buildCompactionActions(
        filesToCompact,
        dataFileRelativePath.replace('.parquet', ''),
        lastBytesAfter,
        currentVersion,
        compactOptions
      )

      // Try to commit
      const newVersion = currentVersion + 1
      const committed = await this.tryDeltaCommit(tableLocation, newVersion, actions)

      if (committed) {
        this.versionCache.set(ns, newVersion)
        return {
          filesCompacted: lastFilesToCompact.length,
          filesCreated: 1,
          bytesBefore: lastBytesBefore,
          bytesAfter: lastBytesAfter,
          durationMs: Date.now() - startTime,
        }
      }

      // Conflict detected - need to re-read files since state changed
      // Clean up old data file and create new one on next iteration
      await this.cleanupOrphanedFile(dataFilePath)
      dataFilePath = null

      // Retry with backoff
      retries++
      if (retries <= this.maxRetries) {
        await this.backoffDelay(retries)
      }
    }

    // Max retries exceeded - cleanup and throw
    await this.cleanupOrphanedFile(dataFilePath)
    const finalVersion = await this.getCurrentVersion(ns)
    throw new CommitConflictError(
      ns,
      finalVersion + 1,
      retries
    )
  }

  /**
   * Normalize compact options with defaults
   */
  private normalizeCompactOptions(options?: CompactOptions): {
    targetFileSize: number
    minFileSize: number
    maxFiles: number
    dryRun: boolean
  } {
    const targetFileSize = options?.targetFileSize ?? 128 * 1024 * 1024 // 128MB default
    return {
      targetFileSize,
      minFileSize: options?.minFileSize ?? targetFileSize / 4,
      maxFiles: options?.maxFiles ?? 100,
      dryRun: options?.dryRun ?? false,
    }
  }

  /**
   * Create empty compact result
   */
  private emptyCompactResult(startTime: number): CompactResult {
    return {
      filesCompacted: 0,
      filesCreated: 0,
      bytesBefore: 0,
      bytesAfter: 0,
      durationMs: Date.now() - startTime,
    }
  }

  /**
   * Analyze files to find candidates for compaction
   */
  private async analyzeFilesForCompaction(
    ns: string,
    tableLocation: string,
    currentVersion: number,
    minFileSize: number
  ): Promise<{
    smallFiles: Array<{ path: string; size: number }>
    totalBytes: number
  }> {
    const activeFilePaths = await this.getActiveFilesAtVersion(ns, currentVersion)
    const fileInfos: Array<{ path: string; size: number }> = []
    let totalBytes = 0

    for (const relativePath of activeFilePaths) {
      const fullPath = `${tableLocation}/${relativePath}`
      const stat = await this.storage.stat(fullPath)
      if (stat) {
        fileInfos.push({ path: relativePath, size: stat.size })
        totalBytes += stat.size
      }
    }

    const smallFiles = fileInfos.filter(f => f.size < minFileSize)
    return { smallFiles, totalBytes }
  }

  /**
   * Read entities from a list of files
   */
  private async readEntitiesFromFiles(
    tableLocation: string,
    files: Array<{ path: string; size: number }>
  ): Promise<Entity[]> {
    const entities: Entity[] = []
    for (const fileInfo of files) {
      const fullPath = `${tableLocation}/${fileInfo.path}`
      try {
        const rows = await readParquet<Record<string, unknown>>(this.storage, fullPath)
        for (const row of rows) {
          entities.push(rowToEntity(row))
        }
      } catch (error) {
        // Skip missing files (can happen during concurrent operations)
        // Propagate other errors (permission, network, corruption, etc.)
        if (!isNotFoundError(error)) {
          throw error
        }
      }
    }
    return entities
  }

  /**
   * Build actions for compaction commit
   */
  private buildCompactionActions(
    filesToCompact: Array<{ path: string; size: number }>,
    newDataFileId: string,
    newFileSize: number,
    readVersion: number,
    options: { targetFileSize: number; minFileSize: number }
  ): DeltaAction[] {
    const actions: DeltaAction[] = []

    // Remove actions for compacted files
    for (const fileInfo of filesToCompact) {
      actions.push({
        remove: {
          path: fileInfo.path,
          deletionTimestamp: Date.now(),
          dataChange: false,
        },
      })
    }

    // Add action for new combined file
    actions.push({
      add: {
        path: `${newDataFileId}.parquet`,
        size: newFileSize,
        modificationTime: Date.now(),
        dataChange: false,
      },
    })

    // Commit info
    actions.push({
      commitInfo: {
        timestamp: Date.now(),
        operation: 'OPTIMIZE',
        operationParameters: {
          targetFileSize: options.targetFileSize,
          minFileSize: options.minFileSize,
        },
        readVersion,
        isBlindAppend: false,
        operationMetrics: {
          numFilesRemoved: String(filesToCompact.length),
          numFilesAdded: '1',
        },
      },
    })

    return actions
  }

  async vacuum(ns: string, options?: VacuumOptions): Promise<VacuumResult> {
    if (this.readOnly) {
      throw new ReadOnlyError('vacuum', 'DeltaBackend')
    }

    const retentionMs = options?.retentionMs ?? 7 * 24 * 60 * 60 * 1000 // 7 days default
    const dryRun = options?.dryRun ?? false
    const cutoffTime = Date.now() - retentionMs

    const currentVersion = await this.getCurrentVersion(ns)
    if (currentVersion < 0) {
      return {
        filesDeleted: 0,
        bytesReclaimed: 0,
        snapshotsExpired: 0,
      }
    }

    const tableLocation = this.getTableLocation(ns)

    // Collect all files that are referenced by active commits
    // We need to keep files referenced by any commit within the retention period
    const referencedFiles = new Set<string>()

    for (let v = 0; v <= currentVersion; v++) {
      const commitPath = `${tableLocation}/_delta_log/${this.formatVersion(v)}.json`
      try {
        const commitData = await this.storage.read(commitPath)
        const commitText = new TextDecoder().decode(commitData)
        const actions = this.parseCommitFile(commitText)

        // Get commit timestamp
        const commitInfo = actions.find((a): a is CommitInfoAction => 'commitInfo' in a)
        const commitTimestamp = commitInfo?.commitInfo.timestamp ?? Date.now()

        // If commit is within retention, mark all its files as referenced
        if (commitTimestamp >= cutoffTime) {
          for (const action of actions) {
            if ('add' in action) {
              referencedFiles.add(action.add.path)
            }
          }
        }
      } catch (error) {
        // Skip missing commits (can happen during concurrent operations)
        // Propagate other errors (permission, network, etc.)
        if (!isNotFoundError(error)) {
          throw error
        }
      }
    }

    // Also always keep files that are active in the current version
    const activeFiles = await this.getActiveFilesAtVersion(ns, currentVersion)
    for (const file of activeFiles) {
      referencedFiles.add(file)
    }

    // List all parquet files in the table directory
    const allFiles = await this.storage.list(`${tableLocation}/`)
    const parquetFiles = allFiles.files.filter(f => f.endsWith('.parquet') && !f.includes('_delta_log'))

    // Find unreferenced files older than retention
    let filesDeleted = 0
    let bytesReclaimed = 0

    for (const fullPath of parquetFiles) {
      // Extract relative path
      const relativePath = fullPath.slice(tableLocation.length + 1)

      if (!referencedFiles.has(relativePath)) {
        const stat = await this.storage.stat(fullPath)
        if (stat && stat.mtime && stat.mtime.getTime() < cutoffTime) {
          if (!dryRun) {
            try {
              await this.storage.delete(fullPath)
              filesDeleted++
              bytesReclaimed += stat.size
            } catch (error) {
              // Skip already-deleted files (can happen during concurrent operations)
              // Propagate other errors (permission, etc.)
              if (!isNotFoundError(error)) {
                throw error
              }
            }
          } else {
            filesDeleted++
            bytesReclaimed += stat.size
          }
        }
      }
    }

    return {
      filesDeleted,
      bytesReclaimed,
      snapshotsExpired: 0, // Delta Lake doesn't expire snapshots in vacuum, that's a separate operation
    }
  }

  async stats(ns: string): Promise<BackendStats> {
    const currentVersion = await this.getCurrentVersion(ns)
    if (currentVersion < 0) {
      return {
        recordCount: 0,
        totalBytes: 0,
        fileCount: 0,
        snapshotCount: 0,
      }
    }

    // Count entities
    const entities = await this.find(ns, {})

    // Get file stats
    const tableLocation = this.getTableLocation(ns)
    const dataFiles = await this.storage.list(`${tableLocation}/`)
    const parquetFiles = dataFiles.files.filter(f => f.endsWith('.parquet'))

    let totalBytes = 0
    for (const file of parquetFiles) {
      const stat = await this.storage.stat(file)
      if (stat) {
        totalBytes += stat.size
      }
    }

    return {
      recordCount: entities.length,
      totalBytes,
      fileCount: parquetFiles.length,
      snapshotCount: currentVersion + 1,
    }
  }

  // ===========================================================================
  // Internal Helpers
  // ===========================================================================

  /**
   * Get the table location for a namespace
   */
  private getTableLocation(ns: string): string {
    return this.location ? `${this.location}/${ns}` : ns
  }

  /**
   * Format version number as zero-padded 20-digit string
   */
  private formatVersion(version: number): string {
    return version.toString().padStart(20, '0')
  }

  /**
   * Get current version of a table (returns -1 if table doesn't exist)
   */
  private async getCurrentVersion(ns: string): Promise<number> {
    // Check cache first
    const cached = this.versionCache.get(ns)
    if (cached !== undefined) {
      return cached
    }

    const tableLocation = this.getTableLocation(ns)
    const logPath = `${tableLocation}/_delta_log/`

    // Check for _last_checkpoint first
    try {
      const checkpointData = await this.storage.read(`${logPath}_last_checkpoint`)
      let checkpoint: LastCheckpoint
      try {
        checkpoint = JSON.parse(new TextDecoder().decode(checkpointData))
      } catch (error) {
        // Invalid checkpoint JSON - fall through to scan from beginning
        // Only suppress JSON parse errors (corrupt checkpoint file)
        if (error instanceof SyntaxError) {
          throw new Error('Invalid checkpoint JSON')
        }
        throw error
      }

      // Look for commits after checkpoint
      let version = checkpoint.version
      while (true) {
        const nextCommit = `${logPath}${this.formatVersion(version + 1)}.json`
        const exists = await this.storage.exists(nextCommit)
        if (!exists) break
        version++
      }

      this.versionCache.set(ns, version)
      return version
    } catch (error) {
      // No checkpoint file found - fall through to scan from beginning
      // Propagate other errors (permission, network, etc.)
      if (!isNotFoundError(error) && !(error instanceof Error && error.message === 'Invalid checkpoint JSON')) {
        throw error
      }
    }

    // Scan commit files
    try {
      const logFiles = await this.storage.list(logPath)
      const commitFiles = logFiles.files
        .filter(f => f.endsWith('.json') && !f.includes('checkpoint'))
        .sort()

      if (commitFiles.length === 0) {
        return -1
      }

      const lastCommit = commitFiles[commitFiles.length - 1]!
      const versionStr = lastCommit.split('/').pop()?.replace('.json', '')
      const version = parseInt(versionStr ?? '-1', 10)

      this.versionCache.set(ns, version)
      return version
    } catch (error) {
      // Log path not found is expected (table doesn't exist)
      // Other errors (permission, network) should be propagated
      if (!isNotFoundError(error)) {
        throw error
      }
      return -1
    }
  }

  /**
   * Get version at a specific timestamp
   */
  private async getVersionAtTimestamp(ns: string, timestampMs: number): Promise<number> {
    const snapshots = await this.listSnapshots(ns)
    if (snapshots.length === 0) return -1

    // Find the latest version at or before the timestamp
    let result = -1
    for (const snap of snapshots) {
      if (snap.timestamp.getTime() <= timestampMs) {
        result = Math.max(result, snap.id as number)
      }
    }

    return result >= 0 ? result : 0
  }

  /**
   * Parse commit file into actions
   */
  private parseCommitFile(content: string): DeltaAction[] {
    const lines = content.trim().split('\n')
    const actions: DeltaAction[] = []
    for (const line of lines) {
      try {
        actions.push(JSON.parse(line) as DeltaAction)
      } catch {
        // Skip invalid JSON lines in commit file
        continue
      }
    }
    return actions
  }

  /**
   * Create a new Delta table
   */
  private async createTable(ns: string, schema?: EntitySchema): Promise<void> {
    const tableLocation = this.getTableLocation(ns)

    // Create directories
    await this.storage.mkdir(tableLocation).catch(() => {})
    await this.storage.mkdir(`${tableLocation}/_delta_log`).catch(() => {})

    // Create protocol action
    const protocol: ProtocolAction = {
      protocol: {
        minReaderVersion: 1,
        minWriterVersion: 2,
      },
    }

    // Create metadata action
    const deltaSchema = schema
      ? this.entitySchemaToDelta(schema)
      : this.createDefaultDeltaSchema()

    const metaData: MetaDataAction = {
      metaData: {
        id: generateUUID(),
        schemaString: JSON.stringify(deltaSchema),
        partitionColumns: [],
        createdTime: Date.now(),
      },
    }

    // Create commit info
    const commitInfo: CommitInfoAction = {
      commitInfo: {
        timestamp: Date.now(),
        operation: 'CREATE TABLE',
        operationParameters: {},
        isBlindAppend: true,
      },
    }

    // Write initial commit
    const commitContent = [protocol, metaData, commitInfo]
      .map(a => JSON.stringify(a))
      .join('\n')

    const commitPath = `${tableLocation}/_delta_log/${this.formatVersion(0)}.json`
    await this.storage.write(commitPath, new TextEncoder().encode(commitContent))

    this.versionCache.set(ns, 0)
  }

  /**
   * Append entities to a Delta table using optimistic concurrency control
   *
   * Uses atomic file creation with ifNoneMatch: '*' to ensure only one
   * process can successfully create a commit file at a given version.
   * If a concurrent write is detected, the operation retries with a fresh version
   * and exponential backoff.
   */
  private async appendEntities<T>(
    ns: string,
    entities: Entity<T>[],
    operation: string
  ): Promise<void> {
    if (entities.length === 0) return
    const tableLocation = this.getTableLocation(ns)

    let retries = 0
    let dataFilePath: string | null = null

    while (retries <= this.maxRetries) {
      // Invalidate version cache on retry
      if (retries > 0) {
        this.versionCache.delete(ns)
      }

      const currentVersion = await this.getCurrentVersion(ns)
      const isNewTable = currentVersion < 0
      const newVersion = isNewTable ? 0 : currentVersion + 1

      // Ensure table directories exist
      if (isNewTable) {
        await this.ensureTableDirectories(tableLocation)
      }

      // Write data file (only on first attempt)
      if (!dataFilePath) {
        dataFilePath = await this.writeDataFile(tableLocation, entities)
      }

      // Build commit actions
      const actions = await this.buildCommitActions(
        tableLocation,
        dataFilePath,
        operation,
        isNewTable,
        currentVersion
      )

      // Try to commit
      const committed = await this.tryDeltaCommit(tableLocation, newVersion, actions)

      if (committed) {
        this.versionCache.set(ns, newVersion)
        // Create checkpoint if needed
        if (newVersion > 0 && newVersion % CHECKPOINT_THRESHOLD === 0) {
          await this.createCheckpoint(ns, newVersion)
        }
        return
      }

      // Conflict detected - retry with backoff
      retries++
      if (retries <= this.maxRetries) {
        await this.backoffDelay(retries)
      }
    }

    // Max retries exceeded - cleanup and throw
    await this.cleanupOrphanedFile(dataFilePath)
    const finalVersion = await this.getCurrentVersion(ns)
    throw new CommitConflictError(
      ns,
      finalVersion + 1,
      retries
    )
  }

  /**
   * Ensure table directories exist
   */
  private async ensureTableDirectories(tableLocation: string): Promise<void> {
    await this.storage.mkdir(tableLocation).catch(() => {})
    await this.storage.mkdir(`${tableLocation}/_delta_log`).catch(() => {})
  }

  /**
   * Write entities to a Parquet data file
   */
  private async writeDataFile<T>(
    tableLocation: string,
    entities: Entity<T>[]
  ): Promise<string> {
    const dataFileId = generateUUID()
    const dataFilePath = `${tableLocation}/${dataFileId}.parquet`
    const parquetSchema = buildEntityParquetSchema()
    const rows = entities.map(entity => entityToRow(entity))
    const writer = new ParquetWriter(this.storage, { compression: 'snappy' })
    await writer.write(dataFilePath, rows, parquetSchema)
    return dataFilePath
  }

  /**
   * Build commit actions for append operation
   */
  private async buildCommitActions(
    tableLocation: string,
    dataFilePath: string,
    operation: string,
    isNewTable: boolean,
    currentVersion: number
  ): Promise<DeltaAction[]> {
    const actions: DeltaAction[] = []

    // First commit needs protocol and metadata
    if (isNewTable) {
      actions.push(this.createProtocolAction())
      actions.push(this.createMetadataAction())
    }

    // Add data file action
    const dataFileRelativePath = dataFilePath.slice(tableLocation.length + 1)
    const stat = await this.storage.stat(dataFilePath)
    actions.push(this.createAddAction(dataFileRelativePath, stat?.size ?? 0))

    // Add commit info
    actions.push(this.createCommitInfoAction(operation, isNewTable ? undefined : currentVersion))

    return actions
  }

  /**
   * Create protocol action for new table
   */
  private createProtocolAction(): ProtocolAction {
    return {
      protocol: {
        minReaderVersion: 1,
        minWriterVersion: 2,
      },
    }
  }

  /**
   * Create metadata action for new table
   */
  private createMetadataAction(): MetaDataAction {
    return {
      metaData: {
        id: generateUUID(),
        schemaString: JSON.stringify(this.createDefaultDeltaSchema()),
        partitionColumns: [],
        createdTime: Date.now(),
      },
    }
  }

  /**
   * Create add action for data file
   */
  private createAddAction(path: string, size: number): AddAction {
    return {
      add: {
        path,
        size,
        modificationTime: Date.now(),
        dataChange: true,
      },
    }
  }

  /**
   * Create commit info action
   */
  private createCommitInfoAction(operation: string, readVersion?: number): CommitInfoAction {
    return {
      commitInfo: {
        timestamp: Date.now(),
        operation,
        operationParameters: {},
        readVersion,
        isBlindAppend: true,
      },
    }
  }

  /**
   * Try to write commit file atomically
   * Returns true if successful, false if conflict
   */
  private async tryDeltaCommit(
    tableLocation: string,
    version: number,
    actions: DeltaAction[]
  ): Promise<boolean> {
    const commitContent = actions.map(a => JSON.stringify(a)).join('\n')
    const commitPath = `${tableLocation}/_delta_log/${this.formatVersion(version)}.json`

    try {
      await this.storage.write(
        commitPath,
        new TextEncoder().encode(commitContent),
        { ifNoneMatch: '*' }
      )
      return true
    } catch (error) {
      if (error instanceof AlreadyExistsError || this.isConflictError(error)) {
        return false
      }
      throw error
    }
  }

  /**
   * Apply exponential backoff delay with jitter
   */
  private async backoffDelay(retryCount: number): Promise<void> {
    const backoffMs = Math.min(
      this.baseBackoffMs * Math.pow(2, retryCount - 1) + Math.random() * this.baseBackoffMs,
      this.maxBackoffMs
    )
    await this.sleep(backoffMs)
  }

  /**
   * Clean up orphaned data file
   */
  private async cleanupOrphanedFile(filePath: string | null): Promise<void> {
    if (filePath) {
      await this.storage.delete(filePath).catch(() => {})
    }
  }

  /**
   * Check if an error indicates a conflict (file already exists)
   */
  private isConflictError(error: unknown): boolean {
    if (error instanceof Error) {
      // Check for AlreadyExistsError or similar
      if ('code' in error && (error as { code: string }).code === 'ALREADY_EXISTS') {
        return true
      }
      // Check message for common patterns
      if (error.message.includes('already exists') || error.message.includes('ALREADY_EXISTS')) {
        return true
      }
    }
    return false
  }

  /**
   * Sleep for specified milliseconds
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  /**
   * Create a checkpoint file
   *
   * Delta Lake checkpoints are Parquet files with the following schema:
   * - txn, add, remove, metaData, protocol, commitInfo (all optional strings containing JSON)
   * Each row represents one action, with the appropriate column containing JSON.
   */
  private async createCheckpoint(ns: string, version: number): Promise<void> {
    const tableLocation = this.getTableLocation(ns)
    const logPath = `${tableLocation}/_delta_log/`

    // Collect all actions up to this version
    const allActions: DeltaAction[] = []
    const activeFiles = new Map<string, AddAction>()

    for (let v = 0; v <= version; v++) {
      const commitPath = `${logPath}${this.formatVersion(v)}.json`
      try {
        const commitData = await this.storage.read(commitPath)
        const commitText = new TextDecoder().decode(commitData)
        const actions = this.parseCommitFile(commitText)

        for (const action of actions) {
          if ('protocol' in action) {
            // Keep latest protocol
            allActions[0] = action
          } else if ('metaData' in action) {
            // Keep latest metadata
            allActions[1] = action
          } else if ('add' in action) {
            activeFiles.set(action.add.path, action)
          } else if ('remove' in action) {
            activeFiles.delete(action.remove.path)
          }
        }
      } catch (error) {
        // Skip missing commits (expected during normal operation)
        // Propagate other errors (permission, network, corruption)
        if (!isNotFoundError(error)) {
          throw error
        }
      }
    }

    // Build checkpoint content
    const checkpointActions: DeltaAction[] = [
      ...allActions.filter(a => a !== undefined),
      ...Array.from(activeFiles.values()),
    ]

    // Convert actions to checkpoint rows (Delta Lake checkpoint Parquet schema)
    const checkpointRows = checkpointActions.map(action => {
      const row: Record<string, string | null> = {
        txn: null,
        add: null,
        remove: null,
        metaData: null,
        protocol: null,
        commitInfo: null,
      }
      if ('protocol' in action) row.protocol = JSON.stringify(action.protocol)
      if ('metaData' in action) row.metaData = JSON.stringify(action.metaData)
      if ('add' in action) row.add = JSON.stringify(action.add)
      if ('remove' in action) row.remove = JSON.stringify(action.remove)
      if ('commitInfo' in action) row.commitInfo = JSON.stringify(action.commitInfo)
      return row
    })

    // Delta Lake checkpoint schema (all optional strings containing JSON)
    const checkpointSchema = {
      txn: { type: 'STRING' as const, optional: true },
      add: { type: 'STRING' as const, optional: true },
      remove: { type: 'STRING' as const, optional: true },
      metaData: { type: 'STRING' as const, optional: true },
      protocol: { type: 'STRING' as const, optional: true },
      commitInfo: { type: 'STRING' as const, optional: true },
    }

    // Write checkpoint as proper Parquet file
    const checkpointPath = `${logPath}${this.formatVersion(version)}.checkpoint.parquet`
    const writer = new ParquetWriter(this.storage, { compression: 'snappy' })
    await writer.write(checkpointPath, checkpointRows, checkpointSchema)

    // Write _last_checkpoint
    const lastCheckpoint: LastCheckpoint = {
      version,
      size: checkpointActions.length,
    }
    await this.storage.write(
      `${logPath}_last_checkpoint`,
      new TextEncoder().encode(JSON.stringify(lastCheckpoint))
    )
  }

  /**
   * Hard delete a single entity
   */
  private async hardDeleteEntity(ns: string, entityId: string): Promise<void> {
    await this.hardDeleteEntities(ns, [entityId])
  }

  /**
   * Hard delete multiple entities using optimistic concurrency control
   *
   * Uses atomic file creation with ifNoneMatch: '*' to ensure only one
   * process can successfully create a commit file at a given version.
   * If a concurrent write is detected, the operation retries with a fresh version
   * and exponential backoff.
   */
  private async hardDeleteEntities(ns: string, entityIds: (string | EntityId)[]): Promise<void> {
    const tableLocation = this.getTableLocation(ns)
    const idSet = new Set(entityIds)

    let retries = 0
    let dataFilePath: string | null = null

    while (retries <= this.maxRetries) {
      // Invalidate version cache on retry
      if (retries > 0) {
        this.versionCache.delete(ns)
      }

      const currentVersion = await this.getCurrentVersion(ns)
      if (currentVersion < 0) {
        // Table doesn't exist or was deleted
        await this.cleanupOrphanedFile(dataFilePath)
        return
      }

      // Read all current entities at this version
      const allEntities = await this.readEntitiesAtVersion(ns, currentVersion, {}, { includeDeleted: true })

      // Filter out the ones to delete
      const remainingEntities = allEntities.filter(e => !idSet.has(e.$id))

      // Get current active files at this version
      const activeFiles = await this.getActiveFilesAtVersion(ns, currentVersion)

      // Build actions for this version
      const actions: DeltaAction[] = []

      // Remove all old files
      for (const oldFile of activeFiles) {
        actions.push({
          remove: {
            path: oldFile,
            deletionTimestamp: Date.now(),
            dataChange: true,
          },
        })
      }

      // Write remaining entities to new file (only if there are remaining entities)
      if (remainingEntities.length > 0) {
        // Clean up old data file if retrying with different state
        if (dataFilePath && retries > 0) {
          await this.cleanupOrphanedFile(dataFilePath)
          dataFilePath = null
        }

        // Write data file (only on first attempt or after state change)
        if (!dataFilePath) {
          const dataFileId = generateUUID()
          dataFilePath = `${tableLocation}/${dataFileId}.parquet`

          const parquetSchema = buildEntityParquetSchema()
          const rows = remainingEntities.map(entity => entityToRow(entity))

          const writer = new ParquetWriter(this.storage, { compression: 'snappy' })
          const writeResult = await writer.write(dataFilePath, rows, parquetSchema)

          // Add new file action
          const dataFileRelativePath = dataFilePath.slice(tableLocation.length + 1)
          actions.push({
            add: {
              path: dataFileRelativePath,
              size: writeResult.size,
              modificationTime: Date.now(),
              dataChange: true,
            },
          })
        } else {
          // Reuse existing data file path
          const dataFileRelativePath = dataFilePath.slice(tableLocation.length + 1)
          const stat = await this.storage.stat(dataFilePath)
          actions.push({
            add: {
              path: dataFileRelativePath,
              size: stat?.size ?? 0,
              modificationTime: Date.now(),
              dataChange: true,
            },
          })
        }
      } else {
        // All entities deleted, clean up any data file from previous attempts
        await this.cleanupOrphanedFile(dataFilePath)
        dataFilePath = null
      }

      // Add commit info
      actions.push({
        commitInfo: {
          timestamp: Date.now(),
          operation: 'DELETE',
          operationParameters: {},
          readVersion: currentVersion,
          isBlindAppend: false,
        },
      })

      // Try to commit with OCC
      const newVersion = currentVersion + 1
      const committed = await this.tryDeltaCommit(tableLocation, newVersion, actions)

      if (committed) {
        this.versionCache.set(ns, newVersion)
        return
      }

      // Conflict detected - retry with backoff
      retries++
      if (retries <= this.maxRetries) {
        await this.backoffDelay(retries)
      }
    }

    // Max retries exceeded - cleanup and throw
    await this.cleanupOrphanedFile(dataFilePath)
    const finalVersion = await this.getCurrentVersion(ns)
    throw new CommitConflictError(
      ns,
      finalVersion + 1,
      retries
    )
  }

  /**
   * Get active files at a specific version
   */
  private async getActiveFilesAtVersion(ns: string, version: number): Promise<string[]> {
    const tableLocation = this.getTableLocation(ns)
    const logPath = `${tableLocation}/_delta_log/`
    const activeFiles = new Map<string, boolean>()

    for (let v = 0; v <= version; v++) {
      const commitPath = `${logPath}${this.formatVersion(v)}.json`
      try {
        const commitData = await this.storage.read(commitPath)
        const commitText = new TextDecoder().decode(commitData)
        const actions = this.parseCommitFile(commitText)

        for (const action of actions) {
          if ('add' in action) {
            activeFiles.set(action.add.path, true)
          } else if ('remove' in action) {
            activeFiles.delete(action.remove.path)
          }
        }
      } catch (error) {
        // Skip missing commits (expected during normal operation)
        // Propagate other errors (permission, network, corruption)
        if (!isNotFoundError(error)) {
          throw error
        }
      }
    }

    return Array.from(activeFiles.keys())
  }

  /**
   * Read entities at a specific version
   */
  private async readEntitiesAtVersion<T extends EntityData = EntityData>(
    ns: string,
    version: number,
    filter?: Filter,
    options?: FindOptions
  ): Promise<Entity<T>[]> {
    const tableLocation = this.getTableLocation(ns)

    // Get active files at this version
    const activeFiles = await this.getActiveFilesAtVersion(ns, version)

    // Read entities from all active files
    const entityMap = new Map<string, Entity<T>>()

    for (const relativePath of activeFiles) {
      const filePath = `${tableLocation}/${relativePath}`
      try {
        const rows = await readParquet<Record<string, unknown>>(this.storage, filePath)

        for (const row of rows) {
          const entity = rowToEntity<T>(row)

          // Keep the latest version of each entity
          const existing = entityMap.get(entity.$id)
          if (!existing || entity.version > existing.version) {
            entityMap.set(entity.$id, entity)
          }
        }
      } catch (error) {
        // Skip missing files (can happen during concurrent operations or time travel)
        // Propagate other errors (permission, network, corruption)
        if (!isNotFoundError(error)) {
          throw error
        }
      }
    }

    // Convert to array and apply filter
    let entities = Array.from(entityMap.values())

    if (filter && Object.keys(filter).length > 0) {
      entities = entities.filter(e => matchesFilter(e as Record<string, unknown>, filter))
    }

    // Apply sorting
    sortEntities(entities, options?.sort)

    // Apply skip and limit
    return applyPagination(entities, options?.skip, options?.limit)
  }

  /**
   * Create default Delta schema for entities
   */
  private createDefaultDeltaSchema(): { type: string; fields: Array<{ name: string; type: string; nullable: boolean }> } {
    return {
      type: 'struct',
      fields: [
        { name: '$id', type: 'string', nullable: false },
        { name: '$type', type: 'string', nullable: false },
        { name: 'name', type: 'string', nullable: false },
        { name: 'createdAt', type: 'string', nullable: false },
        { name: 'createdBy', type: 'string', nullable: false },
        { name: 'updatedAt', type: 'string', nullable: false },
        { name: 'updatedBy', type: 'string', nullable: false },
        { name: 'deletedAt', type: 'string', nullable: true },
        { name: 'deletedBy', type: 'string', nullable: true },
        { name: 'version', type: 'integer', nullable: false },
        { name: '$data', type: 'string', nullable: true },
      ],
    }
  }

  /**
   * Convert EntitySchema to Delta schema
   */
  private entitySchemaToDelta(
    schema: EntitySchema
  ): { type: string; fields: Array<{ name: string; type: string; nullable: boolean }> } {
    return {
      type: 'struct',
      fields: schema.fields.map(field => ({
        name: field.name,
        type: this.entityFieldTypeToDelta(field.type),
        nullable: field.nullable ?? !field.required,
      })),
    }
  }

  /**
   * Convert Delta schema to EntitySchema
   */
  private deltaSchemaToEntitySchema(
    name: string,
    schema: { type: string; fields: Array<{ name: string; type: string; nullable: boolean }> }
  ): EntitySchema {
    return {
      name,
      fields: schema.fields.map(field => ({
        name: field.name,
        type: this.deltaTypeToEntityFieldType(field.type) as SchemaFieldType,
        nullable: field.nullable,
        required: !field.nullable,
      })),
    }
  }

  /**
   * Convert entity field type to Delta type
   */
  private entityFieldTypeToDelta(type: SchemaFieldType): string {
    if (typeof type === 'string') {
      switch (type) {
        case 'string':
          return 'string'
        case 'int':
          return 'integer'
        case 'long':
          return 'long'
        case 'float':
          return 'float'
        case 'double':
          return 'double'
        case 'boolean':
          return 'boolean'
        case 'timestamp':
          return 'timestamp'
        case 'date':
          return 'date'
        case 'binary':
          return 'binary'
        case 'json':
        case 'variant':
          return 'string'
        default:
          return 'string'
      }
    }
    return 'string'
  }

  /**
   * Convert Delta type to entity field type
   */
  private deltaTypeToEntityFieldType(type: string): string {
    switch (type) {
      case 'string':
        return 'string'
      case 'integer':
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
        return 'timestamp'
      case 'date':
        return 'date'
      case 'binary':
        return 'binary'
      default:
        return 'string'
    }
  }

  // ===========================================================================
  // Public methods for snapshot backend
  // ===========================================================================

  /**
   * Read entities at version (for snapshot backend)
   * @internal
   */
  async _readEntitiesAtVersion<T extends EntityData = EntityData>(
    ns: string,
    version: number,
    filter?: Filter,
    options?: FindOptions
  ): Promise<Entity<T>[]> {
    return this.readEntitiesAtVersion<T>(ns, version, filter, options)
  }
}

// =============================================================================
// Snapshot Backend (Read-Only Time-Travel View)
// =============================================================================

/**
 * Read-only backend for querying a specific snapshot
 */
class DeltaSnapshotBackend implements EntityBackend {
  readonly type = 'delta' as const
  readonly supportsTimeTravel = false
  readonly supportsSchemaEvolution = false
  readonly readOnly = true

  constructor(
    private parent: DeltaBackend,
    private ns: string,
    private snapshotVersion: number
  ) {}

  async initialize(): Promise<void> {}
  async close(): Promise<void> {}

  async get<T extends EntityData = EntityData>(
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

  async find<T extends EntityData = EntityData>(
    ns: string,
    filter?: Filter,
    options?: FindOptions
  ): Promise<Entity<T>[]> {
    if (ns !== this.ns) {
      throw new Error(`Snapshot backend only supports namespace: ${this.ns}`)
    }
    return this.parent._readEntitiesAtVersion<T>(ns, this.snapshotVersion, filter, options)
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
 * Create a Delta Lake backend
 */
export function createDeltaBackend(config: DeltaBackendConfig): DeltaBackend {
  return new DeltaBackend(config)
}
