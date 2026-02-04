/**
 * ParqueDB Core Module
 *
 * Contains the main ParqueDBImpl class that coordinates all database operations.
 * This module delegates to focused operation modules for maintainability.
 */

import type {
  Entity,
  EntityData,
  EntityId,
  CreateInput,
  PaginatedResult,
  DeleteResult,
  Filter,
  UpdateInput,
  FindOptions,
  GetOptions,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
  Schema,
  StorageBackend,
  HistoryOptions,
  Event,
  EventOp,
  ValidationMode,
} from '../types'

import { entityTarget, relTarget } from '../types'
import { SchemaValidator } from '../schema/validator'
import { IndexManager } from '../indexes/manager'
import type { IndexDefinition, IndexMetadata, IndexStats } from '../indexes/types'
import { asRelEventPayload } from '../types/cast'
import { DEFAULT_MAX_INBOUND } from '../constants'

import type {
  ParqueDBConfig,
  Collection,
  HistoryResult,
  DiffResult,
  RevertOptions,
  GetRelatedOptions,
  GetRelatedResult,
  ParqueDBTransaction,
  Snapshot,
  SnapshotManager,
  SnapshotQueryStats,
  EventLog,
  ArchiveEventsResult,
  UpsertManyItem,
  UpsertManyOptions,
  UpsertManyResult,
  SnapshotConfig,
  IngestStreamOptions,
  IngestStreamResult,
} from './types'

import {
  DEFAULT_EVENT_LOG_CONFIG,
} from './types'

import { validateNamespace, validateFilter, validateUpdateOperators, normalizeNamespace, toFullId } from './validation'
import { CollectionManager } from './collections'
import { EventLogImpl } from './events'
import {
  getEntityStore,
  getEventStore,
  getArchivedEventStore,
  getSnapshotStore,
  getQueryStatsStore,
  getReverseRelIndex,
  getEntityEventIndex,
  getReconstructionCache,
  clearGlobalState,
  configureEntityStore,
  getEntityCacheStats,
  type EntityStoreConfig,
} from './store'
import type { IStorageRouter, StorageMode } from '../storage/router'
import type { CollectionOptions } from '../types/collection-options'
import type { EmbeddingProvider } from '../embeddings/provider'
import { SnapshotManagerImpl } from './snapshots'

// Import operation modules
import {
  findEntities,
  getEntity,
  createEntity,
  updateEntity,
  deleteEntity,
  deleteManyEntities,
  restoreEntity,
  type EntityOperationsContext,
} from './entity-operations'

// =============================================================================
// Parquet Corruption Detection Constants
// =============================================================================

/**
 * Number of bytes to check at the end of a Parquet file for corruption detection.
 *
 * Parquet files have a specific footer structure:
 * - 4 bytes: Footer length (little-endian)
 * - N bytes: Footer metadata (Thrift-encoded)
 * - 4 bytes: Magic bytes "PAR1"
 *
 * We check the last 12 bytes to capture the footer length, potential corruption
 * in the footer, and the magic bytes. This provides a reasonable window to detect
 * incomplete writes or corrupted data without scanning the entire file.
 */
const PARQUET_FOOTER_CHECK_SIZE = 12

/**
 * Byte value that indicates corrupted or unwritten data in storage.
 *
 * When storage systems (like R2 or S3) have incomplete writes or corruption,
 * the affected bytes are often filled with 0xFF. This is because:
 * - Flash storage cells default to 0xFF when erased
 * - Some filesystems use 0xFF to mark unallocated space
 * - Incomplete write operations may leave trailing 0xFF bytes
 */
const CORRUPTION_INDICATOR_BYTE = 0xFF

/**
 * Minimum number of corruption indicator bytes required to flag a file as corrupted.
 *
 * We use a threshold of 2 rather than 1 to reduce false positives, as a single
 * 0xFF byte could legitimately appear in valid Parquet footer metadata. However,
 * multiple consecutive 0xFF bytes in the footer region strongly indicates
 * incomplete writes or data corruption.
 */
const CORRUPTION_THRESHOLD = 2

import {
  indexRelationshipsForEntity,
  unindexRelationshipsForEntity,
  applyRelationshipOperators,
  hydrateEntity,
  applyMaxInboundToEntity,
  getRelatedEntities,
  type RelationshipOperationsContext,
} from './relationship-operations'

import {
  recordEvent,
  flushEvents,
  archiveEvents,
  reconstructEntityAtTime,
  getEntityHistory,
  getEntityAtVersion,
  computeDiff,
  revertEntity,
  type EventOperationsContext,
} from './event-operations'

import {
  registerSchema as registerSchemaOp,
  validateAgainstSchema,
  type SchemaOperationsContext,
} from './schema-operations'

// =============================================================================
// ParqueDB Implementation
// =============================================================================

/**
 * Internal ParqueDB implementation class
 */
export class ParqueDBImpl {
  private storage: StorageBackend
  private schema: Schema = {}
  private schemaValidator: SchemaValidator | null = null
  private collectionManager: CollectionManager | null = null
  private entities: Map<string, Entity>
  private events: Event[]
  private archivedEvents: Event[]
  private snapshots: Snapshot[]
  private queryStats: Map<string, SnapshotQueryStats>
  private reverseRelIndex: Map<string, Map<string, Set<string>>>
  private entityEventIndex: Map<string, Event[]>
  private reconstructionCache: Map<string, { entity: Entity | null; timestamp: number }>
  private snapshotConfig: SnapshotConfig
  private eventLogConfig: Required<import('./types').EventLogConfig>
  private pendingEvents: Event[] = []
  private flushPromise: Promise<void> | null = null
  private inTransaction = false
  private indexManager: IndexManager
  private storageRouter: IStorageRouter | null = null
  private collectionOptions: Map<string, CollectionOptions> = new Map()
  private _embeddingProvider: EmbeddingProvider | null = null
  private _snapshotManager: SnapshotManagerImpl | null = null
  private _onEvent: ((event: Event) => void | Promise<void>) | null = null

  constructor(config: ParqueDBConfig) {
    if (!config.storage) {
      throw new Error('Storage backend is required')
    }

    this.storage = config.storage
    this.snapshotConfig = config.snapshotConfig || {}
    this.eventLogConfig = { ...DEFAULT_EVENT_LOG_CONFIG, ...config.eventLogConfig }

    // Configure entity cache with size limits before getting the store
    if (config.maxCacheSize !== undefined || config.onCacheEvict !== undefined) {
      const entityStoreConfig: EntityStoreConfig = {}
      if (config.maxCacheSize !== undefined) {
        entityStoreConfig.maxEntities = config.maxCacheSize
      }
      if (config.onCacheEvict) {
        entityStoreConfig.onEvict = config.onCacheEvict
      }
      configureEntityStore(config.storage, entityStoreConfig)
    }

    this.entities = getEntityStore(config.storage)
    this.events = getEventStore(config.storage)
    this.archivedEvents = getArchivedEventStore(config.storage)
    this.snapshots = getSnapshotStore(config.storage)
    this.queryStats = getQueryStatsStore(config.storage)
    this.reverseRelIndex = getReverseRelIndex(config.storage)
    this.entityEventIndex = getEntityEventIndex(config.storage)
    this.reconstructionCache = getReconstructionCache(config.storage)
    this.indexManager = new IndexManager(config.storage)

    if (config.storageRouter) {
      this.storageRouter = config.storageRouter
    }

    if (config.collectionOptions) {
      this.collectionOptions = config.collectionOptions
    }

    if (config.schema) {
      this.registerSchema(config.schema)
    }

    if (config.embeddingProvider) {
      this._embeddingProvider = config.embeddingProvider
    }

    if (config.onEvent) {
      this._onEvent = config.onEvent
    }
  }

  // ===========================================================================
  // Context Builders
  // ===========================================================================

  private getEntityContext(): EntityOperationsContext {
    return {
      storage: this.storage,
      schema: this.schema,
      schemaValidator: this.schemaValidator,
      entities: this.entities,
      events: this.events,
      snapshots: this.snapshots,
      queryStats: this.queryStats,
      indexManager: this.indexManager,
      snapshotConfig: this.snapshotConfig,
      embeddingProvider: this._embeddingProvider,
      recordEvent: (op, target, before, after, actor, meta) =>
        this.recordEventInternal(op, target, before, after, actor, meta),
      reconstructEntityAtTime: (fullId, asOf) =>
        this.reconstructEntityAtTimeInternal(fullId, asOf),
      indexRelationshipsForEntity: (sourceId, entity) =>
        this.indexRelationshipsForEntityInternal(sourceId, entity),
      unindexRelationshipsForEntity: (sourceId, entity) =>
        this.unindexRelationshipsForEntityInternal(sourceId, entity),
      applyRelationshipOperators: (entity, fullId, update) =>
        this.applyRelationshipOperatorsInternal(entity, fullId, update),
      detectParquetCorruption: (data, filePath) =>
        this.detectParquetCorruption(data, filePath),
    }
  }

  private getRelationshipContext(): RelationshipOperationsContext {
    return {
      schema: this.schema,
      entities: this.entities,
      reverseRelIndex: this.reverseRelIndex,
    }
  }

  private getEventContext(): EventOperationsContext {
    return {
      storage: this.storage,
      entities: this.entities,
      events: this.events,
      archivedEvents: this.archivedEvents,
      snapshots: this.snapshots,
      queryStats: this.queryStats,
      entityEventIndex: this.entityEventIndex,
      reconstructionCache: this.reconstructionCache,
      snapshotConfig: this.snapshotConfig,
      eventLogConfig: this.eventLogConfig,
      pendingEvents: this.pendingEvents,
      inTransaction: this.inTransaction,
      flushPromise: this.flushPromise,
      setFlushPromise: (promise) => { this.flushPromise = promise },
      setPendingEvents: (events) => { this.pendingEvents = events },
      getSnapshotManager: () => this.getSnapshotManager(),
      onEvent: this._onEvent ?? undefined,
    }
  }

  private getSchemaContext(): SchemaOperationsContext {
    return {
      schema: this.schema,
      schemaValidator: this.schemaValidator,
      setSchemaValidator: (validator) => { this.schemaValidator = validator },
      setSchema: (schema) => { this.schema = schema },
    }
  }

  // ===========================================================================
  // Internal Helper Methods
  // ===========================================================================

  /**
   * Detects corruption in Parquet file data by checking the footer region.
   *
   * Parquet files have a well-defined footer structure that ends with the magic
   * bytes "PAR1". This method checks the last few bytes of the file for signs
   * of corruption, such as incomplete writes that leave 0xFF bytes.
   *
   * @param data - The raw Parquet file data as a Uint8Array
   * @param _filePath - The file path (unused, kept for logging context)
   * @throws Error if corruption is detected in the Parquet footer
   */
  private detectParquetCorruption(data: Uint8Array, _filePath: string): void {
    if (!data || data.length === 0) return

    // Parquet files must have at least a 4-byte magic header/footer
    if (data.length >= 4) {
      // Check the footer region for corruption indicators
      const lastBytes = data.slice(-PARQUET_FOOTER_CHECK_SIZE)
      let invalidByteCount = 0

      for (let i = 0; i < lastBytes.length; i++) {
        if (lastBytes[i] === CORRUPTION_INDICATOR_BYTE) {
          invalidByteCount++
        }
      }

      // If we find multiple corruption indicator bytes, the file is likely corrupted
      if (invalidByteCount >= CORRUPTION_THRESHOLD) {
        throw new Error(`Event log corruption detected: invalid checksum in parquet file`)
      }
    }
  }

  private recordEventInternal(
    op: EventOp,
    target: string,
    before: Entity | null,
    after: Entity | null,
    actor?: EntityId,
    meta?: Record<string, unknown>
  ): Promise<void> {
    return recordEvent(this.getEventContext(), op, target, before, after, actor, meta)
  }

  private reconstructEntityAtTimeInternal(fullId: string, asOf: Date): Entity | null {
    return reconstructEntityAtTime(this.getEventContext(), fullId, asOf)
  }

  private indexRelationshipsForEntityInternal(sourceId: string, entity: Entity): void {
    indexRelationshipsForEntity(this.getRelationshipContext(), sourceId, entity)
  }

  private unindexRelationshipsForEntityInternal(sourceId: string, entity: Entity): void {
    unindexRelationshipsForEntity(this.getRelationshipContext(), sourceId, entity)
  }

  private applyRelationshipOperatorsInternal<T extends EntityData>(
    entity: Entity,
    fullId: string,
    update: UpdateInput<T>
  ): Entity {
    return applyRelationshipOperators(this.getRelationshipContext(), entity, fullId, update)
  }

  private validateAgainstSchemaInternal(
    namespace: string,
    data: CreateInput,
    validateOnWrite?: boolean | ValidationMode
  ): void {
    validateAgainstSchema(this.getSchemaContext(), namespace, data, validateOnWrite)
  }

  // ===========================================================================
  // Public API - Embedding Provider
  // ===========================================================================

  get embeddingProvider(): EmbeddingProvider | null {
    return this._embeddingProvider
  }

  setEmbeddingProvider(provider: EmbeddingProvider): void {
    this._embeddingProvider = provider
  }

  // ===========================================================================
  // Public API - Schema
  // ===========================================================================

  registerSchema(schema: Schema): void {
    registerSchemaOp(this.getSchemaContext(), schema)
  }

  getSchemaValidator(): SchemaValidator | null {
    return this.schemaValidator
  }

  // ===========================================================================
  // Public API - Lifecycle
  // ===========================================================================

  /**
   * Wait for any pending flush operations to complete.
   * Call this before cleanup to ensure all data is written.
   * This loops until no more flush promises are pending (handles chained flushes).
   */
  async flush(): Promise<void> {
    // Loop until all pending flushes are complete
    // (flushEvents can schedule follow-up flushes for events that arrive during a flush)
    while (this.flushPromise) {
      const currentPromise = this.flushPromise
      try {
        await currentPromise
      } catch {
        // Ignore flush errors during cleanup
      }
      // If flushPromise is still the same (no new flush scheduled), break
      // If a new flush was scheduled, continue the loop
      if (this.flushPromise === currentPromise) {
        break
      }
    }
  }

  /**
   * Synchronously dispose of resources without waiting for pending flushes.
   * Use disposeAsync() if you need to wait for pending operations.
   */
  dispose(): void {
    this.pendingEvents = []
    this.flushPromise = null

    if (this.collectionManager) {
      this.collectionManager.clear()
    }

    clearGlobalState(this.storage)

    this.entities.clear()
    this.events.length = 0
    this.snapshots.length = 0
    this.queryStats.clear()
    this.reverseRelIndex.clear()
    this.entityEventIndex.clear()
    this.reconstructionCache.clear()
  }

  /**
   * Asynchronously dispose of resources, waiting for pending flushes first.
   */
  async disposeAsync(): Promise<void> {
    await this.flush()
    this.dispose()
  }

  // ===========================================================================
  // Public API - Cache Management
  // ===========================================================================

  /**
   * Get statistics about the entity cache.
   *
   * @returns Cache statistics including size, max entries, hits, misses, and evictions
   *
   * @example
   * ```typescript
   * const stats = db.getCacheStats()
   * console.log(`Cache size: ${stats.size}/${stats.maxEntries}`)
   * console.log(`Hit rate: ${(stats.hitRate * 100).toFixed(1)}%`)
   * console.log(`Evictions: ${stats.evictions}`)
   * ```
   */
  getCacheStats(): {
    size: number
    maxEntries: number
    hits: number
    misses: number
    evictions: number
    hitRate: number
  } | undefined {
    return getEntityCacheStats(this.storage)
  }

  // ===========================================================================
  // Public API - Collection
  // ===========================================================================

  collection<T extends EntityData = EntityData>(namespace: string): Collection<T> {
    if (!this.collectionManager) {
      this.collectionManager = new CollectionManager(this)
    }
    return this.collectionManager.get<T>(namespace)
  }

  // ===========================================================================
  // Public API - Storage Router
  // ===========================================================================

  getStorageMode(namespace: string): StorageMode {
    if (this.storageRouter) {
      return this.storageRouter.getStorageMode(namespace)
    }
    return 'flexible'
  }

  getDataPath(namespace: string): string {
    if (this.storageRouter) {
      return this.storageRouter.getDataPath(namespace)
    }
    const normalizedNs = normalizeNamespace(namespace)
    return `data/${normalizedNs}/data.parquet`
  }

  hasTypedSchema(namespace: string): boolean {
    if (this.storageRouter) {
      return this.storageRouter.hasTypedSchema(namespace)
    }
    return false
  }

  getCollectionOptions(namespace: string): CollectionOptions | undefined {
    const normalizedNs = normalizeNamespace(namespace)
    return this.collectionOptions.get(normalizedNs)
  }

  getStorageRouter(): IStorageRouter | null {
    return this.storageRouter
  }

  // ===========================================================================
  // Public API - CRUD Operations
  // ===========================================================================

  async find<T extends EntityData = EntityData>(
    namespace: string,
    filter?: Filter,
    options?: FindOptions
  ): Promise<PaginatedResult<Entity<T>>> {
    return findEntities<T>(this.getEntityContext(), namespace, filter, options)
  }

  async get<T extends EntityData = EntityData>(
    namespace: string,
    id: string,
    options?: GetOptions
  ): Promise<Entity<T> | null> {
    const entity = await getEntity<T>(this.getEntityContext(), namespace, id, options)
    if (!entity) return null

    const fullId = toFullId(namespace, id)

    // Handle maxInbound for reverse relationship fields
    if (options?.maxInbound !== undefined) {
      const resultEntity = applyMaxInboundToEntity(this.getRelationshipContext(), entity, options.maxInbound)
      if (options?.hydrate && options.hydrate.length > 0) {
        return hydrateEntity(this.getRelationshipContext(), resultEntity, fullId, options.hydrate, options.maxInbound)
      }
      return resultEntity
    }

    // Handle hydration if requested
    if (options?.hydrate && options.hydrate.length > 0) {
      const maxInbound = options.maxInbound ?? DEFAULT_MAX_INBOUND
      return hydrateEntity(this.getRelationshipContext(), entity, fullId, options.hydrate, maxInbound)
    }

    return entity
  }

  async getRelated<T extends EntityData = EntityData>(
    namespace: string,
    id: string,
    relationField: string,
    options?: GetRelatedOptions
  ): Promise<GetRelatedResult<T>> {
    return getRelatedEntities<T>(this.getRelationshipContext(), namespace, id, relationField, options)
  }

  async create<T extends EntityData = EntityData>(
    namespace: string,
    data: CreateInput<T>,
    options?: CreateOptions
  ): Promise<Entity<T>> {
    return createEntity<T>(
      this.getEntityContext(),
      namespace,
      data,
      options,
      (ns, d, v) => this.validateAgainstSchemaInternal(ns, d, v)
    )
  }

  async update<T extends EntityData = EntityData>(
    namespace: string,
    id: string,
    update: UpdateInput<T>,
    options?: UpdateOptions
  ): Promise<Entity<T> | null> {
    const result = await updateEntity<T>(this.getEntityContext(), namespace, id, update, options)

    // Record relationship events for $link/$unlink operations
    if (result && (update.$link || update.$unlink)) {
      const fullId = toFullId(namespace, id)
      const [eventNs, ...eventIdParts] = fullId.split('/')
      const actor = options?.actor

      if (update.$link) {
        for (const [predicate, value] of Object.entries(update.$link)) {
          const linkTargets = Array.isArray(value) ? value : [value]
          for (const linkTarget of linkTargets) {
            const toTarget = String(linkTarget).replace('/', ':')
            const fromTarget = entityTarget(eventNs ?? '', eventIdParts.join('/'))
            await this.recordEventInternal(
              'CREATE',
              relTarget(fromTarget, predicate, toTarget),
              null,
              asRelEventPayload({ predicate, to: linkTarget }),
              actor as EntityId | undefined
            )
          }
        }
      }

      if (update.$unlink) {
        for (const [predicate, value] of Object.entries(update.$unlink)) {
          if (value === '$all') continue
          const unlinkTargets = Array.isArray(value) ? value : [value]
          for (const unlinkTarget of unlinkTargets) {
            const toTarget = String(unlinkTarget).replace('/', ':')
            const fromTarget = entityTarget(eventNs ?? '', eventIdParts.join('/'))
            await this.recordEventInternal(
              'DELETE',
              relTarget(fromTarget, predicate, toTarget),
              asRelEventPayload({ predicate, to: unlinkTarget }),
              null,
              actor as EntityId | undefined
            )
          }
        }
      }
    }

    return result
  }

  async delete(
    namespace: string,
    id: string,
    options?: DeleteOptions
  ): Promise<DeleteResult> {
    return deleteEntity(this.getEntityContext(), namespace, id, options)
  }

  async deleteMany(
    namespace: string,
    filter: Filter,
    options?: DeleteOptions
  ): Promise<DeleteResult> {
    return deleteManyEntities(this.getEntityContext(), namespace, filter, options)
  }

  async restore<T extends EntityData = EntityData>(
    namespace: string,
    id: string,
    options?: { actor?: EntityId | undefined }
  ): Promise<Entity<T> | null> {
    return restoreEntity<T>(this.getEntityContext(), namespace, id, options)
  }

  // ===========================================================================
  // Public API - History and Time-Travel
  // ===========================================================================

  async getHistory(namespace: string, id: string, options?: HistoryOptions): Promise<HistoryResult> {
    const fullId = toFullId(namespace, id)
    return this.history(fullId as EntityId, options)
  }

  async history(entityId: EntityId, options?: HistoryOptions): Promise<HistoryResult> {
    return getEntityHistory(this.getEventContext(), entityId, options)
  }

  async getAtVersion<T extends EntityData = EntityData>(
    namespace: string,
    id: string,
    version: number
  ): Promise<Entity<T> | null> {
    return getEntityAtVersion<T>(this.getEventContext(), namespace, id, version)
  }

  async diff(entityId: EntityId, t1: Date, t2: Date): Promise<DiffResult> {
    return computeDiff(this.getEventContext(), entityId, t1, t2)
  }

  async revert<T extends EntityData = EntityData>(
    entityId: EntityId,
    targetTime: Date,
    options?: RevertOptions
  ): Promise<Entity<T>> {
    return revertEntity<T>(
      this.getEventContext(),
      (op, target, before, after, actor, meta) =>
        this.recordEventInternal(op, target, before, after, actor, meta),
      entityId,
      targetTime,
      options
    )
  }

  // ===========================================================================
  // Public API - Event Log
  // ===========================================================================

  archiveEvents(options?: { olderThan?: Date | undefined; maxEvents?: number | undefined }): ArchiveEventsResult {
    return archiveEvents(this.getEventContext(), options)
  }

  getArchivedEvents(): Event[] {
    return [...this.archivedEvents]
  }

  getEventLog(): EventLog {
    return new EventLogImpl(
      this.events,
      this.archivedEvents,
      this.eventLogConfig,
      (options) => this.archiveEvents(options)
    )
  }

  // ===========================================================================
  // Public API - Transactions
  // ===========================================================================

  beginTransaction(): ParqueDBTransaction {
    this.inTransaction = true

    const pendingOps: Array<{
      type: 'create' | 'update' | 'delete'
      namespace: string
      id?: string | undefined
      data?: CreateInput | undefined
      update?: UpdateInput | undefined
      options?: CreateOptions | UpdateOptions | DeleteOptions | undefined
      entity?: Entity | undefined
      beforeState?: Entity | undefined
    }> = []

    const self = this

    return {
      async create<T extends EntityData = EntityData>(
        namespace: string,
        data: CreateInput<T>,
        options?: CreateOptions
      ): Promise<Entity<T>> {
        const entity = await self.create(namespace, data, options)
        pendingOps.push({
          type: 'create',
          namespace,
          data,
          options,
          entity: entity as Entity
        })
        return entity
      },

      async update<T extends EntityData = EntityData>(
        namespace: string,
        id: string,
        update: UpdateInput<T>,
        options?: UpdateOptions
      ): Promise<Entity<T> | null> {
        const fullId = toFullId(namespace, id)
        const beforeState = self.entities.get(fullId)
        const beforeStateCopy = beforeState ? JSON.parse(JSON.stringify(beforeState)) : undefined

        const entity = await self.update(namespace, id, update, options)
        if (entity) {
          pendingOps.push({
            type: 'update',
            namespace,
            id,
            update,
            options,
            entity: entity as Entity,
            beforeState: beforeStateCopy
          })
        }
        return entity
      },

      async delete(namespace: string, id: string, options?: DeleteOptions): Promise<DeleteResult> {
        const fullId = toFullId(namespace, id)
        const beforeState = self.entities.get(fullId)
        const beforeStateCopy = beforeState ? JSON.parse(JSON.stringify(beforeState)) : undefined

        const result = await self.delete(namespace, id, options)
        if (result.deletedCount > 0) {
          pendingOps.push({
            type: 'delete',
            namespace,
            id,
            options,
            beforeState: beforeStateCopy
          })
        }
        return result
      },

      async commit(): Promise<void> {
        self.inTransaction = false
        await flushEvents(self.getEventContext())
        pendingOps.length = 0
      },

      async rollback(): Promise<void> {
        self.inTransaction = false
        self.pendingEvents = []

        for (const op of pendingOps.reverse()) {
          let fullId: string
          if (op.entity?.$id) {
            fullId = op.entity.$id as string
          } else if (op.id) {
            fullId = toFullId(op.namespace, op.id)
          } else {
            continue
          }

          const [targetNs, ...targetIdParts] = fullId.split('/')
          const expectedTarget = entityTarget(targetNs ?? '', targetIdParts.join('/'))

          if (op.type === 'create' && op.entity) {
            // Unindex relationships before removing the entity
            self.unindexRelationshipsForEntityInternal(fullId, op.entity)
            self.entities.delete(fullId)
            const idx = self.events.findIndex(
              e => e.op === 'CREATE' && e.target === expectedTarget
            )
            if (idx >= 0) self.events.splice(idx, 1)
          } else if (op.type === 'update' && op.beforeState) {
            // Unindex relationships from the current (updated) state
            const currentEntity = self.entities.get(fullId)
            if (currentEntity) {
              self.unindexRelationshipsForEntityInternal(fullId, currentEntity)
            }
            // Restore the previous state
            self.entities.set(fullId, op.beforeState)
            // Reindex relationships from the restored state
            self.indexRelationshipsForEntityInternal(fullId, op.beforeState)
            const idx = self.events.findIndex(
              e => e.op === 'UPDATE' && e.target === expectedTarget
            )
            if (idx >= 0) self.events.splice(idx, 1)
          } else if (op.type === 'delete' && op.beforeState) {
            // Restore the entity
            self.entities.set(fullId, op.beforeState)
            // Reindex relationships from the restored entity
            self.indexRelationshipsForEntityInternal(fullId, op.beforeState)
            const idx = self.events.findIndex(
              e => e.op === 'DELETE' && e.target === expectedTarget
            )
            if (idx >= 0) self.events.splice(idx, 1)
          }
        }

        pendingOps.length = 0
      },
    }
  }

  // ===========================================================================
  // Public API - Snapshots
  // ===========================================================================

  getSnapshotManager(): SnapshotManager {
    if (!this._snapshotManager) {
      this._snapshotManager = new SnapshotManagerImpl({
        storage: this.storage,
        entities: this.entities,
        events: this.events,
        snapshots: this.snapshots,
        queryStats: this.queryStats,
      })
    }
    return this._snapshotManager
  }

  // ===========================================================================
  // Public API - Upsert Operations
  // ===========================================================================

  private extractFilterFields(filter: Filter): Record<string, unknown> {
    const filterFields: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(filter)) {
      if (!key.startsWith('$')) {
        filterFields[key] = value
      }
    }
    return filterFields
  }

  private buildUpsertCreateData<T extends EntityData = EntityData>(
    filterFields: Record<string, unknown>,
    update: UpdateInput<T>
  ): Record<string, unknown> {
    const createData: Record<string, unknown> = {
      $type: 'Unknown',
      name: 'Upserted',
      ...filterFields,
      ...update.$set,
      ...update.$setOnInsert,
    }

    if (update.$inc) {
      for (const [key, value] of Object.entries(update.$inc)) {
        createData[key] = ((createData[key] as number) || 0) + (value as number)
      }
    }

    if (update.$push) {
      for (const [key, value] of Object.entries(update.$push)) {
        const pushValue = value as Record<string, unknown>
        if (value && typeof value === 'object' && '$each' in pushValue) {
          createData[key] = [...((pushValue.$each as unknown[]) || [])]
        } else {
          createData[key] = [value]
        }
      }
    }

    if (update.$addToSet) {
      for (const [key, value] of Object.entries(update.$addToSet)) {
        createData[key] = [value]
      }
    }

    if (update.$currentDate) {
      const now = new Date()
      for (const key of Object.keys(update.$currentDate)) {
        createData[key] = now
      }
    }

    return createData
  }

  async upsert<T extends EntityData = EntityData>(
    namespace: string,
    filter: Filter,
    update: UpdateInput<T>,
    options?: { returnDocument?: 'before' | 'after' | undefined }
  ): Promise<Entity<T> | null> {
    validateNamespace(namespace)
    validateFilter(filter)
    validateUpdateOperators(update)

    const result = await this.find<T>(namespace, filter)

    if (result.items.length > 0) {
      const entity = result.items[0]!
      return this.update<T>(namespace, entity.$id as string, update, {
        returnDocument: options?.returnDocument,
      })
    } else {
      const filterFields = this.extractFilterFields(filter)
      const data: CreateInput<T> = this.buildUpsertCreateData(filterFields, update) as CreateInput<T>
      return this.create<T>(namespace, data)
    }
  }

  async upsertMany<T extends EntityData = EntityData>(
    namespace: string,
    items: UpsertManyItem<T>[],
    options?: UpsertManyOptions
  ): Promise<UpsertManyResult> {
    validateNamespace(namespace)

    const result: UpsertManyResult = {
      ok: true,
      insertedCount: 0,
      modifiedCount: 0,
      matchedCount: 0,
      upsertedCount: 0,
      upsertedIds: [],
      errors: [],
    }

    if (items.length === 0) {
      return result
    }

    const ordered = options?.ordered ?? true
    const actor = options?.actor

    for (let i = 0; i < items.length; i++) {
      const item = items[i]!

      try {
        validateFilter(item.filter)
        validateUpdateOperators(item.update)

        const existing = await this.find<T>(namespace, item.filter)

        if (existing.items.length > 0) {
          const entity = existing.items[0]!
          result.matchedCount++

          const updateOptions: UpdateOptions = {
            returnDocument: 'after',
          }

          if (actor) {
            updateOptions.actor = actor
          }

          if (item.options?.expectedVersion !== undefined) {
            updateOptions.expectedVersion = item.options.expectedVersion
          }

          const { $setOnInsert: _, ...updateWithoutSetOnInsert } = item.update as UpdateInput<T> & { $setOnInsert?: unknown | undefined }

          await this.update<T>(namespace, entity.$id as string, updateWithoutSetOnInsert, updateOptions)
          result.modifiedCount++
        } else {
          const filterFields = this.extractFilterFields(item.filter)
          const createData = this.buildUpsertCreateData(filterFields, item.update)

          const createOptions: CreateOptions = {}
          if (actor) {
            createOptions.actor = actor
          }

          const created = await this.create<T>(namespace, createData as CreateInput<T>, createOptions)

          result.insertedCount++
          result.upsertedCount++
          result.upsertedIds.push(created.$id)

          if (item.update.$link) {
            await this.update<T>(namespace, created.$id as string, {
              $link: item.update.$link,
            } as UpdateInput<T>, { actor })
          }
        }
      } catch (error: unknown) {
        result.ok = false
        result.errors.push({
          index: i,
          filter: item.filter,
          error: error instanceof Error ? error : new Error(String(error)),
        })

        if (ordered) {
          break
        }
      }
    }

    return result
  }

  // ===========================================================================
  // Public API - Ingest Stream
  // ===========================================================================

  async ingestStream<T extends EntityData = EntityData>(
    namespace: string,
    source: AsyncIterable<Partial<T>> | Iterable<Partial<T>>,
    options?: IngestStreamOptions<Partial<T>>
  ): Promise<IngestStreamResult> {
    validateNamespace(namespace)

    const batchSize = options?.batchSize ?? 100
    const ordered = options?.ordered ?? true
    const actor = options?.actor
    const skipValidation = options?.skipValidation
    const entityType = options?.entityType
    const transform = options?.transform
    const onProgress = options?.onProgress
    const onBatchComplete = options?.onBatchComplete

    const result: IngestStreamResult = {
      insertedCount: 0,
      failedCount: 0,
      skippedCount: 0,
      insertedIds: [],
      errors: [],
    }

    let batch: Array<Partial<T>> = []
    let batchNumber = 0
    let totalProcessed = 0
    let index = 0

    const processBatch = async () => {
      if (batch.length === 0) return

      batchNumber++
      const batchItems = batch
      batch = []

      for (const doc of batchItems) {
        try {
          const createData = {
            ...doc,
            ...(entityType ? { $type: entityType } : {}),
          } as unknown as CreateInput<T>

          const entity = await this.create<T>(namespace, createData, {
            actor,
            skipValidation,
          })

          result.insertedCount++
          result.insertedIds.push(entity.$id)
        } catch (error: unknown) {
          result.failedCount++
          result.errors.push({
            index: totalProcessed,
            message: error instanceof Error ? error.message : String(error),
            error: error instanceof Error ? error : undefined,
          })

          if (ordered) {
            totalProcessed++
            onProgress?.(totalProcessed)
            return false
          }
        }
        totalProcessed++
        onProgress?.(totalProcessed)
      }

      if (onBatchComplete) {
        onBatchComplete({
          batchNumber,
          batchSize: batchItems.length,
          totalProcessed,
        })
      }

      return true
    }

    try {
      const iterable = Symbol.asyncIterator in source
        ? (source as AsyncIterable<Partial<T>>)
        : (async function* () { yield* source as Iterable<Partial<T>> })()

      for await (const rawDoc of iterable) {
        let doc = rawDoc
        if (transform) {
          const transformed = transform(rawDoc)
          if (transformed === null) {
            result.skippedCount++
            index++
            continue
          }
          doc = transformed
        }

        batch.push(doc)
        index++

        if (batch.length >= batchSize) {
          const shouldContinue = await processBatch()
          if (!shouldContinue) {
            break
          }
        }
      }

      await processBatch()
    } catch (error: unknown) {
      result.failedCount++
      result.errors.push({
        index,
        message: error instanceof Error ? error.message : String(error),
        error: error instanceof Error ? error : undefined,
      })
    }

    return result
  }

  // ===========================================================================
  // Public API - Index Management
  // ===========================================================================

  async createIndex(ns: string, definition: IndexDefinition): Promise<IndexMetadata> {
    validateNamespace(ns)
    return this.indexManager.createIndex(ns, definition)
  }

  async dropIndex(ns: string, indexName: string): Promise<void> {
    validateNamespace(ns)
    return this.indexManager.dropIndex(ns, indexName)
  }

  async listIndexes(ns: string): Promise<IndexMetadata[]> {
    validateNamespace(ns)
    return this.indexManager.listIndexes(ns)
  }

  async getIndex(ns: string, indexName: string): Promise<IndexMetadata | null> {
    validateNamespace(ns)
    return this.indexManager.getIndexMetadata(ns, indexName)
  }

  async rebuildIndex(ns: string, indexName: string): Promise<void> {
    validateNamespace(ns)
    return this.indexManager.rebuildIndex(ns, indexName)
  }

  async getIndexStats(ns: string, indexName: string): Promise<IndexStats> {
    validateNamespace(ns)
    return this.indexManager.getIndexStats(ns, indexName)
  }

  getIndexManager(): IndexManager {
    return this.indexManager
  }

  // ===========================================================================
  // Public API - Materialized View Integration
  // ===========================================================================

  /**
   * Set the event callback for MV integration.
   *
   * This callback is invoked after every event is recorded (CREATE, UPDATE, DELETE,
   * REL_CREATE, REL_DELETE). The callback is fire-and-forget to avoid blocking writes.
   *
   * @param callback - Function to call with each event, or null to disable
   *
   * @example
   * ```typescript
   * import { createMVIntegration } from 'parquedb/materialized-views'
   *
   * const { emitter, engine, bridge } = createMVIntegration()
   *
   * // Set up MV integration
   * db.setEventCallback((event) => emitter.emit(event))
   *
   * // Register MV handlers
   * engine.registerMV({
   *   name: 'OrderAnalytics',
   *   sourceNamespaces: ['orders'],
   *   async process(events) {
   *     // Update MV based on events
   *   }
   * })
   *
   * // Start engine and connect bridge
   * await engine.start()
   * bridge.connect()
   * ```
   */
  setEventCallback(callback: ((event: Event) => void | Promise<void>) | null): void {
    this._onEvent = callback
  }

  /**
   * Get the current event callback.
   */
  getEventCallback(): ((event: Event) => void | Promise<void>) | null {
    return this._onEvent
  }
}
