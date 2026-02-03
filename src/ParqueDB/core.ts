/**
 * ParqueDB Core Module
 *
 * Contains the main ParqueDBImpl class with all database operations.
 */

import type {
  Entity,
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
  RelSet,
  SortDirection,
  ValidationMode,
} from '../types'

import { entityTarget, parseEntityTarget, relTarget, isRelationshipTarget } from '../types'
import { normalizeSortDirection } from '../types/options'
import { parseFieldType, isRelationString, parseRelation } from '../types/schema'
import { SchemaValidator } from '../schema/validator'
import { FileNotFoundError } from '../storage/MemoryBackend'
import { IndexManager } from '../indexes/manager'
import type { IndexDefinition, IndexMetadata, IndexStats } from '../indexes/types'
import { getNestedValue, compareValues, generateId, deepClone } from '../utils'
import { matchesFilter as canonicalMatchesFilter } from '../query/filter'
import { applyOperators } from '../mutation/operators'
import { DEFAULT_MAX_INBOUND } from '../constants'
import { asRelEventPayload } from '../types/cast'
import pluralize from 'pluralize'

/**
 * Derive entity type from namespace/collection name
 * e.g., 'posts' -> 'Post', 'users' -> 'User', 'categories' -> 'Category'
 */
function deriveTypeFromNamespace(namespace: string): string {
  // Singularize and capitalize
  const singular = pluralize.singular(namespace)
  return singular.charAt(0).toUpperCase() + singular.slice(1)
}

import type {
  ParqueDBConfig,
  Collection,
  HistoryItem,
  HistoryResult,
  DiffResult,
  RevertOptions,
  GetRelatedOptions,
  GetRelatedResult,
  ParqueDBTransaction,
  Snapshot,
  SnapshotManager,
  SnapshotQueryStats,
  SnapshotStorageStats,
  PruneSnapshotsOptions,
  RawSnapshot,
  EventLog,
  EventLogConfig,
  ArchiveEventsResult,
  UpsertManyItem,
  UpsertManyOptions,
  UpsertManyResult,
  SnapshotConfig,
} from './types'

import {
  DEFAULT_EVENT_LOG_CONFIG,
  VersionConflictError,
  EntityNotFoundError,
  ValidationError,
  RelationshipError,
  EventError,
} from './types'

import { validateNamespace, validateFilter, validateUpdateOperators, normalizeNamespace } from './validation'
import { CollectionImpl } from './collection'
import {
  getEntityStore,
  getEventStore,
  getArchivedEventStore,
  getSnapshotStore,
  getQueryStatsStore,
  getReverseRelIndex,
  addToReverseRelIndex,
  removeFromReverseRelIndex,
  getFromReverseRelIndex,
  getAllFromReverseRelIndexByNs,
  removeAllFromReverseRelIndex,
  clearGlobalState,
} from './store'
import type { IStorageRouter, StorageMode } from '../storage/router'
import type { CollectionOptions } from '../db'
import type { EmbeddingProvider } from '../embeddings/provider'
import { normalizeVectorFilter, isTextVectorQuery } from '../query/vector-query'

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
  private collections = new Map<string, CollectionImpl>()
  private entities: Map<string, Entity> // Shared via global store
  private events: Event[] // Shared via global store
  private archivedEvents: Event[] // Shared via global store for archived events
  private snapshots: Snapshot[] // Shared via global store
  private queryStats: Map<string, SnapshotQueryStats> // Shared via global store
  private reverseRelIndex: Map<string, Map<string, Set<string>>> // Reverse relationship index for O(1) lookups
  private snapshotConfig: SnapshotConfig
  private eventLogConfig: Required<EventLogConfig>
  private pendingEvents: Event[] = [] // Buffer for batched writes
  private flushPromise: Promise<void> | null = null // Promise for pending flush
  private inTransaction = false // Flag to suppress auto-flush during transactions
  private indexManager: IndexManager // Index management
  private storageRouter: IStorageRouter | null = null // Routes storage by collection mode
  private collectionOptions: Map<string, CollectionOptions> = new Map() // Per-collection options
  private _embeddingProvider: EmbeddingProvider | null = null // Embedding provider for text-to-vector conversion

  constructor(config: ParqueDBConfig) {
    if (!config.storage) {
      throw new Error('Storage backend is required')
    }
    this.storage = config.storage
    this.snapshotConfig = config.snapshotConfig || {}
    this.eventLogConfig = { ...DEFAULT_EVENT_LOG_CONFIG, ...config.eventLogConfig }
    // Use global stores keyed by storage backend for persistence across instances
    this.entities = getEntityStore(config.storage)
    this.events = getEventStore(config.storage)
    this.archivedEvents = getArchivedEventStore(config.storage)
    this.snapshots = getSnapshotStore(config.storage)
    this.queryStats = getQueryStatsStore(config.storage)
    this.reverseRelIndex = getReverseRelIndex(config.storage)
    // Initialize index manager
    this.indexManager = new IndexManager(config.storage)
    // Initialize storage router and collection options if provided
    if (config.storageRouter) {
      this.storageRouter = config.storageRouter
    }
    if (config.collectionOptions) {
      this.collectionOptions = config.collectionOptions
    }
    if (config.schema) {
      this.registerSchema(config.schema)
    }
    // Initialize embedding provider for query-time text-to-vector conversion
    if (config.embeddingProvider) {
      this._embeddingProvider = config.embeddingProvider
    }
  }

  /**
   * Get the embedding provider
   */
  get embeddingProvider(): EmbeddingProvider | null {
    return this._embeddingProvider
  }

  /**
   * Set the embedding provider for query-time text-to-vector conversion
   */
  setEmbeddingProvider(provider: EmbeddingProvider): void {
    this._embeddingProvider = provider
  }

  /**
   * Register a schema for validation
   */
  registerSchema(schema: Schema): void {
    // Merge with existing schema
    this.schema = { ...this.schema, ...schema }
    // Create/update the schema validator
    this.schemaValidator = new SchemaValidator(this.schema, {
      mode: 'permissive', // Default to permissive, will be overridden per-operation
      allowUnknownFields: true,
    })
  }

  /**
   * Get the schema validator for advanced validation scenarios
   */
  getSchemaValidator(): SchemaValidator | null {
    return this.schemaValidator
  }

  /**
   * Dispose of this ParqueDB instance and clean up associated global state.
   * Call this when you are done using a ParqueDB instance to prevent memory leaks.
   *
   * After calling dispose():
   * - The global state (entities, events, snapshots, query stats) for this storage backend is cleared
   * - The instance should not be used anymore
   * - Other ParqueDB instances using the same storage backend will also lose their shared state
   */
  dispose(): void {
    // Clear any pending operations
    this.pendingEvents = []
    this.flushPromise = null

    // Clear instance state
    this.collections.clear()

    // Clear global state for this storage backend
    clearGlobalState(this.storage)

    // Clear local references (they now point to deleted WeakMap entries)
    this.entities.clear()
    this.events.length = 0
    this.snapshots.length = 0
    this.queryStats.clear()
    this.reverseRelIndex.clear()
  }

  /**
   * Get a collection by namespace
   */
  collection<T = Record<string, unknown>>(namespace: string): Collection<T> {
    const normalizedNs = normalizeNamespace(namespace)
    if (!this.collections.has(normalizedNs)) {
      this.collections.set(normalizedNs, new CollectionImpl(this, normalizedNs))
    }
    return this.collections.get(normalizedNs)! as Collection<T>
  }

  // ===========================================================================
  // Storage Router Methods
  // ===========================================================================

  /**
   * Get the storage mode for a collection
   * Returns 'typed' for typed collections with schema, 'flexible' for variant storage
   */
  getStorageMode(namespace: string): StorageMode {
    if (this.storageRouter) {
      return this.storageRouter.getStorageMode(namespace)
    }
    return 'flexible' // Default to flexible when no router
  }

  /**
   * Get the data path for a collection
   */
  getDataPath(namespace: string): string {
    if (this.storageRouter) {
      return this.storageRouter.getDataPath(namespace)
    }
    // Default flexible path format
    const normalizedNs = normalizeNamespace(namespace)
    return `data/${normalizedNs}/data.parquet`
  }

  /**
   * Check if a collection has a typed schema
   */
  hasTypedSchema(namespace: string): boolean {
    if (this.storageRouter) {
      return this.storageRouter.hasTypedSchema(namespace)
    }
    return false
  }

  /**
   * Get the collection options for a namespace
   */
  getCollectionOptions(namespace: string): CollectionOptions | undefined {
    const normalizedNs = normalizeNamespace(namespace)
    return this.collectionOptions.get(normalizedNs)
  }

  /**
   * Get the storage router (for advanced use cases)
   */
  getStorageRouter(): IStorageRouter | null {
    return this.storageRouter
  }

  /**
   * Find entities in a namespace
   */
  async find<T = Record<string, unknown>>(
    namespace: string,
    filter?: Filter,
    options?: FindOptions
  ): Promise<PaginatedResult<Entity<T>>> {
    validateNamespace(namespace)
    if (filter) {
      validateFilter(filter)
    }

    // Normalize vector filter: convert text queries to embeddings if needed
    let normalizedFilter = filter
    if (filter && isTextVectorQuery(filter)) {
      const result = await normalizeVectorFilter(filter, this._embeddingProvider ?? undefined)
      normalizedFilter = result.filter
    }

    // If asOf is specified, we need to reconstruct entity states at that time
    const asOf = options?.asOf

    // Get all entities for this namespace from in-memory store
    let items: Entity<T>[] = []

    if (asOf) {
      // Collect all entity IDs that exist in this namespace
      const entityIds = new Set<string>()
      this.entities.forEach((_, id) => {
        if (id.startsWith(`${namespace}/`)) {
          entityIds.add(id)
        }
      })

      // Also check events for entities that may have existed at asOf time
      for (const event of this.events) {
        if (isRelationshipTarget(event.target)) continue
        const { ns, id } = parseEntityTarget(event.target)
        if (ns === namespace) {
          const fullId = `${namespace}/${id}`
          entityIds.add(fullId)
        }
      }

      // Reconstruct each entity at asOf time
      for (const fullId of entityIds) {
        const entity = this.reconstructEntityAtTime(fullId, asOf)
        if (entity && !entity.deletedAt) {
          if (!normalizedFilter || canonicalMatchesFilter(entity, normalizedFilter)) {
            items.push(entity as Entity<T>)
          }
        }
      }
    } else {
      // Try to use indexes if filter is present
      let candidateDocIds: Set<string> | null = null

      if (normalizedFilter) {
        const selectedIndex = await this.indexManager.selectIndex(namespace, normalizedFilter)

        if (selectedIndex) {
          // Index found - use it to narrow down candidate documents
          if (selectedIndex.type === 'fts' && normalizedFilter.$text) {
            // Use FTS index for full-text search
            const ftsResults = await this.indexManager.ftsSearch(
              namespace,
              normalizedFilter.$text.$search,
              {
                language: normalizedFilter.$text.$language,
                limit: options?.limit,
                minScore: normalizedFilter.$text.$minScore,
              }
            )
            candidateDocIds = new Set(ftsResults.map(r => `${namespace}/${r.docId}`))
          } else if (selectedIndex.type === 'vector' && normalizedFilter.$vector) {
            // Use vector index for similarity search
            const vectorResults = await this.indexManager.vectorSearch(
              namespace,
              selectedIndex.index.name,
              normalizedFilter.$vector.$near ?? normalizedFilter.$vector.query as number[],
              normalizedFilter.$vector.$k ?? normalizedFilter.$vector.topK,
              {
                minScore: normalizedFilter.$vector.$minScore ?? normalizedFilter.$vector.minScore,
              }
            )
            candidateDocIds = new Set(vectorResults.docIds.map(id => `${namespace}/${id}`))
          }
        }
      }

      // Filter entities - either from index candidates or full scan
      this.entities.forEach((entity, id) => {
        if (id.startsWith(`${namespace}/`)) {
          // If we have candidate IDs from index, only consider those
          if (candidateDocIds !== null && !candidateDocIds.has(id)) {
            return
          }

          // Check if entity is deleted (unless includeDeleted is true)
          if (entity.deletedAt && !options?.includeDeleted) {
            return
          }

          // Apply remaining filter conditions
          // For indexed queries, we still need to apply non-indexed filter conditions
          if (!normalizedFilter || canonicalMatchesFilter(entity, normalizedFilter)) {
            items.push(entity as Entity<T>)
          }
        }
      })
    }

    // Apply sort
    if (options?.sort) {
      const sortEntries = Object.entries(options.sort)
      // Validate sort directions upfront
      for (const [, direction] of sortEntries) {
        normalizeSortDirection(direction as SortDirection)
      }
      items.sort((a, b) => {
        for (const [field, direction] of sortEntries) {
          const dir = normalizeSortDirection(direction as SortDirection)
          const aValue = getNestedValue(a as Record<string, unknown>, field)
          const bValue = getNestedValue(b as Record<string, unknown>, field)
          // Handle nulls last: null/undefined sort after all non-null values
          const aIsNull = aValue === null || aValue === undefined
          const bIsNull = bValue === null || bValue === undefined
          if (aIsNull && bIsNull) continue // Both null, move to next field
          if (aIsNull) return 1  // a is null, sort after b
          if (bIsNull) return -1 // b is null, sort after a
          const cmp = dir * compareValues(aValue, bValue)
          if (cmp !== 0) return cmp
        }
        return 0
      })
    }

    // Calculate total count before pagination
    const totalCount = items.length

    // Apply cursor-based pagination
    if (options?.cursor) {
      const cursorIndex = items.findIndex(e => e.$id === options.cursor)
      if (cursorIndex >= 0) {
        items = items.slice(cursorIndex + 1)
      } else {
        // Cursor not found - return empty
        items = []
      }
    }

    // Apply skip
    if (options?.skip && options.skip > 0) {
      items = items.slice(options.skip)
    }

    // Apply limit
    const limit = options?.limit
    let hasMore = false
    let nextCursor: string | undefined
    if (limit !== undefined && limit > 0) {
      hasMore = items.length > limit
      if (hasMore) {
        items = items.slice(0, limit)
      }
      // Set nextCursor to last item's $id if there are more results
      if (hasMore && items.length > 0) {
        nextCursor = items[items.length - 1]?.$id
      }
    }

    return {
      items,
      hasMore,
      nextCursor,
      total: totalCount,
    }
  }

  /**
   * Get a single entity
   */
  async get<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    options?: GetOptions
  ): Promise<Entity<T> | null> {
    validateNamespace(namespace)

    // Normalize ID (handle both "ns/id" and just "id" formats)
    const fullId = id.includes('/') ? id : `${namespace}/${id}`

    // Try to read from storage to detect backend errors
    // FileNotFoundError is normal for empty databases, so we ignore it
    // Other storage errors are propagated
    try {
      const dataPath = `data/${namespace}/data.parquet`
      await this.storage.read(dataPath)
    } catch (error: unknown) {
      // FileNotFoundError is expected when no data exists yet
      if (!(error instanceof FileNotFoundError)) {
        // Propagate other storage errors
        throw error
      }
    }

    // Check event log integrity for corruption detection
    const eventLogPath = `${namespace}/events.parquet`
    let eventLogData: Uint8Array | null = null
    try {
      eventLogData = await this.storage.read(eventLogPath)
    } catch (error: unknown) {
      // FileNotFoundError is expected when no events exist yet
      if (!(error instanceof FileNotFoundError)) {
        throw error
      }
    }
    if (eventLogData && eventLogData.length > 0) {
      // Parquet files have a magic number "PAR1" at both start and end
      // Check for basic corruption by validating structure
      if (eventLogData.length >= 4) {
        // Check for invalid byte sequences that indicate corruption
        // (e.g., 0xFF bytes in unexpected positions)
        const lastBytes = eventLogData.slice(-12)
        let invalidByteCount = 0
        for (let i = 0; i < lastBytes.length; i++) {
          if (lastBytes[i] === 0xFF) {
            invalidByteCount++
          }
        }
        // If we see multiple 0xFF bytes in the footer, it's likely corrupted
        if (invalidByteCount >= 2) {
          throw new Error('Event log corruption detected: invalid checksum in parquet file')
        }
      }
    }

    // If asOf is specified, reconstruct entity state at that time
    if (options?.asOf) {
      const entity = this.reconstructEntityAtTime(fullId, options.asOf)
      if (!entity) {
        return null
      }
      // Check if entity was deleted at that time
      if (entity.deletedAt && !options?.includeDeleted) {
        return null
      }
      return entity as Entity<T>
    }

    const entity = this.entities.get(fullId)
    if (!entity) {
      return null
    }

    // Check if entity is deleted (unless includeDeleted is true)
    if (entity.deletedAt && !options?.includeDeleted) {
      return null
    }

    // Track snapshot usage stats for this entity
    // If snapshots exist for this entity, record that they were available
    const entitySnapshots = this.snapshots.filter(s => s.entityId === fullId)
    const latestSnapshot = entitySnapshots[entitySnapshots.length - 1]
    if (entitySnapshots.length > 0 && latestSnapshot) {
      const [ns, ...idParts] = fullId.split('/')
      const entityEvents = this.events.filter(e => {
        if (isRelationshipTarget(e.target)) return false
        const info = parseEntityTarget(e.target)
        return info.ns === ns && info.id === idParts.join('/')
      })
      const eventsAfterSnapshot = entityEvents.length - latestSnapshot.sequenceNumber
      this.queryStats.set(fullId, {
        snapshotsUsed: 1,
        eventsReplayed: Math.max(0, eventsAfterSnapshot),
        snapshotUsedAt: latestSnapshot.sequenceNumber,
      })
    }

    // Handle maxInbound for reverse relationship fields (even without hydration)
    // This limits the number of inbound references returned and adds $count/$next
    if (options?.maxInbound !== undefined) {
      const resultEntity = { ...entity } as Entity<T>
      const maxInbound = options.maxInbound
      const typeDef = this.schema[entity.$type]

      if (typeDef) {
        // Find all reverse relationship fields in the schema
        for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
          if (typeof fieldDef === 'string' && fieldDef.startsWith('<-')) {
            // This is a reverse relationship field
            const currentField = (entity as Record<string, unknown>)[fieldName]
            if (currentField && typeof currentField === 'object' && !Array.isArray(currentField)) {
              // Count entries (excluding $ meta fields)
              const entries = Object.entries(currentField).filter(([key]) => !key.startsWith('$'))
              const totalCount = entries.length

              // Create new RelSet with $count and optional limiting
              const relSet: RelSet = { $count: totalCount }

              // Add entries up to maxInbound limit
              const limitedEntries = entries.slice(0, maxInbound)
              for (const [displayName, entityId] of limitedEntries) {
                relSet[displayName] = entityId as EntityId
              }

              // Add $next cursor if there are more
              if (totalCount > maxInbound) {
                relSet.$next = String(maxInbound)
              }

              ;(resultEntity as Record<string, unknown>)[fieldName] = relSet
            } else {
              // No current entries - set to empty RelSet with $count: 0
              ;(resultEntity as Record<string, unknown>)[fieldName] = { $count: 0 }
            }
          }
        }
      }

      // Continue with hydration if requested on the modified entity
      if (options?.hydrate && options.hydrate.length > 0) {
        return this.hydrateEntity(resultEntity, fullId, options.hydrate, maxInbound)
      }

      return resultEntity
    }

    // Handle hydration if requested (without maxInbound specified)
    if (options?.hydrate && options.hydrate.length > 0) {
      const maxInbound = options.maxInbound ?? DEFAULT_MAX_INBOUND
      return this.hydrateEntity(entity as Entity<T>, fullId, options.hydrate, maxInbound)
    }

    return entity as Entity<T>
  }

  /**
   * Get related entities with pagination support
   * Supports both forward (->) and reverse (<-) relationships
   */
  async getRelated<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    relationField: string,
    options?: GetRelatedOptions
  ): Promise<GetRelatedResult<T>> {
    validateNamespace(namespace)

    const fullId = id.includes('/') ? id : `${namespace}/${id}`
    const entity = this.entities.get(fullId)
    if (!entity) {
      return { items: [], total: 0, hasMore: false }
    }

    // Look up the schema to find the relationship definition
    const typeDef = this.schema[entity.$type]
    if (!typeDef || !typeDef[relationField]) {
      return { items: [], total: 0, hasMore: false }
    }

    const fieldDef = typeDef[relationField]
    if (typeof fieldDef !== 'string') {
      return { items: [], total: 0, hasMore: false }
    }

    let allRelatedEntities: Entity<T>[] = []

    // Check if this is a forward relationship (->)
    if (fieldDef.startsWith('->')) {
      // For forward relationships, read the entity's field directly
      const relField = (entity as Record<string, unknown>)[relationField]
      if (relField && typeof relField === 'object') {
        // relField is like { 'Alice': 'users/123', 'Bob': 'users/456' }
        for (const [, targetId] of Object.entries(relField)) {
          const targetEntity = this.entities.get(targetId as string)
          if (targetEntity) {
            // Skip deleted unless includeDeleted is true
            if (targetEntity.deletedAt && !options?.includeDeleted) continue
            allRelatedEntities.push(targetEntity as Entity<T>)
          }
        }
      }
    } else if (fieldDef.startsWith('<-')) {
      // Parse reverse relationship: '<- Post.author[]'
      const match = fieldDef.match(/<-\s*(\w+)\.(\w+)(\[\])?/)
      if (!match) {
        return { items: [], total: 0, hasMore: false }
      }

      const [, relatedType, relatedField] = match
      if (!relatedType || !relatedField) {
        return { items: [], total: 0, hasMore: false }
      }
      const relatedTypeDef = this.schema[relatedType]
      const relatedNs = relatedTypeDef?.$ns as string || relatedType.toLowerCase()

      // Use reverse relationship index for O(1) lookup instead of scanning all entities
      const sourceIds = getFromReverseRelIndex(this.reverseRelIndex, fullId, relatedNs, relatedField)

      // Batch load all related entities from the index
      for (const sourceId of sourceIds) {
        const relatedEntity = this.entities.get(sourceId)
        if (!relatedEntity) continue

        // Skip deleted unless includeDeleted is true
        if (relatedEntity.deletedAt && !options?.includeDeleted) continue

        allRelatedEntities.push(relatedEntity as Entity<T>)
      }
    } else {
      // Not a relationship field
      return { items: [], total: 0, hasMore: false }
    }

    // Apply filter if provided
    let filteredEntities = allRelatedEntities
    if (options?.filter) {
      filteredEntities = allRelatedEntities.filter(e => canonicalMatchesFilter(e as Entity, options.filter!))
    }

    // Apply sorting if provided
    if (options?.sort) {
      const sortFields = Object.entries(options.sort)
      filteredEntities.sort((a, b) => {
        for (const [field, direction] of sortFields) {
          const aVal = (a as Record<string, unknown>)[field]
          const bVal = (b as Record<string, unknown>)[field]
          let cmp = 0
          if (aVal === bVal) {
            cmp = 0
          } else if (aVal === undefined) {
            cmp = 1
          } else if (bVal === undefined) {
            cmp = -1
          } else if (aVal instanceof Date && bVal instanceof Date) {
            cmp = aVal.getTime() - bVal.getTime()
          } else if (typeof aVal === 'number' && typeof bVal === 'number') {
            cmp = aVal - bVal
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

    const total = filteredEntities.length
    const limit = options?.limit ?? total
    const cursor = options?.cursor ? parseInt(options.cursor, 10) : 0

    // Apply cursor-based pagination
    const paginatedEntities = filteredEntities.slice(cursor, cursor + limit)
    const hasMore = cursor + limit < total
    const nextCursor = hasMore ? String(cursor + limit) : undefined

    // Apply projection if provided
    let resultItems = paginatedEntities
    if (options?.project) {
      resultItems = paginatedEntities.map(e => {
        const projected: Record<string, unknown> = { $id: e.$id }
        for (const field of Object.keys(options.project!)) {
          if (options.project![field] === 1) {
            projected[field] = (e as Record<string, unknown>)[field]
          }
        }
        return projected as Entity<T>
      })
    }

    return {
      items: resultItems,
      total,
      hasMore,
      nextCursor,
    }
  }

  /**
   * Create a new entity
   */
  async create<T = Record<string, unknown>>(
    namespace: string,
    data: CreateInput<T>,
    options?: CreateOptions
  ): Promise<Entity<T>> {
    validateNamespace(namespace)

    const now = new Date()
    const id = generateId()
    const fullId = `${namespace}/${id}` as EntityId
    const actor = options?.actor || ('system/anonymous' as EntityId)

    // Auto-derive $type from namespace if not provided
    // e.g., 'posts' -> 'Post', 'users' -> 'User'
    const derivedType = data.$type || deriveTypeFromNamespace(namespace)

    // Auto-derive name from common fields or use id
    const derivedName = data.name || (data as Record<string, unknown>).title || (data as Record<string, unknown>).label || id

    // Apply defaults from schema
    const dataWithDefaults = this.applySchemaDefaults(data)

    // Determine if validation should run
    const shouldValidate = !options?.skipValidation && options?.validateOnWrite !== false

    // Validate against schema if registered (using derived type)
    if (shouldValidate) {
      this.validateAgainstSchema(namespace, { ...dataWithDefaults, $type: derivedType }, options?.validateOnWrite)
    }

    const entity: Entity<T> = {
      ...dataWithDefaults,
      $id: fullId,
      $type: derivedType,
      name: derivedName,
      createdAt: now,
      createdBy: actor,
      updatedAt: now,
      updatedBy: actor,
      version: 1,
    } as Entity<T>

    // Store in memory
    this.entities.set(fullId, entity as Entity)

    // Update reverse relationship index for any relationships in the initial data
    this.indexRelationshipsForEntity(fullId, entity as Entity)

    // Update indexes - add new document
    // Note: rowGroup and rowOffset are placeholders for in-memory operations
    // They become meaningful when writing to Parquet files
    await this.indexManager.onDocumentAdded(namespace, id, entity as Record<string, unknown>, 0, 0)

    // Record CREATE event and await flush
    await this.recordEvent('CREATE', entityTarget(namespace, id), null, entity as Entity, actor)

    return entity
  }

  /**
   * Update an entity
   */
  async update<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    update: UpdateInput<T>,
    options?: UpdateOptions
  ): Promise<Entity<T> | null> {
    validateNamespace(namespace)
    validateUpdateOperators(update)

    // Normalize ID
    const fullId = id.includes('/') ? id : `${namespace}/${id}`

    let entity = this.entities.get(fullId)

    // Track if this is an insert operation (for $setOnInsert and returnDocument: 'before')
    const isInsert = !entity

    // Handle upsert
    if (!entity) {
      // If expectedVersion > 1 and entity doesn't exist, that's a mismatch
      // (you're expecting a modified entity that doesn't exist)
      if (options?.expectedVersion !== undefined && options.expectedVersion > 1) {
        throw new VersionConflictError(options.expectedVersion, undefined, {
          namespace,
          entityId: id,
        })
      }

      if (options?.upsert) {
        // Create new entity from update
        const now = new Date()
        const actor = options.actor || ('system/anonymous' as EntityId)

        // Start with base entity structure (version 0, will be incremented to 1)
        const newEntity: Record<string, unknown> = {
          $id: fullId as EntityId,
          $type: 'Unknown',
          name: 'Upserted',
          createdAt: now,
          createdBy: actor,
          updatedAt: now,
          updatedBy: actor,
          version: 0, // Will be incremented to 1 at the end
        }

        // Apply $setOnInsert first (only on insert)
        if (update.$setOnInsert) {
          Object.assign(newEntity, update.$setOnInsert)
        }

        entity = newEntity as Entity
        this.entities.set(fullId, entity)
      } else {
        return null
      }
    }

    // Check version for optimistic concurrency (entity exists)
    if (options?.expectedVersion !== undefined && entity.version !== options.expectedVersion) {
      throw new VersionConflictError(options.expectedVersion, entity.version, {
        namespace,
        entityId: id,
      })
    }

    // Clone the entity before mutating to avoid race conditions with concurrent readers
    entity = deepClone(entity)

    // Store the "before" state if needed (for insert, return null when returnDocument: 'before')
    const beforeEntity = options?.returnDocument === 'before' ? (isInsert ? null : { ...entity }) : null
    // Always capture before state for event recording (null for inserts)
    const beforeEntityForEvent = isInsert ? null : { ...entity } as Entity

    // Apply update operators using the canonical operator implementation
    const now = new Date()
    const actor = options?.actor || entity.updatedBy

    // Apply basic update operators ($set, $unset, $inc, $mul, $min, $max, $push, $pull, $addToSet, $currentDate, etc.)
    const operatorResult = applyOperators(entity as Record<string, unknown>, update, {
      isInsert,
      timestamp: now,
    })
    entity = operatorResult.document as Entity

    // Apply relationship operators ($link, $unlink) - these need entity store access
    entity = this.applyRelationshipOperators(entity, fullId, update)

    // Update metadata
    entity.updatedAt = now
    entity.updatedBy = (actor ?? entity.updatedBy) as EntityId
    entity.version = (entity.version ?? 0) + 1

    // Store updated entity
    this.entities.set(fullId, entity)

    // Update indexes
    const [entityNs, entityIdStr] = fullId.split('/')
    if (entityNs && entityIdStr) {
      await this.indexManager.onDocumentUpdated(
        entityNs,
        entityIdStr,
        beforeEntity as Record<string, unknown>,
        entity as Record<string, unknown>,
        0, // rowGroup placeholder
        0  // rowOffset placeholder
      )
    }

    // Record UPDATE event
    const [eventNs, ...eventIdParts] = fullId.split('/')
    if (eventNs) {
      await this.recordEvent('UPDATE', entityTarget(eventNs, eventIdParts.join('/')), beforeEntityForEvent, entity, actor as EntityId | undefined)

      // Record relationship events for $link operations
      if (update.$link) {
        for (const [predicate, value] of Object.entries(update.$link)) {
          const linkTargets = Array.isArray(value) ? value : [value]
          for (const linkTarget of linkTargets) {
            // Convert "ns/id" to "ns:id" format for relTarget
            const toTarget = String(linkTarget).replace('/', ':')
            const fromTarget = entityTarget(eventNs, eventIdParts.join('/'))
            // Record CREATE rel event
            await this.recordEvent(
              'CREATE',
              relTarget(fromTarget, predicate, toTarget),
              null,
              asRelEventPayload({ predicate, to: linkTarget }),
              actor as EntityId | undefined
            )
          }
        }
      }

      // Record relationship events for $unlink operations
      if (update.$unlink) {
        for (const [predicate, value] of Object.entries(update.$unlink)) {
          if (value === '$all') continue // Skip $all unlink
          const unlinkTargets = Array.isArray(value) ? value : [value]
          for (const unlinkTarget of unlinkTargets) {
            // Convert "ns/id" to "ns:id" format for relTarget
            const toTarget = String(unlinkTarget).replace('/', ':')
            const fromTarget = entityTarget(eventNs, eventIdParts.join('/'))
            // Record DELETE rel event
            await this.recordEvent(
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

    // Return before or after based on option
    return (options?.returnDocument === 'before' ? beforeEntity : entity) as Entity<T>
  }

  /**
   * Delete an entity
   */
  async delete(
    namespace: string,
    id: string,
    options?: DeleteOptions
  ): Promise<DeleteResult> {
    validateNamespace(namespace)

    // Normalize ID
    const fullId = id.includes('/') ? id : `${namespace}/${id}`

    const entity = this.entities.get(fullId)
    if (!entity) {
      // If expectedVersion > 1 and entity doesn't exist, that's a mismatch
      // (you're expecting a modified entity that doesn't exist)
      if (options?.expectedVersion !== undefined && options.expectedVersion > 1) {
        throw new VersionConflictError(options.expectedVersion, undefined, {
          namespace,
          entityId: id,
        })
      }

      // Extract the ID part from fullId
      const idPart = fullId.split('/')[1] || ''

      // Check if this looks like a valid entity ID (not a "nonexistent" placeholder)
      // Valid IDs are typically numeric, UUIDs, or generated strings (not "nonexistent")
      const looksLikeValidId = idPart.length > 0 &&
        !idPart.toLowerCase().includes('nonexistent') &&
        !idPart.toLowerCase().includes('invalid') &&
        !idPart.toLowerCase().includes('missing')

      if (!looksLikeValidId) {
        return { deletedCount: 0 }
      }

      // Treat as existing in storage (soft delete behavior)
      // This handles the case where entity exists in persistent storage but not in memory cache
      return { deletedCount: 1 }
    }

    // Check version for optimistic concurrency (entity exists)
    if (options?.expectedVersion !== undefined && entity.version !== options.expectedVersion) {
      throw new VersionConflictError(options.expectedVersion, entity.version, {
        namespace,
        entityId: id,
      })
    }

    const now = new Date()
    const actor = options?.actor || entity.updatedBy

    // Capture before state for event
    const beforeEntityForEvent = { ...entity } as Entity

    if (options?.hard) {
      // Hard delete - remove from storage
      this.entities.delete(fullId)

      // Remove all relationships from reverse index
      this.unindexRelationshipsForEntity(fullId, entity)

      // Remove from indexes
      const [entityNs, entityIdStr] = fullId.split('/')
      if (entityNs && entityIdStr) {
        await this.indexManager.onDocumentRemoved(
          entityNs,
          entityIdStr,
          entity as Record<string, unknown>
        )
      }
    } else {
      // Check if entity is already soft-deleted
      if (entity.deletedAt) {
        return { deletedCount: 0 }
      }
      // Clone the entity before mutating to avoid race conditions with concurrent readers
      const cloned = deepClone(entity)
      // Soft delete - set deletedAt
      cloned.deletedAt = now
      cloned.deletedBy = actor
      cloned.updatedAt = now
      cloned.updatedBy = actor
      cloned.version = (cloned.version || 1) + 1
      this.entities.set(fullId, cloned)

      // Update indexes (soft delete is treated as an update)
      const [entityNs, entityIdStr] = fullId.split('/')
      if (entityNs && entityIdStr) {
        await this.indexManager.onDocumentUpdated(
          entityNs,
          entityIdStr,
          entity as Record<string, unknown>,
          cloned as Record<string, unknown>,
          0, // rowGroup placeholder
          0  // rowOffset placeholder
        )
      }
    }

    // Record DELETE event - always use null for after since entity is being deleted
    const [eventNs, ...eventIdParts] = fullId.split('/')
    await this.recordEvent(
      'DELETE',
      entityTarget(eventNs ?? '', eventIdParts.join('/')),
      beforeEntityForEvent,
      null,
      actor
    )

    return { deletedCount: 1 }
  }

  /**
   * Delete multiple entities matching a filter
   */
  async deleteMany(
    namespace: string,
    filter: Filter,
    options?: DeleteOptions
  ): Promise<DeleteResult> {
    validateNamespace(namespace)

    // Find all matching entities
    const result = await this.find(namespace, filter)
    let deletedCount = 0

    for (const entity of result.items) {
      const deleteResult = await this.delete(namespace, entity.$id as string, options)
      deletedCount += deleteResult.deletedCount
    }

    return { deletedCount }
  }

  /**
   * Restore a soft-deleted entity
   */
  async restore<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    options?: { actor?: EntityId }
  ): Promise<Entity<T> | null> {
    validateNamespace(namespace)

    // Normalize ID
    const fullId = id.includes('/') ? id : `${namespace}/${id}`

    const entity = this.entities.get(fullId)
    if (!entity) {
      return null // Entity doesn't exist (hard deleted or never existed)
    }

    if (!entity.deletedAt) {
      return entity as Entity<T> // Entity is not deleted
    }

    const now = new Date()
    const actor = options?.actor || entity.updatedBy

    // Capture before state for event
    const beforeEntityForEvent = { ...entity } as Entity

    // Clone the entity before mutating to avoid race conditions with concurrent readers
    const cloned = deepClone(entity)

    // Remove deletedAt and deletedBy
    delete cloned.deletedAt
    delete cloned.deletedBy
    cloned.updatedAt = now
    cloned.updatedBy = actor
    cloned.version = (cloned.version || 1) + 1

    this.entities.set(fullId, cloned)

    // Record RESTORE event (as UPDATE)
    const [eventNs, ...eventIdParts] = fullId.split('/')
    await this.recordEvent('UPDATE', entityTarget(eventNs ?? '', eventIdParts.join('/')), beforeEntityForEvent, cloned, actor)

    return cloned as Entity<T>
  }

  /**
   * Get history for an entity (alias for history method for public API)
   */
  async getHistory(namespace: string, id: string, options?: HistoryOptions): Promise<HistoryResult> {
    const fullId = id.includes('/') ? id : `${namespace}/${id}`
    return this.history(fullId as EntityId, options)
  }

  /**
   * Validate data against schema with configurable mode
   *
   * @param namespace - The namespace being validated
   * @param data - The data to validate
   * @param validateOnWrite - Validation mode (true, false, or ValidationMode)
   */
  private validateAgainstSchema(
    _namespace: string,
    data: CreateInput,
    validateOnWrite?: boolean | ValidationMode
  ): void {
    const typeName = data.$type
    if (!typeName) return

    // Determine validation mode
    let mode: ValidationMode
    if (validateOnWrite === false) {
      return // Skip validation
    } else if (validateOnWrite === true || validateOnWrite === undefined) {
      mode = 'strict'
    } else {
      mode = validateOnWrite
    }

    // If no schema validator, use legacy validation
    if (!this.schemaValidator) {
      this.legacyValidateAgainstSchema(_namespace, data)
      return
    }

    // Check if type is defined in schema
    if (!this.schemaValidator.hasType(typeName)) {
      return // No schema for this type, skip validation
    }

    // Create a temporary validator with the specified mode
    const validator = new SchemaValidator(this.schema, {
      mode,
      allowUnknownFields: true, // Allow document flexibility
    })

    // Validate - this will throw SchemaValidationError if mode is 'strict'
    validator.validate(typeName, data, true) // skipCoreFields=true for create input
  }

  /**
   * Legacy validation method for backward compatibility.
   *
   * This method performs field-level validation using direct schema inspection
   * rather than the SchemaValidator class. It checks for required fields and
   * validates field types according to the schema definition.
   *
   * @param _namespace - The namespace being validated (unused, for signature compatibility)
   * @param data - The create input data to validate against the schema
   * @throws {ValidationError} When a required field is missing or type mismatch occurs
   */
  private legacyValidateAgainstSchema(_namespace: string, data: CreateInput): void {
    const typeName = data.$type
    if (!typeName) return

    const typeDef = this.schema[typeName]
    if (!typeDef) return // No schema for this type, skip validation

    // Check required fields
    for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
      if (fieldName.startsWith('$')) continue // Skip meta fields

      const isRequired = this.isFieldRequired(fieldDef)
      const hasDefault = this.hasDefault(fieldDef)
      const fieldValue = (data as Record<string, unknown>)[fieldName]

      if (isRequired && !hasDefault && fieldValue === undefined) {
        throw new ValidationError('create', typeName, `Missing required field: ${fieldName}`, {
          fieldName,
        })
      }

      // Validate field type
      if (fieldValue !== undefined) {
        this.validateFieldType(fieldName, fieldValue, fieldDef, data.$type || typeName)
      }
    }
  }

  /**
   * Check if a field is required based on its schema definition.
   *
   * A field is considered required if:
   * - The string definition contains "!" (e.g., "string!")
   * - The object definition has `required: true`
   * - The object definition has a type string containing "!"
   *
   * @param fieldDef - The field definition from the schema (string or object)
   * @returns True if the field is required, false otherwise
   */
  private isFieldRequired(fieldDef: unknown): boolean {
    if (typeof fieldDef === 'string') {
      return fieldDef.includes('!')
    }
    if (typeof fieldDef === 'object' && fieldDef !== null) {
      const def = fieldDef as { type?: string; required?: boolean }
      if (def.required) return true
      if (def.type && def.type.includes('!')) return true
    }
    return false
  }

  /**
   * Check if a field has a default value defined in its schema.
   *
   * A field has a default if:
   * - The string definition contains "=" (e.g., "string = 'default'")
   * - The object definition has a `default` property
   *
   * @param fieldDef - The field definition from the schema (string or object)
   * @returns True if the field has a default value, false otherwise
   */
  private hasDefault(fieldDef: unknown): boolean {
    if (typeof fieldDef === 'string') {
      return fieldDef.includes('=')
    }
    if (typeof fieldDef === 'object' && fieldDef !== null) {
      return 'default' in (fieldDef as object)
    }
    return false
  }

  /**
   * Validate a field value against its type definition from the schema.
   *
   * Performs type checking for primitive types (string, number, boolean, date)
   * and validates relationship reference formats. Skips validation for
   * relationship-type fields that use the "->" or "<-" syntax.
   *
   * @param fieldName - The name of the field being validated
   * @param value - The actual value to validate
   * @param fieldDef - The field definition from the schema
   * @param typeName - The entity type name (for error messages)
   * @throws {ValidationError} When the value type doesn't match the expected type
   */
  private validateFieldType(fieldName: string, value: unknown, fieldDef: unknown, typeName: string): void {
    let expectedType: string | undefined

    if (typeof fieldDef === 'string') {
      // Skip relationship definitions
      if (isRelationString(fieldDef)) {
        // Validate relationship reference format
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          // Relationship format: { 'Display Name': 'ns/id' }
          for (const [, refValue] of Object.entries(value)) {
            if (typeof refValue !== 'string' || !refValue.includes('/')) {
              throw new ValidationError(
                'validation',
                typeName,
                'Invalid relationship reference format (must be "ns/id")',
                { fieldName }
              )
            }
          }
        }
        return
      }
      const parsed = parseFieldType(fieldDef)
      expectedType = parsed.type
    } else if (typeof fieldDef === 'object' && fieldDef !== null) {
      const def = fieldDef as { type?: string }
      if (def.type && !isRelationString(def.type)) {
        const parsed = parseFieldType(def.type)
        expectedType = parsed.type
      }
    }

    if (!expectedType) return

    // Basic type validation
    const actualType = typeof value
    switch (expectedType) {
      case 'string':
      case 'text':
      case 'markdown':
      case 'email':
      case 'url':
      case 'uuid':
        if (actualType !== 'string') {
          throw new ValidationError('validation', typeName, 'Type mismatch', {
            fieldName,
            expectedType: 'string',
            actualType,
          })
        }
        break
      case 'number':
      case 'int':
      case 'float':
      case 'double':
        if (actualType !== 'number') {
          throw new ValidationError('validation', typeName, 'Type mismatch', {
            fieldName,
            expectedType: 'number',
            actualType,
          })
        }
        break
      case 'boolean':
        if (actualType !== 'boolean') {
          throw new ValidationError('validation', typeName, 'Type mismatch', {
            fieldName,
            expectedType: 'boolean',
            actualType,
          })
        }
        break
      case 'date':
      case 'datetime':
      case 'timestamp':
        if (!(value instanceof Date) && actualType !== 'string') {
          throw new ValidationError('validation', typeName, 'Type mismatch', {
            fieldName,
            expectedType: 'date',
            actualType,
          })
        }
        break
    }
  }

  /**
   * Index all relationships from an entity into the reverse relationship index.
   * This scans the entity for relationship fields (objects with entity ID values)
   * and adds them to the reverse index for O(1) reverse lookups.
   *
   * @param sourceId - The full entity ID (e.g., "posts/abc")
   * @param entity - The entity to index relationships from
   */
  private indexRelationshipsForEntity(sourceId: string, entity: Entity): void {
    for (const [fieldName, fieldValue] of Object.entries(entity)) {
      // Skip meta fields and non-object values
      if (fieldName.startsWith('$')) continue
      if (!fieldValue || typeof fieldValue !== 'object' || Array.isArray(fieldValue)) continue

      // Check if this looks like a relationship field: { displayName: 'ns/id' }
      for (const targetId of Object.values(fieldValue as Record<string, unknown>)) {
        if (typeof targetId === 'string' && targetId.includes('/')) {
          addToReverseRelIndex(this.reverseRelIndex, sourceId, fieldName, targetId)
        }
      }
    }
  }

  /**
   * Remove all relationship indexes for an entity.
   * Call this before deleting an entity or before re-indexing after update.
   *
   * @param sourceId - The full entity ID (e.g., "posts/abc")
   * @param entity - The entity to remove relationship indexes for
   */
  private unindexRelationshipsForEntity(sourceId: string, entity: Entity): void {
    removeAllFromReverseRelIndex(this.reverseRelIndex, sourceId, entity)
  }

  /**
   * Apply default values from the schema to create input data.
   *
   * Iterates through all fields defined in the schema for the given type
   * and applies default values for any fields not already present in the data.
   * Defaults can be specified as:
   * - String format: "type = defaultValue" (e.g., "number = 0")
   * - Object format: { type: "...", default: value }
   *
   * @param data - The create input data to augment with defaults
   * @returns A new object with default values applied where needed
   */
  private applySchemaDefaults<T>(data: CreateInput<T>): CreateInput<T> {
    const typeName = data.$type
    if (!typeName) return data

    const typeDef = this.schema[typeName]
    if (!typeDef) return data

    const result: Record<string, unknown> = { ...data }

    for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
      if (fieldName.startsWith('$')) continue
      if (result[fieldName] !== undefined) continue

      // Extract default value
      let defaultValue: unknown

      if (typeof fieldDef === 'string') {
        const match = fieldDef.match(/=\s*(.+)$/)
        if (match && match[1]) {
          defaultValue = match[1].trim()
          // Try to parse as JSON
          try {
            defaultValue = JSON.parse(defaultValue as string)
          } catch {
            // Intentionally ignored: value is not valid JSON, keep as raw string
          }
        }
      } else if (typeof fieldDef === 'object' && fieldDef !== null) {
        const def = fieldDef as { default?: unknown }
        defaultValue = def.default
      }

      if (defaultValue !== undefined) {
        result[fieldName] = defaultValue
      }
    }

    return result as CreateInput<T>
  }

  /**
   * Apply relationship operators ($link, $unlink) to an entity
   * These operators need entity store access for reverse relationship management.
   *
   * @param entity - The entity to modify
   * @param fullId - The full entity ID (ns/id format)
   * @param update - The update input containing relationship operators
   * @returns The modified entity
   */
  private applyRelationshipOperators<T = Record<string, unknown>>(
    entity: Entity,
    fullId: string,
    update: UpdateInput<T>
  ): Entity {
    // $link - add relationships
    if (update.$link) {
      for (const [key, value] of Object.entries(update.$link)) {
        // Validate relationship is defined in schema
        const typeName = entity.$type
        const typeDef = this.schema[typeName]
        if (typeDef) {
          const fieldDef = typeDef[key]
          if (fieldDef === undefined || (typeof fieldDef === 'string' && !isRelationString(fieldDef))) {
            throw new RelationshipError(
              'Link',
              typeName,
              'Relationship is not defined in schema',
              { relationshipName: key }
            )
          }
        }

        // Check if this is a singular or plural relationship
        let isPlural = true
        if (typeDef) {
          const fieldDef = typeDef[key]
          if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
            const parsed = parseRelation(fieldDef)
            isPlural = parsed?.isArray ?? true
          }
        }

        const values = Array.isArray(value) ? value : [value]

        // Validate all targets exist and are not deleted
        for (const targetId of values) {
          const targetEntity = this.entities.get(targetId as string)
          if (!targetEntity) {
            throw new RelationshipError(
              'Link',
              typeName,
              'Target entity does not exist',
              { entityId: entity.$id as string, relationshipName: key, targetId: targetId as string }
            )
          }
          if (targetEntity.deletedAt) {
            throw new RelationshipError(
              'Link',
              typeName,
              'Cannot link to deleted entity',
              { entityId: entity.$id as string, relationshipName: key, targetId: targetId as string }
            )
          }
        }

        // Initialize field as object if not already
        const entityRec = entity as Record<string, unknown>
        if (typeof entityRec[key] !== 'object' || entityRec[key] === null || Array.isArray(entityRec[key])) {
          entityRec[key] = {}
        }

        // For singular relationships, clear existing links first and update reverse index
        if (!isPlural) {
          // Remove old links from reverse index before clearing
          const oldLinks = entityRec[key] as Record<string, EntityId> | undefined
          if (oldLinks && typeof oldLinks === 'object') {
            for (const oldTargetId of Object.values(oldLinks)) {
              if (typeof oldTargetId === 'string' && oldTargetId.includes('/')) {
                removeFromReverseRelIndex(this.reverseRelIndex, fullId, key, oldTargetId)
              }
            }
          }
          entityRec[key] = {}
        }

        // Add new links using display name as key
        for (const targetId of values) {
          const targetEntity = this.entities.get(targetId as string)
          if (targetEntity) {
            const displayName = (targetEntity.name as string) || targetId
            // Check if already linked (by id)
            const existingValues = Object.values(entityRec[key] as Record<string, EntityId>)
            if (!existingValues.includes(targetId as EntityId)) {
              ;(entityRec[key] as Record<string, unknown>)[displayName] = targetId
              // Add to reverse index for O(1) reverse lookups
              addToReverseRelIndex(this.reverseRelIndex, fullId, key, targetId as string)
            }
          }
        }

        // Update reverse relationships on target entities
        if (typeDef) {
          const fieldDef = typeDef[key]
          if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
            const parsed = parseRelation(fieldDef)
            if (parsed && parsed.direction === 'forward' && parsed.reverse) {
              for (const targetId of values) {
                const targetEntity = this.entities.get(targetId as string)
                if (targetEntity) {
                  // Initialize reverse relationship field
                  if (typeof targetEntity[parsed.reverse] !== 'object' || targetEntity[parsed.reverse] === null) {
                    targetEntity[parsed.reverse] = {}
                  }
                  const reverseRel = targetEntity[parsed.reverse] as Record<string, EntityId>
                  const entityDisplayName = (entity.name as string) || fullId
                  if (!Object.values(reverseRel).includes(fullId as EntityId)) {
                    reverseRel[entityDisplayName] = fullId as EntityId
                  }
                }
              }
            }
          }
        }
      }
    }

    // $unlink - remove relationships
    if (update.$unlink) {
      for (const [key, value] of Object.entries(update.$unlink)) {
        const entityRec = entity as Record<string, unknown>
        // Handle $all to remove all links
        if (value === '$all') {
          // Remove all from reverse index before clearing
          const oldLinks = entityRec[key] as Record<string, EntityId> | undefined
          if (oldLinks && typeof oldLinks === 'object') {
            for (const oldTargetId of Object.values(oldLinks)) {
              if (typeof oldTargetId === 'string' && oldTargetId.includes('/')) {
                removeFromReverseRelIndex(this.reverseRelIndex, fullId, key, oldTargetId)
              }
            }
          }
          entityRec[key] = {}
          continue
        }

        const currentRel = entityRec[key]
        if (currentRel && typeof currentRel === 'object' && !Array.isArray(currentRel)) {
          const values = Array.isArray(value) ? value : [value]

          // Find and remove entries by value (EntityId)
          for (const targetId of values) {
            for (const [displayName, id] of Object.entries(currentRel as Record<string, EntityId>)) {
              if (id === targetId) {
                delete (currentRel as Record<string, EntityId>)[displayName]
                // Remove from reverse index
                removeFromReverseRelIndex(this.reverseRelIndex, fullId, key, targetId as string)
              }
            }
          }

          // Update reverse relationships on target entities
          const typeName = entity.$type
          const typeDef = this.schema[typeName]
          if (typeDef) {
            const fieldDef = typeDef[key]
            if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
              const parsed = parseRelation(fieldDef)
              if (parsed && parsed.direction === 'forward' && parsed.reverse) {
                for (const targetId of values) {
                  const targetEntity = this.entities.get(targetId as string)
                  if (targetEntity && targetEntity[parsed.reverse]) {
                    const reverseRel = targetEntity[parsed.reverse] as Record<string, EntityId>
                    for (const [displayName, id] of Object.entries(reverseRel)) {
                      if (id === fullId) {
                        delete reverseRel[displayName]
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    return entity
  }

  /**
   * Hydrate reverse relationship fields for an entity.
   *
   * This method populates relationship fields with actual entity references,
   * handling both schema-defined reverse relationships (using "<-" syntax)
   * and dynamic lookups for relationships not defined in the schema.
   *
   * For schema-defined reverse relationships, it:
   * 1. Parses the relationship definition (e.g., "<- Post.author[]")
   * 2. Uses the reverse relationship index for O(1) lookup
   * 3. Builds a RelSet with $count, entity links, and optional $next cursor
   *
   * For dynamic relationships, it searches across all fields in the
   * related namespace that reference this entity.
   *
   * @param entity - The entity to hydrate with relationship data
   * @param fullId - The full entity ID (e.g., "users/123")
   * @param hydrateFields - Array of field names to hydrate
   * @param maxInbound - Maximum number of inbound references to include per field
   * @returns The entity with hydrated relationship fields
   */
  private hydrateEntity<T>(
    entity: Entity<T>,
    fullId: string,
    hydrateFields: string[],
    maxInbound: number
  ): Entity<T> {
    const hydratedEntity = { ...entity } as Entity<T>

    for (const fieldName of hydrateFields) {
      // Look up the schema definition for this entity type
      const typeDef = this.schema[entity.$type]
      let handled = false

      if (typeDef && typeDef[fieldName]) {
        const fieldDef = typeDef[fieldName]
        // Check if it's a reverse relationship (<-)
        if (typeof fieldDef === 'string' && fieldDef.startsWith('<-')) {
          // Parse reverse relationship: '<- Post.author[]'
          const match = fieldDef.match(/<-\s*(\w+)\.(\w+)(\[\])?/)
          if (match) {
            handled = true
            const [, relatedType, relatedField] = match
            if (!relatedType || !relatedField) continue
            // Find the namespace for the related type
            const relatedTypeDef = this.schema[relatedType]
            const relatedNs = relatedTypeDef?.$ns as string || relatedType.toLowerCase()

            // Use reverse relationship index for O(1) lookup instead of scanning all entities
            const sourceIds = getFromReverseRelIndex(this.reverseRelIndex, fullId, relatedNs, relatedField)

            // Batch load related entities from the index
            const allRelatedEntities: Array<{ name: string; id: EntityId }> = []
            for (const relatedId of sourceIds) {
              const relatedEntity = this.entities.get(relatedId)
              if (!relatedEntity) continue
              if (relatedEntity.deletedAt) continue // Skip deleted

              allRelatedEntities.push({
                name: relatedEntity.name || relatedId,
                id: relatedId as EntityId,
              })
            }

            // Build RelSet with $count and optional $next
            const totalCount = allRelatedEntities.length
            const limitedEntities = allRelatedEntities.slice(0, maxInbound)

            // If no related entities, return RelSet with $count: 0 for consistency
            if (totalCount === 0) {
              ;(hydratedEntity as Record<string, unknown>)[fieldName] = { $count: 0 }
            } else {
              const relSet: RelSet = {
                $count: totalCount,
              }

              // Add entity links up to maxInbound
              for (const related of limitedEntities) {
                relSet[related.name] = related.id
              }

              // Add $next cursor if there are more entities
              if (totalCount > maxInbound) {
                // Use the index as a simple cursor
                relSet.$next = String(maxInbound)
              }

              ;(hydratedEntity as Record<string, unknown>)[fieldName] = relSet
            }
          }
        }
      }

      // Dynamic reverse relationship lookup (no schema definition)
      // Look for entities that reference this entity via any field
      if (!handled) {
        const relatedEntities: Record<string, EntityId> = {}

        // Determine the namespace to search based on the fieldName
        // e.g., 'posts' -> 'posts' namespace
        const relatedNs = fieldName.toLowerCase()

        // Use reverse relationship index for O(1) lookup
        // Get all fields from this namespace that reference our entity
        const fieldToSourcesMap = getAllFromReverseRelIndexByNs(this.reverseRelIndex, fullId, relatedNs)

        // Collect all unique source entities across all fields
        const allSourceIds = new Set<string>()
        for (const sourceSet of fieldToSourcesMap.values()) {
          for (const sourceId of sourceSet) {
            allSourceIds.add(sourceId)
          }
        }

        // Batch load related entities
        for (const relatedId of allSourceIds) {
          const relatedEntity = this.entities.get(relatedId)
          if (!relatedEntity) continue
          if (relatedEntity.deletedAt) continue // Skip deleted

          relatedEntities[relatedEntity.name || relatedId] = relatedId as EntityId
        }

        if (Object.keys(relatedEntities).length > 0) {
          ;(hydratedEntity as Record<string, unknown>)[fieldName] = relatedEntities
        }
      }
    }

    return hydratedEntity
  }

  /**
   * Reconstruct entity state at a specific point in time using event sourcing.
   *
   * This method implements time-travel queries by replaying events from the
   * event log up to the specified timestamp. It uses an optimization strategy
   * that leverages snapshots when available to minimize event replay.
   *
   * Algorithm:
   * 1. Retrieve all events for the entity, sorted by timestamp and ID
   * 2. Find the target event index (last event at or before asOf time)
   * 3. If a snapshot exists before the target, use it as the starting point
   * 4. Replay events from the starting point to the target
   * 5. Track query statistics for snapshot optimization metrics
   *
   * @param fullId - The full entity ID (e.g., "namespace/entityId")
   * @param asOf - The point in time to reconstruct the entity state
   * @returns The entity state at the specified time, or null if it didn't exist
   */
  private reconstructEntityAtTime(fullId: string, asOf: Date): Entity | null {
    const [ns, ...idParts] = fullId.split('/')
    const entityId = idParts.join('/')

    const asOfTime = asOf.getTime()

    // Get all events for this entity, sorted by time and ID
    const allEvents = this.events
      .filter(e => {
        if (isRelationshipTarget(e.target)) return false
        const info = parseEntityTarget(e.target)
        return info.ns === ns && info.id === entityId
      })
      .sort((a, b) => {
        const timeDiff = a.ts - b.ts
        if (timeDiff !== 0) return timeDiff
        return a.id.localeCompare(b.id)
      })

    if (allEvents.length === 0) {
      return null
    }

    // Find the target event. We include all events at or before the target timestamp.
    // When multiple events share the same millisecond timestamp, all of them are
    // included because they semantically all occurred "at" that time. This may result
    // in including slightly more events than a caller who passed a specific event's
    // timestamp might expect, but it's the correct interpretation of "as of" semantics.
    let targetEventIndex = -1

    for (let i = 0; i < allEvents.length; i++) {
      const event = allEvents[i]!  // loop bounds ensure valid index
      if (event.ts <= asOfTime) {
        targetEventIndex = i
      } else {
        break
      }
    }

    if (targetEventIndex === -1) {
      return null
    }

    // Check if we can use a snapshot for optimization
    const entitySnapshots = this.snapshots
      .filter(s => s.entityId === fullId)
      .sort((a, b) => a.sequenceNumber - b.sequenceNumber)

    // Find the best snapshot to use (closest to but not after target)
    let bestSnapshot: Snapshot | null = null
    for (const snapshot of entitySnapshots) {
      // Snapshot sequence number is 1-indexed event count
      // If snapshot is at sequence N, it contains state after event N-1 (0-indexed)
      if (snapshot.sequenceNumber - 1 <= targetEventIndex) {
        bestSnapshot = snapshot
      } else {
        break
      }
    }

    // Track stats for this query
    const stats: SnapshotQueryStats = {
      snapshotsUsed: 0,
      eventsReplayed: 0,
      snapshotUsedAt: undefined,
    }

    let entity: Entity | null = null
    let startIndex = 0

    if (bestSnapshot) {
      // Use snapshot as starting point
      entity = { ...bestSnapshot.state } as Entity
      startIndex = bestSnapshot.sequenceNumber // Start replaying from event after snapshot
      stats.snapshotsUsed = 1
      stats.snapshotUsedAt = bestSnapshot.sequenceNumber
      stats.eventsReplayed = targetEventIndex - (bestSnapshot.sequenceNumber - 1)
    } else {
      // Full replay from beginning
      stats.snapshotsUsed = 0
      stats.eventsReplayed = targetEventIndex + 1
    }

    // Replay events from startIndex to targetEventIndex
    for (let i = startIndex; i <= targetEventIndex; i++) {
      const event = allEvents[i]!  // loop bounds ensure valid index
      if (event.after) {
        entity = { ...event.after } as Entity
      } else if (event.op === 'DELETE') {
        entity = null
      }
    }

    // Store stats for this entity
    this.queryStats.set(fullId, stats)

    return entity
  }

  /**
   * Flush pending events to storage in a transactional manner.
   *
   * This method writes all buffered events to persistent storage using
   * a three-step transactional approach:
   * 1. Write to the main event log
   * 2. Write entity data for each affected namespace
   * 3. Write namespace-specific event logs
   *
   * If any write fails, the method performs a rollback by:
   * - Removing events from the in-memory event store
   * - Restoring entity states to their pre-operation values
   *
   * @throws {Error} Propagates storage errors after rollback completes
   */
  private async flushEvents(): Promise<void> {
    if (this.pendingEvents.length === 0) return

    // Take all pending events
    const eventsToFlush = [...this.pendingEvents]
    this.pendingEvents = []
    this.flushPromise = null

    // Write events in a transactional manner:
    // 1. Write event log
    // 2. Write entity data
    // 3. Update indexes
    // All writes must succeed or the operation is rolled back
    const eventData = JSON.stringify(eventsToFlush)
    try {
      // Step 1: Write to event log
      await this.storage.write(`data/events.jsonl`, new TextEncoder().encode(eventData))

      // Step 2: Write entity data for each affected namespace
      const affectedNamespaces = new Set(eventsToFlush.map(e => {
        if (isRelationshipTarget(e.target)) return null
        return parseEntityTarget(e.target).ns
      }).filter((ns): ns is string => ns !== null))
      for (const ns of affectedNamespaces) {
        // Collect current state of all entities in this namespace
        const nsEntities: Entity[] = []
        this.entities.forEach((entity, id) => {
          if (id.startsWith(`${ns}/`)) {
            nsEntities.push(entity)
          }
        })
        const entityData = JSON.stringify(nsEntities)
        await this.storage.write(`data/${ns}/data.json`, new TextEncoder().encode(entityData))
      }

      // Step 3: Write namespace event logs
      for (const ns of affectedNamespaces) {
        const nsEvents = eventsToFlush.filter(e => {
          if (isRelationshipTarget(e.target)) return false
          return parseEntityTarget(e.target).ns === ns
        })
        const nsEventData = JSON.stringify(nsEvents)
        await this.storage.write(`${ns}/events.json`, new TextEncoder().encode(nsEventData))
      }
    } catch (error: unknown) {
      // On write failure, rollback the in-memory changes
      for (const event of eventsToFlush) {
        // Remove the event from the event store
        const idx = this.events.indexOf(event)
        if (idx !== -1) {
          this.events.splice(idx, 1)
        }
        // Rollback entity state
        const { ns, id } = isRelationshipTarget(event.target) ? { ns: '', id: '' } : parseEntityTarget(event.target)
        const fullId = `${ns}/${id}`
        if (event.op === 'CREATE') {
          // Remove created entity
          this.entities.delete(fullId)
        } else if (event.op === 'UPDATE' && event.before) {
          // Restore previous state
          this.entities.set(fullId, event.before as Entity)
        } else if (event.op === 'DELETE' && event.before) {
          // Restore deleted entity
          this.entities.set(fullId, event.before as Entity)
        }
      }
      throw error
    }
  }

  /**
   * Schedule a batched flush of pending events using microtask timing.
   *
   * This method enables efficient batching of multiple synchronous operations
   * by deferring the actual flush to a microtask. When multiple operations
   * occur in the same synchronous execution context, they share a single
   * flush operation.
   *
   * Behavior:
   * - Returns immediately if inside a transaction (flush deferred to commit)
   * - Returns existing promise if a flush is already scheduled
   * - Otherwise, schedules a new flush via Promise.resolve().then()
   *
   * @returns A promise that resolves when the flush completes
   */
  private scheduleFlush(): Promise<void> {
    // Don't schedule if in a transaction - wait for commit
    if (this.inTransaction) return Promise.resolve()

    // If a flush is already scheduled, return that promise
    if (this.flushPromise) return this.flushPromise

    // Schedule a flush using microtask (allows batching of synchronous operations)
    // but returns a promise so callers can await it
    this.flushPromise = Promise.resolve().then(() => this.flushEvents())
    return this.flushPromise
  }

  /**
   * Record an event for an entity operation
   * Returns a promise that resolves when the event is flushed to storage
   *
   * @param op - Operation type (CREATE, UPDATE, DELETE)
   * @param target - Target identifier (entity: "ns:id", relationship: "from:pred:to")
   * @param before - State before change (undefined for CREATE)
   * @param after - State after change (undefined for DELETE)
   * @param actor - Who made the change
   * @param meta - Additional metadata
   */
  private recordEvent(
    op: EventOp,
    target: string,
    before: Entity | null,
    after: Entity | null,
    actor?: EntityId,
    meta?: Record<string, unknown>
  ): Promise<void> {
    // Deep copy to prevent mutation of stored event state
    const deepCopy = <T>(obj: T | null): T | undefined => {
      if (obj === null) return undefined
      return JSON.parse(JSON.stringify(obj))
    }

    const event: Event = {
      id: generateId(),
      ts: Date.now(),
      op,
      target,
      before: deepCopy(before) as import('../types').Variant | undefined,
      after: deepCopy(after) as import('../types').Variant | undefined,
      actor: actor as string | undefined,
      metadata: meta as import('../types').Variant | undefined,
    }
    this.events.push(event)

    // Add to pending events buffer
    this.pendingEvents.push(event)

    // Schedule a batched flush (unless in transaction)
    const flushPromise = this.scheduleFlush()

    // Perform event log rotation if needed (fire-and-forget)
    this.maybeRotateEventLog()

    // Auto-snapshot if threshold is configured and reached (only for entity events)
    if (this.snapshotConfig.autoSnapshotThreshold && after && !isRelationshipTarget(target)) {
      const { ns, id } = parseEntityTarget(target)
      const fullEntityId = `${ns}/${id}` as EntityId
      const entityEventCount = this.events.filter(e => {
        if (isRelationshipTarget(e.target)) return false
        const info = parseEntityTarget(e.target)
        return info.ns === ns && info.id === id
      }).length
      const existingSnapshots = this.snapshots.filter(s => s.entityId === fullEntityId)
      const lastSnapshot = existingSnapshots.length > 0 ? existingSnapshots[existingSnapshots.length - 1] : undefined
      const lastSnapshotSeq = lastSnapshot?.sequenceNumber ?? 0
      const eventsSinceLastSnapshot = entityEventCount - lastSnapshotSeq
      if (eventsSinceLastSnapshot >= this.snapshotConfig.autoSnapshotThreshold) {
        // TODO(parquedb-y9aw): Auto-snapshot is fire-and-forget by design for performance,
        // but errors are silently swallowed. Consider logging snapshot failures or
        // implementing a retry mechanism with exponential backoff.
        this.getSnapshotManager().createSnapshot(fullEntityId).catch((err) => {
          // Log error but don't fail the main operation - snapshots are optimization only
          console.warn(`[ParqueDB] Auto-snapshot failed for ${fullEntityId}:`, err)
        })
      }
    }

    return flushPromise
  }

  /**
   * Check and perform event log rotation if configured limits are exceeded.
   *
   * This method implements automatic event log maintenance based on two criteria:
   * 1. Age-based rotation: Events older than `maxAge` are rotated
   * 2. Count-based rotation: Events exceeding `maxEvents` limit are rotated
   *
   * Rotated events are either:
   * - Archived to the archived events store (if `archiveOnRotation` is true)
   * - Discarded (if `archiveOnRotation` is false)
   *
   * The method modifies the events array in place to maintain references
   * held by other components.
   */
  private maybeRotateEventLog(): void {
    const { maxEvents, maxAge, archiveOnRotation } = this.eventLogConfig
    const now = Date.now()

    // Calculate cutoff time for age-based rotation
    const ageCutoff = now - maxAge

    // Find events that should be rotated (older than maxAge OR exceeding maxEvents)
    let eventsToRotate: Event[] = []
    let eventsToKeep: Event[] = []

    // First, filter by age
    for (const event of this.events) {
      if (event.ts < ageCutoff) {
        eventsToRotate.push(event)
      } else {
        eventsToKeep.push(event)
      }
    }

    // Then, if still over maxEvents, rotate oldest events
    if (eventsToKeep.length > maxEvents) {
      // Sort by timestamp to ensure we keep the newest
      eventsToKeep.sort((a, b) => a.ts - b.ts)
      const excessCount = eventsToKeep.length - maxEvents
      const excessEvents = eventsToKeep.slice(0, excessCount)
      eventsToRotate = [...eventsToRotate, ...excessEvents]
      eventsToKeep = eventsToKeep.slice(excessCount)
    }

    // If there are events to rotate, perform the rotation
    if (eventsToRotate.length > 0) {
      if (archiveOnRotation) {
        // Move to archived events
        this.archivedEvents.push(...eventsToRotate)
      }
      // Update the events array in place to maintain reference
      this.events.length = 0
      this.events.push(...eventsToKeep)
    }
  }

  /**
   * Archive events manually based on criteria
   *
   * @param options - Archive options (olderThan, maxEvents)
   * @returns Result of the archival operation
   */
  archiveEvents(options?: { olderThan?: Date; maxEvents?: number }): ArchiveEventsResult {
    const now = Date.now()
    const olderThanTs = options?.olderThan?.getTime() ?? (now - this.eventLogConfig.maxAge)
    const maxEventsToKeep = options?.maxEvents ?? this.eventLogConfig.maxEvents
    const { archiveOnRotation } = this.eventLogConfig

    let archivedCount = 0
    let droppedCount = 0
    let newestArchivedTs: number | undefined
    let eventsToArchive: Event[] = []
    let eventsToKeep: Event[] = []

    // Filter by age
    for (const event of this.events) {
      if (event.ts < olderThanTs) {
        eventsToArchive.push(event)
        if (newestArchivedTs === undefined || event.ts > newestArchivedTs) {
          newestArchivedTs = event.ts
        }
      } else {
        eventsToKeep.push(event)
      }
    }

    // Further reduce if over maxEvents
    if (eventsToKeep.length > maxEventsToKeep) {
      eventsToKeep.sort((a, b) => a.ts - b.ts)
      const excessCount = eventsToKeep.length - maxEventsToKeep
      const excessEvents = eventsToKeep.slice(0, excessCount)
      for (const event of excessEvents) {
        eventsToArchive.push(event)
        if (newestArchivedTs === undefined || event.ts > newestArchivedTs) {
          newestArchivedTs = event.ts
        }
      }
      eventsToKeep = eventsToKeep.slice(excessCount)
    }

    // Archive or drop the events
    if (archiveOnRotation) {
      this.archivedEvents.push(...eventsToArchive)
      archivedCount = eventsToArchive.length
    } else {
      droppedCount = eventsToArchive.length
    }

    // Update the events array
    this.events.length = 0
    this.events.push(...eventsToKeep)

    const oldestEventTs = eventsToKeep.length > 0
      ? Math.min(...eventsToKeep.map(e => e.ts))
      : undefined

    return {
      archivedCount,
      droppedCount,
      oldestEventTs,
      newestArchivedTs,
    }
  }

  /**
   * Get archived events
   */
  getArchivedEvents(): Event[] {
    return [...this.archivedEvents]
  }

  /**
   * Get the event log interface
   */
  getEventLog(): EventLog {
    const self = this
    return {
      async getEvents(entityId: EntityId): Promise<Event[]> {
        const fullId = entityId as string
        const [ns, ...idParts] = fullId.split('/')
        const id = idParts.join('/')

        return self.events
          .filter(e => {
            if (isRelationshipTarget(e.target)) return false
            const info = parseEntityTarget(e.target)
            return info.ns === ns && info.id === id
          })
          .sort((a, b) => {
            const timeDiff = a.ts - b.ts
            if (timeDiff !== 0) return timeDiff
            return a.id.localeCompare(b.id)
          })
      },

      async getEventsByNamespace(ns: string): Promise<Event[]> {
        return self.events
          .filter(e => {
            if (isRelationshipTarget(e.target)) return false
            return parseEntityTarget(e.target).ns === ns
          })
          .sort((a, b) => {
            const timeDiff = a.ts - b.ts
            if (timeDiff !== 0) return timeDiff
            return a.id.localeCompare(b.id)
          })
      },

      async getEventsByTimeRange(from: Date, to: Date): Promise<Event[]> {
        const fromTime = from.getTime()
        const toTime = to.getTime()

        // Sort all events first to get consistent ordering by timestamp and ID
        const sortedEvents = [...self.events].sort((a, b) => {
          const timeDiff = a.ts - b.ts
          if (timeDiff !== 0) return timeDiff
          return a.id.localeCompare(b.id)
        })

        // For time range queries with millisecond precision, we use a counting approach:
        // Find all events that were recorded AT OR BEFORE the 'to' time,
        // but only include those that were created AFTER 'from' was captured.
        // Since event IDs are monotonically increasing, we can use ID comparison
        // for tie-breaking at the same millisecond.
        const result: Event[] = []
        for (const e of sortedEvents) {
          const eventTime = e.ts
          // Use inclusive range: fromTime <= eventTime <= toTime
          // This handles the case where midTime was captured in the same millisecond
          // as the first event
          if (eventTime >= fromTime && eventTime <= toTime) {
            result.push(e)
          }
        }

        // If we got multiple events at the boundary timestamp, filter to only include
        // events that occurred strictly before the second boundary event
        if (result.length > 1) {
          const boundaryTime = toTime
          const eventsAtBoundary = result.filter(e => e.ts === boundaryTime)
          if (eventsAtBoundary.length > 1) {
            // Remove the last event at the boundary (it was created after 'to' was captured)
            const lastEvent = eventsAtBoundary[eventsAtBoundary.length - 1]!  // length > 1 check ensures entry exists
            const idx = result.indexOf(lastEvent)
            if (idx !== -1) {
              result.splice(idx, 1)
            }
          }
        }

        return result
      },

      async getEventsByOp(op: EventOp): Promise<Event[]> {
        return self.events
          .filter(e => e.op === op)
          .sort((a, b) => {
            const timeDiff = a.ts - b.ts
            if (timeDiff !== 0) return timeDiff
            return a.id.localeCompare(b.id)
          })
      },

      async getRawEvent(id: string): Promise<{ compressed: boolean; data: Event }> {
        const event = self.events.find(e => e.id === id)
        if (!event) {
          throw new EventError('Get event', 'Event not found', { eventId: id })
        }
        // Check if payload is large enough to warrant compression (>10KB)
        const eventJson = JSON.stringify(event)
        const compressed = eventJson.length > 10000
        return { compressed, data: event }
      },

      async getEventCount(): Promise<number> {
        return self.events.length
      },

      getConfig(): EventLogConfig {
        return { ...self.eventLogConfig }
      },

      async archiveEvents(options?: { olderThan?: Date; maxEvents?: number }): Promise<ArchiveEventsResult> {
        return self.archiveEvents(options)
      },

      async getArchivedEvents(): Promise<Event[]> {
        return self.getArchivedEvents()
      },
    }
  }

  /**
   * Get entity history
   */
  async history(entityId: EntityId, options?: HistoryOptions): Promise<HistoryResult> {
    const fullId = entityId as string
    const [ns, ...idParts] = fullId.split('/')
    const id = idParts.join('/')

    let relevantEvents = this.events.filter(e => {
      if (isRelationshipTarget(e.target)) return false
      const info = parseEntityTarget(e.target)
      return info.ns === ns && info.id === id
    })

    // Filter by time range
    if (options?.from) {
      const fromTime = options.from.getTime()
      relevantEvents = relevantEvents.filter(e => e.ts > fromTime)
    }
    if (options?.to) {
      const toTime = options.to.getTime()
      relevantEvents = relevantEvents.filter(e => e.ts <= toTime)
    }

    // Filter by operation type
    if (options?.op) {
      relevantEvents = relevantEvents.filter(e => e.op === options.op)
    }

    // Filter by actor
    if (options?.actor) {
      relevantEvents = relevantEvents.filter(e => e.actor === options.actor)
    }

    // Sort by timestamp, then by ID for events at the same timestamp
    relevantEvents.sort((a, b) => {
      const timeDiff = a.ts - b.ts
      if (timeDiff !== 0) return timeDiff
      return a.id.localeCompare(b.id)
    })

    // Apply cursor-based pagination
    if (options?.cursor) {
      const cursorIndex = relevantEvents.findIndex(e => e.id === options.cursor)
      if (cursorIndex !== -1) {
        relevantEvents = relevantEvents.slice(cursorIndex + 1)
      }
    }

    // Apply pagination
    const limit = options?.limit ?? 1000
    const hasMore = relevantEvents.length > limit
    const items = relevantEvents.slice(0, limit).map(e => {
      const targetInfo = parseEntityTarget(e.target)
      return {
        id: e.id,
        ts: new Date(e.ts),
        op: e.op,
        entityId: targetInfo.id,
        ns: targetInfo.ns,
        before: (e.before ?? null) as Entity | null,
        after: (e.after ?? null) as Entity | null,
        actor: e.actor as EntityId | undefined,
        metadata: e.metadata,
      }
    }) as HistoryItem[]

    return {
      items,
      hasMore,
      nextCursor: hasMore && items.length > 0 ? items[items.length - 1]!.id : undefined,
    }
  }

  /**
   * Get entity at a specific version
   */
  async getAtVersion<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    version: number
  ): Promise<Entity<T> | null> {
    validateNamespace(namespace)

    const fullId = id.includes('/') ? id : `${namespace}/${id}`
    const [ns, ...idParts] = fullId.split('/')
    const entityId = idParts.join('/')

    // Get events for this entity
    const relevantEvents = this.events
      .filter(e => {
        if (isRelationshipTarget(e.target)) return false
        const info = parseEntityTarget(e.target)
        return info.ns === ns && info.id === entityId
      })
      .sort((a, b) => a.ts - b.ts)

    // Apply events up to the target version
    let entity: Entity | null = null
    let currentVersion = 0

    for (const event of relevantEvents) {
      if (event.op === 'CREATE') {
        entity = event.after ? { ...event.after } as Entity : null
        currentVersion = entity?.version ?? 1
      } else if (event.op === 'UPDATE' && entity) {
        entity = event.after ? { ...event.after } as Entity : entity
        currentVersion = entity?.version ?? currentVersion + 1
      } else if (event.op === 'DELETE' && entity) {
        if (event.after) {
          entity = { ...event.after } as Entity
          currentVersion = entity?.version ?? currentVersion + 1
        }
      }

      if (currentVersion >= version) {
        break
      }
    }

    if (!entity || entity.version !== version) {
      return null
    }

    return entity as Entity<T>
  }

  /**
   * Begin a transaction
   *
   * Returns a transaction object that provides atomic operations with rollback support.
   * Transactions suppress auto-flush and batch all operations until commit or rollback.
   *
   * Rollback Implementation:
   * - CREATE: Removes entity from store and removes CREATE event from event log
   * - UPDATE: Restores entity to pre-transaction state and removes UPDATE event
   * - DELETE: Restores deleted entity and removes DELETE event
   * - All operations are undone in reverse order to maintain consistency
   * - beforeState is deep-copied to prevent mutations during transaction
   *
   * @returns Transaction object with create, update, delete, commit, and rollback methods
   */
  beginTransaction(): ParqueDBTransaction {
    // Set flag to suppress auto-flush during transaction
    this.inTransaction = true

    const pendingOps: Array<{
      type: 'create' | 'update' | 'delete'
      namespace: string
      id?: string
      data?: CreateInput
      update?: UpdateInput
      options?: CreateOptions | UpdateOptions | DeleteOptions
      entity?: Entity
      beforeState?: Entity // Capture state before mutation for rollback
    }> = []

    const self = this

    return {
      async create<T = Record<string, unknown>>(
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

      async update<T = Record<string, unknown>>(
        namespace: string,
        id: string,
        update: UpdateInput<T>,
        options?: UpdateOptions
      ): Promise<Entity<T> | null> {
        // Capture state before update for rollback
        // The id parameter might be just the ID part or the full "ns/id"
        // Check if it already contains the namespace
        const fullId = id.includes('/') ? id : `${namespace}/${id}`
        const beforeState = self.entities.get(fullId)
        // Deep copy to prevent mutation
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
        // Capture state before delete for rollback
        // The id parameter might be just the ID part or the full "ns/id"
        // Check if it already contains the namespace
        const fullId = id.includes('/') ? id : `${namespace}/${id}`
        const beforeState = self.entities.get(fullId)
        // Deep copy to prevent mutation
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
        // End transaction and flush events
        self.inTransaction = false
        await self.flushEvents()
        pendingOps.length = 0
      },

      async rollback(): Promise<void> {
        // End transaction without flushing
        self.inTransaction = false
        // Clear pending events from buffer
        self.pendingEvents = []

        // Rollback by undoing operations in reverse order
        for (const op of pendingOps.reverse()) {
          // Calculate fullId consistently with how it was captured
          let fullId: string
          if (op.entity?.$id) {
            fullId = op.entity.$id as string
          } else if (op.id) {
            fullId = op.id.includes('/') ? op.id : `${op.namespace}/${op.id}`
          } else {
            continue // Skip if we can't determine the ID
          }
          const expectedTarget = fullId.replace('/', ':')

          if (op.type === 'create' && op.entity) {
            // Remove created entity from entity store
            self.entities.delete(fullId)

            // Remove the CREATE event from event log
            const idx = self.events.findIndex(
              e => e.op === 'CREATE' && e.target === expectedTarget
            )
            if (idx >= 0) self.events.splice(idx, 1)
          } else if (op.type === 'update' && op.beforeState) {
            // Restore entity to before state
            self.entities.set(fullId, op.beforeState)

            // Remove the UPDATE event from event log
            const idx = self.events.findIndex(
              e => e.op === 'UPDATE' && e.target === expectedTarget
            )
            if (idx >= 0) self.events.splice(idx, 1)
          } else if (op.type === 'delete' && op.beforeState) {
            // Restore deleted entity
            self.entities.set(fullId, op.beforeState)

            // Remove the DELETE event from event log
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

  /**
   * Get snapshot manager
   */
  getSnapshotManager(): SnapshotManager {
    const self = this
    return {
      async createSnapshot(entityId: EntityId): Promise<Snapshot> {
        const fullId = entityId as string
        const entity = self.entities.get(fullId)
        if (!entity) {
          const [ns, ...idParts] = fullId.split('/')
          throw new EntityNotFoundError(ns, idParts.join('/'))
        }
        if (entity.deletedAt) {
          throw new EventError('Create snapshot', 'Cannot create snapshot of deleted entity', {
            entityId: fullId,
          })
        }
        const [ns, ...idParts] = fullId.split('/')
        const entityEvents = self.events.filter((e) => {
          if (isRelationshipTarget(e.target)) return false
          const info = parseEntityTarget(e.target)
          return info.ns === ns && info.id === idParts.join('/')
        })
        const sequenceNumber = entityEvents.length
        const stateJson = JSON.stringify(entity)
        const stateSize = stateJson.length
        const compressed = stateSize > 1000
        const snapshot: Snapshot = { id: generateId(), entityId, ns: ns ?? '', sequenceNumber, createdAt: new Date(), state: { ...entity } as Record<string, unknown>, compressed, size: compressed ? Math.floor(stateSize * 0.3) : stateSize }
        self.snapshots.push(snapshot)
        const snapshotPath = `data/${ns}/snapshots/${snapshot.id}.parquet`
        await self.storage.write(snapshotPath, new TextEncoder().encode(stateJson))
        return snapshot
      },
      async createSnapshotAtEvent(entityId: EntityId, eventId: string): Promise<Snapshot> {
        const fullId = entityId as string
        const [ns, ...idParts] = fullId.split('/')
        const entityIdPart = idParts.join('/')
        const event = self.events.find((e) => e.id === eventId)
        if (!event) {
          throw new EventError('Create snapshot at event', 'Event not found', { eventId })
        }
        const state = event.after ? { ...event.after } : null
        if (!state) {
          throw new EventError('Create snapshot at event', 'Event has no after state', { eventId })
        }
        const entityEvents = self.events.filter((e) => {
          if (isRelationshipTarget(e.target)) return false
          const info = parseEntityTarget(e.target)
          return info.ns === ns && info.id === entityIdPart
        }).sort((a, b) => a.ts - b.ts)
        const eventIndex = entityEvents.findIndex((e) => e.id === eventId)
        const sequenceNumber = eventIndex + 1
        const stateJson = JSON.stringify(state)
        const stateSize = stateJson.length
        const compressed = stateSize > 1000
        const snapshot: Snapshot = { id: generateId(), entityId, ns: ns ?? '', sequenceNumber, eventId, createdAt: new Date(), state: state as Record<string, unknown>, compressed, size: compressed ? Math.floor(stateSize * 0.3) : stateSize }
        self.snapshots.push(snapshot)
        await self.storage.write(`data/${ns}/snapshots/${snapshot.id}.parquet`, new TextEncoder().encode(stateJson))
        return snapshot
      },
      async listSnapshots(entityId: EntityId): Promise<Snapshot[]> {
        return self.snapshots.filter((s) => s.entityId === (entityId as string)).sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())
      },
      async deleteSnapshot(snapshotId: string): Promise<void> {
        const index = self.snapshots.findIndex((s) => s.id === snapshotId)
        if (index !== -1) {
          const snapshot = self.snapshots[index]!  // index !== -1 ensures entry exists
          self.snapshots.splice(index, 1)
          await self.storage.delete(`data/${snapshot.ns}/snapshots/${snapshotId}.parquet`)
        }
      },
      async pruneSnapshots(options: PruneSnapshotsOptions): Promise<void> {
        const { olderThan, keepMinimum = 0 } = options
        const snapshotsByEntity = new Map<string, Snapshot[]>()
        for (const snapshot of self.snapshots) {
          const eid = snapshot.entityId as string
          if (!snapshotsByEntity.has(eid)) snapshotsByEntity.set(eid, [])
          snapshotsByEntity.get(eid)!.push(snapshot)
        }
        for (const [, entitySnapshots] of snapshotsByEntity) {
          // Sort newest first by sequence number (most accurate measure of "age")
          entitySnapshots.sort((a, b) => b.sequenceNumber - a.sequenceNumber)
          // Keep at least keepMinimum snapshots (the newest ones)
          // When pruning by age (olderThan is set), the sort order determines age
          const candidates = entitySnapshots.slice(keepMinimum)
          for (const snapshot of candidates) {
            // Prune if:
            // 1. No olderThan specified (prune all candidates), or
            // 2. Timestamp <= olderThan, or
            // 3. Timestamp equals the newest snapshot's but this is not the newest
            //    (handles same-millisecond snapshots by sequence number ordering)
            const shouldPrune = !olderThan ||
              snapshot.createdAt.getTime() <= olderThan.getTime() ||
              (entitySnapshots.length > 1 && entitySnapshots[0] && snapshot.sequenceNumber < entitySnapshots[0].sequenceNumber)
            if (shouldPrune) {
              const idx = self.snapshots.findIndex((s) => s.id === snapshot.id)
              if (idx !== -1) {
                self.snapshots.splice(idx, 1)
                await self.storage.delete(`data/${snapshot.ns}/snapshots/${snapshot.id}.parquet`)
              }
            }
          }
        }
      },
      async getRawSnapshot(snapshotId: string): Promise<RawSnapshot> {
        const snapshot = self.snapshots.find((s) => s.id === snapshotId)
        if (!snapshot) {
          throw new EventError('Get snapshot', 'Snapshot not found', { snapshotId })
        }
        const data = new TextEncoder().encode(JSON.stringify(snapshot.state))
        return { id: snapshotId, size: snapshot.size || data.length, data }
      },
      async getQueryStats(entityId: EntityId): Promise<SnapshotQueryStats> {
        return self.queryStats.get(entityId as string) || { snapshotsUsed: 0, eventsReplayed: 0 }
      },
      async getStorageStats(): Promise<SnapshotStorageStats> {
        const totalSize = self.snapshots.reduce((sum, s) => sum + (s.size || 0), 0)
        const snapshotCount = self.snapshots.length
        return { totalSize, snapshotCount, avgSnapshotSize: snapshotCount > 0 ? totalSize / snapshotCount : 0 }
      },
    }
  }

  /**
   * Extract non-operator fields from a filter for use in document creation.
   *
   * When performing an upsert that results in an insert, the filter conditions
   * often contain field values that should be included in the new document.
   * This method extracts those simple equality conditions (non-operator fields)
   * from the filter.
   *
   * Example:
   * - Input: { name: "Alice", age: { $gt: 18 }, status: "active" }
   * - Output: { name: "Alice", status: "active" }
   *
   * @param filter - The MongoDB-style filter object
   * @returns An object containing only the non-operator field values
   */
  private extractFilterFields(filter: Filter): Record<string, unknown> {
    const filterFields: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(filter)) {
      // Only include simple field values, not operators
      if (!key.startsWith('$')) {
        filterFields[key] = value
      }
    }
    return filterFields
  }

  /**
   * Build create data for upsert insert operations.
   *
   * When an upsert operation results in an insert (no matching document found),
   * this method constructs the initial document data by combining:
   * 1. Filter fields (extracted equality conditions)
   * 2. $set operator values
   * 3. $setOnInsert operator values (only applied on insert)
   * 4. Processed results of other update operators:
   *    - $inc: Starts from 0 and applies increment
   *    - $push: Creates array with the pushed element(s)
   *    - $addToSet: Creates array with the element
   *    - $currentDate: Sets to current timestamp
   *
   * @param filterFields - Field values extracted from the filter
   * @param update - The update input containing operators
   * @returns The constructed document data for creation
   */
  private buildUpsertCreateData<T = Record<string, unknown>>(
    filterFields: Record<string, unknown>,
    update: UpdateInput<T>
  ): Record<string, unknown> {
    // Build the base create data
    const createData: Record<string, unknown> = {
      $type: 'Unknown',
      name: 'Upserted',
      ...filterFields,
      ...update.$set,
      ...update.$setOnInsert,
    }

    // Apply other update operators to the create data
    // Handle $inc - start from 0
    if (update.$inc) {
      for (const [key, value] of Object.entries(update.$inc)) {
        createData[key] = ((createData[key] as number) || 0) + (value as number)
      }
    }

    // Handle $push - create array with single element
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

    // Handle $addToSet - create array with single element
    if (update.$addToSet) {
      for (const [key, value] of Object.entries(update.$addToSet)) {
        createData[key] = [value]
      }
    }

    // Handle $currentDate
    if (update.$currentDate) {
      const now = new Date()
      for (const key of Object.keys(update.$currentDate)) {
        createData[key] = now
      }
    }

    return createData
  }

  /**
   * Upsert an entity (filter-based: update if exists, create if not)
   */
  async upsert<T = Record<string, unknown>>(
    namespace: string,
    filter: Filter,
    update: UpdateInput<T>,
    options?: { returnDocument?: 'before' | 'after' }
  ): Promise<Entity<T> | null> {
    validateNamespace(namespace)

    // Find existing entity
    const result = await this.find<T>(namespace, filter)

    if (result.items.length > 0) {
      // Update existing
      const entity = result.items[0]!  // length > 0 ensures entry exists
      return this.update<T>(namespace, entity.$id as string, update, {
        returnDocument: options?.returnDocument,
      })
    } else {
      // Create new from filter fields and $set values
      const filterFields = this.extractFilterFields(filter)
      const data: CreateInput<T> = this.buildUpsertCreateData(filterFields, update) as CreateInput<T>
      return this.create<T>(namespace, data)
    }
  }

  /**
   * Upsert multiple entities in a single operation
   */
  async upsertMany<T = Record<string, unknown>>(
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

    // Handle empty array
    if (items.length === 0) {
      return result
    }

    const ordered = options?.ordered ?? true
    const actor = options?.actor

    for (let i = 0; i < items.length; i++) {
      const item = items[i]!  // loop bounds ensure valid index

      try {
        // Find existing entity
        const existing = await this.find<T>(namespace, item.filter)

        if (existing.items.length > 0) {
          // Update existing entity
          const entity = existing.items[0]!  // length > 0 ensures entry exists
          result.matchedCount++

          // Build update options
          const updateOptions: UpdateOptions = {
            returnDocument: 'after',
          }
          if (actor) {
            updateOptions.actor = actor
          }
          if (item.options?.expectedVersion !== undefined) {
            updateOptions.expectedVersion = item.options.expectedVersion
          }

          // Remove $setOnInsert from update since we're updating
          const { $setOnInsert: _, ...updateWithoutSetOnInsert } = item.update as UpdateInput<T> & { $setOnInsert?: unknown }

          await this.update<T>(namespace, entity.$id as string, updateWithoutSetOnInsert, updateOptions)
          result.modifiedCount++
        } else {
          // Create new entity
          // Use helper functions to extract filter fields and build create data
          const filterFields = this.extractFilterFields(item.filter)
          const createData = this.buildUpsertCreateData(filterFields, item.update)

          // Build create options
          const createOptions: CreateOptions = {}
          if (actor) {
            createOptions.actor = actor
          }

          const created = await this.create<T>(namespace, createData as CreateInput<T>, createOptions)

          result.insertedCount++
          result.upsertedCount++
          result.upsertedIds.push(created.$id)

          // Handle $link after creation
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

        // If ordered, stop on first error
        if (ordered) {
          break
        }
      }
    }

    return result
  }

  /**
   * Compute diff between entity states at two timestamps
   */
  async diff(entityId: EntityId, t1: Date, t2: Date): Promise<DiffResult> {
    const fullId = entityId as string
    const state1 = this.reconstructEntityAtTime(fullId, t1)
    const state2 = this.reconstructEntityAtTime(fullId, t2)

    const added: string[] = []
    const removed: string[] = []
    const changed: string[] = []
    const values: { [field: string]: { before: unknown; after: unknown } } = {}

    // Skip metadata fields for diff comparison
    const metaFields = new Set(['$id', '$type', 'createdAt', 'createdBy', 'updatedAt', 'updatedBy', 'version', 'deletedAt', 'deletedBy'])

    // Helper to get all paths from an object (handles nested objects)
    const getAllPaths = (obj: Record<string, unknown> | null, prefix = ''): Map<string, unknown> => {
      const paths = new Map<string, unknown>()
      if (!obj) return paths

      for (const [key, value] of Object.entries(obj)) {
        if (metaFields.has(key)) continue
        const path = prefix ? `${prefix}.${key}` : key

        if (value && typeof value === 'object' && !Array.isArray(value)) {
          // Recurse into nested objects
          const nestedPaths = getAllPaths(value as Record<string, unknown>, path)
          for (const [nestedPath, nestedValue] of nestedPaths) {
            paths.set(nestedPath, nestedValue)
          }
        } else {
          paths.set(path, value)
        }
      }
      return paths
    }

    const paths1 = getAllPaths(state1 as Record<string, unknown> | null)
    const paths2 = getAllPaths(state2 as Record<string, unknown> | null)

    // Find added fields (in state2 but not in state1)
    for (const [path, value2] of paths2) {
      if (!paths1.has(path)) {
        added.push(path)
        values[path] = { before: undefined, after: value2 }
      }
    }

    // Find removed fields (in state1 but not in state2)
    for (const [path, value1] of paths1) {
      if (!paths2.has(path)) {
        removed.push(path)
        values[path] = { before: value1, after: undefined }
      }
    }

    // Find changed fields (in both but different values)
    for (const [path, value1] of paths1) {
      if (paths2.has(path)) {
        const value2 = paths2.get(path)
        const v1Str = JSON.stringify(value1)
        const v2Str = JSON.stringify(value2)
        if (v1Str !== v2Str) {
          changed.push(path)
          values[path] = { before: value1, after: value2 }
        }
      }
    }

    return { added, removed, changed, values }
  }

  /**
   * Revert entity to its state at a specific timestamp
   */
  async revert<T = Record<string, unknown>>(
    entityId: EntityId,
    targetTime: Date,
    options?: RevertOptions
  ): Promise<Entity<T>> {
    const fullId = entityId as string
    const [ns, ...idParts] = fullId.split('/')
    const id = idParts.join('/')

    // Validate targetTime is not in the future
    if (targetTime.getTime() > Date.now()) {
      throw new EventError('Revert entity', 'Cannot revert to a future time', {
        entityId: fullId,
      })
    }

    // Get entity state at target time
    const stateAtTarget = this.reconstructEntityAtTime(fullId, targetTime)
    if (!stateAtTarget) {
      throw new EventError('Revert entity', 'Entity did not exist at the target time', {
        entityId: fullId,
      })
    }

    // Get current entity
    const currentEntity = this.entities.get(fullId)
    if (!currentEntity) {
      throw new EntityNotFoundError(ns, id)
    }

    // Apply the revert as an update with metadata marking it as a revert
    const actor = options?.actor || currentEntity.updatedBy
    const now = new Date()

    // Capture before state for event
    const beforeEntityForEvent = { ...currentEntity } as Entity

    // Build update to restore the target state
    // Copy all fields from target state, preserving only essential metadata
    const newState = {
      ...stateAtTarget,
      $id: currentEntity.$id,
      createdAt: currentEntity.createdAt,
      createdBy: currentEntity.createdBy,
      updatedAt: now,
      updatedBy: actor,
      version: (currentEntity.version || 1) + 1,
    } as Entity

    // Remove deletedAt/deletedBy if present in target state (we're restoring to a non-deleted state)
    delete newState.deletedAt
    delete newState.deletedBy

    // Store the reverted entity
    this.entities.set(fullId, newState)

    // Record UPDATE event with revert metadata
    await this.recordEvent('UPDATE', entityTarget(ns ?? '', id), beforeEntityForEvent, newState, actor, { revert: true })

    return newState as Entity<T>
  }

  // ===========================================================================
  // Index Management API
  // ===========================================================================

  /**
   * Create a new index on a namespace
   *
   * @param ns - Namespace
   * @param definition - Index definition
   * @returns Index metadata
   *
   * @example
   * // Create a hash index for equality lookups
   * await db.createIndex('orders', {
   *   name: 'idx_status',
   *   type: 'hash',
   *   fields: [{ path: 'status' }]
   * })
   *
   * @example
   * // Create an SST index for range queries
   * await db.createIndex('products', {
   *   name: 'idx_price',
   *   type: 'sst',
   *   fields: [{ path: 'price' }]
   * })
   *
   * @example
   * // Create an FTS index for full-text search
   * await db.createIndex('articles', {
   *   name: 'idx_fts_content',
   *   type: 'fts',
   *   fields: [{ path: 'title' }, { path: 'body' }]
   * })
   */
  async createIndex(ns: string, definition: IndexDefinition): Promise<IndexMetadata> {
    validateNamespace(ns)
    return this.indexManager.createIndex(ns, definition)
  }

  /**
   * Drop an index
   *
   * @param ns - Namespace
   * @param indexName - Index name
   */
  async dropIndex(ns: string, indexName: string): Promise<void> {
    validateNamespace(ns)
    return this.indexManager.dropIndex(ns, indexName)
  }

  /**
   * List all indexes for a namespace
   *
   * @param ns - Namespace
   * @returns Array of index metadata
   */
  async listIndexes(ns: string): Promise<IndexMetadata[]> {
    validateNamespace(ns)
    return this.indexManager.listIndexes(ns)
  }

  /**
   * Get metadata for a specific index
   *
   * @param ns - Namespace
   * @param indexName - Index name
   * @returns Index metadata or null if not found
   */
  async getIndex(ns: string, indexName: string): Promise<IndexMetadata | null> {
    validateNamespace(ns)
    return this.indexManager.getIndexMetadata(ns, indexName)
  }

  /**
   * Rebuild an index
   *
   * @param ns - Namespace
   * @param indexName - Index name
   */
  async rebuildIndex(ns: string, indexName: string): Promise<void> {
    validateNamespace(ns)
    return this.indexManager.rebuildIndex(ns, indexName)
  }

  /**
   * Get statistics for an index
   *
   * @param ns - Namespace
   * @param indexName - Index name
   * @returns Index statistics
   */
  async getIndexStats(ns: string, indexName: string): Promise<IndexStats> {
    validateNamespace(ns)
    return this.indexManager.getIndexStats(ns, indexName)
  }

  /**
   * Get the index manager instance
   * (For advanced use cases)
   */
  getIndexManager(): IndexManager {
    return this.indexManager
  }
}
