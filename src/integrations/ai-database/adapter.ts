/**
 * ParqueDB Adapter for ai-database
 *
 * This adapter allows ParqueDB to be used as a backend for ai-database,
 * providing all required DBProvider and DBProviderExtended interface methods.
 *
 * @packageDocumentation
 */

import type { ParqueDB, Collection } from '../../ParqueDB'
import type { Entity, EntityId, Filter, UpdateInput } from '../../types'
import { RelationshipBatchLoader, type BatchLoaderOptions, type BatchLoaderDB } from '../../relationships/batch-loader'

// =============================================================================
// Types from ai-database (replicated to avoid circular dependency)
// =============================================================================

/**
 * List options for querying entities
 */
export interface ListOptions {
  where?: Record<string, unknown>
  orderBy?: string
  order?: 'asc' | 'desc'
  limit?: number
  offset?: number
}

/**
 * Search options extending list options
 */
export interface SearchOptions extends ListOptions {
  fields?: string[]
  minScore?: number
}

/**
 * Semantic search options
 */
export interface SemanticSearchOptions {
  minScore?: number
  limit?: number
}

/**
 * Hybrid search options
 */
export interface HybridSearchOptions {
  minScore?: number
  limit?: number
  offset?: number
  rrfK?: number
  ftsWeight?: number
  semanticWeight?: number
}

/**
 * Semantic search result with score
 */
export interface SemanticSearchResult {
  $id: string
  $type: string
  $score: number
  [key: string]: unknown
}

/**
 * Hybrid search result with RRF and component scores
 */
export interface HybridSearchResult extends SemanticSearchResult {
  $rrfScore: number
  $ftsRank: number
  $semanticRank: number
}

/**
 * Relationship metadata
 */
export interface RelationMetadata {
  matchMode?: 'exact' | 'fuzzy'
  similarity?: number
  matchedType?: string
  [key: string]: unknown
}

/**
 * Event data structure
 */
export interface DBEvent {
  id: string
  actor: string
  event: string
  object?: string
  objectData?: Record<string, unknown>
  result?: string
  resultData?: Record<string, unknown>
  meta?: Record<string, unknown>
  timestamp: Date
}

/**
 * Action data structure
 */
export interface DBAction {
  id: string
  actor: string
  act: string
  action: string
  activity: string
  object?: string
  objectData?: Record<string, unknown>
  status: 'pending' | 'active' | 'completed' | 'failed' | 'cancelled'
  progress?: number
  total?: number
  result?: Record<string, unknown>
  error?: string
  meta?: Record<string, unknown>
  createdAt: Date
  startedAt?: Date
  completedAt?: Date
}

/**
 * Artifact data structure
 */
export interface DBArtifact {
  url: string
  type: string
  sourceHash: string
  content: unknown
  metadata?: Record<string, unknown>
  createdAt: Date
}

/**
 * Options for creating an event
 */
export interface CreateEventOptions {
  actor: string
  event: string
  object?: string
  objectData?: Record<string, unknown>
  result?: string
  resultData?: Record<string, unknown>
  meta?: Record<string, unknown>
}

/**
 * Options for creating an action
 */
export interface CreateActionOptions {
  actor: string
  action: string
  object?: string
  objectData?: Record<string, unknown>
  total?: number
  meta?: Record<string, unknown>
}

/**
 * Embeddings configuration
 */
export interface EmbeddingsConfig {
  provider?: string
  model?: string
  dimensions?: number
  fields?: Record<string, string[]>
}

/**
 * Transaction interface
 */
export interface Transaction {
  get(type: string, id: string): Promise<Record<string, unknown> | null>
  create(type: string, id: string | undefined, data: Record<string, unknown>): Promise<Record<string, unknown>>
  update(type: string, id: string, data: Record<string, unknown>): Promise<Record<string, unknown>>
  delete(type: string, id: string): Promise<boolean>
  relate(
    fromType: string,
    fromId: string,
    relation: string,
    toType: string,
    toId: string,
    metadata?: RelationMetadata
  ): Promise<void>
  commit(): Promise<void>
  rollback(): Promise<void>
}

/**
 * Base database provider interface
 */
export interface DBProvider {
  get(type: string, id: string): Promise<Record<string, unknown> | null>
  list(type: string, options?: ListOptions): Promise<Record<string, unknown>[]>
  search(type: string, query: string, options?: SearchOptions): Promise<Record<string, unknown>[]>
  create(type: string, id: string | undefined, data: Record<string, unknown>): Promise<Record<string, unknown>>
  update(type: string, id: string, data: Record<string, unknown>): Promise<Record<string, unknown>>
  delete(type: string, id: string): Promise<boolean>
  related(type: string, id: string, relation: string): Promise<Record<string, unknown>[]>
  relate(
    fromType: string,
    fromId: string,
    relation: string,
    toType: string,
    toId: string,
    metadata?: RelationMetadata
  ): Promise<void>
  unrelate(
    fromType: string,
    fromId: string,
    relation: string,
    toType: string,
    toId: string
  ): Promise<void>
  beginTransaction?(): Promise<Transaction>
}

/**
 * Extended provider interface with semantic search, events, actions, and artifacts
 */
export interface DBProviderExtended extends DBProvider {
  setEmbeddingsConfig(config: EmbeddingsConfig): void
  semanticSearch(type: string, query: string, options?: SemanticSearchOptions): Promise<SemanticSearchResult[]>
  hybridSearch(type: string, query: string, options?: HybridSearchOptions): Promise<HybridSearchResult[]>

  // Events API
  on(pattern: string, handler: (event: DBEvent) => void | Promise<void>): () => void
  emit(options: CreateEventOptions): Promise<DBEvent>
  emit(type: string, data: unknown): Promise<DBEvent>
  listEvents(options?: {
    event?: string
    actor?: string
    object?: string
    since?: Date
    until?: Date
    limit?: number
  }): Promise<DBEvent[]>
  replayEvents(options: {
    event?: string
    actor?: string
    since?: Date
    handler: (event: DBEvent) => void | Promise<void>
  }): Promise<void>

  // Actions API
  createAction(options: CreateActionOptions | { type: string; data: unknown; total?: number }): Promise<DBAction>
  getAction(id: string): Promise<DBAction | null>
  updateAction(
    id: string,
    updates: Partial<Pick<DBAction, 'status' | 'progress' | 'result' | 'error'>>
  ): Promise<DBAction>
  listActions(options?: {
    status?: DBAction['status']
    action?: string
    actor?: string
    object?: string
    since?: Date
    until?: Date
    limit?: number
  }): Promise<DBAction[]>
  retryAction(id: string): Promise<DBAction>
  cancelAction(id: string): Promise<void>

  // Artifacts API
  getArtifact(url: string, type: string): Promise<DBArtifact | null>
  setArtifact(
    url: string,
    type: string,
    data: { content: unknown; sourceHash: string; metadata?: Record<string, unknown> }
  ): Promise<void>
  deleteArtifact(url: string, type?: string): Promise<void>
  listArtifacts(url: string): Promise<DBArtifact[]>
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Convert type name to namespace (lowercase, pluralized)
 */
function typeToNamespace(type: string): string {
  // Simple pluralization: add 's' if not already ending in 's'
  const lower = type.toLowerCase()
  return lower.endsWith('s') ? lower : lower + 's'
}

/**
 * Generate a ULID-like ID
 */
function generateId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 10)
  return `${timestamp}${random}`
}

/**
 * Convert ai-database ListOptions to ParqueDB FindOptions
 */
function convertListOptions(options?: ListOptions): { filter?: Filter; findOptions?: Record<string, unknown> } {
  if (!options) return {}

  const filter: Filter = options.where ? { ...options.where } : {}

  const findOptions: Record<string, unknown> = {}

  if (options.orderBy) {
    findOptions.sort = {
      [options.orderBy]: options.order === 'desc' ? -1 : 1,
    }
  }

  if (options.limit !== undefined) {
    findOptions.limit = options.limit
  }

  if (options.offset !== undefined) {
    findOptions.skip = options.offset
  }

  return { filter, findOptions }
}

/**
 * Strip namespace from entity ID
 */
function stripNamespace(id: string): string {
  if (id.includes('/')) {
    return id.split('/').slice(1).join('/')
  }
  return id
}

/**
 * Convert Entity to plain Record
 */
function entityToRecord(entity: Entity): Record<string, unknown> {
  const { $id, $type, name, createdAt, createdBy, updatedAt, updatedBy, version, deletedAt, deletedBy, ...rest } = entity
  return {
    $id,
    $type,
    name,
    createdAt,
    createdBy,
    updatedAt,
    updatedBy,
    version,
    ...(deletedAt ? { deletedAt } : {}),
    ...(deletedBy ? { deletedBy } : {}),
    ...rest,
  }
}

// =============================================================================
// ParqueDB Adapter Implementation
// =============================================================================

/**
 * ParqueDB Adapter for ai-database
 *
 * Implements the DBProvider and DBProviderExtended interfaces to allow
 * ParqueDB to be used as a backend for ai-database applications.
 *
 * @example
 * ```typescript
 * import { ParqueDB } from 'parquedb'
 * import { ParqueDBAdapter } from 'parquedb/integrations/ai-database'
 *
 * const db = new ParqueDB({ storage: new MemoryBackend() })
 * const provider = new ParqueDBAdapter(db)
 *
 * // Use with ai-database
 * await provider.create('User', undefined, { name: 'Alice', email: 'alice@example.com' })
 * const users = await provider.list('User', { limit: 10 })
 * ```
 */
/**
 * Options for configuring the ParqueDBAdapter
 */
export interface ParqueDBAdapterOptions {
  /**
   * Enable batch loading for relationships to eliminate N+1 queries.
   * Default: true
   */
  enableBatchLoader?: boolean

  /**
   * Options for the relationship batch loader.
   * Only used if enableBatchLoader is true.
   */
  batchLoaderOptions?: BatchLoaderOptions
}

export class ParqueDBAdapter implements DBProviderExtended {
  private db: ParqueDB
  private embeddingsConfig: EmbeddingsConfig | null = null
  private eventHandlers = new Map<string, Set<(event: DBEvent) => void | Promise<void>>>()
  private batchLoader: RelationshipBatchLoader | null = null

  constructor(db: ParqueDB, options?: ParqueDBAdapterOptions) {
    this.db = db

    // Initialize batch loader if enabled (default: true)
    const enableBatchLoader = options?.enableBatchLoader ?? true
    if (enableBatchLoader) {
      this.batchLoader = new RelationshipBatchLoader(db as BatchLoaderDB, options?.batchLoaderOptions)
    }
  }

  /**
   * Get the batch loader instance for advanced usage
   * Returns null if batch loading is disabled
   */
  getBatchLoader(): RelationshipBatchLoader | null {
    return this.batchLoader
  }

  /**
   * Clear the batch loader cache
   * Useful between requests in server environments
   */
  clearBatchLoader(): void {
    this.batchLoader?.clear()
  }

  // ===========================================================================
  // Read Operations
  // ===========================================================================

  /**
   * Get an entity by type and ID
   */
  async get(type: string, id: string): Promise<Record<string, unknown> | null> {
    const namespace = typeToNamespace(type)
    const localId = stripNamespace(id)
    const entity = await this.db.get(namespace, localId)
    return entity ? entityToRecord(entity) : null
  }

  /**
   * List entities of a given type
   */
  async list(type: string, options?: ListOptions): Promise<Record<string, unknown>[]> {
    const namespace = typeToNamespace(type)
    const { filter, findOptions } = convertListOptions(options)

    const result = await this.db.find(namespace, filter, findOptions)
    return result.items.map(entityToRecord)
  }

  /**
   * Search entities using full-text search
   */
  async search(type: string, query: string, options?: SearchOptions): Promise<Record<string, unknown>[]> {
    const namespace = typeToNamespace(type)
    const { filter, findOptions } = convertListOptions(options)

    // Combine FTS with any existing filters
    const searchFilter: Filter = {
      ...filter,
      $text: {
        $search: query,
        ...(options?.fields ? { $fields: options.fields } : {}),
        ...(options?.minScore ? { $minScore: options.minScore } : {}),
      },
    }

    const result = await this.db.find(namespace, searchFilter, findOptions)
    return result.items.map(entityToRecord)
  }

  // ===========================================================================
  // Write Operations
  // ===========================================================================

  /**
   * Create a new entity
   */
  async create(
    type: string,
    id: string | undefined,
    data: Record<string, unknown>
  ): Promise<Record<string, unknown>> {
    const namespace = typeToNamespace(type)
    const entityData = {
      ...data,
      $type: type,
      name: (data.name as string) || (data.title as string) || id || generateId(),
      ...(id ? { $id: `${namespace}/${id}` } : {}),
    }

    const entity = await this.db.create(namespace, entityData)
    return entityToRecord(entity)
  }

  /**
   * Update an entity
   */
  async update(
    type: string,
    id: string,
    data: Record<string, unknown>
  ): Promise<Record<string, unknown>> {
    const namespace = typeToNamespace(type)
    const localId = stripNamespace(id)

    const updateOp: UpdateInput = {
      $set: data,
    }

    const entity = await this.db.update(namespace, localId, updateOp, { upsert: false })
    if (!entity) {
      throw new Error(`Entity not found: ${type}/${id}`)
    }
    return entityToRecord(entity)
  }

  /**
   * Delete an entity
   */
  async delete(type: string, id: string): Promise<boolean> {
    const namespace = typeToNamespace(type)
    const localId = stripNamespace(id)

    const result = await this.db.delete(namespace, localId)
    return result.deletedCount > 0
  }

  // ===========================================================================
  // Relationship Operations
  // ===========================================================================

  /**
   * Get related entities
   *
   * When batch loading is enabled (default), this method will automatically
   * batch multiple relationship queries together to eliminate N+1 queries.
   *
   * @example
   * ```typescript
   * // These will be batched together when called in parallel
   * const [author1, author2, author3] = await Promise.all([
   *   adapter.related('Post', 'post-1', 'author'),
   *   adapter.related('Post', 'post-2', 'author'),
   *   adapter.related('Post', 'post-3', 'author'),
   * ])
   * ```
   */
  async related(type: string, id: string, relation: string): Promise<Record<string, unknown>[]> {
    const namespace = typeToNamespace(type)
    const localId = stripNamespace(id)

    // Use batch loader if available
    if (this.batchLoader) {
      const entities = await this.batchLoader.load(type, id, relation)
      if (entities.length > 0) {
        return entities.map(entityToRecord)
      }
      // Fall through to direct field access if batch loader returns empty
    } else {
      // First try getRelated (for schema-based relationships)
      const result = await this.db.getRelated(namespace, localId, relation)
      if (result.items.length > 0) {
        return result.items.map(entityToRecord)
      }
    }

    // Fallback: direct field access for schema-less relationships
    const entity = await this.db.get(namespace, localId)
    if (!entity) {
      return []
    }

    // Read the relationship field directly (it's stored as { 'Name': 'ns/id' })
    const relField = (entity as Record<string, unknown>)[relation]
    if (!relField || typeof relField !== 'object' || Array.isArray(relField)) {
      return []
    }

    // Resolve referenced entities
    const results: Record<string, unknown>[] = []
    for (const [, targetId] of Object.entries(relField)) {
      if (typeof targetId === 'string' && targetId.includes('/')) {
        const [targetNs, ...targetIdParts] = targetId.split('/')
        if (targetNs) {
          const targetEntity = await this.db.get(targetNs, targetIdParts.join('/'))
          if (targetEntity) {
            results.push(entityToRecord(targetEntity))
          }
        }
      }
    }

    return results
  }

  /**
   * Create a relationship between entities
   */
  async relate(
    fromType: string,
    fromId: string,
    relation: string,
    toType: string,
    toId: string,
    metadata?: RelationMetadata
  ): Promise<void> {
    const fromNamespace = typeToNamespace(fromType)
    const fromLocalId = stripNamespace(fromId)
    const toNamespace = typeToNamespace(toType)
    const toLocalId = stripNamespace(toId)

    // Use $link operator to create relationship
    const linkOp: UpdateInput = {
      $link: {
        [relation]: `${toNamespace}/${toLocalId}`,
      },
    }

    // If metadata provided, store it in a separate field
    if (metadata) {
      (linkOp.$set as Record<string, unknown>) = linkOp.$set || {}
      ;(linkOp.$set as Record<string, unknown>)[`_rel_${relation}_meta`] = metadata
    }

    await this.db.update(fromNamespace, fromLocalId, linkOp)
  }

  /**
   * Remove a relationship between entities
   */
  async unrelate(
    fromType: string,
    fromId: string,
    relation: string,
    toType: string,
    toId: string
  ): Promise<void> {
    const fromNamespace = typeToNamespace(fromType)
    const fromLocalId = stripNamespace(fromId)
    const toNamespace = typeToNamespace(toType)
    const toLocalId = stripNamespace(toId)

    // Use $unlink operator to remove relationship
    const unlinkOp: UpdateInput = {
      $unlink: {
        [relation]: `${toNamespace}/${toLocalId}`,
      },
    }

    await this.db.update(fromNamespace, fromLocalId, unlinkOp)
  }

  // ===========================================================================
  // Transaction Support
  // ===========================================================================

  /**
   * Begin a new transaction
   */
  async beginTransaction(): Promise<Transaction> {
    const dbTransaction = this.db.beginTransaction()

    // Wrap ParqueDB transaction in ai-database Transaction interface
    return {
      get: async (type: string, id: string) => {
        return this.get(type, id)
      },
      create: async (type: string, id: string | undefined, data: Record<string, unknown>) => {
        return this.create(type, id, data)
      },
      update: async (type: string, id: string, data: Record<string, unknown>) => {
        return this.update(type, id, data)
      },
      delete: async (type: string, id: string) => {
        return this.delete(type, id)
      },
      relate: async (fromType, fromId, relation, toType, toId, metadata) => {
        return this.relate(fromType, fromId, relation, toType, toId, metadata)
      },
      commit: async () => {
        await dbTransaction.commit()
      },
      rollback: async () => {
        await dbTransaction.rollback()
      },
    }
  }

  // ===========================================================================
  // Embeddings Configuration
  // ===========================================================================

  /**
   * Configure embeddings for auto-generation
   */
  setEmbeddingsConfig(config: EmbeddingsConfig): void {
    this.embeddingsConfig = config
  }

  // ===========================================================================
  // Semantic Search
  // ===========================================================================

  /**
   * Semantic search using vector similarity
   * Falls back to FTS if no embedding provider is configured
   */
  async semanticSearch(
    type: string,
    query: string,
    options?: SemanticSearchOptions
  ): Promise<SemanticSearchResult[]> {
    const namespace = typeToNamespace(type)
    const limit = options?.limit || 10

    try {
      // Try vector search first if embedding provider is configured
      const filter: Filter = {
        $vector: {
          $near: query, // Will be converted to embedding
          $k: limit,
          ...(options?.minScore ? { $minScore: options.minScore } : {}),
        },
      }

      const result = await this.db.find(namespace, filter)

      return result.items.map((entity, index) => ({
        ...entityToRecord(entity),
        $id: entity.$id,
        $type: entity.$type,
        // Score decreases with rank (approximate)
        $score: 1 - index * 0.1,
      }))
    } catch (err) {
      // Fall back to FTS if vector search is not available
      const errMessage = err instanceof Error ? err.message : ''
      if (errMessage.includes('embedding provider')) {
        // Use FTS as fallback
        const ftsFilter: Filter = {
          $text: { $search: query },
        }
        const result = await this.db.find(namespace, ftsFilter, { limit })

        return result.items.map((entity, index) => ({
          ...entityToRecord(entity),
          $id: entity.$id,
          $type: entity.$type,
          // Score decreases with rank (approximate)
          $score: 1 - index * 0.1,
        }))
      }
      throw err
    }
  }

  /**
   * Hybrid search combining FTS and semantic
   */
  async hybridSearch(
    type: string,
    query: string,
    options?: HybridSearchOptions
  ): Promise<HybridSearchResult[]> {
    const namespace = typeToNamespace(type)

    // Perform both FTS and semantic search, then combine using RRF
    const limit = options?.limit || 10
    const rrfK = options?.rrfK || 60
    const ftsWeight = options?.ftsWeight ?? 0.5
    const semanticWeight = options?.semanticWeight ?? 0.5

    // FTS search
    const ftsFilter: Filter = {
      $text: { $search: query },
    }
    const ftsResult = await this.db.find(namespace, ftsFilter, { limit: limit * 2 })

    // Semantic search
    const semanticResults = await this.semanticSearch(type, query, {
      limit: limit * 2,
      minScore: options?.minScore,
    })

    // Create rank maps
    const ftsRanks = new Map<string, number>()
    ftsResult.items.forEach((item, idx) => {
      ftsRanks.set(item.$id, idx + 1)
    })

    const semanticRanks = new Map<string, number>()
    semanticResults.forEach((item, idx) => {
      semanticRanks.set(item.$id, idx + 1)
    })

    // Collect all unique IDs
    const allIds = new Set([...ftsRanks.keys(), ...semanticRanks.keys()])

    // Calculate RRF scores
    const scores: Array<{
      id: string
      rrfScore: number
      ftsRank: number
      semanticRank: number
      entity: Record<string, unknown>
    }> = []

    for (const id of allIds) {
      const ftsRank = ftsRanks.get(id) || Infinity
      const semanticRank = semanticRanks.get(id) || Infinity

      // RRF formula: sum of 1/(k + rank) for each retriever
      const ftsScore = ftsRank < Infinity ? ftsWeight / (rrfK + ftsRank) : 0
      const semanticScore = semanticRank < Infinity ? semanticWeight / (rrfK + semanticRank) : 0
      const rrfScore = ftsScore + semanticScore

      // Find the entity
      let entity: Record<string, unknown> | undefined
      const ftsEntity = ftsResult.items.find(e => e.$id === id)
      if (ftsEntity) {
        entity = entityToRecord(ftsEntity)
      } else {
        const semEntity = semanticResults.find(e => e.$id === id)
        if (semEntity) {
          entity = semEntity
        }
      }

      if (entity) {
        scores.push({
          id,
          rrfScore,
          ftsRank: ftsRank === Infinity ? -1 : ftsRank,
          semanticRank: semanticRank === Infinity ? -1 : semanticRank,
          entity,
        })
      }
    }

    // Sort by RRF score descending
    scores.sort((a, b) => b.rrfScore - a.rrfScore)

    // Apply offset and limit
    const offset = options?.offset || 0
    const paginatedScores = scores.slice(offset, offset + limit)

    return paginatedScores.map(s => ({
      ...s.entity,
      $id: s.id,
      $type: s.entity.$type as string,
      $score: s.rrfScore,
      $rrfScore: s.rrfScore,
      $ftsRank: s.ftsRank,
      $semanticRank: s.semanticRank,
    }))
  }

  // ===========================================================================
  // Events API
  // ===========================================================================

  /**
   * Subscribe to events matching a pattern
   */
  on(pattern: string, handler: (event: DBEvent) => void | Promise<void>): () => void {
    if (!this.eventHandlers.has(pattern)) {
      this.eventHandlers.set(pattern, new Set())
    }
    this.eventHandlers.get(pattern)!.add(handler)

    // Return unsubscribe function
    return () => {
      this.eventHandlers.get(pattern)?.delete(handler)
    }
  }

  /**
   * Emit an event
   */
  async emit(optionsOrType: CreateEventOptions | string, data?: unknown): Promise<DBEvent> {
    const eventsNamespace = 'sysevents'

    let eventData: DBEvent

    if (typeof optionsOrType === 'string') {
      // Legacy format: emit(type, data)
      eventData = {
        id: generateId(),
        actor: 'system',
        event: optionsOrType,
        objectData: data as Record<string, unknown>,
        timestamp: new Date(),
      }
    } else {
      // New format: emit(CreateEventOptions)
      eventData = {
        id: generateId(),
        actor: optionsOrType.actor,
        event: optionsOrType.event,
        object: optionsOrType.object,
        objectData: optionsOrType.objectData,
        result: optionsOrType.result,
        resultData: optionsOrType.resultData,
        meta: optionsOrType.meta,
        timestamp: new Date(),
      }
    }

    // Store the event
    await this.db.create(eventsNamespace, {
      $type: 'Event',
      name: eventData.event,
      ...eventData,
    })

    // Notify handlers
    for (const [pattern, handlers] of this.eventHandlers) {
      if (this.matchesEventPattern(eventData.event, pattern)) {
        for (const handler of handlers) {
          await handler(eventData)
        }
      }
    }

    return eventData
  }

  /**
   * List events with optional filters
   */
  async listEvents(options?: {
    event?: string
    actor?: string
    object?: string
    since?: Date
    until?: Date
    limit?: number
  }): Promise<DBEvent[]> {
    const eventsNamespace = 'sysevents'

    const filter: Filter = {}

    if (options?.event) {
      filter.event = options.event
    }
    if (options?.actor) {
      filter.actor = options.actor
    }
    if (options?.object) {
      filter.object = options.object
    }
    if (options?.since || options?.until) {
      filter.timestamp = {}
      if (options?.since) {
        (filter.timestamp as Record<string, Date>).$gte = options.since
      }
      if (options?.until) {
        (filter.timestamp as Record<string, Date>).$lte = options.until
      }
    }

    const result = await this.db.find(eventsNamespace, filter, {
      sort: { timestamp: -1 },
      limit: options?.limit,
    })

    return result.items.map(entity => ({
      id: entity.$id,
      actor: (entity as Record<string, unknown>).actor as string,
      event: (entity as Record<string, unknown>).event as string,
      object: (entity as Record<string, unknown>).object as string | undefined,
      objectData: (entity as Record<string, unknown>).objectData as Record<string, unknown> | undefined,
      result: (entity as Record<string, unknown>).result as string | undefined,
      resultData: (entity as Record<string, unknown>).resultData as Record<string, unknown> | undefined,
      meta: (entity as Record<string, unknown>).meta as Record<string, unknown> | undefined,
      timestamp: (entity as Record<string, unknown>).timestamp as Date,
    }))
  }

  /**
   * Replay events through a handler
   */
  async replayEvents(options: {
    event?: string
    actor?: string
    since?: Date
    handler: (event: DBEvent) => void | Promise<void>
  }): Promise<void> {
    const events = await this.listEvents({
      event: options.event,
      actor: options.actor,
      since: options.since,
    })

    // Replay in chronological order
    events.reverse()

    for (const event of events) {
      await options.handler(event)
    }
  }

  // ===========================================================================
  // Actions API
  // ===========================================================================

  /**
   * Create a new action
   */
  async createAction(
    options: CreateActionOptions | { type: string; data: unknown; total?: number }
  ): Promise<DBAction> {
    const actionsNamespace = 'sysactions'

    let actionFields: Omit<DBAction, 'id'>

    if ('type' in options) {
      // Legacy format
      actionFields = {
        actor: 'system',
        action: options.type,
        act: this.conjugateVerb(options.type).act,
        activity: this.conjugateVerb(options.type).activity,
        objectData: options.data as Record<string, unknown>,
        status: 'pending',
        total: options.total,
        createdAt: new Date(),
      }
    } else {
      // New format
      const verb = this.conjugateVerb(options.action)
      actionFields = {
        actor: options.actor,
        action: options.action,
        act: verb.act,
        activity: verb.activity,
        object: options.object,
        objectData: options.objectData,
        status: 'pending',
        total: options.total,
        meta: options.meta,
        createdAt: new Date(),
      }
    }

    const entity = await this.db.create(actionsNamespace, {
      $type: 'Action',
      name: actionFields.action,
      ...actionFields,
    })

    // Return the action with the entity's $id
    return this.entityToAction(entity)
  }

  /**
   * Get an action by ID
   */
  async getAction(id: string): Promise<DBAction | null> {
    const actionsNamespace = 'sysactions'
    // ID might be full (sysactions/xxx) or just the local part
    const localId = stripNamespace(id)
    const entity = await this.db.get(actionsNamespace, localId)

    if (!entity) return null

    return this.entityToAction(entity)
  }

  /**
   * Update an action
   */
  async updateAction(
    id: string,
    updates: Partial<Pick<DBAction, 'status' | 'progress' | 'result' | 'error'>>
  ): Promise<DBAction> {
    const actionsNamespace = 'sysactions'
    // ID might be full (sysactions/xxx) or just the local part
    const localId = stripNamespace(id)

    const updateData: Record<string, unknown> = { ...updates }

    // Set timestamps based on status
    if (updates.status === 'active' && !updateData.startedAt) {
      updateData.startedAt = new Date()
    }
    if ((updates.status === 'completed' || updates.status === 'failed') && !updateData.completedAt) {
      updateData.completedAt = new Date()
    }

    const entity = await this.db.update(actionsNamespace, localId, { $set: updateData })

    if (!entity) {
      throw new Error(`Action not found: ${id}`)
    }

    return this.entityToAction(entity)
  }

  /**
   * List actions with optional filters
   */
  async listActions(options?: {
    status?: DBAction['status']
    action?: string
    actor?: string
    object?: string
    since?: Date
    until?: Date
    limit?: number
  }): Promise<DBAction[]> {
    const actionsNamespace = 'sysactions'

    const filter: Filter = {}

    if (options?.status) {
      filter.status = options.status
    }
    if (options?.action) {
      filter.action = options.action
    }
    if (options?.actor) {
      filter.actor = options.actor
    }
    if (options?.object) {
      filter.object = options.object
    }
    if (options?.since || options?.until) {
      filter.createdAt = {}
      if (options?.since) {
        (filter.createdAt as Record<string, Date>).$gte = options.since
      }
      if (options?.until) {
        (filter.createdAt as Record<string, Date>).$lte = options.until
      }
    }

    const result = await this.db.find(actionsNamespace, filter, {
      sort: { createdAt: -1 },
      limit: options?.limit,
    })

    return result.items.map(entity => this.entityToAction(entity))
  }

  /**
   * Retry a failed action
   */
  async retryAction(id: string): Promise<DBAction> {
    const action = await this.getAction(id)
    if (!action) {
      throw new Error(`Action not found: ${id}`)
    }

    if (action.status !== 'failed') {
      throw new Error(`Can only retry failed actions, current status: ${action.status}`)
    }

    return this.updateAction(id, {
      status: 'pending',
      error: undefined,
      progress: 0,
    })
  }

  /**
   * Cancel an action
   */
  async cancelAction(id: string): Promise<void> {
    const action = await this.getAction(id)
    if (!action) {
      throw new Error(`Action not found: ${id}`)
    }

    if (action.status === 'completed' || action.status === 'failed') {
      throw new Error(`Cannot cancel action with status: ${action.status}`)
    }

    await this.updateAction(id, { status: 'cancelled' })
  }

  // ===========================================================================
  // Artifacts API
  // ===========================================================================

  /**
   * Get an artifact by URL and type
   */
  async getArtifact(url: string, type: string): Promise<DBArtifact | null> {
    const artifactsNamespace = 'sysartifacts'

    const result = await this.db.find(artifactsNamespace, { url, type }, { limit: 1 })

    if (result.items.length === 0) return null

    const entity = result.items[0]
    return {
      url: (entity as Record<string, unknown>).url as string,
      type: (entity as Record<string, unknown>).type as string,
      sourceHash: (entity as Record<string, unknown>).sourceHash as string,
      content: (entity as Record<string, unknown>).content,
      metadata: (entity as Record<string, unknown>).metadata as Record<string, unknown> | undefined,
      createdAt: (entity as Record<string, unknown>).createdAt as Date,
    }
  }

  /**
   * Set an artifact
   */
  async setArtifact(
    url: string,
    type: string,
    data: { content: unknown; sourceHash: string; metadata?: Record<string, unknown> }
  ): Promise<void> {
    const artifactsNamespace = 'sysartifacts'

    // Check if artifact already exists
    const existing = await this.getArtifact(url, type)

    const artifactData = {
      url,
      type,
      sourceHash: data.sourceHash,
      content: data.content,
      metadata: data.metadata,
      createdAt: new Date(),
    }

    if (existing) {
      // Update existing
      const result = await this.db.find(artifactsNamespace, { url, type }, { limit: 1 })
      if (result.items.length > 0) {
        await this.db.update(artifactsNamespace, result.items[0].$id, {
          $set: artifactData,
        })
      }
    } else {
      // Create new
      await this.db.create(artifactsNamespace, {
        $type: 'Artifact',
        name: `${type}:${url}`,
        ...artifactData,
      })
    }
  }

  /**
   * Delete an artifact
   */
  async deleteArtifact(url: string, type?: string): Promise<void> {
    const artifactsNamespace = 'sysartifacts'

    const filter: Filter = { url }
    if (type) {
      filter.type = type
    }

    const result = await this.db.find(artifactsNamespace, filter)

    for (const entity of result.items) {
      await this.db.delete(artifactsNamespace, entity.$id)
    }
  }

  /**
   * List artifacts for a URL
   */
  async listArtifacts(url: string): Promise<DBArtifact[]> {
    const artifactsNamespace = 'sysartifacts'

    const result = await this.db.find(artifactsNamespace, { url })

    return result.items.map(entity => ({
      url: (entity as Record<string, unknown>).url as string,
      type: (entity as Record<string, unknown>).type as string,
      sourceHash: (entity as Record<string, unknown>).sourceHash as string,
      content: (entity as Record<string, unknown>).content,
      metadata: (entity as Record<string, unknown>).metadata as Record<string, unknown> | undefined,
      createdAt: (entity as Record<string, unknown>).createdAt as Date,
    }))
  }

  // ===========================================================================
  // Private Helper Methods
  // ===========================================================================

  /**
   * Check if an event type matches a pattern
   */
  private matchesEventPattern(eventType: string, pattern: string): boolean {
    if (pattern === '*') return true
    if (pattern.endsWith('.*')) {
      const prefix = pattern.slice(0, -2)
      return eventType.startsWith(prefix + '.')
    }
    return eventType === pattern
  }

  /**
   * Convert an entity to a DBAction
   */
  private entityToAction(entity: Entity): DBAction {
    const record = entity as Record<string, unknown>
    return {
      id: entity.$id,
      actor: record.actor as string,
      action: record.action as string,
      act: record.act as string,
      activity: record.activity as string,
      object: record.object as string | undefined,
      objectData: record.objectData as Record<string, unknown> | undefined,
      status: record.status as DBAction['status'],
      progress: record.progress as number | undefined,
      total: record.total as number | undefined,
      result: record.result as Record<string, unknown> | undefined,
      error: record.error as string | undefined,
      meta: record.meta as Record<string, unknown> | undefined,
      createdAt: record.createdAt as Date,
      startedAt: record.startedAt as Date | undefined,
      completedAt: record.completedAt as Date | undefined,
    }
  }

  /**
   * Conjugate a verb to get act and activity forms
   */
  private conjugateVerb(action: string): { act: string; activity: string } {
    // Simple verb conjugation rules
    const base = action.toLowerCase()

    // Activity: add -ing
    let activity: string
    if (base.endsWith('e')) {
      activity = base.slice(0, -1) + 'ing'
    } else if (base.endsWith('ie')) {
      activity = base.slice(0, -2) + 'ying'
    } else if (
      base.length > 2 &&
      // Check for CVC pattern: vowel before final consonant
      ['a', 'e', 'i', 'o', 'u'].includes(base[base.length - 2]!) &&
      !['a', 'e', 'i', 'o', 'u'].includes(base[base.length - 1]!) &&
      !base.endsWith('w') &&
      !base.endsWith('x') &&
      !base.endsWith('y')
    ) {
      // Double final consonant for CVC pattern (e.g., run -> running)
      activity = base + base[base.length - 1] + 'ing'
    } else {
      activity = base + 'ing'
    }

    // Act: add -s (3rd person singular)
    let act: string
    if (base.endsWith('s') || base.endsWith('x') || base.endsWith('z') ||
        base.endsWith('ch') || base.endsWith('sh')) {
      act = base + 'es'
    } else if (base.endsWith('y') && !['a', 'e', 'i', 'o', 'u'].includes(base[base.length - 2]!)) {
      act = base.slice(0, -1) + 'ies'
    } else {
      act = base + 's'
    }

    return { act, activity }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a ParqueDB provider for ai-database
 *
 * @param db - ParqueDB instance to wrap
 * @param options - Adapter options including batch loader configuration
 * @returns A DBProviderExtended implementation
 *
 * @example
 * ```typescript
 * import { ParqueDB, MemoryBackend } from 'parquedb'
 * import { createParqueDBProvider } from 'parquedb/integrations/ai-database'
 *
 * const db = new ParqueDB({ storage: new MemoryBackend() })
 * const provider = createParqueDBProvider(db)
 *
 * // Use with ai-database
 * setProvider(provider)
 * ```
 *
 * @example
 * ```typescript
 * // With custom batch loader options
 * const provider = createParqueDBProvider(db, {
 *   batchLoaderOptions: {
 *     windowMs: 20,     // 20ms batching window
 *     maxBatchSize: 50  // Flush after 50 requests
 *   }
 * })
 * ```
 */
export function createParqueDBProvider(
  db: ParqueDB,
  options?: ParqueDBAdapterOptions
): DBProviderExtended {
  return new ParqueDBAdapter(db, options)
}
