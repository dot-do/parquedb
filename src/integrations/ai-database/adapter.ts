/**
 * ParqueDB Adapter for ai-database
 *
 * This adapter allows ParqueDB to be used as a backend for ai-database,
 * providing all required DBProvider and DBProviderExtended interface methods.
 *
 * @packageDocumentation
 */

import type { ParqueDB } from '../../ParqueDB'
import type { Entity, EntityId, Filter, UpdateInput } from '../../types'
import { RelationshipBatchLoader, type BatchLoaderOptions, type BatchLoaderDB } from '../../relationships/batch-loader'
import type { EmbeddingProvider } from '../../embeddings/provider'

// =============================================================================
// Types from ai-database (replicated to avoid circular dependency)
// =============================================================================

/**
 * List options for querying entities
 */
export interface ListOptions {
  where?: Record<string, unknown> | undefined
  orderBy?: string | undefined
  order?: 'asc' | 'desc' | undefined
  limit?: number | undefined
  offset?: number | undefined
}

/**
 * Search options extending list options
 */
export interface SearchOptions extends ListOptions {
  fields?: string[] | undefined
  minScore?: number | undefined
}

/**
 * Semantic search options
 */
export interface SemanticSearchOptions {
  minScore?: number | undefined
  limit?: number | undefined
}

/**
 * Hybrid search options
 */
export interface HybridSearchOptions {
  minScore?: number | undefined
  limit?: number | undefined
  offset?: number | undefined
  rrfK?: number | undefined
  ftsWeight?: number | undefined
  semanticWeight?: number | undefined
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
  matchMode?: 'exact' | 'fuzzy' | undefined
  similarity?: number | undefined
  matchedType?: string | undefined
  [key: string]: unknown
}

/**
 * Event data structure
 */
export interface DBEvent {
  id: string
  actor: string
  event: string
  object?: string | undefined
  objectData?: Record<string, unknown> | undefined
  result?: string | undefined
  resultData?: Record<string, unknown> | undefined
  meta?: Record<string, unknown> | undefined
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
  object?: string | undefined
  objectData?: Record<string, unknown> | undefined
  status: 'pending' | 'active' | 'completed' | 'failed' | 'cancelled'
  progress?: number | undefined
  total?: number | undefined
  result?: Record<string, unknown> | undefined
  error?: string | undefined
  meta?: Record<string, unknown> | undefined
  createdAt: Date
  startedAt?: Date | undefined
  completedAt?: Date | undefined
}

/**
 * Artifact data structure
 */
export interface DBArtifact {
  url: string
  type: string
  sourceHash: string
  content: unknown
  metadata?: Record<string, unknown> | undefined
  createdAt: Date
}

/**
 * Options for creating an event
 */
export interface CreateEventOptions {
  actor: string
  event: string
  object?: string | undefined
  objectData?: Record<string, unknown> | undefined
  result?: string | undefined
  resultData?: Record<string, unknown> | undefined
  meta?: Record<string, unknown> | undefined
}

/**
 * Options for creating an action
 */
export interface CreateActionOptions {
  actor: string
  action: string
  object?: string | undefined
  objectData?: Record<string, unknown> | undefined
  total?: number | undefined
  meta?: Record<string, unknown> | undefined
}

/**
 * Embeddings configuration
 */
export interface EmbeddingsConfig {
  provider?: string | undefined
  model?: string | undefined
  dimensions?: number | undefined
  /** Fields to embed per entity type: { Post: ['title', 'content'] } */
  fields?: Record<string, string[]> | undefined
  /** Target field to store embeddings (default: 'embedding') */
  vectorField?: string | undefined
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
    metadata?: RelationMetadata | undefined
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
    metadata?: RelationMetadata | undefined
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
    event?: string | undefined
    actor?: string | undefined
    object?: string | undefined
    since?: Date | undefined
    until?: Date | undefined
    limit?: number | undefined
  }): Promise<DBEvent[]>
  replayEvents(options: {
    event?: string | undefined
    actor?: string | undefined
    since?: Date | undefined
    handler: (event: DBEvent) => void | Promise<void>
  }): Promise<void>

  // Actions API
  createAction(options: CreateActionOptions | { type: string; data: unknown; total?: number | undefined }): Promise<DBAction>
  getAction(id: string): Promise<DBAction | null>
  updateAction(
    id: string,
    updates: Partial<Pick<DBAction, 'status' | 'progress' | 'result' | 'error'>>
  ): Promise<DBAction>
  listActions(options?: {
    status?: DBAction['status'] | undefined
    action?: string | undefined
    actor?: string | undefined
    object?: string | undefined
    since?: Date | undefined
    until?: Date | undefined
    limit?: number | undefined
  }): Promise<DBAction[]>
  retryAction(id: string): Promise<DBAction>
  cancelAction(id: string): Promise<void>

  // Artifacts API
  getArtifact(url: string, type: string): Promise<DBArtifact | null>
  setArtifact(
    url: string,
    type: string,
    data: { content: unknown; sourceHash: string; metadata?: Record<string, unknown> | undefined }
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
function convertListOptions(options?: ListOptions): { filter?: Filter | undefined; findOptions?: Record<string, unknown> | undefined } {
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
  enableBatchLoader?: boolean | undefined

  /**
   * Options for the relationship batch loader.
   * Only used if enableBatchLoader is true.
   */
  batchLoaderOptions?: BatchLoaderOptions | undefined

  /**
   * Embedding provider for auto-generating embeddings on create/update.
   *
   * When configured along with setEmbeddingsConfig(), embeddings will be
   * automatically generated for configured fields.
   *
   * @example
   * ```typescript
   * import { createWorkersAIProvider } from 'parquedb/embeddings'
   *
   * const adapter = new ParqueDBAdapter(db, {
   *   embeddingProvider: createWorkersAIProvider(env.AI)
   * })
   *
   * adapter.setEmbeddingsConfig({
   *   fields: { Post: ['title', 'content'] },
   *   vectorField: 'embedding'
   * })
   *
   * // Embeddings auto-generated on create
   * await adapter.create('Post', undefined, {
   *   title: 'Hello',
   *   content: 'World'
   * })
   * ```
   */
  embeddingProvider?: EmbeddingProvider | undefined
}

export class ParqueDBAdapter implements DBProviderExtended {
  private db: ParqueDB
  private embeddingsConfig: EmbeddingsConfig | null = null
  private embeddingProvider: EmbeddingProvider | null = null
  private eventHandlers = new Map<string, Set<(event: DBEvent) => void | Promise<void>>>()
  private batchLoader: RelationshipBatchLoader | null = null

  constructor(db: ParqueDB, options?: ParqueDBAdapterOptions) {
    this.db = db

    // Initialize batch loader if enabled (default: true)
    const enableBatchLoader = options?.enableBatchLoader ?? true
    if (enableBatchLoader) {
      this.batchLoader = new RelationshipBatchLoader(db as BatchLoaderDB, options?.batchLoaderOptions)
    }

    // Store embedding provider if provided
    if (options?.embeddingProvider) {
      this.embeddingProvider = options.embeddingProvider
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
   *
   * If embeddings are configured for this entity type and an embedding provider
   * is available, embeddings will be automatically generated for the configured fields.
   */
  async create(
    type: string,
    id: string | undefined,
    data: Record<string, unknown>
  ): Promise<Record<string, unknown>> {
    const namespace = typeToNamespace(type)

    // Generate embeddings if configured
    const dataWithEmbeddings = await this.generateEmbeddingsForType(type, data)

    const entityData = {
      ...dataWithEmbeddings,
      $type: type,
      name: (dataWithEmbeddings.name as string) || (dataWithEmbeddings.title as string) || id || generateId(),
      ...(id ? { $id: `${namespace}/${id}` } : {}),
    }

    const entity = await this.db.create(namespace, entityData)
    return entityToRecord(entity)
  }

  /**
   * Update an entity
   *
   * If embeddings are configured for this entity type and the update includes
   * fields that are configured for embedding, embeddings will be automatically
   * regenerated. The embedding is generated from the combined current state
   * of all configured fields (including both existing and updated values).
   */
  async update(
    type: string,
    id: string,
    data: Record<string, unknown>
  ): Promise<Record<string, unknown>> {
    const namespace = typeToNamespace(type)
    const localId = stripNamespace(id)

    // Check if we need to regenerate embeddings
    const updatedFields = Object.keys(data)
    const needsEmbedding = this.shouldRegenerateEmbeddings(type, updatedFields)

    let dataToSet = data

    if (needsEmbedding) {
      // Fetch existing entity to merge with update for complete text
      const existingEntity = await this.db.get(namespace, localId)
      if (existingEntity) {
        // Merge existing data with updates
        const mergedData = {
          ...entityToRecord(existingEntity),
          ...data,
        }
        // Generate embeddings from merged data
        dataToSet = await this.generateEmbeddingsForType(type, mergedData)
        // Only include the original update fields plus the embedding
        const vectorField = this.embeddingsConfig?.vectorField ?? 'embedding'
        dataToSet = {
          ...data,
          [vectorField]: dataToSet[vectorField],
        }
      }
    }

    const updateOp: UpdateInput = {
      $set: dataToSet,
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
    const targetId = `${toNamespace}/${toLocalId}` as EntityId
    const linkOp: UpdateInput = {
      $link: {
        [relation]: targetId,
      } as Record<string, EntityId>,
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
    const unlinkTargetId = `${toNamespace}/${toLocalId}` as EntityId
    const unlinkOp: UpdateInput = {
      $unlink: {
        [relation]: unlinkTargetId,
      } as Record<string, EntityId | EntityId[] | '$all'>,
    }

    await this.db.update(fromNamespace, fromLocalId, unlinkOp)
  }

  // ===========================================================================
  // Transaction Support
  // ===========================================================================

  /**
   * Begin a new transaction
   *
   * Returns a transaction that properly uses ParqueDB's native transaction support.
   * All operations within the transaction are tracked for rollback, and changes
   * are only committed when commit() is called.
   *
   * @example
   * ```typescript
   * const tx = await adapter.beginTransaction()
   * try {
   *   const user = await tx.create('User', undefined, { name: 'Alice' })
   *   await tx.relate('Post', postId, 'author', 'User', user.$id)
   *   await tx.commit()
   * } catch (error) {
   *   await tx.rollback()
   *   throw error
   * }
   * ```
   */
  async beginTransaction(): Promise<Transaction> {
    const dbTransaction = this.db.beginTransaction()
    const self = this

    // Wrap ParqueDB transaction in ai-database Transaction interface
    return {
      /**
       * Get an entity within the transaction.
       * Reads see in-progress changes from this transaction.
       */
      get: async (type: string, id: string) => {
        // Use the adapter's get method - it reads from the entity store which
        // includes uncommitted changes from this transaction
        return self.get(type, id)
      },

      /**
       * Create an entity within the transaction.
       * Uses the underlying ParqueDB transaction for proper rollback support.
       */
      create: async (type: string, id: string | undefined, data: Record<string, unknown>) => {
        const namespace = typeToNamespace(type)
        const entityData = {
          ...data,
          $type: type,
          name: (data.name as string) || (data.title as string) || id || generateId(),
          ...(id ? { $id: `${namespace}/${id}` } : {}),
        }

        const entity = await dbTransaction.create(namespace, entityData)
        return entityToRecord(entity)
      },

      /**
       * Update an entity within the transaction.
       * Uses the underlying ParqueDB transaction for proper rollback support.
       */
      update: async (type: string, id: string, data: Record<string, unknown>) => {
        const namespace = typeToNamespace(type)
        const localId = stripNamespace(id)

        const updateOp: UpdateInput = {
          $set: data,
        }

        const entity = await dbTransaction.update(namespace, localId, updateOp, { upsert: false })
        if (!entity) {
          throw new Error(`Entity not found: ${type}/${id}`)
        }
        return entityToRecord(entity)
      },

      /**
       * Delete an entity within the transaction.
       * Uses the underlying ParqueDB transaction for proper rollback support.
       */
      delete: async (type: string, id: string) => {
        const namespace = typeToNamespace(type)
        const localId = stripNamespace(id)

        const result = await dbTransaction.delete(namespace, localId)
        return result.deletedCount > 0
      },

      /**
       * Create a relationship within the transaction.
       * Uses the underlying ParqueDB transaction's update method for proper rollback support.
       */
      relate: async (fromType, fromId, relation, toType, toId, metadata) => {
        const fromNamespace = typeToNamespace(fromType)
        const fromLocalId = stripNamespace(fromId)
        const toNamespace = typeToNamespace(toType)
        const toLocalId = stripNamespace(toId)

        // Use $link operator to create relationship
        const txTargetId = `${toNamespace}/${toLocalId}` as EntityId
        const linkOp: UpdateInput = {
          $link: {
            [relation]: txTargetId,
          } as Record<string, EntityId>,
        }

        // If metadata provided, store it in a separate field
        if (metadata) {
          (linkOp.$set as Record<string, unknown>) = linkOp.$set || {}
          ;(linkOp.$set as Record<string, unknown>)[`_rel_${relation}_meta`] = metadata
        }

        await dbTransaction.update(fromNamespace, fromLocalId, linkOp)
      },

      /**
       * Commit the transaction.
       * Persists all changes and flushes events to the event log.
       */
      commit: async () => {
        await dbTransaction.commit()
      },

      /**
       * Rollback the transaction.
       * Discards all changes made within this transaction.
       */
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
   *
   * When an embedding provider is configured (either via constructor options
   * or by passing one here), embeddings will be automatically generated for
   * the configured fields on create() and update() operations.
   *
   * @param config - Embeddings configuration
   * @param provider - Optional embedding provider (overrides constructor option)
   *
   * @example
   * ```typescript
   * // Configure with provider
   * adapter.setEmbeddingsConfig({
   *   fields: { Post: ['title', 'content'] },
   *   vectorField: 'embedding'
   * }, createWorkersAIProvider(env.AI))
   *
   * // Or use provider from constructor
   * adapter.setEmbeddingsConfig({
   *   fields: { Post: ['title', 'content'] }
   * })
   * ```
   */
  setEmbeddingsConfig(config: EmbeddingsConfig, provider?: EmbeddingProvider): void {
    this.embeddingsConfig = config
    if (provider) {
      this.embeddingProvider = provider
    }
  }

  /**
   * Get the current embeddings configuration
   */
  getEmbeddingsConfig(): EmbeddingsConfig | null {
    return this.embeddingsConfig
  }

  /**
   * Generate embeddings for entity data based on embeddings config
   *
   * @param type - Entity type
   * @param data - Entity data
   * @returns Updated data with embeddings, or original data if no embedding needed
   */
  private async generateEmbeddingsForType(
    type: string,
    data: Record<string, unknown>
  ): Promise<Record<string, unknown>> {
    // Skip if no config or provider
    if (!this.embeddingsConfig || !this.embeddingProvider) {
      return data
    }

    // Check if this type has configured fields
    const fieldsToEmbed = this.embeddingsConfig.fields?.[type]
    if (!fieldsToEmbed || fieldsToEmbed.length === 0) {
      return data
    }

    // Collect text from configured fields
    const texts: string[] = []
    for (const field of fieldsToEmbed) {
      const value = data[field]
      if (typeof value === 'string' && value.trim()) {
        texts.push(value.trim())
      }
    }

    // Skip if no text to embed
    if (texts.length === 0) {
      return data
    }

    // Concatenate texts with newlines and generate embedding
    const textToEmbed = texts.join('\n\n')
    const embedding = await this.embeddingProvider.embed(textToEmbed)

    // Store embedding in configured vector field (default: 'embedding')
    const vectorField = this.embeddingsConfig.vectorField ?? 'embedding'
    return {
      ...data,
      [vectorField]: embedding,
    }
  }

  /**
   * Check if any of the updated fields require re-generating embeddings
   *
   * @param type - Entity type
   * @param updatedFields - Fields being updated
   * @returns True if embeddings should be regenerated
   */
  private shouldRegenerateEmbeddings(type: string, updatedFields: string[]): boolean {
    if (!this.embeddingsConfig || !this.embeddingProvider) {
      return false
    }

    const fieldsToEmbed = this.embeddingsConfig.fields?.[type]
    if (!fieldsToEmbed || fieldsToEmbed.length === 0) {
      return false
    }

    // Check if any updated field is in the embed fields list
    return updatedFields.some(field => fieldsToEmbed.includes(field))
  }

  // ===========================================================================
  // Semantic Search
  // ===========================================================================

  /**
   * Semantic search using vector similarity
   *
   * Requires an embedding provider to be configured via setEmbeddingsConfig()
   * or passed in the adapter options.
   *
   * Returns results with actual similarity scores from the vector index.
   *
   * @throws Error if no embedding provider is configured
   */
  async semanticSearch(
    type: string,
    query: string,
    options?: SemanticSearchOptions
  ): Promise<SemanticSearchResult[]> {
    const namespace = typeToNamespace(type)
    const limit = options?.limit || 10

    // Convert text query to vector using the adapter's embedding provider
    if (!this.embeddingProvider) {
      throw new Error(
        'Semantic search requires an embedding provider. ' +
        'Configure one via setEmbeddingsConfig() or pass embeddingProvider in adapter options.'
      )
    }

    // Generate embedding for the query text
    const queryVector = await this.embeddingProvider.embed(query)

    // Build the filter with the actual vector (not text)
    const filter: Filter = {
      $vector: {
        query: queryVector,
        field: 'embedding',
        topK: limit,
        ...(options?.minScore ? { minScore: options.minScore } : {}),
        // Legacy format for backward compatibility
        $near: queryVector,
        $k: limit,
        ...(options?.minScore ? { $minScore: options.minScore } : {}),
      },
    }

    const result = await this.db.find(namespace, filter, { limit })

    // Get actual similarity scores from the index manager
    const indexManager = this.db.getIndexManager()
    const indexes = await indexManager.listIndexes(namespace)
    const vectorIndexMeta = indexes.find(idx => idx.definition.type === 'vector')

    // Build a score map from document IDs to similarity scores
    const scoreMap = new Map<string, number>()

    if (vectorIndexMeta) {
      // Perform the vector search to get actual similarity scores
      const vectorResult = await indexManager.vectorSearch(
        namespace,
        vectorIndexMeta.definition.name,
        queryVector,
        limit,
        { minScore: options?.minScore }
      )

      // Map document IDs to their similarity scores
      vectorResult.docIds.forEach((docId, idx) => {
        const score = vectorResult.scores?.[idx] ?? 0
        // Store with both the raw docId and namespace-prefixed version
        scoreMap.set(docId, score)
        scoreMap.set(`${namespace}/${docId}`, score)
      })
    }

    // Map results and ensure limit is respected
    const results = result.items.slice(0, limit).map((entity) => {
      // Look up the actual similarity score, try both full ID and local ID
      const localId = entity.$id.includes('/') ? entity.$id.split('/').slice(1).join('/') : entity.$id
      const score = scoreMap.get(entity.$id) ?? scoreMap.get(localId) ?? 0

      return {
        ...entityToRecord(entity),
        $id: entity.$id,
        $type: entity.$type,
        $score: score,
      }
    })

    return results
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
    event?: string | undefined
    actor?: string | undefined
    object?: string | undefined
    since?: Date | undefined
    until?: Date | undefined
    limit?: number | undefined
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
    event?: string | undefined
    actor?: string | undefined
    since?: Date | undefined
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
    options: CreateActionOptions | { type: string; data: unknown; total?: number | undefined }
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
    status?: DBAction['status'] | undefined
    action?: string | undefined
    actor?: string | undefined
    object?: string | undefined
    since?: Date | undefined
    until?: Date | undefined
    limit?: number | undefined
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
    data: { content: unknown; sourceHash: string; metadata?: Record<string, unknown> | undefined }
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
      const firstItem = result.items[0]
      if (firstItem && firstItem.$id) {
        await this.db.update(artifactsNamespace, firstItem.$id, {
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
