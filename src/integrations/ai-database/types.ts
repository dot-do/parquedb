/**
 * ai-database Provider Interface Types
 *
 * This module defines the interface types for ai-database integration,
 * following the Interface Segregation Principle (ISP). Instead of one
 * large interface, we provide smaller, focused interfaces that can be
 * composed as needed.
 *
 * ## Interface Hierarchy
 *
 * ```
 * DBCrud (basic CRUD operations)
 *   |
 *   +-- DBRelationships (graph relationships)
 *   |     |
 *   |     +-- DBProvider = DBCrud + DBRelationships + basic search
 *   |
 *   +-- DBFullTextSearch (FTS search)
 *   +-- DBSemanticSearch (vector search)
 *   +-- DBHybridSearch (combined FTS + semantic)
 *   |     |
 *   |     +-- DBSearch = DBFullTextSearch + DBSemanticSearch + DBHybridSearch
 *   |
 *   +-- DBEvents (event sourcing)
 *   +-- DBActions (background job tracking)
 *   +-- DBArtifacts (content-addressable storage)
 *   +-- DBTransactions (ACID transactions)
 *         |
 *         +-- DBProviderExtended = DBProvider + DBSearch + DBEvents + DBActions + DBArtifacts
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// Supporting Types
// =============================================================================

/**
 * List options for querying entities
 */
export interface ListOptions {
  /** Filter criteria */
  where?: Record<string, unknown>
  /** Field to order by */
  orderBy?: string
  /** Sort order */
  order?: 'asc' | 'desc'
  /** Maximum number of results */
  limit?: number
  /** Number of results to skip */
  offset?: number
}

/**
 * Search options extending list options
 */
export interface SearchOptions extends ListOptions {
  /** Fields to search in (default: all text fields) */
  fields?: string[]
  /** Minimum relevance score (0.0 to 1.0) */
  minScore?: number
}

/**
 * Semantic search options
 */
export interface SemanticSearchOptions {
  /** Minimum similarity score (0.0 to 1.0) */
  minScore?: number
  /** Maximum number of results */
  limit?: number
}

/**
 * Hybrid search options
 */
export interface HybridSearchOptions {
  /** Minimum combined score (0.0 to 1.0) */
  minScore?: number
  /** Maximum number of results */
  limit?: number
  /** Number of results to skip */
  offset?: number
  /** RRF k parameter (default: 60) */
  rrfK?: number
  /** Weight for FTS component (default: 0.5) */
  ftsWeight?: number
  /** Weight for semantic component (default: 0.5) */
  semanticWeight?: number
}

/**
 * Semantic search result with score
 */
export interface SemanticSearchResult {
  /** Entity ID */
  $id: string
  /** Entity type */
  $type: string
  /** Similarity score (0.0 to 1.0) */
  $score: number
  /** Additional entity fields */
  [key: string]: unknown
}

/**
 * Hybrid search result with RRF and component scores
 */
export interface HybridSearchResult extends SemanticSearchResult {
  /** Combined RRF score */
  $rrfScore: number
  /** FTS rank (-1 if not found in FTS) */
  $ftsRank: number
  /** Semantic rank (-1 if not found in semantic) */
  $semanticRank: number
}

/**
 * Relationship metadata for graph edges
 */
export interface RelationMetadata {
  /** How the relationship was matched */
  matchMode?: 'exact' | 'fuzzy'
  /** Similarity score for fuzzy matches */
  similarity?: number
  /** Matched entity type (for polymorphic relationships) */
  matchedType?: string
  /** Additional edge properties */
  [key: string]: unknown
}

/**
 * Event data structure
 */
export interface DBEvent {
  /** Event ID (ULID) */
  id: string
  /** Actor who triggered the event */
  actor: string
  /** Event type (e.g., "user.created", "post.published") */
  event: string
  /** Object identifier (e.g., "users/alice") */
  object?: string
  /** Object data snapshot */
  objectData?: Record<string, unknown>
  /** Result identifier */
  result?: string
  /** Result data */
  resultData?: Record<string, unknown>
  /** Additional metadata */
  meta?: Record<string, unknown>
  /** When the event occurred */
  timestamp: Date
}

/**
 * Action/job status
 */
export type ActionStatus = 'pending' | 'active' | 'completed' | 'failed' | 'cancelled'

/**
 * Action data structure for background jobs
 */
export interface DBAction {
  /** Action ID */
  id: string
  /** Actor who initiated the action */
  actor: string
  /** 3rd person singular verb (e.g., "processes") */
  act: string
  /** Base action verb (e.g., "process") */
  action: string
  /** Present participle (e.g., "processing") */
  activity: string
  /** Object being acted upon */
  object?: string
  /** Object data */
  objectData?: Record<string, unknown>
  /** Current status */
  status: ActionStatus
  /** Progress count (0 to total) */
  progress?: number
  /** Total items to process */
  total?: number
  /** Result data on completion */
  result?: Record<string, unknown>
  /** Error message on failure */
  error?: string
  /** Additional metadata */
  meta?: Record<string, unknown>
  /** When the action was created */
  createdAt: Date
  /** When the action started processing */
  startedAt?: Date
  /** When the action completed (success or failure) */
  completedAt?: Date
}

/**
 * Artifact data structure for content-addressable storage
 */
export interface DBArtifact {
  /** Source URL (e.g., "https://example.com/doc") */
  url: string
  /** Artifact type (e.g., "markdown", "pdf", "html") */
  type: string
  /** Hash of the source content for cache invalidation */
  sourceHash: string
  /** Transformed/cached content */
  content: unknown
  /** Additional metadata */
  metadata?: Record<string, unknown>
  /** When the artifact was created/updated */
  createdAt: Date
}

/**
 * Options for creating an event
 */
export interface CreateEventOptions {
  /** Actor who triggered the event */
  actor: string
  /** Event type */
  event: string
  /** Object identifier */
  object?: string
  /** Object data snapshot */
  objectData?: Record<string, unknown>
  /** Result identifier */
  result?: string
  /** Result data */
  resultData?: Record<string, unknown>
  /** Additional metadata */
  meta?: Record<string, unknown>
}

/**
 * Options for creating an action
 */
export interface CreateActionOptions {
  /** Actor who initiated the action */
  actor: string
  /** Action verb (e.g., "process", "generate", "export") */
  action: string
  /** Object being acted upon */
  object?: string
  /** Object data */
  objectData?: Record<string, unknown>
  /** Total items to process (for progress tracking) */
  total?: number
  /** Additional metadata */
  meta?: Record<string, unknown>
}

/**
 * Embeddings configuration for auto-generation
 */
export interface EmbeddingsConfig {
  /** Embedding provider (e.g., "openai", "cohere") */
  provider?: string
  /** Model name (e.g., "text-embedding-3-small") */
  model?: string
  /** Embedding dimensions */
  dimensions?: number
  /** Fields to embed per entity type */
  fields?: Record<string, string[]>
}

/**
 * Transaction interface for atomic operations
 */
export interface Transaction {
  /** Get an entity within the transaction */
  get(type: string, id: string): Promise<Record<string, unknown> | null>
  /** Create an entity within the transaction */
  create(type: string, id: string | undefined, data: Record<string, unknown>): Promise<Record<string, unknown>>
  /** Update an entity within the transaction */
  update(type: string, id: string, data: Record<string, unknown>): Promise<Record<string, unknown>>
  /** Delete an entity within the transaction */
  delete(type: string, id: string): Promise<boolean>
  /** Create a relationship within the transaction */
  relate(
    fromType: string,
    fromId: string,
    relation: string,
    toType: string,
    toId: string,
    metadata?: RelationMetadata
  ): Promise<void>
  /** Commit the transaction */
  commit(): Promise<void>
  /** Rollback the transaction */
  rollback(): Promise<void>
}

// =============================================================================
// Focused Capability Interfaces
// =============================================================================

/**
 * CRUD operations interface
 *
 * Provides basic create, read, update, delete operations for entities.
 * This is the most fundamental interface that all database providers should implement.
 *
 * @example
 * ```typescript
 * const crud: DBCrud = provider
 *
 * // Create
 * const user = await crud.create('User', undefined, { name: 'Alice' })
 *
 * // Read
 * const found = await crud.get('User', user.$id)
 * const users = await crud.list('User', { where: { role: 'admin' } })
 *
 * // Update
 * await crud.update('User', user.$id, { name: 'Alice Smith' })
 *
 * // Delete
 * await crud.delete('User', user.$id)
 * ```
 */
export interface DBCrud {
  /**
   * Get a single entity by type and ID
   * @param type - Entity type (e.g., "User", "Post")
   * @param id - Entity ID
   * @returns The entity or null if not found
   */
  get(type: string, id: string): Promise<Record<string, unknown> | null>

  /**
   * List entities of a given type with optional filtering and pagination
   * @param type - Entity type
   * @param options - Query options (where, orderBy, limit, offset)
   * @returns Array of matching entities
   */
  list(type: string, options?: ListOptions): Promise<Record<string, unknown>[]>

  /**
   * Create a new entity
   * @param type - Entity type
   * @param id - Entity ID (optional, auto-generated if not provided)
   * @param data - Entity data
   * @returns The created entity
   */
  create(type: string, id: string | undefined, data: Record<string, unknown>): Promise<Record<string, unknown>>

  /**
   * Update an existing entity
   * @param type - Entity type
   * @param id - Entity ID
   * @param data - Fields to update
   * @returns The updated entity
   * @throws Error if entity not found
   */
  update(type: string, id: string, data: Record<string, unknown>): Promise<Record<string, unknown>>

  /**
   * Delete an entity
   * @param type - Entity type
   * @param id - Entity ID
   * @returns true if deleted, false if not found
   */
  delete(type: string, id: string): Promise<boolean>
}

/**
 * Relationship operations interface
 *
 * Provides graph-like relationship traversal and manipulation.
 * Implement this interface for providers that support entity relationships.
 *
 * @example
 * ```typescript
 * const rels: DBRelationships = provider
 *
 * // Create relationship
 * await rels.relate('User', 'alice', 'posts', 'Post', 'post-1')
 *
 * // Query related entities
 * const posts = await rels.related('User', 'alice', 'posts')
 *
 * // Remove relationship
 * await rels.unrelate('User', 'alice', 'posts', 'Post', 'post-1')
 * ```
 */
export interface DBRelationships {
  /**
   * Get related entities
   * @param type - Source entity type
   * @param id - Source entity ID
   * @param relation - Relationship name
   * @returns Array of related entities
   */
  related(type: string, id: string, relation: string): Promise<Record<string, unknown>[]>

  /**
   * Create a relationship between entities
   * @param fromType - Source entity type
   * @param fromId - Source entity ID
   * @param relation - Relationship name
   * @param toType - Target entity type
   * @param toId - Target entity ID
   * @param metadata - Optional edge properties
   */
  relate(
    fromType: string,
    fromId: string,
    relation: string,
    toType: string,
    toId: string,
    metadata?: RelationMetadata
  ): Promise<void>

  /**
   * Remove a relationship between entities
   * @param fromType - Source entity type
   * @param fromId - Source entity ID
   * @param relation - Relationship name
   * @param toType - Target entity type
   * @param toId - Target entity ID
   */
  unrelate(
    fromType: string,
    fromId: string,
    relation: string,
    toType: string,
    toId: string
  ): Promise<void>
}

/**
 * Full-text search interface
 *
 * Provides keyword-based search using full-text search indexes.
 * Implement this for providers that support FTS (Lucene, Elasticsearch, etc.).
 *
 * @example
 * ```typescript
 * const fts: DBFullTextSearch = provider
 *
 * // Basic search
 * const results = await fts.search('Article', 'TypeScript tutorial')
 *
 * // Search with options
 * const results = await fts.search('Article', 'React', {
 *   fields: ['title', 'content'],
 *   where: { published: true },
 *   limit: 10
 * })
 * ```
 */
export interface DBFullTextSearch {
  /**
   * Search entities using full-text search
   * @param type - Entity type to search
   * @param query - Search query string
   * @param options - Search options
   * @returns Array of matching entities
   */
  search(type: string, query: string, options?: SearchOptions): Promise<Record<string, unknown>[]>
}

/**
 * Semantic (vector) search interface
 *
 * Provides similarity search using vector embeddings.
 * Implement this for providers that support vector similarity search.
 *
 * @example
 * ```typescript
 * const semantic: DBSemanticSearch = provider
 *
 * // Configure embeddings
 * semantic.setEmbeddingsConfig({
 *   provider: 'openai',
 *   model: 'text-embedding-3-small',
 *   dimensions: 1536
 * })
 *
 * // Search by meaning
 * const similar = await semantic.semanticSearch('Article', 'machine learning concepts')
 * ```
 */
export interface DBSemanticSearch {
  /**
   * Configure embeddings for auto-generation
   * @param config - Embeddings configuration
   */
  setEmbeddingsConfig(config: EmbeddingsConfig): void

  /**
   * Search entities using vector similarity
   * @param type - Entity type to search
   * @param query - Query text (will be embedded)
   * @param options - Search options
   * @returns Array of results with similarity scores
   */
  semanticSearch(type: string, query: string, options?: SemanticSearchOptions): Promise<SemanticSearchResult[]>
}

/**
 * Hybrid search interface
 *
 * Combines full-text and semantic search using Reciprocal Rank Fusion (RRF).
 * Best for production search systems that need both keyword and semantic matching.
 *
 * @example
 * ```typescript
 * const hybrid: DBHybridSearch = provider
 *
 * // Hybrid search with custom weights
 * const results = await hybrid.hybridSearch('Article', 'TypeScript best practices', {
 *   ftsWeight: 0.3,
 *   semanticWeight: 0.7,
 *   limit: 20
 * })
 *
 * // Results include RRF scores
 * results.forEach(r => {
 *   console.log(`${r.$id}: RRF=${r.$rrfScore} FTS=${r.$ftsRank} Semantic=${r.$semanticRank}`)
 * })
 * ```
 */
export interface DBHybridSearch {
  /**
   * Search using combined FTS and semantic search with RRF
   * @param type - Entity type to search
   * @param query - Query text
   * @param options - Hybrid search options
   * @returns Array of results with RRF and component scores
   */
  hybridSearch(type: string, query: string, options?: HybridSearchOptions): Promise<HybridSearchResult[]>
}

/**
 * Combined search interface (FTS + Semantic + Hybrid)
 *
 * Implements all search capabilities. Use this when you need the full
 * search feature set. For more granular control, use the individual
 * search interfaces.
 */
export interface DBSearch extends DBFullTextSearch, DBSemanticSearch, DBHybridSearch {}

/**
 * Events operations interface
 *
 * Provides event sourcing capabilities: emit, subscribe, and replay events.
 * Implement this interface for providers that support event-driven architectures.
 *
 * @example
 * ```typescript
 * const events: DBEvents = provider
 *
 * // Subscribe to events
 * const unsubscribe = events.on('user.*', async (event) => {
 *   console.log(`User event: ${event.event}`)
 * })
 *
 * // Emit an event
 * await events.emit({
 *   actor: 'system',
 *   event: 'user.created',
 *   object: 'users/alice',
 *   objectData: { name: 'Alice' }
 * })
 *
 * // List recent events
 * const recent = await events.listEvents({ limit: 100 })
 *
 * // Replay events for rebuilding state
 * await events.replayEvents({
 *   event: 'order.*',
 *   since: new Date('2024-01-01'),
 *   handler: async (event) => {
 *     // Process each event
 *   }
 * })
 * ```
 */
export interface DBEvents {
  /**
   * Subscribe to events matching a pattern
   * @param pattern - Event pattern (e.g., "user.created", "user.*", "*")
   * @param handler - Event handler function
   * @returns Unsubscribe function
   */
  on(pattern: string, handler: (event: DBEvent) => void | Promise<void>): () => void

  /**
   * Emit an event (full options)
   */
  emit(options: CreateEventOptions): Promise<DBEvent>

  /**
   * Emit an event (legacy format)
   * @deprecated Use emit(CreateEventOptions) instead
   */
  emit(type: string, data: unknown): Promise<DBEvent>

  /**
   * List events with optional filters
   * @param options - Filter options
   * @returns Array of events
   */
  listEvents(options?: {
    event?: string
    actor?: string
    object?: string
    since?: Date
    until?: Date
    limit?: number
  }): Promise<DBEvent[]>

  /**
   * Replay events through a handler
   * @param options - Replay options
   */
  replayEvents(options: {
    event?: string
    actor?: string
    since?: Date
    handler: (event: DBEvent) => void | Promise<void>
  }): Promise<void>
}

/**
 * Actions operations interface
 *
 * Provides background job/action tracking with status management.
 * Implement this interface for providers that need to track long-running operations.
 *
 * @example
 * ```typescript
 * const actions: DBActions = provider
 *
 * // Create an action
 * const action = await actions.createAction({
 *   actor: 'user-123',
 *   action: 'export',
 *   object: 'reports/quarterly',
 *   total: 100
 * })
 *
 * // Update progress
 * await actions.updateAction(action.id, { status: 'active' })
 * await actions.updateAction(action.id, { progress: 50 })
 * await actions.updateAction(action.id, {
 *   status: 'completed',
 *   result: { url: 'https://...' }
 * })
 *
 * // List pending actions
 * const pending = await actions.listActions({ status: 'pending' })
 *
 * // Retry a failed action
 * await actions.retryAction(failedAction.id)
 *
 * // Cancel an action
 * await actions.cancelAction(pendingAction.id)
 * ```
 */
export interface DBActions {
  /**
   * Create a new action
   * @param options - Action options
   * @returns The created action
   */
  createAction(options: CreateActionOptions | { type: string; data: unknown; total?: number }): Promise<DBAction>

  /**
   * Get an action by ID
   * @param id - Action ID
   * @returns The action or null if not found
   */
  getAction(id: string): Promise<DBAction | null>

  /**
   * Update an action's status or progress
   * @param id - Action ID
   * @param updates - Fields to update
   * @returns The updated action
   */
  updateAction(
    id: string,
    updates: Partial<Pick<DBAction, 'status' | 'progress' | 'result' | 'error'>>
  ): Promise<DBAction>

  /**
   * List actions with optional filters
   * @param options - Filter options
   * @returns Array of actions
   */
  listActions(options?: {
    status?: ActionStatus
    action?: string
    actor?: string
    object?: string
    since?: Date
    until?: Date
    limit?: number
  }): Promise<DBAction[]>

  /**
   * Retry a failed action (resets status to pending)
   * @param id - Action ID
   * @returns The retried action
   * @throws Error if action is not in failed status
   */
  retryAction(id: string): Promise<DBAction>

  /**
   * Cancel an action
   * @param id - Action ID
   * @throws Error if action is already completed or failed
   */
  cancelAction(id: string): Promise<void>
}

/**
 * Artifacts operations interface
 *
 * Provides content-addressable artifact storage for caching derived content.
 * Use this for caching transformations like:
 * - Markdown rendering
 * - PDF extraction
 * - Image thumbnails
 * - Code syntax highlighting
 *
 * @example
 * ```typescript
 * const artifacts: DBArtifacts = provider
 *
 * // Check for cached artifact
 * const cached = await artifacts.getArtifact('https://example.com/doc', 'markdown')
 *
 * if (!cached || cached.sourceHash !== currentHash) {
 *   // Generate and cache
 *   await artifacts.setArtifact('https://example.com/doc', 'markdown', {
 *     content: renderedMarkdown,
 *     sourceHash: currentHash,
 *     metadata: { wordCount: 1234 }
 *   })
 * }
 *
 * // List all artifacts for a URL
 * const all = await artifacts.listArtifacts('https://example.com/doc')
 *
 * // Delete stale artifacts
 * await artifacts.deleteArtifact('https://example.com/doc', 'markdown')
 * ```
 */
export interface DBArtifacts {
  /**
   * Get an artifact by URL and type
   * @param url - Source URL
   * @param type - Artifact type
   * @returns The artifact or null if not found
   */
  getArtifact(url: string, type: string): Promise<DBArtifact | null>

  /**
   * Set (create or update) an artifact
   * @param url - Source URL
   * @param type - Artifact type
   * @param data - Artifact data including content and sourceHash
   */
  setArtifact(
    url: string,
    type: string,
    data: { content: unknown; sourceHash: string; metadata?: Record<string, unknown> }
  ): Promise<void>

  /**
   * Delete an artifact
   * @param url - Source URL
   * @param type - Artifact type (optional, deletes all types if not specified)
   */
  deleteArtifact(url: string, type?: string): Promise<void>

  /**
   * List all artifacts for a URL
   * @param url - Source URL
   * @returns Array of artifacts
   */
  listArtifacts(url: string): Promise<DBArtifact[]>
}

/**
 * Transaction support interface
 *
 * Provides transactional operations for atomic updates.
 * Implement this interface for providers that support ACID transactions.
 *
 * @example
 * ```typescript
 * const txProvider: DBTransactions = provider
 *
 * const tx = await txProvider.beginTransaction()
 * try {
 *   const user = await tx.create('User', undefined, { name: 'Alice' })
 *   await tx.create('Profile', undefined, { userId: user.$id })
 *   await tx.relate('User', user.$id, 'profile', 'Profile', profile.$id)
 *   await tx.commit()
 * } catch (error) {
 *   await tx.rollback()
 *   throw error
 * }
 * ```
 */
export interface DBTransactions {
  /**
   * Begin a new transaction
   * @returns A transaction object
   */
  beginTransaction(): Promise<Transaction>
}

// =============================================================================
// Composite Interfaces (for backward compatibility)
// =============================================================================

/**
 * Base database provider interface
 *
 * Combines CRUD, relationships, and basic search operations.
 * This is the minimum interface for a database provider.
 *
 * @remarks
 * This interface extends the smaller, focused interfaces (DBCrud, DBRelationships)
 * to maintain backward compatibility while allowing consumers to depend on
 * more specific interfaces when appropriate.
 *
 * For more granular control, use individual interfaces:
 * - `DBCrud` - Just CRUD operations
 * - `DBRelationships` - Just relationship operations
 * - `DBFullTextSearch` - Just FTS search
 */
export interface DBProvider extends DBCrud, DBRelationships, DBFullTextSearch {
  /**
   * Begin a transaction (optional capability)
   */
  beginTransaction?(): Promise<Transaction>
}

/**
 * Extended provider interface with full feature set
 *
 * This composite interface combines all capability interfaces for providers
 * that support the full feature set.
 *
 * @remarks
 * For more granular control and better adherence to the Interface Segregation
 * Principle, consumers can depend on specific capability interfaces instead:
 * - `DBCrud` - Basic CRUD operations
 * - `DBRelationships` - Graph relationship operations
 * - `DBFullTextSearch` - Full-text search
 * - `DBSemanticSearch` - Vector similarity search
 * - `DBHybridSearch` - Combined FTS + semantic search
 * - `DBSearch` - All search capabilities
 * - `DBEvents` - Event sourcing and pub/sub
 * - `DBActions` - Background job tracking
 * - `DBArtifacts` - Content-addressable artifact storage
 * - `DBTransactions` - ACID transaction support
 *
 * @example
 * ```typescript
 * // Use the full interface
 * const provider: DBProviderExtended = createParqueDBProvider(db)
 *
 * // Or use specific capabilities
 * function searchService(search: DBFullTextSearch & DBSemanticSearch) {
 *   // Only depends on search capabilities
 * }
 *
 * function crudService(crud: DBCrud) {
 *   // Only depends on CRUD
 * }
 * ```
 */
export interface DBProviderExtended extends DBProvider, DBSearch, DBEvents, DBActions, DBArtifacts {}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if a provider implements full-text search
 */
export function hasFullTextSearch(provider: unknown): provider is DBFullTextSearch {
  return typeof (provider as DBFullTextSearch)?.search === 'function'
}

/**
 * Check if a provider implements semantic search
 */
export function hasSemanticSearch(provider: unknown): provider is DBSemanticSearch {
  return (
    typeof (provider as DBSemanticSearch)?.semanticSearch === 'function' &&
    typeof (provider as DBSemanticSearch)?.setEmbeddingsConfig === 'function'
  )
}

/**
 * Check if a provider implements hybrid search
 */
export function hasHybridSearch(provider: unknown): provider is DBHybridSearch {
  return typeof (provider as DBHybridSearch)?.hybridSearch === 'function'
}

/**
 * Check if a provider implements event operations
 */
export function hasEvents(provider: unknown): provider is DBEvents {
  return (
    typeof (provider as DBEvents)?.on === 'function' &&
    typeof (provider as DBEvents)?.emit === 'function'
  )
}

/**
 * Check if a provider implements action operations
 */
export function hasActions(provider: unknown): provider is DBActions {
  return (
    typeof (provider as DBActions)?.createAction === 'function' &&
    typeof (provider as DBActions)?.getAction === 'function'
  )
}

/**
 * Check if a provider implements artifact operations
 */
export function hasArtifacts(provider: unknown): provider is DBArtifacts {
  return (
    typeof (provider as DBArtifacts)?.getArtifact === 'function' &&
    typeof (provider as DBArtifacts)?.setArtifact === 'function'
  )
}

/**
 * Check if a provider implements transaction support
 */
export function hasTransactions(provider: unknown): provider is DBTransactions {
  return typeof (provider as DBTransactions)?.beginTransaction === 'function'
}

/**
 * Check if a provider is a full DBProviderExtended
 */
export function isExtendedProvider(provider: unknown): provider is DBProviderExtended {
  return (
    hasFullTextSearch(provider) &&
    hasSemanticSearch(provider) &&
    hasHybridSearch(provider) &&
    hasEvents(provider) &&
    hasActions(provider) &&
    hasArtifacts(provider)
  )
}
