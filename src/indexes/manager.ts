/**
 * Index Manager for ParqueDB
 *
 * Coordinates all index operations including creation, updates,
 * lookups, and maintenance.
 */

import type { StorageBackend } from '../types/storage'
import { IndexCatalogError } from './errors'
import { logger } from '../utils/logger'
import { safeJsonParse, isRecord } from '../utils/json-validation'
import type { Filter } from '../types/filter'
import type {
  IndexDefinition,
  IndexMetadata,
  IndexLookupResult,
  IndexStats,
  IndexEventListener,
  IndexEvent,
  RangeQuery,
  FTSSearchOptions,
  FTSSearchResult,
  VectorSearchOptions,
  VectorSearchResult,
} from './types'
import { VectorIndex } from './vector'
import { FTSIndex } from './fts'

// =============================================================================
// Index Manager Options
// =============================================================================

/**
 * Error handler callback for IndexManager
 */
export type IndexManagerErrorHandler = (error: Error, event: IndexEvent, listener: IndexEventListener) => void

/**
 * Configuration options for IndexManager
 */
export interface IndexManagerOptions {
  /** Base path for index storage */
  basePath?: string
  /**
   * Error handler for listener errors.
   * If provided, called when a listener throws an error.
   * If not provided, errors are logged via console.warn.
   */
  onError?: IndexManagerErrorHandler
  /**
   * If true, collect all listener errors and throw an AggregateError after all listeners have been called.
   * Default: false
   */
  throwOnListenerError?: boolean
}

// =============================================================================
// Index Manager
// =============================================================================

/**
 * Manages all indexes for a ParqueDB instance
 */
export class IndexManager {
  private indexes: Map<string, Map<string, IndexDefinition>> = new Map()
  private metadata: Map<string, Map<string, IndexMetadata>> = new Map()
  private listeners: Set<IndexEventListener> = new Set()
  private loaded: boolean = false
  /** Cache for loaded VectorIndex instances */
  private vectorIndexes: Map<string, VectorIndex> = new Map()
  /** Cache for loaded FTSIndex instances */
  private ftsIndexes: Map<string, FTSIndex> = new Map()
  private basePath: string
  private onError?: IndexManagerErrorHandler
  private throwOnListenerError: boolean

  constructor(
    private storage: StorageBackend,
    options: IndexManagerOptions | string = ''
  ) {
    // Support legacy signature (basePath as string) and new options object
    if (typeof options === 'string') {
      this.basePath = options
      this.throwOnListenerError = false
    } else {
      this.basePath = options.basePath ?? ''
      this.onError = options.onError
      this.throwOnListenerError = options.throwOnListenerError ?? false
    }
  }

  // ===========================================================================
  // Initialization
  // ===========================================================================

  /**
   * Load all index metadata from storage
   *
   * Error handling:
   * - Catalog doesn't exist: Expected, continue with empty catalog
   * - Catalog exists but is corrupted: Log warning, continue with empty catalog
   * - Storage read errors: Re-throw to caller
   */
  async load(): Promise<void> {
    if (this.loaded) return

    const catalogPath = this.getCatalogPath()
    const exists = await this.storage.exists(catalogPath)

    if (!exists) {
      // No catalog exists yet - this is expected for new databases
      this.loaded = true
      return
    }

    try {
      const data = await this.storage.read(catalogPath)
      const result = safeJsonParse(new TextDecoder().decode(data))
      if (!result.ok || !isRecord(result.value)) {
        logger.warn(
          `Index catalog corrupted at ${catalogPath}, starting fresh`,
          new IndexCatalogError(catalogPath, new Error('Invalid JSON or not an object'))
        )
        this.loaded = true
        return
      }
      const catalog = result.value as unknown as IndexCatalog
      this.loadCatalog(catalog)
      this.loaded = true
    } catch (error: unknown) {
      // Catalog exists but failed to read - likely corrupted
      // Log warning but continue with empty catalog (self-healing behavior)
      const cause = error instanceof Error ? error : new Error(String(error))
      logger.warn(
        `Index catalog corrupted at ${catalogPath}, starting fresh`,
        new IndexCatalogError(catalogPath, cause)
      )
      this.loaded = true
    }
  }

  /**
   * Save index catalog to storage
   */
  async save(): Promise<void> {
    const catalog = this.buildCatalog()
    const data = new TextEncoder().encode(JSON.stringify(catalog, null, 2))
    await this.storage.write(this.getCatalogPath(), data)
  }

  // ===========================================================================
  // Index Management
  // ===========================================================================

  /**
   * Create a new index
   *
   * @param ns - Namespace
   * @param definition - Index definition
   * @returns Index metadata
   */
  async createIndex(ns: string, definition: IndexDefinition): Promise<IndexMetadata> {
    await this.load()

    // Validate definition
    this.validateDefinition(definition)

    // Check if index already exists
    if (this.hasIndex(ns, definition.name)) {
      throw new Error(`Index ${definition.name} already exists in namespace ${ns}`)
    }

    // Create metadata
    const metadata: IndexMetadata = {
      definition,
      createdAt: new Date(),
      updatedAt: new Date(),
      entryCount: 0,
      sizeBytes: 0,
      building: true,
      buildProgress: 0,
      version: 1,
    }

    // Store in memory
    if (!this.indexes.has(ns)) {
      this.indexes.set(ns, new Map())
      this.metadata.set(ns, new Map())
    }
    this.indexes.get(ns)!.set(definition.name, definition)
    this.metadata.get(ns)!.set(definition.name, metadata)

    // Emit event
    this.emit({ type: 'build_started', definition })

    // Build the index
    try {
      await this.buildIndex(ns, definition)

      // Update metadata
      metadata.building = false
      metadata.buildProgress = 1
      metadata.updatedAt = new Date()

      // Save catalog
      await this.save()

      this.emit({ type: 'build_completed', definition, stats: await this.getIndexStats(ns, definition.name) })

      return metadata
    } catch (error: unknown) {
      // Remove the failed index
      this.indexes.get(ns)?.delete(definition.name)
      this.metadata.get(ns)?.delete(definition.name)

      this.emit({ type: 'build_failed', definition, error: error instanceof Error ? error : new Error(String(error)) })
      throw error
    }
  }

  /**
   * Drop an index
   *
   * @param ns - Namespace
   * @param indexName - Index name
   */
  async dropIndex(ns: string, indexName: string): Promise<void> {
    await this.load()

    if (!this.hasIndex(ns, indexName)) {
      throw new Error(`Index ${indexName} does not exist in namespace ${ns}`)
    }

    const definition = this.indexes.get(ns)!.get(indexName)!

    // Delete index files
    const indexPath = this.getIndexPath(ns, definition)
    const exists = await this.storage.exists(indexPath)
    if (exists) {
      await this.storage.delete(indexPath)
    }

    // Remove from memory
    this.indexes.get(ns)?.delete(indexName)
    this.metadata.get(ns)?.delete(indexName)

    // Save catalog
    await this.save()
  }

  /**
   * List all indexes for a namespace
   *
   * @param ns - Namespace
   * @returns Array of index metadata
   */
  async listIndexes(ns: string): Promise<IndexMetadata[]> {
    await this.load()

    const nsMetadata = this.metadata.get(ns)
    if (!nsMetadata) return []

    return Array.from(nsMetadata.values())
  }

  /**
   * Get index metadata
   *
   * @param ns - Namespace
   * @param indexName - Index name
   * @returns Index metadata or null
   */
  async getIndexMetadata(ns: string, indexName: string): Promise<IndexMetadata | null> {
    await this.load()

    return this.metadata.get(ns)?.get(indexName) ?? null
  }

  /**
   * Rebuild an index
   *
   * @param ns - Namespace
   * @param indexName - Index name
   */
  async rebuildIndex(ns: string, indexName: string): Promise<void> {
    const metadata = await this.getIndexMetadata(ns, indexName)
    if (!metadata) {
      throw new Error(`Index ${indexName} does not exist in namespace ${ns}`)
    }

    // Mark as building
    metadata.building = true
    metadata.buildProgress = 0

    this.emit({ type: 'build_started', definition: metadata.definition })

    try {
      await this.buildIndex(ns, metadata.definition)

      metadata.building = false
      metadata.buildProgress = 1
      metadata.updatedAt = new Date()
      metadata.version++

      await this.save()

      this.emit({
        type: 'build_completed',
        definition: metadata.definition,
        stats: await this.getIndexStats(ns, indexName),
      })
    } catch (error: unknown) {
      metadata.building = false
      this.emit({ type: 'build_failed', definition: metadata.definition, error: error instanceof Error ? error : new Error(String(error)) })
      throw error
    }
  }

  // ===========================================================================
  // Index Lookup
  // ===========================================================================

  /**
   * Select the best index for a filter
   *
   * @param ns - Namespace
   * @param filter - Query filter
   * @returns Selected index or null
   */
  async selectIndex(ns: string, filter: Filter): Promise<SelectedIndex | null> {
    await this.load()

    const nsIndexes = this.indexes.get(ns)
    if (!nsIndexes || nsIndexes.size === 0) {
      return null
    }

    // Check for $text operator -> use FTS index
    if (filter.$text) {
      const ftsIndex = this.findFTSIndex(ns)
      if (ftsIndex) {
        return {
          index: ftsIndex,
          type: 'fts',
          condition: filter.$text,
        }
      }
    }

    // Check for $vector operator -> use vector index
    if (filter.$vector) {
      const vectorIndex = this.findVectorIndex(ns, filter.$vector.$field)
      if (vectorIndex) {
        return {
          index: vectorIndex,
          type: 'vector',
          field: filter.$vector.$field,
          condition: filter.$vector,
        }
      }
    }

    // NOTE: Hash and SST indexes removed - equality and range queries now use
    // native parquet predicate pushdown on $index_* columns

    return null
  }

  /**
   * Execute a lookup using a hash index
   *
   * @deprecated Hash indexes have been removed - use native parquet predicate pushdown instead
   * @param _ns - Namespace (unused)
   * @param _indexName - Index name (unused)
   * @param _value - Value to look up (unused)
   * @returns Never - this method is deprecated
   */
  async hashLookup(_ns: string, _indexName: string, _value: unknown): Promise<IndexLookupResult> {
    throw new Error(
      `Hash indexes have been removed. Equality queries now use native parquet predicate pushdown ` +
      `on $index_* columns, which is faster than secondary indexes. ` +
      `Use parquet filters with $eq/$in operators directly.`
    )
  }

  /**
   * Execute a range query using parquet predicate pushdown
   *
   * NOTE: SST indexes have been removed - range queries now use native parquet
   * predicate pushdown on $index_* columns, which is faster than secondary indexes.
   *
   * @param _ns - Namespace (unused)
   * @param _indexName - Index name (unused)
   * @param _range - Range query (unused)
   * @returns Never - this method is deprecated
   * @deprecated Use native parquet predicate pushdown instead
   */
  async rangeQuery(_ns: string, _indexName: string, _range: RangeQuery): Promise<IndexLookupResult> {
    throw new Error(
      `SST indexes have been removed. Range queries now use native parquet predicate pushdown ` +
      `on $index_* columns, which is faster than secondary indexes. ` +
      `Use parquet filters with $gt/$gte/$lt/$lte operators directly.`
    )
  }

  /**
   * Execute a full-text search
   *
   * @param ns - Namespace
   * @param query - Search query
   * @param options - Search options
   * @returns Search results
   */
  async ftsSearch(
    ns: string,
    query: string,
    options?: FTSSearchOptions
  ): Promise<FTSSearchResult[]> {
    await this.load()

    // Find FTS index for this namespace
    const ftsIndexDef = this.findFTSIndex(ns)
    if (!ftsIndexDef) {
      return []
    }

    // Get or create the FTS index instance
    const ftsIndex = await this.getFTSIndex(ns, ftsIndexDef.name)
    if (!ftsIndex) {
      return []
    }

    // Execute the search
    return ftsIndex.search(query, options)
  }

  /**
   * Get or create an FTSIndex instance
   */
  private async getFTSIndex(ns: string, indexName: string): Promise<FTSIndex | null> {
    const cacheKey = `${ns}.${indexName}`

    // Return cached instance if available
    if (this.ftsIndexes.has(cacheKey)) {
      return this.ftsIndexes.get(cacheKey)!
    }

    // Get the index definition
    const definition = this.indexes.get(ns)?.get(indexName)
    if (!definition || definition.type !== 'fts') {
      return null
    }

    // Create and load the index
    const ftsIndex = new FTSIndex(
      this.storage,
      ns,
      definition,
      this.basePath
    )
    await ftsIndex.load()

    // Cache it
    this.ftsIndexes.set(cacheKey, ftsIndex)
    return ftsIndex
  }

  /**
   * Execute a vector similarity search
   *
   * @param ns - Namespace
   * @param indexName - Index name
   * @param queryVector - Query vector
   * @param k - Number of results to return
   * @param options - Search options (minScore, efSearch)
   * @returns Vector search results
   */
  async vectorSearch(
    ns: string,
    indexName: string,
    queryVector: number[],
    k: number,
    options?: VectorSearchOptions
  ): Promise<VectorSearchResult> {
    await this.load()

    // Get the vector index
    const vectorIndex = await this.getVectorIndex(ns, indexName)
    if (!vectorIndex) {
      return {
        docIds: [],
        rowGroups: [],
        scores: [],
        exact: false,
        entriesScanned: 0,
      }
    }

    // Execute the search
    return vectorIndex.search(queryVector, k, options)
  }

  /**
   * Get or create a VectorIndex instance
   */
  private async getVectorIndex(ns: string, indexName: string): Promise<VectorIndex | null> {
    const cacheKey = `${ns}.${indexName}`

    // Return cached instance if available
    if (this.vectorIndexes.has(cacheKey)) {
      return this.vectorIndexes.get(cacheKey)!
    }

    // Get the index definition
    const definition = this.indexes.get(ns)?.get(indexName)
    if (!definition || definition.type !== 'vector') {
      return null
    }

    // Create and load the index
    const vectorIndex = new VectorIndex(
      this.storage,
      ns,
      definition,
      this.basePath
    )
    await vectorIndex.load()

    // Cache it
    this.vectorIndexes.set(cacheKey, vectorIndex)
    return vectorIndex
  }

  // ===========================================================================
  // Index Updates
  // ===========================================================================

  /**
   * Update indexes when a document is added
   *
   * @param ns - Namespace
   * @param docId - Document ID
   * @param doc - Document data
   * @param rowGroup - Row group number
   * @param rowOffset - Row offset within row group
   */
  async onDocumentAdded(
    ns: string,
    docId: string,
    doc: Record<string, unknown>,
    rowGroup: number,
    rowOffset: number
  ): Promise<void> {
    const nsIndexes = this.indexes.get(ns)
    if (!nsIndexes) return

    for (const [_indexName, definition] of nsIndexes) {
      await this.addToIndex(ns, definition, docId, doc, rowGroup, rowOffset)
      this.emit({ type: 'entry_added', definition, docId })
    }
  }

  /**
   * Update indexes when a document is removed
   *
   * @param ns - Namespace
   * @param docId - Document ID
   * @param doc - Document data (for finding index entries)
   */
  async onDocumentRemoved(
    ns: string,
    docId: string,
    doc: Record<string, unknown>
  ): Promise<void> {
    const nsIndexes = this.indexes.get(ns)
    if (!nsIndexes) return

    for (const [_indexName, definition] of nsIndexes) {
      await this.removeFromIndex(ns, definition, docId, doc)
      this.emit({ type: 'entry_removed', definition, docId })
    }
  }

  /**
   * Update indexes when a document is modified
   *
   * @param ns - Namespace
   * @param docId - Document ID
   * @param oldDoc - Previous document data
   * @param newDoc - New document data
   * @param rowGroup - New row group number
   * @param rowOffset - New row offset
   */
  async onDocumentUpdated(
    ns: string,
    docId: string,
    oldDoc: Record<string, unknown>,
    newDoc: Record<string, unknown>,
    rowGroup: number,
    rowOffset: number
  ): Promise<void> {
    const nsIndexes = this.indexes.get(ns)
    if (!nsIndexes) return

    for (const [_indexName, definition] of nsIndexes) {
      // Check if any indexed fields changed
      const fieldsChanged = definition.fields.some(field => {
        const oldValue = this.getFieldValue(oldDoc, field.path)
        const newValue = this.getFieldValue(newDoc, field.path)
        return !this.valuesEqual(oldValue, newValue)
      })

      if (fieldsChanged) {
        await this.removeFromIndex(ns, definition, docId, oldDoc)
        await this.addToIndex(ns, definition, docId, newDoc, rowGroup, rowOffset)
      }
    }
  }

  // ===========================================================================
  // Statistics
  // ===========================================================================

  /**
   * Get statistics for an index
   *
   * @param ns - Namespace
   * @param indexName - Index name
   * @returns Index statistics
   */
  async getIndexStats(ns: string, indexName: string): Promise<IndexStats> {
    const metadata = await this.getIndexMetadata(ns, indexName)
    if (!metadata) {
      throw new Error(`Index ${indexName} does not exist in namespace ${ns}`)
    }

    return {
      entryCount: metadata.entryCount,
      sizeBytes: metadata.sizeBytes,
    }
  }

  // ===========================================================================
  // Event Handling
  // ===========================================================================

  /**
   * Add an event listener
   */
  addEventListener(listener: IndexEventListener): void {
    this.listeners.add(listener)
  }

  /**
   * Remove an event listener
   */
  removeEventListener(listener: IndexEventListener): void {
    this.listeners.delete(listener)
  }

  private emit(event: IndexEvent): void {
    const errors: Array<{ error: Error; listener: IndexEventListener }> = []

    for (const listener of this.listeners) {
      try {
        listener(event)
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err))

        if (this.onError) {
          // Call the user-provided error handler
          try {
            this.onError(error, event, listener)
          } catch {
            // Intentionally ignored: errors from the user-provided error handler must not propagate
          }
        } else {
          // Default behavior: log a warning
          logger.warn('[IndexManager] Event listener error:', error)
        }

        if (this.throwOnListenerError) {
          errors.push({ error, listener })
        }
      }
    }

    // If configured to throw, aggregate all errors and throw after all listeners have been called
    if (this.throwOnListenerError && errors.length > 0) {
      const messages = errors.map((e, i) => `[${i + 1}] ${e.error.message}`).join('; ')
      throw new AggregateError(
        errors.map(e => e.error),
        `${errors.length} listener(s) threw errors: ${messages}`
      )
    }
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  private getCatalogPath(): string {
    return this.basePath
      ? `${this.basePath}/_meta/indexes.json`
      : '_meta/indexes.json'
  }

  private getIndexPath(ns: string, definition: IndexDefinition): string {
    const base = this.basePath ? `${this.basePath}/` : ''

    switch (definition.type) {
      case 'fts':
        return `${base}indexes/fts/${ns}/`
      case 'bloom':
        return `${base}indexes/bloom/${ns}.${definition.name}.bloom`
      case 'vector':
        return `${base}indexes/vector/${ns}.${definition.name}.hnsw`
      default:
        throw new Error(`Unknown index type: ${definition.type}`)
    }
  }

  private hasIndex(ns: string, name: string): boolean {
    return this.indexes.get(ns)?.has(name) ?? false
  }

  private validateDefinition(definition: IndexDefinition): void {
    if (!definition.name) {
      throw new Error('Index name is required')
    }

    if (!definition.type) {
      throw new Error('Index type is required')
    }

    if (!definition.fields || definition.fields.length === 0) {
      throw new Error('At least one field is required')
    }

    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(definition.name)) {
      throw new Error('Invalid index name: must start with letter or underscore, contain only alphanumeric and underscore')
    }
  }

  private loadCatalog(catalog: IndexCatalog): void {
    for (const [ns, indexes] of Object.entries(catalog.indexes)) {
      this.indexes.set(ns, new Map())
      this.metadata.set(ns, new Map())

      for (const entry of indexes) {
        this.indexes.get(ns)!.set(entry.definition.name, entry.definition)
        this.metadata.get(ns)!.set(entry.definition.name, {
          ...entry.metadata,
          createdAt: new Date(entry.metadata.createdAt),
          updatedAt: new Date(entry.metadata.updatedAt),
        })
      }
    }
  }

  private buildCatalog(): IndexCatalog {
    const indexes: Record<string, IndexCatalogEntry[]> = {}

    for (const [ns, nsIndexes] of this.indexes) {
      indexes[ns] = []
      for (const [name, definition] of nsIndexes) {
        const metadata = this.metadata.get(ns)!.get(name)!
        indexes[ns].push({
          definition,
          metadata: {
            ...metadata,
            createdAt: metadata.createdAt.toISOString(),
            updatedAt: metadata.updatedAt.toISOString(),
          },
        })
      }
    }

    return {
      version: 1,
      indexes,
    }
  }

  private async buildIndex(ns: string, definition: IndexDefinition): Promise<void> {
    const metadata = this.metadata.get(ns)!.get(definition.name)!
    const cacheKey = `${ns}.${definition.name}`

    switch (definition.type) {
      case 'fts': {
        // Create and cache the FTS index
        const ftsIndex = new FTSIndex(
          this.storage,
          ns,
          definition,
          this.basePath
        )
        this.ftsIndexes.set(cacheKey, ftsIndex)
        // Note: Actual data indexing happens via onDocumentAdded or when data is provided
        await ftsIndex.save()
        break
      }
      case 'vector': {
        // Create and cache the vector index
        const vectorIndex = new VectorIndex(
          this.storage,
          ns,
          definition,
          this.basePath
        )
        this.vectorIndexes.set(cacheKey, vectorIndex)
        // Note: Actual data indexing happens via onDocumentAdded or when data is provided
        await vectorIndex.save()
        break
      }
      case 'bloom': {
        // Bloom filters are typically built during data ingestion
        // The IndexBloomFilter class is used directly during writes
        break
      }
    }

    metadata.buildProgress = 1
    metadata.building = false
    metadata.updatedAt = new Date()
  }

  private async addToIndex(
    ns: string,
    definition: IndexDefinition,
    docId: string,
    doc: Record<string, unknown>,
    rowGroup: number,
    rowOffset: number
  ): Promise<void> {
    const _cacheKey = `${ns}.${definition.name}`
    void _cacheKey // Reserved for future caching optimization

    switch (definition.type) {
      case 'fts': {
        const ftsIndex = await this.getFTSIndex(ns, definition.name)
        if (ftsIndex) {
          ftsIndex.addDocument(docId, doc)
        }
        break
      }
      case 'vector': {
        const vectorIndex = await this.getVectorIndex(ns, definition.name)
        if (vectorIndex) {
          // Extract vector from document based on field path
          const firstField = definition.fields[0]
          if (firstField) {
            const vector = this.getFieldValue(doc, firstField.path)
            if (Array.isArray(vector) && vector.every(v => typeof v === 'number')) {
              vectorIndex.insert(vector as number[], docId, rowGroup, rowOffset)
            }
          }
        }
        break
      }
      case 'bloom': {
        // Bloom filters are typically managed at a different layer
        // during Parquet file writes rather than per-document updates
        break
      }
    }
  }

  private async removeFromIndex(
    ns: string,
    definition: IndexDefinition,
    docId: string,
    doc: Record<string, unknown>
  ): Promise<void> {
    switch (definition.type) {
      case 'fts': {
        const ftsIndex = await this.getFTSIndex(ns, definition.name)
        if (ftsIndex) {
          ftsIndex.removeDocument(docId)
        }
        break
      }
      case 'vector': {
        const vectorIndex = await this.getVectorIndex(ns, definition.name)
        if (vectorIndex) {
          vectorIndex.remove(docId)
        }
        break
      }
      case 'bloom': {
        // Bloom filters don't support removal - they require rebuild
        break
      }
    }
  }

  private findFTSIndex(ns: string): IndexDefinition | null {
    const nsIndexes = this.indexes.get(ns)
    if (!nsIndexes) return null

    for (const definition of nsIndexes.values()) {
      if (definition.type === 'fts') {
        return definition
      }
    }

    return null
  }

  private findVectorIndex(ns: string, field: string): IndexDefinition | null {
    const nsIndexes = this.indexes.get(ns)
    if (!nsIndexes) return null

    for (const definition of nsIndexes.values()) {
      if (definition.type === 'vector' && definition.fields.some(f => f.path === field)) {
        return definition
      }
    }

    return null
  }

  private getFieldValue(obj: Record<string, unknown>, path: string): unknown {
    const parts = path.split('.')
    let current: unknown = obj

    for (const part of parts) {
      if (current === null || current === undefined) return undefined
      if (typeof current !== 'object') return undefined
      current = (current as Record<string, unknown>)[part]
    }

    return current
  }

  private valuesEqual(a: unknown, b: unknown): boolean {
    if (a === b) return true
    if (a === null || a === undefined) return b === null || b === undefined

    if (typeof a !== typeof b) return false

    if (typeof a === 'object') {
      return JSON.stringify(a) === JSON.stringify(b)
    }

    return false
  }
}

// =============================================================================
// Types
// =============================================================================

/**
 * Index catalog stored in _meta/indexes.json
 */
interface IndexCatalog {
  version: number
  indexes: Record<string, IndexCatalogEntry[]>
}

interface IndexCatalogEntry {
  definition: IndexDefinition
  /** Serialized metadata with ISO date strings instead of Date objects */
  metadata: Omit<IndexMetadata, 'createdAt' | 'updatedAt'> & {
    createdAt: string
    updatedAt: string
  }
}

/**
 * Result of index selection
 *
 * NOTE: Hash and SST indexes have been removed - equality and range queries now use
 * native parquet predicate pushdown on $index_* columns
 */
export interface SelectedIndex {
  /** Selected index definition */
  index: IndexDefinition
  /** Index type */
  type: 'fts' | 'vector'
  /** Field being queried (for secondary indexes) */
  field?: string
  /** Query condition */
  condition: unknown
}
