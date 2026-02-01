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

  constructor(
    private storage: StorageBackend,
    private basePath: string = ''
  ) {}

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

    // Extract field conditions for secondary indexes
    const candidates: Array<{
      index: IndexDefinition
      type: 'hash' | 'sst'
      field: string
      condition: unknown
      selectivity: number
    }> = []

    for (const [field, condition] of Object.entries(filter)) {
      if (field.startsWith('$')) continue

      // Find indexes that cover this field
      const indexesForField = this.findIndexesForField(ns, field)

      for (const index of indexesForField) {
        const selectivity = this.estimateSelectivity(condition)

        if (index.type === 'hash' && this.isEqualityCondition(condition)) {
          candidates.push({
            index,
            type: 'hash',
            field,
            condition,
            selectivity,
          })
        } else if (index.type === 'sst') {
          candidates.push({
            index,
            type: 'sst',
            field,
            condition,
            selectivity,
          })
        }
      }
    }

    // Select the most selective index
    if (candidates.length === 0) {
      return null
    }

    candidates.sort((a, b) => a.selectivity - b.selectivity)
    const best = candidates[0]!  // candidates has at least 1 element from earlier checks

    return {
      index: best.index,
      type: best.type,
      field: best.field,
      condition: best.condition,
    }
  }

  /**
   * Execute a lookup using a hash index
   *
   * @param ns - Namespace
   * @param indexName - Index name
   * @param value - Value to look up
   * @returns Lookup result
   */
  async hashLookup(ns: string, indexName: string, value: unknown): Promise<IndexLookupResult> {
    await this.load()

    // This will be implemented by the HashIndex class
    // For now, return empty result
    return {
      docIds: [],
      rowGroups: [],
      exact: true,
      entriesScanned: 0,
    }
  }

  /**
   * Execute a range query using an SST index
   *
   * @param ns - Namespace
   * @param indexName - Index name
   * @param range - Range query
   * @returns Lookup result
   */
  async rangeQuery(ns: string, indexName: string, range: RangeQuery): Promise<IndexLookupResult> {
    await this.load()

    // This will be implemented by the SSTIndex class
    // For now, return empty result
    return {
      docIds: [],
      rowGroups: [],
      exact: true,
      entriesScanned: 0,
    }
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

    // This will be implemented by the FTS module
    // For now, return empty result
    return []
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

    for (const [indexName, definition] of nsIndexes) {
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

    for (const [indexName, definition] of nsIndexes) {
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

    for (const [indexName, definition] of nsIndexes) {
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
    for (const listener of this.listeners) {
      try {
        listener(event)
      } catch (err) {
        // TODO(parquedb-y9aw): Listener errors are silently swallowed.
        // Consider logging or providing an onError callback for monitoring.
        console.warn('[IndexManager] Event listener error:', err)
      }
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
      case 'hash':
      case 'sst':
        return `${base}indexes/secondary/${ns}.${definition.name}.idx.parquet`
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
    // This will be implemented when we add the actual index implementations
    // For now, just update progress
    const metadata = this.metadata.get(ns)!.get(definition.name)!
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
    // Will be implemented by specific index classes
  }

  private async removeFromIndex(
    ns: string,
    definition: IndexDefinition,
    docId: string,
    doc: Record<string, unknown>
  ): Promise<void> {
    // Will be implemented by specific index classes
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

  private findIndexesForField(ns: string, field: string): IndexDefinition[] {
    const nsIndexes = this.indexes.get(ns)
    if (!nsIndexes) return []

    const result: IndexDefinition[] = []

    for (const definition of nsIndexes.values()) {
      if (definition.fields.some(f => f.path === field)) {
        result.push(definition)
      }
    }

    return result
  }

  private isEqualityCondition(condition: unknown): boolean {
    if (condition === null || typeof condition !== 'object') {
      return true // Direct value comparison
    }

    const obj = condition as Record<string, unknown>
    if ('$eq' in obj) return true
    if ('$in' in obj) return true

    // Check if it has range operators
    if ('$gt' in obj || '$gte' in obj || '$lt' in obj || '$lte' in obj) {
      return false
    }

    return true
  }

  private estimateSelectivity(condition: unknown): number {
    // Lower is better (more selective)
    if (condition === null || typeof condition !== 'object') {
      return 0.1 // Direct equality is very selective
    }

    const obj = condition as Record<string, unknown>

    if ('$eq' in obj) return 0.1
    if ('$in' in obj) {
      const values = obj.$in as unknown[]
      return Math.min(0.5, 0.1 * values.length)
    }
    if ('$gt' in obj || '$gte' in obj || '$lt' in obj || '$lte' in obj) {
      return 0.3 // Range queries are less selective
    }

    return 0.5 // Default moderate selectivity
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
 */
export interface SelectedIndex {
  /** Selected index definition */
  index: IndexDefinition
  /** Index type */
  type: 'hash' | 'sst' | 'fts' | 'vector'
  /** Field being queried (for secondary indexes) */
  field?: string
  /** Query condition */
  condition: unknown
}
