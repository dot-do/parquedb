/**
 * Index Types for ParqueDB
 *
 * Core interfaces for secondary indexes and full-text search.
 */

// =============================================================================
// Index Definition
// =============================================================================

/**
 * Type of index
 */
export type IndexType = 'hash' | 'sst' | 'fts' | 'bloom'

/**
 * Index field definition
 */
export interface IndexField {
  /** Field path (supports dot notation for nested fields) */
  path: string
  /** Sort order for SST indexes */
  order?: 'asc' | 'desc'
  /** Weight for FTS indexes (default: 1.0) */
  weight?: number
}

/**
 * Index definition for creating/managing indexes
 */
export interface IndexDefinition {
  /** Unique name for the index within the namespace */
  name: string
  /** Type of index */
  type: IndexType
  /** Fields to index */
  fields: IndexField[]
  /** Whether index is unique (for hash/sst) */
  unique?: boolean
  /** Sparse index - only index documents where field exists */
  sparse?: boolean
  /** TTL in seconds (for automatic expiration) */
  ttlSeconds?: number
  /** FTS-specific options */
  ftsOptions?: FTSIndexOptions
  /** Index metadata */
  metadata?: Record<string, unknown>
}

/**
 * FTS-specific index options
 */
export interface FTSIndexOptions {
  /** Language for stemming/stopwords */
  language?: string
  /** Minimum word length to index */
  minWordLength?: number
  /** Maximum word length to index */
  maxWordLength?: number
  /** Custom stopwords to exclude */
  stopwords?: string[]
  /** Enable position indexing for phrase queries */
  indexPositions?: boolean
}

// =============================================================================
// Index Entry
// =============================================================================

/**
 * Entry stored in an index
 */
export interface IndexEntry {
  /** The indexed key (encoded) */
  key: Uint8Array
  /** Document ID */
  docId: string
  /** Row group number in the data file */
  rowGroup: number
  /** Row offset within the row group */
  rowOffset: number
  /** Additional metadata (e.g., score for FTS) */
  metadata?: Record<string, unknown>
}

/**
 * Entry stored in a hash index
 */
export interface HashIndexEntry {
  /** The indexed value (encoded as key) */
  key: Uint8Array
  /** Document ID */
  docId: string
  /** Row group number */
  rowGroup: number
  /** Row offset within row group */
  rowOffset: number
}

/**
 * Entry stored in an SST index
 */
export interface SSTIndexEntry {
  /** The indexed value (encoded as sortable key) */
  key: Uint8Array
  /** Document ID */
  docId: string
  /** Row group number */
  rowGroup: number
  /** Row offset within row group */
  rowOffset: number
}

/**
 * Entry stored in an FTS inverted index
 */
export interface FTSIndexEntry {
  /** Token (word) */
  token: string
  /** Document ID */
  docId: string
  /** Field the token came from */
  field: string
  /** Term frequency in this document */
  frequency: number
  /** Positions of the token in the field (if indexPositions enabled) */
  positions?: number[]
}

// =============================================================================
// Index Metadata
// =============================================================================

/**
 * Stored metadata about an index
 */
export interface IndexMetadata {
  /** Index definition */
  definition: IndexDefinition
  /** When the index was created */
  createdAt: Date
  /** When the index was last updated */
  updatedAt: Date
  /** Number of entries in the index */
  entryCount: number
  /** Size of the index in bytes */
  sizeBytes: number
  /** Whether the index is currently being built */
  building: boolean
  /** Build progress (0-1) */
  buildProgress?: number
  /** Version for optimistic concurrency */
  version: number
}

// =============================================================================
// Index Operations
// =============================================================================

/**
 * Result of an index lookup
 */
export interface IndexLookupResult {
  /** Matching document IDs */
  docIds: string[]
  /** Row group hints for efficient reading */
  rowGroups: number[]
  /** Whether the result is exact or approximate (bloom filter) */
  exact: boolean
  /** Number of entries scanned */
  entriesScanned: number
}

/**
 * Range query parameters for SST index
 */
export interface RangeQuery {
  /** Greater than */
  $gt?: unknown
  /** Greater than or equal */
  $gte?: unknown
  /** Less than */
  $lt?: unknown
  /** Less than or equal */
  $lte?: unknown
}

/**
 * FTS search options
 */
export interface FTSSearchOptions {
  /** Maximum results to return */
  limit?: number
  /** Minimum score threshold */
  minScore?: number
  /** Language for query analysis */
  language?: string
  /** Highlight matching terms */
  highlight?: boolean
}

/**
 * FTS search result
 */
export interface FTSSearchResult {
  /** Document ID */
  docId: string
  /** BM25 score */
  score: number
  /** Matching tokens */
  matchedTokens: string[]
  /** Highlighted snippets (if highlight enabled) */
  highlights?: Record<string, string[]>
}

// =============================================================================
// Index Interface
// =============================================================================

/**
 * Base interface for all index implementations
 */
export interface Index {
  /** Index definition */
  readonly definition: IndexDefinition
  /** Namespace this index belongs to */
  readonly namespace: string
  /** Whether the index is loaded and ready */
  readonly ready: boolean

  /**
   * Load the index from storage
   */
  load(): Promise<void>

  /**
   * Save the index to storage
   */
  save(): Promise<void>

  /**
   * Get index statistics
   */
  getStats(): IndexStats
}

/**
 * Index statistics
 */
export interface IndexStats {
  /** Number of entries */
  entryCount: number
  /** Size in bytes */
  sizeBytes: number
  /** Unique keys (for hash index) */
  uniqueKeys?: number
  /** Levels (for SST index) */
  levels?: number
  /** Vocabulary size (for FTS index) */
  vocabularySize?: number
  /** Average document length (for FTS index) */
  avgDocLength?: number
}

// =============================================================================
// Index Events
// =============================================================================

/**
 * Events emitted during index operations
 */
export type IndexEvent =
  | { type: 'build_started'; definition: IndexDefinition }
  | { type: 'build_progress'; definition: IndexDefinition; progress: number }
  | { type: 'build_completed'; definition: IndexDefinition; stats: IndexStats }
  | { type: 'build_failed'; definition: IndexDefinition; error: Error }
  | { type: 'entry_added'; definition: IndexDefinition; docId: string }
  | { type: 'entry_removed'; definition: IndexDefinition; docId: string }

/**
 * Index event listener
 */
export type IndexEventListener = (event: IndexEvent) => void
