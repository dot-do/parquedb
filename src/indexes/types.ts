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
 * NOTE: 'hash' and 'sst' removed - native parquet predicate pushdown on $index_* columns
 * is now faster than secondary indexes for equality and range queries
 */
export type IndexType = 'fts' | 'bloom' | 'vector' | 'geo'

/**
 * Index field definition
 */
export interface IndexField {
  /** Field path (supports dot notation for nested fields) */
  path: string
  /** Sort order (kept for backward compatibility) */
  order?: 'asc' | 'desc' | undefined
  /** Weight for FTS indexes (default: 1.0) */
  weight?: number | undefined
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
  /** Whether index is unique (for hash index) */
  unique?: boolean | undefined
  /** Sparse index - only index documents where field exists */
  sparse?: boolean | undefined
  /** TTL in seconds (for automatic expiration) */
  ttlSeconds?: number | undefined
  /** FTS-specific options */
  ftsOptions?: FTSIndexOptions | undefined
  /** Vector-specific options */
  vectorOptions?: VectorIndexOptions | undefined
  /** Geo-specific options */
  geoOptions?: GeoIndexOptions | undefined
  /** Index metadata */
  metadata?: Record<string, unknown> | undefined
}

/**
 * FTS-specific index options
 */
export interface FTSIndexOptions {
  /** Language for stemming/stopwords */
  language?: string | undefined
  /** Minimum word length to index */
  minWordLength?: number | undefined
  /** Maximum word length to index */
  maxWordLength?: number | undefined
  /** Custom stopwords to exclude */
  stopwords?: string[] | undefined
  /** Enable position indexing for phrase queries */
  indexPositions?: boolean | undefined
}

/**
 * Vector distance metric
 */
export type VectorMetric = 'cosine' | 'euclidean' | 'dot'

/**
 * Vector-specific index options (HNSW parameters)
 */
export interface VectorIndexOptions {
  /** Number of dimensions in the vectors */
  dimensions: number
  /** Distance metric to use */
  metric?: VectorMetric | undefined
  /** HNSW M parameter - number of connections per layer (default: 16) */
  m?: number | undefined
  /** HNSW efConstruction parameter - size of dynamic candidate list during construction (default: 200) */
  efConstruction?: number | undefined
}

/**
 * Geo-specific index options
 */
export interface GeoIndexOptions {
  /** Geohash precision for bucketing (1-12, default: 6 = ~1.2km cells) */
  bucketPrecision?: number | undefined
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
  metadata?: Record<string, unknown> | undefined
}

/**
 * Entry stored in a hash index
 * @deprecated Hash indexes have been removed - use parquet predicate pushdown instead
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
 * @deprecated SST indexes have been removed - kept for backward compatibility only
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
  positions?: number[] | undefined
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
  buildProgress?: number | undefined
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
 * Range query parameters
 * NOTE: Range queries now use native parquet predicate pushdown on $index_* columns
 * @deprecated SST indexes have been removed - use parquet predicate pushdown instead
 */
export interface RangeQuery {
  /** Greater than */
  $gt?: unknown | undefined
  /** Greater than or equal */
  $gte?: unknown | undefined
  /** Less than */
  $lt?: unknown | undefined
  /** Less than or equal */
  $lte?: unknown | undefined
}

/**
 * Fuzzy matching options for FTS
 */
export interface FTSFuzzyOptions {
  /** Enable fuzzy matching (default: false) */
  enabled?: boolean | undefined
  /** Maximum edit distance allowed (default: 2) */
  maxDistance?: number | undefined
  /** Minimum term length to apply fuzzy matching (default: 4) */
  minTermLength?: number | undefined
  /** Number of characters that must match exactly at start (default: 1) */
  prefixLength?: number | undefined
}

/**
 * Highlight options for FTS
 */
export interface FTSHighlightOptions {
  /** HTML tag to insert before matches (default: '<mark>') */
  preTag?: string | undefined
  /** HTML tag to insert after matches (default: '</mark>') */
  postTag?: string | undefined
  /** Maximum number of snippets per field (default: 3) */
  maxSnippets?: number | undefined
  /** Maximum length of each snippet in characters (default: 150) */
  maxSnippetLength?: number | undefined
}

/**
 * FTS search options
 */
export interface FTSSearchOptions {
  /** Maximum results to return */
  limit?: number | undefined
  /** Minimum score threshold */
  minScore?: number | undefined
  /** Language for query analysis */
  language?: string | undefined
  /** Highlight matching terms (true for defaults, or options object) */
  highlight?: boolean | FTSHighlightOptions | undefined
  /** Fuzzy matching options for typo tolerance */
  fuzzy?: FTSFuzzyOptions | boolean | undefined
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
  highlights?: Record<string, string[]> | undefined
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
  uniqueKeys?: number | undefined
  /** Levels (kept for backward compatibility) */
  levels?: number | undefined
  /** Vocabulary size (for FTS index) */
  vocabularySize?: number | undefined
  /** Average document length (for FTS index) */
  avgDocLength?: number | undefined
  /** Vector dimensions (for vector index) */
  dimensions?: number | undefined
  /** Max layer in HNSW graph (for vector index) */
  maxLayer?: number | undefined
}

// =============================================================================
// Vector Index Types
// =============================================================================

/**
 * Entry stored in a vector index
 */
export interface VectorIndexEntry {
  /** The vector */
  vector: number[]
  /** Document ID */
  docId: string
  /** Row group number */
  rowGroup: number
  /** Row offset within row group */
  rowOffset: number
}

/**
 * Vector search options
 */
export interface VectorSearchOptions {
  /** Minimum similarity score (0-1 for cosine, depends on metric) */
  minScore?: number | undefined
  /** HNSW efSearch parameter - size of dynamic candidate list during search */
  efSearch?: number | undefined
}

/**
 * Result of a vector similarity search
 */
export interface VectorSearchResult {
  /** Matching document IDs (ordered by similarity) */
  docIds: string[]
  /** Row group hints for efficient reading */
  rowGroups: number[]
  /** Similarity scores for each result */
  scores?: number[] | undefined
  /** Whether the result is exact or approximate */
  exact: boolean
  /** Number of entries scanned */
  entriesScanned: number
}

/**
 * Hybrid search strategy
 *
 * - 'pre-filter': Apply metadata filters first, then vector search on filtered set
 * - 'post-filter': Perform vector search first, then filter results
 * - 'auto': Automatically choose based on estimated filter selectivity
 */
export type HybridSearchStrategy = 'pre-filter' | 'post-filter' | 'auto'

/**
 * Options for hybrid search (vector + metadata filtering)
 */
export interface HybridSearchOptions extends VectorSearchOptions {
  /** Search strategy for combining vector search with filtering */
  strategy?: HybridSearchStrategy | undefined
  /** Candidate IDs to restrict vector search to (for pre-filter strategy) */
  candidateIds?: Set<string> | undefined
  /**
   * Over-fetch multiplier for post-filter strategy.
   * When using post-filter, we fetch topK * multiplier candidates
   * to ensure we have enough results after filtering.
   * Default: 3
   */
  overFetchMultiplier?: number | undefined
}

/**
 * Result of a hybrid search combining vector similarity and metadata filtering
 */
export interface HybridSearchResult extends VectorSearchResult {
  /** Strategy that was used */
  strategyUsed: HybridSearchStrategy
  /** Number of candidates after pre-filtering (if pre-filter strategy) */
  preFilterCount?: number | undefined
  /** Number of results before post-filtering (if post-filter strategy) */
  postFilterCount?: number | undefined
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

// =============================================================================
// Geo Index Types
// =============================================================================

/**
 * Entry stored in a geo index
 */
export interface GeoIndexEntry {
  /** Document ID */
  docId: string
  /** Latitude */
  lat: number
  /** Longitude */
  lng: number
  /** Row group number */
  rowGroup: number
  /** Row offset within row group */
  rowOffset: number
  /** Precomputed geohash */
  geohash: string
}

/**
 * Geo search options
 */
export interface GeoSearchOptions {
  /** Maximum distance in meters */
  maxDistance?: number | undefined
  /** Minimum distance in meters */
  minDistance?: number | undefined
  /** Maximum results to return */
  limit?: number | undefined
}

/**
 * Result of a geo proximity search
 */
export interface GeoSearchResult {
  /** Matching document IDs (ordered by distance) */
  docIds: string[]
  /** Row group hints for efficient reading */
  rowGroups: number[]
  /** Distances in meters for each result */
  distances: number[]
  /** Number of entries scanned */
  entriesScanned: number
}
