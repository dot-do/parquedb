/**
 * ParqueDB Secondary Indexing System - TypeScript Type Definitions
 *
 * These types define the index structures, storage formats, and query interfaces
 * for the comprehensive secondary indexing system.
 */

import { Variant, Namespace, EntityId, Timestamp, Node, Edge } from './graph-schemas';

// ============================================================================
// Core Index Entry Types
// ============================================================================

/**
 * Index entry stored in index files.
 * This is the fundamental unit stored in all index types.
 */
export interface IndexEntry {
  /** Serialized index key (comparable byte array) */
  key: Uint8Array;

  /** Primary key of source row */
  primaryKey: Uint8Array;

  /** Source row group ID for fetching full row */
  rowGroupId: number;

  /** Offset within row group */
  rowOffset: number;

  /** Array index (for multi-entry array indexes) */
  arrayIndex?: number;

  /** Included column values (for covering indexes) */
  included?: Variant[];
}

/**
 * Hash bucket entry with full hash for verification
 */
export interface HashBucketEntry extends IndexEntry {
  /** Full 64-bit hash for collision detection */
  keyHash: bigint;
}

// ============================================================================
// Index Definition Types
// ============================================================================

/**
 * Base properties shared by all index definitions
 */
interface BaseIndexDefinition {
  /** Unique index name */
  name: string;

  /** Target table name */
  table: string;

  /** Sparse index (only non-null values) */
  sparse?: boolean;

  /** Partial index predicate */
  partial?: PartialIndexPredicate;
}

/**
 * B-tree style index for range queries
 */
export interface BTreeIndexDefinition extends BaseIndexDefinition {
  type: 'btree';

  /** Column or Variant path to index */
  column: string;

  /** Sort direction */
  direction?: 'asc' | 'desc';

  /** NULL handling */
  nulls?: 'first' | 'last';
}

/**
 * Hash index for equality lookups
 */
export interface HashIndexDefinition extends BaseIndexDefinition {
  type: 'hash';

  /** Column or Variant path to index */
  column: string;

  /** Number of hash buckets */
  bucketCount?: number;

  /** Hash function to use */
  hashFunction?: 'xxhash64' | 'murmur3' | 'fnv1a';
}

/**
 * Column specification for composite indexes
 */
export interface CompositeColumn {
  /** Column name or Variant path */
  path: string;

  /** Sort direction */
  direction: 'asc' | 'desc';

  /** NULL handling */
  nulls: 'first' | 'last';
}

/**
 * Composite/compound index on multiple columns
 */
export interface CompositeIndexDefinition extends BaseIndexDefinition {
  type: 'composite';

  /** Ordered list of columns */
  columns: CompositeColumn[];

  /** Index type for storage */
  indexType: 'btree' | 'hash';
}

/**
 * Unique index with constraint enforcement
 */
export interface UniqueIndexDefinition extends BaseIndexDefinition {
  type: 'unique';

  /** Columns that must be unique together */
  columns: string[];

  /** Allow deferred constraint checking */
  deferrable?: boolean;

  /** Whether NULLs are considered distinct */
  nullsDistinct?: boolean;
}

/**
 * Covering index with included columns
 */
export interface CoveringIndexDefinition extends BaseIndexDefinition {
  type: 'covering';

  /** Key columns for lookups */
  keyColumns: CompositeColumn[];

  /** Additional columns stored in index */
  includedColumns: string[];

  /** Index type for storage */
  indexType: 'btree' | 'hash';
}

/**
 * Expression index on computed values
 */
export interface ExpressionIndexDefinition extends BaseIndexDefinition {
  type: 'expression';

  /** Expression to compute index key */
  expression: IndexExpression;

  /** Index type for storage */
  indexType: 'btree' | 'hash';
}

/**
 * Nested path index for Variant fields
 */
export interface NestedPathIndexDefinition extends BaseIndexDefinition {
  type: 'nested_path';

  /**
   * Path syntax:
   * - data.user.email      - Object path
   * - data.tags[0]         - Array index
   * - data.tags[*]         - All array elements
   * - data.users[*].email  - Nested array element field
   */
  path: string;

  /** Index type for storage */
  indexType: 'btree' | 'hash';
}

/**
 * Array element index for array field contents
 */
export interface ArrayIndexDefinition extends BaseIndexDefinition {
  type: 'array';

  /** Path to array field */
  arrayPath: string;

  /** Path within array element (for object arrays) */
  elementPath?: string;

  /** Index type for storage */
  indexType: 'btree' | 'hash';

  /** Create entry per element vs whole array */
  multiEntry: boolean;
}

/**
 * Union of all index definition types
 */
export type IndexDefinition =
  | BTreeIndexDefinition
  | HashIndexDefinition
  | CompositeIndexDefinition
  | UniqueIndexDefinition
  | CoveringIndexDefinition
  | ExpressionIndexDefinition
  | NestedPathIndexDefinition
  | ArrayIndexDefinition;

// ============================================================================
// Partial Index Predicates
// ============================================================================

/**
 * Comparison predicate for partial indexes
 */
export interface ComparisonPredicate {
  column: string;
  op: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte';
  value: Variant;
}

/**
 * IN predicate for partial indexes
 */
export interface InPredicate {
  column: string;
  op: 'in';
  values: Variant[];
}

/**
 * NULL check predicate
 */
export interface NullPredicate {
  column: string;
  op: 'is_null' | 'is_not_null';
}

/**
 * Logical AND of predicates
 */
export interface AndPredicate {
  and: PartialIndexPredicate[];
}

/**
 * Logical OR of predicates
 */
export interface OrPredicate {
  or: PartialIndexPredicate[];
}

/**
 * Logical NOT of predicate
 */
export interface NotPredicate {
  not: PartialIndexPredicate;
}

/**
 * Union of all partial index predicate types
 */
export type PartialIndexPredicate =
  | ComparisonPredicate
  | InPredicate
  | NullPredicate
  | AndPredicate
  | OrPredicate
  | NotPredicate;

// ============================================================================
// Expression Index Types
// ============================================================================

/**
 * Column reference expression
 */
export interface ColumnExpression {
  type: 'column';
  path: string;
}

/**
 * Literal value expression
 */
export interface LiteralExpression {
  type: 'literal';
  value: Variant;
}

/**
 * String case conversion
 */
export interface CaseExpression {
  type: 'lower' | 'upper';
  arg: IndexExpression;
}

/**
 * String concatenation
 */
export interface ConcatExpression {
  type: 'concat';
  args: IndexExpression[];
}

/**
 * Substring extraction
 */
export interface SubstringExpression {
  type: 'substring';
  arg: IndexExpression;
  start: number;
  length?: number;
}

/**
 * COALESCE (first non-null)
 */
export interface CoalesceExpression {
  type: 'coalesce';
  args: IndexExpression[];
}

/**
 * Date/time field extraction
 */
export interface ExtractExpression {
  type: 'extract';
  arg: IndexExpression;
  field: 'year' | 'month' | 'day' | 'hour' | 'minute' | 'second';
}

/**
 * JSON path extraction
 */
export interface JsonExtractExpression {
  type: 'json_extract';
  arg: IndexExpression;
  path: string;
}

/**
 * Hash computation
 */
export interface HashExpression {
  type: 'hash';
  arg: IndexExpression;
  algorithm: 'md5' | 'sha256' | 'xxhash64';
}

/**
 * Length computation
 */
export interface LengthExpression {
  type: 'length';
  arg: IndexExpression;
}

/**
 * Arithmetic operation
 */
export interface ArithmeticExpression {
  type: 'arithmetic';
  op: '+' | '-' | '*' | '/' | '%';
  left: IndexExpression;
  right: IndexExpression;
}

/**
 * Union of all expression types
 */
export type IndexExpression =
  | ColumnExpression
  | LiteralExpression
  | CaseExpression
  | ConcatExpression
  | SubstringExpression
  | CoalesceExpression
  | ExtractExpression
  | JsonExtractExpression
  | HashExpression
  | LengthExpression
  | ArithmeticExpression;

// ============================================================================
// Index Manifest and Metadata
// ============================================================================

/**
 * Index column definition in manifest
 */
export interface IndexColumnDef {
  path: string;
  direction?: 'asc' | 'desc';
  nulls?: 'first' | 'last';
  variantType: 'string' | 'int64' | 'float64' | 'timestamp' | 'boolean' | 'binary' | 'variant';
}

/**
 * Index file metadata
 */
export interface IndexFile {
  /** Unique file identifier */
  id: string;

  /** Storage path */
  path: string;

  /** File size in bytes */
  size: number;

  /** Number of entries in file */
  entryCount: number;

  /** Minimum key (base64 encoded) */
  minKey: string;

  /** Maximum key (base64 encoded) */
  maxKey: string;

  /** LSM level (for B-tree indexes) */
  level?: number;

  /** Bucket number (for hash indexes) */
  bucket?: number;

  /** Path to bloom filter file */
  bloomFilterPath?: string;
}

/**
 * LSM-tree level metadata
 */
export interface LSMLevel {
  level: number;
  files: string[];
  totalSize: number;
  fileCount: number;
}

/**
 * Hash index configuration
 */
export interface HashIndexConfig {
  bucketCount: number;
  hashFunction: 'xxhash64' | 'murmur3' | 'fnv1a';
}

/**
 * Index manifest (stored in manifest.json)
 */
export interface IndexManifest {
  /** Manifest schema version */
  version: number;

  /** Unique index identifier */
  indexId: string;

  /** Index name */
  name: string;

  /** Target table */
  table: string;

  /** Index type */
  type: 'btree' | 'hash' | 'composite' | 'unique' | 'covering' | 'expression' | 'nested_path' | 'array';

  /** Key column definitions */
  keyColumns: IndexColumnDef[];

  /** Included columns (for covering indexes) */
  includedColumns?: string[];

  /** Index configuration */
  config: {
    sparse?: boolean;
    partial?: PartialIndexPredicate;
    expression?: IndexExpression;
    unique?: boolean;
    nullsDistinct?: boolean;
    multiEntry?: boolean;
  };

  /** Index data files */
  files: IndexFile[];

  /** Total index size in bytes */
  totalSize: number;

  /** Total number of index entries */
  totalEntries: number;

  /** Index statistics */
  stats: IndexStatistics;

  /** LSM-tree levels (for B-tree indexes) */
  levels?: LSMLevel[];

  /** Hash configuration (for hash indexes) */
  hashConfig?: HashIndexConfig;
}

/**
 * Index statistics for query planning
 */
export interface IndexStatistics {
  /** When index was created */
  createdAt: string;

  /** When statistics were last updated */
  lastUpdated: string;

  /** Time to build index (ms) */
  buildDuration: number;

  /** Number of distinct keys */
  distinctKeys: number;

  /** Average entries per key */
  avgEntriesPerKey: number;

  /** Maximum entries for any key */
  maxEntriesPerKey: number;

  /** Average key size in bytes */
  avgKeySizeBytes: number;

  /** Key value distribution histogram */
  keyDistribution?: HistogramBucket[];

  /** Percentage of NULL values */
  nullPercentage: number;
}

/**
 * Histogram bucket for key distribution
 */
export interface HistogramBucket {
  /** Lower bound (inclusive) */
  lowerBound: string; // Base64 encoded

  /** Upper bound (exclusive) */
  upperBound: string; // Base64 encoded

  /** Number of keys in bucket */
  frequency: number;

  /** Distinct key count in bucket */
  distinctCount: number;
}

// ============================================================================
// Zone Maps / Skip Indexes
// ============================================================================

/**
 * Zone map entry for a single column in a row group
 */
export interface ZoneMapEntry {
  /** Row group identifier */
  rowGroupId: number;

  /** Column path */
  column: string;

  /** Minimum value in row group */
  min: Variant;

  /** Maximum value in row group */
  max: Variant;

  /** Count of NULL values */
  nullCount: number;

  /** Number of rows in row group */
  rowCount: number;

  /** Estimated distinct values */
  distinctCount?: number;

  /** Whether bloom filter exists */
  hasBloomFilter: boolean;
}

/**
 * Zone map index for a table
 */
export interface ZoneMapIndex {
  /** Table name */
  table: string;

  /** Columns with zone maps */
  columns: string[];

  /** Row group -> column -> stats */
  entries: Map<number, Map<string, ZoneMapEntry>>;

  /** Last refresh timestamp */
  lastRefreshed: string;
}

/**
 * Bloom filter metadata
 */
export interface BloomFilterMeta {
  /** Row group ID */
  rowGroupId: number;

  /** Column path */
  column: string;

  /** Path to bloom filter file */
  path: string;

  /** Number of bits in filter */
  numBits: number;

  /** Number of hash functions */
  numHashFunctions: number;

  /** Expected false positive rate */
  falsePositiveRate: number;
}

// ============================================================================
// Query Types
// ============================================================================

/**
 * Query predicate for index lookups
 */
export interface QueryPredicate {
  /** Column or path */
  column: string;

  /** Comparison operator */
  op: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'in' | 'is_null' | 'is_not_null' | 'like' | 'prefix' | 'contains' | 'contains_all' | 'contains_any';

  /** Comparison value */
  value?: Variant;

  /** Values for IN predicate */
  values?: Variant[];
}

/**
 * Query definition
 */
export interface Query {
  /** Target table */
  table: string;

  /** Filter predicates */
  predicates: QueryPredicate[];

  /** Columns to return */
  projection: string[];

  /** Sort specification */
  orderBy?: Array<{
    column: string;
    direction: 'asc' | 'desc';
  }>;

  /** Maximum rows to return */
  limit?: number;

  /** Rows to skip */
  offset?: number;
}

/**
 * Predicate with index pushdown info
 */
export interface PushedPredicate extends QueryPredicate {
  /** Whether pushed to index */
  pushedToIndex: boolean;

  /** Whether pushed to Parquet filter */
  pushedToParquet: boolean;
}

/**
 * Query execution plan
 */
export interface QueryPlan {
  /** Plan type */
  type: 'table_scan' | 'index_scan' | 'index_only_scan' | 'bitmap_scan';

  /** Index used (if any) */
  index?: string;

  /** Scan direction */
  scanDirection?: 'forward' | 'backward';

  /** Predicates with pushdown info */
  predicates: PushedPredicate[];

  /** Projected columns */
  projection: string[];

  /** Estimated cost (arbitrary units) */
  estimatedCost: number;

  /** Estimated result rows */
  estimatedRows: number;

  /** Child plans (for bitmap scans) */
  children?: QueryPlan[];
}

/**
 * Bitmap scan node
 */
export type BitmapNode =
  | { type: 'index_bitmap'; index: string; predicate: QueryPredicate }
  | { type: 'bitmap_and'; children: BitmapNode[] }
  | { type: 'bitmap_or'; children: BitmapNode[] };

/**
 * Bitmap scan plan
 */
export interface BitmapScanPlan extends QueryPlan {
  type: 'bitmap_scan';
  bitmapNodes: BitmapNode[];
}

// ============================================================================
// Index Operations
// ============================================================================

/**
 * Row change for index maintenance
 */
export interface RowChange {
  /** Old row state (null for insert) */
  before: Record<string, Variant> | null;

  /** New row state (null for delete) */
  after: Record<string, Variant> | null;

  /** Primary key */
  primaryKey: Uint8Array;

  /** Row group ID */
  rowGroupId: number;

  /** Row offset */
  rowOffset: number;
}

/**
 * Batch of index updates
 */
export interface IndexUpdateBatch {
  /** New entries to insert */
  inserts: IndexEntry[];

  /** Entries to update (old -> new) */
  updates: Array<{
    old: IndexEntry;
    new: IndexEntry;
  }>;

  /** Entries to delete */
  deletes: IndexEntry[];
}

/**
 * Index operation result
 */
export interface IndexOperationResult {
  /** Whether operation succeeded */
  success: boolean;

  /** Number of entries affected */
  entriesAffected: number;

  /** Operation duration (ms) */
  duration: number;

  /** Error message (if failed) */
  error?: string;
}

/**
 * Index rebuild options
 */
export interface RebuildOptions {
  /** Number of parallel workers */
  parallel?: number;

  /** Create checkpoint before rebuild */
  checkpoint?: boolean;

  /** Allow queries during rebuild */
  online?: boolean;

  /** Drop existing index first */
  dropExisting?: boolean;
}

/**
 * Index rebuild result
 */
export interface RebuildResult {
  /** Entries processed */
  entriesProcessed: number;

  /** Build duration (ms) */
  duration: number;

  /** New index size (bytes) */
  newSize: number;
}

/**
 * Unique constraint violation
 */
export interface UniqueViolation {
  /** Index name */
  index: string;

  /** Conflicting key values */
  existingKey: Variant[];

  /** Primary key of conflicting row */
  conflictingPrimaryKey: Uint8Array;
}

// ============================================================================
// Compaction Types
// ============================================================================

/**
 * Compaction configuration
 */
export interface CompactionConfig {
  /** L0 file count trigger */
  level0FileLimit: number;

  /** Size ratio between levels */
  levelMultiplier: number;

  /** Maximum size per level (bytes) */
  maxLevelSize: number[];

  /** Files to merge at once */
  mergeWidth: number;
}

/**
 * Compaction task
 */
export interface CompactionTask {
  /** Index name */
  indexName: string;

  /** Target level */
  level: number;

  /** Files to compact */
  inputFiles: string[];

  /** Task priority */
  priority: 'low' | 'normal' | 'high';
}

// ============================================================================
// Cost Model
// ============================================================================

/**
 * Cost model parameters
 */
export interface CostModel {
  /** Sequential page read cost */
  seqPageRead: number;

  /** Random page read cost */
  randPageRead: number;

  /** Per-tuple processing cost */
  cpuTupleProcess: number;

  /** Index lookup overhead */
  cpuIndexLookup: number;

  /** Hash computation cost */
  cpuHashCompute: number;

  /** Page size (bytes) */
  pageSize: number;

  /** Buffer pool size (bytes) */
  bufferPoolSize: number;

  /** Effective cache size (bytes) */
  effectiveCacheSize: number;
}

/**
 * Scan cost estimate
 */
export interface ScanCostEstimate {
  /** Index lookup cost */
  indexLookupCost: number;

  /** Page read cost */
  pageReadCost: number;

  /** CPU processing cost */
  cpuCost: number;

  /** Total estimated cost */
  totalCost: number;

  /** Estimated result rows */
  estimatedRows: number;
}

/**
 * Table statistics for cost estimation
 */
export interface TableStatistics {
  /** Total row count */
  rowCount: number;

  /** Total table size (bytes) */
  totalSize: number;

  /** Average rows per row group */
  rowsPerRowGroup: number;

  /** Average rows per page */
  rowsPerPage: number;

  /** Number of row groups */
  rowGroupCount: number;
}

// ============================================================================
// Storage Paths
// ============================================================================

/**
 * Index storage path conventions
 */
export const INDEX_PATHS = {
  /** Root indexes directory */
  root: (ns: string) => `/indexes/${ns}`,

  /** Index directory */
  index: (ns: string, name: string) => `/indexes/${ns}/${name}`,

  /** Index manifest */
  manifest: (ns: string, name: string) => `/indexes/${ns}/${name}/manifest.json`,

  /** B-tree SST files */
  sstLevel: (ns: string, name: string, level: number) =>
    `/indexes/${ns}/${name}/level-${level}`,

  /** Hash bucket files */
  hashBucket: (ns: string, name: string, bucket: number) =>
    `/indexes/${ns}/${name}/bucket-${bucket.toString().padStart(4, '0')}.parquet`,

  /** Bloom filters */
  bloomFilter: (ns: string, name: string, rowGroupId: number) =>
    `/indexes/${ns}/${name}/bloom/bloom-${rowGroupId}.bin`,

  /** Zone maps */
  zoneMaps: (ns: string, table: string) =>
    `/zone_maps/${ns}/${table}/stats.parquet`,
} as const;

// ============================================================================
// Default Configurations
// ============================================================================

/**
 * Default cost model
 */
export const DEFAULT_COST_MODEL: CostModel = {
  seqPageRead: 1.0,
  randPageRead: 4.0,
  cpuTupleProcess: 0.01,
  cpuIndexLookup: 0.005,
  cpuHashCompute: 0.001,
  pageSize: 8192,
  bufferPoolSize: 256 * 1024 * 1024,
  effectiveCacheSize: 1024 * 1024 * 1024,
};

/**
 * Default compaction configuration
 */
export const DEFAULT_COMPACTION: CompactionConfig = {
  level0FileLimit: 4,
  levelMultiplier: 10,
  maxLevelSize: [
    10 * 1024 * 1024,        // L0: 10 MB
    100 * 1024 * 1024,       // L1: 100 MB
    1024 * 1024 * 1024,      // L2: 1 GB
    10 * 1024 * 1024 * 1024, // L3: 10 GB
  ],
  mergeWidth: 4,
};

/**
 * Default hash index configuration
 */
export const DEFAULT_HASH_CONFIG: HashIndexConfig = {
  bucketCount: 256,
  hashFunction: 'xxhash64',
};
