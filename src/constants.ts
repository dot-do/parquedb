/**
 * ParqueDB Constants
 *
 * Centralized constants used throughout the codebase.
 * Eliminates magic numbers and provides single source of truth.
 */

// =============================================================================
// Limits
// =============================================================================

/**
 * Default maximum inbound references to return
 * Used in ParqueDB relationship traversal
 */
export const DEFAULT_MAX_INBOUND = 100

/**
 * Default page size for paginated queries
 */
export const DEFAULT_PAGE_SIZE = 100

/**
 * Maximum batch size for bulk operations
 * Used in migrations and bulk inserts
 */
export const MAX_BATCH_SIZE = 1000

// =============================================================================
// Concurrency
// =============================================================================

/**
 * Default concurrency limit for parallel operations
 * Used in query executor for row group reads
 */
export const DEFAULT_CONCURRENCY = 4

// =============================================================================
// Storage
// =============================================================================

/**
 * Minimum part size for multipart uploads (5MB)
 * R2/S3 minimum requirement
 */
export const MIN_PART_SIZE = 5 * 1024 * 1024

/**
 * Default part size for multipart uploads (8MB)
 */
export const DEFAULT_PART_SIZE = 8 * 1024 * 1024

/**
 * Maximum number of parts for multipart uploads
 * R2/S3 limit is 10,000
 */
export const MAX_PARTS = 10000

/**
 * Default TTL for multipart uploads in milliseconds (30 minutes)
 * Uploads older than this will be cleaned up automatically
 */
export const DEFAULT_MULTIPART_UPLOAD_TTL = 30 * 60 * 1000

// =============================================================================
// Cache
// =============================================================================

/**
 * Default cache TTL in seconds (1 hour)
 * Used for edge caching
 */
export const DEFAULT_CACHE_TTL = 3600

/**
 * Maximum cache size in bytes (2MB)
 * Used for whole-file caching in QueryExecutor
 */
export const MAX_CACHE_SIZE = 2 * 1024 * 1024

// =============================================================================
// Parquet
// =============================================================================

/**
 * Default row group size
 * Used in Parquet writer
 */
export const DEFAULT_ROW_GROUP_SIZE = 10000

/**
 * Default parquet page size in bytes (1MB)
 * Used in Parquet writer
 */
export const DEFAULT_PARQUET_PAGE_SIZE = 1024 * 1024

/**
 * Enable column indexes (ColumnIndex) by default
 * Column indexes store min/max values per page for predicate pushdown
 * This enables page-level filtering in hyparquet's parquetQuery()
 */
export const DEFAULT_ENABLE_COLUMN_INDEX = true

/**
 * Enable offset indexes (OffsetIndex) by default
 * Offset indexes store page locations for efficient page skipping
 * Required when ColumnIndex is present per Parquet spec
 */
export const DEFAULT_ENABLE_OFFSET_INDEX = true

// =============================================================================
// FNV-1a Hash Constants
// =============================================================================

/**
 * FNV-1a hash offset basis (32-bit)
 * Used for key hashing in secondary indexes
 */
export const FNV_OFFSET_BASIS = 2166136261

/**
 * FNV-1a hash prime multiplier (32-bit)
 * Used for key hashing in secondary indexes
 */
export const FNV_PRIME = 16777619

// =============================================================================
// Bloom Filter Constants
// =============================================================================

/**
 * Default bloom filter size in bytes (128KB)
 * Optimized for ~1% false positive rate
 */
export const DEFAULT_BLOOM_SIZE = 131072

/**
 * Default number of hash functions for bloom filters
 * Optimized for ~1% false positive rate
 */
export const DEFAULT_NUM_HASH_FUNCTIONS = 3

/**
 * Bloom filter size per row group (4KB)
 */
export const ROW_GROUP_BLOOM_SIZE = 4096

// =============================================================================
// Index Building Constants
// =============================================================================

/**
 * Batch size for index building progress updates
 * Used in hash.ts and sst.ts for progress callbacks
 */
export const INDEX_PROGRESS_BATCH = 10000

// =============================================================================
// HNSW Vector Index Constants
// =============================================================================

/**
 * Default number of connections per layer in HNSW
 */
export const DEFAULT_HNSW_M = 16

/**
 * Default size of dynamic candidate list during construction
 */
export const DEFAULT_HNSW_EF_CONSTRUCTION = 200

/**
 * Default size of dynamic candidate list during search
 */
export const DEFAULT_HNSW_EF_SEARCH = 50

// =============================================================================
// Event Log Constants
// =============================================================================

/**
 * Default maximum events in event log
 */
export const DEFAULT_MAX_EVENTS = 10000

/**
 * Default max age for events in milliseconds (7 days)
 */
export const DEFAULT_MAX_EVENT_AGE = 7 * 24 * 60 * 60 * 1000

/**
 * Default event buffer size before flush
 */
export const DEFAULT_EVENT_BUFFER_SIZE = 1000

/**
 * Default event buffer bytes before flush (1MB)
 */
export const DEFAULT_EVENT_BUFFER_BYTES = 1024 * 1024

/**
 * Default flush interval in milliseconds
 */
export const DEFAULT_FLUSH_INTERVAL_MS = 5000

/**
 * R2 threshold for event storage (512KB)
 */
export const DEFAULT_R2_THRESHOLD_BYTES = 512 * 1024

// =============================================================================
// Retry Constants
// =============================================================================

/**
 * Default maximum retry attempts
 */
export const DEFAULT_MAX_RETRIES = 3

/**
 * Default base delay for retries in milliseconds
 */
export const DEFAULT_RETRY_BASE_DELAY = 100

/**
 * Default maximum delay for retries in milliseconds
 */
export const DEFAULT_RETRY_MAX_DELAY = 10000

/**
 * Default retry multiplier for exponential backoff
 */
export const DEFAULT_RETRY_MULTIPLIER = 2

/**
 * Default jitter factor for retry delays
 */
export const DEFAULT_RETRY_JITTER_FACTOR = 0.5
