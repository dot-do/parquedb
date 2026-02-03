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

/**
 * Default maximum number of concurrent multipart uploads to track
 * Prevents unbounded memory growth from incomplete uploads
 */
export const DEFAULT_MAX_ACTIVE_UPLOADS = 100

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
// Transaction Constants
// =============================================================================

/**
 * Default transaction timeout in milliseconds (30 seconds)
 */
export const DEFAULT_TIMEOUT_MS = 30000

/**
 * Default transaction retry delay in milliseconds
 */
export const DEFAULT_TRANSACTION_RETRY_DELAY = 100

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

// =============================================================================
// Pagination Constants
// =============================================================================

/**
 * Default limit for paginate() method
 */
export const DEFAULT_PAGINATE_LIMIT = 20

/**
 * Default limit for FTS search results
 */
export const DEFAULT_FTS_SEARCH_LIMIT = 100

// =============================================================================
// Full-Text Search Constants
// =============================================================================

/**
 * Default minimum word length for FTS tokenization
 */
export const DEFAULT_FTS_MIN_WORD_LENGTH = 2

/**
 * Default maximum word length for FTS tokenization
 */
export const DEFAULT_FTS_MAX_WORD_LENGTH = 50

// =============================================================================
// Schema Inference Constants
// =============================================================================

/**
 * Default sample size for schema inference
 */
export const DEFAULT_SCHEMA_SAMPLE_SIZE = 100

/**
 * Default maximum depth for nested schema inference
 */
export const DEFAULT_SCHEMA_MAX_DEPTH = 5

// =============================================================================
// Embedding Constants
// =============================================================================

/**
 * Default cache size for query embeddings
 */
export const DEFAULT_EMBEDDING_CACHE_SIZE = 1000

/**
 * Default TTL for embedding cache in milliseconds (5 minutes)
 */
export const DEFAULT_EMBEDDING_CACHE_TTL = 5 * 60 * 1000

/**
 * Default batch size for background embedding processing
 */
export const DEFAULT_EMBEDDING_BATCH_SIZE = 10

/**
 * Default processing delay for background embeddings in milliseconds
 */
export const DEFAULT_EMBEDDING_PROCESS_DELAY = 1000

/**
 * Default priority for embedding queue items
 */
export const DEFAULT_EMBEDDING_PRIORITY = 100

// =============================================================================
// R2 Storage Constants
// =============================================================================

/**
 * Maximum retries for R2 append operations
 */
export const R2_APPEND_MAX_RETRIES = 10

/**
 * Base delay for R2 append retry backoff in milliseconds
 */
export const R2_APPEND_BASE_DELAY_MS = 10

// =============================================================================
// Batch Loading Constants
// =============================================================================

/**
 * Default batching window in milliseconds
 */
export const DEFAULT_BATCH_WINDOW_MS = 10

/**
 * Default maximum batch size for relationship loading
 */
export const DEFAULT_BATCH_MAX_SIZE = 100

// =============================================================================
// Remote Backend Constants
// =============================================================================

/**
 * Default cache TTL for remote backend stat results in milliseconds (1 minute)
 */
export const DEFAULT_REMOTE_CACHE_TTL = 60000

/**
 * Default timeout for remote operations in milliseconds (30 seconds)
 */
export const DEFAULT_REMOTE_TIMEOUT = 30000

// =============================================================================
// Vector Search Constants
// =============================================================================

/**
 * Default topK for vector similarity search
 */
export const DEFAULT_VECTOR_TOP_K = 10

// =============================================================================
// Index Cache Constants
// =============================================================================

/**
 * Default maximum cache size for indexes in bytes (50MB)
 */
export const DEFAULT_INDEX_CACHE_MAX_BYTES = 50 * 1024 * 1024

// =============================================================================
// Event Archival Constants
// =============================================================================

/**
 * Default days after which segments are archived
 */
export const DEFAULT_ARCHIVE_AFTER_DAYS = 7

/**
 * Default days to retain archived segments before purging
 */
export const DEFAULT_RETENTION_DAYS = 365

// =============================================================================
// Streaming Refresh Constants
// =============================================================================

/**
 * Default batch size for streaming refresh
 */
export const DEFAULT_STREAMING_BATCH_SIZE = 100

/**
 * Default batch timeout for streaming refresh in milliseconds
 */
export const DEFAULT_STREAMING_BATCH_TIMEOUT_MS = 500

/**
 * Default maximum buffer size for streaming refresh
 */
export const DEFAULT_STREAMING_MAX_BUFFER_SIZE = 1000

// =============================================================================
// Subscription Constants
// =============================================================================

/**
 * Maximum subscriptions per WebSocket connection
 */
export const MAX_SUBSCRIPTIONS_PER_CONNECTION = 10

/**
 * Default connection timeout in milliseconds (30 seconds)
 */
export const DEFAULT_CONNECTION_TIMEOUT_MS = 30000

/**
 * Default heartbeat interval in milliseconds (15 seconds)
 */
export const DEFAULT_HEARTBEAT_INTERVAL_MS = 15000

/**
 * Maximum pending events per subscription
 */
export const MAX_PENDING_EVENTS = 1000

/**
 * Default polling interval for event sources in milliseconds
 */
export const DEFAULT_POLLING_INTERVAL_MS = 1000

/**
 * Default max buffer size for event writer sources
 */
export const DEFAULT_EVENT_SOURCE_BUFFER_SIZE = 1000

// =============================================================================
// Query Constants
// =============================================================================

/**
 * Default query result limit
 */
export const DEFAULT_QUERY_LIMIT = 100

// =============================================================================
// Content Retention Constants
// =============================================================================

/**
 * Default content retention period in milliseconds (30 days)
 * Used for generated content and other content tracking MVs
 */
export const DEFAULT_CONTENT_RETENTION_MS = 30 * 24 * 60 * 60 * 1000

/**
 * Default AI usage max age in milliseconds (30 days)
 * Used by AIUsageMV to filter logs to process
 */
export const AI_USAGE_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000

/**
 * Default eval scores max age in milliseconds (30 days)
 * Used by EvalScoresMV to filter scores to process
 */
export const EVAL_SCORES_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000

/**
 * Default AI requests max age in milliseconds (30 days)
 * Used by AIRequestsMV to filter requests to process
 */
export const AI_REQUESTS_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000

// =============================================================================
// Hash Constants
// =============================================================================

/**
 * DJB2 hash algorithm initial value
 * A popular non-cryptographic hash function
 */
export const DJB2_INITIAL = 5381

// =============================================================================
// Byte Size Constants
// =============================================================================

/**
 * Bytes per kilobyte (1024)
 * Used for human-readable byte formatting
 */
export const BYTES_PER_KB = 1024

// =============================================================================
// Time Constants
// =============================================================================

/**
 * Milliseconds per second (1000)
 * Used for human-readable duration formatting
 */
export const MS_PER_SECOND = 1000

/**
 * Milliseconds per minute (60,000)
 * Used for human-readable duration formatting
 */
export const MS_PER_MINUTE = 60 * 1000

/**
 * Milliseconds per hour (3,600,000)
 * Used for human-readable duration formatting
 */
export const MS_PER_HOUR = 60 * 60 * 1000
