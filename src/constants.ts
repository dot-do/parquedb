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

/**
 * Maximum level in HNSW graph hierarchy.
 * This caps the number of layers in the graph to prevent unbounded growth.
 * The probability of a node being assigned to level L is (1/M)^L,
 * so level 32 has probability ~10^-39 for M=16, effectively unreachable.
 */
export const MAX_HNSW_LEVEL = 32

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

/**
 * Default maximum edit distance for fuzzy matching
 * Higher values allow more typos but increase false positives
 */
export const DEFAULT_FTS_FUZZY_MAX_DISTANCE = 2

/**
 * Default minimum term length for fuzzy matching
 * Short terms are matched exactly to avoid excessive false positives
 */
export const DEFAULT_FTS_FUZZY_MIN_TERM_LENGTH = 4

/**
 * Default prefix length for fuzzy matching
 * Characters that must match exactly at the start of the term
 */
export const DEFAULT_FTS_FUZZY_PREFIX_LENGTH = 1

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

/**
 * Maximum number of nodes to keep in memory for vector index
 * Default: 100,000 nodes (~10MB for 128-dim vectors with PQ)
 */
export const DEFAULT_VECTOR_INDEX_MAX_NODES = 100000

/**
 * Maximum memory in bytes for vector index
 * Default: 64MB (fits within Cloudflare Workers 128MB limit with headroom)
 */
export const DEFAULT_VECTOR_INDEX_MAX_BYTES = 64 * 1024 * 1024

/**
 * Number of sub-quantizers for Product Quantization
 * Higher = better recall but more memory
 */
export const DEFAULT_PQ_SUBQUANTIZERS = 8

/**
 * Bits per sub-quantizer code (2^8 = 256 centroids)
 */
export const DEFAULT_PQ_BITS = 8

/**
 * Number of centroids per sub-quantizer (2^DEFAULT_PQ_BITS)
 */
export const DEFAULT_PQ_CENTROIDS = 256

/**
 * Enable Product Quantization by default for large indexes
 * Threshold: indexes with more than this many vectors will use PQ
 */
export const DEFAULT_PQ_THRESHOLD = 10000

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

/**
 * Maximum query result limit (prevents OOM from malicious input)
 */
export const MAX_QUERY_LIMIT = 1000

/**
 * Maximum query offset (prevents excessive skip operations)
 */
export const MAX_QUERY_OFFSET = 100000

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
// Global Storage Constants (Testing/Development)
// =============================================================================

/**
 * Maximum number of namespaces in global storage
 * Prevents unbounded memory growth in long-running test processes
 */
export const DEFAULT_GLOBAL_STORAGE_MAX_NAMESPACES = 100

/**
 * Maximum entities per namespace in global storage
 * LRU eviction applies when this limit is exceeded
 */
export const DEFAULT_GLOBAL_STORAGE_MAX_ENTITIES_PER_NS = 10000

/**
 * Maximum relationships per namespace in global storage
 */
export const DEFAULT_GLOBAL_STORAGE_MAX_RELS_PER_NS = 50000

/**
 * Maximum events in global event log
 * Oldest events are evicted when this limit is exceeded
 */
export const DEFAULT_GLOBAL_EVENT_LOG_MAX_ENTRIES = 10000

/**
 * TTL for global event log entries in milliseconds (1 hour)
 * Events older than this are eligible for cleanup
 */
export const DEFAULT_GLOBAL_EVENT_LOG_TTL_MS = 60 * 60 * 1000

/**
 * Cleanup interval for global storage in milliseconds (5 minutes)
 * How often to run automatic cleanup of expired entries
 */
export const DEFAULT_GLOBAL_STORAGE_CLEANUP_INTERVAL_MS = 5 * 60 * 1000

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

/**
 * Milliseconds per day (86,400,000)
 * Used for human-readable duration formatting
 */
export const MS_PER_DAY = 24 * 60 * 60 * 1000

/**
 * Seconds per day (86,400)
 * Used for CORS Access-Control-Max-Age headers
 */
export const SECONDS_PER_DAY = 86400

// =============================================================================
// Worker Constants
// =============================================================================

/**
 * Default flush interval for Durable Objects in milliseconds (30 seconds)
 */
export const DEFAULT_DO_FLUSH_INTERVAL_MS = 30000

/**
 * Maximum buffer size for TailDO (events before forced flush)
 */
export const DEFAULT_TAIL_BUFFER_SIZE = 1000

/**
 * Maximum pending invalidation signals to keep in DO
 */
export const MAX_PENDING_INVALIDATIONS = 100

/**
 * Entity cache maximum size in DO
 */
export const DEFAULT_ENTITY_CACHE_SIZE = 1000

/**
 * Alias for entity cache max size (used by cache-manager.ts)
 */
export const ENTITY_CACHE_MAX_SIZE = DEFAULT_ENTITY_CACHE_SIZE

/**
 * Event batch count threshold for WAL
 */
export const EVENT_BATCH_COUNT_THRESHOLD = 100

/**
 * Event batch size threshold in bytes (64KB)
 */
export const DEFAULT_EVENT_BATCH_SIZE_BYTES = 64 * 1024

/**
 * Bulk write threshold - entities above this count go directly to R2
 */
export const BULK_WRITE_THRESHOLD = 5

// =============================================================================
// Flush Manager Constants
// =============================================================================

/**
 * Default minimum events before flush
 */
export const DEFAULT_FLUSH_MIN_EVENTS = 100

/**
 * Default maximum flush interval in milliseconds (30 seconds)
 */
export const DEFAULT_FLUSH_MAX_INTERVAL_MS = 30000

/**
 * Default row group size for flush manager
 */
export const DEFAULT_FLUSH_ROW_GROUP_SIZE = DEFAULT_ROW_GROUP_SIZE

// =============================================================================
// WAL-Only Mode & WAL Optimizer Constants
// =============================================================================

/**
 * Default WAL-only mode setting.
 * When true, entity/relationship snapshot tables are skipped in DO SQLite.
 */
export const WAL_ONLY_MODE_DEFAULT = false

/**
 * Minimum batch count before WAL compaction is triggered (default: 10 batches)
 */
export const WAL_COMPACTION_MIN_BATCHES = 10

/**
 * Target batch size after compaction (default: 1000 events)
 */
export const WAL_COMPACTION_TARGET_EVENTS = 1000

/**
 * Maximum blob size before splitting (default: 256KB)
 */
export const WAL_MAX_BLOB_SIZE = 256 * 1024

/**
 * Retention period for flushed batches in ms (default: 7 days)
 */
export const WAL_FLUSHED_RETENTION_MS = 7 * 24 * 60 * 60 * 1000

/**
 * Window size for adaptive threshold calculation in ms (default: 1 minute)
 */
export const WAL_ADAPTIVE_WINDOW_MS = 60 * 1000

/**
 * Minimum batch threshold (default: 10)
 */
export const WAL_MIN_BATCH_THRESHOLD = 10

/**
 * Maximum batch threshold (default: 1000)
 */
export const WAL_MAX_BATCH_THRESHOLD = 1000

/**
 * Compression savings threshold - only use compression if it saves this much (default: 0.9 = 10% savings)
 */
export const COMPRESSION_SAVINGS_THRESHOLD = 0.9

// =============================================================================
// Sync Token Constants
// =============================================================================

/**
 * Maximum nonce cache size for replay protection
 */
export const MAX_NONCE_CACHE_SIZE = 10000

/**
 * Nonce cleanup threshold
 */
export const NONCE_CLEANUP_THRESHOLD = 1000

/**
 * Clock skew tolerance in milliseconds (5 seconds)
 */
export const CLOCK_SKEW_TOLERANCE_MS = 5000

// =============================================================================
// JWT/JWKS Constants
// =============================================================================

/**
 * JWKS cache TTL in milliseconds (1 hour)
 */
export const JWKS_CACHE_TTL = 3600 * 1000

/**
 * JWKS fetch timeout in milliseconds (10 seconds)
 */
export const JWKS_FETCH_TIMEOUT_MS = 10000

// =============================================================================
// Circuit Breaker Constants
// =============================================================================

/**
 * Default failure threshold before opening circuit
 */
export const DEFAULT_FAILURE_THRESHOLD = 5

/**
 * Default success threshold before closing circuit
 */
export const DEFAULT_SUCCESS_THRESHOLD = 2

/**
 * Default circuit breaker reset timeout in milliseconds (30 seconds)
 */
export const DEFAULT_CIRCUIT_RESET_TIMEOUT_MS = 30000

/**
 * Default failure window in milliseconds (60 seconds)
 */
export const DEFAULT_FAILURE_WINDOW_MS = 60000

/**
 * Fast circuit breaker reset timeout in milliseconds (10 seconds)
 */
export const FAST_CIRCUIT_RESET_TIMEOUT_MS = 10000

/**
 * Slow circuit breaker reset timeout in milliseconds (60 seconds)
 */
export const SLOW_CIRCUIT_RESET_TIMEOUT_MS = 60000

/**
 * Fast circuit breaker failure window in milliseconds (30 seconds)
 */
export const FAST_FAILURE_WINDOW_MS = 30000

/**
 * Slow circuit breaker failure window in milliseconds (120 seconds)
 */
export const SLOW_FAILURE_WINDOW_MS = 120000

// =============================================================================
// Lock Acquisition Constants
// =============================================================================

/**
 * Default lock timeout in milliseconds (30 seconds)
 */
export const DEFAULT_LOCK_TIMEOUT_MS = 30000

/**
 * Default lock wait timeout in milliseconds (5 seconds)
 */
export const DEFAULT_LOCK_WAIT_TIMEOUT_MS = 5000

/**
 * Stale lock age threshold in milliseconds (30 seconds)
 */
export const STALE_LOCK_AGE_MS = 30000

// =============================================================================
// Cache TTL Constants
// =============================================================================

/**
 * Default data cache TTL in seconds (1 minute)
 */
export const DEFAULT_DATA_CACHE_TTL_SECONDS = 60

/**
 * Default metadata cache TTL in seconds (5 minutes)
 */
export const DEFAULT_METADATA_CACHE_TTL_SECONDS = 300

/**
 * Default bloom filter cache TTL in seconds (10 minutes)
 */
export const DEFAULT_BLOOM_CACHE_TTL_SECONDS = 600

/**
 * Read-heavy data cache TTL in seconds (5 minutes)
 */
export const READ_HEAVY_DATA_CACHE_TTL_SECONDS = 300

/**
 * Read-heavy metadata cache TTL in seconds (15 minutes)
 */
export const READ_HEAVY_METADATA_CACHE_TTL_SECONDS = 900

/**
 * Read-heavy bloom cache TTL in seconds (30 minutes)
 */
export const READ_HEAVY_BLOOM_CACHE_TTL_SECONDS = 1800

/**
 * Write-heavy data cache TTL in seconds (15 seconds)
 */
export const WRITE_HEAVY_DATA_CACHE_TTL_SECONDS = 15

/**
 * Write-heavy metadata cache TTL in seconds (1 minute)
 */
export const WRITE_HEAVY_METADATA_CACHE_TTL_SECONDS = 60

/**
 * Write-heavy bloom cache TTL in seconds (2 minutes)
 */
export const WRITE_HEAVY_BLOOM_CACHE_TTL_SECONDS = 120

/**
 * GitHub config cache TTL in milliseconds (5 minutes)
 */
export const GITHUB_CONFIG_CACHE_TTL_MS = 5 * 60 * 1000

// =============================================================================
// Batch Processing Constants
// =============================================================================

/**
 * Default tail batch max wait time in milliseconds (10 seconds)
 */
export const DEFAULT_TAIL_BATCH_MAX_WAIT_MS = 10000

/**
 * Default streaming batch wait time in milliseconds (1 second)
 */
export const DEFAULT_STREAMING_BATCH_WAIT_MS = 1000

/**
 * Default observability batch timeout in milliseconds (1 second)
 */
export const DEFAULT_OBSERVABILITY_BATCH_TIMEOUT_MS = 1000

/**
 * Default evalite retention period in milliseconds (30 days)
 */
export const DEFAULT_EVALITE_RETENTION_MS = 30 * 24 * 60 * 60 * 1000

// =============================================================================
// Validation Constants
// =============================================================================

/**
 * Maximum string length for MCP validation
 */
export const MAX_MCP_STRING_LENGTH = 10000

/**
 * Maximum limit for pagination in MCP
 */
export const MAX_MCP_PAGINATION_LIMIT = 1000

/**
 * Maximum string length for prompt/query validation in MCP
 */
export const MAX_MCP_PROMPT_LENGTH = 1000

/**
 * Maximum limit for Payload operations
 */
export const MAX_PAYLOAD_OPERATION_LIMIT = 10000

/**
 * Maximum log length for serialization
 */
export const MAX_LOG_SERIALIZE_LENGTH = 10000

// =============================================================================
// Transaction Stale Threshold
// =============================================================================

/**
 * Stale transaction threshold in milliseconds (5 minutes)
 */
export const STALE_TRANSACTION_THRESHOLD_MS = 5 * 60 * 1000

// =============================================================================
// Rate Limit Window
// =============================================================================

/**
 * Default rate limit window in milliseconds (1 minute)
 */
export const DEFAULT_RATE_LIMIT_WINDOW_MS = 60 * 1000

// =============================================================================
// Migration/Alarm Constants
// =============================================================================

/**
 * Default migration retry base delay in milliseconds (1 second)
 */
export const MIGRATION_RETRY_BASE_DELAY_MS = 1000

/**
 * Migration alarm delay in milliseconds (1 second)
 */
export const MIGRATION_ALARM_DELAY_MS = 1000

/**
 * Sync token URL expiry in milliseconds (1 hour)
 */
export const SYNC_TOKEN_URL_EXPIRY_MS = 3600 * 1000

// =============================================================================
// High Water Mark Constants
// =============================================================================

/**
 * Default high water mark for streaming in bytes (64KB)
 */
export const DEFAULT_HIGH_WATER_MARK = 64 * 1024

// =============================================================================
// Backpressure Constants
// =============================================================================

/**
 * Default backpressure max buffer size in bytes (1MB)
 */
export const DEFAULT_BACKPRESSURE_MAX_BUFFER_BYTES = 1024 * 1024

/**
 * Default backpressure max event count
 */
export const DEFAULT_BACKPRESSURE_MAX_EVENTS = 1000

/**
 * Default backpressure max pending flushes
 */
export const DEFAULT_BACKPRESSURE_MAX_PENDING_FLUSHES = 10

/**
 * Default backpressure release threshold (50%)
 */
export const DEFAULT_BACKPRESSURE_RELEASE_THRESHOLD = 0.5

/**
 * Default backpressure timeout in milliseconds (30 seconds)
 */
export const DEFAULT_BACKPRESSURE_TIMEOUT_MS = 30000

// =============================================================================
// Workers Paid Limit
// =============================================================================

/**
 * Cloudflare Workers paid subrequest limit per invocation
 */
export const WORKERS_PAID_SUBREQUEST_LIMIT = 1000

// =============================================================================
// R2 Multipart Constants
// =============================================================================

/**
 * Maximum part number for multipart uploads (R2/S3 limit)
 */
export const MAX_MULTIPART_PART_NUMBER = 10000

// =============================================================================
// CSRF Token Constants
// =============================================================================

/**
 * Default CSRF token TTL in milliseconds (1 hour)
 */
export const DEFAULT_CSRF_TOKEN_TTL_MS = 3600000

// =============================================================================
// Iceberg Constants
// =============================================================================

/**
 * Default Iceberg max retry delay in milliseconds (10 seconds)
 */
export const DEFAULT_ICEBERG_MAX_RETRY_DELAY_MS = 10000

/**
 * Default write lock timeout in milliseconds (30 seconds)
 */
export const DEFAULT_WRITE_LOCK_TIMEOUT_MS = 30000
