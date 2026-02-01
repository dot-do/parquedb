/**
 * ParqueDB Constants
 *
 * Centralized constants used throughout the codebase.
 * Eliminates magic numbers and provides single source of truth.
 */

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
// Cache Constants
// =============================================================================

/**
 * Maximum cache size in bytes (2MB)
 * Used for whole-file caching in QueryExecutor
 */
export const MAX_CACHE_SIZE = 2 * 1024 * 1024

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
