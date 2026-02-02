/**
 * Index Cache Types
 *
 * Shared type definitions for IndexCache, extracted to avoid
 * circular dependencies between json-validation.ts and IndexCache.ts.
 *
 * NOTE: SST indexes have been removed - native parquet predicate pushdown
 * on $index_* columns is now faster than secondary indexes for range queries.
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Index catalog entry in _catalog.json
 */
export interface IndexCatalogEntry {
  /** Index name */
  name: string
  /** Index type (SST removed - use native parquet predicate pushdown for range queries) */
  type: 'hash' | 'fts'
  /** Indexed field (e.g., '$index_titleType') */
  field: string
  /** Path to index file relative to dataset root */
  path: string
  /** Index file size in bytes */
  sizeBytes: number
  /** Number of entries in the index */
  entryCount: number
  /** Last updated timestamp */
  updatedAt?: string
  /** Whether this index is sharded */
  sharded?: boolean
  /** Path to manifest file (for sharded indexes) */
  manifestPath?: string
}

/**
 * Shard entry in a sharded index manifest
 */
export interface ShardEntry {
  /** Shard name */
  name: string
  /** Path to shard file relative to index directory */
  path: string
  /** Value this shard contains (for hash indexes) */
  value: string | number | boolean
  /** Number of entries in this shard */
  entryCount: number
  /** Min value (kept for backward compatibility) */
  minValue?: unknown
  /** Max value (kept for backward compatibility) */
  maxValue?: unknown
}

/**
 * Sharded index manifest format
 */
export interface ShardedIndexManifest {
  /** Manifest version */
  version: number
  /** Index type (SST removed - use native parquet predicate pushdown for range queries) */
  type: 'hash'
  /** Indexed field */
  field: string
  /** Sharding strategy */
  sharding: 'by-value' | 'by-range'
  /** Whether shards use compact format */
  compact: boolean
  /** Path to bloom filter file (optional) */
  bloomPath?: string
  /** Total entry count across all shards */
  totalEntries?: number
  /** Shard entries */
  shards: ShardEntry[]
}

/**
 * Index catalog stored in _catalog.json
 */
export interface IndexCatalog {
  /** Catalog version */
  version: number
  /** Index entries */
  indexes: IndexCatalogEntry[]
}
