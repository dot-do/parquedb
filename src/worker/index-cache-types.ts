/**
 * Index Cache Types
 *
 * Shared type definitions for IndexCache, extracted to avoid
 * circular dependencies between json-validation.ts and IndexCache.ts.
 *
 * NOTE: Hash and SST indexes have been removed - native parquet predicate pushdown
 * on $index_* columns is now faster than secondary indexes for equality and range queries.
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Index catalog entry in _catalog.json
 *
 * NOTE: Hash indexes have been removed - equality queries now use native
 * parquet predicate pushdown on $index_* columns.
 */
export interface IndexCatalogEntry {
  /** Index name */
  name: string
  /** Index type */
  type: 'fts'
  /** Indexed field (e.g., 'name') */
  field: string
  /** Path to index file relative to dataset root */
  path: string
  /** Index file size in bytes */
  sizeBytes: number
  /** Number of entries in the index */
  entryCount: number
  /** Last updated timestamp */
  updatedAt?: string
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
