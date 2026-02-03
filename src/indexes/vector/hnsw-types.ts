/**
 * HNSW Vector Index Types
 *
 * Type definitions for the Hierarchical Navigable Small World (HNSW) vector index.
 */

/** Precision for vector serialization */
export type VectorPrecision = 'float32' | 'float64'

/**
 * Options for memory-bounded vector index
 */
export interface VectorIndexMemoryOptions {
  /** Maximum number of nodes to keep in memory */
  maxNodes?: number | undefined
  /** Maximum memory in bytes */
  maxBytes?: number | undefined
  /** Callback when a node is evicted from cache */
  onEvict?: ((nodeId: number) => void | undefined | undefined) | undefined
}

/**
 * Metadata about a row group for incremental update tracking
 */
export interface RowGroupMetadata {
  /** Row group number */
  rowGroup: number
  /** Number of vectors indexed from this row group */
  vectorCount: number
  /** Minimum row offset indexed */
  minRowOffset: number
  /** Maximum row offset indexed */
  maxRowOffset: number
  /** Checksum/hash of the row group content for change detection */
  checksum?: string | undefined
  /** Timestamp when this row group was indexed */
  indexedAt: number
}

/**
 * Result of an incremental update operation
 */
export interface IncrementalUpdateResult {
  /** Number of vectors removed (from stale row groups) */
  removed: number
  /** Number of vectors added (from new/changed row groups) */
  added: number
  /** Row groups that were updated */
  updatedRowGroups: number[]
  /** Row groups that were removed */
  removedRowGroups: number[]
  /** Whether the update was successful */
  success: boolean
  /** Error message if update failed */
  error?: string | undefined
}

/**
 * Options for incremental update
 */
export interface IncrementalUpdateOptions {
  /** Row group checksums for change detection (rowGroup -> checksum) */
  checksums?: Map<number, string> | undefined
  /** Specific row groups to update (if not provided, auto-detect changes) */
  rowGroupsToUpdate?: number[] | undefined
  /** Row group number remapping after compaction (oldRowGroup -> newRowGroup) */
  rowGroupRemapping?: Map<number, number> | undefined
  /** Progress callback */
  onProgress?: ((processed: number, total: number) => void | undefined | undefined) | undefined
}

/**
 * HNSW graph node
 */
export interface HNSWNode {
  id: number
  docId: string
  vector: number[]
  rowGroup: number
  rowOffset: number
  /** Connections at each layer: layer -> array of node IDs */
  connections: Map<number, number[]>
  /** Maximum layer this node exists in */
  maxLayer: number
}

/**
 * Search candidate with distance
 */
export interface SearchCandidate {
  nodeId: number
  distance: number
}

/**
 * Error thrown when index configuration doesn't match serialized data.
 * This error should not be silently caught - it indicates a configuration problem.
 */
export class VectorIndexConfigError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'VectorIndexConfigError'
  }
}
