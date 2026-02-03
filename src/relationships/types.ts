/**
 * Relationship Types for ParqueDB
 *
 * Defines relationship metadata types with support for Variant shredding.
 * Frequently-queried fields (matchMode, similarity) are stored as top-level
 * columns for efficient predicate pushdown, while remaining metadata stays
 * in the Variant data column for flexibility.
 *
 * @packageDocumentation
 */

// =============================================================================
// Match Mode Types
// =============================================================================

/**
 * How the relationship was matched
 *
 * - 'exact': Precise match (e.g., user explicitly linked entities)
 * - 'fuzzy': Approximate match (e.g., found via text similarity, entity resolution)
 */
export type MatchMode = 'exact' | 'fuzzy'

// =============================================================================
// Relationship Metadata
// =============================================================================

/**
 * Metadata for a relationship edge
 *
 * This interface defines the structure for relationship metadata with
 * Variant shredding support. Fields marked as "shredded" are stored as
 * top-level Parquet columns for efficient filtering.
 *
 * @example
 * ```typescript
 * const metadata: RelationshipMetadata = {
 *   matchMode: 'fuzzy',
 *   similarity: 0.85,
 *   matchedType: 'name-match',
 *   confidence: 'high',
 *   source: 'entity-resolution-service'
 * }
 * ```
 */
export interface RelationshipMetadata {
  /**
   * How the relationship was matched
   *
   * SHREDDED: Stored as top-level Parquet column for efficient filtering.
   * Use this to distinguish between explicit user-created relationships
   * and automatically-inferred relationships.
   *
   * @example
   * ```typescript
   * // Find all fuzzy-matched relationships
   * const fuzzy = await db.findRelationships({
   *   matchMode: 'fuzzy',
   *   similarity: { $gte: 0.8 }
   * })
   * ```
   */
  matchMode?: MatchMode | undefined

  /**
   * Similarity score for fuzzy matches (0.0 to 1.0)
   *
   * SHREDDED: Stored as top-level Parquet column for efficient filtering.
   * Use for threshold-based queries like "find relationships with
   * similarity >= 0.8".
   *
   * Only meaningful when matchMode is 'fuzzy'.
   *
   * @example
   * ```typescript
   * // Find high-confidence fuzzy matches
   * const highConfidence = await db.findRelationships({
   *   matchMode: 'fuzzy',
   *   similarity: { $gte: 0.9 }
   * })
   * ```
   */
  similarity?: number | undefined

  /**
   * Type of match that produced this relationship
   *
   * Stored in Variant data column (not shredded).
   *
   * @example
   * - 'name-match': Matched by name similarity
   * - 'id-match': Matched by external ID
   * - 'manual': Manually created by user
   */
  matchedType?: string | undefined

  /**
   * Additional metadata fields
   *
   * Stored in Variant data column for flexibility.
   * Use for domain-specific metadata that doesn't need efficient filtering.
   */
  [key: string]: unknown
}

// =============================================================================
// Shredded Relationship Fields
// =============================================================================

/**
 * Fields that are shredded (stored as top-level columns) for efficient querying
 *
 * These fields support predicate pushdown and can use Parquet column statistics
 * for row group skipping.
 */
export interface ShreddedRelationshipFields {
  /** How the relationship was matched */
  matchMode?: MatchMode | undefined
  /** Similarity score for fuzzy matches (0.0 to 1.0) */
  similarity?: number | undefined
}

/**
 * Type guard to check if a value is a valid MatchMode
 */
export function isMatchMode(value: unknown): value is MatchMode {
  return value === 'exact' || value === 'fuzzy'
}

/**
 * Type guard to check if a value is a valid similarity score
 */
export function isValidSimilarity(value: unknown): value is number {
  return typeof value === 'number' && value >= 0 && value <= 1
}

// =============================================================================
// Relationship Filter Types
// =============================================================================

/**
 * Filter options for relationship queries
 *
 * Supports filtering on shredded fields (matchMode, similarity) with
 * predicate pushdown for efficient querying.
 */
export interface RelationshipFilter {
  /** Filter by source namespace */
  fromNs?: string | undefined
  /** Filter by source entity ID */
  fromId?: string | undefined
  /** Filter by predicate name */
  predicate?: string | undefined
  /** Filter by target namespace */
  toNs?: string | undefined
  /** Filter by target entity ID */
  toId?: string | undefined

  // Shredded field filters (support predicate pushdown)

  /** Filter by match mode */
  matchMode?: MatchMode | undefined
  /** Filter by exact similarity score */
  similarity?: number | undefined
  /** Filter by minimum similarity (inclusive) */
  minSimilarity?: number | undefined
  /** Filter by maximum similarity (inclusive) */
  maxSimilarity?: number | undefined
}

// =============================================================================
// Relationship Query Options
// =============================================================================

/**
 * Options for relationship queries
 */
export interface RelationshipQueryOptions {
  /** Maximum number of relationships to return */
  limit?: number | undefined
  /** Number of relationships to skip (for pagination) */
  offset?: number | undefined
  /** Sort field */
  sortBy?: 'similarity' | 'createdAt' | 'predicate' | undefined
  /** Sort direction */
  sortDirection?: 'asc' | 'desc' | undefined
  /** Include deleted relationships */
  includeDeleted?: boolean | undefined
}

// =============================================================================
// Relationship Query Result
// =============================================================================

/**
 * Result of a relationship query
 */
export interface RelationshipQueryResult<T = RelationshipWithMetadata> {
  /** Matching relationships */
  items: T[]
  /** Total count of matching relationships (if available) */
  total?: number | undefined
  /** Whether there are more results */
  hasMore: boolean
  /** Cursor for next page */
  nextCursor?: string | undefined
}

// =============================================================================
// Full Relationship Type (with shredded fields)
// =============================================================================

/**
 * Full relationship structure with shredded metadata fields
 *
 * This extends the base Relationship type from entity.ts with the
 * shredded metadata fields for efficient querying.
 */
export interface RelationshipWithMetadata {
  /** Source namespace */
  fromNs: string
  /** Source entity ID */
  fromId: string
  /** Source entity type */
  fromType?: string | undefined
  /** Source entity display name */
  fromName?: string | undefined

  /** Outbound relationship name (e.g., "author", "category") */
  predicate: string
  /** Inbound relationship name (e.g., "posts", "items") */
  reverse?: string | undefined

  /** Target namespace */
  toNs: string
  /** Target entity ID */
  toId: string
  /** Target entity type */
  toType?: string | undefined
  /** Target entity display name */
  toName?: string | undefined

  // ===========================================================================
  // Shredded Fields (top-level columns for efficient querying)
  // ===========================================================================

  /** How the relationship was matched (SHREDDED) */
  matchMode?: MatchMode | undefined
  /** Similarity score for fuzzy matches (SHREDDED) */
  similarity?: number | undefined

  // ===========================================================================
  // Variant Data (remaining metadata)
  // ===========================================================================

  /** Additional edge properties stored in Variant column */
  data?: Record<string, unknown> | undefined

  // ===========================================================================
  // Audit Fields
  // ===========================================================================

  /** When the relationship was created */
  createdAt: Date
  /** Who created the relationship */
  createdBy?: string | undefined
  /** Soft delete timestamp */
  deletedAt?: Date | undefined
  /** Who deleted the relationship */
  deletedBy?: string | undefined
  /** Optimistic concurrency version */
  version: number
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Extract shredded fields from metadata
 *
 * Separates shredded fields (matchMode, similarity) from the rest of the
 * metadata for efficient storage.
 *
 * @param metadata - Full relationship metadata
 * @returns Object with shredded fields and remaining data
 */
export function extractShreddedFields(metadata: RelationshipMetadata): {
  shredded: ShreddedRelationshipFields
  data: Record<string, unknown>
} {
  const { matchMode, similarity, ...data } = metadata

  const shredded: ShreddedRelationshipFields = {}
  if (matchMode !== undefined) {
    shredded.matchMode = matchMode
  }
  if (similarity !== undefined) {
    shredded.similarity = similarity
  }

  return { shredded, data }
}

/**
 * Merge shredded fields back into metadata
 *
 * Combines shredded fields with Variant data for a complete metadata object.
 *
 * @param shredded - Shredded fields from top-level columns
 * @param data - Remaining data from Variant column
 * @returns Complete relationship metadata
 */
export function mergeShreddedFields(
  shredded: ShreddedRelationshipFields,
  data?: Record<string, unknown>
): RelationshipMetadata {
  return {
    ...data,
    ...(shredded.matchMode !== undefined && { matchMode: shredded.matchMode }),
    ...(shredded.similarity !== undefined && { similarity: shredded.similarity }),
  }
}

/**
 * Validate similarity score is in valid range [0, 1]
 *
 * @param similarity - Similarity score to validate
 * @throws Error if similarity is out of range
 */
export function validateSimilarity(similarity: number): void {
  if (similarity < 0 || similarity > 1) {
    throw new Error(`Similarity must be between 0 and 1, got ${similarity}`)
  }
}

/**
 * Validate match mode and similarity consistency
 *
 * - If matchMode is 'exact', similarity should not be set (or should be 1.0)
 * - If matchMode is 'fuzzy', similarity should be set
 *
 * @param matchMode - Match mode
 * @param similarity - Similarity score
 * @throws Error if validation fails
 */
export function validateMatchModeConsistency(
  matchMode?: MatchMode,
  similarity?: number
): void {
  if (matchMode === 'exact' && similarity !== undefined && similarity !== 1.0) {
    throw new Error(
      `Exact match mode should have similarity of 1.0 or undefined, got ${similarity}`
    )
  }

  if (matchMode === 'fuzzy' && similarity === undefined) {
    throw new Error('Fuzzy match mode requires a similarity score')
  }
}
