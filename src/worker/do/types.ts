/**
 * ParqueDB Durable Object Types
 *
 * Shared type definitions for DO modules.
 */

import type {
  Entity,
  EntityId,
  Event,
  Namespace,
  Id,
  Relationship,
} from '../../types'

// =============================================================================
// Cache Invalidation Types
// =============================================================================

/**
 * Cache invalidation signal stored in DO
 * Workers poll this to know when to invalidate their caches
 */
export interface CacheInvalidationSignal {
  /** Namespace that was modified */
  ns: string
  /** Type of invalidation */
  type: 'entity' | 'relationship' | 'full'
  /** Timestamp of the modification */
  timestamp: number
  /** Version number (monotonically increasing) */
  version: number
  /** Optional entity ID for entity-specific invalidation */
  entityId?: string
}

// =============================================================================
// Operation Options
// =============================================================================

/** Options for create operation */
export interface DOCreateOptions {
  /** Actor performing the operation */
  actor?: string
  /** Skip validation */
  skipValidation?: boolean
}

/** Options for update operation */
export interface DOUpdateOptions {
  /** Actor performing the operation */
  actor?: string
  /** Expected version for optimistic concurrency */
  expectedVersion?: number
  /** Create if not exists */
  upsert?: boolean
}

/** Options for delete operation */
export interface DODeleteOptions {
  /** Actor performing the operation */
  actor?: string
  /** Hard delete (permanent) */
  hard?: boolean
  /** Expected version for optimistic concurrency */
  expectedVersion?: number
}

/** Options for link operation */
export interface DOLinkOptions {
  /** Actor performing the operation */
  actor?: string
  /**
   * How the relationship was matched (SHREDDED)
   * - 'exact': Precise match (user explicitly linked)
   * - 'fuzzy': Approximate match (entity resolution, text similarity)
   */
  matchMode?: 'exact' | 'fuzzy'
  /**
   * Similarity score for fuzzy matches (SHREDDED)
   * Range: 0.0 to 1.0
   * Only meaningful when matchMode is 'fuzzy'
   */
  similarity?: number
  /** Edge data (remaining metadata in Variant) */
  data?: Record<string, unknown>
}

// =============================================================================
// Storage Types
// =============================================================================

/** Entity as stored in SQLite */
export interface StoredEntity {
  [key: string]: SqlStorageValue
  ns: string
  id: string
  type: string
  name: string
  version: number
  created_at: string
  created_by: string
  updated_at: string
  updated_by: string
  deleted_at: string | null
  deleted_by: string | null
  data: string
}

/** Relationship as stored in SQLite */
export interface StoredRelationship {
  [key: string]: SqlStorageValue
  from_ns: string
  from_id: string
  predicate: string
  to_ns: string
  to_id: string
  reverse: string
  version: number
  created_at: string
  created_by: string
  deleted_at: string | null
  deleted_by: string | null
  // Shredded fields (top-level columns for efficient querying)
  match_mode: string | null // 'exact' | 'fuzzy'
  similarity: number | null // 0.0 to 1.0
  // Remaining metadata in Variant
  data: string | null
}

// =============================================================================
// Event Buffer Types
// =============================================================================

/** Event buffer for namespace-based WAL batching */
export interface EventBuffer {
  events: Event[]
  firstSeq: number
  lastSeq: number
  sizeBytes: number
}

// =============================================================================
// Transaction Types
// =============================================================================

/**
 * Transaction snapshot interface for rollback support
 */
export interface TransactionSnapshot {
  counters: Map<string, number>
  entityCache: Map<string, { entity: Entity; version: number }>
  eventBuffer: Event[]
  eventBufferSize: number
  nsEventBuffers: Map<string, EventBuffer>
  relEventBuffers: Map<string, EventBuffer>
  sqlRollbackOps: Array<{
    type:
      | 'entity_insert'
      | 'entity_update'
      | 'entity_delete'
      | 'rel_insert'
      | 'rel_update'
      | 'rel_delete'
      | 'pending_row_group'
    ns: string
    id: string
    predicate?: string
    toNs?: string
    toId?: string
    beforeState?: StoredEntity | StoredRelationship | null
  }>
  pendingR2Paths: string[]
}

// =============================================================================
// Flush Configuration
// =============================================================================

export interface FlushConfig {
  minEvents: number
  maxInterval: number
  maxEvents: number
  rowGroupSize: number
}

// =============================================================================
// Utility Type Conversions
// =============================================================================

/**
 * Convert stored entity to API entity format
 */
export function toEntity(stored: StoredEntity): Entity {
  const data = parseStoredData(stored.data)

  return {
    $id: `${stored.ns}/${stored.id}` as EntityId,
    $type: stored.type,
    name: stored.name,
    createdAt: new Date(stored.created_at),
    createdBy: stored.created_by as EntityId,
    updatedAt: new Date(stored.updated_at),
    updatedBy: stored.updated_by as EntityId,
    deletedAt: stored.deleted_at ? new Date(stored.deleted_at) : undefined,
    deletedBy: stored.deleted_by as EntityId | undefined,
    version: stored.version,
    ...data,
  } as Entity
}

/**
 * Convert stored relationship to API relationship format
 */
export function toRelationship(stored: StoredRelationship): Relationship {
  return {
    fromNs: stored.from_ns as Namespace,
    fromId: stored.from_id as Id,
    fromType: '', // Would need to look up from entity
    fromName: '', // Would need to look up from entity
    predicate: stored.predicate,
    reverse: stored.reverse,
    toNs: stored.to_ns as Namespace,
    toId: stored.to_id as Id,
    toType: '', // Would need to look up from entity
    toName: '', // Would need to look up from entity
    // Shredded fields
    matchMode: stored.match_mode as Relationship['matchMode'],
    similarity: stored.similarity ?? undefined,
    // Audit fields
    createdAt: new Date(stored.created_at),
    createdBy: stored.created_by as EntityId,
    deletedAt: stored.deleted_at ? new Date(stored.deleted_at) : undefined,
    deletedBy: stored.deleted_by as EntityId | undefined,
    version: stored.version,
    data: stored.data ? parseStoredData(stored.data) : undefined,
  }
}

/**
 * Parse stored JSON data safely
 */
export function parseStoredData(data: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(data)
    if (typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>
    }
    return {}
  } catch {
    return {}
  }
}
