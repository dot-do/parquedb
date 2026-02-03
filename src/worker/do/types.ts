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
import {
  DEFAULT_BACKPRESSURE_MAX_BUFFER_BYTES,
  DEFAULT_BACKPRESSURE_MAX_EVENTS,
  DEFAULT_BACKPRESSURE_MAX_PENDING_FLUSHES,
  DEFAULT_BACKPRESSURE_RELEASE_THRESHOLD,
  DEFAULT_BACKPRESSURE_TIMEOUT_MS,
} from '../../constants'

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
  entityId?: string | undefined
}

// =============================================================================
// Operation Options
// =============================================================================

/** Options for create operation */
export interface DOCreateOptions {
  /** Actor performing the operation */
  actor?: string | undefined
  /** Skip validation */
  skipValidation?: boolean | undefined
}

/** Options for update operation */
export interface DOUpdateOptions {
  /** Actor performing the operation */
  actor?: string | undefined
  /** Expected version for optimistic concurrency */
  expectedVersion?: number | undefined
  /** Create if not exists */
  upsert?: boolean | undefined
}

/** Options for delete operation */
export interface DODeleteOptions {
  /** Actor performing the operation */
  actor?: string | undefined
  /** Hard delete (permanent) */
  hard?: boolean | undefined
  /** Expected version for optimistic concurrency */
  expectedVersion?: number | undefined
}

/** Options for link operation */
export interface DOLinkOptions {
  /** Actor performing the operation */
  actor?: string | undefined
  /**
   * How the relationship was matched (SHREDDED)
   * - 'exact': Precise match (user explicitly linked)
   * - 'fuzzy': Approximate match (entity resolution, text similarity)
   */
  matchMode?: 'exact' | 'fuzzy' | undefined
  /**
   * Similarity score for fuzzy matches (SHREDDED)
   * Range: 0.0 to 1.0
   * Only meaningful when matchMode is 'fuzzy'
   */
  similarity?: number | undefined
  /** Edge data (remaining metadata in Variant) */
  data?: Record<string, unknown> | undefined
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
    predicate?: string | undefined
    toNs?: string | undefined
    toId?: string | undefined
    beforeState?: StoredEntity | StoredRelationship | null | undefined
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
// Backpressure Configuration
// =============================================================================

/**
 * Configuration for backpressure handling in WAL managers.
 *
 * Backpressure prevents unbounded memory growth under sustained write load by:
 * - Pausing new writes when buffer exceeds configurable thresholds
 * - Tracking pending flush promises
 * - Providing feedback to callers via Promises
 * - Releasing backpressure when buffer drops below release threshold
 */
export interface BackpressureConfig {
  /**
   * Maximum buffer size in bytes before backpressure is applied.
   * @default 1048576 (1MB)
   */
  maxBufferSizeBytes: number

  /**
   * Maximum number of events in buffer before backpressure is applied.
   * @default 1000
   */
  maxBufferEventCount: number

  /**
   * Maximum number of pending flushes before backpressure is applied.
   * @default 10
   */
  maxPendingFlushes: number

  /**
   * Threshold (0-1) at which backpressure is released.
   * Backpressure is released when buffer drops below this percentage of max.
   * @default 0.5 (50%)
   */
  releaseThreshold: number

  /**
   * Timeout in milliseconds for waiting on backpressure.
   * If exceeded, throws BackpressureTimeoutError.
   * @default 30000 (30 seconds)
   */
  timeoutMs: number
}

/**
 * Default backpressure configuration values.
 */
export const DEFAULT_BACKPRESSURE_CONFIG: BackpressureConfig = {
  maxBufferSizeBytes: DEFAULT_BACKPRESSURE_MAX_BUFFER_BYTES,
  maxBufferEventCount: DEFAULT_BACKPRESSURE_MAX_EVENTS,
  maxPendingFlushes: DEFAULT_BACKPRESSURE_MAX_PENDING_FLUSHES,
  releaseThreshold: DEFAULT_BACKPRESSURE_RELEASE_THRESHOLD,
  timeoutMs: DEFAULT_BACKPRESSURE_TIMEOUT_MS,
}

/**
 * Current state of backpressure in a WAL manager.
 */
export interface BackpressureState {
  /** Whether backpressure is currently active */
  active: boolean

  /** Current buffer size in bytes */
  currentBufferSizeBytes: number

  /** Current number of events in buffer */
  currentEventCount: number

  /** Number of pending flush operations */
  pendingFlushCount: number

  /** Total number of backpressure events since last reset */
  backpressureEvents: number

  /** Total time spent waiting on backpressure in milliseconds */
  totalWaitTimeMs: number

  /** Timestamp of last backpressure activation, or null if never */
  lastBackpressureAt: number | null
}

/**
 * Error thrown when backpressure timeout is exceeded.
 */
export class BackpressureTimeoutError extends Error {
  name = 'BackpressureTimeoutError'

  constructor(
    public readonly timeoutMs: number,
    public readonly state: BackpressureState
  ) {
    super(
      `Backpressure timeout exceeded (${timeoutMs}ms). ` +
        `Buffer: ${state.currentBufferSizeBytes} bytes, ` +
        `${state.currentEventCount} events, ` +
        `${state.pendingFlushCount} pending flushes`
    )
  }
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
