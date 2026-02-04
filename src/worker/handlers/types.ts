/**
 * Shared types for route handlers
 *
 * Uses focused interfaces to reduce coupling - handlers only receive
 * the dependencies they actually need.
 */

import type { Filter } from '../../types/filter'
import type { FindOptions, FindResult, GetOptions, CreateOptions, UpdateOptions, DeleteOptions, UpdateResult, DeleteResult } from '../../types/options'
import type { EntityRecord } from '../../types/entity'
import type { Update } from '../../types/update'
import type { StorageStats } from '../../types/storage'

// =============================================================================
// Focused Worker Interfaces
// =============================================================================

/**
 * Read-only worker interface for query operations.
 * Used by handlers that only need to read data.
 */
export interface WorkerReadInterface {
  /** Find entities matching a filter */
  find<T = EntityRecord>(ns: string, filter?: Filter, options?: FindOptions<T>): Promise<FindResult<T>>
  /** Get a single entity by ID */
  get<T = EntityRecord>(ns: string, id: string, options?: GetOptions): Promise<T | null>
  /** Count entities matching a filter */
  count(ns: string, filter?: Filter): Promise<number>
  /** Check if any entities match a filter */
  exists(ns: string, filter: Filter): Promise<boolean>
  /** Get relationships for an entity */
  getRelationships(
    ns: string,
    entityId: string,
    predicate?: string | undefined,
    options?: { direction?: 'outbound' | 'inbound' | 'both' | undefined; limit?: number | undefined; offset?: number | undefined } | undefined
  ): Promise<Array<{
    predicate: string
    reverse?: string | undefined
    target: { $id: string; $type: string; name?: string | undefined }
    direction: 'outbound' | 'inbound'
  }>>
  /** Get storage statistics */
  getStorageStats(): StorageStats
}

/**
 * Write worker interface for mutation operations.
 * Extends read interface with mutation capabilities.
 */
export interface WorkerWriteInterface extends WorkerReadInterface {
  /** Create a new entity */
  create<T = EntityRecord>(ns: string, data: Partial<T>, options?: CreateOptions): Promise<T>
  /** Update an entity */
  update(ns: string, id: string, update: Update, options?: UpdateOptions): Promise<UpdateResult>
  /** Delete an entity */
  delete(ns: string, id: string, options?: DeleteOptions): Promise<DeleteResult>
}

/**
 * Full worker interface for handlers that need all capabilities.
 *
 * @deprecated Prefer using WorkerReadInterface or WorkerWriteInterface
 * to make handler dependencies explicit.
 */
export interface WorkerInterface extends WorkerWriteInterface {
  // All capabilities inherited from WorkerWriteInterface
}

// =============================================================================
// Focused Handler Context Interfaces
// =============================================================================

/**
 * Minimal request context for handlers.
 * Contains only HTTP request information.
 */
export interface RequestContext {
  /** The incoming HTTP request */
  request: Request
  /** Parsed URL */
  url: URL
  /** Request path */
  path: string
  /** Request start time for latency calculation */
  startTime: number
}

/**
 * Context for read-only handlers.
 * Extends request context with read-only worker access.
 */
export interface ReadHandlerContext extends RequestContext {
  /** Base URL for building links */
  baseUrl: string
  /** Read-only worker instance */
  worker: WorkerReadInterface
  /** Execution context for waitUntil */
  ctx: ExecutionContext
}

/**
 * Context for handlers that need write access.
 * Extends read context with write capabilities.
 */
export interface WriteHandlerContext extends RequestContext {
  /** Base URL for building links */
  baseUrl: string
  /** Worker instance with write access */
  worker: WorkerWriteInterface
  /** Execution context for waitUntil */
  ctx: ExecutionContext
}

/**
 * Full context passed to route handlers.
 * Provides access to all capabilities.
 *
 * @deprecated Prefer using ReadHandlerContext or WriteHandlerContext
 * to make handler dependencies explicit.
 */
export interface HandlerContext extends WriteHandlerContext {
  /** ParqueDB worker instance (full access) */
  worker: WorkerInterface
}
