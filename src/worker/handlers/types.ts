/**
 * Shared types for route handlers
 */

import type { Filter } from '../../types/filter'
import type { FindOptions, FindResult, GetOptions, CreateOptions, UpdateOptions, DeleteOptions, UpdateResult, DeleteResult } from '../../types/options'
import type { EntityRecord } from '../../types/entity'
import type { Update } from '../../types/update'
import type { StorageStats } from '../../types/storage'

/**
 * Interface for the worker instance used by handlers.
 * Defines the minimal interface needed by route handlers to avoid
 * circular dependency with the main worker module.
 */
export interface WorkerInterface {
  /** Find entities matching a filter */
  find<T = EntityRecord>(ns: string, filter?: Filter, options?: FindOptions<T>): Promise<FindResult<T>>
  /** Get a single entity by ID */
  get<T = EntityRecord>(ns: string, id: string, options?: GetOptions): Promise<T | null>
  /** Count entities matching a filter */
  count(ns: string, filter?: Filter): Promise<number>
  /** Check if any entities match a filter */
  exists(ns: string, filter: Filter): Promise<boolean>
  /** Create a new entity */
  create<T = EntityRecord>(ns: string, data: Partial<T>, options?: CreateOptions): Promise<T>
  /** Update an entity */
  update(ns: string, id: string, update: Update, options?: UpdateOptions): Promise<UpdateResult>
  /** Delete an entity */
  delete(ns: string, id: string, options?: DeleteOptions): Promise<DeleteResult>
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
 * Context passed to route handlers
 */
export interface HandlerContext {
  /** The incoming HTTP request */
  request: Request
  /** Parsed URL */
  url: URL
  /** Base URL for building links */
  baseUrl: string
  /** Request path */
  path: string
  /** ParqueDB worker instance */
  worker: WorkerInterface
  /** Request start time for latency calculation */
  startTime: number
  /** Execution context for waitUntil */
  ctx: ExecutionContext
}
