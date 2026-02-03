/**
 * Type definitions for Payload CMS database adapter
 *
 * These types bridge Payload CMS's database interface with ParqueDB's storage layer.
 */

import type { StorageBackend, Entity, EntityId } from '../../types'

// =============================================================================
// Payload CMS Types (minimal subset we need)
// =============================================================================

/**
 * Payload operator types
 * @see https://payloadcms.com/docs/queries/overview#operators
 */
export type PayloadOperator =
  | 'equals'
  | 'not_equals'
  | 'greater_than'
  | 'greater_than_equal'
  | 'less_than'
  | 'less_than_equal'
  | 'in'
  | 'not_in'
  | 'all'
  | 'exists'
  | 'contains'
  | 'like'
  | 'not_like'
  | 'within'
  | 'intersects'
  | 'near'

/**
 * Payload field condition
 */
export interface PayloadWhereField {
  equals?: unknown | undefined
  not_equals?: unknown | undefined
  greater_than?: unknown | undefined
  greater_than_equal?: unknown | undefined
  less_than?: unknown | undefined
  less_than_equal?: unknown | undefined
  in?: unknown[] | undefined
  not_in?: unknown[] | undefined
  all?: unknown[] | undefined
  exists?: boolean | undefined
  contains?: string | undefined
  like?: string | undefined
  not_like?: string | undefined
  within?: unknown | undefined
  intersects?: unknown | undefined
  near?: unknown | undefined
}

/**
 * Payload where clause
 */
export interface PayloadWhere {
  [field: string]: PayloadWhere[] | PayloadWhereField | undefined
  and?: PayloadWhere[] | undefined
  or?: PayloadWhere[] | undefined
}

/**
 * Payload sort specification
 */
export type PayloadSort = string | string[]

/**
 * Payload pagination info
 */
export interface PayloadPaginationInfo {
  hasNextPage: boolean
  hasPrevPage: boolean
  limit: number
  nextPage: number | null
  page: number
  pagingCounter: number
  prevPage: number | null
  totalDocs: number
  totalPages: number
}

/**
 * Payload paginated result
 */
export interface PayloadPaginatedDocs<T = Record<string, unknown>> extends PayloadPaginationInfo {
  docs: T[]
}

/**
 * Payload collection slug (namespace)
 */
export type CollectionSlug = string

/**
 * Payload global slug
 */
export type GlobalSlug = string

// =============================================================================
// Adapter Configuration
// =============================================================================

/**
 * Configuration options for the ParqueDB Payload adapter
 */
export interface PayloadAdapterConfig {
  /**
   * Storage backend for data persistence
   * Can be MemoryBackend, FsBackend, R2Backend, etc.
   */
  storage: StorageBackend

  /**
   * Collection name for storing Payload migrations
   * @default 'payload_migrations'
   */
  migrationCollection?: string | undefined

  /**
   * Collection name for storing global documents
   * @default 'payload_globals'
   */
  globalsCollection?: string | undefined

  /**
   * Collection name suffix for version storage
   * @default '_versions'
   */
  versionsSuffix?: string | undefined

  /**
   * Default actor for audit fields when not provided
   * @default 'system/payload'
   */
  defaultActor?: EntityId | undefined

  /**
   * Enable debug logging
   * @default false
   */
  debug?: boolean | undefined
}

/**
 * Resolved adapter configuration with defaults applied
 */
export interface ResolvedAdapterConfig {
  storage: StorageBackend
  migrationCollection: string
  globalsCollection: string
  versionsSuffix: string
  defaultActor: EntityId
  debug: boolean
}

// =============================================================================
// Transaction Types
// =============================================================================

/**
 * Transaction session tracked by the adapter
 */
export interface TransactionSession {
  id: string
  startedAt: Date
  operations: TransactionOperation[]
}

/**
 * Operation recorded in a transaction
 */
export interface TransactionOperation {
  type: 'create' | 'update' | 'delete'
  collection: string
  id?: string | undefined
  data?: Record<string, unknown> | undefined
  where?: PayloadWhere | undefined
}

// =============================================================================
// Migration Types
// =============================================================================

/**
 * Migration record stored in _payload_migrations
 */
export interface MigrationRecord {
  name: string
  batch: number
  createdAt: Date
}

// =============================================================================
// Version Types
// =============================================================================

/**
 * Version record structure
 */
export interface VersionRecord<T = Record<string, unknown>> {
  id: string
  parent: string
  version: T
  createdAt: Date
  updatedAt: Date
  latest?: boolean | undefined
  autosave?: boolean | undefined
  publishedLocale?: string | undefined
  snapshot?: boolean | undefined
}

// =============================================================================
// Document Transform Types
// =============================================================================

/**
 * Options for transforming Payload documents to ParqueDB entities
 */
export interface ToParqueDBOptions {
  /** Collection/namespace name */
  collection: string
  /** Actor for audit fields */
  actor?: EntityId | undefined
  /** Whether this is an update (vs create) */
  isUpdate?: boolean | undefined
  /** Existing entity (for updates) */
  existingEntity?: Entity | undefined
}

/**
 * Options for transforming ParqueDB entities to Payload documents
 */
export interface ToPayloadOptions {
  /** Collection/namespace name */
  collection: string
  /** Fields to select (projection) */
  select?: Record<string, boolean> | undefined
  /** Fields to populate with related data */
  populate?: Record<string, boolean> | undefined
}

// =============================================================================
// Operation Argument Types (Payload-style)
// =============================================================================

/**
 * Base operation arguments
 */
export interface BaseOperationArgs {
  collection: CollectionSlug
  req?: {
    transactionID?: string | number | undefined
    locale?: string | undefined
    fallbackLocale?: string | undefined
    user?: Record<string, unknown> | undefined
  } | undefined
}

/**
 * Arguments for create operations
 */
export interface CreateArgs extends BaseOperationArgs {
  data: Record<string, unknown>
  draft?: boolean | undefined
}

/**
 * Arguments for find operations
 */
export interface FindArgs extends BaseOperationArgs {
  where?: PayloadWhere | undefined
  sort?: PayloadSort | undefined
  limit?: number | undefined
  page?: number | undefined
  pagination?: boolean | undefined
  draft?: boolean | undefined
  locale?: string | 'all' | undefined
  fallbackLocale?: string | undefined
  depth?: number | undefined
  select?: Record<string, boolean> | undefined
  populate?: Record<string, boolean> | undefined
}

/**
 * Arguments for findOne operations
 */
export interface FindOneArgs extends BaseOperationArgs {
  where: PayloadWhere
  draft?: boolean | undefined
  locale?: string | undefined
  fallbackLocale?: string | undefined
  depth?: number | undefined
  select?: Record<string, boolean> | undefined
  populate?: Record<string, boolean> | undefined
}

/**
 * Arguments for update operations
 */
export interface UpdateOneArgs extends BaseOperationArgs {
  id: string | number
  data: Record<string, unknown>
  draft?: boolean | undefined
  locale?: string | undefined
  autosave?: boolean | undefined
  select?: Record<string, boolean> | undefined
  populate?: Record<string, boolean> | undefined
}

/**
 * Arguments for updateMany operations
 */
export interface UpdateManyArgs extends BaseOperationArgs {
  where: PayloadWhere
  data: Record<string, unknown>
  draft?: boolean | undefined
  locale?: string | undefined
}

/**
 * Arguments for delete operations
 */
export interface DeleteOneArgs extends BaseOperationArgs {
  id: string | number
  where?: PayloadWhere | undefined
}

/**
 * Arguments for deleteMany operations
 */
export interface DeleteManyArgs extends BaseOperationArgs {
  where: PayloadWhere
}

/**
 * Arguments for count operations
 */
export interface CountArgs extends BaseOperationArgs {
  where?: PayloadWhere | undefined
  locale?: string | undefined
}

/**
 * Arguments for distinct value operations
 */
export interface DistinctArgs extends BaseOperationArgs {
  field: string
  where?: PayloadWhere | undefined
}

// =============================================================================
// Version Operation Arguments
// =============================================================================

/**
 * Arguments for creating a version
 */
export interface CreateVersionArgs extends BaseOperationArgs {
  parent: string | number
  versionData: Record<string, unknown>
  autosave?: boolean | undefined
  createdAt?: string | undefined
  publishedLocale?: string | undefined
  snapshot?: boolean | undefined
  updatedAt?: string | undefined
}

/**
 * Arguments for finding versions
 */
export interface FindVersionsArgs extends BaseOperationArgs {
  where?: PayloadWhere | undefined
  sort?: PayloadSort | undefined
  limit?: number | undefined
  page?: number | undefined
  pagination?: boolean | undefined
  locale?: string | undefined
  skip?: number | undefined
}

/**
 * Arguments for deleting versions
 */
export interface DeleteVersionsArgs extends BaseOperationArgs {
  where: PayloadWhere
}

// =============================================================================
// Global Operation Arguments
// =============================================================================

/**
 * Base global operation arguments
 */
export interface BaseGlobalArgs {
  slug: GlobalSlug
  req?: {
    transactionID?: string | number | undefined
    locale?: string | undefined
    fallbackLocale?: string | undefined
    user?: Record<string, unknown> | undefined
  } | undefined
}

/**
 * Arguments for finding a global
 */
export interface FindGlobalArgs extends BaseGlobalArgs {
  locale?: string | undefined
  fallbackLocale?: string | undefined
  draft?: boolean | undefined
  depth?: number | undefined
  select?: Record<string, boolean> | undefined
  populate?: Record<string, boolean> | undefined
}

/**
 * Arguments for creating/updating a global
 */
export interface CreateGlobalArgs extends BaseGlobalArgs {
  data: Record<string, unknown>
  draft?: boolean | undefined
}

/**
 * Arguments for global version operations
 */
export interface CreateGlobalVersionArgs extends BaseGlobalArgs {
  parent: string | number
  versionData: Record<string, unknown>
  autosave?: boolean | undefined
  createdAt?: string | undefined
  publishedLocale?: string | undefined
  snapshot?: boolean | undefined
  updatedAt?: string | undefined
}

export interface FindGlobalVersionsArgs extends BaseGlobalArgs {
  where?: PayloadWhere | undefined
  sort?: PayloadSort | undefined
  limit?: number | undefined
  page?: number | undefined
  pagination?: boolean | undefined
  locale?: string | undefined
  skip?: number | undefined
}

// =============================================================================
// Query Drafts Arguments
// =============================================================================

/**
 * Arguments for querying drafts
 */
export interface QueryDraftsArgs extends BaseOperationArgs {
  where?: PayloadWhere | undefined
  sort?: PayloadSort | undefined
  limit?: number | undefined
  page?: number | undefined
  pagination?: boolean | undefined
  locale?: string | undefined
}

// =============================================================================
// Upsert Arguments
// =============================================================================

/**
 * Arguments for upsert operations
 */
export interface UpsertArgs extends BaseOperationArgs {
  data: Record<string, unknown>
  where: PayloadWhere
  draft?: boolean | undefined
  locale?: string | undefined
  select?: Record<string, boolean> | undefined
  populate?: Record<string, boolean> | undefined
}

// =============================================================================
// Result Types
// =============================================================================

/**
 * Result of a delete operation
 */
export interface DeleteResult {
  docs: Array<{ id: string | number }>
  errors: Array<{ id: string | number; message: string }>
}

/**
 * Result of an update many operation
 */
export interface UpdateManyResult {
  docs: Array<{ id: string | number }>
  errors: Array<{ id: string | number; message: string }>
}
