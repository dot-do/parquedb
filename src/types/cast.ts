/**
 * Type Cast Utilities
 *
 * Provides type-safe casting functions to eliminate `as unknown as T` double assertions.
 * These functions provide proper type bridges for common casting scenarios.
 *
 * IMPORTANT: These are intentional escape hatches for scenarios where TypeScript's
 * type system cannot express the actual runtime relationship between types.
 * Each function documents why the cast is safe.
 */

import type { Entity } from './entity'
import type { StorageBackend } from './storage'
import type { ParqueDBDOStub } from './worker'

// =============================================================================
// Generic Cast Utilities
// =============================================================================

/**
 * Cast a record to a specific type.
 * Use when JSON parsing returns unknown or Record<string, unknown>.
 *
 * @remarks Safe because JSON.parse returns unknown, but we know the schema.
 */
export function asTypedRecord<T>(value: Record<string, unknown>): T {
  return value as T
}

/**
 * Cast an entity to a Record<string, unknown> for iteration.
 * Use when need to iterate over entity fields.
 *
 * @remarks Safe because Entity extends Record<string, unknown>.
 */
export function entityAsRecord<T = unknown>(entity: Entity<T>): Record<string, unknown> {
  return entity as unknown as Record<string, unknown>
}

/**
 * Cast a generic source entity to a different entity type.
 * Use in relationship traversal where source type differs from result type.
 *
 * @remarks Safe when caller knows the runtime type matches.
 */
export function castEntity<R>(entity: Record<string, unknown>): Entity<R> {
  return entity as unknown as Entity<R>
}

// =============================================================================
// Proxy and Dynamic Access
// =============================================================================

/**
 * Cast proxy target for dynamic property access.
 * Use in Proxy handlers when accessing properties on the target object.
 *
 * @remarks Safe because Proxy allows dynamic property access by design.
 */
export function proxyTarget<T>(target: object): Record<string, T> {
  return target as unknown as Record<string, T>
}

/**
 * Cast for lazy proxy return types.
 * Use when creating proxy-based lazy initialization patterns.
 *
 * @remarks Safe when the proxy correctly implements the interface.
 */
export function asProxyResult<T>(proxy: object): T {
  return proxy as unknown as T
}

// =============================================================================
// RPC Promise Chain
// =============================================================================

/**
 * Cast RPC promise for method chaining (map, filter).
 * Use when RPC chain methods transform types.
 *
 * @remarks Safe because RPC chain preserves type relationships.
 */
export function chainRpcPromise<T>(promise: unknown): T {
  return promise as unknown as T
}

// =============================================================================
// Cloudflare Workers Types
// =============================================================================

/**
 * Cast Durable Object stub to typed interface.
 * Use when getting DO stub from namespace.get().
 *
 * @remarks Safe because the DO implements ParqueDBDOStub interface.
 */
export function asDOStub(stub: DurableObjectStub): ParqueDBDOStub {
  return stub as unknown as ParqueDBDOStub
}

/**
 * Cast R2 bucket for index storage adapter.
 * Use when creating index storage adapters from R2 bucket.
 *
 * @remarks Safe because R2Bucket has the required get/head methods.
 */
export function asIndexStorageBucket(bucket: R2Bucket): {
  get(key: string): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>
  head(key: string): Promise<{ size: number } | null>
} {
  return bucket as unknown as {
    get(key: string): Promise<{ arrayBuffer(): Promise<ArrayBuffer> } | null>
    head(key: string): Promise<{ size: number } | null>
  }
}

/**
 * Cast R2 bucket to globalThis.R2Bucket.
 * Use when passing bucket to functions expecting global R2Bucket type.
 *
 * @remarks Safe because the types are identical, just namespaced differently.
 */
export function asGlobalR2Bucket(bucket: R2Bucket): globalThis.R2Bucket {
  return bucket as unknown as globalThis.R2Bucket
}

/**
 * Cast data to BodyInit for Response constructor.
 * Use when creating Response from Uint8Array.
 *
 * @remarks Safe because Uint8Array is a valid BodyInit type.
 */
export function asBodyInit(data: Uint8Array): BodyInit {
  return data as unknown as BodyInit
}

// =============================================================================
// Storage Adapter Casts
// =============================================================================

/**
 * Cast storage adapter to StorageBackend.
 * Use when adapters implement StorageBackend interface but TS doesn't recognize it.
 *
 * @remarks Safe when the adapter correctly implements all StorageBackend methods.
 */
export function asStorageBackend<T>(adapter: T): StorageBackend {
  return adapter as unknown as StorageBackend
}

/**
 * Cast memory storage for FTS index.
 * Use when creating FTS index with in-memory storage adapter.
 *
 * @remarks Safe because the memory adapter implements the required interface.
 */
export function asFTSStorageBackend(adapter: {
  read(path: string): Promise<Uint8Array>
  write(path: string, data: Uint8Array): Promise<void>
  exists(path: string): Promise<boolean>
}): import('./storage').StorageBackend {
  return adapter as unknown as import('./storage').StorageBackend
}

// =============================================================================
// Config and JSON Parsing
// =============================================================================

/**
 * Cast parsed JSON to config type.
 * Use after validating JSON structure with isRecord().
 *
 * @remarks Safe when validation confirms the structure matches expected type.
 */
export function asConfig<T>(value: Record<string, unknown>): T {
  return value as unknown as T
}

/**
 * Cast index catalog from parsed JSON.
 * Use when loading index catalog from storage.
 *
 * @remarks Safe after version validation and structure checks.
 */
export interface IndexCatalog {
  version: number
  indexes: Array<{
    name: string
    type: string
    field: string
    path: string
  }>
}

export function asIndexCatalog(value: Record<string, unknown>): IndexCatalog {
  return value as unknown as IndexCatalog
}

// =============================================================================
// Query Result Casts
// =============================================================================

/**
 * Cast row data to entity type.
 * Use when reading rows from Parquet files.
 *
 * @remarks Safe because Parquet schema matches entity structure.
 */
export function rowAsEntity<T>(row: Record<string, unknown>): T {
  return row as unknown as T
}

/**
 * Cast stats object to Record for serialization.
 * Use when including stats in API responses.
 *
 * @remarks Safe because stats objects are plain records.
 */
export function statsAsRecord(stats: unknown): Record<string, unknown> {
  return stats as unknown as Record<string, unknown>
}

// =============================================================================
// SQL Integration Casts
// =============================================================================

/**
 * Cast SQL result to typed record.
 * Use when returning results from SQL operations.
 *
 * @remarks Safe when query schema matches the expected type.
 */
export function sqlResultAs<T>(result: unknown): T {
  return result as unknown as T
}

/**
 * Cast items array to typed array.
 * Use when projecting columns in SQL SELECT.
 *
 * @remarks Safe after column projection matches schema.
 */
export function sqlItemsAs<T>(items: Record<string, unknown>[]): T[] {
  return items as unknown as T[]
}

// =============================================================================
// Array Operations
// =============================================================================

/**
 * Cast array after mutation (setField for arrays).
 * Use in immutable update helpers when modifying array elements.
 *
 * @remarks Safe because the array type is preserved through mutation.
 */
export function arrayAs<T>(arr: unknown[]): T {
  return arr as unknown as T
}

// =============================================================================
// Route Matching
// =============================================================================

/**
 * Cast regex match groups to typed tuple.
 * Use when extracting route parameters.
 *
 * @remarks Safe when the regex pattern guarantees the capture groups.
 */
export function matchGroupsAs<T extends string[]>(groups: string[]): T {
  return groups as unknown as T
}

// =============================================================================
// Hono Context Casts
// =============================================================================

/**
 * Context variables type for database routes.
 */
export interface DatabaseContextVariables {
  databaseContext?: unknown
  cookieDatabaseId?: string
  actor?: string
  user?: { id: string }
}

/**
 * Cast Hono context.var to typed variables.
 * Use when accessing middleware-injected variables.
 *
 * @remarks Safe when middleware correctly sets the variables.
 */
export function contextVars<T>(vars: unknown): T {
  return vars as unknown as T
}

// =============================================================================
// Event Recording Casts
// =============================================================================

/**
 * Cast relationship data for event recording.
 * Use when creating $link/$unlink events.
 *
 * @remarks Safe because event format accepts this structure.
 */
export function asRelEventPayload(data: { predicate: string; to: unknown }): Entity {
  return data as unknown as Entity
}

// =============================================================================
// Dynamic Module Casts
// =============================================================================

/**
 * Cast dynamically imported module.
 * Use for optional dependencies loaded at runtime.
 *
 * @remarks Safe when the module exports match the expected interface.
 */
export function asDynamicModule<T>(module: unknown): T {
  return module as unknown as T
}

/**
 * Cast builder with private build method.
 * Use for Iceberg snapshot builders that expose build() internally.
 *
 * @remarks Safe when the builder actually has the build method.
 */
export function asBuilder<T>(builder: unknown): { build(): T } {
  return builder as unknown as { build(): T }
}

// =============================================================================
// JWT Payload Casts
// =============================================================================

/**
 * Cast JWT payload to OAuth JWT payload type.
 * Use after JWT verification.
 *
 * @remarks Safe because jwtVerify returns the expected payload structure.
 */
export function asJWTPayload<T>(payload: unknown): T {
  return payload as unknown as T
}

// =============================================================================
// ParqueDB Instance Access
// =============================================================================

/**
 * Extract storage from ParqueDB instance.
 * Use when need direct storage access for Iceberg integration.
 *
 * @remarks Safe when the db instance has a storage property.
 */
export function getStorageFromDB(db: { collection: (ns: string) => unknown }): StorageBackend {
  return (db as unknown as { storage: StorageBackend }).storage
}

// =============================================================================
// Durable Object Casts
// =============================================================================

/**
 * Cast Durable Object namespace.get() result to typed DO interface.
 * Use for DatabaseIndexDO access.
 *
 * @remarks Safe because the DO implements the interface.
 */
export function asDatabaseIndexDO<T>(stub: DurableObjectStub): T {
  return stub as unknown as T
}

// =============================================================================
// Variant Type Casts
// =============================================================================

/**
 * Cast update operations to Variant type.
 * Use when storing update operations in event metadata.
 *
 * @remarks Safe because UpdateOps is structurally compatible with Variant.
 */
export function opsAsVariant<T>(ops: T): import('./entity').Variant {
  return ops as unknown as import('./entity').Variant
}
