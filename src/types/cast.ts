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
import type { StorageBackend, ReadonlyStorageBackend } from './storage'
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
 * Cast read-only storage adapter to ReadonlyStorageBackend.
 * Use for CDN adapters and other read-only storage implementations.
 *
 * @remarks Safe when the adapter correctly implements all ReadonlyStorageBackend methods.
 */
export function asReadonlyStorageBackend<T>(adapter: T): ReadonlyStorageBackend {
  return adapter as unknown as ReadonlyStorageBackend
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
 * @remarks Validates structure before returning typed IndexCatalog.
 */
export interface IndexCatalogDefinition {
  name: string
  type: string
  fields: Array<{ path: string }>
}

export interface IndexCatalogEntry {
  definition: IndexCatalogDefinition
  metadata: Record<string, unknown>
}

export interface IndexCatalog {
  version: number
  indexes: Record<string, IndexCatalogEntry[]>
}

/**
 * Validate and cast index catalog from parsed JSON.
 * Throws descriptive errors for invalid input.
 *
 * @param value - Parsed JSON object
 * @returns Validated IndexCatalog
 * @throws Error if validation fails
 */
export function asIndexCatalog(value: Record<string, unknown>): IndexCatalog {
  // Validate version
  if (value.version === undefined || value.version === null) {
    throw new Error("'version' must be a number")
  }
  if (typeof value.version !== 'number') {
    throw new Error(`'version' must be a number, got ${typeof value.version}`)
  }

  // Validate indexes exists
  if (value.indexes === undefined || value.indexes === null) {
    throw new Error("'indexes' is required")
  }

  // Validate indexes is an object (not array)
  if (Array.isArray(value.indexes)) {
    throw new Error("'indexes' must be a record object, got array")
  }
  if (typeof value.indexes !== 'object') {
    throw new Error(`'indexes' must be a record object, got ${typeof value.indexes}`)
  }

  const indexes = value.indexes as Record<string, unknown>

  // Validate each namespace entry
  for (const [ns, entries] of Object.entries(indexes)) {
    if (!Array.isArray(entries)) {
      throw new Error(`indexes['${ns}'] must be an array`)
    }

    // Validate each entry in the namespace
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i]

      // Entry must be an object
      if (entry === null || typeof entry !== 'object') {
        throw new Error(`indexes['${ns}'][${i}] must be an object`)
      }

      const entryRecord = entry as Record<string, unknown>

      // Validate definition
      if (entryRecord.definition === null || typeof entryRecord.definition !== 'object') {
        throw new Error(`indexes['${ns}'][${i}].definition must be an object`)
      }

      const definition = entryRecord.definition as Record<string, unknown>

      // Validate definition.name
      if (typeof definition.name !== 'string') {
        throw new Error(`indexes['${ns}'][${i}].definition.name must be a string`)
      }

      // Validate definition.type
      if (typeof definition.type !== 'string') {
        throw new Error(`indexes['${ns}'][${i}].definition.type must be a string`)
      }

      // Validate definition.fields
      if (!Array.isArray(definition.fields)) {
        throw new Error(`indexes['${ns}'][${i}].definition.fields must be an array`)
      }

      // Validate metadata
      if (entryRecord.metadata === null || typeof entryRecord.metadata !== 'object') {
        throw new Error(`indexes['${ns}'][${i}].metadata must be an object`)
      }
    }
  }

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

/**
 * Cast a value to unknown[] for array operations.
 * Use when accessing array fields from Record<string, unknown> objects.
 *
 * @remarks Safe when the value is known to be an array at runtime.
 * Returns empty array if value is undefined/null for safer handling.
 */
export function toUnknownArray(value: unknown): unknown[] {
  if (Array.isArray(value)) return value
  return []
}

/**
 * Cast a typed array to unknown[] for storage in untyped caches.
 * Use when storing typed arrays in Map<string, unknown[]> caches.
 *
 * @remarks Safe because the array is retrieved with proper type casting.
 * This bridges typed arrays to untyped cache storage intentionally.
 */
export function asCacheableArray<T>(arr: T[]): unknown[] {
  return arr as unknown[]
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
  databaseContext?: unknown | undefined
  cookieDatabaseId?: string | undefined
  actor?: string | undefined
  user?: { id: string } | undefined
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
// Entity Mutation Casts
// =============================================================================

/**
 * Mutable entity type for update operations.
 * Use when performing field mutations on entities.
 *
 * This type represents an Entity that can be accessed and modified
 * via string keys, which is necessary for update operators like $set, $unset, etc.
 *
 * @remarks Safe because Entity<T> extends Record<string, unknown> via index signature.
 */
export type MutableEntity = Entity & Record<string, unknown>

/**
 * Cast an entity to a mutable record for field access/mutation.
 * Use in update operators and relationship handlers.
 *
 * @remarks Safe because Entity has an index signature allowing string access.
 */
export function asMutableEntity(entity: Entity): MutableEntity {
  return entity as MutableEntity
}

/**
 * Cast a Variant (event state) to Entity.
 * Use when restoring entity state from events.
 *
 * @remarks Safe when the Variant was originally captured from an Entity.
 * Events store entity state in before/after fields as Variant type.
 */
export function variantAsEntity(variant: import('./entity').Variant): Entity {
  return variant as unknown as Entity
}

/**
 * Cast a Variant to Entity or null with proper type narrowing.
 * Use for event before/after fields that may be undefined.
 *
 * @remarks Safe when the Variant was originally captured from an Entity.
 */
export function variantAsEntityOrNull(variant: import('./entity').Variant | undefined): Entity | null {
  if (variant === undefined) return null
  return variant as unknown as Entity
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

// =============================================================================
// Collection Query Result Casts
// =============================================================================

/**
 * Cast collection find() result items to typed array.
 * Use when querying collections where the return type is known.
 *
 * @remarks Safe when the collection schema matches the expected type.
 * Commonly used with observability MVs (AIRequestRecord, LogEntry, etc.)
 */
export function asTypedResults<T>(items: unknown): T[] {
  return items as T[]
}

/**
 * Cast collection findOne() result to typed record.
 * Use when querying a single record where the return type is known.
 *
 * @remarks Safe when the collection schema matches the expected type.
 */
export function asTypedResult<T>(item: unknown): T {
  return item as T
}

/**
 * Cast collection create() result to typed record.
 * Use when creating records where the return type is known.
 *
 * @remarks Safe when the collection schema matches the expected type.
 */
export function asCreatedRecord<T>(record: unknown): T {
  return record as T
}

// =============================================================================
// Type Guards for AI Database Adapter
// =============================================================================

/**
 * Check if a value is a plain record object (not null, not array)
 */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

/**
 * Check if a value is a valid AIDB Event entity
 * Event entities have optional: actor, event, object, timestamp, objectData
 */
export function isAIDBEventEntity(value: unknown): boolean {
  if (!isRecord(value)) return false
  // Validate actor if present
  if (value.actor !== undefined && typeof value.actor !== 'string') return false
  // Validate event if present
  if (value.event !== undefined && typeof value.event !== 'string') return false
  // Validate object if present (can be string or object)
  if (value.object !== undefined && typeof value.object !== 'string' && !isRecord(value.object)) return false
  // Validate timestamp if present (can be Date or string)
  if (value.timestamp !== undefined && !(value.timestamp instanceof Date) && typeof value.timestamp !== 'string') return false
  // Validate objectData if present (must be object)
  if (value.objectData !== undefined && !isRecord(value.objectData)) return false
  return true
}

/**
 * Check if a value is a valid AIDB Action entity
 * Action entities have optional: status, progress, total, result
 */
export function isAIDBActionEntity(value: unknown): boolean {
  if (!isRecord(value)) return false
  // Validate status if present
  const validStatuses = ['pending', 'active', 'completed', 'failed', 'cancelled']
  if (value.status !== undefined && (typeof value.status !== 'string' || !validStatuses.includes(value.status))) return false
  // Validate progress if present
  if (value.progress !== undefined && typeof value.progress !== 'number') return false
  // Validate total if present
  if (value.total !== undefined && typeof value.total !== 'number') return false
  // Validate result if present (must be object or null)
  if (value.result !== undefined && value.result !== null && !isRecord(value.result)) return false
  return true
}

/**
 * Check if a value is a valid AIDB Artifact entity
 * Artifact entities have optional: url, sourceHash, metadata, content
 */
export function isAIDBArtifactEntity(value: unknown): boolean {
  if (!isRecord(value)) return false
  // Validate url if present
  if (value.url !== undefined && typeof value.url !== 'string') return false
  // Validate sourceHash if present
  if (value.sourceHash !== undefined && typeof value.sourceHash !== 'string') return false
  // Validate metadata if present (must be object)
  if (value.metadata !== undefined && !isRecord(value.metadata)) return false
  // content can be any type
  return true
}

/**
 * Check if an object has a relationship field (non-null object field)
 */
export function hasRelationshipField(obj: unknown, field: string): boolean {
  if (!isRecord(obj)) return false
  const fieldValue = obj[field]
  return isRecord(fieldValue)
}

/**
 * Get a relationship field value
 */
export function getRelationshipField(obj: unknown, field: string): Record<string, unknown> | undefined {
  if (!isRecord(obj)) return undefined
  const fieldValue = obj[field]
  if (!isRecord(fieldValue)) return undefined
  return fieldValue
}

/**
 * Get a string field from an object
 */
export function getStringField(obj: unknown, field: string): string | undefined {
  if (!isRecord(obj)) return undefined
  const value = obj[field]
  return typeof value === 'string' ? value : undefined
}

/**
 * Get a number field from an object
 */
export function getNumberField(obj: unknown, field: string): number | undefined {
  if (!isRecord(obj)) return undefined
  const value = obj[field]
  return typeof value === 'number' ? value : undefined
}

/**
 * Get a Date field from an object (parses ISO strings)
 */
export function getDateField(obj: unknown, field: string): Date | undefined {
  if (!isRecord(obj)) return undefined
  const value = obj[field]
  if (value instanceof Date) return value
  if (typeof value === 'string') {
    const parsed = new Date(value)
    return isNaN(parsed.getTime()) ? undefined : parsed
  }
  return undefined
}

/**
 * Get a record field from an object
 */
export function getRecordField(obj: unknown, field: string): Record<string, unknown> | undefined {
  if (!isRecord(obj)) return undefined
  const value = obj[field]
  if (!isRecord(value)) return undefined
  return value
}

/**
 * Get a boolean field from an object
 */
export function getBooleanField(obj: unknown, field: string): boolean | undefined {
  if (!isRecord(obj)) return undefined
  const value = obj[field]
  return typeof value === 'boolean' ? value : undefined
}

/**
 * Get an array field from an object
 */
export function getArrayField(obj: unknown, field: string): unknown[] | undefined {
  if (!isRecord(obj)) return undefined
  const value = obj[field]
  return Array.isArray(value) ? value : undefined
}

/**
 * Get a string array field from an object
 */
export function getStringArrayField(obj: unknown, field: string): string[] | undefined {
  if (!isRecord(obj)) return undefined
  const value = obj[field]
  if (!Array.isArray(value)) return undefined
  if (!value.every((v) => typeof v === 'string')) return undefined
  return value as string[]
}

/**
 * Assert that a value is a valid AIDB Event entity
 * @throws Error if validation fails
 * @returns The validated value
 */
export function assertAIDBEventEntity(value: unknown, context?: string): Record<string, unknown> {
  if (!isAIDBEventEntity(value)) {
    throw new Error(`Invalid AI Database Event entity${context ? ` (${context})` : ''}`)
  }
  return value as Record<string, unknown>
}

/**
 * Assert that a value is a valid AIDB Action entity
 * @throws Error if validation fails
 * @returns The validated value
 */
export function assertAIDBActionEntity(value: unknown, context?: string): Record<string, unknown> {
  if (!isAIDBActionEntity(value)) {
    throw new Error(`Invalid AI Database Action entity${context ? ` (${context})` : ''}`)
  }
  return value as Record<string, unknown>
}

/**
 * Assert that a value is a valid AIDB Artifact entity
 * @throws Error if validation fails
 * @returns The validated value
 */
export function assertAIDBArtifactEntity(value: unknown, context?: string): Record<string, unknown> {
  if (!isAIDBArtifactEntity(value)) {
    throw new Error(`Invalid AI Database Artifact entity${context ? ` (${context})` : ''}`)
  }
  return value as Record<string, unknown>
}

// =============================================================================
// Entity Name Extraction
// =============================================================================

/**
 * Check if an object has a name field
 */
export function hasNameField(obj: unknown): boolean {
  return isRecord(obj) && typeof obj.name === 'string'
}

/**
 * Check if an object has a title field
 */
export function hasTitleField(obj: unknown): boolean {
  return isRecord(obj) && typeof obj.title === 'string'
}

/**
 * Get the entity name from an object (checks name, then title)
 * Returns undefined if name is empty string (falls through to title)
 */
export function getEntityName(obj: unknown): string | undefined {
  if (!isRecord(obj)) return undefined
  if (typeof obj.name === 'string' && obj.name !== '') return obj.name
  if (typeof obj.title === 'string' && obj.title !== '') return obj.title
  return undefined
}

/**
 * Get the entity name with a default fallback
 * Returns default for empty strings
 */
export function getEntityNameOrDefault(obj: unknown, defaultName: string): string {
  return getEntityName(obj) ?? defaultName
}

// =============================================================================
// Entity Type Field Guards
// =============================================================================

/**
 * Check if an object has a type field
 */
export function hasTypeField(obj: unknown): boolean {
  return isRecord(obj) && typeof obj.$type === 'string'
}

/**
 * Get the type field from an object
 */
export function getTypeField(obj: unknown): string | undefined {
  if (!isRecord(obj)) return undefined
  return typeof obj.$type === 'string' ? obj.$type : undefined
}

/**
 * Get the type field with a default fallback
 */
export function getTypeFieldOrDefault(obj: unknown, defaultType: string): string {
  return getTypeField(obj) ?? defaultType
}

// =============================================================================
// Entity ID Field Guards
// =============================================================================

/**
 * Check if an object has a $id field (only $id, not id)
 */
export function hasIdField(obj: unknown): boolean {
  if (!isRecord(obj)) return false
  return typeof obj.$id === 'string'
}

/**
 * Get the $id field from an object (only $id, not id)
 */
export function getIdField(obj: unknown): string | undefined {
  if (!isRecord(obj)) return undefined
  if (typeof obj.$id === 'string') return obj.$id
  return undefined
}

// =============================================================================
// Safe Record Casts
// =============================================================================

/**
 * Safely cast a value to Record<string, unknown>
 * Returns undefined if value is not a record
 */
export function safeAsRecord(value: unknown): Record<string, unknown> | undefined {
  return isRecord(value) ? value : undefined
}

/**
 * Cast a value to Record<string, unknown> or return empty object
 */
export function asRecordOrEmpty(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {}
}
