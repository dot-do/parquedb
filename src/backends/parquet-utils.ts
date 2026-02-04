/**
 * Shared Parquet utilities for entity backends
 *
 * Common utilities used by IcebergBackend, DeltaBackend, and other Parquet-based backends.
 * Provides entity serialization, filter matching, ID generation, and data field extraction.
 */

import type { Entity, EntityId, EntityData, CreateInput, Event } from '../types/entity'
import type { Filter } from '../types/filter'
import type { ParquetSchema } from '../parquet/types'
import { encodeVariant, decodeVariant } from '../parquet/variant'
import { createSafeRegex } from '../utils/safe-regex'

// =============================================================================
// Entity Serialization
// =============================================================================

/**
 * Options for entityToRow conversion
 */
export interface EntityToRowOptions {
  /**
   * Fields to extract into separate Parquet columns (shredding)
   * for predicate pushdown support
   */
  shredFields?: string[] | undefined
}

/**
 * Options for rowToEntity conversion
 */
export interface RowToEntityOptions {
  /**
   * Fields that were shredded into separate columns
   * These take precedence over values in $data
   */
  shredFields?: string[] | undefined
}

/**
 * Convert an entity to a Parquet row
 *
 * Extracts only core identity fields ($id, $type, name) into separate columns.
 * All other fields (including audit fields) are encoded in $data variant.
 *
 * If shredFields is provided, those fields will be extracted into separate
 * top-level columns for predicate pushdown support.
 *
 * @param entity - The entity to convert
 * @param options - Optional configuration for shredding
 * @returns A row object suitable for Parquet writing
 */
export function entityToRow<T>(entity: Entity<T>, options?: EntityToRowOptions): Record<string, unknown> {
  // Extract only identity fields - everything else goes in $data
  const {
    $id,
    $type,
    name: $name,
    ...allOtherFields
  } = entity as Entity<T> & { deletedAt?: Date | undefined; deletedBy?: string | undefined }

  // Convert Date objects to ISO strings for serialization
  const dataFields: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(allOtherFields)) {
    if (value === undefined) continue
    if (value instanceof Date) {
      dataFields[key] = value.toISOString()
    } else {
      dataFields[key] = value
    }
  }

  // Extract shredded fields into separate columns
  const shredFields = options?.shredFields ?? []
  const shreddedColumns: Record<string, unknown> = {}
  const remainingDataFields: Record<string, unknown> = {}

  for (const [key, value] of Object.entries(dataFields)) {
    if (shredFields.includes(key)) {
      // Extract to shredded column (preserve null values)
      shreddedColumns[key] = value
    } else {
      remainingDataFields[key] = value
    }
  }

  // Also check for null values explicitly set on shredded fields
  for (const field of shredFields) {
    if (field in dataFields && dataFields[field] === null) {
      shreddedColumns[field] = null
    }
  }

  // Encode remaining fields as Variant $data, then base64 encode to avoid binary issues
  const variantBytes = encodeVariant(remainingDataFields)
  const $data = bytesToBase64(variantBytes)

  return {
    $id,
    $type,
    $name,
    $data,
    ...shreddedColumns,
  }
}

/**
 * Convert a Parquet row back to an Entity
 *
 * Decodes the $data Variant column which contains all fields except $id, $type, name.
 * If shredFields is provided, those columns take precedence over $data values.
 *
 * @param row - The row from Parquet reader
 * @param options - Optional configuration for shredding
 * @returns Reconstructed Entity
 */
export function rowToEntity<T>(row: Record<string, unknown>, options?: RowToEntityOptions): Entity<T> {
  const {
    $id,
    $type,
    $name,
    $data,
    ...otherColumns
  } = row

  // Decode Variant $data (stored as base64-encoded string)
  let dataFields: Record<string, unknown> = {}
  if (typeof $data === 'string' && $data.length > 0) {
    // $data is base64 encoded - decode to bytes then decode variant
    const bytes = base64ToBytes($data)
    dataFields = decodeVariant(bytes) as Record<string, unknown>
  } else if ($data instanceof Uint8Array && $data.length > 0) {
    // Direct Uint8Array (unlikely with hyparquet)
    dataFields = decodeVariant($data) as Record<string, unknown>
  } else if ($data && typeof $data === 'object') {
    // If $data came back as an object (some parquet readers might do this)
    dataFields = $data as Record<string, unknown>
  }

  // Merge shredded columns - they take precedence over $data values
  const shredFields = options?.shredFields ?? []
  for (const field of shredFields) {
    if (field in otherColumns) {
      const value = otherColumns[field]
      // Null values in shredded columns are treated as missing (undefined)
      if (value !== null) {
        dataFields[field] = value
      } else {
        // Remove from dataFields if shredded column is null
        delete dataFields[field]
      }
    }
  }

  // Extract audit fields from dataFields (they were stored in $data)
  const {
    createdAt,
    createdBy,
    updatedAt,
    updatedBy,
    deletedAt,
    deletedBy,
    version,
    ...userDataFields
  } = dataFields

  return {
    $id: $id as EntityId,
    $type: $type as string,
    name: $name as string,
    createdAt: typeof createdAt === 'string' ? new Date(createdAt) : (createdAt as Date) ?? new Date(),
    createdBy: (createdBy as EntityId) ?? ('' as EntityId),
    updatedAt: typeof updatedAt === 'string' ? new Date(updatedAt) : (updatedAt as Date) ?? new Date(),
    updatedBy: (updatedBy as EntityId) ?? ('' as EntityId),
    ...(deletedAt ? { deletedAt: typeof deletedAt === 'string' ? new Date(deletedAt) : deletedAt as Date } : {}),
    ...(deletedBy ? { deletedBy: deletedBy as EntityId } : {}),
    version: (version as number) ?? 1,
    ...userDataFields,
  } as Entity<T>
}

// =============================================================================
// Parquet Schema
// =============================================================================

/**
 * Build the standard ParquetSchema for entity storage
 *
 * Schema includes:
 * - $id, $type, $name: Core identity fields (all prefixed with $ for consistency)
 * - $data: Base64-encoded Variant for flexible data (including audit fields)
 *
 * NOTE: Audit fields (createdAt, createdBy, updatedAt, updatedBy, deletedAt, deletedBy, version)
 * are stored in $data variant, NOT as separate columns. This keeps the schema lean and
 * audit information is primarily tracked in events.parquet where it naturally belongs.
 *
 * @returns ParquetSchema for entity storage
 */
export function buildEntityParquetSchema(): ParquetSchema {
  return {
    $id: { type: 'STRING', optional: false },
    $type: { type: 'STRING', optional: false },
    $name: { type: 'STRING', optional: false },
    $data: { type: 'STRING', optional: true }, // Base64-encoded Variant (includes audit fields)
  }
}

/**
 * Build the standard ParquetSchema for event storage
 *
 * Schema includes:
 * - id: Event ID (ULID)
 * - ts: Timestamp (ms since epoch)
 * - op: Operation type (CREATE, UPDATE, DELETE)
 * - target: Entity or relationship target string
 * - before: Base64-encoded Variant for state before change
 * - after: Base64-encoded Variant for state after change
 * - actor: Who made the change
 * - metadata: Base64-encoded Variant for additional metadata
 *
 * @returns ParquetSchema for event storage
 */
export function buildEventParquetSchema(): ParquetSchema {
  return {
    id: { type: 'STRING', optional: false },
    ts: { type: 'INT64', optional: false },
    op: { type: 'STRING', optional: false },
    target: { type: 'STRING', optional: false },
    before: { type: 'STRING', optional: true }, // Base64-encoded Variant
    after: { type: 'STRING', optional: true }, // Base64-encoded Variant
    actor: { type: 'STRING', optional: true },
    metadata: { type: 'STRING', optional: true }, // Base64-encoded Variant
  }
}

/**
 * Convert an event to a Parquet row
 *
 * Encodes Variant fields (before, after, metadata) as base64-encoded binary.
 *
 * @param event - The event to convert
 * @returns A row object suitable for Parquet writing
 */
export function eventToRow(event: Event): Record<string, unknown> {
  return {
    id: event.id,
    ts: BigInt(event.ts),
    op: event.op,
    target: event.target,
    before: event.before !== undefined ? btoa(String.fromCharCode(...encodeVariant(event.before))) : null,
    after: event.after !== undefined ? btoa(String.fromCharCode(...encodeVariant(event.after))) : null,
    actor: event.actor ?? null,
    metadata: event.metadata !== undefined ? btoa(String.fromCharCode(...encodeVariant(event.metadata))) : null,
  }
}

/**
 * Convert a Parquet row back to an event
 *
 * @param row - The Parquet row to convert
 * @returns The reconstructed event
 */
export function rowToEvent(row: Record<string, unknown>): {
  id: string
  ts: number
  op: string
  target: string
  before?: unknown | undefined
  after?: unknown | undefined
  actor?: string | undefined
  metadata?: unknown | undefined
} {
  const { id, ts, op, target, before, after, actor, metadata } = row

  const event: {
    id: string
    ts: number
    op: string
    target: string
    before?: unknown | undefined
    after?: unknown | undefined
    actor?: string | undefined
    metadata?: unknown | undefined
  } = {
    id: id as string,
    ts: typeof ts === 'bigint' ? Number(ts) : ts as number,
    op: op as string,
    target: target as string,
  }

  if (before && typeof before === 'string') {
    const bytes = Uint8Array.from(atob(before), c => c.charCodeAt(0))
    event.before = decodeVariant(bytes)
  }
  if (after && typeof after === 'string') {
    const bytes = Uint8Array.from(atob(after), c => c.charCodeAt(0))
    event.after = decodeVariant(bytes)
  }
  if (actor) {
    event.actor = actor as string
  }
  if (metadata && typeof metadata === 'string') {
    const bytes = Uint8Array.from(atob(metadata), c => c.charCodeAt(0))
    event.metadata = decodeVariant(bytes)
  }

  return event
}

// =============================================================================
// Relationship Serialization
// =============================================================================

/**
 * Build the standard ParquetSchema for relationship storage
 *
 * Schema includes:
 * - sourceId: The entity that has the relationship
 * - sourceField: The field name on the source entity
 * - targetId: The entity being referenced
 * - createdAt: When the relationship was created
 *
 * @returns ParquetSchema for relationship storage
 */
export function buildRelationshipParquetSchema(): ParquetSchema {
  return {
    sourceId: { type: 'STRING', optional: false },
    sourceField: { type: 'STRING', optional: false },
    targetId: { type: 'STRING', optional: false },
    createdAt: { type: 'STRING', optional: false },
  }
}

/**
 * Convert a relationship to a Parquet row
 */
export function relationshipToRow(rel: {
  sourceId: string
  sourceField: string
  targetId: string
  createdAt?: string | undefined
}): Record<string, unknown> {
  return {
    sourceId: rel.sourceId,
    sourceField: rel.sourceField,
    targetId: rel.targetId,
    createdAt: rel.createdAt ?? new Date().toISOString(),
  }
}

/**
 * Convert relationships from the reverse index to rows for Parquet
 *
 * @param reverseRelIndex - Map<targetId, Map<sourceKey, Set<sourceId>>>
 * @returns Array of relationship rows
 */
export function reverseRelIndexToRows(
  reverseRelIndex: Map<string, Map<string, Set<string>>>
): Array<{ sourceId: string; sourceField: string; targetId: string; createdAt: string }> {
  const rows: Array<{ sourceId: string; sourceField: string; targetId: string; createdAt: string }> = []
  const now = new Date().toISOString()

  for (const [targetId, sourceMap] of reverseRelIndex) {
    for (const [sourceKey, sourceIds] of sourceMap) {
      // sourceKey is "namespace.fieldName", extract just the fieldName
      const sourceField = sourceKey.includes('.') ? sourceKey.split('.').slice(1).join('.') : sourceKey
      for (const sourceId of sourceIds) {
        rows.push({
          sourceId,
          sourceField,
          targetId,
          createdAt: now,
        })
      }
    }
  }

  return rows
}

// =============================================================================
// Filter Matching
// =============================================================================

/**
 * Check if an entity matches a MongoDB-style filter
 *
 * Supports:
 * - Logical operators: $and, $or, $nor, $not
 * - Comparison operators: $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin
 * - Existence operators: $exists
 * - String operators: $regex
 *
 * @param entity - The entity (or record) to check
 * @param filter - MongoDB-style filter object
 * @returns true if entity matches the filter
 */
export function matchesFilter(entity: Record<string, unknown>, filter: Filter): boolean {
  for (const [key, condition] of Object.entries(filter)) {
    // Handle logical operators
    if (key === '$and') {
      const conditions = condition as Filter[]
      if (!conditions.every(c => matchesFilter(entity, c))) {
        return false
      }
      continue
    }
    if (key === '$or') {
      const conditions = condition as Filter[]
      if (!conditions.some(c => matchesFilter(entity, c))) {
        return false
      }
      continue
    }
    if (key === '$nor') {
      const conditions = condition as Filter[]
      if (conditions.some(c => matchesFilter(entity, c))) {
        return false
      }
      continue
    }
    if (key === '$not') {
      if (matchesFilter(entity, condition as Filter)) {
        return false
      }
      continue
    }

    const value = entity[key]

    // Simple equality check
    if (condition === null || typeof condition !== 'object') {
      if (value !== condition) {
        return false
      }
      continue
    }

    // Handle comparison operators
    const ops = condition as Record<string, unknown>
    for (const [op, expected] of Object.entries(ops)) {
      switch (op) {
        case '$eq':
          if (value !== expected) return false
          break
        case '$ne':
          if (value === expected) return false
          break
        case '$gt':
          if (!(typeof value === 'number' && value > (expected as number))) return false
          break
        case '$gte':
          if (!(typeof value === 'number' && value >= (expected as number))) return false
          break
        case '$lt':
          if (!(typeof value === 'number' && value < (expected as number))) return false
          break
        case '$lte':
          if (!(typeof value === 'number' && value <= (expected as number))) return false
          break
        case '$in':
          if (!(expected as unknown[]).includes(value)) return false
          break
        case '$nin':
          if ((expected as unknown[]).includes(value)) return false
          break
        case '$exists':
          if (expected && value === undefined) return false
          if (!expected && value !== undefined) return false
          break
        case '$regex': {
          const regex = createSafeRegex(expected as string | RegExp)
          if (!regex.test(String(value))) return false
          break
        }
      }
    }
  }

  return true
}

// =============================================================================
// ID Generation
// =============================================================================

/**
 * Generate a unique entity ID
 *
 * Creates a ULID-like ID combining timestamp and random components.
 * Format: {base36_timestamp}{random_8chars}
 *
 * @returns A unique ID string
 */
export function generateEntityId(): string {
  // ULID-like ID: timestamp + random
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 10)
  return `${timestamp}${random}`
}

// =============================================================================
// Data Field Extraction
// =============================================================================

/**
 * Extract data fields from create input
 *
 * Separates user data fields from special fields ($type, name)
 * that are handled separately.
 *
 * @param input - The CreateInput object
 * @returns Object containing only user data fields
 */
export function extractDataFields<T extends EntityData = EntityData>(input: CreateInput<T>): Partial<T> {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { $type, name, ...data } = input
  return data as unknown as Partial<T>
}

// =============================================================================
// Binary Encoding Helpers
// =============================================================================

/**
 * Convert bytes to base64 string
 *
 * @param bytes - The Uint8Array to encode
 * @returns Base64-encoded string
 */
export function bytesToBase64(bytes: Uint8Array): string {
  // Use btoa if available (browser/modern Node.js)
  if (typeof btoa === 'function') {
    let binary = ''
    for (let i = 0; i < bytes.length; i++) {
      binary += String.fromCharCode(bytes[i]!)
    }
    return btoa(binary)
  }
  // Fallback for older Node.js
  return Buffer.from(bytes).toString('base64')
}

/**
 * Convert base64 string to bytes
 *
 * @param base64 - The base64-encoded string
 * @returns Decoded Uint8Array
 */
export function base64ToBytes(base64: string): Uint8Array {
  // Use atob if available (browser/modern Node.js)
  if (typeof atob === 'function') {
    const binary = atob(base64)
    const bytes = new Uint8Array(binary.length)
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i)
    }
    return bytes
  }
  // Fallback for older Node.js
  return new Uint8Array(Buffer.from(base64, 'base64'))
}
