/**
 * Shared Parquet utilities for entity backends
 *
 * Common utilities used by IcebergBackend, DeltaBackend, and other Parquet-based backends.
 * Provides entity serialization, filter matching, ID generation, and data field extraction.
 */

import type { Entity, EntityId, CreateInput } from '../types/entity'
import type { Filter } from '../types/filter'
import type { ParquetSchema } from '../parquet/types'
import { encodeVariant, decodeVariant } from '../parquet/variant'

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
  shredFields?: string[]
}

/**
 * Options for rowToEntity conversion
 */
export interface RowToEntityOptions {
  /**
   * Fields that were shredded into separate columns
   * These take precedence over values in $data
   */
  shredFields?: string[]
}

/**
 * Convert an entity to a Parquet row
 *
 * Extracts core fields ($id, $type, name, audit fields) into separate columns
 * and encodes remaining data fields as a base64-encoded Variant in $data.
 *
 * If shredFields is provided, those fields will be extracted into separate
 * top-level columns for predicate pushdown support.
 *
 * @param entity - The entity to convert
 * @param options - Optional configuration for shredding
 * @returns A row object suitable for Parquet writing
 */
export function entityToRow<T>(entity: Entity<T>, options?: EntityToRowOptions): Record<string, unknown> {
  // Extract core fields
  const {
    $id,
    $type,
    name,
    createdAt,
    createdBy,
    updatedAt,
    updatedBy,
    deletedAt,
    deletedBy,
    version,
    ...dataFields
  } = entity as Entity<T> & { deletedAt?: Date; deletedBy?: string }

  // Filter out undefined values (null should be preserved)
  const filteredDataFields: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(dataFields)) {
    if (value !== undefined) {
      filteredDataFields[key] = value
    }
  }

  // Extract shredded fields into separate columns
  const shredFields = options?.shredFields ?? []
  const shreddedColumns: Record<string, unknown> = {}
  const remainingDataFields: Record<string, unknown> = {}

  for (const [key, value] of Object.entries(filteredDataFields)) {
    if (shredFields.includes(key)) {
      // Extract to shredded column (preserve null values)
      shreddedColumns[key] = value
    } else {
      remainingDataFields[key] = value
    }
  }

  // Also check for null values explicitly set on shredded fields
  // (they may have been filtered above but we want to preserve nulls)
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
    name,
    createdAt: createdAt.toISOString(),
    createdBy,
    updatedAt: updatedAt.toISOString(),
    updatedBy,
    deletedAt: deletedAt?.toISOString() ?? null,
    deletedBy: deletedBy ?? null,
    version,
    $data,
    ...shreddedColumns,
  }
}

/**
 * Convert a Parquet row back to an Entity
 *
 * Decodes the $data Variant column and merges with core fields.
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
    name,
    createdAt,
    createdBy,
    updatedAt,
    updatedBy,
    deletedAt,
    deletedBy,
    version,
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

  return {
    $id: $id as EntityId,
    $type: $type as string,
    name: name as string,
    createdAt: typeof createdAt === 'string' ? new Date(createdAt) : createdAt as Date,
    createdBy: createdBy as EntityId,
    updatedAt: typeof updatedAt === 'string' ? new Date(updatedAt) : updatedAt as Date,
    updatedBy: updatedBy as EntityId,
    ...(deletedAt ? { deletedAt: typeof deletedAt === 'string' ? new Date(deletedAt) : deletedAt as Date } : {}),
    ...(deletedBy ? { deletedBy: deletedBy as EntityId } : {}),
    version: version as number,
    ...dataFields,
  } as Entity<T>
}

// =============================================================================
// Parquet Schema
// =============================================================================

/**
 * Build the standard ParquetSchema for entity storage
 *
 * Schema includes:
 * - $id, $type, name: Core identity fields
 * - createdAt, createdBy, updatedAt, updatedBy: Audit fields
 * - deletedAt, deletedBy: Soft delete fields
 * - version: Optimistic concurrency version
 * - $data: Base64-encoded Variant for flexible data
 *
 * @returns ParquetSchema for entity storage
 */
export function buildEntityParquetSchema(): ParquetSchema {
  return {
    $id: { type: 'STRING', optional: false },
    $type: { type: 'STRING', optional: false },
    name: { type: 'STRING', optional: false },
    createdAt: { type: 'STRING', optional: false }, // ISO timestamp string
    createdBy: { type: 'STRING', optional: false },
    updatedAt: { type: 'STRING', optional: false },
    updatedBy: { type: 'STRING', optional: false },
    deletedAt: { type: 'STRING', optional: true },
    deletedBy: { type: 'STRING', optional: true },
    version: { type: 'INT32', optional: false },
    $data: { type: 'STRING', optional: true }, // Base64-encoded Variant
  }
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
          const regex = expected instanceof RegExp ? expected : new RegExp(expected as string)
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
export function extractDataFields<T>(input: CreateInput<T>): Partial<T> {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { $type, name, ...data } = input
  return data as Partial<T>
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
