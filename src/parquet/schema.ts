/**
 * Schema conversion utilities for Parquet
 *
 * Converts ParqueDB schema definitions to Parquet schemas,
 * handling type mapping, shredding configuration, and
 * entity schema creation.
 */

import type { TypeDefinition, FieldDef, ParsedField } from '../types/schema'
import { parseFieldType, isRelationString } from '../types/schema'
import type {
  ParquetSchema,
  ParquetFieldSchema,
  CreateEntitySchemaOptions,
  ParquetPrimitiveType,
  ParquetLogicalType,
} from './types'

// =============================================================================
// Type Mapping
// =============================================================================

/** Map ParqueDB types to Parquet types */
const TYPE_MAP: Record<string, { type: ParquetPrimitiveType | ParquetLogicalType; optional?: boolean }> = {
  // String types
  string: { type: 'STRING' },
  text: { type: 'STRING' },
  markdown: { type: 'STRING' },
  email: { type: 'STRING' },
  url: { type: 'STRING' },
  uuid: { type: 'STRING' },

  // Numeric types
  number: { type: 'DOUBLE' },
  int: { type: 'INT64' },
  float: { type: 'DOUBLE' },
  double: { type: 'DOUBLE' },

  // Boolean
  boolean: { type: 'BOOLEAN' },

  // Date/Time types
  date: { type: 'DATE' },
  datetime: { type: 'TIMESTAMP_MILLIS' },
  timestamp: { type: 'TIMESTAMP_MILLIS' },

  // Binary types
  json: { type: 'BYTE_ARRAY' },
  binary: { type: 'BYTE_ARRAY' },
}

// =============================================================================
// Core Schema Conversion
// =============================================================================

/**
 * Convert a ParqueDB type string to Parquet type
 *
 * @param parquedbType - The ParqueDB type string (e.g., 'string!', 'int?', 'datetime')
 * @returns Parquet field schema
 */
export function inferParquetType(parquedbType: string): ParquetFieldSchema {
  const parsed = parseFieldType(parquedbType)
  const baseType = parsed.type.toLowerCase()

  // Handle parametric types
  if (baseType.startsWith('decimal')) {
    const match = baseType.match(/decimal\((\d+),(\d+)\)/)
    if (match) {
      return {
        type: 'DECIMAL',
        precision: parseInt(match[1], 10),
        scale: parseInt(match[2], 10),
        optional: !parsed.required,
      }
    }
    return { type: 'DECIMAL', precision: 18, scale: 2, optional: !parsed.required }
  }

  if (baseType.startsWith('varchar') || baseType.startsWith('char')) {
    return { type: 'STRING', optional: !parsed.required }
  }

  if (baseType.startsWith('vector')) {
    return { type: 'BYTE_ARRAY', optional: !parsed.required }
  }

  if (baseType.startsWith('enum')) {
    return { type: 'STRING', optional: !parsed.required }
  }

  // Handle arrays
  if (parsed.isArray) {
    return {
      type: 'BYTE_ARRAY', // Store arrays as JSON
      optional: !parsed.required,
      repetitionType: 'REPEATED',
    }
  }

  // Look up mapped type
  const mapped = TYPE_MAP[baseType]
  if (mapped) {
    return {
      type: mapped.type,
      optional: !parsed.required,
    }
  }

  // Default to BYTE_ARRAY for unknown types
  return { type: 'BYTE_ARRAY', optional: true }
}

/**
 * Convert ParqueDB TypeDefinition to Parquet schema
 *
 * @param typeDef - The ParqueDB type definition
 * @returns Parquet schema
 */
export function toParquetSchema(typeDef: TypeDefinition): ParquetSchema {
  const schema: ParquetSchema = {}

  for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
    // Skip metadata fields
    if (fieldName.startsWith('$')) continue

    // Skip non-field definitions
    if (typeof fieldDef !== 'string' && typeof fieldDef !== 'object') continue
    if (Array.isArray(fieldDef)) continue

    // Skip relationship definitions
    if (typeof fieldDef === 'string' && isRelationString(fieldDef)) continue

    // Parse field definition
    if (typeof fieldDef === 'string') {
      const parquetType = inferParquetType(fieldDef)
      schema[fieldName] = parquetType
    } else if (typeof fieldDef === 'object' && fieldDef !== null && 'type' in fieldDef) {
      const def = fieldDef as { type: string; required?: boolean }
      const parquetType = inferParquetType(def.type)
      if (def.required !== undefined) {
        parquetType.optional = !def.required
      }
      schema[fieldName] = parquetType
    }
  }

  return schema
}

/**
 * Parse a field definition to Parquet schema
 */
function parseFieldDef(fieldName: string, fieldDef: FieldDef): ParquetFieldSchema | null {
  if (typeof fieldDef === 'string') {
    // String shorthand: 'string!', 'int?', etc.
    if (isRelationString(fieldDef)) {
      return null // Skip relationships
    }
    return inferParquetType(fieldDef)
  }

  if (typeof fieldDef === 'object' && fieldDef !== null && 'type' in fieldDef) {
    // Full field definition object
    const def = fieldDef as { type: string; required?: boolean; index?: unknown }
    const parquetType = inferParquetType(def.type)

    // Override optional if explicitly set
    if (def.required !== undefined) {
      parquetType.optional = !def.required
    }

    return parquetType
  }

  return null
}

// =============================================================================
// Entity Schema Creation
// =============================================================================

/**
 * Create schema for entity storage in Parquet
 *
 * Includes fixed system columns, shredded fields for query efficiency,
 * and a Variant column for flexible data.
 *
 * @param options - Schema creation options
 * @returns Complete Parquet schema for entity storage
 */
export function createEntitySchema(options: CreateEntitySchemaOptions = {}): ParquetSchema {
  const { typeDef, shredFields = [], additionalColumns = {} } = options

  const schema: ParquetSchema = {
    // =========================================================================
    // System columns (always present)
    // =========================================================================

    /** Entity ID (namespace/id format) */
    $id: { type: 'STRING', optional: false },

    /** Entity type */
    $type: { type: 'STRING', optional: true },

    /** Human-readable display name */
    name: { type: 'STRING', optional: false },

    // =========================================================================
    // Audit columns
    // =========================================================================

    /** Creation timestamp */
    createdAt: { type: 'TIMESTAMP_MILLIS', optional: false },

    /** Created by (EntityId) */
    createdBy: { type: 'STRING', optional: false },

    /** Last update timestamp */
    updatedAt: { type: 'TIMESTAMP_MILLIS', optional: false },

    /** Updated by (EntityId) */
    updatedBy: { type: 'STRING', optional: false },

    /** Soft delete timestamp */
    deletedAt: { type: 'TIMESTAMP_MILLIS', optional: true },

    /** Deleted by (EntityId) */
    deletedBy: { type: 'STRING', optional: true },

    /** Optimistic concurrency version */
    version: { type: 'INT32', optional: false },

    // =========================================================================
    // Data column
    // =========================================================================

    /** Variant-encoded remaining data */
    $data: { type: 'BYTE_ARRAY', optional: false },
  }

  // =========================================================================
  // Shredded columns
  // =========================================================================

  if (typeDef && shredFields.length > 0) {
    for (const fieldName of shredFields) {
      const fieldDef = typeDef[fieldName]
      if (fieldDef && typeof fieldDef === 'string' && !isRelationString(fieldDef)) {
        schema[fieldName] = inferParquetType(fieldDef)
      } else if (fieldDef && typeof fieldDef === 'object' && 'type' in (fieldDef as object)) {
        const def = fieldDef as { type: string; required?: boolean }
        const parquetType = inferParquetType(def.type)
        if (def.required !== undefined) {
          parquetType.optional = !def.required
        }
        schema[fieldName] = parquetType
      }
    }
  }

  // =========================================================================
  // Additional columns
  // =========================================================================

  for (const [name, fieldSchema] of Object.entries(additionalColumns)) {
    schema[name] = fieldSchema
  }

  return schema
}

/**
 * Create schema for relationship storage in Parquet
 *
 * Used for both forward and reverse relationship indexes.
 *
 * @returns Parquet schema for relationship storage
 */
export function createRelationshipSchema(): ParquetSchema {
  return {
    // Source entity
    fromNs: { type: 'STRING', optional: false },
    fromId: { type: 'STRING', optional: false },
    fromType: { type: 'STRING', optional: true },
    fromName: { type: 'STRING', optional: true },

    // Relationship names
    predicate: { type: 'STRING', optional: false },
    reverse: { type: 'STRING', optional: false },

    // Target entity
    toNs: { type: 'STRING', optional: false },
    toId: { type: 'STRING', optional: false },
    toType: { type: 'STRING', optional: true },
    toName: { type: 'STRING', optional: true },

    // Audit
    createdAt: { type: 'TIMESTAMP_MILLIS', optional: false },
    createdBy: { type: 'STRING', optional: false },
    deletedAt: { type: 'TIMESTAMP_MILLIS', optional: true },
    deletedBy: { type: 'STRING', optional: true },
    version: { type: 'INT32', optional: false },

    // Edge properties (Variant)
    data: { type: 'BYTE_ARRAY', optional: true },
  }
}

/**
 * Create schema for event log storage in Parquet
 *
 * @returns Parquet schema for CDC event log
 */
export function createEventSchema(): ParquetSchema {
  return {
    // Event identity
    id: { type: 'STRING', optional: false },
    ts: { type: 'TIMESTAMP_MILLIS', optional: false },

    // Target info
    target: { type: 'STRING', optional: false }, // 'entity' | 'rel'
    op: { type: 'STRING', optional: false },     // 'CREATE' | 'UPDATE' | 'DELETE'

    // Entity reference
    ns: { type: 'STRING', optional: false },
    entityId: { type: 'STRING', optional: false },

    // State snapshots (Variant)
    before: { type: 'BYTE_ARRAY', optional: true },
    after: { type: 'BYTE_ARRAY', optional: true },

    // Audit
    actor: { type: 'STRING', optional: false },
    metadata: { type: 'BYTE_ARRAY', optional: true },
  }
}

// =============================================================================
// Schema Validation
// =============================================================================

/**
 * Validate a Parquet schema definition
 *
 * @param schema - The schema to validate
 * @returns Validation result with errors if any
 */
export function validateParquetSchema(
  schema: ParquetSchema
): { valid: boolean; errors: string[] } {
  const errors: string[] = []

  for (const [fieldName, fieldSchema] of Object.entries(schema)) {
    // Validate field name
    if (!fieldName || typeof fieldName !== 'string') {
      errors.push(`Invalid field name: ${fieldName}`)
      continue
    }

    // Validate type
    if (!fieldSchema.type) {
      errors.push(`Field '${fieldName}' is missing type`)
    }

    // Validate decimal parameters
    if (fieldSchema.type === 'DECIMAL') {
      if (fieldSchema.precision === undefined) {
        errors.push(`Field '${fieldName}' (DECIMAL) is missing precision`)
      }
      if (fieldSchema.scale === undefined) {
        errors.push(`Field '${fieldName}' (DECIMAL) is missing scale`)
      }
      if (
        fieldSchema.precision !== undefined &&
        fieldSchema.scale !== undefined &&
        fieldSchema.scale > fieldSchema.precision
      ) {
        errors.push(
          `Field '${fieldName}' (DECIMAL) scale cannot be greater than precision`
        )
      }
    }

    // Validate fixed length
    if (fieldSchema.type === 'FIXED_LEN_BYTE_ARRAY') {
      if (fieldSchema.typeLength === undefined || fieldSchema.typeLength <= 0) {
        errors.push(`Field '${fieldName}' (FIXED_LEN_BYTE_ARRAY) is missing or has invalid typeLength`)
      }
    }
  }

  return { valid: errors.length === 0, errors }
}

// =============================================================================
// Schema Utilities
// =============================================================================

/**
 * Get the list of fields to shred from a type definition
 *
 * Returns fields marked in $shred or commonly indexed fields.
 *
 * @param typeDef - The type definition
 * @returns Array of field names to shred
 */
export function getShredFields(typeDef: TypeDefinition): string[] {
  // Explicit shred fields
  if (typeDef.$shred && Array.isArray(typeDef.$shred)) {
    return typeDef.$shred
  }

  // Auto-detect indexed fields
  const shredFields: string[] = []

  for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
    if (fieldName.startsWith('$')) continue
    if (typeof fieldDef === 'string') continue

    if (
      typeof fieldDef === 'object' &&
      fieldDef !== null &&
      'index' in fieldDef &&
      fieldDef.index
    ) {
      shredFields.push(fieldName)
    }
  }

  return shredFields
}

/**
 * Merge two Parquet schemas
 *
 * @param base - Base schema
 * @param override - Schema to merge in (takes precedence)
 * @returns Merged schema
 */
export function mergeSchemas(
  base: ParquetSchema,
  override: ParquetSchema
): ParquetSchema {
  return { ...base, ...override }
}

/**
 * Extract column names from a schema
 *
 * @param schema - The Parquet schema
 * @returns Array of column names
 */
export function getColumnNames(schema: ParquetSchema): string[] {
  return Object.keys(schema)
}

/**
 * Check if a schema has a specific column
 *
 * @param schema - The Parquet schema
 * @param columnName - Column name to check
 * @returns True if column exists
 */
export function hasColumn(schema: ParquetSchema, columnName: string): boolean {
  return columnName in schema
}
