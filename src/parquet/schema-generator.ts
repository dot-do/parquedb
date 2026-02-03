/**
 * Parquet Schema Generator for Typed Collections
 *
 * Converts IceType/ParqueDB schema definitions to Parquet column schemas.
 * This module provides a clean API for generating Parquet schemas from
 * TypeDefinition objects, with support for:
 * - Type mapping from IceType to Parquet types
 * - Required vs optional fields
 * - Array types (as LIST)
 * - System columns (always present)
 * - $data Variant column for flexible data
 *
 * @module parquet/schema-generator
 */

import type { TypeDefinition, FieldDefinition } from '../types/schema'
import { parseFieldType, isRelationString } from '../types/schema'

// =============================================================================
// Types
// =============================================================================

/**
 * Parquet type strings compatible with hyparquet-writer
 *
 * Maps to hyparquet-writer's BasicType union
 */
export type ParquetType =
  | 'BOOLEAN'
  | 'INT32'
  | 'INT64'
  | 'FLOAT'
  | 'DOUBLE'
  | 'BYTE_ARRAY'
  | 'STRING'
  | 'JSON'
  | 'TIMESTAMP'
  | 'UUID'

/**
 * Schema field definition for hyparquet-writer
 *
 * Represents a single column in the Parquet schema
 */
export interface SchemaField {
  /** Parquet type */
  type: ParquetType
  /** Whether the field is optional (nullable) */
  optional: boolean
}

/**
 * Schema tree structure for Parquet file generation
 *
 * Maps column names to their field definitions
 */
export interface SchemaTree {
  [columnName: string]: SchemaField
}

/**
 * Options for schema generation
 */
export interface SchemaGeneratorOptions {
  /**
   * Include $data Variant column for flexible data storage
   * Default: true
   */
  includeDataVariant?: boolean

  /**
   * Include audit columns (createdAt, updatedAt, etc.)
   * Default: true
   */
  includeAuditColumns?: boolean

  /**
   * Include soft delete columns (deletedAt, deletedBy)
   * Default: true
   */
  includeSoftDeleteColumns?: boolean
}

// =============================================================================
// Type Mapping
// =============================================================================

/**
 * Map IceType base types to Parquet types
 *
 * | IceType   | Parquet          |
 * |-----------|------------------|
 * | string    | STRING           |
 * | text      | STRING           |
 * | markdown  | STRING           |
 * | int       | INT32            |
 * | float     | DOUBLE           |
 * | double    | DOUBLE           |
 * | number    | DOUBLE           |
 * | bool      | BOOLEAN          |
 * | boolean   | BOOLEAN          |
 * | date      | STRING (ISO)     |
 * | datetime  | TIMESTAMP        |
 * | timestamp | TIMESTAMP        |
 * | uuid      | STRING           |
 * | email     | STRING           |
 * | url       | STRING           |
 * | json      | JSON             |
 * | binary    | BYTE_ARRAY       |
 */
const ICETYPE_TO_PARQUET: Record<string, ParquetType> = {
  // String types
  string: 'STRING',
  text: 'STRING',
  markdown: 'STRING',
  email: 'STRING',
  url: 'STRING',
  uuid: 'STRING',

  // Numeric types
  int: 'INT32',
  integer: 'INT32',
  float: 'DOUBLE',
  double: 'DOUBLE',
  number: 'DOUBLE',

  // Boolean
  bool: 'BOOLEAN',
  boolean: 'BOOLEAN',

  // Date/Time
  date: 'STRING', // ISO 8601 date string
  datetime: 'TIMESTAMP',
  timestamp: 'TIMESTAMP',

  // Binary/JSON
  json: 'JSON',
  binary: 'BYTE_ARRAY',
}

// =============================================================================
// Core Functions
// =============================================================================

/**
 * Convert an IceType string to a Parquet type
 *
 * Handles parametric types like decimal(10,2), varchar(255), etc.
 *
 * @param iceType - The IceType base type string
 * @returns The corresponding Parquet type
 *
 * @example
 * iceTypeToParquet('string')   // 'STRING'
 * iceTypeToParquet('int')      // 'INT32'
 * iceTypeToParquet('datetime') // 'TIMESTAMP'
 * iceTypeToParquet('json')     // 'JSON'
 */
export function iceTypeToParquet(iceType: string): ParquetType {
  const normalized = iceType.toLowerCase().trim()

  // Handle parametric types
  if (normalized.startsWith('decimal')) {
    // Decimals are stored as DOUBLE for simplicity
    // (Parquet DECIMAL requires complex handling)
    return 'DOUBLE'
  }

  if (normalized.startsWith('varchar') || normalized.startsWith('char')) {
    return 'STRING'
  }

  if (normalized.startsWith('vector')) {
    // Vectors are stored as BYTE_ARRAY (serialized float array)
    return 'BYTE_ARRAY'
  }

  if (normalized.startsWith('enum')) {
    return 'STRING'
  }

  // Direct mapping
  const mapped = ICETYPE_TO_PARQUET[normalized]
  if (mapped) {
    return mapped
  }

  // Default to JSON for unknown types
  return 'JSON'
}

/**
 * Generate a Parquet SchemaTree from a TypeDefinition
 *
 * Creates a schema with:
 * - System columns ($id, $type)
 * - User-defined columns (from TypeDefinition)
 * - Optional $data Variant column
 * - Optional audit columns
 *
 * @param typeDef - The TypeDefinition to convert
 * @param options - Schema generation options
 * @returns SchemaTree compatible with hyparquet-writer
 *
 * @example
 * const Post: TypeDefinition = {
 *   title: 'string!',
 *   content: 'text',
 *   published: 'boolean',
 *   views: 'int',
 * }
 *
 * const schema = generateParquetSchema(Post)
 * // {
 * //   $id: { type: 'STRING', optional: false },
 * //   $type: { type: 'STRING', optional: false },
 * //   title: { type: 'STRING', optional: false },
 * //   content: { type: 'STRING', optional: true },
 * //   published: { type: 'BOOLEAN', optional: true },
 * //   views: { type: 'INT32', optional: true },
 * //   $data: { type: 'JSON', optional: true },
 * //   createdAt: { type: 'TIMESTAMP', optional: false },
 * //   ...
 * // }
 */
export function generateParquetSchema(
  typeDef: TypeDefinition,
  options: SchemaGeneratorOptions = {}
): SchemaTree {
  const {
    includeDataVariant = true,
    includeAuditColumns = true,
    includeSoftDeleteColumns = true,
  } = options

  const columns: SchemaTree = {}

  // =========================================================================
  // System columns (always present)
  // =========================================================================

  columns.$id = { type: 'STRING', optional: false }
  columns.$type = { type: 'STRING', optional: false }

  // =========================================================================
  // User-defined columns
  // =========================================================================

  for (const [name, fieldDef] of Object.entries(typeDef)) {
    // Skip directives/metadata fields
    if (name.startsWith('$')) continue

    // Skip arrays (like $shred, $indexes)
    if (Array.isArray(fieldDef)) continue

    // Skip null/undefined values
    if (fieldDef == null) continue

    // Handle string field definitions
    if (typeof fieldDef === 'string') {
      // Skip relationship definitions
      if (isRelationString(fieldDef)) continue

      const parsed = parseFieldType(fieldDef)
      const parquetType = iceTypeToParquet(parsed.type)

      // Handle array types - store as JSON
      if (parsed.isArray) {
        columns[name] = {
          type: 'JSON',
          optional: !parsed.required,
        }
      } else {
        columns[name] = {
          type: parquetType,
          optional: !parsed.required,
        }
      }
      continue
    }

    // Handle object field definitions
    if (typeof fieldDef === 'object' && fieldDef !== null && 'type' in fieldDef) {
      const def = fieldDef as FieldDefinition
      const parsed = parseFieldType(def.type)
      const parquetType = iceTypeToParquet(parsed.type)

      // Override required if explicitly set
      const isOptional = def.required !== undefined ? !def.required : !parsed.required

      // Handle array types
      if (parsed.isArray) {
        columns[name] = {
          type: 'JSON',
          optional: isOptional,
        }
      } else {
        columns[name] = {
          type: parquetType,
          optional: isOptional,
        }
      }
      continue
    }

    // Skip other types (boolean for $abstract, etc.)
  }

  // =========================================================================
  // $data Variant column
  // =========================================================================

  if (includeDataVariant) {
    columns.$data = { type: 'JSON', optional: true }
  }

  // =========================================================================
  // Audit columns
  // =========================================================================

  if (includeAuditColumns) {
    columns.createdAt = { type: 'TIMESTAMP', optional: false }
    columns.createdBy = { type: 'STRING', optional: false }
    columns.updatedAt = { type: 'TIMESTAMP', optional: false }
    columns.updatedBy = { type: 'STRING', optional: false }
    columns.version = { type: 'INT32', optional: false }
  }

  // =========================================================================
  // Soft delete columns
  // =========================================================================

  if (includeSoftDeleteColumns) {
    columns.deletedAt = { type: 'TIMESTAMP', optional: true }
    columns.deletedBy = { type: 'STRING', optional: true }
  }

  return columns
}

/**
 * Generate a minimal schema with only system columns
 *
 * Useful for generic entity storage without typed fields.
 *
 * @param options - Schema generation options
 * @returns SchemaTree with system columns only
 */
export function generateMinimalSchema(
  options: SchemaGeneratorOptions = {}
): SchemaTree {
  return generateParquetSchema({}, options)
}

/**
 * Convert SchemaTree to hyparquet-writer ColumnSource format
 *
 * This is useful when you need to pass schema information
 * to hyparquet-writer's parquetWriteBuffer function.
 *
 * @param schema - The SchemaTree to convert
 * @param data - The data object to use for column values
 * @returns Array of ColumnSource objects
 */
export function schemaToColumnSources(
  schema: SchemaTree,
  data: Record<string, unknown[]>
): Array<{ name: string; data: unknown[]; type: string; nullable: boolean }> {
  return Object.entries(schema).map(([name, field]) => ({
    name,
    data: data[name] ?? [],
    type: field.type,
    nullable: field.optional,
  }))
}

/**
 * Validate a SchemaTree for correctness
 *
 * @param schema - The schema to validate
 * @returns Validation result with errors if any
 */
export function validateSchemaTree(
  schema: SchemaTree
): { valid: boolean; errors: string[] } {
  const errors: string[] = []

  // Check for required system columns
  if (!schema.$id) {
    errors.push('Missing required system column: $id')
  }
  if (!schema.$type) {
    errors.push('Missing required system column: $type')
  }

  // Validate each field
  for (const [name, field] of Object.entries(schema)) {
    if (!field.type) {
      errors.push(`Field '${name}' is missing type`)
    }

    if (typeof field.optional !== 'boolean') {
      errors.push(`Field '${name}' has invalid optional value (must be boolean)`)
    }
  }

  return {
    valid: errors.length === 0,
    errors,
  }
}

/**
 * Get the list of column names from a SchemaTree
 *
 * @param schema - The schema to extract names from
 * @returns Array of column names
 */
export function getSchemaColumnNames(schema: SchemaTree): string[] {
  return Object.keys(schema)
}

/**
 * Check if a SchemaTree has a specific column
 *
 * @param schema - The schema to check
 * @param columnName - The column name to look for
 * @returns True if the column exists
 */
export function schemaHasColumn(schema: SchemaTree, columnName: string): boolean {
  return columnName in schema
}

/**
 * Get only the required columns from a schema
 *
 * @param schema - The schema to filter
 * @returns Array of column names that are required (not optional)
 */
export function getRequiredColumns(schema: SchemaTree): string[] {
  return Object.entries(schema)
    .filter(([, field]) => !field.optional)
    .map(([name]) => name)
}

/**
 * Get only the optional columns from a schema
 *
 * @param schema - The schema to filter
 * @returns Array of column names that are optional
 */
export function getOptionalColumns(schema: SchemaTree): string[] {
  return Object.entries(schema)
    .filter(([, field]) => field.optional)
    .map(([name]) => name)
}
