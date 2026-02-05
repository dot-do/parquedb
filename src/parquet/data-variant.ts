/**
 * $data Variant Column Optimization
 *
 * Utilities for detecting and optimizing reads from Parquet files that use
 * the $data variant schema pattern (full entity stored as JSON in one column).
 *
 * When a schema has both $id and $data columns, we can optimize SELECT * queries
 * to only read these two columns instead of reassembling from many columns.
 *
 * @see parquedb-1so4: Optimize $data variant reads for SELECT *
 */

import type { ParquetSchema, ParquetFieldSchema } from './types'
import { tryParseJson, isRecord } from '../utils/json-validation'

// =============================================================================
// Schema Detection
// =============================================================================

/**
 * Check if a Parquet schema uses the $data variant pattern
 *
 * The $data variant pattern requires:
 * - A $id column (entity identifier, used for filtering)
 * - A $data column (JSON-encoded entity data)
 *
 * @param schema - Parquet schema to check
 * @returns true if schema uses $data variant pattern
 */
export function detectDataVariantSchema(
  schema: ParquetSchema | Record<string, ParquetFieldSchema>
): boolean {
  const columns = Object.keys(schema)

  // Must have both $id and $data columns
  return columns.includes('$id') && columns.includes('$data')
}

/**
 * Get the columns needed for efficient $data variant reads
 *
 * For SELECT * queries on $data variant schemas, we only need:
 * - $id: For filtering and as the entity identifier
 * - $data: Contains the full entity as JSON
 *
 * Optionally includes additional columns needed for filtering.
 *
 * @param schema - Parquet schema
 * @param filterColumns - Additional columns needed for filtering (optional)
 * @returns Array of column names to read, or null if not a $data variant schema
 */
export function getDataVariantColumns(
  schema: ParquetSchema | Record<string, ParquetFieldSchema>,
  filterColumns?: string[]
): string[] | null {
  // Check if this is a $data variant schema
  if (!detectDataVariantSchema(schema)) {
    return null
  }

  // Base columns: $id and $data
  const columns = ['$id', '$data']

  // Add any filter columns that aren't already included
  if (filterColumns) {
    for (const col of filterColumns) {
      if (!columns.includes(col) && col in schema) {
        columns.push(col)
      }
    }
  }

  return columns
}

// =============================================================================
// Entity Reconstruction
// =============================================================================

/**
 * Row with $data variant column
 */
export interface DataVariantRow {
  $id: string
  $data: string | Record<string, unknown> | null | undefined
  [key: string]: unknown
}

/**
 * Reconstruct an entity from a $data variant row
 *
 * The $data column contains the full entity as JSON. This function:
 * 1. Parses the $data JSON (if string)
 * 2. Merges $id from the row (takes precedence over $data.$id)
 * 3. Returns the reconstructed entity
 *
 * @param row - Row with $id and $data columns
 * @returns Reconstructed entity object
 */
export function reconstructEntityFromDataVariant<T extends Record<string, unknown> = Record<string, unknown>>(
  row: DataVariantRow
): T {
  const { $id, $data } = row

  // Handle null/undefined $data
  if ($data === null || $data === undefined) {
    return { $id } as unknown as T
  }

  // Parse $data if it's a string
  let data: Record<string, unknown>
  if (typeof $data === 'string') {
    const parsed = tryParseJson<Record<string, unknown>>($data)
    if (!parsed || !isRecord(parsed)) {
      // Invalid JSON, return row with $id
      return { $id } as unknown as T
    }
    data = parsed
  } else if (isRecord($data)) {
    data = $data
  } else {
    // Unexpected type, return row with $id
    return { $id } as unknown as T
  }

  // Merge $id from row (takes precedence) with data from $data
  return { ...data, $id } as unknown as T
}

/**
 * Reconstruct multiple entities from $data variant rows
 *
 * Batch version of reconstructEntityFromDataVariant.
 *
 * @param rows - Array of rows with $id and $data columns
 * @returns Array of reconstructed entity objects
 */
export function reconstructEntitiesFromDataVariant<T extends Record<string, unknown> = Record<string, unknown>>(
  rows: DataVariantRow[]
): T[] {
  return rows.map(row => reconstructEntityFromDataVariant<T>(row))
}

// =============================================================================
// Column Projection Helpers
// =============================================================================

/**
 * Check if a column is a system column (starts with $)
 */
export function isSystemColumn(column: string): boolean {
  return column.startsWith('$')
}

/**
 * Check if a column is an index column ($index_*)
 */
export function isIndexColumn(column: string): boolean {
  return column.startsWith('$index_')
}

/**
 * Get the minimal columns needed for a query on a $data variant schema
 *
 * This determines the optimal column projection based on:
 * - Whether filters reference shredded columns
 * - Whether specific projections are requested
 *
 * @param schema - Parquet schema
 * @param options - Query options
 * @returns Optimal column projection, or undefined for all columns
 */
export function getOptimalColumnProjection(
  schema: ParquetSchema | Record<string, ParquetFieldSchema>,
  options: {
    filterColumns?: string[]
    projection?: string[]
  } = {}
): string[] | undefined {
  // Not a $data variant schema - read all columns
  if (!detectDataVariantSchema(schema)) {
    return undefined
  }

  // If specific projection is requested, use it
  if (options.projection && options.projection.length > 0) {
    // Ensure $id and $data are included if any field from $data is needed
    const hasNonSystemProjection = options.projection.some(col => !isSystemColumn(col))
    if (hasNonSystemProjection) {
      const columns = new Set(options.projection)
      columns.add('$id')
      columns.add('$data')
      return Array.from(columns)
    }
    return options.projection
  }

  // SELECT * on $data variant schema - only read $id and $data
  const columns = ['$id', '$data']

  // Add filter columns that are shredded (for predicate pushdown)
  if (options.filterColumns) {
    for (const col of options.filterColumns) {
      if (!columns.includes(col) && col in schema) {
        columns.push(col)
      }
    }
  }

  return columns
}
