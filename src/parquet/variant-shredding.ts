/**
 * Variant Shredding Integration for ParqueDB
 *
 * Integrates hyparquet-writer's Variant shredding functionality to enable
 * predicate pushdown on hot fields stored in Variant columns.
 *
 * Phase 1: Support for $type field shredding from $data Variant column
 *
 * @see https://parquet.apache.org/docs/file-format/types/variantshredding/
 */

import type { Entity } from '../types/entity'
import type { TypeDefinition } from '../types/schema'
import { determineShredFields, DEFAULT_SHRED_CONFIG } from '../types/integrations'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for Variant column shredding
 */
export interface VariantShredWriteConfig {
  /** Column name (e.g., '$data', '$index') */
  columnName: string
  /** Fields to shred into typed columns */
  shredFields: string[]
  /** Optional type overrides for shredded fields */
  fieldTypes?: Record<string, string> | undefined
}

/**
 * Result of shredding entities for Parquet write
 */
export interface ShreddedEntityData {
  /** Schema elements for the shredded Variant column */
  schema: unknown[]
  /** Column data map (path -> values array) */
  columnData: Map<string, unknown[]>
  /** Paths that have statistics for predicate pushdown */
  statisticsPaths: string[]
}

/**
 * Default fields to shred from $data Variant column
 * These are fields commonly used in filters and benefit from statistics
 */
export const DEFAULT_DATA_SHRED_FIELDS = ['$type'] as const

// =============================================================================
// Writer Integration
// =============================================================================

/**
 * Prepare entity data for shredded Variant write
 *
 * Extracts specified fields from entities and prepares them for writing
 * with hyparquet-writer's createShreddedVariantColumn.
 *
 * @param entities - Entities to write
 * @param shredFields - Fields to shred (default: ['$type'])
 * @param columnName - Variant column name (default: '$data')
 * @returns Prepared data for shredded Variant column
 */
export async function prepareShreddedVariantData(
  entities: Entity[],
  shredFields: string[] = [...DEFAULT_DATA_SHRED_FIELDS],
  columnName: string = '$data'
): Promise<ShreddedEntityData> {
  // Dynamically import hyparquet-writer
  const { createShreddedVariantColumn, getStatisticsPaths } = await import('hyparquet-writer')

  // Convert entities to plain objects for shredding
  const objects = entities.map(entity => {
    // Include $type at the top level of the object for shredding
    const obj: Record<string, unknown> = {
      $type: entity.$type,
    }

    // Copy all entity data fields
    for (const [key, value] of Object.entries(entity)) {
      if (key === '$id' || key === '$type') continue // Skip system fields handled separately
      obj[key] = value
    }

    return obj
  })

  // Create shredded Variant column
  const { schema, columnData, shredPaths } = createShreddedVariantColumn(
    columnName,
    objects,
    shredFields,
    { nullable: true }
  )

  // Get statistics paths for predicate pushdown
  const statisticsPaths = getStatisticsPaths(columnName, shredFields)

  return {
    schema,
    columnData,
    statisticsPaths,
  }
}

/**
 * Determine which fields to shred for a type definition
 *
 * Combines explicit $shred directives with auto-detection of hot fields.
 * Always includes $type for efficient type-based filtering.
 *
 * @param typeDef - Type definition (optional)
 * @returns Array of field names to shred
 */
export function getDataShredFields(typeDef?: TypeDefinition): string[] {
  const fields = new Set<string>(['$type']) // Always shred $type

  if (typeDef) {
    // Add fields from type definition
    const typeFields = determineShredFields(typeDef, DEFAULT_SHRED_CONFIG)
    for (const field of typeFields) {
      fields.add(field)
    }
  }

  return [...fields]
}

// =============================================================================
// Reader Integration
// =============================================================================

/**
 * Configuration for reading shredded Variant columns
 */
export interface ShreddedVariantReadConfig {
  /** Variant column name */
  columnName: string
  /** Fields that were shredded */
  shredFields: string[]
}

/**
 * Map a filter path to the Parquet statistics path
 *
 * Transforms user filter paths like '$data.$type' to the actual
 * Parquet column path used for statistics: '$data.typed_value.$type.typed_value'
 *
 * @param filterPath - User filter path (e.g., '$data.$type')
 * @param config - Shredded Variant configuration
 * @returns Parquet statistics path or null if not shredded
 */
export function mapFilterToStatisticsPath(
  filterPath: string,
  config: ShreddedVariantReadConfig
): string | null {
  const { columnName, shredFields } = config

  // Check if path starts with the Variant column name
  if (!filterPath.startsWith(`${columnName}.`)) {
    return null
  }

  // Extract field name from path
  const remainder = filterPath.slice(columnName.length + 1)
  const fieldName = remainder.split('.')[0]

  if (!fieldName || !shredFields.includes(fieldName)) {
    return null
  }

  // Map to Parquet shredding path structure
  return `${columnName}.typed_value.${fieldName}.typed_value`
}

/**
 * Transform filter for shredded Variant pushdown
 *
 * Converts user-facing filter paths to Parquet column paths that
 * can be used with row group statistics.
 *
 * @param filter - User filter
 * @param configs - Shredded Variant configurations
 * @returns Transformed filter with Parquet paths
 */
export function transformFilterForShredding(
  filter: Record<string, unknown>,
  configs: ShreddedVariantReadConfig[]
): Record<string, unknown> {
  const result: Record<string, unknown> = {}

  for (const [key, value] of Object.entries(filter)) {
    // Handle logical operators recursively
    if (key === '$and' || key === '$or' || key === '$nor') {
      result[key] = (value as Record<string, unknown>[]).map(f =>
        transformFilterForShredding(f, configs)
      )
      continue
    }

    if (key === '$not') {
      result[key] = transformFilterForShredding(value as Record<string, unknown>, configs)
      continue
    }

    // Try to map to a shredded statistics path
    let mapped = false
    for (const config of configs) {
      const statsPath = mapFilterToStatisticsPath(key, config)
      if (statsPath) {
        result[statsPath] = value
        mapped = true
        break
      }
    }

    // Pass through unmapped paths unchanged
    if (!mapped) {
      result[key] = value
    }
  }

  return result
}

/**
 * Check if a filter can benefit from shredded Variant pushdown
 *
 * @param filter - Filter to check
 * @param configs - Shredded Variant configurations
 * @returns true if any filter conditions target shredded fields
 */
export function canPushdownWithShredding(
  filter: Record<string, unknown>,
  configs: ShreddedVariantReadConfig[]
): boolean {
  for (const [key, value] of Object.entries(filter)) {
    // Handle logical operators
    if (key === '$and' || key === '$or' || key === '$nor') {
      const subFilters = value as Record<string, unknown>[]
      if (subFilters.some(f => canPushdownWithShredding(f, configs))) {
        return true
      }
      continue
    }

    if (key === '$not') {
      if (canPushdownWithShredding(value as Record<string, unknown>, configs)) {
        return true
      }
      continue
    }

    // Check if this path targets a shredded field
    for (const config of configs) {
      if (mapFilterToStatisticsPath(key, config)) {
        return true
      }
    }
  }

  return false
}

// =============================================================================
// Predicate Pushdown
// =============================================================================

/**
 * Create a row group predicate for shredded field statistics
 *
 * Returns a function that checks if a row group might contain matching values
 * based on min/max statistics.
 *
 * @param condition - Filter condition (value or operator object)
 * @returns Predicate function (min, max) => boolean
 */
export function createShreddedPredicate(
  condition: unknown
): (min: unknown, max: unknown) => boolean {
  // Direct value comparison (equality)
  if (typeof condition !== 'object' || condition === null) {
    return (min, max) => {
      // Value must be within [min, max] range
      return min <= condition && condition <= max
    }
  }

  const ops = condition as Record<string, unknown>

  return (min, max) => {
    // $eq: exact match must be in range
    if ('$eq' in ops) {
      const val = ops.$eq
      if (!(min <= val && val <= max)) return false
    }

    // $in: any value in set must be in range
    if ('$in' in ops && Array.isArray(ops.$in)) {
      if (!ops.$in.some(v => min <= v && v <= max)) return false
    }

    // $gt: max must be > value
    if ('$gt' in ops) {
      if (!(max > ops.$gt)) return false
    }

    // $gte: max must be >= value
    if ('$gte' in ops) {
      if (!(max >= ops.$gte)) return false
    }

    // $lt: min must be < value
    if ('$lt' in ops) {
      if (!(min < ops.$lt)) return false
    }

    // $lte: min must be <= value
    if ('$lte' in ops) {
      if (!(min <= ops.$lte)) return false
    }

    return true
  }
}

/**
 * Check if a row group should be skipped based on shredded field statistics
 *
 * Evaluates each filter condition against the row group's min/max statistics.
 * If any condition is provably unsatisfiable (the predicate returns false),
 * the row group can be safely skipped.
 *
 * @param filter - Filter conditions on shredded paths
 * @param statistics - Row group statistics by column path
 * @returns true if the row group can be safely skipped
 */
export function shouldSkipRowGroup(
  filter: Record<string, unknown>,
  statistics: Map<string, { min: unknown; max: unknown }>
): boolean {
  for (const [path, condition] of Object.entries(filter)) {
    // Skip logical operators (they start with $ but don't contain dots)
    // Column paths like '$data.typed_value.$type.typed_value' are valid paths
    if (path.startsWith('$') && !path.includes('.')) continue

    const stats = statistics.get(path)
    if (!stats) continue // No statistics, can't skip

    // Check if condition is an object (might have operators)
    const predicate = createShreddedPredicate(condition)
    const mightContainMatch = predicate(stats.min, stats.max)

    if (!mightContainMatch) {
      // Row group definitely doesn't contain matching values
      return true
    }
  }

  return false
}

// =============================================================================
// Exports
// =============================================================================

export {
  createShreddedVariantColumn,
  getStatisticsPaths,
  mapFilterPathToStats,
} from 'hyparquet-writer'
