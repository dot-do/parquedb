/**
 * Predicate Pushdown for Parquet Row Group Filtering
 *
 * Converts MongoDB-style filters to Parquet predicates that can prune
 * row groups based on min/max statistics, reducing I/O and improving
 * query performance.
 */

import type {
  Filter,
  FieldOperator,
  ComparisonOperator,
  StringOperator,
  ArrayOperator,
  ExistenceOperator,
  isFieldOperator,
  isComparisonOperator,
  isStringOperator,
  isArrayOperator,
  isExistenceOperator,
} from '../types/filter'
import { deepEqual, compareValues, getNestedValue, getValueType } from '../utils'

// =============================================================================
// Statistics Types
// =============================================================================

/**
 * Statistics from a Parquet row group
 * Used for predicate pushdown to skip row groups that cannot match
 */
export interface RowGroupStats {
  /** Row group index (0-based) */
  rowGroup: number
  /** Number of rows in this row group */
  rowCount: number
  /** Column-level statistics */
  columns: Map<string, ColumnStats>
}

/**
 * Statistics for a single column within a row group
 */
export interface ColumnStats {
  /** Minimum value in the column (undefined if all nulls) */
  min?: unknown
  /** Maximum value in the column (undefined if all nulls) */
  max?: unknown
  /** Number of null values */
  nullCount: number
  /** Number of distinct values (if available) */
  distinctCount?: number
  /** Whether this column has a bloom filter */
  hasBloomFilter: boolean
}

// =============================================================================
// Row Group Selection (Predicate Pushdown)
// =============================================================================

/**
 * Determine which row groups might contain matching rows
 * Uses min/max statistics for pruning
 *
 * @param filter - MongoDB-style filter
 * @param stats - Array of row group statistics
 * @returns Array of row group indices that could match
 */
export function selectRowGroups(
  filter: Filter,
  stats: RowGroupStats[]
): number[] {
  // Empty filter matches all row groups
  if (!filter || Object.keys(filter).length === 0) {
    return stats.map(rg => rg.rowGroup)
  }

  // For each row group, check if filter could possibly match
  return stats
    .filter(rg => couldMatch(filter, rg))
    .map(rg => rg.rowGroup)
}

/**
 * Check if filter could match any rows in row group
 * Returns true if there's any possibility of matching rows
 * Returns false only if we can definitively exclude the row group
 *
 * @param filter - MongoDB-style filter
 * @param stats - Row group statistics
 * @returns true if row group might contain matches
 */
export function couldMatch(filter: Filter, stats: RowGroupStats): boolean {
  // Handle logical operators first
  if (filter.$and) {
    // All conditions must be possible
    return filter.$and.every(subFilter => couldMatch(subFilter, stats))
  }

  if (filter.$or) {
    // At least one condition must be possible
    return filter.$or.some(subFilter => couldMatch(subFilter, stats))
  }

  if (filter.$not) {
    // We can't easily prune for $not, assume match possible
    // (conservative approach - don't miss results)
    return true
  }

  if (filter.$nor) {
    // We can't easily prune for $nor, assume match possible
    return true
  }

  // Handle special operators (text, vector, geo)
  if (filter.$text || filter.$vector || filter.$geo) {
    // These require special indexes, can't prune with stats
    return true
  }

  // Check each field filter
  for (const [field, value] of Object.entries(filter)) {
    // Skip logical operators (already handled)
    if (field.startsWith('$')) continue

    const columnStats = stats.columns.get(field)

    // If we don't have stats for this column, assume it could match
    if (!columnStats) continue

    // Check if the filter could match this column's statistics
    if (!couldMatchColumn(value, columnStats)) {
      return false // Definitely no match in this row group
    }
  }

  return true // Could potentially match
}

/**
 * Check if a field filter could match given column statistics
 *
 * @param value - Field filter value (direct value or operator)
 * @param stats - Column statistics
 * @returns true if the filter could match values in this column
 */
function couldMatchColumn(value: unknown, stats: ColumnStats): boolean {
  // Handle null/undefined
  if (value === null || value === undefined) {
    // Looking for null values - check if column has any nulls
    return stats.nullCount > 0
  }

  // Handle operator objects
  if (typeof value === 'object' && value !== null) {
    return couldMatchOperator(value as Record<string, unknown>, stats)
  }

  // Direct equality - value must be between min and max
  return isValueInRange(value, stats.min, stats.max)
}

/**
 * Check if an operator filter could match given column statistics
 */
function couldMatchOperator(op: Record<string, unknown>, stats: ColumnStats): boolean {
  const { min, max, nullCount } = stats

  // $eq: value must be between min and max
  if ('$eq' in op) {
    if (op.$eq === null) {
      return nullCount > 0
    }
    return isValueInRange(op.$eq, min, max)
  }

  // $ne: unless all values equal the excluded value, could match
  if ('$ne' in op) {
    // If column has any nulls and we're excluding non-null, could match
    if (op.$ne !== null && nullCount > 0) return true
    // If min === max and both equal the excluded value, no match
    if (min !== undefined && max !== undefined && min === max && min === op.$ne) {
      return false
    }
    return true
  }

  // $gt: value must be < max
  if ('$gt' in op) {
    if (max === undefined) return false
    return compareValues(op.$gt, max) < 0
  }

  // $gte: value must be <= max
  if ('$gte' in op) {
    if (max === undefined) return false
    return compareValues(op.$gte, max) <= 0
  }

  // $lt: value must be > min
  if ('$lt' in op) {
    if (min === undefined) return false
    return compareValues(op.$lt, min) > 0
  }

  // $lte: value must be >= min
  if ('$lte' in op) {
    if (min === undefined) return false
    return compareValues(op.$lte, min) >= 0
  }

  // $in: at least one value must be in range
  if ('$in' in op) {
    const values = op.$in as unknown[]
    return values.some(v => {
      if (v === null) return nullCount > 0
      return isValueInRange(v, min, max)
    })
  }

  // $nin: unless all possible values are in the exclusion list, could match
  if ('$nin' in op) {
    // Conservative: assume could match unless proven otherwise
    return true
  }

  // $exists: check null count
  if ('$exists' in op) {
    if (op.$exists === true) {
      // Must have at least one non-null value
      // We don't have total count, so assume could match
      return true
    } else {
      // Must have at least one null value
      return nullCount > 0
    }
  }

  // $type: can't easily prune with min/max stats
  if ('$type' in op) {
    return true
  }

  // String operators ($regex, $startsWith, $endsWith, $contains)
  // These require actual value inspection, but we can sometimes prune with prefix matching
  if ('$regex' in op || '$startsWith' in op || '$endsWith' in op || '$contains' in op) {
    // For $startsWith, we could potentially prune if min/max are strings
    if ('$startsWith' in op && typeof min === 'string' && typeof max === 'string') {
      const prefix = op.$startsWith as string
      // If prefix > max or prefix prefix doesn't overlap [min, max], no match
      // This is a simplified check
      if (!stringPrefixCouldMatch(prefix, min, max)) {
        return false
      }
    }
    return true
  }

  // Array operators ($all, $elemMatch, $size)
  // These require inspecting actual values, can't prune with stats
  if ('$all' in op || '$elemMatch' in op || '$size' in op) {
    return true
  }

  // Unknown operator - assume could match (conservative)
  return true
}

/**
 * Check if a value is within the range [min, max]
 */
function isValueInRange(value: unknown, min: unknown, max: unknown): boolean {
  // If no stats available, assume in range
  if (min === undefined && max === undefined) return true

  // Check lower bound
  if (min !== undefined && compareValues(value, min) < 0) {
    return false
  }

  // Check upper bound
  if (max !== undefined && compareValues(value, max) > 0) {
    return false
  }

  return true
}

// compareValues is imported from ../utils

/**
 * Check if a string prefix could match any value in [min, max]
 */
function stringPrefixCouldMatch(prefix: string, min: string, max: string): boolean {
  // If prefix is alphabetically after max, no match possible
  if (prefix > max) {
    // But only if the prefix start is after max
    // e.g., prefix "z" can't match if max is "a"
    if (prefix.slice(0, Math.min(prefix.length, max.length)) > max) {
      return false
    }
  }

  // If a string starting with prefix would be before min, check more carefully
  // e.g., prefix "a" could match any string starting with "a"
  // If min is "b...", then "a..." < "b...", so no match
  if (prefix < min.slice(0, prefix.length)) {
    return false
  }

  return true
}

// =============================================================================
// Predicate Function Builder
// =============================================================================

/**
 * Convert filter to a predicate function for in-memory row filtering
 * This is used after row groups are selected to filter individual rows
 *
 * @param filter - MongoDB-style filter
 * @returns Predicate function that returns true for matching rows
 */
export function toPredicate(filter: Filter): (row: unknown) => boolean {
  // Empty filter matches everything
  if (!filter || Object.keys(filter).length === 0) {
    return () => true
  }

  // Build the predicate
  return (row: unknown) => evaluateFilter(filter, row)
}

/**
 * Evaluate a filter against a row
 */
function evaluateFilter(filter: Filter, row: unknown): boolean {
  // Handle null row
  if (row === null || row === undefined) {
    return false
  }

  const obj = row as Record<string, unknown>

  // Handle logical operators
  if (filter.$and) {
    return filter.$and.every(subFilter => evaluateFilter(subFilter, row))
  }

  if (filter.$or) {
    return filter.$or.some(subFilter => evaluateFilter(subFilter, row))
  }

  if (filter.$not) {
    return !evaluateFilter(filter.$not, row)
  }

  if (filter.$nor) {
    return !filter.$nor.some(subFilter => evaluateFilter(subFilter, row))
  }

  // Handle special operators (text, vector, geo) - these need special handling
  // For now, return true and let specialized code handle them
  if (filter.$text || filter.$vector || filter.$geo) {
    // These should be handled by specialized indexes
    // If we get here, assume they passed (or the index already filtered)
    return true
  }

  // Check each field filter
  for (const [field, value] of Object.entries(filter)) {
    // Skip logical operators (already handled) and special operators
    if (field.startsWith('$')) continue

    // Get the field value (supports nested fields with dot notation)
    const fieldValue = getNestedValue(obj, field)

    // Evaluate the field condition
    if (!evaluateFieldCondition(fieldValue, value)) {
      return false
    }
  }

  return true
}

// getNestedValue is imported from ../utils

/**
 * Evaluate a field condition against a value
 */
function evaluateFieldCondition(fieldValue: unknown, condition: unknown): boolean {
  // Null/undefined condition - match nullish values
  if (condition === null) {
    return fieldValue === null || fieldValue === undefined
  }

  if (condition === undefined) {
    return true // undefined means no constraint
  }

  // Operator object
  if (typeof condition === 'object' && condition !== null && !Array.isArray(condition) && !(condition instanceof Date)) {
    return evaluateOperators(fieldValue, condition as Record<string, unknown>)
  }

  // Direct equality (handles primitives, dates, arrays)
  return deepEqual(fieldValue, condition)
}

/**
 * Evaluate operator conditions
 */
function evaluateOperators(fieldValue: unknown, operators: Record<string, unknown>): boolean {
  for (const [op, opValue] of Object.entries(operators)) {
    switch (op) {
      // Comparison operators
      case '$eq':
        if (!deepEqual(fieldValue, opValue)) return false
        break

      case '$ne':
        if (deepEqual(fieldValue, opValue)) return false
        break

      case '$gt':
        if (fieldValue === null || fieldValue === undefined) return false
        if (compareValues(fieldValue, opValue) <= 0) return false
        break

      case '$gte':
        if (fieldValue === null || fieldValue === undefined) return false
        if (compareValues(fieldValue, opValue) < 0) return false
        break

      case '$lt':
        if (fieldValue === null || fieldValue === undefined) return false
        if (compareValues(fieldValue, opValue) >= 0) return false
        break

      case '$lte':
        if (fieldValue === null || fieldValue === undefined) return false
        if (compareValues(fieldValue, opValue) > 0) return false
        break

      case '$in':
        if (!Array.isArray(opValue)) return false
        if (!opValue.some(v => deepEqual(fieldValue, v))) return false
        break

      case '$nin':
        if (!Array.isArray(opValue)) return false
        if (opValue.some(v => deepEqual(fieldValue, v))) return false
        break

      // String operators
      case '$regex': {
        if (typeof fieldValue !== 'string') return false
        const pattern = opValue instanceof RegExp
          ? opValue
          : new RegExp(opValue as string, (operators.$options as string) || '')
        if (!pattern.test(fieldValue)) return false
        break
      }

      case '$options':
        // Handled with $regex
        break

      case '$startsWith':
        if (typeof fieldValue !== 'string') return false
        if (!fieldValue.startsWith(opValue as string)) return false
        break

      case '$endsWith':
        if (typeof fieldValue !== 'string') return false
        if (!fieldValue.endsWith(opValue as string)) return false
        break

      case '$contains':
        if (typeof fieldValue !== 'string') return false
        if (!fieldValue.includes(opValue as string)) return false
        break

      // Array operators
      case '$all': {
        if (!Array.isArray(fieldValue)) return false
        const required = opValue as unknown[]
        if (!required.every(v => fieldValue.some(fv => deepEqual(fv, v)))) return false
        break
      }

      case '$elemMatch': {
        if (!Array.isArray(fieldValue)) return false
        const subFilter = opValue as Filter
        if (!fieldValue.some(elem => evaluateFilter(subFilter, elem))) return false
        break
      }

      case '$size':
        if (!Array.isArray(fieldValue)) return false
        if (fieldValue.length !== opValue) return false
        break

      // Existence operators
      case '$exists':
        if (opValue === true) {
          if (fieldValue === undefined) return false
        } else {
          if (fieldValue !== undefined) return false
        }
        break

      case '$type': {
        const actualType = getValueType(fieldValue)
        if (actualType !== opValue) return false
        break
      }

      default:
        // Unknown operator - ignore (or could throw)
        break
    }
  }

  return true
}

// getValueType is imported from ../utils

// deepEqual is imported from ../utils

// =============================================================================
// Field Extraction
// =============================================================================

/**
 * Extract all field names used in a filter (for column projection)
 * This allows reading only the necessary columns from Parquet files
 *
 * @param filter - MongoDB-style filter
 * @returns Array of field names used in the filter
 */
export function extractFilterFields(filter: Filter): string[] {
  const fields = new Set<string>()

  extractFieldsRecursive(filter, fields)

  return Array.from(fields)
}

/**
 * Recursively extract field names from a filter
 */
function extractFieldsRecursive(filter: Filter, fields: Set<string>): void {
  if (!filter || typeof filter !== 'object') return

  // Handle logical operators
  if (filter.$and) {
    filter.$and.forEach(subFilter => extractFieldsRecursive(subFilter, fields))
  }

  if (filter.$or) {
    filter.$or.forEach(subFilter => extractFieldsRecursive(subFilter, fields))
  }

  if (filter.$not) {
    extractFieldsRecursive(filter.$not, fields)
  }

  if (filter.$nor) {
    filter.$nor.forEach(subFilter => extractFieldsRecursive(subFilter, fields))
  }

  // Handle special operators
  if (filter.$text) {
    // Text search may need all text-indexed fields
    // We can't know which fields without schema, so skip
  }

  if (filter.$vector) {
    const vectorOp = filter.$vector as { $field: string }
    if (vectorOp.$field) {
      fields.add(vectorOp.$field)
    }
  }

  // Extract regular field names
  for (const [field, value] of Object.entries(filter)) {
    // Skip operators
    if (field.startsWith('$')) continue

    // Add the field name (could be dot-notation for nested)
    fields.add(field)

    // Also add root field for nested fields
    const rootField = field.split('.')[0]
    if (rootField !== field) {
      fields.add(rootField)
    }

    // Handle $elemMatch which contains a nested filter
    if (value && typeof value === 'object' && '$elemMatch' in value) {
      extractFieldsRecursive((value as { $elemMatch: Filter }).$elemMatch, fields)
    }
  }
}

// =============================================================================
// Statistics Extraction from Parquet Metadata
// =============================================================================

/**
 * Extract row group statistics from Parquet file metadata
 * This is used by QueryExecutor to get stats for predicate pushdown
 *
 * @param metadata - Parquet file metadata (from ParquetReader)
 * @returns Array of row group statistics
 */
export function extractRowGroupStats(metadata: ParquetMetadata): RowGroupStats[] {
  return metadata.rowGroups.map((rg, index) => ({
    rowGroup: index,
    rowCount: rg.numRows,
    columns: extractColumnStats(rg.columns),
  }))
}

/**
 * Extract column statistics from row group metadata
 */
function extractColumnStats(columns: ParquetColumnChunk[]): Map<string, ColumnStats> {
  const statsMap = new Map<string, ColumnStats>()

  for (const column of columns) {
    const stats: ColumnStats = {
      min: column.statistics?.min,
      max: column.statistics?.max,
      nullCount: column.statistics?.nullCount ?? 0,
      distinctCount: column.statistics?.distinctCount,
      hasBloomFilter: column.hasBloomFilter ?? false,
    }
    statsMap.set(column.path.join('.'), stats)
  }

  return statsMap
}

// =============================================================================
// Parquet Metadata Types (for type safety)
// =============================================================================

/** Parquet file metadata structure */
export interface ParquetMetadata {
  /** Schema information */
  schema: ParquetSchemaElement[]
  /** Row group metadata */
  rowGroups: ParquetRowGroup[]
  /** Total number of rows */
  numRows: number
  /** Key-value metadata */
  keyValueMetadata?: Array<{ key: string; value: string }>
}

/** Row group metadata */
export interface ParquetRowGroup {
  /** Column chunks in this row group */
  columns: ParquetColumnChunk[]
  /** Total byte size */
  totalByteSize: number
  /** Number of rows in this row group */
  numRows: number
}

/** Column chunk metadata */
export interface ParquetColumnChunk {
  /** Column path (for nested columns) */
  path: string[]
  /** Encoding types used */
  encodings: string[]
  /** Compression codec */
  compression: string
  /** Byte offset to column data */
  offset: number
  /** Compressed size in bytes */
  compressedSize: number
  /** Uncompressed size in bytes */
  uncompressedSize: number
  /** Column statistics */
  statistics?: ParquetColumnStatistics
  /** Whether this column has a bloom filter */
  hasBloomFilter?: boolean
  /** Offset to bloom filter data */
  bloomFilterOffset?: number
}

/** Column statistics */
export interface ParquetColumnStatistics {
  /** Minimum value */
  min?: unknown
  /** Maximum value */
  max?: unknown
  /** Number of null values */
  nullCount?: number
  /** Number of distinct values */
  distinctCount?: number
}

/** Schema element */
export interface ParquetSchemaElement {
  /** Element name */
  name: string
  /** Element type */
  type?: string
  /** Type length (for fixed-length types) */
  typeLength?: number
  /** Repetition type */
  repetition?: 'REQUIRED' | 'OPTIONAL' | 'REPEATED'
  /** Number of children (for groups) */
  numChildren?: number
}
