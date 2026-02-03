/**
 * Predicate Pushdown Utilities for IcebergBackend
 *
 * Provides predicate pushdown capabilities for Iceberg tables:
 * 1. Manifest-level filtering using partition bounds (when available)
 * 2. Row group statistics filtering using Parquet column min/max
 * 3. Projection pushdown to read only needed columns
 * 4. Limit pushdown for early termination
 *
 * @see https://iceberg.apache.org/spec/#scan-planning
 */

import type { Filter } from '../types/filter'
import type { FindOptions } from '../types/options'
import type { ManifestEntry, ManifestFile } from '@dotdo/iceberg'
import type { ParquetMetadata, RowGroupMetadata, ColumnStatistics } from '../parquet/types'

// =============================================================================
// Types
// =============================================================================

/**
 * Predicate for evaluating against statistics
 */
export interface StatisticsPredicate {
  column: string
  op: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'in'
  value: unknown
}

/**
 * Result of predicate extraction
 */
export interface PredicateExtractionResult {
  /** Predicates that can be evaluated against statistics */
  predicates: StatisticsPredicate[]
  /** Whether any predicates were extracted */
  hasPushdown: boolean
  /** Columns involved in predicates */
  predicateColumns: string[]
}

/**
 * Columns required for entity storage
 */
export const ENTITY_CORE_COLUMNS = [
  '$id',
  '$type',
  'name',
  'createdAt',
  'createdBy',
  'updatedAt',
  'updatedBy',
  'deletedAt',
  'deletedBy',
  'version',
  '$data',
] as const

// =============================================================================
// Filter to Predicates Conversion
// =============================================================================

/**
 * Extract statistics predicates from a MongoDB-style filter
 *
 * Only extracts predicates that can be evaluated against column statistics:
 * - Direct equality: { field: value }
 * - Comparison operators: $eq, $ne, $gt, $gte, $lt, $lte
 * - In operator: $in
 *
 * Complex operators ($regex, $elemMatch, etc.) cannot use statistics.
 *
 * @param filter - MongoDB-style filter
 * @returns Extracted predicates and metadata
 */
export function extractPredicatesFromFilter(filter?: Filter): PredicateExtractionResult {
  const predicates: StatisticsPredicate[] = []

  if (!filter || typeof filter !== 'object') {
    return { predicates: [], hasPushdown: false, predicateColumns: [] }
  }

  for (const [key, value] of Object.entries(filter)) {
    // Skip logical operators at this level
    if (key === '$and' || key === '$or' || key === '$not' || key === '$nor') {
      // Handle $and recursively - all conditions must match
      if (key === '$and' && Array.isArray(value)) {
        for (const subFilter of value) {
          const subResult = extractPredicatesFromFilter(subFilter as Filter)
          predicates.push(...subResult.predicates)
        }
      }
      // $or, $not, $nor cannot be used for statistics-based skipping
      continue
    }

    // Skip special operators
    if (key === '$text' || key === '$vector' || key === '$geo') {
      continue
    }

    // Skip nested fields (require Variant access)
    if (key.includes('.') && !key.startsWith('$')) {
      continue
    }

    // Extract predicates from the value
    const columnPredicates = extractColumnPredicates(key, value)
    predicates.push(...columnPredicates)
  }

  const predicateColumns = [...new Set(predicates.map(p => p.column))]

  return {
    predicates,
    hasPushdown: predicates.length > 0,
    predicateColumns,
  }
}

/**
 * Extract predicates for a single column
 */
function extractColumnPredicates(column: string, value: unknown): StatisticsPredicate[] {
  const predicates: StatisticsPredicate[] = []

  // Handle null/undefined - can't push down
  if (value === null || value === undefined) {
    return predicates
  }

  // Handle operator objects
  if (typeof value === 'object' && !Array.isArray(value) && !(value instanceof Date)) {
    const operators = value as Record<string, unknown>

    if ('$eq' in operators) {
      predicates.push({ column, op: 'eq', value: operators.$eq })
    }
    if ('$ne' in operators) {
      predicates.push({ column, op: 'ne', value: operators.$ne })
    }
    if ('$gt' in operators) {
      predicates.push({ column, op: 'gt', value: operators.$gt })
    }
    if ('$gte' in operators) {
      predicates.push({ column, op: 'gte', value: operators.$gte })
    }
    if ('$lt' in operators) {
      predicates.push({ column, op: 'lt', value: operators.$lt })
    }
    if ('$lte' in operators) {
      predicates.push({ column, op: 'lte', value: operators.$lte })
    }
    if ('$in' in operators && Array.isArray(operators.$in)) {
      predicates.push({ column, op: 'in', value: operators.$in })
    }

    return predicates
  }

  // Direct value = implicit equality
  predicates.push({ column, op: 'eq', value })

  return predicates
}

// =============================================================================
// Row Group Statistics Filtering
// =============================================================================

/**
 * Check if a row group can be skipped based on predicates and statistics
 *
 * Uses min/max statistics to determine if a row group could possibly
 * contain matching rows. If it definitely cannot, returns true to skip.
 *
 * @param rowGroup - Row group metadata with column statistics
 * @param predicates - Predicates to evaluate
 * @returns true if the row group can be safely skipped
 */
export function canSkipRowGroup(
  rowGroup: RowGroupMetadata,
  predicates: StatisticsPredicate[]
): boolean {
  // If no predicates, cannot skip
  if (predicates.length === 0) {
    return false
  }

  // All predicates must pass for the row group to be included
  // If any predicate indicates the row group cannot match, we can skip it
  for (const predicate of predicates) {
    const columnMeta = findColumnMetadata(rowGroup, predicate.column)
    if (!columnMeta?.statistics) {
      // No statistics for this column, cannot skip
      continue
    }

    if (canSkipByStatistics(predicate, columnMeta.statistics)) {
      return true
    }
  }

  return false
}

/**
 * Find column metadata by name in a row group
 */
function findColumnMetadata(
  rowGroup: RowGroupMetadata,
  columnName: string
): { statistics?: ColumnStatistics } | undefined {
  return rowGroup.columns.find(
    col =>
      col.pathInSchema.join('.') === columnName ||
      col.pathInSchema[col.pathInSchema.length - 1] === columnName
  )
}

/**
 * Check if a predicate indicates the statistics range cannot contain matching values
 *
 * @param predicate - The predicate to evaluate
 * @param stats - Column statistics (min/max values)
 * @returns true if the predicate definitely won't match any values in this range
 */
function canSkipByStatistics(
  predicate: StatisticsPredicate,
  stats: ColumnStatistics
): boolean {
  const { min, max } = stats

  // Cannot skip without min/max statistics
  if (min === undefined || max === undefined || min === null || max === null) {
    return false
  }

  const filterVal = predicate.value

  // Handle different predicate types
  switch (predicate.op) {
    case 'eq':
      // Skip if value is outside [min, max] range
      // IMPORTANT: Parquet often truncates string statistics to save space.
      // When comparing strings, we must account for truncation:
      // - If filterVal is shorter than or equal to min/max length, direct comparison is safe
      // - If filterVal is longer, we must use prefix comparison to avoid false positives
      return compareValuesForSkip(filterVal, min, 'lt') || compareValuesForSkip(filterVal, max, 'gt')

    case 'ne':
      // Can only skip if min === max === excluded value
      // (all values in range are the same and equal to excluded value)
      return compareValues(min, max) === 0 && compareValues(min, filterVal) === 0

    case 'gt':
      // Skip if max <= filter value (no value in range can be > filter value)
      // Use conservative comparison for potentially truncated max
      return compareValuesForSkip(max, filterVal, 'lte')

    case 'gte':
      // Skip if max < filter value
      return compareValuesForSkip(max, filterVal, 'lt')

    case 'lt':
      // Skip if min >= filter value (no value in range can be < filter value)
      return compareValuesForSkip(min, filterVal, 'gte')

    case 'lte':
      // Skip if min > filter value
      return compareValuesForSkip(min, filterVal, 'gt')

    case 'in':
      // Skip if no value in the set could be in [min, max] range
      if (!Array.isArray(filterVal)) return false
      return filterVal.every(
        v => compareValuesForSkip(v, min, 'lt') || compareValuesForSkip(v, max, 'gt')
      )

    default:
      return false
  }
}

/**
 * Compare values for skip decision, handling truncated string statistics
 *
 * Parquet truncates string min/max statistics to save space. This means:
 * - The stored min might be a prefix of the actual minimum value
 * - The stored max might be a prefix of the actual maximum value
 *
 * For skip decisions, we must be conservative:
 * - When checking if filterVal < min (for skipping): only skip if filterVal < min prefix
 * - When checking if filterVal > max (for skipping): only skip if filterVal's prefix > max
 *   (because if filterVal starts with max, it might still be within range)
 *
 * @param a - First value (filter value)
 * @param b - Second value (statistic bound, possibly truncated)
 * @param op - Comparison operator for skip decision
 * @returns true if the skip condition is definitely met
 */
function compareValuesForSkip(a: unknown, b: unknown, op: 'lt' | 'gt' | 'lte' | 'gte'): boolean {
  // For strings, handle potential truncation
  if (typeof a === 'string' && typeof b === 'string') {
    // If the statistic (b) is shorter, it may be truncated
    // We need to be conservative in our comparison
    if (b.length < a.length) {
      // b might be truncated - use prefix comparison
      const aPrefix = a.substring(0, b.length)

      switch (op) {
        case 'lt':
          // Can only skip if a's prefix < b (meaning a definitely < any value starting with b)
          return aPrefix < b
        case 'lte':
          // Can only skip if a's prefix < b
          return aPrefix < b
        case 'gt':
          // Can only skip if a's prefix > b (meaning a definitely > any value starting with b)
          return aPrefix > b
        case 'gte':
          // Can only skip if a's prefix > b
          return aPrefix > b
      }
    }

    // No truncation - standard comparison
    const cmp = a.localeCompare(b)
    switch (op) {
      case 'lt':
        return cmp < 0
      case 'lte':
        return cmp <= 0
      case 'gt':
        return cmp > 0
      case 'gte':
        return cmp >= 0
    }
  }

  // For non-strings, use standard comparison
  const cmp = compareValues(a, b)
  switch (op) {
    case 'lt':
      return cmp < 0
    case 'lte':
      return cmp <= 0
    case 'gt':
      return cmp > 0
    case 'gte':
      return cmp >= 0
  }
}

/**
 * Compare two values for ordering
 * Returns negative if a < b, 0 if a === b, positive if a > b
 */
function compareValues(a: unknown, b: unknown): number {
  // Handle null/undefined
  if (a === null || a === undefined) {
    return b === null || b === undefined ? 0 : -1
  }
  if (b === null || b === undefined) {
    return 1
  }

  // Handle numbers
  if (typeof a === 'number' && typeof b === 'number') {
    return a - b
  }

  // Handle strings
  if (typeof a === 'string' && typeof b === 'string') {
    return a.localeCompare(b)
  }

  // Handle dates
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime()
  }

  // Handle BigInt
  if (typeof a === 'bigint' && typeof b === 'bigint') {
    return a < b ? -1 : a > b ? 1 : 0
  }

  // Fallback: convert to string
  return String(a).localeCompare(String(b))
}

// =============================================================================
// Manifest File Filtering (for partitioned tables)
// =============================================================================

/**
 * Check if a manifest file can be skipped based on predicates
 *
 * Note: This currently returns false for unpartitioned tables since they
 * don't have partition-level statistics in manifests. For partitioned tables,
 * this would check partition bounds against predicates.
 *
 * @param manifest - Manifest file metadata
 * @param predicates - Predicates to evaluate
 * @returns true if the manifest can be safely skipped
 */
export function canSkipManifest(
  _manifest: ManifestFile,
  _predicates: StatisticsPredicate[]
): boolean {
  // For unpartitioned tables, we cannot skip manifests based on predicates
  // Partition filtering would require:
  // 1. Knowing the partition spec
  // 2. Having partition bounds in the manifest (partitions field)
  // 3. Evaluating predicates against partition columns

  // TODO: Implement partition-level filtering for partitioned tables
  return false
}

/**
 * Default field ID to column name mapping for entity schema
 * Used to decode lower/upper bounds from manifest entries
 */
const ENTITY_FIELD_ID_TO_NAME: Record<number, string> = {
  1: '$id',
  2: '$type',
  3: 'name',
  4: 'createdAt',
  5: 'createdBy',
  6: 'updatedAt',
  7: 'updatedBy',
  8: 'deletedAt',
  9: 'deletedBy',
  10: 'version',
  11: '$data',
}

/**
 * Default column name to field ID mapping for entity schema
 */
const ENTITY_COLUMN_TO_FIELD_ID: Record<string, number> = {
  '$id': 1,
  '$type': 2,
  'name': 3,
  'createdAt': 4,
  'createdBy': 5,
  'updatedAt': 6,
  'updatedBy': 7,
  'deletedAt': 8,
  'deletedBy': 9,
  'version': 10,
  '$data': 11,
}

/**
 * Decode a binary bound value from Iceberg manifest
 *
 * Iceberg encodes values as binary using its own encoding scheme:
 * - Strings: UTF-8 bytes
 * - Integers: 4 bytes little-endian
 * - Longs: 8 bytes little-endian
 * - Timestamps: 8 bytes little-endian (microseconds since epoch)
 *
 * @param bound - Binary bound value (Uint8Array or already decoded string)
 * @param fieldId - Field ID to determine type
 * @returns Decoded value
 */
function decodeBoundValue(bound: Uint8Array | string, fieldId: number): unknown {
  // Already decoded as string
  if (typeof bound === 'string') {
    return bound
  }

  // Determine type based on field ID
  const columnName = ENTITY_FIELD_ID_TO_NAME[fieldId]

  switch (columnName) {
    case '$id':
    case '$type':
    case 'name':
    case 'createdBy':
    case 'updatedBy':
    case 'deletedBy':
      // String fields - UTF-8 decode
      return new TextDecoder().decode(bound)

    case 'version':
      // Integer field - 4 bytes little-endian
      if (bound.length >= 4) {
        return bound[0]! | (bound[1]! << 8) | (bound[2]! << 16) | (bound[3]! << 24)
      }
      return undefined

    case 'createdAt':
    case 'updatedAt':
    case 'deletedAt':
      // Timestamp fields - 8 bytes little-endian (microseconds since epoch)
      if (bound.length >= 8) {
        const view = new DataView(bound.buffer, bound.byteOffset, bound.byteLength)
        const microseconds = view.getBigInt64(0, true)
        return new Date(Number(microseconds / 1000n))
      }
      return undefined

    default:
      // Try UTF-8 decode for unknown fields (most custom fields are strings)
      try {
        return new TextDecoder().decode(bound)
      } catch {
        return undefined
      }
  }
}

/**
 * Check if a manifest entry can be skipped based on predicates
 *
 * Manifest entries contain data file metadata including:
 * - lower_bounds / upper_bounds (column min/max per file)
 * - null_value_counts
 *
 * Uses lower/upper bounds from the data file statistics when available
 * to skip entire data files that cannot contain matching rows.
 *
 * @param entry - Manifest entry
 * @param predicates - Predicates to evaluate
 * @returns true if the data file can be safely skipped
 */
export function canSkipManifestEntry(
  entry: ManifestEntry,
  predicates: StatisticsPredicate[]
): boolean {
  // If no predicates, cannot skip
  if (predicates.length === 0) {
    return false
  }

  const dataFile = entry['data-file']
  const lowerBounds = dataFile['lower-bounds']
  const upperBounds = dataFile['upper-bounds']

  // If no bounds available, cannot skip
  if (!lowerBounds || !upperBounds) {
    return false
  }

  // Check each predicate against bounds
  for (const predicate of predicates) {
    const fieldId = ENTITY_COLUMN_TO_FIELD_ID[predicate.column]
    if (!fieldId) {
      // Unknown column, cannot use for pushdown
      continue
    }

    const lowerBound = lowerBounds[fieldId]
    const upperBound = upperBounds[fieldId]

    if (lowerBound === undefined || upperBound === undefined) {
      // No bounds for this column, cannot skip based on this predicate
      continue
    }

    // Decode binary bounds to comparable values
    const min = decodeBoundValue(lowerBound, fieldId)
    const max = decodeBoundValue(upperBound, fieldId)

    if (min === undefined || max === undefined) {
      continue
    }

    // Use the existing statistics check logic
    if (canSkipByBounds(predicate, min, max)) {
      return true
    }
  }

  return false
}

/**
 * Check if a predicate indicates the bounds range cannot contain matching values
 *
 * @param predicate - The predicate to evaluate
 * @param min - Minimum bound value
 * @param max - Maximum bound value
 * @returns true if the predicate definitely won't match any values in this range
 */
function canSkipByBounds(
  predicate: StatisticsPredicate,
  min: unknown,
  max: unknown
): boolean {
  const filterVal = predicate.value

  switch (predicate.op) {
    case 'eq':
      // Skip if value is outside [min, max] range
      // Use conservative comparison for potentially truncated bounds
      return compareValuesForSkip(filterVal, min, 'lt') || compareValuesForSkip(filterVal, max, 'gt')

    case 'ne':
      // Can only skip if min === max === excluded value
      return compareValues(min, max) === 0 && compareValues(min, filterVal) === 0

    case 'gt':
      // Skip if max <= filter value (no value in range can be > filter value)
      return compareValuesForSkip(max, filterVal, 'lte')

    case 'gte':
      // Skip if max < filter value
      return compareValuesForSkip(max, filterVal, 'lt')

    case 'lt':
      // Skip if min >= filter value (no value in range can be < filter value)
      return compareValuesForSkip(min, filterVal, 'gte')

    case 'lte':
      // Skip if min > filter value
      return compareValuesForSkip(min, filterVal, 'gt')

    case 'in':
      // Skip if no value in the set could be in [min, max] range
      if (!Array.isArray(filterVal)) return false
      return filterVal.every(
        v => compareValuesForSkip(v, min, 'lt') || compareValuesForSkip(v, max, 'gt')
      )

    default:
      return false
  }
}

// =============================================================================
// Projection Pushdown
// =============================================================================

/**
 * Get the columns needed for a query
 *
 * For entity storage, we always need the core entity columns.
 * If the filter references additional columns that might be shredded,
 * we include those as well.
 *
 * @param filter - Query filter
 * @param options - Query options
 * @param shredColumns - Set of columns that are shredded (stored as native Parquet columns)
 * @returns Array of column names to read
 */
export function getRequiredColumns(
  filter?: Filter,
  options?: FindOptions,
  shredColumns?: Set<string>
): string[] {
  // Always need core entity columns for reconstruction
  const columns = new Set<string>(ENTITY_CORE_COLUMNS)

  // If we have shredded columns, check if filter uses them
  if (shredColumns && filter) {
    const { predicateColumns } = extractPredicatesFromFilter(filter)
    for (const col of predicateColumns) {
      if (shredColumns.has(col)) {
        columns.add(col)
      }
    }
  }

  // If projection is requested in options, we still need core columns
  // but might add specific shredded columns
  if (options?.project && shredColumns) {
    for (const col of Object.keys(options.project)) {
      if (shredColumns.has(col)) {
        columns.add(col)
      }
    }
  }

  return Array.from(columns)
}

// =============================================================================
// Limit Pushdown
// =============================================================================

/**
 * Calculate effective limit considering skip and actual limit
 *
 * For limit pushdown, we need to read at least skip + limit rows
 * before we can return results. However, due to deduplication
 * (same entity ID with different versions), we may need more.
 *
 * @param options - Query options
 * @returns Effective read limit, or undefined for no limit
 */
export function calculateReadLimit(options?: FindOptions): number | undefined {
  if (!options?.limit) {
    return undefined
  }

  const skip = options.skip ?? 0
  const limit = options.limit

  // We need to read at least skip + limit rows
  // Add a buffer for deduplication (entities with multiple versions)
  // This is a heuristic - in practice, most entities have 1-2 versions
  const deduplicationBuffer = Math.ceil((skip + limit) * 0.2)

  return skip + limit + deduplicationBuffer
}

/**
 * Check if we've collected enough entities to satisfy the query
 *
 * Used for early termination when reading from multiple data files.
 * Note: This is an optimization hint, not a guarantee, because:
 * 1. We may need to read all files to get the latest version of each entity
 * 2. Sorting requires all matching entities
 *
 * @param collectedCount - Number of unique entities collected so far
 * @param options - Query options
 * @returns true if we might have enough (can stop reading more files)
 */
export function hasEnoughForLimit(
  collectedCount: number,
  options?: FindOptions
): boolean {
  // If we have sorting, we need all entities (can't early terminate)
  if (options?.sort && Object.keys(options.sort).length > 0) {
    return false
  }

  const requiredCount = calculateReadLimit(options)
  if (!requiredCount) {
    return false
  }

  return collectedCount >= requiredCount
}

// =============================================================================
// Statistics Summary
// =============================================================================

/**
 * Statistics about predicate pushdown effectiveness
 */
export interface PushdownStats {
  /** Total manifest files considered */
  totalManifests: number
  /** Manifest files skipped by predicate pushdown */
  skippedManifests: number
  /** Total data files considered */
  totalDataFiles: number
  /** Data files skipped by predicate pushdown */
  skippedDataFiles: number
  /** Total row groups considered */
  totalRowGroups: number
  /** Row groups skipped by predicate pushdown */
  skippedRowGroups: number
  /** Columns projected (vs all columns) */
  projectedColumns: number
  /** Whether limit pushdown was applied */
  limitPushdown: boolean
}

/**
 * Create initial pushdown stats
 */
export function createPushdownStats(): PushdownStats {
  return {
    totalManifests: 0,
    skippedManifests: 0,
    totalDataFiles: 0,
    skippedDataFiles: 0,
    totalRowGroups: 0,
    skippedRowGroups: 0,
    projectedColumns: 0,
    limitPushdown: false,
  }
}

// =============================================================================
// Shredded Field Predicate Pushdown
// =============================================================================

/**
 * Configuration for shredded predicate pushdown
 */
export interface ShreddedPushdownConfig {
  /** Map of namespace to shred configuration */
  shredding: Record<string, import('./types').VariantShredConfig>
}

/**
 * Result of extracting predicates with shredding support
 */
export interface ShreddedPredicateResult extends PredicateExtractionResult {
  /** Predicates on shredded fields (can use statistics) */
  shreddedPredicates: StatisticsPredicate[]
  /** Predicates that target data inside $data Variant (need row-level eval) */
  variantPredicates: VariantPredicate[]
  /** Whether any shredded predicates were extracted */
  hasShreddedPushdown: boolean
}

/**
 * Predicate that targets a field inside the $data Variant column
 */
export interface VariantPredicate {
  /** Path inside $data (e.g., 'year', 'metadata.readTime') */
  path: string
  /** Filter operator */
  op: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'in'
  /** Filter value */
  value: unknown
}

/**
 * Extract predicates from filter with shredding awareness
 *
 * When fields are shredded, filters on those fields can use Parquet column
 * statistics for row group skipping. Filters on non-shredded fields fall
 * back to Variant/row-level evaluation.
 *
 * @param filter - MongoDB-style filter
 * @param ns - Namespace being queried
 * @param config - Shredding configuration
 * @returns Predicates split between shredded and Variant paths
 */
export function extractPredicatesWithShredding(
  filter: Filter | undefined,
  ns: string,
  config?: ShreddedPushdownConfig
): ShreddedPredicateResult {
  const baseResult = extractPredicatesFromFilter(filter)
  const shreddedPredicates: StatisticsPredicate[] = []
  const variantPredicates: VariantPredicate[] = []

  if (!filter || typeof filter !== 'object') {
    return {
      ...baseResult,
      shreddedPredicates: [],
      variantPredicates: [],
      hasShreddedPushdown: false,
    }
  }

  // Get shred config for this namespace
  const shredConfig = config?.shredding?.[ns]
  const shreddedFields = new Set(shredConfig?.fields ?? [])

  for (const [key, value] of Object.entries(filter)) {
    // Skip logical operators
    if (key === '$and' || key === '$or' || key === '$not' || key === '$nor') {
      // Handle $and recursively
      if (key === '$and' && Array.isArray(value)) {
        for (const subFilter of value) {
          const subResult = extractPredicatesWithShredding(
            subFilter as Filter,
            ns,
            config
          )
          shreddedPredicates.push(...subResult.shreddedPredicates)
          variantPredicates.push(...subResult.variantPredicates)
        }
      }
      continue
    }

    // Skip special operators
    if (key === '$text' || key === '$vector' || key === '$geo') {
      continue
    }

    // Handle dot-notation filters on $data (e.g., '$data.year')
    if (key.startsWith('$data.')) {
      const fieldPath = key.slice(6) // Remove '$data.' prefix
      const topLevelField = fieldPath.split('.')[0]!

      if (shreddedFields.has(topLevelField)) {
        // This field is shredded - we can use statistics on the shredded column
        const preds = extractColumnPredicatesForShredded(topLevelField, value)
        shreddedPredicates.push(...preds)
      } else {
        // Field is not shredded - need Variant evaluation
        const varPreds = extractVariantPredicates(fieldPath, value)
        variantPredicates.push(...varPreds)
      }
      continue
    }

    // Handle non-$data filters
    // Check if it's a shredded field (top-level access)
    if (shreddedFields.has(key)) {
      const preds = extractColumnPredicatesForShredded(key, value)
      shreddedPredicates.push(...preds)
    }
    // Note: Non-shredded, non-$data fields are handled by baseResult.predicates
  }

  return {
    ...baseResult,
    shreddedPredicates,
    variantPredicates,
    hasShreddedPushdown: shreddedPredicates.length > 0,
  }
}

/**
 * Extract predicates for a shredded column
 */
function extractColumnPredicatesForShredded(
  column: string,
  value: unknown
): StatisticsPredicate[] {
  const predicates: StatisticsPredicate[] = []

  if (value === null || value === undefined) {
    return predicates
  }

  if (typeof value === 'object' && !Array.isArray(value) && !(value instanceof Date)) {
    const operators = value as Record<string, unknown>

    if ('$eq' in operators) {
      predicates.push({ column, op: 'eq', value: operators.$eq })
    }
    if ('$ne' in operators) {
      predicates.push({ column, op: 'ne', value: operators.$ne })
    }
    if ('$gt' in operators) {
      predicates.push({ column, op: 'gt', value: operators.$gt })
    }
    if ('$gte' in operators) {
      predicates.push({ column, op: 'gte', value: operators.$gte })
    }
    if ('$lt' in operators) {
      predicates.push({ column, op: 'lt', value: operators.$lt })
    }
    if ('$lte' in operators) {
      predicates.push({ column, op: 'lte', value: operators.$lte })
    }
    if ('$in' in operators && Array.isArray(operators.$in)) {
      predicates.push({ column, op: 'in', value: operators.$in })
    }

    return predicates
  }

  // Direct value = implicit equality
  predicates.push({ column, op: 'eq', value })
  return predicates
}

/**
 * Extract Variant predicates (for non-shredded fields)
 */
function extractVariantPredicates(
  path: string,
  value: unknown
): VariantPredicate[] {
  const predicates: VariantPredicate[] = []

  if (value === null || value === undefined) {
    return predicates
  }

  if (typeof value === 'object' && !Array.isArray(value) && !(value instanceof Date)) {
    const operators = value as Record<string, unknown>

    if ('$eq' in operators) {
      predicates.push({ path, op: 'eq', value: operators.$eq })
    }
    if ('$ne' in operators) {
      predicates.push({ path, op: 'ne', value: operators.$ne })
    }
    if ('$gt' in operators) {
      predicates.push({ path, op: 'gt', value: operators.$gt })
    }
    if ('$gte' in operators) {
      predicates.push({ path, op: 'gte', value: operators.$gte })
    }
    if ('$lt' in operators) {
      predicates.push({ path, op: 'lt', value: operators.$lt })
    }
    if ('$lte' in operators) {
      predicates.push({ path, op: 'lte', value: operators.$lte })
    }
    if ('$in' in operators && Array.isArray(operators.$in)) {
      predicates.push({ path, op: 'in', value: operators.$in })
    }

    return predicates
  }

  // Direct value = implicit equality
  predicates.push({ path, op: 'eq', value })
  return predicates
}

/**
 * Check if a row group can be skipped using both regular and shredded predicates
 *
 * @param rowGroup - Row group metadata
 * @param predicates - Regular predicates (core entity columns)
 * @param shreddedPredicates - Predicates on shredded columns
 * @returns true if the row group can be safely skipped
 */
export function canSkipRowGroupWithShredding(
  rowGroup: RowGroupMetadata,
  predicates: StatisticsPredicate[],
  shreddedPredicates: StatisticsPredicate[]
): boolean {
  // Check regular predicates first
  if (canSkipRowGroup(rowGroup, predicates)) {
    return true
  }

  // Check shredded predicates
  if (shreddedPredicates.length === 0) {
    return false
  }

  for (const predicate of shreddedPredicates) {
    const columnMeta = findColumnMetadata(rowGroup, predicate.column)
    if (!columnMeta?.statistics) {
      continue
    }

    if (canSkipByStatistics(predicate, columnMeta.statistics)) {
      return true
    }
  }

  return false
}

/**
 * Get columns needed for a query with shredding support
 *
 * @param filter - Query filter
 * @param options - Query options
 * @param shreddedFields - Set of shredded field names
 * @returns Array of column names to read
 */
export function getRequiredColumnsWithShredding(
  filter: Filter | undefined,
  options: FindOptions | undefined,
  shreddedFields: Set<string>
): string[] {
  const columns = new Set<string>(ENTITY_CORE_COLUMNS)

  // Add shredded fields that are used in the filter
  if (filter) {
    for (const [key, _value] of Object.entries(filter)) {
      // Handle $data.field notation
      if (key.startsWith('$data.')) {
        const fieldPath = key.slice(6)
        const topLevelField = fieldPath.split('.')[0]!
        if (shreddedFields.has(topLevelField)) {
          columns.add(topLevelField)
        }
      }
      // Handle direct field access
      else if (shreddedFields.has(key)) {
        columns.add(key)
      }
    }
  }

  // Add shredded fields requested in projection
  if (options?.project) {
    for (const col of Object.keys(options.project)) {
      if (shreddedFields.has(col)) {
        columns.add(col)
      }
    }
  }

  // Add shredded fields used in sorting
  if (options?.sort) {
    for (const col of Object.keys(options.sort)) {
      if (shreddedFields.has(col)) {
        columns.add(col)
      }
    }
  }

  return Array.from(columns)
}

/**
 * Rewrite filter to use shredded column names
 *
 * Transforms filters like { '$data.year': { $gte: 2020 } }
 * to use the shredded column { 'year': { $gte: 2020 } }
 *
 * @param filter - Original filter
 * @param shreddedFields - Set of shredded field names
 * @returns Rewritten filter targeting shredded columns
 */
export function rewriteFilterForShredding(
  filter: Filter | undefined,
  shreddedFields: Set<string>
): Filter | undefined {
  if (!filter) return undefined

  const rewritten: Record<string, unknown> = {}

  for (const [key, value] of Object.entries(filter)) {
    // Handle logical operators recursively
    if (key === '$and' && Array.isArray(value)) {
      rewritten.$and = value.map(subFilter =>
        rewriteFilterForShredding(subFilter as Filter, shreddedFields)
      )
      continue
    }
    if (key === '$or' && Array.isArray(value)) {
      rewritten.$or = value.map(subFilter =>
        rewriteFilterForShredding(subFilter as Filter, shreddedFields)
      )
      continue
    }
    if (key === '$nor' && Array.isArray(value)) {
      rewritten.$nor = value.map(subFilter =>
        rewriteFilterForShredding(subFilter as Filter, shreddedFields)
      )
      continue
    }
    if (key === '$not' && typeof value === 'object') {
      rewritten.$not = rewriteFilterForShredding(value as Filter, shreddedFields)
      continue
    }

    // Handle $data.field notation
    if (key.startsWith('$data.')) {
      const fieldPath = key.slice(6)
      const topLevelField = fieldPath.split('.')[0]!

      if (shreddedFields.has(topLevelField) && !fieldPath.includes('.')) {
        // Simple shredded field - rewrite to top-level column
        rewritten[topLevelField] = value
      } else {
        // Nested path or non-shredded - keep original
        rewritten[key] = value
      }
      continue
    }

    // Keep other filters unchanged
    rewritten[key] = value
  }

  return rewritten as Filter
}
