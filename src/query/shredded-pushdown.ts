/**
 * Predicate Pushdown for Shredded Variant Fields
 *
 * Integrates @dotdo/iceberg v3 variant shredding APIs to enable efficient
 * predicate pushdown during query execution. When variant columns are shredded,
 * the typed sub-columns have statistics (min/max bounds) that can be used to
 * skip data files that definitely don't contain matching rows.
 *
 * This module provides:
 * 1. Configuration extraction from Iceberg table properties
 * 2. Filter transformation for shredded paths
 * 3. Data file filtering based on shredded column statistics
 * 4. Row group filtering for fine-grained pushdown
 *
 * @example
 * ```typescript
 * import { ShreddedPushdownContext, filterDataFilesWithShredding } from './shredded-pushdown'
 *
 * // Create context from table metadata
 * const context = ShreddedPushdownContext.fromTableProperties(tableProperties)
 *
 * // Filter data files based on query
 * const filter = { '$data.year': { $gte: 2020 }, '$data.rating': { $gt: 8.0 } }
 * const { files, stats } = filterDataFilesWithShredding(dataFiles, filter, context)
 *
 * console.log(`Scanning ${files.length} of ${stats.totalFiles} files`)
 * console.log(`Skipped ${stats.skippedFiles} files via predicate pushdown`)
 * ```
 *
 * @see https://iceberg.apache.org/spec/#variant-shredding
 */

import type { DataFile } from '@dotdo/iceberg'
import {
  // Configuration extraction
  extractVariantShredConfig,
  parseShredColumnsProperty,
  parseShredFieldsProperty,
  parseFieldTypesProperty,
  getShredFieldsKey,
  getFieldTypesKey,
  VARIANT_SHRED_COLUMNS_KEY,

  // Filter transformation
  transformVariantFilter as icebergTransformVariantFilter,

  // Statistics paths
  extractVariantFilterColumns,
  assignShreddedFieldIds,

  // Data file filtering
  filterDataFiles,
  filterDataFilesWithStats,
  shouldSkipDataFile,

  // Range predicates
  createRangePredicate,
  evaluateRangePredicate,
  combinePredicatesAnd,
  combinePredicatesOr,

  // Types
  type VariantShredPropertyConfig,
  type FilterStats,
  type RangePredicate,
  type PredicateResult,
} from '@dotdo/iceberg'
import { DEFAULT_SHREDDED_STARTING_FIELD_ID } from '../constants'
import { logger } from '../utils/logger'

import type { Filter } from '../types/filter'

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for shredded variant pushdown
 */
export interface ShreddedPushdownConfig {
  /** Variant column configurations */
  readonly configs: readonly VariantShredPropertyConfig[]

  /** Map from statistics paths to field IDs */
  readonly fieldIdMap: Map<string, number>

  /** Starting field ID used for assignment */
  readonly startingFieldId: number
}

/**
 * Result of filtering with statistics
 */
export interface FilterResult {
  /** Data files that may contain matching rows */
  readonly files: readonly DataFile[]

  /** Statistics about the filtering operation */
  readonly stats: FilterStats
}

/**
 * Options for shredded pushdown
 */
export interface ShreddedPushdownOptions {
  /** Starting field ID for shredded columns (default: 1000) */
  startingFieldId?: number | undefined

  /** Enable verbose logging for debugging */
  debug?: boolean | undefined
}

// =============================================================================
// ShreddedPushdownContext
// =============================================================================

/**
 * Context for shredded variant predicate pushdown
 *
 * Encapsulates the configuration needed to perform predicate pushdown on
 * shredded variant columns. Create once per table and reuse across queries.
 *
 * @example
 * ```typescript
 * // From table properties
 * const context = ShreddedPushdownContext.fromTableProperties(properties)
 *
 * // From explicit config
 * const context = ShreddedPushdownContext.fromConfig([
 *   { columnName: '$data', fields: ['year', 'rating'], fieldTypes: { year: 'int', rating: 'double' } }
 * ])
 *
 * // Filter data files
 * const result = context.filterDataFiles(dataFiles, { '$data.year': { $gte: 2020 } })
 * ```
 */
export class ShreddedPushdownContext {
  private readonly configs: readonly VariantShredPropertyConfig[]
  private readonly fieldIdMap: Map<string, number>
  private readonly startingFieldId: number
  private readonly debug: boolean

  /**
   * Create a pushdown context
   *
   * @param config - Configuration for shredded pushdown
   * @param options - Optional settings
   */
  private constructor(
    config: ShreddedPushdownConfig,
    options: ShreddedPushdownOptions = {}
  ) {
    this.configs = config.configs
    this.fieldIdMap = config.fieldIdMap
    this.startingFieldId = config.startingFieldId
    this.debug = options.debug ?? false
  }

  /**
   * Create a pushdown context from Iceberg table properties
   *
   * Extracts variant shredding configuration from the table's properties
   * and sets up field ID mappings for statistics lookup.
   *
   * @param properties - Table properties record
   * @param options - Optional settings
   * @returns Configured pushdown context
   */
  static fromTableProperties(
    properties: Record<string, string>,
    options: ShreddedPushdownOptions = {}
  ): ShreddedPushdownContext {
    const configs = extractVariantShredConfig(properties)
    const startingFieldId = options.startingFieldId ?? DEFAULT_SHREDDED_STARTING_FIELD_ID
    const fieldIdMap = assignShreddedFieldIds(configs, startingFieldId)

    return new ShreddedPushdownContext(
      { configs, fieldIdMap, startingFieldId },
      options
    )
  }

  /**
   * Create a pushdown context from explicit configuration
   *
   * @param configs - Array of variant shred configurations
   * @param options - Optional settings
   * @returns Configured pushdown context
   */
  static fromConfig(
    configs: readonly VariantShredPropertyConfig[],
    options: ShreddedPushdownOptions = {}
  ): ShreddedPushdownContext {
    const startingFieldId = options.startingFieldId ?? DEFAULT_SHREDDED_STARTING_FIELD_ID
    const fieldIdMap = assignShreddedFieldIds(configs, startingFieldId)

    return new ShreddedPushdownContext(
      { configs, fieldIdMap, startingFieldId },
      options
    )
  }

  /**
   * Create an empty pushdown context (no shredding configured)
   *
   * @returns Empty context that passes through all files
   */
  static empty(): ShreddedPushdownContext {
    return new ShreddedPushdownContext({
      configs: [],
      fieldIdMap: new Map(),
      startingFieldId: DEFAULT_SHREDDED_STARTING_FIELD_ID,
    })
  }

  /**
   * Check if any shredding is configured
   */
  get hasShredding(): boolean {
    return this.configs.length > 0
  }

  /**
   * Get the shred configurations
   */
  get shredConfigs(): readonly VariantShredPropertyConfig[] {
    return this.configs
  }

  /**
   * Get shredded column names
   */
  get shreddedColumns(): string[] {
    return this.configs.map((c) => c.columnName)
  }

  /**
   * Get all shredded fields for a column
   */
  getShreddedFields(columnName: string): readonly string[] {
    const config = this.configs.find((c) => c.columnName === columnName)
    return config?.fields ?? []
  }

  /**
   * Check if a filter path targets a shredded field
   *
   * @param path - Filter path like '$data.year'
   * @returns true if the field is shredded
   */
  isShreddedField(path: string): boolean {
    const dotIndex = path.indexOf('.')
    if (dotIndex < 0) return false

    const columnName = path.slice(0, dotIndex)
    const fieldName = path.slice(dotIndex + 1).split('.')[0]

    const config = this.configs.find((c) => c.columnName === columnName)
    return config?.fields.includes(fieldName ?? '') ?? false
  }

  /**
   * Transform a filter for shredded field paths
   *
   * Converts user filter paths like { '$data.year': { $gte: 2020 } }
   * to Parquet column paths for statistics lookup.
   *
   * @param filter - User filter object
   * @returns Transformed filter with Parquet paths
   */
  transformFilter(filter: Filter): Record<string, unknown> {
    if (!this.hasShredding || !filter) {
      return filter as Record<string, unknown>
    }

    // Convert our VariantShredPropertyConfig to the format expected by iceberg
    const result = icebergTransformVariantFilter(
      filter as Record<string, unknown>,
      this.configs
    )
    return result.filter
  }

  /**
   * Extract columns needed for filter evaluation
   *
   * Returns both columns to read and columns for statistics evaluation.
   *
   * @param filter - Filter object
   * @returns Read columns and stats columns
   */
  extractFilterColumns(filter: Filter): {
    readColumns: string[]
    statsColumns: string[]
    shreddedPaths: string[]
  } {
    if (!this.hasShredding || !filter) {
      return { readColumns: [], statsColumns: [], shreddedPaths: [] }
    }

    const result = extractVariantFilterColumns(
      filter as Record<string, unknown>,
      this.configs
    )

    // Identify which paths are shredded
    const shreddedPaths = Object.keys(filter as Record<string, unknown>).filter(
      (path) => this.isShreddedField(path)
    )

    return {
      readColumns: result.readColumns,
      statsColumns: result.statsColumns,
      shreddedPaths,
    }
  }

  /**
   * Filter data files based on shredded column statistics
   *
   * Uses column statistics in data file manifests to skip files that
   * definitely don't contain matching rows.
   *
   * @param dataFiles - Array of data files to filter
   * @param filter - Query filter
   * @returns Filtered data files that may contain matches
   */
  filterDataFiles(dataFiles: readonly DataFile[], filter: Filter): DataFile[] {
    if (!this.hasShredding || !filter || dataFiles.length === 0) {
      return [...dataFiles]
    }

    return filterDataFiles(
      dataFiles,
      filter as Record<string, unknown>,
      this.configs,
      this.fieldIdMap
    )
  }

  /**
   * Filter data files and return statistics
   *
   * @param dataFiles - Array of data files to filter
   * @param filter - Query filter
   * @returns Filtered files and statistics
   */
  filterDataFilesWithStats(
    dataFiles: readonly DataFile[],
    filter: Filter
  ): FilterResult {
    if (!this.hasShredding || !filter || dataFiles.length === 0) {
      return {
        files: [...dataFiles],
        stats: {
          totalFiles: dataFiles.length,
          skippedFiles: 0,
          skippedByField: new Map(),
        },
      }
    }

    const result = filterDataFilesWithStats(
      dataFiles,
      filter as Record<string, unknown>,
      this.configs,
      this.fieldIdMap
    )

    if (this.debug) {
      logger.debug(
        `[ShreddedPushdown] Filtered ${result.stats.totalFiles} files, ` +
          `skipped ${result.stats.skippedFiles} (${((result.stats.skippedFiles / result.stats.totalFiles) * 100).toFixed(1)}%)`
      )
      for (const [field, count] of result.stats.skippedByField) {
        logger.debug(`  - ${field}: skipped ${count} files`)
      }
    }

    return result
  }

  /**
   * Check if a specific data file should be skipped
   *
   * @param dataFile - Data file to evaluate
   * @param filter - Query filter
   * @returns Skip decision with reason
   */
  shouldSkipDataFile(dataFile: DataFile, filter: Filter): PredicateResult {
    if (!this.hasShredding || !filter) {
      return { skip: false }
    }

    return shouldSkipDataFile(
      dataFile,
      filter as Record<string, unknown>,
      this.configs,
      this.fieldIdMap
    )
  }

  /**
   * Create a range predicate from a filter operator
   *
   * @param operator - Filter operator ($gt, $gte, $lt, $lte, $eq, $in)
   * @param value - Value to compare against
   * @returns Range predicate
   */
  createRangePredicate(operator: string, value: unknown): RangePredicate {
    return createRangePredicate(operator, value)
  }

  /**
   * Combine predicates with AND semantics
   */
  combinePredicatesAnd(
    predicates: readonly RangePredicate[]
  ): RangePredicate | null {
    return combinePredicatesAnd(predicates)
  }

  /**
   * Combine predicates with OR semantics
   */
  combinePredicatesOr(
    predicates: readonly RangePredicate[]
  ): RangePredicate[] {
    return combinePredicatesOr(predicates)
  }
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Build table properties for variant shredding configuration
 *
 * Creates the property key-value pairs needed to enable shredding
 * for specified variant columns.
 *
 * @param configs - Shred configurations
 * @returns Properties record to merge with table properties
 *
 * @example
 * ```typescript
 * const props = buildShreddingProperties([
 *   { columnName: '$data', fields: ['year', 'rating'], fieldTypes: { year: 'int', rating: 'double' } }
 * ])
 * // {
 * //   'write.variant.shred-columns': '$data',
 * //   'write.variant.$data.shred-fields': 'year,rating',
 * //   'write.variant.$data.field-types': 'year:int,rating:double'
 * // }
 * ```
 */
export function buildShreddingProperties(
  configs: readonly VariantShredPropertyConfig[]
): Record<string, string> {
  const properties: Record<string, string> = {}

  if (configs.length === 0) {
    return properties
  }

  // Set column list
  properties[VARIANT_SHRED_COLUMNS_KEY] = configs
    .map((c) => c.columnName)
    .join(',')

  // Set fields and types for each column
  for (const config of configs) {
    if (config.fields.length > 0) {
      properties[getShredFieldsKey(config.columnName)] =
        config.fields.join(',')
    }

    const typeEntries = Object.entries(config.fieldTypes)
    if (typeEntries.length > 0) {
      properties[getFieldTypesKey(config.columnName)] = typeEntries
        .map(([field, type]) => `${field}:${type}`)
        .join(',')
    }
  }

  return properties
}

/**
 * Extract shredded filter paths from a filter
 *
 * Returns the filter paths that target shredded variant fields.
 *
 * @param filter - Filter object
 * @param context - Pushdown context
 * @returns Array of shredded paths
 */
export function extractShreddedFilterPaths(
  filter: Filter,
  context: ShreddedPushdownContext
): string[] {
  if (!filter || !context.hasShredding) {
    return []
  }

  const paths: string[] = []
  const filterObj = filter as Record<string, unknown>

  function extract(obj: Record<string, unknown>): void {
    for (const [key, value] of Object.entries(obj)) {
      // Handle logical operators
      if (key === '$and' || key === '$or' || key === '$nor') {
        const conditions = value as Record<string, unknown>[]
        for (const condition of conditions) {
          extract(condition)
        }
        continue
      }
      if (key === '$not') {
        extract(value as Record<string, unknown>)
        continue
      }

      // Skip comparison operators (not field paths)
      // But don't skip $-prefixed column names like '$data.year'
      if (key.startsWith('$') && !key.includes('.')) {
        continue
      }

      // Check if this is a shredded field path
      if (context.isShreddedField(key)) {
        paths.push(key)
      }
    }
  }

  extract(filterObj)
  return [...new Set(paths)]
}

/**
 * Check if a filter has any conditions on shredded fields
 *
 * @param filter - Filter object
 * @param context - Pushdown context
 * @returns true if filter includes shredded field conditions
 */
export function hasShreddedConditions(
  filter: Filter,
  context: ShreddedPushdownContext
): boolean {
  return extractShreddedFilterPaths(filter, context).length > 0
}

/**
 * Estimate pushdown effectiveness for a filter
 *
 * Returns an estimate of how effective predicate pushdown will be
 * based on which filter conditions target shredded fields.
 *
 * @param filter - Filter object
 * @param context - Pushdown context
 * @returns Effectiveness metrics
 */
export function estimatePushdownEffectiveness(
  filter: Filter,
  context: ShreddedPushdownContext
): {
  /** Total filter conditions */
  totalConditions: number
  /** Conditions on shredded fields */
  shreddedConditions: number
  /** Effectiveness ratio (0-1) */
  effectiveness: number
  /** Whether pushdown is likely to help */
  isEffective: boolean
} {
  if (!filter || !context.hasShredding) {
    return {
      totalConditions: 0,
      shreddedConditions: 0,
      effectiveness: 0,
      isEffective: false,
    }
  }

  const filterObj = filter as Record<string, unknown>
  let totalConditions = 0
  let shreddedConditions = 0

  function countConditions(obj: Record<string, unknown>): void {
    for (const [key, value] of Object.entries(obj)) {
      if (key === '$and' || key === '$or' || key === '$nor') {
        const conditions = value as Record<string, unknown>[]
        for (const condition of conditions) {
          countConditions(condition)
        }
        continue
      }
      if (key === '$not') {
        countConditions(value as Record<string, unknown>)
        continue
      }
      // Skip comparison operators (not field paths)
      // But don't skip $-prefixed column names like '$data.year'
      if (key.startsWith('$') && !key.includes('.')) {
        continue
      }

      totalConditions++
      if (context.isShreddedField(key)) {
        shreddedConditions++
      }
    }
  }

  countConditions(filterObj)

  const effectiveness =
    totalConditions > 0 ? shreddedConditions / totalConditions : 0

  return {
    totalConditions,
    shreddedConditions,
    effectiveness,
    isEffective: effectiveness > 0.5, // Consider effective if >50% conditions are on shredded fields
  }
}

// =============================================================================
// Re-exports from @dotdo/iceberg
// =============================================================================

export type {
  VariantShredPropertyConfig,
  FilterStats,
  RangePredicate,
  PredicateResult,
}

export {
  // Configuration utilities
  extractVariantShredConfig,
  parseShredColumnsProperty,
  parseShredFieldsProperty,
  parseFieldTypesProperty,
  getShredFieldsKey,
  getFieldTypesKey,
  VARIANT_SHRED_COLUMNS_KEY,

  // Statistics paths
  extractVariantFilterColumns,
  assignShreddedFieldIds,

  // Data file filtering
  filterDataFiles,
  filterDataFilesWithStats,
  shouldSkipDataFile,

  // Range predicates
  createRangePredicate,
  evaluateRangePredicate,
  combinePredicatesAnd,
  combinePredicatesOr,
}
