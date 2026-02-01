/**
 * Variant Filter Support for Predicate Pushdown
 *
 * Transforms dot-notation filters like { '$index.titleType': 'movie' }
 * into Parquet column paths for statistics-based row group skipping.
 *
 * Based on Parquet Variant Shredding spec:
 * https://parquet.apache.org/docs/file-format/types/variantshredding/
 */

/**
 * Variant column metadata for filter transformation
 */
export interface VariantShredConfig {
  /** Variant column name (e.g., '$index') */
  column: string
  /** Shredded field names */
  fields: string[]
}

/**
 * Transform user filter with dot-notation into Parquet column paths
 *
 * Input: { '$index.titleType': 'movie' }
 * Output: { '$index.typed_value.titleType.typed_value': 'movie' }
 *
 * @param filter - User filter with dot-notation
 * @param config - Variant shred configuration
 * @returns Transformed filter with Parquet column paths
 */
export function transformVariantFilter(
  filter: Record<string, unknown>,
  config: VariantShredConfig[]
): Record<string, unknown> {
  const result: Record<string, unknown> = {}

  for (const [key, value] of Object.entries(filter)) {
    // Handle logical operators recursively
    if (key === '$and' || key === '$or' || key === '$nor') {
      result[key] = (value as Record<string, unknown>[]).map(f =>
        transformVariantFilter(f, config)
      )
      continue
    }

    if (key === '$not') {
      result[key] = transformVariantFilter(value as Record<string, unknown>, config)
      continue
    }

    // Check if this is a dot-notation filter on a Variant column
    const dotIndex = key.indexOf('.')
    if (dotIndex > 0) {
      const columnName = key.slice(0, dotIndex)
      const fieldPath = key.slice(dotIndex + 1)

      // Find matching Variant config
      const variantConfig = config.find(c => c.column === columnName)

      if (variantConfig) {
        // Check if this field is shredded
        const fieldName = fieldPath.split('.')[0]
        if (variantConfig.fields.includes(fieldName)) {
          // Transform to Parquet Variant shredding path
          // $index.titleType -> $index.typed_value.titleType.typed_value
          const parquetPath = `${columnName}.typed_value.${fieldPath}.typed_value`
          result[parquetPath] = value
          continue
        }
      }
    }

    // Pass through unchanged
    result[key] = value
  }

  return result
}

/**
 * Extract column names from filter, handling Variant dot-notation
 *
 * For '$index.titleType', returns both:
 * - '$index' (the Variant column for reading)
 * - '$index.typed_value.titleType.typed_value' (for statistics)
 *
 * @param filter - Filter object
 * @param config - Variant shred configuration
 * @returns Array of column names to read
 */
export function extractVariantFilterColumns(
  filter: Record<string, unknown>,
  config: VariantShredConfig[]
): { readColumns: string[]; statsColumns: string[] } {
  const readColumns = new Set<string>()
  const statsColumns = new Set<string>()

  function extract(f: Record<string, unknown>) {
    for (const [key, value] of Object.entries(f)) {
      if (key === '$and' || key === '$or' || key === '$nor') {
        (value as Record<string, unknown>[]).forEach(extract)
        continue
      }
      if (key === '$not') {
        extract(value as Record<string, unknown>)
        continue
      }
      if (key.startsWith('$')) continue

      // Check for dot-notation
      const dotIndex = key.indexOf('.')
      if (dotIndex > 0) {
        const columnName = key.slice(0, dotIndex)
        const fieldPath = key.slice(dotIndex + 1)

        // Find Variant config
        const variantConfig = config.find(c => c.column === columnName)
        if (variantConfig) {
          const fieldName = fieldPath.split('.')[0]
          if (variantConfig.fields.includes(fieldName)) {
            // Need to read the Variant column
            readColumns.add(columnName)
            // Statistics are on the typed_value path
            statsColumns.add(`${columnName}.typed_value.${fieldPath}.typed_value`)
            continue
          }
        }
      }

      // Regular column
      readColumns.add(key)
      statsColumns.add(key)
    }
  }

  extract(filter)

  return {
    readColumns: [...readColumns],
    statsColumns: [...statsColumns],
  }
}

/**
 * Get nested value from row using dot-notation path
 *
 * @param row - Row object
 * @param path - Dot-notation path (e.g., '$index.titleType')
 * @returns Value at path or undefined
 */
export function getNestedValue(row: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = row

  for (const part of parts) {
    if (current === null || current === undefined) return undefined
    if (typeof current !== 'object') return undefined
    current = (current as Record<string, unknown>)[part]
  }

  return current
}

/**
 * Check if row matches filter with dot-notation support
 *
 * @param row - Row object (may have Variant columns as nested objects)
 * @param filter - Filter with dot-notation
 * @returns True if row matches
 */
export function matchesVariantFilter(
  row: Record<string, unknown>,
  filter: Record<string, unknown>
): boolean {
  for (const [key, condition] of Object.entries(filter)) {
    // Handle logical operators
    if (key === '$and') {
      const subFilters = condition as Record<string, unknown>[]
      if (!subFilters.every(f => matchesVariantFilter(row, f))) return false
      continue
    }
    if (key === '$or') {
      const subFilters = condition as Record<string, unknown>[]
      if (!subFilters.some(f => matchesVariantFilter(row, f))) return false
      continue
    }
    if (key === '$nor') {
      const subFilters = condition as Record<string, unknown>[]
      if (subFilters.some(f => matchesVariantFilter(row, f))) return false
      continue
    }
    if (key === '$not') {
      if (matchesVariantFilter(row, condition as Record<string, unknown>)) return false
      continue
    }

    // Skip other $ operators
    if (key.startsWith('$')) continue

    // Get value (supports dot-notation)
    const value = key.includes('.') ? getNestedValue(row, key) : row[key]

    // Match condition
    if (!matchesCondition(value, condition)) return false
  }

  return true
}

/**
 * Check if value matches condition
 */
function matchesCondition(value: unknown, condition: unknown): boolean {
  // Direct equality
  if (typeof condition !== 'object' || condition === null || Array.isArray(condition)) {
    return value === condition
  }

  // Operator conditions
  const ops = condition as Record<string, unknown>
  for (const [op, target] of Object.entries(ops)) {
    switch (op) {
      case '$eq':
        if (value !== target) return false
        break
      case '$ne':
        if (value === target) return false
        break
      case '$gt':
        if (!((value as number) > (target as number))) return false
        break
      case '$gte':
        if (!((value as number) >= (target as number))) return false
        break
      case '$lt':
        if (!((value as number) < (target as number))) return false
        break
      case '$lte':
        if (!((value as number) <= (target as number))) return false
        break
      case '$in':
        if (!Array.isArray(target) || !target.includes(value)) return false
        break
      case '$nin':
        if (!Array.isArray(target) || target.includes(value)) return false
        break
      case '$exists':
        if (target && value === undefined) return false
        if (!target && value !== undefined) return false
        break
      case '$regex':
        if (typeof value !== 'string') return false
        if (!new RegExp(target as string).test(value)) return false
        break
    }
  }

  return true
}

/**
 * Create range predicate for statistics-based filtering
 * Same logic as hyparquet but exported for use with Variant columns
 */
export function createRangePredicate(
  condition: unknown
): ((min: unknown, max: unknown) => boolean) | null {
  // Direct value comparison
  if (typeof condition !== 'object' || condition === null) {
    return (min, max) => (min as number) <= (condition as number) && (condition as number) <= (max as number)
  }

  const ops = condition as Record<string, unknown>
  const { $eq, $gt, $gte, $lt, $lte, $in } = ops

  return (min, max) => {
    if ($eq !== undefined) {
      return (min as number) <= ($eq as number) && ($eq as number) <= (max as number)
    }

    if ($in && Array.isArray($in)) {
      return $in.some(v => (min as number) <= (v as number) && (v as number) <= (max as number))
    }

    let possible = true

    if ($gt !== undefined) {
      possible = possible && (max as number) > ($gt as number)
    }
    if ($gte !== undefined) {
      possible = possible && (max as number) >= ($gte as number)
    }
    if ($lt !== undefined) {
      possible = possible && (min as number) < ($lt as number)
    }
    if ($lte !== undefined) {
      possible = possible && (min as number) <= ($lte as number)
    }

    return possible
  }
}
