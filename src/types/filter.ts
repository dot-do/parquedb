/**
 * MongoDB-style filter operators for ParqueDB
 */

// Type imports from entity module (types used in filter definitions)

// =============================================================================
// Comparison Operators
// =============================================================================

/** Equality comparison */
export interface EqOperator<T = unknown> {
  $eq: T
}

/** Not equal comparison */
export interface NeOperator<T = unknown> {
  $ne: T
}

/** Greater than comparison */
export interface GtOperator<T = unknown> {
  $gt: T
}

/** Greater than or equal comparison */
export interface GteOperator<T = unknown> {
  $gte: T
}

/** Less than comparison */
export interface LtOperator<T = unknown> {
  $lt: T
}

/** Less than or equal comparison */
export interface LteOperator<T = unknown> {
  $lte: T
}

/** In array comparison */
export interface InOperator<T = unknown> {
  $in: T[]
}

/** Not in array comparison */
export interface NinOperator<T = unknown> {
  $nin: T[]
}

/** All comparison operators */
export type ComparisonOperator<T = unknown> =
  | EqOperator<T>
  | NeOperator<T>
  | GtOperator<T>
  | GteOperator<T>
  | LtOperator<T>
  | LteOperator<T>
  | InOperator<T>
  | NinOperator<T>

// =============================================================================
// String Operators
// =============================================================================

/** Regex match */
export interface RegexOperator {
  $regex: string | RegExp
  $options?: string
}

/** Starts with prefix */
export interface StartsWithOperator {
  $startsWith: string
}

/** Ends with suffix */
export interface EndsWithOperator {
  $endsWith: string
}

/** Contains substring */
export interface ContainsOperator {
  $contains: string
}

/** All string operators */
export type StringOperator =
  | RegexOperator
  | StartsWithOperator
  | EndsWithOperator
  | ContainsOperator

// =============================================================================
// Array Operators
// =============================================================================

/** Array contains all values */
export interface AllOperator<T = unknown> {
  $all: T[]
}

/** Array element matches filter */
export interface ElemMatchOperator {
  $elemMatch: Filter
}

/** Array has specific size */
export interface SizeOperator {
  $size: number
}

/** All array operators */
export type ArrayOperator<T = unknown> =
  | AllOperator<T>
  | ElemMatchOperator
  | SizeOperator

// =============================================================================
// Existence Operators
// =============================================================================

/** Field exists check */
export interface ExistsOperator {
  $exists: boolean
}

/** Type check */
export interface TypeOperator {
  $type: 'null' | 'boolean' | 'number' | 'string' | 'array' | 'object' | 'date'
}

/** Existence operators */
export type ExistenceOperator =
  | ExistsOperator
  | TypeOperator

// =============================================================================
// Special Operators
// =============================================================================

/** Full-text search */
export interface TextOperator {
  $text: {
    /** Search query string */
    $search: string
    /** Language for stemming/stopwords */
    $language?: string
    /** Case sensitive matching */
    $caseSensitive?: boolean
    /** Diacritic sensitive matching */
    $diacriticSensitive?: boolean
    /** Minimum score threshold (0-1) */
    $minScore?: number
  }
}

/**
 * Hybrid search strategy for combining vector search with metadata filtering
 *
 * - 'pre-filter': Apply metadata filters first, then vector search on filtered set (faster for selective filters)
 * - 'post-filter': Perform vector search first, then filter results (faster for broad filters)
 * - 'auto': Automatically choose strategy based on filter selectivity (default)
 */
export type HybridSearchStrategy = 'pre-filter' | 'post-filter' | 'auto'

/** Vector similarity search */
export interface VectorOperator {
  $vector: {
    /** Query vector (or text for auto-embedding if embedder is configured) */
    query: number[] | string
    /** Field containing vectors */
    field: string
    /** Number of results */
    topK: number
    /** Minimum similarity threshold (0-1) */
    minScore?: number
    /** Hybrid search strategy */
    strategy?: HybridSearchStrategy
    /** HNSW efSearch parameter for search quality/speed tradeoff */
    efSearch?: number

    // Legacy format support (deprecated, use query/field/topK instead)
    /** @deprecated Use `query` instead */
    $near?: number[]
    /** @deprecated Use `topK` instead */
    $k?: number
    /** @deprecated Use `field` instead */
    $field?: string
    /** @deprecated Use `minScore` instead */
    $minScore?: number
  }
}

/** Geospatial near query */
export interface GeoOperator {
  $geo: {
    $near: {
      lng: number
      lat: number
    }
    /** Maximum distance in meters */
    $maxDistance?: number
    /** Minimum distance in meters */
    $minDistance?: number
  }
}

/** Special operators */
export type SpecialOperator =
  | TextOperator
  | VectorOperator
  | GeoOperator

// =============================================================================
// Logical Operators
// =============================================================================

/** AND - all conditions must match */
export interface AndOperator {
  $and: Filter[]
}

/** OR - any condition must match */
export interface OrOperator {
  $or: Filter[]
}

/** NOT - condition must not match */
export interface NotOperator {
  $not: Filter
}

/** NOR - none of conditions must match */
export interface NorOperator {
  $nor: Filter[]
}

/** Logical operators */
export type LogicalOperator =
  | AndOperator
  | OrOperator
  | NotOperator
  | NorOperator

// =============================================================================
// Field Filter
// =============================================================================

/** All operators that can be applied to a field */
export type FieldOperator<T = unknown> =
  | ComparisonOperator<T>
  | StringOperator
  | ArrayOperator<T>
  | ExistenceOperator

/** A field filter value (either direct value for equality or operator) */
export type FieldFilter<T = unknown> = T | FieldOperator<T>

// =============================================================================
// Main Filter Type
// =============================================================================

/**
 * MongoDB-style filter object
 *
 * @example
 * // Simple equality
 * { status: 'published' }
 *
 * @example
 * // With operators
 * { score: { $gte: 100 }, status: { $in: ['published', 'featured'] } }
 *
 * @example
 * // Logical operators
 * { $or: [{ status: 'published' }, { featured: true }] }
 *
 * @example
 * // Full-text search
 * { $text: { $search: 'parquet database' } }
 *
 * @example
 * // Vector similarity
 * { $vector: { $near: embedding, $k: 10, $field: 'embedding' } }
 */
export interface Filter {
  /** Field filters - key is field name, value is filter condition */
  [field: string]: FieldFilter | undefined

  /** Logical AND */
  $and?: Filter[]

  /** Logical OR */
  $or?: Filter[]

  /** Logical NOT */
  $not?: Filter

  /** Logical NOR */
  $nor?: Filter[]

  /** Full-text search */
  $text?: TextOperator['$text']

  /** Vector similarity */
  $vector?: VectorOperator['$vector']

  /** Geospatial */
  $geo?: GeoOperator['$geo']
}

// =============================================================================
// Type Guards
// =============================================================================

/** Check if value is a comparison operator */
export function isComparisonOperator(value: unknown): value is ComparisonOperator {
  if (typeof value !== 'object' || value === null) return false
  const keys = Object.keys(value)
  return keys.length === 1 && keys[0] !== undefined && ['$eq', '$ne', '$gt', '$gte', '$lt', '$lte', '$in', '$nin'].includes(keys[0])
}

/** Check if value is a string operator */
export function isStringOperator(value: unknown): value is StringOperator {
  if (typeof value !== 'object' || value === null) return false
  const keys = Object.keys(value)
  return keys.some(k => ['$regex', '$startsWith', '$endsWith', '$contains'].includes(k))
}

/** Check if value is an array operator */
export function isArrayOperator(value: unknown): value is ArrayOperator {
  if (typeof value !== 'object' || value === null) return false
  const keys = Object.keys(value)
  return keys.length === 1 && keys[0] !== undefined && ['$all', '$elemMatch', '$size'].includes(keys[0])
}

/** Check if value is an existence operator */
export function isExistenceOperator(value: unknown): value is ExistenceOperator {
  if (typeof value !== 'object' || value === null) return false
  const keys = Object.keys(value)
  return keys.length === 1 && keys[0] !== undefined && ['$exists', '$type'].includes(keys[0])
}

/** Check if value is any field operator (not a plain value) */
export function isFieldOperator(value: unknown): value is FieldOperator {
  return (
    isComparisonOperator(value) ||
    isStringOperator(value) ||
    isArrayOperator(value) ||
    isExistenceOperator(value)
  )
}

/** Check if filter has logical operators */
export function hasLogicalOperators(filter: Filter): boolean {
  return '$and' in filter || '$or' in filter || '$not' in filter || '$nor' in filter
}

/** Check if filter has special operators */
export function hasSpecialOperators(filter: Filter): boolean {
  return '$text' in filter || '$vector' in filter || '$geo' in filter
}
