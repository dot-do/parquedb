/**
 * MongoDB-style filter operators for ParqueDB
 *
 * This module provides a comprehensive set of query operators compatible with
 * MongoDB's query language. Filters can be used with find(), findOne(), updateMany(),
 * deleteMany(), and other query methods.
 *
 * @module types/filter
 *
 * @example
 * ```typescript
 * // Simple equality
 * { status: 'published' }
 *
 * // Comparison operators
 * { score: { $gte: 100, $lt: 1000 } }
 *
 * // Logical operators
 * { $or: [{ status: 'published' }, { featured: true }] }
 *
 * // String operators
 * { title: { $regex: /^Hello/i } }
 *
 * // Array operators
 * { tags: { $all: ['tech', 'database'] } }
 * ```
 */

// Type imports from entity module (types used in filter definitions)

// =============================================================================
// Comparison Operators
// =============================================================================

/**
 * Equality comparison operator
 *
 * Matches values that are equal to the specified value.
 * Equivalent to direct field comparison: `{ field: value }` is same as `{ field: { $eq: value } }`
 *
 * @typeParam T - Type of the value being compared
 *
 * @example
 * ```typescript
 * // Match posts with exactly 100 views
 * { views: { $eq: 100 } }
 * ```
 */
export interface EqOperator<T = unknown> {
  $eq: T
}

/**
 * Not-equal comparison operator
 *
 * Matches values that are not equal to the specified value.
 *
 * @typeParam T - Type of the value being compared
 *
 * @example
 * ```typescript
 * // Match posts not in draft status
 * { status: { $ne: 'draft' } }
 * ```
 */
export interface NeOperator<T = unknown> {
  $ne: T
}

/**
 * Greater-than comparison operator
 *
 * Matches values that are greater than the specified value.
 * Works with numbers, dates, and strings (lexicographic comparison).
 *
 * @typeParam T - Type of the value being compared
 *
 * @example
 * ```typescript
 * // Match posts with more than 100 views
 * { views: { $gt: 100 } }
 *
 * // Match posts created after a date
 * { createdAt: { $gt: new Date('2024-01-01') } }
 * ```
 */
export interface GtOperator<T = unknown> {
  $gt: T
}

/**
 * Greater-than-or-equal comparison operator
 *
 * Matches values that are greater than or equal to the specified value.
 *
 * @typeParam T - Type of the value being compared
 *
 * @example
 * ```typescript
 * // Match posts with 100 or more views
 * { views: { $gte: 100 } }
 * ```
 */
export interface GteOperator<T = unknown> {
  $gte: T
}

/**
 * Less-than comparison operator
 *
 * Matches values that are less than the specified value.
 *
 * @typeParam T - Type of the value being compared
 *
 * @example
 * ```typescript
 * // Match posts with fewer than 10 comments
 * { commentCount: { $lt: 10 } }
 * ```
 */
export interface LtOperator<T = unknown> {
  $lt: T
}

/**
 * Less-than-or-equal comparison operator
 *
 * Matches values that are less than or equal to the specified value.
 *
 * @typeParam T - Type of the value being compared
 *
 * @example
 * ```typescript
 * // Match posts with 10 or fewer comments
 * { commentCount: { $lte: 10 } }
 * ```
 */
export interface LteOperator<T = unknown> {
  $lte: T
}

/**
 * In-array comparison operator
 *
 * Matches any value that exists in the specified array.
 *
 * @typeParam T - Type of the values in the array
 *
 * @example
 * ```typescript
 * // Match posts with status 'published' or 'featured'
 * { status: { $in: ['published', 'featured'] } }
 * ```
 */
export interface InOperator<T = unknown> {
  $in: T[]
}

/**
 * Not-in-array comparison operator
 *
 * Matches values that do not exist in the specified array.
 *
 * @typeParam T - Type of the values in the array
 *
 * @example
 * ```typescript
 * // Match posts that are not draft or archived
 * { status: { $nin: ['draft', 'archived'] } }
 * ```
 */
export interface NinOperator<T = unknown> {
  $nin: T[]
}

/**
 * Modulo operation operator
 *
 * Matches values where value % divisor equals remainder.
 * Useful for filtering by multiples or distributing work across workers.
 *
 * @example
 * ```typescript
 * // Match every 3rd item (id % 3 === 0)
 * { id: { $mod: [3, 0] } }
 *
 * // Match even numbers
 * { count: { $mod: [2, 0] } }
 *
 * // Match odd numbers
 * { count: { $mod: [2, 1] } }
 *
 * // Distribute work across 4 workers (worker 2 handles id % 4 === 2)
 * { id: { $mod: [4, 2] } }
 * ```
 */
export interface ModOperator {
  /**
   * Array of [divisor, remainder]
   * Matches when value % divisor === remainder
   */
  $mod: [number, number]
}

/**
 * Union of all comparison operators
 *
 * @typeParam T - Type of the value being compared
 */
export type ComparisonOperator<T = unknown> =
  | EqOperator<T>
  | NeOperator<T>
  | GtOperator<T>
  | GteOperator<T>
  | LtOperator<T>
  | LteOperator<T>
  | InOperator<T>
  | NinOperator<T>
  | ModOperator

// =============================================================================
// String Operators
// =============================================================================

/**
 * Regular expression match operator
 *
 * Matches strings using regular expression patterns.
 *
 * @example
 * ```typescript
 * // Case-insensitive regex
 * { title: { $regex: /^hello/i } }
 *
 * // With options string
 * { title: { $regex: '^hello', $options: 'i' } }
 * ```
 */
export interface RegexOperator {
  /**
   * Regular expression pattern (string or RegExp object)
   */
  $regex: string | RegExp

  /**
   * Regex options (i=insensitive, m=multiline, s=dotall)
   * Only used when $regex is a string
   */
  $options?: string | undefined
}

/**
 * Starts-with string operator
 *
 * Matches strings that begin with the specified prefix.
 * More efficient than $regex for prefix matching.
 *
 * NOTE: ParqueDB Extension - This operator is not available in MongoDB.
 * For MongoDB compatibility, use $regex with ^ anchor: { field: { $regex: '^prefix' } }
 *
 * @example
 * ```typescript
 * // Match titles starting with "Hello"
 * { title: { $startsWith: 'Hello' } }
 *
 * // MongoDB-compatible equivalent
 * { title: { $regex: '^Hello' } }
 * ```
 */
export interface StartsWithOperator {
  $startsWith: string
}

/**
 * Ends-with string operator
 *
 * Matches strings that end with the specified suffix.
 *
 * NOTE: ParqueDB Extension - This operator is not available in MongoDB.
 * For MongoDB compatibility, use $regex with $ anchor: { field: { $regex: 'suffix$' } }
 *
 * @example
 * ```typescript
 * // Match emails ending with @example.com
 * { email: { $endsWith: '@example.com' } }
 *
 * // MongoDB-compatible equivalent
 * { email: { $regex: '@example\\.com$' } }
 * ```
 */
export interface EndsWithOperator {
  $endsWith: string
}

/**
 * Contains substring operator
 *
 * Matches strings that contain the specified substring anywhere.
 *
 * NOTE: ParqueDB Extension - This operator is not available in MongoDB.
 * For MongoDB compatibility, use $regex: { field: { $regex: 'substring' } }
 *
 * @example
 * ```typescript
 * // Match content containing "database"
 * { content: { $contains: 'database' } }
 *
 * // MongoDB-compatible equivalent
 * { content: { $regex: 'database' } }
 * ```
 */
export interface ContainsOperator {
  $contains: string
}

/**
 * Union of all string operators
 */
export type StringOperator =
  | RegexOperator
  | StartsWithOperator
  | EndsWithOperator
  | ContainsOperator

// =============================================================================
// Array Operators
// =============================================================================

/**
 * Array contains-all operator
 *
 * Matches arrays that contain all of the specified elements.
 *
 * @typeParam T - Type of array elements
 *
 * @example
 * ```typescript
 * // Match posts tagged with both 'tech' and 'database'
 * { tags: { $all: ['tech', 'database'] } }
 * ```
 */
export interface AllOperator<T = unknown> {
  $all: T[]
}

/**
 * Array element match operator
 *
 * Matches arrays where at least one element satisfies the filter.
 * Used for querying arrays of objects.
 *
 * @example
 * ```typescript
 * // Match posts with a comment scored above 10
 * { comments: { $elemMatch: { score: { $gt: 10 } } } }
 * ```
 */
export interface ElemMatchOperator {
  $elemMatch: Filter
}

/**
 * Array size operator
 *
 * Matches arrays with exactly the specified number of elements.
 *
 * @example
 * ```typescript
 * // Match posts with exactly 3 tags
 * { tags: { $size: 3 } }
 * ```
 */
export interface SizeOperator {
  $size: number
}

/**
 * Union of all array operators
 *
 * @typeParam T - Type of array elements
 */
export type ArrayOperator<T = unknown> =
  | AllOperator<T>
  | ElemMatchOperator
  | SizeOperator

// =============================================================================
// Existence Operators
// =============================================================================

/**
 * Field existence operator
 *
 * Checks whether a field exists or is absent in the document.
 *
 * @example
 * ```typescript
 * // Match posts that have a publishedAt field
 * { publishedAt: { $exists: true } }
 *
 * // Match posts without a deletedAt field
 * { deletedAt: { $exists: false } }
 * ```
 */
export interface ExistsOperator {
  /**
   * True to match documents with the field present,
   * false to match documents where the field is absent
   */
  $exists: boolean
}

/**
 * Type check operator
 *
 * Matches values that are of the specified BSON/JSON type.
 *
 * @example
 * ```typescript
 * // Match posts where views is a number
 * { views: { $type: 'number' } }
 *
 * // Match posts where tags is an array
 * { tags: { $type: 'array' } }
 * ```
 */
export interface TypeOperator {
  /**
   * Expected type name
   */
  $type: 'null' | 'boolean' | 'number' | 'string' | 'array' | 'object' | 'date'
}

/**
 * Union of existence and type checking operators
 */
export type ExistenceOperator =
  | ExistsOperator
  | TypeOperator

// =============================================================================
// Special Operators
// =============================================================================

/**
 * Full-text search operator
 *
 * Performs text search across FTS-indexed fields using stemming,
 * tokenization, and relevance scoring.
 *
 * Requires an FTS index to be created on the collection.
 *
 * @example
 * ```typescript
 * // Simple text search
 * { $text: { $search: 'parquet database performance' } }
 *
 * // With language and minimum score
 * { $text: { $search: 'database', $language: 'english', $minScore: 0.5 } }
 * ```
 */
export interface TextOperator {
  $text: {
    /**
     * Search query string
     * Supports phrase matching with quotes: "exact phrase"
     * Supports term exclusion with minus: -excluded
     */
    $search: string

    /**
     * Language for stemming and stopword removal
     * @default 'english'
     */
    $language?: string | undefined

    /**
     * Whether to perform case-sensitive matching
     * @default false
     */
    $caseSensitive?: boolean | undefined

    /**
     * Whether to treat diacritical marks as distinct
     * @default false
     */
    $diacriticSensitive?: boolean | undefined

    /**
     * Minimum relevance score threshold (0-1)
     * Results with scores below this are filtered out
     */
    $minScore?: number | undefined
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

/**
 * Vector similarity search operator
 *
 * Performs approximate nearest neighbor (ANN) search using vector embeddings.
 * Requires a vector index to be created on the target field.
 *
 * @example
 * ```typescript
 * // Search with a pre-computed vector
 * {
 *   $vector: {
 *     query: [0.1, 0.2, 0.3, ...],  // 1536-dimensional vector
 *     field: 'embedding',
 *     topK: 10,
 *     minScore: 0.7
 *   }
 * }
 *
 * // Search with text (auto-embedding)
 * {
 *   $vector: {
 *     query: 'semantic search query',
 *     field: 'embedding',
 *     topK: 10
 *   }
 * }
 * ```
 */
export interface VectorOperator {
  $vector: {
    /**
     * Query vector or text for auto-embedding
     * If string, will be embedded using the configured embedding model
     */
    query: number[] | string

    /**
     * Name of the field containing embedding vectors
     */
    field: string

    /**
     * Number of nearest neighbors to return
     */
    topK: number

    /**
     * Minimum similarity score threshold (0-1)
     * Results with lower similarity are filtered out
     */
    minScore?: number | undefined

    /**
     * Strategy for combining with metadata filters
     * @default 'auto'
     */
    strategy?: HybridSearchStrategy | undefined

    /**
     * HNSW efSearch parameter for quality/speed tradeoff
     * Higher values = better recall but slower search
     * @default 100
     */
    efSearch?: number | undefined

    // Legacy format support (deprecated, use query/field/topK instead)
    /** @deprecated Use `query` instead */
    $near?: number[] | undefined
    /** @deprecated Use `topK` instead */
    $k?: number | undefined
    /** @deprecated Use `field` instead */
    $field?: string | undefined
    /** @deprecated Use `minScore` instead */
    $minScore?: number | undefined
  }
}

/**
 * Geospatial proximity query operator
 *
 * Finds documents near a geographic point, optionally within a distance range.
 * Requires a geospatial index on the target field.
 *
 * @example
 * ```typescript
 * // Find locations within 10km of San Francisco
 * {
 *   location: {
 *     $geo: {
 *       $near: { lng: -122.4194, lat: 37.7749 },
 *       $maxDistance: 10000  // 10km in meters
 *     }
 *   }
 * }
 * ```
 */
export interface GeoOperator {
  $geo: {
    /**
     * Center point for proximity search
     */
    $near: {
      /** Longitude (-180 to 180) */
      lng: number
      /** Latitude (-90 to 90) */
      lat: number
    }
    /**
     * Maximum distance from center point in meters
     */
    $maxDistance?: number | undefined
    /**
     * Minimum distance from center point in meters
     */
    $minDistance?: number | undefined
  }
}

/**
 * Expression evaluation operator
 *
 * Allows comparing fields within the same document using aggregation operators.
 * Field references use the `$fieldName` syntax.
 *
 * @example
 * ```typescript
 * // Compare two fields for equality
 * { $expr: { $eq: ['$quantity', '$sold'] } }
 *
 * // Check if budget exceeds spending
 * { $expr: { $gt: ['$budget', '$spent'] } }
 *
 * // Compare field to literal
 * { $expr: { $gte: ['$score', 100] } }
 *
 * // Access nested fields
 * { $expr: { $lt: ['$user.balance', '$user.creditLimit'] } }
 * ```
 */
export interface ExprOperator {
  $expr: {
    /** Field equality comparison */
    $eq?: [unknown, unknown] | undefined
    /** Not equal comparison */
    $ne?: [unknown, unknown] | undefined
    /** Greater than comparison */
    $gt?: [unknown, unknown] | undefined
    /** Greater than or equal comparison */
    $gte?: [unknown, unknown] | undefined
    /** Less than comparison */
    $lt?: [unknown, unknown] | undefined
    /** Less than or equal comparison */
    $lte?: [unknown, unknown] | undefined
  }
}

/**
 * Query comment operator
 *
 * Adds a comment to the query for logging and debugging purposes.
 * Has no effect on query matching - purely for documentation.
 *
 * @example
 * ```typescript
 * // Document a complex query
 * {
 *   $comment: 'TICKET-123: Find active premium users for migration',
 *   status: 'active',
 *   tier: 'premium'
 * }
 *
 * // Add context for debugging
 * {
 *   $comment: 'Batch job run at 2024-06-15 - process every 5th record',
 *   id: { $mod: [5, 0] }
 * }
 * ```
 */
export interface CommentOperator {
  /**
   * Comment string for logging/debugging
   * This has no effect on query matching
   */
  $comment: string
}

/**
 * Union of special search operators (text, vector, geo)
 */
export type SpecialOperator =
  | TextOperator
  | VectorOperator
  | GeoOperator
  | ExprOperator
  | CommentOperator

// =============================================================================
// Logical Operators
// =============================================================================

/**
 * Logical AND operator
 *
 * All conditions in the array must match for a document to be included.
 *
 * @example
 * ```typescript
 * // Match published posts with more than 100 views
 * { $and: [{ status: 'published' }, { views: { $gt: 100 } }] }
 * ```
 */
export interface AndOperator {
  $and: Filter[]
}

/**
 * Logical OR operator
 *
 * At least one condition in the array must match for inclusion.
 *
 * @example
 * ```typescript
 * // Match posts that are either published or featured
 * { $or: [{ status: 'published' }, { featured: true }] }
 * ```
 */
export interface OrOperator {
  $or: Filter[]
}

/**
 * Logical NOT operator
 *
 * Inverts the condition - matches documents that don't satisfy the filter.
 *
 * @example
 * ```typescript
 * // Match posts that are NOT drafts
 * { $not: { status: 'draft' } }
 * ```
 */
export interface NotOperator {
  $not: Filter
}

/**
 * Logical NOR operator
 *
 * None of the conditions must match - inverse of OR.
 *
 * @example
 * ```typescript
 * // Match posts that are neither draft nor archived
 * { $nor: [{ status: 'draft' }, { status: 'archived' }] }
 * ```
 */
export interface NorOperator {
  $nor: Filter[]
}

/**
 * Union of all logical operators
 */
export type LogicalOperator =
  | AndOperator
  | OrOperator
  | NotOperator
  | NorOperator

// =============================================================================
// Field-Level Not Operator
// =============================================================================

/**
 * Field-level NOT operator
 *
 * Negates the result of a specific operator applied to a field.
 * This is different from the top-level $not which negates entire filter documents.
 *
 * @example
 * ```typescript
 * // Find users whose name doesn't start with 'admin'
 * { name: { $not: { $regex: '^admin' } } }
 *
 * // Find items where score is NOT greater than 100
 * { score: { $not: { $gt: 100 } } }
 *
 * // Find products not in the expensive category
 * { price: { $not: { $gte: 1000 } } }
 * ```
 */
export interface FieldNotOperator<T = unknown> {
  /**
   * The operator expression to negate
   */
  $not: Omit<FieldOperator<T>, '$not'> | Record<string, unknown>
}

// =============================================================================
// Field Filter
// =============================================================================

/**
 * Union of all operators that can be applied to a field
 *
 * @typeParam T - Type of the field value
 */
export type FieldOperator<T = unknown> =
  | ComparisonOperator<T>
  | StringOperator
  | ArrayOperator<T>
  | ExistenceOperator
  | FieldNotOperator<T>

/**
 * A field filter value
 *
 * Can be either:
 * - A direct value for equality matching: `{ field: 'value' }`
 * - An operator object: `{ field: { $gt: 10 } }`
 *
 * @typeParam T - Type of the field value
 */
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
  $and?: Filter[] | undefined

  /** Logical OR */
  $or?: Filter[] | undefined

  /** Logical NOT */
  $not?: Filter | undefined

  /** Logical NOR */
  $nor?: Filter[] | undefined

  /** Full-text search */
  $text?: TextOperator['$text'] | undefined

  /** Vector similarity */
  $vector?: VectorOperator['$vector'] | undefined

  /** Geospatial */
  $geo?: GeoOperator['$geo'] | undefined

  /** Expression evaluation (compare fields within document) */
  $expr?: ExprOperator['$expr'] | undefined

  /** Query comment for logging/debugging (no effect on matching) */
  $comment?: string | undefined
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Type guard to check if a value is a comparison operator
 *
 * @param value - Value to check
 * @returns True if value is a ComparisonOperator
 */
export function isComparisonOperator(value: unknown): value is ComparisonOperator {
  if (typeof value !== 'object' || value === null) return false
  const keys = Object.keys(value)
  return keys.length === 1 && keys[0] !== undefined && ['$eq', '$ne', '$gt', '$gte', '$lt', '$lte', '$in', '$nin', '$mod'].includes(keys[0])
}

/**
 * Type guard to check if a value is a string operator
 *
 * @param value - Value to check
 * @returns True if value is a StringOperator
 */
export function isStringOperator(value: unknown): value is StringOperator {
  if (typeof value !== 'object' || value === null) return false
  const keys = Object.keys(value)
  return keys.some(k => ['$regex', '$startsWith', '$endsWith', '$contains'].includes(k))
}

/**
 * Type guard to check if a value is an array operator
 *
 * @param value - Value to check
 * @returns True if value is an ArrayOperator
 */
export function isArrayOperator(value: unknown): value is ArrayOperator {
  if (typeof value !== 'object' || value === null) return false
  const keys = Object.keys(value)
  return keys.length === 1 && keys[0] !== undefined && ['$all', '$elemMatch', '$size'].includes(keys[0])
}

/**
 * Type guard to check if a value is an existence operator
 *
 * @param value - Value to check
 * @returns True if value is an ExistenceOperator
 */
export function isExistenceOperator(value: unknown): value is ExistenceOperator {
  if (typeof value !== 'object' || value === null) return false
  const keys = Object.keys(value)
  return keys.length === 1 && keys[0] !== undefined && ['$exists', '$type'].includes(keys[0])
}

/**
 * Type guard to check if a value is a field-level $not operator
 *
 * @param value - Value to check
 * @returns True if value is a FieldNotOperator
 */
export function isFieldNotOperator(value: unknown): value is FieldNotOperator {
  if (typeof value !== 'object' || value === null) return false
  const obj = value as Record<string, unknown>
  return '$not' in obj && typeof obj.$not === 'object' && obj.$not !== null
}

/**
 * Type guard to check if a value is any field operator (not a plain value)
 *
 * @param value - Value to check
 * @returns True if value is a FieldOperator
 */
export function isFieldOperator(value: unknown): value is FieldOperator {
  return (
    isComparisonOperator(value) ||
    isStringOperator(value) ||
    isArrayOperator(value) ||
    isExistenceOperator(value) ||
    isFieldNotOperator(value)
  )
}

/**
 * Check if a filter contains logical operators ($and, $or, $not, $nor)
 *
 * @param filter - Filter to check
 * @returns True if filter uses logical operators
 */
export function hasLogicalOperators(filter: Filter): boolean {
  return '$and' in filter || '$or' in filter || '$not' in filter || '$nor' in filter
}

/**
 * Check if a filter contains special operators ($text, $vector, $geo)
 *
 * @param filter - Filter to check
 * @returns True if filter uses special operators
 */
export function hasSpecialOperators(filter: Filter): boolean {
  return '$text' in filter || '$vector' in filter || '$geo' in filter
}

// =============================================================================
// Individual Operator Type Guards
// =============================================================================

// Helper function to check basic object structure
function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value) && !(value instanceof Date)
}

// -----------------------------------------------------------------------------
// Comparison Operator Type Guards
// -----------------------------------------------------------------------------

/**
 * Type guard for $eq operator
 * @typeParam T - Type of the value being compared
 */
export function is$Eq<T = unknown>(value: unknown): value is EqOperator<T> {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$eq' in value
}

/**
 * Type guard for $ne operator
 * @typeParam T - Type of the value being compared
 */
export function is$Ne<T = unknown>(value: unknown): value is NeOperator<T> {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$ne' in value
}

/**
 * Type guard for $gt operator
 * @typeParam T - Type of the value being compared
 */
export function is$Gt<T = unknown>(value: unknown): value is GtOperator<T> {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$gt' in value
}

/**
 * Type guard for $gte operator
 * @typeParam T - Type of the value being compared
 */
export function is$Gte<T = unknown>(value: unknown): value is GteOperator<T> {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$gte' in value
}

/**
 * Type guard for $lt operator
 * @typeParam T - Type of the value being compared
 */
export function is$Lt<T = unknown>(value: unknown): value is LtOperator<T> {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$lt' in value
}

/**
 * Type guard for $lte operator
 * @typeParam T - Type of the value being compared
 */
export function is$Lte<T = unknown>(value: unknown): value is LteOperator<T> {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$lte' in value
}

/**
 * Type guard for $in operator
 * @typeParam T - Type of the values in the array
 */
export function is$In<T = unknown>(value: unknown): value is InOperator<T> {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$in' in value && Array.isArray((value as Record<string, unknown>).$in)
}

/**
 * Type guard for $nin operator
 * @typeParam T - Type of the values in the array
 */
export function is$Nin<T = unknown>(value: unknown): value is NinOperator<T> {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$nin' in value && Array.isArray((value as Record<string, unknown>).$nin)
}

// -----------------------------------------------------------------------------
// String Operator Type Guards
// -----------------------------------------------------------------------------

/**
 * Type guard for $regex operator
 */
export function is$Regex(value: unknown): value is RegexOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  if (!('$regex' in value)) return false
  // Allow $regex alone or with $options
  const allowedKeys = keys.filter(k => k !== '$regex' && k !== '$options')
  return allowedKeys.length === 0
}

/**
 * Type guard for $startsWith operator
 */
export function is$StartsWith(value: unknown): value is StartsWithOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$startsWith' in value
}

/**
 * Type guard for $endsWith operator
 */
export function is$EndsWith(value: unknown): value is EndsWithOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$endsWith' in value
}

/**
 * Type guard for $contains operator
 */
export function is$Contains(value: unknown): value is ContainsOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$contains' in value
}

// -----------------------------------------------------------------------------
// Array Operator Type Guards
// -----------------------------------------------------------------------------

/**
 * Type guard for $all operator
 * @typeParam T - Type of array elements
 */
export function is$All<T = unknown>(value: unknown): value is AllOperator<T> {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$all' in value && Array.isArray((value as Record<string, unknown>).$all)
}

/**
 * Type guard for $elemMatch operator
 */
export function is$ElemMatch(value: unknown): value is ElemMatchOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  if (keys.length !== 1 || !('$elemMatch' in value)) return false
  const inner = (value as Record<string, unknown>).$elemMatch
  return typeof inner === 'object' && inner !== null
}

/**
 * Type guard for $size operator
 */
export function is$Size(value: unknown): value is SizeOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$size' in value && typeof (value as Record<string, unknown>).$size === 'number'
}

// -----------------------------------------------------------------------------
// Existence Operator Type Guards
// -----------------------------------------------------------------------------

/**
 * Type guard for $exists operator
 */
export function is$Exists(value: unknown): value is ExistsOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$exists' in value && typeof (value as Record<string, unknown>).$exists === 'boolean'
}

/**
 * Type guard for $type operator
 */
const VALID_TYPES = ['null', 'boolean', 'number', 'string', 'array', 'object', 'date'] as const
export function is$Type(value: unknown): value is TypeOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  if (keys.length !== 1 || !('$type' in value)) return false
  const typeValue = (value as Record<string, unknown>).$type
  return typeof typeValue === 'string' && VALID_TYPES.includes(typeValue as typeof VALID_TYPES[number])
}

// -----------------------------------------------------------------------------
// Logical Operator Type Guards
// -----------------------------------------------------------------------------

/**
 * Type guard for $and operator
 */
export function is$And(value: unknown): value is AndOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$and' in value && Array.isArray((value as Record<string, unknown>).$and)
}

/**
 * Type guard for $or operator
 */
export function is$Or(value: unknown): value is OrOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$or' in value && Array.isArray((value as Record<string, unknown>).$or)
}

/**
 * Type guard for $not operator (top-level)
 */
export function is$Not(value: unknown): value is NotOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  if (keys.length !== 1 || !('$not' in value)) return false
  const inner = (value as Record<string, unknown>).$not
  return typeof inner === 'object' && inner !== null
}

/**
 * Type guard for $nor operator
 */
export function is$Nor(value: unknown): value is NorOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  return keys.length === 1 && '$nor' in value && Array.isArray((value as Record<string, unknown>).$nor)
}

// -----------------------------------------------------------------------------
// Special Operator Type Guards
// -----------------------------------------------------------------------------

/**
 * Type guard for $text operator
 */
export function is$Text(value: unknown): value is TextOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  if (keys.length !== 1 || !('$text' in value)) return false
  const text = (value as Record<string, unknown>).$text
  if (typeof text !== 'object' || text === null) return false
  return '$search' in text
}

/**
 * Type guard for $vector operator
 */
export function is$Vector(value: unknown): value is VectorOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  if (keys.length !== 1 || !('$vector' in value)) return false
  const vector = (value as Record<string, unknown>).$vector
  return typeof vector === 'object' && vector !== null
}

/**
 * Type guard for $geo operator
 */
export function is$Geo(value: unknown): value is GeoOperator {
  if (!isObject(value)) return false
  const keys = Object.keys(value)
  if (keys.length !== 1 || !('$geo' in value)) return false
  const geo = (value as Record<string, unknown>).$geo
  if (typeof geo !== 'object' || geo === null) return false
  return '$near' in geo
}
