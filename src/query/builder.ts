/**
 * QueryBuilder - Fluent Query Builder for ParqueDB
 *
 * Provides a chainable API for constructing MongoDB-style queries.
 *
 * @example
 * const results = await collection.builder()
 *   .where('status', 'eq', 'published')
 *   .andWhere('score', 'gte', 80)
 *   .orderBy('createdAt', 'desc')
 *   .limit(10)
 *   .find()
 *
 * @example
 * const { filter, options } = new QueryBuilder()
 *   .where('category', 'in', ['tech', 'science'])
 *   .orderBy('views', 'desc')
 *   .select(['title', 'author'])
 *   .build()
 */

import type { Filter, FieldFilter } from '../types/filter'
import type { FindOptions, Projection, SortSpec } from '../types/options'
import type { Entity } from '../types/entity'

/**
 * Interface for the collection methods used by QueryBuilder.
 * This avoids circular dependencies with the Collection class.
 */
interface QueryableCollection<T> {
  find(filter?: Filter, options?: FindOptions<T>): Promise<Entity<T>[]>
  findOne(filter?: Filter, options?: FindOptions<T>): Promise<Entity<T> | null>
  count(filter?: Filter): Promise<number>
}

/** Supported comparison operators */
export type ComparisonOp =
  | 'eq' | '='
  | 'ne' | '!='
  | 'gt' | '>'
  | 'gte' | '>='
  | 'lt' | '<'
  | 'lte' | '<='
  | 'in'
  | 'nin'

/** Supported negation operators (for notWhere) */
export type NegationOp = ComparisonOp | StringOp

/** Supported string operators */
export type StringOp =
  | 'regex'
  | 'startsWith'
  | 'endsWith'
  | 'contains'

/** Supported existence operators */
export type ExistenceOp = 'exists'

/** All supported operators */
export type QueryOp = ComparisonOp | StringOp | ExistenceOp

/** Operator to MongoDB operator mapping */
const operatorMap: Record<string, string> = {
  'eq': '$eq',
  '=': '$eq',
  'ne': '$ne',
  '!=': '$ne',
  'gt': '$gt',
  '>': '$gt',
  'gte': '$gte',
  '>=': '$gte',
  'lt': '$lt',
  '<': '$lt',
  'lte': '$lte',
  '<=': '$lte',
  'in': '$in',
  'nin': '$nin',
  'regex': '$regex',
  'startsWith': '$startsWith',
  'endsWith': '$endsWith',
  'contains': '$contains',
  'exists': '$exists',
}

/** Valid operators set for validation */
const validOperators = new Set(Object.keys(operatorMap))

/** A single condition in the query */
interface Condition {
  field: string
  op: QueryOp
  value: unknown
  negated?: boolean  // true for notWhere conditions
}

/** Array operator types */
type ArrayOpType = 'all' | 'elemMatch' | 'size'

/** A condition for array operators (handled separately from regular operators) */
interface ArrayCondition {
  field: string
  arrayOp: ArrayOpType
  value: unknown
}

/** A condition for modulo operator */
interface ModCondition {
  field: string
  divisor: number
  remainder: number
}

/** Condition group type - reserved for future query building enhancements */
export type _ConditionGroup = 'and' | 'or'

/** Internal state for building queries */
interface BuilderState {
  conditions: Condition[]
  arrayConditions: ArrayCondition[]
  modConditions: ModCondition[]
  orGroups: Condition[][]
  sort: SortSpec
  limit?: number
  skip?: number
  project?: Projection
}

/**
 * Fluent query builder for ParqueDB collections
 *
 * @typeParam T - The entity type being queried
 */
export class QueryBuilder<T = Record<string, unknown>> {
  private collection?: QueryableCollection<T>
  private state: BuilderState

  /**
   * Create a new QueryBuilder
   *
   * @param collection - Optional collection to execute queries against
   */
  constructor(collection?: QueryableCollection<T>) {
    this.collection = collection
    this.state = {
      conditions: [],
      arrayConditions: [],
      modConditions: [],
      orGroups: [],
      sort: {},
      limit: undefined,
      skip: undefined,
      project: undefined,
    }
  }

  /**
   * Add a WHERE condition to the query
   *
   * @param field - The field name (supports dot notation for nested fields)
   * @param op - The comparison operator
   * @param value - The value to compare against
   * @returns this for method chaining
   *
   * @example
   * builder.where('status', 'eq', 'published')
   * builder.where('score', '>=', 100)
   * builder.where('tags', 'in', ['tech', 'science'])
   */
  where(field: string, op: QueryOp, value: unknown): this {
    if (!validOperators.has(op)) {
      throw new Error(`Invalid operator: ${op}`)
    }

    this.state.conditions.push({ field, op, value })
    return this
  }

  /**
   * Add an AND condition to the query
   *
   * @param field - The field name
   * @param op - The comparison operator
   * @param value - The value to compare against
   * @returns this for method chaining
   *
   * @example
   * builder
   *   .where('status', 'eq', 'published')
   *   .andWhere('featured', 'eq', true)
   */
  andWhere(field: string, op: QueryOp, value: unknown): this {
    return this.where(field, op, value)
  }

  /**
   * Add a negated WHERE condition to the query (field-level $not)
   *
   * Uses field-level $not operator to negate the result of the specified operator.
   * This is different from top-level $not which negates entire sub-filters.
   *
   * @param field - The field name (supports dot notation for nested fields)
   * @param op - The comparison operator to negate
   * @param value - The value to compare against
   * @returns this for method chaining
   *
   * @example
   * // Find users whose name doesn't start with 'admin'
   * builder.notWhere('name', 'regex', '^admin')
   *
   * @example
   * // Find items where score is NOT greater than 100
   * builder.notWhere('score', 'gt', 100)
   *
   * @example
   * // Combine with regular conditions
   * builder
   *   .where('status', 'eq', 'active')
   *   .notWhere('category', 'in', ['restricted', 'private'])
   */
  notWhere(field: string, op: QueryOp, value: unknown): this {
    if (!validOperators.has(op)) {
      throw new Error(`Invalid operator: ${op}`)
    }

    this.state.conditions.push({ field, op, value, negated: true })
    return this
  }

  // =============================================================================
  // Array Operators
  // =============================================================================

  /**
   * Add a $all array condition to the query
   *
   * Matches arrays that contain all of the specified elements.
   *
   * @param field - The field name containing the array
   * @param values - Array of values that must all be present
   * @returns this for method chaining
   *
   * @example
   * // Match posts tagged with both 'tech' and 'database'
   * builder.whereAll('tags', ['tech', 'database'])
   */
  whereAll(field: string, values: unknown[]): this {
    this.state.arrayConditions.push({ field, arrayOp: 'all', value: values })
    return this
  }

  /**
   * Add a $elemMatch array condition to the query
   *
   * Matches arrays where at least one element satisfies the filter.
   * Used for querying arrays of objects.
   *
   * @param field - The field name containing the array
   * @param filter - Filter that at least one array element must match
   * @returns this for method chaining
   *
   * @example
   * // Match posts with a comment scored above 10
   * builder.whereElemMatch('comments', { score: { $gt: 10 } })
   *
   * @example
   * // Match orders with items meeting multiple criteria
   * builder.whereElemMatch('items', { quantity: { $gte: 5 }, price: { $lt: 100 } })
   */
  whereElemMatch(field: string, filter: Filter): this {
    this.state.arrayConditions.push({ field, arrayOp: 'elemMatch', value: filter })
    return this
  }

  /**
   * Add a $size array condition to the query
   *
   * Matches arrays with exactly the specified number of elements.
   *
   * @param field - The field name containing the array
   * @param size - The exact array length to match
   * @returns this for method chaining
   * @throws Error if size is negative
   *
   * @example
   * // Match posts with exactly 3 tags
   * builder.whereSize('tags', 3)
   */
  whereSize(field: string, size: number): this {
    if (size < 0) {
      throw new Error('Size cannot be negative')
    }
    this.state.arrayConditions.push({ field, arrayOp: 'size', value: size })
    return this
  }

  // =============================================================================
  // Modulo Operator
  // =============================================================================

  /**
   * Add a $mod (modulo) condition to the query
   *
   * Matches values where value % divisor equals remainder.
   * Useful for filtering by multiples or distributing work across workers.
   *
   * @param field - The field name containing the numeric value
   * @param divisor - The divisor for the modulo operation
   * @param remainder - The expected remainder
   * @returns this for method chaining
   * @throws Error if divisor is zero or negative
   *
   * @example
   * // Match every 3rd item (id % 3 === 0)
   * builder.whereMod('id', 3, 0)
   *
   * @example
   * // Match even numbers
   * builder.whereMod('count', 2, 0)
   *
   * @example
   * // Match odd numbers
   * builder.whereMod('count', 2, 1)
   *
   * @example
   * // Distribute work across 4 workers (worker 2)
   * builder.whereMod('id', 4, 2)
   */
  whereMod(field: string, divisor: number, remainder: number): this {
    if (divisor <= 0) {
      throw new Error('Divisor must be a positive number')
    }
    if (remainder < 0) {
      throw new Error('Remainder cannot be negative')
    }
    this.state.modConditions.push({ field, divisor, remainder })
    return this
  }

  /**
   * Add an OR condition to the query
   *
   * @param field - The field name
   * @param op - The comparison operator
   * @param value - The value to compare against
   * @returns this for method chaining
   *
   * @example
   * builder
   *   .where('status', 'eq', 'published')
   *   .orWhere('featured', 'eq', true)
   */
  orWhere(field: string, op: QueryOp, value: unknown): this {
    if (!validOperators.has(op)) {
      throw new Error(`Invalid operator: ${op}`)
    }

    // If this is the first orWhere, move current conditions to first OR group
    if (this.state.orGroups.length === 0 && this.state.conditions.length > 0) {
      this.state.orGroups.push([...this.state.conditions])
      this.state.conditions = []
    }

    // Add the new condition as a new OR group
    this.state.orGroups.push([{ field, op, value }])

    return this
  }

  /**
   * Add ORDER BY clause
   *
   * @param field - The field to sort by
   * @param direction - Sort direction ('asc' or 'desc'), defaults to 'asc'
   * @returns this for method chaining
   *
   * @example
   * builder.orderBy('createdAt', 'desc')
   * builder.orderBy('title', 'asc')
   */
  orderBy(field: string, direction: 'asc' | 'desc' = 'asc'): this {
    this.state.sort[field] = direction
    return this
  }

  /**
   * Set maximum number of results to return
   *
   * @param n - Maximum number of results
   * @returns this for method chaining
   *
   * @example
   * builder.limit(20)
   */
  limit(n: number): this {
    if (n < 0) {
      throw new Error('Limit cannot be negative')
    }
    this.state.limit = n
    return this
  }

  /**
   * Set number of results to skip (for pagination)
   *
   * @param n - Number of results to skip
   * @returns this for method chaining
   *
   * @example
   * builder.offset(10) // Skip first 10 results
   */
  offset(n: number): this {
    if (n < 0) {
      throw new Error('Offset cannot be negative')
    }
    this.state.skip = n
    return this
  }

  /**
   * Alias for offset()
   *
   * @param n - Number of results to skip
   * @returns this for method chaining
   */
  skip(n: number): this {
    return this.offset(n)
  }

  /**
   * Set field projection (which fields to include)
   *
   * @param fields - Array of field names to include
   * @returns this for method chaining
   *
   * @example
   * builder.select(['title', 'author', 'createdAt'])
   */
  select(fields: string[]): this {
    this.state.project = {}
    for (const field of fields) {
      this.state.project[field] = 1
    }
    return this
  }

  /**
   * Build the filter and options objects
   *
   * @returns Object containing filter and options for find()
   *
   * @example
   * const { filter, options } = builder.build()
   * await collection.find(filter, options)
   */
  build(): { filter: Filter; options: FindOptions<T> } {
    const filter = this.buildFilter()
    const options = this.buildOptions()
    return { filter, options }
  }

  /**
   * Build the filter object from conditions
   */
  private buildFilter(): Filter {
    // Handle OR groups
    if (this.state.orGroups.length > 0) {
      return {
        $or: this.state.orGroups.map(group => this.conditionsToFilter(group))
      }
    }

    // Check if we have multiple conditions on the same field
    const fieldCounts = new Map<string, number>()
    for (const cond of this.state.conditions) {
      fieldCounts.set(cond.field, (fieldCounts.get(cond.field) || 0) + 1)
    }
    // Also count array conditions
    for (const cond of this.state.arrayConditions) {
      fieldCounts.set(cond.field, (fieldCounts.get(cond.field) || 0) + 1)
    }
    // Also count mod conditions
    for (const cond of this.state.modConditions) {
      fieldCounts.set(cond.field, (fieldCounts.get(cond.field) || 0) + 1)
    }

    const hasDuplicateFields = Array.from(fieldCounts.values()).some(count => count > 1)

    if (hasDuplicateFields) {
      // Use $and when same field appears multiple times
      const allConditions: Filter[] = [
        ...this.state.conditions.map(cond => this.conditionToFilter(cond)),
        ...this.state.arrayConditions.map(cond => this.arrayConditionToFilter(cond)),
        ...this.state.modConditions.map(cond => this.modConditionToFilter(cond))
      ]
      return {
        $and: allConditions
      }
    }

    return this.conditionsToFilter(this.state.conditions)
  }

  /**
   * Convert an array of conditions to a filter object
   */
  private conditionsToFilter(conditions: Condition[]): Filter {
    const filter: Filter = {}

    for (const cond of conditions) {
      const mongoOp = operatorMap[cond.op]
      if (!mongoOp) continue

      if (cond.negated) {
        // Use field-level $not: { field: { $not: { $op: value } } }
        filter[cond.field] = { $not: { [mongoOp]: cond.value } } as FieldFilter
      } else {
        filter[cond.field] = { [mongoOp]: cond.value } as FieldFilter
      }
    }

    // Also add array conditions
    for (const arrayCond of this.state.arrayConditions) {
      const arrayFilter = this.arrayConditionToFilter(arrayCond)
      Object.assign(filter, arrayFilter)
    }

    // Also add mod conditions
    for (const modCond of this.state.modConditions) {
      const modFilter = this.modConditionToFilter(modCond)
      Object.assign(filter, modFilter)
    }

    return filter
  }

  /**
   * Convert a mod condition to a filter object
   */
  private modConditionToFilter(cond: ModCondition): Filter {
    return { [cond.field]: { $mod: [cond.divisor, cond.remainder] } as FieldFilter }
  }

  /**
   * Convert an array condition to a filter object
   */
  private arrayConditionToFilter(cond: ArrayCondition): Filter {
    switch (cond.arrayOp) {
      case 'all':
        return { [cond.field]: { $all: cond.value as unknown[] } as FieldFilter }
      case 'elemMatch':
        return { [cond.field]: { $elemMatch: cond.value as Filter } as FieldFilter }
      case 'size':
        return { [cond.field]: { $size: cond.value as number } as FieldFilter }
    }
  }

  /**
   * Convert a single condition to a filter object
   */
  private conditionToFilter(cond: Condition): Filter {
    const mongoOp = operatorMap[cond.op]
    if (!mongoOp) return {}

    if (cond.negated) {
      // Use field-level $not: { field: { $not: { $op: value } } }
      return { [cond.field]: { $not: { [mongoOp]: cond.value } } as FieldFilter }
    }
    return { [cond.field]: { [mongoOp]: cond.value } as FieldFilter }
  }

  /**
   * Build the options object
   */
  private buildOptions(): FindOptions<T> {
    const options: FindOptions<T> = {}

    if (Object.keys(this.state.sort).length > 0) {
      options.sort = this.state.sort
    }

    if (this.state.limit !== undefined) {
      options.limit = this.state.limit
    }

    if (this.state.skip !== undefined) {
      options.skip = this.state.skip
    }

    if (this.state.project !== undefined) {
      options.project = this.state.project
    }

    return options
  }

  /**
   * Execute the query and return all matching entities
   *
   * @returns Promise resolving to array of matching entities
   * @throws Error if no collection is set
   *
   * @example
   * const results = await builder.find()
   */
  async find(): Promise<Entity<T>[]> {
    if (!this.collection) {
      throw new Error('No collection set. Pass a collection to the constructor or use collection.builder().')
    }

    const { filter, options } = this.build()
    return this.collection.find(filter, options)
  }

  /**
   * Execute the query and return the first matching entity
   *
   * @returns Promise resolving to single entity or null
   * @throws Error if no collection is set
   *
   * @example
   * const result = await builder.findOne()
   */
  async findOne(): Promise<Entity<T> | null> {
    if (!this.collection) {
      throw new Error('No collection set. Pass a collection to the constructor or use collection.builder().')
    }

    const { filter, options } = this.build()
    return this.collection.findOne(filter, { ...options, limit: 1 })
  }

  /**
   * Count matching entities
   *
   * @returns Promise resolving to count of matching entities
   * @throws Error if no collection is set
   *
   * @example
   * const count = await builder.count()
   */
  async count(): Promise<number> {
    if (!this.collection) {
      throw new Error('No collection set. Pass a collection to the constructor or use collection.builder().')
    }

    const { filter } = this.build()
    return this.collection.count(filter)
  }

  /**
   * Create an independent copy of this builder
   *
   * @returns A new QueryBuilder with the same state
   *
   * @example
   * const baseQuery = builder.where('status', 'eq', 'published')
   * const techQuery = baseQuery.clone().andWhere('category', 'eq', 'tech')
   * const lifestyleQuery = baseQuery.clone().andWhere('category', 'eq', 'lifestyle')
   */
  clone(): QueryBuilder<T> {
    const cloned = new QueryBuilder<T>(this.collection)
    cloned.state = {
      conditions: [...this.state.conditions],
      arrayConditions: [...this.state.arrayConditions],
      modConditions: [...this.state.modConditions],
      orGroups: this.state.orGroups.map(group => [...group]),
      sort: { ...this.state.sort },
      limit: this.state.limit,
      skip: this.state.skip,
      project: this.state.project ? { ...this.state.project } : undefined,
    }
    return cloned
  }
}
