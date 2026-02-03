/**
 * Type Guard Factory and Utilities
 *
 * This module provides factory functions for creating type guards, reducing
 * boilerplate in type guard definitions across the codebase.
 *
 * @module types/type-guards
 *
 * @example
 * ```typescript
 * // Create a type guard for { $eq: T }
 * const isEqOperator = createSingleKeyGuard<EqOperator>('$eq')
 *
 * // Create a type guard that checks for array value
 * const isInOperator = createSingleKeyGuard<InOperator>('$in', isArray)
 *
 * // Check if value matches one of several types
 * isOneOf(value, [is$Eq, is$Ne, is$Gt])
 *
 * // Check if all items in array match a type
 * isArrayOf(items, is$Eq)
 * ```
 */

// =============================================================================
// Core Utilities
// =============================================================================

/**
 * Check if a value is a non-null object (not array, not Date)
 */
export function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value) && !(value instanceof Date)
}

/**
 * Check if a value is an array
 */
export function isArray(value: unknown): value is unknown[] {
  return Array.isArray(value)
}

/**
 * Check if a value is a string
 */
export function isString(value: unknown): value is string {
  return typeof value === 'string'
}

/**
 * Check if a value is a number
 */
export function isNumber(value: unknown): value is number {
  return typeof value === 'number'
}

/**
 * Check if a value is a boolean
 */
export function isBoolean(value: unknown): value is boolean {
  return typeof value === 'boolean'
}

// =============================================================================
// Type Guard Factory
// =============================================================================

/**
 * Type guard function signature
 */
export type TypeGuard<T> = (value: unknown) => value is T

/**
 * Validator function for property values
 */
export type ValueValidator = (value: unknown) => boolean

/**
 * Create a type guard for objects with a single required key.
 *
 * This factory handles the most common pattern in our type guards:
 * - Check if value is an object
 * - Check if object has exactly one key (or specific count)
 * - Check if that key is the expected one
 * - Optionally validate the value at that key
 *
 * @param key - The required key name (e.g., '$eq', '$gt')
 * @param valueValidator - Optional function to validate the value at the key
 * @returns A type guard function
 *
 * @example
 * ```typescript
 * // Simple single-key check
 * const is$Eq = createSingleKeyGuard<EqOperator>('$eq')
 *
 * // With value validation (must be array)
 * const is$In = createSingleKeyGuard<InOperator>('$in', isArray)
 *
 * // With value validation (must be number)
 * const is$Size = createSingleKeyGuard<SizeOperator>('$size', isNumber)
 * ```
 */
export function createSingleKeyGuard<T>(
  key: string,
  valueValidator?: ValueValidator
): TypeGuard<T> {
  return (value: unknown): value is T => {
    if (!isObject(value)) return false
    const keys = Object.keys(value)
    if (keys.length !== 1 || !(key in value)) return false
    if (valueValidator && !valueValidator(value[key])) return false
    return true
  }
}

/**
 * Create a type guard for objects that must contain a specific key,
 * but may have additional allowed keys.
 *
 * @param requiredKey - The key that must be present
 * @param allowedKeys - Additional keys that are permitted
 * @param valueValidator - Optional function to validate the value at the required key
 * @returns A type guard function
 *
 * @example
 * ```typescript
 * // $regex can have optional $options
 * const is$Regex = createMultiKeyGuard<RegexOperator>(
 *   '$regex',
 *   ['$options']
 * )
 * ```
 */
export function createMultiKeyGuard<T>(
  requiredKey: string,
  allowedKeys: string[] = [],
  valueValidator?: ValueValidator
): TypeGuard<T> {
  const allAllowed = new Set([requiredKey, ...allowedKeys])
  return (value: unknown): value is T => {
    if (!isObject(value)) return false
    if (!(requiredKey in value)) return false
    // Check that all keys are in the allowed set
    const keys = Object.keys(value)
    if (!keys.every(k => allAllowed.has(k))) return false
    if (valueValidator && !valueValidator(value[requiredKey])) return false
    return true
  }
}

/**
 * Create a type guard for objects with a single key that has a nested object value
 * with a required nested key.
 *
 * @param key - The outer key (e.g., '$text', '$vector')
 * @param nestedKey - The required key in the nested object (e.g., '$search', '$near')
 * @returns A type guard function
 *
 * @example
 * ```typescript
 * // { $text: { $search: '...' } }
 * const is$Text = createNestedKeyGuard<TextOperator>('$text', '$search')
 *
 * // { $geo: { $near: {...} } }
 * const is$Geo = createNestedKeyGuard<GeoOperator>('$geo', '$near')
 * ```
 */
export function createNestedKeyGuard<T>(
  key: string,
  nestedKey: string
): TypeGuard<T> {
  return (value: unknown): value is T => {
    if (!isObject(value)) return false
    const keys = Object.keys(value)
    if (keys.length !== 1 || !(key in value)) return false
    const nested = value[key]
    if (!isObject(nested)) return false
    return nestedKey in nested
  }
}

/**
 * Create a type guard that checks for presence of a key with defined value.
 * Used for update operator type guards that check `'$key' in obj && obj.$key !== undefined`.
 *
 * @param key - The key to check for
 * @returns A type guard function
 *
 * @example
 * ```typescript
 * const hasSet = createHasKeyGuard<UpdateInput, '$set'>('$set')
 * ```
 */
export function createHasKeyGuard<T, K extends keyof T>(
  key: K
): (value: T) => value is T & Required<Pick<T, K>> {
  return (value: T): value is T & Required<Pick<T, K>> => {
    return key in (value as object) && value[key] !== undefined
  }
}

// =============================================================================
// Combinator Utilities
// =============================================================================

/**
 * Check if a value matches any of the provided type guards.
 *
 * @param value - The value to check
 * @param guards - Array of type guard functions to try
 * @returns True if any guard returns true
 *
 * @example
 * ```typescript
 * // Check if value is any comparison operator
 * if (isOneOf(value, [is$Eq, is$Ne, is$Gt, is$Gte, is$Lt, is$Lte])) {
 *   // value is ComparisonOperator
 * }
 * ```
 */
export function isOneOf<T>(
  value: unknown,
  guards: Array<TypeGuard<T>>
): value is T {
  return guards.some(guard => guard(value))
}

/**
 * Check if all items in an array match a type guard.
 *
 * @param items - The array to check
 * @param guard - The type guard to apply to each item
 * @returns True if all items pass the guard
 *
 * @example
 * ```typescript
 * // Check if all filters are valid
 * if (isArrayOf(filters, isFilter)) {
 *   // filters is Filter[]
 * }
 * ```
 */
export function isArrayOf<T>(
  items: unknown,
  guard: TypeGuard<T>
): items is T[] {
  return Array.isArray(items) && items.every(guard)
}

/**
 * Create a type guard that checks for an array of items matching a guard.
 *
 * @param itemGuard - The type guard to apply to each item
 * @returns A type guard for arrays of the item type
 *
 * @example
 * ```typescript
 * const isFilterArray = createArrayGuard<Filter>(isFilter)
 * ```
 */
export function createArrayGuard<T>(
  itemGuard: TypeGuard<T>
): TypeGuard<T[]> {
  return (value: unknown): value is T[] => {
    return Array.isArray(value) && value.every(itemGuard)
  }
}

/**
 * Create a combined type guard from multiple guards (OR logic).
 *
 * @param guards - Array of type guards to combine
 * @returns A new type guard that returns true if any input guard returns true
 *
 * @example
 * ```typescript
 * const isComparisonOp = combineGuards<ComparisonOperator>([
 *   is$Eq, is$Ne, is$Gt, is$Gte, is$Lt, is$Lte, is$In, is$Nin
 * ])
 * ```
 */
export function combineGuards<T>(
  guards: Array<TypeGuard<T>>
): TypeGuard<T> {
  return (value: unknown): value is T => {
    return guards.some(guard => guard(value))
  }
}

// =============================================================================
// Specific Value Validators
// =============================================================================

/**
 * Create a validator that checks if value is one of allowed values
 */
export function oneOf<T>(...allowed: T[]): ValueValidator {
  return (value: unknown) => allowed.includes(value as T)
}

/**
 * Create a validator that checks for a nested object
 */
export function isNestedObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

/**
 * Validator: value must be an array
 */
export const mustBeArray: ValueValidator = isArray

/**
 * Validator: value must be a number
 */
export const mustBeNumber: ValueValidator = isNumber

/**
 * Validator: value must be a boolean
 */
export const mustBeBoolean: ValueValidator = isBoolean

/**
 * Validator: value must be a string
 */
export const mustBeString: ValueValidator = isString

/**
 * Validator: value must be an object (non-null, non-array)
 */
export const mustBeObject: ValueValidator = isNestedObject

/**
 * Validator: value must be a two-element number array [divisor, remainder]
 */
export function isTupleOfNumbers(length: number): ValueValidator {
  return (value: unknown) => {
    return Array.isArray(value) &&
      value.length === length &&
      value.every(v => typeof v === 'number')
  }
}
