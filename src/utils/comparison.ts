/**
 * Shared Comparison and Value Utilities for ParqueDB
 *
 * This module provides canonical implementations of common utility functions
 * used across the codebase for value comparison, equality checking, cloning,
 * and nested value access.
 */

// =============================================================================
// Deep Equality
// =============================================================================

/**
 * Deep equality check for two values
 *
 * Handles:
 * - Primitives (strict equality)
 * - null/undefined (treated as equal - see note below)
 * - Dates (compared by timestamp)
 * - Arrays (element-wise comparison)
 * - Objects (key-value comparison)
 *
 * **Null/Undefined Behavior:**
 * For deep equality (used by $eq, $ne, $in, $nin operators), null and undefined
 * are treated as equivalent. This follows MongoDB's behavior where missing fields
 * are treated the same as fields explicitly set to null for equality comparisons.
 *
 * Examples:
 * - `deepEqual(null, null)` => true
 * - `deepEqual(undefined, undefined)` => true
 * - `deepEqual(null, undefined)` => true (equivalent for equality)
 * - `deepEqual(null, 0)` => false
 * - `deepEqual(undefined, '')` => false
 *
 * @param a - First value
 * @param b - Second value
 * @returns true if values are deeply equal
 *
 * @example
 * deepEqual({ a: 1 }, { a: 1 }) // true
 * deepEqual([1, 2], [1, 2]) // true
 * deepEqual(new Date('2024-01-01'), new Date('2024-01-01')) // true
 */
export function deepEqual(a: unknown, b: unknown): boolean {
  // Identical or both null/undefined
  if (a === b) return true
  if (a === null || a === undefined) return b === null || b === undefined

  // Date comparison
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() === b.getTime()
  }

  // Array comparison
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false
    return a.every((v, i) => deepEqual(v, b[i]))
  }

  // Object comparison - both must be non-null objects at this point
  if (typeof a === 'object' && typeof b === 'object' && a !== null && b !== null) {
    const aObj = a as Record<string, unknown>
    const bObj = b as Record<string, unknown>
    const aKeys = Object.keys(aObj)
    const bKeys = Object.keys(bObj)
    if (aKeys.length !== bKeys.length) return false
    return aKeys.every(k => deepEqual(aObj[k], bObj[k]))
  }

  // Primitive comparison (already failed a === b)
  return false
}

// =============================================================================
// Value Comparison for Ordering
// =============================================================================

/**
 * Compare two values for ordering
 *
 * Handles:
 * - null/undefined (sort first, treated as equal to each other)
 * - Numbers (numeric comparison)
 * - Strings (lexicographic comparison)
 * - Dates (timestamp comparison)
 * - Booleans (false < true)
 * - Cross-type (string conversion fallback)
 *
 * **Null/Undefined Behavior for Sorting:**
 * For ordering/sorting purposes, null and undefined are treated as equivalent
 * and sort before all other values. This ensures consistent sort order
 * regardless of whether a field is missing or explicitly set to null.
 *
 * Examples:
 * - `compareValues(null, null)` => 0 (equal)
 * - `compareValues(undefined, undefined)` => 0 (equal)
 * - `compareValues(null, undefined)` => 0 (equivalent for sorting)
 * - `compareValues(null, 0)` => -1 (nullish sorts first)
 * - `compareValues(1, null)` => 1 (values sort after nullish)
 *
 * @param a - First value
 * @param b - Second value
 * @returns -1 if a < b, 0 if equal, 1 if a > b
 *
 * @example
 * compareValues(1, 2) // -1
 * compareValues('b', 'a') // 1
 * compareValues(null, 1) // -1
 */
export function compareValues(a: unknown, b: unknown): number {
  // Nulls sort first
  if (a === null || a === undefined) {
    return b === null || b === undefined ? 0 : -1
  }
  if (b === null || b === undefined) return 1

  // Same type comparisons
  if (typeof a === 'number' && typeof b === 'number') {
    // Handle NaN - NaN comparisons should return NaN to indicate uncomparable
    if (Number.isNaN(a) || Number.isNaN(b)) return NaN
    return a - b
  }
  if (typeof a === 'string' && typeof b === 'string') {
    // Use simple lexicographic comparison (ASCII order) for consistency
    if (a < b) return -1
    if (a > b) return 1
    return 0
  }
  if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime()
  if (typeof a === 'boolean' && typeof b === 'boolean') return (a ? 1 : 0) - (b ? 1 : 0)

  // Fallback to string comparison
  return String(a).localeCompare(String(b))
}

// =============================================================================
// Nested Value Access
// =============================================================================

/**
 * Get a nested value from an object using dot notation
 *
 * Supports:
 * - Simple paths: 'name'
 * - Nested paths: 'address.city'
 * - Array indices: 'items.0.name'
 *
 * @param obj - Object to read from
 * @param path - Dot-notation path
 * @returns Value at path, or undefined if not found
 *
 * @example
 * getNestedValue({ a: { b: 1 } }, 'a.b') // 1
 * getNestedValue({ items: [{ x: 1 }] }, 'items.0.x') // 1
 */
export function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) return undefined
    if (typeof current !== 'object') return undefined

    if (Array.isArray(current)) {
      const index = parseInt(part, 10)
      if (isNaN(index)) return undefined
      current = current[index]
    } else {
      current = (current as Record<string, unknown>)[part]
    }
  }

  return current
}

// =============================================================================
// Deep Clone
// =============================================================================

/**
 * Deep clone an object, preserving Date instances
 *
 * Uses structuredClone for safe, complete deep copies without
 * custom serialization sentinels.
 *
 * @param obj - Object to clone
 * @returns Deep cloned copy
 *
 * @example
 * const original = { date: new Date(), nested: { value: 1 } }
 * const clone = deepClone(original)
 * clone.nested.value = 2 // original.nested.value is still 1
 */
export function deepClone<T>(obj: T): T {
  if (obj === null || obj === undefined) return obj
  return structuredClone(obj)
}

// =============================================================================
// Null/Undefined Helpers
// =============================================================================

/**
 * Check if a value is null or undefined (nullish)
 *
 * This is the canonical nullish check used throughout ParqueDB for
 * consistent null/undefined handling.
 *
 * @param value - Value to check
 * @returns true if value is null or undefined
 *
 * @example
 * isNullish(null) // true
 * isNullish(undefined) // true
 * isNullish(0) // false
 * isNullish('') // false
 * isNullish(false) // false
 */
export function isNullish(value: unknown): value is null | undefined {
  return value === null || value === undefined
}

/**
 * Check if a value is defined (not null and not undefined)
 *
 * This is the inverse of isNullish and is useful for type narrowing.
 *
 * @param value - Value to check
 * @returns true if value is not null and not undefined
 *
 * @example
 * isDefined(0) // true
 * isDefined('') // true
 * isDefined(false) // true
 * isDefined(null) // false
 * isDefined(undefined) // false
 */
export function isDefined<T>(value: T | null | undefined): value is T {
  return value !== null && value !== undefined
}

// =============================================================================
// Type Helpers
// =============================================================================

/**
 * Get the type of a value (matching MongoDB's $type operator)
 *
 * **Null/Undefined Behavior:**
 * Both null and undefined return 'null' as the type. This matches MongoDB's
 * $type operator behavior where undefined/missing fields are treated as null type.
 *
 * Examples:
 * - `getValueType(null)` => 'null'
 * - `getValueType(undefined)` => 'null'
 *
 * @param value - Value to check
 * @returns Type string: 'null', 'array', 'date', 'boolean', 'number', 'string', 'object'
 *
 * @example
 * getValueType(null) // 'null'
 * getValueType([1, 2]) // 'array'
 * getValueType(new Date()) // 'date'
 */
export function getValueType(value: unknown): string {
  if (value === null) return 'null'
  if (value === undefined) return 'null'
  if (Array.isArray(value)) return 'array'
  if (value instanceof Date) return 'date'
  return typeof value // 'boolean', 'number', 'string', 'object'
}
