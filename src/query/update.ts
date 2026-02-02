/**
 * MongoDB-style Update Operators Engine for ParqueDB
 *
 * Provides immutable document updates with support for:
 * - Field operators: $set, $unset, $rename, $setOnInsert
 * - Numeric operators: $inc, $mul, $min, $max
 * - Array operators: $push, $pull, $pullAll, $addToSet, $pop
 * - Date operators: $currentDate
 * - Bitwise operators: $bit
 */

import type { UpdateInput } from '../types/update'
import type { Filter } from '../types/filter'
import { deepEqual, compareValues, createSafeRegex } from '../utils'
import { validatePath } from '../utils/path-safety'
import { matchesFilter as filterMatchesFilter } from './filter'

// =============================================================================
// Main Update Function
// =============================================================================

/**
 * Apply update operators to a document
 * Returns new document (immutable - does not modify the original)
 *
 * @param doc - Original document to update
 * @param update - MongoDB-style update operators
 * @param options - Optional settings (e.g., for $setOnInsert during upserts)
 * @returns New document with updates applied
 *
 * @example
 * const doc = { name: 'John', count: 5, tags: ['a'] }
 * const updated = applyUpdate(doc, {
 *   $set: { name: 'Jane' },
 *   $inc: { count: 1 },
 *   $push: { tags: 'b' }
 * })
 * // updated = { name: 'Jane', count: 6, tags: ['a', 'b'] }
 */
export function applyUpdate<T extends Record<string, unknown>>(
  doc: T,
  update: UpdateInput<T>,
  options?: UpdateApplyOptions
): T {
  let result = { ...doc }

  // $set - Set field values
  if (update.$set) {
    for (const [key, value] of Object.entries(update.$set)) {
      result = setField(result, key, value)
    }
  }

  // $unset - Remove fields
  if (update.$unset) {
    for (const key of Object.keys(update.$unset)) {
      result = unsetField(result, key)
    }
  }

  // $inc - Increment numeric fields
  if (update.$inc) {
    for (const [key, amount] of Object.entries(update.$inc)) {
      const current = (getField(result, key) as number) ?? 0
      result = setField(result, key, current + (amount as number))
    }
  }

  // $mul - Multiply numeric fields
  if (update.$mul) {
    for (const [key, factor] of Object.entries(update.$mul)) {
      const current = (getField(result, key) as number) ?? 0
      result = setField(result, key, current * (factor as number))
    }
  }

  // $min - Set to minimum of current value and specified value
  if (update.$min) {
    for (const [key, value] of Object.entries(update.$min)) {
      const current = getField(result, key)
      if (current === undefined || compareValues(value, current) < 0) {
        result = setField(result, key, value)
      }
    }
  }

  // $max - Set to maximum of current value and specified value
  if (update.$max) {
    for (const [key, value] of Object.entries(update.$max)) {
      const current = getField(result, key)
      if (current === undefined || compareValues(value, current) > 0) {
        result = setField(result, key, value)
      }
    }
  }

  // $push - Append to array
  if (update.$push) {
    for (const [key, value] of Object.entries(update.$push)) {
      const arr = [...((getField(result, key) as unknown[]) || [])]

      if (isPushModifier(value)) {
        const modifier = value as PushModifier
        let items = [...modifier.$each]

        // Handle $position - insert at specific index
        if (modifier.$position !== undefined) {
          arr.splice(modifier.$position, 0, ...items)
        } else {
          arr.push(...items)
        }

        // Handle $sort - sort the array after push
        if (modifier.$sort !== undefined) {
          sortArray(arr, modifier.$sort)
        }

        // Handle $slice - limit array size
        if (modifier.$slice !== undefined) {
          applySlice(arr, modifier.$slice)
        }
      } else {
        arr.push(value)
      }

      result = setField(result, key, arr)
    }
  }

  // $pull - Remove elements from array matching a condition
  if (update.$pull) {
    for (const [key, condition] of Object.entries(update.$pull)) {
      const arr = (getField(result, key) as unknown[]) || []
      const filtered = arr.filter((item) => {
        if (typeof condition === 'object' && condition !== null && !Array.isArray(condition)) {
          // Check if it's a filter object (has $ operators or field conditions)
          // For $pull, we treat any object as a potential filter/match condition
          return !matchesFilter(item, condition as Filter)
        }
        // Direct value comparison (primitives, arrays, dates)
        return !deepEqual(item, condition)
      })
      result = setField(result, key, filtered)
    }
  }

  // $pullAll - Remove all matching values from array
  if (update.$pullAll) {
    for (const [key, values] of Object.entries(update.$pullAll)) {
      const arr = (getField(result, key) as unknown[]) || []
      const valuesToRemove = values as unknown[]
      const filtered = arr.filter(
        (item) => !valuesToRemove.some((v) => deepEqual(item, v))
      )
      result = setField(result, key, filtered)
    }
  }

  // $addToSet - Add unique value(s) to array
  if (update.$addToSet) {
    for (const [key, value] of Object.entries(update.$addToSet)) {
      const arr = [...((getField(result, key) as unknown[]) || [])]

      // Handle $each modifier
      const items = isEachModifier(value)
        ? (value as { $each: unknown[] }).$each
        : [value]

      for (const item of items) {
        if (!arr.some((existing) => deepEqual(existing, item))) {
          arr.push(item)
        }
      }

      result = setField(result, key, arr)
    }
  }

  // $pop - Remove first or last element from array
  if (update.$pop) {
    for (const [key, direction] of Object.entries(update.$pop)) {
      const arr = [...((getField(result, key) as unknown[]) || [])]
      if (direction === 1) {
        arr.pop() // Remove last
      } else if (direction === -1) {
        arr.shift() // Remove first
      }
      result = setField(result, key, arr)
    }
  }

  // $rename - Rename a field
  if (update.$rename) {
    for (const [oldKey, newKey] of Object.entries(update.$rename)) {
      const value = getField(result, oldKey)
      if (value !== undefined) {
        result = unsetField(result, oldKey)
        result = setField(result, newKey as string, value)
      }
    }
  }

  // $currentDate - Set field to current date
  if (update.$currentDate) {
    for (const [key, value] of Object.entries(update.$currentDate)) {
      let date: Date | number
      if (value === true) {
        date = new Date()
      } else if (
        typeof value === 'object' &&
        value !== null &&
        '$type' in value
      ) {
        const typeSpec = value as { $type: 'date' | 'timestamp' }
        if (typeSpec.$type === 'timestamp') {
          date = Date.now()
        } else {
          date = new Date()
        }
      } else {
        date = new Date()
      }
      result = setField(result, key, date)
    }
  }

  // $setOnInsert - Set fields only if this is an insert (upsert)
  if (update.$setOnInsert && options?.isInsert) {
    for (const [key, value] of Object.entries(update.$setOnInsert)) {
      result = setField(result, key, value)
    }
  }

  // $bit - Bitwise operations
  if (update.$bit) {
    for (const [key, operations] of Object.entries(update.$bit)) {
      const ops = operations as { and?: number; or?: number; xor?: number }
      let current = (getField(result, key) as number) ?? 0

      if (typeof current !== 'number') {
        current = 0
      }

      if (ops.and !== undefined) {
        current = current & ops.and
      }
      if (ops.or !== undefined) {
        current = current | ops.or
      }
      if (ops.xor !== undefined) {
        current = current ^ ops.xor
      }

      result = setField(result, key, current)
    }
  }

  return result
}

// =============================================================================
// Options
// =============================================================================

/**
 * Options for applyUpdate function
 */
export interface UpdateApplyOptions {
  /**
   * Whether this is an insert operation (for $setOnInsert)
   */
  isInsert?: boolean
}

// =============================================================================
// Push Modifier Types
// =============================================================================

/**
 * Push modifier for advanced array operations
 */
interface PushModifier {
  $each: unknown[]
  $position?: number
  $slice?: number
  $sort?: 1 | -1 | Record<string, 1 | -1>
}

/**
 * Check if value is a push modifier object
 */
function isPushModifier(value: unknown): value is PushModifier {
  return (
    typeof value === 'object' &&
    value !== null &&
    '$each' in value &&
    Array.isArray((value as PushModifier).$each)
  )
}

/**
 * Check if value is an $each modifier
 */
function isEachModifier(value: unknown): value is { $each: unknown[] } {
  return (
    typeof value === 'object' &&
    value !== null &&
    '$each' in value &&
    Array.isArray((value as { $each: unknown[] }).$each)
  )
}

/**
 * Check if an object is a filter (has $ operators)
 */
function isFilterObject(obj: unknown): boolean {
  if (typeof obj !== 'object' || obj === null) return false
  const keys = Object.keys(obj)
  return keys.some((k) => k.startsWith('$'))
}

// =============================================================================
// Array Helpers
// =============================================================================

/**
 * Sort an array in place
 */
function sortArray(arr: unknown[], sort: 1 | -1 | Record<string, 1 | -1>): void {
  if (typeof sort === 'number') {
    // Simple sort direction
    arr.sort((a, b) => {
      const cmp = compareValues(a, b)
      return sort === 1 ? cmp : -cmp
    })
  } else {
    // Sort by field(s)
    const sortFields = Object.entries(sort)
    arr.sort((a, b) => {
      for (const [field, direction] of sortFields) {
        const aVal = getField(a as Record<string, unknown>, field)
        const bVal = getField(b as Record<string, unknown>, field)
        const cmp = compareValues(aVal, bVal)
        if (cmp !== 0) {
          return direction === 1 ? cmp : -cmp
        }
      }
      return 0
    })
  }
}

/**
 * Apply $slice to an array (in place)
 */
function applySlice(arr: unknown[], slice: number): void {
  if (slice === 0) {
    // Remove all elements
    arr.length = 0
  } else if (slice > 0) {
    // Keep first N elements
    if (arr.length > slice) {
      arr.length = slice
    }
  } else {
    // Keep last N elements (slice is negative)
    const keep = Math.abs(slice)
    if (arr.length > keep) {
      arr.splice(0, arr.length - keep)
    }
  }
}

// =============================================================================
// Field Access Helpers
// =============================================================================

/**
 * Get a field value from an object using dot notation for nested paths
 *
 * @example
 * getField({ a: { b: 1 } }, 'a.b') // => 1
 * getField({ a: [1, 2] }, 'a.0') // => 1
 */
export function getField(obj: unknown, path: string): unknown {
  validatePath(path)
  if (obj === null || obj === undefined) {
    return undefined
  }

  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) {
      return undefined
    }

    if (typeof current !== 'object') {
      return undefined
    }

    if (Array.isArray(current)) {
      const index = parseInt(part, 10)
      if (isNaN(index)) {
        return undefined
      }
      current = current[index]
    } else {
      current = (current as Record<string, unknown>)[part]
    }
  }

  return current
}

/**
 * Set a field value in an object using dot notation for nested paths
 * Returns a new object (immutable)
 *
 * @example
 * setField({ a: { b: 1 } }, 'a.b', 2) // => { a: { b: 2 } }
 * setField({ a: { b: 1 } }, 'a.c', 3) // => { a: { b: 1, c: 3 } }
 * setField({ items: ['a', 'b'] }, 'items.1', 'x') // => { items: ['a', 'x'] }
 */
export function setField<T>(obj: T, path: string, value: unknown): T {
  validatePath(path)
  const parts = path.split('.')

  // Base case: single part
  if (parts.length === 1) {
    const key = parts[0]!  // length === 1 ensures key exists
    const index = parseInt(key, 10)

    // Handle array index assignment
    if (Array.isArray(obj) && !isNaN(index)) {
      const newArr = [...obj]
      newArr[index] = value
      return newArr as unknown as T
    }

    return {
      ...obj,
      [key]: value,
    }
  }

  // Recursive case: navigate to nested object
  const [head, ...tail] = parts
  if (!head) {
    return obj
  }
  const tailPath = tail.join('.')
  const headIndex = parseInt(head, 10)

  // Check if current obj is an array and head is a numeric index
  if (Array.isArray(obj) && !isNaN(headIndex)) {
    const newArr = [...obj]
    const currentValue = newArr[headIndex]

    // Determine if we need an array or object for the next level
    const nextPart = tail[0]
    const needsArray = nextPart !== undefined && !isNaN(parseInt(nextPart, 10))

    let nested: unknown
    if (currentValue === undefined || currentValue === null) {
      nested = needsArray ? [] : {}
    } else if (typeof currentValue === 'object') {
      nested = Array.isArray(currentValue) ? [...currentValue] : { ...currentValue }
    } else {
      nested = needsArray ? [] : {}
    }

    newArr[headIndex] = setField(nested as Record<string, unknown>, tailPath, value)
    return newArr as unknown as T
  }

  const currentValue = obj === null || obj === undefined
    ? undefined
    : (obj as Record<string, unknown>)[head]

  // Determine if we need an array or object for the next level
  const nextPart = tail[0]
  const needsArray = nextPart !== undefined && !isNaN(parseInt(nextPart, 10))

  let nested: unknown
  if (currentValue === undefined || currentValue === null) {
    nested = needsArray ? [] : {}
  } else if (typeof currentValue === 'object') {
    nested = Array.isArray(currentValue) ? [...currentValue] : { ...currentValue }
  } else {
    // Overwrite primitive with object/array
    nested = needsArray ? [] : {}
  }

  const updatedNested = setField(
    nested as Record<string, unknown>,
    tailPath,
    value
  )

  return {
    ...obj,
    [head]: updatedNested,
  }
}

/**
 * Remove a field from an object using dot notation for nested paths
 * Returns a new object (immutable)
 *
 * @example
 * unsetField({ a: { b: 1, c: 2 } }, 'a.b') // => { a: { c: 2 } }
 */
export function unsetField<T>(obj: T, path: string): T {
  validatePath(path)
  const parts = path.split('.')

  // Base case: single part - remove the field
  if (parts.length === 1) {
    const key = parts[0]!  // length === 1 ensures key exists
    const { [key]: _, ...rest } = obj as Record<string, unknown>
    return rest as T
  }

  // Recursive case: navigate to nested object
  const [head, ...tail] = parts
  if (!head) {
    return obj
  }
  const tailPath = tail.join('.')

  const currentValue = (obj as Record<string, unknown>)[head]

  if (currentValue === undefined || currentValue === null) {
    // Field doesn't exist, return unchanged
    return obj
  }

  if (typeof currentValue !== 'object') {
    // Can't navigate into non-object, return unchanged
    return obj
  }

  const nested = Array.isArray(currentValue)
    ? [...currentValue]
    : { ...currentValue }

  const updatedNested = unsetField(nested as Record<string, unknown>, tailPath)

  return {
    ...obj,
    [head as string]: updatedNested,
  }
}

// =============================================================================
// Comparison and Equality Helpers
// =============================================================================

// compareValues and deepEqual are imported from ../utils

// =============================================================================
// Filter Matching (for $pull)
// =============================================================================

// matchesFilter is imported from ./filter and re-exported below
// The canonical implementation handles both primitives and objects
const matchesFilter = filterMatchesFilter

// =============================================================================
// Validation
// =============================================================================

/**
 * Validate an update operation
 * @throws Error if update is invalid
 */
export function validateUpdate(update: UpdateInput): void {
  // Check for conflicting operators on the same field
  const modifiedFields = new Set<string>()

  const operators = [
    '$set',
    '$unset',
    '$inc',
    '$mul',
    '$min',
    '$max',
    '$push',
    '$pull',
    '$pullAll',
    '$addToSet',
    '$pop',
    '$rename',
    '$currentDate',
    '$bit',
  ] as const

  for (const op of operators) {
    const opValue = update[op as keyof UpdateInput]
    if (opValue && typeof opValue === 'object') {
      for (const field of Object.keys(opValue)) {
        // $rename has special handling - check both old and new field names
        if (op === '$rename') {
          const newField = (opValue as Record<string, string>)[field]!
          if (modifiedFields.has(field)) {
            throw new Error(
              `Conflicting operators: field '${field}' modified by multiple operators`
            )
          }
          if (modifiedFields.has(newField)) {
            throw new Error(
              `Conflicting operators: field '${newField}' modified by multiple operators`
            )
          }
          modifiedFields.add(field)
          modifiedFields.add(newField)
        } else {
          if (modifiedFields.has(field)) {
            throw new Error(
              `Conflicting operators: field '${field}' modified by multiple operators`
            )
          }
          modifiedFields.add(field)
        }
      }
    }
  }
}

// =============================================================================
// Exports
// =============================================================================

// Re-export utility functions for backwards compatibility
export { compareValues, deepEqual } from '../utils'

export {
  matchesFilter,
  sortArray,
  applySlice,
}
