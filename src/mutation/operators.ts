/**
 * MongoDB-style Update Operators for ParqueDB Mutation Layer
 *
 * This module provides functions to apply update operators to documents.
 * It is designed to be pure (no side effects) and immutable (returns new objects).
 *
 * Supported operators:
 * - Field: $set, $unset, $rename, $setOnInsert
 * - Numeric: $inc, $mul, $min, $max
 * - Array: $push, $pull, $pullAll, $addToSet, $pop
 * - Date: $currentDate
 * - Bitwise: $bit
 */

import type { UpdateInput, Filter, EntityId } from '../types'
import type { ApplyOperatorsOptions, ApplyOperatorsResult, RelationshipOperation } from './types'
import { compareValues, deepEqual } from '../utils'

// =============================================================================
// Path Security
// =============================================================================

/**
 * Dangerous path segments that could lead to prototype pollution
 */
const UNSAFE_PATH_SEGMENTS = new Set(['__proto__', 'constructor', 'prototype'])

/**
 * Check if a dot-notation path contains unsafe segments that could lead to prototype pollution
 *
 * @param path - The dot-notation path to check
 * @returns true if the path contains unsafe segments
 */
export function isUnsafePath(path: string): boolean {
  const parts = path.split('.')
  return parts.some(part => UNSAFE_PATH_SEGMENTS.has(part))
}

/**
 * Validate that a path is safe, throwing an error if it contains unsafe segments
 *
 * @param path - The dot-notation path to validate
 * @throws Error if the path is unsafe
 */
export function validatePath(path: string): void {
  if (isUnsafePath(path)) {
    throw new Error(`Unsafe path detected: "${path}" contains a prototype pollution attempt`)
  }
}

// =============================================================================
// Main Operator Application
// =============================================================================

/**
 * Apply all update operators to a document
 *
 * @param doc - Original document
 * @param update - Update operators to apply
 * @param options - Options for operator application
 * @returns Result with updated document and metadata
 */
export function applyOperators<T extends Record<string, unknown>>(
  doc: T,
  update: UpdateInput<T>,
  options?: ApplyOperatorsOptions
): ApplyOperatorsResult {
  const modifiedFields: string[] = []
  const relationshipOps: RelationshipOperation[] = []
  let result: Record<string, unknown> = { ...doc }
  const timestamp = options?.timestamp || new Date()

  // $set - Set field values
  if (update.$set) {
    for (const [key, value] of Object.entries(update.$set)) {
      result = setField(result, key, value)
      modifiedFields.push(key)
    }
  }

  // $unset - Remove fields
  if (update.$unset) {
    for (const key of Object.keys(update.$unset)) {
      result = unsetField(result, key)
      modifiedFields.push(key)
    }
  }

  // $inc - Increment numeric fields
  if (update.$inc) {
    for (const [key, amount] of Object.entries(update.$inc)) {
      const current = (getField(result, key) as number) ?? 0
      if (typeof current !== 'number' && current !== undefined && current !== null) {
        throw new Error(`Cannot apply $inc to non-numeric field: ${key}`)
      }
      result = setField(result, key, (current || 0) + (amount as number))
      modifiedFields.push(key)
    }
  }

  // $mul - Multiply numeric fields
  if (update.$mul) {
    for (const [key, factor] of Object.entries(update.$mul)) {
      const current = (getField(result, key) as number) ?? 0
      result = setField(result, key, current * (factor as number))
      modifiedFields.push(key)
    }
  }

  // $min - Set to minimum of current and specified value
  if (update.$min) {
    for (const [key, value] of Object.entries(update.$min)) {
      const current = getField(result, key)
      if (current === undefined || compareValues(value, current) < 0) {
        result = setField(result, key, value)
        modifiedFields.push(key)
      }
    }
  }

  // $max - Set to maximum of current and specified value
  if (update.$max) {
    for (const [key, value] of Object.entries(update.$max)) {
      const current = getField(result, key)
      if (current === undefined || compareValues(value, current) > 0) {
        result = setField(result, key, value)
        modifiedFields.push(key)
      }
    }
  }

  // $push - Append to array
  if (update.$push) {
    for (const [key, value] of Object.entries(update.$push)) {
      const arr = [...((getField(result, key) as unknown[]) || [])]

      if (isPushModifier(value)) {
        const modifier = value as PushModifier
        const items = [...modifier.$each]

        // Handle $position
        if (modifier.$position !== undefined) {
          arr.splice(modifier.$position, 0, ...items)
        } else {
          arr.push(...items)
        }

        // Handle $sort
        if (modifier.$sort !== undefined) {
          sortArray(arr, modifier.$sort)
        }

        // Handle $slice
        if (modifier.$slice !== undefined) {
          applySlice(arr, modifier.$slice)
        }
      } else {
        arr.push(value)
      }

      result = setField(result, key, arr)
      modifiedFields.push(key)
    }
  }

  // $pull - Remove elements from array
  if (update.$pull) {
    for (const [key, condition] of Object.entries(update.$pull)) {
      const arr = (getField(result, key) as unknown[]) || []
      const filtered = arr.filter((item) => {
        if (isFilterCondition(condition)) {
          return !matchesPullCondition(item, condition as Record<string, unknown>)
        }
        return !deepEqual(item, condition)
      })
      result = setField(result, key, filtered)
      modifiedFields.push(key)
    }
  }

  // $pullAll - Remove all matching values
  if (update.$pullAll) {
    for (const [key, values] of Object.entries(update.$pullAll)) {
      const arr = (getField(result, key) as unknown[]) || []
      const valuesToRemove = values as unknown[]
      const filtered = arr.filter(
        (item) => !valuesToRemove.some((v) => deepEqual(item, v))
      )
      result = setField(result, key, filtered)
      modifiedFields.push(key)
    }
  }

  // $addToSet - Add unique value(s) to array
  if (update.$addToSet) {
    for (const [key, value] of Object.entries(update.$addToSet)) {
      const arr = [...((getField(result, key) as unknown[]) || [])]

      const items = isEachModifier(value)
        ? (value as { $each: unknown[] }).$each
        : [value]

      for (const item of items) {
        if (!arr.some((existing) => deepEqual(existing, item))) {
          arr.push(item)
        }
      }

      result = setField(result, key, arr)
      modifiedFields.push(key)
    }
  }

  // $pop - Remove first or last element
  if (update.$pop) {
    for (const [key, direction] of Object.entries(update.$pop)) {
      const arr = [...((getField(result, key) as unknown[]) || [])]
      if (direction === 1) {
        arr.pop()
      } else if (direction === -1) {
        arr.shift()
      }
      result = setField(result, key, arr)
      modifiedFields.push(key)
    }
  }

  // $rename - Rename fields
  if (update.$rename) {
    for (const [oldKey, newKey] of Object.entries(update.$rename)) {
      const value = getField(result, oldKey)
      if (value !== undefined) {
        result = unsetField(result, oldKey)
        result = setField(result, newKey as string, value)
        modifiedFields.push(oldKey, newKey as string)
      }
    }
  }

  // $currentDate - Set field to current date
  if (update.$currentDate) {
    for (const [key, value] of Object.entries(update.$currentDate)) {
      let date: Date | number
      if (value === true) {
        date = timestamp
      } else if (typeof value === 'object' && value !== null && '$type' in value) {
        const typeSpec = value as { $type: 'date' | 'timestamp' }
        date = typeSpec.$type === 'timestamp' ? timestamp.getTime() : timestamp
      } else {
        date = timestamp
      }
      result = setField(result, key, date)
      modifiedFields.push(key)
    }
  }

  // $setOnInsert - Set fields only on insert
  if (update.$setOnInsert && options?.isInsert) {
    for (const [key, value] of Object.entries(update.$setOnInsert)) {
      result = setField(result, key, value)
      modifiedFields.push(key)
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
      modifiedFields.push(key)
    }
  }

  // $link - Add relationships (handled separately)
  if (update.$link) {
    for (const [predicate, value] of Object.entries(update.$link)) {
      const targets = Array.isArray(value) ? value : [value]
      relationshipOps.push({
        type: 'link',
        predicate,
        targets: targets as EntityId[],
      })
    }
  }

  // $unlink - Remove relationships (handled separately)
  if (update.$unlink) {
    for (const [predicate, value] of Object.entries(update.$unlink)) {
      if (value === '$all') {
        relationshipOps.push({
          type: 'unlink',
          predicate,
          targets: [], // Empty targets means unlink all
        })
      } else {
        const targets = Array.isArray(value) ? value : [value]
        relationshipOps.push({
          type: 'unlink',
          predicate,
          targets: targets as EntityId[],
        })
      }
    }
  }

  return {
    document: result,
    modifiedFields: [...new Set(modifiedFields)],
    relationshipOps,
  }
}

// =============================================================================
// Field Access Helpers
// =============================================================================

/**
 * Get a field value using dot notation
 * @throws Error if path contains unsafe segments (prototype pollution attempt)
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
 * Set a field value using dot notation (immutable)
 * @throws Error if path contains unsafe segments (prototype pollution attempt)
 */
export function setField<T>(obj: T, path: string, value: unknown): T {
  validatePath(path)
  const parts = path.split('.')

  if (parts.length === 1) {
    const key = parts[0]!
    const index = parseInt(key, 10)

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

  const [head, ...tail] = parts
  if (!head) {
    return obj
  }
  const tailPath = tail.join('.')
  const headIndex = parseInt(head, 10)

  if (Array.isArray(obj) && !isNaN(headIndex)) {
    const newArr = [...obj]
    const currentValue = newArr[headIndex]

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
 * Remove a field using dot notation (immutable)
 * @throws Error if path contains unsafe segments (prototype pollution attempt)
 */
export function unsetField<T>(obj: T, path: string): T {
  validatePath(path)
  const parts = path.split('.')

  if (parts.length === 1) {
    const key = parts[0]!
    const { [key]: _, ...rest } = obj as Record<string, unknown>
    return rest as T
  }

  const [head, ...tail] = parts
  if (!head) {
    return obj
  }
  const tailPath = tail.join('.')

  const currentValue = (obj as Record<string, unknown>)[head]

  if (currentValue === undefined || currentValue === null) {
    return obj
  }

  if (typeof currentValue !== 'object') {
    return obj
  }

  const nested = Array.isArray(currentValue)
    ? [...currentValue]
    : { ...currentValue }

  const updatedNested = unsetField(nested as Record<string, unknown>, tailPath)

  return {
    ...obj,
    [head]: updatedNested,
  }
}

// =============================================================================
// Push Modifier Helpers
// =============================================================================

interface PushModifier {
  $each: unknown[]
  $position?: number
  $slice?: number
  $sort?: 1 | -1 | Record<string, 1 | -1>
}

function isPushModifier(value: unknown): value is PushModifier {
  return (
    typeof value === 'object' &&
    value !== null &&
    '$each' in value &&
    Array.isArray((value as PushModifier).$each)
  )
}

function isEachModifier(value: unknown): value is { $each: unknown[] } {
  return (
    typeof value === 'object' &&
    value !== null &&
    '$each' in value &&
    Array.isArray((value as { $each: unknown[] }).$each)
  )
}

function sortArray(arr: unknown[], sort: 1 | -1 | Record<string, 1 | -1>): void {
  if (typeof sort === 'number') {
    arr.sort((a, b) => {
      const cmp = compareValues(a, b)
      return sort === 1 ? cmp : -cmp
    })
  } else {
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

function applySlice(arr: unknown[], slice: number): void {
  if (slice === 0) {
    arr.length = 0
  } else if (slice > 0) {
    if (arr.length > slice) {
      arr.length = slice
    }
  } else {
    const keep = Math.abs(slice)
    if (arr.length > keep) {
      arr.splice(0, arr.length - keep)
    }
  }
}

// =============================================================================
// Pull Condition Helpers
// =============================================================================

function isFilterCondition(condition: unknown): boolean {
  if (typeof condition !== 'object' || condition === null || Array.isArray(condition)) {
    return false
  }
  const keys = Object.keys(condition)
  return keys.length > 0
}

function matchesPullCondition(item: unknown, condition: Record<string, unknown>): boolean {
  const keys = Object.keys(condition)

  // Check if it's a comparison operator condition ($lt, $gt, etc.)
  if (keys.some(k => k.startsWith('$'))) {
    for (const [op, opValue] of Object.entries(condition)) {
      switch (op) {
        case '$lt':
          if (typeof item === 'number' && item < (opValue as number)) return true
          break
        case '$lte':
          if (typeof item === 'number' && item <= (opValue as number)) return true
          break
        case '$gt':
          if (typeof item === 'number' && item > (opValue as number)) return true
          break
        case '$gte':
          if (typeof item === 'number' && item >= (opValue as number)) return true
          break
        case '$eq':
          if (deepEqual(item, opValue)) return true
          break
        case '$ne':
          if (!deepEqual(item, opValue)) return true
          break
        case '$in':
          if (Array.isArray(opValue) && opValue.some(v => deepEqual(item, v))) return true
          break
        case '$nin':
          if (Array.isArray(opValue) && !opValue.some(v => deepEqual(item, v))) return true
          break
      }
    }
    return false
  }

  // It's a field match condition (e.g., { spam: true })
  if (item && typeof item === 'object') {
    const itemObj = item as Record<string, unknown>
    for (const [field, fieldValue] of Object.entries(condition)) {
      if (deepEqual(itemObj[field], fieldValue)) {
        return true
      }
    }
  }

  return false
}

// =============================================================================
// Validation
// =============================================================================

/** Valid update operators */
const VALID_UPDATE_OPERATORS = new Set([
  '$set', '$unset', '$rename', '$setOnInsert',
  '$inc', '$mul', '$min', '$max',
  '$push', '$pull', '$pullAll', '$addToSet', '$pop',
  '$currentDate',
  '$link', '$unlink',
  '$bit',
  '$embed', // ParqueDB AI embedding operator
])

/**
 * Validate update operators
 * @throws Error if update contains invalid operators
 */
export function validateUpdateOperators(update: UpdateInput): void {
  for (const key of Object.keys(update)) {
    if (key.startsWith('$') && !VALID_UPDATE_OPERATORS.has(key)) {
      throw new Error(`Invalid update operator: ${key}`)
    }
  }

  // Check for conflicting operators on the same field
  const modifiedFields = new Set<string>()
  const operators = [
    '$set', '$unset', '$inc', '$mul', '$min', '$max',
    '$push', '$pull', '$pullAll', '$addToSet', '$pop',
    '$rename', '$currentDate', '$bit',
  ] as const

  for (const op of operators) {
    const opValue = update[op as keyof UpdateInput]
    if (opValue && typeof opValue === 'object') {
      for (const field of Object.keys(opValue)) {
        if (op === '$rename') {
          const newField = (opValue as Record<string, string>)[field]!
          if (modifiedFields.has(field)) {
            throw new Error(`Conflicting operators: field '${field}' modified by multiple operators`)
          }
          if (modifiedFields.has(newField)) {
            throw new Error(`Conflicting operators: field '${newField}' modified by multiple operators`)
          }
          modifiedFields.add(field)
          modifiedFields.add(newField)
        } else {
          if (modifiedFields.has(field)) {
            throw new Error(`Conflicting operators: field '${field}' modified by multiple operators`)
          }
          modifiedFields.add(field)
        }
      }
    }
  }
}
