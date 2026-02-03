/**
 * Commutative Operations for Event-Based Merge
 *
 * This module defines which MongoDB-style update operations are commutative
 * (can be applied in any order and combined) vs non-commutative (require
 * conflict resolution when concurrent).
 *
 * Commutative operations (auto-merge):
 * - $inc: Additions combine (1 + 5 = 6)
 * - $addToSet: Set union (no duplicates)
 * - $set on DIFFERENT fields: No overlap
 * - $unset on DIFFERENT fields: No overlap
 *
 * Non-commutative operations (conflict):
 * - $set on SAME field with different values
 * - $push: Array order matters
 * - DELETE vs UPDATE: Cannot combine
 */

import { deepEqual } from '../utils'

// =============================================================================
// Types
// =============================================================================

/**
 * Update operation extracted from an event
 */
export interface UpdateOps {
  readonly $set?: Readonly<Record<string, unknown>>
  readonly $unset?: Readonly<Record<string, unknown>>
  readonly $inc?: Readonly<Record<string, number>>
  readonly $addToSet?: Readonly<Record<string, unknown>>
  readonly $push?: Readonly<Record<string, unknown>>
  readonly $pull?: Readonly<Record<string, unknown>>
  readonly $min?: Readonly<Record<string, unknown>>
  readonly $max?: Readonly<Record<string, unknown>>
  readonly [key: string]: unknown
}

/**
 * A pair of operations to check for commutativity
 */
export interface OperationPair {
  readonly op1: UpdateOps
  readonly op2: UpdateOps
}

// =============================================================================
// Commutativity Detection
// =============================================================================

/**
 * Check if two update operations are commutative (can be combined without conflict)
 *
 * @param op1 First update operation
 * @param op2 Second update operation
 * @returns true if operations can be combined automatically
 */
export function isCommutative(op1: UpdateOps, op2: UpdateOps): boolean {
  // Get all operator types used in both operations
  const ops1 = getOperatorTypes(op1)
  const ops2 = getOperatorTypes(op2)

  // Check each combination of operators
  for (const type1 of ops1) {
    for (const type2 of ops2) {
      if (!areOperatorsCommutative(type1, op1[type1], type2, op2[type2])) {
        return false
      }
    }
  }

  return true
}

/**
 * Get all operator types (keys starting with $) from an operation
 */
function getOperatorTypes(ops: UpdateOps): string[] {
  return Object.keys(ops).filter(k => k.startsWith('$'))
}

/**
 * Check if two specific operators are commutative with each other
 */
function areOperatorsCommutative(
  type1: string,
  value1: unknown,
  type2: string,
  value2: unknown
): boolean {
  // $inc is always commutative with itself
  if (type1 === '$inc' && type2 === '$inc') {
    return true
  }

  // $addToSet is always commutative with itself
  if (type1 === '$addToSet' && type2 === '$addToSet') {
    return true
  }

  // $push is NEVER commutative (order matters)
  if (type1 === '$push' || type2 === '$push') {
    return false
  }

  // $set is commutative with $set only if they affect different fields
  if (type1 === '$set' && type2 === '$set') {
    return !hasOverlappingFields(value1 as Record<string, unknown>, value2 as Record<string, unknown>)
  }

  // $unset is commutative with $unset only if they affect different fields
  if (type1 === '$unset' && type2 === '$unset') {
    return !hasOverlappingFields(value1 as Record<string, unknown>, value2 as Record<string, unknown>)
  }

  // $set and $unset are commutative if they affect different fields
  if ((type1 === '$set' && type2 === '$unset') || (type1 === '$unset' && type2 === '$set')) {
    return !hasOverlappingFields(value1 as Record<string, unknown>, value2 as Record<string, unknown>)
  }

  // $inc is commutative with $set if they affect different fields
  if ((type1 === '$inc' && type2 === '$set') || (type1 === '$set' && type2 === '$inc')) {
    return !hasOverlappingFields(value1 as Record<string, unknown>, value2 as Record<string, unknown>)
  }

  // $addToSet is commutative with $set if they affect different fields
  if ((type1 === '$addToSet' && type2 === '$set') || (type1 === '$set' && type2 === '$addToSet')) {
    return !hasOverlappingFields(value1 as Record<string, unknown>, value2 as Record<string, unknown>)
  }

  // $min and $max are commutative with themselves and each other
  if ((type1 === '$min' || type1 === '$max') && (type2 === '$min' || type2 === '$max')) {
    return !hasOverlappingFields(value1 as Record<string, unknown>, value2 as Record<string, unknown>)
  }

  // Default: different operator types affecting different fields are commutative
  return !hasOverlappingFields(value1 as Record<string, unknown>, value2 as Record<string, unknown>)
}

/**
 * Check if two objects have any overlapping keys
 */
function hasOverlappingFields(obj1: Record<string, unknown>, obj2: Record<string, unknown>): boolean {
  const keys1 = new Set(Object.keys(obj1 || {}))
  const keys2 = Object.keys(obj2 || {})

  for (const key of keys2) {
    if (keys1.has(key)) {
      return true
    }
  }

  return false
}

// =============================================================================
// Operation Combination
// =============================================================================

/**
 * Combine two commutative operations into a single operation
 *
 * @param op1 First update operation
 * @param op2 Second update operation
 * @returns Combined operation
 * @throws Error if operations are not commutative
 */
export function combineOperations(op1: UpdateOps, op2: UpdateOps): UpdateOps {
  const result: UpdateOps = {}

  // Combine $inc by summing values
  if (op1.$inc || op2.$inc) {
    result.$inc = combineInc(op1.$inc, op2.$inc)
  }

  // Combine $addToSet by set union
  if (op1.$addToSet || op2.$addToSet) {
    result.$addToSet = combineAddToSet(op1.$addToSet, op2.$addToSet)
  }

  // Combine $set by merging (must be on different fields)
  if (op1.$set || op2.$set) {
    result.$set = combineSet(op1.$set, op2.$set)
  }

  // Combine $unset by merging (must be on different fields)
  if (op1.$unset || op2.$unset) {
    result.$unset = combineUnset(op1.$unset, op2.$unset)
  }

  // Combine $min by taking minimum
  if (op1.$min || op2.$min) {
    result.$min = combineMin(op1.$min, op2.$min)
  }

  // Combine $max by taking maximum
  if (op1.$max || op2.$max) {
    result.$max = combineMax(op1.$max, op2.$max)
  }

  return result
}

/**
 * Combine $inc operations by summing values
 */
function combineInc(
  inc1?: Record<string, number>,
  inc2?: Record<string, number>
): Record<string, number> {
  const result: Record<string, number> = { ...(inc1 || {}) }

  for (const [key, value] of Object.entries(inc2 || {})) {
    result[key] = (result[key] || 0) + value
  }

  return result
}

/**
 * Combine $addToSet operations using set union
 */
function combineAddToSet(
  set1?: Record<string, unknown>,
  set2?: Record<string, unknown>
): Record<string, unknown> {
  const result: Record<string, unknown> = {}
  const allKeys = new Set([...Object.keys(set1 || {}), ...Object.keys(set2 || {})])

  for (const key of allKeys) {
    const values1 = extractAddToSetValues(set1?.[key])
    const values2 = extractAddToSetValues(set2?.[key])

    // Union the values (deduplication)
    const unionValues: unknown[] = [...values1]
    for (const v of values2) {
      if (!unionValues.some(existing => deepEqual(existing, v))) {
        unionValues.push(v)
      }
    }

    result[key] = { $each: unionValues }
  }

  return result
}

/**
 * Extract values from an $addToSet operation
 */
function extractAddToSetValues(value: unknown): unknown[] {
  if (value === undefined || value === null) {
    return []
  }

  if (typeof value === 'object' && value !== null && '$each' in value) {
    return (value as { $each: unknown[] }).$each
  }

  return [value]
}

/**
 * Combine $set operations (must be on different fields)
 */
function combineSet(
  set1?: Record<string, unknown>,
  set2?: Record<string, unknown>
): Record<string, unknown> {
  return { ...(set1 || {}), ...(set2 || {}) }
}

/**
 * Combine $unset operations (must be on different fields)
 */
function combineUnset(
  unset1?: Record<string, unknown>,
  unset2?: Record<string, unknown>
): Record<string, unknown> {
  return { ...(unset1 || {}), ...(unset2 || {}) }
}

/**
 * Combine $min operations
 */
function combineMin(
  min1?: Record<string, unknown>,
  min2?: Record<string, unknown>
): Record<string, unknown> {
  const result: Record<string, unknown> = { ...(min1 || {}) }

  for (const [key, value] of Object.entries(min2 || {})) {
    if (!(key in result) || compareValues(value, result[key]) < 0) {
      result[key] = value
    }
  }

  return result
}

/**
 * Combine $max operations
 */
function combineMax(
  max1?: Record<string, unknown>,
  max2?: Record<string, unknown>
): Record<string, unknown> {
  const result: Record<string, unknown> = { ...(max1 || {}) }

  for (const [key, value] of Object.entries(max2 || {})) {
    if (!(key in result) || compareValues(value, result[key]) > 0) {
      result[key] = value
    }
  }

  return result
}

/**
 * Compare two values for ordering
 */
function compareValues(a: unknown, b: unknown): number {
  if (a === b) return 0
  if (a === null || a === undefined) return -1
  if (b === null || b === undefined) return 1

  if (typeof a === 'number' && typeof b === 'number') {
    return a - b
  }

  if (typeof a === 'string' && typeof b === 'string') {
    return a.localeCompare(b)
  }

  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime()
  }

  // Fallback to string comparison
  return String(a).localeCompare(String(b))
}

// =============================================================================
// Field Extraction
// =============================================================================

/**
 * Extract all fields affected by an operation
 */
export function getAffectedFields(ops: UpdateOps): Set<string> {
  const fields = new Set<string>()

  for (const [opType, opValue] of Object.entries(ops)) {
    if (opType.startsWith('$') && opValue && typeof opValue === 'object') {
      for (const field of Object.keys(opValue as Record<string, unknown>)) {
        fields.add(field)
      }
    }
  }

  return fields
}

/**
 * Extract operations from an event's after state
 * Events may store operations in an _ops field
 */
export function extractOperations(eventAfter: Record<string, unknown> | undefined): UpdateOps {
  if (!eventAfter) {
    return {}
  }

  // Check for embedded _ops field
  if (eventAfter._ops && typeof eventAfter._ops === 'object') {
    return eventAfter._ops as UpdateOps
  }

  return {}
}

/**
 * Check if an operation is a simple value change (for detecting field-level changes)
 */
export function isSimpleSetOperation(before: unknown, after: unknown): boolean {
  // If no _ops embedded, treat as implicit $set
  return before !== after
}
