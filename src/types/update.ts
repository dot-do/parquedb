/**
 * MongoDB-style update operators for ParqueDB
 */

import type { EntityId, Filter } from './index'

// =============================================================================
// Field Update Operators
// =============================================================================

/** Set field values */
export interface SetOperator<T = Record<string, unknown>> {
  $set: Partial<T>
}

/** Remove fields */
export interface UnsetOperator<T = Record<string, unknown>> {
  $unset: { [K in keyof T]?: '' | 1 | true }
}

/** Rename fields */
export interface RenameOperator {
  $rename: Record<string, string>
}

/** Set field only if it doesn't exist */
export interface SetOnInsertOperator<T = Record<string, unknown>> {
  $setOnInsert: Partial<T>
}

// =============================================================================
// Numeric Operators
// =============================================================================

/** Increment numeric fields */
export interface IncOperator<T = Record<string, unknown>> {
  $inc: { [K in keyof T]?: number }
}

/** Multiply numeric fields */
export interface MulOperator<T = Record<string, unknown>> {
  $mul: { [K in keyof T]?: number }
}

/** Set field to minimum of current and specified value */
export interface MinOperator<T = Record<string, unknown>> {
  $min: Partial<T>
}

/** Set field to maximum of current and specified value */
export interface MaxOperator<T = Record<string, unknown>> {
  $max: Partial<T>
}

// =============================================================================
// Array Operators
// =============================================================================

/** Push value(s) to array */
export interface PushOperator<T = Record<string, unknown>> {
  $push: {
    [K in keyof T]?: T[K] extends (infer E)[]
      ? E | PushModifiers<E>
      : never
  }
}

/** Push modifiers for advanced array operations */
export interface PushModifiers<T> {
  /** Values to push */
  $each: T[]
  /** Position to insert at */
  $position?: number
  /** Limit array size after push */
  $slice?: number
  /** Sort after push */
  $sort?: 1 | -1 | Record<string, 1 | -1>
}

/** Pull value(s) from array */
export interface PullOperator<T = Record<string, unknown>> {
  $pull: {
    [K in keyof T]?: T[K] extends (infer E)[]
      ? E | Filter
      : never
  }
}

/** Pull all matching values from array */
export interface PullAllOperator<T = Record<string, unknown>> {
  $pullAll: {
    [K in keyof T]?: T[K] extends (infer E)[]
      ? E[]
      : never
  }
}

/** Add value to array only if not present */
export interface AddToSetOperator<T = Record<string, unknown>> {
  $addToSet: {
    [K in keyof T]?: T[K] extends (infer E)[]
      ? E | { $each: E[] }
      : never
  }
}

/** Remove first or last element from array */
export interface PopOperator<T = Record<string, unknown>> {
  $pop: {
    [K in keyof T]?: -1 | 1
  }
}

// =============================================================================
// Date Operators
// =============================================================================

/** Set field to current date */
export interface CurrentDateOperator<T = Record<string, unknown>> {
  $currentDate: {
    [K in keyof T]?: true | { $type: 'date' | 'timestamp' }
  }
}

// =============================================================================
// Relationship Operators (ParqueDB-specific)
// =============================================================================

/** Add relationship(s) */
export interface LinkOperator {
  $link: {
    /** Predicate name -> target EntityId(s) */
    [predicate: string]: EntityId | EntityId[]
  }
}

/** Remove relationship(s) */
export interface UnlinkOperator {
  $unlink: {
    /** Predicate name -> target EntityId(s) to remove */
    [predicate: string]: EntityId | EntityId[]
  }
}

// =============================================================================
// Bit Operators (Advanced)
// =============================================================================

/** Bitwise operations */
export interface BitOperator<T = Record<string, unknown>> {
  $bit: {
    [K in keyof T]?: {
      and?: number
      or?: number
      xor?: number
    }
  }
}

// =============================================================================
// Combined Update Input
// =============================================================================

/**
 * MongoDB-style update input
 *
 * @example
 * // Set fields
 * { $set: { status: 'published', publishedAt: new Date() } }
 *
 * @example
 * // Increment counter
 * { $inc: { viewCount: 1 } }
 *
 * @example
 * // Push to array
 * { $push: { tags: 'featured' } }
 *
 * @example
 * // Push multiple with options
 * { $push: { tags: { $each: ['a', 'b'], $position: 0 } } }
 *
 * @example
 * // Add relationship
 * { $link: { categories: 'categories/tech' } }
 *
 * @example
 * // Combined operations
 * {
 *   $set: { status: 'published' },
 *   $inc: { version: 1 },
 *   $currentDate: { updatedAt: true },
 *   $link: { categories: ['categories/tech', 'categories/db'] }
 * }
 */
export interface UpdateInput<T = Record<string, unknown>> {
  // Field updates
  $set?: Partial<T>
  $unset?: { [K in keyof T]?: '' | 1 | true }
  $rename?: Record<string, string>
  $setOnInsert?: Partial<T>

  // Numeric
  $inc?: { [K in keyof T]?: number }
  $mul?: { [K in keyof T]?: number }
  $min?: Partial<T>
  $max?: Partial<T>

  // Array
  $push?: Record<string, unknown>
  $pull?: Record<string, unknown>
  $pullAll?: Record<string, unknown[]>
  $addToSet?: Record<string, unknown>
  $pop?: Record<string, -1 | 1>

  // Date
  $currentDate?: Record<string, true | { $type: 'date' | 'timestamp' }>

  // Relationships (ParqueDB-specific)
  $link?: Record<string, EntityId | EntityId[]>
  $unlink?: Record<string, EntityId | EntityId[] | '$all'>

  // Bit
  $bit?: Record<string, { and?: number; or?: number; xor?: number }>
}

// =============================================================================
// Type Guards
// =============================================================================

/** Check if update has field update operators */
export function hasFieldOperators(update: UpdateInput): boolean {
  return '$set' in update || '$unset' in update || '$rename' in update
}

/** Check if update has numeric operators */
export function hasNumericOperators(update: UpdateInput): boolean {
  return '$inc' in update || '$mul' in update || '$min' in update || '$max' in update
}

/** Check if update has array operators */
export function hasArrayOperators(update: UpdateInput): boolean {
  return '$push' in update || '$pull' in update || '$pullAll' in update ||
         '$addToSet' in update || '$pop' in update
}

/** Check if update has relationship operators */
export function hasRelationshipOperators(update: UpdateInput): boolean {
  return '$link' in update || '$unlink' in update
}

/** Get all operator types in an update */
export function getUpdateOperatorTypes(update: UpdateInput): string[] {
  return Object.keys(update).filter(k => k.startsWith('$'))
}

/**
 * Update type alias (preferred for shorter import)
 */
export type Update<T = Record<string, unknown>> = UpdateInput<T>
