/**
 * MongoDB-style update operators for ParqueDB
 *
 * This module provides update operators for modifying entity data without
 * replacing the entire document. Operators can be combined in a single update.
 *
 * @module types/update
 *
 * @example
 * ```typescript
 * // Combined update with multiple operators
 * await db.Posts.update('post-1', {
 *   $set: { status: 'published' },
 *   $inc: { views: 1 },
 *   $push: { tags: 'featured' },
 *   $currentDate: { publishedAt: true }
 * })
 * ```
 */

import type { EntityId, EntityData } from './entity'
import type { Filter } from './filter'

// =============================================================================
// Field Update Operators
// =============================================================================

/**
 * Set field values operator
 *
 * Sets the value of one or more fields. If a field doesn't exist, it's created.
 *
 * @typeParam T - Shape of the entity being updated (constrained to EntityData)
 *
 * @example
 * ```typescript
 * // Set multiple fields
 * { $set: { status: 'published', title: 'Updated Title' } }
 *
 * // Set nested field using dot notation
 * { $set: { 'metadata.featured': true } }
 * ```
 */
export interface SetOperator<T extends EntityData = EntityData> {
  $set: Partial<T>
}

/**
 * Remove fields operator
 *
 * Removes the specified fields from the document.
 * The value (true, 1, or '') is ignored - only the keys matter.
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * // Remove the 'draft' and 'tempData' fields
 * { $unset: { draft: true, tempData: '' } }
 * ```
 */
export interface UnsetOperator<T extends EntityData = EntityData> {
  $unset: { [K in keyof T]?: '' | 1 | true }
}

/**
 * Rename fields operator
 *
 * Renames fields from old name to new name.
 *
 * @example
 * ```typescript
 * // Rename 'oldName' to 'newName'
 * { $rename: { oldName: 'newName' } }
 * ```
 */
export interface RenameOperator {
  $rename: Record<string, string>
}

/**
 * Set on insert operator (upsert only)
 *
 * Sets field values only when creating a new document (during upsert).
 * Has no effect if the document already exists.
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * // Set initial views only when creating
 * {
 *   $set: { title: 'New Post' },
 *   $setOnInsert: { views: 0, createdVia: 'api' }
 * }
 * ```
 */
export interface SetOnInsertOperator<T extends EntityData = EntityData> {
  $setOnInsert: Partial<T>
}

// =============================================================================
// Numeric Operators
// =============================================================================

/**
 * Increment numeric fields operator
 *
 * Adds the specified value to numeric fields. Use negative values to decrement.
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * // Increment views by 1
 * { $inc: { views: 1 } }
 *
 * // Decrement stock by 5
 * { $inc: { stock: -5 } }
 *
 * // Increment multiple fields
 * { $inc: { views: 1, likes: 1, shares: 1 } }
 * ```
 */
export interface IncOperator<T extends EntityData = EntityData> {
  $inc: { [K in keyof T]?: number }
}

/**
 * Multiply numeric fields operator
 *
 * Multiplies the field value by the specified number.
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * // Double the price
 * { $mul: { price: 2 } }
 *
 * // Apply 10% discount
 * { $mul: { price: 0.9 } }
 * ```
 */
export interface MulOperator<T extends EntityData = EntityData> {
  $mul: { [K in keyof T]?: number }
}

/**
 * Set to minimum operator
 *
 * Updates the field only if the specified value is less than the current value.
 * Useful for tracking minimum values without race conditions.
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * // Track lowest price seen
 * { $min: { lowestPrice: 9.99 } }
 *
 * // Track earliest date
 * { $min: { firstSeen: new Date() } }
 * ```
 */
export interface MinOperator<T extends EntityData = EntityData> {
  $min: Partial<T>
}

/**
 * Set to maximum operator
 *
 * Updates the field only if the specified value is greater than the current value.
 * Useful for tracking maximum values without race conditions.
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * // Track highest score
 * { $max: { highScore: 9500 } }
 *
 * // Track latest activity
 * { $max: { lastActive: new Date() } }
 * ```
 */
export interface MaxOperator<T extends EntityData = EntityData> {
  $max: Partial<T>
}

// =============================================================================
// Array Operators
// =============================================================================

/**
 * Push to array operator
 *
 * Appends values to an array field. Supports modifiers for advanced operations.
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * // Push single value
 * { $push: { tags: 'featured' } }
 *
 * // Push multiple values with modifiers
 * { $push: { tags: { $each: ['a', 'b'], $position: 0 } } }
 *
 * // Push and limit array size
 * { $push: { recentViews: { $each: [userId], $slice: -100 } } }
 * ```
 */
export interface PushOperator<T extends EntityData = EntityData> {
  $push: {
    [K in keyof T]?: T[K] extends (infer E)[]
      ? E | PushModifiers<E>
      : never
  }
}

/**
 * Push modifiers for advanced array operations
 *
 * Used with $push to control insertion position, array size, and sorting.
 *
 * @typeParam T - Type of array elements
 */
export interface PushModifiers<T> {
  /**
   * Values to push (required for using other modifiers)
   */
  $each: T[]

  /**
   * Position to insert at (0 = beginning, negative from end)
   */
  $position?: number

  /**
   * Limit array size after push
   * Positive: keep first N elements
   * Negative: keep last N elements
   */
  $slice?: number

  /**
   * Sort array after push
   * 1 for ascending, -1 for descending
   * Or object for sorting by nested field
   */
  $sort?: 1 | -1 | Record<string, 1 | -1>
}

/**
 * Pull from array operator
 *
 * Removes all array elements that match the specified value or condition.
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * // Remove specific value
 * { $pull: { tags: 'deprecated' } }
 *
 * // Remove by condition (for arrays of objects)
 * { $pull: { comments: { score: { $lt: 0 } } } }
 * ```
 */
export interface PullOperator<T extends EntityData = EntityData> {
  $pull: {
    [K in keyof T]?: T[K] extends (infer E)[]
      ? E | Filter
      : never
  }
}

/**
 * Pull all from array operator
 *
 * Removes all matching values from an array.
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * // Remove multiple specific values
 * { $pullAll: { tags: ['old', 'deprecated', 'legacy'] } }
 * ```
 */
export interface PullAllOperator<T extends EntityData = EntityData> {
  $pullAll: {
    [K in keyof T]?: T[K] extends (infer E)[]
      ? E[]
      : never
  }
}

/**
 * Add to set operator
 *
 * Adds values to an array only if they don't already exist (set semantics).
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * // Add single value if not present
 * { $addToSet: { tags: 'featured' } }
 *
 * // Add multiple values
 * { $addToSet: { tags: { $each: ['a', 'b', 'c'] } } }
 * ```
 */
export interface AddToSetOperator<T extends EntityData = EntityData> {
  $addToSet: {
    [K in keyof T]?: T[K] extends (infer E)[]
      ? E | { $each: E[] }
      : never
  }
}

/**
 * Pop from array operator
 *
 * Removes the first or last element from an array.
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * // Remove last element
 * { $pop: { items: 1 } }
 *
 * // Remove first element
 * { $pop: { items: -1 } }
 * ```
 */
export interface PopOperator<T extends EntityData = EntityData> {
  $pop: {
    /** -1 to remove first element, 1 to remove last element */
    [K in keyof T]?: -1 | 1
  }
}

// =============================================================================
// Date Operators
// =============================================================================

/**
 * Current date operator
 *
 * Sets a field to the current date/time.
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * // Set to current date
 * { $currentDate: { lastModified: true } }
 *
 * // Explicitly as timestamp
 * { $currentDate: { lastModified: { $type: 'timestamp' } } }
 * ```
 */
export interface CurrentDateOperator<T extends EntityData = EntityData> {
  $currentDate: {
    /**
     * true for Date type, or object with $type for explicit type
     */
    [K in keyof T]?: true | { $type: 'date' | 'timestamp' }
  }
}

// =============================================================================
// Relationship Operators (ParqueDB-specific)
// =============================================================================

/**
 * Link relationship operator (ParqueDB-specific)
 *
 * Creates relationships between entities. Relationships are bidirectional
 * and indexed for efficient traversal in both directions.
 *
 * @example
 * ```typescript
 * // Link to single entity
 * { $link: { author: 'users/nathan-123' } }
 *
 * // Link to multiple entities
 * { $link: { categories: ['categories/tech', 'categories/database'] } }
 *
 * // Combined with other updates
 * {
 *   $set: { status: 'published' },
 *   $link: { author: 'users/nathan-123' }
 * }
 * ```
 */
export interface LinkOperator {
  $link: {
    /**
     * Map of predicate name to target EntityId(s)
     * Predicate is the outbound relationship name (e.g., "author", "categories")
     */
    [predicate: string]: EntityId | EntityId[]
  }
}

/**
 * Unlink relationship operator (ParqueDB-specific)
 *
 * Removes relationships between entities.
 *
 * @example
 * ```typescript
 * // Unlink from specific entity
 * { $unlink: { categories: 'categories/tech' } }
 *
 * // Unlink from multiple entities
 * { $unlink: { categories: ['categories/old', 'categories/deprecated'] } }
 *
 * // Unlink all relationships for a predicate
 * { $unlink: { categories: '$all' } }
 * ```
 */
export interface UnlinkOperator {
  $unlink: {
    /**
     * Map of predicate name to target EntityId(s) to remove
     * Use '$all' to remove all relationships for that predicate
     */
    [predicate: string]: EntityId | EntityId[]
  }
}

// =============================================================================
// Bit Operators (Advanced)
// =============================================================================

/**
 * Bitwise operations operator
 *
 * Performs bitwise AND, OR, or XOR operations on integer fields.
 * Useful for flag fields and bitmask operations.
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * // Set bit 2 (value 4)
 * { $bit: { flags: { or: 4 } } }
 *
 * // Clear bit 2
 * { $bit: { flags: { and: ~4 } } }
 *
 * // Toggle bit 2
 * { $bit: { flags: { xor: 4 } } }
 * ```
 */
export interface BitOperator<T extends EntityData = EntityData> {
  $bit: {
    [K in keyof T]?: {
      /** Bitwise AND with current value */
      and?: number
      /** Bitwise OR with current value */
      or?: number
      /** Bitwise XOR with current value */
      xor?: number
    }
  }
}

// =============================================================================
// Embedding Operators (ParqueDB AI)
// =============================================================================

/**
 * Generate embedding for text field(s)
 *
 * @example
 * ```typescript
 * // Embed a single field
 * { $embed: { description: 'embedding' } }
 *
 * // Embed multiple fields into separate vectors
 * { $embed: { title: 'title_embedding', content: 'content_embedding' } }
 *
 * // Embed with options
 * { $embed: { description: { field: 'embedding', model: '@cf/baai/bge-small-en-v1.5' } } }
 * ```
 */
export interface EmbedOperator {
  $embed: {
    /** Source field -> target field or options */
    [sourceField: string]: string | EmbedFieldOptions
  }
}

/**
 * Options for embedding a specific field
 */
export interface EmbedFieldOptions {
  /** Target field to store the embedding vector */
  field: string
  /** Model to use for embedding (default: @cf/baai/bge-m3) */
  model?: string
  /** Whether to overwrite existing embedding */
  overwrite?: boolean
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
export interface UpdateInput<T extends EntityData = EntityData> {
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

  // Embedding (ParqueDB AI)
  $embed?: Record<string, string | EmbedFieldOptions>
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if an update contains field update operators ($set, $unset, $rename)
 *
 * @param update - Update object to check
 * @returns True if update uses field operators
 */
export function hasFieldOperators(update: UpdateInput): boolean {
  return '$set' in update || '$unset' in update || '$rename' in update
}

/**
 * Check if an update contains numeric operators ($inc, $mul, $min, $max)
 *
 * @param update - Update object to check
 * @returns True if update uses numeric operators
 */
export function hasNumericOperators(update: UpdateInput): boolean {
  return '$inc' in update || '$mul' in update || '$min' in update || '$max' in update
}

/**
 * Check if an update contains array operators ($push, $pull, $pullAll, $addToSet, $pop)
 *
 * @param update - Update object to check
 * @returns True if update uses array operators
 */
export function hasArrayOperators(update: UpdateInput): boolean {
  return '$push' in update || '$pull' in update || '$pullAll' in update ||
         '$addToSet' in update || '$pop' in update
}

/**
 * Check if an update contains relationship operators ($link, $unlink)
 *
 * @param update - Update object to check
 * @returns True if update uses relationship operators
 */
export function hasRelationshipOperators(update: UpdateInput): boolean {
  return '$link' in update || '$unlink' in update
}

/**
 * Check if an update contains embedding operators ($embed)
 *
 * @param update - Update object to check
 * @returns True if update uses embedding operators
 */
export function hasEmbeddingOperators(update: UpdateInput): boolean {
  return '$embed' in update
}

/**
 * Get all operator types used in an update
 *
 * @param update - Update object to analyze
 * @returns Array of operator names (e.g., ['$set', '$inc'])
 */
export function getUpdateOperatorTypes(update: UpdateInput): string[] {
  return Object.keys(update).filter(k => k.startsWith('$'))
}

/**
 * Update type alias (preferred for shorter import)
 *
 * @typeParam T - Shape of the entity being updated
 *
 * @example
 * ```typescript
 * import type { Update } from 'parquedb'
 *
 * const update: Update<Post> = {
 *   $set: { title: 'New Title' },
 *   $inc: { views: 1 }
 * }
 * ```
 */
export type Update<T extends EntityData = EntityData> = UpdateInput<T>
