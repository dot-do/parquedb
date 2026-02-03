/**
 * Schema definition types for ParqueDB
 * Compatible with graphdl and icetype
 */

import type { Visibility } from './visibility'

// =============================================================================
// Field Types
// =============================================================================

/** Primitive field types */
export type PrimitiveType =
  | 'string'
  | 'text'           // Long text (no shredding)
  | 'markdown'       // Markdown content
  | 'number'
  | 'int'
  | 'float'
  | 'double'
  | 'boolean'
  | 'date'
  | 'datetime'
  | 'timestamp'
  | 'uuid'
  | 'email'
  | 'url'
  | 'json'           // Arbitrary JSON
  | 'binary'

/** Parametric types */
export type ParametricType =
  | `decimal(${number},${number})`   // decimal(10,2)
  | `varchar(${number})`              // varchar(255)
  | `char(${number})`                 // char(36)
  | `vector(${number})`               // vector(1536)
  | `enum(${string})`                 // enum(draft,published,archived)

/** Index modifier shortcuts */
export type IndexModifier =
  | '#'      // Indexed (boolean true - implies shredded)
  | '##'     // Unique index
  | '#fts'   // Full-text search index
  | '#vec'   // Vector similarity index
  | '#hash'  // Hash index (O(1) lookups)

/** Type with modifiers */
export type TypeWithModifiers =
  | `${PrimitiveType | ParametricType}!`           // Required
  | `${PrimitiveType | ParametricType}?`           // Optional (explicit)
  | `${PrimitiveType | ParametricType}[]`          // Array
  | `${PrimitiveType | ParametricType}[]!`         // Required array
  | `${PrimitiveType | ParametricType} = ${string}` // With default
  // Indexed types (# suffix implies shredded for predicate pushdown)
  | `${PrimitiveType | ParametricType}#`           // Indexed
  | `${PrimitiveType | ParametricType}#!`          // Indexed + required
  | `${PrimitiveType | ParametricType}##`          // Unique index
  | `${PrimitiveType | ParametricType}##!`         // Unique + required
  | `${PrimitiveType | ParametricType}#fts`        // FTS index
  | `${PrimitiveType | ParametricType}#vec`        // Vector index
  | `${PrimitiveType | ParametricType}#hash`       // Hash index

/** All field type strings */
export type FieldTypeString = PrimitiveType | ParametricType | TypeWithModifiers

// =============================================================================
// Relationship Definitions
// =============================================================================

/** Forward relationship: -> Target.reverse */
export type ForwardRelation = `-> ${string}.${string}` | `-> ${string}.${string}[]`

/** Backward relationship (documentation): <- Source.predicate */
export type BackwardRelation = `<- ${string}.${string}` | `<- ${string}.${string}[]`

/** Fuzzy forward relationship: ~> Target */
export type FuzzyForwardRelation = `~> ${string}` | `~> ${string}.${string}`

/** Fuzzy backward relationship: <~ Source */
export type FuzzyBackwardRelation = `<~ ${string}` | `<~ ${string}.${string}`

/** All relationship strings */
export type RelationString = ForwardRelation | BackwardRelation | FuzzyForwardRelation | FuzzyBackwardRelation

// =============================================================================
// Field Definition
// =============================================================================

/** Index type for a field */
export type IndexType = boolean | 'unique' | 'fts' | 'vector' | 'hash'

/** Full field definition object */
export interface FieldDefinition {
  /** Field type */
  type: FieldTypeString | string  // Allow string for dynamic types

  /** Whether field is required */
  required?: boolean

  /** Default value */
  default?: unknown

  /** Index type */
  index?: IndexType

  /** Field description */
  description?: string

  /** Validation pattern (for strings) */
  pattern?: string

  /** Minimum value (for numbers) */
  min?: number

  /** Maximum value (for numbers) */
  max?: number

  /** Minimum length (for strings/arrays) */
  minLength?: number

  /** Maximum length (for strings/arrays) */
  maxLength?: number

  /** Allowed values (for enums) */
  enum?: unknown[]

  /** Vector dimensions (for vector type) */
  dimensions?: number

  /** Vector distance metric (for vector type) */
  metric?: 'cosine' | 'euclidean' | 'dotProduct'

  /** FTS options (for full-text search indexed fields) */
  ftsOptions?: {
    language?: string
    weight?: number
  }

  /** Similarity threshold for fuzzy relationships (0-1) */
  threshold?: number

  /** Custom metadata */
  meta?: Record<string, unknown>
}

/** Field definition - string shorthand or full object */
export type FieldDef = FieldTypeString | RelationString | FieldDefinition

// =============================================================================
// Type Definition
// =============================================================================

/**
 * Type (entity) definition in schema
 *
 * @example
 * const Post: TypeDefinition = {
 *   $type: 'schema:BlogPosting',
 *   $ns: 'https://example.com/posts',
 *   $shred: ['status', 'publishedAt'],
 *
 *   name: 'string!',
 *   title: 'string!',
 *   content: 'markdown!',
 *   status: { type: 'string', default: 'draft', index: true },
 *   publishedAt: 'datetime?',
 *
 *   author: '-> User.posts',
 *   categories: '-> Category.posts[]',
 *   comments: '<- Comment.post[]',
 * }
 */
export interface TypeDefinition {
  /** JSON-LD type URI */
  $type?: string

  /** Default namespace for this type */
  $ns?: string

  /** Fields to shred from Variant (for columnar efficiency) */
  $shred?: string[]

  /** Type description */
  $description?: string

  /** Abstract type (cannot be instantiated directly) */
  $abstract?: boolean

  /** Extends another type */
  $extends?: string

  /** Index definitions */
  $indexes?: IndexDefinition[]

  /**
   * Visibility level for this collection
   * - 'public': Discoverable and accessible by anyone
   * - 'unlisted': Accessible with direct link, not discoverable
   * - 'private': Requires authentication (default)
   *
   * Inherits from database-level $visibility if not specified
   */
  $visibility?: Visibility

  /** Field definitions */
  [fieldName: string]: FieldDef | string | string[] | boolean | IndexDefinition[] | Visibility | undefined
}

// =============================================================================
// Index Definition
// =============================================================================

/** Compound index definition */
export interface IndexDefinition {
  /** Index name */
  name?: string

  /** Fields in index (with optional direction) */
  fields: (string | { field: string; direction?: 1 | -1 })[]

  /** Unique constraint */
  unique?: boolean

  /** Sparse index (only index documents with field) */
  sparse?: boolean

  /** Partial filter expression */
  partialFilterExpression?: Record<string, unknown>

  /** TTL in seconds (for expiring documents) */
  expireAfterSeconds?: number
}

// =============================================================================
// Schema
// =============================================================================

/**
 * Complete schema definition
 *
 * @example
 * const schema: Schema = {
 *   Post: {
 *     $type: 'schema:BlogPosting',
 *     title: 'string!',
 *     content: 'markdown!',
 *     author: '-> User.posts',
 *   },
 *   User: {
 *     $type: 'schema:Person',
 *     name: 'string!',
 *     email: { type: 'email!', index: 'unique' },
 *   },
 *   Comment: {
 *     text: 'string!',
 *     post: '-> Post.comments',
 *     author: '-> User.comments',
 *   }
 * }
 */
export interface Schema {
  [typeName: string]: TypeDefinition
}

// =============================================================================
// Parsed Schema Types
// =============================================================================

/** Parsed field information */
export interface ParsedField {
  name: string
  type: string
  required: boolean
  isArray: boolean
  default?: unknown
  index?: IndexType

  // Relationship info
  isRelation: boolean
  relationDirection?: 'forward' | 'backward'
  relationMode?: 'exact' | 'fuzzy'
  targetType?: string
  reverseName?: string
}

/** Parsed type information */
export interface ParsedType {
  name: string
  typeUri?: string
  namespace?: string
  shredFields: string[]
  fields: Map<string, ParsedField>
  indexes: IndexDefinition[]
  isAbstract: boolean
  extends?: string
}

/** Parsed schema */
export interface ParsedSchema {
  types: Map<string, ParsedType>

  /** Get type by name */
  getType(name: string): ParsedType | undefined

  /** Get all relationship definitions */
  getRelationships(): ParsedRelationship[]

  /** Validate an entity against its type */
  validate(typeName: string, data: unknown): ValidationResult
}

/** Parsed relationship */
export interface ParsedRelationship {
  fromType: string
  fromField: string
  predicate: string
  toType: string
  reverse: string
  isArray: boolean
  direction: 'forward' | 'backward'
  mode: 'exact' | 'fuzzy'
}

/** Validation result */
export interface ValidationResult {
  valid: boolean
  errors: ValidationError[]
}

/** Validation error */
export interface ValidationError {
  path: string
  message: string
  code: string
}

// =============================================================================
// Schema Parsing Helpers
// =============================================================================

/** Parse a relationship string */
export function parseRelation(value: string): ParsedRelationship | null {
  // Forward: -> User.posts or -> User.posts[]
  const forwardMatch = value.match(/^->\s*(\w+)\.(\w+)(\[\])?$/)
  if (forwardMatch) {
    return {
      fromType: '', // Set by caller
      fromField: '', // Set by caller
      predicate: '', // Set by caller
      toType: forwardMatch[1] ?? '',
      reverse: forwardMatch[2] ?? '',
      isArray: !!forwardMatch[3],
      direction: 'forward',
      mode: 'exact',
    }
  }

  // Backward: <- Comment.post[]
  const backwardMatch = value.match(/^<-\s*(\w+)\.(\w+)(\[\])?$/)
  if (backwardMatch) {
    return {
      fromType: backwardMatch[1] ?? '',
      fromField: backwardMatch[2] ?? '',
      predicate: backwardMatch[2] ?? '',
      toType: '', // Set by caller
      reverse: '', // Set by caller
      isArray: !!backwardMatch[3],
      direction: 'backward',
      mode: 'exact',
    }
  }

  // Fuzzy forward: ~> Topic or ~> Topic.interests
  const fuzzyForwardMatch = value.match(/^~>\s*(\w+)(?:\.(\w+))?(\[\])?$/)
  if (fuzzyForwardMatch) {
    return {
      fromType: '',
      fromField: '',
      predicate: '',
      toType: fuzzyForwardMatch[1] ?? '',
      reverse: fuzzyForwardMatch[2] ?? '',
      isArray: !!fuzzyForwardMatch[3],
      direction: 'forward',
      mode: 'fuzzy',
    }
  }

  // Fuzzy backward: <~ Source
  const fuzzyBackwardMatch = value.match(/^<~\s*(\w+)(?:\.(\w+))?(\[\])?$/)
  if (fuzzyBackwardMatch) {
    return {
      fromType: fuzzyBackwardMatch[1] ?? '',
      fromField: fuzzyBackwardMatch[2] ?? '',
      predicate: '',
      toType: '',
      reverse: '',
      isArray: !!fuzzyBackwardMatch[3],
      direction: 'backward',
      mode: 'fuzzy',
    }
  }

  return null
}

/** Parse a field type string */
export function parseFieldType(value: string): {
  type: string
  required: boolean
  isArray: boolean
  index?: IndexType
  default?: string
} {
  let type = value.trim()
  let required = false
  let isArray = false
  let index: IndexType | undefined
  let defaultValue: string | undefined

  // Check for default: "string = 'default'"
  const defaultMatch = type.match(/^(.+?)\s*=\s*(.+)$/)
  if (defaultMatch) {
    type = (defaultMatch[1] ?? '').trim()
    defaultValue = (defaultMatch[2] ?? '').trim()
  }

  // Check for array: "string[]"
  if (type.endsWith('[]!')) {
    type = type.slice(0, -3)
    isArray = true
    required = true
  } else if (type.endsWith('[]')) {
    type = type.slice(0, -2)
    isArray = true
  }

  // Check for index modifiers (must be before required/optional check)
  // Order matters: check longer patterns first
  if (type.endsWith('#fts!') || type.endsWith('#fts')) {
    index = 'fts'
    type = type.replace(/#fts!?$/, '')
    if (value.endsWith('!')) required = true
  } else if (type.endsWith('#vec!') || type.endsWith('#vec')) {
    index = 'vector'
    type = type.replace(/#vec!?$/, '')
    if (value.endsWith('!')) required = true
  } else if (type.endsWith('#hash!') || type.endsWith('#hash')) {
    index = 'hash'
    type = type.replace(/#hash!?$/, '')
    if (value.endsWith('!')) required = true
  } else if (type.endsWith('##!') || type.endsWith('##')) {
    index = 'unique'
    type = type.replace(/##!?$/, '')
    if (value.endsWith('!')) required = true
  } else if (type.endsWith('#!') || type.endsWith('#')) {
    index = true
    type = type.replace(/#!?$/, '')
    if (value.endsWith('!')) required = true
  }

  // Check for required: "string!"
  if (type.endsWith('!')) {
    type = type.slice(0, -1)
    required = true
  }

  // Check for optional: "string?"
  if (type.endsWith('?')) {
    type = type.slice(0, -1)
    required = false
  }

  return { type, required, isArray, index, default: defaultValue }
}

/** Check if a string is a relationship definition */
export function isRelationString(value: string): boolean {
  return /^(->|<-|~>|<~)\s*\w+/.test(value)
}
