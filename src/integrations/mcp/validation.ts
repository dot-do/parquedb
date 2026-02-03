/**
 * Input Validation for MCP Server
 *
 * Provides validation and sanitization functions to prevent:
 * - Prototype pollution attacks
 * - Path traversal attacks
 * - Invalid operator injection
 * - Type confusion attacks
 */

import { isNullish } from '../../utils/comparison'
import {
  MAX_MCP_STRING_LENGTH,
  MAX_MCP_PAGINATION_LIMIT,
  MAX_MCP_PROMPT_LENGTH,
} from '../../constants'

/**
 * Validation error with field information
 */
export class ValidationError extends Error {
  constructor(
    public readonly field: string,
    message: string
  ) {
    super(`Invalid ${field}: ${message}`)
    this.name = 'ValidationError'
  }
}

/**
 * List of allowed MongoDB-style filter operators
 */
const ALLOWED_FILTER_OPERATORS = new Set([
  // Comparison
  '$eq', '$ne', '$gt', '$gte', '$lt', '$lte',
  // Array
  '$in', '$nin', '$all', '$elemMatch', '$size',
  // Logical
  '$and', '$or', '$nor', '$not',
  // String
  '$regex', '$startsWith', '$endsWith', '$contains',
  // Existence/Type
  '$exists', '$type',
])

/**
 * List of allowed MongoDB-style update operators
 */
const ALLOWED_UPDATE_OPERATORS = new Set([
  '$set', '$unset', '$inc', '$dec', '$mul',
  '$push', '$pull', '$pop', '$addToSet',
  '$min', '$max', '$rename', '$currentDate',
])

/**
 * List of allowed aggregation stage operators
 */
const ALLOWED_PIPELINE_STAGES = new Set([
  '$match', '$project', '$limit', '$skip', '$sort',
  '$group', '$count', '$unwind',
])

/**
 * Dangerous property names that could lead to prototype pollution
 */
const DANGEROUS_KEYS = new Set([
  '__proto__', 'constructor', 'prototype',
  '__defineGetter__', '__defineSetter__',
  '__lookupGetter__', '__lookupSetter__',
])

/**
 * Pattern for valid collection names (alphanumeric and underscore, starts with letter)
 */
const COLLECTION_NAME_PATTERN = /^[a-zA-Z][a-zA-Z0-9_]*$/

/**
 * Pattern for valid entity IDs (alphanumeric, hyphen, underscore - no path traversal)
 */
const ENTITY_ID_PATTERN = /^[a-zA-Z0-9_-]+$/

/**
 * Maximum depth for nested objects to prevent stack overflow
 */
const MAX_NESTING_DEPTH = 10

/**
 * Maximum string length for fields
 */
const MAX_STRING_LENGTH = MAX_MCP_STRING_LENGTH

/**
 * Validates and sanitizes a collection name
 */
export function validateCollectionName(name: unknown, field = 'collection'): string {
  if (isNullish(name)) {
    throw new ValidationError(field, 'is required')
  }

  if (typeof name !== 'string') {
    throw new ValidationError(field, `must be a string, got ${typeof name}`)
  }

  const trimmed = name.trim()

  if (trimmed.length === 0) {
    throw new ValidationError(field, 'cannot be empty')
  }

  if (trimmed.length > 100) {
    throw new ValidationError(field, 'must be 100 characters or fewer')
  }

  if (!COLLECTION_NAME_PATTERN.test(trimmed)) {
    throw new ValidationError(
      field,
      'must start with a letter and contain only letters, numbers, and underscores'
    )
  }

  return trimmed
}

/**
 * Validates and sanitizes an entity ID
 */
export function validateEntityId(id: unknown, field = 'id'): string {
  if (isNullish(id)) {
    throw new ValidationError(field, 'is required')
  }

  if (typeof id !== 'string') {
    throw new ValidationError(field, `must be a string, got ${typeof id}`)
  }

  const trimmed = id.trim()

  if (trimmed.length === 0) {
    throw new ValidationError(field, 'cannot be empty')
  }

  if (trimmed.length > 256) {
    throw new ValidationError(field, 'must be 256 characters or fewer')
  }

  // Check for path traversal attempts
  if (trimmed.includes('..') || trimmed.includes('/') || trimmed.includes('\\')) {
    throw new ValidationError(field, 'contains invalid characters (path traversal attempt)')
  }

  if (!ENTITY_ID_PATTERN.test(trimmed)) {
    throw new ValidationError(
      field,
      'must contain only letters, numbers, hyphens, and underscores'
    )
  }

  return trimmed
}

/**
 * Checks if a key is safe (not a dangerous prototype property)
 */
function isSafeKey(key: string): boolean {
  return !DANGEROUS_KEYS.has(key)
}

/**
 * Deeply sanitizes an object to prevent prototype pollution
 * Returns a new object with dangerous keys removed
 */
export function sanitizeObject<T extends Record<string, unknown>>(
  obj: unknown,
  field: string,
  depth = 0
): T {
  if (depth > MAX_NESTING_DEPTH) {
    throw new ValidationError(field, `exceeds maximum nesting depth of ${MAX_NESTING_DEPTH}`)
  }

  if (isNullish(obj)) {
    return {} as T
  }

  if (typeof obj !== 'object') {
    throw new ValidationError(field, `must be an object, got ${typeof obj}`)
  }

  if (Array.isArray(obj)) {
    throw new ValidationError(field, 'must be an object, not an array')
  }

  const result: Record<string, unknown> = Object.create(null)

  for (const key of Object.keys(obj)) {
    if (!isSafeKey(key)) {
      // Skip dangerous keys silently to prevent information leakage
      continue
    }

    const value = (obj as Record<string, unknown>)[key]

    if (isNullish(value)) {
      result[key] = value
    } else if (Array.isArray(value)) {
      result[key] = sanitizeArray(value, `${field}.${key}`, depth + 1)
    } else if (typeof value === 'object') {
      result[key] = sanitizeObject(value as Record<string, unknown>, `${field}.${key}`, depth + 1)
    } else if (typeof value === 'string') {
      if (value.length > MAX_STRING_LENGTH) {
        throw new ValidationError(`${field}.${key}`, `string value exceeds maximum length of ${MAX_STRING_LENGTH}`)
      }
      result[key] = value
    } else {
      result[key] = value
    }
  }

  return result as T
}

/**
 * Sanitizes an array, recursively sanitizing objects within
 */
function sanitizeArray(arr: unknown[], field: string, depth: number): unknown[] {
  if (depth > MAX_NESTING_DEPTH) {
    throw new ValidationError(field, `exceeds maximum nesting depth of ${MAX_NESTING_DEPTH}`)
  }

  return arr.map((item, index) => {
    if (isNullish(item)) {
      return item
    }
    if (Array.isArray(item)) {
      return sanitizeArray(item, `${field}[${index}]`, depth + 1)
    }
    if (typeof item === 'object') {
      return sanitizeObject(item as Record<string, unknown>, `${field}[${index}]`, depth + 1)
    }
    if (typeof item === 'string' && item.length > MAX_STRING_LENGTH) {
      throw new ValidationError(`${field}[${index}]`, `string value exceeds maximum length of ${MAX_STRING_LENGTH}`)
    }
    return item
  })
}

/**
 * Validates that all operator keys in an object are allowed filter operators
 */
function validateFilterOperators(obj: Record<string, unknown>, field: string, depth = 0): void {
  if (depth > MAX_NESTING_DEPTH) {
    throw new ValidationError(field, `exceeds maximum nesting depth of ${MAX_NESTING_DEPTH}`)
  }

  for (const key of Object.keys(obj)) {
    // Keys starting with $ must be valid operators
    if (key.startsWith('$')) {
      if (!ALLOWED_FILTER_OPERATORS.has(key)) {
        throw new ValidationError(field, `contains unknown operator: ${key}`)
      }
    }

    const value = obj[key]
    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
      validateFilterOperators(value as Record<string, unknown>, `${field}.${key}`, depth + 1)
    } else if (Array.isArray(value)) {
      // Check array elements for logical operators like $and, $or
      if (key === '$and' || key === '$or' || key === '$nor') {
        for (let i = 0; i < value.length; i++) {
          const item = value[i]
          if (item !== null && typeof item === 'object' && !Array.isArray(item)) {
            validateFilterOperators(item as Record<string, unknown>, `${field}.${key}[${i}]`, depth + 1)
          }
        }
      }
    }
  }
}

/**
 * Validates and sanitizes a filter object
 */
export function validateFilter(filter: unknown, field = 'filter'): Record<string, unknown> | undefined {
  if (isNullish(filter)) {
    return undefined
  }

  if (typeof filter !== 'object') {
    throw new ValidationError(field, `must be an object, got ${typeof filter}`)
  }

  if (Array.isArray(filter)) {
    throw new ValidationError(field, 'must be an object, not an array')
  }

  const sanitized = sanitizeObject<Record<string, unknown>>(filter, field)
  validateFilterOperators(sanitized, field)

  return sanitized
}

/**
 * Validates that all operator keys in an update object are allowed
 */
function validateUpdateOperators(obj: Record<string, unknown>, field: string): void {
  const keys = Object.keys(obj)

  if (keys.length === 0) {
    throw new ValidationError(field, 'cannot be empty')
  }

  // All top-level keys should be operators
  for (const key of keys) {
    if (!key.startsWith('$')) {
      throw new ValidationError(field, `invalid key "${key}": update operations must use operators like $set, $inc, etc.`)
    }

    if (!ALLOWED_UPDATE_OPERATORS.has(key)) {
      throw new ValidationError(field, `contains unknown operator: ${key}`)
    }

    // Validate that operator value is an object
    const value = obj[key]
    if (value === null || typeof value !== 'object' || Array.isArray(value)) {
      throw new ValidationError(`${field}.${key}`, 'must be an object')
    }
  }
}

/**
 * Validates and sanitizes an update object
 */
export function validateUpdate(update: unknown, field = 'update'): Record<string, unknown> {
  if (isNullish(update)) {
    throw new ValidationError(field, 'is required')
  }

  if (typeof update !== 'object') {
    throw new ValidationError(field, `must be an object, got ${typeof update}`)
  }

  if (Array.isArray(update)) {
    throw new ValidationError(field, 'must be an object, not an array')
  }

  const sanitized = sanitizeObject<Record<string, unknown>>(update, field)
  validateUpdateOperators(sanitized, field)

  return sanitized
}

/**
 * Validates and sanitizes entity data for creation
 */
export function validateEntityData(data: unknown, field = 'data'): Record<string, unknown> {
  if (isNullish(data)) {
    throw new ValidationError(field, 'is required')
  }

  if (typeof data !== 'object') {
    throw new ValidationError(field, `must be an object, got ${typeof data}`)
  }

  if (Array.isArray(data)) {
    throw new ValidationError(field, 'must be an object, not an array')
  }

  const sanitized = sanitizeObject<Record<string, unknown>>(data, field)

  // Ensure 'name' field exists (required for entities)
  if (!('name' in sanitized) || isNullish(sanitized.name)) {
    throw new ValidationError(`${field}.name`, 'is required')
  }

  if (typeof sanitized.name !== 'string') {
    throw new ValidationError(`${field}.name`, `must be a string, got ${typeof sanitized.name}`)
  }

  if (sanitized.name.trim().length === 0) {
    throw new ValidationError(`${field}.name`, 'cannot be empty')
  }

  return sanitized
}

/**
 * Validates and sanitizes an aggregation pipeline
 */
export function validatePipeline(pipeline: unknown, field = 'pipeline'): Array<Record<string, unknown>> {
  if (isNullish(pipeline)) {
    throw new ValidationError(field, 'is required')
  }

  if (!Array.isArray(pipeline)) {
    throw new ValidationError(field, `must be an array, got ${typeof pipeline}`)
  }

  if (pipeline.length === 0) {
    throw new ValidationError(field, 'cannot be empty')
  }

  if (pipeline.length > 20) {
    throw new ValidationError(field, 'cannot exceed 20 stages')
  }

  const result: Array<Record<string, unknown>> = []

  for (let i = 0; i < pipeline.length; i++) {
    const stage = pipeline[i]

    if (stage === null || typeof stage !== 'object' || Array.isArray(stage)) {
      throw new ValidationError(`${field}[${i}]`, 'must be an object')
    }

    const sanitizedStage = sanitizeObject<Record<string, unknown>>(stage, `${field}[${i}]`)
    const stageKeys = Object.keys(sanitizedStage)

    if (stageKeys.length !== 1) {
      throw new ValidationError(
        `${field}[${i}]`,
        'must contain exactly one stage operator'
      )
    }

    const stageOp = stageKeys[0]
    if (!stageOp.startsWith('$')) {
      throw new ValidationError(
        `${field}[${i}]`,
        `stage key "${stageOp}" must start with $`
      )
    }

    if (!ALLOWED_PIPELINE_STAGES.has(stageOp)) {
      throw new ValidationError(
        `${field}[${i}]`,
        `unknown or unsupported stage: ${stageOp}`
      )
    }

    // Validate $match stage filter
    if (stageOp === '$match') {
      const matchValue = sanitizedStage.$match
      if (matchValue !== null && typeof matchValue === 'object' && !Array.isArray(matchValue)) {
        validateFilterOperators(matchValue as Record<string, unknown>, `${field}[${i}].$match`)
      }
    }

    result.push(sanitizedStage)
  }

  return result
}

/**
 * Validates a numeric limit parameter
 */
export function validateLimit(limit: unknown, field = 'limit'): number | undefined {
  if (isNullish(limit)) {
    return undefined
  }

  if (typeof limit !== 'number') {
    throw new ValidationError(field, `must be a number, got ${typeof limit}`)
  }

  if (!Number.isInteger(limit)) {
    throw new ValidationError(field, 'must be an integer')
  }

  if (limit < 0) {
    throw new ValidationError(field, 'must be non-negative')
  }

  if (limit > MAX_MCP_PAGINATION_LIMIT) {
    throw new ValidationError(field, `cannot exceed ${MAX_MCP_PAGINATION_LIMIT}`)
  }

  return limit
}

/**
 * Validates a numeric skip/offset parameter
 */
export function validateSkip(skip: unknown, field = 'skip'): number | undefined {
  if (isNullish(skip)) {
    return undefined
  }

  if (typeof skip !== 'number') {
    throw new ValidationError(field, `must be a number, got ${typeof skip}`)
  }

  if (!Number.isInteger(skip)) {
    throw new ValidationError(field, 'must be an integer')
  }

  if (skip < 0) {
    throw new ValidationError(field, 'must be non-negative')
  }

  return skip
}

/**
 * Validates a sort specification
 */
export function validateSort(sort: unknown, field = 'sort'): Record<string, 1 | -1> | undefined {
  if (isNullish(sort)) {
    return undefined
  }

  if (typeof sort !== 'object') {
    throw new ValidationError(field, `must be an object, got ${typeof sort}`)
  }

  if (Array.isArray(sort)) {
    throw new ValidationError(field, 'must be an object, not an array')
  }

  const sanitized = sanitizeObject<Record<string, unknown>>(sort, field)
  const result: Record<string, 1 | -1> = Object.create(null)

  for (const key of Object.keys(sanitized)) {
    const value = sanitized[key]

    if (value !== 1 && value !== -1) {
      throw new ValidationError(`${field}.${key}`, 'must be 1 (ascending) or -1 (descending)')
    }

    result[key] = value
  }

  return result
}

/**
 * Validates a projection specification
 */
export function validateProject(project: unknown, field = 'project'): Record<string, 0 | 1> | undefined {
  if (isNullish(project)) {
    return undefined
  }

  if (typeof project !== 'object') {
    throw new ValidationError(field, `must be an object, got ${typeof project}`)
  }

  if (Array.isArray(project)) {
    throw new ValidationError(field, 'must be an object, not an array')
  }

  const sanitized = sanitizeObject<Record<string, unknown>>(project, field)
  const result: Record<string, 0 | 1> = Object.create(null)

  for (const key of Object.keys(sanitized)) {
    const value = sanitized[key]

    if (value !== 0 && value !== 1) {
      throw new ValidationError(`${field}.${key}`, 'must be 0 (exclude) or 1 (include)')
    }

    result[key] = value
  }

  return result
}

/**
 * Validates a boolean parameter
 */
export function validateBoolean(value: unknown, field: string, defaultValue?: boolean): boolean {
  if (isNullish(value)) {
    return defaultValue ?? false
  }

  if (typeof value !== 'boolean') {
    throw new ValidationError(field, `must be a boolean, got ${typeof value}`)
  }

  return value
}

/**
 * Validates a search query string
 */
export function validateSearchQuery(query: unknown, field = 'query'): string {
  if (isNullish(query)) {
    throw new ValidationError(field, 'is required')
  }

  if (typeof query !== 'string') {
    throw new ValidationError(field, `must be a string, got ${typeof query}`)
  }

  const trimmed = query.trim()

  if (trimmed.length === 0) {
    throw new ValidationError(field, 'cannot be empty')
  }

  if (trimmed.length > MAX_MCP_PROMPT_LENGTH) {
    throw new ValidationError(field, `must be ${MAX_MCP_PROMPT_LENGTH} characters or fewer`)
  }

  return trimmed
}
