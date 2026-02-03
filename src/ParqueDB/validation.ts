/**
 * ParqueDB Validation Module
 *
 * Contains validation utilities for namespaces, filters, and update operators.
 */

import type { Filter, UpdateInput } from '../types'
import { ValidationError } from './types'

// =============================================================================
// Valid Operators
// =============================================================================

/** Valid filter operators */
export const VALID_FILTER_OPERATORS = new Set([
  '$eq', '$ne', '$gt', '$gte', '$lt', '$lte', '$in', '$nin',
  '$and', '$or', '$not', '$nor',
  '$regex', '$options', '$startsWith', '$endsWith', '$contains',
  '$all', '$elemMatch', '$size',
  '$exists', '$type',
  '$text', '$vector', '$geo',
])

/** Valid update operators */
export const VALID_UPDATE_OPERATORS = new Set([
  '$set', '$unset', '$rename', '$setOnInsert',
  '$inc', '$mul', '$min', '$max',
  '$push', '$pull', '$pullAll', '$addToSet', '$pop',
  '$currentDate',
  '$link', '$unlink',
  '$bit',
  '$embed', // ParqueDB AI embedding operator
])

// =============================================================================
// Validation Functions
// =============================================================================

/**
 * Validate a namespace string
 *
 * @throws {ValidationError} if namespace is invalid
 */
export function validateNamespace(namespace: string): void {
  if (!namespace || typeof namespace !== 'string') {
    throw new ValidationError('namespace', namespace || '', 'Namespace is required and must be a non-empty string')
  }
  if (namespace.includes('/')) {
    throw new ValidationError('namespace', namespace, 'Namespace cannot contain "/" character')
  }
  if (namespace.startsWith('_')) {
    throw new ValidationError('namespace', namespace, 'Namespace cannot start with underscore')
  }
  if (namespace.startsWith('$')) {
    throw new ValidationError('namespace', namespace, 'Namespace cannot start with dollar sign')
  }
}

/**
 * Validate filter operators recursively
 *
 * @throws {ValidationError} if filter contains invalid operators
 */
export function validateFilter(filter: Filter): void {
  if (!filter || typeof filter !== 'object') return

  for (const [key, value] of Object.entries(filter)) {
    if (key.startsWith('$')) {
      if (!VALID_FILTER_OPERATORS.has(key)) {
        throw new ValidationError('filter', '', `Invalid filter operator: ${key}`, {
          fieldName: key,
        })
      }
      // Recursively validate nested filters
      if (key === '$and' || key === '$or' || key === '$nor') {
        if (Array.isArray(value)) {
          value.forEach(v => validateFilter(v as Filter))
        }
      } else if (key === '$not' && typeof value === 'object') {
        validateFilter(value as Filter)
      }
    } else if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      // Field with operators
      for (const op of Object.keys(value as object)) {
        if (op.startsWith('$') && !VALID_FILTER_OPERATORS.has(op)) {
          throw new ValidationError('filter', '', `Invalid filter operator: ${op}`, {
            fieldName: op,
          })
        }
      }
    }
  }
}

/**
 * Validate update operators
 *
 * @throws {ValidationError} if update contains invalid operators
 */
export function validateUpdateOperators(update: UpdateInput): void {
  for (const key of Object.keys(update)) {
    if (key.startsWith('$') && !VALID_UPDATE_OPERATORS.has(key)) {
      throw new ValidationError('update', '', `Invalid update operator: ${key}`, {
        fieldName: key,
      })
    }
  }
}

/**
 * Normalize namespace to lowercase
 */
export function normalizeNamespace(name: string): string {
  // Convert PascalCase/camelCase to lowercase (Posts -> posts, BlogPosts -> blogposts)
  return name.toLowerCase()
}
