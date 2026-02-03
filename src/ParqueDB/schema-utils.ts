/**
 * ParqueDB Schema Utilities Module
 *
 * Contains shared helper functions for schema validation and defaults.
 * These functions are used by both crud.ts and entity-operations.ts.
 */

import type { CreateInput, Schema } from '../types'
import { parseFieldType, isRelationString } from '../types/schema'
import { ValidationError } from './types'
import pluralize from 'pluralize'

/**
 * Derive entity type from namespace/collection name
 * e.g., 'posts' -> 'Post', 'users' -> 'User', 'categories' -> 'Category'
 */
export function deriveTypeFromNamespace(namespace: string): string {
  const singular = pluralize.singular(namespace)
  return singular.charAt(0).toUpperCase() + singular.slice(1)
}

/**
 * Check if a field is required based on its schema definition.
 */
export function isFieldRequired(fieldDef: unknown): boolean {
  if (typeof fieldDef === 'string') {
    return fieldDef.includes('!')
  }

  if (typeof fieldDef === 'object' && fieldDef !== null) {
    const def = fieldDef as { type?: string | undefined; required?: boolean | undefined }
    if (def.required) return true
    if (def.type && def.type.includes('!')) return true
  }

  return false
}

/**
 * Check if a field has a default value defined in its schema.
 */
export function hasDefault(fieldDef: unknown): boolean {
  if (typeof fieldDef === 'string') {
    return fieldDef.includes('=')
  }

  if (typeof fieldDef === 'object' && fieldDef !== null) {
    return 'default' in (fieldDef as object)
  }

  return false
}

/**
 * Validate a field value against its type definition from the schema.
 */
export function validateFieldType(
  fieldName: string,
  value: unknown,
  fieldDef: unknown,
  typeName: string
): void {
  let expectedType: string | undefined

  if (typeof fieldDef === 'string') {
    // Skip relationship definitions
    if (isRelationString(fieldDef)) {
      // Validate relationship reference format
      if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
        for (const [, refValue] of Object.entries(value)) {
          if (typeof refValue !== 'string' || !refValue.includes('/')) {
            throw new ValidationError(
              'validation',
              typeName,
              'Invalid relationship reference format (must be "ns/id")',
              { fieldName }
            )
          }
        }
      }
      return
    }
    const parsed = parseFieldType(fieldDef)
    expectedType = parsed.type
  } else if (typeof fieldDef === 'object' && fieldDef !== null) {
    const def = fieldDef as { type?: string | undefined }
    if (def.type && !isRelationString(def.type)) {
      const parsed = parseFieldType(def.type)
      expectedType = parsed.type
    }
  }

  if (!expectedType) return

  // Basic type validation
  const actualType = typeof value
  switch (expectedType) {
    case 'string':
    case 'text':
    case 'markdown':
    case 'email':
    case 'url':
    case 'uuid':
      if (actualType !== 'string') {
        throw new ValidationError('validation', typeName, 'Type mismatch', {
          fieldName,
          expectedType: 'string',
          actualType,
        })
      }
      break
    case 'number':
    case 'int':
    case 'float':
    case 'double':
      if (actualType !== 'number') {
        throw new ValidationError('validation', typeName, 'Type mismatch', {
          fieldName,
          expectedType: 'number',
          actualType,
        })
      }
      break
    case 'boolean':
      if (actualType !== 'boolean') {
        throw new ValidationError('validation', typeName, 'Type mismatch', {
          fieldName,
          expectedType: 'boolean',
          actualType,
        })
      }
      break
    case 'date':
    case 'datetime':
    case 'timestamp':
      if (!(value instanceof Date) && actualType !== 'string') {
        throw new ValidationError('validation', typeName, 'Type mismatch', {
          fieldName,
          expectedType: 'date',
          actualType,
        })
      }
      break
  }
}

/**
 * Apply default values from the schema to create input data.
 */
export function applySchemaDefaults<T>(
  data: CreateInput<T>,
  schema: Schema
): CreateInput<T> {
  const typeName = data.$type
  if (!typeName) return data

  const typeDef = schema[typeName]
  if (!typeDef) return data

  const result: Record<string, unknown> = { ...data }

  for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
    if (fieldName.startsWith('$')) continue
    if (result[fieldName] !== undefined) continue

    // Extract default value
    let defaultValue: unknown

    if (typeof fieldDef === 'string') {
      const match = fieldDef.match(/=\s*(.+)$/)
      if (match && match[1]) {
        defaultValue = match[1].trim()
        // Try to parse as JSON
        try {
          defaultValue = JSON.parse(defaultValue as string)
        } catch {
          // Intentionally ignored: value is not valid JSON, keep as raw string
        }
      }
    } else if (typeof fieldDef === 'object' && fieldDef !== null) {
      const def = fieldDef as { default?: unknown | undefined }
      defaultValue = def.default
    }

    if (defaultValue !== undefined) {
      result[fieldName] = defaultValue
    }
  }

  return result as CreateInput<T>
}
