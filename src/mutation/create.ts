/**
 * Create Operation Module for ParqueDB Mutation Layer
 *
 * Provides functions for creating new entities with validation,
 * schema default application, and event generation.
 */

import type {
  Entity,
  EntityId,
  CreateInput,
  Schema,
} from '../types'
import {
  MutationContext,
  CreateResult,
  MutationEvent,
  MutationOperationError,
  MutationErrorCodes,
} from './types'
import { generateId } from '../utils'
import { entityTarget } from '../types/entity'

// =============================================================================
// Create Operation
// =============================================================================

/**
 * Options for the create operation
 */
export interface CreateOperationOptions {
  /** Schema for validation and defaults */
  schema?: Schema

  /** Schema validator instance */
  schemaValidator?: SchemaValidatorInterface | null

  /** ID generator function */
  generateId?: () => string
}

/**
 * Schema validator interface (minimal interface for dependency injection)
 */
export interface SchemaValidatorInterface {
  hasType(typeName: string): boolean
  validate(typeName: string, data: unknown, skipCoreFields?: boolean): void
}

/**
 * Execute a create operation
 *
 * @param context - Mutation context
 * @param data - Entity data to create
 * @param options - Create operation options
 * @returns Create result with entity and events
 */
export function executeCreate<T = Record<string, unknown>>(
  context: MutationContext,
  data: CreateInput<T>,
  options?: CreateOperationOptions
): CreateResult<T> {
  // Validate required fields
  if (!context.skipValidation) {
    validateCreateInput(data, context)
  }

  // Validate against schema if available
  if (!context.skipValidation && options?.schema && options.schemaValidator) {
    const typeName = data.$type
    if (typeName && options.schemaValidator.hasType(typeName)) {
      try {
        options.schemaValidator.validate(typeName, data, true) // skipCoreFields=true
      } catch (error) {
        throw new MutationOperationError(
          MutationErrorCodes.VALIDATION_FAILED,
          error instanceof Error ? error.message : 'Schema validation failed'
        )
      }
    }
  }

  // Apply schema defaults
  const dataWithDefaults = options?.schema
    ? applySchemaDefaults(data, options.schema)
    : data

  // Generate ID
  const idGen = options?.generateId || generateId
  const id = idGen()
  const fullId = `${context.namespace}/${id}` as EntityId

  // Build entity
  const entity: Entity<T> = {
    ...dataWithDefaults,
    $id: fullId,
    $type: dataWithDefaults.$type,
    name: dataWithDefaults.name,
    createdAt: context.timestamp,
    createdBy: context.actor,
    updatedAt: context.timestamp,
    updatedBy: context.actor,
    version: 1,
  } as Entity<T>

  // Generate CREATE event
  const event: MutationEvent = {
    op: 'CREATE',
    target: entityTarget(context.namespace, id),
    before: null,
    after: entity as Record<string, unknown>,
    actor: context.actor,
    timestamp: context.timestamp,
  }

  return {
    entity,
    entityId: fullId,
    events: [event],
  }
}

// =============================================================================
// Validation
// =============================================================================

/**
 * Validate create input data
 * @throws MutationOperationError if validation fails
 */
export function validateCreateInput(
  data: CreateInput,
  context: MutationContext
): void {
  if (!data.$type) {
    throw new MutationOperationError(
      MutationErrorCodes.VALIDATION_FAILED,
      'Entity must have a $type field'
    )
  }

  if (!data.name) {
    throw new MutationOperationError(
      MutationErrorCodes.VALIDATION_FAILED,
      'Entity must have a name field'
    )
  }

  // Validate $type is a string
  if (typeof data.$type !== 'string') {
    throw new MutationOperationError(
      MutationErrorCodes.VALIDATION_FAILED,
      '$type must be a string'
    )
  }

  // Validate name is a string
  if (typeof data.name !== 'string') {
    throw new MutationOperationError(
      MutationErrorCodes.VALIDATION_FAILED,
      'name must be a string'
    )
  }
}

// =============================================================================
// Schema Helpers
// =============================================================================

/**
 * Apply default values from schema
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
      const def = fieldDef as { default?: unknown }
      defaultValue = def.default
    }

    if (defaultValue !== undefined) {
      result[fieldName] = defaultValue
    }
  }

  return result as CreateInput<T>
}

/**
 * Validate namespace
 * @throws Error if namespace is invalid
 */
export function validateNamespace(namespace: string): void {
  if (!namespace || typeof namespace !== 'string') {
    throw new Error('Namespace is required and must be a non-empty string')
  }
  if (namespace.includes('/')) {
    throw new Error('Namespace cannot contain "/" character')
  }
  if (namespace.startsWith('_')) {
    throw new Error('Namespace cannot start with underscore')
  }
  if (namespace.startsWith('$')) {
    throw new Error('Namespace cannot start with dollar sign')
  }
}

/**
 * Normalize namespace to lowercase
 */
export function normalizeNamespace(name: string): string {
  return name.toLowerCase()
}
