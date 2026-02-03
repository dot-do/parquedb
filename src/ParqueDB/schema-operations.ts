/**
 * ParqueDB Schema Operations Module
 *
 * Contains operations for schema management: registration, validation,
 * and type derivation utilities.
 */

import type {
  CreateInput,
  Schema,
  ValidationMode,
} from '../types'

import { isRelationString } from '../types/schema'
import { SchemaValidator } from '../schema/validator'

import { ValidationError } from './types'
import {
  isFieldRequired,
  hasDefault,
  validateFieldType,
} from './entity-operations'

// =============================================================================
// Types
// =============================================================================

/**
 * Context object for schema operations.
 * Provides access to shared state and dependencies.
 */
export interface SchemaOperationsContext {
  schema: Schema
  schemaValidator: SchemaValidator | null
  setSchemaValidator: (validator: SchemaValidator | null) => void
  setSchema: (schema: Schema) => void
}

// =============================================================================
// Schema Registration
// =============================================================================

/**
 * Register a schema for validation.
 *
 * @param ctx - The schema operations context
 * @param schema - The schema to register
 */
export function registerSchema(ctx: SchemaOperationsContext, schema: Schema): void {
  // Merge with existing schema
  const mergedSchema = { ...ctx.schema, ...schema }
  ctx.setSchema(mergedSchema)

  // Create/update the schema validator
  const validator = new SchemaValidator(mergedSchema, {
    mode: 'permissive',
    allowUnknownFields: true,
  })
  ctx.setSchemaValidator(validator)
}

// =============================================================================
// Schema Validation
// =============================================================================

/**
 * Validate data against schema with configurable mode.
 *
 * @param ctx - The schema operations context
 * @param namespace - The namespace being validated
 * @param data - The data to validate
 * @param validateOnWrite - Validation mode (true, false, or ValidationMode)
 */
export function validateAgainstSchema(
  ctx: SchemaOperationsContext,
  _namespace: string,
  data: CreateInput,
  validateOnWrite?: boolean | ValidationMode
): void {
  const typeName = data.$type
  if (!typeName) return

  // Determine validation mode
  let mode: ValidationMode
  if (validateOnWrite === false) {
    return // Skip validation
  } else if (validateOnWrite === true || validateOnWrite === undefined) {
    mode = 'strict'
  } else {
    mode = validateOnWrite
  }

  // If no schema validator, use legacy validation
  if (!ctx.schemaValidator) {
    legacyValidateAgainstSchema(ctx, _namespace, data)
    return
  }

  // Check if type is defined in schema
  if (!ctx.schemaValidator.hasType(typeName)) {
    return // No schema for this type, skip validation
  }

  // Create a temporary validator with the specified mode
  const validator = new SchemaValidator(ctx.schema, {
    mode,
    allowUnknownFields: true,
  })

  // Validate - this will throw SchemaValidationError if mode is 'strict'
  validator.validate(typeName, data, true) // skipCoreFields=true for create input
}

/**
 * Legacy validation method for backward compatibility.
 *
 * This method performs field-level validation using direct schema inspection
 * rather than the SchemaValidator class.
 */
export function legacyValidateAgainstSchema(
  ctx: SchemaOperationsContext,
  _namespace: string,
  data: CreateInput
): void {
  const typeName = data.$type
  if (!typeName) return

  const typeDef = ctx.schema[typeName]
  if (!typeDef) return // No schema for this type, skip validation

  // Check required fields
  for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
    if (fieldName.startsWith('$')) continue // Skip meta fields

    const required = isFieldRequired(fieldDef)
    const hasDefaultValue = hasDefault(fieldDef)
    const fieldValue = data[fieldName]

    if (required && !hasDefaultValue && fieldValue === undefined) {
      throw new ValidationError('create', typeName, `Missing required field: ${fieldName}`, {
        fieldName,
      })
    }

    // Validate field type
    if (fieldValue !== undefined) {
      validateFieldType(fieldName, fieldValue, fieldDef, data.$type || typeName)
    }
  }
}

// =============================================================================
// Type Checking Utilities
// =============================================================================

/**
 * Check if a type has a schema definition.
 *
 * @param ctx - The schema operations context
 * @param typeName - The type name to check
 * @returns True if the type has a schema definition
 */
export function hasTypeSchema(ctx: SchemaOperationsContext, typeName: string): boolean {
  if (ctx.schemaValidator) {
    return ctx.schemaValidator.hasType(typeName)
  }
  return typeName in ctx.schema
}

/**
 * Get the schema definition for a type.
 *
 * @param ctx - The schema operations context
 * @param typeName - The type name
 * @returns The type definition or undefined
 */
export function getTypeSchema(
  ctx: SchemaOperationsContext,
  typeName: string
): Record<string, unknown> | undefined {
  return ctx.schema[typeName]
}

/**
 * Get field definition from a type schema.
 *
 * @param ctx - The schema operations context
 * @param typeName - The type name
 * @param fieldName - The field name
 * @returns The field definition or undefined
 */
export function getFieldSchema(
  ctx: SchemaOperationsContext,
  typeName: string,
  fieldName: string
): unknown | undefined {
  const typeDef = ctx.schema[typeName]
  if (!typeDef) return undefined
  return typeDef[fieldName]
}

/**
 * Check if a field is a relationship field.
 *
 * @param ctx - The schema operations context
 * @param typeName - The type name
 * @param fieldName - The field name
 * @returns True if the field is a relationship
 */
export function isRelationshipField(
  ctx: SchemaOperationsContext,
  typeName: string,
  fieldName: string
): boolean {
  const fieldDef = getFieldSchema(ctx, typeName, fieldName)
  if (typeof fieldDef === 'string') {
    return isRelationString(fieldDef)
  }
  if (typeof fieldDef === 'object' && fieldDef !== null) {
    const def = fieldDef as { type?: string | undefined }
    if (def.type) {
      return isRelationString(def.type)
    }
  }
  return false
}

/**
 * Check if a field is a reverse relationship.
 *
 * @param ctx - The schema operations context
 * @param typeName - The type name
 * @param fieldName - The field name
 * @returns True if the field is a reverse relationship (<-)
 */
export function isReverseRelationshipField(
  ctx: SchemaOperationsContext,
  typeName: string,
  fieldName: string
): boolean {
  const fieldDef = getFieldSchema(ctx, typeName, fieldName)
  if (typeof fieldDef === 'string') {
    return fieldDef.startsWith('<-')
  }
  return false
}

/**
 * Get all relationship fields for a type.
 *
 * @param ctx - The schema operations context
 * @param typeName - The type name
 * @returns Array of field names that are relationships
 */
export function getRelationshipFields(
  ctx: SchemaOperationsContext,
  typeName: string
): string[] {
  const typeDef = ctx.schema[typeName]
  if (!typeDef) return []

  const fields: string[] = []
  for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
    if (fieldName.startsWith('$')) continue
    if (typeof fieldDef === 'string' && isRelationString(fieldDef)) {
      fields.push(fieldName)
    }
  }
  return fields
}

/**
 * Get all reverse relationship fields for a type.
 *
 * @param ctx - The schema operations context
 * @param typeName - The type name
 * @returns Array of field names that are reverse relationships
 */
export function getReverseRelationshipFields(
  ctx: SchemaOperationsContext,
  typeName: string
): string[] {
  const typeDef = ctx.schema[typeName]
  if (!typeDef) return []

  const fields: string[] = []
  for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
    if (fieldName.startsWith('$')) continue
    if (typeof fieldDef === 'string' && fieldDef.startsWith('<-')) {
      fields.push(fieldName)
    }
  }
  return fields
}
