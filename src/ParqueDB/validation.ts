/**
 * ParqueDB Validation Module
 *
 * Contains validation utilities for namespaces, filters, and update operators.
 */

import type { EntityId, Filter, UpdateInput } from '../types'
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

// =============================================================================
// Entity ID Validation
// =============================================================================

/**
 * Validate a full EntityId string (format: "namespace/localId")
 *
 * @param id - The entity ID to validate
 * @throws {ValidationError} if the entity ID is invalid
 *
 * @example
 * ```typescript
 * validateEntityId('users/user-123')  // OK
 * validateEntityId('invalidid')       // throws ValidationError
 * ```
 */
export function validateEntityId(id: string): void {
  if (!id || typeof id !== 'string') {
    throw new ValidationError('entityId', id || '', 'Entity ID is required and must be a non-empty string')
  }

  const slashIndex = id.indexOf('/')

  if (slashIndex === -1) {
    throw new ValidationError('entityId', id, 'Entity ID must be in "namespace/id" format')
  }

  if (slashIndex === 0) {
    throw new ValidationError('entityId', id, 'Entity ID namespace cannot be empty')
  }

  const namespace = id.slice(0, slashIndex)
  const localId = id.slice(slashIndex + 1)

  if (localId.length === 0) {
    throw new ValidationError('entityId', id, 'Entity ID local part cannot be empty')
  }

  // Validate namespace constraints
  if (namespace.startsWith('_')) {
    throw new ValidationError('entityId', id, 'Entity ID namespace cannot start with underscore')
  }

  if (namespace.startsWith('$')) {
    throw new ValidationError('entityId', id, 'Entity ID namespace cannot start with dollar sign')
  }
}

/**
 * Validate a local ID (the part after the namespace)
 *
 * @param id - The local ID to validate
 * @throws {ValidationError} if the local ID is invalid
 *
 * @example
 * ```typescript
 * validateLocalId('user-123')  // OK
 * validateLocalId('')          // throws ValidationError
 * ```
 */
export function validateLocalId(id: string): void {
  if (!id || typeof id !== 'string') {
    throw new ValidationError('localId', id || '', 'Local ID is required and must be a non-empty string')
  }
}

/**
 * Convert namespace and id to a full entity ID string.
 *
 * This is a simple utility that handles both "ns/id" and just "id" formats.
 * If the ID already contains a '/', it's returned as-is.
 * Otherwise, it's prefixed with the provided namespace.
 *
 * Unlike `normalizeEntityId`, this function does NOT perform validation.
 * Use this when validation has already been done separately.
 *
 * @param namespace - The namespace to use if ID doesn't have one
 * @param id - The entity ID (can be full "ns/id" or just "id")
 * @returns The full ID in "namespace/id" format
 *
 * @example
 * ```typescript
 * toFullId('users', 'user-123')        // 'users/user-123'
 * toFullId('posts', 'users/user-123')  // 'users/user-123' (unchanged)
 * ```
 */
export function toFullId(namespace: string, id: string): string {
  return id.includes('/') ? id : `${namespace}/${id}`
}

/**
 * Normalize an entity ID with namespace context
 *
 * If the ID already contains a '/', it's treated as a full EntityId and validated.
 * Otherwise, it's prefixed with the provided namespace.
 *
 * @param namespace - The default namespace to use if ID doesn't have one
 * @param id - The entity ID (can be full "ns/id" or just "id")
 * @returns The normalized EntityId in "namespace/id" format
 * @throws {ValidationError} if validation fails
 *
 * @example
 * ```typescript
 * normalizeEntityId('users', 'user-123')        // 'users/user-123'
 * normalizeEntityId('posts', 'users/user-123')  // 'users/user-123' (unchanged)
 * ```
 */
export function normalizeEntityId(namespace: string, id: string): EntityId {
  // Validate namespace first
  validateNamespace(namespace)

  // If ID contains a slash, treat it as a full EntityId
  if (id.includes('/')) {
    validateEntityId(id)
    return id as EntityId
  }

  // Otherwise, validate the local ID and combine with namespace
  validateLocalId(id)

  // Normalize the namespace to lowercase
  const normalizedNs = normalizeNamespace(namespace)

  return `${normalizedNs}/${id}` as EntityId
}

// =============================================================================
// $id Directive Resolution
// =============================================================================

import type { Schema, TypeDefinition, FieldDef } from '../types/schema'
import { generateId } from '../utils'

/**
 * Check if a field is required based on its schema definition.
 * Exported from validation module for use in $id directive validation.
 */
export function isFieldRequired(fieldDef: FieldDef | unknown): boolean {
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
 * Result of resolving an entity ID
 */
export interface ResolvedEntityId {
  /** The full entity ID in "namespace/id" format */
  fullId: EntityId
  /** The local ID part (without namespace) */
  localId: string
}

/**
 * Options for resolving an entity ID
 */
export interface ResolveEntityIdOptions {
  /** The namespace/collection name */
  namespace: string
  /** The type name derived from namespace (e.g., 'User' from 'users') */
  typeName: string
  /** The schema containing type definitions */
  schema: Schema
  /** The create data that may contain $id or the field specified by $id directive */
  data: Record<string, unknown>
}

// =============================================================================
// Directive Validation Helpers
// =============================================================================

/**
 * Options for directive field validation
 */
interface DirectiveValidationOptions {
  /** The directive name (e.g., '$id', '$name') */
  directiveName: string
  /** The type name being validated */
  typeName: string
  /** The type definition containing the directive */
  typeDef: TypeDefinition
  /** The field name specified by the directive */
  fieldName: string
  /** Whether to warn if the field is optional (default: false) */
  warnIfOptional?: boolean
}

/**
 * Validates that a directive references a valid field in the schema.
 *
 * Shared validation logic for $id, $name, and other directives that
 * reference schema fields.
 *
 * Checks:
 * 1. The specified field exists in the type definition
 * 2. The specified field is not itself a directive (doesn't start with $)
 * 3. Optionally warns if the field is optional
 *
 * @param options - Validation options
 * @throws {ValidationError} if validation fails
 */
function validateDirectiveField(options: DirectiveValidationOptions): void {
  const { directiveName, typeName, typeDef, fieldName, warnIfOptional } = options

  // Check if the field is a directive (starts with $) - that's not allowed
  if (fieldName.startsWith('$')) {
    throw new ValidationError(
      'schema',
      typeName,
      `${directiveName} directive cannot reference another directive '${fieldName}'`,
      { fieldName }
    )
  }

  // Check if the field exists in the type definition
  const fieldDef = typeDef[fieldName]
  if (fieldDef === undefined) {
    throw new ValidationError(
      'schema',
      typeName,
      `${directiveName} directive references nonexistent field '${fieldName}'`,
      { fieldName }
    )
  }

  // Warn if the field is not required (but don't throw)
  if (warnIfOptional && !isFieldRequired(fieldDef)) {
    console.warn(
      `[ParqueDB] Warning: ${directiveName} directive on '${typeName}' references optional field '${fieldName}'. ` +
      `Consider making it required.`
    )
  }
}

// =============================================================================
// Directive Validators
// =============================================================================

/**
 * Validates that a $id directive in a schema is valid.
 *
 * Checks:
 * 1. The specified field exists in the type definition
 * 2. The specified field is not a directive
 * 3. Warns if the specified field is optional
 *
 * @param typeName - The name of the type being validated
 * @param typeDef - The type definition
 * @throws {ValidationError} if $id references a nonexistent field or directive
 *
 * @example
 * ```typescript
 * validateIdDirective('User', {
 *   $id: 'email',
 *   email: 'string!#',
 *   name: 'string',
 * })
 * // OK - email field exists and is required
 *
 * validateIdDirective('User', {
 *   $id: 'nonexistent',
 *   email: 'string!#',
 * })
 * // Throws: $id directive references nonexistent field 'nonexistent'
 * ```
 */
export function validateIdDirective(typeName: string, typeDef: TypeDefinition): void {
  const idFieldName = typeDef.$id
  if (!idFieldName || typeof idFieldName !== 'string') {
    return // No $id directive, nothing to validate
  }

  validateDirectiveField({
    directiveName: '$id',
    typeName,
    typeDef,
    fieldName: idFieldName,
    warnIfOptional: true,
  })
}

/**
 * Validates that a $name directive in a schema is valid.
 *
 * Checks:
 * 1. The specified field exists in the type definition
 * 2. The specified field is not a directive
 *
 * @param typeName - The name of the type being validated
 * @param typeDef - The type definition
 * @throws {ValidationError} if $name references a nonexistent field or directive
 *
 * @example
 * ```typescript
 * validateNameDirective('User', {
 *   $name: 'fullName',
 *   email: 'string!#',
 *   fullName: 'string!',
 * })
 * // OK - fullName field exists
 *
 * validateNameDirective('User', {
 *   $name: 'nonexistent',
 *   email: 'string!#',
 * })
 * // Throws: $name directive references nonexistent field 'nonexistent'
 * ```
 */
export function validateNameDirective(typeName: string, typeDef: TypeDefinition): void {
  const nameFieldName = typeDef.$name
  if (!nameFieldName || typeof nameFieldName !== 'string') {
    return // No $name directive, nothing to validate
  }

  validateDirectiveField({
    directiveName: '$name',
    typeName,
    typeDef,
    fieldName: nameFieldName,
    // Note: Unlike $id, we don't warn for optional $name fields
    // because it's less critical - entity still works without a name
    warnIfOptional: false,
  })
}

/**
 * Resolves an entity ID from create data, using:
 * 1. Explicit $id in the data (highest priority)
 * 2. $id directive from schema (uses field value as ID)
 * 3. Generated ULID (fallback)
 *
 * @param options - Resolution options
 * @returns The resolved entity ID (full ID and local part)
 * @throws {ValidationError} if ID resolution fails validation
 *
 * @example
 * ```typescript
 * // With explicit $id
 * resolveEntityId({
 *   namespace: 'users',
 *   typeName: 'User',
 *   schema: {},
 *   data: { $id: 'custom-id', name: 'Alice' }
 * })
 * // Returns { fullId: 'users/custom-id', localId: 'custom-id' }
 *
 * // With $id directive in schema
 * resolveEntityId({
 *   namespace: 'users',
 *   typeName: 'User',
 *   schema: { User: { $id: 'email', email: 'string!#' } },
 *   data: { email: 'alice@example.com', name: 'Alice' }
 * })
 * // Returns { fullId: 'users/alice@example.com', localId: 'alice@example.com' }
 *
 * // Generated ULID fallback
 * resolveEntityId({
 *   namespace: 'posts',
 *   typeName: 'Post',
 *   schema: {},
 *   data: { title: 'Hello World' }
 * })
 * // Returns { fullId: 'posts/01HX...', localId: '01HX...' }
 * ```
 */
export function resolveEntityId(options: ResolveEntityIdOptions): ResolvedEntityId {
  const { namespace, typeName, schema, data } = options

  // Priority 1: Explicit $id in data
  if (data.$id !== undefined) {
    const providedId = String(data.$id)

    // Validate: empty string is not allowed
    if (providedId === '') {
      throw new ValidationError(
        'entityId',
        typeName,
        'Entity ID cannot be an empty string',
        { fieldName: '$id' }
      )
    }

    // If the ID contains '/', treat as full ID
    if (providedId.includes('/')) {
      return {
        fullId: providedId as EntityId,
        localId: providedId.split('/').slice(1).join('/'),
      }
    }

    return {
      fullId: `${namespace}/${providedId}` as EntityId,
      localId: providedId,
    }
  }

  // Priority 2: $id directive in schema
  const typeDef = schema[typeName] as TypeDefinition | undefined
  const idFieldName = typeDef?.$id

  if (idFieldName && typeof idFieldName === 'string') {
    const fieldValue = data[idFieldName]

    if (fieldValue !== undefined) {
      const localId = String(fieldValue)

      // Validate: empty string is not allowed
      if (localId === '') {
        throw new ValidationError(
          'entityId',
          typeName,
          `$id field '${idFieldName}' cannot be an empty string`,
          { fieldName: idFieldName }
        )
      }

      // Validate: local ID cannot contain slashes (slashes separate namespace from local ID)
      if (localId.includes('/')) {
        throw new ValidationError(
          'entityId',
          typeName,
          `$id field '${idFieldName}' cannot contain '/' character. Local IDs must not contain slashes.`,
          { fieldName: idFieldName }
        )
      }

      return {
        fullId: `${namespace}/${localId}` as EntityId,
        localId,
      }
    }
  }

  // Priority 3: Default to 'id' field if present (implicit $id: 'id')
  if (data.id !== undefined) {
    const localId = String(data.id)

    // Validate: empty string is not allowed
    if (localId === '') {
      throw new ValidationError(
        'entityId',
        typeName,
        'id field cannot be an empty string',
        { fieldName: 'id' }
      )
    }

    // Validate: local ID cannot contain slashes
    if (localId.includes('/')) {
      throw new ValidationError(
        'entityId',
        typeName,
        `id field cannot contain '/' character. Local IDs must not contain slashes.`,
        { fieldName: 'id' }
      )
    }

    return {
      fullId: `${namespace}/${localId}` as EntityId,
      localId,
    }
  }

  // Priority 4: Generate a ULID
  const localId = generateId()
  return {
    fullId: `${namespace}/${localId}` as EntityId,
    localId,
  }
}
