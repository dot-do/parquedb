/**
 * Schema Validator
 *
 * Runtime schema validation for ParqueDB entities.
 * Provides enhanced validation with helpful error messages.
 */

import type {
  Schema,
  ParsedSchema,
  ValidationResult,
  ValidationError,
  ParsedField,
} from '../types/schema'

import {
  parseFieldType as _parseFieldType,
  isRelationString as _isRelationString,
  parseRelation as _parseRelation,
} from '../types/schema'

import {
  parseSchema,
} from './parser'

import { logger } from '../utils/logger'

// =============================================================================
// Validation Error Classes
// =============================================================================

/**
 * Error thrown when schema validation fails
 */
export class SchemaValidationError extends Error {
  override name = 'SchemaValidationError'

  /** Validation errors that occurred */
  errors: ValidationError[]

  /** The type being validated */
  typeName: string

  constructor(typeName: string, errors: ValidationError[]) {
    const summary = formatValidationErrors(typeName, errors)
    super(summary)
    this.typeName = typeName
    this.errors = errors
  }

  /**
   * Get a user-friendly summary of the validation errors
   */
  getSummary(): string {
    return formatValidationErrors(this.typeName, this.errors)
  }

  /**
   * Get detailed error information for each field
   */
  getFieldErrors(): Map<string, string[]> {
    const fieldErrors = new Map<string, string[]>()
    for (const error of this.errors) {
      const path = error.path || '_root'
      if (!fieldErrors.has(path)) {
        fieldErrors.set(path, [])
      }
      fieldErrors.get(path)!.push(error.message)
    }
    return fieldErrors
  }
}

/**
 * Format validation errors into a user-friendly message
 */
function formatValidationErrors(typeName: string, errors: ValidationError[]): string {
  if (errors.length === 0) {
    return `Validation passed for type ${typeName}`
  }

  if (errors.length === 1) {
    const error = errors[0]!
    return `Validation failed for ${typeName}: ${error.message}` +
      (error.path ? ` (at ${error.path})` : '')
  }

  const lines = [`Validation failed for ${typeName} with ${errors.length} errors:`]
  for (const error of errors.slice(0, 5)) {
    const location = error.path ? `[${error.path}]` : ''
    lines.push(`  - ${location} ${error.message}`)
  }
  if (errors.length > 5) {
    lines.push(`  ... and ${errors.length - 5} more errors`)
  }

  return lines.join('\n')
}

// =============================================================================
// Validation Mode
// =============================================================================

/**
 * Validation mode determines how strict the validation is
 */
export type ValidationMode = 'strict' | 'permissive' | 'warn'

/**
 * Options for schema validation
 */
export interface SchemaValidatorOptions {
  /**
   * Validation mode:
   * - 'strict': Throws on any validation error (default)
   * - 'permissive': Only validates required fields and types
   * - 'warn': Logs warnings instead of throwing errors
   */
  mode?: ValidationMode | undefined

  /**
   * Allow unknown fields not defined in schema
   * Default: true (permissive by default for document flexibility)
   */
  allowUnknownFields?: boolean | undefined

  /**
   * Validate relationship references exist
   * Default: false (expensive operation)
   */
  validateRelationships?: boolean | undefined

  /**
   * Custom type validators
   */
  customValidators?: Map<string, (value: unknown) => boolean> | undefined
}

// =============================================================================
// Schema Validator
// =============================================================================

/**
 * Schema validator for runtime entity validation
 */
export class SchemaValidator {
  private schema: Schema
  private parsedSchema: ParsedSchema
  private options: Required<SchemaValidatorOptions>

  constructor(schema: Schema, options: SchemaValidatorOptions = {}) {
    this.schema = schema
    this.parsedSchema = parseSchema(schema)
    this.options = {
      mode: options.mode ?? 'strict',
      allowUnknownFields: options.allowUnknownFields ?? true,
      validateRelationships: options.validateRelationships ?? false,
      customValidators: options.customValidators ?? new Map(),
    }
  }

  /**
   * Validate an entity against its schema type
   *
   * @param typeName - The type name to validate against
   * @param data - The data to validate
   * @param skipCoreFields - Skip $id, $type, name validation (for create input)
   * @returns Validation result
   * @throws SchemaValidationError if mode is 'strict' and validation fails
   */
  validate(
    typeName: string,
    data: unknown,
    _skipCoreFields = false
  ): ValidationResult {
    const errors: ValidationError[] = []

    // Check data is an object
    if (!data || typeof data !== 'object' || Array.isArray(data)) {
      errors.push({
        path: '',
        message: 'Data must be a non-null object',
        code: 'INVALID_DATA_TYPE',
      })
      return this.handleResult(typeName, errors)
    }

    const obj = data as Record<string, unknown>

    // Check type exists in schema
    const typeDef = this.parsedSchema.getType(typeName)
    if (!typeDef) {
      // No schema for this type - validation passes (permissive by design)
      return { valid: true, errors: [] }
    }

    // Validate required fields
    for (const [fieldName, field] of typeDef.fields) {
      const value = obj[fieldName]

      // Check required fields
      if (field.required && !this.hasDefault(field)) {
        if (value === undefined || value === null) {
          errors.push({
            path: fieldName,
            message: this.formatMissingFieldMessage(fieldName, field),
            code: 'REQUIRED',
          })
          continue
        }
      }

      // Skip validation if value is undefined and field is optional
      if (value === undefined) continue

      // Validate field type
      const typeErrors = this.validateFieldValue(fieldName, value, field)
      errors.push(...typeErrors)
    }

    // Check for unknown fields if not allowed
    if (!this.options.allowUnknownFields) {
      for (const key of Object.keys(obj)) {
        if (key.startsWith('$')) continue // Skip system fields
        if (!typeDef.fields.has(key)) {
          errors.push({
            path: key,
            message: `Unknown field '${key}' not defined in schema for type ${typeName}`,
            code: 'UNKNOWN_FIELD',
          })
        }
      }
    }

    return this.handleResult(typeName, errors)
  }

  /**
   * Validate a single field value
   */
  private validateFieldValue(
    fieldName: string,
    value: unknown,
    field: ParsedField
  ): ValidationError[] {
    const errors: ValidationError[] = []

    // Handle null values
    if (value === null) {
      if (field.required) {
        errors.push({
          path: fieldName,
          message: `Field '${fieldName}' cannot be null`,
          code: 'NULL_VALUE',
        })
      }
      return errors
    }

    // Handle relationship fields
    if (field.isRelation) {
      return this.validateRelationshipValue(fieldName, value, field)
    }

    // Handle arrays
    if (field.isArray) {
      if (!Array.isArray(value)) {
        errors.push({
          path: fieldName,
          message: `Field '${fieldName}' must be an array, got ${typeof value}`,
          code: 'EXPECTED_ARRAY',
        })
        return errors
      }
      // Validate each element
      for (let i = 0; i < value.length; i++) {
        const elemErrors = this.validateScalarValue(`${fieldName}[${i}]`, value[i], field.type)
        errors.push(...elemErrors)
      }
      return errors
    }

    // Validate scalar value
    return this.validateScalarValue(fieldName, value, field.type)
  }

  /**
   * Validate a scalar (non-array) value against a type
   */
  private validateScalarValue(
    path: string,
    value: unknown,
    type: string
  ): ValidationError[] {
    const errors: ValidationError[] = []

    // Check custom validators first
    if (this.options.customValidators?.has(type)) {
      const validator = this.options.customValidators.get(type)!
      if (!validator(value)) {
        errors.push({
          path,
          message: `Value does not satisfy custom validator for type '${type}'`,
          code: 'CUSTOM_VALIDATION',
        })
      }
      return errors
    }

    const actualType = typeof value

    switch (type) {
      case 'string':
      case 'text':
      case 'markdown':
        if (actualType !== 'string') {
          errors.push({
            path,
            message: `Expected string, got ${actualType}`,
            code: 'TYPE_MISMATCH',
          })
        }
        break

      case 'number':
      case 'float':
      case 'double':
        if (actualType !== 'number') {
          errors.push({
            path,
            message: `Expected number, got ${actualType}`,
            code: 'TYPE_MISMATCH',
          })
        }
        break

      case 'int':
        if (actualType !== 'number') {
          errors.push({
            path,
            message: `Expected integer, got ${actualType}`,
            code: 'TYPE_MISMATCH',
          })
        } else if (!Number.isInteger(value)) {
          errors.push({
            path,
            message: `Expected integer, got floating point number`,
            code: 'TYPE_MISMATCH',
          })
        }
        break

      case 'boolean':
        if (actualType !== 'boolean') {
          errors.push({
            path,
            message: `Expected boolean, got ${actualType}`,
            code: 'TYPE_MISMATCH',
          })
        }
        break

      case 'date':
      case 'datetime':
      case 'timestamp':
        if (!(value instanceof Date) && actualType !== 'string') {
          errors.push({
            path,
            message: `Expected date (Date object or ISO string), got ${actualType}`,
            code: 'TYPE_MISMATCH',
          })
        } else if (actualType === 'string') {
          const parsed = Date.parse(value as string)
          if (isNaN(parsed)) {
            errors.push({
              path,
              message: `Invalid date format: '${value}'`,
              code: 'INVALID_FORMAT',
            })
          }
        }
        break

      case 'uuid':
        if (actualType !== 'string') {
          errors.push({
            path,
            message: `Expected UUID string, got ${actualType}`,
            code: 'TYPE_MISMATCH',
          })
        } else if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value as string)) {
          errors.push({
            path,
            message: `Invalid UUID format: '${value}'`,
            code: 'INVALID_FORMAT',
          })
        }
        break

      case 'email':
        if (actualType !== 'string') {
          errors.push({
            path,
            message: `Expected email string, got ${actualType}`,
            code: 'TYPE_MISMATCH',
          })
        } else if (!/.+@.+\..+/.test(value as string)) {
          errors.push({
            path,
            message: `Invalid email format: '${value}'`,
            code: 'INVALID_FORMAT',
          })
        }
        break

      case 'url':
        if (actualType !== 'string') {
          errors.push({
            path,
            message: `Expected URL string, got ${actualType}`,
            code: 'TYPE_MISMATCH',
          })
        } else {
          try {
            new URL(value as string)
          } catch {
            // Intentionally ignored: URL constructor throws for invalid URLs, which is the validation check
            errors.push({
              path,
              message: `Invalid URL format: '${value}'`,
              code: 'INVALID_FORMAT',
            })
          }
        }
        break

      case 'json':
        // JSON can be any value, no validation needed
        break

      case 'binary':
        if (!(value instanceof Uint8Array) && !(value instanceof ArrayBuffer) && actualType !== 'string') {
          errors.push({
            path,
            message: `Expected binary data (Uint8Array, ArrayBuffer, or base64 string), got ${actualType}`,
            code: 'TYPE_MISMATCH',
          })
        }
        break

      default:
        // Handle parametric types
        if (type.startsWith('decimal(')) {
          if (actualType !== 'number' && actualType !== 'string') {
            errors.push({
              path,
              message: `Expected decimal number, got ${actualType}`,
              code: 'TYPE_MISMATCH',
            })
          }
        } else if (type.startsWith('varchar(') || type.startsWith('char(')) {
          if (actualType !== 'string') {
            errors.push({
              path,
              message: `Expected string, got ${actualType}`,
              code: 'TYPE_MISMATCH',
            })
          } else {
            // Check length constraint
            const match = type.match(/\((\d+)\)/)
            if (match && match[1]) {
              const maxLen = parseInt(match[1], 10)
              if ((value as string).length > maxLen) {
                errors.push({
                  path,
                  message: `String length ${(value as string).length} exceeds maximum ${maxLen}`,
                  code: 'MAX_LENGTH',
                })
              }
            }
          }
        } else if (type.startsWith('vector(')) {
          if (!Array.isArray(value)) {
            errors.push({
              path,
              message: `Expected vector array, got ${actualType}`,
              code: 'TYPE_MISMATCH',
            })
          } else {
            const match = type.match(/\((\d+)\)/)
            if (match && match[1]) {
              const expectedDim = parseInt(match[1], 10)
              if (value.length !== expectedDim) {
                errors.push({
                  path,
                  message: `Vector dimension mismatch: expected ${expectedDim}, got ${value.length}`,
                  code: 'DIMENSION_MISMATCH',
                })
              }
            }
            // Check all elements are numbers
            for (let i = 0; i < value.length; i++) {
              if (typeof value[i] !== 'number') {
                errors.push({
                  path: `${path}[${i}]`,
                  message: `Vector element must be a number, got ${typeof value[i]}`,
                  code: 'TYPE_MISMATCH',
                })
                break // Only report first invalid element
              }
            }
          }
        } else if (type.startsWith('enum(')) {
          if (actualType !== 'string') {
            errors.push({
              path,
              message: `Expected enum string, got ${actualType}`,
              code: 'TYPE_MISMATCH',
            })
          } else {
            const match = type.match(/^enum\((.+)\)$/)
            if (match && match[1]) {
              const allowedValues = match[1].split(',').map(v => v.trim())
              if (!allowedValues.includes(value as string)) {
                errors.push({
                  path,
                  message: `Invalid enum value '${value}'. Allowed values: ${allowedValues.join(', ')}`,
                  code: 'INVALID_ENUM',
                })
              }
            }
          }
        }
        // Unknown types pass validation (for extensibility)
    }

    return errors
  }

  /**
   * Validate relationship field value
   */
  private validateRelationshipValue(
    fieldName: string,
    value: unknown,
    field: ParsedField
  ): ValidationError[] {
    const errors: ValidationError[] = []

    // Relationships can be:
    // 1. String ID: "users/alice"
    // 2. Object with display names: { "Alice": "users/alice" }
    // 3. Array of either (for array relationships)

    if (field.isArray) {
      if (!Array.isArray(value)) {
        // Could be a single relation object for a to-many relationship
        if (typeof value === 'object' && value !== null) {
          return this.validateRelationshipObject(fieldName, value as Record<string, unknown>, field)
        }
        errors.push({
          path: fieldName,
          message: `Relationship '${fieldName}' should be an array or relation object`,
          code: 'EXPECTED_ARRAY',
        })
        return errors
      }
      // Validate each element
      for (let i = 0; i < value.length; i++) {
        const elemErrors = this.validateSingleRelation(`${fieldName}[${i}]`, value[i], field)
        errors.push(...elemErrors)
      }
    } else {
      // Single relationship
      return this.validateSingleRelation(fieldName, value, field)
    }

    return errors
  }

  /**
   * Validate a single relationship reference
   */
  private validateSingleRelation(
    path: string,
    value: unknown,
    field: ParsedField
  ): ValidationError[] {
    const errors: ValidationError[] = []

    if (typeof value === 'string') {
      // Direct ID reference
      if (!value.includes('/')) {
        errors.push({
          path,
          message: `Invalid relationship ID format '${value}'. Expected format: namespace/id`,
          code: 'INVALID_RELATION_FORMAT',
        })
      }
    } else if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      return this.validateRelationshipObject(path, value as Record<string, unknown>, field)
    } else {
      errors.push({
        path,
        message: `Relationship must be a string ID or relation object, got ${typeof value}`,
        code: 'INVALID_RELATION_VALUE',
      })
    }

    return errors
  }

  /**
   * Validate a relationship object (display name -> ID mapping)
   */
  private validateRelationshipObject(
    path: string,
    value: Record<string, unknown>,
    _field: ParsedField
  ): ValidationError[] {
    const errors: ValidationError[] = []

    for (const [displayName, ref] of Object.entries(value)) {
      // Skip special fields like $count, $next
      if (displayName.startsWith('$')) continue

      if (typeof ref !== 'string') {
        errors.push({
          path: `${path}.${displayName}`,
          message: `Relationship reference must be a string ID, got ${typeof ref}`,
          code: 'INVALID_RELATION_VALUE',
        })
      } else if (!ref.includes('/')) {
        errors.push({
          path: `${path}.${displayName}`,
          message: `Invalid relationship ID format '${ref}'. Expected format: namespace/id`,
          code: 'INVALID_RELATION_FORMAT',
        })
      }
    }

    return errors
  }

  /**
   * Check if a field has a default value
   */
  private hasDefault(field: ParsedField): boolean {
    return field.default !== undefined
  }

  /**
   * Format a helpful message for missing required fields
   */
  private formatMissingFieldMessage(fieldName: string, field: ParsedField): string {
    let msg = `Missing required field '${fieldName}'`

    if (field.type) {
      msg += ` (expected ${field.type}`
      if (field.isArray) msg += '[]'
      msg += ')'
    }

    return msg
  }

  /**
   * Handle validation result based on mode
   */
  private handleResult(typeName: string, errors: ValidationError[]): ValidationResult {
    if (errors.length === 0) {
      return { valid: true, errors: [] }
    }

    switch (this.options.mode) {
      case 'strict':
        throw new SchemaValidationError(typeName, errors)

      case 'warn':
        logger.warn(formatValidationErrors(typeName, errors))
        return { valid: false, errors }

      case 'permissive':
      default:
        return { valid: false, errors }
    }
  }

  /**
   * Get the parsed schema for advanced use cases
   */
  getParsedSchema(): ParsedSchema {
    return this.parsedSchema
  }

  /**
   * Get the raw schema
   */
  getSchema(): Schema {
    return this.schema
  }

  /**
   * Check if a type is defined in the schema
   */
  hasType(typeName: string): boolean {
    return this.parsedSchema.getType(typeName) !== undefined
  }

  /**
   * Get field definitions for a type
   */
  getFields(typeName: string): Map<string, ParsedField> | undefined {
    return this.parsedSchema.getType(typeName)?.fields
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a validator for a schema
 */
export function createValidator(
  schema: Schema,
  options?: SchemaValidatorOptions
): SchemaValidator {
  return new SchemaValidator(schema, options)
}

/**
 * Validate data against a schema (convenience function)
 *
 * @param schema - Schema definition
 * @param typeName - Type name to validate against
 * @param data - Data to validate
 * @param options - Validation options
 * @returns Validation result
 */
export function validate(
  schema: Schema,
  typeName: string,
  data: unknown,
  options?: SchemaValidatorOptions
): ValidationResult {
  const validator = new SchemaValidator(schema, options)
  return validator.validate(typeName, data)
}
