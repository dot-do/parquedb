/**
 * JSON Validation Utilities
 *
 * Provides type-safe JSON parsing with runtime validation.
 * Use these helpers when parsing untrusted JSON data from storage,
 * HTTP requests, or external sources.
 *
 * @module utils/json-validation
 */

import { Result, Ok, Err, tryCatch } from '../types/result'

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if a value is a plain object (not null, array, or other types)
 */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/**
 * Check if a value is an array
 */
export function isArray(value: unknown): value is unknown[] {
  return Array.isArray(value)
}

/**
 * Check if a value is a string
 */
export function isString(value: unknown): value is string {
  return typeof value === 'string'
}

/**
 * Check if a value is a number (including finite check)
 */
export function isNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

/**
 * Check if a value is a boolean
 */
export function isBoolean(value: unknown): value is boolean {
  return typeof value === 'boolean'
}

// =============================================================================
// Validation Errors
// =============================================================================

/**
 * Error thrown when JSON parsing fails
 */
export class JsonParseError extends Error {
  public readonly input: string
  public override readonly cause?: Error

  constructor(
    message: string,
    input: string,
    cause?: Error
  ) {
    super(message)
    this.name = 'JsonParseError'
    this.input = input
    this.cause = cause
  }
}

/**
 * Error thrown when parsed JSON doesn't match expected type
 */
export class JsonValidationError extends Error {
  constructor(
    message: string,
    public readonly expectedType: string,
    public readonly actualValue: unknown
  ) {
    super(message)
    this.name = 'JsonValidationError'
  }
}

// =============================================================================
// Safe Parsing Functions
// =============================================================================

/**
 * Safely parse JSON and return a Result
 *
 * @param json - JSON string to parse
 * @returns Result containing parsed value or error
 *
 * @example
 * ```typescript
 * const result = safeJsonParse('{"key": "value"}')
 * if (result.ok) {
 *   console.log(result.value) // { key: 'value' }
 * }
 * ```
 */
export function safeJsonParse(json: string): Result<unknown, JsonParseError> {
  const result = tryCatch(() => JSON.parse(json))
  if (result.ok) {
    return result
  }
  return Err(new JsonParseError(
    `Failed to parse JSON: ${result.error.message}`,
    json.length > 100 ? json.slice(0, 100) + '...' : json,
    result.error
  ))
}

/**
 * Parse JSON and validate it's a record (object)
 *
 * @param json - JSON string to parse
 * @returns Result containing parsed record or error
 *
 * @example
 * ```typescript
 * const result = parseJsonRecord('{"name": "test"}')
 * if (result.ok) {
 *   const name = result.value.name // typed as unknown
 * }
 * ```
 */
export function parseJsonRecord(json: string): Result<Record<string, unknown>, JsonParseError | JsonValidationError> {
  const parseResult = safeJsonParse(json)
  if (!parseResult.ok) {
    return parseResult
  }

  if (!isRecord(parseResult.value)) {
    return Err(new JsonValidationError(
      `Expected object, got ${typeof parseResult.value}`,
      'object',
      parseResult.value
    ))
  }

  return Ok(parseResult.value)
}

/**
 * Parse JSON and validate it's an array
 *
 * @param json - JSON string to parse
 * @returns Result containing parsed array or error
 */
export function parseJsonArray(json: string): Result<unknown[], JsonParseError | JsonValidationError> {
  const parseResult = safeJsonParse(json)
  if (!parseResult.ok) {
    return parseResult
  }

  if (!isArray(parseResult.value)) {
    return Err(new JsonValidationError(
      `Expected array, got ${typeof parseResult.value}`,
      'array',
      parseResult.value
    ))
  }

  return Ok(parseResult.value)
}

// =============================================================================
// Unsafe Parsing with Validation (throws on error)
// =============================================================================

/**
 * Parse JSON expecting a record, throws on invalid input
 *
 * Use this when you need to throw an exception rather than handle a Result.
 * Prefer parseJsonRecord for safer error handling.
 *
 * @param json - JSON string to parse
 * @param context - Optional context for error messages
 * @returns Parsed record
 * @throws JsonParseError if JSON is invalid
 * @throws JsonValidationError if parsed value is not an object
 */
export function parseRecordOrThrow(json: string, context?: string): Record<string, unknown> {
  const result = parseJsonRecord(json)
  if (result.ok) {
    return result.value
  }
  const prefix = context ? `${context}: ` : ''
  throw new Error(`${prefix}${result.error.message}`)
}

/**
 * Parse JSON expecting an array, throws on invalid input
 *
 * @param json - JSON string to parse
 * @param context - Optional context for error messages
 * @returns Parsed array
 * @throws JsonParseError if JSON is invalid
 * @throws JsonValidationError if parsed value is not an array
 */
export function parseArrayOrThrow(json: string, context?: string): unknown[] {
  const result = parseJsonArray(json)
  if (result.ok) {
    return result.value
  }
  const prefix = context ? `${context}: ` : ''
  throw new Error(`${prefix}${result.error.message}`)
}

// =============================================================================
// Schema Validation
// =============================================================================

/**
 * Schema definition for validating parsed JSON
 */
export type JsonSchema = {
  type: 'object'
  properties?: Record<string, { type: 'string' | 'number' | 'boolean' | 'array' | 'object'; required?: boolean | undefined }> | undefined
  additionalProperties?: boolean | undefined
} | {
  type: 'array'
  items?: { type: 'string' | 'number' | 'boolean' | 'object' } | undefined
} | {
  type: 'string' | 'number' | 'boolean'
}

/**
 * Validate a value against a simple schema
 *
 * @param value - Value to validate
 * @param schema - Schema to validate against
 * @returns true if valid, false otherwise
 */
export function validateSchema(value: unknown, schema: JsonSchema): boolean {
  switch (schema.type) {
    case 'string':
      return isString(value)
    case 'number':
      return isNumber(value)
    case 'boolean':
      return isBoolean(value)
    case 'array':
      if (!isArray(value)) return false
      if (schema.items) {
        return value.every(item => validateSchema(item, schema.items!))
      }
      return true
    case 'object':
      if (!isRecord(value)) return false
      if (schema.properties) {
        for (const [key, propSchema] of Object.entries(schema.properties)) {
          if (propSchema.required && !(key in value)) {
            return false
          }
          if (key in value && !validateSchema(value[key], propSchema)) {
            return false
          }
        }
      }
      return true
    default:
      return false
  }
}

/**
 * Parse and validate JSON against a schema
 *
 * @param json - JSON string to parse
 * @param schema - Schema to validate against
 * @returns Result containing validated value or error
 */
export function parseWithSchema<T>(
  json: string,
  schema: JsonSchema
): Result<T, JsonParseError | JsonValidationError> {
  const parseResult = safeJsonParse(json)
  if (!parseResult.ok) {
    return parseResult
  }

  if (!validateSchema(parseResult.value, schema)) {
    return Err(new JsonValidationError(
      `Value does not match schema`,
      schema.type,
      parseResult.value
    ))
  }

  return Ok(parseResult.value as T)
}

// =============================================================================
// Convenience Functions for Common Patterns
// =============================================================================

/**
 * Safely parse stored entity data (expected to be an object)
 *
 * Returns empty object if parsing fails, with optional error logging.
 *
 * @param json - JSON string from storage
 * @param onError - Optional callback for error handling
 * @returns Parsed object or empty object on failure
 */
export function parseStoredData(
  json: string,
  onError?: (error: JsonParseError | JsonValidationError) => void
): Record<string, unknown> {
  const result = parseJsonRecord(json)
  if (result.ok) {
    return result.value
  }
  onError?.(result.error)
  return {}
}

/**
 * Safely parse stored array data
 *
 * Returns empty array if parsing fails, with optional error logging.
 *
 * @param json - JSON string from storage
 * @param onError - Optional callback for error handling
 * @returns Parsed array or empty array on failure
 */
export function parseStoredArray(
  json: string,
  onError?: (error: JsonParseError | JsonValidationError) => void
): unknown[] {
  const result = parseJsonArray(json)
  if (result.ok) {
    return result.value
  }
  onError?.(result.error)
  return []
}

/**
 * Attempt to parse JSON, returning undefined on failure
 *
 * Useful for optional JSON fields that may not be valid.
 */
export function tryParseJson<T = unknown>(json: string): T | undefined {
  const result = safeJsonParse(json)
  return result.ok ? (result.value as T) : undefined
}

// =============================================================================
// Response Validation Functions
// =============================================================================

/**
 * Validate that a response contains required fields
 *
 * @param data - Response data to validate
 * @param fields - Required field names
 * @param context - Context name for error messages
 * @throws Error if any required field is missing
 */
export function validateResponseFields(
  data: unknown,
  fields: string[],
  context: string
): void {
  if (!isRecord(data)) {
    throw new Error(`${context}: Expected object response, got ${typeof data}`)
  }

  for (const field of fields) {
    if (!(field in data) || data[field] === undefined || data[field] === null) {
      throw new Error(`${context}: Missing required field '${field}'`)
    }
  }
}

/**
 * Validate that a response is an array
 *
 * @param data - Response data to validate
 * @param context - Context name for error messages
 * @returns The validated array
 * @throws Error if data is not an array
 */
export function validateResponseArray(
  data: unknown,
  context: string
): unknown[] {
  if (!isArray(data)) {
    throw new Error(`${context}: Expected array response, got ${typeof data}`)
  }
  return data
}

/**
 * Parse JSON with a type guard validation
 *
 * @param json - JSON string to parse
 * @param guard - Type guard function to validate the parsed value
 * @param typeName - Name of expected type for error messages
 * @returns Result containing typed value or error
 *
 * @example
 * ```typescript
 * interface Config { name: string; count: number }
 * const isConfig = (v: unknown): v is Config =>
 *   isRecord(v) && isString(v.name) && isNumber(v.count)
 *
 * const result = parseWithGuard(json, isConfig, 'Config')
 * ```
 */
export function parseWithGuard<T>(
  json: string,
  guard: (value: unknown) => value is T,
  typeName: string
): Result<T, JsonParseError | JsonValidationError> {
  const parseResult = safeJsonParse(json)
  if (!parseResult.ok) {
    return parseResult
  }

  if (!guard(parseResult.value)) {
    return Err(new JsonValidationError(
      `Value does not match expected type ${typeName}`,
      typeName,
      parseResult.value
    ))
  }

  return Ok(parseResult.value)
}
