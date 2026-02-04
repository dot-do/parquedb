/**
 * Schema Parser
 *
 * Re-exports parsing functions from types/schema.ts and adds validation functions.
 */

import type {
  Schema,
  TypeDefinition,
  FieldDef,
  FieldDefinition,
  SchemaIndexType,
  ParsedSchema,
  ParsedType,
  ParsedField,
  ParsedRelationship,
  ValidationResult,
  ValidationError,
} from '../types/schema'

import {
  parseFieldType as _parseFieldType,
  parseRelation as _parseRelation,
  isRelationString as _isRelationString,
} from '../types/schema'
import { DEFAULT_SCHEMA_SAMPLE_SIZE, DEFAULT_SCHEMA_MAX_DEPTH } from '../constants'
import { isNullish } from '../utils/comparison'

// Re-export parsing helpers from types/schema
export { parseFieldType, parseRelation, isRelationString } from '../types/schema'

// =============================================================================
// Constants
// =============================================================================

/** Valid primitive types */
const PRIMITIVE_TYPES = new Set([
  'string',
  'text',
  'markdown',
  'number',
  'int',
  'float',
  'double',
  'boolean',
  'date',
  'datetime',
  'timestamp',
  'uuid',
  'email',
  'url',
  'json',
  'binary',
])

/** Valid parametric type prefixes */
const PARAMETRIC_TYPES = ['decimal', 'varchar', 'char', 'vector', 'enum']

/** Valid index types */
const VALID_INDEX_TYPES: (SchemaIndexType | undefined)[] = [true, false, 'unique', 'fts', 'vector', 'hash', undefined]

/** Reserved metadata field prefixes (only known $-fields are allowed) */
const KNOWN_META_FIELDS = new Set([
  '$type',
  '$ns',
  '$shred',
  '$description',
  '$abstract',
  '$extends',
  '$indexes',
  '$visibility',
])

// =============================================================================
// Schema Validation
// =============================================================================

/**
 * Validation options
 */
export interface ValidationOptions {
  /** Strict mode - fail on unknown fields */
  strict?: boolean | undefined
  /** Check relationship targets exist */
  checkRelationships?: boolean | undefined
}

/**
 * Validate a schema definition
 *
 * @param schema - The schema to validate
 * @param options - Validation options
 * @returns Validation result with any errors
 */
export function validateSchema(schema: Schema, options?: ValidationOptions): ValidationResult {
  const errors: ValidationError[] = []

  // Check for empty schema
  if (!schema || typeof schema !== 'object' || Object.keys(schema).length === 0) {
    errors.push({
      path: '',
      message: 'Schema must contain at least one type definition',
      code: 'EMPTY_SCHEMA',
    })
    return { valid: false, errors }
  }

  // Validate each type
  for (const [typeName, typeDef] of Object.entries(schema)) {
    // Validate type name (must start with uppercase letter and contain only alphanumeric/underscore)
    if (!/^[A-Z][A-Za-z0-9_]*$/.test(typeName)) {
      errors.push({
        path: typeName,
        message: `Type name '${typeName}' must start with uppercase letter and contain only alphanumeric characters or underscores`,
        code: 'INVALID_TYPE_NAME',
      })
    }

    // Validate type definition
    const typeResult = validateTypeDefinition(typeName, typeDef)
    if (!typeResult.valid) {
      errors.push(...typeResult.errors)
    }
  }

  // Optionally check relationship targets
  if (options?.checkRelationships !== false) {
    const relResult = validateRelationshipTargets(schema)
    if (!relResult.valid) {
      errors.push(...relResult.errors)
    }
  }

  return { valid: errors.length === 0, errors }
}

/**
 * Validate a type definition
 *
 * @param typeName - Name of the type
 * @param typeDef - The type definition to validate
 * @returns Validation result with any errors
 */
export function validateTypeDefinition(typeName: string, typeDef: TypeDefinition): ValidationResult {
  const errors: ValidationError[] = []

  // Check for null/non-object
  if (!typeDef || typeof typeDef !== 'object') {
    errors.push({
      path: typeName,
      message: 'Type definition must be an object',
      code: 'INVALID_TYPE_DEFINITION',
    })
    return { valid: false, errors }
  }

  // Count non-meta fields
  let fieldCount = 0

  for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
    // Handle meta fields
    if (fieldName.startsWith('$')) {
      // Check if it's a known meta field
      if (!KNOWN_META_FIELDS.has(fieldName)) {
        errors.push({
          path: `${typeName}.${fieldName}`,
          message: `Unknown metadata field '${fieldName}'. Fields starting with '$' are reserved`,
          code: 'RESERVED_FIELD_NAME',
        })
      }
      continue
    }

    fieldCount++

    // Validate field
    if (typeof fieldDef === 'string') {
      // String field definition
      if (_isRelationString(fieldDef)) {
        // Validate relation string
        if (!isValidRelationString(fieldDef)) {
          errors.push({
            path: `${typeName}.${fieldName}`,
            message: `Invalid relation string: '${fieldDef}'`,
            code: 'INVALID_RELATION',
          })
        }
      } else {
        // Validate field type
        if (!isValidFieldType(fieldDef)) {
          errors.push({
            path: `${typeName}.${fieldName}`,
            message: `Invalid field type: '${fieldDef}'`,
            code: 'INVALID_FIELD_TYPE',
          })
        }
      }
    } else if (fieldDef && typeof fieldDef === 'object' && !Array.isArray(fieldDef)) {
      // Object field definition
      const def = fieldDef as FieldDefinition

      // Check type field
      if (!def.type) {
        errors.push({
          path: `${typeName}.${fieldName}`,
          message: 'Field definition object must have a "type" property',
          code: 'MISSING_TYPE',
        })
      } else if (!isValidFieldType(def.type)) {
        errors.push({
          path: `${typeName}.${fieldName}`,
          message: `Invalid field type: '${def.type}'`,
          code: 'INVALID_FIELD_TYPE',
        })
      }

      // Check index type
      if (def.index !== undefined && !VALID_INDEX_TYPES.includes(def.index)) {
        errors.push({
          path: `${typeName}.${fieldName}.index`,
          message: `Invalid index type: '${def.index}'. Must be boolean, 'unique', 'fts', 'vector', or 'hash'`,
          code: 'INVALID_INDEX_TYPE',
        })
      }
    }
  }

  // Check for empty type (must have at least one field)
  if (fieldCount === 0) {
    errors.push({
      path: typeName,
      message: `Type '${typeName}' must have at least one field`,
      code: 'EMPTY_TYPE',
    })
  }

  return { valid: errors.length === 0, errors }
}

/**
 * Validate that all relationship targets in a schema exist
 *
 * @param schema - The schema to validate
 * @returns Validation result with any errors
 */
export function validateRelationshipTargets(schema: Schema): ValidationResult {
  const errors: ValidationError[] = []
  const typeNames = new Set(Object.keys(schema))

  // Map to track relationships for matching
  // Key: "TargetType.fieldName", Value: { sourceType, sourceField, isBackward }
  const relationshipMap = new Map<string, { sourceType: string; sourceField: string; direction: 'forward' | 'backward'; mode: 'exact' | 'fuzzy' }[]>()

  for (const [typeName, typeDef] of Object.entries(schema)) {
    for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
      if (fieldName.startsWith('$')) continue
      if (typeof fieldDef !== 'string') continue
      if (!_isRelationString(fieldDef)) continue

      const rel = _parseRelation(fieldDef)
      if (!rel) continue

      if (rel.direction === 'forward') {
        // Forward relation: -> Target.reverse
        const targetType = rel.toType!

        // Check target type exists
        if (!typeNames.has(targetType)) {
          errors.push({
            path: `${typeName}.${fieldName}`,
            message: `Relationship target type '${targetType}' does not exist`,
            code: 'MISSING_TARGET_TYPE',
          })
          continue
        }

        // For exact mode, check that reverse field exists on target
        if (rel.mode === 'exact' && rel.reverse) {
          const targetDef = schema[targetType]
          if (!targetDef || !targetDef[rel.reverse]) {
            errors.push({
              path: `${typeName}.${fieldName}`,
              message: `Reverse field '${rel.reverse}' does not exist on type '${targetType}'`,
              code: 'MISSING_REVERSE_FIELD',
            })
            continue
          }

          // Track this relationship for matching
          const key = `${targetType}.${rel.reverse}`
          const existing = relationshipMap.get(key) || []
          existing.push({ sourceType: typeName, sourceField: fieldName, direction: 'forward', mode: rel.mode })
          relationshipMap.set(key, existing)
        }
      } else {
        // Backward relation: <- Source.field
        const sourceType = rel.fromType!

        // Check source type exists
        if (!typeNames.has(sourceType)) {
          errors.push({
            path: `${typeName}.${fieldName}`,
            message: `Relationship source type '${sourceType}' does not exist`,
            code: 'MISSING_SOURCE_TYPE',
          })
          continue
        }

        // For exact mode, check that source field exists on source type
        if (rel.mode === 'exact' && rel.fromField) {
          const sourceDef = schema[sourceType]
          if (!sourceDef || !sourceDef[rel.fromField]) {
            errors.push({
              path: `${typeName}.${fieldName}`,
              message: `Source field '${rel.fromField}' does not exist on type '${sourceType}'`,
              code: 'MISSING_SOURCE_FIELD',
            })
            continue
          }

          // Track this relationship for matching
          const key = `${typeName}.${fieldName}`
          const existing = relationshipMap.get(key) || []
          existing.push({ sourceType, sourceField: rel.fromField, direction: 'backward', mode: rel.mode })
          relationshipMap.set(key, existing)
        }
      }
    }
  }

  // Verify relationship pairs match
  for (const [typeName, typeDef] of Object.entries(schema)) {
    for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
      if (fieldName.startsWith('$')) continue
      if (typeof fieldDef !== 'string') continue
      if (!_isRelationString(fieldDef)) continue

      const rel = _parseRelation(fieldDef)
      if (!rel || rel.mode !== 'exact') continue

      if (rel.direction === 'forward' && rel.reverse) {
        // Check that the reverse field points back correctly
        const targetType = rel.toType!
        const targetDef = schema[targetType]

        // Skip if target type doesn't exist (already caught earlier)
        if (!targetDef) continue

        const reverseFieldDef = targetDef[rel.reverse]

        if (typeof reverseFieldDef === 'string' && _isRelationString(reverseFieldDef)) {
          const reverseRel = _parseRelation(reverseFieldDef)
          if (reverseRel) {
            // The reverse should be either:
            // 1. A backward relation pointing to this type and field: <- ThisType.thisField
            // 2. A forward relation pointing back: -> ThisType.fieldName
            let matches = false

            if (reverseRel.direction === 'backward' && reverseRel.fromType === typeName && reverseRel.fromField === fieldName) {
              matches = true
            } else if (reverseRel.direction === 'forward' && reverseRel.toType === typeName && reverseRel.reverse === fieldName) {
              matches = true
            }

            if (!matches) {
              errors.push({
                path: `${typeName}.${fieldName}`,
                message: `Relationship mismatch: '${typeName}.${fieldName}' points to '${targetType}.${rel.reverse}' but the reverse does not point back correctly`,
                code: 'RELATIONSHIP_MISMATCH',
              })
            }
          }
        }
      }
    }
  }

  return { valid: errors.length === 0, errors }
}

/**
 * Parse a complete schema into a ParsedSchema object
 *
 * @param schema - The schema to parse
 * @returns The parsed schema
 */
export function parseSchema(schema: Schema): ParsedSchema {
  const types = new Map<string, ParsedType>()
  const relationships: ParsedRelationship[] = []

  for (const [typeName, typeDef] of Object.entries(schema)) {
    const parsedType = parseType(typeName, typeDef)
    types.set(typeName, parsedType)

    // Collect relationships
    for (const [fieldName, field] of parsedType.fields) {
      if (field.isRelation && field.targetType) {
        relationships.push({
          fromType: typeName,
          fromField: fieldName,
          predicate: fieldName,
          toType: field.targetType,
          reverse: field.reverseName || fieldName,
          isArray: field.isArray,
          direction: field.relationDirection!,
          mode: field.relationMode!,
        })
      }
    }
  }

  return {
    types,
    getType: (name) => types.get(name),
    getRelationships: () => relationships,
    validate: (typeName, data) => {
      const type = types.get(typeName)
      if (!type) {
        return {
          valid: false,
          errors: [{ path: '', message: `Unknown type: ${typeName}`, code: 'UNKNOWN_TYPE' }],
        }
      }
      return validateEntity(type, data)
    },
  }
}

/**
 * Parse a single type definition
 */
function parseType(name: string, typeDef: TypeDefinition): ParsedType {
  const fields = new Map<string, ParsedField>()
  const indexes = typeDef.$indexes || []
  const shredFields = typeDef.$shred || []

  for (const [fieldName, fieldDef] of Object.entries(typeDef)) {
    if (fieldName.startsWith('$')) continue

    const parsedField = parseField(fieldName, fieldDef as FieldDef)
    fields.set(fieldName, parsedField)
  }

  return {
    name,
    typeUri: typeDef.$type,
    namespace: typeDef.$ns,
    shredFields,
    fields,
    indexes,
    isAbstract: typeDef.$abstract || false,
    extends: typeDef.$extends,
  }
}

/**
 * Parse a single field definition
 */
function parseField(name: string, def: FieldDef): ParsedField {
  if (typeof def === 'string') {
    // Check if it's a relationship
    if (_isRelationString(def)) {
      const rel = _parseRelation(def)!

      // Determine target type and reverse name based on direction
      let targetType: string
      let reverseName: string

      if (rel.direction === 'forward') {
        targetType = rel.toType!
        reverseName = rel.reverse!
      } else {
        targetType = rel.fromType!
        reverseName = rel.fromField!
      }

      return {
        name,
        type: 'relation',
        required: false,
        isArray: rel.isArray,
        isRelation: true,
        relationDirection: rel.direction,
        relationMode: rel.mode,
        targetType,
        reverseName,
      }
    }

    // Regular field type string
    const { type, required, isArray, default: defaultValue } = _parseFieldType(def)
    return {
      name,
      type,
      required,
      isArray,
      default: defaultValue ? parseDefaultValue(defaultValue) : undefined,
      isRelation: false,
    }
  }

  // Object field definition
  const fieldObj = def as FieldDefinition
  const { type, required, isArray } = _parseFieldType(fieldObj.type)

  return {
    name,
    type,
    required: fieldObj.required ?? required,
    isArray,
    default: fieldObj.default,
    index: fieldObj.index,
    isRelation: false,
  }
}

/**
 * Parse a default value string into its actual value
 */
function parseDefaultValue(value: string): unknown {
  // Try to parse as JSON first
  try {
    return JSON.parse(value)
  } catch {
    // Intentionally ignored: value is not JSON, return as-is (for unquoted strings like enum values)
    return value
  }
}

/**
 * Validate an entity against its type definition
 */
function validateEntity(type: ParsedType, data: unknown): ValidationResult {
  const errors: ValidationError[] = []

  if (!data || typeof data !== 'object') {
    errors.push({ path: '', message: 'Data must be an object', code: 'INVALID_TYPE' })
    return { valid: false, errors }
  }

  const obj = data as Record<string, unknown>

  for (const [fieldName, field] of type.fields) {
    const value = obj[fieldName]

    // Check required fields
    if (field.required && isNullish(value)) {
      errors.push({
        path: fieldName,
        message: `Field '${fieldName}' is required`,
        code: 'REQUIRED',
      })
      continue
    }

    // Skip validation if value is undefined and not required
    if (value === undefined) continue

    // Type validation
    const typeError = validateFieldType(fieldName, value, field)
    if (typeError) errors.push(typeError)
  }

  return { valid: errors.length === 0, errors }
}

/**
 * Validate a field value against its type
 */
function validateFieldType(fieldName: string, value: unknown, field: ParsedField): ValidationError | null {
  // Handle null values
  if (value === null) {
    if (field.required) {
      return { path: fieldName, message: `Field '${fieldName}' cannot be null`, code: 'NULL_VALUE' }
    }
    return null
  }

  // Handle arrays
  if (field.isArray) {
    if (!Array.isArray(value)) {
      return { path: fieldName, message: `Field '${fieldName}' must be an array`, code: 'EXPECTED_ARRAY' }
    }
    // Validate each element
    for (let i = 0; i < value.length; i++) {
      const error = validateScalarType(`${fieldName}[${i}]`, value[i], field.type)
      if (error) return error
    }
    return null
  }

  // Handle relations
  if (field.isRelation) {
    // Relations can be strings (IDs) or objects
    if (typeof value !== 'string' && typeof value !== 'object') {
      return { path: fieldName, message: `Field '${fieldName}' must be a string ID or object`, code: 'INVALID_RELATION_VALUE' }
    }
    return null
  }

  // Scalar validation
  return validateScalarType(fieldName, value, field.type)
}

/**
 * Validate a scalar value against a type
 */
function validateScalarType(path: string, value: unknown, type: string): ValidationError | null {
  switch (type) {
    case 'string':
    case 'text':
    case 'markdown':
      if (typeof value !== 'string') {
        return { path, message: `Expected string, got ${typeof value}`, code: 'TYPE_MISMATCH' }
      }
      break

    case 'number':
    case 'int':
    case 'float':
    case 'double':
      if (typeof value !== 'number') {
        return { path, message: `Expected number, got ${typeof value}`, code: 'TYPE_MISMATCH' }
      }
      if (type === 'int' && !Number.isInteger(value)) {
        return { path, message: `Expected integer, got float`, code: 'TYPE_MISMATCH' }
      }
      break

    case 'boolean':
      if (typeof value !== 'boolean') {
        return { path, message: `Expected boolean, got ${typeof value}`, code: 'TYPE_MISMATCH' }
      }
      break

    case 'date':
    case 'datetime':
    case 'timestamp':
      if (typeof value !== 'string' && !(value instanceof Date)) {
        return { path, message: `Expected date string or Date object, got ${typeof value}`, code: 'TYPE_MISMATCH' }
      }
      break

    case 'uuid':
      if (typeof value !== 'string') {
        return { path, message: `Expected UUID string, got ${typeof value}`, code: 'TYPE_MISMATCH' }
      }
      // Basic UUID format check
      if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) {
        return { path, message: `Invalid UUID format`, code: 'INVALID_FORMAT' }
      }
      break

    case 'email':
      if (typeof value !== 'string') {
        return { path, message: `Expected email string, got ${typeof value}`, code: 'TYPE_MISMATCH' }
      }
      // Basic email format check
      if (!/.+@.+\..+/.test(value)) {
        return { path, message: `Invalid email format`, code: 'INVALID_FORMAT' }
      }
      break

    case 'url':
      if (typeof value !== 'string') {
        return { path, message: `Expected URL string, got ${typeof value}`, code: 'TYPE_MISMATCH' }
      }
      break

    case 'json':
      // JSON can be any value
      break

    case 'binary':
      if (!(value instanceof Uint8Array) && !(value instanceof ArrayBuffer) && typeof value !== 'string') {
        return { path, message: `Expected binary data (Uint8Array, ArrayBuffer, or base64 string), got ${typeof value}`, code: 'TYPE_MISMATCH' }
      }
      break

    default:
      // Handle parametric types
      if (type.startsWith('decimal(')) {
        if (typeof value !== 'number' && typeof value !== 'string') {
          return { path, message: `Expected decimal number, got ${typeof value}`, code: 'TYPE_MISMATCH' }
        }
      } else if (type.startsWith('varchar(') || type.startsWith('char(')) {
        if (typeof value !== 'string') {
          return { path, message: `Expected string, got ${typeof value}`, code: 'TYPE_MISMATCH' }
        }
      } else if (type.startsWith('vector(')) {
        if (!Array.isArray(value)) {
          return { path, message: `Expected vector array, got ${typeof value}`, code: 'TYPE_MISMATCH' }
        }
      } else if (type.startsWith('enum(')) {
        if (typeof value !== 'string') {
          return { path, message: `Expected enum string, got ${typeof value}`, code: 'TYPE_MISMATCH' }
        }
        // Extract enum values and check
        const enumMatch = type.match(/^enum\((.+)\)$/)
        if (enumMatch && enumMatch[1]) {
          const allowedValues = enumMatch[1].split(',').map(v => v.trim())
          if (!allowedValues.includes(value)) {
            return { path, message: `Value '${value}' not in enum (${allowedValues.join(', ')})`, code: 'INVALID_ENUM' }
          }
        }
      }
  }

  return null
}

/**
 * Check if a field type string is valid
 *
 * @param value - The field type string to validate
 * @returns true if valid, false otherwise
 */
export function isValidFieldType(value: string): boolean {
  if (!value || typeof value !== 'string') return false

  // Reject relation strings
  if (_isRelationString(value)) return false

  // Trim the value
  let type = value.trim()

  // Remove default value if present
  const defaultMatch = type.match(/^(.+?)\s*=\s*(.+)$/)
  if (defaultMatch && defaultMatch[1]) {
    type = defaultMatch[1].trim()
  }

  // Track modifiers for validation
  let hasRequired = false
  let hasOptional = false

  // Check for array modifiers: []! or []
  if (type.endsWith('[]!')) {
    type = type.slice(0, -3)
    hasRequired = true
  } else if (type.endsWith('[]')) {
    type = type.slice(0, -2)
  }

  // Check for index modifiers (must be before required/optional check)
  // Order matters: check longer patterns first
  if (type.endsWith('#fts!') || type.endsWith('#fts')) {
    type = type.replace(/#fts!?$/, '')
    if (value.includes('#fts!')) hasRequired = true
  } else if (type.endsWith('#vec!') || type.endsWith('#vec')) {
    type = type.replace(/#vec!?$/, '')
    if (value.includes('#vec!')) hasRequired = true
  } else if (type.endsWith('#hash!') || type.endsWith('#hash')) {
    type = type.replace(/#hash!?$/, '')
    if (value.includes('#hash!')) hasRequired = true
  } else if (type.endsWith('##!') || type.endsWith('##')) {
    type = type.replace(/##!?$/, '')
    if (value.includes('##!')) hasRequired = true
  } else if (type.endsWith('#!') || type.endsWith('#')) {
    type = type.replace(/#!?$/, '')
    if (value.includes('#!')) hasRequired = true
  }

  // Check for required: !
  if (type.endsWith('!')) {
    if (hasRequired) {
      // Already had []!, invalid double modifier
      return false
    }
    type = type.slice(0, -1)
    hasRequired = true
  }

  // Check for optional: ?
  if (type.endsWith('?')) {
    type = type.slice(0, -1)
    hasOptional = true
  }

  // Invalid modifier combinations
  if (hasRequired && hasOptional) return false

  // Check remaining for another ! or ?
  if (type.endsWith('!') || type.endsWith('?')) return false

  // Check if it's a primitive type
  if (PRIMITIVE_TYPES.has(type)) return true

  // Check if it's a valid parametric type
  for (const prefix of PARAMETRIC_TYPES) {
    if (type.startsWith(`${prefix}(`)) {
      // Validate parametric syntax
      const match = type.match(new RegExp(`^${prefix}\\((.+)\\)$`))
      if (!match || !match[1]) return false

      const params = match[1]

      switch (prefix) {
        case 'decimal':
          // decimal(precision, scale) - needs two numbers
          if (!/^\d+,\s*\d+$/.test(params)) return false
          break
        case 'varchar':
        case 'char':
        case 'vector':
          // Single number parameter
          if (!/^\d+$/.test(params)) return false
          break
        case 'enum':
          // At least one value
          if (params.trim().length === 0) return false
          break
      }

      return true
    }
  }

  return false
}

/**
 * Check if a relation string is valid
 *
 * @param value - The relation string to validate
 * @returns true if valid, false otherwise
 */
export function isValidRelationString(value: string): boolean {
  if (!value || typeof value !== 'string') return false
  if (!_isRelationString(value)) return false

  // Forward: -> Type.field or -> Type.field[]
  if (/^->\s*\w+\.\w+(\[\])?$/.test(value)) return true

  // Backward: <- Type.field or <- Type.field[]
  if (/^<-\s*\w+\.\w+(\[\])?$/.test(value)) return true

  // Fuzzy forward: ~> Type or ~> Type.field or ~> Type[] or ~> Type.field[]
  if (/^~>\s*\w+(\.\w+)?(\[\])?$/.test(value)) return true

  // Fuzzy backward: <~ Type or <~ Type.field or <~ Type[] or <~ Type.field[]
  if (/^<~\s*\w+(\.\w+)?(\[\])?$/.test(value)) return true

  return false
}

// =============================================================================
// Schema Inference
// =============================================================================

/**
 * Inferred type information for a field
 */
export interface InferredField {
  type: string
  required: boolean
  isArray: boolean
  nested?: InferredSchema | undefined
}

/**
 * Inferred schema from documents
 */
export interface InferredSchema {
  [fieldName: string]: InferredField
}

/**
 * Options for schema inference
 */
export interface InferSchemaOptions {
  /** Number of documents to sample (default: 100) */
  sampleSize?: number | undefined
  /** Fields to mark as required if present in all samples */
  detectRequired?: boolean | undefined
  /** Infer nested object schemas */
  inferNested?: boolean | undefined
  /** Maximum depth for nested schema inference (default: 5) */
  maxDepth?: number | undefined
}

/**
 * Infer a schema from a collection of documents
 *
 * @param documents - Array of documents to analyze
 * @param options - Inference options
 * @returns Inferred schema
 *
 * @example
 * const docs = [
 *   { name: 'Alice', age: 30, email: 'alice@example.com' },
 *   { name: 'Bob', age: 25 },
 * ]
 * const schema = inferSchema(docs)
 * // => {
 * //   name: { type: 'string', required: true, isArray: false },
 * //   age: { type: 'number', required: true, isArray: false },
 * //   email: { type: 'string', required: false, isArray: false },
 * // }
 */
export function inferSchema(
  documents: Record<string, unknown>[],
  options: InferSchemaOptions = {}
): InferredSchema {
  const {
    sampleSize = DEFAULT_SCHEMA_SAMPLE_SIZE,
    detectRequired = true,
    inferNested = true,
    maxDepth = DEFAULT_SCHEMA_MAX_DEPTH,
  } = options

  if (!documents || documents.length === 0) {
    return {}
  }

  // Sample documents
  const sampled = documents.slice(0, sampleSize)
  const totalDocs = sampled.length

  // Track field occurrences and types
  const fieldStats = new Map<string, {
    count: number
    types: Set<string>
    isArray: boolean
    nestedValues: Record<string, unknown>[]
  }>()

  // Analyze each document
  for (const doc of sampled) {
    if (!doc || typeof doc !== 'object') continue

    for (const [key, value] of Object.entries(doc)) {
      // Skip metadata fields
      if (key.startsWith('$')) continue

      const stats = fieldStats.get(key) || {
        count: 0,
        types: new Set(),
        isArray: false,
        nestedValues: [],
      }

      stats.count++

      if (isNullish(value)) {
        stats.types.add('null')
      } else if (Array.isArray(value)) {
        stats.isArray = true
        // Infer type from array elements
        if (value.length > 0) {
          const elementType = inferValueType(value[0])
          stats.types.add(elementType)
          if (elementType === 'object' && inferNested) {
            stats.nestedValues.push(...(value.filter(v => v && typeof v === 'object') as Record<string, unknown>[]))
          }
        } else {
          stats.types.add('json') // Unknown array element type
        }
      } else {
        const valueType = inferValueType(value)
        stats.types.add(valueType)
        if (valueType === 'object' && inferNested) {
          stats.nestedValues.push(value as Record<string, unknown>)
        }
      }

      fieldStats.set(key, stats)
    }
  }

  // Build inferred schema
  const schema: InferredSchema = {}

  for (const [fieldName, stats] of fieldStats) {
    // Determine the primary type
    const types = Array.from(stats.types).filter(t => t !== 'null')
    let primaryType: string = types.length > 0 && types[0] ? types[0] : 'json'

    // If multiple types, use json
    if (types.length > 1) {
      primaryType = 'json'
    }

    const field: InferredField = {
      type: primaryType,
      required: detectRequired ? stats.count === totalDocs && !stats.types.has('null') : false,
      isArray: stats.isArray,
    }

    // Recursively infer nested schema
    if (primaryType === 'object' && inferNested && maxDepth > 0 && stats.nestedValues.length > 0) {
      field.nested = inferSchema(stats.nestedValues, {
        ...options,
        maxDepth: maxDepth - 1,
      })
    }

    schema[fieldName] = field
  }

  return schema
}

/**
 * Infer the type of a single value
 */
function inferValueType(value: unknown): string {
  if (isNullish(value)) return 'null'
  if (typeof value === 'string') {
    // Check for specific string formats
    if (/^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?/.test(value)) return 'datetime'
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) return 'uuid'
    if (/.+@.+\..+/.test(value)) return 'email'
    if (/^https?:\/\//.test(value)) return 'url'
    return 'string'
  }
  if (typeof value === 'number') {
    return Number.isInteger(value) ? 'int' : 'number'
  }
  if (typeof value === 'boolean') return 'boolean'
  if (value instanceof Date) return 'datetime'
  if (Array.isArray(value)) return 'array'
  if (typeof value === 'object') return 'object'
  return 'json'
}

/**
 * Convert an inferred schema to a TypeDefinition
 *
 * @param name - Type name
 * @param schema - Inferred schema
 * @returns TypeDefinition that can be used in a Schema
 */
export function inferredToTypeDefinition(
  _name: string,
  schema: InferredSchema
): TypeDefinition {
  const typeDef: TypeDefinition = {}

  for (const [fieldName, field] of Object.entries(schema)) {
    let typeStr = field.type

    // Handle nested objects
    if (field.type === 'object' && field.nested) {
      // For nested objects, we use 'json' type as ParqueDB stores them in Variant
      typeStr = 'json'
    }

    // Add array modifier
    if (field.isArray) {
      typeStr += '[]'
    }

    // Add required modifier
    if (field.required) {
      typeStr += '!'
    }

    typeDef[fieldName] = typeStr
  }

  return typeDef
}

/**
 * Infer a complete Schema from multiple document collections
 *
 * @param collections - Map of collection name to documents
 * @param options - Inference options
 * @returns Complete schema definition
 *
 * @example
 * const schema = inferSchemaFromCollections({
 *   User: [{ name: 'Alice', email: 'alice@example.com' }],
 *   Post: [{ title: 'Hello', content: 'World', views: 100 }],
 * })
 */
export function inferSchemaFromCollections(
  collections: Record<string, Record<string, unknown>[]>,
  options: InferSchemaOptions = {}
): Schema {
  const schema: Schema = {}

  for (const [collectionName, documents] of Object.entries(collections)) {
    // Convert collection name to type name (capitalize first letter)
    const typeName = collectionName.charAt(0).toUpperCase() + collectionName.slice(1)
    const inferredSchema = inferSchema(documents, options)
    schema[typeName] = inferredToTypeDefinition(typeName, inferredSchema)
  }

  return schema
}

// =============================================================================
// Nested Schema Support
// =============================================================================

/**
 * Nested field definition for object types
 */
export interface NestedFieldDefinition extends FieldDefinition {
  /** Nested schema for object types */
  properties?: Record<string, FieldDef> | undefined
}

/**
 * Parse a nested field definition that may contain object properties
 *
 * @param name - Field name
 * @param def - Field definition (may include nested properties)
 * @returns Parsed field with optional nested schema
 */
export function parseNestedField(name: string, def: FieldDef | NestedFieldDefinition): ParsedField & { properties?: Map<string, ParsedField> | undefined } {
  // First parse as regular field
  const baseParsed = parseFieldFromDef(name, def)

  // Check if this is a nested object definition
  if (typeof def === 'object' && def !== null && !Array.isArray(def)) {
    const nestedDef = def as NestedFieldDefinition
    if (nestedDef.properties && typeof nestedDef.properties === 'object') {
      const properties = new Map<string, ParsedField>()
      for (const [propName, propDef] of Object.entries(nestedDef.properties)) {
        properties.set(propName, parseFieldFromDef(propName, propDef as FieldDef))
      }
      return { ...baseParsed, properties }
    }
  }

  return baseParsed
}

/**
 * Internal helper to parse a field from its definition
 */
function parseFieldFromDef(name: string, def: FieldDef): ParsedField {
  if (typeof def === 'string') {
    // Check if it's a relationship
    if (_isRelationString(def)) {
      const rel = _parseRelation(def)!

      let targetType: string
      let reverseName: string

      if (rel.direction === 'forward') {
        targetType = rel.toType!
        reverseName = rel.reverse!
      } else {
        targetType = rel.fromType!
        reverseName = rel.fromField!
      }

      return {
        name,
        type: 'relation',
        required: false,
        isArray: rel.isArray,
        isRelation: true,
        relationDirection: rel.direction,
        relationMode: rel.mode,
        targetType,
        reverseName,
      }
    }

    // Regular field type string
    const { type, required, isArray, default: defaultValue } = _parseFieldType(def)
    return {
      name,
      type,
      required,
      isArray,
      default: defaultValue ? parseDefaultValueFromString(defaultValue) : undefined,
      isRelation: false,
    }
  }

  // Object field definition
  const fieldObj = def as FieldDefinition
  const { type, required, isArray } = _parseFieldType(fieldObj.type)

  return {
    name,
    type,
    required: fieldObj.required ?? required,
    isArray,
    default: fieldObj.default,
    index: fieldObj.index,
    isRelation: false,
  }
}

/**
 * Parse a default value string into its actual value
 */
function parseDefaultValueFromString(value: string): unknown {
  try {
    return JSON.parse(value)
  } catch {
    // Intentionally ignored: value is not JSON, return as raw string
    return value
  }
}

// =============================================================================
// Entity Validation with Required Fields
// =============================================================================

/**
 * Validate that an entity has the required core fields ($id, $type, name)
 *
 * @param entity - The entity to validate
 * @returns Validation result
 */
export function validateEntityCoreFields(entity: unknown): ValidationResult {
  const errors: ValidationError[] = []

  if (!entity || typeof entity !== 'object') {
    errors.push({
      path: '',
      message: 'Entity must be an object',
      code: 'INVALID_TYPE',
    })
    return { valid: false, errors }
  }

  const obj = entity as Record<string, unknown>

  // Check for $id
  if (!('$id' in obj) || isNullish(obj.$id)) {
    errors.push({
      path: '$id',
      message: 'Entity must have a $id field',
      code: 'MISSING_REQUIRED_FIELD',
    })
  } else if (typeof obj.$id !== 'string') {
    errors.push({
      path: '$id',
      message: '$id must be a string',
      code: 'INVALID_TYPE',
    })
  }

  // Check for $type
  if (!('$type' in obj) || isNullish(obj.$type)) {
    errors.push({
      path: '$type',
      message: 'Entity must have a $type field',
      code: 'MISSING_REQUIRED_FIELD',
    })
  } else if (typeof obj.$type !== 'string') {
    errors.push({
      path: '$type',
      message: '$type must be a string',
      code: 'INVALID_TYPE',
    })
  }

  // Check for name
  if (!('name' in obj) || isNullish(obj.name)) {
    errors.push({
      path: 'name',
      message: 'Entity must have a name field',
      code: 'MISSING_REQUIRED_FIELD',
    })
  } else if (typeof obj.name !== 'string') {
    errors.push({
      path: 'name',
      message: 'name must be a string',
      code: 'INVALID_TYPE',
    })
  }

  return { valid: errors.length === 0, errors }
}

/**
 * Validate an entity against its schema type, including core field validation
 *
 * @param parsedSchema - The parsed schema
 * @param typeName - The type name to validate against
 * @param entity - The entity to validate
 * @param options - Validation options
 * @returns Validation result
 */
export function validateEntityFull(
  parsedSchema: ParsedSchema,
  typeName: string,
  entity: unknown,
  options?: { validateCoreFields?: boolean | undefined }
): ValidationResult {
  const allErrors: ValidationError[] = []

  // Validate core fields if requested
  if (options?.validateCoreFields !== false) {
    const coreResult = validateEntityCoreFields(entity)
    if (!coreResult.valid) {
      allErrors.push(...coreResult.errors)
    }
  }

  // Validate against type schema
  const typeResult = parsedSchema.validate(typeName, entity)
  if (!typeResult.valid) {
    allErrors.push(...typeResult.errors)
  }

  return { valid: allErrors.length === 0, errors: allErrors }
}
