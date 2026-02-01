/**
 * Schema Module
 *
 * Exports schema parsing, validation, and utility functions.
 */

export * from './parser'
export * from './validator'

// Re-export key types for convenience
export type {
  InferredField,
  InferredSchema,
  InferSchemaOptions,
  NestedFieldDefinition,
  ValidationOptions,
} from './parser'

export type {
  ValidationMode,
  SchemaValidatorOptions,
} from './validator'

export {
  SchemaValidationError,
  SchemaValidator,
  createValidator,
  validate,
} from './validator'
