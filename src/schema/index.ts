/**
 * Schema Module
 *
 * Exports schema parsing, validation, and utility functions.
 */

export * from './parser'

// Re-export key types for convenience
export type {
  InferredField,
  InferredSchema,
  InferSchemaOptions,
  NestedFieldDefinition,
  ValidationOptions,
} from './parser'
