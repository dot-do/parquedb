/**
 * Mutation Layer for ParqueDB
 *
 * This module provides a centralized mutation layer following the Command pattern
 * for create, update, and delete operations. It includes:
 *
 * - MutationExecutor: Main class for executing mutations
 * - Operators: MongoDB-style update operators ($set, $inc, $push, etc.)
 * - Create/Update/Delete: Individual operation modules
 * - Types: Type definitions for the mutation system
 *
 * @example
 * ```typescript
 * import { MutationExecutor } from 'parquedb/mutation'
 *
 * const executor = new MutationExecutor({
 *   schema: mySchema,
 *   defaultActor: 'users/admin' as EntityId,
 * })
 *
 * // Create
 * const entity = await executor.create('posts', {
 *   $type: 'Post',
 *   name: 'My Post',
 *   title: 'Hello World',
 * }, entityStore)
 *
 * // Update
 * await executor.update('posts', entity.$id, {
 *   $set: { title: 'Updated Title' },
 *   $inc: { viewCount: 1 },
 * }, entityStore)
 *
 * // Delete
 * await executor.delete('posts', entity.$id, entityStore)
 * ```
 */

// Main executor
export {
  MutationExecutor,
  VersionConflictError,
  type MutationExecutorConfig,
  type EntityStore,
} from './executor'

// Types
export {
  MutationContext,
  CreateResult,
  UpdateResult,
  DeleteResult,
  BulkMutationResult,
  MutationEvent,
  MutationError,
  MutationErrorCode,
  MutationErrorCodes,
  MutationOperationError,
  MutationHooks,
  PreMutationHandler,
  PostMutationHandler,
  ApplyOperatorsOptions,
  ApplyOperatorsResult,
  RelationshipOperation,
  createMutationContext,
} from './types'

// Create operation
export {
  executeCreate,
  validateCreateInput,
  applySchemaDefaults,
  validateNamespace,
  normalizeNamespace,
  type CreateOperationOptions,
  type SchemaValidatorInterface,
} from './create'

// Update operation
export {
  executeUpdate,
  type UpdateOperationOptions,
} from './update'

// Delete operation
export {
  executeDelete,
  applySoftDelete,
  applyRestore,
  executeBulkDelete,
  type DeleteOperationOptions,
  type BulkDeleteResult,
} from './delete'

// Operators
export {
  applyOperators,
  getField,
  setField,
  unsetField,
  validateUpdateOperators,
} from './operators'
