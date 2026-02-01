/**
 * Observability Module for ParqueDB
 *
 * Re-exports all observability types, hooks, and utilities.
 *
 * @module observability
 */

export {
  // Context types
  type HookContext,
  type QueryContext,
  type MutationContext,
  type StorageContext,

  // Result types
  type QueryResult,
  type MutationResult,
  type StorageResult,

  // Hook interfaces
  type QueryHook,
  type MutationHook,
  type StorageHook,
  type ObservabilityHook,

  // Metrics types
  type OperationMetrics,
  type AggregatedMetrics,

  // Classes
  HookRegistry,
  MetricsCollector,

  // Utility functions
  generateOperationId,
  createQueryContext,
  createMutationContext,
  createStorageContext,

  // Global instance
  globalHookRegistry,
} from './hooks'
