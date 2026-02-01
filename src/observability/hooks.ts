/**
 * Observability Hooks for ParqueDB
 *
 * Provides interfaces and utilities for monitoring, tracing, and metrics collection.
 * Hooks can be registered to track queries, mutations, and storage operations.
 *
 * @module observability/hooks
 */

import type { Filter, FindOptions, UpdateInput, CreateInput } from '../types'

// =============================================================================
// Hook Context Types
// =============================================================================

/**
 * Base context for all hook events
 */
export interface HookContext {
  /** Unique identifier for the operation */
  operationId: string
  /** Timestamp when the operation started */
  startTime: number
  /** Namespace/collection being accessed */
  namespace?: string
  /** Additional metadata */
  metadata?: Record<string, unknown>
}

/**
 * Context for query operations
 */
export interface QueryContext extends HookContext {
  /** Type of query operation */
  operationType: 'find' | 'findOne' | 'get' | 'count' | 'aggregate' | 'explain'
  /** Query filter */
  filter?: Filter
  /** Query options (limit, skip, sort, etc.) */
  options?: FindOptions<unknown>
  /** Aggregation pipeline (for aggregate operations) */
  pipeline?: unknown[]
}

/**
 * Context for mutation operations
 */
export interface MutationContext extends HookContext {
  /** Type of mutation operation */
  operationType: 'create' | 'update' | 'delete' | 'updateMany' | 'deleteMany' | 'link' | 'unlink'
  /** Entity ID(s) being mutated */
  entityId?: string | string[]
  /** Create/update data */
  data?: CreateInput<unknown> | UpdateInput<unknown>
  /** Delete options */
  hard?: boolean
}

/**
 * Context for storage operations
 */
export interface StorageContext extends HookContext {
  /** Type of storage operation */
  operationType: 'read' | 'readRange' | 'write' | 'writeAtomic' | 'append' | 'delete' | 'deletePrefix' | 'list' | 'copy' | 'move'
  /** File path being accessed */
  path: string
  /** Byte range for range reads */
  range?: { start: number; end: number }
}

// =============================================================================
// Hook Result Types
// =============================================================================

/**
 * Result info for query operations
 */
export interface QueryResult {
  /** Number of rows returned */
  rowCount: number
  /** Whether the query used an index */
  indexUsed?: string
  /** Execution time in milliseconds */
  durationMs: number
  /** Whether results were cached */
  cached?: boolean
  /** Number of row groups scanned */
  rowGroupsScanned?: number
  /** Number of row groups skipped */
  rowGroupsSkipped?: number
}

/**
 * Result info for mutation operations
 */
export interface MutationResult {
  /** Number of entities affected */
  affectedCount: number
  /** Generated entity ID(s) for creates */
  generatedIds?: string[]
  /** Execution time in milliseconds */
  durationMs: number
  /** New version number(s) */
  newVersion?: number | number[]
}

/**
 * Result info for storage operations
 */
export interface StorageResult {
  /** Bytes read or written */
  bytesTransferred: number
  /** Execution time in milliseconds */
  durationMs: number
  /** ETag of the file */
  etag?: string
  /** Number of files affected (for list, deletePrefix) */
  fileCount?: number
}

// =============================================================================
// Hook Interfaces
// =============================================================================

/**
 * Hook for observing query operations
 */
export interface QueryHook {
  /**
   * Called before a query starts
   * @param context - Query context with filter and options
   */
  onQueryStart?(context: QueryContext): void | Promise<void>

  /**
   * Called after a query completes successfully
   * @param context - Query context
   * @param result - Query result information
   */
  onQueryEnd?(context: QueryContext, result: QueryResult): void | Promise<void>

  /**
   * Called when a query fails
   * @param context - Query context
   * @param error - The error that occurred
   */
  onQueryError?(context: QueryContext, error: Error): void | Promise<void>
}

/**
 * Hook for observing mutation operations
 */
export interface MutationHook {
  /**
   * Called before a mutation starts
   * @param context - Mutation context with data and options
   */
  onMutationStart?(context: MutationContext): void | Promise<void>

  /**
   * Called after a mutation completes successfully
   * @param context - Mutation context
   * @param result - Mutation result information
   */
  onMutationEnd?(context: MutationContext, result: MutationResult): void | Promise<void>

  /**
   * Called when a mutation fails
   * @param context - Mutation context
   * @param error - The error that occurred
   */
  onMutationError?(context: MutationContext, error: Error): void | Promise<void>
}

/**
 * Hook for observing storage operations
 */
export interface StorageHook {
  /**
   * Called when a read operation occurs
   * @param context - Storage context
   * @param result - Operation result
   */
  onRead?(context: StorageContext, result: StorageResult): void | Promise<void>

  /**
   * Called when a write operation occurs
   * @param context - Storage context
   * @param result - Operation result
   */
  onWrite?(context: StorageContext, result: StorageResult): void | Promise<void>

  /**
   * Called when a delete operation occurs
   * @param context - Storage context
   * @param result - Operation result
   */
  onDelete?(context: StorageContext, result: StorageResult): void | Promise<void>

  /**
   * Called when any storage operation fails
   * @param context - Storage context
   * @param error - The error that occurred
   */
  onStorageError?(context: StorageContext, error: Error): void | Promise<void>
}

/**
 * Combined hook interface for all observability events
 */
export interface ObservabilityHook extends QueryHook, MutationHook, StorageHook {}

// =============================================================================
// Metrics Types
// =============================================================================

/**
 * Metrics for a specific operation type
 */
export interface OperationMetrics {
  /** Total count of operations */
  count: number
  /** Total duration in milliseconds */
  totalDurationMs: number
  /** Minimum duration */
  minDurationMs: number
  /** Maximum duration */
  maxDurationMs: number
  /** Error count */
  errorCount: number
  /** Total bytes transferred (for storage) */
  bytesTransferred?: number
  /** Total rows processed (for queries) */
  rowsProcessed?: number
  /** Last updated timestamp */
  lastUpdated: number
}

/**
 * Aggregated metrics across all operations
 */
export interface AggregatedMetrics {
  /** Query operation metrics */
  queries: {
    find: OperationMetrics
    findOne: OperationMetrics
    get: OperationMetrics
    count: OperationMetrics
    aggregate: OperationMetrics
    explain: OperationMetrics
  }
  /** Mutation operation metrics */
  mutations: {
    create: OperationMetrics
    update: OperationMetrics
    delete: OperationMetrics
    updateMany: OperationMetrics
    deleteMany: OperationMetrics
    link: OperationMetrics
    unlink: OperationMetrics
  }
  /** Storage operation metrics */
  storage: {
    read: OperationMetrics
    readRange: OperationMetrics
    write: OperationMetrics
    writeAtomic: OperationMetrics
    append: OperationMetrics
    delete: OperationMetrics
    deletePrefix: OperationMetrics
    list: OperationMetrics
    copy: OperationMetrics
    move: OperationMetrics
  }
  /** Overall system metrics */
  system: {
    startTime: number
    uptime: number
    totalOperations: number
    totalErrors: number
  }
}

// =============================================================================
// Hook Registry
// =============================================================================

/**
 * Registry for managing observability hooks
 */
export class HookRegistry {
  private queryHooks: QueryHook[] = []
  private mutationHooks: MutationHook[] = []
  private storageHooks: StorageHook[] = []

  /**
   * Register a query hook
   * @param hook - Query hook to register
   * @returns Function to unregister the hook
   */
  registerQueryHook(hook: QueryHook): () => void {
    this.queryHooks.push(hook)
    return () => {
      const index = this.queryHooks.indexOf(hook)
      if (index > -1) {
        this.queryHooks.splice(index, 1)
      }
    }
  }

  /**
   * Register a mutation hook
   * @param hook - Mutation hook to register
   * @returns Function to unregister the hook
   */
  registerMutationHook(hook: MutationHook): () => void {
    this.mutationHooks.push(hook)
    return () => {
      const index = this.mutationHooks.indexOf(hook)
      if (index > -1) {
        this.mutationHooks.splice(index, 1)
      }
    }
  }

  /**
   * Register a storage hook
   * @param hook - Storage hook to register
   * @returns Function to unregister the hook
   */
  registerStorageHook(hook: StorageHook): () => void {
    this.storageHooks.push(hook)
    return () => {
      const index = this.storageHooks.indexOf(hook)
      if (index > -1) {
        this.storageHooks.splice(index, 1)
      }
    }
  }

  /**
   * Register a combined observability hook
   * @param hook - Combined hook to register
   * @returns Function to unregister all hook handlers
   */
  registerHook(hook: ObservabilityHook): () => void {
    const unregisterFns: (() => void)[] = []

    if (hook.onQueryStart || hook.onQueryEnd || hook.onQueryError) {
      unregisterFns.push(this.registerQueryHook(hook))
    }
    if (hook.onMutationStart || hook.onMutationEnd || hook.onMutationError) {
      unregisterFns.push(this.registerMutationHook(hook))
    }
    if (hook.onRead || hook.onWrite || hook.onDelete || hook.onStorageError) {
      unregisterFns.push(this.registerStorageHook(hook))
    }

    return () => {
      unregisterFns.forEach(fn => fn())
    }
  }

  /**
   * Clear all registered hooks
   */
  clearHooks(): void {
    this.queryHooks = []
    this.mutationHooks = []
    this.storageHooks = []
  }

  // =========================================================================
  // Query Hook Dispatchers
  // =========================================================================

  /**
   * Dispatch onQueryStart to all registered hooks
   */
  async dispatchQueryStart(context: QueryContext): Promise<void> {
    for (const hook of this.queryHooks) {
      if (hook.onQueryStart) {
        await hook.onQueryStart(context)
      }
    }
  }

  /**
   * Dispatch onQueryEnd to all registered hooks
   */
  async dispatchQueryEnd(context: QueryContext, result: QueryResult): Promise<void> {
    for (const hook of this.queryHooks) {
      if (hook.onQueryEnd) {
        await hook.onQueryEnd(context, result)
      }
    }
  }

  /**
   * Dispatch onQueryError to all registered hooks
   */
  async dispatchQueryError(context: QueryContext, error: Error): Promise<void> {
    for (const hook of this.queryHooks) {
      if (hook.onQueryError) {
        await hook.onQueryError(context, error)
      }
    }
  }

  // =========================================================================
  // Mutation Hook Dispatchers
  // =========================================================================

  /**
   * Dispatch onMutationStart to all registered hooks
   */
  async dispatchMutationStart(context: MutationContext): Promise<void> {
    for (const hook of this.mutationHooks) {
      if (hook.onMutationStart) {
        await hook.onMutationStart(context)
      }
    }
  }

  /**
   * Dispatch onMutationEnd to all registered hooks
   */
  async dispatchMutationEnd(context: MutationContext, result: MutationResult): Promise<void> {
    for (const hook of this.mutationHooks) {
      if (hook.onMutationEnd) {
        await hook.onMutationEnd(context, result)
      }
    }
  }

  /**
   * Dispatch onMutationError to all registered hooks
   */
  async dispatchMutationError(context: MutationContext, error: Error): Promise<void> {
    for (const hook of this.mutationHooks) {
      if (hook.onMutationError) {
        await hook.onMutationError(context, error)
      }
    }
  }

  // =========================================================================
  // Storage Hook Dispatchers
  // =========================================================================

  /**
   * Dispatch onRead to all registered hooks
   */
  async dispatchStorageRead(context: StorageContext, result: StorageResult): Promise<void> {
    for (const hook of this.storageHooks) {
      if (hook.onRead) {
        await hook.onRead(context, result)
      }
    }
  }

  /**
   * Dispatch onWrite to all registered hooks
   */
  async dispatchStorageWrite(context: StorageContext, result: StorageResult): Promise<void> {
    for (const hook of this.storageHooks) {
      if (hook.onWrite) {
        await hook.onWrite(context, result)
      }
    }
  }

  /**
   * Dispatch onDelete to all registered hooks
   */
  async dispatchStorageDelete(context: StorageContext, result: StorageResult): Promise<void> {
    for (const hook of this.storageHooks) {
      if (hook.onDelete) {
        await hook.onDelete(context, result)
      }
    }
  }

  /**
   * Dispatch onStorageError to all registered hooks
   */
  async dispatchStorageError(context: StorageContext, error: Error): Promise<void> {
    for (const hook of this.storageHooks) {
      if (hook.onStorageError) {
        await hook.onStorageError(context, error)
      }
    }
  }

  // =========================================================================
  // Hook Introspection
  // =========================================================================

  /**
   * Get the number of registered query hooks
   */
  get queryHookCount(): number {
    return this.queryHooks.length
  }

  /**
   * Get the number of registered mutation hooks
   */
  get mutationHookCount(): number {
    return this.mutationHooks.length
  }

  /**
   * Get the number of registered storage hooks
   */
  get storageHookCount(): number {
    return this.storageHooks.length
  }

  /**
   * Check if any hooks are registered
   */
  get hasHooks(): boolean {
    return this.queryHooks.length > 0 ||
           this.mutationHooks.length > 0 ||
           this.storageHooks.length > 0
  }
}

// =============================================================================
// Metrics Collector Hook
// =============================================================================

/**
 * Initialize empty operation metrics
 */
function createEmptyMetrics(): OperationMetrics {
  return {
    count: 0,
    totalDurationMs: 0,
    minDurationMs: Infinity,
    maxDurationMs: 0,
    errorCount: 0,
    bytesTransferred: 0,
    rowsProcessed: 0,
    lastUpdated: Date.now(),
  }
}

/**
 * Built-in hook that collects metrics for all operations
 */
export class MetricsCollector implements ObservabilityHook {
  private startTime: number
  private metrics: AggregatedMetrics

  constructor() {
    this.startTime = Date.now()
    this.metrics = this.createEmptyMetrics()
  }

  private createEmptyMetrics(): AggregatedMetrics {
    return {
      queries: {
        find: createEmptyMetrics(),
        findOne: createEmptyMetrics(),
        get: createEmptyMetrics(),
        count: createEmptyMetrics(),
        aggregate: createEmptyMetrics(),
        explain: createEmptyMetrics(),
      },
      mutations: {
        create: createEmptyMetrics(),
        update: createEmptyMetrics(),
        delete: createEmptyMetrics(),
        updateMany: createEmptyMetrics(),
        deleteMany: createEmptyMetrics(),
        link: createEmptyMetrics(),
        unlink: createEmptyMetrics(),
      },
      storage: {
        read: createEmptyMetrics(),
        readRange: createEmptyMetrics(),
        write: createEmptyMetrics(),
        writeAtomic: createEmptyMetrics(),
        append: createEmptyMetrics(),
        delete: createEmptyMetrics(),
        deletePrefix: createEmptyMetrics(),
        list: createEmptyMetrics(),
        copy: createEmptyMetrics(),
        move: createEmptyMetrics(),
      },
      system: {
        startTime: this.startTime,
        uptime: 0,
        totalOperations: 0,
        totalErrors: 0,
      },
    }
  }

  private updateMetrics(
    metrics: OperationMetrics,
    durationMs: number,
    isError: boolean,
    bytesTransferred?: number,
    rowsProcessed?: number
  ): void {
    metrics.count++
    metrics.totalDurationMs += durationMs
    metrics.minDurationMs = Math.min(metrics.minDurationMs, durationMs)
    metrics.maxDurationMs = Math.max(metrics.maxDurationMs, durationMs)
    metrics.lastUpdated = Date.now()

    if (isError) {
      metrics.errorCount++
    }
    if (bytesTransferred !== undefined) {
      metrics.bytesTransferred = (metrics.bytesTransferred || 0) + bytesTransferred
    }
    if (rowsProcessed !== undefined) {
      metrics.rowsProcessed = (metrics.rowsProcessed || 0) + rowsProcessed
    }
  }

  // Query Hooks
  onQueryEnd(context: QueryContext, result: QueryResult): void {
    const opType = context.operationType
    if (opType in this.metrics.queries) {
      this.updateMetrics(
        this.metrics.queries[opType as keyof typeof this.metrics.queries],
        result.durationMs,
        false,
        undefined,
        result.rowCount
      )
    }
    this.metrics.system.totalOperations++
  }

  onQueryError(context: QueryContext, _error: Error): void {
    const opType = context.operationType
    if (opType in this.metrics.queries) {
      const metrics = this.metrics.queries[opType as keyof typeof this.metrics.queries]
      metrics.count++
      metrics.errorCount++
      metrics.lastUpdated = Date.now()
    }
    this.metrics.system.totalOperations++
    this.metrics.system.totalErrors++
  }

  // Mutation Hooks
  onMutationEnd(context: MutationContext, result: MutationResult): void {
    const opType = context.operationType
    if (opType in this.metrics.mutations) {
      this.updateMetrics(
        this.metrics.mutations[opType as keyof typeof this.metrics.mutations],
        result.durationMs,
        false,
        undefined,
        result.affectedCount
      )
    }
    this.metrics.system.totalOperations++
  }

  onMutationError(context: MutationContext, _error: Error): void {
    const opType = context.operationType
    if (opType in this.metrics.mutations) {
      const metrics = this.metrics.mutations[opType as keyof typeof this.metrics.mutations]
      metrics.count++
      metrics.errorCount++
      metrics.lastUpdated = Date.now()
    }
    this.metrics.system.totalOperations++
    this.metrics.system.totalErrors++
  }

  // Storage Hooks
  onRead(context: StorageContext, result: StorageResult): void {
    const opType = context.operationType
    if (opType in this.metrics.storage) {
      this.updateMetrics(
        this.metrics.storage[opType as keyof typeof this.metrics.storage],
        result.durationMs,
        false,
        result.bytesTransferred
      )
    }
    this.metrics.system.totalOperations++
  }

  onWrite(context: StorageContext, result: StorageResult): void {
    const opType = context.operationType
    if (opType in this.metrics.storage) {
      this.updateMetrics(
        this.metrics.storage[opType as keyof typeof this.metrics.storage],
        result.durationMs,
        false,
        result.bytesTransferred
      )
    }
    this.metrics.system.totalOperations++
  }

  onDelete(context: StorageContext, result: StorageResult): void {
    const opType = context.operationType
    if (opType in this.metrics.storage) {
      this.updateMetrics(
        this.metrics.storage[opType as keyof typeof this.metrics.storage],
        result.durationMs,
        false,
        result.bytesTransferred
      )
    }
    this.metrics.system.totalOperations++
  }

  onStorageError(context: StorageContext, _error: Error): void {
    const opType = context.operationType
    if (opType in this.metrics.storage) {
      const metrics = this.metrics.storage[opType as keyof typeof this.metrics.storage]
      metrics.count++
      metrics.errorCount++
      metrics.lastUpdated = Date.now()
    }
    this.metrics.system.totalOperations++
    this.metrics.system.totalErrors++
  }

  /**
   * Get current aggregated metrics
   */
  getMetrics(): AggregatedMetrics {
    this.metrics.system.uptime = Date.now() - this.startTime
    return JSON.parse(JSON.stringify(this.metrics))
  }

  /**
   * Get average latency for a specific operation type
   */
  getAverageLatency(category: 'queries' | 'mutations' | 'storage', operation: string): number {
    const categoryMetrics = this.metrics[category] as Record<string, OperationMetrics>
    const metrics = categoryMetrics[operation]
    if (!metrics || metrics.count === 0) {
      return 0
    }
    return metrics.totalDurationMs / metrics.count
  }

  /**
   * Get error rate for a specific operation type
   */
  getErrorRate(category: 'queries' | 'mutations' | 'storage', operation: string): number {
    const categoryMetrics = this.metrics[category] as Record<string, OperationMetrics>
    const metrics = categoryMetrics[operation]
    if (!metrics || metrics.count === 0) {
      return 0
    }
    return metrics.errorCount / metrics.count
  }

  /**
   * Reset all metrics
   */
  reset(): void {
    this.startTime = Date.now()
    this.metrics = this.createEmptyMetrics()
  }
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Generate a unique operation ID
 */
export function generateOperationId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 8)
  return `${timestamp}-${random}`
}

/**
 * Create a query context
 */
export function createQueryContext(
  operationType: QueryContext['operationType'],
  namespace?: string,
  filter?: Filter,
  options?: FindOptions<unknown>
): QueryContext {
  return {
    operationId: generateOperationId(),
    startTime: Date.now(),
    namespace,
    operationType,
    filter,
    options,
  }
}

/**
 * Create a mutation context
 */
export function createMutationContext(
  operationType: MutationContext['operationType'],
  namespace?: string,
  entityId?: string | string[],
  data?: CreateInput<unknown> | UpdateInput<unknown>
): MutationContext {
  return {
    operationId: generateOperationId(),
    startTime: Date.now(),
    namespace,
    operationType,
    entityId,
    data,
  }
}

/**
 * Create a storage context
 */
export function createStorageContext(
  operationType: StorageContext['operationType'],
  path: string,
  range?: { start: number; end: number }
): StorageContext {
  return {
    operationId: generateOperationId(),
    startTime: Date.now(),
    operationType,
    path,
    range,
  }
}

// =============================================================================
// Global Registry Instance
// =============================================================================

/**
 * Global hook registry instance
 */
export const globalHookRegistry = new HookRegistry()
