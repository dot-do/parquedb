/**
 * Mutation Executor Module for ParqueDB
 *
 * Provides a centralized class for executing mutations with:
 * - Input validation
 * - Schema validation
 * - Operator application
 * - Event recording
 * - Transaction support
 * - Hook execution
 */

import type {
  Entity,
  EntityId,
  EntityData,
  CreateInput,
  UpdateInput,
  Filter,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
  Schema,
} from '../types'
import {
  MutationContext,
  MutationEvent,
  MutationHooks,
  createMutationContext,
  type PreMutationHandler,
  type PostMutationHandler,
} from './types'
import { executeCreate, validateNamespace, SchemaValidatorInterface } from './create'
import { executeUpdate, VersionConflictError } from './update'
import { executeDelete, applySoftDelete, applyRestore } from './delete'
import { validateUpdateOperators } from './operators'
import { generateId } from '../utils'
import { entityTarget } from '../types/entity'
import {
  globalHookRegistry,
  createMutationContext as createObservabilityMutationContext,
} from '../observability'

// =============================================================================
// Mutation Executor
// =============================================================================

/**
 * Configuration for MutationExecutor
 */
export interface MutationExecutorConfig {
  /** Schema for validation */
  schema?: Schema | undefined

  /** Schema validator instance */
  schemaValidator?: SchemaValidatorInterface | null | undefined

  /** Hooks for mutation lifecycle */
  hooks?: MutationHooks | undefined

  /** Default actor for operations */
  defaultActor?: EntityId | undefined

  /** Function to record events */
  recordEvent?: ((event: MutationEvent) => Promise<void>) | undefined
}

/**
 * Entity store interface for the executor
 */
export interface EntityStore {
  get(id: string): Entity | undefined
  set(id: string, entity: Entity): void
  delete(id: string): boolean
  has(id: string): boolean
  forEach(callback: (entity: Entity, id: string) => void): void
}

/**
 * MutationExecutor class for executing database mutations
 *
 * This class provides a centralized way to execute create, update, and delete
 * operations with proper validation, event recording, and hook execution.
 */
export class MutationExecutor {
  private schema: Schema
  private schemaValidator: SchemaValidatorInterface | null
  private hooks: MutationHooks
  private defaultActor: EntityId
  private recordEventFn?: ((event: MutationEvent) => Promise<void>) | undefined

  constructor(config: MutationExecutorConfig = {}) {
    this.schema = config.schema || {}
    this.schemaValidator = config.schemaValidator || null
    this.hooks = config.hooks || {}
    this.defaultActor = config.defaultActor || ('system/anonymous' as EntityId)
    this.recordEventFn = config.recordEvent
  }

  /**
   * Update the schema
   */
  setSchema(schema: Schema): void {
    this.schema = { ...this.schema, ...schema }
  }

  /**
   * Update the schema validator
   */
  setSchemaValidator(validator: SchemaValidatorInterface | null): void {
    this.schemaValidator = validator
  }

  /**
   * Register hooks
   */
  registerHooks(hooks: MutationHooks): void {
    this.hooks = {
      preMutation: [...(this.hooks.preMutation || []), ...(hooks.preMutation || [])],
      postMutation: [...(this.hooks.postMutation || []), ...(hooks.postMutation || [])],
      preCreate: [...(this.hooks.preCreate || []), ...(hooks.preCreate || [])],
      postCreate: [...(this.hooks.postCreate || []), ...(hooks.postCreate || [])],
      preUpdate: [...(this.hooks.preUpdate || []), ...(hooks.preUpdate || [])],
      postUpdate: [...(this.hooks.postUpdate || []), ...(hooks.postUpdate || [])],
      preDelete: [...(this.hooks.preDelete || []), ...(hooks.preDelete || [])],
      postDelete: [...(this.hooks.postDelete || []), ...(hooks.postDelete || [])],
    }
  }

  // ===========================================================================
  // Create Operation
  // ===========================================================================

  /**
   * Execute a create operation
   *
   * @param namespace - Target namespace
   * @param data - Entity data
   * @param store - Entity store
   * @param options - Create options
   * @returns Created entity
   */
  async create<T = Record<string, unknown>>(
    namespace: string,
    data: CreateInput<T>,
    store: EntityStore,
    options?: CreateOptions
  ): Promise<Entity<T>> {
    const startTime = Date.now()
    validateNamespace(namespace)

    const context = createMutationContext(namespace, {
      actor: options?.actor || this.defaultActor,
      skipValidation: options?.skipValidation || false,
      validationMode: options?.validateOnWrite === false ? false :
        (options?.validateOnWrite === true ? 'strict' : options?.validateOnWrite || 'strict'),
    })

    // Create observability context
    const observabilityContext = createObservabilityMutationContext(
      'create',
      namespace,
      undefined,
      data as CreateInput<unknown>
    )

    // Dispatch observability hook: mutation start
    await globalHookRegistry.dispatchMutationStart(observabilityContext)

    try {
      // Execute pre-mutation hooks
      await this.executeHooks('preMutation', context, 'create', data)
      await this.executeHooks('preCreate', context, 'create', data)

      // Execute create
      const result = executeCreate<T>(context, data, {
        schema: this.schema,
        schemaValidator: this.schemaValidator,
        generateId,
      })

      // Store entity
      store.set(result.entityId, result.entity as Entity)

      // Record events
      await this.recordEvents(result.events)

      // Execute post-mutation hooks
      await this.executeHooks('postCreate', context, 'create', result.entity, result.events)
      await this.executeHooks('postMutation', context, 'create', result.entity, result.events)

      // Dispatch observability hook: mutation end
      await globalHookRegistry.dispatchMutationEnd(observabilityContext, {
        affectedCount: 1,
        generatedIds: [result.entityId],
        durationMs: Date.now() - startTime,
        newVersion: (result.entity as Entity).version,
      })

      return result.entity
    } catch (error) {
      // Dispatch observability hook: mutation error
      await globalHookRegistry.dispatchMutationError(
        observabilityContext,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  // ===========================================================================
  // Update Operation
  // ===========================================================================

  /**
   * Execute an update operation
   *
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param update - Update operators
   * @param store - Entity store
   * @param options - Update options
   * @returns Updated entity or null
   */
  async update<T extends EntityData = EntityData>(
    namespace: string,
    id: string,
    update: UpdateInput<T>,
    store: EntityStore,
    options?: UpdateOptions
  ): Promise<Entity<T> | null> {
    const startTime = Date.now()
    validateNamespace(namespace)
    validateUpdateOperators(update)

    const fullId = id.includes('/') ? id : `${namespace}/${id}`

    const context = createMutationContext(namespace, {
      actor: options?.actor || this.defaultActor,
      skipValidation: options?.skipValidation || false,
      validationMode: options?.validateOnWrite === false ? false :
        (options?.validateOnWrite === true ? 'strict' : options?.validateOnWrite || 'strict'),
    })

    // Create observability context
    const observabilityContext = createObservabilityMutationContext(
      'update',
      namespace,
      fullId,
      update as UpdateInput<EntityData>
    )

    // Dispatch observability hook: mutation start
    await globalHookRegistry.dispatchMutationStart(observabilityContext)

    try {
      // Execute pre-mutation hooks
      await this.executeHooks('preMutation', context, 'update', { id: fullId, update })
      await this.executeHooks('preUpdate', context, 'update', { id: fullId, update })

      const existingEntity = store.get(fullId)

      // Execute update
      const result = executeUpdate<T>(context, fullId, update, existingEntity, {
        schema: this.schema,
        expectedVersion: options?.expectedVersion,
        upsert: options?.upsert,
        returnDocument: options?.returnDocument,
        getEntity: (entityId) => store.get(entityId),
        setEntity: (entityId, entity) => store.set(entityId, entity),
      })

      if (!result.modified) {
        // Dispatch observability hook: mutation end (no changes)
        await globalHookRegistry.dispatchMutationEnd(observabilityContext, {
          affectedCount: 0,
          durationMs: Date.now() - startTime,
        })
        return result.entity
      }

      // Store updated entity
      if (result.entity) {
        // Actually, we need to reconstruct from the events
        const createEvent = result.events.find(e => e.op === 'CREATE' || e.op === 'UPDATE')
        if (createEvent?.after) {
          store.set(fullId, createEvent.after as Entity)
        }
      }

      // Record events
      await this.recordEvents(result.events)

      // Execute post-mutation hooks
      await this.executeHooks('postUpdate', context, 'update', result.entity, result.events)
      await this.executeHooks('postMutation', context, 'update', result.entity, result.events)

      // Dispatch observability hook: mutation end
      const updatedEntity = result.entity as Entity | null
      await globalHookRegistry.dispatchMutationEnd(observabilityContext, {
        affectedCount: result.modified ? 1 : 0,
        generatedIds: result.upserted ? [fullId] : undefined,
        durationMs: Date.now() - startTime,
        newVersion: updatedEntity?.version,
      })

      return result.entity
    } catch (error) {
      // Dispatch observability hook: mutation error
      await globalHookRegistry.dispatchMutationError(
        observabilityContext,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  // ===========================================================================
  // Delete Operation
  // ===========================================================================

  /**
   * Execute a delete operation
   *
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param store - Entity store
   * @param options - Delete options
   * @returns Delete result
   */
  async delete(
    namespace: string,
    id: string,
    store: EntityStore,
    options?: DeleteOptions
  ): Promise<{ deletedCount: number }> {
    const startTime = Date.now()
    validateNamespace(namespace)

    const fullId = id.includes('/') ? id : `${namespace}/${id}`

    const context = createMutationContext(namespace, {
      actor: options?.actor || this.defaultActor,
    })

    // Create observability context
    const observabilityContext = createObservabilityMutationContext(
      'delete',
      namespace,
      fullId
    )
    observabilityContext.hard = options?.hard

    // Dispatch observability hook: mutation start
    await globalHookRegistry.dispatchMutationStart(observabilityContext)

    try {
      // Execute pre-mutation hooks
      await this.executeHooks('preMutation', context, 'delete', { id: fullId })
      await this.executeHooks('preDelete', context, 'delete', { id: fullId })

      const existingEntity = store.get(fullId)

      // Execute delete
      const result = executeDelete(context, fullId, existingEntity, {
        expectedVersion: options?.expectedVersion,
        hard: options?.hard,
      })

      // Apply deletion to store
      if (result.deletedCount > 0 && existingEntity) {
        if (options?.hard) {
          store.delete(fullId)
        } else {
          const softDeleted = applySoftDelete({ ...existingEntity }, context)
          store.set(fullId, softDeleted)
        }
      }

      // Record events
      await this.recordEvents(result.events)

      // Execute post-mutation hooks
      await this.executeHooks('postDelete', context, 'delete', result, result.events)
      await this.executeHooks('postMutation', context, 'delete', result, result.events)

      // Dispatch observability hook: mutation end
      await globalHookRegistry.dispatchMutationEnd(observabilityContext, {
        affectedCount: result.deletedCount,
        durationMs: Date.now() - startTime,
      })

      return { deletedCount: result.deletedCount }
    } catch (error) {
      // Dispatch observability hook: mutation error
      await globalHookRegistry.dispatchMutationError(
        observabilityContext,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  // ===========================================================================
  // Restore Operation
  // ===========================================================================

  /**
   * Execute a restore operation (un-delete)
   *
   * @param namespace - Target namespace
   * @param id - Entity ID
   * @param store - Entity store
   * @param options - Options with actor
   * @returns Restored entity or null
   */
  async restore<T = Record<string, unknown>>(
    namespace: string,
    id: string,
    store: EntityStore,
    options?: { actor?: EntityId | undefined }
  ): Promise<Entity<T> | null> {
    validateNamespace(namespace)

    const fullId = id.includes('/') ? id : `${namespace}/${id}`
    const entity = store.get(fullId)

    if (!entity) {
      return null
    }

    if (!entity.deletedAt) {
      return entity as Entity<T>
    }

    const context = createMutationContext(namespace, {
      actor: options?.actor || this.defaultActor,
    })

    const beforeEntity = { ...entity }
    const restored = applyRestore(entity, context)
    store.set(fullId, restored)

    // Generate RESTORE event (as UPDATE)
    const [ns, ...idParts] = fullId.split('/')
    const event: MutationEvent = {
      op: 'UPDATE',
      target: entityTarget(ns || '', idParts.join('/')),
      before: beforeEntity as Record<string, unknown>,
      after: restored as Record<string, unknown>,
      actor: context.actor,
      timestamp: context.timestamp,
    }

    await this.recordEvents([event])

    return restored as Entity<T>
  }

  // ===========================================================================
  // Bulk Operations
  // ===========================================================================

  /**
   * Execute bulk delete operation
   *
   * @param namespace - Target namespace
   * @param filter - Filter for entities to delete
   * @param store - Entity store
   * @param matchFilter - Function to match filter against entity
   * @param options - Delete options
   * @returns Delete result
   */
  async deleteMany(
    namespace: string,
    filter: Filter,
    store: EntityStore,
    matchFilter: (entity: Entity, filter: Filter) => boolean,
    options?: DeleteOptions
  ): Promise<{ deletedCount: number }> {
    const startTime = Date.now()
    validateNamespace(namespace)

    // Create observability context
    const observabilityContext = createObservabilityMutationContext(
      'deleteMany',
      namespace
    )
    observabilityContext.hard = options?.hard
    observabilityContext.metadata = { filter }

    // Dispatch observability hook: mutation start
    await globalHookRegistry.dispatchMutationStart(observabilityContext)

    try {
      let deletedCount = 0
      const deletedIds: string[] = []

      store.forEach((entity, id) => {
        if (id.startsWith(`${namespace}/`)) {
          if (!entity.deletedAt && matchFilter(entity, filter)) {
            // Delete each matching entity
            // This is simplified - the actual implementation would batch these
            const context = createMutationContext(namespace, {
              actor: options?.actor || this.defaultActor,
            })

            if (options?.hard) {
              store.delete(id)
            } else {
              applySoftDelete(entity, context)
              store.set(id, entity)
            }

            deletedCount++
            deletedIds.push(id)
          }
        }
      })

      // Dispatch observability hook: mutation end
      await globalHookRegistry.dispatchMutationEnd(observabilityContext, {
        affectedCount: deletedCount,
        durationMs: Date.now() - startTime,
      })

      return { deletedCount }
    } catch (error) {
      // Dispatch observability hook: mutation error
      await globalHookRegistry.dispatchMutationError(
        observabilityContext,
        error instanceof Error ? error : new Error(String(error))
      )
      throw error
    }
  }

  // ===========================================================================
  // Hook Execution
  // ===========================================================================

  private async executeHooks(
    hookType: keyof MutationHooks,
    context: MutationContext,
    operation: 'create' | 'update' | 'delete',
    data: unknown,
    events?: MutationEvent[]
  ): Promise<void> {
    const hooks = this.hooks[hookType]
    if (!hooks || hooks.length === 0) return

    for (const hook of hooks) {
      if (events) {
        await (hook as PostMutationHandler)(context, operation, data, events)
      } else {
        await (hook as PreMutationHandler)(context, operation, data)
      }
    }
  }

  // ===========================================================================
  // Event Recording
  // ===========================================================================

  private async recordEvents(events: MutationEvent[]): Promise<void> {
    if (!this.recordEventFn) return

    for (const event of events) {
      await this.recordEventFn(event)
    }
  }
}

// Re-export VersionConflictError for convenience
export { VersionConflictError }
