/**
 * ParqueDB Upsert Operations Module
 *
 * Contains upsert and upsertMany operations extracted from core.ts.
 */

import type {
  Entity,
  EntityData,
  EntityId as _EntityId,
  CreateInput,
  Filter,
  UpdateInput,
  CreateOptions,
  UpdateOptions,
  PaginatedResult,
} from '../types'

import type {
  UpsertManyItem,
  UpsertManyOptions,
  UpsertManyResult,
} from './types'

/**
 * Extract non-operator fields from filter to include in created documents
 * Helper for upsert operations
 */
export function extractFilterFields(filter: Filter): Record<string, unknown> {
  const filterFields: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(filter)) {
    if (!key.startsWith('$')) {
      filterFields[key] = value
    }
  }
  return filterFields
}

/**
 * Build create data for upsert insert operations
 * Combines filter fields, $set, $setOnInsert, and applies other operators
 */
export function buildUpsertCreateData<T extends EntityData = EntityData>(
  filterFields: Record<string, unknown>,
  update: UpdateInput<T>
): Record<string, unknown> {
  const createData: Record<string, unknown> = {
    $type: 'Unknown',
    name: 'Upserted',
    ...filterFields,
    ...update.$set,
    ...update.$setOnInsert,
  }

  // Handle $inc - start from 0
  if (update.$inc) {
    for (const [key, value] of Object.entries(update.$inc)) {
      createData[key] = ((createData[key] as number | undefined) ?? 0) + (value as number)
    }
  }

  // Handle $push - create array with single element
  if (update.$push) {
    for (const [key, value] of Object.entries(update.$push)) {
      const pushValue = value as Record<string, unknown>
      if (value && typeof value === 'object' && '$each' in pushValue) {
        createData[key] = [...((pushValue.$each as unknown[] | undefined) ?? [])]
      } else {
        createData[key] = [value]
      }
    }
  }

  // Handle $addToSet - create array with single element
  if (update.$addToSet) {
    for (const [key, value] of Object.entries(update.$addToSet)) {
      createData[key] = [value]
    }
  }

  // Handle $currentDate
  if (update.$currentDate) {
    const now = new Date()
    for (const key of Object.keys(update.$currentDate)) {
      createData[key] = now
    }
  }

  return createData
}

export interface UpsertContext {
  find<T extends EntityData>(namespace: string, filter?: Filter, options?: { limit?: number | undefined }): Promise<PaginatedResult<Entity<T>>>
  create<T extends EntityData>(namespace: string, data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>
  update<T extends EntityData>(namespace: string, id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null>
}

/**
 * Upsert an entity (filter-based: update if exists, create if not)
 */
export async function upsertEntity<T extends EntityData = EntityData>(
  namespace: string,
  filter: Filter,
  update: UpdateInput<T>,
  options: { returnDocument?: 'before' | 'after' | undefined } | undefined,
  ctx: UpsertContext
): Promise<Entity<T> | null> {
  // Find existing entity
  const result = await ctx.find<T>(namespace, filter)

  if (result.items.length > 0) {
    // Update existing
    const entity = result.items[0]!
    return ctx.update<T>(namespace, entity.$id as string, update, {
      returnDocument: options?.returnDocument,
    })
  } else {
    // Create new from filter fields and $set values
    const filterFields = extractFilterFields(filter)
    const data: CreateInput<T> = buildUpsertCreateData(filterFields, update) as CreateInput<T>
    return ctx.create<T>(namespace, data)
  }
}

/**
 * Upsert multiple entities in a single operation
 */
export async function upsertManyEntities<T extends EntityData = EntityData>(
  namespace: string,
  items: UpsertManyItem<T>[],
  options: UpsertManyOptions | undefined,
  ctx: UpsertContext
): Promise<UpsertManyResult> {
  const result: UpsertManyResult = {
    ok: true,
    insertedCount: 0,
    modifiedCount: 0,
    matchedCount: 0,
    upsertedCount: 0,
    upsertedIds: [],
    errors: [],
  }

  if (items.length === 0) {
    return result
  }

  const ordered = options?.ordered ?? true
  const actor = options?.actor

  for (let i = 0; i < items.length; i++) {
    const item = items[i]!

    try {
      // Find existing entity
      const existing = await ctx.find<T>(namespace, item.filter)

      if (existing.items.length > 0) {
        // Update existing entity
        const entity = existing.items[0]!
        result.matchedCount++

        const updateOptions: UpdateOptions = {
          returnDocument: 'after',
        }
        if (actor) {
          updateOptions.actor = actor
        }
        if (item.options?.expectedVersion !== undefined) {
          updateOptions.expectedVersion = item.options.expectedVersion
        }

        // Remove $setOnInsert from update since we're updating
        const { $setOnInsert: _, ...updateWithoutSetOnInsert } = item.update as UpdateInput<T> & { $setOnInsert?: unknown | undefined }

        await ctx.update<T>(namespace, entity.$id as string, updateWithoutSetOnInsert, updateOptions)
        result.modifiedCount++
      } else {
        // Create new entity
        const filterFields = extractFilterFields(item.filter)
        const createData = buildUpsertCreateData(filterFields, item.update)

        const createOptions: CreateOptions = {}
        if (actor) {
          createOptions.actor = actor
        }

        const created = await ctx.create<T>(namespace, createData as CreateInput<T>, createOptions)

        result.insertedCount++
        result.upsertedCount++
        result.upsertedIds.push(created.$id)

        // Handle $link after creation
        if (item.update.$link) {
          await ctx.update<T>(namespace, created.$id as string, {
            $link: item.update.$link,
          } as UpdateInput<T>, { actor })
        }
      }
    } catch (error: unknown) {
      result.ok = false
      result.errors.push({
        index: i,
        filter: item.filter,
        error: error instanceof Error ? error : new Error(String(error)),
      })

      if (ordered) {
        break
      }
    }
  }

  return result
}
