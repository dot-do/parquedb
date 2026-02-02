/**
 * ParqueDB Collection Module
 *
 * Contains the CollectionImpl class that provides a fluent API for entity operations.
 */

import type {
  Entity,
  PaginatedResult,
  DeleteResult,
  Filter,
  UpdateInput,
  FindOptions,
  GetOptions,
  CreateInput,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
} from '../types'

import type {
  Collection,
  UpsertManyItem,
  UpsertManyOptions,
  UpsertManyResult,
} from './types'

// Forward reference to ParqueDBImpl - will be imported at runtime
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type ParqueDBImplType = any

// =============================================================================
// CollectionImpl Class
// =============================================================================

/**
 * Implementation of Collection interface
 */
export class CollectionImpl<T = Record<string, unknown>> implements Collection<T> {
  constructor(
    private db: ParqueDBImplType,
    public readonly namespace: string
  ) {}

  async find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>> {
    return this.db.find<T>(this.namespace, filter, options)
  }

  async get(id: string, options?: GetOptions): Promise<Entity<T> | null> {
    return this.db.get<T>(this.namespace, id, options)
  }

  async create(data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>> {
    return this.db.create<T>(this.namespace, data, options)
  }

  async update(id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null> {
    return this.db.update<T>(this.namespace, id, update, options)
  }

  async delete(id: string, options?: DeleteOptions): Promise<DeleteResult> {
    return this.db.delete(this.namespace, id, options)
  }

  async deleteMany(filter: Filter, options?: DeleteOptions): Promise<DeleteResult> {
    return this.db.deleteMany(this.namespace, filter, options)
  }

  async upsert(filter: Filter, update: UpdateInput<T>, options?: { returnDocument?: 'before' | 'after' }): Promise<Entity<T> | null> {
    return this.db.upsert<T>(this.namespace, filter, update, options)
  }

  async findOne(filter?: Filter, options?: FindOptions): Promise<Entity<T> | null> {
    const result = await this.db.find<T>(this.namespace, filter, { ...options, limit: 1 })
    return result.items[0] ?? null
  }

  async upsertMany(items: UpsertManyItem<T>[], options?: UpsertManyOptions): Promise<UpsertManyResult> {
    return this.db.upsertMany<T>(this.namespace, items, options)
  }
}
