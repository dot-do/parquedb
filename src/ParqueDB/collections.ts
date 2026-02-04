/**
 * ParqueDB Collections Module
 *
 * Handles collection proxying and caching:
 * - Collection instance caching (singleton per namespace)
 * - Namespace normalization
 * - Dynamic collection access via Proxy
 */

import type {
  Entity,
  EntityData,
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
  IngestStreamOptions,
  IngestStreamResult,
} from './types'

import { CollectionImpl } from './collection'
import { normalizeNamespace } from './validation'
import { asCollection } from '../types/cast'

/**
 * Interface for the database methods required by CollectionManager.
 * This mirrors ParqueDBMethods in collection.ts to avoid circular dependencies.
 */
export interface CollectionManagerContext {
  find<T extends EntityData>(namespace: string, filter?: Filter, options?: FindOptions<T>): Promise<PaginatedResult<Entity<T>>>
  get<T extends EntityData>(namespace: string, id: string, options?: GetOptions<T>): Promise<Entity<T> | null>
  create<T extends EntityData>(namespace: string, data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>
  update<T extends EntityData>(namespace: string, id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null>
  delete(namespace: string, id: string, options?: DeleteOptions): Promise<DeleteResult>
  deleteMany(namespace: string, filter: Filter, options?: DeleteOptions): Promise<DeleteResult>
  upsert<T extends EntityData>(namespace: string, filter: Filter, update: UpdateInput<T>, options?: { returnDocument?: 'before' | 'after' | undefined }): Promise<Entity<T> | null>
  upsertMany<T extends EntityData>(namespace: string, items: UpsertManyItem<T>[], options?: UpsertManyOptions): Promise<UpsertManyResult>
  ingestStream<T extends EntityData>(namespace: string, source: AsyncIterable<Partial<T>> | Iterable<Partial<T>>, options?: IngestStreamOptions<Partial<T>>): Promise<IngestStreamResult>
}

/**
 * CollectionManager handles collection instance caching and creation.
 *
 * Collections are singletons per namespace - requesting the same namespace
 * multiple times returns the same CollectionImpl instance.
 *
 * @example
 * ```typescript
 * const manager = new CollectionManager(db)
 * const posts = manager.get<Post>('posts')
 * const samePosts = manager.get<Post>('Posts')  // Same instance (normalized)
 * ```
 */
export class CollectionManager {
  private collections = new Map<string, CollectionImpl>()
  private db: CollectionManagerContext

  constructor(db: CollectionManagerContext) {
    this.db = db
  }

  /**
   * Get or create a collection for the given namespace.
   * Normalizes the namespace and returns a cached instance if available.
   *
   * @param namespace - The collection namespace (e.g., 'posts', 'Posts', 'POSTS')
   * @returns A Collection instance for the namespace
   */
  get<T extends EntityData = EntityData>(namespace: string): Collection<T> {
    const normalizedNs = normalizeNamespace(namespace)

    let collection = this.collections.get(normalizedNs)
    if (!collection) {
      // CollectionImpl expects a db with ParqueDBMethods interface
      // CollectionManagerContext is compatible with ParqueDBMethods
      collection = new CollectionImpl(this.db as never, normalizedNs)
      this.collections.set(normalizedNs, collection)
    }

    return asCollection<T>(collection)
  }

  /**
   * Check if a collection exists in the cache.
   *
   * @param namespace - The collection namespace
   * @returns True if the collection is cached
   */
  has(namespace: string): boolean {
    const normalizedNs = normalizeNamespace(namespace)
    return this.collections.has(normalizedNs)
  }

  /**
   * Clear the collection cache.
   * Useful for testing or when resetting database state.
   */
  clear(): void {
    this.collections.clear()
  }

  /**
   * Get all cached namespace names.
   *
   * @returns Array of normalized namespace names
   */
  getNamespaces(): string[] {
    return Array.from(this.collections.keys())
  }
}

/**
 * Create a collection for a namespace using the provided database context.
 * This is a standalone function alternative to using CollectionManager.
 *
 * @param db - The database context implementing CollectionManagerContext
 * @param namespace - The collection namespace
 * @returns A new Collection instance
 */
export function createCollection<T extends EntityData = EntityData>(
  db: CollectionManagerContext,
  namespace: string
): Collection<T> {
  const normalizedNs = normalizeNamespace(namespace)
  return asCollection<T>(new CollectionImpl(db as never, normalizedNs))
}
