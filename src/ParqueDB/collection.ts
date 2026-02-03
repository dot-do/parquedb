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
  IngestStreamOptions,
  IngestStreamResult,
} from './types'

/**
 * Interface for the database methods used by CollectionImpl.
 * This avoids circular dependencies with ParqueDBImpl while providing type safety.
 */
interface ParqueDBMethods {
  find<T>(namespace: string, filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>>
  get<T>(namespace: string, id: string, options?: GetOptions): Promise<Entity<T> | null>
  create<T>(namespace: string, data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>>
  update<T>(namespace: string, id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null>
  delete(namespace: string, id: string, options?: DeleteOptions): Promise<DeleteResult>
  deleteMany(namespace: string, filter: Filter, options?: DeleteOptions): Promise<DeleteResult>
  upsert<T>(namespace: string, filter: Filter, update: UpdateInput<T>, options?: { returnDocument?: 'before' | 'after' | undefined }): Promise<Entity<T> | null>
  upsertMany<T>(namespace: string, items: UpsertManyItem<T>[], options?: UpsertManyOptions): Promise<UpsertManyResult>
  ingestStream<T>(namespace: string, source: AsyncIterable<Partial<T>> | Iterable<Partial<T>>, options?: IngestStreamOptions<Partial<T>>): Promise<IngestStreamResult>
}

// =============================================================================
// CollectionImpl Class
// =============================================================================

/**
 * Implementation of the Collection interface providing a fluent API for entity operations.
 *
 * CollectionImpl is a thin wrapper around ParqueDB methods that provides namespace-scoped
 * access to CRUD operations. It enables the proxy-based `db.Posts.find()` syntax by
 * capturing the namespace from the property accessor.
 *
 * @typeParam T - The entity data type for type-safe operations
 *
 * @example
 * ```typescript
 * // Access via proxy (recommended)
 * const posts = await db.Posts.find({ status: 'published' })
 *
 * // Direct construction (internal use)
 * const collection = new CollectionImpl<Post>(db, 'posts')
 * const post = await collection.get('01HX...')
 * ```
 */
export class CollectionImpl<T = Record<string, unknown>> implements Collection<T> {
  /**
   * Creates a new CollectionImpl instance.
   *
   * @param db - The ParqueDB methods interface for database operations
   * @param namespace - The namespace (collection name) for this collection
   */
  constructor(
    private db: ParqueDBMethods,
    public readonly namespace: string
  ) {}

  /**
   * Finds entities matching the specified filter criteria.
   *
   * Supports MongoDB-style query operators ($eq, $ne, $gt, $lt, $in, $and, $or, etc.),
   * pagination via cursor or skip/limit, sorting, and field projection.
   *
   * @param filter - Optional MongoDB-style filter object. If omitted, returns all entities.
   * @param options - Optional query options for pagination, sorting, projection, etc.
   * @returns A promise resolving to a paginated result containing matching entities.
   *
   * @throws {ValidationError} When filter syntax is invalid or namespace is malformed.
   *
   * @example
   * ```typescript
   * // Simple equality filter
   * const published = await collection.find({ status: 'published' })
   *
   * // Complex filter with operators
   * const recent = await collection.find({
   *   $and: [
   *     { status: 'published' },
   *     { createdAt: { $gte: new Date('2024-01-01') } }
   *   ]
   * })
   *
   * // With pagination and sorting
   * const page = await collection.find(
   *   { category: 'tech' },
   *   { sort: { createdAt: -1 }, limit: 20, cursor: 'abc...' }
   * )
   *
   * // With field projection
   * const titles = await collection.find({}, { project: { title: 1, author: 1 } })
   * ```
   */
  async find(filter?: Filter, options?: FindOptions): Promise<PaginatedResult<Entity<T>>> {
    return this.db.find<T>(this.namespace, filter, options)
  }

  /**
   * Retrieves a single entity by its unique identifier.
   *
   * @param id - The entity ID. Can be a full EntityId (namespace/id) or just the id part.
   * @param options - Optional get options for time-travel, projection, and hydration.
   * @returns A promise resolving to the entity if found, or null if not found.
   *
   * @throws {ValidationError} When the id format is invalid.
   *
   * @example
   * ```typescript
   * // Get by ID
   * const post = await collection.get('01HX1234ABCD')
   *
   * // With time-travel
   * const oldVersion = await collection.get('01HX...', {
   *   asOf: new Date('2024-01-01')
   * })
   *
   * // With projection
   * const partial = await collection.get('01HX...', {
   *   project: { title: 1, status: 1 }
   * })
   *
   * // Include soft-deleted entities
   * const deleted = await collection.get('01HX...', { includeDeleted: true })
   * ```
   */
  async get(id: string, options?: GetOptions): Promise<Entity<T> | null> {
    return this.db.get<T>(this.namespace, id, options)
  }

  /**
   * Creates a new entity in the collection.
   *
   * Automatically generates a ULID for the entity's $id, sets audit fields
   * (createdAt, updatedAt, version), and logs a CREATE event for time-travel.
   *
   * @param data - The entity data to create. Can include an explicit $id or let one be generated.
   * @param options - Optional create options including actor for audit trails and validation settings.
   * @returns A promise resolving to the newly created entity with all system fields populated.
   *
   * @throws {ValidationError} When required fields are missing or data fails schema validation.
   *
   * @example
   * ```typescript
   * // Basic create
   * const post = await collection.create({
   *   title: 'Hello World',
   *   content: 'My first post',
   *   status: 'draft'
   * })
   * console.log(post.$id) // '01HX1234ABCD'
   *
   * // With explicit ID
   * const user = await collection.create({
   *   $id: 'user-123',
   *   name: 'Alice',
   *   email: 'alice@example.com'
   * })
   *
   * // With audit actor
   * const comment = await collection.create(
   *   { text: 'Great post!' },
   *   { actor: 'users/01HX...' }
   * )
   * ```
   */
  async create(data: CreateInput<T>, options?: CreateOptions): Promise<Entity<T>> {
    return this.db.create<T>(this.namespace, data, options)
  }

  /**
   * Updates an existing entity by ID with the specified update operations.
   *
   * Supports MongoDB-style update operators ($set, $inc, $push, $pull, $unset, etc.)
   * and automatically updates audit fields (updatedAt, version). Logs an UPDATE event.
   *
   * @param id - The entity ID to update.
   * @param update - Update operations to apply. Can be partial data or use operators.
   * @param options - Optional update options including expectedVersion for optimistic concurrency.
   * @returns A promise resolving to the updated entity, or null if not found.
   *
   * @throws {ValidationError} When update data fails schema validation.
   * @throws {VersionConflictError} When expectedVersion doesn't match the current version.
   *
   * @example
   * ```typescript
   * // Simple field update
   * const updated = await collection.update('01HX...', {
   *   $set: { status: 'published', publishedAt: new Date() }
   * })
   *
   * // Increment a counter
   * await collection.update('01HX...', { $inc: { viewCount: 1 } })
   *
   * // Array operations
   * await collection.update('01HX...', {
   *   $push: { tags: 'featured' },
   *   $pull: { categories: 'draft' }
   * })
   *
   * // With optimistic concurrency
   * const result = await collection.update(
   *   '01HX...',
   *   { $set: { title: 'New Title' } },
   *   { expectedVersion: 5 }
   * )
   * ```
   */
  async update(id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null> {
    return this.db.update<T>(this.namespace, id, update, options)
  }

  /**
   * Deletes an entity by ID.
   *
   * By default performs a soft delete (sets deletedAt timestamp). Use the `hard` option
   * for permanent deletion. Logs a DELETE event for time-travel.
   *
   * @param id - The entity ID to delete.
   * @param options - Optional delete options including hard delete and expected version.
   * @returns A promise resolving to the delete result with success status and deleted count.
   *
   * @throws {VersionConflictError} When expectedVersion doesn't match the current version.
   *
   * @example
   * ```typescript
   * // Soft delete (default)
   * const result = await collection.delete('01HX...')
   * console.log(result.deletedCount) // 1
   *
   * // Hard delete (permanent)
   * await collection.delete('01HX...', { hard: true })
   *
   * // With optimistic concurrency
   * await collection.delete('01HX...', { expectedVersion: 3 })
   *
   * // With audit actor
   * await collection.delete('01HX...', { actor: 'users/admin' })
   * ```
   */
  async delete(id: string, options?: DeleteOptions): Promise<DeleteResult> {
    return this.db.delete(this.namespace, id, options)
  }

  /**
   * Deletes multiple entities matching the specified filter.
   *
   * By default performs soft deletes. Use the `hard` option for permanent deletion.
   * Logs DELETE events for each deleted entity.
   *
   * @param filter - MongoDB-style filter to match entities for deletion.
   * @param options - Optional delete options including hard delete mode.
   * @returns A promise resolving to the delete result with total deleted count.
   *
   * @example
   * ```typescript
   * // Delete all drafts
   * const result = await collection.deleteMany({ status: 'draft' })
   * console.log(`Deleted ${result.deletedCount} drafts`)
   *
   * // Hard delete old entities
   * await collection.deleteMany(
   *   { createdAt: { $lt: new Date('2023-01-01') } },
   *   { hard: true }
   * )
   *
   * // Delete by multiple criteria
   * await collection.deleteMany({
   *   $and: [
   *     { status: 'spam' },
   *     { flagCount: { $gte: 5 } }
   *   ]
   * })
   * ```
   */
  async deleteMany(filter: Filter, options?: DeleteOptions): Promise<DeleteResult> {
    return this.db.deleteMany(this.namespace, filter, options)
  }

  /**
   * Updates an existing entity or creates a new one if no match is found.
   *
   * Finds an entity matching the filter and applies the update. If no entity matches,
   * creates a new entity with the filter criteria merged with the update data.
   *
   * @param filter - MongoDB-style filter to find the entity to update.
   * @param update - Update operations to apply to the found or new entity.
   * @param options - Optional options including which document state to return.
   * @returns A promise resolving to the entity (before or after update), or null on certain conditions.
   *
   * @throws {ValidationError} When update data fails schema validation.
   *
   * @example
   * ```typescript
   * // Upsert a user preference
   * const pref = await collection.upsert(
   *   { userId: 'user-123', key: 'theme' },
   *   { $set: { value: 'dark' } }
   * )
   *
   * // Get the document state before the update
   * const before = await collection.upsert(
   *   { email: 'alice@example.com' },
   *   { $set: { lastLogin: new Date() } },
   *   { returnDocument: 'before' }
   * )
   *
   * // Upsert with increment (creates with count=1 if new)
   * await collection.upsert(
   *   { url: 'https://example.com' },
   *   { $inc: { visitCount: 1 } }
   * )
   * ```
   */
  async upsert(filter: Filter, update: UpdateInput<T>, options?: { returnDocument?: 'before' | 'after' | undefined }): Promise<Entity<T> | null> {
    return this.db.upsert<T>(this.namespace, filter, update, options)
  }

  /**
   * Finds a single entity matching the filter.
   *
   * Convenience method equivalent to `find(filter, { limit: 1 })` that returns
   * the entity directly instead of a paginated result.
   *
   * @param filter - Optional MongoDB-style filter. If omitted, returns the first entity found.
   * @param options - Optional query options for sorting, projection, etc.
   * @returns A promise resolving to the first matching entity, or null if none found.
   *
   * @throws {ValidationError} When filter syntax is invalid.
   *
   * @example
   * ```typescript
   * // Find by unique field
   * const user = await collection.findOne({ email: 'alice@example.com' })
   *
   * // Find most recent
   * const latest = await collection.findOne(
   *   { status: 'published' },
   *   { sort: { createdAt: -1 } }
   * )
   *
   * // With projection
   * const partial = await collection.findOne(
   *   { $id: '01HX...' },
   *   { project: { title: 1, author: 1 } }
   * )
   * ```
   */
  async findOne(filter?: Filter, options?: FindOptions): Promise<Entity<T> | null> {
    const result = await this.db.find<T>(this.namespace, filter, { ...options, limit: 1 })
    return result.items[0] ?? null
  }

  /**
   * Performs multiple upsert operations in a single batch.
   *
   * Efficiently processes an array of filter/update pairs, upserting each one.
   * Supports ordered mode (stop on first error) or unordered mode (continue on errors).
   *
   * @param items - Array of upsert items, each containing a filter and update operation.
   * @param options - Optional batch options including ordered mode and actor.
   * @returns A promise resolving to the batch result with counts and any errors.
   *
   * @throws {ValidationError} When any update data fails schema validation (in ordered mode).
   *
   * @example
   * ```typescript
   * // Bulk upsert user preferences
   * const result = await collection.upsertMany([
   *   { filter: { userId: 'u1', key: 'theme' }, update: { $set: { value: 'dark' } } },
   *   { filter: { userId: 'u1', key: 'lang' }, update: { $set: { value: 'en' } } },
   *   { filter: { userId: 'u2', key: 'theme' }, update: { $set: { value: 'light' } } },
   * ])
   *
   * console.log(`Inserted: ${result.insertedCount}, Modified: ${result.modifiedCount}`)
   *
   * // Unordered mode (continue on errors)
   * const result = await collection.upsertMany(items, { ordered: false })
   * if (result.errors.length > 0) {
   *   console.error('Some upserts failed:', result.errors)
   * }
   *
   * // With optimistic concurrency per item
   * await collection.upsertMany([
   *   {
   *     filter: { $id: '01HX...' },
   *     update: { $set: { status: 'approved' } },
   *     options: { expectedVersion: 2 }
   *   }
   * ])
   * ```
   */
  async upsertMany(items: UpsertManyItem<T>[], options?: UpsertManyOptions): Promise<UpsertManyResult> {
    return this.db.upsertMany<T>(this.namespace, items, options)
  }

  /**
   * Ingests a stream of documents into the collection.
   *
   * Efficiently bulk-inserts documents from an async iterable, array, or generator.
   * Supports batching, document transformation, progress callbacks, and error handling.
   *
   * @param source - Async iterable, iterable, or array of documents to ingest.
   * @param options - Optional ingest options for batching, transforms, and callbacks.
   * @returns A promise resolving to the ingest result with counts of inserted, failed, and skipped documents.
   *
   * @example
   * ```typescript
   * // Ingest from an array
   * const result = await collection.ingestStream([
   *   { name: 'Item 1', price: 10 },
   *   { name: 'Item 2', price: 20 },
   *   { name: 'Item 3', price: 30 },
   * ])
   * console.log(`Inserted: ${result.insertedCount}`)
   *
   * // Ingest from async generator with transform
   * async function* fetchProducts() {
   *   for await (const page of apiPages) {
   *     yield* page.products
   *   }
   * }
   *
   * const result = await collection.ingestStream(fetchProducts(), {
   *   batchSize: 100,
   *   transform: (doc) => ({
   *     ...doc,
   *     importedAt: new Date(),
   *     source: 'api'
   *   }),
   *   onProgress: (count) => console.log(`Processed: ${count}`),
   *   onBatchComplete: (stats) => console.log(`Batch ${stats.batchNumber} done`)
   * })
   *
   * // Skip invalid documents with transform returning null
   * const result = await collection.ingestStream(rawData, {
   *   transform: (doc) => doc.isValid ? doc : null,
   *   ordered: false // Continue on errors
   * })
   * console.log(`Skipped: ${result.skippedCount}, Failed: ${result.failedCount}`)
   * ```
   */
  async ingestStream(
    source: AsyncIterable<Partial<T>> | Iterable<Partial<T>>,
    options?: IngestStreamOptions<Partial<T>>
  ): Promise<IngestStreamResult> {
    return this.db.ingestStream<T>(this.namespace, source, options)
  }
}
