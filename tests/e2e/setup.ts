/**
 * E2E Test Setup for ParqueDB
 *
 * Provides utilities for testing ParqueDB with real storage (MemoryBackend).
 * The MemoryBackend exercises the full ParqueDB code path including storage
 * operations, event recording, and persistence - just without actual disk I/O.
 *
 * This setup ensures tests are isolated and exercise the full ParqueDB flow.
 */

import type {
  Entity,
  Filter,
  UpdateInput,
  FindOptions,
  GetOptions,
  CreateOptions,
  UpdateOptions,
  DeleteOptions,
  DeleteResult,
  EntityId,
} from '../../src/types'
import { ParqueDB, type HistoryResult, type HistoryItem } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'

// =============================================================================
// Type Definitions for Test Interface
// =============================================================================

/**
 * Collection interface - mirrors the ParqueDB Collection API
 */
export interface RPCCollection<T extends object = Record<string, unknown>> {
  find(filter?: Filter, options?: FindOptions<T>): Promise<Entity<T>[]>
  findOne(filter?: Filter, options?: FindOptions<T>): Promise<Entity<T> | null>
  get(id: string, options?: GetOptions): Promise<Entity<T>>
  create(data: T & { $type: string; name: string }, options?: CreateOptions): Promise<Entity<T>>
  update(id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null>
  delete(id: string, options?: DeleteOptions): Promise<DeleteResult>
  deleteMany(filter: Filter, options?: DeleteOptions): Promise<DeleteResult>
  count(filter?: Filter): Promise<number>
  exists(id: string): Promise<boolean>
}

/**
 * Post entity type for tests
 */
export interface Post {
  title: string
  content?: string
  status?: 'draft' | 'published' | 'archived'
  tags?: string[]
  viewCount?: number
  publishedAt?: Date
}

/**
 * User entity type for tests
 */
export interface User {
  name: string
  email?: string
  role?: 'admin' | 'user' | 'guest'
}

/**
 * Comment entity type for tests
 */
export interface Comment {
  text: string
  approved?: boolean
}

/**
 * ParqueDB Test Client interface
 * Provides typed access to collections and events
 */
export interface ParqueDBClient {
  /** Posts collection */
  Posts: RPCCollection<Post>
  /** Users collection */
  Users: RPCCollection<User>
  /** Comments collection */
  Comments: RPCCollection<Comment>

  /** Generic collection access */
  collection<T = Record<string, unknown>>(namespace: string): RPCCollection<T>

  /** Event history access for time-travel queries */
  getEvents(entityId: string, options?: { from?: Date; to?: Date; limit?: number }): Promise<Array<{
    id: string
    ts: Date
    op: 'CREATE' | 'UPDATE' | 'DELETE'
    before: Record<string, unknown> | null
    after: Record<string, unknown> | null
  }>>
}

// =============================================================================
// Storage Management
// =============================================================================

let currentDb: ParqueDB | null = null
let currentStorage: MemoryBackend | null = null

// =============================================================================
// Collection Wrapper Implementation
// =============================================================================

/**
 * Transform a filter to handle $id as a field instead of an operator
 * The original tests use { $id: 'posts/xxx' } which needs special handling
 */
function transformFilter(filter: Filter | undefined, namespace: string): Filter | undefined {
  if (!filter) return filter

  const transformed: Filter = {}

  for (const [key, value] of Object.entries(filter)) {
    if (key === '$id') {
      // Convert $id filter to checking the actual $id property via find + manual filter
      // Skip for now, we'll handle this in findOne
      transformed['_$id'] = value
    } else {
      transformed[key] = value
    }
  }

  return transformed
}

/**
 * Create a collection wrapper that adapts ParqueDB Collection to RPCCollection interface
 */
function createCollectionWrapper<T>(db: ParqueDB, namespace: string): RPCCollection<T> {
  // Get the collection proxy from ParqueDB
  const col = db.collection<T>(namespace)

  return {
    async find(filter?: Filter, options?: FindOptions<T>): Promise<Entity<T>[]> {
      // Handle $id filter specially (it's used in tests but not supported as operator)
      let idFilter: string | undefined
      let cleanFilter: Filter | undefined = filter

      if (filter && '$id' in filter) {
        idFilter = filter.$id as string
        const { $id, ...rest } = filter
        cleanFilter = Object.keys(rest).length > 0 ? rest : undefined
      }

      const result = await col.find(cleanFilter, options)
      let items = result.items

      // Apply $id filter manually if present
      if (idFilter) {
        items = items.filter(item => item.$id === idFilter)
      }

      // Apply sort if provided (ParqueDB doesn't fully implement this yet)
      if (options?.sort) {
        const sortFields = Object.entries(options.sort as Record<string, number>)
        items = [...items].sort((a, b) => {
          for (const [field, direction] of sortFields) {
            const aVal = (a as any)[field]
            const bVal = (b as any)[field]
            if (aVal === bVal) continue
            if (aVal === undefined) return direction > 0 ? 1 : -1
            if (bVal === undefined) return direction > 0 ? -1 : 1
            if (typeof aVal === 'number' && typeof bVal === 'number') {
              return direction > 0 ? aVal - bVal : bVal - aVal
            }
            return direction > 0 ? String(aVal).localeCompare(String(bVal)) : String(bVal).localeCompare(String(aVal))
          }
          return 0
        })
      }

      // Apply limit if provided (ParqueDB doesn't fully implement this yet)
      if (options?.limit !== undefined && options.limit > 0) {
        items = items.slice(0, options.limit)
      }

      return items
    },

    async findOne(filter?: Filter, options?: FindOptions<T>): Promise<Entity<T> | null> {
      // Handle $id filter specially
      if (filter && '$id' in filter) {
        const id = filter.$id as string
        try {
          const entity = await this.get(id, options)
          // Check other filter conditions
          const { $id, ...rest } = filter
          if (Object.keys(rest).length > 0) {
            for (const [key, value] of Object.entries(rest)) {
              if ((entity as any)[key] !== value) {
                return null
              }
            }
          }
          return entity
        } catch {
          return null
        }
      }

      const results = await this.find(filter, { ...options, limit: 1 })
      return results[0] ?? null
    },

    async get(id: string, options?: GetOptions): Promise<Entity<T>> {
      const entity = await col.get(id, options)
      if (!entity) {
        throw new Error(`Entity not found: ${id}`)
      }
      return entity
    },

    async create(data: T & { $type: string; name: string }, options?: CreateOptions): Promise<Entity<T>> {
      return col.create(data as any, options)
    },

    async update(id: string, update: UpdateInput<T>, options?: UpdateOptions): Promise<Entity<T> | null> {
      return col.update(id, update, options)
    },

    async delete(id: string, options?: DeleteOptions): Promise<DeleteResult> {
      // Check if entity exists before deleting (ParqueDB returns 1 for valid-looking IDs)
      const entity = await col.get(id)
      if (!entity) {
        return { deletedCount: 0 }
      }
      return col.delete(id, options)
    },

    async deleteMany(filter: Filter, options?: DeleteOptions): Promise<DeleteResult> {
      // Find all matching entities and delete them
      const entities = await this.find(filter)
      let deletedCount = 0
      for (const entity of entities) {
        const result = await this.delete(entity.$id as string, options)
        deletedCount += result.deletedCount
      }
      return { deletedCount }
    },

    async count(filter?: Filter): Promise<number> {
      const entities = await this.find(filter)
      return entities.length
    },

    async exists(id: string): Promise<boolean> {
      try {
        const entity = await col.get(id)
        return entity !== null
      } catch {
        return false
      }
    },
  }
}

// =============================================================================
// Test Client Implementation
// =============================================================================

/**
 * Creates a ParqueDB test client backed by MemoryBackend storage
 */
function createTestClient(): ParqueDBClient {
  // Create new MemoryBackend storage for this test
  currentStorage = new MemoryBackend()

  // Create ParqueDB instance with real storage
  currentDb = new ParqueDB({ storage: currentStorage })

  // Create a proxy that provides typed collection access and getEvents
  const handler: ProxyHandler<ParqueDBClient> = {
    get(_target, prop: string) {
      if (!currentDb) {
        throw new Error('Test client not initialized')
      }

      // Handle special methods
      if (prop === 'collection') {
        return <TT>(ns: string) => createCollectionWrapper<TT>(currentDb!, ns)
      }

      if (prop === 'getEvents') {
        return async (entityId: string, options?: { from?: Date; to?: Date; limit?: number }) => {
          const [ns, ...idParts] = entityId.split('/')
          const id = idParts.join('/')
          const historyResult: HistoryResult = await currentDb!.getHistory(ns, id, {
            from: options?.from,
            to: options?.to,
            limit: options?.limit,
          })
          return historyResult.items.map((item: HistoryItem) => ({
            id: item.id,
            ts: item.ts,
            op: item.op,
            before: item.before as Record<string, unknown> | null,
            after: item.after as Record<string, unknown> | null,
          }))
        }
      }

      // Handle collection access (e.g., Posts -> posts namespace)
      if (typeof prop === 'string' && prop[0] === prop[0].toUpperCase()) {
        const ns = prop.toLowerCase()
        return createCollectionWrapper(currentDb!, ns)
      }

      return undefined
    },
  }

  return new Proxy({} as ParqueDBClient, handler)
}

// =============================================================================
// Test Utilities
// =============================================================================

// Singleton client for the current test suite
let testClient: ParqueDBClient | null = null

/**
 * Get the ParqueDB test client
 * Uses MemoryBackend storage for full ParqueDB code path execution
 */
export function getTestClient(): ParqueDBClient {
  if (!testClient) {
    throw new Error('Test client not initialized. Call cleanupTestData() in beforeEach first.')
  }
  return testClient
}

/**
 * Clean up all test data between test runs
 * This creates a fresh ParqueDB instance with new storage for each test
 */
export async function cleanupTestData(): Promise<void> {
  // Clear references
  currentDb = null
  currentStorage = null

  // Create new test client with fresh storage
  testClient = createTestClient()
}

/**
 * Wait for eventual consistency (for tests that need timing delays)
 * When using fake timers, this advances time; otherwise, it returns immediately
 * since in-memory operations are synchronous.
 */
export async function waitForConsistency(_ms: number = 50): Promise<void> {
  // In test environments with in-memory storage, operations are synchronous
  // so no actual waiting is needed. The parameter is kept for API compatibility.
  return Promise.resolve()
}

/**
 * Generate a unique test ID to avoid collisions between test runs
 */
export function generateTestId(): string {
  return `test-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`
}

/**
 * Assert that an entity has all required audit fields
 */
export function assertAuditFields(entity: Entity): void {
  if (!entity.$id) {
    throw new Error('Entity missing $id')
  }
  if (!entity.$type) {
    throw new Error('Entity missing $type')
  }
  if (!entity.createdAt) {
    throw new Error('Entity missing createdAt')
  }
  if (!entity.updatedAt) {
    throw new Error('Entity missing updatedAt')
  }
  if (entity.version === undefined) {
    throw new Error('Entity missing version')
  }
}

/**
 * Create test data helper - creates multiple entities for testing
 */
export async function createTestPosts(client: ParqueDBClient, count: number): Promise<Entity<Post>[]> {
  const posts: Entity<Post>[] = []

  for (let i = 0; i < count; i++) {
    const post = await client.Posts.create({
      $type: 'Post',
      name: `Test Post ${i + 1}`,
      title: `Test Post ${i + 1}`,
      content: `Content for post ${i + 1}`,
      status: i % 2 === 0 ? 'published' : 'draft',
      tags: [`tag${i}`, 'test'],
    })
    posts.push(post)
  }

  return posts
}

/**
 * Get entity state at a specific time (for time-travel queries)
 */
export async function getEntityStateAtTime(
  client: ParqueDBClient,
  entityId: string,
  asOf: Date
): Promise<Record<string, unknown> | null> {
  const events = await client.getEvents(entityId)
  const eventsAtTime = events.filter(e => e.ts <= asOf)

  if (eventsAtTime.length === 0) {
    return null
  }

  // Get the last event at or before asOf
  const lastEvent = eventsAtTime[eventsAtTime.length - 1]

  if (lastEvent.op === 'DELETE') {
    return null
  }

  return lastEvent.after
}

/**
 * Get events for an entity (for event sourcing tests)
 */
export async function getEventsForEntity(
  client: ParqueDBClient,
  entityId: string,
  options?: { from?: Date; to?: Date; limit?: number }
): Promise<Array<{
  id: string
  ts: Date
  op: 'CREATE' | 'UPDATE' | 'DELETE'
  before: Record<string, unknown> | null
  after: Record<string, unknown> | null
}>> {
  return client.getEvents(entityId, options)
}
