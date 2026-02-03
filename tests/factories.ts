/**
 * Test Data Factories
 *
 * Factory functions for creating test data with sensible defaults.
 * Use these to create consistent test entities across all test types.
 */

import type {
  EntityId,
  EntityRef,
  Entity,
  EntityRecord,
  AuditFields,
  CreateInput,
  Schema,
  TypeDefinition,
  Namespace,
  Id,
} from '../types'

// =============================================================================
// ID Generation
// =============================================================================

let idCounter = 0

/**
 * Generate a unique test ID
 */
export function generateTestId(prefix = 'test'): string {
  idCounter++
  return `${prefix}-${Date.now()}-${idCounter}`
}

/**
 * Reset ID counter (call in beforeEach if needed)
 */
export function resetIdCounter(): void {
  idCounter = 0
}

/**
 * Create a valid EntityId
 */
export function createEntityId(ns: string, id?: string): EntityId {
  return `${ns}/${id ?? generateTestId()}` as EntityId
}

// =============================================================================
// Audit Fields Factory
// =============================================================================

/**
 * Create default audit fields for testing
 */
export function createAuditFields(overrides: Partial<AuditFields> = {}): AuditFields {
  const now = new Date()
  const systemActor = 'system/test' as EntityId

  return {
    createdAt: now,
    createdBy: systemActor,
    updatedAt: now,
    updatedBy: systemActor,
    version: 1,
    ...overrides,
  }
}

// =============================================================================
// Entity Factories
// =============================================================================

/**
 * Create a minimal entity reference for testing
 */
export function createEntityRef(overrides: Partial<EntityRef> = {}): EntityRef {
  const ns = 'test'
  const id = generateTestId()

  return {
    $id: createEntityId(ns, id),
    $type: 'TestEntity',
    name: `Test Entity ${id}`,
    ...overrides,
  }
}

/**
 * Create a test entity with all fields
 */
export function createTestEntity<T extends Record<string, unknown> = Record<string, unknown>>(
  overrides: Partial<Entity<T>> & { data?: T } = {}
): Entity<T> {
  const ref = createEntityRef(overrides)
  const audit = createAuditFields(overrides)
  const { data, ...rest } = overrides

  return {
    ...ref,
    ...audit,
    ...(data ?? {}),
    ...rest,
  } as Entity<T>
}

/**
 * Create multiple test entities
 */
export function createTestEntities<T extends Record<string, unknown> = Record<string, unknown>>(
  count: number,
  overrides: Partial<Entity<T>> | ((index: number) => Partial<Entity<T>>) = {}
): Entity<T>[] {
  return Array.from({ length: count }, (_, i) => {
    const entityOverrides = typeof overrides === 'function' ? overrides(i) : overrides
    return createTestEntity<T>({
      ...entityOverrides,
      name: entityOverrides.name ?? `Test Entity ${i + 1}`,
    })
  })
}

/**
 * Create an entity record (what's stored in data.parquet)
 */
export function createEntityRecord(overrides: Partial<EntityRecord> = {}): EntityRecord {
  const id = generateTestId()
  const audit = createAuditFields(overrides)

  return {
    ns: 'test' as Namespace,
    id: id as Id,
    type: 'TestEntity',
    name: `Test Entity ${id}`,
    data: {},
    ...audit,
    ...overrides,
  }
}

// =============================================================================
// Input Factories
// =============================================================================

/**
 * Create input for creating a new entity
 */
export function createCreateInput<T = Record<string, unknown>>(
  overrides: Partial<CreateInput<T>> = {}
): CreateInput<T> {
  return {
    $type: 'TestEntity',
    name: `New Test Entity ${generateTestId()}`,
    ...overrides,
  } as CreateInput<T>
}

/**
 * Create input for a Post entity
 */
export function createPostInput(overrides: Partial<CreateInput> = {}): CreateInput {
  return {
    $type: 'Post',
    name: `Test Post ${generateTestId()}`,
    title: 'Test Post Title',
    content: 'This is test content for the post.',
    status: 'draft',
    ...overrides,
  }
}

/**
 * Create input for a User entity
 */
export function createUserInput(overrides: Partial<CreateInput> = {}): CreateInput {
  const id = generateTestId('user')
  return {
    $type: 'User',
    name: `Test User ${id}`,
    email: `${id}@test.example.com`,
    ...overrides,
  }
}

// =============================================================================
// Schema Factories
// =============================================================================

/**
 * Create a basic test schema
 */
export function createTestSchema(overrides: Partial<Schema> = {}): Schema {
  return {
    TestEntity: {
      $type: 'schema:Thing',
      name: 'string!',
      description: 'string?',
      tags: 'string[]',
      count: 'int?',
      active: { type: 'boolean', default: true },
    },
    ...overrides,
  }
}

/**
 * Create a blog schema for testing relationships
 */
export function createBlogSchema(): Schema {
  return {
    User: {
      $type: 'schema:Person',
      $ns: 'users',
      name: 'string!',
      email: { type: 'email!', index: 'unique' },
      bio: 'text?',
      avatar: 'url?',
    },
    Post: {
      $type: 'schema:BlogPosting',
      $ns: 'posts',
      $shred: ['status', 'publishedAt'],
      name: 'string!',
      title: 'string!',
      content: 'markdown!',
      excerpt: 'text?',
      status: { type: 'string', default: 'draft', index: true },
      publishedAt: 'datetime?',
      author: '-> User.posts',
      categories: '-> Category.posts[]',
    },
    Category: {
      $type: 'schema:Category',
      $ns: 'categories',
      name: 'string!',
      slug: { type: 'string!', index: 'unique' },
      description: 'text?',
      posts: '<- Post.categories[]',
    },
    Comment: {
      $type: 'schema:Comment',
      $ns: 'comments',
      text: 'string!',
      post: '-> Post.comments',
      author: '-> User.comments',
    },
  }
}

/**
 * Create a type definition for testing
 */
export function createTypeDefinition(overrides: Partial<TypeDefinition> = {}): TypeDefinition {
  return {
    $type: 'schema:Thing',
    name: 'string!',
    ...overrides,
  }
}

// =============================================================================
// Filter Factories
// =============================================================================

/**
 * Create a simple equality filter
 */
export function createEqualityFilter(field: string, value: unknown): Record<string, unknown> {
  return { [field]: value }
}

/**
 * Create a comparison filter
 */
export function createComparisonFilter(
  field: string,
  operator: '$eq' | '$ne' | '$gt' | '$gte' | '$lt' | '$lte' | '$in' | '$nin',
  value: unknown
): Record<string, Record<string, unknown>> {
  return { [field]: { [operator]: value } }
}

/**
 * Create a logical AND filter
 */
export function createAndFilter(...filters: Record<string, unknown>[]): { $and: Record<string, unknown>[] } {
  return { $and: filters }
}

/**
 * Create a logical OR filter
 */
export function createOrFilter(...filters: Record<string, unknown>[]): { $or: Record<string, unknown>[] } {
  return { $or: filters }
}

// =============================================================================
// Binary Data Factories
// =============================================================================

/**
 * Create test binary data
 */
export function createTestData(content: string): Uint8Array {
  return new TextEncoder().encode(content)
}

/**
 * Decode binary data to string
 */
export function decodeData(data: Uint8Array): string {
  return new TextDecoder().decode(data)
}

/**
 * Create random binary data of specified size
 */
export function createRandomData(size: number): Uint8Array {
  const data = new Uint8Array(size)
  for (let i = 0; i < size; i++) {
    data[i] = Math.floor(Math.random() * 256)
  }
  return data
}

// =============================================================================
// Date Factories
// =============================================================================

/**
 * Create a date relative to now
 */
export function createRelativeDate(
  offset: number,
  unit: 'ms' | 's' | 'm' | 'h' | 'd' = 'd'
): Date {
  const multipliers = { ms: 1, s: 1000, m: 60000, h: 3600000, d: 86400000 }
  return new Date(Date.now() + offset * multipliers[unit])
}

/**
 * Create a date in the past
 */
export function createPastDate(days = 7): Date {
  return createRelativeDate(-days, 'd')
}

/**
 * Create a date in the future
 */
export function createFutureDate(days = 7): Date {
  return createRelativeDate(days, 'd')
}

// =============================================================================
// Test Directory Helpers
// =============================================================================

let testDirCounter = 0

/**
 * Generate a unique test directory name that's safe for parallel test execution.
 * Uses a combination of process ID, counter, and timestamp to ensure uniqueness.
 */
export function generateTestDirName(prefix = 'parquedb-test'): string {
  testDirCounter++
  const pid = typeof process !== 'undefined' ? process.pid : 0
  const timestamp = Date.now()
  const random = Math.random().toString(36).slice(2, 8)
  return `${prefix}-${pid}-${timestamp}-${testDirCounter}-${random}`
}

/**
 * Reset test directory counter (call in global test setup if needed)
 */
export function resetTestDirCounter(): void {
  testDirCounter = 0
}
