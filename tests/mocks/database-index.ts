/**
 * DatabaseIndex Mock Factory
 *
 * Provides mock implementations of DatabaseIndex service for testing.
 * These factories replace inline vi.fn() mocks with reusable, typed mock objects.
 */

import { vi, type Mock } from 'vitest'
import type { Visibility } from '../../src/types/visibility'

// =============================================================================
// Types
// =============================================================================

/**
 * Database metadata structure matching DatabaseInfo
 */
export interface MockDatabaseInfo {
  /** Unique database ID */
  id: string
  /** Human-readable name */
  name: string
  /** Description */
  description?: string | null | undefined
  /** R2 bucket name */
  bucket: string
  /** Path prefix within bucket */
  prefix?: string | undefined
  /** When database was created */
  createdAt: Date
  /** When database was last updated */
  updatedAt?: Date | undefined
  /** Who created the database */
  createdBy?: string | undefined
  /** Last access time */
  lastAccessedAt?: Date | undefined
  /** Number of entities */
  entityCount?: number | undefined
  /** Visibility level */
  visibility?: Visibility | undefined
  /** URL-friendly slug */
  slug?: string | undefined
  /** Owner username */
  owner?: string | undefined
  /** Custom metadata */
  metadata?: Record<string, unknown> | undefined
}

/**
 * Mock DatabaseIndex service interface
 */
export interface MockDatabaseIndex {
  list: Mock<[], Promise<MockDatabaseInfo[]>>
  get: Mock<[string], Promise<MockDatabaseInfo | null>>
  getBySlug: Mock<[string, string], Promise<MockDatabaseInfo | null>>
  getByName: Mock<[string], Promise<MockDatabaseInfo | null>>
  register: Mock<[Record<string, unknown>], Promise<MockDatabaseInfo>>
  unregister: Mock<[string], Promise<boolean>>
  update: Mock<[string, Record<string, unknown>], Promise<MockDatabaseInfo | null>>
  recordAccess: Mock<[string], Promise<void>>
  listPublic: Mock<[], Promise<MockDatabaseInfo[]>>

  // Test helpers
  _databases: Map<string, MockDatabaseInfo>
  _clear: () => void
  _addDatabase: (db: MockDatabaseInfo) => void
}

/**
 * Options for creating mock DatabaseIndex
 */
export interface MockDatabaseIndexOptions {
  /**
   * If true, returns a functional in-memory implementation.
   * If false (default), returns spy-only mocks.
   */
  functional?: boolean | undefined

  /**
   * Initial databases to populate
   */
  initialDatabases?: MockDatabaseInfo[] | undefined
}

// =============================================================================
// Default Test Data
// =============================================================================

/**
 * Create a test database info object with sensible defaults
 */
export function createTestDatabase(overrides: Partial<MockDatabaseInfo> = {}): MockDatabaseInfo {
  const id = overrides.id ?? `db_${Math.random().toString(36).slice(2, 10)}`
  return {
    id,
    name: overrides.name ?? 'Test Database',
    description: overrides.description ?? null,
    bucket: overrides.bucket ?? 'test-bucket',
    prefix: overrides.prefix ?? `${id}/`,
    createdAt: overrides.createdAt ?? new Date(),
    updatedAt: overrides.updatedAt ?? new Date(),
    createdBy: overrides.createdBy ?? 'users/user_123',
    visibility: overrides.visibility ?? 'private',
    slug: overrides.slug,
    owner: overrides.owner,
    ...overrides,
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a mock DatabaseIndex service
 *
 * @param options - Configuration options
 * @returns Mock DatabaseIndex instance
 *
 * @example
 * ```typescript
 * // Simple spy-based mock (default)
 * const index = createMockDatabaseIndex()
 * index.get.mockResolvedValue({ id: 'db_123', name: 'Test' })
 *
 * // Functional in-memory implementation
 * const index = createMockDatabaseIndex({
 *   functional: true,
 *   initialDatabases: [{ id: 'db_1', name: 'Production', bucket: 'prod' }]
 * })
 * ```
 */
export function createMockDatabaseIndex(options?: MockDatabaseIndexOptions): MockDatabaseIndex {
  const databases = new Map<string, MockDatabaseInfo>()

  // Initialize with provided data
  if (options?.initialDatabases) {
    for (const db of options.initialDatabases) {
      databases.set(db.id, db)
    }
  }

  if (options?.functional) {
    // Functional implementation
    return {
      _databases: databases,
      _clear: () => databases.clear(),
      _addDatabase: (db: MockDatabaseInfo) => databases.set(db.id, db),

      list: vi.fn(async (): Promise<MockDatabaseInfo[]> => {
        return Array.from(databases.values())
      }),

      get: vi.fn(async (id: string): Promise<MockDatabaseInfo | null> => {
        return databases.get(id) ?? null
      }),

      getBySlug: vi.fn(async (owner: string, slug: string): Promise<MockDatabaseInfo | null> => {
        for (const db of databases.values()) {
          if (db.owner === owner && db.slug === slug) {
            return db
          }
        }
        return null
      }),

      getByName: vi.fn(async (name: string): Promise<MockDatabaseInfo | null> => {
        for (const db of databases.values()) {
          if (db.name === name) {
            return db
          }
        }
        return null
      }),

      register: vi.fn(async (data: Record<string, unknown>): Promise<MockDatabaseInfo> => {
        const id = `db_${Math.random().toString(36).slice(2, 10)}`
        const db: MockDatabaseInfo = {
          id,
          name: data.name as string,
          description: data.description as string | undefined,
          bucket: data.bucket as string,
          prefix: data.prefix as string | undefined,
          createdAt: new Date(),
          visibility: (data.visibility as Visibility) ?? 'private',
          slug: data.slug as string | undefined,
          owner: data.owner as string | undefined,
        }
        databases.set(id, db)
        return db
      }),

      unregister: vi.fn(async (id: string): Promise<boolean> => {
        return databases.delete(id)
      }),

      update: vi.fn(async (id: string, data: Record<string, unknown>): Promise<MockDatabaseInfo | null> => {
        const db = databases.get(id)
        if (!db) return null

        const updated: MockDatabaseInfo = {
          ...db,
          ...data,
          updatedAt: new Date(),
        }
        databases.set(id, updated)
        return updated
      }),

      recordAccess: vi.fn(async (id: string): Promise<void> => {
        const db = databases.get(id)
        if (db) {
          db.lastAccessedAt = new Date()
        }
      }),

      listPublic: vi.fn(async (): Promise<MockDatabaseInfo[]> => {
        return Array.from(databases.values()).filter((db) => db.visibility === 'public')
      }),
    }
  }

  // Spy-based mock with sensible defaults
  return {
    _databases: databases,
    _clear: () => databases.clear(),
    _addDatabase: (db: MockDatabaseInfo) => databases.set(db.id, db),

    list: vi.fn().mockResolvedValue([]),
    get: vi.fn().mockResolvedValue(null),
    getBySlug: vi.fn().mockResolvedValue(null),
    getByName: vi.fn().mockResolvedValue(null),
    register: vi.fn().mockImplementation(async (data: Record<string, unknown>) => ({
      id: `db_${Math.random().toString(36).slice(2, 10)}`,
      name: data.name ?? 'New Database',
      bucket: data.bucket ?? 'test-bucket',
      createdAt: new Date(),
      visibility: 'private',
      ...data,
    })),
    unregister: vi.fn().mockResolvedValue(true),
    update: vi.fn().mockResolvedValue(null),
    recordAccess: vi.fn().mockResolvedValue(undefined),
    listPublic: vi.fn().mockResolvedValue([]),
  }
}

/**
 * Create a mock getDatabaseIndex function that returns the same index for any user
 *
 * @param index - Mock index to return (created if not provided)
 * @returns Mock getDatabaseIndex function
 */
export function createMockGetDatabaseIndex(
  index?: MockDatabaseIndex
): Mock<[string], Promise<MockDatabaseIndex>> {
  const mockIndex = index ?? createMockDatabaseIndex()
  return vi.fn().mockResolvedValue(mockIndex)
}

/**
 * Create a mock getUserDatabaseIndex function (alias for compatibility)
 */
export const createMockGetUserDatabaseIndex = createMockGetDatabaseIndex
