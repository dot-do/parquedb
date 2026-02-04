/**
 * E2E Prisma Client Integration Tests
 *
 * Tests the full Prisma Client integration with ParqueDB including:
 * - Schema integration
 * - CRUD operations
 * - Transactions
 * - Relations
 * - Query filters
 *
 * NO MOCKS - Uses real ParqueDB backends.
 *
 * @see parquedb-17q8 - Epic for Prisma E2E tests
 * @see parquedb-17q8.1 - RED: Write failing tests
 * @see parquedb-17q8.2 - GREEN: Implement tests
 * @see parquedb-17q8.3 - REFACTOR: Clean up implementation
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueDB } from '../../../src/ParqueDB'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { FsBackend } from '../../../src/storage/FsBackend'
import { createPrismaAdapter } from '../../../src/integrations/sql/prisma'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

// =============================================================================
// Test Configuration
// =============================================================================

/** Test timeout for slow operations */
const TEST_TIMEOUT = 5000

// =============================================================================
// Test Fixtures - User Data
// =============================================================================

interface UserFixture {
  name: string
  email: string
  age?: number
  active?: boolean
  role?: string
  settings?: string
}

/** Standard user fixtures for testing */
const USER_FIXTURES = {
  alice: { name: 'Alice', email: 'alice@test.com', age: 25, active: true, role: 'admin' },
  bob: { name: 'Bob', email: 'bob@test.com', age: 30, active: true, role: 'user' },
  charlie: { name: 'Charlie', email: 'charlie@test.com', age: 35, active: false, role: 'user' },
  diana: { name: 'Diana', email: 'diana@test.com', age: 28, active: true, role: 'moderator' },
  eve: { name: 'Eve', email: 'eve@test.com', age: 22, active: false, role: 'user' },
} as const satisfies Record<string, UserFixture>

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Helper to create a fresh test context with adapter
 */
function createTestContext(dbInstance: ParqueDB) {
  const adapter = createPrismaAdapter(dbInstance)
  return { adapter }
}

/**
 * Helper to insert a user via the adapter
 */
async function insertUser(
  adapter: ReturnType<typeof createPrismaAdapter>,
  user: UserFixture
): Promise<number> {
  const fields: string[] = ['name', 'email']
  const placeholders: string[] = ['$1', '$2']
  const args: unknown[] = [user.name, user.email]
  let paramIndex = 3

  if (user.age !== undefined) {
    fields.push('age')
    placeholders.push(`$${paramIndex++}`)
    args.push(user.age)
  }
  if (user.active !== undefined) {
    fields.push('active')
    placeholders.push(`$${paramIndex++}`)
    args.push(user.active)
  }
  if (user.role !== undefined) {
    fields.push('role')
    placeholders.push(`$${paramIndex++}`)
    args.push(user.role)
  }
  if (user.settings !== undefined) {
    fields.push('settings')
    placeholders.push(`$${paramIndex++}`)
    args.push(user.settings)
  }

  return adapter.executeRaw({
    sql: `INSERT INTO User (${fields.join(', ')}) VALUES (${placeholders.join(', ')})`,
    args,
  })
}

/**
 * Helper to seed multiple users from fixtures
 */
async function seedUsers(
  adapter: ReturnType<typeof createPrismaAdapter>,
  userKeys: Array<keyof typeof USER_FIXTURES>
): Promise<void> {
  for (const key of userKeys) {
    await insertUser(adapter, USER_FIXTURES[key])
  }
}

/**
 * Helper to seed all user fixtures
 */
async function seedAllUsers(adapter: ReturnType<typeof createPrismaAdapter>): Promise<void> {
  await seedUsers(adapter, Object.keys(USER_FIXTURES) as Array<keyof typeof USER_FIXTURES>)
}

/**
 * Extract a column value from a query result row
 */
function getColumnValue<T>(
  result: { columns: string[]; rows: unknown[][] },
  row: unknown[],
  columnName: string
): T {
  const index = result.columns.indexOf(columnName)
  if (index === -1) {
    throw new Error(`Column '${columnName}' not found in result`)
  }
  return row[index] as T
}

/**
 * Get the $id from a query result row
 */
function getEntityId(result: { columns: string[]; rows: unknown[][] }, rowIndex = 0): string {
  return getColumnValue<string>(result, result.rows[rowIndex] as unknown[], '$id')
}

// =============================================================================
// Test Suite
// =============================================================================

describe('Prisma Client Integration (E2E)', () => {
  let db: ParqueDB
  let storage: MemoryBackend

  beforeEach(() => {
    storage = new MemoryBackend()
    db = new ParqueDB({ storage })
  })

  afterEach(() => {
    // MemoryBackend is automatically cleaned up on re-instantiation
    // No explicit cleanup needed
  })

  // ===========================================================================
  // 1. Prisma Schema Integration
  // ===========================================================================

  describe('Prisma Schema Integration', () => {
    it('creates adapter with correct provider and name', () => {
      const { adapter } = createTestContext(db)

      expect(adapter.provider).toBe('sqlite')
      expect(adapter.adapterName).toBe('parquedb')
    })

    it('maps Prisma schema models to ParqueDB collections', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)

      const count = await insertUser(adapter, { name: 'Alice', email: 'alice@example.com' })

      expect(count).toBe(1)

      // Collection name should be lowercased
      const collection = db.collection('user')
      const users = await collection.find({})
      expect(users.items).toHaveLength(1)
    })

    it('handles Prisma model with multiple fields', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)

      // Simulate complex Prisma model (Post)
      await adapter.executeRaw({
        sql: "INSERT INTO Post (title, content, published, authorId) VALUES ($1, $2, $3, $4)",
        args: ['Test Post', 'Content here', true, 1],
      })

      const result = await adapter.queryRaw({
        sql: "SELECT * FROM Post WHERE title = $1",
        args: ['Test Post'],
      })

      expect(result.rows).toHaveLength(1)
      expect(result.columns).toContain('title')
      expect(result.columns).toContain('content')
    })

    it('supports Prisma @id field as ParqueDB $id', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)

      await insertUser(adapter, { name: 'Bob', email: 'bob@example.com' })

      const result = await adapter.queryRaw({
        sql: "SELECT * FROM User WHERE name = $1",
        args: ['Bob'],
      })

      expect(result.rows).toHaveLength(1)
      expect(result.columns).toContain('$id')
    })

    it('handles Prisma @unique constraint fields', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)

      await insertUser(adapter, { name: 'User1', email: 'unique@example.com' })

      const result = await adapter.queryRaw({
        sql: "SELECT * FROM User WHERE email = $1",
        args: ['unique@example.com'],
      })

      expect(result.rows).toHaveLength(1)
    })

    it('maps Prisma DateTime to ParqueDB Date handling', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)
      const now = new Date()

      await adapter.executeRaw({
        sql: "INSERT INTO Event (name, timestamp) VALUES ($1, $2)",
        args: ['Test Event', now.toISOString()],
      })

      const result = await adapter.queryRaw({
        sql: "SELECT * FROM Event WHERE name = $1",
        args: ['Test Event'],
      })

      expect(result.rows).toHaveLength(1)
      const timestampIndex = result.columns.indexOf('timestamp')
      expect(timestampIndex).toBeGreaterThanOrEqual(0)
    })
  })

  // ===========================================================================
  // 2. Prisma Client CRUD Operations
  // ===========================================================================

  describe('Prisma Client CRUD Operations', () => {
    describe('create', () => {
      it('creates single entity via Prisma-style INSERT', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const count = await insertUser(adapter, USER_FIXTURES.alice)

        expect(count).toBe(1)

        const result = await adapter.queryRaw({ sql: "SELECT * FROM User", args: [] })
        expect(result.rows).toHaveLength(1)
      })

      it('creates entity with nested data (JSON fields)', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)
        const settings = JSON.stringify({ theme: 'dark', notifications: true })

        await insertUser(adapter, { name: 'Bob', email: 'bob@test.com', settings })

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE name = $1",
          args: ['Bob'],
        })

        expect(result.rows).toHaveLength(1)
      })

      it('creates multiple entities in batch', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        await seedUsers(adapter, ['alice', 'bob', 'charlie'])

        const result = await adapter.queryRaw({ sql: "SELECT * FROM User", args: [] })
        expect(result.rows).toHaveLength(3)
      })

      it('returns created entity with RETURNING clause', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "INSERT INTO User (name, email) VALUES ($1, $2) RETURNING *",
          args: ['Charlie', 'charlie@test.com'],
        })

        expect(result.rows).toHaveLength(1)
        expect(result.columns).toContain('name')
      })
    })

    describe('read (findMany, findUnique, findFirst)', () => {
      beforeEach(async () => {
        const { adapter } = createTestContext(db)
        await seedUsers(adapter, ['alice', 'bob', 'charlie'])
      })

      it('finds all entities (findMany)', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({ sql: "SELECT * FROM User", args: [] })
        expect(result.rows).toHaveLength(3)
      })

      it('finds entity by unique field (findUnique)', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE email = $1 LIMIT 1",
          args: [USER_FIXTURES.alice.email],
        })

        expect(result.rows).toHaveLength(1)
      })

      it('finds first matching entity (findFirst)', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE active = $1 LIMIT 1",
          args: [true],
        })

        expect(result.rows).toHaveLength(1)
      })

      it('handles empty result sets', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE name = $1",
          args: ['NonExistent'],
        })

        expect(result.rows).toHaveLength(0)
      })

      it('supports select with specific columns', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({ sql: "SELECT name, email FROM User", args: [] })

        expect(result.columns).toContain('name')
        expect(result.columns).toContain('email')
        expect(result.columns).not.toContain('age')
      })
    })

    describe('update', () => {
      beforeEach(async () => {
        const { adapter } = createTestContext(db)
        await insertUser(adapter, USER_FIXTURES.alice)
      })

      it('updates single entity by unique field', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const count = await adapter.executeRaw({
          sql: "UPDATE User SET age = $1 WHERE email = $2",
          args: [26, USER_FIXTURES.alice.email],
        })

        expect(count).toBe(1)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE email = $1",
          args: [USER_FIXTURES.alice.email],
        })

        const ageValue = getColumnValue<number>(result, result.rows[0] as unknown[], 'age')
        expect(ageValue).toBe(26)
      })

      it('updates multiple fields at once', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        await adapter.executeRaw({
          sql: "UPDATE User SET name = $1, age = $2 WHERE email = $3",
          args: ['Alicia', 30, USER_FIXTURES.alice.email],
        })

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE email = $1",
          args: [USER_FIXTURES.alice.email],
        })

        const row = result.rows[0] as unknown[]
        expect(getColumnValue<string>(result, row, 'name')).toBe('Alicia')
        expect(getColumnValue<number>(result, row, 'age')).toBe(30)
      })

      it('updates multiple entities (updateMany)', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        // Add more users
        await insertUser(adapter, USER_FIXTURES.bob)
        await insertUser(adapter, USER_FIXTURES.charlie)

        const count = await adapter.executeRaw({
          sql: "UPDATE User SET active = $1 WHERE age > $2",
          args: [false, 25],
        })

        expect(count).toBe(2)
      })

      it('returns 0 when no entities match update criteria', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const count = await adapter.executeRaw({
          sql: "UPDATE User SET age = $1 WHERE name = $2",
          args: [99, 'NonExistent'],
        })

        expect(count).toBe(0)
      })
    })

    describe('delete', () => {
      beforeEach(async () => {
        const { adapter } = createTestContext(db)
        await seedUsers(adapter, ['alice', 'bob', 'charlie'])
      })

      it('deletes single entity by unique field', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const count = await adapter.executeRaw({
          sql: "DELETE FROM User WHERE email = $1",
          args: [USER_FIXTURES.alice.email],
        })

        expect(count).toBe(1)

        const result = await adapter.queryRaw({ sql: "SELECT * FROM User", args: [] })
        expect(result.rows).toHaveLength(2)
      })

      it('deletes multiple entities (deleteMany)', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const count = await adapter.executeRaw({
          sql: "DELETE FROM User WHERE name != $1",
          args: [USER_FIXTURES.alice.name],
        })

        expect(count).toBe(2)

        const result = await adapter.queryRaw({ sql: "SELECT * FROM User", args: [] })
        expect(result.rows).toHaveLength(1)
      })

      it('returns 0 when no entities match delete criteria', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const count = await adapter.executeRaw({
          sql: "DELETE FROM User WHERE name = $1",
          args: ['NonExistent'],
        })

        expect(count).toBe(0)
      })
    })
  })

  // ===========================================================================
  // 3. Transactions
  // ===========================================================================

  describe('Transactions', () => {
    it('starts a transaction and commits successfully', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)
      const tx = await adapter.startTransaction()

      await tx.executeRaw({
        sql: "INSERT INTO User (name, email) VALUES ($1, $2)",
        args: ['TxUser', 'txuser@test.com'],
      })

      await tx.commit()

      const result = await adapter.queryRaw({
        sql: "SELECT * FROM User WHERE name = $1",
        args: ['TxUser'],
      })

      expect(result.rows).toHaveLength(1)
    })

    it('rolls back transaction and discards changes', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)

      // Create a user first (outside transaction)
      await insertUser(adapter, { name: 'ExistingUser', email: 'existing@test.com' })

      const tx = await adapter.startTransaction()

      await tx.executeRaw({
        sql: "INSERT INTO User (name, email) VALUES ($1, $2)",
        args: ['RollbackUser', 'rollback@test.com'],
      })

      await tx.rollback()

      // Rollback user should NOT be visible
      const rollbackResult = await adapter.queryRaw({
        sql: "SELECT * FROM User WHERE name = $1",
        args: ['RollbackUser'],
      })
      expect(rollbackResult.rows).toHaveLength(0)

      // Existing user should still exist
      const existingResult = await adapter.queryRaw({
        sql: "SELECT * FROM User WHERE name = $1",
        args: ['ExistingUser'],
      })
      expect(existingResult.rows).toHaveLength(1)
    })

    it('supports multiple operations in a single transaction', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)
      const tx = await adapter.startTransaction()

      await tx.executeRaw({
        sql: "INSERT INTO User (name, email) VALUES ($1, $2)",
        args: ['User1', 'user1@test.com'],
      })

      await tx.executeRaw({
        sql: "INSERT INTO User (name, email) VALUES ($1, $2)",
        args: ['User2', 'user2@test.com'],
      })

      await tx.executeRaw({
        sql: "UPDATE User SET name = $1 WHERE email = $2",
        args: ['UpdatedUser1', 'user1@test.com'],
      })

      await tx.commit()

      const result = await adapter.queryRaw({
        sql: "SELECT * FROM User ORDER BY email",
        args: [],
      })

      expect(result.rows).toHaveLength(2)
    })

    it('prevents double commit', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)
      const tx = await adapter.startTransaction()

      await tx.executeRaw({
        sql: "INSERT INTO User (name, email) VALUES ($1, $2)",
        args: ['User1', 'user1@test.com'],
      })

      await tx.commit()

      await expect(tx.commit()).rejects.toThrow()
    })

    it('prevents commit after rollback', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)
      const tx = await adapter.startTransaction()

      await tx.executeRaw({
        sql: "INSERT INTO User (name, email) VALUES ($1, $2)",
        args: ['User1', 'user1@test.com'],
      })

      await tx.rollback()

      await expect(tx.commit()).rejects.toThrow()
    })

    it('prevents rollback after commit', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)
      const tx = await adapter.startTransaction()

      await tx.executeRaw({
        sql: "INSERT INTO User (name, email) VALUES ($1, $2)",
        args: ['User1', 'user1@test.com'],
      })

      await tx.commit()

      await expect(tx.rollback()).rejects.toThrow()
    })

    it('supports queryRaw within transaction', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)

      await insertUser(adapter, { name: 'PreExisting', email: 'pre@test.com' })

      const tx = await adapter.startTransaction()

      const result = await tx.queryRaw({
        sql: "SELECT * FROM User WHERE name = $1",
        args: ['PreExisting'],
      })

      expect(result.rows).toHaveLength(1)

      await tx.commit()
    })

    it('isolates transaction from other operations', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)
      const tx = await adapter.startTransaction()

      await tx.executeRaw({
        sql: "INSERT INTO User (name, email) VALUES ($1, $2)",
        args: ['TxUser', 'tx@test.com'],
      })

      // Query outside transaction should not see uncommitted data
      const outsideResult = await adapter.queryRaw({
        sql: "SELECT * FROM User WHERE name = $1",
        args: ['TxUser'],
      })

      expect(outsideResult.rows).toHaveLength(0)

      await tx.commit()
    })
  })

  // ===========================================================================
  // 4. Relations via Prisma
  // ===========================================================================

  describe('Relations', () => {
    describe('One-to-Many Relations', () => {
      it('creates parent with related children', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        // Create user (parent)
        await insertUser(adapter, { name: 'Author', email: 'author@test.com' })

        const userResult = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE email = $1",
          args: ['author@test.com'],
        })
        const userId = getEntityId(userResult)

        // Create posts (children) with foreign key
        await adapter.executeRaw({
          sql: "INSERT INTO Post (title, content, authorId) VALUES ($1, $2, $3)",
          args: ['Post 1', 'Content 1', userId],
        })
        await adapter.executeRaw({
          sql: "INSERT INTO Post (title, content, authorId) VALUES ($1, $2, $3)",
          args: ['Post 2', 'Content 2', userId],
        })

        const posts = await adapter.queryRaw({
          sql: "SELECT * FROM Post WHERE authorId = $1",
          args: [userId],
        })

        expect(posts.rows).toHaveLength(2)
      })

      it('queries parent with nested children (include)', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        await insertUser(adapter, { name: 'Author', email: 'author@test.com' })

        const userResult = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE email = $1",
          args: ['author@test.com'],
        })
        const userId = getEntityId(userResult)

        await adapter.executeRaw({
          sql: "INSERT INTO Post (title, authorId) VALUES ($1, $2)",
          args: ['Post 1', userId],
        })

        // Simulate Prisma include by querying relations separately
        const user = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE email = $1",
          args: ['author@test.com'],
        })
        const authorId = getEntityId(user)

        const userPosts = await adapter.queryRaw({
          sql: "SELECT * FROM Post WHERE authorId = $1",
          args: [authorId],
        })

        expect(userPosts.rows).toHaveLength(1)
      })
    })

    describe('Many-to-Many Relations', () => {
      it('creates entities with many-to-many relations via junction table', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        // Create posts
        await adapter.executeRaw({ sql: "INSERT INTO Post (title) VALUES ($1)", args: ['Post 1'] })
        await adapter.executeRaw({ sql: "INSERT INTO Post (title) VALUES ($1)", args: ['Post 2'] })

        // Create tags
        await adapter.executeRaw({ sql: "INSERT INTO Tag (name) VALUES ($1)", args: ['JavaScript'] })
        await adapter.executeRaw({ sql: "INSERT INTO Tag (name) VALUES ($1)", args: ['TypeScript'] })

        // Get IDs
        const posts = await adapter.queryRaw({ sql: "SELECT * FROM Post", args: [] })
        const tags = await adapter.queryRaw({ sql: "SELECT * FROM Tag", args: [] })

        const post1Id = getEntityId(posts, 0)
        const post2Id = getEntityId(posts, 1)
        const jsTagId = getEntityId(tags, 0)
        const tsTagId = getEntityId(tags, 1)

        // Create junction records (PostTag)
        await adapter.executeRaw({
          sql: "INSERT INTO PostTag (postId, tagId) VALUES ($1, $2)",
          args: [post1Id, jsTagId],
        })
        await adapter.executeRaw({
          sql: "INSERT INTO PostTag (postId, tagId) VALUES ($1, $2)",
          args: [post1Id, tsTagId],
        })
        await adapter.executeRaw({
          sql: "INSERT INTO PostTag (postId, tagId) VALUES ($1, $2)",
          args: [post2Id, tsTagId],
        })

        const post1Tags = await adapter.queryRaw({
          sql: "SELECT * FROM PostTag WHERE postId = $1",
          args: [post1Id],
        })

        expect(post1Tags.rows).toHaveLength(2)
      })
    })

    describe('One-to-One Relations', () => {
      it('creates entities with one-to-one relation', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        await insertUser(adapter, { name: 'User1', email: 'user1@test.com' })

        const userResult = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE email = $1",
          args: ['user1@test.com'],
        })
        const userId = getEntityId(userResult)

        // Create profile (one-to-one with user)
        await adapter.executeRaw({
          sql: "INSERT INTO Profile (bio, userId) VALUES ($1, $2)",
          args: ['A bio here', userId],
        })

        const profile = await adapter.queryRaw({
          sql: "SELECT * FROM Profile WHERE userId = $1",
          args: [userId],
        })

        expect(profile.rows).toHaveLength(1)
      })
    })

    describe('Self-Relations', () => {
      it('handles self-referential relations (e.g., followers)', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        await insertUser(adapter, { name: 'User1', email: 'user1@test.com' })
        await insertUser(adapter, { name: 'User2', email: 'user2@test.com' })

        const users = await adapter.queryRaw({ sql: "SELECT * FROM User", args: [] })
        const user1Id = getEntityId(users, 0)
        const user2Id = getEntityId(users, 1)

        // Create follow relationship
        await adapter.executeRaw({
          sql: "INSERT INTO Follow (followerId, followingId) VALUES ($1, $2)",
          args: [user1Id, user2Id],
        })

        const followers = await adapter.queryRaw({
          sql: "SELECT * FROM Follow WHERE followingId = $1",
          args: [user2Id],
        })

        expect(followers.rows).toHaveLength(1)
      })
    })
  })

  // ===========================================================================
  // 5. Prisma Query Filters
  // ===========================================================================

  describe('Prisma Query Filters', () => {
    beforeEach(async () => {
      const { adapter } = createTestContext(db)
      await seedAllUsers(adapter)
    })

    describe('Equality Filters', () => {
      it('filters by equals', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE name = $1",
          args: [USER_FIXTURES.alice.name],
        })

        expect(result.rows).toHaveLength(1)
      })

      it('filters by not equals', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE name != $1",
          args: [USER_FIXTURES.alice.name],
        })

        expect(result.rows).toHaveLength(4)
      })
    })

    describe('Comparison Filters', () => {
      it('filters by greater than', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE age > $1",
          args: [28],
        })

        expect(result.rows).toHaveLength(2) // Bob (30), Charlie (35)
      })

      it('filters by greater than or equal', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE age >= $1",
          args: [28],
        })

        expect(result.rows).toHaveLength(3) // Bob (30), Charlie (35), Diana (28)
      })

      it('filters by less than', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE age < $1",
          args: [28],
        })

        expect(result.rows).toHaveLength(2) // Alice (25), Eve (22)
      })

      it('filters by less than or equal', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE age <= $1",
          args: [28],
        })

        expect(result.rows).toHaveLength(3) // Alice (25), Diana (28), Eve (22)
      })
    })

    describe('IN Filter', () => {
      it('filters by IN list', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE role IN ('admin', 'moderator')",
          args: [],
        })

        expect(result.rows).toHaveLength(2) // Alice (admin), Diana (moderator)
      })

      it('filters by NOT IN list', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE role NOT IN ('admin', 'moderator')",
          args: [],
        })

        expect(result.rows).toHaveLength(3) // Bob, Charlie, Eve (all users)
      })
    })

    describe('String Filters', () => {
      it('filters by LIKE (contains)', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE email LIKE '%test.com'",
          args: [],
        })

        expect(result.rows).toHaveLength(5)
      })

      it('filters by LIKE (starts with)', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE name LIKE 'A%'",
          args: [],
        })

        expect(result.rows).toHaveLength(1) // Alice
      })

      it('filters by ILIKE (case-insensitive)', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE name ILIKE 'alice'",
          args: [],
        })

        expect(result.rows).toHaveLength(1)
      })
    })

    describe('NULL Filters', () => {
      it('filters by IS NULL', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        // Add a user with null email
        await adapter.executeRaw({
          sql: "INSERT INTO User (name, age) VALUES ($1, $2)",
          args: ['NoEmail', 40],
        })

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE email IS NULL",
          args: [],
        })

        expect(result.rows).toHaveLength(1)
      })

      it('filters by IS NOT NULL', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE email IS NOT NULL",
          args: [],
        })

        expect(result.rows).toHaveLength(5)
      })
    })

    describe('Boolean Filters', () => {
      it('filters by boolean true', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE active = $1",
          args: [true],
        })

        expect(result.rows).toHaveLength(3) // Alice, Bob, Diana
      })

      it('filters by boolean false', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE active = $1",
          args: [false],
        })

        expect(result.rows).toHaveLength(2) // Charlie, Eve
      })
    })

    describe('Logical Operators', () => {
      it('combines filters with AND', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE active = $1 AND age > $2",
          args: [true, 26],
        })

        expect(result.rows).toHaveLength(2) // Bob (30), Diana (28)
      })

      it('combines filters with OR', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE role = $1 OR role = $2",
          args: ['admin', 'moderator'],
        })

        expect(result.rows).toHaveLength(2) // Alice (admin), Diana (moderator)
      })

      it('handles complex AND/OR combinations', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User WHERE (active = $1 AND age > $2) OR role = $3",
          args: [true, 28, 'admin'],
        })

        // Active and age > 28: Bob (30)
        // OR role = admin: Alice
        expect(result.rows.length).toBeGreaterThanOrEqual(2)
      })
    })

    describe('ORDER BY', () => {
      it('orders by single field ascending', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User ORDER BY age ASC",
          args: [],
        })

        expect(result.rows).toHaveLength(5)
        // First should be Eve (22)
        const name = getColumnValue<string>(result, result.rows[0] as unknown[], 'name')
        expect(name).toBe('Eve')
      })

      it('orders by single field descending', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User ORDER BY age DESC",
          args: [],
        })

        expect(result.rows).toHaveLength(5)
        // First should be Charlie (35)
        const name = getColumnValue<string>(result, result.rows[0] as unknown[], 'name')
        expect(name).toBe('Charlie')
      })
    })

    describe('LIMIT and OFFSET (Pagination)', () => {
      it('limits results with LIMIT', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User LIMIT 3",
          args: [],
        })

        expect(result.rows).toHaveLength(3)
      })

      it('skips results with OFFSET', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const result = await adapter.queryRaw({
          sql: "SELECT * FROM User ORDER BY name ASC LIMIT 2 OFFSET 2",
          args: [],
        })

        expect(result.rows).toHaveLength(2)
      })

      it('supports cursor-based pagination pattern', { timeout: TEST_TIMEOUT }, async () => {
        const { adapter } = createTestContext(db)

        const page1 = await adapter.queryRaw({
          sql: "SELECT * FROM User ORDER BY name ASC LIMIT 2",
          args: [],
        })
        expect(page1.rows).toHaveLength(2)

        const page2 = await adapter.queryRaw({
          sql: "SELECT * FROM User ORDER BY name ASC LIMIT 2 OFFSET 2",
          args: [],
        })
        expect(page2.rows).toHaveLength(2)

        // Pages should have different data
        const page1Names = page1.rows.map((r) =>
          getColumnValue<string>(page1, r as unknown[], 'name')
        )
        const page2Names = page2.rows.map((r) =>
          getColumnValue<string>(page2, r as unknown[], 'name')
        )

        expect(page1Names).not.toEqual(page2Names)
      })
    })
  })

  // ===========================================================================
  // 6. Real Backend Tests (FsBackend)
  // ===========================================================================

  describe('With FsBackend (Persistent Storage)', () => {
    let tempDir: string
    let fsDb: ParqueDB

    beforeEach(async () => {
      tempDir = await mkdtemp(join(tmpdir(), 'parquedb-prisma-'))
      fsDb = new ParqueDB({ storage: new FsBackend(tempDir) })
    })

    afterEach(async () => {
      try {
        await rm(tempDir, { recursive: true, force: true })
      } catch {
        // Ignore cleanup errors in tests
      }
    })

    /**
     * Helper to create a fresh DB instance pointing to the same temp directory
     */
    function createFreshDbInstance(): ParqueDB {
      return new ParqueDB({ storage: new FsBackend(tempDir) })
    }

    it('persists data through Prisma adapter with FsBackend', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(fsDb)

      await insertUser(adapter, { name: 'PersistentUser', email: 'persistent@test.com' })

      // Create new DB instance pointing to same storage
      const newAdapter = createPrismaAdapter(createFreshDbInstance())

      const result = await newAdapter.queryRaw({
        sql: "SELECT * FROM User WHERE name = $1",
        args: ['PersistentUser'],
      })

      expect(result.rows).toHaveLength(1)
    })

    it('handles transaction commit with FsBackend', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(fsDb)
      const tx = await adapter.startTransaction()

      await tx.executeRaw({
        sql: "INSERT INTO User (name, email) VALUES ($1, $2)",
        args: ['TxUser', 'tx@test.com'],
      })

      await tx.commit()

      // Verify persistence with fresh instance
      const newAdapter = createPrismaAdapter(createFreshDbInstance())

      const result = await newAdapter.queryRaw({
        sql: "SELECT * FROM User WHERE name = $1",
        args: ['TxUser'],
      })

      expect(result.rows).toHaveLength(1)
    })

    it('handles transaction rollback with FsBackend', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(fsDb)

      // First create a committed user
      await insertUser(adapter, { name: 'CommittedUser', email: 'committed@test.com' })

      // Now start transaction and rollback
      const tx = await adapter.startTransaction()

      await tx.executeRaw({
        sql: "INSERT INTO User (name, email) VALUES ($1, $2)",
        args: ['RollbackUser', 'rollback@test.com'],
      })

      await tx.rollback()

      // Verify with fresh instance
      const newAdapter = createPrismaAdapter(createFreshDbInstance())

      // Rollback user should not exist
      const rollbackResult = await newAdapter.queryRaw({
        sql: "SELECT * FROM User WHERE name = $1",
        args: ['RollbackUser'],
      })
      expect(rollbackResult.rows).toHaveLength(0)

      // Committed user should exist
      const committedResult = await newAdapter.queryRaw({
        sql: "SELECT * FROM User WHERE name = $1",
        args: ['CommittedUser'],
      })
      expect(committedResult.rows).toHaveLength(1)
    })
  })

  // ===========================================================================
  // 7. Error Handling
  // ===========================================================================

  describe('Error Handling', () => {
    it('throws error for invalid SQL syntax', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)

      await expect(
        adapter.queryRaw({ sql: "INVALID SQL SYNTAX HERE", args: [] })
      ).rejects.toThrow()
    })

    it('throws error for queryRaw with non-SELECT statement', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)

      await expect(
        adapter.queryRaw({ sql: "INSERT INTO User (name) VALUES ($1)", args: ['Test'] })
      ).rejects.toThrow()
    })

    it('throws error for executeRaw with SELECT statement', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)

      await expect(
        adapter.executeRaw({ sql: "SELECT * FROM User", args: [] })
      ).rejects.toThrow()
    })

    it('handles missing parameter gracefully', { timeout: TEST_TIMEOUT }, async () => {
      const { adapter } = createTestContext(db)

      // Query with missing parameter - should handle gracefully
      const result = adapter.queryRaw({
        sql: "SELECT * FROM User WHERE name = $1 AND email = $2",
        args: ['Alice'], // Missing second arg
      })

      await expect(result).resolves.toBeDefined()
    })
  })

  // ===========================================================================
  // 8. Debug Mode
  // ===========================================================================

  describe('Debug Mode', () => {
    it('logs queries when debug is enabled', { timeout: TEST_TIMEOUT }, async () => {
      const logs: string[] = []
      const originalDebug = console.debug

      console.debug = (...args: unknown[]) => {
        logs.push(args.join(' '))
      }

      try {
        const adapter = createPrismaAdapter(db, { debug: true })

        await adapter.queryRaw({ sql: "SELECT * FROM User", args: [] })

        expect(logs.some((l) => l.includes('[prisma-parquedb]'))).toBe(true)
      } finally {
        console.debug = originalDebug
      }
    })
  })
})
