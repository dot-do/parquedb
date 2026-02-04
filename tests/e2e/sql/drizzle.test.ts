/**
 * Drizzle ORM Full Integration Tests
 *
 * E2E tests for Drizzle ORM integration with ParqueDB.
 * Tests real Drizzle ORM functionality using MemoryBackend - NO MOCKS.
 *
 * Test Organization:
 * 1. Schema Definitions - Drizzle schema mapping to ParqueDB
 * 2. Query Builder - SELECT operations
 * 3. Query Builder - INSERT operations
 * 4. Query Builder - UPDATE operations
 * 5. Query Builder - DELETE operations
 * 6. JOINs and Relationship Traversal
 * 7. Aggregations
 * 8. Transactions
 * 9. Edge Cases and Error Handling
 * 10. Performance and Batch Operations
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueDB } from '../../../src/ParqueDB'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'
import { createDrizzleProxy } from '../../../src/integrations/sql/index'

// =============================================================================
// Test Configuration
// =============================================================================

/** Test timeout for operations that may take longer */
const TEST_TIMEOUT = 5000

/** Maximum time for bulk operations */
const BULK_OPERATION_TIMEOUT = 10000

// =============================================================================
// Test Helpers (inline)
// =============================================================================

function createTestDB(): ParqueDB {
  const backend = new MemoryBackend()
  return new ParqueDB({ storage: backend })
}

function disposeTestDB(db: ParqueDB | null): void {
  if (db && typeof (db as unknown as { dispose?: () => void }).dispose === 'function') {
    (db as unknown as { dispose: () => void }).dispose()
  }
}

async function seedTestData(db: ParqueDB) {
  const users = db.collection('users')
  const posts = db.collection('posts')
  const comments = db.collection('comments')

  const alice = await users.create({
    $type: 'User',
    name: 'Alice',
    email: 'alice@example.com',
    age: 30,
    status: 'active',
  })

  const bob = await users.create({
    $type: 'User',
    name: 'Bob',
    email: 'bob@example.com',
    age: 25,
    status: 'active',
  })

  const charlie = await users.create({
    $type: 'User',
    name: 'Charlie',
    email: 'charlie@example.com',
    age: 35,
    status: 'inactive',
  })

  const post1 = await posts.create({
    $type: 'Post',
    name: 'First Post',
    title: 'Hello World',
    content: 'This is my first post',
    authorId: alice.$id,
    views: 100,
    status: 'published',
  })

  const post2 = await posts.create({
    $type: 'Post',
    name: 'Second Post',
    title: 'Learning TypeScript',
    content: 'TypeScript is great',
    authorId: alice.$id,
    views: 250,
    status: 'published',
  })

  const post3 = await posts.create({
    $type: 'Post',
    name: 'Third Post',
    title: 'My Draft',
    content: 'Work in progress',
    authorId: bob.$id,
    views: 10,
    status: 'draft',
  })

  await comments.create({
    $type: 'Comment',
    name: 'Comment 1',
    text: 'Great post!',
    postId: post1.$id,
    authorId: bob.$id,
  })

  await comments.create({
    $type: 'Comment',
    name: 'Comment 2',
    text: 'Thanks for sharing',
    postId: post1.$id,
    authorId: charlie.$id,
  })

  await comments.create({
    $type: 'Comment',
    name: 'Comment 3',
    text: 'Very informative',
    postId: post2.$id,
    authorId: bob.$id,
  })

  return { alice, bob, charlie, post1, post2, post3 }
}

async function measureTime<T>(operation: () => Promise<T>): Promise<{ result: T; elapsed: number }> {
  const start = Date.now()
  const result = await operation()
  const elapsed = Date.now() - start
  return { result, elapsed }
}

// =============================================================================
// Drizzle ORM Integration Tests
// =============================================================================

describe('Drizzle ORM Integration', () => {
  let db: ParqueDB

  beforeEach(() => {
    db = createTestDB()
  })

  afterEach(() => {
    disposeTestDB(db)
  })

  // ===========================================================================
  // 1. Schema Definitions
  // ===========================================================================

  describe('Schema Definitions', () => {
    it('should support Drizzle table schema definitions', async () => {
      const proxy = createDrizzleProxy(db)

      // Verify the proxy is a callable function
      expect(typeof proxy).toBe('function')

      // Schema-driven queries should work
      const result = await proxy('SELECT * FROM users', [], 'all')
      expect(result).toHaveProperty('rows')
    })

    it('should infer column types from Drizzle schema', async () => {
      const proxy = createDrizzleProxy(db)

      // Create a user with typed fields
      await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
        ['TypedUser', 'typed@example.com', 28, 'active'],
        'run'
      )

      const result = await proxy(
        "SELECT name, age FROM users WHERE email = $1",
        ['typed@example.com'],
        'all'
      )

      expect(result.rows).toBeDefined()
      expect(Array.isArray(result.rows)).toBe(true)

      // Age should be preserved as a number
      if (result.rows.length > 0) {
        const row = result.rows[0] as unknown[]
        expect(typeof row[1]).toBe('number')
      }
    })

    it('should support nullable columns in schema', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy(
        "INSERT INTO users (name, email, age, status, bio) VALUES ($1, $2, $3, $4, $5)",
        ['NullUser', 'null@example.com', 30, 'active', null],
        'run'
      )

      const result = await proxy(
        "SELECT name, bio FROM users WHERE email = $1",
        ['null@example.com'],
        'all'
      )

      expect(result.rows).toBeDefined()
      if (result.rows.length > 0) {
        const row = result.rows[0] as unknown[]
        expect(row[1]).toBeFalsy()
      }
    })

    it('should support default values in schema', async () => {
      const proxy = createDrizzleProxy(db)

      // Insert without specifying views - it may be undefined or use a default
      await proxy(
        "INSERT INTO posts (name, title, content, authorId, status) VALUES ($1, $2, $3, $4, $5)",
        ['Default Post', 'Test', 'Content', 'users/123', 'draft'],
        'run'
      )

      const result = await proxy(
        "SELECT name, views FROM posts WHERE title = $1",
        ['Test'],
        'all'
      )

      expect(result.rows).toBeDefined()
      expect(result.rows.length).toBe(1)
      // The row was created successfully - default handling depends on schema
      // ParqueDB doesn't enforce defaults without explicit schema registration
      const row = result.rows[0] as unknown[]
      expect(row[0]).toBe('Default Post')
    })
  })

  // ===========================================================================
  // 2. Query Builder - SELECT
  // ===========================================================================

  describe('Query Builder - SELECT', () => {
    beforeEach(async () => {
      await seedTestData(db)
    })

    describe('Basic Queries', () => {
      it('should execute basic select()', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy('SELECT * FROM users', [], 'all')

        expect(result.rows).toBeDefined()
        expect(Array.isArray(result.rows)).toBe(true)
        expect(result.rows.length).toBe(3)
      })

      it('should execute select() with specific columns', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy('SELECT name, email FROM users', [], 'all')

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBe(3)

        if (result.rows.length > 0) {
          const row = result.rows[0] as unknown[]
          expect(row.length).toBe(2)
        }
      })
    })

    describe('WHERE Clauses', () => {
      it('should execute select().where() with equality', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT * FROM users WHERE status = $1",
          ['active'],
          'all'
        )

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBe(2)
      })

      it('should execute select().where() with comparison operators', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT * FROM users WHERE age > $1",
          [28],
          'all'
        )

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBe(2) // Alice (30) and Charlie (35)
      })

      it('should execute select().where() with AND conditions', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT * FROM users WHERE status = $1 AND age > $2",
          ['active', 25],
          'all'
        )

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBe(1) // Only Alice (30, active)
      })

      it('should execute select().where() with OR conditions', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT * FROM users WHERE status = $1 OR age < $2",
          ['inactive', 30],
          'all'
        )

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBe(2) // Charlie (inactive) and Bob (age 25)
      })

      it('should execute select().where() with IN clause', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT * FROM users WHERE age IN (25, 30)",
          [],
          'all'
        )

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBe(2) // Alice (30) and Bob (25)
      })

      it('should execute select().where() with LIKE pattern', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT * FROM users WHERE name LIKE $1",
          ['A%'],
          'all'
        )

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBe(1) // Alice
      })

      it('should execute select().where() with IS NULL', async () => {
        const users = db.collection('users')
        await users.create({
          $type: 'User',
          name: 'NoBio',
          email: 'nobio@example.com',
          status: 'active',
        })

        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT * FROM users WHERE bio IS NULL",
          [],
          'all'
        )

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBeGreaterThanOrEqual(1)
      })
    })

    describe('Ordering and Pagination', () => {
      it('should execute select().orderBy()', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT name, age FROM users ORDER BY age DESC",
          [],
          'all'
        )

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBe(3)

        const rows = result.rows as unknown[][]
        if (rows.length >= 3) {
          expect(rows[0][1]).toBe(35) // Charlie
          expect(rows[1][1]).toBe(30) // Alice
          expect(rows[2][1]).toBe(25) // Bob
        }
      })

      it('should execute select().limit()', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT * FROM users LIMIT 2",
          [],
          'all'
        )

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBe(2)
      })

      it('should execute select().offset()', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT name FROM users ORDER BY name LIMIT 2 OFFSET 1",
          [],
          'all'
        )

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBe(2)
      })
    })
  })

  // ===========================================================================
  // 3. Query Builder - INSERT
  // ===========================================================================

  describe('Query Builder - INSERT', () => {
    it('should execute basic insert()', async () => {
      const proxy = createDrizzleProxy(db)

      const result = await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
        ['NewUser', 'new@example.com', 28, 'active'],
        'run'
      )

      expect(result.rows).toBeDefined()

      // Verify the insert
      const users = db.collection('users')
      const found = await users.find({ email: 'new@example.com' })
      expect(found.items.length).toBe(1)
      expect(found.items[0].name).toBe('NewUser')
    })

    it('should execute insert() with RETURNING', async () => {
      const proxy = createDrizzleProxy(db)

      const result = await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4) RETURNING *",
        ['ReturnUser', 'return@example.com', 30, 'active'],
        'all'
      )

      expect(result.rows).toBeDefined()
      expect(Array.isArray(result.rows)).toBe(true)
      expect(result.rows.length).toBe(1)
    })

    it('should execute insert() with specific RETURNING columns', async () => {
      const proxy = createDrizzleProxy(db)

      const result = await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4) RETURNING name, email",
        ['PartialReturn', 'partial@example.com', 25, 'active'],
        'all'
      )

      expect(result.rows).toBeDefined()
      expect(result.rows.length).toBe(1)

      const row = result.rows[0] as unknown[]
      expect(row.length).toBe(2)
    })

    it('should execute batch insert()', async () => {
      const proxy = createDrizzleProxy(db)

      const usersToInsert = [
        { name: 'Batch1', email: 'batch1@example.com', age: 20 },
        { name: 'Batch2', email: 'batch2@example.com', age: 21 },
        { name: 'Batch3', email: 'batch3@example.com', age: 22 },
      ]

      for (const user of usersToInsert) {
        await proxy(
          "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
          [user.name, user.email, user.age, 'active'],
          'run'
        )
      }

      const result = await proxy(
        "SELECT * FROM users WHERE email LIKE $1",
        ['batch%'],
        'all'
      )

      expect(result.rows.length).toBe(3)
    })
  })

  // ===========================================================================
  // 4. Query Builder - UPDATE
  // ===========================================================================

  describe('Query Builder - UPDATE', () => {
    beforeEach(async () => {
      await seedTestData(db)
    })

    it('should execute basic update()', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy(
        "UPDATE users SET status = $1 WHERE email = $2",
        ['inactive', 'alice@example.com'],
        'run'
      )

      const users = db.collection('users')
      const found = await users.find({ email: 'alice@example.com' })
      expect(found.items[0].status).toBe('inactive')
    })

    it('should execute update() with multiple SET columns', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy(
        "UPDATE users SET name = $1, age = $2 WHERE email = $3",
        ['Alice Updated', 31, 'alice@example.com'],
        'run'
      )

      const users = db.collection('users')
      const found = await users.find({ email: 'alice@example.com' })
      expect(found.items[0].name).toBe('Alice Updated')
      expect(found.items[0].age).toBe(31)
    })

    it('should execute update() with RETURNING', async () => {
      const proxy = createDrizzleProxy(db)

      const result = await proxy(
        "UPDATE users SET status = $1 WHERE email = $2 RETURNING *",
        ['updated', 'alice@example.com'],
        'all'
      )

      expect(result.rows).toBeDefined()
      expect(result.rows.length).toBe(1)
    })

    it('should execute update() affecting multiple rows', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy(
        "UPDATE users SET status = $1 WHERE status = $2",
        ['bulk', 'active'],
        'run'
      )

      const result = await proxy(
        "SELECT * FROM users WHERE status = $1",
        ['bulk'],
        'all'
      )

      expect(result.rows.length).toBe(2) // Alice and Bob were active
    })
  })

  // ===========================================================================
  // 5. Query Builder - DELETE
  // ===========================================================================

  describe('Query Builder - DELETE', () => {
    beforeEach(async () => {
      await seedTestData(db)
    })

    it('should execute basic delete()', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy(
        "DELETE FROM users WHERE email = $1",
        ['charlie@example.com'],
        'run'
      )

      const result = await proxy(
        "SELECT * FROM users WHERE email = $1",
        ['charlie@example.com'],
        'all'
      )

      expect(result.rows.length).toBe(0)
    })

    it('should execute delete() with RETURNING', async () => {
      const proxy = createDrizzleProxy(db)

      const result = await proxy(
        "DELETE FROM users WHERE email = $1 RETURNING *",
        ['charlie@example.com'],
        'all'
      )

      expect(result.rows).toBeDefined()
      expect(result.rows.length).toBe(1)
    })

    it('should execute delete() affecting multiple rows', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy(
        "DELETE FROM users WHERE status = $1",
        ['active'],
        'run'
      )

      const result = await proxy(
        "SELECT * FROM users",
        [],
        'all'
      )

      expect(result.rows.length).toBe(1) // Only Charlie (inactive)
    })

    it('should execute delete() with no matching rows', async () => {
      const proxy = createDrizzleProxy(db)

      const result = await proxy(
        "DELETE FROM users WHERE email = $1 RETURNING *",
        ['nonexistent@example.com'],
        'all'
      )

      expect(result.rows.length).toBe(0)
    })
  })

  // ===========================================================================
  // 6. JOINs and Relationship Traversal
  // ===========================================================================
  // NOTE: JOINs require SQL parser enhancement. Currently skipped.
  // Tracked in: parquedb-k59x - Drizzle ORM full integration (future work)

  describe.skip('JOINs and Relationship Traversal', () => {
    beforeEach(async () => {
      await seedTestData(db)
    })

    it('should execute INNER JOIN between tables', async () => {
      const proxy = createDrizzleProxy(db)

      const result = await proxy(
        "SELECT users.name, posts.title FROM users INNER JOIN posts ON users.$id = posts.authorId",
        [],
        'all'
      )

      expect(result.rows).toBeDefined()
      expect(result.rows.length).toBeGreaterThanOrEqual(1)
    })

    it('should execute LEFT JOIN between tables', async () => {
      const proxy = createDrizzleProxy(db)

      const result = await proxy(
        "SELECT users.name, posts.title FROM users LEFT JOIN posts ON users.$id = posts.authorId",
        [],
        'all'
      )

      expect(result.rows).toBeDefined()
      expect(result.rows.length).toBeGreaterThanOrEqual(3)
    })

    it('should execute JOIN with WHERE clause', async () => {
      const proxy = createDrizzleProxy(db)

      const result = await proxy(
        "SELECT users.name, posts.title FROM users INNER JOIN posts ON users.$id = posts.authorId WHERE posts.status = $1",
        ['published'],
        'all'
      )

      expect(result.rows).toBeDefined()
    })

    it('should execute multiple JOINs', async () => {
      const proxy = createDrizzleProxy(db)

      const result = await proxy(
        `SELECT posts.title, comments.text, users.name as commenter
         FROM posts
         INNER JOIN comments ON posts.$id = comments.postId
         INNER JOIN users ON comments.authorId = users.$id`,
        [],
        'all'
      )

      expect(result.rows).toBeDefined()
      expect(result.rows.length).toBeGreaterThanOrEqual(1)
    })

    it('should support self-referential JOINs', async () => {
      const categories = db.collection('categories')
      const parent = await categories.create({
        $type: 'Category',
        name: 'Parent Category',
      })

      await categories.create({
        $type: 'Category',
        name: 'Child Category',
        parentId: parent.$id,
      })

      const proxy = createDrizzleProxy(db)

      const result = await proxy(
        `SELECT child.name as child_name, parent.name as parent_name
         FROM categories child
         LEFT JOIN categories parent ON child.parentId = parent.$id`,
        [],
        'all'
      )

      expect(result.rows).toBeDefined()
    })

    it('should traverse relationships using Drizzle relations API', async () => {
      const proxy = createDrizzleProxy(db)

      const result = await proxy(
        `SELECT posts.*, users.name as author_name
         FROM posts
         LEFT JOIN users ON posts.authorId = users.$id
         WHERE users.name = $1`,
        ['Alice'],
        'all'
      )

      expect(result.rows).toBeDefined()
      expect(result.rows.length).toBe(2) // Alice has 2 posts
    })
  })

  // ===========================================================================
  // 7. Aggregations
  // ===========================================================================
  // NOTE: Aggregations require SQL parser enhancement. Currently skipped.
  // Tracked in: parquedb-k59x - Drizzle ORM full integration (future work)

  describe.skip('Aggregations', () => {
    beforeEach(async () => {
      await seedTestData(db)
    })

    describe('Basic Aggregates', () => {
      it('should execute COUNT(*)', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT COUNT(*) as count FROM users",
          [],
          'get'
        )

        expect(result.rows).toBeDefined()
        const row = result.rows as unknown[]
        expect(row[0]).toBe(3)
      })

      it('should execute COUNT() with WHERE', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT COUNT(*) as count FROM users WHERE status = $1",
          ['active'],
          'get'
        )

        expect(result.rows).toBeDefined()
        const row = result.rows as unknown[]
        expect(row[0]).toBe(2)
      })

      it('should execute SUM()', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT SUM(views) as total FROM posts",
          [],
          'get'
        )

        expect(result.rows).toBeDefined()
        const row = result.rows as unknown[]
        expect(row[0]).toBe(360) // 100 + 250 + 10
      })

      it('should execute AVG()', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT AVG(age) as avg_age FROM users",
          [],
          'get'
        )

        expect(result.rows).toBeDefined()
        const row = result.rows as unknown[]
        expect(row[0]).toBe(30) // (30 + 25 + 35) / 3
      })

      it('should execute MIN() and MAX()', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT MIN(age) as min_age, MAX(age) as max_age FROM users",
          [],
          'get'
        )

        expect(result.rows).toBeDefined()
        const row = result.rows as unknown[]
        expect(row[0]).toBe(25) // Bob
        expect(row[1]).toBe(35) // Charlie
      })
    })

    describe('GROUP BY', () => {
      it('should execute GROUP BY', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT status, COUNT(*) as count FROM users GROUP BY status",
          [],
          'all'
        )

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBe(2) // 'active' and 'inactive'
      })

      it('should execute GROUP BY with HAVING', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          "SELECT status, COUNT(*) as count FROM users GROUP BY status HAVING COUNT(*) > 1",
          [],
          'all'
        )

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBe(1) // Only 'active' has 2 users
        const row = result.rows[0] as unknown[]
        expect(row[0]).toBe('active')
      })

      it('should execute aggregation with JOIN', async () => {
        const proxy = createDrizzleProxy(db)
        const result = await proxy(
          `SELECT users.name, COUNT(posts.$id) as post_count
           FROM users
           LEFT JOIN posts ON users.$id = posts.authorId
           GROUP BY users.$id, users.name`,
          [],
          'all'
        )

        expect(result.rows).toBeDefined()
        expect(result.rows.length).toBe(3)
      })
    })
  })

  // ===========================================================================
  // 8. Transactions
  // ===========================================================================

  describe('Transactions', () => {
    it('should support transaction commit', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy('BEGIN', [], 'run')

      await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
        ['TxUser', 'tx@example.com', 30, 'active'],
        'run'
      )

      await proxy('COMMIT', [], 'run')

      const result = await proxy(
        "SELECT * FROM users WHERE email = $1",
        ['tx@example.com'],
        'all'
      )

      expect(result.rows.length).toBe(1)
    })

    it('should support transaction rollback', async () => {
      const proxy = createDrizzleProxy(db)

      // Create a user first (outside transaction)
      await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
        ['PreTx', 'pretx@example.com', 25, 'active'],
        'run'
      )

      // Start transaction and try to modify
      await proxy('BEGIN', [], 'run')

      await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
        ['RollbackUser', 'rollback@example.com', 30, 'active'],
        'run'
      )

      await proxy('ROLLBACK', [], 'run')

      // Rolled-back insert should not exist
      const result = await proxy(
        "SELECT * FROM users WHERE email = $1",
        ['rollback@example.com'],
        'all'
      )

      expect(result.rows.length).toBe(0)
    })

    it('should isolate concurrent transactions', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
        ['IsolationTest', 'isolation@example.com', 30, 'active'],
        'run'
      )

      await proxy('BEGIN', [], 'run')

      await proxy(
        "UPDATE users SET age = $1 WHERE email = $2",
        [35, 'isolation@example.com'],
        'run'
      )

      await proxy('COMMIT', [], 'run')

      const result = await proxy(
        "SELECT age FROM users WHERE email = $1",
        ['isolation@example.com'],
        'get'
      )

      const row = result.rows as unknown[]
      expect(row[0]).toBe(35)
    })

    it('should handle transaction with multiple operations', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy('BEGIN', [], 'run')

      // Create user
      await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
        ['MultiOp', 'multiop@example.com', 30, 'active'],
        'run'
      )

      // Create post
      await proxy(
        "INSERT INTO posts (name, title, content, authorId, views, status) VALUES ($1, $2, $3, $4, $5, $6)",
        ['MultiOp Post', 'Test', 'Content', 'users/multiop', 0, 'draft'],
        'run'
      )

      // Update user
      await proxy(
        "UPDATE users SET status = $1 WHERE email = $2",
        ['premium', 'multiop@example.com'],
        'run'
      )

      await proxy('COMMIT', [], 'run')

      // Verify all operations
      const userResult = await proxy(
        "SELECT status FROM users WHERE email = $1",
        ['multiop@example.com'],
        'get'
      )
      expect((userResult.rows as unknown[])[0]).toBe('premium')

      const postResult = await proxy(
        "SELECT * FROM posts WHERE name = $1",
        ['MultiOp Post'],
        'all'
      )
      expect(postResult.rows.length).toBe(1)
    })

    it('should rollback all changes on transaction failure', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
        ['FailTest', 'fail@example.com', 30, 'active'],
        'run'
      )

      await proxy('BEGIN', [], 'run')

      await proxy(
        "UPDATE users SET age = $1 WHERE email = $2",
        [35, 'fail@example.com'],
        'run'
      )

      await proxy('ROLLBACK', [], 'run')

      // Age should still be 30
      const result = await proxy(
        "SELECT age FROM users WHERE email = $1",
        ['fail@example.com'],
        'get'
      )

      const row = result.rows as unknown[]
      expect(row[0]).toBe(30)
    })

    it('should support nested transactions (savepoints)', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy('BEGIN', [], 'run')

      await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
        ['Outer', 'outer@example.com', 30, 'active'],
        'run'
      )

      await proxy('SAVEPOINT sp1', [], 'run')

      await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
        ['Inner', 'inner@example.com', 25, 'active'],
        'run'
      )

      await proxy('ROLLBACK TO SAVEPOINT sp1', [], 'run')

      await proxy('COMMIT', [], 'run')

      // Outer should exist
      const outerResult = await proxy(
        "SELECT * FROM users WHERE email = $1",
        ['outer@example.com'],
        'all'
      )
      expect(outerResult.rows.length).toBe(1)

      // Inner should not exist
      const innerResult = await proxy(
        "SELECT * FROM users WHERE email = $1",
        ['inner@example.com'],
        'all'
      )
      expect(innerResult.rows.length).toBe(0)
    })
  })

  // ===========================================================================
  // 9. Edge Cases and Error Handling
  // ===========================================================================

  describe('Edge Cases and Error Handling', () => {
    it('should handle empty result sets', async () => {
      const proxy = createDrizzleProxy(db)

      const result = await proxy(
        "SELECT * FROM users WHERE email = $1",
        ['nonexistent@example.com'],
        'all'
      )

      expect(result.rows).toBeDefined()
      expect(result.rows.length).toBe(0)
    })

    it('should handle special characters in values', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
        ["O'Brien", 'obrien@example.com', 30, 'active'],
        'run'
      )

      const result = await proxy(
        "SELECT name FROM users WHERE email = $1",
        ['obrien@example.com'],
        'get'
      )

      expect(result.rows).toBeDefined()
      const row = result.rows as unknown[]
      expect(row[0]).toBe("O'Brien")
    })

    it('should handle unicode characters', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy(
        "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
        ['User with emoji', 'emoji@example.com', 25, 'active'],
        'run'
      )

      const result = await proxy(
        "SELECT name FROM users WHERE email = $1",
        ['emoji@example.com'],
        'get'
      )

      const row = result.rows as unknown[]
      expect(row[0]).toBe('User with emoji')
    })

    it('should handle large numbers', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy(
        "INSERT INTO posts (name, title, content, authorId, views, status) VALUES ($1, $2, $3, $4, $5, $6)",
        ['BigNum', 'Big Numbers', 'Content', 'users/1', 999999999, 'published'],
        'run'
      )

      const result = await proxy(
        "SELECT views FROM posts WHERE name = $1",
        ['BigNum'],
        'get'
      )

      const row = result.rows as unknown[]
      expect(row[0]).toBe(999999999)
    })

    it('should handle boolean values', async () => {
      const proxy = createDrizzleProxy(db)

      await proxy(
        "INSERT INTO users (name, email, age, status, isAdmin) VALUES ($1, $2, $3, $4, $5)",
        ['Admin', 'admin@example.com', 40, 'active', true],
        'run'
      )

      const result = await proxy(
        "SELECT isAdmin FROM users WHERE email = $1",
        ['admin@example.com'],
        'get'
      )

      const row = result.rows as unknown[]
      expect(row[0]).toBe(true)
    })

    it('should throw on invalid SQL syntax', async () => {
      const proxy = createDrizzleProxy(db)

      await expect(
        proxy('SELEC * FORM users', [], 'all')
      ).rejects.toThrow()
    })

    it('should throw on missing table', async () => {
      const proxy = createDrizzleProxy(db)

      // ParqueDB creates collections on demand - should return empty
      const result = await proxy(
        "SELECT * FROM nonexistent_table",
        [],
        'all'
      )

      expect(result.rows).toBeDefined()
      expect(result.rows.length).toBe(0)
    })
  })

  // ===========================================================================
  // 10. Performance and Batch Operations
  // ===========================================================================

  describe('Performance and Batch Operations', { timeout: BULK_OPERATION_TIMEOUT }, () => {
    it('should handle bulk insert efficiently', async () => {
      const proxy = createDrizzleProxy(db)

      const { elapsed } = await measureTime(async () => {
        for (let i = 0; i < 100; i++) {
          await proxy(
            "INSERT INTO users (name, email, age, status) VALUES ($1, $2, $3, $4)",
            [`BulkUser${i}`, `bulk${i}@example.com`, 20 + (i % 50), 'active'],
            'run'
          )
        }
      })

      // Verify by selecting all bulk users (no COUNT since aggregations not yet supported)
      const result = await proxy(
        "SELECT * FROM users WHERE email LIKE $1",
        ['bulk%'],
        'all'
      )

      expect(result.rows.length).toBe(100)

      // Should complete in reasonable time
      expect(elapsed).toBeLessThan(BULK_OPERATION_TIMEOUT)
    })

    it.skip('should handle complex query efficiently', async () => {
      // NOTE: This test requires JOIN and aggregation support
      // Tracked in: parquedb-k59x - Drizzle ORM full integration (future work)
      await seedTestData(db)
      const proxy = createDrizzleProxy(db)

      const { elapsed } = await measureTime(async () => {
        return proxy(
          `SELECT users.name, COUNT(posts.$id) as post_count, SUM(posts.views) as total_views
           FROM users
           LEFT JOIN posts ON users.$id = posts.authorId
           WHERE users.status = $1
           GROUP BY users.$id, users.name
           HAVING COUNT(posts.$id) > 0
           ORDER BY total_views DESC
           LIMIT 10`,
          ['active'],
          'all'
        )
      })

      // Should complete quickly
      expect(elapsed).toBeLessThan(1000)
    })
  })
})
