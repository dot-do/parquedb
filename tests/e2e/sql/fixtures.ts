/**
 * SQL Integration Test Fixtures and Helpers
 *
 * Shared test utilities for Drizzle and Prisma integration tests.
 * Provides consistent test data, helper functions, and cleanup utilities.
 */

import { ParqueDB } from '../../../src/ParqueDB'
import { MemoryBackend } from '../../../src/storage/MemoryBackend'

// =============================================================================
// Types
// =============================================================================

/**
 * Test user data structure
 */
export interface TestUser {
  name: string
  email: string
  age?: number
  status?: 'active' | 'inactive'
  bio?: string | null
  isAdmin?: boolean
}

/**
 * Test post data structure
 */
export interface TestPost {
  name: string
  title: string
  content?: string
  authorId: string
  views?: number
  status?: 'draft' | 'published'
}

/**
 * Test comment data structure
 */
export interface TestComment {
  name: string
  text: string
  postId: string
  authorId: string
}

/**
 * Seeded test data result
 */
export interface SeededTestData {
  alice: { $id: string; name: string; email: string; age: number; status: string }
  bob: { $id: string; name: string; email: string; age: number; status: string }
  charlie: { $id: string; name: string; email: string; age: number; status: string }
  post1: { $id: string; name: string; title: string; authorId: string; views: number; status: string }
  post2: { $id: string; name: string; title: string; authorId: string; views: number; status: string }
  post3: { $id: string; name: string; title: string; authorId: string; views: number; status: string }
}

// =============================================================================
// Database Setup Helpers
// =============================================================================

/**
 * Creates a fresh ParqueDB instance with MemoryBackend for isolated testing.
 * Each test should call this to ensure complete isolation.
 */
export function createTestDB(): ParqueDB {
  const backend = new MemoryBackend()
  return new ParqueDB({ storage: backend })
}

/**
 * Disposes a ParqueDB instance if it has a dispose method.
 * Safe to call even if db is null or doesn't have dispose.
 */
export function disposeTestDB(db: ParqueDB | null): void {
  if (db && typeof (db as unknown as { dispose?: () => void }).dispose === 'function') {
    (db as unknown as { dispose: () => void }).dispose()
  }
}

// =============================================================================
// Test Data Fixtures
// =============================================================================

/**
 * Standard test users used across SQL integration tests
 */
export const TEST_USERS: TestUser[] = [
  { name: 'Alice', email: 'alice@example.com', age: 30, status: 'active' },
  { name: 'Bob', email: 'bob@example.com', age: 25, status: 'active' },
  { name: 'Charlie', email: 'charlie@example.com', age: 35, status: 'inactive' },
]

/**
 * Extended test users for filter and query tests
 */
export const EXTENDED_TEST_USERS: TestUser[] = [
  { name: 'Alice', email: 'alice@test.com', age: 25, status: 'active' },
  { name: 'Bob', email: 'bob@test.com', age: 30, status: 'active' },
  { name: 'Charlie', email: 'charlie@test.com', age: 35, status: 'inactive' },
  { name: 'Diana', email: 'diana@test.com', age: 28, status: 'active' },
  { name: 'Eve', email: 'eve@test.com', age: 22, status: 'inactive' },
]

// =============================================================================
// Data Seeding Helpers
// =============================================================================

/**
 * Seeds the database with standard test data for SQL integration tests.
 * Creates users, posts, and comments with relationships.
 *
 * @param db - The ParqueDB instance to seed
 * @returns Object containing references to all created entities
 */
export async function seedTestData(db: ParqueDB): Promise<SeededTestData> {
  const users = db.collection('users')
  const posts = db.collection('posts')
  const comments = db.collection('comments')

  // Create users
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

  // Create posts
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

  // Create comments
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

  return {
    alice: { $id: alice.$id, name: 'Alice', email: 'alice@example.com', age: 30, status: 'active' },
    bob: { $id: bob.$id, name: 'Bob', email: 'bob@example.com', age: 25, status: 'active' },
    charlie: { $id: charlie.$id, name: 'Charlie', email: 'charlie@example.com', age: 35, status: 'inactive' },
    post1: { $id: post1.$id, name: 'First Post', title: 'Hello World', authorId: alice.$id, views: 100, status: 'published' },
    post2: { $id: post2.$id, name: 'Second Post', title: 'Learning TypeScript', authorId: alice.$id, views: 250, status: 'published' },
    post3: { $id: post3.$id, name: 'Third Post', title: 'My Draft', authorId: bob.$id, views: 10, status: 'draft' },
  }
}

/**
 * Seeds extended user data for comprehensive filter testing.
 *
 * @param db - The ParqueDB instance to seed
 */
export async function seedExtendedUsers(db: ParqueDB): Promise<void> {
  const users = db.collection('users')

  for (const user of EXTENDED_TEST_USERS) {
    await users.create({
      $type: 'User',
      name: user.name,
      email: user.email,
      age: user.age,
      status: user.status,
    })
  }
}

// =============================================================================
// Query Result Helpers
// =============================================================================

/**
 * Extracts a column value from a query result row.
 *
 * @param row - The row array from query results
 * @param columns - The column names array from query results
 * @param columnName - The name of the column to extract
 * @returns The value at the specified column, or undefined if not found
 */
export function getColumnValue(
  row: unknown[],
  columns: string[],
  columnName: string
): unknown {
  const index = columns.indexOf(columnName)
  return index >= 0 ? row[index] : undefined
}

/**
 * Extracts multiple column values from a query result row.
 *
 * @param row - The row array from query results
 * @param columns - The column names array from query results
 * @param columnNames - Array of column names to extract
 * @returns Object with column names as keys and their values
 */
export function getColumnValues(
  row: unknown[],
  columns: string[],
  columnNames: string[]
): Record<string, unknown> {
  const result: Record<string, unknown> = {}
  for (const name of columnNames) {
    result[name] = getColumnValue(row, columns, name)
  }
  return result
}

// =============================================================================
// Test Assertion Helpers
// =============================================================================

/**
 * Asserts that a query result has the expected number of rows.
 *
 * @param result - Query result with rows array
 * @param expectedCount - Expected number of rows
 * @param message - Optional assertion message
 */
export function assertRowCount(
  result: { rows: unknown[] },
  expectedCount: number,
  message?: string
): void {
  const actualCount = result.rows.length
  if (actualCount !== expectedCount) {
    throw new Error(
      message ?? `Expected ${expectedCount} rows, got ${actualCount}`
    )
  }
}

/**
 * Asserts that query results contain expected columns.
 *
 * @param result - Query result with columns array
 * @param expectedColumns - Array of expected column names
 */
export function assertColumnsPresent(
  result: { columns: string[] },
  expectedColumns: string[]
): void {
  for (const col of expectedColumns) {
    if (!result.columns.includes(col)) {
      throw new Error(`Expected column "${col}" not found in results`)
    }
  }
}

// =============================================================================
// Performance Testing Helpers
// =============================================================================

/**
 * Measures the execution time of an async operation.
 *
 * @param operation - Async function to measure
 * @returns Object with result and elapsed time in milliseconds
 */
export async function measureTime<T>(
  operation: () => Promise<T>
): Promise<{ result: T; elapsed: number }> {
  const start = Date.now()
  const result = await operation()
  const elapsed = Date.now() - start
  return { result, elapsed }
}

/**
 * Asserts that an operation completes within a time limit.
 *
 * @param operation - Async function to measure
 * @param maxMs - Maximum allowed time in milliseconds
 * @param operationName - Name for error messages
 * @returns The operation result
 */
export async function assertCompletesWithin<T>(
  operation: () => Promise<T>,
  maxMs: number,
  operationName = 'Operation'
): Promise<T> {
  const { result, elapsed } = await measureTime(operation)
  if (elapsed > maxMs) {
    throw new Error(
      `${operationName} took ${elapsed}ms, expected less than ${maxMs}ms`
    )
  }
  return result
}

// =============================================================================
// Test Data Generation
// =============================================================================

/**
 * Generates unique test data with a prefix.
 *
 * @param prefix - Prefix for generated values
 * @param index - Index number for uniqueness
 * @returns Object with unique name and email
 */
export function generateTestUser(prefix: string, index: number): TestUser {
  return {
    name: `${prefix}${index}`,
    email: `${prefix.toLowerCase()}${index}@example.com`,
    age: 20 + (index % 50),
    status: 'active',
  }
}

/**
 * Generates a batch of test users.
 *
 * @param count - Number of users to generate
 * @param prefix - Prefix for names and emails
 * @returns Array of test user objects
 */
export function generateTestUsers(count: number, prefix = 'User'): TestUser[] {
  return Array.from({ length: count }, (_, i) => generateTestUser(prefix, i))
}
