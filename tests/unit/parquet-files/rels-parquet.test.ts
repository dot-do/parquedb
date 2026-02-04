/**
 * Rels.parquet File Creation and Contents Verification Tests
 *
 * This test suite verifies that ParqueDB correctly creates and populates
 * rels.parquet files when entities have relationships. It uses a factory
 * pattern to run the same tests against different storage backends.
 *
 * Tests cover:
 * 1. Basic relationship storage (Post -> User)
 * 2. Schema verification (sourceId, sourceField, targetId, createdAt)
 * 3. Multiple relationships on a single entity
 * 4. Many-to-many relationships
 * 5. Self-referential relationships
 * 6. Bidirectional relationship indexing
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ParqueDB } from '../../../src/ParqueDB'
import { FsBackend } from '../../../src/storage/FsBackend'
import { mkdtemp, rm, readdir, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { StorageBackend } from '../../../src/types/storage'
import {
  createTestFsBackend,
  createTestR2Backend,
  hasR2Credentials,
  cleanupFsBackend,
  cleanupR2Backend,
} from '../../helpers/storage'
import { R2Backend } from '../../../src/storage/R2Backend'

// =============================================================================
// Types
// =============================================================================

interface TestBackendFactory {
  name: string
  create: () => Promise<{ storage: StorageBackend; cleanup: () => Promise<void>; rootPath?: string }>
  skip?: boolean
}

interface RelsRow {
  sourceId: string
  sourceField: string
  targetId: string
  createdAt: string
}

// =============================================================================
// Parquet Reading Utilities
// =============================================================================

/**
 * Read a parquet file and return its rows as objects
 */
async function readParquetFile<T = Record<string, unknown>>(data: Uint8Array): Promise<{
  rows: T[]
  columnNames: string[]
}> {
  const { parquetRead, parquetMetadataAsync } = await import('hyparquet')
  const { compressors } = await import('../../../src/parquet/compression')

  const asyncBuffer = {
    byteLength: data.length,
    slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
      const sliced = data.slice(start, end ?? data.length)
      const buffer = new ArrayBuffer(sliced.byteLength)
      new Uint8Array(buffer).set(sliced)
      return buffer
    },
  }

  // Get metadata to extract column names
  const metadata = await parquetMetadataAsync(asyncBuffer)
  const schema = (metadata.schema || []) as Array<{ name?: string }>
  const columnNames: string[] = schema
    .filter((el) => el.name && el.name !== 'root')
    .map((el) => el.name!)

  // Read rows
  let rawRows: unknown[][] = []
  await parquetRead({
    file: asyncBuffer,
    compressors,
    onComplete: (data: unknown[][]) => {
      rawRows = data
    },
  })

  // Convert array rows to objects
  const rows = rawRows.map((rowArray) => {
    const row: Record<string, unknown> = {}
    for (let i = 0; i < columnNames.length; i++) {
      const colName = columnNames[i]
      if (colName) {
        row[colName] = rowArray[i]
      }
    }
    return row as T
  })

  return { rows, columnNames }
}

// =============================================================================
// Test Factory
// =============================================================================

/**
 * Create test suite for rels.parquet verification
 *
 * This factory function creates identical tests that run against different
 * storage backends to ensure consistent behavior across implementations.
 */
function createRelsParquetTests(
  backendName: string,
  createBackend: () => Promise<{ storage: StorageBackend; cleanup: () => Promise<void>; rootPath?: string }>
) {
  describe(`rels.parquet verification (${backendName})`, () => {
    let storage: StorageBackend
    let db: ParqueDB
    let cleanup: () => Promise<void>
    let rootPath: string | undefined

    beforeEach(async () => {
      const result = await createBackend()
      storage = result.storage
      cleanup = result.cleanup
      rootPath = result.rootPath
      db = new ParqueDB({ storage })
    })

    afterEach(async () => {
      try {
        await db.disposeAsync()
      } catch {
        // Ignore dispose errors
      }
      try {
        await cleanup()
      } catch {
        // Ignore cleanup errors
      }
    })

    // =========================================================================
    // Schema Verification Tests
    // =========================================================================

    describe('schema verification', () => {
      it('should create rels.parquet with correct schema columns', async () => {
        // Create a user first
        const user = await db.create('users', {
          $type: 'User',
          name: 'Alice',
          email: 'alice@example.com',
        })

        // Create a post with relationship to user
        // Note: Relationships are detected when field value is a string containing '/'
        // in the format 'namespace/id' (entity ID format)
        await db.create('posts', {
          $type: 'Post',
          name: 'My First Post',
          title: 'Hello World',
          author: user.$id, // Direct entity ID string triggers relationship detection
        })

        // Flush to ensure data is written
        await db.disposeAsync()

        // Verify rels.parquet exists
        const exists = await storage.exists('rels.parquet')
        expect(exists).toBe(true)

        // Read and verify schema
        const data = await storage.read('rels.parquet')
        const { columnNames } = await readParquetFile(data)

        // Verify all expected columns exist
        expect(columnNames).toContain('sourceId')
        expect(columnNames).toContain('sourceField')
        expect(columnNames).toContain('targetId')
        expect(columnNames).toContain('createdAt')

        // Verify we have exactly 4 columns
        expect(columnNames.length).toBe(4)
      })
    })

    // =========================================================================
    // Basic Relationship Tests
    // =========================================================================

    describe('basic relationship storage', () => {
      it('should store Post -> User author relationship', async () => {
        // Create a user
        const user = await db.create('users', {
          $type: 'User',
          name: 'Bob',
          email: 'bob@example.com',
        })

        // Create a post with author relationship
        // Using direct entity ID string format for relationship detection
        const post = await db.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          title: 'Testing Relationships',
          author: user.$id, // Direct entity ID triggers relationship detection
        })

        // Flush to ensure data is written
        await db.disposeAsync()

        // Read rels.parquet
        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        // Find the author relationship
        const authorRel = rows.find(
          (r) => r.sourceField === 'author' && r.targetId === user.$id
        )

        expect(authorRel).toBeDefined()
        expect(authorRel?.sourceId).toBe(post.$id)
        expect(authorRel?.sourceField).toBe('author')
        expect(authorRel?.targetId).toBe(user.$id)
        expect(authorRel?.createdAt).toBeDefined()
        expect(typeof authorRel?.createdAt).toBe('string')
      })

      it('should store relationship with full entity ID format', async () => {
        // Create entities
        const category = await db.create('categories', {
          $type: 'Category',
          name: 'Technology',
        })

        const article = await db.create('articles', {
          $type: 'Article',
          name: 'Tech News',
          category: category.$id, // Direct entity ID
        })

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        const categoryRel = rows.find(
          (r) => r.sourceField === 'category' && r.sourceId === article.$id
        )

        expect(categoryRel).toBeDefined()
        expect(categoryRel?.targetId).toBe(category.$id)
        // Verify IDs are in ns/id format
        expect(categoryRel?.sourceId).toMatch(/^articles\//)
        expect(categoryRel?.targetId).toMatch(/^categories\//)
      })
    })

    // =========================================================================
    // Multiple Relationships Tests
    // =========================================================================

    describe('multiple relationships', () => {
      it('should store multiple relationships on a single entity', async () => {
        // Create target entities
        const author = await db.create('users', {
          $type: 'User',
          name: 'Charlie',
        })

        const editor = await db.create('users', {
          $type: 'User',
          name: 'Diana',
        })

        const category = await db.create('categories', {
          $type: 'Category',
          name: 'Science',
        })

        // Create post with multiple relationships using direct entity IDs
        const post = await db.create('posts', {
          $type: 'Post',
          name: 'Scientific Discovery',
          author: author.$id,
          editor: editor.$id,
          category: category.$id,
        })

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        // Filter relationships for this post
        const postRels = rows.filter((r) => r.sourceId === post.$id)

        // Should have 3 relationships
        expect(postRels.length).toBe(3)

        // Verify each relationship exists
        const authorRel = postRels.find((r) => r.sourceField === 'author')
        const editorRel = postRels.find((r) => r.sourceField === 'editor')
        const categoryRel = postRels.find((r) => r.sourceField === 'category')

        expect(authorRel?.targetId).toBe(author.$id)
        expect(editorRel?.targetId).toBe(editor.$id)
        expect(categoryRel?.targetId).toBe(category.$id)
      })

      it('should store relationships from multiple source entities', async () => {
        const user = await db.create('users', {
          $type: 'User',
          name: 'Eve',
        })

        const post1 = await db.create('posts', {
          $type: 'Post',
          name: 'Post 1',
          author: user.$id,
        })

        const post2 = await db.create('posts', {
          $type: 'Post',
          name: 'Post 2',
          author: user.$id,
        })

        const post3 = await db.create('posts', {
          $type: 'Post',
          name: 'Post 3',
          author: user.$id,
        })

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        // All three posts should have author relationship to Eve
        const authorRels = rows.filter(
          (r) => r.sourceField === 'author' && r.targetId === user.$id
        )

        expect(authorRels.length).toBe(3)

        const sourceIds = authorRels.map((r) => r.sourceId)
        expect(sourceIds).toContain(post1.$id)
        expect(sourceIds).toContain(post2.$id)
        expect(sourceIds).toContain(post3.$id)
      })
    })

    // =========================================================================
    // Many-to-Many Relationship Tests
    // =========================================================================

    describe('many-to-many relationships', () => {
      it('should store many-to-many relationship via array format', async () => {
        // Create multiple tags
        const tag1 = await db.create('tags', {
          $type: 'Tag',
          name: 'JavaScript',
        })

        const tag2 = await db.create('tags', {
          $type: 'Tag',
          name: 'TypeScript',
        })

        const tag3 = await db.create('tags', {
          $type: 'Tag',
          name: 'Node.js',
        })

        // Create post with multiple tags (many-to-many via array of entity IDs)
        // The extractEntityRefs function detects arrays of entity ID strings
        const post = await db.create('posts', {
          $type: 'Post',
          name: 'JavaScript Tutorial',
          tags: [tag1.$id, tag2.$id, tag3.$id],
        })

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        // Filter tag relationships for this post
        const tagRels = rows.filter(
          (r) => r.sourceId === post.$id && r.sourceField === 'tags'
        )

        expect(tagRels.length).toBe(3)

        const targetIds = tagRels.map((r) => r.targetId)
        expect(targetIds).toContain(tag1.$id)
        expect(targetIds).toContain(tag2.$id)
        expect(targetIds).toContain(tag3.$id)
      })

      it('should store bidirectional many-to-many relationships', async () => {
        // Create users
        const user1 = await db.create('users', {
          $type: 'User',
          name: 'Frank',
        })

        const user2 = await db.create('users', {
          $type: 'User',
          name: 'Grace',
        })

        // Create projects with team members using array of entity IDs
        const project1 = await db.create('projects', {
          $type: 'Project',
          name: 'Project Alpha',
          members: [user1.$id, user2.$id],
        })

        const project2 = await db.create('projects', {
          $type: 'Project',
          name: 'Project Beta',
          members: [user1.$id],
        })

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        // User1 (Frank) should be in both projects
        const frankRels = rows.filter((r) => r.targetId === user1.$id)
        expect(frankRels.length).toBe(2)

        // User2 (Grace) should be in one project
        const graceRels = rows.filter((r) => r.targetId === user2.$id)
        expect(graceRels.length).toBe(1)
        expect(graceRels[0]?.sourceId).toBe(project1.$id)
      })
    })

    // =========================================================================
    // Self-Referential Relationship Tests
    // =========================================================================

    describe('self-referential relationships', () => {
      it('should store parent-child self-reference', async () => {
        // Create parent category
        const parent = await db.create('categories', {
          $type: 'Category',
          name: 'Electronics',
        })

        // Create child category with parent reference using direct entity ID
        const child = await db.create('categories', {
          $type: 'Category',
          name: 'Smartphones',
          parent: parent.$id,
        })

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        const parentRel = rows.find(
          (r) => r.sourceId === child.$id && r.sourceField === 'parent'
        )

        expect(parentRel).toBeDefined()
        expect(parentRel?.targetId).toBe(parent.$id)
        // Both should be in same namespace
        expect(parentRel?.sourceId).toMatch(/^categories\//)
        expect(parentRel?.targetId).toMatch(/^categories\//)
      })

      it('should store user follows user relationship', async () => {
        const user1 = await db.create('users', {
          $type: 'User',
          name: 'Henry',
        })

        const user2 = await db.create('users', {
          $type: 'User',
          name: 'Ivy',
        })

        const user3 = await db.create('users', {
          $type: 'User',
          name: 'Jack',
        })

        // User1 follows User2 and User3 using array of entity IDs
        await db.update('users', user1.$id as string, {
          $set: {
            following: [user2.$id, user3.$id],
          },
        })

        // User2 follows User1
        await db.update('users', user2.$id as string, {
          $set: {
            following: [user1.$id],
          },
        })

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        // User1 follows 2 users
        const user1Following = rows.filter(
          (r) => r.sourceId === user1.$id && r.sourceField === 'following'
        )
        expect(user1Following.length).toBe(2)

        // User2 follows 1 user
        const user2Following = rows.filter(
          (r) => r.sourceId === user2.$id && r.sourceField === 'following'
        )
        expect(user2Following.length).toBe(1)
        expect(user2Following[0]?.targetId).toBe(user1.$id)
      })

      it('should handle hierarchical self-references (grandparent -> parent -> child)', async () => {
        const grandparent = await db.create('nodes', {
          $type: 'Node',
          name: 'Root',
        })

        const parent = await db.create('nodes', {
          $type: 'Node',
          name: 'Branch',
          parent: grandparent.$id,
        })

        const child = await db.create('nodes', {
          $type: 'Node',
          name: 'Leaf',
          parent: parent.$id,
        })

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        // Parent references grandparent
        const parentToGrandparent = rows.find(
          (r) => r.sourceId === parent.$id && r.targetId === grandparent.$id
        )
        expect(parentToGrandparent).toBeDefined()

        // Child references parent
        const childToParent = rows.find(
          (r) => r.sourceId === child.$id && r.targetId === parent.$id
        )
        expect(childToParent).toBeDefined()

        // Grandparent has no parent reference
        const grandparentRefs = rows.filter((r) => r.sourceId === grandparent.$id)
        expect(grandparentRefs.length).toBe(0)
      })
    })

    // =========================================================================
    // Bidirectional Index Verification Tests
    // =========================================================================

    describe('bidirectional relationship indexing', () => {
      it('should index relationships for reverse lookups (target -> sources)', async () => {
        const author = await db.create('users', {
          $type: 'User',
          name: 'Karen',
        })

        const post1 = await db.create('posts', {
          $type: 'Post',
          name: 'Post A',
          author: author.$id,
        })

        const post2 = await db.create('posts', {
          $type: 'Post',
          name: 'Post B',
          author: author.$id,
        })

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        // Query by targetId (reverse lookup)
        const postsForAuthor = rows.filter((r) => r.targetId === author.$id)

        expect(postsForAuthor.length).toBe(2)
        expect(postsForAuthor.map((r) => r.sourceId).sort()).toEqual(
          [post1.$id, post2.$id].sort()
        )
      })

      it('should support querying relationships by sourceField', async () => {
        const user = await db.create('users', {
          $type: 'User',
          name: 'Leo',
        })

        const post = await db.create('posts', {
          $type: 'Post',
          name: 'Test Post',
          author: user.$id,
          reviewer: user.$id,
        })

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        // Query author relationships
        const authorRels = rows.filter(
          (r) => r.sourceId === post.$id && r.sourceField === 'author'
        )
        expect(authorRels.length).toBe(1)

        // Query reviewer relationships
        const reviewerRels = rows.filter(
          (r) => r.sourceId === post.$id && r.sourceField === 'reviewer'
        )
        expect(reviewerRels.length).toBe(1)

        // Both point to same user
        expect(authorRels[0]?.targetId).toBe(user.$id)
        expect(reviewerRels[0]?.targetId).toBe(user.$id)
      })
    })

    // =========================================================================
    // Edge Cases and Data Integrity Tests
    // =========================================================================

    describe('data integrity', () => {
      it('should have valid ISO timestamp in createdAt field', async () => {
        const user = await db.create('users', {
          $type: 'User',
          name: 'Mark',
        })

        await db.create('posts', {
          $type: 'Post',
          name: 'Test',
          author: user.$id,
        })

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        for (const row of rows) {
          // Verify createdAt is a valid ISO timestamp
          const date = new Date(row.createdAt)
          expect(date.toString()).not.toBe('Invalid Date')
          expect(row.createdAt).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)
        }
      })

      it('should not create rels.parquet when no relationships exist', async () => {
        // Create entities without relationships
        await db.create('users', {
          $type: 'User',
          name: 'Nancy',
          email: 'nancy@example.com',
        })

        await db.create('posts', {
          $type: 'Post',
          name: 'Standalone Post',
          title: 'No relationships here',
        })

        await db.disposeAsync()

        // rels.parquet should not exist or be empty
        const exists = await storage.exists('rels.parquet')
        if (exists) {
          const data = await storage.read('rels.parquet')
          const { rows } = await readParquetFile<RelsRow>(data)
          expect(rows.length).toBe(0)
        }
      })

      it('should handle entities with special characters in names', async () => {
        const user = await db.create('users', {
          $type: 'User',
          name: "O'Brien & Co.",
        })

        await db.create('posts', {
          $type: 'Post',
          name: 'Test <Post> "Quoted"',
          author: user.$id,
        })

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        expect(rows.length).toBeGreaterThan(0)
        const authorRel = rows.find((r) => r.sourceField === 'author')
        expect(authorRel?.targetId).toBe(user.$id)
      })
    })

    // =========================================================================
    // Complex Relationship Graph Tests
    // =========================================================================

    describe('complex relationship graphs', () => {
      it('should handle organization -> team -> user hierarchy', async () => {
        // Create organization
        const org = await db.create('organizations', {
          $type: 'Organization',
          name: 'Acme Corp',
        })

        // Create teams with organization reference
        const team1 = await db.create('teams', {
          $type: 'Team',
          name: 'Engineering',
          organization: org.$id,
        })

        const team2 = await db.create('teams', {
          $type: 'Team',
          name: 'Marketing',
          organization: org.$id,
        })

        // Create users with team reference
        const user1 = await db.create('users', {
          $type: 'User',
          name: 'Oscar',
          team: team1.$id,
        })

        const user2 = await db.create('users', {
          $type: 'User',
          name: 'Patricia',
          team: team2.$id,
        })

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        // Teams -> Organization relationships
        const teamOrgRels = rows.filter((r) => r.sourceField === 'organization')
        expect(teamOrgRels.length).toBe(2)
        expect(teamOrgRels.every((r) => r.targetId === org.$id)).toBe(true)

        // Users -> Teams relationships
        const userTeamRels = rows.filter((r) => r.sourceField === 'team')
        expect(userTeamRels.length).toBe(2)

        // Verify correct team assignments
        const oscarTeam = userTeamRels.find((r) => r.sourceId === user1.$id)
        expect(oscarTeam?.targetId).toBe(team1.$id)

        const patriciaTeam = userTeamRels.find((r) => r.sourceId === user2.$id)
        expect(patriciaTeam?.targetId).toBe(team2.$id)
      })

      it('should handle dense relationship networks', async () => {
        // Create 5 users
        const users = await Promise.all(
          Array.from({ length: 5 }, (_, i) =>
            db.create('users', {
              $type: 'User',
              name: `User${i}`,
            })
          )
        )

        // Create posts with various relationships using array format for collaborators
        const posts = await Promise.all(
          users.map(async (user, idx) => {
            // Each user collaborates with next 2 users (circular)
            const collaboratorIds: string[] = []
            for (let j = 1; j <= 2; j++) {
              const collabIdx = (idx + j) % users.length
              const collab = users[collabIdx]
              if (collab) {
                collaboratorIds.push(collab.$id as string)
              }
            }

            return db.create('posts', {
              $type: 'Post',
              name: `Post by User${idx}`,
              author: user.$id,
              collaborators: collaboratorIds,
            })
          })
        )

        await db.disposeAsync()

        const data = await storage.read('rels.parquet')
        const { rows } = await readParquetFile<RelsRow>(data)

        // 5 author relationships + 5*2 = 10 collaborator relationships = 15 total
        expect(rows.length).toBe(15)

        // Each post has exactly 1 author
        const authorRels = rows.filter((r) => r.sourceField === 'author')
        expect(authorRels.length).toBe(5)

        // Each post has 2 collaborators
        const collabRels = rows.filter((r) => r.sourceField === 'collaborators')
        expect(collabRels.length).toBe(10)
      })
    })
  })
}

// =============================================================================
// Backend Factories
// =============================================================================

const backendFactories: TestBackendFactory[] = [
  {
    name: 'FsBackend',
    create: async () => {
      const tempDir = await mkdtemp(join(tmpdir(), 'parquedb-rels-test-'))
      const storage = new FsBackend(tempDir)
      return {
        storage,
        rootPath: tempDir,
        cleanup: async () => {
          try {
            await rm(tempDir, { recursive: true, force: true })
          } catch {
            // Ignore cleanup errors
          }
        },
      }
    },
  },
  {
    name: 'R2Backend',
    skip: !hasR2Credentials(),
    create: async () => {
      const storage = await createTestR2Backend()
      return {
        storage,
        cleanup: async () => {
          await cleanupR2Backend(storage as R2Backend)
        },
      }
    },
  },
]

// =============================================================================
// Run Tests for Each Backend
// =============================================================================

for (const factory of backendFactories) {
  if (factory.skip) {
    describe.skip(`rels.parquet verification (${factory.name})`, () => {
      it('skipped - credentials not available', () => {})
    })
  } else {
    createRelsParquetTests(factory.name, factory.create)
  }
}
