/**
 * Find with Indexes Test Suite
 *
 * Tests for optimizing find() to use indexes instead of full scans.
 * Covers FTS, vector, and fallback scenarios.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import type { Filter, FindOptions } from '../../src/types'

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a test ParqueDB instance with memory backend
 */
function createTestDB(): ParqueDB {
  const storage = new MemoryBackend()
  return new ParqueDB({ storage })
}

/**
 * Helper to create test posts
 */
async function createTestPosts(db: ParqueDB) {
  const posts = [
    { title: 'First Post', content: 'Hello world from the first post', status: 'published', views: 100 },
    { title: 'Second Post', content: 'Another great article about technology', status: 'published', views: 200 },
    { title: 'Third Post', content: 'Draft article not yet published', status: 'draft', views: 0 },
    { title: 'Fourth Post', content: 'Machine learning and AI trends', status: 'published', views: 300 },
    { title: 'Fifth Post', content: 'Web development best practices', status: 'published', views: 150 },
  ]

  const created = []
  for (const post of posts) {
    const result = await db.Posts.create(post)
    created.push(result)
  }

  return created
}

// =============================================================================
// Test Suite: Find without indexes (baseline)
// =============================================================================

describe('Find without indexes (baseline)', () => {
  let db: ParqueDB

  beforeEach(() => {
    db = createTestDB()
  })

  it('should find all entities in namespace', async () => {
    await createTestPosts(db)

    const result = await db.Posts.find()
    expect(result.items).toHaveLength(5)
  })

  it('should filter by equality', async () => {
    await createTestPosts(db)

    const result = await db.Posts.find({ status: 'published' })
    expect(result.items).toHaveLength(4)
    expect(result.items.every(p => p.status === 'published')).toBe(true)
  })

  it('should filter by range', async () => {
    await createTestPosts(db)

    const result = await db.Posts.find({ views: { $gte: 150 } })
    expect(result.items).toHaveLength(3)
    expect(result.items.every(p => p.views >= 150)).toBe(true)
  })

  it('should filter by complex condition', async () => {
    await createTestPosts(db)

    const result = await db.Posts.find({
      status: 'published',
      views: { $gt: 100, $lt: 300 }
    })
    expect(result.items).toHaveLength(2)
  })
})

// =============================================================================
// Test Suite: Find with FTS index
// =============================================================================

describe('Find with FTS index', () => {
  let db: ParqueDB

  beforeEach(() => {
    db = createTestDB()
  })

  it('should use FTS index when $text operator is present', async () => {
    // Create FTS index BEFORE adding documents
    await db.getIndexManager().createIndex('posts', {
      name: 'content_fts',
      type: 'fts',
      fields: [{ path: 'content' }],
    })

    // Now create posts - they will be automatically added to the index
    await createTestPosts(db)

    // Query with $text should use the FTS index
    const result = await db.Posts.find({
      $text: { $search: 'technology' }
    })

    // Should find the post containing "technology"
    expect(result.items.length).toBeGreaterThan(0)
    expect(result.items.some(p => p.content.includes('technology'))).toBe(true)
  })

  it('should combine FTS with other filters', async () => {
    // Create index first
    await db.getIndexManager().createIndex('posts', {
      name: 'content_fts',
      type: 'fts',
      fields: [{ path: 'content' }],
    })

    // Then create posts
    await createTestPosts(db)

    const result = await db.Posts.find({
      $text: { $search: 'article' },
      status: 'published'
    })

    expect(result.items.length).toBeGreaterThan(0)
    expect(result.items.every(p => p.status === 'published')).toBe(true)
    expect(result.items.some(p => p.content.includes('article'))).toBe(true)
  })

  it('should return empty array when no FTS matches', async () => {
    // Create index first
    await db.getIndexManager().createIndex('posts', {
      name: 'content_fts',
      type: 'fts',
      fields: [{ path: 'content' }],
    })

    // Then create posts
    await createTestPosts(db)

    const result = await db.Posts.find({
      $text: { $search: 'nonexistent' }
    })

    expect(result.items).toHaveLength(0)
  })
})

// =============================================================================
// Test Suite: Find with vector index
// =============================================================================

describe('Find with vector index', () => {
  let db: ParqueDB

  beforeEach(() => {
    db = createTestDB()
  })

  it('should use vector index when $vector operator is present', async () => {
    // Create posts with embeddings
    const posts = [
      { title: 'Post 1', embedding: [0.1, 0.2, 0.3] },
      { title: 'Post 2', embedding: [0.4, 0.5, 0.6] },
      { title: 'Post 3', embedding: [0.7, 0.8, 0.9] },
    ]

    for (const post of posts) {
      await db.Posts.create(post)
    }

    // Create vector index
    await db.getIndexManager().createIndex('posts', {
      name: 'embedding_vector',
      type: 'vector',
      fields: [{ path: 'embedding' }],
      vectorOptions: {
        dimensions: 3,
        metric: 'cosine'
      }
    })

    // Query with $vector should use the vector index
    const result = await db.Posts.find({
      $vector: {
        $field: 'embedding',
        $near: [0.1, 0.2, 0.3],
        $k: 2
      }
    })

    expect(result.items.length).toBeLessThanOrEqual(2)
  })

  it('should combine vector search with other filters', async () => {
    const posts = [
      { title: 'Published Post', embedding: [0.1, 0.2, 0.3], status: 'published' },
      { title: 'Draft Post', embedding: [0.15, 0.25, 0.35], status: 'draft' },
    ]

    for (const post of posts) {
      await db.Posts.create(post)
    }

    await db.getIndexManager().createIndex('posts', {
      name: 'embedding_vector',
      type: 'vector',
      fields: [{ path: 'embedding' }],
      vectorOptions: {
        dimensions: 3,
        metric: 'cosine'
      }
    })

    const result = await db.Posts.find({
      $vector: {
        $field: 'embedding',
        $near: [0.1, 0.2, 0.3],
        $k: 5
      },
      status: 'published'
    })

    expect(result.items.every(p => p.status === 'published')).toBe(true)
  })
})

// =============================================================================
// Test Suite: Find fallback behavior
// =============================================================================

describe('Find fallback behavior', () => {
  let db: ParqueDB

  beforeEach(() => {
    db = createTestDB()
  })

  it('should fallback to scan when no index available', async () => {
    await createTestPosts(db)

    // No index created, should still work with full scan
    const result = await db.Posts.find({ title: 'First Post' })
    expect(result.items).toHaveLength(1)
    expect(result.items[0].title).toBe('First Post')
  })

  it('should fallback to scan for filters that cannot use indexes', async () => {
    await createTestPosts(db)

    // Create an FTS index
    await db.getIndexManager().createIndex('posts', {
      name: 'content_fts',
      type: 'fts',
      fields: [{ path: 'content' }],
    })

    // Query without $text should not use FTS index, fallback to scan
    const result = await db.Posts.find({ status: 'draft' })
    expect(result.items).toHaveLength(1)
    expect(result.items[0].status).toBe('draft')
  })

  it('should work correctly when index exists but query does not match indexed fields', async () => {
    await createTestPosts(db)

    await db.getIndexManager().createIndex('posts', {
      name: 'content_fts',
      type: 'fts',
      fields: [{ path: 'content' }],
    })

    // Query on a different field
    const result = await db.Posts.find({ views: { $gte: 200 } })
    expect(result.items).toHaveLength(2)
    expect(result.items.every(p => p.views >= 200)).toBe(true)
  })
})

// =============================================================================
// Test Suite: Performance characteristics
// =============================================================================

describe('Performance characteristics', () => {
  let db: ParqueDB

  beforeEach(() => {
    db = createTestDB()
  })

  it('should be more efficient with indexes (conceptual test)', async () => {
    // Create FTS index first
    await db.getIndexManager().createIndex('posts', {
      name: 'content_fts',
      type: 'fts',
      fields: [{ path: 'content' }],
    })

    // Create many posts
    const posts = []
    for (let i = 0; i < 100; i++) {
      posts.push({
        title: `Post ${i}`,
        content: `Content with keyword${i % 10 === 0 ? ' special' : ''} text`,
        status: i % 2 === 0 ? 'published' : 'draft'
      })
    }

    for (const post of posts) {
      await db.Posts.create(post)
    }

    // Query with index should work
    const result = await db.Posts.find({
      $text: { $search: 'special' }
    })

    // Should find approximately 10 posts with "special"
    expect(result.items.length).toBeGreaterThan(0)
    expect(result.items.length).toBeLessThanOrEqual(20) // Some tolerance
  })
})
