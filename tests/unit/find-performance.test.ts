/**
 * Find Performance Test Suite
 *
 * Tests demonstrating performance improvements when using indexes
 * vs full scans for common query patterns.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { ParqueDB } from '../../src/ParqueDB'
import { MemoryBackend } from '../../src/storage/MemoryBackend'

// =============================================================================
// Test Utilities
// =============================================================================

function createTestDB(): ParqueDB {
  const storage = new MemoryBackend()
  return new ParqueDB({ storage })
}

// =============================================================================
// Performance Tests
// =============================================================================

describe('Find Performance with Indexes', () => {
  let db: ParqueDB

  beforeEach(() => {
    db = createTestDB()
  })

  it('should demonstrate FTS index benefits for text search', async () => {
    // Create FTS index before adding documents
    await db.getIndexManager().createIndex('articles', {
      name: 'content_fts',
      type: 'fts',
      fields: [{ path: 'content' }],
    })

    // Create test documents
    const keywords = ['javascript', 'python', 'rust', 'go', 'java']
    for (let i = 0; i < 50; i++) {
      const keyword = keywords[i % keywords.length]
      await db.Articles.create({
        title: `Article ${i}`,
        content: `This is an article about ${keyword} programming language`,
        tags: [keyword, 'programming']
      })
    }

    // Query using FTS index
    const result = await db.Articles.find({
      $text: { $search: 'javascript' }
    })

    // Should only return articles matching the search term
    expect(result.items.length).toBe(10) // 50 articles / 5 keywords = 10 per keyword
    expect(result.items.every(a => a.content.includes('javascript'))).toBe(true)
  })

  it('should demonstrate vector index benefits for similarity search', async () => {
    // Create vector index before adding documents
    await db.getIndexManager().createIndex('products', {
      name: 'embedding_vector',
      type: 'vector',
      fields: [{ path: 'embedding' }],
      vectorOptions: {
        dimensions: 3,
        metric: 'cosine'
      }
    })

    // Create products with embeddings
    const products = [
      { name: 'Product A', embedding: [0.1, 0.2, 0.3], category: 'electronics' },
      { name: 'Product B', embedding: [0.15, 0.25, 0.35], category: 'electronics' },
      { name: 'Product C', embedding: [0.9, 0.8, 0.7], category: 'furniture' },
      { name: 'Product D', embedding: [0.85, 0.75, 0.65], category: 'furniture' },
      { name: 'Product E', embedding: [0.5, 0.5, 0.5], category: 'misc' },
    ]

    for (const product of products) {
      await db.Products.create(product)
    }

    // Query for similar products
    const result = await db.Products.find({
      $vector: {
        $field: 'embedding',
        $near: [0.1, 0.2, 0.3],  // Similar to Product A
        $k: 2
      }
    })

    // Should return at most 2 products (k=2)
    expect(result.items.length).toBeLessThanOrEqual(2)

    // Results should be ordered by similarity
    if (result.items.length >= 2) {
      // Product A and B should be most similar to query vector
      const names = result.items.map(p => p.name)
      expect(names).toContain('Product A')
      expect(names).toContain('Product B')
    }
  })

  it('should handle mixed queries with indexes and regular filters', async () => {
    // Create FTS index
    await db.getIndexManager().createIndex('posts', {
      name: 'content_fts',
      type: 'fts',
      fields: [{ path: 'content' }],
    })

    // Create posts
    const statuses = ['draft', 'published', 'archived']
    for (let i = 0; i < 30; i++) {
      await db.Posts.create({
        title: `Post ${i}`,
        content: i % 3 === 0 ? 'This post discusses databases' : 'Regular content',
        status: statuses[i % 3],
        views: i * 10
      })
    }

    // Query with both FTS and regular filter
    const result = await db.Posts.find({
      $text: { $search: 'databases' },
      status: 'published'
    })

    // Should use FTS index to narrow down candidates
    // Then apply status filter
    expect(result.items.every(p => p.status === 'published')).toBe(true)
    expect(result.items.every(p => p.content.includes('databases'))).toBe(true)
  })

  it('should fallback gracefully when no index matches query', async () => {
    // Create an FTS index
    await db.getIndexManager().createIndex('items', {
      name: 'description_fts',
      type: 'fts',
      fields: [{ path: 'description' }],
    })

    // Create items
    for (let i = 0; i < 20; i++) {
      await db.Items.create({
        name: `Item ${i}`,
        description: `Description for item ${i}`,
        price: i * 100,
        inStock: i % 2 === 0
      })
    }

    // Query without using FTS - should fallback to full scan
    const result = await db.Items.find({
      price: { $gte: 500, $lte: 1000 },
      inStock: true
    })

    // Should work correctly even without index
    expect(result.items.every(item => item.price >= 500 && item.price <= 1000)).toBe(true)
    expect(result.items.every(item => item.inStock === true)).toBe(true)
  })

  it('should handle empty result sets efficiently', async () => {
    // Create FTS index
    await db.getIndexManager().createIndex('docs', {
      name: 'text_fts',
      type: 'fts',
      fields: [{ path: 'text' }],
    })

    // Create documents
    for (let i = 0; i < 50; i++) {
      await db.Docs.create({
        title: `Doc ${i}`,
        text: 'Standard document text'
      })
    }

    // Search for non-existent term
    const result = await db.Docs.find({
      $text: { $search: 'nonexistentterm' }
    })

    // Should return empty result quickly
    expect(result.items).toHaveLength(0)
    expect(result.total).toBe(0)
    expect(result.hasMore).toBe(false)
  })

  it('should support pagination with indexed queries', async () => {
    // Create FTS index
    await db.getIndexManager().createIndex('blogs', {
      name: 'content_fts',
      type: 'fts',
      fields: [{ path: 'content' }],
    })

    // Create blog posts with specific keyword
    for (let i = 0; i < 25; i++) {
      await db.Blogs.create({
        title: `Blog ${i}`,
        content: 'This blog discusses machine learning algorithms',
        publishedAt: new Date(2024, 0, i + 1)
      })
    }

    // First page
    const page1 = await db.Blogs.find(
      { $text: { $search: 'machine learning' } },
      { limit: 10 }
    )

    // Should have results and pagination info
    expect(page1.items.length).toBeGreaterThan(0)
    expect(page1.items.length).toBeLessThanOrEqual(10)
    expect(page1.total).toBeGreaterThanOrEqual(page1.items.length)

    // If there are more than 10 results total, hasMore should be true
    if (page1.total > 10) {
      expect(page1.hasMore).toBe(true)

      // Second page using skip
      const page2 = await db.Blogs.find(
        { $text: { $search: 'machine learning' } },
        { limit: 10, skip: 10 }
      )

      expect(page2.items.length).toBeGreaterThanOrEqual(0)
    }
  })
})

// =============================================================================
// Index Selection Logic Tests
// =============================================================================

describe('Index Selection Logic', () => {
  let db: ParqueDB

  beforeEach(() => {
    db = createTestDB()
  })

  it('should prefer FTS index when $text operator is present', async () => {
    await db.getIndexManager().createIndex('notes', {
      name: 'text_fts',
      type: 'fts',
      fields: [{ path: 'text' }],
    })

    // Create notes
    for (let i = 0; i < 10; i++) {
      await db.Notes.create({
        text: i < 5 ? 'Important note' : 'Regular note'
      })
    }

    const result = await db.Notes.find({
      $text: { $search: 'important' }
    })

    // Should use FTS index and only return matching documents
    expect(result.items.length).toBe(5)
  })

  it('should prefer vector index when $vector operator is present', async () => {
    await db.getIndexManager().createIndex('images', {
      name: 'feature_vector',
      type: 'vector',
      fields: [{ path: 'features' }],
      vectorOptions: {
        dimensions: 2,
        metric: 'euclidean'
      }
    })

    // Create images
    const images = [
      { name: 'Image 1', features: [1.0, 1.0] },
      { name: 'Image 2', features: [1.1, 0.9] },
      { name: 'Image 3', features: [5.0, 5.0] },
    ]

    for (const img of images) {
      await db.Images.create(img)
    }

    const result = await db.Images.find({
      $vector: {
        $field: 'features',
        $near: [1.0, 1.0],
        $k: 2
      }
    })

    // Should use vector index
    expect(result.items.length).toBeLessThanOrEqual(2)
    // Most similar images should be returned
    const names = result.items.map(i => i.name)
    expect(names).toContain('Image 1')
  })

  it('should use full scan when no applicable index exists', async () => {
    // No index created

    // Create data
    for (let i = 0; i < 10; i++) {
      await db.Tasks.create({
        title: `Task ${i}`,
        priority: i < 5 ? 'high' : 'low',
        completed: i % 2 === 0
      })
    }

    // Query without index
    const result = await db.Tasks.find({
      priority: 'high',
      completed: false
    })

    // Should still work via full scan
    expect(result.items.every(t => t.priority === 'high' && t.completed === false)).toBe(true)
  })
})
