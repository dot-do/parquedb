/**
 * Auto-Resolve Relationships Tests for ParqueDB
 *
 * Tests for automatic resolution of short IDs to full entity IDs when creating
 * entities with relationships.
 *
 * When a schema defines a relationship like `author: '-> User'`, the create()
 * method should auto-resolve short IDs (e.g., 'alice@example.com') to full
 * entity IDs (e.g., 'user/alice@example.com').
 *
 * This is the RED phase - these tests should FAIL until the feature is implemented.
 */

import { describe, it, expect, afterEach } from 'vitest'
import { DB } from '../../src/db'
import { MemoryBackend } from '../../src/storage'
import { clearGlobalStorage } from '../../src/Collection'

describe('auto-resolve relationships', () => {
  afterEach(() => {
    clearGlobalStorage()
  })

  it('auto-resolves relationship to full ID on create', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      User: {
        $id: 'email',
        email: 'string!#',
        name: 'string',
        posts: '<- Post.author[]'
      },
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        author: '-> User'
      }
    }, { storage })

    await db.User.create({ email: 'alice@example.com', name: 'Alice' })

    // Create post with short ID for author
    const post = await db.Post.create({
      slug: 'hello-world',
      title: 'Hello',
      author: 'alice@example.com'  // Short form - should auto-resolve
    })

    // The relationship should be created with the full ID
    // Check via getRelated at the db level (not collection level)
    // Note: $id is 'post/hello-world', local ID is 'hello-world'
    const localId = post.$id.split('/')[1]
    const author = await db.getRelated('post', localId, 'author')
    expect(author.items[0].$id).toBe('user/alice@example.com')
  })

  it('also accepts full ID format', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      User: {
        $id: 'email',
        email: 'string!#',
        name: 'string',
      },
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        author: '-> User'
      }
    }, { storage })

    await db.User.create({ email: 'alice@example.com', name: 'Alice' })

    // Create post with full ID for author
    const post = await db.Post.create({
      slug: 'test-post',
      title: 'Test',
      author: 'user/alice@example.com'  // Full form
    })

    const localId = post.$id.split('/')[1]
    const author = await db.getRelated('post', localId, 'author')
    expect(author.items[0].$id).toBe('user/alice@example.com')
  })

  it('throws error if related entity does not exist', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      User: {
        $id: 'email',
        email: 'string!#',
        name: 'string',
      },
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        author: '-> User'
      }
    }, { storage })

    // Try to create post with non-existent user
    // Should throw an error about the entity not existing (not a validation error)
    await expect(db.Post.create({
      slug: 'orphan-post',
      title: 'Orphan',
      author: 'nobody@example.com'
    })).rejects.toThrow(/not found|does not exist/i)
  })

  it('works without $id directive (uses full ID)', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Author: {
        name: 'string!',
      },
      Book: {
        title: 'string!',
        writer: '-> Author'
      }
    }, { storage })

    const author = await db.Author.create({ name: 'Jane' })

    // Must use full ID when target has no $id directive
    const book = await db.Book.create({
      title: 'Great Book',
      writer: author.$id  // Full ID required
    })

    const localId = book.$id.split('/')[1]
    const writer = await db.getRelated('book', localId, 'writer')
    expect(writer.items[0].name).toBe('Jane')
  })

  it('auto-resolves array of relationships', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Tag: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
      },
      Article: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        tags: '-> Tag[]'
      }
    }, { storage })

    await db.Tag.create({ slug: 'javascript', name: 'JavaScript' })
    await db.Tag.create({ slug: 'typescript', name: 'TypeScript' })

    // Create article with array of short IDs for tags
    const article = await db.Article.create({
      slug: 'my-article',
      title: 'My Article',
      tags: ['javascript', 'typescript']  // Short form array - should auto-resolve
    })

    const localId = article.$id.split('/')[1]
    const tags = await db.getRelated('article', localId, 'tags')
    expect(tags.items).toHaveLength(2)
    expect(tags.items.map(t => t.$id).sort()).toEqual([
      'tag/javascript',
      'tag/typescript'
    ])
  })

  it('handles mixed short and full IDs in array', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Category: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
      },
      Product: {
        $id: 'sku',
        sku: 'string!#',
        name: 'string!',
        categories: '-> Category[]'
      }
    }, { storage })

    await db.Category.create({ slug: 'electronics', name: 'Electronics' })
    await db.Category.create({ slug: 'computers', name: 'Computers' })

    // Create product with mixed short and full IDs
    const product = await db.Product.create({
      sku: 'LAPTOP-001',
      name: 'Laptop',
      categories: ['electronics', 'category/computers']  // Mixed format
    })

    const localId = product.$id.split('/')[1]
    const categories = await db.getRelated('product', localId, 'categories')
    expect(categories.items).toHaveLength(2)
    expect(categories.items.map(c => c.$id).sort()).toEqual([
      'category/computers',
      'category/electronics'
    ])
  })

  it('uses namespace from relationship target type', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Organization: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
      },
      Employee: {
        name: 'string!',
        employer: '-> Organization'
      }
    }, { storage })

    await db.Organization.create({ slug: 'acme-corp', name: 'ACME Corp' })

    // Create employee with short ID (should resolve to organization namespace)
    // Note: The namespace is the lowercased type name, matching proxy behavior
    const employee = await db.Employee.create({
      name: 'John Doe',
      employer: 'acme-corp'  // Should resolve to 'organization/acme-corp'
    })

    const localId = employee.$id.split('/')[1]
    const employer = await db.getRelated('employee', localId, 'employer')
    expect(employer.items[0].$id).toBe('organization/acme-corp')
  })
})
