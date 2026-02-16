/**
 * collection.getRelated() Tests for ParqueDB
 *
 * RED phase tests - these tests define the expected API for the getRelated() method
 * on Collection. They will fail until the GREEN phase implements the method.
 *
 * getRelated() allows traversing relationships to get related entities from a collection.
 */

import { describe, it, expect } from 'vitest'
import { DB, MemoryBackend } from '../../src'
import type { EntityId } from '../../src/types'

describe('collection.getRelated()', () => {
  it('returns related entities via reverse relationship', async () => {
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
    }, { storage: new MemoryBackend() })

    const user = await db.User.create({ email: 'alice@example.com', name: 'Alice' })

    // Create posts and link them to the user
    const post1 = await db.Post.create({ slug: 'post-1', title: 'Post 1' })
    const post2 = await db.Post.create({ slug: 'post-2', title: 'Post 2' })

    await db.Post.update(post1.$id as string, {
      $link: { author: user.$id as EntityId }
    })
    await db.Post.update(post2.$id as string, {
      $link: { author: user.$id as EntityId }
    })

    // Get related via reverse relationship
    const posts = await db.User.getRelated('alice@example.com', 'posts')

    expect(posts.total).toBe(2)
    expect(posts.items.map(p => p.slug)).toContain('post-1')
    expect(posts.items.map(p => p.slug)).toContain('post-2')

    db.dispose()
  })

  it('returns related entity via forward relationship', async () => {
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
    }, { storage: new MemoryBackend() })

    const user = await db.User.create({ email: 'alice@example.com', name: 'Alice' })
    const post = await db.Post.create({ slug: 'post-1', title: 'Post 1' })

    await db.Post.update(post.$id as string, {
      $link: { author: user.$id as EntityId }
    })

    // Get related via forward relationship
    const author = await db.Post.getRelated('post-1', 'author')

    expect(author.total).toBe(1)
    expect(author.items[0].email).toBe('alice@example.com')

    db.dispose()
  })

  it('returns empty result for no relationships', async () => {
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
    }, { storage: new MemoryBackend() })

    await db.User.create({ email: 'bob@example.com', name: 'Bob' })

    const posts = await db.User.getRelated('bob@example.com', 'posts')

    expect(posts.total).toBe(0)
    expect(posts.items).toEqual([])

    db.dispose()
  })

  it('accepts short ID when $id directive is set', async () => {
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
    }, { storage: new MemoryBackend() })

    const user = await db.User.create({ email: 'alice@example.com', name: 'Alice' })
    const post = await db.Post.create({ slug: 'my-post', title: 'My Post' })

    await db.Post.update(post.$id as string, {
      $link: { author: user.$id as EntityId }
    })

    // Use short ID
    const posts = await db.User.getRelated('alice@example.com', 'posts')
    expect(posts.total).toBe(1)

    // Also use short ID for Post
    const author = await db.Post.getRelated('my-post', 'author')
    expect(author.total).toBe(1)

    db.dispose()
  })

  it('throws error for invalid relationship name', async () => {
    const db = DB({
      User: {
        $id: 'email',
        email: 'string!#',
        name: 'string',
      }
    }, { storage: new MemoryBackend() })

    await db.User.create({ email: 'alice@example.com', name: 'Alice' })

    await expect(
      db.User.getRelated('alice@example.com', 'nonexistent')
    ).rejects.toThrow()

    db.dispose()
  })

  it('supports pagination with limit and cursor', async () => {
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
    }, { storage: new MemoryBackend() })

    const user = await db.User.create({ email: 'alice@example.com', name: 'Alice' })

    // Create multiple posts
    const post1 = await db.Post.create({ slug: 'post-1', title: 'Post 1' })
    const post2 = await db.Post.create({ slug: 'post-2', title: 'Post 2' })
    const post3 = await db.Post.create({ slug: 'post-3', title: 'Post 3' })

    await db.Post.update(post1.$id as string, { $link: { author: user.$id as EntityId } })
    await db.Post.update(post2.$id as string, { $link: { author: user.$id as EntityId } })
    await db.Post.update(post3.$id as string, { $link: { author: user.$id as EntityId } })

    // Get first page
    const page1 = await db.User.getRelated('alice@example.com', 'posts', { limit: 2 })

    expect(page1.items.length).toBe(2)
    expect(page1.total).toBe(3)
    expect(page1.hasMore).toBe(true)
    expect(page1.nextCursor).toBeDefined()

    // Get second page
    const page2 = await db.User.getRelated('alice@example.com', 'posts', {
      limit: 2,
      cursor: page1.nextCursor
    })

    expect(page2.items.length).toBe(1)
    expect(page2.hasMore).toBe(false)

    // Ensure no overlap between pages
    const page1Slugs = page1.items.map(p => p.slug)
    const page2Slugs = page2.items.map(p => p.slug)

    page2Slugs.forEach(slug => {
      expect(page1Slugs).not.toContain(slug)
    })

    db.dispose()
  })

  it('supports filtering related entities', async () => {
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
        status: 'string',
        author: '-> User'
      }
    }, { storage: new MemoryBackend() })

    const user = await db.User.create({ email: 'alice@example.com', name: 'Alice' })

    const post1 = await db.Post.create({ slug: 'post-1', title: 'Post 1', status: 'published' })
    const post2 = await db.Post.create({ slug: 'post-2', title: 'Post 2', status: 'draft' })
    const post3 = await db.Post.create({ slug: 'post-3', title: 'Post 3', status: 'published' })

    await db.Post.update(post1.$id as string, { $link: { author: user.$id as EntityId } })
    await db.Post.update(post2.$id as string, { $link: { author: user.$id as EntityId } })
    await db.Post.update(post3.$id as string, { $link: { author: user.$id as EntityId } })

    // Filter to only published posts
    const publishedPosts = await db.User.getRelated('alice@example.com', 'posts', {
      filter: { status: 'published' }
    })

    expect(publishedPosts.total).toBe(2)
    expect(publishedPosts.items.every(p => p.status === 'published')).toBe(true)

    db.dispose()
  })

  it('supports sorting related entities', async () => {
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
        views: 'int',
        author: '-> User'
      }
    }, { storage: new MemoryBackend() })

    const user = await db.User.create({ email: 'alice@example.com', name: 'Alice' })

    const post1 = await db.Post.create({ slug: 'post-1', title: 'Post 1', views: 100 })
    const post2 = await db.Post.create({ slug: 'post-2', title: 'Post 2', views: 50 })
    const post3 = await db.Post.create({ slug: 'post-3', title: 'Post 3', views: 200 })

    await db.Post.update(post1.$id as string, { $link: { author: user.$id as EntityId } })
    await db.Post.update(post2.$id as string, { $link: { author: user.$id as EntityId } })
    await db.Post.update(post3.$id as string, { $link: { author: user.$id as EntityId } })

    // Sort by views descending
    const sortedPosts = await db.User.getRelated('alice@example.com', 'posts', {
      sort: { views: -1 }
    })

    expect(sortedPosts.items[0].views).toBe(200)
    expect(sortedPosts.items[1].views).toBe(100)
    expect(sortedPosts.items[2].views).toBe(50)

    db.dispose()
  })

  it('returns hasMore false when all items fit in one page', async () => {
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
    }, { storage: new MemoryBackend() })

    const user = await db.User.create({ email: 'alice@example.com', name: 'Alice' })
    const post = await db.Post.create({ slug: 'post-1', title: 'Post 1' })

    await db.Post.update(post.$id as string, {
      $link: { author: user.$id as EntityId }
    })

    const posts = await db.User.getRelated('alice@example.com', 'posts', { limit: 10 })

    expect(posts.total).toBe(1)
    expect(posts.items.length).toBe(1)
    expect(posts.hasMore).toBe(false)
    expect(posts.nextCursor).toBeUndefined()

    db.dispose()
  })

  it('throws error when entity does not exist', async () => {
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
    }, { storage: new MemoryBackend() })

    await expect(
      db.User.getRelated('nonexistent@example.com', 'posts')
    ).rejects.toThrow()

    db.dispose()
  })
})
