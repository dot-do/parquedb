/**
 * Auto-Hydrate Relationships Tests for ParqueDB
 *
 * Tests for automatic hydration (population) of relationships when calling get().
 *
 * When a schema defines relationships like `author: '-> User'` (forward) or
 * `posts: '<- Post.author[]'` (reverse), the get() method should automatically
 * hydrate these relationships with the full entity objects, not just IDs.
 *
 * This is the RED phase - these tests should FAIL until the feature is implemented.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { DB, MemoryBackend } from '../../src'

describe('auto-hydrate relationships', () => {
  let db: ReturnType<typeof DB>

  beforeEach(async () => {
    db = DB({
      User: {
        $id: 'email',
        email: 'string!#',
        name: 'string!',
        posts: '<- Post.author[]',
        manager: '-> User',
      },
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        author: '-> User',
      }
    }, { storage: new MemoryBackend() })

    await db.User.create({ email: 'alice@example.com', name: 'Alice' })
    await db.User.create({ email: 'bob@example.com', name: 'Bob', manager: 'alice@example.com' })

    await db.Post.create({ slug: 'post-1', title: 'Post 1', author: 'alice@example.com' })
    await db.Post.create({ slug: 'post-2', title: 'Post 2', author: 'alice@example.com' })
    await db.Post.create({ slug: 'post-3', title: 'Post 3', author: 'bob@example.com' })
  })

  afterEach(() => {
    db.dispose()
  })

  it('forward relationships are auto-populated on get()', async () => {
    const post = await db.Post.get('post-1')

    // author should be the full User object, not just ID
    expect(post?.author).toBeDefined()
    expect(typeof post?.author).toBe('object')
    expect(post?.author?.email).toBe('alice@example.com')
    expect(post?.author?.name).toBe('Alice')
  })

  it('reverse relationships are auto-populated as array', async () => {
    const alice = await db.User.get('alice@example.com')

    // posts should be an array of Post objects
    expect(Array.isArray(alice?.posts)).toBe(true)
    expect(alice?.posts?.length).toBe(2)
    expect(alice?.posts?.[0]?.title).toBeDefined()
  })

  it('reverse relationships have $total metadata', async () => {
    const alice = await db.User.get('alice@example.com')

    expect(alice?.posts?.$total).toBe(2)
  })

  it('reverse relationships have $next when paginated', async () => {
    // Create user with many posts
    await db.User.create({ email: 'prolific@example.com', name: 'Prolific' })
    for (let i = 1; i <= 25; i++) {
      await db.Post.create({
        slug: `many-post-${i}`,
        title: `Many Post ${i}`,
        author: 'prolific@example.com'
      })
    }

    const user = await db.User.get('prolific@example.com', { maxInbound: 10 })

    expect(user?.posts?.length).toBe(10)
    expect(user?.posts?.$total).toBe(25)
    expect(user?.posts?.$next).toBeDefined()
  })

  it('respects depth: 0 to skip hydration', async () => {
    const post = await db.Post.get('post-1', { depth: 0 })

    // author should be just the ID string, not hydrated
    expect(typeof post?.author).toBe('string')
    expect(post?.author).toBe('user/alice@example.com')
  })

  it('nested forward relationship works', async () => {
    const bob = await db.User.get('bob@example.com')

    // manager should be hydrated
    expect(bob?.manager?.email).toBe('alice@example.com')
    expect(bob?.manager?.name).toBe('Alice')
  })

  it('null forward relationship is handled', async () => {
    const alice = await db.User.get('alice@example.com')

    // Alice has no manager
    expect(alice?.manager).toBeNull()
  })
})
