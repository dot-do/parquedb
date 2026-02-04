import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { DB, MemoryBackend, createResultArray, type ResultArray } from '../../src'

describe('elegant array results', () => {
  let db: ReturnType<typeof DB>

  beforeEach(async () => {
    db = DB({
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        status: 'string',
      }
    }, { storage: new MemoryBackend() })

    await db.Post.create({ slug: 'post-1', title: 'Post 1', status: 'published' })
    await db.Post.create({ slug: 'post-2', title: 'Post 2', status: 'published' })
    await db.Post.create({ slug: 'post-3', title: 'Post 3', status: 'draft' })
  })

  afterEach(() => {
    db.dispose()
  })

  it('find() returns array that can be iterated directly', async () => {
    const posts = await db.Post.find({ status: 'published' })

    // Should be directly iterable (no .items)
    const titles: string[] = []
    for (const post of posts) {
      titles.push(post.title)
    }
    expect(titles).toContain('Post 1')
    expect(titles).toContain('Post 2')
  })

  it('find() result has $total metadata', async () => {
    const posts = await db.Post.find({ status: 'published' })
    expect(posts.$total).toBe(2)
  })

  it('find() result has $next cursor when more pages exist', async () => {
    // Create more posts
    for (let i = 4; i <= 15; i++) {
      await db.Post.create({ slug: `post-${i}`, title: `Post ${i}`, status: 'published' })
    }

    const posts = await db.Post.find({ status: 'published' }, { limit: 5 })
    expect(posts.length).toBe(5)
    expect(posts.$total).toBe(14) // 12 new + 2 original published
    expect(posts.$next).toBeDefined()
  })

  it('$next is undefined when all items fit', async () => {
    const posts = await db.Post.find({ status: 'published' })
    expect(posts.$next).toBeUndefined()
  })

  it('$prev is undefined on first page', async () => {
    const posts = await db.Post.find({ status: 'published' })
    expect(posts.$prev).toBeUndefined()
  })

  it('array methods work normally', async () => {
    const posts = await db.Post.find({ status: 'published' }, { sort: { slug: 1 } })

    const mapped = posts.map(p => p.title)
    expect(mapped).toEqual(['Post 1', 'Post 2'])

    const filtered = posts.filter(p => p.slug === 'post-1')
    expect(filtered.length).toBe(1)

    const spread = [...posts]
    expect(spread.length).toBe(2)
  })

  it('length property works', async () => {
    const posts = await db.Post.find({ status: 'published' })
    expect(posts.length).toBe(2)
  })

  it('indexing works', async () => {
    const posts = await db.Post.find({ status: 'published' }, { sort: { slug: 1 } })
    expect(posts[0].title).toBe('Post 1')
    expect(posts[1].title).toBe('Post 2')
  })

  // Edge cases
  describe('edge cases', () => {
    it('handles empty results', async () => {
      const posts = await db.Post.find({ status: 'nonexistent' })
      expect(posts.length).toBe(0)
      expect(posts.$total).toBe(0)
      expect(posts.$next).toBeUndefined()
      expect([...posts]).toEqual([])
      expect(posts.map(p => p.title)).toEqual([])
    })

    it('handles single item results', async () => {
      const posts = await db.Post.find({ slug: 'post-1' })
      expect(posts.length).toBe(1)
      expect(posts.$total).toBe(1)
      expect(posts.$next).toBeUndefined()
      expect(posts[0].title).toBe('Post 1')
    })
  })

  // JSON serialization
  describe('JSON serialization', () => {
    it('JSON.stringify does not include metadata properties', async () => {
      const posts = await db.Post.find({ status: 'published' }, { sort: { slug: 1 } })
      const json = JSON.stringify(posts)
      const parsed = JSON.parse(json)

      // Should be a plain array without metadata
      expect(Array.isArray(parsed)).toBe(true)
      expect(parsed.length).toBe(2)
      expect(parsed[0].title).toBe('Post 1')
      // Metadata should NOT be in the JSON
      expect(parsed.$total).toBeUndefined()
      expect(parsed.$next).toBeUndefined()
      expect(parsed.$prev).toBeUndefined()
      expect(parsed.items).toBeUndefined()
      expect(parsed.total).toBeUndefined()
    })

    it('JSON.stringify works for empty results', async () => {
      const posts = await db.Post.find({ status: 'nonexistent' })
      const json = JSON.stringify(posts)
      expect(json).toBe('[]')
    })
  })

  // Direct createResultArray tests
  describe('createResultArray utility', () => {
    it('supports $prev cursor for bidirectional pagination', () => {
      const items = [{ id: 1 }, { id: 2 }]
      const result = createResultArray(items, {
        total: 100,
        next: 'next_cursor_abc',
        prev: 'prev_cursor_xyz',
      })

      expect(result.$total).toBe(100)
      expect(result.$next).toBe('next_cursor_abc')
      expect(result.$prev).toBe('prev_cursor_xyz')
      expect(result.length).toBe(2)
    })

    it('$prev is undefined when not provided', () => {
      const result = createResultArray([{ id: 1 }], {
        total: 1,
        next: undefined,
      })

      expect(result.$prev).toBeUndefined()
    })

    it('supports "in" operator for all metadata properties', () => {
      const result = createResultArray([{ id: 1 }], {
        total: 10,
        next: 'abc',
        prev: 'xyz',
      })

      expect('$total' in result).toBe(true)
      expect('$next' in result).toBe(true)
      expect('$prev' in result).toBe(true)
      expect('items' in result).toBe(true)
      expect('total' in result).toBe(true)
      expect('hasMore' in result).toBe(true)
      expect('nextCursor' in result).toBe(true)
    })

    it('ResultArray type works correctly', () => {
      interface Item { id: number; name: string }
      const items: Item[] = [{ id: 1, name: 'test' }]
      const result: ResultArray<Item> = createResultArray(items, { total: 1 })

      // Type checking - these should compile
      const first: Item = result[0]
      const mapped: number[] = result.map(x => x.id)
      const total: number = result.$total

      expect(first.name).toBe('test')
      expect(mapped).toEqual([1])
      expect(total).toBe(1)
    })
  })
})
