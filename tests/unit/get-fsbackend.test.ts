import { describe, it, expect, afterEach } from 'vitest'
import { DB, FsBackend } from '../../src'
import { rm } from 'fs/promises'

describe('get() with FsBackend', () => {
  const dbPath = '.db-test-get'

  afterEach(async () => {
    await rm(dbPath, { recursive: true, force: true })
  })

  it('returns entity by full ID', async () => {
    const db = DB({
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
      }
    }, { storage: new FsBackend(dbPath) })

    await db.Post.create({ slug: 'hello-world', title: 'Hello World' })

    const post = await db.Post.get('post/hello-world')
    expect(post).not.toBeNull()
    expect(post?.title).toBe('Hello World')

    db.dispose()
  })

  it('returns entity by short ID with $id directive', async () => {
    const db = DB({
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
      }
    }, { storage: new FsBackend(dbPath) })

    await db.Post.create({ slug: 'hello-world', title: 'Hello World' })

    const post = await db.Post.get('hello-world')
    expect(post).not.toBeNull()
    expect(post?.title).toBe('Hello World')

    db.dispose()
  })

  it('get() and find() return consistent results', async () => {
    const db = DB({
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
      }
    }, { storage: new FsBackend(dbPath) })

    await db.Post.create({ slug: 'test-post', title: 'Test Post' })

    const byFind = await db.Post.find({ slug: 'test-post' })
    const byGet = await db.Post.get('test-post')

    expect(byFind.items[0]).not.toBeNull()
    expect(byGet).not.toBeNull()
    expect(byGet?.title).toBe(byFind.items[0]?.title)

    db.dispose()
  })

  // ============================================================================
  // Bug reproduction tests - get() fails after process restart
  // The key issue is that get() must reconstruct from persisted events, not
  // just the in-memory store. This simulates a "fresh" DB instance.
  // ============================================================================

  it('get() works after creating a new DB instance (simulating restart)', async () => {
    // First, create an entity with one DB instance
    const db1 = DB({
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
      }
    }, { storage: new FsBackend(dbPath) })

    await db1.Post.create({ slug: 'persist-test', title: 'Persist Test' })

    // Verify it works in the same session
    const immediateGet = await db1.Post.get('persist-test')
    expect(immediateGet).not.toBeNull()
    expect(immediateGet?.title).toBe('Persist Test')

    // Flush and dispose
    await db1.flush()
    db1.dispose()

    // Now create a NEW DB instance (simulating app restart/new process)
    const db2 = DB({
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
      }
    }, { storage: new FsBackend(dbPath) })

    // This is where the bug manifests - get() returns undefined
    // because the entity store is empty and event reconstruction fails
    const postAfterRestart = await db2.Post.get('persist-test')
    expect(postAfterRestart).not.toBeNull()
    expect(postAfterRestart?.title).toBe('Persist Test')

    db2.dispose()
  })

  it('find() works after creating a new DB instance but get() does not (bug)', async () => {
    // Create and persist an entity
    const db1 = DB({
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
      }
    }, { storage: new FsBackend(dbPath) })

    await db1.Post.create({ slug: 'find-get-test', title: 'Find Get Test' })
    await db1.flush()
    db1.dispose()

    // New DB instance
    const db2 = DB({
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
      }
    }, { storage: new FsBackend(dbPath) })

    // find() should work because it triggers ensureInitialized()
    const byFind = await db2.Post.find({ slug: 'find-get-test' })
    expect(byFind.items.length).toBe(1)
    expect(byFind.items[0]?.title).toBe('Find Get Test')

    // get() should also work - this is the bug if it fails
    const byGet = await db2.Post.get('find-get-test')
    expect(byGet).not.toBeNull()
    expect(byGet?.title).toBe('Find Get Test')

    db2.dispose()
  })

  it('get() by full ID works after restart', async () => {
    // Create and persist
    const db1 = DB({
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
      }
    }, { storage: new FsBackend(dbPath) })

    const created = await db1.Post.create({ slug: 'full-id-test', title: 'Full ID Test' })
    const fullId = created.$id // Should be 'post/full-id-test'
    await db1.flush()
    db1.dispose()

    // New DB instance
    const db2 = DB({
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
      }
    }, { storage: new FsBackend(dbPath) })

    // Get by full ID
    const byFullId = await db2.Post.get(fullId as string)
    expect(byFullId).not.toBeNull()
    expect(byFullId?.title).toBe('Full ID Test')

    db2.dispose()
  })

  it('get() with ULID-based ID works after restart', async () => {
    // Create with auto-generated ULID (no $id directive)
    const db1 = DB({
      Article: {
        title: 'string!',
        content: 'string',
      }
    }, { storage: new FsBackend(dbPath) })

    const created = await db1.Article.create({ title: 'ULID Test', content: 'Content' })
    const entityId = created.$id as string
    await db1.flush()
    db1.dispose()

    // New DB instance
    const db2 = DB({
      Article: {
        title: 'string!',
        content: 'string',
      }
    }, { storage: new FsBackend(dbPath) })

    // Get by the full ID
    const article = await db2.Article.get(entityId)
    expect(article).not.toBeNull()
    expect(article?.title).toBe('ULID Test')

    db2.dispose()
  })
})
