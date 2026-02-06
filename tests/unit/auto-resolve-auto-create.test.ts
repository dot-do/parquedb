/**
 * Auto-Resolve Relationships with autoCreate Tests for ParqueDB
 *
 * Tests for the async autoResolveRelationships function and the autoCreate option.
 *
 * When autoCreate is enabled, if a create operation references an entity that doesn't
 * exist yet, it should be automatically created in the target namespace rather than
 * throwing a RelationshipResolutionError.
 *
 * These tests cover:
 * 1. Backward-compatible async behavior (existing sync tests still pass)
 * 2. autoCreate via collection.create() with { autoCreate: true }
 * 3. autoCreate creates entities across namespaces
 * 4. autoCreate with ingestStream
 */

import { describe, it, expect, afterEach, beforeEach } from 'vitest'
import { DB } from '../../src/db'
import { MemoryBackend } from '../../src/storage'
import { clearGlobalStorage } from '../../src/Collection'

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create an async generator from an array
 */
async function* arrayToAsyncIterable<T>(items: T[]): AsyncIterable<T> {
  for (const item of items) {
    yield item
  }
}

// =============================================================================
// Section 1: Async autoResolveRelationships behavior (backward compatibility)
// =============================================================================

describe('async autoResolveRelationships behavior', () => {
  afterEach(() => {
    clearGlobalStorage()
  })

  it('still resolves existing entities (backward compatible)', async () => {
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
        tags: '-> Tag[]',
      },
    }, { storage })

    // Pre-create the target entity
    await db.Tag.create({ slug: 'existing-tag', name: 'Existing Tag' })

    // Create article referencing an existing tag by short ID
    // This should still work as before (function is now async but same behavior)
    const article = await db.Article.create({
      slug: 'my-article',
      title: 'My Article',
      tags: ['existing-tag'],
    })

    // Verify the relationship was created
    const localId = article.$id.split('/')[1]
    const tags = await db.getRelated('article', localId!, 'tags')
    expect(tags.items).toHaveLength(1)
    expect(tags.items[0]!.$id).toBe('tag/existing-tag')
  })

  it('still throws on missing entity without autoCreate', async () => {
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
        tags: '-> Tag[]',
      },
    }, { storage })

    // Create article referencing a tag that does NOT exist
    // Without autoCreate, this should still throw RelationshipResolutionError
    await expect(db.Article.create({
      slug: 'my-article',
      title: 'My Article',
      tags: ['nonexistent'],
    })).rejects.toThrow(/not found|does not exist/i)
  })

  it('reverse relationships are never auto-resolved', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      User: {
        $id: 'email',
        email: 'string!#',
        name: 'string',
        posts: '<- Post.author[]',
      },
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        author: '-> User',
      },
    }, { storage })

    // Create a User with a reverse relationship field (posts).
    // Reverse relationships should be ignored/passed through — they are
    // populated by the indexing system, not set by user data.
    // This should NOT throw, and the posts value should be ignored.
    const user = await db.User.create({
      email: 'alice@test.com',
      name: 'Alice',
      posts: ['some-post-id'] as any,
    })

    expect(user.$id).toBe('user/alice@test.com')
    // The reverse relationship should not cause a resolution error
  })

  it('null/undefined relationship values are skipped', async () => {
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
        author: '-> User',
      },
    }, { storage })

    // Create a post with author: null — should not throw
    const post1 = await db.Post.create({
      slug: 'no-author-null',
      title: 'No Author (null)',
      author: null as any,
    })
    expect(post1.$id).toBe('post/no-author-null')

    // Create a post with author: undefined — should not throw
    const post2 = await db.Post.create({
      slug: 'no-author-undefined',
      title: 'No Author (undefined)',
      author: undefined as any,
    })
    expect(post2.$id).toBe('post/no-author-undefined')
  })
})

// =============================================================================
// Section 2: autoCreate integration - creates across namespaces
// =============================================================================

describe('autoCreate integration - creates across namespaces', () => {
  afterEach(() => {
    clearGlobalStorage()
  })

  it('auto-created entities are visible via collection queries', async () => {
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
        author: '-> User',
      },
    }, { storage })

    // Create a Post referencing a User that doesn't exist yet.
    // With autoCreate: true, the User should be auto-created.
    const post = await db.Post.create(
      {
        slug: 'hello-world',
        title: 'Hello World',
        author: 'alice@test.com',
      },
      { autoCreate: true },
    )

    expect(post.$id).toBe('post/hello-world')

    // The auto-created User should now be queryable
    const users = await db.User.find({})
    expect(users.items).toHaveLength(1)
    expect(users.items[0]!.$id).toBe('user/alice@test.com')

    // The User's name should be derived from the ID (email)
    expect(users.items[0]!.name).toBe('alice@test.com')
  })

  it('auto-created entities from objects have full data', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      ZapierCategory: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string',
        description: 'string',
      },
      ZapierApp: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        categories: '-> ZapierCategory[]',
      },
    }, { storage })

    // Create a ZapierApp with categories as objects (not just string IDs).
    // With autoCreate: true, the category objects should be used to create
    // full ZapierCategory entities.
    const app = await db.ZapierApp.create(
      {
        slug: 'slack',
        name: 'Slack',
        categories: [
          { slug: 'crm', title: 'CRM', description: 'CRM tools' },
          { slug: 'communication', title: 'Communication', description: 'Messaging tools' },
        ] as any,
      },
      { autoCreate: true },
    )

    expect(app.$id).toBe('zapierapp/slack')

    // The auto-created categories should have full data
    const crm = await db.ZapierCategory.get('crm')
    expect(crm).not.toBeNull()
    expect(crm!.title).toBe('CRM')
    expect(crm!.description).toBe('CRM tools')

    const comm = await db.ZapierCategory.get('communication')
    expect(comm).not.toBeNull()
    expect(comm!.title).toBe('Communication')
    expect(comm!.description).toBe('Messaging tools')
  })

  it('auto-create works with single relationship (not array)', async () => {
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
        author: '-> User',
      },
    }, { storage })

    // Create a Post with author as an object (single relationship, not array)
    // The User should be auto-created from the object data
    const post = await db.Post.create(
      {
        slug: 'bobs-post',
        title: "Bob's First Post",
        author: { email: 'bob@test.com', name: 'Bob' } as any,
      },
      { autoCreate: true },
    )

    expect(post.$id).toBe('post/bobs-post')

    // Verify the User was auto-created with the correct data
    const bob = await db.User.get('bob@test.com')
    expect(bob).not.toBeNull()
    expect(bob!.name).toBe('Bob')
    expect(bob!.$id).toBe('user/bob@test.com')
  })

  it('auto-created entities get proper $type from target namespace', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      ZapierCategory: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string',
      },
      ZapierApp: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        categories: '-> ZapierCategory[]',
      },
    }, { storage })

    // Create app with autoCreate
    await db.ZapierApp.create(
      {
        slug: 'github',
        name: 'GitHub',
        categories: [{ slug: 'developer-tools', title: 'Developer Tools' }] as any,
      },
      { autoCreate: true },
    )

    // The auto-created entity should have $type derived from target type name
    const category = await db.ZapierCategory.get('developer-tools')
    expect(category).not.toBeNull()
    expect(category!.$type).toBe('ZapierCategory')
  })

  it('handles mix of strings and objects in same array', async () => {
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
        tags: '-> Tag[]',
      },
    }, { storage })

    // Pre-create one tag
    await db.Tag.create({ slug: 'existing-slug', name: 'Existing' })

    // Create article with a mix of string references and object data
    // - 'existing-slug' should resolve to the pre-existing Tag
    // - { slug: 'new-one', name: 'New' } should auto-create a new Tag
    const article = await db.Article.create(
      {
        slug: 'mixed-article',
        title: 'Mixed References',
        tags: ['existing-slug', { slug: 'new-one', name: 'New' }] as any,
      },
      { autoCreate: true },
    )

    expect(article.$id).toBe('article/mixed-article')

    // Verify both tags exist
    const existingTag = await db.Tag.get('existing-slug')
    expect(existingTag).not.toBeNull()
    expect(existingTag!.name).toBe('Existing')

    const newTag = await db.Tag.get('new-one')
    expect(newTag).not.toBeNull()
    expect(newTag!.name).toBe('New')

    // Verify both are linked to the article
    const localId = article.$id.split('/')[1]
    const tags = await db.getRelated('article', localId!, 'tags')
    expect(tags.items).toHaveLength(2)
    const tagIds = tags.items.map((t: any) => t.$id).sort()
    expect(tagIds).toEqual(['tag/existing-slug', 'tag/new-one'])
  })

  it('does not re-create entity that already exists when autoCreate is true', async () => {
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
        author: '-> User',
      },
    }, { storage })

    // Pre-create the user
    await db.User.create({ email: 'alice@test.com', name: 'Alice Original' })

    // Create post with autoCreate — the existing user should NOT be overwritten
    await db.Post.create(
      {
        slug: 'post-1',
        title: 'Post 1',
        author: 'alice@test.com',
      },
      { autoCreate: true },
    )

    // Verify original user data is preserved (not overwritten)
    const alice = await db.User.get('alice@test.com')
    expect(alice).not.toBeNull()
    expect(alice!.name).toBe('Alice Original')

    // Only one user should exist
    const users = await db.User.find({})
    expect(users.items).toHaveLength(1)
  })
})

// =============================================================================
// Section 3: autoCreate with ingestStream
// =============================================================================

describe('autoCreate with ingestStream', () => {
  let db: ReturnType<typeof DB>

  beforeEach(() => {
    const storage = new MemoryBackend()
    db = DB({
      ZapierApp: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        categories: '-> ZapierCategory[]',
      },
      ZapierCategory: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string',
        description: 'string',
        apps: '<- ZapierApp.categories[]',
      },
    }, { storage })
  })

  afterEach(() => {
    clearGlobalStorage()
  })

  it('ingestStream passes autoCreate to create calls', async () => {
    // Ingest apps that reference categories which don't exist yet.
    // With autoCreate: true, the referenced categories should be auto-created.
    const apps = [
      {
        slug: 'slack',
        name: 'Slack',
        categories: ['communication', 'team-chat'],
      },
      {
        slug: 'github',
        name: 'GitHub',
        categories: ['developer-tools', 'version-control'],
      },
    ]

    const result = await db.ingestStream(
      'zapierapp',
      arrayToAsyncIterable(apps),
      { autoCreate: true },
    )

    expect(result.insertedCount).toBe(2)

    // Verify the apps were created
    const foundApps = await db.ZapierApp.find()
    expect(foundApps.items).toHaveLength(2)

    // Verify auto-created categories exist
    const foundCategories = await db.ZapierCategory.find()
    expect(foundCategories.items).toHaveLength(4)

    // Check specific categories
    const commCategory = await db.ZapierCategory.get('communication')
    expect(commCategory).not.toBeNull()
    expect(commCategory!.$type).toBe('ZapierCategory')
  })

  it('ingestStream with autoCreate and object references', async () => {
    // Ingest apps with categories as objects (full data for auto-creation)
    const apps = [
      {
        slug: 'notion',
        name: 'Notion',
        categories: [
          { slug: 'productivity', title: 'Productivity', description: 'Productivity tools' },
          { slug: 'note-taking', title: 'Note Taking', description: 'Note taking apps' },
        ],
      },
    ]

    const result = await db.ingestStream(
      'zapierapp',
      arrayToAsyncIterable(apps),
      { autoCreate: true },
    )

    expect(result.insertedCount).toBe(1)

    // Verify auto-created categories have full data from the objects
    const productivity = await db.ZapierCategory.get('productivity')
    expect(productivity).not.toBeNull()
    expect(productivity!.title).toBe('Productivity')
    expect(productivity!.description).toBe('Productivity tools')
  })

  it('ingestStream without autoCreate still throws on missing entities', async () => {
    const apps = [
      {
        slug: 'slack',
        name: 'Slack',
        categories: ['nonexistent-category'],
      },
    ]

    // Without autoCreate, referencing nonexistent entities should fail
    const result = await db.ingestStream(
      'zapierapp',
      arrayToAsyncIterable(apps),
      { ordered: true },
    )

    // The ingest should report failures (either via errors array or thrown error)
    expect(result.failedCount).toBeGreaterThan(0)
    expect(result.insertedCount).toBe(0)
  })
})
