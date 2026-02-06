/**
 * Auto-Create Linked Records Tests for ParqueDB
 *
 * Tests for automatic creation of target entities when creating an entity with
 * a relationship field and `autoCreate: true` option.
 *
 * When a schema defines a relationship like `author: '-> User'`, the create()
 * method should auto-create missing target entities as stubs when
 * { autoCreate: true } is passed.
 *
 * This is the RED phase - these tests should FAIL until the feature is implemented.
 * The `autoCreate` option does not exist yet on create().
 */

import { describe, it, expect, afterEach } from 'vitest'
import { DB } from '../../src/db'
import { MemoryBackend } from '../../src/storage'
import { clearGlobalStorage } from '../../src/Collection'

// =============================================================================
// Section 1: autoCreate with string IDs
// =============================================================================

describe('autoCreate with string IDs', () => {
  afterEach(() => {
    clearGlobalStorage()
  })

  it('auto-creates stub entity when target does not exist', async () => {
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

    // User 'alice@example.com' does NOT exist yet
    // Create a Post with autoCreate: true — should auto-create User stub
    const post = await db.Post.create({
      slug: 'hello-world',
      title: 'Hello',
      author: 'alice@example.com'
    }, { autoCreate: true })

    // Verify the post was created
    expect(post.$id).toBe('post/hello-world')

    // Verify the auto-created User stub exists
    const user = await db.User.get('alice@example.com')
    expect(user).not.toBeNull()
    expect(user!.$id).toBe('user/alice@example.com')

    // Verify the relationship is properly linked
    const localId = post.$id.split('/')[1]
    const author = await db.getRelated('post', localId!, 'author')
    expect(author.items[0].$id).toBe('user/alice@example.com')
  })

  it('resolves existing entity without creating duplicate', async () => {
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

    // Create User first
    await db.User.create({ email: 'alice@example.com', name: 'Alice' })

    // Create Post with autoCreate: true — should find existing User, not create duplicate
    const post = await db.Post.create({
      slug: 'hello-world',
      title: 'Hello',
      author: 'alice@example.com'
    }, { autoCreate: true })

    // Verify relationship links to existing user
    const localId = post.$id.split('/')[1]
    const author = await db.getRelated('post', localId!, 'author')
    expect(author.items[0].$id).toBe('user/alice@example.com')
    expect(author.items[0].name).toBe('Alice')

    // Verify only 1 User entity exists
    const allUsers = await db.User.find()
    expect(allUsers.items).toHaveLength(1)
  })

  it('auto-creates stub for array relationships', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Tag: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        articles: '<- Article.tags[]'
      },
      Article: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        tags: '-> Tag[]'
      }
    }, { storage })

    // Neither tag exists yet
    const article = await db.Article.create({
      slug: 'my-article',
      title: 'My Article',
      tags: ['javascript', 'typescript']
    }, { autoCreate: true })

    // Verify both Tag stubs were auto-created
    const jsTag = await db.Tag.get('javascript')
    expect(jsTag).not.toBeNull()
    expect(jsTag!.$id).toBe('tag/javascript')

    const tsTag = await db.Tag.get('typescript')
    expect(tsTag).not.toBeNull()
    expect(tsTag!.$id).toBe('tag/typescript')

    // Verify Article.tags relationship links to both
    const localId = article.$id.split('/')[1]
    const tags = await db.getRelated('article', localId!, 'tags')
    expect(tags.items).toHaveLength(2)
    expect(tags.items.map(t => t.$id).sort()).toEqual([
      'tag/javascript',
      'tag/typescript'
    ])
  })

  it('mixed existing and missing in array relationships', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Tag: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        articles: '<- Article.tags[]'
      },
      Article: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        tags: '-> Tag[]'
      }
    }, { storage })

    // Create only 'javascript' tag first
    await db.Tag.create({ slug: 'javascript', name: 'JavaScript' })

    // Create Article with both existing and missing tags
    const article = await db.Article.create({
      slug: 'my-article',
      title: 'My Article',
      tags: ['javascript', 'typescript']
    }, { autoCreate: true })

    // 'javascript' should be resolved from existing, 'typescript' should be auto-created
    const jsTag = await db.Tag.get('javascript')
    expect(jsTag!.name).toBe('JavaScript') // original name preserved

    const tsTag = await db.Tag.get('typescript')
    expect(tsTag).not.toBeNull()
    expect(tsTag!.$id).toBe('tag/typescript')

    // Verify Article has 2 tag relationships
    const localId = article.$id.split('/')[1]
    const tags = await db.getRelated('article', localId!, 'tags')
    expect(tags.items).toHaveLength(2)
  })

  it('auto-created stub has correct $type and audit fields', async () => {
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

    // Create Post with autoCreate: true — auto-creates User stub
    await db.Post.create({
      slug: 'test-post',
      title: 'Test',
      author: 'nobody'
    }, { autoCreate: true })

    // Get the auto-created User stub
    const user = await db.User.get('nobody')
    expect(user).not.toBeNull()

    // Verify $type is 'User'
    expect(user!.$type).toBe('User')

    // Verify audit fields exist
    expect(user!.createdAt).toBeInstanceOf(Date)
    expect(user!.createdBy).toBeDefined()
    expect(user!.updatedAt).toBeInstanceOf(Date)
    expect(user!.updatedBy).toBeDefined()
    expect(user!.version).toBe(1)
  })

  it('is idempotent - second create finds already-created stub', async () => {
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

    // First create: auto-creates User stub for 'alice'
    await db.Post.create({
      slug: 'post-1',
      title: 'Post 1',
      author: 'alice'
    }, { autoCreate: true })

    // Second create: should find already-created stub, not create duplicate
    await db.Post.create({
      slug: 'post-2',
      title: 'Post 2',
      author: 'alice'
    }, { autoCreate: true })

    // Verify only 1 User entity with $id 'user/alice'
    const allUsers = await db.User.find()
    expect(allUsers.items).toHaveLength(1)
    expect(allUsers.items[0].$id).toBe('user/alice')
  })
})

// =============================================================================
// Section 2: autoCreate with object values
// =============================================================================

describe('autoCreate with object values', () => {
  afterEach(() => {
    clearGlobalStorage()
  })

  it('auto-creates entity with full data from object', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      ZapierCategory: {
        $id: 'slug',
        $name: 'title',
        slug: 'string!#',
        title: 'string!',
        description: 'string',
        apps: '<- ZapierApp.categories[]'
      },
      ZapierApp: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        categories: '-> ZapierCategory[]'
      }
    }, { storage })

    // Create ZapierApp with categories as array of objects
    const app = await db.ZapierApp.create({
      slug: 'salesforce',
      name: 'Salesforce',
      categories: [{ slug: 'crm', title: 'CRM', description: 'Sales tools' }]
    }, { autoCreate: true })

    // Verify ZapierCategory 'crm' was auto-created with all provided fields
    const category = await db.ZapierCategory.get('crm')
    expect(category).not.toBeNull()
    expect(category!.$id).toBe('zapiercategory/crm')
    expect(category!.title).toBe('CRM')
    expect(category!.description).toBe('Sales tools')

    // Verify the relationship is linked
    const localId = app.$id.split('/')[1]
    const categories = await db.getRelated('zapierapp', localId!, 'categories')
    expect(categories.items).toHaveLength(1)
    expect(categories.items[0].$id).toBe('zapiercategory/crm')
  })

  it('uses target $id directive to extract ID from object', async () => {
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

    // Pass object with the $id field ('email') of the target type
    const post = await db.Post.create({
      slug: 'test-post',
      title: 'Test',
      author: { email: 'alice@test.com', name: 'Alice' }
    }, { autoCreate: true })

    // Verify entity created with $id derived from the 'email' field
    const user = await db.User.get('alice@test.com')
    expect(user).not.toBeNull()
    expect(user!.$id).toBe('user/alice@test.com')
    expect(user!.name).toBe('Alice')

    // Verify relationship
    const localId = post.$id.split('/')[1]
    const author = await db.getRelated('post', localId!, 'author')
    expect(author.items[0].$id).toBe('user/alice@test.com')
  })

  it('resolves existing entity when object ID matches', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      ZapierCategory: {
        $id: 'slug',
        $name: 'title',
        slug: 'string!#',
        title: 'string!',
        description: 'string',
        apps: '<- ZapierApp.categories[]'
      },
      ZapierApp: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        categories: '-> ZapierCategory[]'
      }
    }, { storage })

    // Pre-create ZapierCategory with slug: 'crm', title: 'CRM'
    await db.ZapierCategory.create({ slug: 'crm', title: 'CRM' })

    // Create ZapierApp with categories that reference existing 'crm'
    await db.ZapierApp.create({
      slug: 'salesforce',
      name: 'Salesforce',
      categories: [{ slug: 'crm', title: 'Updated CRM' }]
    }, { autoCreate: true })

    // Existing 'crm' category should be used (no overwrite), title stays 'CRM'
    const category = await db.ZapierCategory.get('crm')
    expect(category).not.toBeNull()
    expect(category!.title).toBe('CRM') // original title preserved, NOT 'Updated CRM'

    // Verify only 1 ZapierCategory entity
    const allCategories = await db.ZapierCategory.find()
    expect(allCategories.items).toHaveLength(1)
  })

  it('skips objects without recognizable ID field', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      ZapierCategory: {
        $id: 'slug',
        $name: 'title',
        slug: 'string!#',
        title: 'string!',
        apps: '<- ZapierApp.categories[]'
      },
      ZapierApp: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        categories: '-> ZapierCategory[]'
      }
    }, { storage })

    // Pass an object without the $id field ('slug') - should be silently skipped
    const app = await db.ZapierApp.create({
      slug: 'salesforce',
      name: 'Salesforce',
      categories: [{ random: 'stuff' }]
    }, { autoCreate: true })

    // No ZapierCategory should have been created
    const allCategories = await db.ZapierCategory.find()
    expect(allCategories.items).toHaveLength(0)

    // App should still be created, but with no category relationships
    const localId = app.$id.split('/')[1]
    const categories = await db.getRelated('zapierapp', localId!, 'categories')
    expect(categories.items).toHaveLength(0)
  })

  it('auto-creates multiple entities from array of objects', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      ZapierCategory: {
        $id: 'slug',
        $name: 'title',
        slug: 'string!#',
        title: 'string!',
        description: 'string',
        apps: '<- ZapierApp.categories[]'
      },
      ZapierApp: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        categories: '-> ZapierCategory[]'
      }
    }, { storage })

    // Create ZapierApp with multiple category objects
    const app = await db.ZapierApp.create({
      slug: 'hubspot',
      name: 'HubSpot',
      categories: [
        { slug: 'crm', title: 'CRM' },
        { slug: 'marketing', title: 'Marketing' },
        { slug: 'analytics', title: 'Analytics' }
      ]
    }, { autoCreate: true })

    // Verify all 3 ZapierCategory entities were created
    const allCategories = await db.ZapierCategory.find()
    expect(allCategories.items).toHaveLength(3)

    const crm = await db.ZapierCategory.get('crm')
    expect(crm).not.toBeNull()
    expect(crm!.title).toBe('CRM')

    const marketing = await db.ZapierCategory.get('marketing')
    expect(marketing).not.toBeNull()
    expect(marketing!.title).toBe('Marketing')

    const analytics = await db.ZapierCategory.get('analytics')
    expect(analytics).not.toBeNull()
    expect(analytics!.title).toBe('Analytics')

    // Verify ZapierApp is linked to all 3
    const localId = app.$id.split('/')[1]
    const categories = await db.getRelated('zapierapp', localId!, 'categories')
    expect(categories.items).toHaveLength(3)
    expect(categories.items.map(c => c.$id).sort()).toEqual([
      'zapiercategory/analytics',
      'zapiercategory/crm',
      'zapiercategory/marketing'
    ])
  })
})

// =============================================================================
// Section 3: autoCreate defaults and backward compatibility
// =============================================================================

describe('autoCreate defaults and backward compatibility', () => {
  afterEach(() => {
    clearGlobalStorage()
  })

  it('defaults to false - throws on missing target without autoCreate', async () => {
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

    // Existing behavior: create Post with author referencing non-existent User
    // WITHOUT autoCreate — should throw
    await expect(db.Post.create({
      slug: 'orphan-post',
      title: 'Orphan',
      author: 'nobody@example.com'
    })).rejects.toThrow(/not found|does not exist/i)
  })

  it('explicit autoCreate: false throws on missing target', async () => {
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

    // Explicit autoCreate: false — should also throw on missing target
    await expect(db.Post.create({
      slug: 'orphan-post',
      title: 'Orphan',
      author: 'nobody@example.com'
    }, { autoCreate: false })).rejects.toThrow(/not found|does not exist/i)
  })
})
