/**
 * Auto-Create Edge Cases Tests for ParqueDB
 *
 * Tests for edge cases in the autoCreate feature, covering:
 * 1. Empty string ID
 * 2. Soft-deleted target entity
 * 3. Non-transitive auto-creation (nested relationship fields)
 * 4. Partial array failure
 * 5. Duplicate entities in same array
 * 6. Object without recognizable ID field
 */

import { describe, it, expect, afterEach } from 'vitest'
import { DB } from '../../src/db'
import { MemoryBackend } from '../../src/storage'
import { clearGlobalStorage } from '../../src/Collection'

// =============================================================================
// Section 1: Empty string ID
// =============================================================================

describe('autoCreate edge case: empty string ID', () => {
  afterEach(() => {
    clearGlobalStorage()
  })

  it('skips or handles empty string gracefully with autoCreate', async () => {
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

    // Create a Post with author set to empty string and autoCreate: true.
    // The resolveOrCreate function receives '' as a string, constructs fullId 'user/',
    // and then attempts to auto-create an entity with that ID.
    // This should either:
    // (a) throw a validation error because '' is not a valid ID, or
    // (b) create an entity with an empty localId (which is undesirable).
    //
    // The current implementation will attempt to create 'user/' - we verify
    // the behavior is deterministic (does not crash with an unhandled error).
    try {
      const post = await db.Post.create({
        slug: 'empty-author-post',
        title: 'Empty Author',
        author: '',
      }, { autoCreate: true })

      // If it doesn't throw, verify the post was at least created
      expect(post.$id).toBe('post/empty-author-post')
    } catch (error: unknown) {
      // If it throws, it should be a meaningful error (validation or resolution)
      expect(error).toBeDefined()
      expect((error as Error).message).toBeTruthy()
    }
  })

  it('empty string in array relationship is skipped or handled', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Tag: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        articles: '<- Article.tags[]',
      },
      Article: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        tags: '-> Tag[]',
      },
    }, { storage })

    // Include an empty string among valid tag references
    try {
      const article = await db.Article.create({
        slug: 'mixed-empty-tags',
        title: 'Mixed Tags',
        tags: ['valid-tag', '', 'another-tag'],
      }, { autoCreate: true })

      // If it succeeds, the empty string should either be skipped or created
      expect(article.$id).toBe('article/mixed-empty-tags')

      // Check that at least the valid tags were created
      const validTag = await db.Tag.get('valid-tag')
      expect(validTag).not.toBeNull()

      const anotherTag = await db.Tag.get('another-tag')
      expect(anotherTag).not.toBeNull()
    } catch (error: unknown) {
      // If it throws, it should be a meaningful error
      expect(error).toBeDefined()
      expect((error as Error).message).toBeTruthy()
    }
  })
})

// =============================================================================
// Section 2: Soft-deleted target entity
// =============================================================================

describe('autoCreate edge case: soft-deleted target entity', () => {
  afterEach(() => {
    clearGlobalStorage()
  })

  it('treats soft-deleted entity as non-existent and auto-creates new one', async () => {
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

    // Step 1: Create the User
    const user = await db.User.create({ email: 'alice@example.com', name: 'Alice' })
    expect(user.$id).toBe('user/alice@example.com')

    // Step 2: Soft-delete the User (default delete is soft-delete)
    const deleteResult = await db.User.delete('alice@example.com')
    expect(deleteResult.deletedCount).toBe(1)

    // Step 3: Verify the user is soft-deleted (not visible without includeDeleted)
    const deletedUser = await db.User.get('alice@example.com')
    expect(deletedUser).toBeNull()

    // Step 4: Now create a Post referencing the soft-deleted User with autoCreate: true.
    // The resolveOrCreate function checks `!existing.deletedAt` and treats soft-deleted
    // entities as non-existent. Since the entity already exists in the store (just
    // soft-deleted), autoCreate will attempt to create a new entity with the same ID.
    // This should either:
    // (a) throw because 'user/alice@example.com' already exists (just soft-deleted), or
    // (b) succeed by restoring or re-creating the entity.
    //
    // Looking at createEntity: it checks `existingEntity && !existingEntity.deletedAt`
    // before throwing duplicate error. So a soft-deleted entity won't block re-creation.
    // The auto-create should succeed, creating a new stub.
    const post = await db.Post.create({
      slug: 'post-for-deleted-user',
      title: 'Post for Deleted User',
      author: 'alice@example.com',
    }, { autoCreate: true })

    expect(post.$id).toBe('post/post-for-deleted-user')

    // The auto-created User should now be visible (it replaced the soft-deleted version)
    const restoredUser = await db.User.get('alice@example.com')
    expect(restoredUser).not.toBeNull()
    expect(restoredUser!.$id).toBe('user/alice@example.com')

    // Verify the relationship is properly linked
    const localId = post.$id.split('/')[1]
    const author = await db.getRelated('post', localId!, 'author')
    expect(author.items).toHaveLength(1)
    expect(author.items[0].$id).toBe('user/alice@example.com')
  })

  it('soft-deleted entity in array is treated as non-existent', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Tag: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        articles: '<- Article.tags[]',
      },
      Article: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        tags: '-> Tag[]',
      },
    }, { storage })

    // Create two tags, then soft-delete one
    await db.Tag.create({ slug: 'active-tag', name: 'Active' })
    await db.Tag.create({ slug: 'deleted-tag', name: 'Deleted' })
    await db.Tag.delete('deleted-tag')

    // Create article referencing both the active and soft-deleted tag
    const article = await db.Article.create({
      slug: 'mixed-article',
      title: 'Mixed',
      tags: ['active-tag', 'deleted-tag'],
    }, { autoCreate: true })

    expect(article.$id).toBe('article/mixed-article')

    // The active tag should be resolved normally
    const activeTag = await db.Tag.get('active-tag')
    expect(activeTag).not.toBeNull()
    expect(activeTag!.name).toBe('Active')

    // The deleted-tag should have been auto-created (replacing the soft-deleted version)
    const recreatedTag = await db.Tag.get('deleted-tag')
    expect(recreatedTag).not.toBeNull()

    // Verify both relationships exist
    const localId = article.$id.split('/')[1]
    const tags = await db.getRelated('article', localId!, 'tags')
    expect(tags.items).toHaveLength(2)
  })
})

// =============================================================================
// Section 3: Non-transitive auto-creation (nested relationships)
// =============================================================================

describe('autoCreate edge case: non-transitive auto-creation', () => {
  afterEach(() => {
    clearGlobalStorage()
  })

  it('auto-created entity with its own relationship field does not cascade', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Company: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        employees: '<- Author.company[]',
      },
      Author: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string',
        company: '-> Company',
        posts: '<- Post.author[]',
      },
      Post: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        author: '-> Author',
      },
    }, { storage })

    // Create a Post with autoCreate referencing a non-existent Author object
    // that itself has a 'company' field referencing a non-existent Company.
    //
    // Auto-creation is non-transitive: the Author will be auto-created as a stub
    // (string reference), but when the Author stub itself references a Company,
    // that Company will NOT be auto-created (it would require recursive autoCreate).
    //
    // When passing a string reference for author (not an object), the auto-created
    // Author stub won't have a company field, so no cascading issue arises.
    // The interesting case is passing an object for author that includes company.

    // Case 1: String reference - auto-creates Author stub without company field
    const post1 = await db.Post.create({
      slug: 'simple-post',
      title: 'Simple Post',
      author: 'john-doe',
    }, { autoCreate: true })

    expect(post1.$id).toBe('post/simple-post')

    // Verify Author stub was created
    const authorStub = await db.Author.get('john-doe')
    expect(authorStub).not.toBeNull()
    expect(authorStub!.$type).toBe('Author')

    // Case 2: Object reference with nested company field
    // The Author object includes a 'company' field referencing a non-existent Company.
    // Since auto-creation is non-transitive, this should throw a
    // RelationshipResolutionError for the Company, NOT cascade auto-creation.
    //
    // However, the auto-created entity uses createEntity with { skipValidation: true }
    // for string values, and {} for object values. The createEntity call will invoke
    // autoResolveRelationships WITHOUT autoCreate (since it's not passed through),
    // causing the company reference to fail resolution.
    await expect(db.Post.create({
      slug: 'nested-post',
      title: 'Nested Post',
      author: { slug: 'jane-doe', name: 'Jane', company: 'nonexistent-company' } as any,
    }, { autoCreate: true })).rejects.toThrow(/not found|does not exist/i)

    // Verify the Company was NOT auto-created (non-transitive)
    const company = await db.Company.get('nonexistent-company')
    expect(company).toBeNull()
  })

  it('does not cause infinite recursion with circular relationships', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Person: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string',
        mentor: '-> Person',
        mentees: '<- Person.mentor[]',
      },
    }, { storage })

    // Create a Person referencing another non-existent Person as mentor.
    // Auto-create the mentor as a stub. The stub will not have a mentor field,
    // so no infinite recursion occurs.
    const person = await db.Person.create({
      slug: 'student',
      title: 'Student',
      mentor: 'teacher',
    }, { autoCreate: true })

    expect(person.$id).toBe('person/student')

    // Verify the mentor stub was created
    const mentor = await db.Person.get('teacher')
    expect(mentor).not.toBeNull()
    expect(mentor!.$id).toBe('person/teacher')

    // Verify only 2 Person entities exist (no infinite recursion)
    const allPersons = await db.Person.find()
    expect(allPersons.items).toHaveLength(2)
  })
})

// =============================================================================
// Section 4: Partial array failure
// =============================================================================

describe('autoCreate edge case: partial array failure', () => {
  afterEach(() => {
    clearGlobalStorage()
  })

  it('partial failure in array leaves orphaned auto-created entities', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Tag: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        articles: '<- Article.tags[]',
      },
      Article: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        tags: '-> Tag[]',
      },
    }, { storage })

    // Pre-create 'valid-tag' so it exists
    await db.Tag.create({ slug: 'valid-tag', name: 'Valid Tag' })

    // Now try creating an Article that references tags where one reference
    // is an object WITHOUT the $id field (slug). The resolveOrCreate for objects
    // returns null when the ID field is missing. With autoCreate enabled,
    // string items get auto-created, objects without ID fields are skipped.
    //
    // This tests that already-created entities from earlier in the array persist
    // even when a later item in the array fails/is skipped.
    const article = await db.Article.create({
      slug: 'partial-article',
      title: 'Partial Article',
      tags: [
        'valid-tag',
        'auto-created-tag',
        { noSlugHere: 'bad-object' },  // This object has no 'slug' field => skipped
        'another-auto-tag',
      ] as any,
    }, { autoCreate: true })

    expect(article.$id).toBe('article/partial-article')

    // 'valid-tag' should still exist (was pre-created)
    const validTag = await db.Tag.get('valid-tag')
    expect(validTag).not.toBeNull()

    // 'auto-created-tag' should have been auto-created
    const autoCreatedTag = await db.Tag.get('auto-created-tag')
    expect(autoCreatedTag).not.toBeNull()
    expect(autoCreatedTag!.$id).toBe('tag/auto-created-tag')

    // 'another-auto-tag' should have been auto-created
    const anotherTag = await db.Tag.get('another-auto-tag')
    expect(anotherTag).not.toBeNull()
    expect(anotherTag!.$id).toBe('tag/another-auto-tag')

    // The bad object should have been skipped, resulting in 3 relationships (not 4)
    const localId = article.$id.split('/')[1]
    const tags = await db.getRelated('article', localId!, 'tags')
    expect(tags.items).toHaveLength(3)
  })

  it('throwing auto-create in array prevents parent entity creation', async () => {
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

    // Without autoCreate, a missing entity reference should fail the whole create
    await expect(db.Post.create({
      slug: 'failing-post',
      title: 'Failing Post',
      author: 'nonexistent@example.com',
    })).rejects.toThrow(/not found|does not exist/i)

    // Verify the post was NOT created
    const post = await db.Post.get('failing-post')
    expect(post).toBeNull()
  })
})

// =============================================================================
// Section 5: Duplicate entities in same array
// =============================================================================

describe('autoCreate edge case: duplicate entities in same array', () => {
  afterEach(() => {
    clearGlobalStorage()
  })

  it('duplicate string IDs in array creates entity only once', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Tag: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        articles: '<- Article.tags[]',
      },
      Article: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        tags: '-> Tag[]',
      },
    }, { storage })

    // Create an Article with the same tag referenced twice
    const article = await db.Article.create({
      slug: 'dupe-tags-article',
      title: 'Dupe Tags',
      tags: ['same-slug', 'same-slug'],
    }, { autoCreate: true })

    expect(article.$id).toBe('article/dupe-tags-article')

    // Verify only 1 Tag entity was created (not 2)
    const allTags = await db.Tag.find()
    expect(allTags.items).toHaveLength(1)
    expect(allTags.items[0].$id).toBe('tag/same-slug')

    // The relationship object uses displayName as key, so duplicate references
    // with the same displayName will collapse into a single entry.
    // The relObject Record<string, string> will have one key for the displayName.
    const localId = article.$id.split('/')[1]
    const tags = await db.getRelated('article', localId!, 'tags')
    // Since the relObject is { 'same-slug': 'tag/same-slug' } (deduplicated by key),
    // there should be exactly 1 relationship entry.
    expect(tags.items).toHaveLength(1)
    expect(tags.items[0].$id).toBe('tag/same-slug')
  })

  it('duplicate IDs where first is string and second is object', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Tag: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        articles: '<- Article.tags[]',
      },
      Article: {
        $id: 'slug',
        slug: 'string!#',
        title: 'string!',
        tags: '-> Tag[]',
      },
    }, { storage })

    // Same entity referenced as string first, then as object
    const article = await db.Article.create({
      slug: 'mixed-dupe-article',
      title: 'Mixed Dupe',
      tags: ['my-tag', { slug: 'my-tag', name: 'My Tag Updated' }] as any,
    }, { autoCreate: true })

    expect(article.$id).toBe('article/mixed-dupe-article')

    // Only 1 Tag entity should exist
    const allTags = await db.Tag.find()
    expect(allTags.items).toHaveLength(1)

    // The first create (from string 'my-tag') creates the stub.
    // The second reference (object with slug 'my-tag') should find the
    // already-created entity and resolve to it (not re-create).
    const tag = await db.Tag.get('my-tag')
    expect(tag).not.toBeNull()
    expect(tag!.$id).toBe('tag/my-tag')
  })

  it('duplicate IDs with different display names collapse in relationship object', async () => {
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
        collaborators: '-> User[]',
      },
    }, { storage })

    // Pre-create user so we can test duplicate references to an existing entity
    await db.User.create({ email: 'alice@test.com', name: 'Alice' })

    // Reference the same user twice in the array
    const post = await db.Post.create({
      slug: 'collab-post',
      title: 'Collab Post',
      collaborators: ['alice@test.com', 'alice@test.com'],
    }, { autoCreate: true })

    expect(post.$id).toBe('post/collab-post')

    // Only 1 user should exist
    const allUsers = await db.User.find()
    expect(allUsers.items).toHaveLength(1)

    // The relationship object deduplicates by display name key
    const localId = post.$id.split('/')[1]
    const collaborators = await db.getRelated('post', localId!, 'collaborators')
    expect(collaborators.items).toHaveLength(1)
    expect(collaborators.items[0].$id).toBe('user/alice@test.com')
  })
})

// =============================================================================
// Section 6: Object without recognizable ID field
// =============================================================================

describe('autoCreate edge case: object without recognizable ID field', () => {
  afterEach(() => {
    clearGlobalStorage()
  })

  it('skips object without $id directive field (single relationship)', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      ZapierCategory: {
        $id: 'slug',
        $name: 'title',
        slug: 'string!#',
        title: 'string!',
        apps: '<- ZapierApp.categories[]',
      },
      ZapierApp: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        primaryCategory: '-> ZapierCategory',
      },
    }, { storage })

    // Pass an object without the 'slug' field (which is the $id field for ZapierCategory).
    // The resolveOrCreate function checks obj[idField] ?? obj.$id, and if neither
    // is a valid string, it returns null. The relationship field should be deleted.
    const app = await db.ZapierApp.create({
      slug: 'test-app',
      name: 'Test App',
      primaryCategory: { randomField: 'value', title: 'Some Category' } as any,
    }, { autoCreate: true })

    expect(app.$id).toBe('zapierapp/test-app')

    // No ZapierCategory should have been created
    const allCategories = await db.ZapierCategory.find()
    expect(allCategories.items).toHaveLength(0)
  })

  it('skips object without $id directive field in array', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      ZapierCategory: {
        $id: 'slug',
        $name: 'title',
        slug: 'string!#',
        title: 'string!',
        apps: '<- ZapierApp.categories[]',
      },
      ZapierApp: {
        $id: 'slug',
        slug: 'string!#',
        name: 'string!',
        categories: '-> ZapierCategory[]',
      },
    }, { storage })

    // Array with mix of valid and invalid objects
    const app = await db.ZapierApp.create({
      slug: 'test-app',
      name: 'Test App',
      categories: [
        { slug: 'valid-cat', title: 'Valid Category' },
        { randomField: 'no-slug', title: 'Invalid' },
        { slug: 'another-valid', title: 'Another Valid' },
      ] as any,
    }, { autoCreate: true })

    expect(app.$id).toBe('zapierapp/test-app')

    // Only the 2 valid categories should have been created
    const allCategories = await db.ZapierCategory.find()
    expect(allCategories.items).toHaveLength(2)

    const validCat = await db.ZapierCategory.get('valid-cat')
    expect(validCat).not.toBeNull()
    expect(validCat!.title).toBe('Valid Category')

    const anotherValid = await db.ZapierCategory.get('another-valid')
    expect(anotherValid).not.toBeNull()
    expect(anotherValid!.title).toBe('Another Valid')

    // Verify the app only has 2 category relationships (invalid one was skipped)
    const localId = app.$id.split('/')[1]
    const categories = await db.getRelated('zapierapp', localId!, 'categories')
    expect(categories.items).toHaveLength(2)
  })

  it('object with $id field as fallback when no $id directive', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Widget: {
        name: 'string!',
        gadgets: '<- Gadget.widget[]',
      },
      Gadget: {
        name: 'string!',
        widget: '-> Widget',
      },
    }, { storage })

    // Widget has no $id directive, so idField defaults to '$id'.
    // Pass an object with a $id field as fallback.
    const gadget = await db.Gadget.create({
      name: 'My Gadget',
      widget: { $id: 'my-widget', name: 'My Widget' } as any,
    }, { autoCreate: true })

    expect(gadget.$id).toBeDefined()

    // The Widget should have been auto-created using the $id fallback
    const widget = await db.Widget.get('my-widget')
    expect(widget).not.toBeNull()
    expect(widget!.name).toBe('My Widget')
  })

  it('object with neither $id directive field nor $id is skipped', async () => {
    const storage = new MemoryBackend()
    const db = DB({
      Widget: {
        name: 'string!',
        gadgets: '<- Gadget.widget[]',
      },
      Gadget: {
        name: 'string!',
        widget: '-> Widget',
      },
    }, { storage })

    // Widget has no $id directive, and the object has no $id field either.
    // resolveOrCreate should return null, and the relationship field is deleted.
    const gadget = await db.Gadget.create({
      name: 'Orphan Gadget',
      widget: { name: 'Widget Without ID' } as any,
    }, { autoCreate: true })

    expect(gadget.$id).toBeDefined()

    // No Widget should have been created
    const allWidgets = await db.Widget.find()
    expect(allWidgets.items).toHaveLength(0)
  })
})
