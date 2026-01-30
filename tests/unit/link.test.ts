/**
 * Link/Unlink Operation Tests
 *
 * Tests for relationship operations including $link, $unlink,
 * and bidirectional consistency using real FsBackend storage.
 *
 * NO MOCKS - Uses real FsBackend with temp directories.
 * Relationships are persisted to rels/ directory.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { FsBackend } from '../../src/storage/FsBackend'
import { ParqueDB } from '../../src/ParqueDB'
import type {
  EntityId,
  Schema,
} from '../../src/types'

// =============================================================================
// Test Schema with Relationships
// =============================================================================

function createBlogSchema(): Schema {
  return {
    User: {
      $type: 'schema:Person',
      $ns: 'users',
      name: 'string!',
      email: { type: 'email!', index: 'unique' },
      // Reverse relationships
      posts: '<- Post.author[]',
      comments: '<- Comment.author[]',
      followers: '<- User.following[]',
      following: '-> User.followers[]',
    },
    Post: {
      $type: 'schema:BlogPosting',
      $ns: 'posts',
      name: 'string!',
      title: 'string!',
      content: 'markdown!',
      status: { type: 'string', default: 'draft' },
      // Forward relationships
      author: '-> User.posts',
      categories: '-> Category.posts[]',
      tags: '-> Tag.posts[]',
      // Reverse relationships
      comments: '<- Comment.post[]',
    },
    Category: {
      $type: 'schema:Category',
      $ns: 'categories',
      name: 'string!',
      slug: { type: 'string!', index: 'unique' },
      // Reverse relationship
      posts: '<- Post.categories[]',
    },
    Tag: {
      $type: 'schema:Tag',
      $ns: 'tags',
      name: 'string!',
      // Reverse relationship
      posts: '<- Post.tags[]',
    },
    Comment: {
      $type: 'schema:Comment',
      $ns: 'comments',
      text: 'string!',
      // Forward relationships
      post: '-> Post.comments',
      author: '-> User.comments',
    },
  }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('Link Operations', () => {
  let tempDir: string
  let storage: FsBackend
  let db: ParqueDB
  let schema: Schema

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-link-test-'))
    storage = new FsBackend(tempDir)
    schema = createBlogSchema()
    db = new ParqueDB({ storage, schema })
  })

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true })
  })

  // ===========================================================================
  // $link operator
  // ===========================================================================

  describe('$link operator', () => {
    it('creates forward relationship', async () => {
      // Create user and post
      const user = await db.create('users', {
        $type: 'User',
        name: 'John Doe',
        email: 'john@example.com',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'My Post',
        title: 'Hello World',
        content: 'This is my first post',
      })

      // Link post to user as author
      const updated = await db.update('posts', post.$id as string, {
        $link: { author: user.$id as EntityId },
      })

      expect(updated).not.toBeNull()
      expect(updated!.author).toBeDefined()

      // The author field should contain the link
      const authorLink = updated!.author as Record<string, EntityId>
      expect(Object.values(authorLink)).toContain(user.$id)
    })

    it('links single entity', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Jane',
        email: 'jane@example.com',
      })

      const category = await db.create('categories', {
        $type: 'Category',
        name: 'Technology',
        slug: 'tech',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Tech Post',
        title: 'About Tech',
        content: 'Content about tech',
      })

      await db.update('posts', post.$id, {
        $link: { author: user.$id },
      })

      // Link single category
      const updated = await db.update('posts', post.$id as string, {
        $link: { categories: category.$id as EntityId },
      })

      expect(updated).not.toBeNull()
      const categories = updated!.categories as Record<string, EntityId>
      expect(Object.keys(categories).length).toBeGreaterThanOrEqual(1)
      expect(Object.values(categories)).toContain(category.$id)
    })

    it('links multiple entities', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const tech = await db.create('categories', {
        $type: 'Category',
        name: 'Technology',
        slug: 'tech',
      })

      const dbCat = await db.create('categories', {
        $type: 'Category',
        name: 'Databases',
        slug: 'databases',
      })

      const tutorial = await db.create('categories', {
        $type: 'Category',
        name: 'Tutorials',
        slug: 'tutorials',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Multi-Category Post',
        title: 'About Multiple Topics',
        content: 'Content',
      })

      await db.update('posts', post.$id, {
        $link: { author: user.$id },
      })

      // Link multiple categories
      const updated = await db.update('posts', post.$id as string, {
        $link: {
          categories: [
            tech.$id as EntityId,
            dbCat.$id as EntityId,
            tutorial.$id as EntityId,
          ],
        },
      })

      expect(updated).not.toBeNull()
      const categories = updated!.categories as Record<string, EntityId>
      const categoryValues = Object.values(categories).filter(v => typeof v === 'string' && v.startsWith('categories/'))
      expect(categoryValues.length).toBe(3)
      expect(categoryValues).toContain(tech.$id)
      expect(categoryValues).toContain(dbCat.$id)
      expect(categoryValues).toContain(tutorial.$id)
    })

    it('links using display name', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'John Doe',
        email: 'john@example.com',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
      })

      // Link with display name
      const updated = await db.update('posts', post.$id as string, {
        $link: { author: user.$id as EntityId },
      })

      expect(updated).not.toBeNull()
      const author = updated!.author as Record<string, EntityId>
      // The key should be the entity's name
      expect(author['John Doe']).toBe(user.$id)
    })

    it('adds to existing links', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const cat1 = await db.create('categories', {
        $type: 'Category',
        name: 'Tech',
        slug: 'tech',
      })

      const cat2 = await db.create('categories', {
        $type: 'Category',
        name: 'News',
        slug: 'news',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
      })

      await db.update('posts', post.$id, {
        $link: {
          author: user.$id,
          categories: cat1.$id,
        },
      })

      // Add another category
      const updated = await db.update('posts', post.$id as string, {
        $link: { categories: cat2.$id as EntityId },
      })

      expect(updated).not.toBeNull()
      const categories = updated!.categories as Record<string, EntityId>
      const categoryValues = Object.values(categories).filter(v => typeof v === 'string' && v.startsWith('categories/'))
      expect(categoryValues.length).toBe(2)
      expect(categoryValues).toContain(cat1.$id)
      expect(categoryValues).toContain(cat2.$id)
    })
  })

  // ===========================================================================
  // $unlink operator
  // ===========================================================================

  describe('$unlink operator', () => {
    it('removes forward relationship', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
      })

      await db.update('posts', post.$id, {
        $link: { author: user.$id },
      })

      // Remove author link
      const updated = await db.update('posts', post.$id as string, {
        $unlink: { author: user.$id as EntityId },
      })

      expect(updated).not.toBeNull()
      // Author should be empty or undefined
      const author = updated!.author as Record<string, EntityId> | undefined
      if (author) {
        expect(Object.keys(author)).toHaveLength(0)
      }
    })

    it('unlinks single entity', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const cat1 = await db.create('categories', {
        $type: 'Category',
        name: 'Tech',
        slug: 'tech',
      })

      const cat2 = await db.create('categories', {
        $type: 'Category',
        name: 'News',
        slug: 'news',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
      })

      await db.update('posts', post.$id, {
        $link: {
          author: user.$id,
          categories: [cat1.$id, cat2.$id],
        },
      })

      // Unlink single category
      const updated = await db.update('posts', post.$id as string, {
        $unlink: { categories: cat1.$id as EntityId },
      })

      expect(updated).not.toBeNull()
      const categories = updated!.categories as Record<string, EntityId>
      const categoryValues = Object.values(categories).filter(v => typeof v === 'string' && v.startsWith('categories/'))
      expect(categoryValues).not.toContain(cat1.$id)
      expect(categoryValues).toContain(cat2.$id)
    })

    it('unlinks multiple entities', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const cat1 = await db.create('categories', {
        $type: 'Category',
        name: 'Tech',
        slug: 'tech',
      })

      const cat2 = await db.create('categories', {
        $type: 'Category',
        name: 'News',
        slug: 'news',
      })

      const cat3 = await db.create('categories', {
        $type: 'Category',
        name: 'Sports',
        slug: 'sports',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
      })

      await db.update('posts', post.$id, {
        $link: {
          author: user.$id,
          categories: [cat1.$id, cat2.$id, cat3.$id],
        },
      })

      // Unlink multiple categories
      const updated = await db.update('posts', post.$id as string, {
        $unlink: {
          categories: [cat1.$id as EntityId, cat2.$id as EntityId],
        },
      })

      expect(updated).not.toBeNull()
      const categories = updated!.categories as Record<string, EntityId>
      const categoryValues = Object.values(categories).filter(v => typeof v === 'string' && v.startsWith('categories/'))
      expect(categoryValues.length).toBe(1)
      expect(categoryValues).toContain(cat3.$id)
    })

    it('handles non-existent link gracefully', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
      })

      await db.update('posts', post.$id, {
        $link: { author: user.$id },
      })

      // Try to unlink category that was never linked
      const updated = await db.update('posts', post.$id as string, {
        $unlink: { categories: 'categories/nonexistent' as EntityId },
      })

      // Should not throw, just no-op
      expect(updated).not.toBeNull()
    })
  })

  // ===========================================================================
  // Bidirectional consistency
  // ===========================================================================

  describe('bidirectional consistency', () => {
    it('updates reverse relationship automatically', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
      })

      // Link post to user
      await db.update('posts', post.$id as string, {
        $link: { author: user.$id as EntityId },
      })

      // Check reverse relationship on user via getRelated
      // getRelated takes (namespace, localId, relationField)
      const userLocalId = (user.$id as string).split('/')[1]
      const userPosts = await db.getRelated('users', userLocalId, 'posts')

      // The post should appear in the user's posts
      const postIds = userPosts.items.map(p => p.$id)
      expect(postIds).toContain(post.$id)
    })

    it('maintains referential integrity', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const category = await db.create('categories', {
        $type: 'Category',
        name: 'Tech',
        slug: 'tech',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
      })

      await db.update('posts', post.$id, {
        $link: {
          author: user.$id,
          categories: category.$id,
        },
      })

      // Check that category has the post in its reverse relationship
      const categoryLocalId = (category.$id as string).split('/')[1]
      const categoryPosts = await db.getRelated('categories', categoryLocalId, 'posts')

      const postIds = categoryPosts.items.map(p => p.$id)
      expect(postIds).toContain(post.$id)
    })

    it('handles self-referential relationships', async () => {
      const user1 = await db.create('users', {
        $type: 'User',
        name: 'User 1',
        email: 'user1@example.com',
      })

      const user2 = await db.create('users', {
        $type: 'User',
        name: 'User 2',
        email: 'user2@example.com',
      })

      // User1 follows User2
      await db.update('users', user1.$id as string, {
        $link: { following: user2.$id as EntityId },
      })

      // Check that User1 has User2 in following
      const user1LocalId = (user1.$id as string).split('/')[1]
      const following = await db.getRelated('users', user1LocalId, 'following')
      const followingIds = following.items.map(u => u.$id)
      expect(followingIds).toContain(user2.$id)
    })
  })

  // ===========================================================================
  // Link and Unlink in same update
  // ===========================================================================

  describe('combined link and unlink', () => {
    it('links and unlinks in single update', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const oldCategory = await db.create('categories', {
        $type: 'Category',
        name: 'Old Category',
        slug: 'old',
      })

      const newCategory = await db.create('categories', {
        $type: 'Category',
        name: 'New Category',
        slug: 'new',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
      })

      await db.update('posts', post.$id, {
        $link: {
          author: user.$id,
          categories: oldCategory.$id,
        },
      })

      // Swap categories in single update
      const updated = await db.update('posts', post.$id as string, {
        $unlink: { categories: oldCategory.$id as EntityId },
        $link: { categories: newCategory.$id as EntityId },
      })

      expect(updated).not.toBeNull()
      const categories = updated!.categories as Record<string, EntityId>
      const categoryValues = Object.values(categories).filter(v => typeof v === 'string' && v.startsWith('categories/'))
      expect(categoryValues).not.toContain(oldCategory.$id)
      expect(categoryValues).toContain(newCategory.$id)
    })
  })

  // ===========================================================================
  // Edge cases
  // ===========================================================================

  describe('edge cases', () => {
    it('preserves link metadata on update', async () => {
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Post',
        title: 'Title',
        content: 'Content',
      })

      await db.update('posts', post.$id, {
        $link: { author: user.$id },
      })

      // Update other fields
      const updated = await db.update('posts', post.$id as string, {
        $set: { title: 'Updated Title' },
      })

      // Relationships should be preserved
      expect(updated!.author).toBeDefined()
      const author = updated!.author as Record<string, EntityId>
      expect(Object.values(author)).toContain(user.$id)
    })
  })
})
