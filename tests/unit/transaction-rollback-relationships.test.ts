/**
 * Transaction Rollback Relationship Cleanup Tests
 *
 * Tests for issue parquedb-tdxr: Transaction rollback missing relationship cleanup.
 *
 * When a transaction is rolled back, the relationship indexes (reverseRelIndex)
 * must be properly restored to maintain consistency:
 * - CREATE rollback: unindex the relationships from the created entity
 * - UPDATE rollback: unindex current relationships and reindex the restored state
 * - DELETE rollback: reindex the restored entity's relationships
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp } from 'node:fs/promises'
import { cleanupTempDir } from '../setup'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { FsBackend } from '../../src/storage/FsBackend'
import { ParqueDB } from '../../src/ParqueDB'
import type { EntityId, Schema } from '../../src/types'

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
    },
    Post: {
      $type: 'schema:BlogPosting',
      $ns: 'posts',
      name: 'string!',
      title: 'string!',
      content: 'markdown!',
      // Forward relationships
      author: '-> User.posts',
      categories: '-> Category.posts[]',
    },
    Category: {
      $type: 'schema:Category',
      $ns: 'categories',
      name: 'string!',
      slug: { type: 'string!', index: 'unique' },
      // Reverse relationship
      posts: '<- Post.categories[]',
    },
  }
}

// =============================================================================
// Test Suite
// =============================================================================

describe('Transaction Rollback Relationship Cleanup', () => {
  let tempDir: string
  let storage: FsBackend
  let db: ParqueDB
  let schema: Schema

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'parquedb-txn-rel-test-'))
    storage = new FsBackend(tempDir)
    schema = createBlogSchema()
    db = new ParqueDB({ storage, schema })
  })

  afterEach(async () => {
    await cleanupTempDir(tempDir)
  })

  // ===========================================================================
  // CREATE Rollback Tests
  // ===========================================================================

  describe('CREATE rollback relationship cleanup', () => {
    it('should remove relationship indexes when rolling back created entity', async () => {
      // Create a user first (this will be committed)
      const user = await db.create('users', {
        $type: 'User',
        name: 'John Doe',
        email: 'john@example.com',
      })

      // Verify the user exists and has no posts initially
      const userLocalId = (user.$id as string).split('/')[1]
      let userPosts = await db.getRelated('users', userLocalId, 'posts')
      expect(userPosts.items).toHaveLength(0)

      // Begin a transaction and create a post linked to the user
      const tx = db.beginTransaction()

      const post = await tx.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Hello World',
        content: 'Content',
      })

      // Link the post to the user within the transaction
      await tx.update('posts', post.$id as string, {
        $link: { author: user.$id as EntityId },
      })

      // Before rollback: the user should have the post in their posts relationship
      userPosts = await db.getRelated('users', userLocalId, 'posts')
      expect(userPosts.items.length).toBeGreaterThanOrEqual(1)

      // Now rollback the transaction
      await tx.rollback()

      // After rollback: the post should not exist
      const postResult = await db.get('posts', post.$id as string)
      expect(postResult).toBeNull()

      // After rollback: the user's posts relationship should be empty
      userPosts = await db.getRelated('users', userLocalId, 'posts')
      expect(userPosts.items).toHaveLength(0)
    })

    it('should remove multiple relationship indexes when rolling back', async () => {
      // Create user and categories first
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

      // Begin transaction and create post with multiple relationships
      const tx = db.beginTransaction()

      const post = await tx.create('posts', {
        $type: 'Post',
        name: 'Multi-Category Post',
        title: 'Tech News',
        content: 'Content',
      })

      await tx.update('posts', post.$id as string, {
        $link: {
          author: user.$id as EntityId,
          categories: [cat1.$id as EntityId, cat2.$id as EntityId],
        },
      })

      // Verify relationships exist before rollback
      const userLocalId = (user.$id as string).split('/')[1]
      const cat1LocalId = (cat1.$id as string).split('/')[1]
      const cat2LocalId = (cat2.$id as string).split('/')[1]

      let userPosts = await db.getRelated('users', userLocalId, 'posts')
      let cat1Posts = await db.getRelated('categories', cat1LocalId, 'posts')
      let cat2Posts = await db.getRelated('categories', cat2LocalId, 'posts')

      expect(userPosts.items.length).toBeGreaterThanOrEqual(1)
      expect(cat1Posts.items.length).toBeGreaterThanOrEqual(1)
      expect(cat2Posts.items.length).toBeGreaterThanOrEqual(1)

      // Rollback
      await tx.rollback()

      // After rollback: all reverse relationships should be empty
      userPosts = await db.getRelated('users', userLocalId, 'posts')
      cat1Posts = await db.getRelated('categories', cat1LocalId, 'posts')
      cat2Posts = await db.getRelated('categories', cat2LocalId, 'posts')

      expect(userPosts.items).toHaveLength(0)
      expect(cat1Posts.items).toHaveLength(0)
      expect(cat2Posts.items).toHaveLength(0)
    })
  })

  // ===========================================================================
  // UPDATE Rollback Tests
  // ===========================================================================

  describe('UPDATE rollback relationship cleanup', () => {
    it('should restore relationship indexes when rolling back update with $link', async () => {
      // Create user and post with existing relationship
      const user1 = await db.create('users', {
        $type: 'User',
        name: 'User One',
        email: 'user1@example.com',
      })

      const user2 = await db.create('users', {
        $type: 'User',
        name: 'User Two',
        email: 'user2@example.com',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original Post',
        content: 'Content',
      })

      // Link post to user1 (committed)
      await db.update('posts', post.$id as string, {
        $link: { author: user1.$id as EntityId },
      })

      // Verify user1 has the post
      const user1LocalId = (user1.$id as string).split('/')[1]
      const user2LocalId = (user2.$id as string).split('/')[1]

      let user1Posts = await db.getRelated('users', user1LocalId, 'posts')
      let user2Posts = await db.getRelated('users', user2LocalId, 'posts')
      expect(user1Posts.items.map(p => p.$id)).toContain(post.$id)
      expect(user2Posts.items).toHaveLength(0)

      // Begin transaction and change the author to user2
      const tx = db.beginTransaction()

      await tx.update('posts', post.$id as string, {
        $unlink: { author: user1.$id as EntityId },
        $link: { author: user2.$id as EntityId },
      })

      // Before rollback: user2 should have the post, user1 should not
      user1Posts = await db.getRelated('users', user1LocalId, 'posts')
      user2Posts = await db.getRelated('users', user2LocalId, 'posts')
      expect(user1Posts.items.map(p => p.$id)).not.toContain(post.$id)
      expect(user2Posts.items.map(p => p.$id)).toContain(post.$id)

      // Rollback
      await tx.rollback()

      // After rollback: user1 should have the post again, user2 should not
      user1Posts = await db.getRelated('users', user1LocalId, 'posts')
      user2Posts = await db.getRelated('users', user2LocalId, 'posts')
      expect(user1Posts.items.map(p => p.$id)).toContain(post.$id)
      expect(user2Posts.items).toHaveLength(0)
    })

    it('should restore relationship indexes when rolling back update with $unlink', async () => {
      // Create user and post with relationship
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Original Post',
        content: 'Content',
      })

      // Link post to user (committed)
      await db.update('posts', post.$id as string, {
        $link: { author: user.$id as EntityId },
      })

      const userLocalId = (user.$id as string).split('/')[1]

      // Verify relationship exists
      let userPosts = await db.getRelated('users', userLocalId, 'posts')
      expect(userPosts.items.map(p => p.$id)).toContain(post.$id)

      // Begin transaction and unlink the author
      const tx = db.beginTransaction()

      await tx.update('posts', post.$id as string, {
        $unlink: { author: user.$id as EntityId },
      })

      // Before rollback: user should not have the post
      userPosts = await db.getRelated('users', userLocalId, 'posts')
      expect(userPosts.items.map(p => p.$id)).not.toContain(post.$id)

      // Rollback
      await tx.rollback()

      // After rollback: user should have the post again
      userPosts = await db.getRelated('users', userLocalId, 'posts')
      expect(userPosts.items.map(p => p.$id)).toContain(post.$id)
    })
  })

  // ===========================================================================
  // DELETE Rollback Tests
  // ===========================================================================

  describe('DELETE rollback relationship cleanup', () => {
    it('should restore relationship indexes when rolling back delete', async () => {
      // Create user and post with relationship
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Test Post',
        title: 'Will be deleted',
        content: 'Content',
      })

      // Link post to user (committed)
      await db.update('posts', post.$id as string, {
        $link: { author: user.$id as EntityId },
      })

      const userLocalId = (user.$id as string).split('/')[1]

      // Verify relationship exists
      let userPosts = await db.getRelated('users', userLocalId, 'posts')
      expect(userPosts.items.map(p => p.$id)).toContain(post.$id)

      // Begin transaction and delete the post
      const tx = db.beginTransaction()

      await tx.delete('posts', post.$id as string)

      // Before rollback: post should be deleted and relationship gone
      const deletedPost = await db.get('posts', post.$id as string)
      expect(deletedPost).toBeNull()

      userPosts = await db.getRelated('users', userLocalId, 'posts')
      expect(userPosts.items.map(p => p.$id)).not.toContain(post.$id)

      // Rollback
      await tx.rollback()

      // After rollback: post should exist and relationship should be restored
      const restoredPost = await db.get('posts', post.$id as string)
      expect(restoredPost).not.toBeNull()
      expect(restoredPost!.title).toBe('Will be deleted')

      userPosts = await db.getRelated('users', userLocalId, 'posts')
      expect(userPosts.items.map(p => p.$id)).toContain(post.$id)
    })

    it('should restore multiple relationship indexes when rolling back delete', async () => {
      // Create user and categories
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

      // Create post with multiple relationships
      const post = await db.create('posts', {
        $type: 'Post',
        name: 'Multi-Category Post',
        title: 'Tech News',
        content: 'Content',
      })

      await db.update('posts', post.$id as string, {
        $link: {
          author: user.$id as EntityId,
          categories: [cat1.$id as EntityId, cat2.$id as EntityId],
        },
      })

      const userLocalId = (user.$id as string).split('/')[1]
      const cat1LocalId = (cat1.$id as string).split('/')[1]
      const cat2LocalId = (cat2.$id as string).split('/')[1]

      // Verify relationships exist
      let userPosts = await db.getRelated('users', userLocalId, 'posts')
      let cat1Posts = await db.getRelated('categories', cat1LocalId, 'posts')
      let cat2Posts = await db.getRelated('categories', cat2LocalId, 'posts')

      expect(userPosts.items.map(p => p.$id)).toContain(post.$id)
      expect(cat1Posts.items.map(p => p.$id)).toContain(post.$id)
      expect(cat2Posts.items.map(p => p.$id)).toContain(post.$id)

      // Begin transaction and delete the post
      const tx = db.beginTransaction()
      await tx.delete('posts', post.$id as string)

      // Rollback
      await tx.rollback()

      // After rollback: all relationships should be restored
      userPosts = await db.getRelated('users', userLocalId, 'posts')
      cat1Posts = await db.getRelated('categories', cat1LocalId, 'posts')
      cat2Posts = await db.getRelated('categories', cat2LocalId, 'posts')

      expect(userPosts.items.map(p => p.$id)).toContain(post.$id)
      expect(cat1Posts.items.map(p => p.$id)).toContain(post.$id)
      expect(cat2Posts.items.map(p => p.$id)).toContain(post.$id)
    })
  })

  // ===========================================================================
  // Complex Rollback Scenarios
  // ===========================================================================

  describe('complex rollback scenarios', () => {
    it('should handle mixed operations correctly', async () => {
      // Create initial entities
      const user = await db.create('users', {
        $type: 'User',
        name: 'Author',
        email: 'author@example.com',
      })

      const existingPost = await db.create('posts', {
        $type: 'Post',
        name: 'Existing Post',
        title: 'Will be updated',
        content: 'Content',
      })

      await db.update('posts', existingPost.$id as string, {
        $link: { author: user.$id as EntityId },
      })

      const postToDelete = await db.create('posts', {
        $type: 'Post',
        name: 'To Delete',
        title: 'Will be deleted',
        content: 'Content',
      })

      await db.update('posts', postToDelete.$id as string, {
        $link: { author: user.$id as EntityId },
      })

      const userLocalId = (user.$id as string).split('/')[1]

      // Verify initial state: user has 2 posts
      let userPosts = await db.getRelated('users', userLocalId, 'posts')
      expect(userPosts.items).toHaveLength(2)

      // Begin transaction with mixed operations
      const tx = db.beginTransaction()

      // Create new post
      const newPost = await tx.create('posts', {
        $type: 'Post',
        name: 'New Post',
        title: 'Created in tx',
        content: 'Content',
      })

      await tx.update('posts', newPost.$id as string, {
        $link: { author: user.$id as EntityId },
      })

      // Update existing post (unlink from user)
      await tx.update('posts', existingPost.$id as string, {
        $unlink: { author: user.$id as EntityId },
      })

      // Delete post
      await tx.delete('posts', postToDelete.$id as string)

      // Before rollback: user should have only newPost
      userPosts = await db.getRelated('users', userLocalId, 'posts')
      const postIds = userPosts.items.map(p => p.$id)
      expect(postIds).toContain(newPost.$id)
      expect(postIds).not.toContain(existingPost.$id)
      expect(postIds).not.toContain(postToDelete.$id)
      expect(userPosts.items).toHaveLength(1)

      // Rollback
      await tx.rollback()

      // After rollback: user should have original 2 posts (existingPost and postToDelete)
      userPosts = await db.getRelated('users', userLocalId, 'posts')
      const restoredPostIds = userPosts.items.map(p => p.$id)
      expect(restoredPostIds).toContain(existingPost.$id)
      expect(restoredPostIds).toContain(postToDelete.$id)
      expect(restoredPostIds).not.toContain(newPost.$id)
      expect(userPosts.items).toHaveLength(2)
    })
  })
})
