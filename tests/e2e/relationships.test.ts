/**
 * E2E Relationship Tests via RPC
 *
 * Tests the full relationship flow with ParqueDB's relationship model.
 * ParqueDB stores relationships as RelSet objects: { 'Display Name': 'ns/id' }
 *
 * Note: ParqueDB uses a graph-based relationship model with $link/$unlink operators.
 * Full hydration of related entities requires schema registration.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  getTestClient,
  cleanupTestData,
  waitForConsistency,
  type ParqueDBClient,
  type Post,
  type User,
  type Comment,
} from './setup'

describe('Relationships via RPC', () => {
  let client: ParqueDBClient

  beforeEach(async () => {
    await cleanupTestData()
    client = getTestClient()
  })

  afterEach(async () => {
    await cleanupTestData()
  })

  // ===========================================================================
  // Basic Relationship Operations
  // ===========================================================================

  describe('Basic Relationships', () => {
    it('links entities through DO with $link', async () => {
      // Create a user
      const user = await client.Users.create({
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
      })

      // Create a post
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Alice\'s Post',
        title: 'My First Post',
        content: 'Hello World',
      })

      // Link the post to the user as author
      const linkedPost = await client.Posts.update(post.$id, {
        $link: { author: user.$id },
      })

      expect(linkedPost).not.toBeNull()
      // The author relationship should be stored
      expect(linkedPost!.version).toBe(2) // Version incremented after link
    })

    it('stores linked entity reference in RelSet format', async () => {
      const user = await client.Users.create({
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
        role: 'admin',
      })

      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Post by Alice',
        title: 'Linked Post',
      })

      await client.Posts.update(post.$id, {
        $link: { author: user.$id },
      })

      await waitForConsistency()

      // Fetch post - author should be stored as RelSet
      const fetched = await client.Posts.get(post.$id)

      // ParqueDB stores relationships as { 'Display Name': 'ns/id' }
      expect((fetched as any).author).toBeDefined()
      // The author field should contain a reference to the user
      const authorRef = (fetched as any).author
      expect(typeof authorRef).toBe('object')
      // The value should be the user's entity ID
      expect(Object.values(authorRef)[0]).toBe(user.$id)
    })

    it('unlinks entities with $unlink', async () => {
      const user = await client.Users.create({
        $type: 'User',
        name: 'Bob',
      })

      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Bob\'s Post',
        title: 'Will Unlink',
      })

      // Link
      await client.Posts.update(post.$id, {
        $link: { author: user.$id },
      })

      // Unlink
      const unlinked = await client.Posts.update(post.$id, {
        $unlink: { author: user.$id },
      })

      expect(unlinked).not.toBeNull()

      await waitForConsistency()

      // Fetch - author relationship should be empty
      const fetched = await client.Posts.get(post.$id)

      // After unlink, the author field should be an empty object
      expect((fetched as any).author).toBeDefined()
      expect(Object.keys((fetched as any).author)).toHaveLength(0)
    })
  })

  // ===========================================================================
  // One-to-Many Relationships
  // ===========================================================================

  describe('One-to-Many Relationships', () => {
    it('links multiple entities to one parent', async () => {
      const user = await client.Users.create({
        $type: 'User',
        name: 'Author',
      })

      const post1 = await client.Posts.create({
        $type: 'Post',
        name: 'Post 1',
        title: 'First Post',
      })

      const post2 = await client.Posts.create({
        $type: 'Post',
        name: 'Post 2',
        title: 'Second Post',
      })

      const post3 = await client.Posts.create({
        $type: 'Post',
        name: 'Post 3',
        title: 'Third Post',
      })

      // Link all posts to the same author
      await client.Posts.update(post1.$id, { $link: { author: user.$id } })
      await client.Posts.update(post2.$id, { $link: { author: user.$id } })
      await client.Posts.update(post3.$id, { $link: { author: user.$id } })

      await waitForConsistency()

      // All posts should have the same author reference
      const fetchedPost1 = await client.Posts.get(post1.$id)
      const fetchedPost2 = await client.Posts.get(post2.$id)
      const fetchedPost3 = await client.Posts.get(post3.$id)

      // Verify author references
      expect((fetchedPost1 as any).author).toBeDefined()
      expect((fetchedPost2 as any).author).toBeDefined()
      expect((fetchedPost3 as any).author).toBeDefined()

      // All should reference the same user
      expect(Object.values((fetchedPost1 as any).author)[0]).toBe(user.$id)
      expect(Object.values((fetchedPost2 as any).author)[0]).toBe(user.$id)
      expect(Object.values((fetchedPost3 as any).author)[0]).toBe(user.$id)
    })
  })

  // ===========================================================================
  // Many-to-Many Relationships
  // ===========================================================================

  describe('Many-to-Many Relationships', () => {
    it('links entity to multiple targets', async () => {
      const user1 = await client.Users.create({
        $type: 'User',
        name: 'Co-author 1',
      })

      const user2 = await client.Users.create({
        $type: 'User',
        name: 'Co-author 2',
      })

      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Collaborative Post',
        title: 'Multi-Author Post',
      })

      // Link post to multiple authors
      await client.Posts.update(post.$id, {
        $link: { authors: [user1.$id, user2.$id] },
      })

      await waitForConsistency()

      const fetched = await client.Posts.get(post.$id)

      // The authors field should be a RelSet with both users
      expect((fetched as any).authors).toBeDefined()
      const authorsRef = (fetched as any).authors
      expect(typeof authorsRef).toBe('object')

      // Should have both author references
      const authorIds = Object.values(authorsRef) as string[]
      expect(authorIds).toContain(user1.$id)
      expect(authorIds).toContain(user2.$id)
    })

    it('supports adding to existing many-to-many relationship', async () => {
      const user1 = await client.Users.create({ $type: 'User', name: 'User 1' })
      const user2 = await client.Users.create({ $type: 'User', name: 'User 2' })
      const user3 = await client.Users.create({ $type: 'User', name: 'User 3' })

      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Growing Post',
        title: 'Add Authors',
      })

      // Add first author
      await client.Posts.update(post.$id, {
        $link: { authors: user1.$id },
      })

      // Add second and third authors
      await client.Posts.update(post.$id, {
        $link: { authors: [user2.$id, user3.$id] },
      })

      await waitForConsistency()

      const fetched = await client.Posts.get(post.$id)
      const authorIds = Object.values((fetched as any).authors) as string[]

      expect(authorIds).toContain(user1.$id)
      expect(authorIds).toContain(user2.$id)
      expect(authorIds).toContain(user3.$id)
    })

    it('supports removing from many-to-many relationship', async () => {
      const user1 = await client.Users.create({ $type: 'User', name: 'Keep' })
      const user2 = await client.Users.create({ $type: 'User', name: 'Remove' })

      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Shrinking Post',
        title: 'Remove Author',
      })

      await client.Posts.update(post.$id, {
        $link: { authors: [user1.$id, user2.$id] },
      })

      // Remove one author
      await client.Posts.update(post.$id, {
        $unlink: { authors: user2.$id },
      })

      await waitForConsistency()

      const fetched = await client.Posts.get(post.$id)
      const authorIds = Object.values((fetched as any).authors) as string[]

      expect(authorIds).toContain(user1.$id)
      expect(authorIds).not.toContain(user2.$id)
    })
  })

  // ===========================================================================
  // Relationship Edge Cases
  // ===========================================================================

  describe('Relationship Edge Cases', () => {
    it('handles linking to non-existent entity gracefully', async () => {
      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Orphan Link',
        title: 'Link to Nothing',
      })

      // Attempt to link to non-existent user
      await expect(
        client.Posts.update(post.$id, {
          $link: { author: 'users/non-existent' as any },
        })
      ).rejects.toThrow()
    })

    it('handles deleting entity with relationships', async () => {
      const user = await client.Users.create({
        $type: 'User',
        name: 'To Delete',
      })

      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Linked Post',
        title: 'Has Author',
      })

      await client.Posts.update(post.$id, { $link: { author: user.$id } })

      // Delete the user
      await client.Users.delete(user.$id, { hard: true })

      await waitForConsistency()

      // The post should still exist
      const fetched = await client.Posts.get(post.$id)
      expect(fetched).not.toBeNull()

      // The author reference still exists in the post (dangling reference)
      // ParqueDB doesn't automatically clean up dangling references
      expect((fetched as any).author).toBeDefined()
    })

    it('handles duplicate link operations idempotently', async () => {
      const user = await client.Users.create({
        $type: 'User',
        name: 'Author',
      })

      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Double Link',
        title: 'Link Twice',
      })

      // Link the same relationship twice
      await client.Posts.update(post.$id, { $link: { author: user.$id } })
      await client.Posts.update(post.$id, { $link: { author: user.$id } })

      await waitForConsistency()

      const fetched = await client.Posts.get(post.$id)

      // Should only have one author reference, not duplicates
      expect((fetched as any).author).toBeDefined()
      const authorIds = Object.values((fetched as any).author) as string[]
      expect(authorIds.length).toBe(1)
      expect(authorIds[0]).toBe(user.$id)
    })

    it('increments version on relationship changes', async () => {
      const user = await client.Users.create({
        $type: 'User',
        name: 'Version Test',
      })

      const post = await client.Posts.create({
        $type: 'Post',
        name: 'Version Post',
        title: 'Version Test',
      })

      expect(post.version).toBe(1)

      const linked = await client.Posts.update(post.$id, {
        $link: { author: user.$id },
      })

      expect(linked!.version).toBe(2)

      const unlinked = await client.Posts.update(post.$id, {
        $unlink: { author: user.$id },
      })

      expect(unlinked!.version).toBe(3)
    })
  })
})
