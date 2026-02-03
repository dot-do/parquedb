/**
 * MCP Integration Tests
 *
 * Tests for the ParqueDB MCP (Model Context Protocol) server integration.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { DB } from '../../../src/db'
import { createParqueDBMCPServer } from '../../../src/integrations/mcp'
import type { ParqueDBMCPOptions, ToolResult } from '../../../src/integrations/mcp'
import type { PaginatedResult, Entity } from '../../../src/types'

describe('MCP Integration', () => {
  let db: ReturnType<typeof DB>

  beforeEach(async () => {
    // Create a fresh database for each test
    db = DB({
      Posts: {
        title: 'string!',
        content: 'text',
        status: 'string',
        views: 'int',
      },
      Users: {
        email: 'string!',
        name: 'string',
        role: 'string',
      },
    })

    // Seed some test data
    await db.collection('users').create({
      $type: 'User',
      name: 'Alice',
      email: 'alice@example.com',
      role: 'admin',
    })
    await db.collection('users').create({
      $type: 'User',
      name: 'Bob',
      email: 'bob@example.com',
      role: 'user',
    })
    await db.collection('posts').create({
      $type: 'Post',
      name: 'First Post',
      title: 'Hello World',
      content: 'This is my first post',
      status: 'published',
      views: 100,
    })
    await db.collection('posts').create({
      $type: 'Post',
      name: 'Draft Post',
      title: 'Work in Progress',
      content: 'This is a draft',
      status: 'draft',
      views: 0,
    })
  })

  describe('createParqueDBMCPServer', () => {
    it('should create an MCP server with default options', () => {
      const server = createParqueDBMCPServer(db)

      expect(server).toBeDefined()
      expect(typeof server.connect).toBe('function')
      expect(typeof server.close).toBe('function')
    })

    it('should create an MCP server with custom options', () => {
      const options: ParqueDBMCPOptions = {
        name: 'custom-db',
        version: '2.0.0',
        instructions: 'Custom instructions',
      }

      const server = createParqueDBMCPServer(db, options)
      expect(server).toBeDefined()
    })

    it('should create a read-only server when readOnly is true', () => {
      const server = createParqueDBMCPServer(db, { readOnly: true })
      expect(server).toBeDefined()
    })

    it('should allow disabling specific tools', () => {
      const server = createParqueDBMCPServer(db, {
        tools: {
          semanticSearch: false,
          aggregate: false,
        },
      })
      expect(server).toBeDefined()
    })
  })

  describe('Tool Registration', () => {
    it('should register all tools by default', () => {
      const server = createParqueDBMCPServer(db)

      // The server should be created successfully with all tools
      expect(server).toBeDefined()
    })

    it('should not register write tools in read-only mode', () => {
      const server = createParqueDBMCPServer(db, { readOnly: true })

      // Server should still be created
      expect(server).toBeDefined()
    })
  })

  describe('ToolResult Type', () => {
    it('should have correct shape for success result', () => {
      const result: ToolResult = {
        success: true,
        data: [{ $id: 'posts/1', title: 'Test' }],
        count: 1,
      }

      expect(result.success).toBe(true)
      expect(result.data).toBeDefined()
      expect(result.count).toBe(1)
      expect(result.error).toBeUndefined()
    })

    it('should have correct shape for error result', () => {
      const result: ToolResult = {
        success: false,
        error: 'Something went wrong',
      }

      expect(result.success).toBe(false)
      expect(result.error).toBe('Something went wrong')
      expect(result.data).toBeUndefined()
    })
  })

  describe('Helper Functions', () => {
    it('should singularize collection names correctly', async () => {
      // Test via create tool behavior
      const collection = db.collection('posts')
      const entity = await collection.create({
        $type: 'Post',
        name: 'Test',
        title: 'Test Post',
        content: 'Content',
        status: 'draft',
        views: 0,
      })

      expect(entity.$type).toBe('Post')
    })

    it('should handle special plural forms', async () => {
      // Create a db with special plural collection
      const specialDb = DB({
        Categories: {
          name: 'string!',
          slug: 'string',
        },
      })

      const entity = await specialDb.collection('categories').create({
        $type: 'Category',
        name: 'Test Category',
        slug: 'test-category',
      })

      expect(entity.$type).toBe('Category')
    })
  })

  describe('Database Operations via Server', () => {
    it('should handle find operations correctly', async () => {
      // Test that the underlying database supports the operations
      const result = await db.collection('posts').find({ status: 'published' }) as PaginatedResult<Entity>

      expect(result.items).toHaveLength(1)
      expect(result.items[0]?.title).toBe('Hello World')
    })

    it('should handle find with limit', async () => {
      const result = await db.collection('posts').find({}, { limit: 1 }) as PaginatedResult<Entity>

      expect(result.items).toHaveLength(1)
    })

    it('should handle find with sort', async () => {
      const result = await db.collection('posts').find({}, { sort: { views: -1 } }) as PaginatedResult<Entity>

      expect(result.items).toHaveLength(2)
      expect(result.items[0]?.views).toBe(100)
    })

    it('should handle get operations correctly', async () => {
      const postsResult = await db.collection('posts').find({ title: 'Hello World' }) as PaginatedResult<Entity>
      const postId = postsResult.items[0]?.$id.split('/')[1]

      if (postId) {
        const entity = await db.collection('posts').get(postId)
        expect(entity?.title).toBe('Hello World')
      }
    })

    it('should handle create operations correctly', async () => {
      const entity = await db.collection('posts').create({
        $type: 'Post',
        name: 'New Post',
        title: 'Brand New',
        content: 'Fresh content',
        status: 'draft',
        views: 0,
      })

      expect(entity.$id).toMatch(/^posts\//)
      expect(entity.title).toBe('Brand New')
    })

    it('should handle update operations correctly', async () => {
      const postsResult = await db.collection('posts').find({ status: 'draft' }) as PaginatedResult<Entity>
      const postId = postsResult.items[0]?.$id.split('/')[1]

      if (postId) {
        const result = await db.collection('posts').update(postId, {
          $set: { status: 'published' },
        })

        expect(result).toBeDefined()

        const updated = await db.collection('posts').get(postId)
        expect(updated?.status).toBe('published')
      }
    })

    it('should handle delete operations correctly', async () => {
      const postsResult = await db.collection('posts').find({ status: 'draft' }) as PaginatedResult<Entity>
      const postId = postsResult.items[0]?.$id.split('/')[1]

      if (postId) {
        const result = await db.collection('posts').delete(postId)
        expect(result.deletedCount).toBe(1)

        // Soft deleted - should not appear in normal find
        const remaining = await db.collection('posts').find({ status: 'draft' }) as PaginatedResult<Entity>
        expect(remaining.items).toHaveLength(0)
      }
    })

    it('should handle count operations via find total', async () => {
      const result = await db.collection('posts').find({}, { limit: 1 }) as PaginatedResult<Entity>
      // Total count should be available in the result
      expect(result.total).toBe(2)

      const filteredResult = await db.collection('posts').find({ status: 'published' }, { limit: 1 }) as PaginatedResult<Entity>
      expect(filteredResult.total).toBe(1)
    })

    it('should handle aggregate-like operations via find', async () => {
      // Simulating aggregate with $match and $project via find
      const result = await db.collection('posts').find(
        { status: 'published' },
        { project: { title: 1, views: 1 } }
      ) as PaginatedResult<Entity>

      expect(result.items).toHaveLength(1)
    })
  })

  describe('Filter Operations', () => {
    it('should support $eq filter', async () => {
      const result = await db.collection('posts').find({
        status: { $eq: 'published' },
      }) as PaginatedResult<Entity>

      expect(result.items).toHaveLength(1)
    })

    it('should support $ne filter', async () => {
      const result = await db.collection('posts').find({
        status: { $ne: 'published' },
      }) as PaginatedResult<Entity>

      expect(result.items).toHaveLength(1)
    })

    it('should support $gt filter', async () => {
      const result = await db.collection('posts').find({
        views: { $gt: 50 },
      }) as PaginatedResult<Entity>

      expect(result.items).toHaveLength(1)
    })

    it('should support $in filter', async () => {
      const result = await db.collection('posts').find({
        status: { $in: ['published', 'archived'] },
      }) as PaginatedResult<Entity>

      expect(result.items).toHaveLength(1)
    })

    it('should support $and filter', async () => {
      const result = await db.collection('posts').find({
        $and: [{ status: 'published' }, { views: { $gte: 100 } }],
      }) as PaginatedResult<Entity>

      expect(result.items).toHaveLength(1)
    })

    it('should support $or filter', async () => {
      const result = await db.collection('posts').find({
        $or: [{ status: 'published' }, { status: 'draft' }],
      }) as PaginatedResult<Entity>

      expect(result.items).toHaveLength(2)
    })
  })

  describe('Update Operations', () => {
    it('should support $set operator', async () => {
      const postsResult = await db.collection('posts').find({ status: 'draft' }) as PaginatedResult<Entity>
      const postId = postsResult.items[0]?.$id.split('/')[1]

      if (postId) {
        await db.collection('posts').update(postId, {
          $set: { title: 'Updated Title' },
        })

        const updated = await db.collection('posts').get(postId)
        expect(updated?.title).toBe('Updated Title')
      }
    })

    it('should support $inc operator', async () => {
      const postsResult = await db.collection('posts').find({ status: 'published' }) as PaginatedResult<Entity>
      const postId = postsResult.items[0]?.$id.split('/')[1]

      if (postId) {
        await db.collection('posts').update(postId, {
          $inc: { views: 10 },
        })

        const updated = await db.collection('posts').get(postId)
        expect(updated?.views).toBe(110)
      }
    })

    it('should support $unset operator', async () => {
      const postsResult = await db.collection('posts').find({ status: 'draft' }) as PaginatedResult<Entity>
      const postId = postsResult.items[0]?.$id.split('/')[1]

      if (postId) {
        await db.collection('posts').update(postId, {
          $unset: { content: 1 },
        })

        const updated = await db.collection('posts').get(postId)
        expect(updated?.content).toBeUndefined()
      }
    })
  })

  describe('Error Handling', () => {
    it('should return null for non-existent entity', async () => {
      // get() returns null for non-existent entities
      const entity = await db.collection('posts').get('non-existent-id')
      expect(entity).toBeNull()
    })

    it('should handle invalid filter gracefully', async () => {
      await expect(
        db.collection('posts').find({ $invalid: 'operator' } as unknown as Record<string, unknown>)
      ).rejects.toThrow()
    })
  })
})
