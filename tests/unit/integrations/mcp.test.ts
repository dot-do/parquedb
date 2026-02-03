/**
 * MCP Integration Tests
 *
 * Tests for the ParqueDB MCP (Model Context Protocol) server integration.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { DB } from '../../../src/db'
import { createParqueDBMCPServer, ValidationError } from '../../../src/integrations/mcp'
import type { ParqueDBMCPOptions, ToolResult } from '../../../src/integrations/mcp'
import type { PaginatedResult, Entity } from '../../../src/types'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import {
  validateCollectionName,
  validateEntityId,
  validateFilter,
  validateUpdate,
  validateEntityData,
  validatePipeline,
  validateLimit,
  validateSkip,
  validateSort,
  validateProject,
  validateBoolean,
  validateSearchQuery,
  sanitizeObject,
} from '../../../src/integrations/mcp/validation'

/**
 * Helper to parse MCP tool result content
 */
function parseToolResult(content: Array<{ type: string; text?: string }>): ToolResult {
  const textContent = content.find(c => c.type === 'text')
  if (!textContent?.text) {
    throw new Error('No text content in tool result')
  }
  return JSON.parse(textContent.text) as ToolResult
}

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

/**
 * MCP Tool Invocation Tests
 *
 * Tests that verify actual MCP protocol behavior including tool registration,
 * invocation through the MCP client, and proper response formatting.
 */
describe('MCP Tool Invocation', () => {
  let db: ReturnType<typeof DB>
  let server: McpServer
  let client: Client
  let clientTransport: InMemoryTransport
  let serverTransport: InMemoryTransport

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

    // Create MCP server and client with linked transports
    server = createParqueDBMCPServer(db)
    client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

    // Create linked transports
    const [transport1, transport2] = InMemoryTransport.createLinkedPair()
    clientTransport = transport1
    serverTransport = transport2

    // Connect both sides
    await server.connect(serverTransport)
    await client.connect(clientTransport)
  })

  afterEach(async () => {
    await client.close()
    await server.close()
  })

  describe('Tool Registration Verification', () => {
    it('should list all default tools', async () => {
      const result = await client.listTools()

      expect(result.tools).toBeDefined()
      expect(Array.isArray(result.tools)).toBe(true)

      const toolNames = result.tools.map(t => t.name)
      expect(toolNames).toContain('parquedb_find')
      expect(toolNames).toContain('parquedb_get')
      expect(toolNames).toContain('parquedb_create')
      expect(toolNames).toContain('parquedb_update')
      expect(toolNames).toContain('parquedb_delete')
      expect(toolNames).toContain('parquedb_count')
      expect(toolNames).toContain('parquedb_aggregate')
      expect(toolNames).toContain('parquedb_list_collections')
      expect(toolNames).toContain('parquedb_semantic_search')
    })

    it('should have correct schema for parquedb_find tool', async () => {
      const result = await client.listTools()
      const findTool = result.tools.find(t => t.name === 'parquedb_find')

      expect(findTool).toBeDefined()
      expect(findTool?.description).toContain('Query documents')
      expect(findTool?.inputSchema).toBeDefined()
      expect(findTool?.inputSchema.type).toBe('object')
      expect(findTool?.inputSchema.properties).toBeDefined()
      expect(findTool?.inputSchema.properties?.collection).toBeDefined()
      expect(findTool?.inputSchema.properties?.filter).toBeDefined()
      expect(findTool?.inputSchema.properties?.limit).toBeDefined()
      expect(findTool?.inputSchema.properties?.sort).toBeDefined()
    })

    it('should have correct schema for parquedb_create tool', async () => {
      const result = await client.listTools()
      const createTool = result.tools.find(t => t.name === 'parquedb_create')

      expect(createTool).toBeDefined()
      expect(createTool?.description).toContain('Create a new document')
      expect(createTool?.inputSchema).toBeDefined()
      expect(createTool?.inputSchema.properties?.collection).toBeDefined()
      expect(createTool?.inputSchema.properties?.data).toBeDefined()
    })

    it('should not include write tools in read-only mode', async () => {
      // Close existing connections
      await client.close()
      await server.close()

      // Create new read-only server
      const readOnlyServer = createParqueDBMCPServer(db, { readOnly: true })
      const readOnlyClient = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()

      await readOnlyServer.connect(t2)
      await readOnlyClient.connect(t1)

      const result = await readOnlyClient.listTools()
      const toolNames = result.tools.map(t => t.name)

      // Read tools should be present
      expect(toolNames).toContain('parquedb_find')
      expect(toolNames).toContain('parquedb_get')
      expect(toolNames).toContain('parquedb_count')

      // Write tools should NOT be present
      expect(toolNames).not.toContain('parquedb_create')
      expect(toolNames).not.toContain('parquedb_update')
      expect(toolNames).not.toContain('parquedb_delete')

      await readOnlyClient.close()
      await readOnlyServer.close()
    })

    it('should respect disabled tools configuration', async () => {
      // Close existing connections
      await client.close()
      await server.close()

      // Create server with some tools disabled
      const limitedServer = createParqueDBMCPServer(db, {
        tools: {
          semanticSearch: false,
          aggregate: false,
        },
      })
      const limitedClient = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()

      await limitedServer.connect(t2)
      await limitedClient.connect(t1)

      const result = await limitedClient.listTools()
      const toolNames = result.tools.map(t => t.name)

      // Enabled tools should be present
      expect(toolNames).toContain('parquedb_find')
      expect(toolNames).toContain('parquedb_create')

      // Disabled tools should NOT be present
      expect(toolNames).not.toContain('parquedb_semantic_search')
      expect(toolNames).not.toContain('parquedb_aggregate')

      await limitedClient.close()
      await limitedServer.close()
    })
  })

  describe('Tool Invocation - Read Operations', () => {
    it('should invoke parquedb_find and return results', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          filter: { status: 'published' },
        },
      })

      expect(result.content).toBeDefined()
      expect(Array.isArray(result.content)).toBe(true)
      expect(result.isError).toBeFalsy()

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(Array.isArray(parsed.data)).toBe(true)
      expect(parsed.count).toBe(1)
      expect((parsed.data as Array<{ title: string }>)[0]?.title).toBe('Hello World')
    })

    it('should invoke parquedb_find with limit and sort', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          limit: 1,
          sort: { views: -1 },
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(1)
      expect((parsed.data as Array<{ views: number }>)[0]?.views).toBe(100)
    })

    it('should invoke parquedb_get and return single entity', async () => {
      // First find a post to get its ID
      const findResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          filter: { title: 'Hello World' },
        },
      })
      const findParsed = parseToolResult(findResult.content as Array<{ type: string; text?: string }>)
      const postId = ((findParsed.data as Array<{ $id: string }>)[0]?.$id || '').split('/')[1]

      // Now get by ID
      const result = await client.callTool({
        name: 'parquedb_get',
        arguments: {
          collection: 'posts',
          id: postId,
        },
      })

      expect(result.isError).toBeFalsy()
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect((parsed.data as { title: string })?.title).toBe('Hello World')
    })

    it('should invoke parquedb_count and return document count', async () => {
      const result = await client.callTool({
        name: 'parquedb_count',
        arguments: {
          collection: 'posts',
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(2)
    })

    it('should invoke parquedb_count with filter', async () => {
      const result = await client.callTool({
        name: 'parquedb_count',
        arguments: {
          collection: 'posts',
          filter: { status: 'published' },
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(1)
    })

    it('should invoke parquedb_list_collections', async () => {
      const result = await client.callTool({
        name: 'parquedb_list_collections',
        arguments: {},
      })

      // The tool should return a well-formed response
      expect(result.content).toBeDefined()
      expect(result.content).toHaveLength(1)

      const content = result.content[0] as { type: string; text: string }
      expect(content.type).toBe('text')

      const parsed = JSON.parse(content.text)
      // The response should have the standard ToolResult shape
      expect(typeof parsed.success).toBe('boolean')

      // If successful, data should be an array (possibly empty if schema not available)
      if (parsed.success) {
        expect(Array.isArray(parsed.data)).toBe(true)
      }
    })

    it('should invoke parquedb_aggregate with pipeline', async () => {
      const result = await client.callTool({
        name: 'parquedb_aggregate',
        arguments: {
          collection: 'posts',
          pipeline: [
            { $match: { status: 'published' } },
            { $limit: 10 },
          ],
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(1)
    })
  })

  describe('Tool Invocation - Write Operations', () => {
    it('should invoke parquedb_create and return created entity', async () => {
      const result = await client.callTool({
        name: 'parquedb_create',
        arguments: {
          collection: 'posts',
          data: {
            name: 'New MCP Post',
            title: 'Created via MCP',
            content: 'This was created through the MCP protocol',
            status: 'draft',
            views: 0,
          },
        },
      })

      expect(result.isError).toBeFalsy()
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect((parsed.data as { $id: string })?.$id).toMatch(/^posts\//)
      expect((parsed.data as { title: string })?.title).toBe('Created via MCP')
      expect((parsed.data as { $type: string })?.$type).toBe('Post')
    })

    it('should invoke parquedb_update and modify entity', async () => {
      // First find a post to update
      const findResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          filter: { status: 'draft' },
        },
      })
      const findParsed = parseToolResult(findResult.content as Array<{ type: string; text?: string }>)
      const postId = ((findParsed.data as Array<{ $id: string }>)[0]?.$id || '').split('/')[1]

      // Update the post
      const result = await client.callTool({
        name: 'parquedb_update',
        arguments: {
          collection: 'posts',
          id: postId,
          update: {
            $set: { status: 'published', views: 50 },
          },
        },
      })

      expect(result.isError).toBeFalsy()
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)

      // Verify the update
      const getResult = await client.callTool({
        name: 'parquedb_get',
        arguments: {
          collection: 'posts',
          id: postId,
        },
      })
      const getParsed = parseToolResult(getResult.content as Array<{ type: string; text?: string }>)
      expect((getParsed.data as { status: string })?.status).toBe('published')
      expect((getParsed.data as { views: number })?.views).toBe(50)
    })

    it('should invoke parquedb_delete and remove entity', async () => {
      // First create a post to delete
      const createResult = await client.callTool({
        name: 'parquedb_create',
        arguments: {
          collection: 'posts',
          data: {
            name: 'To Delete',
            title: 'Will Be Deleted',
            content: 'This will be deleted',
            status: 'draft',
            views: 0,
          },
        },
      })
      const createParsed = parseToolResult(createResult.content as Array<{ type: string; text?: string }>)
      const postId = ((createParsed.data as { $id: string })?.$id || '').split('/')[1]

      // Delete the post
      const result = await client.callTool({
        name: 'parquedb_delete',
        arguments: {
          collection: 'posts',
          id: postId,
        },
      })

      expect(result.isError).toBeFalsy()
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)

      // Verify deletion (soft delete - entity should be marked deleted)
      const getResult = await client.callTool({
        name: 'parquedb_get',
        arguments: {
          collection: 'posts',
          id: postId,
        },
      })
      const getParsed = parseToolResult(getResult.content as Array<{ type: string; text?: string }>)
      // Soft deleted entities return null when fetched normally
      expect(getParsed.data).toBeNull()
    })
  })

  describe('Tool Output Format Validation', () => {
    it('should return proper ToolResult format for success', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
        },
      })

      expect(result.content).toBeDefined()
      expect(result.content).toHaveLength(1)

      const content = result.content[0] as { type: string; text: string }
      expect(content.type).toBe('text')
      expect(content.text).toBeDefined()

      const parsed = JSON.parse(content.text)
      expect(parsed).toHaveProperty('success')
      expect(parsed).toHaveProperty('data')
      expect(parsed).toHaveProperty('count')
      expect(parsed).toHaveProperty('total')
      expect(parsed).toHaveProperty('hasMore')
    })

    it('should return isError flag for semantic search without vector index', async () => {
      const result = await client.callTool({
        name: 'parquedb_semantic_search',
        arguments: {
          collection: 'posts',
          query: 'hello world',
        },
      })

      // Semantic search should error when no vector index is configured
      expect(result.isError).toBe(true)

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toBeDefined()
      expect(parsed.error).toContain('Semantic search is not available')
    })

    it('should include pagination info in find results', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          limit: 1,
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(1)
      expect(parsed.total).toBe(2)
      expect(typeof parsed.hasMore).toBe('boolean')
    })
  })

  describe('Error Responses', () => {
    it('should return error for invalid tool name', async () => {
      const result = await client.callTool({
        name: 'parquedb_nonexistent',
        arguments: {},
      })

      // MCP SDK returns an error response instead of throwing
      expect(result.isError).toBe(true)

      const content = result.content[0] as { type: string; text: string }
      expect(content.type).toBe('text')
      expect(content.text).toContain('not found')
    })

    it('should return error for missing required arguments', async () => {
      // parquedb_find requires collection
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {},
      })

      // The SDK should validate and return an error
      expect(result.isError).toBe(true)
    })

    it('should return error for invalid filter operator through DB', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          filter: { $invalidOperator: 'value' },
        },
      })

      // The underlying DB should throw, which gets caught and returned as error
      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toBeDefined()
    })

    it('should return null data for non-existent entity', async () => {
      const result = await client.callTool({
        name: 'parquedb_get',
        arguments: {
          collection: 'posts',
          id: 'non-existent-id-12345',
        },
      })

      // Getting non-existent entity is not an error, just returns null
      expect(result.isError).toBeFalsy()
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.data).toBeNull()
    })
  })

  describe('Complex Filter Operations via MCP', () => {
    it('should support $and filter through MCP', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          filter: {
            $and: [
              { status: 'published' },
              { views: { $gte: 100 } },
            ],
          },
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(1)
    })

    it('should support $or filter through MCP', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          filter: {
            $or: [
              { status: 'published' },
              { status: 'draft' },
            ],
          },
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(2)
    })

    it('should support $in filter through MCP', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'users',
          filter: {
            role: { $in: ['admin', 'moderator'] },
          },
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(1)
      expect((parsed.data as Array<{ name: string }>)[0]?.name).toBe('Alice')
    })

    it('should support comparison operators through MCP', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          filter: {
            views: { $gt: 50 },
          },
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(1)
    })
  })

  describe('Update Operators via MCP', () => {
    it('should support $inc operator through MCP', async () => {
      // Find a post
      const findResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          filter: { status: 'published' },
        },
      })
      const findParsed = parseToolResult(findResult.content as Array<{ type: string; text?: string }>)
      const postId = ((findParsed.data as Array<{ $id: string }>)[0]?.$id || '').split('/')[1]

      // Increment views
      await client.callTool({
        name: 'parquedb_update',
        arguments: {
          collection: 'posts',
          id: postId,
          update: {
            $inc: { views: 25 },
          },
        },
      })

      // Verify
      const getResult = await client.callTool({
        name: 'parquedb_get',
        arguments: {
          collection: 'posts',
          id: postId,
        },
      })
      const getParsed = parseToolResult(getResult.content as Array<{ type: string; text?: string }>)
      expect((getParsed.data as { views: number })?.views).toBe(125)
    })

    it('should support $unset operator through MCP', async () => {
      // Find a post
      const findResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          filter: { status: 'draft' },
        },
      })
      const findParsed = parseToolResult(findResult.content as Array<{ type: string; text?: string }>)
      const postId = ((findParsed.data as Array<{ $id: string }>)[0]?.$id || '').split('/')[1]

      // Unset content field
      await client.callTool({
        name: 'parquedb_update',
        arguments: {
          collection: 'posts',
          id: postId,
          update: {
            $unset: { content: 1 },
          },
        },
      })

      // Verify
      const getResult = await client.callTool({
        name: 'parquedb_get',
        arguments: {
          collection: 'posts',
          id: postId,
        },
      })
      const getParsed = parseToolResult(getResult.content as Array<{ type: string; text?: string }>)
      expect((getParsed.data as { content?: string })?.content).toBeUndefined()
    })
  })
})

/**
 * Input Validation Tests
 *
 * Tests for the validation module that prevents security issues and runtime errors.
 */
describe('Input Validation', () => {
  describe('validateCollectionName', () => {
    it('should accept valid collection names', () => {
      expect(validateCollectionName('posts')).toBe('posts')
      expect(validateCollectionName('Users')).toBe('Users')
      expect(validateCollectionName('my_collection')).toBe('my_collection')
      expect(validateCollectionName('Collection123')).toBe('Collection123')
    })

    it('should trim whitespace from collection names', () => {
      expect(validateCollectionName('  posts  ')).toBe('posts')
    })

    it('should reject null or undefined', () => {
      expect(() => validateCollectionName(null)).toThrow(ValidationError)
      expect(() => validateCollectionName(undefined)).toThrow(ValidationError)
      expect(() => validateCollectionName(null)).toThrow('is required')
    })

    it('should reject non-string values', () => {
      expect(() => validateCollectionName(123)).toThrow(ValidationError)
      expect(() => validateCollectionName({})).toThrow(ValidationError)
      expect(() => validateCollectionName([])).toThrow(ValidationError)
      expect(() => validateCollectionName(123)).toThrow('must be a string')
    })

    it('should reject empty strings', () => {
      expect(() => validateCollectionName('')).toThrow(ValidationError)
      expect(() => validateCollectionName('   ')).toThrow(ValidationError)
      expect(() => validateCollectionName('')).toThrow('cannot be empty')
    })

    it('should reject names starting with numbers', () => {
      expect(() => validateCollectionName('123posts')).toThrow(ValidationError)
      expect(() => validateCollectionName('1collection')).toThrow('must start with a letter')
    })

    it('should reject names with special characters', () => {
      expect(() => validateCollectionName('my-collection')).toThrow(ValidationError)
      expect(() => validateCollectionName('my.collection')).toThrow(ValidationError)
      expect(() => validateCollectionName('my/collection')).toThrow(ValidationError)
      expect(() => validateCollectionName('my collection')).toThrow(ValidationError)
    })

    it('should reject overly long names', () => {
      const longName = 'a'.repeat(101)
      expect(() => validateCollectionName(longName)).toThrow(ValidationError)
      expect(() => validateCollectionName(longName)).toThrow('100 characters or fewer')
    })
  })

  describe('validateEntityId', () => {
    it('should accept valid entity IDs', () => {
      expect(validateEntityId('abc123')).toBe('abc123')
      expect(validateEntityId('my-entity-id')).toBe('my-entity-id')
      expect(validateEntityId('entity_123')).toBe('entity_123')
      expect(validateEntityId('ABC-123_xyz')).toBe('ABC-123_xyz')
    })

    it('should trim whitespace from IDs', () => {
      expect(validateEntityId('  abc123  ')).toBe('abc123')
    })

    it('should reject null or undefined', () => {
      expect(() => validateEntityId(null)).toThrow(ValidationError)
      expect(() => validateEntityId(undefined)).toThrow(ValidationError)
    })

    it('should reject non-string values', () => {
      expect(() => validateEntityId(123)).toThrow(ValidationError)
      expect(() => validateEntityId({})).toThrow(ValidationError)
    })

    it('should reject empty strings', () => {
      expect(() => validateEntityId('')).toThrow(ValidationError)
      expect(() => validateEntityId('   ')).toThrow(ValidationError)
    })

    it('should reject path traversal attempts', () => {
      expect(() => validateEntityId('../etc/passwd')).toThrow(ValidationError)
      expect(() => validateEntityId('..\\windows\\system32')).toThrow(ValidationError)
      expect(() => validateEntityId('foo/bar')).toThrow(ValidationError)
      expect(() => validateEntityId('../etc/passwd')).toThrow('path traversal')
    })

    it('should reject IDs with special characters', () => {
      expect(() => validateEntityId('id@domain')).toThrow(ValidationError)
      expect(() => validateEntityId('id with space')).toThrow(ValidationError)
      expect(() => validateEntityId('id.with.dots')).toThrow(ValidationError)
    })

    it('should reject overly long IDs', () => {
      const longId = 'a'.repeat(257)
      expect(() => validateEntityId(longId)).toThrow(ValidationError)
      expect(() => validateEntityId(longId)).toThrow('256 characters or fewer')
    })
  })

  describe('sanitizeObject', () => {
    it('should sanitize simple objects', () => {
      const input = { foo: 'bar', num: 123, bool: true }
      const result = sanitizeObject(input, 'test')
      expect(result).toEqual({ foo: 'bar', num: 123, bool: true })
    })

    it('should handle null/undefined input', () => {
      expect(sanitizeObject(null, 'test')).toEqual({})
      expect(sanitizeObject(undefined, 'test')).toEqual({})
    })

    it('should reject non-object input', () => {
      expect(() => sanitizeObject('string', 'test')).toThrow(ValidationError)
      expect(() => sanitizeObject(123, 'test')).toThrow(ValidationError)
    })

    it('should reject arrays when object expected', () => {
      expect(() => sanitizeObject(['a', 'b'], 'test')).toThrow(ValidationError)
      expect(() => sanitizeObject(['a', 'b'], 'test')).toThrow('must be an object, not an array')
    })

    it('should remove dangerous prototype pollution keys', () => {
      const input = {
        '__proto__': { isAdmin: true },
        'constructor': { prototype: {} },
        'prototype': { evil: true },
        'valid': 'value',
      }
      const result = sanitizeObject(input, 'test')
      expect(result).toEqual({ valid: 'value' })
      expect(result).not.toHaveProperty('__proto__')
      expect(result).not.toHaveProperty('constructor')
      expect(result).not.toHaveProperty('prototype')
    })

    it('should sanitize nested objects recursively', () => {
      const input = {
        outer: {
          '__proto__': { bad: true },
          inner: 'value',
        },
      }
      const result = sanitizeObject(input, 'test')
      expect(result).toEqual({ outer: { inner: 'value' } })
    })

    it('should sanitize arrays within objects', () => {
      const input = {
        items: [
          { '__proto__': { bad: true }, name: 'item1' },
          { name: 'item2' },
        ],
      }
      const result = sanitizeObject(input, 'test')
      expect(result).toEqual({
        items: [
          { name: 'item1' },
          { name: 'item2' },
        ],
      })
    })

    it('should reject deeply nested objects beyond max depth', () => {
      // Create a deeply nested object
      let deepObj: Record<string, unknown> = { value: 'deep' }
      for (let i = 0; i < 15; i++) {
        deepObj = { nested: deepObj }
      }
      expect(() => sanitizeObject(deepObj, 'test')).toThrow(ValidationError)
      expect(() => sanitizeObject(deepObj, 'test')).toThrow('maximum nesting depth')
    })

    it('should reject overly long string values', () => {
      const input = { longString: 'a'.repeat(10001) }
      expect(() => sanitizeObject(input, 'test')).toThrow(ValidationError)
      expect(() => sanitizeObject(input, 'test')).toThrow('maximum length')
    })
  })

  describe('validateFilter', () => {
    it('should accept valid filters', () => {
      expect(validateFilter({ status: 'published' })).toEqual({ status: 'published' })
      expect(validateFilter({ count: { $gt: 10 } })).toEqual({ count: { $gt: 10 } })
    })

    it('should accept null/undefined as no filter', () => {
      expect(validateFilter(null)).toBeUndefined()
      expect(validateFilter(undefined)).toBeUndefined()
    })

    it('should accept all valid filter operators', () => {
      // Comparison operators
      expect(validateFilter({ a: { $eq: 1 } })).toEqual({ a: { $eq: 1 } })
      expect(validateFilter({ a: { $ne: 1 } })).toEqual({ a: { $ne: 1 } })
      expect(validateFilter({ a: { $gt: 1 } })).toEqual({ a: { $gt: 1 } })
      expect(validateFilter({ a: { $gte: 1 } })).toEqual({ a: { $gte: 1 } })
      expect(validateFilter({ a: { $lt: 1 } })).toEqual({ a: { $lt: 1 } })
      expect(validateFilter({ a: { $lte: 1 } })).toEqual({ a: { $lte: 1 } })

      // Array operators
      expect(validateFilter({ a: { $in: [1, 2] } })).toEqual({ a: { $in: [1, 2] } })
      expect(validateFilter({ a: { $nin: [1, 2] } })).toEqual({ a: { $nin: [1, 2] } })

      // Logical operators
      expect(validateFilter({ $and: [{ a: 1 }, { b: 2 }] })).toEqual({ $and: [{ a: 1 }, { b: 2 }] })
      expect(validateFilter({ $or: [{ a: 1 }, { b: 2 }] })).toEqual({ $or: [{ a: 1 }, { b: 2 }] })

      // String operators
      expect(validateFilter({ a: { $regex: 'test' } })).toEqual({ a: { $regex: 'test' } })
      expect(validateFilter({ a: { $startsWith: 'test' } })).toEqual({ a: { $startsWith: 'test' } })

      // Existence operators
      expect(validateFilter({ a: { $exists: true } })).toEqual({ a: { $exists: true } })
    })

    it('should reject unknown operators', () => {
      expect(() => validateFilter({ a: { $invalid: 1 } })).toThrow(ValidationError)
      expect(() => validateFilter({ a: { $invalid: 1 } })).toThrow('unknown operator: $invalid')
    })

    it('should reject non-object filters', () => {
      expect(() => validateFilter('string')).toThrow(ValidationError)
      expect(() => validateFilter(123)).toThrow(ValidationError)
    })

    it('should reject array filters', () => {
      expect(() => validateFilter([{ a: 1 }])).toThrow(ValidationError)
    })

    it('should sanitize and validate nested filters in logical operators', () => {
      const filter = {
        $and: [
          { status: 'active', '__proto__': { bad: true } },
          { count: { $gt: 5, $invalid: 1 } },
        ],
      }
      expect(() => validateFilter(filter)).toThrow('unknown operator: $invalid')
    })
  })

  describe('validateUpdate', () => {
    it('should accept valid update operations', () => {
      expect(validateUpdate({ $set: { name: 'new' } })).toEqual({ $set: { name: 'new' } })
      expect(validateUpdate({ $inc: { count: 1 } })).toEqual({ $inc: { count: 1 } })
      expect(validateUpdate({ $unset: { field: 1 } })).toEqual({ $unset: { field: 1 } })
    })

    it('should accept all valid update operators', () => {
      expect(validateUpdate({ $set: { a: 1 } })).toBeDefined()
      expect(validateUpdate({ $unset: { a: 1 } })).toBeDefined()
      expect(validateUpdate({ $inc: { a: 1 } })).toBeDefined()
      expect(validateUpdate({ $push: { a: 1 } })).toBeDefined()
      expect(validateUpdate({ $pull: { a: 1 } })).toBeDefined()
      expect(validateUpdate({ $addToSet: { a: 1 } })).toBeDefined()
    })

    it('should reject null or undefined', () => {
      expect(() => validateUpdate(null)).toThrow(ValidationError)
      expect(() => validateUpdate(undefined)).toThrow(ValidationError)
    })

    it('should reject empty update objects', () => {
      expect(() => validateUpdate({})).toThrow(ValidationError)
      expect(() => validateUpdate({})).toThrow('cannot be empty')
    })

    it('should reject unknown update operators', () => {
      expect(() => validateUpdate({ $invalid: { a: 1 } })).toThrow(ValidationError)
      expect(() => validateUpdate({ $invalid: { a: 1 } })).toThrow('unknown operator: $invalid')
    })

    it('should reject updates without operators', () => {
      expect(() => validateUpdate({ name: 'new' })).toThrow(ValidationError)
      expect(() => validateUpdate({ name: 'new' })).toThrow('update operations must use operators')
    })

    it('should reject non-object operator values', () => {
      expect(() => validateUpdate({ $set: 'value' })).toThrow(ValidationError)
      expect(() => validateUpdate({ $set: ['array'] })).toThrow(ValidationError)
    })
  })

  describe('validateEntityData', () => {
    it('should accept valid entity data', () => {
      const data = { name: 'Test Entity', title: 'Hello' }
      expect(validateEntityData(data)).toEqual(data)
    })

    it('should reject null or undefined', () => {
      expect(() => validateEntityData(null)).toThrow(ValidationError)
      expect(() => validateEntityData(undefined)).toThrow(ValidationError)
    })

    it('should reject non-object data', () => {
      expect(() => validateEntityData('string')).toThrow(ValidationError)
      expect(() => validateEntityData(123)).toThrow(ValidationError)
    })

    it('should require name field', () => {
      expect(() => validateEntityData({ title: 'No Name' })).toThrow(ValidationError)
      expect(() => validateEntityData({ title: 'No Name' })).toThrow('name')
      expect(() => validateEntityData({ title: 'No Name' })).toThrow('is required')
    })

    it('should require name to be a non-empty string', () => {
      expect(() => validateEntityData({ name: 123 })).toThrow(ValidationError)
      expect(() => validateEntityData({ name: '' })).toThrow(ValidationError)
      expect(() => validateEntityData({ name: '   ' })).toThrow(ValidationError)
    })

    it('should sanitize data and remove dangerous keys', () => {
      const data = {
        name: 'Test',
        '__proto__': { bad: true },
        valid: 'value',
      }
      const result = validateEntityData(data)
      expect(result).toEqual({ name: 'Test', valid: 'value' })
    })
  })

  describe('validatePipeline', () => {
    it('should accept valid pipelines', () => {
      const pipeline = [
        { $match: { status: 'active' } },
        { $project: { name: 1 } },
        { $limit: 10 },
      ]
      const result = validatePipeline(pipeline)
      expect(result).toEqual(pipeline)
    })

    it('should reject null or undefined', () => {
      expect(() => validatePipeline(null)).toThrow(ValidationError)
      expect(() => validatePipeline(undefined)).toThrow(ValidationError)
    })

    it('should reject non-array pipelines', () => {
      expect(() => validatePipeline({ $match: {} })).toThrow(ValidationError)
      expect(() => validatePipeline('string')).toThrow(ValidationError)
    })

    it('should reject empty pipelines', () => {
      expect(() => validatePipeline([])).toThrow(ValidationError)
      expect(() => validatePipeline([])).toThrow('cannot be empty')
    })

    it('should reject pipelines with too many stages', () => {
      const pipeline = Array(21).fill({ $limit: 10 })
      expect(() => validatePipeline(pipeline)).toThrow(ValidationError)
      expect(() => validatePipeline(pipeline)).toThrow('cannot exceed 20 stages')
    })

    it('should reject invalid stage formats', () => {
      expect(() => validatePipeline(['string'])).toThrow(ValidationError)
      expect(() => validatePipeline([null])).toThrow(ValidationError)
    })

    it('should reject stages without $ prefix', () => {
      expect(() => validatePipeline([{ match: {} }])).toThrow(ValidationError)
      expect(() => validatePipeline([{ match: {} }])).toThrow('must start with $')
    })

    it('should reject unknown pipeline stages', () => {
      expect(() => validatePipeline([{ $invalid: {} }])).toThrow(ValidationError)
      expect(() => validatePipeline([{ $invalid: {} }])).toThrow('unknown or unsupported stage')
    })

    it('should reject stages with multiple operators', () => {
      expect(() => validatePipeline([{ $match: {}, $limit: 10 }])).toThrow(ValidationError)
      expect(() => validatePipeline([{ $match: {}, $limit: 10 }])).toThrow('exactly one stage operator')
    })

    it('should validate filter operators within $match stages', () => {
      expect(() => validatePipeline([{ $match: { a: { $invalid: 1 } } }])).toThrow(ValidationError)
    })
  })

  describe('validateLimit', () => {
    it('should accept valid limits', () => {
      expect(validateLimit(10)).toBe(10)
      expect(validateLimit(0)).toBe(0)
      expect(validateLimit(1000)).toBe(1000)
    })

    it('should return undefined for null/undefined', () => {
      expect(validateLimit(null)).toBeUndefined()
      expect(validateLimit(undefined)).toBeUndefined()
    })

    it('should reject non-number values', () => {
      expect(() => validateLimit('10')).toThrow(ValidationError)
      expect(() => validateLimit({})).toThrow(ValidationError)
    })

    it('should reject non-integer values', () => {
      expect(() => validateLimit(10.5)).toThrow(ValidationError)
      expect(() => validateLimit(10.5)).toThrow('must be an integer')
    })

    it('should reject negative values', () => {
      expect(() => validateLimit(-1)).toThrow(ValidationError)
      expect(() => validateLimit(-1)).toThrow('must be non-negative')
    })

    it('should reject values exceeding maximum', () => {
      expect(() => validateLimit(1001)).toThrow(ValidationError)
      expect(() => validateLimit(1001)).toThrow('cannot exceed 1000')
    })
  })

  describe('validateSkip', () => {
    it('should accept valid skip values', () => {
      expect(validateSkip(0)).toBe(0)
      expect(validateSkip(100)).toBe(100)
      expect(validateSkip(10000)).toBe(10000)
    })

    it('should return undefined for null/undefined', () => {
      expect(validateSkip(null)).toBeUndefined()
      expect(validateSkip(undefined)).toBeUndefined()
    })

    it('should reject non-number values', () => {
      expect(() => validateSkip('10')).toThrow(ValidationError)
    })

    it('should reject non-integer values', () => {
      expect(() => validateSkip(10.5)).toThrow(ValidationError)
    })

    it('should reject negative values', () => {
      expect(() => validateSkip(-1)).toThrow(ValidationError)
    })
  })

  describe('validateSort', () => {
    it('should accept valid sort specifications', () => {
      expect(validateSort({ name: 1 })).toEqual({ name: 1 })
      expect(validateSort({ name: -1 })).toEqual({ name: -1 })
      expect(validateSort({ name: 1, date: -1 })).toEqual({ name: 1, date: -1 })
    })

    it('should return undefined for null/undefined', () => {
      expect(validateSort(null)).toBeUndefined()
      expect(validateSort(undefined)).toBeUndefined()
    })

    it('should reject non-object values', () => {
      expect(() => validateSort('name')).toThrow(ValidationError)
      expect(() => validateSort(123)).toThrow(ValidationError)
    })

    it('should reject array values', () => {
      expect(() => validateSort(['name', 1])).toThrow(ValidationError)
    })

    it('should reject invalid sort directions', () => {
      expect(() => validateSort({ name: 0 })).toThrow(ValidationError)
      expect(() => validateSort({ name: 2 })).toThrow(ValidationError)
      expect(() => validateSort({ name: 'asc' })).toThrow(ValidationError)
      expect(() => validateSort({ name: 0 })).toThrow('must be 1 (ascending) or -1 (descending)')
    })
  })

  describe('validateProject', () => {
    it('should accept valid projection specifications', () => {
      expect(validateProject({ name: 1 })).toEqual({ name: 1 })
      expect(validateProject({ password: 0 })).toEqual({ password: 0 })
      expect(validateProject({ name: 1, email: 1 })).toEqual({ name: 1, email: 1 })
    })

    it('should return undefined for null/undefined', () => {
      expect(validateProject(null)).toBeUndefined()
      expect(validateProject(undefined)).toBeUndefined()
    })

    it('should reject non-object values', () => {
      expect(() => validateProject('name')).toThrow(ValidationError)
    })

    it('should reject invalid projection values', () => {
      expect(() => validateProject({ name: 2 })).toThrow(ValidationError)
      expect(() => validateProject({ name: -1 })).toThrow(ValidationError)
      expect(() => validateProject({ name: true })).toThrow(ValidationError)
      expect(() => validateProject({ name: 2 })).toThrow('must be 0 (exclude) or 1 (include)')
    })
  })

  describe('validateBoolean', () => {
    it('should accept boolean values', () => {
      expect(validateBoolean(true, 'test')).toBe(true)
      expect(validateBoolean(false, 'test')).toBe(false)
    })

    it('should return default for null/undefined', () => {
      expect(validateBoolean(null, 'test')).toBe(false)
      expect(validateBoolean(undefined, 'test')).toBe(false)
      expect(validateBoolean(null, 'test', true)).toBe(true)
      expect(validateBoolean(undefined, 'test', false)).toBe(false)
    })

    it('should reject non-boolean values', () => {
      expect(() => validateBoolean('true', 'test')).toThrow(ValidationError)
      expect(() => validateBoolean(1, 'test')).toThrow(ValidationError)
      expect(() => validateBoolean({}, 'test')).toThrow(ValidationError)
    })
  })

  describe('validateSearchQuery', () => {
    it('should accept valid search queries', () => {
      expect(validateSearchQuery('hello world')).toBe('hello world')
      expect(validateSearchQuery('  trimmed  ')).toBe('trimmed')
    })

    it('should reject null or undefined', () => {
      expect(() => validateSearchQuery(null)).toThrow(ValidationError)
      expect(() => validateSearchQuery(undefined)).toThrow(ValidationError)
    })

    it('should reject non-string values', () => {
      expect(() => validateSearchQuery(123)).toThrow(ValidationError)
      expect(() => validateSearchQuery({})).toThrow(ValidationError)
    })

    it('should reject empty strings', () => {
      expect(() => validateSearchQuery('')).toThrow(ValidationError)
      expect(() => validateSearchQuery('   ')).toThrow(ValidationError)
    })

    it('should reject overly long queries', () => {
      const longQuery = 'a'.repeat(1001)
      expect(() => validateSearchQuery(longQuery)).toThrow(ValidationError)
      expect(() => validateSearchQuery(longQuery)).toThrow('1000 characters or fewer')
    })
  })

  describe('ValidationError', () => {
    it('should include field name in error', () => {
      const error = new ValidationError('collection', 'is required')
      expect(error.field).toBe('collection')
      expect(error.message).toBe('Invalid collection: is required')
      expect(error.name).toBe('ValidationError')
    })

    it('should be an instance of Error', () => {
      const error = new ValidationError('test', 'message')
      expect(error).toBeInstanceOf(Error)
      expect(error).toBeInstanceOf(ValidationError)
    })
  })
})

/**
 * MCP Tool Validation Integration Tests
 *
 * Tests that verify validation is properly applied when tools are invoked.
 */
describe('MCP Tool Validation Integration', () => {
  let db: ReturnType<typeof DB>
  let server: McpServer
  let client: Client

  beforeEach(async () => {
    db = DB({
      Posts: {
        title: 'string!',
        content: 'text',
        status: 'string',
        views: 'int',
      },
    })

    await db.collection('posts').create({
      $type: 'Post',
      name: 'Test Post',
      title: 'Hello World',
      content: 'Content',
      status: 'published',
      views: 100,
    })

    server = createParqueDBMCPServer(db)
    client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

    const [t1, t2] = InMemoryTransport.createLinkedPair()
    await server.connect(t2)
    await client.connect(t1)
  })

  afterEach(async () => {
    await client.close()
    await server.close()
  })

  describe('Collection Name Validation', () => {
    it('should reject invalid collection names in find', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: '123invalid',
        },
      })

      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toContain('collection')
    })

    it('should reject path traversal in collection names', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: '../secret',
        },
      })

      expect(result.isError).toBe(true)
    })
  })

  describe('Entity ID Validation', () => {
    it('should reject path traversal in entity IDs', async () => {
      const result = await client.callTool({
        name: 'parquedb_get',
        arguments: {
          collection: 'posts',
          id: '../../../etc/passwd',
        },
      })

      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toContain('path traversal')
    })

    it('should reject IDs with special characters', async () => {
      const result = await client.callTool({
        name: 'parquedb_get',
        arguments: {
          collection: 'posts',
          id: 'id@with.special',
        },
      })

      expect(result.isError).toBe(true)
    })
  })

  describe('Filter Validation', () => {
    it('should reject unknown filter operators', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          filter: { status: { $badOperator: 'value' } },
        },
      })

      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toContain('unknown operator')
    })

    it('should sanitize prototype pollution attempts in filters', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          filter: {
            status: 'published',
            '__proto__': { isAdmin: true },
          },
        },
      })

      // Should succeed but with the dangerous key removed
      expect(result.isError).toBeFalsy()
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
    })
  })

  describe('Update Validation', () => {
    it('should reject unknown update operators', async () => {
      const findResult = await client.callTool({
        name: 'parquedb_find',
        arguments: { collection: 'posts' },
      })
      const findParsed = parseToolResult(findResult.content as Array<{ type: string; text?: string }>)
      const postId = ((findParsed.data as Array<{ $id: string }>)[0]?.$id || '').split('/')[1]

      const result = await client.callTool({
        name: 'parquedb_update',
        arguments: {
          collection: 'posts',
          id: postId,
          update: { $badOperator: { title: 'new' } },
        },
      })

      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toContain('unknown operator')
    })

    it('should reject updates without operators', async () => {
      const findResult = await client.callTool({
        name: 'parquedb_find',
        arguments: { collection: 'posts' },
      })
      const findParsed = parseToolResult(findResult.content as Array<{ type: string; text?: string }>)
      const postId = ((findParsed.data as Array<{ $id: string }>)[0]?.$id || '').split('/')[1]

      const result = await client.callTool({
        name: 'parquedb_update',
        arguments: {
          collection: 'posts',
          id: postId,
          update: { title: 'new' },
        },
      })

      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toContain('must use operators')
    })
  })

  describe('Create Validation', () => {
    it('should reject create without name field', async () => {
      const result = await client.callTool({
        name: 'parquedb_create',
        arguments: {
          collection: 'posts',
          data: {
            title: 'No Name',
            content: 'Content',
          },
        },
      })

      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toContain('name')
    })

    it('should sanitize prototype pollution in create data', async () => {
      const result = await client.callTool({
        name: 'parquedb_create',
        arguments: {
          collection: 'posts',
          data: {
            name: 'Test Post',
            title: 'Title',
            '__proto__': { isAdmin: true },
          },
        },
      })

      // Should succeed with dangerous key removed
      expect(result.isError).toBeFalsy()
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
    })
  })

  describe('Aggregate Pipeline Validation', () => {
    it('should reject unknown pipeline stages', async () => {
      const result = await client.callTool({
        name: 'parquedb_aggregate',
        arguments: {
          collection: 'posts',
          pipeline: [{ $unknownStage: {} }],
        },
      })

      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toContain('unknown or unsupported stage')
    })

    it('should reject invalid filter operators in $match', async () => {
      const result = await client.callTool({
        name: 'parquedb_aggregate',
        arguments: {
          collection: 'posts',
          pipeline: [{ $match: { status: { $badOp: 'value' } } }],
        },
      })

      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toContain('unknown operator')
    })

    it('should reject empty pipeline', async () => {
      const result = await client.callTool({
        name: 'parquedb_aggregate',
        arguments: {
          collection: 'posts',
          pipeline: [],
        },
      })

      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toContain('cannot be empty')
    })
  })

  describe('Limit and Skip Validation', () => {
    it('should reject negative limit', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          limit: -5,
        },
      })

      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toContain('non-negative')
    })

    it('should reject limit exceeding maximum', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          limit: 2000,
        },
      })

      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toContain('cannot exceed 1000')
    })
  })

  describe('Sort and Project Validation', () => {
    it('should reject invalid sort direction', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          sort: { views: 0 },
        },
      })

      // The MCP SDK validates sort values at the schema level (Zod)
      // so the error might be a raw error string or JSON
      expect(result.isError).toBe(true)

      const content = result.content[0] as { type: string; text: string }
      expect(content.type).toBe('text')
      // Either Zod schema validation or our custom validation will catch this
      expect(content.text).toBeDefined()
    })

    it('should reject invalid projection value', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'posts',
          project: { title: 2 },
        },
      })

      // The MCP SDK validates projection values at the schema level (Zod)
      // so the error might be a raw error string or JSON
      expect(result.isError).toBe(true)

      const content = result.content[0] as { type: string; text: string }
      expect(content.type).toBe('text')
      // Either Zod schema validation or our custom validation will catch this
      expect(content.text).toBeDefined()
    })
  })

  describe('Semantic Search Validation', () => {
    it('should reject empty search query', async () => {
      const result = await client.callTool({
        name: 'parquedb_semantic_search',
        arguments: {
          collection: 'posts',
          query: '   ',
        },
      })

      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
      expect(parsed.error).toContain('cannot be empty')
    })
  })
})
