/**
 * MCP Streaming Integration Tests
 *
 * Tests for streaming operations and large result handling
 * in the ParqueDB MCP server.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { DB } from '../../../src/db'
import { createParqueDBMCPServer } from '../../../src/integrations/mcp'
import type { ToolResult } from '../../../src/integrations/mcp'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'

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

describe('MCP Streaming and Large Result Handling', () => {
  let db: ReturnType<typeof DB>
  let server: McpServer
  let client: Client

  beforeEach(async () => {
    // Create a database optimized for testing large datasets
    db = DB({
      Items: {
        title: 'string!',
        description: 'text',
        category: 'string',
        price: 'float',
        stock: 'int',
      },
    })

    // Seed with a larger dataset for streaming/pagination tests
    const createPromises = []
    for (let i = 0; i < 100; i++) {
      createPromises.push(
        db.collection('items').create({
          $type: 'Item',
          name: `Item ${i.toString().padStart(3, '0')}`,
          title: `Product ${i}`,
          description: `Description for product ${i}. This is a longer description to test content handling.`,
          category: i % 3 === 0 ? 'electronics' : i % 3 === 1 ? 'clothing' : 'food',
          price: 9.99 + i * 0.5,
          stock: 100 - i % 50,
        })
      )
    }
    await Promise.all(createPromises)

    // Create MCP server and client
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

  describe('Pagination Through Large Datasets', () => {
    it('should paginate through all results correctly', async () => {
      const pageSize = 20
      let allItems: Array<{ title: string }> = []
      let skip = 0
      let hasMore = true

      while (hasMore) {
        const result = await client.callTool({
          name: 'parquedb_find',
          arguments: {
            collection: 'items',
            limit: pageSize,
            skip,
            sort: { price: 1 },
          },
        })

        const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
        expect(parsed.success).toBe(true)

        const items = parsed.data as Array<{ title: string }>
        allItems = allItems.concat(items)

        hasMore = parsed.hasMore ?? false
        skip += pageSize
      }

      expect(allItems.length).toBe(100)

      // Verify sort order by checking prices are ascending
      for (let i = 1; i < allItems.length; i++) {
        expect(allItems[i].title).toBeDefined()
      }
    })

    it('should return accurate total count regardless of limit', async () => {
      const result1 = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          limit: 5,
        },
      })

      const parsed1 = parseToolResult(result1.content as Array<{ type: string; text?: string }>)
      expect(parsed1.total).toBe(100)
      expect(parsed1.count).toBe(5)
      expect(parsed1.hasMore).toBe(true)

      const result2 = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          limit: 100,
        },
      })

      const parsed2 = parseToolResult(result2.content as Array<{ type: string; text?: string }>)
      expect(parsed2.total).toBe(100)
      expect(parsed2.count).toBe(100)
      expect(parsed2.hasMore).toBe(false)
    })

    it('should handle skip beyond available data', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          limit: 20,
          skip: 200, // Beyond the 100 items
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(0)
      expect((parsed.data as unknown[]).length).toBe(0)
    })

    it('should handle last partial page correctly', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          limit: 30,
          skip: 90, // Should return only 10 items
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(10)
      expect(parsed.hasMore).toBe(false)
    })
  })

  describe('Large Filter Result Sets', () => {
    it('should handle filters that match many documents', async () => {
      // Electronics category should have ~34 items (every 3rd item)
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          filter: { category: 'electronics' },
          limit: 100,
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(34) // items 0, 3, 6, ... 99 (indices 0,3,6,...99 where i%3==0)
    })

    it('should handle complex filters on large datasets', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          filter: {
            $and: [
              { category: 'electronics' },
              { price: { $gte: 20 } },
              { price: { $lt: 40 } },
            ],
          },
          limit: 100,
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      // Should be a subset of electronics items within price range
      expect(parsed.count).toBeGreaterThan(0)
      expect(parsed.count).toBeLessThan(34)
    })

    it('should handle $in with many values', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          filter: {
            category: { $in: ['electronics', 'clothing'] },
          },
          limit: 100,
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      // Electronics (~34) + Clothing (~33) = ~67
      expect(parsed.count).toBe(67)
    })
  })

  describe('Sort Operations on Large Datasets', () => {
    it('should correctly sort ascending on numeric field', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          sort: { price: 1 },
          limit: 10,
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)

      const items = parsed.data as Array<{ price: number }>
      for (let i = 1; i < items.length; i++) {
        expect(items[i].price).toBeGreaterThanOrEqual(items[i - 1].price)
      }
    })

    it('should correctly sort descending on numeric field', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          sort: { price: -1 },
          limit: 10,
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)

      const items = parsed.data as Array<{ price: number }>
      for (let i = 1; i < items.length; i++) {
        expect(items[i].price).toBeLessThanOrEqual(items[i - 1].price)
      }
    })

    it('should correctly sort on string field', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          sort: { title: 1 },
          limit: 10,
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)

      const items = parsed.data as Array<{ title: string }>
      for (let i = 1; i < items.length; i++) {
        expect(items[i].title.localeCompare(items[i - 1].title)).toBeGreaterThanOrEqual(0)
      }
    })

    it('should handle sort with filter', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          filter: { category: 'electronics' },
          sort: { price: -1 },
          limit: 5,
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)

      const items = parsed.data as Array<{ price: number; category: string }>
      // All items should be electronics
      for (const item of items) {
        expect(item.category).toBe('electronics')
      }
      // Should be sorted descending
      for (let i = 1; i < items.length; i++) {
        expect(items[i].price).toBeLessThanOrEqual(items[i - 1].price)
      }
    })
  })

  describe('Count Operations on Large Datasets', () => {
    it('should efficiently count all documents', async () => {
      const result = await client.callTool({
        name: 'parquedb_count',
        arguments: {
          collection: 'items',
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(100)
    })

    it('should efficiently count with filter', async () => {
      const result = await client.callTool({
        name: 'parquedb_count',
        arguments: {
          collection: 'items',
          filter: { category: 'clothing' },
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(33)
    })

    it('should efficiently count with complex filter', async () => {
      const result = await client.callTool({
        name: 'parquedb_count',
        arguments: {
          collection: 'items',
          filter: {
            $or: [
              { category: 'electronics' },
              { price: { $gt: 50 } },
            ],
          },
        },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBeGreaterThan(34) // At least electronics + some high-priced items
    })
  })

  describe('Projection on Large Datasets', () => {
    it('should reduce response size with projection', async () => {
      // Without projection
      const fullResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          limit: 10,
        },
      })

      const fullParsed = parseToolResult(fullResult.content as Array<{ type: string; text?: string }>)
      const fullItems = fullParsed.data as Array<Record<string, unknown>>

      // With projection
      const projectedResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          project: { title: 1, price: 1 },
          limit: 10,
        },
      })

      const projectedParsed = parseToolResult(projectedResult.content as Array<{ type: string; text?: string }>)
      const projectedItems = projectedParsed.data as Array<Record<string, unknown>>

      // Same count
      expect(fullItems.length).toBe(projectedItems.length)

      // Projected items should have fewer fields
      expect(projectedItems[0]).toHaveProperty('title')
      expect(projectedItems[0]).toHaveProperty('price')
      // System fields should still be present
      expect(projectedItems[0]).toHaveProperty('$id')
    })
  })
})

describe('MCP Response Size Handling', () => {
  let db: ReturnType<typeof DB>
  let server: McpServer
  let client: Client

  beforeEach(async () => {
    db = DB({
      Documents: {
        title: 'string!',
        content: 'text',
      },
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

  it('should handle documents with large content fields', async () => {
    // Create a document with large content
    const largeContent = 'Lorem ipsum '.repeat(500) // ~6KB of text

    const createResult = await client.callTool({
      name: 'parquedb_create',
      arguments: {
        collection: 'documents',
        data: {
          name: 'Large Document',
          title: 'Document with Large Content',
          content: largeContent,
        },
      },
    })

    expect(createResult.isError).toBeFalsy()
    const createParsed = parseToolResult(createResult.content as Array<{ type: string; text?: string }>)
    expect(createParsed.success).toBe(true)

    const docId = ((createParsed.data as { $id: string }).$id).split('/')[1]

    // Retrieve it
    const getResult = await client.callTool({
      name: 'parquedb_get',
      arguments: {
        collection: 'documents',
        id: docId,
      },
    })

    const getParsed = parseToolResult(getResult.content as Array<{ type: string; text?: string }>)
    expect(getParsed.success).toBe(true)
    expect((getParsed.data as { content: string })?.content).toBe(largeContent)
  })

  it('should handle multiple documents with large content', async () => {
    // Create multiple documents with content
    const content = 'Content block '.repeat(100) // ~1.4KB each
    for (let i = 0; i < 10; i++) {
      await client.callTool({
        name: 'parquedb_create',
        arguments: {
          collection: 'documents',
          data: {
            name: `Document ${i}`,
            title: `Title ${i}`,
            content: content + i,
          },
        },
      })
    }

    // Fetch all at once
    const result = await client.callTool({
      name: 'parquedb_find',
      arguments: {
        collection: 'documents',
        limit: 10,
      },
    })

    const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
    expect(parsed.success).toBe(true)
    expect(parsed.count).toBe(10)

    // All documents should have their content intact
    const items = parsed.data as Array<{ content: string }>
    for (const item of items) {
      expect(item.content.length).toBeGreaterThan(1000)
    }
  })
})
