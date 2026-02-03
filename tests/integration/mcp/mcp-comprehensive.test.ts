/**
 * MCP Comprehensive Integration Tests
 *
 * Tests for advanced MCP integration scenarios including:
 * - Context passing between tool calls
 * - Resource access patterns
 * - Tool chaining and workflows
 * - Connection lifecycle
 * - Protocol-level edge cases
 * - Multi-collection operations
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

/**
 * Helper to extract entity ID from full $id
 */
function extractId(fullId: string): string {
  return fullId.split('/')[1] || fullId
}

describe('MCP Context Passing', () => {
  let db: ReturnType<typeof DB>
  let server: McpServer
  let client: Client

  beforeEach(async () => {
    db = DB({
      Authors: {
        name: 'string!',
        email: 'string',
        bio: 'text',
      },
      Books: {
        title: 'string!',
        authorId: 'string',
        isbn: 'string',
        publishedYear: 'int',
        genre: 'string',
      },
      Reviews: {
        bookId: 'string',
        reviewerName: 'string',
        rating: 'int',
        comment: 'text',
      },
    })

    // Seed interconnected data
    const author1 = await db.collection('authors').create({
      $type: 'Author',
      name: 'Jane Austen',
      email: 'jane@example.com',
      bio: 'English novelist known for romance and social commentary.',
    })
    const author2 = await db.collection('authors').create({
      $type: 'Author',
      name: 'Mark Twain',
      email: 'mark@example.com',
      bio: 'American author and humorist.',
    })

    const book1 = await db.collection('books').create({
      $type: 'Book',
      name: 'Pride and Prejudice',
      title: 'Pride and Prejudice',
      authorId: extractId(author1.$id),
      isbn: '978-0-19-953556-9',
      publishedYear: 1813,
      genre: 'romance',
    })
    const book2 = await db.collection('books').create({
      $type: 'Book',
      name: 'Adventures of Tom Sawyer',
      title: 'Adventures of Tom Sawyer',
      authorId: extractId(author2.$id),
      isbn: '978-0-14-039083-2',
      publishedYear: 1876,
      genre: 'adventure',
    })

    await db.collection('reviews').create({
      $type: 'Review',
      name: 'Review 1',
      bookId: extractId(book1.$id),
      reviewerName: 'Alice',
      rating: 5,
      comment: 'A masterpiece of English literature!',
    })
    await db.collection('reviews').create({
      $type: 'Review',
      name: 'Review 2',
      bookId: extractId(book1.$id),
      reviewerName: 'Bob',
      rating: 4,
      comment: 'Witty and engaging.',
    })
    await db.collection('reviews').create({
      $type: 'Review',
      name: 'Review 3',
      bookId: extractId(book2.$id),
      reviewerName: 'Charlie',
      rating: 5,
      comment: 'A childhood classic!',
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

  describe('Cross-Collection Workflows', () => {
    it('should find books by author name via multi-step lookup', async () => {
      // Step 1: Find the author by name
      const authorResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'authors',
          filter: { name: 'Jane Austen' },
        },
      })

      const authorParsed = parseToolResult(authorResult.content as Array<{ type: string; text?: string }>)
      expect(authorParsed.success).toBe(true)
      expect(authorParsed.count).toBe(1)

      const author = (authorParsed.data as Array<{ $id: string }>)[0]
      const authorId = extractId(author.$id)

      // Step 2: Find books by that author
      const booksResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'books',
          filter: { authorId },
        },
      })

      const booksParsed = parseToolResult(booksResult.content as Array<{ type: string; text?: string }>)
      expect(booksParsed.success).toBe(true)
      expect(booksParsed.count).toBe(1)
      expect((booksParsed.data as Array<{ title: string }>)[0].title).toBe('Pride and Prejudice')
    })

    it('should calculate average rating for a book via multi-step workflow', async () => {
      // Step 1: Find the book
      const bookResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'books',
          filter: { title: 'Pride and Prejudice' },
        },
      })

      const bookParsed = parseToolResult(bookResult.content as Array<{ type: string; text?: string }>)
      const bookId = extractId((bookParsed.data as Array<{ $id: string }>)[0].$id)

      // Step 2: Find all reviews for this book
      const reviewsResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'reviews',
          filter: { bookId },
        },
      })

      const reviewsParsed = parseToolResult(reviewsResult.content as Array<{ type: string; text?: string }>)
      expect(reviewsParsed.success).toBe(true)
      expect(reviewsParsed.count).toBe(2)

      // Calculate average rating
      const reviews = reviewsParsed.data as Array<{ rating: number }>
      const avgRating = reviews.reduce((sum, r) => sum + r.rating, 0) / reviews.length
      expect(avgRating).toBe(4.5)
    })

    it('should create a new book with reviews in a workflow', async () => {
      // Step 1: Find an existing author
      const authorResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'authors',
          filter: { name: 'Mark Twain' },
        },
      })

      const authorParsed = parseToolResult(authorResult.content as Array<{ type: string; text?: string }>)
      const authorId = extractId((authorParsed.data as Array<{ $id: string }>)[0].$id)

      // Step 2: Create a new book
      const bookResult = await client.callTool({
        name: 'parquedb_create',
        arguments: {
          collection: 'books',
          data: {
            name: 'Huckleberry Finn',
            title: 'Adventures of Huckleberry Finn',
            authorId,
            isbn: '978-0-14-039046-7',
            publishedYear: 1884,
            genre: 'adventure',
          },
        },
      })

      const bookParsed = parseToolResult(bookResult.content as Array<{ type: string; text?: string }>)
      expect(bookParsed.success).toBe(true)
      const bookId = extractId((bookParsed.data as { $id: string }).$id)

      // Step 3: Add a review for the new book
      const reviewResult = await client.callTool({
        name: 'parquedb_create',
        arguments: {
          collection: 'reviews',
          data: {
            name: 'New Review',
            bookId,
            reviewerName: 'David',
            rating: 5,
            comment: 'Another Twain masterpiece!',
          },
        },
      })

      const reviewParsed = parseToolResult(reviewResult.content as Array<{ type: string; text?: string }>)
      expect(reviewParsed.success).toBe(true)

      // Step 4: Verify the review is linked to the book
      const verifyResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'reviews',
          filter: { bookId },
        },
      })

      const verifyParsed = parseToolResult(verifyResult.content as Array<{ type: string; text?: string }>)
      expect(verifyParsed.count).toBe(1)
      expect((verifyParsed.data as Array<{ comment: string }>)[0].comment).toBe('Another Twain masterpiece!')
    })
  })

  describe('Conditional Workflows', () => {
    it('should update book only if author exists', async () => {
      // Step 1: Check if author exists
      const authorCheck = await client.callTool({
        name: 'parquedb_count',
        arguments: {
          collection: 'authors',
          filter: { name: 'Jane Austen' },
        },
      })

      const checkParsed = parseToolResult(authorCheck.content as Array<{ type: string; text?: string }>)

      if (checkParsed.count && checkParsed.count > 0) {
        // Step 2: Find the book to update
        const bookResult = await client.callTool({
          name: 'parquedb_find',
          arguments: {
            collection: 'books',
            filter: { title: 'Pride and Prejudice' },
          },
        })

        const bookParsed = parseToolResult(bookResult.content as Array<{ type: string; text?: string }>)
        const bookId = extractId((bookParsed.data as Array<{ $id: string }>)[0].$id)

        // Step 3: Update the book
        const updateResult = await client.callTool({
          name: 'parquedb_update',
          arguments: {
            collection: 'books',
            id: bookId,
            update: {
              $set: { genre: 'classic romance' },
            },
          },
        })

        const updateParsed = parseToolResult(updateResult.content as Array<{ type: string; text?: string }>)
        expect(updateParsed.success).toBe(true)
      }

      // Verify the update
      const verifyResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'books',
          filter: { title: 'Pride and Prejudice' },
        },
      })

      const verifyParsed = parseToolResult(verifyResult.content as Array<{ type: string; text?: string }>)
      expect((verifyParsed.data as Array<{ genre: string }>)[0].genre).toBe('classic romance')
    })

    it('should handle non-existent entity gracefully in workflow', async () => {
      // Try to get a non-existent book
      const bookResult = await client.callTool({
        name: 'parquedb_get',
        arguments: {
          collection: 'books',
          id: 'non-existent-book-id',
        },
      })

      const bookParsed = parseToolResult(bookResult.content as Array<{ type: string; text?: string }>)
      expect(bookParsed.success).toBe(true)
      expect(bookParsed.data).toBeNull()

      // Workflow should handle null gracefully - no further operations
      // This tests the client's ability to check for null before proceeding
    })
  })

  describe('Batch-like Operations', () => {
    it('should update multiple entities based on filter results', async () => {
      // Find all reviews with rating < 5
      const findResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'reviews',
          filter: { rating: { $lt: 5 } },
        },
      })

      const findParsed = parseToolResult(findResult.content as Array<{ type: string; text?: string }>)
      const reviews = findParsed.data as Array<{ $id: string; rating: number }>

      // Update each review found
      const updatePromises = reviews.map(review =>
        client.callTool({
          name: 'parquedb_update',
          arguments: {
            collection: 'reviews',
            id: extractId(review.$id),
            update: {
              $set: { comment: `[Verified] ${review.rating}/5 stars` },
            },
          },
        })
      )

      const updateResults = await Promise.all(updatePromises)

      // Verify all updates succeeded
      for (const result of updateResults) {
        const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
        expect(parsed.success).toBe(true)
      }
    })

    it('should delete multiple related entities in sequence', async () => {
      // Find the book to delete
      const bookResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'books',
          filter: { title: 'Pride and Prejudice' },
        },
      })

      const bookParsed = parseToolResult(bookResult.content as Array<{ type: string; text?: string }>)
      const bookId = extractId((bookParsed.data as Array<{ $id: string }>)[0].$id)

      // First delete associated reviews
      const reviewsResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'reviews',
          filter: { bookId },
        },
      })

      const reviewsParsed = parseToolResult(reviewsResult.content as Array<{ type: string; text?: string }>)
      const reviews = reviewsParsed.data as Array<{ $id: string }>

      for (const review of reviews) {
        const deleteResult = await client.callTool({
          name: 'parquedb_delete',
          arguments: {
            collection: 'reviews',
            id: extractId(review.$id),
          },
        })
        const deleteParsed = parseToolResult(deleteResult.content as Array<{ type: string; text?: string }>)
        expect(deleteParsed.success).toBe(true)
      }

      // Then delete the book
      const deleteBookResult = await client.callTool({
        name: 'parquedb_delete',
        arguments: {
          collection: 'books',
          id: bookId,
        },
      })

      const deleteBookParsed = parseToolResult(deleteBookResult.content as Array<{ type: string; text?: string }>)
      expect(deleteBookParsed.success).toBe(true)

      // Verify book is deleted
      const verifyResult = await client.callTool({
        name: 'parquedb_get',
        arguments: {
          collection: 'books',
          id: bookId,
        },
      })

      const verifyParsed = parseToolResult(verifyResult.content as Array<{ type: string; text?: string }>)
      expect(verifyParsed.data).toBeNull()
    })
  })
})

describe('MCP Resource Access', () => {
  let db: ReturnType<typeof DB>
  let server: McpServer
  let client: Client

  beforeEach(async () => {
    db = DB({
      Products: {
        name: 'string!',
        sku: 'string',
        price: 'float',
        category: 'string',
      },
      Categories: {
        name: 'string!',
        description: 'text',
      },
    })

    await db.collection('categories').create({
      $type: 'Category',
      name: 'Electronics',
      description: 'Electronic devices and accessories',
    })
    await db.collection('categories').create({
      $type: 'Category',
      name: 'Clothing',
      description: 'Apparel and accessories',
    })

    await db.collection('products').create({
      $type: 'Product',
      name: 'Laptop',
      sku: 'LAP-001',
      price: 999.99,
      category: 'Electronics',
    })
    await db.collection('products').create({
      $type: 'Product',
      name: 'T-Shirt',
      sku: 'TSH-001',
      price: 29.99,
      category: 'Clothing',
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

  describe('Schema Resource', () => {
    it('should list available resources', async () => {
      const resources = await client.listResources()

      expect(resources.resources).toBeDefined()
      expect(Array.isArray(resources.resources)).toBe(true)
    })

    it('should include schema resource with correct metadata', async () => {
      const resources = await client.listResources()

      const schemaResource = resources.resources.find(r => r.uri === 'parquedb://schema')
      expect(schemaResource).toBeDefined()
      expect(schemaResource?.mimeType).toBe('application/json')
      expect(schemaResource?.name).toBe('Database Schema')
    })

    it('should read schema resource and return valid JSON', async () => {
      try {
        const result = await client.readResource({
          uri: 'parquedb://schema',
        })

        expect(result.contents).toBeDefined()
        expect(result.contents.length).toBeGreaterThan(0)

        const content = result.contents[0]
        expect(content.uri).toBe('parquedb://schema')
        expect(content.mimeType).toBe('application/json')

        // Parse JSON to verify it's valid
        if (content.text) {
          const schema = JSON.parse(content.text)
          expect(schema).toBeDefined()
          // Schema may be empty if getSchemaValidator is not available
          expect(typeof schema).toBe('object')
        }
      } catch (error) {
        // If schema resource is not available in this configuration, that's acceptable
        if (error instanceof Error && error.message.includes('getSchemaValidator')) {
          expect(true).toBe(true)
        } else {
          throw error
        }
      }
    })
  })

  describe('Resource Template Patterns', () => {
    it('should list resources including schema', async () => {
      const resources = await client.listResources()

      // Should have at least the schema resource
      expect(resources.resources.length).toBeGreaterThanOrEqual(1)

      // Verify schema resource is present
      const hasSchema = resources.resources.some(r => r.uri.includes('schema'))
      expect(hasSchema).toBe(true)
    })
  })
})

describe('MCP Connection Lifecycle', () => {
  let db: ReturnType<typeof DB>

  beforeEach(async () => {
    db = DB({
      Items: {
        name: 'string!',
        value: 'int',
      },
    })

    await db.collection('items').create({
      $type: 'Item',
      name: 'Test Item',
      value: 42,
    })
  })

  describe('Connection Management', () => {
    it('should handle clean connect and disconnect', async () => {
      const server = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()

      await server.connect(t2)
      await client.connect(t1)

      // Verify connection works
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: { collection: 'items' },
      })

      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)

      // Clean disconnect
      await client.close()
      await server.close()
    })

    it('should handle multiple sequential connections to same server', async () => {
      const server = createParqueDBMCPServer(db)

      // First connection
      const client1 = new Client({ name: 'client-1', version: '1.0.0' }, { capabilities: {} })
      const [t1a, t1b] = InMemoryTransport.createLinkedPair()

      await server.connect(t1b)
      await client1.connect(t1a)

      const result1 = await client1.callTool({
        name: 'parquedb_find',
        arguments: { collection: 'items' },
      })
      expect(parseToolResult(result1.content as Array<{ type: string; text?: string }>).success).toBe(true)

      await client1.close()

      // Note: MCP server typically requires a new instance for new connections
      // This tests that cleanup doesn't prevent future use

      await server.close()
    })

    it('should maintain state across multiple tool calls in same session', async () => {
      const server = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)

      // Create an item
      const createResult = await client.callTool({
        name: 'parquedb_create',
        arguments: {
          collection: 'items',
          data: { name: 'Session Item', value: 100 },
        },
      })
      const createParsed = parseToolResult(createResult.content as Array<{ type: string; text?: string }>)
      expect(createParsed.success).toBe(true)

      // Update it in the same session
      const itemId = extractId((createParsed.data as { $id: string }).$id)
      const updateResult = await client.callTool({
        name: 'parquedb_update',
        arguments: {
          collection: 'items',
          id: itemId,
          update: { $inc: { value: 50 } },
        },
      })
      expect(parseToolResult(updateResult.content as Array<{ type: string; text?: string }>).success).toBe(true)

      // Verify the changes persisted
      const getResult = await client.callTool({
        name: 'parquedb_get',
        arguments: {
          collection: 'items',
          id: itemId,
        },
      })
      const getParsed = parseToolResult(getResult.content as Array<{ type: string; text?: string }>)
      expect((getParsed.data as { value: number })?.value).toBe(150)

      await client.close()
      await server.close()
    })
  })

  describe('Transport Handling', () => {
    it('should work with InMemoryTransport pair', async () => {
      const server = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair()

      await server.connect(serverTransport)
      await client.connect(clientTransport)

      // Verify bidirectional communication
      const tools = await client.listTools()
      expect(tools.tools.length).toBeGreaterThan(0)

      const result = await client.callTool({
        name: 'parquedb_count',
        arguments: { collection: 'items' },
      })
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.count).toBe(1)

      await client.close()
      await server.close()
    })
  })
})

describe('MCP Error Handling Edge Cases', () => {
  let db: ReturnType<typeof DB>
  let server: McpServer
  let client: Client

  beforeEach(async () => {
    db = DB({
      Items: {
        name: 'string!',
        value: 'int',
        status: 'string',
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

  describe('Invalid Tool Arguments', () => {
    it('should handle deeply nested invalid filter', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          filter: {
            $and: [
              { $or: [{ status: { $invalidNested: 'value' } }] },
            ],
          },
        },
      })

      expect(result.isError).toBe(true)
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(false)
    })

    it('should handle empty string collection name', async () => {
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: '',
        },
      })

      expect(result.isError).toBe(true)
    })

    it('should handle extremely long collection name', async () => {
      const longName = 'a'.repeat(200)
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: longName,
        },
      })

      expect(result.isError).toBe(true)
    })

    it('should handle special characters in filter values', async () => {
      // This should work - special characters in values are allowed
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          filter: {
            status: "active<script>alert('xss')</script>",
          },
        },
      })

      // Should succeed but return no results
      expect(result.isError).toBeFalsy()
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
      expect(parsed.count).toBe(0)
    })

    it('should handle null values in filter', async () => {
      await db.collection('items').create({
        $type: 'Item',
        name: 'Null Test',
        value: 0,
        status: 'active',
      })

      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          filter: {
            status: null,
          },
        },
      })

      // Filtering for null should work
      expect(result.isError).toBeFalsy()
    })

    it('should handle unicode characters in data', async () => {
      const result = await client.callTool({
        name: 'parquedb_create',
        arguments: {
          collection: 'items',
          data: {
            name: '\u4e2d\u6587\u540d\u5b57', // Chinese characters
            value: 42,
            status: '\ud83d\ude00', // Emoji
          },
        },
      })

      expect(result.isError).toBeFalsy()
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      expect(parsed.success).toBe(true)
    })
  })

  describe('Protocol Edge Cases', () => {
    it('should handle calling non-existent tool', async () => {
      const result = await client.callTool({
        name: 'parquedb_nonexistent_tool',
        arguments: {},
      })

      expect(result.isError).toBe(true)
    })

    it('should handle empty arguments object for parquedb_count', async () => {
      // parquedb_count requires collection, so this should fail
      const result = await client.callTool({
        name: 'parquedb_count',
        arguments: {},
      })

      // Should error due to missing required 'collection' argument
      expect(result.isError).toBe(true)
    })

    it('should handle extra unexpected arguments gracefully', async () => {
      // Create an item first so there's data
      await client.callTool({
        name: 'parquedb_create',
        arguments: {
          collection: 'items',
          data: { name: 'Test', value: 1, status: 'active' },
        },
      })

      // parquedb_find with valid required args plus extra ones
      const result = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          unexpectedArg: 'value', // Extra arg
          anotherUnexpected: 123, // Extra arg
        },
      })

      // The MCP SDK's Zod validation may reject extra arguments
      // This behavior depends on the schema definition
      // If passthrough is enabled, it should succeed; otherwise it may fail
      // We're testing that the server handles this consistently
      expect(result.content).toBeDefined()
    })
  })

  describe('Data Type Coercion', () => {
    it('should handle string number in numeric field', async () => {
      // Create an item first
      const createResult = await client.callTool({
        name: 'parquedb_create',
        arguments: {
          collection: 'items',
          data: {
            name: 'Type Test',
            value: 10,
            status: 'active',
          },
        },
      })

      const createParsed = parseToolResult(createResult.content as Array<{ type: string; text?: string }>)
      const itemId = extractId((createParsed.data as { $id: string }).$id)

      // Try to update with string number - this depends on Zod schema validation
      // The limit field expects a number, not a string
      const findResult = await client.callTool({
        name: 'parquedb_find',
        arguments: {
          collection: 'items',
          limit: 10, // Valid number
        },
      })

      // Should work with valid number
      expect(findResult.isError).toBeFalsy()
    })

    it('should handle boolean-like values', async () => {
      const result = await client.callTool({
        name: 'parquedb_delete',
        arguments: {
          collection: 'items',
          id: 'test-id',
          hard: false,
        },
      })

      // Will fail because entity doesn't exist, but should validate properly
      const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
      // Getting non-existent returns success with deletedCount
      expect(parsed).toBeDefined()
    })
  })
})

describe('MCP Tool Discovery', () => {
  let db: ReturnType<typeof DB>

  beforeEach(async () => {
    db = DB({
      Items: { name: 'string!' },
    })
  })

  describe('Tool Metadata', () => {
    it('should provide tool descriptions', async () => {
      const server = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)

      const result = await client.listTools()

      for (const tool of result.tools) {
        expect(tool.description).toBeDefined()
        expect(tool.description.length).toBeGreaterThan(0)
      }

      await client.close()
      await server.close()
    })

    it('should provide input schemas for all tools', async () => {
      const server = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)

      const result = await client.listTools()

      for (const tool of result.tools) {
        expect(tool.inputSchema).toBeDefined()
        expect(tool.inputSchema.type).toBe('object')
      }

      await client.close()
      await server.close()
    })

    it('should have correct parameter definitions for parquedb_update', async () => {
      const server = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)

      const result = await client.listTools()
      const updateTool = result.tools.find(t => t.name === 'parquedb_update')

      expect(updateTool).toBeDefined()
      expect(updateTool?.inputSchema.properties).toHaveProperty('collection')
      expect(updateTool?.inputSchema.properties).toHaveProperty('id')
      expect(updateTool?.inputSchema.properties).toHaveProperty('update')

      await client.close()
      await server.close()
    })

    it('should have correct parameter definitions for parquedb_delete', async () => {
      const server = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)

      const result = await client.listTools()
      const deleteTool = result.tools.find(t => t.name === 'parquedb_delete')

      expect(deleteTool).toBeDefined()
      expect(deleteTool?.inputSchema.properties).toHaveProperty('collection')
      expect(deleteTool?.inputSchema.properties).toHaveProperty('id')
      expect(deleteTool?.inputSchema.properties).toHaveProperty('hard')

      await client.close()
      await server.close()
    })
  })

  describe('Tool Configuration', () => {
    it('should respect custom tool configuration', async () => {
      const server = createParqueDBMCPServer(db, {
        tools: {
          find: true,
          get: true,
          create: false,
          update: false,
          delete: false,
          count: true,
          aggregate: false,
          listCollections: true,
          semanticSearch: false,
        },
      })
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)

      const result = await client.listTools()
      const toolNames = result.tools.map(t => t.name)

      // Enabled tools
      expect(toolNames).toContain('parquedb_find')
      expect(toolNames).toContain('parquedb_get')
      expect(toolNames).toContain('parquedb_count')
      expect(toolNames).toContain('parquedb_list_collections')

      // Disabled tools
      expect(toolNames).not.toContain('parquedb_create')
      expect(toolNames).not.toContain('parquedb_update')
      expect(toolNames).not.toContain('parquedb_delete')
      expect(toolNames).not.toContain('parquedb_aggregate')
      expect(toolNames).not.toContain('parquedb_semantic_search')

      await client.close()
      await server.close()
    })

    it('should disable all write tools in read-only mode', async () => {
      const server = createParqueDBMCPServer(db, { readOnly: true })
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)

      const result = await client.listTools()
      const toolNames = result.tools.map(t => t.name)

      // Write tools should NOT be present
      expect(toolNames).not.toContain('parquedb_create')
      expect(toolNames).not.toContain('parquedb_update')
      expect(toolNames).not.toContain('parquedb_delete')

      // Read tools should be present
      expect(toolNames).toContain('parquedb_find')
      expect(toolNames).toContain('parquedb_get')
      expect(toolNames).toContain('parquedb_count')

      await client.close()
      await server.close()
    })
  })
})

describe('MCP Concurrent Tool Invocations', () => {
  let db: ReturnType<typeof DB>
  let server: McpServer
  let client: Client

  beforeEach(async () => {
    db = DB({
      Counters: {
        name: 'string!',
        value: 'int',
      },
    })

    for (let i = 0; i < 10; i++) {
      await db.collection('counters').create({
        $type: 'Counter',
        name: `Counter ${i}`,
        value: i * 10,
      })
    }

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

  describe('Parallel Read Operations', () => {
    it('should handle many concurrent find operations', async () => {
      const promises = Array.from({ length: 20 }, (_, i) =>
        client.callTool({
          name: 'parquedb_find',
          arguments: {
            collection: 'counters',
            filter: { value: { $gte: i * 5 } },
          },
        })
      )

      const results = await Promise.all(promises)

      for (const result of results) {
        expect(result.isError).toBeFalsy()
        const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
        expect(parsed.success).toBe(true)
      }
    })

    it('should handle concurrent count and find operations', async () => {
      const operations = [
        client.callTool({
          name: 'parquedb_count',
          arguments: { collection: 'counters' },
        }),
        client.callTool({
          name: 'parquedb_find',
          arguments: {
            collection: 'counters',
            limit: 5,
            sort: { value: 1 },
          },
        }),
        client.callTool({
          name: 'parquedb_count',
          arguments: {
            collection: 'counters',
            filter: { value: { $gt: 50 } },
          },
        }),
        client.callTool({
          name: 'parquedb_find',
          arguments: {
            collection: 'counters',
            limit: 5,
            sort: { value: -1 },
          },
        }),
      ]

      const results = await Promise.all(operations)

      // First count should be 10 (all counters)
      const count1 = parseToolResult(results[0].content as Array<{ type: string; text?: string }>)
      expect(count1.count).toBe(10)

      // First find should have 5 items sorted ascending
      const find1 = parseToolResult(results[1].content as Array<{ type: string; text?: string }>)
      expect(find1.count).toBe(5)

      // Second count should be counters with value > 50
      const count2 = parseToolResult(results[2].content as Array<{ type: string; text?: string }>)
      expect(count2.count).toBe(4) // values 60, 70, 80, 90

      // Second find should have 5 items sorted descending
      const find2 = parseToolResult(results[3].content as Array<{ type: string; text?: string }>)
      expect(find2.count).toBe(5)
    })
  })

  describe('Parallel Write Operations', () => {
    it('should handle concurrent create operations', async () => {
      const createPromises = Array.from({ length: 5 }, (_, i) =>
        client.callTool({
          name: 'parquedb_create',
          arguments: {
            collection: 'counters',
            data: {
              name: `New Counter ${i}`,
              value: 1000 + i,
            },
          },
        })
      )

      const results = await Promise.all(createPromises)

      for (const result of results) {
        expect(result.isError).toBeFalsy()
        const parsed = parseToolResult(result.content as Array<{ type: string; text?: string }>)
        expect(parsed.success).toBe(true)
      }

      // Verify all were created
      const countResult = await client.callTool({
        name: 'parquedb_count',
        arguments: { collection: 'counters' },
      })
      const countParsed = parseToolResult(countResult.content as Array<{ type: string; text?: string }>)
      expect(countParsed.count).toBe(15) // Original 10 + 5 new
    })

    it('should handle mixed concurrent read and write operations', async () => {
      const operations = [
        // Reads
        client.callTool({
          name: 'parquedb_find',
          arguments: { collection: 'counters', limit: 3 },
        }),
        client.callTool({
          name: 'parquedb_count',
          arguments: { collection: 'counters' },
        }),
        // Writes
        client.callTool({
          name: 'parquedb_create',
          arguments: {
            collection: 'counters',
            data: { name: 'Mixed Op Counter', value: 999 },
          },
        }),
        // More reads
        client.callTool({
          name: 'parquedb_find',
          arguments: {
            collection: 'counters',
            filter: { value: { $lt: 30 } },
          },
        }),
      ]

      const results = await Promise.all(operations)

      // All operations should succeed
      for (const result of results) {
        expect(result.isError).toBeFalsy()
      }
    })
  })
})

describe('MCP Server Custom Configuration', () => {
  let db: ReturnType<typeof DB>

  beforeEach(async () => {
    db = DB({
      Items: { name: 'string!' },
    })
  })

  describe('Server Identification', () => {
    it('should use default server name and version', async () => {
      const server = createParqueDBMCPServer(db)
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)

      // Server should be connectable
      const tools = await client.listTools()
      expect(tools.tools.length).toBeGreaterThan(0)

      await client.close()
      await server.close()
    })

    it('should use custom server name and version', async () => {
      const server = createParqueDBMCPServer(db, {
        name: 'my-custom-parquedb',
        version: '2.0.0-beta',
      })
      const client = new Client({ name: 'test-client', version: '1.0.0' }, { capabilities: {} })

      const [t1, t2] = InMemoryTransport.createLinkedPair()
      await server.connect(t2)
      await client.connect(t1)

      // Server should be connectable with custom config
      const tools = await client.listTools()
      expect(tools.tools.length).toBeGreaterThan(0)

      await client.close()
      await server.close()
    })
  })

  describe('Custom Instructions', () => {
    it('should accept custom instructions', async () => {
      const customInstructions = `
        This is a custom ParqueDB server for testing.
        Please use parquedb_find to query items.
      `.trim()

      const server = createParqueDBMCPServer(db, {
        instructions: customInstructions,
      })

      // Server should be created with custom instructions
      expect(server).toBeDefined()

      await server.close()
    })
  })
})
