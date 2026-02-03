/**
 * ParqueDB MCP Server
 *
 * Exposes ParqueDB operations to AI agents via the Model Context Protocol (MCP).
 *
 * @example
 * ```typescript
 * import { DB } from 'parquedb'
 * import { createParqueDBMCPServer } from 'parquedb/mcp'
 * import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
 *
 * const db = DB({
 *   Posts: { title: 'string!', content: 'text', status: 'string' },
 *   Users: { email: 'string!', name: 'string' }
 * })
 *
 * const mcpServer = createParqueDBMCPServer(db)
 * await mcpServer.connect(new StdioServerTransport())
 * ```
 */

import { McpServer, ResourceTemplate } from '@modelcontextprotocol/sdk/server/mcp.js'
import { z } from 'zod'
import type { ParqueDB } from '../../ParqueDB'
import type { PaginatedResult, Entity } from '../../types'
import type {
  ParqueDBMCPOptions,
  ToolResult,
  CollectionInfo,
} from './types'

// Default version from package.json context
const DEFAULT_VERSION = '0.1.0'

/**
 * Default instructions for AI agents using the ParqueDB MCP server
 */
const DEFAULT_INSTRUCTIONS = `
ParqueDB MCP Server provides access to a document database with the following capabilities:

## Available Tools

### Query Operations
- **parquedb_find**: Query documents from a collection with optional filters, sorting, and pagination
- **parquedb_get**: Retrieve a single document by ID
- **parquedb_count**: Count documents matching a filter
- **parquedb_aggregate**: Run aggregation pipelines

### Write Operations
- **parquedb_create**: Create a new document in a collection
- **parquedb_update**: Update an existing document using MongoDB-style operators
- **parquedb_delete**: Delete a document (soft delete by default)

### Utility Operations
- **parquedb_list_collections**: List all available collections

### Search Operations
- **parquedb_semantic_search**: Search documents using natural language (if vector indexes are available)

## Filter Syntax
Filters use MongoDB-style query operators:
- Comparison: $eq, $ne, $gt, $gte, $lt, $lte
- Array: $in, $nin, $all
- Logical: $and, $or, $nor, $not
- String: $regex, $startsWith, $endsWith, $contains
- Existence: $exists
- Type: $type

## Update Operators
Updates use MongoDB-style operators:
- $set: Set field values
- $unset: Remove fields
- $inc: Increment numeric values
- $push: Add to arrays
- $pull: Remove from arrays
- $addToSet: Add unique values to arrays

## Best Practices
1. Use filters to limit results and improve performance
2. Always specify a limit for find operations to avoid large result sets
3. Use projection to only request needed fields
4. Prefer soft delete (default) over hard delete for data safety
`.trim()

/**
 * Create a ParqueDB MCP server instance
 *
 * @param db - ParqueDB instance to expose via MCP
 * @param options - Server configuration options
 * @returns Configured McpServer instance
 */
export function createParqueDBMCPServer(
  db: ParqueDB,
  options: ParqueDBMCPOptions = {}
): McpServer {
  const {
    name = 'parquedb',
    version = DEFAULT_VERSION,
    instructions = DEFAULT_INSTRUCTIONS,
    tools = {},
    readOnly = false,
  } = options

  // Resolve tool availability
  const enabledTools = {
    find: tools.find ?? true,
    get: tools.get ?? true,
    create: readOnly ? false : (tools.create ?? true),
    update: readOnly ? false : (tools.update ?? true),
    delete: readOnly ? false : (tools.delete ?? true),
    count: tools.count ?? true,
    aggregate: tools.aggregate ?? true,
    listCollections: tools.listCollections ?? true,
    semanticSearch: tools.semanticSearch ?? true,
  }

  // Create the MCP server
  const server = new McpServer(
    { name, version },
    {
      instructions,
      capabilities: {
        tools: {},
        resources: {},
      },
    }
  )

  // Register tools based on configuration

  // parquedb_find - Query documents from a collection
  if (enabledTools.find) {
    server.registerTool(
      'parquedb_find',
      {
        description: 'Query documents from a collection with optional filters, sorting, and pagination',
        inputSchema: {
          collection: z.string().describe('Collection name to query'),
          filter: z.record(z.unknown()).optional().describe('MongoDB-style filter object'),
          limit: z.number().optional().default(20).describe('Maximum number of results (default: 20)'),
          skip: z.number().optional().describe('Number of results to skip'),
          sort: z.record(z.union([z.literal(1), z.literal(-1)])).optional().describe('Sort specification'),
          project: z.record(z.union([z.literal(0), z.literal(1)])).optional().describe('Field projection'),
        },
      },
      async (args) => {
        try {
          const collection = db.collection(args.collection)
          const result = await collection.find(args.filter, {
            limit: args.limit,
            skip: args.skip,
            sort: args.sort as Record<string, 1 | -1>,
            project: args.project as Record<string, 0 | 1>,
          }) as PaginatedResult<Entity>

          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: true,
                data: result.items,
                count: result.items.length,
                total: result.total,
                hasMore: result.hasMore,
                nextCursor: result.nextCursor,
              } satisfies ToolResult, null, 2),
            }],
          }
        } catch (error) {
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: false,
                error: error instanceof Error ? error.message : String(error),
              } satisfies ToolResult, null, 2),
            }],
            isError: true,
          }
        }
      }
    )
  }

  // parquedb_get - Get a single document by ID
  if (enabledTools.get) {
    server.registerTool(
      'parquedb_get',
      {
        description: 'Retrieve a single document by its ID',
        inputSchema: {
          collection: z.string().describe('Collection name'),
          id: z.string().describe('Entity ID (without namespace prefix)'),
          project: z.record(z.union([z.literal(0), z.literal(1)])).optional().describe('Field projection'),
        },
      },
      async (args) => {
        try {
          const collection = db.collection(args.collection)
          const entity = await collection.get(args.id, {
            project: args.project as Record<string, 0 | 1>,
          })

          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: true,
                data: entity,
              } satisfies ToolResult, null, 2),
            }],
          }
        } catch (error) {
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: false,
                error: error instanceof Error ? error.message : String(error),
              } satisfies ToolResult, null, 2),
            }],
            isError: true,
          }
        }
      }
    )
  }

  // parquedb_create - Create a new document
  if (enabledTools.create) {
    server.registerTool(
      'parquedb_create',
      {
        description: 'Create a new document in a collection',
        inputSchema: {
          collection: z.string().describe('Collection name'),
          data: z.record(z.unknown()).describe('Document data (must include name field)'),
        },
      },
      async (args) => {
        try {
          const collection = db.collection(args.collection)

          // Ensure $type is set based on collection if not provided
          const data = {
            ...args.data,
            $type: args.data.$type ?? capitalizeFirst(singularize(args.collection)),
          }

          const entity = await collection.create(data as Parameters<typeof collection.create>[0])

          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: true,
                data: entity,
              } satisfies ToolResult, null, 2),
            }],
          }
        } catch (error) {
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: false,
                error: error instanceof Error ? error.message : String(error),
              } satisfies ToolResult, null, 2),
            }],
            isError: true,
          }
        }
      }
    )
  }

  // parquedb_update - Update a document
  if (enabledTools.update) {
    server.registerTool(
      'parquedb_update',
      {
        description: 'Update an existing document using MongoDB-style update operators',
        inputSchema: {
          collection: z.string().describe('Collection name'),
          id: z.string().describe('Entity ID to update'),
          update: z.record(z.unknown()).describe('Update operations ($set, $unset, $inc, etc.)'),
        },
      },
      async (args) => {
        try {
          const collection = db.collection(args.collection)
          const result = await collection.update(args.id, args.update)

          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: true,
                data: result,
              } satisfies ToolResult, null, 2),
            }],
          }
        } catch (error) {
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: false,
                error: error instanceof Error ? error.message : String(error),
              } satisfies ToolResult, null, 2),
            }],
            isError: true,
          }
        }
      }
    )
  }

  // parquedb_delete - Delete a document
  if (enabledTools.delete) {
    server.registerTool(
      'parquedb_delete',
      {
        description: 'Delete a document (soft delete by default, use hard=true for permanent deletion)',
        inputSchema: {
          collection: z.string().describe('Collection name'),
          id: z.string().describe('Entity ID to delete'),
          hard: z.boolean().optional().default(false).describe('Perform permanent deletion'),
        },
      },
      async (args) => {
        try {
          const collection = db.collection(args.collection)
          const result = await collection.delete(args.id, { hard: args.hard })

          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: true,
                data: result,
              } satisfies ToolResult, null, 2),
            }],
          }
        } catch (error) {
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: false,
                error: error instanceof Error ? error.message : String(error),
              } satisfies ToolResult, null, 2),
            }],
            isError: true,
          }
        }
      }
    )
  }

  // parquedb_count - Count documents
  if (enabledTools.count) {
    server.registerTool(
      'parquedb_count',
      {
        description: 'Count documents in a collection matching an optional filter',
        inputSchema: {
          collection: z.string().describe('Collection name'),
          filter: z.record(z.unknown()).optional().describe('Optional filter to count matching documents'),
        },
      },
      async (args) => {
        try {
          // Use find with limit 0 to get total count from PaginatedResult
          const collection = db.collection(args.collection)
          const result = await collection.find(args.filter, { limit: 1 }) as PaginatedResult<Entity>
          const count = result.total ?? result.items.length

          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: true,
                data: { count },
                count,
              } satisfies ToolResult, null, 2),
            }],
          }
        } catch (error) {
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: false,
                error: error instanceof Error ? error.message : String(error),
              } satisfies ToolResult, null, 2),
            }],
            isError: true,
          }
        }
      }
    )
  }

  // parquedb_aggregate - Run aggregation pipeline
  // Note: Aggregation is performed client-side using find + transform
  if (enabledTools.aggregate) {
    server.registerTool(
      'parquedb_aggregate',
      {
        description: 'Run an aggregation pipeline on a collection (supports $match, $project, $limit, $skip stages)',
        inputSchema: {
          collection: z.string().describe('Collection name'),
          pipeline: z.array(z.record(z.unknown())).describe('Aggregation pipeline stages'),
        },
      },
      async (args) => {
        try {
          // Parse pipeline stages and convert to find options
          let filter: Record<string, unknown> | undefined
          let project: Record<string, 0 | 1> | undefined
          let limit: number | undefined
          let skip: number | undefined

          for (const stage of args.pipeline) {
            if ('$match' in stage && typeof stage.$match === 'object') {
              filter = stage.$match as Record<string, unknown>
            } else if ('$project' in stage && typeof stage.$project === 'object') {
              project = stage.$project as Record<string, 0 | 1>
            } else if ('$limit' in stage && typeof stage.$limit === 'number') {
              limit = stage.$limit
            } else if ('$skip' in stage && typeof stage.$skip === 'number') {
              skip = stage.$skip
            }
          }

          const collection = db.collection(args.collection)
          const result = await collection.find(filter, {
            limit,
            skip,
            project,
          }) as PaginatedResult<Entity>

          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: true,
                data: result.items,
                count: result.items.length,
              } satisfies ToolResult, null, 2),
            }],
          }
        } catch (error) {
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: false,
                error: error instanceof Error ? error.message : String(error),
              } satisfies ToolResult, null, 2),
            }],
            isError: true,
          }
        }
      }
    )
  }

  // parquedb_list_collections - List available collections
  if (enabledTools.listCollections) {
    server.registerTool(
      'parquedb_list_collections',
      {
        description: 'List all available collections in the database',
        inputSchema: {},
      },
      async () => {
        try {
          // Get collections from schema validator if available
          const schemaValidator = (db as unknown as { getSchemaValidator?: () => { getSchema?: () => Record<string, unknown> } | null }).getSchemaValidator?.()
          const collections: CollectionInfo[] = []

          if (schemaValidator && typeof schemaValidator.getSchema === 'function') {
            const schema = schemaValidator.getSchema()
            for (const name of Object.keys(schema)) {
              collections.push({
                name,
                namespace: name.toLowerCase(),
              })
            }
          }

          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: true,
                data: collections,
                count: collections.length,
              } satisfies ToolResult, null, 2),
            }],
          }
        } catch (error) {
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: false,
                error: error instanceof Error ? error.message : String(error),
              } satisfies ToolResult, null, 2),
            }],
            isError: true,
          }
        }
      }
    )
  }

  // parquedb_semantic_search - Semantic/vector search
  if (enabledTools.semanticSearch) {
    server.registerTool(
      'parquedb_semantic_search',
      {
        description: 'Search documents using natural language semantic similarity (requires vector index)',
        inputSchema: {
          collection: z.string().describe('Collection name'),
          query: z.string().describe('Natural language search query'),
          limit: z.number().optional().default(10).describe('Maximum number of results'),
          filter: z.record(z.unknown()).optional().describe('Optional pre-filter'),
        },
      },
      async (args) => {
        try {
          const collection = db.collection(args.collection)

          // Check if semantic search is available
          if (typeof (collection as unknown as { semanticSearch?: unknown }).semanticSearch !== 'function') {
            return {
              content: [{
                type: 'text',
                text: JSON.stringify({
                  success: false,
                  error: 'Semantic search is not available. A vector index must be configured for this collection.',
                } satisfies ToolResult, null, 2),
              }],
              isError: true,
            }
          }

          // Call semantic search if available
          const semanticSearchFn = (collection as unknown as { semanticSearch: (query: string, options?: { limit?: number; filter?: unknown }) => Promise<unknown[]> }).semanticSearch
          const results = await semanticSearchFn(args.query, {
            limit: args.limit,
            filter: args.filter,
          })

          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: true,
                data: results,
                count: Array.isArray(results) ? results.length : 0,
              } satisfies ToolResult, null, 2),
            }],
          }
        } catch (error) {
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: false,
                error: error instanceof Error ? error.message : String(error),
              } satisfies ToolResult, null, 2),
            }],
            isError: true,
          }
        }
      }
    )
  }

  // Register resources for collection data access
  server.registerResource(
    'Database Schema',
    new ResourceTemplate('parquedb://schema', {
      list: async () => ({
        resources: [{
          uri: 'parquedb://schema',
          name: 'Database Schema',
          description: 'ParqueDB database schema definition',
          mimeType: 'application/json',
        }],
      }),
    }),
    {
      description: 'The database schema definition including all collections and their fields',
      mimeType: 'application/json',
    },
    async () => {
      // Get schema from validator if available
      const schemaValidator = (db as unknown as { getSchemaValidator?: () => { getSchema?: () => Record<string, unknown> } | null }).getSchemaValidator?.()
      let schema: Record<string, unknown> = {}
      if (schemaValidator && typeof schemaValidator.getSchema === 'function') {
        schema = schemaValidator.getSchema()
      }

      return {
        contents: [{
          uri: 'parquedb://schema',
          mimeType: 'application/json',
          text: JSON.stringify(schema, null, 2),
        }],
      }
    }
  )

  return server
}

/**
 * Helper to singularize a collection name
 */
function singularize(name: string): string {
  // Basic singularization - handles common cases
  if (name.endsWith('ies')) {
    return name.slice(0, -3) + 'y'
  }
  if (name.endsWith('es') && (name.endsWith('shes') || name.endsWith('ches') || name.endsWith('xes') || name.endsWith('sses'))) {
    return name.slice(0, -2)
  }
  if (name.endsWith('s') && !name.endsWith('ss')) {
    return name.slice(0, -1)
  }
  return name
}

/**
 * Helper to capitalize first letter
 */
function capitalizeFirst(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1)
}

// Re-export types
export type { ParqueDBMCPOptions, ToolResult, CollectionInfo } from './types'
