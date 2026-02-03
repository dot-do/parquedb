/**
 * MCP Integration for ParqueDB
 *
 * Exposes ParqueDB operations to AI agents via the Model Context Protocol (MCP).
 *
 * @example
 * ```typescript
 * import { DB } from 'parquedb'
 * import { createParqueDBMCPServer } from 'parquedb/mcp'
 * import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
 *
 * // Create database
 * const db = DB({
 *   Posts: { title: 'string!', content: 'text', status: 'string' },
 *   Users: { email: 'string!', name: 'string' }
 * })
 *
 * // Create and start MCP server
 * const mcpServer = createParqueDBMCPServer(db)
 * const transport = new StdioServerTransport()
 * await mcpServer.connect(transport)
 *
 * // For read-only access
 * const readOnlyServer = createParqueDBMCPServer(db, { readOnly: true })
 *
 * // With custom configuration
 * const customServer = createParqueDBMCPServer(db, {
 *   name: 'my-database',
 *   version: '1.0.0',
 *   instructions: 'Custom instructions for AI agents',
 *   tools: {
 *     semanticSearch: false,  // Disable semantic search
 *   },
 * })
 * ```
 *
 * @packageDocumentation
 */

export { createParqueDBMCPServer } from './server'

export type {
  ParqueDBMCPOptions,
  ToolResult,
  CollectionInfo,
  FindToolParams,
  GetToolParams,
  CreateToolParams,
  UpdateToolParams,
  DeleteToolParams,
  CountToolParams,
  AggregateToolParams,
  SemanticSearchToolParams,
} from './types'
