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
 * // Create MCP server handle
 * const handle = createParqueDBMCPServer(db)
 * const transport = new StdioServerTransport()
 * await handle.server.connect(transport)
 *
 * // When done, clean up resources
 * await handle.server.close()
 * await handle.dispose()
 *
 * // For read-only access
 * const readOnlyHandle = createParqueDBMCPServer(db, { readOnly: true })
 *
 * // With custom configuration
 * const customHandle = createParqueDBMCPServer(db, {
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
export { ValidationError } from './validation'

export type {
  ParqueDBMCPOptions,
  ParqueDBMCPServerHandle,
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
