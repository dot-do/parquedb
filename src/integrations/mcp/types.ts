/**
 * MCP Integration Types
 *
 * Type definitions for the ParqueDB MCP (Model Context Protocol) server.
 */

import type { Filter, UpdateInput } from '../../types'

/**
 * Configuration options for the ParqueDB MCP server
 */
export interface ParqueDBMCPOptions {
  /** Server name (defaults to 'parquedb') */
  name?: string
  /** Server version (defaults to package version) */
  version?: string
  /** Instructions for AI agents on how to use this server */
  instructions?: string
  /** Enable/disable specific tools (all enabled by default) */
  tools?: {
    find?: boolean
    get?: boolean
    create?: boolean
    update?: boolean
    delete?: boolean
    count?: boolean
    aggregate?: boolean
    listCollections?: boolean
    semanticSearch?: boolean
  }
  /** Enable read-only mode (disables create, update, delete) */
  readOnly?: boolean
}

/**
 * Parameters for the parquedb_find tool
 */
export interface FindToolParams {
  /** Collection name to query */
  collection: string
  /** MongoDB-style filter object */
  filter?: Filter
  /** Maximum number of results to return */
  limit?: number
  /** Number of results to skip */
  skip?: number
  /** Sort specification (field: 1 for ascending, -1 for descending) */
  sort?: Record<string, 1 | -1>
  /** Fields to include/exclude in results */
  project?: Record<string, 0 | 1>
}

/**
 * Parameters for the parquedb_get tool
 */
export interface GetToolParams {
  /** Collection name */
  collection: string
  /** Entity ID (without namespace prefix) */
  id: string
  /** Fields to include/exclude */
  project?: Record<string, 0 | 1>
}

/**
 * Parameters for the parquedb_create tool
 */
export interface CreateToolParams {
  /** Collection name */
  collection: string
  /** Entity data to create */
  data: Record<string, unknown>
}

/**
 * Parameters for the parquedb_update tool
 */
export interface UpdateToolParams {
  /** Collection name */
  collection: string
  /** Entity ID to update */
  id: string
  /** MongoDB-style update operations */
  update: UpdateInput<unknown>
}

/**
 * Parameters for the parquedb_delete tool
 */
export interface DeleteToolParams {
  /** Collection name */
  collection: string
  /** Entity ID to delete */
  id: string
  /** Perform hard delete (permanent) instead of soft delete */
  hard?: boolean
}

/**
 * Parameters for the parquedb_count tool
 */
export interface CountToolParams {
  /** Collection name */
  collection: string
  /** Optional filter to count matching documents */
  filter?: Filter
}

/**
 * Parameters for the parquedb_aggregate tool
 */
export interface AggregateToolParams {
  /** Collection name */
  collection: string
  /** Aggregation pipeline stages */
  pipeline: Array<Record<string, unknown>>
}

/**
 * Parameters for the parquedb_semantic_search tool
 */
export interface SemanticSearchToolParams {
  /** Collection name */
  collection: string
  /** Natural language search query */
  query: string
  /** Maximum number of results */
  limit?: number
  /** Optional filter to apply before semantic search */
  filter?: Filter
}

/**
 * Tool result wrapper for MCP
 */
export interface ToolResult<T = unknown> {
  /** Whether the operation succeeded */
  success: boolean
  /** Result data (on success) */
  data?: T
  /** Error message (on failure) */
  error?: string
  /** Number of items affected/returned */
  count?: number
  /** Total count (for paginated results) */
  total?: number
  /** Whether there are more results */
  hasMore?: boolean
  /** Cursor for next page */
  nextCursor?: string
}

/**
 * Collection metadata for list_collections tool
 */
export interface CollectionInfo {
  /** Collection name */
  name: string
  /** Collection namespace */
  namespace: string
  /** Estimated document count */
  estimatedCount?: number
}
