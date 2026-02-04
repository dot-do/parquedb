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
  name?: string | undefined
  /** Server version (defaults to package version) */
  version?: string | undefined
  /** Instructions for AI agents on how to use this server */
  instructions?: string | undefined
  /** Enable/disable specific tools (all enabled by default) */
  tools?: {
    find?: boolean | undefined
    get?: boolean | undefined
    create?: boolean | undefined
    update?: boolean | undefined
    delete?: boolean | undefined
    count?: boolean | undefined
    aggregate?: boolean | undefined
    listCollections?: boolean | undefined
    semanticSearch?: boolean | undefined
  } | undefined
  /** Enable read-only mode (disables create, update, delete) */
  readOnly?: boolean | undefined
}

/**
 * Parameters for the parquedb_find tool
 */
export interface FindToolParams {
  /** Collection name to query */
  collection: string
  /** MongoDB-style filter object */
  filter?: Filter | undefined
  /** Maximum number of results to return */
  limit?: number | undefined
  /** Number of results to skip */
  skip?: number | undefined
  /** Sort specification (field: 1 for ascending, -1 for descending) */
  sort?: Record<string, 1 | -1> | undefined
  /** Fields to include/exclude in results */
  project?: Record<string, 0 | 1> | undefined
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
  project?: Record<string, 0 | 1> | undefined
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
  update: UpdateInput<Record<string, unknown>>
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
  hard?: boolean | undefined
}

/**
 * Parameters for the parquedb_count tool
 */
export interface CountToolParams {
  /** Collection name */
  collection: string
  /** Optional filter to count matching documents */
  filter?: Filter | undefined
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
  limit?: number | undefined
  /** Optional filter to apply before semantic search */
  filter?: Filter | undefined
}

/**
 * Tool result wrapper for MCP
 */
export interface ToolResult<T = unknown> {
  /** Whether the operation succeeded */
  success: boolean
  /** Result data (on success) */
  data?: T | undefined
  /** Error message (on failure) */
  error?: string | undefined
  /** Number of items affected/returned */
  count?: number | undefined
  /** Total count (for paginated results) */
  total?: number | undefined
  /** Whether there are more results */
  hasMore?: boolean | undefined
  /** Cursor for next page */
  nextCursor?: string | undefined
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
  estimatedCount?: number | undefined
}
