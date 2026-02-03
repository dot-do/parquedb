# MCP Server

ParqueDB provides a Model Context Protocol (MCP) server that exposes database operations to AI agents like Claude. This enables AI assistants to query, create, update, and delete data through a standardized tool interface.

## Installation

```bash
npm install parquedb @modelcontextprotocol/sdk
```

## Quick Start

```typescript
import { ParqueDB, MemoryBackend } from 'parquedb'
import { createParqueDBMCPServer } from 'parquedb/integrations/mcp'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'

// Create database with schema
const db = new ParqueDB({
  storage: new MemoryBackend(),
  schema: {
    Posts: { title: 'string!', content: 'text', status: 'string' },
    Users: { email: 'string!', name: 'string' },
  },
})

// Create MCP server
const mcpServer = createParqueDBMCPServer(db)

// Start with stdio transport
const transport = new StdioServerTransport()
await mcpServer.connect(transport)
```

## API Reference

### `createParqueDBMCPServer(db, options?)`

Creates an MCP server instance exposing ParqueDB operations.

```typescript
function createParqueDBMCPServer(
  db: ParqueDB,
  options?: ParqueDBMCPOptions
): McpServer
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `db` | `ParqueDB` | ParqueDB instance to expose |
| `options` | `ParqueDBMCPOptions` | Server configuration |

### Configuration Options

```typescript
interface ParqueDBMCPOptions {
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
```

## Available Tools

The MCP server exposes the following tools to AI agents:

### Query Tools

#### `parquedb_find`

Query documents from a collection with optional filters, sorting, and pagination.

**Parameters:**
- `collection` (required): Collection name to query
- `filter` (optional): MongoDB-style filter object
- `limit` (optional): Maximum number of results (default: 20)
- `skip` (optional): Number of results to skip
- `sort` (optional): Sort specification (field: 1 for asc, -1 for desc)
- `project` (optional): Field projection (1 to include, 0 to exclude)

**Example usage by AI:**
```
Use parquedb_find with:
- collection: "posts"
- filter: { status: "published" }
- limit: 10
- sort: { createdAt: -1 }
```

#### `parquedb_get`

Retrieve a single document by its ID.

**Parameters:**
- `collection` (required): Collection name
- `id` (required): Entity ID (without namespace prefix)
- `project` (optional): Field projection

**Example usage by AI:**
```
Use parquedb_get with:
- collection: "users"
- id: "user-123"
```

#### `parquedb_count`

Count documents matching an optional filter.

**Parameters:**
- `collection` (required): Collection name
- `filter` (optional): Filter to count matching documents

**Example usage by AI:**
```
Use parquedb_count with:
- collection: "posts"
- filter: { status: "draft" }
```

#### `parquedb_aggregate`

Run an aggregation pipeline on a collection.

**Parameters:**
- `collection` (required): Collection name
- `pipeline` (required): Aggregation pipeline stages

Supported stages: `$match`, `$project`, `$limit`, `$skip`

**Example usage by AI:**
```
Use parquedb_aggregate with:
- collection: "orders"
- pipeline: [
    { $match: { status: "completed" } },
    { $project: { total: 1, customerId: 1 } },
    { $limit: 100 }
  ]
```

### Write Tools

#### `parquedb_create`

Create a new document in a collection.

**Parameters:**
- `collection` (required): Collection name
- `data` (required): Document data (must include name field)

**Example usage by AI:**
```
Use parquedb_create with:
- collection: "posts"
- data: {
    name: "My New Post",
    title: "Getting Started with ParqueDB",
    content: "...",
    status: "draft"
  }
```

#### `parquedb_update`

Update an existing document using MongoDB-style update operators.

**Parameters:**
- `collection` (required): Collection name
- `id` (required): Entity ID to update
- `update` (required): Update operations ($set, $unset, $inc, etc.)

**Example usage by AI:**
```
Use parquedb_update with:
- collection: "posts"
- id: "post-123"
- update: {
    $set: { status: "published", publishedAt: "2024-01-15T00:00:00Z" }
  }
```

#### `parquedb_delete`

Delete a document (soft delete by default).

**Parameters:**
- `collection` (required): Collection name
- `id` (required): Entity ID to delete
- `hard` (optional): Perform permanent deletion (default: false)

**Example usage by AI:**
```
Use parquedb_delete with:
- collection: "posts"
- id: "post-123"
```

### Utility Tools

#### `parquedb_list_collections`

List all available collections in the database.

**Parameters:** None

**Example usage by AI:**
```
Use parquedb_list_collections to see available data types.
```

#### `parquedb_semantic_search`

Search documents using natural language semantic similarity.

**Parameters:**
- `collection` (required): Collection name
- `query` (required): Natural language search query
- `limit` (optional): Maximum number of results (default: 10)
- `filter` (optional): Optional pre-filter

**Example usage by AI:**
```
Use parquedb_semantic_search with:
- collection: "posts"
- query: "machine learning tutorials for beginners"
- limit: 5
```

Note: Requires a vector index to be configured for the collection.

## Tool Response Format

All tools return responses in a consistent format:

```typescript
interface ToolResult<T = unknown> {
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
```

## Filter Syntax

The MCP server supports MongoDB-style query operators:

### Comparison Operators
- `$eq`: Equal
- `$ne`: Not equal
- `$gt`: Greater than
- `$gte`: Greater than or equal
- `$lt`: Less than
- `$lte`: Less than or equal

### Array Operators
- `$in`: Match any value in array
- `$nin`: Not in array
- `$all`: Match all values in array

### Logical Operators
- `$and`: Match all conditions
- `$or`: Match any condition
- `$nor`: Match none of the conditions
- `$not`: Negate a condition

### String Operators
- `$regex`: Regular expression match
- `$startsWith`: Starts with string
- `$endsWith`: Ends with string
- `$contains`: Contains substring

### Other Operators
- `$exists`: Field exists or not
- `$type`: Match field type

## Update Operators

The `parquedb_update` tool supports these operators:

- `$set`: Set field values
- `$unset`: Remove fields
- `$inc`: Increment numeric values
- `$push`: Add to arrays
- `$pull`: Remove from arrays
- `$addToSet`: Add unique values to arrays

## Resources

The MCP server also exposes resources:

### `parquedb://schema`

The database schema definition including all collections and their fields.

**MIME Type:** `application/json`

AI agents can access this resource to understand the data model before querying.

## Configuration Examples

### Read-Only Mode

For safety when exposing to AI agents:

```typescript
const mcpServer = createParqueDBMCPServer(db, {
  readOnly: true,
})
```

### Custom Instructions

Provide context-specific instructions to AI agents:

```typescript
const mcpServer = createParqueDBMCPServer(db, {
  name: 'blog-database',
  instructions: `
This database contains blog posts and user data.

## Collections
- Posts: Blog articles with title, content, status, and author
- Users: User profiles with name and email

## Important Notes
- Always check post status before displaying (only show 'published' posts to end users)
- User emails are PII - only expose when explicitly requested
- Use soft delete for posts (default) to allow recovery

## Common Queries
- Get recent posts: find posts, filter by status=published, sort by createdAt desc
- Get user's posts: find posts, filter by authorId
- Search posts: use semantic search with natural language queries
  `.trim(),
})
```

### Selective Tool Enabling

Enable only specific tools:

```typescript
const mcpServer = createParqueDBMCPServer(db, {
  tools: {
    find: true,
    get: true,
    count: true,
    listCollections: true,
    create: false,
    update: false,
    delete: false,
    aggregate: false,
    semanticSearch: true,
  },
})
```

## Transport Options

### Stdio Transport

For command-line tools and local AI agents:

```typescript
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'

const transport = new StdioServerTransport()
await mcpServer.connect(transport)
```

### HTTP Transport

For web-based AI agents:

```typescript
import { HttpServerTransport } from '@modelcontextprotocol/sdk/server/http.js'

const transport = new HttpServerTransport({
  port: 3000,
  path: '/mcp',
})
await mcpServer.connect(transport)
```

## Claude Desktop Integration

To use with Claude Desktop, add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "parquedb": {
      "command": "node",
      "args": ["/path/to/your/mcp-server.js"],
      "env": {
        "DATABASE_URL": "r2://bucket-name"
      }
    }
  }
}
```

Example server script (`mcp-server.js`):

```typescript
import { ParqueDB, R2Backend } from 'parquedb'
import { createParqueDBMCPServer } from 'parquedb/integrations/mcp'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'

const db = new ParqueDB({
  storage: new R2Backend(process.env.R2_BUCKET),
  schema: {
    Posts: { title: 'string!', content: 'text', status: 'string' },
    Users: { email: 'string!', name: 'string' },
  },
})

const mcpServer = createParqueDBMCPServer(db, {
  name: 'my-database',
  readOnly: true, // Safe default for AI access
})

const transport = new StdioServerTransport()
await mcpServer.connect(transport)
```

## Examples

### Blog Platform Backend

```typescript
import { ParqueDB, R2Backend } from 'parquedb'
import { createParqueDBMCPServer } from 'parquedb/integrations/mcp'

const db = new ParqueDB({
  storage: new R2Backend(env.BLOG_BUCKET),
  schema: {
    Posts: {
      title: 'string!',
      slug: 'string!',
      content: 'text!',
      excerpt: 'text',
      status: 'string', // draft, published, archived
      featuredImage: 'string',
      tags: 'string[]',
    },
    Authors: {
      name: 'string!',
      email: 'string!',
      bio: 'text',
      avatar: 'string',
    },
    Comments: {
      content: 'text!',
      authorName: 'string!',
      authorEmail: 'string!',
      status: 'string', // pending, approved, spam
    },
  },
})

const mcpServer = createParqueDBMCPServer(db, {
  name: 'blog-cms',
  version: '1.0.0',
  instructions: `
Blog CMS Database

## Workflow
1. Use parquedb_list_collections to see available types
2. Query posts with parquedb_find, filter by status for public content
3. Use semantic search to find related posts
4. Create drafts with parquedb_create, set status to "draft"
5. Publish by updating status to "published"

## Best Practices
- Always include a slug when creating posts
- Check comment status before displaying (only show "approved")
- Use tags for categorization
  `.trim(),
})
```

### E-commerce Inventory

```typescript
const db = new ParqueDB({
  storage: new R2Backend(env.INVENTORY_BUCKET),
  schema: {
    Products: {
      sku: 'string!',
      name: 'string!',
      description: 'text',
      price: 'number!',
      category: 'string!',
      stock: 'number',
      status: 'string', // active, discontinued, out_of_stock
    },
    Categories: {
      name: 'string!',
      slug: 'string!',
      description: 'text',
    },
    Inventory: {
      productSku: 'string!',
      warehouse: 'string!',
      quantity: 'number!',
      lastUpdated: 'datetime!',
    },
  },
})

// Read-only for AI agents (write operations through API only)
const mcpServer = createParqueDBMCPServer(db, {
  name: 'inventory-db',
  readOnly: true,
  tools: {
    find: true,
    get: true,
    count: true,
    aggregate: true,
    semanticSearch: true,
    listCollections: true,
    create: false,
    update: false,
    delete: false,
  },
})
```

## Security Best Practices

### 1. Use Read-Only Mode by Default

Unless your AI agent needs to modify data, use read-only mode:

```typescript
const mcpServer = createParqueDBMCPServer(db, { readOnly: true })
```

### 2. Limit Exposed Collections

Only expose collections that AI agents need access to:

```typescript
// Create a filtered view of the database
const publicDB = new ParqueDB({
  storage: db.storage,
  schema: {
    Posts: schema.Posts,
    // Don't expose Users, Orders, etc.
  },
})

const mcpServer = createParqueDBMCPServer(publicDB)
```

### 3. Sanitize Custom Instructions

Don't include sensitive information in instructions:

```typescript
// Bad - exposes internal details
instructions: `Admin password is 'secret123'. API key is 'sk-xxx'.`

// Good - only operational guidance
instructions: `Use status field to filter published content.`
```

### 4. Monitor Usage

Log MCP requests for auditing:

```typescript
// Wrap the server with logging
const originalConnect = mcpServer.connect.bind(mcpServer)
mcpServer.connect = async (transport) => {
  // Add request logging middleware
  return originalConnect(transport)
}
```

### 5. Rate Limiting

Implement rate limiting for HTTP transports:

```typescript
import { rateLimit } from 'express-rate-limit'

const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // 100 requests per window
})

app.use('/mcp', limiter)
```
