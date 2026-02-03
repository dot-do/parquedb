# AI Integrations Overview

ParqueDB provides first-class integrations for AI applications, enabling seamless data persistence for agents, evaluation frameworks, and language model operations.

## Architecture

```
+------------------+     +------------------+     +------------------+
|   AI Agents      |     |   LLM Apps       |     |   Eval Systems   |
|  (Claude, GPT)   |     | (Vercel AI SDK)  |     |    (Evalite)     |
+--------+---------+     +--------+---------+     +--------+---------+
         |                        |                        |
         v                        v                        v
+--------+---------+     +--------+---------+     +--------+---------+
|   MCP Server     |     |   AI SDK         |     |   Evalite        |
|                  |     |   Middleware     |     |   Adapter        |
+--------+---------+     +--------+---------+     +--------+---------+
         |                        |                        |
         +------------------------+------------------------+
                                  |
                          +-------v-------+
                          |  ai-database  |
                          |   Adapter     |
                          +-------+-------+
                                  |
                          +-------v-------+
                          |   ParqueDB    |
                          |    Core       |
                          +-------+-------+
                                  |
         +------------------------+------------------------+
         |                        |                        |
+--------v--------+      +--------v--------+      +--------v--------+
|   Memory        |      |      R2         |      |      S3         |
|   Backend       |      |    Storage      |      |    Storage      |
+-----------------+      +-----------------+      +-----------------+
```

## Available Integrations

### 1. ai-database Adapter

The `ai-database` adapter provides a universal database interface for AI applications, implementing the `DBProvider` and `DBProviderExtended` interfaces.

**Key Features:**
- CRUD operations with type-safe interfaces
- Relationship management with batch loading
- Full-text and semantic search
- Events and actions tracking
- Artifact storage for caching

```typescript
import { ParqueDB, MemoryBackend } from 'parquedb'
import { createParqueDBProvider } from 'parquedb/integrations/ai-database'

const db = new ParqueDB({ storage: new MemoryBackend() })
const provider = createParqueDBProvider(db)

// Use with ai-database applications
await provider.create('User', undefined, { name: 'Alice', email: 'alice@example.com' })
const users = await provider.list('User', { limit: 10 })
```

[Full Documentation](./ai-database-adapter.md)

### 2. Vercel AI SDK Middleware

Middleware for the Vercel AI SDK that provides response caching and request logging.

**Key Features:**
- Response caching to reduce API costs
- Comprehensive request logging
- Cache statistics and management
- Support for both generate and stream operations

```typescript
import { createParqueDBMiddleware } from 'parquedb/integrations/ai-sdk'
import { wrapLanguageModel, generateText } from 'ai'
import { openai } from '@ai-sdk/openai'

const middleware = createParqueDBMiddleware({
  db,
  cache: { enabled: true, ttlSeconds: 3600 },
  logging: { enabled: true, level: 'standard' },
})

const model = wrapLanguageModel({
  model: openai('gpt-4'),
  middleware,
})
```

[Full Documentation](./vercel-ai-sdk.md)

### 3. MCP Server

A Model Context Protocol (MCP) server that exposes ParqueDB operations to AI agents like Claude.

**Key Features:**
- Full CRUD tool operations
- Semantic search support
- Read-only mode for safety
- MongoDB-style query syntax

```typescript
import { createParqueDBMCPServer } from 'parquedb/integrations/mcp'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'

const mcpServer = createParqueDBMCPServer(db)
await mcpServer.connect(new StdioServerTransport())
```

[Full Documentation](./mcp-server.md)

### 4. Evalite Adapter

Storage adapter for Evalite, the TypeScript AI evaluation framework.

**Key Features:**
- Persistent storage for evaluation runs
- Score tracking and history
- LLM trace storage with token usage
- Dashboard-ready analytics queries

```typescript
import { defineConfig } from 'evalite'
import { createEvaliteAdapter } from 'parquedb/integrations/evalite'

export default defineConfig({
  storage: () => createEvaliteAdapter({
    storage: new R2Backend(env.EVALITE_BUCKET),
  }),
})
```

[Full Documentation](./evalite.md)

## Import Paths

All AI integrations can be imported from the main integrations module:

```typescript
// All-in-one import
import {
  createParqueDBProvider,
  createParqueDBMiddleware,
  createParqueDBMCPServer,
  createEvaliteAdapter,
} from 'parquedb/integrations'

// Or from specific modules
import { createParqueDBProvider } from 'parquedb/integrations/ai-database'
import { createParqueDBMiddleware } from 'parquedb/integrations/ai-sdk'
import { createParqueDBMCPServer } from 'parquedb/integrations/mcp'
import { createEvaliteAdapter } from 'parquedb/integrations/evalite'
```

## Storage Backends

All integrations work with any ParqueDB storage backend:

| Backend | Use Case |
|---------|----------|
| `MemoryBackend` | Local development, testing |
| `FSBackend` | Node.js local storage |
| `R2Backend` | Cloudflare Workers production |
| `S3Backend` | AWS production |

## Best Practices

### 1. Use Batch Loading

The ai-database adapter includes automatic batch loading for relationships, eliminating N+1 queries:

```typescript
// These queries are automatically batched
const [author1, author2, author3] = await Promise.all([
  provider.related('Post', 'post-1', 'author'),
  provider.related('Post', 'post-2', 'author'),
  provider.related('Post', 'post-3', 'author'),
])
```

### 2. Enable Caching Wisely

For AI SDK middleware, cache deterministic queries but be careful with temperature > 0:

```typescript
const middleware = createParqueDBMiddleware({
  db,
  cache: {
    enabled: true,
    ttlSeconds: 3600,
    excludeFromKey: ['temperature'], // Exclude non-deterministic params
  },
})
```

### 3. Use Read-Only Mode for Agents

When exposing your database to AI agents, use read-only mode unless writes are explicitly needed:

```typescript
const mcpServer = createParqueDBMCPServer(db, { readOnly: true })
```

### 4. Clear Batch Loader Between Requests

In server environments, clear the batch loader cache between requests:

```typescript
// After handling a request
provider.clearBatchLoader()
```

## Next Steps

- [ai-database Adapter](./ai-database-adapter.md) - Complete reference
- [Vercel AI SDK Middleware](./vercel-ai-sdk.md) - Caching and logging
- [MCP Server](./mcp-server.md) - AI agent integration
- [Evalite Adapter](./evalite.md) - Evaluation storage
