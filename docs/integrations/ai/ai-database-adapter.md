# ai-database Adapter

The ParqueDB adapter for `ai-database` provides a complete implementation of the `DBProvider` and `DBProviderExtended` interfaces, allowing ParqueDB to serve as the storage backend for AI applications.

## Installation

```bash
npm install parquedb
# ai-database is an optional peer dependency
npm install ai-database
```

## Quick Start

```typescript
import { ParqueDB, MemoryBackend } from 'parquedb'
import { createParqueDBProvider } from 'parquedb/integrations/ai-database'

// Create a ParqueDB instance
const db = new ParqueDB({ storage: new MemoryBackend() })

// Create the ai-database provider
const provider = createParqueDBProvider(db)

// Use the provider
const user = await provider.create('User', undefined, {
  name: 'Alice',
  email: 'alice@example.com',
})

const users = await provider.list('User', { limit: 10 })
```

## API Reference

### Factory Function

#### `createParqueDBProvider(db, options?)`

Creates a new ParqueDB provider implementing `DBProviderExtended`.

```typescript
function createParqueDBProvider(
  db: ParqueDB,
  options?: ParqueDBAdapterOptions
): DBProviderExtended
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `db` | `ParqueDB` | ParqueDB instance to wrap |
| `options` | `ParqueDBAdapterOptions` | Optional configuration |

**Options:**

```typescript
interface ParqueDBAdapterOptions {
  /** Enable batch loading for relationships (default: true) */
  enableBatchLoader?: boolean
  /** Options for the relationship batch loader */
  batchLoaderOptions?: {
    /** Batching window in milliseconds (default: 10ms) */
    windowMs?: number
    /** Maximum batch size before flush (default: 100) */
    maxBatchSize?: number
  }
}
```

### Class: ParqueDBAdapter

The main adapter class implementing both `DBProvider` and `DBProviderExtended`.

#### Constructor

```typescript
const adapter = new ParqueDBAdapter(db, options?)
```

### Read Operations

#### `get(type, id)`

Retrieve a single entity by type and ID.

```typescript
const user = await provider.get('User', 'user-123')
// Returns: { $id: 'users/user-123', $type: 'User', name: 'Alice', ... } | null
```

#### `list(type, options?)`

List entities of a given type with optional filtering and pagination.

```typescript
const users = await provider.list('User', {
  where: { status: 'active' },
  orderBy: 'createdAt',
  order: 'desc',
  limit: 10,
  offset: 0,
})
```

**ListOptions:**

```typescript
interface ListOptions {
  where?: Record<string, unknown>
  orderBy?: string
  order?: 'asc' | 'desc'
  limit?: number
  offset?: number
}
```

#### `search(type, query, options?)`

Full-text search across entities.

```typescript
const results = await provider.search('Post', 'typescript tutorial', {
  fields: ['title', 'content'],
  minScore: 0.5,
  limit: 20,
})
```

**SearchOptions:**

```typescript
interface SearchOptions extends ListOptions {
  fields?: string[]
  minScore?: number
}
```

### Write Operations

#### `create(type, id, data)`

Create a new entity.

```typescript
const post = await provider.create('Post', undefined, {
  title: 'Hello World',
  content: 'My first post...',
  status: 'draft',
})
// ID is auto-generated if not provided

// Or with a specific ID
const post = await provider.create('Post', 'my-post-id', { ... })
```

#### `update(type, id, data)`

Update an existing entity.

```typescript
const updated = await provider.update('Post', 'post-123', {
  status: 'published',
  publishedAt: new Date(),
})
```

#### `delete(type, id)`

Delete an entity.

```typescript
const deleted = await provider.delete('Post', 'post-123')
// Returns: true if deleted, false if not found
```

### Relationship Operations

#### `related(type, id, relation)`

Get related entities. Automatically batches multiple calls.

```typescript
// Single call
const author = await provider.related('Post', 'post-123', 'author')

// Batched calls (all executed in a single query)
const [author1, author2, author3] = await Promise.all([
  provider.related('Post', 'post-1', 'author'),
  provider.related('Post', 'post-2', 'author'),
  provider.related('Post', 'post-3', 'author'),
])
```

#### `relate(fromType, fromId, relation, toType, toId, metadata?)`

Create a relationship between entities.

```typescript
await provider.relate('Post', 'post-123', 'author', 'User', 'user-456', {
  matchMode: 'exact',
})
```

**RelationMetadata:**

```typescript
interface RelationMetadata {
  matchMode?: 'exact' | 'fuzzy'
  similarity?: number
  matchedType?: string
  [key: string]: unknown
}
```

#### `unrelate(fromType, fromId, relation, toType, toId)`

Remove a relationship.

```typescript
await provider.unrelate('Post', 'post-123', 'author', 'User', 'user-456')
```

### Transaction Support

#### `beginTransaction()`

Begin a new transaction.

```typescript
const tx = await provider.beginTransaction()

try {
  const user = await tx.create('User', undefined, { name: 'Alice' })
  const post = await tx.create('Post', undefined, { title: 'Hello' })
  await tx.relate('Post', post.$id, 'author', 'User', user.$id)
  await tx.commit()
} catch (error) {
  await tx.rollback()
  throw error
}
```

**Transaction Interface:**

```typescript
interface Transaction {
  get(type: string, id: string): Promise<Record<string, unknown> | null>
  create(type: string, id: string | undefined, data: Record<string, unknown>): Promise<Record<string, unknown>>
  update(type: string, id: string, data: Record<string, unknown>): Promise<Record<string, unknown>>
  delete(type: string, id: string): Promise<boolean>
  relate(fromType: string, fromId: string, relation: string, toType: string, toId: string, metadata?: RelationMetadata): Promise<void>
  commit(): Promise<void>
  rollback(): Promise<void>
}
```

### Semantic Search

#### `setEmbeddingsConfig(config)`

Configure automatic embedding generation.

```typescript
provider.setEmbeddingsConfig({
  provider: 'openai',
  model: 'text-embedding-3-small',
  dimensions: 1536,
  fields: {
    Post: ['title', 'content'],
    User: ['bio'],
  },
})
```

**EmbeddingsConfig:**

```typescript
interface EmbeddingsConfig {
  provider?: string
  model?: string
  dimensions?: number
  fields?: Record<string, string[]>
}
```

#### `semanticSearch(type, query, options?)`

Search using vector similarity.

```typescript
const results = await provider.semanticSearch('Post', 'machine learning tutorials', {
  limit: 10,
  minScore: 0.7,
})
// Returns: SemanticSearchResult[]
```

**SemanticSearchResult:**

```typescript
interface SemanticSearchResult {
  $id: string
  $type: string
  $score: number
  [key: string]: unknown
}
```

#### `hybridSearch(type, query, options?)`

Combine full-text and semantic search using Reciprocal Rank Fusion (RRF).

```typescript
const results = await provider.hybridSearch('Post', 'typescript best practices', {
  limit: 10,
  ftsWeight: 0.3,
  semanticWeight: 0.7,
  rrfK: 60,
})
// Returns: HybridSearchResult[]
```

**HybridSearchOptions:**

```typescript
interface HybridSearchOptions {
  minScore?: number
  limit?: number
  offset?: number
  rrfK?: number           // RRF constant (default: 60)
  ftsWeight?: number      // FTS weight (default: 0.5)
  semanticWeight?: number // Semantic weight (default: 0.5)
}
```

**HybridSearchResult:**

```typescript
interface HybridSearchResult extends SemanticSearchResult {
  $rrfScore: number
  $ftsRank: number
  $semanticRank: number
}
```

### Events API

Track events in your AI application.

#### `emit(options)`

Emit an event.

```typescript
// Full options
const event = await provider.emit({
  actor: 'user-123',
  event: 'chat.message.sent',
  object: 'conversation-456',
  objectData: { content: 'Hello!' },
  result: 'message-789',
  resultData: { tokens: 150 },
  meta: { model: 'gpt-4' },
})

// Simple format
const event = await provider.emit('user.signup', { email: 'alice@example.com' })
```

**CreateEventOptions:**

```typescript
interface CreateEventOptions {
  actor: string
  event: string
  object?: string
  objectData?: Record<string, unknown>
  result?: string
  resultData?: Record<string, unknown>
  meta?: Record<string, unknown>
}
```

#### `on(pattern, handler)`

Subscribe to events.

```typescript
// Exact match
const unsubscribe = provider.on('chat.message.sent', async (event) => {
  console.log('Message sent:', event)
})

// Wildcard match
provider.on('chat.*', async (event) => {
  console.log('Chat event:', event.event)
})

// All events
provider.on('*', async (event) => {
  await analyticsService.track(event)
})

// Unsubscribe
unsubscribe()
```

#### `listEvents(options?)`

Query stored events.

```typescript
const events = await provider.listEvents({
  event: 'chat.message.sent',
  actor: 'user-123',
  since: new Date('2024-01-01'),
  until: new Date(),
  limit: 100,
})
```

#### `replayEvents(options)`

Replay events through a handler.

```typescript
await provider.replayEvents({
  event: 'user.signup',
  since: new Date('2024-01-01'),
  handler: async (event) => {
    await processSignup(event.objectData)
  },
})
```

### Actions API

Track long-running actions with progress.

#### `createAction(options)`

Create a new action.

```typescript
const action = await provider.createAction({
  actor: 'user-123',
  action: 'generate',
  object: 'report-456',
  objectData: { format: 'pdf' },
  total: 100,
  meta: { priority: 'high' },
})
```

**DBAction:**

```typescript
interface DBAction {
  id: string
  actor: string
  act: string        // "generates"
  action: string     // "generate"
  activity: string   // "generating"
  object?: string
  objectData?: Record<string, unknown>
  status: 'pending' | 'active' | 'completed' | 'failed' | 'cancelled'
  progress?: number
  total?: number
  result?: Record<string, unknown>
  error?: string
  meta?: Record<string, unknown>
  createdAt: Date
  startedAt?: Date
  completedAt?: Date
}
```

#### `getAction(id)`

Get an action by ID.

```typescript
const action = await provider.getAction('action-123')
```

#### `updateAction(id, updates)`

Update action progress or status.

```typescript
// Update progress
await provider.updateAction('action-123', {
  status: 'active',
  progress: 50,
})

// Mark complete
await provider.updateAction('action-123', {
  status: 'completed',
  result: { url: 'https://example.com/report.pdf' },
})

// Mark failed
await provider.updateAction('action-123', {
  status: 'failed',
  error: 'Timeout exceeded',
})
```

#### `listActions(options?)`

Query actions.

```typescript
const pendingActions = await provider.listActions({
  status: 'pending',
  action: 'generate',
  actor: 'user-123',
  limit: 50,
})
```

#### `retryAction(id)` / `cancelAction(id)`

Retry failed actions or cancel pending/active actions.

```typescript
// Retry a failed action
const retried = await provider.retryAction('action-123')

// Cancel an action
await provider.cancelAction('action-456')
```

### Artifacts API

Cache computed artifacts (e.g., embeddings, transformations).

#### `setArtifact(url, type, data)`

Store an artifact.

```typescript
await provider.setArtifact(
  'https://example.com/document.pdf',
  'embedding',
  {
    content: [0.1, 0.2, 0.3, ...], // embedding vector
    sourceHash: 'sha256:abc123...',
    metadata: { model: 'text-embedding-3-small', dimensions: 1536 },
  }
)
```

#### `getArtifact(url, type)`

Retrieve a cached artifact.

```typescript
const artifact = await provider.getArtifact(
  'https://example.com/document.pdf',
  'embedding'
)

if (artifact && artifact.sourceHash === currentHash) {
  // Use cached embedding
  return artifact.content
}
```

**DBArtifact:**

```typescript
interface DBArtifact {
  url: string
  type: string
  sourceHash: string
  content: unknown
  metadata?: Record<string, unknown>
  createdAt: Date
}
```

#### `listArtifacts(url)`

List all artifacts for a URL.

```typescript
const artifacts = await provider.listArtifacts('https://example.com/document.pdf')
// Returns artifacts of all types for this URL
```

#### `deleteArtifact(url, type?)`

Delete artifacts.

```typescript
// Delete specific type
await provider.deleteArtifact('https://example.com/document.pdf', 'embedding')

// Delete all types for URL
await provider.deleteArtifact('https://example.com/document.pdf')
```

### Batch Loader

The adapter includes automatic batch loading for relationship queries.

#### `getBatchLoader()`

Access the batch loader instance.

```typescript
const batchLoader = adapter.getBatchLoader()
if (batchLoader) {
  // Access cache statistics, etc.
}
```

#### `clearBatchLoader()`

Clear the batch loader cache. Call between requests in server environments.

```typescript
// In your request handler
async function handleRequest(req: Request) {
  try {
    // Handle request...
    return response
  } finally {
    adapter.clearBatchLoader()
  }
}
```

## Type Conversion

The adapter automatically converts between ParqueDB types and ai-database types:

| ai-database | ParqueDB |
|-------------|----------|
| `User` type | `users` namespace |
| `Post` type | `posts` namespace |
| Entity ID `post-123` | Full ID `posts/post-123` |

Type names are lowercased and pluralized for namespace conversion.

## Examples

### Chat Application

```typescript
import { createParqueDBProvider } from 'parquedb/integrations/ai-database'

const provider = createParqueDBProvider(db)

// Create a conversation
const conversation = await provider.create('Conversation', undefined, {
  name: 'Chat with AI',
  createdBy: 'user-123',
})

// Add a message
const message = await provider.create('Message', undefined, {
  content: 'Hello, AI!',
  role: 'user',
})

// Link message to conversation
await provider.relate(
  'Message', message.$id,
  'conversation',
  'Conversation', conversation.$id
)

// Get conversation messages
const messages = await provider.related(
  'Conversation',
  conversation.$id,
  'messages'
)
```

### Document Processing Pipeline

```typescript
const provider = createParqueDBProvider(db)

// Create processing action
const action = await provider.createAction({
  actor: 'system',
  action: 'process',
  object: 'document-123',
  total: 3,
})

// Step 1: Extract text
await provider.updateAction(action.id, { status: 'active', progress: 1 })
const text = await extractText(documentUrl)

// Step 2: Generate embeddings
await provider.updateAction(action.id, { progress: 2 })
const embedding = await generateEmbedding(text)

// Step 3: Store artifact
await provider.setArtifact(documentUrl, 'embedding', {
  content: embedding,
  sourceHash: await hashContent(text),
  metadata: { model: 'text-embedding-3-small' },
})

await provider.updateAction(action.id, {
  status: 'completed',
  progress: 3,
  result: { embeddingDimensions: embedding.length },
})

// Emit completion event
await provider.emit({
  actor: 'system',
  event: 'document.processed',
  object: 'document-123',
  result: action.id,
})
```

### Semantic Search with Filters

```typescript
const provider = createParqueDBProvider(db)

// Configure embeddings
provider.setEmbeddingsConfig({
  provider: 'openai',
  model: 'text-embedding-3-small',
  dimensions: 1536,
  fields: { Document: ['title', 'content'] },
})

// Hybrid search with category filter
const results = await provider.hybridSearch('Document', 'machine learning', {
  limit: 20,
  ftsWeight: 0.3,
  semanticWeight: 0.7,
})

// Filter results by category post-search
const filtered = results.filter(doc => doc.category === 'tutorials')
```
