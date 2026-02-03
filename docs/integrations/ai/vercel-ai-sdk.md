# Vercel AI SDK Middleware

ParqueDB provides middleware for the Vercel AI SDK that enables response caching and comprehensive request logging. This helps reduce API costs, improve latency, and maintain audit trails for AI operations.

## Installation

```bash
npm install parquedb ai @ai-sdk/openai
```

## Quick Start

```typescript
import { ParqueDB, MemoryBackend } from 'parquedb'
import { createParqueDBMiddleware } from 'parquedb/integrations/ai-sdk'
import { wrapLanguageModel, generateText } from 'ai'
import { openai } from '@ai-sdk/openai'

// Create database
const db = new ParqueDB({ storage: new MemoryBackend() })

// Create middleware with caching and logging
const middleware = createParqueDBMiddleware({
  db,
  cache: {
    enabled: true,
    ttlSeconds: 3600, // 1 hour
  },
  logging: {
    enabled: true,
    level: 'standard',
  },
})

// Wrap your model
const model = wrapLanguageModel({
  model: openai('gpt-4'),
  middleware,
})

// Use as normal - responses are cached and logged automatically
const result = await generateText({
  model,
  prompt: 'What is TypeScript?',
})
```

## API Reference

### `createParqueDBMiddleware(options)`

Creates middleware compatible with the AI SDK's `wrapLanguageModel()` function.

```typescript
function createParqueDBMiddleware(
  options: ParqueDBMiddlewareOptions
): LanguageModelV3Middleware
```

**ParqueDBMiddlewareOptions:**

```typescript
interface ParqueDBMiddlewareOptions {
  /** ParqueDB instance for storage */
  db: ParqueDB
  /** Cache configuration */
  cache?: CacheConfig
  /** Logging configuration */
  logging?: LoggingConfig
}
```

### Cache Configuration

```typescript
interface CacheConfig {
  /** Enable response caching (default: false) */
  enabled: boolean

  /** Time-to-live for cached entries in seconds (default: 3600 = 1 hour) */
  ttlSeconds?: number

  /** Collection name for cache storage (default: 'ai_cache') */
  collection?: string

  /** Custom hash function for generating cache keys */
  hashFn?: (params: unknown) => string | Promise<string>

  /** Fields to exclude from cache key generation */
  excludeFromKey?: string[]
}
```

### Logging Configuration

```typescript
interface LoggingConfig {
  /** Enable request/response logging (default: false) */
  enabled: boolean

  /** Collection name for log storage (default: 'ai_logs') */
  collection?: string

  /** Log level */
  level?: 'minimal' | 'standard' | 'verbose'

  /** Custom metadata to include with each log entry */
  metadata?: Record<string, unknown>

  /** Callback for custom log processing (runs after database write) */
  onLog?: (entry: LogEntry) => void | Promise<void>
}
```

**Log Levels:**

| Level | Includes |
|-------|----------|
| `minimal` | Model ID, latency, cached status |
| `standard` | + usage (tokens), response text |
| `verbose` | + full prompt and response objects |

## Configuration Examples

### Caching Only

```typescript
const middleware = createParqueDBMiddleware({
  db,
  cache: {
    enabled: true,
    ttlSeconds: 7200, // 2 hours
    collection: 'my_ai_cache',
  },
})
```

### Logging Only

```typescript
const middleware = createParqueDBMiddleware({
  db,
  logging: {
    enabled: true,
    level: 'verbose',
    collection: 'my_ai_logs',
    metadata: {
      app: 'my-app',
      version: '1.0.0',
      environment: 'production',
    },
  },
})
```

### Exclude Fields from Cache Key

Useful for excluding non-deterministic parameters:

```typescript
const middleware = createParqueDBMiddleware({
  db,
  cache: {
    enabled: true,
    excludeFromKey: ['temperature', 'seed'], // Exclude randomness params
  },
})
```

### Custom Hash Function

```typescript
const middleware = createParqueDBMiddleware({
  db,
  cache: {
    enabled: true,
    hashFn: async (params) => {
      // Custom hashing logic
      const json = JSON.stringify(params)
      const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(json))
      return `cache_${Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, '0')).join('')}`
    },
  },
})
```

### Custom Log Handler

```typescript
const middleware = createParqueDBMiddleware({
  db,
  logging: {
    enabled: true,
    level: 'standard',
    onLog: async (entry) => {
      // Send to external analytics
      await analytics.track('ai_request', {
        modelId: entry.modelId,
        latencyMs: entry.latencyMs,
        tokens: entry.usage?.totalTokens,
        cached: entry.cached,
      })

      // Alert on errors
      if (entry.error) {
        await alerting.notify('AI request failed', entry.error)
      }
    },
  },
})
```

## Storage Types

### CacheEntry

```typescript
interface CacheEntry {
  /** Cache key (hash of request parameters) */
  key: string
  /** Original request parameters */
  params: Record<string, unknown>
  /** Cached response data */
  response: unknown
  /** Model ID used for the request */
  modelId?: string
  /** Cache hit count */
  hitCount: number
  /** When this entry was created */
  createdAt: Date
  /** When this entry expires */
  expiresAt: Date
  /** When this entry was last accessed */
  lastAccessedAt: Date
}
```

### LogEntry

```typescript
interface LogEntry {
  /** Unique log ID */
  $id: string
  /** Log type */
  $type: string
  /** Log name (model + timestamp) */
  name: string
  /** Timestamp of the request */
  timestamp: Date
  /** Model ID */
  modelId?: string
  /** Provider ID */
  providerId?: string
  /** Request type: 'generate' or 'stream' */
  requestType: 'generate' | 'stream'
  /** Prompt messages (if logging level allows) */
  prompt?: unknown
  /** Response data (if logging level allows) */
  response?: unknown
  /** Response text (extracted for convenience) */
  responseText?: string
  /** Token usage information */
  usage?: {
    promptTokens?: number
    completionTokens?: number
    totalTokens?: number
  }
  /** Request latency in milliseconds */
  latencyMs: number
  /** Whether the request was cached */
  cached: boolean
  /** Finish reason */
  finishReason?: string
  /** Additional metadata */
  metadata?: Record<string, unknown>
  /** Error information (if request failed) */
  error?: {
    name: string
    message: string
    stack?: string
  }
}
```

## Utility Functions

### `hashParams(params, modelId, excludeFields?)`

Generate a deterministic hash of request parameters.

```typescript
import { hashParams } from 'parquedb/integrations/ai-sdk'

const key = await hashParams(
  { prompt: 'Hello', temperature: 0.7 },
  'gpt-4',
  ['temperature']
)
// Returns: 'cache_1abc2def'
```

### `isExpired(entry)`

Check if a cache entry has expired.

```typescript
import { isExpired } from 'parquedb/integrations/ai-sdk'

const entry = await db.collection('ai_cache').findOne({ key: 'cache_123' })
if (entry && !isExpired(entry)) {
  // Use cached response
}
```

### `queryCacheEntries(db, options?)`

Query cached responses.

```typescript
import { queryCacheEntries } from 'parquedb/integrations/ai-sdk'

// Get recent cache entries
const entries = await queryCacheEntries(db, {
  limit: 100,
  sortBy: 'hitCount',
  sortOrder: 'desc',
})

// Get entries for a specific model
const gpt4Entries = await queryCacheEntries(db, {
  modelId: 'gpt-4',
  includeExpired: false,
})
```

**Options:**

```typescript
interface QueryCacheOptions {
  collection?: string
  modelId?: string
  limit?: number
  sortBy?: 'createdAt' | 'hitCount' | 'lastAccessedAt'
  sortOrder?: 'asc' | 'desc'
  includeExpired?: boolean
}
```

### `queryLogEntries(db, options?)`

Query log entries.

```typescript
import { queryLogEntries } from 'parquedb/integrations/ai-sdk'

// Get recent logs
const logs = await queryLogEntries(db, {
  limit: 50,
})

// Get error logs only
const errorLogs = await queryLogEntries(db, {
  errorsOnly: true,
})

// Get logs for a specific model in the last 24 hours
const gpt4Logs = await queryLogEntries(db, {
  modelId: 'gpt-4',
  since: new Date(Date.now() - 24 * 60 * 60 * 1000),
})
```

**Options:**

```typescript
interface QueryLogOptions {
  collection?: string
  modelId?: string
  requestType?: 'generate' | 'stream'
  since?: Date
  until?: Date
  limit?: number
  errorsOnly?: boolean
  cachedOnly?: boolean
}
```

### `clearExpiredCache(db, options?)`

Remove expired cache entries.

```typescript
import { clearExpiredCache } from 'parquedb/integrations/ai-sdk'

// Clean up expired entries
const deletedCount = await clearExpiredCache(db)
console.log(`Deleted ${deletedCount} expired cache entries`)

// Custom collection
await clearExpiredCache(db, { collection: 'my_ai_cache' })
```

### `getCacheStats(db, options?)`

Get cache statistics.

```typescript
import { getCacheStats } from 'parquedb/integrations/ai-sdk'

const stats = await getCacheStats(db)

console.log(`Total entries: ${stats.totalEntries}`)
console.log(`Active entries: ${stats.activeEntries}`)
console.log(`Expired entries: ${stats.expiredEntries}`)
console.log(`Total hits: ${stats.totalHits}`)
console.log(`Oldest entry: ${stats.oldestEntry}`)
console.log(`Newest entry: ${stats.newestEntry}`)
```

**Returns:**

```typescript
interface CacheStats {
  totalEntries: number
  activeEntries: number
  expiredEntries: number
  totalHits: number
  oldestEntry?: Date
  newestEntry?: Date
}
```

## Streaming Support

The middleware supports streaming responses, but note that streams are not cached by design:

1. Streams are consumed once and cannot be replayed without buffering
2. Buffering defeats the purpose of streaming (lower latency)

Streaming requests are still logged when logging is enabled.

```typescript
import { streamText } from 'ai'

const stream = await streamText({
  model, // Wrapped model
  prompt: 'Write a story...',
})

// Stream is logged but not cached
for await (const chunk of stream.textStream) {
  process.stdout.write(chunk)
}
```

## Examples

### Production Setup with R2

```typescript
import { ParqueDB, R2Backend } from 'parquedb'
import { createParqueDBMiddleware } from 'parquedb/integrations/ai-sdk'
import { wrapLanguageModel } from 'ai'
import { openai } from '@ai-sdk/openai'

export function createAIModel(env: Env) {
  const db = new ParqueDB({
    storage: new R2Backend(env.AI_STORAGE_BUCKET),
  })

  const middleware = createParqueDBMiddleware({
    db,
    cache: {
      enabled: true,
      ttlSeconds: 86400, // 24 hours
      collection: 'ai_cache',
    },
    logging: {
      enabled: true,
      level: 'standard',
      collection: 'ai_logs',
      metadata: {
        environment: env.ENVIRONMENT,
        version: env.VERSION,
      },
    },
  })

  return wrapLanguageModel({
    model: openai('gpt-4', { apiKey: env.OPENAI_API_KEY }),
    middleware,
  })
}
```

### Monitoring Dashboard

```typescript
import { queryLogEntries, getCacheStats } from 'parquedb/integrations/ai-sdk'

async function getDashboardData(db: ParqueDB) {
  const [cacheStats, recentLogs, errorLogs] = await Promise.all([
    getCacheStats(db),
    queryLogEntries(db, { limit: 100 }),
    queryLogEntries(db, { errorsOnly: true, limit: 50 }),
  ])

  // Calculate metrics
  const totalRequests = recentLogs.length
  const cachedRequests = recentLogs.filter(l => l.cached).length
  const cacheHitRate = totalRequests > 0 ? cachedRequests / totalRequests : 0

  const avgLatency = recentLogs.reduce((sum, l) => sum + l.latencyMs, 0) / totalRequests
  const totalTokens = recentLogs.reduce((sum, l) => sum + (l.usage?.totalTokens ?? 0), 0)

  return {
    cacheStats,
    metrics: {
      totalRequests,
      cacheHitRate: `${(cacheHitRate * 100).toFixed(1)}%`,
      avgLatency: `${avgLatency.toFixed(0)}ms`,
      totalTokens,
      errorCount: errorLogs.length,
    },
    recentErrors: errorLogs.slice(0, 10),
  }
}
```

### Scheduled Cache Cleanup

```typescript
import { clearExpiredCache, getCacheStats } from 'parquedb/integrations/ai-sdk'

// Cloudflare Workers scheduled handler
export default {
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    const db = new ParqueDB({
      storage: new R2Backend(env.AI_STORAGE_BUCKET),
    })

    const statsBefore = await getCacheStats(db)
    const deleted = await clearExpiredCache(db)
    const statsAfter = await getCacheStats(db)

    console.log({
      message: 'Cache cleanup completed',
      deletedEntries: deleted,
      entriesBefore: statsBefore.totalEntries,
      entriesAfter: statsAfter.totalEntries,
    })
  },
}
```

## Materialized Views Integration

ParqueDB's AI SDK integration includes **stream collections** that can be used as sources for materialized views (MVs). This enables powerful analytics capabilities like daily usage tracking, error monitoring, and cost analysis.

### Stream Collections

Import the `AIRequests` and `Generations` stream collections from `parquedb/ai-sdk`:

```typescript
import { DB } from 'parquedb'
import { AIRequests, Generations, createParqueDBMiddleware } from 'parquedb/ai-sdk'
import { wrapLanguageModel, generateText } from 'ai'
import { openai } from '@ai-sdk/openai'

const db = DB({
  // Stream collections - auto-wire ingestion via $ingest: 'ai-sdk'
  AIRequests,
  Generations,

  // Your other collections...
  User: {
    email: 'string!',
    name: 'string?',
  },
}, {
  storage: { type: 'fs', path: './data' },
})
```

The `$ingest: 'ai-sdk'` directive on these collections automatically wires up the middleware to write request/generation data when included in your schema.

### Example Schema with MVs

Combine stream collections with materialized views for AI analytics:

```typescript
import { DB } from 'parquedb'
import { AIRequests, Generations, createParqueDBMiddleware } from 'parquedb/ai-sdk'

const db = DB({
  // =========================================================================
  // Stream Collections (imported - handle ingestion automatically)
  // =========================================================================
  AIRequests,     // All AI SDK requests (generate/stream)
  Generations,    // Generated text/objects

  // =========================================================================
  // Materialized Views (use $from - computed from stream collections)
  // =========================================================================

  // Daily usage aggregates by model
  DailyAIUsage: {
    $from: 'AIRequests',
    $groupBy: [{ date: '$timestamp' }, 'modelId'],
    $compute: {
      requestCount: { $count: '*' },
      totalTokens: { $sum: 'tokens' },
      avgLatency: { $avg: 'latencyMs' },
      cacheHitRate: { $avg: { $cond: ['$cached', 1, 0] } },
      errorCount: { $sum: { $cond: [{ $exists: '$error' }, 1, 0] } },
    },
  },

  // AI errors for alerting and debugging
  AIErrors: {
    $from: 'AIRequests',
    $filter: { error: { $exists: true } },
  },

  // Generated objects (filtered from all generations)
  GeneratedObjects: {
    $from: 'Generations',
    $filter: { contentType: 'object' },
  },

}, {
  storage: { type: 'fs', path: './data' },
})
```

### How It Works

1. **Stream Collections**: `AIRequests` and `Generations` are regular collections with a special `$ingest: 'ai-sdk'` directive
2. **Auto-wiring**: When these collections are in your schema, `createParqueDBMiddleware({ db })` automatically writes to them
3. **MVs via $from**: Collections with `$from` are materialized views - they compute data from source collections
4. **Streaming Refresh**: MVs update automatically when source data changes (default mode)

### Wiring Up the Middleware

After defining your schema with stream collections, create the middleware:

```typescript
// Middleware automatically writes to AIRequests and Generations
const middleware = createParqueDBMiddleware({
  db,
  cache: { enabled: true, ttlSeconds: 3600 },
  logging: { enabled: true, level: 'standard' },
})

const model = wrapLanguageModel({
  model: openai('gpt-4'),
  middleware,
})

// Use the model - data flows to collections and MVs automatically
const result = await generateText({ model, prompt: 'Hello!' })
```

### Querying AI Analytics

Query the MVs just like any other collection:

```typescript
// Get daily usage for a specific model
const gpt4Usage = await db.DailyAIUsage.find({
  modelId: 'gpt-4',
  date: { $gte: '2024-01-01', $lte: '2024-01-31' },
})

// Get all errors from the last 24 hours
const recentErrors = await db.AIErrors.find({
  timestamp: { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) },
})

// Get generated objects
const objects = await db.GeneratedObjects.find({
  modelId: 'gpt-4',
})

// Calculate totals across days
const januaryUsage = await db.DailyAIUsage.find({
  date: { $gte: '2024-01-01', $lt: '2024-02-01' },
})
const totalTokens = januaryUsage.reduce((sum, day) => sum + day.totalTokens, 0)
const avgCacheHitRate = januaryUsage.reduce((sum, day) => sum + day.cacheHitRate, 0) / januaryUsage.length
```

### Available MV Patterns

| MV Pattern | Use Case | Key Fields |
|------------|----------|------------|
| `DailyAIUsage` | Cost tracking, usage trends | `date`, `modelId`, `requestCount`, `totalTokens` |
| `AIErrors` | Error monitoring, alerting | `error`, `modelId`, `timestamp` |
| `GeneratedObjects` | Structured output analytics | `contentType`, `content`, `modelId` |
| `CacheHitRates` | Cache effectiveness | `modelId`, `hitRate`, `cachedRequests` |
| `ModelErrorRates` | Reliability tracking | `modelId`, `errorRate`, `totalRequests` |

### Stream Collection Schemas

**AIRequests** - Captures all AI SDK requests:

| Field | Type | Description |
|-------|------|-------------|
| `modelId` | `string!` | Model ID (e.g., 'gpt-4') |
| `providerId` | `string!` | Provider ID (e.g., 'openai') |
| `requestType` | `string!` | 'generate' or 'stream' |
| `tokens` | `int?` | Total tokens used |
| `latencyMs` | `int!` | Request latency |
| `cached` | `boolean!` | Whether from cache |
| `error` | `variant?` | Error info if failed |
| `timestamp` | `timestamp!` | Request timestamp |

**Generations** - Captures generated content:

| Field | Type | Description |
|-------|------|-------------|
| `modelId` | `string!` | Model ID |
| `contentType` | `string!` | 'text' or 'object' |
| `content` | `variant!` | Generated content |
| `prompt` | `string?` | Original prompt |
| `tokens` | `int?` | Tokens used |
| `timestamp` | `timestamp!` | Generation timestamp |

## Best Practices

### 1. Use Appropriate Cache TTL

- Short TTL (1-4 hours) for frequently changing content
- Long TTL (24+ hours) for stable, reference content
- Consider your API costs vs freshness requirements

### 2. Exclude Non-Deterministic Parameters

For reproducible caching, exclude parameters that don't affect the core response:

```typescript
excludeFromKey: ['temperature', 'seed', 'topP', 'presencePenalty']
```

### 3. Choose the Right Log Level

| Environment | Recommended Level |
|-------------|-------------------|
| Development | `verbose` |
| Staging | `standard` |
| Production | `minimal` or `standard` |

### 4. Set Up Regular Cache Cleanup

Schedule periodic cleanup of expired entries to keep storage costs low:

```typescript
// Run daily
cron.schedule('0 0 * * *', async () => {
  await clearExpiredCache(db)
})
```

### 5. Monitor Cache Performance

Track cache hit rates and adjust TTL accordingly:

```typescript
const stats = await getCacheStats(db)
const logs = await queryLogEntries(db, { since: new Date(Date.now() - 86400000) })
const hitRate = logs.filter(l => l.cached).length / logs.length

if (hitRate < 0.3) {
  console.warn('Low cache hit rate, consider increasing TTL')
}
```
