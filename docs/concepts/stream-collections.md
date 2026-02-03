---
title: Stream Collections
description: Understanding stream collections and the $ingest directive for automatic data ingestion
---

Stream collections are specialized collections that automatically ingest data from external sources. They bridge the gap between external data producers (AI SDKs, Cloudflare Workers, evaluation frameworks) and ParqueDB's query and analytics capabilities.

## Stream Collections vs Regular Collections

| Aspect | Regular Collections | Stream Collections |
|--------|--------------------|--------------------|
| **Data Source** | Direct CRUD operations | Automatic ingestion from external sources |
| **Creation** | Define schema inline | Import from integration packages |
| **Updates** | `create()`, `update()`, `delete()` | Data flows in automatically |
| **Use Case** | Application data | Observability, logging, AI telemetry |

Regular collections store your application's core data - users, orders, products. You control every write.

Stream collections capture external data streams - AI requests, worker logs, evaluation scores. The integration handles ingestion; you focus on analysis.

```typescript
import { DB } from 'parquedb'

// Import stream collections from integrations
import { AIRequests, Generations } from 'parquedb/ai-sdk'
import { TailEvents } from 'parquedb/tail'
import { EvalRuns, EvalScores } from 'parquedb/evalite'

const db = DB({
  // Regular collections - you write to these
  User: {
    email: 'string!',
    name: 'string',
  },

  // Stream collections - data flows in automatically
  AIRequests,
  Generations,
  TailEvents,
  EvalRuns,
  EvalScores,
})
```

## The $ingest Directive

Stream collections use the `$ingest` directive to declare their data source. This tells ParqueDB how to wire up automatic ingestion.

```typescript
// What parquedb/ai-sdk exports internally
export const AIRequests = {
  $type: 'AIRequest',
  modelId: 'string!',
  providerId: 'string!',
  requestType: 'string!',
  tokens: 'int?',
  latencyMs: 'int!',
  cached: 'boolean!',
  error: 'variant?',
  timestamp: 'timestamp!',

  $ingest: 'ai-sdk',  // Wires up middleware ingestion
}
```

When you include a collection with `$ingest` in your schema:

1. ParqueDB creates the collection
2. The integration's ingestion handler is activated
3. Data flows automatically from the source to the collection
4. The collection is queryable like any other collection

## Available Stream Collections

### parquedb/ai-sdk

For AI SDK (Vercel AI) observability:

```typescript
import { AIRequests, Generations } from 'parquedb/ai-sdk'
```

| Collection | Description |
|------------|-------------|
| `AIRequests` | All AI SDK requests with model, latency, tokens, errors |
| `Generations` | Generated text and objects with content type |

### parquedb/tail

For Cloudflare Workers tail events:

```typescript
import { TailEvents } from 'parquedb/tail'
```

| Collection | Description |
|------------|-------------|
| `TailEvents` | Worker invocations with logs, exceptions, request/response data |

### parquedb/evalite

For Evalite evaluation framework:

```typescript
import { EvalRuns, EvalScores } from 'parquedb/evalite'
```

| Collection | Description |
|------------|-------------|
| `EvalRuns` | Evaluation run metadata |
| `EvalScores` | Individual scores from evaluation runs |

## Stream Collections and Materialized Views

Stream collections become powerful when combined with Materialized Views. The `$from` directive in MVs can reference any collection, including stream collections.

```typescript
const db = DB({
  // Stream collection (imported)
  AIRequests,

  // MV that aggregates the stream data
  DailyAIUsage: {
    $from: 'AIRequests',  // References the stream collection
    $groupBy: ['modelId', { date: '$timestamp' }],
    $compute: {
      requestCount: { $count: '*' },
      totalTokens: { $sum: 'tokens' },
      avgLatency: { $avg: 'latencyMs' },
    },
  },
})
```

This pattern separates concerns:

- **Stream collection**: Raw event ingestion (high volume, append-only)
- **Materialized View**: Aggregated analytics (pre-computed, fast queries)

## Creating Custom Stream Collections

You can create custom stream collections for your own data sources using the `$ingest` directive.

### Define the Collection Schema

```typescript
// my-stream.ts
export const MyEvents = {
  $type: 'MyEvent',
  eventType: 'string!',
  payload: 'variant!',
  source: 'string!',
  timestamp: 'timestamp!',

  $ingest: 'my-custom-source',
}
```

### Register the Ingestion Handler

```typescript
import { registerIngestHandler } from 'parquedb'

registerIngestHandler('my-custom-source', {
  // Called when schema is initialized
  setup: async (db, collectionName) => {
    // Set up your data source connection
  },

  // Called to start ingestion
  start: async (db, collectionName) => {
    // Begin streaming data to db[collectionName].create()
  },

  // Called on shutdown
  stop: async () => {
    // Clean up resources
  },
})
```

### Use in Your Schema

```typescript
import { MyEvents } from './my-stream'

const db = DB({
  MyEvents,

  // Create MVs from your custom stream
  MyEventsByType: {
    $from: 'MyEvents',
    $groupBy: ['eventType', { hour: '$timestamp' }],
    $compute: { count: { $count: '*' } },
  },
})
```

## Complete Example: AI Observability Pipeline

Here is a full example showing the flow from import to query.

### 1. Define the Schema

```typescript
// db.ts
import { DB } from 'parquedb'
import { AIRequests, Generations } from 'parquedb/ai-sdk'

export const db = DB({
  // Stream collections (imported)
  AIRequests,
  Generations,

  // MVs for analytics
  DailyUsage: {
    $from: 'AIRequests',
    $groupBy: ['modelId', 'providerId', { date: '$timestamp' }],
    $compute: {
      requests: { $count: '*' },
      tokens: { $sum: 'tokens' },
      avgLatency: { $avg: 'latencyMs' },
      errors: { $sum: { $cond: [{ $exists: '$error' }, 1, 0] } },
    },
    $refresh: { mode: 'streaming' },
  },

  ErrorLog: {
    $from: 'AIRequests',
    $filter: { error: { $exists: true } },
  },
})
```

### 2. Wire Up AI SDK Middleware

```typescript
// ai.ts
import { createParqueDBMiddleware } from 'parquedb/ai-sdk'
import { wrapLanguageModel, generateText } from 'ai'
import { openai } from '@ai-sdk/openai'
import { db } from './db'

const middleware = createParqueDBMiddleware({ db })

const model = wrapLanguageModel({
  model: openai('gpt-4'),
  middleware,
})

// Every call automatically logs to AIRequests and Generations
const result = await generateText({
  model,
  prompt: 'Explain stream collections in one sentence.',
})
```

### 3. Query Analytics

```typescript
// Query the aggregated MV
const usage = await db.DailyUsage.find({
  modelId: 'gpt-4',
  date: { $gte: '2024-01-01' },
})

// Query raw stream data
const recentErrors = await db.ErrorLog.find({
  timestamp: { $gte: new Date(Date.now() - 3600000) },
})

// SQL query across MVs
const costByModel = await db.sql`
  SELECT modelId, SUM(tokens) as totalTokens
  FROM DailyUsage
  WHERE date >= '2024-01-01'
  GROUP BY modelId
  ORDER BY totalTokens DESC
`
```

## Key Concepts Summary

- **Stream collections** handle automatic ingestion from external sources
- The **$ingest directive** declares the data source and wires up ingestion
- **Import** stream collections from integration packages (`parquedb/ai-sdk`, `parquedb/tail`, `parquedb/evalite`)
- Stream collections are **sources for MVs** via the `$from` directive
- Create **custom stream collections** for your own data sources
- Query stream collections and their MVs like any other ParqueDB collection

## Related Documentation

- [Materialized Views](/docs/architecture/materialized-views) - Full MV design and capabilities
- [AI SDK Integration](/docs/integrations/ai-sdk) - AI SDK middleware setup
- [Tail Integration](/docs/integrations/tail) - Cloudflare Workers tail events
- [Evalite Integration](/docs/integrations/evalite) - Evaluation framework integration
