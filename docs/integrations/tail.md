# Tail Worker Integration

ParqueDB provides first-class support for ingesting [Cloudflare Workers Tail Events](https://developers.cloudflare.com/workers/observability/logs/tail-workers/) into Parquet files. This enables powerful observability, analytics, and debugging capabilities for your Workers applications.

## Overview

Cloudflare Workers tail events capture comprehensive runtime information:

- **Request/Response data** - URL, method, status, headers
- **Console logs** - All `console.log/warn/error` output
- **Exceptions** - Uncaught errors with stack traces
- **Outcome** - Success, exception, exceeded CPU/memory limits
- **Timing** - Request duration and timestamps

ParqueDB captures these events into the `TailEvents` stream collection, which can then power materialized views for logs, errors, and request analytics.

## Architecture

```
┌─────────────────────┐    ┌──────────────────────────────────────────────┐
│  Application        │    │  Tail Worker                                  │
│  Workers            │───▶│                                              │
│  (producers)        │    │  ┌────────────────────────────────────────┐  │
└─────────────────────┘    │  │  TailEvent[] batch → db.TailEvents     │  │
                           │  └────────────────────────────────────────┘  │
                           │              │                                │
                           │    ┌─────────┼─────────┐                     │
                           │    ▼         ▼         ▼                     │
                           │ ┌──────┐ ┌──────┐ ┌──────────┐               │
                           │ │ Logs │ │Errors│ │ Requests │  MVs          │
                           │ │      │ │      │ │          │  ($from)      │
                           │ └──────┘ └──────┘ └──────────┘               │
                           │    │         │         │                     │
                           │    └─────────┴─────────┘                     │
                           │              ▼                                │
                           │  ┌────────────────────────────────────────┐  │
                           │  │  ParqueDB (R2 Parquet files)           │  │
                           │  └────────────────────────────────────────┘  │
                           └──────────────────────────────────────────────┘
```

The tail worker is deployed separately from your main ParqueDB worker because:
1. A worker cannot be its own tail consumer (would create an infinite loop)
2. Tail workers have different scaling and deployment concerns
3. Pure ingestion workload, no query serving

## TailEvents Stream Collection

The `TailEvents` stream collection is imported from `parquedb/tail` and handles raw tail event ingestion:

```typescript
import { TailEvents } from 'parquedb/tail'
```

### Schema

```typescript
export const TailEvents = {
  $type: 'TailEvent',
  scriptName: 'string!',        // Worker script name
  outcome: 'string!',           // 'ok' | 'exception' | 'exceededCpu' | 'exceededMemory' | 'unknown'
  eventTimestamp: 'timestamp!', // When the event occurred
  event: 'variant?',            // { request, response } for fetch events
  logs: 'variant[]',            // Array of console log messages
  exceptions: 'variant[]',      // Array of uncaught exceptions
  diagnosticsChannelEvents: 'variant[]', // Diagnostics channel events
  $ingest: 'tail',              // Internal: wires up tail handler ingestion
}
```

### Event Variant Structure

The `event` field contains request/response information for fetch events:

```typescript
interface TailEventData {
  request?: {
    url: string
    method: string
    headers: Record<string, string>
    cf?: {
      colo: string
      country: string
      city?: string
      region?: string
      timezone?: string
      asn?: number
      // ... other CF properties
    }
  }
  response?: {
    status: number
  }
  scheduledTime?: number  // For cron triggers
  queue?: string          // For queue consumers
}
```

### Log Entry Structure

Each entry in the `logs` array:

```typescript
interface LogEntry {
  level: 'log' | 'debug' | 'info' | 'warn' | 'error'
  message: unknown[]  // Arguments passed to console method
  timestamp: number
}
```

### Exception Structure

Each entry in the `exceptions` array:

```typescript
interface ExceptionEntry {
  name: string
  message: string
  timestamp: number
}
```

## Pre-built Helper MVs

ParqueDB provides three commonly-used materialized views for tail events. Import them from `parquedb/tail`:

```typescript
import { TailEvents, WorkerLogs, WorkerErrors, WorkerRequests } from 'parquedb/tail'
```

### WorkerLogs

Extracts and flattens console logs from tail events:

```typescript
export const WorkerLogs = {
  $from: 'TailEvents',
  $unnest: 'logs',  // Flatten the logs array
  $select: {
    scriptName: '$scriptName',
    level: '$logs.level',
    message: '$logs.message',
    timestamp: '$logs.timestamp',
    colo: '$event.request.cf.colo',
  },
}
```

### WorkerErrors

Filters to events with errors or non-ok outcomes:

```typescript
export const WorkerErrors = {
  $from: 'TailEvents',
  $filter: {
    $or: [
      { outcome: { $ne: 'ok' } },
      { 'exceptions.0': { $exists: true } },
    ],
  },
  $select: {
    scriptName: '$scriptName',
    outcome: '$outcome',
    exceptionName: '$exceptions.0.name',
    exceptionMessage: '$exceptions.0.message',
    url: '$event.request.url',
    status: '$event.response.status',
    colo: '$event.request.cf.colo',
    timestamp: '$eventTimestamp',
  },
}
```

### WorkerRequests

Extracts HTTP request details for request analytics:

```typescript
export const WorkerRequests = {
  $from: 'TailEvents',
  $filter: { event: { $exists: true } },
  $select: {
    scriptName: '$scriptName',
    method: '$event.request.method',
    url: '$event.request.url',
    status: '$event.response.status',
    outcome: '$outcome',
    colo: '$event.request.cf.colo',
    country: '$event.request.cf.country',
    timestamp: '$eventTimestamp',
  },
}
```

## Complete Tail Worker Example

### Schema Definition

```typescript
// src/tail/db.ts
import { DB } from 'parquedb'
import { TailEvents, WorkerLogs, WorkerErrors, WorkerRequests } from 'parquedb/tail'

export const db = DB({
  // Stream collection - ingests raw tail events
  TailEvents,

  // Pre-built MVs
  WorkerLogs,
  WorkerErrors,
  WorkerRequests,

  // Custom MV: Aggregated metrics by endpoint
  EndpointMetrics: {
    $from: 'WorkerRequests',  // MV from another MV
    $groupBy: ['scriptName', 'method', { hour: '$timestamp' }],
    $compute: {
      requestCount: { $count: '*' },
      errorRate: { $avg: { $cond: [{ $ne: ['$outcome', 'ok'] }, 1, 0] } },
    },
    $refresh: { mode: 'scheduled', schedule: '0 * * * *' },
  },

  // Custom MV: Error rate by colo
  ColoErrors: {
    $from: 'WorkerErrors',
    $groupBy: ['colo', { hour: '$timestamp' }],
    $compute: {
      errorCount: { $count: '*' },
      uniqueScripts: { $countDistinct: 'scriptName' },
    },
  },

}, {
  storage: { type: 'r2', bucket: env.PARQUEDB },
})
```

### Tail Worker Implementation

```typescript
// src/tail/index.ts
import { db } from './db'

export interface Env {
  PARQUEDB: R2Bucket
}

export default {
  async tail(events: TraceItem[], env: Env, ctx: ExecutionContext) {
    // Transform Cloudflare trace items to TailEvents schema
    const tailEvents = events.map(event => ({
      scriptName: event.scriptName ?? 'unknown',
      outcome: event.outcome,
      eventTimestamp: new Date(event.eventTimestamp),
      event: event.event,
      logs: event.logs ?? [],
      exceptions: event.exceptions ?? [],
      diagnosticsChannelEvents: event.diagnosticsChannelEvents ?? [],
    }))

    // Write to the stream collection - MVs update automatically
    ctx.waitUntil(db.TailEvents.bulkCreate(tailEvents))
  },
}
```

### Tail Worker wrangler.toml

```toml
# wrangler.toml for the tail worker
name = "parquedb-tail"
main = "src/tail/index.ts"
compatibility_date = "2024-01-01"

[[r2_buckets]]
binding = "PARQUEDB"
bucket_name = "parquedb-data"
```

## Producer Worker Configuration

To send tail events to your ParqueDB tail worker, configure the `tail_consumers` in your application worker's `wrangler.toml`:

```toml
# wrangler.toml for your application worker
name = "my-app"
main = "src/index.ts"
compatibility_date = "2024-01-01"

# Send tail events to the ParqueDB tail worker
[[tail_consumers]]
service = "parquedb-tail"
```

### Multiple Producer Workers

You can configure multiple workers to send events to the same tail worker:

```toml
# api-worker/wrangler.toml
name = "api-worker"
[[tail_consumers]]
service = "parquedb-tail"
```

```toml
# auth-worker/wrangler.toml
name = "auth-worker"
[[tail_consumers]]
service = "parquedb-tail"
```

```toml
# webhook-worker/wrangler.toml
name = "webhook-worker"
[[tail_consumers]]
service = "parquedb-tail"
```

All events are collected in the same `TailEvents` collection, distinguished by `scriptName`.

## Querying the Data

### Using find()

```typescript
// Get recent errors
const errors = await db.WorkerErrors.find({
  timestamp: { $gte: new Date(Date.now() - 3600000) },  // Last hour
})

// Get errors for a specific worker
const apiErrors = await db.WorkerErrors.find({
  scriptName: 'api-worker',
  outcome: 'exception',
})

// Get requests by status code
const serverErrors = await db.WorkerRequests.find({
  status: { $gte: 500 },
  timestamp: { $gte: new Date('2024-01-15') },
})

// Get logs by level
const errorLogs = await db.WorkerLogs.find({
  level: 'error',
  scriptName: 'my-worker',
})

// Get endpoint metrics
const metrics = await db.EndpointMetrics.find({
  scriptName: 'api-worker',
  hour: { $gte: new Date('2024-01-15') },
})
```

### Using SQL

```typescript
// Error rate by worker
const errorRates = await db.sql`
  SELECT
    scriptName,
    COUNT(*) as total,
    SUM(CASE WHEN outcome != 'ok' THEN 1 ELSE 0 END) as errors,
    ROUND(SUM(CASE WHEN outcome != 'ok' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as error_rate
  FROM WorkerRequests
  WHERE timestamp > NOW() - INTERVAL '1 hour'
  GROUP BY scriptName
  ORDER BY error_rate DESC
`

// Top endpoints by request count
const topEndpoints = await db.sql`
  SELECT
    scriptName,
    method,
    url,
    COUNT(*) as request_count,
    AVG(CASE WHEN outcome != 'ok' THEN 1 ELSE 0 END) as error_rate
  FROM WorkerRequests
  WHERE timestamp > NOW() - INTERVAL '24 hours'
  GROUP BY scriptName, method, url
  ORDER BY request_count DESC
  LIMIT 20
`

// Recent exceptions with context
const recentExceptions = await db.sql`
  SELECT
    scriptName,
    exceptionName,
    exceptionMessage,
    url,
    colo,
    timestamp
  FROM WorkerErrors
  WHERE exceptionName IS NOT NULL
    AND timestamp > NOW() - INTERVAL '1 hour'
  ORDER BY timestamp DESC
  LIMIT 50
`

// Requests by geographic region
const geoBreakdown = await db.sql`
  SELECT
    country,
    colo,
    COUNT(*) as requests,
    AVG(CASE WHEN status >= 500 THEN 1 ELSE 0 END) as error_rate
  FROM WorkerRequests
  WHERE timestamp > NOW() - INTERVAL '24 hours'
  GROUP BY country, colo
  ORDER BY requests DESC
`

// Log message search
const debugLogs = await db.sql`
  SELECT
    scriptName,
    level,
    message,
    timestamp
  FROM WorkerLogs
  WHERE level IN ('warn', 'error')
    AND timestamp > NOW() - INTERVAL '1 hour'
  ORDER BY timestamp DESC
  LIMIT 100
`
```

## Custom Materialized Views

Beyond the pre-built MVs, you can create custom views for your specific needs:

### Request Duration Percentiles

```typescript
RequestLatency: {
  $from: 'TailEvents',
  $filter: { 'event.request': { $exists: true } },
  $groupBy: ['scriptName', { hour: '$eventTimestamp' }],
  $compute: {
    p50: { $percentile: ['duration', 0.5] },
    p95: { $percentile: ['duration', 0.95] },
    p99: { $percentile: ['duration', 0.99] },
    requestCount: { $count: '*' },
  },
  $refresh: { mode: 'scheduled', schedule: '0 * * * *' },
}
```

### Error Clustering

```typescript
ErrorPatterns: {
  $from: 'WorkerErrors',
  $groupBy: ['scriptName', 'exceptionName', 'exceptionMessage'],
  $compute: {
    occurrences: { $count: '*' },
    firstSeen: { $min: 'timestamp' },
    lastSeen: { $max: 'timestamp' },
    affectedColos: { $countDistinct: 'colo' },
  },
}
```

### Status Code Distribution

```typescript
StatusCodes: {
  $from: 'WorkerRequests',
  $groupBy: ['scriptName', 'status', { day: '$timestamp' }],
  $compute: {
    count: { $count: '*' },
  },
}
```

## TypeScript Types

For full type safety when working with tail events:

```typescript
import type {
  TailEvent,
  TailEventData,
  LogEntry,
  ExceptionEntry,
  WorkerLog,
  WorkerError,
  WorkerRequest,
} from 'parquedb/tail'

// Type-safe queries
const errors: WorkerError[] = await db.WorkerErrors.find({
  outcome: 'exception',
}).items

// Type-safe event handling
function processEvent(event: TailEvent) {
  if (event.outcome !== 'ok') {
    console.log(`Error in ${event.scriptName}: ${event.outcome}`)
  }
}
```

## Best Practices

### 1. Filter Early

Create MVs that filter to relevant data to reduce storage and query costs:

```typescript
// Good: Filter to errors only
WorkerErrors: {
  $from: 'TailEvents',
  $filter: { outcome: { $ne: 'ok' } },
}

// Instead of filtering at query time
await db.TailEvents.find({ outcome: { $ne: 'ok' } })
```

### 2. Use Scheduled Refresh for Aggregates

Aggregated MVs don't need real-time updates:

```typescript
DailyMetrics: {
  $from: 'WorkerRequests',
  $groupBy: [{ day: '$timestamp' }],
  $compute: { /* ... */ },
  $refresh: { mode: 'scheduled', schedule: '0 * * * *' },  // Hourly
}
```

### 3. Partition by Time

For large volumes, consider time-based partitioning in your queries:

```typescript
// Always include time bounds
const recentErrors = await db.WorkerErrors.find({
  timestamp: { $gte: new Date(Date.now() - 86400000) },  // Last 24h
  scriptName: 'my-worker',
})
```

### 4. Sample High-Volume Events

For very high traffic workers, consider sampling:

```typescript
SampledRequests: {
  $from: 'TailEvents',
  $filter: {
    $expr: { $mod: [{ $hash: '$eventTimestamp' }, 10] },  // 10% sample
  },
}
```

## Troubleshooting

### Events Not Appearing

1. Verify the tail consumer is configured in your producer worker's `wrangler.toml`
2. Check that the tail worker is deployed and healthy
3. Verify R2 bucket bindings are correct

### High Latency

1. Use scheduled refresh for aggregate MVs
2. Add time bounds to queries
3. Consider sampling for high-volume workers

### Missing Data

1. Check for exceptions in the tail worker logs
2. Verify the schema matches the incoming event structure
3. Check R2 bucket permissions
