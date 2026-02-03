# Materialized Views API Reference

This document covers the API for creating, querying, and managing Materialized Views in ParqueDB.

For conceptual overview and architecture, see [Architecture: Materialized Views](../architecture/materialized-views.md).

## Table of Contents

- [Creating Views](#creating-views)
- [Querying Views](#querying-views)
- [View Maintenance](#view-maintenance)
- [Refresh Modes](#refresh-modes)
- [Aggregation Functions](#aggregation-functions)
- [Stream Collections](#stream-collections)
- [Runtime Adapters](#runtime-adapters)

---

## Creating Views

### Inline in DB(schema)

MVs are defined inline with `$from` directive:

```typescript
const db = DB({
  // Regular collection
  Order: {
    total: 'decimal(10,2)!',
    status: 'string!',
    customer: '-> Customer',
  },

  // Materialized View (has $from)
  OrderAnalytics: {
    $from: 'Order',
    $expand: ['customer'],
    $flatten: { 'customer': 'buyer' },
  },
}, { storage })
```

### Standalone defineView()

For reusable/shareable MV definitions:

```typescript
import { defineView } from 'parquedb'

export const OrderAnalytics = defineView({
  $from: 'Order',
  $expand: ['customer', 'items.product'],
  $flatten: { 'customer': 'buyer' },
  $compute: {
    totalItems: { $count: 'items' },
    orderValue: { $sum: 'items.price' },
  },
})

// Use in any DB
const db = DB({
  Order: { /* ... */ },
  OrderAnalytics,
}, { storage })
```

### Register Programmatically

```typescript
// Define and register a view
const view = db.defineView({
  $type: 'MyView',
  $from: 'MyCollection',
  $refresh: { mode: 'streaming' }
})

// Or register from schema
await db.registerView(myViewDefinition)
```

---

## Querying Views

### Direct Query

```typescript
const results = await db.views.MyView.find({ status: 'active' })
```

### With Options

```typescript
const results = await db.views.MyView.find(
  { status: 'active' },
  {
    sort: { createdAt: -1 },
    limit: 100,
    asOf: new Date('2024-01-15')  // Time-travel
  }
)
```

### SQL Queries

```typescript
const errorRate = await db.sql`
  SELECT scriptName, COUNT(*) as total,
         SUM(CASE WHEN outcome != 'ok' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as error_rate
  FROM WorkerRequests
  WHERE timestamp > NOW() - INTERVAL '1 hour'
  GROUP BY scriptName
  ORDER BY error_rate DESC
`
```

### Query Routing

Queries automatically use MVs when beneficial:

```typescript
// Original query
const result = await db.Orders.find({
  status: 'completed'
}, {
  include: ['customer', 'items.product'],
  sort: { createdAt: -1 }
})

// Query planner checks:
// 1. Is there an MV that covers this query?
// 2. Is the MV fresh enough (within grace period)?
// 3. Is it more efficient to use MV vs source?

// If MV exists and is fresh, rewrites to:
const result = await db.views.OrderAnalyticsView.find({
  status: 'completed'
}, {
  sort: { createdAt: -1 }
})
```

### Grace Period

For stale MVs, a grace period allows continued use:

```typescript
$refresh: {
  mode: 'scheduled',
  schedule: '0 * * * *',
  gracePeriod: '15m',  // Allow 15 min stale data
}
```

---

## View Maintenance

### Manual Refresh

```typescript
await db.refreshView('MyView')
await db.refreshView('MyView', { full: true })  // Force full refresh
```

### Check Staleness

```typescript
const state = await db.getViewState('MyView')
// { state: 'stale', lastRefresh: Date, staleSince: Date }
```

### List All Views

```typescript
const views = await db.listViews()
// [{ name: 'MyView', state: 'fresh', ... }, ...]
```

### Drop a View

```typescript
await db.dropView('MyView')
```

### Alter Refresh Settings

```typescript
await db.alterView('MyView', {
  $refresh: {
    mode: 'scheduled',
    schedule: '0 */6 * * *'  // Every 6 hours
  }
})
```

### Vacuum Old Snapshots

```typescript
await db.vacuumView('MyView', { retentionDays: 7 })
```

---

## Refresh Modes

### Streaming (Default)

Updates immediately when source changes:

```typescript
OrderAnalytics: {
  $from: 'Order',
  $expand: ['customer'],
  // $refresh: { mode: 'streaming' }  // implied
}
```

Implementation uses CDC event log:

```typescript
interface MVTrigger {
  sourceNs: string           // 'orders'
  eventTypes: EventOp[]      // ['CREATE', 'UPDATE', 'DELETE']
  mvName: string             // 'RealtimeOrdersView'
  refreshFn: (events: Event[]) => Promise<void>
}
```

### Scheduled

Refreshes on cron schedule:

```typescript
DailySales: {
  $from: 'Order',
  $groupBy: [{ date: '$createdAt' }],
  $compute: { revenue: { $sum: 'total' } },
  $refresh: {
    mode: 'scheduled',
    schedule: '0 * * * *',  // hourly
    timezone: 'UTC',
    strategy: 'replace',    // 'replace' | 'append'
  },
}
```

Implementation uses DO Alarms:

```typescript
async alarm() {
  const mvs = await this.getMVsDueForRefresh()
  for (const mv of mvs) {
    await this.refreshMV(mv)
  }
}
```

### Manual

Only refreshes when explicitly called:

```typescript
AdHocReport: {
  $from: 'Order',
  $refresh: { mode: 'manual' },
}

// Refresh via API
await db.refreshView('AdHocReportView')
```

---

## Incremental vs Full Refresh

### Incremental (Preferred)

Only processes delta changes since last refresh.

**Requirements**:
- Source operations are INSERT-only (no UPDATE/DELETE)
- Aggregates limited to: SUM, MIN, MAX, COUNT, AVG
- No compaction occurred on source tables since last refresh

```typescript
async function incrementalRefresh(mv: MaterializedView) {
  const lastSnapshot = mv.lineage.sourceSnapshots.get(mv.sourceName)
  const currentSnapshot = await getLatestSnapshot(mv.sourceName)

  if (lastSnapshot === currentSnapshot) {
    return // Already fresh
  }

  // Read only delta since last snapshot
  const delta = await readChanges(mv.sourceName, {
    fromSnapshot: lastSnapshot,
    toSnapshot: currentSnapshot
  })

  // Process and append delta
  const processed = await processProjection(delta, mv.definition)
  await mv.storageTable.append(processed)

  // Update lineage
  mv.lineage.sourceSnapshots.set(mv.sourceName, currentSnapshot)
  mv.lineage.refreshVersionId = generateVersionId()
}
```

### Full Refresh (Fallback)

Recomputes entire MV from scratch.

**Triggers**:
- Source had UPDATE/DELETE operations
- Aggregates require full scan (STDDEV, VARIANCE, etc.)
- Source was compacted since last refresh
- Schema evolution occurred

```typescript
async function fullRefresh(mv: MaterializedView) {
  // Read all source data
  const allData = await readAll(mv.sourceName)

  // Process entire dataset
  const processed = await processProjection(allData, mv.definition)

  // Atomically replace storage table
  await mv.storageTable.replace(processed)

  // Update lineage
  mv.lineage.sourceSnapshots = await getCurrentSnapshots(mv.sources)
  mv.lineage.refreshVersionId = generateVersionId()
}
```

---

## Aggregation Functions

### Pre-computed Aggregates

```typescript
const OrderMetrics = defineView({
  $type: 'OrderMetricsView',
  $from: 'Order',

  // Group by dimensions
  $groupBy: ['status', 'customer.tier', { day: '$createdAt' }],

  // Compute aggregates
  $compute: {
    orderCount: { $count: '*' },
    totalRevenue: { $sum: 'total' },
    avgOrderValue: { $avg: 'total' },
    minOrder: { $min: 'total' },
    maxOrder: { $max: 'total' },
  }
})
```

### Supported Functions

| Function | Incremental | Description |
|----------|-------------|-------------|
| `$count` | Yes | Count of records |
| `$sum` | Yes | Sum of values |
| `$min` | Yes | Minimum value |
| `$max` | Yes | Maximum value |
| `$avg` | Yes* | Average (stored as sum/count) |
| `$first` | No | First value |
| `$last` | No | Last value |
| `$stddev` | No | Standard deviation |
| `$variance` | No | Variance |

*AVG is incrementally refreshable by storing sum and count separately.

---

## Expansion Directives

### $expand

Inline related entities (denormalization):

```typescript
$expand: [
  'customer',                    // Adds customer_* fields
  'customer.address',            // Nested: customer_address_* fields
  'items.product',               // Array relation: items_product_* fields
  'items.product.category'       // 3-level nesting
]
```

### $flatten

Rename expanded field prefixes:

```typescript
$flatten: {
  'customer': 'buyer',           // customer_name -> buyer_name
  'customer.address': 'shipping' // customer_address_city -> shipping_city
}
```

---

## Stream Collections

External data sources are imported as **stream collections** - regular collections that handle ingestion automatically.

### How Stream Collections Work

```typescript
// parquedb/tail exports the TailEvents stream collection
export const TailEvents = {
  $type: 'TailEvent',
  scriptName: 'string!',
  outcome: 'string!',
  eventTimestamp: 'timestamp!',
  event: 'variant?',
  logs: 'variant[]',
  exceptions: 'variant[]',
  $ingest: 'tail',  // Internal: wires up tail handler ingestion
}

// Use in your schema - ingestion happens automatically
const db = DB({
  TailEvents,  // Import the stream collection

  // Create MVs from it
  WorkerErrors: {
    $from: 'TailEvents',
    $filter: { outcome: { $ne: 'ok' } },
  },
}, { storage })
```

When you include a stream collection in your schema:
1. ParqueDB creates the collection
2. The `$ingest` directive wires up the data source
3. Data flows in automatically (via tail handler, middleware, etc.)
4. MVs with `$from` can reference it like any other collection

### Available Stream Collections

#### parquedb/ai-sdk

```typescript
export const AIRequests = {
  $type: 'AIRequest',
  modelId: 'string!',
  providerId: 'string!',
  requestType: 'string!',     // 'generate' | 'stream'
  tokens: 'int?',
  latencyMs: 'int!',
  cached: 'boolean!',
  error: 'variant?',
  timestamp: 'timestamp!',
  $ingest: 'ai-sdk',
}

export const Generations = {
  $type: 'Generation',
  modelId: 'string!',
  contentType: 'string!',     // 'text' | 'object'
  content: 'variant!',
  prompt: 'string?',
  tokens: 'int?',
  timestamp: 'timestamp!',
  $ingest: 'ai-sdk',
}
```

#### parquedb/tail

```typescript
export const TailEvents = {
  $type: 'TailEvent',
  scriptName: 'string!',
  outcome: 'string!',
  event: 'variant?',
  logs: 'variant[]',
  exceptions: 'variant[]',
  timestamp: 'timestamp!',
  $ingest: 'tail',
}
```

#### parquedb/evalite

```typescript
export const EvalRuns = {
  $type: 'EvalRun',
  runId: 'int!',
  runType: 'string!',
  timestamp: 'timestamp!',
  $ingest: 'evalite',
}

export const EvalScores = {
  $type: 'EvalScore',
  runId: 'int!',
  suiteName: 'string!',
  scorerName: 'string!',
  score: 'decimal(5,4)!',
  timestamp: 'timestamp!',
  $ingest: 'evalite',
}
```

---

## Runtime Adapters

For Node.js and non-Workers environments, stream adapters enable local MV processing.

### EventEmitter Adapter (Node.js)

```typescript
import { createStreamAdapter } from 'parquedb'

const nodeAdapter = createStreamAdapter({
  type: 'node',
  emitter: myEventEmitter,
  event: 'data',
})
```

### Async Iterator Adapter

```typescript
const iteratorAdapter = createStreamAdapter({
  type: 'iterator',
  source: async function* () {
    for await (const event of myAsyncSource) {
      yield event
    }
  },
})
```

### Polling Adapter

```typescript
const pollingAdapter = createStreamAdapter({
  type: 'poll',
  fetch: async () => fetchLatestEvents(),
  intervalMs: 1000,
})
```

### Stream Processor

```typescript
const processor = db.createStreamProcessor({
  views: [WorkerLogs, WorkerErrors, WorkerRequests],
  adapter: nodeAdapter,
  batchSize: 100,
  flushIntervalMs: 5000,
})

await processor.start()

// Later
await processor.stop()
```

---

## Cloudflare Workers Integration

### Streaming MVs with Durable Objects

```typescript
// In ParqueDBDO
async handleWrite(ns: string, op: EventOp, entity: Entity) {
  // Log event
  await this.eventLog.append({ op, target: `${ns}:${entity.$id}`, ... })

  // Trigger streaming MVs
  const mvs = this.getStreamingMVs(ns)
  for (const mv of mvs) {
    await this.enqueueMVUpdate(mv, { op, entity })
  }
}

// Process MV updates (batched for efficiency)
async processMVUpdates() {
  const batch = await this.dequeueMVUpdates()
  for (const [mvName, events] of groupByMV(batch)) {
    await this.refreshMVIncremental(mvName, events)
  }
}
```

### Scheduled MVs with Alarms

```typescript
// Schedule refresh via alarm
async scheduleRefresh(mvName: string, schedule: string) {
  const nextRun = parseSchedule(schedule).next()
  await this.state.storage.setAlarm(nextRun.getTime())
}

async alarm() {
  const mvs = await this.getMVsDueForRefresh()
  for (const mv of mvs) {
    await this.refreshMV(mv.name)
  }
  await this.scheduleNextAlarms()
}
```

---

## MV Metadata Schema

```typescript
interface MVMetadata {
  viewId: string
  name: string

  definition: {
    $type: string
    $from: string
    $expand?: string[]
    $flatten?: Record<string, string>
    $filter?: Filter
    $groupBy?: (string | Record<string, string>)[]
    $compute?: Record<string, AggregateExpr>
  }

  refresh: {
    mode: 'streaming' | 'scheduled' | 'manual'
    schedule?: string
    gracePeriod?: string
    strategy?: 'replace' | 'append'
  }

  lineage: MVLineage
  schema: IcebergSchema
  storageTableLocation: string
}

interface MVLineage {
  sourceSnapshots: Map<string, SnapshotId>
  refreshVersionId: string
  lastRefreshTime: Date
}
```

---

## Performance Characteristics

| Operation | Target Latency | Notes |
|-----------|---------------|-------|
| Query fresh MV | 5-20ms | Direct Parquet read |
| Query stale MV (with grace) | 5-20ms | Uses cached data |
| Query stale MV (no grace) | 50-500ms | Falls back to source |
| Streaming refresh (incremental) | 10-50ms | Per event batch |
| Streaming refresh (full) | 100ms-10s | Depends on data size |
| Scheduled refresh | 1s-60s | Background, non-blocking |

---

## Related Documentation

- [Architecture: Materialized Views](../architecture/materialized-views.md) - Core concepts and design
