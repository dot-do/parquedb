# ParqueDB Materialized Views Design

## Overview

Materialized Views (MVs) in ParqueDB combine:
1. **IceType Directives** - Schema definition with `$from`, `$expand`, `$flatten`, `$compute`
2. **ClickHouse-style Refresh Modes** - Streaming (CDC-triggered) and Scheduled (cron-based)
3. **Backend-Agnostic Storage** - MVs work on Native, Iceberg, and Delta Lake backends

## Backend Support

MVs are **backend-agnostic** - the same MV definition works across all storage backends:

| Backend | Streaming MVs | Scheduled MVs | Staleness Detection |
|---------|--------------|---------------|---------------------|
| **Native** | CDC event log | DO Alarms | Event sequence IDs |
| **Iceberg** | CDC event log | DO Alarms | Snapshot IDs |
| **Delta Lake** | CDC event log | DO Alarms | Transaction log version |

The MV storage table uses the **same backend** as the source tables by default, or can be configured explicitly:

```typescript
$refresh: {
  mode: 'streaming',
  backend: 'iceberg',  // Override: store MV in Iceberg even if source is Native
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Materialized View                             │
├─────────────────────────────────────────────────────────────────┤
│  Definition (IceType Directives)                                │
│  ├── $from: 'Order'                                             │
│  ├── $expand: ['customer', 'items.product']                     │
│  ├── $flatten: { 'customer': 'buyer' }                          │
│  └── $compute: { totalItems: { $count: 'items' } }              │
├─────────────────────────────────────────────────────────────────┤
│  Storage Table (any backend)                                    │
│  ├── data/*.parquet (precomputed results)                       │
│  ├── metadata/ (snapshots, manifests)                           │
│  └── lineage: { sourceSnapshots, refreshVersionId }             │
├─────────────────────────────────────────────────────────────────┤
│  Refresh Strategy                                               │
│  ├── mode: 'streaming' | 'scheduled' | 'manual'                 │
│  ├── schedule?: '0 */1 * * *' (cron for scheduled)              │
│  └── incrementalEligible: boolean                               │
└─────────────────────────────────────────────────────────────────┘
```

## MV Definition API

### Inline in DB(schema, config)

MVs are inferred by the presence of `$from` - no special directive needed:

```typescript
import { DB } from 'parquedb'

// Import stream collections from packages
import { Generations, AIRequests } from 'parquedb/ai-sdk'
import { EvalRuns, EvalScores } from 'parquedb/evalite'
import { TailEvents } from 'parquedb/tail'

const db = DB({
  // =========================================================================
  // Regular Collections (no $from)
  // =========================================================================
  Customer: {
    name: 'string!',
    email: 'string!',
    tier: 'string?',
  },

  Order: {
    total: 'decimal(10,2)!',
    status: 'string!',
    customer: '-> Customer',
    items: '<- OrderItem[]',
  },

  OrderItem: {
    quantity: 'int!',
    price: 'decimal(10,2)!',
    product: '-> Product',
  },

  Product: {
    name: 'string!',
    sku: 'string!',
    price: 'decimal(10,2)!',
  },

  // =========================================================================
  // Stream Collections (imported - handle ingestion automatically)
  // =========================================================================
  Generations,    // AI-generated text/objects from AI SDK
  AIRequests,     // AI SDK request/response logs
  TailEvents,     // Cloudflare Workers tail events
  EvalRuns,       // Evalite evaluation runs
  EvalScores,     // Evalite scores

  // =========================================================================
  // Materialized Views (have $from - computed from other collections)
  // =========================================================================

  // Denormalized view of orders with expanded relations
  OrderAnalytics: {
    $from: 'Order',
    $expand: ['customer', 'items.product'],
    $flatten: { 'customer': 'buyer' },
  },

  // Aggregated daily sales
  DailySales: {
    $from: 'Order',
    $groupBy: [{ date: '$createdAt' }, 'status'],
    $compute: {
      orderCount: { $count: '*' },
      revenue: { $sum: 'total' },
      avgOrder: { $avg: 'total' },
    },
    $refresh: { mode: 'scheduled', schedule: '0 * * * *' },
  },

  // AI usage analytics (MV from imported stream collection)
  DailyAIUsage: {
    $from: 'AIRequests',
    $groupBy: [{ date: '$timestamp' }, 'modelId'],
    $compute: {
      requestCount: { $count: '*' },
      totalTokens: { $sum: 'tokens' },
      avgLatency: { $avg: 'latencyMs' },
      errorCount: { $sum: { $cond: [{ $exists: '$error' }, 1, 0] } },
    },
  },

  // Worker errors (filtered view of tail events)
  WorkerErrors: {
    $from: 'TailEvents',
    $filter: { outcome: { $ne: 'ok' } },
  },

  // Request analytics by endpoint
  EndpointMetrics: {
    $from: 'TailEvents',
    $filter: { 'event.request': { $exists: true } },
    $groupBy: ['scriptName', 'event.request.method', { path: '$event.request.url' }],
    $compute: {
      requestCount: { $count: '*' },
      avgLatency: { $avg: 'duration' },
      errorRate: { $avg: { $cond: [{ $ne: ['$outcome', 'ok'] }, 1, 0] } },
    },
  },

}, {
  storage: { type: 'fs', path: './data' },
})

// Access collections and views the same way
const orders = await db.Order.find({ status: 'completed' })
const analytics = await db.OrderAnalytics.find({ buyer_tier: 'premium' })
const sales = await db.DailySales.find({ date: '2024-01-15' })
const aiUsage = await db.DailyAIUsage.find({ modelId: 'gpt-4' })
const errors = await db.WorkerErrors.find({ scriptName: 'my-worker' })
```

### How It Works

The `$from` directive distinguishes MVs from collections:

| Has `$from`? | Type | Storage | Updates |
|--------------|------|---------|---------|
| No | Collection | Direct writes | CRUD operations |
| Yes | Materialized View | Computed from source | Refresh (streaming/scheduled) |

### Stream Collections

External data sources (AI SDK, tail events, evalite) are imported as **stream collections**.
These are regular collections that handle ingestion automatically:

```typescript
// parquedb/ai-sdk exports:
export const Generations = {
  $type: 'Generation',
  modelId: 'string!',
  content: 'variant!',        // Generated text or object
  contentType: 'string!',     // 'text' | 'object'
  tokens: 'int?',
  timestamp: 'timestamp!',
  // ... internal ingestion wiring
}

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
}

// parquedb/tail exports:
export const TailEvents = {
  $type: 'TailEvent',
  scriptName: 'string!',
  outcome: 'string!',
  event: 'variant?',          // Request/response info
  logs: 'variant[]',
  exceptions: 'variant[]',
  timestamp: 'timestamp!',
}
```

When you include these in your schema, ParqueDB automatically:
1. Creates the collection
2. Wires up ingestion from the source (AI SDK middleware, tail handler, etc.)
3. Makes the data available for MVs via `$from`

### MVs Are Always `$from`

Every MV references a source collection with `$from`:

```typescript
// MV from your collection
OrderAnalytics: {
  $from: 'Order',
  $expand: ['customer'],
}

// MV from imported stream collection
DailyAIUsage: {
  $from: 'AIRequests',
  $groupBy: [{ date: '$timestamp' }],
  $compute: { count: { $count: '*' } },
}

// MV from another MV (cascading)
WeeklyAIUsage: {
  $from: 'DailyAIUsage',
  $groupBy: [{ week: '$date' }],
  $compute: { totalRequests: { $sum: 'requestCount' } },
}
```

### Refresh Modes

Default is `streaming` (updates on every write to source). Override with `$refresh`:

```typescript
// Streaming (default) - updates immediately when source changes
OrderAnalytics: {
  $from: 'Order',
  $expand: ['customer'],
  // $refresh: { mode: 'streaming' }  // implied
}

// Scheduled - refreshes on cron schedule
DailySales: {
  $from: 'Order',
  $groupBy: [{ date: '$createdAt' }],
  $compute: { revenue: { $sum: 'total' } },
  $refresh: { mode: 'scheduled', schedule: '0 * * * *' },  // hourly
}

// Manual - only refreshes when explicitly called
AdHocReport: {
  $from: 'Order',
  $refresh: { mode: 'manual' },
}
```

### Standalone defineView()

For reusable/shareable MV definitions:

```typescript
import { defineView } from 'parquedb'

// Define once
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
  OrderAnalytics,  // Shorthand
}, { storage })
```

### Projection Types

| Type | Use Case | Optimization |
|------|----------|--------------|
| `'olap'` | Analytics queries | Denormalized, columnar, partitioned by time |
| `'oltp'` | Operational queries | Lightweight denormalization, fast point lookups |
| `'both'` | Hybrid workloads | Balanced approach |

### Expansion Directives

```typescript
// $expand - Inline related entities (denormalization)
$expand: [
  'customer',                    // Adds customer_* fields
  'customer.address',            // Nested: customer_address_* fields
  'items.product',               // Array relation: items_product_* fields
  'items.product.category'       // 3-level nesting
]

// $flatten - Rename expanded field prefixes
$flatten: {
  'customer': 'buyer',           // customer_name → buyer_name
  'customer.address': 'shipping' // customer_address_city → shipping_city
}
```

## Refresh Modes

### 1. Streaming MVs (ClickHouse-style)

Triggered automatically on every write to source tables.

```typescript
const RealtimeOrders = defineView({
  $type: 'RealtimeOrdersView',
  $from: 'Order',
  $expand: ['customer'],

  $refresh: {
    mode: 'streaming',
    // Triggered on INSERT/UPDATE/DELETE to 'orders' collection
    // Incremental append for INSERT-only workloads
    // Full refresh for UPDATE/DELETE (or when incremental not possible)
  }
})
```

**Implementation**: Uses ParqueDB's CDC event log
```typescript
// Event triggers MV update
interface MVTrigger {
  sourceNs: string           // 'orders'
  eventTypes: EventOp[]      // ['CREATE', 'UPDATE', 'DELETE']
  mvName: string             // 'RealtimeOrdersView'
  refreshFn: (events: Event[]) => Promise<void>
}
```

### 2. Scheduled MVs (ClickHouse REFRESH EVERY style)

Refreshed on a cron schedule.

```typescript
const DailyAnalytics = defineView({
  $type: 'DailyAnalyticsView',
  $from: 'Order',
  $expand: ['customer', 'items.product'],

  $refresh: {
    mode: 'scheduled',
    schedule: '0 2 * * *',    // Daily at 2 AM
    strategy: 'replace',       // 'replace' | 'append'
    timezone: 'UTC',
  }
})
```

**Implementation**: Uses Cloudflare Durable Object Alarms or cron triggers
```typescript
// Scheduled via DO alarm
async alarm() {
  const mvs = await this.getMVsDueForRefresh()
  for (const mv of mvs) {
    await this.refreshMV(mv)
  }
}
```

### 3. Manual MVs

Refreshed only on explicit command.

```typescript
const AdHocReport = defineView({
  $type: 'AdHocReportView',
  $from: 'Order',

  $refresh: {
    mode: 'manual'
  }
})

// Refresh via API
await db.refreshView('AdHocReportView')
```

## Incremental vs Full Refresh

### Incremental Refresh (Preferred)

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

## Staleness Detection

MVs use Iceberg snapshot IDs for staleness detection:

```typescript
interface MVLineage {
  // Snapshot ID of each source table at last refresh
  sourceSnapshots: Map<string, SnapshotId>

  // Version ID of the MV definition when last refreshed
  refreshVersionId: string

  // Timestamp of last refresh
  lastRefreshTime: Date
}

type MVState = 'fresh' | 'stale' | 'invalid'

function getMVState(mv: MaterializedView): MVState {
  // Check if MV definition changed
  if (mv.currentVersionId !== mv.lineage.refreshVersionId) {
    return 'invalid' // Definition changed, must full refresh
  }

  // Check if any source changed
  for (const [source, snapshotId] of mv.lineage.sourceSnapshots) {
    const currentSnapshot = getCurrentSnapshot(source)
    if (currentSnapshot !== snapshotId) {
      return 'stale' // Source changed, needs refresh
    }
  }

  return 'fresh'
}
```

## Query Routing

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

## Aggregation Support

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

### Supported Aggregate Functions

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

## Storage Layout

```
warehouse/
  database/
    _views/
      OrderAnalyticsView/
        metadata/
          version-hint.text
          v1.metadata.json         # MV definition + lineage
          snap-*.avro              # Snapshots
        data/
          *.parquet                # Precomputed data
```

### MV Metadata Schema

```typescript
interface MVMetadata {
  // View identification
  viewId: string
  name: string

  // MV definition
  definition: {
    $type: string
    $from: string              // Source collection
    $expand?: string[]
    $flatten?: Record<string, string>
    $filter?: Filter
    $groupBy?: (string | Record<string, string>)[]
    $compute?: Record<string, AggregateExpr>
  }

  // Refresh configuration
  refresh: {
    mode: 'streaming' | 'scheduled' | 'manual'
    schedule?: string
    gracePeriod?: string
    strategy?: 'replace' | 'append'
  }

  // Current state
  lineage: MVLineage

  // Schema of materialized data
  schema: IcebergSchema

  // Iceberg table for storage
  storageTableLocation: string
}
```

## API Reference

### Creating Views

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

### Querying Views

```typescript
// Direct query
const results = await db.views.MyView.find({ status: 'active' })

// With options
const results = await db.views.MyView.find(
  { status: 'active' },
  {
    sort: { createdAt: -1 },
    limit: 100,
    asOf: new Date('2024-01-15')  // Time-travel
  }
)
```

### Refreshing Views

```typescript
// Manual refresh
await db.refreshView('MyView')
await db.refreshView('MyView', { full: true })  // Force full refresh

// Check staleness
const state = await db.getViewState('MyView')
// { state: 'stale', lastRefresh: Date, staleSince: Date }

// List all views
const views = await db.listViews()
// [{ name: 'MyView', state: 'fresh', ... }, ...]
```

### View Maintenance

```typescript
// Drop a view
await db.dropView('MyView')

// Alter refresh settings
await db.alterView('MyView', {
  $refresh: {
    mode: 'scheduled',
    schedule: '0 */6 * * *'  // Every 6 hours
  }
})

// Vacuum old snapshots
await db.vacuumView('MyView', { retentionDays: 7 })
```

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
  // Reschedule
  await this.scheduleNextAlarms()
}
```

## Performance Characteristics

| Operation | Target Latency | Notes |
|-----------|---------------|-------|
| Query fresh MV | 5-20ms | Direct Parquet read |
| Query stale MV (with grace) | 5-20ms | Uses cached data |
| Query stale MV (no grace) | 50-500ms | Falls back to source |
| Streaming refresh (incremental) | 10-50ms | Per event batch |
| Streaming refresh (full) | 100ms-10s | Depends on data size |
| Scheduled refresh | 1s-60s | Background, non-blocking |

## Stream Collections (External Sources)

External data sources (Cloudflare tail events, AI SDK, evalite) are handled by
**stream collections** - imported types that automatically wire up ingestion.

### How Stream Collections Work

```typescript
// parquedb/tail exports the TailEvents stream collection
export const TailEvents = {
  $type: 'TailEvent',
  scriptName: 'string!',
  outcome: 'string!',         // 'ok' | 'exception' | 'exceededCpu' | ...
  eventTimestamp: 'timestamp!',
  event: 'variant?',          // { request, response } for fetch events
  logs: 'variant[]',          // Console.log messages
  exceptions: 'variant[]',    // Uncaught exceptions
  // Internal: wires up tail handler ingestion
  $ingest: 'tail',
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

## Example: Tail Worker Analytics

A concrete example: ingesting Cloudflare tail events into three MVs for observability.

### Architecture

```
┌─────────────────────┐    ┌──────────────────────────────────────────────┐
│  Application        │    │  Tail Worker (src/tail/index.ts)             │
│  Workers            │───▶│                                              │
│  (producers)        │    │  ┌────────────────────────────────────────┐  │
└─────────────────────┘    │  │  TailEvent[] batch → TailEvents        │  │
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

### Schema Definition

```typescript
// src/tail/db.ts
import { DB } from 'parquedb'
import { TailEvents } from 'parquedb/tail'

export const db = DB({
  // Stream collection (imported) - ingests raw tail events
  TailEvents,

  // MV: Extract logs from tail events
  WorkerLogs: {
    $from: 'TailEvents',
    $unnest: 'logs',  // Flatten the logs array
    $select: {
      scriptName: '$scriptName',
      level: '$logs.level',
      message: '$logs.message',
      timestamp: '$logs.timestamp',
      colo: '$event.request.cf.colo',
    },
  },

  // MV: Filter to errors only
  WorkerErrors: {
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
  },

  // MV: Request analytics
  WorkerRequests: {
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
  },

  // MV: Aggregated metrics by endpoint
  EndpointMetrics: {
    $from: 'WorkerRequests',  // MV from another MV
    $groupBy: ['scriptName', 'method', { hour: '$timestamp' }],
    $compute: {
      requestCount: { $count: '*' },
      avgLatency: { $avg: 'latency' },
      errorRate: { $avg: { $cond: [{ $ne: ['$outcome', 'ok'] }, 1, 0] } },
    },
    $refresh: { mode: 'scheduled', schedule: '0 * * * *' },
  },

}, {
  storage: { type: 'r2', bucket: env.PARQUEDB },
})
```

### Tail Worker Implementation

```typescript
// src/tail/index.ts
import { db } from './db'

export default {
  async tail(events: TailItem[], env: Env, ctx: ExecutionContext) {
    // Just write to the stream collection - MVs update automatically
    ctx.waitUntil(db.TailEvents.bulkCreate(events))
  },
}
```

### Producer Worker Configuration

```toml
# wrangler.toml of your application worker
name = "my-app"
main = "src/index.ts"

[[tail_consumers]]
service = "parquedb-tail"  # The tail worker above
```

### Querying the MVs

```typescript
// Query errors from last hour
const errors = await db.WorkerErrors.find({
  outcome: 'exception',
  timestamp: { $gte: new Date(Date.now() - 3600000) },
})

// Get endpoint metrics
const metrics = await db.EndpointMetrics.find({
  scriptName: 'my-api',
  hour: { $gte: new Date('2024-01-15') },
})

// SQL query across MVs
const errorRate = await db.sql`
  SELECT scriptName, COUNT(*) as total,
         SUM(CASE WHEN outcome != 'ok' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as error_rate
  FROM WorkerRequests
  WHERE timestamp > NOW() - INTERVAL '1 hour'
  GROUP BY scriptName
  ORDER BY error_rate DESC
`
```

### Why src/tail?

The tail worker lives in `src/tail/` (not `src/worker/`) because:
1. **Can't log itself** - A worker can't be its own tail consumer (infinite loop)
2. **Separate deployment** - Deployed independently from main ParqueDB worker
3. **Different concerns** - Pure ingestion, no query serving

## Local Streaming MVs (Node.js)

Streaming MVs also work locally for development and non-Workers environments.

### Runtime Adapters

```typescript
import { createStreamAdapter } from 'parquedb'

// Node.js - EventEmitter
const nodeAdapter = createStreamAdapter({
  type: 'node',
  emitter: myEventEmitter,  // EventEmitter instance
  event: 'data',            // Event name to listen for
})

// Generic - Async iterator
const iteratorAdapter = createStreamAdapter({
  type: 'iterator',
  source: async function* () {
    for await (const event of myAsyncSource) {
      yield event
    }
  },
})

// Pull-based - Polling
const pollingAdapter = createStreamAdapter({
  type: 'poll',
  fetch: async () => fetchLatestEvents(),
  intervalMs: 1000,
})
```

### Local Processing Loop

```typescript
// Start local MV processing
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

## AI Observability MVs

Pre-built stream collections and MVs for AI SDK and evalite.

### Schema with AI Observability

```typescript
import { DB } from 'parquedb'

// Stream collections (handle ingestion automatically)
import { AIRequests, Generations } from 'parquedb/ai-sdk'
import { EvalRuns, EvalScores } from 'parquedb/evalite'

const db = DB({
  // =========================================================================
  // Stream Collections (imported - auto-ingest from AI SDK / evalite)
  // =========================================================================
  AIRequests,     // All AI SDK requests (generate/stream)
  Generations,    // Generated text/objects

  EvalRuns,       // Evalite evaluation runs
  EvalScores,     // Evalite scores

  // =========================================================================
  // MVs for AI Analytics
  // =========================================================================

  // Daily usage aggregates
  DailyAIUsage: {
    $from: 'AIRequests',
    $groupBy: ['modelId', 'providerId', { date: '$timestamp' }],
    $compute: {
      requestCount: { $count: '*' },
      totalTokens: { $sum: 'tokens' },
      avgLatency: { $avg: 'latencyMs' },
      cacheHitRate: { $avg: { $cond: ['$cached', 1, 0] } },
      errorCount: { $sum: { $cond: [{ $exists: '$error' }, 1, 0] } },
    },
  },

  // AI errors for alerting
  AIErrors: {
    $from: 'AIRequests',
    $filter: { error: { $exists: true } },
  },

  // Generated content by type
  GeneratedObjects: {
    $from: 'Generations',
    $filter: { contentType: 'object' },
  },

  // =========================================================================
  // MVs for Eval Analytics
  // =========================================================================

  // Score trends over time
  EvalTrends: {
    $from: 'EvalScores',
    $groupBy: ['suiteName', 'scorerName', { week: '$timestamp' }],
    $compute: {
      avgScore: { $avg: 'score' },
      minScore: { $min: 'score' },
      maxScore: { $max: 'score' },
      runCount: { $count: '*' },
    },
  },

}, {
  storage: { type: 'fs', path: './data' },
})
```

### Stream Collection Schemas

These are what `parquedb/ai-sdk` and `parquedb/evalite` export:

```typescript
// parquedb/ai-sdk
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
  $ingest: 'ai-sdk',          // Wires up middleware ingestion
}

export const Generations = {
  $type: 'Generation',
  modelId: 'string!',
  contentType: 'string!',     // 'text' | 'object'
  content: 'variant!',        // The generated text or object
  prompt: 'string?',
  tokens: 'int?',
  timestamp: 'timestamp!',
  $ingest: 'ai-sdk',
}

// parquedb/evalite
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

### Local Usage with AI SDK

```typescript
// ai-app.ts
import { DB } from 'parquedb'
import { AIRequests, Generations } from 'parquedb/ai-sdk'
import { createParqueDBMiddleware } from 'parquedb/ai-sdk'
import { wrapLanguageModel, generateText, generateObject } from 'ai'
import { openai } from '@ai-sdk/openai'

// Define schema with AI stream collections and MVs
const db = DB({
  AIRequests,
  Generations,

  DailyUsage: {
    $from: 'AIRequests',
    $groupBy: [{ date: '$timestamp' }, 'modelId'],
    $compute: { count: { $count: '*' }, tokens: { $sum: 'tokens' } },
  },
}, {
  storage: { type: 'fs', path: './data' },
})

// Middleware automatically writes to AIRequests and Generations
const middleware = createParqueDBMiddleware({ db })

const model = wrapLanguageModel({
  model: openai('gpt-4'),
  middleware,
})

// Use model - data flows to collections automatically
const result = await generateText({ model, prompt: 'Hello!' })

// Query the MVs
const usage = await db.DailyUsage.find({ date: '2024-01-15' })
const generations = await db.Generations.find({ contentType: 'object' })
```

### Local Usage with Evalite

```typescript
// evalite.config.ts
import { defineConfig } from 'evalite'
import { createEvaliteAdapter } from 'parquedb/evalite'

export default defineConfig({
  storage: () => createEvaliteAdapter({
    storage: { type: 'fs', path: './data' },
  }),
})

// Separately, define your analytics DB
import { DB } from 'parquedb'
import { EvalRuns, EvalScores } from 'parquedb/evalite'

const analyticsDb = DB({
  EvalRuns,
  EvalScores,

  ScoreTrends: {
    $from: 'EvalScores',
    $groupBy: ['suiteName', { week: '$timestamp' }],
    $compute: {
      avgScore: { $avg: 'score' },
      runCount: { $count: '*' },
    },
  },
}, {
  storage: { type: 'fs', path: './data' },
})

// Query eval analytics
const trends = await analyticsDb.ScoreTrends.find({ suiteName: 'my-eval' })
```

### Architecture: Local vs Workers

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Stream Sources                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐     │
│  │  Tail       │   │  AI SDK     │   │  Evalite    │   │  Custom     │     │
│  │  Events     │   │  Middleware │   │  Adapter    │   │  Streams    │     │
│  │  (Workers)  │   │  (Node.js)  │   │  (Node.js)  │   │  (Any)      │     │
│  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘     │
│         │                 │                 │                 │             │
│         └─────────────────┼─────────────────┼─────────────────┘             │
│                           │                 │                               │
│                           ▼                 ▼                               │
│                 ┌─────────────────────────────────────┐                     │
│                 │      Stream Processor               │                     │
│                 │  ┌─────────┐  ┌─────────┐          │                     │
│                 │  │ Filter  │→ │Transform│→ Batch   │                     │
│                 │  └─────────┘  └─────────┘          │                     │
│                 └─────────────────┬───────────────────┘                     │
│                                   │                                         │
│                                   ▼                                         │
│                 ┌─────────────────────────────────────┐                     │
│                 │      MV Storage                      │                     │
│                 │  Native | Iceberg | Delta Lake      │                     │
│                 │  (Parquet files)                    │                     │
│                 └─────────────────────────────────────┘                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Future Enhancements

1. **Cascading MVs** - MVs that depend on other MVs
2. **Partial Refresh** - Refresh only affected partitions
3. **Query Rewrite** - Automatic rewrite to use MVs
4. **MV Recommendations** - Suggest MVs based on query patterns
5. **Multi-table Joins** - MVs spanning multiple source tables

## Related Epics

- `parquedb-4l5g` - Epic: Materialized Views (this design)
- `parquedb-o9bw` - Epic: Vector Search Integration
- `parquedb-pmm8` - Epic: Full-Text Search
- `parquedb-nxje` - Epic: Variant Shredding (enables efficient MV filters)
