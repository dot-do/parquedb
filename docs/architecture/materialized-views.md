# ParqueDB Materialized Views Design

## Overview

Materialized Views (MVs) in ParqueDB combine:
1. **IceType Projections** - Schema definition with `$projection`, `$from`, `$expand`, `$flatten`
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
│  Definition (IceType Projection)                                │
│  ├── $type: 'OrderAnalyticsView'                                │
│  ├── $projection: 'olap'                                        │
│  ├── $from: 'Order'                                             │
│  ├── $expand: ['customer', 'items.product']                     │
│  └── $flatten: { 'customer': 'buyer' }                          │
├─────────────────────────────────────────────────────────────────┤
│  Storage Table (Iceberg)                                        │
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

### Basic Definition

```typescript
import { defineView } from 'parquedb'

// Define a materialized view using IceType projection syntax
const OrderAnalytics = defineView({
  // IceType projection directives
  $type: 'OrderAnalyticsView',
  $projection: 'olap',
  $from: 'Order',
  $expand: ['customer', 'items.product'],
  $flatten: {
    'customer': 'buyer',
    'items.product': 'lineItem'
  },

  // ParqueDB MV-specific options
  $refresh: {
    mode: 'streaming',        // 'streaming' | 'scheduled' | 'manual'
    // schedule: '0 * * * *', // For scheduled mode (cron)
    // retention: '7d',       // How long to keep old snapshots
  },

  // Optional: computed/aggregated fields
  $compute: {
    totalItems: { $count: 'items' },
    orderValue: { $sum: 'items.price' },
  }
})
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
  $projection: 'olap',
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
  $projection: 'olap',
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
  $projection: 'olap',
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
  $projection: 'olap',
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

  // IceType projection definition
  definition: {
    $type: string
    $projection: 'oltp' | 'olap' | 'both'
    $from?: string
    $expand?: string[]
    $flatten?: Record<string, string>
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
  $projection: 'olap',
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

## Event Stream MVs (External Sources)

MVs can also be populated from **external event streams** rather than ParqueDB collections.
This is useful for ingesting data from Cloudflare tail workers, webhooks, queues, etc.

### Event Stream Definition

```typescript
import { defineStreamView } from 'parquedb'

// MV populated from an external event stream (not a ParqueDB collection)
const WorkerLogs = defineStreamView({
  $type: 'WorkerLog',
  $stream: 'tail',  // External stream source

  // Schema for the materialized data
  $schema: {
    scriptName: 'string!',
    level: 'string!',       // debug, info, log, warn, error
    message: 'string!',
    timestamp: 'timestamp!',
    requestId: 'string?',
    colo: 'string?',
  },

  // Transform incoming events to the schema
  $transform: (event: TailItem) => event.logs.map(log => ({
    scriptName: event.scriptName,
    level: log.level,
    message: JSON.stringify(log.message),
    timestamp: new Date(log.timestamp),
    requestId: event.event?.request?.headers?.['cf-ray'],
    colo: event.event?.request?.cf?.colo,
  })),

  // Storage options
  $refresh: {
    mode: 'streaming',
    backend: 'native',  // Use simple Parquet files (no Iceberg overhead)
  },
})
```

## Example: Tail Worker Analytics

A concrete example: ingesting Cloudflare tail events into three MVs for observability.

### Architecture

```
┌─────────────────────┐    ┌──────────────────────────────────────────────┐
│  Application        │    │  Tail Worker (src/tail/index.ts)             │
│  Workers            │───▶│                                              │
│  (producers)        │    │  ┌────────────────────────────────────────┐  │
└─────────────────────┘    │  │  TailEvent[] batch                     │  │
                           │  └────────────────────────────────────────┘  │
                           │              │                                │
                           │    ┌─────────┼─────────┐                     │
                           │    ▼         ▼         ▼                     │
                           │ ┌──────┐ ┌──────┐ ┌──────────┐               │
                           │ │ Logs │ │Errors│ │ Requests │               │
                           │ │  MV  │ │  MV  │ │    MV    │               │
                           │ └──────┘ └──────┘ └──────────┘               │
                           │    │         │         │                     │
                           │    └─────────┼─────────┘                     │
                           │              ▼                                │
                           │  ┌────────────────────────────────────────┐  │
                           │  │  ParqueDB (R2 Parquet files)           │  │
                           │  └────────────────────────────────────────┘  │
                           └──────────────────────────────────────────────┘
```

### MV Definitions

```typescript
// src/tail/views.ts
import { defineStreamView } from 'parquedb'

// ============================================================================
// 1. Worker Logs MV - All console.log messages
// ============================================================================
export const WorkerLogs = defineStreamView({
  $type: 'WorkerLog',
  $stream: 'tail',
  $schema: {
    $id: 'string!',
    scriptName: 'string!',
    level: 'string!',
    message: 'string!',
    timestamp: 'timestamp!',
    colo: 'string?',
    url: 'string?',
  },
  $transform: (event: TailItem) => event.logs.map(log => ({
    $id: `${event.eventTimestamp}-${log.timestamp}`,
    scriptName: event.scriptName,
    level: log.level,
    message: Array.isArray(log.message) ? log.message.join(' ') : String(log.message),
    timestamp: new Date(log.timestamp),
    colo: event.event?.request?.cf?.colo,
    url: event.event?.request?.url,
  })),
  $refresh: { mode: 'streaming', backend: 'native' },
})

// ============================================================================
// 2. Errors MV - Exceptions and error outcomes
// ============================================================================
export const WorkerErrors = defineStreamView({
  $type: 'WorkerError',
  $stream: 'tail',
  $schema: {
    $id: 'string!',
    scriptName: 'string!',
    outcome: 'string!',
    exceptionName: 'string?',
    exceptionMessage: 'string?',
    timestamp: 'timestamp!',
    url: 'string?',
    status: 'int?',
    colo: 'string?',
  },
  $filter: (event: TailItem) =>
    event.outcome !== 'ok' || event.exceptions.length > 0,
  $transform: (event: TailItem) => ({
    $id: `${event.scriptName}-${event.eventTimestamp}`,
    scriptName: event.scriptName,
    outcome: event.outcome,
    exceptionName: event.exceptions[0]?.name,
    exceptionMessage: event.exceptions[0]?.message,
    timestamp: new Date(event.eventTimestamp),
    url: event.event?.request?.url,
    status: event.event?.response?.status,
    colo: event.event?.request?.cf?.colo,
  }),
  $refresh: { mode: 'streaming', backend: 'native' },
})

// ============================================================================
// 3. Requests MV - Web analytics (all requests)
// ============================================================================
export const WorkerRequests = defineStreamView({
  $type: 'WorkerRequest',
  $stream: 'tail',
  $schema: {
    $id: 'string!',
    scriptName: 'string!',
    method: 'string!',
    url: 'string!',
    pathname: 'string!',
    status: 'int!',
    outcome: 'string!',
    colo: 'string!',
    country: 'string?',
    timestamp: 'timestamp!',
  },
  $filter: (event: TailItem) => event.event != null,
  $transform: (event: TailItem) => {
    const url = new URL(event.event!.request.url)
    return {
      $id: `${event.scriptName}-${event.eventTimestamp}`,
      scriptName: event.scriptName,
      method: event.event!.request.method,
      url: event.event!.request.url,
      pathname: url.pathname,
      status: event.event!.response?.status ?? 0,
      outcome: event.outcome,
      colo: event.event!.request.cf?.colo ?? 'unknown',
      country: event.event!.request.cf?.country,
      timestamp: new Date(event.eventTimestamp),
    }
  },
  $refresh: { mode: 'streaming', backend: 'native' },
})
```

### Tail Worker Implementation

```typescript
// src/tail/index.ts
import { ParqueDB } from 'parquedb'
import { WorkerLogs, WorkerErrors, WorkerRequests } from './views'

export interface Env {
  PARQUEDB: R2Bucket
}

export default {
  async tail(events: TailItem[], env: Env, ctx: ExecutionContext) {
    const db = new ParqueDB({
      storage: { type: 'r2', bucket: env.PARQUEDB },
      backend: 'native',
    })

    // Process each MV
    ctx.waitUntil(Promise.all([
      db.ingestStream(WorkerLogs, events),
      db.ingestStream(WorkerErrors, events),
      db.ingestStream(WorkerRequests, events),
    ]))
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
// Query logs
const errors = await db.WorkerErrors.find({
  outcome: 'exception',
  timestamp: { $gte: new Date(Date.now() - 3600000) }  // Last hour
})

// Analytics query
const requestsByStatus = await db.WorkerRequests.aggregate([
  { $match: { timestamp: { $gte: new Date('2024-01-01') } } },
  { $group: { _id: '$status', count: { $sum: 1 } } },
])

// Error rate by worker
const errorRate = await db.sql`
  SELECT
    scriptName,
    COUNT(*) as total,
    SUM(CASE WHEN outcome != 'ok' THEN 1 ELSE 0 END) as errors,
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
