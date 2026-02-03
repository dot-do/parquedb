# ParqueDB Materialized Views

Materialized Views (MVs) in ParqueDB precompute and store query results for fast analytics. They combine IceType directives for schema definition with ClickHouse-style refresh modes.

## Quick Start: The $from Pattern

The presence of `$from` distinguishes MVs from regular collections:

```typescript
import { DB } from 'parquedb'
import { TailEvents } from 'parquedb/tail'  // Stream collection

const db = DB({
  // Regular collection - no $from, supports CRUD
  Order: {
    total: 'decimal(10,2)!',
    status: 'string!',
    customer: '-> Customer',
  },

  // Stream collection - imported, handles ingestion automatically
  TailEvents,

  // Materialized View - has $from, computed from source
  OrderAnalytics: {
    $from: 'Order',
    $expand: ['customer'],
    $flatten: { 'customer': 'buyer' },
  },

  // MV from stream collection
  WorkerErrors: {
    $from: 'TailEvents',
    $filter: { outcome: { $ne: 'ok' } },
  },
}, { storage })

// Access collections and views the same way
const orders = await db.Order.find({ status: 'completed' })
const analytics = await db.OrderAnalytics.find({ buyer_tier: 'premium' })
```

| Has `$from`? | Type | Storage | Updates |
|--------------|------|---------|---------|
| No | Collection | Direct writes | CRUD operations |
| Yes | Materialized View | Computed from source | Refresh (streaming/scheduled) |

---

## Stream Collections and $ingest

External data sources (Cloudflare tail events, AI SDK, evalite) use **stream collections** - imported types that automatically wire up ingestion via the `$ingest` directive.

### How Stream Collections Work

```typescript
// parquedb/tail exports:
export const TailEvents = {
  $type: 'TailEvent',
  scriptName: 'string!',
  outcome: 'string!',         // 'ok' | 'exception' | 'exceededCpu' | ...
  eventTimestamp: 'timestamp!',
  event: 'variant?',          // { request, response } for fetch events
  logs: 'variant[]',
  exceptions: 'variant[]',
  $ingest: 'tail',            // <-- Wires up tail handler ingestion
}
```

When you include a stream collection in your schema:

1. **ParqueDB creates the collection** - Schema is registered like any other collection
2. **`$ingest` wires up the data source** - Tail handler, middleware, etc. configured automatically
3. **Data flows in automatically** - No manual writes needed
4. **MVs reference it via `$from`** - Works like any other collection

### Available Stream Collections

| Package | Collections | Source |
|---------|-------------|--------|
| `parquedb/tail` | `TailEvents` | Cloudflare Workers tail events |
| `parquedb/ai-sdk` | `AIRequests`, `Generations` | AI SDK middleware |
| `parquedb/evalite` | `EvalRuns`, `EvalScores` | Evalite adapter |

---

## Architecture

```
                    Materialized View
+---------------------------------------------------------------+
|  Definition (IceType Directives)                              |
|  - $from: 'Order'                                             |
|  - $expand: ['customer', 'items.product']                     |
|  - $flatten: { 'customer': 'buyer' }                          |
|  - $compute: { totalItems: { $count: 'items' } }              |
+---------------------------------------------------------------+
|  Storage Table (any backend)                                  |
|  - data/*.parquet (precomputed results)                       |
|  - metadata/ (snapshots, manifests)                           |
|  - lineage: { sourceSnapshots, refreshVersionId }             |
+---------------------------------------------------------------+
|  Refresh Strategy                                             |
|  - mode: 'streaming' | 'scheduled' | 'manual'                 |
|  - schedule?: '0 */1 * * *' (cron for scheduled)              |
|  - incrementalEligible: boolean                               |
+---------------------------------------------------------------+
```

### Storage Layout

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

---

## Refresh Modes

Default is `streaming` (updates on every write to source). Override with `$refresh`:

### Streaming (Default)

Updates immediately when source changes:

```typescript
OrderAnalytics: {
  $from: 'Order',
  $expand: ['customer'],
  // $refresh: { mode: 'streaming' }  // implied
}
```

### Scheduled

Refreshes on cron schedule:

```typescript
DailySales: {
  $from: 'Order',
  $groupBy: [{ date: '$createdAt' }],
  $compute: { revenue: { $sum: 'total' } },
  $refresh: { mode: 'scheduled', schedule: '0 * * * *' },  // hourly
}
```

### Manual

Only refreshes when explicitly called:

```typescript
AdHocReport: {
  $from: 'Order',
  $refresh: { mode: 'manual' },
}
```

---

## Backend Support

MVs are **backend-agnostic** - the same MV definition works across all storage backends:

| Backend | Streaming MVs | Scheduled MVs | Staleness Detection |
|---------|--------------|---------------|---------------------|
| **Native** | CDC event log | DO Alarms | Event sequence IDs |
| **Iceberg** | CDC event log | DO Alarms | Snapshot IDs |
| **Delta Lake** | CDC event log | DO Alarms | Transaction log version |

Override storage backend:

```typescript
$refresh: {
  mode: 'streaming',
  backend: 'iceberg',  // Store MV in Iceberg even if source is Native
}
```

---

## Example: Tail Worker Analytics

A complete example: ingesting Cloudflare tail events into MVs for observability.

### Architecture

```
+---------------------+    +----------------------------------------------+
|  Application        |    |  Tail Worker (src/tail/index.ts)             |
|  Workers            |--->|                                              |
|  (producers)        |    |  +----------------------------------------+  |
+---------------------+    |  |  TailEvent[] batch -> TailEvents       |  |
                           |  +----------------------------------------+  |
                           |              |                                |
                           |    +---------+---------+                     |
                           |    v         v         v                     |
                           | +------+ +------+ +----------+               |
                           | | Logs | |Errors| | Requests |  MVs          |
                           | |      | |      | |          |  ($from)      |
                           | +------+ +------+ +----------+               |
                           |    |         |         |                     |
                           |    +---------+---------+                     |
                           |              v                                |
                           |  +----------------------------------------+  |
                           |  |  ParqueDB (R2 Parquet files)           |  |
                           |  +----------------------------------------+  |
                           +----------------------------------------------+
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

  // MV: Aggregated metrics (from another MV)
  EndpointMetrics: {
    $from: 'WorkerRequests',
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

---

## Example: AI Observability

Using AI SDK stream collections for AI usage analytics.

```typescript
import { DB } from 'parquedb'
import { AIRequests, Generations } from 'parquedb/ai-sdk'
import { EvalRuns, EvalScores } from 'parquedb/evalite'

const db = DB({
  // Stream collections (auto-ingest from AI SDK / evalite)
  AIRequests,
  Generations,
  EvalRuns,
  EvalScores,

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

---

## MV Directives Reference

### $from (Required)

Source collection or MV:

```typescript
$from: 'Order'           // From collection
$from: 'WorkerRequests'  // From another MV (cascading)
```

### $expand

Inline related entities (denormalization):

```typescript
$expand: [
  'customer',                    // Adds customer_* fields
  'customer.address',            // Nested: customer_address_* fields
  'items.product',               // Array relation: items_product_* fields
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

### $filter

Filter source records:

```typescript
$filter: { status: 'completed' }
$filter: { outcome: { $ne: 'ok' } }
```

### $select

Project specific fields:

```typescript
$select: {
  scriptName: '$scriptName',
  level: '$logs.level',
}
```

### $unnest

Flatten array fields:

```typescript
$unnest: 'logs'  // One row per log entry
```

### $groupBy

Group by dimensions:

```typescript
$groupBy: ['status', 'customer.tier', { day: '$createdAt' }]
```

### $compute

Compute aggregates:

```typescript
$compute: {
  orderCount: { $count: '*' },
  totalRevenue: { $sum: 'total' },
  avgOrderValue: { $avg: 'total' },
}
```

### $refresh

Refresh configuration:

```typescript
$refresh: {
  mode: 'streaming' | 'scheduled' | 'manual',
  schedule: '0 * * * *',     // Cron (for scheduled)
  gracePeriod: '15m',        // Allow stale data
  strategy: 'replace',       // 'replace' | 'append'
  backend: 'iceberg',        // Override storage backend
}
```

---

## Staleness Detection

MVs use snapshot/version IDs for staleness detection:

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
  // Definition changed -> invalid (must full refresh)
  if (mv.currentVersionId !== mv.lineage.refreshVersionId) {
    return 'invalid'
  }

  // Any source changed -> stale
  for (const [source, snapshotId] of mv.lineage.sourceSnapshots) {
    const currentSnapshot = getCurrentSnapshot(source)
    if (currentSnapshot !== snapshotId) {
      return 'stale'
    }
  }

  return 'fresh'
}
```

---

## Local vs Workers Architecture

```
                          Stream Sources
+-----------------------------------------------------------------------------+
|                                                                             |
|  +-----------+   +-----------+   +-----------+   +-----------+             |
|  | Tail      |   | AI SDK    |   | Evalite   |   | Custom    |             |
|  | Events    |   | Middleware|   | Adapter   |   | Streams   |             |
|  | (Workers) |   | (Node.js) |   | (Node.js) |   | (Any)     |             |
|  +-----+-----+   +-----+-----+   +-----+-----+   +-----+-----+             |
|        |               |               |               |                   |
|        +---------------+---------------+---------------+                   |
|                        |               |                                   |
|                        v               v                                   |
|               +-------------------------------------+                       |
|               |      Stream Processor              |                       |
|               |  Filter -> Transform -> Batch      |                       |
|               +------------------+------------------+                       |
|                                  |                                         |
|                                  v                                         |
|               +-------------------------------------+                       |
|               |      MV Storage                     |                       |
|               |  Native | Iceberg | Delta Lake     |                       |
|               |  (Parquet files)                   |                       |
|               +-------------------------------------+                       |
|                                                                             |
+-----------------------------------------------------------------------------+
```

---

## Future Enhancements

1. **Cascading MVs** - MVs that depend on other MVs (partially implemented)
2. **Partial Refresh** - Refresh only affected partitions
3. **Query Rewrite** - Automatic rewrite to use MVs
4. **MV Recommendations** - Suggest MVs based on query patterns
5. **Multi-table Joins** - MVs spanning multiple source tables

---

## Related Documentation

- [API Reference: Materialized Views](../api/materialized-views.md) - Complete API documentation
- [Architecture: Graph-First](./graph-first-architecture.md) - Relationship indexing
- [Architecture: Secondary Indexes](./secondary-indexes.md) - Index types

## Related Issues

- `parquedb-4l5g` - Epic: Materialized Views
- `parquedb-o9bw` - Epic: Vector Search Integration
- `parquedb-pmm8` - Epic: Full-Text Search
