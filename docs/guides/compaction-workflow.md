# Compaction & Migration Workflow Setup

This guide explains how to set up the event-driven compaction and progressive migration system.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        WRITE PATH                                │
│  Writers → R2 (native Parquet with writer ID in filename)       │
└─────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                   R2 EVENT NOTIFICATIONS                         │
│  Configured in Cloudflare Dashboard → sends to Queue            │
└─────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      QUEUE CONSUMER                              │
│  Batches events, tracks writers per time window                 │
│  CompactionStateDO stores state across invocations              │
└─────────────────────────────────────────────────────────────────┘
                          │
                          ▼ (when window ready)
┌─────────────────────────────────────────────────────────────────┐
│               COMPACTION WORKFLOW                                │
│  1. Read all files from window (per-writer sorted)              │
│  2. Merge-sort efficiently                                       │
│  3. Write to Iceberg/Delta (progressive migration!)             │
│  4. Delete old native files                                      │
└─────────────────────────────────────────────────────────────────┘
```

## Step 1: Deploy the Worker

```bash
npx wrangler deploy
```

This deploys:
- Queue consumer for `parquedb-compaction-events`
- CompactionStateDO for tracking windows
- CompactionMigrationWorkflow for processing
- MigrationWorkflow for standalone migrations

## Step 2: Create the Queue

```bash
npx wrangler queues create parquedb-compaction-events
```

## Step 3: Configure R2 Event Notifications

In the Cloudflare Dashboard:

1. Go to **R2** → **parquedb** bucket → **Settings**
2. Scroll to **Event notifications**
3. Click **Add notification**
4. Configure:

| Setting | Value |
|---------|-------|
| **Event types** | `object-create` |
| **Prefix filter** | `data/` (only watch entity data) |
| **Suffix filter** | `.parquet` |
| **Queue** | `parquedb-compaction-events` |

5. Click **Add notification**

## File Naming Convention

For writer-aware compaction to work, files must follow this pattern:

```
data/{namespace}/{timestamp}-{writerId}-{sequence}.parquet
```

Example:
```
data/users/1700001234567-writer1-0.parquet
data/users/1700001234567-writer2-0.parquet
data/users/1700001235000-writer1-1.parquet
```

## Configuration Options

The queue consumer accepts these options (configured in `src/worker/index.ts`):

```typescript
{
  windowSizeMs: 60 * 60 * 1000,    // 1 hour time windows
  minFilesToCompact: 10,           // Min files before compacting
  maxWaitTimeMs: 5 * 60 * 1000,    // Wait for late writers
  targetFormat: 'iceberg',         // Progressive migration target
  namespacePrefix: 'data/',        // Watch this prefix
}
```

## How Writer-Aware Batching Works

1. **Track Writers**: Each file's writer ID is extracted from the filename
2. **Time Windows**: Files are grouped into hourly windows
3. **Wait for Quorum**: System tracks all active writers and waits for all to report
4. **Timeout Fallback**: If a writer goes silent, proceed after `maxWaitTimeMs`
5. **Efficient Merge**: Since each writer's data is pre-sorted, merge-sort is O(n)

## Progressive Migration

During compaction, data is automatically migrated from native Parquet to the target format:

| Source | Target | What Happens |
|--------|--------|--------------|
| Native Parquet | Iceberg | Creates Iceberg metadata, manifests, data files |
| Native Parquet | Delta | Creates Delta log, checkpoint files |

This means:
- **No separate migration step** - happens naturally during compaction
- **Zero downtime** - old files serve reads until compaction completes
- **Incremental** - only newly written data gets migrated each cycle

## Monitoring

### Check Compaction State

```bash
curl https://parquedb.workers.do/compaction/status
```

Returns:
```json
{
  "activeWindows": 3,
  "knownWriters": ["writer1", "writer2"],
  "activeWriters": ["writer1", "writer2"],
  "windows": [
    {
      "key": "users:1700000000000",
      "windowStart": "2024-11-14T00:00:00.000Z",
      "windowEnd": "2024-11-14T01:00:00.000Z",
      "writers": ["writer1", "writer2"],
      "fileCount": 15,
      "totalSize": 10485760
    }
  ]
}
```

### Check Workflow Status

```bash
# List running workflows
npx wrangler workflows list compaction-migration-workflow

# Get specific workflow status
npx wrangler workflows status compaction-migration-workflow <instance-id>
```

## Troubleshooting

### Events Not Arriving

1. Check R2 event notification is configured correctly
2. Verify queue exists: `npx wrangler queues list`
3. Check queue has consumer: should show in `wrangler.jsonc`

### Compaction Not Triggering

1. Check `minFilesToCompact` threshold
2. Verify time window has passed + grace period
3. Check CompactionStateDO status endpoint

### Workflow Failures

Workflows are resumable - check status and logs:

```bash
npx wrangler workflows status compaction-migration-workflow <id>
npx wrangler tail --format=json | jq 'select(.message | contains("compaction"))'
```

## Cost Considerations

- **Queue**: $0.40 per million operations
- **Workflows**: $0.15 per 1,000 step runs
- **R2 Event Notifications**: Free (included with R2)
- **Durable Objects**: $0.15 per million requests

For a typical workload with 1M writes/day:
- ~1M queue messages = $0.40
- ~1K workflow steps = $0.15
- Total: ~$0.55/day for compaction
