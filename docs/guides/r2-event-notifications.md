# R2 Event Notifications for Compaction

This guide explains how to set up the compaction architecture for ParqueDB's tail worker analytics pipeline.

## Architecture Overview

```
TailDO -> writes raw events to R2 (NDJSON files)
              ↓ object-create notification
        Queue -> Compaction Consumer Worker (observable via tail)
              ↓ writes Parquet segments
        R2 -> object-create notification (optional)
              ↓
        Queue -> MV refresh / downstream processing
```

### Benefits

1. **Fully Observable**: The Compaction Consumer is a regular Worker, observable via tail workers
2. **Decoupled Processing**: TailDO and compaction are independent - failures in one don't affect the other
3. **Horizontal Scaling**: Queue consumers auto-scale based on load
4. **Replay Capability**: Raw event files are preserved for debugging or reprocessing
5. **Cost Efficient**: Queue-based processing is more cost-effective than DO compute time

## Prerequisites

- Cloudflare Workers account
- R2 bucket created (`parquedb-logs`)
- Queue created (`parquedb-raw-events`)

## Setup Steps

### 1. Create R2 Bucket

```bash
wrangler r2 bucket create parquedb-logs
```

### 2. Create Queue

```bash
wrangler queues create parquedb-raw-events
```

### 3. Configure R2 Event Notifications

Enable event notifications on your R2 bucket to send object-create events to the queue.

**Via Cloudflare Dashboard:**

1. Go to R2 > parquedb-logs > Settings
2. Click "Event notifications"
3. Add a notification rule:
   - Event type: `object-create`
   - Prefix filter: `raw-events/` (optional, but recommended)
   - Queue: `parquedb-raw-events`

**Via Wrangler (CLI):**

```bash
# Note: As of late 2024, R2 event notifications configuration via wrangler
# requires using the API directly. The dashboard is the easiest method.
```

**Via API:**

```bash
curl -X PUT \
  "https://api.cloudflare.com/client/v4/accounts/{account_id}/event_notifications/r2/{bucket_name}/configuration/queues/{queue_id}" \
  -H "Authorization: Bearer {api_token}" \
  -H "Content-Type: application/json" \
  -d '{
    "rules": [{
      "prefix": "raw-events/",
      "actions": ["PutObject", "CompleteMultipartUpload"]
    }]
  }'
```

### 4. Deploy the Workers

Create a wrangler.toml for the event-driven setup:

```toml
# wrangler.toml

name = "parquedb-tail"
main = "src/worker/tail-streaming.ts"
compatibility_date = "2024-09-02"
compatibility_flags = ["nodejs_compat"]

# =============================================================================
# Tail Consumers - List workers to observe
# =============================================================================
[[tail_consumers]]
service = "parquedb-tail"

# =============================================================================
# Durable Objects
# =============================================================================
[[durable_objects.bindings]]
name = "TAIL_DO"
class_name = "TailDO"

[[migrations]]
tag = "v1"
new_classes = ["TailDO"]

# =============================================================================
# R2 Storage
# =============================================================================
[[r2_buckets]]
binding = "LOGS_BUCKET"
bucket_name = "parquedb-logs"

# =============================================================================
# Configuration
# =============================================================================
[vars]
# Prefix for raw event files
RAW_EVENTS_PREFIX = "raw-events"

# Batch size before writing raw events file
RAW_EVENTS_BATCH_SIZE = "100"

# Flush interval in ms
FLUSH_INTERVAL_MS = "30000"
```

Create a separate wrangler.toml for the compaction consumer:

```toml
# wrangler-compaction.toml

name = "parquedb-compaction"
main = "src/worker/compaction-consumer.ts"
compatibility_date = "2024-09-02"
compatibility_flags = ["nodejs_compat"]

# =============================================================================
# Queue Consumer
# =============================================================================
[[queues.consumers]]
queue = "parquedb-raw-events"
max_batch_size = 100
max_batch_timeout = 30
max_retries = 3
dead_letter_queue = "parquedb-dlq"  # Optional: for failed messages

# =============================================================================
# R2 Storage
# =============================================================================
[[r2_buckets]]
binding = "LOGS_BUCKET"
bucket_name = "parquedb-logs"

# =============================================================================
# Downstream Queue (Optional)
# =============================================================================
# Uncomment to send notifications when Parquet files are written
# [[queues.producers]]
# binding = "DOWNSTREAM_QUEUE"
# queue = "parquedb-parquet-notifications"

# =============================================================================
# Configuration
# =============================================================================
[vars]
RAW_EVENTS_PREFIX = "raw-events"
PARQUET_PREFIX = "logs/workers"
FLUSH_THRESHOLD = "1000"
COMPRESSION = "lz4"
```

### 5. Deploy

```bash
# Deploy the tail worker
wrangler deploy

# Deploy the compaction consumer
wrangler deploy -c wrangler-compaction.toml
```

## Observing the Compaction Worker

Since the Compaction Consumer is a regular Worker (not a Durable Object), you can observe it:

```bash
# Tail the compaction worker
wrangler tail parquedb-compaction
```

This shows:
- When messages are received from the queue
- Processing results for each raw event file
- When Parquet files are written
- Any errors during processing

## Data Flow

### 1. Raw Events Written by TailDO

Path: `raw-events/{timestamp}-{doId}-{batchSeq}.ndjson`

Format (NDJSON):
```json
{"doId":"abc123","createdAt":1704067200000,"batchSeq":42}
{"scriptName":"my-worker","outcome":"ok","eventTimestamp":1704067199000,"logs":[...],"exceptions":[]}
{"scriptName":"my-worker","outcome":"exception","eventTimestamp":1704067199500,"logs":[...],"exceptions":[{"name":"Error","message":"Something went wrong"}]}
```

### 2. R2 Event Notification

Sent to queue:
```json
{
  "account": "abc123",
  "bucket": "parquedb-logs",
  "object": {
    "key": "raw-events/1704067200000-abc123-42.ndjson",
    "size": 4096,
    "eTag": "\"abc123\""
  },
  "eventType": "object-create",
  "eventTime": "2024-01-01T00:00:00Z"
}
```

### 3. Parquet Files Written by Compaction Consumer

Path: `logs/workers/year=YYYY/month=MM/day=DD/hour=HH/logs-{timestamp}.parquet`

## Monitoring

### Queue Metrics

Monitor via Cloudflare Dashboard > Queues > parquedb-raw-events:
- Messages delivered
- Messages acked/retried/dead-lettered
- Consumer lag

### R2 Metrics

Monitor via Cloudflare Dashboard > R2 > parquedb-logs:
- Storage used
- Object count
- Operations per second

### Worker Metrics

Monitor via Cloudflare Dashboard > Workers:
- TailDO: Invocations, duration, errors
- Compaction Consumer: Invocations, duration, errors

## Troubleshooting

### Raw events not being written

1. Verify R2 bucket binding is correct
2. Check TailDO logs: `wrangler tail parquedb-tail`

### Queue not receiving notifications

1. Verify R2 event notification is configured
2. Check prefix filter matches your `RAW_EVENTS_PREFIX`
3. Check queue exists and binding is correct

### Compaction worker not processing

1. Check queue consumer is deployed
2. Verify R2 bucket binding is correct
3. Check for errors: `wrangler tail parquedb-compaction`

### Messages going to dead letter queue

1. Check DLQ: `wrangler queues messages parquedb-dlq`
2. Review the message content for malformed data
3. Check compaction worker logs for specific errors

## Cost Considerations

| Component | Billing |
|-----------|---------|
| TailDO WebSocket | Duration + Requests |
| R2 Storage | Storage + Operations |
| Queue | Messages processed |
| Compaction Worker | Invocations + Duration |

The event-driven architecture typically costs more in queue operations but less in DO compute time, especially at scale.

## Related Documentation

- [Cloudflare R2 Event Notifications](https://developers.cloudflare.com/r2/buckets/event-notifications/)
- [Cloudflare Queues](https://developers.cloudflare.com/queues/)
- [Cloudflare Durable Objects](https://developers.cloudflare.com/durable-objects/)
- [ParqueDB Materialized Views](../architecture/materialized-views.md)
