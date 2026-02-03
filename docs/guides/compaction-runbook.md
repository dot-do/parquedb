# Compaction Operational Runbook

This runbook provides operational procedures for managing the ParqueDB compaction system, including handling stuck jobs, manual workflow retry, disaster recovery, and troubleshooting common issues.

## Table of Contents

- [System Overview](#system-overview)
- [Monitoring and Health Checks](#monitoring-and-health-checks)
- [Handling Stuck Compaction Jobs](#handling-stuck-compaction-jobs)
- [Manual Workflow Retry Procedures](#manual-workflow-retry-procedures)
- [Disaster Recovery](#disaster-recovery)
- [Troubleshooting Common Issues](#troubleshooting-common-issues)
- [Emergency Procedures](#emergency-procedures)
- [Maintenance Operations](#maintenance-operations)

---

## System Overview

### Architecture

The compaction system consists of several components:

```
Writers → R2 (native Parquet) → Event Notification → Queue → CompactionStateDO
                                                                    ↓
                                                         CompactionMigrationWorkflow
                                                                    ↓
                                                         Compacted Parquet Files
```

**Key Components:**

| Component | Purpose | State Location |
|-----------|---------|----------------|
| `CompactionStateDO` | Tracks windows and writers per namespace | Durable Object storage |
| `CompactionMigrationWorkflow` | Processes compaction batches | Cloudflare Workflows |
| Queue Consumer | Routes R2 events to DOs | Stateless |
| R2 Event Notifications | Triggers on file uploads | Cloudflare R2 |

### Window States

Windows transition through these states:

```
pending → processing → dispatched → (deleted on completion)
                ↓
          (timeout reset to pending)
```

| State | Description | Automatic Recovery |
|-------|-------------|-------------------|
| `pending` | Ready for compaction | N/A |
| `processing` | workflow.create() in progress | Reset after 5 minutes |
| `dispatched` | Workflow running | Workflow handles retries |

---

## Monitoring and Health Checks

### Check Compaction Status

**Single namespace status:**

```bash
# Via HTTP endpoint
curl https://your-worker.workers.dev/compaction/status?namespace=users

# Via wrangler
npx wrangler tail --search "compaction" --format json
```

**Expected response:**

```json
{
  "namespace": "users",
  "priority": 2,
  "effectiveMaxWaitTimeMs": 900000,
  "backpressure": "none",
  "activeWindows": 3,
  "queueMetrics": {
    "pendingWindows": 2,
    "processingWindows": 1,
    "dispatchedWindows": 0
  },
  "health": {
    "status": "healthy",
    "issues": []
  },
  "knownWriters": ["writer1", "writer2"],
  "activeWriters": ["writer1", "writer2"],
  "oldestWindowAge": 1800000,
  "totalPendingFiles": 45,
  "windowsStuckInProcessing": 0,
  "windows": [...]
}
```

### Health Check Endpoint

```bash
curl https://your-worker.workers.dev/compaction/health
```

**Health status meanings:**

| Status | Meaning | Action |
|--------|---------|--------|
| `healthy` | Normal operation | None |
| `degraded` | Performance issues | Investigate |
| `unhealthy` | Critical issues | Immediate action |

### Key Metrics to Monitor

| Metric | Warning Threshold | Critical Threshold |
|--------|-------------------|-------------------|
| `windowsStuckInProcessing` | > 0 | > 3 |
| `oldestWindowAge` | > 2 hours | > 4 hours |
| `activeWindows` | > 10 | > 20 |
| `totalPendingFiles` | > 1000 | > 5000 |

### Check Workflow Status

```bash
# List running workflows
npx wrangler workflows list compaction-migration-workflow

# Get specific workflow status
npx wrangler workflows status compaction-migration-workflow <instance-id>

# View workflow logs
npx wrangler tail --search "workflow" --format json
```

---

## Handling Stuck Compaction Jobs

### Identifying Stuck Windows

**Symptoms:**

- `windowsStuckInProcessing > 0` in health check
- Windows remain in `processing` state for > 5 minutes
- No new compacted files appearing in R2

**Diagnosis:**

```bash
# Check for stuck windows
curl https://your-worker.workers.dev/compaction/status?namespace=users | jq '.windows[] | select(.processingStatus.state == "processing")'
```

Look for windows where:
- `processingStatus.state` is `"processing"`
- `processingStatus.startedAt` is > 5 minutes ago

### Automatic Recovery

The system automatically resets stuck windows after 5 minutes (`PROCESSING_TIMEOUT_MS`). The reset happens on the next queue consumer invocation.

**To trigger manual reset:**

```bash
# Send a dummy event to trigger queue processing
# This will cause the DO to check for stuck windows
curl -X POST https://your-worker.workers.dev/compaction/trigger-check?namespace=users
```

### Manual Window Reset

If automatic recovery fails, manually reset the window:

```bash
# POST to rollback-processing endpoint
curl -X POST https://your-worker.workers.dev/compaction/internal/rollback-processing \
  -H "Content-Type: application/json" \
  -d '{"windowKey": "1700000000000"}'
```

**Warning:** Only use this if the workflow has definitely failed. Check workflow status first.

### Verifying Recovery

After reset, verify the window is back to pending:

```bash
curl https://your-worker.workers.dev/compaction/status?namespace=users | jq '.windows[] | select(.key == "1700000000000")'
```

Expected: `processingStatus.state` should be `"pending"`.

---

## Manual Workflow Retry Procedures

### When to Retry Manually

- Workflow failed with transient error (network timeout, rate limit)
- Workflow stuck without completing
- Need to reprocess after fixing data issues

### Retry a Failed Workflow

**Step 1: Identify the failed workflow**

```bash
npx wrangler workflows list compaction-migration-workflow --status failed
```

**Step 2: Get workflow details**

```bash
npx wrangler workflows status compaction-migration-workflow <workflow-id>
```

**Step 3: Reset the window to pending**

```bash
curl -X POST https://your-worker.workers.dev/compaction/internal/rollback-processing \
  -H "Content-Type: application/json" \
  -d '{"windowKey": "<window-key>"}'
```

**Step 4: Trigger compaction check**

The window will be picked up on the next queue consumer run. To trigger immediately:

```bash
# Option 1: Wait for next R2 event
# Option 2: Send test event via queue
npx wrangler queues send parquedb-compaction-events '{"action":"PutObject","object":{"key":"data/test/trigger.parquet","size":0}}'
```

### Manually Trigger Compaction Workflow

For emergency situations, create a workflow instance directly:

```typescript
// Via Workers script or wrangler
const instance = await env.COMPACTION_WORKFLOW.create({
  params: {
    namespace: 'users',
    windowStart: 1700000000000,
    windowEnd: 1700003600000,
    files: [
      'data/users/1700001234-writer1-0.parquet',
      'data/users/1700001235-writer2-0.parquet',
    ],
    writers: ['writer1', 'writer2'],
    targetFormat: 'native',
  }
})
```

**Warning:** Ensure the window is not already being processed to avoid duplicate compaction.

### Retry with Different Parameters

If compaction fails due to batch size issues:

```typescript
const instance = await env.COMPACTION_WORKFLOW.create({
  params: {
    namespace: 'users',
    windowStart: 1700000000000,
    windowEnd: 1700003600000,
    files: [...],
    writers: [...],
    targetFormat: 'native',
    // Reduce batch size for problematic windows
    maxFilesPerStep: 25,
    // Enable streaming merge for large windows
    useStreamingMerge: true,
    maxStreamingMemoryBytes: 64 * 1024 * 1024,
  }
})
```

---

## Disaster Recovery

### Scenario 1: Durable Object Data Loss

**Symptoms:**

- CompactionStateDO returns empty state
- Previously tracked windows are missing

**Recovery procedure:**

1. **Stop incoming writes temporarily** (if possible)

2. **Rebuild state from R2 files:**

```bash
# List uncompacted files
npx wrangler r2 object list your-bucket --prefix "data/" | grep -E '\d+-\w+-\d+\.parquet$'
```

3. **Re-ingest file events:**

```typescript
// Script to re-process files
async function reprocessFiles(bucket: R2Bucket, env: Env) {
  const objects = await bucket.list({ prefix: 'data/' })

  for (const obj of objects.objects) {
    if (!obj.key.endsWith('.parquet')) continue

    // Parse file info
    const match = obj.key.match(/(\d+)-([^-]+)-(\d+)\.parquet$/)
    if (!match) continue

    // Simulate R2 event
    await env.COMPACTION_QUEUE.send({
      action: 'PutObject',
      bucket: 'your-bucket',
      object: {
        key: obj.key,
        size: obj.size,
        eTag: obj.etag,
      },
      eventTime: new Date().toISOString(),
    })
  }
}
```

4. **Resume writes**

### Scenario 2: Queue Backlog Overflow

**Symptoms:**

- Queue messages aging out
- Compaction falling behind

**Recovery procedure:**

1. **Increase queue consumer batch size:**

```jsonc
// wrangler.jsonc
"queues": {
  "consumers": [{
    "queue": "parquedb-compaction-events",
    "max_batch_size": 100,
    "max_batch_timeout": 30
  }]
}
```

2. **Temporarily lower compaction thresholds:**

Update consumer config:
```typescript
const config: CompactionConsumerConfig = {
  minFilesToCompact: 5,  // Lower threshold
  maxWaitTimeMs: 60000,  // Shorter wait
}
```

3. **Scale horizontally** (if using time bucket sharding):

Enable time bucket sharding for high-throughput namespaces:
```typescript
timeBucketSharding: {
  enabled: true,
  namespacesWithSharding: ['users', 'events'],
}
```

4. **Clear stale messages:**

If messages are too old to be useful:
```bash
# Purge queue (use with caution!)
npx wrangler queues purge parquedb-compaction-events
```

### Scenario 3: R2 Bucket Corruption/Loss

**Recovery from backup:**

```bash
# Restore from backup bucket
npx wrangler r2 object copy your-backup-bucket/data/ your-bucket/data/ --recursive

# Rebuild compaction state (see Scenario 1)
```

### Scenario 4: Workflow Service Outage

**During outage:**

- Windows will accumulate in `processing` state
- After 5 minutes, they auto-reset to `pending`

**After recovery:**

1. Windows will automatically retry
2. Monitor for increased backlog
3. May need to temporarily increase resources

---

## Troubleshooting Common Issues

### Issue: Events Not Arriving at Queue

**Diagnosis:**

```bash
# Check queue exists
npx wrangler queues list

# Check for dead letter queue messages
npx wrangler queues list --show-dlq

# Verify R2 event notification config
# (Check Cloudflare Dashboard → R2 → Bucket → Settings)
```

**Solutions:**

1. Verify R2 event notification is configured correctly
2. Check queue name matches exactly
3. Ensure prefix/suffix filters are correct

### Issue: Compaction Not Triggering

**Diagnosis:**

```bash
curl https://your-worker.workers.dev/compaction/status?namespace=users | jq '{
  activeWindows,
  pendingFiles: .totalPendingFiles,
  oldestAge: .oldestWindowAge,
  config: {
    minFiles: 10,
    maxWait: 300000
  }
}'
```

**Possible causes and solutions:**

| Cause | Solution |
|-------|----------|
| Below minFilesToCompact | Wait for more files or lower threshold |
| Window too recent | Wait for maxWaitTimeMs to pass |
| Missing active writers | Wait for timeout or check writer health |
| Backpressure active | Clear backlog or increase priority |

### Issue: Workflow Fails Mid-Batch

**Diagnosis:**

```bash
npx wrangler workflows status compaction-migration-workflow <id>
```

**Common errors and solutions:**

| Error | Cause | Solution |
|-------|-------|----------|
| `Memory limit exceeded` | Batch too large | Enable streaming merge, reduce maxFilesPerStep |
| `CPU time exceeded` | Processing timeout | Reduce batch size, optimize queries |
| `R2 rate limit` | Too many R2 operations | Increase batch cooldown, reduce parallelism |
| `Parquet parse error` | Corrupted file | Identify and remove bad file, retry |

### Issue: High Latency in Compaction

**Diagnosis:**

```bash
# Check workflow step durations
npx wrangler workflows status compaction-migration-workflow <id> --verbose

# Check file sizes
npx wrangler r2 object list your-bucket --prefix "data/users/" | head -20
```

**Solutions:**

1. **Enable streaming merge** for large batches:
```typescript
useStreamingMerge: true,
maxStreamingMemoryBytes: 128 * 1024 * 1024,
```

2. **Reduce batch size**:
```typescript
maxFilesPerStep: 25,
```

3. **Increase worker resources**:
```jsonc
"usage_model": "unbound"
```

### Issue: Namespace Priority Not Working

**Verify priority is set:**

```bash
curl https://your-worker.workers.dev/compaction/status?namespace=users | jq '.priority'
```

**Set priority:**

```bash
curl -X POST https://your-worker.workers.dev/compaction/config?namespace=users \
  -H "Content-Type: application/json" \
  -d '{"priority": 0}'  # 0=critical, 1=high, 2=medium, 3=background
```

**Priority wait times:**

| Priority | Max Wait Time |
|----------|---------------|
| 0 (critical) | 1 minute |
| 1 (high) | 5 minutes |
| 2 (medium) | 15 minutes |
| 3 (background) | 1 hour |

---

## Emergency Procedures

### Emergency Stop Compaction

**To immediately stop all compaction:**

1. **Disable queue consumer:**

```bash
# Remove consumer from wrangler.jsonc and redeploy
npx wrangler deploy
```

2. **Or pause the queue:**

Via Cloudflare Dashboard: Queues → parquedb-compaction-events → Pause

### Emergency Clear Stuck State

**Nuclear option - reset all DO state:**

```bash
# This will lose all tracking state!
# Only use if rebuilding from R2 is acceptable

curl -X DELETE https://your-worker.workers.dev/compaction/reset?namespace=users
```

### Emergency Rollback Compacted Files

If compacted files are corrupt:

1. **Identify affected time range**
2. **Delete compacted files:**
```bash
npx wrangler r2 object delete your-bucket "data/users/year=2026/month=01/day=15/compacted-*.parquet"
```
3. **Restore from source files** (if not deleted)
4. **Rebuild from backup** (if source deleted)

---

## Maintenance Operations

### Regular Health Checks

Run daily:

```bash
#!/bin/bash
# health-check.sh

NAMESPACES=("users" "posts" "events")
ENDPOINT="https://your-worker.workers.dev"

for ns in "${NAMESPACES[@]}"; do
  status=$(curl -s "$ENDPOINT/compaction/status?namespace=$ns")
  stuck=$(echo "$status" | jq '.windowsStuckInProcessing')
  age=$(echo "$status" | jq '.oldestWindowAge')

  if [ "$stuck" -gt 0 ]; then
    echo "ALERT: $ns has $stuck stuck windows"
  fi

  # Alert if oldest window > 2 hours (7200000 ms)
  if [ "$age" -gt 7200000 ]; then
    echo "WARNING: $ns oldest window age: ${age}ms"
  fi
done
```

### Periodic Cleanup

**Clean up old dispatched windows:**

The system automatically deletes windows on successful workflow completion. If orphaned windows remain:

```bash
curl -X POST https://your-worker.workers.dev/compaction/cleanup?namespace=users
```

### Capacity Planning

Monitor these trends:

1. **Files per window** - Should stay consistent
2. **Compaction frequency** - Increases with write volume
3. **Processing time** - Should stay under 5 minutes

**Scale triggers:**

| Condition | Action |
|-----------|--------|
| > 1000 writes/sec | Enable time bucket sharding |
| > 100 files/window avg | Reduce window size |
| Processing > 3 min avg | Increase maxFilesPerStep or enable streaming |

### Updating Configuration

**Change window size (requires careful planning):**

1. Wait for all current windows to complete
2. Update config
3. Deploy
4. Monitor for correct window boundaries

```typescript
const config: CompactionConsumerConfig = {
  windowSizeMs: 30 * 60 * 1000,  // 30 minutes instead of 1 hour
}
```

---

## Quick Reference

### Emergency Commands

```bash
# Check health
curl https://your-worker.workers.dev/compaction/health

# Check namespace status
curl https://your-worker.workers.dev/compaction/status?namespace=users

# List workflows
npx wrangler workflows list compaction-migration-workflow

# View logs
npx wrangler tail --search "compaction"

# Reset stuck window
curl -X POST https://your-worker.workers.dev/compaction/internal/rollback-processing \
  -d '{"windowKey": "KEY"}'
```

### Key Thresholds

| Parameter | Default | Description |
|-----------|---------|-------------|
| `windowSizeMs` | 3600000 (1h) | Time window duration |
| `minFilesToCompact` | 10 | Minimum files to trigger |
| `maxWaitTimeMs` | 300000 (5m) | Wait for late writers |
| `PROCESSING_TIMEOUT_MS` | 300000 (5m) | Stuck window timeout |
| `WRITER_INACTIVE_THRESHOLD_MS` | 1800000 (30m) | Writer considered inactive |

### Contact Information

Document your escalation path:

| Level | Contact | When |
|-------|---------|------|
| L1 | On-call engineer | First response |
| L2 | Platform team | Persistent issues |
| L3 | Cloudflare support | Infrastructure issues |

---

## Related Documentation

- [Compaction Workflow Setup](./compaction-workflow.md) - Initial setup guide
- [Production Runbook](./production-runbook.md) - General operational procedures
- [Backup and Restore](./backup-restore.md) - Data recovery procedures
- [R2 Event Notifications](./r2-event-notifications.md) - Event notification setup
