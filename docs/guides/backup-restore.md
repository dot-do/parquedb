# Backup and Restore Guide

This guide covers backup strategies, restore procedures, and disaster recovery for ParqueDB databases.

## Table of Contents

- [Overview](#overview)
- [Storage Architecture](#storage-architecture)
- [Full Data Export](#full-data-export)
- [Incremental Backup Strategies](#incremental-backup-strategies)
- [Point-in-Time Recovery](#point-in-time-recovery)
- [Disaster Recovery](#disaster-recovery)
- [Cross-Region Backup (R2)](#cross-region-backup-r2)
- [Restore Verification](#restore-verification)
- [Common Failure Scenarios](#common-failure-scenarios)
- [Best Practices](#best-practices)

## Overview

ParqueDB's architecture provides multiple layers of data protection:

1. **Parquet Files** - Immutable columnar data files (`data.parquet`, `rels.parquet`)
2. **Event Log** - Complete audit history enabling point-in-time recovery
3. **Sync Manifests** - File-level checksums for integrity verification
4. **Snapshots** - Periodic state captures for faster recovery

## Storage Architecture

Understanding the file layout is essential for backup operations:

```
{database}/
├── _meta/
│   ├── manifest.json         # Sync manifest with file hashes
│   └── schema.json           # Database schema
├── data/
│   └── {namespace}/
│       └── data.parquet      # Entity storage
├── rels/
│   ├── forward/
│   │   └── {namespace}.parquet
│   └── reverse/
│       └── {namespace}.parquet
├── events/
│   ├── current.parquet       # Active event log
│   ├── seg-0001.parquet      # Event segments
│   └── archive/              # Archived events
│       └── {year}/{month}/
└── snapshots/
    └── {timestamp}/          # Point-in-time snapshots
```

## Full Data Export

### Using the CLI

Export your entire database using the ParqueDB CLI:

```bash
# Export all namespaces to JSON
parquedb export users ./backup/users.json
parquedb export posts ./backup/posts.json
parquedb export comments ./backup/comments.json

# Export to NDJSON (streaming format, better for large datasets)
parquedb export users ./backup/users.ndjson -f ndjson

# Export to CSV
parquedb export users ./backup/users.csv -f csv
```

### Programmatic Export

```typescript
import { DB } from 'parquedb'
import { FsBackend } from 'parquedb'
import { writeFile } from 'fs/promises'

const db = DB(schema, {
  storage: new FsBackend({ root: './data' })
})

// Export a single collection
const users = await db.User.find({})
await writeFile('./backup/users.json', JSON.stringify(users, null, 2))

// Export with streaming for large datasets
const stream = await db.Post.findStream({})
const outputStream = createWriteStream('./backup/posts.ndjson')

for await (const post of stream) {
  outputStream.write(JSON.stringify(post) + '\n')
}
outputStream.end()
```

### Full Filesystem Backup

For complete backup including all indexes and metadata:

```bash
# Using rsync for local backup
rsync -avz --checksum ./data/ ./backup/$(date +%Y%m%d)/

# Using tar for archival
tar -czvf backup-$(date +%Y%m%d).tar.gz ./data/

# Verify archive integrity
tar -tzvf backup-$(date +%Y%m%d).tar.gz
```

## Incremental Backup Strategies

### Using Sync Manifests

ParqueDB tracks file changes via sync manifests. Leverage this for incremental backups:

```typescript
import { SyncEngine } from 'parquedb/sync'
import { FsBackend, R2Backend } from 'parquedb'

const syncEngine = new SyncEngine({
  local: new FsBackend({ root: './data' }),
  remote: new R2Backend(env.BACKUP_BUCKET, { prefix: 'backups/' }),
  databaseId: 'my-database',
  name: 'Production DB'
})

// Check what needs backing up
const status = await syncEngine.status()
console.log('Files to upload:', status.diff.toUpload.length)
console.log('Files unchanged:', status.diff.unchanged.length)

// Perform incremental backup
const result = await syncEngine.push({
  conflictStrategy: 'local-wins',
  onProgress: (progress) => {
    console.log(`${progress.processed}/${progress.total} files uploaded`)
  }
})

console.log('Backup complete:', result.uploaded.length, 'files')
```

### Event-Based Incremental Backup

Back up only new events since last backup:

```typescript
import { SegmentReader } from 'parquedb/events'

// Track last backed up segment
const lastBackedUpSeq = await getLastBackedUpSequence()

// Get new segments
const segments = await manifest.getSegments()
const newSegments = segments.filter(s => s.seq > lastBackedUpSeq)

for (const segment of newSegments) {
  const data = await storage.read(segment.path)
  await backupStorage.write(`events/seg-${segment.seq}.parquet`, data)
  await updateLastBackedUpSequence(segment.seq)
}
```

### Scheduled Backup Script

```bash
#!/bin/bash
# backup.sh - Run daily via cron

set -e

BACKUP_DIR="/backups/parquedb"
DATA_DIR="./data"
DATE=$(date +%Y%m%d)
RETENTION_DAYS=30

# Create backup directory
mkdir -p "$BACKUP_DIR/$DATE"

# Incremental sync using manifest
parquedb push --dry-run 2>&1 | tee "$BACKUP_DIR/$DATE/manifest-diff.txt"

# Full backup on Sunday, incremental otherwise
if [ $(date +%u) -eq 7 ]; then
  # Full backup
  tar -czvf "$BACKUP_DIR/$DATE/full.tar.gz" "$DATA_DIR"
else
  # Incremental using rsync
  rsync -avz --checksum --link-dest="$BACKUP_DIR/latest" \
    "$DATA_DIR/" "$BACKUP_DIR/$DATE/"
fi

# Update latest symlink
ln -sfn "$BACKUP_DIR/$DATE" "$BACKUP_DIR/latest"

# Cleanup old backups
find "$BACKUP_DIR" -type d -mtime +$RETENTION_DAYS -exec rm -rf {} +

echo "Backup complete: $BACKUP_DIR/$DATE"
```

## Point-in-Time Recovery

ParqueDB's event sourcing architecture enables recovery to any point in time.

### Understanding the Event Log

Events are stored with full before/after state:

```typescript
interface Event {
  id: string        // ULID (time-ordered)
  ts: number        // Timestamp (ms since epoch)
  target: string    // Entity target (ns:id)
  op: 'CREATE' | 'UPDATE' | 'DELETE'
  before: Variant | null  // State before operation
  after: Variant | null   // State after operation
  actor: string     // Who made the change
  metadata: object  // Additional context
}
```

### Replay Events to a Specific Time

```typescript
import { EventReplayer, BatchEventSource } from 'parquedb/events'

// Create event source from segments
const eventSource = new BatchEventSource(async (minTs, maxTs) => {
  return await loadEventSegments(minTs, maxTs)
})

const replayer = new EventReplayer(eventSource)

// Reconstruct entity state at a specific timestamp
const targetTime = new Date('2024-01-15T10:30:00Z').getTime()

const result = await replayer.replayEntity('posts:post-123', {
  at: targetTime
})

if (result.existed) {
  console.log('State at target time:', result.state)
  console.log('Events replayed:', result.eventsReplayed)
}
```

### Restore Entire Database to Point-in-Time

```typescript
import { EventCompactor, StateCollector } from 'parquedb/events'

// Collect state up to recovery point
const collector = new StateCollector()
const recoveryTime = new Date('2024-01-15T10:30:00Z').getTime()

// Load all event segments
const segments = await manifest.getSegmentsInRange(0, recoveryTime)

for (const segment of segments) {
  const batch = await loadSegment(segment)
  // Filter events before recovery time
  const filteredEvents = batch.events.filter(e => e.ts <= recoveryTime)
  collector.processBatch({ ...batch, events: filteredEvents })
}

// Get recovered state
const entities = collector.getExistingEntities()
const relationships = collector.getExistingRelationships()

console.log(`Recovered ${entities.length} entities`)
console.log(`Recovered ${relationships.length} relationships`)

// Write recovered state to new Parquet files
await stateWriter.writeEntities(entities)
await stateWriter.writeRelationships(relationships)
```

### Using Snapshots for Faster Recovery

Snapshots accelerate recovery by providing checkpoints:

```typescript
import { InMemorySnapshotStorage, replayEventsWithSnapshots } from 'parquedb/events'

const snapshotStorage = new InMemorySnapshotStorage()

// Replay with snapshot support
const result = await replayEventsWithSnapshots(
  eventSource,
  snapshotStorage,
  'posts',
  'post-123',
  {
    at: targetTime,
    createSnapshot: true  // Create snapshot for future use
  }
)

console.log('Used snapshot:', result.usedSnapshot)
console.log('Events from snapshot:', result.eventsFromSnapshot)
```

## Disaster Recovery

### Recovery Time Objective (RTO) Considerations

| Recovery Method | RTO | Data Loss Risk |
|-----------------|-----|----------------|
| Hot standby (R2 replication) | Minutes | None |
| Event replay from backup | Hours | Since last event backup |
| Full restore from snapshot | 30-60 min | Since last snapshot |
| Full restore from archive | Hours | Since last archive |

### Disaster Recovery Procedure

1. **Assess the situation**
   ```bash
   # Check database status
   parquedb stats

   # Verify manifest integrity
   cat ./data/_meta/manifest.json | jq '.files | length'
   ```

2. **Stop write operations**
   ```typescript
   // In Workers, put DO in read-only mode
   await env.PARQUEDB.idFromName('main').get().setReadOnly(true)
   ```

3. **Identify recovery point**
   ```typescript
   // Find last known good state
   const segments = await manifest.getSegments()
   const lastGoodSegment = segments
     .filter(s => s.maxTs < corruptionTimestamp)
     .pop()

   console.log('Recovering to:', new Date(lastGoodSegment.maxTs))
   ```

4. **Perform recovery**
   ```bash
   # Create recovery environment
   mkdir ./recovery

   # Restore from backup
   tar -xzvf backup-latest.tar.gz -C ./recovery

   # Or pull from remote backup
   parquedb pull backup/my-database --directory ./recovery
   ```

5. **Verify and promote**
   ```bash
   # Verify recovered data
   parquedb stats --directory ./recovery

   # Run validation queries
   parquedb query posts '{}' --directory ./recovery --limit 10

   # Promote recovery to production
   mv ./data ./data.corrupted
   mv ./recovery ./data
   ```

### Automated Failover Script

```bash
#!/bin/bash
# failover.sh - Automated disaster recovery

set -e

PRIMARY_BUCKET="parquedb-primary"
BACKUP_BUCKET="parquedb-backup"
HEALTH_CHECK_URL="https://api.example.com/health"

# Check primary health
if ! curl -sf "$HEALTH_CHECK_URL" > /dev/null; then
  echo "Primary unhealthy, initiating failover..."

  # Pull latest backup
  parquedb pull "$BACKUP_BUCKET/production" --directory ./recovery

  # Verify integrity
  if parquedb stats --directory ./recovery > /dev/null 2>&1; then
    echo "Backup verified, promoting to production"

    # Swap directories atomically
    mv ./data ./data.failed.$(date +%s)
    mv ./recovery ./data

    # Restart services
    systemctl restart parquedb-worker

    # Notify
    curl -X POST "$SLACK_WEBHOOK" -d '{"text":"ParqueDB failover complete"}'
  else
    echo "Backup verification failed!"
    exit 1
  fi
fi
```

## Cross-Region Backup (R2)

### Multi-Region Replication Strategy

Cloudflare R2 provides automatic global distribution. For additional protection:

```typescript
import { SyncEngine } from 'parquedb/sync'
import { R2Backend } from 'parquedb'

// Primary region bucket
const primaryBackend = new R2Backend(env.PRIMARY_BUCKET, {
  prefix: 'parquedb/'
})

// Backup region bucket (different account or region)
const backupBackend = new R2Backend(env.BACKUP_BUCKET, {
  prefix: 'parquedb-backup/'
})

// Sync primary to backup
const syncEngine = new SyncEngine({
  local: primaryBackend,
  remote: backupBackend,
  databaseId: 'production',
  name: 'Production DB'
})

// Scheduled cross-region sync
export async function syncToBackupRegion() {
  const result = await syncEngine.push({
    conflictStrategy: 'local-wins'
  })

  console.log(`Synced ${result.uploaded.length} files to backup region`)
  return result
}
```

### R2 Bucket Configuration

```toml
# wrangler.toml

# Primary bucket
[[r2_buckets]]
binding = "PRIMARY_BUCKET"
bucket_name = "parquedb-primary"

# Backup bucket (can be in different account)
[[r2_buckets]]
binding = "BACKUP_BUCKET"
bucket_name = "parquedb-backup"
jurisdiction = "eu"  # Optional: different jurisdiction
```

### Cross-Region Sync Worker

```typescript
// workers/backup-sync.ts
import { SyncEngine } from 'parquedb/sync'
import { R2Backend } from 'parquedb'

export default {
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    const sync = new SyncEngine({
      local: new R2Backend(env.PRIMARY_BUCKET),
      remote: new R2Backend(env.BACKUP_BUCKET),
      databaseId: env.DATABASE_ID,
      name: 'Production'
    })

    ctx.waitUntil(sync.push({ conflictStrategy: 'local-wins' }))
  }
}

// wrangler.toml
[triggers]
crons = ["0 */6 * * *"]  # Every 6 hours
```

## Restore Verification

### Integrity Checks

```typescript
import { SyncManifest, diffManifests } from 'parquedb/sync'
import crypto from 'crypto'

async function verifyBackup(
  backupStorage: StorageBackend,
  originalManifest: SyncManifest
): Promise<{ valid: boolean; errors: string[] }> {
  const errors: string[] = []

  // Load backup manifest
  const backupManifestData = await backupStorage.read('_meta/manifest.json')
  const backupManifest = JSON.parse(
    new TextDecoder().decode(backupManifestData)
  ) as SyncManifest

  // Verify each file
  for (const [path, expectedFile] of Object.entries(backupManifest.files)) {
    try {
      const data = await backupStorage.read(path)

      // Verify size
      if (data.length !== expectedFile.size) {
        errors.push(`${path}: size mismatch (${data.length} vs ${expectedFile.size})`)
        continue
      }

      // Verify hash
      const hash = crypto.createHash('sha256').update(data).digest('hex')
      if (hash !== expectedFile.hash) {
        errors.push(`${path}: hash mismatch`)
        continue
      }
    } catch (err) {
      errors.push(`${path}: ${err.message}`)
    }
  }

  return {
    valid: errors.length === 0,
    errors
  }
}
```

### Data Consistency Verification

```typescript
async function verifyDataConsistency(db: ParqueDB): Promise<void> {
  // Verify entity counts match across forward/reverse relationships
  const namespaces = await db.getNamespaces()

  for (const ns of namespaces) {
    const collection = db.collection(ns)

    // Count entities
    const entityCount = await collection.count({})
    console.log(`${ns}: ${entityCount} entities`)

    // Verify relationships are bidirectional
    const entities = await collection.find({})
    for (const entity of entities) {
      const relationships = await collection.getRelationships(entity.$id)

      for (const rel of relationships) {
        // Verify reverse relationship exists
        const reverseRels = await db.collection(rel.toNs)
          .getRelationships(rel.toId, { predicate: rel.reverse })

        const hasReverse = reverseRels.some(
          r => r.toNs === ns && r.toId === entity.$id
        )

        if (!hasReverse) {
          console.warn(`Missing reverse relationship: ${entity.$id} -> ${rel.toId}`)
        }
      }
    }
  }
}
```

### Restore Test Script

```bash
#!/bin/bash
# test-restore.sh - Verify backup can be restored

set -e

BACKUP_FILE=$1
TEST_DIR=$(mktemp -d)

echo "Testing restore of $BACKUP_FILE to $TEST_DIR"

# Extract backup
tar -xzf "$BACKUP_FILE" -C "$TEST_DIR"

# Verify database loads
parquedb stats --directory "$TEST_DIR/data"

# Run sample queries
parquedb query users '{"limit": 1}' --directory "$TEST_DIR/data"

# Verify manifest checksums
node -e "
const fs = require('fs');
const crypto = require('crypto');
const manifest = JSON.parse(fs.readFileSync('$TEST_DIR/data/_meta/manifest.json'));

let valid = 0, invalid = 0;
for (const [path, file] of Object.entries(manifest.files)) {
  const data = fs.readFileSync('$TEST_DIR/data/' + path);
  const hash = crypto.createHash('sha256').update(data).digest('hex');
  if (hash === file.hash) valid++; else invalid++;
}
console.log('Valid files:', valid, 'Invalid files:', invalid);
process.exit(invalid > 0 ? 1 : 0);
"

# Cleanup
rm -rf "$TEST_DIR"

echo "Restore verification complete!"
```

## Common Failure Scenarios

### 1. Corrupted Parquet File

**Symptoms:** Read errors, truncated data

**Recovery:**
```typescript
// Identify corrupted file
const corrupted = 'data/posts/data.parquet'

// Restore from event log
const collector = new StateCollector()
const events = await eventSource.getEventsForTarget('posts:*')

for (const event of events) {
  if (event.target.startsWith('posts:')) {
    collector.processEvent(event)
  }
}

// Write new Parquet file
const posts = collector.getExistingEntities()
await writeParquetFile(corrupted, posts)
```

### 2. Missing Event Segments

**Symptoms:** Gap in event history, inconsistent state

**Recovery:**
```typescript
// Check for gaps in segment sequence
const segments = await manifest.getSegments()
const seqs = segments.map(s => s.seq).sort((a, b) => a - b)

for (let i = 1; i < seqs.length; i++) {
  if (seqs[i] !== seqs[i-1] + 1) {
    console.warn(`Gap in segments: ${seqs[i-1]} -> ${seqs[i]}`)

    // Attempt to restore from archive
    const archivePath = `events/archive/seg-${seqs[i-1] + 1}.parquet`
    const archived = await archiver.restore(archivePath)
  }
}
```

### 3. Manifest Corruption

**Symptoms:** Sync failures, file mismatch errors

**Recovery:**
```bash
# Rebuild manifest from filesystem
parquedb init --rebuild-manifest --directory ./data

# Or rebuild programmatically
const manifest = await syncEngine.buildLocalManifest('private')
await syncEngine.saveLocalManifest(manifest)
```

### 4. Accidental Deletion

**Symptoms:** Missing entities, 404 errors

**Recovery:**
```typescript
// Find deletion event
const events = await eventSource.getEventsForTarget('posts:deleted-post')
const deleteEvent = events.find(e => e.op === 'DELETE')

if (deleteEvent && deleteEvent.before) {
  // Recreate entity from before state
  await db.Post.create({
    $id: 'deleted-post',
    ...deleteEvent.before
  })

  console.log('Entity restored from event log')
}
```

### 5. R2 Bucket Access Issues

**Symptoms:** Network errors, timeout errors

**Recovery:**
```typescript
// Implement retry with exponential backoff
import { R2Backend, R2OperationError } from 'parquedb/storage'

async function resilientRead(
  backend: R2Backend,
  path: string,
  maxRetries = 3
): Promise<Uint8Array> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await backend.read(path)
    } catch (error) {
      if (error instanceof R2OperationError && attempt < maxRetries - 1) {
        const delay = Math.pow(2, attempt) * 1000
        await new Promise(r => setTimeout(r, delay))
        continue
      }
      throw error
    }
  }
  throw new Error('Max retries exceeded')
}
```

## Best Practices

### Backup Schedule

| Data Type | Frequency | Retention |
|-----------|-----------|-----------|
| Full database | Weekly | 4 weeks |
| Incremental (events) | Hourly | 7 days |
| Snapshots | Daily | 30 days |
| Cross-region sync | Every 6 hours | Mirror |

### Monitoring and Alerts

```typescript
// Monitor backup health
export async function checkBackupHealth(env: Env): Promise<void> {
  const manifest = await loadManifest(env.PRIMARY_BUCKET)
  const lastSync = new Date(manifest.lastSyncedAt)
  const hoursSinceSync = (Date.now() - lastSync.getTime()) / 3600000

  if (hoursSinceSync > 24) {
    await sendAlert(`Backup stale: ${hoursSinceSync.toFixed(1)} hours old`)
  }

  // Check backup integrity
  const backupManifest = await loadManifest(env.BACKUP_BUCKET)
  const diff = diffManifests(manifest, backupManifest)

  if (diff.toUpload.length > 100) {
    await sendAlert(`Backup behind: ${diff.toUpload.length} files pending`)
  }
}
```

### Security Considerations

1. **Encrypt backups at rest**
   ```bash
   # Encrypt backup archive
   gpg --symmetric --cipher-algo AES256 backup.tar.gz
   ```

2. **Use separate credentials for backup bucket**
   ```toml
   # wrangler.toml - Use different API token for backup
   [[r2_buckets]]
   binding = "BACKUP_BUCKET"
   bucket_name = "parquedb-backup"
   # Access via separate service token with write-only permissions
   ```

3. **Verify backup integrity before deletion**
   ```bash
   # Never delete old backups until new backup is verified
   ./test-restore.sh backup-new.tar.gz && rm backup-old.tar.gz
   ```

### Documentation and Runbooks

Maintain these documents alongside your backups:

1. **Recovery runbook** - Step-by-step restore procedure
2. **Contact list** - Who to notify during incidents
3. **Schema history** - Track schema changes for event replay compatibility
4. **Test schedule** - Regular restore testing cadence

---

For more information:

- [Sync System Documentation](../architecture/sync.md)
- [Event Sourcing Guide](../architecture/event-sourcing.md)
- [CLI Reference](../cli-reference.md)
