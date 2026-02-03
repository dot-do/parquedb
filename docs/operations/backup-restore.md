# Backup and Restore Operations

Practical procedures for backing up and restoring ParqueDB databases.

## Storage Backends

ParqueDB supports multiple storage backends:

| Backend | Type | Use Case |
|---------|------|----------|
| `FsBackend` | Local filesystem | Development, Node.js servers |
| `R2Backend` | Cloudflare R2 | Production, Cloudflare Workers |
| `MemoryBackend` | In-memory | Testing |

## Backup Procedures

### Local Filesystem (FsBackend)

**Quick backup with rsync:**

```bash
# Incremental backup
rsync -avz --checksum ./data/ /backups/parquedb/$(date +%Y%m%d)/

# Verify backup
ls -la /backups/parquedb/$(date +%Y%m%d)/
```

**Archive backup:**

```bash
# Create compressed archive
tar -czvf backup-$(date +%Y%m%d-%H%M%S).tar.gz ./data/

# Verify archive
tar -tzvf backup-*.tar.gz | head -20
```

### Cloudflare R2 (R2Backend)

**Using the sync system:**

```bash
# Push to remote backup
parquedb push --visibility private --slug backups/$(date +%Y%m%d)

# Preview changes first
parquedb push --dry-run
```

**Programmatic backup:**

```typescript
import { createSyncEngine } from 'parquedb/sync/engine'
import { FsBackend, R2Backend } from 'parquedb/storage'

const engine = createSyncEngine({
  local: new FsBackend('./data'),
  remote: new R2Backend(env.BACKUP_BUCKET, { prefix: 'backups/' }),
  databaseId: 'production',
  name: 'production-backup',
})

// Check status
const { diff, isSynced } = await engine.status()
console.log(`Files to upload: ${diff.toUpload.length}`)

// Push backup
const result = await engine.push({ conflictStrategy: 'local-wins' })
console.log(`Backed up ${result.uploaded.length} files`)
```

**Cross-region backup:**

```typescript
// Backup to multiple R2 buckets
const regions = [
  { bucket: env.BUCKET_US, region: 'us' },
  { bucket: env.BUCKET_EU, region: 'eu' },
]

for (const { bucket, region } of regions) {
  const engine = createSyncEngine({
    local: new R2Backend(env.PRIMARY_BUCKET),
    remote: new R2Backend(bucket),
    databaseId: `backup-${region}`,
    name: `${region}-replica`,
  })
  await engine.push({ conflictStrategy: 'local-wins' })
}
```

### Scheduled Backup (Cloudflare Workers)

```typescript
// backup-worker.ts
export default {
  async scheduled(event: ScheduledEvent, env: Env) {
    const engine = createSyncEngine({
      local: new R2Backend(env.PRIMARY_BUCKET),
      remote: new R2Backend(env.BACKUP_BUCKET, {
        prefix: `daily/${new Date().toISOString().split('T')[0]}/`,
      }),
      databaseId: env.DATABASE_ID,
      name: 'daily-backup',
    })

    await engine.push({ conflictStrategy: 'local-wins' })
  }
}
```

```toml
# wrangler.toml
[triggers]
crons = ["0 0 * * *"]  # Daily at midnight UTC
```

## Restore Procedures

### Restore from Local Backup

```bash
# Restore from rsync backup
cp -r /backups/parquedb/20260203/ ./data/

# Restore from archive
tar -xzvf backup-20260203-120000.tar.gz -C ./
```

### Restore from R2

**Using CLI:**

```bash
# Pull from remote
parquedb pull username/backup-20260203 --directory ./restored

# Verify restored data
parquedb stats --directory ./restored
```

**Programmatic restore:**

```typescript
import { createSyncEngine } from 'parquedb/sync/engine'
import { FsBackend, R2Backend } from 'parquedb/storage'

const engine = createSyncEngine({
  local: new FsBackend('./restored'),
  remote: new R2Backend(env.BACKUP_BUCKET, { prefix: 'backups/20260203/' }),
  databaseId: 'restore',
  name: 'restore-operation',
})

// Pull from backup
const result = await engine.pull({ conflictStrategy: 'remote-wins' })
console.log(`Restored ${result.downloaded.length} files`)
```

### Selective Restore

Restore specific namespaces:

```typescript
async function restoreNamespaces(
  backup: StorageBackend,
  target: StorageBackend,
  namespaces: string[]
) {
  for (const ns of namespaces) {
    // Restore entity data
    const dataPath = `data/${ns}/data.parquet`
    if (await backup.exists(dataPath)) {
      await target.write(dataPath, await backup.read(dataPath))
    }

    // Restore relationships
    for (const dir of ['forward', 'reverse']) {
      const relPath = `rels/${dir}/${ns}.parquet`
      if (await backup.exists(relPath)) {
        await target.write(relPath, await backup.read(relPath))
      }
    }
  }
}
```

## Point-in-Time Recovery

ParqueDB's event log enables recovery to any point in time.

### Event Log Structure

Events are stored with full before/after state:

```
events/
  current.parquet      # Active events
  seg-0001.parquet     # Event segments
  seg-0002.parquet
```

### Replay to Specific Time

```typescript
import { EventReplayer, BatchEventSource } from 'parquedb/events/replay'

// Create event source
const eventSource = new BatchEventSource(async (minTs, maxTs) => {
  return await loadEventSegments(storage, minTs, maxTs)
})

const replayer = new EventReplayer(eventSource)

// Recover entity state at specific time
const targetTime = new Date('2026-02-01T12:00:00Z').getTime()
const result = await replayer.replayEntity('posts:post-123', { at: targetTime })

if (result.existed && result.state) {
  console.log('State at recovery point:', result.state)
}
```

### Full Database PITR

```typescript
async function recoverDatabase(
  storage: StorageBackend,
  targetTime: Date
): Promise<Entity[]> {
  const targetTs = targetTime.getTime()
  const eventSource = await createEventSourceFromStorage(storage)
  const replayer = new EventReplayer(eventSource)

  // Get all events up to target time
  const events = await eventSource.getEventsInRange(0, targetTs)

  // Get unique entity targets
  const targets = [...new Set(events.map(e => e.target))]

  // Replay each entity
  const entities: Entity[] = []
  for (const target of targets) {
    const result = await replayer.replayEntity(target, { at: targetTs })
    if (result.existed && result.state) {
      entities.push(result.state as Entity)
    }
  }

  return entities
}
```

### CLI Time-Travel Query

```bash
# Query state at specific time
parquedb query posts --at "2026-02-01T12:00:00Z" --filter '{"status": "published"}'

# Export snapshot at point in time
parquedb export --at "2026-02-01T12:00:00Z" --output snapshot.parquet
```

## Verification

### Verify Backup Integrity

```typescript
import crypto from 'crypto'

async function verifyBackup(
  storage: StorageBackend
): Promise<{ valid: boolean; errors: string[] }> {
  const errors: string[] = []

  // Load manifest
  const manifestData = await storage.read('_meta/manifest.json')
  const manifest = JSON.parse(new TextDecoder().decode(manifestData))

  // Verify each file
  for (const [path, entry] of Object.entries(manifest.files)) {
    const file = entry as { hash: string; size: number; hashAlgorithm: string }

    if (!await storage.exists(path)) {
      errors.push(`Missing: ${path}`)
      continue
    }

    const data = await storage.read(path)
    if (data.length !== file.size) {
      errors.push(`Size mismatch: ${path}`)
      continue
    }

    const hash = crypto.createHash(file.hashAlgorithm).update(data).digest('hex')
    if (hash !== file.hash) {
      errors.push(`Hash mismatch: ${path}`)
    }
  }

  return { valid: errors.length === 0, errors }
}
```

### Quick Verification Script

```bash
#!/bin/bash
set -e

BACKUP_DIR=$1

# Check manifest exists
if [ ! -f "$BACKUP_DIR/_meta/manifest.json" ]; then
  echo "ERROR: No manifest found"
  exit 1
fi

# Count files
FILE_COUNT=$(find "$BACKUP_DIR" -type f -name "*.parquet" | wc -l)
echo "Parquet files: $FILE_COUNT"

# Verify database loads
parquedb stats --directory "$BACKUP_DIR"

echo "Verification complete"
```

## Disaster Recovery

### Recovery Procedure

1. **Assess**: Check database status and identify failure point
2. **Stop writes**: Put system in read-only mode if possible
3. **Identify recovery point**: Find last known good backup or event timestamp
4. **Restore**: Pull from backup or replay events
5. **Verify**: Run integrity checks and test queries
6. **Promote**: Switch to restored database

### Quick Recovery Commands

```bash
# 1. Pull latest backup
parquedb pull username/production-backup --directory ./recovery

# 2. Verify
parquedb stats --directory ./recovery

# 3. Test query
parquedb query users '{}' --directory ./recovery --limit 5

# 4. Promote (swap directories)
mv ./data ./data.failed
mv ./recovery ./data
```

## Best Practices

| Practice | Recommendation |
|----------|----------------|
| Backup frequency | Hourly incremental, daily full |
| Retention | 7 daily, 4 weekly, 12 monthly |
| Cross-region | At least 2 geographic regions |
| Testing | Monthly restore drills |
| Monitoring | Alert if backup > 2 hours old |

### Backup Schedule Example

```toml
# wrangler.toml
[triggers]
crons = [
  "0 * * * *",   # Hourly incremental
  "0 0 * * *",   # Daily full
  "0 0 * * 0"    # Weekly cross-region
]
```

### Monitoring

```typescript
async function checkBackupHealth(env: Env): Promise<void> {
  const manifest = await loadManifest(env.BACKUP_BUCKET)
  const lastBackup = new Date(manifest.lastSyncedAt)
  const hoursSince = (Date.now() - lastBackup.getTime()) / 3600000

  if (hoursSince > 2) {
    await sendAlert(`Backup stale: ${hoursSince.toFixed(1)} hours old`)
  }
}
```

## Related Documentation

- [Sync System](../SYNC.md) - Push/pull operations
- [Storage Backends](../backends.md) - Backend configuration
- [Deployment Guide](../deployment/backup-restore.md) - Detailed deployment procedures
