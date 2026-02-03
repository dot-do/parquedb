---
title: Backup and Restore
description: Comprehensive guide to backup and restore procedures for ParqueDB, including full and incremental exports, point-in-time recovery using the event log, cross-region strategies, and disaster recovery procedures.
---

ParqueDB's architecture provides multiple mechanisms for data protection and recovery. This guide covers backup strategies, disaster recovery procedures, and restore verification steps.

## Table of Contents

- [Overview](#overview)
- [Backup Architecture](#backup-architecture)
- [Full Database Backup](#full-database-backup)
- [Incremental Backup](#incremental-backup)
- [Point-in-Time Recovery](#point-in-time-recovery)
- [Cross-Region Backup](#cross-region-backup)
- [Disaster Recovery Procedures](#disaster-recovery-procedures)
- [Restore Procedures](#restore-procedures)
- [Restore Verification](#restore-verification)
- [Automated Backup Scheduling](#automated-backup-scheduling)
- [Best Practices](#best-practices)

---

## Overview

ParqueDB uses an event-sourced architecture where the event log is the source of truth. This design provides powerful backup and recovery capabilities:

| Feature | Benefit |
|---------|---------|
| **Event Log** | Complete audit trail; enables point-in-time recovery |
| **Immutable Parquet Files** | Efficient incremental backups via sync |
| **Manifest Tracking** | Hash-based change detection |
| **Sync System** | Built-in push/pull for cloud backup |

### Data Files to Back Up

```
database-root/
  _meta/
    manifest.json          # Sync manifest (tracks all files)
  data/
    {namespace}/
      data.parquet         # Entity data
  rels/
    forward/
      {namespace}.parquet  # Forward relationships
    reverse/
      {namespace}.parquet  # Reverse relationships
  events/
    seg-{seq}.parquet      # Event segments (time-travel)
    manifest.json          # Event manifest
  indexes/
    bloom/
      {namespace}.bloom    # Bloom filter indexes
```

---

## Backup Architecture

### Three-Tier Backup Strategy

ParqueDB supports a tiered backup approach:

```
+------------------+     +------------------+     +------------------+
|   Tier 1: Hot    |     |   Tier 2: Warm   |     |   Tier 3: Cold   |
|                  |     |                  |     |                  |
|  Primary R2      |---->|  Backup Region   |---->|  Archive Storage |
|  (live data)     |     |  (cross-region)  |     |  (long-term)     |
|                  |     |                  |     |                  |
|  RPO: 0          |     |  RPO: minutes    |     |  RPO: hours      |
|  RTO: immediate  |     |  RTO: minutes    |     |  RTO: hours      |
+------------------+     +------------------+     +------------------+
```

### Recovery Point Objective (RPO)

| Backup Type | RPO | Use Case |
|-------------|-----|----------|
| Event Log | Near-zero | Point-in-time recovery |
| Sync Push | Minutes | Regular cloud backup |
| Cross-region | Minutes | Regional disaster |
| Archive | Hours/Days | Long-term compliance |

---

## Full Database Backup

### Using the CLI

The simplest way to create a full backup is using the sync system:

```bash
# Push entire database to remote storage
parquedb push --visibility private

# Push to a specific backup location
parquedb push --visibility private --slug backups/$(date +%Y%m%d)

# Preview what will be backed up
parquedb push --dry-run
```

### Programmatic Full Backup

```typescript
import { createSyncEngine } from 'parquedb/sync/engine'
import { FsBackend } from 'parquedb/storage'
import { R2Backend } from 'parquedb/storage/R2Backend'

async function fullBackup(
  sourceDir: string,
  backupBucket: R2Bucket,
  backupPrefix: string
): Promise<BackupResult> {
  const local = new FsBackend(sourceDir)
  const remote = new R2Backend(backupBucket, { prefix: backupPrefix })

  const engine = createSyncEngine({
    local,
    remote,
    databaseId: 'backup',
    name: `backup-${new Date().toISOString()}`,
    onProgress: (progress) => {
      console.log(`Backing up: ${progress.currentFile} (${progress.percent}%)`)
    },
  })

  const result = await engine.push({ dryRun: false })

  return {
    filesUploaded: result.uploaded.length,
    totalBytes: result.uploaded.reduce((sum, f) => sum + f.size, 0),
    timestamp: new Date().toISOString(),
  }
}
```

### Full Backup via Wrangler CLI

For direct R2 backup without the sync system:

```bash
# List all files to back up
parquedb list --format json > backup-manifest.json

# Copy all Parquet files to backup bucket
for file in $(parquedb list --format paths); do
  npx wrangler r2 object put backup-bucket/$file --file $file
done
```

---

## Incremental Backup

ParqueDB's sync system provides efficient incremental backups using manifest-based change detection.

### How Incremental Backup Works

1. **Manifest Comparison**: Compare local manifest with backup manifest
2. **Hash-Based Detection**: Only files with changed hashes are uploaded
3. **Efficient Transfer**: Skip unchanged files entirely

```typescript
import { SyncEngine, createSyncEngine } from 'parquedb/sync/engine'

async function incrementalBackup(engine: SyncEngine): Promise<IncrementalBackupResult> {
  // Check what changed since last backup
  const { diff, isSynced } = await engine.status()

  if (isSynced) {
    console.log('No changes since last backup')
    return { filesUploaded: 0, skipped: diff.unchanged.length }
  }

  console.log('Changes detected:')
  console.log(`  New files: ${diff.toUpload.length}`)
  console.log(`  Modified: ${diff.conflicts.length}`)
  console.log(`  Unchanged: ${diff.unchanged.length}`)

  // Push only changed files
  const result = await engine.push({
    dryRun: false,
    conflictStrategy: 'local-wins', // Backup always uses local version
  })

  return {
    filesUploaded: result.uploaded.length,
    skipped: diff.unchanged.length,
    bytesTransferred: result.uploaded.reduce((sum, f) => sum + f.size, 0),
  }
}
```

### Incremental Backup CLI

```bash
# Sync only changed files to backup
parquedb sync --strategy local-wins

# Check what would be synced
parquedb sync --dry-run --status
```

### Event-Based Incremental Backup

For finer-grained incremental backup, back up only new event segments:

```typescript
import { readEventManifest } from 'parquedb/events/manifest'

async function backupNewEvents(
  local: StorageBackend,
  remote: StorageBackend,
  lastBackupSeq: number
): Promise<number> {
  const manifest = await readEventManifest(local)

  // Find segments created since last backup
  const newSegments = manifest.segments.filter(seg => seg.seq > lastBackupSeq)

  for (const segment of newSegments) {
    const data = await local.read(segment.path)
    await remote.write(segment.path, data)
    console.log(`Backed up event segment: ${segment.path}`)
  }

  // Update manifest on remote
  await remote.write('events/manifest.json', JSON.stringify(manifest))

  return manifest.nextSeq - 1 // Return last backed-up sequence
}
```

---

## Point-in-Time Recovery

ParqueDB's event log enables recovery to any point in time. The event log captures every CREATE, UPDATE, and DELETE operation with before/after state.

### Understanding the Event Log

```typescript
interface Event {
  id: string           // ULID (sortable, unique)
  ts: number           // Timestamp (ms since epoch)
  target: string       // 'namespace:entityId'
  op: 'CREATE' | 'UPDATE' | 'DELETE'
  before: Variant | null  // State before operation
  after: Variant | null   // State after operation
  actor?: string       // Who performed the operation
  metadata?: unknown   // Additional context
}
```

### Reconstructing State at a Point in Time

```typescript
import { EventReplayer, BatchEventSource } from 'parquedb/events/replay'
import { loadEventSegments } from 'parquedb/events/segment'

async function pointInTimeRecovery(
  storage: StorageBackend,
  targetTimestamp: number
): Promise<Map<string, Entity>> {
  // Load all event segments
  const segments = await loadEventSegments(storage)

  // Create event source from segments
  const eventSource = new BatchEventSource(async (minTs, maxTs) => {
    return segments.filter(seg =>
      seg.minTs <= (maxTs ?? Infinity) &&
      seg.maxTs >= (minTs ?? 0)
    )
  })

  // Create replayer
  const replayer = new EventReplayer(eventSource)

  // Get all unique targets from events
  const targets = new Set<string>()
  for (const segment of segments) {
    for (const event of segment.events) {
      if (event.ts <= targetTimestamp) {
        targets.add(event.target)
      }
    }
  }

  // Replay each entity to the target timestamp
  const results = await replayer.replayEntities(
    Array.from(targets),
    { at: targetTimestamp }
  )

  // Build entity map
  const entities = new Map<string, Entity>()
  for (const [target, result] of results) {
    if (result.existed && result.state) {
      entities.set(target, result.state as Entity)
    }
  }

  return entities
}
```

### CLI Point-in-Time Query

```bash
# Query database state at a specific time
parquedb query posts --at "2026-02-01T12:00:00Z" --filter '{"status": "published"}'

# Export snapshot at a point in time
parquedb export --at "2026-02-01T12:00:00Z" --output snapshot-20260201.parquet
```

### Recovery Workflow

```
1. Identify target recovery time
         |
         v
2. Load event segments covering that time range
         |
         v
3. Replay events to reconstruct state
         |
         v
4. Verify recovered data matches expectations
         |
         v
5. Write recovered state to new database
         |
         v
6. Validate relationships and indexes
```

### Practical PITR Example

```typescript
async function recoverToPointInTime(
  backupStorage: StorageBackend,
  targetStorage: StorageBackend,
  recoveryPoint: Date
): Promise<RecoveryResult> {
  const targetTs = recoveryPoint.getTime()

  console.log(`Recovering to: ${recoveryPoint.toISOString()}`)

  // 1. Load events
  const eventSource = await createEventSourceFromStorage(backupStorage)
  const replayer = new EventReplayer(eventSource)

  // 2. Get all namespaces
  const namespaces = await listNamespaces(backupStorage)
  const recoveredEntities: Entity[] = []

  for (const ns of namespaces) {
    // 3. Get entity IDs from events
    const events = await eventSource.getEventsInRange(0, targetTs)
    const entityIds = new Set(
      events
        .filter(e => e.target.startsWith(`${ns}:`))
        .map(e => e.target.split(':')[1])
    )

    // 4. Replay each entity
    for (const entityId of entityIds) {
      const result = await replayer.replayEntity(`${ns}:${entityId}`, { at: targetTs })
      if (result.existed && result.state) {
        recoveredEntities.push({
          $id: `${ns}/${entityId}`,
          $type: ns,
          ...result.state,
        } as Entity)
      }
    }
  }

  // 5. Write to target storage
  const db = new ParqueDB({ storage: targetStorage })
  for (const entity of recoveredEntities) {
    await db.collection(entity.$type).create(entity)
  }

  // 6. Flush to Parquet
  await db.flush()

  return {
    recoveredEntities: recoveredEntities.length,
    namespaces: namespaces.length,
    recoveryPoint: recoveryPoint.toISOString(),
  }
}
```

---

## Cross-Region Backup

For disaster recovery, maintain backups in multiple geographic regions.

### R2 Multi-Region Strategy

```typescript
// Configure regional backup buckets
const backupConfig = {
  primary: {
    bucket: env.BUCKET_US,
    region: 'us',
  },
  secondary: {
    bucket: env.BUCKET_EU,
    region: 'eu',
  },
  tertiary: {
    bucket: env.BUCKET_APAC,
    region: 'apac',
  },
}

async function crossRegionBackup(
  localBackend: StorageBackend,
  config: typeof backupConfig
): Promise<void> {
  // Backup to all regions in parallel
  await Promise.all([
    backupToRegion(localBackend, config.primary),
    backupToRegion(localBackend, config.secondary),
    backupToRegion(localBackend, config.tertiary),
  ])
}

async function backupToRegion(
  local: StorageBackend,
  regionConfig: { bucket: R2Bucket; region: string }
): Promise<void> {
  const remote = new R2Backend(regionConfig.bucket)

  const engine = createSyncEngine({
    local,
    remote,
    databaseId: `backup-${regionConfig.region}`,
    name: `${regionConfig.region}-backup`,
  })

  await engine.push({ dryRun: false })
  console.log(`Backup to ${regionConfig.region} complete`)
}
```

### Wrangler Configuration for Multi-Region

```toml
# wrangler.toml

# Primary bucket (US)
[[r2_buckets]]
binding = "BUCKET_US"
bucket_name = "parquedb-backup-us"

# Secondary bucket (EU)
[[r2_buckets]]
binding = "BUCKET_EU"
bucket_name = "parquedb-backup-eu"
jurisdiction = "eu"

# Tertiary bucket (APAC)
[[r2_buckets]]
binding = "BUCKET_APAC"
bucket_name = "parquedb-backup-apac"
```

### Cross-Region Sync Worker

```typescript
export default {
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    // Run cross-region sync every hour
    if (event.cron === '0 * * * *') {
      await crossRegionBackup(
        new R2Backend(env.PRIMARY_BUCKET),
        {
          primary: { bucket: env.BUCKET_US, region: 'us' },
          secondary: { bucket: env.BUCKET_EU, region: 'eu' },
          tertiary: { bucket: env.BUCKET_APAC, region: 'apac' },
        }
      )
    }
  }
}
```

---

## Disaster Recovery Procedures

### Scenario 1: Accidental Data Deletion

**Symptoms**: User accidentally deleted critical entities.

**Recovery Steps**:

```bash
# 1. Identify deletion time from event log
parquedb events --filter '{"op": "DELETE"}' --after "2026-02-01" --namespace posts

# 2. Find the last good state before deletion
parquedb query posts --at "2026-02-01T11:59:00Z" --id "posts/critical-post"

# 3. Restore the specific entity
parquedb restore --from-event-log --entity "posts/critical-post" --at "2026-02-01T11:59:00Z"
```

**Programmatic Recovery**:

```typescript
async function recoverDeletedEntity(
  db: ParqueDB,
  entityId: string,
  beforeDeleteTime: Date
): Promise<Entity | null> {
  const eventSource = await db.getEventSource()
  const replayer = new EventReplayer(eventSource)

  const result = await replayer.replayEntity(entityId, {
    at: beforeDeleteTime.getTime(),
  })

  if (result.existed && result.state) {
    // Recreate the entity
    const [ns, id] = entityId.split(':')
    return await db.collection(ns).create({
      ...result.state,
      $id: `${ns}/${id}`,
    })
  }

  return null
}
```

### Scenario 2: Corrupted Data

**Symptoms**: Application errors due to invalid data state.

**Recovery Steps**:

```bash
# 1. Identify when corruption occurred
parquedb events --namespace corrupted-ns --format detailed

# 2. Create a point-in-time snapshot before corruption
parquedb export --at "2026-02-01T10:00:00Z" --namespace corrupted-ns --output clean-data.parquet

# 3. Clear corrupted data
parquedb drop corrupted-ns --confirm

# 4. Restore from clean snapshot
parquedb import --file clean-data.parquet --namespace corrupted-ns
```

### Scenario 3: Regional Outage

**Symptoms**: Primary region R2 bucket unavailable.

**Recovery Steps**:

```typescript
async function failoverToBackupRegion(
  env: Env,
  failedRegion: string
): Promise<void> {
  // 1. Identify available backup region
  const backupBucket = failedRegion === 'us' ? env.BUCKET_EU : env.BUCKET_US

  // 2. Verify backup integrity
  const backup = new R2Backend(backupBucket)
  const manifest = await readManifest(backup)

  if (!manifest) {
    throw new Error('Backup manifest not found')
  }

  // 3. Update application to use backup
  console.log(`Failing over to backup region`)
  console.log(`Last backup: ${manifest.lastSyncedAt}`)
  console.log(`Files: ${Object.keys(manifest.files).length}`)

  // 4. Optionally restore to a new primary bucket
  // await restoreFromBackup(backup, env.NEW_PRIMARY_BUCKET)
}
```

### Scenario 4: Complete Data Loss

**Symptoms**: All primary and event data unavailable.

**Recovery Steps**:

```bash
# 1. Pull from remote backup
parquedb pull backup-username/production-backup --directory ./restored

# 2. Verify data integrity
parquedb verify ./restored

# 3. Check entity counts
parquedb stats ./restored

# 4. Start application with restored data
PARQUEDB_PATH=./restored npm start
```

---

## Restore Procedures

### Full Database Restore

```typescript
import { SyncEngine, createSyncEngine } from 'parquedb/sync/engine'

async function fullRestore(
  backupBucket: R2Bucket,
  targetDir: string,
  backupPath?: string
): Promise<RestoreResult> {
  const remote = new R2Backend(backupBucket, {
    prefix: backupPath ?? '',
  })
  const local = new FsBackend(targetDir)

  const engine = createSyncEngine({
    local,
    remote,
    databaseId: 'restore',
    name: 'restore-operation',
    onProgress: (progress) => {
      console.log(`Restoring: ${progress.currentFile} (${progress.percent}%)`)
    },
  })

  const result = await engine.pull({
    conflictStrategy: 'remote-wins', // Backup always wins
  })

  return {
    filesRestored: result.downloaded.length,
    totalBytes: result.downloaded.reduce((sum, f) => sum + f.size, 0),
    timestamp: new Date().toISOString(),
  }
}
```

### Selective Restore

Restore specific namespaces or entities:

```typescript
async function selectiveRestore(
  backup: StorageBackend,
  target: StorageBackend,
  namespaces: string[]
): Promise<void> {
  for (const ns of namespaces) {
    // Restore entity data
    const dataPath = `data/${ns}/data.parquet`
    if (await backup.exists(dataPath)) {
      const data = await backup.read(dataPath)
      await target.write(dataPath, data)
    }

    // Restore relationships
    const fwdPath = `rels/forward/${ns}.parquet`
    const revPath = `rels/reverse/${ns}.parquet`

    if (await backup.exists(fwdPath)) {
      await target.write(fwdPath, await backup.read(fwdPath))
    }
    if (await backup.exists(revPath)) {
      await target.write(revPath, await backup.read(revPath))
    }

    // Restore indexes
    const bloomPath = `indexes/bloom/${ns}.bloom`
    if (await backup.exists(bloomPath)) {
      await target.write(bloomPath, await backup.read(bloomPath))
    }
  }

  console.log(`Restored namespaces: ${namespaces.join(', ')}`)
}
```

### CLI Restore Commands

```bash
# Full restore from remote backup
parquedb pull username/backup-20260201 --directory ./restored

# Selective restore
parquedb pull username/backup --directory ./restored --namespaces posts,users

# Restore with conflict handling
parquedb pull username/backup --strategy remote-wins
```

---

## Restore Verification

Always verify restored data before putting it into production.

### Automated Verification

```typescript
interface VerificationResult {
  passed: boolean
  checks: VerificationCheck[]
  warnings: string[]
  errors: string[]
}

interface VerificationCheck {
  name: string
  passed: boolean
  expected?: unknown
  actual?: unknown
}

async function verifyRestore(
  original: StorageBackend,
  restored: StorageBackend
): Promise<VerificationResult> {
  const checks: VerificationCheck[] = []
  const warnings: string[] = []
  const errors: string[] = []

  // 1. Verify manifest exists
  const manifest = await readManifest(restored)
  checks.push({
    name: 'Manifest exists',
    passed: manifest !== null,
  })

  if (!manifest) {
    errors.push('Manifest not found in restored data')
    return { passed: false, checks, warnings, errors }
  }

  // 2. Verify file counts match
  const originalManifest = await readManifest(original)
  if (originalManifest) {
    const originalCount = Object.keys(originalManifest.files).length
    const restoredCount = Object.keys(manifest.files).length
    checks.push({
      name: 'File count matches',
      passed: originalCount === restoredCount,
      expected: originalCount,
      actual: restoredCount,
    })
  }

  // 3. Verify file hashes
  for (const [path, entry] of Object.entries(manifest.files)) {
    const exists = await restored.exists(path)
    if (!exists) {
      errors.push(`Missing file: ${path}`)
      continue
    }

    const data = await restored.read(path)
    const hash = await computeHash(data, entry.hashAlgorithm)

    if (hash !== entry.hash) {
      errors.push(`Hash mismatch for ${path}: expected ${entry.hash}, got ${hash}`)
    }
  }

  checks.push({
    name: 'All file hashes valid',
    passed: errors.filter(e => e.includes('Hash mismatch')).length === 0,
  })

  // 4. Verify Parquet files are readable
  const namespaces = await listNamespaces(restored)
  for (const ns of namespaces) {
    try {
      const dataPath = `data/${ns}/data.parquet`
      if (await restored.exists(dataPath)) {
        const data = await restored.read(dataPath)
        await verifyParquetFile(data)
      }
    } catch (error) {
      errors.push(`Invalid Parquet file for namespace ${ns}: ${error}`)
    }
  }

  checks.push({
    name: 'All Parquet files readable',
    passed: errors.filter(e => e.includes('Invalid Parquet')).length === 0,
  })

  // 5. Verify entity counts
  for (const ns of namespaces) {
    const originalCount = await countEntities(original, ns)
    const restoredCount = await countEntities(restored, ns)

    if (originalCount !== restoredCount) {
      warnings.push(
        `Entity count mismatch for ${ns}: expected ${originalCount}, got ${restoredCount}`
      )
    }
  }

  checks.push({
    name: 'Entity counts match',
    passed: warnings.filter(w => w.includes('Entity count')).length === 0,
  })

  // 6. Verify relationships
  const relCheck = await verifyRelationships(restored)
  checks.push({
    name: 'Relationships valid',
    passed: relCheck.valid,
  })
  if (!relCheck.valid) {
    errors.push(...relCheck.errors)
  }

  return {
    passed: errors.length === 0,
    checks,
    warnings,
    errors,
  }
}
```

### CLI Verification

```bash
# Verify restored database
parquedb verify ./restored

# Compare with original
parquedb verify ./restored --compare ./original

# Detailed verification report
parquedb verify ./restored --format detailed --output verification-report.json
```

### Verification Checklist

| Check | Description | Severity |
|-------|-------------|----------|
| Manifest exists | `_meta/manifest.json` present | Critical |
| File count | All files in manifest exist | Critical |
| Hash verification | File hashes match manifest | Critical |
| Parquet validity | All `.parquet` files readable | Critical |
| Entity counts | Entity counts match expectations | Warning |
| Relationships | Forward/reverse relationships consistent | Warning |
| Indexes | Bloom filters valid | Warning |
| Event log | Event segments readable and ordered | Warning |

---

## Automated Backup Scheduling

### Cloudflare Workers Cron

```typescript
// backup-worker.ts
export default {
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    const backupType = getBackupType(event.cron)

    switch (backupType) {
      case 'hourly':
        // Incremental backup every hour
        await incrementalBackup(env)
        break

      case 'daily':
        // Full backup at midnight UTC
        await fullBackup(env, `daily/${formatDate(new Date())}`)
        break

      case 'weekly':
        // Cross-region sync on Sundays
        await crossRegionBackup(env)
        break
    }
  }
}

function getBackupType(cron: string): 'hourly' | 'daily' | 'weekly' {
  if (cron === '0 * * * *') return 'hourly'
  if (cron === '0 0 * * *') return 'daily'
  if (cron === '0 0 * * 0') return 'weekly'
  return 'hourly'
}
```

### Wrangler Cron Configuration

```toml
# wrangler.toml

[triggers]
crons = [
  "0 * * * *",   # Hourly incremental
  "0 0 * * *",   # Daily full backup
  "0 0 * * 0"    # Weekly cross-region
]
```

### GitHub Actions Backup

```yaml
# .github/workflows/backup.yml
name: Database Backup

on:
  schedule:
    - cron: '0 0 * * *'  # Daily at midnight UTC
  workflow_dispatch:      # Allow manual triggers

jobs:
  backup:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Install dependencies
        run: npm ci

      - name: Run backup
        env:
          CLOUDFLARE_API_TOKEN: ${{ secrets.CF_API_TOKEN }}
          CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CF_ACCOUNT_ID }}
        run: |
          npx parquedb push --visibility private --slug backups/$(date +%Y%m%d)

      - name: Verify backup
        run: |
          npx parquedb pull backups/$(date +%Y%m%d) --directory ./verify
          npx parquedb verify ./verify

      - name: Notify on failure
        if: failure()
        uses: actions/github-script@v7
        with:
          script: |
            github.rest.issues.create({
              owner: context.repo.owner,
              repo: context.repo.repo,
              title: 'Backup Failed',
              body: 'Daily backup failed. Check workflow run for details.',
              labels: ['backup', 'urgent']
            })
```

---

## Best Practices

### Backup Strategy

1. **Follow 3-2-1 Rule**: 3 copies, 2 different media types, 1 offsite
2. **Test Restores Regularly**: Monthly restore drills to verify procedures
3. **Document Recovery Time**: Know your actual RTO, not theoretical
4. **Version Backups**: Keep multiple backup versions (daily for 7 days, weekly for 4 weeks, monthly for 12 months)

### Event Log Management

1. **Set Appropriate Retention**: Balance recovery needs with storage costs
2. **Compact Regularly**: Run compaction to optimize event log size
3. **Archive Old Events**: Move old event segments to cold storage

```typescript
// Recommended retention policy
const retentionPolicy = {
  eventLog: '90d',      // Keep 90 days of events
  dailyBackups: '7d',   // Keep 7 daily backups
  weeklyBackups: '4w',  // Keep 4 weekly backups
  monthlyBackups: '12m', // Keep 12 monthly backups
}
```

### Monitoring and Alerting

```typescript
// Monitor backup health
async function checkBackupHealth(env: Env): Promise<BackupHealthStatus> {
  const lastBackup = await getLastBackupTime(env.BACKUP_BUCKET)
  const hoursSinceBackup = (Date.now() - lastBackup) / (1000 * 60 * 60)

  return {
    healthy: hoursSinceBackup < 2, // Alert if no backup in 2 hours
    lastBackupTime: new Date(lastBackup).toISOString(),
    hoursSinceBackup,
  }
}
```

### Security Considerations

1. **Encrypt Backups at Rest**: Use R2's built-in encryption
2. **Restrict Backup Access**: Separate API tokens for backup operations
3. **Audit Backup Access**: Log all backup/restore operations
4. **Secure Transfer**: Always use HTTPS for backup transfers

```typescript
// Backup with encryption metadata
await storage.write(path, data, {
  metadata: {
    'x-amz-server-side-encryption': 'AES256',
    'x-backup-timestamp': new Date().toISOString(),
    'x-backup-operator': operatorId,
  }
})
```

---

## Next Steps

- [R2 Setup](./r2-setup.md) - Configure R2 storage for backups
- [Database Sync](../SYNC.md) - Detailed sync system documentation
- [Consistency Model](../architecture/consistency.md) - Understand data consistency
- [Cloudflare Workers](./cloudflare-workers.md) - Deploy backup Workers
