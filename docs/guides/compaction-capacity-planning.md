---
title: Compaction Capacity Planning Guide
description: Memory requirements, R2 costs, sharding decisions, and performance tuning for ParqueDB compaction workflows
---

This guide provides comprehensive capacity planning information for ParqueDB's event-driven compaction system. Use this document to estimate resource requirements, plan costs, determine when to enable sharding, and optimize performance.

## Table of Contents

- [Overview](#overview)
- [Memory Requirements](#memory-requirements)
  - [Workflow Memory Model](#workflow-memory-model)
  - [Memory Sizing by Data Volume](#memory-sizing-by-data-volume)
  - [Streaming Merge Configuration](#streaming-merge-configuration)
- [R2 Storage and Operation Costs](#r2-storage-and-operation-costs)
  - [Storage Costs](#storage-costs)
  - [Operation Costs](#operation-costs)
  - [Cost Estimation Examples](#cost-estimation-examples)
- [Cloudflare Workers and Workflows Costs](#cloudflare-workers-and-workflows-costs)
- [When to Enable Sharding](#when-to-enable-sharding)
  - [Namespace Sharding](#namespace-sharding)
  - [Time Bucket Sharding](#time-bucket-sharding)
  - [Partition Sharding](#partition-sharding)
- [Scaling Considerations](#scaling-considerations)
  - [Write Throughput Scaling](#write-throughput-scaling)
  - [Compaction Throughput Scaling](#compaction-throughput-scaling)
  - [Multi-Namespace Scaling](#multi-namespace-scaling)
- [Performance Tuning](#performance-tuning)
  - [Window Configuration](#window-configuration)
  - [Batch Size Optimization](#batch-size-optimization)
  - [Priority-Based Scheduling](#priority-based-scheduling)
- [Cost Optimization Strategies](#cost-optimization-strategies)
  - [Reducing R2 Operations](#reducing-r2-operations)
  - [Workflow Efficiency](#workflow-efficiency)
  - [Storage Optimization](#storage-optimization)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Capacity Planning Worksheets](#capacity-planning-worksheets)

---

## Overview

ParqueDB's compaction system transforms raw event files into optimized Parquet segments. The key components that affect capacity planning are:

```
Writers → R2 (raw files) → Event Notification → Queue → CompactionStateDO → Workflow
                                                              ↓
                                                    Compacted Parquet files
```

**Key resources to plan for:**

| Resource | What Affects It | Primary Cost Driver |
|----------|----------------|---------------------|
| Memory | Row count per window, average row size | Workflow execution |
| R2 Storage | Data volume, retention period | Storage tier |
| R2 Operations | Write frequency, compaction frequency | Class A/B operations |
| Workers | Request volume, queue messages | Invocations |
| Workflows | Number of compaction runs | Step executions |
| Durable Objects | Namespace count, update frequency | Requests |

---

## Memory Requirements

### Workflow Memory Model

Cloudflare Workers have a **128MB memory limit**. Compaction workflows must operate within this constraint. Memory usage is determined by:

```
Peak Memory = (Rows in batch) x (Average row size) x (Multiplier for processing)
```

The processing multiplier accounts for:
- Reading Parquet files into memory (1x)
- Deserialized row objects (1.2-1.5x depending on data types)
- Merge-sort working memory (0.2-0.5x)
- Output buffer before write (1x)

**Safe rule of thumb**: Peak memory is approximately **3x** the raw data size being processed.

### Memory Sizing by Data Volume

| Rows per Batch | Avg Row Size | Raw Data Size | Est. Peak Memory | Safe? |
|----------------|--------------|---------------|------------------|-------|
| 1,000 | 500 bytes | 500 KB | 1.5 MB | Yes |
| 10,000 | 500 bytes | 5 MB | 15 MB | Yes |
| 50,000 | 500 bytes | 25 MB | 75 MB | Yes |
| 100,000 | 500 bytes | 50 MB | 150 MB | **No** - use streaming |
| 10,000 | 2 KB | 20 MB | 60 MB | Yes |
| 50,000 | 2 KB | 100 MB | 300 MB | **No** - use streaming |
| 10,000 | 10 KB | 100 MB | 300 MB | **No** - use streaming |

**Recommendation**: For windows with more than **40MB of raw data**, enable streaming merge.

### Streaming Merge Configuration

When data exceeds memory limits, enable streaming merge for memory-bounded k-way merge sort:

```typescript
// In workflow params
{
  useStreamingMerge: 'auto',  // Automatically detect based on data size
  maxStreamingMemoryBytes: 128 * 1024 * 1024,  // 128MB
  estimatedAvgRowBytes: 500,  // Tune based on your data
}
```

**`useStreamingMerge` options:**
- `'auto'` (default): Automatically use streaming when estimated memory exceeds threshold
- `true`: Always use streaming merge
- `false`: Always use in-memory merge (may OOM on large windows)

**Tuning streaming merge:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `maxStreamingMemoryBytes` | 128MB | Maximum heap for merge operation |
| `estimatedAvgRowBytes` | 500 | Used to estimate row count from file sizes |
| `maxFilesPerStep` | 50 | Files processed per workflow step |

**Chunk size calculation:**

```
Optimal chunk size = maxStreamingMemoryBytes / (file_count x avgRowBytes x 2)
```

The streaming sorter uses a min-heap to merge pre-sorted chunks from each file, ensuring memory stays bounded regardless of total data size.

---

## R2 Storage and Operation Costs

### Storage Costs

R2 storage pricing (as of 2026):

| Tier | Price | Notes |
|------|-------|-------|
| Standard | $0.015/GB/month | First 10GB free |
| Infrequent Access | $0.01/GB/month | For data accessed < 1x/month |

**Storage estimation formula:**

```
Monthly Storage = (Daily writes x Avg entity size x Retention days) + (Compacted data)
```

**Example calculation:**

| Scenario | Daily Writes | Avg Size | Retention | Compaction Ratio | Est. Storage |
|----------|--------------|----------|-----------|------------------|--------------|
| Small app | 10,000 | 1 KB | 30 days | 3:1 | ~100 MB |
| Medium app | 100,000 | 1 KB | 30 days | 3:1 | ~1 GB |
| Large app | 1,000,000 | 1 KB | 30 days | 3:1 | ~10 GB |
| Enterprise | 10,000,000 | 2 KB | 90 days | 3:1 | ~600 GB |

**Parquet compression benefits:**

| Codec | Compression Ratio | Read Speed | Write Speed |
|-------|-------------------|------------|-------------|
| None | 1:1 | Fastest | Fastest |
| Snappy | 2-3:1 | Fast | Fast |
| LZ4 | 2.5-3.5:1 | Fast | Fast |
| GZIP | 4-6:1 | Slower | Slower |
| ZSTD | 4-7:1 | Medium | Medium |

**Recommendation**: Use Snappy for production workloads (best balance of speed and compression).

### Operation Costs

R2 operation pricing:

| Operation | Class | Price | Free Tier |
|-----------|-------|-------|-----------|
| PUT, POST, LIST | Class A | $4.50/million | 1M/month |
| GET, HEAD | Class B | $0.36/million | 10M/month |
| DELETE | Free | $0 | Unlimited |
| Egress | Free | $0 | Unlimited |

**Compaction operation breakdown:**

Per compaction window:
1. **Read source files**: 1 GET per file (Class B)
2. **Write compacted file**: 1 PUT (Class A)
3. **Delete source files**: N DELETEs (Free)
4. **Update metadata**: 1-2 PUTs (Class A)

**Formula:**
```
Operations per window = (source_files x 1 GET) + (2-3 PUTs) + (source_files DELETEs)
```

### Cost Estimation Examples

#### Small Application (10K writes/day)

| Item | Quantity | Unit Cost | Monthly Cost |
|------|----------|-----------|--------------|
| Storage | 100 MB | $0.015/GB | $0.00 (free tier) |
| Class A ops | 1,000 | $4.50/M | $0.00 (free tier) |
| Class B ops | 10,000 | $0.36/M | $0.00 (free tier) |
| **Total** | | | **$0.00** |

#### Medium Application (100K writes/day)

| Item | Quantity | Unit Cost | Monthly Cost |
|------|----------|-----------|--------------|
| Storage | 1 GB | $0.015/GB | $0.015 |
| Class A ops | 10,000 | $4.50/M | $0.045 |
| Class B ops | 100,000 | $0.36/M | $0.036 |
| **Total** | | | **~$0.10** |

#### Large Application (1M writes/day)

| Item | Quantity | Unit Cost | Monthly Cost |
|------|----------|-----------|--------------|
| Storage | 10 GB | $0.015/GB | $0.15 |
| Class A ops | 100,000 | $4.50/M | $0.45 |
| Class B ops | 1,000,000 | $0.36/M | $0.36 |
| **Total** | | | **~$1.00** |

#### Enterprise (10M writes/day)

| Item | Quantity | Unit Cost | Monthly Cost |
|------|----------|-----------|--------------|
| Storage | 600 GB | $0.015/GB | $9.00 |
| Class A ops | 1,000,000 | $4.50/M | $4.50 |
| Class B ops | 10,000,000 | $0.36/M | $3.60 |
| **Total** | | | **~$17.10** |

---

## Cloudflare Workers and Workflows Costs

### Workers Pricing

| Component | Free Tier | Paid Plan |
|-----------|-----------|-----------|
| Requests | 100K/day | $0.30/million |
| CPU time | 10ms | $0.02/million CPU-ms (Unbound) |
| Memory | 128MB | 128MB |

### Workflows Pricing

| Component | Price |
|-----------|-------|
| Step executions | $0.15/1,000 steps |
| Sleep (per day) | 1 step equivalent |

**Compaction workflow steps per run:**
- Analyze files: 1 step
- Grace period: 1 step
- Check late writers: 1 step
- Process batch: 1 step per batch
- Cooldown: 1 step per batch (except last)
- Finalize: 1 step

**Formula:**
```
Steps per workflow = 4 + (batches x 2) - 1
```

**Example**: Window with 200 files, 50 files/batch = 4 batches
- Steps = 4 + (4 x 2) - 1 = **11 steps**
- Cost = 11 x $0.00015 = **$0.00165** per compaction

### Queue Pricing

| Component | Price |
|-----------|-------|
| Standard operations | $0.40/million |
| Batch messages | Counted per message |

**Queue usage per write:**
- 1 R2 event notification per file written
- Batched processing (up to 100 messages per invocation)

### Durable Objects Pricing

| Component | Price |
|-----------|-------|
| Requests | $0.15/million |
| Storage | $0.20/GB/month |
| Duration | $12.50/million GB-s |

**CompactionStateDO usage:**
- 1 request per queue batch (batched updates)
- Minimal storage (window metadata only)
- Short duration (stateless between requests)

### Complete Cost Example (1M writes/day)

| Component | Daily Usage | Monthly Cost |
|-----------|-------------|--------------|
| R2 Storage | 10 GB | $0.15 |
| R2 Class A | 100K ops | $0.45 |
| R2 Class B | 1M ops | $0.36 |
| Queue | 1M messages | $0.40 |
| Workflows | 720 runs x 11 steps | $1.19 |
| Workers | 10K invocations | $0.003 |
| Durable Objects | 10K requests | $0.0015 |
| **Total** | | **~$2.55/month** |

---

## When to Enable Sharding

### Namespace Sharding

ParqueDB automatically uses namespace sharding - each namespace gets its own `CompactionStateDO` instance. This is **always enabled** and requires no configuration.

**Benefits:**
- Eliminates single-DO bottleneck
- Parallel compaction across namespaces
- Independent scaling per namespace

**When to create more namespaces:**
- Logical data separation (e.g., by tenant, data type)
- Different retention/compaction policies needed
- Performance isolation requirements

### Time Bucket Sharding

For high-throughput namespaces (>1,000 writes/second), enable time bucket sharding:

```typescript
// In queue consumer config
{
  timeBucketSharding: {
    enabled: true,
    namespacesWithSharding: ['high-volume-namespace'],
    bucketSizeMs: 3600000,  // 1 hour buckets
    maxBucketAgeHours: 24,
  }
}
```

**When to enable:**

| Writes/Second | Recommendation |
|---------------|----------------|
| < 100 | Namespace sharding only |
| 100 - 1,000 | Namespace sharding sufficient |
| 1,000 - 10,000 | Consider time bucket sharding |
| > 10,000 | Time bucket sharding required |

**Trade-offs:**
- **Pro**: Eliminates DO contention at extreme scale
- **Con**: More complex status aggregation
- **Con**: More DO instances (storage/cost)

### Partition Sharding

For namespaces exceeding 1GB or 10 million entities, consider partition sharding within the namespace:

| Shard Strategy | Best For | Configuration |
|----------------|----------|---------------|
| Type-based | Distinct entity types | `shard_strategy: 'type'` |
| Time-based | Time-series data | `shard_strategy: 'time'` |
| Hash-based | Uniform access patterns | `shard_strategy: 'hash'` |

**File structure with sharding:**

```
{ns}/
├── data.parquet              # Small/legacy types
└── _shards/
    ├── type=Person/
    │   └── data.parquet
    └── type=Order/
        └── data.parquet
```

**Thresholds for partition sharding:**

| Metric | Threshold | Action |
|--------|-----------|--------|
| File size | > 1 GB | Enable sharding |
| Entity count | > 10 million | Enable sharding |
| Row groups | > 1,000 | Enable sharding |

---

## Scaling Considerations

### Write Throughput Scaling

| Scale | Architecture | Notes |
|-------|--------------|-------|
| < 100/sec | Single writer | Simple, no coordination needed |
| 100-1,000/sec | Multiple writers | Writer IDs in filenames for merge-sort |
| 1,000-10,000/sec | Sharded writers + time buckets | Enable time bucket sharding |
| > 10,000/sec | Partitioned namespace | Consider multiple namespaces |

**Writer coordination:**
- Each writer appends a unique ID to filenames
- Compaction workflow performs merge-sort across writers
- Writers don't need coordination - just write to R2

### Compaction Throughput Scaling

**Factors limiting compaction throughput:**

1. **Workflow execution rate**: Cloudflare limits concurrent workflows
2. **R2 read throughput**: Parallel reads improve performance
3. **Memory constraints**: Large windows need streaming merge
4. **Workflow step limits**: More batches = more steps = higher cost

**Scaling strategies:**

| Bottleneck | Solution |
|------------|----------|
| Single namespace backlog | Increase `maxFilesPerStep` |
| Memory limits | Enable streaming merge |
| Workflow rate | Reduce window size (more frequent compaction) |
| Cross-namespace | Namespaces compact in parallel |

### Multi-Namespace Scaling

Compaction scales linearly with namespaces because each namespace:
- Has its own `CompactionStateDO`
- Triggers independent workflows
- Uses separate R2 prefixes

**Resource scaling by namespace count:**

| Namespaces | DO Instances | Max Concurrent Workflows |
|------------|--------------|--------------------------|
| 1 | 1 | 1-5 |
| 10 | 10 | 10-50 |
| 100 | 100 | 100-500 |

**Note**: Workflows for different namespaces run in parallel automatically.

---

## Performance Tuning

### Window Configuration

| Parameter | Default | Range | Impact |
|-----------|---------|-------|--------|
| `windowSizeMs` | 1 hour | 5min - 24hr | Smaller = more frequent compaction |
| `minFilesToCompact` | 10 | 1-1000 | Higher = fewer, larger compactions |
| `maxWaitTimeMs` | 5 minutes | 1min - 1hr | Grace period for late writers |

**Tuning recommendations:**

| Use Case | Window Size | Min Files | Max Wait |
|----------|-------------|-----------|----------|
| Real-time analytics | 5-15 min | 5 | 1 min |
| Standard workload | 1 hour | 10 | 5 min |
| Batch processing | 6-24 hours | 50 | 30 min |
| Low volume | 24 hours | 100 | 1 hour |

### Batch Size Optimization

The `maxFilesPerStep` parameter controls how many files are processed per workflow step:

| Files/Step | Memory Impact | Cost Impact | Throughput |
|------------|---------------|-------------|------------|
| 10 | Low | Higher (more steps) | Lower |
| 50 (default) | Medium | Balanced | Good |
| 100 | High | Lower (fewer steps) | Higher |
| 200 | Very High | Lowest | Highest |

**Formula for optimal batch size:**

```
maxFilesPerStep = min(
  maxStreamingMemoryBytes / (avgFileSize x 3),
  desiredFilesPerStep
)
```

### Priority-Based Scheduling

Configure namespace priority for scheduling under load:

```typescript
// Set via CompactionStateDO /config endpoint
await stateDO.fetch('http://internal/config', {
  method: 'POST',
  body: JSON.stringify({ priority: 0 })  // 0 = critical, 3 = background
})
```

| Priority | Max Wait Time | Backpressure Behavior |
|----------|---------------|------------------------|
| P0 (Critical) | 1 minute | Never skipped |
| P1 (High) | 5 minutes | Skipped under severe backpressure |
| P2 (Medium) | 15 minutes | Skipped under severe backpressure |
| P3 (Background) | 1 hour | Skipped under any backpressure |

**Backpressure thresholds:**
- Normal: 10+ pending windows
- Severe: 20+ pending windows

---

## Cost Optimization Strategies

### Reducing R2 Operations

1. **Batch writes**: Group multiple entities into single Parquet files
   - 100 entities/file instead of 1 entity/file = 100x fewer operations

2. **Increase window size**: Fewer, larger compactions
   - 1-hour windows vs 5-minute = 12x fewer compaction runs

3. **Increase minFilesToCompact**: Batch more files per compaction
   - 50 files vs 10 = 5x fewer compactions

4. **Use efficient compression**: Reduce file sizes
   - Snappy compression = 2-3x smaller files

### Workflow Efficiency

1. **Increase maxFilesPerStep**: Fewer workflow steps
   - 100 files/step vs 50 = ~50% fewer steps

2. **Tune streaming thresholds**: Only stream when necessary
   - In-memory merge is faster for small windows

3. **Optimize window timing**: Match to data patterns
   - Align windows to natural data boundaries (hourly, daily)

### Storage Optimization

1. **Enable compression**: Use Snappy (default)
   - Typically 2-3x compression ratio

2. **Configure retention**: Delete old data
   - Event retention can be shorter than entity retention

3. **Use Infrequent Access tier**: For archival data
   - 33% cheaper for rarely accessed data

4. **Compact aggressively**: Reduce file count
   - Many small files = more overhead than fewer large files

---

## Monitoring and Alerting

### Key Metrics to Monitor

| Metric | Source | Warning Threshold | Critical Threshold |
|--------|--------|-------------------|-------------------|
| Pending windows | CompactionStateDO | > 10 | > 20 |
| Oldest window age | CompactionStateDO | > 2 hours | > 6 hours |
| Windows stuck in processing | CompactionStateDO | > 0 | > 2 |
| Workflow failures | Cloudflare dashboard | > 1% | > 5% |
| Memory usage | Workflow logs | > 100MB | > 120MB |
| Queue backlog | Cloudflare Queues | > 10K | > 100K |

### Health Check Endpoints

```bash
# Check compaction status for a namespace
curl https://your-worker.workers.dev/compaction/status?namespace=users

# Check overall compaction health
curl https://your-worker.workers.dev/compaction/health
```

**Health response format:**

```json
{
  "status": "healthy",
  "namespaces": {
    "users": {
      "status": "healthy",
      "metrics": {
        "activeWindows": 3,
        "oldestWindowAge": 1800000,
        "totalPendingFiles": 45,
        "windowsStuckInProcessing": 0
      },
      "issues": []
    }
  },
  "alerts": []
}
```

### Alerting Rules

Configure alerts for:

1. **Window backlog**: Pending windows > threshold
2. **Stuck processing**: Windows in processing > 5 minutes
3. **Workflow failures**: Failed workflow rate > 1%
4. **Memory pressure**: OOM errors in workflow logs
5. **Queue backlog**: Messages > 10K

---

## Capacity Planning Worksheets

### Worksheet 1: Estimate Monthly Costs

Fill in your values:

```
Daily write volume:           ___________ writes/day
Average entity size:          ___________ bytes
Retention period:             ___________ days
Compression ratio:            ___________ (typically 2-3x)

Estimated storage:
  Raw daily data = writes/day x entity_size = __________ bytes
  Monthly raw = daily x 30 = __________ GB
  Compressed = monthly_raw / compression_ratio = __________ GB
  With retention = compressed x (retention/30) = __________ GB

  Storage cost = GB x $0.015 = $__________

Estimated operations:
  Write ops/day = writes/day = __________
  Compaction ops/day = writes/day / files_per_window x 3 = __________
  Read ops/day = compactions x files_per_window = __________

  Monthly Class A = (write_ops + compaction_ops) x 30 = __________
  Monthly Class B = read_ops x 30 = __________

  Class A cost = (ops - 1M) x $0.0000045 = $__________
  Class B cost = (ops - 10M) x $0.00000036 = $__________

Estimated workflow costs:
  Compactions/day = writes/day / (files_per_window x minFiles) = __________
  Steps/compaction = 4 + (files_per_window / maxFilesPerStep x 2) = __________
  Monthly steps = compactions x 30 x steps = __________

  Workflow cost = steps x $0.00015 = $__________

Total monthly cost: $__________
```

### Worksheet 2: Memory Sizing

```
Files per window:             ___________ files
Average file size:            ___________ MB
Average rows per file:        ___________ rows
Average row size:             ___________ bytes

Total rows per window = files x rows_per_file = __________ rows
Raw data size = files x file_size = __________ MB
Peak memory estimate = raw_data x 3 = __________ MB

Decision:
  [ ] Peak memory < 40MB: Use in-memory merge (useStreamingMerge: false)
  [ ] Peak memory 40-100MB: Use auto (useStreamingMerge: 'auto')
  [ ] Peak memory > 100MB: Force streaming (useStreamingMerge: true)

If streaming:
  maxStreamingMemoryBytes = 128 * 1024 * 1024 = 134217728
  Optimal chunk size = maxMemory / (files x avgRowBytes x 2) = __________
```

### Worksheet 3: Sharding Decision

```
Write throughput:             ___________ writes/second
Namespace count:              ___________ namespaces
Largest namespace size:       ___________ GB
Largest namespace entities:   ___________ count

Namespace sharding: Always enabled (automatic)

Time bucket sharding:
  [ ] < 1,000 writes/sec: Not needed
  [ ] 1,000 - 10,000 writes/sec: Consider enabling
  [ ] > 10,000 writes/sec: Required

Partition sharding:
  [ ] Namespace < 1GB: Not needed
  [ ] Namespace 1-10GB: Consider type or time sharding
  [ ] Namespace > 10GB: Required - choose strategy:
      [ ] type: For distinct entity types
      [ ] time: For time-series data
      [ ] hash: For uniform access patterns
```

---

## Quick Reference

### Default Configuration Values

| Parameter | Default Value |
|-----------|---------------|
| `windowSizeMs` | 3,600,000 (1 hour) |
| `minFilesToCompact` | 10 |
| `maxWaitTimeMs` | 300,000 (5 minutes) |
| `maxFilesPerStep` | 50 |
| `useStreamingMerge` | 'auto' |
| `maxStreamingMemoryBytes` | 134,217,728 (128MB) |
| `estimatedAvgRowBytes` | 500 |
| `targetFormat` | 'native' |
| `deleteSource` | true |

### Cost Quick Reference

| Resource | Price | Free Tier |
|----------|-------|-----------|
| R2 Storage | $0.015/GB/month | 10 GB |
| R2 Class A | $4.50/million | 1 million |
| R2 Class B | $0.36/million | 10 million |
| Queue | $0.40/million messages | - |
| Workflow steps | $0.15/1,000 steps | - |
| DO requests | $0.15/million | - |

### Memory Limits

| Constraint | Limit |
|------------|-------|
| Worker memory | 128 MB |
| Worker CPU (Bundled) | 50 ms |
| Worker CPU (Unbound) | 30 seconds |
| Workflow step timeout | 15 minutes |
| DO storage | 10 GB per account |

---

*Last updated: February 2026*
