---
title: Production Runbook & Troubleshooting Guide
description: Comprehensive operational documentation for deploying, monitoring, and troubleshooting ParqueDB in production environments.
---

This document provides operational guidance for running ParqueDB in production, including deployment checklists, monitoring setup, common issues and resolutions, performance tuning, and incident response procedures.

## Table of Contents

- [Deployment Checklist](#deployment-checklist)
  - [Cloudflare Workers Setup](#cloudflare-workers-setup)
  - [R2 Bucket Configuration](#r2-bucket-configuration)
  - [Durable Objects Setup](#durable-objects-setup)
  - [Environment Variables](#environment-variables)
- [Monitoring Setup](#monitoring-setup)
  - [Key Metrics to Track](#key-metrics-to-track)
  - [Alerting Thresholds](#alerting-thresholds)
  - [Dashboard Recommendations](#dashboard-recommendations)
- [Common Issues & Resolutions](#common-issues--resolutions)
  - [Error Code Reference](#error-code-reference)
  - [Troubleshooting Steps](#troubleshooting-steps)
  - [FAQ](#faq)
- [Performance Tuning](#performance-tuning)
  - [Configuration Options](#configuration-options)
  - [Caching Strategies](#caching-strategies)
  - [Index Optimization](#index-optimization)
- [Incident Response](#incident-response)
  - [On-Call Procedures](#on-call-procedures)
  - [Escalation Paths](#escalation-paths)
  - [Post-Incident Review](#post-incident-review)

---

## Deployment Checklist

### Cloudflare Workers Setup

Complete these steps before deploying to production:

#### 1. Cloudflare Account Setup

- [ ] Cloudflare account created with Workers Paid plan ($5/month recommended)
- [ ] Wrangler CLI installed and authenticated (`wrangler login`)
- [ ] API token created with "Edit Cloudflare Workers" permissions
- [ ] API token stored in CI/CD secrets (e.g., `CLOUDFLARE_API_TOKEN`)

```bash
# Verify authentication
wrangler whoami

# Verify wrangler version
wrangler --version
```

#### 2. Project Configuration

- [ ] `wrangler.jsonc` created with required bindings
- [ ] `nodejs_compat` compatibility flag enabled
- [ ] Entry point exports `ParqueDBDO` class

```jsonc
// wrangler.jsonc - Required settings
{
  "name": "your-parquedb-api",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-30",
  "compatibility_flags": ["nodejs_compat"],

  "durable_objects": {
    "bindings": [{ "name": "PARQUEDB", "class_name": "ParqueDBDO" }]
  },

  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["ParqueDBDO"] }
  ],

  "r2_buckets": [
    { "binding": "BUCKET", "bucket_name": "your-bucket-name" }
  ]
}
```

#### 3. Entry Point Configuration

```typescript
// src/index.ts - Must export ParqueDBDO
export { ParqueDBDO } from 'parquedb/worker'

// Your Worker implementation
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    // ...
  }
}
```

#### 4. Pre-Deployment Verification

- [ ] Local development tested (`wrangler dev`)
- [ ] Health check endpoint responds (`/health`)
- [ ] Basic CRUD operations work
- [ ] R2 bucket accessible
- [ ] DO operations function correctly

```bash
# Test locally
wrangler dev

# Verify health
curl http://localhost:8787/health
```

### R2 Bucket Configuration

#### 1. Create Buckets

```bash
# Production bucket
wrangler r2 bucket create your-bucket-prod

# Staging bucket
wrangler r2 bucket create your-bucket-staging

# Preview bucket (local development)
wrangler r2 bucket create your-bucket-preview

# Verify buckets exist
wrangler r2 bucket list
```

#### 2. Bucket Configuration Checklist

- [ ] Production bucket created with appropriate region/jurisdiction
- [ ] Staging bucket created for pre-production testing
- [ ] Preview bucket created for local development
- [ ] CORS rules configured if direct browser access needed
- [ ] Lifecycle policies documented (ParqueDB handles cleanup)

#### 3. Bucket Naming Convention

| Environment | Bucket Name | Purpose |
|-------------|-------------|---------|
| Production | `{project}-parquedb-prod` | Production data |
| Staging | `{project}-parquedb-staging` | Pre-production testing |
| Preview | `{project}-parquedb-preview` | Local development |

#### 4. Data Residency (if required)

For EU or GDPR compliance:

```jsonc
// wrangler.jsonc
"r2_buckets": [
  {
    "binding": "BUCKET",
    "bucket_name": "your-bucket-eu",
    "jurisdiction": "eu"
  }
]
```

### Durable Objects Setup

#### 1. Migration Configuration

```jsonc
// wrangler.jsonc - Migrations are REQUIRED for SQLite DOs
"migrations": [
  {
    "tag": "v1",
    "new_sqlite_classes": ["ParqueDBDO"]
  }
]
```

#### 2. Adding New DO Classes

When adding new Durable Object classes, increment the migration tag:

```jsonc
"migrations": [
  { "tag": "v1", "new_sqlite_classes": ["ParqueDBDO"] },
  { "tag": "v2", "new_sqlite_classes": ["DatabaseIndexDO"] }
]
```

#### 3. DO Verification

- [ ] `ParqueDBDO` exported from entry point
- [ ] Migrations configured in `wrangler.jsonc`
- [ ] SQLite storage class specified
- [ ] DO binding name matches code (`env.PARQUEDB`)

### Environment Variables

#### 1. Non-Sensitive Variables (wrangler.jsonc)

```jsonc
"vars": {
  "ENVIRONMENT": "production",
  "LOG_LEVEL": "warn",

  // Cache configuration
  "CACHE_DATA_TTL": "60",
  "CACHE_METADATA_TTL": "300",
  "CACHE_BLOOM_TTL": "600",
  "CACHE_STALE_WHILE_REVALIDATE": "true"
}
```

#### 2. Secrets (Wrangler CLI)

```bash
# Set secrets for production
wrangler secret put AUTH_SECRET --env production

# Set secrets for staging
wrangler secret put AUTH_SECRET --env staging

# List secrets
wrangler secret list
```

#### 3. Environment Variable Reference

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `ENVIRONMENT` | vars | `development` | Environment name |
| `LOG_LEVEL` | vars | `info` | Logging level (debug, info, warn, error) |
| `CACHE_DATA_TTL` | vars | `60` | Data cache TTL in seconds |
| `CACHE_METADATA_TTL` | vars | `300` | Metadata cache TTL in seconds |
| `CACHE_BLOOM_TTL` | vars | `600` | Bloom filter cache TTL in seconds |
| `CACHE_STALE_WHILE_REVALIDATE` | vars | `true` | Enable stale-while-revalidate |
| `AUTH_SECRET` | secret | - | Authentication secret |

#### 4. Deployment Commands

```bash
# Deploy to staging
wrangler deploy --env staging

# Deploy to production
wrangler deploy --env production

# Verify deployment
wrangler deployments list
```

---

## Monitoring Setup

### Key Metrics to Track

#### 1. Request Metrics

| Metric | Description | Normal Range |
|--------|-------------|--------------|
| **Request Rate** | Requests per second | Varies by load |
| **Error Rate** | Percentage of 4xx/5xx responses | < 1% |
| **Latency p50** | Median response time | < 50ms |
| **Latency p99** | 99th percentile response time | < 200ms |

#### 2. Durable Object Metrics

| Metric | Description | Warning Threshold |
|--------|-------------|-------------------|
| **DO Requests** | Requests routed to DOs | Monitor for spikes |
| **DO CPU Time** | CPU time per DO request | > 50ms |
| **DO Cold Starts** | Number of cold starts | High frequency indicates issues |
| **DO Memory** | Memory usage per DO | Approaching 128MB limit |

#### 3. R2 Metrics

| Metric | Description | Monitor For |
|--------|-------------|-------------|
| **Read Operations** | Class B operations (reads) | Cost monitoring |
| **Write Operations** | Class A operations (writes) | Cost monitoring |
| **Storage Size** | Total R2 storage used | Capacity planning |
| **Egress** | Data transferred out | Always $0 (R2 benefit) |

#### 4. Cache Metrics

| Metric | Description | Target |
|--------|-------------|--------|
| **Cache Hit Rate** | Percentage of cache hits | > 80% |
| **Cache Size** | Bytes in edge cache | Monitor growth |
| **Stale Serves** | Stale-while-revalidate serves | Normal behavior |

#### 5. Query Metrics

| Metric | Description | Target |
|--------|-------------|--------|
| **Query Latency** | Time to execute queries | p50 < 20ms |
| **Rows Scanned** | Row groups examined | Monitor for full scans |
| **Index Usage** | Percentage using indexes | > 90% for indexed fields |

### Alerting Thresholds

#### Critical Alerts (Page immediately)

| Condition | Threshold | Action |
|-----------|-----------|--------|
| Error Rate | > 5% for 5 minutes | Page on-call |
| P99 Latency | > 2s for 5 minutes | Page on-call |
| DO Unavailable | Any namespace unreachable | Page on-call |
| Worker Deployment Failed | Deployment error | Page on-call |

#### Warning Alerts (Notify team)

| Condition | Threshold | Action |
|-----------|-----------|--------|
| Error Rate | > 1% for 10 minutes | Slack notification |
| P99 Latency | > 500ms for 10 minutes | Slack notification |
| Cache Hit Rate | < 60% for 30 minutes | Slack notification |
| R2 Operations Spike | > 2x normal for 15 minutes | Slack notification |

#### Info Alerts (Log for review)

| Condition | Threshold | Action |
|-----------|-----------|--------|
| Cold Start Rate | > 10% of requests | Log for review |
| CPU Time Warning | > 30ms average | Log for review |
| Storage Growth | > 10% daily | Log for review |

### Dashboard Recommendations

#### 1. Overview Dashboard

Create a high-level dashboard with:

- Request rate and error rate graphs
- P50/P95/P99 latency percentiles
- Cache hit ratio
- Active DO instances by namespace
- R2 operations (reads/writes)

#### 2. Performance Dashboard

Deep-dive performance metrics:

- Query latency by operation type (find, get, create, update, delete)
- Row groups scanned per query
- Index hit rate
- Parquet file sizes and counts
- Memory usage trends

#### 3. Cost Dashboard

Track Cloudflare costs:

- Worker requests (daily/monthly)
- R2 Class A operations (writes)
- R2 Class B operations (reads)
- R2 storage size
- DO requests
- Projected monthly cost

#### 4. Cloudflare Analytics Integration

```typescript
// Log structured metrics
console.log(JSON.stringify({
  type: 'metric',
  operation: 'find',
  namespace: 'posts',
  latencyMs: 45,
  rowsReturned: 20,
  cacheHit: true,
  timestamp: new Date().toISOString()
}))
```

Use Wrangler tail to view logs in real-time:

```bash
# All logs
wrangler tail

# Filter by errors
wrangler tail --status error

# Filter by search term
wrangler tail --search "posts"

# JSON format for parsing
wrangler tail --format json
```

---

## Common Issues & Resolutions

### Error Code Reference

#### HTTP Status Codes

| Code | Meaning | Common Causes |
|------|---------|---------------|
| 400 | Bad Request | Invalid filter syntax, malformed JSON |
| 401 | Unauthorized | Missing or invalid auth token |
| 403 | Forbidden | Insufficient permissions |
| 404 | Not Found | Entity or namespace doesn't exist |
| 409 | Conflict | Optimistic concurrency failure (version mismatch) |
| 413 | Payload Too Large | Request body exceeds limit |
| 429 | Too Many Requests | Rate limit exceeded |
| 500 | Internal Server Error | Unexpected server error |
| 503 | Service Unavailable | DO overloaded or R2 unavailable |

#### ParqueDB Error Codes

| Error | Description | Resolution |
|-------|-------------|------------|
| `ENTITY_NOT_FOUND` | Entity with ID doesn't exist | Verify entity ID |
| `NAMESPACE_NOT_FOUND` | Namespace hasn't been created | Create namespace first |
| `VALIDATION_ERROR` | Data doesn't match schema | Check schema requirements |
| `VERSION_MISMATCH` | Optimistic concurrency failure | Retry with fresh data |
| `RELATIONSHIP_ERROR` | Invalid relationship target | Verify target entity exists |
| `INDEX_ERROR` | Index operation failed | Rebuild index |
| `PARQUET_ERROR` | Parquet file corrupted/invalid | Check file integrity |
| `R2_ERROR` | R2 operation failed | Check R2 status |
| `DO_ERROR` | Durable Object error | Check DO logs |

### Troubleshooting Steps

#### "Durable Object not found"

**Symptoms:**
- 500 errors when creating/updating entities
- Error message: `Durable Object not found` or `Class not found`

**Diagnosis:**

1. Check DO class is exported:
```typescript
// src/index.ts must have:
export { ParqueDBDO } from 'parquedb/worker'
```

2. Verify migrations in wrangler.jsonc:
```jsonc
"migrations": [
  { "tag": "v1", "new_sqlite_classes": ["ParqueDBDO"] }
]
```

3. Check deployment includes DO:
```bash
wrangler deployments list
```

**Resolution:**
1. Add missing export
2. Add/fix migrations
3. Redeploy: `wrangler deploy`

---

#### "R2 bucket not found"

**Symptoms:**
- 500 errors on read operations
- Error: `R2 bucket not found` or `BUCKET is not defined`

**Diagnosis:**

1. Verify bucket exists:
```bash
wrangler r2 bucket list
```

2. Check binding name in wrangler.jsonc matches code:
```jsonc
"r2_buckets": [
  { "binding": "BUCKET", "bucket_name": "your-actual-bucket" }
]
```

3. Verify bucket name matches exactly (case-sensitive)

**Resolution:**
1. Create bucket if missing: `wrangler r2 bucket create your-bucket`
2. Fix binding name in wrangler.jsonc
3. Redeploy

---

#### "nodejs_compat required"

**Symptoms:**
- Runtime errors about missing Node.js APIs
- Error: `Buffer is not defined` or similar

**Diagnosis:**

Check compatibility_flags in wrangler.jsonc:
```jsonc
"compatibility_flags": ["nodejs_compat"]
```

**Resolution:**
1. Add `nodejs_compat` flag
2. Ensure compatibility_date is recent (2024+)
3. Redeploy

---

#### "Memory limit exceeded"

**Symptoms:**
- Workers crashing mid-request
- Error: `Memory limit exceeded` or `Worker exceeded memory limit`

**Diagnosis:**
- Check query result sizes
- Review projection usage
- Check for memory leaks in custom code

**Resolution:**

1. Use pagination:
```typescript
const results = await db.find('posts', filter, { limit: 100 })
```

2. Use projections:
```typescript
const results = await db.find('posts', filter, {
  project: { title: 1, status: 1 }
})
```

3. Avoid loading entire datasets

---

#### "CPU time limit exceeded"

**Symptoms:**
- Requests timing out
- Error: `CPU time limit exceeded`

**Diagnosis:**
- Check query complexity
- Review index usage
- Check for full table scans

**Resolution:**

1. Add indexes for frequently queried fields:
```typescript
await posts.createIndex({ field: 'status', type: 'hash' })
```

2. Use limits in queries:
```typescript
const results = await db.find('posts', filter, { limit: 50 })
```

3. Upgrade to Workers Unbound:
```jsonc
"usage_model": "unbound"
```

---

#### "Slow first request (cold start)"

**Symptoms:**
- First request after idle period is slow (3-5s)
- Subsequent requests are fast

**Diagnosis:**
- This is normal Worker cold start behavior
- Compounded by R2 cache miss

**Resolution:**

1. Enable cache warming with cron:
```jsonc
"triggers": {
  "crons": ["*/5 * * * *"]
}
```

```typescript
export default {
  async scheduled(event, env, ctx) {
    const db = new ParqueDBWorker(ctx, env)
    await db.find('posts', {}, { limit: 1 })
  }
}
```

2. Use aggressive caching configuration
3. Consider pre-warming popular queries

---

#### "Version mismatch"

**Symptoms:**
- 409 Conflict errors on update
- Error: `Version mismatch: expected X, got Y`

**Diagnosis:**
- Another request updated the entity between read and write
- This is optimistic concurrency working correctly

**Resolution:**

Implement retry logic:
```typescript
async function updateWithRetry(id, update, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const entity = await db.get('posts', id)
      return await db.update('posts', id, update, {
        expectedVersion: entity.version
      })
    } catch (e) {
      if (!e.message.includes('Version mismatch') || i === maxRetries - 1) {
        throw e
      }
      // Retry with fresh data
    }
  }
}
```

---

#### "Cache not invalidating"

**Symptoms:**
- Stale data returned after writes
- Changes not visible immediately

**Diagnosis:**
- Cache invalidation is eventual, not instant
- Edge caches propagate globally

**Resolution:**

1. For critical reads, skip cache:
```typescript
const fresh = await readPath.readParquet(path, { skipCache: true })
```

2. Read through DO for guaranteed freshness:
```typescript
const entity = await doStub.get('posts', id)
```

3. Lower cache TTLs for frequently updated data:
```jsonc
"vars": {
  "CACHE_DATA_TTL": "15"
}
```

### FAQ

**Q: How long does it take for writes to be visible in reads?**

A: Writes are immediately visible in the Durable Object. R2 propagation typically takes <1 second. Edge cache invalidation can take up to the configured TTL (default 60s). For immediate consistency, read through the DO or skip cache.

---

**Q: What's the maximum entity size?**

A: There's no hard limit, but entities larger than 1MB may impact performance. Consider storing large blobs in R2 directly and referencing them by key.

---

**Q: Can I run ParqueDB in multiple regions?**

A: Yes. R2 automatically replicates data globally. DOs are single-instance but can be accessed from any region. For data residency requirements, use jurisdiction hints when creating R2 buckets.

---

**Q: How do I migrate data between environments?**

A: Use the CLI sync commands:
```bash
# Export from staging
parquedb export --env staging --output data.parquet

# Import to production
parquedb import --env production --input data.parquet
```

---

**Q: What happens if a DO crashes mid-write?**

A: The DO uses SQLite transactions, so writes are atomic. If a crash occurs, the write either completed fully or not at all. No partial writes are possible.

---

**Q: How do I handle schema migrations?**

A: ParqueDB supports schema evolution. See [Schema Evolution](../schema-evolution.md) for details on adding fields, changing types, and migrating data.

---

## Performance Tuning

### Configuration Options

#### Cache Configuration

| Configuration | Data TTL | Metadata TTL | Use Case |
|---------------|----------|--------------|----------|
| **Default** | 60s | 300s | Balanced workloads |
| **Read-Heavy** | 300s | 900s | Analytics, dashboards |
| **Write-Heavy** | 15s | 60s | Frequently updated data |
| **No Cache** | 0 | 0 | Development, debugging |

```jsonc
// wrangler.jsonc
"vars": {
  "CACHE_DATA_TTL": "60",
  "CACHE_METADATA_TTL": "300",
  "CACHE_BLOOM_TTL": "600",
  "CACHE_STALE_WHILE_REVALIDATE": "true"
}
```

#### Query Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `limit` | 100 | Maximum results per query |
| `maxTimeMs` | 50 | Query timeout (ms) |
| `hint` | auto | Index hint |
| `explain` | false | Return query plan |

### Caching Strategies

#### 1. Read-Heavy Workloads (Analytics, Dashboards)

```typescript
// Use aggressive caching
import { READ_HEAVY_CACHE_CONFIG } from 'parquedb/worker'

// Configure in environment
// CACHE_DATA_TTL=300
// CACHE_METADATA_TTL=900
```

Benefits:
- Higher cache hit rate
- Lower R2 costs
- Faster response times

Trade-offs:
- Data may be up to 5 minutes stale

#### 2. Write-Heavy Workloads (Real-time Updates)

```typescript
// Use conservative caching
import { WRITE_HEAVY_CACHE_CONFIG } from 'parquedb/worker'

// Configure in environment
// CACHE_DATA_TTL=15
// CACHE_METADATA_TTL=60
```

Benefits:
- Fresh data within 15-60 seconds
- Suitable for frequently changing data

Trade-offs:
- More R2 reads
- Higher costs

#### 3. Stale-While-Revalidate

Enable for best of both worlds:
```jsonc
"vars": {
  "CACHE_STALE_WHILE_REVALIDATE": "true"
}
```

Benefits:
- Always fast responses (serve from cache)
- Background refresh keeps data relatively fresh
- Maximum staleness = 2x TTL

### Index Optimization

#### When to Create Indexes

| Query Pattern | Recommended Index | Impact |
|---------------|-------------------|--------|
| Equality (`status = 'published'`) | Hash | 400x faster |
| Range (`price > 100`) | SST | 125x faster |
| Full-text search | FTS | Essential for text queries |
| Vector similarity | HNSW | Required for semantic search |

#### Index Creation

```typescript
// Hash index for equality lookups
await db.createIndex('posts', { field: 'status', type: 'hash' })

// SST index for range queries
await db.createIndex('posts', { field: 'createdAt', type: 'sst' })

// Full-text search index
await db.createIndex('posts', { fields: ['title', 'content'], type: 'fts' })
```

#### Index Maintenance

- **Rebuild frequency**: Weekly for write-heavy workloads
- **Monitor size**: Indexes should be < 10% of data size
- **Review usage**: Use `explain: true` to verify index usage

```typescript
// Check query plan
const plan = await db.find('posts', { status: 'published' }, { explain: true })
// { usesIndex: true, indexName: 'status_hash', estimatedRows: 1000 }
```

#### Bloom Filters

Enable bloom filters for fast negative lookups:

```typescript
// Enabled automatically for ID fields
// Useful for existence checks before expensive queries

// Fast: Returns immediately if definitely not present
const exists = await db.exists('posts', { $id: 'posts/unknown-id' })
```

---

## Incident Response

### On-Call Procedures

#### 1. Incident Detection

Incidents may be detected via:
- Automated alerts (error rate, latency)
- User reports
- Monitoring dashboards
- Cloudflare status page

#### 2. Initial Assessment (First 5 minutes)

1. **Acknowledge alert** in your alerting system
2. **Check Cloudflare status**: https://www.cloudflarestatus.com/
3. **Review recent deployments**: `wrangler deployments list`
4. **Check logs**: `wrangler tail --status error`

#### 3. Triage Severity

| Severity | Criteria | Response Time |
|----------|----------|---------------|
| **P1 - Critical** | Complete outage, data loss risk | Immediate |
| **P2 - High** | Degraded performance, partial outage | 15 minutes |
| **P3 - Medium** | Minor impact, workaround available | 1 hour |
| **P4 - Low** | Cosmetic, non-urgent | Next business day |

#### 4. Communication

For P1/P2 incidents:
1. Create incident channel (e.g., Slack #incident-YYYY-MM-DD)
2. Notify stakeholders
3. Post regular updates (every 15 minutes for P1, 30 for P2)

#### 5. Investigation Steps

```bash
# 1. Check Worker status
wrangler deployments list

# 2. View recent logs
wrangler tail --format json | head -100

# 3. Check specific namespace
wrangler tail --search "posts" --status error

# 4. Verify R2 connectivity
wrangler r2 object list your-bucket --prefix "data/"

# 5. Check DO health via debug endpoint
curl https://your-worker.workers.dev/debug/health
```

### Escalation Paths

#### Level 1: On-Call Engineer

**Responsibilities:**
- Initial triage and diagnosis
- Apply known fixes (rollback, cache clear, restart)
- Escalate if unable to resolve in 30 minutes

**Can do:**
- Rollback deployments
- Clear caches
- Adjust configuration
- Contact Level 2

#### Level 2: Senior Engineer

**Responsibilities:**
- Deep technical investigation
- Complex debugging
- Code fixes
- Coordinate with Cloudflare support

**Can do:**
- Deploy hotfixes
- Access production data
- Contact Cloudflare support
- Contact Level 3

#### Level 3: Engineering Lead / Cloudflare Support

**Responsibilities:**
- Architecture decisions
- Major incident coordination
- Vendor escalation

**Contact:**
- Cloudflare Enterprise Support: https://dash.cloudflare.com/support
- Cloudflare Status: https://www.cloudflarestatus.com/

### Common Incident Runbooks

#### Runbook: High Error Rate

1. Check error types in logs:
   ```bash
   wrangler tail --status error --format json
   ```

2. If 5xx errors:
   - Check recent deployments
   - Verify R2/DO accessibility
   - Check for resource exhaustion

3. If 4xx errors:
   - Review client request patterns
   - Check for attack/abuse
   - Verify authentication configuration

4. Quick fixes:
   - Rollback: `wrangler rollback`
   - Clear cache: Redeploy or manual invalidation
   - Scale up: Enable Workers Unbound

#### Runbook: High Latency

1. Identify slow operations:
   ```bash
   wrangler tail --search "latency" --format json
   ```

2. Check cache hit rates:
   ```bash
   curl https://your-worker.workers.dev/debug/cache
   ```

3. Common causes:
   - Cache miss storm after deployment
   - Large query result sets
   - Missing indexes
   - R2 regional issues

4. Quick fixes:
   - Pre-warm cache
   - Add query limits
   - Create missing indexes
   - Lower cache TTLs temporarily

#### Runbook: Deployment Failure

1. Check deployment status:
   ```bash
   wrangler deployments list
   ```

2. Review deployment logs in Cloudflare dashboard

3. Common causes:
   - Build errors (TypeScript, dependencies)
   - Invalid wrangler.jsonc
   - Missing migrations
   - Size limits exceeded

4. Resolution:
   - Fix build errors locally
   - Validate configuration
   - Redeploy: `wrangler deploy`

### Post-Incident Review

#### Timeline (within 48 hours)

1. **Document timeline**: What happened, when, actions taken
2. **Identify root cause**: Why did it happen?
3. **Assess impact**: Users affected, duration, data loss
4. **Action items**: What prevents recurrence?

#### Post-Incident Report Template

```markdown
## Incident Report: [INCIDENT_ID]

**Date**: YYYY-MM-DD
**Duration**: HH:MM - HH:MM (X hours Y minutes)
**Severity**: P1/P2/P3
**Status**: Resolved

### Summary
Brief description of what happened.

### Timeline
- HH:MM - Alert triggered for [condition]
- HH:MM - On-call acknowledged
- HH:MM - Root cause identified
- HH:MM - Fix deployed
- HH:MM - Incident resolved

### Root Cause
Detailed explanation of why the incident occurred.

### Impact
- X users affected
- Y requests failed
- Z minutes of degraded service

### Resolution
What was done to resolve the incident.

### Action Items
- [ ] [Action 1] - Owner - Due Date
- [ ] [Action 2] - Owner - Due Date
- [ ] [Action 3] - Owner - Due Date

### Lessons Learned
What we learned and how we'll prevent this in the future.
```

#### Blameless Culture

- Focus on systems, not individuals
- Ask "what" and "how", not "who"
- Every incident is a learning opportunity
- Share learnings across the team

---

## Quick Reference Card

### Emergency Commands

```bash
# Rollback to previous deployment
wrangler rollback

# View error logs
wrangler tail --status error

# Check deployment status
wrangler deployments list

# Verify R2 bucket
wrangler r2 bucket list

# Check Worker health
curl https://your-worker.workers.dev/health
```

### Key URLs

| Resource | URL |
|----------|-----|
| Cloudflare Dashboard | https://dash.cloudflare.com |
| Cloudflare Status | https://www.cloudflarestatus.com |
| Workers Metrics | Dashboard > Workers > Analytics |
| R2 Metrics | Dashboard > R2 > [Bucket] > Metrics |

### Contact Information

Document your team's contact information:

| Role | Contact |
|------|---------|
| On-Call Primary | [Pager/Phone] |
| On-Call Secondary | [Pager/Phone] |
| Engineering Lead | [Email/Slack] |
| Cloudflare Support | support@cloudflare.com |

---

## Related Documentation

- [Cloudflare Workers Deployment](../deployment/cloudflare-workers.md) - Complete deployment guide
- [R2 Setup](../deployment/r2-setup.md) - R2 configuration details
- [Configuration Reference](../deployment/configuration.md) - All configuration options
- [Consistency Model](../architecture/consistency.md) - Understanding consistency guarantees
- [Performance Benchmarks](../benchmarks.md) - Performance targets and optimization

