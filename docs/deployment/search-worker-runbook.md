---
title: Search Worker Deployment Runbook
description: Step-by-step guide for deploying and operating the ParqueDB Search Worker to Cloudflare Workers with R2 storage.
---

This runbook provides step-by-step procedures for deploying, maintaining, and troubleshooting the ParqueDB Search Worker (`parquedb-search`), which provides search endpoints for O*NET, UNSPSC, and IMDB datasets.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [R2 Bucket Creation](#r2-bucket-creation)
- [Data Upload](#data-upload)
- [Worker Deployment](#worker-deployment)
- [Health Verification](#health-verification)
- [Rollback Procedure](#rollback-procedure)
- [Monitoring and Alerts](#monitoring-and-alerts)
- [Troubleshooting](#troubleshooting)

---

## Overview

### Architecture

```
                    +-----------------------------------------------+
                    |            Cloudflare Network                 |
                    |                                               |
    Request ------->|  +------------------+    +----------------+   |
    /search/onet    |  |  parquedb-search |    |  R2 Bucket     |   |
    /search/unspsc  |  |  Worker          |--->|  (DATA)        |   |
    /search/imdb    |  |                  |    |                |   |
    /search/health  |  +------------------+    +----------------+   |
                    |          |                                    |
                    |          v                                    |
                    |  +------------------+                         |
                    |  |  parquedb-tail   |   (Tail Consumer)       |
                    |  |  Worker          |   (CPU time metrics)    |
                    |  +------------------+                         |
                    +-----------------------------------------------+
```

### Components

| Component | Purpose | Location |
|-----------|---------|----------|
| `parquedb-search` | Search API Worker | `snippets/worker/search.ts` |
| `parquedb-tail` | Tail consumer for metrics | `examples/tail-worker/` |
| `parquedb-search-data` | R2 bucket for JSON data | Cloudflare R2 |

### Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /search/onet?q=engineer` | Search O*NET occupations |
| `GET /search/unspsc?q=computer` | Search UNSPSC categories |
| `GET /search/imdb?q=matrix` | Search IMDB titles |
| `GET /search/health` | Health check endpoint |

---

## Prerequisites

### 1. Cloudflare Account

- [ ] Cloudflare account with Workers enabled
- [ ] Workers Paid plan recommended ($5/month) for higher limits
- [ ] Account ID available (Dashboard > Overview > Account ID)

### 2. Wrangler CLI

```bash
# Install Wrangler globally
npm install -g wrangler

# Verify installation
wrangler --version

# Authenticate with Cloudflare
wrangler login

# Verify authentication
wrangler whoami
```

### 3. Required Credentials

| Credential | How to Obtain | Usage |
|------------|---------------|-------|
| `CLOUDFLARE_ACCOUNT_ID` | Dashboard > Overview | R2 bucket creation, deployment |
| `CLOUDFLARE_API_TOKEN` | Dashboard > API Tokens | CI/CD deployment |

### 4. Data Files

Ensure you have the following JSON data files ready:

| File | Description | Approximate Size |
|------|-------------|------------------|
| `onet-occupations.json` | O*NET occupation data | ~2MB |
| `unspsc.json` | UNSPSC category data | ~5MB |
| `imdb-titles.json` | IMDB titles data | ~50MB |

---

## Environment Setup

### 1. Clone Repository

```bash
git clone https://github.com/parquedb/parquedb.git
cd parquedb
```

### 2. Install Dependencies

```bash
npm install
# or
pnpm install
```

### 3. Set Environment Variables

```bash
# Set in your shell or .env file
export CLOUDFLARE_ACCOUNT_ID="your-account-id"

# For CI/CD, also set:
export CLOUDFLARE_API_TOKEN="your-api-token"
```

### 4. Verify Configuration

Review the worker configuration at `snippets/worker/wrangler.toml`:

```toml
name = "parquedb-search"
main = "search.ts"
compatibility_date = "2024-09-02"
compatibility_flags = ["nodejs_compat"]

[[r2_buckets]]
binding = "DATA"
bucket_name = "parquedb-search-data"

[[tail_consumers]]
service = "parquedb-tail"

[observability]
enabled = true

[observability.logs]
enabled = true
invocation_logs = true

[vars]
CDN_BASE = "https://cdn.workers.do/parquedb-benchmarks/snippets"
```

---

## R2 Bucket Creation

### Step 1: Create the Data Bucket

```bash
# Create the R2 bucket for search data
wrangler r2 bucket create parquedb-search-data

# Expected output:
# Creating bucket parquedb-search-data with default storage class set to Standard.
# Created bucket parquedb-search-data with default storage class set to Standard.
```

### Step 2: Verify Bucket Creation

```bash
# List all buckets to confirm
wrangler r2 bucket list

# Expected output should include:
# parquedb-search-data
```

### Step 3: (Optional) Create Preview Bucket

For local development with `wrangler dev`:

```bash
wrangler r2 bucket create parquedb-search-data-preview
```

Update `wrangler.toml` if using preview bucket:

```toml
[[r2_buckets]]
binding = "DATA"
bucket_name = "parquedb-search-data"
preview_bucket_name = "parquedb-search-data-preview"
```

### Rollback: Delete Bucket

If you need to start fresh:

```bash
# WARNING: This deletes ALL data in the bucket
wrangler r2 bucket delete parquedb-search-data
```

---

## Data Upload

### Step 1: Prepare Data Files

Ensure data files are in JSON format:

```bash
# Check file sizes and validity
ls -la snippets/data/
# or wherever your data files are located

# Validate JSON format
cat onet-occupations.json | jq . > /dev/null && echo "Valid JSON"
```

### Step 2: Upload Data Files

```bash
# Upload O*NET data
wrangler r2 object put parquedb-search-data/onet-occupations.json \
  --file ./data/onet-occupations.json \
  --content-type application/json

# Upload UNSPSC data
wrangler r2 object put parquedb-search-data/unspsc.json \
  --file ./data/unspsc.json \
  --content-type application/json

# Upload IMDB data
wrangler r2 object put parquedb-search-data/imdb-titles.json \
  --file ./data/imdb-titles.json \
  --content-type application/json
```

### Step 3: Verify Upload

```bash
# List objects in bucket
wrangler r2 object list parquedb-search-data

# Expected output:
# onet-occupations.json  (size, date)
# unspsc.json            (size, date)
# imdb-titles.json       (size, date)

# Check specific object metadata
wrangler r2 object head parquedb-search-data/onet-occupations.json
```

### Step 4: (Optional) Upload to Preview Bucket

For local development:

```bash
wrangler r2 object put parquedb-search-data-preview/onet-occupations.json \
  --file ./data/onet-occupations.json \
  --content-type application/json
# Repeat for other files...
```

### Data Update Procedure

When updating data files:

1. Upload new file with timestamp suffix:
   ```bash
   wrangler r2 object put parquedb-search-data/onet-occupations-20260203.json \
     --file ./data/onet-occupations.json
   ```

2. Test with the new file (update wrangler.toml temporarily)

3. Replace the original:
   ```bash
   wrangler r2 object put parquedb-search-data/onet-occupations.json \
     --file ./data/onet-occupations.json
   ```

4. Delete the timestamped backup after verification:
   ```bash
   wrangler r2 object delete parquedb-search-data/onet-occupations-20260203.json
   ```

---

## Worker Deployment

### Step 1: Local Development Test

```bash
# Start local development server
wrangler dev --config snippets/worker/wrangler.toml

# Test endpoints locally
curl http://localhost:8787/search/health
curl "http://localhost:8787/search/onet?q=engineer"
curl "http://localhost:8787/search/unspsc?q=computer"
curl "http://localhost:8787/search/imdb?q=matrix"
```

### Step 2: Deploy to Production

```bash
# Deploy the worker
wrangler deploy --config snippets/worker/wrangler.toml

# Expected output:
# Total Upload: XX KiB / gzip: XX KiB
# Uploaded parquedb-search (X.XX sec)
# Published parquedb-search (X.XX sec)
#   https://parquedb-search.{account}.workers.dev
```

### Step 3: Verify Deployment

```bash
# List recent deployments
wrangler deployments list --config snippets/worker/wrangler.toml

# Check deployment details
wrangler deployments view --config snippets/worker/wrangler.toml
```

### Step 4: Record Deployment

Document the deployment:

| Field | Value |
|-------|-------|
| Timestamp | `date -u +"%Y-%m-%dT%H:%M:%SZ"` |
| Version | Deployment ID from wrangler |
| Deployer | Your name |
| Changes | Brief description |

### CI/CD Deployment (GitHub Actions)

For automated deployments, add this workflow:

```yaml
# .github/workflows/deploy-search-worker.yml
name: Deploy Search Worker

on:
  push:
    branches: [main]
    paths:
      - 'snippets/worker/**'
      - '.github/workflows/deploy-search-worker.yml'
  workflow_dispatch:

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Install dependencies
        run: npm ci

      - name: Deploy Search Worker
        uses: cloudflare/wrangler-action@v3
        with:
          apiToken: ${{ secrets.CLOUDFLARE_API_TOKEN }}
          accountId: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}
          workingDirectory: snippets/worker
          command: deploy
```

---

## Health Verification

### Step 1: Health Check Endpoint

```bash
# Check worker health
curl -s https://parquedb-search.{account}.workers.dev/search/health | jq .

# Expected response:
# {
#   "status": "ok",
#   "datasets": ["onet", "unspsc", "imdb"],
#   "cached": [],
#   "r2Status": "ok (size: XXXX)",
#   "hasDataBinding": true
# }
```

### Step 2: Functional Verification

Test each search endpoint:

```bash
# O*NET search
curl -s "https://parquedb-search.{account}.workers.dev/search/onet?q=engineer&limit=5" | jq '.data | length'
# Expected: 5 (or fewer if less results)

# UNSPSC search
curl -s "https://parquedb-search.{account}.workers.dev/search/unspsc?q=computer&limit=5" | jq '.data | length'
# Expected: 5 (or fewer if less results)

# IMDB search
curl -s "https://parquedb-search.{account}.workers.dev/search/imdb?q=matrix&limit=5" | jq '.data | length'
# Expected: 5 (or fewer if less results)
```

### Step 3: Performance Verification

```bash
# Check response times
curl -s -w "\nTotal time: %{time_total}s\n" \
  "https://parquedb-search.{account}.workers.dev/search/onet?q=engineer" \
  -o /dev/null

# Target: < 500ms for first request, < 100ms for cached
```

### Step 4: Verify Tail Worker Connection

```bash
# Check logs for tail worker events
wrangler tail parquedb-search --config snippets/worker/wrangler.toml
```

### Health Check Script

Save this as `scripts/verify-search-worker.sh`:

```bash
#!/bin/bash
set -e

WORKER_URL="${1:-https://parquedb-search.{account}.workers.dev}"

echo "Verifying search worker at: $WORKER_URL"

# Health check
echo -n "Health check: "
HEALTH=$(curl -s "$WORKER_URL/search/health")
if echo "$HEALTH" | jq -e '.status == "ok"' > /dev/null; then
  echo "PASS"
else
  echo "FAIL"
  echo "$HEALTH"
  exit 1
fi

# R2 status
echo -n "R2 connection: "
if echo "$HEALTH" | jq -e '.r2Status | startswith("ok")' > /dev/null; then
  echo "PASS"
else
  echo "FAIL - R2 status: $(echo $HEALTH | jq -r '.r2Status')"
  exit 1
fi

# O*NET search
echo -n "O*NET search: "
ONET=$(curl -s "$WORKER_URL/search/onet?q=engineer&limit=1")
if echo "$ONET" | jq -e '.data | length > 0' > /dev/null; then
  echo "PASS ($(echo $ONET | jq '.pagination.total') results)"
else
  echo "FAIL"
  exit 1
fi

# UNSPSC search
echo -n "UNSPSC search: "
UNSPSC=$(curl -s "$WORKER_URL/search/unspsc?q=computer&limit=1")
if echo "$UNSPSC" | jq -e '.data | length > 0' > /dev/null; then
  echo "PASS ($(echo $UNSPSC | jq '.pagination.total') results)"
else
  echo "FAIL"
  exit 1
fi

# IMDB search
echo -n "IMDB search: "
IMDB=$(curl -s "$WORKER_URL/search/imdb?q=matrix&limit=1")
if echo "$IMDB" | jq -e '.data | length > 0' > /dev/null; then
  echo "PASS ($(echo $IMDB | jq '.pagination.total') results)"
else
  echo "FAIL"
  exit 1
fi

echo ""
echo "All checks passed!"
```

---

## Rollback Procedure

### Immediate Rollback

If a deployment causes issues, rollback immediately:

```bash
# Rollback to previous version
wrangler rollback --config snippets/worker/wrangler.toml

# Confirm rollback
wrangler deployments list --config snippets/worker/wrangler.toml
```

### Rollback to Specific Version

```bash
# List available versions
wrangler deployments list --config snippets/worker/wrangler.toml

# Note the deployment ID of the target version
# Then redeploy from that commit:
git checkout <commit-hash>
wrangler deploy --config snippets/worker/wrangler.toml
```

### Data Rollback

If data needs to be rolled back:

1. **Identify the issue:**
   ```bash
   # Check current data
   wrangler r2 object head parquedb-search-data/onet-occupations.json
   ```

2. **Restore from backup:**
   ```bash
   # If you have a backup
   wrangler r2 object put parquedb-search-data/onet-occupations.json \
     --file ./backup/onet-occupations.json
   ```

3. **Clear worker cache** (restart worker by redeploying):
   ```bash
   wrangler deploy --config snippets/worker/wrangler.toml
   ```

### Rollback Checklist

- [ ] Identify the issue (logs, errors, user reports)
- [ ] Execute rollback command
- [ ] Verify health check passes
- [ ] Verify functional tests pass
- [ ] Notify stakeholders
- [ ] Document incident

---

## Monitoring and Alerts

### Real-Time Logs

```bash
# Stream all logs
wrangler tail parquedb-search --config snippets/worker/wrangler.toml

# Filter by errors only
wrangler tail parquedb-search --config snippets/worker/wrangler.toml --status error

# Filter by specific search term
wrangler tail parquedb-search --config snippets/worker/wrangler.toml --search "onet"

# JSON format for parsing
wrangler tail parquedb-search --config snippets/worker/wrangler.toml --format json
```

### Key Metrics to Monitor

| Metric | Normal Range | Alert Threshold |
|--------|--------------|-----------------|
| Request latency (p50) | < 50ms | > 200ms |
| Request latency (p99) | < 200ms | > 1s |
| Error rate | < 0.1% | > 1% |
| R2 read operations | Varies | 2x normal spike |
| Cache hit rate | > 80% | < 50% |

### Cloudflare Dashboard

Monitor via Cloudflare Dashboard:

1. **Workers Analytics**: Dashboard > Workers & Pages > parquedb-search > Analytics
2. **R2 Metrics**: Dashboard > R2 > parquedb-search-data > Metrics
3. **Real-time Logs**: Dashboard > Workers & Pages > parquedb-search > Logs

### Alerting Setup

Consider setting up alerts for:

- **High error rate**: > 1% errors over 5 minutes
- **High latency**: p99 > 1s over 5 minutes
- **R2 unavailable**: Health check shows R2 status not "ok"

---

## Troubleshooting

### Common Issues

#### Issue: "R2 object missing" Error

**Symptoms:**
- Health check shows `r2Status: "object not found"`
- Search endpoints return 500 errors

**Diagnosis:**
```bash
# Check if objects exist
wrangler r2 object list parquedb-search-data
```

**Resolution:**
1. Verify bucket name in wrangler.toml matches actual bucket
2. Re-upload missing data files (see [Data Upload](#data-upload))

---

#### Issue: Worker Not Responding

**Symptoms:**
- Requests timeout or return 502/503

**Diagnosis:**
```bash
# Check deployment status
wrangler deployments list --config snippets/worker/wrangler.toml

# Check logs for errors
wrangler tail parquedb-search --config snippets/worker/wrangler.toml --status error
```

**Resolution:**
1. Verify worker is deployed
2. Check for syntax errors in code
3. Rollback to previous working version

---

#### Issue: Slow First Request

**Symptoms:**
- First request takes 2-5 seconds
- Subsequent requests are fast

**Diagnosis:**
This is normal cold start behavior combined with R2 data fetch.

**Resolution:**
1. This is expected - data is cached after first request
2. For critical applications, implement cache warming:
   ```bash
   # Warm cache with a cron job
   curl "https://parquedb-search.{account}.workers.dev/search/onet?q=test"
   curl "https://parquedb-search.{account}.workers.dev/search/unspsc?q=test"
   curl "https://parquedb-search.{account}.workers.dev/search/imdb?q=test"
   ```

---

#### Issue: "nodejs_compat" Error

**Symptoms:**
- Runtime errors about missing Node.js APIs
- Error: `Buffer is not defined`

**Diagnosis:**
Check wrangler.toml for compatibility flag.

**Resolution:**
Ensure wrangler.toml includes:
```toml
compatibility_flags = ["nodejs_compat"]
```

---

#### Issue: Data Binding Not Found

**Symptoms:**
- Health check shows `hasDataBinding: false`
- Error: `env.DATA is undefined`

**Diagnosis:**
```bash
# Verify R2 binding in wrangler.toml
cat snippets/worker/wrangler.toml | grep -A2 "r2_buckets"
```

**Resolution:**
1. Verify R2 bucket binding in wrangler.toml:
   ```toml
   [[r2_buckets]]
   binding = "DATA"
   bucket_name = "parquedb-search-data"
   ```
2. Verify bucket exists: `wrangler r2 bucket list`
3. Redeploy worker

---

### Emergency Contacts

| Role | Contact |
|------|---------|
| On-Call Engineer | [Your contact info] |
| Engineering Lead | [Your contact info] |
| Cloudflare Support | https://dash.cloudflare.com/support |

### Useful Links

| Resource | URL |
|----------|-----|
| Cloudflare Dashboard | https://dash.cloudflare.com |
| Cloudflare Status | https://www.cloudflarestatus.com |
| Worker Analytics | Dashboard > Workers > parquedb-search > Analytics |
| R2 Metrics | Dashboard > R2 > parquedb-search-data > Metrics |

---

## Appendix

### Quick Reference Commands

```bash
# Deploy
wrangler deploy --config snippets/worker/wrangler.toml

# Rollback
wrangler rollback --config snippets/worker/wrangler.toml

# View logs
wrangler tail parquedb-search --config snippets/worker/wrangler.toml

# Check health
curl https://parquedb-search.{account}.workers.dev/search/health

# List deployments
wrangler deployments list --config snippets/worker/wrangler.toml

# List R2 objects
wrangler r2 object list parquedb-search-data

# Upload data
wrangler r2 object put parquedb-search-data/FILE.json --file ./FILE.json
```

### Configuration Reference

| Setting | Value | Description |
|---------|-------|-------------|
| Worker Name | `parquedb-search` | Worker identifier |
| R2 Bucket | `parquedb-search-data` | Data storage bucket |
| R2 Binding | `DATA` | R2 binding name in code |
| Tail Consumer | `parquedb-tail` | CPU metrics worker |
| Compatibility | `nodejs_compat` | Node.js API support |

---

## Related Documentation

- [Cloudflare Workers Deployment](./cloudflare-workers.md) - Complete Workers deployment guide
- [R2 Setup](./r2-setup.md) - Detailed R2 configuration
- [Production Runbook](../guides/production-runbook.md) - General production operations
- [Backup and Restore](./backup-restore.md) - Data backup procedures
