# Production Readiness Checklist

This document provides a comprehensive checklist for deploying ParqueDB in production environments. It covers error handling, logging, monitoring, and operational concerns.

## Quick Status

ParqueDB provides built-in production-ready infrastructure:

| Concern | Status | Location |
|---------|--------|----------|
| Error Handling | Complete | `src/errors/index.ts` |
| Logging | Complete | `src/utils/logger.ts` |
| Metrics Collection | Complete | `src/observability/hooks.ts` |
| Circuit Breakers | Complete | `src/storage/CircuitBreaker.ts` |
| Worker Logs | Complete | `src/streaming/worker-logs.ts` |
| AI Observability | Complete | `src/observability/ai/` |
| Production Runbook | Complete | `docs/guides/production-runbook.md` |

---

## 1. Error Handling

### Error Hierarchy

ParqueDB uses a standardized error hierarchy with codes for programmatic handling:

```typescript
import {
  ParqueDBError,
  ValidationError,
  NotFoundError,
  ConflictError,
  StorageError,
  AuthorizationError,
  QueryError,
  TimeoutError,
  ErrorCode,
} from 'parquedb/errors'
```

### Error Codes Reference

| Category | Codes | Use Case |
|----------|-------|----------|
| General (1xxx) | `UNKNOWN`, `INTERNAL`, `TIMEOUT`, `CANCELLED` | Unexpected failures |
| Validation (2xxx) | `VALIDATION_FAILED`, `INVALID_INPUT`, `INVALID_TYPE`, `REQUIRED_FIELD` | Input validation |
| Not Found (3xxx) | `ENTITY_NOT_FOUND`, `COLLECTION_NOT_FOUND`, `INDEX_NOT_FOUND` | Missing resources |
| Conflict (4xxx) | `VERSION_CONFLICT`, `ALREADY_EXISTS`, `ETAG_MISMATCH`, `UNIQUE_CONSTRAINT` | Concurrency issues |
| Relationship (5xxx) | `RELATIONSHIP_ERROR`, `INVALID_RELATIONSHIP`, `CIRCULAR_RELATIONSHIP` | Graph operations |
| Query (6xxx) | `QUERY_ERROR`, `INVALID_FILTER`, `QUERY_TIMEOUT` | Query execution |
| Storage (7xxx) | `STORAGE_ERROR`, `STORAGE_READ_ERROR`, `QUOTA_EXCEEDED`, `PATH_TRAVERSAL` | Storage backend |
| Authorization (8xxx) | `PERMISSION_DENIED`, `AUTHENTICATION_REQUIRED`, `TOKEN_EXPIRED` | Access control |
| Configuration (9xxx) | `CONFIGURATION_ERROR`, `INVALID_CONFIG`, `MISSING_CONFIG` | Setup issues |
| RPC (10xxx) | `RPC_ERROR`, `RPC_TIMEOUT`, `RPC_UNAVAILABLE` | Remote calls |
| Index (11xxx) | `INDEX_ERROR`, `INDEX_BUILD_ERROR`, `INDEX_LOAD_ERROR` | Index operations |

### Type Guards

Use type guards for safe error handling:

```typescript
import {
  isParqueDBError,
  isNotFoundError,
  isConflictError,
  isVersionConflictError,
  isStorageError,
  isAuthorizationError,
} from 'parquedb/errors'

try {
  await db.update('posts', id, data)
} catch (error) {
  if (isVersionConflictError(error)) {
    // Retry with fresh data
  } else if (isNotFoundError(error)) {
    // Create instead
  } else if (isAuthorizationError(error)) {
    // Return 401/403
  } else {
    throw error
  }
}
```

### Error Serialization (RPC)

Errors serialize cleanly for RPC:

```typescript
try {
  await db.create('posts', data)
} catch (error) {
  if (isParqueDBError(error)) {
    return Response.json(error.toJSON(), { status: 400 })
  }
}

// On client:
const data = await response.json()
const error = ParqueDBError.fromJSON(data)
```

### Checklist: Error Handling

- [ ] Use specific error types (e.g., `ValidationError`, not generic `Error`)
- [ ] Include context in errors (namespace, entityId, operation)
- [ ] Use type guards for error handling branches
- [ ] Chain errors with `cause` for debugging
- [ ] Handle version conflicts with retry logic
- [ ] Return appropriate HTTP status codes

---

## 2. Logging

### Logger Interface

ParqueDB provides a pluggable logging interface:

```typescript
import { Logger, setLogger, consoleLogger, noopLogger } from 'parquedb/utils'

// Enable console logging (development)
setLogger(consoleLogger)

// Disable logging (production, or use custom)
setLogger(noopLogger)

// Custom logger
setLogger({
  debug: (msg, ...args) => myLogger.debug(msg, args),
  info: (msg, ...args) => myLogger.info(msg, args),
  warn: (msg, ...args) => myLogger.warn(msg, args),
  error: (msg, err, ...args) => myLogger.error(msg, err, args),
})
```

### Structured Logging for Workers

For Cloudflare Workers, use structured JSON logging for `wrangler tail`:

```typescript
// Recommended: structured JSON logs
console.log(JSON.stringify({
  type: 'metric',
  operation: 'find',
  namespace: 'posts',
  latencyMs: 45,
  rowsReturned: 20,
  cacheHit: true,
  timestamp: new Date().toISOString()
}))

// View logs
// wrangler tail --format json
```

### Worker Logs Materialized View

For persistent log storage, use `WorkerLogsMV`:

```typescript
import { createWorkerLogsMV } from 'parquedb/streaming'

const workerLogs = createWorkerLogsMV({
  storage: myStorage,
  datasetPath: 'logs/workers',
  flushThreshold: 1000,
  flushIntervalMs: 30000,
})

// In your Tail Worker
export default {
  async tail(event, env) {
    await workerLogs.ingestTailEvent(event)
  }
}
```

### Checklist: Logging

- [ ] Configure appropriate log level per environment
- [ ] Use structured JSON logging in production
- [ ] Set up Tail Worker for persistent log collection
- [ ] Implement log retention policies
- [ ] Avoid logging sensitive data (auth tokens, PII)
- [ ] Include request IDs for correlation

---

## 3. Monitoring & Metrics

### Observability Hooks

Register hooks to track all database operations:

```typescript
import {
  HookRegistry,
  MetricsCollector,
  globalHookRegistry,
  createQueryContext,
  createMutationContext,
} from 'parquedb/observability'

// Built-in metrics collector
const metrics = new MetricsCollector()
globalHookRegistry.registerHook(metrics)

// Get aggregated metrics
const stats = metrics.getMetrics()
console.log('Total operations:', stats.system.totalOperations)
console.log('Error rate:', stats.system.totalErrors / stats.system.totalOperations)
```

### Custom Hooks

Create custom hooks for your monitoring system:

```typescript
globalHookRegistry.registerHook({
  onQueryStart(context) {
    // Track query start in your APM
  },
  onQueryEnd(context, result) {
    sendMetric('query.latency', result.durationMs, {
      namespace: context.namespace,
      operation: context.operationType,
    })
  },
  onQueryError(context, error) {
    sendMetric('query.error', 1, {
      namespace: context.namespace,
      errorCode: error.code,
    })
  },
})
```

### Key Metrics to Track

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| Request Rate | Requests per second | Varies by load |
| Error Rate | % of 4xx/5xx responses | > 1% warning, > 5% critical |
| Latency p50 | Median response time | > 50ms |
| Latency p99 | 99th percentile | > 200ms warning, > 2s critical |
| Cache Hit Rate | % of cache hits | < 60% warning |
| DO Cold Starts | Cold start frequency | High frequency |

### Circuit Breaker Monitoring

Monitor circuit breaker health for resilience:

```typescript
import {
  CircuitBreakerMonitor,
  globalCircuitBreakerMonitor,
} from 'parquedb/storage'

// Register backends
globalCircuitBreakerMonitor.register('r2', r2Backend)

// Check health
const health = globalCircuitBreakerMonitor.getHealthStatus()
console.log('Healthy:', health.healthy)
console.log('Open circuits:', health.openCount)

// Subscribe to state changes
globalCircuitBreakerMonitor.onStateChange((event) => {
  if (event.toState === 'OPEN') {
    sendAlert(`Circuit ${event.name} opened: ${event.reason}`)
  }
})
```

### AI Usage Tracking

Track AI API costs and usage:

```typescript
import { createAIUsageMV } from 'parquedb/observability/ai'

const usageMV = createAIUsageMV(db, {
  granularity: 'day',
})

await usageMV.refresh()

const summary = await usageMV.getSummary({
  from: new Date('2026-02-01'),
  to: new Date('2026-02-03'),
})

console.log(`Total cost: $${summary.estimatedTotalCost.toFixed(2)}`)
```

### Checklist: Monitoring

- [ ] Enable observability hooks in production
- [ ] Configure alerting thresholds
- [ ] Set up dashboard for key metrics
- [ ] Monitor circuit breaker states
- [ ] Track storage operations (R2 costs)
- [ ] Set up log aggregation and analysis

---

## 4. Resilience

### Circuit Breakers

ParqueDB includes circuit breakers for storage backends:

```typescript
import {
  CircuitBreaker,
  createStorageCircuitBreaker,
  createFastFailCircuitBreaker,
} from 'parquedb/storage'

// Standard storage circuit breaker
const breaker = createStorageCircuitBreaker('r2-primary', (from, to, name) => {
  console.log(`Circuit ${name}: ${from} -> ${to}`)
})

// Execute with protection
const data = await breaker.execute(() => r2.get(key))
```

### Circuit Breaker States

| State | Behavior |
|-------|----------|
| CLOSED | Normal operation, requests pass through |
| OPEN | Failure threshold exceeded, requests fail fast |
| HALF_OPEN | Testing recovery, limited requests allowed |

### Retry Strategies

Implement retry logic for transient failures:

```typescript
async function withRetry<T>(
  operation: () => Promise<T>,
  maxRetries = 3,
  delayMs = 1000
): Promise<T> {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await operation()
    } catch (error) {
      if (i === maxRetries - 1) throw error
      if (!isRetryable(error)) throw error
      await sleep(delayMs * Math.pow(2, i)) // Exponential backoff
    }
  }
  throw new Error('Unreachable')
}

function isRetryable(error: unknown): boolean {
  if (isParqueDBError(error)) {
    return error.code === ErrorCode.TIMEOUT ||
           error.code === ErrorCode.RPC_UNAVAILABLE ||
           error.code === ErrorCode.NETWORK_ERROR
  }
  return false
}
```

### Checklist: Resilience

- [ ] Enable circuit breakers for external calls
- [ ] Implement retry with exponential backoff
- [ ] Handle version conflicts gracefully
- [ ] Set appropriate timeouts
- [ ] Plan for R2/DO unavailability
- [ ] Test failure scenarios

---

## 5. Security

### Input Validation

ParqueDB validates inputs automatically, but add defense in depth:

```typescript
import { assertValid, ValidationError } from 'parquedb/errors'

// Use assertions
assertValid(data.email.includes('@'), 'Invalid email format', { field: 'email' })

// Schema validation is enforced
const entity = await db.create('users', {
  email: 'user@example.com', // Will be validated against schema
})
```

### Path Traversal Protection

Storage backends protect against path traversal:

```typescript
// This will throw PathTraversalError
await storage.read('../../../etc/passwd')
// Error: Path traversal attempt detected
```

### Authentication

Implement authentication for your Workers:

```typescript
import { AuthorizationError, PermissionDeniedError } from 'parquedb/errors'

export default {
  async fetch(request, env) {
    const token = request.headers.get('Authorization')
    if (!token) {
      throw new AuthorizationError(
        'Authentication required',
        ErrorCode.AUTHENTICATION_REQUIRED
      )
    }

    const user = await validateToken(token)
    if (!user.canAccess('posts')) {
      throw new PermissionDeniedError('posts', 'read')
    }

    // Continue...
  }
}
```

### Checklist: Security

- [ ] Validate all user inputs
- [ ] Implement authentication
- [ ] Use parameterized queries (no SQL injection)
- [ ] Protect against path traversal (built-in)
- [ ] Secure secrets with Wrangler secrets
- [ ] Enable CORS appropriately
- [ ] Rate limit API endpoints

---

## 6. Deployment Checklist

### Pre-Deployment

- [ ] All tests passing (`pnpm test`)
- [ ] TypeScript compiles without errors
- [ ] Environment variables configured
- [ ] Secrets set via `wrangler secret`
- [ ] R2 buckets created
- [ ] DO migrations configured
- [ ] `nodejs_compat` flag enabled

### Configuration

```jsonc
// wrangler.jsonc
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
    { "binding": "BUCKET", "bucket_name": "your-bucket" }
  ],

  "vars": {
    "ENVIRONMENT": "production",
    "LOG_LEVEL": "warn",
    "CACHE_DATA_TTL": "60",
    "CACHE_METADATA_TTL": "300"
  }
}
```

### Post-Deployment Verification

```bash
# Verify deployment
wrangler deployments list

# Check health
curl https://your-worker.workers.dev/health

# View logs
wrangler tail --status error

# Verify R2
wrangler r2 bucket list
```

### Checklist: Deployment

- [ ] Deploy to staging first
- [ ] Verify health endpoint
- [ ] Run smoke tests
- [ ] Monitor initial traffic
- [ ] Have rollback ready (`wrangler rollback`)
- [ ] Document deployment in changelog

---

## 7. Operational Runbook

See [Production Runbook](./guides/production-runbook.md) for:

- Deployment checklist
- Monitoring setup
- Common issues and resolutions
- Performance tuning
- Incident response procedures

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

---

## Summary

ParqueDB provides comprehensive production-ready infrastructure:

1. **Error Handling**: Typed error hierarchy with codes, serialization, and type guards
2. **Logging**: Pluggable logger interface with structured logging support
3. **Monitoring**: Observability hooks with built-in metrics collection
4. **Resilience**: Circuit breakers with configurable thresholds
5. **Security**: Input validation, path traversal protection, auth patterns
6. **Deployment**: Complete configuration and verification guides
7. **Operations**: Comprehensive runbook with incident response procedures

All components are designed to work together in Cloudflare Workers environments while remaining testable in Node.js.
