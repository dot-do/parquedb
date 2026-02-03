# Compaction Local Development Guide

This guide provides instructions for testing and developing the ParqueDB compaction system locally.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Local Testing with Vitest](#local-testing-with-vitest)
- [Local Development with Miniflare](#local-development-with-miniflare)
- [Integration Testing](#integration-testing)
- [Debugging Tips](#debugging-tips)
- [Example Workflow](#example-workflow)

---

## Prerequisites

### Node.js Version

ParqueDB requires Node.js 18.0.0 or later:

```bash
node --version  # Should be >= 18.0.0
```

If you need to update Node.js, we recommend using a version manager like `nvm`:

```bash
nvm install 18
nvm use 18
```

### pnpm Setup

Install pnpm if you have not already:

```bash
npm install -g pnpm
```

Install project dependencies:

```bash
pnpm install
```

### Wrangler Installation

Wrangler is included as a dev dependency, but you can also install it globally:

```bash
# Use the project's local version (recommended)
pnpm exec wrangler --version

# Or install globally
npm install -g wrangler
```

---

## Local Testing with Vitest

### Running Compaction Unit Tests

The compaction system has comprehensive unit tests that run without any external dependencies.

**Run all compaction-related tests:**

```bash
# Run all unit tests matching "compaction"
pnpm test -- --filter=compaction

# Run specific test files
pnpm test tests/unit/workflows/compaction-state-do.test.ts
pnpm test tests/unit/workflows/compaction-workflow.test.ts
pnpm test tests/unit/events/compaction.test.ts
```

**Available compaction test files:**

| Test File | Description |
|-----------|-------------|
| `tests/unit/workflows/compaction-state-do.test.ts` | CompactionStateDO state machine tests |
| `tests/unit/workflows/compaction-workflow.test.ts` | Workflow orchestration tests |
| `tests/unit/workflows/compaction-validation.test.ts` | Input validation tests |
| `tests/unit/workflows/compaction-state-machine.test.ts` | State transition tests |
| `tests/unit/events/compaction.test.ts` | Event compaction logic tests |
| `tests/unit/worker/compaction-consumer.test.ts` | Queue consumer tests |
| `tests/unit/worker/compaction-health.test.ts` | Health check endpoint tests |
| `tests/e2e/compaction-workflow.test.ts` | End-to-end workflow tests |
| `tests/e2e/compaction-data-integrity.test.ts` | Data integrity verification |

### Using the TestableCompactionStateDO Helper

The `TestableCompactionStateDO` is a test-friendly implementation of the production `CompactionStateDO` that can be used without deploying to Workers:

```typescript
import { describe, it, expect, beforeEach } from 'vitest'
import {
  TestableCompactionStateDO,
  MockDurableObjectState,
  createUpdateRequest,
  createUpdate,
} from './__helpers__/testable-compaction-state-do'

describe('My Compaction Tests', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  it('should track file updates', async () => {
    const request = new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(createUpdateRequest({
        updates: [
          createUpdate({ writerId: 'writer1', file: 'file1.parquet' }),
          createUpdate({ writerId: 'writer2', file: 'file2.parquet' }),
        ],
      })),
    })

    const response = await compactionDO.fetch(request)
    expect(response.status).toBe(200)

    // Use test helpers to inspect state
    expect(compactionDO.getKnownWriters()).toContain('writer1')
    expect(compactionDO.getKnownWriters()).toContain('writer2')
    expect(compactionDO.getWindowCount()).toBe(1)
  })
})
```

**Helper functions available:**

- `createUpdateRequest(overrides)` - Creates a properly structured update request
- `createUpdate(overrides)` - Creates a single file update with defaults
- `MockDurableObjectState` - Mock DO state with in-memory storage
- `MockDurableObjectStorage` - Mock storage backend

### Mocking R2 and Queues

For tests that interact with R2 or Queues, use the mock implementations:

```typescript
import { vi } from 'vitest'

// Mock R2 Bucket
class MockR2Bucket {
  private files: Map<string, { data: Uint8Array; size: number }> = new Map()

  async get(key: string) {
    const file = this.files.get(key)
    if (!file) return null
    return {
      key,
      size: file.size,
      async arrayBuffer() {
        return file.data.buffer
      },
      async text() {
        return new TextDecoder().decode(file.data)
      },
    }
  }

  async put(key: string, data: Uint8Array | string) {
    const uint8 = typeof data === 'string'
      ? new TextEncoder().encode(data)
      : data
    this.files.set(key, { data: uint8, size: uint8.length })
    return { key, size: uint8.length }
  }

  async delete(key: string | string[]) {
    const keys = Array.isArray(key) ? key : [key]
    for (const k of keys) {
      this.files.delete(k)
    }
  }

  async list(options?: { prefix?: string }) {
    const prefix = options?.prefix ?? ''
    const objects = []
    for (const [key, file] of this.files) {
      if (key.startsWith(prefix)) {
        objects.push({ key, size: file.size })
      }
    }
    return { objects, truncated: false }
  }
}

// Mock Queue Message
function createMockMessage<T>(body: T) {
  return {
    body,
    ack: vi.fn(),
    retry: vi.fn(),
    attempts: 1,
  }
}

// Usage in tests
const bucket = new MockR2Bucket()
const message = createMockMessage({
  action: 'PutObject',
  bucket: 'test-bucket',
  object: { key: 'data/users/file.parquet', size: 1024 },
  eventTime: new Date().toISOString(),
})
```

---

## Local Development with Miniflare

### Setting Up Miniflare for Local Development

Vitest Pool Workers uses Miniflare under the hood for running Worker tests with real bindings.

**Create or update `vitest.workspace.ts`:**

The project already has this configured. Key settings for compaction testing:

```typescript
defineWorkersConfig({
  test: {
    name: 'e2e',
    include: ['tests/e2e/**/*.workers.test.ts'],
    poolOptions: {
      workers: {
        wrangler: {
          configPath: './wrangler.jsonc',
          environment: 'test',
        },
        isolatedStorage: true,
        miniflare: {
          compatibilityDate: '2026-01-28',
          compatibilityFlags: ['nodejs_compat'],
          r2Buckets: {
            BUCKET: 'parquedb-test',
          },
          durableObjects: {
            COMPACTION_STATE: {
              className: 'CompactionStateDO',
            },
          },
          queues: {
            COMPACTION_QUEUE: {
              queueName: 'parquedb-compaction-events',
            },
          },
        },
      },
    },
  },
})
```

### Configuring Local R2 Bucket

Miniflare provides a local R2 implementation that persists to disk:

```typescript
// In vitest.workspace.ts or wrangler.jsonc
miniflare: {
  r2Buckets: {
    BUCKET: 'parquedb-test',
    LOGS_BUCKET: 'parquedb-logs',
  },
  // Optional: persist R2 data between test runs
  r2Persist: './.r2-data',
}
```

**For wrangler dev mode:**

```bash
# Start local development server with R2
pnpm exec wrangler dev --local --persist

# This creates .wrangler/state for local R2 data
```

### Simulating R2 Event Notifications Locally

R2 event notifications are not available locally, but you can simulate them:

**Option 1: Direct queue message injection**

```typescript
// In your test or dev script
const message = {
  action: 'PutObject',
  bucket: 'parquedb',
  object: {
    key: 'data/users/1700001234-writer1-0.parquet',
    size: 1024,
    eTag: '"abc123"',
  },
  eventTime: new Date().toISOString(),
}

// If using wrangler dev with queue binding
await env.COMPACTION_QUEUE.send(message)
```

**Option 2: HTTP endpoint for testing**

Create a development-only endpoint:

```typescript
// Add to your worker for local testing
if (request.url.includes('/dev/simulate-r2-event')) {
  const body = await request.json()
  await env.COMPACTION_QUEUE.send(body)
  return new Response('Event simulated')
}
```

**Option 3: File watcher script**

```typescript
// scripts/watch-r2-local.ts
import { watch } from 'fs'
import { R2Backend } from '../src/storage/R2Backend'

const WATCH_DIR = './.wrangler/state/r2/parquedb-test'

watch(WATCH_DIR, { recursive: true }, async (event, filename) => {
  if (filename?.endsWith('.parquet')) {
    console.log(`File ${event}: ${filename}`)
    // Trigger compaction check via HTTP
    await fetch('http://localhost:8787/dev/simulate-r2-event', {
      method: 'POST',
      body: JSON.stringify({
        action: 'PutObject',
        object: { key: filename, size: 1024 },
      }),
    })
  }
})
```

### Testing Queue Consumers

To test the queue consumer locally:

```bash
# Start wrangler in dev mode
pnpm exec wrangler dev --local

# In another terminal, send test messages
pnpm exec wrangler queues send parquedb-compaction-events '{
  "action": "PutObject",
  "bucket": "parquedb",
  "object": {
    "key": "data/users/1700001234-writer1-0.parquet",
    "size": 1024
  }
}'
```

---

## Integration Testing

### How to Run E2E Compaction Tests

**Run all E2E tests (Node.js mock environment):**

```bash
pnpm test:e2e
```

**Run worker E2E tests (with Miniflare):**

```bash
pnpm test:e2e:workers
```

**Run specific compaction E2E tests:**

```bash
pnpm test tests/e2e/compaction-workflow.test.ts
pnpm test tests/e2e/compaction-data-integrity.test.ts
```

### Using vitest-pool-workers

The `@cloudflare/vitest-pool-workers` package enables testing with real Cloudflare bindings:

```typescript
// tests/e2e/compaction.workers.test.ts
import { describe, it, expect } from 'vitest'
import { env } from 'cloudflare:test'

describe('Compaction E2E', () => {
  it('should process R2 events through the queue', async () => {
    // Write a file to R2
    await env.BUCKET.put(
      'data/users/1700001234-writer1-0.parquet',
      new Uint8Array([/* parquet data */])
    )

    // Send queue message
    const message = {
      action: 'PutObject',
      object: { key: 'data/users/1700001234-writer1-0.parquet', size: 1024 },
    }

    // Process through queue consumer
    // ...
  })
})
```

### Environment Setup

**Required environment variables for testing:**

```bash
# .env.test (create if needed)
CF_ACCOUNT_ID=your-account-id  # Only for remote tests
```

**Test configuration in wrangler.jsonc:**

```jsonc
{
  "env": {
    "test": {
      "name": "parquedb-test",
      "r2_buckets": [
        { "binding": "BUCKET", "bucket_name": "parquedb-test" }
      ],
      "vars": {
        "ENVIRONMENT": "test"
      }
    }
  }
}
```

---

## Debugging Tips

### Logging Configuration

**Enable verbose logging in tests:**

```typescript
import { logger } from '@/utils/logger'

beforeAll(() => {
  // Set log level to debug
  logger.setLevel('debug')
})
```

**View logs during wrangler dev:**

```bash
pnpm exec wrangler dev --local --log-level debug
```

### Inspecting DO State

**In tests:**

```typescript
// Using TestableCompactionStateDO
const statusResponse = await compactionDO.fetch(
  new Request('http://internal/status')
)
const status = await statusResponse.json()
console.log('DO Status:', JSON.stringify(status, null, 2))
```

**Via HTTP (wrangler dev):**

```bash
curl http://localhost:8787/compaction/status?namespace=users | jq
```

**Inspect raw storage:**

```typescript
// In tests with MockDurableObjectState
const rawState = state.getData('compactionState')
console.log('Raw state:', rawState)

// Per-window storage
const windowData = state.getData('window:1700000000000')
console.log('Window data:', windowData)
```

### Common Issues and Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| Tests hang indefinitely | Fake timers not advancing | Use `vi.advanceTimersByTime()` |
| R2 operations fail | Missing mock setup | Use `MockR2Bucket` class |
| Queue messages not processed | Consumer not invoked | Call consumer manually in tests |
| Window never becomes ready | Timestamp too recent | Use old timestamp (`Date.now() - 2 * 60 * 60 * 1000`) |
| Files below threshold | `minFilesToCompact` not met | Add more files (default: 10) |
| State not persisting | Wrong storage key | Check `compactionState` vs `metadata` keys |

**Common timestamp pitfall:**

```typescript
// WRONG - window is too recent
const updates = [createUpdate({ timestamp: Date.now() })]

// CORRECT - window is old enough to be ready
const oldTimestamp = Date.now() - (3600000 + 400000) // 1hr + buffer
const updates = [createUpdate({ timestamp: oldTimestamp })]
```

**Meeting minimum file threshold:**

```typescript
// Need at least 10 files by default
const updates = Array.from({ length: 15 }, (_, i) =>
  createUpdate({
    timestamp: oldTimestamp,
    file: `file${i}.parquet`,
  })
)
```

---

## Example Workflow

### Step-by-Step Guide to Test a Compaction Change

**1. Write a failing test first (TDD):**

```bash
# Create or modify test file
code tests/unit/workflows/compaction-my-feature.test.ts
```

```typescript
import { describe, it, expect, beforeEach } from 'vitest'
import {
  TestableCompactionStateDO,
  MockDurableObjectState,
  createUpdateRequest,
  createUpdate,
} from './__helpers__/testable-compaction-state-do'

describe('My new compaction feature', () => {
  let state: MockDurableObjectState
  let compactionDO: TestableCompactionStateDO

  beforeEach(() => {
    state = new MockDurableObjectState()
    compactionDO = new TestableCompactionStateDO(state)
  })

  it('should do the new thing', async () => {
    // Arrange
    const oldTimestamp = Date.now() - (3600000 + 400000)
    const updates = Array.from({ length: 15 }, (_, i) =>
      createUpdate({ timestamp: oldTimestamp, file: `file${i}.parquet` })
    )

    // Act
    const response = await compactionDO.fetch(new Request('http://internal/update', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(createUpdateRequest({ updates })),
    }))

    // Assert
    const result = await response.json()
    expect(result.myNewField).toBeDefined() // This will fail initially
  })
})
```

**2. Run the test to verify it fails:**

```bash
pnpm test tests/unit/workflows/compaction-my-feature.test.ts
```

**3. Implement the feature:**

Edit the relevant source files in `src/workflows/` or `src/worker/`.

**4. Run the test again to verify it passes:**

```bash
pnpm test tests/unit/workflows/compaction-my-feature.test.ts
```

**5. Run all compaction tests to check for regressions:**

```bash
pnpm test -- --filter=compaction
```

**6. Run E2E tests:**

```bash
pnpm test tests/e2e/compaction-workflow.test.ts
```

**7. Test locally with wrangler dev:**

```bash
# Terminal 1: Start dev server
pnpm exec wrangler dev --local

# Terminal 2: Send test requests
curl -X POST http://localhost:8787/compaction/update \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": "users",
    "updates": [
      {
        "namespace": "users",
        "writerId": "writer1",
        "file": "data/users/1700001234-writer1-0.parquet",
        "timestamp": 1700001234000,
        "size": 1024
      }
    ],
    "config": {
      "windowSizeMs": 3600000,
      "minFilesToCompact": 10,
      "maxWaitTimeMs": 300000,
      "targetFormat": "native"
    }
  }'

# Check status
curl http://localhost:8787/compaction/status?namespace=users | jq
```

**8. Run the full test suite before committing:**

```bash
pnpm test
```

---

## Quick Reference

### Test Commands

```bash
# All tests
pnpm test

# Unit tests only
pnpm test:unit

# E2E tests (Node.js)
pnpm test:e2e

# E2E tests (Workers with Miniflare)
pnpm test:e2e:workers

# Specific file
pnpm test tests/unit/workflows/compaction-state-do.test.ts

# Watch mode
pnpm test -- --watch

# With coverage
pnpm test -- --coverage
```

### Key Test Files

| Path | Purpose |
|------|---------|
| `tests/unit/workflows/__helpers__/testable-compaction-state-do.ts` | Testable DO implementation |
| `tests/e2e/compaction-workflow.test.ts` | Full workflow E2E tests |
| `tests/setup.ts` | Global test setup |
| `vitest.config.ts` | Default Vitest config |
| `vitest.workspace.ts` | Workspace config with Workers |

### Related Documentation

- [Compaction Workflow Setup](./compaction-workflow.md) - Production setup guide
- [Compaction Runbook](./compaction-runbook.md) - Operational procedures
- [R2 Event Notifications](./r2-event-notifications.md) - Event notification setup
- [E2E Benchmarks](./e2e-benchmarks.md) - Performance testing guide
