# Evalite Adapter

ParqueDB provides a storage adapter for [Evalite](https://evalite.dev), the TypeScript AI evaluation framework. This enables persistent storage of evaluation runs, results, scores, and traces using ParqueDB's flexible storage backends.

## Installation

```bash
npm install parquedb evalite
```

## Quick Start

```typescript
import { defineConfig } from 'evalite'
import { createEvaliteAdapter } from 'parquedb/integrations/evalite'
import { MemoryBackend } from 'parquedb/storage'

export default defineConfig({
  storage: () => createEvaliteAdapter({
    storage: new MemoryBackend(),
  }),
})
```

## Production Setup with R2

```typescript
import { defineConfig } from 'evalite'
import { createEvaliteAdapter } from 'parquedb/integrations/evalite'
import { R2Backend } from 'parquedb/storage'

export default defineConfig({
  storage: () => createEvaliteAdapter({
    storage: new R2Backend(env.EVALITE_BUCKET),
    collectionPrefix: 'evalite',
    debug: false,
  }),
})
```

## API Reference

### `createEvaliteAdapter(config)`

Creates a new Evalite storage adapter using ParqueDB.

```typescript
function createEvaliteAdapter(
  config: EvaliteAdapterConfig
): ParqueDBEvaliteAdapter
```

### Configuration

```typescript
interface EvaliteAdapterConfig {
  /**
   * Storage backend for data persistence
   */
  storage: StorageBackend

  /**
   * Collection prefix for Evalite data
   * @default 'evalite'
   */
  collectionPrefix?: string

  /**
   * Default actor for audit fields
   * @default 'system/evalite'
   */
  defaultActor?: EntityId

  /**
   * Enable debug logging
   * @default false
   */
  debug?: boolean
}
```

## Data Model

The adapter creates the following collections (prefixed with `collectionPrefix`):

### Collections

| Collection | Description |
|------------|-------------|
| `{prefix}_runs` | Evaluation runs (full or partial) |
| `{prefix}_suites` | Test suites within runs |
| `{prefix}_evals` | Individual evaluation results |
| `{prefix}_scores` | Scorer outputs for each evaluation |
| `{prefix}_traces` | LLM execution traces with token usage |

### Entity Relationships

```
Run (1) ─────> (*) Suite
Suite (1) ───> (*) Eval
Eval (1) ────> (*) Score
Eval (1) ────> (*) Trace
```

## Types

### Run Types

```typescript
/** Run type - full or partial evaluation run */
type RunType = 'full' | 'partial'

/** Evaluation run - a collection of suites executed together */
interface EvalRun {
  /** Unique run ID */
  id: number
  /** Run type */
  runType: RunType
  /** When the run was created */
  createdAt: string
}
```

### Suite Types

```typescript
/** Suite status */
type SuiteStatus = 'running' | 'success' | 'fail'

/** Evaluation suite - a group of related evaluations */
interface EvalSuite {
  /** Unique suite ID */
  id: number
  /** Parent run ID */
  runId: number
  /** Suite name (typically file path) */
  name: string
  /** Suite status */
  status: SuiteStatus
  /** Total duration in milliseconds */
  duration: number
  /** When the suite was created */
  createdAt: string
}
```

### Eval Types

```typescript
/** Evaluation status */
type EvalStatus = 'running' | 'success' | 'fail'

/** Individual evaluation result */
interface EvalResult {
  /** Unique eval ID */
  id: number
  /** Parent suite ID */
  suiteId: number
  /** Execution duration in milliseconds */
  duration: number
  /** Input data passed to the task */
  input: unknown
  /** Output produced by the task */
  output: unknown
  /** Expected output (if provided) */
  expected?: unknown
  /** Evaluation status */
  status: EvalStatus
  /** Column ordering for display */
  colOrder: number
  /** Custom rendered columns */
  renderedColumns?: unknown
  /** When the eval was created */
  createdAt: string
  /** All scores for this eval (populated on read) */
  scores?: EvalScore[]
  /** All traces for this eval (populated on read) */
  traces?: EvalTrace[]
}
```

### Score Types

```typescript
/** Scorer result for an evaluation */
interface EvalScore {
  /** Unique score ID */
  id: number
  /** Parent eval ID */
  evalId: number
  /** Scorer name */
  name: string
  /** Score value (0-1 range) */
  score: number
  /** Optional description */
  description?: string
  /** Additional metadata */
  metadata?: unknown
  /** When the score was created */
  createdAt: string
}
```

### Trace Types

```typescript
/** LLM execution trace */
interface EvalTrace {
  /** Unique trace ID */
  id: number
  /** Parent eval ID */
  evalId: number
  /** Input to the LLM call */
  input: unknown
  /** Output from the LLM call */
  output: unknown
  /** Start time (ms since epoch) */
  startTime: number
  /** End time (ms since epoch) */
  endTime: number
  /** Input tokens used */
  inputTokens?: number
  /** Output tokens generated */
  outputTokens?: number
  /** Total tokens (input + output) */
  totalTokens?: number
  /** Column ordering for display */
  colOrder: number
  /** When the trace was created */
  createdAt: string
}
```

## Entity Managers

The adapter exposes entity managers matching Evalite's interface:

### runs

```typescript
// Create a new run
const run = await adapter.runs.create({
  runType: 'full', // or 'partial'
})

// Get runs with filtering
const runs = await adapter.runs.getMany({
  runType: 'full',
  limit: 10,
  orderBy: 'createdAt',
})
```

### suites

```typescript
// Create a suite
const suite = await adapter.suites.create({
  runId: run.id,
  name: 'my-evaluation-suite',
  status: 'running',
})

// Update suite
await adapter.suites.update({
  id: suite.id,
  status: 'success',
  duration: 1500,
})

// Get suites
const suites = await adapter.suites.getMany({
  runId: run.id,
  status: 'success',
})
```

### evals

```typescript
// Create an eval
const evalResult = await adapter.evals.create({
  suiteId: suite.id,
  input: { prompt: 'Hello, AI!' },
  output: { response: 'Hello, human!' },
  expected: { response: 'Hello, human!' },
  status: 'success',
  colOrder: 0,
})

// Update eval
await adapter.evals.update({
  id: evalResult.id,
  status: 'success',
  duration: 250,
})

// Get evals with scores and traces
const evals = await adapter.evals.getMany({
  suiteId: suite.id,
  includeScores: true,
  includeTraces: true,
})
```

### scores

```typescript
// Create a score
const score = await adapter.scores.create({
  evalId: evalResult.id,
  name: 'accuracy',
  score: 0.95,
  description: 'Response matches expected output',
})

// Get scores
const scores = await adapter.scores.getMany({
  evalId: evalResult.id,
  name: 'accuracy',
})
```

### traces

```typescript
// Create a trace
const trace = await adapter.traces.create({
  evalId: evalResult.id,
  input: { messages: [{ role: 'user', content: 'Hello' }] },
  output: { content: 'Hello!' },
  startTime: Date.now() - 500,
  endTime: Date.now(),
  inputTokens: 10,
  outputTokens: 5,
  totalTokens: 15,
  colOrder: 0,
})

// Get traces
const traces = await adapter.traces.getMany({
  evalId: evalResult.id,
})
```

## Analytics Methods

### `getRunWithResults(runId)`

Get a complete run with all nested data populated.

```typescript
const runWithResults = await adapter.getRunWithResults(run.id)

console.log(runWithResults)
// {
//   id: 1,
//   runType: 'full',
//   createdAt: '2024-01-15T10:00:00Z',
//   suites: [
//     {
//       id: 1,
//       name: 'my-suite',
//       status: 'success',
//       duration: 1500,
//       evals: [
//         {
//           id: 1,
//           input: { ... },
//           output: { ... },
//           scores: [{ name: 'accuracy', score: 0.95 }],
//           traces: [{ inputTokens: 10, outputTokens: 5 }],
//         }
//       ]
//     }
//   ],
//   stats: {
//     totalSuites: 1,
//     totalEvals: 10,
//     successCount: 9,
//     failCount: 1,
//     runningCount: 0,
//     averageScore: 0.92,
//     totalDuration: 1500,
//     totalTokens: 1500
//   }
// }
```

**RunStats:**

```typescript
interface RunStats {
  /** Total number of suites */
  totalSuites: number
  /** Total number of evals */
  totalEvals: number
  /** Successful evals */
  successCount: number
  /** Failed evals */
  failCount: number
  /** Running evals */
  runningCount: number
  /** Average score across all evals */
  averageScore: number
  /** Total duration in milliseconds */
  totalDuration: number
  /** Total tokens used */
  totalTokens: number
}
```

### `getScoreHistory(evalName, options?)`

Get score history for a specific evaluation over time. Useful for dashboards showing score trends.

```typescript
const history = await adapter.getScoreHistory('my-evaluation-suite', {
  limit: 100,
  from: new Date('2024-01-01'),
  to: new Date(),
  scorerName: 'accuracy', // Filter by specific scorer
})

console.log(history)
// [
//   {
//     timestamp: '2024-01-15T10:00:00Z',
//     runId: 1,
//     averageScore: 0.85,
//     minScore: 0.7,
//     maxScore: 1.0,
//     evalCount: 10
//   },
//   {
//     timestamp: '2024-01-16T10:00:00Z',
//     runId: 2,
//     averageScore: 0.92,
//     minScore: 0.8,
//     maxScore: 1.0,
//     evalCount: 10
//   }
// ]
```

**ScoreHistoryOptions:**

```typescript
interface ScoreHistoryOptions {
  /** Maximum number of data points */
  limit?: number
  /** Start date */
  from?: Date
  /** End date */
  to?: Date
  /** Filter by scorer name */
  scorerName?: string
}
```

## Convenience Methods

### `saveRun(run)`

Store an evaluation run.

```typescript
const run = await adapter.saveRun({
  id: 1,
  runType: 'full',
})
```

### `saveResults(runId, suiteName, results)`

Store complete evaluation results in a single call.

```typescript
await adapter.saveResults(run.id, 'my-suite', [
  {
    input: { prompt: 'What is 2+2?' },
    output: { response: '4' },
    expected: { response: '4' },
    scores: [
      { name: 'accuracy', score: 1.0, description: 'Correct answer' },
      { name: 'latency', score: 0.9, description: 'Fast response' },
    ],
    traces: [
      {
        input: { messages: [{ role: 'user', content: 'What is 2+2?' }] },
        output: { content: '4' },
        startTime: Date.now() - 200,
        endTime: Date.now(),
        inputTokens: 15,
        outputTokens: 3,
        totalTokens: 18,
      },
    ],
  },
])
```

## Examples

### Basic Evalite Configuration

```typescript
// evalite.config.ts
import { defineConfig } from 'evalite'
import { createEvaliteAdapter } from 'parquedb/integrations/evalite'
import { MemoryBackend } from 'parquedb/storage'

export default defineConfig({
  storage: () => createEvaliteAdapter({
    storage: new MemoryBackend(),
  }),
})
```

### Production with S3

```typescript
// evalite.config.ts
import { defineConfig } from 'evalite'
import { createEvaliteAdapter } from 'parquedb/integrations/evalite'
import { S3Backend } from 'parquedb/storage'

export default defineConfig({
  storage: () => createEvaliteAdapter({
    storage: new S3Backend({
      bucket: process.env.EVALITE_BUCKET!,
      region: process.env.AWS_REGION!,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
      },
    }),
    collectionPrefix: 'evalite_prod',
  }),
})
```

### Custom Dashboard Data

```typescript
import { createEvaliteAdapter } from 'parquedb/integrations/evalite'
import { R2Backend } from 'parquedb/storage'

async function getDashboardData(env: Env) {
  const adapter = createEvaliteAdapter({
    storage: new R2Backend(env.EVALITE_BUCKET),
  })

  // Get recent runs
  const recentRuns = await adapter.runs.getMany({
    limit: 10,
    orderBy: '-createdAt',
  })

  // Get detailed data for latest run
  const latestRun = recentRuns[0]
  const runDetails = latestRun
    ? await adapter.getRunWithResults(latestRun.id)
    : null

  // Get score history for key evaluations
  const accuracyHistory = await adapter.getScoreHistory('accuracy-suite', {
    limit: 30,
    scorerName: 'accuracy',
  })

  return {
    recentRuns,
    latestRunDetails: runDetails,
    accuracyTrend: accuracyHistory,
  }
}
```

### Programmatic Evaluation Storage

```typescript
import { createEvaliteAdapter } from 'parquedb/integrations/evalite'
import { MemoryBackend } from 'parquedb/storage'

async function runCustomEvaluation() {
  const adapter = createEvaliteAdapter({
    storage: new MemoryBackend(),
    debug: true,
  })

  // Create run
  const run = await adapter.runs.create({ runType: 'full' })

  // Create suite
  const suite = await adapter.suites.create({
    runId: run.id,
    name: 'custom-evaluation',
  })

  // Run evaluations
  for (const testCase of testCases) {
    const startTime = Date.now()

    // Execute the AI task
    const output = await runAITask(testCase.input)

    const endTime = Date.now()

    // Create eval
    const evalResult = await adapter.evals.create({
      suiteId: suite.id,
      input: testCase.input,
      output,
      expected: testCase.expected,
      status: 'success',
    })

    // Calculate and store scores
    for (const scorer of scorers) {
      const score = await scorer.score(output, testCase.expected)
      await adapter.scores.create({
        evalId: evalResult.id,
        name: scorer.name,
        score: score.value,
        description: score.description,
      })
    }

    // Store trace
    await adapter.traces.create({
      evalId: evalResult.id,
      input: testCase.input,
      output,
      startTime,
      endTime,
      totalTokens: output.usage?.totalTokens,
    })

    // Update eval with duration
    await adapter.evals.update({
      id: evalResult.id,
      duration: endTime - startTime,
    })
  }

  // Complete suite
  await adapter.suites.update({
    id: suite.id,
    status: 'success',
    duration: Date.now() - new Date(suite.createdAt).getTime(),
  })

  // Get full results
  return adapter.getRunWithResults(run.id)
}
```

## Storage Backend Options

### Memory Backend (Development/Testing)

```typescript
import { MemoryBackend } from 'parquedb/storage'

createEvaliteAdapter({
  storage: new MemoryBackend(),
})
```

### Filesystem Backend (Local Development)

```typescript
import { FSBackend } from 'parquedb/storage'

createEvaliteAdapter({
  storage: new FSBackend('./evalite-data'),
})
```

### R2 Backend (Cloudflare Workers)

```typescript
import { R2Backend } from 'parquedb/storage'

createEvaliteAdapter({
  storage: new R2Backend(env.EVALITE_BUCKET),
})
```

### S3 Backend (AWS)

```typescript
import { S3Backend } from 'parquedb/storage'

createEvaliteAdapter({
  storage: new S3Backend({
    bucket: 'evalite-data',
    region: 'us-east-1',
  }),
})
```

## Best Practices

### 1. Use Meaningful Suite Names

Use descriptive names that help identify evaluations:

```typescript
// Good
'translation-quality-tests'
'sentiment-analysis-accuracy'
'rag-retrieval-relevance'

// Avoid
'test1'
'suite'
```

### 2. Include Structured Input/Output

Store structured data for better analysis:

```typescript
// Good
{
  input: {
    prompt: 'Translate to French: Hello',
    language: 'fr',
    formality: 'informal',
  },
  output: {
    translation: 'Salut',
    confidence: 0.95,
    alternates: ['Bonjour'],
  },
}

// Avoid
{
  input: 'Translate to French: Hello',
  output: 'Salut',
}
```

### 3. Track Token Usage

Always capture token usage in traces for cost analysis:

```typescript
await adapter.traces.create({
  evalId,
  input,
  output,
  startTime,
  endTime,
  inputTokens: response.usage.prompt_tokens,
  outputTokens: response.usage.completion_tokens,
  totalTokens: response.usage.total_tokens,
})
```

### 4. Use Score Ranges Consistently

Keep scores in the 0-1 range for consistency:

```typescript
// Good: Normalized scores
{ name: 'accuracy', score: 0.85 }
{ name: 'fluency', score: 0.92 }

// Avoid: Different scales
{ name: 'accuracy', score: 85 }      // 0-100
{ name: 'fluency', score: 4.6 }      // 0-5
```

### 5. Separate Production and Development Data

Use different collection prefixes:

```typescript
// Development
createEvaliteAdapter({
  storage: backend,
  collectionPrefix: 'evalite_dev',
})

// Production
createEvaliteAdapter({
  storage: backend,
  collectionPrefix: 'evalite_prod',
})
```
