#!/usr/bin/env tsx
/**
 * Load Testing Suite for Compaction System
 *
 * Tests the compaction system under realistic data volumes to validate:
 * - Scalability across multiple namespaces
 * - High write throughput (files/second)
 * - Large file handling
 * - High concurrency scenarios
 *
 * Usage:
 *   tsx scripts/load-test-compaction.ts [options]
 *
 * Options:
 *   --scenario=<name>   Run predefined scenario: A, B, C, D, or custom
 *   --endpoint=<url>    Base URL (default: http://localhost:8787)
 *   --duration=<sec>    Test duration in seconds (default: 60)
 *   --namespaces=<n>    Number of namespaces (default: 10)
 *   --files-per-sec=<n> Target files per second (default: 100)
 *   --file-size=<bytes> Average file size in bytes (default: 1024)
 *   --writers=<n>       Number of concurrent writers (default: 1)
 *   --poll-interval=<ms> Health poll interval in ms (default: 10000)
 *   --output=<format>   Output format: table, json, csv (default: table)
 *   --output-file=<path> Output file path (default: stdout)
 *   --dry-run           Show config without running
 *   --help              Show this help
 *
 * Predefined Scenarios:
 *   A: 10 namespaces, 100 files/sec, 1KB files, 10 min (baseline)
 *   B: 100 namespaces, 1000 files/sec, 10KB files, 1 hour (high volume)
 *   C: 5 namespaces, 10 files/sec, 100MB files, 30 min (large files)
 *   D: 1 namespace, 500 files/sec, 1KB files, 10 min (high concurrency, 50 writers)
 *
 * @example
 * # Run scenario A against local miniflare
 * tsx scripts/load-test-compaction.ts --scenario=A
 *
 * # Run scenario B against staging
 * tsx scripts/load-test-compaction.ts --scenario=B --endpoint=https://staging.example.com
 *
 * # Custom configuration
 * tsx scripts/load-test-compaction.ts --namespaces=20 --files-per-sec=200 --duration=300
 *
 * # Output to CSV file
 * tsx scripts/load-test-compaction.ts --scenario=A --output=csv --output-file=results.csv
 */

// =============================================================================
// Types
// =============================================================================

interface LoadTestConfig {
  scenario: string
  endpoint: string
  durationSec: number
  numNamespaces: number
  filesPerSecond: number
  fileSizeBytes: number
  numWriters: number
  pollIntervalMs: number
  outputFormat: 'table' | 'json' | 'csv'
  outputFile: string | null
  dryRun: boolean
}

interface HealthMetric {
  timestamp: number
  status: 'healthy' | 'degraded' | 'unhealthy' | 'error'
  totalActiveWindows: number
  oldestWindowAgeMs: number
  totalPendingFiles: number
  windowsStuckInProcessing: number
  namespaceCount: number
  alerts: string[]
  error?: string
}

interface WriteMetric {
  timestamp: number
  namespace: string
  writerId: string
  fileKey: string
  sizeBytes: number
  latencyMs: number
  success: boolean
  error?: string
}

interface WorkflowMetric {
  timestamp: number
  workflowId: string
  namespace: string
  status: 'created' | 'failed'
  error?: string
}

interface LoadTestResults {
  config: LoadTestConfig
  startTime: number
  endTime: number
  durationMs: number
  healthMetrics: HealthMetric[]
  writeMetrics: WriteMetric[]
  workflowMetrics: WorkflowMetric[]
  summary: LoadTestSummary
}

interface LoadTestSummary {
  totalFilesWritten: number
  totalBytesWritten: number
  totalFilesSucceeded: number
  totalFilesFailed: number
  writeLatencyP50: number
  writeLatencyP95: number
  writeLatencyP99: number
  writeLatencyMax: number
  actualFilesPerSecond: number
  healthChecks: number
  healthStatusCounts: Record<string, number>
  maxActiveWindows: number
  maxPendingFiles: number
  maxOldestWindowAgeMs: number
  workflowsCreated: number
  workflowsFailed: number
}

// =============================================================================
// Predefined Scenarios
// =============================================================================

const SCENARIOS: Record<string, Partial<LoadTestConfig>> = {
  A: {
    scenario: 'A',
    durationSec: 600, // 10 minutes
    numNamespaces: 10,
    filesPerSecond: 100,
    fileSizeBytes: 1024, // 1KB
    numWriters: 1,
  },
  B: {
    scenario: 'B',
    durationSec: 3600, // 1 hour
    numNamespaces: 100,
    filesPerSecond: 1000,
    fileSizeBytes: 10240, // 10KB
    numWriters: 10,
  },
  C: {
    scenario: 'C',
    durationSec: 1800, // 30 minutes
    numNamespaces: 5,
    filesPerSecond: 10,
    fileSizeBytes: 100 * 1024 * 1024, // 100MB
    numWriters: 1,
  },
  D: {
    scenario: 'D',
    durationSec: 600, // 10 minutes
    numNamespaces: 1,
    filesPerSecond: 500,
    fileSizeBytes: 1024, // 1KB
    numWriters: 50,
  },
}

const DEFAULT_CONFIG: LoadTestConfig = {
  scenario: 'custom',
  endpoint: 'http://localhost:8787',
  durationSec: 60,
  numNamespaces: 10,
  filesPerSecond: 100,
  fileSizeBytes: 1024,
  numWriters: 1,
  pollIntervalMs: 10000,
  outputFormat: 'table',
  outputFile: null,
  dryRun: false,
}

// =============================================================================
// Argument Parsing
// =============================================================================

function parseArgs(): LoadTestConfig {
  const args = process.argv.slice(2)
  let config = { ...DEFAULT_CONFIG }

  for (const arg of args) {
    if (arg === '--help' || arg === '-h') {
      printHelp()
      process.exit(0)
    }

    const [key, value] = arg.split('=')
    switch (key) {
      case '--scenario':
        if (value && SCENARIOS[value]) {
          config = { ...config, ...SCENARIOS[value] }
        } else if (value) {
          console.error(`Unknown scenario: ${value}. Available: ${Object.keys(SCENARIOS).join(', ')}`)
          process.exit(1)
        }
        break
      case '--endpoint':
        config.endpoint = value ?? config.endpoint
        break
      case '--duration':
        config.durationSec = parseInt(value ?? '60', 10)
        break
      case '--namespaces':
        config.numNamespaces = parseInt(value ?? '10', 10)
        break
      case '--files-per-sec':
        config.filesPerSecond = parseInt(value ?? '100', 10)
        break
      case '--file-size':
        config.fileSizeBytes = parseInt(value ?? '1024', 10)
        break
      case '--writers':
        config.numWriters = parseInt(value ?? '1', 10)
        break
      case '--poll-interval':
        config.pollIntervalMs = parseInt(value ?? '10000', 10)
        break
      case '--output':
        config.outputFormat = (value as 'table' | 'json' | 'csv') ?? 'table'
        break
      case '--output-file':
        config.outputFile = value ?? null
        break
      case '--dry-run':
        config.dryRun = true
        break
    }
  }

  return config
}

function printHelp(): void {
  console.log(`
Load Testing Suite for ParqueDB Compaction System

Usage:
  tsx scripts/load-test-compaction.ts [options]

Options:
  --scenario=<name>     Run predefined scenario: A, B, C, D
  --endpoint=<url>      Base URL (default: http://localhost:8787)
  --duration=<sec>      Test duration in seconds (default: 60)
  --namespaces=<n>      Number of namespaces (default: 10)
  --files-per-sec=<n>   Target files per second (default: 100)
  --file-size=<bytes>   Average file size in bytes (default: 1024)
  --writers=<n>         Number of concurrent writers (default: 1)
  --poll-interval=<ms>  Health poll interval in ms (default: 10000)
  --output=<format>     Output format: table, json, csv (default: table)
  --output-file=<path>  Output file path (default: stdout)
  --dry-run             Show config without running
  --help                Show this help

Predefined Scenarios:
  A: Baseline - 10 namespaces, 100 files/sec, 1KB files, 10 min
  B: High Volume - 100 namespaces, 1000 files/sec, 10KB files, 1 hour
  C: Large Files - 5 namespaces, 10 files/sec, 100MB files, 30 min
  D: High Concurrency - 1 namespace, 500 files/sec, 50 writers, 10 min

Examples:
  # Run scenario A against local miniflare
  tsx scripts/load-test-compaction.ts --scenario=A

  # Run scenario B against staging
  tsx scripts/load-test-compaction.ts --scenario=B --endpoint=https://staging.example.com

  # Custom configuration
  tsx scripts/load-test-compaction.ts --namespaces=20 --files-per-sec=200 --duration=300

  # Output to CSV
  tsx scripts/load-test-compaction.ts --scenario=A --output=csv --output-file=results.csv
`)
}

// =============================================================================
// Utilities
// =============================================================================

function generateWriterId(): string {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
  let id = ''
  for (let i = 0; i < 8; i++) {
    id += chars.charAt(Math.floor(Math.random() * chars.length))
  }
  return id
}

function generateParquetLikeData(sizeBytes: number): Uint8Array {
  // Generate realistic-looking parquet file data
  // Parquet magic bytes: PAR1 at start and end
  const data = new Uint8Array(sizeBytes)

  // PAR1 magic at start
  data[0] = 0x50 // P
  data[1] = 0x41 // A
  data[2] = 0x52 // R
  data[3] = 0x31 // 1

  // Fill middle with random data simulating compressed column data
  for (let i = 4; i < sizeBytes - 4; i++) {
    data[i] = Math.floor(Math.random() * 256)
  }

  // PAR1 magic at end
  if (sizeBytes > 7) {
    data[sizeBytes - 4] = 0x50 // P
    data[sizeBytes - 3] = 0x41 // A
    data[sizeBytes - 2] = 0x52 // R
    data[sizeBytes - 1] = 0x31 // 1
  }

  return data
}

function percentile(arr: number[], p: number): number {
  if (arr.length === 0) return 0
  const sorted = [...arr].sort((a, b) => a - b)
  const idx = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, idx)] ?? 0
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)}GB`
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms.toFixed(0)}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  if (ms < 3600000) return `${(ms / 60000).toFixed(1)}m`
  return `${(ms / 3600000).toFixed(1)}h`
}

// =============================================================================
// Health Polling
// =============================================================================

async function pollHealth(endpoint: string): Promise<HealthMetric> {
  const timestamp = Date.now()

  try {
    const response = await fetch(`${endpoint}/compaction/health`, {
      method: 'GET',
      headers: { 'Accept': 'application/json' },
    })

    if (!response.ok) {
      return {
        timestamp,
        status: 'error',
        totalActiveWindows: 0,
        oldestWindowAgeMs: 0,
        totalPendingFiles: 0,
        windowsStuckInProcessing: 0,
        namespaceCount: 0,
        alerts: [],
        error: `HTTP ${response.status}: ${response.statusText}`,
      }
    }

    const data = await response.json() as {
      status: 'healthy' | 'degraded' | 'unhealthy'
      namespaces: Record<string, {
        metrics: {
          activeWindows: number
          oldestWindowAge: number
          totalPendingFiles: number
          windowsStuckInProcessing: number
        }
      }>
      alerts: string[]
    }

    // Aggregate metrics across all namespaces
    let totalActiveWindows = 0
    let maxOldestWindowAge = 0
    let totalPendingFiles = 0
    let totalStuck = 0

    for (const ns of Object.values(data.namespaces)) {
      totalActiveWindows += ns.metrics.activeWindows
      maxOldestWindowAge = Math.max(maxOldestWindowAge, ns.metrics.oldestWindowAge)
      totalPendingFiles += ns.metrics.totalPendingFiles
      totalStuck += ns.metrics.windowsStuckInProcessing
    }

    return {
      timestamp,
      status: data.status,
      totalActiveWindows,
      oldestWindowAgeMs: maxOldestWindowAge,
      totalPendingFiles,
      windowsStuckInProcessing: totalStuck,
      namespaceCount: Object.keys(data.namespaces).length,
      alerts: data.alerts,
    }
  } catch (err) {
    return {
      timestamp,
      status: 'error',
      totalActiveWindows: 0,
      oldestWindowAgeMs: 0,
      totalPendingFiles: 0,
      windowsStuckInProcessing: 0,
      namespaceCount: 0,
      alerts: [],
      error: err instanceof Error ? err.message : String(err),
    }
  }
}

// =============================================================================
// File Upload Simulation
// =============================================================================

interface FileUploadResult {
  writeMetric: WriteMetric
  workflowMetric?: WorkflowMetric
}

async function uploadFile(
  endpoint: string,
  namespace: string,
  writerId: string,
  seqNum: number,
  data: Uint8Array
): Promise<FileUploadResult> {
  const timestamp = Date.now()
  const timestampSec = Math.floor(timestamp / 1000)
  const fileKey = `data/${namespace}/${timestampSec}-${writerId}-${seqNum}.parquet`

  const startTime = performance.now()

  try {
    // Upload to R2 via the worker endpoint
    // This simulates the R2 write that would trigger an event notification
    const response = await fetch(`${endpoint}/upload`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/octet-stream',
        'X-File-Key': fileKey,
      },
      body: data,
    })

    const latencyMs = performance.now() - startTime

    if (!response.ok) {
      return {
        writeMetric: {
          timestamp,
          namespace,
          writerId,
          fileKey,
          sizeBytes: data.length,
          latencyMs,
          success: false,
          error: `HTTP ${response.status}: ${response.statusText}`,
        },
      }
    }

    // Check if a workflow was triggered
    const responseData = await response.json().catch(() => ({})) as {
      workflowId?: string
      workflowCreated?: boolean
    }

    const result: FileUploadResult = {
      writeMetric: {
        timestamp,
        namespace,
        writerId,
        fileKey,
        sizeBytes: data.length,
        latencyMs,
        success: true,
      },
    }

    if (responseData.workflowId) {
      result.workflowMetric = {
        timestamp,
        workflowId: responseData.workflowId,
        namespace,
        status: 'created',
      }
    }

    return result
  } catch (err) {
    return {
      writeMetric: {
        timestamp,
        namespace,
        writerId,
        fileKey,
        sizeBytes: data.length,
        latencyMs: performance.now() - startTime,
        success: false,
        error: err instanceof Error ? err.message : String(err),
      },
    }
  }
}

// =============================================================================
// Writer Task
// =============================================================================

interface WriterContext {
  endpoint: string
  namespace: string
  writerId: string
  fileSizeBytes: number
  filesPerSecond: number
  durationMs: number
  writeMetrics: WriteMetric[]
  workflowMetrics: WorkflowMetric[]
  stopSignal: { stop: boolean }
}

async function writerTask(ctx: WriterContext): Promise<void> {
  const intervalMs = 1000 / ctx.filesPerSecond
  const startTime = Date.now()
  let seqNum = 0

  while (!ctx.stopSignal.stop && (Date.now() - startTime) < ctx.durationMs) {
    const iterStart = Date.now()

    // Generate and upload file
    const data = generateParquetLikeData(ctx.fileSizeBytes)
    const result = await uploadFile(
      ctx.endpoint,
      ctx.namespace,
      ctx.writerId,
      seqNum++,
      data
    )

    ctx.writeMetrics.push(result.writeMetric)
    if (result.workflowMetric) {
      ctx.workflowMetrics.push(result.workflowMetric)
    }

    // Rate limiting - wait for next interval
    const elapsed = Date.now() - iterStart
    const sleepMs = Math.max(0, intervalMs - elapsed)
    if (sleepMs > 0) {
      await new Promise(resolve => setTimeout(resolve, sleepMs))
    }
  }
}

// =============================================================================
// Load Test Runner
// =============================================================================

async function runLoadTest(config: LoadTestConfig): Promise<LoadTestResults> {
  console.log('\n' + '='.repeat(70))
  console.log('ParqueDB Compaction Load Test')
  console.log('='.repeat(70))
  console.log(`Scenario: ${config.scenario}`)
  console.log(`Endpoint: ${config.endpoint}`)
  console.log(`Duration: ${formatDuration(config.durationSec * 1000)}`)
  console.log(`Namespaces: ${config.numNamespaces}`)
  console.log(`Writers: ${config.numWriters}`)
  console.log(`Target files/sec: ${config.filesPerSecond}`)
  console.log(`File size: ${formatBytes(config.fileSizeBytes)}`)
  console.log(`Expected total files: ${config.filesPerSecond * config.durationSec}`)
  console.log(`Expected total data: ${formatBytes(config.filesPerSecond * config.durationSec * config.fileSizeBytes)}`)
  console.log('='.repeat(70))

  const startTime = Date.now()
  const durationMs = config.durationSec * 1000
  const healthMetrics: HealthMetric[] = []
  const writeMetrics: WriteMetric[] = []
  const workflowMetrics: WorkflowMetric[] = []
  const stopSignal = { stop: false }

  // Generate namespace names
  const namespaces: string[] = []
  for (let i = 0; i < config.numNamespaces; i++) {
    namespaces.push(`load-test-ns-${i.toString().padStart(3, '0')}`)
  }

  // Create writer contexts
  // Distribute writers across namespaces round-robin
  const writerContexts: WriterContext[] = []
  const filesPerWriter = Math.ceil(config.filesPerSecond / config.numWriters)

  for (let i = 0; i < config.numWriters; i++) {
    const namespace = namespaces[i % config.numNamespaces] ?? namespaces[0] ?? 'default'
    writerContexts.push({
      endpoint: config.endpoint,
      namespace,
      writerId: generateWriterId(),
      fileSizeBytes: config.fileSizeBytes,
      filesPerSecond: filesPerWriter,
      durationMs,
      writeMetrics,
      workflowMetrics,
      stopSignal,
    })
  }

  // Start health polling
  const healthPollInterval = setInterval(async () => {
    const metric = await pollHealth(config.endpoint)
    healthMetrics.push(metric)

    // Progress update
    const elapsed = Date.now() - startTime
    const progress = Math.min(100, (elapsed / durationMs) * 100)
    const totalWrites = writeMetrics.length
    const successWrites = writeMetrics.filter(m => m.success).length
    process.stdout.write(
      `\rProgress: ${progress.toFixed(1)}% | Writes: ${successWrites}/${totalWrites} | ` +
      `Windows: ${metric.totalActiveWindows} | Pending: ${metric.totalPendingFiles} | ` +
      `Status: ${metric.status}   `
    )
  }, config.pollIntervalMs)

  // Initial health check
  const initialHealth = await pollHealth(config.endpoint)
  healthMetrics.push(initialHealth)

  console.log('\nStarting load test...')
  console.log(`Initial health: ${initialHealth.status}`)

  // Start all writers
  const writerPromises = writerContexts.map(ctx => writerTask(ctx))

  // Wait for duration or all writers to complete
  const timeoutPromise = new Promise<void>(resolve => {
    setTimeout(() => {
      stopSignal.stop = true
      resolve()
    }, durationMs)
  })

  await Promise.race([
    Promise.all(writerPromises),
    timeoutPromise,
  ])

  // Stop health polling
  clearInterval(healthPollInterval)
  stopSignal.stop = true

  // Final health check
  const finalHealth = await pollHealth(config.endpoint)
  healthMetrics.push(finalHealth)

  const endTime = Date.now()

  console.log('\n\nLoad test completed.')
  console.log(`Final health: ${finalHealth.status}`)

  // Calculate summary
  const successWrites = writeMetrics.filter(m => m.success)
  const latencies = successWrites.map(m => m.latencyMs)

  const summary: LoadTestSummary = {
    totalFilesWritten: writeMetrics.length,
    totalBytesWritten: writeMetrics.reduce((sum, m) => sum + m.sizeBytes, 0),
    totalFilesSucceeded: successWrites.length,
    totalFilesFailed: writeMetrics.length - successWrites.length,
    writeLatencyP50: percentile(latencies, 50),
    writeLatencyP95: percentile(latencies, 95),
    writeLatencyP99: percentile(latencies, 99),
    writeLatencyMax: Math.max(0, ...latencies),
    actualFilesPerSecond: successWrites.length / (config.durationSec || 1),
    healthChecks: healthMetrics.length,
    healthStatusCounts: healthMetrics.reduce((counts, m) => {
      counts[m.status] = (counts[m.status] || 0) + 1
      return counts
    }, {} as Record<string, number>),
    maxActiveWindows: Math.max(0, ...healthMetrics.map(m => m.totalActiveWindows)),
    maxPendingFiles: Math.max(0, ...healthMetrics.map(m => m.totalPendingFiles)),
    maxOldestWindowAgeMs: Math.max(0, ...healthMetrics.map(m => m.oldestWindowAgeMs)),
    workflowsCreated: workflowMetrics.filter(m => m.status === 'created').length,
    workflowsFailed: workflowMetrics.filter(m => m.status === 'failed').length,
  }

  return {
    config,
    startTime,
    endTime,
    durationMs: endTime - startTime,
    healthMetrics,
    writeMetrics,
    workflowMetrics,
    summary,
  }
}

// =============================================================================
// Output Formatting
// =============================================================================

function printTableResults(results: LoadTestResults): void {
  const { summary, config } = results

  console.log('\n' + '='.repeat(70))
  console.log('LOAD TEST RESULTS')
  console.log('='.repeat(70))

  console.log('\nConfiguration:')
  console.log(`  Scenario: ${config.scenario}`)
  console.log(`  Duration: ${formatDuration(results.durationMs)}`)
  console.log(`  Namespaces: ${config.numNamespaces}`)
  console.log(`  Writers: ${config.numWriters}`)
  console.log(`  Target files/sec: ${config.filesPerSecond}`)
  console.log(`  File size: ${formatBytes(config.fileSizeBytes)}`)

  console.log('\nWrite Performance:')
  console.log(`  Total files: ${summary.totalFilesWritten}`)
  console.log(`  Succeeded: ${summary.totalFilesSucceeded}`)
  console.log(`  Failed: ${summary.totalFilesFailed}`)
  console.log(`  Total data: ${formatBytes(summary.totalBytesWritten)}`)
  console.log(`  Actual files/sec: ${summary.actualFilesPerSecond.toFixed(1)}`)
  console.log(`  Latency P50: ${summary.writeLatencyP50.toFixed(1)}ms`)
  console.log(`  Latency P95: ${summary.writeLatencyP95.toFixed(1)}ms`)
  console.log(`  Latency P99: ${summary.writeLatencyP99.toFixed(1)}ms`)
  console.log(`  Latency Max: ${summary.writeLatencyMax.toFixed(1)}ms`)

  console.log('\nCompaction Health:')
  console.log(`  Health checks: ${summary.healthChecks}`)
  console.log(`  Status distribution: ${JSON.stringify(summary.healthStatusCounts)}`)
  console.log(`  Max active windows: ${summary.maxActiveWindows}`)
  console.log(`  Max pending files: ${summary.maxPendingFiles}`)
  console.log(`  Max window age: ${formatDuration(summary.maxOldestWindowAgeMs)}`)

  console.log('\nWorkflows:')
  console.log(`  Created: ${summary.workflowsCreated}`)
  console.log(`  Failed: ${summary.workflowsFailed}`)

  // Print alerts if any
  const alerts = results.healthMetrics.flatMap(m => m.alerts).filter((v, i, a) => a.indexOf(v) === i)
  if (alerts.length > 0) {
    console.log('\nAlerts:')
    for (const alert of alerts) {
      console.log(`  - ${alert}`)
    }
  }

  console.log('\n' + '='.repeat(70))
}

function generateCsvOutput(results: LoadTestResults): string {
  const lines: string[] = []

  // Health metrics CSV
  lines.push('# Health Metrics')
  lines.push('timestamp,status,activeWindows,oldestWindowAgeMs,pendingFiles,stuckWindows,namespaces')
  for (const m of results.healthMetrics) {
    lines.push([
      m.timestamp,
      m.status,
      m.totalActiveWindows,
      m.oldestWindowAgeMs,
      m.totalPendingFiles,
      m.windowsStuckInProcessing,
      m.namespaceCount,
    ].join(','))
  }

  lines.push('')
  lines.push('# Write Metrics Sample (first 1000)')
  lines.push('timestamp,namespace,writerId,fileKey,sizeBytes,latencyMs,success')
  for (const m of results.writeMetrics.slice(0, 1000)) {
    lines.push([
      m.timestamp,
      m.namespace,
      m.writerId,
      m.fileKey,
      m.sizeBytes,
      m.latencyMs.toFixed(2),
      m.success,
    ].join(','))
  }

  lines.push('')
  lines.push('# Summary')
  lines.push('metric,value')
  const summary = results.summary
  lines.push(`totalFilesWritten,${summary.totalFilesWritten}`)
  lines.push(`totalBytesWritten,${summary.totalBytesWritten}`)
  lines.push(`totalFilesSucceeded,${summary.totalFilesSucceeded}`)
  lines.push(`totalFilesFailed,${summary.totalFilesFailed}`)
  lines.push(`writeLatencyP50,${summary.writeLatencyP50.toFixed(2)}`)
  lines.push(`writeLatencyP95,${summary.writeLatencyP95.toFixed(2)}`)
  lines.push(`writeLatencyP99,${summary.writeLatencyP99.toFixed(2)}`)
  lines.push(`writeLatencyMax,${summary.writeLatencyMax.toFixed(2)}`)
  lines.push(`actualFilesPerSecond,${summary.actualFilesPerSecond.toFixed(2)}`)
  lines.push(`maxActiveWindows,${summary.maxActiveWindows}`)
  lines.push(`maxPendingFiles,${summary.maxPendingFiles}`)
  lines.push(`maxOldestWindowAgeMs,${summary.maxOldestWindowAgeMs}`)
  lines.push(`workflowsCreated,${summary.workflowsCreated}`)
  lines.push(`workflowsFailed,${summary.workflowsFailed}`)

  return lines.join('\n')
}

function generateJsonOutput(results: LoadTestResults): string {
  // Reduce write metrics to a sample for JSON output (full data can be huge)
  const output = {
    ...results,
    writeMetrics: results.writeMetrics.slice(0, 1000),
    writeMetricsTotal: results.writeMetrics.length,
    writeMetricsSampled: results.writeMetrics.length > 1000,
  }
  return JSON.stringify(output, null, 2)
}

async function outputResults(results: LoadTestResults, config: LoadTestConfig): Promise<void> {
  let output: string

  switch (config.outputFormat) {
    case 'json':
      output = generateJsonOutput(results)
      break
    case 'csv':
      output = generateCsvOutput(results)
      break
    case 'table':
    default:
      printTableResults(results)
      // For table format with output file, also write JSON
      if (config.outputFile) {
        output = generateJsonOutput(results)
      } else {
        return
      }
      break
  }

  if (config.outputFile) {
    const fs = await import('fs/promises')
    await fs.writeFile(config.outputFile, output, 'utf-8')
    console.log(`\nResults written to: ${config.outputFile}`)
  } else {
    console.log(output)
  }
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  const config = parseArgs()

  if (config.dryRun) {
    console.log('Configuration (dry run):')
    console.log(JSON.stringify(config, null, 2))
    return
  }

  try {
    const results = await runLoadTest(config)
    await outputResults(results, config)
  } catch (err) {
    console.error('\nLoad test failed:', err instanceof Error ? err.message : String(err))
    process.exit(1)
  }
}

main().catch(err => {
  console.error('Fatal error:', err)
  process.exit(1)
})
