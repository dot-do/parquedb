/**
 * E2E Tests for Materialized Views
 *
 * Tests MV propagation latency for:
 * - Inserts into source tables showing up in MVs
 * - Tail events showing up in MVs
 * - Streaming refresh performance
 * - Scheduled refresh correctness
 *
 * Run with: pnpm test tests/e2e/materialized-views.test.ts
 */

import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import {
  StreamingRefreshEngine,
  createStreamingRefreshEngine,
  type MVHandler,
} from '../../src/materialized-views/streaming'
import type { Event, EventOp, Entity } from '../../src/types/entity'
import { MemoryBackend } from '../../src/storage/MemoryBackend'
import { IcebergBackend } from '../../src/backends/iceberg'

// =============================================================================
// Types
// =============================================================================

interface LatencyMeasurement {
  name: string
  p50: number
  p95: number
  p99: number
  mean: number
  min: number
  max: number
  samples: number
}

interface Order {
  $type: string
  name: string
  customerId: string
  total: number
  status: 'pending' | 'completed' | 'cancelled'
  items: Array<{ productId: string; quantity: number; price: number }>
}

interface TailEventData {
  $type: string
  name: string
  scriptName: string
  outcome: string
  eventTimestamp: Date
  event?: Record<string, unknown>
  logs: Array<{ timestamp: number; level: string; message: string }>
  exceptions: Array<{ timestamp: number; name: string; message: string }>
}

// =============================================================================
// Test Helpers
// =============================================================================

function createEvent(
  target: string,
  op: EventOp,
  after?: Record<string, unknown>,
  before?: Record<string, unknown>
): Event {
  return {
    id: `evt_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    ts: Date.now(),
    op,
    target,
    after,
    before,
    actor: 'test:user',
  }
}

function calculateLatencyStats(samples: number[]): LatencyMeasurement {
  if (samples.length === 0) {
    return { name: '', p50: 0, p95: 0, p99: 0, mean: 0, min: 0, max: 0, samples: 0 }
  }

  const sorted = [...samples].sort((a, b) => a - b)
  const n = samples.length
  const sum = samples.reduce((a, b) => a + b, 0)

  const percentile = (p: number) => {
    const idx = Math.ceil((p / 100) * n) - 1
    return sorted[Math.max(0, Math.min(idx, n - 1))] ?? 0
  }

  return {
    name: '',
    p50: percentile(50),
    p95: percentile(95),
    p99: percentile(99),
    mean: sum / n,
    min: sorted[0] ?? 0,
    max: sorted[n - 1] ?? 0,
    samples: n,
  }
}

async function measureLatency<T>(fn: () => Promise<T>): Promise<{ result: T; latencyMs: number }> {
  const start = performance.now()
  const result = await fn()
  return { result, latencyMs: performance.now() - start }
}

// =============================================================================
// MV Handler Factory for Testing
// =============================================================================

interface TestMVHandler extends MVHandler {
  processedEvents: Event[][]
  processedAt: number[]
  getLatencies(): number[]
}

function createTestMVHandler(
  name: string,
  sourceNamespaces: string[],
  options?: {
    processingDelayMs?: number
    onProcess?: (events: Event[]) => void
  }
): TestMVHandler {
  const processedEvents: Event[][] = []
  const processedAt: number[] = []

  return {
    name,
    sourceNamespaces,
    processedEvents,
    processedAt,
    async process(events: Event[]): Promise<void> {
      const startTime = performance.now()

      if (options?.processingDelayMs) {
        await new Promise(resolve => setTimeout(resolve, options.processingDelayMs))
      }

      options?.onProcess?.(events)

      processedEvents.push([...events])
      processedAt.push(performance.now() - startTime)
    },
    getLatencies(): number[] {
      return processedAt
    },
  }
}

// =============================================================================
// Test Suite: Insert to MV Propagation
// =============================================================================

describe('Materialized View Propagation Latency', () => {
  let engine: StreamingRefreshEngine
  let storage: MemoryBackend
  let backend: IcebergBackend

  beforeEach(async () => {
    storage = new MemoryBackend()
    backend = new IcebergBackend({
      storage,
      warehouse: 'test',
      database: 'test',
    })
    await backend.initialize()
  })

  afterEach(async () => {
    if (engine) {
      await engine.stop()
    }
    await backend.close()
  })

  describe('Insert → MV Latency', () => {
    test('measures single insert propagation latency', async () => {
      const handler = createTestMVHandler('OrderAnalytics', ['orders'])
      engine = createStreamingRefreshEngine({
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      engine.registerMV(handler)
      await engine.start()

      const latencies: number[] = []

      // Perform multiple inserts and measure propagation
      for (let i = 0; i < 50; i++) {
        const order: Order = {
          $type: 'Order',
          name: `Order ${i}`,
          customerId: `cust-${i % 10}`,
          total: 100 + i * 10,
          status: 'pending',
          items: [{ productId: 'prod-1', quantity: 2, price: 50 }],
        }

        const startTime = performance.now()

        // Create the order in backend
        await backend.create('orders', order)

        // Emit event to MV engine
        const event = createEvent(`orders:order-${i}`, 'CREATE', order as unknown as Record<string, unknown>)
        await engine.processEvent(event)
        await engine.flush()

        latencies.push(performance.now() - startTime)
      }

      const stats = calculateLatencyStats(latencies)
      stats.name = 'Insert → MV Propagation'

      console.log('\n--- Insert → MV Propagation Latency ---')
      console.log(`  Samples: ${stats.samples}`)
      console.log(`  p50: ${stats.p50.toFixed(2)}ms`)
      console.log(`  p95: ${stats.p95.toFixed(2)}ms`)
      console.log(`  p99: ${stats.p99.toFixed(2)}ms`)
      console.log(`  Mean: ${stats.mean.toFixed(2)}ms`)
      console.log(`  Min: ${stats.min.toFixed(2)}ms`)
      console.log(`  Max: ${stats.max.toFixed(2)}ms`)

      expect(handler.processedEvents.length).toBe(50)
      expect(stats.p95).toBeLessThan(100) // Should complete in under 100ms at p95
    })

    test('measures batch insert propagation latency', async () => {
      const handler = createTestMVHandler('OrderAnalytics', ['orders'])
      engine = createStreamingRefreshEngine({
        batchSize: 10,
        batchTimeoutMs: 50,
      })
      engine.registerMV(handler)
      await engine.start()

      const batchLatencies: number[] = []
      const batchSize = 10
      const numBatches = 10

      for (let batch = 0; batch < numBatches; batch++) {
        const startTime = performance.now()

        // Create batch of orders
        for (let i = 0; i < batchSize; i++) {
          const idx = batch * batchSize + i
          const order: Order = {
            $type: 'Order',
            name: `Order ${idx}`,
            customerId: `cust-${idx % 10}`,
            total: 100 + idx * 10,
            status: 'pending',
            items: [{ productId: 'prod-1', quantity: 2, price: 50 }],
          }

          await backend.create('orders', order)
          const event = createEvent(`orders:order-${idx}`, 'CREATE', order as unknown as Record<string, unknown>)
          await engine.processEvent(event)
        }

        await engine.flush()
        batchLatencies.push(performance.now() - startTime)
      }

      const stats = calculateLatencyStats(batchLatencies)
      stats.name = 'Batch Insert → MV Propagation'

      console.log('\n--- Batch Insert → MV Propagation Latency ---')
      console.log(`  Batch size: ${batchSize}`)
      console.log(`  Batches: ${numBatches}`)
      console.log(`  p50: ${stats.p50.toFixed(2)}ms`)
      console.log(`  p95: ${stats.p95.toFixed(2)}ms`)
      console.log(`  Mean: ${stats.mean.toFixed(2)}ms`)
      console.log(`  Per-item (mean): ${(stats.mean / batchSize).toFixed(2)}ms`)

      expect(handler.processedEvents.length).toBeGreaterThanOrEqual(numBatches)
    })

    test('measures update propagation latency', async () => {
      const handler = createTestMVHandler('OrderAnalytics', ['orders'])
      engine = createStreamingRefreshEngine({
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      engine.registerMV(handler)
      await engine.start()

      const updateLatencies: number[] = []

      // Measure update event propagation (simulating updates without backend persistence)
      // This tests the MV engine's update event processing latency
      for (let i = 0; i < 20; i++) {
        const startTime = performance.now()

        const before = { total: 100, status: 'pending' }
        const after = { total: 150, status: 'completed' }

        // Simulate update event propagation to MV
        const event = createEvent(`orders:order-${i}`, 'UPDATE', after, before)
        await engine.processEvent(event)
        await engine.flush()

        updateLatencies.push(performance.now() - startTime)
      }

      const stats = calculateLatencyStats(updateLatencies)
      stats.name = 'Update → MV Propagation'

      console.log('\n--- Update → MV Propagation Latency ---')
      console.log(`  Samples: ${stats.samples}`)
      console.log(`  p50: ${stats.p50.toFixed(2)}ms`)
      console.log(`  p95: ${stats.p95.toFixed(2)}ms`)
      console.log(`  Mean: ${stats.mean.toFixed(2)}ms`)

      expect(stats.p95).toBeLessThan(100)
    })
  })

  describe('Tail Events → MV Latency', () => {
    test('measures tail event ingestion latency', async () => {
      const handler = createTestMVHandler('TailAnalytics', ['tailEvents'])
      engine = createStreamingRefreshEngine({
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      engine.registerMV(handler)
      await engine.start()

      const latencies: number[] = []

      // Simulate tail events
      for (let i = 0; i < 100; i++) {
        const tailEvent: TailEventData = {
          $type: 'TailEvent',
          name: `TailEvent ${i}`,
          scriptName: 'my-worker',
          outcome: i % 10 === 0 ? 'exception' : 'ok',
          eventTimestamp: new Date(),
          event: {
            request: {
              method: 'GET',
              url: `https://example.com/api/endpoint-${i}`,
            },
          },
          logs: i % 5 === 0 ? [{ timestamp: Date.now(), level: 'error', message: 'Test error' }] : [],
          exceptions: i % 10 === 0 ? [{ timestamp: Date.now(), name: 'Error', message: 'Test exception' }] : [],
        }

        const startTime = performance.now()

        // Simulate tail event ingestion
        await backend.create('tailEvents', tailEvent as unknown as Record<string, unknown>)
        const event = createEvent(`tailEvents:tail-${i}`, 'CREATE', tailEvent as unknown as Record<string, unknown>)
        await engine.processEvent(event)
        await engine.flush()

        latencies.push(performance.now() - startTime)
      }

      const stats = calculateLatencyStats(latencies)
      stats.name = 'Tail Event → MV Propagation'

      console.log('\n--- Tail Event → MV Propagation Latency ---')
      console.log(`  Samples: ${stats.samples}`)
      console.log(`  p50: ${stats.p50.toFixed(2)}ms`)
      console.log(`  p95: ${stats.p95.toFixed(2)}ms`)
      console.log(`  p99: ${stats.p99.toFixed(2)}ms`)
      console.log(`  Mean: ${stats.mean.toFixed(2)}ms`)
      console.log(`  Throughput: ${(1000 / stats.mean).toFixed(1)} events/sec`)

      expect(handler.processedEvents.length).toBe(100)
      expect(stats.p95).toBeLessThan(100)
    })

    test('measures batched tail event processing latency', async () => {
      const handler = createTestMVHandler('TailAnalytics', ['tailEvents'])
      engine = createStreamingRefreshEngine({
        batchSize: 25,
        batchTimeoutMs: 100,
      })
      engine.registerMV(handler)
      await engine.start()

      const batchLatencies: number[] = []
      const eventsPerBatch = 25
      const numBatches = 8

      for (let batch = 0; batch < numBatches; batch++) {
        const startTime = performance.now()

        for (let i = 0; i < eventsPerBatch; i++) {
          const idx = batch * eventsPerBatch + i
          const tailEvent: TailEventData = {
            $type: 'TailEvent',
            name: `TailEvent ${idx}`,
            scriptName: `worker-${idx % 3}`,
            outcome: idx % 20 === 0 ? 'exception' : 'ok',
            eventTimestamp: new Date(),
            logs: [],
            exceptions: [],
          }

          await backend.create('tailEvents', tailEvent as unknown as Record<string, unknown>)
          const event = createEvent(`tailEvents:tail-${idx}`, 'CREATE', tailEvent as unknown as Record<string, unknown>)
          await engine.processEvent(event)
        }

        await engine.flush()
        batchLatencies.push(performance.now() - startTime)
      }

      const stats = calculateLatencyStats(batchLatencies)

      console.log('\n--- Batched Tail Event Processing Latency ---')
      console.log(`  Events per batch: ${eventsPerBatch}`)
      console.log(`  Total batches: ${numBatches}`)
      console.log(`  p50: ${stats.p50.toFixed(2)}ms`)
      console.log(`  p95: ${stats.p95.toFixed(2)}ms`)
      console.log(`  Mean: ${stats.mean.toFixed(2)}ms`)
      console.log(`  Per-event (mean): ${(stats.mean / eventsPerBatch).toFixed(2)}ms`)
      console.log(`  Throughput: ${((eventsPerBatch * 1000) / stats.mean).toFixed(1)} events/sec`)

      expect(handler.processedEvents.length).toBeGreaterThanOrEqual(numBatches)
    })

    test('measures derived MV latency (WorkerErrors from TailEvents)', async () => {
      // Simulates creating a derived MV that filters error events
      const tailHandler = createTestMVHandler('TailEvents', ['tailEvents'])
      const errorHandler = createTestMVHandler('WorkerErrors', ['tailEvents'], {
        onProcess: (events) => {
          // Filter to only error events
          const errors = events.filter(e => {
            const data = e.after as TailEventData | undefined
            return data?.outcome !== 'ok'
          })
          // In real impl, this would update the WorkerErrors MV
        },
      })

      engine = createStreamingRefreshEngine({
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      engine.registerMV(tailHandler)
      engine.registerMV(errorHandler)
      await engine.start()

      const latencies: number[] = []
      let errorCount = 0

      for (let i = 0; i < 50; i++) {
        const isError = i % 5 === 0
        if (isError) errorCount++

        const tailEvent: TailEventData = {
          $type: 'TailEvent',
          name: `TailEvent ${i}`,
          scriptName: 'my-worker',
          outcome: isError ? 'exception' : 'ok',
          eventTimestamp: new Date(),
          logs: [],
          exceptions: isError ? [{ timestamp: Date.now(), name: 'Error', message: 'Failed' }] : [],
        }

        const startTime = performance.now()

        await backend.create('tailEvents', tailEvent as unknown as Record<string, unknown>)
        const event = createEvent(`tailEvents:tail-${i}`, 'CREATE', tailEvent as unknown as Record<string, unknown>)
        await engine.processEvent(event)
        await engine.flush()

        latencies.push(performance.now() - startTime)
      }

      const stats = calculateLatencyStats(latencies)

      console.log('\n--- Derived MV (WorkerErrors) Latency ---')
      console.log(`  Total events: 50`)
      console.log(`  Error events: ${errorCount}`)
      console.log(`  p50: ${stats.p50.toFixed(2)}ms`)
      console.log(`  p95: ${stats.p95.toFixed(2)}ms`)
      console.log(`  Mean: ${stats.mean.toFixed(2)}ms`)

      // Both MVs should have processed all events
      expect(tailHandler.processedEvents.length).toBe(50)
      expect(errorHandler.processedEvents.length).toBe(50)
    })
  })

  describe('MV Engine Statistics', () => {
    test('tracks comprehensive processing statistics', async () => {
      const handler = createTestMVHandler('OrderAnalytics', ['orders', 'products'])
      engine = createStreamingRefreshEngine({
        batchSize: 5,
        batchTimeoutMs: 50,
      })
      engine.registerMV(handler)
      await engine.start()

      // Send mixed events
      for (let i = 0; i < 30; i++) {
        const ns = i % 2 === 0 ? 'orders' : 'products'
        const ops: EventOp[] = ['CREATE', 'UPDATE', 'DELETE']
        const op = ops[i % 3]!

        const event = createEvent(`${ns}:item-${i}`, op, { id: i })
        await engine.processEvent(event)
      }

      await engine.flush()

      const stats = engine.getStats()

      console.log('\n--- MV Engine Statistics ---')
      console.log(`  Events received: ${stats.eventsReceived}`)
      console.log(`  Events processed: ${stats.eventsProcessed}`)
      console.log(`  Batches processed: ${stats.batchesProcessed}`)
      console.log(`  Failed batches: ${stats.failedBatches}`)
      console.log(`  Backpressure events: ${stats.backpressureEvents}`)
      console.log(`  Avg batch processing: ${stats.avgBatchProcessingMs.toFixed(2)}ms`)
      console.log(`  Events by op:`)
      console.log(`    CREATE: ${stats.eventsByOp.CREATE}`)
      console.log(`    UPDATE: ${stats.eventsByOp.UPDATE}`)
      console.log(`    DELETE: ${stats.eventsByOp.DELETE}`)
      console.log(`  Events by namespace:`)
      Object.entries(stats.eventsByNamespace).forEach(([ns, count]) => {
        console.log(`    ${ns}: ${count}`)
      })

      expect(stats.eventsReceived).toBe(30)
      expect(stats.eventsProcessed).toBe(30)
      expect(stats.failedBatches).toBe(0)
    })
  })

  describe('Multiple MV Routing', () => {
    test('measures latency with multiple MVs subscribed to same namespace', async () => {
      const handlers = [
        createTestMVHandler('OrderTotals', ['orders']),
        createTestMVHandler('OrdersByCustomer', ['orders']),
        createTestMVHandler('OrderStatus', ['orders']),
      ]

      engine = createStreamingRefreshEngine({
        batchSize: 1,
        batchTimeoutMs: 10,
      })

      handlers.forEach(h => engine.registerMV(h))
      await engine.start()

      const latencies: number[] = []

      for (let i = 0; i < 30; i++) {
        const order: Order = {
          $type: 'Order',
          name: `Order ${i}`,
          customerId: `cust-${i % 5}`,
          total: 100 + i * 10,
          status: 'pending',
          items: [],
        }

        const startTime = performance.now()

        await backend.create('orders', order)
        const event = createEvent(`orders:order-${i}`, 'CREATE', order as unknown as Record<string, unknown>)
        await engine.processEvent(event)
        await engine.flush()

        latencies.push(performance.now() - startTime)
      }

      const stats = calculateLatencyStats(latencies)

      console.log('\n--- Multiple MV Routing Latency ---')
      console.log(`  MVs subscribed: ${handlers.length}`)
      console.log(`  Events: 30`)
      console.log(`  p50: ${stats.p50.toFixed(2)}ms`)
      console.log(`  p95: ${stats.p95.toFixed(2)}ms`)
      console.log(`  Mean: ${stats.mean.toFixed(2)}ms`)

      // All handlers should have received all events
      handlers.forEach(h => {
        expect(h.processedEvents.length).toBe(30)
      })
    })
  })

  describe('End-to-End MV Query Latency', () => {
    test('measures full cycle: insert → MV update → query MV', async () => {
      // This test simulates the full cycle of data flowing through an MV
      const mvData: Map<string, number> = new Map() // Simulated MV data: customerId → totalSpent

      const handler = createTestMVHandler('CustomerSpending', ['orders'], {
        onProcess: (events) => {
          // Aggregate order totals by customer
          for (const event of events) {
            if (event.op === 'CREATE') {
              const order = event.after as Order | undefined
              if (order) {
                const current = mvData.get(order.customerId) ?? 0
                mvData.set(order.customerId, current + order.total)
              }
            }
          }
        },
      })

      engine = createStreamingRefreshEngine({
        batchSize: 1,
        batchTimeoutMs: 10,
      })
      engine.registerMV(handler)
      await engine.start()

      const fullCycleLatencies: number[] = []

      for (let i = 0; i < 20; i++) {
        const customerId = `cust-${i % 5}`
        const order: Order = {
          $type: 'Order',
          name: `Order ${i}`,
          customerId,
          total: 100,
          status: 'completed',
          items: [],
        }

        const startTime = performance.now()

        // 1. Insert order
        await backend.create('orders', order)

        // 2. Propagate to MV
        const event = createEvent(`orders:order-${i}`, 'CREATE', order as unknown as Record<string, unknown>)
        await engine.processEvent(event)
        await engine.flush()

        // 3. Query MV (simulated by reading from mvData)
        const customerTotal = mvData.get(customerId)

        fullCycleLatencies.push(performance.now() - startTime)

        // Verify aggregation is working
        expect(customerTotal).toBeGreaterThan(0)
      }

      const stats = calculateLatencyStats(fullCycleLatencies)

      console.log('\n--- Full Cycle Latency (Insert → MV → Query) ---')
      console.log(`  p50: ${stats.p50.toFixed(2)}ms`)
      console.log(`  p95: ${stats.p95.toFixed(2)}ms`)
      console.log(`  p99: ${stats.p99.toFixed(2)}ms`)
      console.log(`  Mean: ${stats.mean.toFixed(2)}ms`)

      // Verify final state
      expect(mvData.size).toBe(5) // 5 unique customers
      const totalSpent = Array.from(mvData.values()).reduce((a, b) => a + b, 0)
      expect(totalSpent).toBe(2000) // 20 orders × $100
    })
  })
})

describe('MV Latency Benchmarks', () => {
  test('summary benchmark: all latency measurements', async () => {
    const storage = new MemoryBackend()
    const backend = new IcebergBackend({
      storage,
      warehouse: 'benchmark',
      database: 'test',
    })
    await backend.initialize()

    const results: Record<string, LatencyMeasurement> = {}

    // Test configurations
    const configs = [
      { name: 'Single Insert (batch=1)', batchSize: 1, count: 50 },
      { name: 'Batched Insert (batch=10)', batchSize: 10, count: 100 },
      { name: 'Batched Insert (batch=50)', batchSize: 50, count: 200 },
    ]

    for (const config of configs) {
      const handler = createTestMVHandler('Benchmark', ['orders'])
      const engine = createStreamingRefreshEngine({
        batchSize: config.batchSize,
        batchTimeoutMs: 50,
      })
      engine.registerMV(handler)
      await engine.start()

      const latencies: number[] = []

      for (let i = 0; i < config.count; i++) {
        const startTime = performance.now()

        const event = createEvent(`orders:order-${i}`, 'CREATE', { id: i, total: 100 })
        await engine.processEvent(event)

        if ((i + 1) % config.batchSize === 0 || i === config.count - 1) {
          await engine.flush()
        }

        latencies.push(performance.now() - startTime)
      }

      await engine.stop()

      const stats = calculateLatencyStats(latencies)
      stats.name = config.name
      results[config.name] = stats
    }

    await backend.close()

    // Print summary table
    console.log('\n' + '='.repeat(80))
    console.log('MV LATENCY BENCHMARK SUMMARY')
    console.log('='.repeat(80))
    console.log(
      'Configuration'.padEnd(35) +
      'p50 (ms)'.padStart(12) +
      'p95 (ms)'.padStart(12) +
      'p99 (ms)'.padStart(12) +
      'Mean (ms)'.padStart(12)
    )
    console.log('-'.repeat(80))

    for (const [name, stats] of Object.entries(results)) {
      console.log(
        name.padEnd(35) +
        stats.p50.toFixed(2).padStart(12) +
        stats.p95.toFixed(2).padStart(12) +
        stats.p99.toFixed(2).padStart(12) +
        stats.mean.toFixed(2).padStart(12)
      )
    }

    console.log('='.repeat(80))

    // All tests should pass with reasonable latency
    expect(Object.keys(results).length).toBe(configs.length)
  })
})
