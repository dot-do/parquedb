# Materialized View Latency Benchmarks

Measures the propagation latency from source table changes to materialized view updates.

## Test Scenarios

### 1. Insert → MV Propagation

Measures how quickly inserts in source tables appear in derived MVs.

```
Source Table (Orders) → [Event] → Streaming Engine → MV (OrderAnalytics)
```

**Results (p50 latency):**
- Single insert: ~1.4ms
- Batch insert (10 items): ~1.5ms per item
- Full cycle (insert → MV → query): ~0.9ms

### 2. Tail Events → MV

Measures latency from Cloudflare Worker tail events to analytics MVs.

```
Worker Execution → [Tail Event] → TailEvents Collection → WorkerErrors MV
```

**Results:**
- Single event: ~1.6ms p50
- Throughput: ~627 events/sec
- Batched (25 events): ~2.3ms per event (433 events/sec)

### 3. Derived MV Latency

Measures latency through multiple MV layers (e.g., TailEvents → WorkerErrors).

**Results:**
- Filter-based derived MV: ~1.2ms p50

## Running the Tests

```bash
# Run all MV latency tests
pnpm test tests/e2e/materialized-views.test.ts

# Run with verbose output
pnpm test tests/e2e/materialized-views.test.ts -- --reporter=verbose
```

## Test Output Example

```
--- Insert → MV Propagation Latency ---
  Samples: 50
  p50: 1.40ms
  p95: 2.50ms
  p99: 23.72ms
  Mean: 2.01ms
  Min: 1.18ms
  Max: 23.72ms

--- Tail Event → MV Propagation Latency ---
  Samples: 100
  p50: 1.58ms
  p95: 2.22ms
  p99: 2.71ms
  Mean: 1.59ms
  Throughput: 627.2 events/sec

--- Full Cycle Latency (Insert → MV → Query) ---
  p50: 0.92ms
  p95: 1.13ms
  p99: 1.17ms
  Mean: 0.94ms
```

## Streaming Engine Configuration

The streaming refresh engine can be tuned for different latency/throughput tradeoffs:

```typescript
const engine = createStreamingRefreshEngine({
  // Smaller batches = lower latency, higher overhead
  batchSize: 1,        // Process events immediately
  batchTimeoutMs: 10,  // Flush partial batches quickly

  // Larger batches = higher throughput, more latency
  batchSize: 100,      // Wait for full batches
  batchTimeoutMs: 500, // Allow 500ms for batches to fill
})
```

### Configuration Recommendations

| Use Case | batchSize | batchTimeoutMs | Expected Latency |
|----------|-----------|----------------|------------------|
| Real-time dashboards | 1 | 10 | < 2ms |
| Analytics | 10 | 100 | < 20ms |
| Batch processing | 100 | 500 | < 100ms |
| High throughput | 1000 | 1000 | < 500ms |

## Latency Breakdown

### Single Event Path

```
1. Event created                    0ms
2. Event routed to MV handler      +0.1ms
3. Handler processes event         +0.3ms
4. MV state updated               +0.5ms
5. Flush completed                +0.5ms
                          Total: ~1.4ms
```

### Batched Event Path

```
1. Events buffered                  0ms
2. Batch threshold reached         +Xms (depends on batchSize)
3. Batch processed (parallel)      +2ms
4. All MV states updated           +1ms
5. Flush completed                 +1ms
                          Total: ~4ms + buffer time
```

## Monitoring MV Performance

The streaming engine provides statistics:

```typescript
const stats = engine.getStats()

console.log({
  eventsReceived: stats.eventsReceived,
  eventsProcessed: stats.eventsProcessed,
  batchesProcessed: stats.batchesProcessed,
  failedBatches: stats.failedBatches,
  backpressureEvents: stats.backpressureEvents,
  avgBatchProcessingMs: stats.avgBatchProcessingMs,
  eventsByOp: stats.eventsByOp,
  eventsByNamespace: stats.eventsByNamespace,
})
```

## Backpressure Handling

When MVs can't keep up with event volume:

```typescript
const engine = createStreamingRefreshEngine({
  maxBufferSize: 1000, // Apply backpressure at 1000 events
})

// Monitor backpressure
engine.onError((error, context) => {
  if (context?.mvName) {
    console.log(`MV ${context.mvName} experiencing backpressure`)
  }
})
```

## Best Practices

1. **Keep MV handlers fast** - Avoid I/O in handlers when possible
2. **Use appropriate batch sizes** - Balance latency vs throughput
3. **Monitor backpressure** - Scale handlers if backpressure increases
4. **Test with realistic load** - Benchmark with production-like event rates
5. **Use streaming refresh** - Prefer streaming over scheduled for low latency
