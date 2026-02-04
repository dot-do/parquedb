# Benchmark Baselines

This directory contains baseline benchmark results used for regression detection.

## Files

- **`production.json`** - Production baseline for CDN-backed storage benchmarks

## How Baselines Work

Baselines represent expected performance characteristics for benchmark patterns. When new benchmarks run, results are compared against these baselines to detect regressions.

### Regression Thresholds

| Metric | Threshold | Description |
|--------|-----------|-------------|
| P50 Latency | > 20% increase | Median latency regression |
| P95 Latency | > 25% increase | Tail latency regression |
| P99 Latency | > 30% increase | Extreme tail latency regression |
| Throughput | > 15% decrease | Operations per second regression |
| Pass Rate | > 10% decrease | Test success rate regression |

## Updating Baselines

### Automatic Updates

Baselines are automatically updated on successful main branch runs when no regression is detected. This ensures baselines stay current with legitimate performance improvements.

### Manual Updates

To manually update the baseline:

1. **Run benchmarks locally:**
   ```bash
   npx tsx tests/benchmarks/storage-benchmark-runner.ts \
     --backend=cdn \
     --datasets=blog,ecommerce \
     --iterations=10 \
     --output=json > tests/benchmarks/baseline/production.json
   ```

2. **Via CI workflow:**
   Trigger the "Performance Benchmarks" workflow with `update_baseline: true`:
   - Go to Actions > Performance Benchmarks > Run workflow
   - Check "Update baseline after successful run"
   - Click "Run workflow"

3. **From PR comment:**
   Comment `/benchmark --update-baseline` on a PR to run benchmarks and update baselines if successful.

### When to Update

Update baselines when:
- Adding new benchmark patterns
- Making intentional performance improvements
- Changing infrastructure (e.g., upgrading CF Workers plan)
- Adjusting target latencies for patterns

Do NOT update baselines to hide regressions.

## Baseline Format

```json
{
  "config": {
    "backend": "cdn",
    "datasets": ["blog", "ecommerce"],
    "iterations": 10,
    "warmup": 2,
    "timeout": 30000
  },
  "metadata": {
    "startedAt": "ISO timestamp",
    "completedAt": "ISO timestamp",
    "durationMs": 300000,
    "runnerVersion": "1.0.0"
  },
  "results": [
    {
      "backend": "cdn",
      "dataset": "blog",
      "pattern": "Get post by ID",
      "category": "point-read",
      "targetMs": 50,
      "latencyMs": {
        "min": 15,
        "max": 45,
        "mean": 25,
        "p50": 23,
        "p95": 40,
        "p99": 44,
        "stdDev": 8
      },
      "passedTarget": true,
      "successCount": 10,
      "failureCount": 0
    }
  ],
  "summary": {
    "totalPatterns": 6,
    "passedPatterns": 6,
    "failedPatterns": 0,
    "avgLatencyMs": 55,
    "p95LatencyMs": 95
  }
}
```

## CI Integration

The benchmark workflow uses baselines as follows:

1. **PR Runs:** Compare against baseline, fail if regression detected
2. **Main Branch Runs:** Compare against baseline, auto-update if no regression
3. **Scheduled Runs:** Compare against baseline, alert on regression

### Viewing Comparison Results

Comparison results are posted as:
- PR comments on pull requests
- Job summaries in GitHub Actions
- Artifacts attached to workflow runs

## Troubleshooting

### False Positives

If benchmarks are flagging regressions that aren't real:
1. Check if infrastructure changed (CF region, network conditions)
2. Run multiple benchmark iterations to reduce noise
3. Consider adjusting thresholds if needed

### Missing Baseline

If the baseline file is missing:
1. Run benchmarks to generate a new baseline
2. The workflow will skip regression detection and just output results
3. Subsequent runs will use the new baseline

### Outdated Baseline

If the baseline is outdated (e.g., patterns have changed):
1. Run the full benchmark suite
2. Review results to ensure they're reasonable
3. Update the baseline with new results
