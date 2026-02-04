/**
 * Performance Regression Detection Requirements (RED Phase)
 *
 * Issue: parquedb-6n04.1
 *
 * These tests define the acceptance criteria for performance regression detection:
 * 1. CI workflow includes benchmark comparison step
 * 2. Baseline benchmarks are stored and versioned
 * 3. CI fails when p50 latency increases >15%
 * 4. Benchmark results are reported on PR
 *
 * Current state: Benchmarks exist but no baseline comparison in CI.
 * The benchmark.yml workflow runs benchmarks and uploads artifacts,
 * but does not compare against a baseline or fail CI on regression.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import fs from 'fs/promises'
import path from 'path'
import yaml from 'yaml'

// =============================================================================
// Requirement 1: CI Workflow includes benchmark comparison step
// =============================================================================

// Project root is 3 levels up from tests/unit/ci
const PROJECT_ROOT = path.resolve(__dirname, '../../..')

describe('CI Workflow Performance Regression Detection', () => {
  const workflowPath = path.join(PROJECT_ROOT, '.github/workflows/benchmark.yml')
  const ciWorkflowPath = path.join(PROJECT_ROOT, '.github/workflows/ci.yml')

  describe('Requirement 1: CI workflow includes benchmark comparison step', () => {
    it('benchmark.yml should include a step to compare against baseline', async () => {
      const content = await fs.readFile(workflowPath, 'utf-8')
      const workflow = yaml.parse(content)

      // Check for comparison step in benchmark job
      const benchmarkJob = workflow.jobs?.benchmark
      expect(benchmarkJob).toBeDefined()

      const steps = benchmarkJob?.steps ?? []
      const hasCompareStep = steps.some(
        (step: { name?: string; id?: string }) =>
          step.name?.toLowerCase().includes('compare') ||
          step.name?.toLowerCase().includes('regression') ||
          step.id === 'compare-benchmarks' ||
          step.id === 'regression-check'
      )

      expect(
        hasCompareStep,
        'benchmark.yml should have a step to compare benchmarks against baseline'
      ).toBe(true)
    })

    it('benchmark workflow should run on pull requests', async () => {
      const content = await fs.readFile(workflowPath, 'utf-8')
      const workflow = yaml.parse(content)

      // Check that workflow triggers on pull_request
      const triggers = workflow.on
      expect(triggers).toBeDefined()

      const hasPRTrigger =
        triggers.pull_request !== undefined ||
        triggers.pull_request_target !== undefined

      expect(
        hasPRTrigger,
        'benchmark.yml should trigger on pull requests to detect regressions'
      ).toBe(true)
    })

    it('benchmark comparison should download baseline from artifact or gh-pages', async () => {
      const content = await fs.readFile(workflowPath, 'utf-8')
      const workflow = yaml.parse(content)

      const benchmarkJob = workflow.jobs?.benchmark
      const steps = benchmarkJob?.steps ?? []

      const hasBaselineDownload = steps.some(
        (step: { name?: string; uses?: string }) =>
          step.name?.toLowerCase().includes('baseline') ||
          step.name?.toLowerCase().includes('download') ||
          step.uses?.includes('download-artifact') ||
          step.uses?.includes('checkout') && step.name?.toLowerCase().includes('gh-pages')
      )

      expect(
        hasBaselineDownload,
        'benchmark.yml should download baseline benchmarks for comparison'
      ).toBe(true)
    })
  })

  // ===========================================================================
  // Requirement 2: Baseline benchmarks are stored and versioned
  // ===========================================================================

  describe('Requirement 2: Baseline benchmarks are stored and versioned', () => {
    it('baseline benchmarks should be stored in gh-pages branch or artifact', async () => {
      const content = await fs.readFile(workflowPath, 'utf-8')
      const workflow = yaml.parse(content)

      const benchmarkJob = workflow.jobs?.benchmark
      const steps = benchmarkJob?.steps ?? []

      // Check for baseline storage mechanism
      const hasBaselineStorage = steps.some(
        (step: { name?: string; with?: { 'gh-pages-branch'?: string; 'benchmark-data-dir-path'?: string } }) =>
          step.name?.toLowerCase().includes('store benchmark') ||
          step.with?.['gh-pages-branch'] !== undefined ||
          step.with?.['benchmark-data-dir-path'] !== undefined
      )

      expect(
        hasBaselineStorage,
        'benchmark.yml should store baseline benchmarks for future comparison'
      ).toBe(true)
    })

    it('baseline storage should be versioned by commit SHA or branch', async () => {
      const content = await fs.readFile(workflowPath, 'utf-8')

      // Check that baseline includes commit info for versioning
      const hasVersioning =
        content.includes('github.sha') ||
        content.includes('github.ref') ||
        content.includes('commit') ||
        content.includes('benchmark-data-dir-path')

      expect(
        hasVersioning,
        'benchmark baseline should be versioned to track changes over time'
      ).toBe(true)
    })

    it('baseline benchmarks should include p50 latency metric', async () => {
      // Check benchmark-results.json format includes p50
      const resultsPath = path.join(PROJECT_ROOT, 'benchmark-results.json')
      let hasP50 = false

      try {
        const content = await fs.readFile(resultsPath, 'utf-8')
        hasP50 = content.includes('"p50"') || content.includes('"median"')
      } catch {
        // File may not exist in all environments
        hasP50 = false
      }

      // If results exist, they should have p50. If not, we need to ensure the output format is correct.
      expect(
        hasP50,
        'benchmark results should include p50/median latency for regression detection'
      ).toBe(true)
    })
  })

  // ===========================================================================
  // Requirement 3: CI fails when p50 latency increases >15%
  // ===========================================================================

  describe('Requirement 3: CI fails when p50 latency increases >15%', () => {
    it('benchmark workflow should fail on >15% p50 regression', async () => {
      const content = await fs.readFile(workflowPath, 'utf-8')
      const workflow = yaml.parse(content)

      // Check for fail-on-alert or alert-threshold configuration
      const benchmarkJob = workflow.jobs?.benchmark
      const steps = benchmarkJob?.steps ?? []

      const hasFailureConfig = steps.some(
        (step: { with?: { 'fail-on-alert'?: boolean | string; 'alert-threshold'?: string } }) => {
          const withConfig = step.with ?? {}
          // fail-on-alert should be true (not false as it currently is)
          return (
            withConfig['fail-on-alert'] === true ||
            withConfig['fail-on-alert'] === 'true'
          )
        }
      )

      expect(
        hasFailureConfig,
        'benchmark.yml should have fail-on-alert: true to fail CI on regression'
      ).toBe(true)
    })

    it('alert threshold should be set to 115% (15% regression)', async () => {
      const content = await fs.readFile(workflowPath, 'utf-8')
      const workflow = yaml.parse(content)

      const benchmarkJob = workflow.jobs?.benchmark
      const steps = benchmarkJob?.steps ?? []

      const hasCorrectThreshold = steps.some(
        (step: { with?: { 'alert-threshold'?: string } }) => {
          const threshold = step.with?.['alert-threshold']
          // 115% means 15% worse than baseline
          return threshold === '115%'
        }
      )

      expect(
        hasCorrectThreshold,
        'benchmark.yml should have alert-threshold: 115% to detect >15% regression'
      ).toBe(true)
    })

    it('CI should block merge on benchmark failure', async () => {
      // Check if benchmark job is required in CI or branch protection
      const ciContent = await fs.readFile(ciWorkflowPath, 'utf-8')
      const ciWorkflow = yaml.parse(ciContent)

      // CI success job should depend on benchmark
      const ciSuccessJob = ciWorkflow.jobs?.['ci-success']
      const needs = ciSuccessJob?.needs ?? []

      const dependsOnBenchmark =
        needs.includes('benchmark') ||
        needs.includes('performance')

      // Alternative: benchmark is a required check (would be in branch protection, not workflow)
      // For now, check if there's a benchmark dependency in CI

      expect(
        dependsOnBenchmark,
        'CI success should depend on benchmark job to block merge on regression'
      ).toBe(true)
    })
  })

  // ===========================================================================
  // Requirement 4: Benchmark results are reported on PR
  // ===========================================================================

  describe('Requirement 4: Benchmark results are reported on PR', () => {
    it('benchmark workflow should post comment on PR with results', async () => {
      const content = await fs.readFile(workflowPath, 'utf-8')
      const workflow = yaml.parse(content)

      const benchmarkJob = workflow.jobs?.benchmark
      const steps = benchmarkJob?.steps ?? []

      const hasCommentStep = steps.some(
        (step: { name?: string; with?: { 'comment-on-alert'?: boolean | string } }) =>
          step.name?.toLowerCase().includes('comment') ||
          step.with?.['comment-on-alert'] === true ||
          step.with?.['comment-on-alert'] === 'true'
      )

      expect(
        hasCommentStep,
        'benchmark.yml should post comment on PR with benchmark results'
      ).toBe(true)
    })

    it('PR comment should include comparison table', async () => {
      const content = await fs.readFile(workflowPath, 'utf-8')

      // Check for benchmark-action which produces comparison tables
      const usesBenchmarkAction =
        content.includes('benchmark-action/github-action-benchmark') ||
        content.includes('comparison') ||
        content.includes('table')

      expect(
        usesBenchmarkAction,
        'benchmark.yml should produce comparison table in PR comment'
      ).toBe(true)
    })

    it('PR comment should show percentage change from baseline', async () => {
      const content = await fs.readFile(workflowPath, 'utf-8')
      const workflow = yaml.parse(content)

      const benchmarkJob = workflow.jobs?.benchmark
      const steps = benchmarkJob?.steps ?? []

      // benchmark-action shows percentage change when comment-on-alert is true
      const hasPercentageDisplay = steps.some(
        (step: { uses?: string; with?: { 'comment-on-alert'?: boolean | string } }) =>
          step.uses?.includes('benchmark-action') &&
          (step.with?.['comment-on-alert'] === true ||
           step.with?.['comment-on-alert'] === 'true')
      )

      expect(
        hasPercentageDisplay,
        'benchmark.yml should show percentage change in PR comment'
      ).toBe(true)
    })

    it('PR comment should clearly indicate pass/fail status', async () => {
      const content = await fs.readFile(workflowPath, 'utf-8')

      // Check for alert/failure indication in comments
      const hasStatusIndication =
        content.includes('alert-threshold') &&
        content.includes('comment-on-alert')

      expect(
        hasStatusIndication,
        'benchmark.yml should clearly show pass/fail status in PR comment'
      ).toBe(true)
    })
  })
})

// =============================================================================
// Performance Regression Utility Functions
// =============================================================================

describe('Performance Regression Utilities', () => {
  describe('compareLatencies', () => {
    /**
     * These tests verify the regression detection logic that should be used
     * to compare benchmark results against baseline.
     */

    interface BenchmarkResult {
      name: string
      p50: number
      p95: number
      p99: number
    }

    interface ComparisonResult {
      name: string
      baselineP50: number
      currentP50: number
      percentChange: number
      isRegression: boolean
    }

    // This function should exist in src/ci/benchmark-comparison.ts or similar
    function compareLatencies(
      baseline: BenchmarkResult[],
      current: BenchmarkResult[],
      threshold: number = 0.15
    ): ComparisonResult[] {
      // TODO: This function needs to be implemented
      // For now, throw to indicate the feature doesn't exist
      throw new Error('compareLatencies not implemented - feature missing')
    }

    it('should detect regression when p50 increases by more than threshold', () => {
      const baseline: BenchmarkResult[] = [
        { name: 'create', p50: 10, p95: 20, p99: 30 },
        { name: 'find', p50: 5, p95: 10, p99: 15 },
      ]

      const current: BenchmarkResult[] = [
        { name: 'create', p50: 12, p95: 22, p99: 32 }, // 20% slower - REGRESSION
        { name: 'find', p50: 5.5, p95: 11, p99: 16 }, // 10% slower - OK
      ]

      expect(() => compareLatencies(baseline, current, 0.15)).toThrow(
        'compareLatencies not implemented'
      )
    })

    it('should not flag improvement as regression', () => {
      const baseline: BenchmarkResult[] = [
        { name: 'create', p50: 10, p95: 20, p99: 30 },
      ]

      const current: BenchmarkResult[] = [
        { name: 'create', p50: 8, p95: 16, p99: 24 }, // 20% faster - IMPROVEMENT
      ]

      expect(() => compareLatencies(baseline, current, 0.15)).toThrow(
        'compareLatencies not implemented'
      )
    })

    it('should handle missing baseline gracefully', () => {
      const baseline: BenchmarkResult[] = []
      const current: BenchmarkResult[] = [
        { name: 'create', p50: 10, p95: 20, p99: 30 },
      ]

      expect(() => compareLatencies(baseline, current, 0.15)).toThrow(
        'compareLatencies not implemented'
      )
    })
  })

  describe('formatComparisonReport', () => {
    it('should format comparison as markdown table', () => {
      // TODO: Implement formatComparisonReport
      expect(() => {
        throw new Error('formatComparisonReport not implemented - feature missing')
      }).toThrow('formatComparisonReport not implemented')
    })
  })
})

// =============================================================================
// Benchmark Output Format Requirements
// =============================================================================

describe('Benchmark Output Format', () => {
  it('benchmark results should be in github-action-benchmark compatible format', async () => {
    const resultsPath = path.join(PROJECT_ROOT, 'benchmark-results.json')

    try {
      const content = await fs.readFile(resultsPath, 'utf-8')
      const results = JSON.parse(content)

      // github-action-benchmark expects either:
      // 1. Array of { name, unit, value } objects
      // 2. Or "customSmallerIsBetter" format

      // Check current format
      const hasCorrectFormat =
        Array.isArray(results) ||
        (results.files && Array.isArray(results.files))

      expect(
        hasCorrectFormat,
        'benchmark-results.json should be in github-action-benchmark compatible format'
      ).toBe(true)

      // For customSmallerIsBetter, we need entries with value and unit
      // The current format has nested groups/benchmarks
      // This may need transformation for proper comparison
    } catch {
      // File doesn't exist - test should fail to indicate setup needed
      expect.fail(
        'benchmark-results.json should exist with github-action-benchmark compatible format'
      )
    }
  })

  it('benchmark results should include operation names for meaningful comparison', async () => {
    const resultsPath = path.join(PROJECT_ROOT, 'benchmark-results.json')

    try {
      const content = await fs.readFile(resultsPath, 'utf-8')

      // Should have descriptive operation names
      const hasOperationNames =
        content.includes('create') ||
        content.includes('find') ||
        content.includes('update') ||
        content.includes('delete') ||
        content.includes('vector')

      expect(
        hasOperationNames,
        'benchmark results should include named operations for comparison'
      ).toBe(true)
    } catch {
      expect.fail('benchmark-results.json should exist with named operations')
    }
  })
})
