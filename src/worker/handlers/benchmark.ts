/**
 * Benchmark Route Handlers
 *
 * Handles all /benchmark* routes for performance testing.
 */

import type { RouteHandlerContext } from '../route-registry'
import { MissingBucketError } from '../r2-errors'
import { handleBenchmarkRequest } from '../benchmark'
import { handleDatasetBenchmarkRequest } from '../benchmark-datasets'
import { handleIndexedBenchmarkRequest } from '../benchmark-indexed'
import { handleBackendsBenchmarkRequest } from '../benchmark-backends'
import { handleDatasetBackendsBenchmarkRequest } from '../benchmark-datasets-backends'

/**
 * Handle /benchmark - Basic benchmark
 */
export async function handleBenchmark(ctx: RouteHandlerContext): Promise<Response> {
  const { request, env } = ctx

  if (!env.BUCKET) {
    throw new MissingBucketError('BUCKET', 'Required for benchmark operations.')
  }

  return handleBenchmarkRequest(
    request,
    env.BUCKET as Parameters<typeof handleBenchmarkRequest>[1]
  )
}

/**
 * Handle /benchmark-datasets - Dataset benchmark
 */
export async function handleBenchmarkDatasets(ctx: RouteHandlerContext): Promise<Response> {
  const { request, env } = ctx

  if (!env.BUCKET) {
    throw new MissingBucketError('BUCKET', 'Required for dataset benchmark operations.')
  }

  return handleDatasetBenchmarkRequest(
    request,
    env.BUCKET as Parameters<typeof handleDatasetBenchmarkRequest>[1]
  )
}

/**
 * Handle /benchmark-indexed - Secondary index benchmark
 */
export async function handleBenchmarkIndexed(ctx: RouteHandlerContext): Promise<Response> {
  const { request, env } = ctx

  if (!env.BUCKET) {
    throw new MissingBucketError('BUCKET', 'Required for indexed benchmark operations.')
  }

  return handleIndexedBenchmarkRequest(
    request,
    env.BUCKET as Parameters<typeof handleIndexedBenchmarkRequest>[1]
  )
}

/**
 * Handle /benchmark/backends - Backend comparison benchmark
 */
export async function handleBenchmarkBackends(ctx: RouteHandlerContext): Promise<Response> {
  const { request, env } = ctx

  if (!env.CDN_BUCKET) {
    throw new MissingBucketError('CDN_BUCKET', 'Required for backend comparison benchmarks.')
  }

  return handleBackendsBenchmarkRequest(
    request,
    env.CDN_BUCKET as Parameters<typeof handleBackendsBenchmarkRequest>[1]
  )
}

/**
 * Handle /benchmark/datasets/backends - Dataset + backend benchmark
 */
export async function handleBenchmarkDatasetBackends(ctx: RouteHandlerContext): Promise<Response> {
  const { request, env } = ctx

  if (!env.BUCKET) {
    throw new MissingBucketError('BUCKET', 'Required for dataset backend benchmarks.')
  }

  return handleDatasetBackendsBenchmarkRequest(
    request,
    env.BUCKET as Parameters<typeof handleDatasetBackendsBenchmarkRequest>[1]
  )
}
