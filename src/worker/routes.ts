/**
 * Route Definitions
 *
 * Centralized route definitions using the route registry pattern.
 * All routes are registered here and the fetch handler dispatches to them.
 */

import { createRouteRegistry, route, type RouteHandler, type RouteHandlerContext, type RouteDefinition } from './route-registry'
import {
  handleRoot,
  handleHealth,
  handleMetrics,
  handleDebugR2,
  handleDebugEntity,
  handleDebugIndexes,
  handleDebugQuery,
  handleDebugCache,
  handleDatasetsList,
  handleDatasetDetail,
  handleCollectionList,
  handleEntityDetail,
  handleRelationshipTraversal,
  handleNsRoute,
  type HandlerContext,
} from './handlers'
import {
  handleBenchmark,
  handleBenchmarkDatasets,
  handleBenchmarkIndexed,
  handleBenchmarkBackends,
  handleBenchmarkDatasetBackends,
} from './handlers/benchmark'
import { handleMigration } from './handlers/migration'
import { handleVacuumStart, handleVacuumStatus } from './handlers/vacuum'
import {
  handleCompactionStatus,
  handleCompactionHealth,
  handleCompactionDashboard,
  handleCompactionMetrics,
  handleCompactionMetricsJson,
} from './handlers/compaction'

// =============================================================================
// Adapter Functions
// =============================================================================

/**
 * Adapt a HandlerContext handler to a RouteHandlerContext handler
 */
function adaptHandler(
  handler: (ctx: HandlerContext) => Promise<Response> | Response
): RouteHandler {
  return (ctx: RouteHandlerContext) => handler(ctx)
}

/**
 * Adapt a debug handler that needs env
 */
function adaptDebugHandler(
  handler: (ctx: HandlerContext, env: RouteHandlerContext['env']) => Promise<Response>
): RouteHandler {
  return (ctx: RouteHandlerContext) => handler(ctx, ctx.env)
}

/**
 * Adapt a dataset handler with params
 */
function adaptDatasetHandler(
  handler: (ctx: HandlerContext, datasetId: string) => Promise<Response> | Response
): RouteHandler {
  return (ctx: RouteHandlerContext) => handler(ctx, ctx.params.dataset!)
}

/**
 * Adapt a collection handler with params
 */
function adaptCollectionHandler(
  handler: (ctx: HandlerContext, datasetId: string, collectionId: string) => Promise<Response>
): RouteHandler {
  return (ctx: RouteHandlerContext) => handler(ctx, ctx.params.dataset!, ctx.params.collection!)
}

/**
 * Adapt an entity handler with params
 */
function adaptEntityHandler(
  handler: (ctx: HandlerContext, datasetId: string, collectionId: string, entityId: string) => Promise<Response>
): RouteHandler {
  return (ctx: RouteHandlerContext) => handler(ctx, ctx.params.dataset!, ctx.params.collection!, ctx.params.id!)
}

/**
 * Adapt a relationship handler with params
 */
function adaptRelationshipHandler(
  handler: (ctx: HandlerContext, datasetId: string, collectionId: string, entityId: string, predicate: string) => Promise<Response>
): RouteHandler {
  return (ctx: RouteHandlerContext) => handler(ctx, ctx.params.dataset!, ctx.params.collection!, ctx.params.id!, ctx.params.predicate!)
}

/**
 * Adapt an ns handler with params
 */
function adaptNsHandler(
  handler: (ctx: HandlerContext, ns: string, id?: string) => Promise<Response>
): RouteHandler {
  return (ctx: RouteHandlerContext) => handler(ctx, ctx.params.ns!, ctx.params.id)
}

// =============================================================================
// Route Definitions
// =============================================================================

/**
 * Core API routes
 */
const coreRoutes: RouteDefinition[] = [
  // Root - API Overview (exempt from rate limiting)
  route.get('/', adaptHandler(handleRoot), {
    description: 'API overview and documentation links',
    rateLimit: 'exempt',
  }),

  // Health check (exempt from rate limiting for monitoring)
  route.get('/health', adaptHandler(handleHealth), {
    description: 'Health check endpoint for monitoring',
    rateLimit: 'exempt',
  }),

  // Prometheus metrics
  route.get('/metrics', adaptHandler(handleMetrics), {
    description: 'Prometheus metrics endpoint',
    rateLimit: 'default',
  }),
]

/**
 * Benchmark routes
 */
const benchmarkRoutes: RouteDefinition[] = [
  route.get('/benchmark', handleBenchmark, {
    description: 'Basic R2 I/O benchmark',
    rateLimit: 'benchmark',
  }),
  route.get('/benchmark-datasets', handleBenchmarkDatasets, {
    description: 'Dataset I/O benchmark',
    rateLimit: 'benchmark',
  }),
  route.get('/benchmark-indexed', handleBenchmarkIndexed, {
    description: 'Secondary index benchmark',
    rateLimit: 'benchmark',
  }),
  route.get('/benchmark/backends', handleBenchmarkBackends, {
    description: 'Backend comparison benchmark (Native/Iceberg/Delta)',
    rateLimit: 'benchmark',
  }),
  route.get('/benchmark/datasets/backends', handleBenchmarkDatasetBackends, {
    description: 'Dataset + backend benchmark',
    rateLimit: 'benchmark',
  }),
]

/**
 * Debug routes (all require authentication)
 */
const debugRoutes: RouteDefinition[] = [
  route.get('/debug/r2', adaptDebugHandler(handleDebugR2), {
    description: 'Test R2 connectivity',
    rateLimit: 'debug',
  }),
  route.get('/debug/entity', adaptDebugHandler(handleDebugEntity), {
    description: 'Raw entity data debug',
    rateLimit: 'debug',
  }),
  route.get('/debug/indexes', adaptDebugHandler(handleDebugIndexes), {
    description: 'Index selection debugging',
    rateLimit: 'debug',
  }),
  route.get('/debug/query', adaptDebugHandler(handleDebugQuery), {
    description: 'Query with full diagnostics',
    rateLimit: 'debug',
  }),
  route.get('/debug/cache', adaptDebugHandler(handleDebugCache), {
    description: 'Cache statistics',
    rateLimit: 'debug',
  }),
]

/**
 * Migration routes
 */
const migrationRoutes: RouteDefinition[] = [
  // Match all /migrate* paths and delegate to handler
  route.get('/migrate', handleMigration, {
    description: 'Get migration status',
    rateLimit: 'migration',
  }),
  route.post('/migrate', handleMigration, {
    description: 'Start backend migration',
    rateLimit: 'migration',
  }),
  route.get('/migrate/status', handleMigration, {
    description: 'Get migration status',
    rateLimit: 'migration',
  }),
  route.post('/migrate/cancel', handleMigration, {
    description: 'Cancel running migration',
    rateLimit: 'migration',
  }),
  route.get('/migrate/jobs', handleMigration, {
    description: 'List migration history',
    rateLimit: 'migration',
  }),
]

/**
 * Vacuum routes
 */
const vacuumRoutes: RouteDefinition[] = [
  route.post('/vacuum/start', handleVacuumStart, {
    description: 'Start vacuum workflow',
    rateLimit: 'vacuum',
  }),
  route.get('/vacuum/status/:id', handleVacuumStatus, {
    description: 'Get vacuum workflow status',
    rateLimit: 'vacuum',
  }),
]

/**
 * Compaction routes
 */
const compactionRoutes: RouteDefinition[] = [
  route.get('/compaction/status', handleCompactionStatus, {
    description: 'Get compaction status for namespace(s)',
    rateLimit: 'compaction',
  }),
  route.get('/compaction/health', handleCompactionHealth, {
    description: 'Aggregated health check for monitoring',
    rateLimit: 'compaction',
  }),
  route.get('/compaction/dashboard', handleCompactionDashboard, {
    description: 'HTML monitoring dashboard',
    rateLimit: 'compaction',
  }),
  route.get('/compaction/metrics', handleCompactionMetrics, {
    description: 'Prometheus metrics export',
    rateLimit: 'compaction',
  }),
  route.get('/compaction/metrics/json', handleCompactionMetricsJson, {
    description: 'JSON time-series export',
    rateLimit: 'compaction',
  }),
]

/**
 * Dataset browsing routes
 */
const datasetRoutes: RouteDefinition[] = [
  route.get('/datasets', adaptHandler(handleDatasetsList), {
    description: 'List all datasets',
    rateLimit: 'datasets',
  }),
  route.get('/datasets/:dataset', adaptDatasetHandler(handleDatasetDetail), {
    description: 'Dataset detail and collections',
    rateLimit: 'datasets',
  }),
  route.get('/datasets/:dataset/:collection', adaptCollectionHandler(handleCollectionList), {
    description: 'Collection entity list',
    rateLimit: 'datasets',
  }),
  // Relationship traversal must come before entity detail (more specific)
  route.get('/datasets/:dataset/:collection/:id/:predicate', adaptRelationshipHandler(handleRelationshipTraversal), {
    description: 'Relationship traversal',
    rateLimit: 'datasets',
  }),
  route.get('/datasets/:dataset/:collection/:id', adaptEntityHandler(handleEntityDetail), {
    description: 'Entity detail',
    rateLimit: 'datasets',
  }),
]

/**
 * Legacy /ns routes (backwards compatibility)
 */
const nsRoutes: RouteDefinition[] = [
  {
    method: ['GET', 'POST', 'PATCH', 'DELETE'],
    pattern: '/ns/:ns',
    handler: adaptNsHandler(handleNsRoute),
    description: 'Legacy namespace operations',
    rateLimit: 'ns',
  },
  {
    method: ['GET', 'PATCH', 'DELETE'],
    pattern: '/ns/:ns/:id',
    handler: adaptNsHandler(handleNsRoute),
    description: 'Legacy namespace entity operations',
    rateLimit: 'ns',
  },
]

// =============================================================================
// Registry Setup
// =============================================================================

/**
 * Create and configure the route registry
 */
export function createWorkerRouteRegistry() {
  const registry = createRouteRegistry()

  // Register routes in order of specificity
  registry.registerAll(coreRoutes)
  registry.registerAll(benchmarkRoutes)
  registry.registerAll(debugRoutes)
  registry.registerAll(migrationRoutes)
  registry.registerAll(vacuumRoutes)
  registry.registerAll(compactionRoutes)
  registry.registerAll(datasetRoutes)
  registry.registerAll(nsRoutes)

  return registry
}

/**
 * Singleton registry instance
 */
let registryInstance: ReturnType<typeof createRouteRegistry> | null = null

/**
 * Get the worker route registry singleton
 */
export function getWorkerRouteRegistry() {
  if (!registryInstance) {
    registryInstance = createWorkerRouteRegistry()
  }
  return registryInstance
}
