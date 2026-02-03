/**
 * Migration Durable Object
 *
 * Handles backend migrations in batches to work within subrequest limits.
 * Uses DO alarm() to continue processing without hitting the 1,000 subrequest limit.
 *
 * Flow:
 * 1. Receive migration request via HTTP or RPC
 * 2. Process a batch of entities (stay under ~800 subrequests)
 * 3. Persist progress to SQLite
 * 4. Set alarm to continue
 * 5. Repeat until complete
 *
 * @example
 * ```typescript
 * // Start migration via Worker
 * const id = env.MIGRATION_DO.idFromName('default')
 * const stub = env.MIGRATION_DO.get(id)
 * const response = await stub.fetch(new Request('http://internal/migrate', {
 *   method: 'POST',
 *   body: JSON.stringify({
 *     from: 'native',
 *     to: 'iceberg',
 *     namespaces: ['users', 'posts'],
 *   }),
 * }))
 * ```
 */

import { DurableObject } from 'cloudflare:workers'
import { R2Backend } from '../storage/R2Backend'
import {
  migrateBackend,
  detectExistingFormat,
  discoverNamespaces,
  type BackendType,
} from '../backends'
import { logger } from '../utils/logger'
import { extractBearerToken, verifyJWT } from './jwt-utils'

// =============================================================================
// Types
// =============================================================================

interface MigrationJob {
  id: string
  from: BackendType | 'auto'
  to: BackendType
  namespaces: string[]
  batchSize: number
  deleteSource: boolean
  status: 'pending' | 'running' | 'completed' | 'failed'
  progress: {
    currentNamespace: string | null
    namespacesCompleted: string[]
    entitiesMigrated: number
    currentOffset: number
    errors: string[]
  }
  createdAt: number
  updatedAt: number
  completedAt: number | null
}

interface MigrationRequest {
  from?: BackendType | 'auto' | undefined
  to: BackendType
  namespaces?: string[] | undefined
  batchSize?: number | undefined
  deleteSource?: boolean | undefined
}

interface Env {
  BUCKET: R2Bucket
  /** JWKS URI for JWT token verification */
  JWKS_URI?: string | undefined
}

// =============================================================================
// Constants
// =============================================================================

/**
 * Batch size for migration operations.
 * Stay well under 1,000 subrequests (read + write = 2 per entity)
 */
const DEFAULT_BATCH_SIZE = 400

/**
 * Delay between batches to avoid overwhelming R2
 */
const BATCH_DELAY_MS = 100

/**
 * Maximum retries for failed operations
 */
const MAX_RETRIES = 3

// =============================================================================
// Migration Durable Object
// =============================================================================

export class MigrationDO extends DurableObject<Env> {
  private storage: DurableObjectStorage
  private r2Backend: R2Backend | null = null

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.storage = ctx.storage
  }

  /**
   * Get R2Backend instance (lazy initialization)
   */
  private getR2Backend(): R2Backend {
    if (!this.r2Backend) {
      this.r2Backend = new R2Backend(this.env.BUCKET as unknown as import('../storage/types/r2').R2Bucket)
    }
    return this.r2Backend
  }

  /**
   * Verify that the request has valid authentication for migration endpoints.
   * Returns an error Response if authentication fails, null if authenticated.
   *
   * SECURITY: All migration endpoints require authentication to prevent:
   * - Unauthorized migrations that consume resources
   * - Cancellation of legitimate migrations
   * - Exposure of internal database structure
   */
  private async requireMigrationAuth(request: Request): Promise<Response | null> {
    const token = extractBearerToken(request)

    if (!token) {
      return new Response(JSON.stringify({
        error: 'Authentication required. Provide a valid Bearer token.',
      }), {
        status: 401,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // Use the Env interface which includes JWKS_URI
    const verifyResult = await verifyJWT(token, this.env as Parameters<typeof verifyJWT>[1])
    if (!verifyResult.valid) {
      return new Response(JSON.stringify({
        error: verifyResult.error ?? 'Invalid token',
      }), {
        status: 401,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    return null // Authentication successful
  }

  /**
   * Handle HTTP requests
   *
   * SECURITY: All migration endpoints require authentication via Bearer token.
   * These endpoints can trigger expensive operations and expose internal state.
   */
  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // Require authentication for all migration endpoints
    const authError = await this.requireMigrationAuth(request)
    if (authError) {
      return authError
    }

    try {
      switch (path) {
        case '/migrate':
          if (request.method === 'POST') {
            return this.handleStartMigration(request)
          }
          break

        case '/status':
          return this.handleGetStatus()

        case '/cancel':
          if (request.method === 'POST') {
            return this.handleCancelMigration()
          }
          break

        case '/jobs':
          return this.handleListJobs()
      }

      return new Response('Not Found', { status: 404 })
    } catch (error) {
      logger.error('MigrationDO error', { error })
      return new Response(JSON.stringify({
        error: error instanceof Error ? error.message : 'Unknown error',
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  /**
   * Handle alarm - continue batch processing
   */
  override async alarm(): Promise<void> {
    logger.info('MigrationDO alarm triggered')

    const currentJob = await this.storage.get<MigrationJob>('currentJob')

    if (!currentJob || currentJob.status !== 'running') {
      logger.info('No active migration job', { status: currentJob?.status })
      return
    }

    logger.info('Processing migration batch', {
      jobId: currentJob.id,
      namespace: currentJob.progress.currentNamespace,
    })

    try {
      await this.processBatch(currentJob)
    } catch (error) {
      logger.error('Migration batch failed', { error, jobId: currentJob.id })

      currentJob.progress.errors.push(
        error instanceof Error ? error.message : 'Unknown error'
      )
      currentJob.updatedAt = Date.now()

      // Retry or fail
      if (currentJob.progress.errors.length >= MAX_RETRIES) {
        currentJob.status = 'failed'
        currentJob.completedAt = Date.now()
      } else {
        // Schedule retry with backoff
        const delay = Math.pow(2, currentJob.progress.errors.length) * 1000
        await this.ctx.storage.setAlarm(Date.now() + delay)
      }

      await this.storage.put('currentJob', currentJob)
    }
  }

  /**
   * Start a new migration job
   */
  private async handleStartMigration(request: Request): Promise<Response> {
    // Check for existing job
    const existingJob = await this.storage.get<MigrationJob>('currentJob')
    if (existingJob && existingJob.status === 'running') {
      return new Response(JSON.stringify({
        error: 'Migration already in progress',
        job: existingJob,
      }), {
        status: 409,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    const body = await request.json() as MigrationRequest
    const storage = this.getR2Backend()

    // Discover namespaces if not specified
    let namespaces = body.namespaces
    if (!namespaces || namespaces.length === 0) {
      namespaces = await discoverNamespaces(storage)
    }

    if (namespaces.length === 0) {
      return new Response(JSON.stringify({
        error: 'No namespaces found to migrate',
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // Create job
    const job: MigrationJob = {
      id: `migration-${Date.now()}`,
      from: body.from ?? 'auto',
      to: body.to,
      namespaces,
      batchSize: body.batchSize ?? DEFAULT_BATCH_SIZE,
      deleteSource: body.deleteSource ?? false,
      status: 'running',
      progress: {
        currentNamespace: namespaces[0] ?? null,
        namespacesCompleted: [],
        entitiesMigrated: 0,
        currentOffset: 0,
        errors: [],
      },
      createdAt: Date.now(),
      updatedAt: Date.now(),
      completedAt: null,
    }

    await this.storage.put('currentJob', job)

    // Schedule processing via alarm
    // Note: Using 1000ms to ensure alarm fires reliably
    await this.ctx.storage.setAlarm(Date.now() + 1000)
    logger.info('Alarm set for migration processing')

    // Also use waitUntil to ensure the alarm gets set before response
    this.ctx.waitUntil(Promise.resolve().then(() => {
      logger.info('waitUntil: Migration job created', { jobId: job.id })
    }))

    return new Response(JSON.stringify({
      message: 'Migration started',
      job,
    }), {
      status: 202,
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Process a batch of entities
   */
  private async processBatch(job: MigrationJob): Promise<void> {
    const storage = this.getR2Backend()
    const namespace = job.progress.currentNamespace

    if (!namespace) {
      // No more namespaces - migration complete
      job.status = 'completed'
      job.completedAt = Date.now()
      job.updatedAt = Date.now()
      await this.storage.put('currentJob', job)

      logger.info('Migration completed', {
        jobId: job.id,
        entitiesMigrated: job.progress.entitiesMigrated,
        namespacesCompleted: job.progress.namespacesCompleted,
      })
      return
    }

    logger.info(`Processing batch for ${namespace}`, {
      offset: job.progress.currentOffset,
      batchSize: job.batchSize,
    })

    // Detect source format
    let fromFormat = job.from
    if (fromFormat === 'auto') {
      logger.info(`Detecting format for ${namespace}`)
      const { formats, primary } = await detectExistingFormat(storage, namespace)
      logger.info(`Format detection result`, { namespace, formats, primary })
      if (!primary) {
        logger.warn(`No data found for ${namespace}, skipping`)
        await this.moveToNextNamespace(job)
        return
      }
      fromFormat = primary
    }
    logger.info(`Source format determined: ${fromFormat}`)

    // Skip if already in target format
    if (fromFormat === job.to) {
      logger.info(`${namespace} already in ${job.to} format, skipping`)
      await this.moveToNextNamespace(job)
      return
    }

    // Run migration for this namespace
    // The migrateBackend function handles reading and writing
    const result = await migrateBackend({
      storage,
      from: fromFormat,
      to: job.to,
      namespaces: [namespace],
      batchSize: job.batchSize,
      deleteSource: job.deleteSource,
      onProgress: (progress) => {
        logger.debug(`Migration progress: ${progress.namespace}`, {
          migrated: progress.entitiesMigrated,
          total: progress.totalEntities,
          phase: progress.phase,
        })
      },
    })

    job.progress.entitiesMigrated += result.entitiesMigrated

    if (result.errors.length > 0) {
      job.progress.errors.push(...result.errors)
    }

    // Move to next namespace
    await this.moveToNextNamespace(job)
  }

  /**
   * Move to the next namespace in the queue
   */
  private async moveToNextNamespace(job: MigrationJob): Promise<void> {
    const currentNamespace = job.progress.currentNamespace

    if (currentNamespace) {
      job.progress.namespacesCompleted.push(currentNamespace)
    }

    // Find next namespace
    const nextIndex = job.progress.namespacesCompleted.length
    const nextNamespace = job.namespaces[nextIndex] ?? null

    job.progress.currentNamespace = nextNamespace
    job.progress.currentOffset = 0
    job.updatedAt = Date.now()

    await this.storage.put('currentJob', job)

    if (nextNamespace) {
      // Schedule next batch
      await this.ctx.storage.setAlarm(Date.now() + BATCH_DELAY_MS)
    } else {
      // All namespaces done
      job.status = 'completed'
      job.completedAt = Date.now()
      await this.storage.put('currentJob', job)

      logger.info('Migration completed', {
        jobId: job.id,
        entitiesMigrated: job.progress.entitiesMigrated,
        errors: job.progress.errors,
      })
    }
  }

  /**
   * Get current migration status
   */
  private async handleGetStatus(): Promise<Response> {
    const job = await this.storage.get<MigrationJob>('currentJob')

    if (!job) {
      return new Response(JSON.stringify({
        status: 'idle',
        message: 'No migration job found',
      }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    const progress = job.namespaces.length > 0
      ? (job.progress.namespacesCompleted.length / job.namespaces.length) * 100
      : 0

    return new Response(JSON.stringify({
      job,
      progressPercent: Math.round(progress),
      summary: {
        total: job.namespaces.length,
        completed: job.progress.namespacesCompleted.length,
        current: job.progress.currentNamespace,
        entitiesMigrated: job.progress.entitiesMigrated,
        errors: job.progress.errors.length,
      },
    }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Cancel current migration
   */
  private async handleCancelMigration(): Promise<Response> {
    const job = await this.storage.get<MigrationJob>('currentJob')

    if (!job || job.status !== 'running') {
      return new Response(JSON.stringify({
        error: 'No active migration to cancel',
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    // Cancel alarm
    await this.ctx.storage.deleteAlarm()

    job.status = 'failed'
    job.progress.errors.push('Cancelled by user')
    job.completedAt = Date.now()
    job.updatedAt = Date.now()

    await this.storage.put('currentJob', job)

    return new Response(JSON.stringify({
      message: 'Migration cancelled',
      job,
    }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * List all migration jobs
   */
  private async handleListJobs(): Promise<Response> {
    const currentJob = await this.storage.get<MigrationJob>('currentJob')
    const history = await this.storage.get<MigrationJob[]>('jobHistory') ?? []

    return new Response(JSON.stringify({
      current: currentJob,
      history: history.slice(-10), // Last 10 jobs
    }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }
}

export default MigrationDO
