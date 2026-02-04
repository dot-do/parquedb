/**
 * Backend Migration Workflow
 *
 * Uses Cloudflare Workflows for reliable, resumable migrations that can handle
 * millions of entities without hitting subrequest limits.
 *
 * Key benefits:
 * - Each step can use up to 1,000 subrequests
 * - Up to 1,024 steps per workflow = 1M+ total operations
 * - Automatic state persistence between steps
 * - Resume from where you left off if interrupted
 * - Can run for up to 1 year with sleeps
 *
 * @example
 * ```typescript
 * // Start migration workflow
 * const instance = await env.MIGRATION_WORKFLOW.create({
 *   params: {
 *     to: 'iceberg',
 *     namespaces: ['users', 'posts'],
 *     batchSize: 400,
 *   }
 * })
 *
 * // Check status
 * const status = await instance.status()
 * ```
 */

import {
  WorkflowEntrypoint,
  WorkflowEvent,
  WorkflowStep,
} from 'cloudflare:workers'
import { R2Backend } from '../storage/R2Backend'
import {
  migrateBackend,
  detectExistingFormat,
  discoverNamespaces,
  type BackendType,
} from '../backends'
import { logger } from '../utils/logger'
import { toInternalR2Bucket } from './utils'

// =============================================================================
// Types
// =============================================================================

export interface MigrationWorkflowParams {
  /** Target format */
  to: BackendType
  /** Source format (or 'auto' to detect) */
  from?: BackendType | 'auto' | undefined
  /** Specific namespaces to migrate (empty = discover all) */
  namespaces?: string[] | undefined
  /** Batch size per step (default: 400, max ~450 to stay under 1000 subrequests) */
  batchSize?: number | undefined
  /** Delete source data after migration */
  deleteSource?: boolean | undefined
}

interface MigrationState {
  /** All namespaces to migrate */
  namespaces: string[]
  /** Current namespace index */
  currentIndex: number
  /** Total entities migrated */
  totalMigrated: number
  /** Errors encountered */
  errors: string[]
  /** Start time */
  startedAt: number
}

interface Env {
  BUCKET: R2Bucket
}

// =============================================================================
// Constants
// =============================================================================

/**
 * Default batch size - stay well under 1,000 subrequests
 * Each entity needs ~2 subrequests (read + write), plus metadata operations
 */
const DEFAULT_BATCH_SIZE = 400

/**
 * Maximum namespaces per workflow
 * With 1,024 steps and multiple namespaces needing multiple batches,
 * limit to a reasonable number
 */
const MAX_NAMESPACES = 500

// =============================================================================
// Migration Workflow
// =============================================================================

export class MigrationWorkflow extends WorkflowEntrypoint<Env, MigrationWorkflowParams> {
  /**
   * Main workflow execution
   */
  override async run(event: WorkflowEvent<MigrationWorkflowParams>, step: WorkflowStep) {
    const params = event.payload
    const to = params.to
    const from = params.from ?? 'auto'
    const batchSize = Math.min(params.batchSize ?? DEFAULT_BATCH_SIZE, 450)
    const deleteSource = params.deleteSource ?? false

    // Step 1: Initialize and discover namespaces
    const state = await step.do('initialize', async () => {
      const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))

      let namespaces = params.namespaces ?? []

      if (namespaces.length === 0) {
        // Discover all namespaces
        namespaces = await discoverNamespaces(storage)
        logger.info(`Discovered ${namespaces.length} namespaces`)
      }

      if (namespaces.length > MAX_NAMESPACES) {
        throw new Error(`Too many namespaces (${namespaces.length}). Max is ${MAX_NAMESPACES}. Split into multiple workflows.`)
      }

      const initialState: MigrationState = {
        namespaces,
        currentIndex: 0,
        totalMigrated: 0,
        errors: [],
        startedAt: Date.now(),
      }

      return initialState
    })

    // Process each namespace
    let currentState = state

    while (currentState.currentIndex < currentState.namespaces.length) {
      const namespace = currentState.namespaces[currentState.currentIndex]
      if (!namespace) break

      // Step N: Migrate one namespace
      currentState = await step.do(`migrate-${namespace}`, async () => {
        const storage = new R2Backend(toInternalR2Bucket(this.env.BUCKET))

        logger.info(`Starting migration for ${namespace}`, {
          index: currentState.currentIndex,
          total: currentState.namespaces.length,
        })

        // Detect source format
        let sourceFormat = from
        if (sourceFormat === 'auto') {
          const { primary } = await detectExistingFormat(storage, namespace)
          if (!primary) {
            logger.warn(`No data found for ${namespace}, skipping`)
            return {
              ...currentState,
              currentIndex: currentState.currentIndex + 1,
            }
          }
          sourceFormat = primary
        }

        // Skip if already in target format
        if (sourceFormat === to) {
          logger.info(`${namespace} already in ${to} format, skipping`)
          return {
            ...currentState,
            currentIndex: currentState.currentIndex + 1,
          }
        }

        // Run migration
        try {
          const result = await migrateBackend({
            storage,
            from: sourceFormat,
            to,
            namespaces: [namespace],
            batchSize,
            deleteSource,
            onProgress: (progress) => {
              logger.debug(`Progress: ${progress.namespace}`, {
                migrated: progress.entitiesMigrated,
                total: progress.totalEntities,
                phase: progress.phase,
              })
            },
          })

          if (result.errors.length > 0) {
            return {
              ...currentState,
              currentIndex: currentState.currentIndex + 1,
              totalMigrated: currentState.totalMigrated + result.entitiesMigrated,
              errors: [...currentState.errors, ...result.errors.map(e => `${namespace}: ${e}`)],
            }
          }

          logger.info(`Migrated ${namespace}`, {
            entities: result.entitiesMigrated,
            duration: result.durationMs,
          })

          return {
            ...currentState,
            currentIndex: currentState.currentIndex + 1,
            totalMigrated: currentState.totalMigrated + result.entitiesMigrated,
          }
        } catch (err) {
          const errorMsg = err instanceof Error ? err.message : 'Unknown error'
          logger.error(`Migration failed for ${namespace}`, { error: errorMsg })

          return {
            ...currentState,
            currentIndex: currentState.currentIndex + 1,
            errors: [...currentState.errors, `${namespace}: ${errorMsg}`],
          }
        }
      })

      // Optional: Add a small sleep between namespaces to avoid overwhelming R2
      if (currentState.currentIndex < currentState.namespaces.length) {
        await step.sleep('cooldown', 100) // 100ms
      }
    }

    // Final step: Return summary
    const summary = await step.do('finalize', async () => {
      const duration = Date.now() - currentState.startedAt

      return {
        success: currentState.errors.length === 0,
        namespacesMigrated: currentState.currentIndex,
        totalNamespaces: currentState.namespaces.length,
        entitiesMigrated: currentState.totalMigrated,
        errors: currentState.errors,
        durationMs: duration,
      }
    })

    logger.info('Migration workflow completed', summary)

    return summary
  }
}

export default MigrationWorkflow
