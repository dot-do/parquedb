/**
 * Materialized View Refresh Workflow
 *
 * Cloudflare Workflow that executes MV refresh operations.
 * Triggered by handleMVRefreshQueue when MVs are ready for refresh.
 *
 * Architecture:
 * ```
 * MVRefreshStateDO.get-ready-mvs
 *           ↓
 * handleMVRefreshQueue → This Workflow
 *                              ↓
 *                    Step 1: Read source data
 *                              ↓
 *                    Step 2: Apply MV transformation
 *                              ↓
 *                    Step 3: Write MV data to R2
 *                              ↓
 *                    Step 4: Notify MVRefreshStateDO
 * ```
 *
 * The workflow is resumable - if it fails mid-execution, it can be
 * restarted from the last completed step.
 */

import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

/**
 * Parameters for the MV refresh workflow
 */
export interface MVRefreshWorkflowParams {
  /** Name of the MV to refresh */
  mvName: string
  /** Source namespace that changed */
  source: string
  /** List of changed files that triggered the refresh */
  changedFiles: string[]
  /** Type of refresh: full (recompute all) or incremental (only changes) */
  refreshType: 'full' | 'incremental'
}

/**
 * Result of the MV refresh workflow
 */
export interface MVRefreshWorkflowResult {
  /** Whether the refresh succeeded */
  success: boolean
  /** Name of the MV that was refreshed */
  mvName: string
  /** Path to the refreshed MV data */
  outputPath?: string | undefined
  /** Number of rows in the refreshed MV */
  rowCount?: number | undefined
  /** Error message if failed */
  error?: string | undefined
  /** Duration of the refresh in milliseconds */
  durationMs: number
}

/**
 * Workflow step context
 */
interface WorkflowStep {
  do<T>(name: string, fn: () => Promise<T>): Promise<T>
}

/**
 * Environment bindings for the MV refresh workflow
 */
export interface MVRefreshWorkflowEnv {
  /** R2 bucket for Parquet file storage */
  BUCKET: R2Bucket
  /** Durable Object namespace for MV refresh state tracking */
  MV_REFRESH_STATE: DurableObjectNamespace
}

// =============================================================================
// Workflow Implementation
// =============================================================================

/**
 * Cloudflare Workflow for MV refresh operations
 *
 * This workflow:
 * 1. Reads source data from R2
 * 2. Applies MV transformation (aggregation, filtering, etc.)
 * 3. Writes refreshed MV data to R2
 * 4. Notifies MVRefreshStateDO of completion
 *
 * Each step is wrapped in a workflow step for durability.
 */
export class MVRefreshWorkflow {
  /**
   * Run the MV refresh workflow
   */
  async run(
    event: { payload: { params: MVRefreshWorkflowParams } },
    step: WorkflowStep,
    env: MVRefreshWorkflowEnv
  ): Promise<MVRefreshWorkflowResult> {
    const { mvName, source, changedFiles, refreshType } = event.payload.params
    const startTime = Date.now()
    let outputPath: string | undefined
    let rowCount: number | undefined

    logger.info('Starting MV refresh workflow', {
      mvName,
      source,
      changedFiles: changedFiles.length,
      refreshType,
    })

    try {
      // Step 1: Read source data
      const sourceData = await step.do('read-source-data', async () => {
        logger.debug('Reading source data', { mvName, source, files: changedFiles.length })

        // In production, this would read the actual Parquet files
        // For now, we simulate reading by checking files exist
        const fileStats: Array<{ key: string; size: number }> = []

        for (const file of changedFiles) {
          const head = await env.BUCKET.head(file)
          if (head) {
            fileStats.push({ key: file, size: head.size })
          }
        }

        return { files: fileStats, rowCount: fileStats.length * 100 } // Simulated row count
      })

      // Step 2: Apply MV transformation
      const transformed = await step.do('apply-transformation', async () => {
        logger.debug('Applying MV transformation', { mvName, refreshType })

        // In production, this would apply the MV's transformation logic
        // (aggregation, filtering, joins, etc.)
        // For now, we simulate the transformation
        return {
          data: `Transformed data for ${mvName}`,
          rowCount: sourceData.rowCount,
        }
      })

      // Step 3: Write MV data to R2
      outputPath = await step.do('write-mv-data', async () => {
        const path = `_views/${mvName}/data.parquet`
        logger.debug('Writing MV data', { mvName, path })

        // In production, this would write actual Parquet data
        // For now, we write simulated data
        await env.BUCKET.put(path, JSON.stringify({
          mvName,
          source,
          refreshType,
          refreshedAt: new Date().toISOString(),
          rowCount: transformed.rowCount,
        }))

        return path
      })

      rowCount = transformed.rowCount

      // Step 4: Notify MVRefreshStateDO of completion
      await step.do('notify-completion', async () => {
        const stateId = env.MV_REFRESH_STATE.idFromName('mv-refresh')
        const stateDO = env.MV_REFRESH_STATE.get(stateId)

        // Get workflow ID from event (in production, this would be available)
        // For now, we use a placeholder
        const workflowId = `mv-refresh-${mvName}-${startTime}`

        await stateDO.fetch(new Request('http://internal/workflow-complete', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            mvName,
            workflowId,
            success: true,
          }),
        }))

        logger.info('Notified MVRefreshStateDO of completion', { mvName, workflowId })
      })

      const durationMs = Date.now() - startTime
      logger.info('MV refresh workflow completed', {
        mvName,
        outputPath,
        rowCount,
        durationMs,
      })

      return {
        success: true,
        mvName,
        outputPath,
        rowCount,
        durationMs,
      }
    } catch (err) {
      const durationMs = Date.now() - startTime
      const errorMessage = err instanceof Error ? err.message : 'Unknown error'

      logger.error('MV refresh workflow failed', {
        mvName,
        error: errorMessage,
        durationMs,
      })

      // Notify MVRefreshStateDO of failure
      try {
        const stateId = env.MV_REFRESH_STATE.idFromName('mv-refresh')
        const stateDO = env.MV_REFRESH_STATE.get(stateId)
        const workflowId = `mv-refresh-${mvName}-${startTime}`

        await stateDO.fetch(new Request('http://internal/workflow-complete', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            mvName,
            workflowId,
            success: false,
          }),
        }))
      } catch {
        // Ignore notification failure
      }

      return {
        success: false,
        mvName,
        error: errorMessage,
        durationMs,
      }
    }
  }
}

export default { MVRefreshWorkflow }
