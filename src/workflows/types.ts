/**
 * Shared workflow types
 *
 * These types are derived from the main Env interface to ensure consistency
 * across all workflow files. Using Pick<Env, ...> ensures that when the main
 * Env type changes, these derived types remain in sync.
 */

import type { Env } from '../types/worker'

// =============================================================================
// Workflow Environment Types
// =============================================================================

/**
 * Environment bindings for compaction/migration workflows
 * Includes BUCKET for R2 operations and COMPACTION_STATE for completion notifications
 */
export type WorkflowEnv = Pick<Env, 'BUCKET'> & Required<Pick<Env, 'COMPACTION_STATE'>>

/**
 * Environment bindings for the compaction queue consumer
 * Uses Required<Pick<...>> to derive BUCKET and COMPACTION_STATE from the main Env
 * (making them non-optional), with a narrower COMPACTION_WORKFLOW type since
 * only create() is needed
 */
export type CompactionQueueEnv = Required<Pick<Env, 'BUCKET' | 'COMPACTION_STATE'>> & {
  /**
   * Workflow binding for starting compaction workflows
   * Using a narrower type than the full COMPACTION_WORKFLOW since
   * the queue consumer only needs to create new workflow instances
   */
  COMPACTION_WORKFLOW: {
    create(options: { params: unknown }): Promise<{ id: string }>
  }
}

// Re-export the main Env for convenience
export type { Env } from '../types/worker'
