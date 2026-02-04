/**
 * Materialized View Refresh Queue Consumer
 *
 * Receives R2 event notifications and notifies MVRefreshStateDO of source data changes.
 * This triggers the MV refresh pipeline for streaming MVs.
 *
 * Architecture:
 * ```
 * R2 Write → Event Notification → Queue → This Consumer
 *                                              ↓
 *                                    MVRefreshStateDO.notify-change
 *                                              ↓
 *                                    MVRefreshStateDO.get-ready-mvs
 *                                              ↓
 *                                    MVRefreshWorkflow.create()
 * ```
 *
 * Follows the same pattern as compaction-queue-consumer.ts
 */

import { logger } from '../utils/logger'
import type {
  MVRefreshConsumerConfig,
  MVRefreshResponse,
} from './mv-refresh-state-do'

// =============================================================================
// Types
// =============================================================================

/**
 * R2 Event Notification message from queue
 * Same shape as compaction queue consumer
 */
export interface R2EventMessage {
  account: string
  bucket: string
  object: {
    key: string
    size: number
    eTag: string
  }
  action: 'PutObject' | 'CopyObject' | 'CompleteMultipartUpload' | 'DeleteObject' | 'LifecycleDeletion'
  eventTime: string
}

/**
 * Environment bindings for the MV refresh queue consumer
 */
export interface MVRefreshQueueEnv {
  /** Durable Object namespace for MV refresh state tracking */
  MV_REFRESH_STATE: DurableObjectNamespace

  /** Workflow binding for MV refresh workflows */
  MV_REFRESH_WORKFLOW: {
    create(options: { params: unknown }): Promise<{ id: string }>
  }
}

/**
 * MV refresh workflow parameters
 */
export interface MVRefreshWorkflowParams {
  mvName: string
  source: string
  changedFiles: string[]
  refreshType: 'full' | 'incremental'
}

// =============================================================================
// Queue Consumer Handler
// =============================================================================

/**
 * Handle a batch of R2 event notifications for MV refresh
 *
 * Processes events, groups by namespace, notifies MVRefreshStateDO,
 * and triggers workflows for ready MVs.
 */
export async function handleMVRefreshQueue(
  batch: MessageBatch<R2EventMessage>,
  env: MVRefreshQueueEnv,
  config: MVRefreshConsumerConfig = {}
): Promise<void> {
  logger.debug('Received MV refresh batch', {
    messageCount: batch.messages.length,
    queue: batch.queue,
  })

  const {
    namespacePrefix = 'data/',
    debounceMs = 1000,
    maxWaitMs = 5000,
  } = config

  // Group changes by namespace
  const changesByNamespace = new Map<string, string[]>()

  for (const message of batch.messages) {
    const event = message.body

    // Only process object-create events for Parquet files
    const isCreateAction = event.action === 'PutObject' ||
      event.action === 'CopyObject' ||
      event.action === 'CompleteMultipartUpload'

    if (!isCreateAction) {
      logger.debug('Skipping non-create action', { action: event.action })
      message.ack()
      continue
    }

    if (!event.object.key.endsWith('.parquet')) {
      logger.debug('Skipping non-parquet file', { key: event.object.key })
      message.ack()
      continue
    }

    if (!event.object.key.startsWith(namespacePrefix)) {
      logger.debug('Skipping file outside prefix', { key: event.object.key, prefix: namespacePrefix })
      message.ack()
      continue
    }

    // Parse namespace from key: data/{namespace}/{timestamp}-{writer}-{seq}.parquet
    const keyWithoutPrefix = event.object.key.slice(namespacePrefix.length)
    const parts = keyWithoutPrefix.split('/')
    const namespace = parts.slice(0, -1).join('/')

    if (!namespace) {
      logger.debug('Skipping file with no namespace', { key: event.object.key })
      message.ack()
      continue
    }

    // Group by namespace
    const files = changesByNamespace.get(namespace) ?? []
    files.push(event.object.key)
    changesByNamespace.set(namespace, files)

    message.ack()
  }

  if (changesByNamespace.size === 0) {
    logger.debug('No matching changes to process')
    return
  }

  // Get the MV refresh state DO
  const stateId = env.MV_REFRESH_STATE.idFromName('mv-refresh')
  const stateDO = env.MV_REFRESH_STATE.get(stateId)

  // Notify changes for each namespace
  const now = Date.now()
  for (const [namespace, files] of changesByNamespace) {
    await stateDO.fetch(new Request('http://internal/notify-change', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace,
        files,
        timestamp: now,
      }),
    }))

    logger.debug('Notified MV refresh state DO', {
      namespace,
      fileCount: files.length,
    })
  }

  // Get MVs ready for refresh
  const readyResponse = await stateDO.fetch(new Request('http://internal/get-ready-mvs', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      config: { debounceMs, maxWaitMs },
    }),
  }))

  const { mvsReady } = await readyResponse.json() as MVRefreshResponse

  // Trigger workflows for ready MVs
  for (const mv of mvsReady) {
    try {
      const workflowParams: MVRefreshWorkflowParams = {
        mvName: mv.mvName,
        source: mv.source,
        changedFiles: mv.changedFiles,
        refreshType: mv.changedFiles.length > 10 ? 'full' : 'incremental',
      }

      const workflow = await env.MV_REFRESH_WORKFLOW.create({
        params: workflowParams,
      })

      logger.info('MV refresh workflow started', {
        workflowId: workflow.id,
        mvName: mv.mvName,
        source: mv.source,
        changedFiles: mv.changedFiles.length,
      })

      // Confirm dispatch
      await stateDO.fetch(new Request('http://internal/confirm-dispatch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          mvName: mv.mvName,
          workflowId: workflow.id,
        }),
      }))
    } catch (err) {
      logger.error('Failed to start MV refresh workflow', {
        mvName: mv.mvName,
        error: err instanceof Error ? err.message : 'Unknown',
      })

      // On failure, the MV will remain in 'processing' state
      // and can be recovered later via stuck window recovery
    }
  }
}

export default { handleMVRefreshQueue }
