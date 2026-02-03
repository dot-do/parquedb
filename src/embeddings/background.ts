/**
 * Background Embedding Generation for ParqueDB
 *
 * Provides asynchronous embedding generation using Cloudflare Durable Object
 * Alarms. When entities are created or updated, they can be queued for
 * background embedding generation instead of blocking the write operation.
 *
 * Features:
 * - Queue metrics (depth, processed, failed counts)
 * - Dead letter queue for exhausted items
 * - Webhook/callback support for failure notifications
 * - Manual retry API for failed items
 * - Structured logging integration
 *
 * @example
 * ```typescript
 * // In ParqueDBDO
 * const embeddingQueue = new EmbeddingQueue(this.ctx.storage, {
 *   provider: createWorkersAIProvider(env.AI),
 *   fields: ['description', 'content'],
 *   vectorField: 'embedding',
 *   batchSize: 10,
 *   onError: async (item, error) => {
 *     // Send to alerting service
 *     await alertService.notify('embedding-failure', { item, error })
 *   }
 * })
 *
 * // On entity create/update
 * await embeddingQueue.enqueue('posts', 'abc123')
 *
 * // In alarm handler
 * async alarm() {
 *   await embeddingQueue.processQueue()
 * }
 *
 * // Check metrics
 * const metrics = await embeddingQueue.getMetrics()
 * console.log(`Queue depth: ${metrics.queueDepth}, Failed: ${metrics.totalFailed}`)
 *
 * // Retry failed items
 * const deadLetterItems = await embeddingQueue.getDeadLetterItems()
 * await embeddingQueue.retryDeadLetterItem('posts', 'abc123')
 * ```
 */

import type { EmbeddingProvider } from './provider'
import {
  DEFAULT_EMBEDDING_BATCH_SIZE,
  DEFAULT_EMBEDDING_PROCESS_DELAY,
  DEFAULT_EMBEDDING_PRIORITY,
  DEFAULT_MAX_RETRIES,
} from '../constants'
import { logger } from '../utils/logger'

// =============================================================================
// Types
// =============================================================================

/**
 * Dead letter queue item for items that exhausted retries
 */
export interface DeadLetterItem {
  /** Entity type/namespace */
  entityType: string

  /** Entity ID */
  entityId: string

  /** When the item was originally queued */
  createdAt: number

  /** When the item was moved to dead letter queue */
  movedAt: number

  /** Total number of attempts made */
  attempts: number

  /** Last error message */
  lastError: string

  /** Priority of the original item */
  priority?: number
}

/**
 * Queue metrics for monitoring
 */
export interface QueueMetrics {
  /** Current number of items in the queue */
  queueDepth: number

  /** Total items processed successfully (since metrics were last reset) */
  totalProcessed: number

  /** Total items that failed (since metrics were last reset) */
  totalFailed: number

  /** Number of items in the dead letter queue */
  deadLetterCount: number

  /** Average processing time in ms (last batch) */
  avgProcessingTimeMs: number

  /** Error rate (failed / (processed + failed)) */
  errorRate: number

  /** Timestamp of last metrics update */
  lastUpdated: number

  /** Queue backlog age in ms (age of oldest item) */
  backlogAgeMs: number
}

/**
 * Error callback type for embedding failures
 */
export type ErrorCallback = (
  item: EmbeddingQueueItem,
  error: Error | string,
  isExhausted: boolean
) => Promise<void> | void

/**
 * Configuration for background embedding generation
 */
export interface BackgroundEmbeddingConfig {
  /** Embedding provider to use */
  provider: EmbeddingProvider

  /** Source fields to embed (will be concatenated) */
  fields: string[]

  /** Target field to store the embedding vector */
  vectorField: string

  /** Number of items to process per batch (default: 10) */
  batchSize?: number

  /** Maximum retry attempts for failed embeddings (default: 3) */
  retryAttempts?: number

  /** Delay in ms before processing queue (default: 1000) */
  processDelay?: number

  /** Separator for concatenating multiple fields (default: '\n\n') */
  fieldSeparator?: string

  /** Callback for error notifications */
  onError?: ErrorCallback

  /** Whether to enable dead letter queue (default: true) */
  enableDeadLetter?: boolean
}

/**
 * Queue item for pending embedding generation
 */
export interface EmbeddingQueueItem {
  /** Entity type/namespace */
  entityType: string

  /** Entity ID */
  entityId: string

  /** When the item was queued */
  createdAt: number

  /** Number of processing attempts */
  attempts: number

  /** Last error message if any */
  lastError?: string

  /** Priority (lower = higher priority) */
  priority?: number
}

/**
 * Result of queue processing
 */
export interface QueueProcessingResult {
  /** Number of items successfully processed */
  processed: number

  /** Number of items that failed */
  failed: number

  /** Number of items remaining in queue */
  remaining: number

  /** Errors encountered during processing */
  errors: Array<{
    entityType: string
    entityId: string
    error: string
  }>
}

/**
 * Queue statistics
 */
export interface QueueStats {
  /** Total items in queue */
  total: number

  /** Items pending (0 attempts) */
  pending: number

  /** Items being retried (1+ attempts) */
  retrying: number

  /** Oldest item timestamp */
  oldestItem?: number
}

/**
 * Entity loader function type
 * Should return the entity data or null if not found
 */
export type EntityLoader = (
  entityType: string,
  entityId: string
) => Promise<Record<string, unknown> | null>

/**
 * Entity updater function type
 * Should update the entity with the embedding vector
 */
export type EntityUpdater = (
  entityType: string,
  entityId: string,
  vectorField: string,
  vector: number[]
) => Promise<void>

// =============================================================================
// EmbeddingQueue Class
// =============================================================================

/**
 * Stored metrics for persistence
 */
interface StoredMetrics {
  totalProcessed: number
  totalFailed: number
  avgProcessingTimeMs: number
  lastUpdated: number
}

/**
 * Queue for background embedding generation
 *
 * Uses Durable Object storage for persistence and alarms for processing.
 * Items are stored with a prefix to enable efficient listing and cleanup.
 * Includes error monitoring, dead letter queue, and metrics collection.
 */
export class EmbeddingQueue {
  /** Storage interface (DurableObjectStorage) */
  private storage: DurableObjectStorage

  /** Queue configuration */
  private config: Required<BackgroundEmbeddingConfig> & { enableDeadLetter: boolean }

  /** Function to load entity data */
  private entityLoader?: EntityLoader

  /** Function to update entity with embedding */
  private entityUpdater?: EntityUpdater

  /** Queue key prefix */
  private static readonly QUEUE_PREFIX = 'embed_queue:'

  /** Dead letter queue key prefix */
  private static readonly DEAD_LETTER_PREFIX = 'embed_dlq:'

  /** Metrics key */
  private static readonly METRICS_KEY = 'embed_queue_metrics'

  /** Stats key (deprecated, kept for backwards compat) */
  private static readonly STATS_KEY = 'embed_queue_stats'

  /**
   * Create a new EmbeddingQueue
   *
   * @param storage - Durable Object storage
   * @param config - Queue configuration
   */
  constructor(storage: DurableObjectStorage, config: BackgroundEmbeddingConfig) {
    this.storage = storage
    this.config = {
      ...config,
      batchSize: config.batchSize ?? DEFAULT_EMBEDDING_BATCH_SIZE,
      retryAttempts: config.retryAttempts ?? DEFAULT_MAX_RETRIES,
      processDelay: config.processDelay ?? DEFAULT_EMBEDDING_PROCESS_DELAY,
      fieldSeparator: config.fieldSeparator ?? '\n\n',
      onError: config.onError,
      enableDeadLetter: config.enableDeadLetter ?? true,
    }

    logger.debug('EmbeddingQueue initialized', {
      batchSize: this.config.batchSize,
      retryAttempts: this.config.retryAttempts,
      processDelay: this.config.processDelay,
      enableDeadLetter: this.config.enableDeadLetter,
    })
  }

  /**
   * Set the entity loader function
   *
   * @param loader - Function to load entity data
   */
  setEntityLoader(loader: EntityLoader): void {
    this.entityLoader = loader
  }

  /**
   * Set the entity updater function
   *
   * @param updater - Function to update entity with embedding
   */
  setEntityUpdater(updater: EntityUpdater): void {
    this.entityUpdater = updater
  }

  // ===========================================================================
  // Queue Operations
  // ===========================================================================

  /**
   * Add an entity to the embedding queue
   *
   * @param entityType - Entity type/namespace
   * @param entityId - Entity ID
   * @param priority - Optional priority (lower = higher priority)
   */
  async enqueue(entityType: string, entityId: string, priority = DEFAULT_EMBEDDING_PRIORITY): Promise<void> {
    const key = this.getQueueKey(entityType, entityId)

    // Check if already queued
    const existing = await this.storage.get<EmbeddingQueueItem>(key)
    if (existing && existing.attempts < this.config.retryAttempts) {
      // Already queued and not exhausted retries, skip
      return
    }

    const item: EmbeddingQueueItem = {
      entityType,
      entityId,
      createdAt: Date.now(),
      attempts: 0,
      priority,
    }

    await this.storage.put(key, item)

    // Schedule alarm if not already set
    await this.ensureAlarmScheduled()
  }

  /**
   * Add multiple entities to the queue
   *
   * @param items - Array of [entityType, entityId] tuples
   * @param priority - Optional priority for all items
   */
  async enqueueBatch(
    items: Array<[string, string]>,
    priority = 100
  ): Promise<void> {
    if (items.length === 0) return

    const puts = new Map<string, EmbeddingQueueItem>()
    const now = Date.now()

    for (const [entityType, entityId] of items) {
      const key = this.getQueueKey(entityType, entityId)
      puts.set(key, {
        entityType,
        entityId,
        createdAt: now,
        attempts: 0,
        priority,
      })
    }

    await this.storage.put(puts)
    await this.ensureAlarmScheduled()
  }

  /**
   * Remove an item from the queue
   *
   * @param entityType - Entity type/namespace
   * @param entityId - Entity ID
   */
  async dequeue(entityType: string, entityId: string): Promise<void> {
    const key = this.getQueueKey(entityType, entityId)
    await this.storage.delete(key)
  }

  /**
   * Remove multiple items from the queue
   *
   * @param items - Array of [entityType, entityId] tuples
   */
  async dequeueBatch(items: Array<[string, string]>): Promise<void> {
    const keys = items.map(([entityType, entityId]) =>
      this.getQueueKey(entityType, entityId)
    )
    await this.storage.delete(keys)
  }

  // ===========================================================================
  // Queue Processing
  // ===========================================================================

  /**
   * Process the embedding queue
   *
   * Called from the DO alarm handler. Processes a batch of items,
   * generates embeddings, and updates entities.
   */
  async processQueue(): Promise<QueueProcessingResult> {
    const startTime = Date.now()

    logger.debug('Processing embedding queue')

    // Get batch of pending items
    const pending = await this.storage.list<EmbeddingQueueItem>({
      prefix: EmbeddingQueue.QUEUE_PREFIX,
      limit: this.config.batchSize,
    })

    if (pending.size === 0) {
      logger.debug('Embedding queue is empty')
      return { processed: 0, failed: 0, remaining: 0, errors: [] }
    }

    const batch = Array.from(pending.entries())
      .map(([key, item]) => ({ key, ...item }))
      .filter(item => item.attempts < this.config.retryAttempts)
      // Sort by priority (lower first), then by createdAt (older first)
      .sort((a, b) => {
        const priorityDiff = (a.priority ?? DEFAULT_EMBEDDING_PRIORITY) - (b.priority ?? DEFAULT_EMBEDDING_PRIORITY)
        if (priorityDiff !== 0) return priorityDiff
        return a.createdAt - b.createdAt
      })
      .slice(0, this.config.batchSize)

    if (batch.length === 0) {
      // All items have exhausted retries, clean them up
      logger.info('Moving exhausted items to dead letter queue', { count: pending.size })
      await this.cleanupExhaustedItems(Array.from(pending.keys()))
      return { processed: 0, failed: 0, remaining: 0, errors: [] }
    }

    logger.debug('Processing embedding batch', { batchSize: batch.length })

    const result: QueueProcessingResult = {
      processed: 0,
      failed: 0,
      remaining: 0,
      errors: [],
    }

    // Load entities
    const entities = await this.loadEntities(batch)

    // Filter out entities that couldn't be loaded
    const validBatch: Array<{
      key: string
      entityType: string
      entityId: string
      entity: Record<string, unknown>
    }> = []

    for (let i = 0; i < batch.length; i++) {
      const item = batch[i]!
      const entity = entities[i]

      if (!entity) {
        // Entity not found, mark as failed and remove from queue
        logger.warn('Entity not found during embedding generation', {
          entityType: item.entityType,
          entityId: item.entityId,
        })
        await this.storage.delete(item.key)
        result.failed++
        result.errors.push({
          entityType: item.entityType,
          entityId: item.entityId,
          error: 'Entity not found',
        })

        // Notify error callback
        await this.notifyError(item, 'Entity not found', false)
        continue
      }

      validBatch.push({
        key: item.key,
        entityType: item.entityType,
        entityId: item.entityId,
        entity,
      })
    }

    if (validBatch.length === 0) {
      // Check remaining items
      const remaining = await this.storage.list({
        prefix: EmbeddingQueue.QUEUE_PREFIX,
        limit: 1,
      })
      result.remaining = remaining.size > 0 ? pending.size - batch.length : 0

      // Update metrics
      await this.updateMetrics(result.processed, result.failed, Date.now() - startTime)
      return result
    }

    // Extract text for embedding
    const texts = validBatch.map(item => this.extractText(item.entity))

    // Generate embeddings in batch
    let vectors: number[][]
    try {
      vectors = await this.config.provider.embedBatch(texts)
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Embedding generation failed'

      logger.error('Embedding batch generation failed', error, {
        batchSize: batch.length,
        entityTypes: [...new Set(batch.map(b => b.entityType))],
      })

      // Embedding generation failed, increment attempts for all items
      const updates = new Map<string, EmbeddingQueueItem>()
      for (const item of batch) {
        const newAttempts = item.attempts + 1
        const isExhausted = newAttempts >= this.config.retryAttempts

        updates.set(item.key, {
          ...item,
          attempts: newAttempts,
          lastError: errorMessage,
        })

        // Notify error callback
        await this.notifyError(item, errorMessage, isExhausted)
      }
      await this.storage.put(updates)

      // Schedule retry if there are items with remaining attempts
      const hasRetriable = batch.some(item => item.attempts + 1 < this.config.retryAttempts)
      if (hasRetriable) {
        await this.ensureAlarmScheduled(5000) // Retry after 5 seconds
      }

      result.failed = batch.length
      for (const item of batch) {
        result.errors.push({
          entityType: item.entityType,
          entityId: item.entityId,
          error: errorMessage,
        })
      }

      // Update metrics
      await this.updateMetrics(result.processed, result.failed, Date.now() - startTime)
      return result
    }

    // Update entities with vectors
    const successfulKeys: string[] = []
    for (let i = 0; i < validBatch.length; i++) {
      const item = validBatch[i]!
      const vector = vectors[i]!

      try {
        await this.updateEntity(item.entityType, item.entityId, vector)
        successfulKeys.push(item.key)
        result.processed++
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : 'Entity update failed'

        logger.error('Entity update failed during embedding', error, {
          entityType: item.entityType,
          entityId: item.entityId,
        })

        // Update failed, increment attempts
        const queueItem = batch.find(b => b.key === item.key)
        if (queueItem) {
          const newAttempts = queueItem.attempts + 1
          const isExhausted = newAttempts >= this.config.retryAttempts

          await this.storage.put(item.key, {
            ...queueItem,
            attempts: newAttempts,
            lastError: errorMessage,
          })

          // Notify error callback
          await this.notifyError(queueItem, errorMessage, isExhausted)
        }
        result.failed++
        result.errors.push({
          entityType: item.entityType,
          entityId: item.entityId,
          error: errorMessage,
        })
      }
    }

    // Remove successfully processed items from queue
    if (successfulKeys.length > 0) {
      await this.storage.delete(successfulKeys)
    }

    // Check if more items remain
    const remainingItems = await this.storage.list({
      prefix: EmbeddingQueue.QUEUE_PREFIX,
      limit: 1,
    })

    result.remaining = remainingItems.size
    if (result.remaining > 0) {
      // Schedule next batch processing
      await this.ensureAlarmScheduled(100) // Process next batch quickly
    }

    const processingTime = Date.now() - startTime

    logger.info('Embedding queue batch processed', {
      processed: result.processed,
      failed: result.failed,
      remaining: result.remaining,
      processingTimeMs: processingTime,
    })

    // Update metrics
    await this.updateMetrics(result.processed, result.failed, processingTime)

    return result
  }

  // ===========================================================================
  // Queue Management
  // ===========================================================================

  /**
   * Get queue statistics
   */
  async getStats(): Promise<QueueStats> {
    const items = await this.storage.list<EmbeddingQueueItem>({
      prefix: EmbeddingQueue.QUEUE_PREFIX,
    })

    let pending = 0
    let retrying = 0
    let oldestItem: number | undefined

    for (const item of items.values()) {
      if (item.attempts === 0) {
        pending++
      } else {
        retrying++
      }
      if (!oldestItem || item.createdAt < oldestItem) {
        oldestItem = item.createdAt
      }
    }

    return {
      total: items.size,
      pending,
      retrying,
      oldestItem,
    }
  }

  /**
   * Get all items in the queue
   *
   * @param limit - Maximum number of items to return
   */
  async getItems(limit = 100): Promise<EmbeddingQueueItem[]> {
    const items = await this.storage.list<EmbeddingQueueItem>({
      prefix: EmbeddingQueue.QUEUE_PREFIX,
      limit,
    })

    return Array.from(items.values())
  }

  /**
   * Clear all items from the queue
   */
  async clear(): Promise<number> {
    const items = await this.storage.list({
      prefix: EmbeddingQueue.QUEUE_PREFIX,
    })

    const keys = Array.from(items.keys())
    if (keys.length > 0) {
      await this.storage.delete(keys)
    }

    return keys.length
  }

  /**
   * Clear items that have exhausted all retry attempts
   */
  async clearFailed(): Promise<number> {
    const items = await this.storage.list<EmbeddingQueueItem>({
      prefix: EmbeddingQueue.QUEUE_PREFIX,
    })

    const failedKeys: string[] = []
    for (const [key, item] of items.entries()) {
      if (item.attempts >= this.config.retryAttempts) {
        failedKeys.push(key)
      }
    }

    if (failedKeys.length > 0) {
      await this.storage.delete(failedKeys)
    }

    return failedKeys.length
  }

  // ===========================================================================
  // Helper Methods
  // ===========================================================================

  /**
   * Generate queue key for an entity
   */
  private getQueueKey(entityType: string, entityId: string): string {
    return `${EmbeddingQueue.QUEUE_PREFIX}${entityType}:${entityId}`
  }

  /**
   * Ensure an alarm is scheduled for processing
   */
  private async ensureAlarmScheduled(delay?: number): Promise<void> {
    const currentAlarm = await this.storage.getAlarm()
    if (!currentAlarm) {
      const processDelay = delay ?? this.config.processDelay
      await this.storage.setAlarm(Date.now() + processDelay)
    }
  }

  /**
   * Load entities for a batch of queue items
   */
  private async loadEntities(
    batch: Array<{ entityType: string; entityId: string }>
  ): Promise<Array<Record<string, unknown> | null>> {
    if (!this.entityLoader) {
      throw new Error('Entity loader not set. Call setEntityLoader() first.')
    }

    return Promise.all(
      batch.map(item => this.entityLoader!(item.entityType, item.entityId))
    )
  }

  /**
   * Extract text from entity for embedding
   */
  private extractText(entity: Record<string, unknown>): string {
    const texts: string[] = []

    for (const field of this.config.fields) {
      const value = this.getNestedValue(entity, field)
      if (typeof value === 'string' && value.trim()) {
        texts.push(value.trim())
      }
    }

    return texts.join(this.config.fieldSeparator)
  }

  /**
   * Get nested value from object using dot notation
   */
  private getNestedValue(obj: Record<string, unknown>, path: string): unknown {
    const parts = path.split('.')
    let current: unknown = obj

    for (const part of parts) {
      if (current === null || current === undefined) return undefined
      if (typeof current !== 'object') return undefined
      current = (current as Record<string, unknown>)[part]
    }

    return current
  }

  /**
   * Update entity with embedding vector
   */
  private async updateEntity(
    entityType: string,
    entityId: string,
    vector: number[]
  ): Promise<void> {
    if (!this.entityUpdater) {
      throw new Error('Entity updater not set. Call setEntityUpdater() first.')
    }

    await this.entityUpdater(entityType, entityId, this.config.vectorField, vector)
  }

  /**
   * Clean up items that have exhausted all retry attempts
   * Moves them to the dead letter queue if enabled
   */
  private async cleanupExhaustedItems(keys: string[]): Promise<void> {
    const exhausted: string[] = []
    const items = await this.storage.get<EmbeddingQueueItem>(keys)
    const deadLetterPuts = new Map<string, DeadLetterItem>()
    const now = Date.now()

    for (const [key, item] of Object.entries(items)) {
      if (item && item.attempts >= this.config.retryAttempts) {
        exhausted.push(key)

        // Move to dead letter queue if enabled
        if (this.config.enableDeadLetter) {
          const dlqKey = this.getDeadLetterKey(item.entityType, item.entityId)
          deadLetterPuts.set(dlqKey, {
            entityType: item.entityType,
            entityId: item.entityId,
            createdAt: item.createdAt,
            movedAt: now,
            attempts: item.attempts,
            lastError: item.lastError || 'Unknown error',
            priority: item.priority,
          })

          logger.warn('Item moved to dead letter queue', {
            entityType: item.entityType,
            entityId: item.entityId,
            attempts: item.attempts,
            lastError: item.lastError,
          })

          // Notify error callback that item is exhausted
          await this.notifyError(item, item.lastError || 'Max retries exhausted', true)
        }
      }
    }

    // Write to dead letter queue first, then delete from main queue
    if (deadLetterPuts.size > 0) {
      await this.storage.put(deadLetterPuts)
    }

    if (exhausted.length > 0) {
      await this.storage.delete(exhausted)
    }
  }

  /**
   * Generate dead letter queue key for an entity
   */
  private getDeadLetterKey(entityType: string, entityId: string): string {
    return `${EmbeddingQueue.DEAD_LETTER_PREFIX}${entityType}:${entityId}`
  }

  /**
   * Notify error callback if configured
   */
  private async notifyError(
    item: EmbeddingQueueItem,
    error: Error | string,
    isExhausted: boolean
  ): Promise<void> {
    if (this.config.onError) {
      try {
        await this.config.onError(item, error, isExhausted)
      } catch (callbackError) {
        logger.error('Error callback failed', callbackError, {
          entityType: item.entityType,
          entityId: item.entityId,
        })
      }
    }
  }

  /**
   * Update stored metrics
   */
  private async updateMetrics(
    processed: number,
    failed: number,
    processingTimeMs: number
  ): Promise<void> {
    const existing = await this.storage.get<StoredMetrics>(EmbeddingQueue.METRICS_KEY)

    const metrics: StoredMetrics = {
      totalProcessed: (existing?.totalProcessed ?? 0) + processed,
      totalFailed: (existing?.totalFailed ?? 0) + failed,
      avgProcessingTimeMs: processingTimeMs, // Store last batch time
      lastUpdated: Date.now(),
    }

    await this.storage.put(EmbeddingQueue.METRICS_KEY, metrics)
  }

  // ===========================================================================
  // Metrics and Monitoring
  // ===========================================================================

  /**
   * Get comprehensive queue metrics for monitoring
   */
  async getMetrics(): Promise<QueueMetrics> {
    // Get queue items
    const queueItems = await this.storage.list<EmbeddingQueueItem>({
      prefix: EmbeddingQueue.QUEUE_PREFIX,
    })

    // Get dead letter items
    const dlqItems = await this.storage.list<DeadLetterItem>({
      prefix: EmbeddingQueue.DEAD_LETTER_PREFIX,
    })

    // Get stored metrics
    const storedMetrics = await this.storage.get<StoredMetrics>(EmbeddingQueue.METRICS_KEY)

    // Calculate backlog age
    let backlogAgeMs = 0
    const now = Date.now()
    for (const item of queueItems.values()) {
      const age = now - item.createdAt
      if (age > backlogAgeMs) {
        backlogAgeMs = age
      }
    }

    // Calculate error rate
    const totalProcessed = storedMetrics?.totalProcessed ?? 0
    const totalFailed = storedMetrics?.totalFailed ?? 0
    const total = totalProcessed + totalFailed
    const errorRate = total > 0 ? totalFailed / total : 0

    return {
      queueDepth: queueItems.size,
      totalProcessed,
      totalFailed,
      deadLetterCount: dlqItems.size,
      avgProcessingTimeMs: storedMetrics?.avgProcessingTimeMs ?? 0,
      errorRate,
      lastUpdated: storedMetrics?.lastUpdated ?? 0,
      backlogAgeMs,
    }
  }

  /**
   * Reset metrics counters
   */
  async resetMetrics(): Promise<void> {
    await this.storage.delete(EmbeddingQueue.METRICS_KEY)
    logger.info('Embedding queue metrics reset')
  }

  // ===========================================================================
  // Dead Letter Queue Management
  // ===========================================================================

  /**
   * Get all items in the dead letter queue
   *
   * @param limit - Maximum number of items to return
   */
  async getDeadLetterItems(limit = 100): Promise<DeadLetterItem[]> {
    const items = await this.storage.list<DeadLetterItem>({
      prefix: EmbeddingQueue.DEAD_LETTER_PREFIX,
      limit,
    })

    return Array.from(items.values())
  }

  /**
   * Get dead letter queue count
   */
  async getDeadLetterCount(): Promise<number> {
    const items = await this.storage.list({
      prefix: EmbeddingQueue.DEAD_LETTER_PREFIX,
    })
    return items.size
  }

  /**
   * Retry a specific item from the dead letter queue
   *
   * @param entityType - Entity type/namespace
   * @param entityId - Entity ID
   * @param priority - Optional new priority
   */
  async retryDeadLetterItem(
    entityType: string,
    entityId: string,
    priority?: number
  ): Promise<boolean> {
    const dlqKey = this.getDeadLetterKey(entityType, entityId)
    const item = await this.storage.get<DeadLetterItem>(dlqKey)

    if (!item) {
      logger.warn('Dead letter item not found for retry', { entityType, entityId })
      return false
    }

    // Remove from dead letter queue
    await this.storage.delete(dlqKey)

    // Re-enqueue with fresh attempts
    await this.enqueue(entityType, entityId, priority ?? item.priority ?? DEFAULT_EMBEDDING_PRIORITY)

    logger.info('Dead letter item re-queued for retry', {
      entityType,
      entityId,
      originalAttempts: item.attempts,
    })

    return true
  }

  /**
   * Retry all items in the dead letter queue
   *
   * @param limit - Maximum number of items to retry
   */
  async retryAllDeadLetterItems(limit = 100): Promise<number> {
    const items = await this.storage.list<DeadLetterItem>({
      prefix: EmbeddingQueue.DEAD_LETTER_PREFIX,
      limit,
    })

    let retried = 0
    const dlqKeysToDelete: string[] = []
    const queueItems = new Map<string, EmbeddingQueueItem>()
    const now = Date.now()

    for (const [dlqKey, item] of items.entries()) {
      const queueKey = this.getQueueKey(item.entityType, item.entityId)
      queueItems.set(queueKey, {
        entityType: item.entityType,
        entityId: item.entityId,
        createdAt: now,
        attempts: 0,
        priority: item.priority,
      })
      dlqKeysToDelete.push(dlqKey)
      retried++
    }

    if (retried > 0) {
      // Add to main queue
      await this.storage.put(queueItems)
      // Remove from dead letter queue
      await this.storage.delete(dlqKeysToDelete)
      // Schedule processing
      await this.ensureAlarmScheduled()

      logger.info('Dead letter items re-queued for retry', { count: retried })
    }

    return retried
  }

  /**
   * Clear all items from the dead letter queue
   */
  async clearDeadLetterQueue(): Promise<number> {
    const items = await this.storage.list({
      prefix: EmbeddingQueue.DEAD_LETTER_PREFIX,
    })

    const keys = Array.from(items.keys())
    if (keys.length > 0) {
      await this.storage.delete(keys)
      logger.info('Dead letter queue cleared', { count: keys.length })
    }

    return keys.length
  }

  /**
   * Delete a specific item from the dead letter queue
   *
   * @param entityType - Entity type/namespace
   * @param entityId - Entity ID
   */
  async deleteDeadLetterItem(entityType: string, entityId: string): Promise<boolean> {
    const dlqKey = this.getDeadLetterKey(entityType, entityId)
    const item = await this.storage.get<DeadLetterItem>(dlqKey)

    if (!item) {
      return false
    }

    await this.storage.delete(dlqKey)
    logger.debug('Dead letter item deleted', { entityType, entityId })
    return true
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a background embedding queue
 *
 * @param storage - Durable Object storage
 * @param config - Queue configuration
 * @returns Configured EmbeddingQueue instance
 */
export function createEmbeddingQueue(
  storage: DurableObjectStorage,
  config: BackgroundEmbeddingConfig
): EmbeddingQueue {
  return new EmbeddingQueue(storage, config)
}

/**
 * Configuration builder for background embeddings
 */
export class BackgroundEmbeddingConfigBuilder {
  private config: Partial<BackgroundEmbeddingConfig> = {}

  /**
   * Set the embedding provider
   */
  provider(provider: EmbeddingProvider): this {
    this.config.provider = provider
    return this
  }

  /**
   * Set the source fields to embed
   */
  fields(fields: string[]): this {
    this.config.fields = fields
    return this
  }

  /**
   * Set the target vector field
   */
  vectorField(field: string): this {
    this.config.vectorField = field
    return this
  }

  /**
   * Set the batch size
   */
  batchSize(size: number): this {
    this.config.batchSize = size
    return this
  }

  /**
   * Set the retry attempts
   */
  retryAttempts(attempts: number): this {
    this.config.retryAttempts = attempts
    return this
  }

  /**
   * Set the process delay
   */
  processDelay(delay: number): this {
    this.config.processDelay = delay
    return this
  }

  /**
   * Set the field separator
   */
  fieldSeparator(separator: string): this {
    this.config.fieldSeparator = separator
    return this
  }

  /**
   * Set the error callback
   */
  onError(callback: ErrorCallback): this {
    this.config.onError = callback
    return this
  }

  /**
   * Enable or disable the dead letter queue
   */
  enableDeadLetter(enable: boolean): this {
    this.config.enableDeadLetter = enable
    return this
  }

  /**
   * Build the configuration
   */
  build(): BackgroundEmbeddingConfig {
    if (!this.config.provider) {
      throw new Error('Embedding provider is required')
    }
    if (!this.config.fields || this.config.fields.length === 0) {
      throw new Error('At least one source field is required')
    }
    if (!this.config.vectorField) {
      throw new Error('Vector field is required')
    }

    return this.config as BackgroundEmbeddingConfig
  }
}

/**
 * Create a configuration builder
 */
export function configureBackgroundEmbeddings(): BackgroundEmbeddingConfigBuilder {
  return new BackgroundEmbeddingConfigBuilder()
}
