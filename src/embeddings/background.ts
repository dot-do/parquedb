/**
 * Background Embedding Generation for ParqueDB
 *
 * Provides asynchronous embedding generation using Cloudflare Durable Object
 * Alarms. When entities are created or updated, they can be queued for
 * background embedding generation instead of blocking the write operation.
 *
 * @example
 * ```typescript
 * // In ParqueDBDO
 * const embeddingQueue = new EmbeddingQueue(this.ctx.storage, {
 *   provider: createWorkersAIProvider(env.AI),
 *   fields: ['description', 'content'],
 *   vectorField: 'embedding',
 *   batchSize: 10
 * })
 *
 * // On entity create/update
 * await embeddingQueue.enqueue('posts', 'abc123')
 *
 * // In alarm handler
 * async alarm() {
 *   await embeddingQueue.processQueue()
 * }
 * ```
 */

import type { EmbeddingProvider } from './provider'

// =============================================================================
// Types
// =============================================================================

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
 * Queue for background embedding generation
 *
 * Uses Durable Object storage for persistence and alarms for processing.
 * Items are stored with a prefix to enable efficient listing and cleanup.
 */
export class EmbeddingQueue {
  /** Storage interface (DurableObjectStorage) */
  private storage: DurableObjectStorage

  /** Queue configuration */
  private config: Required<BackgroundEmbeddingConfig>

  /** Function to load entity data */
  private entityLoader?: EntityLoader

  /** Function to update entity with embedding */
  private entityUpdater?: EntityUpdater

  /** Queue key prefix */
  private static readonly QUEUE_PREFIX = 'embed_queue:'

  /** Stats key */
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
      batchSize: config.batchSize ?? 10,
      retryAttempts: config.retryAttempts ?? 3,
      processDelay: config.processDelay ?? 1000,
      fieldSeparator: config.fieldSeparator ?? '\n\n',
    }
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
  async enqueue(entityType: string, entityId: string, priority = 100): Promise<void> {
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
    // Get batch of pending items
    const pending = await this.storage.list<EmbeddingQueueItem>({
      prefix: EmbeddingQueue.QUEUE_PREFIX,
      limit: this.config.batchSize,
    })

    if (pending.size === 0) {
      return { processed: 0, failed: 0, remaining: 0, errors: [] }
    }

    const batch = Array.from(pending.entries())
      .map(([key, item]) => ({ key, ...item }))
      .filter(item => item.attempts < this.config.retryAttempts)
      // Sort by priority (lower first), then by createdAt (older first)
      .sort((a, b) => {
        const priorityDiff = (a.priority ?? 100) - (b.priority ?? 100)
        if (priorityDiff !== 0) return priorityDiff
        return a.createdAt - b.createdAt
      })
      .slice(0, this.config.batchSize)

    if (batch.length === 0) {
      // All items have exhausted retries, clean them up
      await this.cleanupExhaustedItems(Array.from(pending.keys()))
      return { processed: 0, failed: 0, remaining: 0, errors: [] }
    }

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
        await this.storage.delete(item.key)
        result.failed++
        result.errors.push({
          entityType: item.entityType,
          entityId: item.entityId,
          error: 'Entity not found',
        })
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
      return result
    }

    // Extract text for embedding
    const texts = validBatch.map(item => this.extractText(item.entity))

    // Generate embeddings in batch
    let vectors: number[][]
    try {
      vectors = await this.config.provider.embedBatch(texts)
    } catch (error) {
      // Embedding generation failed, increment attempts for all items
      const updates = new Map<string, EmbeddingQueueItem>()
      for (const item of batch) {
        updates.set(item.key, {
          ...item,
          attempts: item.attempts + 1,
          lastError: error instanceof Error ? error.message : 'Embedding generation failed',
        })
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
          error: error instanceof Error ? error.message : 'Embedding generation failed',
        })
      }
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
        // Update failed, increment attempts
        const queueItem = batch.find(b => b.key === item.key)
        if (queueItem) {
          await this.storage.put(item.key, {
            ...queueItem,
            attempts: queueItem.attempts + 1,
            lastError: error instanceof Error ? error.message : 'Entity update failed',
          })
        }
        result.failed++
        result.errors.push({
          entityType: item.entityType,
          entityId: item.entityId,
          error: error instanceof Error ? error.message : 'Entity update failed',
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
   */
  private async cleanupExhaustedItems(keys: string[]): Promise<void> {
    const exhausted: string[] = []
    const items = await this.storage.get<EmbeddingQueueItem>(keys)

    for (const [key, item] of Object.entries(items)) {
      if (item && item.attempts >= this.config.retryAttempts) {
        exhausted.push(key)
      }
    }

    if (exhausted.length > 0) {
      await this.storage.delete(exhausted)
    }
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
