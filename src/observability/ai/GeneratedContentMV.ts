/**
 * GeneratedContentMV - Materialized View for Generated Content Tracking
 *
 * Provides tracking and analysis of AI-generated content including:
 * - Content metadata and references storage
 * - Content versioning support
 * - Filtering by model, content type, time range
 * - Aggregation and statistics
 * - Version history tracking
 *
 * Unlike the streaming GeneratedContentMV (in src/streaming), this MV uses
 * ParqueDB collections for storage, enabling richer querying and relationships.
 *
 * @example
 * ```typescript
 * import { GeneratedContentMV } from 'parquedb/observability'
 * import { DB } from 'parquedb'
 *
 * const db = DB()
 * const contentMV = new GeneratedContentMV(db, {
 *   collection: 'generated_content',
 *   maxAgeMs: 30 * 24 * 60 * 60 * 1000, // 30 days
 * })
 *
 * // Record content
 * await contentMV.record({
 *   modelId: 'gpt-4',
 *   providerId: 'openai',
 *   contentType: 'text',
 *   content: 'Generated response...',
 *   tokenCount: 150,
 * })
 *
 * // Query content
 * const content = await contentMV.find({
 *   modelId: 'gpt-4',
 *   from: new Date('2026-02-01'),
 * })
 *
 * // Create a new version
 * const newVersionId = await contentMV.createVersion(content[0].$id, {
 *   content: 'Revised response...',
 *   versionReason: 'user_edit',
 * })
 *
 * // Get version history
 * const history = await contentMV.getVersionHistory(newVersionId)
 * ```
 *
 * @module observability/ai/GeneratedContentMV
 */

import type { ParqueDB } from '../../ParqueDB'
import type { ModelPricing } from './types'
import { DEFAULT_MODEL_PRICING } from './types'
import {
  DEFAULT_CONTENT_RETENTION_MS,
  MAX_BATCH_SIZE,
  DEFAULT_QUERY_LIMIT,
  DJB2_INITIAL,
} from '../../constants'

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_COLLECTION = 'generated_content'
const DEFAULT_MAX_AGE_MS = DEFAULT_CONTENT_RETENTION_MS
const DEFAULT_BATCH_SIZE = MAX_BATCH_SIZE
const DEFAULT_LIMIT = DEFAULT_QUERY_LIMIT

// =============================================================================
// Types
// =============================================================================

/**
 * Type of generated content
 */
export type GeneratedContentType =
  | 'text'              // Plain text response
  | 'code'              // Code/programming content
  | 'json'              // Structured JSON object
  | 'markdown'          // Markdown formatted text
  | 'html'              // HTML content
  | 'tool_call'         // Tool/function call
  | 'tool_result'       // Tool/function result
  | 'image_description' // Image description/alt text
  | 'embedding'         // Vector embedding
  | 'other'             // Other/unknown content type

/**
 * Content classification for safety/sensitivity
 */
export type ContentClassification =
  | 'safe'              // Safe content
  | 'sensitive'         // May contain sensitive information
  | 'pii'               // Contains personally identifiable information
  | 'flagged'           // Flagged for review
  | 'unclassified'      // Not yet classified

/**
 * Finish reason from the model
 */
export type FinishReason =
  | 'stop'              // Natural completion
  | 'length'            // Hit max token limit
  | 'tool_calls'        // Model wants to call tools
  | 'content_filter'    // Filtered by safety system
  | 'error'             // Error during generation
  | 'unknown'           // Unknown reason

/**
 * AI provider identifier
 */
export type AIProvider =
  | 'openai'
  | 'anthropic'
  | 'google'
  | 'mistral'
  | 'groq'
  | 'cohere'
  | 'replicate'
  | 'huggingface'
  | 'ollama'
  | 'azure'
  | 'aws-bedrock'
  | 'workers-ai'
  | 'custom'
  | string

/**
 * Stored generated content record
 */
export interface GeneratedContentRecord {
  /** Unique content ID */
  $id: string
  /** Type identifier */
  $type: 'GeneratedContent'
  /** Display name */
  name: string
  /** Content ID (user-facing, may be custom) */
  contentId: string
  /** Request ID for correlation with AI requests */
  requestId: string
  /** Content timestamp */
  timestamp: Date
  /** Model identifier */
  modelId: string
  /** Provider identifier */
  providerId: AIProvider
  /** Type of content */
  contentType: GeneratedContentType
  /** The generated content */
  content: string
  /** Content length in characters */
  contentLength: number
  /** Token count for the generated content */
  tokenCount: number
  /** Prompt token count */
  promptTokenCount: number
  /** Total token count */
  totalTokenCount: number
  /** Estimated cost in USD */
  estimatedCost: number
  /** Finish reason */
  finishReason: FinishReason
  /** Latency in milliseconds */
  latencyMs: number
  /** Whether this was a streaming response */
  isStreaming: boolean
  /** Whether this was served from cache */
  isCached: boolean
  /** Content classification */
  classification: ContentClassification
  /** Content hash for deduplication */
  contentHash?: string
  /** Tool name (if contentType is tool_call or tool_result) */
  toolName?: string
  /** Tool call ID for correlation */
  toolCallId?: string
  /** Language detected in the content */
  language?: string
  /** Session/conversation ID */
  sessionId?: string
  /** User identifier */
  userId?: string
  /** Application identifier */
  appId?: string
  /** Environment */
  environment?: string
  /** Custom metadata */
  metadata?: Record<string, unknown>
  /** When the record was created */
  createdAt: Date
  // Versioning fields
  /** Version number (1 = first version) */
  version: number
  /** ID of the parent content (null for first version) */
  parentContentId?: string
  /** Root content ID (tracks the original content in a version chain) */
  rootContentId?: string
  /** Reason for creating this version */
  versionReason?: string
}

/**
 * Input for recording generated content
 */
export interface RecordContentInput {
  /** Model identifier */
  modelId: string
  /** Provider identifier */
  providerId: AIProvider
  /** Type of content */
  contentType: GeneratedContentType
  /** The generated content (string or object to be serialized) */
  content: string | Record<string, unknown>
  /** Request ID for correlation */
  requestId?: string
  /** Token count */
  tokenCount?: number
  /** Prompt token count */
  promptTokenCount?: number
  /** Total token count */
  totalTokenCount?: number
  /** Finish reason */
  finishReason?: FinishReason
  /** Latency in milliseconds */
  latencyMs?: number
  /** Whether this was a streaming response */
  isStreaming?: boolean
  /** Whether this was served from cache */
  isCached?: boolean
  /** Content classification */
  classification?: ContentClassification
  /** Tool name */
  toolName?: string
  /** Tool call ID */
  toolCallId?: string
  /** Language detected */
  language?: string
  /** Session/conversation ID */
  sessionId?: string
  /** User identifier */
  userId?: string
  /** Application identifier */
  appId?: string
  /** Environment */
  environment?: string
  /** Custom content ID */
  contentId?: string
  /** Custom timestamp */
  timestamp?: Date
  /** Custom cost override */
  estimatedCost?: number
  /** Custom metadata */
  metadata?: Record<string, unknown>
  // Versioning fields
  /** Parent content ID (for creating versions) */
  parentContentId?: string
  /** Root content ID */
  rootContentId?: string
  /** Reason for creating this version */
  versionReason?: string
}

/**
 * Query options for finding generated content
 */
export interface ContentQueryOptions {
  /** Filter by model ID */
  modelId?: string
  /** Filter by provider ID */
  providerId?: AIProvider
  /** Filter by content type */
  contentType?: GeneratedContentType
  /** Filter by classification */
  classification?: ContentClassification
  /** Filter by finish reason */
  finishReason?: FinishReason
  /** Filter by user ID */
  userId?: string
  /** Filter by app ID */
  appId?: string
  /** Filter by session ID */
  sessionId?: string
  /** Filter by environment */
  environment?: string
  /** Start time (inclusive) */
  from?: Date
  /** End time (exclusive) */
  to?: Date
  /** Include only cached content */
  cachedOnly?: boolean
  /** Include only streaming content */
  streamingOnly?: boolean
  /** Filter by root content ID (get all versions) */
  rootContentId?: string
  /** Maximum results */
  limit?: number
  /** Skip first N results */
  offset?: number
  /** Sort field */
  sort?: 'timestamp' | '-timestamp' | 'contentLength' | '-contentLength' | 'estimatedCost' | '-estimatedCost' | 'version' | '-version'
}

/**
 * Statistics for generated content
 */
export interface ContentStats {
  /** Total number of content records */
  totalRecords: number
  /** Unique content count (by rootContentId) */
  uniqueContentCount: number
  /** Total versions created */
  totalVersions: number
  /** Average versions per content */
  avgVersionsPerContent: number
  /** Cached content count */
  cacheHits: number
  /** Cache hit ratio (0-1) */
  cacheHitRatio: number
  /** Token statistics */
  tokens: {
    totalTokens: number
    totalPromptTokens: number
    avgTokensPerContent: number
  }
  /** Cost statistics */
  cost: {
    totalCost: number
    avgCost: number
    cacheSavings: number
  }
  /** Content length statistics */
  contentLength: {
    total: number
    avg: number
    min: number
    max: number
  }
  /** Breakdown by content type */
  byContentType: Record<string, number>
  /** Breakdown by model */
  byModel: Record<string, { count: number; cost: number; avgLength: number }>
  /** Breakdown by provider */
  byProvider: Record<string, { count: number; cost: number; avgLength: number }>
  /** Breakdown by classification */
  byClassification: Record<string, number>
  /** Time range */
  timeRange: { from: Date; to: Date }
}

/**
 * Configuration for GeneratedContentMV
 */
export interface GeneratedContentMVConfig {
  /** Collection name for storing content (default: 'generated_content') */
  collection?: string
  /** Maximum age of content to keep (default: 30 days) */
  maxAgeMs?: number
  /** Batch size for operations (default: 1000) */
  batchSize?: number
  /** Custom model pricing */
  customPricing?: ModelPricing[]
  /** Whether to merge with default pricing (default: true) */
  mergeWithDefaultPricing?: boolean
  /** Enable debug logging */
  debug?: boolean
}

/**
 * Resolved configuration with defaults
 */
export interface ResolvedContentMVConfig {
  collection: string
  maxAgeMs: number
  batchSize: number
  pricing: Map<string, ModelPricing>
  debug: boolean
}

/**
 * Result of a cleanup operation
 */
export interface CleanupResult {
  /** Whether cleanup completed successfully */
  success: boolean
  /** Total records deleted */
  deletedCount: number
  /** Duration of cleanup in milliseconds */
  durationMs: number
  /** Error message if failed */
  error?: string
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Generate a unique content ID
 */
export function generateContentId(): string {
  const timestamp = Date.now().toString(36)
  const random = Math.random().toString(36).substring(2, 10)
  return `gc_${timestamp}_${random}`
}

/**
 * Simple hash function for content (djb2 algorithm)
 */
export function hashContent(content: string): string {
  let hash = DJB2_INITIAL
  for (let i = 0; i < content.length; i++) {
    hash = ((hash << 5) + hash) ^ content.charCodeAt(i)
  }
  return (hash >>> 0).toString(36)
}

/**
 * Build pricing lookup map
 */
function buildPricingMap(
  customPricing: ModelPricing[] | undefined,
  mergeWithDefault: boolean
): Map<string, ModelPricing> {
  const map = new Map<string, ModelPricing>()

  if (mergeWithDefault) {
    for (const pricing of DEFAULT_MODEL_PRICING) {
      const key = `${pricing.modelId}:${pricing.providerId}`
      map.set(key, pricing)
    }
  }

  if (customPricing) {
    for (const pricing of customPricing) {
      const key = `${pricing.modelId}:${pricing.providerId}`
      map.set(key, pricing)
    }
  }

  return map
}

/**
 * Calculate cost from tokens and pricing
 */
function calculateCost(
  modelId: string,
  providerId: string,
  promptTokens: number,
  completionTokens: number,
  pricingMap: Map<string, ModelPricing>
): number {
  const pricing = pricingMap.get(`${modelId}:${providerId}`)
  if (!pricing) {
    // Try to find by partial match
    for (const [key, p] of pricingMap) {
      if (key.startsWith(modelId) || modelId.startsWith(key.split(':')[0]!)) {
        const inputCost = (promptTokens / 1_000_000) * p.inputPricePerMillion
        const outputCost = (completionTokens / 1_000_000) * p.outputPricePerMillion
        return inputCost + outputCost
      }
    }
    return 0
  }

  const inputCost = (promptTokens / 1_000_000) * pricing.inputPricePerMillion
  const outputCost = (completionTokens / 1_000_000) * pricing.outputPricePerMillion
  return inputCost + outputCost
}

/**
 * Resolve configuration with defaults
 */
function resolveConfig(config: GeneratedContentMVConfig): ResolvedContentMVConfig {
  return {
    collection: config.collection ?? DEFAULT_COLLECTION,
    maxAgeMs: config.maxAgeMs ?? DEFAULT_MAX_AGE_MS,
    batchSize: config.batchSize ?? DEFAULT_BATCH_SIZE,
    pricing: buildPricingMap(config.customPricing, config.mergeWithDefaultPricing ?? true),
    debug: config.debug ?? false,
  }
}

// =============================================================================
// GeneratedContentMV Class
// =============================================================================

/**
 * GeneratedContentMV - Materialized View for Generated Content Tracking
 *
 * Provides comprehensive tracking and analysis of AI-generated content
 * with support for content versioning and rich querying.
 */
export class GeneratedContentMV {
  private readonly db: ParqueDB
  private readonly config: ResolvedContentMVConfig

  /**
   * Create a new GeneratedContentMV instance
   *
   * @param db - ParqueDB instance
   * @param config - Configuration options
   */
  constructor(db: ParqueDB, config: GeneratedContentMVConfig = {}) {
    this.db = db
    this.config = resolveConfig(config)
  }

  /**
   * Record a single generated content
   *
   * @param input - Content data
   * @returns Created content record
   */
  async record(input: RecordContentInput): Promise<GeneratedContentRecord> {
    const collection = this.db.collection(this.config.collection)
    const now = new Date()

    // Serialize content if it's an object
    const contentStr = typeof input.content === 'string'
      ? input.content
      : JSON.stringify(input.content)

    const tokenCount = input.tokenCount ?? 0
    const promptTokenCount = input.promptTokenCount ?? 0
    const completionTokens = tokenCount
    const totalTokenCount = input.totalTokenCount ?? (promptTokenCount + tokenCount)

    const estimatedCost = input.estimatedCost ?? calculateCost(
      input.modelId,
      input.providerId,
      promptTokenCount,
      completionTokens,
      this.config.pricing
    )

    const contentId = input.contentId ?? generateContentId()
    const timestamp = input.timestamp ?? now

    // Handle versioning
    const isNewVersion = !!input.parentContentId
    const version = isNewVersion ? 2 : 1 // Will be overwritten if parent exists
    const rootContentId = input.rootContentId ?? (isNewVersion ? undefined : contentId)

    const data: Omit<GeneratedContentRecord, '$id'> = {
      $type: 'GeneratedContent',
      name: contentId,
      contentId,
      requestId: input.requestId ?? contentId,
      timestamp,
      modelId: input.modelId,
      providerId: input.providerId,
      contentType: input.contentType,
      content: contentStr,
      contentLength: contentStr.length,
      tokenCount,
      promptTokenCount,
      totalTokenCount,
      estimatedCost,
      finishReason: input.finishReason ?? 'unknown',
      latencyMs: input.latencyMs ?? 0,
      isStreaming: input.isStreaming ?? false,
      isCached: input.isCached ?? false,
      classification: input.classification ?? 'unclassified',
      contentHash: hashContent(contentStr),
      toolName: input.toolName,
      toolCallId: input.toolCallId,
      language: input.language,
      sessionId: input.sessionId,
      userId: input.userId,
      appId: input.appId,
      environment: input.environment,
      metadata: input.metadata,
      createdAt: now,
      version,
      parentContentId: input.parentContentId,
      rootContentId,
      versionReason: input.versionReason,
    }

    const created = await collection.create(data as Record<string, unknown>)
    return created as unknown as GeneratedContentRecord
  }

  /**
   * Record multiple content records in batch
   *
   * @param inputs - Array of content data
   * @returns Array of created content records
   */
  async recordMany(inputs: RecordContentInput[]): Promise<GeneratedContentRecord[]> {
    const collection = this.db.collection(this.config.collection)
    const now = new Date()

    const records = inputs.map(input => {
      const contentStr = typeof input.content === 'string'
        ? input.content
        : JSON.stringify(input.content)

      const tokenCount = input.tokenCount ?? 0
      const promptTokenCount = input.promptTokenCount ?? 0
      const completionTokens = tokenCount
      const totalTokenCount = input.totalTokenCount ?? (promptTokenCount + tokenCount)

      const estimatedCost = input.estimatedCost ?? calculateCost(
        input.modelId,
        input.providerId,
        promptTokenCount,
        completionTokens,
        this.config.pricing
      )

      const contentId = input.contentId ?? generateContentId()
      const timestamp = input.timestamp ?? now

      const isNewVersion = !!input.parentContentId
      const version = isNewVersion ? 2 : 1
      const rootContentId = input.rootContentId ?? (isNewVersion ? undefined : contentId)

      return {
        $type: 'GeneratedContent',
        name: contentId,
        contentId,
        requestId: input.requestId ?? contentId,
        timestamp,
        modelId: input.modelId,
        providerId: input.providerId,
        contentType: input.contentType,
        content: contentStr,
        contentLength: contentStr.length,
        tokenCount,
        promptTokenCount,
        totalTokenCount,
        estimatedCost,
        finishReason: input.finishReason ?? 'unknown',
        latencyMs: input.latencyMs ?? 0,
        isStreaming: input.isStreaming ?? false,
        isCached: input.isCached ?? false,
        classification: input.classification ?? 'unclassified',
        contentHash: hashContent(contentStr),
        toolName: input.toolName,
        toolCallId: input.toolCallId,
        language: input.language,
        sessionId: input.sessionId,
        userId: input.userId,
        appId: input.appId,
        environment: input.environment,
        metadata: input.metadata,
        createdAt: now,
        version,
        parentContentId: input.parentContentId,
        rootContentId,
        versionReason: input.versionReason,
      }
    })

    const created = await collection.createMany(records as Record<string, unknown>[])
    return created as unknown as GeneratedContentRecord[]
  }

  /**
   * Find content records matching the query options
   *
   * @param options - Query options
   * @returns Array of matching content records
   */
  async find(options: ContentQueryOptions = {}): Promise<GeneratedContentRecord[]> {
    const collection = this.db.collection(this.config.collection)
    const filter: Record<string, unknown> = {}

    // Apply filters
    if (options.modelId) {
      filter.modelId = options.modelId
    }

    if (options.providerId) {
      filter.providerId = options.providerId
    }

    if (options.contentType) {
      filter.contentType = options.contentType
    }

    if (options.classification) {
      filter.classification = options.classification
    }

    if (options.finishReason) {
      filter.finishReason = options.finishReason
    }

    if (options.userId) {
      filter.userId = options.userId
    }

    if (options.appId) {
      filter.appId = options.appId
    }

    if (options.sessionId) {
      filter.sessionId = options.sessionId
    }

    if (options.environment) {
      filter.environment = options.environment
    }

    if (options.from || options.to) {
      filter.timestamp = {}
      if (options.from) {
        (filter.timestamp as Record<string, unknown>).$gte = options.from
      }
      if (options.to) {
        (filter.timestamp as Record<string, unknown>).$lt = options.to
      }
    }

    if (options.cachedOnly !== undefined) {
      filter.isCached = options.cachedOnly
    }

    if (options.streamingOnly !== undefined) {
      filter.isStreaming = options.streamingOnly
    }

    if (options.rootContentId) {
      filter.rootContentId = options.rootContentId
    }

    // Build sort
    const sortField = options.sort?.replace('-', '') ?? 'timestamp'
    const sortOrder = options.sort?.startsWith('-') ? -1 : 1

    const results = await collection.find(filter, {
      limit: options.limit ?? DEFAULT_LIMIT,
      sort: { [sortField]: sortOrder },
    })

    return results as unknown as GeneratedContentRecord[]
  }

  /**
   * Get a single content record by ID
   *
   * @param contentId - The content ID
   * @returns The content record or null
   */
  async findOne(contentId: string): Promise<GeneratedContentRecord | null> {
    const collection = this.db.collection(this.config.collection)
    const results = await collection.find({ contentId }, { limit: 1 })

    if (results.length === 0) {
      return null
    }

    return results[0] as unknown as GeneratedContentRecord
  }

  /**
   * Create a new version of existing content
   *
   * @param parentContentId - The content ID of the parent version
   * @param input - New content data
   * @returns Created content record
   */
  async createVersion(
    parentContentId: string,
    input: Omit<RecordContentInput, 'parentContentId' | 'rootContentId'>
  ): Promise<GeneratedContentRecord> {
    // Find the parent content
    const parent = await this.findOne(parentContentId)
    if (!parent) {
      throw new Error(`Parent content not found: ${parentContentId}`)
    }

    // Determine root and version
    const rootContentId = parent.rootContentId ?? parent.contentId
    const newVersion = parent.version + 1

    // Find the highest version in this chain to ensure correct version number
    const existingVersions = await this.find({
      rootContentId,
      sort: '-version',
      limit: 1,
    })

    const maxVersion = existingVersions.length > 0 ? existingVersions[0]!.version : parent.version
    const finalVersion = Math.max(newVersion, maxVersion + 1)

    const result = await this.record({
      ...input,
      parentContentId,
      rootContentId,
    })

    // Update the version number if needed
    if (finalVersion !== result.version) {
      const collection = this.db.collection(this.config.collection)
      const id = result.$id.split('/').pop()
      if (id) {
        await collection.update(id, {
          $set: { version: finalVersion },
        })
        result.version = finalVersion
      }
    }

    return result
  }

  /**
   * Get version history for a content record
   *
   * @param contentId - Any content ID in the version chain
   * @returns Array of all versions, sorted by version number
   */
  async getVersionHistory(contentId: string): Promise<GeneratedContentRecord[]> {
    // First find the content to get its rootContentId
    const content = await this.findOne(contentId)
    if (!content) {
      return []
    }

    const rootId = content.rootContentId ?? content.contentId

    // Find all versions with this rootContentId
    const collection = this.db.collection(this.config.collection)

    // Get the root content
    const rootResults = await collection.find({ contentId: rootId }, { limit: 1 })

    // Get all children
    const childResults = await collection.find(
      { rootContentId: rootId },
      { limit: 1000, sort: { version: 1 } }
    )

    // Combine and sort
    const allVersions = [...rootResults, ...childResults] as unknown as GeneratedContentRecord[]

    // Dedupe by contentId and sort by version
    const seen = new Set<string>()
    return allVersions
      .filter(v => {
        if (seen.has(v.contentId)) return false
        seen.add(v.contentId)
        return true
      })
      .sort((a, b) => a.version - b.version)
  }

  /**
   * Get the latest version of a content record
   *
   * @param contentId - Any content ID in the version chain
   * @returns The latest version record or null
   */
  async getLatestVersion(contentId: string): Promise<GeneratedContentRecord | null> {
    const history = await this.getVersionHistory(contentId)
    if (history.length === 0) {
      return null
    }
    return history[history.length - 1]!
  }

  /**
   * Get aggregated statistics for generated content
   *
   * @param options - Query options to filter content
   * @returns Aggregated statistics
   */
  async getStats(options: ContentQueryOptions = {}): Promise<ContentStats> {
    // Get all matching content (up to a reasonable limit)
    const content = await this.find({
      ...options,
      limit: options.limit ?? 10000,
    })

    if (content.length === 0) {
      const now = new Date()
      return {
        totalRecords: 0,
        uniqueContentCount: 0,
        totalVersions: 0,
        avgVersionsPerContent: 0,
        cacheHits: 0,
        cacheHitRatio: 0,
        tokens: {
          totalTokens: 0,
          totalPromptTokens: 0,
          avgTokensPerContent: 0,
        },
        cost: {
          totalCost: 0,
          avgCost: 0,
          cacheSavings: 0,
        },
        contentLength: {
          total: 0,
          avg: 0,
          min: 0,
          max: 0,
        },
        byContentType: {},
        byModel: {},
        byProvider: {},
        byClassification: {},
        timeRange: { from: options.from ?? now, to: options.to ?? now },
      }
    }

    // Calculate counts
    const cacheHits = content.filter(c => c.isCached).length
    const rootIds = new Set(content.map(c => c.rootContentId ?? c.contentId))
    const uniqueContentCount = rootIds.size
    const totalVersions = content.filter(c => c.version > 1).length

    // Calculate token stats
    const totalTokens = content.reduce((sum, c) => sum + c.tokenCount, 0)
    const totalPromptTokens = content.reduce((sum, c) => sum + c.promptTokenCount, 0)

    // Calculate cost stats
    const nonCachedContent = content.filter(c => !c.isCached)
    const cachedContent = content.filter(c => c.isCached)
    const totalCost = nonCachedContent.reduce((sum, c) => sum + c.estimatedCost, 0)
    const cacheSavings = cachedContent.reduce((sum, c) => sum + c.estimatedCost, 0)

    // Calculate content length stats
    const contentLengths = content.map(c => c.contentLength)
    const totalLength = contentLengths.reduce((a, b) => a + b, 0)

    // Calculate breakdowns
    const byContentType: Record<string, number> = {}
    const byModel: Record<string, { count: number; cost: number; avgLength: number; totalLength: number }> = {}
    const byProvider: Record<string, { count: number; cost: number; avgLength: number; totalLength: number }> = {}
    const byClassification: Record<string, number> = {}

    for (const c of content) {
      // By content type
      byContentType[c.contentType] = (byContentType[c.contentType] ?? 0) + 1

      // By model
      if (!byModel[c.modelId]) {
        byModel[c.modelId] = { count: 0, cost: 0, avgLength: 0, totalLength: 0 }
      }
      byModel[c.modelId]!.count++
      byModel[c.modelId]!.cost += c.isCached ? 0 : c.estimatedCost
      byModel[c.modelId]!.totalLength += c.contentLength

      // By provider
      if (!byProvider[c.providerId]) {
        byProvider[c.providerId] = { count: 0, cost: 0, avgLength: 0, totalLength: 0 }
      }
      byProvider[c.providerId]!.count++
      byProvider[c.providerId]!.cost += c.isCached ? 0 : c.estimatedCost
      byProvider[c.providerId]!.totalLength += c.contentLength

      // By classification
      byClassification[c.classification] = (byClassification[c.classification] ?? 0) + 1
    }

    // Calculate averages for breakdowns
    const cleanByModel: Record<string, { count: number; cost: number; avgLength: number }> = {}
    for (const [k, v] of Object.entries(byModel)) {
      cleanByModel[k] = {
        count: v.count,
        cost: v.cost,
        avgLength: v.count > 0 ? v.totalLength / v.count : 0,
      }
    }

    const cleanByProvider: Record<string, { count: number; cost: number; avgLength: number }> = {}
    for (const [k, v] of Object.entries(byProvider)) {
      cleanByProvider[k] = {
        count: v.count,
        cost: v.cost,
        avgLength: v.count > 0 ? v.totalLength / v.count : 0,
      }
    }

    // Get time range from content
    const timestamps = content.map(c => c.timestamp.getTime())
    const minTs = Math.min(...timestamps)
    const maxTs = Math.max(...timestamps)

    return {
      totalRecords: content.length,
      uniqueContentCount,
      totalVersions,
      avgVersionsPerContent: uniqueContentCount > 0 ? content.length / uniqueContentCount : 0,
      cacheHits,
      cacheHitRatio: content.length > 0 ? cacheHits / content.length : 0,
      tokens: {
        totalTokens,
        totalPromptTokens,
        avgTokensPerContent: content.length > 0 ? totalTokens / content.length : 0,
      },
      cost: {
        totalCost,
        avgCost: content.length > 0 ? totalCost / content.length : 0,
        cacheSavings,
      },
      contentLength: {
        total: totalLength,
        avg: content.length > 0 ? totalLength / content.length : 0,
        min: contentLengths.length > 0 ? Math.min(...contentLengths) : 0,
        max: contentLengths.length > 0 ? Math.max(...contentLengths) : 0,
      },
      byContentType,
      byModel: cleanByModel,
      byProvider: cleanByProvider,
      byClassification,
      timeRange: {
        from: options.from ?? new Date(minTs),
        to: options.to ?? new Date(maxTs),
      },
    }
  }

  /**
   * Get content by hash for deduplication check
   *
   * @param contentHash - The content hash
   * @returns Matching content records
   */
  async findByHash(contentHash: string): Promise<GeneratedContentRecord[]> {
    const collection = this.db.collection(this.config.collection)
    const results = await collection.find({ contentHash }, { limit: 100 })
    return results as unknown as GeneratedContentRecord[]
  }

  /**
   * Delete old content beyond the max age
   *
   * Uses batch delete for efficiency (O(1) vs O(n) individual deletes).
   *
   * @param options - Optional cleanup options
   * @returns Cleanup result with deleted count and details
   */
  async cleanup(options?: {
    /** Override the default max age */
    maxAgeMs?: number
    /** Progress callback */
    onProgress?: (progress: { deletedSoFar: number; percentage: number }) => void
  }): Promise<CleanupResult> {
    const collection = this.db.collection(this.config.collection)
    const maxAgeMs = options?.maxAgeMs ?? this.config.maxAgeMs
    const cutoffDate = new Date(Date.now() - maxAgeMs)
    const startTime = Date.now()

    try {
      // Count total to delete
      const filter = { timestamp: { $lt: cutoffDate } }
      const totalToDelete = await collection.count(filter)

      if (totalToDelete === 0) {
        return {
          success: true,
          deletedCount: 0,
          durationMs: Date.now() - startTime,
        }
      }

      // Use batch delete for efficiency
      const result = await collection.deleteMany(filter, { hard: true })

      options?.onProgress?.({
        deletedSoFar: result.deletedCount,
        percentage: 100,
      })

      return {
        success: true,
        deletedCount: result.deletedCount,
        durationMs: Date.now() - startTime,
      }
    } catch (error) {
      return {
        success: false,
        deletedCount: 0,
        durationMs: Date.now() - startTime,
        error: error instanceof Error ? error.message : String(error),
      }
    }
  }

  /**
   * Get a retention manager for background cleanup scheduling
   *
   * @returns RetentionManager instance configured for this MV
   */
  getRetentionManager(): import('../retention').RetentionManager {
    // Lazy import to avoid circular dependencies
    const { RetentionManager } = require('../retention')
    return new RetentionManager(this.db, {
      collection: this.config.collection,
      maxAgeMs: this.config.maxAgeMs,
      timestampField: 'timestamp',
      debug: this.config.debug,
    })
  }

  /**
   * Get the current configuration
   */
  getConfig(): ResolvedContentMVConfig {
    return { ...this.config }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a GeneratedContentMV instance
 *
 * @param db - ParqueDB instance
 * @param config - Configuration options
 * @returns GeneratedContentMV instance
 *
 * @example
 * ```typescript
 * const contentMV = createGeneratedContentMV(db)
 * await contentMV.record({ modelId: 'gpt-4', ... })
 * const stats = await contentMV.getStats()
 * ```
 */
export function createGeneratedContentMV(db: ParqueDB, config: GeneratedContentMVConfig = {}): GeneratedContentMV {
  return new GeneratedContentMV(db, config)
}
