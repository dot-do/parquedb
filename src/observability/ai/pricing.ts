/**
 * Model Pricing Service for ParqueDB AI Observability
 *
 * Provides auto-updating model pricing with:
 * - API endpoint fetching from provider APIs or aggregators
 * - Periodic refresh with configurable intervals
 * - Version/timestamp tracking
 * - Fallback to cached pricing when API unavailable
 * - User override mechanism for enterprise pricing
 *
 * @example
 * ```typescript
 * import { ModelPricingService, createModelPricingService } from 'parquedb/observability/ai'
 *
 * const pricingService = createModelPricingService({
 *   refreshIntervalMs: 24 * 60 * 60 * 1000, // Daily refresh
 *   enterpriseOverrides: [
 *     { modelId: 'gpt-4', providerId: 'openai', inputPricePerMillion: 25.00, outputPricePerMillion: 50.00 }
 *   ]
 * })
 *
 * // Start auto-refresh
 * await pricingService.startAutoRefresh()
 *
 * // Get pricing for a model
 * const pricing = pricingService.getPricing('gpt-4', 'openai')
 *
 * // Stop auto-refresh when done
 * pricingService.stopAutoRefresh()
 * ```
 *
 * @module observability/ai/pricing
 */

import type { ModelPricing } from './types'
import { DEFAULT_MODEL_PRICING } from './types'
import { logger } from '../../utils/logger'

// =============================================================================
// Types
// =============================================================================

/**
 * Pricing data source identifier
 */
export type PricingSource =
  | 'default'      // Built-in DEFAULT_MODEL_PRICING
  | 'api'          // Fetched from pricing API
  | 'override'     // User/enterprise override
  | 'cached'       // Loaded from cache

/**
 * Extended pricing with metadata
 */
export interface PricingWithMetadata extends ModelPricing {
  /** Source of this pricing data */
  source: PricingSource
  /** When this specific pricing was last updated */
  lastUpdated: Date
  /** Version number for this pricing entry */
  version: number
}

/**
 * Pricing cache entry
 */
export interface PricingCache {
  /** Version of the cached pricing data */
  version: number
  /** When the cache was last updated */
  lastUpdated: Date
  /** Cached pricing entries */
  entries: PricingWithMetadata[]
}

/**
 * Result of a pricing fetch operation
 */
export interface PricingFetchResult {
  /** Whether the fetch was successful */
  success: boolean
  /** Number of pricing entries fetched */
  entriesCount: number
  /** Source of the pricing data */
  source: PricingSource
  /** Duration of the fetch in milliseconds */
  durationMs: number
  /** Error message if failed */
  error?: string | undefined
  /** Version of the fetched pricing */
  version?: number | undefined
}

/**
 * Configuration for the pricing service
 */
export interface ModelPricingServiceConfig {
  /**
   * Auto-refresh interval in milliseconds
   * Default: 24 hours (86400000ms)
   */
  refreshIntervalMs?: number | undefined

  /**
   * URL for the pricing API endpoint
   * Default: 'https://api.llmprices.dev/v1/prices'
   *
   * The API should return an array of ModelPricing objects
   */
  pricingApiUrl?: string | undefined

  /**
   * Custom fetch function for API calls
   * Useful for testing or custom auth
   */
  fetchFn?: typeof fetch | undefined

  /**
   * Enterprise/custom price overrides
   * These take precedence over API and default pricing
   */
  enterpriseOverrides?: ModelPricing[] | undefined

  /**
   * Maximum age of cached pricing before refresh is required (ms)
   * Default: 7 days
   */
  cacheMaxAgeMs?: number | undefined

  /**
   * Whether to use default pricing as fallback
   * Default: true
   */
  useDefaultFallback?: boolean | undefined

  /**
   * Callback when pricing is updated
   */
  onPricingUpdated?: ((result: PricingFetchResult) => void) | undefined

  /**
   * Enable debug logging
   * Default: false
   */
  debug?: boolean | undefined
}

/**
 * Resolved configuration with defaults applied
 */
export interface ResolvedPricingServiceConfig {
  refreshIntervalMs: number
  pricingApiUrl: string
  fetchFn: typeof fetch
  enterpriseOverrides: ModelPricing[]
  cacheMaxAgeMs: number
  useDefaultFallback: boolean
  onPricingUpdated?: ((result: PricingFetchResult) => void) | undefined
  debug: boolean
}

/**
 * Pricing service status
 */
export interface PricingServiceStatus {
  /** Whether auto-refresh is currently running */
  isAutoRefreshActive: boolean
  /** Last successful refresh time */
  lastRefresh?: Date | undefined
  /** Last refresh result */
  lastRefreshResult?: PricingFetchResult | undefined
  /** Current cache version */
  cacheVersion: number
  /** Number of pricing entries */
  entryCount: number
  /** Next scheduled refresh time */
  nextRefresh?: Date | undefined
}

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_REFRESH_INTERVAL_MS = 24 * 60 * 60 * 1000 // 24 hours
const DEFAULT_PRICING_API_URL = 'https://api.llmprices.dev/v1/prices'
const DEFAULT_CACHE_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000 // 7 days
const FETCH_TIMEOUT_MS = 10000 // 10 seconds

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Generate a unique key for a model+provider combination
 */
function getPricingKey(modelId: string, providerId: string): string {
  return `${modelId}:${providerId}`
}

/**
 * Normalize model ID for flexible matching
 *
 * Handles variations like 'gpt-4-0613' -> 'gpt-4'
 */
function normalizeModelId(modelId: string): string {
  if (!modelId) return 'unknown'

  // Remove date suffixes (e.g., -0613, -2024-04-09, -20240229)
  let normalized = modelId.replace(/-\d{4}(-\d{2}(-\d{2})?)?$/, '')
  normalized = normalized.replace(/-\d{8}$/, '')
  normalized = normalized.replace(/-\d{4}$/, '')

  return normalized
}

/**
 * Create default pricing with metadata
 */
function createDefaultPricingWithMetadata(): PricingWithMetadata[] {
  const now = new Date()
  return DEFAULT_MODEL_PRICING.map((p, _index) => ({
    ...p,
    source: 'default' as PricingSource,
    lastUpdated: now,
    version: 1,
  }))
}

/**
 * Resolve configuration with defaults
 */
function resolveConfig(config: ModelPricingServiceConfig): ResolvedPricingServiceConfig {
  return {
    refreshIntervalMs: config.refreshIntervalMs ?? DEFAULT_REFRESH_INTERVAL_MS,
    pricingApiUrl: config.pricingApiUrl ?? DEFAULT_PRICING_API_URL,
    fetchFn: config.fetchFn ?? fetch,
    enterpriseOverrides: config.enterpriseOverrides ?? [],
    cacheMaxAgeMs: config.cacheMaxAgeMs ?? DEFAULT_CACHE_MAX_AGE_MS,
    useDefaultFallback: config.useDefaultFallback ?? true,
    onPricingUpdated: config.onPricingUpdated,
    debug: config.debug ?? false,
  }
}

// =============================================================================
// ModelPricingService Class
// =============================================================================

/**
 * ModelPricingService - Auto-updating model pricing with caching and fallbacks
 *
 * Provides:
 * - Periodic API-based pricing updates
 * - Version and timestamp tracking
 * - Enterprise override support
 * - Fallback to cached/default pricing
 */
export class ModelPricingService {
  private readonly config: ResolvedPricingServiceConfig
  private pricingMap: Map<string, PricingWithMetadata> = new Map()
  private cache: PricingCache
  private autoRefreshTimer?: ReturnType<typeof setInterval> | undefined
  private lastRefreshResult?: PricingFetchResult

  /**
   * Create a new ModelPricingService instance
   *
   * @param config - Configuration options
   */
  constructor(config: ModelPricingServiceConfig = {}) {
    this.config = resolveConfig(config)

    // Initialize cache with default pricing
    const defaultPricing = createDefaultPricingWithMetadata()
    this.cache = {
      version: 1,
      lastUpdated: new Date(),
      entries: defaultPricing,
    }

    // Build initial pricing map
    this.buildPricingMap()

    this.log('ModelPricingService initialized')
  }

  // ---------------------------------------------------------------------------
  // Public Methods - Pricing Access
  // ---------------------------------------------------------------------------

  /**
   * Get pricing for a specific model and provider
   *
   * @param modelId - Model identifier
   * @param providerId - Provider identifier
   * @returns Pricing with metadata, or undefined if not found
   */
  getPricing(modelId: string, providerId: string): PricingWithMetadata | undefined {
    // Try exact match first
    const exactKey = getPricingKey(modelId, providerId)
    let pricing = this.pricingMap.get(exactKey)

    if (pricing) {
      return pricing
    }

    // Try normalized model ID
    const normalizedKey = getPricingKey(normalizeModelId(modelId), providerId)
    pricing = this.pricingMap.get(normalizedKey)

    return pricing
  }

  /**
   * Get all pricing entries
   *
   * @returns Array of all pricing entries with metadata
   */
  getAllPricing(): PricingWithMetadata[] {
    return Array.from(this.pricingMap.values())
  }

  /**
   * Get pricing by provider
   *
   * @param providerId - Provider identifier
   * @returns Array of pricing entries for the provider
   */
  getPricingByProvider(providerId: string): PricingWithMetadata[] {
    return this.getAllPricing().filter(p => p.providerId === providerId)
  }

  /**
   * Check if pricing exists for a model
   *
   * @param modelId - Model identifier
   * @param providerId - Provider identifier
   * @returns True if pricing exists
   */
  hasPricing(modelId: string, providerId: string): boolean {
    return this.getPricing(modelId, providerId) !== undefined
  }

  // ---------------------------------------------------------------------------
  // Public Methods - Pricing Updates
  // ---------------------------------------------------------------------------

  /**
   * Manually refresh pricing from the API
   *
   * @returns Result of the refresh operation
   */
  async refresh(): Promise<PricingFetchResult> {
    const startTime = Date.now()

    try {
      this.log('Starting pricing refresh...')

      // Fetch from API
      const fetchResult = await this.fetchFromApi()

      if (fetchResult.success && fetchResult.entries) {
        // Update cache
        this.cache = {
          version: this.cache.version + 1,
          lastUpdated: new Date(),
          entries: fetchResult.entries,
        }

        // Rebuild pricing map
        this.buildPricingMap()

        const result: PricingFetchResult = {
          success: true,
          entriesCount: fetchResult.entries.length,
          source: 'api',
          durationMs: Date.now() - startTime,
          version: this.cache.version,
        }

        this.lastRefreshResult = result
        this.config.onPricingUpdated?.(result)
        this.log(`Pricing refresh successful: ${result.entriesCount} entries`)

        return result
      }

      // API fetch failed - use cached/default pricing
      this.log('API fetch failed, using fallback pricing')

      const result: PricingFetchResult = {
        success: false,
        entriesCount: this.pricingMap.size,
        source: 'cached',
        durationMs: Date.now() - startTime,
        error: fetchResult.error,
        version: this.cache.version,
      }

      this.lastRefreshResult = result
      return result
    } catch (error) {
      const result: PricingFetchResult = {
        success: false,
        entriesCount: this.pricingMap.size,
        source: 'cached',
        durationMs: Date.now() - startTime,
        error: error instanceof Error ? error.message : String(error),
        version: this.cache.version,
      }

      this.lastRefreshResult = result
      this.log(`Pricing refresh failed: ${result.error}`)

      return result
    }
  }

  /**
   * Set enterprise/custom price overrides
   *
   * Overrides take precedence over API and default pricing.
   *
   * @param overrides - Array of pricing overrides
   */
  setEnterpriseOverrides(overrides: ModelPricing[]): void {
    this.config.enterpriseOverrides.length = 0
    this.config.enterpriseOverrides.push(...overrides)
    this.buildPricingMap()
    this.log(`Set ${overrides.length} enterprise overrides`)
  }

  /**
   * Add a single enterprise override
   *
   * @param pricing - Pricing override to add
   */
  addEnterpriseOverride(pricing: ModelPricing): void {
    // Remove existing override for same model+provider
    const existingIndex = this.config.enterpriseOverrides.findIndex(
      p => p.modelId === pricing.modelId && p.providerId === pricing.providerId
    )
    if (existingIndex >= 0) {
      this.config.enterpriseOverrides.splice(existingIndex, 1)
    }

    this.config.enterpriseOverrides.push(pricing)
    this.buildPricingMap()
    this.log(`Added enterprise override for ${pricing.modelId}:${pricing.providerId}`)
  }

  /**
   * Remove an enterprise override
   *
   * @param modelId - Model identifier
   * @param providerId - Provider identifier
   * @returns True if override was removed
   */
  removeEnterpriseOverride(modelId: string, providerId: string): boolean {
    const index = this.config.enterpriseOverrides.findIndex(
      p => p.modelId === modelId && p.providerId === providerId
    )
    if (index >= 0) {
      this.config.enterpriseOverrides.splice(index, 1)
      this.buildPricingMap()
      this.log(`Removed enterprise override for ${modelId}:${providerId}`)
      return true
    }
    return false
  }

  /**
   * Get all enterprise overrides
   *
   * @returns Array of enterprise override entries
   */
  getEnterpriseOverrides(): ModelPricing[] {
    return [...this.config.enterpriseOverrides]
  }

  // ---------------------------------------------------------------------------
  // Public Methods - Auto-Refresh
  // ---------------------------------------------------------------------------

  /**
   * Start automatic pricing refresh
   *
   * @param immediate - Whether to refresh immediately (default: true)
   */
  async startAutoRefresh(immediate: boolean = true): Promise<void> {
    if (this.autoRefreshTimer) {
      this.log('Auto-refresh already running')
      return
    }

    this.log(`Starting auto-refresh (interval: ${this.config.refreshIntervalMs}ms)`)

    // Perform immediate refresh if requested
    if (immediate) {
      await this.refresh()
    }

    // Set up periodic refresh
    this.autoRefreshTimer = setInterval(async () => {
      await this.refresh()
    }, this.config.refreshIntervalMs)
  }

  /**
   * Stop automatic pricing refresh
   */
  stopAutoRefresh(): void {
    if (this.autoRefreshTimer) {
      clearInterval(this.autoRefreshTimer)
      this.autoRefreshTimer = undefined
      this.log('Auto-refresh stopped')
    }
  }

  /**
   * Check if auto-refresh is currently active
   */
  isAutoRefreshActive(): boolean {
    return this.autoRefreshTimer !== undefined
  }

  // ---------------------------------------------------------------------------
  // Public Methods - Cache Management
  // ---------------------------------------------------------------------------

  /**
   * Get the current cache
   *
   * @returns Current pricing cache
   */
  getCache(): PricingCache {
    return { ...this.cache, entries: [...this.cache.entries] }
  }

  /**
   * Load pricing from a cache
   *
   * Useful for persisting pricing across restarts.
   *
   * @param cache - Cache to load
   */
  loadCache(cache: PricingCache): void {
    // Validate cache age
    const cacheAge = Date.now() - new Date(cache.lastUpdated).getTime()
    if (cacheAge > this.config.cacheMaxAgeMs) {
      this.log('Cache is stale, will refresh from API')
      // Still load it as fallback, but mark for refresh
    }

    this.cache = {
      version: cache.version,
      lastUpdated: new Date(cache.lastUpdated),
      entries: cache.entries.map(e => ({
        ...e,
        source: 'cached' as PricingSource,
        lastUpdated: new Date(e.lastUpdated),
      })),
    }

    this.buildPricingMap()
    this.log(`Loaded cache: ${cache.entries.length} entries, version ${cache.version}`)
  }

  /**
   * Check if cache is stale and needs refresh
   *
   * @returns True if cache is older than cacheMaxAgeMs
   */
  isCacheStale(): boolean {
    const cacheAge = Date.now() - this.cache.lastUpdated.getTime()
    return cacheAge > this.config.cacheMaxAgeMs
  }

  /**
   * Get cache age in milliseconds
   */
  getCacheAge(): number {
    return Date.now() - this.cache.lastUpdated.getTime()
  }

  // ---------------------------------------------------------------------------
  // Public Methods - Status
  // ---------------------------------------------------------------------------

  /**
   * Get current service status
   *
   * @returns Service status information
   */
  getStatus(): PricingServiceStatus {
    return {
      isAutoRefreshActive: this.isAutoRefreshActive(),
      lastRefresh: this.lastRefreshResult ? new Date(Date.now() - (this.lastRefreshResult.durationMs ?? 0)) : undefined,
      lastRefreshResult: this.lastRefreshResult,
      cacheVersion: this.cache.version,
      entryCount: this.pricingMap.size,
      nextRefresh: this.autoRefreshTimer
        ? new Date(this.cache.lastUpdated.getTime() + this.config.refreshIntervalMs)
        : undefined,
    }
  }

  // ---------------------------------------------------------------------------
  // Private Methods
  // ---------------------------------------------------------------------------

  /**
   * Fetch pricing from the API
   */
  private async fetchFromApi(): Promise<{ success: boolean; entries?: PricingWithMetadata[] | undefined; error?: string | undefined }> {
    try {
      // Create abort controller for timeout
      const controller = new AbortController()
      const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)

      try {
        const response = await this.config.fetchFn(this.config.pricingApiUrl, {
          method: 'GET',
          headers: {
            'Accept': 'application/json',
            'User-Agent': 'ParqueDB-AI-Observability/1.0',
          },
          signal: controller.signal,
        })

        clearTimeout(timeoutId)

        if (!response.ok) {
          return {
            success: false,
            error: `API returned ${response.status}: ${response.statusText}`,
          }
        }

        const data = await response.json() as ModelPricing[] | { prices: ModelPricing[] }

        // Handle both array and object responses
        const prices = Array.isArray(data) ? data : (data.prices ?? [])

        if (!Array.isArray(prices)) {
          return {
            success: false,
            error: 'API response is not an array of prices',
          }
        }

        // Convert to PricingWithMetadata
        const now = new Date()
        const entries: PricingWithMetadata[] = prices.map((p, _index) => ({
          modelId: p.modelId,
          providerId: p.providerId,
          inputPricePerMillion: p.inputPricePerMillion,
          outputPricePerMillion: p.outputPricePerMillion,
          displayName: p.displayName,
          supportsStreaming: p.supportsStreaming,
          contextWindow: p.contextWindow,
          updatedAt: p.updatedAt ? new Date(p.updatedAt) : now,
          source: 'api' as PricingSource,
          lastUpdated: now,
          version: this.cache.version + 1,
        }))

        return { success: true, entries }
      } catch (fetchError) {
        clearTimeout(timeoutId)
        throw fetchError
      }
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') {
        return { success: false, error: 'API request timed out' }
      }
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      }
    }
  }

  /**
   * Build the pricing map from cache and overrides
   */
  private buildPricingMap(): void {
    this.pricingMap.clear()
    const now = new Date()

    // 1. Add default pricing as base (if enabled)
    if (this.config.useDefaultFallback) {
      for (const pricing of DEFAULT_MODEL_PRICING) {
        const key = getPricingKey(pricing.modelId, pricing.providerId)
        this.pricingMap.set(key, {
          ...pricing,
          source: 'default',
          lastUpdated: now,
          version: 1,
        })
      }
    }

    // 2. Layer cached/API pricing on top
    for (const pricing of this.cache.entries) {
      if (pricing.source !== 'default') {
        const key = getPricingKey(pricing.modelId, pricing.providerId)
        this.pricingMap.set(key, pricing)
      }
    }

    // 3. Apply enterprise overrides (highest priority)
    for (const override of this.config.enterpriseOverrides) {
      const key = getPricingKey(override.modelId, override.providerId)
      this.pricingMap.set(key, {
        ...override,
        source: 'override',
        lastUpdated: now,
        version: this.cache.version,
      })
    }
  }

  /**
   * Log a debug message
   */
  private log(message: string): void {
    if (this.config.debug) {
      logger.debug(`[ModelPricingService] ${message}`)
    }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a ModelPricingService instance
 *
 * @param config - Configuration options
 * @returns ModelPricingService instance
 *
 * @example
 * ```typescript
 * const pricingService = createModelPricingService({
 *   refreshIntervalMs: 12 * 60 * 60 * 1000, // 12 hours
 *   enterpriseOverrides: [
 *     { modelId: 'gpt-4', providerId: 'openai', inputPricePerMillion: 25.00, outputPricePerMillion: 50.00 }
 *   ]
 * })
 * ```
 */
export function createModelPricingService(config: ModelPricingServiceConfig = {}): ModelPricingService {
  return new ModelPricingService(config)
}

// =============================================================================
// Singleton Instance (Optional)
// =============================================================================

let defaultPricingService: ModelPricingService | undefined

/**
 * Get the default pricing service singleton
 *
 * Creates a new instance if one doesn't exist.
 *
 * @param config - Configuration for the singleton (only used on first call)
 * @returns Default ModelPricingService instance
 */
export function getDefaultPricingService(config?: ModelPricingServiceConfig): ModelPricingService {
  if (!defaultPricingService) {
    defaultPricingService = createModelPricingService(config)
  }
  return defaultPricingService
}

/**
 * Reset the default pricing service singleton
 *
 * Useful for testing or reconfiguration.
 */
export function resetDefaultPricingService(): void {
  if (defaultPricingService) {
    defaultPricingService.stopAutoRefresh()
    defaultPricingService = undefined
  }
}
