/**
 * Tests for ModelPricingService - Auto-updating model pricing
 *
 * Tests the pricing service functionality including:
 * - Pricing lookup with normalization
 * - Enterprise overrides
 * - API-based refresh
 * - Caching and fallback behavior
 * - Auto-refresh functionality
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  ModelPricingService,
  createModelPricingService,
  getDefaultPricingService,
  resetDefaultPricingService,
  type PricingWithMetadata,
  type ModelPricingServiceConfig,
  type PricingCache,
} from '../../../../src/observability/ai/pricing'
import { DEFAULT_MODEL_PRICING } from '../../../../src/observability/ai/types'

// =============================================================================
// Mock API Responses
// =============================================================================

const mockApiPricing = [
  { modelId: 'gpt-4', providerId: 'openai', inputPricePerMillion: 28.00, outputPricePerMillion: 56.00, contextWindow: 8192 },
  { modelId: 'gpt-4-turbo', providerId: 'openai', inputPricePerMillion: 9.00, outputPricePerMillion: 27.00, contextWindow: 128000 },
  { modelId: 'claude-3-opus', providerId: 'anthropic', inputPricePerMillion: 14.00, outputPricePerMillion: 70.00, contextWindow: 200000 },
  { modelId: 'new-model-2026', providerId: 'openai', inputPricePerMillion: 1.00, outputPricePerMillion: 2.00, contextWindow: 256000 },
]

function createMockFetch(responseData: unknown, options?: { status?: number; delay?: number; error?: Error }) {
  return vi.fn().mockImplementation(async () => {
    if (options?.delay) {
      // Use vi.advanceTimersByTimeAsync when fake timers are active
      await vi.advanceTimersByTimeAsync(options.delay)
    }

    if (options?.error) {
      throw options.error
    }

    return {
      ok: (options?.status ?? 200) >= 200 && (options?.status ?? 200) < 300,
      status: options?.status ?? 200,
      statusText: options?.status === 200 ? 'OK' : 'Error',
      json: async () => responseData,
    }
  })
}

// =============================================================================
// Tests
// =============================================================================

describe('ModelPricingService', () => {
  afterEach(() => {
    resetDefaultPricingService()
    vi.clearAllTimers()
    vi.useRealTimers()
  })

  // ---------------------------------------------------------------------------
  // Initialization Tests
  // ---------------------------------------------------------------------------

  describe('initialization', () => {
    it('should initialize with default pricing', () => {
      const service = createModelPricingService()

      expect(service.getAllPricing().length).toBeGreaterThan(0)
      expect(service.hasPricing('gpt-4', 'openai')).toBe(true)
      expect(service.hasPricing('claude-3-opus', 'anthropic')).toBe(true)
    })

    it('should initialize with correct default pricing values', () => {
      const service = createModelPricingService()

      const gpt4Pricing = service.getPricing('gpt-4', 'openai')
      expect(gpt4Pricing).toBeDefined()
      expect(gpt4Pricing!.source).toBe('default')
      expect(gpt4Pricing!.inputPricePerMillion).toBe(30.00)
      expect(gpt4Pricing!.outputPricePerMillion).toBe(60.00)
    })

    it('should set cache version to 1 initially', () => {
      const service = createModelPricingService()
      const cache = service.getCache()

      expect(cache.version).toBe(1)
    })

    it('should track last updated date', () => {
      const before = new Date()
      const service = createModelPricingService()
      const after = new Date()

      const cache = service.getCache()
      expect(cache.lastUpdated.getTime()).toBeGreaterThanOrEqual(before.getTime())
      expect(cache.lastUpdated.getTime()).toBeLessThanOrEqual(after.getTime())
    })
  })

  // ---------------------------------------------------------------------------
  // Pricing Lookup Tests
  // ---------------------------------------------------------------------------

  describe('getPricing', () => {
    it('should return pricing for exact model+provider match', () => {
      const service = createModelPricingService()

      const pricing = service.getPricing('gpt-4', 'openai')
      expect(pricing).toBeDefined()
      expect(pricing!.modelId).toBe('gpt-4')
      expect(pricing!.providerId).toBe('openai')
    })

    it('should return undefined for unknown model', () => {
      const service = createModelPricingService()

      const pricing = service.getPricing('nonexistent-model', 'openai')
      expect(pricing).toBeUndefined()
    })

    it('should normalize model IDs with date suffixes', () => {
      const service = createModelPricingService()

      // Model with date suffix should match base model
      const pricing = service.getPricing('gpt-4-0613', 'openai')
      expect(pricing).toBeDefined()
      expect(pricing!.modelId).toBe('gpt-4')
    })

    it('should normalize model IDs with full date suffixes', () => {
      const service = createModelPricingService()

      const pricing = service.getPricing('claude-3-opus-20240229', 'anthropic')
      expect(pricing).toBeDefined()
      expect(pricing!.modelId).toBe('claude-3-opus')
    })
  })

  describe('getAllPricing', () => {
    it('should return all pricing entries', () => {
      const service = createModelPricingService()

      const allPricing = service.getAllPricing()
      expect(allPricing.length).toBe(DEFAULT_MODEL_PRICING.length)
    })

    it('should include metadata for all entries', () => {
      const service = createModelPricingService()

      const allPricing = service.getAllPricing()
      for (const pricing of allPricing) {
        expect(pricing.source).toBeDefined()
        expect(pricing.lastUpdated).toBeInstanceOf(Date)
        expect(typeof pricing.version).toBe('number')
      }
    })
  })

  describe('getPricingByProvider', () => {
    it('should filter pricing by provider', () => {
      const service = createModelPricingService()

      const openaiPricing = service.getPricingByProvider('openai')
      expect(openaiPricing.length).toBeGreaterThan(0)
      expect(openaiPricing.every(p => p.providerId === 'openai')).toBe(true)

      const anthropicPricing = service.getPricingByProvider('anthropic')
      expect(anthropicPricing.length).toBeGreaterThan(0)
      expect(anthropicPricing.every(p => p.providerId === 'anthropic')).toBe(true)
    })

    it('should return empty array for unknown provider', () => {
      const service = createModelPricingService()

      const unknownPricing = service.getPricingByProvider('unknown-provider')
      expect(unknownPricing).toEqual([])
    })
  })

  // ---------------------------------------------------------------------------
  // Enterprise Override Tests
  // ---------------------------------------------------------------------------

  describe('enterprise overrides', () => {
    it('should apply enterprise overrides on initialization', () => {
      const service = createModelPricingService({
        enterpriseOverrides: [
          { modelId: 'gpt-4', providerId: 'openai', inputPricePerMillion: 25.00, outputPricePerMillion: 50.00 },
        ],
      })

      const pricing = service.getPricing('gpt-4', 'openai')
      expect(pricing).toBeDefined()
      expect(pricing!.inputPricePerMillion).toBe(25.00)
      expect(pricing!.outputPricePerMillion).toBe(50.00)
      expect(pricing!.source).toBe('override')
    })

    it('should add enterprise override via setEnterpriseOverrides', () => {
      const service = createModelPricingService()

      service.setEnterpriseOverrides([
        { modelId: 'claude-3-opus', providerId: 'anthropic', inputPricePerMillion: 12.00, outputPricePerMillion: 60.00 },
      ])

      const pricing = service.getPricing('claude-3-opus', 'anthropic')
      expect(pricing!.inputPricePerMillion).toBe(12.00)
      expect(pricing!.source).toBe('override')
    })

    it('should add single enterprise override via addEnterpriseOverride', () => {
      const service = createModelPricingService()

      service.addEnterpriseOverride({
        modelId: 'custom-model',
        providerId: 'custom-provider',
        inputPricePerMillion: 5.00,
        outputPricePerMillion: 10.00,
      })

      const pricing = service.getPricing('custom-model', 'custom-provider')
      expect(pricing).toBeDefined()
      expect(pricing!.inputPricePerMillion).toBe(5.00)
      expect(pricing!.source).toBe('override')
    })

    it('should replace existing override when adding duplicate', () => {
      const service = createModelPricingService({
        enterpriseOverrides: [
          { modelId: 'gpt-4', providerId: 'openai', inputPricePerMillion: 25.00, outputPricePerMillion: 50.00 },
        ],
      })

      service.addEnterpriseOverride({
        modelId: 'gpt-4',
        providerId: 'openai',
        inputPricePerMillion: 20.00,
        outputPricePerMillion: 40.00,
      })

      const pricing = service.getPricing('gpt-4', 'openai')
      expect(pricing!.inputPricePerMillion).toBe(20.00)

      // Should only have one override for this model+provider
      const overrides = service.getEnterpriseOverrides()
      const gpt4Overrides = overrides.filter(o => o.modelId === 'gpt-4' && o.providerId === 'openai')
      expect(gpt4Overrides.length).toBe(1)
    })

    it('should remove enterprise override', () => {
      const service = createModelPricingService({
        enterpriseOverrides: [
          { modelId: 'gpt-4', providerId: 'openai', inputPricePerMillion: 25.00, outputPricePerMillion: 50.00 },
        ],
      })

      const removed = service.removeEnterpriseOverride('gpt-4', 'openai')
      expect(removed).toBe(true)

      // Should fall back to default pricing
      const pricing = service.getPricing('gpt-4', 'openai')
      expect(pricing!.inputPricePerMillion).toBe(30.00)
      expect(pricing!.source).toBe('default')
    })

    it('should return false when removing non-existent override', () => {
      const service = createModelPricingService()

      const removed = service.removeEnterpriseOverride('nonexistent', 'provider')
      expect(removed).toBe(false)
    })

    it('should get all enterprise overrides', () => {
      const overrides = [
        { modelId: 'gpt-4', providerId: 'openai', inputPricePerMillion: 25.00, outputPricePerMillion: 50.00 },
        { modelId: 'claude-3-opus', providerId: 'anthropic', inputPricePerMillion: 12.00, outputPricePerMillion: 60.00 },
      ]

      const service = createModelPricingService({ enterpriseOverrides: overrides })

      const retrieved = service.getEnterpriseOverrides()
      expect(retrieved.length).toBe(2)
    })

    it('should maintain override priority over API pricing', async () => {
      const service = createModelPricingService({
        enterpriseOverrides: [
          { modelId: 'gpt-4', providerId: 'openai', inputPricePerMillion: 20.00, outputPricePerMillion: 40.00 },
        ],
        fetchFn: createMockFetch(mockApiPricing),
      })

      await service.refresh()

      const pricing = service.getPricing('gpt-4', 'openai')
      expect(pricing!.inputPricePerMillion).toBe(20.00) // Override value, not API value
      expect(pricing!.source).toBe('override')
    })
  })

  // ---------------------------------------------------------------------------
  // API Refresh Tests
  // ---------------------------------------------------------------------------

  describe('refresh', () => {
    it('should fetch pricing from API', async () => {
      const mockFetch = createMockFetch(mockApiPricing)
      const service = createModelPricingService({ fetchFn: mockFetch })

      const result = await service.refresh()

      expect(result.success).toBe(true)
      expect(result.entriesCount).toBe(mockApiPricing.length)
      expect(result.source).toBe('api')
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('should update pricing after successful refresh', async () => {
      const service = createModelPricingService({
        fetchFn: createMockFetch(mockApiPricing),
      })

      await service.refresh()

      // New model from API should be available
      const newModel = service.getPricing('new-model-2026', 'openai')
      expect(newModel).toBeDefined()
      expect(newModel!.inputPricePerMillion).toBe(1.00)
      expect(newModel!.source).toBe('api')
    })

    it('should increment cache version on successful refresh', async () => {
      const service = createModelPricingService({
        fetchFn: createMockFetch(mockApiPricing),
      })

      const initialVersion = service.getCache().version
      await service.refresh()
      const newVersion = service.getCache().version

      expect(newVersion).toBe(initialVersion + 1)
    })

    it('should handle API errors gracefully', async () => {
      const service = createModelPricingService({
        fetchFn: createMockFetch({}, { status: 500 }),
      })

      const result = await service.refresh()

      expect(result.success).toBe(false)
      expect(result.source).toBe('cached')
      expect(result.error).toContain('500')
    })

    it('should handle network errors gracefully', async () => {
      const service = createModelPricingService({
        fetchFn: createMockFetch({}, { error: new Error('Network error') }),
      })

      const result = await service.refresh()

      expect(result.success).toBe(false)
      expect(result.source).toBe('cached')
      expect(result.error).toContain('Network error')
    })

    it('should preserve default pricing when API fails', async () => {
      const service = createModelPricingService({
        fetchFn: createMockFetch({}, { error: new Error('API unavailable') }),
      })

      await service.refresh()

      // Default pricing should still be available
      const pricing = service.getPricing('gpt-4', 'openai')
      expect(pricing).toBeDefined()
      expect(pricing!.source).toBe('default')
    })

    it('should handle API response with prices wrapper', async () => {
      const service = createModelPricingService({
        fetchFn: createMockFetch({ prices: mockApiPricing }),
      })

      const result = await service.refresh()

      expect(result.success).toBe(true)
      expect(result.entriesCount).toBe(mockApiPricing.length)
    })

    it('should call onPricingUpdated callback on successful refresh', async () => {
      const onPricingUpdated = vi.fn()
      const service = createModelPricingService({
        fetchFn: createMockFetch(mockApiPricing),
        onPricingUpdated,
      })

      await service.refresh()

      expect(onPricingUpdated).toHaveBeenCalledTimes(1)
      expect(onPricingUpdated).toHaveBeenCalledWith(expect.objectContaining({
        success: true,
        source: 'api',
      }))
    })
  })

  // ---------------------------------------------------------------------------
  // Cache Tests
  // ---------------------------------------------------------------------------

  describe('caching', () => {
    it('should get current cache', () => {
      const service = createModelPricingService()

      const cache = service.getCache()
      expect(cache.version).toBe(1)
      expect(cache.entries.length).toBeGreaterThan(0)
      expect(cache.lastUpdated).toBeInstanceOf(Date)
    })

    it('should load cache from external source', () => {
      const service = createModelPricingService()

      const externalCache: PricingCache = {
        version: 5,
        lastUpdated: new Date(),
        entries: [
          {
            modelId: 'cached-model',
            providerId: 'cached-provider',
            inputPricePerMillion: 2.00,
            outputPricePerMillion: 4.00,
            source: 'api',
            lastUpdated: new Date(),
            version: 5,
          },
        ],
      }

      service.loadCache(externalCache)

      const pricing = service.getPricing('cached-model', 'cached-provider')
      expect(pricing).toBeDefined()
      expect(pricing!.inputPricePerMillion).toBe(2.00)
    })

    it('should mark cached entries as cached source after loading', () => {
      const service = createModelPricingService()

      const externalCache: PricingCache = {
        version: 5,
        lastUpdated: new Date(),
        entries: [
          {
            modelId: 'cached-model',
            providerId: 'cached-provider',
            inputPricePerMillion: 2.00,
            outputPricePerMillion: 4.00,
            source: 'api',
            lastUpdated: new Date(),
            version: 5,
          },
        ],
      }

      service.loadCache(externalCache)

      const pricing = service.getPricing('cached-model', 'cached-provider')
      expect(pricing!.source).toBe('cached')
    })

    it('should detect stale cache', () => {
      const service = createModelPricingService({
        cacheMaxAgeMs: 1000, // 1 second
      })

      // Fresh cache should not be stale
      expect(service.isCacheStale()).toBe(false)

      // Load old cache
      const oldCache: PricingCache = {
        version: 1,
        lastUpdated: new Date(Date.now() - 2000), // 2 seconds ago
        entries: [],
      }
      service.loadCache(oldCache)

      expect(service.isCacheStale()).toBe(true)
    })

    it('should report cache age', () => {
      const service = createModelPricingService()

      const age = service.getCacheAge()
      expect(age).toBeGreaterThanOrEqual(0)
      expect(age).toBeLessThan(1000) // Should be very recent
    })
  })

  // ---------------------------------------------------------------------------
  // Auto-Refresh Tests
  // ---------------------------------------------------------------------------

  describe('auto-refresh', () => {
    beforeEach(() => {
      vi.useFakeTimers()
    })

    afterEach(() => {
      vi.useRealTimers()
    })

    it('should start auto-refresh', async () => {
      const mockFetch = createMockFetch(mockApiPricing)
      const service = createModelPricingService({
        fetchFn: mockFetch,
        refreshIntervalMs: 1000,
      })

      await service.startAutoRefresh()

      expect(service.isAutoRefreshActive()).toBe(true)
      expect(mockFetch).toHaveBeenCalledTimes(1) // Immediate refresh

      service.stopAutoRefresh()
    })

    it('should refresh periodically', async () => {
      const mockFetch = createMockFetch(mockApiPricing)
      const service = createModelPricingService({
        fetchFn: mockFetch,
        refreshIntervalMs: 1000,
      })

      await service.startAutoRefresh()
      expect(mockFetch).toHaveBeenCalledTimes(1)

      // Advance time by 1 second
      await vi.advanceTimersByTimeAsync(1000)
      expect(mockFetch).toHaveBeenCalledTimes(2)

      // Advance time by another second
      await vi.advanceTimersByTimeAsync(1000)
      expect(mockFetch).toHaveBeenCalledTimes(3)

      service.stopAutoRefresh()
    })

    it('should skip immediate refresh if requested', async () => {
      const mockFetch = createMockFetch(mockApiPricing)
      const service = createModelPricingService({
        fetchFn: mockFetch,
        refreshIntervalMs: 1000,
      })

      await service.startAutoRefresh(false) // No immediate refresh

      expect(mockFetch).toHaveBeenCalledTimes(0)

      service.stopAutoRefresh()
    })

    it('should stop auto-refresh', async () => {
      const mockFetch = createMockFetch(mockApiPricing)
      const service = createModelPricingService({
        fetchFn: mockFetch,
        refreshIntervalMs: 1000,
      })

      await service.startAutoRefresh()
      service.stopAutoRefresh()

      expect(service.isAutoRefreshActive()).toBe(false)

      // Advance time - should not trigger more fetches
      await vi.advanceTimersByTimeAsync(5000)
      expect(mockFetch).toHaveBeenCalledTimes(1) // Only the initial fetch
    })

    it('should not start duplicate auto-refresh', async () => {
      const mockFetch = createMockFetch(mockApiPricing)
      const service = createModelPricingService({
        fetchFn: mockFetch,
        refreshIntervalMs: 1000,
      })

      await service.startAutoRefresh()
      await service.startAutoRefresh() // Second call should be ignored

      expect(mockFetch).toHaveBeenCalledTimes(1) // Only one initial fetch

      service.stopAutoRefresh()
    })
  })

  // ---------------------------------------------------------------------------
  // Status Tests
  // ---------------------------------------------------------------------------

  describe('status', () => {
    it('should report correct status', () => {
      const service = createModelPricingService()

      const status = service.getStatus()

      expect(status.isAutoRefreshActive).toBe(false)
      expect(status.cacheVersion).toBe(1)
      expect(status.entryCount).toBeGreaterThan(0)
      expect(status.lastRefresh).toBeUndefined()
      expect(status.nextRefresh).toBeUndefined()
    })

    it('should update status after refresh', async () => {
      const service = createModelPricingService({
        fetchFn: createMockFetch(mockApiPricing),
      })

      await service.refresh()
      const status = service.getStatus()

      expect(status.cacheVersion).toBe(2)
      expect(status.lastRefreshResult).toBeDefined()
      expect(status.lastRefreshResult!.success).toBe(true)
    })
  })

  // ---------------------------------------------------------------------------
  // Singleton Tests
  // ---------------------------------------------------------------------------

  describe('singleton', () => {
    it('should create singleton instance', () => {
      const service1 = getDefaultPricingService()
      const service2 = getDefaultPricingService()

      expect(service1).toBe(service2)
    })

    it('should reset singleton', () => {
      const service1 = getDefaultPricingService()
      resetDefaultPricingService()
      const service2 = getDefaultPricingService()

      expect(service1).not.toBe(service2)
    })
  })

  // ---------------------------------------------------------------------------
  // Edge Cases
  // ---------------------------------------------------------------------------

  describe('edge cases', () => {
    it('should handle empty API response', async () => {
      const service = createModelPricingService({
        fetchFn: createMockFetch([]),
      })

      const result = await service.refresh()

      expect(result.success).toBe(true)
      expect(result.entriesCount).toBe(0)
    })

    it('should handle invalid API response format', async () => {
      const service = createModelPricingService({
        fetchFn: createMockFetch({ invalid: 'response' }),
      })

      const result = await service.refresh()

      // Should succeed with empty entries from invalid format
      expect(result.success).toBe(true)
      expect(result.entriesCount).toBe(0)
    })

    it('should work without default fallback', () => {
      const service = createModelPricingService({
        useDefaultFallback: false,
        enterpriseOverrides: [
          { modelId: 'custom-model', providerId: 'custom', inputPricePerMillion: 1.00, outputPricePerMillion: 2.00 },
        ],
      })

      // Default models should not be available
      expect(service.getPricing('gpt-4', 'openai')).toBeUndefined()

      // Custom model should be available
      expect(service.getPricing('custom-model', 'custom')).toBeDefined()
    })

    it('should handle concurrent refresh calls', async () => {
      vi.useFakeTimers()
      try {
        let callCount = 0
        const mockFetch = vi.fn().mockImplementation(async () => {
          callCount++
          await vi.advanceTimersByTimeAsync(100)
          return {
            ok: true,
            status: 200,
            json: async () => mockApiPricing,
          }
        })

        const service = createModelPricingService({ fetchFn: mockFetch })

        // Start two refreshes concurrently
        const [result1, result2] = await Promise.all([
          service.refresh(),
          service.refresh(),
        ])

        // Both should complete (no deduplication - each refresh runs)
        expect(result1.success).toBe(true)
        expect(result2.success).toBe(true)
      } finally {
        vi.useRealTimers()
      }
    })
  })
})
