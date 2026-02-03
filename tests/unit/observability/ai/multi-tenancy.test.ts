/**
 * Tests for Multi-Tenancy Support in Observability MVs
 *
 * Tests tenant isolation, scoped storage, and cross-tenant analytics
 * for AIUsageMV, AIRequestsMV, and GeneratedContentMV.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  AIUsageMV,
  createAIUsageMV,
  type AIUsageMVConfig,
  type MultiTenantConfig,
} from '../../../../src/observability/ai'
import {
  AIRequestsMV,
  createAIRequestsMV,
  type AIRequestsMVConfig,
} from '../../../../src/observability/ai/AIRequestsMV'
import {
  GeneratedContentMV,
  createGeneratedContentMV,
  type GeneratedContentMVConfig,
} from '../../../../src/observability/ai/GeneratedContentMV'

// =============================================================================
// Mock ParqueDB
// =============================================================================

interface MockRecord {
  $id: string
  [key: string]: unknown
}

function createMockDB() {
  const collections: Map<string, Map<string, MockRecord>> = new Map()
  let idCounter = 0

  const getCollection = (name: string) => {
    if (!collections.has(name)) {
      collections.set(name, new Map())
    }
    return collections.get(name)!
  }

  return {
    _collections: collections,
    collection: vi.fn((name: string) => {
      const store = getCollection(name)
      return {
        find: vi.fn(async (filter?: Record<string, unknown>, options?: { limit?: number; sort?: Record<string, number> }) => {
          let results = Array.from(store.values())

          // Apply filters
          if (filter) {
            results = results.filter(item => {
              for (const [key, value] of Object.entries(filter)) {
                if (key === 'timestamp' && typeof value === 'object' && value !== null) {
                  const ts = item[key] as Date
                  const filterObj = value as Record<string, unknown>
                  if (filterObj.$gte && ts < (filterObj.$gte as Date)) return false
                  if (filterObj.$lt && ts >= (filterObj.$lt as Date)) return false
                } else if (typeof value === 'boolean' || typeof value === 'string' || typeof value === 'number') {
                  if (item[key] !== value) return false
                }
              }
              return true
            })
          }

          // Apply sort
          if (options?.sort) {
            const [sortKey, sortDir] = Object.entries(options.sort)[0]!
            results.sort((a, b) => {
              const aVal = a[sortKey]
              const bVal = b[sortKey]
              if (typeof aVal === 'string' && typeof bVal === 'string') {
                return sortDir > 0 ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
              }
              if (typeof aVal === 'number' && typeof bVal === 'number') {
                return sortDir > 0 ? aVal - bVal : bVal - aVal
              }
              return 0
            })
          }

          // Apply limit
          if (options?.limit) {
            results = results.slice(0, options.limit)
          }

          return results
        }),
        create: vi.fn(async (data: Record<string, unknown>) => {
          const id = `${name}/${++idCounter}`
          const record = { $id: id, ...data } as MockRecord
          store.set(id, record)
          return record
        }),
        createMany: vi.fn(async (items: Record<string, unknown>[]) => {
          return Promise.all(items.map(async (data) => {
            const id = `${name}/${++idCounter}`
            const record = { $id: id, ...data } as MockRecord
            store.set(id, record)
            return record
          }))
        }),
        update: vi.fn(async (id: string, update: Record<string, unknown>) => {
          const fullId = `${name}/${id}`
          const record = store.get(fullId)
          if (record && update.$set) {
            Object.assign(record, update.$set)
          }
          return { matchedCount: 1, modifiedCount: 1 }
        }),
        count: vi.fn(async (filter?: Record<string, unknown>) => {
          let results = Array.from(store.values())
          if (filter) {
            results = results.filter(item => {
              for (const [key, value] of Object.entries(filter)) {
                if (item[key] !== value) return false
              }
              return true
            })
          }
          return results.length
        }),
        deleteMany: vi.fn(async () => ({ deletedCount: 0 })),
      }
    }),
  }
}

// =============================================================================
// AIUsageMV Multi-Tenancy Tests
// =============================================================================

describe('AIUsageMV Multi-Tenancy', () => {
  describe('configuration', () => {
    it('should accept tenantId in config', () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const config: AIUsageMVConfig = {
        tenantId: 'tenant-123',
      }

      const mv = new AIUsageMV(db, config)
      const resolvedConfig = mv.getConfig()

      expect(resolvedConfig.tenantId).toBe('tenant-123')
    })

    it('should create tenant-scoped collection names when tenantScopedStorage is true', () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const config: AIUsageMVConfig = {
        tenantId: 'acme',
        tenantScopedStorage: true,
      }

      const mv = new AIUsageMV(db, config)
      const resolvedConfig = mv.getConfig()

      expect(resolvedConfig.sourceCollection).toBe('tenant_acme/ai_logs')
      expect(resolvedConfig.targetCollection).toBe('tenant_acme/ai_usage')
    })

    it('should use default collection names when tenantId is not set', () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIUsageMV>[0]

      const mv = new AIUsageMV(db, {})
      const resolvedConfig = mv.getConfig()

      expect(resolvedConfig.sourceCollection).toBe('ai_logs')
      expect(resolvedConfig.targetCollection).toBe('ai_usage')
    })

    it('should default allowCrossTenantQueries to false', () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIUsageMV>[0]

      const mv = new AIUsageMV(db, {})
      const resolvedConfig = mv.getConfig()

      expect(resolvedConfig.allowCrossTenantQueries).toBe(false)
    })
  })

  describe('getUsage with tenant filtering', () => {
    it('should throw error when allTenants is true but not allowed', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db, {
        tenantId: 'tenant-123',
        allowCrossTenantQueries: false,
      })

      await expect(mv.getUsage({ allTenants: true }))
        .rejects.toThrow('Cross-tenant queries are not enabled')
    })

    it('should allow allTenants query when allowCrossTenantQueries is true', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db, {
        allowCrossTenantQueries: true,
      })

      // Should not throw
      const results = await mv.getUsage({ allTenants: true })
      expect(results).toEqual([])
    })
  })

  describe('getTenantSummary', () => {
    it('should throw error when no tenantId is provided', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db, {})

      await expect(mv.getTenantSummary())
        .rejects.toThrow('Tenant ID is required for getTenantSummary')
    })

    it('should use config tenantId when not specified', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db, { tenantId: 'tenant-123' })

      const summary = await mv.getTenantSummary()

      expect(summary.tenantId).toBe('tenant-123')
    })

    it('should use provided tenantId over config', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db, { tenantId: 'tenant-123' })

      const summary = await mv.getTenantSummary('tenant-456')

      expect(summary.tenantId).toBe('tenant-456')
    })

    it('should return correct summary structure', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db, { tenantId: 'tenant-123' })

      const summary = await mv.getTenantSummary()

      expect(summary).toMatchObject({
        tenantId: 'tenant-123',
        totalRequests: 0,
        successCount: 0,
        errorCount: 0,
        errorRate: 0,
        tokens: {
          total: 0,
          prompt: 0,
          completion: 0,
        },
        cost: {
          total: 0,
          byModel: {},
          byProvider: {},
        },
        avgLatencyMs: 0,
        cacheHitRatio: 0,
      })
    })
  })

  describe('getCrossTenantAggregates', () => {
    it('should throw error when cross-tenant queries are not enabled', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db, {
        allowCrossTenantQueries: false,
      })

      await expect(mv.getCrossTenantAggregates())
        .rejects.toThrow('Cross-tenant queries are not enabled')
    })

    it('should return empty array when no data', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIUsageMV>[0]
      const mv = new AIUsageMV(db, {
        allowCrossTenantQueries: true,
      })

      const results = await mv.getCrossTenantAggregates()

      expect(results).toEqual([])
    })
  })
})

// =============================================================================
// AIRequestsMV Multi-Tenancy Tests
// =============================================================================

describe('AIRequestsMV Multi-Tenancy', () => {
  describe('configuration', () => {
    it('should accept tenantId in config', () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIRequestsMV>[0]
      const config: AIRequestsMVConfig = {
        tenantId: 'tenant-123',
      }

      const mv = new AIRequestsMV(db, config)
      const resolvedConfig = mv.getConfig()

      expect(resolvedConfig.tenantId).toBe('tenant-123')
    })

    it('should create tenant-scoped collection when enabled', () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIRequestsMV>[0]
      const config: AIRequestsMVConfig = {
        tenantId: 'acme',
        tenantScopedStorage: true,
      }

      const mv = new AIRequestsMV(db, config)
      const resolvedConfig = mv.getConfig()

      expect(resolvedConfig.collection).toBe('tenant_acme/ai_requests')
    })
  })

  describe('record with tenantId', () => {
    it('should include config tenantId in recorded requests', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIRequestsMV>[0]
      const mv = new AIRequestsMV(db, { tenantId: 'tenant-123' })

      const record = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
      })

      expect(record.tenantId).toBe('tenant-123')
    })

    it('should allow input tenantId to override config', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIRequestsMV>[0]
      const mv = new AIRequestsMV(db, { tenantId: 'tenant-123' })

      const record = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        requestType: 'generate',
        latencyMs: 500,
        tenantId: 'tenant-456',
      })

      expect(record.tenantId).toBe('tenant-456')
    })
  })

  describe('find with tenant filtering', () => {
    it('should throw error when allTenants is true but not allowed', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIRequestsMV>[0]
      const mv = new AIRequestsMV(db, {
        tenantId: 'tenant-123',
        allowCrossTenantQueries: false,
      })

      await expect(mv.find({ allTenants: true }))
        .rejects.toThrow('Cross-tenant queries are not enabled')
    })

    it('should allow allTenants query when allowCrossTenantQueries is true', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createAIRequestsMV>[0]
      const mv = new AIRequestsMV(db, {
        allowCrossTenantQueries: true,
      })

      // Should not throw
      const results = await mv.find({ allTenants: true })
      expect(results).toEqual([])
    })
  })
})

// =============================================================================
// GeneratedContentMV Multi-Tenancy Tests
// =============================================================================

describe('GeneratedContentMV Multi-Tenancy', () => {
  describe('configuration', () => {
    it('should accept tenantId in config', () => {
      const db = createMockDB() as unknown as Parameters<typeof createGeneratedContentMV>[0]
      const config: GeneratedContentMVConfig = {
        tenantId: 'tenant-123',
      }

      const mv = new GeneratedContentMV(db, config)
      const resolvedConfig = mv.getConfig()

      expect(resolvedConfig.tenantId).toBe('tenant-123')
    })

    it('should create tenant-scoped collection when enabled', () => {
      const db = createMockDB() as unknown as Parameters<typeof createGeneratedContentMV>[0]
      const config: GeneratedContentMVConfig = {
        tenantId: 'acme',
        tenantScopedStorage: true,
      }

      const mv = new GeneratedContentMV(db, config)
      const resolvedConfig = mv.getConfig()

      expect(resolvedConfig.collection).toBe('tenant_acme/generated_content')
    })
  })

  describe('record with tenantId', () => {
    it('should include config tenantId in recorded content', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createGeneratedContentMV>[0]
      const mv = new GeneratedContentMV(db, { tenantId: 'tenant-123' })

      const record = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Hello, world!',
      })

      expect(record.tenantId).toBe('tenant-123')
    })

    it('should allow input tenantId to override config', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createGeneratedContentMV>[0]
      const mv = new GeneratedContentMV(db, { tenantId: 'tenant-123' })

      const record = await mv.record({
        modelId: 'gpt-4',
        providerId: 'openai',
        contentType: 'text',
        content: 'Hello, world!',
        tenantId: 'tenant-456',
      })

      expect(record.tenantId).toBe('tenant-456')
    })
  })

  describe('find with tenant filtering', () => {
    it('should throw error when allTenants is true but not allowed', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createGeneratedContentMV>[0]
      const mv = new GeneratedContentMV(db, {
        tenantId: 'tenant-123',
        allowCrossTenantQueries: false,
      })

      await expect(mv.find({ allTenants: true }))
        .rejects.toThrow('Cross-tenant queries are not enabled')
    })

    it('should allow allTenants query when allowCrossTenantQueries is true', async () => {
      const db = createMockDB() as unknown as Parameters<typeof createGeneratedContentMV>[0]
      const mv = new GeneratedContentMV(db, {
        allowCrossTenantQueries: true,
      })

      // Should not throw
      const results = await mv.find({ allTenants: true })
      expect(results).toEqual([])
    })
  })
})

// =============================================================================
// Multi-Tenant Config Type Tests
// =============================================================================

describe('MultiTenantConfig types', () => {
  it('should allow minimal config', () => {
    const config: MultiTenantConfig = {}
    expect(config).toBeDefined()
  })

  it('should allow full config', () => {
    const config: MultiTenantConfig = {
      tenantId: 'tenant-123',
      tenantScopedStorage: true,
      allowCrossTenantQueries: true,
    }
    expect(config.tenantId).toBe('tenant-123')
    expect(config.tenantScopedStorage).toBe(true)
    expect(config.allowCrossTenantQueries).toBe(true)
  })
})
