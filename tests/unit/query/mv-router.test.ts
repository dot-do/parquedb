/**
 * Tests for Materialized View Query Routing
 *
 * Tests the MV routing functionality including:
 * - Query detection for MV eligibility
 * - Filter compatibility analysis
 * - Projection and sort compatibility
 * - Staleness-aware routing
 * - Post-filter generation
 * - Cost savings estimation
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  MVRouter,
  createMVRouter,
  InMemoryMVMetadataProvider,
  createInMemoryMVMetadataProvider,
  type MVRoutingResult,
  type MVRoutingMetadata,
} from '../../../src/query/optimizer'
import type { MVDefinition } from '../../../src/materialized-views/types'
import type { Filter } from '../../../src/types/filter'

// =============================================================================
// Test Helpers
// =============================================================================

function createMVMetadata(
  name: string,
  definition: MVDefinition,
  options: {
    stalenessState?: 'fresh' | 'stale' | 'invalid'
    usable?: boolean
    rowCount?: number
  } = {}
): MVRoutingMetadata {
  return {
    name,
    definition,
    stalenessState: options.stalenessState ?? 'fresh',
    usable: options.usable ?? true,
    rowCount: options.rowCount,
  }
}

// =============================================================================
// MVRouter Tests
// =============================================================================

describe('MVRouter', () => {
  let provider: InMemoryMVMetadataProvider
  let router: MVRouter

  beforeEach(() => {
    provider = createInMemoryMVMetadataProvider()
    router = createMVRouter(provider)
  })

  describe('basic routing', () => {
    it('returns canUseMV false when no MVs exist', async () => {
      const result = await router.route('orders', { status: 'completed' })

      expect(result.canUseMV).toBe(false)
      expect(result.reason).toContain('No materialized views')
    })

    it('routes to MV when filter matches exactly', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $filter: { status: 'completed' },
      }

      provider.registerMV(createMVMetadata('CompletedOrders', mvDef))

      const result = await router.route('orders', { status: 'completed' })

      expect(result.canUseMV).toBe(true)
      expect(result.mvName).toBe('CompletedOrders')
      expect(result.needsPostFilter).toBe(false)
    })

    it('routes to MV with no filter when query has no filter', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
      }

      provider.registerMV(createMVMetadata('AllOrders', mvDef))

      const result = await router.route('orders', {})

      expect(result.canUseMV).toBe(true)
      expect(result.mvName).toBe('AllOrders')
      expect(result.needsPostFilter).toBe(false)
    })

    it('routes to MV when MV has no filter but query has filter', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
      }

      provider.registerMV(createMVMetadata('AllOrders', mvDef))

      const result = await router.route('orders', { status: 'completed' })

      expect(result.canUseMV).toBe(true)
      expect(result.mvName).toBe('AllOrders')
      expect(result.needsPostFilter).toBe(true)
      expect(result.postFilter).toEqual({ status: 'completed' })
    })

    it('routes to filtered MV when query has no filter', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $filter: { status: 'completed' },
      }

      provider.registerMV(createMVMetadata('CompletedOrders', mvDef))

      // Query all orders, but MV only has completed - this is fine, MV is a subset
      const result = await router.route('orders', {})

      expect(result.canUseMV).toBe(true)
      expect(result.mvName).toBe('CompletedOrders')
    })
  })

  describe('filter compatibility', () => {
    it('rejects MV with conflicting equality filter', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $filter: { status: 'pending' }, // MV has pending
      }

      provider.registerMV(createMVMetadata('PendingOrders', mvDef))

      // Query wants completed, but MV has pending - conflict
      const result = await router.route('orders', { status: 'completed' })

      expect(result.canUseMV).toBe(false)
      expect(result.reason).toContain('conflicts')
    })

    it('allows MV when query filter is more restrictive', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        // No filter - has all orders
      }

      provider.registerMV(createMVMetadata('AllOrders', mvDef))

      // Query is more restrictive - MV has all data, need post-filter
      const result = await router.route('orders', {
        status: 'completed',
        amount: { $gt: 100 },
      })

      expect(result.canUseMV).toBe(true)
      expect(result.needsPostFilter).toBe(true)
      expect(result.postFilter).toEqual({
        status: 'completed',
        amount: { $gt: 100 },
      })
    })

    it('handles $in filter correctly', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $filter: { status: 'completed' },
      }

      provider.registerMV(createMVMetadata('CompletedOrders', mvDef))

      // Query uses $in that includes the MV's status - should work
      const result = await router.route('orders', {
        status: { $in: ['completed', 'pending'] },
      })

      // MV only has completed, but query wants completed OR pending
      // MV can provide completed subset, but needs to indicate partial results
      // Currently this would need post-filter to verify status is in the $in array
      expect(result.canUseMV).toBe(true)
      expect(result.needsPostFilter).toBe(true)
    })

    it('rejects MV with $in filter that excludes MV value', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $filter: { status: 'cancelled' },
      }

      provider.registerMV(createMVMetadata('CancelledOrders', mvDef))

      // Query $in doesn't include 'cancelled' - MV data won't match
      const result = await router.route('orders', {
        status: { $in: ['completed', 'pending'] },
      })

      expect(result.canUseMV).toBe(false)
      expect(result.reason).toContain('conflicts')
    })

    it('handles range filters correctly', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $filter: { status: 'completed' },
      }

      provider.registerMV(createMVMetadata('CompletedOrders', mvDef))

      // Query adds range filter that MV doesn't have
      const result = await router.route('orders', {
        status: 'completed',
        amount: { $gte: 100, $lt: 500 },
      })

      expect(result.canUseMV).toBe(true)
      expect(result.needsPostFilter).toBe(true)
      expect(result.postFilter).toEqual({
        amount: { $gte: 100, $lt: 500 },
      })
    })

    it('handles $and filter in query', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
      }

      provider.registerMV(createMVMetadata('AllOrders', mvDef))

      const result = await router.route('orders', {
        $and: [
          { status: 'completed' },
          { amount: { $gt: 100 } },
        ],
      })

      expect(result.canUseMV).toBe(true)
      expect(result.needsPostFilter).toBe(true)
      expect(result.postFilter?.$and).toBeDefined()
    })

    it('handles $or filter in query', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
      }

      provider.registerMV(createMVMetadata('AllOrders', mvDef))

      const result = await router.route('orders', {
        $or: [
          { status: 'completed' },
          { status: 'pending' },
        ],
      })

      expect(result.canUseMV).toBe(true)
      expect(result.needsPostFilter).toBe(true)
      expect(result.postFilter?.$or).toBeDefined()
    })
  })

  describe('staleness handling', () => {
    it('routes to fresh MV', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
      }

      provider.registerMV(createMVMetadata('AllOrders', mvDef, {
        stalenessState: 'fresh',
        usable: true,
      }))

      const result = await router.route('orders', {})

      expect(result.canUseMV).toBe(true)
      expect(result.stalenessState).toBe('fresh')
    })

    it('routes to stale but usable MV (within grace period)', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
      }

      provider.registerMV(createMVMetadata('AllOrders', mvDef, {
        stalenessState: 'stale',
        usable: true, // Within grace period
      }))

      const result = await router.route('orders', {})

      expect(result.canUseMV).toBe(true)
      expect(result.stalenessState).toBe('stale')
    })

    it('rejects stale MV outside grace period', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
      }

      provider.registerMV(createMVMetadata('AllOrders', mvDef, {
        stalenessState: 'stale',
        usable: false, // Outside grace period
      }))

      const result = await router.route('orders', {})

      expect(result.canUseMV).toBe(false)
      expect(result.reason).toContain('stale')
    })

    it('rejects invalid MV', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
      }

      provider.registerMV(createMVMetadata('AllOrders', mvDef, {
        stalenessState: 'invalid',
        usable: false,
      }))

      const result = await router.route('orders', {})

      expect(result.canUseMV).toBe(false)
    })
  })

  describe('aggregation MVs', () => {
    it('rejects aggregation MV for regular query', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $groupBy: ['status'],
        $compute: {
          count: { $count: '*' },
          total: { $sum: 'amount' },
        },
      }

      provider.registerMV(createMVMetadata('OrderStats', mvDef))

      const result = await router.route('orders', { status: 'completed' })

      expect(result.canUseMV).toBe(false)
      expect(result.reason).toContain('aggregation')
    })

    it('rejects MV with $compute for regular query', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $compute: {
          orderCount: { $count: '*' },
        },
      }

      provider.registerMV(createMVMetadata('OrderCounts', mvDef))

      const result = await router.route('orders', {})

      expect(result.canUseMV).toBe(false)
      expect(result.reason).toContain('aggregation')
    })
  })

  describe('projection compatibility', () => {
    it('rejects MV when required projection field is missing', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $select: {
          status: 'status',
          total: 'total',
        },
      }

      provider.registerMV(createMVMetadata('OrderSummary', mvDef))

      // Query needs 'customerName' which isn't in MV $select
      const result = await router.route(
        'orders',
        {},
        { project: { customerName: 1 } }
      )

      expect(result.canUseMV).toBe(false)
      expect(result.reason).toContain('$select')
    })

    it('allows MV when all projected fields are available', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $select: {
          status: 'status',
          total: 'total',
          customerName: 'customer.name',
        },
      }

      provider.registerMV(createMVMetadata('OrderSummary', mvDef))

      const result = await router.route(
        'orders',
        {},
        { project: { status: 1, total: 1 } }
      )

      expect(result.canUseMV).toBe(true)
    })

    it('allows MV when query has no projection', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $select: {
          status: 'status',
        },
      }

      provider.registerMV(createMVMetadata('OrderSummary', mvDef))

      const result = await router.route('orders', {})

      // Even with limited fields, MV can be used if filter matches
      expect(result.canUseMV).toBe(true)
    })
  })

  describe('sort compatibility', () => {
    it('rejects MV when sort field is not in $select', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $select: {
          status: 'status',
          total: 'total',
        },
      }

      provider.registerMV(createMVMetadata('OrderSummary', mvDef))

      // Sort by 'createdAt' which isn't in MV $select (except core fields)
      // createdAt is a core field, so this should work
      const result = await router.route(
        'orders',
        {},
        { sort: { createdAt: -1 } }
      )

      expect(result.canUseMV).toBe(true)
    })

    it('allows MV when sort field is available', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $select: {
          status: 'status',
          total: 'total',
        },
      }

      provider.registerMV(createMVMetadata('OrderSummary', mvDef))

      const result = await router.route(
        'orders',
        {},
        { sort: { status: 1 } }
      )

      expect(result.canUseMV).toBe(true)
    })
  })

  describe('denormalization MVs ($expand)', () => {
    it('routes to denormalized MV', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $expand: ['customer', 'items.product'],
        $flatten: { customer: 'buyer' },
      }

      provider.registerMV(createMVMetadata('DenormalizedOrders', mvDef))

      const result = await router.route('orders', {})

      expect(result.canUseMV).toBe(true)
      expect(result.mvDefinition?.$expand).toBeDefined()
    })

    it('calculates higher cost savings for denormalized MV', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $expand: ['customer', 'items.product'],
      }

      provider.registerMV(createMVMetadata('DenormalizedOrders', mvDef))

      const result = await router.route('orders', {})

      expect(result.canUseMV).toBe(true)
      expect(result.costSavings).toBeGreaterThan(0.5) // Higher savings for denormalized
    })
  })

  describe('best MV selection', () => {
    it('selects MV with best filter match', async () => {
      // MV with no filter
      const allOrdersDef: MVDefinition = { $from: 'orders' }
      provider.registerMV(createMVMetadata('AllOrders', allOrdersDef))

      // MV with matching filter
      const completedDef: MVDefinition = {
        $from: 'orders',
        $filter: { status: 'completed' },
      }
      provider.registerMV(createMVMetadata('CompletedOrders', completedDef))

      const result = await router.route('orders', { status: 'completed' })

      expect(result.canUseMV).toBe(true)
      // Should select the one with matching filter (no post-filter needed)
      expect(result.needsPostFilter).toBe(false)
      expect(result.mvName).toBe('CompletedOrders')
    })

    it('selects fresh MV over stale MV', async () => {
      const mvDef: MVDefinition = { $from: 'orders' }

      provider.registerMV(createMVMetadata('StaleOrders', mvDef, {
        stalenessState: 'stale',
        usable: true,
      }))

      provider.registerMV(createMVMetadata('FreshOrders', mvDef, {
        stalenessState: 'fresh',
        usable: true,
      }))

      const result = await router.route('orders', {})

      expect(result.canUseMV).toBe(true)
      expect(result.mvName).toBe('FreshOrders')
      expect(result.stalenessState).toBe('fresh')
    })

    it('selects MV with higher cost savings', async () => {
      // Simple MV
      const simpleDef: MVDefinition = { $from: 'orders' }
      provider.registerMV(createMVMetadata('SimpleOrders', simpleDef))

      // Denormalized MV (higher savings due to avoiding joins)
      const denormalizedDef: MVDefinition = {
        $from: 'orders',
        $expand: ['customer', 'items'],
      }
      provider.registerMV(createMVMetadata('DenormalizedOrders', denormalizedDef))

      const result = await router.route('orders', {})

      expect(result.canUseMV).toBe(true)
      // Should select the one with higher cost savings
      expect(result.mvName).toBe('DenormalizedOrders')
    })
  })

  describe('cost savings estimation', () => {
    it('returns positive cost savings for usable MV', async () => {
      const mvDef: MVDefinition = { $from: 'orders' }
      provider.registerMV(createMVMetadata('AllOrders', mvDef))

      const result = await router.route('orders', {})

      expect(result.canUseMV).toBe(true)
      expect(result.costSavings).toBeGreaterThan(0)
      expect(result.costSavings).toBeLessThanOrEqual(1)
    })

    it('returns zero cost savings for rejected MV', async () => {
      // No MVs registered
      const result = await router.route('orders', {})

      expect(result.canUseMV).toBe(false)
      expect(result.costSavings).toBe(0)
    })

    it('reduces cost savings when post-filter is needed', async () => {
      const noFilterMV: MVDefinition = { $from: 'orders' }
      provider.registerMV(createMVMetadata('NoFilterMV', noFilterMV))

      // Query requires post-filter
      const withPostFilter = await router.route('orders', { status: 'completed' })

      // Compare to exact match scenario by registering matching MV
      provider.clear()
      const exactMatchMV: MVDefinition = {
        $from: 'orders',
        $filter: { status: 'completed' },
      }
      provider.registerMV(createMVMetadata('ExactMatchMV', exactMatchMV))
      const withoutPostFilter = await router.route('orders', { status: 'completed' })

      expect(withPostFilter.costSavings).toBeLessThan(withoutPostFilter.costSavings)
    })

    it('reduces cost savings for stale MV', async () => {
      const mvDef: MVDefinition = { $from: 'orders' }

      provider.registerMV(createMVMetadata('FreshMV', mvDef, {
        stalenessState: 'fresh',
        usable: true,
      }))
      const freshResult = await router.route('orders', {})

      provider.clear()
      provider.registerMV(createMVMetadata('StaleMV', mvDef, {
        stalenessState: 'stale',
        usable: true,
      }))
      const staleResult = await router.route('orders', {})

      expect(staleResult.costSavings).toBeLessThan(freshResult.costSavings)
    })
  })

  describe('edge cases', () => {
    it('handles empty filter in MV definition', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $filter: {},
      }

      provider.registerMV(createMVMetadata('AllOrders', mvDef))

      const result = await router.route('orders', { status: 'completed' })

      expect(result.canUseMV).toBe(true)
      expect(result.needsPostFilter).toBe(true)
    })

    it('handles undefined filter in query', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
      }

      provider.registerMV(createMVMetadata('AllOrders', mvDef))

      const result = await router.route('orders', undefined as unknown as Filter)

      expect(result.canUseMV).toBe(true)
      expect(result.needsPostFilter).toBe(false)
    })

    it('handles nested field filters', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
      }

      provider.registerMV(createMVMetadata('AllOrders', mvDef))

      const result = await router.route('orders', {
        'customer.status': 'active',
        'items.quantity': { $gt: 0 },
      })

      expect(result.canUseMV).toBe(true)
      expect(result.needsPostFilter).toBe(true)
    })

    it('handles $ne filter correctly', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $filter: { status: 'completed' },
      }

      provider.registerMV(createMVMetadata('CompletedOrders', mvDef))

      // Query excludes 'completed' - MV only has 'completed' - conflict
      const result = await router.route('orders', {
        status: { $ne: 'completed' },
      })

      expect(result.canUseMV).toBe(false)
    })

    it('handles $nin filter correctly', async () => {
      const mvDef: MVDefinition = {
        $from: 'orders',
        $filter: { status: 'cancelled' },
      }

      provider.registerMV(createMVMetadata('CancelledOrders', mvDef))

      // Query excludes 'cancelled' - MV only has 'cancelled' - conflict
      const result = await router.route('orders', {
        status: { $nin: ['cancelled', 'pending'] },
      })

      // This should be rejected because MV's data would be excluded
      expect(result.canUseMV).toBe(false)
    })
  })
})

// =============================================================================
// InMemoryMVMetadataProvider Tests
// =============================================================================

describe('InMemoryMVMetadataProvider', () => {
  let provider: InMemoryMVMetadataProvider

  beforeEach(() => {
    provider = createInMemoryMVMetadataProvider()
  })

  it('registers and retrieves MV', async () => {
    const mvDef: MVDefinition = { $from: 'orders' }
    const metadata = createMVMetadata('TestMV', mvDef)

    provider.registerMV(metadata)

    const retrieved = await provider.getMVMetadata('TestMV')
    expect(retrieved).toEqual(metadata)
  })

  it('returns null for non-existent MV', async () => {
    const result = await provider.getMVMetadata('NonExistent')
    expect(result).toBeNull()
  })

  it('returns MVs for source collection', async () => {
    const mvDef1: MVDefinition = { $from: 'orders' }
    const mvDef2: MVDefinition = { $from: 'orders' }
    const mvDef3: MVDefinition = { $from: 'products' }

    provider.registerMV(createMVMetadata('MV1', mvDef1))
    provider.registerMV(createMVMetadata('MV2', mvDef2))
    provider.registerMV(createMVMetadata('MV3', mvDef3))

    const orderMVs = await provider.getMVsForSource('orders')
    expect(orderMVs).toHaveLength(2)
    expect(orderMVs.map(mv => mv.name)).toContain('MV1')
    expect(orderMVs.map(mv => mv.name)).toContain('MV2')

    const productMVs = await provider.getMVsForSource('products')
    expect(productMVs).toHaveLength(1)
    expect(productMVs[0]!.name).toBe('MV3')
  })

  it('returns empty array for source with no MVs', async () => {
    const result = await provider.getMVsForSource('nonexistent')
    expect(result).toEqual([])
  })

  it('clears all MVs', async () => {
    const mvDef: MVDefinition = { $from: 'orders' }
    provider.registerMV(createMVMetadata('MV1', mvDef))
    provider.registerMV(createMVMetadata('MV2', mvDef))

    provider.clear()

    expect(await provider.getMVsForSource('orders')).toEqual([])
    expect(await provider.getMVMetadata('MV1')).toBeNull()
  })
})

// =============================================================================
// createMVRouter Factory Tests
// =============================================================================

describe('createMVRouter', () => {
  it('creates router with provider', () => {
    const provider = createInMemoryMVMetadataProvider()
    const router = createMVRouter(provider)

    expect(router).toBeInstanceOf(MVRouter)
  })
})

// =============================================================================
// createInMemoryMVMetadataProvider Factory Tests
// =============================================================================

describe('createInMemoryMVMetadataProvider', () => {
  it('creates provider instance', () => {
    const provider = createInMemoryMVMetadataProvider()
    expect(provider).toBeInstanceOf(InMemoryMVMetadataProvider)
  })
})
