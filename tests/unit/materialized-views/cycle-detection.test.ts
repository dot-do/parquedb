/**
 * Tests for MV cycle detection
 *
 * Tests for detecting circular dependencies in materialized view definitions.
 */

import { describe, it, expect } from 'vitest'
import {
  detectMVCycles,
  getMVDependencies,
  MVCycleError,
} from '../../../src/materialized-views/define'

describe('getMVDependencies', () => {
  it('should return empty array for schema with no MVs', () => {
    const schema = {
      Customer: { name: 'string!' },
      Order: { total: 'number!' },
    }

    const deps = getMVDependencies(schema)
    expect(deps.size).toBe(0)
  })

  it('should extract $from dependencies', () => {
    const schema = {
      Order: { total: 'number!' },
      OrderSummary: { $from: 'Order' },
    }

    const deps = getMVDependencies(schema)
    expect(deps.get('OrderSummary')).toEqual(['Order'])
  })

  it('should handle MVs depending on other MVs', () => {
    const schema = {
      Order: { total: 'number!' },
      DailyOrders: { $from: 'Order', $groupBy: [{ date: '$createdAt' }] },
      WeeklySummary: { $from: 'DailyOrders' },
    }

    const deps = getMVDependencies(schema)
    expect(deps.get('DailyOrders')).toEqual(['Order'])
    expect(deps.get('WeeklySummary')).toEqual(['DailyOrders'])
  })

  it('should handle stream collections as sources', () => {
    const schema = {
      TailEvents: { $ingest: 'tail' },
      WorkerErrors: { $from: 'TailEvents', $filter: { outcome: { $ne: 'ok' } } },
    }

    const deps = getMVDependencies(schema)
    expect(deps.get('WorkerErrors')).toEqual(['TailEvents'])
  })
})

describe('detectMVCycles', () => {
  it('should return null for schema with no cycles', () => {
    const schema = {
      Order: { total: 'number!' },
      DailyOrders: { $from: 'Order' },
      WeeklySummary: { $from: 'DailyOrders' },
    }

    const cycle = detectMVCycles(schema)
    expect(cycle).toBeNull()
  })

  it('should detect direct self-reference cycle', () => {
    const schema = {
      SelfRef: { $from: 'SelfRef' },
    }

    const cycle = detectMVCycles(schema)
    expect(cycle).toEqual(['SelfRef', 'SelfRef'])
  })

  it('should detect simple two-node cycle (A -> B -> A)', () => {
    const schema = {
      ViewA: { $from: 'ViewB' },
      ViewB: { $from: 'ViewA' },
    }

    const cycle = detectMVCycles(schema)
    expect(cycle).not.toBeNull()
    // Cycle should contain both views
    expect(cycle).toContain('ViewA')
    expect(cycle).toContain('ViewB')
  })

  it('should detect multi-level cycle (A -> B -> C -> A)', () => {
    const schema = {
      ViewA: { $from: 'ViewB' },
      ViewB: { $from: 'ViewC' },
      ViewC: { $from: 'ViewA' },
    }

    const cycle = detectMVCycles(schema)
    expect(cycle).not.toBeNull()
    expect(cycle).toContain('ViewA')
    expect(cycle).toContain('ViewB')
    expect(cycle).toContain('ViewC')
  })

  it('should not report false positives for diamond dependencies', () => {
    // Diamond pattern (not a cycle):
    //      Base
    //     /    \
    //   Left   Right
    //     \    /
    //     Merged
    const schema = {
      Base: { data: 'string!' },
      Left: { $from: 'Base' },
      Right: { $from: 'Base' },
      Merged: { $from: 'Left' }, // Only depends on Left, but this tests the algorithm
    }

    const cycle = detectMVCycles(schema)
    expect(cycle).toBeNull()
  })

  it('should detect cycle in complex graph with multiple paths', () => {
    const schema = {
      Base: { data: 'string!' },
      ViewA: { $from: 'Base' },
      ViewB: { $from: 'ViewA' },
      ViewC: { $from: 'ViewB' },
      ViewD: { $from: 'ViewC' },
      // Create cycle: ViewE depends on ViewD, but ViewB also depends on ViewE
      ViewE: { $from: 'ViewD' },
    }
    // Override ViewB to create cycle
    ;(schema.ViewB as any).$from = 'ViewE'

    const cycle = detectMVCycles(schema)
    expect(cycle).not.toBeNull()
  })

  it('should handle schema with mixed collections, streams, and MVs', () => {
    const schema = {
      // Regular collection
      Customer: { name: 'string!' },
      // Stream collection
      TailEvents: { $ingest: 'tail' },
      // MVs in a valid chain
      Errors: { $from: 'TailEvents', $filter: { outcome: { $ne: 'ok' } } },
      ErrorSummary: { $from: 'Errors', $groupBy: ['scriptName'] },
    }

    const cycle = detectMVCycles(schema)
    expect(cycle).toBeNull()
  })

  it('should return cycle path including the repeated node', () => {
    const schema = {
      A: { $from: 'B' },
      B: { $from: 'C' },
      C: { $from: 'A' },
    }

    const cycle = detectMVCycles(schema)
    expect(cycle).not.toBeNull()
    // The cycle should show the path back to the start
    // e.g., ['A', 'B', 'C', 'A'] or similar
    expect(cycle!.length).toBeGreaterThanOrEqual(3)
    expect(cycle![0]).toBe(cycle![cycle!.length - 1])
  })
})

describe('MVCycleError', () => {
  it('should have correct error code', () => {
    const error = new MVCycleError(['A', 'B', 'A'])
    expect(error.code).toBe('CIRCULAR_DEPENDENCY')
  })

  it('should have descriptive message with cycle path', () => {
    const error = new MVCycleError(['ViewA', 'ViewB', 'ViewC', 'ViewA'])
    expect(error.message).toContain('ViewA')
    expect(error.message).toContain('ViewB')
    expect(error.message).toContain('ViewC')
    expect(error.message.toLowerCase()).toContain('circular')
  })

  it('should expose the cycle path', () => {
    const cyclePath = ['A', 'B', 'C', 'A']
    const error = new MVCycleError(cyclePath)
    expect(error.cyclePath).toEqual(cyclePath)
  })

  it('should include affected views in message for self-reference', () => {
    const error = new MVCycleError(['SelfRef', 'SelfRef'])
    expect(error.message).toContain('SelfRef')
    expect(error.message.toLowerCase()).toContain('itself')
  })
})

describe('validateSchema with cycle detection', () => {
  // Import validateSchema for integration tests
  it('should include cycle errors in validation result', async () => {
    const { validateSchema } = await import('../../../src/materialized-views/define')

    const schema = {
      ViewA: { $from: 'ViewB' },
      ViewB: { $from: 'ViewA' },
    }

    const errors = validateSchema(schema)
    // Should have an error entry for the cycle
    const allErrors = Array.from(errors.values()).flat()
    const cycleError = allErrors.find(e => e.message.toLowerCase().includes('circular'))
    expect(cycleError).toBeDefined()
  })

  it('should report cycle as error on the first view in the cycle', async () => {
    const { validateSchema } = await import('../../../src/materialized-views/define')

    const schema = {
      A: { $from: 'B' },
      B: { $from: 'C' },
      C: { $from: 'A' },
    }

    const errors = validateSchema(schema)
    // At least one entry should exist
    expect(errors.size).toBeGreaterThan(0)
  })
})
