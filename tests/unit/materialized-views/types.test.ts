/**
 * Materialized View Types Tests
 *
 * Comprehensive tests for MV type definitions, type guards,
 * validation functions, and defineView API.
 */

import { describe, it, expect } from 'vitest'
import {
  // Types
  type RefreshMode,
  type RefreshStrategy,
  type MVStatus,
  type ViewState,
  type ViewName,
  type MVId,
  type ViewDefinition,
  type ViewMetadata,
  type ViewQuery,
  type ViewOptions,
  type ViewStats,
  type ScheduleOptions,
  type ViewValidationError,
  type DefineViewInput,
  type MaterializedViewDefinition,

  // Constants
  RefreshMode as RefreshModeEnum,
  RefreshStrategy as RefreshStrategyEnum,
  MVStatus as MVStatusEnum,
  DEFAULT_VIEW_OPTIONS,

  // Branded type creators
  viewName,
  mvId,

  // Type guards
  isRefreshMode,
  isRefreshStrategy,
  isViewState,
  isMVStatus,
  isValidViewDefinition,
  isValidMaterializedViewDefinition,
  isPipelineQuery,
  isSimpleQuery,

  // Validation
  isValidViewName,
  isValidCronExpression,
  validateCronExpression,
  type CronValidationResult,
  validateViewDefinition,

  // Conversion
  toViewDefinition,
  fromViewDefinition,

  // API
  defineView,
} from '@/materialized-views/types'

// =============================================================================
// Branded Type Tests
// =============================================================================

describe('Branded Types', () => {
  describe('ViewName', () => {
    it('creates a ViewName from string', () => {
      const name = viewName('test_view')
      expect(name).toBe('test_view')
      // Type check - should compile
      const _: ViewName = name
    })

    it('accepts any string', () => {
      expect(viewName('my-view')).toBe('my-view')
      expect(viewName('MyView123')).toBe('MyView123')
      expect(viewName('')).toBe('')
    })
  })

  describe('MVId', () => {
    it('creates an MVId from string', () => {
      const id = mvId('mv_123')
      expect(id).toBe('mv_123')
      // Type check - should compile
      const _: MVId = id
    })

    it('accepts any string', () => {
      expect(mvId('uuid-like-id')).toBe('uuid-like-id')
      expect(mvId('123')).toBe('123')
    })
  })
})

// =============================================================================
// Enum-like Constants Tests
// =============================================================================

describe('Enum Constants', () => {
  describe('RefreshMode', () => {
    it('has all expected values', () => {
      expect(RefreshModeEnum.Streaming).toBe('streaming')
      expect(RefreshModeEnum.Scheduled).toBe('scheduled')
      expect(RefreshModeEnum.Manual).toBe('manual')
    })
  })

  describe('RefreshStrategy', () => {
    it('has all expected values', () => {
      expect(RefreshStrategyEnum.Full).toBe('full')
      expect(RefreshStrategyEnum.Incremental).toBe('incremental')
      expect(RefreshStrategyEnum.Streaming).toBe('streaming')
    })
  })

  describe('MVStatus', () => {
    it('has all expected values', () => {
      expect(MVStatusEnum.Creating).toBe('creating')
      expect(MVStatusEnum.Ready).toBe('ready')
      expect(MVStatusEnum.Refreshing).toBe('refreshing')
      expect(MVStatusEnum.Stale).toBe('stale')
      expect(MVStatusEnum.Error).toBe('error')
      expect(MVStatusEnum.Disabled).toBe('disabled')
    })
  })
})

// =============================================================================
// Type Guard Tests
// =============================================================================

describe('Type Guards', () => {
  describe('isRefreshMode', () => {
    it('returns true for valid refresh modes', () => {
      expect(isRefreshMode('streaming')).toBe(true)
      expect(isRefreshMode('scheduled')).toBe(true)
      expect(isRefreshMode('manual')).toBe(true)
    })

    it('returns false for invalid values', () => {
      expect(isRefreshMode('invalid')).toBe(false)
      expect(isRefreshMode('')).toBe(false)
      expect(isRefreshMode(null)).toBe(false)
      expect(isRefreshMode(undefined)).toBe(false)
      expect(isRefreshMode(123)).toBe(false)
      expect(isRefreshMode({})).toBe(false)
    })
  })

  describe('isRefreshStrategy', () => {
    it('returns true for valid refresh strategies', () => {
      expect(isRefreshStrategy('full')).toBe(true)
      expect(isRefreshStrategy('incremental')).toBe(true)
      expect(isRefreshStrategy('streaming')).toBe(true)
    })

    it('returns false for invalid values', () => {
      expect(isRefreshStrategy('invalid')).toBe(false)
      expect(isRefreshStrategy('merge')).toBe(false) // Not in current type
      expect(isRefreshStrategy(null)).toBe(false)
      expect(isRefreshStrategy(undefined)).toBe(false)
    })
  })

  describe('isViewState', () => {
    it('returns true for valid view states', () => {
      expect(isViewState('pending')).toBe(true)
      expect(isViewState('building')).toBe(true)
      expect(isViewState('ready')).toBe(true)
      expect(isViewState('stale')).toBe(true)
      expect(isViewState('error')).toBe(true)
      expect(isViewState('disabled')).toBe(true)
    })

    it('returns false for invalid values', () => {
      expect(isViewState('invalid')).toBe(false)
      expect(isViewState('creating')).toBe(false) // This is MVStatus, not ViewState
      expect(isViewState(null)).toBe(false)
    })
  })

  describe('isMVStatus', () => {
    it('returns true for valid MV statuses', () => {
      expect(isMVStatus('creating')).toBe(true)
      expect(isMVStatus('ready')).toBe(true)
      expect(isMVStatus('refreshing')).toBe(true)
      expect(isMVStatus('stale')).toBe(true)
      expect(isMVStatus('error')).toBe(true)
      expect(isMVStatus('disabled')).toBe(true)
    })

    it('returns false for invalid values', () => {
      expect(isMVStatus('invalid')).toBe(false)
      expect(isMVStatus('pending')).toBe(false) // This is ViewState, not MVStatus
      expect(isMVStatus(null)).toBe(false)
    })
  })

  describe('isValidViewDefinition', () => {
    it('returns true for valid definitions', () => {
      const validDef = {
        name: viewName('test_view'),
        source: 'users',
        query: { filter: { status: 'active' } },
        options: { refreshMode: 'manual' },
      }
      expect(isValidViewDefinition(validDef)).toBe(true)
    })

    it('returns false for null', () => {
      expect(isValidViewDefinition(null)).toBe(false)
    })

    it('returns false for non-objects', () => {
      expect(isValidViewDefinition('string')).toBe(false)
      expect(isValidViewDefinition(123)).toBe(false)
      expect(isValidViewDefinition([])).toBe(false)
    })

    it('returns false for missing required fields', () => {
      expect(isValidViewDefinition({})).toBe(false)
      expect(isValidViewDefinition({ name: 'test' })).toBe(false)
      expect(isValidViewDefinition({ name: 'test', source: 'users' })).toBe(false)
      expect(
        isValidViewDefinition({
          name: 'test',
          source: 'users',
          query: {},
        })
      ).toBe(false)
    })

    it('returns false for invalid field types', () => {
      expect(
        isValidViewDefinition({
          name: 123, // Should be string
          source: 'users',
          query: {},
          options: {},
        })
      ).toBe(false)

      expect(
        isValidViewDefinition({
          name: 'test',
          source: null, // Should be string
          query: {},
          options: {},
        })
      ).toBe(false)

      expect(
        isValidViewDefinition({
          name: 'test',
          source: 'users',
          query: 'invalid', // Should be object
          options: {},
        })
      ).toBe(false)
    })
  })

  describe('isPipelineQuery', () => {
    it('returns true for pipeline queries', () => {
      const query: ViewQuery = {
        pipeline: [{ $match: { status: 'active' } }],
      }
      expect(isPipelineQuery(query)).toBe(true)
    })

    it('returns false for empty pipelines', () => {
      const query: ViewQuery = { pipeline: [] }
      expect(isPipelineQuery(query)).toBe(false)
    })

    it('returns false for simple queries', () => {
      const query: ViewQuery = { filter: { status: 'active' } }
      expect(isPipelineQuery(query)).toBe(false)
    })

    it('returns false when pipeline is undefined', () => {
      const query: ViewQuery = { filter: {} }
      expect(isPipelineQuery(query)).toBe(false)
    })
  })

  describe('isSimpleQuery', () => {
    it('returns true for filter-only queries', () => {
      const query: ViewQuery = { filter: { status: 'active' } }
      expect(isSimpleQuery(query)).toBe(true)
    })

    it('returns true for projection queries', () => {
      const query: ViewQuery = { project: { name: 1, email: 1 } }
      expect(isSimpleQuery(query)).toBe(true)
    })

    it('returns true for combined filter/project/sort', () => {
      const query: ViewQuery = {
        filter: { status: 'active' },
        project: { name: 1 },
        sort: { createdAt: -1 },
      }
      expect(isSimpleQuery(query)).toBe(true)
    })

    it('returns false for pipeline queries', () => {
      const query: ViewQuery = {
        pipeline: [{ $match: {} }],
      }
      expect(isSimpleQuery(query)).toBe(false)
    })
  })
})

// =============================================================================
// Validation Tests
// =============================================================================

describe('Validation Functions', () => {
  describe('isValidViewName', () => {
    it('accepts valid names starting with letter', () => {
      expect(isValidViewName('my_view')).toBe(true)
      expect(isValidViewName('MyView')).toBe(true)
      expect(isValidViewName('view123')).toBe(true)
      expect(isValidViewName('active_users_v2')).toBe(true)
    })

    it('accepts valid names starting with underscore', () => {
      expect(isValidViewName('_private_view')).toBe(true)
      expect(isValidViewName('_123')).toBe(true)
    })

    it('rejects empty strings', () => {
      expect(isValidViewName('')).toBe(false)
    })

    it('rejects names starting with numbers', () => {
      expect(isValidViewName('123view')).toBe(false)
      expect(isValidViewName('1_view')).toBe(false)
    })

    it('rejects names with special characters', () => {
      expect(isValidViewName('my-view')).toBe(false)
      expect(isValidViewName('my.view')).toBe(false)
      expect(isValidViewName('my view')).toBe(false)
      expect(isValidViewName('my@view')).toBe(false)
    })

    it('rejects non-string values', () => {
      expect(isValidViewName(null as any)).toBe(false)
      expect(isValidViewName(undefined as any)).toBe(false)
      expect(isValidViewName(123 as any)).toBe(false)
    })
  })

  describe('isValidCronExpression', () => {
    it('accepts valid 5-part cron expressions', () => {
      expect(isValidCronExpression('* * * * *')).toBe(true)
      expect(isValidCronExpression('0 * * * *')).toBe(true)
      expect(isValidCronExpression('0 0 * * *')).toBe(true)
      expect(isValidCronExpression('30 4 1 * *')).toBe(true)
      expect(isValidCronExpression('0 22 * * 1-5')).toBe(true)
    })

    it('accepts step expressions', () => {
      expect(isValidCronExpression('*/15 * * * *')).toBe(true)
      expect(isValidCronExpression('0 */2 * * *')).toBe(true)
      expect(isValidCronExpression('0-30/5 * * * *')).toBe(true)
    })

    it('accepts list expressions', () => {
      expect(isValidCronExpression('0,15,30,45 * * * *')).toBe(true)
      expect(isValidCronExpression('0 0 1,15 * *')).toBe(true)
    })

    it('accepts range expressions', () => {
      expect(isValidCronExpression('0 9-17 * * *')).toBe(true)
      expect(isValidCronExpression('0 0 * * 1-5')).toBe(true)
    })

    it('accepts combined expressions', () => {
      expect(isValidCronExpression('0,30 9-17 * * 1-5')).toBe(true)
    })

    it('rejects expressions with wrong number of parts', () => {
      expect(isValidCronExpression('')).toBe(false)
      expect(isValidCronExpression('*')).toBe(false)
      expect(isValidCronExpression('* *')).toBe(false)
      expect(isValidCronExpression('* * *')).toBe(false)
      expect(isValidCronExpression('* * * *')).toBe(false)
      expect(isValidCronExpression('* * * * * *')).toBe(false)
      expect(isValidCronExpression('* * * * * * *')).toBe(false)
    })

    it('rejects out-of-range minute values', () => {
      expect(isValidCronExpression('60 * * * *')).toBe(false)
      expect(isValidCronExpression('-1 * * * *')).toBe(false)
    })

    it('rejects out-of-range hour values', () => {
      expect(isValidCronExpression('0 24 * * *')).toBe(false)
      expect(isValidCronExpression('0 -1 * * *')).toBe(false)
    })

    it('rejects out-of-range day of month values', () => {
      expect(isValidCronExpression('0 0 0 * *')).toBe(false)
      expect(isValidCronExpression('0 0 32 * *')).toBe(false)
    })

    it('rejects out-of-range month values', () => {
      expect(isValidCronExpression('0 0 * 0 *')).toBe(false)
      expect(isValidCronExpression('0 0 * 13 *')).toBe(false)
    })

    it('rejects out-of-range day of week values', () => {
      expect(isValidCronExpression('0 0 * * 7')).toBe(false)
      expect(isValidCronExpression('0 0 * * -1')).toBe(false)
    })

    it('rejects invalid range expressions', () => {
      expect(isValidCronExpression('5-3 * * * *')).toBe(false) // start > end
      expect(isValidCronExpression('0-60 * * * *')).toBe(false) // end out of range
      expect(isValidCronExpression('-5-10 * * * *')).toBe(false) // start out of range
    })

    it('rejects invalid step expressions', () => {
      expect(isValidCronExpression('*/0 * * * *')).toBe(false) // zero step
      expect(isValidCronExpression('*/-1 * * * *')).toBe(false) // negative step
      expect(isValidCronExpression('*/abc * * * *')).toBe(false) // non-numeric step
      expect(isValidCronExpression('*/ * * * *')).toBe(false) // missing step
    })

    it('rejects non-integer values', () => {
      expect(isValidCronExpression('1.5 * * * *')).toBe(false)
      expect(isValidCronExpression('abc * * * *')).toBe(false)
    })

    it('rejects non-string values', () => {
      expect(isValidCronExpression(null as any)).toBe(false)
      expect(isValidCronExpression(undefined as any)).toBe(false)
    })
  })

  describe('validateCronExpression', () => {
    it('returns valid for correct expressions', () => {
      const result = validateCronExpression('0 * * * *')
      expect(result.valid).toBe(true)
      expect(result.error).toBeUndefined()
    })

    it('provides detailed error for wrong number of parts', () => {
      const result = validateCronExpression('* * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('5 fields')
      expect(result.error).toContain('got 3')
    })

    it('provides detailed error for out-of-range minute', () => {
      const result = validateCronExpression('60 * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('minute')
      expect(result.error).toContain('60')
      expect(result.error).toContain('out of range')
      expect(result.error).toContain('0-59')
      expect(result.field).toBe('minute')
    })

    it('provides detailed error for out-of-range hour', () => {
      const result = validateCronExpression('0 24 * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('hour')
      expect(result.error).toContain('24')
      expect(result.error).toContain('0-23')
      expect(result.field).toBe('hour')
    })

    it('provides detailed error for out-of-range day of month', () => {
      const result = validateCronExpression('0 0 32 * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('day of month')
      expect(result.error).toContain('32')
      expect(result.error).toContain('1-31')
      expect(result.field).toBe('day of month')
    })

    it('provides detailed error for out-of-range month', () => {
      const result = validateCronExpression('0 0 * 13 *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('month')
      expect(result.error).toContain('13')
      expect(result.error).toContain('1-12')
      expect(result.field).toBe('month')
    })

    it('provides detailed error for out-of-range day of week', () => {
      const result = validateCronExpression('0 0 * * 7')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('day of week')
      expect(result.error).toContain('7')
      expect(result.error).toContain('0-6')
      expect(result.field).toBe('day of week')
    })

    it('provides detailed error for invalid range', () => {
      const result = validateCronExpression('5-3 * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('start (5)')
      expect(result.error).toContain('greater than end (3)')
      expect(result.field).toBe('minute')
    })

    it('provides detailed error for invalid step', () => {
      const result = validateCronExpression('*/0 * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('step')
      expect(result.error).toContain("'0'")
      expect(result.error).toContain('positive integer')
    })

    it('provides detailed error for non-integer value', () => {
      const result = validateCronExpression('abc * * * *')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('minute')
      expect(result.error).toContain("'abc'")
      expect(result.error).toContain('not a valid integer')
    })

    it('provides detailed error for empty string', () => {
      const result = validateCronExpression('')
      expect(result.valid).toBe(false)
      expect(result.error).toContain('non-empty string')
    })

    it('provides detailed error for null/undefined', () => {
      expect(validateCronExpression(null as any).error).toContain('non-empty string')
      expect(validateCronExpression(undefined as any).error).toContain('non-empty string')
    })
  })

  describe('validateViewDefinition', () => {
    it('returns no errors for valid definitions', () => {
      const def = {
        name: viewName('test_view'),
        source: 'users',
        query: { filter: { status: 'active' } },
        options: { refreshMode: 'manual' as RefreshMode },
      }
      const errors = validateViewDefinition(def)
      expect(errors).toHaveLength(0)
    })

    it('returns error for missing name', () => {
      const def = {
        source: 'users',
        query: {},
        options: {},
      }
      const errors = validateViewDefinition(def as any)
      expect(errors.some((e) => e.field === 'name')).toBe(true)
    })

    it('returns error for invalid name', () => {
      const def = {
        name: '123-invalid',
        source: 'users',
        query: {},
        options: {},
      }
      const errors = validateViewDefinition(def as any)
      expect(errors.some((e) => e.field === 'name')).toBe(true)
    })

    it('returns error for missing source', () => {
      const def = {
        name: viewName('test'),
        query: {},
        options: {},
      }
      const errors = validateViewDefinition(def as any)
      expect(errors.some((e) => e.field === 'source')).toBe(true)
    })

    it('returns error for missing query', () => {
      const def = {
        name: viewName('test'),
        source: 'users',
        options: {},
      }
      const errors = validateViewDefinition(def as any)
      expect(errors.some((e) => e.field === 'query')).toBe(true)
    })

    it('returns error for missing options', () => {
      const def = {
        name: viewName('test'),
        source: 'users',
        query: {},
      }
      const errors = validateViewDefinition(def as any)
      expect(errors.some((e) => e.field === 'options')).toBe(true)
    })

    it('returns error for invalid refresh mode', () => {
      const def = {
        name: viewName('test'),
        source: 'users',
        query: {},
        options: { refreshMode: 'invalid' as any },
      }
      const errors = validateViewDefinition(def as any)
      expect(errors.some((e) => e.field === 'options.refreshMode')).toBe(true)
    })

    it('returns error for invalid refresh strategy', () => {
      const def = {
        name: viewName('test'),
        source: 'users',
        query: {},
        options: { refreshStrategy: 'invalid' as any },
      }
      const errors = validateViewDefinition(def as any)
      expect(errors.some((e) => e.field === 'options.refreshStrategy')).toBe(true)
    })

    it('returns error for scheduled mode without schedule', () => {
      const def = {
        name: viewName('test'),
        source: 'users',
        query: {},
        options: { refreshMode: 'scheduled' as RefreshMode },
      }
      const errors = validateViewDefinition(def as any)
      expect(errors.some((e) => e.field === 'options.schedule')).toBe(true)
    })

    it('returns error for schedule without cron or interval', () => {
      const def = {
        name: viewName('test'),
        source: 'users',
        query: {},
        options: { refreshMode: 'scheduled' as RefreshMode, schedule: {} },
      }
      const errors = validateViewDefinition(def as any)
      expect(
        errors.some(
          (e) =>
            e.field === 'options.schedule' &&
            e.message.includes('cron or intervalMs')
        )
      ).toBe(true)
    })

    it('returns error for invalid cron expression', () => {
      const def = {
        name: viewName('test'),
        source: 'users',
        query: {},
        options: {
          refreshMode: 'scheduled' as RefreshMode,
          schedule: { cron: 'invalid' },
        },
      }
      const errors = validateViewDefinition(def as any)
      expect(errors.some((e) => e.field === 'options.schedule.cron')).toBe(true)
    })

    it('returns error for invalid interval', () => {
      const def = {
        name: viewName('test'),
        source: 'users',
        query: {},
        options: {
          refreshMode: 'scheduled' as RefreshMode,
          schedule: { intervalMs: -1 },
        },
      }
      const errors = validateViewDefinition(def as any)
      expect(errors.some((e) => e.field === 'options.schedule.intervalMs')).toBe(true)
    })

    it('accepts valid scheduled config with cron', () => {
      const def = {
        name: viewName('test'),
        source: 'users',
        query: {},
        options: {
          refreshMode: 'scheduled' as RefreshMode,
          schedule: { cron: '0 * * * *' },
        },
      }
      const errors = validateViewDefinition(def as any)
      expect(errors).toHaveLength(0)
    })

    it('accepts valid scheduled config with interval', () => {
      const def = {
        name: viewName('test'),
        source: 'users',
        query: {},
        options: {
          refreshMode: 'scheduled' as RefreshMode,
          schedule: { intervalMs: 60000 },
        },
      }
      const errors = validateViewDefinition(def as any)
      expect(errors).toHaveLength(0)
    })

    it('returns multiple errors for multiple issues', () => {
      const def = {
        name: '123-invalid',
        query: 'not-object',
      }
      const errors = validateViewDefinition(def as any)
      expect(errors.length).toBeGreaterThan(1)
    })
  })
})

// =============================================================================
// defineView API Tests
// =============================================================================

describe('defineView API', () => {
  it('creates a valid view definition with defaults', () => {
    const result = defineView({
      name: 'active_users',
      source: 'users',
      query: { filter: { status: 'active' } },
    })

    expect(result.success).toBe(true)
    expect(result.definition).toBeDefined()
    expect(result.errors).toBeUndefined()

    const def = result.definition!
    expect(def.name).toBe('active_users')
    expect(def.source).toBe('users')
    expect(def.query).toEqual({ filter: { status: 'active' } })
    expect(def.options.refreshMode).toBe('manual')
    expect(def.options.refreshStrategy).toBe('full')
    expect(def.options.populateOnCreate).toBe(false)
  })

  it('applies custom options', () => {
    const result = defineView({
      name: 'streaming_view',
      source: 'events',
      query: { filter: {} },
      options: {
        refreshMode: 'streaming',
        refreshStrategy: 'incremental',
        description: 'Real-time event view',
      },
    })

    expect(result.success).toBe(true)
    const def = result.definition!
    expect(def.options.refreshMode).toBe('streaming')
    expect(def.options.refreshStrategy).toBe('incremental')
    expect(def.options.description).toBe('Real-time event view')
  })

  it('creates scheduled view with cron', () => {
    const result = defineView({
      name: 'hourly_summary',
      source: 'logs',
      query: {
        pipeline: [
          { $match: { level: 'error' } },
          { $group: { _id: '$service', count: { $sum: 1 } } },
        ],
      },
      options: {
        refreshMode: 'scheduled',
        schedule: { cron: '0 * * * *', timezone: 'UTC' },
      },
    })

    expect(result.success).toBe(true)
    const def = result.definition!
    expect(def.options.refreshMode).toBe('scheduled')
    expect(def.options.schedule?.cron).toBe('0 * * * *')
    expect(def.options.schedule?.timezone).toBe('UTC')
  })

  it('returns errors for invalid input', () => {
    const result = defineView({
      name: '123-invalid-name',
      source: 'users',
      query: { filter: {} },
    })

    expect(result.success).toBe(false)
    expect(result.definition).toBeUndefined()
    expect(result.errors).toBeDefined()
    expect(result.errors!.length).toBeGreaterThan(0)
    expect(result.errors!.some((e) => e.field === 'name')).toBe(true)
  })

  it('returns errors for scheduled mode without schedule', () => {
    const result = defineView({
      name: 'test_view',
      source: 'users',
      query: { filter: {} },
      options: { refreshMode: 'scheduled' },
    })

    expect(result.success).toBe(false)
    expect(result.errors).toBeDefined()
    expect(result.errors!.some((e) => e.field === 'options.schedule')).toBe(true)
  })

  it('preserves DEFAULT_VIEW_OPTIONS values when not overridden', () => {
    const result = defineView({
      name: 'test_view',
      source: 'users',
      query: { filter: {} },
      options: { description: 'Custom description' },
    })

    expect(result.success).toBe(true)
    const def = result.definition!
    // Defaults should be applied
    expect(def.options.refreshMode).toBe(DEFAULT_VIEW_OPTIONS.refreshMode)
    expect(def.options.refreshStrategy).toBe(DEFAULT_VIEW_OPTIONS.refreshStrategy)
    expect(def.options.populateOnCreate).toBe(DEFAULT_VIEW_OPTIONS.populateOnCreate)
    // Custom option should be preserved
    expect(def.options.description).toBe('Custom description')
  })
})

// =============================================================================
// Interface Type Tests (compile-time checks)
// =============================================================================

describe('Interface Types', () => {
  it('ViewQuery supports filter', () => {
    const query: ViewQuery = {
      filter: { status: 'active', views: { $gt: 100 } },
    }
    expect(query.filter).toBeDefined()
  })

  it('ViewQuery supports projection', () => {
    const query: ViewQuery = {
      project: { name: 1, email: 1, password: 0 },
    }
    expect(query.project).toBeDefined()
  })

  it('ViewQuery supports sort', () => {
    const query: ViewQuery = {
      sort: { createdAt: -1, name: 1 },
    }
    expect(query.sort).toBeDefined()
  })

  it('ViewQuery supports aggregation pipeline', () => {
    const query: ViewQuery = {
      pipeline: [
        { $match: { status: 'active' } },
        { $group: { _id: '$type', total: { $sum: 1 } } },
        { $sort: { total: -1 } },
        { $limit: 10 },
      ],
    }
    expect(query.pipeline).toHaveLength(4)
  })

  it('ViewOptions supports all options', () => {
    const options: ViewOptions = {
      refreshMode: 'scheduled',
      refreshStrategy: 'incremental',
      schedule: { cron: '0 * * * *', timezone: 'UTC' },
      maxStalenessMs: 60000,
      populateOnCreate: true,
      indexes: ['status', 'createdAt'],
      description: 'Test view',
      tags: ['production', 'critical'],
      metadata: { owner: 'team-a' },
    }
    expect(options.refreshMode).toBe('scheduled')
  })

  it('ScheduleOptions supports cron or interval', () => {
    const cronSchedule: ScheduleOptions = {
      cron: '0 0 * * *',
      timezone: 'America/New_York',
    }
    expect(cronSchedule.cron).toBeDefined()

    const intervalSchedule: ScheduleOptions = {
      intervalMs: 300000,
    }
    expect(intervalSchedule.intervalMs).toBe(300000)
  })

  it('ViewStats has all required fields', () => {
    const stats: ViewStats = {
      totalRefreshes: 100,
      successfulRefreshes: 95,
      failedRefreshes: 5,
      avgRefreshDurationMs: 2500,
      queryCount: 10000,
      cacheHitRatio: 0.85,
    }
    expect(stats.successfulRefreshes + stats.failedRefreshes).toBe(stats.totalRefreshes)
  })

  it('ViewMetadata tracks all state', () => {
    const metadata: ViewMetadata = {
      definition: {
        name: viewName('test'),
        source: 'users',
        query: { filter: {} },
        options: { refreshMode: 'manual' },
      },
      state: 'ready',
      createdAt: new Date(),
      lastRefreshedAt: new Date(),
      nextRefreshAt: new Date(Date.now() + 3600000),
      lastRefreshDurationMs: 1500,
      documentCount: 5000,
      sizeBytes: 1024 * 1024,
      error: undefined,
      version: 5,
    }
    expect(metadata.version).toBe(5)
  })
})

// =============================================================================
// MaterializedViewDefinition Tests
// =============================================================================

describe('MaterializedViewDefinition', () => {
  describe('isValidMaterializedViewDefinition', () => {
    it('returns true for valid simple MV definition', () => {
      const mvDef = {
        name: 'active_users',
        source: 'users',
        filter: { status: 'active' },
        refreshStrategy: 'incremental',
      }
      expect(isValidMaterializedViewDefinition(mvDef)).toBe(true)
    })

    it('returns true for valid pipeline MV definition', () => {
      const mvDef = {
        name: 'user_stats',
        source: 'users',
        pipeline: [{ $group: { _id: '$status', count: { $sum: 1 } } }],
        refreshStrategy: 'full',
      }
      expect(isValidMaterializedViewDefinition(mvDef)).toBe(true)
    })

    it('returns true for streaming refresh strategy', () => {
      const mvDef = {
        name: 'live_events',
        source: 'events',
        refreshStrategy: 'streaming',
      }
      expect(isValidMaterializedViewDefinition(mvDef)).toBe(true)
    })

    it('returns false for missing name', () => {
      const mvDef = {
        source: 'users',
        refreshStrategy: 'full',
      }
      expect(isValidMaterializedViewDefinition(mvDef)).toBe(false)
    })

    it('returns false for missing source', () => {
      const mvDef = {
        name: 'test_view',
        refreshStrategy: 'full',
      }
      expect(isValidMaterializedViewDefinition(mvDef)).toBe(false)
    })

    it('returns false for missing refresh strategy', () => {
      const mvDef = {
        name: 'test_view',
        source: 'users',
      }
      expect(isValidMaterializedViewDefinition(mvDef)).toBe(false)
    })

    it('returns false for invalid refresh strategy', () => {
      const mvDef = {
        name: 'test_view',
        source: 'users',
        refreshStrategy: 'merge', // Not valid anymore
      }
      expect(isValidMaterializedViewDefinition(mvDef)).toBe(false)
    })

    it('returns false for non-object values', () => {
      expect(isValidMaterializedViewDefinition(null)).toBe(false)
      expect(isValidMaterializedViewDefinition(undefined)).toBe(false)
      expect(isValidMaterializedViewDefinition('string')).toBe(false)
      expect(isValidMaterializedViewDefinition(123)).toBe(false)
    })
  })

  describe('toViewDefinition', () => {
    it('converts simple MV definition to ViewDefinition', () => {
      const mvDef = {
        name: 'active_users',
        description: 'Active users only',
        source: 'users',
        filter: { status: 'active' },
        sort: { createdAt: -1 },
        refreshStrategy: 'incremental' as const,
        refreshMode: 'streaming' as const,
        maxStalenessMs: 5000,
        indexes: ['email'],
        meta: { owner: 'admin' },
      }

      const viewDef = toViewDefinition(mvDef)

      expect(viewDef.name).toBe('active_users')
      expect(viewDef.source).toBe('users')
      expect(viewDef.query.filter).toEqual({ status: 'active' })
      expect(viewDef.query.sort).toEqual({ createdAt: -1 })
      expect(viewDef.options.refreshMode).toBe('streaming')
      expect(viewDef.options.refreshStrategy).toBe('incremental')
      expect(viewDef.options.maxStalenessMs).toBe(5000)
      expect(viewDef.options.indexes).toEqual(['email'])
      expect(viewDef.options.description).toBe('Active users only')
      expect(viewDef.options.metadata).toEqual({ owner: 'admin' })
    })

    it('converts pipeline MV definition to ViewDefinition', () => {
      const mvDef = {
        name: 'user_stats',
        source: 'users',
        pipeline: [
          { $group: { _id: '$status', count: { $sum: 1 } } },
        ],
        refreshStrategy: 'full' as const,
        schedule: { cron: '0 * * * *' },
      }

      const viewDef = toViewDefinition(mvDef)

      expect(viewDef.query.pipeline).toEqual([
        { $group: { _id: '$status', count: { $sum: 1 } } },
      ])
      expect(viewDef.options.schedule).toEqual({ cron: '0 * * * *' })
    })

    it('handles minimal MV definition', () => {
      const mvDef = {
        name: 'minimal_view',
        source: 'data',
        refreshStrategy: 'full' as const,
      }

      const viewDef = toViewDefinition(mvDef)

      expect(viewDef.name).toBe('minimal_view')
      expect(viewDef.source).toBe('data')
      expect(viewDef.options.refreshMode).toBe('manual')
    })
  })

  describe('fromViewDefinition', () => {
    it('converts ViewDefinition back to MaterializedViewDefinition', () => {
      const viewDef = {
        name: viewName('user_stats'),
        source: 'users',
        query: {
          pipeline: [{ $group: { _id: '$status', count: { $sum: 1 } } }],
        },
        options: {
          refreshMode: 'scheduled' as const,
          refreshStrategy: 'full' as const,
          schedule: { cron: '0 * * * *' },
          description: 'User statistics',
        },
      }

      const mvDef = fromViewDefinition(viewDef)

      expect(mvDef.name).toBe('user_stats')
      expect(mvDef.source).toBe('users')
      expect(mvDef.pipeline).toEqual([{ $group: { _id: '$status', count: { $sum: 1 } } }])
      expect(mvDef.refreshStrategy).toBe('full')
      expect(mvDef.refreshMode).toBe('scheduled')
      expect(mvDef.schedule).toEqual({ cron: '0 * * * *' })
      expect(mvDef.description).toBe('User statistics')
    })

    it('converts simple filter ViewDefinition', () => {
      const viewDef = {
        name: viewName('active_users'),
        source: 'users',
        query: {
          filter: { status: 'active' },
          project: { name: 1, email: 1 },
          sort: { createdAt: -1 },
        },
        options: {
          refreshMode: 'manual' as const,
          refreshStrategy: 'incremental' as const,
        },
      }

      const mvDef = fromViewDefinition(viewDef)

      expect(mvDef.filter).toEqual({ status: 'active' })
      expect(mvDef.project).toEqual({ name: 1, email: 1 })
      expect(mvDef.sort).toEqual({ createdAt: -1 })
      expect(mvDef.refreshStrategy).toBe('incremental')
    })
  })

  describe('round-trip conversion', () => {
    it('preserves all fields in round-trip', () => {
      const original = {
        name: 'test_view',
        description: 'Test description',
        source: 'posts',
        filter: { published: true },
        project: { title: 1, content: 1 },
        sort: { createdAt: -1 },
        refreshStrategy: 'streaming' as const,
        refreshMode: 'streaming' as const,
        maxStalenessMs: 1000,
        populateOnCreate: true,
        indexes: ['title', 'author'],
        dependencies: ['users'],
        meta: { priority: 'high' },
      }

      const converted = fromViewDefinition(toViewDefinition(original))

      expect(converted.name).toBe(original.name)
      expect(converted.description).toBe(original.description)
      expect(converted.source).toBe(original.source)
      expect(converted.filter).toEqual(original.filter)
      expect(converted.project).toEqual(original.project)
      expect(converted.sort).toEqual(original.sort)
      expect(converted.refreshStrategy).toBe(original.refreshStrategy)
      expect(converted.refreshMode).toBe(original.refreshMode)
      expect(converted.maxStalenessMs).toBe(original.maxStalenessMs)
      expect(converted.populateOnCreate).toBe(original.populateOnCreate)
      expect(converted.indexes).toEqual(original.indexes)
    })
  })
})

// =============================================================================
// IngestSource Type Tests
// =============================================================================

import {
  type IngestSource,
  type KnownIngestSource,
  type CustomIngestSource,
  type CollectionDefinition,
  customIngestSource,
  isKnownIngestSource,
  isIngestSource,
  KNOWN_INGEST_SOURCES,
} from '@/materialized-views/types'
import { isCustomIngestSource, getCustomSourceHandler } from '@/materialized-views/types'

describe('IngestSource Types', () => {
  describe('KNOWN_INGEST_SOURCES', () => {
    it('contains all expected known sources', () => {
      expect(KNOWN_INGEST_SOURCES).toContain('ai-sdk')
      expect(KNOWN_INGEST_SOURCES).toContain('tail')
      expect(KNOWN_INGEST_SOURCES).toContain('evalite')
    })

    it('has exactly 3 known sources', () => {
      // Known sources are: 'ai-sdk', 'tail', 'evalite'
      // Custom sources use template literal: `custom:${string}`
      expect(KNOWN_INGEST_SOURCES).toHaveLength(3)
    })

    it('is readonly', () => {
      // Type check - the array should be readonly
      const sources: readonly KnownIngestSource[] = KNOWN_INGEST_SOURCES
      expect(sources).toBeDefined()
    })

    it('does not include "custom" as a known source (uses template literal instead)', () => {
      // 'custom' alone is not valid - custom sources use 'custom:handler-name' format
      expect(KNOWN_INGEST_SOURCES).not.toContain('custom')
    })
  })

  describe('customIngestSource', () => {
    it('creates a CustomIngestSource with the custom: prefix', () => {
      const source = customIngestSource('my-handler')
      expect(source).toBe('custom:my-handler')
    })

    it('adds custom: prefix to any string', () => {
      expect(customIngestSource('custom-webhook')).toBe('custom:custom-webhook')
      expect(customIngestSource('123')).toBe('custom:123')
      expect(customIngestSource('_internal')).toBe('custom:_internal')
    })

    it('returns a value usable as IngestSource', () => {
      const source: IngestSource = customIngestSource('my-source')
      expect(source).toBe('custom:my-source')
    })
  })

  describe('isKnownIngestSource', () => {
    it('returns true for known sources', () => {
      expect(isKnownIngestSource('ai-sdk')).toBe(true)
      expect(isKnownIngestSource('tail')).toBe(true)
      expect(isKnownIngestSource('evalite')).toBe(true)
    })

    it('returns false for custom sources (they use custom: prefix)', () => {
      expect(isKnownIngestSource('custom')).toBe(false)
      expect(isKnownIngestSource('custom:my-handler')).toBe(false)
    })

    it('returns false for unknown string sources', () => {
      expect(isKnownIngestSource('my-custom-handler')).toBe(false)
      expect(isKnownIngestSource('webhook')).toBe(false)
      expect(isKnownIngestSource('')).toBe(false)
    })

    it('returns false for non-string values', () => {
      expect(isKnownIngestSource(null)).toBe(false)
      expect(isKnownIngestSource(undefined)).toBe(false)
      expect(isKnownIngestSource(123)).toBe(false)
      expect(isKnownIngestSource({})).toBe(false)
      expect(isKnownIngestSource(['ai-sdk'])).toBe(false)
    })

    it('narrows type correctly', () => {
      const source: string = 'ai-sdk'
      if (isKnownIngestSource(source)) {
        // TypeScript should narrow to KnownIngestSource
        const known: KnownIngestSource = source
        expect(known).toBe('ai-sdk')
      }
    })
  })

  describe('isCustomIngestSource', () => {
    it('returns true for sources with custom: prefix', () => {
      expect(isCustomIngestSource('custom:my-handler')).toBe(true)
      expect(isCustomIngestSource('custom:webhook')).toBe(true)
      expect(isCustomIngestSource('custom:stripe-events')).toBe(true)
    })

    it('returns false for known sources', () => {
      expect(isCustomIngestSource('ai-sdk')).toBe(false)
      expect(isCustomIngestSource('tail')).toBe(false)
      expect(isCustomIngestSource('evalite')).toBe(false)
    })

    it('returns false for strings without custom: prefix', () => {
      expect(isCustomIngestSource('my-handler')).toBe(false)
      expect(isCustomIngestSource('custom')).toBe(false)
      expect(isCustomIngestSource('')).toBe(false)
    })

    it('returns false for incomplete custom: prefix', () => {
      // 'custom:' alone (without handler name) is not valid
      expect(isCustomIngestSource('custom:')).toBe(false)
    })

    it('returns false for non-string values', () => {
      expect(isCustomIngestSource(null)).toBe(false)
      expect(isCustomIngestSource(undefined)).toBe(false)
      expect(isCustomIngestSource(123)).toBe(false)
      expect(isCustomIngestSource({})).toBe(false)
    })
  })

  describe('isIngestSource', () => {
    it('returns true for known sources', () => {
      expect(isIngestSource('ai-sdk')).toBe(true)
      expect(isIngestSource('tail')).toBe(true)
      expect(isIngestSource('evalite')).toBe(true)
    })

    it('returns true for custom sources with custom: prefix', () => {
      expect(isIngestSource('custom:my-handler')).toBe(true)
      expect(isIngestSource('custom:webhook')).toBe(true)
      expect(isIngestSource('custom:stripe-events')).toBe(true)
    })

    it('returns false for strings without known source or custom: prefix', () => {
      // These are now rejected - type safety requires explicit prefixing
      expect(isIngestSource('my-custom-handler')).toBe(false)
      expect(isIngestSource('webhook')).toBe(false)
      expect(isIngestSource('some-other-source')).toBe(false)
    })

    it('returns false for empty strings', () => {
      expect(isIngestSource('')).toBe(false)
    })

    it('returns false for non-string values', () => {
      expect(isIngestSource(null)).toBe(false)
      expect(isIngestSource(undefined)).toBe(false)
      expect(isIngestSource(123)).toBe(false)
      expect(isIngestSource({})).toBe(false)
    })

    it('narrows type correctly', () => {
      const value: unknown = 'custom:my-source'
      if (isIngestSource(value)) {
        // TypeScript should narrow to IngestSource
        const source: IngestSource = value
        expect(source).toBe('custom:my-source')
      }
    })
  })

  describe('getCustomSourceHandler', () => {
    it('extracts handler name from custom source', () => {
      expect(getCustomSourceHandler('custom:my-handler')).toBe('my-handler')
      expect(getCustomSourceHandler('custom:stripe-events')).toBe('stripe-events')
      expect(getCustomSourceHandler('custom:123')).toBe('123')
    })
  })

  describe('Type discrimination patterns', () => {
    it('supports switch on known sources', () => {
      const handleSource = (source: KnownIngestSource): string => {
        switch (source) {
          case 'ai-sdk':
            return 'AI SDK handler'
          case 'tail':
            return 'Tail handler'
          case 'evalite':
            return 'Evalite handler'
        }
      }

      expect(handleSource('ai-sdk')).toBe('AI SDK handler')
      expect(handleSource('tail')).toBe('Tail handler')
      expect(handleSource('evalite')).toBe('Evalite handler')
    })

    it('supports discriminating between known and custom sources', () => {
      const describeSource = (source: IngestSource): string => {
        if (isKnownIngestSource(source)) {
          return `Known source: ${source}`
        }
        // For custom sources, extract the handler name
        return `Custom source: ${getCustomSourceHandler(source as CustomIngestSource)}`
      }

      expect(describeSource('ai-sdk')).toBe('Known source: ai-sdk')
      expect(describeSource(customIngestSource('my-handler'))).toBe('Custom source: my-handler')
    })
  })

  describe('CollectionDefinition with IngestSource', () => {
    it('allows known sources directly', () => {
      const collection = {
        $type: 'AIRequest',
        $ingest: 'ai-sdk' as KnownIngestSource,
        modelId: 'string!',
      }
      expect(collection.$ingest).toBe('ai-sdk')
    })

    it('uses custom: prefix for custom handlers', () => {
      const collection = {
        $type: 'CustomEvent',
        $ingest: customIngestSource('my-webhook'),
        eventType: 'string!',
      }
      expect(collection.$ingest).toBe('custom:my-webhook')
    })

    it('allows template literal syntax for custom sources', () => {
      const collection = {
        $type: 'WebhookEvent',
        $ingest: 'custom:stripe-events' as CustomIngestSource,
        payload: 'json!',
      }
      expect(collection.$ingest).toBe('custom:stripe-events')
    })
  })

  describe('CollectionFieldValue (tightened index signature)', () => {
    it('accepts IceType field definition strings', () => {
      const collection: CollectionDefinition = {
        $type: 'User',
        name: 'string!',          // required string
        email: 'string!#',        // required string, indexed
        age: 'int?',              // optional int
        score: 'float!',          // required float
        active: 'boolean!',       // required boolean
        createdAt: 'timestamp!',  // required timestamp
        bio: 'text?',             // optional text
        metadata: 'json?',        // optional json
        tags: 'string[]',         // array of strings
      }
      expect(collection.$type).toBe('User')
      expect(collection.name).toBe('string!')
      expect(collection.email).toBe('string!#')
      expect(collection.age).toBe('int?')
    })

    it('accepts relationship field definitions', () => {
      const collection: CollectionDefinition = {
        $type: 'Post',
        title: 'string!',
        author: '-> User',                    // relationship to User
        tags: '-> Tag.posts[]',               // many-to-many via posts
        category: '-> Category.posts',        // belongs to category
      }
      expect(collection.author).toBe('-> User')
      expect(collection.tags).toBe('-> Tag.posts[]')
    })

    it('accepts $type directive as string', () => {
      const collection: CollectionDefinition = {
        $type: 'CustomEntity',
        field: 'string!',
      }
      expect(collection.$type).toBe('CustomEntity')
    })

    it('accepts $ingest directive with known sources', () => {
      const collection: CollectionDefinition = {
        $type: 'AIRequest',
        $ingest: 'ai-sdk',
        modelId: 'string!',
      }
      expect(collection.$ingest).toBe('ai-sdk')
    })

    it('accepts $ingest directive with custom sources', () => {
      const collection: CollectionDefinition = {
        $type: 'WebhookEvent',
        $ingest: customIngestSource('my-webhook'),
        payload: 'json!',
      }
      expect(collection.$ingest).toBe('custom:my-webhook')
    })

    it('accepts undefined for optional fields', () => {
      const collection: CollectionDefinition = {
        $type: 'Minimal',
        optionalField: undefined,
      }
      expect(collection.optionalField).toBeUndefined()
    })

    it('supports full stream collection pattern', () => {
      const collection: CollectionDefinition = {
        $type: 'TailEvent',
        $ingest: 'tail',
        scriptName: 'string!',
        outcome: 'string!',
        eventTimestamp: 'timestamp!',
        cpuTime: 'int?',
        wallTime: 'int?',
      }
      expect(collection.$type).toBe('TailEvent')
      expect(collection.$ingest).toBe('tail')
      expect(collection.scriptName).toBe('string!')
    })
  })
})

// =============================================================================
// Edge Cases Tests
// =============================================================================

describe('Edge Cases', () => {
  describe('View Names', () => {
    it('handles single character names', () => {
      expect(isValidViewName('a')).toBe(true)
      expect(isValidViewName('_')).toBe(true)
    })

    it('handles very long names', () => {
      const longName = 'a'.repeat(100)
      expect(isValidViewName(longName)).toBe(true)
    })

    it('handles unicode characters', () => {
      // Currently only ASCII alphanumeric + underscore
      expect(isValidViewName('viewé')).toBe(false)
      expect(isValidViewName('view_name')).toBe(true)
    })
  })

  describe('Cron Expressions', () => {
    it('handles complex cron patterns', () => {
      expect(isValidCronExpression('0 0 1,15 * *')).toBe(true) // 1st and 15th
      expect(isValidCronExpression('0 */2 * * *')).toBe(true) // Every 2 hours
      expect(isValidCronExpression('0 0 * * 0')).toBe(true) // Sundays
    })
  })

  describe('defineView with empty queries', () => {
    it('accepts empty filter object', () => {
      const result = defineView({
        name: 'all_users',
        source: 'users',
        query: { filter: {} },
      })
      expect(result.success).toBe(true)
    })

    it('accepts completely empty query', () => {
      const result = defineView({
        name: 'all_users',
        source: 'users',
        query: {},
      })
      expect(result.success).toBe(true)
    })
  })
})
