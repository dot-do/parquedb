/**
 * Tests for defineView() input validation
 *
 * Tests for validating materialized view definitions with the DefineViewInput API.
 */

import { describe, it, expect } from 'vitest'
import {
  defineView,
  MVDefinitionError,
} from '../../../src/materialized-views/define'

describe('defineView input validation', () => {
  describe('valid inputs', () => {
    it('should create a simple view with $from', () => {
      const view = defineView({ $from: 'Orders' })

      expect(view.$from).toBe('Orders')
      expect(view.$refresh?.mode).toBe('streaming')
    })

    it('should create a view with $expand array', () => {
      const view = defineView({
        $from: 'Orders',
        $expand: ['customer', 'items.product'],
      })

      expect(view.$expand).toEqual(['customer', 'items.product'])
    })

    it('should create a view with $flatten', () => {
      const view = defineView({
        $from: 'Orders',
        $expand: ['customer'],
        $flatten: { customer: 'buyer' },
      })

      expect(view.$flatten).toEqual({ customer: 'buyer' })
    })

    it('should create a view with $filter', () => {
      const view = defineView({
        $from: 'Orders',
        $filter: { status: 'completed' },
      })

      expect(view.$filter).toEqual({ status: 'completed' })
    })

    it('should create a view with $select', () => {
      const view = defineView({
        $from: 'Orders',
        $select: { orderId: '$id', total: 'total' },
      })

      expect(view.$select).toEqual({ orderId: '$id', total: 'total' })
    })

    it('should create a view with $groupBy', () => {
      const view = defineView({
        $from: 'Orders',
        $groupBy: ['status', { date: '$createdAt' }],
      })

      expect(view.$groupBy).toEqual(['status', { date: '$createdAt' }])
    })

    it('should create a view with $compute', () => {
      const view = defineView({
        $from: 'Orders',
        $compute: {
          count: { $count: '*' },
          totalRevenue: { $sum: 'total' },
        },
      })

      expect(view.$compute).toEqual({
        count: { $count: '*' },
        totalRevenue: { $sum: 'total' },
      })
    })

    it('should create a view with $unnest', () => {
      const view = defineView({
        $from: 'Orders',
        $unnest: 'items',
      })

      expect(view.$unnest).toBe('items')
    })

    it('should create a view with $refresh config', () => {
      const view = defineView({
        $from: 'Orders',
        $refresh: { mode: 'scheduled', schedule: '0 * * * *' },
      })

      expect(view.$refresh?.mode).toBe('scheduled')
      expect(view.$refresh?.schedule).toBe('0 * * * *')
    })
  })

  describe('$from validation', () => {
    it('should reject missing $from', () => {
      // @ts-expect-error - Testing runtime validation
      expect(() => defineView({})).toThrow(MVDefinitionError)
    })

    it('should reject empty $from', () => {
      expect(() => defineView({ $from: '' })).toThrow(MVDefinitionError)
    })

    it('should reject non-string $from', () => {
      // @ts-expect-error - Testing runtime validation
      expect(() => defineView({ $from: 123 })).toThrow(MVDefinitionError)
    })

    it('should include error code in $from validation error', () => {
      try {
        defineView({ $from: '' })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(MVDefinitionError)
        expect((error as MVDefinitionError).code).toBe('INVALID_FROM')
        expect((error as MVDefinitionError).field).toBe('$from')
      }
    })
  })

  describe('$expand validation', () => {
    it('should reject non-array $expand', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $expand: 'customer',
      })).toThrow(MVDefinitionError)
    })

    it('should reject $expand with non-string items', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $expand: ['customer', 123],
      })).toThrow(MVDefinitionError)
    })

    it('should reject $expand with empty string items', () => {
      expect(() => defineView({
        $from: 'Orders',
        $expand: ['customer', ''],
      })).toThrow(MVDefinitionError)
    })

    it('should include error code in $expand validation error', () => {
      try {
        defineView({
          $from: 'Orders',
          // @ts-expect-error - Testing runtime validation
          $expand: 'not-an-array',
        })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(MVDefinitionError)
        expect((error as MVDefinitionError).code).toBe('INVALID_EXPAND')
        expect((error as MVDefinitionError).field).toBe('$expand')
      }
    })
  })

  describe('$flatten validation', () => {
    it('should reject non-object $flatten', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $flatten: 'invalid',
      })).toThrow(MVDefinitionError)
    })

    it('should reject array $flatten', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $flatten: ['customer'],
      })).toThrow(MVDefinitionError)
    })

    it('should reject $flatten with non-string values', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $flatten: { customer: 123 },
      })).toThrow(MVDefinitionError)
    })

    it('should reject $flatten with empty string values', () => {
      expect(() => defineView({
        $from: 'Orders',
        $flatten: { customer: '' },
      })).toThrow(MVDefinitionError)
    })

    it('should include error code in $flatten validation error', () => {
      try {
        defineView({
          $from: 'Orders',
          // @ts-expect-error - Testing runtime validation
          $flatten: 'invalid',
        })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(MVDefinitionError)
        expect((error as MVDefinitionError).code).toBe('INVALID_FLATTEN')
        expect((error as MVDefinitionError).field).toBe('$flatten')
      }
    })
  })

  describe('$select validation', () => {
    it('should reject non-object $select', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $select: 'invalid',
      })).toThrow(MVDefinitionError)
    })

    it('should reject array $select', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $select: ['field1', 'field2'],
      })).toThrow(MVDefinitionError)
    })

    it('should reject $select with non-string values', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $select: { total: 123 },
      })).toThrow(MVDefinitionError)
    })
  })

  describe('$groupBy validation', () => {
    it('should reject non-array $groupBy', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $groupBy: 'status',
      })).toThrow(MVDefinitionError)
    })

    it('should reject $groupBy with invalid items', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $groupBy: ['status', 123],
      })).toThrow(MVDefinitionError)
    })

    it('should include error code in $groupBy validation error', () => {
      try {
        defineView({
          $from: 'Orders',
          // @ts-expect-error - Testing runtime validation
          $groupBy: 'not-an-array',
        })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(MVDefinitionError)
        expect((error as MVDefinitionError).code).toBe('INVALID_GROUP_BY')
        expect((error as MVDefinitionError).field).toBe('$groupBy')
      }
    })
  })

  describe('$compute validation', () => {
    it('should reject non-object $compute', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $compute: 'invalid',
      })).toThrow(MVDefinitionError)
    })

    it('should reject array $compute', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $compute: [{ $count: '*' }],
      })).toThrow(MVDefinitionError)
    })

    it('should reject $compute with non-object values', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $compute: { count: 'invalid' },
      })).toThrow(MVDefinitionError)
    })

    it('should include error code in $compute validation error', () => {
      try {
        defineView({
          $from: 'Orders',
          // @ts-expect-error - Testing runtime validation
          $compute: 'not-an-object',
        })
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(MVDefinitionError)
        expect((error as MVDefinitionError).code).toBe('INVALID_COMPUTE')
        expect((error as MVDefinitionError).field).toBe('$compute')
      }
    })
  })

  describe('$unnest validation', () => {
    it('should reject non-string $unnest', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $unnest: 123,
      })).toThrow(MVDefinitionError)
    })

    it('should reject empty string $unnest', () => {
      expect(() => defineView({
        $from: 'Orders',
        $unnest: '',
      })).toThrow(MVDefinitionError)
    })
  })

  describe('$refresh validation', () => {
    it('should reject invalid refresh mode', () => {
      expect(() => defineView({
        $from: 'Orders',
        // @ts-expect-error - Testing runtime validation
        $refresh: { mode: 'invalid' },
      })).toThrow(MVDefinitionError)
    })

    it('should reject scheduled mode without schedule', () => {
      expect(() => defineView({
        $from: 'Orders',
        $refresh: { mode: 'scheduled' },
      })).toThrow(MVDefinitionError)
    })

    it('should reject invalid cron expression', () => {
      expect(() => defineView({
        $from: 'Orders',
        $refresh: { mode: 'scheduled', schedule: 'invalid-cron' },
      })).toThrow(MVDefinitionError)
    })

    it('should accept valid scheduled refresh', () => {
      const view = defineView({
        $from: 'Orders',
        $refresh: { mode: 'scheduled', schedule: '0 * * * *' },
      })

      expect(view.$refresh?.mode).toBe('scheduled')
      expect(view.$refresh?.schedule).toBe('0 * * * *')
    })

    it('should accept manual refresh mode', () => {
      const view = defineView({
        $from: 'Orders',
        $refresh: { mode: 'manual' },
      })

      expect(view.$refresh?.mode).toBe('manual')
    })
  })

  describe('MVDefinitionError', () => {
    it('should have correct error name', () => {
      try {
        defineView({ $from: '' })
        expect.fail('Should have thrown')
      } catch (error) {
        expect((error as MVDefinitionError).name).toBe('MVDefinitionError')
      }
    })

    it('should have descriptive message', () => {
      try {
        defineView({ $from: '' })
        expect.fail('Should have thrown')
      } catch (error) {
        expect((error as MVDefinitionError).message).toContain('$from')
      }
    })
  })
})
