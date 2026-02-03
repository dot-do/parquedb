/**
 * Property-Based Tests for Filter Evaluation
 *
 * Uses fast-check to generate random filter expressions and documents
 * to verify filter evaluation properties and invariants.
 *
 * Coverage:
 * 1. Comparison operators ($eq, $ne, $gt, $gte, $lt, $lte)
 * 2. Logical operators ($and, $or, $not)
 * 3. Array operators ($in, $nin, $all, $elemMatch)
 * 4. String operators ($regex)
 * 5. Edge cases (null, undefined, empty arrays, nested objects)
 */

import { describe, it, expect } from 'vitest'
import fc from 'fast-check'
import { matchesFilter, matchesCondition, createPredicate } from '../../../src/query/filter'
import type { Filter } from '../../../src/types/filter'

// =============================================================================
// Custom Arbitraries (Generators)
// =============================================================================

/**
 * Generator for primitive values (excluding symbols and bigint for simplicity)
 */
const primitiveArb = fc.oneof(
  fc.string(),
  fc.integer(),
  fc.double({ noNaN: true, noDefaultInfinity: true }),
  fc.boolean(),
  fc.constant(null),
  fc.constant(undefined),
)

/**
 * Generator for JSON-compatible values (recursive)
 */
const jsonValueArb: fc.Arbitrary<unknown> = fc.letrec(tie => ({
  value: fc.oneof(
    { weight: 5, arbitrary: fc.string() },
    { weight: 5, arbitrary: fc.integer() },
    { weight: 3, arbitrary: fc.double({ noNaN: true, noDefaultInfinity: true }) },
    { weight: 3, arbitrary: fc.boolean() },
    { weight: 2, arbitrary: fc.constant(null) },
    { weight: 1, arbitrary: fc.array(tie('value'), { maxLength: 5 }) },
    { weight: 1, arbitrary: fc.dictionary(fc.string({ minLength: 1, maxLength: 10 }), tie('value'), { maxKeys: 5 }) },
  ),
})).value

/**
 * Safe field name generator - avoids special characters that cause issues
 */
const safeFieldNameArb = fc.string({ minLength: 1, maxLength: 20 }).filter(s =>
  !s.startsWith('$') &&
  !s.includes('.') &&
  !['constructor', 'prototype', '__proto__', 'toString', 'valueOf', 'hasOwnProperty'].includes(s)
)

/**
 * Generator for simple documents (flat objects)
 */
const simpleDocArb = fc.dictionary(
  safeFieldNameArb,
  primitiveArb,
  { minKeys: 1, maxKeys: 10 }
)

/**
 * Generator for nested documents
 */
const nestedDocArb: fc.Arbitrary<Record<string, unknown>> = fc.letrec(tie => ({
  doc: fc.dictionary(
    safeFieldNameArb,
    fc.oneof(
      { weight: 3, arbitrary: primitiveArb },
      { weight: 1, arbitrary: tie('doc') },
      { weight: 1, arbitrary: fc.array(primitiveArb, { maxLength: 5 }) },
    ),
    { minKeys: 1, maxKeys: 5 }
  ),
})).doc as fc.Arbitrary<Record<string, unknown>>

/**
 * Generator for comparable numeric values
 */
const comparableNumberArb = fc.integer({ min: -1000, max: 1000 })

/**
 * Generator for comparable string values
 */
const comparableStringArb = fc.string({ minLength: 0, maxLength: 20 })

/**
 * Generator for dates
 */
const dateArb = fc.date({ min: new Date('2000-01-01'), max: new Date('2030-12-31') })

// =============================================================================
// 1. Comparison Operators Properties
// =============================================================================

describe('Property: Comparison Operators', () => {
  describe('$eq properties', () => {
    it('value $eq itself is always true (reflexivity)', () => {
      fc.assert(
        fc.property(primitiveArb, (value) => {
          // Skip NaN since NaN !== NaN
          if (typeof value === 'number' && Number.isNaN(value)) return true
          return matchesCondition(value, { $eq: value }) === true
        }),
        { numRuns: 500 }
      )
    })

    it('$eq is symmetric: (a $eq b) === (b $eq a) for deep equality', () => {
      fc.assert(
        fc.property(primitiveArb, primitiveArb, (a, b) => {
          // Skip NaN
          if ((typeof a === 'number' && Number.isNaN(a)) || (typeof b === 'number' && Number.isNaN(b))) return true
          const aEqB = matchesCondition(a, { $eq: b })
          const bEqA = matchesCondition(b, { $eq: a })
          return aEqB === bEqA
        }),
        { numRuns: 500 }
      )
    })

    it('direct value comparison equals $eq operator', () => {
      fc.assert(
        fc.property(
          simpleDocArb,
          safeFieldNameArb,
          (doc, field) => {
            if (!(field in doc)) return true
            const value = doc[field]
            // Skip NaN
            if (typeof value === 'number' && Number.isNaN(value)) return true
            const directResult = matchesFilter(doc, { [field]: value })
            const eqResult = matchesFilter(doc, { [field]: { $eq: value } })
            return directResult === eqResult
          }
        ),
        { numRuns: 500 }
      )
    })
  })

  describe('$ne properties', () => {
    it('$ne is the negation of $eq', () => {
      fc.assert(
        fc.property(primitiveArb, primitiveArb, (value, target) => {
          // Skip NaN
          if ((typeof value === 'number' && Number.isNaN(value)) || (typeof target === 'number' && Number.isNaN(target))) return true
          const eqResult = matchesCondition(value, { $eq: target })
          const neResult = matchesCondition(value, { $ne: target })
          return eqResult !== neResult
        }),
        { numRuns: 500 }
      )
    })

    it('value $ne itself is always false', () => {
      fc.assert(
        fc.property(primitiveArb, (value) => {
          // Skip NaN
          if (typeof value === 'number' && Number.isNaN(value)) return true
          return matchesCondition(value, { $ne: value }) === false
        }),
        { numRuns: 500 }
      )
    })
  })

  describe('$gt, $gte, $lt, $lte properties', () => {
    it('$gt and $lte are complements for non-null values', () => {
      fc.assert(
        fc.property(comparableNumberArb, comparableNumberArb, (value, target) => {
          const gtResult = matchesCondition(value, { $gt: target })
          const lteResult = matchesCondition(value, { $lte: target })
          return gtResult !== lteResult
        }),
        { numRuns: 500 }
      )
    })

    it('$lt and $gte are complements for non-null values', () => {
      fc.assert(
        fc.property(comparableNumberArb, comparableNumberArb, (value, target) => {
          const ltResult = matchesCondition(value, { $lt: target })
          const gteResult = matchesCondition(value, { $gte: target })
          return ltResult !== gteResult
        }),
        { numRuns: 500 }
      )
    })

    it('$gte equals $gt OR $eq', () => {
      fc.assert(
        fc.property(comparableNumberArb, comparableNumberArb, (value, target) => {
          const gteResult = matchesCondition(value, { $gte: target })
          const gtResult = matchesCondition(value, { $gt: target })
          const eqResult = matchesCondition(value, { $eq: target })
          return gteResult === (gtResult || eqResult)
        }),
        { numRuns: 500 }
      )
    })

    it('$lte equals $lt OR $eq', () => {
      fc.assert(
        fc.property(comparableNumberArb, comparableNumberArb, (value, target) => {
          const lteResult = matchesCondition(value, { $lte: target })
          const ltResult = matchesCondition(value, { $lt: target })
          const eqResult = matchesCondition(value, { $eq: target })
          return lteResult === (ltResult || eqResult)
        }),
        { numRuns: 500 }
      )
    })

    it('transitivity: if a > b and b > c then a > c', () => {
      fc.assert(
        fc.property(
          comparableNumberArb,
          comparableNumberArb,
          comparableNumberArb,
          (a, b, c) => {
            const aGtB = matchesCondition(a, { $gt: b })
            const bGtC = matchesCondition(b, { $gt: c })
            const aGtC = matchesCondition(a, { $gt: c })
            // If a > b and b > c, then a > c must be true
            if (aGtB && bGtC) {
              return aGtC === true
            }
            return true // No assertion if premise is false
          }
        ),
        { numRuns: 500 }
      )
    })

    it('antisymmetry: if a > b then NOT (b > a)', () => {
      fc.assert(
        fc.property(comparableNumberArb, comparableNumberArb, (a, b) => {
          const aGtB = matchesCondition(a, { $gt: b })
          const bGtA = matchesCondition(b, { $gt: a })
          // Cannot both be true
          return !(aGtB && bGtA)
        }),
        { numRuns: 500 }
      )
    })

    it('trichotomy: exactly one of (a < b), (a == b), (a > b) for numbers', () => {
      fc.assert(
        fc.property(comparableNumberArb, comparableNumberArb, (a, b) => {
          const aLtB = matchesCondition(a, { $lt: b })
          const aEqB = matchesCondition(a, { $eq: b })
          const aGtB = matchesCondition(a, { $gt: b })
          const trueCount = [aLtB, aEqB, aGtB].filter(Boolean).length
          return trueCount === 1
        }),
        { numRuns: 500 }
      )
    })

    it('null/undefined always returns false for comparison operators', () => {
      fc.assert(
        fc.property(comparableNumberArb, fc.constantFrom(null, undefined), (target, value) => {
          const gtResult = matchesCondition(value, { $gt: target })
          const gteResult = matchesCondition(value, { $gte: target })
          const ltResult = matchesCondition(value, { $lt: target })
          const lteResult = matchesCondition(value, { $lte: target })
          return !gtResult && !gteResult && !ltResult && !lteResult
        }),
        { numRuns: 100 }
      )
    })

    it('string comparison is lexicographic', () => {
      fc.assert(
        fc.property(comparableStringArb, comparableStringArb, (a, b) => {
          const aGtB = matchesCondition(a, { $gt: b })
          const jsGt = a > b
          return aGtB === jsGt
        }),
        { numRuns: 500 }
      )
    })
  })

  describe('range query properties', () => {
    it('value in [min, max] satisfies $gte min AND $lte max', () => {
      fc.assert(
        fc.property(
          comparableNumberArb,
          comparableNumberArb,
          comparableNumberArb,
          (value, a, b) => {
            const min = Math.min(a, b)
            const max = Math.max(a, b)
            const inRange = value >= min && value <= max
            const filterResult = matchesCondition(value, { $gte: min, $lte: max })
            return filterResult === inRange
          }
        ),
        { numRuns: 500 }
      )
    })

    it('exclusive range: $gt min AND $lt max', () => {
      fc.assert(
        fc.property(
          comparableNumberArb,
          comparableNumberArb,
          comparableNumberArb,
          (value, a, b) => {
            const min = Math.min(a, b)
            const max = Math.max(a, b)
            const inRange = value > min && value < max
            const filterResult = matchesCondition(value, { $gt: min, $lt: max })
            return filterResult === inRange
          }
        ),
        { numRuns: 500 }
      )
    })
  })
})

// =============================================================================
// 2. Logical Operators Properties
// =============================================================================

describe('Property: Logical Operators', () => {
  describe('$and properties', () => {
    it('$and with single condition equals that condition', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc)
          if (keys.length === 0) return true
          const key = keys[0]!
          const value = doc[key]
          if (typeof value === 'number' && Number.isNaN(value)) return true

          const singleResult = matchesFilter(doc, { [key]: value })
          const andResult = matchesFilter(doc, { $and: [{ [key]: value }] })
          return singleResult === andResult
        }),
        { numRuns: 500 }
      )
    })

    it('$and is commutative', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc).slice(0, 2)
          if (keys.length < 2) return true
          const [k1, k2] = keys as [string, string]
          const v1 = doc[k1]
          const v2 = doc[k2]
          if ((typeof v1 === 'number' && Number.isNaN(v1)) || (typeof v2 === 'number' && Number.isNaN(v2))) return true

          const order1 = matchesFilter(doc, { $and: [{ [k1]: v1 }, { [k2]: v2 }] })
          const order2 = matchesFilter(doc, { $and: [{ [k2]: v2 }, { [k1]: v1 }] })
          return order1 === order2
        }),
        { numRuns: 500 }
      )
    })

    it('$and with empty array is true (vacuous truth)', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          return matchesFilter(doc, { $and: [] }) === true
        }),
        { numRuns: 100 }
      )
    })

    it('$and is associative', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc).slice(0, 3)
          if (keys.length < 3) return true
          const [k1, k2, k3] = keys as [string, string, string]
          const v1 = doc[k1]
          const v2 = doc[k2]
          const v3 = doc[k3]
          if ([v1, v2, v3].some(v => typeof v === 'number' && Number.isNaN(v))) return true

          // (A AND B) AND C
          const leftAssoc = matchesFilter(doc, {
            $and: [
              { $and: [{ [k1]: v1 }, { [k2]: v2 }] },
              { [k3]: v3 }
            ]
          })
          // A AND (B AND C)
          const rightAssoc = matchesFilter(doc, {
            $and: [
              { [k1]: v1 },
              { $and: [{ [k2]: v2 }, { [k3]: v3 }] }
            ]
          })
          return leftAssoc === rightAssoc
        }),
        { numRuns: 500 }
      )
    })

    it('$and with any false condition is false (short-circuit)', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc)
          if (keys.length === 0) return true
          const key = keys[0]!
          // Create a condition that definitely won't match
          const impossibleFilter: Filter = {
            $and: [
              { [key]: '__impossible_value_12345__' }
            ]
          }
          return matchesFilter(doc, impossibleFilter) === false
        }),
        { numRuns: 100 }
      )
    })
  })

  describe('$or properties', () => {
    it('$or with single condition equals that condition', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc)
          if (keys.length === 0) return true
          const key = keys[0]!
          const value = doc[key]
          if (typeof value === 'number' && Number.isNaN(value)) return true

          const singleResult = matchesFilter(doc, { [key]: value })
          const orResult = matchesFilter(doc, { $or: [{ [key]: value }] })
          return singleResult === orResult
        }),
        { numRuns: 500 }
      )
    })

    it('$or is commutative', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc).slice(0, 2)
          if (keys.length < 2) return true
          const [k1, k2] = keys as [string, string]

          const order1 = matchesFilter(doc, { $or: [{ [k1]: 'x' }, { [k2]: 'y' }] })
          const order2 = matchesFilter(doc, { $or: [{ [k2]: 'y' }, { [k1]: 'x' }] })
          return order1 === order2
        }),
        { numRuns: 500 }
      )
    })

    it('$or with empty array is false', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          return matchesFilter(doc, { $or: [] }) === false
        }),
        { numRuns: 100 }
      )
    })

    it('$or with any true condition is true', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc)
          if (keys.length === 0) return true
          const key = keys[0]!
          const value = doc[key]
          if (typeof value === 'number' && Number.isNaN(value)) return true

          // One matching, one not matching
          const result = matchesFilter(doc, {
            $or: [
              { [key]: value }, // matches
              { [key]: '__impossible__' } // doesn't match
            ]
          })
          return result === true
        }),
        { numRuns: 500 }
      )
    })

    it('$or is associative', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc).slice(0, 3)
          if (keys.length < 3) return true
          const [k1, k2, k3] = keys as [string, string, string]

          // (A OR B) OR C
          const leftAssoc = matchesFilter(doc, {
            $or: [
              { $or: [{ [k1]: 'a' }, { [k2]: 'b' }] },
              { [k3]: 'c' }
            ]
          })
          // A OR (B OR C)
          const rightAssoc = matchesFilter(doc, {
            $or: [
              { [k1]: 'a' },
              { $or: [{ [k2]: 'b' }, { [k3]: 'c' }] }
            ]
          })
          return leftAssoc === rightAssoc
        }),
        { numRuns: 500 }
      )
    })
  })

  describe('$not properties', () => {
    it('$not is involution: NOT(NOT(x)) === x', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc)
          if (keys.length === 0) return true
          const key = keys[0]!
          const value = doc[key]
          if (typeof value === 'number' && Number.isNaN(value)) return true

          const original = matchesFilter(doc, { [key]: value })
          const doubleNot = matchesFilter(doc, { $not: { $not: { [key]: value } } })
          return original === doubleNot
        }),
        { numRuns: 500 }
      )
    })

    it('$not empty filter is always false', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          // Empty filter matches everything, so $not {} should be false
          return matchesFilter(doc, { $not: {} }) === false
        }),
        { numRuns: 100 }
      )
    })

    it('$not inverts the match result', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc)
          if (keys.length === 0) return true
          const key = keys[0]!
          const value = doc[key]
          if (typeof value === 'number' && Number.isNaN(value)) return true

          const withoutNot = matchesFilter(doc, { [key]: value })
          const withNot = matchesFilter(doc, { $not: { [key]: value } })
          return withoutNot !== withNot
        }),
        { numRuns: 500 }
      )
    })
  })

  describe('De Morgan laws', () => {
    it('NOT(A AND B) === (NOT A) OR (NOT B)', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc).slice(0, 2)
          if (keys.length < 2) return true
          const [k1, k2] = keys as [string, string]
          const v1 = doc[k1]
          const v2 = doc[k2]
          if ([v1, v2].some(v => typeof v === 'number' && Number.isNaN(v))) return true

          // NOT(A AND B)
          const notAnd = matchesFilter(doc, {
            $not: { $and: [{ [k1]: v1 }, { [k2]: v2 }] }
          })
          // (NOT A) OR (NOT B)
          const orNots = matchesFilter(doc, {
            $or: [
              { $not: { [k1]: v1 } },
              { $not: { [k2]: v2 } }
            ]
          })
          return notAnd === orNots
        }),
        { numRuns: 500 }
      )
    })

    it('NOT(A OR B) === (NOT A) AND (NOT B)', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc).slice(0, 2)
          if (keys.length < 2) return true
          const [k1, k2] = keys as [string, string]
          const v1 = doc[k1]
          const v2 = doc[k2]
          if ([v1, v2].some(v => typeof v === 'number' && Number.isNaN(v))) return true

          // NOT(A OR B)
          const notOr = matchesFilter(doc, {
            $not: { $or: [{ [k1]: v1 }, { [k2]: v2 }] }
          })
          // (NOT A) AND (NOT B)
          const andNots = matchesFilter(doc, {
            $and: [
              { $not: { [k1]: v1 } },
              { $not: { [k2]: v2 } }
            ]
          })
          return notOr === andNots
        }),
        { numRuns: 500 }
      )
    })
  })

  describe('$nor properties', () => {
    it('$nor is equivalent to NOT($or)', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc).slice(0, 2)
          if (keys.length < 2) return true
          const [k1, k2] = keys as [string, string]

          const norResult = matchesFilter(doc, {
            $nor: [{ [k1]: 'x' }, { [k2]: 'y' }]
          })
          const notOrResult = matchesFilter(doc, {
            $not: { $or: [{ [k1]: 'x' }, { [k2]: 'y' }] }
          })
          return norResult === notOrResult
        }),
        { numRuns: 500 }
      )
    })

    it('$nor with empty array is true', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          return matchesFilter(doc, { $nor: [] }) === true
        }),
        { numRuns: 100 }
      )
    })
  })

  describe('distributive laws', () => {
    it('A AND (B OR C) === (A AND B) OR (A AND C)', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc).slice(0, 3)
          if (keys.length < 3) return true
          const [k1, k2, k3] = keys as [string, string, string]
          const v1 = doc[k1]
          if (typeof v1 === 'number' && Number.isNaN(v1)) return true

          // A AND (B OR C)
          const left = matchesFilter(doc, {
            $and: [
              { [k1]: v1 },
              { $or: [{ [k2]: 'b' }, { [k3]: 'c' }] }
            ]
          })
          // (A AND B) OR (A AND C)
          const right = matchesFilter(doc, {
            $or: [
              { $and: [{ [k1]: v1 }, { [k2]: 'b' }] },
              { $and: [{ [k1]: v1 }, { [k3]: 'c' }] }
            ]
          })
          return left === right
        }),
        { numRuns: 500 }
      )
    })
  })
})

// =============================================================================
// 3. Array Operators Properties
// =============================================================================

describe('Property: Array Operators', () => {
  describe('$in properties', () => {
    it('$in [x] is equivalent to $eq x', () => {
      fc.assert(
        fc.property(primitiveArb, (value) => {
          if (typeof value === 'number' && Number.isNaN(value)) return true
          const inResult = matchesCondition(value, { $in: [value] })
          const eqResult = matchesCondition(value, { $eq: value })
          return inResult === eqResult
        }),
        { numRuns: 500 }
      )
    })

    it('$in empty array is always false', () => {
      fc.assert(
        fc.property(primitiveArb, (value) => {
          return matchesCondition(value, { $in: [] }) === false
        }),
        { numRuns: 100 }
      )
    })

    it('$in is order-independent', () => {
      fc.assert(
        fc.property(
          fc.array(comparableNumberArb, { minLength: 1, maxLength: 5 }),
          comparableNumberArb,
          (arr, value) => {
            const original = matchesCondition(value, { $in: arr })
            const reversed = matchesCondition(value, { $in: [...arr].reverse() })
            return original === reversed
          }
        ),
        { numRuns: 500 }
      )
    })

    it('value in array implies $in matches', () => {
      fc.assert(
        fc.property(
          fc.array(comparableNumberArb, { minLength: 1, maxLength: 10 }),
          (arr) => {
            // Pick a random element from the array
            const value = arr[Math.floor(Math.random() * arr.length)]
            return matchesCondition(value, { $in: arr }) === true
          }
        ),
        { numRuns: 500 }
      )
    })
  })

  describe('$nin properties', () => {
    it('$nin is the negation of $in', () => {
      fc.assert(
        fc.property(
          fc.array(comparableNumberArb, { maxLength: 5 }),
          comparableNumberArb,
          (arr, value) => {
            const inResult = matchesCondition(value, { $in: arr })
            const ninResult = matchesCondition(value, { $nin: arr })
            return inResult !== ninResult
          }
        ),
        { numRuns: 500 }
      )
    })

    it('$nin empty array is always true', () => {
      fc.assert(
        fc.property(primitiveArb, (value) => {
          return matchesCondition(value, { $nin: [] }) === true
        }),
        { numRuns: 100 }
      )
    })
  })

  describe('$all properties', () => {
    it('$all empty array always matches arrays', () => {
      fc.assert(
        fc.property(
          fc.array(comparableNumberArb, { maxLength: 5 }),
          (arr) => {
            return matchesCondition(arr, { $all: [] }) === true
          }
        ),
        { numRuns: 100 }
      )
    })

    it('$all [x] matches arrays containing x', () => {
      fc.assert(
        fc.property(
          fc.array(comparableNumberArb, { minLength: 1, maxLength: 5 }),
          (arr) => {
            const elem = arr[0]
            return matchesCondition(arr, { $all: [elem] }) === true
          }
        ),
        { numRuns: 500 }
      )
    })

    it('$all is order-independent', () => {
      fc.assert(
        fc.property(
          fc.array(comparableNumberArb, { minLength: 2, maxLength: 5 }),
          (arr) => {
            const [a, b] = arr
            const result1 = matchesCondition(arr, { $all: [a, b] })
            const result2 = matchesCondition(arr, { $all: [b, a] })
            return result1 === result2
          }
        ),
        { numRuns: 500 }
      )
    })

    it('$all fails for non-arrays', () => {
      fc.assert(
        fc.property(comparableNumberArb, (value) => {
          return matchesCondition(value, { $all: [value] }) === false
        }),
        { numRuns: 100 }
      )
    })

    it('subset property: if $all A matches, then $all B matches for B subset of A', () => {
      fc.assert(
        fc.property(
          fc.array(comparableNumberArb, { minLength: 3, maxLength: 10 }),
          (arr) => {
            const uniqueArr = [...new Set(arr)]
            if (uniqueArr.length < 2) return true
            const allArr = matchesCondition(uniqueArr, { $all: uniqueArr })
            const subsetLen = Math.max(1, Math.floor(uniqueArr.length / 2))
            const subset = uniqueArr.slice(0, subsetLen)
            const allSubset = matchesCondition(uniqueArr, { $all: subset })
            // If all elements match, then subset must also match
            if (allArr) return allSubset === true
            return true
          }
        ),
        { numRuns: 500 }
      )
    })
  })

  describe('$elemMatch properties', () => {
    it('$elemMatch with simple equality filter', () => {
      fc.assert(
        fc.property(
          fc.array(
            fc.record({ value: comparableNumberArb }),
            { minLength: 1, maxLength: 5 }
          ),
          (arr) => {
            const firstElem = arr[0]!
            const result = matchesCondition(arr, {
              $elemMatch: { value: firstElem.value }
            })
            return result === true
          }
        ),
        { numRuns: 500 }
      )
    })

    it('$elemMatch fails for non-arrays', () => {
      fc.assert(
        fc.property(comparableNumberArb, (value) => {
          return matchesCondition(value, { $elemMatch: { x: 1 } }) === false
        }),
        { numRuns: 100 }
      )
    })

    it('$elemMatch with empty filter matches non-empty arrays', () => {
      fc.assert(
        fc.property(
          fc.array(fc.record({ x: comparableNumberArb }), { minLength: 1, maxLength: 5 }),
          (arr) => {
            return matchesCondition(arr, { $elemMatch: {} }) === true
          }
        ),
        { numRuns: 100 }
      )
    })
  })

  describe('$size properties', () => {
    it('$size matches exact array length', () => {
      fc.assert(
        fc.property(
          fc.array(comparableNumberArb, { minLength: 0, maxLength: 10 }),
          (arr) => {
            return matchesCondition(arr, { $size: arr.length }) === true
          }
        ),
        { numRuns: 500 }
      )
    })

    it('$size fails for wrong length', () => {
      fc.assert(
        fc.property(
          fc.array(comparableNumberArb, { minLength: 1, maxLength: 10 }),
          (arr) => {
            return matchesCondition(arr, { $size: arr.length + 1 }) === false
          }
        ),
        { numRuns: 500 }
      )
    })

    it('$size fails for non-arrays', () => {
      fc.assert(
        fc.property(comparableNumberArb, (value) => {
          return matchesCondition(value, { $size: 1 }) === false
        }),
        { numRuns: 100 }
      )
    })
  })
})

// =============================================================================
// 4. String Operators Properties
// =============================================================================

describe('Property: String Operators', () => {
  describe('$regex properties', () => {
    it('empty regex matches all strings', () => {
      fc.assert(
        fc.property(comparableStringArb, (str) => {
          return matchesCondition(str, { $regex: '' }) === true
        }),
        { numRuns: 500 }
      )
    })

    it('literal string regex matches itself', () => {
      fc.assert(
        fc.property(
          fc.string({ minLength: 1, maxLength: 10 }).filter(s =>
            // Filter out regex special characters to avoid escaping issues
            !/[.*+?^${}()|[\]\\]/.test(s)
          ),
          (str) => {
            return matchesCondition(str, { $regex: str }) === true
          }
        ),
        { numRuns: 500 }
      )
    })

    it('$regex fails for non-strings', () => {
      fc.assert(
        fc.property(comparableNumberArb, (value) => {
          return matchesCondition(value, { $regex: 'test' }) === false
        }),
        { numRuns: 100 }
      )
    })

    it('^pattern matches strings starting with pattern', () => {
      fc.assert(
        fc.property(
          fc.string({ minLength: 1, maxLength: 5 }).filter(s => /^[a-zA-Z]+$/.test(s)),
          fc.string({ maxLength: 10 }),
          (prefix, suffix) => {
            const fullString = prefix + suffix
            return matchesCondition(fullString, { $regex: `^${prefix}` }) === true
          }
        ),
        { numRuns: 500 }
      )
    })

    it('pattern$ matches strings ending with pattern', () => {
      fc.assert(
        fc.property(
          fc.string({ maxLength: 10 }),
          fc.string({ minLength: 1, maxLength: 5 }).filter(s => /^[a-zA-Z]+$/.test(s)),
          (prefix, suffix) => {
            const fullString = prefix + suffix
            return matchesCondition(fullString, { $regex: `${suffix}$` }) === true
          }
        ),
        { numRuns: 500 }
      )
    })

    it('case insensitive flag works', () => {
      fc.assert(
        fc.property(
          fc.string({ minLength: 1, maxLength: 10 }).filter(s => /^[a-zA-Z]+$/.test(s)),
          (str) => {
            const upper = str.toUpperCase()
            const lower = str.toLowerCase()
            // Without 'i' flag, case matters
            // With 'i' flag, should match
            return matchesCondition(upper, { $regex: lower, $options: 'i' }) === true
          }
        ),
        { numRuns: 500 }
      )
    })
  })

  describe('$startsWith properties', () => {
    it('string starts with itself', () => {
      fc.assert(
        fc.property(comparableStringArb, (str) => {
          return matchesCondition(str, { $startsWith: str }) === true
        }),
        { numRuns: 500 }
      )
    })

    it('string starts with empty string', () => {
      fc.assert(
        fc.property(comparableStringArb, (str) => {
          return matchesCondition(str, { $startsWith: '' }) === true
        }),
        { numRuns: 500 }
      )
    })

    it('$startsWith fails for non-strings', () => {
      fc.assert(
        fc.property(comparableNumberArb, (value) => {
          return matchesCondition(value, { $startsWith: 'test' }) === false
        }),
        { numRuns: 100 }
      )
    })
  })

  describe('$endsWith properties', () => {
    it('string ends with itself', () => {
      fc.assert(
        fc.property(comparableStringArb, (str) => {
          return matchesCondition(str, { $endsWith: str }) === true
        }),
        { numRuns: 500 }
      )
    })

    it('string ends with empty string', () => {
      fc.assert(
        fc.property(comparableStringArb, (str) => {
          return matchesCondition(str, { $endsWith: '' }) === true
        }),
        { numRuns: 500 }
      )
    })
  })

  describe('$contains properties', () => {
    it('string contains itself', () => {
      fc.assert(
        fc.property(comparableStringArb, (str) => {
          return matchesCondition(str, { $contains: str }) === true
        }),
        { numRuns: 500 }
      )
    })

    it('string contains empty string', () => {
      fc.assert(
        fc.property(comparableStringArb, (str) => {
          return matchesCondition(str, { $contains: '' }) === true
        }),
        { numRuns: 500 }
      )
    })

    it('if $startsWith x then $contains x', () => {
      fc.assert(
        fc.property(
          comparableStringArb,
          fc.string({ minLength: 0, maxLength: 5 }),
          (str, prefix) => {
            const startsWithResult = matchesCondition(str, { $startsWith: prefix })
            const containsResult = matchesCondition(str, { $contains: prefix })
            // startsWith implies contains
            if (startsWithResult) return containsResult === true
            return true
          }
        ),
        { numRuns: 500 }
      )
    })

    it('if $endsWith x then $contains x', () => {
      fc.assert(
        fc.property(
          comparableStringArb,
          fc.string({ minLength: 0, maxLength: 5 }),
          (str, suffix) => {
            const endsWithResult = matchesCondition(str, { $endsWith: suffix })
            const containsResult = matchesCondition(str, { $contains: suffix })
            // endsWith implies contains
            if (endsWithResult) return containsResult === true
            return true
          }
        ),
        { numRuns: 500 }
      )
    })
  })
})

// =============================================================================
// 5. Edge Cases Properties
// =============================================================================

describe('Property: Edge Cases', () => {
  describe('null/undefined handling', () => {
    it('null equals undefined for $eq (MongoDB behavior)', () => {
      expect(matchesCondition(null, { $eq: null })).toBe(true)
      expect(matchesCondition(undefined, { $eq: null })).toBe(true)
      expect(matchesCondition(null, { $eq: undefined })).toBe(true)
      expect(matchesCondition(undefined, { $eq: undefined })).toBe(true)
    })

    it('missing field matches null condition', () => {
      fc.assert(
        fc.property(
          safeFieldNameArb,
          (field) => {
            if (field === 'other') return true // skip if same as existing field
            const doc = { other: 'value' }
            return matchesFilter(doc, { [field]: null }) === true
          }
        ),
        { numRuns: 100 }
      )
    })

    it('null document always fails (with non-empty filters)', () => {
      fc.assert(
        fc.property(
          fc.dictionary(
            safeFieldNameArb,
            primitiveArb,
            { minKeys: 1, maxKeys: 5 }
          ),
          (filter) => {
            return matchesFilter(null, filter as Filter) === false
          }
        ),
        { numRuns: 100 }
      )
    })

    it('null document matches with empty filter (vacuous truth)', () => {
      // Empty filter matches everything, including null/undefined
      // This is consistent with the behavior that {} means "no conditions"
      expect(matchesFilter(null, {})).toBe(true)
    })

    it('undefined document always fails (with non-empty filters)', () => {
      fc.assert(
        fc.property(
          fc.dictionary(
            safeFieldNameArb,
            primitiveArb,
            { minKeys: 1, maxKeys: 5 }
          ),
          (filter) => {
            return matchesFilter(undefined, filter as Filter) === false
          }
        ),
        { numRuns: 100 }
      )
    })

    it('undefined document matches with empty filter (vacuous truth)', () => {
      // Empty filter matches everything, including null/undefined
      // This is consistent with the behavior that {} means "no conditions"
      expect(matchesFilter(undefined, {})).toBe(true)
    })
  })

  describe('empty filter/document handling', () => {
    it('empty filter matches any document', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          return matchesFilter(doc, {}) === true
        }),
        { numRuns: 500 }
      )
    })

    it('empty document only matches empty filters or null checks', () => {
      expect(matchesFilter({}, {})).toBe(true)
      expect(matchesFilter({}, { x: null })).toBe(true) // Missing = null
      expect(matchesFilter({}, { x: 'value' })).toBe(false)
    })
  })

  describe('nested object handling', () => {
    it('dot notation accesses nested fields', () => {
      fc.assert(
        fc.property(
          safeFieldNameArb.filter(s => s.length <= 5),
          safeFieldNameArb.filter(s => s.length <= 5),
          primitiveArb,
          (outerKey, innerKey, value) => {
            if (typeof value === 'number' && Number.isNaN(value)) return true
            const doc = { [outerKey]: { [innerKey]: value } }
            const result = matchesFilter(doc, { [`${outerKey}.${innerKey}`]: value })
            return result === true
          }
        ),
        { numRuns: 500 }
      )
    })

    it('missing nested path treated as undefined', () => {
      fc.assert(
        fc.property(
          safeFieldNameArb.filter(s => s.length <= 5),
          safeFieldNameArb.filter(s => s.length <= 5),
          (outerKey, innerKey) => {
            const doc = { [outerKey]: 'not-an-object' }
            // Missing nested path should be undefined, which equals null
            const result = matchesFilter(doc, { [`${outerKey}.${innerKey}`]: null })
            return result === true
          }
        ),
        { numRuns: 100 }
      )
    })
  })

  describe('empty array handling', () => {
    it('empty array equals empty array', () => {
      expect(matchesCondition([], { $eq: [] })).toBe(true)
    })

    it('$size 0 matches empty array', () => {
      expect(matchesCondition([], { $size: 0 })).toBe(true)
    })

    it('$all [] matches any array', () => {
      fc.assert(
        fc.property(
          fc.array(comparableNumberArb, { maxLength: 10 }),
          (arr) => {
            return matchesCondition(arr, { $all: [] }) === true
          }
        ),
        { numRuns: 100 }
      )
    })
  })

  describe('type coercion edge cases', () => {
    it('no implicit type coercion between string and number', () => {
      fc.assert(
        fc.property(comparableNumberArb, (num) => {
          const str = String(num)
          return matchesCondition(num, { $eq: str }) === false
        }),
        { numRuns: 500 }
      )
    })

    it('boolean 1/0 not coerced to true/false', () => {
      expect(matchesCondition(1, { $eq: true })).toBe(false)
      expect(matchesCondition(0, { $eq: false })).toBe(false)
      expect(matchesCondition(true, { $eq: 1 })).toBe(false)
      expect(matchesCondition(false, { $eq: 0 })).toBe(false)
    })
  })

  describe('$exists properties', () => {
    it('$exists true fails for undefined', () => {
      fc.assert(
        fc.property(
          safeFieldNameArb,
          (field) => {
            if (field === 'other') return true // skip if same as existing field
            const doc = { other: 'value' }
            return matchesFilter(doc, { [field]: { $exists: true } }) === false
          }
        ),
        { numRuns: 100 }
      )
    })

    it('$exists false succeeds for undefined', () => {
      fc.assert(
        fc.property(
          safeFieldNameArb,
          (field) => {
            if (field === 'other') return true // skip if same as existing field
            const doc = { other: 'value' }
            return matchesFilter(doc, { [field]: { $exists: false } }) === true
          }
        ),
        { numRuns: 100 }
      )
    })

    it('$exists true succeeds for null (field exists but is null)', () => {
      fc.assert(
        fc.property(
          safeFieldNameArb,
          (field) => {
            const doc = { [field]: null }
            return matchesFilter(doc, { [field]: { $exists: true } }) === true
          }
        ),
        { numRuns: 100 }
      )
    })

    it('$exists true and false are complements', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          const keys = Object.keys(doc)
          if (keys.length === 0) return true
          const key = keys[0]!
          const existsTrue = matchesFilter(doc, { [key]: { $exists: true } })
          const existsFalse = matchesFilter(doc, { [key]: { $exists: false } })
          return existsTrue !== existsFalse
        }),
        { numRuns: 500 }
      )
    })
  })

  describe('$type properties', () => {
    it('$type string matches strings', () => {
      fc.assert(
        fc.property(comparableStringArb, (str) => {
          return matchesCondition(str, { $type: 'string' }) === true
        }),
        { numRuns: 500 }
      )
    })

    it('$type number matches numbers', () => {
      fc.assert(
        fc.property(comparableNumberArb, (num) => {
          return matchesCondition(num, { $type: 'number' }) === true
        }),
        { numRuns: 500 }
      )
    })

    it('$type boolean matches booleans', () => {
      fc.assert(
        fc.property(fc.boolean(), (bool) => {
          return matchesCondition(bool, { $type: 'boolean' }) === true
        }),
        { numRuns: 100 }
      )
    })

    it('$type array matches arrays', () => {
      fc.assert(
        fc.property(
          fc.array(comparableNumberArb, { maxLength: 5 }),
          (arr) => {
            return matchesCondition(arr, { $type: 'array' }) === true
          }
        ),
        { numRuns: 500 }
      )
    })

    it('$type object matches plain objects', () => {
      fc.assert(
        fc.property(simpleDocArb, (doc) => {
          return matchesCondition(doc, { $type: 'object' }) === true
        }),
        { numRuns: 500 }
      )
    })

    it('$type null matches null and undefined', () => {
      expect(matchesCondition(null, { $type: 'null' })).toBe(true)
      expect(matchesCondition(undefined, { $type: 'null' })).toBe(true)
    })
  })
})

// =============================================================================
// 6. createPredicate Properties
// =============================================================================

describe('Property: createPredicate', () => {
  it('createPredicate produces same results as matchesFilter', () => {
    fc.assert(
      fc.property(simpleDocArb, (doc) => {
        const keys = Object.keys(doc)
        if (keys.length === 0) return true
        const key = keys[0]!
        const value = doc[key]
        if (typeof value === 'number' && Number.isNaN(value)) return true

        const filter: Filter = { [key]: value }
        const predicate = createPredicate(filter)
        const predicateResult = predicate(doc)
        const filterResult = matchesFilter(doc, filter)
        return predicateResult === filterResult
      }),
      { numRuns: 500 }
    )
  })

  it('predicate is reusable across multiple documents', () => {
    fc.assert(
      fc.property(
        fc.array(simpleDocArb, { minLength: 2, maxLength: 10 }),
        (docs) => {
          const filter: Filter = { status: 'active' }
          const predicate = createPredicate(filter)

          // All results should match direct matchesFilter calls
          return docs.every(doc =>
            predicate(doc) === matchesFilter(doc, filter)
          )
        }
      ),
      { numRuns: 100 }
    )
  })
})

// =============================================================================
// 7. Complex Combined Properties
// =============================================================================

describe('Property: Complex Combined Filters', () => {
  it('combining $and and $or respects boolean logic', () => {
    fc.assert(
      fc.property(
        fc.record({
          a: comparableNumberArb,
          b: comparableNumberArb,
          c: comparableNumberArb,
        }),
        (doc) => {
          // A AND (B OR C) with actual values
          const aMatches = doc.a > 0
          const bMatches = doc.b > 0
          const cMatches = doc.c > 0

          const filter: Filter = {
            $and: [
              { a: { $gt: 0 } },
              { $or: [{ b: { $gt: 0 } }, { c: { $gt: 0 } }] }
            ]
          }

          const expected = aMatches && (bMatches || cMatches)
          const actual = matchesFilter(doc, filter)
          return expected === actual
        }
      ),
      { numRuns: 500 }
    )
  })

  it('filter idempotence: applying same filter twice gives same result', () => {
    fc.assert(
      fc.property(simpleDocArb, (doc) => {
        const keys = Object.keys(doc)
        if (keys.length === 0) return true
        const key = keys[0]!
        const value = doc[key]
        if (typeof value === 'number' && Number.isNaN(value)) return true

        const filter: Filter = { [key]: value }
        const first = matchesFilter(doc, filter)
        const second = matchesFilter(doc, filter)
        return first === second
      }),
      { numRuns: 500 }
    )
  })

  it('nested $not with comparison operators', () => {
    fc.assert(
      fc.property(comparableNumberArb, comparableNumberArb, (value, target) => {
        // $not: { $gt: target } should equal $lte: target for non-null values
        const notGt = matchesCondition(value, { $not: { $gt: target } })
        const lte = matchesCondition(value, { $lte: target })
        return notGt === lte
      }),
      { numRuns: 500 }
    )
  })
})
