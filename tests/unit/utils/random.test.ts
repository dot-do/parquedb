/**
 * Tests for cryptographically secure random utilities
 */

import { describe, it, expect } from 'vitest'
import {
  getRandomBytes,
  getRandomInt,
  getSecureRandom,
  getRandomBase36,
  getRandomBase32,
  getRandom48Bit,
  getUUID,
} from '../../../src/utils/random'

describe('random utilities', () => {
  describe('getRandomBytes', () => {
    it('returns Uint8Array of correct length', () => {
      const bytes = getRandomBytes(16)
      expect(bytes).toBeInstanceOf(Uint8Array)
      expect(bytes.length).toBe(16)
    })

    it('returns different values on each call', () => {
      const bytes1 = getRandomBytes(16)
      const bytes2 = getRandomBytes(16)
      // Extremely unlikely to be equal if truly random
      expect(bytes1.toString()).not.toBe(bytes2.toString())
    })

    it('handles various lengths', () => {
      expect(getRandomBytes(0).length).toBe(0)
      expect(getRandomBytes(1).length).toBe(1)
      expect(getRandomBytes(32).length).toBe(32)
      expect(getRandomBytes(100).length).toBe(100)
    })
  })

  describe('getRandomInt', () => {
    it('returns values in range [0, max)', () => {
      for (let i = 0; i < 100; i++) {
        const value = getRandomInt(10)
        expect(value).toBeGreaterThanOrEqual(0)
        expect(value).toBeLessThan(10)
        expect(Number.isInteger(value)).toBe(true)
      }
    })

    it('returns 0 when max is 1', () => {
      for (let i = 0; i < 10; i++) {
        expect(getRandomInt(1)).toBe(0)
      }
    })

    it('throws for invalid max values', () => {
      expect(() => getRandomInt(0)).toThrow(RangeError)
      expect(() => getRandomInt(-1)).toThrow(RangeError)
      expect(() => getRandomInt(0x100000001)).toThrow(RangeError)
    })

    it('returns varied distribution', () => {
      const counts = new Array(10).fill(0)
      for (let i = 0; i < 1000; i++) {
        counts[getRandomInt(10)]++
      }
      // Each bucket should have roughly 100 values (allow for variance)
      for (const count of counts) {
        expect(count).toBeGreaterThan(50)
        expect(count).toBeLessThan(200)
      }
    })
  })

  describe('getSecureRandom', () => {
    it('returns values in range [0, 1)', () => {
      for (let i = 0; i < 100; i++) {
        const value = getSecureRandom()
        expect(value).toBeGreaterThanOrEqual(0)
        expect(value).toBeLessThan(1)
      }
    })

    it('returns different values on each call', () => {
      const values = new Set<number>()
      for (let i = 0; i < 100; i++) {
        values.add(getSecureRandom())
      }
      // Should have many unique values (extremely unlikely to have collisions)
      expect(values.size).toBeGreaterThan(90)
    })
  })

  describe('getRandomBase36', () => {
    it('returns string of correct length', () => {
      expect(getRandomBase36(5).length).toBe(5)
      expect(getRandomBase36(10).length).toBe(10)
      expect(getRandomBase36(0).length).toBe(0)
    })

    it('returns only alphanumeric characters', () => {
      const result = getRandomBase36(100)
      expect(result).toMatch(/^[0-9a-z]*$/)
    })

    it('returns different values on each call', () => {
      const val1 = getRandomBase36(16)
      const val2 = getRandomBase36(16)
      expect(val1).not.toBe(val2)
    })
  })

  describe('getRandomBase32', () => {
    it('returns string of correct length', () => {
      expect(getRandomBase32(5).length).toBe(5)
      expect(getRandomBase32(16).length).toBe(16)
    })

    it('returns only Crockford Base32 characters', () => {
      const result = getRandomBase32(100)
      // Crockford Base32 excludes I, L, O, U
      expect(result).toMatch(/^[0-9A-HJKMNP-TV-Z]*$/)
    })

    it('returns different values on each call', () => {
      const val1 = getRandomBase32(16)
      const val2 = getRandomBase32(16)
      expect(val1).not.toBe(val2)
    })
  })

  describe('getRandom48Bit', () => {
    it('returns a number in valid range', () => {
      for (let i = 0; i < 100; i++) {
        const value = getRandom48Bit()
        expect(value).toBeGreaterThanOrEqual(0)
        expect(value).toBeLessThan(0x1000000000000) // 2^48
        expect(Number.isInteger(value)).toBe(true)
      }
    })

    it('returns different values on each call', () => {
      const values = new Set<number>()
      for (let i = 0; i < 100; i++) {
        values.add(getRandom48Bit())
      }
      // Should have many unique values
      expect(values.size).toBeGreaterThan(90)
    })
  })

  describe('getUUID', () => {
    it('returns a valid UUID v4 format', () => {
      const uuid = getUUID()
      // UUID v4 format: 8-4-4-4-12 hex characters
      expect(uuid).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
      )
    })

    it('returns different values on each call', () => {
      const uuid1 = getUUID()
      const uuid2 = getUUID()
      expect(uuid1).not.toBe(uuid2)
    })

    it('has correct version and variant bits', () => {
      const uuid = getUUID()
      // Version 4 has '4' at position 14
      expect(uuid[14]).toBe('4')
      // Variant bits at position 19 should be 8, 9, a, or b
      expect(['8', '9', 'a', 'b']).toContain(uuid[19])
    })
  })
})
