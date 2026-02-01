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
  generateULID,
  generateId,
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

  describe('generateULID', () => {
    it('returns a 26-character string', () => {
      const ulid = generateULID()
      expect(ulid.length).toBe(26)
    })

    it('returns only Crockford Base32 characters (uppercase)', () => {
      const ulid = generateULID()
      // Crockford Base32 excludes I, L, O, U
      expect(ulid).toMatch(/^[0-9A-HJKMNP-TV-Z]+$/)
    })

    it('is lexicographically sortable by time', () => {
      // Generate IDs with a small delay to ensure different timestamps
      const ulid1 = generateULID()

      // Wait a bit to ensure different timestamp
      const start = Date.now()
      while (Date.now() === start) {
        // busy wait for next millisecond
      }

      const ulid2 = generateULID()

      // ulid2 should be lexicographically greater than ulid1
      expect(ulid2 > ulid1).toBe(true)
    })

    it('returns unique values on each call', () => {
      const ulid1 = generateULID()
      const ulid2 = generateULID()
      expect(ulid1).not.toBe(ulid2)
    })

    it('generates unique IDs under concurrent generation', () => {
      // Generate 10,000 IDs as fast as possible
      const ids = new Set<string>()
      const count = 10000

      for (let i = 0; i < count; i++) {
        ids.add(generateULID())
      }

      // All IDs should be unique
      expect(ids.size).toBe(count)
    })

    it('generates unique IDs in parallel (simulated concurrent access)', async () => {
      // Simulate concurrent generation using Promise.all
      const count = 1000
      const promises = Array.from({ length: count }, () =>
        Promise.resolve(generateULID())
      )

      const ids = await Promise.all(promises)
      const uniqueIds = new Set(ids)

      // All IDs should be unique
      expect(uniqueIds.size).toBe(count)
    })

    it('maintains timestamp prefix for time-based ordering', () => {
      const before = Date.now()
      const ulid = generateULID()
      const after = Date.now()

      // Extract timestamp portion (first 10 characters)
      const timestampPart = ulid.slice(0, 10)

      // The timestamp should decode to a value in range [before, after]
      // Decode Crockford Base32
      const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'
      let decoded = 0
      for (const char of timestampPart) {
        decoded = decoded * 32 + ENCODING.indexOf(char)
      }

      expect(decoded).toBeGreaterThanOrEqual(before)
      expect(decoded).toBeLessThanOrEqual(after)
    })
  })

  describe('generateId', () => {
    it('returns a 26-character lowercase string', () => {
      const id = generateId()
      expect(id.length).toBe(26)
      expect(id).toBe(id.toLowerCase())
    })

    it('returns only lowercase Crockford Base32 characters', () => {
      const id = generateId()
      // Lowercase Crockford Base32
      expect(id).toMatch(/^[0-9a-hjkmnp-tv-z]+$/)
    })

    it('is lexicographically sortable by time', () => {
      const id1 = generateId()

      // Wait a bit to ensure different timestamp
      const start = Date.now()
      while (Date.now() === start) {
        // busy wait for next millisecond
      }

      const id2 = generateId()

      // id2 should be lexicographically greater than id1
      expect(id2 > id1).toBe(true)
    })

    it('generates unique IDs under high-frequency concurrent generation', () => {
      // This is the critical test for the race condition fix
      // Generate IDs as fast as possible within the same millisecond
      const ids = new Set<string>()
      const count = 10000

      for (let i = 0; i < count; i++) {
        ids.add(generateId())
      }

      // All IDs should be unique - if there was a race condition,
      // we would see duplicates
      expect(ids.size).toBe(count)
    })

    it('is safe for multi-instance generation (no shared state)', () => {
      // The new implementation should have no shared mutable state
      // that could cause race conditions. Each call gets fresh randomness.
      // Generate many IDs in quick succession
      const batches = 10
      const perBatch = 1000
      const allIds = new Set<string>()

      for (let batch = 0; batch < batches; batch++) {
        for (let i = 0; i < perBatch; i++) {
          allIds.add(generateId())
        }
      }

      expect(allIds.size).toBe(batches * perBatch)
    })
  })
})
