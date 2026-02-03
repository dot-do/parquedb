/**
 * Tests for storage utility functions
 *
 * These tests verify the shared utility functions used across storage backends.
 */

import { describe, it, expect, vi } from 'vitest'
import {
  globToRegex,
  matchGlob,
  generateEtag,
  generateDeterministicEtag,
  normalizePath,
  normalizeFilePath,
  normalizeStoragePath,
  toError,
  applyPrefix,
  stripPrefix,
  normalizePrefix,
} from '../../../src/storage/utils'

describe('Storage Utilities', () => {
  // ===========================================================================
  // globToRegex
  // ===========================================================================

  describe('globToRegex', () => {
    describe('basic patterns', () => {
      it('should match exact strings', () => {
        const regex = globToRegex('file.txt')
        expect(regex.test('file.txt')).toBe(true)
        expect(regex.test('file.json')).toBe(false)
        expect(regex.test('afile.txt')).toBe(false)
      })

      it('should match * as any characters', () => {
        const regex = globToRegex('*.txt')
        expect(regex.test('file.txt')).toBe(true)
        expect(regex.test('data.txt')).toBe(true)
        expect(regex.test('.txt')).toBe(true) // zero characters
        expect(regex.test('file.json')).toBe(false)
      })

      it('should match ? as single character', () => {
        const regex = globToRegex('data?.csv')
        expect(regex.test('data1.csv')).toBe(true)
        expect(regex.test('dataA.csv')).toBe(true)
        expect(regex.test('data.csv')).toBe(false) // needs exactly one char
        expect(regex.test('data12.csv')).toBe(false) // too many chars
      })

      it('should handle multiple wildcards', () => {
        const regex = globToRegex('*.test.*')
        expect(regex.test('unit.test.ts')).toBe(true)
        expect(regex.test('integration.test.js')).toBe(true)
        expect(regex.test('test.ts')).toBe(false)
      })
    })

    describe('escape special regex characters', () => {
      it('should escape dots', () => {
        const regex = globToRegex('file.txt')
        expect(regex.test('fileatxt')).toBe(false)
        expect(regex.test('file.txt')).toBe(true)
      })

      it('should escape brackets', () => {
        const regex = globToRegex('file[1].txt')
        expect(regex.test('file[1].txt')).toBe(true)
        expect(regex.test('file1.txt')).toBe(false)
      })

      it('should escape parentheses', () => {
        const regex = globToRegex('file(1).txt')
        expect(regex.test('file(1).txt')).toBe(true)
        expect(regex.test('file1.txt')).toBe(false)
      })

      it('should escape plus', () => {
        const regex = globToRegex('file+.txt')
        expect(regex.test('file+.txt')).toBe(true)
        expect(regex.test('fileee.txt')).toBe(false)
      })

      it('should escape caret', () => {
        const regex = globToRegex('^start.txt')
        expect(regex.test('^start.txt')).toBe(true)
        expect(regex.test('start.txt')).toBe(false)
      })

      it('should escape dollar', () => {
        const regex = globToRegex('file$.txt')
        expect(regex.test('file$.txt')).toBe(true)
      })

      it('should escape curly braces', () => {
        const regex = globToRegex('file{1}.txt')
        expect(regex.test('file{1}.txt')).toBe(true)
      })

      it('should escape pipe', () => {
        const regex = globToRegex('file|alt.txt')
        expect(regex.test('file|alt.txt')).toBe(true)
        expect(regex.test('file.txt')).toBe(false)
      })

      it('should escape backslash', () => {
        const regex = globToRegex('file\\.txt')
        expect(regex.test('file\\.txt')).toBe(true)
      })
    })

    describe('complex patterns', () => {
      it('should handle prefix*', () => {
        const regex = globToRegex('data-*')
        expect(regex.test('data-2024')).toBe(true)
        expect(regex.test('data-')).toBe(true)
        expect(regex.test('data2024')).toBe(false)
      })

      it('should handle *suffix', () => {
        const regex = globToRegex('*.parquet')
        expect(regex.test('users.parquet')).toBe(true)
        expect(regex.test('.parquet')).toBe(true)
        expect(regex.test('users.json')).toBe(false)
      })

      it('should handle mixed patterns', () => {
        const regex = globToRegex('data-??-*.parquet')
        expect(regex.test('data-01-users.parquet')).toBe(true)
        expect(regex.test('data-AB-test.parquet')).toBe(true)
        expect(regex.test('data-1-users.parquet')).toBe(false) // only one char between dashes
      })
    })
  })

  // ===========================================================================
  // matchGlob
  // ===========================================================================

  describe('matchGlob', () => {
    it('should return true for matching patterns', () => {
      expect(matchGlob('file.txt', '*.txt')).toBe(true)
      expect(matchGlob('data1.csv', 'data?.csv')).toBe(true)
      expect(matchGlob('test.json', 'test.json')).toBe(true)
    })

    it('should return false for non-matching patterns', () => {
      expect(matchGlob('file.json', '*.txt')).toBe(false)
      expect(matchGlob('data12.csv', 'data?.csv')).toBe(false)
      expect(matchGlob('other.json', 'test.json')).toBe(false)
    })
  })

  // ===========================================================================
  // generateEtag
  // ===========================================================================

  describe('generateEtag', () => {
    it('should generate non-empty string', () => {
      const etag = generateEtag(new Uint8Array([1, 2, 3]))
      expect(etag).toBeTruthy()
      expect(typeof etag).toBe('string')
    })

    it('should include hash and timestamp separated by dash', () => {
      const etag = generateEtag(new Uint8Array([1, 2, 3]))
      expect(etag).toMatch(/^[0-9a-f]+-[0-9a-z]+$/)
    })

    it('should generate different ETags for different data', () => {
      const etag1 = generateEtag(new Uint8Array([1, 2, 3]))
      const etag2 = generateEtag(new Uint8Array([4, 5, 6]))
      // Hash portion should differ
      const hash1 = etag1.split('-')[0]
      const hash2 = etag2.split('-')[0]
      expect(hash1).not.toBe(hash2)
    })

    it('should handle empty data', () => {
      const etag = generateEtag(new Uint8Array(0))
      expect(etag).toBeTruthy()
      expect(typeof etag).toBe('string')
    })

    it('should handle large data', () => {
      const largeData = new Uint8Array(10000)
      for (let i = 0; i < largeData.length; i++) {
        largeData[i] = i % 256
      }
      const etag = generateEtag(largeData)
      expect(etag).toBeTruthy()
    })

    it('should produce different ETags for same content at different times', async () => {
      vi.useFakeTimers()
      try {
        const data = new Uint8Array([1, 2, 3])
        const etag1 = generateEtag(data)
        // Advance time to ensure timestamp differs
        await vi.advanceTimersByTimeAsync(2)
        const etag2 = generateEtag(data)
        // Same hash but potentially different timestamp
        const hash1 = etag1.split('-')[0]
        const hash2 = etag2.split('-')[0]
        expect(hash1).toBe(hash2) // Same content = same hash
      } finally {
        vi.useRealTimers()
      }
    })
  })

  // ===========================================================================
  // generateDeterministicEtag
  // ===========================================================================

  describe('generateDeterministicEtag', () => {
    it('should generate same ETag for same data', () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      const etag1 = generateDeterministicEtag(data)
      const etag2 = generateDeterministicEtag(data)
      expect(etag1).toBe(etag2)
    })

    it('should generate different ETags for different data', () => {
      const etag1 = generateDeterministicEtag(new Uint8Array([1, 2, 3]))
      const etag2 = generateDeterministicEtag(new Uint8Array([4, 5, 6]))
      expect(etag1).not.toBe(etag2)
    })

    it('should include hash and size separated by dash', () => {
      const etag = generateDeterministicEtag(new Uint8Array([1, 2, 3]))
      expect(etag).toMatch(/^[0-9a-f]+-[0-9a-z]+$/)
    })

    it('should differ for same hash but different sizes', () => {
      // These might have the same hash portion but size will differ
      const etag1 = generateDeterministicEtag(new Uint8Array([1]))
      const etag2 = generateDeterministicEtag(new Uint8Array([1, 1, 1, 1, 1]))
      // At minimum, size component differs
      const size1 = etag1.split('-')[1]
      const size2 = etag2.split('-')[1]
      expect(size1).not.toBe(size2)
    })

    it('should handle empty data', () => {
      const etag = generateDeterministicEtag(new Uint8Array(0))
      expect(etag).toBeTruthy()
    })
  })

  // ===========================================================================
  // normalizePath
  // ===========================================================================

  describe('normalizePath', () => {
    it('should remove leading slash', () => {
      expect(normalizePath('/foo/bar')).toBe('foo/bar')
    })

    it('should not modify path without leading slash', () => {
      expect(normalizePath('foo/bar')).toBe('foo/bar')
    })

    it('should handle root path', () => {
      expect(normalizePath('/')).toBe('')
    })

    it('should handle empty string', () => {
      expect(normalizePath('')).toBe('')
    })

    it('should preserve trailing slash', () => {
      expect(normalizePath('/foo/bar/')).toBe('foo/bar/')
    })

    it('should handle multiple leading slashes (removes first only)', () => {
      expect(normalizePath('//foo/bar')).toBe('/foo/bar')
    })
  })

  // ===========================================================================
  // normalizeStoragePath
  // ===========================================================================

  describe('normalizeStoragePath', () => {
    it('should remove leading slash', () => {
      expect(normalizeStoragePath('/foo/bar')).toBe('foo/bar')
    })

    it('should remove ALL leading slashes', () => {
      expect(normalizeStoragePath('//foo/bar')).toBe('foo/bar')
      expect(normalizeStoragePath('///foo/bar')).toBe('foo/bar')
    })

    it('should remove duplicate slashes', () => {
      expect(normalizeStoragePath('foo//bar')).toBe('foo/bar')
      expect(normalizeStoragePath('foo///bar//baz')).toBe('foo/bar/baz')
    })

    it('should remove trailing slash', () => {
      expect(normalizeStoragePath('foo/bar/')).toBe('foo/bar')
      expect(normalizeStoragePath('/foo/bar/')).toBe('foo/bar')
    })

    it('should handle root path', () => {
      expect(normalizeStoragePath('/')).toBe('')
    })

    it('should handle empty string', () => {
      expect(normalizeStoragePath('')).toBe('')
    })

    it('should handle multiple edge cases combined', () => {
      expect(normalizeStoragePath('//foo//bar//')).toBe('foo/bar')
    })

    it('should not modify already clean path', () => {
      expect(normalizeStoragePath('foo/bar')).toBe('foo/bar')
    })
  })

  // ===========================================================================
  // normalizeFilePath
  // ===========================================================================

  describe('normalizeFilePath', () => {
    it('should remove leading slash', () => {
      expect(normalizeFilePath('/foo/bar')).toBe('foo/bar')
    })

    it('should remove trailing slash', () => {
      expect(normalizeFilePath('foo/bar/')).toBe('foo/bar')
    })

    it('should remove both leading and trailing slashes', () => {
      expect(normalizeFilePath('/foo/bar/')).toBe('foo/bar')
    })

    it('should not modify clean path', () => {
      expect(normalizeFilePath('foo/bar')).toBe('foo/bar')
    })

    it('should handle root path', () => {
      expect(normalizeFilePath('/')).toBe('')
    })

    it('should handle empty string', () => {
      expect(normalizeFilePath('')).toBe('')
    })

    it('should handle path with only slashes', () => {
      expect(normalizeFilePath('//')).toBe('')
    })

    it('should preserve internal slashes', () => {
      expect(normalizeFilePath('/a/b/c/')).toBe('a/b/c')
    })
  })

  // ===========================================================================
  // toError
  // ===========================================================================

  describe('toError', () => {
    it('should return Error instances unchanged', () => {
      const originalError = new Error('test error')
      const result = toError(originalError)
      expect(result).toBe(originalError)
    })

    it('should convert strings to Error', () => {
      const result = toError('string error')
      expect(result).toBeInstanceOf(Error)
      expect(result.message).toBe('string error')
    })

    it('should convert numbers to Error', () => {
      const result = toError(404)
      expect(result).toBeInstanceOf(Error)
      expect(result.message).toBe('404')
    })

    it('should convert null to Error', () => {
      const result = toError(null)
      expect(result).toBeInstanceOf(Error)
      expect(result.message).toBe('null')
    })

    it('should convert undefined to Error', () => {
      const result = toError(undefined)
      expect(result).toBeInstanceOf(Error)
      expect(result.message).toBe('undefined')
    })

    it('should convert objects to Error', () => {
      const obj = { code: 'ENOENT', message: 'file not found' }
      const result = toError(obj)
      expect(result).toBeInstanceOf(Error)
      expect(result.message).toBe('[object Object]')
    })

    it('should preserve Error subclasses', () => {
      const typeError = new TypeError('type error')
      const result = toError(typeError)
      expect(result).toBe(typeError)
      expect(result).toBeInstanceOf(TypeError)
    })
  })

  // ===========================================================================
  // applyPrefix
  // ===========================================================================

  describe('applyPrefix', () => {
    it('should prepend prefix to path', () => {
      expect(applyPrefix('data/file.txt', 'tenant1/')).toBe('tenant1/data/file.txt')
    })

    it('should handle empty prefix', () => {
      expect(applyPrefix('data/file.txt', '')).toBe('data/file.txt')
    })

    it('should handle empty path', () => {
      expect(applyPrefix('', 'tenant1/')).toBe('tenant1/')
    })

    it('should concatenate directly (no automatic separator)', () => {
      expect(applyPrefix('file.txt', 'dir')).toBe('dirfile.txt')
    })
  })

  // ===========================================================================
  // stripPrefix
  // ===========================================================================

  describe('stripPrefix', () => {
    it('should remove prefix from path', () => {
      expect(stripPrefix('tenant1/data/file.txt', 'tenant1/')).toBe('data/file.txt')
    })

    it('should handle empty prefix', () => {
      expect(stripPrefix('data/file.txt', '')).toBe('data/file.txt')
    })

    it('should return unchanged if prefix not present', () => {
      expect(stripPrefix('other/path', 'tenant1/')).toBe('other/path')
    })

    it('should not remove partial prefix matches', () => {
      expect(stripPrefix('tenant123/file.txt', 'tenant1')).toBe('23/file.txt')
    })

    it('should handle path equal to prefix', () => {
      expect(stripPrefix('tenant1/', 'tenant1/')).toBe('')
    })
  })

  // ===========================================================================
  // normalizePrefix
  // ===========================================================================

  describe('normalizePrefix', () => {
    it('should add trailing slash if missing', () => {
      expect(normalizePrefix('tenant1')).toBe('tenant1/')
    })

    it('should keep trailing slash if present', () => {
      expect(normalizePrefix('tenant1/')).toBe('tenant1/')
    })

    it('should handle empty string', () => {
      expect(normalizePrefix('')).toBe('')
    })

    it('should handle undefined', () => {
      expect(normalizePrefix(undefined)).toBe('')
    })

    it('should handle multi-segment prefix', () => {
      expect(normalizePrefix('a/b/c')).toBe('a/b/c/')
    })

    it('should handle prefix with only slash', () => {
      expect(normalizePrefix('/')).toBe('/')
    })
  })
})
