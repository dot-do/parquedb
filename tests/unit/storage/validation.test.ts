/**
 * Tests for storage backend validation utilities
 *
 * These tests verify the shared validation functions work correctly
 * and provide consistent error messages across all backends.
 */

import { describe, it, expect } from 'vitest'
import {
  validateRange,
  validatePath,
  validatePartNumber,
  validateData,
  InvalidRangeError,
} from '../../../src/storage/validation'
import { InvalidPathError } from '../../../src/storage/errors'

describe('Storage Validation Utilities', () => {
  // ===========================================================================
  // validateRange
  // ===========================================================================

  describe('validateRange', () => {
    describe('valid ranges', () => {
      it('should accept start = 0, end = 0', () => {
        expect(() => validateRange(0, 0)).not.toThrow()
      })

      it('should accept start = 0, end = 10', () => {
        expect(() => validateRange(0, 10)).not.toThrow()
      })

      it('should accept start = 100, end = 200', () => {
        expect(() => validateRange(100, 200)).not.toThrow()
      })

      it('should accept start = end (zero-length range)', () => {
        expect(() => validateRange(50, 50)).not.toThrow()
      })

      it('should accept very large values', () => {
        expect(() => validateRange(0, Number.MAX_SAFE_INTEGER)).not.toThrow()
      })
    })

    describe('invalid ranges - negative start', () => {
      it('should throw InvalidRangeError for start = -1', () => {
        expect(() => validateRange(-1, 10)).toThrow(InvalidRangeError)
      })

      it('should throw InvalidRangeError for start = -100', () => {
        expect(() => validateRange(-100, 10)).toThrow(InvalidRangeError)
      })

      it('should include start value in error message', () => {
        try {
          validateRange(-5, 10)
          expect.fail('Should have thrown')
        } catch (error) {
          expect(error).toBeInstanceOf(InvalidRangeError)
          expect((error as InvalidRangeError).message).toContain('-5')
          expect((error as InvalidRangeError).message).toContain('non-negative')
        }
      })

      it('should set start and end properties on error', () => {
        try {
          validateRange(-5, 10)
          expect.fail('Should have thrown')
        } catch (error) {
          expect((error as InvalidRangeError).start).toBe(-5)
          expect((error as InvalidRangeError).end).toBe(10)
        }
      })
    })

    describe('invalid ranges - end < start', () => {
      it('should throw InvalidRangeError when end < start', () => {
        expect(() => validateRange(10, 5)).toThrow(InvalidRangeError)
      })

      it('should throw InvalidRangeError when end is much smaller than start', () => {
        expect(() => validateRange(100, 50)).toThrow(InvalidRangeError)
      })

      it('should include both values in error message', () => {
        try {
          validateRange(20, 10)
          expect.fail('Should have thrown')
        } catch (error) {
          expect(error).toBeInstanceOf(InvalidRangeError)
          expect((error as InvalidRangeError).message).toContain('20')
          expect((error as InvalidRangeError).message).toContain('10')
        }
      })

      it('should set start and end properties on error', () => {
        try {
          validateRange(20, 10)
          expect.fail('Should have thrown')
        } catch (error) {
          expect((error as InvalidRangeError).start).toBe(20)
          expect((error as InvalidRangeError).end).toBe(10)
        }
      })
    })
  })

  // ===========================================================================
  // validatePath
  // ===========================================================================

  describe('validatePath', () => {
    it('should accept non-empty strings', () => {
      expect(() => validatePath('test.txt', 'read')).not.toThrow()
    })

    it('should accept empty string (depends on backend behavior)', () => {
      // Empty string is valid for list operations
      expect(() => validatePath('', 'list')).not.toThrow()
    })

    it('should accept paths with slashes', () => {
      expect(() => validatePath('a/b/c.txt', 'read')).not.toThrow()
    })

    it('should throw InvalidPathError for null path', () => {
      expect(() => validatePath(null as unknown as string, 'read')).toThrow(InvalidPathError)
    })

    it('should throw InvalidPathError for undefined path', () => {
      expect(() => validatePath(undefined as unknown as string, 'read')).toThrow(InvalidPathError)
    })

    it('should include operation name in error message', () => {
      try {
        validatePath(null as unknown as string, 'read')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(InvalidPathError)
        expect((error as InvalidPathError).message).toContain('read')
      }
    })
  })

  // ===========================================================================
  // validatePartNumber
  // ===========================================================================

  describe('validatePartNumber', () => {
    it('should accept part number 1', () => {
      expect(() => validatePartNumber(1)).not.toThrow()
    })

    it('should accept part number 10000', () => {
      expect(() => validatePartNumber(10000)).not.toThrow()
    })

    it('should accept part number 5000 (middle)', () => {
      expect(() => validatePartNumber(5000)).not.toThrow()
    })

    it('should throw for part number 0', () => {
      expect(() => validatePartNumber(0)).toThrow()
    })

    it('should throw for negative part number', () => {
      expect(() => validatePartNumber(-1)).toThrow()
    })

    it('should throw for part number > 10000', () => {
      expect(() => validatePartNumber(10001)).toThrow()
    })

    it('should include part number in error message', () => {
      try {
        validatePartNumber(99999)
        expect.fail('Should have thrown')
      } catch (error) {
        expect((error as Error).message).toContain('99999')
      }
    })
  })

  // ===========================================================================
  // validateData
  // ===========================================================================

  describe('validateData', () => {
    it('should accept non-empty Uint8Array', () => {
      expect(() => validateData(new Uint8Array([1, 2, 3]), 'write')).not.toThrow()
    })

    it('should accept empty Uint8Array', () => {
      expect(() => validateData(new Uint8Array(0), 'write')).not.toThrow()
    })

    it('should throw for null data', () => {
      expect(() => validateData(null, 'write')).toThrow()
    })

    it('should throw for undefined data', () => {
      expect(() => validateData(undefined, 'write')).toThrow()
    })

    it('should include operation name in error message', () => {
      try {
        validateData(null, 'write')
        expect.fail('Should have thrown')
      } catch (error) {
        expect((error as Error).message).toContain('write')
      }
    })
  })

  // ===========================================================================
  // InvalidRangeError
  // ===========================================================================

  describe('InvalidRangeError', () => {
    it('should have name "InvalidRangeError"', () => {
      const error = new InvalidRangeError('test', 0, 10)
      expect(error.name).toBe('InvalidRangeError')
    })

    it('should be an instance of Error', () => {
      const error = new InvalidRangeError('test', 0, 10)
      expect(error).toBeInstanceOf(Error)
    })

    it('should store start and end values', () => {
      const error = new InvalidRangeError('test', 5, 15)
      expect(error.start).toBe(5)
      expect(error.end).toBe(15)
    })

    it('should preserve the error message', () => {
      const error = new InvalidRangeError('custom message', 0, 10)
      expect(error.message).toBe('custom message')
    })
  })
})
