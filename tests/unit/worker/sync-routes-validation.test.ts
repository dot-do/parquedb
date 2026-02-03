/**
 * Tests for URL parameter validation in sync-routes.ts
 *
 * These tests verify that URL-derived parameters are properly validated
 * to prevent path traversal attacks and other security issues.
 */

import { describe, expect, it } from 'vitest'
import {
  validateUrlParameter,
  validateDatabaseId,
  validateFilePath,
  InvalidUrlParameterError,
} from '../../../src/worker/sync-routes'

describe('URL Parameter Validation', () => {
  describe('validateUrlParameter', () => {
    it('accepts valid simple values', () => {
      expect(() => validateUrlParameter('valid-value', 'param')).not.toThrow()
      expect(() => validateUrlParameter('test123', 'param')).not.toThrow()
      expect(() => validateUrlParameter('abc_def', 'param')).not.toThrow()
    })

    it('rejects empty values', () => {
      expect(() => validateUrlParameter('', 'param')).toThrow(InvalidUrlParameterError)
      expect(() => validateUrlParameter('   ', 'param')).toThrow(InvalidUrlParameterError)
    })

    it('rejects null bytes', () => {
      expect(() => validateUrlParameter('test\0value', 'param')).toThrow(InvalidUrlParameterError)
      expect(() => validateUrlParameter('test%00value', 'param')).toThrow(InvalidUrlParameterError)
    })

    it('rejects line breaks', () => {
      expect(() => validateUrlParameter('test\nvalue', 'param')).toThrow(InvalidUrlParameterError)
      expect(() => validateUrlParameter('test\rvalue', 'param')).toThrow(InvalidUrlParameterError)
      expect(() => validateUrlParameter('test%0avalue', 'param')).toThrow(InvalidUrlParameterError)
      expect(() => validateUrlParameter('test%0dvalue', 'param')).toThrow(InvalidUrlParameterError)
    })

    it('rejects path traversal sequences', () => {
      expect(() => validateUrlParameter('../etc/passwd', 'param')).toThrow(InvalidUrlParameterError)
      expect(() => validateUrlParameter('..\\windows\\system32', 'param')).toThrow(InvalidUrlParameterError)
      expect(() => validateUrlParameter('foo/../bar', 'param')).toThrow(InvalidUrlParameterError)
      expect(() => validateUrlParameter('foo/..\\bar', 'param')).toThrow(InvalidUrlParameterError)
    })

    it('rejects URL-encoded path traversal', () => {
      expect(() => validateUrlParameter('%2e%2e/etc/passwd', 'param')).toThrow(InvalidUrlParameterError)
      expect(() => validateUrlParameter('%2E%2E/etc/passwd', 'param')).toThrow(InvalidUrlParameterError)
      expect(() => validateUrlParameter('foo/%2e%2e/bar', 'param')).toThrow(InvalidUrlParameterError)
    })

    it('rejects double-encoded path traversal', () => {
      // %252e%252e is double-encoded ..
      expect(() => validateUrlParameter('%252e%252e/etc/passwd', 'param')).toThrow(InvalidUrlParameterError)
    })

    it('provides meaningful error messages', () => {
      try {
        validateUrlParameter('../etc/passwd', 'databaseId')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(InvalidUrlParameterError)
        expect((error as InvalidUrlParameterError).parameter).toBe('databaseId')
        expect((error as InvalidUrlParameterError).message).toContain('path traversal')
      }
    })
  })

  describe('validateDatabaseId', () => {
    it('accepts valid database IDs', () => {
      expect(() => validateDatabaseId('db-123')).not.toThrow()
      expect(() => validateDatabaseId('my_database')).not.toThrow()
      expect(() => validateDatabaseId('ABC123')).not.toThrow()
      expect(() => validateDatabaseId('test-db-456')).not.toThrow()
    })

    it('rejects database IDs with path traversal', () => {
      expect(() => validateDatabaseId('../other-db')).toThrow(InvalidUrlParameterError)
      expect(() => validateDatabaseId('..\\other-db')).toThrow(InvalidUrlParameterError)
    })

    it('rejects database IDs with slashes', () => {
      expect(() => validateDatabaseId('db/123')).toThrow(InvalidUrlParameterError)
      expect(() => validateDatabaseId('db\\123')).toThrow(InvalidUrlParameterError)
    })

    it('rejects database IDs with special characters', () => {
      expect(() => validateDatabaseId('db@123')).toThrow(InvalidUrlParameterError)
      expect(() => validateDatabaseId('db#123')).toThrow(InvalidUrlParameterError)
      expect(() => validateDatabaseId('db$123')).toThrow(InvalidUrlParameterError)
      expect(() => validateDatabaseId('db.123')).toThrow(InvalidUrlParameterError)
    })

    it('rejects database IDs with null bytes', () => {
      expect(() => validateDatabaseId('db\0123')).toThrow(InvalidUrlParameterError)
      expect(() => validateDatabaseId('db%00123')).toThrow(InvalidUrlParameterError)
    })

    it('rejects empty database IDs', () => {
      expect(() => validateDatabaseId('')).toThrow(InvalidUrlParameterError)
      expect(() => validateDatabaseId('   ')).toThrow(InvalidUrlParameterError)
    })
  })

  describe('validateFilePath', () => {
    it('accepts valid relative file paths', () => {
      expect(() => validateFilePath('data/file.parquet')).not.toThrow()
      expect(() => validateFilePath('events/2024/01/events.parquet')).not.toThrow()
      expect(() => validateFilePath('file.json')).not.toThrow()
      expect(() => validateFilePath('data_backup/test-file.parquet')).not.toThrow()
    })

    it('accepts paths with dots in filenames (not traversal)', () => {
      expect(() => validateFilePath('data/file.backup.parquet')).not.toThrow()
      expect(() => validateFilePath('.hidden/file.txt')).not.toThrow()
    })

    it('rejects absolute paths', () => {
      expect(() => validateFilePath('/etc/passwd')).toThrow(InvalidUrlParameterError)
      expect(() => validateFilePath('\\windows\\system32')).toThrow(InvalidUrlParameterError)
    })

    it('rejects path traversal sequences', () => {
      expect(() => validateFilePath('../secret/file.txt')).toThrow(InvalidUrlParameterError)
      expect(() => validateFilePath('data/../../../etc/passwd')).toThrow(InvalidUrlParameterError)
      expect(() => validateFilePath('data/..\\..\\windows')).toThrow(InvalidUrlParameterError)
    })

    it('rejects null bytes', () => {
      expect(() => validateFilePath('data/file\0.txt')).toThrow(InvalidUrlParameterError)
      expect(() => validateFilePath('data/file%00.txt')).toThrow(InvalidUrlParameterError)
    })

    it('rejects empty paths', () => {
      expect(() => validateFilePath('')).toThrow(InvalidUrlParameterError)
      expect(() => validateFilePath('   ')).toThrow(InvalidUrlParameterError)
    })

    it('rejects paths with URL-encoded traversal', () => {
      expect(() => validateFilePath('%2e%2e/secret')).toThrow(InvalidUrlParameterError)
      expect(() => validateFilePath('data/%2e%2e/secret')).toThrow(InvalidUrlParameterError)
    })

    it('handles already-decoded paths correctly', () => {
      // When paths come from decodeURIComponent in the caller
      expect(() => validateFilePath('data/test file.txt')).not.toThrow()
      expect(() => validateFilePath('data/test+file.txt')).not.toThrow()
    })
  })

  describe('InvalidUrlParameterError', () => {
    it('is an instance of Error', () => {
      const error = new InvalidUrlParameterError('test message', 'param', 'value')
      expect(error).toBeInstanceOf(Error)
      expect(error).toBeInstanceOf(InvalidUrlParameterError)
    })

    it('has the correct name', () => {
      const error = new InvalidUrlParameterError('test message', 'param', 'value')
      expect(error.name).toBe('InvalidUrlParameterError')
    })

    it('stores parameter and value', () => {
      const error = new InvalidUrlParameterError('test message', 'databaseId', '../malicious')
      expect(error.parameter).toBe('databaseId')
      expect(error.value).toBe('../malicious')
      expect(error.message).toBe('test message')
    })
  })

  describe('Security edge cases', () => {
    it('rejects various path traversal encodings', () => {
      const traversalAttempts = [
        '../',
        '..\\',
        '..',
        '%2e%2e/',
        '%2E%2E/',
        '%2e%2e%2f',
        '..%2f',
        '..%5c',
        '%2e%2e%5c',
        '....//....//etc/passwd',
        '..;/etc/passwd',
      ]

      for (const attempt of traversalAttempts) {
        expect(
          () => validateFilePath(attempt),
          `Should reject: ${attempt}`
        ).toThrow(InvalidUrlParameterError)
      }
    })

    it('rejects null byte injection attempts', () => {
      const nullByteAttempts = [
        'file\x00.txt',
        'file%00.txt',
        '%00file.txt',
      ]

      for (const attempt of nullByteAttempts) {
        expect(
          () => validateFilePath(attempt),
          `Should reject: ${attempt}`
        ).toThrow(InvalidUrlParameterError)
      }
    })

    it('rejects header injection attempts via newlines', () => {
      const headerInjectionAttempts = [
        'file\nHeader: injected',
        'file\rHeader: injected',
        'file%0aHeader: injected',
        'file%0dHeader: injected',
        'file\r\nHeader: injected',
        'file%0d%0aHeader: injected',
      ]

      for (const attempt of headerInjectionAttempts) {
        expect(
          () => validateFilePath(attempt),
          `Should reject: ${attempt}`
        ).toThrow(InvalidUrlParameterError)
      }
    })
  })
})
