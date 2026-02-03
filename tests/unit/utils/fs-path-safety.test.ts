/**
 * Filesystem Path Safety Tests
 *
 * Tests for path traversal protection in filesystem operations.
 * These tests verify that dangerous path patterns (.., null bytes, etc.)
 * are detected and rejected to prevent unauthorized file access.
 */

import { describe, it, expect } from 'vitest'
import { resolve, join } from 'node:path'
import {
  PathValidationError,
  hasDangerousCharacters,
  hasPathTraversal,
  escapesBaseDirectory,
  validateFilePath,
  validateFilePathWithAllowedDirs,
  sanitizeFilePath,
} from '@/utils/fs-path-safety'

describe('Filesystem Path Safety', () => {
  const baseDir = '/app/data'

  describe('hasDangerousCharacters', () => {
    it('returns false for safe paths', () => {
      expect(hasDangerousCharacters('file.txt')).toBe(false)
      expect(hasDangerousCharacters('path/to/file.json')).toBe(false)
      expect(hasDangerousCharacters('data.csv')).toBe(false)
      expect(hasDangerousCharacters('file-with-dashes.txt')).toBe(false)
      expect(hasDangerousCharacters('file_with_underscores.txt')).toBe(false)
    })

    it('detects null byte injection', () => {
      expect(hasDangerousCharacters('file.txt\0.jpg')).toBe(true)
      expect(hasDangerousCharacters('\0file.txt')).toBe(true)
      expect(hasDangerousCharacters('file\0')).toBe(true)
    })

    it('detects newline injection', () => {
      expect(hasDangerousCharacters('file\n.txt')).toBe(true)
      expect(hasDangerousCharacters('file\r.txt')).toBe(true)
      expect(hasDangerousCharacters('file\r\n.txt')).toBe(true)
    })
  })

  describe('hasPathTraversal', () => {
    it('returns false for safe relative paths', () => {
      expect(hasPathTraversal('file.txt')).toBe(false)
      expect(hasPathTraversal('subdir/file.txt')).toBe(false)
      expect(hasPathTraversal('a/b/c/file.txt')).toBe(false)
      expect(hasPathTraversal('./file.txt')).toBe(false)
      expect(hasPathTraversal('./subdir/file.txt')).toBe(false)
    })

    it('returns false for absolute paths without traversal', () => {
      expect(hasPathTraversal('/app/data/file.txt')).toBe(false)
      expect(hasPathTraversal('/var/log/app.log')).toBe(false)
    })

    it('detects parent directory traversal', () => {
      expect(hasPathTraversal('../file.txt')).toBe(true)
      expect(hasPathTraversal('../../etc/passwd')).toBe(true)
      expect(hasPathTraversal('subdir/../../../etc/passwd')).toBe(true)
    })

    it('detects traversal in middle of path', () => {
      expect(hasPathTraversal('data/../secret/file.txt')).toBe(true)
      expect(hasPathTraversal('a/b/../c/d/../../../file.txt')).toBe(true)
    })

    it('detects Windows-style path traversal', () => {
      expect(hasPathTraversal('..\\windows\\system32')).toBe(true)
      expect(hasPathTraversal('subdir\\..\\..\\file.txt')).toBe(true)
    })

    it('does not false positive on similar patterns', () => {
      // File names that contain dots but are not traversal
      expect(hasPathTraversal('file..txt')).toBe(false)
      expect(hasPathTraversal('...hidden')).toBe(false)
      expect(hasPathTraversal('file.name.with.dots.txt')).toBe(false)
    })
  })

  describe('escapesBaseDirectory', () => {
    it('returns false for paths within base directory', () => {
      expect(escapesBaseDirectory(baseDir, 'file.txt')).toBe(false)
      expect(escapesBaseDirectory(baseDir, 'subdir/file.txt')).toBe(false)
      expect(escapesBaseDirectory(baseDir, 'a/b/c/file.txt')).toBe(false)
    })

    it('returns true for absolute paths outside base', () => {
      expect(escapesBaseDirectory(baseDir, '/etc/passwd')).toBe(true)
      expect(escapesBaseDirectory(baseDir, '/var/log/app.log')).toBe(true)
      expect(escapesBaseDirectory(baseDir, '/tmp/file.txt')).toBe(true)
    })

    it('returns true for traversal that escapes base', () => {
      expect(escapesBaseDirectory(baseDir, '../secret.txt')).toBe(true)
      expect(escapesBaseDirectory(baseDir, '../../etc/passwd')).toBe(true)
      expect(escapesBaseDirectory(baseDir, 'subdir/../../../etc/passwd')).toBe(true)
    })

    it('returns false for traversal that stays within base', () => {
      // ./subdir/../file.txt resolves to ./file.txt which is still in base
      expect(escapesBaseDirectory(baseDir, 'subdir/../file.txt')).toBe(false)
      expect(escapesBaseDirectory(baseDir, 'a/b/../c/file.txt')).toBe(false)
    })
  })

  describe('validateFilePath', () => {
    it('accepts safe relative paths', () => {
      expect(() => validateFilePath(baseDir, 'file.txt')).not.toThrow()
      expect(() => validateFilePath(baseDir, 'subdir/file.txt')).not.toThrow()
    })

    it('throws PathValidationError for null bytes', () => {
      expect(() => validateFilePath(baseDir, 'file\0.txt'))
        .toThrow(PathValidationError)
      expect(() => validateFilePath(baseDir, 'file\0.txt'))
        .toThrow('dangerous characters')
    })

    it('throws PathValidationError for newlines', () => {
      expect(() => validateFilePath(baseDir, 'file\n.txt'))
        .toThrow(PathValidationError)
    })

    it('throws PathValidationError for path traversal', () => {
      expect(() => validateFilePath(baseDir, '../file.txt'))
        .toThrow(PathValidationError)
      expect(() => validateFilePath(baseDir, '../file.txt'))
        .toThrow('traversal sequence')
    })

    it('throws PathValidationError for escaping base directory', () => {
      expect(() => validateFilePath(baseDir, '/etc/passwd'))
        .toThrow(PathValidationError)
      expect(() => validateFilePath(baseDir, '/etc/passwd'))
        .toThrow('escapes the allowed directory')
    })

    it('includes the invalid path in the error', () => {
      try {
        validateFilePath(baseDir, '../secret.txt')
        expect.fail('Should have thrown')
      } catch (error) {
        expect(error).toBeInstanceOf(PathValidationError)
        expect((error as PathValidationError).path).toBe('../secret.txt')
      }
    })
  })

  describe('validateFilePathWithAllowedDirs', () => {
    const cwd = process.cwd()
    const tmpDir = '/tmp'
    const allowedDirs = [cwd, tmpDir]

    it('accepts relative paths within cwd', () => {
      expect(() => validateFilePathWithAllowedDirs(cwd, 'data.json', allowedDirs))
        .not.toThrow()
    })

    it('accepts absolute paths within allowed directories', () => {
      expect(() => validateFilePathWithAllowedDirs(cwd, join(tmpDir, 'data.json'), allowedDirs))
        .not.toThrow()
      expect(() => validateFilePathWithAllowedDirs(cwd, join(cwd, 'subdir', 'data.json'), allowedDirs))
        .not.toThrow()
    })

    it('rejects absolute paths outside allowed directories', () => {
      expect(() => validateFilePathWithAllowedDirs(cwd, '/etc/passwd', allowedDirs))
        .toThrow(PathValidationError)
    })

    it('still rejects dangerous characters', () => {
      expect(() => validateFilePathWithAllowedDirs(cwd, 'file\0.txt', allowedDirs))
        .toThrow(PathValidationError)
    })

    it('still rejects path traversal', () => {
      expect(() => validateFilePathWithAllowedDirs(cwd, '../file.txt', allowedDirs))
        .toThrow(PathValidationError)
    })
  })

  describe('sanitizeFilePath', () => {
    it('returns resolved absolute path for safe inputs', () => {
      const result = sanitizeFilePath(baseDir, 'file.txt')
      expect(result).toBe(resolve(baseDir, 'file.txt'))
    })

    it('resolves nested paths', () => {
      const result = sanitizeFilePath(baseDir, 'subdir/file.txt')
      expect(result).toBe(resolve(baseDir, 'subdir/file.txt'))
    })

    it('throws for unsafe paths', () => {
      expect(() => sanitizeFilePath(baseDir, '../file.txt'))
        .toThrow(PathValidationError)
    })
  })
})
