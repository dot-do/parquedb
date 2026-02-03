/**
 * Tests for shared storage error classes
 *
 * Verifies:
 * - Error creation with all codes
 * - Error inheritance hierarchy
 * - Helper methods (isNotFound, isPreconditionFailed, isConflict)
 * - Type guards (isStorageError, isNotFoundError, etc.)
 * - Backward compatibility aliases
 */

import { describe, it, expect } from 'vitest'
import {
  StorageError,
  StorageErrorCode,
  NotFoundError,
  AlreadyExistsError,
  ETagMismatchError,
  PermissionDeniedError,
  NetworkError,
  InvalidPathError,
  QuotaExceededError,
  DirectoryNotEmptyError,
  DirectoryNotFoundError,
  PathTraversalError,
  OperationError,
  // Backward compatibility aliases
  FileNotFoundError,
  VersionMismatchError,
  FileExistsError,
  // Type guards
  isStorageError,
  isNotFoundError,
  isETagMismatchError,
  isAlreadyExistsError,
} from '../../../src/storage/errors'

describe('Storage Errors', () => {
  // ===========================================================================
  // StorageError (Base Class)
  // ===========================================================================

  describe('StorageError', () => {
    it('should create error with message and code', () => {
      const error = new StorageError('test error', StorageErrorCode.NOT_FOUND)

      expect(error.message).toBe('test error')
      expect(error.code).toBe(StorageErrorCode.NOT_FOUND)
      expect(error.name).toBe('StorageError')
    })

    it('should create error with path', () => {
      const error = new StorageError('test', StorageErrorCode.NOT_FOUND, 'test/path.txt')

      expect(error.path).toBe('test/path.txt')
    })

    it('should create error with cause', () => {
      const cause = new Error('underlying error')
      const error = new StorageError('test', StorageErrorCode.NOT_FOUND, undefined, cause)

      expect(error.cause).toBe(cause)
    })

    it('should be an instance of Error', () => {
      const error = new StorageError('test', StorageErrorCode.NOT_FOUND)

      expect(error).toBeInstanceOf(Error)
    })

    describe('helper methods', () => {
      it('isNotFound should return true for NOT_FOUND code', () => {
        const error = new StorageError('test', StorageErrorCode.NOT_FOUND)
        expect(error.isNotFound()).toBe(true)
      })

      it('isNotFound should return false for other codes', () => {
        const error = new StorageError('test', StorageErrorCode.ALREADY_EXISTS)
        expect(error.isNotFound()).toBe(false)
      })

      it('isPreconditionFailed should return true for ETAG_MISMATCH code', () => {
        const error = new StorageError('test', StorageErrorCode.ETAG_MISMATCH)
        expect(error.isPreconditionFailed()).toBe(true)
      })

      it('isPreconditionFailed should return false for other codes', () => {
        const error = new StorageError('test', StorageErrorCode.NOT_FOUND)
        expect(error.isPreconditionFailed()).toBe(false)
      })

      it('isConflict should return true for ALREADY_EXISTS code', () => {
        const error = new StorageError('test', StorageErrorCode.ALREADY_EXISTS)
        expect(error.isConflict()).toBe(true)
      })

      it('isConflict should return false for other codes', () => {
        const error = new StorageError('test', StorageErrorCode.NOT_FOUND)
        expect(error.isConflict()).toBe(false)
      })
    })
  })

  // ===========================================================================
  // NotFoundError
  // ===========================================================================

  describe('NotFoundError', () => {
    it('should create error with path', () => {
      const error = new NotFoundError('test/file.txt')

      expect(error.message).toBe('File not found: test/file.txt')
      expect(error.code).toBe(StorageErrorCode.NOT_FOUND)
      expect(error.path).toBe('test/file.txt')
      expect(error.name).toBe('NotFoundError')
    })

    it('should be an instance of StorageError', () => {
      const error = new NotFoundError('test.txt')

      expect(error).toBeInstanceOf(StorageError)
      expect(error).toBeInstanceOf(Error)
    })

    it('should accept a cause', () => {
      const cause = new Error('underlying')
      const error = new NotFoundError('test.txt', cause)

      expect(error.cause).toBe(cause)
    })

    it('isNotFound should return true', () => {
      const error = new NotFoundError('test.txt')
      expect(error.isNotFound()).toBe(true)
    })
  })

  // ===========================================================================
  // AlreadyExistsError
  // ===========================================================================

  describe('AlreadyExistsError', () => {
    it('should create error with path', () => {
      const error = new AlreadyExistsError('test/file.txt')

      expect(error.message).toBe('File already exists: test/file.txt')
      expect(error.code).toBe(StorageErrorCode.ALREADY_EXISTS)
      expect(error.path).toBe('test/file.txt')
      expect(error.name).toBe('AlreadyExistsError')
    })

    it('should be an instance of StorageError', () => {
      const error = new AlreadyExistsError('test.txt')

      expect(error).toBeInstanceOf(StorageError)
    })

    it('isConflict should return true', () => {
      const error = new AlreadyExistsError('test.txt')
      expect(error.isConflict()).toBe(true)
    })
  })

  // ===========================================================================
  // ETagMismatchError
  // ===========================================================================

  describe('ETagMismatchError', () => {
    it('should create error with etag details', () => {
      const error = new ETagMismatchError('test.txt', 'expected-etag', 'actual-etag')

      expect(error.message).toBe('ETag mismatch for test.txt: expected expected-etag, got actual-etag')
      expect(error.code).toBe(StorageErrorCode.ETAG_MISMATCH)
      expect(error.path).toBe('test.txt')
      expect(error.expectedEtag).toBe('expected-etag')
      expect(error.actualEtag).toBe('actual-etag')
      expect(error.name).toBe('ETagMismatchError')
    })

    it('should handle null expected etag', () => {
      const error = new ETagMismatchError('test.txt', null, 'actual-etag')

      expect(error.message).toBe('ETag mismatch for test.txt: expected null, got actual-etag')
      expect(error.expectedEtag).toBeNull()
    })

    it('should handle null actual etag', () => {
      const error = new ETagMismatchError('test.txt', 'expected-etag', null)

      expect(error.message).toBe('ETag mismatch for test.txt: expected expected-etag, got null')
      expect(error.actualEtag).toBeNull()
    })

    it('should be an instance of StorageError', () => {
      const error = new ETagMismatchError('test.txt', 'a', 'b')

      expect(error).toBeInstanceOf(StorageError)
    })

    it('isPreconditionFailed should return true', () => {
      const error = new ETagMismatchError('test.txt', 'a', 'b')
      expect(error.isPreconditionFailed()).toBe(true)
    })
  })

  // ===========================================================================
  // PermissionDeniedError
  // ===========================================================================

  describe('PermissionDeniedError', () => {
    it('should create error with path', () => {
      const error = new PermissionDeniedError('test.txt')

      expect(error.message).toBe('Permission denied: test.txt')
      expect(error.code).toBe(StorageErrorCode.PERMISSION_DENIED)
      expect(error.name).toBe('PermissionDeniedError')
    })

    it('should include operation in message', () => {
      const error = new PermissionDeniedError('test.txt', 'write')

      expect(error.message).toBe('Permission denied: test.txt (write)')
    })

    it('should be an instance of StorageError', () => {
      const error = new PermissionDeniedError('test.txt')

      expect(error).toBeInstanceOf(StorageError)
    })
  })

  // ===========================================================================
  // NetworkError
  // ===========================================================================

  describe('NetworkError', () => {
    it('should create error with message', () => {
      const error = new NetworkError('Connection timeout')

      expect(error.message).toBe('Connection timeout')
      expect(error.code).toBe(StorageErrorCode.NETWORK_ERROR)
      expect(error.name).toBe('NetworkError')
    })

    it('should accept path and cause', () => {
      const cause = new Error('socket error')
      const error = new NetworkError('Failed to connect', 'test.txt', cause)

      expect(error.path).toBe('test.txt')
      expect(error.cause).toBe(cause)
    })

    it('should be an instance of StorageError', () => {
      const error = new NetworkError('timeout')

      expect(error).toBeInstanceOf(StorageError)
    })
  })

  // ===========================================================================
  // InvalidPathError
  // ===========================================================================

  describe('InvalidPathError', () => {
    it('should create error with path', () => {
      const error = new InvalidPathError('bad/path')

      expect(error.message).toBe('Invalid path: bad/path')
      expect(error.code).toBe(StorageErrorCode.INVALID_PATH)
      expect(error.name).toBe('InvalidPathError')
    })

    it('should include reason in message', () => {
      const error = new InvalidPathError('', 'path cannot be empty')

      expect(error.message).toBe('Invalid path: : path cannot be empty')
    })

    it('should be an instance of StorageError', () => {
      const error = new InvalidPathError('')

      expect(error).toBeInstanceOf(StorageError)
    })
  })

  // ===========================================================================
  // QuotaExceededError
  // ===========================================================================

  describe('QuotaExceededError', () => {
    it('should create error with path', () => {
      const error = new QuotaExceededError('test.txt')

      expect(error.message).toBe('Quota exceeded for test.txt')
      expect(error.code).toBe(StorageErrorCode.QUOTA_EXCEEDED)
      expect(error.name).toBe('QuotaExceededError')
    })

    it('should include quota details', () => {
      const error = new QuotaExceededError('test.txt', 1000, 1500)

      expect(error.message).toBe('Quota exceeded for test.txt (used 1500 of 1000 bytes)')
      expect(error.quotaBytes).toBe(1000)
      expect(error.usedBytes).toBe(1500)
    })

    it('should be an instance of StorageError', () => {
      const error = new QuotaExceededError('test.txt')

      expect(error).toBeInstanceOf(StorageError)
    })
  })

  // ===========================================================================
  // DirectoryNotEmptyError
  // ===========================================================================

  describe('DirectoryNotEmptyError', () => {
    it('should create error with path', () => {
      const error = new DirectoryNotEmptyError('test/dir')

      expect(error.message).toBe('Directory not empty: test/dir')
      expect(error.code).toBe(StorageErrorCode.DIRECTORY_NOT_EMPTY)
      expect(error.name).toBe('DirectoryNotEmptyError')
    })

    it('should be an instance of StorageError', () => {
      const error = new DirectoryNotEmptyError('test')

      expect(error).toBeInstanceOf(StorageError)
    })
  })

  // ===========================================================================
  // DirectoryNotFoundError
  // ===========================================================================

  describe('DirectoryNotFoundError', () => {
    it('should create error with path', () => {
      const error = new DirectoryNotFoundError('test/dir')

      expect(error.message).toBe('Directory not found: test/dir')
      expect(error.code).toBe(StorageErrorCode.DIRECTORY_NOT_FOUND)
      expect(error.name).toBe('DirectoryNotFoundError')
    })

    it('should be an instance of StorageError', () => {
      const error = new DirectoryNotFoundError('test')

      expect(error).toBeInstanceOf(StorageError)
    })
  })

  // ===========================================================================
  // PathTraversalError
  // ===========================================================================

  describe('PathTraversalError', () => {
    it('should create error with path', () => {
      const error = new PathTraversalError('../etc/passwd')

      expect(error.message).toBe('Path traversal attempt detected: ../etc/passwd')
      expect(error.code).toBe(StorageErrorCode.INVALID_PATH)
      expect(error.name).toBe('PathTraversalError')
    })

    it('should be an instance of StorageError', () => {
      const error = new PathTraversalError('..')

      expect(error).toBeInstanceOf(StorageError)
    })
  })

  // ===========================================================================
  // OperationError
  // ===========================================================================

  describe('OperationError', () => {
    it('should create error with message and operation', () => {
      const error = new OperationError('Failed to read', 'read', 'test.txt')

      expect(error.message).toBe('Failed to read')
      expect(error.operation).toBe('read')
      expect(error.path).toBe('test.txt')
      expect(error.code).toBe(StorageErrorCode.OPERATION_ERROR)
      expect(error.name).toBe('OperationError')
    })

    it('should accept cause', () => {
      const cause = new Error('underlying')
      const error = new OperationError('Failed', 'write', undefined, cause)

      expect(error.cause).toBe(cause)
    })

    it('should be an instance of StorageError', () => {
      const error = new OperationError('test', 'read')

      expect(error).toBeInstanceOf(StorageError)
    })
  })

  // ===========================================================================
  // Backward Compatibility Aliases
  // ===========================================================================

  describe('Backward Compatibility Aliases', () => {
    it('FileNotFoundError should be NotFoundError', () => {
      expect(FileNotFoundError).toBe(NotFoundError)

      const error = new FileNotFoundError('test.txt')
      expect(error).toBeInstanceOf(NotFoundError)
      expect(error.name).toBe('NotFoundError')
    })

    it('VersionMismatchError should be ETagMismatchError', () => {
      expect(VersionMismatchError).toBe(ETagMismatchError)

      const error = new VersionMismatchError('test.txt', 'a', 'b')
      expect(error).toBeInstanceOf(ETagMismatchError)
      expect(error.name).toBe('ETagMismatchError')
    })

    it('FileExistsError should be AlreadyExistsError', () => {
      expect(FileExistsError).toBe(AlreadyExistsError)

      const error = new FileExistsError('test.txt')
      expect(error).toBeInstanceOf(AlreadyExistsError)
      expect(error.name).toBe('AlreadyExistsError')
    })
  })

  // ===========================================================================
  // Type Guards
  // ===========================================================================

  describe('Type Guards', () => {
    describe('isStorageError', () => {
      it('should return true for StorageError', () => {
        const error = new StorageError('test', StorageErrorCode.NOT_FOUND)
        expect(isStorageError(error)).toBe(true)
      })

      it('should return true for NotFoundError', () => {
        const error = new NotFoundError('test.txt')
        expect(isStorageError(error)).toBe(true)
      })

      it('should return false for regular Error', () => {
        const error = new Error('test')
        expect(isStorageError(error)).toBe(false)
      })

      it('should return false for non-error values', () => {
        expect(isStorageError(null)).toBe(false)
        expect(isStorageError(undefined)).toBe(false)
        expect(isStorageError('error string')).toBe(false)
        expect(isStorageError({ message: 'fake error' })).toBe(false)
      })
    })

    describe('isNotFoundError', () => {
      it('should return true for NotFoundError', () => {
        const error = new NotFoundError('test.txt')
        expect(isNotFoundError(error)).toBe(true)
      })

      it('should return true for StorageError with NOT_FOUND code', () => {
        const error = new StorageError('test', StorageErrorCode.NOT_FOUND)
        expect(isNotFoundError(error)).toBe(true)
      })

      it('should return false for other StorageErrors', () => {
        const error = new AlreadyExistsError('test.txt')
        expect(isNotFoundError(error)).toBe(false)
      })

      it('should return false for regular Error', () => {
        const error = new Error('not found')
        expect(isNotFoundError(error)).toBe(false)
      })
    })

    describe('isETagMismatchError', () => {
      it('should return true for ETagMismatchError', () => {
        const error = new ETagMismatchError('test.txt', 'a', 'b')
        expect(isETagMismatchError(error)).toBe(true)
      })

      it('should return true for StorageError with ETAG_MISMATCH code', () => {
        const error = new StorageError('test', StorageErrorCode.ETAG_MISMATCH)
        expect(isETagMismatchError(error)).toBe(true)
      })

      it('should return false for other StorageErrors', () => {
        const error = new NotFoundError('test.txt')
        expect(isETagMismatchError(error)).toBe(false)
      })
    })

    describe('isAlreadyExistsError', () => {
      it('should return true for AlreadyExistsError', () => {
        const error = new AlreadyExistsError('test.txt')
        expect(isAlreadyExistsError(error)).toBe(true)
      })

      it('should return true for StorageError with ALREADY_EXISTS code', () => {
        const error = new StorageError('test', StorageErrorCode.ALREADY_EXISTS)
        expect(isAlreadyExistsError(error)).toBe(true)
      })

      it('should return false for other StorageErrors', () => {
        const error = new NotFoundError('test.txt')
        expect(isAlreadyExistsError(error)).toBe(false)
      })
    })
  })

  // ===========================================================================
  // Error Inheritance Chain
  // ===========================================================================

  describe('Error Inheritance Chain', () => {
    const errorClasses = [
      NotFoundError,
      AlreadyExistsError,
      PermissionDeniedError,
      NetworkError,
      InvalidPathError,
      QuotaExceededError,
      DirectoryNotEmptyError,
      DirectoryNotFoundError,
      PathTraversalError,
      OperationError,
    ]

    for (const ErrorClass of errorClasses) {
      it(`${ErrorClass.name} should extend StorageError`, () => {
        // Create with minimal required args
        let error: StorageError
        if (ErrorClass === ETagMismatchError) {
          error = new ETagMismatchError('test', 'a', 'b')
        } else if (ErrorClass === OperationError) {
          error = new OperationError('test', 'op')
        } else if (ErrorClass === NetworkError) {
          error = new NetworkError('test')
        } else {
          error = new (ErrorClass as new (path: string) => StorageError)('test')
        }

        expect(error).toBeInstanceOf(StorageError)
        expect(error).toBeInstanceOf(Error)
      })
    }

    it('ETagMismatchError should extend StorageError', () => {
      const error = new ETagMismatchError('test', 'a', 'b')
      expect(error).toBeInstanceOf(StorageError)
      expect(error).toBeInstanceOf(Error)
    })
  })

  // ===========================================================================
  // StorageErrorCode Enum
  // ===========================================================================

  describe('StorageErrorCode', () => {
    it('should have all expected codes', () => {
      expect(StorageErrorCode.NOT_FOUND).toBe('NOT_FOUND')
      expect(StorageErrorCode.ALREADY_EXISTS).toBe('ALREADY_EXISTS')
      expect(StorageErrorCode.ETAG_MISMATCH).toBe('ETAG_MISMATCH')
      expect(StorageErrorCode.PERMISSION_DENIED).toBe('PERMISSION_DENIED')
      expect(StorageErrorCode.NETWORK_ERROR).toBe('NETWORK_ERROR')
      expect(StorageErrorCode.INVALID_PATH).toBe('INVALID_PATH')
      expect(StorageErrorCode.QUOTA_EXCEEDED).toBe('QUOTA_EXCEEDED')
      expect(StorageErrorCode.DIRECTORY_NOT_EMPTY).toBe('DIRECTORY_NOT_EMPTY')
      expect(StorageErrorCode.DIRECTORY_NOT_FOUND).toBe('DIRECTORY_NOT_FOUND')
      expect(StorageErrorCode.OPERATION_ERROR).toBe('OPERATION_ERROR')
    })
  })
})
