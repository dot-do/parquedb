/**
 * Tests for the standardized ParqueDB error handling module
 *
 * Verifies:
 * - Base ParqueDBError functionality
 * - All error subclasses
 * - Error codes
 * - Serialization/deserialization
 * - Type guards
 * - Factory functions
 */

import { describe, it, expect } from 'vitest'
import {
  // Base class
  ParqueDBError,
  ErrorCode,
  // Validation errors
  ValidationError,
  // Not found errors
  NotFoundError,
  EntityNotFoundError,
  IndexNotFoundError,
  EventNotFoundError,
  SnapshotNotFoundError,
  FileNotFoundError,
  // Conflict errors
  ConflictError,
  VersionConflictError,
  AlreadyExistsError,
  ETagMismatchError,
  UniqueConstraintError,
  // Relationship errors
  RelationshipError,
  // Query errors
  QueryError,
  InvalidFilterError,
  // Storage errors
  StorageError,
  QuotaExceededError,
  InvalidPathError,
  PathTraversalError,
  NetworkError,
  // Authorization errors
  AuthorizationError,
  PermissionDeniedError,
  // Configuration errors
  ConfigurationError,
  // Timeout error
  TimeoutError,
  // RPC errors
  RpcError,
  // Index errors
  IndexError,
  IndexBuildError,
  IndexLoadError,
  IndexAlreadyExistsError,
  // Event errors
  EventError,
  // Type guards
  isParqueDBError,
  isValidationError,
  isNotFoundError,
  isEntityNotFoundError,
  isConflictError,
  isVersionConflictError,
  isETagMismatchError,
  isAlreadyExistsError,
  isStorageError,
  isRelationshipError,
  isQueryError,
  isAuthorizationError,
  isRpcError,
  isIndexError,
  isEventError,
  // Factory functions
  wrapError,
  errorFromStatus,
  assertValid,
  assertFound,
  // Types
  type SerializedError,
} from '../../../src/errors'

describe('ParqueDB Error Handling', () => {
  // ===========================================================================
  // ParqueDBError (Base Class)
  // ===========================================================================

  describe('ParqueDBError', () => {
    it('should create error with message and code', () => {
      const error = new ParqueDBError('test error', ErrorCode.INTERNAL)

      expect(error.message).toBe('test error')
      expect(error.code).toBe(ErrorCode.INTERNAL)
      expect(error.name).toBe('ParqueDBError')
      expect(error.context).toEqual({})
    })

    it('should create error with context', () => {
      const error = new ParqueDBError('test', ErrorCode.INTERNAL, {
        operation: 'create',
        namespace: 'users',
      })

      expect(error.context).toEqual({
        operation: 'create',
        namespace: 'users',
      })
    })

    it('should create error with cause', () => {
      const cause = new Error('underlying error')
      const error = new ParqueDBError('test', ErrorCode.INTERNAL, undefined, cause)

      expect(error.cause).toBe(cause)
    })

    it('should be an instance of Error', () => {
      const error = new ParqueDBError('test', ErrorCode.INTERNAL)

      expect(error).toBeInstanceOf(Error)
    })

    describe('toJSON', () => {
      it('should serialize error to JSON', () => {
        const error = new ParqueDBError('test error', ErrorCode.INTERNAL, {
          key: 'value',
        })

        const json = error.toJSON()

        expect(json.name).toBe('ParqueDBError')
        expect(json.code).toBe(ErrorCode.INTERNAL)
        expect(json.message).toBe('test error')
        expect(json.context).toEqual({ key: 'value' })
      })

      it('should serialize nested cause', () => {
        const cause = new ParqueDBError('cause error', ErrorCode.TIMEOUT)
        const error = new ParqueDBError('outer error', ErrorCode.INTERNAL, undefined, cause)

        const json = error.toJSON()

        expect(json.cause).toBeDefined()
        expect(json.cause?.code).toBe(ErrorCode.TIMEOUT)
        expect(json.cause?.message).toBe('cause error')
      })

      it('should omit empty context', () => {
        const error = new ParqueDBError('test', ErrorCode.INTERNAL)
        const json = error.toJSON()

        expect(json.context).toBeUndefined()
      })
    })

    describe('fromJSON', () => {
      it('should deserialize error from JSON', () => {
        const json: SerializedError = {
          name: 'ParqueDBError',
          code: ErrorCode.INTERNAL,
          message: 'test error',
          context: { key: 'value' },
        }

        const error = ParqueDBError.fromJSON(json)

        expect(error.message).toBe('test error')
        expect(error.code).toBe(ErrorCode.INTERNAL)
        expect(error.context).toEqual({ key: 'value' })
      })

      it('should deserialize nested cause', () => {
        const json: SerializedError = {
          name: 'ParqueDBError',
          code: ErrorCode.INTERNAL,
          message: 'outer',
          cause: {
            name: 'ParqueDBError',
            code: ErrorCode.TIMEOUT,
            message: 'inner',
          },
        }

        const error = ParqueDBError.fromJSON(json)

        expect(error.cause).toBeInstanceOf(ParqueDBError)
        expect((error.cause as ParqueDBError).code).toBe(ErrorCode.TIMEOUT)
      })
    })

    describe('helper methods', () => {
      it('is() should check error code', () => {
        const error = new ParqueDBError('test', ErrorCode.NOT_FOUND)

        expect(error.is(ErrorCode.NOT_FOUND)).toBe(true)
        expect(error.is(ErrorCode.INTERNAL)).toBe(false)
      })

      it('isCategory() should check code category', () => {
        const error = new ParqueDBError('test', ErrorCode.ENTITY_NOT_FOUND)

        expect(error.isCategory('NOT_FOUND')).toBe(true)
        expect(error.isCategory('CONFLICT')).toBe(false)
      })
    })
  })

  // ===========================================================================
  // Validation Errors
  // ===========================================================================

  describe('ValidationError', () => {
    it('should create error with message', () => {
      const error = new ValidationError('Invalid input')

      expect(error.message).toBe('Invalid input')
      expect(error.code).toBe(ErrorCode.VALIDATION_FAILED)
      expect(error.name).toBe('ValidationError')
    })

    it('should include field context', () => {
      const error = new ValidationError('Field is required', {
        field: 'email',
        namespace: 'users',
      })

      expect(error.code).toBe(ErrorCode.REQUIRED_FIELD)
      expect(error.field).toBe('email')
      expect(error.namespace).toBe('users')
    })

    it('should include type mismatch context', () => {
      const error = new ValidationError('Type mismatch', {
        field: 'age',
        expectedType: 'number',
        actualType: 'string',
      })

      expect(error.code).toBe(ErrorCode.INVALID_TYPE)
      expect(error.expectedType).toBe('number')
      expect(error.actualType).toBe('string')
    })

    it('should be instance of ParqueDBError', () => {
      const error = new ValidationError('test')
      expect(error).toBeInstanceOf(ParqueDBError)
    })
  })

  // ===========================================================================
  // Not Found Errors
  // ===========================================================================

  describe('NotFoundError', () => {
    it('should create error with message', () => {
      const error = new NotFoundError('Resource not found')

      expect(error.message).toBe('Resource not found')
      expect(error.code).toBe(ErrorCode.NOT_FOUND)
      expect(error.name).toBe('NotFoundError')
    })

    it('should be instance of ParqueDBError', () => {
      const error = new NotFoundError('test')
      expect(error).toBeInstanceOf(ParqueDBError)
    })
  })

  describe('EntityNotFoundError', () => {
    it('should create error with namespace and entityId', () => {
      const error = new EntityNotFoundError('users', 'abc123')

      expect(error.message).toBe('Entity not found: users/abc123')
      expect(error.code).toBe(ErrorCode.ENTITY_NOT_FOUND)
      expect(error.namespace).toBe('users')
      expect(error.entityId).toBe('abc123')
      expect(error.name).toBe('EntityNotFoundError')
    })

    it('should be instance of NotFoundError', () => {
      const error = new EntityNotFoundError('users', 'abc')
      expect(error).toBeInstanceOf(NotFoundError)
    })
  })

  describe('IndexNotFoundError', () => {
    it('should create error with index name and namespace', () => {
      const error = new IndexNotFoundError('email_idx', 'users')

      expect(error.message).toBe('Index "email_idx" not found in namespace "users"')
      expect(error.code).toBe(ErrorCode.INDEX_NOT_FOUND)
      expect(error.indexName).toBe('email_idx')
      expect(error.namespace).toBe('users')
    })
  })

  describe('EventNotFoundError', () => {
    it('should create error with event ID', () => {
      const error = new EventNotFoundError('evt_123')

      expect(error.message).toBe('Event not found: evt_123')
      expect(error.eventId).toBe('evt_123')
    })
  })

  describe('SnapshotNotFoundError', () => {
    it('should create error with snapshot ID', () => {
      const error = new SnapshotNotFoundError('snap_123')

      expect(error.message).toBe('Snapshot not found: snap_123')
      expect(error.snapshotId).toBe('snap_123')
    })
  })

  describe('FileNotFoundError', () => {
    it('should create error with path', () => {
      const error = new FileNotFoundError('data/users.parquet')

      expect(error.message).toBe('File not found: data/users.parquet')
      expect(error.path).toBe('data/users.parquet')
    })
  })

  // ===========================================================================
  // Conflict Errors
  // ===========================================================================

  describe('ConflictError', () => {
    it('should create error with message', () => {
      const error = new ConflictError('Conflict occurred')

      expect(error.message).toBe('Conflict occurred')
      expect(error.code).toBe(ErrorCode.CONFLICT)
      expect(error.name).toBe('ConflictError')
    })
  })

  describe('VersionConflictError', () => {
    it('should create error with version details', () => {
      const error = new VersionConflictError(5, 3)

      expect(error.message).toBe('Version conflict: expected 5, got 3')
      expect(error.code).toBe(ErrorCode.VERSION_CONFLICT)
      expect(error.expectedVersion).toBe(5)
      expect(error.actualVersion).toBe(3)
    })

    it('should include namespace and entityId in message', () => {
      const error = new VersionConflictError(5, 3, {
        namespace: 'posts',
        entityId: 'abc123',
      })

      expect(error.message).toBe('Version conflict: expected 5, got 3 for posts/abc123')
      expect(error.namespace).toBe('posts')
      expect(error.entityId).toBe('abc123')
    })

    it('should handle undefined actual version', () => {
      const error = new VersionConflictError(5, undefined, {
        namespace: 'posts',
        entityId: 'nonexistent',
      })

      expect(error.message).toBe('Version conflict: expected 5, got undefined for posts/nonexistent')
      expect(error.actualVersion).toBeUndefined()
    })
  })

  describe('AlreadyExistsError', () => {
    it('should create error with resource info', () => {
      const error = new AlreadyExistsError('User', 'john@example.com')

      expect(error.message).toBe('User already exists: john@example.com')
      expect(error.code).toBe(ErrorCode.ALREADY_EXISTS)
      expect(error.resource).toBe('User')
      expect(error.identifier).toBe('john@example.com')
    })
  })

  describe('ETagMismatchError', () => {
    it('should create error with etag details', () => {
      const error = new ETagMismatchError('file.txt', 'abc', 'xyz')

      expect(error.message).toBe('ETag mismatch for file.txt: expected abc, got xyz')
      expect(error.code).toBe(ErrorCode.ETAG_MISMATCH)
      expect(error.path).toBe('file.txt')
      expect(error.expectedEtag).toBe('abc')
      expect(error.actualEtag).toBe('xyz')
    })

    it('should handle null etags', () => {
      const error = new ETagMismatchError('file.txt', null, 'xyz')

      expect(error.message).toBe('ETag mismatch for file.txt: expected null, got xyz')
      expect(error.expectedEtag).toBeNull()
    })
  })

  describe('UniqueConstraintError', () => {
    it('should create error with index and value', () => {
      const error = new UniqueConstraintError('email_unique', 'test@example.com', 'users')

      expect(error.message).toBe('Unique constraint violation on index "email_unique": duplicate value "test@example.com"')
      expect(error.code).toBe(ErrorCode.UNIQUE_CONSTRAINT)
      expect(error.indexName).toBe('email_unique')
      expect(error.value).toBe('test@example.com')
      expect(error.namespace).toBe('users')
    })

    it('should serialize object values', () => {
      const error = new UniqueConstraintError('compound_idx', { a: 1, b: 2 })

      expect(error.message).toContain('{"a":1,"b":2}')
    })
  })

  // ===========================================================================
  // Relationship Errors
  // ===========================================================================

  describe('RelationshipError', () => {
    it('should create error with operation and namespace', () => {
      const error = new RelationshipError('Link', 'Post', 'Target not found')

      expect(error.message).toBe("Link failed for 'Post': Target not found")
      expect(error.code).toBe(ErrorCode.RELATIONSHIP_ERROR)
      expect(error.operation).toBe('Link')
      expect(error.namespace).toBe('Post')
    })

    it('should include full context', () => {
      const error = new RelationshipError('Link', 'Post', 'not defined', {
        entityId: 'abc123',
        relationshipName: 'author',
        targetId: 'users/xyz',
      })

      expect(error.message).toBe("Link failed for Post/abc123 relationship 'author' with target users/xyz: not defined")
      expect(error.entityId).toBe('abc123')
      expect(error.relationshipName).toBe('author')
      expect(error.targetId).toBe('users/xyz')
    })
  })

  // ===========================================================================
  // Query Errors
  // ===========================================================================

  describe('QueryError', () => {
    it('should create error with message', () => {
      const error = new QueryError('Query failed')

      expect(error.message).toBe('Query failed')
      expect(error.code).toBe(ErrorCode.QUERY_ERROR)
      expect(error.name).toBe('QueryError')
    })
  })

  describe('InvalidFilterError', () => {
    it('should create error with filter', () => {
      const filter = { $invalid: true }
      const error = new InvalidFilterError('Unknown operator', filter)

      expect(error.message).toBe('Unknown operator')
      expect(error.code).toBe(ErrorCode.INVALID_FILTER)
      expect(error.filter).toEqual(filter)
    })
  })

  // ===========================================================================
  // Storage Errors
  // ===========================================================================

  describe('StorageError', () => {
    it('should create error with path', () => {
      const error = new StorageError('Read failed', ErrorCode.STORAGE_READ_ERROR, {
        path: 'data/file.txt',
      })

      expect(error.message).toBe('Read failed')
      expect(error.code).toBe(ErrorCode.STORAGE_READ_ERROR)
      expect(error.path).toBe('data/file.txt')
    })
  })

  describe('QuotaExceededError', () => {
    it('should create error with quota details', () => {
      const error = new QuotaExceededError('data/large.txt', 1000, 1500)

      expect(error.message).toBe('Quota exceeded for data/large.txt (used 1500 of 1000 bytes)')
      expect(error.code).toBe(ErrorCode.QUOTA_EXCEEDED)
      expect(error.quotaBytes).toBe(1000)
      expect(error.usedBytes).toBe(1500)
    })
  })

  describe('InvalidPathError', () => {
    it('should create error with path and reason', () => {
      const error = new InvalidPathError('', 'path cannot be empty')

      expect(error.message).toBe('Invalid path: : path cannot be empty')
      expect(error.code).toBe(ErrorCode.INVALID_PATH)
      expect(error.reason).toBe('path cannot be empty')
    })
  })

  describe('PathTraversalError', () => {
    it('should create error with path', () => {
      const error = new PathTraversalError('../etc/passwd')

      expect(error.message).toBe('Path traversal attempt detected: ../etc/passwd')
      expect(error.code).toBe(ErrorCode.PATH_TRAVERSAL)
    })
  })

  describe('NetworkError', () => {
    it('should create error with message', () => {
      const error = new NetworkError('Connection timeout', 'api/data')

      expect(error.message).toBe('Connection timeout')
      expect(error.code).toBe(ErrorCode.NETWORK_ERROR)
      expect(error.path).toBe('api/data')
    })
  })

  // ===========================================================================
  // Authorization Errors
  // ===========================================================================

  describe('AuthorizationError', () => {
    it('should create error with message', () => {
      const error = new AuthorizationError('Not authorized')

      expect(error.message).toBe('Not authorized')
      expect(error.code).toBe(ErrorCode.AUTHORIZATION_ERROR)
    })
  })

  describe('PermissionDeniedError', () => {
    it('should create error with resource', () => {
      const error = new PermissionDeniedError('users/abc123', 'delete')

      expect(error.message).toBe('Permission denied: users/abc123 (delete)')
      expect(error.code).toBe(ErrorCode.PERMISSION_DENIED)
      expect(error.resource).toBe('users/abc123')
      expect(error.action).toBe('delete')
    })
  })

  // ===========================================================================
  // Configuration Errors
  // ===========================================================================

  describe('ConfigurationError', () => {
    it('should create error with message', () => {
      const error = new ConfigurationError('Invalid configuration', {
        configKey: 'storage.backend',
      })

      expect(error.message).toBe('Invalid configuration')
      expect(error.code).toBe(ErrorCode.CONFIGURATION_ERROR)
    })
  })

  // ===========================================================================
  // Timeout Error
  // ===========================================================================

  describe('TimeoutError', () => {
    it('should create error with operation and timeout', () => {
      const error = new TimeoutError('query', 5000)

      expect(error.message).toBe('Operation "query" timed out after 5000ms')
      expect(error.code).toBe(ErrorCode.TIMEOUT)
      expect(error.operation).toBe('query')
      expect(error.timeoutMs).toBe(5000)
    })
  })

  // ===========================================================================
  // RPC Errors
  // ===========================================================================

  describe('RpcError', () => {
    it('should create error with status', () => {
      const error = new RpcError('Request failed', 500, {
        method: 'POST',
        endpoint: '/api/users',
      })

      expect(error.message).toBe('Request failed')
      expect(error.code).toBe(ErrorCode.RPC_ERROR)
      expect(error.status).toBe(500)
      expect(error.method).toBe('POST')
      expect(error.endpoint).toBe('/api/users')
    })
  })

  // ===========================================================================
  // Index Errors
  // ===========================================================================

  describe('IndexError', () => {
    it('should create error with index context', () => {
      const error = new IndexError('Index operation failed', ErrorCode.INDEX_ERROR, {
        indexName: 'email_idx',
        namespace: 'users',
      })

      expect(error.message).toBe('Index operation failed')
      expect(error.indexName).toBe('email_idx')
      expect(error.namespace).toBe('users')
    })
  })

  describe('IndexBuildError', () => {
    it('should create error with cause', () => {
      const cause = new Error('Out of memory')
      const error = new IndexBuildError('large_idx', cause)

      expect(error.message).toBe('Failed to build index "large_idx": Out of memory')
      expect(error.code).toBe(ErrorCode.INDEX_BUILD_ERROR)
      expect(error.cause).toBe(cause)
    })
  })

  describe('IndexLoadError', () => {
    it('should create error with path', () => {
      const cause = new Error('File corrupted')
      const error = new IndexLoadError('email_idx', '/data/indexes/email.idx', cause)

      expect(error.message).toBe('Failed to load index "email_idx" from "/data/indexes/email.idx": File corrupted')
      expect(error.path).toBe('/data/indexes/email.idx')
    })
  })

  describe('IndexAlreadyExistsError', () => {
    it('should create error', () => {
      const error = new IndexAlreadyExistsError('email_idx', 'users')

      expect(error.message).toBe('Index "email_idx" already exists in namespace "users"')
      expect(error.code).toBe(ErrorCode.INDEX_ALREADY_EXISTS)
    })
  })

  // ===========================================================================
  // Event Errors
  // ===========================================================================

  describe('EventError', () => {
    it('should create error with operation', () => {
      const error = new EventError('Get event', 'Event not found', {
        eventId: 'evt_123',
      })

      expect(error.message).toBe('Get event failed event evt_123: Event not found')
      expect(error.code).toBe(ErrorCode.EVENT_ERROR)
      expect(error.operation).toBe('Get event')
      expect(error.eventId).toBe('evt_123')
    })

    it('should handle snapshot context', () => {
      const error = new EventError('Get snapshot', 'Snapshot not found', {
        snapshotId: 'snap_123',
      })

      expect(error.message).toBe('Get snapshot failed snapshot snap_123: Snapshot not found')
      expect(error.snapshotId).toBe('snap_123')
    })

    it('should handle entity context', () => {
      const error = new EventError('Revert', 'Entity did not exist', {
        entityId: 'posts/abc123',
      })

      expect(error.message).toBe('Revert failed for entity posts/abc123: Entity did not exist')
      expect(error.entityId).toBe('posts/abc123')
    })
  })

  // ===========================================================================
  // Type Guards
  // ===========================================================================

  describe('Type Guards', () => {
    describe('isParqueDBError', () => {
      it('should return true for ParqueDBError', () => {
        expect(isParqueDBError(new ParqueDBError('test', ErrorCode.INTERNAL))).toBe(true)
      })

      it('should return true for subclasses', () => {
        expect(isParqueDBError(new ValidationError('test'))).toBe(true)
        expect(isParqueDBError(new NotFoundError('test'))).toBe(true)
      })

      it('should return false for regular Error', () => {
        expect(isParqueDBError(new Error('test'))).toBe(false)
      })

      it('should return false for non-errors', () => {
        expect(isParqueDBError(null)).toBe(false)
        expect(isParqueDBError('error')).toBe(false)
      })
    })

    describe('isValidationError', () => {
      it('should return true for ValidationError', () => {
        expect(isValidationError(new ValidationError('test'))).toBe(true)
      })

      it('should return false for other errors', () => {
        expect(isValidationError(new NotFoundError('test'))).toBe(false)
      })
    })

    describe('isNotFoundError', () => {
      it('should return true for NotFoundError', () => {
        expect(isNotFoundError(new NotFoundError('test'))).toBe(true)
      })

      it('should return true for EntityNotFoundError', () => {
        expect(isNotFoundError(new EntityNotFoundError('users', 'abc'))).toBe(true)
      })

      it('should return true for ParqueDBError with NOT_FOUND code', () => {
        const error = new ParqueDBError('test', ErrorCode.FILE_NOT_FOUND)
        expect(isNotFoundError(error)).toBe(true)
      })
    })

    describe('isEntityNotFoundError', () => {
      it('should return true for EntityNotFoundError', () => {
        expect(isEntityNotFoundError(new EntityNotFoundError('users', 'abc'))).toBe(true)
      })

      it('should return false for generic NotFoundError', () => {
        expect(isEntityNotFoundError(new NotFoundError('test'))).toBe(false)
      })
    })

    describe('isConflictError', () => {
      it('should return true for ConflictError', () => {
        expect(isConflictError(new ConflictError('test'))).toBe(true)
      })

      it('should return true for VersionConflictError', () => {
        expect(isConflictError(new VersionConflictError(1, 2))).toBe(true)
      })

      it('should return true for ParqueDBError with conflict codes', () => {
        expect(isConflictError(new ParqueDBError('test', ErrorCode.ALREADY_EXISTS))).toBe(true)
        expect(isConflictError(new ParqueDBError('test', ErrorCode.ETAG_MISMATCH))).toBe(true)
      })
    })

    describe('isVersionConflictError', () => {
      it('should return true for VersionConflictError', () => {
        expect(isVersionConflictError(new VersionConflictError(1, 2))).toBe(true)
      })

      it('should return false for other conflict errors', () => {
        expect(isVersionConflictError(new AlreadyExistsError('User', 'abc'))).toBe(false)
      })
    })

    describe('isETagMismatchError', () => {
      it('should return true for ETagMismatchError', () => {
        expect(isETagMismatchError(new ETagMismatchError('file', 'a', 'b'))).toBe(true)
      })

      it('should return true for ParqueDBError with ETAG_MISMATCH code', () => {
        const error = new ParqueDBError('test', ErrorCode.ETAG_MISMATCH)
        expect(isETagMismatchError(error)).toBe(true)
      })
    })

    describe('isAlreadyExistsError', () => {
      it('should return true for AlreadyExistsError', () => {
        expect(isAlreadyExistsError(new AlreadyExistsError('User', 'abc'))).toBe(true)
      })
    })

    describe('isStorageError', () => {
      it('should return true for StorageError', () => {
        expect(isStorageError(new StorageError('test'))).toBe(true)
      })

      it('should return true for storage subclasses', () => {
        expect(isStorageError(new QuotaExceededError('test'))).toBe(true)
        expect(isStorageError(new NetworkError('test'))).toBe(true)
      })
    })

    describe('isRelationshipError', () => {
      it('should return true for RelationshipError', () => {
        expect(isRelationshipError(new RelationshipError('Link', 'Post', 'test'))).toBe(true)
      })
    })

    describe('isQueryError', () => {
      it('should return true for QueryError', () => {
        expect(isQueryError(new QueryError('test'))).toBe(true)
      })

      it('should return true for InvalidFilterError', () => {
        expect(isQueryError(new InvalidFilterError('test'))).toBe(true)
      })
    })

    describe('isAuthorizationError', () => {
      it('should return true for AuthorizationError', () => {
        expect(isAuthorizationError(new AuthorizationError('test'))).toBe(true)
      })

      it('should return true for PermissionDeniedError', () => {
        expect(isAuthorizationError(new PermissionDeniedError('resource'))).toBe(true)
      })
    })

    describe('isRpcError', () => {
      it('should return true for RpcError', () => {
        expect(isRpcError(new RpcError('test', 500))).toBe(true)
      })

      it('should return true for ParqueDBError with RPC codes', () => {
        const error = new ParqueDBError('test', ErrorCode.RPC_TIMEOUT)
        expect(isRpcError(error)).toBe(true)
      })
    })

    describe('isIndexError', () => {
      it('should return true for IndexError', () => {
        expect(isIndexError(new IndexError('test'))).toBe(true)
      })

      it('should return true for index subclasses', () => {
        expect(isIndexError(new IndexBuildError('idx', new Error('fail')))).toBe(true)
      })
    })

    describe('isEventError', () => {
      it('should return true for EventError', () => {
        expect(isEventError(new EventError('Get', 'test'))).toBe(true)
      })
    })
  })

  // ===========================================================================
  // Factory Functions
  // ===========================================================================

  describe('Factory Functions', () => {
    describe('wrapError', () => {
      it('should return ParqueDBError as-is', () => {
        const error = new ValidationError('test')
        expect(wrapError(error)).toBe(error)
      })

      it('should wrap regular Error', () => {
        const error = new Error('original')
        const wrapped = wrapError(error, { operation: 'test' })

        expect(wrapped).toBeInstanceOf(ParqueDBError)
        expect(wrapped.message).toBe('original')
        expect(wrapped.cause).toBe(error)
        expect(wrapped.context.operation).toBe('test')
      })

      it('should wrap string error', () => {
        const wrapped = wrapError('string error')

        expect(wrapped).toBeInstanceOf(ParqueDBError)
        expect(wrapped.message).toBe('string error')
        expect(wrapped.code).toBe(ErrorCode.UNKNOWN)
      })
    })

    describe('errorFromStatus', () => {
      it('should create ValidationError for 400', () => {
        const error = errorFromStatus(400)
        expect(error).toBeInstanceOf(ValidationError)
      })

      it('should create AuthorizationError for 401', () => {
        const error = errorFromStatus(401)
        expect(error).toBeInstanceOf(AuthorizationError)
        expect(error.code).toBe(ErrorCode.AUTHENTICATION_REQUIRED)
      })

      it('should create PermissionDeniedError for 403', () => {
        const error = errorFromStatus(403)
        expect(error).toBeInstanceOf(PermissionDeniedError)
      })

      it('should create NotFoundError for 404', () => {
        const error = errorFromStatus(404)
        expect(error).toBeInstanceOf(NotFoundError)
      })

      it('should create ConflictError for 409', () => {
        const error = errorFromStatus(409)
        expect(error).toBeInstanceOf(ConflictError)
      })

      it('should create generic error for 500', () => {
        const error = errorFromStatus(500)
        expect(error.code).toBe(ErrorCode.INTERNAL)
      })

      it('should use custom message', () => {
        const error = errorFromStatus(404, 'User not found')
        expect(error.message).toBe('User not found')
      })
    })

    describe('assertValid', () => {
      it('should not throw when condition is true', () => {
        expect(() => assertValid(true, 'test')).not.toThrow()
      })

      it('should throw ValidationError when condition is false', () => {
        expect(() => assertValid(false, 'Invalid value')).toThrow(ValidationError)
      })

      it('should include context in error', () => {
        try {
          assertValid(false, 'Invalid', { field: 'email' })
        } catch (e) {
          expect((e as ValidationError).context.field).toBe('email')
        }
      })
    })

    describe('assertFound', () => {
      it('should not throw when value is defined', () => {
        expect(() => assertFound('value', 'test')).not.toThrow()
        expect(() => assertFound(0, 'test')).not.toThrow()
        expect(() => assertFound(false, 'test')).not.toThrow()
      })

      it('should throw NotFoundError when value is null', () => {
        expect(() => assertFound(null, 'Not found')).toThrow(NotFoundError)
      })

      it('should throw NotFoundError when value is undefined', () => {
        expect(() => assertFound(undefined, 'Not found')).toThrow(NotFoundError)
      })

      it('should use custom code', () => {
        try {
          assertFound(null, 'test', ErrorCode.ENTITY_NOT_FOUND)
        } catch (e) {
          expect((e as NotFoundError).code).toBe(ErrorCode.ENTITY_NOT_FOUND)
        }
      })
    })
  })

  // ===========================================================================
  // Error Inheritance Chain
  // ===========================================================================

  describe('Error Inheritance Chain', () => {
    const errorInstances: [string, ParqueDBError][] = [
      ['ValidationError', new ValidationError('test')],
      ['NotFoundError', new NotFoundError('test')],
      ['EntityNotFoundError', new EntityNotFoundError('ns', 'id')],
      ['ConflictError', new ConflictError('test')],
      ['VersionConflictError', new VersionConflictError(1, 2)],
      ['AlreadyExistsError', new AlreadyExistsError('Resource', 'id')],
      ['ETagMismatchError', new ETagMismatchError('path', 'a', 'b')],
      ['RelationshipError', new RelationshipError('Op', 'ns', 'msg')],
      ['QueryError', new QueryError('test')],
      ['StorageError', new StorageError('test')],
      ['AuthorizationError', new AuthorizationError('test')],
      ['RpcError', new RpcError('test', 500)],
      ['IndexError', new IndexError('test')],
      ['EventError', new EventError('op', 'msg')],
      ['TimeoutError', new TimeoutError('op', 1000)],
      ['ConfigurationError', new ConfigurationError('test')],
    ]

    for (const [name, error] of errorInstances) {
      it(`${name} should extend ParqueDBError and Error`, () => {
        expect(error).toBeInstanceOf(ParqueDBError)
        expect(error).toBeInstanceOf(Error)
      })
    }
  })

  // ===========================================================================
  // ErrorCode Enum
  // ===========================================================================

  describe('ErrorCode', () => {
    it('should have all expected codes', () => {
      // General
      expect(ErrorCode.UNKNOWN).toBe('UNKNOWN')
      expect(ErrorCode.INTERNAL).toBe('INTERNAL')
      expect(ErrorCode.TIMEOUT).toBe('TIMEOUT')

      // Validation
      expect(ErrorCode.VALIDATION_FAILED).toBe('VALIDATION_FAILED')
      expect(ErrorCode.INVALID_INPUT).toBe('INVALID_INPUT')
      expect(ErrorCode.REQUIRED_FIELD).toBe('REQUIRED_FIELD')

      // Not found
      expect(ErrorCode.NOT_FOUND).toBe('NOT_FOUND')
      expect(ErrorCode.ENTITY_NOT_FOUND).toBe('ENTITY_NOT_FOUND')

      // Conflict
      expect(ErrorCode.VERSION_CONFLICT).toBe('VERSION_CONFLICT')
      expect(ErrorCode.ALREADY_EXISTS).toBe('ALREADY_EXISTS')
      expect(ErrorCode.ETAG_MISMATCH).toBe('ETAG_MISMATCH')

      // Storage
      expect(ErrorCode.STORAGE_ERROR).toBe('STORAGE_ERROR')
      expect(ErrorCode.QUOTA_EXCEEDED).toBe('QUOTA_EXCEEDED')

      // Authorization
      expect(ErrorCode.PERMISSION_DENIED).toBe('PERMISSION_DENIED')
    })
  })
})
