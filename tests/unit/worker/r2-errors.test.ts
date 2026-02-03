/**
 * R2 Error Handling Tests
 *
 * Tests for the R2 bucket error handling utilities.
 */

import { describe, it, expect, vi } from 'vitest'
import {
  MissingBucketError,
  BucketOperationError,
  requireBucket,
  getCdnBucket,
  hasBucket,
  hasCdnBucket,
  handleBucketError,
  buildMissingBucketResponse,
  buildBucketOperationErrorResponse,
} from '../../../src/worker/r2-errors'
import type { Env } from '../../../src/types/worker'

describe('R2 Error Types', () => {
  describe('MissingBucketError', () => {
    it('should create error with bucket name', () => {
      const error = new MissingBucketError('BUCKET')
      expect(error.name).toBe('MissingBucketError')
      expect(error.bucketName).toBe('BUCKET')
      expect(error.message).toContain('BUCKET')
      expect(error.message).toContain('not configured')
    })

    it('should include context when provided', () => {
      const error = new MissingBucketError('BUCKET', 'Required for testing.')
      expect(error.message).toContain('Required for testing.')
      expect(error.context).toBe('Required for testing.')
    })

    it('should be instanceof Error', () => {
      const error = new MissingBucketError('BUCKET')
      expect(error).toBeInstanceOf(Error)
      expect(error).toBeInstanceOf(MissingBucketError)
    })
  })

  describe('BucketOperationError', () => {
    it('should create error with operation details', () => {
      const error = new BucketOperationError('get', 'BUCKET', 'data/test.parquet')
      expect(error.name).toBe('BucketOperationError')
      expect(error.operation).toBe('get')
      expect(error.bucketName).toBe('BUCKET')
      expect(error.path).toBe('data/test.parquet')
    })

    it('should include cause when provided', () => {
      const cause = new Error('Network error')
      const error = new BucketOperationError('put', 'BUCKET', 'data/test.parquet', cause)
      expect(error.cause).toBe(cause)
      expect(error.message).toContain('Network error')
    })

    it('should be instanceof Error', () => {
      const error = new BucketOperationError('get', 'BUCKET', 'data/test.parquet')
      expect(error).toBeInstanceOf(Error)
      expect(error).toBeInstanceOf(BucketOperationError)
    })
  })
})

describe('Validation Functions', () => {
  describe('requireBucket', () => {
    it('should return bucket when configured', () => {
      const mockBucket = {} as R2Bucket
      const env = { BUCKET: mockBucket } as Env
      const result = requireBucket(env)
      expect(result).toBe(mockBucket)
    })

    it('should throw MissingBucketError when not configured', () => {
      const env = {} as Env
      expect(() => requireBucket(env)).toThrow(MissingBucketError)
    })
  })

  describe('getCdnBucket', () => {
    it('should return CDN bucket when configured', () => {
      const mockBucket = {} as R2Bucket
      const env = { CDN_BUCKET: mockBucket } as Env
      const result = getCdnBucket(env)
      expect(result).toBe(mockBucket)
    })

    it('should return undefined when not configured', () => {
      const env = {} as Env
      const result = getCdnBucket(env)
      expect(result).toBeUndefined()
    })
  })

  describe('hasBucket', () => {
    it('should return true when BUCKET is configured', () => {
      const env = { BUCKET: {} as R2Bucket } as Env
      expect(hasBucket(env)).toBe(true)
    })

    it('should return false when BUCKET is not configured', () => {
      const env = {} as Env
      expect(hasBucket(env)).toBe(false)
    })
  })

  describe('hasCdnBucket', () => {
    it('should return true when CDN_BUCKET is configured', () => {
      const env = { CDN_BUCKET: {} as R2Bucket } as Env
      expect(hasCdnBucket(env)).toBe(true)
    })

    it('should return false when CDN_BUCKET is not configured', () => {
      const env = {} as Env
      expect(hasCdnBucket(env)).toBe(false)
    })
  })
})

describe('Response Helpers', () => {
  describe('buildMissingBucketResponse', () => {
    it('should return 503 response', async () => {
      const error = new MissingBucketError('BUCKET')
      const response = buildMissingBucketResponse(error)

      expect(response.status).toBe(503)

      const body = await response.json() as Record<string, unknown>
      expect(body.error).toBe('Service Unavailable')
      expect(body.bucket).toBe('BUCKET')
      expect(body.code).toBe('MISSING_BUCKET')
    })
  })

  describe('buildBucketOperationErrorResponse', () => {
    it('should return 500 response', async () => {
      const error = new BucketOperationError('get', 'BUCKET', 'data/test.parquet')
      const response = buildBucketOperationErrorResponse(error)

      expect(response.status).toBe(500)

      const body = await response.json() as Record<string, unknown>
      expect(body.error).toBe('Storage Error')
      expect(body.operation).toBe('get')
      expect(body.bucket).toBe('BUCKET')
      expect(body.path).toBe('data/test.parquet')
      expect(body.code).toBe('BUCKET_OPERATION_FAILED')
    })
  })

  describe('handleBucketError', () => {
    it('should handle MissingBucketError', () => {
      const error = new MissingBucketError('BUCKET')
      const response = handleBucketError(error)

      expect(response).not.toBeNull()
      expect(response!.status).toBe(503)
    })

    it('should handle BucketOperationError', () => {
      const error = new BucketOperationError('get', 'BUCKET', 'data/test.parquet')
      const response = handleBucketError(error)

      expect(response).not.toBeNull()
      expect(response!.status).toBe(500)
    })

    it('should return null for other errors', () => {
      const error = new Error('Generic error')
      const response = handleBucketError(error)

      expect(response).toBeNull()
    })

    it('should return null for non-errors', () => {
      const response = handleBucketError('not an error')
      expect(response).toBeNull()
    })
  })
})
