/**
 * Search Worker Error Types Unit Tests
 *
 * Tests for snippets/worker/search-errors.ts which provides typed error variants
 * for the search worker with proper HTTP status codes.
 */

import { describe, it, expect } from 'vitest'
import {
  SearchError,
  NotFoundError,
  InvalidParamError,
  R2Error,
  ParseError,
  isSearchError,
  handleSearchError,
  endpointNotFound,
  datasetNotFound,
  invalidLimit,
  invalidOffset,
  r2OperationFailed,
  jsonParseFailed,
  type SearchErrorCode,
  type SearchErrorResponse,
} from '../../../snippets/worker/search-errors'

// =============================================================================
// NotFoundError Tests
// =============================================================================

describe('NotFoundError', () => {
  it('should have correct properties', () => {
    const error = new NotFoundError('Test message', '/test/path', ['/a', '/b'])

    expect(error.name).toBe('NotFoundError')
    expect(error.code).toBe('not_found')
    expect(error.statusCode).toBe(404)
    expect(error.message).toBe('Test message')
    expect(error.path).toBe('/test/path')
    expect(error.availableEndpoints).toEqual(['/a', '/b'])
  })

  it('should be an instance of Error and SearchError', () => {
    const error = new NotFoundError('Test')

    expect(error).toBeInstanceOf(Error)
    expect(error).toBeInstanceOf(SearchError)
    expect(error).toBeInstanceOf(NotFoundError)
  })

  it('should convert to JSON correctly', () => {
    const error = new NotFoundError('Test message', '/test/path', ['/a', '/b'])
    const json = error.toJSON()

    expect(json).toEqual({
      error: 'NotFoundError',
      code: 'not_found',
      message: 'Test message',
      path: '/test/path',
      availableEndpoints: ['/a', '/b'],
    })
  })

  it('should omit undefined optional properties in JSON', () => {
    const error = new NotFoundError('Test message')
    const json = error.toJSON()

    expect(json).toEqual({
      error: 'NotFoundError',
      code: 'not_found',
      message: 'Test message',
    })
    expect(json).not.toHaveProperty('path')
    expect(json).not.toHaveProperty('availableEndpoints')
  })

  it('should build HTTP 404 response', async () => {
    const error = new NotFoundError('Resource not found', '/unknown')
    const response = error.toResponse()

    expect(response.status).toBe(404)
    expect(response.headers.get('Content-Type')).toBe('application/json')

    const body = (await response.json()) as SearchErrorResponse
    expect(body.error).toBe('NotFoundError')
    expect(body.code).toBe('not_found')
    expect(body.message).toBe('Resource not found')
    expect(body.path).toBe('/unknown')
  })
})

// =============================================================================
// InvalidParamError Tests
// =============================================================================

describe('InvalidParamError', () => {
  it('should have correct properties', () => {
    const error = new InvalidParamError('Invalid value', 'limit', 'abc')

    expect(error.name).toBe('InvalidParamError')
    expect(error.code).toBe('invalid_param')
    expect(error.statusCode).toBe(400)
    expect(error.message).toBe('Invalid value')
    expect(error.param).toBe('limit')
    expect(error.value).toBe('abc')
  })

  it('should be an instance of Error and SearchError', () => {
    const error = new InvalidParamError('Test', 'param')

    expect(error).toBeInstanceOf(Error)
    expect(error).toBeInstanceOf(SearchError)
    expect(error).toBeInstanceOf(InvalidParamError)
  })

  it('should convert to JSON correctly', () => {
    const error = new InvalidParamError('Invalid limit', 'limit')
    const json = error.toJSON()

    expect(json).toEqual({
      error: 'InvalidParamError',
      code: 'invalid_param',
      message: 'Invalid limit',
      param: 'limit',
    })
  })

  it('should build HTTP 400 response', async () => {
    const error = new InvalidParamError('Limit must be positive', 'limit', '-5')
    const response = error.toResponse()

    expect(response.status).toBe(400)
    expect(response.headers.get('Content-Type')).toBe('application/json')

    const body = (await response.json()) as SearchErrorResponse
    expect(body.error).toBe('InvalidParamError')
    expect(body.code).toBe('invalid_param')
    expect(body.param).toBe('limit')
  })
})

// =============================================================================
// R2Error Tests
// =============================================================================

describe('R2Error', () => {
  it('should have correct properties', () => {
    const cause = new Error('Connection timeout')
    const error = new R2Error('R2 failed', 'onet', 'get', cause)

    expect(error.name).toBe('R2Error')
    expect(error.code).toBe('r2_error')
    expect(error.statusCode).toBe(500)
    expect(error.message).toBe('R2 failed')
    expect(error.dataset).toBe('onet')
    expect(error.operation).toBe('get')
    expect(error.cause).toBe(cause)
  })

  it('should be an instance of Error and SearchError', () => {
    const error = new R2Error('Test')

    expect(error).toBeInstanceOf(Error)
    expect(error).toBeInstanceOf(SearchError)
    expect(error).toBeInstanceOf(R2Error)
  })

  it('should convert to JSON correctly', () => {
    const error = new R2Error('R2 operation failed', 'imdb', 'get')
    const json = error.toJSON()

    expect(json).toEqual({
      error: 'R2Error',
      code: 'r2_error',
      message: 'R2 operation failed',
      dataset: 'imdb',
    })
  })

  it('should build HTTP 500 response', async () => {
    const error = new R2Error('Storage unavailable', 'onet')
    const response = error.toResponse()

    expect(response.status).toBe(500)
    expect(response.headers.get('Content-Type')).toBe('application/json')

    const body = (await response.json()) as SearchErrorResponse
    expect(body.error).toBe('R2Error')
    expect(body.code).toBe('r2_error')
    expect(body.dataset).toBe('onet')
  })
})

// =============================================================================
// ParseError Tests
// =============================================================================

describe('ParseError', () => {
  it('should have correct properties', () => {
    const cause = new Error('Unexpected token')
    const error = new ParseError('JSON parse failed', 'unspsc', cause)

    expect(error.name).toBe('ParseError')
    expect(error.code).toBe('parse_error')
    expect(error.statusCode).toBe(500)
    expect(error.message).toBe('JSON parse failed')
    expect(error.dataset).toBe('unspsc')
    expect(error.cause).toBe(cause)
  })

  it('should be an instance of Error and SearchError', () => {
    const error = new ParseError('Test')

    expect(error).toBeInstanceOf(Error)
    expect(error).toBeInstanceOf(SearchError)
    expect(error).toBeInstanceOf(ParseError)
  })

  it('should convert to JSON correctly', () => {
    const error = new ParseError('Invalid JSON', 'onet')
    const json = error.toJSON()

    expect(json).toEqual({
      error: 'ParseError',
      code: 'parse_error',
      message: 'Invalid JSON',
      dataset: 'onet',
    })
  })

  it('should build HTTP 500 response', async () => {
    const error = new ParseError('Failed to parse data', 'imdb')
    const response = error.toResponse()

    expect(response.status).toBe(500)
    expect(response.headers.get('Content-Type')).toBe('application/json')

    const body = (await response.json()) as SearchErrorResponse
    expect(body.error).toBe('ParseError')
    expect(body.code).toBe('parse_error')
    expect(body.dataset).toBe('imdb')
  })
})

// =============================================================================
// isSearchError Tests
// =============================================================================

describe('isSearchError', () => {
  it('should return true for NotFoundError', () => {
    expect(isSearchError(new NotFoundError('test'))).toBe(true)
  })

  it('should return true for InvalidParamError', () => {
    expect(isSearchError(new InvalidParamError('test', 'param'))).toBe(true)
  })

  it('should return true for R2Error', () => {
    expect(isSearchError(new R2Error('test'))).toBe(true)
  })

  it('should return true for ParseError', () => {
    expect(isSearchError(new ParseError('test'))).toBe(true)
  })

  it('should return false for regular Error', () => {
    expect(isSearchError(new Error('test'))).toBe(false)
  })

  it('should return false for non-Error objects', () => {
    expect(isSearchError({ message: 'test' })).toBe(false)
    expect(isSearchError('error')).toBe(false)
    expect(isSearchError(null)).toBe(false)
    expect(isSearchError(undefined)).toBe(false)
  })
})

// =============================================================================
// handleSearchError Tests
// =============================================================================

describe('handleSearchError', () => {
  it('should return 404 for NotFoundError', async () => {
    const error = new NotFoundError('Not found')
    const response = handleSearchError(error)

    expect(response.status).toBe(404)
    const body = (await response.json()) as SearchErrorResponse
    expect(body.code).toBe('not_found')
  })

  it('should return 400 for InvalidParamError', async () => {
    const error = new InvalidParamError('Invalid', 'limit')
    const response = handleSearchError(error)

    expect(response.status).toBe(400)
    const body = (await response.json()) as SearchErrorResponse
    expect(body.code).toBe('invalid_param')
  })

  it('should return 500 for R2Error', async () => {
    const error = new R2Error('R2 failed')
    const response = handleSearchError(error)

    expect(response.status).toBe(500)
    const body = (await response.json()) as SearchErrorResponse
    expect(body.code).toBe('r2_error')
  })

  it('should return 500 for ParseError', async () => {
    const error = new ParseError('Parse failed')
    const response = handleSearchError(error)

    expect(response.status).toBe(500)
    const body = (await response.json()) as SearchErrorResponse
    expect(body.code).toBe('parse_error')
  })

  it('should return 500 for regular Error', async () => {
    const error = new Error('Something went wrong')
    const response = handleSearchError(error)

    expect(response.status).toBe(500)
    const body = (await response.json()) as { code: string; message: string }
    expect(body.code).toBe('unknown_error')
    expect(body.message).toBe('Something went wrong')
  })

  it('should return 500 for non-Error objects', async () => {
    const response = handleSearchError('string error')

    expect(response.status).toBe(500)
    const body = (await response.json()) as { code: string; message: string }
    expect(body.code).toBe('unknown_error')
    expect(body.message).toBe('Unknown error')
  })

  it('should have JSON content-type header', async () => {
    const response = handleSearchError(new NotFoundError('test'))
    expect(response.headers.get('Content-Type')).toBe('application/json')
  })
})

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('Factory Functions', () => {
  describe('endpointNotFound', () => {
    it('should create NotFoundError with path and endpoints', () => {
      const endpoints = ['/search/onet', '/search/unspsc']
      const error = endpointNotFound('/invalid', endpoints)

      expect(error).toBeInstanceOf(NotFoundError)
      expect(error.message).toBe('Unknown endpoint: /invalid')
      expect(error.path).toBe('/invalid')
      expect(error.availableEndpoints).toEqual(endpoints)
    })
  })

  describe('datasetNotFound', () => {
    it('should create NotFoundError for missing dataset', () => {
      const error = datasetNotFound('onet', 'onet-occupations.json')

      expect(error).toBeInstanceOf(NotFoundError)
      expect(error.message).toContain("Dataset 'onet' not found")
      expect(error.message).toContain('onet-occupations.json')
      expect(error.path).toBe('onet-occupations.json')
    })
  })

  describe('invalidLimit', () => {
    it('should create InvalidParamError for invalid limit', () => {
      const error = invalidLimit('abc')

      expect(error).toBeInstanceOf(InvalidParamError)
      expect(error.param).toBe('limit')
      expect(error.value).toBe('abc')
      expect(error.message).toContain("Invalid limit parameter: 'abc'")
      expect(error.message).toContain('positive integer')
    })
  })

  describe('invalidOffset', () => {
    it('should create InvalidParamError for invalid offset', () => {
      const error = invalidOffset('-5')

      expect(error).toBeInstanceOf(InvalidParamError)
      expect(error.param).toBe('offset')
      expect(error.value).toBe('-5')
      expect(error.message).toContain("Invalid offset parameter: '-5'")
      expect(error.message).toContain('non-negative integer')
    })
  })

  describe('r2OperationFailed', () => {
    it('should create R2Error with cause', () => {
      const cause = new Error('Connection refused')
      const error = r2OperationFailed('get', 'onet', cause)

      expect(error).toBeInstanceOf(R2Error)
      expect(error.dataset).toBe('onet')
      expect(error.operation).toBe('get')
      expect(error.cause).toBe(cause)
      expect(error.message).toContain('R2 get failed')
      expect(error.message).toContain("dataset 'onet'")
      expect(error.message).toContain('Connection refused')
    })

    it('should create R2Error without cause', () => {
      const error = r2OperationFailed('list', 'imdb')

      expect(error).toBeInstanceOf(R2Error)
      expect(error.dataset).toBe('imdb')
      expect(error.operation).toBe('list')
      expect(error.cause).toBeUndefined()
      expect(error.message).toContain('R2 list failed')
      expect(error.message).not.toContain(':')
    })
  })

  describe('jsonParseFailed', () => {
    it('should create ParseError with cause', () => {
      const cause = new Error('Unexpected token')
      const error = jsonParseFailed('unspsc', cause)

      expect(error).toBeInstanceOf(ParseError)
      expect(error.dataset).toBe('unspsc')
      expect(error.cause).toBe(cause)
      expect(error.message).toContain('Failed to parse JSON')
      expect(error.message).toContain("dataset 'unspsc'")
      expect(error.message).toContain('Unexpected token')
    })

    it('should create ParseError without cause', () => {
      const error = jsonParseFailed('onet')

      expect(error).toBeInstanceOf(ParseError)
      expect(error.dataset).toBe('onet')
      expect(error.cause).toBeUndefined()
      expect(error.message).not.toContain(':')
    })
  })
})

// =============================================================================
// Error Code Type Tests
// =============================================================================

describe('SearchErrorCode type', () => {
  it('should enforce valid error codes', () => {
    // Type check - these should compile
    const validCodes: SearchErrorCode[] = ['not_found', 'invalid_param', 'r2_error', 'parse_error']

    expect(validCodes).toHaveLength(4)
    expect(validCodes).toContain('not_found')
    expect(validCodes).toContain('invalid_param')
    expect(validCodes).toContain('r2_error')
    expect(validCodes).toContain('parse_error')
  })
})
