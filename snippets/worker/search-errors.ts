/**
 * Search Worker Error Types
 *
 * Provides typed error variants for the search worker:
 * - NotFoundError: Resource not found (404)
 * - InvalidParamError: Invalid request parameter (400)
 * - R2Error: R2 storage operation failure (500)
 * - ParseError: Data parsing failure (500)
 *
 * These errors allow proper HTTP status codes to be returned:
 * - 400 for client errors (invalid_param)
 * - 404 for not found errors (not_found)
 * - 500 for server errors (r2_error, parse_error)
 */

// =============================================================================
// Error Codes
// =============================================================================

export type SearchErrorCode = 'not_found' | 'invalid_param' | 'r2_error' | 'parse_error'

// =============================================================================
// Base Error Class
// =============================================================================

/**
 * Base class for all search worker errors
 */
export abstract class SearchError extends Error {
  abstract readonly code: SearchErrorCode
  abstract readonly statusCode: number

  constructor(message: string) {
    super(message)
    Object.setPrototypeOf(this, new.target.prototype)
  }

  /**
   * Convert error to JSON response body
   */
  toJSON(): SearchErrorResponse {
    return {
      error: this.name,
      code: this.code,
      message: this.message,
    }
  }

  /**
   * Build HTTP Response for this error
   */
  toResponse(): Response {
    return Response.json(this.toJSON(), {
      status: this.statusCode,
      headers: { 'Content-Type': 'application/json' },
    })
  }
}

// =============================================================================
// Error Response Types
// =============================================================================

export interface SearchErrorResponse {
  error: string
  code: SearchErrorCode
  message: string
  param?: string
  dataset?: string
  path?: string
  availableEndpoints?: string[]
}

// =============================================================================
// Specific Error Classes
// =============================================================================

/**
 * Error for resources that could not be found
 *
 * Returns HTTP 404 Not Found
 */
export class NotFoundError extends SearchError {
  override readonly name = 'NotFoundError'
  readonly code: SearchErrorCode = 'not_found'
  readonly statusCode = 404

  constructor(
    message: string,
    public readonly path?: string,
    public readonly availableEndpoints?: string[]
  ) {
    super(message)
  }

  override toJSON(): SearchErrorResponse {
    return {
      ...super.toJSON(),
      ...(this.path !== undefined && { path: this.path }),
      ...(this.availableEndpoints !== undefined && { availableEndpoints: this.availableEndpoints }),
    }
  }
}

/**
 * Error for invalid request parameters
 *
 * Returns HTTP 400 Bad Request
 */
export class InvalidParamError extends SearchError {
  override readonly name = 'InvalidParamError'
  readonly code: SearchErrorCode = 'invalid_param'
  readonly statusCode = 400

  constructor(
    message: string,
    public readonly param: string,
    public readonly value?: string
  ) {
    super(message)
  }

  override toJSON(): SearchErrorResponse {
    return {
      ...super.toJSON(),
      param: this.param,
    }
  }
}

/**
 * Error for R2 storage operation failures
 *
 * Returns HTTP 500 Internal Server Error
 */
export class R2Error extends SearchError {
  override readonly name = 'R2Error'
  readonly code: SearchErrorCode = 'r2_error'
  readonly statusCode = 500

  constructor(
    message: string,
    public readonly dataset?: string,
    public readonly operation?: string,
    public override readonly cause?: Error
  ) {
    super(message)
  }

  override toJSON(): SearchErrorResponse {
    return {
      ...super.toJSON(),
      ...(this.dataset !== undefined && { dataset: this.dataset }),
    }
  }
}

/**
 * Error for data parsing failures
 *
 * Returns HTTP 500 Internal Server Error
 */
export class ParseError extends SearchError {
  override readonly name = 'ParseError'
  readonly code: SearchErrorCode = 'parse_error'
  readonly statusCode = 500

  constructor(
    message: string,
    public readonly dataset?: string,
    public override readonly cause?: Error
  ) {
    super(message)
  }

  override toJSON(): SearchErrorResponse {
    return {
      ...super.toJSON(),
      ...(this.dataset !== undefined && { dataset: this.dataset }),
    }
  }
}

// =============================================================================
// Error Handling Utilities
// =============================================================================

/**
 * Check if an error is a SearchError
 */
export function isSearchError(error: unknown): error is SearchError {
  return error instanceof SearchError
}

/**
 * Handle any error and return appropriate HTTP Response
 *
 * - SearchError: Uses error's statusCode and toResponse()
 * - Other errors: Returns 500 Internal Server Error
 */
export function handleSearchError(error: unknown): Response {
  if (isSearchError(error)) {
    return error.toResponse()
  }

  // Unknown error - return generic 500
  const message = error instanceof Error ? error.message : 'Unknown error'
  return Response.json(
    {
      error: 'Internal Server Error',
      code: 'unknown_error',
      message,
    },
    {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    }
  )
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a NotFoundError for unknown endpoints
 */
export function endpointNotFound(path: string, availableEndpoints: string[]): NotFoundError {
  return new NotFoundError(`Unknown endpoint: ${path}`, path, availableEndpoints)
}

/**
 * Create a NotFoundError for missing R2 objects
 */
export function datasetNotFound(dataset: string, key: string): NotFoundError {
  return new NotFoundError(`Dataset '${dataset}' not found (key: ${key})`, key)
}

/**
 * Create an InvalidParamError for invalid limit values
 */
export function invalidLimit(value: string): InvalidParamError {
  return new InvalidParamError(
    `Invalid limit parameter: '${value}'. Must be a positive integer.`,
    'limit',
    value
  )
}

/**
 * Create an InvalidParamError for invalid offset values
 */
export function invalidOffset(value: string): InvalidParamError {
  return new InvalidParamError(
    `Invalid offset parameter: '${value}'. Must be a non-negative integer.`,
    'offset',
    value
  )
}

/**
 * Create an R2Error for failed R2 operations
 */
export function r2OperationFailed(
  operation: string,
  dataset: string,
  cause?: Error
): R2Error {
  return new R2Error(
    `R2 ${operation} failed for dataset '${dataset}'${cause ? `: ${cause.message}` : ''}`,
    dataset,
    operation,
    cause
  )
}

/**
 * Create a ParseError for JSON parsing failures
 */
export function jsonParseFailed(dataset: string, cause?: Error): ParseError {
  return new ParseError(
    `Failed to parse JSON data for dataset '${dataset}'${cause ? `: ${cause.message}` : ''}`,
    dataset,
    cause
  )
}
