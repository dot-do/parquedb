/**
 * Shared validation utilities for storage backends
 *
 * Provides standardized input validation across all backend implementations
 * to ensure consistent error handling and behavior.
 */

/**
 * Error thrown when range parameters are invalid
 */
export class InvalidRangeError extends Error {
  override readonly name = 'InvalidRangeError'
  readonly start: number
  readonly end: number

  constructor(message: string, start: number, end: number) {
    super(message)
    Object.setPrototypeOf(this, InvalidRangeError.prototype)
    this.start = start
    this.end = end
  }
}

/**
 * Validate range parameters for readRange operations
 *
 * @param start - Start byte position (must be >= 0)
 * @param end - End byte position (must be >= start)
 * @throws InvalidRangeError if parameters are invalid
 */
export function validateRange(start: number, end: number): void {
  if (start < 0) {
    throw new InvalidRangeError(
      `Invalid range: start (${start}) must be non-negative`,
      start,
      end
    )
  }
  if (end < start) {
    throw new InvalidRangeError(
      `Invalid range: end (${end}) must be >= start (${start})`,
      start,
      end
    )
  }
}

/**
 * Validate that a path is not empty
 *
 * @param path - The path to validate
 * @param operation - The operation name for error messages
 * @throws Error if path is empty
 */
export function validatePath(path: string, operation: string): void {
  if (path === undefined || path === null) {
    throw new Error(`${operation}: path is required`)
  }
}

/**
 * Validate part number for multipart uploads (1-10000)
 *
 * @param partNumber - The part number to validate
 * @throws Error if part number is out of range
 */
export function validatePartNumber(partNumber: number): void {
  if (partNumber < 1 || partNumber > 10000) {
    throw new Error(`Invalid part number: ${partNumber}. Must be between 1 and 10000`)
  }
}

/**
 * Validate data is not null or undefined
 *
 * @param data - The data to validate
 * @param operation - The operation name for error messages
 * @throws Error if data is null or undefined
 */
export function validateData(data: Uint8Array | null | undefined, operation: string): void {
  if (data === null || data === undefined) {
    throw new Error(`${operation}: data is required`)
  }
}
