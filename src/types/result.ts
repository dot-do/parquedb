/**
 * Result Type for Type-Safe Error Handling
 *
 * This module provides a Result<T, E> type for representing operations that may
 * succeed or fail, following the functional programming pattern. Using Result
 * instead of throwing exceptions provides better type safety and makes error
 * handling explicit.
 *
 * @example
 * ```typescript
 * function parseNumber(s: string): Result<number, string> {
 *   const n = Number(s)
 *   return Number.isNaN(n)
 *     ? Err('Invalid number')
 *     : Ok(n)
 * }
 *
 * const result = parseNumber('42')
 * if (isOk(result)) {
 *   console.log(result.value) // 42
 * } else {
 *   console.log(result.error) // Would be error message
 * }
 * ```
 */

// =============================================================================
// Core Result Type
// =============================================================================

/**
 * A discriminated union representing either a successful result (Ok) or a failure (Err).
 *
 * - `{ ok: true; value: T }` - Success case containing the value
 * - `{ ok: false; error: E }` - Failure case containing the error
 *
 * @typeParam T - The type of the success value
 * @typeParam E - The type of the error (defaults to Error)
 */
export type Result<T, E = Error> =
  | { ok: true; value: T }
  | { ok: false; error: E }

// =============================================================================
// Constructors
// =============================================================================

/**
 * Creates a successful Result containing the given value.
 *
 * @param value - The success value
 * @returns A Result in the Ok state
 *
 * @example
 * ```typescript
 * const result = Ok(42)
 * // result: { ok: true, value: 42 }
 * ```
 */
export const Ok = <T>(value: T): Result<T, never> => ({ ok: true, value })

/**
 * Creates a failed Result containing the given error.
 *
 * @param error - The error value
 * @returns A Result in the Err state
 *
 * @example
 * ```typescript
 * const result = Err(new Error('Something went wrong'))
 * // result: { ok: false, error: Error('Something went wrong') }
 * ```
 */
export const Err = <E>(error: E): Result<never, E> => ({ ok: false, error })

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Type guard to check if a Result is in the Ok (success) state.
 *
 * @param result - The Result to check
 * @returns true if the Result is Ok, false otherwise
 *
 * @example
 * ```typescript
 * const result = Ok(42)
 * if (isOk(result)) {
 *   // TypeScript knows result.value exists here
 *   console.log(result.value) // 42
 * }
 * ```
 */
export function isOk<T, E>(result: Result<T, E>): result is { ok: true; value: T } {
  return result.ok === true
}

/**
 * Type guard to check if a Result is in the Err (failure) state.
 *
 * @param result - The Result to check
 * @returns true if the Result is Err, false otherwise
 *
 * @example
 * ```typescript
 * const result = Err('Something went wrong')
 * if (isErr(result)) {
 *   // TypeScript knows result.error exists here
 *   console.log(result.error) // 'Something went wrong'
 * }
 * ```
 */
export function isErr<T, E>(result: Result<T, E>): result is { ok: false; error: E } {
  return result.ok === false
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Extracts the value from a Result, throwing if the Result is an Err.
 *
 * Use this when you're certain the Result is Ok, or when you want to
 * propagate the error as an exception.
 *
 * @param result - The Result to unwrap
 * @returns The contained value if Ok
 * @throws The contained error if Err
 *
 * @example
 * ```typescript
 * const ok = Ok(42)
 * console.log(unwrap(ok)) // 42
 *
 * const err = Err(new Error('fail'))
 * unwrap(err) // throws Error('fail')
 * ```
 */
export function unwrap<T, E>(result: Result<T, E>): T {
  if (isOk(result)) {
    return result.value
  }
  throw result.error
}

/**
 * Extracts the value from a Result, returning a default value if the Result is an Err.
 *
 * This is a safe way to extract a value without risking an exception.
 *
 * @param result - The Result to unwrap
 * @param defaultValue - The value to return if Result is Err
 * @returns The contained value if Ok, otherwise the default value
 *
 * @example
 * ```typescript
 * const ok = Ok(42)
 * console.log(unwrapOr(ok, 0)) // 42
 *
 * const err = Err(new Error('fail'))
 * console.log(unwrapOr(err, 0)) // 0
 * ```
 */
export function unwrapOr<T, E>(result: Result<T, E>, defaultValue: T): T {
  if (isOk(result)) {
    return result.value
  }
  return defaultValue
}

/**
 * Extracts the error from a Result, throwing if the Result is Ok.
 *
 * @param result - The Result to unwrap
 * @returns The contained error if Err
 * @throws Error if the Result is Ok
 *
 * @example
 * ```typescript
 * const err = Err('Something went wrong')
 * console.log(unwrapErr(err)) // 'Something went wrong'
 *
 * const ok = Ok(42)
 * unwrapErr(ok) // throws Error('Called unwrapErr on Ok value')
 * ```
 */
export function unwrapErr<T, E>(result: Result<T, E>): E {
  if (isErr(result)) {
    return result.error
  }
  throw new Error('Called unwrapErr on Ok value')
}

/**
 * Applies a function to the value inside an Ok Result, leaving Err unchanged.
 *
 * @param result - The Result to map over
 * @param fn - The function to apply to the Ok value
 * @returns A new Result with the mapped value, or the original Err
 *
 * @example
 * ```typescript
 * const ok = Ok(2)
 * const doubled = map(ok, x => x * 2)
 * // doubled: { ok: true, value: 4 }
 *
 * const err = Err('fail')
 * const mapped = map(err, x => x * 2)
 * // mapped: { ok: false, error: 'fail' }
 * ```
 */
export function map<T, U, E>(result: Result<T, E>, fn: (value: T) => U): Result<U, E> {
  if (isOk(result)) {
    return Ok(fn(result.value))
  }
  return result
}

/**
 * Applies a function to the error inside an Err Result, leaving Ok unchanged.
 *
 * @param result - The Result to map over
 * @param fn - The function to apply to the Err error
 * @returns A new Result with the mapped error, or the original Ok
 *
 * @example
 * ```typescript
 * const err = Err('fail')
 * const mapped = mapErr(err, e => new Error(e))
 * // mapped: { ok: false, error: Error('fail') }
 *
 * const ok = Ok(42)
 * const unchanged = mapErr(ok, e => new Error(e))
 * // unchanged: { ok: true, value: 42 }
 * ```
 */
export function mapErr<T, E, F>(result: Result<T, E>, fn: (error: E) => F): Result<T, F> {
  if (isErr(result)) {
    return Err(fn(result.error))
  }
  return result
}

/**
 * Chains Result-returning functions, short-circuiting on the first Err.
 *
 * Also known as `flatMap` or `bind` in other languages.
 *
 * @param result - The Result to chain from
 * @param fn - A function that takes the Ok value and returns a new Result
 * @returns The Result from fn if input is Ok, otherwise the original Err
 *
 * @example
 * ```typescript
 * function parseNumber(s: string): Result<number, string> {
 *   const n = Number(s)
 *   return Number.isNaN(n) ? Err('Invalid number') : Ok(n)
 * }
 *
 * function double(n: number): Result<number, string> {
 *   return Ok(n * 2)
 * }
 *
 * const result = andThen(parseNumber('21'), double)
 * // result: { ok: true, value: 42 }
 * ```
 */
export function andThen<T, U, E>(result: Result<T, E>, fn: (value: T) => Result<U, E>): Result<U, E> {
  if (isOk(result)) {
    return fn(result.value)
  }
  return result
}

/**
 * Converts a function that may throw into one that returns a Result.
 *
 * @param fn - A function that may throw
 * @returns A Result containing the return value or the caught error
 *
 * @example
 * ```typescript
 * const result = tryCatch(() => JSON.parse('{"key": "value"}'))
 * // result: { ok: true, value: { key: 'value' } }
 *
 * const invalid = tryCatch(() => JSON.parse('invalid json'))
 * // invalid: { ok: false, error: SyntaxError }
 * ```
 */
export function tryCatch<T>(fn: () => T): Result<T, Error> {
  try {
    return Ok(fn())
  } catch (error) {
    return Err(error instanceof Error ? error : new Error(String(error)))
  }
}

/**
 * Converts an async function that may throw into one that returns a Promise<Result>.
 *
 * @param fn - An async function that may throw
 * @returns A Promise resolving to a Result containing the return value or the caught error
 *
 * @example
 * ```typescript
 * const result = await tryCatchAsync(async () => {
 *   const response = await fetch('/api/data')
 *   return response.json()
 * })
 * ```
 */
export async function tryCatchAsync<T>(fn: () => Promise<T>): Promise<Result<T, Error>> {
  try {
    return Ok(await fn())
  } catch (error) {
    return Err(error instanceof Error ? error : new Error(String(error)))
  }
}

/**
 * Converts a Result into an optional value, returning undefined for Err.
 *
 * @param result - The Result to convert
 * @returns The value if Ok, undefined if Err
 *
 * @example
 * ```typescript
 * const ok = Ok(42)
 * console.log(toOption(ok)) // 42
 *
 * const err = Err('fail')
 * console.log(toOption(err)) // undefined
 * ```
 */
export function toOption<T, E>(result: Result<T, E>): T | undefined {
  return isOk(result) ? result.value : undefined
}

/**
 * Converts a nullable value into a Result.
 *
 * @param value - The value to convert
 * @param error - The error to use if value is null or undefined
 * @returns Ok(value) if value is not null/undefined, otherwise Err(error)
 *
 * @example
 * ```typescript
 * const result = fromNullable(user.name, new Error('Name is required'))
 * ```
 */
export function fromNullable<T, E>(value: T | null | undefined, error: E): Result<T, E> {
  return value != null ? Ok(value) : Err(error)
}
