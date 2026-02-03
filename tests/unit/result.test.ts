/**
 * Result Type Unit Tests
 *
 * Comprehensive tests for the Result<T, E> type and its utility functions.
 */

import { describe, it, expect } from 'vitest'
import {
  Ok,
  Err,
  isOk,
  isErr,
  unwrap,
  unwrapOr,
  unwrapErr,
  map,
  mapErr,
  andThen,
  tryCatch,
  tryCatchAsync,
  toOption,
  fromNullable,
  type Result,
} from '../../src/types/result'

describe('Result Type', () => {
  describe('Ok constructor', () => {
    it('should create an Ok result with the given value', () => {
      const result = Ok(42)
      expect(result).toEqual({ ok: true, value: 42 })
    })

    it('should work with different types', () => {
      expect(Ok('hello')).toEqual({ ok: true, value: 'hello' })
      expect(Ok(true)).toEqual({ ok: true, value: true })
      expect(Ok(null)).toEqual({ ok: true, value: null })
      expect(Ok({ foo: 'bar' })).toEqual({ ok: true, value: { foo: 'bar' } })
      expect(Ok([1, 2, 3])).toEqual({ ok: true, value: [1, 2, 3] })
    })

    it('should work with undefined as a valid value', () => {
      const result = Ok(undefined)
      expect(result.ok).toBe(true)
      expect(result.value).toBe(undefined)
    })
  })

  describe('Err constructor', () => {
    it('should create an Err result with the given error', () => {
      const result = Err('Something went wrong')
      expect(result).toEqual({ ok: false, error: 'Something went wrong' })
    })

    it('should work with Error objects', () => {
      const error = new Error('Test error')
      const result = Err(error)
      expect(result.ok).toBe(false)
      expect(result.error).toBe(error)
    })

    it('should work with custom error types', () => {
      type CustomError = { code: number; message: string }
      const error: CustomError = { code: 404, message: 'Not found' }
      const result = Err(error)
      expect(result.ok).toBe(false)
      expect(result.error).toEqual({ code: 404, message: 'Not found' })
    })
  })

  describe('isOk type guard', () => {
    it('should return true for Ok results', () => {
      const result = Ok(42)
      expect(isOk(result)).toBe(true)
    })

    it('should return false for Err results', () => {
      const result = Err('error')
      expect(isOk(result)).toBe(false)
    })

    it('should narrow the type correctly', () => {
      const result: Result<number, string> = Ok(42)
      if (isOk(result)) {
        // TypeScript should know result.value is number here
        const value: number = result.value
        expect(value).toBe(42)
      }
    })
  })

  describe('isErr type guard', () => {
    it('should return true for Err results', () => {
      const result = Err('error')
      expect(isErr(result)).toBe(true)
    })

    it('should return false for Ok results', () => {
      const result = Ok(42)
      expect(isErr(result)).toBe(false)
    })

    it('should narrow the type correctly', () => {
      const result: Result<number, string> = Err('error')
      if (isErr(result)) {
        // TypeScript should know result.error is string here
        const error: string = result.error
        expect(error).toBe('error')
      }
    })
  })

  describe('unwrap', () => {
    it('should return the value for Ok results', () => {
      const result = Ok(42)
      expect(unwrap(result)).toBe(42)
    })

    it('should throw for Err results', () => {
      const result = Err(new Error('Test error'))
      expect(() => unwrap(result)).toThrow('Test error')
    })

    it('should throw the exact error for Err results', () => {
      const error = new Error('Specific error')
      const result = Err(error)
      expect(() => unwrap(result)).toThrow(error)
    })

    it('should throw non-Error values as-is', () => {
      const result = Err('string error')
      expect(() => unwrap(result)).toThrow('string error')
    })
  })

  describe('unwrapOr', () => {
    it('should return the value for Ok results', () => {
      const result = Ok(42)
      expect(unwrapOr(result, 0)).toBe(42)
    })

    it('should return the default for Err results', () => {
      const result: Result<number, string> = Err('error')
      expect(unwrapOr(result, 0)).toBe(0)
    })

    it('should work with different types', () => {
      const okString = Ok('hello')
      expect(unwrapOr(okString, 'default')).toBe('hello')

      const errString: Result<string, Error> = Err(new Error('error'))
      expect(unwrapOr(errString, 'default')).toBe('default')
    })

    it('should handle null and undefined defaults', () => {
      const err: Result<string | null, Error> = Err(new Error('error'))
      expect(unwrapOr(err, null)).toBe(null)
    })
  })

  describe('unwrapErr', () => {
    it('should return the error for Err results', () => {
      const result = Err('error message')
      expect(unwrapErr(result)).toBe('error message')
    })

    it('should throw for Ok results', () => {
      const result = Ok(42)
      expect(() => unwrapErr(result)).toThrow('Called unwrapErr on Ok value')
    })

    it('should return Error objects', () => {
      const error = new Error('Test error')
      const result = Err(error)
      expect(unwrapErr(result)).toBe(error)
    })
  })

  describe('map', () => {
    it('should transform the value for Ok results', () => {
      const result = Ok(21)
      const doubled = map(result, (x) => x * 2)
      expect(doubled).toEqual({ ok: true, value: 42 })
    })

    it('should pass through Err results unchanged', () => {
      const result: Result<number, string> = Err('error')
      const mapped = map(result, (x) => x * 2)
      expect(mapped).toEqual({ ok: false, error: 'error' })
    })

    it('should allow type transformation', () => {
      const result = Ok(42)
      const stringified = map(result, (x) => x.toString())
      expect(stringified).toEqual({ ok: true, value: '42' })
    })

    it('should chain multiple maps', () => {
      const result = Ok(2)
      const chained = map(map(result, (x) => x + 1), (x) => x * 2)
      expect(chained).toEqual({ ok: true, value: 6 })
    })
  })

  describe('mapErr', () => {
    it('should transform the error for Err results', () => {
      const result: Result<number, string> = Err('error')
      const mapped = mapErr(result, (e) => new Error(e))
      expect(mapped.ok).toBe(false)
      if (!mapped.ok) {
        expect(mapped.error).toBeInstanceOf(Error)
        expect(mapped.error.message).toBe('error')
      }
    })

    it('should pass through Ok results unchanged', () => {
      const result: Result<number, string> = Ok(42)
      const mapped = mapErr(result, (e) => new Error(e))
      expect(mapped).toEqual({ ok: true, value: 42 })
    })

    it('should allow error type transformation', () => {
      type ApiError = { code: number; message: string }
      const result: Result<number, string> = Err('not found')
      const mapped = mapErr(result, (e): ApiError => ({ code: 404, message: e }))
      expect(mapped).toEqual({ ok: false, error: { code: 404, message: 'not found' } })
    })
  })

  describe('andThen (flatMap/bind)', () => {
    function parseNumber(s: string): Result<number, string> {
      const n = Number(s)
      return Number.isNaN(n) ? Err('Invalid number') : Ok(n)
    }

    function divide(a: number, b: number): Result<number, string> {
      return b === 0 ? Err('Division by zero') : Ok(a / b)
    }

    it('should chain successful results', () => {
      const result = andThen(parseNumber('42'), (n) => divide(n, 2))
      expect(result).toEqual({ ok: true, value: 21 })
    })

    it('should short-circuit on first error', () => {
      const result = andThen(parseNumber('invalid'), (n) => divide(n, 2))
      expect(result).toEqual({ ok: false, error: 'Invalid number' })
    })

    it('should propagate errors from chained function', () => {
      const result = andThen(parseNumber('42'), (n) => divide(n, 0))
      expect(result).toEqual({ ok: false, error: 'Division by zero' })
    })

    it('should allow multiple chains', () => {
      const result = andThen(
        andThen(parseNumber('100'), (n) => divide(n, 2)),
        (n) => divide(n, 5)
      )
      expect(result).toEqual({ ok: true, value: 10 })
    })
  })

  describe('tryCatch', () => {
    it('should return Ok for successful functions', () => {
      const result = tryCatch(() => JSON.parse('{"key": "value"}'))
      expect(result.ok).toBe(true)
      if (result.ok) {
        expect(result.value).toEqual({ key: 'value' })
      }
    })

    it('should return Err for throwing functions', () => {
      const result = tryCatch(() => JSON.parse('invalid json'))
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error).toBeInstanceOf(SyntaxError)
      }
    })

    it('should wrap non-Error throws in Error', () => {
      const result = tryCatch(() => {
        throw 'string error'
      })
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error).toBeInstanceOf(Error)
        expect(result.error.message).toBe('string error')
      }
    })

    it('should preserve Error objects', () => {
      const customError = new TypeError('Custom type error')
      const result = tryCatch(() => {
        throw customError
      })
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error).toBe(customError)
      }
    })
  })

  describe('tryCatchAsync', () => {
    it('should return Ok for successful async functions', async () => {
      const result = await tryCatchAsync(async () => {
        return Promise.resolve(42)
      })
      expect(result).toEqual({ ok: true, value: 42 })
    })

    it('should return Err for rejected promises', async () => {
      const result = await tryCatchAsync(async () => {
        throw new Error('Async error')
      })
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.message).toBe('Async error')
      }
    })

    it('should wrap non-Error rejections', async () => {
      const result = await tryCatchAsync(async () => {
        return Promise.reject('string rejection')
      })
      expect(result.ok).toBe(false)
      if (!result.ok) {
        expect(result.error.message).toBe('string rejection')
      }
    })

    it('should handle complex async operations', async () => {
      const result = await tryCatchAsync(async () => {
        const value = await Promise.resolve(21)
        return value * 2
      })
      expect(result).toEqual({ ok: true, value: 42 })
    })
  })

  describe('toOption', () => {
    it('should return the value for Ok results', () => {
      const result = Ok(42)
      expect(toOption(result)).toBe(42)
    })

    it('should return undefined for Err results', () => {
      const result = Err('error')
      expect(toOption(result)).toBeUndefined()
    })

    it('should handle Ok with undefined value', () => {
      const result = Ok(undefined)
      expect(toOption(result)).toBeUndefined()
    })

    it('should handle Ok with null value', () => {
      const result = Ok(null)
      expect(toOption(result)).toBeNull()
    })
  })

  describe('fromNullable', () => {
    it('should return Ok for non-null values', () => {
      const result = fromNullable(42, new Error('Value was null'))
      expect(result).toEqual({ ok: true, value: 42 })
    })

    it('should return Ok for empty string (not null)', () => {
      const result = fromNullable('', new Error('Value was null'))
      expect(result).toEqual({ ok: true, value: '' })
    })

    it('should return Ok for zero (not null)', () => {
      const result = fromNullable(0, new Error('Value was null'))
      expect(result).toEqual({ ok: true, value: 0 })
    })

    it('should return Ok for false (not null)', () => {
      const result = fromNullable(false, new Error('Value was null'))
      expect(result).toEqual({ ok: true, value: false })
    })

    it('should return Err for null', () => {
      const error = new Error('Value was null')
      const result = fromNullable(null, error)
      expect(result).toEqual({ ok: false, error })
    })

    it('should return Err for undefined', () => {
      const error = new Error('Value was undefined')
      const result = fromNullable(undefined, error)
      expect(result).toEqual({ ok: false, error })
    })

    it('should work with string errors', () => {
      const result = fromNullable<number, string>(null, 'Value required')
      expect(result).toEqual({ ok: false, error: 'Value required' })
    })
  })

  describe('Type inference', () => {
    it('should correctly infer types in conditional branches', () => {
      const result: Result<number, string> = Math.random() > 0.5 ? Ok(42) : Err('error')

      if (isOk(result)) {
        // TypeScript should allow this without type assertions
        const doubled: number = result.value * 2
        expect(typeof doubled).toBe('number')
      } else {
        // TypeScript should allow this without type assertions
        const upper: string = result.error.toUpperCase()
        expect(typeof upper).toBe('string')
      }
    })

    it('should work with generic functions', () => {
      function identity<T>(result: Result<T, Error>): Result<T, Error> {
        return result
      }

      const ok = identity(Ok(42))
      const err = identity(Err(new Error('error')))

      expect(isOk(ok)).toBe(true)
      expect(isErr(err)).toBe(true)
    })
  })

  describe('Real-world usage patterns', () => {
    it('should handle validation chains', () => {
      function validateEmail(email: string): Result<string, string> {
        return email.includes('@') ? Ok(email) : Err('Invalid email format')
      }

      function validateLength(s: string): Result<string, string> {
        return s.length >= 5 ? Ok(s) : Err('Too short')
      }

      const validEmail = andThen(validateLength('test@example.com'), validateEmail)
      expect(validEmail).toEqual({ ok: true, value: 'test@example.com' })

      const shortEmail = andThen(validateLength('a@b'), validateEmail)
      expect(shortEmail).toEqual({ ok: false, error: 'Too short' })

      const invalidEmail = andThen(validateLength('invalid-email'), validateEmail)
      expect(invalidEmail).toEqual({ ok: false, error: 'Invalid email format' })
    })

    it('should handle parsing operations', () => {
      function parseJSON<T>(json: string): Result<T, Error> {
        return tryCatch(() => JSON.parse(json) as T)
      }

      type User = { name: string; age: number }

      const validResult = parseJSON<User>('{"name": "Alice", "age": 30}')
      expect(isOk(validResult)).toBe(true)
      if (isOk(validResult)) {
        expect(validResult.value.name).toBe('Alice')
        expect(validResult.value.age).toBe(30)
      }

      const invalidResult = parseJSON<User>('invalid')
      expect(isErr(invalidResult)).toBe(true)
    })

    it('should handle optional chaining patterns', () => {
      interface Config {
        database?: {
          host?: string | undefined
        }
      }

      function getDbHost(config: Config): Result<string, string> {
        return fromNullable(config.database?.host, 'Database host not configured')
      }

      expect(getDbHost({ database: { host: 'localhost' } })).toEqual({ ok: true, value: 'localhost' })
      expect(getDbHost({ database: {} })).toEqual({ ok: false, error: 'Database host not configured' })
      expect(getDbHost({})).toEqual({ ok: false, error: 'Database host not configured' })
    })
  })
})
