/**
 * Path Safety Utilities for ParqueDB
 *
 * Provides protection against prototype pollution attacks by validating
 * dot-notation paths used in document field access and mutation operations.
 *
 * Prototype pollution occurs when an attacker can set properties like
 * __proto__, constructor, or prototype on an object, potentially modifying
 * the behavior of all objects in the runtime.
 *
 * @module utils/path-safety
 */

/**
 * Set of path segments that are dangerous and could lead to prototype pollution.
 * These segments should never appear in user-controlled dot-notation paths.
 */
export const UNSAFE_PATH_SEGMENTS = new Set(['__proto__', 'constructor', 'prototype'])

/**
 * Set of dangerous top-level keys that should be rejected in user-provided objects.
 * These are the same as UNSAFE_PATH_SEGMENTS but checked at the object key level.
 */
export const DANGEROUS_KEYS = new Set(['__proto__', 'constructor', 'prototype'])

/**
 * Check if a dot-notation path contains unsafe segments that could lead to prototype pollution.
 *
 * @param path - The dot-notation path to check (e.g., "a.b.c" or "__proto__.polluted")
 * @returns true if the path contains unsafe segments
 *
 * @example
 * isUnsafePath('a.b.c')              // false
 * isUnsafePath('__proto__.polluted')  // true
 * isUnsafePath('a.constructor.b')     // true
 * isUnsafePath('a.prototype.b')       // true
 */
export function isUnsafePath(path: string): boolean {
  const parts = path.split('.')
  return parts.some(part => UNSAFE_PATH_SEGMENTS.has(part))
}

/**
 * Check if a key is a dangerous prototype pollution key.
 *
 * @param key - The key to check
 * @returns true if the key is dangerous
 *
 * @example
 * isDangerousKey('__proto__')   // true
 * isDangerousKey('constructor') // true
 * isDangerousKey('prototype')   // true
 * isDangerousKey('name')        // false
 */
export function isDangerousKey(key: string): boolean {
  return DANGEROUS_KEYS.has(key)
}

/**
 * Validate that a path is safe, throwing an error if it contains unsafe segments.
 *
 * @param path - The dot-notation path to validate
 * @throws Error if the path contains a prototype pollution attempt
 *
 * @example
 * validatePath('a.b.c')              // OK
 * validatePath('__proto__.polluted')  // throws Error
 */
export function validatePath(path: string): void {
  if (isUnsafePath(path)) {
    throw new Error(`Unsafe path detected: "${path}" contains a prototype pollution attempt`)
  }
}

/**
 * Validate that a key is not a dangerous prototype pollution key.
 *
 * @param key - The key to validate
 * @throws Error if the key is dangerous
 *
 * @example
 * validateKey('name')        // OK
 * validateKey('__proto__')   // throws Error
 */
export function validateKey(key: string): void {
  if (isDangerousKey(key)) {
    throw new Error(`Invalid key: ${key}`)
  }
}

/**
 * Validate all keys in an object are safe (not prototype pollution vectors).
 * This performs a shallow check of the object's own keys.
 *
 * @param obj - The object to validate
 * @throws Error if any key is dangerous
 *
 * @example
 * validateObjectKeys({ name: 'John', age: 30 })  // OK
 * validateObjectKeys({ __proto__: {} })           // throws Error
 */
export function validateObjectKeys(obj: Record<string, unknown> | null | undefined): void {
  if (!obj || typeof obj !== 'object') {
    return
  }
  for (const key of Object.keys(obj)) {
    if (isDangerousKey(key)) {
      throw new Error(`Invalid key: ${key}`)
    }
  }
}

/**
 * Recursively validate all keys in an object and its nested objects/arrays.
 * This is more thorough but slower than validateObjectKeys.
 *
 * @param obj - The object to validate
 * @param maxDepth - Maximum recursion depth (default: 10)
 * @throws Error if any key is dangerous
 *
 * @example
 * validateObjectKeysDeep({ user: { name: 'John' } })           // OK
 * validateObjectKeysDeep({ user: { __proto__: {} } })          // throws Error
 * validateObjectKeysDeep({ items: [{ __proto__: {} }] })       // throws Error
 */
export function validateObjectKeysDeep(
  obj: unknown,
  maxDepth: number = 10
): void {
  if (maxDepth <= 0) {
    return // Prevent infinite recursion
  }

  if (!obj || typeof obj !== 'object') {
    return
  }

  if (Array.isArray(obj)) {
    for (const item of obj) {
      validateObjectKeysDeep(item, maxDepth - 1)
    }
    return
  }

  const record = obj as Record<string, unknown>
  for (const key of Object.keys(record)) {
    if (isDangerousKey(key)) {
      throw new Error(`Invalid key: ${key}`)
    }
    validateObjectKeysDeep(record[key], maxDepth - 1)
  }
}

/**
 * Sanitize an object by removing dangerous keys (shallow).
 * Returns a new object without the dangerous keys.
 *
 * @param obj - The object to sanitize
 * @returns A new object with dangerous keys removed
 *
 * @example
 * sanitizeObject({ name: 'John', __proto__: {} })  // { name: 'John' }
 */
export function sanitizeObject<T extends Record<string, unknown>>(
  obj: T | null | undefined
): Record<string, unknown> {
  if (!obj || typeof obj !== 'object') {
    return {}
  }
  const result: Record<string, unknown> = {}
  for (const key of Object.keys(obj)) {
    if (!isDangerousKey(key)) {
      result[key] = obj[key]
    }
  }
  return result
}
