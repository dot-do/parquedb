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
