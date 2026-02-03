/**
 * Shared utility functions for storage backends
 *
 * This module provides common utility functions used across multiple
 * storage backend implementations to reduce code duplication.
 */

/**
 * Convert a glob pattern to a regular expression
 *
 * Supports basic glob patterns:
 * - `*` matches zero or more characters
 * - `?` matches exactly one character
 * - All other special regex characters are escaped
 *
 * @param pattern - The glob pattern to convert
 * @returns A RegExp that matches the pattern
 *
 * @example
 * ```typescript
 * globToRegex('*.txt').test('file.txt')     // true
 * globToRegex('*.txt').test('file.json')    // false
 * globToRegex('data?.csv').test('data1.csv') // true
 * globToRegex('file[1].txt').test('file[1].txt') // true (brackets escaped)
 * ```
 */
export function globToRegex(pattern: string): RegExp {
  const escaped = pattern
    .replace(/[.+^${}()|[\]\\]/g, '\\$&') // Escape special regex chars except * and ?
    .replace(/\*/g, '.*') // * matches any characters
    .replace(/\?/g, '.') // ? matches single character
  return new RegExp(`^${escaped}$`)
}

/**
 * Test if a filename matches a glob pattern
 *
 * @param filename - The filename to test
 * @param pattern - The glob pattern to match against
 * @returns true if the filename matches the pattern
 *
 * @example
 * ```typescript
 * matchGlob('file.txt', '*.txt')      // true
 * matchGlob('file.txt', '*.json')     // false
 * matchGlob('data1.csv', 'data?.csv') // true
 * ```
 */
export function matchGlob(filename: string, pattern: string): boolean {
  const regex = globToRegex(pattern)
  return regex.test(filename)
}

/**
 * Generate an ETag from data content using FNV-1a hash
 *
 * This is a fast, non-cryptographic hash suitable for change detection.
 * The resulting ETag includes a timestamp component to ensure uniqueness
 * even for identical content written at different times.
 *
 * @param data - The data to generate an ETag for
 * @returns An ETag string in the format "{hash}-{timestamp}"
 *
 * @example
 * ```typescript
 * const etag = generateEtag(new Uint8Array([1, 2, 3]))
 * // Returns something like "7c9e63b5-lk8p2x"
 * ```
 */
export function generateEtag(data: Uint8Array): string {
  // FNV-1a hash - fast and suitable for change detection
  let hash = 2166136261 // FNV offset basis
  for (let i = 0; i < data.length; i++) {
    hash ^= data[i]!
    hash = (hash * 16777619) >>> 0 // FNV prime, keep as unsigned 32-bit
  }
  // Include timestamp to ensure different ETags for same content at different times
  const timestamp = Date.now().toString(36)
  return `${hash.toString(16)}-${timestamp}`
}

/**
 * Generate a deterministic ETag from data content (no timestamp)
 *
 * Use this when you need the same content to always produce the same ETag.
 * This is useful for caching scenarios where identical content should
 * have matching ETags regardless of when it was written.
 *
 * @param data - The data to generate an ETag for
 * @returns A deterministic ETag string based on content hash and size
 *
 * @example
 * ```typescript
 * const etag1 = generateDeterministicEtag(new Uint8Array([1, 2, 3]))
 * const etag2 = generateDeterministicEtag(new Uint8Array([1, 2, 3]))
 * // etag1 === etag2 (always true for same content)
 * ```
 */
export function generateDeterministicEtag(data: Uint8Array): string {
  // FNV-1a hash
  let hash = 2166136261
  for (let i = 0; i < data.length; i++) {
    hash ^= data[i]!
    hash = (hash * 16777619) >>> 0
  }
  // Include size for additional uniqueness without timestamp
  return `${hash.toString(16)}-${data.length.toString(36)}`
}

/**
 * Normalize a storage path by removing leading slashes and handling edge cases
 *
 * @param path - The path to normalize
 * @returns The normalized path without leading slashes
 *
 * @example
 * ```typescript
 * normalizePath('/foo/bar')  // 'foo/bar'
 * normalizePath('foo/bar')   // 'foo/bar'
 * normalizePath('/')         // ''
 * normalizePath('')          // ''
 * ```
 */
export function normalizePath(path: string): string {
  // Remove leading slash
  if (path.startsWith('/')) {
    path = path.slice(1)
  }
  return path
}

/**
 * Normalize a storage path, also removing trailing slashes
 *
 * Useful for file paths where trailing slashes are not meaningful.
 *
 * @param path - The path to normalize
 * @returns The normalized path without leading or trailing slashes
 *
 * @example
 * ```typescript
 * normalizeFilePath('/foo/bar/')  // 'foo/bar'
 * normalizeFilePath('foo/bar')    // 'foo/bar'
 * normalizeFilePath('/')          // ''
 * ```
 */
export function normalizeFilePath(path: string): string {
  // Remove leading slash
  if (path.startsWith('/')) {
    path = path.slice(1)
  }
  // Remove trailing slash
  if (path.endsWith('/')) {
    path = path.slice(0, -1)
  }
  return path
}
