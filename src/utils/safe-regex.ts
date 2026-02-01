/**
 * Safe regex creation with ReDoS protection
 *
 * This module provides utilities to safely create RegExp objects from user input,
 * protecting against catastrophic backtracking (ReDoS attacks).
 *
 * @module utils/safe-regex
 */

/**
 * Configuration options for regex safety checks
 */
export interface SafeRegexOptions {
  /** Maximum allowed pattern length (default: 1000) */
  maxLength?: number
  /** Maximum allowed quantifier depth (default: 2) */
  maxQuantifierDepth?: number
  /** Allow backreferences like \1, \2 (default: false) */
  allowBackreferences?: boolean
}

const DEFAULT_OPTIONS: Required<SafeRegexOptions> = {
  maxLength: 1000,
  maxQuantifierDepth: 2,
  allowBackreferences: false,
}

/**
 * Error thrown when a regex pattern is considered unsafe
 */
export class UnsafeRegexError extends Error {
  constructor(
    message: string,
    public readonly pattern: string
  ) {
    super(`Unsafe regex pattern: ${message}`)
    Object.setPrototypeOf(this, UnsafeRegexError.prototype)
  }
  override readonly name = 'UnsafeRegexError'
}

/**
 * Patterns that indicate potentially dangerous regex constructs
 *
 * These patterns detect:
 * - Nested quantifiers like (a+)+ or (a*)*
 * - Overlapping alternations with quantifiers like (a|a)+
 * - Multiple adjacent quantifiers
 */
const DANGEROUS_PATTERNS = [
  // Nested quantifiers: (a+)+, (a*)+, (a+)*, etc.
  /\([^)]*[+*]\)[+*?]|\([^)]*[+*]\)\{/,
  // Quantifier on alternation where branches overlap: (a|a)+
  /\([^)]*\|[^)]*\)[+*]/,
  // Repeated groups with internal quantifiers: (.+)+
  /\(\.[+*]\)[+*]/,
  // Adjacent quantifiers on the same construct
  /[+*?]\{|\{[^}]*\}[+*?]/,
  // Backreferences (optional, enabled by default)
  // Exponential backtracking with word boundaries and quantifiers
  /\\b.*[+*].*\\b/,
]

/**
 * Count the depth of nested groups with quantifiers
 */
function countQuantifierDepth(pattern: string): number {
  let maxDepth = 0
  let currentDepth = 0
  let inCharClass = false

  for (let i = 0; i < pattern.length; i++) {
    const char = pattern[i]
    const prevChar = i > 0 ? pattern[i - 1] : ''

    // Skip escaped characters
    if (prevChar === '\\') continue

    // Track character classes - quantifiers inside don't count
    if (char === '[' && prevChar !== '\\') {
      inCharClass = true
      continue
    }
    if (char === ']' && prevChar !== '\\') {
      inCharClass = false
      continue
    }

    if (inCharClass) continue

    // Track group depth
    if (char === '(') {
      currentDepth++
    } else if (char === ')') {
      // Check if this group has a quantifier following it
      const nextChar = pattern[i + 1]
      if (nextChar === '+' || nextChar === '*' || nextChar === '?' || nextChar === '{') {
        maxDepth = Math.max(maxDepth, currentDepth)
      }
      currentDepth = Math.max(0, currentDepth - 1)
    }
  }

  return maxDepth
}

/**
 * Check if a regex pattern contains backreferences
 */
function hasBackreferences(pattern: string): boolean {
  // Match \1 through \9 (and higher) that aren't inside character classes
  // This is a simplified check - we look for \N patterns
  return /\\[1-9]/.test(pattern)
}

/**
 * Validate a regex pattern for safety
 *
 * @param pattern - The regex pattern to validate
 * @param options - Safety check options
 * @throws {UnsafeRegexError} If the pattern is considered unsafe
 */
export function validateRegexPattern(
  pattern: string,
  options: SafeRegexOptions = {}
): void {
  const opts = { ...DEFAULT_OPTIONS, ...options }

  // Check pattern length
  if (pattern.length > opts.maxLength) {
    throw new UnsafeRegexError(
      `Pattern exceeds maximum length of ${opts.maxLength} characters`,
      pattern
    )
  }

  // Check for backreferences if not allowed
  if (!opts.allowBackreferences && hasBackreferences(pattern)) {
    throw new UnsafeRegexError(
      'Backreferences are not allowed',
      pattern
    )
  }

  // Check for dangerous patterns
  for (const dangerousPattern of DANGEROUS_PATTERNS) {
    if (dangerousPattern.test(pattern)) {
      throw new UnsafeRegexError(
        'Pattern contains potentially dangerous constructs (nested quantifiers or overlapping alternations)',
        pattern
      )
    }
  }

  // Check quantifier nesting depth
  const depth = countQuantifierDepth(pattern)
  if (depth > opts.maxQuantifierDepth) {
    throw new UnsafeRegexError(
      `Pattern has quantifier nesting depth of ${depth}, which exceeds maximum of ${opts.maxQuantifierDepth}`,
      pattern
    )
  }
}

/**
 * Create a RegExp safely from a pattern string
 *
 * This function validates the pattern for potential ReDoS vulnerabilities
 * before creating the RegExp object.
 *
 * @param pattern - The regex pattern (string or RegExp)
 * @param flags - Optional regex flags (e.g., 'i', 'g', 'm')
 * @param options - Safety check options
 * @returns A new RegExp object
 * @throws {UnsafeRegexError} If the pattern is considered unsafe
 * @throws {SyntaxError} If the pattern is invalid regex syntax
 *
 * @example
 * ```typescript
 * // Safe pattern
 * const regex = createSafeRegex('^hello', 'i')
 *
 * // Throws UnsafeRegexError
 * createSafeRegex('(a+)+$') // nested quantifiers
 * ```
 */
export function createSafeRegex(
  pattern: string | RegExp,
  flags?: string,
  options?: SafeRegexOptions
): RegExp {
  // If already a RegExp, validate its source
  if (pattern instanceof RegExp) {
    validateRegexPattern(pattern.source, options)
    // Return new RegExp with potentially updated flags
    return new RegExp(pattern.source, flags ?? pattern.flags)
  }

  // Validate the pattern string
  validateRegexPattern(pattern, options)

  // Create and return the RegExp
  return new RegExp(pattern, flags || '')
}

/**
 * Check if a pattern is safe without throwing
 *
 * @param pattern - The regex pattern to check
 * @param options - Safety check options
 * @returns true if the pattern is safe, false otherwise
 */
export function isRegexSafe(
  pattern: string | RegExp,
  options?: SafeRegexOptions
): boolean {
  try {
    const source = pattern instanceof RegExp ? pattern.source : pattern
    validateRegexPattern(source, options)
    return true
  } catch {
    return false
  }
}
