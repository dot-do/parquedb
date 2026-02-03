/**
 * Safe regex creation with ReDoS protection
 *
 * This module provides utilities to safely create RegExp objects from user input,
 * protecting against catastrophic backtracking (ReDoS attacks).
 *
 * ## Protection Mechanisms
 *
 * 1. **Pattern-based detection**: Identifies known dangerous constructs like nested
 *    quantifiers, overlapping alternations, and exponential backtracking patterns.
 *
 * 2. **Structural analysis**: Analyzes group nesting depth, repetition count, and
 *    character class complexity to detect deeply nested structures.
 *
 * 3. **Star height analysis**: Computes the "star height" (nesting level of Kleene
 *    stars and plus operators) to detect patterns that can cause exponential behavior.
 *
 * 4. **Length limits**: Enforces maximum pattern length to prevent memory exhaustion.
 *
 * 5. **Complexity scoring**: Computes an overall complexity score based on multiple
 *    factors to catch patterns that may evade individual checks.
 *
 * 6. **Execution timeout**: Optional timeout wrapper for regex execution to protect
 *    against patterns that slip through static analysis.
 *
 * @module utils/safe-regex
 */

import { logger } from './logger'

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
  /** Maximum star height (nested repetitions) allowed (default: 2) */
  maxStarHeight?: number
  /** Maximum group nesting depth (default: 10) */
  maxGroupDepth?: number
  /** Maximum total repetitions in pattern (default: 20) */
  maxRepetitions?: number
  /** Maximum complexity score allowed (default: 25) */
  maxComplexityScore?: number
  /** Maximum character class size (default: 100) */
  maxCharClassSize?: number
  /** Maximum alternation branches (default: 15) */
  maxAlternationBranches?: number
}

const DEFAULT_OPTIONS: Required<SafeRegexOptions> = {
  maxLength: 1000,
  maxQuantifierDepth: 2,
  allowBackreferences: false,
  maxStarHeight: 2,
  maxGroupDepth: 10,
  maxRepetitions: 20,
  maxComplexityScore: 25,
  maxCharClassSize: 100,
  maxAlternationBranches: 15,
}

/**
 * Result of complexity analysis
 */
export interface ComplexityAnalysis {
  /** Overall complexity score (higher = more dangerous) */
  score: number
  /** Breakdown of individual factors */
  factors: {
    starHeight: number
    groupDepth: number
    quantifierDepth: number
    repetitionCount: number
    alternationCount: number
    charClassComplexity: number
    patternLength: number
    nestedQuantifiers: number
    overlappingRanges: number
  }
  /** Whether the pattern is considered safe */
  isSafe: boolean
  /** Warnings about potentially problematic constructs */
  warnings: string[]
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
 * - Deeply nested groups with repetition
 * - Polynomial and exponential backtracking patterns
 */
const DANGEROUS_PATTERNS: Array<{ pattern: RegExp; description: string }> = [
  // Nested quantifiers: (a+)+, (a*)+, (a+)*, etc.
  { pattern: /\([^)]*[+*]\)[+*?]|\([^)]*[+*]\)\{/, description: 'nested quantifiers' },
  // Quantifier on alternation where branches overlap: (a|a)+
  { pattern: /\([^)]*\|[^)]*\)[+*]/, description: 'quantified alternation' },
  // Repeated groups with internal quantifiers: (.+)+
  { pattern: /\(\.[+*]\)[+*]/, description: 'repeated wildcard group' },
  // Adjacent quantifiers on the same construct
  { pattern: /[+*?]\{|\{[^}]*\}[+*?]/, description: 'adjacent quantifiers' },
  // Exponential backtracking with word boundaries and quantifiers
  { pattern: /\\b.*[+*].*\\b/, description: 'word boundary with quantifiers' },
  // Overlapping character classes with quantifiers: [a-z]*[a-z]+
  { pattern: /\[[^\]]+\][*+]\[[^\]]+\][*+]/, description: 'overlapping character classes' },
  // Quantifier after optional group: (a?)+
  { pattern: /\([^)]*\?\)[+*]/, description: 'quantified optional group' },
  // Empty group with quantifier: ()+
  { pattern: /\(\)[+*?]/, description: 'empty quantified group' },
  // Recursive-like patterns: .*.*
  { pattern: /\.\*\.\*/, description: 'consecutive wildcards' },
  // Polynomial backtracking: .*a.*a.*a
  { pattern: /\.\*[^.]\.\*[^.]\.\*/, description: 'polynomial backtracking' },
  // Large unbounded repetition: {100,} or {1000,}
  { pattern: /\{(\d{3,}),?\}/, description: 'excessive repetition count' },
  // Nested groups with alternation and quantifier: ((a|b)+)+
  { pattern: /\(\([^)]*\|[^)]*\)[+*]\)[+*]/, description: 'deeply nested alternation' },
  // Catastrophic backtracking with $ anchor: .*$
  { pattern: /\.\+[^$]*\$|\.\*[^$]+\$/, description: 'greedy quantifier before anchor' },
  // Multiple adjacent wildcards with quantifiers
  { pattern: /\.[+*]\.[+*]/, description: 'adjacent wildcard quantifiers' },
  // Lookahead/lookbehind with quantifiers inside
  { pattern: /\(\?[=!<][^)]*[+*][^)]*\)[+*]/, description: 'quantified lookaround' },
  // Possessive-like patterns that can still backtrack
  { pattern: /\[[^\]]*-[^\]]*-[^\]]*\][+*]/, description: 'complex character class with quantifier' },
  // Word character class repeated multiple times
  { pattern: /\\w[+*]\\w[+*]\\w[+*]/, description: 'repeated word class patterns' },
  // Digit class repeated multiple times with wildcards
  { pattern: /\\d[+*].*\\d[+*]/, description: 'digit patterns with wildcards' },
]

/**
 * Additional patterns for detecting subtle ReDoS vulnerabilities
 */
const SUBTLE_DANGEROUS_PATTERNS: Array<{ pattern: RegExp; description: string; scoreImpact: number }> = [
  // Overlapping alternation at start/end
  { pattern: /^\^?\([^)]*\|[^)]*\)[+*]/, description: 'anchored alternation with quantifier', scoreImpact: 3 },
  // Nested optional groups
  { pattern: /\(\([^)]*\)?\)[+*]/, description: 'nested optional groups', scoreImpact: 2 },
  // Character class with quantifier followed by same class
  { pattern: /\[([a-z])-([a-z])\][+*].*\[\1-\2\]/, description: 'repeated identical ranges', scoreImpact: 2 },
  // Multiple capture groups with quantifiers
  { pattern: /\([^)]+\)[+*].*\([^)]+\)[+*].*\([^)]+\)[+*]/, description: 'multiple quantified groups', scoreImpact: 2 },
  // Alternation with overlapping prefixes
  { pattern: /\((\w+)\|(\1\w+)\)[+*]/, description: 'overlapping prefix alternation', scoreImpact: 4 },
  // Lazy quantifier followed by greedy
  { pattern: /[+*]\?.*[+*][^?]/, description: 'mixed lazy and greedy quantifiers', scoreImpact: 1 },
  // Unbounded repetition in alternation
  { pattern: /\([^|)]*[+*][^|)]*\|[^|)]*[+*][^|)]*\)[+*]/, description: 'unbounded alternation branches', scoreImpact: 3 },
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
 * Calculate the "star height" of a regex pattern.
 * Star height is the maximum nesting level of Kleene stars (* and +).
 * Higher star heights indicate patterns that can cause exponential backtracking.
 *
 * For example:
 * - "a*" has star height 1
 * - "(a*)*" has star height 2
 * - "((a*)*)*" has star height 3
 */
function calculateStarHeight(pattern: string): number {
  let maxHeight = 0
  let currentHeight = 0
  let inCharClass = false
  const groupStack: number[] = [] // Track star height at each group level

  for (let i = 0; i < pattern.length; i++) {
    const char = pattern[i]
    const prevChar = i > 0 ? pattern[i - 1] : ''

    // Skip escaped characters
    if (prevChar === '\\') continue

    // Track character classes
    if (char === '[' && prevChar !== '\\') {
      inCharClass = true
      continue
    }
    if (char === ']' && prevChar !== '\\') {
      inCharClass = false
      continue
    }

    if (inCharClass) continue

    // Track group nesting
    if (char === '(') {
      groupStack.push(currentHeight)
    } else if (char === ')') {
      const groupHeight = groupStack.pop() ?? 0
      // Check if this group has a quantifier following it
      const nextChar = pattern[i + 1]
      if (nextChar === '+' || nextChar === '*') {
        // If the group contains a star/plus, this is nested
        if (currentHeight > groupHeight) {
          currentHeight = currentHeight + 1
        } else {
          currentHeight = groupHeight + 1
        }
        maxHeight = Math.max(maxHeight, currentHeight)
      } else {
        currentHeight = groupHeight
      }
    } else if (char === '*' || char === '+') {
      // Direct quantifier (not on a group)
      currentHeight = Math.max(currentHeight, 1)
      maxHeight = Math.max(maxHeight, currentHeight)
    }
  }

  return maxHeight
}

/**
 * Count the maximum group nesting depth (not just quantified groups)
 */
function countGroupDepth(pattern: string): number {
  let maxDepth = 0
  let currentDepth = 0
  let inCharClass = false

  for (let i = 0; i < pattern.length; i++) {
    const char = pattern[i]
    const prevChar = i > 0 ? pattern[i - 1] : ''

    // Skip escaped characters
    if (prevChar === '\\') continue

    // Track character classes
    if (char === '[' && prevChar !== '\\') {
      inCharClass = true
      continue
    }
    if (char === ']' && prevChar !== '\\') {
      inCharClass = false
      continue
    }

    if (inCharClass) continue

    if (char === '(') {
      currentDepth++
      maxDepth = Math.max(maxDepth, currentDepth)
    } else if (char === ')') {
      currentDepth = Math.max(0, currentDepth - 1)
    }
  }

  return maxDepth
}

/**
 * Count total repetition operators in the pattern
 */
function countRepetitions(pattern: string): number {
  let count = 0
  let inCharClass = false

  for (let i = 0; i < pattern.length; i++) {
    const char = pattern[i]
    const prevChar = i > 0 ? pattern[i - 1] : ''

    // Skip escaped characters
    if (prevChar === '\\') continue

    // Track character classes
    if (char === '[' && prevChar !== '\\') {
      inCharClass = true
      continue
    }
    if (char === ']' && prevChar !== '\\') {
      inCharClass = false
      continue
    }

    if (inCharClass) continue

    if (char === '*' || char === '+' || char === '?' || char === '{') {
      count++
    }
  }

  return count
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
 * Count alternation branches in the pattern
 */
function countAlternationBranches(pattern: string): number {
  let maxBranches = 0
  let currentBranches = 1
  let depth = 0
  let inCharClass = false
  let branchesAtDepth: number[] = [1]

  for (let i = 0; i < pattern.length; i++) {
    const char = pattern[i]
    const prevChar = i > 0 ? pattern[i - 1] : ''

    if (prevChar === '\\') continue

    if (char === '[' && prevChar !== '\\') {
      inCharClass = true
      continue
    }
    if (char === ']' && prevChar !== '\\') {
      inCharClass = false
      continue
    }

    if (inCharClass) continue

    if (char === '(') {
      depth++
      branchesAtDepth[depth] = 1
    } else if (char === ')') {
      maxBranches = Math.max(maxBranches, branchesAtDepth[depth] || 1)
      depth = Math.max(0, depth - 1)
    } else if (char === '|') {
      branchesAtDepth[depth] = (branchesAtDepth[depth] || 1) + 1
      currentBranches = branchesAtDepth[depth]
    }
  }

  return Math.max(maxBranches, currentBranches)
}

/**
 * Analyze character class complexity
 */
function analyzeCharClassComplexity(pattern: string): { maxSize: number; totalClasses: number; hasOverlapping: boolean } {
  let maxSize = 0
  let totalClasses = 0
  let inCharClass = false
  let currentClassSize = 0
  let currentClassContent = ''
  const classContents: string[] = []

  for (let i = 0; i < pattern.length; i++) {
    const char = pattern[i]
    const prevChar = i > 0 ? pattern[i - 1] : ''

    if (prevChar === '\\') {
      if (inCharClass) {
        currentClassSize++
        currentClassContent += prevChar + char
      }
      continue
    }

    if (char === '[' && !inCharClass) {
      inCharClass = true
      totalClasses++
      currentClassSize = 0
      currentClassContent = ''
      continue
    }

    if (char === ']' && inCharClass) {
      inCharClass = false
      maxSize = Math.max(maxSize, currentClassSize)
      classContents.push(currentClassContent)
      continue
    }

    if (inCharClass) {
      currentClassSize++
      currentClassContent += char
    }
  }

  // Check for overlapping character classes
  let hasOverlapping = false
  for (let i = 0; i < classContents.length - 1; i++) {
    for (let j = i + 1; j < classContents.length; j++) {
      if (classContents[i] === classContents[j] && classContents[i].length > 0) {
        hasOverlapping = true
        break
      }
    }
    if (hasOverlapping) break
  }

  return { maxSize, totalClasses, hasOverlapping }
}

/**
 * Detect deeply nested structures that could cause exponential backtracking
 */
function detectDeeplyNestedStructures(pattern: string): { depth: number; hasQuantifiedNesting: boolean; nestedQuantifierCount: number } {
  let maxDepth = 0
  let currentDepth = 0
  let hasQuantifiedNesting = false
  let nestedQuantifierCount = 0
  let inCharClass = false
  const quantifiedDepths: number[] = []

  for (let i = 0; i < pattern.length; i++) {
    const char = pattern[i]
    const prevChar = i > 0 ? pattern[i - 1] : ''
    const nextChar = pattern[i + 1]

    if (prevChar === '\\') continue

    if (char === '[' && prevChar !== '\\') {
      inCharClass = true
      continue
    }
    if (char === ']' && prevChar !== '\\') {
      inCharClass = false
      continue
    }

    if (inCharClass) continue

    if (char === '(') {
      currentDepth++
      maxDepth = Math.max(maxDepth, currentDepth)
    } else if (char === ')') {
      // Check if this group is quantified
      if (nextChar === '+' || nextChar === '*' || nextChar === '?' || nextChar === '{') {
        quantifiedDepths.push(currentDepth)
        // Check for nested quantified groups
        for (const qd of quantifiedDepths) {
          if (qd < currentDepth) {
            hasQuantifiedNesting = true
            nestedQuantifierCount++
          }
        }
      }
      currentDepth = Math.max(0, currentDepth - 1)
    }
  }

  return { depth: maxDepth, hasQuantifiedNesting, nestedQuantifierCount }
}

/**
 * Calculate the complexity score for a regex pattern
 * Higher scores indicate more dangerous patterns
 */
export function calculateComplexityScore(pattern: string, options: SafeRegexOptions = {}): ComplexityAnalysis {
  const opts = { ...DEFAULT_OPTIONS, ...options }
  const warnings: string[] = []

  // Calculate individual factors
  const starHeight = calculateStarHeight(pattern)
  const groupDepth = countGroupDepth(pattern)
  const quantifierDepth = countQuantifierDepth(pattern)
  const repetitionCount = countRepetitions(pattern)
  const alternationCount = countAlternationBranches(pattern)
  const charClassInfo = analyzeCharClassComplexity(pattern)
  const nestingInfo = detectDeeplyNestedStructures(pattern)

  // Score each factor (weighted)
  const factors = {
    starHeight: starHeight * 4, // High weight - star height is very dangerous
    groupDepth: Math.max(0, groupDepth - 3) * 1.5, // Penalize deep nesting over 3
    quantifierDepth: quantifierDepth * 3, // High weight
    repetitionCount: Math.max(0, repetitionCount - 5) * 0.5, // Mild penalty for many repetitions
    alternationCount: Math.max(0, alternationCount - 3) * 1, // Penalty for many branches
    charClassComplexity: charClassInfo.hasOverlapping ? 3 : 0, // Overlapping classes are risky
    patternLength: Math.max(0, pattern.length - 200) * 0.02, // Mild penalty for long patterns
    nestedQuantifiers: nestingInfo.nestedQuantifierCount * 5, // Very high weight
    overlappingRanges: charClassInfo.hasOverlapping ? 2 : 0,
  }

  // Check subtle dangerous patterns and add to score
  let subtlePatternScore = 0
  for (const { pattern: dp, description, scoreImpact } of SUBTLE_DANGEROUS_PATTERNS) {
    if (dp.test(pattern)) {
      subtlePatternScore += scoreImpact
      warnings.push(`Detected: ${description}`)
    }
  }

  // Calculate total score
  let score = Object.values(factors).reduce((a, b) => a + b, 0) + subtlePatternScore

  // Add warnings for specific issues
  if (starHeight > 1) warnings.push(`Star height of ${starHeight} may cause exponential backtracking`)
  if (quantifierDepth > 1) warnings.push(`Quantifier depth of ${quantifierDepth} is potentially dangerous`)
  if (nestingInfo.hasQuantifiedNesting) warnings.push('Contains nested quantified groups')
  if (alternationCount > 5) warnings.push(`High alternation count (${alternationCount}) may impact performance`)
  if (charClassInfo.maxSize > 50) warnings.push(`Large character class (${charClassInfo.maxSize} chars)`)

  return {
    score,
    factors,
    isSafe: score <= opts.maxComplexityScore,
    warnings,
  }
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
  for (const { pattern: dangerousPattern, description } of DANGEROUS_PATTERNS) {
    if (dangerousPattern.test(pattern)) {
      throw new UnsafeRegexError(
        `Pattern contains dangerous construct: ${description}`,
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

  // Check star height (nesting of repetition operators)
  const starHeight = calculateStarHeight(pattern)
  if (starHeight > opts.maxStarHeight) {
    throw new UnsafeRegexError(
      `Pattern has star height of ${starHeight}, which exceeds maximum of ${opts.maxStarHeight}`,
      pattern
    )
  }

  // Check maximum group depth
  const groupDepth = countGroupDepth(pattern)
  if (groupDepth > opts.maxGroupDepth) {
    throw new UnsafeRegexError(
      `Pattern has group nesting depth of ${groupDepth}, which exceeds maximum of ${opts.maxGroupDepth}`,
      pattern
    )
  }

  // Check total repetition count
  const repetitions = countRepetitions(pattern)
  if (repetitions > opts.maxRepetitions) {
    throw new UnsafeRegexError(
      `Pattern has ${repetitions} repetition operators, which exceeds maximum of ${opts.maxRepetitions}`,
      pattern
    )
  }

  // Check alternation branches
  const alternationBranches = countAlternationBranches(pattern)
  if (alternationBranches > opts.maxAlternationBranches) {
    throw new UnsafeRegexError(
      `Pattern has ${alternationBranches} alternation branches, which exceeds maximum of ${opts.maxAlternationBranches}`,
      pattern
    )
  }

  // Check character class size
  const charClassInfo = analyzeCharClassComplexity(pattern)
  if (charClassInfo.maxSize > opts.maxCharClassSize) {
    throw new UnsafeRegexError(
      `Pattern has character class with ${charClassInfo.maxSize} characters, which exceeds maximum of ${opts.maxCharClassSize}`,
      pattern
    )
  }

  // Check deeply nested structures
  const nestingInfo = detectDeeplyNestedStructures(pattern)
  if (nestingInfo.hasQuantifiedNesting && nestingInfo.nestedQuantifierCount > 1) {
    throw new UnsafeRegexError(
      `Pattern has ${nestingInfo.nestedQuantifierCount} nested quantified groups, which is potentially dangerous`,
      pattern
    )
  }

  // Check overall complexity score
  const complexity = calculateComplexityScore(pattern, options)
  if (!complexity.isSafe) {
    throw new UnsafeRegexError(
      `Pattern complexity score of ${complexity.score.toFixed(1)} exceeds maximum of ${opts.maxComplexityScore}`,
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
    // Intentionally ignored: validation failure means the regex pattern is unsafe
    return false
  }
}

/**
 * Error thrown when regex execution times out
 */
export class RegexTimeoutError extends Error {
  constructor(
    public readonly pattern: string,
    public readonly timeoutMs: number
  ) {
    super(`Regex execution timed out after ${timeoutMs}ms`)
    Object.setPrototypeOf(this, RegexTimeoutError.prototype)
  }
  override readonly name = 'RegexTimeoutError'
}

/**
 * Options for regex execution with timeout
 */
export interface RegexExecOptions {
  /** Timeout in milliseconds (default: 1000) */
  timeoutMs?: number
  /** Maximum input length to process (default: 100000) */
  maxInputLength?: number
}

const DEFAULT_EXEC_OPTIONS: Required<RegexExecOptions> = {
  timeoutMs: 1000,
  maxInputLength: 100000,
}

/**
 * Execute a regex test with timeout protection
 *
 * This function provides runtime protection against ReDoS attacks by
 * limiting execution time. It's useful as a defense-in-depth measure
 * for patterns that pass static analysis but may still be vulnerable.
 *
 * Note: In JavaScript, we cannot truly interrupt regex execution.
 * This implementation uses a pre-check heuristic based on input length
 * and pattern complexity to estimate potential execution time.
 *
 * @param regex - The RegExp to execute
 * @param input - The string to test against
 * @param options - Execution options
 * @returns The test result (true/false)
 * @throws {RegexTimeoutError} If execution is estimated to exceed timeout
 * @throws {Error} If input exceeds maximum length
 *
 * @example
 * ```typescript
 * const regex = createSafeRegex('^hello')
 * const result = safeRegexTest(regex, 'hello world') // true
 * ```
 */
export function safeRegexTest(
  regex: RegExp,
  input: string,
  options: RegexExecOptions = {}
): boolean {
  const opts = { ...DEFAULT_EXEC_OPTIONS, ...options }

  // Check input length
  if (input.length > opts.maxInputLength) {
    throw new Error(
      `Input length ${input.length} exceeds maximum of ${opts.maxInputLength}`
    )
  }

  // Estimate potential execution time based on pattern complexity and input length
  // This is a heuristic - true timeout would require worker threads
  const complexity = calculateComplexityScore(regex.source)
  const estimatedComplexity = complexity.score * Math.log2(input.length + 1)

  // If estimated complexity is very high, reject preemptively
  if (estimatedComplexity > 100) {
    throw new RegexTimeoutError(regex.source, opts.timeoutMs)
  }

  // For potentially dangerous patterns on long inputs, use chunked testing
  if (complexity.score > 10 && input.length > 1000) {
    return safeChunkedTest(regex, input, opts)
  }

  // Execute the regex
  const startTime = performance.now()
  const result = regex.test(input)
  const elapsed = performance.now() - startTime

  // Log warning if execution took longer than expected
  if (elapsed > opts.timeoutMs * 0.5) {
    logger.warn(
      `Regex execution took ${elapsed.toFixed(0)}ms (pattern: ${regex.source.slice(0, 50)}...)`
    )
  }

  return result
}

/**
 * Execute a regex match with timeout protection
 *
 * @param regex - The RegExp to execute
 * @param input - The string to match against
 * @param options - Execution options
 * @returns The match result or null
 * @throws {RegexTimeoutError} If execution is estimated to exceed timeout
 */
export function safeRegexMatch(
  regex: RegExp,
  input: string,
  options: RegexExecOptions = {}
): RegExpMatchArray | null {
  const opts = { ...DEFAULT_EXEC_OPTIONS, ...options }

  if (input.length > opts.maxInputLength) {
    throw new Error(
      `Input length ${input.length} exceeds maximum of ${opts.maxInputLength}`
    )
  }

  const complexity = calculateComplexityScore(regex.source)
  const estimatedComplexity = complexity.score * Math.log2(input.length + 1)

  if (estimatedComplexity > 100) {
    throw new RegexTimeoutError(regex.source, opts.timeoutMs)
  }

  return input.match(regex)
}

/**
 * Execute a regex exec with timeout protection
 *
 * @param regex - The RegExp to execute
 * @param input - The string to exec against
 * @param options - Execution options
 * @returns The exec result or null
 * @throws {RegexTimeoutError} If execution is estimated to exceed timeout
 */
export function safeRegexExec(
  regex: RegExp,
  input: string,
  options: RegexExecOptions = {}
): RegExpExecArray | null {
  const opts = { ...DEFAULT_EXEC_OPTIONS, ...options }

  if (input.length > opts.maxInputLength) {
    throw new Error(
      `Input length ${input.length} exceeds maximum of ${opts.maxInputLength}`
    )
  }

  const complexity = calculateComplexityScore(regex.source)
  const estimatedComplexity = complexity.score * Math.log2(input.length + 1)

  if (estimatedComplexity > 100) {
    throw new RegexTimeoutError(regex.source, opts.timeoutMs)
  }

  return regex.exec(input)
}

/**
 * Test regex against input in chunks to prevent long-running operations
 * This is a mitigation strategy, not a true timeout
 */
function safeChunkedTest(
  regex: RegExp,
  input: string,
  options: Required<RegexExecOptions>
): boolean {
  const chunkSize = 1000
  const startTime = performance.now()

  // For anchored patterns, we can't chunk
  if (regex.source.startsWith('^') || regex.source.endsWith('$')) {
    // Fall back to full test with time tracking
    const result = regex.test(input)
    const elapsed = performance.now() - startTime
    if (elapsed > options.timeoutMs) {
      logger.warn(`Regex exceeded soft timeout: ${elapsed.toFixed(0)}ms`)
    }
    return result
  }

  // Test in chunks with overlap
  const overlap = 100
  for (let i = 0; i < input.length; i += chunkSize - overlap) {
    const chunk = input.slice(i, i + chunkSize)
    if (regex.test(chunk)) {
      return true
    }

    // Check elapsed time
    const elapsed = performance.now() - startTime
    if (elapsed > options.timeoutMs) {
      throw new RegexTimeoutError(regex.source, options.timeoutMs)
    }
  }

  return false
}

/**
 * Analyze a pattern and return detailed safety information
 *
 * This is useful for debugging and understanding why a pattern was rejected.
 *
 * @param pattern - The regex pattern to analyze
 * @param options - Safety check options
 * @returns Detailed analysis including complexity breakdown and recommendations
 */
export function analyzeRegexSafety(
  pattern: string,
  options: SafeRegexOptions = {}
): {
  isSafe: boolean
  complexity: ComplexityAnalysis
  violations: string[]
  recommendations: string[]
} {
  const opts = { ...DEFAULT_OPTIONS, ...options }
  const violations: string[] = []
  const recommendations: string[] = []

  // Check all conditions and collect violations
  if (pattern.length > opts.maxLength) {
    violations.push(`Pattern length (${pattern.length}) exceeds maximum (${opts.maxLength})`)
    recommendations.push('Simplify the pattern or split into multiple patterns')
  }

  if (!opts.allowBackreferences && hasBackreferences(pattern)) {
    violations.push('Pattern contains backreferences')
    recommendations.push('Remove backreferences or enable allowBackreferences option')
  }

  for (const { pattern: dp, description } of DANGEROUS_PATTERNS) {
    if (dp.test(pattern)) {
      violations.push(`Contains dangerous construct: ${description}`)
    }
  }

  const depth = countQuantifierDepth(pattern)
  if (depth > opts.maxQuantifierDepth) {
    violations.push(`Quantifier depth (${depth}) exceeds maximum (${opts.maxQuantifierDepth})`)
    recommendations.push('Reduce nesting of quantified groups')
  }

  const starHeight = calculateStarHeight(pattern)
  if (starHeight > opts.maxStarHeight) {
    violations.push(`Star height (${starHeight}) exceeds maximum (${opts.maxStarHeight})`)
    recommendations.push('Avoid nested repetition operators like (a+)+')
  }

  const groupDepth = countGroupDepth(pattern)
  if (groupDepth > opts.maxGroupDepth) {
    violations.push(`Group depth (${groupDepth}) exceeds maximum (${opts.maxGroupDepth})`)
    recommendations.push('Flatten nested groups where possible')
  }

  const repetitions = countRepetitions(pattern)
  if (repetitions > opts.maxRepetitions) {
    violations.push(`Repetition count (${repetitions}) exceeds maximum (${opts.maxRepetitions})`)
    recommendations.push('Reduce the number of quantifiers in the pattern')
  }

  const alternationBranches = countAlternationBranches(pattern)
  if (alternationBranches > opts.maxAlternationBranches) {
    violations.push(`Alternation branches (${alternationBranches}) exceeds maximum (${opts.maxAlternationBranches})`)
    recommendations.push('Reduce alternation branches or split into multiple patterns')
  }

  const complexity = calculateComplexityScore(pattern, options)
  if (!complexity.isSafe) {
    violations.push(`Complexity score (${complexity.score.toFixed(1)}) exceeds maximum (${opts.maxComplexityScore})`)
    recommendations.push('Simplify the pattern to reduce overall complexity')
  }

  // Add complexity-based recommendations
  if (complexity.factors.starHeight > 4) {
    recommendations.push('Use atomic groups or possessive quantifiers if available')
  }
  if (complexity.factors.nestedQuantifiers > 0) {
    recommendations.push('Avoid patterns like (a+)+ which cause exponential backtracking')
  }

  return {
    isSafe: violations.length === 0,
    complexity,
    violations,
    recommendations: Array.from(new Set(recommendations)), // Deduplicate
  }
}
