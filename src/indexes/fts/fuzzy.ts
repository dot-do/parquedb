/**
 * Fuzzy Matching for Full-Text Search
 *
 * Provides typo tolerance using Levenshtein distance (edit distance).
 * Supports configurable max distance, prefix matching, and minimum term length.
 */

import {
  DEFAULT_FTS_FUZZY_MAX_DISTANCE,
  DEFAULT_FTS_FUZZY_MIN_TERM_LENGTH,
  DEFAULT_FTS_FUZZY_PREFIX_LENGTH,
} from '../../constants'
import type { FTSFuzzyOptions } from '../types'

// =============================================================================
// Types
// =============================================================================

/**
 * Fuzzy match result
 */
export interface FuzzyMatch {
  /** The matched term from the vocabulary */
  term: string
  /** Edit distance from the query term */
  distance: number
}

/**
 * Normalized fuzzy options with defaults applied
 */
export interface NormalizedFuzzyOptions {
  enabled: boolean
  maxDistance: number
  minTermLength: number
  prefixLength: number
}

// =============================================================================
// Option Normalization
// =============================================================================

/**
 * Normalize fuzzy options, applying defaults
 *
 * @param options - Fuzzy options (boolean or object)
 * @returns Normalized options with all defaults applied
 */
export function normalizeFuzzyOptions(
  options: FTSFuzzyOptions | boolean | undefined
): NormalizedFuzzyOptions {
  if (options === undefined || options === false) {
    return {
      enabled: false,
      maxDistance: DEFAULT_FTS_FUZZY_MAX_DISTANCE,
      minTermLength: DEFAULT_FTS_FUZZY_MIN_TERM_LENGTH,
      prefixLength: DEFAULT_FTS_FUZZY_PREFIX_LENGTH,
    }
  }

  if (options === true) {
    return {
      enabled: true,
      maxDistance: DEFAULT_FTS_FUZZY_MAX_DISTANCE,
      minTermLength: DEFAULT_FTS_FUZZY_MIN_TERM_LENGTH,
      prefixLength: DEFAULT_FTS_FUZZY_PREFIX_LENGTH,
    }
  }

  return {
    enabled: options.enabled ?? true,
    maxDistance: options.maxDistance ?? DEFAULT_FTS_FUZZY_MAX_DISTANCE,
    minTermLength: options.minTermLength ?? DEFAULT_FTS_FUZZY_MIN_TERM_LENGTH,
    prefixLength: options.prefixLength ?? DEFAULT_FTS_FUZZY_PREFIX_LENGTH,
  }
}

// =============================================================================
// Levenshtein Distance
// =============================================================================

/**
 * Calculate Levenshtein distance between two strings
 *
 * Uses Wagner-Fischer algorithm with O(min(m,n)) space complexity.
 *
 * @param a - First string
 * @param b - Second string
 * @returns Edit distance (number of insertions, deletions, or substitutions)
 */
export function levenshteinDistance(a: string, b: string): number {
  // Ensure a is the shorter string for space optimization
  if (a.length > b.length) {
    ;[a, b] = [b, a]
  }

  const m = a.length
  const n = b.length

  // Base cases
  if (m === 0) return n
  if (n === 0) return m

  // Use single row for space optimization
  let prevRow = new Array<number>(m + 1)
  let currRow = new Array<number>(m + 1)

  // Initialize first row
  for (let i = 0; i <= m; i++) {
    prevRow[i] = i
  }

  // Fill in the rest of the matrix
  for (let j = 1; j <= n; j++) {
    currRow[0] = j

    for (let i = 1; i <= m; i++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1
      currRow[i] = Math.min(
        prevRow[i]! + 1,      // deletion
        currRow[i - 1]! + 1,  // insertion
        prevRow[i - 1]! + cost // substitution
      )
    }

    // Swap rows
    ;[prevRow, currRow] = [currRow, prevRow]
  }

  return prevRow[m]!
}

/**
 * Calculate Levenshtein distance with early termination
 *
 * More efficient when we only care if distance is within a threshold.
 *
 * @param a - First string
 * @param b - Second string
 * @param maxDistance - Maximum distance to compute
 * @returns Edit distance, or maxDistance + 1 if exceeds threshold
 */
export function levenshteinDistanceBounded(
  a: string,
  b: string,
  maxDistance: number
): number {
  // Quick length check - if length difference exceeds max, can't match
  const lenDiff = Math.abs(a.length - b.length)
  if (lenDiff > maxDistance) {
    return maxDistance + 1
  }

  // Ensure a is the shorter string
  if (a.length > b.length) {
    ;[a, b] = [b, a]
  }

  const m = a.length
  const n = b.length

  // Base cases
  if (m === 0) return Math.min(n, maxDistance + 1)
  if (n === 0) return Math.min(m, maxDistance + 1)

  // Use single row with bounded computation
  let prevRow = new Array<number>(m + 1)
  let currRow = new Array<number>(m + 1)

  // Initialize first row
  for (let i = 0; i <= m; i++) {
    prevRow[i] = i
  }

  // Fill in the matrix with early termination
  for (let j = 1; j <= n; j++) {
    currRow[0] = j
    let minInRow = j

    // Only compute values within the band
    const start = Math.max(1, j - maxDistance)
    const end = Math.min(m, j + maxDistance)

    // Set out-of-band values to max + 1
    if (start > 1) {
      currRow[start - 1] = maxDistance + 1
    }

    for (let i = start; i <= end; i++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1
      currRow[i] = Math.min(
        (prevRow[i] ?? maxDistance + 1) + 1,
        (currRow[i - 1] ?? maxDistance + 1) + 1,
        (prevRow[i - 1] ?? maxDistance + 1) + cost
      )
      minInRow = Math.min(minInRow, currRow[i]!)
    }

    // Early termination if all values in row exceed threshold
    if (minInRow > maxDistance) {
      return maxDistance + 1
    }

    // Swap rows
    ;[prevRow, currRow] = [currRow, prevRow]
  }

  return Math.min(prevRow[m]!, maxDistance + 1)
}

// =============================================================================
// Damerau-Levenshtein Distance
// =============================================================================

/**
 * Calculate Damerau-Levenshtein distance between two strings
 *
 * Includes transposition as a single edit operation (e.g., "ab" -> "ba" = 1).
 * More appropriate for typo detection since transpositions are common typos.
 *
 * @param a - First string
 * @param b - Second string
 * @returns Edit distance (insertions, deletions, substitutions, or transpositions)
 */
export function damerauLevenshteinDistance(a: string, b: string): number {
  const m = a.length
  const n = b.length

  if (m === 0) return n
  if (n === 0) return m

  // Create matrix with extra row/column for initial values
  const d: number[][] = Array.from({ length: m + 1 }, () =>
    new Array<number>(n + 1).fill(0)
  )

  // Initialize first row and column
  for (let i = 0; i <= m; i++) d[i]![0] = i
  for (let j = 0; j <= n; j++) d[0]![j] = j

  // Fill in the matrix
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1

      d[i]![j] = Math.min(
        d[i - 1]![j]! + 1,      // deletion
        d[i]![j - 1]! + 1,      // insertion
        d[i - 1]![j - 1]! + cost // substitution
      )

      // Check for transposition
      if (
        i > 1 &&
        j > 1 &&
        a[i - 1] === b[j - 2] &&
        a[i - 2] === b[j - 1]
      ) {
        d[i]![j] = Math.min(d[i]![j]!, d[i - 2]![j - 2]! + cost)
      }
    }
  }

  return d[m]![n]!
}

/**
 * Calculate Damerau-Levenshtein distance with early termination
 *
 * @param a - First string
 * @param b - Second string
 * @param maxDistance - Maximum distance to compute
 * @returns Edit distance, or maxDistance + 1 if exceeds threshold
 */
export function damerauLevenshteinDistanceBounded(
  a: string,
  b: string,
  maxDistance: number
): number {
  // Quick length check
  const lenDiff = Math.abs(a.length - b.length)
  if (lenDiff > maxDistance) {
    return maxDistance + 1
  }

  const m = a.length
  const n = b.length

  if (m === 0) return Math.min(n, maxDistance + 1)
  if (n === 0) return Math.min(m, maxDistance + 1)

  // For bounded computation, use the basic algorithm but check min in each row
  const d: number[][] = Array.from({ length: m + 1 }, () =>
    new Array<number>(n + 1).fill(maxDistance + 1)
  )

  for (let i = 0; i <= Math.min(m, maxDistance); i++) d[i]![0] = i
  for (let j = 0; j <= Math.min(n, maxDistance); j++) d[0]![j] = j

  for (let i = 1; i <= m; i++) {
    let minInRow = maxDistance + 1

    const start = Math.max(1, i - maxDistance)
    const end = Math.min(n, i + maxDistance)

    for (let j = start; j <= end; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1

      d[i]![j] = Math.min(
        d[i - 1]![j]! + 1,
        d[i]![j - 1]! + 1,
        d[i - 1]![j - 1]! + cost
      )

      // Transposition
      if (
        i > 1 &&
        j > 1 &&
        a[i - 1] === b[j - 2] &&
        a[i - 2] === b[j - 1]
      ) {
        d[i]![j] = Math.min(d[i]![j]!, d[i - 2]![j - 2]! + cost)
      }

      minInRow = Math.min(minInRow, d[i]![j]!)
    }

    if (minInRow > maxDistance) {
      return maxDistance + 1
    }
  }

  return Math.min(d[m]![n]!, maxDistance + 1)
}

// =============================================================================
// Fuzzy Matching
// =============================================================================

/**
 * Check if a term should use fuzzy matching based on options
 *
 * @param term - The search term
 * @param options - Fuzzy options
 * @returns true if fuzzy matching should be applied
 */
export function shouldApplyFuzzy(
  term: string,
  options: NormalizedFuzzyOptions
): boolean {
  if (!options.enabled) return false
  return term.length >= options.minTermLength
}

/**
 * Check if two terms match considering prefix requirement
 *
 * @param queryTerm - The query term
 * @param vocabTerm - The vocabulary term
 * @param prefixLength - Number of characters that must match exactly
 * @returns true if prefix matches
 */
export function prefixMatches(
  queryTerm: string,
  vocabTerm: string,
  prefixLength: number
): boolean {
  if (prefixLength <= 0) return true
  if (queryTerm.length < prefixLength || vocabTerm.length < prefixLength) {
    return false
  }
  return queryTerm.slice(0, prefixLength) === vocabTerm.slice(0, prefixLength)
}

/**
 * Find fuzzy matches for a term in a vocabulary
 *
 * @param queryTerm - The search term (should already be normalized/stemmed)
 * @param vocabulary - Iterable of terms to search
 * @param options - Fuzzy matching options
 * @returns Array of matching terms with their edit distances
 */
export function findFuzzyMatches(
  queryTerm: string,
  vocabulary: Iterable<string>,
  options: NormalizedFuzzyOptions
): FuzzyMatch[] {
  const matches: FuzzyMatch[] = []

  // Check if fuzzy should be applied to this term
  if (!shouldApplyFuzzy(queryTerm, options)) {
    return matches
  }

  const { maxDistance, prefixLength } = options

  for (const vocabTerm of vocabulary) {
    // Quick prefix check
    if (!prefixMatches(queryTerm, vocabTerm, prefixLength)) {
      continue
    }

    // Use Damerau-Levenshtein for better typo detection (includes transpositions)
    const distance = damerauLevenshteinDistanceBounded(
      queryTerm,
      vocabTerm,
      maxDistance
    )

    if (distance <= maxDistance) {
      matches.push({ term: vocabTerm, distance })
    }
  }

  // Sort by distance (closest matches first), then alphabetically for consistency
  matches.sort((a, b) => {
    if (a.distance !== b.distance) {
      return a.distance - b.distance
    }
    return a.term.localeCompare(b.term)
  })

  return matches
}

/**
 * Expand query terms with fuzzy matches
 *
 * @param queryTerms - Original query terms (normalized/stemmed)
 * @param vocabulary - Iterable of terms in the index
 * @param options - Fuzzy matching options
 * @returns Map of original terms to their expansions (including exact match)
 */
export function expandQueryTerms(
  queryTerms: string[],
  vocabulary: Iterable<string>,
  options: NormalizedFuzzyOptions
): Map<string, FuzzyMatch[]> {
  const expansions = new Map<string, FuzzyMatch[]>()
  const vocabSet = vocabulary instanceof Set
    ? vocabulary
    : new Set(vocabulary)

  for (const term of queryTerms) {
    const termMatches: FuzzyMatch[] = []

    // Always include exact match if it exists
    if (vocabSet.has(term)) {
      termMatches.push({ term, distance: 0 })
    }

    // Find fuzzy matches
    if (shouldApplyFuzzy(term, options)) {
      const fuzzyMatches = findFuzzyMatches(term, vocabSet, options)
      for (const match of fuzzyMatches) {
        // Avoid duplicating exact match
        if (match.distance > 0) {
          termMatches.push(match)
        }
      }
    }

    expansions.set(term, termMatches)
  }

  return expansions
}

/**
 * Calculate score penalty for fuzzy match
 *
 * Fuzzy matches receive a penalty based on edit distance.
 * Distance 0 (exact) = 1.0, Distance 1 = 0.8, Distance 2 = 0.6, etc.
 *
 * @param distance - Edit distance
 * @param maxDistance - Maximum allowed distance
 * @returns Score multiplier (0.0 - 1.0)
 */
export function fuzzyScorePenalty(distance: number, maxDistance: number): number {
  if (distance === 0) return 1.0
  if (distance > maxDistance) return 0.0
  // Linear penalty: distance 1 = 0.8, distance 2 = 0.6, etc.
  return Math.max(0, 1.0 - (distance * 0.2))
}
