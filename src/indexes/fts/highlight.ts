/**
 * Highlight and Snippet Generation for Full-Text Search
 *
 * Provides utilities for highlighting matching terms in text
 * and generating relevant snippets from search results.
 */

import { porterStem } from './tokenizer'

// =============================================================================
// Types
// =============================================================================

/**
 * Options for text highlighting
 */
export interface HighlightOptions {
  /** HTML tag to insert before matches (default: '<mark>') */
  preTag?: string
  /** HTML tag to insert after matches (default: '</mark>') */
  postTag?: string
  /** Whether to match stemmed forms of terms (default: false) */
  matchStemmed?: boolean
  /** Whether to escape HTML in the original text (default: true) */
  escapeHtml?: boolean
}

/**
 * Options for snippet generation
 */
export interface SnippetOptions extends HighlightOptions {
  /** Maximum length of the snippet in characters (default: 150) */
  maxLength?: number
  /** String to use for ellipsis (default: '...') */
  ellipsis?: string
  /** Number of characters to show before the match (default: calculated from maxLength) */
  contextBefore?: number
  /** Number of characters to show after the match (default: calculated from maxLength) */
  contextAfter?: number
}

/**
 * Options for generating highlights for multiple fields
 */
export interface GenerateHighlightsOptions extends SnippetOptions {
  /** Maximum number of snippets per field (default: 3) */
  maxSnippets?: number
  /** Maximum length for snippets (default: 150) */
  maxSnippetLength?: number
  /** Whether to include fields with no matches (default: false) */
  includeNonMatching?: boolean
}

/**
 * Match information for a term in text
 */
interface TermMatch {
  /** Start index in the text */
  start: number
  /** End index in the text */
  end: number
  /** The actual matched text (preserves original case) */
  matchedText: string
}

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_PRE_TAG = '<mark>'
const DEFAULT_POST_TAG = '</mark>'
const DEFAULT_MAX_LENGTH = 150
const DEFAULT_ELLIPSIS = '...'
const DEFAULT_MAX_SNIPPETS = 3

// =============================================================================
// HTML Escaping
// =============================================================================

/**
 * Escape HTML special characters
 */
function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

// =============================================================================
// Term Matching
// =============================================================================

/**
 * Find all matches of terms in text
 *
 * @param text - The text to search
 * @param terms - Array of terms to find
 * @param matchStemmed - Whether to match stemmed forms
 * @returns Array of matches sorted by position
 */
function findMatches(
  text: string,
  terms: string[],
  matchStemmed: boolean = false
): TermMatch[] {
  if (terms.length === 0) return []

  const matches: TermMatch[] = []
  const lowerText = text.toLowerCase()

  // Extract word boundaries for stemmed matching
  const wordRegex = /[a-zA-Z0-9]+/g
  const words: Array<{ word: string; start: number; end: number; stemmed: string }> = []

  if (matchStemmed) {
    let match: RegExpExecArray | null
    while ((match = wordRegex.exec(text)) !== null) {
      words.push({
        word: match[0],
        start: match.index,
        end: match.index + match[0].length,
        stemmed: porterStem(match[0].toLowerCase()),
      })
    }
  }

  // Normalize and deduplicate terms
  const normalizedTerms = new Set<string>()
  const stemmedTerms = new Set<string>()

  for (const term of terms) {
    const lower = term.toLowerCase()
    normalizedTerms.add(lower)
    if (matchStemmed) {
      // Add both the term itself (in case it's already stemmed) and its stemmed form
      stemmedTerms.add(lower)
      stemmedTerms.add(porterStem(lower))
    }
  }

  if (matchStemmed) {
    // Match based on stemmed forms
    for (const word of words) {
      // Check if the word's stem matches any of our stemmed terms
      // or if the word itself matches any term directly
      if (stemmedTerms.has(word.stemmed) || stemmedTerms.has(word.word.toLowerCase())) {
        matches.push({
          start: word.start,
          end: word.end,
          matchedText: word.word,
        })
      }
    }
  } else {
    // Direct substring matching
    for (const term of normalizedTerms) {
      let pos = 0
      while ((pos = lowerText.indexOf(term, pos)) !== -1) {
        // Check if this is a word boundary match (not in the middle of a word)
        const beforeOk = pos === 0 || !/[a-zA-Z0-9]/.test(lowerText[pos - 1]!)
        const afterOk = pos + term.length >= lowerText.length ||
          !/[a-zA-Z0-9]/.test(lowerText[pos + term.length]!)

        if (beforeOk && afterOk) {
          matches.push({
            start: pos,
            end: pos + term.length,
            matchedText: text.slice(pos, pos + term.length),
          })
        }
        pos += 1
      }
    }
  }

  // Sort by start position
  matches.sort((a, b) => a.start - b.start)

  // Merge overlapping matches (keep the longer one)
  const merged: TermMatch[] = []
  for (const match of matches) {
    const last = merged[merged.length - 1]
    if (last && match.start < last.end) {
      // Overlapping - keep the longer match
      if (match.end > last.end) {
        merged[merged.length - 1] = {
          start: last.start,
          end: match.end,
          matchedText: text.slice(last.start, match.end),
        }
      }
    } else {
      merged.push(match)
    }
  }

  return merged
}

// =============================================================================
// Highlighting
// =============================================================================

/**
 * Highlight matching terms in text
 *
 * @param text - The text to highlight
 * @param terms - Array of terms to highlight (can be stemmed or original)
 * @param options - Highlight options
 * @returns Text with matched terms wrapped in highlight tags
 */
export function highlightText(
  text: string,
  terms: string[],
  options: HighlightOptions = {}
): string {
  const {
    preTag = DEFAULT_PRE_TAG,
    postTag = DEFAULT_POST_TAG,
    matchStemmed = false,
    escapeHtml: shouldEscape = true,
  } = options

  if (terms.length === 0 || !text) {
    return shouldEscape ? escapeHtml(text) : text
  }

  const matches = findMatches(text, terms, matchStemmed)

  if (matches.length === 0) {
    return shouldEscape ? escapeHtml(text) : text
  }

  // Build result string
  const parts: string[] = []
  let lastEnd = 0

  for (const match of matches) {
    // Add text before match
    if (match.start > lastEnd) {
      const before = text.slice(lastEnd, match.start)
      parts.push(shouldEscape ? escapeHtml(before) : before)
    }

    // Add highlighted match (escape the matched text too)
    const matchedText = shouldEscape ? escapeHtml(match.matchedText) : match.matchedText
    parts.push(preTag + matchedText + postTag)

    lastEnd = match.end
  }

  // Add remaining text
  if (lastEnd < text.length) {
    const remaining = text.slice(lastEnd)
    parts.push(shouldEscape ? escapeHtml(remaining) : remaining)
  }

  return parts.join('')
}

// =============================================================================
// Snippet Generation
// =============================================================================

/**
 * Generate a snippet around matching terms
 *
 * @param text - The full text
 * @param terms - Terms to highlight and center on
 * @param options - Snippet options
 * @returns A snippet with highlighted terms
 */
export function generateSnippet(
  text: string,
  terms: string[],
  options: SnippetOptions = {}
): string {
  const {
    maxLength = DEFAULT_MAX_LENGTH,
    ellipsis = DEFAULT_ELLIPSIS,
    preTag = DEFAULT_PRE_TAG,
    postTag = DEFAULT_POST_TAG,
    matchStemmed = false,
    escapeHtml: shouldEscape = true,
  } = options

  if (!text) return ''

  const matches = findMatches(text, terms, matchStemmed)

  // If no matches, return beginning of text
  if (matches.length === 0) {
    if (text.length <= maxLength) {
      return shouldEscape ? escapeHtml(text) : text
    }
    const truncated = text.slice(0, maxLength)
    // Try to break at word boundary
    const lastSpace = truncated.lastIndexOf(' ')
    const breakPoint = lastSpace > maxLength * 0.5 ? lastSpace : maxLength
    return (shouldEscape ? escapeHtml(text.slice(0, breakPoint)) : text.slice(0, breakPoint)) + ellipsis
  }

  // Find the best match to center on (prefer matches with more context available)
  const centerMatch = findBestCenterMatch(text, matches, maxLength)

  // Calculate snippet boundaries
  const halfLength = Math.floor(maxLength / 2)
  let start = Math.max(0, centerMatch.start - halfLength)
  let end = Math.min(text.length, centerMatch.end + halfLength)

  // Adjust to try to use full maxLength
  const currentLength = end - start
  if (currentLength < maxLength) {
    const remaining = maxLength - currentLength
    if (start > 0) {
      start = Math.max(0, start - remaining)
    } else {
      end = Math.min(text.length, end + remaining)
    }
  }

  // Try to break at word boundaries
  if (start > 0) {
    const searchStart = Math.max(0, start - 10)
    const prefix = text.slice(searchStart, start + 10)
    const spaceInPrefix = prefix.indexOf(' ')
    if (spaceInPrefix !== -1) {
      start = searchStart + spaceInPrefix + 1
    }
  }

  if (end < text.length) {
    const searchEnd = Math.min(text.length, end + 10)
    const suffix = text.slice(end - 10, searchEnd)
    const lastSpaceInSuffix = suffix.lastIndexOf(' ')
    if (lastSpaceInSuffix !== -1) {
      end = (end - 10) + lastSpaceInSuffix
    }
  }

  // Extract snippet
  let snippet = text.slice(start, end)

  // Add ellipsis
  const needsStartEllipsis = start > 0
  const needsEndEllipsis = end < text.length

  // Highlight the snippet
  // Adjust matches for snippet offset
  const snippetMatches = matches
    .filter(m => m.start >= start && m.end <= end)
    .map(m => ({
      ...m,
      start: m.start - start,
      end: m.end - start,
      matchedText: text.slice(m.start, m.end),
    }))

  // Build highlighted snippet
  const parts: string[] = []
  if (needsStartEllipsis) parts.push(ellipsis)

  let lastEnd = 0
  for (const match of snippetMatches) {
    if (match.start > lastEnd) {
      const before = snippet.slice(lastEnd, match.start)
      parts.push(shouldEscape ? escapeHtml(before) : before)
    }
    const matchedText = shouldEscape ? escapeHtml(match.matchedText) : match.matchedText
    parts.push(preTag + matchedText + postTag)
    lastEnd = match.end
  }

  if (lastEnd < snippet.length) {
    const remaining = snippet.slice(lastEnd)
    parts.push(shouldEscape ? escapeHtml(remaining) : remaining)
  }

  if (needsEndEllipsis) parts.push(ellipsis)

  return parts.join('')
}

/**
 * Find the best match to center the snippet on
 */
function findBestCenterMatch(
  text: string,
  matches: TermMatch[],
  maxLength: number
): TermMatch {
  if (matches.length === 1) return matches[0]!

  // Prefer matches that are not at the very beginning or end
  // This gives more context around the match
  const halfLength = Math.floor(maxLength / 2)

  let bestMatch = matches[0]!
  let bestScore = 0

  for (const match of matches) {
    const contextBefore = Math.min(match.start, halfLength)
    const contextAfter = Math.min(text.length - match.end, halfLength)
    const score = contextBefore + contextAfter

    if (score > bestScore) {
      bestScore = score
      bestMatch = match
    }
  }

  return bestMatch
}

// =============================================================================
// Multi-field Highlights
// =============================================================================

/**
 * Generate highlights for multiple document fields
 *
 * @param doc - The document object
 * @param fields - Array of field paths to highlight
 * @param terms - Terms to highlight
 * @param options - Options for highlight generation
 * @returns Object mapping field names to arrays of highlighted snippets
 */
export function generateHighlights(
  doc: Record<string, unknown>,
  fields: string[],
  terms: string[],
  options: GenerateHighlightsOptions = {}
): Record<string, string[]> {
  const {
    maxSnippets = DEFAULT_MAX_SNIPPETS,
    maxSnippetLength = DEFAULT_MAX_LENGTH,
    includeNonMatching = false,
    preTag = DEFAULT_PRE_TAG,
    postTag = DEFAULT_POST_TAG,
    matchStemmed = false,
    escapeHtml: shouldEscape = true,
  } = options

  const result: Record<string, string[]> = {}

  for (const fieldPath of fields) {
    const value = getNestedValue(doc, fieldPath)
    if (typeof value !== 'string') continue

    const matches = findMatches(value, terms, matchStemmed)

    if (matches.length === 0) {
      if (includeNonMatching) {
        result[fieldPath] = [shouldEscape ? escapeHtml(value) : value]
      }
      continue
    }

    // For short text, just highlight the whole thing
    if (value.length <= maxSnippetLength * 1.5) {
      result[fieldPath] = [
        highlightText(value, terms, { preTag, postTag, matchStemmed, escapeHtml: shouldEscape }),
      ]
      continue
    }

    // For long text, generate multiple snippets around different matches
    const snippets: string[] = []
    const usedRanges: Array<{ start: number; end: number }> = []

    // Group matches that are close together
    const matchGroups: TermMatch[][] = []
    let currentGroup: TermMatch[] = []

    for (const match of matches) {
      if (currentGroup.length === 0) {
        currentGroup.push(match)
      } else {
        const lastInGroup = currentGroup[currentGroup.length - 1]!
        if (match.start - lastInGroup.end < maxSnippetLength / 2) {
          currentGroup.push(match)
        } else {
          matchGroups.push(currentGroup)
          currentGroup = [match]
        }
      }
    }
    if (currentGroup.length > 0) {
      matchGroups.push(currentGroup)
    }

    // Generate snippets for each group
    for (const group of matchGroups) {
      if (snippets.length >= maxSnippets) break

      // Check if this range overlaps with already-used ranges
      const groupStart = group[0]!.start - maxSnippetLength / 2
      const groupEnd = group[group.length - 1]!.end + maxSnippetLength / 2

      const overlaps = usedRanges.some(
        range => !(groupEnd < range.start || groupStart > range.end)
      )

      if (!overlaps) {
        const snippet = generateSnippet(value, terms, {
          maxLength: maxSnippetLength,
          preTag,
          postTag,
          matchStemmed,
          escapeHtml: shouldEscape,
        })
        snippets.push(snippet)
        usedRanges.push({ start: groupStart, end: groupEnd })
      }
    }

    if (snippets.length > 0) {
      result[fieldPath] = snippets
    }
  }

  return result
}

/**
 * Get a nested value from an object using dot notation
 */
function getNestedValue(obj: Record<string, unknown>, path: string): unknown {
  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (current === null || current === undefined) return undefined
    if (typeof current !== 'object') return undefined
    current = (current as Record<string, unknown>)[part]
  }

  return current
}
