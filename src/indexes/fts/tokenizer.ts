/**
 * Tokenizer for Full-Text Search
 *
 * Provides text tokenization with:
 * - Lowercase normalization
 * - Whitespace/punctuation splitting
 * - Stopword removal
 * - Porter stemming
 */

import type { Token, TokenizerOptions } from './types'

// =============================================================================
// Default Stopwords
// =============================================================================

/**
 * English stopwords
 */
export const ENGLISH_STOPWORDS = new Set([
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by', 'for',
  'if', 'in', 'into', 'is', 'it', 'no', 'not', 'of', 'on', 'or',
  'such', 'that', 'the', 'their', 'then', 'there', 'these', 'they',
  'this', 'to', 'was', 'will', 'with', 'would', 'could', 'should',
  'have', 'has', 'had', 'do', 'does', 'did', 'i', 'you', 'he', 'she',
  'we', 'what', 'which', 'who', 'when', 'where', 'why', 'how',
  'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other',
  'some', 'any', 'only', 'own', 'same', 'so', 'than', 'too', 'very',
  'can', 'just', 'also', 'now', 'here', 'about', 'after', 'before',
  'between', 'during', 'from', 'through', 'under', 'above', 'below',
  'up', 'down', 'out', 'off', 'over', 'again', 'further', 'once',
  'am', 'been', 'being', 'were', 'its', 'your', 'him', 'her', 'my',
  'me', 'our', 'us', 'them', 'his', 'hers', 'ours', 'theirs', 'yours',
])

// =============================================================================
// Tokenizer
// =============================================================================

/**
 * Tokenize text into normalized terms
 *
 * @param text - Input text
 * @param options - Tokenizer options
 * @returns Array of tokens
 */
export function tokenize(text: string, options: TokenizerOptions = {}): Token[] {
  const {
    minWordLength = 2,
    maxWordLength = 50,
    stopwords = ENGLISH_STOPWORDS,
    stem = true,
  } = options

  const tokens: Token[] = []

  // Split on whitespace and punctuation, keeping track of positions
  const wordRegex = /[a-zA-Z0-9]+/g
  let match: RegExpExecArray | null

  let position = 0
  while ((match = wordRegex.exec(text)) !== null) {
    const original = match[0]
    const startOffset = match.index
    const endOffset = match.index + original.length

    // Skip if too short or too long
    if (original.length < minWordLength || original.length > maxWordLength) {
      continue
    }

    // Lowercase
    let term = original.toLowerCase()

    // Skip stopwords
    if (stopwords.has(term)) {
      position++
      continue
    }

    // Apply stemming
    if (stem) {
      term = porterStem(term)
    }

    tokens.push({
      original,
      term,
      position,
      startOffset,
      endOffset,
    })

    position++
  }

  return tokens
}

/**
 * Tokenize for search query (less aggressive)
 *
 * @param query - Search query
 * @param options - Tokenizer options
 * @returns Array of query terms
 */
export function tokenizeQuery(query: string, options: TokenizerOptions = {}): string[] {
  const {
    minWordLength = 2,
    maxWordLength = 50,
    stem = true,
  } = options

  const terms: string[] = []
  const wordRegex = /[a-zA-Z0-9]+/g
  let match: RegExpExecArray | null

  while ((match = wordRegex.exec(query)) !== null) {
    const word = match[0]

    if (word.length < minWordLength || word.length > maxWordLength) {
      continue
    }

    let term = word.toLowerCase()

    // Don't filter stopwords in queries - they might be intentional
    if (stem) {
      term = porterStem(term)
    }

    terms.push(term)
  }

  return terms
}

// =============================================================================
// Porter Stemmer (Simplified)
// =============================================================================

/**
 * Simplified Porter stemmer for English
 * Based on Martin Porter's algorithm
 */
export function porterStem(word: string): string {
  if (word.length <= 2) {
    return word
  }

  // Step 1a: SSES -> SS, IES -> I, SS -> SS, S ->
  let stem = word
  if (stem.endsWith('sses')) {
    stem = stem.slice(0, -2)
  } else if (stem.endsWith('ies')) {
    stem = stem.slice(0, -2)
  } else if (stem.endsWith('ss')) {
    // keep
  } else if (stem.endsWith('s')) {
    stem = stem.slice(0, -1)
  }

  // Step 1b: (m>0) EED -> EE, (*v*) ED ->, (*v*) ING ->
  if (stem.endsWith('eed')) {
    if (measureConsonants(stem.slice(0, -3)) > 0) {
      stem = stem.slice(0, -1)
    }
  } else if (stem.endsWith('ed')) {
    const prefix = stem.slice(0, -2)
    if (hasVowel(prefix)) {
      stem = prefix
      stem = step1bPostProcess(stem)
    }
  } else if (stem.endsWith('ing')) {
    const prefix = stem.slice(0, -3)
    if (hasVowel(prefix)) {
      stem = prefix
      stem = step1bPostProcess(stem)
    }
  }

  // Step 1c: (*v*) Y -> I
  if (stem.endsWith('y')) {
    const prefix = stem.slice(0, -1)
    if (hasVowel(prefix)) {
      stem = prefix + 'i'
    }
  }

  // Step 2: (m>0) suffix replacements
  stem = step2(stem)

  // Step 3: (m>0) suffix replacements
  stem = step3(stem)

  // Step 4: (m>1) suffix removals
  stem = step4(stem)

  // Step 5a: (m>1) E ->, (m=1 and not *o) E ->
  if (stem.endsWith('e')) {
    const prefix = stem.slice(0, -1)
    const m = measureConsonants(prefix)
    if (m > 1 || (m === 1 && !endsWithCVC(prefix))) {
      stem = prefix
    }
  }

  // Step 5b: (m>1 and *d and *L) -> single letter
  if (stem.length > 1 && stem.endsWith('ll') && measureConsonants(stem.slice(0, -1)) > 1) {
    stem = stem.slice(0, -1)
  }

  return stem
}

function step1bPostProcess(stem: string): string {
  if (stem.endsWith('at') || stem.endsWith('bl') || stem.endsWith('iz')) {
    return stem + 'e'
  }

  // Double consonant
  if (stem.length >= 2) {
    const last = stem[stem.length - 1]!  // length >= 2 ensures valid index
    const secondLast = stem[stem.length - 2]
    if (last === secondLast && isConsonant(stem, stem.length - 1)) {
      if (!['l', 's', 'z'].includes(last)) {
        return stem.slice(0, -1)
      }
    }
  }

  // *o
  if (endsWithCVC(stem) && measureConsonants(stem) === 1) {
    return stem + 'e'
  }

  return stem
}

function step2(stem: string): string {
  const suffixes: Record<string, string> = {
    'ational': 'ate',
    'tional': 'tion',
    'enci': 'ence',
    'anci': 'ance',
    'izer': 'ize',
    'abli': 'able',
    'alli': 'al',
    'entli': 'ent',
    'eli': 'e',
    'ousli': 'ous',
    'ization': 'ize',
    'ation': 'ate',
    'ator': 'ate',
    'alism': 'al',
    'iveness': 'ive',
    'fulness': 'ful',
    'ousness': 'ous',
    'aliti': 'al',
    'iviti': 'ive',
    'biliti': 'ble',
  }

  for (const [suffix, replacement] of Object.entries(suffixes)) {
    if (stem.endsWith(suffix)) {
      const prefix = stem.slice(0, -suffix.length)
      if (measureConsonants(prefix) > 0) {
        return prefix + replacement
      }
    }
  }

  return stem
}

function step3(stem: string): string {
  const suffixes: Record<string, string> = {
    'icate': 'ic',
    'ative': '',
    'alize': 'al',
    'iciti': 'ic',
    'ical': 'ic',
    'ful': '',
    'ness': '',
  }

  for (const [suffix, replacement] of Object.entries(suffixes)) {
    if (stem.endsWith(suffix)) {
      const prefix = stem.slice(0, -suffix.length)
      if (measureConsonants(prefix) > 0) {
        return prefix + replacement
      }
    }
  }

  return stem
}

function step4(stem: string): string {
  const suffixes = [
    'al', 'ance', 'ence', 'er', 'ic', 'able', 'ible', 'ant', 'ement',
    'ment', 'ent', 'ion', 'ou', 'ism', 'ate', 'iti', 'ous', 'ive', 'ize',
  ]

  for (const suffix of suffixes) {
    if (stem.endsWith(suffix)) {
      const prefix = stem.slice(0, -suffix.length)

      // Special case for 'ion'
      if (suffix === 'ion') {
        if (prefix.endsWith('s') || prefix.endsWith('t')) {
          if (measureConsonants(prefix) > 1) {
            return prefix
          }
        }
      } else if (measureConsonants(prefix) > 1) {
        return prefix
      }
    }
  }

  return stem
}

// Helper functions

function isVowel(word: string, index: number): boolean {
  const c = word[index]
  if (!c) return false
  if ('aeiou'.includes(c)) return true
  if (c === 'y' && index > 0 && !isVowel(word, index - 1)) return true
  return false
}

function isConsonant(word: string, index: number): boolean {
  return !isVowel(word, index)
}

function hasVowel(word: string): boolean {
  for (let i = 0; i < word.length; i++) {
    if (isVowel(word, i)) return true
  }
  return false
}

function measureConsonants(word: string): number {
  // m = [C](VC){m}[V]
  // Count VC sequences
  let m = 0
  let i = 0

  // Skip initial consonants
  while (i < word.length && isConsonant(word, i)) i++

  while (i < word.length) {
    // Must see vowels then consonants
    while (i < word.length && isVowel(word, i)) i++
    if (i >= word.length) break

    while (i < word.length && isConsonant(word, i)) i++
    m++
  }

  return m
}

function endsWithCVC(word: string): boolean {
  // *o - ends with CVC where second C is not W, X, or Y
  const len = word.length
  if (len < 3) return false

  if (
    isConsonant(word, len - 3) &&
    isVowel(word, len - 2) &&
    isConsonant(word, len - 1)
  ) {
    const lastChar = word[len - 1]!  // len >= 3 ensures valid index
    return !['w', 'x', 'y'].includes(lastChar)
  }

  return false
}
