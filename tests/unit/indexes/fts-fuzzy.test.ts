/**
 * Tests for FTS Fuzzy Matching
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  levenshteinDistance,
  levenshteinDistanceBounded,
  damerauLevenshteinDistance,
  damerauLevenshteinDistanceBounded,
  findFuzzyMatches,
  expandQueryTerms,
  normalizeFuzzyOptions,
  fuzzyScorePenalty,
  shouldApplyFuzzy,
  prefixMatches,
} from '@/indexes/fts/fuzzy'
import { FTSIndex } from '@/indexes/fts/search'
import { MemoryBackend } from '@/storage/MemoryBackend'
import type { IndexDefinition } from '@/indexes/types'

// =============================================================================
// Levenshtein Distance Tests
// =============================================================================

describe('levenshteinDistance', () => {
  it('returns 0 for identical strings', () => {
    expect(levenshteinDistance('hello', 'hello')).toBe(0)
    expect(levenshteinDistance('', '')).toBe(0)
  })

  it('returns correct distance for single character difference', () => {
    expect(levenshteinDistance('cat', 'bat')).toBe(1) // substitution
    expect(levenshteinDistance('cat', 'cats')).toBe(1) // insertion
    expect(levenshteinDistance('cats', 'cat')).toBe(1) // deletion
  })

  it('returns correct distance for multiple edits', () => {
    expect(levenshteinDistance('kitten', 'sitting')).toBe(3)
    expect(levenshteinDistance('saturday', 'sunday')).toBe(3)
  })

  it('handles empty strings', () => {
    expect(levenshteinDistance('', 'abc')).toBe(3)
    expect(levenshteinDistance('abc', '')).toBe(3)
  })

  it('is commutative', () => {
    expect(levenshteinDistance('abc', 'def')).toBe(levenshteinDistance('def', 'abc'))
    expect(levenshteinDistance('hello', 'hallo')).toBe(levenshteinDistance('hallo', 'hello'))
  })
})

describe('levenshteinDistanceBounded', () => {
  it('returns exact distance when within bound', () => {
    expect(levenshteinDistanceBounded('cat', 'bat', 2)).toBe(1)
    expect(levenshteinDistanceBounded('kitten', 'sitting', 5)).toBe(3)
  })

  it('returns maxDistance + 1 when exceeds bound', () => {
    expect(levenshteinDistanceBounded('hello', 'world', 2)).toBe(3)
    expect(levenshteinDistanceBounded('abcdef', 'xyz', 2)).toBe(3)
  })

  it('handles quick length check optimization', () => {
    expect(levenshteinDistanceBounded('abc', 'abcdefgh', 2)).toBe(3)
  })
})

// =============================================================================
// Damerau-Levenshtein Distance Tests
// =============================================================================

describe('damerauLevenshteinDistance', () => {
  it('returns 0 for identical strings', () => {
    expect(damerauLevenshteinDistance('hello', 'hello')).toBe(0)
  })

  it('counts transposition as single edit', () => {
    // Standard Levenshtein would count this as 2 (delete + insert)
    // Damerau-Levenshtein counts it as 1 (transposition)
    expect(damerauLevenshteinDistance('ab', 'ba')).toBe(1)
    expect(damerauLevenshteinDistance('abc', 'bac')).toBe(1)
  })

  it('handles common typos', () => {
    expect(damerauLevenshteinDistance('teh', 'the')).toBe(1) // transposition
    expect(damerauLevenshteinDistance('recieve', 'receive')).toBe(1) // transposition
  })

  it('handles multiple edits including transpositions', () => {
    expect(damerauLevenshteinDistance('abcd', 'badc')).toBe(2) // two transpositions
  })
})

describe('damerauLevenshteinDistanceBounded', () => {
  it('returns exact distance when within bound', () => {
    expect(damerauLevenshteinDistanceBounded('ab', 'ba', 2)).toBe(1)
  })

  it('returns maxDistance + 1 when exceeds bound', () => {
    expect(damerauLevenshteinDistanceBounded('hello', 'world', 2)).toBe(3)
  })
})

// =============================================================================
// Fuzzy Options Tests
// =============================================================================

describe('normalizeFuzzyOptions', () => {
  it('returns disabled options for undefined', () => {
    const opts = normalizeFuzzyOptions(undefined)
    expect(opts.enabled).toBe(false)
  })

  it('returns disabled options for false', () => {
    const opts = normalizeFuzzyOptions(false)
    expect(opts.enabled).toBe(false)
  })

  it('returns enabled options with defaults for true', () => {
    const opts = normalizeFuzzyOptions(true)
    expect(opts.enabled).toBe(true)
    expect(opts.maxDistance).toBe(2)
    expect(opts.minTermLength).toBe(4)
    expect(opts.prefixLength).toBe(1)
  })

  it('merges custom options with defaults', () => {
    const opts = normalizeFuzzyOptions({ maxDistance: 3, prefixLength: 2 })
    expect(opts.enabled).toBe(true)
    expect(opts.maxDistance).toBe(3)
    expect(opts.minTermLength).toBe(4) // default
    expect(opts.prefixLength).toBe(2)
  })
})

describe('shouldApplyFuzzy', () => {
  const enabledOpts = normalizeFuzzyOptions(true)
  const disabledOpts = normalizeFuzzyOptions(false)

  it('returns false when fuzzy is disabled', () => {
    expect(shouldApplyFuzzy('database', disabledOpts)).toBe(false)
  })

  it('returns false for short terms', () => {
    expect(shouldApplyFuzzy('cat', enabledOpts)).toBe(false) // 3 chars < minTermLength (4)
  })

  it('returns true for terms meeting minimum length', () => {
    expect(shouldApplyFuzzy('data', enabledOpts)).toBe(true) // 4 chars >= minTermLength
    expect(shouldApplyFuzzy('database', enabledOpts)).toBe(true)
  })
})

describe('prefixMatches', () => {
  it('returns true when prefix length is 0', () => {
    expect(prefixMatches('abc', 'xyz', 0)).toBe(true)
  })

  it('returns true when prefixes match', () => {
    expect(prefixMatches('database', 'databse', 4)).toBe(true)
    expect(prefixMatches('hello', 'help', 3)).toBe(true)
  })

  it('returns false when prefixes differ', () => {
    expect(prefixMatches('database', 'aatabase', 1)).toBe(false)
    expect(prefixMatches('hello', 'jello', 1)).toBe(false)
  })

  it('returns false when terms are too short', () => {
    expect(prefixMatches('ab', 'abc', 3)).toBe(false)
  })
})

describe('fuzzyScorePenalty', () => {
  it('returns 1.0 for exact matches (distance 0)', () => {
    expect(fuzzyScorePenalty(0, 2)).toBe(1.0)
  })

  it('applies penalty based on distance', () => {
    expect(fuzzyScorePenalty(1, 2)).toBe(0.8)
    expect(fuzzyScorePenalty(2, 2)).toBe(0.6)
  })

  it('returns 0 for distance exceeding max', () => {
    expect(fuzzyScorePenalty(3, 2)).toBe(0.0)
  })
})

// =============================================================================
// Fuzzy Matching Tests
// =============================================================================

describe('findFuzzyMatches', () => {
  const vocabulary = new Set(['database', 'datbase', 'databasee', 'data', 'base', 'databaes'])
  const opts = normalizeFuzzyOptions(true)

  it('finds matches within edit distance', () => {
    const matches = findFuzzyMatches('database', vocabulary, opts)

    const terms = matches.map(m => m.term)
    expect(terms).toContain('datbase') // distance 1: missing 'a'
    expect(terms).toContain('databasee') // distance 1: extra 'e'
    expect(terms).toContain('databaes') // distance 1: transposition
  })

  it('respects prefix length', () => {
    const optsWithPrefix = normalizeFuzzyOptions({ prefixLength: 3 })
    const vocabWithDifferentPrefix = new Set(['database', 'xatabase', 'dbtabase'])

    const matches = findFuzzyMatches('database', vocabWithDifferentPrefix, optsWithPrefix)
    const terms = matches.map(m => m.term)

    // 'xatabase' has different prefix, should not match
    expect(terms).not.toContain('xatabase')
    // 'dbtabase' has different prefix at position 2
    expect(terms).not.toContain('dbtabase')
  })

  it('returns empty for short terms', () => {
    const matches = findFuzzyMatches('cat', vocabulary, opts)
    expect(matches).toHaveLength(0) // 'cat' is too short
  })

  it('sorts by distance then alphabetically', () => {
    // 'databass' has distance 1 from 'database' (substitution of e->s)
    // 'databaes' has distance 1 from 'database' (transposition of s and e)
    // 'databases' has distance 1 from 'database' (insertion of s)
    const testVocab = new Set(['databases', 'databass', 'databaes'])
    const matches = findFuzzyMatches('database', testVocab, opts)

    // All have distance 1, so should be alphabetically sorted
    expect(matches.length).toBe(3)
    // Verify all have the same distance
    expect(matches.every(m => m.distance === 1)).toBe(true)
    // Check alphabetical order
    const terms = matches.map(m => m.term)
    const sortedTerms = [...terms].sort()
    expect(terms).toEqual(sortedTerms)
  })
})

describe('expandQueryTerms', () => {
  const vocabulary = new Set(['search', 'serch', 'database', 'databasee', 'quick'])
  const opts = normalizeFuzzyOptions(true)

  it('includes exact matches with distance 0', () => {
    const expansions = expandQueryTerms(['search'], vocabulary, opts)
    const searchMatches = expansions.get('search')!

    expect(searchMatches.some(m => m.term === 'search' && m.distance === 0)).toBe(true)
  })

  it('includes fuzzy matches', () => {
    const expansions = expandQueryTerms(['search'], vocabulary, opts)
    const searchMatches = expansions.get('search')!

    expect(searchMatches.some(m => m.term === 'serch' && m.distance > 0)).toBe(true)
  })

  it('handles multiple query terms', () => {
    const expansions = expandQueryTerms(['search', 'database'], vocabulary, opts)

    expect(expansions.has('search')).toBe(true)
    expect(expansions.has('database')).toBe(true)
  })

  it('does not duplicate exact matches in fuzzy results', () => {
    const expansions = expandQueryTerms(['search'], vocabulary, opts)
    const searchMatches = expansions.get('search')!

    const exactMatches = searchMatches.filter(m => m.term === 'search')
    expect(exactMatches).toHaveLength(1)
  })
})

// =============================================================================
// FTSIndex Fuzzy Search Integration Tests
// =============================================================================

describe('FTSIndex fuzzy search', () => {
  let storage: MemoryBackend
  let definition: IndexDefinition

  beforeEach(() => {
    storage = new MemoryBackend()
    definition = {
      name: 'idx_fts',
      type: 'fts',
      fields: [{ path: 'title' }, { path: 'content' }],
      ftsOptions: {
        language: 'en',
        minWordLength: 2,
        indexPositions: true,
      },
    }
  })

  it('finds documents with typos when fuzzy enabled', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      { docId: 'doc1', doc: { title: 'Introduction to Databases', content: 'Learn about database systems' } },
      { docId: 'doc2', doc: { title: 'Web Development Guide', content: 'Building modern websites' } },
    ])

    // Search with typo 'databse' instead of 'database'
    const resultsWithFuzzy = index.search('databse', { fuzzy: true })
    expect(resultsWithFuzzy.length).toBeGreaterThan(0)

    // Without fuzzy, should not find anything
    const resultsWithoutFuzzy = index.search('databse', { fuzzy: false })
    expect(resultsWithoutFuzzy).toHaveLength(0)
  })

  it('ranks exact matches higher than fuzzy matches', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      { docId: 'exact', doc: { title: 'Database Tutorial' } },
      { docId: 'fuzzy', doc: { title: 'Databse Tutorial' } }, // typo in indexed doc
    ])

    // Search for correct term
    const results = index.search('database', { fuzzy: true })

    expect(results.length).toBeGreaterThanOrEqual(2)
    // Exact match should rank first
    expect(results[0]!.docId).toBe('exact')
  })

  it('handles transposition typos', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      { docId: 'doc1', doc: { title: 'Receive notifications' } },
    ])

    // Common typo: 'recieve' (ei -> ie transposition)
    const results = index.search('recieve', { fuzzy: true })

    expect(results.length).toBeGreaterThan(0)
    expect(results[0]!.docId).toBe('doc1')
  })

  it('respects fuzzy options', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      { docId: 'doc1', doc: { title: 'elasticsearch tutorial' } },
    ])

    // With maxDistance 2, 'elasticserch' (missing 'a') should match
    const resultsDistance2 = index.search('elasticserch', {
      fuzzy: { maxDistance: 2 },
    })
    expect(resultsDistance2.length).toBeGreaterThan(0)

    // With maxDistance 0, only exact matches are allowed
    // 'elasticserch' != 'elasticsearch' (differs by 1 char), so no match
    const resultsDistance0 = index.search('elasticserch', {
      fuzzy: { maxDistance: 0, enabled: true },
    })
    expect(resultsDistance0).toHaveLength(0)
  })

  it('does not apply fuzzy to short terms', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      { docId: 'doc1', doc: { title: 'The cat sat' } },
    ])

    // 'cot' is too short (3 chars) for fuzzy matching by default
    // so it won't match 'cat' even with fuzzy enabled
    const results = index.search('cot', { fuzzy: true })

    expect(results).toHaveLength(0)
  })

  it('respects minTermLength option', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      { docId: 'doc1', doc: { title: 'The cat sat' } },
    ])

    // With minTermLength 3, 'cot' should match 'cat'
    const results = index.search('cot', {
      fuzzy: { minTermLength: 3 },
    })

    expect(results.length).toBeGreaterThan(0)
  })

  it('works with multiple search terms', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      { docId: 'doc1', doc: { title: 'Database Performance Tuning' } },
      { docId: 'doc2', doc: { title: 'Web Performance Tips' } },
    ])

    // Both terms have typos
    const results = index.search('databse performnce', { fuzzy: true })

    expect(results.length).toBeGreaterThan(0)
    // Doc with both matches should rank higher
    expect(results[0]!.docId).toBe('doc1')
  })

  it('applies fuzzy option as boolean shorthand', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      { docId: 'doc1', doc: { title: 'Introduction' } },
    ])

    // true should enable fuzzy with defaults
    const results = index.search('introducton', { fuzzy: true })
    expect(results.length).toBeGreaterThan(0)
  })

  it('getVocabulary returns indexed terms', () => {
    const index = new FTSIndex(storage, 'articles', definition)

    index.buildFromArray([
      { docId: 'doc1', doc: { title: 'Database Systems' } },
      { docId: 'doc2', doc: { title: 'Web Development' } },
    ])

    const vocab = Array.from(index.getVocabulary())

    // Should contain stemmed terms
    expect(vocab).toContain('databas') // stemmed from 'database'
    expect(vocab).toContain('system')
    expect(vocab).toContain('web')
    expect(vocab).toContain('develop')
  })
})
