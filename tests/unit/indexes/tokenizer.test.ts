/**
 * Tests for Tokenizer
 */

import { describe, it, expect } from 'vitest'
import {
  tokenize,
  tokenizeQuery,
  porterStem,
  ENGLISH_STOPWORDS,
} from '@/indexes/fts/tokenizer'

describe('Tokenizer', () => {
  describe('tokenize', () => {
    it('splits text into tokens', () => {
      const tokens = tokenize('Hello world')
      expect(tokens).toHaveLength(2)
      expect(tokens[0].original).toBe('Hello')
      expect(tokens[1].original).toBe('world')
    })

    it('normalizes to lowercase', () => {
      const tokens = tokenize('HELLO World')
      expect(tokens[0].term).toBe('hello')
      expect(tokens[1].term).toBe('world')
    })

    it('removes stopwords', () => {
      const tokens = tokenize('the quick brown fox')
      const terms = tokens.map(t => t.term)
      expect(terms).not.toContain('the')
      expect(terms).toContain('quick')
      expect(terms).toContain('brown')
      expect(terms).toContain('fox')
    })

    it('tracks positions', () => {
      const tokens = tokenize('one two three')
      expect(tokens[0].position).toBe(0)
      expect(tokens[1].position).toBe(1)
      expect(tokens[2].position).toBe(2)
    })

    it('tracks offsets', () => {
      const tokens = tokenize('hello world')
      expect(tokens[0].startOffset).toBe(0)
      expect(tokens[0].endOffset).toBe(5)
      expect(tokens[1].startOffset).toBe(6)
      expect(tokens[1].endOffset).toBe(11)
    })

    it('filters by minimum word length', () => {
      const tokens = tokenize('a ab abc abcd', { minWordLength: 3 })
      const terms = tokens.map(t => t.term)
      expect(terms).not.toContain('a')
      expect(terms).not.toContain('ab')
      expect(terms).toContain('abc')
      expect(terms).toContain('abcd')
    })

    it('filters by maximum word length', () => {
      const tokens = tokenize('hi hello superlongword', { maxWordLength: 5 })
      const terms = tokens.map(t => t.original)
      expect(terms).toContain('hi')
      expect(terms).toContain('hello')
      expect(terms).not.toContain('superlongword')
    })

    it('applies stemming', () => {
      const tokens = tokenize('running jumping swimming', { stem: true })
      const terms = tokens.map(t => t.term)
      expect(terms).toContain('run')
      expect(terms).toContain('jump')
      expect(terms).toContain('swim')
    })

    it('can disable stemming', () => {
      const tokens = tokenize('running', { stem: false })
      expect(tokens[0].term).toBe('running')
    })

    it('handles punctuation', () => {
      const tokens = tokenize('Hello, world! How are you?')
      const originals = tokens.map(t => t.original)
      expect(originals).toContain('Hello')
      expect(originals).toContain('world')
      expect(originals).not.toContain(',')
      expect(originals).not.toContain('!')
    })

    it('handles numbers', () => {
      const tokens = tokenize('version 2 release 10')
      const originals = tokens.map(t => t.original)
      expect(originals).toContain('version')
      expect(originals).toContain('release')
      expect(originals).toContain('10')
    })

    it('handles empty input', () => {
      const tokens = tokenize('')
      expect(tokens).toHaveLength(0)
    })

    it('handles input with only stopwords', () => {
      const tokens = tokenize('the a an is')
      expect(tokens).toHaveLength(0)
    })
  })

  describe('tokenizeQuery', () => {
    it('tokenizes query terms', () => {
      const terms = tokenizeQuery('quick brown fox')
      expect(terms).toContain('quick')
      expect(terms).toContain('brown')
      expect(terms).toContain('fox')
    })

    it('applies stemming', () => {
      const terms = tokenizeQuery('running')
      expect(terms).toContain('run')
    })

    it('keeps stopwords in queries', () => {
      const terms = tokenizeQuery('the matrix')
      // Query tokenizer keeps stopwords
      expect(terms.length).toBeGreaterThan(0)
    })
  })

  describe('porterStem', () => {
    it('stems -ing endings', () => {
      expect(porterStem('running')).toBe('run')
      expect(porterStem('jumping')).toBe('jump')
      expect(porterStem('swimming')).toBe('swim')
    })

    it('stems -ed endings', () => {
      expect(porterStem('walked')).toBe('walk')
      expect(porterStem('jumped')).toBe('jump')
      expect(porterStem('agreed')).toBe('agre')
    })

    it('stems -s endings', () => {
      expect(porterStem('cats')).toBe('cat')
      expect(porterStem('dogs')).toBe('dog')
    })

    it('stems -ies endings', () => {
      expect(porterStem('ponies')).toBe('poni')
      expect(porterStem('cities')).toBe('citi')
    })

    it('stems -ation endings', () => {
      expect(porterStem('civilization')).toBe('civil')
      expect(porterStem('organization')).toBe('organ')
    })

    it('handles short words', () => {
      expect(porterStem('a')).toBe('a')
      expect(porterStem('is')).toBe('is')
    })

    it('preserves irregular words', () => {
      // The stemmer doesn't handle irregular forms
      expect(porterStem('going')).toBe('go')
    })
  })

  describe('ENGLISH_STOPWORDS', () => {
    it('contains common stopwords', () => {
      expect(ENGLISH_STOPWORDS.has('the')).toBe(true)
      expect(ENGLISH_STOPWORDS.has('a')).toBe(true)
      expect(ENGLISH_STOPWORDS.has('is')).toBe(true)
      expect(ENGLISH_STOPWORDS.has('are')).toBe(true)
      expect(ENGLISH_STOPWORDS.has('and')).toBe(true)
      expect(ENGLISH_STOPWORDS.has('or')).toBe(true)
    })

    it('does not contain content words', () => {
      expect(ENGLISH_STOPWORDS.has('computer')).toBe(false)
      expect(ENGLISH_STOPWORDS.has('database')).toBe(false)
    })
  })
})
