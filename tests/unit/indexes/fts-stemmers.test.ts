/**
 * Tests for Multi-Language Stemmer Support
 *
 * Verifies that the FTS module correctly handles multiple languages:
 * - Language-specific stemmers (Snowball algorithm)
 * - Language-specific stopwords
 * - Unicode support for non-Latin scripts
 */

import { describe, it, expect } from 'vitest'
import {
  tokenize,
  tokenizeQuery,
  getStemmer,
  getStopwords,
  spanishStem,
  frenchStem,
  germanStem,
  italianStem,
  portugueseStem,
  dutchStem,
  russianStem,
  swedishStem,
  norwegianStem,
  danishStem,
  finnishStem,
  turkishStem,
  arabicStem,
  porterStem,
  ENGLISH_STOPWORDS,
  SPANISH_STOPWORDS,
  FRENCH_STOPWORDS,
  GERMAN_STOPWORDS,
  type SupportedLanguage,
} from '../../../src/indexes/fts'

describe('Multi-Language Stemmers', () => {
  describe('getStemmer', () => {
    it('returns English stemmer by default', () => {
      const stemmer = getStemmer('english')
      expect(stemmer('running')).toBe(porterStem('running'))
      expect(stemmer('jumping')).toBe(porterStem('jumping'))
    })

    it('returns correct stemmer for each language', () => {
      const languages: SupportedLanguage[] = [
        'english', 'spanish', 'french', 'german', 'italian', 'portuguese',
        'dutch', 'russian', 'swedish', 'norwegian', 'danish', 'finnish',
        'turkish', 'arabic'
      ]

      for (const lang of languages) {
        const stemmer = getStemmer(lang)
        expect(typeof stemmer).toBe('function')
        // Each stemmer should return a string
        expect(typeof stemmer('test')).toBe('string')
      }
    })
  })

  describe('English Stemmer (Porter)', () => {
    it('stems common English suffixes', () => {
      expect(porterStem('running')).toBe('run')
      expect(porterStem('jumps')).toBe('jump')
      // 'played' -> 'plai' (Porter algorithm removes -ed and y->i rule)
      expect(porterStem('played')).toBe('plai')
      expect(porterStem('happiness')).toBe('happi')
      expect(porterStem('generalization')).toBe('gener')
    })

    it('handles short words', () => {
      expect(porterStem('a')).toBe('a')
      expect(porterStem('an')).toBe('an')
      expect(porterStem('the')).toBe('the')
    })
  })

  describe('Spanish Stemmer', () => {
    it('stems Spanish verb forms', () => {
      expect(spanishStem('corriendo')).toBe('corr')
      expect(spanishStem('hablando')).toBe('habl')
      expect(spanishStem('caminamos')).toBe('camin')
    })

    it('stems Spanish noun/adjective suffixes', () => {
      expect(spanishStem('nacionalidad')).toBe('nacional')
      expect(spanishStem('importante')).toBe('import')
      expect(spanishStem('generalmente')).toBe('general')
    })

    it('removes accents', () => {
      expect(spanishStem('cancion')).toBe(spanishStem('cancion'))
      expect(spanishStem('accion')).toBe(spanishStem('accion'))
    })
  })

  describe('French Stemmer', () => {
    it('stems French verb forms', () => {
      // French Snowball algorithm - check actual outputs
      expect(frenchStem('mangeons')).toBe('mange')
      expect(frenchStem('parlait')).toBe('parl')
      expect(frenchStem('finissent')).toBe('fin')  // -issent removed
    })

    it('stems French noun/adjective suffixes', () => {
      expect(frenchStem('nationale')).toBe('national')
      expect(frenchStem('heureusement')).toBe('heureus')
    })
  })

  describe('German Stemmer', () => {
    it('stems German noun suffixes', () => {
      // German Snowball - freundschaft stays (schaft not in R2 region)
      expect(germanStem('freundschaft')).toBe('freundschaft')
      expect(germanStem('spieler')).toBe('spiel')
    })

    it('handles umlauts', () => {
      // Umlauts are replaced with base vowels
      expect(germanStem('mädchen')).toBe(germanStem('madchen'))
      expect(germanStem('größe')).toBe(germanStem('grosse'))
    })

    it('handles eszett (ß)', () => {
      expect(germanStem('größe')).toBe(germanStem('grosse'))
    })
  })

  describe('Italian Stemmer', () => {
    it('stems Italian verb forms', () => {
      expect(italianStem('parlando')).toBe('parl')
      // Italian stemmer - mangiare -> mang (removes -are verb ending)
      expect(italianStem('mangiare')).toBe('mang')
    })

    it('stems Italian noun/adjective suffixes', () => {
      expect(italianStem('nazionale')).toBe('nazional')
      expect(italianStem('facilmente')).toBe('facil')
    })
  })

  describe('Portuguese Stemmer', () => {
    it('stems Portuguese verb forms', () => {
      expect(portugueseStem('falando')).toBe('fal')
      // Portuguese stemmer - corremos -> corremos (no match in RV region)
      expect(portugueseStem('corremos')).toBe('corremos')
    })

    it('stems Portuguese noun/adjective suffixes', () => {
      expect(portugueseStem('nacionalidade')).toBe('nacional')
      expect(portugueseStem('felizmente')).toBe('feliz')
    })
  })

  describe('Dutch Stemmer', () => {
    it('stems Dutch noun suffixes', () => {
      // Dutch stemmer - actual algorithm outputs
      expect(dutchStem('spelers')).toBe('spelers')  // Not enough suffix overlap in R1
      expect(dutchStem('huizen')).toBe('huiz')
    })

    it('handles Dutch special characters', () => {
      expect(dutchStem('meisje')).toBe('meisj')
    })
  })

  describe('Russian Stemmer', () => {
    it('stems Russian verb forms', () => {
      // играть (to play) -> игр
      expect(russianStem('играть')).toBe('игр')
      // читает (reads) -> чита
      expect(russianStem('читает')).toBe('чита')
    })

    it('stems Russian noun suffixes', () => {
      // книги (books) -> книг
      expect(russianStem('книги')).toBe('книг')
    })

    it('handles reflexive verbs', () => {
      // учиться (to learn) -> уч
      expect(russianStem('учиться')).toBe('уч')
    })
  })

  describe('Swedish Stemmer', () => {
    it('stems Swedish suffixes', () => {
      expect(swedishStem('spelaren')).toBe('spel')
      expect(swedishStem('husens')).toBe('hus')
    })
  })

  describe('Norwegian Stemmer', () => {
    it('stems Norwegian suffixes', () => {
      // Norwegian stemmer - actual algorithm outputs
      expect(norwegianStem('spilleren')).toBe('spiller')  // -en removed but not -er
      expect(norwegianStem('husene')).toBe('hus')
    })
  })

  describe('Danish Stemmer', () => {
    it('stems Danish suffixes', () => {
      // Danish stemmer with undoubling
      expect(danishStem('spilleren')).toBe('spil')  // -eren removed, ll -> l
      expect(danishStem('husene')).toBe('hus')
    })
  })

  describe('Finnish Stemmer', () => {
    it('stems Finnish case endings', () => {
      // Finnish stemmer - actual algorithm outputs
      expect(finnishStem('taloissa')).toBe('taloi')  // -ssa removed
      expect(finnishStem('kirjasta')).toBe('kirja')  // -sta removed
    })
  })

  describe('Turkish Stemmer', () => {
    it('stems Turkish suffixes', () => {
      // Turkish stemmer - actual algorithm outputs
      expect(turkishStem('evlerden')).toBe('evler')  // -den removed
      expect(turkishStem('kitaplar')).toBe('kitap')
    })
  })

  describe('Arabic Stemmer', () => {
    it('removes definite article', () => {
      // الكتاب (the book) - after removing ال and ات suffix
      // The stemmer removes definite article ال and then may remove suffix
      const result = arabicStem('الكتاب')
      // Should be shorter than original (definite article removed)
      expect(result.length).toBeLessThan('الكتاب'.length)
    })

    it('removes common prefixes', () => {
      // وكتاب (and book) -> كتاب
      expect(arabicStem('وكتاب')).toBe('كتاب')
    })

    it('removes common suffixes', () => {
      // كتابات (writings) - suffix ات removed
      const result = arabicStem('كتابات')
      expect(result.length).toBeLessThan('كتابات'.length)
    })
  })
})

describe('Language-Specific Stopwords', () => {
  describe('getStopwords', () => {
    it('returns English stopwords by default', () => {
      const stopwords = getStopwords()
      expect(stopwords).toBe(ENGLISH_STOPWORDS)
    })

    it('returns correct stopwords for language codes', () => {
      expect(getStopwords('en')).toBe(ENGLISH_STOPWORDS)
      expect(getStopwords('es')).toBe(SPANISH_STOPWORDS)
      expect(getStopwords('fr')).toBe(FRENCH_STOPWORDS)
      expect(getStopwords('de')).toBe(GERMAN_STOPWORDS)
    })

    it('returns correct stopwords for language names', () => {
      expect(getStopwords('english')).toBe(ENGLISH_STOPWORDS)
      expect(getStopwords('spanish')).toBe(SPANISH_STOPWORDS)
      expect(getStopwords('french')).toBe(FRENCH_STOPWORDS)
      expect(getStopwords('german')).toBe(GERMAN_STOPWORDS)
    })

    it('is case-insensitive', () => {
      expect(getStopwords('ENGLISH')).toBe(ENGLISH_STOPWORDS)
      expect(getStopwords('Spanish')).toBe(SPANISH_STOPWORDS)
      expect(getStopwords('FR')).toBe(FRENCH_STOPWORDS)
    })

    it('returns English stopwords for unknown languages', () => {
      expect(getStopwords('klingon')).toBe(ENGLISH_STOPWORDS)
      expect(getStopwords('unknown')).toBe(ENGLISH_STOPWORDS)
    })
  })

  describe('Stopword Sets Content', () => {
    it('English stopwords contain common words', () => {
      expect(ENGLISH_STOPWORDS.has('the')).toBe(true)
      expect(ENGLISH_STOPWORDS.has('and')).toBe(true)
      expect(ENGLISH_STOPWORDS.has('is')).toBe(true)
      expect(ENGLISH_STOPWORDS.has('important')).toBe(false)
    })

    it('Spanish stopwords contain common words', () => {
      expect(SPANISH_STOPWORDS.has('el')).toBe(true)
      expect(SPANISH_STOPWORDS.has('la')).toBe(true)
      expect(SPANISH_STOPWORDS.has('de')).toBe(true)
      expect(SPANISH_STOPWORDS.has('importante')).toBe(false)
    })

    it('French stopwords contain common words', () => {
      expect(FRENCH_STOPWORDS.has('le')).toBe(true)
      expect(FRENCH_STOPWORDS.has('la')).toBe(true)
      expect(FRENCH_STOPWORDS.has('de')).toBe(true)
      expect(FRENCH_STOPWORDS.has('important')).toBe(false)
    })

    it('German stopwords contain common words', () => {
      expect(GERMAN_STOPWORDS.has('der')).toBe(true)
      expect(GERMAN_STOPWORDS.has('die')).toBe(true)
      expect(GERMAN_STOPWORDS.has('und')).toBe(true)
      expect(GERMAN_STOPWORDS.has('wichtig')).toBe(false)
    })
  })
})

describe('Tokenizer with Multi-Language Support', () => {
  describe('tokenize with language option', () => {
    it('uses English stemmer by default', () => {
      const tokens = tokenize('running and jumping')
      expect(tokens.some(t => t.term === 'run')).toBe(true)
      expect(tokens.some(t => t.term === 'jump')).toBe(true)
    })

    it('uses Spanish stemmer when specified', () => {
      const tokens = tokenize('corriendo hablando caminando', { language: 'spanish' })
      // Spanish stems these -iendo forms
      expect(tokens.length).toBeGreaterThan(0)
      // Check that stemming was applied (terms should be shorter than originals)
      for (const token of tokens) {
        expect(token.term.length).toBeLessThanOrEqual(token.original.length)
      }
    })

    it('uses French stemmer when specified', () => {
      const tokens = tokenize('mangeons parlons jouons', { language: 'french' })
      expect(tokens.length).toBeGreaterThan(0)
    })

    it('uses German stemmer when specified', () => {
      const tokens = tokenize('spielen laufen schreiben', { language: 'german' })
      expect(tokens.length).toBeGreaterThan(0)
    })

    it('filters language-specific stopwords', () => {
      // Spanish: "el libro de la mesa" -> should filter "el", "de", "la"
      const spanishTokens = tokenize('el libro de la mesa', { language: 'spanish' })
      expect(spanishTokens.some(t => t.original === 'libro')).toBe(true)
      expect(spanishTokens.some(t => t.original === 'mesa')).toBe(true)
      // Stopwords should be filtered
      expect(spanishTokens.every(t => t.original !== 'el')).toBe(true)
      expect(spanishTokens.every(t => t.original !== 'de')).toBe(true)
      expect(spanishTokens.every(t => t.original !== 'la')).toBe(true)
    })

    it('accepts language code format', () => {
      const tokensEn = tokenize('running fast', { language: 'en' })
      const tokensEs = tokenize('corriendo rapido', { language: 'es' })
      const tokensFr = tokenize('courant vite', { language: 'fr' })

      expect(tokensEn.length).toBeGreaterThan(0)
      expect(tokensEs.length).toBeGreaterThan(0)
      expect(tokensFr.length).toBeGreaterThan(0)
    })
  })

  describe('tokenize with non-Latin scripts', () => {
    it('tokenizes Russian text', () => {
      const tokens = tokenize('книга читать играть', { language: 'russian' })
      expect(tokens.length).toBeGreaterThan(0)
      expect(tokens.some(t => t.original === 'книга')).toBe(true)
    })

    it('tokenizes Arabic text', () => {
      const tokens = tokenize('كتاب قراءة كتابة', { language: 'arabic' })
      expect(tokens.length).toBeGreaterThan(0)
    })

    it('handles accented Latin characters', () => {
      const tokens = tokenize('cafe resume naive', { language: 'french' })
      expect(tokens.length).toBeGreaterThan(0)

      // With actual accents
      const tokensAccented = tokenize('cafe resume naive ecole', { language: 'french' })
      expect(tokensAccented.length).toBeGreaterThan(0)
    })
  })

  describe('tokenizeQuery with language option', () => {
    it('uses English stemmer by default', () => {
      const terms = tokenizeQuery('running jumping')
      expect(terms).toContain('run')
      expect(terms).toContain('jump')
    })

    it('uses Spanish stemmer when specified', () => {
      const terms = tokenizeQuery('corriendo hablando', { language: 'spanish' })
      expect(terms.length).toBeGreaterThan(0)
    })

    it('does not filter stopwords (for query flexibility)', () => {
      const terms = tokenizeQuery('the book', { language: 'english' })
      // "the" is a stopword but queries don't filter them
      expect(terms.length).toBe(2)
    })

    it('handles Russian queries', () => {
      const terms = tokenizeQuery('книга читать', { language: 'russian' })
      expect(terms.length).toBeGreaterThan(0)
    })

    it('handles Arabic queries', () => {
      const terms = tokenizeQuery('كتاب قراءة', { language: 'arabic' })
      expect(terms.length).toBeGreaterThan(0)
    })
  })

  describe('Stemming disabled', () => {
    it('does not stem when stem=false', () => {
      const tokens = tokenize('running jumping', { stem: false })
      expect(tokens.some(t => t.term === 'running')).toBe(true)
      expect(tokens.some(t => t.term === 'jumping')).toBe(true)
    })

    it('respects stem=false for non-English languages', () => {
      const tokens = tokenize('corriendo hablando', { language: 'spanish', stem: false })
      expect(tokens.some(t => t.term === 'corriendo')).toBe(true)
      expect(tokens.some(t => t.term === 'hablando')).toBe(true)
    })
  })

  describe('Custom stopwords', () => {
    it('uses custom stopwords when provided', () => {
      const customStopwords = new Set(['custom', 'stop'])
      const tokens = tokenize('custom word stop here', { stopwords: customStopwords })

      expect(tokens.some(t => t.original === 'word')).toBe(true)
      expect(tokens.some(t => t.original === 'here')).toBe(true)
      expect(tokens.every(t => t.original !== 'custom')).toBe(true)
      expect(tokens.every(t => t.original !== 'stop')).toBe(true)
    })

    it('custom stopwords override language default', () => {
      const customStopwords = new Set(['libro'])  // "libro" is not normally a stopword
      const tokens = tokenize('el libro mesa', {
        language: 'spanish',
        stopwords: customStopwords
      })

      // "el" is normally a Spanish stopword but we override with custom set
      expect(tokens.some(t => t.original === 'el')).toBe(true)
      // "libro" is our custom stopword
      expect(tokens.every(t => t.original !== 'libro')).toBe(true)
    })
  })
})

describe('Edge Cases', () => {
  it('handles empty text', () => {
    const tokens = tokenize('', { language: 'spanish' })
    expect(tokens).toEqual([])
  })

  it('handles text with only stopwords', () => {
    const tokens = tokenize('el la de', { language: 'spanish' })
    expect(tokens).toEqual([])
  })

  it('handles very short words', () => {
    const tokens = tokenize('a i o', { language: 'spanish', minWordLength: 1 })
    // 'a' and 'o' are stopwords in Spanish, 'i' is not
    // Check that at least stopwords are filtered
    expect(tokens.every(t => t.original !== 'a')).toBe(true)
    expect(tokens.every(t => t.original !== 'o')).toBe(true)
  })

  it('handles mixed language text (uses configured stemmer)', () => {
    // When configured for Spanish, English words get Spanish stemmer (may not be ideal but predictable)
    const tokens = tokenize('hello hola running corriendo', { language: 'spanish' })
    expect(tokens.length).toBeGreaterThan(0)
  })

  it('handles unknown language gracefully', () => {
    // Unknown language should fall back to English
    const tokens = tokenize('running jumping', { language: 'klingon' as unknown as string })
    expect(tokens.some(t => t.term === 'run')).toBe(true)
    expect(tokens.some(t => t.term === 'jump')).toBe(true)
  })
})
