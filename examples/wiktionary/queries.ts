/**
 * Wiktionary Query Functions
 *
 * Comprehensive query API for Wiktionary data stored in ParqueDB:
 * - Word and definition lookups
 * - Translations and cross-language features
 * - Pronunciations and rhymes
 * - Word relationships (synonyms, antonyms, etc.)
 * - Etymology chains and cognates
 * - Inflected forms
 * - Collocations
 * - Word frequency data
 *
 * @see ./schema.ts for entity definitions
 * @see ./load.ts for data loading
 */

import type { StorageBackend } from '../../src/types/storage'
import { ParquetReader } from '../../src/parquet/reader'
import {
  type WordEntity,
  type DefinitionEntity,
  type PronunciationEntity,
  type TranslationEntity,
  type RelatedWordEntity,
  type EtymologyEntity,
  type FormEntity,
  type DescendantEntity,
  type CompoundEntity,
  type CollocationEntity,
  type CognateEntity,
  type EtymologyLinkEntity,
  type WordFrequencyEntity,
  type RelationType,
  type EtymologyLinkType,
  WIKTIONARY_PATHS,
} from './schema'

// =============================================================================
// Query Helper Functions
// =============================================================================

/**
 * Get the first letter of a word for partition lookup
 */
function getFirstLetter(word: string): string {
  const first = word.charAt(0).toLowerCase()
  if (!/[a-z\u00C0-\u024F]/.test(first)) {
    return '#'
  }
  return first
}

/**
 * URL-safe slug for ID matching
 */
function slugify(text: string): string {
  return encodeURIComponent(text.toLowerCase().replace(/\s+/g, '_'))
}

// =============================================================================
// Word Queries
// =============================================================================

/**
 * Look up a word by its exact form
 *
 * @example
 * ```typescript
 * const results = await lookupWord(storage, 'en', 'run')
 * // Returns all parts of speech for "run" (noun, verb, etc.)
 * ```
 */
export async function lookupWord(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<WordEntity[]> {
  const reader = new ParquetReader({ storage })
  const letter = getFirstLetter(word)
  const path = WIKTIONARY_PATHS.words(languageCode, letter)

  try {
    const allWords = await reader.read<WordEntity>(path)
    return allWords.filter(w => w.word.toLowerCase() === word.toLowerCase())
  } catch (e) {
    // File might not exist if no words with this letter
    return []
  }
}

/**
 * Look up a specific word with a specific part of speech
 *
 * @example
 * ```typescript
 * const verb = await lookupWordPos(storage, 'en', 'run', 'verb')
 * ```
 */
export async function lookupWordPos(
  storage: StorageBackend,
  languageCode: string,
  word: string,
  pos: string
): Promise<WordEntity | null> {
  const results = await lookupWord(storage, languageCode, word)
  return results.find(w => w.pos === pos) ?? null
}

/**
 * Search for words matching a prefix
 *
 * @example
 * ```typescript
 * const words = await searchWordsPrefix(storage, 'en', 'run', 10)
 * // Returns: run, runaway, runner, running, runway, etc.
 * ```
 */
export async function searchWordsPrefix(
  storage: StorageBackend,
  languageCode: string,
  prefix: string,
  limit: number = 100
): Promise<WordEntity[]> {
  const reader = new ParquetReader({ storage })
  const letter = getFirstLetter(prefix)
  const path = WIKTIONARY_PATHS.words(languageCode, letter)
  const lowerPrefix = prefix.toLowerCase()

  try {
    const allWords = await reader.read<WordEntity>(path)
    return allWords
      .filter(w => w.word.toLowerCase().startsWith(lowerPrefix))
      .slice(0, limit)
  } catch (e) {
    return []
  }
}

/**
 * Get words by part of speech
 *
 * @example
 * ```typescript
 * const verbs = await getWordsByPos(storage, 'en', 'a', 'verb', 100)
 * ```
 */
export async function getWordsByPos(
  storage: StorageBackend,
  languageCode: string,
  letter: string,
  pos: string,
  limit: number = 100
): Promise<WordEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.words(languageCode, letter)

  try {
    const allWords = await reader.read<WordEntity>(path)
    return allWords.filter(w => w.pos === pos).slice(0, limit)
  } catch (e) {
    return []
  }
}

// =============================================================================
// Definition Queries
// =============================================================================

/**
 * Get all definitions for a word
 *
 * @example
 * ```typescript
 * const defs = await getDefinitions(storage, 'en', 'run')
 * // Returns all senses across all parts of speech
 * ```
 */
export async function getDefinitions(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<DefinitionEntity[]> {
  const reader = new ParquetReader({ storage })
  const letter = getFirstLetter(word)
  const path = WIKTIONARY_PATHS.definitions(languageCode, letter)

  try {
    const allDefs = await reader.read<DefinitionEntity>(path)
    return allDefs.filter(d => d.word.toLowerCase() === word.toLowerCase())
  } catch (e) {
    return []
  }
}

/**
 * Get definitions for a specific word and part of speech
 *
 * @example
 * ```typescript
 * const verbDefs = await getDefinitionsForPos(storage, 'en', 'run', 'verb')
 * ```
 */
export async function getDefinitionsForPos(
  storage: StorageBackend,
  languageCode: string,
  word: string,
  pos: string
): Promise<DefinitionEntity[]> {
  const defs = await getDefinitions(storage, languageCode, word)
  return defs.filter(d => d.pos === pos)
}

/**
 * Search definitions by keyword
 *
 * @example
 * ```typescript
 * const defs = await searchDefinitions(storage, 'en', 'a', 'quickly')
 * // Returns definitions containing "quickly"
 * ```
 */
export async function searchDefinitions(
  storage: StorageBackend,
  languageCode: string,
  letter: string,
  keyword: string,
  limit: number = 100
): Promise<DefinitionEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.definitions(languageCode, letter)
  const lowerKeyword = keyword.toLowerCase()

  try {
    const allDefs = await reader.read<DefinitionEntity>(path)
    return allDefs
      .filter(d => d.gloss.toLowerCase().includes(lowerKeyword))
      .slice(0, limit)
  } catch (e) {
    return []
  }
}

// =============================================================================
// Form (Inflection) Queries
// =============================================================================

/**
 * Get all inflected forms for a word (plurals, conjugations, declensions)
 *
 * @example
 * ```typescript
 * const forms = await getWordForms(storage, 'en', 'run')
 * // Returns: runs, ran, running, etc. with grammatical tags
 * ```
 */
export async function getWordForms(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<FormEntity[]> {
  const reader = new ParquetReader({ storage })
  const letter = getFirstLetter(word)
  const path = WIKTIONARY_PATHS.forms(languageCode, letter)

  try {
    const allForms = await reader.read<FormEntity>(path)
    return allForms.filter(f => f.word.toLowerCase() === word.toLowerCase())
  } catch (e) {
    return []
  }
}

/**
 * Get forms filtered by grammatical tags
 *
 * @example
 * ```typescript
 * const pastForms = await getFormsByTags(storage, 'en', 'run', ['past'])
 * // Returns: ran
 * ```
 */
export async function getFormsByTags(
  storage: StorageBackend,
  languageCode: string,
  word: string,
  tags: string[]
): Promise<FormEntity[]> {
  const forms = await getWordForms(storage, languageCode, word)
  return forms.filter(f =>
    tags.every(tag => f.tags?.includes(tag))
  )
}

/**
 * Find the base word for an inflected form (reverse lookup)
 *
 * @example
 * ```typescript
 * const base = await findBaseWord(storage, 'en', 'running')
 * // Returns the form entity linking "running" to "run"
 * ```
 */
export async function findBaseWord(
  storage: StorageBackend,
  languageCode: string,
  inflectedForm: string
): Promise<FormEntity[]> {
  const reader = new ParquetReader({ storage })
  const letter = getFirstLetter(inflectedForm)
  const path = WIKTIONARY_PATHS.forms(languageCode, letter)

  try {
    const allForms = await reader.read<FormEntity>(path)
    return allForms.filter(f => f.form.toLowerCase() === inflectedForm.toLowerCase())
  } catch (e) {
    return []
  }
}

// =============================================================================
// Pronunciation Queries
// =============================================================================

/**
 * Get pronunciations for a word
 *
 * @example
 * ```typescript
 * const prons = await getPronunciations(storage, 'en', 'tomato')
 * // Returns IPA, audio links, etc.
 * ```
 */
export async function getPronunciations(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<PronunciationEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.pronunciations(languageCode)

  try {
    const allProns = await reader.read<PronunciationEntity>(path)
    return allProns.filter(p => p.word.toLowerCase() === word.toLowerCase())
  } catch (e) {
    return []
  }
}

/**
 * Get words with audio recordings
 *
 * @example
 * ```typescript
 * const wordsWithAudio = await getWordsWithAudio(storage, 'en', 100)
 * ```
 */
export async function getWordsWithAudio(
  storage: StorageBackend,
  languageCode: string,
  limit: number = 100
): Promise<PronunciationEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.pronunciations(languageCode)

  try {
    const allProns = await reader.read<PronunciationEntity>(path)
    return allProns
      .filter(p => p.audioOgg || p.audioMp3)
      .slice(0, limit)
  } catch (e) {
    return []
  }
}

/**
 * Find words that rhyme with a given word
 *
 * @example
 * ```typescript
 * const rhymes = await findRhymes(storage, 'en', 'day')
 * // Returns words like: say, way, play, etc.
 * ```
 */
export async function findRhymes(
  storage: StorageBackend,
  languageCode: string,
  word: string,
  limit: number = 100
): Promise<PronunciationEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.pronunciations(languageCode)

  try {
    // First, get the rhyme pattern for the input word
    const allProns = await reader.read<PronunciationEntity>(path)
    const wordPron = allProns.find(p =>
      p.word.toLowerCase() === word.toLowerCase() && p.rhymes
    )

    if (!wordPron?.rhymes) {
      // If no rhyme pattern stored, try to match by IPA ending
      const targetPron = allProns.find(p =>
        p.word.toLowerCase() === word.toLowerCase() && p.ipa
      )

      if (targetPron?.ipa) {
        // Extract the rhyme-relevant part (last vowel sound onward)
        const ipaEnding = targetPron.ipa.slice(-4)
        return allProns
          .filter(p =>
            p.ipa?.endsWith(ipaEnding) &&
            p.word.toLowerCase() !== word.toLowerCase()
          )
          .slice(0, limit)
      }

      return []
    }

    // Find all words with the same rhyme pattern
    return allProns
      .filter(p =>
        p.rhymes === wordPron.rhymes &&
        p.word.toLowerCase() !== word.toLowerCase()
      )
      .slice(0, limit)
  } catch (e) {
    return []
  }
}

/**
 * Find rhyming words by IPA rhyme pattern
 *
 * @example
 * ```typescript
 * const rhymes = await findRhymesByPattern(storage, 'en', '-eɪ')
 * // Returns words rhyming with "day", "say", etc.
 * ```
 */
export async function findRhymesByPattern(
  storage: StorageBackend,
  languageCode: string,
  rhymePattern: string,
  limit: number = 100
): Promise<PronunciationEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.pronunciations(languageCode)

  try {
    const allProns = await reader.read<PronunciationEntity>(path)
    return allProns
      .filter(p => p.rhymes === rhymePattern)
      .slice(0, limit)
  } catch (e) {
    return []
  }
}

// =============================================================================
// Translation Queries
// =============================================================================

/**
 * Get translations of a word to a specific language
 *
 * @example
 * ```typescript
 * const spanishTranslations = await getTranslations(storage, 'en', 'es', 'hello')
 * // Returns Spanish translations of "hello"
 * ```
 */
export async function getTranslations(
  storage: StorageBackend,
  sourceLanguageCode: string,
  targetLanguageCode: string,
  word: string
): Promise<TranslationEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.translations(sourceLanguageCode, targetLanguageCode)

  try {
    const allTrans = await reader.read<TranslationEntity>(path)
    return allTrans.filter(t => t.sourceWord.toLowerCase() === word.toLowerCase())
  } catch (e) {
    return []
  }
}

/**
 * Get all translations of a word to any language
 *
 * Note: This requires scanning all translation files, which can be slow.
 * Consider caching language pairs or using a different query strategy.
 *
 * @example
 * ```typescript
 * const allTranslations = await getAllTranslations(storage, 'en', 'hello')
 * ```
 */
export async function getAllTranslations(
  storage: StorageBackend,
  sourceLanguageCode: string,
  word: string
): Promise<Map<string, TranslationEntity[]>> {
  const results = new Map<string, TranslationEntity[]>()

  // List translation files for this source language
  const result = await storage.list(`wiktionary/translations/src=${sourceLanguageCode}/`)

  for (const file of result.files) {
    // Extract target language from path
    const match = file.match(/tgt=([^/]+)/)
    if (!match) continue
    const targetLang = match[1]

    const translations = await getTranslations(storage, sourceLanguageCode, targetLang, word)
    if (translations.length > 0) {
      results.set(targetLang, translations)
    }
  }

  return results
}

/**
 * Reverse translation lookup - find words that translate to a given word
 *
 * @example
 * ```typescript
 * const sources = await reverseTranslationLookup(storage, 'en', 'es', 'hola')
 * // Returns English words that translate to "hola"
 * ```
 */
export async function reverseTranslationLookup(
  storage: StorageBackend,
  sourceLanguageCode: string,
  targetLanguageCode: string,
  targetWord: string
): Promise<TranslationEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.translations(sourceLanguageCode, targetLanguageCode)

  try {
    const allTrans = await reader.read<TranslationEntity>(path)
    return allTrans.filter(t => t.targetWord.toLowerCase() === targetWord.toLowerCase())
  } catch (e) {
    return []
  }
}

/**
 * Compare translations of a word across two languages
 *
 * @example
 * ```typescript
 * const comparison = await compareTranslations(storage, 'water', 'en', 'es', 'fr')
 * // Returns { es: [...spanish translations], fr: [...french translations] }
 * ```
 */
export async function compareTranslations(
  storage: StorageBackend,
  word: string,
  sourceLanguageCode: string,
  targetLang1: string,
  targetLang2: string
): Promise<{
  lang1: TranslationEntity[]
  lang2: TranslationEntity[]
  common: string[]
}> {
  const [trans1, trans2] = await Promise.all([
    getTranslations(storage, sourceLanguageCode, targetLang1, word),
    getTranslations(storage, sourceLanguageCode, targetLang2, word),
  ])

  // Find common senses being translated
  const senses1 = new Set(trans1.map(t => t.sense).filter(Boolean))
  const senses2 = new Set(trans2.map(t => t.sense).filter(Boolean))
  const common = Array.from(senses1).filter(s => senses2.has(s)) as string[]

  return {
    lang1: trans1,
    lang2: trans2,
    common,
  }
}

/**
 * Build a translation graph for a word across multiple languages
 *
 * @example
 * ```typescript
 * const graph = await buildTranslationGraph(storage, 'water', 'en', ['es', 'fr', 'de', 'it'])
 * // Returns a map of language -> translations
 * ```
 */
export async function buildTranslationGraph(
  storage: StorageBackend,
  word: string,
  sourceLanguageCode: string,
  targetLanguages: string[]
): Promise<Map<string, TranslationEntity[]>> {
  const results = new Map<string, TranslationEntity[]>()

  await Promise.all(
    targetLanguages.map(async lang => {
      const translations = await getTranslations(storage, sourceLanguageCode, lang, word)
      if (translations.length > 0) {
        results.set(lang, translations)
      }
    })
  )

  return results
}

// =============================================================================
// Related Word Queries
// =============================================================================

/**
 * Get synonyms for a word
 *
 * @example
 * ```typescript
 * const synonyms = await getSynonyms(storage, 'en', 'happy')
 * // Returns: joyful, content, pleased, etc.
 * ```
 */
export async function getSynonyms(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<RelatedWordEntity[]> {
  return getRelatedWords(storage, languageCode, word, 'synonym')
}

/**
 * Get antonyms for a word
 *
 * @example
 * ```typescript
 * const antonyms = await getAntonyms(storage, 'en', 'happy')
 * // Returns: sad, unhappy, miserable, etc.
 * ```
 */
export async function getAntonyms(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<RelatedWordEntity[]> {
  return getRelatedWords(storage, languageCode, word, 'antonym')
}

/**
 * Get derived terms (words derived from this word)
 *
 * @example
 * ```typescript
 * const derived = await getDerivedTerms(storage, 'en', 'happy')
 * // Returns: happiness, happily, unhappy, etc.
 * ```
 */
export async function getDerivedTerms(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<RelatedWordEntity[]> {
  return getRelatedWords(storage, languageCode, word, 'derived')
}

/**
 * Get related words of a specific type
 *
 * @example
 * ```typescript
 * const hypernyms = await getRelatedWords(storage, 'en', 'dog', 'hypernym')
 * // Returns: animal, mammal, canine, etc.
 * ```
 */
export async function getRelatedWords(
  storage: StorageBackend,
  languageCode: string,
  word: string,
  relationType: RelationType
): Promise<RelatedWordEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.relatedWords(languageCode, relationType)

  try {
    const allRels = await reader.read<RelatedWordEntity>(path)
    return allRels.filter(r => r.word.toLowerCase() === word.toLowerCase())
  } catch (e) {
    return []
  }
}

/**
 * Get all semantic relationships for a word
 *
 * @example
 * ```typescript
 * const allRels = await getAllRelatedWords(storage, 'en', 'dog')
 * // Returns map of relation type to related words
 * ```
 */
export async function getAllRelatedWords(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<Map<RelationType, RelatedWordEntity[]>> {
  const results = new Map<RelationType, RelatedWordEntity[]>()
  const types: RelationType[] = [
    'synonym', 'antonym', 'hypernym', 'hyponym',
    'holonym', 'meronym', 'troponym', 'coordinate',
    'derived', 'related'
  ]

  await Promise.all(
    types.map(async relType => {
      const rels = await getRelatedWords(storage, languageCode, word, relType)
      if (rels.length > 0) {
        results.set(relType, rels)
      }
    })
  )

  return results
}

/**
 * Find words that have this word as a related term (reverse lookup)
 *
 * @example
 * ```typescript
 * const parents = await findRelatedTo(storage, 'en', 'happiness', 'derived')
 * // Returns words that have "happiness" as a derived term (e.g., "happy")
 * ```
 */
export async function findRelatedTo(
  storage: StorageBackend,
  languageCode: string,
  relatedWord: string,
  relationType: RelationType
): Promise<RelatedWordEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.relatedWords(languageCode, relationType)

  try {
    const allRels = await reader.read<RelatedWordEntity>(path)
    return allRels.filter(r => r.relatedWord.toLowerCase() === relatedWord.toLowerCase())
  } catch (e) {
    return []
  }
}

// =============================================================================
// Etymology Queries
// =============================================================================

/**
 * Get etymology for a word
 *
 * @example
 * ```typescript
 * const etymology = await getEtymology(storage, 'en', 'algorithm')
 * // Returns the origin story of "algorithm"
 * ```
 */
export async function getEtymology(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<EtymologyEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.etymologies(languageCode)

  try {
    const allEtyms = await reader.read<EtymologyEntity>(path)
    return allEtyms.filter(e => e.word.toLowerCase() === word.toLowerCase())
  } catch (e) {
    return []
  }
}

/**
 * Search etymologies for language of origin
 *
 * @example
 * ```typescript
 * const latinWords = await searchEtymologies(storage, 'en', 'Latin')
 * // Returns words derived from Latin
 * ```
 */
export async function searchEtymologies(
  storage: StorageBackend,
  languageCode: string,
  keyword: string,
  limit: number = 100
): Promise<EtymologyEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.etymologies(languageCode)
  const lowerKeyword = keyword.toLowerCase()

  try {
    const allEtyms = await reader.read<EtymologyEntity>(path)
    return allEtyms
      .filter(e => e.text.toLowerCase().includes(lowerKeyword))
      .slice(0, limit)
  } catch (e) {
    return []
  }
}

/**
 * Trace the full etymology chain for a word
 *
 * @example
 * ```typescript
 * const chain = await traceEtymology(storage, 'en', 'algorithm')
 * // Returns: algorithm <- Arabic: al-khwarizmi <- Persian: Khwarezm
 * ```
 */
export async function traceEtymology(
  storage: StorageBackend,
  languageCode: string,
  word: string,
  maxDepth: number = 10
): Promise<{
  word: string
  language: string
  chain: EtymologyLinkEntity[]
  etymology: EtymologyEntity | null
}> {
  const reader = new ParquetReader({ storage })

  // Get the main etymology text
  const etymologies = await getEtymology(storage, languageCode, word)
  const etymology = etymologies[0] ?? null

  // Get etymology links
  const path = WIKTIONARY_PATHS.etymologyLinks(languageCode)
  let chain: EtymologyLinkEntity[] = []

  try {
    const allLinks = await reader.read<EtymologyLinkEntity>(path)
    const wordLinks = allLinks
      .filter(l => l.word.toLowerCase() === word.toLowerCase())
      .sort((a, b) => a.distance - b.distance)

    chain = wordLinks.slice(0, maxDepth)
  } catch (e) {
    // No etymology links found
  }

  return {
    word,
    language: languageCode,
    chain,
    etymology,
  }
}

/**
 * Get etymology links by link type
 *
 * @example
 * ```typescript
 * const borrowed = await getEtymologyLinksByType(storage, 'en', 'borrowed')
 * // Returns all words borrowed from other languages
 * ```
 */
export async function getEtymologyLinksByType(
  storage: StorageBackend,
  languageCode: string,
  linkType: EtymologyLinkType,
  limit: number = 100
): Promise<EtymologyLinkEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.etymologyLinks(languageCode)

  try {
    const allLinks = await reader.read<EtymologyLinkEntity>(path)
    return allLinks
      .filter(l => l.linkType === linkType)
      .slice(0, limit)
  } catch (e) {
    return []
  }
}

/**
 * Find words borrowed from a specific language
 *
 * @example
 * ```typescript
 * const frenchLoans = await findBorrowedWords(storage, 'en', 'French')
 * // Returns English words borrowed from French
 * ```
 */
export async function findBorrowedWords(
  storage: StorageBackend,
  languageCode: string,
  sourceLanguage: string,
  limit: number = 100
): Promise<EtymologyLinkEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.etymologyLinks(languageCode)
  const lowerSource = sourceLanguage.toLowerCase()

  try {
    const allLinks = await reader.read<EtymologyLinkEntity>(path)
    return allLinks
      .filter(l =>
        (l.linkType === 'borrowed' || l.linkType === 'learned') &&
        l.ancestorLanguage.toLowerCase().includes(lowerSource)
      )
      .slice(0, limit)
  } catch (e) {
    return []
  }
}

// =============================================================================
// Cognate Queries
// =============================================================================

/**
 * Find cognates of a word (same root across languages)
 *
 * @example
 * ```typescript
 * const cognates = await findCognates(storage, 'en', 'mother')
 * // Returns: German "Mutter", Spanish "madre", etc.
 * ```
 */
export async function findCognates(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<CognateEntity[]> {
  // First get the word's etymology to find its root
  const etymologies = await getEtymology(storage, languageCode, word)
  if (etymologies.length === 0) return []

  // Try to determine root language from etymology
  const etymText = etymologies[0].text
  const rootMatch = etymText.match(/(?:from|ultimately from)\s+(Proto-[A-Za-z-]+)/i)

  if (!rootMatch) return []

  const rootLangCode = rootMatch[1].toLowerCase().replace(/[^a-z]/g, '').slice(0, 5)
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.cognates(rootLangCode)

  try {
    const allCognates = await reader.read<CognateEntity>(path)
    // Find cognates sharing the same root
    const wordCognate = allCognates.find(c =>
      c.word.toLowerCase() === word.toLowerCase() &&
      c.languageCode === languageCode
    )

    if (!wordCognate) return []

    // Return all cognates with the same root
    return allCognates.filter(c =>
      c.root === wordCognate.root &&
      !(c.word.toLowerCase() === word.toLowerCase() && c.languageCode === languageCode)
    )
  } catch (e) {
    return []
  }
}

/**
 * Get cognates by root language
 *
 * @example
 * ```typescript
 * const pieCognates = await getCognatesByRootLanguage(storage, 'proto-indo-european')
 * // Returns words sharing Proto-Indo-European roots
 * ```
 */
export async function getCognatesByRootLanguage(
  storage: StorageBackend,
  rootLanguageCode: string,
  limit: number = 100
): Promise<CognateEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.cognates(rootLanguageCode)

  try {
    const allCognates = await reader.read<CognateEntity>(path)
    return allCognates.slice(0, limit)
  } catch (e) {
    return []
  }
}

// =============================================================================
// Descendant Queries
// =============================================================================

/**
 * Get descendants of a word in other languages
 *
 * @example
 * ```typescript
 * const descendants = await getDescendants(storage, 'la', 'aqua')
 * // Returns: Spanish "agua", French "eau", Italian "acqua", etc.
 * ```
 */
export async function getDescendants(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<DescendantEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.descendants(languageCode)

  try {
    const allDescs = await reader.read<DescendantEntity>(path)
    return allDescs.filter(d => d.sourceWord.toLowerCase() === word.toLowerCase())
  } catch (e) {
    return []
  }
}

/**
 * Find ancestors of a word (words it descended from)
 *
 * @example
 * ```typescript
 * const ancestors = await findAncestors(storage, 'es', 'agua')
 * // Returns: Latin "aqua"
 * ```
 */
export async function findAncestors(
  storage: StorageBackend,
  targetWord: string,
  targetLanguage: string
): Promise<DescendantEntity[]> {
  // This requires scanning multiple language files
  // In practice, you'd want an index for this
  const results: DescendantEntity[] = []

  try {
    const listResult = await storage.list('wiktionary/descendants/')

    for (const file of listResult.files) {
      const match = file.match(/lang=([^/]+)/)
      if (!match) continue

      const langCode = match[1]
      const reader = new ParquetReader({ storage })

      try {
        const allDescs = await reader.read<DescendantEntity>(file)
        const matching = allDescs.filter(d =>
          d.descendantWord.toLowerCase() === targetWord.toLowerCase() &&
          (d.targetLanguage.toLowerCase().includes(targetLanguage.toLowerCase()) ||
           d.targetLanguageCode === targetLanguage)
        )
        results.push(...matching)
      } catch (e) {
        continue
      }
    }
  } catch (e) {
    // Listing failed
  }

  return results
}

// =============================================================================
// Compound Word Queries
// =============================================================================

/**
 * Get components of a compound word
 *
 * @example
 * ```typescript
 * const parts = await getCompoundComponents(storage, 'en', 'sunflower')
 * // Returns: sun, flower
 * ```
 */
export async function getCompoundComponents(
  storage: StorageBackend,
  languageCode: string,
  compound: string
): Promise<CompoundEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.compounds(languageCode)

  try {
    const allComps = await reader.read<CompoundEntity>(path)
    return allComps
      .filter(c => c.compound.toLowerCase() === compound.toLowerCase())
      .sort((a, b) => a.position - b.position)
  } catch (e) {
    return []
  }
}

/**
 * Find compound words containing a specific component
 *
 * @example
 * ```typescript
 * const compounds = await findCompoundsWithComponent(storage, 'en', 'sun')
 * // Returns: sunflower, sunshine, sunset, etc.
 * ```
 */
export async function findCompoundsWithComponent(
  storage: StorageBackend,
  languageCode: string,
  component: string,
  limit: number = 100
): Promise<CompoundEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.compounds(languageCode)

  try {
    const allComps = await reader.read<CompoundEntity>(path)
    return allComps
      .filter(c => c.component.toLowerCase() === component.toLowerCase())
      .slice(0, limit)
  } catch (e) {
    return []
  }
}

// =============================================================================
// Collocation Queries
// =============================================================================

/**
 * Get common collocations for a word
 *
 * @example
 * ```typescript
 * const collocations = await getCollocations(storage, 'en', 'make')
 * // Returns: decision, money, sense, etc.
 * ```
 */
export async function getCollocations(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<CollocationEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.collocations(languageCode)

  try {
    const allColls = await reader.read<CollocationEntity>(path)
    return allColls.filter(c => c.word.toLowerCase() === word.toLowerCase())
  } catch (e) {
    return []
  }
}

/**
 * Find words that commonly collocate with a given word
 *
 * @example
 * ```typescript
 * const collocates = await findCollocates(storage, 'en', 'decision')
 * // Returns: make, take, reach, etc.
 * ```
 */
export async function findCollocates(
  storage: StorageBackend,
  languageCode: string,
  collocate: string
): Promise<CollocationEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.collocations(languageCode)

  try {
    const allColls = await reader.read<CollocationEntity>(path)
    return allColls.filter(c => c.collocate.toLowerCase() === collocate.toLowerCase())
  } catch (e) {
    return []
  }
}

// =============================================================================
// Word Frequency Queries
// =============================================================================

/**
 * Get frequency rank for a word
 *
 * @example
 * ```typescript
 * const freq = await getFrequencyRank(storage, 'en', 'the')
 * // Returns: rank 1, perMillion: ~70000
 * ```
 */
export async function getFrequencyRank(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<WordFrequencyEntity | null> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.frequencies(languageCode)

  try {
    const allFreqs = await reader.read<WordFrequencyEntity>(path)
    return allFreqs.find(f => f.word.toLowerCase() === word.toLowerCase()) ?? null
  } catch (e) {
    return null
  }
}

/**
 * Get top N most frequent words
 *
 * @example
 * ```typescript
 * const topWords = await getTopFrequentWords(storage, 'en', 100)
 * // Returns the 100 most common English words
 * ```
 */
export async function getTopFrequentWords(
  storage: StorageBackend,
  languageCode: string,
  limit: number = 100
): Promise<WordFrequencyEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.frequencies(languageCode)

  try {
    const allFreqs = await reader.read<WordFrequencyEntity>(path)
    return allFreqs
      .sort((a, b) => a.rank - b.rank)
      .slice(0, limit)
  } catch (e) {
    return []
  }
}

/**
 * Get words within a frequency range
 *
 * @example
 * ```typescript
 * const mediumFreq = await getWordsByFrequencyRange(storage, 'en', 1000, 2000)
 * // Returns words ranked 1000-2000 in frequency
 * ```
 */
export async function getWordsByFrequencyRange(
  storage: StorageBackend,
  languageCode: string,
  minRank: number,
  maxRank: number
): Promise<WordFrequencyEntity[]> {
  const reader = new ParquetReader({ storage })
  const path = WIKTIONARY_PATHS.frequencies(languageCode)

  try {
    const allFreqs = await reader.read<WordFrequencyEntity>(path)
    return allFreqs
      .filter(f => f.rank >= minRank && f.rank <= maxRank)
      .sort((a, b) => a.rank - b.rank)
  } catch (e) {
    return []
  }
}

// =============================================================================
// Aggregate Queries
// =============================================================================

/**
 * Get complete word information (all entities)
 *
 * @example
 * ```typescript
 * const info = await getCompleteWordInfo(storage, 'en', 'run')
 * console.log(info.words)        // All parts of speech
 * console.log(info.definitions)  // All definitions
 * console.log(info.forms)        // Inflections
 * console.log(info.etymology)    // Word origin
 * ```
 */
export async function getCompleteWordInfo(
  storage: StorageBackend,
  languageCode: string,
  word: string
): Promise<{
  words: WordEntity[]
  definitions: DefinitionEntity[]
  pronunciations: PronunciationEntity[]
  forms: FormEntity[]
  synonyms: RelatedWordEntity[]
  antonyms: RelatedWordEntity[]
  derivedTerms: RelatedWordEntity[]
  etymology: EtymologyEntity[]
  collocations: CollocationEntity[]
  frequency: WordFrequencyEntity | null
}> {
  // Run queries in parallel
  const [
    words,
    definitions,
    pronunciations,
    forms,
    synonyms,
    antonyms,
    derivedTerms,
    etymology,
    collocations,
    frequency
  ] = await Promise.all([
    lookupWord(storage, languageCode, word),
    getDefinitions(storage, languageCode, word),
    getPronunciations(storage, languageCode, word),
    getWordForms(storage, languageCode, word),
    getSynonyms(storage, languageCode, word),
    getAntonyms(storage, languageCode, word),
    getDerivedTerms(storage, languageCode, word),
    getEtymology(storage, languageCode, word),
    getCollocations(storage, languageCode, word),
    getFrequencyRank(storage, languageCode, word),
  ])

  return {
    words,
    definitions,
    pronunciations,
    forms,
    synonyms,
    antonyms,
    derivedTerms,
    etymology,
    collocations,
    frequency,
  }
}

/**
 * Count entities by language
 *
 * @example
 * ```typescript
 * const counts = await countByLanguage(storage, 'en')
 * // Returns entity counts for English
 * ```
 */
export async function countByLanguage(
  storage: StorageBackend,
  languageCode: string
): Promise<{
  words: number
  definitions: number
  pronunciations: number
  etymologies: number
  forms: number
  collocations: number
}> {
  let wordCount = 0
  let defCount = 0
  let formCount = 0

  // Count words, definitions, forms by letter
  for (const letter of 'abcdefghijklmnopqrstuvwxyz#'.split('')) {
    try {
      const reader = new ParquetReader({ storage })
      const wordPath = WIKTIONARY_PATHS.words(languageCode, letter)
      const defPath = WIKTIONARY_PATHS.definitions(languageCode, letter)
      const formPath = WIKTIONARY_PATHS.forms(languageCode, letter)

      const [words, defs, forms] = await Promise.all([
        reader.read<WordEntity>(wordPath).catch(() => []),
        reader.read<DefinitionEntity>(defPath).catch(() => []),
        reader.read<FormEntity>(formPath).catch(() => []),
      ])

      wordCount += words.length
      defCount += defs.length
      formCount += forms.length
    } catch (e) {
      // File doesn't exist
    }
  }

  // Count pronunciations, etymologies, collocations
  let pronCount = 0
  let etymCount = 0
  let collCount = 0

  try {
    const reader = new ParquetReader({ storage })
    const [prons, etyms, colls] = await Promise.all([
      reader.read<PronunciationEntity>(WIKTIONARY_PATHS.pronunciations(languageCode)).catch(() => []),
      reader.read<EtymologyEntity>(WIKTIONARY_PATHS.etymologies(languageCode)).catch(() => []),
      reader.read<CollocationEntity>(WIKTIONARY_PATHS.collocations(languageCode)).catch(() => []),
    ])
    pronCount = prons.length
    etymCount = etyms.length
    collCount = colls.length
  } catch (e) {
    // Files don't exist
  }

  return {
    words: wordCount,
    definitions: defCount,
    pronunciations: pronCount,
    etymologies: etymCount,
    forms: formCount,
    collocations: collCount,
  }
}

// =============================================================================
// Example Usage
// =============================================================================

/**
 * Example: Interactive word lookup
 */
async function exampleLookup(storage: StorageBackend): Promise<void> {
  console.log('=== Word Lookup Example ===\n')

  const word = 'run'
  const info = await getCompleteWordInfo(storage, 'en', word)

  console.log(`Word: ${word}`)
  console.log(`\nParts of speech: ${info.words.map(w => w.pos).join(', ')}`)

  console.log('\nDefinitions:')
  for (const def of info.definitions.slice(0, 5)) {
    console.log(`  [${def.pos}] ${def.gloss}`)
  }

  if (info.forms.length > 0) {
    console.log('\nForms:')
    for (const form of info.forms.slice(0, 5)) {
      console.log(`  ${form.form} (${form.tags?.join(', ') || 'unknown'})`)
    }
  }

  if (info.pronunciations.length > 0) {
    console.log('\nPronunciation:')
    for (const pron of info.pronunciations.slice(0, 2)) {
      if (pron.ipa) console.log(`  IPA: ${pron.ipa}`)
    }
  }

  if (info.synonyms.length > 0) {
    console.log(`\nSynonyms: ${info.synonyms.map(s => s.relatedWord).slice(0, 5).join(', ')}`)
  }

  if (info.etymology.length > 0) {
    console.log(`\nEtymology: ${info.etymology[0].text.slice(0, 200)}...`)
  }

  if (info.frequency) {
    console.log(`\nFrequency rank: ${info.frequency.rank}`)
  }
}

/**
 * Example: Translation comparison
 */
async function exampleTranslationComparison(storage: StorageBackend): Promise<void> {
  console.log('\n=== Translation Comparison Example ===\n')

  const word = 'water'
  const comparison = await compareTranslations(storage, word, 'en', 'es', 'fr')

  console.log(`Translations of "${word}":`)
  console.log(`\nSpanish: ${comparison.lang1.map(t => t.targetWord).join(', ')}`)
  console.log(`French: ${comparison.lang2.map(t => t.targetWord).join(', ')}`)
  if (comparison.common.length > 0) {
    console.log(`\nCommon senses: ${comparison.common.join(', ')}`)
  }
}

/**
 * Example: Etymology tracing
 */
async function exampleEtymology(storage: StorageBackend): Promise<void> {
  console.log('\n=== Etymology Tracing Example ===\n')

  const word = 'algorithm'
  const trace = await traceEtymology(storage, 'en', word)

  console.log(`Etymology of "${word}":`)
  if (trace.etymology) {
    console.log(`\n${trace.etymology.text.slice(0, 300)}`)
  }

  if (trace.chain.length > 0) {
    console.log('\nEtymology chain:')
    for (const link of trace.chain) {
      console.log(`  <- ${link.ancestorWord} (${link.ancestorLanguage}, ${link.linkType})`)
    }
  }
}

/**
 * Example: Finding rhymes
 */
async function exampleRhymes(storage: StorageBackend): Promise<void> {
  console.log('\n=== Rhyme Finding Example ===\n')

  const word = 'day'
  const rhymes = await findRhymes(storage, 'en', word, 20)

  console.log(`Words that rhyme with "${word}":`)
  console.log(rhymes.map(r => r.word).join(', '))
}

/**
 * Example: Cognate search
 */
async function exampleCognates(storage: StorageBackend): Promise<void> {
  console.log('\n=== Cognate Search Example ===\n')

  const word = 'mother'
  const cognates = await findCognates(storage, 'en', word)

  console.log(`Cognates of "${word}":`)
  for (const cog of cognates.slice(0, 10)) {
    console.log(`  ${cog.language}: ${cog.word}`)
  }
}

// Export examples for testing
export const examples = {
  lookup: exampleLookup,
  translationComparison: exampleTranslationComparison,
  etymology: exampleEtymology,
  rhymes: exampleRhymes,
  cognates: exampleCognates,
}
