/**
 * Wiktionary Example - Load and query Wiktionary data from kaikki.org
 *
 * This comprehensive example demonstrates:
 * - Loading Wiktionary JSONL data into ParqueDB
 * - Querying words, definitions, and pronunciations
 * - Finding translations across languages
 * - Exploring word relationships (synonyms, antonyms, etc.)
 * - Tracing etymologies and finding cognates
 * - Working with inflected forms
 * - Finding rhymes and collocations
 * - Word frequency data
 *
 * @example
 * ```typescript
 * import {
 *   loadWiktionary,
 *   lookupWord,
 *   getDefinitions,
 *   getWordForms,
 *   traceEtymology,
 *   findCognates,
 *   getCollocations,
 *   findRhymes,
 *   compareTranslations,
 *   getFrequencyRank,
 * } from './examples/wiktionary'
 *
 * // Load data
 * await loadWiktionary(storage, 'https://kaikki.org/dictionary/English/...')
 *
 * // Query words
 * const word = await lookupWord(storage, 'en', 'algorithm')
 * const defs = await getDefinitions(storage, 'en', 'algorithm')
 *
 * // Get inflected forms
 * const forms = await getWordForms(storage, 'en', 'run')
 * // -> runs, ran, running
 *
 * // Trace etymology
 * const etym = await traceEtymology(storage, 'en', 'algorithm')
 * // -> algorithm <- Arabic al-khwarizmi <- ...
 *
 * // Find cognates
 * const cognates = await findCognates(storage, 'en', 'mother')
 * // -> German Mutter, Spanish madre, ...
 *
 * // Get collocations
 * const collocations = await getCollocations(storage, 'en', 'make')
 * // -> decision, money, sense, ...
 *
 * // Find rhymes
 * const rhymes = await findRhymes(storage, 'en', 'day')
 * // -> say, way, play, ...
 *
 * // Compare translations
 * const trans = await compareTranslations(storage, 'water', 'en', 'es', 'fr')
 * // -> { lang1: [agua], lang2: [eau], common: [...] }
 *
 * // Get frequency rank
 * const freq = await getFrequencyRank(storage, 'en', 'the')
 * // -> rank: 1
 * ```
 *
 * @see ./README.md for full documentation
 */

// Schema types and constants
export * from './schema'

// Loader functions
export { loadWiktionary, streamWiktionary } from './load'

// Query functions
export {
  // Word queries
  lookupWord,
  lookupWordPos,
  searchWordsPrefix,
  getWordsByPos,

  // Definition queries
  getDefinitions,
  getDefinitionsForPos,
  searchDefinitions,

  // Form (inflection) queries
  getWordForms,
  getFormsByTags,
  findBaseWord,

  // Pronunciation queries
  getPronunciations,
  getWordsWithAudio,
  findRhymes,
  findRhymesByPattern,

  // Translation queries
  getTranslations,
  getAllTranslations,
  reverseTranslationLookup,
  compareTranslations,
  buildTranslationGraph,

  // Related word queries
  getSynonyms,
  getAntonyms,
  getDerivedTerms,
  getRelatedWords,
  getAllRelatedWords,
  findRelatedTo,

  // Etymology queries
  getEtymology,
  searchEtymologies,
  traceEtymology,
  getEtymologyLinksByType,
  findBorrowedWords,

  // Cognate queries
  findCognates,
  getCognatesByRootLanguage,

  // Descendant queries
  getDescendants,
  findAncestors,

  // Compound word queries
  getCompoundComponents,
  findCompoundsWithComponent,

  // Collocation queries
  getCollocations,
  findCollocates,

  // Word frequency queries
  getFrequencyRank,
  getTopFrequentWords,
  getWordsByFrequencyRange,

  // Aggregate queries
  getCompleteWordInfo,
  countByLanguage,

  // Examples
  examples,
} from './queries'
