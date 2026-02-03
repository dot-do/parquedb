/**
 * Full-Text Search Module Exports
 */

// Types
export type {
  Token,
  Posting,
  DocumentStats,
  CorpusStats,
  FTSResult,
  TokenizerOptions,
} from './types'

// Tokenizer
export {
  tokenize,
  tokenizeQuery,
  porterStem,
  getStopwords,
  ENGLISH_STOPWORDS,
  SPANISH_STOPWORDS,
  FRENCH_STOPWORDS,
  GERMAN_STOPWORDS,
  ITALIAN_STOPWORDS,
  PORTUGUESE_STOPWORDS,
  DUTCH_STOPWORDS,
  RUSSIAN_STOPWORDS,
  SWEDISH_STOPWORDS,
  NORWEGIAN_STOPWORDS,
  DANISH_STOPWORDS,
  FINNISH_STOPWORDS,
  TURKISH_STOPWORDS,
  ARABIC_STOPWORDS,
  STOPWORDS_BY_LANGUAGE,
} from './tokenizer'

// Multi-language Stemmers
export {
  getStemmer,
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
  type SupportedLanguage,
  type Stemmer,
} from './stemmers'

// Inverted Index
export { InvertedIndex } from './inverted-index'

// Scoring
export {
  BM25Scorer,
  DEFAULT_BM25_CONFIG,
  logScaledTf,
  augmentedTf,
  luceneIdf,
  tfidf,
  type BM25Config,
} from './scoring'

// Search
export { FTSIndex, buildFTSIndex } from './search'

// Highlight/Snippet Generation
export {
  highlightText,
  generateSnippet,
  generateHighlights,
  type HighlightOptions,
  type SnippetOptions,
  type GenerateHighlightsOptions,
} from './highlight'

// Fuzzy Matching
export {
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
  type FuzzyMatch,
  type NormalizedFuzzyOptions,
} from './fuzzy'
