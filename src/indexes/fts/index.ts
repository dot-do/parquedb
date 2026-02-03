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
  ENGLISH_STOPWORDS,
} from './tokenizer'

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
