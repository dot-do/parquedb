/**
 * FTS Types for ParqueDB
 */

/**
 * Token extracted from text
 */
export interface Token {
  /** Original term before normalization */
  original: string
  /** Normalized/stemmed term */
  term: string
  /** Position in the text (0-based) */
  position: number
  /** Start offset in original text */
  startOffset: number
  /** End offset in original text */
  endOffset: number
}

/**
 * Posting in inverted index
 */
export interface Posting {
  /** Document ID */
  docId: string
  /** Field the term came from */
  field: string
  /** Term frequency in this document */
  frequency: number
  /** Positions of the term */
  positions: number[]
}

/**
 * Document stats for scoring
 */
export interface DocumentStats {
  /** Document ID */
  docId: string
  /** Field lengths (field -> word count) */
  fieldLengths: Map<string, number>
  /** Total word count */
  totalLength: number
}

/**
 * Corpus statistics for BM25
 */
export interface CorpusStats {
  /** Total number of documents */
  documentCount: number
  /** Average document length */
  avgDocLength: number
  /** Document frequency for each term (term -> doc count) */
  documentFrequency: Map<string, number>
}

/**
 * FTS search result
 */
export interface FTSResult {
  /** Document ID */
  docId: string
  /** BM25 score */
  score: number
  /** Matched terms */
  matchedTerms: string[]
  /** Term frequencies */
  termFrequencies: Map<string, number>
}

/**
 * Tokenizer options
 */
export interface TokenizerOptions {
  /** Language for stemming */
  language?: string | undefined
  /** Minimum word length */
  minWordLength?: number | undefined
  /** Maximum word length */
  maxWordLength?: number | undefined
  /** Custom stopwords */
  stopwords?: Set<string> | undefined
  /** Whether to apply stemming */
  stem?: boolean | undefined
}
