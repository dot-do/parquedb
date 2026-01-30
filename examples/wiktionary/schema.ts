/**
 * Wiktionary Schema Definitions
 *
 * Entity types for loading Wiktionary data from kaikki.org JSONL format
 * into ParqueDB. Based on the wiktextract output format.
 *
 * @see https://kaikki.org/dictionary/
 * @see https://github.com/tatuylonen/wiktextract
 */

import type { ParquetSchema } from '../../src/parquet/types'

// =============================================================================
// Wiktionary Entry Types (from kaikki.org JSONL)
// =============================================================================

/**
 * Raw Wiktionary entry from kaikki.org JSONL
 */
export interface WiktionaryEntry {
  /** The headword being defined */
  word: string
  /** Full language name (e.g., "English") */
  lang: string
  /** ISO language code (e.g., "en") */
  lang_code: string
  /** Part of speech (noun, verb, adj, etc.) */
  pos: string
  /** Etymology text/description */
  etymology_text?: string
  /** Etymology number (when multiple etymologies exist) */
  etymology_number?: number
  /** Word senses/definitions */
  senses?: WiktionarySense[]
  /** Pronunciation data */
  sounds?: WiktionarySound[]
  /** Translation data */
  translations?: WiktionaryTranslation[]
  /** Inflected forms */
  forms?: WiktionaryForm[]
  /** Derived terms */
  derived?: WiktionaryLinkage[]
  /** Related terms */
  related?: WiktionaryLinkage[]
  /** Synonyms */
  synonyms?: WiktionaryLinkage[]
  /** Antonyms */
  antonyms?: WiktionaryLinkage[]
  /** Hypernyms (broader terms) */
  hypernyms?: WiktionaryLinkage[]
  /** Hyponyms (narrower terms) */
  hyponyms?: WiktionaryLinkage[]
  /** Holonyms (whole of which this is part) */
  holonyms?: WiktionaryLinkage[]
  /** Meronyms (parts of this) */
  meronyms?: WiktionaryLinkage[]
  /** Troponyms */
  troponyms?: WiktionaryLinkage[]
  /** Coordinate terms */
  coordinate_terms?: WiktionaryLinkage[]
  /** Categories this word belongs to */
  categories?: WiktionaryCategory[]
  /** Wikipedia reference */
  wikipedia?: string[]
  /** Wikidata IDs */
  wikidata?: string[]
  /** Head templates (for inflection tables) */
  head_templates?: WiktionaryTemplate[]
  /** Inflection templates */
  inflection_templates?: WiktionaryTemplate[]
  /** Descendants in other languages */
  descendants?: WiktionaryDescendant[]
  /** Redirect target (for redirect entries) */
  redirect?: string
  /** Original page title */
  title?: string
}

/**
 * Word sense/definition
 */
export interface WiktionarySense {
  /** Unique sense ID */
  id?: string
  /** Definition/gloss text */
  glosses?: string[]
  /** Raw gloss without links */
  raw_glosses?: string[]
  /** Tags (grammar, usage, domain) */
  tags?: string[]
  /** Qualifier tags */
  qualifier?: string
  /** Usage examples */
  examples?: WiktionaryExample[]
  /** Synonyms for this sense */
  synonyms?: WiktionaryLinkage[]
  /** Antonyms for this sense */
  antonyms?: WiktionaryLinkage[]
  /** Hypernyms for this sense */
  hypernyms?: WiktionaryLinkage[]
  /** Hyponyms for this sense */
  hyponyms?: WiktionaryLinkage[]
  /** Topics/domains */
  topics?: string[]
  /** Categories */
  categories?: WiktionaryCategory[]
  /** Form of another word */
  form_of?: WiktionaryFormOf[]
  /** Wikidata sense ID */
  wikidata?: string[]
  /** Wikipedia links */
  wikipedia?: string[]
  /** Sense ID in source */
  senseid?: string[]
  /** Alt forms of this sense */
  alt_of?: WiktionaryFormOf[]
  /** Links within gloss */
  links?: Array<[string, string]>
}

/**
 * Form-of reference
 */
export interface WiktionaryFormOf {
  word: string
  extra?: string
}

/**
 * Pronunciation data
 */
export interface WiktionarySound {
  /** IPA transcription */
  ipa?: string
  /** Phonemic transcription */
  phonemic?: string
  /** Audio file URL */
  audio?: string
  /** Audio file text/description */
  text?: string
  /** Ogg vorbis audio URL */
  ogg_url?: string
  /** MP3 audio URL */
  mp3_url?: string
  /** Rhymes */
  rhymes?: string
  /** Homophones */
  homophones?: string[]
  /** Enpr transcription */
  enpr?: string
  /** Tags (dialect, accent, etc.) */
  tags?: string[]
  /** Audio license */
  audio_license?: string
}

/**
 * Translation to another language
 */
export interface WiktionaryTranslation {
  /** Translation text */
  word?: string
  /** Target language name */
  lang?: string
  /** Target language code */
  code?: string
  /** Romanization */
  roman?: string
  /** Sense/meaning this translates */
  sense?: string
  /** Tags */
  tags?: string[]
  /** Alt form */
  alt?: string
  /** Notes */
  note?: string
}

/**
 * Inflected or variant form
 */
export interface WiktionaryForm {
  /** Form text */
  form: string
  /** Grammatical tags (tense, person, number, etc.) */
  tags?: string[]
  /** IPA pronunciation */
  ipa?: string
  /** Source */
  source?: string
  /** Roman transliteration */
  roman?: string
}

/**
 * Semantic linkage (synonym, antonym, related, derived, etc.)
 */
export interface WiktionaryLinkage {
  /** Linked word */
  word: string
  /** Sense/meaning */
  sense?: string
  /** Tags */
  tags?: string[]
  /** Topics */
  topics?: string[]
  /** Source (for borrowed terms) */
  source?: string
  /** Alt text */
  alt?: string
  /** Roman transliteration */
  roman?: string
  /** English translation (for non-English entries) */
  english?: string
  /** Language code (for multi-language entries) */
  lang_code?: string
}

/**
 * Usage example
 */
export interface WiktionaryExample {
  /** Example text */
  text?: string
  /** English translation (for non-English) */
  english?: string
  /** Reference source */
  ref?: string
  /** Romanization */
  roman?: string
  /** Type of example */
  type?: string
  /** Note */
  note?: string
}

/**
 * Category reference
 */
export interface WiktionaryCategory {
  /** Category name */
  name: string
  /** Category kind */
  kind?: string
  /** Language code */
  langcode?: string
  /** Original category string */
  orig?: string
  /** Parent categories */
  parents?: string[]
  /** Source */
  source?: string
}

/**
 * Template data
 */
export interface WiktionaryTemplate {
  /** Template name */
  name: string
  /** Template arguments */
  args?: Record<string, string>
  /** Expanded output */
  expansion?: string
}

/**
 * Descendant in language family tree
 */
export interface WiktionaryDescendant {
  /** Depth in tree */
  depth: number
  /** Template data */
  templates?: WiktionaryTemplate[]
  /** Text content */
  text?: string
  /** Tags */
  tags?: string[]
}

// =============================================================================
// ParqueDB Entity Schemas
// =============================================================================

/**
 * Word entity - main entry for a word in a specific language
 *
 * Partition: language/word[0:2]/
 * Sort: word, pos
 */
export interface WordEntity {
  /** Entity ID: words/{lang_code}/{word}/{pos} */
  $id: string
  /** Entity type */
  $type: 'Word'
  /** Display name */
  name: string
  /** The headword */
  word: string
  /** Language name */
  language: string
  /** ISO language code */
  languageCode: string
  /** Part of speech */
  pos: string
  /** Etymology text */
  etymologyText?: string
  /** Etymology number */
  etymologyNumber?: number
  /** Categories (flattened) */
  categories?: string[]
  /** Wikipedia link */
  wikipedia?: string
  /** Wikidata ID */
  wikidata?: string
  /** Number of senses */
  senseCount: number
  /** Number of translations */
  translationCount: number
  /** Has audio */
  hasAudio: boolean
  /** Has IPA */
  hasIpa: boolean
  /** Raw data reference */
  rawData?: Record<string, unknown>
}

/**
 * Definition entity - a single sense/meaning of a word
 *
 * Partition: language/
 * Sort: word, senseIndex
 */
export interface DefinitionEntity {
  /** Entity ID: definitions/{lang_code}/{word}/{pos}/{index} */
  $id: string
  /** Entity type */
  $type: 'Definition'
  /** Display name */
  name: string
  /** Parent word ID */
  wordId: string
  /** Word text */
  word: string
  /** Language code */
  languageCode: string
  /** Part of speech */
  pos: string
  /** Sense index */
  senseIndex: number
  /** Main gloss/definition */
  gloss: string
  /** All glosses */
  glosses?: string[]
  /** Tags (grammar, register, domain) */
  tags?: string[]
  /** Topics/semantic domains */
  topics?: string[]
  /** Usage examples (text only) */
  examples?: string[]
  /** Wikidata sense ID */
  wikidata?: string
}

/**
 * Pronunciation entity
 *
 * Partition: language/
 * Sort: word, type
 */
export interface PronunciationEntity {
  /** Entity ID: pronunciations/{lang_code}/{word}/{index} */
  $id: string
  /** Entity type */
  $type: 'Pronunciation'
  /** Display name */
  name: string
  /** Parent word ID */
  wordId: string
  /** Word text */
  word: string
  /** Language code */
  languageCode: string
  /** IPA transcription */
  ipa?: string
  /** Audio file URL (ogg) */
  audioOgg?: string
  /** Audio file URL (mp3) */
  audioMp3?: string
  /** Audio description */
  audioText?: string
  /** Dialect/accent tags */
  tags?: string[]
  /** Rhymes */
  rhymes?: string
  /** Homophones */
  homophones?: string[]
}

/**
 * Translation entity
 *
 * Partition: source_language/target_language/
 * Sort: word
 */
export interface TranslationEntity {
  /** Entity ID: translations/{src_lang}/{tgt_lang}/{word}/{index} */
  $id: string
  /** Entity type */
  $type: 'Translation'
  /** Display name */
  name: string
  /** Parent word ID */
  wordId: string
  /** Source word */
  sourceWord: string
  /** Source language code */
  sourceLanguageCode: string
  /** Target word */
  targetWord: string
  /** Target language name */
  targetLanguage: string
  /** Target language code */
  targetLanguageCode: string
  /** Romanization */
  romanization?: string
  /** Sense being translated */
  sense?: string
  /** Tags */
  tags?: string[]
}

/**
 * Related word entity - synonyms, antonyms, derived terms, etc.
 *
 * Partition: language/relation_type/
 * Sort: word
 */
export interface RelatedWordEntity {
  /** Entity ID: related/{lang_code}/{type}/{word}/{related_word} */
  $id: string
  /** Entity type */
  $type: 'RelatedWord'
  /** Display name */
  name: string
  /** Parent word ID */
  wordId: string
  /** Source word */
  word: string
  /** Language code */
  languageCode: string
  /** Relation type (synonym, antonym, derived, etc.) */
  relationType: RelationType
  /** Related word */
  relatedWord: string
  /** Sense (if specific to a meaning) */
  sense?: string
  /** Tags */
  tags?: string[]
}

/**
 * Relation types
 */
export type RelationType =
  | 'synonym'
  | 'antonym'
  | 'hypernym'
  | 'hyponym'
  | 'holonym'
  | 'meronym'
  | 'troponym'
  | 'coordinate'
  | 'derived'
  | 'related'

/**
 * Etymology entity - word origin and history
 *
 * Partition: language/
 * Sort: word
 */
export interface EtymologyEntity {
  /** Entity ID: etymologies/{lang_code}/{word}/{etymology_number} */
  $id: string
  /** Entity type */
  $type: 'Etymology'
  /** Display name */
  name: string
  /** Parent word ID */
  wordId: string
  /** Word text */
  word: string
  /** Language code */
  languageCode: string
  /** Etymology number (when multiple) */
  etymologyNumber: number
  /** Etymology text */
  text: string
}

// =============================================================================
// Parquet Schemas for Storage
// =============================================================================

/**
 * Parquet schema for Word entities
 */
export const WORD_PARQUET_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  $type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  word: { type: 'STRING', optional: false },
  language: { type: 'STRING', optional: false },
  languageCode: { type: 'STRING', optional: false },
  pos: { type: 'STRING', optional: false },
  etymologyText: { type: 'STRING', optional: true },
  etymologyNumber: { type: 'INT32', optional: true },
  categories: { type: 'JSON', optional: true },
  wikipedia: { type: 'STRING', optional: true },
  wikidata: { type: 'STRING', optional: true },
  senseCount: { type: 'INT32', optional: false },
  translationCount: { type: 'INT32', optional: false },
  hasAudio: { type: 'BOOLEAN', optional: false },
  hasIpa: { type: 'BOOLEAN', optional: false },
}

/**
 * Parquet schema for Definition entities
 */
export const DEFINITION_PARQUET_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  $type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  wordId: { type: 'STRING', optional: false },
  word: { type: 'STRING', optional: false },
  languageCode: { type: 'STRING', optional: false },
  pos: { type: 'STRING', optional: false },
  senseIndex: { type: 'INT32', optional: false },
  gloss: { type: 'STRING', optional: false },
  glosses: { type: 'JSON', optional: true },
  tags: { type: 'JSON', optional: true },
  topics: { type: 'JSON', optional: true },
  examples: { type: 'JSON', optional: true },
  wikidata: { type: 'STRING', optional: true },
}

/**
 * Parquet schema for Pronunciation entities
 */
export const PRONUNCIATION_PARQUET_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  $type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  wordId: { type: 'STRING', optional: false },
  word: { type: 'STRING', optional: false },
  languageCode: { type: 'STRING', optional: false },
  ipa: { type: 'STRING', optional: true },
  audioOgg: { type: 'STRING', optional: true },
  audioMp3: { type: 'STRING', optional: true },
  audioText: { type: 'STRING', optional: true },
  tags: { type: 'JSON', optional: true },
  rhymes: { type: 'STRING', optional: true },
  homophones: { type: 'JSON', optional: true },
}

/**
 * Parquet schema for Translation entities
 */
export const TRANSLATION_PARQUET_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  $type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  wordId: { type: 'STRING', optional: false },
  sourceWord: { type: 'STRING', optional: false },
  sourceLanguageCode: { type: 'STRING', optional: false },
  targetWord: { type: 'STRING', optional: false },
  targetLanguage: { type: 'STRING', optional: false },
  targetLanguageCode: { type: 'STRING', optional: false },
  romanization: { type: 'STRING', optional: true },
  sense: { type: 'STRING', optional: true },
  tags: { type: 'JSON', optional: true },
}

/**
 * Parquet schema for RelatedWord entities
 */
export const RELATED_WORD_PARQUET_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  $type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  wordId: { type: 'STRING', optional: false },
  word: { type: 'STRING', optional: false },
  languageCode: { type: 'STRING', optional: false },
  relationType: { type: 'STRING', optional: false },
  relatedWord: { type: 'STRING', optional: false },
  sense: { type: 'STRING', optional: true },
  tags: { type: 'JSON', optional: true },
}

/**
 * Parquet schema for Etymology entities
 */
export const ETYMOLOGY_PARQUET_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  $type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  wordId: { type: 'STRING', optional: false },
  word: { type: 'STRING', optional: false },
  languageCode: { type: 'STRING', optional: false },
  etymologyNumber: { type: 'INT32', optional: false },
  text: { type: 'STRING', optional: false },
}

/**
 * Form entity - Inflected forms (plurals, conjugations, declensions)
 *
 * Partition: language/
 * Sort: word, formType
 */
export interface FormEntity {
  /** Entity ID: forms/{lang_code}/{word}/{form}/{index} */
  $id: string
  /** Entity type */
  $type: 'Form'
  /** Display name */
  name: string
  /** Parent word ID */
  wordId: string
  /** Base word */
  word: string
  /** Language code */
  languageCode: string
  /** Part of speech of base word */
  pos: string
  /** The inflected form text */
  form: string
  /** Grammatical tags (tense, number, case, etc.) */
  tags?: string[]
  /** IPA pronunciation of form */
  ipa?: string
  /** Romanization */
  romanization?: string
  /** Source of form data */
  source?: string
}

/**
 * Parquet schema for Form entities
 */
export const FORM_PARQUET_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  $type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  wordId: { type: 'STRING', optional: false },
  word: { type: 'STRING', optional: false },
  languageCode: { type: 'STRING', optional: false },
  pos: { type: 'STRING', optional: false },
  form: { type: 'STRING', optional: false },
  tags: { type: 'JSON', optional: true },
  ipa: { type: 'STRING', optional: true },
  romanization: { type: 'STRING', optional: true },
  source: { type: 'STRING', optional: true },
}

/**
 * Descendant entity - Words derived from this word in other languages
 *
 * Partition: source_language/
 * Sort: word, targetLanguage
 */
export interface DescendantEntity {
  /** Entity ID: descendants/{src_lang}/{word}/{tgt_lang}/{descendant}/{index} */
  $id: string
  /** Entity type */
  $type: 'Descendant'
  /** Display name */
  name: string
  /** Parent word ID */
  wordId: string
  /** Source word (ancestor) */
  sourceWord: string
  /** Source language code */
  sourceLanguageCode: string
  /** Descendant word */
  descendantWord: string
  /** Target language name */
  targetLanguage: string
  /** Target language code */
  targetLanguageCode?: string
  /** Depth in descendant tree (1 = direct) */
  depth: number
  /** Tags (borrowed, inherited, learned, etc.) */
  tags?: string[]
  /** Text description */
  text?: string
}

/**
 * Parquet schema for Descendant entities
 */
export const DESCENDANT_PARQUET_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  $type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  wordId: { type: 'STRING', optional: false },
  sourceWord: { type: 'STRING', optional: false },
  sourceLanguageCode: { type: 'STRING', optional: false },
  descendantWord: { type: 'STRING', optional: false },
  targetLanguage: { type: 'STRING', optional: false },
  targetLanguageCode: { type: 'STRING', optional: true },
  depth: { type: 'INT32', optional: false },
  tags: { type: 'JSON', optional: true },
  text: { type: 'STRING', optional: true },
}

/**
 * Compound entity - Compound words and their components
 *
 * Partition: language/
 * Sort: compound, component
 */
export interface CompoundEntity {
  /** Entity ID: compounds/{lang_code}/{compound}/{component}/{index} */
  $id: string
  /** Entity type */
  $type: 'Compound'
  /** Display name */
  name: string
  /** Compound word ID */
  compoundWordId: string
  /** Component word ID (if known) */
  componentWordId?: string
  /** The compound word */
  compound: string
  /** Language code */
  languageCode: string
  /** Component word */
  component: string
  /** Position of component (1, 2, 3, etc.) */
  position: number
  /** Component type (head, modifier, prefix, suffix, etc.) */
  componentType?: string
  /** Tags */
  tags?: string[]
}

/**
 * Parquet schema for Compound entities
 */
export const COMPOUND_PARQUET_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  $type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  compoundWordId: { type: 'STRING', optional: false },
  componentWordId: { type: 'STRING', optional: true },
  compound: { type: 'STRING', optional: false },
  languageCode: { type: 'STRING', optional: false },
  component: { type: 'STRING', optional: false },
  position: { type: 'INT32', optional: false },
  componentType: { type: 'STRING', optional: true },
  tags: { type: 'JSON', optional: true },
}

/**
 * Collocation entity - Common word pairings
 *
 * Partition: language/
 * Sort: word, collocate
 */
export interface CollocationEntity {
  /** Entity ID: collocations/{lang_code}/{word}/{collocate}/{index} */
  $id: string
  /** Entity type */
  $type: 'Collocation'
  /** Display name */
  name: string
  /** Parent word ID */
  wordId: string
  /** Base word */
  word: string
  /** Language code */
  languageCode: string
  /** Collocate word */
  collocate: string
  /** Collocation type (adj+noun, verb+noun, etc.) */
  collocationType?: string
  /** Full collocation phrase */
  phrase?: string
  /** Frequency score (if available) */
  frequency?: number
  /** Example usage */
  example?: string
  /** Tags */
  tags?: string[]
}

/**
 * Parquet schema for Collocation entities
 */
export const COLLOCATION_PARQUET_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  $type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  wordId: { type: 'STRING', optional: false },
  word: { type: 'STRING', optional: false },
  languageCode: { type: 'STRING', optional: false },
  collocate: { type: 'STRING', optional: false },
  collocationType: { type: 'STRING', optional: true },
  phrase: { type: 'STRING', optional: true },
  frequency: { type: 'DOUBLE', optional: true },
  example: { type: 'STRING', optional: true },
  tags: { type: 'JSON', optional: true },
}

/**
 * Cognate entity - Words sharing the same root across languages
 *
 * Partition: root_language/
 * Sort: root, language
 */
export interface CognateEntity {
  /** Entity ID: cognates/{root_lang}/{root}/{lang_code}/{word} */
  $id: string
  /** Entity type */
  $type: 'Cognate'
  /** Display name */
  name: string
  /** Word ID */
  wordId: string
  /** The cognate word */
  word: string
  /** Language of this cognate */
  languageCode: string
  /** Language name */
  language: string
  /** Shared etymological root */
  root: string
  /** Root language (e.g., Proto-Indo-European) */
  rootLanguage: string
  /** Root language code */
  rootLanguageCode: string
  /** Meaning in this language */
  meaning?: string
  /** Tags */
  tags?: string[]
}

/**
 * Parquet schema for Cognate entities
 */
export const COGNATE_PARQUET_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  $type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  wordId: { type: 'STRING', optional: false },
  word: { type: 'STRING', optional: false },
  languageCode: { type: 'STRING', optional: false },
  language: { type: 'STRING', optional: false },
  root: { type: 'STRING', optional: false },
  rootLanguage: { type: 'STRING', optional: false },
  rootLanguageCode: { type: 'STRING', optional: false },
  meaning: { type: 'STRING', optional: true },
  tags: { type: 'JSON', optional: true },
}

/**
 * EtymologyLink entity - Links between words in etymology chains
 *
 * Partition: language/
 * Sort: word, ancestor
 */
export interface EtymologyLinkEntity {
  /** Entity ID: etymology_links/{lang_code}/{word}/{ancestor_lang}/{ancestor} */
  $id: string
  /** Entity type */
  $type: 'EtymologyLink'
  /** Display name */
  name: string
  /** Word ID */
  wordId: string
  /** The word */
  word: string
  /** Language code */
  languageCode: string
  /** Ancestor word */
  ancestorWord: string
  /** Ancestor language */
  ancestorLanguage: string
  /** Ancestor language code */
  ancestorLanguageCode: string
  /** Link type (inherited, borrowed, derived, etc.) */
  linkType: EtymologyLinkType
  /** Distance in etymology chain (1 = direct parent) */
  distance: number
  /** Intermediate languages in chain */
  intermediateLanguages?: string[]
  /** Notes */
  notes?: string
}

/**
 * Etymology link types
 */
export type EtymologyLinkType =
  | 'inherited'   // Direct descent in same language family
  | 'borrowed'    // Borrowed from another language
  | 'learned'     // Learned borrowing (e.g., from Latin/Greek)
  | 'calque'      // Loan translation
  | 'derived'     // Morphologically derived
  | 'compound'    // Part of a compound word
  | 'cognate'     // Shares common ancestor
  | 'uncertain'   // Uncertain etymology

/**
 * Parquet schema for EtymologyLink entities
 */
export const ETYMOLOGY_LINK_PARQUET_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  $type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  wordId: { type: 'STRING', optional: false },
  word: { type: 'STRING', optional: false },
  languageCode: { type: 'STRING', optional: false },
  ancestorWord: { type: 'STRING', optional: false },
  ancestorLanguage: { type: 'STRING', optional: false },
  ancestorLanguageCode: { type: 'STRING', optional: false },
  linkType: { type: 'STRING', optional: false },
  distance: { type: 'INT32', optional: false },
  intermediateLanguages: { type: 'JSON', optional: true },
  notes: { type: 'STRING', optional: true },
}

/**
 * WordFrequency entity - Word usage frequency data
 *
 * Partition: language/
 * Sort: rank
 */
export interface WordFrequencyEntity {
  /** Entity ID: frequencies/{lang_code}/{word} */
  $id: string
  /** Entity type */
  $type: 'WordFrequency'
  /** Display name */
  name: string
  /** Word ID */
  wordId: string
  /** The word */
  word: string
  /** Language code */
  languageCode: string
  /** Frequency rank (1 = most common) */
  rank: number
  /** Raw frequency count (if available) */
  count?: number
  /** Frequency per million words */
  perMillion?: number
  /** Corpus source */
  source?: string
  /** Part of speech (if frequency varies by POS) */
  pos?: string
}

/**
 * Parquet schema for WordFrequency entities
 */
export const WORD_FREQUENCY_PARQUET_SCHEMA: ParquetSchema = {
  $id: { type: 'STRING', optional: false },
  $type: { type: 'STRING', optional: false },
  name: { type: 'STRING', optional: false },
  wordId: { type: 'STRING', optional: false },
  word: { type: 'STRING', optional: false },
  languageCode: { type: 'STRING', optional: false },
  rank: { type: 'INT32', optional: false },
  count: { type: 'INT64', optional: true },
  perMillion: { type: 'DOUBLE', optional: true },
  source: { type: 'STRING', optional: true },
  pos: { type: 'STRING', optional: true },
}

// =============================================================================
// Storage Configuration
// =============================================================================

/**
 * Storage path conventions for Wiktionary data
 */
export const WIKTIONARY_PATHS = {
  /** Base path for wiktionary data */
  base: 'wiktionary',

  /** Words partitioned by language and first letter */
  words: (langCode: string, letter: string) =>
    `wiktionary/words/lang=${langCode}/letter=${letter.toLowerCase()}/data.parquet`,

  /** Definitions partitioned by language */
  definitions: (langCode: string, letter: string) =>
    `wiktionary/definitions/lang=${langCode}/letter=${letter.toLowerCase()}/data.parquet`,

  /** Pronunciations partitioned by language */
  pronunciations: (langCode: string) =>
    `wiktionary/pronunciations/lang=${langCode}/data.parquet`,

  /** Translations partitioned by source and target language */
  translations: (srcLang: string, tgtLang: string) =>
    `wiktionary/translations/src=${srcLang}/tgt=${tgtLang}/data.parquet`,

  /** Related words partitioned by language and relation type */
  relatedWords: (langCode: string, relationType: RelationType) =>
    `wiktionary/related/lang=${langCode}/type=${relationType}/data.parquet`,

  /** Etymologies partitioned by language */
  etymologies: (langCode: string) =>
    `wiktionary/etymologies/lang=${langCode}/data.parquet`,

  /** Forms (inflections) partitioned by language */
  forms: (langCode: string, letter: string) =>
    `wiktionary/forms/lang=${langCode}/letter=${letter.toLowerCase()}/data.parquet`,

  /** Descendants partitioned by source language */
  descendants: (langCode: string) =>
    `wiktionary/descendants/lang=${langCode}/data.parquet`,

  /** Compounds partitioned by language */
  compounds: (langCode: string) =>
    `wiktionary/compounds/lang=${langCode}/data.parquet`,

  /** Collocations partitioned by language */
  collocations: (langCode: string) =>
    `wiktionary/collocations/lang=${langCode}/data.parquet`,

  /** Cognates partitioned by root language */
  cognates: (rootLangCode: string) =>
    `wiktionary/cognates/root_lang=${rootLangCode}/data.parquet`,

  /** Etymology links partitioned by language */
  etymologyLinks: (langCode: string) =>
    `wiktionary/etymology_links/lang=${langCode}/data.parquet`,

  /** Word frequencies partitioned by language */
  frequencies: (langCode: string) =>
    `wiktionary/frequencies/lang=${langCode}/data.parquet`,

  /** Manifest file */
  manifest: 'wiktionary/_manifest.json',
} as const

/**
 * Loader configuration options
 */
export interface WiktionaryLoaderConfig {
  /** Languages to load (empty = all) */
  languages?: string[]
  /** Maximum entries to process (for testing) */
  maxEntries?: number
  /** Batch size for writes */
  batchSize?: number
  /** Row group size for parquet files */
  rowGroupSize?: number
  /** Enable verbose logging */
  verbose?: boolean
  /** Skip redirect entries */
  skipRedirects?: boolean
}

/**
 * Default loader configuration
 */
export const DEFAULT_LOADER_CONFIG: Required<WiktionaryLoaderConfig> = {
  languages: [],
  maxEntries: Infinity,
  batchSize: 10000,
  rowGroupSize: 50000,
  verbose: false,
  skipRedirects: true,
}

/**
 * Manifest file structure
 */
export interface WiktionaryManifest {
  /** When the load started */
  loadStarted: string
  /** When the load completed */
  loadCompleted?: string
  /** Source URL */
  sourceUrl: string
  /** Total entries processed */
  totalEntries: number
  /** Entries by language */
  entriesByLanguage: Record<string, number>
  /** Entity counts by type */
  entityCounts: {
    words: number
    definitions: number
    pronunciations: number
    translations: number
    relatedWords: number
    etymologies: number
    forms: number
    descendants: number
    compounds: number
    collocations: number
    cognates: number
    etymologyLinks: number
    frequencies: number
  }
  /** Files written */
  files: string[]
  /** Loader configuration used */
  config: WiktionaryLoaderConfig
}
