/**
 * Tokenizer for Full-Text Search
 *
 * Provides text tokenization with:
 * - Lowercase normalization
 * - Whitespace/punctuation splitting
 * - Stopword removal
 * - Multi-language stemming (Porter, Snowball)
 */

import type { Token, TokenizerOptions } from './types'
import { getStemmer, type SupportedLanguage, type Stemmer } from './stemmers'

// =============================================================================
// Default Stopwords by Language
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

/**
 * Spanish stopwords
 */
export const SPANISH_STOPWORDS = new Set([
  'de', 'la', 'que', 'el', 'en', 'y', 'a', 'los', 'del', 'se', 'las',
  'por', 'un', 'para', 'con', 'no', 'una', 'su', 'al', 'lo', 'como',
  'mas', 'pero', 'sus', 'le', 'ya', 'o', 'este', 'si', 'porque', 'esta',
  'entre', 'cuando', 'muy', 'sin', 'sobre', 'ser', 'tiene', 'tambien',
  'me', 'hasta', 'hay', 'donde', 'quien', 'desde', 'todo', 'nos', 'durante',
  'todos', 'uno', 'les', 'ni', 'contra', 'otros', 'ese', 'eso', 'ante',
  'ellos', 'e', 'esto', 'mi', 'antes', 'algunos', 'que', 'unos', 'yo',
  'otro', 'otras', 'otra', 'el', 'tanto', 'esa', 'estos', 'mucho', 'quienes',
  'nada', 'muchos', 'cual', 'poco', 'ella', 'estar', 'estas', 'algunas',
  'algo', 'nosotros', 'tu', 'ellas', 'ambos', 'quien', 'mas', 'cual',
])

/**
 * French stopwords
 */
export const FRENCH_STOPWORDS = new Set([
  'le', 'la', 'les', 'de', 'du', 'des', 'un', 'une', 'et', 'est', 'en',
  'que', 'qui', 'dans', 'ce', 'il', 'ne', 'sur', 'se', 'au', 'par', 'pour',
  'pas', 'sont', 'avec', 'plus', 'son', 'sa', 'ses', 'mais', 'ou', 'cette',
  'on', 'elle', 'comme', 'tout', 'nous', 'lui', 'leur', 'fait', 'aux',
  'ont', 'ces', 'meme', 'peut', 'bien', 'aussi', 'sans', 'entre', 'apres',
  'sous', 'tous', 'je', 'tu', 'vous', 'ils', 'elles', 'mes', 'tes', 'nos',
  'vos', 'leurs', 'mon', 'ton', 'notre', 'votre', 'si', 'non', 'oui',
  'donc', 'car', 'ni', 'soit', 'quand', 'quel', 'quelle', 'quels', 'quelles',
])

/**
 * German stopwords
 */
export const GERMAN_STOPWORDS = new Set([
  'der', 'die', 'das', 'und', 'in', 'zu', 'den', 'von', 'ist', 'mit',
  'sich', 'des', 'auf', 'fur', 'nicht', 'als', 'auch', 'es', 'an', 'er',
  'hat', 'aus', 'bei', 'eine', 'einem', 'einer', 'eines', 'ein', 'dem',
  'war', 'werden', 'noch', 'wird', 'nach', 'am', 'sind', 'im', 'oder',
  'wie', 'sein', 'dass', 'sie', 'so', 'wenn', 'uber', 'haben', 'aber',
  'nur', 'ich', 'du', 'wir', 'ihr', 'mir', 'dir', 'mich', 'dich', 'uns',
  'euch', 'was', 'wer', 'wo', 'wann', 'warum', 'welche', 'welcher', 'welches',
])

/**
 * Italian stopwords
 */
export const ITALIAN_STOPWORDS = new Set([
  'il', 'lo', 'la', 'i', 'gli', 'le', 'di', 'a', 'da', 'in', 'con', 'su',
  'per', 'tra', 'fra', 'un', 'uno', 'una', 'e', 'che', 'non', 'si', 'come',
  'ma', 'del', 'della', 'dello', 'dei', 'degli', 'delle', 'al', 'alla',
  'allo', 'ai', 'agli', 'alle', 'dal', 'dalla', 'dallo', 'dai', 'dagli',
  'dalle', 'nel', 'nella', 'nello', 'nei', 'negli', 'nelle', 'sul', 'sulla',
  'sullo', 'sui', 'sugli', 'sulle', 'essere', 'avere', 'sono', 'sei', 'ha',
  'ho', 'hai', 'abbiamo', 'avete', 'hanno', 'questo', 'questa', 'questi',
  'queste', 'quello', 'quella', 'quelli', 'quelle', 'io', 'tu', 'lui', 'lei',
  'noi', 'voi', 'loro', 'mio', 'mia', 'miei', 'mie', 'tuo', 'tua', 'tuoi', 'tue',
])

/**
 * Portuguese stopwords
 */
export const PORTUGUESE_STOPWORDS = new Set([
  'o', 'a', 'os', 'as', 'um', 'uma', 'uns', 'umas', 'de', 'da', 'do', 'das',
  'dos', 'em', 'na', 'no', 'nas', 'nos', 'por', 'para', 'com', 'sem', 'sob',
  'sobre', 'que', 'e', 'mas', 'ou', 'se', 'como', 'quando', 'onde', 'porque',
  'eu', 'tu', 'ele', 'ela', 'nos', 'vos', 'eles', 'elas', 'meu', 'minha',
  'meus', 'minhas', 'teu', 'tua', 'teus', 'tuas', 'seu', 'sua', 'seus', 'suas',
  'este', 'esta', 'estes', 'estas', 'esse', 'essa', 'esses', 'essas', 'aquele',
  'aquela', 'aqueles', 'aquelas', 'isto', 'isso', 'aquilo', 'ser', 'estar',
  'ter', 'haver', 'foi', 'era', 'tinha', 'havia', 'sido', 'sendo', 'tendo',
])

/**
 * Dutch stopwords
 */
export const DUTCH_STOPWORDS = new Set([
  'de', 'en', 'van', 'ik', 'te', 'dat', 'die', 'in', 'een', 'hij', 'het',
  'niet', 'zijn', 'is', 'was', 'op', 'aan', 'met', 'als', 'voor', 'had',
  'er', 'maar', 'om', 'hem', 'dan', 'zou', 'of', 'wat', 'mijn', 'men',
  'dit', 'zo', 'door', 'over', 'ze', 'zich', 'bij', 'ook', 'tot', 'je',
  'mij', 'uit', 'der', 'daar', 'haar', 'naar', 'heb', 'hoe', 'heeft',
  'hebben', 'deze', 'u', 'want', 'nog', 'zal', 'me', 'zij', 'nu', 'ge',
  'geen', 'omdat', 'iets', 'worden', 'toch', 'al', 'waren', 'veel', 'meer',
])

/**
 * Russian stopwords
 */
export const RUSSIAN_STOPWORDS = new Set([
  '\u0438', '\u0432', '\u043d\u0435', '\u043d\u0430', '\u044f', '\u0431\u044b\u0442\u044c',
  '\u043e\u043d', '\u0441', '\u0447\u0442\u043e', '\u0430', '\u043f\u043e', '\u044d\u0442\u043e',
  '\u043e\u043d\u0430', '\u043a', '\u0442\u044b', '\u043c\u044b', '\u0432\u044b',
  '\u043e\u043d\u0438', '\u0432\u0435\u0441\u044c', '\u0435\u0433\u043e', '\u0437\u0430',
  '\u043e\u0442', '\u0442\u0430\u043a', '\u0435\u0435', '\u0442\u043e\u043b\u044c\u043a\u043e',
  '\u0438\u043b\u0438', '\u043c\u043d\u0435', '\u0431\u044b\u043b\u043e', '\u0431\u044b\u043b\u0430',
  '\u0443', '\u0436\u0435', '\u0432\u043e\u0442', '\u043e', '\u043d\u043e', '\u0434\u0430',
  '\u043a\u0430\u043a', '\u0441\u0435\u0431\u044f', '\u0434\u043b\u044f', '\u0435\u0449\u0435',
  '\u0431\u0435\u0437', '\u043c\u043e\u0436\u043d\u043e', '\u043a\u043e\u0433\u0434\u0430',
  '\u0443\u0436\u0435', '\u043d\u0443', '\u0442\u0435\u043f\u0435\u0440\u044c',
])

/**
 * Swedish stopwords
 */
export const SWEDISH_STOPWORDS = new Set([
  'och', 'det', 'att', 'i', 'en', 'jag', 'hon', 'som', 'han', 'pa', 'den',
  'med', 'var', 'sig', 'for', 'sa', 'till', 'ar', 'men', 'ett', 'om', 'hade',
  'de', 'av', 'icke', 'mig', 'du', 'henne', 'da', 'sin', 'nu', 'har', 'inte',
  'hans', 'honom', 'skulle', 'hennes', 'dar', 'min', 'man', 'ej', 'vid', 'kunde',
  'nagot', 'fran', 'ut', 'nar', 'efter', 'upp', 'vi', 'dem', 'vara', 'vart',
  'varit', 'bli', 'blir', 'blev', 'blivit', 'kunna', 'under', 'maste', 'nagra',
])

/**
 * Norwegian stopwords
 */
export const NORWEGIAN_STOPWORDS = new Set([
  'og', 'i', 'jeg', 'det', 'at', 'en', 'et', 'den', 'til', 'er', 'som',
  'pa', 'de', 'med', 'han', 'av', 'ikke', 'der', 'sa', 'var', 'meg',
  'seg', 'men', 'ett', 'har', 'om', 'vi', 'min', 'mitt', 'ha', 'hade',
  'hun', 'na', 'over', 'da', 'ved', 'fra', 'du', 'ut', 'sin', 'dem',
  'oss', 'opp', 'man', 'kan', 'hans', 'hvor', 'eller', 'hva', 'skal', 'selv',
  'sjol', 'her', 'alle', 'vil', 'bli', 'ble', 'blitt', 'kunne', 'inn', 'nar',
])

/**
 * Danish stopwords
 */
export const DANISH_STOPWORDS = new Set([
  'og', 'i', 'jeg', 'det', 'at', 'en', 'den', 'til', 'er', 'som', 'pa',
  'de', 'med', 'han', 'af', 'ikke', 'der', 'sa', 'var', 'mig', 'sig',
  'men', 'et', 'har', 'om', 'vi', 'min', 'havde', 'ham', 'hun', 'nu',
  'over', 'da', 'fra', 'du', 'ud', 'sin', 'dem', 'os', 'op', 'man',
  'hans', 'hvor', 'eller', 'hvad', 'skal', 'selv', 'her', 'alle', 'vil',
  'blev', 'kunne', 'ind', 'nar', 'vaere', 'dog', 'noget', 'ville', 'jo', 'deres',
])

/**
 * Finnish stopwords
 */
export const FINNISH_STOPWORDS = new Set([
  'ja', 'on', 'ei', 'ole', 'han', 'se', 'etta', 'kun', 'niin', 'kuin',
  'mutta', 'tai', 'jos', 'vain', 'ovat', 'oli', 'myos', 'sita', 'nama',
  'joka', 'mika', 'olla', 'hanen', 'tama', 'ne', 'jotka', 'etta', 'mutta',
  'tai', 'sitten', 'koska', 'ennen', 'tanaan', 'huomenna', 'eilen', 'mina',
  'sina', 'me', 'te', 'he', 'minun', 'sinun', 'hanen', 'meidan', 'teidan',
  'heidan', 'tahan', 'siina', 'tassa', 'nyt', 'jo', 'viela', 'aina', 'kaikki',
])

/**
 * Turkish stopwords
 */
export const TURKISH_STOPWORDS = new Set([
  've', 'bir', 'bu', 'da', 'de', 'ne', 'icin', 'ile', 'mi', 'mu', 'gibi',
  'ama', 'ancak', 'daha', 'en', 'her', 'o', 'su', 'sonra', 'bazi', 'hem',
  'kadar', 'cok', 'biri', 'var', 'yok', 'olan', 'olarak', 'ise', 'ya',
  'ben', 'sen', 'biz', 'siz', 'onlar', 'benim', 'senin', 'onun', 'bizim',
  'sizin', 'onlarin', 'kim', 'kime', 'neden', 'nasil', 'nerede', 'ne zaman',
  'hangisi', 'hangi', 'ki', 'eger', 'fakat', 'veya', 'ya da', 'hem de',
])

/**
 * Arabic stopwords
 */
export const ARABIC_STOPWORDS = new Set([
  '\u0648', '\u0641\u064a', '\u0645\u0646', '\u0639\u0644\u0649', '\u0623\u0646',
  '\u0625\u0644\u0649', '\u0627\u0644\u062a\u064a', '\u0627\u0644\u0630\u064a',
  '\u0647\u0630\u0627', '\u0647\u0630\u0647', '\u0645\u0639', '\u0643\u0627\u0646',
  '\u0644\u0627', '\u0639\u0646', '\u0628\u0639\u062f', '\u0642\u0628\u0644', '\u0628\u064a\u0646',
  '\u0623\u0648', '\u0623\u064a', '\u0643\u0644', '\u0644\u0645', '\u0645\u0627',
  '\u0647\u0648', '\u0647\u064a', '\u0647\u0645', '\u0623\u0646\u0627', '\u0623\u0646\u062a',
  '\u0646\u062d\u0646', '\u0623\u0646\u062a\u0645', '\u0644\u0643\u0646', '\u062b\u0645',
  '\u062d\u062a\u0649', '\u0625\u0630\u0627', '\u0644\u0623\u0646', '\u0644\u0643\u064a',
])

/**
 * Map of language codes to stopword sets
 */
export const STOPWORDS_BY_LANGUAGE: Record<string, Set<string>> = {
  'en': ENGLISH_STOPWORDS,
  'english': ENGLISH_STOPWORDS,
  'es': SPANISH_STOPWORDS,
  'spanish': SPANISH_STOPWORDS,
  'fr': FRENCH_STOPWORDS,
  'french': FRENCH_STOPWORDS,
  'de': GERMAN_STOPWORDS,
  'german': GERMAN_STOPWORDS,
  'it': ITALIAN_STOPWORDS,
  'italian': ITALIAN_STOPWORDS,
  'pt': PORTUGUESE_STOPWORDS,
  'portuguese': PORTUGUESE_STOPWORDS,
  'nl': DUTCH_STOPWORDS,
  'dutch': DUTCH_STOPWORDS,
  'ru': RUSSIAN_STOPWORDS,
  'russian': RUSSIAN_STOPWORDS,
  'sv': SWEDISH_STOPWORDS,
  'swedish': SWEDISH_STOPWORDS,
  'no': NORWEGIAN_STOPWORDS,
  'norwegian': NORWEGIAN_STOPWORDS,
  'da': DANISH_STOPWORDS,
  'danish': DANISH_STOPWORDS,
  'fi': FINNISH_STOPWORDS,
  'finnish': FINNISH_STOPWORDS,
  'tr': TURKISH_STOPWORDS,
  'turkish': TURKISH_STOPWORDS,
  'ar': ARABIC_STOPWORDS,
  'arabic': ARABIC_STOPWORDS,
}

/**
 * Get stopwords for a given language
 * @param language - Language code or name
 * @returns Set of stopwords for the language
 */
export function getStopwords(language?: string): Set<string> {
  if (!language) return ENGLISH_STOPWORDS
  return STOPWORDS_BY_LANGUAGE[language.toLowerCase()] ?? ENGLISH_STOPWORDS
}

/**
 * Normalize language code to SupportedLanguage type
 */
function normalizeLanguage(language?: string): SupportedLanguage {
  if (!language) return 'english'

  const langMap: Record<string, SupportedLanguage> = {
    'en': 'english',
    'english': 'english',
    'es': 'spanish',
    'spanish': 'spanish',
    'fr': 'french',
    'french': 'french',
    'de': 'german',
    'german': 'german',
    'it': 'italian',
    'italian': 'italian',
    'pt': 'portuguese',
    'portuguese': 'portuguese',
    'nl': 'dutch',
    'dutch': 'dutch',
    'ru': 'russian',
    'russian': 'russian',
    'sv': 'swedish',
    'swedish': 'swedish',
    'no': 'norwegian',
    'norwegian': 'norwegian',
    'da': 'danish',
    'danish': 'danish',
    'fi': 'finnish',
    'finnish': 'finnish',
    'tr': 'turkish',
    'turkish': 'turkish',
    'ar': 'arabic',
    'arabic': 'arabic',
  }

  return langMap[language.toLowerCase()] ?? 'english'
}

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
    language,
    minWordLength = 2,
    maxWordLength = 50,
    stopwords,
    stem = true,
  } = options

  // Get language-specific stopwords if not explicitly provided
  const effectiveStopwords = stopwords ?? getStopwords(language)

  // Get language-specific stemmer
  const normalizedLang = normalizeLanguage(language)
  const stemmer: Stemmer = getStemmer(normalizedLang)

  const tokens: Token[] = []

  // Use Unicode-aware regex for non-Latin scripts (Russian, Arabic)
  // Standard regex for Latin scripts
  const wordRegex = normalizedLang === 'russian'
    ? /[\u0400-\u04FF\u0500-\u052F]+/g  // Cyrillic
    : normalizedLang === 'arabic'
    ? /[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]+/g  // Arabic
    : /[a-zA-Z0-9\u00C0-\u024F\u1E00-\u1EFF]+/g  // Latin + extended Latin (accents)

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
    if (effectiveStopwords.has(term)) {
      position++
      continue
    }

    // Apply stemming
    if (stem) {
      term = stemmer(term)
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
    language,
    minWordLength = 2,
    maxWordLength = 50,
    stem = true,
  } = options

  // Get language-specific stemmer
  const normalizedLang = normalizeLanguage(language)
  const stemmer: Stemmer = getStemmer(normalizedLang)

  const terms: string[] = []

  // Use Unicode-aware regex for non-Latin scripts (Russian, Arabic)
  // Standard regex for Latin scripts
  const wordRegex = normalizedLang === 'russian'
    ? /[\u0400-\u04FF\u0500-\u052F]+/g  // Cyrillic
    : normalizedLang === 'arabic'
    ? /[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]+/g  // Arabic
    : /[a-zA-Z0-9\u00C0-\u024F\u1E00-\u1EFF]+/g  // Latin + extended Latin (accents)

  let match: RegExpExecArray | null

  while ((match = wordRegex.exec(query)) !== null) {
    const word = match[0]

    if (word.length < minWordLength || word.length > maxWordLength) {
      continue
    }

    let term = word.toLowerCase()

    // Don't filter stopwords in queries - they might be intentional
    if (stem) {
      term = stemmer(term)
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
