/**
 * Multi-Language Stemmers for Full-Text Search
 *
 * Provides Snowball-based stemmers for multiple languages:
 * - English (Porter)
 * - Spanish
 * - French
 * - German
 * - Italian
 * - Portuguese
 * - Dutch
 * - Russian
 * - Swedish
 * - Norwegian
 * - Danish
 * - Finnish
 * - Turkish
 * - Arabic (basic)
 */

import { porterStem } from './tokenizer'

// =============================================================================
// Supported Languages
// =============================================================================

export type SupportedLanguage =
  | 'english'
  | 'spanish'
  | 'french'
  | 'german'
  | 'italian'
  | 'portuguese'
  | 'dutch'
  | 'russian'
  | 'swedish'
  | 'norwegian'
  | 'danish'
  | 'finnish'
  | 'turkish'
  | 'arabic'

export type Stemmer = (word: string) => string

/**
 * Get stemmer function for a given language
 */
export function getStemmer(language: SupportedLanguage): Stemmer {
  switch (language) {
    case 'english':
      return porterStem
    case 'spanish':
      return spanishStem
    case 'french':
      return frenchStem
    case 'german':
      return germanStem
    case 'italian':
      return italianStem
    case 'portuguese':
      return portugueseStem
    case 'dutch':
      return dutchStem
    case 'russian':
      return russianStem
    case 'swedish':
      return swedishStem
    case 'norwegian':
      return norwegianStem
    case 'danish':
      return danishStem
    case 'finnish':
      return finnishStem
    case 'turkish':
      return turkishStem
    case 'arabic':
      return arabicStem
    default:
      return porterStem
  }
}

// =============================================================================
// Spanish Stemmer (Snowball)
// =============================================================================

/**
 * Spanish Snowball stemmer
 */
export function spanishStem(word: string): string {
  if (word.length <= 2) return word

  let stem = word.toLowerCase()

  // Remove attached pronouns
  const pronounSuffixes = [
    'selas', 'selos', 'nosla', 'noslo', 'oslas', 'oslos',
    'sela', 'selo', 'mela', 'melo', 'tela', 'telo', 'nos', 'les', 'las', 'los',
    'la', 'le', 'lo', 'me', 'se', 'te',
  ]
  for (const suffix of pronounSuffixes) {
    if (stem.length > suffix.length + 2 && stem.endsWith(suffix)) {
      const prefix = stem.slice(0, -suffix.length)
      if (/(?:ando|iendo|ar|er|ir)$/.test(prefix)) {
        stem = prefix
        break
      }
    }
  }

  // Step 1: Standard suffix removal
  const step1Suffixes: [string, string][] = [
    ['amientos', ''], ['imientos', ''], ['amiento', ''], ['imiento', ''],
    ['aciones', ''], ['uciones', ''], ['adores', ''], ['adoras', ''],
    ['ancias', ''], ['encias', ''], ['logías', 'log'], ['idades', ''],
    ['antes', ''], ['ación', ''], ['ución', ''], ['mente', ''],
    ['adora', ''], ['ador', ''], ['antes', ''], ['ancia', ''],
    ['encia', ''], ['logía', 'log'], ['idad', ''], ['able', ''],
    ['ible', ''], ['ante', ''], ['oso', ''], ['osa', ''],
    ['ivo', ''], ['iva', ''],
  ]

  for (const [suffix, replacement] of step1Suffixes) {
    if (stem.endsWith(suffix)) {
      stem = stem.slice(0, -suffix.length) + replacement
      break
    }
  }

  // Step 2: Verb suffixes
  const verbSuffixes = [
    'aríamos', 'eríamos', 'iríamos', 'iéramos', 'iésemos',
    'aremos', 'eremos', 'iremos', 'ábamos', 'aríais', 'eríais',
    'iríais', 'ierais', 'ieseis', 'asteis', 'isteis', 'ábais',
    'arías', 'erías', 'irías', 'ieran', 'iesen', 'ieron', 'iendo',
    'ieras', 'ieses', 'abais', 'arais', 'aseis', 'éamos',
    'amos', 'aron', 'aban', 'aran', 'asen', 'aste', 'iste',
    'ando', 'aron', 'aban', 'aran', 'asen', 'emos', 'imos',
    'aron', 'aría', 'ería', 'iría', 'iera', 'iese', 'aste',
    'iste', 'aban', 'aran', 'asen', 'ando', 'aron', 'aban',
    'ar', 'er', 'ir', 'as', 'es', 'ís', 'an', 'en',
    'ió', 'ía', 'ad', 'ed', 'id',
  ]

  for (const suffix of verbSuffixes) {
    if (stem.endsWith(suffix)) {
      stem = stem.slice(0, -suffix.length)
      break
    }
  }

  // Step 3: Residual suffix
  const residualSuffixes = ['os', 'as', 'es', 'o', 'a', 'e']
  for (const suffix of residualSuffixes) {
    if (stem.endsWith(suffix)) {
      stem = stem.slice(0, -suffix.length)
      break
    }
  }

  // Remove accents
  stem = removeAccents(stem)

  return stem
}

// =============================================================================
// French Stemmer (Snowball)
// =============================================================================

/**
 * French Snowball stemmer
 */
export function frenchStem(word: string): string {
  if (word.length <= 2) return word

  let stem = word.toLowerCase()
  stem = removeAccents(stem)

  // Mark vowels and find RV, R1, R2 regions
  const rv = findRV(stem)
  const r1 = findR1(stem)
  const r2 = findR2(stem, r1)

  // Step 1: Standard suffix removal
  const step1Suffixes: [string, string, number][] = [
    ['issements', '', r2],
    ['issement', '', r2],
    ['atrices', '', r2],
    ['atrice', '', r2],
    ['ateurs', '', r2],
    ['ations', '', r2],
    ['ateur', '', r2],
    ['ation', '', r2],
    ['ement', '', rv],
    ['ences', '', r2],
    ['ence', '', r2],
    ['ances', '', r2],
    ['ance', '', r2],
    ['ites', '', r2],
    ['ite', '', r2],
    ['ives', '', r2],
    ['ive', '', r2],
    ['ifs', '', r2],
    ['if', '', r2],
    ['euses', '', r2],
    ['euse', '', r2],
    ['eux', '', r2],
  ]

  for (const [suffix, replacement, region] of step1Suffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= region) {
      stem = stem.slice(0, -suffix.length) + replacement
      return stem
    }
  }

  // Step 2a: Verb suffixes (following i)
  const verbSuffixesA = [
    'issantes', 'issante', 'issants', 'issant', 'isses', 'issez',
    'issions', 'ission', 'issiez', 'issais', 'issait', 'issent',
    'isse', 'issi', 'ira', 'iras', 'irent', 'irez', 'irons',
    'iront', 'irai', 'irais', 'irait', 'iriez', 'irions', 'is', 'it',
  ]

  for (const suffix of verbSuffixesA) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= rv) {
      const prefix = stem.slice(0, -suffix.length)
      if (prefix.length > 0 && !/[aeiouy]/.test(prefix[prefix.length - 1]!)) {
        stem = prefix
        return stem
      }
    }
  }

  // Step 2b: Other verb suffixes
  const verbSuffixesB = [
    'erions', 'eriez', 'erent', 'erons', 'erait', 'erais', 'eront',
    'erai', 'eras', 'erez', 'ions', 'ait', 'ais', 'ant', 'ons',
    'ez', 'es', 'er', 'ie', 'ir', 'it', 'e',
  ]

  for (const suffix of verbSuffixesB) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= rv) {
      stem = stem.slice(0, -suffix.length)
      break
    }
  }

  // Step 3: Final cleanup
  if (stem.length > 0) {
    const last = stem[stem.length - 1]
    if (last === 'y') {
      stem = stem.slice(0, -1) + 'i'
    } else if (last === 'c' || last === 'ç') {
      stem = stem.slice(0, -1) + 'c'
    }
  }

  return stem
}

// =============================================================================
// German Stemmer (Snowball)
// =============================================================================

/**
 * German Snowball stemmer
 */
export function germanStem(word: string): string {
  if (word.length <= 2) return word

  let stem = word.toLowerCase()

  // Replace ß with ss
  stem = stem.replace(/ß/g, 'ss')

  // Replace umlauts
  stem = stem.replace(/ä/g, 'a').replace(/ö/g, 'o').replace(/ü/g, 'u')

  const r1 = findR1(stem)
  const r2 = findR2(stem, r1)

  // Step 1: Remove -em, -ern, -er, -en, -es, -e, -s (from R1)
  const step1Suffixes = ['ern', 'em', 'er', 'en', 'es', 'e', 's']
  for (const suffix of step1Suffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= r1) {
      if (suffix === 's') {
        // Only delete 's' if preceded by valid letter
        const char = stem[stem.length - 2]
        if (char && 'bdfghklmnrt'.includes(char)) {
          stem = stem.slice(0, -1)
        }
      } else {
        stem = stem.slice(0, -suffix.length)
      }
      break
    }
  }

  // Step 2: Remove -est, -er, -en (from R1)
  const step2Suffixes = ['est', 'er', 'en']
  for (const suffix of step2Suffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= r1) {
      stem = stem.slice(0, -suffix.length)
      break
    }
  }

  // Step 3: Remove derivational suffixes
  const step3Suffixes: [string, boolean][] = [
    ['isch', false],
    ['lich', false],
    ['heit', false],
    ['keit', false],
    ['ung', false],
    ['ig', true],
    ['ik', false],
  ]

  for (const [suffix, checkE] of step3Suffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= r2) {
      stem = stem.slice(0, -suffix.length)
      if (checkE && stem.endsWith('e') && stem.length - 1 >= r1) {
        stem = stem.slice(0, -1)
      }
      break
    }
  }

  return stem
}

// =============================================================================
// Italian Stemmer (Snowball)
// =============================================================================

/**
 * Italian Snowball stemmer
 */
export function italianStem(word: string): string {
  if (word.length <= 2) return word

  let stem = word.toLowerCase()
  stem = removeAccents(stem)

  const rv = findRV(stem)
  const r1 = findR1(stem)
  const r2 = findR2(stem, r1)

  // Step 0: Attached pronoun removal
  const pronounSuffixes = [
    'gliela', 'gliele', 'glieli', 'glielo', 'gliene',
    'sela', 'sele', 'seli', 'selo', 'sene', 'tela', 'tele', 'teli', 'telo', 'tene',
    'cela', 'cele', 'celi', 'celo', 'cene', 'vela', 'vele', 'veli', 'velo', 'vene',
    'mela', 'mele', 'meli', 'melo', 'mene',
    'ci', 'la', 'le', 'li', 'lo', 'mi', 'ne', 'si', 'ti', 'vi',
  ]

  for (const suffix of pronounSuffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= rv) {
      const prefix = stem.slice(0, -suffix.length)
      if (/(?:ando|endo|ar|er|ir)$/.test(prefix)) {
        stem = prefix
        break
      }
    }
  }

  // Step 1: Standard suffixes
  const step1Suffixes: [string, number][] = [
    ['amenti', r2], ['amento', r2], ['imenti', r2], ['imento', r2],
    ['azione', r2], ['azioni', r2], ['atore', r2], ['atori', r2],
    ['logia', r2], ['logie', r2], ['abile', r2], ['abili', r2],
    ['ibile', r2], ['ibili', r2], ['mente', r2], ['ista', r2],
    ['iste', r2], ['isti', r2], ['ismo', r2], ['ismi', r2],
    ['anza', r2], ['anze', r2], ['enza', r2], ['enze', r2],
    ['ico', r2], ['ici', r2], ['ica', r2], ['ice', r2], ['iche', r2], ['ichi', r2],
    ['oso', r2], ['osi', r2], ['osa', r2], ['ose', r2],
    ['ivo', r2], ['ivi', r2], ['iva', r2], ['ive', r2],
  ]

  for (const [suffix, region] of step1Suffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= region) {
      stem = stem.slice(0, -suffix.length)
      return stem
    }
  }

  // Step 2: Verb suffixes
  const verbSuffixes = [
    'erebbero', 'irebbero', 'assero', 'essero', 'issero',
    'eranno', 'iranno', 'erebbe', 'irebbe', 'assimo',
    'emmo', 'immo', 'arono', 'erono', 'irono',
    'avano', 'evano', 'ivano', 'avate', 'evate', 'ivate',
    'ammo', 'ando', 'endo', 'isco', 'isca', 'isce', 'isci',
    'ante', 'ente', 'ava', 'eva', 'iva', 'ato', 'eto', 'ito',
    'ata', 'eta', 'ita', 'are', 'ere', 'ire', 'ano', 'ono',
    'ar', 'er', 'ir',
  ]

  for (const suffix of verbSuffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= rv) {
      stem = stem.slice(0, -suffix.length)
      break
    }
  }

  // Step 3: Final vowel
  if (stem.length > rv + 1 && /[aeiou]$/.test(stem)) {
    stem = stem.slice(0, -1)
  }

  return stem
}

// =============================================================================
// Portuguese Stemmer (Snowball)
// =============================================================================

/**
 * Portuguese Snowball stemmer
 */
export function portugueseStem(word: string): string {
  if (word.length <= 2) return word

  let stem = word.toLowerCase()
  stem = removeAccents(stem)

  const rv = findRV(stem)
  const r1 = findR1(stem)
  const r2 = findR2(stem, r1)

  // Step 1: Standard suffix removal
  const step1Suffixes: [string, number][] = [
    ['amentos', r2], ['imentos', r2], ['amento', r2], ['imento', r2],
    ['adoras', r2], ['adores', r2], ['adora', r2], ['ador', r2],
    ['acoes', r2], ['acao', r2], ['logias', r2], ['logia', r2],
    ['encias', r2], ['encia', r2], ['mente', r1], ['idade', r2],
    ['idades', r2], ['ivos', r2], ['ivas', r2], ['ivo', r2], ['iva', r2],
    ['osas', r2], ['osos', r2], ['osa', r2], ['oso', r2],
  ]

  for (const [suffix, region] of step1Suffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= region) {
      stem = stem.slice(0, -suffix.length)
      return stem
    }
  }

  // Step 2: Verb suffixes
  const verbSuffixes = [
    'ariamos', 'eriamos', 'iriamos', 'assemos', 'essemos', 'issemos',
    'aremos', 'eremos', 'iremos', 'avamos', 'aramos', 'eramos', 'iramos',
    'areis', 'ereis', 'ireis', 'asseis', 'esseis', 'isseis',
    'arieis', 'erieis', 'irieis', 'avam', 'arem', 'erem', 'irem',
    'ando', 'endo', 'indo', 'adas', 'idos', 'adas', 'idos',
    'arao', 'erao', 'irao', 'aria', 'eria', 'iria', 'asse', 'esse', 'isse',
    'ara', 'era', 'ira', 'ava', 'ada', 'ida', 'ado', 'ido',
    'ar', 'er', 'ir', 'as', 'es', 'is', 'am', 'em', 'ei', 'ou',
  ]

  for (const suffix of verbSuffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= rv) {
      stem = stem.slice(0, -suffix.length)
      break
    }
  }

  // Step 3: Final cleanup
  if (stem.endsWith('i') && stem.length > rv + 1 && stem[stem.length - 2] === 'c') {
    stem = stem.slice(0, -1)
  }

  return stem
}

// =============================================================================
// Dutch Stemmer (Snowball)
// =============================================================================

/**
 * Dutch Snowball stemmer
 */
export function dutchStem(word: string): string {
  if (word.length <= 2) return word

  let stem = word.toLowerCase()

  // Replace special characters
  stem = stem.replace(/ä/g, 'a').replace(/ë/g, 'e').replace(/ï/g, 'i')
    .replace(/ö/g, 'o').replace(/ü/g, 'u').replace(/ij/g, 'y')

  const r1 = Math.max(findR1(stem), 3)
  const r2 = findR2(stem, r1)

  // Step 1: Remove -heden, -heid, -en, -e (from R1)
  if (stem.endsWith('heden') && stem.length - 5 >= r1) {
    stem = stem.slice(0, -5) + 'heid'
  } else if (stem.endsWith('heid') && stem.length - 4 >= r2) {
    stem = stem.slice(0, -4)
    if (stem.endsWith('c')) {
      // Keep
    }
  } else if (stem.endsWith('en') && stem.length - 2 >= r1) {
    const prefix = stem.slice(0, -2)
    if (prefix.length > 0 && !/[aeiou]$/.test(prefix)) {
      stem = prefix
      // Undouble consonant
      if (stem.length >= 2 && stem[stem.length - 1] === stem[stem.length - 2]) {
        stem = stem.slice(0, -1)
      }
    }
  } else if (stem.endsWith('e') && stem.length - 1 >= r1) {
    const prefix = stem.slice(0, -1)
    if (prefix.length > 0 && !/[aeiou]$/.test(prefix)) {
      stem = prefix
      // Undouble consonant
      if (stem.length >= 2 && stem[stem.length - 1] === stem[stem.length - 2]) {
        stem = stem.slice(0, -1)
      }
    }
  }

  // Step 2: Remove -end, -ing (from R1)
  if ((stem.endsWith('end') || stem.endsWith('ing')) && stem.length - 3 >= r1) {
    stem = stem.slice(0, -3)
    if (stem.endsWith('ig') && stem.length - 2 >= r2 && !stem.endsWith('eig')) {
      stem = stem.slice(0, -2)
    } else {
      // Undouble
      if (stem.length >= 2 && stem[stem.length - 1] === stem[stem.length - 2]) {
        stem = stem.slice(0, -1)
      }
    }
  }

  // Step 3: Remove -ig, -lijk, -baar (from R2)
  const step3Suffixes = ['lijk', 'baar', 'ig']
  for (const suffix of step3Suffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= r2) {
      if (suffix === 'ig' && stem.endsWith('eig')) continue
      stem = stem.slice(0, -suffix.length)
      if (suffix === 'lijk' && stem.endsWith('e') && stem.length - 1 >= r1) {
        const prefix = stem.slice(0, -1)
        if (!/[aeiou]$/.test(prefix)) {
          stem = prefix
        }
      }
      break
    }
  }

  return stem
}

// =============================================================================
// Russian Stemmer (Snowball)
// =============================================================================

/**
 * Russian Snowball stemmer (simplified)
 */
export function russianStem(word: string): string {
  if (word.length <= 2) return word

  let stem = word.toLowerCase()
  const rv = findRVCyrillic(stem)

  // Step 1: Perfect gerund endings
  const perfectGerundEndings = [
    'ившись', 'ывшись', 'ивши', 'ывши', 'вшись', 'вши', 'ив', 'ыв', 'в',
  ]
  for (const ending of perfectGerundEndings) {
    if (stem.endsWith(ending) && stem.length - ending.length >= rv) {
      stem = stem.slice(0, -ending.length)
      return stem
    }
  }

  // Step 2: Reflexive endings
  if ((stem.endsWith('ся') || stem.endsWith('сь')) && stem.length - 2 >= rv) {
    stem = stem.slice(0, -2)
  }

  // Step 3: Adjectival endings
  const adjEndings = [
    'ейшими', 'ейшая', 'ейшее', 'ейший', 'ейшие', 'ейшую',
    'ающими', 'ующими', 'ящими', 'ющими',
    'авшая', 'авшее', 'авший', 'авшие', 'авшую',
    'ившая', 'ившее', 'ивший', 'ившие', 'ившую',
    'ывшая', 'ывшее', 'ывший', 'ывшие', 'ывшую',
    'ающая', 'ающее', 'ающий', 'ающие', 'ающую',
    'ующая', 'ующее', 'ующий', 'ующие', 'ующую',
    'ящая', 'ящее', 'ящий', 'ящие', 'ящую',
    'ющая', 'ющее', 'ющий', 'ющие', 'ющую',
    'ему', 'ому', 'его', 'ого', 'ими', 'ами', 'ыми',
    'ей', 'ий', 'ый', 'ой', 'ая', 'яя', 'ую', 'юю',
    'ее', 'ие', 'ые', 'ое', 'ем', 'им', 'ым', 'ом',
  ]
  for (const ending of adjEndings) {
    if (stem.endsWith(ending) && stem.length - ending.length >= rv) {
      stem = stem.slice(0, -ending.length)
      return stem
    }
  }

  // Step 4: Verb endings
  const verbEndings = [
    'ейте', 'уйте', 'ите', 'йте', 'ете', 'ете',
    'ала', 'али', 'ало', 'ать', 'ана', 'ано', 'ают',
    'яла', 'яли', 'яло', 'ять', 'яна', 'яно', 'яют',
    'ила', 'или', 'ило', 'ить', 'ена', 'ено',
    'ыла', 'ыли', 'ыло', 'ыть',
    'ула', 'ули', 'уло', 'уть',
    'ует', 'ует', 'ют', 'ят', 'ат', 'ет', 'ит', 'ут',
    'ла', 'ли', 'ло', 'на', 'но', 'ть', 'й', 'л', 'н',
  ]
  for (const ending of verbEndings) {
    if (stem.endsWith(ending) && stem.length - ending.length >= rv) {
      stem = stem.slice(0, -ending.length)
      return stem
    }
  }

  // Step 5: Noun endings
  const nounEndings = [
    'иями', 'ями', 'ами', 'ией', 'ьей', 'ием', 'ьем',
    'иях', 'ях', 'ах', 'ии', 'ьи', 'ию', 'ью', 'ия', 'ья',
    'ев', 'ов', 'ей', 'ий', 'ой', 'ем', 'им', 'ом', 'ым',
    'ей', 'ий', 'ый', 'ой', 'ая', 'яя', 'ую', 'юю',
    'е', 'и', 'о', 'у', 'ы', 'ю', 'я', 'ь', 'а',
  ]
  for (const ending of nounEndings) {
    if (stem.endsWith(ending) && stem.length - ending.length >= rv) {
      stem = stem.slice(0, -ending.length)
      break
    }
  }

  // Step 6: Derivational suffixes
  if (stem.endsWith('ость') || stem.endsWith('ост')) {
    stem = stem.slice(0, stem.endsWith('ость') ? -4 : -3)
  }

  // Step 7: Remove trailing и or ь
  if (stem.endsWith('и') || stem.endsWith('ь')) {
    stem = stem.slice(0, -1)
  }

  return stem
}

// =============================================================================
// Swedish Stemmer (Snowball)
// =============================================================================

/**
 * Swedish Snowball stemmer
 */
export function swedishStem(word: string): string {
  if (word.length <= 2) return word

  let stem = word.toLowerCase()
  stem = stem.replace(/ä/g, 'a').replace(/å/g, 'a').replace(/ö/g, 'o')

  const r1 = findR1(stem)

  // Step 1: Remove suffixes
  const suffixes = [
    'astens', 'andets', 'arens', 'ernas', 'ornas', 'arnas',
    'hetens', 'astens', 'andes', 'orens', 'adens',
    'andes', 'andet', 'arens', 'ernas', 'ornas', 'arnas',
    'anden', 'arena', 'arna', 'erna', 'orna', 'ande', 'arna',
    'aste', 'enas', 'ades', 'ades', 'erna', 'orna',
    'aren', 'ades', 'ades', 'arna', 'erna', 'orna',
    'ens', 'ade', 'are', 'ast', 'het', 'and', 'ens',
    'ad', 'ar', 'er', 'or', 'en', 'es', 'at', 'as',
    'a', 'e',
  ]

  for (const suffix of suffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= r1) {
      if (suffix === 's') {
        const prev = stem[stem.length - 2]
        if (prev && 'bcdfghjklmnoprtvy'.includes(prev)) {
          stem = stem.slice(0, -1)
        }
      } else {
        stem = stem.slice(0, -suffix.length)
      }
      break
    }
  }

  // Step 2: Remove final suffix -s
  if (stem.endsWith('s') && stem.length > r1) {
    const prev = stem[stem.length - 2]
    if (prev && 'bcdfghjklmnoprtvy'.includes(prev)) {
      stem = stem.slice(0, -1)
    }
  }

  // Step 3: Remove -lig, -ig, -els (from R1)
  const step3Suffixes = ['lig', 'els', 'ig']
  for (const suffix of step3Suffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= r1) {
      stem = stem.slice(0, -suffix.length)
      break
    }
  }

  return stem
}

// =============================================================================
// Norwegian Stemmer (Snowball)
// =============================================================================

/**
 * Norwegian Snowball stemmer
 */
export function norwegianStem(word: string): string {
  if (word.length <= 2) return word

  let stem = word.toLowerCase()
  stem = stem.replace(/æ/g, 'a').replace(/ø/g, 'o').replace(/å/g, 'a')

  const r1 = findR1(stem)

  // Step 1: Remove suffixes
  const suffixes = [
    'hetenes', 'hetene', 'hetens', 'heten', 'heter', 'endes',
    'andes', 'enes', 'erte', 'ende', 'ande', 'enes',
    'ane', 'ene', 'ede', 'ets', 'het', 'ast', 'ert',
    'en', 'ar', 'er', 'et', 'es', 'as', 'a', 'e',
  ]

  for (const suffix of suffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= r1) {
      if (suffix === 's') {
        const prev = stem[stem.length - 2]
        if (prev && 'bcdfghjklmnoprtvyz'.includes(prev)) {
          stem = stem.slice(0, -1)
        }
      } else {
        stem = stem.slice(0, -suffix.length)
      }
      break
    }
  }

  // Step 2: Remove final suffix -s
  if (stem.endsWith('s') && stem.length > r1) {
    const prev = stem[stem.length - 2]
    if (prev && 'bcdfghjklmnoprtvyz'.includes(prev)) {
      stem = stem.slice(0, -1)
    }
  }

  // Step 3: Remove -lig, -ig, -els (from R1)
  const step3Suffixes = ['lig', 'els', 'ig']
  for (const suffix of step3Suffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= r1) {
      stem = stem.slice(0, -suffix.length)
      break
    }
  }

  return stem
}

// =============================================================================
// Danish Stemmer (Snowball)
// =============================================================================

/**
 * Danish Snowball stemmer
 */
export function danishStem(word: string): string {
  if (word.length <= 2) return word

  let stem = word.toLowerCase()
  stem = stem.replace(/æ/g, 'a').replace(/ø/g, 'o').replace(/å/g, 'a')

  const r1 = findR1(stem)

  // Step 1: Remove suffixes
  const suffixes = [
    'erendes', 'erende', 'hedens', 'ethed', 'erede', 'heder', 'heden',
    'endes', 'ernes', 'erens', 'erets', 'ered', 'ende', 'erne', 'eren',
    'eret', 'enes', 'hed', 'ene', 'ere', 'ens', 'ers', 'ets',
    'en', 'er', 'es', 'et', 'e',
  ]

  for (const suffix of suffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= r1) {
      if (suffix === 's') {
        const prev = stem[stem.length - 2]
        if (prev && 'abcdfghjklmnoprtvyz'.includes(prev)) {
          stem = stem.slice(0, -1)
        }
      } else {
        stem = stem.slice(0, -suffix.length)
      }
      break
    }
  }

  // Step 2: Remove final suffix -s
  if (stem.endsWith('s') && stem.length > r1) {
    const prev = stem[stem.length - 2]
    if (prev && 'abcdfghjklmnoprtvyz'.includes(prev)) {
      stem = stem.slice(0, -1)
    }
  }

  // Step 3: Remove -igst, -lig, -ig (from R1)
  const step3Suffixes = ['igst', 'lig', 'ig']
  for (const suffix of step3Suffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= r1) {
      stem = stem.slice(0, -suffix.length)
      break
    }
  }

  // Step 4: Undouble
  if (stem.length >= 2 && stem[stem.length - 1] === stem[stem.length - 2]) {
    stem = stem.slice(0, -1)
  }

  return stem
}

// =============================================================================
// Finnish Stemmer (Snowball)
// =============================================================================

/**
 * Finnish Snowball stemmer (simplified)
 */
export function finnishStem(word: string): string {
  if (word.length <= 2) return word

  let stem = word.toLowerCase()
  stem = stem.replace(/ä/g, 'a').replace(/ö/g, 'o')

  const r1 = findR1(stem)
  const r2 = findR2(stem, r1)

  // Step 1: Remove case endings
  const caseEndings = [
    'kinaan', 'kaan', 'kin', 'han', 'hen', 'hin', 'hon', 'hun', 'hyn',
    'kaan', 'keen', 'kiin', 'koon', 'kuun', 'kyn',
    'ssa', 'ssa', 'sta', 'sta', 'lla', 'lla', 'lta', 'lta',
    'lle', 'na', 'na', 'ta', 'ta', 'a', 'a', 'n', 't',
  ]

  for (const ending of caseEndings) {
    if (stem.endsWith(ending) && stem.length - ending.length >= r1) {
      stem = stem.slice(0, -ending.length)
      break
    }
  }

  // Step 2: Remove possessive suffixes
  const possessiveSuffixes = [
    'mme', 'nne', 'nsa', 'nsa', 'si', 'ni', 'an', 'en',
  ]

  for (const suffix of possessiveSuffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= r1) {
      stem = stem.slice(0, -suffix.length)
      break
    }
  }

  // Step 3: Remove derivational suffixes
  const derivationalSuffixes = ['ja', 'inen', 'lainen', 'llinen', 'sti']
  for (const suffix of derivationalSuffixes) {
    if (stem.endsWith(suffix) && stem.length - suffix.length >= r2) {
      stem = stem.slice(0, -suffix.length)
      break
    }
  }

  return stem
}

// =============================================================================
// Turkish Stemmer
// =============================================================================

/**
 * Turkish stemmer (simplified)
 */
export function turkishStem(word: string): string {
  if (word.length <= 2) return word

  let stem = word.toLowerCase()

  // Turkish vowel harmony groups
  const suffixes = [
    // Nominal suffixes
    'lardan', 'lerden', 'larin', 'lerin', 'lara', 'lere',
    'lar', 'ler', 'dan', 'den', 'tan', 'ten', 'da', 'de', 'ta', 'te',
    'in', 'in', 'un', 'un', 'a', 'e', 'i', 'i', 'u', 'u',
    // Verb suffixes
    'yorlar', 'yorsun', 'yoruz', 'yorum', 'iyor', 'uyor', 'yor',
    'ecek', 'acak', 'mek', 'mak', 'mis', 'mus', 'di', 'du', 'ti', 'tu',
  ]

  for (const suffix of suffixes) {
    if (stem.endsWith(suffix) && stem.length > suffix.length + 2) {
      stem = stem.slice(0, -suffix.length)
      break
    }
  }

  return stem
}

// =============================================================================
// Arabic Stemmer (Basic)
// =============================================================================

/**
 * Arabic light stemmer (simplified)
 * Removes common prefixes and suffixes
 */
export function arabicStem(word: string): string {
  if (word.length <= 2) return word

  let stem = word

  // Remove definite article al-
  if (stem.startsWith('\u0627\u0644')) {
    // ال (al-)
    stem = stem.slice(2)
  }

  // Remove common prefixes
  const prefixes = [
    '\u0648', // و (wa-)
    '\u0641', // ف (fa-)
    '\u0628', // ب (bi-)
    '\u0643', // ك (ka-)
    '\u0644', // ل (li-)
    '\u0633', // س (sa-)
  ]
  for (const prefix of prefixes) {
    if (stem.startsWith(prefix) && stem.length > 3) {
      stem = stem.slice(1)
      break
    }
  }

  // Remove common suffixes
  const suffixes = [
    '\u0648\u0646', // ون (-un)
    '\u0627\u062a', // ات (-at)
    '\u064a\u0646', // ين (-in)
    '\u0629', // ة (-a)
    '\u0647', // ه (-h)
    '\u064a', // ي (-i)
  ]
  for (const suffix of suffixes) {
    if (stem.endsWith(suffix) && stem.length > suffix.length + 2) {
      stem = stem.slice(0, -suffix.length)
      break
    }
  }

  return stem
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Find R1 region (after first vowel-consonant sequence)
 */
function findR1(word: string): number {
  const vowels = 'aeiouyäöü'
  let i = 0

  // Skip initial vowels
  while (i < word.length && !vowels.includes(word[i]!)) {
    i++
  }
  // Skip vowels
  while (i < word.length && vowels.includes(word[i]!)) {
    i++
  }

  return i
}

/**
 * Find R2 region (R1 of R1)
 */
function findR2(word: string, r1: number): number {
  if (r1 >= word.length) return word.length
  const r1Region = word.slice(r1)
  return r1 + findR1(r1Region)
}

/**
 * Find RV region for Romance languages
 */
function findRV(word: string): number {
  const vowels = 'aeiouáéíóúàèìòùâêîôûäëïöü'

  if (word.length < 2) return word.length

  // If second letter is consonant, RV is after next vowel
  if (!vowels.includes(word[1]!)) {
    for (let i = 2; i < word.length; i++) {
      if (vowels.includes(word[i]!)) {
        return i + 1
      }
    }
    return word.length
  }

  // If first two letters are vowels, RV is after next consonant
  if (vowels.includes(word[0]!) && vowels.includes(word[1]!)) {
    for (let i = 2; i < word.length; i++) {
      if (!vowels.includes(word[i]!)) {
        return i + 1
      }
    }
    return word.length
  }

  // Otherwise RV is after position 3
  return 3
}

/**
 * Find RV for Cyrillic text (Russian)
 */
function findRVCyrillic(word: string): number {
  const vowels = '\u0430\u0435\u0438\u043e\u0443\u044b\u044d\u044e\u044f\u0451' // аеиоуыэюяё

  for (let i = 0; i < word.length; i++) {
    if (vowels.includes(word[i]!)) {
      return i + 1
    }
  }
  return word.length
}

/**
 * Remove accents from text
 */
function removeAccents(text: string): string {
  return text
    .replace(/[áàâäã]/g, 'a')
    .replace(/[éèêë]/g, 'e')
    .replace(/[íìîï]/g, 'i')
    .replace(/[óòôöõ]/g, 'o')
    .replace(/[úùûü]/g, 'u')
    .replace(/[ñ]/g, 'n')
    .replace(/[ç]/g, 'c')
}
