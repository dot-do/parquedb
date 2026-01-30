# Wiktionary Example

Load and query Wiktionary data from [kaikki.org](https://kaikki.org/dictionary/) using ParqueDB.

This comprehensive example demonstrates:
- Streaming large JSONL files (10GB+)
- Parsing and transforming to 13 entity types
- Partitioned Parquet storage for efficient queries
- Cross-language features (translations, cognates, borrowed words)
- Etymology tracing and linguistic analysis
- Memory-efficient chunked processing

## Data Source

[Kaikki.org](https://kaikki.org/dictionary/) provides machine-readable Wiktionary extracts in JSONL format. The data is extracted using [wiktextract](https://github.com/tatuylonen/wiktextract) and includes:

- **500+ languages** with varying depth of data
- **Definitions** with glosses, examples, and semantic tags
- **Pronunciations** with IPA, audio files, rhymes
- **Translations** between languages
- **Etymology** and word origins
- **Inflected forms** (plurals, conjugations, declensions)
- **Semantic relationships** (synonyms, antonyms, hypernyms, etc.)
- **Cognates** across language families
- **Compound word** components

### Example Files

| Language | Entries | Size | URL |
|----------|---------|------|-----|
| English | 1.7M+ | ~2GB | [Download](https://kaikki.org/dictionary/English/kaikki.org-dictionary-English.jsonl) |
| All Languages | 9M+ | ~20GB | [Download](https://kaikki.org/dictionary/rawdata.html) |

## Entity Schema

The loader transforms Wiktionary entries into 13 entity types:

### Core Entities

#### Word

Main entry for a word in a specific language. One entry per word/language/part-of-speech combination.

```typescript
interface WordEntity {
  $id: string              // words/{lang}/{word}/{pos}
  $type: 'Word'
  word: string             // The headword
  language: string         // Full language name
  languageCode: string     // ISO code (en, es, de, etc.)
  pos: string              // Part of speech (noun, verb, adj, etc.)
  etymologyText?: string   // Etymology description
  senseCount: number       // Number of definitions
  translationCount: number // Number of translations
  hasAudio: boolean        // Has audio pronunciation
  hasIpa: boolean          // Has IPA transcription
}
```

#### Definition

Individual sense/meaning of a word. Multiple definitions per word.

```typescript
interface DefinitionEntity {
  $id: string              // definitions/{lang}/{word}/{pos}/{index}
  $type: 'Definition'
  wordId: string           // Parent word entity ID
  word: string
  pos: string
  senseIndex: number       // Order within word
  gloss: string            // Main definition text
  tags?: string[]          // Grammar, register, domain tags
  topics?: string[]        // Semantic domains
  examples?: string[]      // Usage examples
}
```

#### Pronunciation

IPA transcriptions and audio links.

```typescript
interface PronunciationEntity {
  $id: string              // pronunciations/{lang}/{word}/{index}
  $type: 'Pronunciation'
  wordId: string
  word: string
  ipa?: string             // IPA transcription
  audioOgg?: string        // Ogg audio URL
  audioMp3?: string        // MP3 audio URL
  tags?: string[]          // Dialect/accent tags
  rhymes?: string          // Rhyme pattern
  homophones?: string[]    // Words that sound the same
}
```

### New Entity Types

#### Form (Inflections)

Inflected forms: plurals, conjugations, declensions.

```typescript
interface FormEntity {
  $id: string              // forms/{lang}/{word}/{form}/{index}
  $type: 'Form'
  wordId: string           // Base word entity ID
  word: string             // Base word
  pos: string              // Part of speech
  form: string             // The inflected form text
  tags?: string[]          // Grammatical tags (past, plural, etc.)
  ipa?: string             // Pronunciation of form
  romanization?: string    // For non-Latin scripts
}
```

#### Descendant

Words derived from this word in other languages.

```typescript
interface DescendantEntity {
  $id: string              // descendants/{lang}/{word}/{tgt_lang}/{desc}/{index}
  $type: 'Descendant'
  sourceWord: string       // Ancestor word
  sourceLanguageCode: string
  descendantWord: string   // Descendant in another language
  targetLanguage: string
  targetLanguageCode?: string
  depth: number            // Depth in tree (1 = direct)
  tags?: string[]          // borrowed, inherited, etc.
}
```

#### Compound

Compound words and their components.

```typescript
interface CompoundEntity {
  $id: string              // compounds/{lang}/{compound}/{component}/{index}
  $type: 'Compound'
  compound: string         // The compound word
  component: string        // Component word
  position: number         // Position in compound (1, 2, 3...)
  componentType?: string   // head, modifier, prefix, suffix
}
```

#### Collocation

Common word pairings.

```typescript
interface CollocationEntity {
  $id: string              // collocations/{lang}/{word}/{collocate}/{index}
  $type: 'Collocation'
  word: string             // Base word
  collocate: string        // Common co-occurring word
  phrase?: string          // Full collocation phrase
  frequency?: number       // Frequency score
  example?: string         // Usage example
}
```

#### Cognate

Words sharing the same root across languages.

```typescript
interface CognateEntity {
  $id: string              // cognates/{root_lang}/{root}/{lang}/{word}
  $type: 'Cognate'
  word: string             // The cognate word
  languageCode: string     // Language of this cognate
  language: string         // Full language name
  root: string             // Shared etymological root
  rootLanguage: string     // Root language (e.g., Proto-Indo-European)
  rootLanguageCode: string
}
```

#### EtymologyLink

Links between words in etymology chains.

```typescript
interface EtymologyLinkEntity {
  $id: string              // etymology_links/{lang}/{word}/{anc_lang}/{ancestor}
  $type: 'EtymologyLink'
  word: string             // The word
  ancestorWord: string     // Ancestor word
  ancestorLanguage: string
  ancestorLanguageCode: string
  linkType: EtymologyLinkType  // inherited, borrowed, learned, etc.
  distance: number         // Distance in chain (1 = direct parent)
}

type EtymologyLinkType =
  | 'inherited'  // Direct descent in same language family
  | 'borrowed'   // Borrowed from another language
  | 'learned'    // Learned borrowing (Latin/Greek)
  | 'calque'     // Loan translation
  | 'derived'    // Morphologically derived
  | 'compound'   // Part of compound word
  | 'cognate'    // Shares common ancestor
  | 'uncertain'  // Uncertain etymology
```

#### WordFrequency

Word usage frequency data.

```typescript
interface WordFrequencyEntity {
  $id: string              // frequencies/{lang}/{word}
  $type: 'WordFrequency'
  word: string
  languageCode: string
  rank: number             // Frequency rank (1 = most common)
  count?: number           // Raw frequency count
  perMillion?: number      // Frequency per million words
  source?: string          // Corpus source
}
```

### Existing Entities

#### Translation

Cross-language translations.

```typescript
interface TranslationEntity {
  $id: string              // translations/{src}/{tgt}/{word}/{index}
  $type: 'Translation'
  sourceWord: string
  sourceLanguageCode: string
  targetWord: string
  targetLanguage: string
  targetLanguageCode: string
  romanization?: string
  sense?: string
}
```

#### RelatedWord

Semantic relationships.

```typescript
interface RelatedWordEntity {
  $id: string              // related/{lang}/{type}/{word}/{related}
  $type: 'RelatedWord'
  wordId: string
  word: string
  relationType: RelationType
  relatedWord: string
  sense?: string
}

type RelationType =
  | 'synonym' | 'antonym' | 'hypernym' | 'hyponym'
  | 'holonym' | 'meronym' | 'derived' | 'related'
  | 'troponym' | 'coordinate'
```

#### Etymology

Word origin text.

```typescript
interface EtymologyEntity {
  $id: string              // etymologies/{lang}/{word}/{num}
  $type: 'Etymology'
  wordId: string
  word: string
  etymologyNumber: number
  text: string
}
```

## Storage Layout

Data is partitioned for efficient querying:

```
wiktionary/
  _manifest.json                    # Load metadata
  words/
    lang=en/
      letter=a/data.parquet
      letter=b/data.parquet
      ...
  definitions/
    lang=en/
      letter=a/data.parquet
      ...
  pronunciations/
    lang=en/data.parquet
  translations/
    src=en/
      tgt=es/data.parquet
      tgt=de/data.parquet
  related/
    lang=en/
      type=synonym/data.parquet
      type=antonym/data.parquet
  etymologies/
    lang=en/data.parquet
  forms/                            # NEW
    lang=en/
      letter=a/data.parquet
  descendants/                      # NEW
    lang=la/data.parquet
  compounds/                        # NEW
    lang=en/data.parquet
  collocations/                     # NEW
    lang=en/data.parquet
  cognates/                         # NEW
    root_lang=proto/data.parquet
  etymology_links/                  # NEW
    lang=en/data.parquet
  frequencies/                      # NEW
    lang=en/data.parquet
```

## Query API

### Word and Definition Queries

```typescript
import {
  lookupWord,
  getDefinitions,
  getDefinitionsForPos,
  searchWordsPrefix,
} from './queries'

// Look up a word
const words = await lookupWord(storage, 'en', 'algorithm')

// Get all definitions
const defs = await getDefinitions(storage, 'en', 'run')

// Get definitions for specific POS
const verbDefs = await getDefinitionsForPos(storage, 'en', 'run', 'verb')

// Search by prefix
const prefixMatches = await searchWordsPrefix(storage, 'en', 'algo', 20)
```

### Form (Inflection) Queries

```typescript
import {
  getWordForms,
  getFormsByTags,
  findBaseWord,
} from './queries'

// Get all forms of a word
const forms = await getWordForms(storage, 'en', 'run')
// -> runs, ran, running

// Get specific forms by grammatical tags
const pastForms = await getFormsByTags(storage, 'en', 'run', ['past'])
// -> ran

// Find base word from inflected form
const base = await findBaseWord(storage, 'en', 'running')
// -> run
```

### Etymology and Cognate Queries

```typescript
import {
  traceEtymology,
  findCognates,
  findBorrowedWords,
  getDescendants,
} from './queries'

// Trace full etymology chain
const chain = await traceEtymology(storage, 'en', 'algorithm')
// -> algorithm <- Arabic al-khwarizmi <- Persian Khwarezm

// Find cognates across languages
const cognates = await findCognates(storage, 'en', 'mother')
// -> German "Mutter", Spanish "madre", Russian "mat'"

// Find borrowed words from a language
const frenchLoans = await findBorrowedWords(storage, 'en', 'French', 100)
// -> restaurant, ballet, coup, ...

// Get descendants in other languages
const descendants = await getDescendants(storage, 'la', 'aqua')
// -> Spanish "agua", French "eau", Italian "acqua"
```

### Translation Queries

```typescript
import {
  getTranslations,
  compareTranslations,
  buildTranslationGraph,
  reverseTranslationLookup,
} from './queries'

// Get translations to specific language
const spanish = await getTranslations(storage, 'en', 'es', 'hello')

// Compare translations across languages
const comparison = await compareTranslations(storage, 'water', 'en', 'es', 'fr')
// -> { lang1: [agua], lang2: [eau], common: ['water (noun)'] }

// Build translation graph
const graph = await buildTranslationGraph(storage, 'water', 'en', ['es', 'fr', 'de', 'it'])

// Reverse lookup
const sources = await reverseTranslationLookup(storage, 'en', 'es', 'hola')
// -> English words that translate to "hola"
```

### Rhyme and Collocation Queries

```typescript
import {
  findRhymes,
  getCollocations,
  findCollocates,
} from './queries'

// Find rhyming words
const rhymes = await findRhymes(storage, 'en', 'day', 20)
// -> say, way, play, stay, ...

// Get collocations for a word
const collocations = await getCollocations(storage, 'en', 'make')
// -> decision, money, sense, mistake, ...

// Find what words collocate with a given word
const collocates = await findCollocates(storage, 'en', 'decision')
// -> make, reach, take, ...
```

### Frequency Queries

```typescript
import {
  getFrequencyRank,
  getTopFrequentWords,
  getWordsByFrequencyRange,
} from './queries'

// Get frequency rank of a word
const freq = await getFrequencyRank(storage, 'en', 'the')
// -> { rank: 1, perMillion: 70000 }

// Get most frequent words
const top100 = await getTopFrequentWords(storage, 'en', 100)

// Get words in frequency range
const mediumFreq = await getWordsByFrequencyRange(storage, 'en', 1000, 2000)
```

### Compound Word Queries

```typescript
import {
  getCompoundComponents,
  findCompoundsWithComponent,
} from './queries'

// Get components of a compound
const parts = await getCompoundComponents(storage, 'en', 'sunflower')
// -> sun, flower

// Find compounds containing a component
const compounds = await findCompoundsWithComponent(storage, 'en', 'sun')
// -> sunflower, sunshine, sunset, sunrise, ...
```

### Aggregate Queries

```typescript
import { getCompleteWordInfo, countByLanguage } from './queries'

// Get complete word information
const info = await getCompleteWordInfo(storage, 'en', 'run')
console.log(info.words)          // Word entities
console.log(info.definitions)    // All senses
console.log(info.forms)          // Inflected forms
console.log(info.pronunciations) // IPA, audio
console.log(info.etymology)      // Word origin
console.log(info.collocations)   // Common pairings
console.log(info.frequency)      // Usage frequency

// Count entities by language
const counts = await countByLanguage(storage, 'en')
```

## Usage

### Loading Data

```typescript
import { R2Backend } from 'parquedb/storage/R2Backend'
import { loadWiktionary } from './load'

const storage = new R2Backend(env.WIKTIONARY_BUCKET)

await loadWiktionary(
  storage,
  'https://kaikki.org/dictionary/English/kaikki.org-dictionary-English.jsonl',
  {
    batchSize: 10000,
    rowGroupSize: 50000,
    verbose: true,
  }
)
```

### Configuration Options

```typescript
interface WiktionaryLoaderConfig {
  languages?: string[]     // Filter by language codes
  maxEntries?: number      // Limit entries (for testing)
  batchSize?: number       // Batch size for writes (default: 10000)
  rowGroupSize?: number    // Parquet row group size (default: 50000)
  verbose?: boolean        // Enable progress logging
  skipRedirects?: boolean  // Skip redirect entries (default: true)
}
```

### Streaming for Custom Processing

```typescript
import { streamWiktionary } from './load'

for await (const { entry, entities } of streamWiktionary(url)) {
  // entry: Raw Wiktionary entry
  // entities.word: WordEntity
  // entities.definitions: DefinitionEntity[]
  // entities.forms: FormEntity[]
  // entities.pronunciations: PronunciationEntity[]
  // entities.translations: TranslationEntity[]
  // entities.descendants: DescendantEntity[]
  // entities.compounds: CompoundEntity[]
  // entities.collocations: CollocationEntity[]
  // entities.cognates: CognateEntity[]
  // entities.etymologyLinks: EtymologyLinkEntity[]
  // etc.

  await customHandler(entry, entities)
}
```

## Memory Efficiency

The loader is designed for large files:

1. **Streaming JSONL parsing** - Processes line by line, never loads entire file
2. **Chunked buffering** - Accumulates entities until batch size, then flushes
3. **Partitioned writes** - Each partition written independently
4. **Multipart upload** - Large Parquet files use multipart for reliability

Memory usage stays constant regardless of input file size.

## Performance Tips

1. **Use language filter** for faster loading:
   ```typescript
   await loadWiktionary(storage, url, { languages: ['en'] })
   ```

2. **Increase batch size** for fewer write operations:
   ```typescript
   await loadWiktionary(storage, url, { batchSize: 50000 })
   ```

3. **Query specific partitions** instead of scanning:
   ```typescript
   // Fast: queries single partition
   const words = await searchWordsPrefix(storage, 'en', 'algo')
   ```

## Example Output

Loading English Wiktionary produces approximately:

| Entity Type | Count |
|-------------|-------|
| Words | ~600K |
| Definitions | ~1.2M |
| Pronunciations | ~400K |
| Translations | ~2M |
| Related Words | ~500K |
| Etymologies | ~200K |
| Forms | ~800K |
| Descendants | ~50K |
| Compounds | ~30K |
| Collocations | ~100K |
| Etymology Links | ~150K |
| Cognates | ~20K |

Total storage: ~700MB compressed Parquet files.

## Cloudflare Worker Example

```typescript
import { R2Backend } from 'parquedb/storage/R2Backend'
import {
  lookupWord,
  getDefinitions,
  getWordForms,
  findRhymes,
  traceEtymology,
} from './queries'

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const storage = new R2Backend(env.WIKTIONARY_BUCKET)
    const url = new URL(request.url)
    const word = url.searchParams.get('word')
    const lang = url.searchParams.get('lang') || 'en'
    const action = url.searchParams.get('action') || 'lookup'

    if (!word) {
      return new Response('Missing word parameter', { status: 400 })
    }

    switch (action) {
      case 'lookup': {
        const [words, definitions] = await Promise.all([
          lookupWord(storage, lang, word),
          getDefinitions(storage, lang, word),
        ])
        return Response.json({ words, definitions })
      }

      case 'forms': {
        const forms = await getWordForms(storage, lang, word)
        return Response.json({ forms })
      }

      case 'rhymes': {
        const rhymes = await findRhymes(storage, lang, word, 50)
        return Response.json({ rhymes: rhymes.map(r => r.word) })
      }

      case 'etymology': {
        const etym = await traceEtymology(storage, lang, word)
        return Response.json(etym)
      }

      default:
        return new Response('Unknown action', { status: 400 })
    }
  }
}
```

## License

Wiktionary data is available under [CC BY-SA 3.0](https://creativecommons.org/licenses/by-sa/3.0/). When using this data, please cite:

> Tatu Ylonen: Wiktextract: Wiktionary as Machine-Readable Structured Data, Proceedings of the 13th Language Resources and Evaluation Conference (LREC), pp. 1317-1325, Marseille, 20-25 June 2022.

## See Also

- [kaikki.org](https://kaikki.org/dictionary/) - Data source
- [wiktextract](https://github.com/tatuylonen/wiktextract) - Extraction tool
- [ParqueDB](../../README.md) - Database documentation
