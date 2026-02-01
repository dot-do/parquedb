# Wiktionary Query Patterns for ParqueDB

This document catalogs real-world query patterns for the Wiktionary dataset stored in ParqueDB's dual Variant architecture (`$id | $index_* | $data` columns). These patterns are organized by use case and include recommendations for index columns and optimization strategies.

## Table of Contents

1. [Overview](#overview)
2. [Query Pattern Summary Table](#query-pattern-summary-table)
3. [Language Learning Applications](#language-learning-applications)
4. [Dictionary and Thesaurus Applications](#dictionary-and-thesaurus-applications)
5. [Translation Services](#translation-services)
6. [Word Games](#word-games)
7. [NLP and AI Training Pipelines](#nlp-and-ai-training-pipelines)
8. [Spell Checkers and Autocomplete](#spell-checkers-and-autocomplete)
9. [Etymology Exploration Tools](#etymology-exploration-tools)
10. [Index Recommendations](#index-recommendations)

---

## Overview

### Data Model Summary

The Wiktionary dataset in ParqueDB consists of 13 entity types:

| Entity Type | Primary Key Pattern | Volume (English) |
|-------------|---------------------|------------------|
| Word | `words/{lang}/{word}/{pos}` | ~600K |
| Definition | `definitions/{lang}/{word}/{pos}/{idx}` | ~1.2M |
| Pronunciation | `pronunciations/{lang}/{word}/{idx}` | ~400K |
| Translation | `translations/{src}/{tgt}/{word}/{idx}` | ~2M |
| RelatedWord | `related/{lang}/{type}/{word}/{related}` | ~500K |
| Etymology | `etymologies/{lang}/{word}/{num}` | ~200K |
| Form | `forms/{lang}/{word}/{form}/{idx}` | ~800K |
| Descendant | `descendants/{lang}/{word}/{tgt}/{desc}/{idx}` | ~50K |
| Compound | `compounds/{lang}/{compound}/{component}/{idx}` | ~30K |
| Collocation | `collocations/{lang}/{word}/{collocate}/{idx}` | ~100K |
| Cognate | `cognates/{root_lang}/{root}/{lang}/{word}` | ~20K |
| EtymologyLink | `etymology_links/{lang}/{word}/{anc_lang}/{anc}` | ~150K |
| WordFrequency | `frequencies/{lang}/{word}` | ~100K |

### Selectivity Definitions

- **High**: Query returns < 0.1% of data (< 1K rows from 1M)
- **Medium**: Query returns 0.1% - 5% of data
- **Low**: Query returns > 5% of data (may require full scan)

---

## Query Pattern Summary Table

| # | Use Case | Query Pattern | Index Columns | Selectivity | Stats Pushdown |
|---|----------|---------------|---------------|-------------|----------------|
| 1 | Language Learning | Vocabulary by frequency band | `$index_rank`, `$index_languageCode` | High | Yes |
| 2 | Language Learning | Words with audio | `$index_languageCode`, `$index_hasAudio` | Medium | Yes |
| 3 | Language Learning | Spaced repetition export | `$index_languageCode`, `$index_rank`, `$index_pos` | Medium | Yes |
| 4 | Dictionary | Exact word lookup | `$index_word`, `$index_languageCode` | High | Yes |
| 5 | Dictionary | Prefix search (autocomplete) | `$index_word` | High | Yes |
| 6 | Dictionary | POS-filtered lookup | `$index_word`, `$index_pos` | High | Yes |
| 7 | Thesaurus | Synonym lookup | `$index_word`, `$index_relationType` | High | Yes |
| 8 | Thesaurus | Semantic hierarchy (hypernym/hyponym) | `$index_word`, `$index_relationType` | High | Yes |
| 9 | Translation | Direct translation lookup | `$index_sourceWord`, `$index_targetLanguageCode` | High | Yes |
| 10 | Translation | Reverse translation | `$index_targetWord`, `$index_sourceLanguageCode` | High | Yes |
| 11 | Translation | Multi-language translation graph | `$index_sourceWord` | Medium | Yes |
| 12 | Word Games | Rhyme finder | `$index_rhymes` | High | Yes |
| 13 | Word Games | Scrabble valid words (length + letters) | `$index_languageCode`, `$index_word` | Low | Partial |
| 14 | Word Games | Crossword pattern match | `$index_word` (regex fallback) | Low | No |
| 15 | Word Games | Anagram solver | `$index_sortedLetters` (derived) | High | Yes |
| 16 | NLP Pipeline | Lemmatization (form to base) | `$index_form`, `$index_languageCode` | High | Yes |
| 17 | NLP Pipeline | POS tag distribution | `$index_languageCode`, `$index_pos` | Low | Yes |
| 18 | NLP Pipeline | Domain vocabulary extraction | `$index_topics`, `$index_tags` | Medium | Partial |
| 19 | Spell Check | Near-match suggestions | `$index_word` (edit distance) | High | Partial |
| 20 | Autocomplete | Prefix + frequency ranking | `$index_word`, `$index_rank` | High | Yes |
| 21 | Etymology | Trace word origin | `$index_word`, `$index_linkType` | High | Yes |
| 22 | Etymology | Borrowed words by source | `$index_ancestorLanguage`, `$index_linkType` | Medium | Yes |
| 23 | Etymology | Cognate discovery | `$index_root`, `$index_rootLanguage` | High | Yes |
| 24 | Linguistics | Descendant tree | `$index_sourceWord`, `$index_sourceLanguageCode` | High | Yes |

---

## Language Learning Applications

### Pattern 1: Vocabulary by Frequency Band

**Business Question**: Get the 1000 most common words for a beginner Spanish course.

**Use Case**: Duolingo, Babbel, Memrise vocabulary lists

```typescript
// Filter
{
  languageCode: { $eq: 'es' },
  rank: { $lte: 1000 }
}

// Sort
{ rank: 1 }

// Projection
{ word: 1, rank: 1, definitions: 1, pos: 1 }
```

**Index Columns**: `$index_languageCode`, `$index_rank`

**Selectivity**: High - Returns exactly 1000 rows from ~100K frequency entries

**Row-Group Statistics**: Excellent. Min/max on `rank` allows skipping row groups where `min_rank > 1000`. Partition on `languageCode` limits scan to single partition.

---

### Pattern 2: Words with Audio Pronunciations

**Business Question**: Find all German words that have audio recordings for listening practice.

**Use Case**: Pronunciation training modules, audio flashcards

```typescript
// Filter on Word entity
{
  languageCode: { $eq: 'de' },
  hasAudio: { $eq: true }
}

// Join to Pronunciation entity for audio URLs
// Filter
{
  languageCode: { $eq: 'de' },
  $or: [
    { audioOgg: { $exists: true } },
    { audioMp3: { $exists: true } }
  ]
}
```

**Index Columns**: `$index_languageCode`, `$index_hasAudio`

**Selectivity**: Medium - ~30-40% of entries have audio in major languages

**Row-Group Statistics**: Good. Boolean `hasAudio` column enables skipping row groups where `max_hasAudio = false`.

---

### Pattern 3: Spaced Repetition Export

**Business Question**: Export intermediate French verbs (frequency rank 500-2000) for Anki deck generation.

**Use Case**: SRS deck builders, vocabulary export tools

```typescript
// Filter
{
  languageCode: { $eq: 'fr' },
  pos: { $eq: 'verb' },
  rank: { $gte: 500, $lte: 2000 }
}

// Projection (join Word + Definition + Pronunciation)
{
  word: 1,
  definitions: { $slice: 3 },  // Top 3 definitions
  ipa: 1,
  examples: { $slice: 2 },     // 2 example sentences
  rank: 1
}
```

**Index Columns**: `$index_languageCode`, `$index_pos`, `$index_rank`

**Selectivity**: Medium - ~1500 verbs in the rank range

**Row-Group Statistics**: Excellent. Compound filter on indexed columns enables multi-level pruning.

---

## Dictionary and Thesaurus Applications

### Pattern 4: Exact Word Lookup

**Business Question**: Get all information about the English word "algorithm".

**Use Case**: Core dictionary lookup API endpoint

```typescript
// Filter
{
  word: { $eq: 'algorithm' },
  languageCode: { $eq: 'en' }
}
```

**Index Columns**: `$index_word`, `$index_languageCode`

**Selectivity**: High - Returns 1-5 rows (one per POS)

**Row-Group Statistics**: Excellent. String min/max on `word` column can skip row groups where `'algorithm' < min_word OR 'algorithm' > max_word`. Partition by `letter=a` further restricts scan.

---

### Pattern 5: Prefix Search (Autocomplete)

**Business Question**: Find all English words starting with "electr" for autocomplete.

**Use Case**: Search-as-you-type, dictionary search bars

```typescript
// Filter (using $regex or range query)
{
  languageCode: { $eq: 'en' },
  word: { $gte: 'electr', $lt: 'electr\uffff' }
  // Or: word: { $regex: '^electr' }
}

// Limit
{ $limit: 20 }

// Sort by frequency for relevance
{ rank: 1 }
```

**Index Columns**: `$index_word`

**Selectivity**: High - Typically 10-50 matches for 5+ character prefixes

**Row-Group Statistics**: Good. Range query on sorted `word` column enables efficient skip. Letter-based partitioning (`letter=e`) restricts to single partition.

---

### Pattern 6: Part-of-Speech Filtered Lookup

**Business Question**: Get only the verb definitions of "run".

**Use Case**: Grammar-aware dictionary apps, language learner tools

```typescript
// Filter
{
  word: { $eq: 'run' },
  languageCode: { $eq: 'en' },
  pos: { $eq: 'verb' }
}
```

**Index Columns**: `$index_word`, `$index_languageCode`, `$index_pos`

**Selectivity**: High - Returns single Word entity

**Row-Group Statistics**: Excellent. All three filter columns are indexable.

---

### Pattern 7: Synonym Lookup

**Business Question**: Find all synonyms for "happy".

**Use Case**: Thesaurus feature, writing assistance tools

```typescript
// Query RelatedWord entity
{
  word: { $eq: 'happy' },
  languageCode: { $eq: 'en' },
  relationType: { $eq: 'synonym' }
}
```

**Index Columns**: `$index_word`, `$index_languageCode`, `$index_relationType`

**Selectivity**: High - 10-50 synonyms per word

**Row-Group Statistics**: Excellent. Partition by `type=synonym` combined with word index provides highly selective access.

---

### Pattern 8: Semantic Hierarchy Navigation

**Business Question**: Find all hypernyms (broader terms) and hyponyms (narrower terms) for "dog".

**Use Case**: Semantic explorer, ontology navigation, taxonomy browsers

```typescript
// Hypernyms query
{
  word: { $eq: 'dog' },
  languageCode: { $eq: 'en' },
  relationType: { $eq: 'hypernym' }
}
// Returns: animal, mammal, canine, pet

// Hyponyms query
{
  word: { $eq: 'dog' },
  languageCode: { $eq: 'en' },
  relationType: { $eq: 'hyponym' }
}
// Returns: poodle, labrador, beagle, terrier
```

**Index Columns**: `$index_word`, `$index_relationType`

**Selectivity**: High - 5-30 related terms per relationship type

**Row-Group Statistics**: Excellent. Direct partition access via `type=hypernym` or `type=hyponym`.

---

## Translation Services

### Pattern 9: Direct Translation Lookup

**Business Question**: Translate "water" from English to Spanish.

**Use Case**: Core translation API, language learning translation feature

```typescript
// Filter on Translation entity
{
  sourceWord: { $eq: 'water' },
  sourceLanguageCode: { $eq: 'en' },
  targetLanguageCode: { $eq: 'es' }
}
```

**Index Columns**: `$index_sourceWord`, `$index_sourceLanguageCode`, `$index_targetLanguageCode`

**Selectivity**: High - 1-10 translations per word pair

**Row-Group Statistics**: Excellent. Partition structure `src=en/tgt=es/` provides O(1) file access. Word index enables row-group pruning.

---

### Pattern 10: Reverse Translation Lookup

**Business Question**: Find English words that translate to Spanish "agua".

**Use Case**: Bilingual dictionary, reverse lookup feature

```typescript
// Filter
{
  targetWord: { $eq: 'agua' },
  sourceLanguageCode: { $eq: 'en' },
  targetLanguageCode: { $eq: 'es' }
}
```

**Index Columns**: `$index_targetWord`, `$index_sourceLanguageCode`, `$index_targetLanguageCode`

**Selectivity**: High - 1-5 source words typically

**Row-Group Statistics**: Good. Requires index on `targetWord` since data is sorted by `sourceWord`. Bloom filter on `targetWord` would help.

---

### Pattern 11: Multi-Language Translation Graph

**Business Question**: Get translations of "love" to all available languages for a translation comparison feature.

**Use Case**: Polyglot translation apps, linguistic research tools

```typescript
// Requires scanning multiple partition files
// For each target language partition:
{
  sourceWord: { $eq: 'love' },
  sourceLanguageCode: { $eq: 'en' }
}

// Aggregate results across partitions
```

**Index Columns**: `$index_sourceWord`

**Selectivity**: Medium - Scans many partitions but highly selective within each

**Row-Group Statistics**: Good within partition. Consider denormalized "all translations" view for frequent queries.

---

## Word Games

### Pattern 12: Rhyme Finder

**Business Question**: Find all English words that rhyme with "day".

**Use Case**: Poetry assistants, songwriting tools, Rhyme Zone-style apps

```typescript
// Step 1: Get rhyme pattern for "day"
{
  word: { $eq: 'day' },
  languageCode: { $eq: 'en' },
  rhymes: { $exists: true }
}
// Returns rhymes: "-eɪ"

// Step 2: Find all words with same rhyme pattern
{
  languageCode: { $eq: 'en' },
  rhymes: { $eq: '-eɪ' }
}
```

**Index Columns**: `$index_rhymes`, `$index_languageCode`

**Selectivity**: High - Typically 50-200 words per rhyme pattern

**Row-Group Statistics**: Good. Rhyme pattern is a categorical value with limited cardinality. Bloom filter highly effective.

---

### Pattern 13: Scrabble Valid Words

**Business Question**: Find all valid 7-letter English words containing the letters A, E, T (for Scrabble assistance).

**Use Case**: Scrabble helpers, Words With Friends tools

```typescript
// Filter
{
  languageCode: { $eq: 'en' },
  // Word length check (derived or computed)
  word: { $regex: '^.{7}$' },
  // Contains specific letters
  word: { $regex: '(?=.*a)(?=.*e)(?=.*t)' }
}

// Better approach: pre-computed derived field
{
  languageCode: { $eq: 'en' },
  wordLength: { $eq: 7 },
  containsLetters: { $all: ['a', 'e', 't'] }
}
```

**Index Columns**: `$index_wordLength` (derived), `$index_languageCode`

**Selectivity**: Low - Length filter alone has ~5% selectivity; letter filtering reduces further

**Row-Group Statistics**: Limited. Regex queries cannot use statistics. Pre-computed length field enables pushdown.

---

### Pattern 14: Crossword Pattern Match

**Business Question**: Find 5-letter words matching pattern "S_A_E" (for crossword solving).

**Use Case**: Crossword solvers, word puzzle helpers

```typescript
// Filter using regex
{
  languageCode: { $eq: 'en' },
  word: { $regex: '^s.a.e$', $options: 'i' }
}
// Returns: shade, shake, shame, shape, share, shave, skate, slake, slate, snake, space, spade, spare, stage, stake, stale, stare, state, suave
```

**Index Columns**: None effective for regex patterns

**Selectivity**: Low - Requires scan of letter partition

**Row-Group Statistics**: No pushdown possible. First letter partition (`letter=s`) reduces scan. Consider trigram or n-gram indexes for pattern matching.

---

### Pattern 15: Anagram Solver

**Business Question**: Find all words that are anagrams of "listen".

**Use Case**: Anagram games, word puzzle solvers

```typescript
// Requires derived field: sorted letters
// "listen" -> "eilnst"

{
  languageCode: { $eq: 'en' },
  sortedLetters: { $eq: 'eilnst' }
}
// Returns: listen, silent, tinsel, enlist, inlets
```

**Index Columns**: `$index_sortedLetters` (derived column)

**Selectivity**: High - Typically 1-10 anagrams per letter combination

**Row-Group Statistics**: Excellent if `sortedLetters` is materialized as indexed column. Bloom filter highly effective.

---

## NLP and AI Training Pipelines

### Pattern 16: Lemmatization (Form to Base Word)

**Business Question**: Find the base form (lemma) of "running".

**Use Case**: Text preprocessing pipelines, tokenization

```typescript
// Query Form entity
{
  form: { $eq: 'running' },
  languageCode: { $eq: 'en' }
}
// Returns: { word: 'run', pos: 'verb', tags: ['present participle'] }
```

**Index Columns**: `$index_form`, `$index_languageCode`

**Selectivity**: High - 1-3 base words per inflected form

**Row-Group Statistics**: Good. Form partitioned by letter. Index on `form` enables row-group skip.

---

### Pattern 17: Part-of-Speech Tag Distribution

**Business Question**: Count words by POS tag for training a POS tagger.

**Use Case**: NLP model training data generation

```typescript
// Aggregation query
{
  languageCode: { $eq: 'en' }
}

// Group by pos, count
// SELECT pos, COUNT(*) FROM words WHERE languageCode = 'en' GROUP BY pos
```

**Index Columns**: `$index_languageCode`, `$index_pos`

**Selectivity**: Low - Full scan of language partition

**Row-Group Statistics**: Limited benefit for aggregation. Pre-computed statistics in manifest recommended.

---

### Pattern 18: Domain Vocabulary Extraction

**Business Question**: Extract all medical terminology for domain-specific NLP model.

**Use Case**: Specialized vocabulary lists, domain adaptation

```typescript
// Query Definition entity
{
  languageCode: { $eq: 'en' },
  $or: [
    { topics: { $contains: 'medicine' } },
    { topics: { $contains: 'anatomy' } },
    { topics: { $contains: 'pathology' } },
    { tags: { $contains: 'medical' } }
  ]
}
```

**Index Columns**: `$index_topics` (array index), `$index_tags` (array index)

**Selectivity**: Medium - ~2-5% of definitions are medical

**Row-Group Statistics**: Partial. Array contains queries benefit from bloom filters but not min/max statistics. Consider inverted index on topics.

---

## Spell Checkers and Autocomplete

### Pattern 19: Near-Match Suggestions (Spell Check)

**Business Question**: Find correct spellings for misspelled "accomodate".

**Use Case**: Spell checker suggestions, fuzzy search

```typescript
// Approach 1: Edit distance (Levenshtein)
// Requires UDF or application-side filtering
{
  languageCode: { $eq: 'en' },
  word: { $editDistance: { value: 'accomodate', maxDistance: 2 } }
}
// Returns: accommodate

// Approach 2: Phonetic matching (Soundex/Metaphone)
{
  languageCode: { $eq: 'en' },
  soundex: { $eq: soundex('accomodate') }
}
```

**Index Columns**: `$index_soundex` (derived), `$index_metaphone` (derived)

**Selectivity**: High with phonetic index - 10-50 candidates

**Row-Group Statistics**: Limited for edit distance. Phonetic codes enable statistics pushdown.

---

### Pattern 20: Autocomplete with Frequency Ranking

**Business Question**: Suggest completions for "comp" sorted by word frequency.

**Use Case**: Search bar autocomplete, predictive text

```typescript
// Filter
{
  languageCode: { $eq: 'en' },
  word: { $gte: 'comp', $lt: 'comp\uffff' }
}

// Join with frequency data and sort
// Sort by rank ascending (most frequent first)
{ rank: 1 }

// Limit
{ $limit: 10 }
```

**Index Columns**: `$index_word`, `$index_rank`

**Selectivity**: High - Prefix filter highly selective

**Row-Group Statistics**: Excellent. Range query on sorted word column. Top-K with frequency sort benefits from pre-sorted data.

---

## Etymology Exploration Tools

### Pattern 21: Trace Word Origin

**Business Question**: Trace the etymology of "salary" back to its origin.

**Use Case**: Etymology explorer apps, linguistic education tools

```typescript
// Get etymology text
{
  word: { $eq: 'salary' },
  languageCode: { $eq: 'en' }
}

// Get etymology links (chain of ancestors)
{
  word: { $eq: 'salary' },
  languageCode: { $eq: 'en' }
}
// Returns chain:
// salary <- Latin "salarium" (salt money) <- Latin "sal" (salt)
```

**Index Columns**: `$index_word`, `$index_languageCode`

**Selectivity**: High - 1-5 etymology entries per word

**Row-Group Statistics**: Excellent. Direct lookup pattern.

---

### Pattern 22: Borrowed Words by Source Language

**Business Question**: Find all English words borrowed from French.

**Use Case**: Linguistic research, language history education

```typescript
// Query EtymologyLink entity
{
  languageCode: { $eq: 'en' },
  linkType: { $in: ['borrowed', 'learned'] },
  ancestorLanguage: { $regex: '^French' }
}
```

**Index Columns**: `$index_linkType`, `$index_ancestorLanguage`

**Selectivity**: Medium - ~5-10% of English words are French loans

**Row-Group Statistics**: Good. Filter on categorical `linkType` enables row-group skip. Consider partition by `ancestorLanguage`.

---

### Pattern 23: Cognate Discovery

**Business Question**: Find all cognates of English "mother" across Indo-European languages.

**Use Case**: Comparative linguistics tools, language family explorers

```typescript
// Step 1: Find the root for "mother"
{
  word: { $eq: 'mother' },
  languageCode: { $eq: 'en' }
}
// Returns: root = "*mehter-", rootLanguage = "Proto-Indo-European"

// Step 2: Find all cognates with same root
{
  root: { $eq: '*mehter-' },
  rootLanguage: { $eq: 'Proto-Indo-European' }
}
// Returns: German "Mutter", Spanish "madre", Russian "mat'", etc.
```

**Index Columns**: `$index_root`, `$index_rootLanguage`

**Selectivity**: High - 5-30 cognates per root

**Row-Group Statistics**: Excellent. Partition by `rootLanguage` combined with root index provides selective access.

---

### Pattern 24: Language Descendant Tree

**Business Question**: Find all Romance language words descended from Latin "aqua".

**Use Case**: Language family tree visualization, historical linguistics

```typescript
// Query Descendant entity
{
  sourceWord: { $eq: 'aqua' },
  sourceLanguageCode: { $eq: 'la' }
}
// Returns:
// Spanish "agua", French "eau", Italian "acqua",
// Portuguese "agua", Romanian "apa", etc.
```

**Index Columns**: `$index_sourceWord`, `$index_sourceLanguageCode`

**Selectivity**: High - 10-100 descendants per word

**Row-Group Statistics**: Excellent. Partition by source language provides direct access.

---

## Index Recommendations

### Recommended `$index_*` Columns by Entity Type

#### Word Entity
```
$index_word           - Primary lookup key
$index_languageCode   - Language partition filter
$index_pos            - Part of speech filter
$index_hasAudio       - Audio availability filter
$index_hasIpa         - IPA availability filter
$index_senseCount     - Complexity filter
$index_wordLength     - (derived) Game queries
$index_sortedLetters  - (derived) Anagram queries
```

#### Definition Entity
```
$index_word           - Join key to Word
$index_languageCode   - Language filter
$index_pos            - POS filter
$index_topics         - Domain extraction (bloom filter)
$index_tags           - Grammar/usage filter (bloom filter)
```

#### Pronunciation Entity
```
$index_word           - Join key
$index_languageCode   - Language filter
$index_rhymes         - Rhyme lookup
$index_ipa            - Phonetic search
```

#### Translation Entity
```
$index_sourceWord     - Forward lookup
$index_targetWord     - Reverse lookup
$index_sourceLanguageCode - Source filter
$index_targetLanguageCode - Target filter
```

#### Form Entity
```
$index_form           - Lemmatization lookup (critical)
$index_word           - Base word lookup
$index_languageCode   - Language filter
$index_tags           - Grammatical form filter
```

#### RelatedWord Entity
```
$index_word           - Source word lookup
$index_relatedWord    - Reverse lookup
$index_relationType   - Relationship filter
$index_languageCode   - Language filter
```

#### EtymologyLink Entity
```
$index_word           - Forward lookup
$index_ancestorWord   - Reverse lookup
$index_ancestorLanguage - Source language filter
$index_linkType       - Relationship type filter
```

#### Cognate Entity
```
$index_word           - Word lookup
$index_root           - Root lookup
$index_rootLanguage   - Root language filter
$index_languageCode   - Language filter
```

#### WordFrequency Entity
```
$index_word           - Word lookup
$index_rank           - Frequency band queries
$index_languageCode   - Language filter
```

### Bloom Filter Candidates

The following columns benefit from bloom filters due to high cardinality and equality-only queries:

1. `word` - All entities
2. `targetWord` - Translation entity
3. `relatedWord` - RelatedWord entity
4. `form` - Form entity
5. `ancestorWord` - EtymologyLink entity
6. `topics` - Definition entity (array membership)
7. `rhymes` - Pronunciation entity
8. `sortedLetters` - Word entity (derived)
9. `soundex` / `metaphone` - Word entity (derived)

### Partition Strategy Summary

| Entity | Partition Key(s) | Rationale |
|--------|------------------|-----------|
| Word | `lang`, `letter` | Language-first, then alphabetical for prefix queries |
| Definition | `lang`, `letter` | Matches Word partitioning for joins |
| Pronunciation | `lang` | Smaller volume, single partition per language |
| Translation | `src_lang`, `tgt_lang` | Language pair isolation |
| RelatedWord | `lang`, `relationType` | Relationship-type queries common |
| Form | `lang`, `letter` | Form lookup by first letter |
| EtymologyLink | `lang` | Single language view typical |
| Cognate | `rootLanguage` | Root-centric queries |
| WordFrequency | `lang` | Language-specific frequency lists |

---

## Conclusion

The Wiktionary dataset in ParqueDB supports diverse query patterns across language learning, dictionary, translation, gaming, NLP, and linguistic research applications. Key optimization strategies include:

1. **Leverage partitioning** - Language and letter-based partitions reduce scan scope
2. **Index high-cardinality lookup columns** - `word`, `form`, `targetWord` benefit most
3. **Use bloom filters** - Effective for existence checks on string columns
4. **Pre-compute derived fields** - `wordLength`, `sortedLetters`, `soundex` enable new query patterns
5. **Row-group statistics** - Min/max pushdown works well for range queries on `rank`, sorted `word` columns

For production deployments, consider materializing frequently-joined views (Word + Definition + Pronunciation) to reduce multi-entity lookups.
