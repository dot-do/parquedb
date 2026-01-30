/**
 * Wiktionary Streaming Loader for ParqueDB
 *
 * Streams and parses Wiktionary JSONL data from kaikki.org,
 * transforms entries into ParqueDB entities, and stores using
 * the ParqueDB API with proper entity and relationship management.
 *
 * Data flows to:
 * - data/ - Entity storage (words, definitions, pronunciations, etc.)
 * - rels/ - Relationship indexes (word-definition links, etc.)
 *
 * Designed for streaming large files (10GB+) with:
 * - Line-by-line JSONL parsing
 * - Chunked batch processing
 * - Memory-efficient buffering
 * - ParqueDB collection API for storage
 * - $link for word-definition relationships
 *
 * Usage:
 *   npx tsx examples/wiktionary/load.ts
 *
 * Environment:
 *   WIKTIONARY_URL - Source JSONL URL (default: English)
 *   BATCH_SIZE - Entries per batch (default: 100)
 *   MAX_ENTRIES - Max entries to process (default: unlimited)
 *   DATA_DIR - Directory for ParqueDB data (default: ./wiktionary-data)
 *
 * @see https://kaikki.org/dictionary/
 */

import { ParqueDB, FsBackend } from '../../src'
import type { CreateInput, EntityId, Entity } from '../../src/types'
import {
  type WiktionaryEntry,
  type WiktionaryLoaderConfig,
  type WiktionaryManifest,
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
  type EtymologyLinkType,
  type RelationType,
  DEFAULT_LOADER_CONFIG,
} from './schema'

// =============================================================================
// Line-by-Line JSONL Stream Parser
// =============================================================================

/**
 * Parse a JSONL stream line by line
 *
 * Handles partial lines at chunk boundaries by buffering incomplete lines.
 */
async function* parseJsonlStream(
  stream: ReadableStream<Uint8Array>
): AsyncGenerator<WiktionaryEntry> {
  const decoder = new TextDecoder('utf-8')
  const reader = stream.getReader()
  let buffer = ''
  let lineNumber = 0

  try {
    while (true) {
      const { done, value } = await reader.read()

      if (done) {
        // Process any remaining data in buffer
        if (buffer.trim()) {
          lineNumber++
          try {
            yield JSON.parse(buffer) as WiktionaryEntry
          } catch (e) {
            console.warn(`Failed to parse line ${lineNumber}: ${(e as Error).message}`)
          }
        }
        break
      }

      // Append new data to buffer
      buffer += decoder.decode(value, { stream: true })

      // Process complete lines
      const lines = buffer.split('\n')
      // Keep the last (potentially incomplete) line in buffer
      buffer = lines.pop() || ''

      for (const line of lines) {
        if (!line.trim()) continue
        lineNumber++

        try {
          yield JSON.parse(line) as WiktionaryEntry
        } catch (e) {
          console.warn(`Failed to parse line ${lineNumber}: ${(e as Error).message}`)
        }
      }
    }
  } finally {
    reader.releaseLock()
  }
}

// =============================================================================
// Entity Transformers
// =============================================================================

/**
 * Transform a Wiktionary entry to a Word entity input
 */
function toWordInput(entry: WiktionaryEntry): CreateInput<WordEntity> {
  return {
    $type: 'Word',
    name: `${entry.word} (${entry.lang}, ${entry.pos})`,
    word: entry.word,
    language: entry.lang,
    languageCode: entry.lang_code,
    pos: entry.pos,
    etymologyText: entry.etymology_text,
    etymologyNumber: entry.etymology_number,
    categories: entry.categories?.map(c => c.name),
    wikipedia: entry.wikipedia?.[0],
    wikidata: entry.wikidata?.[0],
    senseCount: entry.senses?.length ?? 0,
    translationCount: entry.translations?.length ?? 0,
    hasAudio: entry.sounds?.some(s => s.audio || s.ogg_url || s.mp3_url) ?? false,
    hasIpa: entry.sounds?.some(s => s.ipa) ?? false,
  }
}

/**
 * Transform a Wiktionary entry to Definition entity inputs
 */
function toDefinitionInputs(entry: WiktionaryEntry): CreateInput<DefinitionEntity>[] {
  if (!entry.senses?.length) return []

  return entry.senses.map((sense, index) => {
    const gloss = sense.glosses?.[0] || sense.raw_glosses?.[0] || ''

    return {
      $type: 'Definition',
      name: gloss.length > 60 ? gloss.slice(0, 57) + '...' : gloss,
      word: entry.word,
      languageCode: entry.lang_code,
      pos: entry.pos,
      senseIndex: index,
      gloss,
      glosses: sense.glosses,
      tags: sense.tags,
      topics: sense.topics,
      examples: sense.examples?.map(e => e.text).filter(Boolean) as string[],
      wikidata: sense.wikidata?.[0],
    }
  })
}

/**
 * Transform a Wiktionary entry to Pronunciation entity inputs
 */
function toPronunciationInputs(entry: WiktionaryEntry): CreateInput<PronunciationEntity>[] {
  if (!entry.sounds?.length) return []

  return entry.sounds.map((sound) => ({
    $type: 'Pronunciation',
    name: sound.ipa || sound.text || entry.word,
    word: entry.word,
    languageCode: entry.lang_code,
    ipa: sound.ipa,
    audioOgg: sound.ogg_url,
    audioMp3: sound.mp3_url,
    audioText: sound.text,
    tags: sound.tags,
    rhymes: sound.rhymes,
    homophones: sound.homophones,
  }))
}

/**
 * Transform a Wiktionary entry to Translation entity inputs
 */
function toTranslationInputs(entry: WiktionaryEntry): CreateInput<TranslationEntity>[] {
  if (!entry.translations?.length) return []

  return entry.translations
    .filter(t => t.word && t.code)
    .map((trans) => ({
      $type: 'Translation',
      name: `${entry.word} -> ${trans.word} (${trans.lang || trans.code})`,
      sourceWord: entry.word,
      sourceLanguageCode: entry.lang_code,
      targetWord: trans.word!,
      targetLanguage: trans.lang || trans.code!,
      targetLanguageCode: trans.code!,
      romanization: trans.roman,
      sense: trans.sense,
      tags: trans.tags,
    }))
}

/**
 * Transform a Wiktionary entry to RelatedWord entity inputs
 */
function toRelatedWordInputs(entry: WiktionaryEntry): CreateInput<RelatedWordEntity>[] {
  const inputs: CreateInput<RelatedWordEntity>[] = []

  const processLinkages = (
    linkages: typeof entry.synonyms | undefined,
    relationType: RelationType
  ) => {
    if (!linkages?.length) return

    for (const linkage of linkages) {
      if (!linkage.word) continue

      inputs.push({
        $type: 'RelatedWord',
        name: `${entry.word} ${relationType} ${linkage.word}`,
        word: entry.word,
        languageCode: entry.lang_code,
        relationType,
        relatedWord: linkage.word,
        sense: linkage.sense,
        tags: linkage.tags,
      })
    }
  }

  processLinkages(entry.synonyms, 'synonym')
  processLinkages(entry.antonyms, 'antonym')
  processLinkages(entry.hypernyms, 'hypernym')
  processLinkages(entry.hyponyms, 'hyponym')
  processLinkages(entry.holonyms, 'holonym')
  processLinkages(entry.meronyms, 'meronym')
  processLinkages(entry.troponyms, 'troponym')
  processLinkages(entry.coordinate_terms, 'coordinate')
  processLinkages(entry.derived, 'derived')
  processLinkages(entry.related, 'related')

  return inputs
}

/**
 * Transform a Wiktionary entry to Etymology entity input
 */
function toEtymologyInput(entry: WiktionaryEntry): CreateInput<EtymologyEntity> | null {
  if (!entry.etymology_text) return null

  const etymNum = entry.etymology_number ?? 1

  return {
    $type: 'Etymology',
    name: `Etymology of ${entry.word}`,
    word: entry.word,
    languageCode: entry.lang_code,
    etymologyNumber: etymNum,
    text: entry.etymology_text,
  }
}

/**
 * Transform a Wiktionary entry to Form entity inputs (inflections)
 */
function toFormInputs(entry: WiktionaryEntry): CreateInput<FormEntity>[] {
  if (!entry.forms?.length) return []

  return entry.forms.map((form) => ({
    $type: 'Form',
    name: `${form.form} (${form.tags?.join(', ') || 'form'} of ${entry.word})`,
    word: entry.word,
    languageCode: entry.lang_code,
    pos: entry.pos,
    form: form.form,
    tags: form.tags,
    ipa: form.ipa,
    romanization: form.roman,
    source: form.source,
  }))
}

/**
 * Transform a Wiktionary entry to Descendant entity inputs
 */
function toDescendantInputs(entry: WiktionaryEntry): CreateInput<DescendantEntity>[] {
  if (!entry.descendants?.length) return []

  const inputs: CreateInput<DescendantEntity>[] = []

  for (const desc of entry.descendants) {
    if (desc.templates?.length) {
      for (const template of desc.templates) {
        const args = template.args || {}
        const targetLang = args['1'] || args['lang'] || ''
        const targetWord = args['2'] || args['word'] || ''

        if (targetLang && targetWord) {
          inputs.push({
            $type: 'Descendant',
            name: `${entry.word} (${entry.lang}) -> ${targetWord} (${targetLang})`,
            sourceWord: entry.word,
            sourceLanguageCode: entry.lang_code,
            descendantWord: targetWord,
            targetLanguage: targetLang,
            targetLanguageCode: targetLang.toLowerCase().slice(0, 3),
            depth: desc.depth,
            tags: desc.tags,
            text: desc.text,
          })
        }
      }
    } else if (desc.text) {
      const match = desc.text.match(/^([^:]+):\s*(.+)$/)
      if (match) {
        const [, targetLang, targetWord] = match

        inputs.push({
          $type: 'Descendant',
          name: `${entry.word} (${entry.lang}) -> ${targetWord} (${targetLang})`,
          sourceWord: entry.word,
          sourceLanguageCode: entry.lang_code,
          descendantWord: targetWord.trim(),
          targetLanguage: targetLang.trim(),
          depth: desc.depth,
          tags: desc.tags,
          text: desc.text,
        })
      }
    }
  }

  return inputs
}

/**
 * Extract compound components from a word based on head templates
 */
function toCompoundInputs(entry: WiktionaryEntry): CreateInput<CompoundEntity>[] {
  const inputs: CreateInput<CompoundEntity>[] = []

  for (const template of entry.head_templates || []) {
    if (template.name?.includes('compound') || template.name === 'compound') {
      const args = template.args || {}
      let position = 1

      for (const [key, value] of Object.entries(args)) {
        if (/^\d+$/.test(key) && value && value !== entry.lang_code) {
          inputs.push({
            $type: 'Compound',
            name: `${entry.word} = ...${value}...`,
            compound: entry.word,
            languageCode: entry.lang_code,
            component: value,
            position,
            componentType: position === 1 ? 'head' : 'modifier',
          })
          position++
        }
      }
    }
  }

  if (entry.etymology_text?.toLowerCase().includes('compound of')) {
    const compoundMatch = entry.etymology_text.match(/[Cc]ompound of\s+([^,]+(?:,\s*[^,]+)*)/i)
    if (compoundMatch) {
      const parts = compoundMatch[1].split(/\s+(?:and|\+)\s+/)
      let position = 1

      for (const part of parts) {
        const cleanPart = part.replace(/["'"']/g, '').trim().split(/\s/)[0]
        if (cleanPart && cleanPart.length > 0) {
          if (!inputs.find(e => e.component === cleanPart)) {
            inputs.push({
              $type: 'Compound',
              name: `${entry.word} = ...${cleanPart}...`,
              compound: entry.word,
              languageCode: entry.lang_code,
              component: cleanPart,
              position,
              componentType: position === 1 ? 'head' : 'modifier',
            })
            position++
          }
        }
      }
    }
  }

  return inputs
}

/**
 * Extract collocations from usage examples
 */
function toCollocationInputs(entry: WiktionaryEntry): CreateInput<CollocationEntity>[] {
  const inputs: CreateInput<CollocationEntity>[] = []
  const seenCollocates = new Set<string>()

  for (const sense of entry.senses || []) {
    for (const example of sense.examples || []) {
      if (example.text) {
        const words = example.text.toLowerCase().split(/\s+/)
        const targetIndex = words.findIndex(w =>
          w.replace(/[^a-z]/g, '') === entry.word.toLowerCase()
        )

        if (targetIndex !== -1) {
          const before = targetIndex > 0 ? words[targetIndex - 1].replace(/[^a-z]/g, '') : null
          const after = targetIndex < words.length - 1 ? words[targetIndex + 1].replace(/[^a-z]/g, '') : null

          for (const collocate of [before, after].filter(Boolean) as string[]) {
            if (collocate.length > 2 && !seenCollocates.has(collocate)) {
              seenCollocates.add(collocate)
              inputs.push({
                $type: 'Collocation',
                name: `${entry.word} + ${collocate}`,
                word: entry.word,
                languageCode: entry.lang_code,
                collocate,
                phrase: example.text,
                example: example.text,
              })
            }
          }
        }
      }
    }
  }

  return inputs
}

/**
 * Parse etymology text to extract ancestry information
 */
function toEtymologyLinkInputs(entry: WiktionaryEntry): CreateInput<EtymologyLinkEntity>[] {
  if (!entry.etymology_text) return []

  const inputs: CreateInput<EtymologyLinkEntity>[] = []
  const etymText = entry.etymology_text

  const patterns = [
    { pattern: /[Ff]rom\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+([^\s,.(]+)/g, type: 'inherited' as EtymologyLinkType },
    { pattern: /[Bb]orrowed\s+from\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+([^\s,.(]+)/g, type: 'borrowed' as EtymologyLinkType },
    { pattern: /[Ll]earned\s+borrowing\s+from\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+([^\s,.(]+)/g, type: 'learned' as EtymologyLinkType },
    { pattern: /[Cc]alque\s+(?:of|from)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+([^\s,.(]+)/g, type: 'calque' as EtymologyLinkType },
    { pattern: /[Dd]erived\s+from\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+([^\s,.(]+)/g, type: 'derived' as EtymologyLinkType },
  ]

  let distance = 1
  for (const { pattern, type } of patterns) {
    let match: RegExpExecArray | null
    while ((match = pattern.exec(etymText)) !== null) {
      const [, ancestorLang, ancestorWord] = match
      const cleanWord = ancestorWord.replace(/["'"'*]/g, '')

      if (cleanWord && cleanWord.length > 0) {
        if (!inputs.find(e => e.ancestorWord === cleanWord && e.ancestorLanguage === ancestorLang)) {
          inputs.push({
            $type: 'EtymologyLink',
            name: `${entry.word} <- ${cleanWord} (${ancestorLang})`,
            word: entry.word,
            languageCode: entry.lang_code,
            ancestorWord: cleanWord,
            ancestorLanguage: ancestorLang,
            ancestorLanguageCode: ancestorLang.toLowerCase().slice(0, 3),
            linkType: type,
            distance,
          })
          distance++
        }
      }
    }
  }

  return inputs
}

/**
 * Extract cognate information from etymology
 */
function toCognateInputs(entry: WiktionaryEntry): CreateInput<CognateEntity>[] {
  if (!entry.etymology_text) return []

  const inputs: CreateInput<CognateEntity>[] = []
  const etymText = entry.etymology_text

  const cognatePattern = /[Cc]ognate(?:s)?\s+(?:with|to)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+([^\s,.(]+)/g

  let match: RegExpExecArray | null
  while ((match = cognatePattern.exec(etymText)) !== null) {
    const [, cognateLang, cognateWord] = match
    const cleanWord = cognateWord.replace(/["'"'*]/g, '')

    if (cleanWord && cleanWord.length > 0) {
      const rootMatch = etymText.match(/(?:from|ultimately from)\s+(Proto-[A-Za-z-]+)/i)
      const rootLang = rootMatch?.[1] || 'Unknown'
      const rootLangCode = rootLang.toLowerCase().replace(/[^a-z]/g, '').slice(0, 5)

      inputs.push({
        $type: 'Cognate',
        name: `${entry.word} ~ ${cleanWord} (${cognateLang})`,
        word: entry.word,
        languageCode: entry.lang_code,
        language: entry.lang,
        root: entry.word,
        rootLanguage: rootLang,
        rootLanguageCode: rootLangCode,
      })
    }
  }

  return inputs
}

// =============================================================================
// Batch Processing Types
// =============================================================================

interface ProcessedEntry {
  word: CreateInput<WordEntity>
  definitions: CreateInput<DefinitionEntity>[]
  pronunciations: CreateInput<PronunciationEntity>[]
  translations: CreateInput<TranslationEntity>[]
  relatedWords: CreateInput<RelatedWordEntity>[]
  etymology: CreateInput<EtymologyEntity> | null
  forms: CreateInput<FormEntity>[]
  descendants: CreateInput<DescendantEntity>[]
  compounds: CreateInput<CompoundEntity>[]
  collocations: CreateInput<CollocationEntity>[]
  cognates: CreateInput<CognateEntity>[]
  etymologyLinks: CreateInput<EtymologyLinkEntity>[]
}

// =============================================================================
// Batch Writer using ParqueDB API
// =============================================================================

/**
 * Process a batch of entries using ParqueDB API with $link for relationships
 */
async function processBatch(
  db: ParqueDB,
  entries: ProcessedEntry[],
  manifest: WiktionaryManifest,
  verbose: boolean
): Promise<void> {
  const words = db.collection<WordEntity>('words')
  const definitions = db.collection<DefinitionEntity>('definitions')
  const pronunciations = db.collection<PronunciationEntity>('pronunciations')
  const translations = db.collection<TranslationEntity>('translations')
  const relatedWords = db.collection<RelatedWordEntity>('relatedWords')
  const etymologies = db.collection<EtymologyEntity>('etymologies')
  const forms = db.collection<FormEntity>('forms')
  const descendants = db.collection<DescendantEntity>('descendants')
  const compounds = db.collection<CompoundEntity>('compounds')
  const collocations = db.collection<CollocationEntity>('collocations')
  const cognates = db.collection<CognateEntity>('cognates')
  const etymologyLinks = db.collection<EtymologyLinkEntity>('etymologyLinks')

  for (const entry of entries) {
    try {
      // 1. Create the word entity
      const wordEntity = await words.create(entry.word)
      const wordId = wordEntity.$id
      manifest.entityCounts.words++

      // Helper to create child entities and link them to the word
      const createChildEntities = async <T extends { wordId?: string }>(
        collection: ReturnType<typeof db.collection>,
        inputs: CreateInput<T>[],
        predicate: string,
        countKey: keyof typeof manifest.entityCounts
      ) => {
        const childIds: EntityId[] = []

        for (const input of inputs) {
          try {
            // Add wordId to the input for reference
            const entityInput = { ...input, wordId } as CreateInput<T>
            const entity = await collection.create(entityInput)
            childIds.push(entity.$id)
            manifest.entityCounts[countKey]++
          } catch (error) {
            if (verbose) {
              console.warn(`Failed to create ${predicate}:`, error)
            }
          }
        }

        // Link all child entities to the word using $link
        if (childIds.length > 0) {
          try {
            await words.update(wordId.split('/')[1], {
              $link: { [predicate]: childIds },
            })
          } catch (error) {
            if (verbose) {
              console.warn(`Failed to link ${predicate} to word:`, error)
            }
          }
        }
      }

      // 2. Create definitions and link to word
      await createChildEntities(definitions, entry.definitions, 'definitions', 'definitions')

      // 3. Create pronunciations and link to word
      await createChildEntities(pronunciations, entry.pronunciations, 'pronunciations', 'pronunciations')

      // 4. Create translations and link to word
      await createChildEntities(translations, entry.translations, 'translations', 'translations')

      // 5. Create related words and link to word
      await createChildEntities(relatedWords, entry.relatedWords, 'relatedWords', 'relatedWords')

      // 6. Create etymology and link to word
      if (entry.etymology) {
        await createChildEntities(etymologies, [entry.etymology], 'etymology', 'etymologies')
      }

      // 7. Create forms and link to word
      await createChildEntities(forms, entry.forms, 'forms', 'forms')

      // 8. Create descendants and link to word
      await createChildEntities(descendants, entry.descendants, 'descendants', 'descendants')

      // 9. Create compounds and link to word
      await createChildEntities(compounds, entry.compounds, 'compounds', 'compounds')

      // 10. Create collocations and link to word
      await createChildEntities(collocations, entry.collocations, 'collocations', 'collocations')

      // 11. Create cognates and link to word
      await createChildEntities(cognates, entry.cognates, 'cognates', 'cognates')

      // 12. Create etymology links and link to word
      await createChildEntities(etymologyLinks, entry.etymologyLinks, 'etymologyLinks', 'etymologyLinks')

    } catch (error) {
      if (verbose) {
        console.warn(`Failed to process word entry: ${entry.word.name}`, error)
      }
    }
  }
}

// =============================================================================
// Main Loader Function
// =============================================================================

/**
 * Load Wiktionary data from kaikki.org into ParqueDB
 *
 * Uses the ParqueDB API with:
 * - db.collection('words').create({...}) for creating entities
 * - db.collection('definitions').create({...}) for child entities
 * - $link operator to establish word-definition relationships
 *
 * Data flows to:
 * - data/{ns}/data.parquet - Entity storage
 * - rels/forward/{ns}.parquet - Forward relationship indexes
 * - rels/reverse/{ns}.parquet - Reverse relationship indexes
 *
 * @param dataDir - Directory for ParqueDB data storage
 * @param sourceUrl - URL to JSONL file
 * @param config - Loader configuration
 */
export async function loadWiktionary(
  dataDir: string,
  sourceUrl: string,
  config: Partial<WiktionaryLoaderConfig> = {}
): Promise<WiktionaryManifest> {
  const fullConfig: Required<WiktionaryLoaderConfig> = {
    ...DEFAULT_LOADER_CONFIG,
    ...config,
    batchSize: config.batchSize ?? 100, // Default to smaller batch for API calls
  }

  // Initialize ParqueDB with FsBackend for local storage
  const storage = new FsBackend(dataDir)
  const db = new ParqueDB({ storage })

  const manifest: WiktionaryManifest = {
    loadStarted: new Date().toISOString(),
    sourceUrl,
    totalEntries: 0,
    entriesByLanguage: {},
    entityCounts: {
      words: 0,
      definitions: 0,
      pronunciations: 0,
      translations: 0,
      relatedWords: 0,
      etymologies: 0,
      forms: 0,
      descendants: 0,
      compounds: 0,
      collocations: 0,
      cognates: 0,
      etymologyLinks: 0,
      frequencies: 0,
    },
    files: [],
    config: fullConfig,
  }

  console.log(`Starting Wiktionary load from ${sourceUrl}`)
  console.log(`Data directory: ${dataDir}`)
  console.log(`Using ParqueDB with FsBackend`)
  console.log(`Config: ${JSON.stringify(fullConfig, null, 2)}`)

  // Fetch the JSONL stream
  const response = await fetch(sourceUrl)
  if (!response.ok) {
    throw new Error(`Failed to fetch ${sourceUrl}: ${response.status} ${response.statusText}`)
  }
  if (!response.body) {
    throw new Error('Response has no body')
  }

  let batch: ProcessedEntry[] = []
  let entriesProcessed = 0
  let lastLogTime = Date.now()

  // Process entries from stream
  for await (const entry of parseJsonlStream(response.body)) {
    // Check max entries limit
    if (entriesProcessed >= fullConfig.maxEntries) {
      console.log(`Reached max entries limit (${fullConfig.maxEntries})`)
      break
    }

    // Skip redirects
    if (fullConfig.skipRedirects && entry.redirect) {
      continue
    }

    // Filter by language
    if (fullConfig.languages.length > 0 && !fullConfig.languages.includes(entry.lang_code)) {
      continue
    }

    entriesProcessed++
    manifest.totalEntries++
    manifest.entriesByLanguage[entry.lang_code] = (manifest.entriesByLanguage[entry.lang_code] ?? 0) + 1

    // Transform entry to processed structure
    const processed: ProcessedEntry = {
      word: toWordInput(entry),
      definitions: toDefinitionInputs(entry),
      pronunciations: toPronunciationInputs(entry),
      translations: toTranslationInputs(entry),
      relatedWords: toRelatedWordInputs(entry),
      etymology: toEtymologyInput(entry),
      forms: toFormInputs(entry),
      descendants: toDescendantInputs(entry),
      compounds: toCompoundInputs(entry),
      collocations: toCollocationInputs(entry),
      cognates: toCognateInputs(entry),
      etymologyLinks: toEtymologyLinkInputs(entry),
    }

    batch.push(processed)

    // Process batch when full
    if (batch.length >= fullConfig.batchSize) {
      console.log(`Processing batch of ${batch.length} entries...`)
      await processBatch(db, batch, manifest, fullConfig.verbose)
      batch = []
    }

    // Progress logging
    const now = Date.now()
    if (fullConfig.verbose && now - lastLogTime > 5000) {
      console.log(`Processed ${entriesProcessed} entries, ${manifest.entityCounts.words} words created...`)
      lastLogTime = now
    }
  }

  // Process remaining batch
  if (batch.length > 0) {
    console.log(`Processing final batch of ${batch.length} entries...`)
    await processBatch(db, batch, manifest, fullConfig.verbose)
  }

  // Write manifest
  manifest.loadCompleted = new Date().toISOString()
  const manifestJson = JSON.stringify(manifest, null, 2)
  await storage.write(
    'wiktionary/_manifest.json',
    new TextEncoder().encode(manifestJson),
    { contentType: 'application/json' }
  )
  manifest.files.push('wiktionary/_manifest.json')

  console.log('\nLoad complete!')
  console.log(`Total entries: ${manifest.totalEntries}`)
  console.log(`Entity counts:`)
  console.log(`  Words: ${manifest.entityCounts.words}`)
  console.log(`  Definitions: ${manifest.entityCounts.definitions}`)
  console.log(`  Pronunciations: ${manifest.entityCounts.pronunciations}`)
  console.log(`  Translations: ${manifest.entityCounts.translations}`)
  console.log(`  Related words: ${manifest.entityCounts.relatedWords}`)
  console.log(`  Etymologies: ${manifest.entityCounts.etymologies}`)
  console.log(`  Forms: ${manifest.entityCounts.forms}`)
  console.log(`  Descendants: ${manifest.entityCounts.descendants}`)
  console.log(`  Compounds: ${manifest.entityCounts.compounds}`)
  console.log(`  Collocations: ${manifest.entityCounts.collocations}`)
  console.log(`  Cognates: ${manifest.entityCounts.cognates}`)
  console.log(`  Etymology links: ${manifest.entityCounts.etymologyLinks}`)
  console.log(`Files written: ${manifest.files.length}`)

  return manifest
}

// =============================================================================
// Streaming Iterator for Custom Processing
// =============================================================================

/**
 * Stream Wiktionary entries with custom transformation
 *
 * Use this for custom processing pipelines or when you need
 * control over how entries are processed.
 *
 * @example
 * ```typescript
 * for await (const { entry, inputs } of streamWiktionary(url, config)) {
 *   // Custom processing
 *   await myCustomHandler(entry, inputs)
 * }
 * ```
 */
export async function* streamWiktionary(
  sourceUrl: string,
  config: Partial<WiktionaryLoaderConfig> = {}
): AsyncGenerator<{
  entry: WiktionaryEntry
  inputs: ProcessedEntry
}> {
  const fullConfig: Required<WiktionaryLoaderConfig> = {
    ...DEFAULT_LOADER_CONFIG,
    ...config,
  }

  const response = await fetch(sourceUrl)
  if (!response.ok) {
    throw new Error(`Failed to fetch ${sourceUrl}: ${response.status}`)
  }
  if (!response.body) {
    throw new Error('Response has no body')
  }

  let entriesProcessed = 0

  for await (const entry of parseJsonlStream(response.body)) {
    if (entriesProcessed >= fullConfig.maxEntries) break
    if (fullConfig.skipRedirects && entry.redirect) continue
    if (fullConfig.languages.length > 0 && !fullConfig.languages.includes(entry.lang_code)) continue

    entriesProcessed++

    yield {
      entry,
      inputs: {
        word: toWordInput(entry),
        definitions: toDefinitionInputs(entry),
        pronunciations: toPronunciationInputs(entry),
        translations: toTranslationInputs(entry),
        relatedWords: toRelatedWordInputs(entry),
        etymology: toEtymologyInput(entry),
        forms: toFormInputs(entry),
        descendants: toDescendantInputs(entry),
        compounds: toCompoundInputs(entry),
        collocations: toCollocationInputs(entry),
        cognates: toCognateInputs(entry),
        etymologyLinks: toEtymologyLinkInputs(entry),
      },
    }
  }
}

// =============================================================================
// CLI Entry Point
// =============================================================================

/**
 * Run loader from command line
 */
async function main(): Promise<void> {
  const sourceUrl = process.env.WIKTIONARY_URL ??
    'https://kaikki.org/dictionary/English/kaikki.org-dictionary-English.jsonl'

  const dataDir = process.env.DATA_DIR ?? './wiktionary-data'

  const config: WiktionaryLoaderConfig = {
    languages: process.env.LANGUAGES?.split(',') ?? [],
    maxEntries: process.env.MAX_ENTRIES ? parseInt(process.env.MAX_ENTRIES) : undefined,
    batchSize: process.env.BATCH_SIZE ? parseInt(process.env.BATCH_SIZE) : 100,
    verbose: process.env.VERBOSE === 'true',
  }

  await loadWiktionary(dataDir, sourceUrl, config)
}

// Run if executed directly
if (typeof require !== 'undefined' && require.main === module) {
  main().catch(console.error)
}
