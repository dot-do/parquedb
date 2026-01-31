#!/usr/bin/env npx tsx
/**
 * Generate sample data for testing examples
 *
 * Creates small synthetic datasets for:
 * - Wikidata (sample entities)
 * - CommonCrawl (sample hosts/links)
 * - Wiktionary (sample dictionary entries)
 */

import { ParqueDB, FsBackend } from '../src'
import type { CreateInput } from '../src/types'
import * as fs from 'fs'
import * as path from 'path'

// =============================================================================
// Wikidata Sample
// =============================================================================

async function generateWikidataSample(outputDir: string) {
  console.log('Generating Wikidata sample...')
  const startTime = Date.now()

  const storage = new FsBackend(outputDir)
  const db = new ParqueDB({ storage })

  const items = db.collection('items')
  const properties = db.collection('properties')
  const claims = db.collection('claims')

  // Create some sample properties
  const sampleProperties = [
    { id: 'P31', label: 'instance of' },
    { id: 'P279', label: 'subclass of' },
    { id: 'P17', label: 'country' },
    { id: 'P131', label: 'located in administrative territorial entity' },
    { id: 'P569', label: 'date of birth' },
    { id: 'P570', label: 'date of death' },
    { id: 'P106', label: 'occupation' },
    { id: 'P27', label: 'country of citizenship' },
    { id: 'P19', label: 'place of birth' },
    { id: 'P20', label: 'place of death' },
  ]

  let propertyCount = 0
  for (const prop of sampleProperties) {
    await properties.create({
      $id: `properties/${prop.id}`,
      $type: 'wikidata:Property',
      name: prop.label,
      wikidataId: prop.id,
      datatype: 'wikibase-item',
      labelEn: prop.label,
    } as CreateInput)
    propertyCount++
  }

  // Create sample items (famous people, places, concepts)
  const sampleItems = [
    { id: 'Q5', label: 'human', type: 'concept' },
    { id: 'Q515', label: 'city', type: 'concept' },
    { id: 'Q6256', label: 'country', type: 'concept' },
    { id: 'Q30', label: 'United States of America', type: 'country' },
    { id: 'Q84', label: 'London', type: 'city' },
    { id: 'Q90', label: 'Paris', type: 'city' },
    { id: 'Q1', label: 'universe', type: 'concept' },
    { id: 'Q2', label: 'Earth', type: 'planet' },
    { id: 'Q76', label: 'Barack Obama', type: 'human' },
    { id: 'Q23', label: 'George Washington', type: 'human' },
    { id: 'Q9439', label: 'Victoria', type: 'human' },
    { id: 'Q42', label: 'Douglas Adams', type: 'human' },
  ]

  let itemCount = 0
  for (const item of sampleItems) {
    await items.create({
      $id: `items/${item.id}`,
      $type: 'wikidata:Item',
      name: item.label,
      wikidataId: item.id,
      itemType: item.type,
      labelEn: item.label,
    } as CreateInput)
    itemCount++
  }

  // Create some sample claims
  const sampleClaims = [
    { subject: 'Q76', property: 'P31', object: 'Q5', name: 'Barack Obama instance of human' },
    { subject: 'Q23', property: 'P31', object: 'Q5', name: 'George Washington instance of human' },
    { subject: 'Q42', property: 'P31', object: 'Q5', name: 'Douglas Adams instance of human' },
    { subject: 'Q84', property: 'P31', object: 'Q515', name: 'London instance of city' },
    { subject: 'Q90', property: 'P31', object: 'Q515', name: 'Paris instance of city' },
    { subject: 'Q30', property: 'P31', object: 'Q6256', name: 'USA instance of country' },
    { subject: 'Q76', property: 'P27', object: 'Q30', name: 'Barack Obama citizen of USA' },
    { subject: 'Q42', property: 'P27', object: 'Q30', name: 'Douglas Adams citizen of USA' },
  ]

  let claimCount = 0
  for (const claim of sampleClaims) {
    await claims.create({
      $id: `claims/claim-${claimCount + 1}`,
      $type: 'wikidata:Claim',
      name: claim.name,
      subjectId: claim.subject,
      propertyId: claim.property,
      objectId: claim.object,
      rank: 'normal',
      snaktype: 'value',
      datatype: 'wikibase-entityid',
    } as CreateInput)
    claimCount++
  }

  const durationMs = Date.now() - startTime

  // Write manifest
  const manifest = {
    dataset: 'wikidata-sample',
    loadedAt: new Date().toISOString(),
    entityCounts: {
      items: itemCount,
      properties: propertyCount,
      claims: claimCount,
    },
    durationMs,
  }

  fs.writeFileSync(path.join(outputDir, 'manifest.json'), JSON.stringify(manifest, null, 2))

  console.log(`  Items: ${itemCount}`)
  console.log(`  Properties: ${propertyCount}`)
  console.log(`  Claims: ${claimCount}`)
  console.log(`  Duration: ${durationMs}ms`)
}

// =============================================================================
// CommonCrawl Sample
// =============================================================================

async function generateCommonCrawlSample(outputDir: string) {
  console.log('Generating CommonCrawl sample...')
  const startTime = Date.now()

  const storage = new FsBackend(outputDir)
  const db = new ParqueDB({ storage })

  const hosts = db.collection('hosts')
  const links = db.collection('links')

  // Create sample hosts
  const sampleHosts = [
    { hostname: 'example.com', tld: 'com', sld: 'example' },
    { hostname: 'google.com', tld: 'com', sld: 'google' },
    { hostname: 'github.com', tld: 'com', sld: 'github' },
    { hostname: 'wikipedia.org', tld: 'org', sld: 'wikipedia' },
    { hostname: 'stackoverflow.com', tld: 'com', sld: 'stackoverflow' },
    { hostname: 'mozilla.org', tld: 'org', sld: 'mozilla' },
    { hostname: 'w3.org', tld: 'org', sld: 'w3' },
    { hostname: 'cloudflare.com', tld: 'com', sld: 'cloudflare' },
    { hostname: 'nodejs.org', tld: 'org', sld: 'nodejs' },
    { hostname: 'npmjs.com', tld: 'com', sld: 'npmjs' },
    { hostname: 'docs.google.com', tld: 'com', sld: 'google', subdomain: 'docs' },
    { hostname: 'mail.google.com', tld: 'com', sld: 'google', subdomain: 'mail' },
  ]

  let hostCount = 0
  for (const host of sampleHosts) {
    await hosts.create({
      $id: `hosts/host-${hostCount + 1}`,
      $type: 'Host',
      name: host.hostname,
      reversedHostname: host.hostname.split('.').reverse().join('.'),
      tld: host.tld,
      sld: host.sld,
      subdomain: host.subdomain || null,
      hostId: hostCount + 1,
      crawlId: 'cc-sample-2025',
      ingestedAt: new Date(),
      isActive: true,
    } as CreateInput)
    hostCount++
  }

  // Create sample links
  const sampleLinks = [
    { from: 1, to: 2, name: 'example.com -> google.com' },
    { from: 1, to: 3, name: 'example.com -> github.com' },
    { from: 3, to: 4, name: 'github.com -> wikipedia.org' },
    { from: 3, to: 5, name: 'github.com -> stackoverflow.com' },
    { from: 5, to: 4, name: 'stackoverflow.com -> wikipedia.org' },
    { from: 6, to: 7, name: 'mozilla.org -> w3.org' },
    { from: 8, to: 9, name: 'cloudflare.com -> nodejs.org' },
    { from: 9, to: 10, name: 'nodejs.org -> npmjs.com' },
    { from: 10, to: 3, name: 'npmjs.com -> github.com' },
    { from: 2, to: 11, name: 'google.com -> docs.google.com' },
    { from: 2, to: 12, name: 'google.com -> mail.google.com' },
  ]

  let linkCount = 0
  for (const link of sampleLinks) {
    await links.create({
      $id: `links/link-${linkCount + 1}`,
      $type: 'Link',
      name: link.name,
      fromHostId: link.from,
      toHostId: link.to,
      crawlId: 'cc-sample-2025',
      linkCount: 1,
      ingestedAt: new Date(),
    } as CreateInput)
    linkCount++
  }

  const durationMs = Date.now() - startTime

  // Write manifest
  const manifest = {
    dataset: 'commoncrawl-sample',
    loadedAt: new Date().toISOString(),
    entityCounts: {
      hosts: hostCount,
      links: linkCount,
    },
    durationMs,
  }

  fs.writeFileSync(path.join(outputDir, 'manifest.json'), JSON.stringify(manifest, null, 2))

  console.log(`  Hosts: ${hostCount}`)
  console.log(`  Links: ${linkCount}`)
  console.log(`  Duration: ${durationMs}ms`)
}

// =============================================================================
// Wiktionary Sample
// =============================================================================

async function generateWiktionarySample(outputDir: string) {
  console.log('Generating Wiktionary sample...')
  const startTime = Date.now()

  const storage = new FsBackend(outputDir)
  const db = new ParqueDB({ storage })

  const words = db.collection('words')
  const definitions = db.collection('definitions')
  const pronunciations = db.collection('pronunciations')
  const translations = db.collection('translations')
  const relatedWords = db.collection('relatedWords')

  // Sample English dictionary entries
  const sampleWords = [
    {
      word: 'hello',
      pos: 'interjection',
      defs: ['A greeting used when meeting someone', 'Used to answer a telephone call'],
      ipa: '/həˈloʊ/',
      translations: [{ lang: 'Spanish', word: 'hola' }, { lang: 'French', word: 'bonjour' }],
      synonyms: ['hi', 'hey', 'greetings'],
    },
    {
      word: 'world',
      pos: 'noun',
      defs: ['The Earth and all its inhabitants', 'A particular region or group of people'],
      ipa: '/wɜːld/',
      translations: [{ lang: 'Spanish', word: 'mundo' }, { lang: 'French', word: 'monde' }],
      synonyms: ['globe', 'earth', 'planet'],
    },
    {
      word: 'run',
      pos: 'verb',
      defs: ['To move swiftly on foot', 'To operate or function', 'To manage or direct'],
      ipa: '/ɹʌn/',
      translations: [{ lang: 'Spanish', word: 'correr' }, { lang: 'French', word: 'courir' }],
      synonyms: ['sprint', 'dash', 'race'],
    },
    {
      word: 'beautiful',
      pos: 'adjective',
      defs: ['Pleasing to the senses or mind aesthetically', 'Very good or skilled'],
      ipa: '/ˈbjuːtɪfəl/',
      translations: [{ lang: 'Spanish', word: 'hermoso' }, { lang: 'French', word: 'beau' }],
      synonyms: ['lovely', 'gorgeous', 'attractive'],
    },
    {
      word: 'database',
      pos: 'noun',
      defs: ['A structured set of data held in a computer', 'An organized collection of information'],
      ipa: '/ˈdeɪtəˌbeɪs/',
      translations: [{ lang: 'Spanish', word: 'base de datos' }, { lang: 'French', word: 'base de données' }],
      synonyms: ['databank', 'data store'],
    },
    {
      word: 'parquet',
      pos: 'noun',
      defs: ['A type of wooden flooring made of blocks arranged in a geometric pattern', 'A columnar storage format for data'],
      ipa: '/pɑːˈkeɪ/',
      translations: [{ lang: 'Spanish', word: 'parqué' }, { lang: 'French', word: 'parquet' }],
      synonyms: ['flooring'],
    },
    {
      word: 'query',
      pos: 'noun',
      defs: ['A question, especially one expressing doubt', 'A request for data from a database'],
      ipa: '/ˈkwɪəri/',
      translations: [{ lang: 'Spanish', word: 'consulta' }, { lang: 'French', word: 'requête' }],
      synonyms: ['question', 'inquiry'],
    },
    {
      word: 'index',
      pos: 'noun',
      defs: ['An alphabetical list of references', 'A data structure for fast lookups'],
      ipa: '/ˈɪndeks/',
      translations: [{ lang: 'Spanish', word: 'índice' }, { lang: 'French', word: 'index' }],
      synonyms: ['catalog', 'directory'],
    },
  ]

  let wordCount = 0
  let definitionCount = 0
  let pronunciationCount = 0
  let translationCount = 0
  let relatedWordCount = 0

  for (const entry of sampleWords) {
    // Create word
    const wordEntity = await words.create({
      $id: `words/${entry.word}-${entry.pos}`,
      $type: 'Word',
      name: `${entry.word} (English, ${entry.pos})`,
      word: entry.word,
      language: 'English',
      languageCode: 'en',
      pos: entry.pos,
      senseCount: entry.defs.length,
      translationCount: entry.translations.length,
      hasIpa: true,
    } as CreateInput)
    wordCount++

    // Create definitions
    for (let i = 0; i < entry.defs.length; i++) {
      await definitions.create({
        $id: `definitions/${entry.word}-${entry.pos}-def${i + 1}`,
        $type: 'Definition',
        name: entry.defs[i].slice(0, 50),
        word: entry.word,
        languageCode: 'en',
        pos: entry.pos,
        senseIndex: i,
        gloss: entry.defs[i],
      } as CreateInput)
      definitionCount++
    }

    // Create pronunciation
    await pronunciations.create({
      $id: `pronunciations/${entry.word}-${entry.pos}-ipa`,
      $type: 'Pronunciation',
      name: entry.ipa,
      word: entry.word,
      languageCode: 'en',
      ipa: entry.ipa,
    } as CreateInput)
    pronunciationCount++

    // Create translations
    for (const trans of entry.translations) {
      await translations.create({
        $id: `translations/${entry.word}-${trans.lang.toLowerCase()}`,
        $type: 'Translation',
        name: `${entry.word} -> ${trans.word} (${trans.lang})`,
        sourceWord: entry.word,
        sourceLanguageCode: 'en',
        targetWord: trans.word,
        targetLanguage: trans.lang,
        targetLanguageCode: trans.lang.slice(0, 2).toLowerCase(),
      } as CreateInput)
      translationCount++
    }

    // Create related words (synonyms)
    for (const syn of entry.synonyms) {
      await relatedWords.create({
        $id: `relatedWords/${entry.word}-syn-${syn}`,
        $type: 'RelatedWord',
        name: `${entry.word} synonym ${syn}`,
        word: entry.word,
        languageCode: 'en',
        relationType: 'synonym',
        relatedWord: syn,
      } as CreateInput)
      relatedWordCount++
    }
  }

  const durationMs = Date.now() - startTime

  // Write manifest
  const manifest = {
    dataset: 'wiktionary-sample',
    loadedAt: new Date().toISOString(),
    entityCounts: {
      words: wordCount,
      definitions: definitionCount,
      pronunciations: pronunciationCount,
      translations: translationCount,
      relatedWords: relatedWordCount,
    },
    durationMs,
  }

  fs.writeFileSync(path.join(outputDir, 'manifest.json'), JSON.stringify(manifest, null, 2))

  console.log(`  Words: ${wordCount}`)
  console.log(`  Definitions: ${definitionCount}`)
  console.log(`  Pronunciations: ${pronunciationCount}`)
  console.log(`  Translations: ${translationCount}`)
  console.log(`  Related words: ${relatedWordCount}`)
  console.log(`  Duration: ${durationMs}ms`)
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  const baseDir = process.cwd()

  console.log('Generating sample data for ParqueDB examples\n')
  console.log('=' .repeat(60))

  // Wikidata
  console.log('\n1. Wikidata')
  console.log('-'.repeat(40))
  const wikidataDir = path.join(baseDir, 'data', 'wikidata')
  fs.mkdirSync(wikidataDir, { recursive: true })
  await generateWikidataSample(wikidataDir)

  // CommonCrawl
  console.log('\n2. CommonCrawl')
  console.log('-'.repeat(40))
  const commoncrawlDir = path.join(baseDir, 'data', 'commoncrawl')
  fs.mkdirSync(commoncrawlDir, { recursive: true })
  await generateCommonCrawlSample(commoncrawlDir)

  // Wiktionary
  console.log('\n3. Wiktionary')
  console.log('-'.repeat(40))
  const wiktionaryDir = path.join(baseDir, 'data', 'wiktionary')
  fs.mkdirSync(wiktionaryDir, { recursive: true })
  await generateWiktionarySample(wiktionaryDir)

  console.log('\n' + '='.repeat(60))
  console.log('Sample data generation complete!')
}

main().catch(console.error)
