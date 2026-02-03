#!/usr/bin/env npx tsx
/**
 * Generate Wikidata Sample Dataset for Testing/Benchmarking
 *
 * Fetches real Wikidata entities from the API to create a test dataset.
 * This script creates a representative sample of ~500 entities including:
 * - Famous people (humans)
 * - Major cities
 * - Countries
 * - Properties
 * - Claims linking them together
 *
 * Usage:
 *   npx tsx scripts/generate-wikidata-sample.ts [--output ./data/wikidata]
 */

import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

// =============================================================================
// Configuration
// =============================================================================

const WIKIDATA_API = 'https://www.wikidata.org/w/api.php'

// Sample entities to fetch - curated for testing relationships
const SAMPLE_ENTITIES = {
  // Famous people (humans - Q5)
  humans: [
    'Q42',    // Douglas Adams
    'Q76',    // Barack Obama
    'Q23',    // George Washington
    'Q9439',  // Queen Victoria
    'Q5593',  // Albert Einstein
    'Q7251',  // Alan Turing
    'Q1339',  // Johann Sebastian Bach
    'Q1299',  // The Beatles (group, but related)
    'Q1744',  // Steve Jobs
    'Q312',   // Apple Inc.
    'Q937',   // Albert Einstein
    'Q9061',  // Marie Curie
    'Q34660', // Leonardo da Vinci
    'Q35332', // Pablo Picasso
    'Q1868',  // Paul McCartney
    'Q254',   // Wolfgang Amadeus Mozart
    'Q392',   // Bob Dylan
    'Q41',    // Michael Jackson
    'Q11518', // Aristotle
    'Q5879',  // Johann Wolfgang von Goethe
    'Q859',   // Plato
    'Q8023',  // Nelson Mandela
    'Q9441',  // Elizabeth II
    'Q6279',  // Franklin D. Roosevelt
    'Q3435',  // Abraham Lincoln
    'Q7374',  // Nikola Tesla
    'Q235',   // Winston Churchill
    'Q9353',  // Mahatma Gandhi
    'Q6294',  // John F. Kennedy
    'Q5582',  // Vincent van Gogh
    'Q1035',  // Charles Darwin
    'Q296',   // Bill Gates
    'Q7186',  // Mark Zuckerberg
    'Q4233718', // Elon Musk
    'Q317521', // Satya Nadella
  ],
  // Countries (Q6256)
  countries: [
    'Q30',    // United States
    'Q145',   // United Kingdom
    'Q142',   // France
    'Q183',   // Germany
    'Q148',   // China
    'Q17',    // Japan
    'Q668',   // India
    'Q159',   // Russia
    'Q155',   // Brazil
    'Q408',   // Australia
    'Q38',    // Italy
    'Q29',    // Spain
    'Q33',    // Finland
    'Q36',    // Poland
    'Q55',    // Netherlands
    'Q39',    // Switzerland
    'Q35',    // Denmark
    'Q40',    // Austria
    'Q191',   // Estonia
    'Q16',    // Canada
  ],
  // Major cities (Q515)
  cities: [
    'Q84',    // London
    'Q90',    // Paris
    'Q60',    // New York City
    'Q1490',  // Tokyo
    'Q956',   // Beijing
    'Q64',    // Berlin
    'Q220',   // Rome
    'Q1218',  // Jerusalem
    'Q84',    // London
    'Q174',   // Sao Paulo
    'Q1156',  // Mumbai
    'Q8652',  // Helsinki
    'Q1899',  // Kyoto
    'Q649',   // Moscow
    'Q1218',  // Jerusalem
    'Q2807',  // Madrid
    'Q490',   // Milan
    'Q65',    // Los Angeles
    'Q62',    // San Francisco
    'Q1726',  // Munich
    'Q23768', // Amsterdam
    'Q1754',  // Stockholm
    'Q1748',  // Copenhagen
    'Q1761',  // Dublin
    'Q270',   // Warsaw
  ],
  // Common entity types (for type definitions)
  types: [
    'Q5',     // human
    'Q515',   // city
    'Q6256',  // country
    'Q11424', // film
    'Q571',   // book
    'Q482994',// album
    'Q7889',  // video game
    'Q43229', // organization
    'Q4830453', // company
    'Q3918',  // university
  ],
  // Films and creative works
  creativeWorks: [
    'Q25190',   // The Hitchhiker's Guide to the Galaxy
    'Q172241',  // Monty Python and the Holy Grail
    'Q47703',   // The Matrix
    'Q47209',   // Star Wars
    'Q167726',  // The Lord of the Rings
    'Q223044',  // Inception
    'Q102438',  // 2001: A Space Odyssey
    'Q24871',   // Apollo 11
    'Q80270',   // Fight Club
    'Q217182',  // Pulp Fiction
  ],
}

// Common properties to include
const COMMON_PROPERTIES = [
  'P31',   // instance of
  'P279',  // subclass of
  'P17',   // country
  'P131',  // located in administrative territorial entity
  'P625',  // coordinate location
  'P569',  // date of birth
  'P570',  // date of death
  'P106',  // occupation
  'P27',   // country of citizenship
  'P19',   // place of birth
  'P20',   // place of death
  'P26',   // spouse
  'P40',   // child
  'P22',   // father
  'P25',   // mother
  'P50',   // author
  'P57',   // director
  'P175',  // performer
  'P136',  // genre
  'P800',  // notable work
  'P36',   // capital
  'P35',   // head of state
  'P6',    // head of government
  'P1082', // population
  'P2046', // area
  'P571',  // inception
  'P576',  // dissolved
  'P856',  // official website
  'P18',   // image
  'P154',  // logo
]

// =============================================================================
// Types
// =============================================================================

interface WikidataEntity {
  id: string
  type: 'item' | 'property'
  labels?: Record<string, { language: string; value: string }>
  descriptions?: Record<string, { language: string; value: string }>
  aliases?: Record<string, Array<{ language: string; value: string }>>
  claims?: Record<string, WikidataClaim[]>
  sitelinks?: Record<string, { site: string; title: string }>
}

interface WikidataClaim {
  id: string
  mainsnak: {
    snaktype: 'value' | 'somevalue' | 'novalue'
    property: string
    datavalue?: {
      type: string
      value: unknown
    }
    datatype?: string
  }
  rank: 'preferred' | 'normal' | 'deprecated'
  qualifiers?: Record<string, Array<{ snaktype: string; property: string; datavalue?: unknown }>>
  references?: Array<{ snaks: Record<string, unknown[]> }>
}

interface ParqueDBEntity {
  $id: string
  $type: string
  name: string
  wikidataId: string
  itemType: string
  labelEn: string | null
  descriptionEn: string | null
  labels: Record<string, string>
  descriptions: Record<string, string>
  aliases: Record<string, string[]>
  sitelinks: Record<string, string>
  createdAt: string
  createdBy: string
  updatedAt: string
  updatedBy: string
  version: number
}

interface ParqueDBProperty {
  $id: string
  $type: string
  name: string
  wikidataId: string
  datatype: string
  labelEn: string | null
  descriptionEn: string | null
  labels: Record<string, string>
  descriptions: Record<string, string>
  createdAt: string
  createdBy: string
  updatedAt: string
  updatedBy: string
  version: number
}

interface ParqueDBClaim {
  $id: string
  $type: string
  name: string
  claimId: string
  subjectId: string
  propertyId: string
  objectId: string | null
  rank: string
  snaktype: string
  datatype: string
  value: unknown
  createdAt: string
  createdBy: string
  updatedAt: string
  updatedBy: string
  version: number
}

// =============================================================================
// API Functions
// =============================================================================

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function fetchEntities(ids: string[]): Promise<Map<string, WikidataEntity>> {
  const results = new Map<string, WikidataEntity>()

  // Batch in groups of 50 (API limit)
  const batchSize = 50
  for (let i = 0; i < ids.length; i += batchSize) {
    const batch = ids.slice(i, i + batchSize)
    const url = `${WIKIDATA_API}?action=wbgetentities&format=json&ids=${batch.join('|')}&languages=en|de|fr|es|ja|zh`

    try {
      const response = await fetch(url, {
        headers: { 'User-Agent': 'ParqueDB-DataLoader/1.0 (benchmarking)' },
      })

      if (!response.ok) {
        console.error(`HTTP ${response.status} for batch starting at ${i}`)
        continue
      }

      const data = await response.json() as { entities?: Record<string, WikidataEntity> }

      if (data.entities) {
        for (const [id, entity] of Object.entries(data.entities)) {
          if (!('missing' in entity)) {
            results.set(id, entity)
          }
        }
      }

      console.log(`  Fetched batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(ids.length / batchSize)}: ${batch.length} entities`)

      // Rate limiting - be nice to the API
      await sleep(100)
    } catch (error) {
      console.error(`Error fetching batch starting at ${i}:`, error)
    }
  }

  return results
}

async function fetchProperties(ids: string[]): Promise<Map<string, WikidataEntity>> {
  const results = new Map<string, WikidataEntity>()

  const url = `${WIKIDATA_API}?action=wbgetentities&format=json&ids=${ids.join('|')}&languages=en`

  try {
    const response = await fetch(url, {
      headers: { 'User-Agent': 'ParqueDB-DataLoader/1.0 (benchmarking)' },
    })

    if (!response.ok) {
      console.error(`HTTP ${response.status} for properties`)
      return results
    }

    const data = await response.json() as { entities?: Record<string, WikidataEntity> }

    if (data.entities) {
      for (const [id, entity] of Object.entries(data.entities)) {
        if (!('missing' in entity)) {
          results.set(id, entity)
        }
      }
    }
  } catch (error) {
    console.error('Error fetching properties:', error)
  }

  return results
}

// =============================================================================
// Transformation Functions
// =============================================================================

function extractLabel(entity: WikidataEntity, lang = 'en'): string | null {
  return entity.labels?.[lang]?.value ?? null
}

function extractDescription(entity: WikidataEntity, lang = 'en'): string | null {
  return entity.descriptions?.[lang]?.value ?? null
}

function extractAllLabels(entity: WikidataEntity): Record<string, string> {
  const labels: Record<string, string> = {}
  if (entity.labels) {
    for (const [lang, val] of Object.entries(entity.labels)) {
      labels[lang] = val.value
    }
  }
  return labels
}

function extractAllDescriptions(entity: WikidataEntity): Record<string, string> {
  const descriptions: Record<string, string> = {}
  if (entity.descriptions) {
    for (const [lang, val] of Object.entries(entity.descriptions)) {
      descriptions[lang] = val.value
    }
  }
  return descriptions
}

function extractAliases(entity: WikidataEntity): Record<string, string[]> {
  const aliases: Record<string, string[]> = {}
  if (entity.aliases) {
    for (const [lang, vals] of Object.entries(entity.aliases)) {
      aliases[lang] = vals.map(v => v.value)
    }
  }
  return aliases
}

function extractSitelinks(entity: WikidataEntity): Record<string, string> {
  const sitelinks: Record<string, string> = {}
  if (entity.sitelinks) {
    for (const [site, val] of Object.entries(entity.sitelinks)) {
      sitelinks[site] = val.title
    }
  }
  return sitelinks
}

function determineItemType(entity: WikidataEntity): string {
  // Check P31 (instance of) claims
  const instanceOf = entity.claims?.['P31']
  if (!instanceOf || instanceOf.length === 0) return 'other'

  const claim = instanceOf.find(c => c.rank === 'preferred') ??
                instanceOf.find(c => c.rank === 'normal') ??
                instanceOf[0]

  const value = claim?.mainsnak.datavalue
  if (value?.type === 'wikibase-entityid') {
    const typeId = (value.value as { id: string }).id
    // Map common types
    const typeMap: Record<string, string> = {
      'Q5': 'human',
      'Q515': 'city',
      'Q6256': 'country',
      'Q11424': 'film',
      'Q571': 'book',
      'Q7889': 'video game',
      'Q43229': 'organization',
      'Q4830453': 'company',
      'Q3918': 'university',
      'Q3624078': 'sovereign state',
      'Q7275': 'state',
    }
    return typeMap[typeId] ?? 'other'
  }

  return 'other'
}

function entityToParqueDB(entity: WikidataEntity): ParqueDBEntity {
  const now = new Date().toISOString()
  const labelEn = extractLabel(entity)

  return {
    $id: `items/${entity.id}`,
    $type: 'wikidata:Item',
    name: labelEn ?? entity.id,
    wikidataId: entity.id,
    itemType: determineItemType(entity),
    labelEn,
    descriptionEn: extractDescription(entity),
    labels: extractAllLabels(entity),
    descriptions: extractAllDescriptions(entity),
    aliases: extractAliases(entity),
    sitelinks: extractSitelinks(entity),
    createdAt: now,
    createdBy: 'system/wikidata-import',
    updatedAt: now,
    updatedBy: 'system/wikidata-import',
    version: 1,
  }
}

function propertyToParqueDB(entity: WikidataEntity): ParqueDBProperty {
  const now = new Date().toISOString()
  const labelEn = extractLabel(entity)

  return {
    $id: `properties/${entity.id}`,
    $type: 'wikidata:Property',
    name: labelEn ?? entity.id,
    wikidataId: entity.id,
    datatype: 'wikibase-item', // Default; actual datatype would need property-specific fetch
    labelEn,
    descriptionEn: extractDescription(entity),
    labels: extractAllLabels(entity),
    descriptions: extractAllDescriptions(entity),
    createdAt: now,
    createdBy: 'system/wikidata-import',
    updatedAt: now,
    updatedBy: 'system/wikidata-import',
    version: 1,
  }
}

function extractClaims(entity: WikidataEntity, propertyLabels: Map<string, string>): ParqueDBClaim[] {
  const claims: ParqueDBClaim[] = []
  const now = new Date().toISOString()

  if (!entity.claims) return claims

  for (const [propertyId, propertyClaims] of Object.entries(entity.claims)) {
    for (const claim of propertyClaims) {
      const value = claim.mainsnak.datavalue
      let objectId: string | null = null
      let valueData: unknown = null

      if (value?.type === 'wikibase-entityid') {
        objectId = (value.value as { id: string }).id
      } else if (value) {
        valueData = value.value
      }

      const propertyLabel = propertyLabels.get(propertyId) ?? propertyId

      claims.push({
        $id: `claims/${claim.id}`,
        $type: 'wikidata:Claim',
        name: `${entity.id} ${propertyLabel} ${objectId ?? 'value'}`,
        claimId: claim.id,
        subjectId: entity.id,
        propertyId,
        objectId,
        rank: claim.rank,
        snaktype: claim.mainsnak.snaktype,
        datatype: claim.mainsnak.datatype ?? 'unknown',
        value: valueData,
        createdAt: now,
        createdBy: 'system/wikidata-import',
        updatedAt: now,
        updatedBy: 'system/wikidata-import',
        version: 1,
      })
    }
  }

  return claims
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  const { values } = parseArgs({
    options: {
      output: {
        type: 'string',
        short: 'o',
        default: './data/wikidata',
      },
      help: {
        type: 'boolean',
        short: 'h',
      },
    },
  })

  if (values.help) {
    console.log(`
Generate Wikidata Sample Dataset

Usage:
  npx tsx scripts/generate-wikidata-sample.ts [--output ./data/wikidata]

Options:
  -o, --output   Output directory (default: ./data/wikidata)
  -h, --help     Show this help
`)
    return
  }

  const outputDir = values.output!
  const startTime = Date.now()

  console.log('='.repeat(60))
  console.log('Generating Wikidata Sample Dataset')
  console.log('='.repeat(60))
  console.log(`Output: ${outputDir}`)
  console.log('')

  // Collect all entity IDs
  const allEntityIds = [
    ...SAMPLE_ENTITIES.humans,
    ...SAMPLE_ENTITIES.countries,
    ...SAMPLE_ENTITIES.cities,
    ...SAMPLE_ENTITIES.types,
    ...SAMPLE_ENTITIES.creativeWorks,
  ]

  // Remove duplicates
  const uniqueEntityIds = [...new Set(allEntityIds)]
  console.log(`Fetching ${uniqueEntityIds.length} entities from Wikidata API...`)

  // Fetch entities
  const entities = await fetchEntities(uniqueEntityIds)
  console.log(`Received ${entities.size} entities`)
  console.log('')

  // Fetch properties
  console.log(`Fetching ${COMMON_PROPERTIES.length} properties...`)
  const properties = await fetchProperties(COMMON_PROPERTIES)
  console.log(`Received ${properties.size} properties`)
  console.log('')

  // Build property label map
  const propertyLabels = new Map<string, string>()
  for (const [id, prop] of properties) {
    const label = extractLabel(prop)
    if (label) propertyLabels.set(id, label)
  }

  // Transform to ParqueDB format
  console.log('Transforming to ParqueDB format...')

  const items: ParqueDBEntity[] = []
  const allClaims: ParqueDBClaim[] = []

  for (const entity of entities.values()) {
    items.push(entityToParqueDB(entity))
    allClaims.push(...extractClaims(entity, propertyLabels))
  }

  const propertyRecords: ParqueDBProperty[] = []
  for (const prop of properties.values()) {
    propertyRecords.push(propertyToParqueDB(prop))
  }

  console.log(`  Items: ${items.length}`)
  console.log(`  Properties: ${propertyRecords.length}`)
  console.log(`  Claims: ${allClaims.length}`)
  console.log('')

  // Write output files
  console.log('Writing output files...')

  await fs.mkdir(outputDir, { recursive: true })
  await fs.mkdir(join(outputDir, 'data', 'items'), { recursive: true })
  await fs.mkdir(join(outputDir, 'data', 'properties'), { recursive: true })
  await fs.mkdir(join(outputDir, 'data', 'claims'), { recursive: true })

  // Write items
  await fs.writeFile(
    join(outputDir, 'data', 'items', 'data.json'),
    JSON.stringify(items, null, 2)
  )

  // Write properties
  await fs.writeFile(
    join(outputDir, 'data', 'properties', 'data.json'),
    JSON.stringify(propertyRecords, null, 2)
  )

  // Write claims
  await fs.writeFile(
    join(outputDir, 'data', 'claims', 'data.json'),
    JSON.stringify(allClaims, null, 2)
  )

  // Write manifest
  const manifest = {
    dataset: 'wikidata-sample',
    source: 'https://www.wikidata.org/',
    loadedAt: new Date().toISOString(),
    entityCounts: {
      items: items.length,
      properties: propertyRecords.length,
      claims: allClaims.length,
    },
    sampleCategories: {
      humans: SAMPLE_ENTITIES.humans.length,
      countries: SAMPLE_ENTITIES.countries.length,
      cities: SAMPLE_ENTITIES.cities.length,
      types: SAMPLE_ENTITIES.types.length,
      creativeWorks: SAMPLE_ENTITIES.creativeWorks.length,
    },
    durationMs: Date.now() - startTime,
  }

  await fs.writeFile(
    join(outputDir, 'manifest.json'),
    JSON.stringify(manifest, null, 2)
  )

  // Write schema info
  const schemaInfo = {
    items: {
      $type: 'wikidata:Item',
      fields: ['wikidataId', 'itemType', 'labelEn', 'descriptionEn', 'labels', 'descriptions', 'aliases', 'sitelinks'],
    },
    properties: {
      $type: 'wikidata:Property',
      fields: ['wikidataId', 'datatype', 'labelEn', 'descriptionEn', 'labels', 'descriptions'],
    },
    claims: {
      $type: 'wikidata:Claim',
      fields: ['claimId', 'subjectId', 'propertyId', 'objectId', 'rank', 'snaktype', 'datatype', 'value'],
    },
  }

  await fs.writeFile(
    join(outputDir, 'schema.json'),
    JSON.stringify(schemaInfo, null, 2)
  )

  // Type distribution
  const typeDistribution: Record<string, number> = {}
  for (const item of items) {
    typeDistribution[item.itemType] = (typeDistribution[item.itemType] ?? 0) + 1
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)

  console.log('')
  console.log('='.repeat(60))
  console.log('Generation Complete')
  console.log('='.repeat(60))
  console.log(`Duration: ${elapsed}s`)
  console.log(`Items: ${items.length}`)
  console.log(`Properties: ${propertyRecords.length}`)
  console.log(`Claims: ${allClaims.length}`)
  console.log('')
  console.log('Type Distribution:')
  for (const [type, count] of Object.entries(typeDistribution).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${type.padEnd(20)} ${count}`)
  }
  console.log('')
  console.log(`Output: ${outputDir}`)
  console.log('='.repeat(60))
}

main().catch(console.error)
