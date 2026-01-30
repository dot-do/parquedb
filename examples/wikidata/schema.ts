/**
 * Wikidata Schema for ParqueDB
 *
 * Defines the entity types and relationships for storing Wikidata in ParqueDB.
 */

// =============================================================================
// Type Definitions
// =============================================================================

/** Multi-language text value */
export interface LanguageValue {
  language: string
  value: string
}

/** Map of language codes to values */
export interface LanguageMap {
  [language: string]: LanguageValue
}

/** Map of language codes to alias arrays */
export interface AliasMap {
  [language: string]: LanguageValue[]
}

/** Sitelink to Wikipedia/Wikimedia */
export interface Sitelink {
  site: string
  title: string
  badges?: string[]
  url?: string
}

/** Map of site IDs to sitelinks */
export interface SitelinkMap {
  [site: string]: Sitelink
}

/** Wikidata time value */
export interface TimeValue {
  time: string // ISO 8601 with + prefix
  timezone: number
  before: number
  after: number
  precision: number
  calendarmodel: string
}

/** Wikidata quantity value */
export interface QuantityValue {
  amount: string
  unit: string
  upperBound?: string
  lowerBound?: string
}

/** Wikidata coordinate value */
export interface CoordinateValue {
  latitude: number
  longitude: number
  altitude?: number
  precision?: number
  globe: string
}

/** Wikidata entity reference */
export interface EntityReference {
  'entity-type': 'item' | 'property'
  'numeric-id': number
  id: string
}

/** Wikidata monolingual text */
export interface MonolingualText {
  text: string
  language: string
}

/** Possible datavalue types */
export type DataValueType =
  | { type: 'wikibase-entityid'; value: EntityReference }
  | { type: 'string'; value: string }
  | { type: 'time'; value: TimeValue }
  | { type: 'quantity'; value: QuantityValue }
  | { type: 'globecoordinate'; value: CoordinateValue }
  | { type: 'monolingualtext'; value: MonolingualText }

/** Snak (statement value) */
export interface Snak {
  snaktype: 'value' | 'somevalue' | 'novalue'
  property: string
  datavalue?: DataValueType
  datatype?: string
  hash?: string
}

/** Reference (source for a claim) */
export interface Reference {
  hash: string
  snaks: { [property: string]: Snak[] }
  'snaks-order': string[]
}

/** Claim (statement) */
export interface Claim {
  mainsnak: Snak
  type: 'statement'
  id: string
  rank: 'preferred' | 'normal' | 'deprecated'
  qualifiers?: { [property: string]: Snak[] }
  'qualifiers-order'?: string[]
  references?: Reference[]
}

/** Raw Wikidata entity from JSON dump */
export interface WikidataEntity {
  id: string
  type: 'item' | 'property' | 'lexeme'
  labels?: LanguageMap
  descriptions?: LanguageMap
  aliases?: AliasMap
  claims?: { [property: string]: Claim[] }
  sitelinks?: SitelinkMap
  datatype?: string // For properties
  lastrevid?: number
  modified?: string
}

// =============================================================================
// ParqueDB Schema
// =============================================================================

/**
 * ParqueDB schema for Wikidata
 */
export const wikidataSchema = {
  // Items (Q-numbers)
  Item: {
    $type: 'wikidata:Item',
    $namespace: 'items',

    // Core fields
    id: { type: 'string', index: 'unique' },
    itemType: { type: 'string', index: true }, // Resolved from P31

    // Multi-language content (stored as Variant)
    labels: { type: 'variant' },
    descriptions: { type: 'variant' },
    aliases: { type: 'variant' },
    sitelinks: { type: 'variant' },

    // Extracted for indexing
    labelEn: { type: 'string', index: 'fts' },
    descriptionEn: { type: 'string' },

    // Metadata
    lastRevision: { type: 'int64' },
    modified: { type: 'timestamp' },

    // Relationships (claims are stored separately)
    claims: '<- Claim.subject[]',
  },

  // Properties (P-numbers)
  Property: {
    $type: 'wikidata:Property',
    $namespace: 'properties',

    id: { type: 'string', index: 'unique' },
    datatype: { type: 'string', index: true },

    labels: { type: 'variant' },
    descriptions: { type: 'variant' },
    aliases: { type: 'variant' },

    labelEn: { type: 'string', index: 'fts' },
    descriptionEn: { type: 'string' },

    lastRevision: { type: 'int64' },
    modified: { type: 'timestamp' },

    // Relationships
    claims: '<- Claim.property[]',
  },

  // Claims (statements linking items)
  Claim: {
    $type: 'wikidata:Claim',
    $namespace: 'claims',

    id: { type: 'string', index: 'unique' }, // Claim GUID
    rank: { type: 'string' }, // preferred/normal/deprecated

    // Main snak value (simplified)
    snaktype: { type: 'string' },
    datatype: { type: 'string' },
    value: { type: 'variant' }, // Actual value

    // Qualifiers and references (stored as Variant)
    qualifiers: { type: 'variant' },
    references: { type: 'variant' },

    // Relationships
    subject: '-> Item.claims',      // The item this claim is about
    property: '-> Property.claims', // The property being asserted
    object: '-> Item?',             // Target item (if entity reference)
  },

  // Qualifier (metadata on claims)
  Qualifier: {
    $type: 'wikidata:Qualifier',
    $namespace: 'qualifiers',

    id: { type: 'string', index: 'unique' },
    snaktype: { type: 'string' },
    datatype: { type: 'string' },
    value: { type: 'variant' },

    // Relationships
    claim: '-> Claim',
    property: '-> Property',
  },
} as const

// =============================================================================
// Type Categories
// =============================================================================

/**
 * Common Wikidata types for partitioning
 */
export const ENTITY_TYPES = {
  // People
  human: 'Q5',

  // Places
  city: 'Q515',
  country: 'Q6256',
  state: 'Q7275',
  continent: 'Q5107',
  mountain: 'Q8502',
  river: 'Q4022',
  lake: 'Q23397',
  island: 'Q23442',

  // Organizations
  organization: 'Q43229',
  company: 'Q4830453',
  university: 'Q3918',
  governmentAgency: 'Q327333',

  // Creative works
  film: 'Q11424',
  book: 'Q571',
  album: 'Q482994',
  song: 'Q7366',
  videoGame: 'Q7889',
  tvSeries: 'Q5398426',

  // Science
  scientificArticle: 'Q13442814',
  gene: 'Q7187',
  protein: 'Q8054',
  chemicalCompound: 'Q11173',
  disease: 'Q12136',

  // Other
  taxon: 'Q16521',
  asteroid: 'Q3863',
  star: 'Q523',
} as const

/**
 * Group entity types into categories for partitioning
 */
export const TYPE_CATEGORIES: Record<string, string[]> = {
  human: [ENTITY_TYPES.human],
  location: [
    ENTITY_TYPES.city,
    ENTITY_TYPES.country,
    ENTITY_TYPES.state,
    ENTITY_TYPES.continent,
    ENTITY_TYPES.mountain,
    ENTITY_TYPES.river,
    ENTITY_TYPES.lake,
    ENTITY_TYPES.island,
  ],
  organization: [
    ENTITY_TYPES.organization,
    ENTITY_TYPES.company,
    ENTITY_TYPES.university,
    ENTITY_TYPES.governmentAgency,
  ],
  creativeWork: [
    ENTITY_TYPES.film,
    ENTITY_TYPES.book,
    ENTITY_TYPES.album,
    ENTITY_TYPES.song,
    ENTITY_TYPES.videoGame,
    ENTITY_TYPES.tvSeries,
  ],
  science: [
    ENTITY_TYPES.scientificArticle,
    ENTITY_TYPES.gene,
    ENTITY_TYPES.protein,
    ENTITY_TYPES.chemicalCompound,
    ENTITY_TYPES.disease,
  ],
  nature: [
    ENTITY_TYPES.taxon,
    ENTITY_TYPES.asteroid,
    ENTITY_TYPES.star,
  ],
}

/**
 * Reverse lookup: Q-ID to category
 */
export const TYPE_TO_CATEGORY: Record<string, string> = {}
for (const [category, types] of Object.entries(TYPE_CATEGORIES)) {
  for (const type of types) {
    TYPE_TO_CATEGORY[type] = category
  }
}

// =============================================================================
// Property Categories
// =============================================================================

/**
 * Common properties for partitioning claims
 */
export const COMMON_PROPERTIES = {
  // Classification
  instanceOf: 'P31',
  subclassOf: 'P279',

  // Geography
  country: 'P17',
  locatedIn: 'P131',
  coordinate: 'P625',
  capital: 'P36',

  // Time
  dateOfBirth: 'P569',
  dateOfDeath: 'P570',
  inception: 'P571',
  dissolved: 'P576',
  publicationDate: 'P577',

  // People
  occupation: 'P106',
  employer: 'P108',
  educatedAt: 'P69',
  citizenship: 'P27',
  placeOfBirth: 'P19',
  placeOfDeath: 'P20',

  // Relationships
  spouse: 'P26',
  child: 'P40',
  father: 'P22',
  mother: 'P25',

  // Creative works
  author: 'P50',
  director: 'P57',
  performer: 'P175',
  genre: 'P136',
  notableWork: 'P800',

  // Identifiers
  imdbId: 'P345',
  isbnCode: 'P212',
  doi: 'P356',

  // Images
  image: 'P18',
  logo: 'P154',
} as const

/**
 * High-cardinality properties that get their own partitions
 */
export const PARTITIONED_PROPERTIES = [
  COMMON_PROPERTIES.instanceOf,
  COMMON_PROPERTIES.subclassOf,
  COMMON_PROPERTIES.country,
  COMMON_PROPERTIES.locatedIn,
  COMMON_PROPERTIES.occupation,
  COMMON_PROPERTIES.author,
  COMMON_PROPERTIES.genre,
] as const

// =============================================================================
// Parquet Schema Definitions
// =============================================================================

/**
 * Parquet schema for items
 */
export const ITEMS_PARQUET_SCHEMA = {
  ns: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  ts: { type: 'INT64', encoding: 'DELTA' },
  type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  name: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  item_type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  label_en: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  description_en: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  labels: { type: 'BYTE_ARRAY', encoding: 'PLAIN' }, // Variant JSON
  descriptions: { type: 'BYTE_ARRAY', encoding: 'PLAIN' }, // Variant JSON
  aliases: { type: 'BYTE_ARRAY', encoding: 'PLAIN' }, // Variant JSON
  sitelinks: { type: 'BYTE_ARRAY', encoding: 'PLAIN' }, // Variant JSON
  last_revision: { type: 'INT64', encoding: 'PLAIN', optional: true },
  modified: { type: 'INT64', encoding: 'PLAIN', optional: true },
  version: { type: 'INT32', encoding: 'PLAIN' },
  deleted: { type: 'BOOLEAN', encoding: 'PLAIN' },
} as const

/**
 * Parquet schema for properties
 */
export const PROPERTIES_PARQUET_SCHEMA = {
  ns: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  ts: { type: 'INT64', encoding: 'DELTA' },
  type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  name: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  datatype: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  label_en: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  description_en: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true },
  labels: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  descriptions: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  aliases: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  last_revision: { type: 'INT64', encoding: 'PLAIN', optional: true },
  modified: { type: 'INT64', encoding: 'PLAIN', optional: true },
  version: { type: 'INT32', encoding: 'PLAIN' },
  deleted: { type: 'BOOLEAN', encoding: 'PLAIN' },
} as const

/**
 * Parquet schema for claims
 */
export const CLAIMS_PARQUET_SCHEMA = {
  ns: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' }, // Claim GUID
  ts: { type: 'INT64', encoding: 'DELTA' },
  type: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  subject_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' }, // Q-number
  property_id: { type: 'BYTE_ARRAY', encoding: 'DICT' }, // P-number
  object_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true }, // Q-number if entity ref
  rank: { type: 'BYTE_ARRAY', encoding: 'DICT' }, // preferred/normal/deprecated
  snaktype: { type: 'BYTE_ARRAY', encoding: 'DICT' }, // value/somevalue/novalue
  datatype: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  value: { type: 'BYTE_ARRAY', encoding: 'PLAIN' }, // Variant JSON
  qualifiers: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true }, // Variant JSON
  references: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true }, // Variant JSON
  version: { type: 'INT32', encoding: 'PLAIN' },
  deleted: { type: 'BOOLEAN', encoding: 'PLAIN' },
} as const

/**
 * Parquet schema for edges (relationships)
 */
export const EDGES_PARQUET_SCHEMA = {
  ns: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  from_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  to_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },
  rel_type: { type: 'BYTE_ARRAY', encoding: 'DICT' }, // P-number
  ts: { type: 'INT64', encoding: 'DELTA' },
  claim_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' }, // Reference to claim
  rank: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  operator: { type: 'BYTE_ARRAY', encoding: 'DICT' },
  bidirectional: { type: 'BOOLEAN', encoding: 'PLAIN' },
  confidence: { type: 'FLOAT', encoding: 'PLAIN', optional: true },
  version: { type: 'INT32', encoding: 'PLAIN' },
  deleted: { type: 'BOOLEAN', encoding: 'PLAIN' },
} as const

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Extract primary type (P31 instance of) from claims
 */
export function extractPrimaryType(claims?: { [property: string]: Claim[] }): string | null {
  const instanceOfClaims = claims?.['P31']
  if (!instanceOfClaims || instanceOfClaims.length === 0) return null

  // Get the first preferred or normal rank claim
  const claim = instanceOfClaims.find(c => c.rank === 'preferred') ??
                instanceOfClaims.find(c => c.rank === 'normal') ??
                instanceOfClaims[0]

  const value = claim.mainsnak.datavalue
  if (value?.type === 'wikibase-entityid') {
    return value.value.id
  }

  return null
}

/**
 * Get category for a type Q-ID
 */
export function getCategoryForType(typeId: string | null): string {
  if (!typeId) return 'other'
  return TYPE_TO_CATEGORY[typeId] ?? 'other'
}

/**
 * Extract English label from labels map
 */
export function getEnglishLabel(labels?: LanguageMap): string | null {
  return labels?.['en']?.value ?? null
}

/**
 * Extract English description from descriptions map
 */
export function getEnglishDescription(descriptions?: LanguageMap): string | null {
  return descriptions?.['en']?.value ?? null
}

/**
 * Convert Wikidata entity to ParqueDB item record
 */
export function entityToItemRecord(entity: WikidataEntity): Record<string, unknown> {
  const primaryType = extractPrimaryType(entity.claims)
  const category = getCategoryForType(primaryType)

  return {
    ns: 'wikidata',
    id: entity.id,
    ts: BigInt(Date.now()) * BigInt(1000),
    type: 'Item',
    name: getEnglishLabel(entity.labels) ?? entity.id,
    item_type: category,
    label_en: getEnglishLabel(entity.labels),
    description_en: getEnglishDescription(entity.descriptions),
    labels: JSON.stringify(entity.labels ?? {}),
    descriptions: JSON.stringify(entity.descriptions ?? {}),
    aliases: JSON.stringify(entity.aliases ?? {}),
    sitelinks: JSON.stringify(entity.sitelinks ?? {}),
    last_revision: entity.lastrevid ? BigInt(entity.lastrevid) : null,
    modified: entity.modified ? BigInt(new Date(entity.modified).getTime()) * BigInt(1000) : null,
    version: 1,
    deleted: false,
  }
}

/**
 * Convert Wikidata entity to ParqueDB property record
 */
export function entityToPropertyRecord(entity: WikidataEntity): Record<string, unknown> {
  return {
    ns: 'wikidata',
    id: entity.id,
    ts: BigInt(Date.now()) * BigInt(1000),
    type: 'Property',
    name: getEnglishLabel(entity.labels) ?? entity.id,
    datatype: entity.datatype ?? 'unknown',
    label_en: getEnglishLabel(entity.labels),
    description_en: getEnglishDescription(entity.descriptions),
    labels: JSON.stringify(entity.labels ?? {}),
    descriptions: JSON.stringify(entity.descriptions ?? {}),
    aliases: JSON.stringify(entity.aliases ?? {}),
    last_revision: entity.lastrevid ? BigInt(entity.lastrevid) : null,
    modified: entity.modified ? BigInt(new Date(entity.modified).getTime()) * BigInt(1000) : null,
    version: 1,
    deleted: false,
  }
}

/**
 * Convert claim to ParqueDB claim record
 */
export function claimToRecord(
  subjectId: string,
  propertyId: string,
  claim: Claim
): Record<string, unknown> {
  const value = claim.mainsnak.datavalue
  let objectId: string | null = null

  // Extract object ID if this is an entity reference
  if (value?.type === 'wikibase-entityid') {
    objectId = value.value.id
  }

  return {
    ns: 'wikidata',
    id: claim.id,
    ts: BigInt(Date.now()) * BigInt(1000),
    type: 'Claim',
    subject_id: subjectId,
    property_id: propertyId,
    object_id: objectId,
    rank: claim.rank,
    snaktype: claim.mainsnak.snaktype,
    datatype: claim.mainsnak.datatype ?? 'unknown',
    value: JSON.stringify(value ?? null),
    qualifiers: claim.qualifiers ? JSON.stringify(claim.qualifiers) : null,
    references: claim.references ? JSON.stringify(claim.references) : null,
    version: 1,
    deleted: false,
  }
}

/**
 * Convert claim to edge record (for item-to-item relationships)
 */
export function claimToEdgeRecord(
  subjectId: string,
  propertyId: string,
  claim: Claim
): Record<string, unknown> | null {
  const value = claim.mainsnak.datavalue

  // Only create edges for entity references
  if (value?.type !== 'wikibase-entityid') {
    return null
  }

  const objectId = value.value.id

  return {
    ns: 'wikidata',
    from_id: subjectId,
    to_id: objectId,
    rel_type: propertyId,
    ts: BigInt(Date.now()) * BigInt(1000),
    claim_id: claim.id,
    rank: claim.rank,
    operator: '->',
    bidirectional: false,
    confidence: null,
    version: 1,
    deleted: false,
  }
}

/**
 * Determine partition key for a claim based on property
 */
export function getClaimPartitionKey(propertyId: string): string {
  if (PARTITIONED_PROPERTIES.includes(propertyId as typeof PARTITIONED_PROPERTIES[number])) {
    return propertyId
  }
  return 'other'
}

/**
 * Determine partition key for an item based on type
 */
export function getItemPartitionKey(entity: WikidataEntity): string {
  const primaryType = extractPrimaryType(entity.claims)
  return getCategoryForType(primaryType)
}
