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
// Type Partitions for Multi-File Output
// =============================================================================

/**
 * Type-specific file partitions for Wikidata entities
 *
 * Partitions ~120M entities into manageable files by type:
 * - people: ~10M humans (Q5)
 * - organizations: ~3M organizations, companies, universities
 * - places: ~5M cities, countries, geographic features
 * - works: ~5M films, books, albums, games
 * - taxa: ~10M biological taxa
 * - other: ~85M everything else
 */
export const TYPE_PARTITIONS = {
  people: {
    types: ['Q5'], // Human
    file: 'people.parquet',
    estimatedCount: 10_000_000,
  },
  organizations: {
    types: ['Q43229', 'Q4830453', 'Q3918', 'Q327333'], // Organization, company, university, govt agency
    file: 'organizations.parquet',
    estimatedCount: 3_000_000,
  },
  places: {
    types: ['Q515', 'Q6256', 'Q7275', 'Q5107', 'Q8502', 'Q4022', 'Q23397', 'Q23442'], // Cities, countries, states, continents, mountains, rivers, lakes, islands
    file: 'places.parquet',
    estimatedCount: 5_000_000,
  },
  works: {
    types: ['Q11424', 'Q571', 'Q482994', 'Q7366', 'Q7889', 'Q5398426'], // Films, books, albums, songs, games, TV series
    file: 'works.parquet',
    estimatedCount: 5_000_000,
  },
  taxa: {
    types: ['Q16521'], // Taxon (biological taxonomy)
    file: 'taxa.parquet',
    estimatedCount: 10_000_000,
  },
  other: {
    types: [], // Catch-all for everything else
    file: 'other.parquet',
    estimatedCount: 85_000_000,
  },
} as const

export type PartitionName = keyof typeof TYPE_PARTITIONS

/**
 * Map Q-ID types to partition names
 */
export const TYPE_TO_PARTITION: Record<string, PartitionName> = {}
for (const [partition, config] of Object.entries(TYPE_PARTITIONS) as [PartitionName, typeof TYPE_PARTITIONS[PartitionName]][]) {
  for (const typeId of config.types) {
    TYPE_TO_PARTITION[typeId] = partition
  }
}

/**
 * Get partition name for an entity based on its P31 (instance of) values
 */
export function getPartitionForEntity(entity: WikidataEntity): PartitionName {
  const p31Values = entity.claims?.['P31']?.map(c => c.mainsnak.datavalue?.type === 'wikibase-entityid' ? (c.mainsnak.datavalue?.value as { id: string })?.id : null).filter(Boolean) ?? []

  for (const typeId of p31Values) {
    if (typeId && TYPE_TO_PARTITION[typeId]) {
      return TYPE_TO_PARTITION[typeId]
    }
  }

  return 'other'
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
// External Identifier Properties
// =============================================================================

/**
 * External identifier properties with URL templates
 */
export const EXTERNAL_ID_PROPERTIES: Record<string, { name: string; urlTemplate?: string }> = {
  // Entertainment
  'P345': { name: 'IMDb ID', urlTemplate: 'https://www.imdb.com/title/$1' },
  'P1712': { name: 'Metacritic ID', urlTemplate: 'https://www.metacritic.com/$1' },
  'P1258': { name: 'Rotten Tomatoes ID', urlTemplate: 'https://www.rottentomatoes.com/$1' },
  'P1874': { name: 'Netflix ID', urlTemplate: 'https://www.netflix.com/title/$1' },
  'P8055': { name: 'Disney+ ID' },
  'P4983': { name: 'TMDb movie ID', urlTemplate: 'https://www.themoviedb.org/movie/$1' },
  'P4985': { name: 'TMDb TV series ID', urlTemplate: 'https://www.themoviedb.org/tv/$1' },

  // Music
  'P434': { name: 'MusicBrainz artist ID', urlTemplate: 'https://musicbrainz.org/artist/$1' },
  'P436': { name: 'MusicBrainz release group ID', urlTemplate: 'https://musicbrainz.org/release-group/$1' },
  'P1902': { name: 'Spotify artist ID', urlTemplate: 'https://open.spotify.com/artist/$1' },
  'P2722': { name: 'Discogs artist ID', urlTemplate: 'https://www.discogs.com/artist/$1' },
  'P2850': { name: 'iTunes artist ID', urlTemplate: 'https://music.apple.com/artist/$1' },

  // Academic/Science
  'P356': { name: 'DOI', urlTemplate: 'https://doi.org/$1' },
  'P496': { name: 'ORCID', urlTemplate: 'https://orcid.org/$1' },
  'P698': { name: 'PubMed ID', urlTemplate: 'https://pubmed.ncbi.nlm.nih.gov/$1' },
  'P932': { name: 'PubMed Central ID', urlTemplate: 'https://www.ncbi.nlm.nih.gov/pmc/articles/$1' },
  'P1053': { name: 'ResearchGate publication ID', urlTemplate: 'https://www.researchgate.net/publication/$1' },
  'P2427': { name: 'GRID ID', urlTemplate: 'https://www.grid.ac/institutes/$1' },
  'P6366': { name: 'Microsoft Academic ID' },
  'P4012': { name: 'Semantic Scholar author ID', urlTemplate: 'https://www.semanticscholar.org/author/$1' },

  // Books
  'P212': { name: 'ISBN-13' },
  'P957': { name: 'ISBN-10' },
  'P675': { name: 'Google Books ID', urlTemplate: 'https://books.google.com/books?id=$1' },
  'P1085': { name: 'LibraryThing work ID', urlTemplate: 'https://www.librarything.com/work/$1' },
  'P648': { name: 'Open Library ID', urlTemplate: 'https://openlibrary.org/works/$1' },
  'P2969': { name: 'Goodreads book ID', urlTemplate: 'https://www.goodreads.com/book/show/$1' },

  // Social Media
  'P2002': { name: 'Twitter username', urlTemplate: 'https://twitter.com/$1' },
  'P2013': { name: 'Facebook ID', urlTemplate: 'https://www.facebook.com/$1' },
  'P2003': { name: 'Instagram username', urlTemplate: 'https://www.instagram.com/$1' },
  'P2397': { name: 'YouTube channel ID', urlTemplate: 'https://www.youtube.com/channel/$1' },
  'P2037': { name: 'GitHub username', urlTemplate: 'https://github.com/$1' },
  'P4264': { name: 'LinkedIn company ID', urlTemplate: 'https://www.linkedin.com/company/$1' },
  'P4015': { name: 'Vimeo ID', urlTemplate: 'https://vimeo.com/$1' },
  'P3417': { name: 'Quora topic ID', urlTemplate: 'https://www.quora.com/topic/$1' },
  'P2847': { name: 'Google+ ID' },
  'P7085': { name: 'TikTok username', urlTemplate: 'https://www.tiktok.com/@$1' },
  'P8604': { name: 'Twitch channel ID', urlTemplate: 'https://www.twitch.tv/$1' },

  // Reference
  'P646': { name: 'Freebase ID' },
  'P227': { name: 'GND ID', urlTemplate: 'https://d-nb.info/gnd/$1' },
  'P214': { name: 'VIAF ID', urlTemplate: 'https://viaf.org/viaf/$1' },
  'P244': { name: 'Library of Congress authority ID', urlTemplate: 'https://id.loc.gov/authorities/$1' },
  'P213': { name: 'ISNI', urlTemplate: 'https://isni.org/isni/$1' },
  'P349': { name: 'National Diet Library ID', urlTemplate: 'https://id.ndl.go.jp/auth/ndlna/$1' },
  'P268': { name: 'BnF ID', urlTemplate: 'https://catalogue.bnf.fr/ark:/12148/cb$1' },
  'P269': { name: 'IdRef ID', urlTemplate: 'https://www.idref.fr/$1' },

  // Other
  'P856': { name: 'official website' },
  'P854': { name: 'reference URL' },
  'P973': { name: 'described at URL' },
}

/**
 * URL-type properties (not external-id but useful to extract)
 */
export const URL_PROPERTIES = ['P856', 'P854', 'P973', 'P1324', 'P1581'] as const

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

/**
 * Parquet schema for external identifiers
 * Stores all external IDs and URLs extracted from Wikidata claims
 */
export const IDENTIFIERS_PARQUET_SCHEMA = {
  entity_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },      // Q-number
  property_id: { type: 'BYTE_ARRAY', encoding: 'DICT' },     // P-number (indexed)
  property_name: { type: 'BYTE_ARRAY', encoding: 'DICT' },   // Human-readable property name
  value: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },          // The identifier value
  source_url: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true }, // Constructed URL if applicable
} as const

/** Record for identifier storage */
export interface IdentifierRecord {
  entity_id: string
  property_id: string
  property_name: string
  value: string
  source_url: string | null
}

/**
 * Parquet schema for geographic coordinates
 * Stores P625 coordinate values with geohash for spatial indexing
 */
export const COORDINATES_PARQUET_SCHEMA = {
  entity_id: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },      // Q-number
  entity_name: { type: 'BYTE_ARRAY', encoding: 'PLAIN', optional: true }, // English label
  entity_type: { type: 'BYTE_ARRAY', encoding: 'DICT', optional: true },  // Primary P31 type
  lat: { type: 'DOUBLE', encoding: 'PLAIN' },
  lng: { type: 'DOUBLE', encoding: 'PLAIN' },
  precision: { type: 'DOUBLE', encoding: 'PLAIN', optional: true },
  globe: { type: 'BYTE_ARRAY', encoding: 'DICT' },           // Q2 = Earth
  geohash: { type: 'BYTE_ARRAY', encoding: 'PLAIN' },        // For spatial index
  // Bounding box components for native Parquet spatial statistics (GeoParquet style)
  bbox_xmin: { type: 'DOUBLE', encoding: 'PLAIN' },
  bbox_xmax: { type: 'DOUBLE', encoding: 'PLAIN' },
  bbox_ymin: { type: 'DOUBLE', encoding: 'PLAIN' },
  bbox_ymax: { type: 'DOUBLE', encoding: 'PLAIN' },
} as const

/** Record for coordinate storage */
export interface CoordinateRecord {
  entity_id: string
  entity_name: string | null
  entity_type: string | null
  lat: number
  lng: number
  precision: number | null
  globe: string
  geohash: string
  // Bounding box (for point data, min = max)
  bbox_xmin: number
  bbox_xmax: number
  bbox_ymin: number
  bbox_ymax: number
}

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

// =============================================================================
// Identifier Extraction
// =============================================================================

/**
 * Extract all external identifiers from an entity's claims
 */
export function extractIdentifiers(entity: WikidataEntity): IdentifierRecord[] {
  const records: IdentifierRecord[] = []

  if (!entity.claims) return records

  for (const [propertyId, claims] of Object.entries(entity.claims)) {
    // Check if this is a known external ID property
    const propInfo = EXTERNAL_ID_PROPERTIES[propertyId]
    if (!propInfo) continue

    for (const claim of claims) {
      // Skip deprecated claims
      if (claim.rank === 'deprecated') continue

      const datavalue = claim.mainsnak.datavalue
      if (!datavalue) continue

      // Handle external-id and string types
      if (datavalue.type === 'string') {
        const value = datavalue.value
        let sourceUrl: string | null = null

        // Construct URL if template available
        if (propInfo.urlTemplate) {
          sourceUrl = propInfo.urlTemplate.replace('$1', encodeURIComponent(value))
        }

        records.push({
          entity_id: entity.id,
          property_id: propertyId,
          property_name: propInfo.name,
          value,
          source_url: sourceUrl,
        })
      }
    }
  }

  // Also extract URL-type claims
  for (const urlProp of URL_PROPERTIES) {
    const claims = entity.claims[urlProp]
    if (!claims) continue

    for (const claim of claims) {
      if (claim.rank === 'deprecated') continue

      const datavalue = claim.mainsnak.datavalue
      if (!datavalue || datavalue.type !== 'string') continue

      const url = datavalue.value
      const propInfo = EXTERNAL_ID_PROPERTIES[urlProp] ?? { name: urlProp }

      records.push({
        entity_id: entity.id,
        property_id: urlProp,
        property_name: propInfo.name,
        value: url,
        source_url: url,
      })
    }
  }

  return records
}

// =============================================================================
// Coordinate Extraction
// =============================================================================

/**
 * Extract P625 (coordinate location) from an entity
 * Returns null if no coordinates found
 */
export function extractCoordinates(
  entity: WikidataEntity,
  encodeGeohash: (lat: number, lng: number, precision?: number) => string
): CoordinateRecord | null {
  const p625Claims = entity.claims?.['P625']
  if (!p625Claims || p625Claims.length === 0) return null

  // Get preferred or first normal-rank claim
  const claim = p625Claims.find(c => c.rank === 'preferred') ??
                p625Claims.find(c => c.rank === 'normal') ??
                p625Claims[0]

  const datavalue = claim.mainsnak.datavalue
  if (!datavalue || datavalue.type !== 'globecoordinate') return null

  const { latitude, longitude, precision, globe } = datavalue.value as CoordinateValue

  // Skip non-Earth coordinates (e.g., Moon, Mars)
  // Q2 = Earth, Q405 = Moon, Q111 = Mars
  const globeId = globe.replace('http://www.wikidata.org/entity/', '')
  if (globeId !== 'Q2') return null

  // Compute geohash with precision 8 (~38m)
  const geohash = encodeGeohash(latitude, longitude, 8)

  // Get entity name and type
  const entityName = getEnglishLabel(entity.labels)
  const entityType = extractPrimaryType(entity.claims)

  return {
    entity_id: entity.id,
    entity_name: entityName,
    entity_type: entityType,
    lat: latitude,
    lng: longitude,
    precision: precision ?? null,
    globe: globeId,
    geohash,
    // For point data, bbox is the same point
    bbox_xmin: longitude,
    bbox_xmax: longitude,
    bbox_ymin: latitude,
    bbox_ymax: latitude,
  }
}
