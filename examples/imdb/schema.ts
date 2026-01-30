/**
 * IMDB Dataset Schema for ParqueDB
 *
 * Defines the graph schema for IMDB data including:
 * - Titles (movies, shows, episodes)
 * - People (actors, directors, writers)
 * - AlternateTitles (localized titles from title.akas.tsv)
 * - Ratings (separate entity for proper graph structure)
 * - Genres (separate entity with relationships to titles)
 * - Keywords (plot keywords)
 * - Relationships (acted_in, directed, wrote, collaborations, title links, etc.)
 *
 * Data source: https://datasets.imdbws.com/
 */

import type { Schema } from '../../src/types/schema'

// =============================================================================
// IMDB Entity Types
// =============================================================================

/**
 * Title types in IMDB
 */
export type TitleType =
  | 'movie'
  | 'short'
  | 'tvSeries'
  | 'tvEpisode'
  | 'tvMovie'
  | 'tvSpecial'
  | 'tvMiniSeries'
  | 'tvShort'
  | 'video'
  | 'videoGame'

/**
 * Person profession categories
 */
export type Profession =
  | 'actor'
  | 'actress'
  | 'director'
  | 'writer'
  | 'producer'
  | 'composer'
  | 'cinematographer'
  | 'editor'
  | 'production_designer'
  | 'self'
  | 'archive_footage'
  | 'archive_sound'

/**
 * Principal cast/crew category
 */
export type PrincipalCategory =
  | 'actor'
  | 'actress'
  | 'self'
  | 'director'
  | 'writer'
  | 'producer'
  | 'composer'
  | 'cinematographer'
  | 'editor'
  | 'production_designer'
  | 'archive_footage'
  | 'archive_sound'

// =============================================================================
// Raw IMDB TSV Row Types
// =============================================================================

/** Row from title.basics.tsv */
export interface TitleBasicsRow {
  tconst: string
  titleType: string
  primaryTitle: string
  originalTitle: string
  isAdult: string // '0' or '1'
  startYear: string // YYYY or '\N'
  endYear: string // YYYY or '\N'
  runtimeMinutes: string // number or '\N'
  genres: string // comma-separated or '\N'
}

/** Row from title.ratings.tsv */
export interface TitleRatingsRow {
  tconst: string
  averageRating: string // decimal
  numVotes: string // integer
}

/** Row from title.crew.tsv */
export interface TitleCrewRow {
  tconst: string
  directors: string // comma-separated nconsts or '\N'
  writers: string // comma-separated nconsts or '\N'
}

/** Row from title.principals.tsv */
export interface TitlePrincipalsRow {
  tconst: string
  ordering: string
  nconst: string
  category: string
  job: string // or '\N'
  characters: string // JSON array or '\N'
}

/** Row from title.episode.tsv */
export interface TitleEpisodeRow {
  tconst: string
  parentTconst: string
  seasonNumber: string // integer or '\N'
  episodeNumber: string // integer or '\N'
}

/** Row from name.basics.tsv */
export interface NameBasicsRow {
  nconst: string
  primaryName: string
  birthYear: string // YYYY or '\N'
  deathYear: string // YYYY or '\N'
  primaryProfession: string // comma-separated or '\N'
  knownForTitles: string // comma-separated tconsts or '\N'
}

/** Row from title.akas.tsv */
export interface TitleAkasRow {
  titleId: string
  ordering: string
  title: string
  region: string // or '\N'
  language: string // or '\N'
  types: string // comma-separated or '\N'
  attributes: string // comma-separated or '\N'
  isOriginalTitle: string // '0' or '1'
}

// =============================================================================
// Parsed Entity Types
// =============================================================================

/** Parsed Title entity */
export interface Title {
  id: string // tconst
  type: TitleType
  primaryTitle: string
  originalTitle: string
  isAdult: boolean
  startYear: number | null
  endYear: number | null
  runtimeMinutes: number | null
  genres: string[]
}

/** Parsed Person entity */
export interface Person {
  id: string // nconst
  name: string
  birthYear: number | null
  deathYear: number | null
  professions: string[]
}

/** Parsed Rating entity */
export interface Rating {
  titleId: string
  averageRating: number
  numVotes: number
}

/** Parsed Episode relationship */
export interface Episode {
  episodeId: string
  seriesId: string
  seasonNumber: number | null
  episodeNumber: number | null
}

/** Parsed Principal (person-title relationship) */
export interface Principal {
  titleId: string
  personId: string
  ordering: number
  category: PrincipalCategory
  job: string | null
  characters: string[]
}

/** Parsed alternate title */
export interface AlternateTitle {
  titleId: string
  ordering: number
  title: string
  region: string | null
  language: string | null
  types: string[]
  attributes: string[]
  isOriginalTitle: boolean
}

/** Genre entity for proper graph structure */
export interface Genre {
  id: string // normalized genre name as ID
  name: string
  titleCount?: number // for analytics
}

/** Title-Genre relationship (many-to-many) */
export interface TitleGenre {
  titleId: string
  genreId: string
}

/** Keyword entity for plot keywords */
export interface Keyword {
  id: string // normalized keyword as ID
  keyword: string
}

/** Title-Keyword relationship */
export interface TitleKeyword {
  titleId: string
  keywordId: string
}

/** Title-Title relationship types */
export type TitleLinkType =
  | 'sequel_to'
  | 'prequel_to'
  | 'remake_of'
  | 'spinoff_of'
  | 'based_on'
  | 'followed_by'
  | 'related_to'

/** Title-Title relationship */
export interface TitleLink {
  fromTitleId: string
  toTitleId: string
  linkType: TitleLinkType
}

/** Person-Person relationship types */
export type PersonRelationType =
  | 'collaborated_with'
  | 'frequently_works_with'
  | 'mentored_by'
  | 'mentor_of'

/** Person-Person collaboration relationship */
export interface PersonCollaboration {
  person1Id: string
  person2Id: string
  collaborationType: PersonRelationType
  sharedTitles: number // count of titles worked on together
  startYear: number | null
  endYear: number | null
}

/** Career milestone for a person */
export interface CareerMilestone {
  personId: string
  titleId: string
  year: number
  milestoneType: 'first_credit' | 'breakthrough' | 'peak_rating' | 'most_votes'
  role: string
}

// =============================================================================
// GraphDL Schema Definition
// =============================================================================

/**
 * IMDB schema for ParqueDB
 *
 * Entities:
 * - Title: Movies, TV shows, episodes, etc.
 * - Person: Actors, directors, writers, etc.
 * - AlternateTitle: Localized titles from title.akas.tsv
 * - Rating: Separate entity for proper graph structure
 * - Genre: Separate entity with relationships to titles
 * - Keyword: Plot keywords
 *
 * Relationships:
 * - Title -> Person (via principals): acted_in, directed, wrote, produced, etc.
 * - Title -> Title (via episodes): parent_series
 * - Title -> AlternateTitles (one-to-many)
 * - Title -> Genres (many-to-many)
 * - Title -> Keywords (many-to-many)
 * - Title -> Title (sequels, remakes, spinoffs)
 * - Person -> Person (collaborations, mentorship)
 */
export const imdbSchema: Schema = {
  Title: {
    $type: 'imdb:Title',
    $ns: 'imdb',
    $shred: ['type', 'startYear', 'genres'],

    // Core fields
    primaryTitle: 'string!',
    originalTitle: 'string!',
    type: 'string!', // TitleType enum
    isAdult: 'boolean = false',
    startYear: 'int?',
    endYear: 'int?',
    runtimeMinutes: 'int?',
    genres: 'string[]', // kept for backward compatibility, also have Genre entities

    // Relationships
    directors: '[<- Person.directed]',
    writers: '[<- Person.wrote]',
    cast: '[<- Person.actedIn]',
    producers: '[<- Person.produced]',

    // Episode relationship (for TV episodes)
    parentSeries: '-> Title.episodes',
    episodes: '[<- Title.parentSeries]',

    // Alternate titles
    alternateTitles: '[<- AlternateTitle.title]',

    // Rating (now a separate entity)
    rating: '<- Rating.title',

    // Genre relationships
    genreList: '[<- Genre.titles]',

    // Keywords
    keywords: '[<- Keyword.titles]',

    // Title-to-title links
    sequels: '[<- Title.prequelOf]',
    prequelOf: '-> Title.sequels',
    remakeOf: '-> Title.remakes',
    remakes: '[<- Title.remakeOf]',
    spinoffs: '[<- Title.spinoffOf]',
    spinoffOf: '-> Title.spinoffs',
    relatedTitles: '[~> Title]',
  },

  Person: {
    $type: 'imdb:Person',
    $ns: 'imdb',
    $shred: ['birthYear', 'professions'],

    // Core fields
    name: 'string!',
    birthYear: 'int?',
    deathYear: 'int?',
    professions: 'string[]',

    // Relationships (outbound from Person to Title)
    actedIn: '[-> Title.cast]',
    directed: '[-> Title.directors]',
    wrote: '[-> Title.writers]',
    produced: '[-> Title.producers]',

    // Known for (top titles)
    knownFor: '[~> Title]',

    // Person-to-person relationships
    collaborators: '[~> Person]', // bidirectional
    frequentCollaborators: '[~> Person]', // computed: 3+ titles together
    mentors: '[-> Person.mentees]',
    mentees: '[<- Person.mentors]',
  },

  /**
   * Principal: Edge entity for person-title relationships
   * Stores additional metadata about the relationship
   */
  Principal: {
    $type: 'imdb:Principal',
    $ns: 'imdb',

    // Edge endpoints
    title: '-> Title',
    person: '-> Person',

    // Edge properties
    ordering: 'int!',
    category: 'string!', // actor, director, writer, etc.
    job: 'string?',
    characters: 'string[]',
  },

  /**
   * AlternateTitle: Localized titles from title.akas.tsv
   */
  AlternateTitle: {
    $type: 'imdb:AlternateTitle',
    $ns: 'imdb',
    $shred: ['region', 'language'],

    // Reference to parent title
    title: '-> Title',

    // Alternate title properties
    ordering: 'int!',
    localizedTitle: 'string!',
    region: 'string?', // ISO 3166-1 alpha-2
    language: 'string?', // ISO 639-1
    types: 'string[]', // alternative, working, imdbDisplay, etc.
    attributes: 'string[]', // literal title, romanization, etc.
    isOriginalTitle: 'boolean = false',
  },

  /**
   * Rating: Separate entity for ratings
   */
  Rating: {
    $type: 'imdb:Rating',
    $ns: 'imdb',
    $shred: ['averageRating'],

    // Reference to title
    title: '-> Title',

    // Rating properties
    averageRating: 'float!',
    numVotes: 'int!',
  },

  /**
   * Genre: Separate entity for genres
   */
  Genre: {
    $type: 'imdb:Genre',
    $ns: 'imdb',

    // Genre name (also serves as ID)
    name: 'string!',

    // Relationships to titles
    titles: '[~> Title]',

    // Analytics
    titleCount: 'int?',
    averageRating: 'float?',
  },

  /**
   * TitleGenre: Edge entity for title-genre relationships
   */
  TitleGenre: {
    $type: 'imdb:TitleGenre',
    $ns: 'imdb',

    title: '-> Title',
    genre: '-> Genre',
  },

  /**
   * Keyword: Plot keywords
   */
  Keyword: {
    $type: 'imdb:Keyword',
    $ns: 'imdb',

    keyword: 'string!',
    titles: '[~> Title]',
  },

  /**
   * TitleKeyword: Edge entity for title-keyword relationships
   */
  TitleKeyword: {
    $type: 'imdb:TitleKeyword',
    $ns: 'imdb',

    title: '-> Title',
    keyword: '-> Keyword',
  },

  /**
   * TitleLink: Edge entity for title-to-title relationships
   * Represents sequels, prequels, remakes, spinoffs, etc.
   */
  TitleLink: {
    $type: 'imdb:TitleLink',
    $ns: 'imdb',

    fromTitle: '-> Title',
    toTitle: '-> Title',
    linkType: 'string!', // sequel_to, prequel_to, remake_of, spinoff_of, based_on, related_to
  },

  /**
   * PersonCollaboration: Edge entity for person-to-person relationships
   * Computed from shared titles
   */
  PersonCollaboration: {
    $type: 'imdb:PersonCollaboration',
    $ns: 'imdb',

    person1: '-> Person',
    person2: '-> Person',

    // Collaboration properties
    collaborationType: 'string!', // collaborated_with, frequently_works_with, mentored_by
    sharedTitleCount: 'int!',
    firstCollabYear: 'int?',
    lastCollabYear: 'int?',
    sharedTitleIds: 'string[]', // list of title IDs
  },

  /**
   * CareerMilestone: Tracks key moments in a person's career
   */
  CareerMilestone: {
    $type: 'imdb:CareerMilestone',
    $ns: 'imdb',

    person: '-> Person',
    title: '-> Title',

    year: 'int!',
    milestoneType: 'string!', // first_credit, breakthrough, peak_rating, most_votes
    role: 'string!', // acted_in, directed, etc.
    rating: 'float?',
    votes: 'int?',
  },
}

// =============================================================================
// Storage Paths
// =============================================================================

/**
 * R2/storage path conventions for IMDB data
 */
export const IMDB_STORAGE_PATHS = {
  /** Base path for IMDB data */
  base: 'imdb',

  /** Titles partitioned by type */
  titles: (type: TitleType) => `imdb/titles/type=${type}`,

  /** People data */
  people: 'imdb/people',

  /** Relationships by predicate */
  relationships: (predicate: string) => `imdb/relationships/${predicate}`,

  /** Ratings (separate entity) */
  ratings: 'imdb/ratings',

  /** Episodes (parent-child relationships) */
  episodes: 'imdb/episodes',

  /** Alternate titles partitioned by region */
  alternateTitles: 'imdb/alternate_titles',
  alternateTitlesByRegion: (region: string) => `imdb/alternate_titles/region=${region}`,

  /** Genres */
  genres: 'imdb/genres',
  titleGenres: 'imdb/title_genres',

  /** Keywords */
  keywords: 'imdb/keywords',
  titleKeywords: 'imdb/title_keywords',

  /** Title-to-title links */
  titleLinks: 'imdb/title_links',

  /** Person-to-person collaborations */
  personCollaborations: 'imdb/person_collaborations',

  /** Career milestones */
  careerMilestones: 'imdb/career_milestones',
} as const

// =============================================================================
// Parsing Utilities
// =============================================================================

/**
 * Parse IMDB null value
 */
export function parseImdbValue<T>(value: string, parser: (v: string) => T): T | null {
  if (value === '\\N' || value === '') {
    return null
  }
  return parser(value)
}

/**
 * Parse IMDB comma-separated array
 */
export function parseImdbArray(value: string): string[] {
  if (value === '\\N' || value === '') {
    return []
  }
  return value.split(',').map((s) => s.trim())
}

/**
 * Parse IMDB boolean (0/1)
 */
export function parseImdbBoolean(value: string): boolean {
  return value === '1'
}

/**
 * Parse IMDB integer
 */
export function parseImdbInt(value: string): number | null {
  if (value === '\\N' || value === '') {
    return null
  }
  const num = parseInt(value, 10)
  return isNaN(num) ? null : num
}

/**
 * Parse IMDB float
 */
export function parseImdbFloat(value: string): number | null {
  if (value === '\\N' || value === '') {
    return null
  }
  const num = parseFloat(value)
  return isNaN(num) ? null : num
}

/**
 * Parse IMDB JSON array (for characters field)
 */
export function parseImdbJsonArray(value: string): string[] {
  if (value === '\\N' || value === '') {
    return []
  }
  try {
    const parsed = JSON.parse(value)
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

/**
 * Parse title.basics row to Title entity
 */
export function parseTitleBasics(row: TitleBasicsRow): Title {
  return {
    id: row.tconst,
    type: row.titleType as TitleType,
    primaryTitle: row.primaryTitle,
    originalTitle: row.originalTitle,
    isAdult: parseImdbBoolean(row.isAdult),
    startYear: parseImdbInt(row.startYear),
    endYear: parseImdbInt(row.endYear),
    runtimeMinutes: parseImdbInt(row.runtimeMinutes),
    genres: parseImdbArray(row.genres),
  }
}

/**
 * Parse name.basics row to Person entity
 */
export function parseNameBasics(row: NameBasicsRow): Person {
  return {
    id: row.nconst,
    name: row.primaryName,
    birthYear: parseImdbInt(row.birthYear),
    deathYear: parseImdbInt(row.deathYear),
    professions: parseImdbArray(row.primaryProfession),
  }
}

/**
 * Parse title.ratings row to Rating
 */
export function parseTitleRatings(row: TitleRatingsRow): Rating {
  return {
    titleId: row.tconst,
    averageRating: parseFloat(row.averageRating),
    numVotes: parseInt(row.numVotes, 10),
  }
}

/**
 * Parse title.principals row to Principal
 */
export function parseTitlePrincipals(row: TitlePrincipalsRow): Principal {
  return {
    titleId: row.tconst,
    personId: row.nconst,
    ordering: parseInt(row.ordering, 10),
    category: row.category as PrincipalCategory,
    job: parseImdbValue(row.job, (v) => v),
    characters: parseImdbJsonArray(row.characters),
  }
}

/**
 * Parse title.episode row to Episode
 */
export function parseTitleEpisode(row: TitleEpisodeRow): Episode {
  return {
    episodeId: row.tconst,
    seriesId: row.parentTconst,
    seasonNumber: parseImdbInt(row.seasonNumber),
    episodeNumber: parseImdbInt(row.episodeNumber),
  }
}

/**
 * Parse title.akas row to AlternateTitle
 */
export function parseTitleAkas(row: TitleAkasRow): AlternateTitle {
  return {
    titleId: row.titleId,
    ordering: parseInt(row.ordering, 10),
    title: row.title,
    region: parseImdbValue(row.region, (v) => v),
    language: parseImdbValue(row.language, (v) => v),
    types: parseImdbArray(row.types),
    attributes: parseImdbArray(row.attributes),
    isOriginalTitle: parseImdbBoolean(row.isOriginalTitle),
  }
}

// =============================================================================
// Batch Sizes and Limits
// =============================================================================

/**
 * Configuration for streaming and batching
 */
export const IMDB_CONFIG = {
  /** Rows per Parquet row group */
  rowGroupSize: 100_000,

  /** Rows to buffer before writing */
  batchSize: 50_000,

  /** Maximum concurrent file operations */
  maxConcurrency: 4,

  /** Multipart upload part size (5MB minimum for R2) */
  partSize: 10 * 1024 * 1024, // 10MB

  /** Memory limit for buffering (256MB) */
  memoryLimit: 256 * 1024 * 1024,
} as const
