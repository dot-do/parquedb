/**
 * IMDB Example for ParqueDB
 *
 * Demonstrates loading and querying the IMDB dataset as a graph database.
 *
 * Features:
 * - Entity types: Title, Person, AlternateTitle, Rating, Genre, Keyword
 * - Relationships: acted_in, directed, wrote, episode_of, title links, collaborations
 * - Graph traversal: 1-hop, 2-hop, 3-hop queries
 * - Analytics: genre co-occurrence, career trajectories, breakthrough roles
 * - Benchmarks: cold start, cache warm, aggregation performance
 *
 * @example
 * ```typescript
 * import { loadAll, searchTitles, getFilmography } from './examples/imdb'
 *
 * // Load data
 * await loadAll(bucket, { prefix: 'imdb' })
 *
 * // Query
 * const movies = await searchTitles(bucket, 'inception')
 * const films = await getFilmography(bucket, 'nm0000138')
 *
 * // Find by localized title
 * const japanese = await findByAlternateTitle(bucket, 'JP', '千と千尋の神隠し')
 *
 * // Genre analysis
 * const cooccurrences = await getGenreCooccurrence(bucket)
 *
 * // Career analysis
 * const trajectory = await getCareerTrajectory(bucket, 'nm0000138')
 *
 * // Benchmarks
 * const results = await runBenchmarks(bucket)
 * console.log(formatBenchmarkReport(results))
 * ```
 */

// Schema types and utilities
export {
  // Entity types
  type Title,
  type Person,
  type Rating,
  type Episode,
  type Principal,
  type AlternateTitle,
  type Genre,
  type TitleGenre,
  type Keyword,
  type TitleKeyword,
  type TitleLink,
  type TitleLinkType,
  type PersonCollaboration,
  type PersonRelationType,
  type CareerMilestone,
  type TitleType,
  type Profession,
  type PrincipalCategory,

  // Raw row types
  type TitleBasicsRow,
  type TitleRatingsRow,
  type TitleCrewRow,
  type TitlePrincipalsRow,
  type TitleEpisodeRow,
  type NameBasicsRow,
  type TitleAkasRow,

  // Schema definition
  imdbSchema,

  // Storage paths
  IMDB_STORAGE_PATHS,
  IMDB_CONFIG,

  // Parsing utilities
  parseImdbValue,
  parseImdbArray,
  parseImdbBoolean,
  parseImdbInt,
  parseImdbFloat,
  parseImdbJsonArray,
  parseTitleBasics,
  parseNameBasics,
  parseTitleRatings,
  parseTitlePrincipals,
  parseTitleEpisode,
  parseTitleAkas,
} from './schema'

// Loader
export {
  // Dataset URLs
  IMDB_DATASETS,

  // Streaming utilities
  TSVParser,
  BatchAccumulator,
  MultipartUploadWriter,
  type TSVRow,

  // Main filesystem loader
  loadImdb,
  type LoadImdbOptions,
  type LoadImdbStats,
  type DatasetStats,

  // Legacy R2 bucket loaders (for backward compatibility)
  loadTitles,
  loadPeople,
  loadPrincipals,
  loadCrew,
  loadEpisodes,
  loadAlternateTitles,
  loadRatings,
  loadGenres,
  loadPersonCollaborations,
  loadCareerMilestones,

  // Legacy main loaders
  loadAll,
  loadExtended,

  // Types
  type LoaderOptions,
  type LoaderStats,
  type ProgressInfo,
} from './load'

// Queries
export {
  // Basic search
  searchTitles,
  searchPeople,
  getTitle,
  getPerson,
  getTopRated,

  // Graph traversal (1-hop)
  getFilmography,
  getCastAndCrew,

  // Graph traversal (2-hop)
  findCollaborations,
  findSimilarTitles,

  // Episodes
  getEpisodes,
  getSeriesForEpisode,

  // Alternate titles
  findByAlternateTitle,
  getAlternateTitles,

  // Genres
  getGenres,
  getGenreCooccurrence,
  getTitlesByGenre,

  // Ratings
  getRating,
  getRatingDistribution,

  // Director-Actor pairs
  findDirectorActorPairs,

  // Career analysis
  getCareerTrajectory,
  findBreakthroughRoles,

  // Awards (placeholder)
  getAwardWinners,

  // Analytics
  getGenreStats,
  getYearStats,

  // Benchmarks
  runBenchmarks,
  formatBenchmarkReport,

  // Types
  type SearchResult,
  type TitleSearchResult,
  type PersonSearchResult,
  type FilmographyEntry,
  type CastMember,
  type SearchOptions,
  type FilmographyOptions,
  type AlternateTitleResult,
  type GenreInfo,
  type GenreCooccurrence,
  type DirectorActorPair,
  type CareerPhase,
  type CareerTrajectory,
  type BreakthroughRole,
  type RatingInfo,
  type AwardWinner,
  type BenchmarkResult,
  type BenchmarkSuite,
} from './queries'
