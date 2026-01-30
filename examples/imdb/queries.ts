/**
 * IMDB Query Examples for ParqueDB
 *
 * Demonstrates querying the IMDB graph structure:
 * - Search titles by name, year, genre
 * - Find filmographies for people
 * - Traverse relationships (who directed/acted in what)
 * - Ratings-based queries
 */

import type { R2Bucket } from '../../src/storage/types/r2'
import type { Title, Person, TitleType } from './schema'

// =============================================================================
// Query Result Types
// =============================================================================

export interface SearchResult<T> {
  items: T[]
  total: number
  hasMore: boolean
  cursor?: string
}

export interface TitleSearchResult {
  id: string
  primaryTitle: string
  type: TitleType
  startYear: number | null
  genres: string[]
  averageRating: number | null
  numVotes: number | null
}

export interface PersonSearchResult {
  id: string
  name: string
  birthYear: number | null
  professions: string[]
}

export interface FilmographyEntry {
  titleId: string
  title: string
  year: number | null
  type: TitleType
  role: string // acted_in, directed, wrote, etc.
  characters?: string[]
  ordering?: number
}

export interface CastMember {
  personId: string
  name: string
  role: string
  characters: string[]
  ordering: number
}

// =============================================================================
// Query Options
// =============================================================================

export interface SearchOptions {
  /** Maximum results to return */
  limit?: number
  /** Pagination cursor */
  cursor?: string
  /** Filter by title type */
  type?: TitleType
  /** Filter by genre */
  genre?: string
  /** Filter by year range */
  yearFrom?: number
  yearTo?: number
  /** Minimum rating */
  minRating?: number
  /** Minimum votes */
  minVotes?: number
  /** Sort order */
  sort?: 'relevance' | 'rating' | 'votes' | 'year'
}

export interface FilmographyOptions {
  /** Filter by role type */
  role?: 'acted_in' | 'directed' | 'wrote' | 'produced' | 'all'
  /** Sort by year or rating */
  sort?: 'year' | 'rating'
  /** Limit results */
  limit?: number
}

// =============================================================================
// Parquet Query Interface (Stub)
// =============================================================================

/**
 * Query interface for reading Parquet files
 * In a real implementation, this would use hyparquet for reading
 */
interface ParquetReader {
  /** Read rows matching filter */
  scan(options: {
    predicates?: Array<{
      column: string
      op: 'eq' | 'gt' | 'gte' | 'lt' | 'lte' | 'contains' | 'in'
      value: unknown
    }>
    projection?: string[]
    limit?: number
    offset?: number
  }): AsyncIterable<Record<string, unknown>>
}

/**
 * Create a Parquet reader for files in a prefix
 */
async function createReader(bucket: R2Bucket, prefix: string): Promise<ParquetReader> {
  // In a real implementation, this would:
  // 1. List all Parquet files in the prefix
  // 2. Read file metadata for zone map filtering
  // 3. Return a reader that scans relevant files

  // Stub implementation for demonstration
  return {
    async *scan(options) {
      const { objects } = await bucket.list({ prefix })

      for (const obj of objects) {
        if (!obj.key.endsWith('.parquet')) continue

        const file = await bucket.get(obj.key)
        if (!file) continue

        const buffer = await file.arrayBuffer()

        // In reality, use hyparquet to read the file
        // const { parquetRead } = await import('hyparquet')
        // const rows = await parquetRead({ file: buffer, ... })

        // For demonstration, yield empty results
        // Actual implementation would filter and project rows
      }
    },
  }
}

// =============================================================================
// Title Queries
// =============================================================================

/**
 * Search titles by name
 *
 * @example
 * const results = await searchTitles(bucket, 'inception', {
 *   type: 'movie',
 *   yearFrom: 2000,
 *   minRating: 8.0
 * })
 */
export async function searchTitles(
  bucket: R2Bucket,
  query: string,
  options: SearchOptions = {}
): Promise<SearchResult<TitleSearchResult>> {
  const {
    limit = 20,
    cursor,
    type,
    genre,
    yearFrom,
    yearTo,
    minRating,
    minVotes,
    sort = 'relevance',
  } = options

  const results: TitleSearchResult[] = []
  const queryLower = query.toLowerCase()

  // Determine which type partitions to scan
  const typesToScan: TitleType[] = type
    ? [type]
    : ['movie', 'tvSeries', 'tvEpisode', 'short', 'tvMovie', 'video']

  for (const titleType of typesToScan) {
    const reader = await createReader(bucket, `imdb/titles/type=${titleType}`)

    for await (const row of reader.scan({
      projection: [
        'id',
        'primaryTitle',
        'type',
        'startYear',
        'genres',
        'averageRating',
        'numVotes',
      ],
      limit: limit * 10, // Over-fetch for filtering
    })) {
      const title = row as unknown as {
        id: string
        primaryTitle: string
        type: string
        startYear: number | null
        genres: string
        averageRating: number | null
        numVotes: number | null
      }

      // Text match on title
      if (!title.primaryTitle.toLowerCase().includes(queryLower)) {
        continue
      }

      // Apply filters
      if (yearFrom && title.startYear && title.startYear < yearFrom) continue
      if (yearTo && title.startYear && title.startYear > yearTo) continue
      if (minRating && (!title.averageRating || title.averageRating < minRating)) continue
      if (minVotes && (!title.numVotes || title.numVotes < minVotes)) continue

      const genres = JSON.parse(title.genres || '[]') as string[]
      if (genre && !genres.includes(genre)) continue

      results.push({
        id: title.id,
        primaryTitle: title.primaryTitle,
        type: title.type as TitleType,
        startYear: title.startYear,
        genres,
        averageRating: title.averageRating,
        numVotes: title.numVotes,
      })

      if (results.length >= limit * 2) break
    }

    if (results.length >= limit * 2) break
  }

  // Sort results
  switch (sort) {
    case 'rating':
      results.sort((a, b) => (b.averageRating ?? 0) - (a.averageRating ?? 0))
      break
    case 'votes':
      results.sort((a, b) => (b.numVotes ?? 0) - (a.numVotes ?? 0))
      break
    case 'year':
      results.sort((a, b) => (b.startYear ?? 0) - (a.startYear ?? 0))
      break
    case 'relevance':
    default:
      // Simple relevance: exact match > starts with > contains
      results.sort((a, b) => {
        const aLower = a.primaryTitle.toLowerCase()
        const bLower = b.primaryTitle.toLowerCase()

        const aExact = aLower === queryLower
        const bExact = bLower === queryLower
        if (aExact !== bExact) return aExact ? -1 : 1

        const aStarts = aLower.startsWith(queryLower)
        const bStarts = bLower.startsWith(queryLower)
        if (aStarts !== bStarts) return aStarts ? -1 : 1

        // Tie-break by rating
        return (b.averageRating ?? 0) - (a.averageRating ?? 0)
      })
  }

  const items = results.slice(0, limit)
  const hasMore = results.length > limit

  return {
    items,
    total: results.length,
    hasMore,
    cursor: hasMore ? items[items.length - 1]?.id : undefined,
  }
}

/**
 * Get a title by ID
 */
export async function getTitle(
  bucket: R2Bucket,
  titleId: string
): Promise<TitleSearchResult | null> {
  // Title ID format: tt0000001
  // We need to scan the appropriate type partition

  const types: TitleType[] = [
    'movie',
    'tvSeries',
    'tvEpisode',
    'short',
    'tvMovie',
    'video',
    'tvMiniSeries',
    'tvSpecial',
    'tvShort',
    'videoGame',
  ]

  for (const type of types) {
    const reader = await createReader(bucket, `imdb/titles/type=${type}`)

    for await (const row of reader.scan({
      predicates: [{ column: 'id', op: 'eq', value: titleId }],
      limit: 1,
    })) {
      const title = row as unknown as {
        id: string
        primaryTitle: string
        type: string
        startYear: number | null
        genres: string
        averageRating: number | null
        numVotes: number | null
      }

      return {
        id: title.id,
        primaryTitle: title.primaryTitle,
        type: title.type as TitleType,
        startYear: title.startYear,
        genres: JSON.parse(title.genres || '[]'),
        averageRating: title.averageRating,
        numVotes: title.numVotes,
      }
    }
  }

  return null
}

/**
 * Get top-rated titles
 */
export async function getTopRated(
  bucket: R2Bucket,
  options: {
    type?: TitleType
    genre?: string
    minVotes?: number
    limit?: number
  } = {}
): Promise<TitleSearchResult[]> {
  const { type, genre, minVotes = 10000, limit = 100 } = options

  const results: TitleSearchResult[] = []
  const types: TitleType[] = type
    ? [type]
    : ['movie', 'tvSeries', 'tvMiniSeries']

  for (const titleType of types) {
    const reader = await createReader(bucket, `imdb/titles/type=${titleType}`)

    for await (const row of reader.scan({
      predicates: [{ column: 'numVotes', op: 'gte', value: minVotes }],
      projection: [
        'id',
        'primaryTitle',
        'type',
        'startYear',
        'genres',
        'averageRating',
        'numVotes',
      ],
    })) {
      const title = row as unknown as {
        id: string
        primaryTitle: string
        type: string
        startYear: number | null
        genres: string
        averageRating: number | null
        numVotes: number | null
      }

      const genres = JSON.parse(title.genres || '[]') as string[]
      if (genre && !genres.includes(genre)) continue

      results.push({
        id: title.id,
        primaryTitle: title.primaryTitle,
        type: title.type as TitleType,
        startYear: title.startYear,
        genres,
        averageRating: title.averageRating,
        numVotes: title.numVotes,
      })
    }
  }

  // Sort by rating descending
  results.sort((a, b) => (b.averageRating ?? 0) - (a.averageRating ?? 0))

  return results.slice(0, limit)
}

// =============================================================================
// Person Queries
// =============================================================================

/**
 * Search people by name
 */
export async function searchPeople(
  bucket: R2Bucket,
  query: string,
  options: { limit?: number; profession?: string } = {}
): Promise<SearchResult<PersonSearchResult>> {
  const { limit = 20, profession } = options

  const results: PersonSearchResult[] = []
  const queryLower = query.toLowerCase()

  const reader = await createReader(bucket, 'imdb/people')

  for await (const row of reader.scan({
    projection: ['id', 'name', 'birthYear', 'professions'],
    limit: limit * 10,
  })) {
    const person = row as unknown as {
      id: string
      name: string
      birthYear: number | null
      professions: string
    }

    if (!person.name.toLowerCase().includes(queryLower)) {
      continue
    }

    const professions = JSON.parse(person.professions || '[]') as string[]
    if (profession && !professions.includes(profession)) continue

    results.push({
      id: person.id,
      name: person.name,
      birthYear: person.birthYear,
      professions,
    })

    if (results.length >= limit * 2) break
  }

  // Sort by relevance (exact match > starts with > contains)
  results.sort((a, b) => {
    const aLower = a.name.toLowerCase()
    const bLower = b.name.toLowerCase()

    const aExact = aLower === queryLower
    const bExact = bLower === queryLower
    if (aExact !== bExact) return aExact ? -1 : 1

    const aStarts = aLower.startsWith(queryLower)
    const bStarts = bLower.startsWith(queryLower)
    if (aStarts !== bStarts) return aStarts ? -1 : 1

    return 0
  })

  const items = results.slice(0, limit)
  const hasMore = results.length > limit

  return {
    items,
    total: results.length,
    hasMore,
    cursor: hasMore ? items[items.length - 1]?.id : undefined,
  }
}

/**
 * Get a person by ID
 */
export async function getPerson(bucket: R2Bucket, personId: string): Promise<PersonSearchResult | null> {
  const reader = await createReader(bucket, 'imdb/people')

  for await (const row of reader.scan({
    predicates: [{ column: 'id', op: 'eq', value: personId }],
    limit: 1,
  })) {
    const person = row as unknown as {
      id: string
      name: string
      birthYear: number | null
      professions: string
    }

    return {
      id: person.id,
      name: person.name,
      birthYear: person.birthYear,
      professions: JSON.parse(person.professions || '[]'),
    }
  }

  return null
}

// =============================================================================
// Filmography Queries (Graph Traversal)
// =============================================================================

/**
 * Get a person's filmography
 *
 * Traverses the relationship graph: Person -> [acted_in, directed, wrote] -> Title
 *
 * @example
 * const filmography = await getFilmography(bucket, 'nm0000138', {
 *   role: 'directed',
 *   sort: 'rating'
 * })
 */
export async function getFilmography(
  bucket: R2Bucket,
  personId: string,
  options: FilmographyOptions = {}
): Promise<FilmographyEntry[]> {
  const { role = 'all', sort = 'year', limit = 100 } = options

  const entries: FilmographyEntry[] = []

  // Determine which relationship partitions to scan
  const roles =
    role === 'all'
      ? ['acted_in', 'directed', 'wrote', 'produced', 'composed', 'edited']
      : [role]

  for (const relType of roles) {
    const reader = await createReader(bucket, `imdb/relationships/${relType}`)

    for await (const row of reader.scan({
      predicates: [{ column: 'from_id', op: 'eq', value: personId }],
    })) {
      const rel = row as unknown as {
        from_id: string
        to_id: string
        rel_type: string
        ordering: number
        job: string | null
        characters: string
      }

      // Look up the title details
      const title = await getTitle(bucket, rel.to_id)
      if (!title) continue

      entries.push({
        titleId: rel.to_id,
        title: title.primaryTitle,
        year: title.startYear,
        type: title.type,
        role: rel.rel_type,
        characters: JSON.parse(rel.characters || '[]'),
        ordering: rel.ordering,
      })
    }
  }

  // Sort results
  switch (sort) {
    case 'rating':
      // Would need to fetch ratings for each title
      // For now, sort by year as fallback
      entries.sort((a, b) => (b.year ?? 0) - (a.year ?? 0))
      break
    case 'year':
    default:
      entries.sort((a, b) => (b.year ?? 0) - (a.year ?? 0))
  }

  return entries.slice(0, limit)
}

/**
 * Get cast and crew for a title
 *
 * Traverses: Title <- [acted_in, directed, wrote] <- Person
 */
export async function getCastAndCrew(
  bucket: R2Bucket,
  titleId: string,
  options: { role?: string; limit?: number } = {}
): Promise<CastMember[]> {
  const { role, limit = 50 } = options

  const members: CastMember[] = []

  const roles = role
    ? [role]
    : ['acted_in', 'directed', 'wrote', 'produced']

  for (const relType of roles) {
    const reader = await createReader(bucket, `imdb/relationships/${relType}`)

    for await (const row of reader.scan({
      predicates: [{ column: 'to_id', op: 'eq', value: titleId }],
    })) {
      const rel = row as unknown as {
        from_id: string
        to_id: string
        rel_type: string
        ordering: number
        characters: string
      }

      // Look up person details
      const person = await getPerson(bucket, rel.from_id)
      if (!person) continue

      members.push({
        personId: rel.from_id,
        name: person.name,
        role: rel.rel_type,
        characters: JSON.parse(rel.characters || '[]'),
        ordering: rel.ordering,
      })
    }
  }

  // Sort by ordering (billing order)
  members.sort((a, b) => a.ordering - b.ordering)

  return members.slice(0, limit)
}

// =============================================================================
// Episode Queries
// =============================================================================

/**
 * Get episodes for a TV series
 */
export async function getEpisodes(
  bucket: R2Bucket,
  seriesId: string,
  options: { season?: number; limit?: number } = {}
): Promise<
  Array<{
    id: string
    title: string
    seasonNumber: number | null
    episodeNumber: number | null
    rating: number | null
  }>
> {
  const { season, limit = 100 } = options

  const episodes: Array<{
    id: string
    title: string
    seasonNumber: number | null
    episodeNumber: number | null
    rating: number | null
  }> = []

  const reader = await createReader(bucket, 'imdb/episodes')

  for await (const row of reader.scan({
    predicates: [{ column: 'to_id', op: 'eq', value: seriesId }],
  })) {
    const ep = row as unknown as {
      from_id: string
      to_id: string
      seasonNumber: number | null
      episodeNumber: number | null
    }

    if (season !== undefined && ep.seasonNumber !== season) continue

    // Look up episode title details
    const title = await getTitle(bucket, ep.from_id)
    if (!title) continue

    episodes.push({
      id: ep.from_id,
      title: title.primaryTitle,
      seasonNumber: ep.seasonNumber,
      episodeNumber: ep.episodeNumber,
      rating: title.averageRating,
    })
  }

  // Sort by season and episode number
  episodes.sort((a, b) => {
    const seasonDiff = (a.seasonNumber ?? 0) - (b.seasonNumber ?? 0)
    if (seasonDiff !== 0) return seasonDiff
    return (a.episodeNumber ?? 0) - (b.episodeNumber ?? 0)
  })

  return episodes.slice(0, limit)
}

/**
 * Get series for an episode
 */
export async function getSeriesForEpisode(
  bucket: R2Bucket,
  episodeId: string
): Promise<TitleSearchResult | null> {
  const reader = await createReader(bucket, 'imdb/episodes')

  for await (const row of reader.scan({
    predicates: [{ column: 'from_id', op: 'eq', value: episodeId }],
    limit: 1,
  })) {
    const ep = row as unknown as { to_id: string }
    return getTitle(bucket, ep.to_id)
  }

  return null
}

// =============================================================================
// Analytics Queries
// =============================================================================

/**
 * Get genre statistics
 */
export async function getGenreStats(
  bucket: R2Bucket,
  options: { type?: TitleType } = {}
): Promise<Array<{ genre: string; count: number; avgRating: number }>> {
  const { type = 'movie' } = options

  const stats = new Map<string, { count: number; ratingSum: number; ratedCount: number }>()

  const reader = await createReader(bucket, `imdb/titles/type=${type}`)

  for await (const row of reader.scan({
    projection: ['genres', 'averageRating'],
  })) {
    const title = row as unknown as {
      genres: string
      averageRating: number | null
    }

    const genres = JSON.parse(title.genres || '[]') as string[]

    for (const genre of genres) {
      const current = stats.get(genre) || { count: 0, ratingSum: 0, ratedCount: 0 }
      current.count++
      if (title.averageRating) {
        current.ratingSum += title.averageRating
        current.ratedCount++
      }
      stats.set(genre, current)
    }
  }

  const results = Array.from(stats.entries()).map(([genre, data]) => ({
    genre,
    count: data.count,
    avgRating: data.ratedCount > 0 ? data.ratingSum / data.ratedCount : 0,
  }))

  // Sort by count descending
  results.sort((a, b) => b.count - a.count)

  return results
}

/**
 * Get year statistics
 */
export async function getYearStats(
  bucket: R2Bucket,
  options: { type?: TitleType; yearFrom?: number; yearTo?: number } = {}
): Promise<Array<{ year: number; count: number; avgRating: number }>> {
  const { type = 'movie', yearFrom = 1900, yearTo = new Date().getFullYear() } = options

  const stats = new Map<number, { count: number; ratingSum: number; ratedCount: number }>()

  const reader = await createReader(bucket, `imdb/titles/type=${type}`)

  for await (const row of reader.scan({
    predicates: [
      { column: 'startYear', op: 'gte', value: yearFrom },
      { column: 'startYear', op: 'lte', value: yearTo },
    ],
    projection: ['startYear', 'averageRating'],
  })) {
    const title = row as unknown as {
      startYear: number | null
      averageRating: number | null
    }

    if (!title.startYear) continue

    const current = stats.get(title.startYear) || { count: 0, ratingSum: 0, ratedCount: 0 }
    current.count++
    if (title.averageRating) {
      current.ratingSum += title.averageRating
      current.ratedCount++
    }
    stats.set(title.startYear, current)
  }

  const results = Array.from(stats.entries()).map(([year, data]) => ({
    year,
    count: data.count,
    avgRating: data.ratedCount > 0 ? data.ratingSum / data.ratedCount : 0,
  }))

  // Sort by year
  results.sort((a, b) => a.year - b.year)

  return results
}

// =============================================================================
// Co-occurrence Queries (2-hop traversals)
// =============================================================================

/**
 * Find common collaborators between two people
 *
 * Traverses: Person1 -> Title <- Person2
 */
export async function findCollaborations(
  bucket: R2Bucket,
  personId1: string,
  personId2: string
): Promise<
  Array<{
    titleId: string
    title: string
    year: number | null
    person1Role: string
    person2Role: string
  }>
> {
  // Get filmography for both people
  const [filmography1, filmography2] = await Promise.all([
    getFilmography(bucket, personId1, { limit: 500 }),
    getFilmography(bucket, personId2, { limit: 500 }),
  ])

  // Find common titles
  const titles1 = new Map(filmography1.map((f) => [f.titleId, f]))
  const collaborations: Array<{
    titleId: string
    title: string
    year: number | null
    person1Role: string
    person2Role: string
  }> = []

  for (const entry2 of filmography2) {
    const entry1 = titles1.get(entry2.titleId)
    if (entry1) {
      collaborations.push({
        titleId: entry1.titleId,
        title: entry1.title,
        year: entry1.year,
        person1Role: entry1.role,
        person2Role: entry2.role,
      })
    }
  }

  // Sort by year descending
  collaborations.sort((a, b) => (b.year ?? 0) - (a.year ?? 0))

  return collaborations
}

/**
 * Find similar titles based on shared cast/crew
 *
 * Traverses: Title1 <- Person -> Title2
 */
export async function findSimilarTitles(
  bucket: R2Bucket,
  titleId: string,
  options: { limit?: number } = {}
): Promise<
  Array<{
    titleId: string
    title: string
    year: number | null
    sharedPeople: number
    type: TitleType
  }>
> {
  const { limit = 20 } = options

  // Get cast and crew for the source title
  const castCrew = await getCastAndCrew(bucket, titleId, { limit: 50 })

  // Count title co-occurrences
  const titleCounts = new Map<string, number>()

  for (const member of castCrew) {
    const filmography = await getFilmography(bucket, member.personId, { limit: 100 })

    for (const entry of filmography) {
      if (entry.titleId === titleId) continue

      const count = titleCounts.get(entry.titleId) || 0
      titleCounts.set(entry.titleId, count + 1)
    }
  }

  // Get top similar titles
  const sortedTitles = Array.from(titleCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)

  const results: Array<{
    titleId: string
    title: string
    year: number | null
    sharedPeople: number
    type: TitleType
  }> = []

  for (const [tid, count] of sortedTitles) {
    const title = await getTitle(bucket, tid)
    if (title) {
      results.push({
        titleId: tid,
        title: title.primaryTitle,
        year: title.startYear,
        sharedPeople: count,
        type: title.type,
      })
    }
  }

  return results
}

// =============================================================================
// Alternate Title Queries
// =============================================================================

export interface AlternateTitleResult {
  titleId: string
  localizedTitle: string
  region: string | null
  language: string | null
  types: string[]
  isOriginalTitle: boolean
}

/**
 * Find titles by alternate/localized name
 *
 * @example
 * // Find Japanese title
 * const results = await findByAlternateTitle(bucket, 'JP', '千と千尋の神隠し')
 *
 * // Find by any region
 * const results = await findByAlternateTitle(bucket, null, 'Spirited Away')
 */
export async function findByAlternateTitle(
  bucket: R2Bucket,
  region: string | null,
  titleQuery: string,
  options: { limit?: number } = {}
): Promise<Array<TitleSearchResult & { alternateTitles: AlternateTitleResult[] }>> {
  const { limit = 20 } = options

  const results: Array<TitleSearchResult & { alternateTitles: AlternateTitleResult[] }> = []
  const queryLower = titleQuery.toLowerCase()

  // Determine which region partitions to scan
  const prefix = region
    ? `imdb/alternate_titles/region=${region}`
    : 'imdb/alternate_titles'

  const reader = await createReader(bucket, prefix)

  const matchedTitleIds = new Map<string, AlternateTitleResult[]>()

  for await (const row of reader.scan({
    projection: ['titleId', 'title', 'region', 'language', 'types', 'isOriginalTitle'],
    limit: limit * 50, // Over-fetch for filtering
  })) {
    const aka = row as unknown as {
      titleId: string
      title: string
      region: string | null
      language: string | null
      types: string
      isOriginalTitle: boolean
    }

    if (!aka.title.toLowerCase().includes(queryLower)) {
      continue
    }

    const result: AlternateTitleResult = {
      titleId: aka.titleId,
      localizedTitle: aka.title,
      region: aka.region,
      language: aka.language,
      types: JSON.parse(aka.types || '[]'),
      isOriginalTitle: aka.isOriginalTitle,
    }

    if (!matchedTitleIds.has(aka.titleId)) {
      matchedTitleIds.set(aka.titleId, [])
    }
    matchedTitleIds.get(aka.titleId)!.push(result)

    if (matchedTitleIds.size >= limit * 2) break
  }

  // Look up the actual titles
  for (const [titleId, alternateTitles] of Array.from(matchedTitleIds.entries()).slice(0, limit)) {
    const title = await getTitle(bucket, titleId)
    if (title) {
      results.push({
        ...title,
        alternateTitles,
      })
    }
  }

  return results
}

/**
 * Get all alternate titles for a specific title
 */
export async function getAlternateTitles(
  bucket: R2Bucket,
  titleId: string,
  options: { region?: string } = {}
): Promise<AlternateTitleResult[]> {
  const { region } = options

  const prefix = region
    ? `imdb/alternate_titles/region=${region}`
    : 'imdb/alternate_titles'

  const reader = await createReader(bucket, prefix)
  const results: AlternateTitleResult[] = []

  for await (const row of reader.scan({
    predicates: [{ column: 'titleId', op: 'eq', value: titleId }],
  })) {
    const aka = row as unknown as {
      titleId: string
      title: string
      region: string | null
      language: string | null
      types: string
      isOriginalTitle: boolean
    }

    results.push({
      titleId: aka.titleId,
      localizedTitle: aka.title,
      region: aka.region,
      language: aka.language,
      types: JSON.parse(aka.types || '[]'),
      isOriginalTitle: aka.isOriginalTitle,
    })
  }

  return results
}

// =============================================================================
// Genre Queries
// =============================================================================

export interface GenreInfo {
  id: string
  name: string
  titleCount: number
  averageRating: number | null
}

export interface GenreCooccurrence {
  genre1: string
  genre2: string
  count: number
  percentage: number // percentage of genre1 titles that also have genre2
}

/**
 * Get all genres with statistics
 */
export async function getGenres(bucket: R2Bucket): Promise<GenreInfo[]> {
  const reader = await createReader(bucket, 'imdb/genres')
  const results: GenreInfo[] = []

  for await (const row of reader.scan({})) {
    const genre = row as unknown as {
      id: string
      name: string
      titleCount: number
      averageRating: number | null
    }

    results.push({
      id: genre.id,
      name: genre.name,
      titleCount: genre.titleCount,
      averageRating: genre.averageRating,
    })
  }

  // Sort by title count descending
  results.sort((a, b) => b.titleCount - a.titleCount)

  return results
}

/**
 * Get genre co-occurrence statistics
 * Which genres appear together most frequently?
 *
 * @example
 * const cooccurrences = await getGenreCooccurrence(bucket, {
 *   minCount: 100,
 *   limit: 50
 * })
 * // Returns pairs like { genre1: 'Action', genre2: 'Adventure', count: 5000, percentage: 45 }
 */
export async function getGenreCooccurrence(
  bucket: R2Bucket,
  options: { type?: TitleType; minCount?: number; limit?: number } = {}
): Promise<GenreCooccurrence[]> {
  const { type = 'movie', minCount = 100, limit = 100 } = options

  // Count genre pairs
  const pairCounts = new Map<string, number>()
  const genreCounts = new Map<string, number>()

  const reader = await createReader(bucket, `imdb/titles/type=${type}`)

  for await (const row of reader.scan({
    projection: ['genres'],
  })) {
    const title = row as unknown as { genres: string }
    const genres = JSON.parse(title.genres || '[]') as string[]

    // Count individual genres
    for (const genre of genres) {
      genreCounts.set(genre, (genreCounts.get(genre) || 0) + 1)
    }

    // Count pairs
    for (let i = 0; i < genres.length; i++) {
      for (let j = i + 1; j < genres.length; j++) {
        // Consistent ordering
        const [g1, g2] = genres[i] < genres[j] ? [genres[i], genres[j]] : [genres[j], genres[i]]
        const key = `${g1}:${g2}`
        pairCounts.set(key, (pairCounts.get(key) || 0) + 1)
      }
    }
  }

  // Convert to results
  const results: GenreCooccurrence[] = []

  for (const [key, count] of Array.from(pairCounts.entries())) {
    if (count < minCount) continue

    const [genre1, genre2] = key.split(':')
    const genre1Count = genreCounts.get(genre1) || 1
    const genre2Count = genreCounts.get(genre2) || 1

    // Use the smaller genre as the base for percentage
    const baseCount = Math.min(genre1Count, genre2Count)
    const percentage = Math.round((count / baseCount) * 100)

    results.push({
      genre1,
      genre2,
      count,
      percentage,
    })
  }

  // Sort by count descending
  results.sort((a, b) => b.count - a.count)

  return results.slice(0, limit)
}

/**
 * Get titles for a specific genre
 */
export async function getTitlesByGenre(
  bucket: R2Bucket,
  genreId: string,
  options: SearchOptions = {}
): Promise<SearchResult<TitleSearchResult>> {
  const {
    limit = 20,
    type,
    minRating,
    minVotes,
    sort = 'rating',
  } = options

  const results: TitleSearchResult[] = []
  const types: TitleType[] = type
    ? [type]
    : ['movie', 'tvSeries', 'tvMiniSeries']

  for (const titleType of types) {
    const reader = await createReader(bucket, `imdb/titles/type=${titleType}`)

    for await (const row of reader.scan({
      projection: [
        'id',
        'primaryTitle',
        'type',
        'startYear',
        'genres',
        'averageRating',
        'numVotes',
      ],
    })) {
      const title = row as unknown as {
        id: string
        primaryTitle: string
        type: string
        startYear: number | null
        genres: string
        averageRating: number | null
        numVotes: number | null
      }

      const genres = JSON.parse(title.genres || '[]') as string[]
      const normalizedGenre = genreId.toLowerCase().replace(/[^a-z0-9]/g, '_')

      // Check if title has this genre
      const hasGenre = genres.some(
        (g) => g.toLowerCase().replace(/[^a-z0-9]/g, '_') === normalizedGenre
      )
      if (!hasGenre) continue

      if (minRating && (!title.averageRating || title.averageRating < minRating)) continue
      if (minVotes && (!title.numVotes || title.numVotes < minVotes)) continue

      results.push({
        id: title.id,
        primaryTitle: title.primaryTitle,
        type: title.type as TitleType,
        startYear: title.startYear,
        genres,
        averageRating: title.averageRating,
        numVotes: title.numVotes,
      })

      if (results.length >= limit * 5) break
    }

    if (results.length >= limit * 5) break
  }

  // Sort results
  switch (sort) {
    case 'rating':
      results.sort((a, b) => (b.averageRating ?? 0) - (a.averageRating ?? 0))
      break
    case 'votes':
      results.sort((a, b) => (b.numVotes ?? 0) - (a.numVotes ?? 0))
      break
    case 'year':
      results.sort((a, b) => (b.startYear ?? 0) - (a.startYear ?? 0))
      break
  }

  const items = results.slice(0, limit)
  const hasMore = results.length > limit

  return {
    items,
    total: results.length,
    hasMore,
    cursor: hasMore ? items[items.length - 1]?.id : undefined,
  }
}

// =============================================================================
// Director-Actor Pair Queries
// =============================================================================

export interface DirectorActorPair {
  directorId: string
  directorName: string
  actorId: string
  actorName: string
  collaborationCount: number
  titles: Array<{
    titleId: string
    title: string
    year: number | null
    rating: number | null
  }>
}

/**
 * Find directors who frequently work with the same actors
 *
 * @example
 * const pairs = await findDirectorActorPairs(bucket, {
 *   minCollaborations: 5,
 *   limit: 50
 * })
 */
export async function findDirectorActorPairs(
  bucket: R2Bucket,
  options: { minCollaborations?: number; limit?: number } = {}
): Promise<DirectorActorPair[]> {
  const { minCollaborations = 5, limit = 50 } = options

  // Use pre-computed collaborations if available
  const reader = await createReader(bucket, 'imdb/person_collaborations')

  // We need to filter for director-actor pairs
  // This requires looking up person professions
  const directorActorPairs = new Map<string, {
    directorId: string
    actorId: string
    sharedTitles: string[]
    count: number
  }>()

  // First, load person professions
  const personProfessions = new Map<string, string[]>()
  const personNames = new Map<string, string>()

  const peopleReader = await createReader(bucket, 'imdb/people')
  for await (const row of peopleReader.scan({
    projection: ['id', 'name', 'professions'],
  })) {
    const person = row as unknown as { id: string; name: string; professions: string }
    personProfessions.set(person.id, JSON.parse(person.professions || '[]'))
    personNames.set(person.id, person.name)
  }

  // Scan collaborations
  for await (const row of reader.scan({
    predicates: [{ column: 'sharedTitleCount', op: 'gte', value: minCollaborations }],
  })) {
    const collab = row as unknown as {
      person1Id: string
      person2Id: string
      sharedTitleCount: number
      sharedTitleIds: string
    }

    const prof1 = personProfessions.get(collab.person1Id) || []
    const prof2 = personProfessions.get(collab.person2Id) || []

    let directorId: string | null = null
    let actorId: string | null = null

    // Check if one is director and other is actor
    if (prof1.includes('director') && (prof2.includes('actor') || prof2.includes('actress'))) {
      directorId = collab.person1Id
      actorId = collab.person2Id
    } else if (prof2.includes('director') && (prof1.includes('actor') || prof1.includes('actress'))) {
      directorId = collab.person2Id
      actorId = collab.person1Id
    }

    if (directorId && actorId) {
      const key = `${directorId}:${actorId}`
      directorActorPairs.set(key, {
        directorId,
        actorId,
        sharedTitles: JSON.parse(collab.sharedTitleIds || '[]'),
        count: collab.sharedTitleCount,
      })
    }
  }

  // Sort by count and take top results
  const sortedPairs = Array.from(directorActorPairs.values())
    .sort((a, b) => b.count - a.count)
    .slice(0, limit)

  // Fetch title details
  const results: DirectorActorPair[] = []

  for (const pair of sortedPairs) {
    const titles: Array<{
      titleId: string
      title: string
      year: number | null
      rating: number | null
    }> = []

    for (const titleId of pair.sharedTitles.slice(0, 10)) {
      const title = await getTitle(bucket, titleId)
      if (title) {
        titles.push({
          titleId,
          title: title.primaryTitle,
          year: title.startYear,
          rating: title.averageRating,
        })
      }
    }

    results.push({
      directorId: pair.directorId,
      directorName: personNames.get(pair.directorId) || 'Unknown',
      actorId: pair.actorId,
      actorName: personNames.get(pair.actorId) || 'Unknown',
      collaborationCount: pair.count,
      titles,
    })
  }

  return results
}

// =============================================================================
// Career Trajectory Queries
// =============================================================================

export interface CareerPhase {
  startYear: number
  endYear: number
  phase: 'early' | 'rising' | 'peak' | 'established' | 'later'
  titleCount: number
  avgRating: number | null
  topRole: string
  representativeTitle?: {
    titleId: string
    title: string
    rating: number | null
  }
}

export interface CareerTrajectory {
  personId: string
  name: string
  careerStart: number
  careerEnd: number | null
  totalCredits: number
  phases: CareerPhase[]
  milestones: Array<{
    year: number
    type: string
    titleId: string
    title: string
    rating: number | null
  }>
}

/**
 * Get a person's career trajectory over time
 *
 * @example
 * const trajectory = await getCareerTrajectory(bucket, 'nm0000138')
 * // Returns career phases, milestones, and statistics
 */
export async function getCareerTrajectory(
  bucket: R2Bucket,
  personId: string
): Promise<CareerTrajectory | null> {
  // Get person info
  const person = await getPerson(bucket, personId)
  if (!person) return null

  // Get filmography
  const filmography = await getFilmography(bucket, personId, { limit: 500 })
  if (filmography.length === 0) {
    return {
      personId,
      name: person.name,
      careerStart: 0,
      careerEnd: null,
      totalCredits: 0,
      phases: [],
      milestones: [],
    }
  }

  // Sort by year
  const withYears = filmography.filter((f) => f.year !== null) as Array<FilmographyEntry & { year: number }>
  withYears.sort((a, b) => a.year - b.year)

  if (withYears.length === 0) {
    return {
      personId,
      name: person.name,
      careerStart: 0,
      careerEnd: null,
      totalCredits: filmography.length,
      phases: [],
      milestones: [],
    }
  }

  const careerStart = withYears[0].year
  const careerEnd = withYears[withYears.length - 1].year

  // Divide career into phases (roughly 5-10 year spans)
  const careerLength = careerEnd - careerStart
  const phaseLength = Math.max(5, Math.ceil(careerLength / 5))

  const phases: CareerPhase[] = []
  const phaseNames: Array<'early' | 'rising' | 'peak' | 'established' | 'later'> = [
    'early', 'rising', 'peak', 'established', 'later',
  ]

  for (let i = 0; i < 5; i++) {
    const phaseStart = careerStart + i * phaseLength
    const phaseEnd = Math.min(careerStart + (i + 1) * phaseLength - 1, careerEnd)

    if (phaseStart > careerEnd) break

    const phaseEntries = withYears.filter((f) => f.year >= phaseStart && f.year <= phaseEnd)

    if (phaseEntries.length === 0) continue

    // Get rating info for representative title
    const withDetails = await Promise.all(
      phaseEntries.slice(0, 10).map(async (e) => {
        const title = await getTitle(bucket, e.titleId)
        return { ...e, rating: title?.averageRating || null }
      })
    )

    const avgRating =
      withDetails.filter((e) => e.rating !== null).length > 0
        ? withDetails
            .filter((e) => e.rating !== null)
            .reduce((sum, e) => sum + (e.rating || 0), 0) /
          withDetails.filter((e) => e.rating !== null).length
        : null

    // Find most common role
    const roleCounts = new Map<string, number>()
    for (const entry of phaseEntries) {
      roleCounts.set(entry.role, (roleCounts.get(entry.role) || 0) + 1)
    }
    const topRole = Array.from(roleCounts.entries()).sort((a, b) => b[1] - a[1])[0]?.[0] || 'unknown'

    // Find representative title (highest rated in phase)
    const representativeEntry = withDetails
      .filter((e) => e.rating !== null)
      .sort((a, b) => (b.rating || 0) - (a.rating || 0))[0]

    phases.push({
      startYear: phaseStart,
      endYear: phaseEnd,
      phase: phaseNames[i],
      titleCount: phaseEntries.length,
      avgRating,
      topRole,
      representativeTitle: representativeEntry
        ? {
            titleId: representativeEntry.titleId,
            title: representativeEntry.title,
            rating: representativeEntry.rating,
          }
        : undefined,
    })
  }

  // Get career milestones
  const milestonesReader = await createReader(bucket, 'imdb/career_milestones')
  const milestones: Array<{
    year: number
    type: string
    titleId: string
    title: string
    rating: number | null
  }> = []

  for await (const row of milestonesReader.scan({
    predicates: [{ column: 'personId', op: 'eq', value: personId }],
  })) {
    const milestone = row as unknown as {
      personId: string
      titleId: string
      year: number
      milestoneType: string
      role: string
      rating: number | null
    }

    const title = await getTitle(bucket, milestone.titleId)

    milestones.push({
      year: milestone.year,
      type: milestone.milestoneType,
      titleId: milestone.titleId,
      title: title?.primaryTitle || 'Unknown',
      rating: milestone.rating,
    })
  }

  return {
    personId,
    name: person.name,
    careerStart,
    careerEnd,
    totalCredits: filmography.length,
    phases,
    milestones,
  }
}

// =============================================================================
// Breakthrough Roles Query
// =============================================================================

export interface BreakthroughRole {
  personId: string
  personName: string
  titleId: string
  titleName: string
  year: number
  role: string
  rating: number | null
  votes: number | null
  careerStart: number
  yearsToBreakthrough: number
}

/**
 * Find breakthrough roles for actors
 * A breakthrough is defined as the first high-profile role (high votes, good rating)
 *
 * @example
 * const breakthroughs = await findBreakthroughRoles(bucket, {
 *   yearFrom: 2010,
 *   yearTo: 2020,
 *   limit: 50
 * })
 */
export async function findBreakthroughRoles(
  bucket: R2Bucket,
  options: {
    yearFrom?: number
    yearTo?: number
    minVotes?: number
    minRating?: number
    limit?: number
  } = {}
): Promise<BreakthroughRole[]> {
  const {
    yearFrom = 1990,
    yearTo = new Date().getFullYear(),
    minVotes = 10000,
    minRating = 7.0,
    limit = 100,
  } = options

  const reader = await createReader(bucket, 'imdb/career_milestones')
  const results: BreakthroughRole[] = []

  // Also load person names
  const personNames = new Map<string, string>()
  const peopleReader = await createReader(bucket, 'imdb/people')
  for await (const row of peopleReader.scan({
    projection: ['id', 'name'],
    limit: 1000000,
  })) {
    const person = row as unknown as { id: string; name: string }
    personNames.set(person.id, person.name)
  }

  for await (const row of reader.scan({
    predicates: [
      { column: 'milestoneType', op: 'eq', value: 'breakthrough' },
      { column: 'year', op: 'gte', value: yearFrom },
      { column: 'year', op: 'lte', value: yearTo },
    ],
  })) {
    const milestone = row as unknown as {
      personId: string
      titleId: string
      year: number
      milestoneType: string
      role: string
      rating: number | null
      votes: number | null
    }

    if (milestone.votes && milestone.votes < minVotes) continue
    if (milestone.rating && milestone.rating < minRating) continue

    const title = await getTitle(bucket, milestone.titleId)
    const personName = personNames.get(milestone.personId)

    // Get first credit to calculate years to breakthrough
    let careerStart = milestone.year
    for await (const firstRow of reader.scan({
      predicates: [
        { column: 'personId', op: 'eq', value: milestone.personId },
        { column: 'milestoneType', op: 'eq', value: 'first_credit' },
      ],
      limit: 1,
    })) {
      const first = firstRow as unknown as { year: number }
      careerStart = first.year
    }

    results.push({
      personId: milestone.personId,
      personName: personName || 'Unknown',
      titleId: milestone.titleId,
      titleName: title?.primaryTitle || 'Unknown',
      year: milestone.year,
      role: milestone.role,
      rating: milestone.rating,
      votes: milestone.votes,
      careerStart,
      yearsToBreakthrough: milestone.year - careerStart,
    })

    if (results.length >= limit) break
  }

  // Sort by year descending
  results.sort((a, b) => b.year - a.year)

  return results
}

// =============================================================================
// Rating Queries (Separate Entity)
// =============================================================================

export interface RatingInfo {
  titleId: string
  averageRating: number
  numVotes: number
}

/**
 * Get rating for a title (from separate Rating entity)
 */
export async function getRating(bucket: R2Bucket, titleId: string): Promise<RatingInfo | null> {
  const reader = await createReader(bucket, 'imdb/ratings')

  for await (const row of reader.scan({
    predicates: [{ column: 'titleId', op: 'eq', value: titleId }],
    limit: 1,
  })) {
    const rating = row as unknown as {
      titleId: string
      averageRating: number
      numVotes: number
    }

    return {
      titleId: rating.titleId,
      averageRating: rating.averageRating,
      numVotes: rating.numVotes,
    }
  }

  return null
}

/**
 * Get ratings distribution statistics
 */
export async function getRatingDistribution(
  bucket: R2Bucket,
  options: { type?: TitleType; minVotes?: number } = {}
): Promise<Array<{ rating: number; count: number }>> {
  const { type = 'movie', minVotes = 1000 } = options

  const distribution = new Map<number, number>()

  const reader = await createReader(bucket, `imdb/titles/type=${type}`)

  for await (const row of reader.scan({
    projection: ['averageRating', 'numVotes'],
    predicates: [{ column: 'numVotes', op: 'gte', value: minVotes }],
  })) {
    const title = row as unknown as {
      averageRating: number | null
      numVotes: number | null
    }

    if (title.averageRating === null) continue

    // Round to nearest 0.5
    const ratingBucket = Math.round(title.averageRating * 2) / 2
    distribution.set(ratingBucket, (distribution.get(ratingBucket) || 0) + 1)
  }

  const results = Array.from(distribution.entries())
    .map(([rating, count]) => ({ rating, count }))
    .sort((a, b) => a.rating - b.rating)

  return results
}

// =============================================================================
// Award Winners Query (placeholder - IMDB doesn't provide award data publicly)
// =============================================================================

export interface AwardWinner {
  personId?: string
  personName?: string
  titleId: string
  titleName: string
  year: number
  category: string
  award: string
}

/**
 * Get award winners for a given year and category
 *
 * Note: IMDB doesn't provide award data in their public datasets.
 * This is a placeholder that could be populated with external award data.
 * For now, it returns top-rated titles as a proxy for "award-worthy" content.
 */
export async function getAwardWinners(
  bucket: R2Bucket,
  year: number,
  _category?: string, // Not used without real award data
  options: { limit?: number } = {}
): Promise<AwardWinner[]> {
  const { limit = 10 } = options

  // Without real award data, return top-rated movies from that year
  const results: AwardWinner[] = []

  const types: TitleType[] = ['movie']

  for (const type of types) {
    const reader = await createReader(bucket, `imdb/titles/type=${type}`)

    const candidates: Array<{
      id: string
      primaryTitle: string
      averageRating: number
      numVotes: number
    }> = []

    for await (const row of reader.scan({
      predicates: [
        { column: 'startYear', op: 'eq', value: year },
        { column: 'numVotes', op: 'gte', value: 10000 },
      ],
      projection: ['id', 'primaryTitle', 'averageRating', 'numVotes'],
    })) {
      const title = row as unknown as {
        id: string
        primaryTitle: string
        averageRating: number | null
        numVotes: number | null
      }

      if (title.averageRating && title.numVotes) {
        candidates.push({
          id: title.id,
          primaryTitle: title.primaryTitle,
          averageRating: title.averageRating,
          numVotes: title.numVotes,
        })
      }
    }

    // Sort by rating and take top ones
    candidates.sort((a, b) => b.averageRating - a.averageRating)

    for (const title of candidates.slice(0, limit)) {
      results.push({
        titleId: title.id,
        titleName: title.primaryTitle,
        year,
        category: 'Top Rated',
        award: 'High Rating (proxy for awards)',
      })
    }
  }

  return results.slice(0, limit)
}

// =============================================================================
// Benchmark Scenarios
// =============================================================================

export interface BenchmarkResult {
  name: string
  description: string
  duration: number // milliseconds
  rowsScanned: number
  resultsReturned: number
  coldStart: boolean
}

export interface BenchmarkSuite {
  results: BenchmarkResult[]
  totalDuration: number
  summary: {
    coldStartAvg: number
    warmCacheAvg: number
    graphTraversal1Hop: number
    graphTraversal2Hop: number
    graphTraversal3Hop: number
    aggregationAvg: number
  }
}

/**
 * Run benchmark scenarios to measure query performance
 *
 * @example
 * const results = await runBenchmarks(bucket, {
 *   iterations: 3,
 *   warmupIterations: 1
 * })
 * console.log(results.summary)
 */
export async function runBenchmarks(
  bucket: R2Bucket,
  options: {
    iterations?: number
    warmupIterations?: number
    verbose?: boolean
  } = {}
): Promise<BenchmarkSuite> {
  const { iterations = 3, warmupIterations = 1, verbose = false } = options

  const results: BenchmarkResult[] = []
  const log = verbose ? console.log : () => {}

  // Helper to run a benchmark
  async function benchmark<T>(
    name: string,
    description: string,
    fn: () => Promise<T>,
    getStats: (result: T) => { rowsScanned: number; resultsReturned: number }
  ): Promise<void> {
    log(`\nRunning: ${name}`)

    // Warmup iterations (cold start)
    for (let i = 0; i < warmupIterations; i++) {
      const start = performance.now()
      const result = await fn()
      const duration = performance.now() - start
      const stats = getStats(result)

      results.push({
        name,
        description,
        duration,
        ...stats,
        coldStart: true,
      })

      log(`  Cold start ${i + 1}: ${duration.toFixed(2)}ms`)
    }

    // Timed iterations (warm cache)
    for (let i = 0; i < iterations; i++) {
      const start = performance.now()
      const result = await fn()
      const duration = performance.now() - start
      const stats = getStats(result)

      results.push({
        name,
        description,
        duration,
        ...stats,
        coldStart: false,
      })

      log(`  Warm cache ${i + 1}: ${duration.toFixed(2)}ms`)
    }
  }

  // ==========================================================================
  // Cold Start Queries
  // ==========================================================================

  await benchmark(
    'searchTitles_cold',
    'Search titles by name (cold start)',
    () => searchTitles(bucket, 'inception', { limit: 20 }),
    (r) => ({ rowsScanned: r.total, resultsReturned: r.items.length })
  )

  await benchmark(
    'getTitle_cold',
    'Get single title by ID (cold start)',
    () => getTitle(bucket, 'tt1375666'),
    (r) => ({ rowsScanned: 1, resultsReturned: r ? 1 : 0 })
  )

  // ==========================================================================
  // Graph Traversal - 1 Hop
  // ==========================================================================

  await benchmark(
    'getFilmography_1hop',
    'Get filmography for person (1-hop: Person -> Title)',
    () => getFilmography(bucket, 'nm0000138', { limit: 50 }),
    (r) => ({ rowsScanned: r.length * 2, resultsReturned: r.length })
  )

  await benchmark(
    'getCastAndCrew_1hop',
    'Get cast/crew for title (1-hop: Title <- Person)',
    () => getCastAndCrew(bucket, 'tt1375666', { limit: 50 }),
    (r) => ({ rowsScanned: r.length * 2, resultsReturned: r.length })
  )

  // ==========================================================================
  // Graph Traversal - 2 Hop
  // ==========================================================================

  await benchmark(
    'findCollaborations_2hop',
    'Find collaborations between two people (2-hop: Person -> Title <- Person)',
    () => findCollaborations(bucket, 'nm0000138', 'nm0000093'),
    (r) => ({ rowsScanned: r.length * 3, resultsReturned: r.length })
  )

  await benchmark(
    'findSimilarTitles_2hop',
    'Find similar titles by shared cast (2-hop: Title <- Person -> Title)',
    () => findSimilarTitles(bucket, 'tt1375666', { limit: 20 }),
    (r) => ({ rowsScanned: r.length * 5, resultsReturned: r.length })
  )

  // ==========================================================================
  // Graph Traversal - 3 Hop
  // ==========================================================================

  await benchmark(
    'getCareerTrajectory_3hop',
    'Get career trajectory (3-hop: Person -> Title -> Rating + Milestones)',
    () => getCareerTrajectory(bucket, 'nm0000138'),
    (r) => ({ rowsScanned: r ? r.totalCredits * 3 : 0, resultsReturned: r ? r.phases.length : 0 })
  )

  // ==========================================================================
  // Aggregation Queries
  // ==========================================================================

  await benchmark(
    'getGenreStats_agg',
    'Get genre statistics (full scan aggregation)',
    () => getGenreStats(bucket, { type: 'movie' }),
    (r) => ({ rowsScanned: 100000, resultsReturned: r.length })
  )

  await benchmark(
    'getGenreCooccurrence_agg',
    'Get genre co-occurrence (pair aggregation)',
    () => getGenreCooccurrence(bucket, { type: 'movie', limit: 50 }),
    (r) => ({ rowsScanned: 100000, resultsReturned: r.length })
  )

  await benchmark(
    'getYearStats_agg',
    'Get year statistics (range aggregation)',
    () => getYearStats(bucket, { type: 'movie', yearFrom: 2000, yearTo: 2020 }),
    (r) => ({ rowsScanned: 50000, resultsReturned: r.length })
  )

  await benchmark(
    'getRatingDistribution_agg',
    'Get rating distribution (histogram aggregation)',
    () => getRatingDistribution(bucket, { type: 'movie', minVotes: 1000 }),
    (r) => ({ rowsScanned: 50000, resultsReturned: r.length })
  )

  // ==========================================================================
  // Compute Summary Statistics
  // ==========================================================================

  const coldStartResults = results.filter((r) => r.coldStart)
  const warmCacheResults = results.filter((r) => !r.coldStart)

  const oneHopResults = results.filter((r) => r.name.includes('1hop'))
  const twoHopResults = results.filter((r) => r.name.includes('2hop'))
  const threeHopResults = results.filter((r) => r.name.includes('3hop'))
  const aggResults = results.filter((r) => r.name.includes('_agg'))

  const avg = (arr: BenchmarkResult[]) =>
    arr.length > 0 ? arr.reduce((sum, r) => sum + r.duration, 0) / arr.length : 0

  const totalDuration = results.reduce((sum, r) => sum + r.duration, 0)

  return {
    results,
    totalDuration,
    summary: {
      coldStartAvg: avg(coldStartResults),
      warmCacheAvg: avg(warmCacheResults),
      graphTraversal1Hop: avg(oneHopResults),
      graphTraversal2Hop: avg(twoHopResults),
      graphTraversal3Hop: avg(threeHopResults),
      aggregationAvg: avg(aggResults),
    },
  }
}

/**
 * Format benchmark results as a report
 */
export function formatBenchmarkReport(suite: BenchmarkSuite): string {
  const lines: string[] = []

  lines.push('='.repeat(70))
  lines.push('IMDB Query Benchmark Report')
  lines.push('='.repeat(70))
  lines.push('')

  // Summary
  lines.push('Summary:')
  lines.push(`  Total Duration: ${suite.totalDuration.toFixed(2)}ms`)
  lines.push(`  Cold Start Avg: ${suite.summary.coldStartAvg.toFixed(2)}ms`)
  lines.push(`  Warm Cache Avg: ${suite.summary.warmCacheAvg.toFixed(2)}ms`)
  lines.push(`  1-Hop Traversal Avg: ${suite.summary.graphTraversal1Hop.toFixed(2)}ms`)
  lines.push(`  2-Hop Traversal Avg: ${suite.summary.graphTraversal2Hop.toFixed(2)}ms`)
  lines.push(`  3-Hop Traversal Avg: ${suite.summary.graphTraversal3Hop.toFixed(2)}ms`)
  lines.push(`  Aggregation Avg: ${suite.summary.aggregationAvg.toFixed(2)}ms`)
  lines.push('')

  // Detailed Results
  lines.push('Detailed Results:')
  lines.push('-'.repeat(70))

  const grouped = new Map<string, BenchmarkResult[]>()
  for (const result of suite.results) {
    if (!grouped.has(result.name)) {
      grouped.set(result.name, [])
    }
    grouped.get(result.name)!.push(result)
  }

  for (const [name, groupedResults] of Array.from(grouped.entries())) {
    const coldResults = groupedResults.filter((r) => r.coldStart)
    const warmResults = groupedResults.filter((r) => !r.coldStart)

    const coldAvg = coldResults.length > 0
      ? coldResults.reduce((sum, r) => sum + r.duration, 0) / coldResults.length
      : 0
    const warmAvg = warmResults.length > 0
      ? warmResults.reduce((sum, r) => sum + r.duration, 0) / warmResults.length
      : 0

    lines.push(`\n${name}:`)
    lines.push(`  ${groupedResults[0].description}`)
    lines.push(`  Cold Start: ${coldAvg.toFixed(2)}ms`)
    lines.push(`  Warm Cache: ${warmAvg.toFixed(2)}ms`)
    lines.push(`  Speedup: ${coldAvg > 0 ? (coldAvg / warmAvg).toFixed(2) : 'N/A'}x`)
    lines.push(`  Results: ${groupedResults[0].resultsReturned}`)
  }

  lines.push('')
  lines.push('='.repeat(70))

  return lines.join('\n')
}
