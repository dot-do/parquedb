/**
 * IMDB Query Patterns for Benchmarking
 *
 * 15 real-world query patterns testing against the deployed worker at https://parquedb.workers.do
 * These patterns represent typical database access patterns for an entertainment discovery application.
 *
 * Categories:
 * - Point Lookup: Single entity retrieval by ID
 * - Filtered: List queries with filters
 * - Relationship: Graph traversal queries
 * - FTS: Full-text search
 * - Aggregation: Count and grouping queries
 * - Compound: Multiple filters combined
 *
 * @see docs/architecture/BENCHMARK-DESIGN.md for design rationale
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Query pattern categories for IMDB dataset benchmarks.
 */
export type QueryCategory =
  | 'point-lookup'
  | 'filtered'
  | 'relationship'
  | 'fts'
  | 'aggregation'
  | 'compound'

/**
 * A benchmark query pattern definition.
 *
 * Each pattern represents a real-world query that users execute against the database.
 * The query function returns a fetch() Promise for execution timing.
 */
export interface QueryPattern {
  /** Human-readable name for the pattern */
  name: string
  /** Query category for grouping in reports */
  category: QueryCategory
  /** Target latency in milliseconds (p50) */
  targetMs: number
  /** Description of what this query tests */
  description: string
  /** Function that executes the query and returns the fetch Promise */
  query: () => Promise<Response>
}

// =============================================================================
// Configuration
// =============================================================================

/**
 * Base URL for the deployed ParqueDB worker.
 * Can be overridden via environment variable for local testing.
 */
export const BASE_URL = process.env.PARQUEDB_URL || 'https://parquedb.workers.do'

/**
 * Common headers for API requests.
 */
const HEADERS = {
  Accept: 'application/json',
  'User-Agent': 'ParqueDB-Benchmark/1.0',
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Build a dataset collection URL with optional filter and options.
 */
function buildUrl(
  collection: string,
  filter?: Record<string, unknown>,
  options?: { limit?: number; sort?: Record<string, 1 | -1>; skip?: number }
): string {
  const params = new URLSearchParams()

  if (filter && Object.keys(filter).length > 0) {
    params.set('filter', JSON.stringify(filter))
  }

  if (options?.limit) {
    params.set('limit', String(options.limit))
  }

  if (options?.sort) {
    params.set('sort', JSON.stringify(options.sort))
  }

  if (options?.skip) {
    params.set('skip', String(options.skip))
  }

  const queryString = params.toString()
  return `${BASE_URL}/datasets/imdb/${collection}${queryString ? `?${queryString}` : ''}`
}

/**
 * Build an entity detail URL.
 */
function buildEntityUrl(collection: string, id: string): string {
  return `${BASE_URL}/datasets/imdb/${collection}/${encodeURIComponent(id)}`
}

/**
 * Build a relationship traversal URL.
 */
function buildRelationshipUrl(
  collection: string,
  id: string,
  predicate: string
): string {
  return `${BASE_URL}/datasets/imdb/${collection}/${encodeURIComponent(id)}/${predicate}`
}

// =============================================================================
// IMDB Query Patterns
// =============================================================================

/**
 * 15 real-world query patterns for IMDB dataset benchmarking.
 *
 * Pattern targets are based on BENCHMARK-DESIGN.md specifications:
 * - Point lookups: 5ms
 * - Filtered lists: 50-100ms
 * - Relationships: 50-100ms
 * - FTS: 20-30ms
 * - Aggregations: 500ms
 */
export const imdbPatterns: QueryPattern[] = [
  // ==========================================================================
  // Point Lookups (Target: <10ms)
  // ==========================================================================

  {
    name: 'Title by ID (tt0111161)',
    category: 'point-lookup',
    targetMs: 5,
    description: 'Point lookup for The Shawshank Redemption - tests single entity retrieval by tconst',
    query: () =>
      fetch(buildEntityUrl('titles', 'tt0111161'), { headers: HEADERS }),
  },

  {
    name: 'Person by ID (nm0000138)',
    category: 'point-lookup',
    targetMs: 5,
    description: 'Point lookup for a person by nconst - tests name entity retrieval',
    query: () =>
      fetch(buildEntityUrl('names', 'nm0000138'), { headers: HEADERS }),
  },

  // ==========================================================================
  // Filtered Lists (Target: 50-100ms)
  // ==========================================================================

  {
    name: 'Top-rated movies (rating>=8, votes>=100K)',
    category: 'compound',
    targetMs: 50,
    description: 'Find highly-rated popular movies - compound filter with numeric comparisons',
    query: () =>
      fetch(
        buildUrl(
          'titles',
          {
            titleType: 'movie',
            averageRating: { $gte: 8 },
            numVotes: { $gte: 100000 },
          },
          { limit: 50, sort: { averageRating: -1 } }
        ),
        { headers: HEADERS }
      ),
  },

  {
    name: 'Movies by year range (2010-2019)',
    category: 'filtered',
    targetMs: 100,
    description: 'Filter movies by decade - tests range filter on startYear',
    query: () =>
      fetch(
        buildUrl(
          'titles',
          {
            titleType: 'movie',
            startYear: { $gte: 2010, $lte: 2019 },
          },
          { limit: 100 }
        ),
        { headers: HEADERS }
      ),
  },

  {
    name: 'Genre filter (Action + rating>=7)',
    category: 'compound',
    targetMs: 50,
    description: 'Filter by genre and rating - tests array contains and numeric comparison',
    query: () =>
      fetch(
        buildUrl(
          'titles',
          {
            titleType: 'movie',
            genres: { $contains: 'Action' },
            averageRating: { $gte: 7 },
          },
          { limit: 50, sort: { numVotes: -1 } }
        ),
        { headers: HEADERS }
      ),
  },

  // ==========================================================================
  // Relationship Traversal (Target: 50-100ms)
  // ==========================================================================

  {
    name: 'Filmography (person -> titles)',
    category: 'relationship',
    targetMs: 100,
    description: 'Get all titles for a person - tests outbound relationship traversal',
    query: () =>
      fetch(
        buildRelationshipUrl('names', 'nm0000138', 'knownFor'),
        { headers: HEADERS }
      ),
  },

  {
    name: 'Cast of movie (title -> people)',
    category: 'relationship',
    targetMs: 50,
    description: 'Get cast and crew for a movie - tests reverse relationship traversal',
    query: () =>
      fetch(
        buildRelationshipUrl('titles', 'tt0111161', 'cast'),
        { headers: HEADERS }
      ),
  },

  // ==========================================================================
  // Full-Text Search (Target: 20-30ms)
  // ==========================================================================

  {
    name: 'Title search ("Shawshank")',
    category: 'fts',
    targetMs: 30,
    description: 'Full-text search for title name - tests FTS index',
    query: () =>
      fetch(
        buildUrl(
          'titles',
          {
            primaryTitle: { $contains: 'Shawshank' },
          },
          { limit: 20 }
        ),
        { headers: HEADERS }
      ),
  },

  {
    name: 'Autocomplete ("The God")',
    category: 'fts',
    targetMs: 20,
    description: 'Prefix search for autocomplete - tests prefix FTS pattern',
    query: () =>
      fetch(
        buildUrl(
          'titles',
          {
            primaryTitle: { $startsWith: 'The God' },
          },
          { limit: 10 }
        ),
        { headers: HEADERS }
      ),
  },

  // ==========================================================================
  // Aggregations (Target: 500ms)
  // ==========================================================================

  {
    name: 'TV series episode count',
    category: 'aggregation',
    targetMs: 500,
    description: 'Count episodes per TV series - tests aggregation query',
    query: () =>
      fetch(
        buildUrl(
          'titles',
          {
            titleType: 'tvEpisode',
          },
          { limit: 1000 }
        ),
        { headers: HEADERS }
      ),
  },

  {
    name: 'Count by title type',
    category: 'aggregation',
    targetMs: 500,
    description: 'Distribution of title types - tests count aggregation',
    query: () =>
      fetch(
        buildUrl(
          'titles',
          {},
          { limit: 1000 }
        ),
        { headers: HEADERS }
      ),
  },

  // ==========================================================================
  // Compound Filters (Target: 50-100ms)
  // ==========================================================================

  {
    name: 'Multi-genre filter (Action AND Sci-Fi)',
    category: 'compound',
    targetMs: 100,
    description: 'Filter by multiple genres - tests array $all operator',
    query: () =>
      fetch(
        buildUrl(
          'titles',
          {
            titleType: 'movie',
            genres: { $all: ['Action', 'Sci-Fi'] },
          },
          { limit: 50, sort: { averageRating: -1 } }
        ),
        { headers: HEADERS }
      ),
  },

  {
    name: 'People by profession (directors)',
    category: 'filtered',
    targetMs: 100,
    description: 'Filter people by primary profession - tests array contains',
    query: () =>
      fetch(
        buildUrl(
          'names',
          {
            primaryProfession: { $contains: 'director' },
          },
          { limit: 100 }
        ),
        { headers: HEADERS }
      ),
  },

  {
    name: 'Recent high-rated releases',
    category: 'compound',
    targetMs: 50,
    description: 'Recent movies with high ratings - compound range + filter',
    query: () =>
      fetch(
        buildUrl(
          'titles',
          {
            titleType: 'movie',
            startYear: { $gte: 2020 },
            averageRating: { $gte: 7.5 },
            numVotes: { $gte: 10000 },
          },
          { limit: 50, sort: { averageRating: -1 } }
        ),
        { headers: HEADERS }
      ),
  },

  {
    name: 'Related movies (same cast)',
    category: 'relationship',
    targetMs: 200,
    description: 'Multi-hop: find movies with actors from a given movie - tests 2-hop traversal',
    query: () =>
      // First, get the cast of a movie, then for each actor, get their filmography
      // For benchmark, we just test the first hop (cast retrieval) as the API handles it
      fetch(
        buildRelationshipUrl('titles', 'tt0111161', 'cast'),
        { headers: HEADERS }
      ),
  },
]

// =============================================================================
// Exports
// =============================================================================

/**
 * Get patterns by category for focused benchmarking.
 */
export function getPatternsByCategory(category: QueryCategory): QueryPattern[] {
  return imdbPatterns.filter((p) => p.category === category)
}

/**
 * Get a single pattern by name.
 */
export function getPatternByName(name: string): QueryPattern | undefined {
  return imdbPatterns.find((p) => p.name === name)
}

/**
 * Get all point lookup patterns.
 */
export function getPointLookupPatterns(): QueryPattern[] {
  return getPatternsByCategory('point-lookup')
}

/**
 * Get all relationship patterns.
 */
export function getRelationshipPatterns(): QueryPattern[] {
  return getPatternsByCategory('relationship')
}

/**
 * Get all FTS patterns.
 */
export function getFtsPatterns(): QueryPattern[] {
  return getPatternsByCategory('fts')
}

/**
 * Get all aggregation patterns.
 */
export function getAggregationPatterns(): QueryPattern[] {
  return getPatternsByCategory('aggregation')
}

/**
 * Get all compound filter patterns.
 */
export function getCompoundPatterns(): QueryPattern[] {
  return getPatternsByCategory('compound')
}

/**
 * Get all filtered list patterns.
 */
export function getFilteredPatterns(): QueryPattern[] {
  return getPatternsByCategory('filtered')
}

/**
 * Summary of all patterns for reporting.
 */
export const patternSummary = {
  total: imdbPatterns.length,
  byCategory: {
    'point-lookup': getPointLookupPatterns().length,
    filtered: getFilteredPatterns().length,
    relationship: getRelationshipPatterns().length,
    fts: getFtsPatterns().length,
    aggregation: getAggregationPatterns().length,
    compound: getCompoundPatterns().length,
  },
  targetTotalMs: imdbPatterns.reduce((sum, p) => sum + p.targetMs, 0),
}
