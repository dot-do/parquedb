/**
 * Benchmark Query Definitions for ParqueDB
 *
 * 50+ real-world query patterns across IMDB, O*NET, and UNSPSC datasets
 * for measuring native Parquet predicate pushdown performance on $index_* columns.
 *
 * All queries use $index_* columns which have Parquet min/max statistics,
 * enabling row group skipping during query execution.
 */

import type { Filter } from '../types/filter'

// =============================================================================
// Types
// =============================================================================

/**
 * Benchmark query definition
 */
export interface BenchmarkQuery {
  /** Unique query identifier */
  id: string
  /** Human-readable name */
  name: string
  /** Dataset this query runs against */
  dataset: 'imdb' | 'imdb-1m' | 'onet-full' | 'unspsc-full'
  /** Collection within the dataset */
  collection: string
  /** Query category */
  category: 'equality' | 'range' | 'compound' | 'fts'
  /** MongoDB-style filter using $index_* columns for native pushdown */
  filter: Filter
  /** Expected selectivity (high = few results, medium, low = many results) */
  selectivity: 'high' | 'medium' | 'low'
  /** Description of what the query tests */
  description: string
}

// =============================================================================
// IMDB Queries (18) - Using native pushdown on $index_* columns
// =============================================================================

export const IMDB_QUERIES: BenchmarkQuery[] = [
  // Equality queries - use $index_* columns with min/max statistics
  {
    id: 'imdb-eq-movie',
    name: 'Movies only',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'equality',
    filter: { $index_titleType: 'movie' },
    selectivity: 'low',
    description: 'Filter titles to movies only using native pushdown on titleType',
  },
  {
    id: 'imdb-eq-tvseries',
    name: 'TV Series only',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'equality',
    filter: { $index_titleType: 'tvSeries' },
    selectivity: 'low',
    description: 'Filter titles to TV series only using native pushdown',
  },
  {
    id: 'imdb-eq-short',
    name: 'Short films only',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'equality',
    filter: { $index_titleType: 'short' },
    selectivity: 'medium',
    description: 'Filter to short films using native pushdown',
  },
  {
    id: 'imdb-eq-tvmovie',
    name: 'TV Movies only',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'equality',
    filter: { $index_titleType: 'tvMovie' },
    selectivity: 'high',
    description: 'Filter to TV movies using native pushdown',
  },

  // Range queries - use $index_* columns with min/max statistics for row group skipping
  {
    id: 'imdb-range-recent',
    name: 'Recent titles (2020-2025)',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'range',
    filter: { $index_startYear: { $gte: 2020, $lte: 2025 } },
    selectivity: 'high',
    description: 'Range query for recent titles using native pushdown on startYear',
  },
  {
    id: 'imdb-range-2010s',
    name: '2010s titles',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'range',
    filter: { $index_startYear: { $gte: 2010, $lt: 2020 } },
    selectivity: 'medium',
    description: 'Range query for 2010s decade using native pushdown',
  },
  {
    id: 'imdb-range-2000s',
    name: '2000s titles',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'range',
    filter: { $index_startYear: { $gte: 2000, $lt: 2010 } },
    selectivity: 'medium',
    description: 'Range query for 2000s decade using native pushdown',
  },
  {
    id: 'imdb-range-classic',
    name: 'Classic titles (pre-2000)',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'range',
    filter: { $index_startYear: { $lt: 2000 } },
    selectivity: 'low',
    description: 'Range query for classic titles using native pushdown',
  },
  {
    id: 'imdb-range-top-rated',
    name: 'Top rated (>=8.0)',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'range',
    filter: { $index_averageRating: { $gte: 8.0 } },
    selectivity: 'high',
    description: 'Range query for highly rated titles using native pushdown',
  },
  {
    id: 'imdb-range-good',
    name: 'Good titles (>=7.0)',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'range',
    filter: { $index_averageRating: { $gte: 7.0 } },
    selectivity: 'medium',
    description: 'Range query for good titles using native pushdown',
  },
  {
    id: 'imdb-range-average',
    name: 'Average titles (6.0-7.0)',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'range',
    filter: { $index_averageRating: { $gte: 6.0, $lt: 7.0 } },
    selectivity: 'low',
    description: 'Range query for average-rated titles using native pushdown',
  },
  {
    id: 'imdb-range-popular',
    name: 'Popular (>=10000 votes)',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'range',
    filter: { $index_numVotes: { $gte: 10000 } },
    selectivity: 'high',
    description: 'Range query for popular titles by vote count',
  },

  // Compound queries - combine multiple $index_* columns
  {
    id: 'imdb-compound-movie-recent',
    name: 'Recent movies',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'compound',
    filter: { $index_titleType: 'movie', $index_startYear: { $gte: 2020 } },
    selectivity: 'high',
    description: 'Compound: movies from 2020 onwards',
  },
  {
    id: 'imdb-compound-movie-toprated',
    name: 'Top rated movies',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'compound',
    filter: { $index_titleType: 'movie', $index_averageRating: { $gte: 8.0 } },
    selectivity: 'high',
    description: 'Compound: movies with rating >= 8.0',
  },
  {
    id: 'imdb-compound-tv-2010s',
    name: 'TV series from 2010s',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'compound',
    filter: { $index_titleType: 'tvSeries', $index_startYear: { $gte: 2010, $lt: 2020 } },
    selectivity: 'medium',
    description: 'Compound: TV series from 2010s decade',
  },

  // FTS queries - full-text search
  {
    id: 'imdb-fts-dark-knight',
    name: 'Search: dark knight',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'fts',
    filter: { $text: { $search: 'dark knight' } },
    selectivity: 'high',
    description: 'Full-text search for "dark knight"',
  },
  {
    id: 'imdb-fts-star-wars',
    name: 'Search: star wars',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'fts',
    filter: { $text: { $search: 'star wars' } },
    selectivity: 'high',
    description: 'Full-text search for "star wars"',
  },
  {
    id: 'imdb-fts-lord-rings',
    name: 'Search: lord rings',
    dataset: 'imdb-1m',
    collection: 'titles',
    category: 'fts',
    filter: { $text: { $search: 'lord rings' } },
    selectivity: 'high',
    description: 'Full-text search for "lord rings"',
  },
]

// =============================================================================
// IMDB 100K Queries (9) - Smaller dataset that fits in Worker limits
// =============================================================================

export const IMDB_100K_QUERIES: BenchmarkQuery[] = [
  // Equality queries
  {
    id: 'imdb100k-eq-movie',
    name: 'Movies only',
    dataset: 'imdb',
    collection: 'titles',
    category: 'equality',
    filter: { $index_titleType: 'movie' },
    selectivity: 'low',
    description: 'Filter titles to movies only using native pushdown on titleType',
  },
  {
    id: 'imdb100k-eq-tvseries',
    name: 'TV Series only',
    dataset: 'imdb',
    collection: 'titles',
    category: 'equality',
    filter: { $index_titleType: 'tvSeries' },
    selectivity: 'low',
    description: 'Filter titles to TV series only using native pushdown',
  },
  // Range queries
  {
    id: 'imdb100k-range-year-2020s',
    name: 'Year 2020-2025',
    dataset: 'imdb',
    collection: 'titles',
    category: 'range',
    filter: { $index_startYear: { $gte: 2020, $lte: 2025 } },
    selectivity: 'high',
    description: 'Recent titles (2020-2025) using native pushdown',
  },
  {
    id: 'imdb100k-range-rating-high',
    name: 'Rating >= 8.0',
    dataset: 'imdb',
    collection: 'titles',
    category: 'range',
    filter: { $index_averageRating: { $gte: 8.0 } },
    selectivity: 'high',
    description: 'High-rated titles using native pushdown',
  },
  {
    id: 'imdb100k-range-votes-popular',
    name: 'Votes >= 10000',
    dataset: 'imdb',
    collection: 'titles',
    category: 'range',
    filter: { $index_numVotes: { $gte: 10000 } },
    selectivity: 'high',
    description: 'Popular titles by vote count using native pushdown',
  },
  // Compound queries
  {
    id: 'imdb100k-compound-movie-recent',
    name: 'Recent movies',
    dataset: 'imdb',
    collection: 'titles',
    category: 'compound',
    filter: { $index_titleType: 'movie', $index_startYear: { $gte: 2020 } },
    selectivity: 'high',
    description: 'Compound query: movies from 2020 onwards',
  },
  {
    id: 'imdb100k-compound-highrated-movie',
    name: 'High-rated movies',
    dataset: 'imdb',
    collection: 'titles',
    category: 'compound',
    filter: { $index_titleType: 'movie', $index_averageRating: { $gte: 8.0 } },
    selectivity: 'high',
    description: 'Compound query: movies with rating >= 8.0',
  },
  // FTS queries
  {
    id: 'imdb100k-fts-star',
    name: 'Search: star',
    dataset: 'imdb',
    collection: 'titles',
    category: 'fts',
    filter: { $text: { $search: 'star' } },
    selectivity: 'medium',
    description: 'Full-text search for "star" in title',
  },
  {
    id: 'imdb100k-fts-love',
    name: 'Search: love',
    dataset: 'imdb',
    collection: 'titles',
    category: 'fts',
    filter: { $text: { $search: 'love' } },
    selectivity: 'medium',
    description: 'Full-text search for "love" in title',
  },
]

// =============================================================================
// O*NET Queries (18) - Using native pushdown on $index_* columns
// =============================================================================

export const ONET_QUERIES: BenchmarkQuery[] = [
  // Equality queries
  {
    id: 'onet-eq-jobzone-1',
    name: 'Job Zone 1',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'equality',
    filter: { $index_jobZone: 1 },
    selectivity: 'medium',
    description: 'Filter occupations by Job Zone 1 (little preparation)',
  },
  {
    id: 'onet-eq-jobzone-4',
    name: 'Job Zone 4',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'equality',
    filter: { $index_jobZone: 4 },
    selectivity: 'medium',
    description: 'Filter occupations by Job Zone 4 (considerable preparation)',
  },
  {
    id: 'onet-eq-jobzone-5',
    name: 'Job Zone 5',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'equality',
    filter: { $index_jobZone: 5 },
    selectivity: 'high',
    description: 'Filter occupations by Job Zone 5 (extensive preparation)',
  },
  {
    id: 'onet-eq-scaleid-im',
    name: 'Importance scale',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'equality',
    filter: { $index_scaleId: 'IM' },
    selectivity: 'low',
    description: 'Filter data values by Importance scale',
  },
  {
    id: 'onet-eq-scaleid-lv',
    name: 'Level scale',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'equality',
    filter: { $index_scaleId: 'LV' },
    selectivity: 'low',
    description: 'Filter data values by Level scale',
  },

  // Range queries
  {
    id: 'onet-range-soc-15',
    name: 'Computer occupations (15-*)',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'range',
    filter: { $index_socCode: { $gte: '15-0000.00', $lt: '16-0000.00' } },
    selectivity: 'medium',
    description: 'Range query for computer and IT occupations (SOC 15-xxxx)',
  },
  {
    id: 'onet-range-soc-29',
    name: 'Healthcare occupations (29-*)',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'range',
    filter: { $index_socCode: { $gte: '29-0000.00', $lt: '30-0000.00' } },
    selectivity: 'medium',
    description: 'Range query for healthcare occupations (SOC 29-xxxx)',
  },
  {
    id: 'onet-range-soc-11',
    name: 'Management occupations (11-*)',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'range',
    filter: { $index_socCode: { $gte: '11-0000.00', $lt: '12-0000.00' } },
    selectivity: 'medium',
    description: 'Range query for management occupations (SOC 11-xxxx)',
  },
  {
    id: 'onet-range-importance-high',
    name: 'High importance (>=4.0)',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'range',
    filter: { $index_dataValue: { $gte: 4.0 } },
    selectivity: 'medium',
    description: 'Range query for high importance values',
  },
  {
    id: 'onet-range-importance-critical',
    name: 'Critical importance (>=4.5)',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'range',
    filter: { $index_dataValue: { $gte: 4.5 } },
    selectivity: 'high',
    description: 'Range query for critical importance values',
  },
  {
    id: 'onet-range-level-high',
    name: 'High level (>=5.0)',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'range',
    filter: { $index_dataValue: { $gte: 5.0 } },
    selectivity: 'high',
    description: 'Range query for high level requirements',
  },

  // Compound queries
  {
    id: 'onet-compound-jz5-importance',
    name: 'Job Zone 5 + high importance',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'compound',
    filter: { $index_jobZone: 5, $index_dataValue: { $gte: 4.0 } },
    selectivity: 'high',
    description: 'Compound: JZ5 occupations with high importance skills',
  },
  {
    id: 'onet-compound-soc15-scaleim',
    name: 'Computer + importance scale',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'compound',
    filter: { $index_socCode: { $gte: '15-0000.00', $lt: '16-0000.00' }, $index_scaleId: 'IM' },
    selectivity: 'high',
    description: 'Compound: Computer occupations with importance scale',
  },
  {
    id: 'onet-compound-jz4-level',
    name: 'Job Zone 4 + high level',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'compound',
    filter: { $index_jobZone: 4, $index_dataValue: { $gte: 5.0 } },
    selectivity: 'high',
    description: 'Compound: JZ4 occupations with high level requirements',
  },

  // FTS queries
  {
    id: 'onet-fts-software',
    name: 'Search: software',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'fts',
    filter: { $text: { $search: 'software' } },
    selectivity: 'high',
    description: 'Full-text search for software-related occupations',
  },
  {
    id: 'onet-fts-nursing',
    name: 'Search: nursing',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'fts',
    filter: { $text: { $search: 'nursing' } },
    selectivity: 'high',
    description: 'Full-text search for nursing occupations',
  },
  {
    id: 'onet-fts-management',
    name: 'Search: management',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'fts',
    filter: { $text: { $search: 'management' } },
    selectivity: 'medium',
    description: 'Full-text search for management occupations',
  },
  {
    id: 'onet-fts-engineer',
    name: 'Search: engineer',
    dataset: 'onet-full',
    collection: 'occupations',
    category: 'fts',
    filter: { $text: { $search: 'engineer' } },
    selectivity: 'medium',
    description: 'Full-text search for engineering occupations',
  },
]

// =============================================================================
// UNSPSC Queries (18) - Using native pushdown on $index_* columns
// =============================================================================

export const UNSPSC_QUERIES: BenchmarkQuery[] = [
  // Equality queries
  {
    id: 'unspsc-eq-segment-43',
    name: 'Segment 43 (IT)',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'equality',
    filter: { $index_segmentCode: '43' },
    selectivity: 'medium',
    description: 'Filter to IT equipment and supplies (segment 43)',
  },
  {
    id: 'unspsc-eq-segment-44',
    name: 'Segment 44 (Office)',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'equality',
    filter: { $index_segmentCode: '44' },
    selectivity: 'medium',
    description: 'Filter to office equipment and supplies (segment 44)',
  },
  {
    id: 'unspsc-eq-segment-50',
    name: 'Segment 50 (Food)',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'equality',
    filter: { $index_segmentCode: '50' },
    selectivity: 'medium',
    description: 'Filter to food and beverage products (segment 50)',
  },
  {
    id: 'unspsc-eq-segment-72',
    name: 'Segment 72 (Building)',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'equality',
    filter: { $index_segmentCode: '72' },
    selectivity: 'medium',
    description: 'Filter to building and construction (segment 72)',
  },
  {
    id: 'unspsc-eq-segment-80',
    name: 'Segment 80 (Services)',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'equality',
    filter: { $index_segmentCode: '80' },
    selectivity: 'medium',
    description: 'Filter to management and business services (segment 80)',
  },

  // Range queries - codes are strings in the data
  {
    id: 'unspsc-range-it',
    name: 'IT range (43000000-43999999)',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'range',
    filter: { $index_code: { $gte: '43000000', $lt: '44000000' } },
    selectivity: 'medium',
    description: 'Range query for IT category codes',
  },
  {
    id: 'unspsc-range-office',
    name: 'Office range (44000000-44999999)',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'range',
    filter: { $index_code: { $gte: '44000000', $lt: '45000000' } },
    selectivity: 'medium',
    description: 'Range query for office category codes',
  },
  {
    id: 'unspsc-range-food',
    name: 'Food range (50000000-50999999)',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'range',
    filter: { $index_code: { $gte: '50000000', $lt: '51000000' } },
    selectivity: 'medium',
    description: 'Range query for food category codes',
  },
  {
    id: 'unspsc-range-computers',
    name: 'Computers (43210000-43219999)',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'range',
    filter: { $index_code: { $gte: '43210000', $lt: '43220000' } },
    selectivity: 'high',
    description: 'Range query for computer hardware family',
  },
  {
    id: 'unspsc-range-software',
    name: 'Software (43230000-43239999)',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'range',
    filter: { $index_code: { $gte: '43230000', $lt: '43240000' } },
    selectivity: 'high',
    description: 'Range query for software family',
  },

  // Hierarchy drill-down queries
  {
    id: 'unspsc-hier-family-4310',
    name: 'Family 4310 (Networking)',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'range',
    filter: { $index_code: { $gte: '43100000', $lt: '43110000' } },
    selectivity: 'high',
    description: 'Drill down to networking equipment family',
  },
  {
    id: 'unspsc-hier-class-431015',
    name: 'Class 431015 (Routers)',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'range',
    filter: { $index_code: { $gte: '43101500', $lt: '43101600' } },
    selectivity: 'high',
    description: 'Drill down to router class',
  },

  // Compound queries
  {
    id: 'unspsc-compound-it-active',
    name: 'Active IT products',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'compound',
    filter: { $index_segmentCode: '43', status: 'active' },
    selectivity: 'medium',
    description: 'Compound: IT segment + active status',
  },
  {
    id: 'unspsc-compound-services-pro',
    name: 'Professional services',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'compound',
    filter: { $index_segmentCode: '80', $index_code: { $gte: '80100000', $lt: '80200000' } },
    selectivity: 'high',
    description: 'Compound: Services segment + professional range',
  },

  // FTS queries
  {
    id: 'unspsc-fts-computer',
    name: 'Search: computer',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'fts',
    filter: { $text: { $search: 'computer' } },
    selectivity: 'medium',
    description: 'Full-text search for computer products',
  },
  {
    id: 'unspsc-fts-office-supplies',
    name: 'Search: office supplies',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'fts',
    filter: { $text: { $search: 'office supplies' } },
    selectivity: 'medium',
    description: 'Full-text search for office supplies',
  },
  {
    id: 'unspsc-fts-printer',
    name: 'Search: printer',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'fts',
    filter: { $text: { $search: 'printer' } },
    selectivity: 'high',
    description: 'Full-text search for printers',
  },
  {
    id: 'unspsc-fts-software',
    name: 'Search: software',
    dataset: 'unspsc-full',
    collection: 'commodities',
    category: 'fts',
    filter: { $text: { $search: 'software' } },
    selectivity: 'medium',
    description: 'Full-text search for software products',
  },
]

// =============================================================================
// Combined Query List
// =============================================================================

/**
 * All benchmark queries across all datasets
 */
export const ALL_QUERIES: BenchmarkQuery[] = [
  ...IMDB_QUERIES,
  ...IMDB_100K_QUERIES,
  ...ONET_QUERIES,
  ...UNSPSC_QUERIES,
]

/**
 * Get queries for a specific dataset
 */
export function getQueriesForDataset(dataset: 'imdb' | 'imdb-1m' | 'onet-full' | 'unspsc-full'): BenchmarkQuery[] {
  return ALL_QUERIES.filter(q => q.dataset === dataset)
}

/**
 * Get queries by category
 */
export function getQueriesByCategory(category: BenchmarkQuery['category']): BenchmarkQuery[] {
  return ALL_QUERIES.filter(q => q.category === category)
}

/**
 * Summary statistics about the query set
 */
export const QUERY_STATS = {
  total: ALL_QUERIES.length,
  byDataset: {
    'imdb': IMDB_100K_QUERIES.length,
    'imdb-1m': IMDB_QUERIES.length,
    'onet-full': ONET_QUERIES.length,
    'unspsc-full': UNSPSC_QUERIES.length,
  },
  byCategory: {
    equality: ALL_QUERIES.filter(q => q.category === 'equality').length,
    range: ALL_QUERIES.filter(q => q.category === 'range').length,
    compound: ALL_QUERIES.filter(q => q.category === 'compound').length,
    fts: ALL_QUERIES.filter(q => q.category === 'fts').length,
  },
  bySelectivity: {
    high: ALL_QUERIES.filter(q => q.selectivity === 'high').length,
    medium: ALL_QUERIES.filter(q => q.selectivity === 'medium').length,
    low: ALL_QUERIES.filter(q => q.selectivity === 'low').length,
  },
}
