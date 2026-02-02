/**
 * Dataset Configuration for ParqueDB Worker
 *
 * Defines available example datasets with their collections and predicates.
 */

// =============================================================================
// Dataset Types
// =============================================================================

export interface DatasetConfig {
  name: string
  description: string
  collections: string[]
  source: string
  prefix: string
  /** Relationship predicates for graph navigation */
  predicates?: Record<string, string[]>
  /** Singular form of predicates (for *Scores field lookup) */
  singular?: Record<string, string>
}

// =============================================================================
// Dataset Configuration
// =============================================================================

export const DATASETS: Record<string, DatasetConfig> = {
  imdb: {
    name: 'IMDB',
    description: 'Internet Movie Database - 7M+ titles, ratings, cast & crew',
    collections: ['titles', 'names', 'ratings', 'principals', 'crew'],
    source: 'https://datasets.imdbws.com/',
    prefix: 'imdb',
  },
  'onet-graph': {
    name: 'O*NET',
    description: 'Occupational Information Network - 1,016 occupations with skills, abilities, knowledge relationships',
    collections: ['occupations', 'skills', 'abilities', 'knowledge'],
    source: 'https://www.onetcenter.org/database.html',
    prefix: 'onet-graph',
    predicates: {
      occupations: ['skills', 'abilities', 'knowledge'],
      skills: ['requiredBy'],
      abilities: ['requiredBy'],
      knowledge: ['requiredBy'],
    },
    singular: {
      skills: 'skill',
      abilities: 'ability',
      knowledge: 'knowledge',
      requiredBy: 'requiredBy',
    },
  },
  'onet-optimized': {
    name: 'O*NET (Optimized)',
    description: 'O*NET with optimized single-column format for fast lookups',
    collections: ['occupations', 'skills', 'abilities', 'knowledge'],
    source: 'https://www.onetcenter.org/database.html',
    prefix: 'onet-optimized',
  },
  unspsc: {
    name: 'UNSPSC',
    description: 'United Nations Standard Products and Services Code - Product taxonomy',
    collections: ['segments', 'families', 'classes', 'commodities'],
    source: 'https://www.unspsc.org/',
    prefix: 'unspsc',
  },
  wikidata: {
    name: 'Wikidata',
    description: 'Structured knowledge base - Entities, properties, claims',
    collections: ['entities', 'properties'],
    source: 'https://www.wikidata.org/',
    prefix: 'wikidata',
  },
}

/**
 * Get a dataset configuration by ID
 */
export function getDataset(id: string): DatasetConfig | undefined {
  return DATASETS[id]
}

/**
 * Get all dataset IDs
 */
export function getDatasetIds(): string[] {
  return Object.keys(DATASETS)
}
