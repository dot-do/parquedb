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
  predicates?: Record<string, string[]> | undefined
  /** Singular form of predicates (for *Scores field lookup) */
  singular?: Record<string, string> | undefined
}

// =============================================================================
// Dataset Configuration
// =============================================================================

export const DATASETS: Record<string, DatasetConfig> = {
  imdb: {
    name: 'IMDB',
    description: 'Internet Movie Database - Sample titles and names',
    collections: ['titles', 'names'],
    source: 'https://datasets.imdbws.com/',
    prefix: 'imdb',
  },
  // Alias: 'onet' provides simpler URL (uses same data as onet-graph)
  onet: {
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
  // 'onet-optimized' removed: ETL produces consolidated format (data.parquet, rels.parquet)
  // but this config expected per-collection files. Use 'onet-graph' instead which works correctly.
  unspsc: {
    name: 'UNSPSC',
    description: 'United Nations Standard Products and Services Code - Product taxonomy',
    collections: ['segments', 'families', 'classes', 'commodities'],
    source: 'https://www.unspsc.org/',
    prefix: 'unspsc',
  },
  wikidata: {
    name: 'Wikidata',
    description: 'Wikidata knowledge graph sample - items, properties, and claims',
    collections: ['items', 'properties', 'claims'],
    source: 'https://www.wikidata.org/',
    prefix: 'wikidata',
    predicates: {
      items: ['instanceOf', 'subclassOf', 'country', 'locatedIn'],
      claims: ['subject', 'property', 'object'],
    },
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
