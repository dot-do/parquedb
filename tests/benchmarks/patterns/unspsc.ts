/**
 * UNSPSC Query Patterns for ParqueDB Benchmarks
 *
 * 15 real-world query patterns for the UNSPSC (United Nations Standard Products
 * and Services Code) dataset. Tests REAL queries against the deployed worker at
 * https://parquedb.workers.do.
 *
 * Reference: docs/architecture/BENCHMARK-DESIGN.md
 *
 * UNSPSC Code Structure:
 * - Segment: 2 digits (e.g., 43 = IT)
 * - Family: 4 digits (e.g., 4310 = Networking equipment)
 * - Class: 6 digits (e.g., 431015 = Routers)
 * - Commodity: 8 digits (e.g., 43101501 = Network router)
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Query pattern category based on BENCHMARK-DESIGN.md
 */
export type QueryCategory =
  | 'point-lookup'
  | 'filtered'
  | 'relationship'
  | 'fts'
  | 'aggregation'
  | 'compound'

/**
 * Query pattern definition for benchmarking
 */
export interface QueryPattern {
  /** Human-readable pattern name */
  name: string
  /** Pattern category for grouping */
  category: QueryCategory
  /** Target latency in milliseconds */
  targetMs: number
  /** Function that executes the query and returns the Response */
  query: () => Promise<Response>
}

// =============================================================================
// Constants
// =============================================================================

/** Base URL for the deployed ParqueDB worker */
const BASE_URL = 'https://parquedb.workers.do'

/** UNSPSC dataset endpoints */
const DATASET = 'unspsc'

/** Collection endpoints */
const COLLECTIONS = {
  segments: `${BASE_URL}/datasets/${DATASET}/segments`,
  families: `${BASE_URL}/datasets/${DATASET}/families`,
  classes: `${BASE_URL}/datasets/${DATASET}/classes`,
  commodities: `${BASE_URL}/datasets/${DATASET}/commodities`,
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Build a URL with query parameters
 */
function buildUrl(
  baseUrl: string,
  params?: Record<string, string | number | boolean | undefined>
): string {
  if (!params) return baseUrl
  const url = new URL(baseUrl)
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) {
      url.searchParams.set(key, String(value))
    }
  }
  return url.toString()
}

/**
 * Build a URL with a MongoDB-style filter
 */
function buildFilterUrl(
  baseUrl: string,
  filter: Record<string, unknown>,
  options?: { limit?: number; skip?: number }
): string {
  const params: Record<string, string | number | undefined> = {
    filter: JSON.stringify(filter),
    limit: options?.limit,
    skip: options?.skip,
  }
  return buildUrl(baseUrl, params)
}

// =============================================================================
// UNSPSC Query Patterns (15 patterns from BENCHMARK-DESIGN.md)
// =============================================================================

export const unspscPatterns: QueryPattern[] = [
  // -------------------------------------------------------------------------
  // Pattern 1: Exact code lookup (43101501) - Point lookup - 5ms
  // -------------------------------------------------------------------------
  {
    name: 'Exact code lookup (43101501)',
    category: 'point-lookup',
    targetMs: 5,
    query: async () => {
      const url = buildFilterUrl(COLLECTIONS.commodities, {
        code: '43101501',
      }, { limit: 1 })
      return fetch(url)
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 2: Segment filter (43 = IT) - Equality - 50ms
  // -------------------------------------------------------------------------
  {
    name: 'Segment filter (43 = IT)',
    category: 'filtered',
    targetMs: 50,
    query: async () => {
      const url = buildFilterUrl(COLLECTIONS.commodities, {
        segment: '43',
      }, { limit: 100 })
      return fetch(url)
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 3: Family drill-down (4310) - Equality - 20ms
  // -------------------------------------------------------------------------
  {
    name: 'Family drill-down (4310)',
    category: 'filtered',
    targetMs: 20,
    query: async () => {
      const url = buildFilterUrl(COLLECTIONS.commodities, {
        family: '4310',
      }, { limit: 100 })
      return fetch(url)
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 4: Class filter (431015) - Equality - 20ms
  // -------------------------------------------------------------------------
  {
    name: 'Class filter (431015)',
    category: 'filtered',
    targetMs: 20,
    query: async () => {
      const url = buildFilterUrl(COLLECTIONS.commodities, {
        class: '431015',
      }, { limit: 100 })
      return fetch(url)
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 5: Code prefix search (4310*) - Prefix - 50ms
  // -------------------------------------------------------------------------
  {
    name: 'Code prefix search (4310*)',
    category: 'filtered',
    targetMs: 50,
    query: async () => {
      const url = buildFilterUrl(COLLECTIONS.commodities, {
        code: { $regex: '^4310' },
      }, { limit: 100 })
      return fetch(url)
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 6: Multi-segment (43, 44) - $in query - 100ms
  // -------------------------------------------------------------------------
  {
    name: 'Multi-segment (43, 44)',
    category: 'filtered',
    targetMs: 100,
    query: async () => {
      const url = buildFilterUrl(COLLECTIONS.commodities, {
        segment: { $in: ['43', '44'] },
      }, { limit: 100 })
      return fetch(url)
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 7: Text search ("laptop") - FTS - 50ms
  // -------------------------------------------------------------------------
  {
    name: 'Text search ("laptop")',
    category: 'fts',
    targetMs: 50,
    query: async () => {
      const url = buildFilterUrl(COLLECTIONS.commodities, {
        $text: { $search: 'laptop' },
      }, { limit: 50 })
      return fetch(url)
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 8: Breadcrumb (4 parallel) - Parallel point lookups - 20ms
  // -------------------------------------------------------------------------
  {
    name: 'Breadcrumb (4 parallel lookups)',
    category: 'point-lookup',
    targetMs: 20,
    query: async () => {
      // For a commodity like 43101501, fetch its hierarchy:
      // - Segment: 43
      // - Family: 4310
      // - Class: 431015
      // - Commodity: 43101501
      // Execute all 4 lookups in parallel
      const responses = await Promise.all([
        fetch(buildFilterUrl(COLLECTIONS.segments, { code: '43' }, { limit: 1 })),
        fetch(buildFilterUrl(COLLECTIONS.families, { code: '4310' }, { limit: 1 })),
        fetch(buildFilterUrl(COLLECTIONS.classes, { code: '431015' }, { limit: 1 })),
        fetch(buildFilterUrl(COLLECTIONS.commodities, { code: '43101501' }, { limit: 1 })),
      ])
      // Return the last response (commodity) as the representative response
      return responses[3]
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 9: Sibling commodities - Filtered - 20ms
  // -------------------------------------------------------------------------
  {
    name: 'Sibling commodities (same class)',
    category: 'filtered',
    targetMs: 20,
    query: async () => {
      // Find all commodities in the same class as 43101501 (class 431015)
      const url = buildFilterUrl(COLLECTIONS.commodities, {
        class: '431015',
      }, { limit: 50 })
      return fetch(url)
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 10: Bulk validation (500 codes) - Batch $in - 300ms
  // -------------------------------------------------------------------------
  {
    name: 'Bulk validation (500 codes)',
    category: 'filtered',
    targetMs: 300,
    query: async () => {
      // Generate 500 sample codes to validate
      // Using real UNSPSC code patterns from IT segment (43)
      const codes: string[] = []
      for (let family = 10; family <= 30; family++) {
        for (let cls = 10; cls <= 20; cls++) {
          for (let commodity = 1; commodity <= 25; commodity++) {
            if (codes.length >= 500) break
            codes.push(`43${family.toString().padStart(2, '0')}${cls.toString().padStart(2, '0')}${commodity.toString().padStart(2, '0')}`)
          }
          if (codes.length >= 500) break
        }
        if (codes.length >= 500) break
      }

      const url = buildFilterUrl(COLLECTIONS.commodities, {
        code: { $in: codes },
      }, { limit: 500 })
      return fetch(url)
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 11: FTS + hierarchy scope - Compound - 100ms
  // -------------------------------------------------------------------------
  {
    name: 'FTS + hierarchy scope ("printer" in IT)',
    category: 'compound',
    targetMs: 100,
    query: async () => {
      const url = buildFilterUrl(COLLECTIONS.commodities, {
        segment: '43',
        $text: { $search: 'printer' },
      }, { limit: 50 })
      return fetch(url)
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 12: Deprecated codes in segment - Compound - 30ms
  // -------------------------------------------------------------------------
  {
    name: 'Deprecated codes in segment',
    category: 'compound',
    targetMs: 30,
    query: async () => {
      const url = buildFilterUrl(COLLECTIONS.commodities, {
        segment: '43',
        status: 'deprecated',
      }, { limit: 100 })
      return fetch(url)
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 13: Hierarchy export (segment) - Large result - 500ms
  // -------------------------------------------------------------------------
  {
    name: 'Hierarchy export (full segment)',
    category: 'filtered',
    targetMs: 500,
    query: async () => {
      // Export all commodities in segment 43 (IT)
      const url = buildFilterUrl(COLLECTIONS.commodities, {
        segment: '43',
      }, { limit: 1000 })
      return fetch(url)
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 14: Segment distribution - Aggregation - 200ms
  // -------------------------------------------------------------------------
  {
    name: 'Segment distribution (count by segment)',
    category: 'aggregation',
    targetMs: 200,
    query: async () => {
      // Get all segments to understand distribution
      // Note: This is a simplified aggregation - actual aggregation would
      // require server-side support for GROUP BY operations
      const url = buildUrl(COLLECTIONS.segments, { limit: 100 })
      return fetch(url)
    },
  },

  // -------------------------------------------------------------------------
  // Pattern 15: Code range (43000000-44000000) - Range - 100ms
  // -------------------------------------------------------------------------
  {
    name: 'Code range (43000000-44000000)',
    category: 'filtered',
    targetMs: 100,
    query: async () => {
      const url = buildFilterUrl(COLLECTIONS.commodities, {
        code: {
          $gte: '43000000',
          $lt: '44000000',
        },
      }, { limit: 100 })
      return fetch(url)
    },
  },
]

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Get patterns by category
 */
export function getPatternsByCategory(category: QueryCategory): QueryPattern[] {
  return unspscPatterns.filter(p => p.category === category)
}

/**
 * Get pattern by name
 */
export function getPatternByName(name: string): QueryPattern | undefined {
  return unspscPatterns.find(p => p.name === name)
}

/**
 * Summary statistics about the query patterns
 */
export const PATTERN_STATS = {
  total: unspscPatterns.length,
  byCategory: {
    'point-lookup': unspscPatterns.filter(p => p.category === 'point-lookup').length,
    filtered: unspscPatterns.filter(p => p.category === 'filtered').length,
    relationship: unspscPatterns.filter(p => p.category === 'relationship').length,
    fts: unspscPatterns.filter(p => p.category === 'fts').length,
    aggregation: unspscPatterns.filter(p => p.category === 'aggregation').length,
    compound: unspscPatterns.filter(p => p.category === 'compound').length,
  },
  totalTargetMs: unspscPatterns.reduce((sum, p) => sum + p.targetMs, 0),
  avgTargetMs: unspscPatterns.reduce((sum, p) => sum + p.targetMs, 0) / unspscPatterns.length,
}
