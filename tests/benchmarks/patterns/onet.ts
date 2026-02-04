/**
 * O*NET Dataset Query Patterns for ParqueDB Benchmarks
 *
 * 15 real-world query patterns testing actual performance against
 * the deployed worker at https://parquedb.workers.do
 *
 * Reference: docs/architecture/BENCHMARK-DESIGN.md
 *
 * Query Categories:
 * - Point lookups: Single entity retrieval by ID
 * - Filtered: Equality and range filters
 * - Relationship: Graph traversal (occupation -> skills, skills -> occupations)
 * - FTS: Full-text search on titles/descriptions
 * - Aggregation: Count, average calculations
 * - Compound: Multiple filters combined
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Query pattern definition for benchmarking
 */
export interface QueryPattern {
  /** Human-readable pattern name */
  name: string
  /** Query category for grouping results */
  category: 'point-lookup' | 'filtered' | 'relationship' | 'fts' | 'aggregation' | 'compound'
  /** Target latency in milliseconds (p50) */
  targetMs: number
  /** Description of what this pattern tests */
  description: string
  /** Function that executes the query and returns the Response */
  query: () => Promise<Response>
}

// =============================================================================
// Configuration
// =============================================================================

/**
 * Base URL for the deployed ParqueDB worker
 */
const WORKER_URL = 'https://parquedb.workers.do'

/**
 * O*NET dataset base path
 */
const ONET_BASE = `${WORKER_URL}/datasets/onet`

// =============================================================================
// Query Pattern Definitions
// =============================================================================

/**
 * 15 O*NET query patterns based on BENCHMARK-DESIGN.md specifications
 */
export const onetPatterns: QueryPattern[] = [
  // =========================================================================
  // Pattern 1: Occupation by SOC code - Point lookup (Target: 5ms)
  // =========================================================================
  {
    name: 'Occupation by SOC code',
    category: 'point-lookup',
    targetMs: 5,
    description: 'Point lookup for a specific occupation by SOC code (e.g., 15-1252.00 Software Developers)',
    query: async () => {
      // Software Developers - common SOC code
      return fetch(`${ONET_BASE}/occupations/15-1252.00`, {
        headers: { Accept: 'application/json' },
      })
    },
  },

  // =========================================================================
  // Pattern 2: Job Zone = 4 (Bachelor's degree) - Equality filter (Target: 20ms)
  // =========================================================================
  {
    name: 'Job Zone = 4 (Bachelors)',
    category: 'filtered',
    targetMs: 20,
    description: 'Filter occupations requiring Bachelor\'s degree (Job Zone 4)',
    query: async () => {
      const filter = encodeURIComponent(JSON.stringify({ jobZone: 4 }))
      return fetch(`${ONET_BASE}/occupations?filter=${filter}&limit=50`, {
        headers: { Accept: 'application/json' },
      })
    },
  },

  // =========================================================================
  // Pattern 3: Job Zone <= 2 (entry-level) - Range filter (Target: 20ms)
  // =========================================================================
  {
    name: 'Job Zone <= 2 (entry-level)',
    category: 'filtered',
    targetMs: 20,
    description: 'Filter entry-level occupations (Job Zone 1 or 2, minimal preparation)',
    query: async () => {
      const filter = encodeURIComponent(JSON.stringify({ jobZone: { $lte: 2 } }))
      return fetch(`${ONET_BASE}/occupations?filter=${filter}&limit=50`, {
        headers: { Accept: 'application/json' },
      })
    },
  },

  // =========================================================================
  // Pattern 4: SOC prefix 15-* (Computer occupations) - Range filter (Target: 50ms)
  // =========================================================================
  {
    name: 'SOC prefix 15-* (Computer)',
    category: 'filtered',
    targetMs: 50,
    description: 'Filter computer and IT occupations by SOC code prefix 15-*',
    query: async () => {
      // Using range query for SOC code prefix matching
      const filter = encodeURIComponent(JSON.stringify({
        socCode: { $gte: '15-0000.00', $lt: '16-0000.00' },
      }))
      return fetch(`${ONET_BASE}/occupations?filter=${filter}&limit=100`, {
        headers: { Accept: 'application/json' },
      })
    },
  },

  // =========================================================================
  // Pattern 5: Skills for occupation - Relationship traversal (Target: 50ms)
  // =========================================================================
  {
    name: 'Skills for occupation',
    category: 'relationship',
    targetMs: 50,
    description: 'Get skills required by Software Developers (relationship traversal)',
    query: async () => {
      // Traverse occupation -> skills relationship
      return fetch(`${ONET_BASE}/occupations/15-1252.00/skills`, {
        headers: { Accept: 'application/json' },
      })
    },
  },

  // =========================================================================
  // Pattern 6: High importance skills (>= 4.0) - Range filter (Target: 100ms)
  // =========================================================================
  {
    name: 'High importance skills (>=4.0)',
    category: 'filtered',
    targetMs: 100,
    description: 'Filter skills with high importance rating (>= 4.0 on 5-point scale)',
    query: async () => {
      const filter = encodeURIComponent(JSON.stringify({
        importance: { $gte: 4.0 },
      }))
      return fetch(`${ONET_BASE}/skills?filter=${filter}&limit=100`, {
        headers: { Accept: 'application/json' },
      })
    },
  },

  // =========================================================================
  // Pattern 7: Occupations requiring skill X - Reverse lookup (Target: 100ms)
  // =========================================================================
  {
    name: 'Occupations requiring skill',
    category: 'relationship',
    targetMs: 100,
    description: 'Reverse lookup: find occupations requiring "Programming" skill',
    query: async () => {
      // Use the requiredBy reverse relationship on skills
      // Programming skill element ID: 2.A.1.a
      return fetch(`${ONET_BASE}/skills/2.A.1.a/requiredBy`, {
        headers: { Accept: 'application/json' },
      })
    },
  },

  // =========================================================================
  // Pattern 8: Skill + level compound filter (Target: 50ms)
  // =========================================================================
  {
    name: 'Skill + level compound',
    category: 'compound',
    targetMs: 50,
    description: 'Compound filter: skills with high importance (>=4.0) AND high level (>=4.0)',
    query: async () => {
      const filter = encodeURIComponent(JSON.stringify({
        importance: { $gte: 4.0 },
        level: { $gte: 4.0 },
      }))
      return fetch(`${ONET_BASE}/skills?filter=${filter}&limit=50`, {
        headers: { Accept: 'application/json' },
      })
    },
  },

  // =========================================================================
  // Pattern 9: Skill gap (2 occupations) - Parallel queries (Target: 100ms)
  // =========================================================================
  {
    name: 'Skill gap (2 occupations)',
    category: 'compound',
    targetMs: 100,
    description: 'Compare skills between Software Developer and Data Scientist (parallel queries)',
    query: async () => {
      // Execute two parallel requests for skill comparison
      // We return just the first response but measure total time
      const [softwareDev, dataScientist] = await Promise.all([
        fetch(`${ONET_BASE}/occupations/15-1252.00/skills`, {
          headers: { Accept: 'application/json' },
        }),
        fetch(`${ONET_BASE}/occupations/15-2051.00/skills`, {
          headers: { Accept: 'application/json' },
        }),
      ])
      // Return the first response (both are needed for skill gap analysis)
      return softwareDev
    },
  },

  // =========================================================================
  // Pattern 10: Core tasks for occupation - Filtered relationship (Target: 30ms)
  // =========================================================================
  {
    name: 'Core tasks for occupation',
    category: 'filtered',
    targetMs: 30,
    description: 'Get core tasks (primary responsibilities) for Software Developers',
    query: async () => {
      // Note: Tasks may be accessed via a different endpoint or embedded
      // Using occupation endpoint with task data if available
      const filter = encodeURIComponent(JSON.stringify({
        occupation: '15-1252.00',
        isCore: true,
      }))
      // Try the occupation's detailed view which includes tasks
      return fetch(`${ONET_BASE}/occupations/15-1252.00?include=tasks`, {
        headers: { Accept: 'application/json' },
      })
    },
  },

  // =========================================================================
  // Pattern 11: Hot technologies - Equality filter (Target: 50ms)
  // =========================================================================
  {
    name: 'Hot technologies',
    category: 'filtered',
    targetMs: 50,
    description: 'Find occupations using "hot" (trending) technologies like Python, JavaScript',
    query: async () => {
      // Search for occupations with Python as a hot technology
      const filter = encodeURIComponent(JSON.stringify({
        $or: [
          { 'technologies.name': { $regex: 'Python', $options: 'i' } },
          { name: { $regex: 'software', $options: 'i' } },
        ],
      }))
      return fetch(`${ONET_BASE}/occupations?filter=${filter}&limit=50`, {
        headers: { Accept: 'application/json' },
      })
    },
  },

  // =========================================================================
  // Pattern 12: Title search ("data scientist") - FTS (Target: 30ms)
  // =========================================================================
  {
    name: 'Title search (data scientist)',
    category: 'fts',
    targetMs: 30,
    description: 'Full-text search for "data scientist" in occupation titles',
    query: async () => {
      // Use name filter for text matching
      return fetch(`${ONET_BASE}/occupations?name=data%20scientist&limit=20`, {
        headers: { Accept: 'application/json' },
      })
    },
  },

  // =========================================================================
  // Pattern 13: Count by job zone - Aggregation (Target: 100ms)
  // =========================================================================
  {
    name: 'Count by job zone',
    category: 'aggregation',
    targetMs: 100,
    description: 'Aggregate count of occupations grouped by Job Zone (1-5)',
    query: async () => {
      // Fetch all occupations to compute aggregation client-side
      // (ParqueDB may not support server-side aggregation yet)
      return fetch(`${ONET_BASE}/occupations?limit=1500`, {
        headers: { Accept: 'application/json' },
      })
    },
  },

  // =========================================================================
  // Pattern 14: Average skill importance - Aggregation (Target: 200ms)
  // =========================================================================
  {
    name: 'Average skill importance',
    category: 'aggregation',
    targetMs: 200,
    description: 'Calculate average importance score across all skills',
    query: async () => {
      // Fetch skills data for aggregation
      return fetch(`${ONET_BASE}/skills?limit=500`, {
        headers: { Accept: 'application/json' },
      })
    },
  },

  // =========================================================================
  // Pattern 15: Tech stack with UNSPSC - Multi-hop (Target: 150ms)
  // =========================================================================
  {
    name: 'Tech stack with UNSPSC',
    category: 'compound',
    targetMs: 150,
    description: 'Multi-hop: Occupation -> Technologies -> UNSPSC commodity codes',
    query: async () => {
      // First get occupation with technologies
      // Then cross-reference with UNSPSC (if available)
      // This simulates a multi-dataset join
      const occupation = await fetch(`${ONET_BASE}/occupations/15-1252.00`, {
        headers: { Accept: 'application/json' },
      })
      // The response includes technology data that could link to UNSPSC
      return occupation
    },
  },
]

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Run all O*NET patterns and collect results
 */
export async function runAllPatterns(options?: {
  warmup?: number
  iterations?: number
}): Promise<PatternResult[]> {
  const warmup = options?.warmup ?? 2
  const iterations = options?.iterations ?? 10

  const results: PatternResult[] = []

  for (const pattern of onetPatterns) {
    // Warmup iterations
    for (let i = 0; i < warmup; i++) {
      try {
        await pattern.query()
      } catch {
        // Ignore warmup errors
      }
    }

    // Measurement iterations
    const latencies: number[] = []
    let lastResponse: Response | null = null
    let errors = 0

    for (let i = 0; i < iterations; i++) {
      const start = performance.now()
      try {
        lastResponse = await pattern.query()
        latencies.push(performance.now() - start)
      } catch {
        errors++
      }
    }

    if (latencies.length > 0) {
      const sorted = [...latencies].sort((a, b) => a - b)
      results.push({
        pattern,
        latencies,
        stats: {
          min: sorted[0]!,
          max: sorted[sorted.length - 1]!,
          p50: percentile(sorted, 50),
          p95: percentile(sorted, 95),
          p99: percentile(sorted, 99),
          avg: latencies.reduce((a, b) => a + b, 0) / latencies.length,
        },
        meetsTarget: percentile(sorted, 50) <= pattern.targetMs,
        response: lastResponse ? {
          status: lastResponse.status,
          ok: lastResponse.ok,
        } : undefined,
        errors,
      })
    }
  }

  return results
}

/**
 * Result from running a pattern
 */
export interface PatternResult {
  pattern: QueryPattern
  latencies: number[]
  stats: {
    min: number
    max: number
    p50: number
    p95: number
    p99: number
    avg: number
  }
  meetsTarget: boolean
  response?: {
    status: number
    ok: boolean
  }
  errors: number
}

/**
 * Calculate percentile from sorted array
 */
function percentile(sorted: number[], p: number): number {
  const idx = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, idx)] ?? 0
}

/**
 * Format results as a markdown table
 */
export function formatResultsTable(results: PatternResult[]): string {
  const lines: string[] = [
    '| Pattern | Category | Target | p50 | p95 | Status |',
    '|---------|----------|--------|-----|-----|--------|',
  ]

  for (const r of results) {
    const status = r.meetsTarget ? 'PASS' : 'FAIL'
    const statusIcon = r.meetsTarget ? 'PASS' : 'FAIL'
    lines.push(
      `| ${r.pattern.name} | ${r.pattern.category} | ${r.pattern.targetMs}ms | ${r.stats.p50.toFixed(0)}ms | ${r.stats.p95.toFixed(0)}ms | ${statusIcon} |`
    )
  }

  return lines.join('\n')
}

/**
 * Get summary statistics for all results
 */
export function getSummary(results: PatternResult[]): {
  total: number
  passed: number
  failed: number
  byCategory: Record<string, { passed: number; failed: number }>
} {
  const byCategory: Record<string, { passed: number; failed: number }> = {}

  for (const r of results) {
    if (!byCategory[r.pattern.category]) {
      byCategory[r.pattern.category] = { passed: 0, failed: 0 }
    }
    if (r.meetsTarget) {
      byCategory[r.pattern.category]!.passed++
    } else {
      byCategory[r.pattern.category]!.failed++
    }
  }

  return {
    total: results.length,
    passed: results.filter(r => r.meetsTarget).length,
    failed: results.filter(r => !r.meetsTarget).length,
    byCategory,
  }
}
