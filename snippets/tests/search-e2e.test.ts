import { describe, test, expect, beforeAll } from 'vitest'

const BASE_URL = 'https://cdn.workers.do'

// Test multiple versions
const VERSIONS = ['v6', 'v7', 'v8', 'v9']

interface SearchResponse {
  data: unknown[]
  total: number
  limit: number
  offset: number
  facets?: Record<string, { value: string; count: number }[]>
  stats?: Record<string, { min: number; max: number; avg: number }>
  didYouMean?: string
  timing?: Record<string, number>
  cpuBudget?: { used: number; limit: number; exceeded: boolean }
  error?: string
}

async function search(version: string, dataset: string, params: string): Promise<SearchResponse> {
  const url = `${BASE_URL}/search-${version}/${dataset}?${params}`
  const response = await fetch(url)
  return response.json() as Promise<SearchResponse>
}

describe('Search Worker E2E Tests', () => {
  describe('Health Check', () => {
    for (const version of VERSIONS) {
      test(`${version} health endpoint returns ok`, async () => {
        const response = await fetch(`${BASE_URL}/search-${version}/health`)
        const data = await response.json() as { ok: boolean; version: number; features: string[] }

        expect(data.ok).toBe(true)
        expect(data.version).toBeGreaterThanOrEqual(6)
        expect(data.features).toBeInstanceOf(Array)
        expect(data.features.length).toBeGreaterThan(0)
      })
    }
  })

  describe('Basic Search', () => {
    for (const version of VERSIONS) {
      test(`${version} finds matrix movies`, async () => {
        const result = await search(version, 'imdb', 'q=matrix')

        expect(result.total).toBeGreaterThan(0)
        expect(result.data.length).toBeGreaterThan(0)

        // Check that results contain "matrix"
        const titles = result.data.map((d: any) => d.primaryTitle?.toLowerCase() || '')
        expect(titles.some(t => t.includes('matrix'))).toBe(true)
      })

      test(`${version} respects limit parameter`, async () => {
        const result = await search(version, 'imdb', 'q=love&limit=5')

        expect(result.limit).toBe(5)
        expect(result.data.length).toBeLessThanOrEqual(5)
      })

      test(`${version} handles empty query`, async () => {
        const result = await search(version, 'imdb', 'limit=10')

        // Should return some results (browse mode)
        expect(result.data.length).toBeGreaterThan(0)
      })
    }
  })

  describe('Filters', () => {
    for (const version of VERSIONS) {
      test(`${version} filters by type`, async () => {
        const result = await search(version, 'imdb', 'type=movie&limit=20')

        expect(result.data.length).toBeGreaterThan(0)
        for (const doc of result.data as any[]) {
          expect(doc.titleType).toBe('movie')
        }
      })

      test(`${version} filters by year range`, async () => {
        const result = await search(version, 'imdb', 'year_gte=2010&year_lte=2015&limit=20')

        for (const doc of result.data as any[]) {
          expect(doc.startYear).toBeGreaterThanOrEqual(2010)
          expect(doc.startYear).toBeLessThanOrEqual(2015)
        }
      })

      test(`${version} combines filters`, async () => {
        const result = await search(version, 'imdb', 'q=love&type=movie&year_gte=2000')

        for (const doc of result.data as any[]) {
          expect(doc.titleType).toBe('movie')
          expect(doc.startYear).toBeGreaterThanOrEqual(2000)
        }
      })
    }
  })

  describe('Sorting', () => {
    for (const version of VERSIONS) {
      test(`${version} sorts by year descending`, async () => {
        const result = await search(version, 'imdb', 'type=movie&sort=startYear:desc&limit=10')

        const years = (result.data as any[]).map(d => d.startYear).filter(y => y)
        for (let i = 1; i < years.length; i++) {
          expect(years[i]).toBeLessThanOrEqual(years[i - 1]!)
        }
      })

      test(`${version} sorts by year ascending`, async () => {
        const result = await search(version, 'imdb', 'type=movie&sort=startYear:asc&limit=10')

        const years = (result.data as any[]).map(d => d.startYear).filter(y => y)
        for (let i = 1; i < years.length; i++) {
          expect(years[i]).toBeGreaterThanOrEqual(years[i - 1]!)
        }
      })
    }
  })

  describe('Highlighting', () => {
    for (const version of VERSIONS) {
      test(`${version} highlights matching terms`, async () => {
        const result = await search(version, 'imdb', 'q=matrix&highlight=true')

        const highlighted = (result.data as any[]).some(d =>
          d.primaryTitle?.includes('**') || d.originalTitle?.includes('**')
        )
        expect(highlighted).toBe(true)
      })
    }
  })

  describe('Facets', () => {
    for (const version of VERSIONS) {
      test(`${version} returns genre facets`, async () => {
        const result = await search(version, 'imdb', 'q=love&facets=genres')

        expect(result.facets).toBeDefined()
        expect(result.facets!.genres).toBeDefined()
        expect(result.facets!.genres.length).toBeGreaterThan(0)

        // Check facet structure
        const firstFacet = result.facets!.genres[0]!
        expect(firstFacet.value).toBeDefined()
        expect(firstFacet.count).toBeGreaterThan(0)
      })

      test(`${version} handles array field facets (genres)`, async () => {
        const result = await search(version, 'imdb', 'type=movie&facets=genres')

        // Genres is an array field - should still work
        expect(result.facets?.genres).toBeDefined()
        expect(result.facets!.genres.some(f => f.value === 'Drama')).toBe(true)
      })
    }
  })

  describe('Stats', () => {
    for (const version of VERSIONS) {
      test(`${version} returns numeric stats`, async () => {
        const result = await search(version, 'imdb', 'type=movie&stats=startYear')

        expect(result.stats).toBeDefined()
        expect(result.stats!.startYear).toBeDefined()

        const stats = result.stats!.startYear!
        expect(stats.min).toBeLessThanOrEqual(stats.max)
        expect(stats.avg).toBeGreaterThanOrEqual(stats.min)
        expect(stats.avg).toBeLessThanOrEqual(stats.max)
      })
    }
  })

  describe('Fuzzy Search', () => {
    for (const version of VERSIONS) {
      test(`${version} finds results with typos using ~`, async () => {
        // "matrx" should match "matrix"
        const result = await search(version, 'imdb', 'q=matrx~')

        // Should either find results or suggest correction
        const hasResults = result.total > 0
        const hasSuggestion = !!result.didYouMean

        expect(hasResults || hasSuggestion).toBe(true)
      })
    }
  })

  describe('Autocomplete', () => {
    for (const version of VERSIONS) {
      test(`${version} returns suggestions`, async () => {
        const response = await fetch(`${BASE_URL}/search-${version}/suggest/imdb?q=mat`)
        const data = await response.json() as { suggestions: string[]; query: string }

        expect(data.suggestions).toBeInstanceOf(Array)
        // Should suggest terms starting with "mat"
        if (data.suggestions.length > 0) {
          expect(data.suggestions.every(s => s.startsWith('mat'))).toBe(true)
        }
      })
    }
  })

  describe('CPU Budget (v9)', () => {
    test('v9 tracks CPU time', async () => {
      const result = await search('v9', 'imdb', 'q=love&timing=true')

      expect(result.timing).toBeDefined()
      expect(result.cpuBudget).toBeDefined()
      expect(result.cpuBudget!.limit).toBe(4)
      expect(typeof result.cpuBudget!.used).toBe('number')
      expect(typeof result.cpuBudget!.exceeded).toBe('boolean')
    })

    test('v9 CPU stays under budget for simple queries', async () => {
      const result = await search('v9', 'imdb', 'q=matrix&timing=true')

      expect(result.cpuBudget!.exceeded).toBe(false)
    })
  })

  describe('Error Handling', () => {
    for (const version of VERSIONS) {
      test(`${version} returns 404 for unknown dataset`, async () => {
        const response = await fetch(`${BASE_URL}/search-${version}/unknown?q=test`)
        expect(response.status).toBe(404)
      })

      test(`${version} handles invalid parameters gracefully`, async () => {
        const result = await search(version, 'imdb', 'limit=invalid')

        // Should not crash, may return default limit
        expect(result.error).toBeUndefined()
      })
    }
  })

  describe('Performance', () => {
    test('Response time is under 500ms', async () => {
      const start = performance.now()
      await search('v9', 'imdb', 'q=matrix')
      const elapsed = performance.now() - start

      expect(elapsed).toBeLessThan(500)
    })

    test('Complex query responds under 1000ms', async () => {
      const start = performance.now()
      await search('v9', 'imdb', 'q=love&facets=genres,titleType&stats=startYear&sort=startYear:desc')
      const elapsed = performance.now() - start

      expect(elapsed).toBeLessThan(1000)
    })
  })

  describe('Cross-Dataset', () => {
    test('O*NET search works', async () => {
      const result = await search('v9', 'onet', 'q=engineer')

      expect(result.total).toBeGreaterThan(0)
    })

    test('UNSPSC search works', async () => {
      const result = await search('v9', 'unspsc', 'q=computer')

      expect(result.total).toBeGreaterThan(0)
    })
  })
})
