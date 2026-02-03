/**
 * Tests for Studio QuickSwitcher (Cmd+K) fuzzy search logic
 *
 * @module
 */

import { describe, it, expect } from 'vitest'

// Test the fuzzy search scoring algorithm directly
// This mirrors the logic in QuickSwitcher.tsx

function fuzzyScore(query: string, target: string): number {
  const queryLower = query.toLowerCase()
  const targetLower = target.toLowerCase()

  // Exact match gets highest score
  if (targetLower === queryLower) return 1000

  // Starts with query
  if (targetLower.startsWith(queryLower)) return 500 + (query.length / target.length) * 100

  // Contains query as substring
  const index = targetLower.indexOf(queryLower)
  if (index >= 0) return 200 + (query.length / target.length) * 100 - index

  // Character-by-character fuzzy match
  let qi = 0
  let score = 0
  let prevMatch = -2

  for (let ti = 0; ti < targetLower.length && qi < queryLower.length; ti++) {
    if (targetLower[ti] === queryLower[qi]) {
      if (ti === prevMatch + 1) {
        score += 10
      }
      if (ti === 0 || target[ti - 1] === ' ' || target[ti - 1] === '-' || target[ti - 1] === '_') {
        score += 5
      }
      score += 1
      prevMatch = ti
      qi++
    }
  }

  if (qi < queryLower.length) return -1

  return score
}

interface MockDatabase {
  id: string
  name: string
  slug?: string
  description?: string
  lastAccessedAt?: Date
  visibility?: string
  entityCount?: number
}

function searchDatabases(
  databases: MockDatabase[],
  query: string,
  maxResults: number
): MockDatabase[] {
  if (!query.trim()) {
    return [...databases]
      .sort((a, b) => {
        const aTime = a.lastAccessedAt?.getTime() ?? 0
        const bTime = b.lastAccessedAt?.getTime() ?? 0
        return bTime - aTime
      })
      .slice(0, maxResults)
  }

  const scored = databases
    .map((db) => {
      const nameScore = fuzzyScore(query, db.name)
      const slugScore = db.slug ? fuzzyScore(query, db.slug) : -1
      const descScore = db.description ? fuzzyScore(query, db.description) * 0.5 : -1
      const bestScore = Math.max(nameScore, slugScore, descScore)
      return { db, score: bestScore }
    })
    .filter(({ score }) => score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, maxResults)

  return scored.map(({ db }) => db)
}

describe('QuickSwitcher', () => {
  describe('fuzzyScore', () => {
    it('should give highest score for exact match', () => {
      expect(fuzzyScore('test', 'test')).toBe(1000)
    })

    it('should be case-insensitive for exact match', () => {
      expect(fuzzyScore('Test', 'test')).toBe(1000)
      expect(fuzzyScore('test', 'TEST')).toBe(1000)
    })

    it('should give high score for prefix match', () => {
      const score = fuzzyScore('pro', 'production')
      expect(score).toBeGreaterThan(400)
      expect(score).toBeLessThan(1000)
    })

    it('should give moderate score for substring match', () => {
      const score = fuzzyScore('duct', 'production')
      expect(score).toBeGreaterThan(100)
      expect(score).toBeLessThan(500)
    })

    it('should give lower score for fuzzy match', () => {
      const score = fuzzyScore('pdn', 'production')
      expect(score).toBeGreaterThan(0)
      expect(score).toBeLessThan(200)
    })

    it('should return -1 for no match', () => {
      expect(fuzzyScore('xyz', 'production')).toBe(-1)
    })

    it('should prefer earlier substring matches', () => {
      const earlyMatch = fuzzyScore('test', 'testing database')
      const lateMatch = fuzzyScore('test', 'database testing')
      expect(earlyMatch).toBeGreaterThan(lateMatch)
    })

    it('should reward word boundary matches', () => {
      // "md" matching "my-database" should score well because
      // both 'm' and 'd' are at word boundaries
      const wordBoundary = fuzzyScore('md', 'my-database')
      expect(wordBoundary).toBeGreaterThan(0)
    })

    it('should reward consecutive character matches', () => {
      const consecutive = fuzzyScore('pro', 'production')
      const scattered = fuzzyScore('pdo', 'production')
      expect(consecutive).toBeGreaterThan(scattered)
    })
  })

  describe('searchDatabases', () => {
    const databases: MockDatabase[] = [
      {
        id: 'db_1',
        name: 'Production Database',
        slug: 'production',
        description: 'Main production data',
        lastAccessedAt: new Date('2024-01-15'),
        entityCount: 10000,
      },
      {
        id: 'db_2',
        name: 'Staging Database',
        slug: 'staging',
        description: 'Pre-production environment',
        lastAccessedAt: new Date('2024-01-14'),
        entityCount: 5000,
      },
      {
        id: 'db_3',
        name: 'Development',
        slug: 'dev',
        description: 'Local development database',
        lastAccessedAt: new Date('2024-01-16'),
        entityCount: 100,
      },
      {
        id: 'db_4',
        name: 'Analytics',
        slug: 'analytics',
        description: 'Analytics and reporting data',
        lastAccessedAt: new Date('2024-01-10'),
        entityCount: 50000,
      },
      {
        id: 'db_5',
        name: 'Test Suite',
        slug: 'test',
        description: 'Automated test data',
        lastAccessedAt: new Date('2024-01-12'),
        entityCount: 200,
      },
    ]

    it('should return databases sorted by lastAccessedAt when no query', () => {
      const results = searchDatabases(databases, '', 10)
      expect(results.length).toBe(5)
      // Most recently accessed first
      expect(results[0]!.id).toBe('db_3') // Jan 16
      expect(results[1]!.id).toBe('db_1') // Jan 15
      expect(results[2]!.id).toBe('db_2') // Jan 14
    })

    it('should filter by name', () => {
      const results = searchDatabases(databases, 'Production', 10)
      expect(results.length).toBeGreaterThanOrEqual(1)
      expect(results[0]!.name).toBe('Production Database')
    })

    it('should filter by slug', () => {
      const results = searchDatabases(databases, 'dev', 10)
      expect(results.length).toBeGreaterThanOrEqual(1)
      expect(results.some(r => r.slug === 'dev')).toBe(true)
    })

    it('should filter by description', () => {
      const results = searchDatabases(databases, 'reporting', 10)
      expect(results.length).toBeGreaterThanOrEqual(1)
      expect(results[0]!.id).toBe('db_4')
    })

    it('should rank exact matches highest', () => {
      const results = searchDatabases(databases, 'Analytics', 10)
      expect(results[0]!.name).toBe('Analytics')
    })

    it('should limit results to maxResults', () => {
      const results = searchDatabases(databases, 'a', 2)
      expect(results.length).toBeLessThanOrEqual(2)
    })

    it('should return empty array when no match', () => {
      const results = searchDatabases(databases, 'zzzzzzz', 10)
      expect(results.length).toBe(0)
    })

    it('should handle empty database list', () => {
      const results = searchDatabases([], 'test', 10)
      expect(results.length).toBe(0)
    })

    it('should handle whitespace-only query as no query', () => {
      const results = searchDatabases(databases, '   ', 10)
      expect(results.length).toBe(5)
    })

    it('should perform fuzzy matching', () => {
      // "stag" should match "Staging Database"
      const results = searchDatabases(databases, 'stag', 10)
      expect(results.length).toBeGreaterThanOrEqual(1)
      expect(results[0]!.slug).toBe('staging')
    })
  })

  describe('keyboard shortcut detection', () => {
    it('should identify macOS modifier key', () => {
      // On macOS, the modifier key should be "Cmd"
      const platform = 'MacIntel'
      const modKey = platform.includes('Mac') ? 'Cmd' : 'Ctrl'
      expect(modKey).toBe('Cmd')
    })

    it('should identify Windows/Linux modifier key', () => {
      const platform = 'Win32'
      const modKey = platform.includes('Mac') ? 'Cmd' : 'Ctrl'
      expect(modKey).toBe('Ctrl')
    })
  })
})
