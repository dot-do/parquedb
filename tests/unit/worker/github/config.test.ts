import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  parseConfig,
  loadConfigFromRepo,
  shouldCreateBranch,
  shouldIgnoreBranch,
  parseTTL,
  clearConfigCache,
  invalidateConfigCache,
  getConfigCacheTTL,
  type ParqueDBGitHubConfig
} from '../../../../src/worker/github/config'

// Clear cache before each test to ensure isolation
beforeEach(() => {
  clearConfigCache()
})

describe('ParqueDB GitHub config parser', () => {
  describe('parseConfig', () => {
    it('parses valid config', () => {
      const yaml = `
database:
  name: my-database

branches:
  auto_create:
    - 'feature/*'
    - 'fix/*'
  ignore:
    - 'dependabot/*'

preview:
  enabled: true
  ttl: 24h
  visibility: unlisted

merge:
  required_check: true
  default_strategy: manual

diff:
  auto_comment: true
  max_entities: 100
  show_samples: false
`
      const config = parseConfig(yaml)
      expect(config.database.name).toBe('my-database')
      expect(config.branches.auto_create).toEqual(['feature/*', 'fix/*'])
      expect(config.branches.ignore).toEqual(['dependabot/*'])
      expect(config.preview.enabled).toBe(true)
      expect(config.preview.ttl).toBe('24h')
      expect(config.preview.visibility).toBe('unlisted')
      expect(config.merge.required_check).toBe(true)
      expect(config.merge.default_strategy).toBe('manual')
      expect(config.diff.auto_comment).toBe(true)
      expect(config.diff.max_entities).toBe(100)
      expect(config.diff.show_samples).toBe(false)
    })

    it('returns parsed config object', () => {
      const yaml = `
database:
  name: test-db
`
      const config = parseConfig(yaml)
      expect(config).toHaveProperty('database')
      expect(config).toHaveProperty('branches')
      expect(config).toHaveProperty('preview')
      expect(config).toHaveProperty('merge')
      expect(config).toHaveProperty('diff')
    })
  })

  describe('default values', () => {
    it('uses default database name from repo', () => {
      const yaml = ''  // Empty config
      const config = parseConfig(yaml, 'my-repo')
      expect(config.database.name).toBe('my-repo')
    })

    it('defaults branches.auto_create to feature/*, fix/*', () => {
      const yaml = 'database:\n  name: test'
      const config = parseConfig(yaml)
      expect(config.branches.auto_create).toEqual(['feature/*', 'fix/*'])
    })

    it('defaults branches.ignore to dependabot/*, renovate/*', () => {
      const yaml = 'database:\n  name: test'
      const config = parseConfig(yaml)
      expect(config.branches.ignore).toEqual(['dependabot/*', 'renovate/*'])
    })

    it('defaults preview.enabled to true', () => {
      const yaml = 'database:\n  name: test'
      const config = parseConfig(yaml)
      expect(config.preview.enabled).toBe(true)
    })

    it('defaults preview.ttl to 24h', () => {
      const yaml = 'database:\n  name: test'
      const config = parseConfig(yaml)
      expect(config.preview.ttl).toBe('24h')
    })

    it('defaults preview.visibility to unlisted', () => {
      const yaml = 'database:\n  name: test'
      const config = parseConfig(yaml)
      expect(config.preview.visibility).toBe('unlisted')
    })

    it('defaults merge.required_check to true', () => {
      const yaml = 'database:\n  name: test'
      const config = parseConfig(yaml)
      expect(config.merge.required_check).toBe(true)
    })

    it('defaults merge.default_strategy to manual', () => {
      const yaml = 'database:\n  name: test'
      const config = parseConfig(yaml)
      expect(config.merge.default_strategy).toBe('manual')
    })

    it('defaults diff.auto_comment to true', () => {
      const yaml = 'database:\n  name: test'
      const config = parseConfig(yaml)
      expect(config.diff.auto_comment).toBe(true)
    })

    it('defaults diff.max_entities to 100', () => {
      const yaml = 'database:\n  name: test'
      const config = parseConfig(yaml)
      expect(config.diff.max_entities).toBe(100)
    })

    it('defaults diff.show_samples to false', () => {
      const yaml = 'database:\n  name: test'
      const config = parseConfig(yaml)
      expect(config.diff.show_samples).toBe(false)
    })
  })

  describe('validation', () => {
    it('validates branch patterns are valid globs', () => {
      const yaml = `
branches:
  auto_create:
    - '[invalid'
`
      expect(() => parseConfig(yaml)).toThrow(/invalid glob pattern/i)
    })

    it('validates preview.ttl format', () => {
      const yaml = `
preview:
  ttl: invalid
`
      expect(() => parseConfig(yaml)).toThrow(/invalid ttl format/i)
    })

    it('validates preview.visibility is valid', () => {
      const yaml = `
preview:
  visibility: invalid
`
      expect(() => parseConfig(yaml)).toThrow(/invalid visibility/i)
    })

    it('validates merge.default_strategy is valid', () => {
      const yaml = `
merge:
  default_strategy: invalid
`
      expect(() => parseConfig(yaml)).toThrow(/invalid merge strategy/i)
    })

    it('validates diff.max_entities is positive number', () => {
      const yaml = `
diff:
  max_entities: -1
`
      expect(() => parseConfig(yaml)).toThrow(/max_entities must be positive/i)
    })
  })

  describe('loading from repo', () => {
    it('loads config from .github/parquedb.yml', async () => {
      // Mock GitHub API
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        text: async () => 'database:\n  name: test-db',
      })
      global.fetch = mockFetch

      const config = await loadConfigFromRepo('owner', 'repo')
      expect(config.database.name).toBe('test-db')
      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining('.github/parquedb.yml'),
        expect.any(Object)
      )
    })

    it('loads config from .github/parquedb.yaml', async () => {
      // Mock GitHub API - first file not found, second succeeds
      const mockFetch = vi.fn()
        .mockResolvedValueOnce({ ok: false, status: 404 })
        .mockResolvedValueOnce({
          ok: true,
          text: async () => 'database:\n  name: test-db',
        })
      global.fetch = mockFetch

      const config = await loadConfigFromRepo('owner', 'repo')
      expect(config.database.name).toBe('test-db')
      expect(mockFetch).toHaveBeenCalledTimes(2)
    })

    it('returns defaults when file not found', async () => {
      const mockFetch = vi.fn().mockResolvedValue({ ok: false, status: 404 })
      global.fetch = mockFetch

      const config = await loadConfigFromRepo('owner', 'repo')
      expect(config.database.name).toBe('repo')
      expect(config.branches.auto_create).toEqual(['feature/*', 'fix/*'])
    })

    it('throws on invalid YAML syntax', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        text: async () => 'database:\n  name: [invalid yaml',
      })
      global.fetch = mockFetch

      await expect(loadConfigFromRepo('owner', 'repo')).rejects.toThrow(/yaml/i)
    })

    it('caches config per repo', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        text: async () => 'database:\n  name: test-db',
      })
      global.fetch = mockFetch

      await loadConfigFromRepo('owner', 'repo')
      await loadConfigFromRepo('owner', 'repo')

      // Should only fetch once due to caching
      expect(mockFetch).toHaveBeenCalledTimes(1)
    })

    it('deduplicates concurrent requests (race condition prevention)', async () => {
      // This test verifies that when multiple concurrent requests come in
      // for the same repo config, only ONE fetch is made (promise deduplication)
      vi.useFakeTimers()
      try {
        let fetchCallCount = 0
        const mockFetch = vi.fn().mockImplementation(async () => {
          fetchCallCount++
          // Simulate network delay to ensure requests overlap
          await vi.advanceTimersByTimeAsync(50)
          return {
            ok: true,
            text: async () => 'database:\n  name: test-db',
          }
        })
        global.fetch = mockFetch

        // Fire off 5 concurrent requests for the same repo
        const promises = [
          loadConfigFromRepo('owner', 'concurrent-repo'),
          loadConfigFromRepo('owner', 'concurrent-repo'),
          loadConfigFromRepo('owner', 'concurrent-repo'),
          loadConfigFromRepo('owner', 'concurrent-repo'),
          loadConfigFromRepo('owner', 'concurrent-repo'),
        ]

        const results = await Promise.all(promises)

        // All results should be the same config object
        expect(results[0].database.name).toBe('test-db')
        for (const result of results) {
          expect(result).toBe(results[0]) // Same reference
        }

        // Critical: only ONE fetch should have been made
        // The promise deduplication ensures that when multiple concurrent requests
        // arrive, they all share the same in-flight promise rather than each
        // starting their own fetch. This prevents the "thundering herd" problem.
        expect(fetchCallCount).toBe(1)
        expect(mockFetch).toHaveBeenCalledTimes(1)
      } finally {
        vi.useRealTimers()
      }
    })

    it('handles concurrent request errors correctly', async () => {
      vi.useFakeTimers()
      try {
        const mockFetch = vi.fn().mockImplementation(async () => {
          await vi.advanceTimersByTimeAsync(10)
          return {
            ok: true,
            text: async () => { throw new Error('Network error') },
          }
        })
        global.fetch = mockFetch

        // Fire concurrent requests that will fail
        const promises = [
          loadConfigFromRepo('owner', 'error-repo'),
          loadConfigFromRepo('owner', 'error-repo'),
          loadConfigFromRepo('owner', 'error-repo'),
        ]

        // All should reject with the same error
        await expect(Promise.all(promises)).rejects.toThrow()

        // After error, cache should be cleared so retry works
        clearConfigCache()
        const mockFetchSuccess = vi.fn().mockResolvedValue({
          ok: true,
          text: async () => 'database:\n  name: recovered',
        })
        global.fetch = mockFetchSuccess

        const result = await loadConfigFromRepo('owner', 'error-repo')
        expect(result.database.name).toBe('recovered')
      } finally {
        vi.useRealTimers()
      }
    })

    it('respects cache TTL and refetches after expiration', async () => {
      // This test verifies that cached configs expire after the TTL
      // We use vi.useFakeTimers to control time
      vi.useFakeTimers()

      let fetchCallCount = 0
      const mockFetch = vi.fn().mockImplementation(async () => {
        fetchCallCount++
        return {
          ok: true,
          text: async () => `database:\n  name: fetch-${fetchCallCount}`,
        }
      })
      global.fetch = mockFetch

      // First request - should fetch
      const result1 = await loadConfigFromRepo('owner', 'ttl-repo')
      expect(result1.database.name).toBe('fetch-1')
      expect(fetchCallCount).toBe(1)

      // Second request immediately - should use cache
      const result2 = await loadConfigFromRepo('owner', 'ttl-repo')
      expect(result2.database.name).toBe('fetch-1')
      expect(fetchCallCount).toBe(1) // No new fetch

      // Advance time past TTL (5 minutes)
      await vi.advanceTimersByTimeAsync(getConfigCacheTTL() + 1000)

      // Third request after TTL - should refetch
      const result3 = await loadConfigFromRepo('owner', 'ttl-repo')
      expect(result3.database.name).toBe('fetch-2')
      expect(fetchCallCount).toBe(2) // New fetch occurred

      vi.useRealTimers()
    })

    it('invalidateConfigCache forces refetch on next request', async () => {
      let fetchCallCount = 0
      const mockFetch = vi.fn().mockImplementation(async () => {
        fetchCallCount++
        return {
          ok: true,
          text: async () => `database:\n  name: fetch-${fetchCallCount}`,
        }
      })
      global.fetch = mockFetch

      // First request - should fetch
      const result1 = await loadConfigFromRepo('owner', 'invalidate-repo')
      expect(result1.database.name).toBe('fetch-1')
      expect(fetchCallCount).toBe(1)

      // Second request - should use cache
      const result2 = await loadConfigFromRepo('owner', 'invalidate-repo')
      expect(result2.database.name).toBe('fetch-1')
      expect(fetchCallCount).toBe(1)

      // Invalidate the cache
      invalidateConfigCache('owner', 'invalidate-repo')

      // Third request after invalidation - should refetch
      const result3 = await loadConfigFromRepo('owner', 'invalidate-repo')
      expect(result3.database.name).toBe('fetch-2')
      expect(fetchCallCount).toBe(2)
    })

    it('invalidateConfigCache only affects the specified repo', async () => {
      const mockFetch = vi.fn().mockImplementation(async (url: string) => {
        const repoMatch = url.match(/github\.com\/owner\/([^/]+)/)
        const repo = repoMatch ? repoMatch[1] : 'unknown'
        return {
          ok: true,
          text: async () => `database:\n  name: ${repo}`,
        }
      })
      global.fetch = mockFetch

      // Load configs for two repos
      await loadConfigFromRepo('owner', 'repo-a')
      await loadConfigFromRepo('owner', 'repo-b')
      expect(mockFetch).toHaveBeenCalledTimes(2)

      // Invalidate only repo-a
      invalidateConfigCache('owner', 'repo-a')

      // Fetch repo-a again - should refetch
      await loadConfigFromRepo('owner', 'repo-a')
      expect(mockFetch).toHaveBeenCalledTimes(3)

      // Fetch repo-b again - should still be cached
      await loadConfigFromRepo('owner', 'repo-b')
      expect(mockFetch).toHaveBeenCalledTimes(3) // No additional fetch
    })
  })

  describe('pattern matching', () => {
    describe('shouldCreateBranch', () => {
      it('matches feature/* pattern', () => {
        const config: ParqueDBGitHubConfig = {
          database: { name: 'test' },
          branches: { auto_create: ['feature/*'], ignore: [] },
          preview: { enabled: true, ttl: '24h', visibility: 'unlisted' },
          merge: { required_check: true, default_strategy: 'manual' },
          diff: { auto_comment: true, max_entities: 100, show_samples: false },
        }
        expect(shouldCreateBranch(config, 'feature/foo')).toBe(true)
      })

      it('matches nested patterns', () => {
        const config: ParqueDBGitHubConfig = {
          database: { name: 'test' },
          branches: { auto_create: ['feature/**'], ignore: [] },
          preview: { enabled: true, ttl: '24h', visibility: 'unlisted' },
          merge: { required_check: true, default_strategy: 'manual' },
          diff: { auto_comment: true, max_entities: 100, show_samples: false },
        }
        expect(shouldCreateBranch(config, 'feature/foo/bar')).toBe(true)
      })

      it('does not match non-matching patterns', () => {
        const config: ParqueDBGitHubConfig = {
          database: { name: 'test' },
          branches: { auto_create: ['feature/*'], ignore: [] },
          preview: { enabled: true, ttl: '24h', visibility: 'unlisted' },
          merge: { required_check: true, default_strategy: 'manual' },
          diff: { auto_comment: true, max_entities: 100, show_samples: false },
        }
        expect(shouldCreateBranch(config, 'hotfix/foo')).toBe(false)
      })
    })

    describe('shouldIgnoreBranch', () => {
      it('matches ignore patterns', () => {
        const config: ParqueDBGitHubConfig = {
          database: { name: 'test' },
          branches: { auto_create: [], ignore: ['dependabot/*'] },
          preview: { enabled: true, ttl: '24h', visibility: 'unlisted' },
          merge: { required_check: true, default_strategy: 'manual' },
          diff: { auto_comment: true, max_entities: 100, show_samples: false },
        }
        expect(shouldIgnoreBranch(config, 'dependabot/npm-lodash')).toBe(true)
      })

      it('ignore takes precedence over auto_create', () => {
        const config: ParqueDBGitHubConfig = {
          database: { name: 'test' },
          branches: { auto_create: ['*'], ignore: ['dependabot/*'] },
          preview: { enabled: true, ttl: '24h', visibility: 'unlisted' },
          merge: { required_check: true, default_strategy: 'manual' },
          diff: { auto_comment: true, max_entities: 100, show_samples: false },
        }
        expect(shouldIgnoreBranch(config, 'dependabot/npm-lodash')).toBe(true)
        expect(shouldCreateBranch(config, 'dependabot/npm-lodash')).toBe(false)
      })
    })
  })

  describe('TTL parsing', () => {
    it('parses hours (24h)', () => {
      const ms = parseTTL('24h')
      expect(ms).toBe(86400000) // 24 * 60 * 60 * 1000
    })

    it('parses days (7d)', () => {
      const ms = parseTTL('7d')
      expect(ms).toBe(604800000) // 7 * 24 * 60 * 60 * 1000
    })

    it('parses minutes (30m)', () => {
      const ms = parseTTL('30m')
      expect(ms).toBe(1800000) // 30 * 60 * 1000
    })

    it('defaults to hours if no unit', () => {
      const ms = parseTTL('24')
      expect(ms).toBe(86400000) // Same as 24h
    })

    it('throws on invalid format', () => {
      expect(() => parseTTL('invalid')).toThrow(/invalid ttl/i)
    })
  })

  describe('strategy validation', () => {
    it('accepts ours strategy', () => {
      const yaml = `
merge:
  default_strategy: ours
`
      const config = parseConfig(yaml)
      expect(config.merge.default_strategy).toBe('ours')
    })

    it('accepts theirs strategy', () => {
      const yaml = `
merge:
  default_strategy: theirs
`
      const config = parseConfig(yaml)
      expect(config.merge.default_strategy).toBe('theirs')
    })

    it('accepts newest strategy', () => {
      const yaml = `
merge:
  default_strategy: newest
`
      const config = parseConfig(yaml)
      expect(config.merge.default_strategy).toBe('newest')
    })

    it('accepts manual strategy', () => {
      const yaml = `
merge:
  default_strategy: manual
`
      const config = parseConfig(yaml)
      expect(config.merge.default_strategy).toBe('manual')
    })

    it('rejects invalid strategy', () => {
      const yaml = `
merge:
  default_strategy: invalid
`
      expect(() => parseConfig(yaml)).toThrow(/invalid merge strategy/i)
    })
  })

  describe('visibility validation', () => {
    it('accepts public visibility', () => {
      const yaml = `
preview:
  visibility: public
`
      const config = parseConfig(yaml)
      expect(config.preview.visibility).toBe('public')
    })

    it('accepts unlisted visibility', () => {
      const yaml = `
preview:
  visibility: unlisted
`
      const config = parseConfig(yaml)
      expect(config.preview.visibility).toBe('unlisted')
    })

    it('accepts private visibility', () => {
      const yaml = `
preview:
  visibility: private
`
      const config = parseConfig(yaml)
      expect(config.preview.visibility).toBe('private')
    })

    it('rejects invalid visibility', () => {
      const yaml = `
preview:
  visibility: invalid
`
      expect(() => parseConfig(yaml)).toThrow(/invalid visibility/i)
    })
  })

  describe('partial config override', () => {
    it('merges partial config with defaults', () => {
      const yaml = `
database:
  name: custom-db
branches:
  auto_create:
    - 'release/*'
`
      const config = parseConfig(yaml)
      expect(config.database.name).toBe('custom-db')
      expect(config.branches.auto_create).toEqual(['release/*'])
      // Other fields should use defaults
      expect(config.branches.ignore).toEqual(['dependabot/*', 'renovate/*'])
      expect(config.preview.enabled).toBe(true)
      expect(config.merge.default_strategy).toBe('manual')
    })

    it('allows empty arrays to override defaults', () => {
      const yaml = `
branches:
  ignore: []
`
      const config = parseConfig(yaml)
      expect(config.branches.ignore).toEqual([])
    })
  })

  describe('glob pattern validation', () => {
    it('accepts valid glob patterns', () => {
      const yaml = `
branches:
  auto_create:
    - 'feature/*'
    - 'feature/**'
    - 'fix-*'
    - '*.tmp'
`
      expect(() => parseConfig(yaml)).not.toThrow()
    })

    it('rejects unclosed brackets', () => {
      const yaml = `
branches:
  auto_create:
    - '[invalid'
`
      expect(() => parseConfig(yaml)).toThrow(/invalid glob/i)
    })

    it('rejects unclosed braces', () => {
      const yaml = `
branches:
  auto_create:
    - '{invalid'
`
      expect(() => parseConfig(yaml)).toThrow(/invalid glob/i)
    })
  })

  describe('ReDoS protection', () => {
    it('handles complex glob patterns without catastrophic backtracking', () => {
      // This pattern could cause ReDoS with naive regex conversion
      // Pattern like */**/*/**/* generates regex with nested quantifiers
      const config: ParqueDBGitHubConfig = {
        database: { name: 'test' },
        branches: { auto_create: ['*/**/*/**/*'], ignore: [] },
        preview: { enabled: true, ttl: '24h', visibility: 'unlisted' },
        merge: { required_check: true, default_strategy: 'manual' },
        diff: { auto_comment: true, max_entities: 100, show_samples: false },
      }

      // Create a malicious input that would cause exponential backtracking
      // This string has many segments that could match the pattern ambiguously
      const maliciousInput = 'a/' + 'b/'.repeat(25) + 'c/d/e'

      // Should complete within reasonable time (not hang)
      const startTime = Date.now()
      const result = shouldCreateBranch(config, maliciousInput)
      const elapsed = Date.now() - startTime

      // Should complete in under 100ms, not hang for seconds/minutes
      expect(elapsed).toBeLessThan(100)
      expect(result).toBe(true)
    })

    it('handles patterns with many wildcards efficiently', () => {
      const config: ParqueDBGitHubConfig = {
        database: { name: 'test' },
        branches: { auto_create: ['**/*/*/*/*/**'], ignore: [] },
        preview: { enabled: true, ttl: '24h', visibility: 'unlisted' },
        merge: { required_check: true, default_strategy: 'manual' },
        diff: { auto_comment: true, max_entities: 100, show_samples: false },
      }

      // Another potentially malicious input
      const maliciousInput = 'x/'.repeat(30) + 'y'

      const startTime = Date.now()
      const result = shouldCreateBranch(config, maliciousInput)
      const elapsed = Date.now() - startTime

      // Should complete quickly
      expect(elapsed).toBeLessThan(100)
      expect(result).toBe(true)
    })

    it('rejects overly complex patterns to prevent abuse', () => {
      // Patterns with excessive nesting should be rejected to prevent potential DoS
      // Pattern complexity is measured by counting wildcards
      const yaml = `
branches:
  auto_create:
    - '${'**/'.repeat(20)}*'
`
      expect(() => parseConfig(yaml)).toThrow(/pattern.*complex/i)
    })

    it('handles pathological patterns safely using DP algorithm', () => {
      // This test verifies that the DP-based glob matching handles patterns
      // that would cause catastrophic backtracking with naive regex.
      // The pattern a*a*a*a*a*b with input aaaa...c would hang with regex
      // but completes instantly with our DP implementation.
      // Note: pattern stays within MAX_WILDCARD_COUNT (10)
      const config: ParqueDBGitHubConfig = {
        database: { name: 'test' },
        branches: { auto_create: ['a*a*a*a*a*b'], ignore: [] },
        preview: { enabled: true, ttl: '24h', visibility: 'unlisted' },
        merge: { required_check: true, default_strategy: 'manual' },
        diff: { auto_comment: true, max_entities: 100, show_samples: false },
      }

      // This input would cause catastrophic backtracking with naive regex
      const maliciousInput = 'a'.repeat(30) + 'c'

      const startTime = Date.now()
      const result = shouldCreateBranch(config, maliciousInput)
      const elapsed = Date.now() - startTime

      // Should complete quickly (< 100ms), not hang for minutes
      expect(elapsed).toBeLessThan(100)
      expect(result).toBe(false) // Doesn't match because it ends in 'c' not 'b'
    })
  })
})
