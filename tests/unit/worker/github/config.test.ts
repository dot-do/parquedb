import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  parseConfig,
  loadConfigFromRepo,
  shouldCreateBranch,
  shouldIgnoreBranch,
  parseTTL,
  clearConfigCache,
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
})
