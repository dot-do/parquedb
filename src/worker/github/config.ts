/**
 * GitHub config parser for .github/parquedb.yml configuration files
 */

import * as yaml from 'yaml'

/**
 * ParqueDB GitHub config interface
 */
export interface ParqueDBGitHubConfig {
  database: {
    name: string
  }
  branches: {
    auto_create: string[]
    ignore: string[]
  }
  preview: {
    enabled: boolean
    ttl: string
    visibility: 'public' | 'unlisted' | 'private'
  }
  merge: {
    required_check: boolean
    default_strategy: 'ours' | 'theirs' | 'newest' | 'manual'
  }
  diff: {
    auto_comment: boolean
    max_entities: number
    show_samples: boolean
  }
}

/**
 * Partial config type for parsing
 */
interface PartialConfig {
  database?: Partial<ParqueDBGitHubConfig['database']>
  branches?: Partial<ParqueDBGitHubConfig['branches']>
  preview?: Partial<ParqueDBGitHubConfig['preview']>
  merge?: Partial<ParqueDBGitHubConfig['merge']>
  diff?: Partial<ParqueDBGitHubConfig['diff']>
}

/**
 * Valid visibility values
 */
const VALID_VISIBILITY = ['public', 'unlisted', 'private'] as const

/**
 * Valid merge strategy values
 */
const VALID_MERGE_STRATEGIES = ['ours', 'theirs', 'newest', 'manual'] as const

/**
 * Cache entry with timestamp for TTL support
 */
interface ConfigCacheEntry {
  config: ParqueDBGitHubConfig
  cachedAt: number
}

/**
 * Default cache TTL in milliseconds (5 minutes)
 * Config files rarely change, but we want to pick up updates reasonably quickly
 */
const CONFIG_CACHE_TTL_MS = 5 * 60 * 1000

/**
 * Config cache for repository configs with TTL support
 *
 * Uses promise deduplication to prevent race conditions when multiple
 * concurrent requests try to load the same config simultaneously.
 *
 * The cache uses a two-phase approach:
 * 1. configCache: Stores resolved configs with timestamps for TTL checking
 * 2. configPromiseCache: Stores in-flight promises to deduplicate concurrent loads
 *
 * This prevents the "thundering herd" problem where multiple concurrent requests
 * all check the cache, find it empty or expired, and all start fetching the same config.
 */
const configCache = new Map<string, ConfigCacheEntry>()

/**
 * In-flight promise cache for deduplication
 *
 * This is the critical piece that prevents race conditions. When a load is in
 * progress, subsequent requests for the same config will receive the same
 * promise rather than starting their own fetch.
 *
 * The promise is stored synchronously BEFORE any async operation begins,
 * ensuring that even if the event loop yields during the fetch, other requests
 * will find and reuse the existing promise.
 */
const configPromiseCache = new Map<string, Promise<ParqueDBGitHubConfig>>()

/**
 * Get default config values
 */
function getDefaultConfig(repoName?: string): ParqueDBGitHubConfig {
  return {
    database: { name: repoName ?? '' },
    branches: {
      auto_create: ['feature/*', 'fix/*'],
      ignore: ['dependabot/*', 'renovate/*']
    },
    preview: {
      enabled: true,
      ttl: '24h',
      visibility: 'unlisted'
    },
    merge: {
      required_check: true,
      default_strategy: 'manual'
    },
    diff: {
      auto_comment: true,
      max_entities: 100,
      show_samples: false
    }
  }
}

/**
 * Maximum allowed ** wildcards in a pattern to prevent ReDoS
 */
const MAX_DOUBLE_STAR_COUNT = 5

/**
 * Maximum allowed total wildcards (* and **) in a pattern
 */
const MAX_WILDCARD_COUNT = 10

/**
 * Validate a glob pattern for basic syntax errors and complexity
 */
function validateGlobPattern(pattern: string): void {
  // Check for unclosed brackets
  let bracketDepth = 0
  for (let i = 0; i < pattern.length; i++) {
    const char = pattern[i]
    if (char === '[') bracketDepth++
    else if (char === ']') bracketDepth--
  }
  if (bracketDepth !== 0) {
    throw new Error(`Invalid glob pattern: unclosed bracket in "${pattern}"`)
  }

  // Check for unclosed braces
  let braceDepth = 0
  for (let i = 0; i < pattern.length; i++) {
    const char = pattern[i]
    if (char === '{') braceDepth++
    else if (char === '}') braceDepth--
  }
  if (braceDepth !== 0) {
    throw new Error(`Invalid glob pattern: unclosed brace in "${pattern}"`)
  }

  // Check for pattern complexity to prevent ReDoS attacks
  // Count ** occurrences
  const doubleStarMatches = pattern.match(/\*\*/g)
  const doubleStarCount = doubleStarMatches ? doubleStarMatches.length : 0
  if (doubleStarCount > MAX_DOUBLE_STAR_COUNT) {
    throw new Error(
      `Invalid glob pattern: too complex (${doubleStarCount} recursive wildcards, max ${MAX_DOUBLE_STAR_COUNT}) in "${pattern}"`
    )
  }

  // Count total wildcards (single * that are not part of **)
  // First replace ** with placeholder, then count remaining *
  const withoutDoubleStar = pattern.replace(/\*\*/g, '')
  const singleStarCount = (withoutDoubleStar.match(/\*/g) || []).length
  const totalWildcards = doubleStarCount + singleStarCount
  if (totalWildcards > MAX_WILDCARD_COUNT) {
    throw new Error(
      `Invalid glob pattern: too complex (${totalWildcards} wildcards, max ${MAX_WILDCARD_COUNT}) in "${pattern}"`
    )
  }
}

/**
 * Validate TTL format (e.g., 24h, 7d, 30m)
 */
function validateTTL(ttl: string): void {
  const match = ttl.match(/^(\d+)(h|d|m)?$/)
  if (!match) {
    throw new Error(`Invalid TTL format: "${ttl}". Expected format like 24h, 7d, or 30m`)
  }
}

/**
 * Parse TTL string to milliseconds
 * Supports h (hours), d (days), m (minutes)
 * Defaults to hours if no unit specified
 */
export function parseTTL(ttl: string): number {
  const match = ttl.match(/^(\d+)(h|d|m)?$/)
  if (!match) {
    throw new Error(`Invalid TTL format: "${ttl}". Expected format like 24h, 7d, or 30m`)
  }

  const value = parseInt(match[1], 10)
  const unit = match[2] || 'h' // Default to hours

  switch (unit) {
    case 'h':
      return value * 60 * 60 * 1000
    case 'd':
      return value * 24 * 60 * 60 * 1000
    case 'm':
      return value * 60 * 1000
    default:
      return value * 60 * 60 * 1000
  }
}

/**
 * Parse YAML config string and return typed config object
 */
export function parseConfig(yamlStr: string, repoName?: string): ParqueDBGitHubConfig {
  const defaults = getDefaultConfig(repoName)

  // Parse empty string as empty object
  let parsed: PartialConfig = {}
  if (yamlStr.trim()) {
    try {
      parsed = yaml.parse(yamlStr) ?? {}
    } catch (error) {
      throw new Error(`YAML parse error: ${error instanceof Error ? error.message : String(error)}`)
    }
  }

  // Merge with defaults
  const config: ParqueDBGitHubConfig = {
    database: {
      name: parsed.database?.name ?? defaults.database.name
    },
    branches: {
      auto_create: parsed.branches?.auto_create !== undefined
        ? parsed.branches.auto_create
        : defaults.branches.auto_create,
      ignore: parsed.branches?.ignore !== undefined
        ? parsed.branches.ignore
        : defaults.branches.ignore
    },
    preview: {
      enabled: parsed.preview?.enabled ?? defaults.preview.enabled,
      ttl: parsed.preview?.ttl ?? defaults.preview.ttl,
      visibility: parsed.preview?.visibility ?? defaults.preview.visibility
    },
    merge: {
      required_check: parsed.merge?.required_check ?? defaults.merge.required_check,
      default_strategy: parsed.merge?.default_strategy ?? defaults.merge.default_strategy
    },
    diff: {
      auto_comment: parsed.diff?.auto_comment ?? defaults.diff.auto_comment,
      max_entities: parsed.diff?.max_entities ?? defaults.diff.max_entities,
      show_samples: parsed.diff?.show_samples ?? defaults.diff.show_samples
    }
  }

  // Validate branch patterns
  for (const pattern of config.branches.auto_create) {
    validateGlobPattern(pattern)
  }
  for (const pattern of config.branches.ignore) {
    validateGlobPattern(pattern)
  }

  // Validate TTL
  validateTTL(config.preview.ttl)

  // Validate visibility
  if (!VALID_VISIBILITY.includes(config.preview.visibility)) {
    throw new Error(`Invalid visibility: "${config.preview.visibility}". Must be one of: ${VALID_VISIBILITY.join(', ')}`)
  }

  // Validate merge strategy
  if (!VALID_MERGE_STRATEGIES.includes(config.merge.default_strategy)) {
    throw new Error(`Invalid merge strategy: "${config.merge.default_strategy}". Must be one of: ${VALID_MERGE_STRATEGIES.join(', ')}`)
  }

  // Validate max_entities
  if (config.diff.max_entities <= 0) {
    throw new Error('max_entities must be positive')
  }

  return config
}

/**
 * Match a branch name against a glob pattern using a safe non-backtracking algorithm.
 * Supports * (single path segment) and ** (multiple segments)
 *
 * This implementation uses dynamic programming to avoid ReDoS vulnerabilities
 * that can occur with regex-based glob matching.
 */
function matchGlob(pattern: string, branchName: string): boolean {
  // Split pattern into tokens: literals, *, **
  const tokens = tokenizeGlob(pattern)

  // Use DP to match - dp[i][j] means tokens[0..i-1] matches branchName[0..j-1]
  // This avoids the exponential backtracking of regex
  return matchTokens(tokens, branchName)
}

/**
 * Token types for glob patterns
 */
type GlobToken =
  | { type: 'literal'; value: string }
  | { type: 'star' }      // * - matches any single segment (no /)
  | { type: 'doubleStar' } // ** - matches any number of segments

/**
 * Tokenize a glob pattern into a sequence of tokens
 */
function tokenizeGlob(pattern: string): GlobToken[] {
  const tokens: GlobToken[] = []
  let i = 0

  while (i < pattern.length) {
    if (pattern[i] === '*') {
      if (pattern[i + 1] === '*') {
        tokens.push({ type: 'doubleStar' })
        i += 2
      } else {
        tokens.push({ type: 'star' })
        i++
      }
    } else {
      // Collect literal characters
      let literal = ''
      while (i < pattern.length && pattern[i] !== '*') {
        literal += pattern[i]
        i++
      }
      if (literal) {
        tokens.push({ type: 'literal', value: literal })
      }
    }
  }

  return tokens
}

/**
 * Match tokens against a string using dynamic programming
 * Returns true if the tokens match the entire string
 */
function matchTokens(tokens: GlobToken[], str: string): boolean {
  // dp[i][j] = true if tokens[0..i-1] matches str[0..j-1]
  // To avoid O(n*m) space, we use two rows
  const n = tokens.length
  const m = str.length

  // prev[j] = dp[i-1][j], curr[j] = dp[i][j]
  let prev = new Array(m + 1).fill(false)
  let curr = new Array(m + 1).fill(false)

  // Base case: empty pattern matches empty string
  prev[0] = true

  // Handle leading ** tokens (they can match empty string)
  for (let i = 0; i < n; i++) {
    if (tokens[i].type === 'doubleStar') {
      prev[0] = true
    } else {
      break
    }
  }

  for (let i = 1; i <= n; i++) {
    const token = tokens[i - 1]
    curr.fill(false)

    if (token.type === 'literal') {
      // Literal must match exactly at this position
      const lit = token.value
      for (let j = lit.length; j <= m; j++) {
        if (prev[j - lit.length] && str.slice(j - lit.length, j) === lit) {
          curr[j] = true
        }
      }
    } else if (token.type === 'star') {
      // * matches any sequence not containing /
      for (let j = 0; j <= m; j++) {
        if (prev[j]) {
          // Match zero or more non-slash characters starting at j
          let k = j
          curr[k] = true
          while (k < m && str[k] !== '/') {
            k++
            curr[k] = true
          }
        }
      }
    } else if (token.type === 'doubleStar') {
      // ** matches any sequence including /
      // Once we can match up to some position, we can match to any later position
      let canMatch = false
      for (let j = 0; j <= m; j++) {
        if (prev[j]) {
          canMatch = true
        }
        curr[j] = canMatch
      }
    }

    // Swap prev and curr
    ;[prev, curr] = [curr, prev]
  }

  return prev[m]
}

/**
 * Check if a branch should be auto-created based on config patterns
 * Note: Returns false if branch matches any ignore pattern
 */
export function shouldCreateBranch(config: ParqueDBGitHubConfig, branchName: string): boolean {
  // Ignore takes precedence
  if (shouldIgnoreBranch(config, branchName)) {
    return false
  }

  // Check if matches any auto_create pattern
  for (const pattern of config.branches.auto_create) {
    if (matchGlob(pattern, branchName)) {
      return true
    }
  }

  return false
}

/**
 * Check if a branch should be ignored based on config patterns
 */
export function shouldIgnoreBranch(config: ParqueDBGitHubConfig, branchName: string): boolean {
  for (const pattern of config.branches.ignore) {
    if (matchGlob(pattern, branchName)) {
      return true
    }
  }
  return false
}

/**
 * Internal function to actually load config from GitHub
 * This is called only once per cache key, protected by promise deduplication
 */
async function loadConfigFromRepoInternal(owner: string, repo: string): Promise<ParqueDBGitHubConfig> {
  const paths = [
    `.github/parquedb.yml`,
    `.github/parquedb.yaml`
  ]

  for (const path of paths) {
    const url = `https://raw.githubusercontent.com/${owner}/${repo}/main/${path}`
    const response = await fetch(url, {
      headers: {
        'Accept': 'text/plain'
      }
    })

    if (response.ok) {
      const content = await response.text()
      try {
        const config = parseConfig(content, repo)
        return config
      } catch (error) {
        throw new Error(`YAML error in ${path}: ${error instanceof Error ? error.message : String(error)}`)
      }
    }

    // If not 404, it's an error we should report
    if (response.status !== 404) {
      throw new Error(`Failed to load config from ${path}: ${response.status}`)
    }
  }

  // Return defaults if no config file found
  return getDefaultConfig(repo)
}

/**
 * Load config from a GitHub repository
 * Tries .github/parquedb.yml first, then .github/parquedb.yaml
 * Returns defaults if neither file exists
 *
 * Uses promise deduplication to prevent race conditions when multiple
 * concurrent requests try to load the same config simultaneously
 */
export async function loadConfigFromRepo(owner: string, repo: string): Promise<ParqueDBGitHubConfig> {
  const cacheKey = `${owner}/${repo}`
  const now = Date.now()

  // Check result cache first (fastest path)
  // Use TTL to ensure we pick up config changes reasonably quickly
  const cached = configCache.get(cacheKey)
  if (cached && (now - cached.cachedAt) < CONFIG_CACHE_TTL_MS) {
    return cached.config
  }

  // Check if there's already an in-flight request for this key
  // This handles the race condition where multiple concurrent requests
  // all find the cache empty/expired at the same time
  const existingPromise = configPromiseCache.get(cacheKey)
  if (existingPromise) {
    return existingPromise
  }

  // CRITICAL: Store the promise BEFORE any async operation starts.
  // This is the key to preventing the race condition - all code from
  // here until configPromiseCache.set() executes synchronously.
  const loadPromise = loadConfigFromRepoInternal(owner, repo)
    .then(config => {
      // On success, cache the result with timestamp and clean up the promise cache
      configCache.set(cacheKey, { config, cachedAt: Date.now() })
      configPromiseCache.delete(cacheKey)
      return config
    })
    .catch(error => {
      // On error, clean up the promise cache so retry is possible
      // Don't update configCache - keep stale entry if it exists
      configPromiseCache.delete(cacheKey)
      throw error
    })

  // This MUST happen synchronously after creating the promise chain above
  configPromiseCache.set(cacheKey, loadPromise)

  return loadPromise
}

/**
 * Invalidate the cache for a specific repository
 *
 * Use this when you know a repo's config has changed (e.g., after a push event)
 * to force the next request to fetch fresh data.
 *
 * Note: If there's an in-flight request for this repo, subsequent requests
 * will still receive that result. The invalidation takes effect for requests
 * after the in-flight one completes.
 *
 * @param owner - Repository owner
 * @param repo - Repository name
 */
export function invalidateConfigCache(owner: string, repo: string): void {
  const cacheKey = `${owner}/${repo}`
  configCache.delete(cacheKey)
  // Note: We don't delete from configPromiseCache because that could cause
  // duplicate fetches. The in-flight request will complete and cache its result,
  // which will then be immediately stale and refetched on the next request.
}

/**
 * Clear the entire config cache (useful for testing)
 * Clears both the result cache and the in-flight promise cache
 */
export function clearConfigCache(): void {
  configCache.clear()
  configPromiseCache.clear()
}

/**
 * Get the current cache TTL in milliseconds (useful for testing)
 */
export function getConfigCacheTTL(): number {
  return CONFIG_CACHE_TTL_MS
}
