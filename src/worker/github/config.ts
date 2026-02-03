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
 * Config cache for repository configs
 */
const configCache = new Map<string, ParqueDBGitHubConfig>()

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
 * Validate a glob pattern for basic syntax errors
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
 * Match a branch name against a glob pattern
 * Supports * (single path segment) and ** (multiple segments)
 */
function matchGlob(pattern: string, branchName: string): boolean {
  // Convert glob pattern to regex
  // First, replace ** with a placeholder to handle it separately
  const DOUBLE_STAR_PLACEHOLDER = '\x00DOUBLESTAR\x00'
  let regex = pattern.replace(/\*\*/g, DOUBLE_STAR_PLACEHOLDER)

  // Escape special regex characters (except * which we'll handle)
  regex = regex.replace(/[.+^${}()|[\]\\]/g, '\\$&')

  // Handle * (match anything except /)
  regex = regex.replace(/\*/g, '[^/]*')

  // Handle ** (match anything including /)
  regex = regex.replace(new RegExp(DOUBLE_STAR_PLACEHOLDER.replace(/\x00/g, '\\x00'), 'g'), '.*')

  // Anchor the pattern
  regex = `^${regex}$`

  return new RegExp(regex).test(branchName)
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
 * Load config from a GitHub repository
 * Tries .github/parquedb.yml first, then .github/parquedb.yaml
 * Returns defaults if neither file exists
 */
export async function loadConfigFromRepo(owner: string, repo: string): Promise<ParqueDBGitHubConfig> {
  const cacheKey = `${owner}/${repo}`

  // Check cache first
  const cached = configCache.get(cacheKey)
  if (cached) {
    return cached
  }

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
        configCache.set(cacheKey, config)
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
  const defaults = getDefaultConfig(repo)
  configCache.set(cacheKey, defaults)
  return defaults
}

/**
 * Clear the config cache (useful for testing)
 */
export function clearConfigCache(): void {
  configCache.clear()
}
