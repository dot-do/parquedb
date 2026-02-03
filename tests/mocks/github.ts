/**
 * GitHub Service Mock Factories
 *
 * Provides mock implementations of GitHub-related services for testing webhooks
 * and GitHub integration features.
 */

import { vi, type Mock } from 'vitest'

// =============================================================================
// Types
// =============================================================================

/**
 * Mock DatabaseIndex service for GitHub integration
 */
export interface MockGitHubDatabaseIndex {
  getUserByInstallation: Mock<[number], Promise<{ id: string } | null>>
  linkGitHubInstallation: Mock<[string, number, Record<string, unknown>], Promise<void>>
  unlinkGitHubInstallation: Mock<[number], Promise<boolean>>
  getDatabaseByRepo: Mock<[string], Promise<MockGitHubDatabase | null>>
}

/**
 * Database with GitHub config
 */
export interface MockGitHubDatabase {
  id: string
  name?: string
  defaultBranch?: string
  previewUrl?: string
  config?: {
    preview?: { enabled?: boolean }
    branches?: {
      auto_create?: string[]
      auto_delete?: boolean
    }
    merge?: {
      strategy?: 'ours' | 'theirs'
    }
  }
}

/**
 * Mock ParqueDB branch service
 */
export interface MockParqueDBBranch {
  create: Mock<[string, { from: string }], Promise<{ name: string }>>
  delete: Mock<[string], Promise<boolean>>
  exists: Mock<[string], Promise<boolean>>
}

/**
 * Mock ParqueDB service for GitHub integration
 */
export interface MockParqueDBService {
  branch: MockParqueDBBranch
  merge: Mock<[string, string, unknown?], Promise<{ conflicts: unknown[] }>>
  diff: Mock<[string, string], Promise<{ entities: Record<string, unknown>; relationships: Record<string, unknown> }>>
}

/**
 * Mock Octokit checks API
 */
export interface MockOctokitChecks {
  create: Mock<[Record<string, unknown>], Promise<{ data: { id: number } }>>
  update: Mock<[Record<string, unknown>], Promise<{ data: { id: number } }>>
}

/**
 * Mock Octokit issues API
 */
export interface MockOctokitIssues {
  createComment: Mock<[Record<string, unknown>], Promise<{ data: { id: number } }>>
  updateComment: Mock<[Record<string, unknown>], Promise<{ data: { id: number } }>>
  listComments: Mock<[Record<string, unknown>], Promise<{ data: Array<{ id: number; body: string }> }>>
}

/**
 * Mock Octokit reactions API
 */
export interface MockOctokitReactions {
  createForIssueComment: Mock<[Record<string, unknown>], Promise<{ data: { id: number } }>>
}

/**
 * Mock Octokit client
 */
export interface MockOctokitClient {
  rest: {
    checks: MockOctokitChecks
    issues: MockOctokitIssues
    reactions: MockOctokitReactions
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a mock GitHub DatabaseIndex service
 *
 * @returns Mock DatabaseIndex for GitHub webhooks
 *
 * @example
 * ```typescript
 * const index = createMockGitHubDatabaseIndex()
 * index.getUserByInstallation.mockResolvedValue({ id: 'user_123' })
 * ```
 */
export function createMockGitHubDatabaseIndex(): MockGitHubDatabaseIndex {
  return {
    getUserByInstallation: vi.fn().mockResolvedValue(null),
    linkGitHubInstallation: vi.fn().mockResolvedValue(undefined),
    unlinkGitHubInstallation: vi.fn().mockResolvedValue(true),
    getDatabaseByRepo: vi.fn().mockResolvedValue(null),
  }
}

/**
 * Create a mock ParqueDB service for GitHub integration
 *
 * @returns Mock ParqueDB service
 *
 * @example
 * ```typescript
 * const parquedb = createMockParqueDBService()
 * await handleCreate(payload, index, parquedb)
 * expect(parquedb.branch.create).toHaveBeenCalled()
 * ```
 */
export function createMockParqueDBService(): MockParqueDBService {
  return {
    branch: {
      create: vi.fn().mockResolvedValue({ name: 'branch' }),
      delete: vi.fn().mockResolvedValue(true),
      exists: vi.fn().mockResolvedValue(true),
    },
    merge: vi.fn().mockResolvedValue({ conflicts: [] }),
    diff: vi.fn().mockResolvedValue({ entities: {}, relationships: {} }),
  }
}

/**
 * Create a mock Octokit client
 *
 * @returns Mock Octokit client
 *
 * @example
 * ```typescript
 * const octokit = createMockOctokitClient()
 * octokit.rest.issues.listComments.mockResolvedValue({
 *   data: [{ id: 1, body: 'Existing comment' }]
 * })
 * ```
 */
export function createMockOctokitClient(): MockOctokitClient {
  return {
    rest: {
      checks: {
        create: vi.fn().mockResolvedValue({ data: { id: 1 } }),
        update: vi.fn().mockResolvedValue({ data: { id: 1 } }),
      },
      issues: {
        createComment: vi.fn().mockResolvedValue({ data: { id: 1 } }),
        updateComment: vi.fn().mockResolvedValue({ data: { id: 1 } }),
        listComments: vi.fn().mockResolvedValue({ data: [] }),
      },
      reactions: {
        createForIssueComment: vi.fn().mockResolvedValue({ data: { id: 1 } }),
      },
    },
  }
}

// =============================================================================
// Payload Factories
// =============================================================================

/**
 * Create an installation created payload
 */
export function createInstallationCreatedPayload(overrides: {
  installationId?: number
  login?: string
  type?: 'User' | 'Organization'
  repositories?: Array<{ full_name: string }>
} = {}): {
  action: 'created'
  installation: { id: number; account: { login: string; type: string } }
  repositories?: Array<{ full_name: string }>
} {
  return {
    action: 'created',
    installation: {
      id: overrides.installationId ?? 12345,
      account: {
        login: overrides.login ?? 'testuser',
        type: overrides.type ?? 'User',
      },
    },
    repositories: overrides.repositories,
  }
}

/**
 * Create an installation deleted payload
 */
export function createInstallationDeletedPayload(installationId: number = 12345): {
  action: 'deleted'
  installation: { id: number }
} {
  return {
    action: 'deleted',
    installation: { id: installationId },
  }
}

/**
 * Create a branch create payload
 */
export function createBranchCreatePayload(overrides: {
  ref?: string
  refType?: 'branch' | 'tag'
  repository?: string
  installationId?: number
} = {}): {
  ref: string
  ref_type: 'branch' | 'tag'
  repository: { full_name: string }
  installation?: { id: number }
} {
  return {
    ref: overrides.ref ?? 'feature/new-feature',
    ref_type: overrides.refType ?? 'branch',
    repository: { full_name: overrides.repository ?? 'testuser/mydb' },
    installation: overrides.installationId ? { id: overrides.installationId } : undefined,
  }
}

/**
 * Create a branch delete payload
 */
export function createBranchDeletePayload(overrides: {
  ref?: string
  refType?: 'branch' | 'tag'
  repository?: string
  installationId?: number
} = {}): {
  ref: string
  ref_type: 'branch' | 'tag'
  repository: { full_name: string }
  installation?: { id: number }
} {
  return {
    ref: overrides.ref ?? 'feature/old-feature',
    ref_type: overrides.refType ?? 'branch',
    repository: { full_name: overrides.repository ?? 'testuser/mydb' },
    installation: overrides.installationId ? { id: overrides.installationId } : undefined,
  }
}

/**
 * Create a pull request payload
 */
export function createPullRequestPayload(overrides: {
  action?: 'opened' | 'synchronize' | 'closed'
  number?: number
  headRef?: string
  headSha?: string
  baseRef?: string
  merged?: boolean
  mergeCommitSha?: string
  repository?: string
  installationId?: number
} = {}): {
  action: string
  number: number
  pull_request: {
    head: { ref: string; sha?: string }
    base: { ref: string }
    merged?: boolean
    merge_commit_sha?: string
  }
  repository: { full_name: string }
  installation?: { id: number }
} {
  return {
    action: overrides.action ?? 'opened',
    number: overrides.number ?? 42,
    pull_request: {
      head: {
        ref: overrides.headRef ?? 'feature/test',
        sha: overrides.headSha,
      },
      base: { ref: overrides.baseRef ?? 'main' },
      merged: overrides.merged,
      merge_commit_sha: overrides.mergeCommitSha,
    },
    repository: { full_name: overrides.repository ?? 'testuser/mydb' },
    installation: overrides.installationId ? { id: overrides.installationId } : undefined,
  }
}
