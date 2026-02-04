/**
 * GitHub Webhook Handlers for ParqueDB
 *
 * Handles GitHub webhooks to sync Git branches with ParqueDB database branches.
 * Supports:
 * - Installation events (link/unlink GitHub installations)
 * - Branch creation/deletion
 * - Pull request lifecycle (preview branches, merge, comments)
 *
 * @module
 */

import { logger } from '../../utils/logger'
import { createSafeRegex } from '../../utils/safe-regex'

// =============================================================================
// Types
// =============================================================================

/**
 * Data for linking a GitHub installation
 */
export interface GitHubInstallationLinkData {
  readonly login: string
  readonly type: string
  readonly repositories?: readonly string[] | undefined
}

/**
 * Interface for the DatabaseIndex service
 */
export interface DatabaseIndexService {
  readonly getUserByInstallation: (installationId: number) => Promise<{ readonly id: string } | null>
  readonly linkGitHubInstallation: (
    userId: string,
    installationId: number,
    data: GitHubInstallationLinkData
  ) => Promise<void>
  readonly unlinkGitHubInstallation: (installationId: number) => Promise<boolean>
  readonly getDatabaseByRepo: (fullName: string) => Promise<DatabaseInfo | null>
}

/**
 * Branch configuration options
 */
export interface BranchConfig {
  readonly auto_create?: readonly string[] | undefined
  readonly auto_delete?: boolean | undefined
  readonly ignore?: readonly string[] | undefined
}

/**
 * Preview configuration options
 */
export interface PreviewConfig {
  readonly enabled?: boolean | undefined
}

/**
 * Merge configuration options
 */
export interface MergeConfig {
  readonly strategy?: string | undefined
}

/**
 * Database configuration
 */
export interface DatabaseConfig {
  readonly branches?: BranchConfig | undefined
  readonly preview?: PreviewConfig | undefined
  readonly merge?: MergeConfig | undefined
}

/**
 * Database info returned from index
 */
export interface DatabaseInfo {
  readonly id: string
  readonly defaultBranch?: string | undefined
  readonly previewUrl?: string | undefined
  readonly config?: DatabaseConfig | undefined
}

/**
 * Branch creation options
 */
export interface BranchCreateOptions {
  readonly from: string
}

/**
 * Merge options
 */
export interface MergeServiceOptions {
  readonly strategy?: string | undefined
}

/**
 * Merge conflict result
 */
export interface MergeConflictResult {
  readonly entityId: string
  readonly field: string
}

/**
 * Merge result
 */
export interface MergeServiceResult {
  readonly conflicts: readonly MergeConflictResult[]
}

/**
 * Diff change counts
 */
export interface DiffChangeCounts {
  readonly added?: number | undefined
  readonly modified?: number | undefined
  readonly deleted?: number | undefined
}

/**
 * Diff result
 */
export interface DiffResult {
  readonly entities: DiffChangeCounts
  readonly relationships: DiffChangeCounts
}

/**
 * Branch service interface
 */
export interface BranchService {
  readonly create: (name: string, options: BranchCreateOptions) => Promise<{ readonly name: string }>
  readonly delete: (name: string) => Promise<boolean>
  readonly exists: (name: string) => Promise<boolean>
}

/**
 * Interface for ParqueDB branch operations
 */
export interface ParqueDBService {
  readonly branch: BranchService
  readonly merge: (
    source: string,
    target: string,
    options?: MergeServiceOptions | undefined
  ) => Promise<MergeServiceResult>
  readonly diff: (source: string, target: string) => Promise<DiffResult>
}

/**
 * Check output parameters
 */
export interface CheckOutput {
  readonly title: string
  readonly summary: string
}

/**
 * Check create parameters
 */
export interface CheckCreateParams {
  readonly owner: string
  readonly repo: string
  readonly name: string
  readonly head_sha: string
  readonly status: string
  readonly output?: CheckOutput | undefined
}

/**
 * Check update parameters
 */
export interface CheckUpdateParams {
  readonly owner: string
  readonly repo: string
  readonly check_run_id: number
  readonly status?: string | undefined
  readonly conclusion?: string | undefined
  readonly output?: CheckOutput | undefined
}

/**
 * Comment create parameters
 */
export interface CommentCreateParams {
  readonly owner: string
  readonly repo: string
  readonly issue_number: number
  readonly body: string
}

/**
 * Comment update parameters
 */
export interface CommentUpdateParams {
  readonly owner: string
  readonly repo: string
  readonly comment_id: number
  readonly body: string
}

/**
 * Comment list parameters
 */
export interface CommentListParams {
  readonly owner: string
  readonly repo: string
  readonly issue_number: number
}

/**
 * Comment data
 */
export interface CommentData {
  readonly id: number
  readonly body: string
}

/**
 * Reaction create parameters
 */
export interface ReactionCreateParams {
  readonly owner: string
  readonly repo: string
  readonly comment_id: number
  readonly content: string
}

/**
 * API response with data
 */
export interface ApiResponse<T> {
  readonly data: T
}

/**
 * Interface for Octokit client
 */
export interface OctokitClient {
  readonly rest: {
    readonly checks: {
      readonly create: (params: CheckCreateParams) => Promise<ApiResponse<{ readonly id: number }>>
      readonly update: (params: CheckUpdateParams) => Promise<ApiResponse<{ readonly id: number }>>
    }
    readonly issues: {
      readonly createComment: (params: CommentCreateParams) => Promise<ApiResponse<{ readonly id: number }>>
      readonly updateComment: (params: CommentUpdateParams) => Promise<ApiResponse<{ readonly id: number }>>
      readonly listComments: (params: CommentListParams) => Promise<ApiResponse<readonly CommentData[]>>
    }
    readonly reactions: {
      readonly createForIssueComment: (params: ReactionCreateParams) => Promise<ApiResponse<{ readonly id: number }>>
    }
  }
}

// =============================================================================
// Webhook Signature Verification
// =============================================================================

/**
 * Verify GitHub webhook signature using HMAC-SHA256
 *
 * @param payload - Raw request body
 * @param signature - X-Hub-Signature-256 header value
 * @param secret - Webhook secret
 * @returns true if signature is valid
 * @throws Error if signature header is missing
 */
export async function verifyWebhookSignature(
  payload: string,
  signature: string | null,
  secret: string
): Promise<boolean> {
  if (!signature) {
    throw new Error('Missing signature header')
  }

  if (!signature.startsWith('sha256=')) {
    return false
  }

  const signatureHex = signature.slice('sha256='.length)
  const encoder = new TextEncoder()
  const keyData = encoder.encode(secret)
  const messageData = encoder.encode(payload)

  const key = await crypto.subtle.importKey(
    'raw',
    keyData,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  )

  const signatureBuffer = await crypto.subtle.sign('HMAC', key, messageData)
  const signatureArray = new Uint8Array(signatureBuffer)

  const expectedHex = Array.from(signatureArray)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('')

  // Constant-time comparison to prevent timing attacks
  return timingSafeEqual(signatureHex, expectedHex)
}

/**
 * Constant-time string comparison to prevent timing attacks
 */
function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) {
    return false
  }

  let result = 0
  for (let i = 0; i < a.length; i++) {
    result |= a.charCodeAt(i) ^ b.charCodeAt(i)
  }

  return result === 0
}

// =============================================================================
// Installation Handlers
// =============================================================================

/**
 * Installation account information
 */
export interface InstallationAccount {
  readonly login: string
  readonly type: 'User' | 'Organization'
}

/**
 * Installation information
 */
export interface InstallationInfo {
  readonly id: number
  readonly account: InstallationAccount
}

/**
 * Repository reference
 */
export interface RepositoryRef {
  readonly full_name: string
}

/**
 * Payload for installation.created webhook
 */
export interface InstallationCreatedPayload {
  readonly action: 'created'
  readonly installation: InstallationInfo
  readonly repositories?: readonly RepositoryRef[] | undefined
}

/**
 * Handle installation.created webhook
 *
 * Links the GitHub installation to a ParqueDB user account.
 */
export async function handleInstallationCreated(
  payload: InstallationCreatedPayload,
  databaseIndex: DatabaseIndexService
): Promise<void> {
  const { installation, repositories } = payload

  // Try to find the ParqueDB user by installation
  const user = await databaseIndex.getUserByInstallation(installation.id)

  if (!user) {
    // User not found - log warning but don't throw
    logger.warn(
      `[GitHub] No ParqueDB user found for installation ${installation.id} (${installation.account.login})`
    )
    return
  }

  // Link the installation to the user
  await databaseIndex.linkGitHubInstallation(user.id, installation.id, {
    login: installation.account.login,
    type: installation.account.type,
    repositories: repositories?.map((r) => r.full_name),
  })
}

/**
 * Payload for installation.deleted webhook
 */
export interface InstallationDeletedPayload {
  readonly action: 'deleted'
  readonly installation: {
    readonly id: number
  }
}

/**
 * Handle installation.deleted webhook
 *
 * Unlinks the GitHub installation from the ParqueDB account.
 */
export async function handleInstallationDeleted(
  payload: InstallationDeletedPayload,
  databaseIndex: DatabaseIndexService
): Promise<void> {
  // Unlink the installation - this is idempotent
  await databaseIndex.unlinkGitHubInstallation(payload.installation.id)
}

// =============================================================================
// Branch Handlers
// =============================================================================

/**
 * Default branch patterns to ignore
 */
const DEFAULT_IGNORE_PATTERNS = ['dependabot/*', 'renovate/*', 'snyk-*']

/**
 * Protected branch names that cannot be deleted
 */
const PROTECTED_BRANCHES = ['main', 'master']

/**
 * Payload for create (branch/tag) webhook
 */
export interface CreatePayload {
  readonly ref: string
  readonly ref_type: 'branch' | 'tag'
  readonly repository: RepositoryRef
  readonly installation?: {
    readonly id: number
  } | undefined
}

/**
 * Handle create (branch) webhook
 *
 * Creates a database branch matching the git branch.
 */
export async function handleCreate(
  payload: CreatePayload,
  databaseIndex: DatabaseIndexService,
  parqueDB: ParqueDBService
): Promise<void> {
  // Ignore tag creation events
  if (payload.ref_type !== 'branch') {
    return
  }

  const { ref, repository } = payload

  // Get database for this repo
  const db = await databaseIndex.getDatabaseByRepo(repository.full_name)
  if (!db) {
    return
  }

  // Check ignore patterns
  const ignorePatterns = db.config?.branches?.ignore || DEFAULT_IGNORE_PATTERNS
  if (matchesPattern(ref, ignorePatterns)) {
    return
  }

  // Check auto_create patterns if configured
  const autoCreatePatterns = db.config?.branches?.auto_create
  if (autoCreatePatterns && autoCreatePatterns.length > 0) {
    if (!matchesPattern(ref, autoCreatePatterns)) {
      return
    }
  }

  // Create the database branch
  const baseBranch = db.defaultBranch || 'main'
  await parqueDB.branch.create(ref, { from: baseBranch })
}

/**
 * Payload for delete (branch/tag) webhook
 */
export interface DeletePayload {
  readonly ref: string
  readonly ref_type: 'branch' | 'tag'
  readonly repository: RepositoryRef
  readonly installation?: {
    readonly id: number
  } | undefined
}

/**
 * Handle delete (branch) webhook
 *
 * Deletes the corresponding database branch.
 */
export async function handleDelete(
  payload: DeletePayload,
  databaseIndex: DatabaseIndexService,
  parqueDB: ParqueDBService
): Promise<void> {
  // Ignore tag deletion events
  if (payload.ref_type !== 'branch') {
    return
  }

  const { ref, repository } = payload

  // Protect main/master branches
  if (PROTECTED_BRANCHES.includes(ref)) {
    return
  }

  // Get database for this repo
  const db = await databaseIndex.getDatabaseByRepo(repository.full_name)
  if (!db) {
    return
  }

  // Check if branch exists before deleting
  const exists = await parqueDB.branch.exists(ref)
  if (!exists) {
    return
  }

  // Delete the database branch
  await parqueDB.branch.delete(ref)
}

// =============================================================================
// Pull Request Handlers
// =============================================================================

/**
 * Pull request branch reference
 */
export interface PullRequestBranchRef {
  readonly ref: string
  readonly sha?: string | undefined
}

/**
 * Pull request base reference
 */
export interface PullRequestBaseRef {
  readonly ref: string
}

/**
 * Pull request data
 */
export interface PullRequestData {
  readonly merged?: boolean | undefined
  readonly head: PullRequestBranchRef
  readonly base: PullRequestBaseRef
  readonly merge_commit_sha?: string | undefined
}

/**
 * Payload for pull_request webhook
 */
export interface PullRequestPayload {
  readonly action: 'opened' | 'synchronize' | 'closed'
  readonly number: number
  readonly pull_request: PullRequestData
  readonly repository: RepositoryRef
  readonly installation?: {
    readonly id: number
  } | undefined
}

/**
 * Handle pull_request.opened webhook
 *
 * Creates a preview branch and posts initial diff comment.
 */
export async function handlePullRequestOpened(
  payload: PullRequestPayload,
  databaseIndex: DatabaseIndexService,
  parqueDB: ParqueDBService,
  octokit: OctokitClient
): Promise<void> {
  const { number, pull_request, repository } = payload
  const [owner, repo] = repository.full_name.split('/')

  // Get database for this repo
  const db = await databaseIndex.getDatabaseByRepo(repository.full_name)
  if (!db) {
    return
  }

  // Check if preview is disabled
  if (db.config?.preview?.enabled === false) {
    return
  }

  // Create preview branch
  const previewBranch = `pr-${number}`
  await parqueDB.branch.create(previewBranch, { from: pull_request.head.ref })

  // Get diff
  const diff = await parqueDB.diff(pull_request.head.ref, pull_request.base.ref)

  // Build comment body
  const commentBody = buildDiffComment(diff, db.previewUrl, previewBranch)

  // Post diff comment
  await octokit.rest.issues.createComment({
    owner: owner!,
    repo: repo!,
    issue_number: number,
    body: commentBody,
  })

  // Create merge check run
  await octokit.rest.checks.create({
    owner: owner!,
    repo: repo!,
    name: 'ParqueDB Merge Check',
    head_sha: pull_request.head.sha || '',
    status: 'in_progress',
    output: {
      title: 'Checking database merge compatibility',
      summary: 'Analyzing database changes...',
    },
  })
}

/**
 * Handle pull_request.synchronize webhook
 *
 * Updates the diff comment and re-runs merge check.
 */
export async function handlePullRequestSynchronize(
  payload: PullRequestPayload,
  databaseIndex: DatabaseIndexService,
  parqueDB: ParqueDBService,
  octokit: OctokitClient
): Promise<void> {
  const { number, pull_request, repository } = payload
  const [owner, repo] = repository.full_name.split('/')

  // Get database for this repo
  const db = await databaseIndex.getDatabaseByRepo(repository.full_name)
  if (!db) {
    return
  }

  // Get diff
  const diff = await parqueDB.diff(pull_request.head.ref, pull_request.base.ref)

  // Build comment body
  const commentBody = buildDiffComment(diff, db.previewUrl, `pr-${number}`)

  // Find existing comment
  const comments = await octokit.rest.issues.listComments({
    owner: owner!,
    repo: repo!,
    issue_number: number,
  })

  const existingComment = comments.data.find((c) =>
    c.body.includes('## \u{1F5C4}\uFE0F Database Changes')
  )

  if (existingComment) {
    // Update existing comment
    await octokit.rest.issues.updateComment({
      owner: owner!,
      repo: repo!,
      comment_id: existingComment.id,
      body: commentBody,
    })
  } else {
    // Create new comment
    await octokit.rest.issues.createComment({
      owner: owner!,
      repo: repo!,
      issue_number: number,
      body: commentBody,
    })
  }

  // Create merge check run
  await octokit.rest.checks.create({
    owner: owner!,
    repo: repo!,
    name: 'ParqueDB Merge Check',
    head_sha: pull_request.head.sha || '',
    status: 'in_progress',
    output: {
      title: 'Checking database merge compatibility',
      summary: 'Analyzing database changes...',
    },
  })
}

/**
 * Handle pull_request.closed webhook
 *
 * Merges database branch if PR was merged, or cleans up preview branch.
 */
export async function handlePullRequestClosed(
  payload: PullRequestPayload,
  databaseIndex: DatabaseIndexService,
  parqueDB: ParqueDBService,
  octokit: OctokitClient
): Promise<void> {
  const { number, pull_request, repository } = payload
  const [owner, repo] = repository.full_name.split('/')
  const previewBranch = `pr-${number}`

  // Get database for this repo
  const db = await databaseIndex.getDatabaseByRepo(repository.full_name)
  if (!db) {
    return
  }

  if (pull_request.merged) {
    // PR was merged - merge database branch
    const mergeOptions = db.config?.merge?.strategy
      ? { strategy: db.config.merge.strategy }
      : undefined

    const result = await parqueDB.merge(
      pull_request.head.ref,
      pull_request.base.ref,
      mergeOptions
    )

    // Delete preview branch
    const previewExists = await parqueDB.branch.exists(previewBranch)
    if (previewExists) {
      await parqueDB.branch.delete(previewBranch)
    }

    // Delete feature branch if auto_delete is enabled
    if (db.config?.branches?.auto_delete) {
      await parqueDB.branch.delete(pull_request.head.ref)
    }

    // Post confirmation comment
    if (result.conflicts.length > 0) {
      const conflictList = result.conflicts
        .map((c) => `- Entity \`${c.entityId}\`: field \`${c.field}\``)
        .join('\n')

      await octokit.rest.issues.createComment({
        owner: owner!,
        repo: repo!,
        issue_number: number,
        body: `## \u26A0\uFE0F Merge conflicts detected\n\nThe following conflicts were found during the database merge:\n\n${conflictList}\n\nPlease review and resolve these conflicts manually.`,
      })
    } else {
      await octokit.rest.issues.createComment({
        owner: owner!,
        repo: repo!,
        issue_number: number,
        body: `## \u2705 Database merged successfully\n\nThe database branch \`${pull_request.head.ref}\` has been merged into \`${pull_request.base.ref}\`.`,
      })
    }
  } else {
    // PR was closed without merging - just clean up preview branch
    const previewExists = await parqueDB.branch.exists(previewBranch)
    if (previewExists) {
      await parqueDB.branch.delete(previewBranch)
    }

    // Post closure comment
    await octokit.rest.issues.createComment({
      owner: owner!,
      repo: repo!,
      issue_number: number,
      body: `## \u{1F9F9} Preview branch cleaned up\n\nThe preview database branch \`${previewBranch}\` has been deleted.`,
    })
  }
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Check if a string matches any of the given glob patterns
 */
function matchesPattern(value: string, patterns: readonly string[]): boolean {
  for (const pattern of patterns) {
    if (pattern.endsWith('*')) {
      // Prefix match
      const prefix = pattern.slice(0, -1)
      if (value.startsWith(prefix)) {
        return true
      }
    } else if (pattern.startsWith('*')) {
      // Suffix match
      const suffix = pattern.slice(1)
      if (value.endsWith(suffix)) {
        return true
      }
    } else if (pattern.includes('*')) {
      // Convert glob to regex - escape special regex characters first
      const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&')
      const regexPattern = '^' + escaped.replace(/\*/g, '.*').replace(/\?/g, '.') + '$'
      const regex = createSafeRegex(regexPattern)
      if (regex.test(value)) {
        return true
      }
    } else {
      // Exact match
      if (value === pattern) {
        return true
      }
    }
  }
  return false
}

/**
 * Build the diff comment body
 */
function buildDiffComment(
  diff: {
    entities: { added?: number | undefined; modified?: number | undefined; deleted?: number | undefined }
    relationships: { added?: number | undefined; modified?: number | undefined; deleted?: number | undefined }
  },
  previewUrl?: string,
  previewBranch?: string
): string {
  const lines: string[] = ['## \u{1F5C4}\uFE0F Database Changes', '']

  // Entity changes
  const entities = diff.entities || {}
  if (entities.added || entities.modified || entities.deleted) {
    lines.push('### Entities')
    if (entities.added) lines.push(`- **Added:** ${entities.added}`)
    if (entities.modified) lines.push(`- **Modified:** ${entities.modified}`)
    if (entities.deleted) lines.push(`- **Deleted:** ${entities.deleted}`)
    lines.push('')
  }

  // Relationship changes
  const relationships = diff.relationships || {}
  if (relationships.added || relationships.modified || relationships.deleted) {
    lines.push('### Relationships')
    if (relationships.added) lines.push(`- **Added:** ${relationships.added}`)
    if (relationships.modified)
      lines.push(`- **Modified:** ${relationships.modified}`)
    if (relationships.deleted)
      lines.push(`- **Deleted:** ${relationships.deleted}`)
    lines.push('')
  }

  // Preview URL
  if (previewUrl && previewBranch) {
    lines.push('### Preview')
    lines.push(`[View preview database](${previewUrl}/${previewBranch})`)
    lines.push('')
  }

  return lines.join('\n')
}
