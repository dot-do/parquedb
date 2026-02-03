/**
 * GitHub Webhook Router for ParqueDB
 *
 * Handles GitHub App webhooks and routes them to appropriate handlers.
 * Integrates with the ParqueDB Worker for database CI/CD operations.
 *
 * @module
 */

import type { Env } from '../../types/worker'
import { logger } from '../../utils/logger'
import {
  verifyWebhookSignature,
  handleInstallationCreated,
  handleInstallationDeleted,
  handleCreate,
  handleDelete,
  handlePullRequestOpened,
  handlePullRequestSynchronize,
  handlePullRequestClosed,
  type DatabaseIndexService,
  type ParqueDBService,
  type OctokitClient,
  type InstallationCreatedPayload,
  type InstallationDeletedPayload,
  type CreatePayload,
  type DeletePayload,
  type PullRequestPayload,
} from './handlers'
import {
  parseCommand,
  handlePreviewCommand,
  handleDiffCommand,
  handleResolveCommand,
  handleSchemaCommand,
  handleHelpCommand,
  handleUnknownCommand,
  checkPermissions,
  type CommandContext,
  type ParqueDBClient,
  type OctokitClient as CommandOctokitClient,
} from './commands'

// =============================================================================
// Types
// =============================================================================

/**
 * GitHub App environment bindings
 */
export interface GitHubAppEnv extends Env {
  /** GitHub App ID */
  GITHUB_APP_ID: string
  /** GitHub App private key (PEM format) */
  GITHUB_APP_PRIVATE_KEY: string
  /** GitHub webhook secret */
  GITHUB_WEBHOOK_SECRET: string
}

/**
 * Issue comment webhook payload
 */
interface IssueCommentPayload {
  action: 'created' | 'edited' | 'deleted'
  issue: {
    number: number
    pull_request?: { url: string }
  }
  comment: {
    id: number
    body: string
    user: { login: string }
  }
  repository: {
    full_name: string
    owner: { login: string }
    name: string
  }
  installation?: { id: number }
}

/**
 * Pull request from issue comment payload
 */
interface PullRequestFromComment {
  head: { ref: string; sha: string }
  base: { ref: string }
}

// =============================================================================
// Webhook Handler
// =============================================================================

/**
 * Handle incoming GitHub webhooks
 *
 * Routes webhooks to appropriate handlers based on event type.
 *
 * @param request - Incoming webhook request
 * @param env - Environment bindings
 * @returns Response indicating success or failure
 */
export async function handleGitHubWebhook(
  request: Request,
  env: GitHubAppEnv
): Promise<Response> {
  // Get event type from headers
  const eventType = request.headers.get('x-github-event')
  const deliveryId = request.headers.get('x-github-delivery')
  const signature = request.headers.get('x-hub-signature-256')

  if (!eventType) {
    return new Response(JSON.stringify({ error: 'Missing X-GitHub-Event header' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    })
  }

  // Read request body
  const payload = await request.text()

  // Verify webhook signature
  try {
    const isValid = await verifyWebhookSignature(payload, signature, env.GITHUB_WEBHOOK_SECRET)
    if (!isValid) {
      return new Response(JSON.stringify({ error: 'Invalid webhook signature' }), {
        status: 401,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  } catch (error) {
    return new Response(
      JSON.stringify({ error: error instanceof Error ? error.message : 'Signature verification failed' }),
      {
        status: 401,
        headers: { 'Content-Type': 'application/json' },
      }
    )
  }

  // Parse payload
  let data: unknown
  try {
    data = JSON.parse(payload)
  } catch {
    return new Response(JSON.stringify({ error: 'Invalid JSON payload' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    })
  }

  // Create service adapters
  const databaseIndex = createDatabaseIndexAdapter(env)
  const parqueDB = createParqueDBAdapter(env)
  const octokit = await createOctokitAdapter(env, data)

  try {
    // Route to appropriate handler
    switch (eventType) {
      case 'installation': {
        const installationPayload = data as { action: string }
        if (installationPayload.action === 'created') {
          await handleInstallationCreated(
            data as InstallationCreatedPayload,
            databaseIndex
          )
        } else if (installationPayload.action === 'deleted') {
          await handleInstallationDeleted(
            data as InstallationDeletedPayload,
            databaseIndex
          )
        }
        break
      }

      case 'create': {
        await handleCreate(data as CreatePayload, databaseIndex, parqueDB)
        break
      }

      case 'delete': {
        await handleDelete(data as DeletePayload, databaseIndex, parqueDB)
        break
      }

      case 'pull_request': {
        const prPayload = data as PullRequestPayload
        switch (prPayload.action) {
          case 'opened':
            await handlePullRequestOpened(prPayload, databaseIndex, parqueDB, octokit)
            break
          case 'synchronize':
            await handlePullRequestSynchronize(prPayload, databaseIndex, parqueDB, octokit)
            break
          case 'closed':
            await handlePullRequestClosed(prPayload, databaseIndex, parqueDB, octokit)
            break
        }
        break
      }

      case 'issue_comment': {
        const commentPayload = data as IssueCommentPayload
        // Only handle comments on PRs
        if (commentPayload.action === 'created' && commentPayload.issue.pull_request) {
          await handleIssueComment(commentPayload, env, databaseIndex, parqueDB, octokit)
        }
        break
      }

      case 'ping':
        // GitHub sends a ping event when the webhook is first configured
        logger.debug(`[GitHub Webhook] Ping received, delivery: ${deliveryId}`)
        break

      default:
        logger.debug(`[GitHub Webhook] Unhandled event type: ${eventType}`)
    }

    return new Response(JSON.stringify({ success: true, event: eventType }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  } catch (error) {
    logger.error(`[GitHub Webhook] Error handling ${eventType}:`, error)
    return new Response(
      JSON.stringify({
        error: 'Internal error',
        message: error instanceof Error ? error.message : 'Unknown error',
      }),
      {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      }
    )
  }
}

// =============================================================================
// Issue Comment Handler (Slash Commands)
// =============================================================================

/**
 * Handle issue_comment.created webhook for slash commands
 */
async function handleIssueComment(
  payload: IssueCommentPayload,
  env: GitHubAppEnv,
  _databaseIndex: DatabaseIndexService,
  parqueDB: ParqueDBService,
  octokit: OctokitClient
): Promise<void> {
  const { issue, comment, repository } = payload
  const [owner, repo] = repository.full_name.split('/')

  // Parse command from comment
  const parsed = parseCommand(comment.body)
  if (!parsed) {
    return // Not a ParqueDB command
  }

  // Get PR details for branch context
  const prDetails = await getPullRequestDetails(octokit, owner!, repo!, issue.number)

  // Build command context
  const context: CommandContext = {
    issueNumber: issue.number,
    repo: { owner: owner!, repo: repo! },
    commentId: comment.id,
    prBranch: prDetails?.head.ref,
    baseBranch: prDetails?.base.ref,
    headBranch: prDetails?.head.ref,
    commentAuthor: comment.user.login,
  }

  // Create command-specific adapters
  const commandOctokit = octokit as unknown as CommandOctokitClient
  const commandParqueDB = parqueDB as unknown as ParqueDBClient

  // Check permissions for write commands
  if (['resolve', 'preview'].includes(parsed.command)) {
    const hasPermission = await checkPermissions(context, commandOctokit)
    if (!hasPermission) {
      await commandOctokit.rest.issues.createComment({
        owner: owner!,
        repo: repo!,
        issue_number: issue.number,
        body: 'You do not have permission to run this command. Write access is required.',
      })
      return
    }
  }

  // Route to command handler
  switch (parsed.command) {
    case 'preview':
      await handlePreviewCommand(context, commandParqueDB, commandOctokit)
      break
    case 'diff':
      await handleDiffCommand(context, [...parsed.args], commandParqueDB, commandOctokit)
      break
    case 'resolve':
      await handleResolveCommand(
        context,
        [...parsed.args],
        { ...parsed.flags },
        commandParqueDB,
        commandOctokit
      )
      break
    case 'schema':
      await handleSchemaCommand(context, commandParqueDB, commandOctokit)
      break
    case 'help':
      await handleHelpCommand(context, commandOctokit)
      break
    default:
      await handleUnknownCommand(context, parsed.command, commandOctokit)
  }
}

/**
 * Get PR details from GitHub API
 */
async function getPullRequestDetails(
  octokit: OctokitClient,
  owner: string,
  repo: string,
  prNumber: number
): Promise<PullRequestFromComment | null> {
  try {
    // Use the octokit rest API to get PR details
    // Note: This requires adding a pulls.get method to OctokitClient
    // For now, we'll use fetch directly
    const response = await fetch(
      `https://api.github.com/repos/${owner}/${repo}/pulls/${prNumber}`,
      {
        headers: {
          Accept: 'application/vnd.github.v3+json',
          // Authorization header would be set by the octokit instance
        },
      }
    )

    if (!response.ok) {
      logger.error(`[GitHub Webhook] Failed to get PR details: ${response.status}`)
      return null
    }

    const pr = (await response.json()) as PullRequestFromComment
    return pr
  } catch (error) {
    logger.error('[GitHub Webhook] Error fetching PR details:', error)
    return null
  }
}

// =============================================================================
// Service Adapters
// =============================================================================

/**
 * Create DatabaseIndex service adapter
 *
 * This adapter bridges the webhook handlers to the DatabaseIndexDO.
 * In a real implementation, this would use the Durable Object bindings.
 */
function createDatabaseIndexAdapter(_env: GitHubAppEnv): DatabaseIndexService {
  // Stub implementation - actual DO binding integration would use:
  // const stub = env.DATABASE_INDEX.get(env.DATABASE_INDEX.idFromName('global'))
  // For now, return a stub that logs operations
  return {
    getUserByInstallation: async (installationId: number) => {
      logger.debug(`[DatabaseIndex] Looking up user for installation ${installationId}`)
      // In production, this would query the DO
      return null
    },
    linkGitHubInstallation: async (userId: string, installationId: number, data) => {
      logger.debug(`[DatabaseIndex] Linking installation ${installationId} to user ${userId}`, data)
      // In production, this would update the DO
    },
    unlinkGitHubInstallation: async (installationId: number) => {
      logger.debug(`[DatabaseIndex] Unlinking installation ${installationId}`)
      // In production, this would update the DO
      return true
    },
    getDatabaseByRepo: async (fullName: string) => {
      logger.debug(`[DatabaseIndex] Looking up database for repo ${fullName}`)
      // In production, this would query the DO
      return null
    },
  }
}

/**
 * Create ParqueDB service adapter
 *
 * This adapter bridges the webhook handlers to the ParqueDB DO.
 * In a real implementation, this would use the Durable Object bindings.
 */
function createParqueDBAdapter(_env: GitHubAppEnv): ParqueDBService {
  // Stub implementation - actual DO binding integration would use:
  // const stub = env.PARQUEDB.get(env.PARQUEDB.idFromName(databaseId))
  // For now, return a stub that logs operations
  return {
    branch: {
      create: async (name: string, options) => {
        logger.debug(`[ParqueDB] Creating branch ${name}`, options)
        return { name }
      },
      delete: async (name: string) => {
        logger.debug(`[ParqueDB] Deleting branch ${name}`)
        return true
      },
      exists: async (name: string) => {
        logger.debug(`[ParqueDB] Checking if branch ${name} exists`)
        return false
      },
    },
    merge: async (source: string, target: string, options) => {
      logger.debug(`[ParqueDB] Merging ${source} into ${target}`, options)
      return { conflicts: [] }
    },
    diff: async (source: string, target: string) => {
      logger.debug(`[ParqueDB] Computing diff between ${source} and ${target}`)
      return { entities: {}, relationships: {} }
    },
  }
}

/**
 * Create Octokit adapter for GitHub API calls
 *
 * In a real implementation, this would use the GitHub App authentication
 * with the installation access token.
 */
async function createOctokitAdapter(
  _env: GitHubAppEnv,
  payload: unknown
): Promise<OctokitClient> {
  // Get installation ID from payload
  const installationId = (payload as { installation?: { id: number } })?.installation?.id

  // GitHub App authentication would require:
  // 1. Generating JWT using App private key (stored in env.GITHUB_APP_PRIVATE_KEY)
  // 2. Exchanging JWT for installation access token via POST /app/installations/{id}/access_tokens
  // 3. Creating authenticated Octokit client with the installation token
  // For now, return a stub client that logs operations.

  logger.debug(`[GitHub] Creating Octokit client for installation ${installationId}`)

  // Return a stub client that logs operations
  return {
    rest: {
      checks: {
        create: async (params) => {
          logger.debug('[GitHub] Creating check run', params)
          return { data: { id: Date.now() } }
        },
        update: async (params) => {
          logger.debug('[GitHub] Updating check run', params)
          return { data: { id: params.check_run_id } }
        },
      },
      issues: {
        createComment: async (params) => {
          logger.debug('[GitHub] Creating comment', params)
          return { data: { id: Date.now() } }
        },
        updateComment: async (params) => {
          logger.debug('[GitHub] Updating comment', params)
          return { data: { id: params.comment_id } }
        },
        listComments: async (params) => {
          logger.debug('[GitHub] Listing comments', params)
          return { data: [] }
        },
      },
      reactions: {
        createForIssueComment: async (params) => {
          logger.debug('[GitHub] Creating reaction', params)
          return { data: { id: Date.now() } }
        },
      },
    },
  }
}

// =============================================================================
// Exports
// =============================================================================

export { handleGitHubWebhook as default }
