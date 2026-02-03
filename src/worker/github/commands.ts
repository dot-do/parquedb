/**
 * Slash command implementations for GitHub PR comments
 *
 * Supports commands like:
 * - /parquedb preview - Create/refresh preview branch
 * - /parquedb diff [collection] - Show data diff
 * - /parquedb resolve <entity> --ours|--theirs|--newest - Resolve conflicts
 * - /parquedb schema - Show schema diff
 * - /parquedb help - Show help
 */

// Command parsing types
export interface ParsedCommand {
  readonly command: string
  readonly args: readonly string[]
  readonly flags: Readonly<Record<string, boolean>>
}

export interface RepoIdentifier {
  readonly owner: string
  readonly repo: string
}

export interface CommandConfig {
  readonly show_samples?: boolean
  readonly max_entities?: number
}

export interface CommandContext {
  readonly issueNumber: number
  readonly repo: RepoIdentifier
  readonly commentId: number
  readonly prBranch?: string
  readonly baseBranch?: string
  readonly headBranch?: string
  readonly checkRunId?: number
  readonly commentAuthor?: string
  readonly config?: CommandConfig
}

export interface BranchCreateOpts {
  readonly name: string
  readonly from: string
  readonly overwrite?: boolean
}

export interface BranchCreateResult {
  readonly name: string
  readonly url: string
}

export interface DiffOpts {
  readonly base: string
  readonly head: string
  readonly collection?: string
}

export interface ResolveOpts {
  readonly entities: readonly string[]
  readonly strategy: string
}

export interface SchemaDiffOpts {
  readonly base: string
  readonly head: string
}

export interface BranchClient {
  readonly create: (opts: BranchCreateOpts) => Promise<BranchCreateResult>
  readonly exists: (name: string) => Promise<boolean>
}

export interface SchemaClient {
  readonly diff: (opts: SchemaDiffOpts) => Promise<SchemaDiffResult>
}

export interface ParqueDBClient {
  readonly branch: BranchClient
  readonly diff: (opts: DiffOpts) => Promise<DiffResult>
  readonly merge: (opts: unknown) => Promise<unknown>
  readonly resolve: (opts: ResolveOpts) => Promise<ResolveResult>
  readonly schema: SchemaClient
}

export interface DiffResult {
  collections: Record<string, CollectionDiff>
}

export interface CollectionDiff {
  added: number
  removed: number
  modified: number
  samples?: {
    added?: Array<{ $id: string; name?: string; [key: string]: unknown }>
    removed?: Array<{ $id: string; name?: string; [key: string]: unknown }>
    modified?: Array<{ $id: string; name?: string; [key: string]: unknown }>
  }
}

export interface ResolveResult {
  resolved: string[]
  strategy: string
}

export interface SchemaDiffResult {
  changes: SchemaChange[]
  breakingChanges: BreakingChange[]
}

export interface SchemaChange {
  type: string
  collection: string
  field: string
  fieldType?: string
}

export interface BreakingChange {
  type: string
  collection: string
  field: string
  oldType?: string
  newType?: string
  migrationHint?: string
}

export interface OctokitClient {
  rest: {
    issues: {
      createComment: (opts: { owner: string; repo: string; issue_number: number; body: string }) => Promise<unknown>
    }
    reactions: {
      createForIssueComment: (opts: { owner: string; repo: string; comment_id: number; content: string }) => Promise<unknown>
    }
    repos: {
      getCollaboratorPermissionLevel: (opts: { owner: string; repo: string; username: string }) => Promise<{ data: { permission: string } }>
    }
  }
  checks: {
    update: (opts: { owner: string; repo: string; check_run_id: number; status: string }) => Promise<unknown>
  }
}

// Valid commands for similarity matching
const VALID_COMMANDS = ['preview', 'diff', 'resolve', 'schema', 'help']

/**
 * Parse a single /parquedb command from a comment
 */
export function parseCommand(comment: string): ParsedCommand | null {
  // First, check if this is a command at the start of a line (or start of comment)
  // Commands at start of line can have arguments; commands in middle of text don't
  const startOfLineMatch = comment.match(/(?:^|\n)\s*\/parquedb\s+(\S+)(?:\s+([^\n]*))?/)

  if (startOfLineMatch) {
    const command = startOfLineMatch[1]
    const rest = startOfLineMatch[2] || ''

    // Parse args and flags from the rest
    const parts = rest.split(/\s+/).filter(Boolean)
    const args: string[] = []
    const flags: Record<string, boolean> = {}

    for (const part of parts) {
      if (part.startsWith('--')) {
        const flagName = part.slice(2)
        flags[flagName] = true
      } else {
        args.push(part)
      }
    }

    return { command, args, flags }
  }

  // Command is in the middle of text - just parse the command name, no arguments
  const midMatch = comment.match(/\/parquedb\s+(\S+)/)
  if (midMatch) {
    return { command: midMatch[1], args: [], flags: {} }
  }

  return null
}

/**
 * Parse multiple /parquedb commands from a comment
 */
export function parseCommands(comment: string): ParsedCommand[] {
  const commands: ParsedCommand[] = []
  const lines = comment.split('\n')

  for (const line of lines) {
    const parsed = parseCommand(line)
    if (parsed) {
      commands.push(parsed)
    }
  }

  return commands
}

/**
 * Handle /parquedb preview command
 * Creates or refreshes a preview branch
 */
export async function handlePreviewCommand(
  context: CommandContext,
  db: ParqueDBClient,
  octokit: OctokitClient
): Promise<void> {
  const { issueNumber, repo, commentId, prBranch } = context
  const previewBranch = `preview/pr-${issueNumber}`

  try {
    const exists = await db.branch.exists(previewBranch)

    let result
    if (exists) {
      // Refresh existing preview
      result = await db.branch.create({
        name: previewBranch,
        from: prBranch!,
        overwrite: true,
      })
    } else {
      // Create new preview
      result = await db.branch.create({
        name: previewBranch,
        from: prBranch!,
      })
    }

    // React with rocket emoji on success
    await octokit.rest.reactions.createForIssueComment({
      owner: repo.owner,
      repo: repo.repo,
      comment_id: commentId,
      content: 'rocket',
    })

    // Post preview URL
    await octokit.rest.issues.createComment({
      owner: repo.owner,
      repo: repo.repo,
      issue_number: issueNumber,
      body: `Preview environment created!\n\n${result.url}`,
    })
  } catch (error) {
    // React with confused emoji on error
    await octokit.rest.reactions.createForIssueComment({
      owner: repo.owner,
      repo: repo.repo,
      comment_id: commentId,
      content: 'confused',
    })

    // Post error message
    await octokit.rest.issues.createComment({
      owner: repo.owner,
      repo: repo.repo,
      issue_number: issueNumber,
      body: `Error creating preview:\n\n${(error as Error).message}`,
    })
  }
}

/**
 * Handle /parquedb diff command
 * Shows data differences between branches
 */
export async function handleDiffCommand(
  context: CommandContext,
  args: string[],
  db: ParqueDBClient,
  octokit: OctokitClient
): Promise<void> {
  const { issueNumber, repo, baseBranch, headBranch, commentId, config } = context
  const collection = args[0]

  // React with eyes emoji
  await octokit.rest.reactions.createForIssueComment({
    owner: repo.owner,
    repo: repo.repo,
    comment_id: commentId,
    content: 'eyes',
  })

  const diffOpts: { base: string; head: string; collection?: string } = {
    base: baseBranch!,
    head: headBranch!,
  }
  if (collection) {
    diffOpts.collection = collection
  }

  const result = await db.diff(diffOpts)

  // Check if there are any changes
  const hasChanges = Object.keys(result.collections).length > 0

  if (!hasChanges) {
    await octokit.rest.issues.createComment({
      owner: repo.owner,
      repo: repo.repo,
      issue_number: issueNumber,
      body: 'No changes detected between branches.',
    })
    return
  }

  // Format diff as markdown table
  let body = '## Data Diff\n\n'
  body += '| Collection | Added | Removed | Modified |\n'
  body += '| --- | --- | --- | --- |\n'

  for (const [collectionName, diff] of Object.entries(result.collections)) {
    body += `| ${collectionName} | ${diff.added} | ${diff.removed} | ${diff.modified} |\n`
  }

  // Show samples if configured
  if (config?.show_samples) {
    for (const [collectionName, diff] of Object.entries(result.collections)) {
      if (diff.samples?.added && diff.samples.added.length > 0) {
        const maxEntities = config.max_entities || diff.samples.added.length
        const total = diff.added
        const showing = Math.min(maxEntities, diff.samples.added.length)

        body += `\n### ${collectionName} - Added Entities`
        if (showing < total) {
          body += ` (showing ${showing} of ${total})`
        }
        body += '\n\n'

        const samplesToShow = diff.samples.added.slice(0, maxEntities)
        for (const sample of samplesToShow) {
          body += `- **${sample.$id}**: ${sample.name || 'unnamed'}\n`
        }
      }
    }
  }

  await octokit.rest.issues.createComment({
    owner: repo.owner,
    repo: repo.repo,
    issue_number: issueNumber,
    body,
  })
}

/**
 * Handle /parquedb resolve command
 * Resolves merge conflicts
 */
export async function handleResolveCommand(
  context: CommandContext,
  args: string[],
  flags: Record<string, boolean>,
  db: ParqueDBClient,
  octokit: OctokitClient
): Promise<void> {
  const { issueNumber, repo, commentId, checkRunId } = context

  // Determine strategy from flags
  let strategy: string
  if (flags.ours) {
    strategy = 'ours'
  } else if (flags.theirs) {
    strategy = 'theirs'
  } else if (flags.newest) {
    strategy = 'newest'
  } else {
    strategy = 'ours' // default
  }

  try {
    const result = await db.resolve({
      entities: args,
      strategy,
    })

    // React with checkmark on success
    await octokit.rest.reactions.createForIssueComment({
      owner: repo.owner,
      repo: repo.repo,
      comment_id: commentId,
      content: '+1',
    })

    // Post success message
    await octokit.rest.issues.createComment({
      owner: repo.owner,
      repo: repo.repo,
      issue_number: issueNumber,
      body: `Resolved ${result.resolved.length} conflicts using **${strategy}** strategy.`,
    })

    // Trigger merge check re-run
    if (checkRunId) {
      await octokit.checks.update({
        owner: repo.owner,
        repo: repo.repo,
        check_run_id: checkRunId,
        status: 'queued',
      })
    }
  } catch (error) {
    await octokit.rest.issues.createComment({
      owner: repo.owner,
      repo: repo.repo,
      issue_number: issueNumber,
      body: `Error resolving conflicts:\n\n${(error as Error).message}`,
    })
  }
}

/**
 * Handle /parquedb schema command
 * Shows schema differences between branches
 */
export async function handleSchemaCommand(
  context: CommandContext,
  db: ParqueDBClient,
  octokit: OctokitClient
): Promise<void> {
  const { issueNumber, repo, baseBranch, headBranch } = context

  const result = await db.schema.diff({
    base: baseBranch!,
    head: headBranch!,
  })

  const hasChanges = result.changes.length > 0 || result.breakingChanges.length > 0

  if (!hasChanges) {
    await octokit.rest.issues.createComment({
      owner: repo.owner,
      repo: repo.repo,
      issue_number: issueNumber,
      body: 'No schema changes detected between branches.',
    })
    return
  }

  let body = '## Schema Diff\n\n'

  // Show breaking changes with warning
  if (result.breakingChanges.length > 0) {
    body += '\n### Breaking Changes\n\n'
    for (const change of result.breakingChanges) {
      body += `- ${change.type}: ${change.collection}.${change.field}`
      if (change.oldType && change.newType) {
        body += ` (${change.oldType} -> ${change.newType})`
      }
      body += '\n'
      if (change.migrationHint) {
        body += `  - Migration hint: ${change.migrationHint}\n`
      }
    }
    body = body.replace('### Breaking Changes', '\u26A0\uFE0F ### Breaking Changes')
  }

  // Show non-breaking changes
  if (result.changes.length > 0) {
    body += '\n### Changes\n\n'
    for (const change of result.changes) {
      body += `- ${change.type}: ${change.collection}.${change.field}`
      if (change.fieldType) {
        body += ` (${change.fieldType})`
      }
      body += '\n'
    }
  }

  await octokit.rest.issues.createComment({
    owner: repo.owner,
    repo: repo.repo,
    issue_number: issueNumber,
    body,
  })
}

/**
 * Handle /parquedb help command
 * Posts help message with available commands
 */
export async function handleHelpCommand(
  context: CommandContext,
  octokit: OctokitClient
): Promise<void> {
  const { issueNumber, repo } = context

  const body = `## ParqueDB Commands

Available slash commands:

- \`/parquedb preview\` - Create or refresh a preview environment
- \`/parquedb diff [collection]\` - Show data differences between branches
- \`/parquedb resolve <entity> --ours|--theirs|--newest\` - Resolve merge conflicts
- \`/parquedb schema\` - Show schema differences between branches
- \`/parquedb help\` - Show this help message

### Examples

\`\`\`
/parquedb preview
/parquedb diff users
/parquedb resolve posts/123 --ours
/parquedb schema
\`\`\`
`

  await octokit.rest.issues.createComment({
    owner: repo.owner,
    repo: repo.repo,
    issue_number: issueNumber,
    body,
  })
}

/**
 * Calculate Levenshtein distance between two strings
 */
function levenshteinDistance(a: string, b: string): number {
  const matrix: number[][] = []

  for (let i = 0; i <= b.length; i++) {
    matrix[i] = [i]
  }

  for (let j = 0; j <= a.length; j++) {
    matrix[0][j] = j
  }

  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b.charAt(i - 1) === a.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1]
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1, // substitution
          matrix[i][j - 1] + 1, // insertion
          matrix[i - 1][j] + 1 // deletion
        )
      }
    }
  }

  return matrix[b.length][a.length]
}

/**
 * Find the most similar command
 */
function findSimilarCommand(input: string): string | null {
  let bestMatch: string | null = null
  let bestDistance = Infinity

  for (const cmd of VALID_COMMANDS) {
    const distance = levenshteinDistance(input.toLowerCase(), cmd)
    if (distance < bestDistance && distance <= 3) {
      bestDistance = distance
      bestMatch = cmd
    }
  }

  return bestMatch
}

/**
 * Handle unknown command
 * Posts help message and suggests similar commands
 */
export async function handleUnknownCommand(
  context: CommandContext,
  command: string,
  octokit: OctokitClient
): Promise<void> {
  const { issueNumber, repo } = context

  const similar = findSimilarCommand(command)
  let body = `Unknown command: \`${command}\`\n\n`

  if (similar) {
    body += `Did you mean \`/parquedb ${similar}\`?\n\n`
  }

  body += `Use \`/parquedb help\` to see available commands.`

  await octokit.rest.issues.createComment({
    owner: repo.owner,
    repo: repo.repo,
    issue_number: issueNumber,
    body,
  })
}

/**
 * Check if user has write permissions to the repository
 */
export async function checkPermissions(
  context: CommandContext,
  octokit: OctokitClient
): Promise<boolean> {
  const { repo, commentAuthor } = context

  if (!commentAuthor) {
    return false
  }

  const result = await octokit.rest.repos.getCollaboratorPermissionLevel({
    owner: repo.owner,
    repo: repo.repo,
    username: commentAuthor,
  })

  const permission = result.data.permission
  return permission === 'write' || permission === 'admin'
}
