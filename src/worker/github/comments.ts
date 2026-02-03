/**
 * PR comment formatting for ParqueDB GitHub integration
 */

export interface Octokit {
  rest: {
    issues: {
      listComments: (params: ListCommentsParams) => Promise<{ data: Comment[] }>
      createComment: (params: CreateCommentParams) => Promise<{ data: { id: number } }>
      updateComment: (params: UpdateCommentParams) => Promise<{ data: { id: number } }>
    }
  }
}

interface ListCommentsParams {
  readonly owner: string
  readonly repo: string
  readonly issue_number: number
}

interface CreateCommentParams {
  readonly owner: string
  readonly repo: string
  readonly issue_number: number
  readonly body: string
}

interface UpdateCommentParams {
  readonly owner: string
  readonly repo: string
  readonly comment_id: number
  readonly body: string
}

interface Comment {
  readonly id: number
  readonly user: { readonly login: string } | null
  readonly body?: string
}

export interface CollectionDiffCounts {
  readonly added: number
  readonly removed: number
  readonly modified: number
}

export interface DiffData {
  readonly collections: Readonly<Record<string, CollectionDiffCounts>>
}

export interface PreviewUrlOptions {
  readonly owner: string
  readonly repo: string
  readonly pr: number
}

export interface MergeStatusOptions {
  readonly status: 'clean' | 'conflicts' | 'warnings'
  readonly conflictCount?: number
}

export interface Conflict {
  readonly ns: string
  readonly entityId: string
  readonly field: string
  readonly ours: string
  readonly theirs: string
}

export interface SchemaField {
  readonly name: string
  readonly type: string
}

export interface ModifiedField {
  readonly name: string
  readonly oldType: string
  readonly newType: string
}

export interface CollectionSchemaChanges {
  readonly added: readonly SchemaField[]
  readonly removed: readonly SchemaField[]
  readonly modified: readonly ModifiedField[]
}

export interface SchemaChanges {
  readonly [collection: string]: CollectionSchemaChanges
}

export interface FindCommentOptions {
  readonly owner: string
  readonly repo: string
  readonly pr: number
}

export interface UpsertCommentOptions {
  readonly owner: string
  readonly repo: string
  readonly pr: number
  readonly body: string
}

const BOT_USERNAME = 'parquedb[bot]'
const COMMENT_MARKER = '## \uD83D\uDDC4\uFE0F Database Changes'
const MAX_COLLECTIONS = 20

/**
 * Format a diff as a PR comment
 */
export function formatDiffComment(diff: DiffData): string {
  const collections = Object.keys(diff.collections)
  const timestamp = new Date().toISOString()

  let comment = `${COMMENT_MARKER}\n\n`

  if (collections.length === 0) {
    comment += 'No database changes detected in this PR.\n\n'
  } else {
    comment += '| Collection | Added | Removed | Modified |\n'
    comment += '|------------|-------|---------|----------|\n'

    let totalAdded = 0
    let totalRemoved = 0
    let totalModified = 0

    const displayCollections = collections.slice(0, MAX_COLLECTIONS)
    const remainingCount = collections.length - MAX_COLLECTIONS

    for (const name of displayCollections) {
      const c = diff.collections[name]
      comment += `| ${name} | +${c.added} | -${c.removed} | ~${c.modified} |\n`
      totalAdded += c.added
      totalRemoved += c.removed
      totalModified += c.modified
    }

    // Add totals from remaining collections
    for (const name of collections.slice(MAX_COLLECTIONS)) {
      const c = diff.collections[name]
      totalAdded += c.added
      totalRemoved += c.removed
      totalModified += c.modified
    }

    if (remainingCount > 0) {
      comment += `| *...and ${remainingCount} more* | | | |\n`
    }

    comment += `| **Total** | +${totalAdded} | -${totalRemoved} | ~${totalModified} |\n\n`
  }

  comment += '---\n'
  comment += `*Powered by [ParqueDB](https://parque.db) | Updated ${timestamp}*\n`

  return comment
}

/**
 * Format a preview URL
 */
export function formatPreviewUrl(options: PreviewUrlOptions): string {
  return `https://preview.parque.db/${options.owner}/${options.repo}/pr-${options.pr}`
}

/**
 * Format merge status indicator
 */
export function formatMergeStatus(options: MergeStatusOptions): string {
  switch (options.status) {
    case 'clean':
      return '\u2705 Clean merge possible'
    case 'conflicts':
      return `\u26A0\uFE0F ${options.conflictCount} conflict${options.conflictCount !== 1 ? 's' : ''} detected`
    case 'warnings':
      return '\u2139\uFE0F Schema changes detected'
    default:
      return 'Unknown status'
  }
}

/**
 * Format conflict details
 */
export function formatConflictDetails(conflicts: Conflict[]): string {
  let text = '## Merge Conflicts\n\n'

  for (const conflict of conflicts) {
    text += `### ${conflict.ns}/${conflict.entityId}\n\n`
    text += `**Field:** \`${conflict.field}\`\n`
    text += `- **Ours (base):** \`${conflict.ours}\`\n`
    text += `- **Theirs (incoming):** \`${conflict.theirs}\`\n\n`
  }

  text += '## Resolution Commands\n\n'
  text += '```\n'
  for (const conflict of conflicts) {
    text += `/parquedb resolve ${conflict.entityId} --ours   # Keep base value\n`
    text += `/parquedb resolve ${conflict.entityId} --theirs # Accept incoming value\n`
  }
  text += '```\n'

  return text
}

/**
 * Format schema changes
 */
export function formatSchemaChanges(changes: SchemaChanges): string {
  const collections = Object.keys(changes)
  let text = '## Schema Changes\n\n'

  for (const collection of collections) {
    const change = changes[collection]
    const hasChanges =
      change.added.length > 0 || change.removed.length > 0 || change.modified.length > 0

    if (!hasChanges) continue

    text += `### ${collection}\n\n`

    for (const field of change.added) {
      text += `+ ${collection}.${field.name}: ${field.type}\n`
    }

    for (const field of change.removed) {
      text += `- ${collection}.${field.name}: ${field.type} \u26A0\uFE0F BREAKING\n`
    }

    for (const field of change.modified) {
      text += `~ ${collection}.${field.name}: ${field.oldType} \u2192 ${field.newType}\n`
    }

    text += '\n'
  }

  return text
}

/**
 * Find existing ParqueDB comment on PR
 */
export async function findExistingComment(
  octokit: Octokit,
  options: FindCommentOptions
): Promise<number | null> {
  const response = await octokit.rest.issues.listComments({
    owner: options.owner,
    repo: options.repo,
    issue_number: options.pr,
  })

  const comment = response.data.find(
    (c) => c.user?.login === BOT_USERNAME && c.body?.includes(COMMENT_MARKER)
  )

  return comment?.id ?? null
}

/**
 * Create or update PR comment
 */
export async function upsertComment(
  octokit: Octokit,
  options: UpsertCommentOptions
): Promise<{ id: number }> {
  const existingId = await findExistingComment(octokit, {
    owner: options.owner,
    repo: options.repo,
    pr: options.pr,
  })

  if (existingId) {
    const result = await octokit.rest.issues.updateComment({
      owner: options.owner,
      repo: options.repo,
      comment_id: existingId,
      body: options.body,
    })
    return { id: result.data.id }
  } else {
    const result = await octokit.rest.issues.createComment({
      owner: options.owner,
      repo: options.repo,
      issue_number: options.pr,
      body: options.body,
    })
    return { id: result.data.id }
  }
}
