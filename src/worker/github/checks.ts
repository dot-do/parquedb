/**
 * GitHub Checks API functions for ParqueDB merge checks
 */

export interface Octokit {
  rest: {
    checks: {
      create: (params: CreateCheckParams) => Promise<{ data: { id: number; status?: string } }>
      update: (params: UpdateCheckParams) => Promise<{ data: { id: number; conclusion?: string } }>
    }
  }
}

interface CreateCheckParams {
  owner: string
  repo: string
  name: string
  head_sha: string
  status: 'queued' | 'in_progress' | 'completed'
  output?: {
    title: string
    summary: string
    text?: string
    annotations?: Annotation[]
  }
}

interface UpdateCheckParams {
  owner: string
  repo: string
  check_run_id: number
  status?: 'queued' | 'in_progress' | 'completed'
  conclusion?: 'success' | 'failure' | 'neutral' | 'cancelled' | 'skipped' | 'timed_out' | 'action_required'
  output?: {
    title: string
    summary: string
    text?: string
    annotations?: Annotation[]
  }
}

interface Annotation {
  path: string
  start_line: number
  end_line: number
  annotation_level: 'notice' | 'warning' | 'failure'
  message: string
  title?: string
}

export interface CreateMergeCheckOptions {
  owner: string
  repo: string
  pr: number
  headSha: string
}

export interface MergePreview {
  collections: Record<string, { added: number; removed: number; modified: number }>
}

export interface UpdateCheckSuccessOptions {
  owner: string
  repo: string
  checkId: number
  mergePreview: MergePreview
}

export interface Conflict {
  ns: string
  entityId: string
  field: string
  ours: string
  theirs: string
}

export interface UpdateCheckFailureOptions {
  owner: string
  repo: string
  checkId: number
  conflicts: Conflict[]
}

export interface SchemaWarning {
  type: 'breaking' | 'warning'
  collection: string
  field: string
  message: string
}

export interface UpdateCheckWithSchemaWarningsOptions {
  owner: string
  repo: string
  checkId: number
  warnings: SchemaWarning[]
}

const CHECK_NAME = 'ParqueDB Merge Check'

/**
 * Create a new merge check with pending status
 */
export async function createMergeCheck(
  octokit: Octokit,
  options: CreateMergeCheckOptions
): Promise<{ id: number }> {
  const result = await octokit.rest.checks.create({
    owner: options.owner,
    repo: options.repo,
    name: CHECK_NAME,
    head_sha: options.headSha,
    status: 'in_progress',
    output: {
      title: 'Analyzing database changes...',
      summary: 'Checking for merge conflicts and schema changes.',
    },
  })

  return { id: result.data.id }
}

/**
 * Update check to success conclusion
 */
export async function updateCheckSuccess(
  octokit: Octokit,
  options: UpdateCheckSuccessOptions
): Promise<void> {
  const collections = options.mergePreview.collections
  const collectionNames = Object.keys(collections)

  let text = ''
  if (collectionNames.length > 0) {
    text = '## Entity Changes\n\n'
    text += '| Collection | Added | Removed | Modified |\n'
    text += '|------------|-------|---------|----------|\n'

    for (const name of collectionNames) {
      const c = collections[name]
      text += `| ${name} | +${c.added} | -${c.removed} | ~${c.modified} |\n`
    }
  } else {
    text = 'No entity changes detected.\n'
  }

  text += '\nNo conflicts detected.'

  await octokit.rest.checks.update({
    owner: options.owner,
    repo: options.repo,
    check_run_id: options.checkId,
    status: 'completed',
    conclusion: 'success',
    output: {
      title: 'Clean merge - No conflicts',
      summary: 'Clean merge possible. No conflicts detected.',
      text,
    },
  })
}

/**
 * Update check to failure conclusion with conflict details
 */
export async function updateCheckFailure(
  octokit: Octokit,
  options: UpdateCheckFailureOptions
): Promise<void> {
  const { conflicts } = options
  const conflictCount = conflicts.length

  let text = '## Merge Conflicts\n\n'
  text += `Found ${conflictCount} conflict${conflictCount !== 1 ? 's' : ''} that must be resolved:\n\n`

  for (const conflict of conflicts) {
    text += `### ${conflict.ns}/${conflict.entityId}\n\n`
    text += `**Field:** \`${conflict.field}\`\n`
    text += `- **Ours (base):** \`${conflict.ours}\`\n`
    text += `- **Theirs (incoming):** \`${conflict.theirs}\`\n\n`
  }

  text += '## Resolution\n\n'
  text += 'Use the following commands to resolve conflicts:\n\n'
  text += '```\n'
  for (const conflict of conflicts) {
    text += `/parquedb resolve ${conflict.ns}/${conflict.entityId} --ours   # Keep base value\n`
    text += `/parquedb resolve ${conflict.ns}/${conflict.entityId} --theirs # Accept incoming value\n`
  }
  text += '```\n'

  const annotations: Annotation[] = conflicts.map((conflict) => ({
    path: `data/${conflict.ns}/data.parquet`,
    start_line: 1,
    end_line: 1,
    annotation_level: 'warning',
    message: `Conflict in field '${conflict.field}': ours='${conflict.ours}' vs theirs='${conflict.theirs}'`,
    title: `${conflict.ns}/${conflict.entityId}`,
  }))

  await octokit.rest.checks.update({
    owner: options.owner,
    repo: options.repo,
    check_run_id: options.checkId,
    status: 'completed',
    conclusion: 'failure',
    output: {
      title: `${conflictCount} conflict${conflictCount !== 1 ? 's' : ''} found`,
      summary: `Found ${conflictCount} merge conflict${conflictCount !== 1 ? 's' : ''} that require resolution.`,
      text,
      annotations,
    },
  })
}

/**
 * Update check with schema warnings (neutral conclusion)
 */
export async function updateCheckWithSchemaWarnings(
  octokit: Octokit,
  options: UpdateCheckWithSchemaWarningsOptions
): Promise<void> {
  const { warnings } = options
  const breakingChanges = warnings.filter((w) => w.type === 'breaking')

  let text = '## Schema Warnings\n\n'

  if (breakingChanges.length > 0) {
    text += '### Breaking Changes\n\n'
    for (const warning of breakingChanges) {
      text += `- **${warning.collection}.${warning.field}**: ${warning.message}\n`
    }
    text += '\n'
  }

  text += '## Migration Required\n\n'
  text += 'These schema changes may require a migration before merging:\n\n'
  for (const warning of warnings) {
    text += `- \`${warning.collection}.${warning.field}\`: ${warning.message}\n`
  }

  await octokit.rest.checks.update({
    owner: options.owner,
    repo: options.repo,
    check_run_id: options.checkId,
    status: 'completed',
    conclusion: 'neutral',
    output: {
      title: `${breakingChanges.length} breaking change${breakingChanges.length !== 1 ? 's' : ''} detected`,
      summary: `Found ${breakingChanges.length} breaking schema change${breakingChanges.length !== 1 ? 's' : ''} that may require migration.`,
      text,
    },
  })
}
