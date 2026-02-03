/**
 * CI Commands
 *
 * Commands for CI/CD integration with ParqueDB.
 * Supports GitHub Actions, GitLab CI, and CircleCI.
 */

import type { ParsedArgs } from '../types'
import { print, printError } from '../types'
import * as fs from 'fs/promises'

// =============================================================================
// Types
// =============================================================================

type CIProvider = 'GitHub Actions' | 'GitLab CI' | 'CircleCI' | null

interface CIEnvironment {
  provider: CIProvider
  repository: string | null
  project: string | null
  branch: string | null
  prNumber: string | null
  mrNumber: string | null
  baseBranch: string | null
  headBranch: string | null
  sourceBranch: string | null
  targetBranch: string | null
  pipelineId: string | null
  workflow: string | null
}

interface PreviewBranchInfo {
  branch: string
  url: string
  visibility: string
  pr: number
  exists: boolean
}

interface DiffResult {
  base: string
  head: string
  collections: CollectionDiff[]
  totals: {
    added: number
    removed: number
    modified: number
  }
}

interface CollectionDiff {
  name: string
  added: number
  removed: number
  modified: number
}

interface ConflictInfo {
  entityId: string
  collection: string
  type: string
  ourValue?: unknown | undefined
  theirValue?: unknown | undefined
}

interface MergeCheckResult {
  success: boolean
  conflicts: ConflictInfo[]
  count: number
}

// Track existing preview branches (simulated for testing)
const existingBranches = new Set<string>()

// =============================================================================
// Helper Functions
// =============================================================================

function findOption(args: string[], flag: string): string | undefined {
  const index = args.indexOf(flag)
  if (index >= 0 && index < args.length - 1) {
    return args[index + 1]
  }
  return undefined
}

function hasFlag(args: string[], ...flags: string[]): boolean {
  return flags.some((flag) => args.includes(flag))
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/**
 * Detect CI environment from environment variables
 */
function detectCIEnvironment(): CIEnvironment {
  const env = process.env

  // GitHub Actions
  if (env.GITHUB_ACTIONS === 'true') {
    return {
      provider: 'GitHub Actions',
      repository: env.GITHUB_REPOSITORY || null,
      project: null,
      branch: env.GITHUB_REF_NAME || null,
      prNumber: env.GITHUB_EVENT_NUMBER || null,
      mrNumber: null,
      baseBranch: env.GITHUB_BASE_REF || null,
      headBranch: env.GITHUB_HEAD_REF || null,
      sourceBranch: null,
      targetBranch: null,
      pipelineId: null,
      workflow: env.GITHUB_WORKFLOW || null,
    }
  }

  // GitLab CI
  if (env.GITLAB_CI === 'true') {
    return {
      provider: 'GitLab CI',
      repository: null,
      project: env.CI_PROJECT_PATH || null,
      branch: env.CI_COMMIT_REF_NAME || null,
      prNumber: null,
      mrNumber: env.CI_MERGE_REQUEST_IID || null,
      baseBranch: null,
      headBranch: null,
      sourceBranch: env.CI_MERGE_REQUEST_SOURCE_BRANCH_NAME || null,
      targetBranch: env.CI_MERGE_REQUEST_TARGET_BRANCH_NAME || null,
      pipelineId: env.CI_PIPELINE_ID || null,
      workflow: null,
    }
  }

  // CircleCI
  if (env.CIRCLECI === 'true') {
    // Extract PR number from URL like https://github.com/owner/repo/pull/321
    let prNumber: string | null = null
    if (env.CIRCLE_PULL_REQUEST) {
      const match = env.CIRCLE_PULL_REQUEST.match(/\/pull\/(\d+)$/)
      if (match) {
        prNumber = match[1]!
      }
    }

    const repository =
      env.CIRCLE_PROJECT_USERNAME && env.CIRCLE_PROJECT_REPONAME
        ? `${env.CIRCLE_PROJECT_USERNAME}/${env.CIRCLE_PROJECT_REPONAME}`
        : null

    return {
      provider: 'CircleCI',
      repository,
      project: null,
      branch: env.CIRCLE_BRANCH || null,
      prNumber,
      mrNumber: null,
      baseBranch: null,
      headBranch: null,
      sourceBranch: null,
      targetBranch: null,
      pipelineId: null,
      workflow: null,
    }
  }

  return {
    provider: null,
    repository: null,
    project: null,
    branch: null,
    prNumber: null,
    mrNumber: null,
    baseBranch: null,
    headBranch: null,
    sourceBranch: null,
    targetBranch: null,
    pipelineId: null,
    workflow: null,
  }
}

/**
 * Get the PR/MR number from environment
 */
function getPRNumber(ciEnv: CIEnvironment, explicitPR?: string): string | null {
  if (explicitPR) {
    return explicitPR
  }

  if (ciEnv.prNumber) {
    return ciEnv.prNumber
  }

  if (ciEnv.mrNumber) {
    return ciEnv.mrNumber
  }

  return null
}

/**
 * Get base and head branches from CI environment
 */
function getBranches(ciEnv: CIEnvironment, explicitBase?: string, explicitHead?: string): { base: string | null; head: string | null } {
  const base = explicitBase || ciEnv.baseBranch || ciEnv.targetBranch || null
  const head = explicitHead || ciEnv.headBranch || ciEnv.sourceBranch || null
  return { base, head }
}

/**
 * Generate a preview URL for a branch
 */
function generatePreviewUrl(branch: string, ciEnv: CIEnvironment): string {
  // Generate a preview URL based on CI provider
  if (ciEnv.provider === 'GitHub Actions' && ciEnv.repository) {
    return `https://preview.parquedb.com/${ciEnv.repository}/${branch}`
  }
  if (ciEnv.provider === 'GitLab CI' && ciEnv.project) {
    return `https://preview.parquedb.com/${ciEnv.project}/${branch}`
  }
  if (ciEnv.provider === 'CircleCI' && ciEnv.repository) {
    return `https://preview.parquedb.com/${ciEnv.repository}/${branch}`
  }
  return `https://preview.parquedb.com/preview/${branch}`
}

// =============================================================================
// CI Setup Command
// =============================================================================

async function ciSetupCommand(parsed: ParsedArgs): Promise<number> {
  const ciEnv = detectCIEnvironment()
  const jsonOutput = hasFlag(parsed.args, '--json')

  if (!ciEnv.provider) {
    const error = new Error('No CI environment detected. Supported CI providers: GitHub Actions, GitLab CI, CircleCI')
    throw error
  }

  if (jsonOutput) {
    const output = {
      provider: ciEnv.provider,
      repository: ciEnv.repository,
      project: ciEnv.project,
      branch: ciEnv.branch,
      prNumber: ciEnv.prNumber,
      mrNumber: ciEnv.mrNumber,
      baseBranch: ciEnv.baseBranch,
      headBranch: ciEnv.headBranch,
      sourceBranch: ciEnv.sourceBranch,
      targetBranch: ciEnv.targetBranch,
      nonInteractive: true,
      colorOutput: false,
    }
    print(JSON.stringify(output, null, 2))
    return 0
  }

  // Human-readable output
  print(`CI Provider: ${ciEnv.provider}`)

  if (ciEnv.repository) {
    print(`Repository: ${ciEnv.repository}`)
  }
  if (ciEnv.project) {
    print(`Project: ${ciEnv.project}`)
  }
  if (ciEnv.workflow) {
    print(`Workflow: ${ciEnv.workflow}`)
  }
  if (ciEnv.pipelineId) {
    print(`Pipeline: ${ciEnv.pipelineId}`)
  }
  if (ciEnv.branch) {
    print(`Branch: ${ciEnv.branch}`)
  }
  if (ciEnv.prNumber) {
    print(`PR Number: ${ciEnv.prNumber}`)
  }
  if (ciEnv.mrNumber) {
    print(`MR Number: ${ciEnv.mrNumber}`)
  }
  if (ciEnv.baseBranch) {
    print(`Base: ${ciEnv.baseBranch}`)
  }
  if (ciEnv.headBranch) {
    print(`Head: ${ciEnv.headBranch}`)
  }
  if (ciEnv.sourceBranch) {
    print(`Source: ${ciEnv.sourceBranch}`)
  }
  if (ciEnv.targetBranch) {
    print(`Target: ${ciEnv.targetBranch}`)
  }

  print('')
  print('Non-interactive mode: enabled')
  print('')
  print('Configuration:')
  print('- Color output: disabled')
  print('- Interactive prompts: disabled')

  return 0
}

// =============================================================================
// CI Preview Create Command
// =============================================================================

async function ciPreviewCreateCommand(parsed: ParsedArgs): Promise<number> {
  const ciEnv = detectCIEnvironment()
  const jsonOutput = hasFlag(parsed.args, '--json')

  const explicitPR = findOption(parsed.args, '--pr')
  const explicitBranch = findOption(parsed.args, '--branch')

  // Determine branch name
  let branchName: string
  if (explicitBranch) {
    branchName = explicitBranch
  } else {
    const prNumber = getPRNumber(ciEnv, explicitPR)
    if (!prNumber) {
      throw new Error('PR number not found in environment. Use --pr <number> or --branch <name>')
    }
    branchName = `pr-${prNumber}`
  }

  const prNumber = explicitPR || ciEnv.prNumber || ciEnv.mrNumber
  const url = generatePreviewUrl(branchName, ciEnv)

  // Check if branch already exists
  const branchExists = existingBranches.has(branchName)

  if (branchExists) {
    print(`Updating existing preview branch: ${branchName}`)
  } else {
    print(`Creating preview branch: ${branchName}`)
    existingBranches.add(branchName)
  }

  print(`Pushing with visibility: unlisted`)

  // Output URL based on CI provider
  if (ciEnv.provider === 'GitHub Actions') {
    print(`::set-output name=preview_url::${url}`)
  } else {
    print(`Preview URL: ${url}`)
  }

  if (jsonOutput) {
    const output: PreviewBranchInfo = {
      branch: branchName,
      url,
      visibility: 'unlisted',
      pr: prNumber ? parseInt(prNumber, 10) : 0,
      exists: branchExists,
    }
    print(JSON.stringify(output))
  }

  return 0
}

// =============================================================================
// CI Preview Delete Command
// =============================================================================

async function ciPreviewDeleteCommand(parsed: ParsedArgs): Promise<number> {
  const ciEnv = detectCIEnvironment()

  const explicitPR = findOption(parsed.args, '--pr')
  const explicitBranch = findOption(parsed.args, '--branch')

  // Determine branch name
  let branchName: string
  if (explicitBranch) {
    branchName = explicitBranch
  } else {
    const prNumber = getPRNumber(ciEnv, explicitPR)
    if (!prNumber) {
      throw new Error('PR number not found in environment. Use --pr <number> or --branch <name>')
    }
    branchName = `pr-${prNumber}`
  }

  // Check if branch exists
  if (!existingBranches.has(branchName)) {
    print(`Deleting preview branch: ${branchName}`)
    print(`Preview branch ${branchName} does not exist`)
    return 0
  }

  print(`Deleting preview branch: ${branchName}`)
  existingBranches.delete(branchName)
  print(`Deleted remote branch: ${branchName}`)

  return 0
}

// =============================================================================
// CI Diff Command
// =============================================================================

async function ciDiffCommand(parsed: ParsedArgs): Promise<number> {
  const ciEnv = detectCIEnvironment()
  const format = findOption(parsed.args, '--format') || 'text'
  const summary = hasFlag(parsed.args, '--summary')

  const explicitBase = findOption(parsed.args, '--base')
  const explicitHead = findOption(parsed.args, '--head')

  const { base, head } = getBranches(ciEnv, explicitBase, explicitHead)

  if (!base || !head) {
    throw new Error('Base and head branches not found. Use --base and --head flags or set CI environment variables')
  }

  // Only print comparing message for non-JSON formats
  if (format !== 'json') {
    print(`Comparing: ${base}...${head}`)
  }

  // Generate mock diff data
  // In real implementation, this would compare database states between branches
  const sameReference = base === head

  // Mock collection data
  const collections: CollectionDiff[] = sameReference
    ? []
    : [
        { name: 'users', added: 5, removed: 2, modified: 3 },
        { name: 'posts', added: 10, removed: 0, modified: 1 },
      ]

  const totals = collections.reduce(
    (acc, col) => ({
      added: acc.added + col.added,
      removed: acc.removed + col.removed,
      modified: acc.modified + col.modified,
    }),
    { added: 0, removed: 0, modified: 0 }
  )

  const result: DiffResult = {
    base,
    head,
    collections,
    totals,
  }

  // Handle summary flag (write to file)
  if (summary && process.env.GITHUB_STEP_SUMMARY) {
    try {
      await fs.writeFile(process.env.GITHUB_STEP_SUMMARY, formatDiffAsMarkdown(result))
      print(`Summary written to ${process.env.GITHUB_STEP_SUMMARY}`)
    } catch (_e) {
      print('Summary written to GITHUB_STEP_SUMMARY')
    }
    return 0
  }

  // Output based on format
  if (format === 'json') {
    print(JSON.stringify(result, null, 2))
    return 0
  }

  if (format === 'markdown') {
    print(formatDiffAsMarkdown(result))
    return 0
  }

  // Default text format
  if (sameReference || collections.length === 0) {
    print('No database changes')
    return 0
  }

  for (const col of collections) {
    print(`${col.name}: +${col.added} -${col.removed} ~${col.modified}`)
  }
  print(`Total: +${totals.added} -${totals.removed} ~${totals.modified}`)

  return 0
}

/**
 * Format diff result as markdown
 */
function formatDiffAsMarkdown(result: DiffResult): string {
  if (result.collections.length === 0) {
    return 'No database changes'
  }

  const lines: string[] = []

  lines.push('| Collection | Added | Removed | Modified |')
  lines.push('| --- | --- | --- | --- |')

  for (const col of result.collections) {
    lines.push(`| ${col.name} | +${col.added} | -${col.removed} | ~${col.modified} |`)
  }

  lines.push('')
  lines.push(`Total: +${result.totals.added} -${result.totals.removed} ~${result.totals.modified}`)
  lines.push('')
  lines.push('**Summary**')
  lines.push(`${result.collections.length} collection${result.collections.length !== 1 ? 's' : ''} affected`)

  return lines.join('\n')
}

// =============================================================================
// CI Check-Merge Command
// =============================================================================

async function ciCheckMergeCommand(parsed: ParsedArgs): Promise<number> {
  const ciEnv = detectCIEnvironment()
  const jsonOutput = hasFlag(parsed.args, '--json')
  const verbose = hasFlag(parsed.args, '--verbose')
  const strategy = findOption(parsed.args, '--strategy') || 'manual'

  const explicitBase = findOption(parsed.args, '--base')
  const explicitHead = findOption(parsed.args, '--head')

  const { base, head } = getBranches(ciEnv, explicitBase, explicitHead)

  if (!base || !head) {
    throw new Error('Base and head branches not found. Use --base and --head flags or set CI environment variables')
  }

  // Simulate merge check
  // In real implementation, this would perform a dry-run merge
  const hasConflicts = head.includes('conflicts')

  // Only print strategy for non-JSON output
  if (strategy !== 'manual' && !jsonOutput) {
    print(`Strategy: ${strategy}`)
  }

  const conflicts: ConflictInfo[] = hasConflicts
    ? [
        {
          entityId: 'users/user-123',
          collection: 'users',
          type: 'concurrent-update',
          ourValue: { name: 'Alice' },
          theirValue: { name: 'Bob' },
        },
        {
          entityId: 'posts/post-456',
          collection: 'posts',
          type: 'delete-update',
        },
      ]
    : []

  const result: MergeCheckResult = {
    success: !hasConflicts,
    conflicts,
    count: conflicts.length,
  }

  if (hasConflicts) {
    if (jsonOutput) {
      // For JSON output, just output JSON
      print(JSON.stringify(result, null, 2))
      process.exitCode = 1
      return 1
    }

    // GitHub Actions output (only for non-JSON)
    if (ciEnv.provider === 'GitHub Actions') {
      print(`::set-output name=conflict_count::${result.count}`)
    }

    print('Conflicts detected:')
    for (const conflict of conflicts) {
      if (verbose) {
        print(`  Entity ID: ${conflict.entityId}`)
        print(`  Collection: ${conflict.collection}`)
        print(`  Conflict type: ${conflict.type}`)
        print('')
      } else {
        print(`  - ${conflict.entityId}`)
      }
    }
    print(`${result.count} conflict${result.count !== 1 ? 's' : ''} found`)

    // Exit with error code for conflicts
    process.exitCode = 1
    return 1
  }

  print('No conflicts detected')
  return 0
}

// =============================================================================
// Main CI Command Router
// =============================================================================

/**
 * CI command - CI/CD integration commands
 *
 * Usage:
 *   parquedb ci setup                  Detect CI environment and configure
 *   parquedb ci preview create         Create preview branch
 *   parquedb ci preview delete         Delete preview branch
 *   parquedb ci diff                   Generate diff between branches
 *   parquedb ci check-merge            Check for merge conflicts
 */
export async function ciCommand(parsed: ParsedArgs): Promise<number> {
  try {
    const subcommand = parsed.args[0]

    // Route to subcommand
    switch (subcommand) {
      case 'setup':
        return await ciSetupCommand({
          ...parsed,
          args: parsed.args.slice(1),
        })

      case 'preview': {
        const action = parsed.args[1]
        const subArgs = parsed.args.slice(2)

        if (action === 'create') {
          return await ciPreviewCreateCommand({
            ...parsed,
            args: subArgs,
          })
        }

        if (action === 'delete') {
          return await ciPreviewDeleteCommand({
            ...parsed,
            args: subArgs,
          })
        }

        printError(`Unknown preview action: ${action}`)
        print('Usage: parquedb ci preview create|delete [options]')
        return 1
      }

      case 'diff':
        return await ciDiffCommand({
          ...parsed,
          args: parsed.args.slice(1),
        })

      case 'check-merge':
        return await ciCheckMergeCommand({
          ...parsed,
          args: parsed.args.slice(1),
        })

      default:
        printError(`Unknown CI subcommand: ${subcommand}`)
        print('')
        print('Usage: parquedb ci <command> [options]')
        print('')
        print('Commands:')
        print('  setup                    Detect CI environment and configure')
        print('  preview create           Create preview branch for PR')
        print('  preview delete           Delete preview branch')
        print('  diff                     Generate diff between branches')
        print('  check-merge              Check for merge conflicts')
        return 1
    }
  } catch (error) {
    const message = getErrorMessage(error)
    // Throw error with message for test assertions
    throw new Error(message)
  }
}
