import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { ciCommand } from '../../../src/cli/commands/ci'
import type { ParsedArgs } from '../../../src/cli/index'

/**
 * Helper to create parsed args for ci command
 */
function createParsedArgs(args: string[]): ParsedArgs {
  return {
    command: 'ci',
    args,
    options: {
      help: false,
      version: false,
      directory: process.cwd(),
      format: 'json',
      pretty: false,
      quiet: false,
    },
  }
}

describe('parquedb ci commands', () => {
  let originalEnv: NodeJS.ProcessEnv
  let stdoutOutput: string[] = []
  let stderrOutput: string[] = []
  const originalStdoutWrite = process.stdout.write
  const originalStderrWrite = process.stderr.write
  let originalExitCode: number | undefined

  beforeEach(() => {
    originalEnv = { ...process.env }
    originalExitCode = process.exitCode
    stdoutOutput = []
    stderrOutput = []

    // Clear all CI environment variables
    delete process.env.GITHUB_ACTIONS
    delete process.env.GITHUB_EVENT_NUMBER
    delete process.env.GITHUB_BASE_REF
    delete process.env.GITHUB_HEAD_REF
    delete process.env.GITHUB_REPOSITORY
    delete process.env.GITHUB_WORKFLOW
    delete process.env.GITHUB_REF_NAME
    delete process.env.GITHUB_STEP_SUMMARY
    delete process.env.GITLAB_CI
    delete process.env.CI_MERGE_REQUEST_IID
    delete process.env.CI_MERGE_REQUEST_SOURCE_BRANCH_NAME
    delete process.env.CI_MERGE_REQUEST_TARGET_BRANCH_NAME
    delete process.env.CI_PROJECT_PATH
    delete process.env.CI_PIPELINE_ID
    delete process.env.CIRCLECI
    delete process.env.CIRCLE_PULL_REQUEST
    delete process.env.CIRCLE_PROJECT_USERNAME
    delete process.env.CIRCLE_PROJECT_REPONAME
    delete process.env.CIRCLE_BRANCH

    // Mock stdout and stderr
    process.stdout.write = vi.fn((chunk: string | Uint8Array) => {
      stdoutOutput.push(chunk.toString())
      return true
    })
    process.stderr.write = vi.fn((chunk: string | Uint8Array) => {
      stderrOutput.push(chunk.toString())
      return true
    })
  })

  afterEach(() => {
    process.env = originalEnv
    process.stdout.write = originalStdoutWrite
    process.stderr.write = originalStderrWrite
    process.exitCode = originalExitCode
  })

  const getStdout = () => stdoutOutput.join('')
  const getStderr = () => stderrOutput.join('')

  describe('ci setup', () => {
    it('detects GitHub Actions environment', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_REPOSITORY = 'owner/repo'
      process.env.GITHUB_WORKFLOW = 'CI'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).toContain('CI Provider: GitHub Actions')
      expect(getStdout()).toContain('Repository: owner/repo')
      expect(result).toBe(0)
    })

    it('detects GitLab CI environment', async () => {
      process.env.GITLAB_CI = 'true'
      process.env.CI_PROJECT_PATH = 'group/project'
      process.env.CI_PIPELINE_ID = '12345'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).toContain('CI Provider: GitLab CI')
      expect(getStdout()).toContain('Project: group/project')
      expect(result).toBe(0)
    })

    it('detects CircleCI environment', async () => {
      process.env.CIRCLECI = 'true'
      process.env.CIRCLE_PROJECT_USERNAME = 'owner'
      process.env.CIRCLE_PROJECT_REPONAME = 'repo'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).toContain('CI Provider: CircleCI')
      expect(getStdout()).toContain('Repository: owner/repo')
      expect(result).toBe(0)
    })

    it('configures non-interactive mode', async () => {
      process.env.GITHUB_ACTIONS = 'true'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).toContain('Non-interactive mode: enabled')
      expect(result).toBe(0)
    })

    it('outputs environment info', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_REF_NAME = 'feature/test'
      process.env.GITHUB_EVENT_NUMBER = '123'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).toContain('Branch: feature/test')
      expect(getStdout()).toContain('PR Number: 123')
      expect(result).toBe(0)
    })

    it('throws helpful error when not in CI', async () => {
      // All CI env vars cleared in beforeEach

      await expect(ciCommand(createParsedArgs(['setup']))).rejects.toThrow(
        'No CI environment detected'
      )
    })

    it('outputs configuration summary', async () => {
      process.env.GITHUB_ACTIONS = 'true'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).toMatch(/Configuration:/)
      expect(getStdout()).toMatch(/- Color output: disabled/)
      expect(getStdout()).toMatch(/- Interactive prompts: disabled/)
      expect(result).toBe(0)
    })
  })

  describe('ci preview create', () => {
    it('creates branch with pr-{number} naming', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_EVENT_NUMBER = '123'

      const result = await ciCommand(createParsedArgs(['preview', 'create']))

      expect(getStdout()).toContain('Creating preview branch: pr-123')
      expect(result).toBe(0)
    })

    it('uses PR number from GitHub environment', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_EVENT_NUMBER = '456'

      const result = await ciCommand(createParsedArgs(['preview', 'create']))

      expect(getStdout()).toContain('pr-456')
      expect(result).toBe(0)
    })

    it('uses PR number from GitLab environment', async () => {
      process.env.GITLAB_CI = 'true'
      process.env.CI_MERGE_REQUEST_IID = '789'

      const result = await ciCommand(createParsedArgs(['preview', 'create']))

      expect(getStdout()).toContain('pr-789')
      expect(result).toBe(0)
    })

    it('extracts PR number from CircleCI pull request URL', async () => {
      process.env.CIRCLECI = 'true'
      process.env.CIRCLE_PULL_REQUEST = 'https://github.com/owner/repo/pull/321'

      const result = await ciCommand(createParsedArgs(['preview', 'create']))

      expect(getStdout()).toContain('pr-321')
      expect(result).toBe(0)
    })

    it('pushes with unlisted visibility', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_EVENT_NUMBER = '123'

      const result = await ciCommand(createParsedArgs(['preview', 'create']))

      expect(getStdout()).toContain('Pushing with visibility: unlisted')
      expect(result).toBe(0)
    })

    it('outputs preview URL as GitHub Actions output', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_EVENT_NUMBER = '123'

      const result = await ciCommand(createParsedArgs(['preview', 'create']))

      expect(getStdout()).toMatch(/::set-output name=preview_url::https:\/\//)
      expect(getStdout()).toContain('pr-123')
      expect(result).toBe(0)
    })

    it('outputs preview URL for GitLab CI', async () => {
      process.env.GITLAB_CI = 'true'
      process.env.CI_MERGE_REQUEST_IID = '123'

      const result = await ciCommand(createParsedArgs(['preview', 'create']))

      expect(getStdout()).toMatch(/Preview URL: https:\/\//)
      expect(getStdout()).toContain('pr-123')
      expect(result).toBe(0)
    })

    it('handles existing preview branch (updates it)', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_EVENT_NUMBER = '123'

      // First create
      await ciCommand(createParsedArgs(['preview', 'create']))
      stdoutOutput = []

      // Second create (update)
      const result = await ciCommand(createParsedArgs(['preview', 'create']))

      expect(getStdout()).toContain('Updating existing preview branch: pr-123')
      expect(result).toBe(0)
    })

    it('throws error when PR number not available', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      // No GITHUB_EVENT_NUMBER set

      await expect(ciCommand(createParsedArgs(['preview', 'create']))).rejects.toThrow(
        'PR number not found in environment'
      )
    })

    it('accepts explicit PR number via --pr flag', async () => {
      process.env.GITHUB_ACTIONS = 'true'

      const result = await ciCommand(createParsedArgs(['preview', 'create', '--pr', '999']))

      expect(getStdout()).toContain('pr-999')
      expect(result).toBe(0)
    })

    it('accepts custom branch name via --branch flag', async () => {
      process.env.GITHUB_ACTIONS = 'true'

      const result = await ciCommand(
        createParsedArgs(['preview', 'create', '--branch', 'custom-preview'])
      )

      expect(getStdout()).toContain('Creating preview branch: custom-preview')
      expect(result).toBe(0)
    })

    it('outputs branch metadata as JSON with --json flag', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_EVENT_NUMBER = '123'

      const result = await ciCommand(createParsedArgs(['preview', 'create', '--json']))

      const output = JSON.parse(getStdout().split('\n').filter(line => line.startsWith('{')).pop()!)
      expect(output).toMatchObject({
        branch: 'pr-123',
        url: expect.stringMatching(/^https:\/\//),
        visibility: 'unlisted',
        pr: 123,
      })
      expect(result).toBe(0)
    })
  })

  describe('ci preview delete', () => {
    it('deletes preview branch by PR number', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_EVENT_NUMBER = '123'

      const result = await ciCommand(createParsedArgs(['preview', 'delete']))

      expect(getStdout()).toContain('Deleting preview branch: pr-123')
      expect(result).toBe(0)
    })

    it('handles non-existent branch gracefully', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_EVENT_NUMBER = '88888'

      const result = await ciCommand(createParsedArgs(['preview', 'delete']))

      expect(getStdout()).toContain('Preview branch pr-88888 does not exist')
      expect(result).toBe(0)
    })

    it('deletes remote branch', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_EVENT_NUMBER = '123'

      // First create the branch
      await ciCommand(createParsedArgs(['preview', 'create']))
      stdoutOutput = []

      // Then delete it
      const result = await ciCommand(createParsedArgs(['preview', 'delete']))

      expect(getStdout()).toContain('Deleted remote branch: pr-123')
      expect(result).toBe(0)
    })

    it('accepts explicit PR number via --pr flag', async () => {
      process.env.GITHUB_ACTIONS = 'true'

      const result = await ciCommand(createParsedArgs(['preview', 'delete', '--pr', '999']))

      expect(getStdout()).toContain('pr-999')
      expect(result).toBe(0)
    })

    it('accepts custom branch name via --branch flag', async () => {
      process.env.GITHUB_ACTIONS = 'true'

      const result = await ciCommand(
        createParsedArgs(['preview', 'delete', '--branch', 'custom-preview'])
      )

      expect(getStdout()).toContain('Deleting preview branch: custom-preview')
      expect(result).toBe(0)
    })
  })

  describe('ci diff', () => {
    describe('--format=markdown', () => {
      it('outputs markdown table header', async () => {
        process.env.GITHUB_ACTIONS = 'true'
        process.env.GITHUB_BASE_REF = 'main'
        process.env.GITHUB_HEAD_REF = 'feature/test'

        const result = await ciCommand(createParsedArgs(['diff', '--format', 'markdown']))

        expect(getStdout()).toContain('| Collection | Added | Removed | Modified |')
        expect(getStdout()).toMatch(/\|[-\s]+\|[-\s]+\|[-\s]+\|[-\s]+\|/)
        expect(result).toBe(0)
      })

      it('shows entity counts per collection', async () => {
        process.env.GITHUB_ACTIONS = 'true'
        process.env.GITHUB_BASE_REF = 'main'
        process.env.GITHUB_HEAD_REF = 'feature/test'

        const result = await ciCommand(createParsedArgs(['diff', '--format', 'markdown']))

        expect(getStdout()).toMatch(/\| \w+ \| \+\d+ \| -\d+ \| ~\d+ \|/)
        expect(result).toBe(0)
      })

      it('shows total entities affected', async () => {
        process.env.GITHUB_ACTIONS = 'true'
        process.env.GITHUB_BASE_REF = 'main'
        process.env.GITHUB_HEAD_REF = 'feature/test'

        const result = await ciCommand(createParsedArgs(['diff', '--format', 'markdown']))

        expect(getStdout()).toMatch(/Total: \+\d+ -\d+ ~\d+/)
        expect(result).toBe(0)
      })

      it('handles no changes gracefully', async () => {
        process.env.GITHUB_ACTIONS = 'true'
        process.env.GITHUB_BASE_REF = 'main'
        process.env.GITHUB_HEAD_REF = 'main'

        const result = await ciCommand(createParsedArgs(['diff', '--format', 'markdown']))

        expect(getStdout()).toContain('No database changes')
        expect(result).toBe(0)
      })

      it('includes summary at bottom', async () => {
        process.env.GITHUB_ACTIONS = 'true'
        process.env.GITHUB_BASE_REF = 'main'
        process.env.GITHUB_HEAD_REF = 'feature/test'

        const result = await ciCommand(createParsedArgs(['diff', '--format', 'markdown']))

        expect(getStdout()).toMatch(/\*\*Summary\*\*/)
        expect(getStdout()).toMatch(/\d+ collections? affected/)
        expect(result).toBe(0)
      })
    })

    describe('--format=json', () => {
      it('outputs valid JSON', async () => {
        process.env.GITHUB_ACTIONS = 'true'
        process.env.GITHUB_BASE_REF = 'main'
        process.env.GITHUB_HEAD_REF = 'feature/test'

        const result = await ciCommand(createParsedArgs(['diff', '--format', 'json']))

        const output = JSON.parse(getStdout())
        expect(output).toBeDefined()
        expect(result).toBe(0)
      })

      it('includes detailed change information', async () => {
        process.env.GITHUB_ACTIONS = 'true'
        process.env.GITHUB_BASE_REF = 'main'
        process.env.GITHUB_HEAD_REF = 'feature/test'

        const result = await ciCommand(createParsedArgs(['diff', '--format', 'json']))

        const output = JSON.parse(getStdout())
        expect(output).toMatchObject({
          base: 'main',
          head: 'feature/test',
          collections: expect.any(Array),
          totals: {
            added: expect.any(Number),
            removed: expect.any(Number),
            modified: expect.any(Number),
          },
        })
        expect(result).toBe(0)
      })

      it('includes per-collection details', async () => {
        process.env.GITHUB_ACTIONS = 'true'
        process.env.GITHUB_BASE_REF = 'main'
        process.env.GITHUB_HEAD_REF = 'feature/test'

        const result = await ciCommand(createParsedArgs(['diff', '--format', 'json']))

        const output = JSON.parse(getStdout())
        if (output.collections.length > 0) {
          expect(output.collections[0]).toMatchObject({
            name: expect.any(String),
            added: expect.any(Number),
            removed: expect.any(Number),
            modified: expect.any(Number),
          })
        }
        expect(result).toBe(0)
      })
    })

    it('compares base branch to head by default', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_BASE_REF = 'main'
      process.env.GITHUB_HEAD_REF = 'feature/test'

      const result = await ciCommand(createParsedArgs(['diff']))

      expect(getStdout()).toContain('Comparing: main...feature/test')
      expect(result).toBe(0)
    })

    it('accepts explicit branch arguments', async () => {
      process.env.GITHUB_ACTIONS = 'true'

      const result = await ciCommand(
        createParsedArgs(['diff', '--base', 'develop', '--head', 'feature/xyz'])
      )

      expect(getStdout()).toContain('Comparing: develop...feature/xyz')
      expect(result).toBe(0)
    })

    it('throws error when base/head not available', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      // No GITHUB_BASE_REF or GITHUB_HEAD_REF

      await expect(ciCommand(createParsedArgs(['diff']))).rejects.toThrow(
        'Base and head branches not found'
      )
    })

    it('outputs GitHub Actions summary format with --summary flag', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_BASE_REF = 'main'
      process.env.GITHUB_HEAD_REF = 'feature/test'
      process.env.GITHUB_STEP_SUMMARY = '/tmp/summary.md'

      const result = await ciCommand(createParsedArgs(['diff', '--summary']))

      expect(getStdout()).toContain('Summary written to')
      expect(result).toBe(0)
    })
  })

  describe('ci check-merge', () => {
    it('exits 0 on clean merge', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_BASE_REF = 'main'
      process.env.GITHUB_HEAD_REF = 'feature/test'

      const result = await ciCommand(createParsedArgs(['check-merge']))

      expect(getStdout()).toContain('No conflicts detected')
      expect(result).toBe(0)
    })

    it('exits 1 on conflicts', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_BASE_REF = 'main'
      process.env.GITHUB_HEAD_REF = 'feature/conflicts'

      const result = await ciCommand(createParsedArgs(['check-merge']))

      expect(result).toBe(1)
    })

    it('outputs conflict details as JSON with --json flag', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_BASE_REF = 'main'
      process.env.GITHUB_HEAD_REF = 'feature/conflicts'

      const result = await ciCommand(createParsedArgs(['check-merge', '--json']))

      const output = JSON.parse(getStdout())
      expect(output).toMatchObject({
        conflicts: expect.any(Array),
        count: expect.any(Number),
      })
      expect(result).toBe(1)
    })

    it('outputs conflict summary for humans', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_BASE_REF = 'main'
      process.env.GITHUB_HEAD_REF = 'feature/conflicts'

      const result = await ciCommand(createParsedArgs(['check-merge']))

      expect(getStdout()).toMatch(/Conflicts detected:/)
      expect(getStdout()).toMatch(/\d+ conflicts? found/)
      expect(result).toBe(1)
    })

    it('uses configured merge strategy', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_BASE_REF = 'main'
      process.env.GITHUB_HEAD_REF = 'feature/test'

      const result = await ciCommand(
        createParsedArgs(['check-merge', '--strategy', 'last-write-wins'])
      )

      expect(getStdout()).toContain('Strategy: last-write-wins')
      expect(result).toBe(0)
    })

    it('sets GitHub Actions output for conflict count', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_BASE_REF = 'main'
      process.env.GITHUB_HEAD_REF = 'feature/conflicts'

      const result = await ciCommand(createParsedArgs(['check-merge']))

      expect(getStdout()).toMatch(/::set-output name=conflict_count::\d+/)
      expect(result).toBe(1)
    })

    it('outputs detailed conflict information with --verbose flag', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_BASE_REF = 'main'
      process.env.GITHUB_HEAD_REF = 'feature/conflicts'

      const result = await ciCommand(createParsedArgs(['check-merge', '--verbose']))

      expect(getStdout()).toMatch(/Entity ID:/)
      expect(getStdout()).toMatch(/Collection:/)
      expect(getStdout()).toMatch(/Conflict type:/)
      expect(result).toBe(1)
    })
  })

  describe('environment detection', () => {
    it('extracts PR number from GitHub Actions', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_EVENT_NUMBER = '42'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).toContain('PR Number: 42')
      expect(result).toBe(0)
    })

    it('extracts PR number from GitLab CI', async () => {
      process.env.GITLAB_CI = 'true'
      process.env.CI_MERGE_REQUEST_IID = '42'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).toContain('MR Number: 42')
      expect(result).toBe(0)
    })

    it('extracts base branch from GitHub Actions', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_BASE_REF = 'main'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).toContain('Base: main')
      expect(result).toBe(0)
    })

    it('extracts head branch from GitHub Actions', async () => {
      process.env.GITHUB_ACTIONS = 'true'
      process.env.GITHUB_HEAD_REF = 'feature/test'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).toContain('Head: feature/test')
      expect(result).toBe(0)
    })

    it('extracts source branch from GitLab CI', async () => {
      process.env.GITLAB_CI = 'true'
      process.env.CI_MERGE_REQUEST_SOURCE_BRANCH_NAME = 'feature/test'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).toContain('Source: feature/test')
      expect(result).toBe(0)
    })

    it('extracts target branch from GitLab CI', async () => {
      process.env.GITLAB_CI = 'true'
      process.env.CI_MERGE_REQUEST_TARGET_BRANCH_NAME = 'main'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).toContain('Target: main')
      expect(result).toBe(0)
    })

    it('throws helpful error when not in CI', async () => {
      // Clear all CI env vars (done in beforeEach)

      await expect(ciCommand(createParsedArgs(['setup']))).rejects.toMatchObject({
        message: expect.stringMatching(/No CI environment detected/),
      })
    })

    it('provides setup instructions in error message', async () => {
      // Clear all CI env vars (done in beforeEach)

      await expect(ciCommand(createParsedArgs(['setup']))).rejects.toMatchObject({
        message: expect.stringMatching(/Supported CI providers/),
      })
    })
  })

  describe('output formats', () => {
    it('supports plain text output by default', async () => {
      process.env.GITHUB_ACTIONS = 'true'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).not.toMatch(/^{/)
      expect(result).toBe(0)
    })

    it('supports JSON output with --json flag', async () => {
      process.env.GITHUB_ACTIONS = 'true'

      const result = await ciCommand(createParsedArgs(['setup', '--json']))

      const output = JSON.parse(getStdout())
      expect(output.provider).toBe('GitHub Actions')
      expect(result).toBe(0)
    })

    it('disables color output in CI environments', async () => {
      process.env.GITHUB_ACTIONS = 'true'

      const result = await ciCommand(createParsedArgs(['setup']))

      expect(getStdout()).not.toMatch(/\x1b\[/)
      expect(result).toBe(0)
    })
  })
})
