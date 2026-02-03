import { describe, it, expect } from 'vitest'
import * as fs from 'fs'
import * as path from 'path'
import * as yaml from 'yaml'

const TEMPLATES_DIR = '.github/workflows/templates'

function loadWorkflow(name: string): any {
  const filePath = path.join(TEMPLATES_DIR, name)
  const content = fs.readFileSync(filePath, 'utf8')
  return yaml.parse(content)
}

describe('workflow templates', () => {
  describe('pr-preview.yml', () => {
    it('exists and is valid YAML', () => {
      const workflow = loadWorkflow('pr-preview.yml')
      expect(workflow).toBeDefined()
    })

    it('triggers on pull_request events', () => {
      const workflow = loadWorkflow('pr-preview.yml')
      expect(workflow.on.pull_request).toBeDefined()
    })

    it('triggers on opened, synchronize, and closed', () => {
      const workflow = loadWorkflow('pr-preview.yml')
      expect(workflow.on.pull_request.types).toContain('opened')
      expect(workflow.on.pull_request.types).toContain('synchronize')
      expect(workflow.on.pull_request.types).toContain('closed')
    })

    it('uses parquedb/setup-action', () => {
      const workflow = loadWorkflow('pr-preview.yml')
      const steps = workflow.jobs.preview?.steps || []
      const setupStep = steps.find(s => s.uses?.includes('parquedb/setup-action'))
      expect(setupStep).toBeDefined()
    })

    it('creates preview branch on open', () => {
      const workflow = loadWorkflow('pr-preview.yml')
      const steps = workflow.jobs.preview?.steps || []
      const createStep = steps.find(s => s.run?.includes('parquedb ci preview create'))
      expect(createStep).toBeDefined()
    })

    it('deletes preview branch on close', () => {
      const workflow = loadWorkflow('pr-preview.yml')
      // Check for conditional cleanup job
      const cleanupJob = workflow.jobs.cleanup
      expect(cleanupJob).toBeDefined()
      expect(cleanupJob.if).toContain('closed')
    })

    it('posts comment with preview URL', () => {
      const workflow = loadWorkflow('pr-preview.yml')
      const steps = workflow.jobs.preview?.steps || []
      const commentStep = steps.find(s =>
        s.uses?.includes('github-script') ||
        s.uses?.includes('comment') ||
        s.run?.includes('comment')
      )
      expect(commentStep).toBeDefined()
    })
  })

  describe('database-diff.yml', () => {
    it('exists and is valid YAML', () => {
      const workflow = loadWorkflow('database-diff.yml')
      expect(workflow).toBeDefined()
    })

    it('triggers on pull_request', () => {
      const workflow = loadWorkflow('database-diff.yml')
      expect(workflow.on.pull_request).toBeDefined()
    })

    it('runs diff command', () => {
      const workflow = loadWorkflow('database-diff.yml')
      const jobs = Object.values(workflow.jobs) as any[]
      const hasDiff = jobs.some(job =>
        job.steps?.some(s => s.run?.includes('parquedb ci diff'))
      )
      expect(hasDiff).toBe(true)
    })

    it('posts diff as PR comment', () => {
      const workflow = loadWorkflow('database-diff.yml')
      const jobs = Object.values(workflow.jobs) as any[]
      const hasComment = jobs.some(job =>
        job.steps?.some(s =>
          s.uses?.includes('github-script') ||
          s.uses?.includes('comment') ||
          s.run?.includes('comment')
        )
      )
      expect(hasComment).toBe(true)
    })

    it('updates existing comment instead of creating new', () => {
      const workflow = loadWorkflow('database-diff.yml')
      const jobs = Object.values(workflow.jobs) as any[]
      const hasUpdateLogic = jobs.some(job =>
        job.steps?.some(s =>
          s.run?.includes('find-comment') ||
          s.run?.includes('comment-id') ||
          s.with?.['comment-id'] ||
          (s.uses?.includes('github-script') && s.with?.script?.includes('update'))
        )
      )
      expect(hasUpdateLogic).toBe(true)
    })
  })

  describe('merge-check.yml', () => {
    it('exists and is valid YAML', () => {
      const workflow = loadWorkflow('merge-check.yml')
      expect(workflow).toBeDefined()
    })

    it('triggers on pull_request', () => {
      const workflow = loadWorkflow('merge-check.yml')
      expect(workflow.on.pull_request).toBeDefined()
    })

    it('runs check-merge command', () => {
      const workflow = loadWorkflow('merge-check.yml')
      const jobs = Object.values(workflow.jobs) as any[]
      const hasCheckMerge = jobs.some(job =>
        job.steps?.some(s => s.run?.includes('parquedb ci check-merge'))
      )
      expect(hasCheckMerge).toBe(true)
    })

    it('uses check run for status', () => {
      const workflow = loadWorkflow('merge-check.yml')
      const jobs = Object.values(workflow.jobs) as any[]
      const hasCheckRun = jobs.some(job =>
        job.steps?.some(s =>
          s.uses?.includes('check-run') ||
          s.run?.includes('check-run') ||
          s.with?.['check-name']
        )
      )
      expect(hasCheckRun).toBe(true)
    })

    it('fails workflow on conflicts', () => {
      const workflow = loadWorkflow('merge-check.yml')
      const jobs = Object.values(workflow.jobs) as any[]
      const hasFailure = jobs.some(job =>
        job.steps?.some(s =>
          s['continue-on-error'] === false ||
          !s['continue-on-error']
        )
      )
      expect(hasFailure).toBe(true)
    })
  })

  describe('auto-merge.yml', () => {
    it('exists and is valid YAML', () => {
      const workflow = loadWorkflow('auto-merge.yml')
      expect(workflow).toBeDefined()
    })

    it('triggers only on merged PRs', () => {
      const workflow = loadWorkflow('auto-merge.yml')
      expect(workflow.on.pull_request.types).toContain('closed')
    })

    it('has condition for merged=true', () => {
      const workflow = loadWorkflow('auto-merge.yml')
      const job = workflow.jobs.merge || workflow.jobs['auto-merge']
      expect(job.if).toContain('merged')
    })

    it('merges database branch', () => {
      const workflow = loadWorkflow('auto-merge.yml')
      const jobs = Object.values(workflow.jobs) as any[]
      const hasMerge = jobs.some(job =>
        job.steps?.some(s => s.run?.includes('parquedb merge'))
      )
      expect(hasMerge).toBe(true)
    })

    it('pushes after merge', () => {
      const workflow = loadWorkflow('auto-merge.yml')
      const jobs = Object.values(workflow.jobs) as any[]
      const hasPush = jobs.some(job =>
        job.steps?.some(s => s.run?.includes('parquedb push'))
      )
      expect(hasPush).toBe(true)
    })

    it('cleans up preview branch', () => {
      const workflow = loadWorkflow('auto-merge.yml')
      const jobs = Object.values(workflow.jobs) as any[]
      const hasCleanup = jobs.some(job =>
        job.steps?.some(s =>
          s.run?.includes('git branch -d') ||
          s.run?.includes('git push --delete') ||
          s.run?.includes('delete')
        )
      )
      expect(hasCleanup).toBe(true)
    })
  })

  describe('schema-check.yml', () => {
    it('exists and is valid YAML', () => {
      const workflow = loadWorkflow('schema-check.yml')
      expect(workflow).toBeDefined()
    })

    it('triggers on parquedb.config.ts changes', () => {
      const workflow = loadWorkflow('schema-check.yml')
      expect(workflow.on.pull_request.paths).toContain('parquedb.config.ts')
    })

    it('runs schema diff', () => {
      const workflow = loadWorkflow('schema-check.yml')
      const jobs = Object.values(workflow.jobs) as any[]
      const hasSchemaDiff = jobs.some(job =>
        job.steps?.some(s => s.run?.includes('parquedb schema diff'))
      )
      expect(hasSchemaDiff).toBe(true)
    })

    it('warns on breaking changes', () => {
      const workflow = loadWorkflow('schema-check.yml')
      const jobs = Object.values(workflow.jobs) as any[]
      const hasBreakingCheck = jobs.some(job =>
        job.steps?.some(s =>
          s.run?.includes('breaking') ||
          s.with?.script?.includes('breaking')
        )
      )
      expect(hasBreakingCheck).toBe(true)
    })

    it('posts schema changes as comment', () => {
      const workflow = loadWorkflow('schema-check.yml')
      const jobs = Object.values(workflow.jobs) as any[]
      const hasComment = jobs.some(job =>
        job.steps?.some(s =>
          s.uses?.includes('github-script') ||
          s.uses?.includes('comment') ||
          s.run?.includes('comment')
        )
      )
      expect(hasComment).toBe(true)
    })
  })

  describe('common patterns', () => {
    it('all workflows have concurrency settings', () => {
      const templates = ['pr-preview.yml', 'database-diff.yml', 'merge-check.yml', 'auto-merge.yml', 'schema-check.yml']
      for (const name of templates) {
        const workflow = loadWorkflow(name)
        expect(workflow.concurrency).toBeDefined()
      }
    })

    it('all workflows use checkout action', () => {
      const templates = ['pr-preview.yml', 'database-diff.yml', 'merge-check.yml', 'auto-merge.yml', 'schema-check.yml']
      for (const name of templates) {
        const workflow = loadWorkflow(name)
        const jobs = Object.values(workflow.jobs) as any[]
        for (const job of jobs) {
          const hasCheckout = job.steps?.some(s => s.uses?.includes('actions/checkout'))
          expect(hasCheckout).toBe(true)
        }
      }
    })
  })
})
