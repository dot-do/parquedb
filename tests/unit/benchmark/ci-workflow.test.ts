import { describe, it, expect, beforeAll } from 'vitest'
import * as fs from 'fs'
import * as path from 'path'
import * as yaml from 'yaml'

const WORKFLOW_PATH = '.github/workflows/e2e-benchmark.yml'

interface WorkflowInput {
  description: string
  required?: boolean
  default?: string | boolean
  type?: string
}

interface WorkflowTrigger {
  schedule?: Array<{ cron: string }>
  workflow_dispatch?: {
    inputs?: Record<string, WorkflowInput>
  }
  pull_request?: {
    types?: string[]
    paths?: string[]
  }
  push?: {
    branches?: string[]
  }
}

interface WorkflowStep {
  name?: string
  uses?: string
  run?: string
  with?: Record<string, unknown>
  env?: Record<string, string>
  if?: string
  id?: string
  'continue-on-error'?: boolean
}

interface WorkflowJob {
  name?: string
  'runs-on': string
  'timeout-minutes'?: number
  steps: WorkflowStep[]
  if?: string
  needs?: string | string[]
  environment?: string
}

interface Workflow {
  name: string
  on: WorkflowTrigger
  env?: Record<string, string>
  jobs: Record<string, WorkflowJob>
  concurrency?: {
    group: string
    'cancel-in-progress'?: boolean
  }
}

describe('E2E Benchmark CI Workflow', () => {
  let workflow: Workflow

  beforeAll(() => {
    const filePath = path.resolve(WORKFLOW_PATH)
    const content = fs.readFileSync(filePath, 'utf8')
    workflow = yaml.parse(content) as Workflow
  })

  describe('YAML validity', () => {
    it('parses without errors', () => {
      expect(workflow).toBeDefined()
      expect(workflow.name).toBe('E2E Benchmark')
    })

    it('has valid structure', () => {
      expect(workflow.on).toBeDefined()
      expect(workflow.jobs).toBeDefined()
      expect(Object.keys(workflow.jobs).length).toBeGreaterThan(0)
    })
  })

  describe('triggers', () => {
    it('has schedule trigger', () => {
      expect(workflow.on.schedule).toBeDefined()
      expect(workflow.on.schedule).toBeInstanceOf(Array)
      expect(workflow.on.schedule!.length).toBeGreaterThan(0)
    })

    it('schedule runs daily at 6 AM UTC', () => {
      const schedule = workflow.on.schedule!
      const cronExpression = schedule[0].cron
      expect(cronExpression).toBe('0 6 * * *')
    })

    it('has workflow_dispatch trigger for manual runs', () => {
      expect(workflow.on.workflow_dispatch).toBeDefined()
    })

    it('workflow_dispatch has url input', () => {
      const inputs = workflow.on.workflow_dispatch?.inputs
      expect(inputs).toBeDefined()
      expect(inputs?.url).toBeDefined()
      expect(inputs?.url.description).toBe('Worker URL to benchmark')
      expect(inputs?.url.default).toBe('https://parquedb.workers.do')
    })

    it('workflow_dispatch has datasets input', () => {
      const inputs = workflow.on.workflow_dispatch?.inputs
      expect(inputs?.datasets).toBeDefined()
      expect(inputs?.datasets.description).toContain('datasets')
    })

    it('workflow_dispatch has iterations input', () => {
      const inputs = workflow.on.workflow_dispatch?.inputs
      expect(inputs?.iterations).toBeDefined()
      expect(inputs?.iterations.default).toBe('5')
    })

    it('workflow_dispatch has fail_on_regression input', () => {
      const inputs = workflow.on.workflow_dispatch?.inputs
      expect(inputs?.fail_on_regression).toBeDefined()
      expect(inputs?.fail_on_regression.type).toBe('boolean')
      expect(inputs?.fail_on_regression.default).toBe(true)
    })

    it('workflow_dispatch has update_baseline input', () => {
      const inputs = workflow.on.workflow_dispatch?.inputs
      expect(inputs?.update_baseline).toBeDefined()
      expect(inputs?.update_baseline.type).toBe('boolean')
      expect(inputs?.update_baseline.default).toBe(false)
    })

    it('does NOT trigger on regular pull_request events', () => {
      // This workflow should only run on schedule or manual dispatch
      expect(workflow.on.pull_request).toBeUndefined()
    })

    it('does NOT trigger on push events', () => {
      expect(workflow.on.push).toBeUndefined()
    })
  })

  describe('required jobs', () => {
    it('has benchmark job', () => {
      expect(workflow.jobs.benchmark).toBeDefined()
    })

    it('benchmark job has correct name', () => {
      expect(workflow.jobs.benchmark.name).toBe('E2E Benchmark')
    })

    it('benchmark job runs on ubuntu-latest', () => {
      expect(workflow.jobs.benchmark['runs-on']).toBe('ubuntu-latest')
    })

    it('benchmark job has timeout', () => {
      expect(workflow.jobs.benchmark['timeout-minutes']).toBeDefined()
      expect(workflow.jobs.benchmark['timeout-minutes']).toBeLessThanOrEqual(60)
    })
  })

  describe('required secrets', () => {
    it('references CLOUDFLARE_API_TOKEN', () => {
      const jobSteps = workflow.jobs.benchmark.steps
      const stepsUsingSecret = jobSteps.filter(step =>
        step.env && Object.values(step.env).some(v =>
          typeof v === 'string' && v.includes('CLOUDFLARE_API_TOKEN')
        )
      )
      expect(stepsUsingSecret.length).toBeGreaterThan(0)
    })

    it('references CLOUDFLARE_ACCOUNT_ID', () => {
      const jobSteps = workflow.jobs.benchmark.steps
      const stepsUsingSecret = jobSteps.filter(step =>
        step.env && Object.values(step.env).some(v =>
          typeof v === 'string' && v.includes('CLOUDFLARE_ACCOUNT_ID')
        )
      )
      expect(stepsUsingSecret.length).toBeGreaterThan(0)
    })

    it('references E2E_SLACK_WEBHOOK_URL for alerts', () => {
      const jobSteps = workflow.jobs.benchmark.steps
      const slackStep = jobSteps.find(step =>
        step.name?.toLowerCase().includes('slack') ||
        step.run?.includes('SLACK_WEBHOOK')
      )
      expect(slackStep).toBeDefined()
    })
  })

  describe('environment variables', () => {
    it('has WORKER_URL env var', () => {
      expect(workflow.env?.WORKER_URL).toBeDefined()
    })

    it('has R2_BUCKET env var', () => {
      expect(workflow.env?.R2_BUCKET).toBe('parquedb-benchmarks')
    })

    it('has ENVIRONMENT env var', () => {
      expect(workflow.env?.ENVIRONMENT).toBe('production')
    })
  })

  describe('required steps', () => {
    it('has checkout step', () => {
      const steps = workflow.jobs.benchmark.steps
      const checkoutStep = steps.find(s => s.uses?.includes('actions/checkout'))
      expect(checkoutStep).toBeDefined()
    })

    it('has setup-node step', () => {
      const steps = workflow.jobs.benchmark.steps
      const nodeStep = steps.find(s => s.uses?.includes('actions/setup-node'))
      expect(nodeStep).toBeDefined()
    })

    it('uses Node.js 20.x', () => {
      const steps = workflow.jobs.benchmark.steps
      const nodeStep = steps.find(s => s.uses?.includes('actions/setup-node'))
      expect(nodeStep?.with?.['node-version']).toBe('20.x')
    })

    it('has install dependencies step', () => {
      const steps = workflow.jobs.benchmark.steps
      const installStep = steps.find(s =>
        s.run?.includes('npm ci') || s.run?.includes('pnpm install')
      )
      expect(installStep).toBeDefined()
    })

    it('has wrangler install step', () => {
      const steps = workflow.jobs.benchmark.steps
      const wranglerStep = steps.find(s => s.run?.includes('wrangler'))
      expect(wranglerStep).toBeDefined()
    })

    it('has baseline download step', () => {
      const steps = workflow.jobs.benchmark.steps
      const baselineStep = steps.find(s =>
        s.name?.toLowerCase().includes('baseline') &&
        s.name?.toLowerCase().includes('download')
      )
      expect(baselineStep).toBeDefined()
    })

    it('has benchmark run step', () => {
      const steps = workflow.jobs.benchmark.steps
      const benchmarkStep = steps.find(s =>
        s.name?.toLowerCase().includes('benchmark') &&
        s.run?.includes('runner.ts')
      )
      expect(benchmarkStep).toBeDefined()
    })

    it('has results upload step', () => {
      const steps = workflow.jobs.benchmark.steps
      const uploadStep = steps.find(s =>
        s.name?.toLowerCase().includes('upload') &&
        s.name?.toLowerCase().includes('results')
      )
      expect(uploadStep).toBeDefined()
    })

    it('has baseline update step', () => {
      const steps = workflow.jobs.benchmark.steps
      const updateStep = steps.find(s =>
        s.name?.toLowerCase().includes('update') &&
        s.name?.toLowerCase().includes('baseline')
      )
      expect(updateStep).toBeDefined()
    })

    it('has artifact upload step', () => {
      const steps = workflow.jobs.benchmark.steps
      const artifactStep = steps.find(s =>
        s.uses?.includes('actions/upload-artifact')
      )
      expect(artifactStep).toBeDefined()
    })

    it('has summary display step', () => {
      const steps = workflow.jobs.benchmark.steps
      const summaryStep = steps.find(s =>
        s.name?.toLowerCase().includes('summary') ||
        s.run?.includes('GITHUB_STEP_SUMMARY')
      )
      expect(summaryStep).toBeDefined()
    })

    it('has Slack alert step', () => {
      const steps = workflow.jobs.benchmark.steps
      const slackStep = steps.find(s =>
        s.name?.toLowerCase().includes('slack') ||
        s.name?.toLowerCase().includes('alert')
      )
      expect(slackStep).toBeDefined()
    })
  })

  describe('baseline handling', () => {
    it('downloads baseline from R2', () => {
      const steps = workflow.jobs.benchmark.steps
      const baselineStep = steps.find(s =>
        s.name?.toLowerCase().includes('download') &&
        s.name?.toLowerCase().includes('baseline')
      )
      expect(baselineStep?.run).toContain('r2 object get')
    })

    it('baseline step has continue-on-error for missing baseline', () => {
      const steps = workflow.jobs.benchmark.steps
      const baselineStep = steps.find(s =>
        s.name?.toLowerCase().includes('download') &&
        s.name?.toLowerCase().includes('baseline')
      )
      expect(baselineStep?.['continue-on-error']).toBe(true)
    })

    it('baseline step outputs existence flag', () => {
      const steps = workflow.jobs.benchmark.steps
      const baselineStep = steps.find(s =>
        s.name?.toLowerCase().includes('download') &&
        s.name?.toLowerCase().includes('baseline')
      )
      expect(baselineStep?.run).toContain('baseline_exists')
      expect(baselineStep?.run).toContain('GITHUB_OUTPUT')
    })

    it('uploads results to R2 with timestamp', () => {
      const steps = workflow.jobs.benchmark.steps
      const uploadStep = steps.find(s =>
        s.name?.toLowerCase().includes('upload') &&
        s.name?.toLowerCase().includes('r2')
      )
      expect(uploadStep?.run).toContain('r2 object put')
    })

    it('update baseline step is conditional', () => {
      const steps = workflow.jobs.benchmark.steps
      const updateStep = steps.find(s =>
        s.name?.toLowerCase().includes('update') &&
        s.name?.toLowerCase().includes('baseline')
      )
      expect(updateStep?.if).toContain('update_baseline')
    })
  })

  describe('regression detection', () => {
    it('benchmark runner supports fail-on-regression flag', () => {
      const steps = workflow.jobs.benchmark.steps
      const benchmarkStep = steps.find(s =>
        s.run?.includes('runner.ts')
      )
      expect(benchmarkStep?.run).toContain('fail-on-regression')
    })

    it('benchmark runner supports baseline comparison', () => {
      const steps = workflow.jobs.benchmark.steps
      const benchmarkStep = steps.find(s =>
        s.run?.includes('runner.ts')
      )
      expect(benchmarkStep?.run).toContain('baseline')
    })
  })

  describe('slack alerting', () => {
    it('slack alert triggers on failure', () => {
      const steps = workflow.jobs.benchmark.steps
      const slackStep = steps.find(s =>
        s.name?.toLowerCase().includes('slack') ||
        s.name?.toLowerCase().includes('alert')
      )
      expect(slackStep?.if).toContain('failure()')
    })

    it('slack alert includes regression information', () => {
      const steps = workflow.jobs.benchmark.steps
      const slackStep = steps.find(s =>
        s.name?.toLowerCase().includes('slack') ||
        s.name?.toLowerCase().includes('alert')
      )
      expect(slackStep?.run).toContain('Regression')
    })

    it('slack alert has continue-on-error', () => {
      const steps = workflow.jobs.benchmark.steps
      const slackStep = steps.find(s =>
        s.name?.toLowerCase().includes('slack') ||
        s.name?.toLowerCase().includes('alert')
      )
      expect(slackStep?.['continue-on-error']).toBe(true)
    })
  })

  describe('artifacts and retention', () => {
    it('uploads benchmark artifacts', () => {
      const steps = workflow.jobs.benchmark.steps
      const artifactStep = steps.find(s =>
        s.uses?.includes('upload-artifact')
      )
      expect(artifactStep?.with?.name).toBe('benchmark-results')
    })

    it('artifact upload runs always', () => {
      const steps = workflow.jobs.benchmark.steps
      const artifactStep = steps.find(s =>
        s.uses?.includes('upload-artifact')
      )
      expect(artifactStep?.if).toContain('always()')
    })

    it('artifacts have retention policy', () => {
      const steps = workflow.jobs.benchmark.steps
      const artifactStep = steps.find(s =>
        s.uses?.includes('upload-artifact')
      )
      expect(artifactStep?.with?.['retention-days']).toBeDefined()
    })

    it('includes both results.json and benchmark-output.json', () => {
      const steps = workflow.jobs.benchmark.steps
      const artifactStep = steps.find(s =>
        s.uses?.includes('upload-artifact')
      )
      expect(artifactStep?.with?.path).toContain('results.json')
      expect(artifactStep?.with?.path).toContain('benchmark-output.json')
    })
  })
})
