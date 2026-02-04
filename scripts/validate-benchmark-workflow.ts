#!/usr/bin/env npx tsx
/**
 * Validate the E2E benchmark workflow configuration
 *
 * This script parses the benchmark workflow YAML and validates:
 * - Required jobs exist
 * - Required steps are present
 * - Environment variables are configured
 * - Secrets are referenced correctly
 *
 * Usage: npx tsx scripts/validate-benchmark-workflow.ts
 */

import * as fs from 'fs'
import * as path from 'path'
import * as yaml from 'yaml'

const WORKFLOW_PATH = '.github/workflows/e2e-benchmark.yml'

interface ValidationResult {
  valid: boolean
  errors: string[]
  warnings: string[]
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
}

interface Workflow {
  name: string
  on: {
    schedule?: Array<{ cron: string }>
    workflow_dispatch?: {
      inputs?: Record<string, {
        description: string
        required?: boolean
        default?: string | boolean
        type?: string
      }>
    }
  }
  env?: Record<string, string>
  jobs: Record<string, WorkflowJob>
}

function validateWorkflow(workflowPath: string): ValidationResult {
  const errors: string[] = []
  const warnings: string[] = []

  // Check file exists
  if (!fs.existsSync(workflowPath)) {
    return {
      valid: false,
      errors: [`Workflow file not found: ${workflowPath}`],
      warnings: [],
    }
  }

  // Parse YAML
  let workflow: Workflow
  try {
    const content = fs.readFileSync(workflowPath, 'utf8')
    workflow = yaml.parse(content) as Workflow
  } catch (e) {
    return {
      valid: false,
      errors: [`Failed to parse workflow YAML: ${e}`],
      warnings: [],
    }
  }

  // Validate name
  if (!workflow.name) {
    errors.push('Workflow must have a name')
  }

  // Validate triggers
  if (!workflow.on) {
    errors.push('Workflow must have triggers (on)')
  } else {
    // Check for schedule
    if (!workflow.on.schedule || workflow.on.schedule.length === 0) {
      errors.push('Workflow must have schedule trigger for automated benchmarking')
    }

    // Check for workflow_dispatch
    if (!workflow.on.workflow_dispatch) {
      errors.push('Workflow must have workflow_dispatch trigger for manual runs')
    } else {
      const inputs = workflow.on.workflow_dispatch.inputs || {}

      // Check required inputs
      const requiredInputs = ['url', 'iterations', 'fail_on_regression', 'update_baseline']
      for (const input of requiredInputs) {
        if (!inputs[input]) {
          errors.push(`Missing required input: ${input}`)
        }
      }

      // Check input types
      if (inputs.fail_on_regression && inputs.fail_on_regression.type !== 'boolean') {
        warnings.push('fail_on_regression should be boolean type')
      }
      if (inputs.update_baseline && inputs.update_baseline.type !== 'boolean') {
        warnings.push('update_baseline should be boolean type')
      }
    }
  }

  // Validate environment variables
  const requiredEnvVars = ['WORKER_URL', 'R2_BUCKET', 'ENVIRONMENT']
  for (const envVar of requiredEnvVars) {
    if (!workflow.env?.[envVar]) {
      errors.push(`Missing required environment variable: ${envVar}`)
    }
  }

  // Validate jobs
  if (!workflow.jobs || Object.keys(workflow.jobs).length === 0) {
    errors.push('Workflow must have at least one job')
  } else {
    // Check for benchmark job
    if (!workflow.jobs.benchmark) {
      errors.push('Workflow must have a "benchmark" job')
    } else {
      const job = workflow.jobs.benchmark

      // Validate runner
      if (job['runs-on'] !== 'ubuntu-latest') {
        warnings.push('Benchmark job should run on ubuntu-latest')
      }

      // Validate timeout
      if (!job['timeout-minutes']) {
        warnings.push('Benchmark job should have a timeout-minutes setting')
      } else if (job['timeout-minutes'] > 60) {
        warnings.push('Benchmark job timeout exceeds 60 minutes')
      }

      // Validate required steps
      const steps = job.steps || []

      const requiredSteps = [
        { pattern: /actions\/checkout/, name: 'Checkout' },
        { pattern: /actions\/setup-node/, name: 'Setup Node.js' },
        { pattern: /npm ci|pnpm install/, name: 'Install dependencies' },
        { pattern: /wrangler/, name: 'Wrangler setup' },
        { pattern: /r2 object get.*baseline/, name: 'Download baseline' },
        { pattern: /runner\.ts/, name: 'Run benchmark' },
        { pattern: /r2 object put/, name: 'Upload results' },
        { pattern: /upload-artifact/, name: 'Upload artifacts' },
        { pattern: /GITHUB_STEP_SUMMARY/, name: 'Display summary' },
        { pattern: /slack|alert/i, name: 'Slack notification' },
      ]

      for (const required of requiredSteps) {
        const found = steps.some(step => {
          const uses = step.uses || ''
          const run = step.run || ''
          const name = step.name || ''
          return required.pattern.test(uses) ||
                 required.pattern.test(run) ||
                 required.pattern.test(name)
        })

        if (!found) {
          errors.push(`Missing required step: ${required.name}`)
        }
      }

      // Validate secrets are used correctly
      const secretPatterns = [
        { pattern: /CLOUDFLARE_API_TOKEN/, name: 'CLOUDFLARE_API_TOKEN' },
        { pattern: /CLOUDFLARE_ACCOUNT_ID/, name: 'CLOUDFLARE_ACCOUNT_ID' },
      ]

      for (const secret of secretPatterns) {
        const found = steps.some(step => {
          const env = JSON.stringify(step.env || {})
          return secret.pattern.test(env)
        })

        if (!found) {
          errors.push(`Missing required secret reference: ${secret.name}`)
        }
      }

      // Check for Slack webhook reference
      const hasSlackWebhook = steps.some(step => {
        const run = step.run || ''
        return /E2E_SLACK_WEBHOOK_URL/.test(run)
      })

      if (!hasSlackWebhook) {
        warnings.push('Slack webhook secret (E2E_SLACK_WEBHOOK_URL) not found')
      }

      // Validate baseline handling
      const baselineStep = steps.find(s =>
        s.name?.toLowerCase().includes('baseline') &&
        s.name?.toLowerCase().includes('download')
      )

      if (baselineStep) {
        if (!baselineStep['continue-on-error']) {
          warnings.push('Baseline download step should have continue-on-error: true')
        }
        if (!baselineStep.id) {
          warnings.push('Baseline download step should have an id for output reference')
        }
      }

      // Validate artifact retention
      const artifactStep = steps.find(s =>
        s.uses?.includes('upload-artifact')
      )

      if (artifactStep && !artifactStep.with?.['retention-days']) {
        warnings.push('Artifact upload should have retention-days setting')
      }
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  }
}

function main() {
  console.log('Validating E2E Benchmark Workflow...\n')

  const workflowPath = path.resolve(WORKFLOW_PATH)
  const result = validateWorkflow(workflowPath)

  if (result.errors.length > 0) {
    console.log('ERRORS:')
    for (const error of result.errors) {
      console.log(`  [x] ${error}`)
    }
    console.log()
  }

  if (result.warnings.length > 0) {
    console.log('WARNINGS:')
    for (const warning of result.warnings) {
      console.log(`  [!] ${warning}`)
    }
    console.log()
  }

  if (result.valid) {
    console.log('Workflow validation PASSED')
    if (result.warnings.length > 0) {
      console.log(`(${result.warnings.length} warning(s))`)
    }
    process.exit(0)
  } else {
    console.log('Workflow validation FAILED')
    console.log(`${result.errors.length} error(s), ${result.warnings.length} warning(s)`)
    process.exit(1)
  }
}

main()
