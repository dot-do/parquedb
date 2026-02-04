import { describe, it, expect, beforeAll } from 'vitest'
import { readFileSync } from 'fs'
import { resolve } from 'path'
import { parse as parseYaml } from 'yaml'

/**
 * Coverage Threshold Requirements Tests (RED Phase - parquedb-zbfw.1)
 *
 * These tests define the acceptance criteria for coverage enforcement in CI:
 * 1. CI must include coverage threshold flags
 * 2. CI must fail when coverage drops below 80% lines
 * 3. CI must fail when coverage drops below 80% functions
 * 4. Coverage report must be generated and accessible
 *
 * Requirements:
 * - Lines coverage: >= 80%
 * - Functions coverage: >= 80%
 * - Coverage report uploaded as artifact
 */

describe('CI Coverage Threshold Requirements', () => {
  let ciConfig: Record<string, unknown>
  let vitestConfig: string

  beforeAll(() => {
    // Read CI workflow configuration
    const ciYamlPath = resolve(__dirname, '../../../.github/workflows/ci.yml')
    const ciYaml = readFileSync(ciYamlPath, 'utf-8')
    ciConfig = parseYaml(ciYaml) as Record<string, unknown>

    // Read vitest configuration
    const vitestConfigPath = resolve(__dirname, '../../../vitest.config.ts')
    vitestConfig = readFileSync(vitestConfigPath, 'utf-8')
  })

  describe('Coverage threshold configuration', () => {
    it('should have coverage thresholds defined in vitest.config.ts', () => {
      // Verify thresholds are defined
      expect(vitestConfig).toContain('thresholds')
      expect(vitestConfig).toContain('lines: 80')
      expect(vitestConfig).toContain('functions: 80')
    })

    it('should require minimum 80% line coverage', () => {
      // The threshold should be at least 80%
      const linesMatch = vitestConfig.match(/lines:\s*(\d+)/)
      expect(linesMatch).not.toBeNull()
      const linesThreshold = parseInt(linesMatch![1], 10)
      expect(linesThreshold).toBeGreaterThanOrEqual(80)
    })

    it('should require minimum 80% function coverage', () => {
      // The threshold should be at least 80%
      const functionsMatch = vitestConfig.match(/functions:\s*(\d+)/)
      expect(functionsMatch).not.toBeNull()
      const functionsThreshold = parseInt(functionsMatch![1], 10)
      expect(functionsThreshold).toBeGreaterThanOrEqual(80)
    })
  })

  describe('CI workflow coverage enforcement', () => {
    it('should run tests with coverage in CI', () => {
      const jobs = ciConfig.jobs as Record<string, unknown>
      const testUnitJob = jobs['test-unit'] as Record<string, unknown>
      const steps = testUnitJob.steps as Array<Record<string, unknown>>

      // Find the test step
      const testStep = steps.find(
        (step) => typeof step.run === 'string' && step.run.includes('test')
      )

      expect(testStep).toBeDefined()
      expect(testStep!.run).toContain('--coverage')
    })

    it('should enforce coverage thresholds to fail CI on low coverage', () => {
      // REQUIREMENT: CI must fail when coverage drops below thresholds
      // This can be done via:
      // 1. Vitest's --coverage.thresholds.check flag (new approach)
      // 2. Setting coverage.thresholds in config with check: true
      // 3. Using a separate coverage check step

      const jobs = ciConfig.jobs as Record<string, unknown>
      const testUnitJob = jobs['test-unit'] as Record<string, unknown>
      const steps = testUnitJob.steps as Array<Record<string, unknown>>

      // Find the test step that runs with coverage
      const testStep = steps.find(
        (step) => typeof step.run === 'string' && step.run.includes('--coverage')
      )

      expect(testStep).toBeDefined()

      // The test command should include threshold enforcement
      // Either via CLI flags or vitest config with thresholds.check enabled
      const testCommand = testStep!.run as string

      // Check for threshold enforcement - at least one of these patterns should match:
      // 1. --coverage.thresholds.check (explicit CLI flag)
      // 2. Having thresholds in vitest.config.ts with automatic enforcement
      const hasExplicitThresholdCheck =
        testCommand.includes('--coverage.thresholds.check') ||
        testCommand.includes('--coverage.lines') ||
        testCommand.includes('--coverage.functions')

      // CRITICAL: This test SHOULD FAIL currently because CI doesn't enforce thresholds
      // The fix will be to add threshold enforcement to the CI command
      expect(hasExplicitThresholdCheck).toBe(true)
    })

    it('should use threshold values of at least 80% for lines', () => {
      const jobs = ciConfig.jobs as Record<string, unknown>
      const testUnitJob = jobs['test-unit'] as Record<string, unknown>
      const steps = testUnitJob.steps as Array<Record<string, unknown>>

      const testStep = steps.find(
        (step) => typeof step.run === 'string' && step.run.includes('--coverage')
      )

      const testCommand = testStep?.run as string | undefined

      // If using CLI flags, verify the threshold values
      if (testCommand?.includes('--coverage.lines')) {
        const match = testCommand.match(/--coverage\.lines[=\s]+(\d+)/)
        if (match) {
          expect(parseInt(match[1], 10)).toBeGreaterThanOrEqual(80)
        }
      }

      // If using vitest config thresholds, they should be enforced
      // This relies on the coverage.thresholds configuration having effect
      // Currently, vitest does NOT fail on threshold violations by default
      // We need --coverage.thresholds.check=true or equivalent

      // REQUIREMENT: Verify threshold enforcement is enabled in CI
      expect(
        testCommand?.includes('--coverage.thresholds') ||
        testCommand?.includes('--coverage.lines=80') ||
        vitestConfig.includes('check: true') ||
        vitestConfig.includes("check: 'warn'")
      ).toBe(true)
    })

    it('should use threshold values of at least 80% for functions', () => {
      const jobs = ciConfig.jobs as Record<string, unknown>
      const testUnitJob = jobs['test-unit'] as Record<string, unknown>
      const steps = testUnitJob.steps as Array<Record<string, unknown>>

      const testStep = steps.find(
        (step) => typeof step.run === 'string' && step.run.includes('--coverage')
      )

      const testCommand = testStep?.run as string | undefined

      // If using CLI flags, verify the threshold values
      if (testCommand?.includes('--coverage.functions')) {
        const match = testCommand.match(/--coverage\.functions[=\s]+(\d+)/)
        if (match) {
          expect(parseInt(match[1], 10)).toBeGreaterThanOrEqual(80)
        }
      }

      // Same requirement as above - enforcement must be enabled
      expect(
        testCommand?.includes('--coverage.thresholds') ||
        testCommand?.includes('--coverage.functions=80') ||
        vitestConfig.includes('check: true') ||
        vitestConfig.includes("check: 'warn'")
      ).toBe(true)
    })
  })

  describe('Coverage report accessibility', () => {
    it('should upload coverage report as artifact', () => {
      const jobs = ciConfig.jobs as Record<string, unknown>
      const testUnitJob = jobs['test-unit'] as Record<string, unknown>
      const steps = testUnitJob.steps as Array<Record<string, unknown>>

      // Find artifact upload step
      const uploadStep = steps.find(
        (step) => step.uses?.toString().includes('upload-artifact')
      )

      expect(uploadStep).toBeDefined()
      expect((uploadStep as Record<string, unknown>).with).toBeDefined()

      const withConfig = (uploadStep as Record<string, unknown>).with as Record<string, unknown>
      expect(withConfig.path).toContain('coverage')
    })

    it('should generate coverage report in multiple formats', () => {
      // Verify vitest.config.ts configures multiple report formats
      expect(vitestConfig).toContain('reporter')

      // Should include at least text (console), html (local viewing), and lcov (CI tools)
      expect(vitestConfig).toMatch(/reporter.*text/s)
      expect(vitestConfig).toMatch(/reporter.*html/s)
      expect(vitestConfig).toMatch(/reporter.*lcov/s)
    })

    it('should set reasonable retention period for coverage artifacts', () => {
      const jobs = ciConfig.jobs as Record<string, unknown>
      const testUnitJob = jobs['test-unit'] as Record<string, unknown>
      const steps = testUnitJob.steps as Array<Record<string, unknown>>

      const uploadStep = steps.find(
        (step) => step.uses?.toString().includes('upload-artifact')
      )

      const withConfig = (uploadStep as Record<string, unknown>).with as Record<string, unknown>

      // Retention should be at least 1 day but not excessive
      const retention = withConfig['retention-days'] as number
      expect(retention).toBeGreaterThanOrEqual(1)
      expect(retention).toBeLessThanOrEqual(30)
    })
  })

  describe('CI failure behavior', () => {
    it('should cause CI to fail when line coverage is below 80%', () => {
      // This is a specification test - the implementation must ensure:
      // 1. Vitest exits with non-zero code when coverage < 80%
      // 2. CI job fails as a result

      // The current CI command: npm run test:unit -- --run --coverage
      // Does NOT enforce thresholds - vitest's thresholds config only WARNS by default

      // REQUIREMENT: The test command must include enforcement
      const jobs = ciConfig.jobs as Record<string, unknown>
      const testUnitJob = jobs['test-unit'] as Record<string, unknown>
      const steps = testUnitJob.steps as Array<Record<string, unknown>>

      const testStep = steps.find(
        (step) => typeof step.run === 'string' && step.run.includes('--coverage')
      )

      // Must have threshold check enabled to fail on low coverage
      const testCommand = testStep!.run as string
      const enforcesCoverage =
        testCommand.includes('--coverage.thresholds.check') ||
        testCommand.includes('--coverage.thresholds.100') ||
        vitestConfig.includes('thresholds: {') && vitestConfig.includes('check: true')

      // This assertion documents the requirement and SHOULD FAIL on current CI
      expect(enforcesCoverage).toBe(true)
    })

    it('should cause CI to fail when function coverage is below 80%', () => {
      // Same requirement as line coverage - function coverage must be enforced
      const jobs = ciConfig.jobs as Record<string, unknown>
      const testUnitJob = jobs['test-unit'] as Record<string, unknown>
      const steps = testUnitJob.steps as Array<Record<string, unknown>>

      const testStep = steps.find(
        (step) => typeof step.run === 'string' && step.run.includes('--coverage')
      )

      const testCommand = testStep!.run as string
      const enforcesCoverage =
        testCommand.includes('--coverage.thresholds.check') ||
        testCommand.includes('--coverage.functions') ||
        vitestConfig.includes('thresholds: {') && vitestConfig.includes('check: true')

      expect(enforcesCoverage).toBe(true)
    })
  })
})
