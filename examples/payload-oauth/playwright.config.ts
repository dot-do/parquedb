import { defineConfig, devices } from '@playwright/test'
import path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

/**
 * Playwright configuration for E2E tests
 *
 * @see https://playwright.dev/docs/test-configuration
 */
export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: 'html',
  use: {
    baseURL: process.env.TEST_APP_URL || 'http://localhost:3000',
    trace: 'on-first-retry',
  },

  // Global setup starts JWKS server and generates tokens
  globalSetup: path.join(__dirname, 'tests/global-setup.ts'),
  globalTeardown: path.join(__dirname, 'tests/global-teardown.ts'),

  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],

  // The app is started by global-setup or should be run externally
  // webServer is configured conditionally
  webServer: {
    command: 'pnpm dev',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
    timeout: 120 * 1000,
    env: {
      WORKOS_JWKS_URI: process.env.TEST_JWKS_URI || 'http://localhost:3456/.well-known/jwks.json',
      PAYLOAD_SECRET: 'test-secret-for-e2e-testing',
      DATA_DIR: path.join(__dirname, 'test-data'),
    },
  },
})
