/**
 * Playwright Global Setup
 *
 * This runs once before all tests to:
 * 1. Start the local JWKS server
 * 2. Generate test tokens
 * 3. Write tokens to a file for tests to use
 *
 * Note: The Next.js app is started by Playwright's webServer config.
 */

import { startJWKSServer, type JWKSServerResult } from './test-utils/jwt-server'
import { generateAllTestTokens, writeTokensToFile } from './test-utils/test-tokens'
import path from 'path'
import { fileURLToPath } from 'url'
import fs from 'fs/promises'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const JWKS_PORT = 3456
const APP_PORT = 3000
const TOKENS_FILE = path.join(__dirname, '.test-tokens.json')
const STATE_FILE = path.join(__dirname, '.test-state.json')

let jwksServer: JWKSServerResult | null = null

export default async function globalSetup() {
  console.log('\n=== Global Setup Starting ===\n')

  try {
    // 1. Start JWKS server
    console.log(`Starting JWKS server on port ${JWKS_PORT}...`)
    jwksServer = await startJWKSServer(JWKS_PORT)
    console.log(`JWKS server started: ${jwksServer.jwksUri}`)

    // 2. Generate test tokens
    console.log('Generating test tokens...')
    const tokens = await generateAllTestTokens(
      jwksServer.privateKey,
      jwksServer.kid
    )

    // 3. Write tokens to file (for tests to read)
    await writeTokensToFile(tokens, TOKENS_FILE)

    // 4. Save state for teardown (server PID tracking)
    await fs.writeFile(STATE_FILE, JSON.stringify({
      jwksPort: JWKS_PORT,
      appPort: APP_PORT,
      tokensFile: TOKENS_FILE,
      jwksUri: jwksServer.jwksUri,
    }))

    // 5. Export tokens as environment variables for tests
    // These are available in the test process
    process.env.TEST_ADMIN_TOKEN = tokens.admin
    process.env.TEST_EDITOR_TOKEN = tokens.editor
    process.env.TEST_VIEWER_TOKEN = tokens.viewer
    process.env.TEST_NO_ROLES_TOKEN = tokens.noRoles
    process.env.TEST_EXPIRED_TOKEN = tokens.expired
    process.env.TEST_JWKS_URI = jwksServer.jwksUri
    process.env.TEST_APP_URL = `http://localhost:${APP_PORT}`

    console.log('\n=== Global Setup Complete ===\n')
  } catch (error) {
    console.error('Global setup failed:', error)
    // Clean up on failure
    if (jwksServer) {
      const { stopJWKSServer } = await import('./test-utils/jwt-server')
      await stopJWKSServer(jwksServer.server)
    }
    throw error
  }
}
