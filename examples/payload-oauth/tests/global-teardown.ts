/**
 * Playwright Global Teardown
 *
 * This runs once after all tests to:
 * 1. Clean up test token files
 * 2. Clean up test data directory
 *
 * Note: The JWKS server runs in the same process as global-setup,
 * so it will be cleaned up when the process exits.
 */

import path from 'path'
import fs from 'fs/promises'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const TOKENS_FILE = path.join(__dirname, '.test-tokens.json')
const STATE_FILE = path.join(__dirname, '.test-state.json')

export default async function globalTeardown() {
  console.log('\n=== Global Teardown Starting ===\n')

  try {
    // Clean up the token files
    try {
      await fs.unlink(TOKENS_FILE)
      console.log('Cleaned up test tokens file')
    } catch {
      // File may not exist
    }

    try {
      await fs.unlink(STATE_FILE)
      console.log('Cleaned up test state file')
    } catch {
      // File may not exist
    }

    // Clean up test data directory
    const testDataDir = path.join(__dirname, '..', 'test-data')
    try {
      await fs.rm(testDataDir, { recursive: true, force: true })
      console.log('Cleaned up test data directory')
    } catch {
      // Directory may not exist
    }

    console.log('\n=== Global Teardown Complete ===\n')
  } catch (error) {
    console.error('Global teardown error:', error)
    // Don't throw - we want other cleanup to continue
  }
}
