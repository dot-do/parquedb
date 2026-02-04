/**
 * Playwright Global Teardown
 *
 * Cleanup after E2E tests complete.
 * Currently minimal - just logs completion.
 */

export default async function globalTeardown(): Promise<void> {
  console.log('E2E tests completed')
}
