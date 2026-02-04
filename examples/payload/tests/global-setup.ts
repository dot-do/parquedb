/**
 * Playwright Global Setup
 *
 * Seeds the test user before running E2E tests.
 * Uses fetch to call the Payload API since the dev server is already running.
 */

export default async function globalSetup(): Promise<void> {
  const serverURL = 'http://localhost:3000'

  // Wait for server to be ready - check the admin login page
  // This may take a while as Next.js compiles on first request
  let retries = 120 // 2 minutes
  while (retries > 0) {
    try {
      const response = await fetch(`${serverURL}/admin/login`, {
        method: 'GET',
      })
      // Accept any response that indicates the server is up
      if (response.status !== 0) {
        console.log(`Server responded with status ${response.status}`)
        break
      }
    } catch (error) {
      // Server not ready yet - this is expected during startup
      if (retries % 10 === 0) {
        console.log(`Waiting for server... (${retries}s remaining)`)
      }
    }
    retries--
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }

  if (retries === 0) {
    throw new Error('Server did not become ready in time')
  }

  // Give server a moment to fully stabilize
  await new Promise((resolve) => setTimeout(resolve, 2000))

  const testUser = {
    email: 'admin@test.com',
    password: 'testpassword123',
  }

  // Try to login first to check if user exists
  const loginResponse = await fetch(`${serverURL}/api/users/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(testUser),
  })

  if (loginResponse.ok) {
    console.log('Test user already exists and login successful')
    return
  }

  // Check if any users exist by trying to access the users endpoint
  const usersResponse = await fetch(`${serverURL}/api/users`, {
    method: 'GET',
    headers: { 'Content-Type': 'application/json' },
  })

  // If we get a 200 with empty docs, or a 401/403, we need to create the first user
  let needsFirstUser = false
  if (usersResponse.ok) {
    const data = await usersResponse.json()
    needsFirstUser = !data.docs || data.docs.length === 0
  } else {
    // If we can't access users API, assume we need to create first user
    needsFirstUser = true
  }

  if (needsFirstUser) {
    // Payload's first user creation endpoint
    const createResponse = await fetch(`${serverURL}/api/users/first-register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(testUser),
    })

    if (createResponse.ok) {
      console.log('Test user created via first-register')
      return
    }

    // Try the standard create endpoint (for when first-register doesn't exist)
    const standardCreateResponse = await fetch(`${serverURL}/api/users`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(testUser),
    })

    if (standardCreateResponse.ok) {
      console.log('Test user created via standard API')
      return
    }

    console.log('Note: Could not create test user automatically.')
    console.log('The first user may need to be created through the admin UI.')
    console.log(`Visit ${serverURL}/admin to create: email=${testUser.email}, password=${testUser.password}`)
  } else {
    console.log('Users exist but login failed. Check credentials.')
  }
}
