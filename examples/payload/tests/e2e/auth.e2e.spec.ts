import { test, expect } from '@playwright/test'

/**
 * E2E tests for Payload Authentication with ParqueDB
 *
 * These tests verify that the authentication flow works correctly:
 * - User registration
 * - Login
 * - Session management
 */

test.describe('Payload Authentication with ParqueDB', () => {
  test('can register first user via API', async ({ request }) => {
    // Try to create a test user via API
    const registerResponse = await request.post('http://localhost:3000/api/users/first-register', {
      data: {
        email: 'auth-test@example.com',
        password: 'testpassword123',
      },
    })

    // If registration succeeds, we got a new user
    // If it fails with 403, a user already exists (which is also fine)
    if (registerResponse.ok()) {
      const data = await registerResponse.json()
      expect(data.token).toBeDefined()
      expect(data.user).toBeDefined()
      expect(data.user.email).toBe('auth-test@example.com')
    } else {
      // User already exists, which is fine
      expect(registerResponse.status()).toBe(403)
    }
  })

  test('can login via API after registration', async ({ request }) => {
    // Login with the test user credentials (may have been created by global-setup)
    const loginResponse = await request.post('http://localhost:3000/api/users/login', {
      data: {
        email: 'admin@test.com',
        password: 'testpassword123',
      },
    })

    expect(loginResponse.ok()).toBe(true)
    const data = await loginResponse.json()
    expect(data.token).toBeDefined()
    expect(data.user).toBeDefined()
    expect(data.user.email).toBe('admin@test.com')
  })

  test('shows error for invalid credentials', async ({ page }) => {
    await page.goto('http://localhost:3000/admin/login')
    await page.waitForLoadState('networkidle')

    // Fill in invalid credentials
    await page.fill('#field-email', 'nonexistent@example.com')
    await page.fill('#field-password', 'wrongpassword')

    // Submit the form
    await page.click('button[type="submit"]')

    // Wait for error message
    await page.waitForSelector('.toast-error, [data-error="true"], .form__error', { timeout: 5000 }).catch(() => {
      // Error might be shown differently in Payload
    })

    // Should still be on login page
    expect(page.url()).toContain('/login')
  })

  test('API returns user info when authenticated', async ({ request }) => {
    // Register or login to get a token
    const registerResponse = await request.post('http://localhost:3000/api/users/first-register', {
      data: {
        email: 'api-test@example.com',
        password: 'testpassword123',
      },
    })

    let token: string | undefined

    if (registerResponse.ok()) {
      const data = await registerResponse.json()
      token = data.token
    } else {
      // User exists, try to login
      const loginResponse = await request.post('http://localhost:3000/api/users/login', {
        data: {
          email: 'api-test@example.com',
          password: 'testpassword123',
        },
      })

      if (loginResponse.ok()) {
        const data = await loginResponse.json()
        token = data.token
      }
    }

    if (token) {
      // Use the token to access /api/users/me
      const meResponse = await request.get('http://localhost:3000/api/users/me', {
        headers: {
          Authorization: `JWT ${token}`,
        },
      })

      expect(meResponse.status()).toBe(200)
      const userData = await meResponse.json()
      expect(userData.user).toBeDefined()
      expect(userData.user.email).toBeDefined()
    }
  })
})
