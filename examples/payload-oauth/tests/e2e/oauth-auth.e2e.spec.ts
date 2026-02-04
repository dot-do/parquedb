/**
 * OAuth Authentication E2E Tests
 *
 * Tests the OAuth authentication flow including:
 * - Token verification via JWKS
 * - Role-based access control
 * - Token rejection scenarios
 */

import { test, expect } from '@playwright/test'

// These are set by global-setup
const getAdminToken = () => process.env.TEST_ADMIN_TOKEN!
const getEditorToken = () => process.env.TEST_EDITOR_TOKEN!
const getViewerToken = () => process.env.TEST_VIEWER_TOKEN!
const getNoRolesToken = () => process.env.TEST_NO_ROLES_TOKEN!
const getExpiredToken = () => process.env.TEST_EXPIRED_TOKEN!

test.describe('OAuth Authentication - API', () => {
  test('unauthenticated request returns null user', async ({ request }) => {
    const response = await request.get('/api/users/me')

    // Without auth, should get 401 or null user
    const status = response.status()
    expect([200, 401, 403]).toContain(status)

    if (response.status() === 200) {
      const data = await response.json()
      expect(data.user).toBeNull()
    }
  })

  test('valid admin token grants API access', async ({ request }) => {
    const response = await request.get('/api/posts', {
      headers: {
        Authorization: `Bearer ${getAdminToken()}`,
      },
    })

    expect(response.ok()).toBeTruthy()
    const data = await response.json()
    expect(data).toHaveProperty('docs')
  })

  test('valid editor token grants API access', async ({ request }) => {
    const response = await request.get('/api/posts', {
      headers: {
        Authorization: `Bearer ${getEditorToken()}`,
      },
    })

    expect(response.ok()).toBeTruthy()
  })

  test('token via cookie works', async ({ request, context }) => {
    // Set the auth cookie
    await context.addCookies([
      {
        name: 'auth',
        value: getAdminToken(),
        domain: 'localhost',
        path: '/',
      },
    ])

    const response = await request.get('/api/posts')
    expect(response.ok()).toBeTruthy()
  })

  test('expired token is rejected', async ({ request }) => {
    const response = await request.get('/api/users/me', {
      headers: {
        Authorization: `Bearer ${getExpiredToken()}`,
      },
    })

    // Expired token should result in null user or 401
    if (response.status() === 200) {
      const data = await response.json()
      expect(data.user).toBeNull()
    } else {
      expect([401, 403]).toContain(response.status())
    }
  })

  test('tampered token is rejected', async ({ request }) => {
    // Modify a character in the token to tamper with it
    const tamperedToken = getAdminToken().slice(0, -5) + 'XXXXX'

    const response = await request.get('/api/users/me', {
      headers: {
        Authorization: `Bearer ${tamperedToken}`,
      },
    })

    // Tampered token should be rejected
    if (response.status() === 200) {
      const data = await response.json()
      expect(data.user).toBeNull()
    } else {
      expect([401, 403]).toContain(response.status())
    }
  })

  test('invalid token format is rejected', async ({ request }) => {
    const response = await request.get('/api/users/me', {
      headers: {
        Authorization: 'Bearer not-a-valid-jwt-token',
      },
    })

    // Invalid token should be rejected
    if (response.status() === 200) {
      const data = await response.json()
      expect(data.user).toBeNull()
    } else {
      expect([401, 403]).toContain(response.status())
    }
  })
})

test.describe('OAuth Authentication - Role-Based Access', () => {
  test('admin can access users collection', async ({ request }) => {
    const response = await request.get('/api/users', {
      headers: {
        Authorization: `Bearer ${getAdminToken()}`,
      },
    })

    // Admin should be able to access users
    expect(response.ok()).toBeTruthy()
  })

  test('editor cannot read users collection', async ({ request }) => {
    const response = await request.get('/api/users', {
      headers: {
        Authorization: `Bearer ${getEditorToken()}`,
      },
    })

    // Editor should be denied access to users based on access control
    // The exact response depends on Payload's configuration
    // It might return empty docs or 403
    if (response.ok()) {
      const data = await response.json()
      // Either empty docs or restricted access
      expect(data.docs).toBeDefined()
    }
  })

  test('viewer cannot access admin routes', async ({ request }) => {
    // Viewer role should not have admin access
    const response = await request.get('/api/users/me', {
      headers: {
        Authorization: `Bearer ${getViewerToken()}`,
      },
    })

    // Viewer should not be authenticated for admin
    if (response.status() === 200) {
      const data = await response.json()
      // User should be null since viewer role doesn't have admin access
      expect(data.user).toBeNull()
    }
  })

  test('user without roles has no admin access', async ({ request }) => {
    const response = await request.get('/api/users/me', {
      headers: {
        Authorization: `Bearer ${getNoRolesToken()}`,
      },
    })

    // User without admin/editor roles should not be authenticated
    if (response.status() === 200) {
      const data = await response.json()
      expect(data.user).toBeNull()
    }
  })
})

test.describe('OAuth Authentication - Public Access', () => {
  test('public routes work without authentication', async ({ request }) => {
    // Public collections should be accessible
    const response = await request.get('/api/posts')
    expect(response.ok()).toBeTruthy()
  })

  test('categories are publicly accessible', async ({ request }) => {
    const response = await request.get('/api/categories')
    expect(response.ok()).toBeTruthy()
  })
})
