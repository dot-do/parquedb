import { test, expect } from '@playwright/test'

/**
 * E2E tests for Payload Admin Panel with ParqueDB
 *
 * These tests verify that the admin UI loads and renders correctly.
 * Tests requiring authentication are limited due to a known issue
 * with the ParqueDB event sourcing where findOne returns null immediately
 * after create (tracked as a core ParqueDB issue).
 */

test.describe('Payload Admin Panel with ParqueDB', () => {
  test('admin login page loads correctly', async ({ page }) => {
    await page.goto('http://localhost:3000/admin/login')
    await page.waitForLoadState('networkidle')

    // Verify login form is present
    const emailInput = page.locator('#field-email')
    const passwordInput = page.locator('#field-password')
    const loginButton = page.locator('button[type="submit"]')

    await expect(emailInput).toBeVisible()
    await expect(passwordInput).toBeVisible()
    await expect(loginButton).toBeVisible()
  })

  test('admin redirects to login when not authenticated', async ({ page }) => {
    await page.goto('http://localhost:3000/admin')
    await page.waitForLoadState('networkidle')

    // Should redirect to login
    await expect(page).toHaveURL(/\/admin\/login/)
  })

  test('forgot password link is visible', async ({ page }) => {
    await page.goto('http://localhost:3000/admin/login')
    await page.waitForLoadState('networkidle')

    const forgotPasswordLink = page.locator('a[href="/admin/forgot"]')
    await expect(forgotPasswordLink).toBeVisible()
  })

  test('API endpoint returns proper response', async ({ page }) => {
    // Test that the API is accessible
    const response = await page.request.get('http://localhost:3000/api/users/me')
    expect(response.status()).toBe(200)

    const data = await response.json()
    // Should return null user when not authenticated
    expect(data.user).toBeNull()
  })

  test('CSS and styles are loaded', async ({ page }) => {
    await page.goto('http://localhost:3000/admin/login')
    await page.waitForLoadState('networkidle')

    // Check that the page has proper styling (not broken CSS)
    const loginButton = page.locator('button[type="submit"]')
    const buttonBox = await loginButton.boundingBox()

    // Button should have reasonable dimensions (not collapsed)
    expect(buttonBox).not.toBeNull()
    expect(buttonBox!.width).toBeGreaterThan(50)
    expect(buttonBox!.height).toBeGreaterThan(10) // Adjusted for actual Payload button height
  })
})
