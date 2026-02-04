/**
 * CSRF Validation Handler Tests
 *
 * Tests for CSRF validation functions used by worker handlers.
 * Tests origin/referer validation, X-Requested-With header checks,
 * and error response formatting.
 */

import { describe, it, expect } from 'vitest'
import {
  validateCsrfRequest,
  csrfErrorResponse,
} from '../../../../src/worker/handlers/csrf-validation'

describe('CSRF Validation', () => {
  // ===========================================================================
  // validateCsrfRequest - Valid Requests
  // ===========================================================================

  describe('validateCsrfRequest - valid requests', () => {
    it('should accept same-origin request with X-Requested-With', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
        headers: {
          'Origin': 'https://api.example.com',
          'X-Requested-With': 'XMLHttpRequest',
        },
      })

      const result = validateCsrfRequest(request)

      expect(result.valid).toBe(true)
      expect(result.reason).toBeUndefined()
    })

    it('should accept request with ParqueDB X-Requested-With value', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
        headers: {
          'Origin': 'https://api.example.com',
          'X-Requested-With': 'ParqueDB',
        },
      })

      const result = validateCsrfRequest(request)

      expect(result.valid).toBe(true)
    })

    it('should accept request with fetch X-Requested-With value', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
        headers: {
          'Origin': 'https://api.example.com',
          'X-Requested-With': 'fetch',
        },
      })

      const result = validateCsrfRequest(request)

      expect(result.valid).toBe(true)
    })

    it('should accept request with Referer instead of Origin', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
        headers: {
          'Referer': 'https://api.example.com/dashboard',
          'X-Requested-With': 'XMLHttpRequest',
        },
      })

      const result = validateCsrfRequest(request)

      expect(result.valid).toBe(true)
    })

    it('should accept request with allowed origin', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
        headers: {
          'Origin': 'https://admin.example.com',
          'X-Requested-With': 'XMLHttpRequest',
        },
      })

      const result = validateCsrfRequest(request, ['https://admin.example.com'])

      expect(result.valid).toBe(true)
    })

    it('should accept request with wildcard allowed origins', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
        headers: {
          'Origin': 'https://any-domain.com',
          'X-Requested-With': 'XMLHttpRequest',
        },
      })

      const result = validateCsrfRequest(request, ['*'])

      expect(result.valid).toBe(true)
    })

    it('should accept API request with no Origin/Referer but X-Requested-With', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
        headers: {
          'X-Requested-With': 'XMLHttpRequest',
        },
      })

      const result = validateCsrfRequest(request)

      expect(result.valid).toBe(true)
    })

    it('should accept custom X-Requested-With value', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
        headers: {
          'Origin': 'https://api.example.com',
          'X-Requested-With': 'CustomClient',
        },
      })

      const result = validateCsrfRequest(request)

      expect(result.valid).toBe(true)
    })
  })

  // ===========================================================================
  // validateCsrfRequest - Invalid Requests
  // ===========================================================================

  describe('validateCsrfRequest - invalid requests', () => {
    it('should reject request with no Origin, Referer, or X-Requested-With', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
      })

      const result = validateCsrfRequest(request)

      expect(result.valid).toBe(false)
      expect(result.reason).toContain('Missing Origin/Referer')
    })

    it('should reject cross-origin request without allowed origins', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
        headers: {
          'Origin': 'https://evil.com',
          'X-Requested-With': 'XMLHttpRequest',
        },
      })

      const result = validateCsrfRequest(request)

      expect(result.valid).toBe(false)
      expect(result.reason).toContain("Origin 'https://evil.com' is not allowed")
    })

    it('should reject cross-origin Referer without allowed origins', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
        headers: {
          'Referer': 'https://evil.com/attack',
          'X-Requested-With': 'XMLHttpRequest',
        },
      })

      const result = validateCsrfRequest(request)

      expect(result.valid).toBe(false)
      expect(result.reason).toContain("Referer origin 'https://evil.com' is not allowed")
    })

    it('should reject request with Origin but missing X-Requested-With', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
        headers: {
          'Origin': 'https://api.example.com',
        },
      })

      const result = validateCsrfRequest(request)

      expect(result.valid).toBe(false)
      expect(result.reason).toContain('Missing X-Requested-With')
    })

    it('should reject request with invalid Referer URL', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
        headers: {
          'Referer': 'not-a-valid-url',
          'X-Requested-With': 'XMLHttpRequest',
        },
      })

      const result = validateCsrfRequest(request)

      expect(result.valid).toBe(false)
      expect(result.reason).toContain('Invalid Referer header')
    })

    it('should reject cross-origin even if in non-matching allowed origins', () => {
      const request = new Request('https://api.example.com/ns/users', {
        method: 'POST',
        headers: {
          'Origin': 'https://evil.com',
          'X-Requested-With': 'XMLHttpRequest',
        },
      })

      const result = validateCsrfRequest(request, ['https://admin.example.com'])

      expect(result.valid).toBe(false)
      expect(result.reason).toContain("Origin 'https://evil.com' is not allowed")
    })
  })

  // ===========================================================================
  // csrfErrorResponse
  // ===========================================================================

  describe('csrfErrorResponse', () => {
    it('should return 403 status', () => {
      const response = csrfErrorResponse('Test reason')

      expect(response.status).toBe(403)
    })

    it('should include error details in body', async () => {
      const response = csrfErrorResponse('Missing Origin header')
      const body = (await response.json()) as Record<string, unknown>

      expect(body.error).toBe('CSRF validation failed')
      expect(body.code).toBe('CSRF_VALIDATION_FAILED')
      expect(body.reason).toBe('Missing Origin header')
      expect(body.hint).toContain('X-Requested-With')
    })

    it('should return JSON content type', () => {
      const response = csrfErrorResponse('Test')

      expect(response.headers.get('Content-Type')).toContain('application/json')
    })
  })
})
