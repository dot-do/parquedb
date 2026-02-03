/**
 * ParqueDB Security Module
 *
 * Provides security utilities for ParqueDB applications:
 * - CSRF protection middleware
 * - Secure CORS configuration
 * - Token generation and verification
 *
 * @example
 * ```typescript
 * import { csrf, generateCsrfToken, verifyCsrfToken } from 'parquedb/security'
 *
 * // Apply CSRF middleware to mutation routes
 * app.use('/api/*', csrf({
 *   allowedOrigins: ['https://app.example.com'],
 * }))
 * ```
 *
 * @module
 */

export {
  // CSRF Middleware
  csrf,
  csrfToken,
  validateCsrf,

  // Token Functions
  generateCsrfToken,
  verifyCsrfToken,

  // CORS Helpers
  buildSecureCorsHeaders,
  getAllowedOriginHeader,

  // Types
  type CsrfOptions,
  type CsrfValidationResult,
  type CsrfTokenPayload,
} from './csrf'
