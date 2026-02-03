/**
 * Slug Validation Utilities
 *
 * Provides consistent slug validation across the codebase.
 * Slugs must be lowercase alphanumeric with hyphens, 1-64 characters.
 *
 * Rules:
 * - Short slugs (1-3 chars): Only alphanumeric, no hyphens
 * - Long slugs (3-64 chars): Alphanumeric and hyphens, cannot start/end with hyphen
 *
 * @module utils/slug
 */

/**
 * Minimum length for a valid slug
 */
export const SLUG_MIN_LENGTH = 1

/**
 * Maximum length for a valid slug
 */
export const SLUG_MAX_LENGTH = 64

/**
 * Regex pattern for slugs 3+ characters (must not start/end with hyphen)
 */
const SLUG_PATTERN_LONG = /^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$/

/**
 * Regex pattern for short slugs (1-3 characters, alphanumeric only)
 */
const SLUG_PATTERN_SHORT = /^[a-z0-9]{1,3}$/

/**
 * Validate slug format
 *
 * A valid slug:
 * - Is 1-64 characters long
 * - Contains only lowercase letters, numbers, and hyphens
 * - Cannot start or end with a hyphen (for slugs with hyphens)
 * - Short slugs (1-3 chars) must be alphanumeric only
 *
 * @param slug - The slug to validate
 * @returns true if the slug is valid, false otherwise
 *
 * @example
 * ```ts
 * isValidSlug('my-dataset')     // true
 * isValidSlug('ab')             // true (short slug)
 * isValidSlug('my_dataset')     // false (underscores not allowed)
 * isValidSlug('My-Dataset')     // false (uppercase not allowed)
 * isValidSlug('-my-dataset')    // false (cannot start with hyphen)
 * isValidSlug('a-b')            // false (too short for hyphen)
 * ```
 */
export function isValidSlug(slug: string): boolean {
  if (typeof slug !== 'string') {
    return false
  }
  return SLUG_PATTERN_LONG.test(slug) || SLUG_PATTERN_SHORT.test(slug)
}

/**
 * Error message for invalid slug format
 */
export const SLUG_ERROR_MESSAGE = `Invalid slug: must be ${SLUG_MIN_LENGTH}-${SLUG_MAX_LENGTH} lowercase alphanumeric characters with hyphens`

/**
 * Validate slug and throw an error if invalid
 *
 * @param slug - The slug to validate
 * @throws Error if the slug is invalid
 *
 * @example
 * ```ts
 * validateSlug('my-dataset')  // passes
 * validateSlug('My-Dataset')  // throws Error
 * ```
 */
export function validateSlug(slug: string): void {
  if (!isValidSlug(slug)) {
    throw new Error(SLUG_ERROR_MESSAGE)
  }
}
