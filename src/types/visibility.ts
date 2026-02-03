/**
 * Visibility Types for ParqueDB
 *
 * Controls access to databases and collections:
 * - public: Discoverable and accessible by anyone
 * - unlisted: Accessible with direct link, not discoverable
 * - private: Requires authentication
 */

/**
 * Visibility level for databases and collections
 *
 * | Value     | Discoverable | Anonymous Read | Requires Auth |
 * |-----------|-------------|----------------|---------------|
 * | public    | Yes         | Yes            | No            |
 * | unlisted  | No          | Yes (with link)| No            |
 * | private   | No          | No             | Yes           |
 */
export type Visibility = 'public' | 'unlisted' | 'private'

/**
 * Default visibility for new databases and collections
 * Private by default for security
 */
export const DEFAULT_VISIBILITY: Visibility = 'private'

/**
 * All valid visibility values
 */
export const VISIBILITY_VALUES: readonly Visibility[] = ['public', 'unlisted', 'private'] as const

/**
 * Check if a value is a valid visibility
 */
export function isValidVisibility(value: unknown): value is Visibility {
  return typeof value === 'string' && VISIBILITY_VALUES.includes(value as Visibility)
}

/**
 * Parse visibility from string, returning default if invalid
 */
export function parseVisibility(value: string | undefined): Visibility {
  if (value && isValidVisibility(value)) {
    return value
  }
  return DEFAULT_VISIBILITY
}

/**
 * Check if visibility allows anonymous read access
 */
export function allowsAnonymousRead(visibility: Visibility): boolean {
  return visibility === 'public' || visibility === 'unlisted'
}

/**
 * Check if visibility allows discovery (listing)
 */
export function allowsDiscovery(visibility: Visibility): boolean {
  return visibility === 'public'
}
