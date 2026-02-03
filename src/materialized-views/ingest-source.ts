/**
 * IngestSource Types for ParqueDB Stream Collections
 *
 * Provides type-safe ingest source definitions using template literal types
 * instead of the lossy `'literal' | string` pattern.
 *
 * The old pattern `'ai-sdk' | 'tail' | 'evalite' | string` defeats type safety
 * because TypeScript collapses it to just `string`, losing autocomplete and
 * type narrowing capabilities.
 *
 * This module provides:
 * - `KnownIngestSource`: Literal union of known sources
 * - `CustomIngestSource`: Template literal type for custom sources (`custom:${string}`)
 * - `IngestSource`: Union that preserves type safety
 * - Type guards for runtime discrimination
 *
 * @example
 * ```typescript
 * // Known sources - autocomplete works!
 * const aiCollection = { $ingest: 'ai-sdk' }
 *
 * // Custom sources - use template literal prefix
 * const customCollection = { $ingest: 'custom:my-webhook' }
 * ```
 */

/**
 * Known ingest source identifiers
 *
 * - 'ai-sdk': AI SDK middleware (generates AIRequests, Generations)
 * - 'tail': Cloudflare Workers tail events
 * - 'evalite': Evalite evaluation framework
 */
export type KnownIngestSource = 'ai-sdk' | 'tail' | 'evalite'

/**
 * Array of known ingest source values for validation
 */
export const KNOWN_INGEST_SOURCES: readonly KnownIngestSource[] = [
  'ai-sdk',
  'tail',
  'evalite',
] as const

/**
 * Custom ingest source using template literal type
 *
 * Use this for user-defined ingest handlers. The `custom:` prefix ensures
 * type safety while allowing any string value after the prefix.
 *
 * @example
 * ```typescript
 * const source: CustomIngestSource = 'custom:my-webhook'
 * const source2: CustomIngestSource = 'custom:stripe-events'
 * ```
 */
export type CustomIngestSource = `custom:${string}`

/**
 * Ingest source type - either a known source or a custom source with prefix
 *
 * This design provides type safety:
 * - Known sources get autocomplete and type narrowing
 * - Custom sources use the `custom:` prefix for explicit identification
 *
 * @example
 * ```typescript
 * // Using known sources (type-safe with autocomplete)
 * const aiCollection: CollectionDefinition = {
 *   $type: 'AIRequest',
 *   $ingest: 'ai-sdk',  // Autocomplete works!
 * }
 *
 * // Using custom sources (template literal prefix)
 * const customCollection: CollectionDefinition = {
 *   $type: 'CustomEvent',
 *   $ingest: 'custom:my-handler',  // Type-safe, no helper needed!
 * }
 * ```
 */
export type IngestSource = KnownIngestSource | CustomIngestSource

/**
 * Create a custom ingest source from a string
 *
 * Utility function for programmatic creation of custom sources.
 * For static definitions, prefer the template literal syntax: `'custom:my-handler'`
 *
 * @param source - The custom source identifier string (without prefix)
 * @returns A CustomIngestSource with the `custom:` prefix
 *
 * @example
 * ```typescript
 * const mySource = customIngestSource('my-handler')
 * // Returns: 'custom:my-handler'
 *
 * // Prefer direct syntax when possible:
 * const collection: CollectionDefinition = {
 *   $type: 'MyEvent',
 *   $ingest: 'custom:my-handler',
 * }
 * ```
 */
export function customIngestSource(source: string): CustomIngestSource {
  return `custom:${source}` as CustomIngestSource
}

/**
 * Check if a value is a known ingest source
 *
 * @param value - The value to check
 * @returns true if the value is one of the known ingest sources
 *
 * @example
 * ```typescript
 * if (isKnownIngestSource(source)) {
 *   // source is narrowed to KnownIngestSource
 *   switch (source) {
 *     case 'ai-sdk': // ...
 *     case 'tail': // ...
 *   }
 * }
 * ```
 */
export function isKnownIngestSource(value: unknown): value is KnownIngestSource {
  return typeof value === 'string' && KNOWN_INGEST_SOURCES.includes(value as KnownIngestSource)
}

/**
 * Check if a value is a custom ingest source (has `custom:` prefix)
 *
 * @param value - The value to check
 * @returns true if the value is a custom ingest source
 *
 * @example
 * ```typescript
 * if (isCustomIngestSource(source)) {
 *   // source is narrowed to CustomIngestSource
 *   const handlerName = source.slice(7) // Remove 'custom:' prefix
 * }
 * ```
 */
export function isCustomIngestSource(value: unknown): value is CustomIngestSource {
  return typeof value === 'string' && value.startsWith('custom:') && value.length > 7
}

/**
 * Check if a value is a valid ingest source (known or custom with prefix)
 *
 * @param value - The value to check
 * @returns true if the value is a valid ingest source
 *
 * @example
 * ```typescript
 * if (isIngestSource(value)) {
 *   // value is narrowed to IngestSource
 *   if (isKnownIngestSource(value)) {
 *     // Handle known source
 *   } else {
 *     // Handle custom source (has 'custom:' prefix)
 *   }
 * }
 * ```
 */
export function isIngestSource(value: unknown): value is IngestSource {
  return isKnownIngestSource(value) || isCustomIngestSource(value)
}

/**
 * Extract the handler name from a custom ingest source
 *
 * @param source - A custom ingest source
 * @returns The handler name without the `custom:` prefix
 *
 * @example
 * ```typescript
 * const source: CustomIngestSource = 'custom:my-webhook'
 * const handler = getCustomSourceHandler(source) // 'my-webhook'
 * ```
 */
export function getCustomSourceHandler(source: CustomIngestSource): string {
  return source.slice(7) // Remove 'custom:' prefix
}
