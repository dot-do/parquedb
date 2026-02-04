/**
 * Type utilities for reducing unsafe type assertions
 *
 * These helpers provide safer alternatives to `as unknown as T` patterns
 * by using proper type narrowing, guards, and typed proxy creation.
 *
 * @module utils/type-utils
 */

import pluralize from 'pluralize'
import { isNullish, isNotNullish } from './comparison'

// Re-export nullish utilities from comparison.ts (canonical source)
export { isNullish, isNotNullish }

/**
 * Check if a value is a plain object (not null, not array, not primitive)
 *
 * @param value - Value to check
 * @returns true if value is a plain object
 *
 * @example
 * isObject({}) // true
 * isObject({ a: 1 }) // true
 * isObject(null) // false
 * isObject([]) // false
 * isObject('string') // false
 */
export function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

/**
 * Check if a value is an array
 *
 * @param value - Value to check
 * @returns true if value is an array
 *
 * @example
 * isArray([]) // true
 * isArray([1, 2, 3]) // true
 * isArray(null) // false
 * isArray({}) // false
 */
export function isArray(value: unknown): value is unknown[] {
  return Array.isArray(value)
}

/**
 * Get a nested value from an object using dot notation
 *
 * @param obj - Object to read from (can be null/undefined)
 * @param path - Dot-notation path
 * @returns Value at path, or undefined if not found
 *
 * @example
 * getNestedValue({ a: { b: 1 } }, 'a.b') // 1
 * getNestedValue(null, 'a.b') // undefined
 */
export function getNestedValue(obj: unknown, path: string): unknown {
  if (isNullish(obj)) return undefined

  const parts = path.split('.')
  let current: unknown = obj

  for (const part of parts) {
    if (isNullish(current)) return undefined
    if (typeof current !== 'object') return undefined
    current = (current as Record<string, unknown>)[part]
  }

  return current
}

/**
 * Return the first non-nullish value from a list of values
 *
 * Similar to JavaScript's nullish coalescing operator (??) but works with
 * multiple values.
 *
 * @param values - Values to check
 * @returns First non-nullish value, or undefined if all are nullish
 *
 * @example
 * coalesce(null, undefined, 'found') // 'found'
 * coalesce(0, 1) // 0 (0 is not nullish)
 * coalesce('', 'fallback') // '' (empty string is not nullish)
 */
export function coalesce<T>(...values: (T | null | undefined)[]): T | undefined {
  for (const value of values) {
    if (isNotNullish(value)) {
      return value
    }
  }
  return undefined
}

/**
 * Return the first non-nullish value from a list, with a guaranteed default
 *
 * @param defaultValue - Default value to return if all others are nullish
 * @param values - Values to check
 * @returns First non-nullish value, or defaultValue
 *
 * @example
 * coalesceDefault('default', null, undefined) // 'default'
 * coalesceDefault(100, 0) // 0 (0 is not nullish)
 */
export function coalesceDefault<T>(defaultValue: T, ...values: (T | null | undefined)[]): T {
  for (const value of values) {
    if (isNotNullish(value)) {
      return value
    }
  }
  return defaultValue
}

// =============================================================================
// Typed Proxy Helpers
// =============================================================================

/**
 * Create a proxy with proper typing
 *
 * Use this instead of `new Proxy({}, handler) as unknown as T`
 *
 * @example
 * ```typescript
 * const proxy = createTypedProxy<DBInstance>({
 *   get(target, prop) {
 *     return target[prop]
 *   }
 * })
 * ```
 */
export function createTypedProxy<T extends object, O extends object = object>(
  target: O,
  handler: ProxyHandler<T>
): T {
  // The cast here is necessary because Proxy's type system doesn't support
  // returning a different type than the target. This is the canonical pattern
  // for typed proxies in TypeScript.
  return new Proxy(target, handler as ProxyHandler<O>) as T
}

// =============================================================================
// Type Bridging Helpers
// =============================================================================

/**
 * Bridge between @cloudflare/workers-types R2Bucket and our internal R2Bucket type
 *
 * The @cloudflare/workers-types R2Bucket and our internal R2Bucket interface
 * are structurally compatible at runtime but have minor type differences
 * (e.g., string literal vs string for storageClass). This function provides
 * explicit type bridging with documentation.
 *
 * @example
 * ```typescript
 * import type { R2Bucket } from '../storage/types/r2'
 * const bucket = toR2Bucket<R2Bucket>(env.BUCKET)
 * return new R2Backend(bucket)
 * ```
 */
export function toR2Bucket<T>(bucket: unknown): T {
  // Cloudflare's R2Bucket from @cloudflare/workers-types and our internal
  // R2Bucket interface are structurally compatible. The type difference is
  // due to @cloudflare/workers-types using `string` where we use `"Standard"`.
  // This intentional type crossing is safe because the runtime behavior matches.
  return bucket as T
}

/**
 * Create an empty object proxy with proper typing
 *
 * Shorthand for `createTypedProxy({}, handler)` which is the most common case
 */
export function createEmptyProxy<T extends object>(
  handler: ProxyHandler<T>
): T {
  return createTypedProxy<T>({}, handler)
}

// =============================================================================
// Cloudflare Workers Type Helpers
// =============================================================================

/**
 * Get a typed Durable Object stub from a namespace
 *
 * Cloudflare's DurableObjectNamespace.get() returns DurableObjectStub which doesn't
 * include RPC method types. This helper provides proper typing for RPC calls.
 *
 * @example
 * ```typescript
 * const stub = getDOStub<ParqueDBDOStub>(env.PARQUEDB, doId)
 * const result = await stub.create(ns, data)
 * ```
 */
export function getDOStub<T>(
  namespace: DurableObjectNamespace,
  id: DurableObjectId
): T {
  // Cloudflare Worker RPC stubs are duck-typed at runtime.
  // The DO returns a DurableObjectStub that implements the DO's public methods.
  // TypeScript can't know the DO's interface at compile time, so we need this cast.
  return namespace.get(id) as T
}

/**
 * Get a typed Durable Object stub by name
 *
 * Convenience wrapper that combines idFromName + get
 *
 * @example
 * ```typescript
 * const stub = getDOStubByName<ParqueDBDOStub>(env.PARQUEDB, 'users')
 * ```
 */
export function getDOStubByName<T>(
  namespace: DurableObjectNamespace,
  name: string
): T {
  const id = namespace.idFromName(name)
  return getDOStub<T>(namespace, id)
}

// =============================================================================
// Hono Context Helpers
// =============================================================================

/**
 * Get a typed variable from Hono context
 *
 * Hono's c.var type depends on the Variables generic, but when using
 * sub-routers or middleware from different modules, the types don't align.
 * This provides type-safe access with explicit typing.
 *
 * @example
 * ```typescript
 * const user = getContextVar<AuthUser>(c, 'user')
 * ```
 */
export function getContextVar<T>(
  c: { var: Record<string, unknown> },
  key: string
): T | undefined {
  return c.var[key] as T | undefined
}

/**
 * Get a typed variable from Hono context with a default value
 */
export function getContextVarOr<T>(
  c: { var: Record<string, unknown> },
  key: string,
  defaultValue: T
): T {
  return (c.var[key] as T | undefined) ?? defaultValue
}

// =============================================================================
// Record Type Helpers
// =============================================================================

/**
 * Cast to a generic record type
 *
 * Use when you know an object is a record but TypeScript doesn't.
 * Prefer type guards when possible, but this is useful for intermediate
 * processing where full validation isn't needed.
 *
 * @example
 * ```typescript
 * const record = asRecord(entity)
 * const value = record['customField']
 * ```
 */
export function asRecord(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object') {
    throw new TypeError(`Expected object, got ${typeof value}`)
  }
  return value as Record<string, unknown>
}

/**
 * Safely cast to record, returning undefined if not an object
 */
export function toRecord(value: unknown): Record<string, unknown> | undefined {
  if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
    return value as Record<string, unknown>
  }
  return undefined
}

// =============================================================================
// Config Parsing Helpers
// =============================================================================

/**
 * Type guard for checking if a parsed value matches expected interface
 *
 * Use with safeJsonParse to validate config files:
 *
 * @example
 * ```typescript
 * interface Config { name: string; count: number }
 *
 * const result = safeJsonParse(data)
 * if (result.ok && isConfigLike<Config>(result.value, ['name', 'count'])) {
 *   // result.value is now typed as Config
 * }
 * ```
 */
export function isConfigLike<T extends Record<string, unknown>>(
  value: unknown,
  requiredKeys: (keyof T)[]
): value is T {
  if (value === null || typeof value !== 'object') {
    return false
  }
  const record = value as Record<string, unknown>
  return requiredKeys.every(key => key in record)
}

/**
 * Parse a config object with validation
 *
 * Returns the typed config or undefined if validation fails.
 * More specific than `as unknown as T` because it checks structure.
 */
export function parseConfig<T extends Record<string, unknown>>(
  value: unknown,
  requiredKeys: (keyof T)[]
): T | undefined {
  if (isConfigLike<T>(value, requiredKeys)) {
    return value
  }
  return undefined
}

// =============================================================================
// Array Type Helpers
// =============================================================================

/**
 * Type assertion for array operations that change element type
 *
 * When manipulating arrays (like in setField), TypeScript can't always track
 * that an array operation preserves the container type. This documents
 * the intentional type boundary.
 *
 * @example
 * ```typescript
 * const newArr = [...arr]
 * newArr[index] = value
 * return asArray<T>(newArr)
 * ```
 */
export function asArray<T>(arr: unknown[]): T {
  // This cast is intentional: when we clone and modify an array,
  // we know the result maintains the same type as the input.
  // TypeScript can't prove this statically for generic containers.
  return arr as unknown as T
}

// =============================================================================
// Dynamic Access Helpers
// =============================================================================

/**
 * Access a collection by name from a database instance
 *
 * ParqueDB uses Proxy-based collection access (db.Posts, db.Users).
 * TypeScript can't know the collection names at compile time,
 * so this provides type-safe dynamic access.
 *
 * @example
 * ```typescript
 * const collection = getCollection(db, 'Posts')
 * await collection.find({})
 * ```
 */
export function getCollection<T>(
  db: Record<string, unknown>,
  name: string
): T | undefined {
  const collection = db[name]
  if (collection && typeof collection === 'object') {
    return collection as T
  }
  return undefined
}

/**
 * Extend a function with additional properties
 *
 * Used when building function objects that have both call signature
 * and property access (like the sql template tag with sql.raw method).
 *
 * @example
 * ```typescript
 * const fn = function(...args) { ... }
 * return extendFunction(fn, { raw: async (...) => { ... } })
 * ```
 */
export function extendFunction<T extends (...args: never[]) => unknown, E extends object>(
  fn: T,
  extensions: E
): T & E {
  return Object.assign(fn, extensions)
}

// =============================================================================
// Type/Namespace Conversion
// =============================================================================

/**
 * Convert a type name to a namespace (lowercase, pluralized)
 *
 * Uses the 'pluralize' library for proper English pluralization including:
 * - Regular plurals: User -> users, Post -> posts
 * - -y to -ies: Category -> categories, Company -> companies
 * - -s to -ses: Status -> statuses, Alias -> aliases
 * - -x to -xes/-ces: Index -> indices, Box -> boxes
 * - Irregular plurals: Person -> people, Child -> children
 * - Uncountable nouns: News -> news, Equipment -> equipment
 *
 * @param type - The type name (e.g., 'User', 'Post', 'Category')
 * @returns The namespace (e.g., 'users', 'posts', 'categories')
 *
 * @example
 * typeToNamespace('User') // 'users'
 * typeToNamespace('Category') // 'categories'
 * typeToNamespace('Person') // 'people'
 * typeToNamespace('News') // 'news'
 */
export function typeToNamespace(type: string): string {
  const lower = type.toLowerCase()
  return pluralize.plural(lower)
}

/**
 * Convert a namespace to a type name (capitalized, singularized)
 *
 * Uses the 'pluralize' library for proper English singularization including:
 * - Regular singulars: users -> User, posts -> Post
 * - -ies to -y: categories -> Category, companies -> Company
 * - -ses to -s: statuses -> Status, aliases -> Alias
 * - Irregular singulars: people -> Person, children -> Child
 * - Uncountable nouns: news -> News, equipment -> Equipment
 *
 * @param namespace - The namespace (e.g., 'users', 'posts', 'categories')
 * @returns The type name (e.g., 'User', 'Post', 'Category')
 *
 * @example
 * namespaceToType('users') // 'User'
 * namespaceToType('categories') // 'Category'
 * namespaceToType('people') // 'Person'
 * namespaceToType('news') // 'News'
 */
export function namespaceToType(namespace: string): string {
  const singular = pluralize.singular(namespace)
  return singular.charAt(0).toUpperCase() + singular.slice(1)
}
